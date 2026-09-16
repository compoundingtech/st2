//! The environment that a new interactive login shell gives to user work.

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context as _, Result};
use wait_timeout::ChildExt as _;

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;

const SHELL_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn login_environment() -> Result<BTreeMap<String, String>> {
    let shell = account_shell().context("the user account has no default shell")?;
    login_environment_from(&shell, SHELL_STARTUP_TIMEOUT)
}

fn login_environment_from(shell: &Path, timeout: Duration) -> Result<BTreeMap<String, String>> {
    let mut output = tempfile::tempfile().context("create the shell environment buffer")?;
    let output_writer = output
        .try_clone()
        .context("clone the shell environment buffer")?;
    let mut command = Command::new(shell);
    command
        .args(["-l", "-i", "-c", "/usr/bin/printf '\\0'; /usr/bin/env -0"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(output_writer))
        .stderr(Stdio::null());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("run the default shell {}", shell.display()))?;
    let status = match child
        .wait_timeout(timeout)
        .context("wait for the default shell")?
    {
        Some(status) => status,
        None => {
            #[cfg(unix)]
            let killed = unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) == 0 };
            #[cfg(not(unix))]
            let killed = child.kill().is_ok();
            anyhow::ensure!(killed, "stop the timed-out default shell");
            let _ = child.wait();
            anyhow::bail!(
                "the default shell {} did not finish startup within {} seconds",
                shell.display(),
                timeout.as_secs_f64()
            );
        }
    };
    anyhow::ensure!(
        status.success(),
        "the default shell {} exited with {}",
        shell.display(),
        status
    );
    output.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    output.read_to_end(&mut bytes)?;
    parse_login_environment(&bytes)
}

pub fn resolve_executable(
    program: &str,
    environment: &BTreeMap<String, String>,
) -> Result<PathBuf> {
    if program.contains('/') {
        return Ok(PathBuf::from(program));
    }
    let path = environment
        .get("PATH")
        .context("the default shell did not export PATH")?;
    for directory in std::env::split_paths(path) {
        let candidate = directory.join(program);
        if candidate.is_file() && is_executable(&candidate) {
            return Ok(candidate);
        }
    }
    anyhow::bail!("`{program}` is not executable on the default shell PATH")
}

pub fn materialize_environment(
    declared: &BTreeMap<String, String>,
    executable: &Path,
) -> Result<BTreeMap<String, String>> {
    let environment = login_environment()?;
    overlay_environment(environment, declared, executable)
}

fn overlay_environment(
    mut environment: BTreeMap<String, String>,
    declared: &BTreeMap<String, String>,
    executable: &Path,
) -> Result<BTreeMap<String, String>> {
    let shell_path = environment
        .get("PATH")
        .cloned()
        .context("the default shell did not export PATH")?;
    environment.extend(
        declared
            .iter()
            .map(|(name, value)| (name.clone(), value.replace("${PATH}", &shell_path))),
    );
    prepend_executable_dir(&mut environment, executable)?;
    for name in [
        "OLDPWD",
        "PWD",
        "SHLVL",
        "TERM",
        "TERM_PROGRAM",
        "TERM_PROGRAM_VERSION",
        "TERM_SESSION_ID",
        "_",
    ] {
        if !declared.contains_key(name) {
            environment.remove(name);
        }
    }
    Ok(environment)
}

pub fn expand_path_placeholder(value: &mut String, environment: &BTreeMap<String, String>) {
    if let Some(path) = environment.get("PATH") {
        *value = value.replace("${PATH}", path);
    }
}

fn parse_login_environment(bytes: &[u8]) -> Result<BTreeMap<String, String>> {
    let start = bytes
        .iter()
        .position(|byte| *byte == 0)
        .context("the default shell did not start the environment record")?;
    let mut environment = BTreeMap::new();
    for record in bytes[start + 1..].split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let record = std::str::from_utf8(record).context("the shell environment is not UTF-8")?;
        let Some((name, value)) = record.split_once('=') else {
            continue;
        };
        if valid_name(name) {
            environment.insert(name.into(), value.into());
        }
    }
    anyhow::ensure!(
        environment.contains_key("PATH"),
        "the default shell did not export PATH"
    );
    Ok(environment)
}

fn valid_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(unix)]
fn account_shell() -> Option<PathBuf> {
    let entry = unsafe { libc::getpwuid(libc::getuid()) };
    if entry.is_null() {
        return std::env::var_os("SHELL").map(PathBuf::from);
    }
    let shell = unsafe { CStr::from_ptr((*entry).pw_shell) };
    let shell = shell.to_string_lossy();
    (!shell.is_empty()).then(|| PathBuf::from(shell.as_ref()))
}

#[cfg(not(unix))]
fn account_shell() -> Option<PathBuf> {
    std::env::var_os("SHELL").map(PathBuf::from)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    path.metadata()
        .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

fn prepend_executable_dir(
    environment: &mut BTreeMap<String, String>,
    executable: &Path,
) -> Result<()> {
    let Some(directory) = executable.parent() else {
        return Ok(());
    };
    let current = environment.get("PATH").cloned().unwrap_or_default();
    let paths = std::iter::once(directory.to_path_buf())
        .chain(std::env::split_paths(&current).filter(|path| path != directory));
    environment.insert(
        "PATH".into(),
        std::env::join_paths(paths)?.to_string_lossy().into_owned(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn parser_ignores_shell_startup_output_before_the_record() {
        let parsed = parse_login_environment(
            b"welcome from the shell\n\0PATH=/custom/bin:/usr/bin\0LANG=en_US.UTF-8\0BAD-NAME=x\0",
        )
        .unwrap();
        assert_eq!(parsed["PATH"], "/custom/bin:/usr/bin");
        assert_eq!(parsed["LANG"], "en_US.UTF-8");
        assert!(!parsed.contains_key("BAD-NAME"));
    }

    #[test]
    fn declared_values_overlay_the_shell_and_expand_its_path() {
        let environment = BTreeMap::from([
            ("PATH".into(), "/shell/bin:/usr/bin".into()),
            ("VALUE".into(), "shell".into()),
            ("TERM".into(), "stale".into()),
        ]);
        let declared = BTreeMap::from([
            ("CUSTOM".into(), "/shim:${PATH}".into()),
            ("VALUE".into(), "declared".into()),
        ]);
        let environment =
            overlay_environment(environment, &declared, Path::new("/st3/bin/st3")).unwrap();
        assert_eq!(environment["CUSTOM"], "/shim:/shell/bin:/usr/bin");
        assert_eq!(environment["VALUE"], "declared");
        assert_eq!(environment["PATH"], "/st3/bin:/shell/bin:/usr/bin");
        assert!(!environment.contains_key("TERM"));
    }

    #[cfg(unix)]
    #[test]
    fn login_probe_accepts_startup_output_and_reads_the_exported_environment() {
        let root = tempfile::tempdir().unwrap();
        let shell = root.path().join("shell");
        std::fs::write(
            &shell,
            "#!/bin/sh\nprintf 'startup output\\n'\nprintf '\\0PATH=/fresh/bin:/usr/bin\\0FRESH=yes\\0'\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&shell).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&shell, permissions).unwrap();

        let environment = login_environment_from(&shell, Duration::from_secs(1)).unwrap();

        assert_eq!(environment["PATH"], "/fresh/bin:/usr/bin");
        assert_eq!(environment["FRESH"], "yes");
    }

    #[cfg(unix)]
    #[test]
    fn login_probe_stops_a_shell_whose_startup_does_not_finish() {
        let root = tempfile::tempdir().unwrap();
        let shell = root.path().join("shell");
        std::fs::write(&shell, "#!/bin/sh\nsleep 5\n").unwrap();
        let mut permissions = std::fs::metadata(&shell).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&shell, permissions).unwrap();

        let error = login_environment_from(&shell, Duration::from_millis(25)).unwrap_err();

        assert!(error.to_string().contains("did not finish startup"));
    }
}
