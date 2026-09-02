use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use anyhow::{Context as _, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Launch {
    Shell(String),
    Argv(Vec<String>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PtyObservation {
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct PtyRuntime {
    binary: String,
    root: PathBuf,
}

impl PtyRuntime {
    pub fn new(root: PathBuf) -> Self {
        crate::warn_if_degraded("st3");
        Self {
            binary: "pty".into(),
            root,
        }
    }

    pub fn with_binary(mut self, binary: impl Into<String>) -> Self {
        self.binary = binary.into();
        self
    }

    pub fn snapshot(&self) -> Result<Vec<PtyObservation>> {
        let output = self.command().args(["list", "--json"]).output()?;
        require_success("list PTYs", output).and_then(|bytes| {
            serde_json::from_slice(&bytes).context("parse the atomic PTY snapshot")
        })
    }

    pub fn spawn(
        &self,
        id: &str,
        launch: &Launch,
        cwd: &Path,
        env: &BTreeMap<String, String>,
        display_name: Option<&str>,
        tags: &BTreeMap<String, String>,
    ) -> Result<()> {
        let unit = crate::scope_unit("st3", id);
        let mut arguments = vec![
            OsString::from("run"),
            OsString::from("-d"),
            OsString::from("--force"),
            OsString::from("--id"),
            OsString::from(id),
            OsString::from("--cwd"),
            cwd.as_os_str().to_os_string(),
        ];
        if let Some(display_name) = display_name {
            arguments.extend([OsString::from("--name"), OsString::from(display_name)]);
        } else {
            arguments.push(OsString::from("--no-display-name"));
        }
        let mut terminal_env = env.clone();
        terminal_env
            .entry("TERM".into())
            .or_insert_with(|| "xterm-256color".into());
        for (key, value) in &terminal_env {
            arguments.extend([
                OsString::from("--env"),
                OsString::from(format!("{key}={value}")),
            ]);
        }
        let mut effective_tags = tags.clone();
        effective_tags.insert(
            "st3.isolation".into(),
            isolation_name(crate::isolation_mode()).into(),
        );
        if crate::isolation_mode() == crate::Isolation::Scope {
            effective_tags.insert("st3.scope-unit".into(), unit.clone());
        }
        for (key, value) in effective_tags {
            arguments.extend([
                OsString::from("--tag"),
                OsString::from(format!("{key}={value}")),
            ]);
        }
        arguments.push(OsString::from("--"));
        match launch {
            Launch::Shell(source) => {
                arguments.extend([OsString::from("sh"), OsString::from("-c"), source.into()]);
            }
            Launch::Argv(argv) => arguments.extend(argv.iter().map(OsString::from)),
        }
        let argument_refs = arguments
            .iter()
            .map(OsString::as_os_str)
            .collect::<Vec<_>>();
        const ATTEMPTS: u32 = 4;
        let mut last_error = String::new();
        for attempt in 0..ATTEMPTS {
            let mut command =
                crate::wrap_isolated(&unit, std::ffi::OsStr::new(&self.binary), &argument_refs);
            command.env("PTY_ROOT", &self.root);
            let output = command.output()?;
            if output.status.success() {
                return Ok(());
            }
            last_error = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if !last_error.contains("already in use") || attempt + 1 == ATTEMPTS {
                break;
            }
            let _ = self.remove(id);
            std::thread::sleep(Duration::from_millis(100 * u64::from(attempt + 1)));
        }
        anyhow::bail!("spawn PTY failed: {last_error}")
    }

    pub fn stop(&self, id: &str) -> Result<()> {
        self.stop_if(id, None)
    }

    pub fn stop_if(&self, id: &str, expected_incarnation: Option<&str>) -> Result<()> {
        self.require_incarnation(id, expected_incarnation)?;
        let output = self.command().args(["kill", id]).output()?;
        require_success("stop PTY", output)?;
        Ok(())
    }

    pub fn kill(&self, id: &str) -> Result<()> {
        self.kill_if(id, None)
    }

    pub fn kill_if(&self, id: &str, expected_incarnation: Option<&str>) -> Result<()> {
        self.signal_if(id, expected_incarnation, libc::SIGKILL)
    }

    pub fn signal_if(
        &self,
        id: &str,
        expected_incarnation: Option<&str>,
        signal: i32,
    ) -> Result<()> {
        let observation = self
            .snapshot()?
            .into_iter()
            .find(|item| item.name == id)
            .with_context(|| format!("PTY `{id}` is not present"))?;
        ensure_incarnation(id, &observation, expected_incarnation)?;
        let pid = observation
            .pid
            .with_context(|| format!("PTY `{id}` has no process identity"))?;
        let group = unsafe { libc::kill(-(pid as i32), signal) };
        if group != 0 {
            let direct = unsafe { libc::kill(pid as i32, signal) };
            if direct != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).with_context(|| format!("signal {signal} to PTY process"));
                }
            }
        }
        Ok(())
    }

    fn require_incarnation(&self, id: &str, expected_incarnation: Option<&str>) -> Result<()> {
        let observation = self
            .snapshot()?
            .into_iter()
            .find(|item| item.name == id)
            .with_context(|| format!("PTY `{id}` is not present"))?;
        ensure_incarnation(id, &observation, expected_incarnation)
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let output = self.command().args(["remove", id]).output()?;
        require_success("remove PTY", output)?;
        Ok(())
    }

    pub fn attach(&self, id: &str) -> Result<()> {
        let status = self
            .command()
            .args(["attach", id])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;
        anyhow::ensure!(status.success(), "PTY attach failed with {status}");
        Ok(())
    }

    pub fn send_line(&self, id: &str, text: &str) -> Result<()> {
        self.send_line_if(id, text, None)
    }

    pub fn send_line_if(
        &self,
        id: &str,
        text: &str,
        expected_incarnation: Option<&str>,
    ) -> Result<()> {
        self.require_incarnation(id, expected_incarnation)?;
        let output = self
            .command()
            .args(["send", id, "--seq", text, "--seq", "key:return"])
            .output()?;
        require_success("send PTY input", output)?;
        Ok(())
    }

    pub fn send_raw(&self, id: &str, bytes: &[u8]) -> Result<()> {
        self.send_raw_if(id, bytes, None)
    }

    pub fn send_raw_if(
        &self,
        id: &str,
        bytes: &[u8],
        expected_incarnation: Option<&str>,
    ) -> Result<()> {
        self.require_incarnation(id, expected_incarnation)?;
        anyhow::ensure!(
            !bytes.contains(&0),
            "terminal input cannot contain a NUL byte"
        );
        let output = self
            .command()
            .arg("send")
            .arg(id)
            .arg(OsString::from_vec(bytes.to_vec()))
            .output()?;
        require_success("send PTY input", output)?;
        Ok(())
    }

    pub fn send_key(&self, id: &str, key: &str) -> Result<()> {
        self.send_key_if(id, key, None)
    }

    pub fn send_key_if(
        &self,
        id: &str,
        key: &str,
        expected_incarnation: Option<&str>,
    ) -> Result<()> {
        self.require_incarnation(id, expected_incarnation)?;
        let output = self
            .command()
            .args(["send", id, "--seq", &format!("key:{key}")])
            .output()?;
        require_success("send PTY key", output)?;
        Ok(())
    }

    pub fn screen(&self, id: &str) -> Result<String> {
        let output = self.command().args(["peek", "--plain", id]).output()?;
        let bytes = require_success("read PTY screen", output)?;
        String::from_utf8(bytes).context("the PTY screen is not UTF-8")
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn binary(&self) -> &str {
        &self.binary
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command.env("PTY_ROOT", &self.root);
        command
    }
}

fn isolation_name(mode: crate::Isolation) -> &'static str {
    match mode {
        crate::Isolation::Scope => "scope",
        crate::Isolation::Detached => "detached",
        crate::Isolation::DegradedDetached => "degraded-detached",
    }
}

fn ensure_incarnation(
    id: &str,
    observation: &PtyObservation,
    expected_incarnation: Option<&str>,
) -> Result<()> {
    let current = match (&observation.pid, &observation.created_at) {
        (Some(pid), Some(created_at)) => Some(format!("{pid}:{created_at}")),
        _ => None,
    };
    if expected_incarnation.is_some_and(|expected| current.as_deref() != Some(expected)) {
        anyhow::bail!("PTY `{id}` changed incarnation before the control action");
    }
    Ok(())
}

fn require_success(action: &str, output: Output) -> Result<Vec<u8>> {
    anyhow::ensure!(
        output.status.success(),
        "{action} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn terminal_input_checks_the_expected_incarnation() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("fake-pty");
        fs::write(
            &binary,
            r#"#!/bin/sh
if [ "$1" = list ]; then
  printf '[{"name":"work","status":"running","pid":42,"createdAt":"now"}]'
  exit 0
fi
exit 0
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let runtime =
            PtyRuntime::new(root.path().join("registry")).with_binary(binary.to_string_lossy());

        runtime
            .send_line_if("work", "hello", Some("42:now"))
            .unwrap();
        runtime
            .send_raw_if("work", b"bytes", Some("42:now"))
            .unwrap();
        runtime
            .send_key_if("work", "escape", Some("42:now"))
            .unwrap();
        let error = runtime
            .send_key_if("work", "escape", Some("41:old"))
            .unwrap_err();
        assert!(error.to_string().contains("changed incarnation"));
    }

    #[test]
    fn spawn_records_the_shared_isolation_mode() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("fake-pty-spawn");
        fs::write(&binary, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.args\"\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let runtime =
            PtyRuntime::new(root.path().join("registry")).with_binary(binary.to_string_lossy());
        runtime
            .spawn(
                "work",
                &Launch::Argv(vec!["sh".into(), "-c".into(), "true".into()]),
                root.path(),
                &BTreeMap::new(),
                None,
                &BTreeMap::new(),
            )
            .unwrap();
        let arguments = fs::read_to_string(binary.with_extension("args")).unwrap();
        assert!(arguments.contains("st3.isolation="));
        assert!(arguments.contains("TERM=xterm-256color"));
        assert!(arguments.contains("--force"));
        if crate::isolation_mode() == crate::Isolation::Scope {
            assert!(arguments.contains("st3.scope-unit=st3-work-"));
        }
    }

    #[test]
    fn spawn_preserves_an_explicit_terminal_type() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("fake-pty-term");
        fs::write(&binary, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.args\"\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let runtime =
            PtyRuntime::new(root.path().join("registry")).with_binary(binary.to_string_lossy());

        runtime
            .spawn(
                "work",
                &Launch::Argv(vec!["true".into()]),
                root.path(),
                &BTreeMap::from([("TERM".into(), "screen-256color".into())]),
                None,
                &BTreeMap::new(),
            )
            .unwrap();

        let arguments = fs::read_to_string(binary.with_extension("args")).unwrap();
        assert!(arguments.contains("TERM=screen-256color"));
        assert!(!arguments.contains("TERM=xterm-256color"));
    }

    #[test]
    fn spawn_reaps_a_recent_session_id_and_retries() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("fake-pty-retry");
        fs::write(
            &binary,
            r#"#!/bin/sh
if [ "$1" = run ]; then
  count=0
  test ! -f "$0.count" || count="$(cat "$0.count")"
  count=$((count + 1))
  printf '%s\n' "$count" > "$0.count"
  if [ "$count" -eq 1 ]; then
    printf '%s\n' 'Session id "work" is already in use.' >&2
    exit 1
  fi
fi
if [ "$1" = remove ]; then
  touch "$0.removed"
fi
exit 0
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let runtime =
            PtyRuntime::new(root.path().join("registry")).with_binary(binary.to_string_lossy());

        runtime
            .spawn(
                "work",
                &Launch::Argv(vec!["true".into()]),
                root.path(),
                &BTreeMap::new(),
                None,
                &BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            fs::read_to_string(binary.with_extension("count"))
                .unwrap()
                .trim(),
            "2"
        );
        assert!(binary.with_extension("removed").is_file());
    }
}
