//! `st2 service install|status|uninstall` — install `st2 up` as a **systemd-user** unit so the
//! supervisor comes back on boot and on crash.
//!
//! **Linux-only, by design (the maintainer).** The Mac stays MANUAL — the maintainer runs `st2 up` themselves there
//! (TCC: a launchd-owned process can't inherit his GUI/keychain trust), so we deliberately do NOT
//! ship a launchd path like fabric does. On macOS (or anything non-systemd) this bails loud.
//!
//! A service restart is safe because st2 spawns each task in its own transient scope
//! (`systemd-run --user --scope`, see `isolate.rs`) —
//! a SIBLING of this unit, not a child. So stopping/restarting `st2.service` reaps only the
//! supervisor loop; the agents survive in their scopes and a fresh supervisor ADOPTS them
//! (tests/nomad_survival.rs + tests/transport_isolation.rs). That is the whole reason this unit is
//! now a safe thing to install.

use std::{
    env,
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::{fs, process::Command};

use anyhow::{Context, Result, bail};

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "st2.service";
/// 1 GiB is generous headroom for an I/O-light sync supervisor (folder-watch + sleep + shell-outs);
/// the agents themselves live in sibling scopes and are NOT bounded by this.
pub const DEFAULT_MEMORY_MAX_MB: u64 = 1024;

/// Everything the unit's `ExecStart` needs, resolved from the invoking environment.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// Absolute path to the `st2` binary (`env::current_exe()` at install time).
    exe: PathBuf,
    /// Absolute catalog (or spec-file) path handed to `st2 up`.
    catalog: PathBuf,
    /// Baked `--host`; `None` lets `st2 up` auto-detect the hostname (matching a manual `st2 up`).
    host: Option<String>,
    /// Explicit command search path inherited by st2 and every task it launches. A systemd user
    /// manager normally has only system directories on PATH, while pty/Codex commonly live under
    /// user-local or version-manager directories.
    path: String,
    /// Optional machine-local pty registry. This is deliberately independent of the synced catalog:
    /// an explicit adoption can use an existing registry without syncing pid/socket state.
    pty_root: Option<PathBuf>,
    memory_max_mb: u64,
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        catalog: impl Into<PathBuf>,
        host: Option<String>,
        path: impl Into<String>,
        pty_root: Option<PathBuf>,
        memory_max_mb: u64,
    ) -> Result<Self> {
        if memory_max_mb == 0 {
            bail!("--memory-max-mb must be greater than zero");
        }
        let path = path.into();
        if path.is_empty() {
            bail!("service PATH cannot be empty");
        }
        if pty_root.as_ref().is_some_and(|root| !root.is_absolute()) {
            bail!("--pty-root must be an absolute path");
        }
        Ok(Self {
            exe: exe.into(),
            catalog: catalog.into(),
            host,
            path,
            pty_root,
            memory_max_mb,
        })
    }

    /// The `ExecStart` argv: `<st2> up --catalog <catalog> [--host <h>]`.
    fn program_arguments(&self) -> Vec<String> {
        let mut args = vec![
            self.exe.display().to_string(),
            "up".to_string(),
            "--catalog".to_string(),
            self.catalog.display().to_string(),
        ];
        if let Some(host) = &self.host {
            args.push("--host".to_string());
            args.push(host.clone());
        }
        args
    }
}

/// `st2 service install [--catalog <catalog>] [--host H] [--pty-root PATH]
/// [--memory-max-mb N]`.
pub fn install(
    catalog: &Path,
    host: Option<String>,
    pty_root: Option<PathBuf>,
    memory_max_mb: u64,
) -> Result<()> {
    let exe = env::current_exe().context("failed to resolve the current st2 executable")?;
    let path = service_path(&exe)?;
    // A systemd unit runs from no shell and no cwd — the catalog MUST be absolute, and it must exist
    // now (you install the service against an existing catalog, not a future one).
    let catalog = catalog.canonicalize().with_context(|| {
        format!(
            "catalog {} does not exist — create it before installing the service",
            catalog.display()
        )
    })?;
    let pty_root = pty_root
        .map(|root| {
            root.canonicalize()
                .with_context(|| format!("pty root {} does not exist", root.display()))
        })
        .transpose()?;
    let spec = ServiceSpec::new(exe, &catalog, host, path, pty_root, memory_max_mb)?;

    install_systemd_user(&spec)?;

    println!("installed");
    println!("catalog\t{}", catalog.display());
    match &spec.host {
        Some(h) => println!("host\t{h}"),
        None => println!("host\t(auto-detected at runtime)"),
    }
    match &spec.pty_root {
        Some(root) => println!("pty-root\t{}", root.display()),
        None => println!("pty-root\tresolved from host/catalog config at runtime"),
    }
    println!("memory-max-mb\t{memory_max_mb}");
    Ok(())
}

/// Persist the invoking PATH in the unit, prepending the installed st2 directory when necessary.
/// This makes the unit independent of systemd's sparse manager environment while retaining the
/// exact version-manager/user-local directories the operator verified at install time.
fn service_path(exe: &Path) -> Result<String> {
    let ambient = env::var_os("PATH").context("PATH is not set")?;
    let mut entries: Vec<PathBuf> = env::split_paths(&ambient).collect();
    if let Some(parent) = exe.parent()
        && !entries.iter().any(|entry| entry == parent)
    {
        entries.insert(0, parent.to_path_buf());
    }
    env::join_paths(entries)
        .context("service PATH contains an unsupported byte")
        .map(|path| path.to_string_lossy().into_owned())
}

/// `st2 service status` — show the unit's systemd status.
pub fn status() -> Result<()> {
    status_systemd_user()
}

/// `st2 service uninstall` — stop, disable, and remove the unit. Idempotent.
pub fn uninstall() -> Result<()> {
    uninstall_systemd_user()?;
    println!("uninstalled");
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_systemd_user(spec: &ServiceSpec) -> Result<()> {
    let unit_path = systemd_user_unit_path()?;
    if let Some(parent) = unit_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&unit_path, render_systemd_user_unit(spec))
        .with_context(|| format!("failed to write {}", unit_path.display()))?;

    // daemon-reload → enable (boot) → restart (start now, idempotent over a running instance).
    run_command("systemctl", &["--user", "daemon-reload"])?;
    run_command("systemctl", &["--user", "enable", SERVICE_NAME])?;
    run_command("systemctl", &["--user", "restart", SERVICE_NAME])?;
    println!("unit\t{}", unit_path.display());
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn install_systemd_user(_spec: &ServiceSpec) -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn status_systemd_user() -> Result<()> {
    run_command("systemctl", &["--user", "status", SERVICE_NAME, "--no-pager"])
}

#[cfg(not(target_os = "linux"))]
fn status_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn uninstall_systemd_user() -> Result<()> {
    // Best-effort stop+disable (ignore "not loaded"), then remove the unit and reload.
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", SERVICE_NAME])
        .status();
    let unit_path = systemd_user_unit_path()?;
    if unit_path.exists() {
        fs::remove_file(&unit_path)
            .with_context(|| format!("failed to remove {}", unit_path.display()))?;
    }
    run_command("systemctl", &["--user", "daemon-reload"])?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn uninstall_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> Result<()> {
    bail!(
        "st2 service is Linux/systemd-user only (headless hosts like hetz). On macOS, run \
         `st2 up --catalog <catalog>` yourself — the Mac stays manual for TCC reasons (a launchd-owned \
         process can't inherit your GUI/keychain trust)."
    )
}

#[cfg(target_os = "linux")]
fn run_command(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {program} {}", args.join(" ")))?;
    if !status.success() {
        bail!("{program} {} failed with status {status}", args.join(" "));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME").map(PathBuf::from).context("HOME is not set")
}

#[cfg(target_os = "linux")]
fn systemd_user_unit_path() -> Result<PathBuf> {
    let base = match env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => home_dir()?.join(".config"),
    };
    Ok(base.join("systemd/user").join(SERVICE_NAME))
}

/// Render the systemd-user unit. Pure (no I/O) so it is unit-testable on any OS.
pub fn render_systemd_user_unit(spec: &ServiceSpec) -> String {
    let exec_start = spec
        .program_arguments()
        .iter()
        .map(|arg| systemd_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
Description=st2 supervisor (st2 up)\n\
After=network.target\n\
\n\
[Service]\n\
Type=simple\n\
Environment={}\n\
{}\
ExecStart={exec_start}\n\
Restart=on-failure\n\
RestartSec=5s\n\
MemoryMax={}M\n\
WorkingDirectory={}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        systemd_quote_arg(&format!("PATH={}", spec.path)),
        spec.pty_root
            .as_ref()
            .map(|root| format!(
                "Environment={}\n",
                systemd_quote_arg(&format!("PTY_ROOT={}", root.display()))
            ))
            .unwrap_or_default(),
        spec.memory_max_mb,
        systemd_quote_arg(&spec.catalog.display().to_string())
    )
}

/// Quote an argument for a systemd `ExecStart` line: bare if it is all "safe" chars, else
/// double-quoted with systemd's escapes (`\`, `"`, `$`→`$$`, `%`→`%%`). Mirrors fabric.
fn systemd_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && arg.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'.' | b'_' | b':' | b'-' | b'+' | b'=')
        })
    {
        return arg.to_string();
    }

    let mut quoted = String::from("\"");
    for ch in arg.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '$' => quoted.push_str("$$"),
            '%' => quoted.push_str("%%"),
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_runs_st2_up_with_restart_and_memory_limit() -> Result<()> {
        let spec = ServiceSpec::new(
            "/home/user/.cargo/bin/st2",
            "/home/user/catalog",
            None,
            "/home/user/.cargo/bin:/home/user/.local/bin:/usr/bin",
            None,
            DEFAULT_MEMORY_MAX_MB,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(
            unit.contains("ExecStart=/home/user/.cargo/bin/st2 up --catalog /home/user/catalog")
        );
        // No --host baked when unset → st2 up auto-detects, same as a manual run.
        assert!(!unit.contains("--host"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("RestartSec=5s"));
        assert!(unit.contains("MemoryMax=1024M"));
        assert!(unit.contains("WorkingDirectory=/home/user/catalog"));
        assert!(unit.contains(
            "Environment=PATH=/home/user/.cargo/bin:/home/user/.local/bin:/usr/bin"
        ));
        assert!(!unit.contains("Environment=PTY_ROOT="));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(unit.contains("Description=st2 supervisor (st2 up)"));
        Ok(())
    }

    #[test]
    fn unit_bakes_host_when_provided() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/st2",
            "/srv/catalog",
            Some("hetz".to_string()),
            "/usr/local/bin:/usr/bin",
            Some(PathBuf::from("/srv/legacy-pty")),
            512,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(
            unit.contains("ExecStart=/usr/local/bin/st2 up --catalog /srv/catalog --host hetz")
        );
        assert!(unit.contains("MemoryMax=512M"));
        assert!(unit.contains("Environment=PTY_ROOT=/srv/legacy-pty"));
        Ok(())
    }

    #[test]
    fn unit_quotes_paths_with_spaces_and_escapes_specifiers() -> Result<()> {
        let spec = ServiceSpec::new(
            "/opt/st2 tools/st2",
            "/srv/cat 100%",
            None,
            "/opt/st2 tools:/usr/bin",
            Some(PathBuf::from("/srv/pty 100%")),
            256,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(
            unit.contains("ExecStart=\"/opt/st2 tools/st2\" up --catalog \"/srv/cat 100%%\"")
        );
        assert!(unit.contains("WorkingDirectory=\"/srv/cat 100%%\""));
        assert!(unit.contains("Environment=\"PATH=/opt/st2 tools:/usr/bin\""));
        assert!(unit.contains("Environment=\"PTY_ROOT=/srv/pty 100%%\""));
        Ok(())
    }

    #[test]
    fn zero_memory_max_is_rejected() {
        let err = ServiceSpec::new("/bin/st2", "/cat", None, "/bin", None, 0).unwrap_err();
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn empty_path_and_relative_pty_root_are_rejected() {
        let err = ServiceSpec::new("/bin/st2", "/cat", None, "", None, 1).unwrap_err();
        assert!(err.to_string().contains("PATH cannot be empty"));

        let err = ServiceSpec::new(
            "/bin/st2",
            "/cat",
            None,
            "/bin",
            Some(PathBuf::from("relative")),
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("absolute"));
    }
}
