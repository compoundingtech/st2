//! Install the st3 daemon as a Linux systemd user service.

use std::env;
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::{fs, process::Command};

use anyhow::{Context as _, Result};

#[cfg(not(target_os = "linux"))]
use anyhow::bail;

use crate::config::Config;

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "st3.service";
pub const DEFAULT_MEMORY_MAX_MB: u64 = 1024;

#[derive(Clone, Debug)]
pub struct ServiceSpec {
    exe: PathBuf,
    config: Config,
    path: String,
    memory_max_mb: u64,
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        config: Config,
        path: impl Into<String>,
        memory_max_mb: u64,
    ) -> Result<Self> {
        let path = path.into();
        anyhow::ensure!(!path.is_empty(), "the service PATH cannot be empty");
        anyhow::ensure!(
            memory_max_mb > 0,
            "the memory limit must be greater than zero"
        );
        anyhow::ensure!(
            config.state_dir.is_absolute() && config.socket.is_absolute(),
            "the service state directory and socket must be absolute"
        );
        anyhow::ensure!(
            config
                .pty_root
                .as_ref()
                .is_none_or(|root| root.is_absolute()),
            "the service PTY root must be absolute"
        );
        config.validate()?;
        Ok(Self {
            exe: exe.into(),
            config,
            path,
            memory_max_mb,
        })
    }

    fn program_arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            self.exe.display().to_string(),
            "up".into(),
            "--node".into(),
            self.config.node.clone(),
            "--state-dir".into(),
            self.config.state_dir.display().to_string(),
            "--socket".into(),
            self.config.socket.display().to_string(),
        ];
        if let Some(pty_root) = &self.config.pty_root {
            arguments.extend(["--pty-root".into(), pty_root.display().to_string()]);
        }
        if let Some(peer_listen) = &self.config.peer_listen {
            arguments.extend(["--peer-listen".into(), peer_listen.clone()]);
        }
        for peer in &self.config.peers {
            arguments.extend(["--peer".into(), format!("{}={}", peer.name, peer.url)]);
        }
        arguments
    }
}

pub fn install(mut config: Config) -> Result<()> {
    anyhow::ensure!(
        st_runtime::isolation_mode() != st_runtime::Isolation::DegradedDetached,
        "st3 service install needs a working systemd user manager and transient user scopes"
    );
    let exe = env::current_exe().context("resolve the current st3 executable")?;
    let current = env::current_dir().context("resolve the service install directory")?;
    config.state_dir = absolute_from(&current, &config.state_dir);
    config.socket = absolute_from(&current, &config.socket);
    config.pty_root = config
        .pty_root
        .as_ref()
        .map(|root| absolute_from(&current, root));
    let path = service_path(&exe)?;
    let spec = ServiceSpec::new(exe, config, path, DEFAULT_MEMORY_MAX_MB)?;
    install_systemd_user(&spec)?;
    println!("installed");
    Ok(())
}

fn absolute_from(current: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        current.join(path)
    }
}

pub fn status() -> Result<()> {
    status_systemd_user()
}

pub fn uninstall() -> Result<()> {
    uninstall_systemd_user()?;
    println!("uninstalled");
    Ok(())
}

fn service_path(exe: &Path) -> Result<String> {
    let ambient = env::var_os("PATH").context("PATH is not set")?;
    let mut entries = env::split_paths(&ambient).collect::<Vec<_>>();
    if let Some(parent) = exe.parent()
        && !entries.iter().any(|entry| entry == parent)
    {
        entries.insert(0, parent.to_path_buf());
    }
    env::join_paths(entries)
        .context("the service PATH contains an unsupported byte")
        .map(|path| path.to_string_lossy().into_owned())
}

#[cfg(target_os = "linux")]
fn install_systemd_user(spec: &ServiceSpec) -> Result<()> {
    let unit_path = systemd_user_unit_path()?;
    if let Some(parent) = unit_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&unit_path, render_systemd_user_unit(spec))?;
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
    run_command(
        "systemctl",
        &["--user", "status", SERVICE_NAME, "--no-pager"],
    )
}

#[cfg(not(target_os = "linux"))]
fn status_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn uninstall_systemd_user() -> Result<()> {
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", SERVICE_NAME])
        .status();
    let unit_path = systemd_user_unit_path()?;
    if unit_path.exists() {
        fs::remove_file(unit_path)?;
    }
    run_command("systemctl", &["--user", "daemon-reload"])
}

#[cfg(not(target_os = "linux"))]
fn uninstall_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> Result<()> {
    bail!("st3 service is available only with a Linux systemd user manager")
}

#[cfg(target_os = "linux")]
fn run_command(program: &str, arguments: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(arguments)
        .status()
        .with_context(|| format!("run {program} {}", arguments.join(" ")))?;
    anyhow::ensure!(status.success(), "{program} failed with {status}");
    Ok(())
}

#[cfg(target_os = "linux")]
fn systemd_user_unit_path() -> Result<PathBuf> {
    let base = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")));
    Ok(base
        .context("HOME and XDG_CONFIG_HOME are not set")?
        .join("systemd/user")
        .join(SERVICE_NAME))
}

pub fn render_systemd_user_unit(spec: &ServiceSpec) -> String {
    let exec_start = spec
        .program_arguments()
        .iter()
        .map(|argument| systemd_quote_arg(argument))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
Description=st3 claims graph daemon\n\
After=network.target\n\
\n\
[Service]\n\
Type=simple\n\
Environment={}\n\
ExecStart={exec_start}\n\
Restart=on-failure\n\
RestartSec=5s\n\
MemoryMax={}M\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        systemd_quote_arg(&format!("PATH={}", spec.path)),
        spec.memory_max_mb,
    )
}

fn systemd_quote_arg(argument: &str) -> String {
    if !argument.is_empty()
        && argument.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'.' | b'_' | b':' | b'-' | b'+' | b'=')
        })
    {
        return argument.into();
    }
    let mut quoted = String::from("\"");
    for character in argument.chars() {
        match character {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '$' => quoted.push_str("$$"),
            '%' => quoted.push_str("%%"),
            _ => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerConfig;

    #[test]
    fn unit_bakes_the_effective_config_and_limit() -> Result<()> {
        let config = Config {
            node: "node-a".into(),
            state_dir: "/var/lib/st3".into(),
            pty_root: Some("/var/lib/pty".into()),
            socket: "/run/user/1000/st3.sock".into(),
            peer_listen: Some("127.0.0.1:31313".into()),
            peers: vec![PeerConfig {
                name: "node-b".into(),
                url: "http://node-b:31313".into(),
            }],
        };
        let spec = ServiceSpec::new("/usr/bin/st3", config, "/usr/bin", 1024)?;
        let unit = render_systemd_user_unit(&spec);
        assert!(unit.contains("ExecStart=/usr/bin/st3 up --node node-a"));
        assert!(unit.contains("--state-dir /var/lib/st3"));
        assert!(unit.contains("--pty-root /var/lib/pty"));
        assert!(unit.contains("--peer node-b=http://node-b:31313"));
        assert!(unit.contains("MemoryMax=1024M"));
        assert!(unit.contains("Restart=on-failure"));
        Ok(())
    }

    #[test]
    fn unit_quotes_spaces_and_systemd_specifiers() -> Result<()> {
        let config = Config {
            state_dir: "/tmp/st3 state 100%".into(),
            socket: "/tmp/st3 socket".into(),
            ..Config::default()
        };
        let spec = ServiceSpec::new("/opt/st3 tools/st3", config, "/opt/st3 tools", 1024)?;
        let unit = render_systemd_user_unit(&spec);
        assert!(unit.contains("\"/opt/st3 tools/st3\""));
        assert!(unit.contains("\"/tmp/st3 state 100%%\""));
        Ok(())
    }
}
