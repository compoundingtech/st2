use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node: String,
    pub state_dir: PathBuf,
    pub pty_root: Option<PathBuf>,
    pub socket: PathBuf,
    pub peer_listen: Option<String>,
    pub peers: Vec<PeerConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let state_dir = xdg_dir("XDG_STATE_HOME", ".local/state").join("st3");
        let socket = env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| state_dir.join("run"))
            .join("st3.sock");
        Self {
            node: host_name(),
            state_dir,
            pty_root: None,
            socket,
            peer_listen: None,
            peers: Vec::new(),
        }
    }
}

impl Config {
    pub fn default_path() -> PathBuf {
        xdg_dir("XDG_CONFIG_HOME", ".config")
            .join("st3")
            .join("config.toml")
    }

    pub fn load(path: Option<&Path>) -> Result<Self> {
        let selected = path
            .map(Path::to_path_buf)
            .unwrap_or_else(Self::default_path);
        let bytes = match fs::read_to_string(&selected) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && path.is_none() => {
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read st3 config {}", selected.display()));
            }
        };
        let mut config: Self = toml::from_str(&bytes)
            .with_context(|| format!("parse st3 config {}", selected.display()))?;
        let defaults = Self::default();
        if config.node.is_empty() {
            config.node = defaults.node;
        }
        if config.state_dir.as_os_str().is_empty() {
            config.state_dir = defaults.state_dir;
        }
        if config.socket.as_os_str().is_empty() {
            config.socket = defaults.socket;
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.node.trim().is_empty(), "the st3 node label is empty");
        anyhow::ensure!(
            self.peers
                .iter()
                .all(|peer| !peer.name.is_empty() && !peer.url.is_empty()),
            "each peer needs a name and URL"
        );
        let mut names = std::collections::HashSet::new();
        for peer in &self.peers {
            anyhow::ensure!(names.insert(&peer.name), "peer '{}' repeats", peer.name);
            anyhow::ensure!(
                peer.name != self.node,
                "peer '{}' uses the local node label",
                peer.name
            );
            anyhow::ensure!(
                peer.url.starts_with("http://"),
                "peer '{}' must use plain http:// in st3 v1",
                peer.name
            );
        }
        Ok(())
    }
}

fn xdg_dir(variable: &str, home_suffix: &str) -> PathBuf {
    env::var_os(variable).map(PathBuf::from).unwrap_or_else(|| {
        env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(home_suffix)
    })
}

fn host_name() -> String {
    env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| fs::read_to_string("/etc/hostname").ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_a_local_only_daemon() {
        let config = Config::default();
        assert!(config.peer_listen.is_none());
        assert!(config.peers.is_empty());
        assert!(config.socket.ends_with("st3.sock"));
    }
}
