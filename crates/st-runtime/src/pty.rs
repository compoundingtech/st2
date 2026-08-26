use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

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
        let mut command = self.command();
        command
            .arg("run")
            .arg("-d")
            .arg("--force")
            .args(["--id", id, "--cwd"]);
        command.arg(cwd);
        if let Some(display_name) = display_name {
            command.args(["--name", display_name]);
        } else {
            command.arg("--no-display-name");
        }
        for (key, value) in env {
            command.args(["--env", &format!("{key}={value}")]);
        }
        for (key, value) in tags {
            command.args(["--tag", &format!("{key}={value}")]);
        }
        command.arg("--");
        match launch {
            Launch::Shell(source) => command.args(["sh", "-c", source]),
            Launch::Argv(argv) => command.args(argv),
        };
        let output = command.output()?;
        require_success("spawn PTY", output)?;
        Ok(())
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
