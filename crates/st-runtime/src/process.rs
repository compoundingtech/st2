use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const SCHEMA: &str = "st.runtime.exec-generation.v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecGeneration {
    pub schema: String,
    pub pid: u32,
    pub created_at_unix_ms: u128,
    pub start_token: u64,
    pub generation_id: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecObservation {
    Running(ExecGeneration),
    Exited(ExecGeneration),
    Indeterminate(String),
}

#[derive(Clone, Debug)]
pub struct ExecRuntime {
    state_dir: PathBuf,
    log_dir: PathBuf,
    owned_children: Arc<Mutex<BTreeMap<u32, Child>>>,
}

impl ExecRuntime {
    pub fn new(state_dir: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            state_dir,
            log_dir,
            owned_children: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn spawn(
        &self,
        id: &str,
        launch: &crate::Launch,
        cwd: &Path,
        env: &BTreeMap<String, String>,
    ) -> Result<ExecGeneration> {
        fs::create_dir_all(&self.state_dir)?;
        fs::create_dir_all(&self.log_dir)?;
        let log_path = self.log_dir.join(format!("{id}.log"));
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&log_path)
            .with_context(|| format!("open exec log {}", log_path.display()))?;

        let mut command = match launch {
            crate::Launch::Shell(source) => {
                let mut command = Command::new("sh");
                command.args(["-c", source]);
                command
            }
            crate::Launch::Argv(argv) => {
                let (program, arguments) = argv
                    .split_first()
                    .context("an argv launch must contain a program")?;
                let mut command = Command::new(program);
                command.args(arguments);
                command
            }
        };
        command
            .current_dir(cwd)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let child = command.spawn().context("spawn detached exec")?;
        let pid = child.id();
        let start_token = process_start_token(pid)?;
        let created_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let generation_id = generation_id(id, pid, start_token, created_at_unix_ms);
        let generation = ExecGeneration {
            schema: SCHEMA.into(),
            pid,
            created_at_unix_ms,
            start_token,
            generation_id,
            exit_code: None,
        };
        self.write_generation(id, &generation)?;
        self.owned_children
            .lock()
            .expect("exec child mutex poisoned")
            .insert(pid, child);
        Ok(generation)
    }

    pub fn observe(&self, id: &str) -> Result<Option<ExecObservation>> {
        let path = self.record_path(id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut generation: ExecGeneration = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => return Ok(Some(ExecObservation::Indeterminate(error.to_string()))),
        };
        if generation.schema != SCHEMA {
            return Ok(Some(ExecObservation::Indeterminate(format!(
                "unsupported generation schema {}",
                generation.schema
            ))));
        }
        let owned_exit = {
            let mut children = self
                .owned_children
                .lock()
                .expect("exec child mutex poisoned");
            let status = children
                .get_mut(&generation.pid)
                .map(Child::try_wait)
                .transpose()?
                .flatten();
            if status.is_some() {
                children.remove(&generation.pid);
            }
            status
        };
        if let Some(status) = owned_exit {
            generation.exit_code = status.code();
            self.write_generation(id, &generation)?;
            return Ok(Some(ExecObservation::Exited(generation)));
        }
        match process_identity(generation.pid) {
            Ok((state, token)) if token == generation.start_token && state != 'Z' => {
                Ok(Some(ExecObservation::Running(generation)))
            }
            Ok((_state, token)) if token == generation.start_token => {
                Ok(Some(ExecObservation::Exited(generation)))
            }
            Ok(_) => Ok(Some(ExecObservation::Exited(generation))),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(Some(ExecObservation::Exited(generation)))
            }
            Err(error) => Ok(Some(ExecObservation::Indeterminate(error.to_string()))),
        }
    }

    pub fn stop(&self, id: &str) -> Result<()> {
        self.stop_if(id, None)
    }

    pub fn stop_if(&self, id: &str, expected_generation: Option<&str>) -> Result<()> {
        self.signal_if(id, expected_generation, libc::SIGTERM)
    }

    pub fn signal_if(
        &self,
        id: &str,
        expected_generation: Option<&str>,
        signal: i32,
    ) -> Result<()> {
        let Some(ExecObservation::Running(generation)) = self.observe(id)? else {
            return Ok(());
        };
        if expected_generation.is_some_and(|expected| expected != generation.generation_id) {
            anyhow::bail!("exec `{id}` changed incarnation before signal {signal}");
        }
        let result = unsafe { libc::kill(-(generation.pid as i32), signal) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error)
                    .with_context(|| format!("signal {signal} to exec process group"));
            }
        }
        Ok(())
    }

    pub fn kill(&self, id: &str) -> Result<()> {
        self.kill_if(id, None)
    }

    pub fn kill_if(&self, id: &str, expected_generation: Option<&str>) -> Result<()> {
        self.signal_if(id, expected_generation, libc::SIGKILL)
    }

    pub fn read_log(&self, id: &str) -> Result<Option<String>> {
        let path = self.log_dir.join(format!("{id}.log"));
        match fs::read_to_string(&path) {
            Ok(log) => Ok(Some(log)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read exec log {}", path.display())),
        }
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        match fs::remove_file(self.record_path(id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn record_path(&self, id: &str) -> PathBuf {
        self.state_dir.join(format!("{id}.json"))
    }

    fn write_generation(&self, id: &str, generation: &ExecGeneration) -> Result<()> {
        let path = self.record_path(id);
        let temporary = path.with_extension(format!("json.tmp.{}", std::process::id()));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, generation)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn process_start_token(pid: u32) -> Result<u64> {
    process_identity(pid).map(|(_, token)| token)
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Result<(char, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat.rfind(')').context("malformed process stat")?;
    let tail = stat.get(end + 2..).context("malformed process stat tail")?;
    let mut fields = tail.split_whitespace();
    let state = fields
        .next()
        .and_then(|value| value.chars().next())
        .context("process stat lacks a state")?;
    let token = fields
        .nth(18)
        .context("process stat lacks a start token")?
        .parse()?;
    Ok((state, token))
}

#[cfg(not(target_os = "linux"))]
pub fn process_start_token(pid: u32) -> Result<u64> {
    process_identity(pid).map(|(_, token)| token)
}

#[cfg(not(target_os = "linux"))]
fn process_identity(pid: u32) -> Result<(char, u64)> {
    let result = unsafe { libc::kill(pid as i32, 0) };
    if result == 0 {
        Ok(('R', pid as u64))
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn generation_id(id: &str, pid: u32, start_token: u64, created_at_unix_ms: u128) -> String {
    let mut hash = Sha256::new();
    hash.update(id.as_bytes());
    hash.update(pid.to_be_bytes());
    hash.update(start_token.to_be_bytes());
    hash.update(created_at_unix_ms.to_be_bytes());
    format!("{:x}", hash.finalize())
}
