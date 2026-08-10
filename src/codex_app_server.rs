//! Controlled Codex app-server launch and persistent thread ownership.
//!
//! Native delivery cannot infer a thread from cwd, process, PTY, or `thread/list`. This module
//! starts a dedicated provider daemon, initializes an observer connection before the interactive
//! client starts, and binds a typed start or successful-resume event to the exact wrapper process
//! incarnation that owns the PTY launch. Message watching and delivery are deliberately later
//! layers; this module establishes only the topology and identity boundary they consume.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::io::AsRawFd as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tungstenite::{Message, WebSocket};

pub const SUPPORTED_CODEX_CLI_VERSION: &str = "codex-cli 0.145.0";
const RUNTIME_SCHEMA: &str = "st2.codex-runtime.v1";
const BINDING_SCHEMA: &str = "st2.codex-thread-binding.v1";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_POLL: Duration = Duration::from_millis(100);
const SOCKET_PATH_BUDGET: usize = 96;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexRuntime {
    schema: String,
    agent: String,
    runtime_id: String,
    incarnation: String,
}

impl CodexRuntime {
    fn fresh(agent: String, runtime_id: String) -> Result<Self> {
        Ok(Self {
            schema: RUNTIME_SCHEMA.to_string(),
            agent,
            runtime_id,
            incarnation: random_token()?,
        })
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexThreadBinding {
    schema: String,
    agent: String,
    runtime_id: String,
    runtime_incarnation: String,
    thread_id: String,
}

impl CodexThreadBinding {
    fn new(runtime: &CodexRuntime, thread_id: String) -> Self {
        Self {
            schema: BINDING_SCHEMA.to_string(),
            agent: runtime.agent.clone(),
            runtime_id: runtime.runtime_id.clone(),
            runtime_incarnation: runtime.incarnation.clone(),
            thread_id,
        }
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn runtime_incarnation(&self) -> &str {
        &self.runtime_incarnation
    }
}

/// Run one authored Codex argv behind a dedicated app server and initialized control connection.
pub fn run_controlled(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    codex_argv: Vec<String>,
) -> Result<()> {
    anyhow::ensure!(
        !codex_argv.is_empty(),
        "Codex controlled launch argv is empty"
    );
    ensure_supported_version(&codex_argv[0])?;

    let state_dir = state_dir(catalog_root, &identity);
    secure_dir(&state_dir)?;
    let _owner_lock = acquire_owner_lock(&state_dir)?;
    let binding_path = state_dir.join("binding.json");
    let resume_thread = load_resume_thread(&binding_path, &identity, &runtime_id)?;

    let socket_path = socket_path(catalog_root, &identity)?;
    let socket_dir = socket_path
        .parent()
        .context("Codex app-server socket has no parent")?;
    secure_dir(socket_dir)?;
    match fs::symlink_metadata(&socket_path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Codex app-server path already exists and is not a socket: {}",
                socket_path.display()
            );
            match UnixStream::connect(&socket_path) {
                Ok(_) => anyhow::bail!(
                    "Codex app-server socket {} is already live; refusing a second control owner",
                    socket_path.display()
                ),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    fs::remove_file(&socket_path).with_context(|| {
                        format!("removing stale Codex socket {}", socket_path.display())
                    })?;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "checking existing Codex socket {} before launch",
                            socket_path.display()
                        )
                    });
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("checking Codex socket path {}", socket_path.display()));
        }
    }

    // Publish a new incarnation only after this process holds the owner lock and has proved that no
    // older daemon is live. A rejected second owner must not invalidate the first owner's binding.
    let runtime = CodexRuntime::fresh(identity, runtime_id)?;
    atomic_json(&state_dir.join("runtime.json"), &runtime)?;

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(state_dir.join("app-server.log"))?;
    let endpoint = format!("unix://{}", socket_path.display());
    let mut server = Command::new(&codex_argv[0])
        .args(["app-server", "--listen", &endpoint])
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .with_context(|| format!("starting {} app-server", codex_argv[0]))?;

    let result = run_connected(
        &mut server,
        &socket_path,
        &endpoint,
        &state_dir,
        &runtime,
        &codex_argv,
        resume_thread.as_deref(),
    );
    terminate_child(&mut server);
    let _ = fs::remove_file(&socket_path);
    result
}

fn run_connected(
    server: &mut Child,
    socket_path: &Path,
    endpoint: &str,
    state_dir: &Path,
    runtime: &CodexRuntime,
    codex_argv: &[String],
    resume_thread: Option<&str>,
) -> Result<()> {
    let control = connect_control(server, socket_path, STARTUP_TIMEOUT)?;
    let shutdown = control.try_clone()?;
    let websocket = initialize_control(control)?;
    let (events_tx, events_rx) = mpsc::channel();
    let binding_path = state_dir.join("binding.json");
    let runtime_for_reader = runtime.clone();
    let expected_resume = resume_thread.map(str::to_owned);
    let event_thread = thread::spawn(move || {
        pump_control(
            websocket,
            &binding_path,
            &runtime_for_reader,
            expected_resume.as_deref(),
            events_tx,
        )
    });

    // The initialized observer is already reading before this child can issue thread/start or
    // thread/resume. Insert the remote endpoint as a global Codex option and preserve every authored
    // argument after the provider executable.
    let mut tui_command = Command::new(&codex_argv[0]);
    tui_command.args(controlled_tui_args(
        endpoint,
        &codex_argv[1..],
        resume_thread,
    )?);
    let mut tui = tui_command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("starting controlled {} TUI", codex_argv[0]))?;

    let result = wait_for_binding(&mut tui, &events_rx, STARTUP_TIMEOUT)
        .and_then(|_| monitor_bound_tui(&mut tui, &events_rx));
    if result.is_err() {
        terminate_child(&mut tui);
    }
    let _ = shutdown.shutdown(Shutdown::Both);
    let _ = event_thread.join();
    result
}

fn controlled_tui_args(
    endpoint: &str,
    authored_args: &[String],
    resume_thread: Option<&str>,
) -> Result<Vec<String>> {
    let mut args = vec!["--remote".to_string(), endpoint.to_string()];
    let Some(thread_id) = resume_thread else {
        args.extend_from_slice(authored_args);
        return Ok(args);
    };
    let Some(insertion) = resume_insertion_index(authored_args)? else {
        args.extend_from_slice(authored_args);
        return Ok(args);
    };
    args.push("resume".to_string());
    // Codex models these flags on the `resume` command as well as the root command. Keep them
    // before SESSION_ID so clap does not treat a following flag as the optional prompt.
    args.extend_from_slice(&authored_args[..insertion]);
    args.push(thread_id.to_string());
    args.extend_from_slice(&authored_args[insertion..]);
    Ok(args)
}

/// Find where a pinned Codex 0.145.0 interactive argv begins its prompt or subcommand.
///
/// Automatic resume must insert `resume <thread>` after global options and before the authored
/// prompt. Unknown options fail closed because guessing can turn an option value into a prompt or a
/// prompt into a session selector. `--image` is variadic, so automatic resume requires an explicit
/// `--` boundary when that option is present.
fn resume_insertion_index(authored_args: &[String]) -> Result<Option<usize>> {
    let delimiter = authored_args.iter().position(|arg| arg == "--");
    let mut index = 0;
    while index < authored_args.len() {
        let argument = authored_args[index].as_str();
        if argument == "--" {
            return Ok(Some(index));
        }
        if !argument.starts_with('-') || argument == "-" {
            return if matches!(argument, "resume" | "fork") {
                Ok(None)
            } else {
                Ok(Some(index))
            };
        }

        if matches!(
            argument,
            "--strict-config"
                | "--oss"
                | "--dangerously-bypass-approvals-and-sandbox"
                | "--dangerously-bypass-hook-trust"
                | "--search"
                | "--no-alt-screen"
        ) {
            index += 1;
            continue;
        }
        anyhow::ensure!(
            !matches!(argument, "-h" | "--help" | "-V" | "--version"),
            "cannot automatically resume a Codex help or version invocation"
        );

        let exact_value_option = matches!(
            argument,
            "-c" | "--config"
                | "--enable"
                | "--disable"
                | "--remote-auth-token-env"
                | "-m"
                | "--model"
                | "--local-provider"
                | "-p"
                | "--profile"
                | "-s"
                | "--sandbox"
                | "-C"
                | "--cd"
                | "--add-dir"
                | "-a"
                | "--ask-for-approval"
        );
        if exact_value_option {
            anyhow::ensure!(
                index + 1 < authored_args.len(),
                "Codex option '{argument}' has no value"
            );
            index += 2;
            continue;
        }
        if matches!(argument, "-i" | "--image")
            || argument.starts_with("-i=")
            || argument.starts_with("--image=")
        {
            let boundary = delimiter.context(
                "automatic Codex resume with variadic --image requires an explicit `--` prompt boundary",
            )?;
            return Ok(Some(boundary));
        }

        let long_value = [
            "--config=",
            "--enable=",
            "--disable=",
            "--remote-auth-token-env=",
            "--model=",
            "--local-provider=",
            "--profile=",
            "--sandbox=",
            "--cd=",
            "--add-dir=",
            "--ask-for-approval=",
        ]
        .iter()
        .any(|prefix| argument.starts_with(prefix));
        let short_value = ["-c", "-m", "-p", "-s", "-C", "-a"]
            .iter()
            .any(|prefix| argument.starts_with(prefix) && argument.len() > prefix.len());
        anyhow::ensure!(
            long_value || short_value,
            "cannot automatically resume through unknown Codex option '{argument}'"
        );
        index += 1;
    }
    Ok(Some(authored_args.len()))
}

fn connect_control(
    server: &mut Child,
    socket_path: &Path,
    timeout: Duration,
) -> Result<UnixStream> {
    let deadline = Instant::now() + timeout;
    loop {
        match UnixStream::connect(socket_path) {
            Ok(stream) => return Ok(stream),
            Err(error) if Instant::now() < deadline => {
                if let Some(status) = server.try_wait()? {
                    anyhow::bail!("Codex app-server exited before control connected: {status}");
                }
                if error.kind() != std::io::ErrorKind::NotFound
                    && error.kind() != std::io::ErrorKind::ConnectionRefused
                {
                    return Err(error).with_context(|| {
                        format!("connecting Codex control socket {}", socket_path.display())
                    });
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Codex control socket {} was not ready within {}s",
                        socket_path.display(),
                        timeout.as_secs()
                    )
                });
            }
        }
    }
}

fn initialize_control(stream: UnixStream) -> Result<WebSocket<UnixStream>> {
    stream.set_read_timeout(Some(STARTUP_TIMEOUT))?;
    let (mut websocket, response) = tungstenite::client("ws://localhost/", stream)
        .map_err(|error| anyhow::anyhow!("Codex WebSocket handshake failed: {error}"))?;
    anyhow::ensure!(
        response.status().as_u16() == 101,
        "Codex WebSocket handshake returned {}",
        response.status()
    );
    write_json_message(
        &mut websocket,
        &json!({
            "method": "initialize",
            "id": 0,
            "params": {
                "clientInfo": {
                    "name": "st2",
                    "title": "st2",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": { "experimentalApi": true }
            }
        }),
    )?;

    loop {
        let message = read_json_message(&mut websocket)?
            .context("Codex app-server closed the control connection during initialize")?;
        if message.get("id") != Some(&Value::from(0)) {
            continue;
        }
        if let Some(error) = message.get("error") {
            anyhow::bail!("Codex app-server rejected initialize: {error}");
        }
        anyhow::ensure!(
            message.get("result").is_some(),
            "Codex app-server initialize response has no result"
        );
        break;
    }
    write_json_message(
        &mut websocket,
        &json!({ "method": "initialized", "params": {} }),
    )?;
    websocket.get_ref().set_read_timeout(None)?;
    Ok(websocket)
}

#[derive(Debug)]
enum ControlEvent {
    Bound,
    Closed,
    Failed(String),
}

fn pump_control(
    mut websocket: WebSocket<UnixStream>,
    binding_path: &Path,
    runtime: &CodexRuntime,
    expected_resume: Option<&str>,
    events: Sender<ControlEvent>,
) {
    let result = (|| -> Result<()> {
        let mut bound_thread: Option<String> = None;
        loop {
            let Some(message) = read_json_message(&mut websocket)? else {
                let _ = events.send(ControlEvent::Closed);
                return Ok(());
            };
            let thread_id = match message.get("method").and_then(Value::as_str) {
                Some("thread/started") => message
                    .pointer("/params/thread/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .context("thread/started has no non-empty params.thread.id")?,
                Some("thread/status/changed") if expected_resume.is_some() => {
                    let thread_id = message
                        .pointer("/params/threadId")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .context("thread/status/changed has no non-empty params.threadId")?;
                    let status = message
                        .pointer("/params/status/type")
                        .and_then(Value::as_str)
                        .context("thread/status/changed has no params.status.type")?;
                    if Some(thread_id) != expected_resume || !matches!(status, "idle" | "active") {
                        continue;
                    }
                    thread_id
                }
                _ => continue,
            };
            match bound_thread.as_deref() {
                None => {
                    atomic_json(
                        binding_path,
                        &CodexThreadBinding::new(runtime, thread_id.to_string()),
                    )?;
                    bound_thread = Some(thread_id.to_string());
                    let _ = events.send(ControlEvent::Bound);
                }
                Some(bound) if bound == thread_id => {}
                // A dedicated daemon can emit secondary thread starts for review/fork flows. The
                // first TUI-owned thread remains the binding; never silently rebind it.
                Some(_) => {}
            }
        }
    })();
    if let Err(error) = result {
        let _ = events.send(ControlEvent::Failed(format!("{error:#}")));
    }
}

fn wait_for_binding(
    tui: &mut Child,
    events: &Receiver<ControlEvent>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = tui.try_wait()? {
            anyhow::bail!("controlled Codex TUI exited before thread binding: {status}");
        }
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(CONTROL_POLL);
        if wait.is_zero() {
            anyhow::bail!(
                "controlled Codex TUI did not establish typed thread ownership within {}s",
                timeout.as_secs()
            );
        }
        match events.recv_timeout(wait) {
            Ok(ControlEvent::Bound) => return Ok(()),
            Ok(ControlEvent::Closed) => {
                anyhow::bail!("Codex control connection closed before thread binding")
            }
            Ok(ControlEvent::Failed(error)) => {
                anyhow::bail!("Codex control failed before thread binding: {error}")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("Codex control observer ended before thread binding")
            }
        }
    }
}

fn monitor_bound_tui(tui: &mut Child, events: &Receiver<ControlEvent>) -> Result<()> {
    loop {
        if let Some(status) = tui.try_wait()? {
            return completed_tui(status);
        }
        match events.recv_timeout(CONTROL_POLL) {
            Ok(ControlEvent::Bound) => {}
            Ok(ControlEvent::Closed) => {
                anyhow::bail!("Codex control connection closed while the TUI was live")
            }
            Ok(ControlEvent::Failed(error)) => {
                anyhow::bail!("Codex control failed while the TUI was live: {error}")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("Codex control observer ended while the TUI was live")
            }
        }
    }
}

fn completed_tui(status: ExitStatus) -> Result<()> {
    anyhow::ensure!(
        status.success(),
        "controlled Codex TUI exited with {status}"
    );
    Ok(())
}

fn ensure_supported_version(codex: &str) -> Result<()> {
    let output = Command::new(codex)
        .arg("--version")
        .output()
        .with_context(|| format!("reading Codex version from {codex}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{codex} --version failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let actual = String::from_utf8(output.stdout)
        .context("Codex version output is not UTF-8")?
        .trim()
        .to_string();
    anyhow::ensure!(
        actual == SUPPORTED_CODEX_CLI_VERSION,
        "unsupported Codex app-server protocol version '{actual}' (expected '{SUPPORTED_CODEX_CLI_VERSION}')"
    );
    Ok(())
}

pub fn state_dir(catalog_root: &Path, identity: &str) -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    state_dir_in(&base, catalog_root, identity)
}

fn state_dir_in(base: &Path, catalog_root: &Path, identity: &str) -> PathBuf {
    base.join("st2")
        .join("codex")
        .join(runtime_key(catalog_root, identity))
}

fn socket_path(catalog_root: &Path, identity: &str) -> Result<PathBuf> {
    let key = runtime_key(catalog_root, identity);
    let preferred = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .map(|base| base.join("st2-codex").join(format!("{key}.sock")));
    if let Some(path) = preferred
        && path.as_os_str().as_bytes().len() <= SOCKET_PATH_BUDGET
    {
        return Ok(path);
    }
    let path = PathBuf::from("/tmp")
        .join(format!("st2-{}", unsafe { libc::geteuid() }))
        .join("codex")
        .join(format!("{key}.sock"));
    anyhow::ensure!(
        path.as_os_str().as_bytes().len() <= SOCKET_PATH_BUDGET,
        "Codex app-server socket path is too long: {}",
        path.display()
    );
    Ok(path)
}

fn runtime_key(catalog_root: &Path, identity: &str) -> String {
    let mut hash = Sha256::new();
    for value in [catalog_root.as_os_str().as_bytes(), identity.as_bytes()] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    let digest = format!("{:x}", hash.finalize());
    digest[..24].to_string()
}

fn secure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn acquire_owner_lock(state_dir: &Path) -> Result<File> {
    let path = state_dir.join("owner.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&path)
        .with_context(|| format!("opening Codex runtime owner lock {}", path.display()))?;
    // SAFETY: `file` owns this descriptor until the returned guard is dropped. `flock` does not
    // access Rust memory, and closing the descriptor releases the process-scoped lock after crash.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("Codex runtime already has an owner at {}", path.display()));
    }
    Ok(file)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state file has no parent")?;
    secure_dir(parent)?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        random_token()?
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

pub fn load_current_binding(
    path: &Path,
    runtime: &CodexRuntime,
) -> Result<Option<CodexThreadBinding>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let binding: CodexThreadBinding = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        binding.schema == BINDING_SCHEMA,
        "unsupported Codex binding schema"
    );
    anyhow::ensure!(
        binding.agent == runtime.agent
            && binding.runtime_id == runtime.runtime_id
            && binding.runtime_incarnation == runtime.incarnation,
        "Codex thread binding belongs to a different runtime incarnation"
    );
    Ok(Some(binding))
}

fn load_resume_thread(path: &Path, agent: &str, runtime_id: &str) -> Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let binding: CodexThreadBinding = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        binding.schema == BINDING_SCHEMA,
        "unsupported Codex binding schema"
    );
    anyhow::ensure!(
        binding.agent == agent && binding.runtime_id == runtime_id,
        "Codex resume binding belongs to a different agent runtime"
    );
    anyhow::ensure!(
        !binding.thread_id.is_empty(),
        "Codex resume binding has an empty thread id"
    );
    Ok(Some(binding.thread_id))
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_json_message(websocket: &mut WebSocket<UnixStream>, value: &Value) -> Result<()> {
    websocket.send(Message::Text(value.to_string().into()))?;
    Ok(())
}

fn read_json_message(websocket: &mut WebSocket<UnixStream>) -> Result<Option<Value>> {
    loop {
        let message = match websocket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        match message {
            Message::Text(text) => {
                let value = serde_json::from_str(&text)
                    .context("decoding Codex app-server WebSocket JSON")?;
                return Ok(Some(value));
            }
            Message::Close(_) => return Ok(None),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Binary(_) | Message::Frame(_) => {
                anyhow::bail!("Codex app-server sent a non-text WebSocket message")
            }
        }
    }
}

fn terminate_child(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn control_initializes_before_recording_the_first_thread_only() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            let initialize = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialize["method"], "initialize");
            assert_eq!(initialize["params"]["clientInfo"]["name"], "st2");
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            let initialized = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialized["method"], "initialized");
            write_json_message(
                &mut websocket,
                &json!({ "method": "thread/started", "params": { "thread": { "id": "thread-main" } } }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({ "method": "thread/started", "params": { "thread": { "id": "thread-review" } } }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream).unwrap();
        let state = tmp.path().join("state");
        let binding_path = state.join("binding.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let pump = thread::spawn(move || {
            pump_control(websocket, &binding_for_pump, &runtime_for_pump, None, tx)
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let binding = load_current_binding(&binding_path, &runtime)
            .unwrap()
            .unwrap();
        assert_eq!(binding.thread_id(), "thread-main");
    }

    #[test]
    fn a_successfully_loaded_expected_resume_is_bound_to_the_new_incarnation() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            let initialize = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialize["method"], "initialize");
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            let initialized = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialized["method"], "initialized");
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-unrelated",
                        "status": { "type": "active", "activeFlags": [] }
                    }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-prior",
                        "status": { "type": "idle" }
                    }
                }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream).unwrap();
        let binding_path = tmp.path().join("state/binding.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &runtime_for_pump,
                Some("thread-prior"),
                tx,
            )
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let binding = load_current_binding(&binding_path, &runtime)
            .unwrap()
            .unwrap();
        assert_eq!(binding.thread_id(), "thread-prior");
    }

    #[test]
    fn a_binding_from_another_runtime_incarnation_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("binding.json");
        let prior = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let current = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        atomic_json(
            &path,
            &CodexThreadBinding::new(&prior, "thread-prior".into()),
        )
        .unwrap();
        assert_eq!(
            load_resume_thread(&path, "h.worker", "h.worker").unwrap(),
            Some("thread-prior".into()),
            "a validated prior binding may select resume but must not become current ownership"
        );
        let error = load_current_binding(&path, &current).unwrap_err();
        assert!(error.to_string().contains("different runtime incarnation"));
    }

    #[test]
    fn controlled_tui_resumes_a_prior_binding_without_overriding_authored_selection() {
        let authored = vec!["--model".into(), "gpt-test".into(), "boot".into()];
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &authored, None).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "--model",
                "gpt-test",
                "boot"
            ]
        );
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &authored, Some("thread-prior")).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "--model",
                "gpt-test",
                "thread-prior",
                "boot"
            ]
        );
        assert_eq!(
            controlled_tui_args(
                "unix:///server.sock",
                &["resume".into(), "thread-explicit".into()],
                Some("thread-prior")
            )
            .unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "thread-explicit"
            ]
        );

        let fork = vec![
            "--dangerously-bypass-hook-trust".into(),
            "fork".into(),
            "thread-explicit".into(),
        ];
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &fork, Some("thread-prior")).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "--dangerously-bypass-hook-trust",
                "fork",
                "thread-explicit"
            ]
        );
    }

    #[test]
    fn controlled_tui_resume_fails_closed_at_ambiguous_option_boundaries() {
        let unknown = controlled_tui_args(
            "unix:///server.sock",
            &["--future-option".into(), "value".into(), "prompt".into()],
            Some("thread-prior"),
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown Codex option"));

        let image = controlled_tui_args(
            "unix:///server.sock",
            &["--image".into(), "one.png".into(), "prompt".into()],
            Some("thread-prior"),
        )
        .unwrap_err();
        assert!(image.to_string().contains("explicit `--`"));

        assert_eq!(
            controlled_tui_args(
                "unix:///server.sock",
                &[
                    "--image".into(),
                    "one.png".into(),
                    "--".into(),
                    "prompt".into(),
                ],
                Some("thread-prior"),
            )
            .unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "--image",
                "one.png",
                "thread-prior",
                "--",
                "prompt"
            ]
        );
    }

    #[test]
    fn state_key_is_path_and_identity_specific_without_embedding_either() {
        let base = Path::new("/state");
        let first = state_dir_in(base, Path::new("/catalog/a"), "h.worker");
        let second = state_dir_in(base, Path::new("/catalog/b"), "h.worker");
        assert_ne!(first, second);
        assert!(first.starts_with("/state/st2/codex"));
        assert!(!first.display().to_string().contains("worker"));
        assert!(!first.display().to_string().contains("catalog/a"));
    }

    #[test]
    fn runtime_owner_lock_is_nonblocking_and_released_on_close() {
        let tmp = tempfile::tempdir().unwrap();
        let first = acquire_owner_lock(tmp.path()).unwrap();
        let error = acquire_owner_lock(tmp.path()).unwrap_err();
        assert!(error.to_string().contains("already has an owner"));
        drop(first);
        acquire_owner_lock(tmp.path()).unwrap();
    }
}
