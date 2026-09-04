//! Execution (M2/M3) — the side-effecting half that turns a reconcile plan into real pty operations,
//! plus the supervisor loop that reconciles on a folder-watch + timer.
//!
//! Everything st2 does to the world goes through the [`Runner`] trait: list sessions, spawn a pty
//! from its explicit launch, kill a session, remove a dead one. The production [`PtyCli`] shells out
//! to the `pty` CLI; tests swap in a fake, so plan execution is verified without spawning a single
//! real process. st2 stays harness-agnostic here too — it either runs shell source verbatim under
//! `sh -c` or passes a structured argv directly.
//!
//! The loop is decoupled Nomad-style: stopping st2 never tears down its agents — they are detached
//! pty sessions and keep running; only a `retired` spec tears an agent down.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read as _, Seek as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use opentelemetry::trace::Status;
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::exec_backend::ExecBackend;
use crate::flapping::FlappingCap;
use crate::message;
use crate::reconcile::{
    PtyPresentation, ReconcilePlan, Session, TaskCompileContext, TaskLaunch, TaskTarget,
    compile_generated_tasks,
};
use crate::task_inventory::{
    DesiredRuntime, ObservationBatch, ObservedState, RuntimeGeneration, RuntimeObservation,
    RuntimeObserver, generation_id,
};
use agent_spec::spec::TaskKind;

// This is an outer containment bound for a wedged runtime, not a fleet-scalability mechanism.
const PTY_LIST_TIMEOUT: Duration = Duration::from_secs(2);
const PTY_DAEMON_SHUTDOWN_WAIT: Duration = Duration::from_secs(6);
const MAX_PRESENTATION_PATCHES_PER_PASS: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct PresentationPatchCursor {
    after_id: Option<String>,
}

impl PresentationPatchCursor {
    fn batch<'a>(&mut self, presentation: &'a [PtyPresentation]) -> Vec<&'a PtyPresentation> {
        let mut ordered = presentation.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| left.pty_id.cmp(&right.pty_id));
        if ordered.is_empty() {
            return Vec::new();
        }
        let start = self.after_id.as_ref().map_or(0, |after_id| {
            let next = ordered.partition_point(|item| item.pty_id <= *after_id);
            if next == ordered.len() { 0 } else { next }
        });
        let batch = (0..ordered.len().min(MAX_PRESENTATION_PATCHES_PER_PASS))
            .map(|offset| ordered[(start + offset) % ordered.len()])
            .collect::<Vec<_>>();
        self.after_id = batch.last().map(|item| item.pty_id.clone());
        batch
    }
}

/// Per-stream cap for captured child diagnostics. Tail-preserving: when output exceeds the cap,
/// the LAST [`CAPTURE_CAP_BYTES`] bytes are kept — recent output is what a failure message needs,
/// and an uncapped capture lets one chatty child balloon sidecar memory without bound.
pub(crate) const CAPTURE_CAP_BYTES: usize = 256 * 1024;

/// One captured child stream capped to [`CAPTURE_CAP_BYTES`], keeping the tail.
pub(crate) struct BoundedStream {
    pub bytes: Vec<u8>, // last <= cap bytes
    pub total: u64,     // complete stream size before capping
}

impl BoundedStream {
    pub fn truncated(&self) -> bool {
        self.total as usize > self.bytes.len()
    }
}

/// Read back at most `cap` bytes of a temp-file capture, preserving the tail. The file is stat'ed
/// and seek'ed straight to `len - cap`, so the cost is O(cap) no matter how much the child wrote.
pub(crate) fn read_bounded_tail(
    file: &mut std::fs::File,
    cap: usize,
) -> std::io::Result<BoundedStream> {
    let total = file.metadata()?.len();
    let skip = total.saturating_sub(cap as u64);
    file.seek(std::io::SeekFrom::Start(skip))?;
    let mut bytes = Vec::with_capacity((total - skip) as usize);
    file.take(cap as u64).read_to_end(&mut bytes)?;
    Ok(BoundedStream { bytes, total })
}

/// Send an already-killed child to ONE shared reaper thread instead of spawning a detached thread
/// per timed-out child: under a timeout storm one-thread-per-child accumulates without bound.
/// The thread starts lazily on first use.
pub(crate) fn reap_detached(child: std::process::Child) {
    static REAPER: std::sync::LazyLock<std::sync::mpsc::Sender<Child>> =
        std::sync::LazyLock::new(|| {
            let (sender, receiver) = std::sync::mpsc::channel::<Child>();
            // Thread-spawn exhaustion is the only failure mode; panicking here surfaces it at the
            // call site instead of silently leaking unreaped children.
            std::thread::Builder::new()
                .name("st2-child-reaper".to_string())
                .spawn(move || {
                    for mut child in receiver {
                        let _ = child.wait();
                    }
                })
                .expect("spawn shared child reaper thread");
            sender
        });
    let _ = REAPER.send(child);
}

/// Run a non-interactive child with bounded output capture: each stream keeps at most its last
/// [`CAPTURE_CAP_BYTES`] bytes (tail-preserving, with a diagnostic line on truncation). Regular
/// temporary files keep an escaped descendant that inherited stdout/stderr from blocking cleanup
/// after the direct child times out.
/// The child still gets a fresh process group so the common wrapper-and-descendants case is reaped.
#[cfg(test)]
fn output_with_timeout(command: &mut Command, timeout: Duration) -> anyhow::Result<Output> {
    output_with_input_timeout(command, timeout, None)
}

fn terminate_and_reap_before(mut child: Child, pid: i32, deadline: Instant) {
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.kill();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(20)),
                );
            }
            Ok(None) | Err(_) => {
                reap_detached(child);
                return;
            }
        }
    }
}

fn write_all_before(
    mut stdin: ChildStdin,
    mut input: &[u8],
    deadline: Instant,
) -> anyhow::Result<bool> {
    let fd = stdin.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error()).context("read metadata stdin flags");
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(std::io::Error::last_os_error()).context("make metadata stdin nonblocking");
    }
    while !input.is_empty() {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        match stdin.write(input) {
            Ok(0) => {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero))
                    .context("write metadata patch payload");
            }
            Ok(written) => input = &input[written..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Ok(false);
                }
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(20)),
                );
            }
            Err(error) => return Err(error).context("write metadata patch payload"),
        }
    }
    Ok(true)
}

fn output_with_input_timeout(
    command: &mut Command,
    timeout: Duration,
    input: Option<Vec<u8>>,
) -> anyhow::Result<Output> {
    output_with_input_timeout_observed(command, timeout, input, |_| {})
}

/// `on_spawn` observes the direct child's pid at the moment it exists. The child is `setsid`, so
/// that pid is also its process group id — the group this function signals on every failure path.
/// Tests need it to assert the child was reaped, and the child cannot supply it: a test whose
/// deadline expires before the child is first scheduled would never see anything the child wrote.
///
/// It runs BEFORE the child deadline starts, and tests rely on that: a lifecycle test blocks in
/// `on_spawn` until the fixture reached the state it wants to measure, so fork+exec scheduling is
/// paid outside the deadline instead of out of it. See
/// `tests::the_spawn_observer_runs_before_the_child_deadline_starts`.
fn output_with_input_timeout_observed(
    command: &mut Command,
    timeout: Duration,
    input: Option<Vec<u8>>,
    on_spawn: impl FnOnce(i32),
) -> anyhow::Result<Output> {
    run_captured(command, timeout, input, on_spawn, false)
}

/// Like [`output_with_timeout`], but returns the COMPLETE stdout: callers parse structured data
/// (e.g. `pty list --json`) that must be whole, and capping it would corrupt the parse for large
/// fleets. Stdout is therefore intentionally uncapped — one chatty child can balloon this buffer.
/// Stderr stays tail-capped at [`CAPTURE_CAP_BYTES`] with a diagnostic line on truncation,
/// because stderr is only surfaced inside error messages.
pub(crate) fn output_full_stdout_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> anyhow::Result<Output> {
    run_captured(command, timeout, None, |_| {}, true)
}

/// Shared spawn/wait/read-back core. The child is `setsid`, so its pid is also its process group
/// id — the group this function signals on every failure path.
fn run_captured(
    command: &mut Command,
    timeout: Duration,
    input: Option<Vec<u8>>,
    on_spawn: impl FnOnce(i32),
    full_stdout: bool,
) -> anyhow::Result<Output> {
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut child = command.spawn()?;
    let pid = child.id() as i32;
    on_spawn(pid);
    // Load-bearing order: the deadline starts after `on_spawn` returns, so a test that blocks there
    // as a readiness barrier spends none of `timeout` on fork+exec. Moving this line above
    // `on_spawn` is silent in production and makes every barrier test load-sensitive again.
    let deadline = Instant::now() + timeout;
    if let Some(input) = input {
        let Some(stdin) = child.stdin.take() else {
            terminate_and_reap_before(child, pid, deadline);
            anyhow::bail!("metadata patch child has no piped stdin");
        };
        match write_all_before(stdin, &input, deadline) {
            Ok(true) => {}
            Ok(false) => {
                terminate_and_reap_before(child, pid, deadline);
                anyhow::bail!("timed out after {:.1}s", timeout.as_secs_f64());
            }
            Err(error) => {
                terminate_and_reap_before(child, pid, deadline);
                return Err(error);
            }
        }
    }
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_and_reap_before(child, pid, deadline);
            anyhow::bail!("timed out after {:.1}s", timeout.as_secs_f64());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stdout_stream = if full_stdout {
        // Intentionally uncapped: callers parse structured data that must be whole.
        stdout.rewind()?;
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes)?;
        BoundedStream {
            total: bytes.len() as u64,
            bytes,
        }
    } else {
        read_bounded_tail(&mut stdout, CAPTURE_CAP_BYTES)?
    };
    let stderr_stream = read_bounded_tail(&mut stderr, CAPTURE_CAP_BYTES)?;
    let program = command.get_program().to_string_lossy();
    for (stream, name) in [(&stdout_stream, "stdout"), (&stderr_stream, "stderr")] {
        if stream.truncated() {
            eprintln!(
                "st2: truncated {name} capture of `{program}`: keeping last {} of {} bytes (cap {CAPTURE_CAP_BYTES})",
                stream.bytes.len(),
                stream.total,
            );
        }
    }
    Ok(Output {
        status,
        stdout: stdout_stream.bytes,
        stderr: stderr_stream.bytes,
    })
}

/// Resolve a task's working directory: declared `cwd` (expanded), else the agent's `workspace`
/// (expanded), else the spec file's directory (spec.md §2). A relative value is joined to the spec
/// dir; an absolute one replaces it.
pub(crate) fn resolve_task_cwd(
    target: &TaskTarget,
    spec_dir: &Path,
    catalog_root: &Path,
) -> PathBuf {
    match target.cwd.as_deref().or(target.workspace.as_deref()) {
        Some(c) => spec_dir.join(crate::expand::expand_catalog(c, catalog_root)),
        None => spec_dir.to_path_buf(),
    }
}

/// The set of task operations st2 needs. Abstracted so execution is testable against a fake.
pub trait Runner {
    /// ACTUAL state: every task session the runner can see (unioned across backends).
    fn list_sessions(&self) -> anyhow::Result<Vec<Session>>;
    /// Spawn `target` in the background from its explicit launch. `spec_dir` is the spec file's
    /// directory — part of the cwd fallback chain (task.cwd → workspace → spec dir).
    fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()>;
    /// Atomically reconcile display metadata and the complete st2-owned tag snapshot for one exact
    /// existing PTY ID. The default is a no-op for non-PTY test/backends.
    fn patch_presentation(&self, _presentation: &PtyPresentation) -> anyhow::Result<()> {
        Ok(())
    }
    /// SIGTERM a running session.
    fn kill(&self, pty_id: &str) -> anyhow::Result<()>;
    /// Reap an exited session before restarting it. Backends may preserve bounded diagnostics here.
    fn reap_for_restart(&self, pty_id: &str) -> anyhow::Result<()> {
        self.remove(pty_id)
    }
    /// Finally remove an exited session's files (retirement/final garbage collection).
    fn remove(&self, pty_id: &str) -> anyhow::Result<()>;
}

/// Production [`Runner`]. Shells out to the `pty` CLI for tasks. (M1a routes both `pty` and `exec`
/// tasks here; the terminal-free `exec` backend lands in M1b — R09.)
pub struct PtyCli {
    /// The `pty` binary (defaults to `pty` on PATH).
    bin: String,
    /// The catalog root — the value of `$CATALOG` during `$`-expansion (spec.md §2 / R11).
    catalog_root: PathBuf,
}

impl Default for PtyCli {
    fn default() -> Self {
        Self {
            bin: "pty".to_string(),
            catalog_root: PathBuf::from("."),
        }
    }
}

/// One entry of `pty list --json` — only the fields st2 needs.
#[derive(Debug, Deserialize)]
struct PtyListEntry {
    /// The pinned session id (matches `--id`), st2's key back to a declared task.
    name: String,
    /// `running` | `exited` | `vanished`.
    status: String,
    /// The process exit code once `exited` (absent while running or `vanished`).
    #[serde(rename = "exitCode", default)]
    exit_code: Option<i64>,
    /// PTY daemon PID. Together with `createdAt`, this identifies one generation.
    #[serde(default)]
    pid: Option<u32>,
    /// PTY-owned generation creation time.
    #[serde(rename = "createdAt", default)]
    created_at: Option<String>,
    #[serde(rename = "displayName", default)]
    display_name: Option<String>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct PtyMetadataPatch<'a> {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    display_name: Option<&'a Option<String>>,
    tags: &'a BTreeMap<String, Option<String>>,
}

/// The `PTY_ROOT` st2 uses for a pty op. An EXPORTED ambient `PTY_ROOT` WINS — a decoupled partition,
/// e.g. an eval run's short `/tmp/stev-<runid>` that dodges the 104-byte unix-socket-path limit that a
/// deep `<catalog>/pty` would blow — else what the catalog itself declares
/// ([`crate::catalog::pty_root`]), else the native default `<catalog>/pty`. Applied uniformly to
/// spawn and list/kill so st2 always manages sessions where it put them.
pub fn effective_pty_root(catalog_root: &Path) -> PathBuf {
    effective_pty_root_from(catalog_root, std::env::var_os("PTY_ROOT"))
}

/// The testable core of [`effective_pty_root`] — the ambient value is injected rather than read from
/// the process env, so tests don't race on the global environment.
fn effective_pty_root_from(catalog_root: &Path, ambient: Option<std::ffi::OsString>) -> PathBuf {
    match ambient {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => crate::catalog::pty_root(catalog_root),
    }
}

/// The portable bound on a unix socket address. Darwin caps `sun_path` at 104 bytes and Linux at
/// 108, so a declaration has to satisfy the smaller one to be admissible on either.
pub const PORTABLE_SOCKET_PATH_LIMIT: usize = 104;

/// The socket `pty` binds for one session: `<PTY_ROOT>/<session-id>.sock`.
///
/// Measured against the pty binary rather than assumed: for a 21-byte root and a 78-byte id it
/// reports a 105-byte path exceeding the limit by 1, which is `root + '/' + id + ".sock"`.
pub fn session_socket_path(pty_root: &Path, session_id: &str) -> PathBuf {
    pty_root.join(format!("{session_id}.sock"))
}

/// The resolved socket path and its overage, when a task's session socket cannot be bound.
///
/// `pty` refuses the bind rather than truncating, so such a task can never spawn: it fails
/// identically on every reconcile pass, forever. The bound is not a constant — the usable identity
/// length is what remains of the limit after the resolved pty root — so it is always derived from
/// the root actually in use.
pub fn session_socket_overage(pty_root: &Path, session_id: &str) -> Option<(PathBuf, usize)> {
    let path = session_socket_path(pty_root, session_id);
    let bytes = path.as_os_str().as_encoded_bytes().len();
    bytes
        .checked_sub(PORTABLE_SOCKET_PATH_LIMIT)
        .filter(|over| *over > 0)
        .map(|over| (path, over))
}

impl PtyCli {
    /// A `PtyCli` rooted at `catalog_root` (used for `$CATALOG` expansion).
    pub fn new(catalog_root: PathBuf) -> Self {
        Self {
            bin: "pty".to_string(),
            catalog_root,
        }
    }

    /// Expand `$VAR`/`${VAR}` against the ambient env plus `$CATALOG` = the catalog root.
    fn expand(&self, s: &str) -> String {
        crate::expand::expand_catalog(s, &self.catalog_root)
    }

    /// Resolve a task's working directory (see [`resolve_task_cwd`]).
    fn resolve_cwd(&self, target: &TaskTarget, spec_dir: &Path) -> PathBuf {
        resolve_task_cwd(target, spec_dir, &self.catalog_root)
    }

    /// The st2-owned part of a PTY task's environment. This same final map is both inherited by the
    /// initial `pty run` process and persisted through repeatable `--env KEY=VALUE` arguments, so a
    /// manual `pty restart` recreates the task without snapshotting unrelated ambient OS variables.
    fn managed_task_env(&self, target: &TaskTarget) -> BTreeMap<OsString, OsString> {
        let mut env = BTreeMap::from([
            (
                OsString::from("CATALOG"),
                self.catalog_root.as_os_str().to_os_string(),
            ),
            (
                OsString::from("ST_ROOT"),
                self.catalog_root.as_os_str().to_os_string(),
            ),
            (
                OsString::from("PTY_ROOT"),
                effective_pty_root(&self.catalog_root).into_os_string(),
            ),
            (OsString::from("TERM"), OsString::from("xterm-256color")),
        ]);
        if let Ok(path) = crate::hooks::hooks_root() {
            env.insert(OsString::from("ST_HOOKS"), path.into_os_string());
        }
        for (key, value) in &target.env {
            let value = if key == "PTY_ROOT" {
                effective_pty_root(&self.catalog_root).into_os_string()
            } else {
                OsString::from(self.expand(value))
            };
            env.insert(OsString::from(key), value);
        }
        env
    }

    /// Expand direct arguments against the environment that the managed task receives. The task
    /// overlay wins over the launcher environment, as it does after the process starts.
    fn expand_managed(&self, value: &str, managed_env: &BTreeMap<OsString, OsString>) -> String {
        crate::expand::expand_vars(value, |key| {
            managed_env
                .get(OsStr::new(key))
                .map(|value| value.to_string_lossy().into_owned())
                .or_else(|| std::env::var(key).ok())
        })
    }

    /// Build (but do not run) the `pty run` invocation for `target`. Split out so the exact argv +
    /// env can be unit-tested without spawning anything.
    ///
    /// `$VAR`s are expanded here for task-authored values that do NOT pass through a shell — env,
    /// tags, `cwd`, and direct argv — because `pty` passes them through verbatim. The st2-owned
    /// presentation snapshot remains literal so initial spawn and later metadata patches agree.
    /// Shell source is left unexpanded: `sh -c` expands it at spawn from the same env.
    fn build_run_command(&self, target: &TaskTarget, spec_dir: &Path) -> Command {
        let cwd = self.resolve_cwd(target, spec_dir);
        let mut cmd = Command::new(&self.bin);
        cmd.arg("run")
            .arg("-d") // detached: leave it running in the background
            .arg("--force") // st2 itself may run inside a pty session; allow nesting
            .args(["--id", &target.pty_id]);
        match target
            .presentation
            .as_ref()
            .map(|presentation| &presentation.display_name)
        {
            Some(Some(Some(name))) if name == &target.pty_id => {
                cmd.arg("--no-display-name");
            }
            Some(Some(Some(name))) => {
                cmd.args(["--name", name]);
            }
            Some(Some(None)) => {
                cmd.arg("--no-display-name");
            }
            // Secondary tasks retain the established task-specific presentation convention.
            _ if target.pty_id == target.bus_id => {
                cmd.arg("--no-display-name");
            }
            _ => {
                cmd.args(["--name", &target.bus_id]);
            }
        }
        cmd.arg("--cwd").arg(&cwd);
        let mut tags = target
            .tags
            .iter()
            .map(|(key, value)| (key.clone(), self.expand(value)))
            .collect::<BTreeMap<_, _>>();
        if let Some(presentation) = &target.presentation {
            for (key, value) in &presentation.tags {
                match value {
                    Some(value) => {
                        tags.insert(key.clone(), value.clone());
                    }
                    None => {
                        tags.remove(key);
                    }
                }
            }
        }
        for (k, v) in &tags {
            cmd.arg("--tag").arg(format!("{k}={v}"));
        }
        // Managed agent and DING sessions retain PTY exit evidence until the lifecycle owner records
        // the receipt and explicitly removes the generation. This prevents face607 clean-exit reaping
        // from erasing diagnostics needed for adoption/loss investigation.
        if target.name == "agent" || target.name == "ding" {
            cmd.arg("--tag").arg("keep=true");
        }
        // Apply the resolved managed overlay to the initial launcher exactly as before, and also
        // persist it in PTY metadata for manual restart. PTY applies repeated `--env` entries
        // last-wins, then forcibly injects the new session's own PTY_SESSION identity.
        let managed_env = self.managed_task_env(target);
        cmd.envs(&managed_env);
        // Coding-agent command runners commonly set NO_COLOR for their own captured output. That
        // ambient preference belongs to the launcher, not to the interactive agent it happens to
        // reconcile. Agent Spec env remains authoritative when an agent deliberately opts out.
        if target.name == "agent" && !target.env.contains_key("NO_COLOR") {
            cmd.env_remove("NO_COLOR");
            cmd.arg("--unset-env").arg("NO_COLOR");
        }
        for (key, value) in &managed_env {
            let mut assignment = key.clone();
            assignment.push("=");
            assignment.push(value);
            cmd.arg("--env").arg(assignment);
        }
        cmd.arg("--");
        match &target.launch {
            // Run shell source verbatim — st2 never parses or splits it.
            TaskLaunch::Shell(command) => {
                cmd.arg("sh").arg("-c").arg(command);
            }
            // Direct mode preserves argument boundaries and introduces no shell process.
            TaskLaunch::Argv(argv) => {
                debug_assert!(!argv.is_empty());
                cmd.args(
                    argv.iter()
                        .map(|arg| self.expand_managed(arg, &managed_env)),
                );
            }
        }
        cmd
    }

    /// Pure, typed PTY observation. A missing root is known empty and is not
    /// passed to `pty`, because observation must not create it.
    fn task_observations(&self, desired_ids: &HashSet<&str>) -> ObservationBatch {
        let root = effective_pty_root(&self.catalog_root);
        self.task_observations_at_root(desired_ids, &root)
    }

    fn task_observations_at_root(
        &self,
        desired_ids: &HashSet<&str>,
        root: &Path,
    ) -> ObservationBatch {
        if desired_ids.is_empty() {
            return ObservationBatch {
                complete: true,
                ..ObservationBatch::default()
            };
        }
        // Retain the admitted directory inode across the external probe. `pty
        // list` creates PTY_ROOT when absent, so a path removed and recreated
        // during the call must never be confused with the admitted registry.
        let root_handle = match File::open(root) {
            Ok(handle) => handle,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return ObservationBatch {
                    complete: true,
                    ..ObservationBatch::default()
                };
            }
            Err(error) => {
                return ObservationBatch {
                    complete: false,
                    observations: Vec::new(),
                    errors: vec![format!(
                        "cannot inspect PTY root {}: {error}",
                        root.display()
                    )],
                };
            }
        };
        let metadata = match root_handle.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                return ObservationBatch {
                    complete: false,
                    observations: Vec::new(),
                    errors: vec![format!(
                        "cannot inspect admitted PTY root {}: {error}",
                        root.display()
                    )],
                };
            }
        };
        if !metadata.is_dir() {
            return ObservationBatch {
                complete: false,
                observations: Vec::new(),
                errors: vec![format!("PTY root {} is not a directory", root.display())],
            };
        }
        let entries = match self.list_entries_at(root) {
            Ok(entries) => entries,
            Err(error) => {
                return ObservationBatch {
                    complete: false,
                    observations: Vec::new(),
                    errors: vec![error.to_string()],
                };
            }
        };
        let final_metadata = match std::fs::metadata(root) {
            Ok(final_metadata) => final_metadata,
            Err(error) => {
                return ObservationBatch {
                    complete: false,
                    observations: Vec::new(),
                    errors: vec![format!(
                        "PTY root {} disappeared during observation: {error}",
                        root.display()
                    )],
                };
            }
        };
        if !final_metadata.is_dir()
            || final_metadata.dev() != metadata.dev()
            || final_metadata.ino() != metadata.ino()
        {
            return ObservationBatch {
                complete: false,
                observations: Vec::new(),
                errors: vec![format!(
                    "PTY root {} changed identity during observation",
                    root.display()
                )],
            };
        }
        let mut observations = Vec::with_capacity(entries.len());
        let mut errors = Vec::new();
        for entry in entries {
            if !desired_ids.contains(entry.name.as_str()) {
                continue;
            }
            let state = match entry.status.as_str() {
                "running" => match (entry.pid, entry.created_at.as_deref()) {
                    (Some(pid), Some(created_at)) => {
                        let generation_id =
                            generation_id("pty", &entry.name, pid, created_at, None);
                        match RuntimeGeneration::new(pid, created_at.to_owned(), generation_id) {
                            Ok(generation) => ObservedState::Running(generation),
                            Err(error) => {
                                let message = format!(
                                    "invalid PTY task {:?} generation: {error}",
                                    entry.name
                                );
                                errors.push(message.clone());
                                ObservedState::Indeterminate(message)
                            }
                        }
                    }
                    _ => {
                        let message = format!(
                            "running PTY task {:?} lacks pid or createdAt generation evidence",
                            entry.name
                        );
                        errors.push(message.clone());
                        ObservedState::Indeterminate(message)
                    }
                },
                "exited" => ObservedState::Exited,
                "vanished" => ObservedState::Vanished,
                other => {
                    let message = format!("PTY task {:?} has unknown status {other:?}", entry.name);
                    errors.push(message.clone());
                    ObservedState::Indeterminate(message)
                }
            };
            observations.push(RuntimeObservation {
                runtime_id: entry.name,
                state,
            });
        }
        ObservationBatch {
            complete: errors.is_empty(),
            observations,
            errors,
        }
    }

    fn patch_presentation(&self, presentation: &PtyPresentation) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&PtyMetadataPatch {
            display_name: presentation.display_name.as_ref(),
            tags: &presentation.tags,
        })?;
        let out = output_with_input_timeout(
            Command::new(&self.bin)
                .args(["metadata", "patch", "--id", &presentation.pty_id])
                .env("PTY_ROOT", effective_pty_root(&self.catalog_root)),
            PTY_LIST_TIMEOUT,
            Some(payload),
        )
        .map_err(|error| anyhow::anyhow!("`pty metadata patch --id` failed: {error}"))?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty metadata patch --id {}` failed: {}",
                presentation.pty_id,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn list_entries(&self) -> anyhow::Result<Vec<PtyListEntry>> {
        self.list_entries_at(&effective_pty_root(&self.catalog_root))
    }

    fn list_entries_at(&self, root: &Path) -> anyhow::Result<Vec<PtyListEntry>> {
        let out = output_full_stdout_with_timeout(
            Command::new(&self.bin)
                .args(["list", "--json"])
                .env("PTY_ROOT", root),
            PTY_LIST_TIMEOUT,
        )
        .map_err(|error| anyhow::anyhow!("`pty list --json` failed: {error}"))?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty list --json` failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        serde_json::from_slice(&out.stdout)
            .map_err(|error| anyhow::anyhow!("parsing `pty list --json`: {error}"))
    }
}

/// Apply both assignments and removals from an inner command to its isolation wrapper.
fn apply_command_env(source: &Command, target: &mut Command) {
    for (key, value) in source.get_envs() {
        match value {
            Some(value) => {
                target.env(key, value);
            }
            None => {
                target.env_remove(key);
            }
        }
    }
}

impl Runner for PtyCli {
    fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        Ok(self
            .list_entries()?
            .into_iter()
            .map(|e| Session {
                pty_id: e.name,
                alive: e.status == "running",
                exit_code: e.exit_code,
                presentation: Some(crate::reconcile::ObservedPtyPresentation {
                    display_name: e.display_name,
                    tags: e.tags,
                }),
            })
            .collect())
    }

    fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
        // Isolate the pty session in its own scope (R21b): `systemd-run --scope` wraps the `pty run`
        // invocation, and because the per-session `pty-daemon` inherits the caller's cgroup (a
        // double-fork does not move cgroups), the daemon + session land in the scope — a sibling of
        // the transport unit that a transport/supervisor cgroup-cascade cannot reach. Pass-through on
        // non-systemd hosts. The inner argv (built + unit-tested by `build_run_command`) is wrapped
        // verbatim and its env re-applied so it reaches `pty` (and thus the session) through the scope.
        let inner = self.build_run_command(target, spec_dir);
        let program = inner.get_program().to_os_string();
        let args: Vec<OsString> = inner.get_args().map(|a| a.to_os_string()).collect();
        let arg_refs: Vec<&std::ffi::OsStr> = args.iter().map(|a| a.as_os_str()).collect();
        let unit = crate::isolate::scope_unit(&target.pty_id);

        // Atomic reap-then-respawn: a session id JUST reaped in this same pass (execute's
        // reap-then-respawn after a hard-kill) can linger microseconds in the per-session pty daemon —
        // `pty rm` frees the registry entry but the daemon's socket/lock isn't released yet, so an
        // immediate `pty run` fails "id already in use". Reap the lingering corpse + brief backoff +
        // retry closes the window WITHIN the pass, instead of leaving `--once` to error and relying on a
        // later loop cycle to self-heal (loop mode did; `--once` had one shot). Bounded; on a persistent
        // failure it surfaces the error unchanged.
        const SPAWN_ATTEMPTS: u32 = 4;
        let mut last_err = String::new();
        for attempt in 0..SPAWN_ATTEMPTS {
            let mut cmd = crate::isolate::wrap(&unit, program.as_os_str(), &arg_refs);
            apply_command_env(&inner, &mut cmd);
            let out = cmd.output()?;
            if out.status.success() {
                return Ok(());
            }
            last_err = String::from_utf8_lossy(&out.stderr).trim().to_string();
            let corpse_race = last_err.contains("already in use");
            if !corpse_race || attempt + 1 == SPAWN_ATTEMPTS {
                break;
            }
            // A lingering corpse blocks the id — reap it, then back off before retrying.
            let _ = Command::new(&self.bin)
                .arg("rm")
                .arg(&target.pty_id)
                .env("PTY_ROOT", effective_pty_root(&self.catalog_root))
                .output();
            std::thread::sleep(Duration::from_millis(100 * u64::from(attempt + 1)));
        }
        anyhow::bail!("spawning pty '{}' failed: {last_err}", target.pty_id);
    }

    fn patch_presentation(&self, presentation: &PtyPresentation) -> anyhow::Result<()> {
        PtyCli::patch_presentation(self, presentation)
    }

    fn kill(&self, pty_id: &str) -> anyhow::Result<()> {
        let out = Command::new(&self.bin)
            .arg("kill")
            .arg(pty_id)
            .env("PTY_ROOT", effective_pty_root(&self.catalog_root))
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty kill {pty_id}` failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn reap_for_restart(&self, pty_id: &str) -> anyhow::Result<()> {
        // An exited PTY can remain in its daemon's 500ms shutdown window. Removing its files during
        // that window and immediately reusing the id is unsafe: the old generation's final cleanup
        // can unlink the new generation's socket/pid and leave `pty run` waiting for its full startup
        // timeout. Wait for the recorded daemon to finish its bounded shutdown before removing the
        // corpse. PTY may self-reap the files while we wait; "not found" is therefore success here.
        let pty_root = effective_pty_root(&self.catalog_root);
        let daemon_pid = std::fs::read_to_string(pty_root.join(format!("{pty_id}.pid")))
            .ok()
            .and_then(|raw| raw.trim().parse::<i32>().ok());
        if let Some(pid) = daemon_pid {
            let deadline = Instant::now() + PTY_DAEMON_SHUTDOWN_WAIT;
            while crate::host_lock::process_alive(pid) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(25));
            }
            if crate::host_lock::process_alive(pid) {
                anyhow::bail!(
                    "pty daemon {pid} for '{pty_id}' did not finish its bounded shutdown"
                );
            }
        }

        let out = Command::new(&self.bin)
            .arg("rm")
            .arg(pty_id)
            .env("PTY_ROOT", &pty_root)
            .output()?;
        if out.status.success() || String::from_utf8_lossy(&out.stderr).contains("not found") {
            return Ok(());
        }
        anyhow::bail!(
            "`pty rm {pty_id}` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
    }

    fn remove(&self, pty_id: &str) -> anyhow::Result<()> {
        let out = Command::new(&self.bin)
            .arg("rm")
            .arg(pty_id)
            .env("PTY_ROOT", effective_pty_root(&self.catalog_root))
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty rm {pty_id}` failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }
}

/// The production [`Runner`]: routes `pty` tasks to the `pty` CLI and `exec` tasks to the
/// terminal-free [`ExecBackend`], presenting one unified session view. kill/remove route by the kind
/// recorded during the last `list_sessions` (with a both-backends fallback).
pub struct SystemRunner {
    pty: PtyCli,
    exec: ExecBackend,
    /// id → kind, refreshed each `list_sessions`, so kill/remove hit the right backend.
    index: RefCell<HashMap<String, TaskKind>>,
}

impl SystemRunner {
    /// `catalog_root` roots `$CATALOG`; `exec_state_dir` is where exec pids/logs live (machine-local).
    pub fn new(catalog_root: PathBuf, exec_state_dir: PathBuf) -> Self {
        Self {
            pty: PtyCli::new(catalog_root.clone()),
            exec: ExecBackend::new(exec_state_dir, catalog_root),
            index: RefCell::new(HashMap::new()),
        }
    }

    /// Fully retire a launched stream runtime before its declaration is removed.
    pub fn retire(&self, runtime_id: &str) -> anyhow::Result<()> {
        match self.index.borrow().get(runtime_id) {
            Some(TaskKind::Exec) => self.exec.retire(runtime_id),
            Some(TaskKind::Pty) => {
                self.pty.kill(runtime_id)?;
                self.pty.reap_for_restart(runtime_id)
            }
            None => anyhow::bail!("runtime '{runtime_id}' disappeared before retirement"),
        }
    }
}

impl RuntimeObserver for SystemRunner {
    fn observe(&self, desired: &[DesiredRuntime]) -> ObservationBatch {
        let pty_ids = desired
            .iter()
            .filter(|runtime| runtime.kind == TaskKind::Pty)
            .map(|runtime| runtime.runtime_id.as_str())
            .collect::<HashSet<_>>();
        let mut batch = self.pty.task_observations(&pty_ids);

        for runtime in desired
            .iter()
            .filter(|runtime| runtime.kind == TaskKind::Exec)
        {
            match self.exec.observe_generation_optional(&runtime.runtime_id) {
                Ok(None) => {
                    // Positive absence is filled by the declaration/runtime join.
                }
                Ok(Some(crate::exec_backend::ExecGenerationObservation::Running {
                    pid,
                    created_at,
                    generation_id,
                })) => {
                    let state = match RuntimeGeneration::new(pid, created_at, generation_id) {
                        Ok(generation) => ObservedState::Running(generation),
                        Err(error) => {
                            let message = format!(
                                "invalid exec task {:?} generation: {error}",
                                runtime.runtime_id
                            );
                            batch.errors.push(message.clone());
                            ObservedState::Indeterminate(message)
                        }
                    };
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state,
                    });
                }
                Ok(Some(crate::exec_backend::ExecGenerationObservation::Exited { .. })) => {
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state: ObservedState::Exited,
                    });
                }
                Ok(Some(crate::exec_backend::ExecGenerationObservation::Indeterminate {
                    reason,
                    ..
                })) => {
                    let message = format!(
                        "exec task {:?} is indeterminate: {reason}",
                        runtime.runtime_id
                    );
                    batch.errors.push(message.clone());
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state: ObservedState::Indeterminate(message),
                    });
                }
                Err(error) => {
                    let message = format!("observe exec task {:?}: {error:#}", runtime.runtime_id);
                    batch.errors.push(message.clone());
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state: ObservedState::Indeterminate(message),
                    });
                }
            }
        }
        batch.complete &= batch.errors.is_empty();
        batch
    }
}

impl Runner for SystemRunner {
    fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        let mut idx = self.index.borrow_mut();
        idx.clear();
        let mut all = self.pty.list_sessions()?;
        for s in &all {
            idx.insert(s.pty_id.clone(), TaskKind::Pty);
        }
        let ex = self.exec.list()?;
        for s in &ex {
            idx.insert(s.pty_id.clone(), TaskKind::Exec);
        }
        all.extend(ex);
        Ok(all)
    }

    fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
        match target.kind {
            TaskKind::Pty => self.pty.spawn(target, spec_dir),
            TaskKind::Exec => self.exec.spawn(target, spec_dir),
        }
    }

    fn patch_presentation(&self, presentation: &PtyPresentation) -> anyhow::Result<()> {
        self.pty.patch_presentation(presentation)
    }

    fn kill(&self, pty_id: &str) -> anyhow::Result<()> {
        match self.index.borrow().get(pty_id) {
            Some(TaskKind::Exec) => self.exec.kill(pty_id),
            Some(TaskKind::Pty) => self.pty.kill(pty_id),
            None => self.pty.kill(pty_id).or_else(|_| self.exec.kill(pty_id)),
        }
    }

    fn reap_for_restart(&self, pty_id: &str) -> anyhow::Result<()> {
        match self.index.borrow().get(pty_id) {
            Some(TaskKind::Exec) => self.exec.reap_for_restart(pty_id),
            Some(TaskKind::Pty) => self.pty.reap_for_restart(pty_id),
            None => {
                let _ = self.pty.reap_for_restart(pty_id);
                self.exec.reap_for_restart(pty_id)
            }
        }
    }

    fn remove(&self, pty_id: &str) -> anyhow::Result<()> {
        match self.index.borrow().get(pty_id) {
            Some(TaskKind::Exec) => self.exec.remove(pty_id),
            Some(TaskKind::Pty) => self.pty.remove(pty_id),
            None => {
                let _ = self.pty.remove(pty_id);
                let _ = self.exec.remove(pty_id);
                Ok(())
            }
        }
    }
}

/// The machine-local state root shared by host runtime state and supervisor-scoped channels.
pub(crate) fn state_root() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// The machine-local runner-state dir for a host's exec tasks: `$XDG_STATE_HOME/st2/<host>/exec`
/// (falling back to `~/.local/state`). Not synced — pids are host-local.
pub fn exec_state_dir(host: &str) -> PathBuf {
    state_root().join("st2").join(host).join("exec")
}

/// A task st2 gave up restarting (crash-looped past its `restart{}` policy, mode=fail) — carries what
/// the supervisor loop needs to SURFACE it: the parked task, its agent, and who to notify (M2.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashLoop {
    /// The parked task's pty id.
    pub pty_id: String,
    /// The owning agent's identity and (declared) host — resolved to a bus id when surfacing.
    pub identity: String,
    pub host: Option<String>,
    /// The agent's `supervisor` (from its spec), the crash-ding recipient. `None` → nobody to notify.
    pub supervisor: Option<String>,
}

impl CrashLoop {
    /// The parked agent's bus id (`<host>.<identity>`), using `this_host` when the spec omits a host.
    pub fn agent_bus_id(&self, this_host: &str) -> String {
        format!(
            "{}.{}",
            self.host.as_deref().unwrap_or(this_host),
            self.identity
        )
    }
}

/// Owned, human-readable summary of one reconcile+execute pass (no borrows of the plan/specs).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct UpReport {
    /// The pass could not obtain an authoritative session snapshot, so it deliberately performed no
    /// reconciliation. Long-running supervisors retry; a one-shot caller must exit unsuccessfully.
    pub skipped: bool,
    /// Task IDs that st2 started without a restart reap in this pass.
    pub launched: Vec<String>,
    /// Task IDs that st2 restarted successfully in this pass. st2 reaped a dead active record
    /// before it spawned the replacement. These IDs are not first launches or final garbage
    /// collection.
    pub restarted: Vec<String>,
    /// pty ids torn down (retired agents) this pass.
    pub torn_down: Vec<String>,
    /// Task IDs in final garbage collection. st2 did not spawn replacements.
    pub gc: Vec<String>,
    /// pty ids whose GC/relaunch was DEFERRED this pass by the liveness debounce — a task that read
    /// not-alive but was alive within the grace window, i.e. a transient `pty list` flicker under load,
    /// left alone rather than destructively reaped (R21c). Not "noteworthy" (it's a no-op by design).
    pub deferred: Vec<String>,
    /// dead or absent adopt-only task ids held without reap or launch.
    pub held: Vec<String>,
    /// pty ids the flapping-cap refused to (re)launch this pass (parked / crash-looping).
    pub flapping: Vec<String>,
    /// pty ids released from a park this pass by an explicit operator request.
    pub unparked: Vec<String>,
    /// Rich crash-loop records (a superset of `flapping`) — the source for supervisor surfacing.
    pub crash_loops: Vec<CrashLoop>,
    /// identities adopted (already fully present).
    pub adopted: Vec<String>,
    /// identities skipped as belonging to another host.
    pub other_host: Vec<String>,
    /// identities with no runnable task (unrendered).
    pub unrunnable: Vec<String>,
    /// discovery warnings (mismatches, …).
    pub warnings: Vec<String>,
    /// discovery + execution errors (non-fatal; collected).
    pub errors: Vec<String>,
}

impl UpReport {
    fn absorb(&mut self, mut other: UpReport) {
        self.skipped |= other.skipped;
        self.launched.append(&mut other.launched);
        self.restarted.append(&mut other.restarted);
        self.torn_down.append(&mut other.torn_down);
        self.gc.append(&mut other.gc);
        self.deferred.append(&mut other.deferred);
        self.held.append(&mut other.held);
        self.flapping.append(&mut other.flapping);
        self.unparked.append(&mut other.unparked);
        self.crash_loops.append(&mut other.crash_loops);
        self.adopted.append(&mut other.adopted);
        self.other_host.append(&mut other.other_host);
        self.unrunnable.append(&mut other.unrunnable);
        self.warnings.append(&mut other.warnings);
        self.errors.append(&mut other.errors);
    }
    /// True when the pass actually changed something (or hit an error) — used to keep the loop's log
    /// quiet on no-op ticks.
    pub fn is_noteworthy(&self) -> bool {
        self.skipped
            || !self.launched.is_empty()
            || !self.restarted.is_empty()
            || !self.torn_down.is_empty()
            || !self.gc.is_empty()
            || !self.flapping.is_empty()
            || !self.unparked.is_empty()
            || !self.warnings.is_empty()
            || !self.errors.is_empty()
    }
}

/// Why a task is parked, in the operator's terms. Published with every marker so the fault reads on
/// its own, without the reader having to already know what `mode = fail` means.
pub const PARK_REASON: &str = "crash-looped past its restart{} policy (mode=fail)";

/// Grant the operator's pending unpark requests into `cap`.
///
/// Call this *before* the pass plans anything, so a granted recovery relaunches in the same pass that
/// granted it. Deferring it to the next pass would make the operator's targeted act take up to a full
/// `--interval` to show any effect, which reads as "it did nothing" and invites a second attempt.
///
/// Only the long-lived supervisor loops may call it. A one-shot `up_once` builds a fresh
/// [`FlappingCap`] with an empty parked set, so consuming a request there would silently discard it.
pub fn grant_unpark_requests(cap: &mut FlappingCap, request_dir: &Path, report: &mut UpReport) {
    let (ids, errors) = crate::park::take_unpark_requests(request_dir);
    report.errors.extend(errors);
    for id in ids {
        if cap.unpark(&id) {
            report.unparked.push(id);
        } else {
            report
                .warnings
                .push(format!("unpark '{id}': not parked; nothing to recover"));
        }
    }
}

/// Republish the parked projection to match `cap`.
///
/// Call this *after* the pass has executed, so a task parked by this very pass is already visible to
/// the next `st2 tasks`. Republishing also retracts the marker of anything no longer parked, so a
/// recovered task stops reporting a fault without anyone remembering to clean up.
///
/// Supervisor loops only, for the same reason as [`grant_unpark_requests`]: publishing an empty
/// one-shot cap would wipe the running supervisor's projection and hide every live park.
pub fn publish_parks(
    cap: &FlappingCap,
    projection: &crate::park::ParkProjection,
    report: &mut UpReport,
) {
    let parked: std::collections::BTreeSet<String> = cap.parked_ids().cloned().collect();
    report
        .errors
        .extend(projection.publish(&parked, PARK_REASON));
}

/// A supervisor loop's end of the park channel: the projection it publishes and the request dir it
/// drains. Bundled because the two are only ever used together, and only by a loop that owns a
/// long-lived [`FlappingCap`].
///
/// A supervisor that cannot identify its own process generation cannot write a believable marker, so
/// it publishes nothing rather than something a reader would have to guess about. Parking itself is
/// unaffected — it degrades to exactly the pre-#204 behaviour, loudly.
pub struct ParkChannel {
    projection: Option<crate::park::ParkProjection>,
    request_dir: Option<PathBuf>,
}

impl ParkChannel {
    pub fn for_supervisor(catalog_root: &Path, host: &str) -> Self {
        let scope = match crate::park::SupervisorScope::current(catalog_root, host) {
            Ok(scope) => scope,
            Err(error) => {
                tracing::warn!(
                    "st2: cannot open the supervisor park channel ({error}); parks remain terminal but cannot be observed or explicitly released."
                );
                return Self {
                    projection: None,
                    request_dir: None,
                };
            }
        };
        let projection = match crate::park::ParkProjection::current(scope.park_dir()) {
            Ok(projection) => Some(projection),
            Err(error) => {
                tracing::warn!(
                    "st2: cannot publish parked tasks ({error}); `st2 tasks` will not show park faults for this supervisor."
                );
                None
            }
        };
        Self {
            projection,
            request_dir: Some(scope.unpark_request_dir()),
        }
    }

    fn grant_requests(&self, cap: &mut FlappingCap, report: &mut UpReport) {
        if let Some(request_dir) = &self.request_dir {
            grant_unpark_requests(cap, request_dir, report);
        }
    }

    fn publish(&self, cap: &FlappingCap, report: &mut UpReport) {
        if let Some(projection) = &self.projection {
            publish_parks(cap, projection, report);
        }
    }
}

/// Execute a plan against a runner, folding results into `report` and consulting/updating the
/// flapping-cap. Order matters: reap standalone corpses, then (cap-gated) reap-and-respawn each
/// launch target, then kill teardowns. Per-op errors are collected, never fatal.
pub fn execute(
    plan: &ReconcilePlan,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    report: &mut UpReport,
) {
    execute_with_presentation_cursor(
        plan,
        runner,
        cap,
        &mut PresentationPatchCursor::default(),
        report,
        &mut |_| {},
    );
}

fn stop_live_derived_companions(
    launch: &crate::reconcile::Launch<'_>,
    runner: &dyn Runner,
    report: &mut UpReport,
) {
    for companion_id in &launch.live_derived {
        match runner.kill(companion_id) {
            Ok(()) => report.torn_down.push(companion_id.clone()),
            Err(error) => report.errors.push(format!(
                "kill unavailable derived companion {companion_id}: {error}"
            )),
        }
    }
}

/// Bounded driver label for lifecycle metrics (`task_launches_total` / `task_reaps_total`).
/// Typed drivers report their own name; legacy routed and hand-authored seats are classified
/// by what their launch actually invokes. Anything unrecognizable collapses to `other`, so
/// the label stays a closed set: `codex|claude|opencode|pi|omp|exec|other`. Observational only —
/// callers gate on [`crate::metrics::enabled`], and it never influences reconcile decisions.
fn driver_label(launch: &crate::reconcile::Launch<'_>, target: &TaskTarget) -> &'static str {
    if target.kind == TaskKind::Exec {
        return "exec";
    }
    if let Some(driver) = &launch.spec.driver {
        return driver.name();
    }
    // Legacy routed (`deliver "mcp"` → claude-session, ...) or hand-authored seats: inspect
    // the launch source by its alphanumeric tokens.
    let tokens = |needle: &str| match &target.launch {
        TaskLaunch::Argv(argv) => argv.iter().any(|arg| {
            arg.split(|c: char| !c.is_ascii_alphanumeric())
                .any(|t| t == needle)
        }),
        TaskLaunch::Shell(command) => command
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|t| t == needle),
    };
    if tokens("codex") {
        "codex"
    } else if tokens("claude") {
        "claude"
    } else if tokens("opencode") {
        "opencode"
    } else if tokens("omp") {
        "omp"
    } else if tokens("pi") {
        "pi"
    } else {
        "other"
    }
}

fn execute_with_presentation_cursor(
    plan: &ReconcilePlan,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    presentation_cursor: &mut PresentationPatchCursor,
    report: &mut UpReport,
    on_canonical_live: &mut dyn FnMut(&agent_spec::spec::AgentSpec),
) {

    // The corpses tied to a launch target (dead, non-keep, active ptys) are reaped inside the launch
    // loop so a parked flapper keeps its evidence. Everything else in `gc` (e.g. a retired agent's
    // dead sessions) is reaped here.
    let launch_ids: HashSet<&str> = plan
        .launch
        .iter()
        .flat_map(|l| l.tasks.iter().map(|t| t.pty_id.as_str()))
        .collect();
    let gc_set: HashSet<&str> = plan.gc.iter().map(String::as_str).collect();

    for id in &plan.gc {
        if launch_ids.contains(id.as_str()) {
            continue; // reaped in the launch loop, gated by the cap
        }
        match runner.remove(id) {
            Ok(()) => report.gc.push(id.clone()),
            Err(e) => report.errors.push(format!("rm {id}: {e}")),
        }
    }

    for launch in &plan.launch {
        let spec_dir = launch.spec.path.parent().unwrap_or_else(|| Path::new("."));
        let policy = launch.spec.restart_policy();
        let launched_agent = launch
            .tasks
            .iter()
            .find(|target| target.name == "agent" && !target.derived);
        let mut agent_available = launched_agent.is_none();
        let ordered_targets = launched_agent.into_iter().chain(
            launch
                .tasks
                .iter()
                .filter(|target| launched_agent.is_none_or(|agent| agent.pty_id != target.pty_id)),
        );
        for target in ordered_targets {
            if target.derived && !agent_available {
                continue;
            }
            let now = Instant::now();
            match cap.decide(&target.pty_id, now, &policy) {
                crate::flapping::RestartDecision::Allow => {}
                crate::flapping::RestartDecision::GaveUp => {
                    // Parked (mode=fail exhausted): surface it, leave the corpse as evidence.
                    if !report.flapping.contains(&target.pty_id) {
                        report.flapping.push(target.pty_id.clone());
                        report.crash_loops.push(CrashLoop {
                            pty_id: target.pty_id.clone(),
                            identity: launch.spec.identity.clone(),
                            host: launch.spec.host.clone(),
                            supervisor: launch.spec.supervisor.clone(),
                        });
                    }
                    if target.name == "agent" && !target.derived {
                        agent_available = false;
                        stop_live_derived_companions(launch, runner, report);
                    }
                    continue;
                }
                // Delaying / RateLimited: transient — skip quietly, retry a later pass, keep the corpse.
                crate::flapping::RestartDecision::Delaying
                | crate::flapping::RestartDecision::RateLimited => {
                    if target.name == "agent" && !target.derived {
                        agent_available = false;
                    }
                    continue;
                }
            }
            // Reap the dead record before st2 starts a replacement. A dead record blocks the
            // replacement. The backend preserves its bounded diagnostics.
            let restarting = gc_set.contains(target.pty_id.as_str());
            if restarting {
                match runner.reap_for_restart(&target.pty_id) {
                    Ok(()) => {
                        crate::metrics::record_task_reap(driver_label(launch, target));
                    }
                    Err(e) => {
                        report
                            .errors
                            .push(format!("reap {} for restart: {e}", target.pty_id));
                        if target.name == "agent" && !target.derived {
                            agent_available = false;
                            stop_live_derived_companions(launch, runner, report);
                        }
                        continue;
                    }
                }
            }
            let spawn_started = Instant::now();
            match runner.spawn(target, spec_dir) {
                Ok(()) => {
                    crate::metrics::record_session_start(
                        spawn_started.elapsed(),
                        driver_label(launch, target),
                    );
                    cap.record(&target.pty_id, now);
                    if restarting {
                        report.restarted.push(target.pty_id.clone());
                    } else {
                        report.launched.push(target.pty_id.clone());
                    }
                    if target.name == "agent" && !target.derived {
                        agent_available = true;
                        // Baseline the canonical seat synchronously at the exact transition that
                        // made it live. A later target may block while its carriers keep changing.
                        on_canonical_live(launch.spec);
                    }
                }
                Err(e) => {
                    report.errors.push(format!("spawn {}: {e}", target.pty_id));
                    if target.name == "agent" && !target.derived {
                        agent_available = false;
                        stop_live_derived_companions(launch, runner, report);
                    }
                }
            }
        }
    }

    // Uptime is what forgives a `mode = fail` budget, so every pass is closed, not only the ones
    // that launched something. The cap is told what the pass PROVED alive (`plan.live`) rather than
    // being left to infer it from what the pass did not launch: this plan may have been narrowed
    // after reconcile (hook gating, flicker debouncing) or built from a reduced spec set (an owner
    // that failed to materialize), and a task dropped that way is unobserved, not healthy. A pass
    // that bailed before `execute` (lock failure, skipped) never gets here and credits nothing —
    // the same safe direction.
    cap.end_pass(Instant::now(), &plan.live);

    let mut failed_retirement_teardowns = HashSet::new();
    for td in &plan.teardown {
        let mut failed = false;
        for id in &td.pty_ids {
            match runner.kill(id) {
                Ok(()) => report.torn_down.push(id.clone()),
                Err(e) => {
                    failed = true;
                    report.errors.push(format!("kill {id}: {e}"));
                }
            }
        }
        if failed {
            failed_retirement_teardowns.insert(td.spec.path.clone());
        }
    }
    for spec in &plan.settle_retirement {
        if failed_retirement_teardowns.contains(&spec.path) {
            continue;
        }
        let agent_dir = spec.path.parent().unwrap_or_else(|| Path::new("."));
        if let Err(error) = crate::message::archive_inbox(agent_dir) {
            report.errors.push(format!(
                "archive retired inbox for {}: {error:#}",
                spec.identity
            ));
        }
    }

    // Presentation never delays lifecycle convergence. Drift repair is bounded to eight sequential
    // children, keeping its worst-case 2s-per-child containment below the 30s supervisor cadence;
    // remaining drift is observed and retried on later passes.
    for presentation in presentation_cursor.batch(&plan.presentation) {
        if let Err(error) = runner.patch_presentation(presentation) {
            report
                .errors
                .push(format!("metadata patch {}: {error}", presentation.pty_id));
        }
    }
    let deferred_presentation = plan
        .presentation
        .len()
        .saturating_sub(MAX_PRESENTATION_PATCHES_PER_PASS);
    if deferred_presentation > 0 {
        report.warnings.push(format!(
            "deferred {deferred_presentation} presentation patches after bounded batch of {MAX_PRESENTATION_PATCHES_PER_PASS}"
        ));
    }

    report
        .adopted
        .extend(plan.adopt.iter().map(|s| s.identity.clone()));
    report.held.extend(plan.held.iter().cloned());
    report
        .other_host
        .extend(plan.other_host.iter().map(|s| s.identity.clone()));
    report
        .unrunnable
        .extend(plan.unrunnable.iter().map(|s| s.identity.clone()));
}

/// The grace window for the liveness debounce (see [`LivenessDebounce`]): a task read not-alive but
/// alive within this window is treated as a transient `pty list` flicker and left alone, not reaped.
/// ~10s comfortably covers a load-induced misread burst while a genuinely-dead task is still reaped
/// only one grace-window late.
const DEBOUNCE_GRACE: Duration = Duration::from_secs(10);

/// Absorbs transient `pty list` misreports so the reconcile loop never destructively GCs a HEALTHY
/// agent (R21c). Under concurrent multi-agent pty load (e.g. an eval run), `pty list --json` can
/// momentarily report a live session as not-running; reconcile would then classify it `Dead` → `pty
/// rm` (destroys the session) + re-launch. This tracks the last time each id was seen ALIVE; a task
/// that reads not-alive but was alive within [`DEBOUNCE_GRACE`] is a flicker — its GC and re-launch
/// are DEFERRED (left alone) until it reads not-alive continuously past the grace (a stable death,
/// which is then reaped normally). It defers BOTH the destructive GC and the noisy "already in use"
/// re-launch. A never-seen task (a genuinely-new launch) is never deferred.
pub struct LivenessDebounce {
    last_alive: HashMap<String, Instant>,
    grace: Duration,
}

impl LivenessDebounce {
    pub fn new(grace: Duration) -> Self {
        Self {
            last_alive: HashMap::new(),
            grace,
        }
    }

    /// Record which ids are alive as of `now`, and forget ids not seen alive within the grace (bounds
    /// memory; a long-dead id past the grace is no longer debounced anyway).
    fn observe(&mut self, sessions: &[Session], now: Instant) {
        for s in sessions {
            if s.alive {
                self.last_alive.insert(s.pty_id.clone(), now);
            }
        }
        self.last_alive
            .retain(|_, &mut t| now.duration_since(t) < self.grace);
    }

    /// True if `id` was seen alive within the grace ending at `now` — a recent flicker, defer it.
    fn recently_alive(&self, id: &str, now: Instant) -> bool {
        self.last_alive
            .get(id)
            .is_some_and(|&t| now.duration_since(t) < self.grace)
    }

    /// Remove recently-alive ids from the plan's GC and launch sets (they're flickers, not real
    /// deaths). Returns the deferred ids for the report. Genuinely-dead (past-grace) and never-seen
    /// (new) tasks are left in the plan and handled normally.
    fn defer_flickers(&self, plan: &mut ReconcilePlan, now: Instant) -> Vec<String> {
        let mut deferred = Vec::new();
        plan.gc.retain(|id| {
            let flicker = self.recently_alive(id, now);
            if flicker {
                deferred.push(id.clone());
            }
            !flicker
        });
        for launch in &mut plan.launch {
            launch.tasks.retain(|t| {
                let flicker = self.recently_alive(&t.pty_id, now);
                if flicker && !deferred.contains(&t.pty_id) {
                    deferred.push(t.pty_id.clone());
                }
                !flicker
            });
        }
        plan.launch.retain(|l| !l.tasks.is_empty());
        deferred
    }
}

/// Specs whose canonical agent seat this pass proved live. Desired state and whole-spec adoption
/// are not evidence: the canonical task itself must have been observed alive or spawned successfully.
fn live_resync_specs(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    sessions: &[Session],
    report: &UpReport,
) -> Vec<agent_spec::spec::AgentSpec> {
    let live_task_ids = sessions
        .iter()
        .filter(|session| session.alive)
        .map(|session| session.pty_id.as_str())
        .chain(report.launched.iter().map(String::as_str))
        .chain(report.restarted.iter().map(String::as_str))
        .collect::<HashSet<_>>();
    specs
        .iter()
        .filter(|spec| {
            spec.tasks.iter().any(|task| {
                if task.name != "agent" {
                    return false;
                }
                let task_id = task
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("{}.{}", spec.bus_id(this_host), task.name));
                live_task_ids.contains(task_id.as_str())
            })
        })
        .cloned()
        .collect()
}

/// One full reconcile pass: discover → list actual → reconcile → execute. On a `pty list` failure the
/// pass is SKIPPED (the error is recorded but nothing is reconciled) — treating a transient list
/// failure as "no sessions" would double-spawn everything. `cap` carries flapping state across passes;
/// `debounce` carries per-id liveness so a transient not-alive flicker isn't destructively reaped.
fn reconcile_pass(
    root: &Path,
    this_host: &str,
    task_context: &TaskCompileContext,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
    presentation_cursor: &mut PresentationPatchCursor,
    resync: Option<&crate::resync::ResyncSupervisor>,
    resource_profiles: Option<&crate::resource_profile_supervisor::ResourceProfileSupervisor>,
) -> UpReport {
    let _catalog_lock = {
        let span = catalog_lock_span();
        let entered = span.as_ref().map(tracing::Span::enter);
        let result = crate::CatalogLock::shared(root);
        finish_child_span(span.as_ref(), result.is_err());
        drop(entered);
        match result {
            Ok(lock) => lock,
            Err(error) => {
                return UpReport {
                    skipped: true,
                    errors: vec![format!(
                        "acquire shared catalog-authoring lock (pass skipped): {error:#}"
                    )],
                    ..Default::default()
                };
            }
        }
    };
    let found = {
        let span = catalog_discover_span();
        let entered = span.as_ref().map(tracing::Span::enter);
        let found = crate::discover(root);
        if let Some(span) = &span {
            span.record("st2.catalog.spec_count", span_count(found.specs.len()));
            span.record("st2.report.warning_count", span_count(found.warnings.len()));
            span.record("st2.report.error_count", span_count(found.errors.len()));
            finish_child_span(Some(span), !found.errors.is_empty());
        }
        drop(entered);
        found
    };
    let mut report = UpReport {
        warnings: found.warnings.clone(),
        errors: found
            .errors
            .iter()
            .map(|e| format!("{}: {}", e.path.display(), e.message))
            .collect(),
        ..Default::default()
    };

    if let Err(error) = crate::reconcile::validate_task_identities(&found.specs, this_host) {
        report.errors.push(error.to_string());
        return report;
    }

    // Verify before touching any Codex workspace. A missing/stale/partial hook set must not rewrite
    // an already-live agent's settings to a nonexistent path. Codex specs remain in reconciliation
    // so live sessions can still be adopted; only their materialization and any new launch defer.
    //
    // pi needs the same verified set, but for its launch only: nothing it renders references
    // `$ST_HOOKS`, while `st2 driver pi-session` cannot start without the channel extension. So a
    // pi agent contributes to this verification without having its materialization deferred.
    let tracer_export_enabled = crate::telemetry::tracer_export_enabled();
    let needs_codex_hooks = crate::hooks::required_by_codex(&found.specs, this_host, root);
    let needs_pi_hooks = if needs_codex_hooks && !tracer_export_enabled {
        false
    } else {
        crate::hooks::required_by_pi(&found.specs, this_host, root)
    };
    let needs_omp_hooks = if (needs_codex_hooks || needs_pi_hooks) && !tracer_export_enabled {
        false
    } else {
        crate::hooks::required_by_omp(&found.specs, this_host, root)
    };
    let needs_hooks = needs_codex_hooks || needs_pi_hooks || needs_omp_hooks;
    let hook_error = {
        let consumer = match (needs_codex_hooks, needs_pi_hooks, needs_omp_hooks) {
            (true, true, true) => "codex+pi+omp",
            (true, true, false) => "codex+pi",
            (true, false, true) => "codex+omp",
            (false, true, true) => "pi+omp",
            (true, false, false) => "codex",
            (false, true, false) => "pi",
            (false, false, true) => "omp",
            (false, false, false) => "none",
        };
        let span = needs_hooks
            .then(|| lifecycle_hooks_span(consumer))
            .flatten();
        let entered = span.as_ref().map(tracing::Span::enter);
        let hook_error = needs_hooks
            .then(crate::hooks::verify_required_set)
            .transpose()
            .err()
            .map(|error| error.to_string());
        finish_child_span(span.as_ref(), hook_error.is_some());
        drop(entered);
        hook_error
    };
    if let Some(error) = &hook_error {
        report.errors.push(format!(
            "verify this binary's lifecycle hooks before harness materialization: {error}; materialization deferred"
        ));
    }
    let materializable_specs = found
        .specs
        .iter()
        .filter(|spec| {
            hook_error.is_none() || !crate::hooks::required_by_codex_agent(spec, this_host, root)
        })
        .cloned()
        .collect::<Vec<_>>();

    // Ordered, idempotent pre-boot materialization, with ownership checked against the complete
    // active fleet even when another gate defers one owner's writes. A gating render failure removes
    // only that agent from this pass; advisory git-exclude failures remain warnings and never block
    // a launch.
    let materialized = {
        let span = catalog_materialize_span("catalog");
        let entered = span.as_ref().map(tracing::Span::enter);
        let materialized = crate::materialize::materialize_catalog_against(
            root,
            &materializable_specs,
            &found.specs,
            this_host,
        );
        if let Some(span) = &span {
            span.record(
                "st2.materialize.failure_count",
                span_count(materialized.failed_agents.len()),
            );
            span.record(
                "st2.report.warning_count",
                span_count(materialized.warnings.len()),
            );
            span.record(
                "st2.report.error_count",
                span_count(materialized.errors.len()),
            );
            finish_child_span(Some(span), !materialized.errors.is_empty());
        }
        drop(entered);
        materialized
    };
    report.warnings.extend(materialized.warnings);
    report.errors.extend(materialized.errors);
    let mut compiled_specs = Vec::new();
    for mut spec in found.specs.iter().cloned() {
        if let Err(error) =
            compile_generated_tasks(std::slice::from_mut(&mut spec), this_host, task_context)
        {
            report.errors.push(format!(
                "compile generated tasks for {}: {error:#}",
                spec.path.display()
            ));
            continue;
        }
        compiled_specs.push(spec);
    }
    let eligible_specs = compiled_specs
        .iter()
        .filter(|spec| !materialized.failed_agents.contains(&spec.bus_id(this_host)))
        .cloned()
        .collect::<Vec<_>>();

    let sessions = {
        let span = runtime_observe_span();
        let entered = span.as_ref().map(tracing::Span::enter);
        let sessions = runner.list_sessions();
        if let (Some(span), Ok(sessions)) = (span.as_ref(), sessions.as_ref()) {
            span.record("st2.runtime.session_count", span_count(sessions.len()));
        }
        finish_child_span(span.as_ref(), sessions.is_err());
        drop(entered);
        match sessions {
            Ok(sessions) => sessions,
            Err(e) => {
                report.skipped = true;
                report
                    .errors
                    .push(format!("list sessions (pass skipped): {e}"));
                return report;
            }
        }
    };
    let now = Instant::now();
    debounce.observe(&sessions, now);
    let mut plan = match crate::reconcile(&eligible_specs, &sessions, this_host) {
        Ok(plan) => plan,
        Err(error) => {
            report.errors.push(error.to_string());
            return report;
        }
    };
    report.deferred = debounce.defer_flickers(&mut plan, now);
    if let Some(resync) = resync {
        for launch in &plan.launch {
            if launch.tasks.iter().any(|task| task.name == "agent") {
                resync.deactivate(launch.spec, this_host);
            }
        }
    }
    if let Some(resource_profiles) = resource_profiles {
        for launch in &plan.launch {
            if launch.tasks.iter().any(|task| task.name == "agent") {
                resource_profiles.deactivate(launch.spec);
            }
        }
    }
    gate_harness_launches_on_hooks(&mut plan, root, &mut report, |_| match &hook_error {
        Some(error) => anyhow::bail!("{error}"),
        None => Ok(()),
    });
    if let Some(resync) = resync {
        // Existing canonical seats are established by this pass's observation. Reinstall their
        // complete catalog-aware sets synchronously before unrelated repairs can block. The
        // targeted upsert retains unchanged baselines and pending transitions, so this is
        // idempotent across steady-state passes.
        for spec in live_resync_specs(&compiled_specs, this_host, &sessions, &report) {
            report
                .warnings
                .extend(resync.install_live(&spec, &found.specs, this_host));
        }
    }
    let mut boundary_warnings = Vec::new();
    let mut install_new_live_seat = |spec: &agent_spec::spec::AgentSpec| {
        if let Some(resync) = resync {
            boundary_warnings.extend(resync.install_live(spec, &found.specs, this_host));
        }
    };
    execute_reconcile(
        &plan,
        runner,
        cap,
        presentation_cursor,
        &mut report,
        &mut install_new_live_seat,
    );
    report.warnings.extend(boundary_warnings);
    if resync.is_some() || resource_profiles.is_some() {
        let loaded = crate::catalog::declared_profile_catalog(root)
            .context("parse resource profiles in catalog.kdl");
        let catalog_profile_error = loaded.is_err();
        let malformed_declarations = found
            .errors
            .iter()
            .map(|error| error.path.clone())
            .filter(|path| {
                !catalog_profile_error || *path != crate::catalog::config_path(root)
            })
            .collect::<Vec<_>>();
        let (config, profiles) = match loaded {
            Ok(loaded) => loaded,
            Err(error) => {
                report.errors.push(format!("{error:#}"));
                (
                    crate::catalog::CatalogConfig::default(),
                    agent_spec::profile::ResourceProfileRegistry::empty(),
                )
            }
        };
        let live_subscription_specs =
            live_resync_specs(&compiled_specs, this_host, &sessions, &report);
        if let Some(resource_profiles) = resource_profiles {
            let generation = match crate::catalog_lock::read_generation_token(root) {
                Ok(generation) => generation,
                Err(error) => {
                    report.errors.push(format!(
                        "read catalog generation for Resource Profiles: {error:#}"
                    ));
                    None
                }
            };
            report.warnings.extend(
                resource_profiles
                    .refresh(&config, &profiles, generation, &live_subscription_specs)
                    .warnings,
            );
        }
        if let Some(resync) = resync {
            let passive = match crate::catalog::passive_profiles(&config, &profiles) {
                Ok(passive) => passive,
                Err(error) => {
                    report.errors.push(format!(
                        "derive passive Resource Profile registry: {error:#}"
                    ));
                    agent_spec::profile::ResourceProfileRegistry::empty()
                }
            };
            report.warnings.extend(resync.refresh_with_profiles(
                passive,
                &found.specs,
                &live_subscription_specs,
                this_host,
                &sessions,
                &malformed_declarations,
            ));
        }
    }
    report
}

/// A missing Codex, pi, or omp agent must not launch against stale lifecycle hooks. Suppress the
/// affected agent launches (including their sidecars) and surface the error when verification fails.
///
/// Workspace trust belongs to the declared provider command and its selected account-specific
/// runtime. Reconciliation deliberately does not mutate an ambient Codex config: an account selector
/// may choose `CODEX_HOME` only after this process launches the command, so such a write would target
/// the wrong state and could not satisfy the launched seat's trust gate.
fn lifecycle_hook_consumer(needs_codex: bool, needs_pi: bool, needs_omp: bool) -> &'static str {
    match (needs_codex, needs_pi, needs_omp) {
        (true, true, true) => "codex+pi+omp",
        (true, true, false) => "codex+pi",
        (true, false, true) => "codex+omp",
        (false, true, true) => "pi+omp",
        (true, false, false) => "codex",
        (false, true, false) => "pi",
        (false, false, true) => "omp",
        (false, false, false) => unreachable!("gated launch has a lifecycle-hook consumer"),
    }
}

fn gate_harness_launches_on_hooks<'a, V>(
    plan: &mut ReconcilePlan<'a>,
    catalog_root: &Path,
    report: &mut UpReport,
    verify_hooks: V,
) where
    V: FnOnce(Option<&'static str>) -> anyhow::Result<()>,
{
    let mut gated_agents = Vec::new();
    let mut needs_codex = false;
    let mut needs_pi = false;
    let mut needs_omp = false;
    for launch in &plan.launch {
        let mut gated = false;
        for target in &launch.tasks {
            if target.name != "agent" {
                continue;
            }
            let invokes_codex = crate::hooks::launch_invokes_codex(&target.launch, catalog_root);
            let invokes_pi = crate::hooks::launch_invokes_pi(&target.launch, catalog_root);
            let invokes_omp = crate::hooks::launch_invokes_omp(&target.launch, catalog_root);
            needs_codex |= invokes_codex;
            needs_pi |= invokes_pi;
            needs_omp |= invokes_omp;
            gated |= invokes_codex || invokes_pi || invokes_omp;
        }
        if gated {
            gated_agents.push(launch.spec.identity.clone());
        }
    }
    if gated_agents.is_empty() {
        return;
    }

    let consumer = crate::telemetry::tracer_export_enabled()
        .then(|| lifecycle_hook_consumer(needs_codex, needs_pi, needs_omp));
    if let Err(error) = verify_hooks(consumer) {
        plan.launch
            .retain(|launch| !gated_agents.contains(&launch.spec.identity));
        report.errors.push(format!(
            "verify lifecycle hooks for new Codex, pi, or omp agent(s) {}: {error}; launch suppressed",
            gated_agents.join(", ")
        ));
    }
}

/// Root span for one reconcile pass. The compatibility name remains `st2.reconcile_pass`;
/// `span.label` and `st2.reconcile.path` distinguish the bounded path enum.
fn reconcile_span(this_host: &str, path: &'static str) -> tracing::Span {
    tracing::info_span!(
        "st2.reconcile_pass",
        "span.label" = path,
        "st2.host" = this_host,
        "st2.reconcile.path" = path,
        "st2.crash_loops" = tracing::field::Empty,
        "st2.unparked" = tracing::field::Empty,
        "st2.report.errors" = tracing::field::Empty,
        "st2.report.warnings" = tracing::field::Empty,
        "st2.reconcile.skipped" = tracing::field::Empty,
        "st2.result" = tracing::field::Empty,
    )
}

fn catalog_lock_span() -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.catalog.lock",
            "span.label" = "shared",
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn catalog_discover_span() -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.catalog.discover",
            "span.label" = "catalog",
            "st2.catalog.spec_count" = tracing::field::Empty,
            "st2.report.warning_count" = tracing::field::Empty,
            "st2.report.error_count" = tracing::field::Empty,
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn lifecycle_hooks_span(consumer: &'static str) -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.hooks.verify",
            "span.label" = "lifecycle hooks",
            "st2.hooks.consumer" = consumer,
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn catalog_materialize_span(label: &'static str) -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.catalog.materialize",
            "span.label" = label,
            "st2.materialize.failure_count" = tracing::field::Empty,
            "st2.report.warning_count" = tracing::field::Empty,
            "st2.report.error_count" = tracing::field::Empty,
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn runtime_observe_span() -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.runtime.observe",
            "span.label" = "all sessions",
            "st2.runtime.session_count" = tracing::field::Empty,
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn reconcile_execute_span() -> Option<tracing::Span> {
    crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.reconcile.execute",
            "span.label" = "apply plan",
            "st2.plan.launch_count" = tracing::field::Empty,
            "st2.plan.gc_count" = tracing::field::Empty,
            "st2.plan.teardown_count" = tracing::field::Empty,
            "st2.report.warning_count" = tracing::field::Empty,
            "st2.report.error_count" = tracing::field::Empty,
            "st2.result" = tracing::field::Empty,
        )
    })
}

fn span_count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn finish_child_span(span: Option<&tracing::Span>, failed: bool) {
    let Some(span) = span else {
        return;
    };
    span.record("st2.result", if failed { "fail" } else { "pass" });
    if failed {
        span.set_status(Status::error(""));
    }
}

fn execute_reconcile(
    plan: &ReconcilePlan,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    presentation_cursor: &mut PresentationPatchCursor,
    report: &mut UpReport,
    on_canonical_live: &mut dyn FnMut(&agent_spec::spec::AgentSpec),
) {
    let span = reconcile_execute_span();
    let before = span
        .as_ref()
        .map(|_| (report.warnings.len(), report.errors.len()));
    let _entered = span.as_ref().map(tracing::Span::enter);
    execute_with_presentation_cursor(
        plan,
        runner,
        cap,
        presentation_cursor,
        report,
        on_canonical_live,
    );
    if let (Some(span), Some((warnings_before, errors_before))) = (span.as_ref(), before) {
        span.record("st2.plan.launch_count", span_count(plan.launch.len()));
        span.record("st2.plan.gc_count", span_count(plan.gc.len()));
        span.record("st2.plan.teardown_count", span_count(plan.teardown.len()));
        span.record(
            "st2.report.warning_count",
            span_count(report.warnings.len().saturating_sub(warnings_before)),
        );
        let added_errors = report.errors.len().saturating_sub(errors_before);
        span.record("st2.report.error_count", span_count(added_errors));
        finish_child_span(Some(span), added_errors > 0);
    }
}

/// Stamp bounded pass outcomes onto the root and emit the deterministic per-pass completion log.
/// Call while the span is entered.
fn finish_reconcile_pass(span: &tracing::Span, report: &UpReport) {
    span.record("st2.crash_loops", span_count(report.crash_loops.len()));
    span.record("st2.unparked", span_count(report.unparked.len()));
    span.record("st2.report.errors", span_count(report.errors.len()));
    span.record("st2.report.warnings", span_count(report.warnings.len()));
    span.record("st2.reconcile.skipped", report.skipped);
    let failed = !report.errors.is_empty();
    let result = if failed { "fail" } else { "pass" };
    span.record("st2.result", result);
    if failed {
        span.set_status(Status::error(""));
    }
    tracing::info!(target: "st2", result, "reconcile pass complete");
}

fn finish_failed_reconcile_pass(span: &tracing::Span) {
    span.record("st2.crash_loops", 0_i64);
    span.record("st2.unparked", 0_i64);
    span.record("st2.report.errors", 1_i64);
    span.record("st2.report.warnings", 0_i64);
    span.record("st2.reconcile.skipped", true);
    span.record("st2.result", "fail");
    span.set_status(Status::error(""));
    tracing::info!(target: "st2", result = "fail", "reconcile pass complete");
}

/// One reconcile pass with a throwaway flapping-cap (`st2 up --once`). Returns an owned report;
/// never `Err` — all failures are collected in `report.errors`. The debounce is throwaway too: a
/// single pass has no prior liveness history, so it defers nothing (correct — one-shot has no flicker).
pub fn up_once(root: &Path, this_host: &str, runner: &dyn Runner) -> anyhow::Result<UpReport> {
    let task_context = TaskCompileContext::current(root.to_path_buf())?;
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    let started = Instant::now();
    let span = reconcile_span(this_host, "catalog");
    let report = {
        let _entered = span.enter();
        let report = reconcile_pass(root,
        this_host,
        &task_context,
        runner,
        &mut FlappingCap::default(),
        &mut debounce,
        &mut PresentationPatchCursor::default(),
        None, None);
        finish_reconcile_pass(&span, &report);
        report
    };
    crate::metrics::record_reconcile_pass(started.elapsed(), !report.errors.is_empty());
    Ok(report)
}

/// Like [`reconcile_pass`] but over IN-MEMORY specs (a single-file st2 spec's team) rather than a
/// discovered catalog — the `st2 up <spec>` path. Same reconcile/execute/flapping/liveness-debounce
/// core; `this_host` filters, `cap`/`debounce` carry state across passes.
/// One supervised pass over an in-memory spec team, carrying `cap`+`debounce` across calls (so a
/// respawn is flicker-tolerant AND flapping-capped). The `st2 up <spec>` loop uses it per interval; the
/// `supervise` eval calls it per wait-tick so a fault-injected dead seat is respawned FROM SPEC exactly
/// once (the carried cap rate-limits a same-episode second respawn; the carried debounce absorbs a
/// transient `pty list` misread of a healthy seat).
pub fn reconcile_pass_specs(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
    reconcile_pass_specs_with_cursor(
        specs,
        this_host,
        runner,
        cap,
        debounce,
        &mut PresentationPatchCursor::default(),
    )
}

pub(crate) fn reconcile_pass_specs_with_cursor(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
    presentation_cursor: &mut PresentationPatchCursor,
) -> UpReport {
    let started = Instant::now();
    let span = reconcile_span(this_host, "spec");
    let mut report = UpReport::default();
    {
        let _entered = span.enter();
        if let Err(error) = crate::reconcile::validate_task_identities(specs, this_host) {
            report.errors.push(error.to_string());
        } else {
            let observe_span = runtime_observe_span();
            let observe_entered = observe_span.as_ref().map(tracing::Span::enter);
            let sessions = runner.list_sessions();
            if let (Some(span), Ok(sessions)) = (observe_span.as_ref(), sessions.as_ref()) {
                span.record("st2.runtime.session_count", span_count(sessions.len()));
            }
            finish_child_span(observe_span.as_ref(), sessions.is_err());
            drop(observe_entered);
            match sessions {
                Ok(sessions) => reconcile_specs_with_sessions_in_span(
                    specs,
                    &sessions,
                    this_host,
                    runner,
                    cap,
                    debounce,
                    presentation_cursor,
                    &mut report,
                ),
                Err(error) => {
                    report.skipped = true;
                    report
                        .errors
                        .push(format!("list sessions (pass skipped): {error}"));
                }
            }
        }
        finish_reconcile_pass(&span, &report);
    }
    crate::metrics::record_reconcile_pass(started.elapsed(), !report.errors.is_empty());
    report
}

/// Reconcile an in-memory team against an already captured session snapshot. Eval supervision uses
/// this so crash classification and reconciliation see the same terminal state: otherwise a clean
/// process can exit between two `pty list` calls, be reaped by the second call, then look like a
/// vanished crash on the next tick. The external snapshot deliberately omits
/// `st2.runtime.observe`; its provenance is outside this pass.
pub(crate) fn reconcile_pass_specs_with_sessions(
    specs: &[agent_spec::spec::AgentSpec],
    sessions: &[Session],
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
    presentation_cursor: &mut PresentationPatchCursor,
) -> UpReport {
    let started = Instant::now();
    let span = reconcile_span(this_host, "spec");
    let mut report = UpReport::default();
    {
        let _entered = span.enter();
        reconcile_specs_with_sessions_in_span(
            specs,
            sessions,
            this_host,
            runner,
            cap,
            debounce,
            presentation_cursor,
            &mut report,
        );
        finish_reconcile_pass(&span, &report);
    }
    crate::metrics::record_reconcile_pass(started.elapsed(), !report.errors.is_empty());
    report
}

fn reconcile_specs_with_sessions_in_span(
    specs: &[agent_spec::spec::AgentSpec],
    sessions: &[Session],
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
    presentation_cursor: &mut PresentationPatchCursor,
    report: &mut UpReport,
) {
    let now = Instant::now();
    debounce.observe(sessions, now);
    match crate::reconcile(specs, sessions, this_host) {
        Ok(mut plan) => {
            report.deferred = debounce.defer_flickers(&mut plan, now);
            execute_reconcile(
                &plan,
                runner,
                cap,
                presentation_cursor,
                report,
                &mut |_| {},
            );
        }
        Err(error) => report.errors.push(error.to_string()),
    }
}

/// One reconcile pass over an in-memory spec team (`st2 up <spec> --once`). Throwaway cap+debounce
/// (a single pass has no flicker history); never `Err` — failures collect in `report.errors`.
pub fn up_once_specs(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
) -> UpReport {
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    reconcile_pass_specs(
        specs,
        this_host,
        runner,
        &mut FlappingCap::default(),
        &mut debounce,
    )
}

/// One bounded task-scoped pass over already-discovered specs. Selector resolution precedes any runner call.
pub fn up_once_selected_specs(
    catalog_root: &Path,
    specs: &[crate::spec::AgentSpec],
    selector: &str,
    this_host: &str,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let span = reconcile_span(this_host, "selected");
    let result = {
        let _entered = span.enter();
        let result = up_once_selected_specs_with_gates(
            catalog_root,
            specs,
            selector,
            this_host,
            runner,
            |consumer| {
                let hook_span = consumer.and_then(lifecycle_hooks_span);
                let hook_entered = hook_span.as_ref().map(tracing::Span::enter);
                let result = crate::hooks::verify_installed().map(|_| ());
                finish_child_span(hook_span.as_ref(), result.is_err());
                drop(hook_entered);
                result
            },
        );
        match &result {
            Ok(report) => finish_reconcile_pass(&span, report),
            Err(_) => finish_failed_reconcile_pass(&span),
        }
        result
    };
    result
}

/// Discover a folder catalog once, resolve one task before any owner hook/render mutation, then
/// materialize only that owner and execute the selected plan.
pub fn up_once_selected(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let span = reconcile_span(this_host, "selected");
    let result = {
        let _entered = span.enter();
        let result = (|| {
            let _catalog_lock = {
                let lock_span = catalog_lock_span();
                let lock_entered = lock_span.as_ref().map(tracing::Span::enter);
                let result = crate::CatalogLock::shared(catalog_root)
                    .context("acquire shared catalog-authoring lock for selected reconcile");
                finish_child_span(lock_span.as_ref(), result.is_err());
                drop(lock_entered);
                result?
            };
            let found = {
                let discover_span = catalog_discover_span();
                let discover_entered = discover_span.as_ref().map(tracing::Span::enter);
                let found = crate::discovery::discover(catalog_root);
                if let Some(span) = &discover_span {
                    span.record("st2.catalog.spec_count", span_count(found.specs.len()));
                    span.record("st2.report.warning_count", span_count(found.warnings.len()));
                    span.record("st2.report.error_count", span_count(found.errors.len()));
                    finish_child_span(Some(span), !found.errors.is_empty());
                }
                drop(discover_entered);
                found
            };
            let (owner, _, _) = crate::reconcile::resolve_task(&found.specs, selector, this_host)?;
            let mut report = UpReport::default();
            report.warnings.extend(found.warnings);
            report.errors.extend(
                found
                    .errors
                    .into_iter()
                    .map(|e| format!("{}: {}", e.path.display(), e.message)),
            );
            if let Err(error) = crate::reconcile::validate_task_identities(&found.specs, this_host)
            {
                report.errors.push(error.to_string());
                return Ok(report);
            }
            let owner = owner.clone();
            if crate::hooks::required_by_codex_agent(&owner, this_host, catalog_root) {
                let hook_span = lifecycle_hooks_span("codex");
                let hook_entered = hook_span.as_ref().map(tracing::Span::enter);
                let verification = crate::hooks::verify_installed();
                finish_child_span(hook_span.as_ref(), verification.is_err());
                drop(hook_entered);
                if let Err(error) = verification {
                    report
                        .errors
                        .push(format!("verify lifecycle hooks: {error}"));
                    return Ok(report);
                }
            }
            let materialized = {
                let materialize_span = catalog_materialize_span("selected owner");
                let materialize_entered = materialize_span.as_ref().map(tracing::Span::enter);
                let materialized = crate::materialize::materialize_catalog_against(
                    catalog_root,
                    std::slice::from_ref(&owner),
                    &found.specs,
                    this_host,
                );
                if let Some(span) = &materialize_span {
                    span.record(
                        "st2.materialize.failure_count",
                        span_count(materialized.failed_agents.len()),
                    );
                    span.record(
                        "st2.report.warning_count",
                        span_count(materialized.warnings.len()),
                    );
                    span.record(
                        "st2.report.error_count",
                        span_count(materialized.errors.len()),
                    );
                    finish_child_span(Some(span), !materialized.errors.is_empty());
                }
                drop(materialize_entered);
                materialized
            };
            report.warnings.extend(materialized.warnings);
            let owner_materialization_failed = !materialized.failed_agents.is_empty();
            report.errors.extend(materialized.errors);
            if owner_materialization_failed {
                return Ok(report);
            }
            let execution = up_once_selected_specs_with_gates(
                catalog_root,
                &found.specs,
                selector,
                this_host,
                runner,
                |_| Ok(()),
            )?;
            report.absorb(execution);
            Ok(report)
        })();
        match &result {
            Ok(report) => finish_reconcile_pass(&span, report),
            Err(_) => finish_failed_reconcile_pass(&span),
        }
        result
    };
    result
}

fn up_once_selected_specs_with_gates<V>(
    catalog_root: &Path,
    specs: &[crate::spec::AgentSpec],
    selector: &str,
    this_host: &str,
    runner: &dyn Runner,
    verify_hooks: V,
) -> anyhow::Result<UpReport>
where
    V: FnOnce(Option<&'static str>) -> anyhow::Result<()>,
{
    crate::reconcile::resolve_task(specs, selector, this_host)?;
    crate::reconcile::validate_task_identities(specs, this_host)?;
    let task_context = TaskCompileContext::current(catalog_root.to_path_buf())?;
    let mut compiled_specs = specs.to_vec();
    compile_generated_tasks(&mut compiled_specs, this_host, &task_context)?;
    let sessions = {
        let observe_span = runtime_observe_span();
        let observe_entered = observe_span.as_ref().map(tracing::Span::enter);
        let sessions = runner.list_sessions();
        if let (Some(span), Ok(sessions)) = (observe_span.as_ref(), sessions.as_ref()) {
            span.record("st2.runtime.session_count", span_count(sessions.len()));
        }
        finish_child_span(observe_span.as_ref(), sessions.is_err());
        drop(observe_entered);
        sessions.map_err(|e| anyhow::anyhow!("list sessions: {e}"))?
    };
    let mut plan =
        crate::reconcile::reconcile_selected(&compiled_specs, &sessions, this_host, selector)?;
    let mut report = UpReport::default();
    gate_harness_launches_on_hooks(&mut plan, catalog_root, &mut report, verify_hooks);
    execute_reconcile(
        &plan,
        runner,
        &mut FlappingCap::default(),
        &mut PresentationPatchCursor::default(),
        &mut report,
        &mut |_| {},
    );
    Ok(report)
}

/// Supervise an in-memory spec team: keep-alive + respawn on a timer, behaving exactly like
/// [`up_loop`] over a catalog (same
/// FlappingCap, LivenessDebounce, crash-loop surfacing, and "stop leaves sessions running"). Timer-only
/// (a spec is one static file — no folder to watch; edit + restart to change it). `root` roots
/// `$CATALOG` + crash-loop surfacing. Runs until SIGINT/SIGTERM.
pub fn up_loop_specs(
    specs: &[agent_spec::spec::AgentSpec],
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    interval: Duration,
    mut on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    install_signal_handler();
    let mut cap = FlappingCap::default();
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    let mut presentation_cursor = PresentationPatchCursor::default();
    let mut reported_flapping: HashSet<String> = HashSet::new();
    let mut recurring_warnings = RecurringWarnings::default();
    let park_channel = ParkChannel::for_supervisor(root, this_host);
    loop {
        let mut pre = UpReport::default();
        park_channel.grant_requests(&mut cap, &mut pre);
        let mut report = reconcile_pass_specs_with_cursor(
            specs,
            this_host,
            runner,
            &mut cap,
            &mut debounce,
            &mut presentation_cursor,
        );
        pre.absorb(report);
        report = pre;
        for id in &report.unparked {
            reported_flapping.remove(id);
        }
        for cl in &report.crash_loops {
            if reported_flapping.insert(cl.pty_id.clone()) {
                // Counted once per park (the initial transition), not per pass: a task stays
                // parked, so per-pass counting would inflate crash_loops_total unboundedly.
                crate::metrics::record_crash_loop();
                tracing::error!(
                    "st2: GAVE UP on '{id}' — crash-looping past its restart{{}} policy (mode=fail); leaving it parked and its last session for inspection. It is reported as parked by `st2 tasks`. Fix the cause, then `st2 unpark {id}` — no supervisor restart needed.",
                    id = cl.pty_id
                );
                surface_crash_loop(root, this_host, cl);
            }
        }
        recurring_warnings.filter(&mut report);
        on_report(&report);
        if STOP.load(Ordering::SeqCst) {
            break;
        }
        // Sleep the interval in 250ms slices so Ctrl-C is responsive (timer-only; no fs-watch).
        let slices = (interval.as_millis() / 250).max(1);
        for _ in 0..slices {
            if STOP.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        if STOP.load(Ordering::SeqCst) {
            break;
        }
    }
    eprintln!(
        "st2: stopping; leaving sessions running (agents are decoupled from the supervisor)."
    );
    crate::event::clear_owner_binding(root, this_host);
    Ok(())
}

/// Explicit teardown (`st2 down`) — kill EVERY live task of this host's catalog agents. This is the
/// one operation that ends tasks (the Nomad model: stopping the supervisor never does). Idempotent:
/// tasks already gone are simply not in the live set. Per-kill errors are collected, never fatal.
pub fn down(root: &Path, this_host: &str, runner: &dyn Runner) -> anyhow::Result<UpReport> {
    let _catalog_lock = crate::CatalogLock::shared(root)
        .context("acquire shared catalog-authoring lock for teardown")?;
    let found = crate::discover(root);
    let mut report = UpReport {
        warnings: found.warnings.clone(),
        errors: found
            .errors
            .iter()
            .map(|e| format!("{}: {}", e.path.display(), e.message))
            .collect(),
        ..Default::default()
    };
    teardown_specs(&found.specs, this_host, runner, &mut report)?;
    Ok(report)
}

/// `st2 down` for a single-file team spec: tear down the declared team's live sessions on this host.
/// The symmetric verb to `up`/`ls` over a spec — the "stop the fleet cleanly" step of the swap runbook.
/// Sessions persist across an `st2 up` supervisor exit (nomad-decoupled), so this is how you actually
/// stop them. `specs` are the already-resolved [`AgentSpec`]s (from `spec_to_agent_specs`).
pub fn down_specs(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let mut report = UpReport::default();
    teardown_specs(specs, this_host, runner, &mut report)?;
    Ok(report)
}

/// Shared teardown core: kill every live task session declared on this host. Task session ids are
/// derived identically to how reconcile spawns them (explicit `task.id`, else `<bus_id>.<task>`), so
/// the catalog `down` and the spec `down_specs` tear down exactly what `up`/`up_*_specs` launched.
fn teardown_specs(
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
    report: &mut UpReport,
) -> anyhow::Result<()> {
    let live: HashSet<String> = runner
        .list_sessions()?
        .into_iter()
        .filter(|s| s.alive)
        .map(|s| s.pty_id)
        .collect();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            report.other_host.push(spec.identity.clone());
            continue;
        }
        let bus_id = spec.bus_id(this_host);
        for task in &spec.tasks {
            let id = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{bus_id}.{}", task.name));
            if live.contains(&id) {
                match runner.kill(&id) {
                    Ok(()) => report.torn_down.push(id),
                    Err(e) => report.errors.push(format!("kill {id}: {e}")),
                }
            }
        }
    }
    Ok(())
}

// ---- The supervisor loop (M3) ----------------------------------------------------------------

/// Set by SIGINT/SIGTERM to break the loop cleanly (agents keep running — they're decoupled).
static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn install_signal_handler() {
    // A plain `signal()` handler is enough to flip an atomic flag; no reentrant work is done here.
    // Cast through a fn pointer (not the zero-sized fn item) to the C handler type.
    let handler = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
}

fn drain(rx: &Receiver<()>) {
    while rx.try_recv().is_ok() {}
}

#[derive(Debug, PartialEq, Eq)]
enum ReconcileWake {
    Change,
    Interval,
    Stop,
}

/// Wait for the next reconciliation trigger: a declaration change, the timer fallback, or a stop.
/// Stop stays responsive in bounded slices; a disconnected watcher channel must never masquerade
/// as a change — that turned the nominal timer fallback into a tight full-catalog reconcile loop.
fn wait_for_reconcile(rx: &Receiver<()>, interval: Duration, stop: &AtomicBool) -> ReconcileWake {
    let deadline = Instant::now() + interval;
    loop {
        if stop.load(Ordering::SeqCst) {
            return ReconcileWake::Stop;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return ReconcileWake::Interval;
        };
        let slice = remaining.min(Duration::from_millis(250));
        match rx.recv_timeout(slice) {
            Ok(()) => {
                drain(rx); // coalesce a burst of events into one pass
                return ReconcileWake::Change;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => std::thread::sleep(slice),
        }
    }
}

/// Best-effort supervisor watch: installation failure is diagnosed once, then reconciliation
/// continues on the timer alone instead of silently losing immediate wakeups forever.
fn best_effort_catalog_watcher(
    root: &Path,
    tx: Sender<()>,
) -> Option<crate::watch::CatalogDeclarationWatcher> {
    match crate::watch::watch_catalog_declarations(root, tx) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            tracing::warn!(
                "st2: cannot watch catalog declarations: {error}; immediate catalog changes are unavailable, continuing with timer polling."
            );
            None
        }
    }
}

/// Suppresses warnings that persist across passes while still re-surfacing one that clears and
/// returns. An unchanged advisory failure (a non-Git workspace failing its git-exclude, say) must
/// be diagnosed once, not once per reconcile pass.
#[derive(Default)]
struct RecurringWarnings {
    emitted: HashSet<String>,
}

impl RecurringWarnings {
    fn filter(&mut self, report: &mut UpReport) {
        let current: HashSet<_> = report.warnings.iter().cloned().collect();
        self.emitted.retain(|warning| current.contains(warning));
        report
            .warnings
            .retain(|warning| self.emitted.insert(warning.clone()));
    }
}

/// The supervisor loop: reconcile on a timer AND on folder changes until interrupted. The fs-watch is
/// best-effort; the `interval` timer is the always-on fallback. `on_report` is called once per pass
pub fn up_loop(
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    interval: Duration,
    on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    install_signal_handler();
    up_loop_until(
        root,
        this_host,
        runner,
        interval,
        &STOP,
        best_effort_catalog_watcher,
        on_report,
    )
}

fn up_loop_until(
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    interval: Duration,
    stop: &AtomicBool,
    install_watcher: impl FnOnce(&Path, Sender<()>) -> Option<crate::watch::CatalogDeclarationWatcher>,
    mut on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    let task_context = TaskCompileContext::current(root.to_path_buf())?;
    let (tx, rx) = channel::<()>();
    let mut watcher = install_watcher(root, tx);
    let mut cap = FlappingCap::default();
    // Carries per-id liveness across passes so a transient `pty list` flicker under load isn't
    // destructively GC'd (R21c). Fresh throwaway in `up_once` — a single pass has no flicker to absorb.
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    let mut presentation_cursor = PresentationPatchCursor::default();

    // Surface each parked crash-loop once (not every pass): an stderr line AND a message to the
    // agent's supervisor over the native bus, so a crash-loop isn't only visible to whoever is
    // watching the log.
    // Profile parsing and stream-owner publication both belong behind the catalog read fence.
    // Defer them together until the first readable pass: an incomplete catalog apply keeps a
    // resident supervisor alive and retrying without exposing declarations or starting runtime I/O.
    // Once initialized, every reconcile pass reloads profiles and atomically replaces the registry
    // with the watch set; malformed later edits install an empty, fail-closed profile set.
    let mut resync = None;
    let mut resource_profiles = None;
    let mut reported_flapping: HashSet<String> = HashSet::new();
    let mut recurring_warnings = RecurringWarnings::default();
    let park_channel = ParkChannel::for_supervisor(root, this_host);

    loop {
        let mut pre = UpReport::default();
        park_channel.grant_requests(&mut cap, &mut pre);
        if resync.is_none() {
            let catalog_lock = match crate::CatalogLock::shared(root) {
                Ok(lock) => lock,
                Err(error) => {
                    pre.skipped = true;
                    pre.errors.push(format!(
                        "acquire shared catalog-authoring lock for resident initialization (pass skipped): {error:#}"
                    ));
                    on_report(&pre);
                    if stop.load(Ordering::SeqCst)
                        || wait_for_reconcile(&rx, interval, stop) == ReconcileWake::Stop
                    {
                        break;
                    }
                    continue;
                }
            };
            let (config, profiles) = crate::catalog::declared_profile_catalog(root)
                .context("parse resource profiles in catalog.kdl")?;
            let passive_profiles = crate::catalog::passive_profiles(&config, &profiles)
                .context("derive passive Resource Profile registry")?;
            crate::event::publish_owner_binding_under_lock(root, this_host, &catalog_lock)
                .context("publish machine-local stream owner binding")?;
            resync = Some(crate::resync::ResyncSupervisor::with_profiles(
                root.to_path_buf(),
                this_host.to_owned(),
                passive_profiles,
            ));
            resource_profiles = Some(
                crate::resource_profile_supervisor::ResourceProfileSupervisor::new(
                    root.to_path_buf(),
                    this_host.to_owned(),
                )?,
            );
        }
        let mut report = {
            let started = Instant::now();
            let span = reconcile_span(this_host, "catalog");
            let pass = {
                let _entered = span.enter();
                let pass = reconcile_pass(root,
                this_host,
                &task_context,
                runner,
                &mut cap,
                &mut debounce,
                &mut presentation_cursor,
                resync.as_ref(),
                resource_profiles.as_ref());
                finish_reconcile_pass(&span, &pass);
                pass
            };
            crate::metrics::record_reconcile_pass(started.elapsed(), !pass.errors.is_empty());
            pass
        };
        pre.absorb(report);
        report = pre;
        if let Some(watcher) = &mut watcher {
            watcher.refresh();
        }
        // A recovered task that crash-loops again is a new crash-loop, so it must be able to surface
        // again. Leaving the id in the dedup set would make every park after the first one silent.
        for id in report.unparked.iter() {
            reported_flapping.remove(id);
        }
        recurring_warnings.filter(&mut report);
        park_channel.publish(&cap, &mut report);
        for cl in &report.crash_loops {
            if reported_flapping.insert(cl.pty_id.clone()) {
                // Counted once per park (the initial transition), not per pass: a task stays
                // parked, so per-pass counting would inflate crash_loops_total unboundedly.
                crate::metrics::record_crash_loop();
                tracing::error!(
                    "st2: GAVE UP on '{id}' — crash-looping past its restart{{}} policy (mode=fail); leaving it parked and its last session for inspection. It is reported as parked by `st2 tasks`. Fix the cause, then `st2 unpark {id}` — no supervisor restart needed.",
                    id = cl.pty_id
                );
                surface_crash_loop(root, this_host, cl);
            }
        }
        on_report(&report);

        if stop.load(Ordering::SeqCst) {
            break;
        }

        // Wait for a declaration change or the timer fallback in stop-responsive slices.
        if wait_for_reconcile(&rx, interval, stop) == ReconcileWake::Stop {
            break;
        }
    }

    eprintln!(
        "st2: stopping; leaving sessions running (agents are decoupled from the supervisor)."
    );
    Ok(())
}

/// Surface a crash-loop to the parked agent's `supervisor` over the native bus (M2.4): a one-shot
/// `crash-loop`-tagged message, so a crash-looping agent isn't only an stderr line the operator has to
/// be watching (the exact miss that let a 45-min outage run). Best-effort — a missing supervisor,
/// an unresolvable supervisor, or a send failure is logged, never fatal. Dedup (once per park) is the
/// caller's job.
pub fn surface_crash_loop(catalog_root: &Path, this_host: &str, cl: &CrashLoop) {
    let agent = cl.agent_bus_id(this_host);
    let Some(supervisor) = cl.supervisor.as_deref() else {
        tracing::warn!(
            "st2: crash-loop '{}' ({agent}) has no supervisor to notify.",
            cl.pty_id
        );
        return;
    };
    let Ok(Some(agent_dir)) = message::resolve_agent_dir(catalog_root, supervisor, this_host)
    else {
        tracing::warn!(
            "st2: crash-loop '{}': supervisor '{supervisor}' not found in the catalog to notify.",
            cl.pty_id
        );
        return;
    };
    let subject = format!("crash-loop: {agent} parked");
    // `st2 unpark` relaunches into the identical failure when the cause is structural, so the
    // notice must not offer it as a recovery verb: acting on that advice restarts the storm. The
    // test is the same predicate admission uses, not the wording of a spawn error.
    let unbindable_socket =
        session_socket_overage(&effective_pty_root(catalog_root), &cl.pty_id);
    let body = match &unbindable_socket {
        Some((socket, over)) => format!(
            "st2 gave up restarting task '{id}' (agent {agent}) — it crash-looped past its \
             restart{{}} policy (mode=fail) and is parked. The cause is structural and `st2 \
             unpark` cannot recover it: the task's session socket path {socket} is {bytes} bytes, \
             exceeding the {limit}-byte portable limit by {over}, so every launch fails the same \
             way. Shorten the identity or task id by at least {over} bytes, or declare a shorter \
             pty root; the declaration has to change before this task can run.",
            id = cl.pty_id,
            socket = socket.display(),
            bytes = PORTABLE_SOCKET_PATH_LIMIT + over,
            limit = PORTABLE_SOCKET_PATH_LIMIT,
        ),
        None => format!(
            "st2 gave up restarting task '{id}' (agent {agent}) — it crash-looped past its \
             restart{{}} policy (mode=fail) and is parked. Its last dead session is left as \
             evidence, and `st2 tasks` reports the park. Investigate the cause, then `st2 unpark \
             {id}` to recover just this task — restarting st2 is not required and would cold-boot \
             every task on the host.",
            id = cl.pty_id
        ),
    };
    let from = format!("st2.{this_host}"); // the runner is the sender
    let tags = ["crash-loop".to_string()];
    if let Err(e) = message::send_to_inbox(
        &message::inbox_dir(&agent_dir),
        &from,
        Some(&subject),
        None,
        &tags,
        &body,
    ) {
        tracing::warn!(
            "st2: failed to notify supervisor '{supervisor}' of crash-loop '{}': {e}",
            cl.pty_id
        );
    }
}

/// Best-effort detection of this machine's short hostname (the catalog's host segment), used as the
/// default reconcile host filter. Falls back to `localhost` if it can't be determined.
pub fn detect_host() -> String {
    // `hostname` is ubiquitous; take the first dotted label (short name, e.g. `hetz`).
    if let Ok(out) = Command::new("hostname").output()
        && out.status.success()
    {
        let full = String::from_utf8_lossy(&out.stdout);
        if let Some(short) = full.trim().split('.').next()
            && !short.is_empty()
        {
            return short.to_string();
        }
    }
    "localhost".to_string()
}

mod tests {
    use super::*;
    use agent_spec::spec::{
        AgentSpec, Driver, JobType, OmpDriver, Task, TaskKind, TaskLifecycle,
    };
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::OsStr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::mpsc;


    /// The bound is derived from the resolved pty root, never a fixed maximum identity length.
    ///
    /// `pty` binds `<PTY_ROOT>/<session-id>.sock`, so the separator plus the five-byte suffix is
    /// the fixed overhead and the usable identity length is whatever remains of the limit. These
    /// numbers are measured against the pty binary itself: with a 21-byte root it accepts a
    /// 77-byte id and refuses a 78-byte one as "a socket path of 105 bytes, which exceeds the
    /// 104-byte kernel limit by 1".
    #[test]
    fn session_socket_overage_is_derived_from_the_resolved_root() {
        let short_root = Path::new("/tmp/ptyprobe-1960953");
        let fits = "a".repeat(77);
        let over = "a".repeat(78);

        assert_eq!(
            session_socket_path(short_root, &fits)
                .as_os_str()
                .as_encoded_bytes()
                .len(),
            PORTABLE_SOCKET_PATH_LIMIT,
            "the accepted id must land exactly on the limit"
        );
        assert!(session_socket_overage(short_root, &fits).is_none());

        let (path, overage) =
            session_socket_overage(short_root, &over).expect("one byte over is refused");
        assert_eq!(overage, 1);
        assert_eq!(path, short_root.join(format!("{over}.sock")));

        // A deeper root shrinks every identity's budget on that host: the same id that fitted
        // above is now 26 bytes over.
        let deep_root = Path::new("/home/user/.local/state/st2/default/catalog/pty");
        assert_eq!(
            session_socket_overage(deep_root, &fits).map(|(_, over)| over),
            Some(26)
        );
    }

    #[cfg(target_os = "linux")]
    fn linux_process_state(pid: i32) -> Option<char> {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()?
            .rsplit_once(") ")?
            .1
            .chars()
            .next()
    }

    /// Block until the fixture publishes `marker`, which its script creates by an atomic rename so
    /// the barrier never observes a half-written file. Called from `on_spawn`, which runs before
    /// [`run_captured`] starts the child deadline: fork+exec scheduling is therefore paid here and
    /// not out of the deadline the test then measures. The ceiling is deliberately far larger than
    /// any plausible fork+exec — it bounds a fixture that never ran at all, and is not itself the
    /// behaviour under test, so a loaded host cannot turn it into a failure.
    fn await_fixture_ready(pid: i32, marker: &Path, what: &str) {
        const CEILING: Duration = Duration::from_secs(30);
        let deadline = Instant::now() + CEILING;
        while !marker.exists() {
            if Instant::now() >= deadline {
                // Do not leak the fixture's long sleeper into the test host on the way out.
                unsafe {
                    libc::kill(-pid, libc::SIGKILL);
                    libc::kill(pid, libc::SIGKILL);
                }
                panic!(
                    "{what} within {CEILING:?}: {} never appeared",
                    marker.display()
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn process_can_retain_cleanup_resources(pid: i32) -> bool {
        #[cfg(target_os = "linux")]
        if linux_process_state(pid) == Some('Z') {
            return false;
        }
        crate::host_lock::process_alive(pid)
    }

    fn target(id: &str, cmd: &str) -> TaskTarget {
        TaskTarget {
            kind: TaskKind::Pty,
            pty_id: id.to_string(),
            bus_id: "hetz.demo".to_string(),
            name: "agent".to_string(),
            derived: false,
            launch: TaskLaunch::Shell(cmd.to_string()),
            cwd: None,
            workspace: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
            presentation: None,
        }
    }

    struct GateRunner {
        list_calls: Cell<usize>,
    }

    impl Runner for GateRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            self.list_calls.set(self.list_calls.get() + 1);
            Ok(Vec::new())
        }

        fn spawn(&self, _target: &TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            panic!("gate runner must not spawn")
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            panic!("gate runner must not kill")
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            panic!("gate runner must not remove")
        }
    }

    #[derive(Default)]
    struct PersistentPatchRunner {
        patched: RefCell<Vec<String>>,
    }

    impl Runner for PersistentPatchRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            unreachable!("presentation execution does not list sessions")
        }

        fn spawn(&self, _target: &TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            unreachable!("presentation-only plan must not spawn")
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            unreachable!("presentation-only plan must not kill")
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            unreachable!("presentation-only plan must not remove")
        }

        fn patch_presentation(&self, presentation: &PtyPresentation) -> anyhow::Result<()> {
            self.patched.borrow_mut().push(presentation.pty_id.clone());
            if presentation.pty_id.as_str() < "host.presented.08" {
                anyhow::bail!("simulated persistent metadata failure");
            }
            Ok(())
        }
    }

    #[test]
    fn bounded_presentation_batches_are_deterministic_and_do_not_starve() {
        let plan = ReconcilePlan {
            presentation: (0..12)
                .rev()
                .map(|index| PtyPresentation {
                    pty_id: format!("host.presented.{index:02}"),
                    display_name: None,
                    tags: BTreeMap::new(),
                })
                .collect(),
            ..ReconcilePlan::default()
        };
        let runner = PersistentPatchRunner::default();
        let mut cap = FlappingCap::default();
        let mut cursor = PresentationPatchCursor::default();

        for _ in 0..2 {
            execute_with_presentation_cursor(
                &plan,
                &runner,
                &mut cap,
                &mut cursor,
                &mut UpReport::default(),
                &mut |_| {},
            );
        }

        let attempted = runner.patched.borrow();
        assert_eq!(
            &attempted[..8],
            &(0..8)
                .map(|index| format!("host.presented.{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(attempted.len(), 16);
        for index in 8..12 {
            assert!(attempted.contains(&format!("host.presented.{index:02}")));
        }
    }

    #[test]
    fn lifecycle_hook_consumer_is_a_closed_enum() {
        let consumers = [
            lifecycle_hook_consumer(true, false, false),
            lifecycle_hook_consumer(false, true, false),
            lifecycle_hook_consumer(false, false, true),
            lifecycle_hook_consumer(true, true, false),
            lifecycle_hook_consumer(true, false, true),
            lifecycle_hook_consumer(false, true, true),
            lifecycle_hook_consumer(true, true, true),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(
            consumers,
            BTreeSet::from([
                "codex",
                "pi",
                "omp",
                "codex+pi",
                "codex+omp",
                "pi+omp",
                "codex+pi+omp",
            ])
        );
    }

    #[test]
    fn selected_codex_gate_suppresses_launch_on_stale_hooks() {
        let spec = AgentSpec {
            identity: "codex".into(),
            name: None,
            description: None,
            host: None,
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            desired_state: crate::AgentDesiredState::Running,
            keep: false,
            restart: None,
            delivery: None,
            session_driver: None,
            driver: None,
            delivery_readiness: None,
            resources: vec![],
            streams: Vec::new(),
            tasks: vec![Task {
                kind: TaskKind::Pty,
                derived: false,
                name: "agent".into(),
                id: Some("test.codex.agent".into()),
                command: None,
                argv: Some(vec!["$CATALOG/bin/codex".into(), "--version".into()]),
                cwd: None,
                tags: BTreeMap::new(),
                env: BTreeMap::new(),
                keep: false,
                lifecycle: TaskLifecycle::Service,
            }],
            path: "/tmp/spec.kdl".into(),
        };
        let runner = GateRunner {
            list_calls: Cell::new(0),
        };
        let report = up_once_selected_specs_with_gates(
            Path::new("/tmp"),
            &[spec],
            "test.codex.agent",
            "test",
            &runner,
            |consumer| {
                assert_eq!(consumer, None);
                anyhow::bail!("stale receipt")
            },
        )
        .unwrap();
        assert_eq!(runner.list_calls.get(), 1);
        assert!(report.launched.is_empty());
        assert!(report.errors.iter().any(|error| {
            error.contains("stale receipt") && error.contains("launch suppressed")
        }));
    }

    #[test]
    fn selected_identity_conflict_refuses_before_hook_verification_or_inventory() {
        let mut spec = AgentSpec {
            identity: "codex".into(),
            name: None,
            description: None,
            host: None,
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            desired_state: crate::AgentDesiredState::Running,
            keep: false,
            restart: None,
            delivery: None,
            session_driver: None,
            driver: None,
            delivery_readiness: None,
            resources: vec![],
            streams: Vec::new(),
            tasks: vec![Task {
                kind: TaskKind::Pty,
                derived: false,
                name: "agent".into(),
                id: Some("test.codex.agent".into()),
                command: None,
                argv: Some(vec!["$CATALOG/bin/codex".into(), "--version".into()]),
                cwd: None,
                tags: BTreeMap::new(),
                env: BTreeMap::new(),
                keep: false,
                lifecycle: TaskLifecycle::Service,
            }],
            path: "/tmp/spec.kdl".into(),
        };
        spec.tasks[0]
            .env
            .insert("ST_AGENT".into(), "wrong.actor".into());
        let runner = GateRunner {
            list_calls: Cell::new(0),
        };
        let verify_calls = Cell::new(0);

        let error = up_once_selected_specs_with_gates(
            Path::new("/tmp"),
            &[spec],
            "test.codex.agent",
            "test",
            &runner,
            |_| {
                verify_calls.set(verify_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("conflicting ST_AGENT"));
        assert_eq!(verify_calls.get(), 0);
        assert_eq!(runner.list_calls.get(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn idle_supervisor_does_not_spin_on_its_own_catalog_reads() {
        let catalog = tempfile::tempdir().unwrap();
        let stop = AtomicBool::new(false);
        let mut passes = 0usize;

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(350));
                stop.store(true, Ordering::SeqCst);
            });
            up_loop_until(
                catalog.path(),
                "test-host",
                &GateRunner {
                    list_calls: Cell::new(0),
                },
                Duration::from_secs(60),
                &stop,
                best_effort_catalog_watcher,
                |_| passes += 1,
            )
            .unwrap();
        });

        assert!(
            passes <= 2,
            "idle supervisor must wait instead of reconciling its own read events: {passes} passes"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_watch_installation_keeps_supervisor_on_timer_cadence() {
        let catalog = tempfile::tempdir().unwrap();
        let agent = catalog.path().join("agents/test-host/live");
        std::fs::create_dir_all(&agent).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "test-host"; command "x" }"#,
        )
        .unwrap();
        let stop = AtomicBool::new(false);
        let mut passes = 0usize;
        let (started_tx, started_rx) = mpsc::sync_channel(1);

        std::thread::scope(|scope| {
            let stop = &stop;
            scope.spawn(move || {
                started_rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(350));
                stop.store(true, Ordering::SeqCst);
            });
            up_loop_until(
                catalog.path(),
                "test-host",
                &SpawnCountingRunner::default(),
                Duration::from_millis(100),
                &stop,
                |_, _| None, // watcher installation fails, as it did on dev3's oversized catalog
                |_| {
                    passes += 1;
                    let _ = started_tx.try_send(());
                },
            )
            .unwrap();
        });

        assert!(
            (2..=6).contains(&passes),
            "a disconnected watcher channel must fall back to timer cadence, not spin: \
             {passes} passes in ~350ms at a 100ms interval"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervisor_still_wakes_on_declaration_change_with_live_watcher() {
        let catalog = tempfile::tempdir().unwrap();
        let agent = catalog.path().join("agents/test-host/live");
        std::fs::create_dir_all(&agent).unwrap();
        let spec = agent.join("agent.kdl");
        std::fs::write(&spec, r#"agent "live" { host "test-host"; command "x" }"#).unwrap();
        let stop = AtomicBool::new(false);
        let passes = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = passes.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let started = Instant::now();

        std::thread::scope(|scope| {
            scope.spawn(move || {
                started_rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(200));
                std::fs::write(
                    &spec,
                    r#"agent "live" { host "test-host"; command "changed" }"#,
                )
                .unwrap();
            });
            scope.spawn({
                let stop = &stop;
                let passes = &passes;
                move || {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while passes.load(Ordering::SeqCst) < 2
                        && !stop.load(Ordering::SeqCst)
                        && Instant::now() < deadline
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    stop.store(true, Ordering::SeqCst);
                }
            });
            up_loop_until(
                catalog.path(),
                "test-host",
                &SpawnCountingRunner::default(),
                Duration::from_secs(60),
                &stop,
                best_effort_catalog_watcher,
                |_| {
                    passes.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.try_send(());
                },
            )
            .unwrap();
        });

        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a declaration mutation must wake the supervisor long before the 60s timer"
        );
        assert!(observed.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn resident_loop_reloads_added_changed_removed_and_malformed_profiles() {
        let catalog = tempfile::tempdir().unwrap();
        let agent = catalog.path().join("agents/test-host/live");
        std::fs::create_dir_all(&agent).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" {
  host "test-host"
  command "true"
  resource "alpha" uri="alpha://test-host/live" reason="Alpha."
  resource "beta" uri="beta://test-host/live" reason="Beta."
}"#,
        )
        .unwrap();
        let missing = catalog.path().join("missing.wasm");
        let profile = |scheme: &str| {
            format!(
                "profile {scheme:?} {{ wasm {:?} }}\n",
                missing.display().to_string()
            )
        };
        let config = crate::catalog::config_path(catalog.path());
        let runner = SpawnCountingRunner::default();
        runner
            .sessions
            .borrow_mut()
            .push(sess("test-host.live.agent", true));
        let stop = AtomicBool::new(false);
        let mut reports = Vec::new();

        up_loop_until(
            catalog.path(),
            "test-host",
            &runner,
            Duration::from_millis(5),
            &stop,
            |_, _| None,
            |report| {
                let pass = reports.len();
                reports.push((report.warnings.clone(), report.errors.clone()));
                match pass {
                    0 => std::fs::write(&config, profile("alpha")).unwrap(),
                    1 => std::fs::write(&config, profile("beta")).unwrap(),
                    2 => std::fs::write(&config, "").unwrap(),
                    3 => stop.store(true, Ordering::SeqCst),
                    _ => unreachable!("profile removal run stops after four passes"),
                }
            },
        )
        .unwrap();

        let profile_warnings = |reports: &Vec<(Vec<String>, Vec<String>)>, pass: usize| {
            reports[pass]
                .0
                .iter()
                .filter(|warning| warning.contains("resync profile"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert!(
            profile_warnings(&reports, 0).is_empty(),
            "no profile is initially declared"
        );
        assert!(
            profile_warnings(&reports, 1)
                .iter()
                .any(|warning| warning.contains("resource 'alpha'")),
            "an added profile takes effect: {:?}",
            reports[1]
        );
        assert!(
            profile_warnings(&reports, 2)
                .iter()
                .any(|warning| warning.contains("resource 'beta'"))
                && !profile_warnings(&reports, 2)
                    .iter()
                    .any(|warning| warning.contains("resource 'alpha'")),
            "changing definitions replaces the registry: {:?}",
            reports[2]
        );
        assert!(
            profile_warnings(&reports, 3).is_empty(),
            "removing every profile removes the old resolution semantics: {:?}",
            reports[3]
        );

        // A separate resident lifetime starts valid, then makes the envelope malformed. The
        // initial hard parse still accepts the valid declaration; the later edit must clear its
        // active semantics rather than silently carrying them forward.
        std::fs::write(&config, profile("alpha")).unwrap();
        stop.store(false, Ordering::SeqCst);
        let mut malformed_reports = Vec::new();
        up_loop_until(
            catalog.path(),
            "test-host",
            &runner,
            Duration::from_millis(5),
            &stop,
            |_, _| None,
            |report| {
                let pass = malformed_reports.len();
                malformed_reports.push((report.warnings.clone(), report.errors.clone()));
                match pass {
                    0 => std::fs::write(
                        &config,
                        r#"profiel "alpha" { wasm "missing.wasm" }"#,
                    )
                    .unwrap(),
                    1 => stop.store(true, Ordering::SeqCst),
                    _ => unreachable!("malformed profile run stops after two passes"),
                }
            },
        )
        .unwrap();
        assert!(
            profile_warnings(&malformed_reports, 0)
                .iter()
                .any(|warning| warning.contains("resource 'alpha'")),
            "the profile is active before the malformed edit: {:?}",
            malformed_reports[0]
        );
        assert!(
            malformed_reports[1]
                .1
                .iter()
                .any(|error| error.contains("unknown catalog.kdl top-level node 'profiel'"))
                && profile_warnings(&malformed_reports, 1).is_empty(),
            "malformed catalog state is reported and fails closed instead of retaining alpha: {:?}",
            malformed_reports[1]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn disconnected_watcher_channel_waits_out_the_interval_instead_of_spinning() {
        let (_tx, rx) = channel::<()>();
        let started = Instant::now();
        assert_eq!(
            wait_for_reconcile(&rx, Duration::from_millis(80), &AtomicBool::new(false)),
            ReconcileWake::Interval,
            "disconnection must be treated as silence, not as a change"
        );
        assert!(started.elapsed() >= Duration::from_millis(75));
    }

    // ── liveness debounce (R21c): a transient `pty list` not-alive flicker under load must not
    //    destructively GC/relaunch a HEALTHY agent; a stable death must still be reaped ──────────────

    use crate::reconcile::Launch;
    fn sess(id: &str, alive: bool) -> Session {
        Session {
            pty_id: id.to_string(),
            alive,
            exit_code: None,
            presentation: None,
        }
    }

    /// Records spawns and reports every launch as succeeding, so a pass can be driven repeatedly.
    #[derive(Default)]
    struct SpawnCountingRunner {
        sessions: RefCell<Vec<Session>>,
        spawned: RefCell<Vec<String>>,
    }

    impl Runner for SpawnCountingRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            Ok(self.sessions.borrow().clone())
        }

        fn spawn(&self, target: &TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            self.spawned.borrow_mut().push(target.pty_id.clone());
            Ok(())
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn patch_presentation(&self, _presentation: &PtyPresentation) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// `execute` must close every pass, or a task that recovers is never forgiven and eventually
    /// parks even though it is healthy — the opposite of the crash-loop bug the cap exists for.
    ///
    /// The unit tests in `flapping.rs` call `end_pass` by hand, so they cannot catch it never being
    /// called from a reconcile pass. This one drives the real `execute` path. `interval = 0s` makes
    /// any survived pass count as recovery, keeping the test free of wall-clock sleeping.
    #[test]
    fn execute_closes_each_pass_so_a_recovered_task_regains_its_fail_budget() {
        let mut spec = spec_fixture();
        spec.restart = Some(agent_spec::spec::Restart {
            attempts: 3,
            interval: Duration::from_secs(0),
            delay: Duration::from_secs(0),
            mode: agent_spec::spec::RestartMode::Fail,
        });
        let runner = SpawnCountingRunner::default();
        let mut cap = FlappingCap::default();

        fn dying(spec: &AgentSpec) -> ReconcilePlan<'_> {
            ReconcilePlan {
                launch: vec![Launch {
                    spec,
                    tasks: vec![target("hetz.demo.agent", "x")],
                    live_derived: Vec::new(),
                }],
                ..ReconcilePlan::default()
            }
        }

        // Two failing passes: two of three launches spent.
        for _ in 0..2 {
            execute(&dying(&spec), &runner, &mut cap, &mut UpReport::default());
        }
        assert_eq!(runner.spawned.borrow().len(), 2, "two launches spent");

        // A pass that launches nothing because it found the task alive. That observation — not the
        // empty launch set — is what forgives the budget.
        execute(
            &ReconcilePlan {
                live: vec!["hetz.demo.agent".to_string()],
                ..ReconcilePlan::default()
            },
            &runner,
            &mut cap,
            &mut UpReport::default(),
        );

        // Having recovered, it gets the full budget back: three more launches, then parked. Without
        // the pass being closed it would park after only one more.
        let mut last = UpReport::default();
        for _ in 0..4 {
            last = UpReport::default();
            execute(&dying(&spec), &runner, &mut cap, &mut last);
        }
        assert_eq!(
            runner.spawned.borrow().len(),
            5,
            "recovery must restore the full `attempts` budget, not leave it partly spent"
        );
        assert_eq!(
            last.flapping,
            vec!["hetz.demo.agent".to_string()],
            "and it still parks in the end"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn persistent_advisory_warnings_surface_once_not_per_pass() {
        let catalog = tempfile::tempdir().unwrap();
        let agent = catalog.path().join("agents/test-host/live");
        std::fs::create_dir_all(&agent).unwrap();
        std::fs::create_dir_all(catalog.path().join("workspace")).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" {
  host "test-host"
  command "true"
  workspace "$CATALOG/workspace"
  render { git-exclude "scratch.txt" }
}"#,
        )
        .unwrap();
        let stop = AtomicBool::new(false);
        let mut passes = 0usize;
        let mut warnings_seen = 0usize;
        let (started_tx, started_rx) = mpsc::sync_channel(1);

        std::thread::scope(|scope| {
            let stop = &stop;
            scope.spawn(move || {
                started_rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(300));
                stop.store(true, Ordering::SeqCst);
            });
            up_loop_until(
                catalog.path(),
                "test-host",
                &SpawnCountingRunner::default(),
                Duration::from_millis(50),
                &stop,
                |_, _| None,
                |report| {
                    passes += 1;
                    warnings_seen += report.warnings.len();
                    let _ = started_tx.try_send(());
                },
            )
            .unwrap();
        });

        assert!(
            passes >= 3,
            "the loop must have run several passes for this to say anything: {passes}"
        );
        assert_eq!(
            warnings_seen, 1,
            "an unchanged advisory failure must be diagnosed once across {passes} passes"
        );
    }
    /// A pass can execute a plan the task was never in: `up_once` drops an owner whose
    /// materialization failed, `gate_harness_launches_on_hooks` strips gated launches, and
    /// `defer_flickers` removes debounced ones — each after the pass is already committed to
    /// running. Silence about a task is not evidence it is alive, and crediting uptime for it lets
    /// a permanently-dead task refill its budget on every gated pass and never park. Identical to
    /// the recovery test above except that the quiet pass does not report the task live.
    #[test]
    fn a_pass_that_omits_a_task_does_not_credit_it_with_uptime() {
        let mut spec = spec_fixture();
        spec.restart = Some(agent_spec::spec::Restart {
            attempts: 3,
            interval: Duration::from_secs(0),
            delay: Duration::from_secs(0),
            mode: agent_spec::spec::RestartMode::Fail,
        });
        let runner = SpawnCountingRunner::default();
        let mut cap = FlappingCap::default();

        fn dying(spec: &AgentSpec) -> ReconcilePlan<'_> {
            ReconcilePlan {
                launch: vec![Launch {
                    spec,
                    tasks: vec![target("hetz.demo.agent", "x")],
                    live_derived: Vec::new(),
                }],
                ..ReconcilePlan::default()
            }
        }

        // Two failing passes: two of three launches spent.
        for _ in 0..2 {
            execute(&dying(&spec), &runner, &mut cap, &mut UpReport::default());
        }
        assert_eq!(runner.spawned.borrow().len(), 2, "two launches spent");

        // The task is dropped from this pass — not launched, and not observed alive either.
        execute(
            &ReconcilePlan::default(),
            &runner,
            &mut cap,
            &mut UpReport::default(),
        );

        // The budget must be where the failures left it: one launch remains, then it parks.
        let mut last = UpReport::default();
        for _ in 0..4 {
            last = UpReport::default();
            execute(&dying(&spec), &runner, &mut cap, &mut last);
        }
        assert_eq!(
            runner.spawned.borrow().len(),
            3,
            "an unobserved pass must not forgive the failure budget"
        );
        assert_eq!(
            last.flapping,
            vec!["hetz.demo.agent".to_string()],
            "and the task must still park"
        );
    }

    fn spec_fixture() -> AgentSpec {
        AgentSpec {
            identity: "demo".into(),
            name: None,
            description: None,
            host: Some("hetz".into()),
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            desired_state: crate::AgentDesiredState::Running,
            keep: false,
            restart: None,
            delivery: None,
            session_driver: None,
            driver: None,
            delivery_readiness: None,
            resources: vec![],
            streams: Vec::new(),
            tasks: vec![],
            path: std::path::PathBuf::from("/x"),
        }
    }

    #[test]
    fn driver_labels_include_typed_and_argv_omp_but_remain_bounded() {
        let legacy_spec = spec_fixture();
        let legacy_launch = Launch {
            spec: &legacy_spec,
            tasks: Vec::new(),
            live_derived: Vec::new(),
        };
        let mut omp_argv = target("hetz.demo.agent", "unused");
        omp_argv.launch = TaskLaunch::Argv(vec![
            "st2".into(),
            "driver".into(),
            "omp-session".into(),
        ]);
        let mut exec = target("hetz.demo.agent", "codex");
        exec.kind = TaskKind::Exec;
        let targets = [
            target("hetz.demo.agent", "codex"),
            target("hetz.demo.agent", "claude"),
            target("hetz.demo.agent", "opencode"),
            target("hetz.demo.agent", "pi"),
            omp_argv,
            exec,
            target("hetz.demo.agent", "unrecognized"),
        ];
        let labels = targets
            .iter()
            .map(|target| driver_label(&legacy_launch, target))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            labels,
            BTreeSet::from(["codex", "claude", "opencode", "pi", "omp", "exec", "other"])
        );

        let mut typed_spec = spec_fixture();
        typed_spec.driver = Some(Driver::Omp(OmpDriver {
            model: None,
            effort: None,
            prompt: String::new(),
            args: Vec::new(),
        }));
        let typed_launch = Launch {
            spec: &typed_spec,
            tasks: Vec::new(),
            live_derived: Vec::new(),
        };
        assert_eq!(
            driver_label(&typed_launch, &target("hetz.demo.agent", "claude")),
            "omp",
            "typed driver identity must take precedence over argv heuristics"
        );
    }

    #[test]
    fn resync_watch_eligibility_requires_a_proven_live_agent_seat() {
        let spec = |identity: &str, explicit_id: Option<&str>| {
            let mut spec = spec_fixture();
            spec.identity = identity.to_owned();
            spec.tasks = vec![Task {
                kind: TaskKind::Pty,
                derived: false,
                name: "agent".into(),
                id: explicit_id.map(str::to_owned),
                command: Some("agent".into()),
                argv: None,
                cwd: None,
                tags: BTreeMap::new(),
                env: BTreeMap::new(),
                keep: false,
                lifecycle: TaskLifecycle::Service,
            }];
            spec
        };
        let specs = vec![
            spec("desired", None),
            spec("dead-adopted", None),
            spec("observed-live", None),
            spec("launched", None),
            spec("restarted", Some("custom-seat")),
        ];
        let sessions = vec![
            sess("hetz.dead-adopted.agent", false),
            // A live canonical seat remains eligible even when a missing companion means the
            // whole spec was not adopted and the companion later fails to launch.
            sess("hetz.observed-live.agent", true),
        ];
        let report = UpReport {
            adopted: vec!["dead-adopted".into()],
            launched: vec![
                "hetz.launched.agent".into(),
                // A successfully launched companion is not evidence of a live agent seat.
                "hetz.desired.ding".into(),
            ],
            restarted: vec!["custom-seat".into()],
            ..UpReport::default()
        };

        let eligible = live_resync_specs(&specs, "hetz", &sessions, &report)
            .into_iter()
            .map(|spec| spec.identity)
            .collect::<Vec<_>>();
        assert_eq!(eligible, vec!["observed-live", "launched", "restarted"]);
    }

    struct BlockingLaunchRunner {
        sessions: RefCell<Vec<Session>>,
        fail_id: Option<String>,
        block_id: String,
        entered: mpsc::SyncSender<()>,
        release: RefCell<mpsc::Receiver<()>>,
    }

    impl Runner for BlockingLaunchRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            Ok(self.sessions.borrow().clone())
        }

        fn spawn(&self, target: &TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            if self.fail_id.as_deref() == Some(&target.pty_id) {
                anyhow::bail!("simulated launch failure");
            }
            if target.pty_id == self.block_id {
                self.entered.send(()).unwrap();
                self.release.borrow_mut().recv().unwrap();
            }
            Ok(())
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    struct SteadyChainRunner {
        sessions: std::sync::Mutex<Vec<Session>>,
        block_id: String,
        entered: mpsc::SyncSender<()>,
        release: std::sync::Mutex<mpsc::Receiver<()>>,
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    impl Runner for SteadyChainRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            Ok(self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        fn spawn(&self, target: &TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            if target.pty_id == self.block_id {
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .recv()
                    .unwrap();
            }
            self.sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(sess(&target.pty_id, true));
            Ok(())
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn write_resync_agent(catalog: &Path, identity: &str) -> (PathBuf, PathBuf) {
        let agent_dir = catalog.join("agents/hetz").join(identity);
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            format!(
                r#"agent "{identity}" {{
  host "hetz"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}}"#
            ),
        )
        .unwrap();
        let goal = resources.join("goal.md");
        std::fs::write(&goal, "before\n").unwrap();
        (agent_dir, goal)
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn write_notify_chain_profile(catalog: &Path) {
        let resolver_dir = catalog.join("resolvers");
        std::fs::create_dir_all(&resolver_dir).unwrap();
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("crates/agent-spec/tests/fixtures/demo_resolver.wasm"),
            resolver_dir.join("goal.wasm"),
        )
        .unwrap();
        std::fs::write(
            crate::catalog::config_path(catalog),
            r#"profile "dev.schickling.agent-goal" {
  wasm "resolvers/goal.wasm"
  class "immediate"
  notify-chain #true
}
"#,
        )
        .unwrap();
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn write_notify_chain_agent(
        catalog: &Path,
        identity: &str,
        supervisor: Option<&str>,
        later_task: bool,
    ) -> (PathBuf, PathBuf) {
        write_notify_chain_agent_with_state(catalog, identity, supervisor, later_task, None)
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn write_notify_chain_agent_with_state(
        catalog: &Path,
        identity: &str,
        supervisor: Option<&str>,
        later_task: bool,
        desired_state: Option<&str>,
    ) -> (PathBuf, PathBuf) {
        let agent_dir = catalog.join("agents/hetz").join(identity);
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        let supervisor = supervisor
            .map(|supervisor| format!("  supervisor {supervisor:?}\n"))
            .unwrap_or_default();
        let desired_state = desired_state
            .map(|desired_state| format!("  {desired_state}\n"))
            .unwrap_or_default();
        let later_task = if later_task {
            "  exec \"later\" { command \"true\" }\n"
        } else {
            ""
        };
        std::fs::write(
            agent_dir.join("agent.kdl"),
            format!(
                r#"agent "{identity}" {{
  host "hetz"
{supervisor}{desired_state}  command "agent"
{later_task}  resource "goal" uri="dev.schickling.agent-goal://hetz/{identity}" reason="Layer."
}}
"#
            ),
        )
        .unwrap();
        let goal = resources.join("goal.md");
        std::fs::write(&goal, "before\n").unwrap();
        (agent_dir, goal)
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn current_resync_event_for_key(agent_dir: &Path, key: &str) -> Option<String> {
        let expected = format!("key: {key}");
        std::fs::read_dir(agent_dir.join("resources/inbox"))
            .ok()?
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
            .find(|body| {
                body.lines().any(|line| line == "stream: resync")
                    && body.lines().any(|line| line == expected)
            })
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn wait_for_resync_event_for_key(agent_dir: &Path, key: &str) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = current_resync_event_for_key(agent_dir, key) {
                return Some(body);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn wait_for_resync_event_key_change(
        agent_dir: &Path,
        key: &str,
        prior: &str,
    ) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = current_resync_event_for_key(agent_dir, key)
                && body != prior
            {
                return Some(body);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn current_resync_event(agent_dir: &Path) -> Option<String> {
        std::fs::read_dir(agent_dir.join("resources/inbox"))
            .ok()?
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
            .find(|body| body.lines().any(|line| line == "stream: resync"))
    }

    fn wait_for_resync_event(agent_dir: &Path) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = current_resync_event(agent_dir) {
                return Some(body);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_resync_event_change(agent_dir: &Path, prior: &str) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = current_resync_event(agent_dir)
                && body != prior
            {
                return Some(body);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(feature = "wasm-resolver")]
    #[test]
    fn up_loop_keeps_complete_notify_chain_sets_during_steady_reconcile() {
        let catalog = tempfile::tempdir().unwrap();
        write_notify_chain_profile(catalog.path());
        let (root_dir, root_goal) =
            write_notify_chain_agent(catalog.path(), "root", None, false);
        let (lead_dir, lead_goal) =
            write_notify_chain_agent(catalog.path(), "lead", Some("hetz.root"), false);
        let (worker_dir, _worker_goal) =
            write_notify_chain_agent(catalog.path(), "worker", Some("hetz.lead"), false);
        let specs = crate::discover_strict(catalog.path()).specs;
        let task_id = |spec: &AgentSpec, task: &Task| {
            task.id
                .clone()
                .unwrap_or_else(|| format!("{}.{}", spec.bus_id("hetz"), task.name))
        };
        let mut sessions = Vec::new();
        for spec in &specs {
            for task in &spec.tasks {
                sessions.push(sess(&task_id(spec, task), true));
            }
        }
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = SteadyChainRunner {
            sessions: std::sync::Mutex::new(sessions),
            block_id: "hetz.worker.later".to_owned(),
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
        };
        let stop = AtomicBool::new(false);
        let (first_report_tx, first_report_rx) = mpsc::sync_channel(1);
        let observer_catalog = catalog.path().to_path_buf();

        let evidence = std::thread::scope(|scope| {
            let observer_stop = &stop;
            let observer = scope.spawn(move || {
                first_report_rx.recv().unwrap();

                std::fs::write(&root_goal, "steady baseline transition\n").unwrap();
                let root_initial = wait_for_resync_event_for_key(&root_dir, "goal");
                let lead_initial =
                    wait_for_resync_event_for_key(&lead_dir, "goal@hetz.root");
                let worker_initial =
                    wait_for_resync_event_for_key(&worker_dir, "goal@hetz.root");

                write_notify_chain_agent(
                    &observer_catalog,
                    "worker",
                    Some("hetz.lead"),
                    true,
                );
                let entered = entered_rx.recv_timeout(Duration::from_secs(5)).is_ok();
                let root_after_reconcile = root_initial.as_deref().and_then(|prior| {
                    std::fs::write(&root_goal, "transition during steady reconcile\n").unwrap();
                    wait_for_resync_event_key_change(&root_dir, "goal", prior)
                });
                let lead_after_reconcile = lead_initial.as_deref().and_then(|prior| {
                    wait_for_resync_event_key_change(&lead_dir, "goal@hetz.root", prior)
                });
                let worker_after_reconcile = worker_initial.as_deref().and_then(|prior| {
                    wait_for_resync_event_key_change(&worker_dir, "goal@hetz.root", prior)
                });

                std::fs::write(&lead_goal, "lead transition during steady reconcile\n").unwrap();
                let lead_own = wait_for_resync_event_for_key(&lead_dir, "goal");
                let worker_from_lead =
                    wait_for_resync_event_for_key(&worker_dir, "goal@hetz.lead");

                let _ = release_tx.send(());
                observer_stop.store(true, Ordering::SeqCst);
                (
                    entered,
                    root_initial,
                    lead_initial,
                    worker_initial,
                    root_after_reconcile,
                    lead_after_reconcile,
                    worker_after_reconcile,
                    lead_own,
                    worker_from_lead,
                )
            });
            up_loop_until(
                catalog.path(),
                "hetz",
                &runner,
                Duration::from_millis(25),
                &stop,
                |_, _| None,
                |_| {
                    let _ = first_report_tx.try_send(());
                },
            )
            .unwrap();
            observer.join().unwrap()
        });

        assert!(evidence.0, "the steady-state reconcile must reach its later task");
        assert!(evidence.1.is_some(), "root must receive its own transition");
        assert!(
            evidence.2.is_some() && evidence.3.is_some(),
            "root transition must fan out through lead and worker"
        );
        assert!(
            evidence.4.is_some() && evidence.5.is_some() && evidence.6.is_some(),
            "a steady reconcile must not replace chain sets with self-only sets"
        );
        assert!(
            evidence.7.is_some() && evidence.8.is_some(),
            "lead transition must reach lead and worker"
        );
        assert_up_loop_full_refresh_keeps_a_retired_middle_as_live_child_topology();
    }

    #[cfg(all(test, feature = "wasm-resolver"))]
    fn assert_up_loop_full_refresh_keeps_a_retired_middle_as_live_child_topology() {
        for retirement in [
            "retired #true",
            "desired-state \"retired\" reason=\"fixture\"",
        ] {
            let catalog = tempfile::tempdir().unwrap();
            write_notify_chain_profile(catalog.path());
            let (root_dir, root_goal) =
                write_notify_chain_agent(catalog.path(), "root", None, false);
            let (middle_dir, _middle_goal) = write_notify_chain_agent_with_state(
                catalog.path(),
                "middle",
                Some("hetz.root"),
                false,
                Some(retirement),
            );
            let (child_dir, _child_goal) =
                write_notify_chain_agent(catalog.path(), "child", Some("hetz.middle"), false);
            let specs = crate::discover_strict(catalog.path()).specs;
            let sessions = specs
                .iter()
                .filter(|spec| spec.desired_state.is_running())
                .flat_map(|spec| {
                    spec.tasks.iter().map(|task| {
                        let id = task
                            .id
                            .clone()
                            .unwrap_or_else(|| format!("{}.{}", spec.bus_id("hetz"), task.name));
                        sess(&id, true)
                    })
                })
                .collect();
            let (entered_tx, _entered_rx) = mpsc::sync_channel(1);
            let (_release_tx, release_rx) = mpsc::channel();
            let runner = SteadyChainRunner {
                sessions: std::sync::Mutex::new(sessions),
                block_id: "never-block".to_owned(),
                entered: entered_tx,
                release: std::sync::Mutex::new(release_rx),
            };
            let stop = AtomicBool::new(false);
            let missing_supervisor = AtomicBool::new(false);
            let (first_report_tx, first_report_rx) = mpsc::sync_channel(1);

            let evidence = std::thread::scope(|scope| {
                let observer_stop = &stop;
                let observer = scope.spawn(move || {
                    first_report_rx.recv().unwrap();
                    // Let the asynchronous full refresh replace the synchronous install before
                    // mutating the root carrier. The child must retain the complete catalog chain.
                    std::thread::sleep(Duration::from_millis(300));
                    std::fs::write(&root_goal, "root transition after full refresh\n").unwrap();
                    let root_event = wait_for_resync_event_for_key(&root_dir, "goal");
                    let child_event =
                        wait_for_resync_event_for_key(&child_dir, "goal@hetz.root");
                    let middle_event = current_resync_event_for_key(&middle_dir, "goal@hetz.root");
                    observer_stop.store(true, Ordering::SeqCst);
                    (root_event, child_event, middle_event)
                });
                up_loop_until(
                    catalog.path(),
                    "hetz",
                    &runner,
                    Duration::from_millis(25),
                    &stop,
                    |_, _| None,
                    |report| {
                        if report
                            .errors
                            .iter()
                            .chain(&report.warnings)
                            .any(|message| message.contains("MissingSupervisor"))
                        {
                            missing_supervisor.store(true, Ordering::SeqCst);
                        }
                        let _ = first_report_tx.try_send(());
                    },
                )
                .unwrap();
                observer.join().unwrap()
            });

            assert!(
                evidence.0.is_some(),
                "root must receive its own event ({retirement})"
            );
            assert!(
                evidence.1.is_some(),
                "the live child must receive exactly its owner-qualified root event through the \
                 retired middle after full refresh ({retirement})"
            );
            assert!(
                evidence.2.is_none(),
                "the retired middle must own no active subscription ({retirement})"
            );
            assert!(
                !missing_supervisor.load(Ordering::SeqCst),
                "the complete catalog graph must prevent MissingSupervisor ({retirement})"
            );
        }
    }

    #[test]
    fn compile_invalid_seat_does_not_block_existing_live_resync_watch() {
        let catalog = tempfile::tempdir().unwrap();
        let (live_dir, live_goal) = write_resync_agent(catalog.path(), "live");
        let broken_dir = catalog.path().join("agents/hetz/broken");
        let broken_resources = broken_dir.join("resources");
        std::fs::create_dir_all(&broken_resources).unwrap();
        std::fs::create_dir_all(catalog.path().join("broken-workspace")).unwrap();
        let broken_declaration = broken_dir.join("agent.kdl");
        std::fs::write(
            &broken_declaration,
            r#"agent "broken" {
  host "hetz"
  deliver "mcp"
  workspace "$CATALOG/broken-workspace"
  exec "agent" { command "true" }
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let broken_goal = broken_resources.join("goal.md");
        std::fs::write(&broken_goal, "before\n").unwrap();
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();

        let runner = SpawnCountingRunner {
            sessions: RefCell::new(vec![
                sess("hetz.live", true),
                sess("hetz.broken.agent", true),
            ]),
            ..SpawnCountingRunner::default()
        };
        let task_context = TaskCompileContext::current(catalog.path().to_path_buf()).unwrap();
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());
        let mut cap = FlappingCap::default();
        let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
        let mut presentation_cursor = PresentationPatchCursor::default();

        let first = reconcile_pass(catalog.path(),
        "hetz",
        &task_context,
        &runner,
        &mut cap,
        &mut debounce,
        &mut presentation_cursor,
        Some(&resync), None);
        assert!(
            first.errors.iter().any(|error| {
                error.contains("compile generated tasks")
                    && error.contains("non-PTY canonical task")
            }),
            "{first:#?}"
        );
        assert!(!first.skipped, "the valid subset completed its pass");
        assert!(
            runner.spawned.borrow().is_empty(),
            "the compile-invalid seat must not launch"
        );

        std::fs::write(&live_goal, "changed while compile failed\n").unwrap();
        let first_event = wait_for_resync_event(&live_dir)
            .expect("the already-live valid seat must stay watched across the compile error");
        assert!(first_event.contains(r#""binding":"goal""#), "{first_event}");

        std::fs::write(&broken_goal, "invalid seat changed\n").unwrap();
        std::thread::sleep(Duration::from_millis(750));
        assert!(
            current_resync_event(&broken_dir).is_none(),
            "a compile-invalid seat must not be watched even when its canonical task is live"
        );

        std::fs::write(&live_goal, "changed while declaration is corrected\n").unwrap();
        std::fs::write(
            &broken_declaration,
            r#"agent "broken" {
  host "hetz"
  deliver "mcp"
  workspace "$CATALOG/broken-workspace"
  pty "agent" { command "true" }
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let corrected = reconcile_pass(catalog.path(),
        "hetz",
        &task_context,
        &runner,
        &mut cap,
        &mut debounce,
        &mut presentation_cursor,
        Some(&resync), None);
        assert!(
            corrected
                .errors
                .iter()
                .all(|error| !error.contains("compile generated tasks")),
            "{corrected:#?}"
        );
        assert!(corrected.launched.is_empty(), "{corrected:#?}");
        assert!(
            corrected.adopted.iter().any(|identity| identity == "broken"),
            "the corrected already-live seat should be adopted: {corrected:#?}"
        );
        let corrected_event = wait_for_resync_event_change(&live_dir, &first_event)
            .expect("correcting another declaration must not reseed and hide the live transition");
        assert!(corrected_event.contains(r#""binding":"goal""#), "{corrected_event}");
    }

    #[test]
    fn materialization_failure_retains_only_the_observed_live_resync_watch() {
        let catalog = tempfile::tempdir().unwrap();
        let write_broken_agent = |identity: &str| {
            let (agent_dir, goal) = write_resync_agent(catalog.path(), identity);
            let workspace = catalog.path().join(format!("{identity}-workspace"));
            std::fs::create_dir_all(&workspace).unwrap();
            std::fs::write(
                agent_dir.join("agent.kdl"),
                format!(
                    r#"agent "{identity}" {{
  host "hetz"
  workspace "{}"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
  render {{
    copy "_templates/{identity}.md" "AGENTS.md"
  }}
}}"#,
                    workspace.display()
                ),
            )
            .unwrap();
            (agent_dir, goal)
        };
        let (live_dir, live_goal) = write_broken_agent("live");
        let (dormant_dir, dormant_goal) = write_broken_agent("dormant");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();

        let runner = SpawnCountingRunner {
            sessions: RefCell::new(vec![sess("hetz.live", true)]),
            ..SpawnCountingRunner::default()
        };
        let task_context = TaskCompileContext::current(catalog.path().to_path_buf()).unwrap();
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());
        let mut cap = FlappingCap::default();
        let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
        let mut presentation_cursor = PresentationPatchCursor::default();

        let failed = reconcile_pass(catalog.path(),
        "hetz",
        &task_context,
        &runner,
        &mut cap,
        &mut debounce,
        &mut presentation_cursor,
        Some(&resync), None);
        assert!(
            failed
                .errors
                .iter()
                .filter(|error| error.contains("copy source"))
                .count()
                >= 2,
            "{failed:#?}"
        );
        assert!(
            runner.spawned.borrow().is_empty(),
            "materialization-failed seats must not launch"
        );

        std::fs::write(&live_goal, "changed while materialization failed\n").unwrap();
        std::fs::write(&dormant_goal, "unwatched while materialization failed\n").unwrap();
        let first_event = wait_for_resync_event(&live_dir)
            .expect("the observed live seat must remain watched through materialization failure");
        assert!(first_event.contains(r#""binding":"goal""#), "{first_event}");
        std::thread::sleep(Duration::from_millis(750));
        assert!(
            current_resync_event(&dormant_dir).is_none(),
            "a materialization-failed seat without an observed live session must stay unwatched"
        );

        std::fs::write(&live_goal, "changed immediately before recovery\n").unwrap();
        std::fs::create_dir_all(catalog.path().join("_templates")).unwrap();
        std::fs::write(catalog.path().join("_templates/live.md"), "rendered\n").unwrap();
        let recovered = reconcile_pass(catalog.path(),
        "hetz",
        &task_context,
        &runner,
        &mut cap,
        &mut debounce,
        &mut presentation_cursor,
        Some(&resync), None);
        assert!(
            recovered
                .errors
                .iter()
                .all(|error| !error.contains("_templates/live.md")),
            "{recovered:#?}"
        );
        assert!(recovered.launched.is_empty(), "{recovered:#?}");
        let recovered_event = wait_for_resync_event_change(&live_dir, &first_event)
            .expect("recovery must preserve the pending transition instead of silently reseeding");
        assert!(
            recovered_event.contains(r#""binding":"goal""#),
            "{recovered_event}"
        );
    }

    fn execute_resync_plan(
        plan: &ReconcilePlan<'_>,
        runner: &dyn Runner,
        specs: &[AgentSpec],
        resync: &crate::resync::ResyncSupervisor,
    ) -> UpReport {
        let mut report = UpReport::default();
        let mut install_count = 0;
        execute_with_presentation_cursor(
            plan,
            runner,
            &mut FlappingCap::default(),
            &mut PresentationPatchCursor::default(),
            &mut report,
            &mut |spec| {
                install_count += 1;
                assert!(resync.install_live(spec, specs, "hetz").is_empty());
            },
        );
        assert!(
            resync
                .refresh(
                    specs,
                    &live_resync_specs(specs, "hetz", &[], &report),
                    "hetz",
                    &[],
                    &[],
                )
                .is_empty()
        );
        assert!(install_count > 0 || report.launched.is_empty());
        report
    }

    #[cfg(feature = "wasm-resolver")]
    #[test]
    fn notify_chain_launch_boundary_installs_ancestors_before_a_later_task_finishes() {
        let catalog = tempfile::tempdir().unwrap();
        write_notify_chain_profile(catalog.path());
        let (_root_dir, root_goal) =
            write_notify_chain_agent(catalog.path(), "root", None, false);
        write_notify_chain_agent(catalog.path(), "lead", Some("hetz.root"), false);
        let (worker_dir, _worker_goal) =
            write_notify_chain_agent(catalog.path(), "worker", Some("hetz.lead"), false);
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let specs = crate::discover_strict(catalog.path()).specs;
        let worker = specs
            .iter()
            .find(|spec| spec.identity == "worker")
            .unwrap();
        let mut later = target("hetz.worker.later", "later");
        later.name = "later".into();
        later.derived = true;
        let plan = ReconcilePlan {
            launch: vec![Launch {
                spec: worker,
                tasks: vec![target("hetz.worker.agent", "agent"), later],
                live_derived: Vec::new(),
            }],
            ..ReconcilePlan::default()
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = BlockingLaunchRunner {
            sessions: RefCell::new(Vec::new()),
            fail_id: None,
            block_id: "hetz.worker.later".to_owned(),
            entered: entered_tx,
            release: RefCell::new(release_rx),
        };
        let resync = crate::resync::ResyncSupervisor::with_profiles(
            catalog.path().to_path_buf(),
            "hetz".into(),
            crate::catalog::declared_profiles(catalog.path()).unwrap(),
        );

        let event = std::thread::scope(|scope| {
            let observer = scope.spawn(move || {
                entered_rx.recv().unwrap();
                std::fs::write(&root_goal, "changed while later task launches\n").unwrap();
                let event =
                    wait_for_resync_event_for_key(&worker_dir, "goal@hetz.root");
                release_tx.send(()).unwrap();
                event
            });
            let report = execute_resync_plan(&plan, &runner, &specs, &resync);
            assert_eq!(
                report.launched,
                ["hetz.worker.agent", "hetz.worker.later"]
            );
            observer.join().unwrap()
        })
        .expect("the fresh worker must receive its ancestor transition before full refresh");
        assert!(event.contains("key: goal@hetz.root"), "{event}");
    }

    #[test]
    fn resync_launch_boundary_seeds_first_seat_before_later_seat_finishes() {
        let catalog = tempfile::tempdir().unwrap();
        let (first_dir, first_goal) = write_resync_agent(catalog.path(), "first");
        write_resync_agent(catalog.path(), "second");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let specs = crate::discover_strict(catalog.path()).specs;
        let first = specs.iter().find(|spec| spec.identity == "first").unwrap();
        let second = specs.iter().find(|spec| spec.identity == "second").unwrap();
        let plan = ReconcilePlan {
            launch: vec![
                Launch {
                    spec: first,
                    tasks: vec![target("hetz.first.agent", "agent")],
                    live_derived: Vec::new(),
                },
                Launch {
                    spec: second,
                    tasks: vec![target("hetz.second.agent", "agent")],
                    live_derived: Vec::new(),
                },
            ],
            ..ReconcilePlan::default()
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = BlockingLaunchRunner {
            sessions: RefCell::new(Vec::new()),
            fail_id: None,
            block_id: "hetz.second.agent".to_owned(),
            entered: entered_tx,
            release: RefCell::new(release_rx),
        };
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());

        std::thread::scope(|scope| {
            scope.spawn(move || {
                entered_rx.recv().unwrap();
                std::fs::write(&first_goal, "changed while second launches\n").unwrap();
                std::thread::sleep(Duration::from_secs(1));
                release_tx.send(()).unwrap();
            });
            let report = execute_resync_plan(&plan, &runner, &specs, &resync);
            assert_eq!(
                report.launched,
                ["hetz.first.agent", "hetz.second.agent"]
            );
        });

        let event = wait_for_resync_event(&first_dir)
            .expect("the first seat must observe a carrier transition during the later launch");
        assert!(event.contains(r#""binding":"goal""#), "{event}");
    }

    #[test]
    fn resync_launch_boundary_excludes_failed_canonical_seat() {
        let catalog = tempfile::tempdir().unwrap();
        let (first_dir, first_goal) = write_resync_agent(catalog.path(), "first");
        write_resync_agent(catalog.path(), "second");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let specs = crate::discover_strict(catalog.path()).specs;
        let first = specs.iter().find(|spec| spec.identity == "first").unwrap();
        let second = specs.iter().find(|spec| spec.identity == "second").unwrap();
        let plan = ReconcilePlan {
            launch: vec![
                Launch {
                    spec: first,
                    tasks: vec![target("hetz.first.agent", "agent")],
                    live_derived: Vec::new(),
                },
                Launch {
                    spec: second,
                    tasks: vec![target("hetz.second.agent", "agent")],
                    live_derived: Vec::new(),
                },
            ],
            ..ReconcilePlan::default()
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = BlockingLaunchRunner {
            sessions: RefCell::new(Vec::new()),
            fail_id: Some("hetz.first.agent".to_owned()),
            block_id: "hetz.second.agent".to_owned(),
            entered: entered_tx,
            release: RefCell::new(release_rx),
        };
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());

        std::thread::scope(|scope| {
            scope.spawn(move || {
                entered_rx.recv().unwrap();
                std::fs::write(&first_goal, "changed after failed launch\n").unwrap();
                std::thread::sleep(Duration::from_secs(1));
                release_tx.send(()).unwrap();
            });
            let report = execute_resync_plan(&plan, &runner, &specs, &resync);
            assert_eq!(report.launched, ["hetz.second.agent"]);
            assert!(report.errors.iter().any(|error| {
                error.contains("hetz.first.agent") && error.contains("simulated launch failure")
            }));
        });

        std::thread::sleep(Duration::from_millis(750));
        assert!(
            current_resync_event(&first_dir).is_none(),
            "desired-but-failed canonical seats must remain unwatched"
        );
    }

    #[test]
    fn dead_resync_seat_is_deactivated_before_its_relaunch_blocks() {
        let catalog = tempfile::tempdir().unwrap();
        let (agent_dir, goal) = write_resync_agent(catalog.path(), "worker");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = BlockingLaunchRunner {
            sessions: RefCell::new(vec![sess("hetz.worker", true)]),
            fail_id: None,
            block_id: "hetz.worker".to_owned(),
            entered: entered_tx,
            release: RefCell::new(release_rx),
        };
        let task_context = TaskCompileContext::current(catalog.path().to_path_buf()).unwrap();
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());
        let mut cap = FlappingCap::default();
        let mut debounce = LivenessDebounce::new(Duration::ZERO);
        let mut presentation_cursor = PresentationPatchCursor::default();

        let seeded = reconcile_pass(catalog.path(),
        "hetz",
        &task_context,
        &runner,
        &mut cap,
        &mut debounce,
        &mut presentation_cursor,
        Some(&resync), None);
        assert!(seeded.adopted.iter().any(|identity| identity == "worker"));
        *runner.sessions.borrow_mut() = vec![sess("hetz.worker", false)];
        let blocked_goal = goal.clone();

        let relaunched = std::thread::scope(|scope| {
            scope.spawn(move || {
                entered_rx.recv().unwrap();
                std::fs::write(&blocked_goal, "changed while replacement launch blocks\n").unwrap();
                std::thread::sleep(Duration::from_secs(1));
                release_tx.send(()).unwrap();
            });
            reconcile_pass(catalog.path(),
            "hetz",
            &task_context,
            &runner,
            &mut cap,
            &mut debounce,
            &mut presentation_cursor,
            Some(&resync), None)
        });
        assert_eq!(relaunched.restarted, ["hetz.worker"]);
        std::thread::sleep(Duration::from_millis(750));
        assert!(
            current_resync_event(&agent_dir).is_none(),
            "a carrier mutation while no canonical seat is live must not emit"
        );

        std::fs::write(&goal, "changed after replacement launch\n").unwrap();
        let event = wait_for_resync_event(&agent_dir)
            .expect("the successful replacement must receive a fresh silent baseline");
        assert!(event.contains(r#""binding":"goal""#), "{event}");
    }

    /// A publication the resync worker cannot finish must not hold up a reconcile pass.
    ///
    /// Every per-seat `install_live` handshake is answered by the same worker thread that runs
    /// publications, so a publication in progress serializes the whole pass behind it. That is the
    /// coupling which let a terminal-refusal loop keep every pass from completing for two hours
    /// (#431): the refusals only had power because they denied the pass that would have ended
    /// them. Blocking one real publication on the recipient's stream lock is the sharpest form of
    /// the same coupling — a slow publication makes a pass late, a stuck one makes it never
    /// finish — and it holds the pass at exactly the point `emit_admitted` serializes.
    #[test]
    fn reconcile_pass_completes_while_a_resync_publication_is_blocked() {
        use std::os::fd::AsRawFd as _;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let catalog = tempfile::tempdir().unwrap();
        let (agent_dir, goal) = write_resync_agent(catalog.path(), "worker");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let runner = SpawnCountingRunner {
            sessions: RefCell::new(vec![sess("hetz.worker", true)]),
            ..Default::default()
        };
        let task_context = TaskCompileContext::current(catalog.path().to_path_buf()).unwrap();
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());
        let mut cap = FlappingCap::default();
        let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
        let mut presentation_cursor = PresentationPatchCursor::default();

        let seeded = reconcile_pass(
            catalog.path(),
            "hetz",
            &task_context,
            &runner,
            &mut cap,
            &mut debounce,
            &mut presentation_cursor,
            Some(&resync),
            None,
        );
        assert!(
            seeded.adopted.iter().any(|identity| identity == "worker"),
            "{seeded:#?}"
        );

        // One completed publication first: it creates the recipient's resync stream state
        // directory, whose `.lock` is the gate below, and proves the publication path is live.
        std::fs::write(&goal, "changed before the gate closes\n").unwrap();
        let published = wait_for_resync_event(&agent_dir)
            .expect("the live seat must observe its first carrier transition");

        let gate = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(agent_dir.join("resources/streams/resync/.lock"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(gate.as_raw_fd(), libc::LOCK_EX) },
            0,
            "the test must own the stream lock the publication path takes"
        );
        let gate_fd = gate.as_raw_fd();

        // The worker has no other work and `IMMEDIATE_WINDOW` is 500 ms, so it is inside the
        // blocked publication well before this wait ends; the unchanged event is the positive
        // evidence that the publication has not completed.
        std::fs::write(&goal, "changed while the gate is closed\n").unwrap();
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(
            current_resync_event(&agent_dir).as_deref(),
            Some(published.as_str()),
            "the gate must hold the second publication open"
        );

        // The watchdog releases the gate only when the pass fails to complete on its own, which
        // is what separates a decoupled pass from one that merely finished after the rescue.
        let rescued = AtomicBool::new(false);
        let rescued_flag = &rescued;
        let (finished_tx, finished_rx) = mpsc::channel::<()>();
        let pass = std::thread::scope(|scope| {
            scope.spawn(move || {
                if finished_rx.recv_timeout(Duration::from_secs(20)).is_err() {
                    rescued_flag.store(true, AtomicOrdering::SeqCst);
                    unsafe { libc::flock(gate_fd, libc::LOCK_UN) };
                }
            });
            let pass = reconcile_pass(
                catalog.path(),
                "hetz",
                &task_context,
                &runner,
                &mut cap,
                &mut debounce,
                &mut presentation_cursor,
                Some(&resync),
                None,
            );
            let _ = finished_tx.send(());
            pass
        });
        assert!(
            !rescued.load(AtomicOrdering::SeqCst),
            "the pass only completed after the blocked publication was released: {pass:#?}"
        );
        assert!(
            pass.adopted.iter().any(|identity| identity == "worker"),
            "{pass:#?}"
        );
        drop(gate);
    }

    #[test]
    fn resync_launch_boundary_preserves_baseline_across_derived_companion() {
        let catalog = tempfile::tempdir().unwrap();
        let (agent_dir, goal) = write_resync_agent(catalog.path(), "worker");
        crate::event::publish_owner_binding_for_test(catalog.path(), "hetz").unwrap();
        let specs = crate::discover_strict(catalog.path()).specs;
        let spec = &specs[0];
        let mut derived = target("hetz.worker.ding", "ding");
        derived.name = "ding".into();
        derived.derived = true;
        let plan = ReconcilePlan {
            launch: vec![Launch {
                spec,
                tasks: vec![target("hetz.worker.agent", "agent"), derived],
                live_derived: Vec::new(),
            }],
            ..ReconcilePlan::default()
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let runner = BlockingLaunchRunner {
            sessions: RefCell::new(Vec::new()),
            fail_id: None,
            block_id: "hetz.worker.ding".to_owned(),
            entered: entered_tx,
            release: RefCell::new(release_rx),
        };
        let resync =
            crate::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".into());
        let installs = AtomicUsize::new(0);
        let mut report = UpReport::default();

        std::thread::scope(|scope| {
            scope.spawn(move || {
                entered_rx.recv().unwrap();
                std::fs::write(&goal, "changed while companion launches\n").unwrap();
                std::thread::sleep(Duration::from_secs(1));
                release_tx.send(()).unwrap();
            });
            execute_with_presentation_cursor(
                &plan,
                &runner,
                &mut FlappingCap::default(),
                &mut PresentationPatchCursor::default(),
                &mut report,
                &mut |spec| {
                    installs.fetch_add(1, AtomicOrdering::SeqCst);
                    assert!(resync.install_live(spec, &specs, "hetz").is_empty());
                },
            );
        });
        assert!(
            resync
                .refresh(
                    &specs,
                    &live_resync_specs(&specs, "hetz", &[], &report),
                    "hetz",
                    &[],
                    &[],
                )
                .is_empty()
        );

        assert_eq!(
            installs.load(AtomicOrdering::SeqCst),
            1,
            "only the canonical task transition may install its watch set"
        );
        let event = wait_for_resync_event(&agent_dir)
            .expect("the companion launch and final refresh must preserve the canonical baseline");
        assert!(event.contains(r#""binding":"goal""#), "{event}");
    }

    #[test]
    fn debounce_absorbs_a_gc_flicker_but_reaps_a_stable_death() {
        let t0 = Instant::now();
        let mut db = LivenessDebounce::new(Duration::from_secs(10));
        db.observe(&[sess("hetz.demo.agent", true)], t0);

        // Flicker: reads not-alive 1s later but was alive within the grace → deferred (left running).
        let mut plan = ReconcilePlan::default();
        plan.gc.push("hetz.demo.agent".into());
        let deferred = db.defer_flickers(&mut plan, t0 + Duration::from_secs(1));
        assert!(
            plan.gc.is_empty(),
            "a recently-alive flicker must NOT be GC'd"
        );
        assert_eq!(deferred, vec!["hetz.demo.agent".to_string()]);

        // CENTRAL anti-over-correction check: a STABLE death past the grace IS still reaped — the
        // debounce must never MASK a real death.
        let mut plan = ReconcilePlan::default();
        plan.gc.push("hetz.demo.agent".into());
        let deferred = db.defer_flickers(&mut plan, t0 + Duration::from_secs(11));
        assert_eq!(
            plan.gc,
            vec!["hetz.demo.agent".to_string()],
            "a stable death must still be reaped"
        );
        assert!(deferred.is_empty());
    }

    #[test]
    fn effective_pty_root_prefers_an_exported_ambient_root_else_catalog_pty() {
        let cat = std::path::Path::new("/deep/sandbox/st-root");
        // No ambient PTY_ROOT → the rendered default `<catalog>/pty`.
        assert_eq!(effective_pty_root_from(cat, None), cat.join("pty"));
        assert_eq!(
            effective_pty_root_from(cat, Some("".into())),
            cat.join("pty"),
            "empty is treated as unset"
        );
        // An exported ambient PTY_ROOT (e.g. an eval's short decoupled root) WINS — so a deep catalog
        // path can't blow the unix-socket limit, and spawn agrees with list/kill.
        let short = std::ffi::OsString::from("/tmp/stev-abc123");
        assert_eq!(
            effective_pty_root_from(cat, Some(short)),
            std::path::PathBuf::from("/tmp/stev-abc123")
        );
    }

    #[test]
    fn a_catalog_declared_root_outranks_the_default_but_never_an_ambient_one() {
        let tmp = tempfile::tempdir().unwrap();
        let cat = tmp.path();
        std::fs::write(
            cat.join(crate::catalog::CONFIG_FILE),
            "catalog { pty-root \"/run/agents/pty\" }\n",
        )
        .unwrap();

        // The declaration replaces the `<catalog>/pty` default for every st2 pty op — so a reader
        // that resolves the catalog finds the sessions without being handed an env var.
        assert_eq!(
            effective_pty_root_from(cat, None),
            std::path::PathBuf::from("/run/agents/pty")
        );
        // An explicit ambient root still wins: an eval run's short decoupled partition must be able
        // to override a catalog it copied from.
        assert_eq!(
            effective_pty_root_from(cat, Some("/tmp/stev-abc123".into())),
            std::path::PathBuf::from("/tmp/stev-abc123")
        );
    }

    #[test]
    fn debounce_never_defers_a_never_seen_task() {
        let t0 = Instant::now();
        let db = LivenessDebounce::new(Duration::from_secs(10));
        // A genuinely-new target (never observed alive) is handled immediately, not deferred.
        let mut plan = ReconcilePlan::default();
        plan.gc.push("hetz.brandnew.agent".into());
        let deferred = db.defer_flickers(&mut plan, t0);
        assert_eq!(plan.gc, vec!["hetz.brandnew.agent".to_string()]);
        assert!(deferred.is_empty());
    }

    #[test]
    fn debounce_defers_a_flickering_launch_target_too() {
        let t0 = Instant::now();
        let mut db = LivenessDebounce::new(Duration::from_secs(10));
        db.observe(&[sess("hetz.demo.agent", true)], t0);

        // The same recently-alive id showing up as a launch target (Absent/Dead) is also deferred —
        // no noisy "already in use" re-launch of a live session.
        let spec = spec_fixture();
        let mut plan = ReconcilePlan::default();
        plan.launch.push(Launch {
            spec: &spec,
            tasks: vec![target("hetz.demo.agent", "x")],
            live_derived: Vec::new(),
        });
        let deferred = db.defer_flickers(&mut plan, t0 + Duration::from_secs(2));
        assert!(
            plan.launch.is_empty(),
            "a recently-alive flicker must NOT be re-launched"
        );
        assert_eq!(deferred, vec!["hetz.demo.agent".to_string()]);
    }

    #[test]
    fn codex_hook_gate_accepts_new_agents_without_mutating_the_launch_plan() {
        let mut left = spec_fixture();
        left.identity = "left".into();
        left.path = PathBuf::from("/catalog/node/left/agent.kdl");
        let mut right = spec_fixture();
        right.identity = "right".into();
        right.path = PathBuf::from("/catalog/node/right/agent.kdl");
        let mut left_agent = target("node.left.agent", "exec codex --model gpt-5");
        left_agent.workspace = Some("/workspaces/shared".into());
        let mut right_agent = target("node.right.agent", "/opt/bin/codex --model gpt-5");
        right_agent.workspace = Some("/workspaces/shared".into());
        let mut plan = ReconcilePlan::default();
        plan.launch.push(Launch {
            spec: &left,
            tasks: vec![left_agent],
            live_derived: Vec::new(),
        });
        plan.launch.push(Launch {
            spec: &right,
            tasks: vec![right_agent],
            live_derived: Vec::new(),
        });
        let expected = plan
            .launch
            .iter()
            .map(|launch| launch.spec.identity.clone())
            .collect::<Vec<_>>();
        let mut report = UpReport::default();

        gate_harness_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, |_| Ok(()));

        assert_eq!(
            plan.launch
                .iter()
                .map(|launch| launch.spec.identity.clone())
                .collect::<Vec<_>>(),
            expected,
            "successful hook verification must leave the launch plan unchanged"
        );
        assert_eq!(plan.launch.len(), 2);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn codex_hook_gate_does_not_touch_adopted_agents_or_sidecar_only_repairs() {
        let mut spec = spec_fixture();
        spec.identity = "root".into();
        let mut ding = target("node.root.ding", "st2 ding");
        ding.name = "ding".into();
        let mut plan = ReconcilePlan::default();
        plan.adopt.push(&spec);
        plan.launch.push(Launch {
            spec: &spec,
            tasks: vec![ding],
            live_derived: Vec::new(),
        });
        let mut report = UpReport::default();

        gate_harness_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, |_| {
            panic!("an already-live Codex agent must not enter the hook gate")
        });

        assert_eq!(plan.adopt, [&spec]);
        assert_eq!(plan.launch.len(), 1);
        assert_eq!(plan.launch[0].tasks[0].name, "ding");
        assert!(report.errors.is_empty());
    }

    #[test]
    fn hook_verification_failure_suppresses_only_new_codex_agents() {
        let mut codex = spec_fixture();
        codex.identity = "codex".into();
        codex.path = PathBuf::from("/catalog/node/codex/agent.kdl");
        let mut claude = spec_fixture();
        claude.identity = "claude".into();
        claude.path = PathBuf::from("/catalog/node/claude/agent.kdl");
        let mut codex_agent = target("node.codex.agent", "exec codex");
        codex_agent.workspace = Some("/workspaces/codex".into());
        let claude_agent = target("node.claude.agent", "exec claude");
        let mut plan = ReconcilePlan::default();
        plan.launch.push(Launch {
            spec: &codex,
            tasks: vec![codex_agent],
            live_derived: Vec::new(),
        });
        plan.launch.push(Launch {
            spec: &claude,
            tasks: vec![claude_agent],
            live_derived: Vec::new(),
        });
        let mut report = UpReport::default();

        gate_harness_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, |_| {
            anyhow::bail!("stale receipt")
        });

        assert_eq!(
            plan.launch
                .iter()
                .map(|launch| launch.spec.identity.as_str())
                .collect::<Vec<_>>(),
            ["claude"]
        );
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].contains("stale receipt"));
        assert!(report.errors[0].contains("launch suppressed"));
    }

    /// The built `pty run` argv runs the command verbatim under `sh -c`, detached, with the pinned id
    /// and the established fallback presentation when no Agent Spec name is projected.
    #[test]
    fn build_run_command_wraps_command_in_sh_c() {
        let cli = PtyCli::default();
        let t = target(
            "hetz.demo.agent",
            "exec claude --permission-mode bypassPermissions 'boot'",
        );
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));

        assert_eq!(cmd.get_program(), OsStr::new("pty"));
        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        // Stable launch/config arguments precede the persisted environment and command separator.
        assert_eq!(&args[0..2], &["run", "-d"]);
        assert!(args.contains(&"--force".to_string()));
        let id_pos = args.iter().position(|a| a == "--id").unwrap();
        assert_eq!(args[id_pos + 1], "hetz.demo.agent");
        let name_pos = args.iter().position(|a| a == "--name").unwrap();
        assert_eq!(args[name_pos + 1], "hetz.demo");
        let sep = args.iter().position(|a| a == "--").unwrap();
        assert_eq!(
            &args[sep + 1..],
            &[
                "sh",
                "-c",
                "exec claude --permission-mode bypassPermissions 'boot'"
            ]
        );
    }

    #[test]
    fn build_run_command_projects_primary_name_and_owned_tags_at_spawn() {
        let key = "ST2_TEST_PRESENTATION_LITERAL_71c";
        unsafe { std::env::set_var(key, "expanded") }

        let cli = PtyCli::default();
        let mut t = target("hetz.demo", "codex");
        t.bus_id = "hetz.demo".to_owned();
        t.tags
            .insert("unrelated".to_owned(), "preserved".to_owned());
        t.presentation = Some(PtyPresentation {
            pty_id: "hetz.demo".to_owned(),
            display_name: Some(Some("Build owner".to_owned())),
            tags: BTreeMap::from([
                ("agent.presentation.schema".to_owned(), Some("1".to_owned())),
                ("agent.actor.path".to_owned(), Some("hetz.demo".to_owned())),
                (
                    "agent.presentation.description".to_owned(),
                    Some(format!("${key}")),
                ),
            ]),
        });
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let args = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        let name = args.iter().position(|arg| arg == "--name").unwrap();
        assert_eq!(args[name + 1], "Build owner");
        let tags = args
            .windows(2)
            .filter(|pair| pair[0] == "--tag")
            .map(|pair| pair[1].as_str())
            .collect::<BTreeSet<_>>();
        assert!(tags.contains("unrelated=preserved"));
        assert!(tags.contains("agent.presentation.schema=1"));
        assert!(tags.contains("agent.actor.path=hetz.demo"));
        assert!(tags.contains("agent.presentation.description=$ST2_TEST_PRESENTATION_LITERAL_71c"));
    }

    #[test]
    fn metadata_patch_uses_exact_id_and_one_json_stdin_payload() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("pty-capture");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.args\"\ncat > \"$0.stdin\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cli = PtyCli {
            bin: executable.display().to_string(),
            catalog_root: temporary.path().to_path_buf(),
        };
        let presentation = PtyPresentation {
            pty_id: "stable.agent.id".to_owned(),
            display_name: Some(None),
            tags: BTreeMap::from([
                ("agent.presentation.schema".to_owned(), Some("1".to_owned())),
                ("agent.presentation.description".to_owned(), None),
            ]),
        };

        cli.patch_presentation(&presentation).unwrap();

        assert_eq!(
            std::fs::read_to_string(executable.with_extension("args")).unwrap(),
            "metadata\npatch\n--id\nstable.agent.id\n"
        );
        let payload: serde_json::Value =
            serde_json::from_slice(&std::fs::read(executable.with_extension("stdin")).unwrap())
                .unwrap();
        assert_eq!(payload["displayName"], serde_json::Value::Null);
        assert_eq!(payload["tags"]["agent.presentation.schema"], "1");
        assert_eq!(
            payload["tags"]["agent.presentation.description"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn input_write_failure_terminates_and_reaps_the_child() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("close-stdin");
        let stdin_closed = temporary.path().join("stdin-closed");
        // The script signals only AFTER closing its stdin, so the barrier below returns exactly when
        // the read end is gone and the parent's very next write must fail with EPIPE.
        std::fs::write(
            &executable,
            "#!/bin/sh\nexec 0<&-\n: > \"$READY.tmp\"\nmv \"$READY.tmp\" \"$READY\"\nsleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let input = vec![b'x'; 1024 * 1024];
        // The pid comes from the parent at spawn, and the barrier makes the deadline measure only
        // the behaviour under test. Without it the 1s budget also had to cover fork+exec of the
        // shell, so a loaded host reported `timed out after 1.0s` instead of `Broken pipe` — the
        // fixture's scheduling consumed the deadline the assertion is about.
        let mut spawned = None;
        let error = output_with_input_timeout_observed(
            Command::new(&executable).env("READY", &stdin_closed),
            Duration::from_secs(1),
            Some(input),
            |pid| {
                spawned = Some(pid);
                await_fixture_ready(pid, &stdin_closed, "the child never closed its stdin");
            },
        )
        .unwrap_err();
        let pid = spawned.expect("the child was spawned before the input write failed");

        assert!(
            format!("{error:#}").contains("Broken pipe"),
            "unexpected write error: {error:#}"
        );
        assert!(
            !crate::host_lock::process_alive(pid),
            "failed metadata child {pid} was not terminated and reaped"
        );
    }

    /// The process-group kill is the entire stated reason [`terminate_and_reap_before`] exists — its
    /// docstring is about an escaped descendant that inherited stdout/stderr and would otherwise
    /// block cleanup. Nothing constructed such a descendant, so `kill(-pid, SIGKILL)` was asserted
    /// by no test: removing it alone left the suite green, because `child.kill()` already satisfies
    /// every assertion that only looks at the direct child.
    #[test]
    fn the_group_kill_reaps_a_descendant_that_outlives_the_direct_child() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("spawn-descendant");
        let descendant_pidfile = temporary.path().join("descendant.pid");
        // The descendant inherits stdout/stderr and outlives the direct child, which is exactly the
        // shape the docstring describes. `child.kill()` cannot reach it; only the group signal can.
        // It publishes its pid by atomic rename, so the barrier never reads a truncated file.
        std::fs::write(
            &executable,
            "#!/bin/sh\nsh -c 'printf \"%s\" \"$$\" > \"$DESCENDANT_PIDFILE.tmp\"; mv \"$DESCENDANT_PIDFILE.tmp\" \"$DESCENDANT_PIDFILE\"; sleep 60' &\nsleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

        // This test *requires* the child to have run — a descendant it never forked is nothing to
        // reap. Waiting for the pidfile inside `on_spawn` makes that a barrier instead of a race:
        // the deadline then only has to outlast a `sleep`, never a fork+exec, so a loaded host can
        // no longer end the run before the fixture has built the thing under test.
        let error = output_with_input_timeout_observed(
            Command::new(&executable).env("DESCENDANT_PIDFILE", &descendant_pidfile),
            Duration::from_millis(500),
            None,
            |pid| {
                await_fixture_ready(
                    pid,
                    &descendant_pidfile,
                    "the child never forked a descendant, so this case would test nothing",
                )
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("timed out"),
            "unexpected error: {error:#}"
        );

        let descendant = std::fs::read_to_string(&descendant_pidfile)
            .expect("the readiness barrier returned without a pidfile")
            .parse::<i32>()
            .unwrap();

        // Generous on purpose: the descendant is orphaned by the same group kill, so its exit is
        // observable only once the reparenting init reaps it. That latency is not the behaviour
        // under test, and waiting longer costs nothing when the kill did reach it.
        let deadline = Instant::now() + Duration::from_secs(5);
        while process_can_retain_cleanup_resources(descendant) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let survived = process_can_retain_cleanup_resources(descendant);
        if survived {
            // Do not leak a 60s sleeper into the test host when the assertion is about to fail.
            unsafe { libc::kill(descendant, libc::SIGKILL) };
        }
        assert!(
            !survived,
            "escaped descendant {descendant} survived cleanup: the process-group kill did not reach it"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_zombie_cannot_retain_cleanup_resources() {
        let mut child = Command::new("sh").arg("-c").arg("exit 0").spawn().unwrap();
        let pid = child.id() as i32;
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = None;
        while Instant::now() < deadline {
            state = linux_process_state(pid);
            if state == Some('Z') {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let kill_probe_considered_alive = crate::host_lock::process_alive(pid);
        let retained_cleanup_resources = process_can_retain_cleanup_resources(pid);
        let _ = child.wait();

        assert_eq!(state, Some('Z'), "child did not become a zombie");
        assert!(
            kill_probe_considered_alive,
            "the fixture must expose kill(pid, 0) treating a zombie as alive"
        );
        assert!(
            !retained_cleanup_resources,
            "a terminated zombie cannot retain cleanup resources"
        );
    }

    #[test]
    fn input_write_obeys_the_child_deadline() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("ignore-stdin");
        std::fs::write(&executable, "#!/bin/sh\nsleep 60\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let input = vec![b'x'; 1024 * 1024];
        // The pid comes from the parent at spawn, not from the child, and this case cannot use a
        // readiness barrier: it is precisely the one where the child may never be scheduled. The
        // write blocks as soon as the pipe buffer fills, which needs no execution by the child at
        // all, and the deadline then terminates the whole group. Anything the child was supposed to
        // record would never be written, so a test that waits for it fails on exactly the condition
        // it exists to cover. The observed spawn instant is therefore also the clock: timing from
        // before the call would charge fork+exec to the 1s budget this assertion polices.
        let mut spawned = None;
        let error = output_with_input_timeout_observed(
            &mut Command::new(&executable),
            Duration::from_millis(100),
            Some(input),
            |pid| spawned = Some((pid, Instant::now())),
        )
        .unwrap_err();
        let (pid, started) =
            spawned.expect("the child was spawned before the input deadline expired");

        assert!(
            format!("{error:#}").contains("timed out"),
            "unexpected write error: {error:#}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "blocked stdin write ignored the child deadline"
        );
        let reap_deadline = Instant::now() + Duration::from_secs(1);
        while crate::host_lock::process_alive(pid) && Instant::now() < reap_deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!crate::host_lock::process_alive(pid));
    }

    /// The two lifecycle tests above block in `on_spawn` until their fixture reached the state under
    /// test, which only keeps them load-insensitive because [`run_captured`] starts the child
    /// deadline AFTER `on_spawn` returns. Nothing else proves that order: reversing it leaves every
    /// other test green on an idle host and silently puts both back on a race with the scheduler.
    #[test]
    fn the_spawn_observer_runs_before_the_child_deadline_starts() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("close-stdin");
        let stdin_closed = temporary.path().join("stdin-closed");
        std::fs::write(
            &executable,
            "#!/bin/sh\nexec 0<&-\n: > \"$READY.tmp\"\nmv \"$READY.tmp\" \"$READY\"\nsleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let timeout = Duration::from_millis(200);

        // The barrier deliberately outlasts `timeout`, so the outcome depends on the order alone and
        // on nothing the host's scheduler does. Deadline after `on_spawn`: the write meets a closed
        // read end and fails with EPIPE at once. Deadline before `on_spawn`: it has already expired
        // when the barrier returns, so the write never runs and the call reports a timeout instead.
        let error = output_with_input_timeout_observed(
            Command::new(&executable).env("READY", &stdin_closed),
            timeout,
            Some(vec![b'x'; 1024]),
            |pid| {
                await_fixture_ready(pid, &stdin_closed, "the child never closed its stdin");
                std::thread::sleep(timeout * 2);
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("Broken pipe"),
            "the child deadline started before `on_spawn` returned: {error:#}"
        );
    }

    #[test]
    fn bounded_capture_keeps_the_tail_of_an_oversized_stream() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("flood");
        // Start marker, 1 MiB of filler (4x the cap, so both streams truncate), end marker.
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf START; head -c 1048576 /dev/zero; printf END\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

        let output =
            output_with_timeout(&mut Command::new(&executable), Duration::from_secs(5)).unwrap();

        assert_eq!(output.stdout.len(), CAPTURE_CAP_BYTES);
        assert!(
            output.stdout.ends_with(b"END"),
            "capped stdout lost the tail"
        );
        assert!(
            !output.stdout.starts_with(b"START"),
            "capped stdout kept the head instead of the tail"
        );
        // stderr is empty here, so only the stdout read-back may have been capped.
    }

    #[test]
    fn full_stdout_variant_returns_complete_output_larger_than_the_cap() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("flood");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf START; head -c 1048576 /dev/zero; printf END\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

        let output =
            output_full_stdout_with_timeout(&mut Command::new(&executable), Duration::from_secs(5))
                .unwrap();

        assert!(output.stdout.len() > CAPTURE_CAP_BYTES);
        assert!(
            output.stdout.starts_with(b"START") && output.stdout.ends_with(b"END"),
            "full-stdout variant truncated structured output: {} bytes",
            output.stdout.len()
        );
    }

    /// Proves the shared reaper actually waits: the killed child is observed as a zombie BEFORE
    /// `reap_detached` runs, so only the reaper's `wait()` can clear that state.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_shared_reaper_reaps_a_killed_child() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = child.kill();
        let deadline = Instant::now() + Duration::from_secs(1);
        while linux_process_state(pid) != Some('Z') && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            linux_process_state(pid),
            Some('Z'),
            "fixture did not produce a zombie"
        );

        reap_detached(child);
        let deadline = Instant::now() + Duration::from_secs(2);
        while linux_process_state(pid) == Some('Z') && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_ne!(
            linux_process_state(pid),
            Some('Z'),
            "the shared reaper did not reap the killed child {pid}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn undrained_reader_does_not_retain_the_nonblocking_writer() {
        use std::os::fd::{FromRawFd as _, OwnedFd};

        let mut pipe_fds = [0; 2];
        assert_eq!(
            unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) },
            0
        );
        let reader = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
        let pipe = std::fs::read_link(format!("/proc/self/fd/{}", reader.as_raw_fd())).unwrap();
        let started = Instant::now();

        assert!(
            !write_all_before(
                ChildStdin::from(writer),
                &vec![b'x'; 1024 * 1024],
                Instant::now() + Duration::from_millis(100),
            )
            .unwrap()
        );
        let retained_writers = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .filter(|target| target == &pipe)
            .count();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            retained_writers, 1,
            "the undrained pipe retained a writer after the deadline"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn expired_write_deadline_prevents_further_progress() {
        use std::os::fd::{FromRawFd as _, OwnedFd};

        let mut pipe_fds = [0; 2];
        assert_eq!(
            unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) },
            0
        );
        let _reader = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };

        assert!(
            !write_all_before(ChildStdin::from(writer), b"x", Instant::now()).unwrap(),
            "an expired child deadline still allowed stdin progress"
        );
    }

    #[test]
    fn expired_cleanup_deadline_hands_reaping_off_without_blocking() {
        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let pid = child.id() as i32;
        let started = Instant::now();
        terminate_and_reap_before(child, pid, Instant::now());

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "expired cleanup deadline blocked the caller"
        );
        let reap_deadline = Instant::now() + Duration::from_secs(1);
        while crate::host_lock::process_alive(pid) && Instant::now() < reap_deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !crate::host_lock::process_alive(pid),
            "background reaper did not collect child {pid}"
        );
    }

    #[test]
    fn build_run_command_passes_direct_argv_without_a_shell() {
        let cli = PtyCli::new(PathBuf::from("/my/catalog"));
        let mut t = target("hetz.demo.agent", "unused");
        t.launch = TaskLaunch::Argv(vec![
            "axe".into(),
            "agent".into(),
            "exec".into(),
            "--".into(),
            "claude".into(),
            "--resume".into(),
            "$CATALOG/session id".into(),
        ]);
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let args = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let sep = args.iter().position(|arg| arg == "--").unwrap();

        assert_eq!(
            &args[sep + 1..],
            [
                "axe",
                "agent",
                "exec",
                "--",
                "claude",
                "--resume",
                "/my/catalog/session id"
            ]
        );
        assert!(!args[sep + 1..].iter().any(|arg| arg == "sh"));
    }

    #[test]
    fn build_run_command_expands_direct_argv_with_the_managed_agent_environment() {
        let cli = PtyCli::new(PathBuf::from("/eval/catalog"));
        let mut t = target("local.worker", "unused");
        t.env.insert("ST_AGENT".into(), "local.worker".into());
        t.env.insert("ST_ROOT".into(), "/eval/catalog".into());
        t.launch = TaskLaunch::Argv(vec![
            "claude".into(),
            "$ST_AGENT reads $ST_ROOT and $CATALOG".into(),
        ]);

        let cmd = cli.build_run_command(&t, Path::new("/eval/catalog/local/worker"));
        let args = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let separator = args.iter().position(|arg| arg == "--").unwrap();

        assert_eq!(
            &args[separator + 1..],
            [
                "claude",
                "local.worker reads /eval/catalog and /eval/catalog"
            ]
        );
    }

    #[test]
    fn build_run_command_persists_the_complete_managed_environment_before_the_command() {
        let cli = PtyCli::new(PathBuf::from("/my/catalog"));
        let mut t = target("hetz.demo.agent", "exec codex 'boot'");
        t.env.insert("CUSTOM".into(), "task-value".into());
        t.env.insert("ST_AGENT".into(), "hetz.demo".into());
        t.env.insert("ST_ROOT".into(), "$CATALOG/custom-bus".into());
        t.env.insert("TERM".into(), "screen-256color".into());
        t.env
            .insert("PTY_ROOT".into(), "/declared/root/must-not-win".into());
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));

        let args: Vec<String> = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        let separator = args.iter().position(|arg| arg == "--").unwrap();
        let mut persisted = BTreeMap::new();
        let mut index = 0;
        while index < separator {
            if args[index] == "--env" {
                let (key, value) = args[index + 1].split_once('=').unwrap();
                assert!(
                    persisted
                        .insert(key.to_string(), value.to_string())
                        .is_none(),
                    "the final managed overlay needs only one persisted value per key"
                );
                index += 2;
            } else {
                index += 1;
            }
        }
        let inherited = cmd
            .get_envs()
            .filter_map(|(key, value)| {
                value.map(|value| {
                    (
                        key.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            persisted, inherited,
            "initial process env and restart-persisted env must be the same resolved overlay"
        );
        assert_eq!(
            persisted.get("CATALOG").map(String::as_str),
            Some("/my/catalog")
        );
        assert_eq!(
            persisted.get("ST_ROOT").map(String::as_str),
            Some("/my/catalog/custom-bus")
        );
        assert_eq!(
            persisted.get("PTY_ROOT").map(String::as_str),
            Some(
                effective_pty_root(&cli.catalog_root)
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(
            persisted.get("TERM").map(String::as_str),
            Some("screen-256color")
        );
        assert_eq!(
            persisted.get("ST_AGENT").map(String::as_str),
            Some("hetz.demo")
        );
        assert_eq!(
            persisted.get("CUSTOM").map(String::as_str),
            Some("task-value")
        );
        assert!(persisted.contains_key("ST_HOOKS"));
    }

    #[test]
    fn build_run_command_omits_an_alias_equal_to_the_lifecycle_id() {
        let cli = PtyCli::default();
        let mut t = target("hetz.demo", "exec codex 'boot'");
        t.bus_id = t.pty_id.clone();
        t.presentation = Some(PtyPresentation {
            pty_id: t.pty_id.clone(),
            display_name: Some(Some(t.pty_id.clone())),
            tags: BTreeMap::new(),
        });
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            args.iter()
                .position(|arg| arg == "--id")
                .map(|position| args[position + 1].as_str()),
            Some("hetz.demo")
        );
        assert!(
            !args.iter().any(|arg| arg == "--name"),
            "pty rejects a display name equal to the stable session id"
        );
        assert!(
            args.iter().any(|arg| arg == "--no-display-name"),
            "without this flag pty would create an unrelated automatic alias"
        );
    }

    #[test]
    fn build_run_command_defaults_cwd_to_spec_dir_and_passes_tags_and_env() {
        let cli = PtyCli::default();
        let mut t = target("hetz.demo.agent", "exec claude 'boot'");
        t.tags.insert("role".into(), "agent".into());
        t.env.insert("ST_AGENT".into(), "hetz.demo-claude".into());
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));

        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let cwd_pos = args.iter().position(|a| a == "--cwd").unwrap();
        assert_eq!(args[cwd_pos + 1], "/cat/hetz/demo"); // no cwd, no workspace → spec dir
        let tag_pos = args.iter().position(|a| a == "--tag").unwrap();
        assert_eq!(args[tag_pos + 1], "role=agent");

        // env injected onto the child process
        let envs: BTreeMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("ST_AGENT"),
            Some(&Some("hetz.demo-claude".to_string()))
        );
        assert_eq!(
            envs.get("TERM"),
            Some(&Some("xterm-256color".to_string())),
            "headless st2 launches must not pass TERM=dumb into an interactive harness"
        );
        assert!(
            envs.get("ST_HOOKS")
                .and_then(Option::as_deref)
                .is_some_and(|path| !path.contains("/sets/sha256-")),
            "managed tasks keep ST_HOOKS at the receipt-bearing root; only rendered hook commands use a versioned set"
        );
    }

    #[test]
    fn managed_agent_scrubs_ambient_no_color_unless_explicitly_declared() {
        let cli = PtyCli::default();
        let agent = target("hetz.demo.agent", "exec claude 'boot'");
        let command = cli.build_run_command(&agent, Path::new("/cat/hetz/demo"));
        assert_eq!(
            command
                .get_envs()
                .find(|(key, _)| *key == OsStr::new("NO_COLOR"))
                .map(|(_, value)| value),
            Some(None),
            "ambient NO_COLOR must not silently disable an interactive agent's color"
        );
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--unset-env", "NO_COLOR"]),
            "the removal must be persisted for PTY restart"
        );

        let mut explicit = target("hetz.explicit.agent", "exec claude 'boot'");
        explicit.env.insert("NO_COLOR".into(), "1".into());
        let command = cli.build_run_command(&explicit, Path::new("/cat/hetz/explicit"));
        assert_eq!(
            command
                .get_envs()
                .find(|(key, _)| *key == OsStr::new("NO_COLOR"))
                .and_then(|(_, value)| value),
            Some(OsStr::new("1")),
            "an explicit Agent Spec NO_COLOR remains authoritative"
        );
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            !args
                .windows(2)
                .any(|pair| pair == ["--unset-env", "NO_COLOR"]),
            "an explicit assignment must not also persist a removal"
        );
    }

    #[test]
    fn non_agent_task_does_not_claim_no_color_policy() {
        let cli = PtyCli::default();
        let mut task = target("hetz.demo.sidecar", "exec sleep 1");
        task.name = "sidecar".into();
        let command = cli.build_run_command(&task, Path::new("/cat/hetz/demo"));

        assert!(
            command
                .get_envs()
                .all(|(key, _)| key != OsStr::new("NO_COLOR")),
            "non-agent services keep the caller's ambient NO_COLOR semantics"
        );
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            !args
                .windows(2)
                .any(|pair| pair == ["--unset-env", "NO_COLOR"]),
            "non-agent services must not persist an st2-owned removal"
        );
    }

    #[test]
    fn isolation_wrapper_preserves_environment_removals() {
        let mut inner = Command::new("pty");
        inner.env("TERM", "xterm-256color").env_remove("NO_COLOR");
        let mut outer = Command::new("systemd-run");

        apply_command_env(&inner, &mut outer);

        let env = outer
            .get_envs()
            .map(|(key, value)| (key.to_os_string(), value.map(OsStr::to_os_string)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get(OsStr::new("TERM")).and_then(Option::as_deref),
            Some(OsStr::new("xterm-256color"))
        );
        assert_eq!(env.get(OsStr::new("NO_COLOR")), Some(&None));
    }

    #[test]
    fn build_run_command_allows_a_task_to_override_the_default_term() {
        let cli = PtyCli::default();
        let mut t = target("hetz.demo.agent", "exec codex 'boot'");
        t.env.insert("TERM".into(), "screen-256color".into());
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let term = cmd
            .get_envs()
            .find(|(key, _)| *key == OsStr::new("TERM"))
            .and_then(|(_, value)| value)
            .map(|value| value.to_string_lossy().into_owned());
        assert_eq!(term.as_deref(), Some("screen-256color"));
    }

    #[test]
    fn build_run_command_defaults_cwd_to_workspace_when_task_cwd_absent() {
        let cli = PtyCli::default();
        let mut t = target("hetz.demo.agent", "exec claude 'boot'");
        t.workspace = Some("/repos/demo".into()); // no task cwd → workspace (spec.md §2)
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let cwd_pos = args.iter().position(|a| a == "--cwd").unwrap();
        assert_eq!(args[cwd_pos + 1], "/repos/demo");
    }

    #[test]
    fn build_run_command_expands_catalog_var_and_sets_it_in_env() {
        let cli = PtyCli::new(PathBuf::from("/my/catalog"));
        let mut t = target("hetz.demo.agent", "run");
        t.env.insert("DATA".into(), "$CATALOG/evals/x".into());
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));
        let envs: BTreeMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("DATA"),
            Some(&Some("/my/catalog/evals/x".to_string()))
        );
        assert_eq!(envs.get("CATALOG"), Some(&Some("/my/catalog".to_string())));
    }

    #[test]
    fn build_run_command_expands_vars_in_env_cwd_and_tags_but_not_command() {
        // Unique var name so the process-global set_var can't collide with a parallel test.
        let key = "ST2_TEST_EXPAND_NET_9f3";
        unsafe { std::env::set_var(key, "/net/xyz") }

        let cli = PtyCli::default();
        let mut t = target("hetz.demo.agent", "exec claude $ST2_TEST_EXPAND_NET_9f3/go");
        t.cwd = Some(format!("${key}/work"));
        t.tags.insert("net".into(), format!("${key}"));
        t.env.insert("ST_ROOT".into(), format!("${key}/custom-bus"));
        let cmd = cli.build_run_command(&t, Path::new("/cat/hetz/demo"));

        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        // cwd expanded (absolute → replaces the spec dir)
        let cwd_pos = args.iter().position(|a| a == "--cwd").unwrap();
        assert_eq!(args[cwd_pos + 1], "/net/xyz/work");
        // tag value expanded
        let tag_pos = args.iter().position(|a| a == "--tag").unwrap();
        assert_eq!(args[tag_pos + 1], "net=/net/xyz");
        // command left verbatim for sh -c to expand at spawn
        assert_eq!(
            args.last().unwrap(),
            "exec claude $ST2_TEST_EXPAND_NET_9f3/go"
        );

        // env value expanded
        let envs: std::collections::BTreeMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("ST_ROOT"),
            Some(&Some("/net/xyz/custom-bus".to_string()))
        );

        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn resolve_cwd_honors_relative_absolute_workspace_and_default() {
        let cli = PtyCli::default();
        let mut t = target("x", "y");
        // relative cwd → joined onto the spec dir
        t.cwd = Some("sub".into());
        assert_eq!(
            cli.resolve_cwd(&t, Path::new("/cat/hetz/demo")),
            Path::new("/cat/hetz/demo/sub")
        );
        // absolute cwd → replaces
        t.cwd = Some("/repos/fabric".into());
        assert_eq!(
            cli.resolve_cwd(&t, Path::new("/cat/hetz/demo")),
            Path::new("/repos/fabric")
        );
        // no cwd but a workspace → workspace
        t.cwd = None;
        t.workspace = Some("/repos/ws".into());
        assert_eq!(
            cli.resolve_cwd(&t, Path::new("/cat/hetz/demo")),
            Path::new("/repos/ws")
        );
        // neither → spec dir
        t.workspace = None;
        assert_eq!(
            cli.resolve_cwd(&t, Path::new("/cat/hetz/demo")),
            Path::new("/cat/hetz/demo")
        );
    }

    #[test]
    fn detect_host_returns_a_nonempty_short_name() {
        let h = detect_host();
        assert!(!h.is_empty());
        assert!(!h.contains('.'), "short name only, got {h}");
    }

    #[test]
    fn task_observation_of_missing_pty_root_is_complete_and_does_not_create_it() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        std::fs::create_dir(&catalog).unwrap();
        std::fs::write(
            catalog.join("catalog.kdl"),
            format!(
                "catalog {{ pty-root {:?} }}\n",
                tmp.path().join("missing-pty").display().to_string()
            ),
        )
        .unwrap();
        let root = effective_pty_root_from(&catalog, None);
        assert!(!root.exists());
        let batch = PtyCli::new(catalog).task_observations(&HashSet::from(["h.worker"]));
        assert!(batch.complete, "{:?}", batch.errors);
        assert!(batch.observations.is_empty());
        assert!(!root.exists(), "read-only observation created the PTY root");
    }

    #[test]
    fn unreadable_pty_root_evidence_is_indeterminate_not_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let loop_path = tmp.path().join("pty-loop");
        std::fs::create_dir(&catalog).unwrap();
        std::os::unix::fs::symlink(&loop_path, &loop_path).unwrap();
        std::fs::write(
            catalog.join("catalog.kdl"),
            format!(
                "catalog {{ pty-root {:?} }}\n",
                loop_path.display().to_string()
            ),
        )
        .unwrap();
        let batch = PtyCli::new(catalog)
            .task_observations_at_root(&HashSet::from(["h.worker"]), &loop_path);
        assert!(!batch.complete);
        assert!(batch.observations.is_empty());
        assert!(
            batch.errors[0].contains("cannot inspect PTY root"),
            "{:?}",
            batch.errors
        );
    }

    #[test]
    fn removed_and_recreated_pty_root_is_indeterminate_not_absent() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("pty");
        std::fs::create_dir(&root).unwrap();
        let fake = tmp.path().join("pty-bin");
        std::fs::write(
            &fake,
            "#!/bin/sh\nrmdir \"$PTY_ROOT\"\nmkdir \"$PTY_ROOT\"\nprintf '%s\\n' '[]'\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake, permissions).unwrap();

        let batch = PtyCli {
            bin: fake.display().to_string(),
            catalog_root: tmp.path().join("catalog"),
        }
        .task_observations_at_root(&HashSet::from(["h.worker"]), &root);
        assert!(!batch.complete);
        assert!(batch.observations.is_empty());
        assert!(
            batch.errors[0].contains("changed identity during observation"),
            "{:?}",
            batch.errors
        );
    }

    #[test]
    fn pty_task_observation_preserves_exact_generation_and_closed_states() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let pty_root = tmp.path().join("pty");
        std::fs::create_dir_all(&catalog).unwrap();
        std::fs::create_dir(&pty_root).unwrap();
        std::fs::write(
            catalog.join("catalog.kdl"),
            format!(
                "catalog {{ pty-root {:?} }}\n",
                pty_root.display().to_string()
            ),
        )
        .unwrap();
        let fake = tmp.path().join("pty-bin");
        std::fs::write(
            &fake,
            r#"#!/bin/sh
printf '%s\n' '[{"name":"h.live","status":"running","pid":41,"createdAt":"2026-07-31T10:00:00.000Z","displayName":"Build owner","tags":{"agent.presentation.schema":"1","unrelated":"preserved"}},{"name":"h.exit","status":"exited","exitCode":0,"pid":42,"createdAt":"2026-07-31T09:00:00.000Z"},{"name":"h.gone","status":"vanished","pid":43,"createdAt":"2026-07-31T08:00:00.000Z"}]'
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake, permissions).unwrap();

        let cli = PtyCli {
            bin: fake.display().to_string(),
            catalog_root: catalog,
        };
        let desired = HashSet::from(["h.live", "h.exit", "h.gone"]);
        let first = cli.task_observations(&desired);
        let second = cli.task_observations(&desired);
        assert!(first.complete, "{:?}", first.errors);
        assert_eq!(first, second, "same PTY evidence changed generation");
        let ObservedState::Running(generation) = &first.observations[0].state else {
            panic!("running PTY lost generation: {:?}", first.observations[0]);
        };
        assert_eq!(generation.pid(), 41);
        assert_eq!(generation.created_at(), "2026-07-31T10:00:00.000Z");
        assert!(generation.generation_id().starts_with("sha256:"));
        assert_eq!(first.observations[1].state, ObservedState::Exited);
        assert_eq!(first.observations[2].state, ObservedState::Vanished);

        let sessions = cli.list_sessions().unwrap();
        let presentation = sessions[0].presentation.as_ref().unwrap();
        assert_eq!(presentation.display_name.as_deref(), Some("Build owner"));
        assert_eq!(
            presentation
                .tags
                .get("agent.presentation.schema")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            presentation.tags.get("unrelated").map(String::as_str),
            Some("preserved")
        );
    }

    #[test]
    fn running_pty_without_complete_generation_is_indeterminate() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let pty_root = tmp.path().join("pty");
        std::fs::create_dir_all(&catalog).unwrap();
        std::fs::create_dir(&pty_root).unwrap();
        std::fs::write(
            catalog.join("catalog.kdl"),
            format!(
                "catalog {{ pty-root {:?} }}\n",
                pty_root.display().to_string()
            ),
        )
        .unwrap();
        let fake = tmp.path().join("pty-bin");
        std::fs::write(
            &fake,
            "#!/bin/sh\nprintf '%s\\n' '[{\"name\":\"h.live\",\"status\":\"running\"}]'\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake, permissions).unwrap();
        let batch = PtyCli {
            bin: fake.display().to_string(),
            catalog_root: catalog,
        }
        .task_observations(&HashSet::from(["h.live"]));
        assert!(!batch.complete);
        assert!(matches!(
            batch.observations[0].state,
            ObservedState::Indeterminate(_)
        ));
    }
}
