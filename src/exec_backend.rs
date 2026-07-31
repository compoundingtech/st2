//! The `exec` backend (M1b) — terminal-free process supervision (R09).
//!
//! `pty` tasks go to the `pty` CLI (which allocates a pseudo-terminal — an agent harness). `exec`
//! tasks (the ding, daemons, a stage's script) must NOT allocate a terminal, so st2 supervises them
//! directly: shell source through `sh -c`, or a structured argv with no shell. The process is spawned
//! in its own session (`setsid` — no controlling tty, and decoupled from st2 so it survives st2 exit,
//! Nomad-style), stdout/stderr go to a log file, and the pid is recorded in a machine-local
//! runner-state dir. Liveness is `kill(pid, 0)`; st2 best-effort reaps its own exited children so a
//! zombie never reads as alive. The same `setsid` session doubles as the teardown unit: an explicit
//! kill targets the whole process GROUP, so a task's children die with it (see
//! [`ExecBackend::kill`]) — a plain stop of st2 never touches it.
//!
//! State is machine-local (pids don't sync across hosts) and keyed by host, so a restarted st2 on the
//! same host adopts the exec processes it left running.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::host_lock::process_alive;
use crate::reconcile::{Session, TaskLaunch, TaskTarget};
use crate::run::resolve_task_cwd;

pub(crate) const EXEC_GENERATION_SCHEMA_V1: &str = "st2.exec-generation.v1";
pub(crate) const EXEC_GENERATION_SCHEMA_V2: &str = "st2.exec-generation.v2";

/// Exact Linux cgroup-v2 capability published with a generation. Path names
/// alone are never authority: retirement must reopen the path without
/// symlinks and match both device and inode.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecIsolation {
    pub kind: String,
    pub unit: String,
    pub cgroup_path: String,
    pub cgroup_device: u64,
    pub cgroup_inode: u64,
}

/// The immutable identity of one exec process generation.
///
/// `start_time_ticks` is the kernel's process-start token: Linux clock ticks since boot, or the
/// macOS process start timestamp in microseconds. It is deliberately paired with the pid: a reused
/// pid can never make a different generation read as running.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecGeneration {
    pub schema: String,
    pub pid: u32,
    pub created_at: String,
    pub start_time_ticks: u64,
    pub generation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation: Option<ExecIsolation>,
}

/// A generation observation is explicitly indeterminate when legacy evidence cannot safely identify
/// a process. Callers must not collapse that case into "running".
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecGenerationObservation {
    Known {
        generation: ExecGeneration,
        alive: bool,
    },
    Indeterminate {
        pid: Option<i32>,
        reason: String,
        /// Conservative compatibility projection for the old boolean `Session` API.
        alive_for_reconcile: bool,
    },
}

/// Supervises `exec` tasks as terminal-free processes.
pub struct ExecBackend {
    /// Where `<id>.pid` lives (machine-local; not synced).
    state_dir: PathBuf,
    /// The catalog root — the value of `$CATALOG` during expansion.
    catalog_root: PathBuf,
}

impl ExecBackend {
    pub fn new(state_dir: PathBuf, catalog_root: PathBuf) -> Self {
        Self {
            state_dir,
            catalog_root,
        }
    }

    fn pid_path(&self, id: &str) -> PathBuf {
        self.state_dir.join(format!("{id}.pid"))
    }
    /// Where an exec's stdout+stderr are auto-logged: `<catalog>/logs/<full-label>.log` — a
    /// discoverable, greppable location keyed by the task's full label (e.g. `logs/mix.sup.ding.log`).
    /// Execs run as detached sidecars, so without this a wedged/crashed ding's output vanishes; here it
    /// stays inspectable after the fact. (Distinct from the machine-local pid state in `state_dir`.)
    fn log_path(&self, id: &str) -> PathBuf {
        self.catalog_root.join("logs").join(format!("{id}.log"))
    }

    /// The one bounded prior generation retained across a crash restart.
    fn previous_log_path(&self, id: &str) -> PathBuf {
        self.catalog_root.join("logs").join(format!("{id}.log.1"))
    }

    /// Spawn `target` as a terminal-free, detached process and record its pid.
    pub fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
        fs::create_dir_all(&self.state_dir)?;
        let cwd = resolve_task_cwd(target, spec_dir, &self.catalog_root);
        let log_file = self.log_path(&target.pty_id);
        if let Some(parent) = log_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let log = fs::File::options()
            .create(true)
            .append(true)
            .open(&log_file)?;

        // Spawn the task into its own isolation domain (R21b): on systemd Linux, its own transient
        // scope (own cgroup, sibling of the transport unit) so a transport/supervisor cgroup-cascade
        // kill cannot take it; elsewhere a plain pass-through detached by the `setsid` below. Env, cwd,
        // and stdio set here reach the task in both modes.
        let unit = crate::isolate::scope_unit(&target.pty_id);
        let (program, args): (OsString, Vec<OsString>) = match &target.launch {
            TaskLaunch::Shell(command) => (
                OsString::from("sh"),
                vec![OsString::from("-c"), OsString::from(command)],
            ),
            TaskLaunch::Argv(argv) => {
                debug_assert!(!argv.is_empty());
                let mut expanded = argv
                    .iter()
                    .map(|arg| {
                        OsString::from(crate::expand::expand_catalog(arg, &self.catalog_root))
                    })
                    .collect::<Vec<_>>();
                let program = expanded.remove(0);
                (program, expanded)
            }
        };
        let arg_refs = args.iter().map(OsString::as_os_str).collect::<Vec<_>>();
        let mut cmd = crate::isolate::wrap(&unit, &program, &arg_refs);
        cmd.current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .env("CATALOG", &self.catalog_root)
            .env("ST_ROOT", &self.catalog_root)
            .env(
                "PTY_ROOT",
                crate::run::effective_pty_root(&self.catalog_root),
            );
        if let Ok(path) = crate::hooks::hooks_root() {
            cmd.env("ST_HOOKS", path);
        }
        for (k, v) in &target.env {
            // PTY_ROOT resolves to the EFFECTIVE root (an exported ambient one wins over the rendered
            // `$CATALOG/pty`) so a ding — an exec task that reads PTY_ROOT to find the pty it pokes —
            // targets the same partition st2's pty ops use.
            if k == "PTY_ROOT" {
                cmd.env(
                    "PTY_ROOT",
                    crate::run::effective_pty_root(&self.catalog_root),
                );
            } else {
                cmd.env(k, crate::expand::expand_catalog(v, &self.catalog_root));
            }
        }
        // New session: no controlling terminal (R09) and decoupled from st2's process group, so it
        // survives st2 exit and `kill(-pid)` teardown reaps the whole group. In Scope mode this runs
        // on the outer `systemd-run`, which exec-chains into the workload, so the workload is the
        // session leader and `child.id()` is its pid. Still st2's child, so st2 can reap it (see `list`).
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }

        let child = cmd.spawn()?;
        let pid = child.id();
        let start_time_ticks = match process_start_time_ticks(pid as i32) {
            Ok(value) => value,
            Err(error) => {
                // Never leave a live process behind without its generation record.
                terminate_unpublished(pid);
                return Err(error).with_context(|| {
                    format!("identifying spawned exec generation '{}'", target.pty_id)
                });
            }
        };
        let created_at = match rfc3339_utc(SystemTime::now()) {
            Ok(created_at) => created_at,
            Err(error) => {
                terminate_unpublished(pid);
                return Err(error)
                    .with_context(|| format!("timestamping exec generation '{}'", target.pty_id));
            }
        };
        let isolation = match crate::isolate::mode() {
            crate::isolate::Isolation::Scope => match capture_scope_isolation(pid, &unit) {
                Ok(isolation) => Some(isolation),
                Err(error) => {
                    // A legitimately short task can exit and have its `--collect` scope removed
                    // before the parent gets scheduled to capture it. It no longer needs a kill
                    // capability; retain a v1 record so observation/restart can report the exit.
                    // A still-running task without exact scope authority is never published.
                    let exited = unsafe {
                        let mut status = 0;
                        libc::waitpid(pid as i32, &mut status, libc::WNOHANG) == pid as i32
                    };
                    if exited {
                        None
                    } else {
                        terminate_unpublished(pid);
                        return Err(error).with_context(|| {
                            format!("capturing exact exec scope '{}'", target.pty_id)
                        });
                    }
                }
            },
            crate::isolate::Isolation::Detached | crate::isolate::Isolation::DegradedDetached => {
                None
            }
        };
        let schema = if isolation.is_some() {
            EXEC_GENERATION_SCHEMA_V2
        } else {
            EXEC_GENERATION_SCHEMA_V1
        };
        let generation = ExecGeneration {
            schema: schema.to_string(),
            pid,
            generation_id: generation_id(
                &target.pty_id,
                pid,
                &created_at,
                start_time_ticks,
                isolation.as_ref(),
            ),
            created_at,
            start_time_ticks,
            isolation,
        };
        if let Err(error) = self.publish_generation(&target.pty_id, &generation) {
            // Publication is the ownership boundary. If it fails, tear down the otherwise-untracked
            // process group before returning the error.
            terminate_unpublished(pid);
            return Err(error);
        }
        // Detach: dropping Child neither waits nor kills; `list` reaps exited children.
        drop(child);
        Ok(())
    }

    /// List known exec tasks with liveness. Best-effort reaps our own exited children first so a
    /// zombie (whose pid still exists) is not mistaken for alive.
    pub fn list(&self) -> anyhow::Result<Vec<Session>> {
        let mut out = Vec::new();
        let dir = match fs::read_dir(&self.state_dir) {
            Ok(d) => d,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(error) => return Err(error.into()),
        };
        for entry in dir.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("pid") {
                continue;
            }
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let observation = self.observe_generation_path(id, &path);
            let alive = match observation {
                ExecGenerationObservation::Known {
                    generation,
                    alive: _,
                } => {
                    // Reap if it's our exited child (ECHILD after an st2 restart is fine — it's now
                    // init's). Re-observe afterwards so a reaped zombie does not read as alive.
                    unsafe {
                        let mut status = 0;
                        libc::waitpid(generation.pid as i32, &mut status, libc::WNOHANG);
                    }
                    observation_alive_for_reconcile(self.observe_generation_path(id, &path))
                }
                ExecGenerationObservation::Indeterminate {
                    alive_for_reconcile,
                    ..
                } => alive_for_reconcile,
            };
            out.push(Session {
                pty_id: id.to_string(),
                alive,
                exit_code: None,
            });
        }
        Ok(out)
    }

    /// SIGTERM the whole task — the process GROUP, not just the recorded pid. `spawn` puts each exec
    /// task in its own session via `setsid`, so the recorded pid is the group leader (pgid == pid) and
    /// `kill(-pid)` reaps the leader AND anything it forked: a `sh -c` wrapper's grandchild (dash forks
    /// rather than exec-replacing a bare command), a compound command's pipeline, a daemon's workers.
    /// Killing only the leader would leave the real workload orphaned — an incomplete teardown, which
    /// is precisely the guarantee st2 must not break. Targeting the group is safe: setsid always
    /// succeeds for a freshly-forked child (never itself a group leader), so the group is the task's
    /// own, never st2's.
    pub fn kill(&self, id: &str) -> anyhow::Result<()> {
        let pid = match self.observe_generation(id)? {
            ExecGenerationObservation::Known {
                generation,
                alive: true,
            } => generation.pid as i32,
            ExecGenerationObservation::Known { alive: false, .. } => return Ok(()),
            ExecGenerationObservation::Indeterminate { reason, .. } => {
                anyhow::bail!("exec generation for '{id}' is indeterminate: {reason}")
            }
        };
        // Negative target = the process group led by `pid`.
        let ret = unsafe { libc::kill(-pid, libc::SIGTERM) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            // ESRCH (the whole group is already gone) is not a failure for our purposes.
            if err.raw_os_error() != Some(libc::ESRCH) {
                anyhow::bail!("kill exec group '{id}' (pgid {pid}): {err}");
            }
        }
        Ok(())
    }

    /// Reap a crashed exec before restarting it: remove only the stale pid and rotate the current
    /// diagnostic log to one bounded prior generation. The next spawn creates a fresh current log,
    /// leaving `<id>.log` + `<id>.log.1` inspectable without unbounded crash-loop accumulation.
    pub fn reap_for_restart(&self, id: &str) -> anyhow::Result<()> {
        let current = self.log_path(id);
        if current.exists() {
            let previous = self.previous_log_path(id);
            let _ = fs::remove_file(&previous);
            fs::rename(&current, &previous)?;
        }
        // Remove the pid only after diagnostics are safely rotated. If rotation fails, retaining the
        // dead pid lets the next reconciliation retry instead of treating the task as a fresh launch
        // and appending a new execution to the unrotated log.
        let _ = fs::remove_file(self.pid_path(id));
        Ok(())
    }

    /// Finally remove an exec task's runner-state and both bounded log generations. This is the
    /// retirement/final-GC path, not ordinary crash relaunch.
    pub fn remove(&self, id: &str) -> anyhow::Result<()> {
        let _ = fs::remove_file(self.pid_path(id));
        let _ = fs::remove_file(self.log_path(id));
        let _ = fs::remove_file(self.previous_log_path(id));
        Ok(())
    }

    /// Observe one exec without ever treating a reused pid as the recorded generation.
    pub fn observe_generation(&self, id: &str) -> anyhow::Result<ExecGenerationObservation> {
        self.observe_generation_optional(id)?
            .ok_or_else(|| anyhow::anyhow!("reading pid for exec '{id}': no generation record"))
    }

    /// Observe exactly one desired exec id. `None` means only that its state record is absent.
    pub fn observe_generation_optional(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<ExecGenerationObservation>> {
        let path = self.pid_path(id);
        match path.try_exists() {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("checking exec generation {}", path.display()));
            }
        }
        Ok(Some(self.observe_generation_path(id, &path)))
    }

    fn observe_generation_path(&self, id: &str, path: &Path) -> ExecGenerationObservation {
        let raw = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) => {
                return indeterminate(None, format!("reading {}: {error}", path.display()), true);
            }
        };
        if let Ok(pid) = raw.trim().parse::<i32>() {
            return self.observe_legacy_generation(id, path, pid);
        }
        let generation: ExecGeneration = match serde_json::from_str(&raw) {
            Ok(record) => record,
            Err(error) => {
                return indeterminate(None, format!("strict JSON parse failed: {error}"), true);
            }
        };
        if let Err(reason) = validate_generation(id, &generation) {
            return indeterminate(Some(generation.pid as i32), reason, true);
        }
        match generation_process_state(&generation) {
            GenerationProcessState::Running => ExecGenerationObservation::Known {
                generation,
                alive: true,
            },
            GenerationProcessState::Exited => ExecGenerationObservation::Known {
                generation,
                alive: false,
            },
            GenerationProcessState::Mismatch => indeterminate(
                Some(generation.pid as i32),
                "recorded startTimeTicks does not match the live pid",
                true,
            ),
        }
    }

    fn observe_legacy_generation(
        &self,
        id: &str,
        path: &Path,
        pid: i32,
    ) -> ExecGenerationObservation {
        if pid <= 0 || !process_alive(pid) {
            return indeterminate(
                Some(pid),
                "legacy pid is not a live process",
                process_alive(pid),
            );
        }
        let start_time_ticks = match process_start_time_ticks(pid) {
            Ok(value) => value,
            Err(error) => {
                return indeterminate(
                    Some(pid),
                    format!("cannot read legacy process start token: {error:#}"),
                    true,
                );
            }
        };
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) => {
                return indeterminate(
                    Some(pid),
                    format!("cannot stat legacy pid file: {error}"),
                    true,
                );
            }
        };
        let modified = match metadata.modified() {
            Ok(modified) => modified,
            Err(error) => {
                return indeterminate(
                    Some(pid),
                    format!("legacy pid file has no usable mtime: {error}"),
                    true,
                );
            }
        };
        match legacy_pid_predates_record(pid, start_time_ticks, modified) {
            Ok(true) => {}
            Ok(false) => {
                return indeterminate(
                    Some(pid),
                    "legacy pid file predates the current process generation",
                    true,
                );
            }
            Err(error) => {
                return indeterminate(
                    Some(pid),
                    format!("cannot order legacy pid file and process start: {error:#}"),
                    true,
                );
            }
        }
        // Read the start token again after all legacy evidence. A process that changed under us is
        // never promoted to a strict record.
        if process_start_time_ticks(pid).ok() != Some(start_time_ticks) {
            return indeterminate(
                Some(pid),
                "legacy process generation changed while observed",
                true,
            );
        }
        let created_at = match rfc3339_utc(modified) {
            Ok(value) => value,
            Err(error) => {
                return indeterminate(
                    Some(pid),
                    format!("cannot encode legacy pid-file mtime: {error:#}"),
                    true,
                );
            }
        };
        let generation = ExecGeneration {
            schema: EXEC_GENERATION_SCHEMA_V1.to_string(),
            pid: pid as u32,
            generation_id: generation_id(id, pid as u32, &created_at, start_time_ticks, None),
            created_at,
            start_time_ticks,
            isolation: None,
        };
        // Observation is read-only. Existing legacy files remain unchanged until normal lifecycle
        // replacement; only future spawns publish strict JSON.
        ExecGenerationObservation::Known {
            generation,
            alive: true,
        }
    }

    fn publish_generation(&self, id: &str, generation: &ExecGeneration) -> anyhow::Result<()> {
        let path = self.pid_path(id);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("exec generation path has no parent"))?;
        fs::create_dir_all(parent)?;
        let mut bytes = serde_json::to_vec(generation)?;
        bytes.push(b'\n');
        let mut temp = tempfile::Builder::new()
            .prefix(".exec-generation.")
            .tempfile_in(parent)?;
        temp.write_all(&bytes)?;
        temp.as_file().sync_all()?;
        temp.persist(&path)
            .map_err(|error| error.error)
            .with_context(|| format!("publishing exec generation {}", path.display()))?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn terminate_unpublished(pid: u32) {
    unsafe {
        libc::kill(-(pid as i32), libc::SIGTERM);
        libc::waitpid(pid as i32, std::ptr::null_mut(), 0);
    }
}

#[cfg(target_os = "linux")]
fn capture_scope_isolation(pid: u32, unit: &str) -> anyhow::Result<ExecIsolation> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match capture_scope_isolation_once(pid, unit) {
            Ok(capability) => return Ok(capability),
            Err(error) if std::time::Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(target_os = "linux")]
fn capture_scope_isolation_once(pid: u32, unit: &str) -> anyhow::Result<ExecIsolation> {
    let cgroup = fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .with_context(|| format!("read /proc/{pid}/cgroup"))?;
    let cgroup_path = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| anyhow::anyhow!("process {pid} is not in unified cgroup v2"))?;
    anyhow::ensure!(
        Path::new(cgroup_path)
            .file_name()
            .and_then(|name| name.to_str())
            == Some(unit),
        "process {pid} cgroup {:?} does not name exact scope {unit:?}",
        cgroup_path
    );

    let control_group = Command::new("systemctl")
        .args(["--user", "show", unit, "--property=ControlGroup", "--value"])
        .output()
        .with_context(|| format!("query exact systemd scope {unit}"))?;
    anyhow::ensure!(
        control_group.status.success(),
        "systemd did not confirm exact scope {unit}: {}",
        String::from_utf8_lossy(&control_group.stderr).trim()
    );
    let control_group = String::from_utf8(control_group.stdout)?.trim().to_string();
    anyhow::ensure!(
        control_group == cgroup_path,
        "scope {unit} ControlGroup {:?} differs from process cgroup {:?}",
        control_group,
        cgroup_path
    );

    let relative = cgroup_path
        .strip_prefix('/')
        .ok_or_else(|| anyhow::anyhow!("cgroup path must be absolute"))?;
    anyhow::ensure!(
        !relative
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".."),
        "cgroup path contains an unsafe component"
    );
    let path = Path::new("/sys/fs/cgroup").join(relative);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("stat exact cgroup {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "exact cgroup is not a real directory: {}",
        path.display()
    );
    let members = fs::read_to_string(path.join("cgroup.procs"))
        .with_context(|| format!("read exact cgroup members {}", path.display()))?;
    anyhow::ensure!(
        members.lines().any(|line| line.trim() == pid.to_string()),
        "exact scope {unit} does not contain leader {pid}"
    );
    for control in ["cgroup.freeze", "cgroup.events", "cgroup.kill"] {
        let metadata = fs::symlink_metadata(path.join(control))
            .with_context(|| format!("stat exact cgroup control {control}"))?;
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "exact cgroup control {control} is not a real file"
        );
    }
    Ok(ExecIsolation {
        kind: "systemd-cgroup-v2-scope".to_string(),
        unit: unit.to_string(),
        cgroup_path: cgroup_path.to_string(),
        cgroup_device: metadata.dev(),
        cgroup_inode: metadata.ino(),
    })
}

#[cfg(not(target_os = "linux"))]
fn capture_scope_isolation(_pid: u32, _unit: &str) -> anyhow::Result<ExecIsolation> {
    anyhow::bail!("exact cgroup-v2 scope capture is unsupported on this platform")
}

fn indeterminate(
    pid: Option<i32>,
    reason: impl Into<String>,
    alive_for_reconcile: bool,
) -> ExecGenerationObservation {
    ExecGenerationObservation::Indeterminate {
        pid,
        reason: reason.into(),
        alive_for_reconcile,
    }
}

fn observation_alive_for_reconcile(observation: ExecGenerationObservation) -> bool {
    match observation {
        ExecGenerationObservation::Known { alive, .. } => alive,
        ExecGenerationObservation::Indeterminate {
            alive_for_reconcile,
            ..
        } => alive_for_reconcile,
    }
}

pub(crate) fn validate_generation(id: &str, generation: &ExecGeneration) -> Result<(), String> {
    if !matches!(
        generation.schema.as_str(),
        EXEC_GENERATION_SCHEMA_V1 | EXEC_GENERATION_SCHEMA_V2
    ) {
        return Err(format!("unsupported schema {:?}", generation.schema));
    }
    match (generation.schema.as_str(), generation.isolation.as_ref()) {
        (EXEC_GENERATION_SCHEMA_V1, None) => {}
        (EXEC_GENERATION_SCHEMA_V2, Some(isolation)) => {
            if isolation.kind != "systemd-cgroup-v2-scope"
                || !isolation.unit.ends_with(".scope")
                || !isolation.cgroup_path.starts_with('/')
                || isolation.cgroup_path.contains("/../")
                || isolation.cgroup_device == 0
                || isolation.cgroup_inode == 0
            {
                return Err("invalid exact cgroup-v2 isolation capability".to_string());
            }
        }
        _ => return Err("generation schema and isolation capability disagree".to_string()),
    }
    if generation.pid == 0 || generation.pid > i32::MAX as u32 {
        return Err("pid must be positive".to_string());
    }
    if !crate::task_inventory::is_rfc3339_utc_millis(&generation.created_at) {
        return Err("createdAt must be an RFC3339 UTC timestamp with milliseconds".to_string());
    }
    let expected = generation_id(
        id,
        generation.pid,
        &generation.created_at,
        generation.start_time_ticks,
        generation.isolation.as_ref(),
    );
    if generation.generation_id != expected {
        return Err("generationId does not match the generation fields".to_string());
    }
    Ok(())
}

enum GenerationProcessState {
    Running,
    Exited,
    Mismatch,
}

fn generation_process_state(generation: &ExecGeneration) -> GenerationProcessState {
    let pid = generation.pid as i32;
    if !process_alive(pid) {
        return GenerationProcessState::Exited;
    }
    let first = process_start_time_ticks(pid).ok();
    if first != Some(generation.start_time_ticks) {
        return if process_alive(pid) {
            GenerationProcessState::Mismatch
        } else {
            GenerationProcessState::Exited
        };
    }
    // The second read closes the check/use window enough for an observation: a generation change at
    // either edge makes the result non-running.
    if !process_alive(pid) {
        GenerationProcessState::Exited
    } else if process_start_time_ticks(pid).ok() == Some(generation.start_time_ticks) {
        GenerationProcessState::Running
    } else {
        GenerationProcessState::Mismatch
    }
}

fn generation_id(
    runtime_id: &str,
    pid: u32,
    created_at: &str,
    start_time_ticks: u64,
    isolation: Option<&ExecIsolation>,
) -> String {
    let base = crate::task_inventory::generation_id(
        "exec",
        runtime_id,
        pid,
        created_at,
        Some(start_time_ticks),
    );
    let Some(isolation) = isolation else {
        return base;
    };
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"st2.exec-generation.v2\0");
    for field in [
        base.as_bytes(),
        isolation.kind.as_bytes(),
        isolation.unit.as_bytes(),
        isolation.cgroup_path.as_bytes(),
        isolation.cgroup_device.to_string().as_bytes(),
        isolation.cgroup_inode.to_string().as_bytes(),
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(target_os = "linux")]
fn process_start_time_ticks(pid: i32) -> anyhow::Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let after_comm = stat
        .rsplit_once(") ")
        .ok_or_else(|| anyhow::anyhow!("malformed /proc/{pid}/stat"))?
        .1;
    // Fields after comm begin at field 3 (state); starttime is field 22, therefore index 19.
    after_comm
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| anyhow::anyhow!("missing starttime in /proc/{pid}/stat"))?
        .parse()
        .with_context(|| format!("parsing starttime in /proc/{pid}/stat"))
}

#[cfg(target_os = "macos")]
fn process_start_time_ticks(pid: i32) -> anyhow::Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::proc_taskallinfo>::zeroed();
    let expected = std::mem::size_of::<libc::proc_taskallinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKALLINFO,
            0,
            info.as_mut_ptr().cast(),
            expected,
        )
    };
    if read != expected {
        anyhow::bail!("proc_pidinfo({pid}) returned {read}, expected {expected}");
    }
    let info = unsafe { info.assume_init() };
    Ok(info
        .pbsd
        .pbi_start_tvsec
        .saturating_mul(1_000_000)
        .saturating_add(info.pbsd.pbi_start_tvusec))
}

#[cfg(target_os = "linux")]
fn legacy_pid_predates_record(
    _pid: i32,
    start_time_ticks: u64,
    modified: SystemTime,
) -> anyhow::Result<bool> {
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_second <= 0 {
        anyhow::bail!("sysconf(_SC_CLK_TCK) failed");
    }
    let uptime: f64 = fs::read_to_string("/proc/uptime")?
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing /proc/uptime value"))?
        .parse()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let modified = modified.duration_since(UNIX_EPOCH)?.as_secs_f64();
    let tick = 1.0 / ticks_per_second as f64;
    // A start token denotes a clock-tick bucket. Requiring the pid-file mtime to follow the *end* of
    // that bucket means a process created after the legacy record (pid reuse) cannot be accepted.
    let latest_start = now - uptime + start_time_ticks as f64 * tick + tick;
    Ok(modified >= latest_start)
}

#[cfg(target_os = "macos")]
fn legacy_pid_predates_record(
    _pid: i32,
    start_time_ticks: u64,
    modified: SystemTime,
) -> anyhow::Result<bool> {
    let modified_micros = modified.duration_since(UNIX_EPOCH)?.as_micros();
    Ok(u128::from(start_time_ticks) <= modified_micros)
}

fn rfc3339_utc(time: SystemTime) -> anyhow::Result<String> {
    let duration = time.duration_since(UNIX_EPOCH)?;
    let seconds = duration.as_secs() as libc::time_t;
    let millis = duration.subsec_millis();
    let mut tm = std::mem::MaybeUninit::<libc::tm>::zeroed();
    if unsafe { libc::gmtime_r(&seconds, tm.as_mut_ptr()) }.is_null() {
        anyhow::bail!("gmtime_r failed");
    }
    let tm = unsafe { tm.assume_init() };
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        millis
    ))
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::thread::sleep;
    use std::time::Duration;

    fn target(id: &str) -> TaskTarget {
        TaskTarget {
            kind: crate::spec::TaskKind::Exec,
            pty_id: id.to_string(),
            bus_id: "host.test".to_string(),
            name: "probe".to_string(),
            launch: TaskLaunch::Shell("sleep 30".to_string()),
            cwd: None,
            workspace: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
        }
    }

    #[test]
    fn spawn_atomically_publishes_strict_generation_json() {
        let temp = tempfile::tempdir().unwrap();
        let catalog = temp.path().join("catalog");
        fs::create_dir_all(&catalog).unwrap();
        let backend = ExecBackend::new(temp.path().join("state"), catalog);
        let id = "host.test.probe";

        backend.spawn(&target(id), temp.path()).unwrap();
        let raw = fs::read_to_string(backend.pid_path(id)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert!(
            keys == [
                "createdAt",
                "generationId",
                "pid",
                "schema",
                "startTimeTicks",
            ] || keys
                == [
                    "createdAt",
                    "generationId",
                    "isolation",
                    "pid",
                    "schema",
                    "startTimeTicks",
                ],
            "unexpected generation keys: {keys:?}"
        );
        let generation: ExecGeneration = serde_json::from_value(value).unwrap();
        assert!(matches!(
            generation.schema.as_str(),
            EXEC_GENERATION_SCHEMA_V1 | EXEC_GENERATION_SCHEMA_V2
        ));
        assert!(crate::task_inventory::is_rfc3339_utc_millis(
            &generation.created_at
        ));
        assert_eq!(
            generation.generation_id,
            generation_id(
                id,
                generation.pid,
                &generation.created_at,
                generation.start_time_ticks,
                generation.isolation.as_ref(),
            )
        );
        assert!(matches!(
            backend.observe_generation(id).unwrap(),
            ExecGenerationObservation::Known { alive: true, .. }
        ));

        backend.kill(id).unwrap();
        backend.remove(id).unwrap();
    }

    #[test]
    fn replacement_exec_process_gets_a_new_generation_id() {
        let temp = tempfile::tempdir().unwrap();
        let catalog = temp.path().join("catalog");
        fs::create_dir_all(&catalog).unwrap();
        let backend = ExecBackend::new(temp.path().join("state"), catalog);
        let id = "host.test.replacement";

        backend.spawn(&target(id), temp.path()).unwrap();
        let first = match backend.observe_generation(id).unwrap() {
            ExecGenerationObservation::Known { generation, .. } => generation.generation_id,
            observation => panic!("unexpected first generation: {observation:?}"),
        };
        backend.kill(id).unwrap();
        let mut exited = false;
        for _ in 0..50 {
            if backend
                .list()
                .unwrap()
                .iter()
                .any(|session| session.pty_id == id && !session.alive)
            {
                exited = true;
                break;
            }
            sleep(Duration::from_millis(20));
        }
        assert!(exited, "first exec generation did not exit");
        backend.reap_for_restart(id).unwrap();
        backend.spawn(&target(id), temp.path()).unwrap();
        let second = match backend.observe_generation(id).unwrap() {
            ExecGenerationObservation::Known { generation, .. } => generation.generation_id,
            observation => panic!("unexpected replacement generation: {observation:?}"),
        };
        assert_ne!(second, first);

        backend.kill(id).unwrap();
        backend.remove(id).unwrap();
    }

    #[test]
    fn strict_json_rejects_unknown_fields_as_indeterminate() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let backend = ExecBackend::new(state, temp.path().join("catalog"));
        let id = "host.test.unknown";
        fs::write(
            backend.pid_path(id),
            format!(
                "{{\"schema\":\"{EXEC_GENERATION_SCHEMA_V1}\",\"pid\":{},\
                 \"createdAt\":\"2026-07-31T00:00:00.000Z\",\"startTimeTicks\":1,\
                 \"generationId\":\"sha256:nope\",\"extra\":true}}",
                std::process::id()
            ),
        )
        .unwrap();

        assert!(matches!(
            backend.observe_generation(id).unwrap(),
            ExecGenerationObservation::Indeterminate { reason, .. }
                if reason.contains("unknown field")
        ));
        assert!(
            backend.list().unwrap()[0].alive,
            "malformed evidence must not trigger a duplicate relaunch"
        );
    }

    #[test]
    fn start_token_mismatch_cannot_report_running_or_signal_reused_pid() {
        let temp = tempfile::tempdir().unwrap();
        let catalog = temp.path().join("catalog");
        fs::create_dir_all(&catalog).unwrap();
        let backend = ExecBackend::new(temp.path().join("state"), catalog);
        let id = "host.test.reused";
        backend.spawn(&target(id), temp.path()).unwrap();

        let path = backend.pid_path(id);
        let mut generation: ExecGeneration =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let actual_pid = generation.pid;
        generation.start_time_ticks = generation.start_time_ticks.saturating_add(1);
        generation.generation_id = generation_id(
            id,
            generation.pid,
            &generation.created_at,
            generation.start_time_ticks,
            generation.isolation.as_ref(),
        );
        backend.publish_generation(id, &generation).unwrap();

        assert!(matches!(
            backend.observe_generation(id).unwrap(),
            ExecGenerationObservation::Indeterminate {
                alive_for_reconcile: true,
                ..
            }
        ));
        assert!(
            backend
                .kill(id)
                .unwrap_err()
                .to_string()
                .contains("indeterminate"),
            "kill must refuse a generation mismatch"
        );
        assert!(
            backend.list().unwrap()[0].alive,
            "a live reused pid must not trigger a duplicate relaunch"
        );
        assert!(
            process_alive(actual_pid as i32),
            "a start-token mismatch must not signal the current pid"
        );

        unsafe {
            libc::kill(-(actual_pid as i32), libc::SIGTERM);
        }
        backend.remove(id).unwrap();
    }

    #[test]
    fn legacy_plain_pid_is_observed_read_only_with_trustworthy_mtime_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let backend = ExecBackend::new(state, temp.path().join("catalog"));
        let id = "host.test.legacy";
        let pid = std::process::id() as i32;
        // This process predates the pid-file by far more than one kernel tick, making the legacy
        // ordering proof unambiguous.
        sleep(Duration::from_millis(20));
        let legacy = pid.to_string();
        fs::write(backend.pid_path(id), &legacy).unwrap();

        assert!(matches!(
            backend.observe_generation(id).unwrap(),
            ExecGenerationObservation::Known {
                generation,
                alive: true
            } if generation.pid == pid as u32
                && generation.schema == EXEC_GENERATION_SCHEMA_V1
                && crate::task_inventory::is_rfc3339_utc_millis(&generation.created_at)
        ));
        assert_eq!(
            fs::read_to_string(backend.pid_path(id)).unwrap(),
            legacy,
            "observation must not migrate or rewrite legacy state"
        );
        backend.remove(id).unwrap();
    }

    #[test]
    fn unavailable_legacy_process_is_indeterminate_not_running() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let backend = ExecBackend::new(state, temp.path().join("catalog"));
        let id = "host.test.dead-legacy";
        fs::write(backend.pid_path(id), "2000000000").unwrap();

        assert!(matches!(
            backend.observe_generation(id).unwrap(),
            ExecGenerationObservation::Indeterminate { pid, .. }
                if pid == Some(2_000_000_000)
        ));
        assert!(!backend.list().unwrap()[0].alive);
    }

    #[test]
    fn optional_observation_distinguishes_absence_from_indeterminate_state() {
        let temp = tempfile::tempdir().unwrap();
        let backend = ExecBackend::new(temp.path().join("state"), temp.path().join("catalog"));
        assert_eq!(
            backend
                .observe_generation_optional("host.test.absent")
                .unwrap(),
            None
        );
    }
}
