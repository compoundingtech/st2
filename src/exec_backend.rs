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
use std::ffi::OsString;
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::host_lock::process_alive;
use crate::reconcile::{Session, TaskLaunch, TaskTarget};
use crate::run::resolve_task_cwd;

/// Read-only identity observation for the existing plain-PID exec record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecGenerationObservation {
    Running {
        pid: u32,
        created_at: String,
        generation_id: String,
    },
    Indeterminate {
        reason: String,
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
        fs::write(self.pid_path(&target.pty_id), child.id().to_string())?;
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
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("reading exec pid record {}", path.display()))?;
            let pid = raw
                .trim()
                .parse::<i32>()
                .with_context(|| format!("parsing exec pid record {}", path.display()))?;
            // Reap if it's our exited child (ECHILD after an st2 restart is fine — it's now init's).
            unsafe {
                let mut status = 0;
                libc::waitpid(pid, &mut status, libc::WNOHANG);
            }
            out.push(Session {
                pty_id: id.to_string(),
                alive: process_alive(pid),
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
        let pid = self.read_pid(id)?;
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

    /// Observe exactly one desired exec id without changing its state record or
    /// any lifecycle behavior. `None` means only that the record is absent.
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
        Ok(Some(observe_legacy_generation(id, &path)))
    }

    fn read_pid(&self, id: &str) -> anyhow::Result<i32> {
        let raw = fs::read_to_string(self.pid_path(id))
            .map_err(|e| anyhow::anyhow!("reading pid for exec '{id}': {e}"))?;
        raw.trim()
            .parse::<i32>()
            .map_err(|_| anyhow::anyhow!("bad pid file for exec '{id}'"))
    }
}

fn observe_legacy_generation(id: &str, path: &Path) -> ExecGenerationObservation {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => {
            return indeterminate(format!("reading {}: {error}", path.display()));
        }
    };
    let pid = match raw.trim().parse::<i32>() {
        Ok(pid) => pid,
        Err(error) => {
            return indeterminate(format!(
                "parsing legacy pid record {}: {error}",
                path.display()
            ));
        }
    };
    if pid <= 0 || !process_alive(pid) {
        return indeterminate("legacy pid is not a live process");
    }
    let start_time_ticks = match process_start_time_ticks(pid) {
        Ok(value) => value,
        Err(error) => {
            return indeterminate(format!("cannot read legacy process start token: {error:#}"));
        }
    };
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return indeterminate(format!("cannot stat legacy pid file: {error}"));
        }
    };
    let modified = match metadata.modified() {
        Ok(modified) => modified,
        Err(error) => {
            return indeterminate(format!("legacy pid file has no usable mtime: {error}"));
        }
    };
    match legacy_pid_predates_record(start_time_ticks, modified) {
        Ok(true) => {}
        Ok(false) => {
            return indeterminate("legacy pid file predates the current process generation");
        }
        Err(error) => {
            return indeterminate(format!(
                "cannot order legacy pid file and process start: {error:#}"
            ));
        }
    }
    // Close the observation window: a changed process generation is never promoted.
    if process_start_time_ticks(pid).ok() != Some(start_time_ticks) {
        return indeterminate("legacy process generation changed while observed");
    }
    let created_at = match process_created_at(start_time_ticks).and_then(rfc3339_utc) {
        Ok(value) => value,
        Err(error) => {
            return indeterminate(format!(
                "cannot encode legacy process start time: {error:#}"
            ));
        }
    };
    let generation_id = crate::task_inventory::generation_id(
        "exec",
        id,
        pid as u32,
        &created_at,
        Some(start_time_ticks),
    );
    ExecGenerationObservation::Running {
        pid: pid as u32,
        created_at,
        generation_id,
    }
}

fn indeterminate(reason: impl Into<String>) -> ExecGenerationObservation {
    ExecGenerationObservation::Indeterminate {
        reason: reason.into(),
    }
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
fn legacy_pid_predates_record(start_time_ticks: u64, modified: SystemTime) -> anyhow::Result<bool> {
    let ticks_per_second = clock_ticks_per_second()?;
    let uptime: f64 = fs::read_to_string("/proc/uptime")?
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing /proc/uptime value"))?
        .parse()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let modified = modified.duration_since(UNIX_EPOCH)?.as_secs_f64();
    let tick = 1.0 / ticks_per_second as f64;
    // The start token denotes a clock-tick bucket. The record must follow the
    // end of that bucket, otherwise PID reuse cannot be ruled out.
    let latest_start = now - uptime + start_time_ticks as f64 * tick + tick;
    Ok(modified >= latest_start)
}

#[cfg(target_os = "macos")]
fn legacy_pid_predates_record(start_time_ticks: u64, modified: SystemTime) -> anyhow::Result<bool> {
    let modified_micros = modified.duration_since(UNIX_EPOCH)?.as_micros();
    Ok(u128::from(start_time_ticks) <= modified_micros)
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_second() -> anyhow::Result<u64> {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks <= 0 {
        anyhow::bail!("sysconf(_SC_CLK_TCK) failed");
    }
    Ok(ticks as u64)
}

#[cfg(target_os = "linux")]
fn process_created_at(start_time_ticks: u64) -> anyhow::Result<SystemTime> {
    let boot_seconds = fs::read_to_string("/proc/stat")?
        .lines()
        .find_map(|line| line.strip_prefix("btime "))
        .ok_or_else(|| anyhow::anyhow!("missing btime in /proc/stat"))?
        .parse::<u64>()?;
    let ticks_per_second = clock_ticks_per_second()?;
    let seconds = start_time_ticks / ticks_per_second;
    let remainder = start_time_ticks % ticks_per_second;
    let nanos = remainder.saturating_mul(1_000_000_000) / ticks_per_second;
    Ok(UNIX_EPOCH
        + Duration::from_secs(boot_seconds.saturating_add(seconds))
        + Duration::from_nanos(nanos))
}

#[cfg(target_os = "macos")]
fn process_created_at(start_time_ticks: u64) -> anyhow::Result<SystemTime> {
    Ok(UNIX_EPOCH + Duration::from_micros(start_time_ticks))
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
mod generation_observation_tests {
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;

    #[test]
    fn legacy_plain_pid_is_observed_without_rewriting_it() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let backend = ExecBackend::new(state, tmp.path().join("catalog"));
        let id = "host.test.legacy";
        let pid = std::process::id();
        // Leave an unambiguous interval between process creation and record publication.
        sleep(Duration::from_millis(20));
        let legacy = pid.to_string();
        fs::write(backend.pid_path(id), &legacy).unwrap();

        assert!(matches!(
            backend.observe_generation_optional(id).unwrap(),
            Some(ExecGenerationObservation::Running {
                pid: observed,
                ref created_at,
                ref generation_id,
            }) if observed == pid
                && crate::task_inventory::is_rfc3339_utc_millis(created_at)
                && generation_id.starts_with("sha256:")
        ));
        assert_eq!(
            fs::read_to_string(backend.pid_path(id)).unwrap(),
            legacy,
            "observation rewrote the legacy state record"
        );
    }

    #[test]
    fn absent_record_is_none_and_does_not_create_state() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("missing-state");
        let backend = ExecBackend::new(state.clone(), tmp.path().join("catalog"));
        assert_eq!(
            backend
                .observe_generation_optional("host.test.absent")
                .unwrap(),
            None
        );
        assert!(!state.exists());
    }

    #[test]
    fn malformed_and_dead_legacy_records_are_indeterminate() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let backend = ExecBackend::new(state, tmp.path().join("catalog"));
        fs::write(backend.pid_path("host.test.bad"), "not-a-pid").unwrap();
        fs::write(backend.pid_path("host.test.dead"), "2000000000").unwrap();

        assert!(matches!(
            backend
                .observe_generation_optional("host.test.bad")
                .unwrap(),
            Some(ExecGenerationObservation::Indeterminate { reason })
                if reason.contains("parsing legacy pid record")
        ));
        assert!(matches!(
            backend
                .observe_generation_optional("host.test.dead")
                .unwrap(),
            Some(ExecGenerationObservation::Indeterminate { reason })
                if reason.contains("not a live process")
        ));
    }

    #[test]
    fn record_older_than_current_process_generation_cannot_be_promoted() {
        let pid = std::process::id() as i32;
        let start = process_start_time_ticks(pid).unwrap();
        assert!(
            !legacy_pid_predates_record(start, UNIX_EPOCH + Duration::from_secs(1)).unwrap(),
            "an ancient record must not identify the current PID generation"
        );
    }
}
