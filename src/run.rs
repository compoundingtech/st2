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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsString;
use std::io::{Read as _, Seek as _, Write as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::cutover_admission::{RuntimeMutate, RuntimeMutationAdmission};
use crate::exec_backend::ExecBackend;
use crate::flapping::FlappingCap;
use crate::host_lock::HostOwnership;
use crate::message;
use crate::reconcile::{PtyPresentation, ReconcilePlan, Session, TaskLaunch, TaskTarget};
use crate::task_inventory::{
    DesiredRuntime, ObservationBatch, ObservedState, RuntimeGeneration, RuntimeObservation,
    RuntimeObserver, generation_id,
};
use agent_spec::spec::{TaskKind, TaskLifecycle};

// This is an outer containment bound for a wedged runtime, not a fleet-scalability mechanism.
const PTY_LIST_TIMEOUT: Duration = Duration::from_secs(2);
const PTY_DAEMON_SHUTDOWN_WAIT: Duration = Duration::from_secs(6);
const OBSERVED_PROCESS_PID_TAG: &str = "st2.observation.process.pid";

/// Run a non-interactive child with bounded output capture. Regular temporary files keep an escaped
/// descendant that inherited stdout/stderr from blocking cleanup after the direct child times out.
/// The child still gets a fresh process group so the common wrapper-and-descendants case is reaped.
fn output_with_timeout(command: &mut Command, timeout: Duration) -> anyhow::Result<Output> {
    output_with_input_timeout(command, timeout, None)
}

fn output_with_input_timeout(
    command: &mut Command,
    timeout: Duration,
    input: Option<&[u8]>,
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
    if let Some(input) = input {
        child
            .stdin
            .take()
            .context("metadata patch child has no piped stdin")?
            .write_all(input)?;
    }
    let pid = child.id() as i32;
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            let _ = child.kill();
            // Never turn the timeout into another unbounded wait. Reap asynchronously so a
            // long-running supervisor does not accumulate zombies, while a wedged runtime cannot
            // hold up this failed probe or a short-lived doctor process.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            anyhow::bail!("timed out after {:.1}s", timeout.as_secs_f64());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    stdout.rewind()?;
    stderr.rewind()?;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    stdout.read_to_end(&mut stdout_bytes)?;
    stderr.read_to_end(&mut stderr_bytes)?;
    Ok(Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
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

    /// Read-only, generation-bearing observation used by exact cutover adoption. Ordinary fake
    /// runners need not implement this; refusing an incomplete observation is safer than inferring
    /// an adoptable generation from the lossy [`Session`] view.
    fn observe_runtime(&self, _desired: &[DesiredRuntime]) -> ObservationBatch {
        ObservationBatch {
            complete: false,
            errors: vec!["runner does not provide generation-bearing runtime observation".into()],
            ..ObservationBatch::default()
        }
    }

    /// Complete read-only census of the effective PTY root. Provider cutover proof uses this
    /// instead of a requested-id projection so an undeclared competing provider cannot hide.
    fn observe_pty_root(&self) -> ObservationBatch {
        ObservationBatch {
            complete: false,
            errors: vec!["runner does not provide a complete PTY-root census".into()],
            ..ObservationBatch::default()
        }
    }
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
    /// PTY daemon PID. Together with `createdAt`, this is pty's stable generation tuple.
    #[serde(default)]
    pid: Option<u32>,
    /// PTY-owned generation creation time.
    #[serde(rename = "createdAt", default)]
    created_at: Option<String>,
    /// Immutable-at-proof launch metadata published by Axe onto this exact PTY.
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct PtyMetadataPatch<'a> {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    display_name: Option<&'a Option<String>>,
    tags: &'a BTreeMap<String, Option<String>>,
}

#[derive(Debug, Deserialize)]
struct PtyStatsEntry {
    name: String,
    process: PtyStatsProcess,
    daemon: PtyStatsDaemon,
    #[serde(rename = "createdAt")]
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct PtyStatsProcess {
    alive: bool,
    pid: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct PtyStatsDaemon {
    pid: u32,
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

    /// Build (but do not run) the `pty run` invocation for `target`. Split out so the exact argv +
    /// env can be unit-tested without spawning anything.
    ///
    /// `$VAR`s are expanded here for everything that does NOT pass through a shell — env values, tag
    /// values, `cwd`, and direct argv — because `pty` passes them through verbatim. Shell source is
    /// left unexpanded: `sh -c` expands it at spawn from the same env (which includes `$CATALOG`).
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
        let mut tags = target.tags.clone();
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
            cmd.arg("--tag").arg(format!("{k}={}", self.expand(v)));
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
                cmd.args(argv.iter().map(|arg| self.expand(arg)));
            }
        }
        cmd
    }

    /// Pure, typed PTY observation for task inventory. A missing root is known
    /// empty and is not passed to `pty`, because observation must not create it.
    fn task_observations(&self, desired_ids: &HashSet<&str>) -> ObservationBatch {
        let root = effective_pty_root(&self.catalog_root);
        self.task_observations_at_root(Some(desired_ids), &root)
    }

    fn all_task_observations(&self) -> ObservationBatch {
        let root = effective_pty_root(&self.catalog_root);
        let mut batch = self.task_observations_at_root(None, &root);
        if batch.complete
            && batch
                .observations
                .iter()
                .any(|observed| matches!(observed.state, ObservedState::Running(_)))
            && let Err(error) = self.enrich_running_process_pids(&root, &mut batch)
        {
            batch.complete = false;
            batch.errors.push(error.to_string());
        }
        batch
    }

    fn task_observations_at_root(
        &self,
        desired_ids: Option<&HashSet<&str>>,
        root: &Path,
    ) -> ObservationBatch {
        if desired_ids.is_some_and(HashSet::is_empty) {
            return ObservationBatch {
                complete: true,
                ..ObservationBatch::default()
            };
        }
        let metadata = match std::fs::metadata(root) {
            Ok(metadata) => metadata,
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
        let mut observations = Vec::with_capacity(entries.len());
        let mut errors = Vec::new();
        for entry in entries {
            if desired_ids.is_some_and(|ids| !ids.contains(entry.name.as_str())) {
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
                tags: entry.tags,
            });
        }
        ObservationBatch {
            complete: errors.is_empty(),
            observations,
            errors,
        }
    }

    fn list_entries(&self) -> anyhow::Result<Vec<PtyListEntry>> {
        self.list_entries_at(&effective_pty_root(&self.catalog_root))
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
            Some(&payload),
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

    fn list_entries_at(&self, root: &Path) -> anyhow::Result<Vec<PtyListEntry>> {
        let out = output_with_timeout(
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

    fn enrich_running_process_pids(
        &self,
        root: &Path,
        batch: &mut ObservationBatch,
    ) -> anyhow::Result<()> {
        let out = output_with_timeout(
            Command::new(&self.bin)
                .args(["stats", "--json"])
                .env("PTY_ROOT", root),
            PTY_LIST_TIMEOUT,
        )
        .map_err(|error| anyhow::anyhow!("`pty stats --json` failed: {error}"))?;
        anyhow::ensure!(
            out.status.success(),
            "`pty stats --json` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stats = serde_json::from_slice::<Vec<PtyStatsEntry>>(&out.stdout)
            .map_err(|error| anyhow::anyhow!("parsing `pty stats --json`: {error}"))?;
        let mut by_name = BTreeMap::new();
        for entry in &stats {
            anyhow::ensure!(
                by_name.insert(entry.name.as_str(), entry).is_none(),
                "`pty stats --json` contains duplicate session {:?}",
                entry.name
            );
        }
        for observed in &mut batch.observations {
            let ObservedState::Running(generation) = &observed.state else {
                continue;
            };
            let entry = by_name.get(observed.runtime_id.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "`pty stats --json` is missing running session {:?}",
                    observed.runtime_id
                )
            })?;
            anyhow::ensure!(
                entry.daemon.pid == generation.pid()
                    && entry.created_at == generation.created_at()
                    && entry.process.alive,
                "`pty stats --json` generation differs for running session {:?}",
                observed.runtime_id
            );
            let pid = entry.process.pid.ok_or_else(|| {
                anyhow::anyhow!(
                    "`pty stats --json` lacks child pid for running session {:?}",
                    observed.runtime_id
                )
            })?;
            anyhow::ensure!(
                pid > 0,
                "`pty stats --json` has invalid child pid for running session {:?}",
                observed.runtime_id
            );
            observed
                .tags
                .insert(OBSERVED_PROCESS_PID_TAG.to_owned(), pid.to_string());
        }
        Ok(())
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
        let envs: Vec<(OsString, OsString)> = inner
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_os_string(), v.to_os_string())))
            .collect();

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
            for (k, v) in &envs {
                cmd.env(k, v);
            }
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
                Ok(Some(crate::exec_backend::ExecGenerationObservation::Known {
                    generation,
                    alive,
                })) => {
                    let state = if alive {
                        match RuntimeGeneration::new(
                            generation.pid,
                            generation.created_at,
                            generation.generation_id,
                        ) {
                            Ok(generation) => ObservedState::Running(generation),
                            Err(error) => {
                                let message = format!(
                                    "invalid exec task {:?} generation: {error}",
                                    runtime.runtime_id
                                );
                                batch.errors.push(message.clone());
                                ObservedState::Indeterminate(message)
                            }
                        }
                    } else {
                        ObservedState::Exited
                    };
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state,
                        tags: BTreeMap::new(),
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
                        tags: BTreeMap::new(),
                    });
                }
                Err(error) => {
                    let message = format!("observe exec task {:?}: {error:#}", runtime.runtime_id);
                    batch.errors.push(message.clone());
                    batch.observations.push(RuntimeObservation {
                        runtime_id: runtime.runtime_id.clone(),
                        state: ObservedState::Indeterminate(message),
                        tags: BTreeMap::new(),
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

    fn observe_runtime(&self, desired: &[DesiredRuntime]) -> ObservationBatch {
        RuntimeObserver::observe(self, desired)
    }

    fn observe_pty_root(&self) -> ObservationBatch {
        self.pty.all_task_observations()
    }
}

/// The machine-local runner-state dir for a host's exec tasks: `$XDG_STATE_HOME/st2/<host>/exec`
/// (falling back to `~/.local/state`). Not synced — pids are host-local.
pub fn exec_state_dir(host: &str) -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    base.join("st2").join(host).join("exec")
}

/// A task st2 gave up restarting (crash-looped past its `restart{}` policy, mode=fail) — carries what
/// the supervisor loop needs to SURFACE it: the parked task, its agent, and who to notify (M2.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
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
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpReport {
    /// The pass could not obtain an authoritative session snapshot, so it deliberately performed no
    /// reconciliation. Long-running supervisors retry; a one-shot caller must exit unsuccessfully.
    pub skipped: bool,
    /// pty ids spawned this pass.
    pub launched: Vec<String>,
    /// pty ids torn down (retired agents) this pass.
    pub torn_down: Vec<String>,
    /// pty ids garbage-collected (dead, non-`keep`) this pass.
    pub gc: Vec<String>,
    /// pty ids whose GC/relaunch was DEFERRED this pass by the liveness debounce — a task that read
    /// not-alive but was alive within the grace window, i.e. a transient `pty list` flicker under load,
    /// left alone rather than destructively reaped (R21c). Not "noteworthy" (it's a no-op by design).
    pub deferred: Vec<String>,
    /// dead or absent adopt-only task ids held without reap or launch.
    pub held: Vec<String>,
    /// pty ids the flapping-cap refused to (re)launch this pass (parked / crash-looping).
    pub flapping: Vec<String>,
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
        self.torn_down.append(&mut other.torn_down);
        self.gc.append(&mut other.gc);
        self.deferred.append(&mut other.deferred);
        self.held.append(&mut other.held);
        self.flapping.append(&mut other.flapping);
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
            || !self.torn_down.is_empty()
            || !self.gc.is_empty()
            || !self.flapping.is_empty()
            || !self.errors.is_empty()
    }
}

/// Execute a plan against a runner, folding results into `report` and consulting/updating the
/// flapping-cap. Order matters: reap standalone corpses, then (cap-gated) reap-and-respawn each
/// launch target, then kill teardowns. Per-op errors are collected, never fatal.
pub fn execute(
    permission: &RuntimeMutate<'_>,
    catalog_root: &Path,
    this_host: &str,
    plan: &ReconcilePlan,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    report: &mut UpReport,
) {
    let Ok(canonical_root) = catalog_root.canonicalize() else {
        report.errors.push(format!(
            "canonicalize runtime mutation catalog {}",
            catalog_root.display()
        ));
        return;
    };
    if permission.catalog().as_path() != canonical_root || permission.host().as_str() != this_host {
        report.errors.push(format!(
            "runtime mutation permission is for ({}, {}), not ({}, {this_host})",
            permission.catalog().as_path().display(),
            permission.host().as_str(),
            canonical_root.display(),
        ));
        return;
    }
    // Presentation is an independent metadata path. A failure is visible and retried by the next
    // reconcile pass, but never authorizes stop, reap, restart, or replacement.
    for presentation in &plan.presentation {
        if let Err(error) = runner.patch_presentation(presentation) {
            report
                .errors
                .push(format!("metadata patch {}: {error}", presentation.pty_id));
        }
    }
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
        for target in &launch.tasks {
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
                    continue;
                }
                // Delaying / RateLimited: transient — skip quietly, retry a later pass, keep the corpse.
                crate::flapping::RestartDecision::Delaying
                | crate::flapping::RestartDecision::RateLimited => continue,
            }
            // Reap the corpse first (a dead session blocks respawn), preserving any backend-owned
            // bounded diagnostics, then respawn.
            if gc_set.contains(target.pty_id.as_str()) {
                match runner.reap_for_restart(&target.pty_id) {
                    Ok(()) => report.gc.push(target.pty_id.clone()),
                    Err(e) => {
                        report
                            .errors
                            .push(format!("reap {} for restart: {e}", target.pty_id));
                        continue;
                    }
                }
            }
            match runner.spawn(target, spec_dir) {
                Ok(()) => {
                    cap.record(&target.pty_id, now);
                    report.launched.push(target.pty_id.clone());
                }
                Err(e) => report.errors.push(format!("spawn {}: {e}", target.pty_id)),
            }
        }
    }

    for td in &plan.teardown {
        for id in &td.pty_ids {
            match runner.kill(id) {
                Ok(()) => report.torn_down.push(id.clone()),
                Err(e) => report.errors.push(format!("kill {id}: {e}")),
            }
        }
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

/// One full reconcile pass: discover → list actual → reconcile → execute. On a `pty list` failure the
/// pass is SKIPPED (the error is recorded but nothing is reconciled) — treating a transient list
/// failure as "no sessions" would double-spawn everything. `cap` carries flapping state across passes;
/// `debounce` carries per-id liveness so a transient not-alive flicker isn't destructively reaped.
fn reconcile_pass(
    ownership: &HostOwnership,
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
    let admission = match RuntimeMutationAdmission::ordinary(ownership) {
        Ok(admission) => admission,
        Err(error) => {
            return UpReport {
                skipped: true,
                errors: vec![format!(
                    "runtime mutation admission denied (pass skipped): {error}"
                )],
                ..Default::default()
            };
        }
    };
    reconcile_pass_admitted(
        &admission.permission(),
        root,
        this_host,
        runner,
        cap,
        debounce,
    )
}

pub(crate) fn reconcile_pass_admitted(
    permission: &RuntimeMutate<'_>,
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
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

    // Verify before touching any Codex workspace. A missing/stale/partial hook set must not rewrite
    // an already-live agent's settings to a nonexistent path. Codex specs remain in reconciliation
    // so live sessions can still be adopted; only their materialization and any new launch defer.
    let hook_error = crate::hooks::required_by_codex(&found.specs, this_host, root)
        .then(crate::hooks::verify_required_set)
        .transpose()
        .err()
        .map(|error| error.to_string());
    if let Some(error) = &hook_error {
        report.errors.push(format!(
            "verify this binary's lifecycle hooks before Codex materialization: {error}; materialization deferred"
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
    let materialized = crate::materialize::materialize_catalog_against_admitted(
        permission,
        root,
        &materializable_specs,
        &found.specs,
        this_host,
    );
    report.warnings.extend(materialized.warnings);
    report.errors.extend(materialized.errors);
    let eligible_specs: Vec<_> = found
        .specs
        .iter()
        .filter(|spec| !materialized.failed_agents.contains(&spec.bus_id(this_host)))
        .cloned()
        .collect();

    let sessions = match runner.list_sessions() {
        Ok(s) => s,
        Err(e) => {
            report.skipped = true;
            report
                .errors
                .push(format!("list sessions (pass skipped): {e}"));
            return report;
        }
    };
    let now = Instant::now();
    debounce.observe(&sessions, now);
    let mut plan = crate::reconcile(&eligible_specs, &sessions, this_host);
    report.deferred = debounce.defer_flickers(&mut plan, now);
    gate_codex_launches_on_hooks(&mut plan, root, &mut report, || match &hook_error {
        Some(error) => anyhow::bail!("{error}"),
        None => Ok(()),
    });
    execute(permission, root, this_host, &plan, runner, cap, &mut report);
    report
}

/// A missing Codex agent must not launch against stale lifecycle hooks. Suppress the affected agent
/// launches (including their sidecars) and surface the error when hook verification fails.
///
/// Workspace trust belongs to the declared provider command and its selected account-specific
/// runtime. Reconciliation deliberately does not mutate an ambient Codex config: an account selector
/// may choose `CODEX_HOME` only after this process launches the command, so such a write would target
/// the wrong state and could not satisfy the launched seat's trust gate.
fn gate_codex_launches_on_hooks<'a, V>(
    plan: &mut ReconcilePlan<'a>,
    catalog_root: &Path,
    report: &mut UpReport,
    verify_hooks: V,
) where
    V: FnOnce() -> anyhow::Result<()>,
{
    let mut gated_agents = Vec::new();
    for launch in &plan.launch {
        let Some(_) = launch.tasks.iter().find(|target| {
            target.name == "agent"
                && crate::hooks::launch_invokes_codex(&target.launch, catalog_root)
        }) else {
            continue;
        };
        gated_agents.push(launch.spec.identity.clone());
    }
    if gated_agents.is_empty() {
        return;
    }

    if let Err(error) = verify_hooks() {
        plan.launch
            .retain(|launch| !gated_agents.contains(&launch.spec.identity));
        report.errors.push(format!(
            "verify lifecycle hooks for new Codex agent(s) {}: {error}; launch suppressed",
            gated_agents.join(", ")
        ));
    }
}

/// One reconcile pass with a throwaway flapping-cap (`st2 up --once`). Returns an owned report;
/// never `Err` — all failures are collected in `report.errors`. The debounce is throwaway too: a
/// single pass has no prior liveness history, so it defers nothing (correct — one-shot has no flicker).
pub fn up_once(root: &Path, this_host: &str, runner: &dyn Runner) -> anyhow::Result<UpReport> {
    let ownership = HostOwnership::acquire(root, this_host)
        .context("acquire runtime host ownership for one-shot reconcile")?;
    up_once_with_ownership(&ownership, runner)
}

/// Reconcile once while retaining caller-owned host authority.
///
/// Cutover finalization uses this seam to continue into the successor without a
/// drop-and-reacquire window.
pub fn up_once_with_ownership(
    ownership: &HostOwnership,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    Ok(reconcile_pass(
        &ownership,
        ownership.catalog(),
        ownership.host(),
        runner,
        &mut FlappingCap::default(),
        &mut debounce,
    ))
}

pub(crate) trait RuntimeObservationSource {
    fn observe_complete_pty_root(&self) -> ObservationBatch;
}

impl<T: Runner + ?Sized> RuntimeObservationSource for T {
    fn observe_complete_pty_root(&self) -> ObservationBatch {
        Runner::observe_pty_root(self)
    }
}

pub(crate) struct ProviderFleetRuntimeObserver<'a, O: RuntimeObservationSource + ?Sized> {
    action: &'a crate::cutover_admission::ProviderFleetProofAction,
    observer: &'a O,
}

impl<'a, O: RuntimeObservationSource + ?Sized> ProviderFleetRuntimeObserver<'a, O> {
    pub(crate) fn new(
        action: &'a crate::cutover_admission::ProviderFleetProofAction,
        observer: &'a O,
    ) -> Self {
        Self { action, observer }
    }
}

impl<O: RuntimeObservationSource + ?Sized> crate::cutover_admission::ProviderFleetObserver
    for ProviderFleetRuntimeObserver<'_, O>
{
    fn observe_provider_rows(
        &self,
        catalog: &crate::cutover_admission::CanonicalCatalog,
        host: &crate::cutover_admission::HostId,
    ) -> crate::cutover_admission::AdmissionResult<
        Vec<crate::cutover_admission::ProviderTaskObservation>,
    > {
        use crate::cutover_admission::{
            AdmissionError, ProviderTaskObservation, ProviderTaskStatus,
        };

        let found = crate::discover(catalog.as_path());
        if !found.errors.is_empty() {
            return Err(AdmissionError::Invalid(
                "provider runtime observation requires exact catalog discovery".to_owned(),
            ));
        }
        let mut provider_runtime_ids = Vec::with_capacity(self.action.providers.len());
        let mut catalog_provider_ids = BTreeSet::new();
        for spec in found
            .specs
            .iter()
            .filter(|spec| spec.resolved_host(host.as_str()) == host.as_str() && !spec.retired)
        {
            for task in spec
                .tasks
                .iter()
                .filter(|task| task.name == "agent" && !task.derived && task.kind == TaskKind::Pty)
            {
                let runtime_id = task.id.clone().unwrap_or_else(|| {
                    format!("{}.{}.{}", host.as_str(), spec.identity, task.name)
                });
                if task.lifecycle != TaskLifecycle::AdoptOnly {
                    return Err(AdmissionError::Conflict(format!(
                        "local provider PTY {runtime_id:?} is not adopt-only"
                    )));
                }
                if !catalog_provider_ids.insert(runtime_id.clone()) {
                    return Err(AdmissionError::Conflict(format!(
                        "local provider PTY id {runtime_id:?} is declared more than once"
                    )));
                }
            }
        }
        for entry in &self.action.providers {
            let mut matches = found
                .specs
                .iter()
                .filter(|spec| {
                    spec.identity == entry.identity
                        && spec.resolved_host(host.as_str()) == host.as_str()
                })
                .flat_map(|spec| {
                    spec.tasks
                        .iter()
                        .filter(|task| task.name == "agent" && !task.derived)
                        .map(move |task| (spec, task))
                });
            let Some((spec, task)) = matches.next() else {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} has no exact authored runtime",
                    entry.identity
                )));
            };
            if matches.next().is_some() {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} has multiple authored runtimes",
                    entry.identity
                )));
            }
            provider_runtime_ids.push((
                entry,
                task.id.clone().unwrap_or_else(|| {
                    format!("{}.{}.{}", host.as_str(), spec.identity, task.name)
                }),
            ));
        }
        let expected_ids = provider_runtime_ids
            .iter()
            .map(|(_, runtime_id)| runtime_id.clone())
            .collect::<BTreeSet<_>>();
        if expected_ids.len() != provider_runtime_ids.len() || catalog_provider_ids != expected_ids
        {
            return Err(AdmissionError::Conflict(
                "provider action is not an exact projection of all local adopt-only provider PTYs"
                    .to_owned(),
            ));
        }

        let batch = self.observer.observe_complete_pty_root();
        if !batch.complete || !batch.errors.is_empty() {
            return Err(AdmissionError::Invalid(format!(
                "complete PTY-root observation is incomplete: {}",
                batch.errors.join("; ")
            )));
        }
        validate_complete_provider_pty_census(&expected_ids, &batch)?;
        let mut observed_by_id = BTreeMap::new();
        for observed in &batch.observations {
            observed_by_id.insert(observed.runtime_id.as_str(), observed);
        }

        let mut authored_providers = Vec::with_capacity(provider_runtime_ids.len());
        for (entry, runtime_id) in provider_runtime_ids {
            let observed = observed_by_id
                .get(runtime_id.as_str())
                .expect("declared provider presence checked");
            let receipt = observe_prompt_authority(entry, host, &runtime_id, &observed.tags)?;
            let (status, runtime_generation_id) = match &observed.state {
                ObservedState::Running(generation) => {
                    let process_pid = observed
                        .tags
                        .get(OBSERVED_PROCESS_PID_TAG)
                        .ok_or_else(|| {
                            AdmissionError::Invalid(format!(
                                "provider {runtime_id:?} lacks exact PTY child-process evidence"
                            ))
                        })?
                        .parse::<u32>()
                        .map_err(|error| {
                            AdmissionError::Invalid(format!(
                                "provider {runtime_id:?} child-process pid is invalid: {error}"
                            ))
                        })?;
                    observe_effective_prompt_injection(entry, &receipt, process_pid)?;
                    (
                        ProviderTaskStatus::Running,
                        Some(generation.generation_id().to_owned()),
                    )
                }
                ObservedState::Exited | ObservedState::Vanished => {
                    (ProviderTaskStatus::Stopped, None)
                }
                ObservedState::Indeterminate(reason) => {
                    return Err(AdmissionError::Invalid(format!(
                        "provider {runtime_id:?} is indeterminate: {reason}"
                    )));
                }
                ObservedState::Absent => (ProviderTaskStatus::Absent, None),
            };
            authored_providers.push(ProviderTaskObservation {
                identity: entry.identity.clone(),
                status,
                runtime_generation_id,
                prompt: Some(entry.prompt.clone()),
            });
        }

        let recheck = self.observer.observe_complete_pty_root();
        validate_complete_provider_pty_census(&expected_ids, &recheck)?;
        validate_provider_carriers_unchanged(&expected_ids, &batch, &recheck)?;

        Ok(authored_providers)
    }
}

pub(crate) fn validate_provider_action_preflight(
    declaration_root: &Path,
    logical_catalog: &Path,
    host: &crate::cutover_admission::HostId,
    action: &crate::cutover_admission::ProviderFleetProofAction,
    expected_catalog_sha256: &str,
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::AdmissionError;

    let observed_sha256 = crate::catalog_transaction::declaration_root_sha256_locked(
        declaration_root,
    )
    .map_err(|error| {
        AdmissionError::Invalid(format!(
            "compute prospective provider catalog digest: {error:#}"
        ))
    })?;
    if observed_sha256 != expected_catalog_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "prospective provider catalog digest mismatch: expected {expected_catalog_sha256}, found {observed_sha256}"
        )));
    }
    let found = crate::discover(declaration_root);
    if !found.errors.is_empty() || !found.warnings.is_empty() {
        return Err(AdmissionError::Invalid(
            "provider preflight requires warning-free exact prospective catalog discovery"
                .to_owned(),
        ));
    }
    let authored = found
        .specs
        .iter()
        .filter(|spec| spec.resolved_host(host.as_str()) == host.as_str() && !spec.retired)
        .flat_map(|spec| {
            spec.tasks
                .iter()
                .filter(|task| task.name == "agent" && !task.derived && task.kind == TaskKind::Pty)
                .map(move |task| (spec, task))
        })
        .collect::<Vec<_>>();
    if authored.len() != action.providers.len()
        || authored
            .iter()
            .any(|(_, task)| task.lifecycle != TaskLifecycle::AdoptOnly)
    {
        return Err(AdmissionError::Conflict(
            "prospective catalog provider inventory is not the exact adopt-only action fleet"
                .to_owned(),
        ));
    }
    for entry in &action.providers {
        let matches = authored
            .iter()
            .filter(|(spec, _)| spec.identity == entry.identity)
            .collect::<Vec<_>>();
        let [(spec, task)] = matches.as_slice() else {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} is missing or non-unique in prospective catalog",
                entry.identity
            )));
        };
        if entry.host != *host {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} host differs from preflight host",
                entry.identity
            )));
        }
        let relative_spec = spec.path.strip_prefix(declaration_root).map_err(|_| {
            AdmissionError::Invalid("prospective provider declaration escaped its root".to_owned())
        })?;
        let logical_spec_dir = logical_catalog
            .join(relative_spec)
            .parent()
            .unwrap_or(logical_catalog)
            .to_path_buf();
        let workspace = spec
            .workspace
            .as_deref()
            .map(|value| {
                logical_spec_dir.join(crate::expand::expand_catalog(value, logical_catalog))
            })
            .ok_or_else(|| {
                AdmissionError::Conflict(format!(
                    "provider {:?} has no authored workspace",
                    entry.identity
                ))
            })?;
        if workspace != entry.workspace {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} workspace differs from prospective catalog",
                entry.identity
            )));
        }
        let argv = match (&task.command, &task.argv) {
            (None, Some(argv)) => argv
                .iter()
                .map(|argument| crate::expand::expand_catalog(argument, logical_catalog))
                .collect::<Vec<_>>(),
            _ => {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} does not have one structured argv",
                    entry.identity
                )));
            }
        };
        if argv != entry.canonical_argv {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} argv differs from prospective catalog",
                entry.identity
            )));
        }
        for (flag, expected) in [
            ("--persona", entry.persona.as_str()),
            ("--harness", entry.harness.as_str()),
            ("--model", entry.model.as_str()),
            ("--effort", entry.effort.as_str()),
            ("--mode", entry.mode.as_str()),
            ("--boot", entry.boot_contract.as_str()),
        ] {
            if exact_provider_argv_axis(&argv, flag)? != expected {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} {flag} differs from prospective catalog",
                    entry.identity
                )));
            }
        }
        let persona = task
            .env
            .get("AGENT_PERSONA")
            .map(|value| crate::expand::expand_catalog(value, logical_catalog));
        if persona.as_deref() != Some(entry.persona.as_str()) {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} AGENT_PERSONA differs from prospective catalog",
                entry.identity
            )));
        }
        let profile = task
            .env
            .get("AGENT_RUNTIME_PROFILE")
            .map(|value| PathBuf::from(crate::expand::expand_catalog(value, logical_catalog)));
        if profile.as_deref() != Some(entry.prompt.runtime_profile_path.as_path()) {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} runtime profile differs from prospective catalog",
                entry.identity
            )));
        }
    }
    Ok(())
}

fn exact_provider_argv_axis<'a>(
    argv: &'a [String],
    flag: &str,
) -> crate::cutover_admission::AdmissionResult<&'a str> {
    use crate::cutover_admission::AdmissionError;

    let inline = format!("{flag}=");
    let mut values = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if argv[index] == flag {
            let value = argv.get(index + 1).ok_or_else(|| {
                AdmissionError::Invalid(format!("provider argv {flag} has no value"))
            })?;
            values.push(value.as_str());
            index += 2;
            continue;
        }
        if let Some(value) = argv[index].strip_prefix(&inline) {
            values.push(value);
        }
        index += 1;
    }
    match values.as_slice() {
        [value] if !value.is_empty() && !value.starts_with("--") => Ok(*value),
        [] => Err(AdmissionError::Invalid(format!(
            "provider argv omits {flag}"
        ))),
        _ => Err(AdmissionError::Invalid(format!(
            "provider argv has invalid or repeated {flag}"
        ))),
    }
}

fn validate_complete_provider_pty_census(
    expected_ids: &BTreeSet<String>,
    batch: &ObservationBatch,
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::AdmissionError;

    if !batch.complete || !batch.errors.is_empty() {
        return Err(AdmissionError::Invalid(
            "provider proof requires one complete PTY-root census".to_owned(),
        ));
    }
    let mut observed_ids = BTreeSet::new();
    for observed in &batch.observations {
        if !observed_ids.insert(observed.runtime_id.as_str()) {
            return Err(AdmissionError::Conflict(format!(
                "complete PTY-root census contains duplicate runtime id {:?}",
                observed.runtime_id
            )));
        }
        if !expected_ids.contains(&observed.runtime_id)
            && (observed.runtime_id.ends_with(".agent")
                || observed.tags.get("role").map(String::as_str) == Some("agent")
                || observed.tags.get("run.role").map(String::as_str) == Some("coding-agent")
                || observed.tags.contains_key("agent.provider")
                || observed.tags.contains_key("agent.harness")
                || observed.tags.contains_key("agent.tool")
                || observed.tags.keys().any(|name| {
                    matches!(
                        name.as_str(),
                        "agent.launch.receipt.schema"
                            | "agent.launch.receipt.path"
                            | "agent.launch.receipt.sha256"
                            | "agent.generation.id"
                    )
                }))
        {
            return Err(AdmissionError::Conflict(format!(
                "complete PTY-root census contains undeclared provider PTY {:?}",
                observed.runtime_id
            )));
        }
    }
    if expected_ids
        .iter()
        .any(|runtime_id| !observed_ids.contains(runtime_id.as_str()))
    {
        return Err(AdmissionError::Conflict(
            "complete PTY-root census is missing a declared provider PTY".to_owned(),
        ));
    }
    Ok(())
}

fn validate_provider_carriers_unchanged(
    expected_ids: &BTreeSet<String>,
    before: &ObservationBatch,
    after: &ObservationBatch,
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::AdmissionError;

    fn rows<'a>(
        expected_ids: &BTreeSet<String>,
        batch: &'a ObservationBatch,
    ) -> BTreeMap<&'a str, &'a RuntimeObservation> {
        batch
            .observations
            .iter()
            .filter(|row| expected_ids.contains(&row.runtime_id))
            .map(|row| (row.runtime_id.as_str(), row))
            .collect::<BTreeMap<_, _>>()
    }
    let before = rows(expected_ids, before);
    let after = rows(expected_ids, after);
    if before.len() != expected_ids.len()
        || after.len() != expected_ids.len()
        || before
            .iter()
            .any(|(runtime_id, row)| after.get(runtime_id).copied() != Some(*row))
    {
        return Err(AdmissionError::Conflict(
            "provider carrier changed after live prompt-injection inspection".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchReceiptPathDigest {
    path: PathBuf,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchReceiptInjection {
    kind: crate::cutover_admission::PromptInjectionKind,
    seam: String,
    prompt_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AxeLaunchReceipt {
    schema: String,
    phase: String,
    runtime_id: String,
    generation_id: String,
    identity: String,
    workspace: PathBuf,
    provider: String,
    account: String,
    persona: String,
    harness: String,
    model: String,
    effort: String,
    mode: String,
    boot_contract: String,
    runtime_profile: LaunchReceiptPathDigest,
    persona_prompt: LaunchReceiptPathDigest,
    injection: LaunchReceiptInjection,
    canonical_provider_argv: Vec<String>,
    provider_argv_sha256: String,
    trajectory_sha256: String,
}

struct ObservedPromptAuthority {
    receipt: AxeLaunchReceipt,
    account_executable: PathBuf,
    account_environment: BTreeMap<String, String>,
    all_account_environment_keys: BTreeSet<String>,
}

fn observe_prompt_authority(
    entry: &crate::cutover_admission::ProviderFleetEntry,
    host: &crate::cutover_admission::HostId,
    runtime_id: &str,
    tags: &BTreeMap<String, String>,
) -> crate::cutover_admission::AdmissionResult<ObservedPromptAuthority> {
    use crate::cutover_admission::{AdmissionError, PromptInjectionKind};

    let prompt = &entry.prompt;
    let required_tags = [
        ("agent.launch.receipt.schema", "axe.agent-launch-receipt.v1"),
        (
            "agent.launch.receipt.path",
            prompt.launch_receipt_path.to_str().ok_or_else(|| {
                AdmissionError::Invalid("launch receipt path is not UTF-8".to_owned())
            })?,
        ),
        (
            "agent.launch.receipt.sha256",
            prompt.launch_receipt_sha256.as_str(),
        ),
        ("agent.generation.id", entry.launch_generation_id.as_str()),
    ];
    for (name, expected) in required_tags {
        if tags.get(name).map(String::as_str) != Some(expected) {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} PTY tag {name:?} does not bind the exact launch receipt",
                entry.identity
            )));
        }
    }

    let profile_bytes = read_bounded_regular(&prompt.runtime_profile_path, "runtime profile")
        .map_err(|error| AdmissionError::Invalid(error.to_string()))?;
    let observed_profile = format!("{:x}", Sha256::digest(&profile_bytes));
    if observed_profile != prompt.runtime_profile_sha256 || observed_profile != entry.profile_sha256
    {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} runtime profile digest mismatch",
            entry.identity
        )));
    }
    let profile: serde_json::Value = serde_json::from_slice(&profile_bytes).map_err(|error| {
        AdmissionError::Invalid(format!("parse provider runtime profile: {error}"))
    })?;
    if profile
        .pointer(&format!(
            "/personas/prompts/{}",
            entry.persona.replace('~', "~0").replace('/', "~1")
        ))
        .and_then(serde_json::Value::as_str)
        != prompt.persona_prompt_path.to_str()
    {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} runtime profile does not map its persona to the exact prompt",
            entry.identity
        )));
    }
    let (account_executable, account_environment, all_account_environment_keys) =
        profile_account_binding(&profile, entry)?;
    let persona_prompt_bytes = read_bounded_regular(&prompt.persona_prompt_path, "persona prompt")
        .map_err(|error| AdmissionError::Invalid(error.to_string()))?;
    let observed_prompt = format!("{:x}", Sha256::digest(&persona_prompt_bytes));
    if observed_prompt != prompt.persona_prompt_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} persona prompt digest mismatch",
            entry.identity
        )));
    }

    let receipt_bytes = read_bounded_regular(&prompt.launch_receipt_path, "Axe launch receipt")
        .map_err(|error| AdmissionError::Invalid(error.to_string()))?;
    let observed_receipt = format!("{:x}", Sha256::digest(&receipt_bytes));
    if observed_receipt != prompt.launch_receipt_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} launch receipt digest mismatch",
            entry.identity
        )));
    }
    let receipt: AxeLaunchReceipt = serde_json::from_slice(&receipt_bytes)
        .map_err(|error| AdmissionError::Invalid(format!("parse Axe launch receipt: {error}")))?;
    let canonical = serde_json::to_vec(&receipt).map_err(|error| {
        AdmissionError::Invalid(format!("canonicalize Axe launch receipt: {error}"))
    })?;
    if canonical != receipt_bytes {
        return Err(AdmissionError::Invalid(
            "Axe launch receipt is not byte-for-byte canonical JSON".to_owned(),
        ));
    }
    let expected_identity = format!("{}.{}", host.as_str(), entry.identity);
    if receipt.schema != "axe.agent-launch-receipt.v1"
        || receipt.phase != "prepared"
        || receipt.runtime_id != runtime_id
        || receipt.generation_id != entry.launch_generation_id
        || receipt.identity != expected_identity
        || receipt.workspace != entry.workspace
        || receipt.provider != entry.provider
        || receipt.account != entry.account
        || receipt.persona != entry.persona
        || receipt.harness != entry.harness
        || receipt.model != entry.model
        || receipt.effort != entry.effort
        || receipt.mode != entry.mode
        || receipt.boot_contract != entry.boot_contract
        || receipt.runtime_profile.path != prompt.runtime_profile_path
        || receipt.runtime_profile.sha256 != prompt.runtime_profile_sha256
        || receipt.persona_prompt.path != prompt.persona_prompt_path
        || receipt.persona_prompt.sha256 != prompt.persona_prompt_sha256
        || receipt.injection.kind != prompt.injection_kind
        || receipt.injection.seam
            != match prompt.injection_kind {
                PromptInjectionKind::ClaudeAppendSystemPromptFile => {
                    "argv:--append-system-prompt-file"
                }
                PromptInjectionKind::CodexDeveloperInstructions => "argv:-c:developer_instructions",
                PromptInjectionKind::OpencodeSystemPromptFile => "env:AGENT_SYSTEM_PROMPT_FILE",
                PromptInjectionKind::PiAppendSystemPromptFile => "argv:--append-system-prompt",
            }
        || receipt.injection.prompt_sha256 != prompt.persona_prompt_sha256
        || receipt.trajectory_sha256 != entry.trajectory_sha256
        || receipt.canonical_provider_argv.is_empty()
    {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} Axe launch receipt does not bind the exact trajectory",
            entry.identity
        )));
    }
    let provider_argv_sha256 = provider_argv_sha256(
        &receipt.canonical_provider_argv,
        prompt.injection_kind,
        &prompt.persona_prompt_sha256,
        &persona_prompt_bytes,
    )?;
    if provider_argv_sha256 != receipt.provider_argv_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} canonical provider argv digest mismatch",
            entry.identity
        )));
    }
    Ok(ObservedPromptAuthority {
        receipt,
        account_executable,
        account_environment,
        all_account_environment_keys,
    })
}

fn profile_account_binding(
    profile: &serde_json::Value,
    entry: &crate::cutover_admission::ProviderFleetEntry,
) -> crate::cutover_admission::AdmissionResult<(PathBuf, BTreeMap<String, String>, BTreeSet<String>)>
{
    use crate::cutover_admission::AdmissionError;

    let accounts = profile
        .get("accounts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            AdmissionError::Invalid("runtime profile has no accounts array".to_owned())
        })?;
    let mut all_keys = BTreeSet::new();
    let mut selected = Vec::new();
    for account in accounts {
        let env = account
            .pointer("/execution/env")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| {
                AdmissionError::Invalid("runtime profile account has no execution.env".to_owned())
            })?;
        for (key, value) in env {
            if !value.is_string() {
                return Err(AdmissionError::Invalid(format!(
                    "runtime profile account env {key:?} is not a string"
                )));
            }
            all_keys.insert(key.clone());
        }
        if account.get("accountId").and_then(serde_json::Value::as_str)
            == Some(entry.account.as_str())
        {
            selected.push((account, env));
        }
    }
    let [(account, env)] = selected.as_slice() else {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} account is missing or non-unique in runtime profile",
            entry.identity
        )));
    };
    if account.get("harness").and_then(serde_json::Value::as_str) != Some(entry.harness.as_str()) {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} account harness differs from runtime profile",
            entry.identity
        )));
    }
    let executable = account
        .pointer("/execution/binPath")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| {
            AdmissionError::Invalid("runtime profile account has no execution.binPath".to_owned())
        })?;
    let environment = env
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value.as_str().expect("validated string").to_owned(),
            )
        })
        .collect();
    Ok((executable, environment, all_keys))
}

fn provider_argv_sha256(
    projected: &[String],
    injection_kind: crate::cutover_admission::PromptInjectionKind,
    prompt_sha256: &str,
    prompt_bytes: &[u8],
) -> crate::cutover_admission::AdmissionResult<String> {
    use crate::cutover_admission::{AdmissionError, PromptInjectionKind};
    let mut argv = projected.to_vec();
    if injection_kind == PromptInjectionKind::CodexDeveloperInstructions {
        let placeholder = format!("developer_instructions=<prompt-sha256:{prompt_sha256}>");
        let indexes = argv
            .iter()
            .enumerate()
            .filter_map(|(index, value)| (value == &placeholder).then_some(index))
            .collect::<Vec<_>>();
        let [index] = indexes.as_slice() else {
            return Err(AdmissionError::Conflict(
                "Codex provider argv must contain one exact redacted prompt placeholder".to_owned(),
            ));
        };
        let prompt = std::str::from_utf8(prompt_bytes).map_err(|error| {
            AdmissionError::Invalid(format!("Codex persona prompt is not UTF-8: {error}"))
        })?;
        let encoded = serde_json::to_string(prompt).map_err(|error| {
            AdmissionError::Invalid(format!("encode Codex developer instructions: {error}"))
        })?;
        argv[*index] = format!("developer_instructions={encoded}");
    }
    let mut hash = Sha256::new();
    hash.update(b"axe.agent-launch-provider-argv.v1\0");
    for argument in argv {
        hash.update((argument.len() as u64).to_be_bytes());
        hash.update(argument.as_bytes());
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn observe_effective_prompt_injection(
    entry: &crate::cutover_admission::ProviderFleetEntry,
    authority: &ObservedPromptAuthority,
    pid: u32,
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::AdmissionError;

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (entry, authority, pid);
        return Err(AdmissionError::Invalid(
            "live provider injection observation is unsupported on this platform".to_owned(),
        ));
    }

    #[cfg(target_os = "linux")]
    {
        let snapshot = read_stable_proc_snapshot(pid).map_err(|error| {
            AdmissionError::Invalid(format!(
                "read stable live provider {:?} process snapshot: {error:#}",
                entry.identity
            ))
        })?;
        let prompt_bytes =
            read_bounded_regular(&entry.prompt.persona_prompt_path, "persona prompt")
                .map_err(|error| AdmissionError::Invalid(error.to_string()))?;
        validate_effective_prompt_injection(
            entry.prompt.injection_kind,
            &entry.prompt.persona_prompt_path,
            &prompt_bytes,
            &snapshot.argv,
            &snapshot.env,
        )?;
        validate_live_provider_argv(
            &authority.receipt.canonical_provider_argv,
            &authority.receipt.provider_argv_sha256,
            entry.prompt.injection_kind,
            &entry.prompt.persona_prompt_sha256,
            &prompt_bytes,
            &snapshot.argv,
            &authority.account_executable,
            &snapshot.identity,
        )?;
        validate_live_account_environment(
            &authority.account_environment,
            &authority.all_account_environment_keys,
            &snapshot.env,
        )
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcIdentity {
    start_time_ticks: u64,
    executable: PathBuf,
    executable_device: u64,
    executable_inode: u64,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcSnapshot {
    identity: ProcIdentity,
    argv: Vec<String>,
    env: Vec<String>,
}

#[cfg(target_os = "linux")]
fn read_stable_proc_snapshot(pid: u32) -> anyhow::Result<ProcSnapshot> {
    let first = read_proc_snapshot_once(pid)?;
    let second = read_proc_snapshot_once(pid)?;
    validate_stable_proc_snapshots(&first, &second)?;
    Ok(second)
}

#[cfg(target_os = "linux")]
fn read_proc_snapshot_once(pid: u32) -> anyhow::Result<ProcSnapshot> {
    let before = read_proc_identity(pid)?;
    let argv = read_proc_nul_list(pid, "cmdline")?;
    let env = read_proc_nul_list(pid, "environ")?;
    let after = read_proc_identity(pid)?;
    anyhow::ensure!(
        before == after,
        "provider process identity changed while cmdline/environment were read"
    );
    Ok(ProcSnapshot {
        identity: after,
        argv,
        env,
    })
}

#[cfg(target_os = "linux")]
fn validate_stable_proc_snapshots(
    first: &ProcSnapshot,
    second: &ProcSnapshot,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        first == second,
        "provider cmdline/environment changed between complete process snapshots"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_proc_identity(pid: u32) -> anyhow::Result<ProcIdentity> {
    let root = PathBuf::from(format!("/proc/{pid}"));
    let stat = std::fs::read_to_string(root.join("stat"))
        .with_context(|| format!("read {}/stat", root.display()))?;
    let suffix = stat
        .rsplit_once(") ")
        .map(|(_, suffix)| suffix)
        .ok_or_else(|| anyhow::anyhow!("{}/stat has no process-name terminator", root.display()))?;
    let start_time_ticks = suffix
        .split_ascii_whitespace()
        .nth(19)
        .ok_or_else(|| anyhow::anyhow!("{}/stat lacks starttime", root.display()))?
        .parse::<u64>()
        .with_context(|| format!("parse {}/stat starttime", root.display()))?;
    let executable = std::fs::read_link(root.join("exe"))
        .with_context(|| format!("read {}/exe", root.display()))?;
    let executable_metadata = std::fs::metadata(root.join("exe"))
        .with_context(|| format!("inspect {}/exe", root.display()))?;
    Ok(ProcIdentity {
        start_time_ticks,
        executable,
        executable_device: executable_metadata.dev(),
        executable_inode: executable_metadata.ino(),
    })
}

#[cfg(target_os = "linux")]
fn read_proc_nul_list(pid: u32, name: &str) -> anyhow::Result<Vec<String>> {
    const MAX_PROC_BYTES: u64 = 4 * 1024 * 1024;
    let path = PathBuf::from(format!("/proc/{pid}/{name}"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(MAX_PROC_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_PROC_BYTES,
        "{} exceeds {MAX_PROC_BYTES} bytes",
        path.display()
    );
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    bytes
        .split(|byte| *byte == 0)
        .map(|value| {
            String::from_utf8(value.to_vec())
                .with_context(|| format!("{} contains non-UTF-8 data", path.display()))
        })
        .collect()
}

fn validate_effective_prompt_injection(
    kind: crate::cutover_admission::PromptInjectionKind,
    prompt_path: &Path,
    prompt_bytes: &[u8],
    argv: &[String],
    env: &[String],
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::{AdmissionError, PromptInjectionKind};

    let prompt_path = prompt_path
        .to_str()
        .ok_or_else(|| AdmissionError::Invalid("persona prompt path is not UTF-8".to_owned()))?;
    let prompt = std::str::from_utf8(prompt_bytes).map_err(|error| {
        AdmissionError::Invalid(format!("persona prompt is not UTF-8: {error}"))
    })?;
    let encoded_prompt = serde_json::to_string(prompt).map_err(|error| {
        AdmissionError::Invalid(format!("encode Codex developer instructions: {error}"))
    })?;
    let codex_value = format!("developer_instructions={encoded_prompt}");

    let flag_values = |flag: &str| {
        argv.iter()
            .enumerate()
            .filter(|(_, value)| value.as_str() == flag)
            .map(|(index, _)| argv.get(index + 1).map(String::as_str))
            .collect::<Vec<_>>()
    };
    let claude = flag_values("--append-system-prompt-file");
    let pi = flag_values("--append-system-prompt");
    let codex = argv
        .iter()
        .enumerate()
        .filter(|(_, value)| value.as_str() == "-c")
        .filter_map(|(index, _)| argv.get(index + 1).map(String::as_str))
        .filter(|value| value.starts_with("developer_instructions="))
        .collect::<Vec<_>>();
    let opencode = env
        .iter()
        .filter_map(|value| value.strip_prefix("AGENT_SYSTEM_PROMPT_FILE="))
        .collect::<Vec<_>>();

    let exact = match kind {
        PromptInjectionKind::ClaudeAppendSystemPromptFile => {
            claude == [Some(prompt_path)]
                && pi.is_empty()
                && codex.is_empty()
                && opencode.is_empty()
        }
        PromptInjectionKind::CodexDeveloperInstructions => {
            codex == [codex_value.as_str()]
                && claude.is_empty()
                && pi.is_empty()
                && opencode.is_empty()
        }
        PromptInjectionKind::OpencodeSystemPromptFile => {
            opencode == [prompt_path] && claude.is_empty() && pi.is_empty() && codex.is_empty()
        }
        PromptInjectionKind::PiAppendSystemPromptFile => {
            pi == [Some(prompt_path)]
                && claude.is_empty()
                && codex.is_empty()
                && opencode.is_empty()
        }
    };
    if !exact {
        return Err(AdmissionError::Conflict(
            "live provider process does not contain one exact harness-specific prompt injection"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_live_provider_argv(
    projected: &[String],
    expected_sha256: &str,
    injection_kind: crate::cutover_admission::PromptInjectionKind,
    prompt_sha256: &str,
    prompt_bytes: &[u8],
    actual: &[String],
    account_executable: &Path,
    process_identity: &ProcIdentity,
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::{AdmissionError, PromptInjectionKind};

    let projected_executable = projected.first().ok_or_else(|| {
        AdmissionError::Invalid("canonical provider argv has no executable".to_owned())
    })?;
    if Path::new(projected_executable) != account_executable {
        return Err(AdmissionError::Conflict(
            "canonical provider executable differs from selected account binding".to_owned(),
        ));
    }
    let executable = account_executable.canonicalize().map_err(|error| {
        AdmissionError::Invalid(format!(
            "canonicalize selected account executable {}: {error}",
            account_executable.display()
        ))
    })?;
    let metadata = std::fs::metadata(&executable).map_err(|error| {
        AdmissionError::io(
            format!(
                "inspect selected account executable {}",
                executable.display()
            ),
            error,
        )
    })?;
    if executable != process_identity.executable
        || metadata.dev() != process_identity.executable_device
        || metadata.ino() != process_identity.executable_inode
    {
        return Err(AdmissionError::Conflict(
            "live provider executable identity differs from selected account executable".to_owned(),
        ));
    }

    let mut normalized = actual.to_vec();
    if injection_kind == PromptInjectionKind::CodexDeveloperInstructions {
        let prompt = std::str::from_utf8(prompt_bytes).map_err(|error| {
            AdmissionError::Invalid(format!("Codex persona prompt is not UTF-8: {error}"))
        })?;
        let encoded = serde_json::to_string(prompt).map_err(|error| {
            AdmissionError::Invalid(format!("encode Codex developer instructions: {error}"))
        })?;
        let exact = format!("developer_instructions={encoded}");
        let indexes = normalized
            .iter()
            .enumerate()
            .filter_map(|(index, value)| (value == &exact).then_some(index))
            .collect::<Vec<_>>();
        let [index] = indexes.as_slice() else {
            return Err(AdmissionError::Conflict(
                "live Codex argv does not contain one exact developer-instructions value"
                    .to_owned(),
            ));
        };
        normalized[*index] = format!("developer_instructions=<prompt-sha256:{prompt_sha256}>");
    }
    if normalized != projected {
        return Err(AdmissionError::Conflict(
            "live provider argv differs from the canonical Axe projection".to_owned(),
        ));
    }
    let mut hash = Sha256::new();
    hash.update(b"axe.agent-launch-provider-argv.v1\0");
    for argument in actual {
        hash.update((argument.len() as u64).to_be_bytes());
        hash.update(argument.as_bytes());
    }
    if format!("{:x}", hash.finalize()) != expected_sha256 {
        return Err(AdmissionError::Conflict(
            "live provider argv differs from the receipt provider argv digest".to_owned(),
        ));
    }
    Ok(())
}

fn validate_live_account_environment(
    selected: &BTreeMap<String, String>,
    all_account_keys: &BTreeSet<String>,
    live: &[String],
) -> crate::cutover_admission::AdmissionResult<()> {
    use crate::cutover_admission::AdmissionError;

    let mut environment = BTreeMap::new();
    for assignment in live {
        let Some((key, value)) = assignment.split_once('=') else {
            return Err(AdmissionError::Invalid(
                "live provider environment contains an assignment without `=`".to_owned(),
            ));
        };
        if environment.insert(key, value).is_some() {
            return Err(AdmissionError::Conflict(format!(
                "live provider environment repeats {key:?}"
            )));
        }
    }
    for key in all_account_keys {
        match selected.get(key) {
            Some(expected) if environment.get(key.as_str()).copied() == Some(expected.as_str()) => {
            }
            None if !environment.contains_key(key.as_str()) => {}
            _ => {
                return Err(AdmissionError::Conflict(format!(
                    "live provider account environment key {key:?} differs from selected runtime-profile account"
                )));
            }
        }
    }
    Ok(())
}

fn read_bounded_regular(path: &Path, label: &str) -> anyhow::Result<Vec<u8>> {
    const MAX_CANDIDATE_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| format!("open {label} {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= MAX_CANDIDATE_ARTIFACT_BYTES,
        "{label} {} must be a regular file of at most {MAX_CANDIDATE_ARTIFACT_BYTES} bytes",
        path.display()
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {label} {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() as u64 == metadata.len(),
        "{label} {} changed size while read",
        path.display()
    );
    Ok(bytes)
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
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
    let ownership = match HostOwnership::acquire(root, this_host) {
        Ok(ownership) => ownership,
        Err(error) => {
            return UpReport {
                skipped: true,
                errors: vec![format!(
                    "acquire runtime host ownership (pass skipped): {error}"
                )],
                ..Default::default()
            };
        }
    };
    let admission = match RuntimeMutationAdmission::ordinary(&ownership) {
        Ok(admission) => admission,
        Err(error) => {
            return UpReport {
                skipped: true,
                errors: vec![format!(
                    "runtime mutation admission denied (pass skipped): {error}"
                )],
                ..Default::default()
            };
        }
    };
    reconcile_pass_specs_admitted(
        &admission.permission(),
        specs,
        root,
        this_host,
        runner,
        cap,
        debounce,
    )
}

fn reconcile_pass_specs_admitted(
    permission: &RuntimeMutate<'_>,
    specs: &[agent_spec::spec::AgentSpec],
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
    let mut report = UpReport::default();
    let sessions = match runner.list_sessions() {
        Ok(s) => s,
        Err(e) => {
            report.skipped = true;
            report
                .errors
                .push(format!("list sessions (pass skipped): {e}"));
            return report;
        }
    };
    reconcile_pass_specs_with_sessions(
        permission, specs, root, &sessions, this_host, runner, cap, debounce,
    )
}

/// Reconcile an in-memory team against an already captured session snapshot. Eval supervision uses
/// this so crash classification and reconciliation see the same terminal state: otherwise a clean
/// process can exit between two `pty list` calls, be reaped by the second call, then look like a
/// vanished crash on the next tick.
pub(crate) fn reconcile_pass_specs_with_sessions(
    permission: &RuntimeMutate<'_>,
    specs: &[agent_spec::spec::AgentSpec],
    root: &Path,
    sessions: &[Session],
    this_host: &str,
    runner: &dyn Runner,
    cap: &mut FlappingCap,
    debounce: &mut LivenessDebounce,
) -> UpReport {
    let mut report = UpReport::default();
    let now = Instant::now();
    debounce.observe(sessions, now);
    let mut plan = crate::reconcile(specs, sessions, this_host);
    report.deferred = debounce.defer_flickers(&mut plan, now);
    execute(permission, root, this_host, &plan, runner, cap, &mut report);
    report
}

/// One reconcile pass over an in-memory spec team (`st2 up <spec> --once`). Throwaway cap+debounce
/// (a single pass has no flicker history); never `Err` — failures collect in `report.errors`.
pub fn up_once_specs(
    specs: &[agent_spec::spec::AgentSpec],
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
) -> UpReport {
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    reconcile_pass_specs(
        specs,
        root,
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
    crate::reconcile::resolve_task(specs, selector, this_host)?;
    let ownership = HostOwnership::acquire(catalog_root, this_host)
        .context("acquire runtime host ownership for selected reconcile")?;
    let admission = RuntimeMutationAdmission::ordinary(&ownership)?;
    up_once_selected_specs_with_gates(
        &admission.permission(),
        catalog_root,
        specs,
        selector,
        this_host,
        runner,
        || crate::hooks::verify_installed().map(|_| ()),
    )
}

/// Discover a folder catalog once, resolve one task before any owner hook/render mutation, then
/// materialize only that owner and execute the selected plan.
pub fn up_once_selected(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let ownership = HostOwnership::acquire(catalog_root, this_host)
        .context("acquire runtime host ownership for selected reconcile")?;
    let admission = RuntimeMutationAdmission::ordinary(&ownership)?;
    let found = crate::discovery::discover(catalog_root);
    let (owner, _, _) = crate::reconcile::resolve_task(&found.specs, selector, this_host)?;
    let mut report = UpReport::default();
    report.warnings.extend(found.warnings);
    report.errors.extend(
        found
            .errors
            .into_iter()
            .map(|e| format!("{}: {}", e.path.display(), e.message)),
    );
    let owner = owner.clone();
    if crate::hooks::required_by_codex_agent(&owner, this_host, catalog_root)
        && let Err(error) = crate::hooks::verify_installed()
    {
        report
            .errors
            .push(format!("verify lifecycle hooks: {error}"));
        return Ok(report);
    }
    let materialized = crate::materialize::materialize_catalog_against_admitted(
        &admission.permission(),
        catalog_root,
        std::slice::from_ref(&owner),
        &found.specs,
        this_host,
    );
    report.warnings.extend(materialized.warnings);
    let owner_materialization_failed = !materialized.failed_agents.is_empty();
    report.errors.extend(materialized.errors);
    if owner_materialization_failed {
        return Ok(report);
    }
    let execution = up_once_selected_specs_with_gates(
        &admission.permission(),
        catalog_root,
        &found.specs,
        selector,
        this_host,
        runner,
        || Ok(()),
    )?;
    report.absorb(execution);
    Ok(report)
}

fn up_once_selected_specs_with_gates<V>(
    permission: &RuntimeMutate<'_>,
    catalog_root: &Path,
    specs: &[crate::spec::AgentSpec],
    selector: &str,
    this_host: &str,
    runner: &dyn Runner,
    verify_hooks: V,
) -> anyhow::Result<UpReport>
where
    V: FnOnce() -> anyhow::Result<()>,
{
    crate::reconcile::resolve_task(specs, selector, this_host)?;
    let sessions = runner
        .list_sessions()
        .map_err(|e| anyhow::anyhow!("list sessions: {e}"))?;
    let mut plan = crate::reconcile::reconcile_selected(specs, &sessions, this_host, selector)?;
    let mut report = UpReport::default();
    gate_codex_launches_on_hooks(&mut plan, catalog_root, &mut report, verify_hooks);
    execute(
        permission,
        catalog_root,
        this_host,
        &plan,
        runner,
        &mut FlappingCap::default(),
        &mut report,
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
    let ownership = HostOwnership::acquire(root, this_host)
        .context("acquire runtime host ownership for spec supervisor")?;
    install_signal_handler();
    let mut cap = FlappingCap::default();
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);
    let mut reported_flapping: HashSet<String> = HashSet::new();
    loop {
        let report = match RuntimeMutationAdmission::ordinary(&ownership) {
            Ok(admission) => reconcile_pass_specs_admitted(
                &admission.permission(),
                specs,
                root,
                this_host,
                runner,
                &mut cap,
                &mut debounce,
            ),
            Err(error) => UpReport {
                skipped: true,
                errors: vec![format!(
                    "runtime mutation admission denied (pass skipped): {error}"
                )],
                ..Default::default()
            },
        };
        for cl in &report.crash_loops {
            if reported_flapping.insert(cl.pty_id.clone()) {
                eprintln!(
                    "st2: GAVE UP on '{}' — crash-looping past its restart{{}} policy (mode=fail); leaving it parked and its last session for inspection. Fix the cause, then restart st2.",
                    cl.pty_id
                );
                surface_crash_loop(root, this_host, cl);
            }
        }
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
    Ok(())
}

/// Explicit teardown (`st2 down`) — kill EVERY live task of this host's catalog agents. This is the
/// one operation that ends tasks (the Nomad model: stopping the supervisor never does). Idempotent:
/// tasks already gone are simply not in the live set. Per-kill errors are collected, never fatal.
pub fn down(root: &Path, this_host: &str, runner: &dyn Runner) -> anyhow::Result<UpReport> {
    let ownership = HostOwnership::acquire(root, this_host)
        .context("acquire runtime host ownership for teardown")?;
    let admission = RuntimeMutationAdmission::ordinary(&ownership)?;
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
    teardown_specs(
        &admission.permission(),
        root,
        &found.specs,
        this_host,
        runner,
        &mut report,
    )?;
    Ok(report)
}

/// `st2 down` for a single-file team spec: tear down the declared team's live sessions on this host.
/// The symmetric verb to `up`/`ls` over a spec — the "stop the fleet cleanly" step of the swap runbook.
/// Sessions persist across an `st2 up` supervisor exit (nomad-decoupled), so this is how you actually
/// stop them. `specs` are the already-resolved [`AgentSpec`]s (from `spec_to_agent_specs`).
pub fn down_specs(
    specs: &[agent_spec::spec::AgentSpec],
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
) -> anyhow::Result<UpReport> {
    let ownership = HostOwnership::acquire(root, this_host)
        .context("acquire runtime host ownership for spec teardown")?;
    let admission = RuntimeMutationAdmission::ordinary(&ownership)?;
    let mut report = UpReport::default();
    teardown_specs(
        &admission.permission(),
        root,
        specs,
        this_host,
        runner,
        &mut report,
    )?;
    Ok(report)
}

/// Shared teardown core: kill every live task session declared on this host. Task session ids are
/// derived identically to how reconcile spawns them (explicit `task.id`, else `<bus_id>.<task>`), so
/// the catalog `down` and the spec `down_specs` tear down exactly what `up`/`up_*_specs` launched.
fn teardown_specs(
    permission: &RuntimeMutate<'_>,
    root: &Path,
    specs: &[agent_spec::spec::AgentSpec],
    this_host: &str,
    runner: &dyn Runner,
    report: &mut UpReport,
) -> anyhow::Result<()> {
    let canonical_root = root
        .canonicalize()
        .with_context(|| format!("canonicalize runtime mutation catalog {}", root.display()))?;
    if permission.catalog().as_path() != canonical_root || permission.host().as_str() != this_host {
        anyhow::bail!(
            "runtime mutation permission is for ({}, {}), not ({}, {this_host})",
            permission.catalog().as_path().display(),
            permission.host().as_str(),
            canonical_root.display(),
        );
    }
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

/// The supervisor loop: reconcile on a timer AND on folder changes until interrupted. The fs-watch is
/// best-effort; the `interval` timer is the always-on fallback. `on_report` is called once per pass
/// (e.g. to log a summary). Returns when a stop signal arrives — agents are left running.
pub fn up_loop(
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    interval: Duration,
    on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    let ownership = HostOwnership::acquire(root, this_host)
        .context("acquire runtime host ownership for supervisor")?;
    up_loop_with_ownership(ownership, runner, interval, on_report)
}

/// Enter the resident supervisor while consuming already-retained host authority.
///
/// This is the no-gap successor handoff from a finalized cutover.
pub fn up_loop_with_ownership(
    ownership: HostOwnership,
    runner: &dyn Runner,
    interval: Duration,
    on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    up_loop_with_ownership_ready(ownership, runner, interval, || Ok(()), on_report)
}

/// Enter the resident supervisor and invoke `on_ready` exactly once while the caller's host
/// authority is still retained and immediately before the first reconcile pass can begin.
pub fn up_loop_with_ownership_ready(
    ownership: HostOwnership,
    runner: &dyn Runner,
    interval: Duration,
    on_ready: impl FnOnce() -> anyhow::Result<()>,
    on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    let root = ownership.catalog().to_path_buf();
    let this_host = ownership.host().to_owned();
    install_signal_handler();
    on_ready().context("publish retained-ownership supervisor readiness")?;
    up_loop_until(
        &ownership, &root, &this_host, runner, interval, &STOP, on_report,
    )
}

fn up_loop_until(
    ownership: &HostOwnership,
    root: &Path,
    this_host: &str,
    runner: &dyn Runner,
    interval: Duration,
    stop: &AtomicBool,
    mut on_report: impl FnMut(&UpReport),
) -> anyhow::Result<()> {
    let (tx, rx) = channel::<()>();
    let _watcher = crate::watch::watch_catalog_declarations(root, tx);
    let mut cap = FlappingCap::default();
    // Carries per-id liveness across passes so a transient `pty list` flicker under load isn't
    // destructively GC'd (R21c). Fresh throwaway in `up_once` — a single pass has no flicker to absorb.
    let mut debounce = LivenessDebounce::new(DEBOUNCE_GRACE);

    // Surface each parked crash-loop once (not every pass): an stderr line AND a message to the
    // agent's supervisor over the native bus, so a crash-loop isn't only visible to whoever is
    // watching the log.
    let mut reported_flapping: HashSet<String> = HashSet::new();

    loop {
        let report = reconcile_pass(ownership, root, this_host, runner, &mut cap, &mut debounce);
        for cl in &report.crash_loops {
            if reported_flapping.insert(cl.pty_id.clone()) {
                eprintln!(
                    "st2: GAVE UP on '{}' — crash-looping past its restart{{}} policy (mode=fail); leaving it parked and its last session for inspection. Fix the cause, then restart st2.",
                    cl.pty_id
                );
                surface_crash_loop(root, this_host, cl);
            }
        }
        on_report(&report);

        if stop.load(Ordering::SeqCst) {
            break;
        }

        // Sleep the interval in 250ms slices, waking early on a folder change or a stop signal.
        let slices = (interval.as_millis() / 250).max(1);
        for _ in 0..slices {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            match rx.recv_timeout(Duration::from_millis(250)) {
                Ok(()) => {
                    drain(&rx); // coalesce a burst of events into one pass
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        if stop.load(Ordering::SeqCst) {
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
        eprintln!(
            "st2: crash-loop '{}' ({agent}) has no supervisor to notify.",
            cl.pty_id
        );
        return;
    };
    let Some(agent_dir) = message::resolve_agent_dir(catalog_root, supervisor, this_host) else {
        eprintln!(
            "st2: crash-loop '{}': supervisor '{supervisor}' not found in the catalog to notify.",
            cl.pty_id
        );
        return;
    };
    let subject = format!("crash-loop: {agent} parked");
    let body = format!(
        "st2 gave up restarting task '{}' (agent {agent}) — it crash-looped past its restart{{}} \
         policy (mode=fail) and is parked. Its last dead session is left as evidence. Investigate the \
         cause, then restart st2 to unpark it.",
        cl.pty_id
    );
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
        eprintln!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use agent_spec::spec::{AgentSpec, JobType, Task, TaskKind, TaskLifecycle};
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::ffi::OsStr;

    #[test]
    fn axe_launch_receipt_links_all_four_harnesses_to_the_exact_pty_generation() {
        use crate::cutover_admission::{
            HostId, LaunchPromptAuthority, PromptInjectionKind, ProviderFleetEntry,
        };

        for (provider, harness, kind, seam) in [
            (
                "claude-code",
                "claude",
                PromptInjectionKind::ClaudeAppendSystemPromptFile,
                "argv:--append-system-prompt-file",
            ),
            (
                "codex",
                "codex",
                PromptInjectionKind::CodexDeveloperInstructions,
                "argv:-c:developer_instructions",
            ),
            (
                "opencode",
                "opencode",
                PromptInjectionKind::OpencodeSystemPromptFile,
                "env:AGENT_SYSTEM_PROMPT_FILE",
            ),
            (
                "pi-mono",
                "pi",
                PromptInjectionKind::PiAppendSystemPromptFile,
                "argv:--append-system-prompt",
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let workspace = temp.path().join("workspace");
            std::fs::create_dir(&workspace).unwrap();
            let prompt_path = temp.path().join("worker-prompt.md");
            let prompt_bytes = b"exact worker prompt\n";
            std::fs::write(&prompt_path, prompt_bytes).unwrap();
            let prompt_sha256 = format!("{:x}", Sha256::digest(prompt_bytes));
            let profile_path = temp.path().join("profile.json");
            let profile_bytes = serde_json::to_vec(&serde_json::json!({
                "personas": {"prompts": {"worker": prompt_path}},
                "accounts": [{
                    "accountId": "account-a",
                    "harness": harness,
                    "execution": {
                        "binPath": format!("/nix/store/{harness}/bin/{harness}"),
                        "env": {},
                    },
                }],
            }))
            .unwrap();
            std::fs::write(&profile_path, &profile_bytes).unwrap();
            let profile_sha256 = format!("{:x}", Sha256::digest(&profile_bytes));
            let receipt_path = temp.path().join("receipt.json");
            let adapter_argv = vec![
                "/nix/store/axe/bin/axe".to_owned(),
                "agent".to_owned(),
                "launch".to_owned(),
                "--harness".to_owned(),
                harness.to_owned(),
                "--persona".to_owned(),
                "worker".to_owned(),
                "--model".to_owned(),
                "model-a".to_owned(),
                "--effort".to_owned(),
                "high".to_owned(),
                "--mode".to_owned(),
                "managed-unattended".to_owned(),
                "--boot".to_owned(),
                "managed-v1".to_owned(),
            ];
            let mut entry = ProviderFleetEntry {
                identity: "worker-a".to_owned(),
                host: HostId::parse("testhost").unwrap(),
                provider: provider.to_owned(),
                account: "account-a".to_owned(),
                persona: "worker".to_owned(),
                workspace: workspace.clone(),
                prompt: LaunchPromptAuthority {
                    runtime_profile_path: profile_path.clone(),
                    runtime_profile_sha256: profile_sha256.clone(),
                    persona_prompt_path: prompt_path.clone(),
                    persona_prompt_sha256: prompt_sha256.clone(),
                    launch_receipt_path: receipt_path.clone(),
                    launch_receipt_sha256: String::new(),
                    injection_kind: kind,
                },
                argv_sha256: crate::cutover_admission::candidate_argv_sha256(&adapter_argv),
                canonical_argv: adapter_argv,
                profile_sha256: profile_sha256.clone(),
                harness: harness.to_owned(),
                model: "model-a".to_owned(),
                effort: "high".to_owned(),
                mode: "managed-unattended".to_owned(),
                boot_contract: "managed-v1".to_owned(),
                launch_generation_id: "axe-generation-a".to_owned(),
                runtime_generation_id: "pty-generation-a".to_owned(),
                trajectory_sha256: String::new(),
            };
            entry.trajectory_sha256 =
                crate::cutover_admission::provider_trajectory_sha256(&entry).unwrap();
            let projected_argv = if kind == PromptInjectionKind::CodexDeveloperInstructions {
                vec![
                    "codex".to_owned(),
                    "-c".to_owned(),
                    format!("developer_instructions=<prompt-sha256:{prompt_sha256}>"),
                ]
            } else {
                vec![
                    harness.to_owned(),
                    "--model".to_owned(),
                    "model-a".to_owned(),
                ]
            };
            let provider_argv_sha256 =
                provider_argv_sha256(&projected_argv, kind, &prompt_sha256, prompt_bytes).unwrap();
            let receipt = AxeLaunchReceipt {
                schema: "axe.agent-launch-receipt.v1".to_owned(),
                phase: "prepared".to_owned(),
                runtime_id: "testhost.worker-a.agent".to_owned(),
                generation_id: entry.launch_generation_id.clone(),
                identity: "testhost.worker-a".to_owned(),
                workspace: workspace.clone(),
                provider: provider.to_owned(),
                account: "account-a".to_owned(),
                persona: "worker".to_owned(),
                harness: harness.to_owned(),
                model: "model-a".to_owned(),
                effort: "high".to_owned(),
                mode: "managed-unattended".to_owned(),
                boot_contract: "managed-v1".to_owned(),
                runtime_profile: LaunchReceiptPathDigest {
                    path: profile_path,
                    sha256: profile_sha256,
                },
                persona_prompt: LaunchReceiptPathDigest {
                    path: prompt_path,
                    sha256: prompt_sha256.clone(),
                },
                injection: LaunchReceiptInjection {
                    kind,
                    seam: seam.to_owned(),
                    prompt_sha256,
                },
                canonical_provider_argv: projected_argv,
                provider_argv_sha256,
                trajectory_sha256: entry.trajectory_sha256.clone(),
            };
            let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
            std::fs::write(&receipt_path, &receipt_bytes).unwrap();
            entry.prompt.launch_receipt_sha256 = format!("{:x}", Sha256::digest(&receipt_bytes));
            let mut tags = BTreeMap::from([
                (
                    "agent.launch.receipt.schema".to_owned(),
                    "axe.agent-launch-receipt.v1".to_owned(),
                ),
                (
                    "agent.launch.receipt.path".to_owned(),
                    receipt_path.display().to_string(),
                ),
                (
                    "agent.launch.receipt.sha256".to_owned(),
                    entry.prompt.launch_receipt_sha256.clone(),
                ),
                (
                    "agent.generation.id".to_owned(),
                    entry.launch_generation_id.clone(),
                ),
            ]);
            observe_prompt_authority(
                &entry,
                &HostId::parse("testhost").unwrap(),
                "testhost.worker-a.agent",
                &tags,
            )
            .unwrap();
            tags.insert(
                "agent.generation.id".to_owned(),
                "foreign-generation".to_owned(),
            );
            assert!(
                observe_prompt_authority(
                    &entry,
                    &HostId::parse("testhost").unwrap(),
                    "testhost.worker-a.agent",
                    &tags,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn live_prompt_injection_requires_one_exact_harness_specific_seam() {
        use crate::cutover_admission::PromptInjectionKind;

        let prompt_path = Path::new("/nix/store/persona/prompt.md");
        let prompt_bytes = b"exact persona prompt\n";
        let encoded = serde_json::to_string("exact persona prompt\n").unwrap();
        let cases = [
            (
                PromptInjectionKind::ClaudeAppendSystemPromptFile,
                vec![
                    "claude".to_owned(),
                    "--append-system-prompt-file".to_owned(),
                    prompt_path.display().to_string(),
                ],
                Vec::new(),
            ),
            (
                PromptInjectionKind::CodexDeveloperInstructions,
                vec![
                    "codex".to_owned(),
                    "-c".to_owned(),
                    format!("developer_instructions={encoded}"),
                ],
                Vec::new(),
            ),
            (
                PromptInjectionKind::OpencodeSystemPromptFile,
                vec!["opencode".to_owned()],
                vec![format!(
                    "AGENT_SYSTEM_PROMPT_FILE={}",
                    prompt_path.display()
                )],
            ),
            (
                PromptInjectionKind::PiAppendSystemPromptFile,
                vec![
                    "pi".to_owned(),
                    "--append-system-prompt".to_owned(),
                    prompt_path.display().to_string(),
                ],
                Vec::new(),
            ),
        ];
        for (kind, argv, env) in cases {
            validate_effective_prompt_injection(kind, prompt_path, prompt_bytes, &argv, &env)
                .unwrap();

            let mut duplicate = argv.clone();
            match kind {
                PromptInjectionKind::ClaudeAppendSystemPromptFile => duplicate.extend([
                    "--append-system-prompt-file".to_owned(),
                    prompt_path.display().to_string(),
                ]),
                PromptInjectionKind::CodexDeveloperInstructions => {
                    duplicate.extend(["-c".to_owned(), format!("developer_instructions={encoded}")])
                }
                PromptInjectionKind::OpencodeSystemPromptFile => {}
                PromptInjectionKind::PiAppendSystemPromptFile => duplicate.extend([
                    "--append-system-prompt".to_owned(),
                    prompt_path.display().to_string(),
                ]),
            }
            let mut duplicate_env = env.clone();
            if kind == PromptInjectionKind::OpencodeSystemPromptFile {
                duplicate_env.push(format!(
                    "AGENT_SYSTEM_PROMPT_FILE={}",
                    prompt_path.display()
                ));
            }
            assert!(
                validate_effective_prompt_injection(
                    kind,
                    prompt_path,
                    prompt_bytes,
                    &duplicate,
                    &duplicate_env,
                )
                .is_err(),
                "duplicate {kind:?} injection was accepted"
            );

            let mut competing_env = env.clone();
            competing_env.push(format!(
                "AGENT_SYSTEM_PROMPT_FILE={}",
                prompt_path.display()
            ));
            if kind != PromptInjectionKind::OpencodeSystemPromptFile {
                assert!(
                    validate_effective_prompt_injection(
                        kind,
                        prompt_path,
                        prompt_bytes,
                        &argv,
                        &competing_env,
                    )
                    .is_err(),
                    "competing {kind:?} injection was accepted"
                );
            }
        }
    }

    #[test]
    fn live_provider_argv_rejects_binary_model_or_effort_drift_with_exact_prompt_seam() {
        use crate::cutover_admission::PromptInjectionKind;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("claude");
        std::fs::write(&executable, b"provider\n").unwrap();
        let executable = executable.canonicalize().unwrap();
        let executable_metadata = std::fs::metadata(&executable).unwrap();
        let process_identity = ProcIdentity {
            start_time_ticks: 42,
            executable: executable.clone(),
            executable_device: executable_metadata.dev(),
            executable_inode: executable_metadata.ino(),
        };
        let prompt = b"exact persona prompt\n";
        let prompt_sha256 = format!("{:x}", Sha256::digest(prompt));
        let projected = vec![
            executable.display().to_string(),
            "--model".to_owned(),
            "sonnet".to_owned(),
            "--effort".to_owned(),
            "high".to_owned(),
            "--append-system-prompt-file".to_owned(),
            "/nix/store/persona/prompt.md".to_owned(),
        ];
        let expected_sha256 = provider_argv_sha256(
            &projected,
            PromptInjectionKind::ClaudeAppendSystemPromptFile,
            &prompt_sha256,
            prompt,
        )
        .unwrap();
        validate_live_provider_argv(
            &projected,
            &expected_sha256,
            PromptInjectionKind::ClaudeAppendSystemPromptFile,
            &prompt_sha256,
            prompt,
            &projected,
            &executable,
            &process_identity,
        )
        .unwrap();

        for (index, replacement) in [(0, "/foreign/bin/claude"), (2, "opus"), (4, "low")] {
            let mut changed = projected.clone();
            changed[index] = replacement.to_owned();
            assert!(
                validate_live_provider_argv(
                    &projected,
                    &expected_sha256,
                    PromptInjectionKind::ClaudeAppendSystemPromptFile,
                    &prompt_sha256,
                    prompt,
                    &changed,
                    &executable,
                    &process_identity,
                )
                .is_err()
            );
        }

        let foreign_executable = temp.path().join("foreign-claude");
        std::fs::write(&foreign_executable, b"foreign\n").unwrap();
        let foreign_executable = foreign_executable.canonicalize().unwrap();
        let foreign_metadata = std::fs::metadata(&foreign_executable).unwrap();
        let foreign_identity = ProcIdentity {
            start_time_ticks: process_identity.start_time_ticks,
            executable: foreign_executable,
            executable_device: foreign_metadata.dev(),
            executable_inode: foreign_metadata.ino(),
        };
        assert!(
            validate_live_provider_argv(
                &projected,
                &expected_sha256,
                PromptInjectionKind::ClaudeAppendSystemPromptFile,
                &prompt_sha256,
                prompt,
                &projected,
                &executable,
                &foreign_identity,
            )
            .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stable_proc_snapshot_rejects_argv_or_environment_drift_with_same_carrier() {
        let stable = ProcSnapshot {
            identity: ProcIdentity {
                start_time_ticks: 42,
                executable: PathBuf::from("/nix/store/provider/bin/provider"),
                executable_device: 7,
                executable_inode: 8,
            },
            argv: vec!["provider".to_owned(), "--model=sonnet".to_owned()],
            env: vec!["AGENT_SYSTEM_PROMPT_FILE=/nix/store/prompt".to_owned()],
        };
        validate_stable_proc_snapshots(&stable, &stable).unwrap();

        let mut argv_drift = stable.clone();
        argv_drift.argv[1] = "--model=opus".to_owned();
        assert!(validate_stable_proc_snapshots(&stable, &argv_drift).is_err());

        let mut env_drift = stable.clone();
        env_drift.env[0] = "AGENT_SYSTEM_PROMPT_FILE=/foreign/prompt".to_owned();
        assert!(validate_stable_proc_snapshots(&stable, &env_drift).is_err());
    }

    #[test]
    fn live_account_environment_requires_selected_values_and_scrubs_unselected_keys() {
        let selected = BTreeMap::from([("CODEX_HOME".to_owned(), "/accounts/codex-a".to_owned())]);
        let all_keys = BTreeSet::from(["CODEX_HOME".to_owned(), "CLAUDE_CONFIG_DIR".to_owned()]);
        validate_live_account_environment(
            &selected,
            &all_keys,
            &[
                "PATH=/bin".to_owned(),
                "CODEX_HOME=/accounts/codex-a".to_owned(),
            ],
        )
        .unwrap();
        assert!(
            validate_live_account_environment(
                &selected,
                &all_keys,
                &["CODEX_HOME=/accounts/codex-b".to_owned()],
            )
            .is_err()
        );
        assert!(
            validate_live_account_environment(
                &selected,
                &all_keys,
                &[
                    "CODEX_HOME=/accounts/codex-a".to_owned(),
                    "CLAUDE_CONFIG_DIR=/accounts/claude-b".to_owned(),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn provider_census_allows_unrelated_pty_but_rejects_undeclared_provider_and_duplicates() {
        let expected = BTreeSet::from(["custom-provider-runtime".to_owned()]);
        let provider = RuntimeObservation {
            runtime_id: "custom-provider-runtime".to_owned(),
            state: ObservedState::Absent,
            tags: BTreeMap::from([(
                "agent.launch.receipt.schema".to_owned(),
                "axe.agent-launch-receipt.v1".to_owned(),
            )]),
        };
        let unrelated = RuntimeObservation {
            runtime_id: "host.shell".to_owned(),
            state: ObservedState::Absent,
            tags: BTreeMap::from([("run.role".to_owned(), "dev-server".to_owned())]),
        };
        let clean = ObservationBatch {
            complete: true,
            observations: vec![provider.clone(), unrelated],
            errors: Vec::new(),
        };
        validate_complete_provider_pty_census(&expected, &clean).unwrap();
        validate_provider_carriers_unchanged(&expected, &clean, &clean).unwrap();

        let mut changed_carrier = clean.clone();
        changed_carrier.observations[0].tags.insert(
            OBSERVED_PROCESS_PID_TAG.to_owned(),
            "different-child".to_owned(),
        );
        assert!(validate_provider_carriers_unchanged(&expected, &clean, &changed_carrier).is_err());

        let foreign = RuntimeObservation {
            runtime_id: "arbitrary-provider-id".to_owned(),
            state: ObservedState::Absent,
            tags: BTreeMap::from([("role".to_owned(), "agent".to_owned())]),
        };
        let foreign_batch = ObservationBatch {
            complete: true,
            observations: vec![provider.clone(), foreign],
            errors: Vec::new(),
        };
        assert!(validate_complete_provider_pty_census(&expected, &foreign_batch).is_err());

        let legacy = RuntimeObservation {
            runtime_id: "co2-bear-a078".to_owned(),
            state: ObservedState::Absent,
            tags: BTreeMap::from([("run.role".to_owned(), "coding-agent".to_owned())]),
        };
        let legacy_batch = ObservationBatch {
            complete: true,
            observations: vec![provider.clone(), legacy],
            errors: Vec::new(),
        };
        assert!(validate_complete_provider_pty_census(&expected, &legacy_batch).is_err());

        let duplicate_batch = ObservationBatch {
            complete: true,
            observations: vec![provider.clone(), provider],
            errors: Vec::new(),
        };
        assert!(validate_complete_provider_pty_census(&expected, &duplicate_batch).is_err());
    }

    #[test]
    fn complete_pty_census_binds_daemon_generation_to_live_child_process_pid() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let pty = temp.path().join("pty");
        std::fs::write(
            &pty,
            r#"#!/bin/sh
printf '%s\n' '[{"name":"custom-provider-runtime","process":{"alive":true,"pid":99},"daemon":{"pid":41},"createdAt":"2026-07-31T10:00:00.000Z"}]'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&pty, std::fs::Permissions::from_mode(0o755)).unwrap();
        let observer = PtyCli {
            bin: pty.display().to_string(),
            catalog_root: temp.path().to_path_buf(),
        };
        let mut batch = ObservationBatch {
            complete: true,
            observations: vec![RuntimeObservation {
                runtime_id: "custom-provider-runtime".to_owned(),
                state: ObservedState::Running(
                    RuntimeGeneration::new(
                        41,
                        "2026-07-31T10:00:00.000Z".to_owned(),
                        "generation-a".to_owned(),
                    )
                    .unwrap(),
                ),
                tags: BTreeMap::new(),
            }],
            errors: Vec::new(),
        };
        observer
            .enrich_running_process_pids(temp.path(), &mut batch)
            .unwrap();
        assert_eq!(
            batch.observations[0]
                .tags
                .get(OBSERVED_PROCESS_PID_TAG)
                .map(String::as_str),
            Some("99")
        );

        if let ObservedState::Running(generation) = &mut batch.observations[0].state {
            *generation = RuntimeGeneration::new(
                42,
                "2026-07-31T10:00:00.000Z".to_owned(),
                "generation-b".to_owned(),
            )
            .unwrap();
        }
        assert!(
            observer
                .enrich_running_process_pids(temp.path(), &mut batch)
                .is_err()
        );
    }

    #[test]
    fn provider_observer_rejects_catalog_agent_pty_that_is_not_adopt_only_before_observation() {
        use crate::cutover_admission::{
            CanonicalCatalog, HostId, LaunchPromptAuthority, PromptInjectionKind,
            ProviderFleetEntry, ProviderFleetObserver, ProviderFleetProofAction,
        };

        struct NeverObserve;
        impl RuntimeObservationSource for NeverObserve {
            fn observe_complete_pty_root(&self) -> ObservationBatch {
                panic!("non-adopt-only catalog must fail before runtime observation")
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(
            temp.path().join("agent.kdl"),
            format!(
                r#"agent "worker" {{
  identity "worker"
  host "testhost"
  workspace {:?}
  pty "agent" {{
    id "custom-provider-runtime"
    argv "true"
  }}
}}
"#,
                workspace
            ),
        )
        .unwrap();
        let host = HostId::parse("testhost").unwrap();
        let action = ProviderFleetProofAction {
            providers: vec![ProviderFleetEntry {
                identity: "worker".to_owned(),
                host: host.clone(),
                provider: "codex".to_owned(),
                account: "account-a".to_owned(),
                persona: "worker".to_owned(),
                workspace: workspace.clone(),
                prompt: LaunchPromptAuthority {
                    runtime_profile_path: workspace.join("profile.json"),
                    runtime_profile_sha256: "0".repeat(64),
                    persona_prompt_path: workspace.join("prompt.md"),
                    persona_prompt_sha256: "1".repeat(64),
                    launch_receipt_path: workspace.join("receipt.json"),
                    launch_receipt_sha256: "2".repeat(64),
                    injection_kind: PromptInjectionKind::CodexDeveloperInstructions,
                },
                canonical_argv: vec!["axe".to_owned()],
                argv_sha256: "3".repeat(64),
                profile_sha256: "0".repeat(64),
                harness: "codex".to_owned(),
                model: "model-a".to_owned(),
                effort: "high".to_owned(),
                mode: "managed-unattended".to_owned(),
                boot_contract: "managed-v1".to_owned(),
                launch_generation_id: "launch-a".to_owned(),
                runtime_generation_id: "runtime-a".to_owned(),
                trajectory_sha256: "4".repeat(64),
            }],
            providers_sha256: "5".repeat(64),
        };
        let catalog = CanonicalCatalog::open(temp.path()).unwrap();
        let runtime = ProviderFleetRuntimeObserver::new(&action, &NeverObserve);
        assert!(runtime.observe_provider_rows(&catalog, &host).is_err());
    }

    fn target(id: &str, cmd: &str) -> TaskTarget {
        TaskTarget {
            kind: TaskKind::Pty,
            pty_id: id.to_string(),
            bus_id: "hetz.demo".to_string(),
            name: "agent".to_string(),
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

    #[test]
    fn selected_codex_gate_suppresses_launch_on_stale_hooks() {
        let catalog = tempfile::tempdir().unwrap();
        let ownership = HostOwnership::acquire(catalog.path(), "test").unwrap();
        let admission = RuntimeMutationAdmission::ordinary(&ownership).unwrap();
        let spec = AgentSpec {
            identity: "codex".into(),
            name: None,
            description: None,
            host: None,
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            retired: false,
            keep: false,
            restart: None,
            resources: vec![],
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
            path: catalog.path().join("spec.kdl"),
        };
        let runner = GateRunner {
            list_calls: Cell::new(0),
        };
        let report = up_once_selected_specs_with_gates(
            &admission.permission(),
            catalog.path(),
            &[spec],
            "test.codex.agent",
            "test",
            &runner,
            || anyhow::bail!("stale receipt"),
        )
        .unwrap();
        assert_eq!(runner.list_calls.get(), 1);
        assert!(report.launched.is_empty());
        assert!(report.errors.iter().any(|error| {
            error.contains("stale receipt") && error.contains("launch suppressed")
        }));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn idle_supervisor_does_not_spin_on_its_own_catalog_reads() {
        let catalog = tempfile::tempdir().unwrap();
        let stop = AtomicBool::new(false);
        let mut passes = 0usize;
        let ownership = HostOwnership::acquire(catalog.path(), "test-host").unwrap();

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(350));
                stop.store(true, Ordering::SeqCst);
            });
            up_loop_until(
                &ownership,
                catalog.path(),
                "test-host",
                &GateRunner {
                    list_calls: Cell::new(0),
                },
                Duration::from_secs(60),
                &stop,
                |_| passes += 1,
            )
            .unwrap();
        });

        assert!(
            passes <= 2,
            "idle supervisor must wait instead of reconciling its own read events: {passes} passes"
        );
    }

    // ── liveness debounce (R21c): a transient `pty list` not-alive flicker under load must not
    //    destructively GC/relaunch a HEALTHY agent; a stable death must still be reaped ──────────────

    use crate::reconcile::Launch;
    fn sess(id: &str, alive: bool) -> Session {
        Session {
            pty_id: id.to_string(),
            alive,
            exit_code: None,
        }
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
            retired: false,
            keep: false,
            restart: None,
            resources: vec![],
            tasks: vec![],
            path: std::path::PathBuf::from("/x"),
        }
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
        });
        plan.launch.push(Launch {
            spec: &right,
            tasks: vec![right_agent],
        });
        let expected = plan
            .launch
            .iter()
            .map(|launch| launch.spec.identity.clone())
            .collect::<Vec<_>>();
        let mut report = UpReport::default();

        gate_codex_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, || Ok(()));

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
        });
        let mut report = UpReport::default();

        gate_codex_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, || {
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
        });
        plan.launch.push(Launch {
            spec: &claude,
            tasks: vec![claude_agent],
        });
        let mut report = UpReport::default();

        gate_codex_launches_on_hooks(&mut plan, Path::new("/catalog"), &mut report, || {
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
        let cli = PtyCli::default();
        let mut t = target("hetz.demo", "codex");
        t.bus_id = "hetz.demo".to_owned();
        t.tags.insert("unrelated".to_owned(), "preserved".to_owned());
        t.presentation = Some(PtyPresentation {
            pty_id: "hetz.demo".to_owned(),
            display_name: Some(Some("Build owner".to_owned())),
            tags: BTreeMap::from([
                (
                    "agent.presentation.schema".to_owned(),
                    Some("1".to_owned()),
                ),
                (
                    "agent.actor.path".to_owned(),
                    Some("hetz.demo".to_owned()),
                ),
                ("agent.presentation.description".to_owned(), None),
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
        assert!(!tags.iter().any(|tag| tag.starts_with("agent.presentation.description=")));
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
                (
                    "agent.presentation.schema".to_owned(),
                    Some("1".to_owned()),
                ),
                ("agent.presentation.description".to_owned(), None),
            ]),
        };

        cli.patch_presentation(&presentation).unwrap();

        assert_eq!(
            std::fs::read_to_string(executable.with_extension("args")).unwrap(),
            "metadata\npatch\n--id\nstable.agent.id\n"
        );
        let payload: serde_json::Value = serde_json::from_slice(
            &std::fs::read(executable.with_extension("stdin")).unwrap(),
        )
        .unwrap();
        assert_eq!(payload["displayName"], serde_json::Value::Null);
        assert_eq!(payload["tags"]["agent.presentation.schema"], "1");
        assert_eq!(
            payload["tags"]["agent.presentation.description"],
            serde_json::Value::Null
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
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.unwrap().to_string_lossy().into_owned(),
                )
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
        let batch = PtyCli::new(catalog).task_observations(&HashSet::new());
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
            .task_observations_at_root(Some(&HashSet::from(["h.worker.agent"])), &loop_path);
        assert!(!batch.complete);
        assert!(batch.observations.is_empty());
        assert!(
            batch.errors[0].contains("cannot inspect PTY root"),
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
printf '%s\n' '[{"name":"h.live","status":"running","pid":41,"createdAt":"2026-07-31T10:00:00.000Z"},{"name":"h.exit","status":"exited","exitCode":0,"pid":42,"createdAt":"2026-07-31T09:00:00.000Z"},{"name":"h.gone","status":"vanished","pid":43,"createdAt":"2026-07-31T08:00:00.000Z"}]'
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
