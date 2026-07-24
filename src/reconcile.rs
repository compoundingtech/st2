//! Reconcile — the declarative core (VRS R02). Computes **DESIRED** (discovered `service` specs,
//! host-filtered to this machine) vs **ACTUAL** (live task sessions/processes) and returns a plan:
//! which tasks to launch, tear down (retired), adopt (running), skip (other host), and GC.
//!
//! Pure and side-effect-free, so it is exhaustively unit-testable; execution lives behind backends
//! (the `pty` CLI for `pty` tasks, direct process supervision for terminal-free `exec` tasks). st2
//! reconciles at the **task** level — an agent declares several tasks (its harness pty + its ding
//! exec) and each is kept running independently; a spec with one live task and one dead task is a
//! launch of just the missing one.

use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::spec::{AgentSpec, TaskKind};

/// ACTUAL state: one running/known task as st2 observes it (unioned across backends).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The pinned task id — the key that matches a session back to a declared task.
    pub pty_id: String,
    /// `true` while running; `false` once exited/vanished (a GC candidate).
    pub alive: bool,
    /// The process exit code once exited (`None` while running, or if killed/vanished with no code).
    /// Reconcile ignores this; it exists only for crash-vs-clean-exit detection (the crash-ding).
    pub exit_code: Option<i64>,
}

/// A concrete task st2 should spawn — everything a backend needs, resolved from the spec. Produced
/// only for tasks that carry an explicit `command`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTarget {
    /// `pty` (terminal) or `exec` (terminal-free) — selects the backend.
    pub kind: TaskKind,
    /// Resolved task id (the spec's `id`, or `<bus_id>.<name>` fallback).
    pub pty_id: String,
    /// The agent bus id this task belongs to (`<host>.<identity>`).
    pub bus_id: String,
    /// The task name (`agent`, `ding`, …).
    pub name: String,
    /// The exact command line to exec, run verbatim under `sh -c`.
    pub command: String,
    /// Declared working dir; `None` → default to `workspace`, else the spec dir (resolved at spawn).
    pub cwd: Option<String>,
    /// The agent's workspace — the cwd default when `cwd` is unset.
    pub workspace: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    /// GC pin (task-level `keep`, or the agent-level `keep`).
    pub keep: bool,
}

/// An agent to launch, with the specific tasks that are missing (not already live).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch<'a> {
    pub spec: &'a AgentSpec,
    pub tasks: Vec<TaskTarget>,
}

/// An agent to tear down (retired) — the live task ids to kill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Teardown<'a> {
    pub spec: &'a AgentSpec,
    pub pty_ids: Vec<String>,
}

/// The reconcile plan — DESIRED vs ACTUAL, host-filtered. Pure output; execution applies it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcilePlan<'a> {
    /// This host, active service, with ≥1 declared task not currently live → spawn the missing tasks.
    pub launch: Vec<Launch<'a>>,
    /// This host, retired, with live sessions → kill them.
    pub teardown: Vec<Teardown<'a>>,
    /// This host, active service, every declared task already present (live, or dead+`keep` frozen).
    pub adopt: Vec<&'a AgentSpec>,
    /// host != this machine → skipped; another machine's st2 owns it.
    pub other_host: Vec<&'a AgentSpec>,
    /// This host, active service, but no task carries a command (unrendered) → nothing to run.
    pub unrunnable: Vec<&'a AgentSpec>,
    /// Dead, non-`keep` sessions of declared tasks → reap (`rm`).
    pub gc: Vec<String>,
}

/// The state of a declared task's session in the ACTUAL world.
enum SessionState {
    Alive,
    Dead,
    Absent,
}

fn session_state(by_id: &HashMap<&str, bool>, pty_id: &str) -> SessionState {
    match by_id.get(pty_id) {
        Some(true) => SessionState::Alive,
        Some(false) => SessionState::Dead,
        None => SessionState::Absent,
    }
}

/// Resolve a task's on-disk id: the explicit `id`, else `<bus_id>.<name>`.
fn resolve_task_id(bus_id: &str, name: &str, explicit: Option<&str>) -> String {
    match explicit {
        Some(id) => id.to_string(),
        None => format!("{bus_id}.{name}"),
    }
}

/// Compute the reconcile plan for `specs` given observed `sessions`, filtering to `this_host`.
pub fn reconcile<'a>(specs: &'a [AgentSpec], sessions: &[Session], this_host: &str) -> ReconcilePlan<'a> {
    let by_id: HashMap<&str, bool> =
        sessions.iter().map(|s| (s.pty_id.as_str(), s.alive)).collect();

    let mut plan = ReconcilePlan::default();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            plan.other_host.push(spec);
            continue;
        }
        let bus_id = spec.bus_id(this_host);

        if spec.retired {
            let mut teardown_ids = Vec::new();
            for t in &spec.tasks {
                let id = resolve_task_id(&bus_id, &t.name, t.id.as_deref());
                let keep = t.keep || spec.keep;
                match session_state(&by_id, &id) {
                    SessionState::Alive => teardown_ids.push(id),
                    SessionState::Dead if !keep => plan.gc.push(id),
                    _ => {}
                }
            }
            if !teardown_ids.is_empty() {
                plan.teardown.push(Teardown { spec, pty_ids: teardown_ids });
            }
            continue;
        }

        let targets: Vec<TaskTarget> = spec
            .tasks
            .iter()
            .filter_map(|t| {
                let command = t.command.clone()?;
                Some(TaskTarget {
                    kind: t.kind,
                    pty_id: resolve_task_id(&bus_id, &t.name, t.id.as_deref()),
                    bus_id: bus_id.clone(),
                    name: t.name.clone(),
                    command,
                    cwd: t.cwd.clone(),
                    workspace: spec.workspace.clone(),
                    tags: t.tags.clone(),
                    env: t.env.clone(),
                    keep: t.keep || spec.keep,
                })
            })
            .collect();

        if targets.is_empty() {
            plan.unrunnable.push(spec);
            continue;
        }

        let mut to_launch = Vec::new();
        for target in targets {
            match session_state(&by_id, &target.pty_id) {
                SessionState::Alive => {}
                SessionState::Dead if target.keep => {}
                SessionState::Dead => {
                    plan.gc.push(target.pty_id.clone());
                    to_launch.push(target);
                }
                SessionState::Absent => to_launch.push(target),
            }
        }

        if to_launch.is_empty() {
            plan.adopt.push(spec);
        } else {
            plan.launch.push(Launch { spec, tasks: to_launch });
        }
    }
    plan
}
