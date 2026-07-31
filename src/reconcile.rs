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

use agent_spec::spec::{AgentSpec, TaskKind, TaskLifecycle};

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
/// only for tasks that carry an explicit `command` or `argv`.
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
    /// How to launch the task: shell source or a direct program argument vector.
    pub launch: TaskLaunch,
    /// Declared working dir; `None` → default to `workspace`, else the spec dir (resolved at spawn).
    pub cwd: Option<String>,
    /// The agent's workspace — the cwd default when `cwd` is unset.
    pub workspace: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    /// GC pin (task-level `keep`, or the agent-level `keep`).
    pub keep: bool,
}

/// A resolved task launch accepted by the execution backends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskLaunch {
    /// Shell source, preserved verbatim and passed to `sh -c`.
    Shell(String),
    /// A non-empty vector whose first element is the program.
    Argv(Vec<String>),
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
    /// This host, active service, but no task carries a launch (unrendered) → nothing to run.
    pub unrunnable: Vec<&'a AgentSpec>,
    /// Dead, non-`keep` sessions of declared tasks → reap (`rm`).
    pub gc: Vec<String>,
    /// Dead or absent `adopt-only` task ids held without reap or launch.
    pub held: Vec<String>,
}

/// Resolve one exact local task selector (`host.agent.task` or explicit task id) without mutation.
pub fn resolve_task<'a>(
    specs: &'a [AgentSpec],
    selector: &str,
    this_host: &str,
) -> anyhow::Result<(&'a AgentSpec, &'a crate::spec::Task, String)> {
    let mut matches = Vec::new();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            continue;
        }
        for task in &spec.tasks {
            let runtime = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{}.{}", spec.bus_id(this_host), task.name));
            let qualified = format!("{}.{}", spec.bus_id(this_host), task.name);
            if selector == runtime || selector == qualified {
                matches.push((spec, task, runtime));
            }
        }
    }
    match matches.as_slice() {
        [(spec, task, runtime)] => Ok((*spec, *task, runtime.clone())),
        [] => anyhow::bail!("task selector {selector:?} did not resolve to one local task"),
        _ => anyhow::bail!("task selector {selector:?} is ambiguous"),
    }
}

/// Pure task-scoped plan: resolve first, then retain only the selected runtime target.
pub fn reconcile_selected<'a>(
    specs: &'a [AgentSpec],
    sessions: &[Session],
    this_host: &str,
    selector: &str,
) -> anyhow::Result<ReconcilePlan<'a>> {
    let (owner, task, runtime) = resolve_task(specs, selector, this_host)?;
    let mut plan = ReconcilePlan::default();
    let actual = sessions.iter().find(|s| s.pty_id == runtime);
    if owner.retired {
        if let Some(s) = actual {
            if s.alive {
                plan.teardown.push(Teardown {
                    spec: owner,
                    pty_ids: vec![runtime],
                });
            } else if !(task.keep || owner.keep) {
                plan.gc.push(runtime);
            }
        }
        return Ok(plan);
    }
    let launch = match (&task.command, &task.argv) {
        (Some(command), None) => TaskLaunch::Shell(command.clone()),
        (None, Some(argv)) => TaskLaunch::Argv(argv.clone()),
        (None, None) => {
            plan.unrunnable.push(owner);
            return Ok(plan);
        }
        (Some(_), Some(_)) => {
            unreachable!("discovery rejects tasks carrying both command and argv")
        }
    };
    let bus_id = owner.bus_id(this_host);
    let mut env = task.env.clone();
    if let Some(supervisor) = &owner.supervisor {
        env.insert("ST_SUPERVISOR".into(), supervisor.clone());
    } else {
        env.remove("ST_SUPERVISOR");
    }
    let target = TaskTarget {
        kind: task.kind,
        pty_id: runtime.clone(),
        bus_id,
        name: task.name.clone(),
        launch,
        cwd: task.cwd.clone(),
        workspace: owner.workspace.clone(),
        tags: task.tags.clone(),
        env,
        keep: task.keep || owner.keep,
    };
    match actual {
        Some(s) if s.alive => plan.adopt.push(owner),
        _ if task.lifecycle == TaskLifecycle::AdoptOnly => plan.held.push(runtime),
        Some(_) if target.keep => plan.adopt.push(owner),
        Some(_) => {
            plan.gc.push(runtime);
            plan.launch.push(Launch {
                spec: owner,
                tasks: vec![target],
            });
        }
        _ => plan.launch.push(Launch {
            spec: owner,
            tasks: vec![target],
        }),
    }
    Ok(plan)
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
pub fn reconcile<'a>(
    specs: &'a [AgentSpec],
    sessions: &[Session],
    this_host: &str,
) -> ReconcilePlan<'a> {
    let by_id: HashMap<&str, bool> = sessions
        .iter()
        .map(|s| (s.pty_id.as_str(), s.alive))
        .collect();

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
                plan.teardown.push(Teardown {
                    spec,
                    pty_ids: teardown_ids,
                });
            }
            continue;
        }

        if !spec.is_runnable() {
            plan.unrunnable.push(spec);
            continue;
        }

        let targets: Vec<(TaskTarget, TaskLifecycle)> = spec
            .tasks
            .iter()
            .filter_map(|t| {
                let launch = match (&t.command, &t.argv) {
                    (Some(command), None) => TaskLaunch::Shell(command.clone()),
                    (None, Some(argv)) => TaskLaunch::Argv(argv.clone()),
                    (None, None) => return None,
                    (Some(_), Some(_)) => {
                        unreachable!("discovery rejects tasks carrying both command and argv")
                    }
                };
                // `supervisor` is the single source of truth. Hooks and harnesses consume the
                // derived environment variable, but catalog authors/renderers never need to
                // duplicate the relationship in env{} (and cannot accidentally make it disagree).
                let mut env = t.env.clone();
                if let Some(supervisor) = &spec.supervisor {
                    env.insert("ST_SUPERVISOR".to_string(), supervisor.clone());
                } else {
                    env.remove("ST_SUPERVISOR");
                }
                Some((
                    TaskTarget {
                        kind: t.kind,
                        pty_id: resolve_task_id(&bus_id, &t.name, t.id.as_deref()),
                        bus_id: bus_id.clone(),
                        name: t.name.clone(),
                        launch,
                        cwd: t.cwd.clone(),
                        workspace: spec.workspace.clone(),
                        tags: t.tags.clone(),
                        env,
                        keep: t.keep || spec.keep,
                    },
                    t.lifecycle,
                ))
            })
            .collect();

        debug_assert!(!targets.is_empty());

        let mut to_launch = Vec::new();
        let held_before = plan.held.len();
        for (target, lifecycle) in targets {
            match session_state(&by_id, &target.pty_id) {
                SessionState::Alive => {}
                SessionState::Dead | SessionState::Absent
                    if lifecycle == TaskLifecycle::AdoptOnly =>
                {
                    plan.held.push(target.pty_id.clone());
                }
                SessionState::Dead if target.keep => {}
                SessionState::Dead => {
                    plan.gc.push(target.pty_id.clone());
                    to_launch.push(target);
                }
                SessionState::Absent => to_launch.push(target),
            }
        }

        if to_launch.is_empty() && plan.held.len() == held_before {
            plan.adopt.push(spec);
        } else if !to_launch.is_empty() {
            plan.launch.push(Launch {
                spec,
                tasks: to_launch,
            });
        }
    }
    plan
}
