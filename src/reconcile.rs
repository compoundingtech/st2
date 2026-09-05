//! Reconcile — the declarative core (VRS R02). Computes **DESIRED** (discovered `service` specs,
//! host-filtered to this machine) vs **ACTUAL** (live task sessions/processes) and returns a plan:
//! which tasks to launch, tear down (retired), adopt (running), skip (other host), and GC.
//!
//! Pure and side-effect-free, so it is exhaustively unit-testable; execution lives behind backends
//! (the `pty` CLI for `pty` tasks, direct process supervision for terminal-free `exec` tasks). st2
//! reconciles at the **task** level — explicitly authored sibling tasks remain independent. A
//! generated companion is instead eligible only with its canonical agent task and is stopped when
//! that target is held or terminally parked.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use agent_spec::spec::{
    AgentSpec, DeliveryTransport, Driver, TaskKind, TaskLifecycle, stream_name_of_task,
};
use crate::supervisor_chain::{resolve_edge, supervisor_edge};
use crate::AddressBook;
use kdl::KdlValue;

/// Immutable inputs captured once before generated tasks are compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCompileContext {
    catalog_root: PathBuf,
    st2_executable: PathBuf,
}

impl TaskCompileContext {
    pub fn new(catalog_root: PathBuf, st2_executable: PathBuf) -> Result<Self> {
        anyhow::ensure!(
            catalog_root.is_absolute(),
            "task compilation catalog root is not absolute: {}",
            catalog_root.display()
        );
        anyhow::ensure!(
            st2_executable.is_absolute(),
            "task compilation st2 executable is not absolute: {}",
            st2_executable.display()
        );
        anyhow::ensure!(
            st2_executable
                .try_exists()
                .with_context(|| format!("checking st2 executable {}", st2_executable.display()))?,
            "task compilation st2 executable does not exist: {}",
            st2_executable.display()
        );
        anyhow::ensure!(
            st2_executable
                .metadata()
                .with_context(|| format!("reading st2 executable {}", st2_executable.display()))?
                .is_file(),
            "task compilation st2 executable is not a file: {}",
            st2_executable.display()
        );
        Ok(Self {
            catalog_root,
            st2_executable,
        })
    }

    pub fn current(catalog_root: PathBuf) -> Result<Self> {
        let catalog_root = if catalog_root.is_absolute() {
            catalog_root
        } else {
            std::env::current_dir()
                .context("resolving the task compilation catalog root")?
                .join(catalog_root)
        };
        let st2_executable =
            std::env::current_exe().context("resolving the running st2 executable")?;
        Self::new(catalog_root, st2_executable)
    }

    pub fn st2_executable(&self) -> &Path {
        &self.st2_executable
    }

    pub fn catalog_root(&self) -> &Path {
        &self.catalog_root
    }
}

/// Compile every runner-owned launch marker into an exact invocation of this st2 binary.
pub fn compile_generated_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    for spec in specs.iter() {
        crate::driver::ensure_single_source(spec)?;
    }
    compile_driver_agent_tasks(specs, this_host, context)?;
    compile_claude_session_agent_tasks(specs, this_host, context)?;
    compile_pi_session_agent_tasks(specs, this_host, context)?;
    compile_generated_ding_tasks(specs, this_host, context)?;
    compile_app_server_agent_tasks(specs, this_host, context)?;
    // Claude's MCP server remains declared to Claude itself. The canonical task wrapper owns only
    // the provider lifetime and its presence lease. It does not add a supervisor-owned companion.
    Ok(())
}

/// Compile the shared printed driver expansion into the canonical agent task.
pub fn compile_driver_agent_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    let st2_executable = context
        .st2_executable
        .to_str()
        .context("running st2 executable path is not UTF-8")?
        .to_owned();
    let catalog_root = context
        .catalog_root
        .to_str()
        .context("catalog root is not UTF-8")?
        .to_owned();

    for spec in specs {
        let Some(driver) = spec.driver.as_ref() else {
            continue;
        };
        // Diagnostics name the human route; the compiled launch carries the immutable ID.
        let address = spec.bus_address(this_host);
        let expansion = crate::driver::expand_driver(spec, this_host)?;
        let argv_nodes = expansion
            .nodes()
            .iter()
            .filter(|node| node.name().value() == "argv")
            .collect::<Vec<_>>();
        let [argv_node] = argv_nodes.as_slice() else {
            anyhow::bail!(
                "agent '{address}' driver expansion produced {} argv nodes; expected exactly one",
                argv_nodes.len()
            );
        };
        anyhow::ensure!(
            argv_node.children().is_none()
                && argv_node.entries().iter().all(|entry| {
                    entry.name().is_none() && matches!(entry.value(), KdlValue::String(_))
                }),
            "agent '{address}' driver expansion produced a non-string argv"
        );
        let mut argv = argv_node
            .entries()
            .iter()
            .map(|entry| match entry.value() {
                KdlValue::String(value) => value.clone(),
                _ => unreachable!("the argv shape check accepts only strings"),
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !argv.is_empty(),
            "agent '{address}' driver expansion produced an empty argv"
        );

        let wrapper = match driver {
            Driver::Codex(_) => "codex",
            Driver::Claude(_) => "claude-session",
            Driver::Pi(_) => "pi-session",
            Driver::OpenCode(_) => "opencode-session",
            Driver::Omp(_) => "omp-session",
        };
        anyhow::ensure!(
            argv.first().map(String::as_str) == Some("st2")
                && argv.get(1).map(String::as_str) == Some("--catalog")
                && argv.get(2).map(String::as_str) == Some("$CATALOG")
                && argv.get(3).map(String::as_str) == Some("driver")
                && argv.get(4).map(String::as_str) == Some(wrapper),
            "agent '{address}' driver expansion has an unexpected {wrapper} wrapper prefix"
        );
        argv[0] = st2_executable.clone();
        argv[2] = catalog_root.clone();

        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates
            .next()
            .with_context(|| format!("agent '{address}' driver has no canonical `agent` task"))?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{address}' driver has more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{address}' driver canonical task is not a PTY"
        );
        task.command = None;
        task.argv = Some(argv);
    }
    Ok(())
}

/// Route legacy MCP delivery through the same Claude session wrapper as a typed driver.
pub fn compile_claude_session_agent_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    compile_session_wrapped_agent_tasks(
        specs,
        this_host,
        context,
        DeliveryTransport::Mcp,
        "claude-session",
    )
}

/// Route legacy pi-channel delivery through the same pi session wrapper as a typed driver.
///
/// pi's channel is an extension the wrapper injects, so — unlike `mcp` — this transport adds
/// nothing to the workspace. A hand-authored pi task therefore needs only the wrapper.
pub fn compile_pi_session_agent_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    compile_session_wrapped_agent_tasks(
        specs,
        this_host,
        context,
        DeliveryTransport::PiChannel,
        "pi-session",
    )
}

/// The shape both session-wrapped transports share: take the one canonical PTY `agent` task's
/// authored launch and re-express it as that provider's st2 wrapper, preserving the runtime id.
fn compile_session_wrapped_agent_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
    transport: DeliveryTransport,
    wrapper: &str,
) -> Result<()> {
    let st2_executable = context
        .st2_executable
        .to_str()
        .context("running st2 executable path is not UTF-8")?
        .to_owned();
    let catalog_root = context
        .catalog_root
        .to_str()
        .context("catalog root is not UTF-8")?
        .to_owned();

    for spec in specs {
        if spec.driver.is_some() || spec.delivery != Some(transport) {
            continue;
        }
        let selected = transport.as_str();
        let agent_id = spec.agent_id(this_host);
        let address = spec.bus_address(this_host);
        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates.next().with_context(|| {
            format!(
                "agent '{address}' selects `deliver \"{selected}\"` but has no canonical `agent` task"
            )
        })?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{address}' selects `deliver \"{selected}\"` with more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{address}' selects `deliver \"{selected}\"` for a non-PTY canonical task"
        );
        let provider = match (&task.command, &task.argv) {
            (None, Some(argv)) => argv.clone(),
            (Some(command), None) => {
                vec!["sh".to_string(), "-c".to_string(), command.clone()]
            }
            (None, None) => Vec::new(),
            (Some(_), Some(_)) => {
                unreachable!("discovery rejects tasks carrying both command and argv")
            }
        };
        anyhow::ensure!(
            !provider.is_empty(),
            "agent '{address}' selects `deliver \"{selected}\"` with an empty canonical argv"
        );
        let runtime_id = task
            .id
            .clone()
            .unwrap_or_else(|| format!("{agent_id}.{}", task.name));
        let mut argv = vec![
            st2_executable.clone(),
            "--catalog".to_string(),
            catalog_root.clone(),
            "driver".to_string(),
            wrapper.to_string(),
            "--id".to_string(),
            agent_id,
            "--runtime-id".to_string(),
            runtime_id,
            "--".to_string(),
        ];
        argv.extend(provider);
        task.command = None;
        task.argv = Some(argv);
    }
    Ok(())
}

/// Replace only runner-generated companion markers with exact direct argv. Authored tasks never
/// carry `derived=true`, so source that happens to invoke `st2 ding` or `st2 stream run` remains
/// byte-for-byte unchanged.
///
/// The single `ensure!` below is the fail-closed "unsupported derived task" gate. Every derived
/// companion kind must be named in the exhaustive match; an unrecognized derived task refuses the
/// pass rather than reaching a runner with an unbound placeholder command.
pub fn compile_generated_ding_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    let st2_executable = context
        .st2_executable
        .to_str()
        .context("running st2 executable path is not UTF-8")?
        .to_owned();
    for spec in specs {
        let agent_id = spec.agent_id(this_host);
        for task in &mut spec.tasks {
            if !task.derived {
                continue;
            }
            let is_ding = task.name == "ding" || task.name.ends_with(".ding");
            let task_name = task.name.clone();
            let stream_name = stream_name_of_task(&task_name).map(str::to_owned);
            anyhow::ensure!(
                task.kind == TaskKind::Exec && (is_ding || stream_name.is_some()),
                "unsupported derived task: {}",
                task.name
            );
            let effective_root = task
                .env
                .get("ST_ROOT")
                .map(|root| crate::expand::expand_catalog(root, &context.catalog_root))
                .unwrap_or_else(|| context.catalog_root.display().to_string());
            anyhow::ensure!(
                Path::new(&effective_root).is_absolute(),
                "derived companion root is not absolute: {effective_root}"
            );
            if let Some(stream_name) = stream_name {
                anyhow::ensure!(
                    task.command.is_some() != task.argv.is_some(),
                    "derived stream task '{task_name}' for stream '{stream_name}' has no exact launch"
                );
                continue;
            }
            task.command = None;
            task.argv = Some(vec![
                st2_executable.clone(),
                "ding".to_string(),
                // Agent-spec lowers `st2 ding --id <agent-id>`; the late-bound rewrite keeps that
                // exact-ID form rather than handing DING a mutable route.
                "--id".to_string(),
                agent_id.clone(),
                "--root".to_string(),
                effective_root,
            ]);
        }
    }
    Ok(())
}

/// Route an explicitly selected Codex native transport through st2's controlled-launch wrapper.
///
/// The wrapper owns the provider daemon and its control connection, so it can complete the
/// initialize handshake before the interactive client is allowed to create or resume a thread.
/// App-server delivery therefore requires structured argv: rewriting opaque shell source would be
/// unsound, and an already-remote launch would have two competing control owners.
pub fn compile_app_server_agent_tasks(
    specs: &mut [AgentSpec],
    this_host: &str,
    context: &TaskCompileContext,
) -> Result<()> {
    let st2_executable = context
        .st2_executable
        .to_str()
        .context("running st2 executable path is not UTF-8")?
        .to_owned();
    let catalog_root = context
        .catalog_root
        .to_str()
        .context("catalog root is not UTF-8")?
        .to_owned();

    for spec in specs {
        if spec.driver.is_some() {
            continue;
        }
        if spec.delivery != Some(DeliveryTransport::AppServer) {
            continue;
        }
        let agent_id = spec.agent_id(this_host);
        let address = spec.bus_address(this_host);
        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates.next().with_context(|| {
            format!(
                "agent '{address}' selects `deliver \"app-server\"` but has no canonical `agent` task"
            )
        })?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{address}' selects `deliver \"app-server\"` with more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{address}' selects `deliver \"app-server\"` for a non-PTY canonical task"
        );
        let authored = task.argv.clone().with_context(|| {
            format!(
                "agent '{address}' selects `deliver \"app-server\"`; its canonical task must use structured `argv`, not shell `command`"
            )
        })?;
        anyhow::ensure!(
            !authored.is_empty(),
            "agent '{address}' selects `deliver \"app-server\"` with an empty canonical argv"
        );
        anyhow::ensure!(
            !authored
                .iter()
                .any(|arg| arg == "--remote" || arg.starts_with("--remote=")),
            "agent '{address}' selects `deliver \"app-server\"` but its canonical argv already declares `--remote`"
        );
        let runtime_id = task
            .id
            .clone()
            .unwrap_or_else(|| format!("{agent_id}.{}", task.name));
        let mut argv = vec![
            st2_executable.clone(),
            "--catalog".to_string(),
            catalog_root.clone(),
            "codex-app-server".to_string(),
            "--id".to_string(),
            agent_id,
            "--runtime-id".to_string(),
            runtime_id,
            "--".to_string(),
        ];
        argv.extend(authored);
        task.command = None;
        task.argv = Some(argv);
    }
    Ok(())
}

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
    /// PTY presentation observed in the same authoritative inventory snapshot. Exec sessions and
    /// older/partial observations leave this unknown so reconciliation repairs them fail-closed.
    pub presentation: Option<ObservedPtyPresentation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPtyPresentation {
    pub display_name: Option<String>,
    pub tags: BTreeMap<String, String>,
}

/// A concrete task st2 should spawn — everything a backend needs, resolved from the spec. Produced
/// only for tasks that carry an explicit `command` or `argv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTarget {
    /// `pty` (terminal) or `exec` (terminal-free) — selects the backend.
    pub kind: TaskKind,
    /// Resolved task id (the spec's explicit task `id`, or the `<agent-id>.<name>` default).
    pub pty_id: String,
    /// The immutable agent ID that owns this task. Everything keyed off ownership — `ST_AGENT`,
    /// default task IDs, adoption, teardown, park accounting — reads this, never the address.
    pub agent_id: String,
    /// The owning agent's current bus address: the human route, used for presentation only. A
    /// change here must never move a task ID, a launch fingerprint, or an adoption decision.
    pub bus_address: String,
    /// The task name (`agent`, `ding`, …).
    pub name: String,
    /// Generated from another task rather than authored as an independent sibling.
    pub derived: bool,
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
    /// Desired PTY-only presentation projected at spawn. Exec tasks carry `None`.
    pub presentation: Option<PtyPresentation>,
}

/// Exact, non-lifecycle metadata desired for one managed PTY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyPresentation {
    /// Exact stable PTY task ID. Automation must never resolve this as a display alias.
    pub pty_id: String,
    /// `Some(value)` updates primary-agent display metadata; `Some(None)` clears it. `None` preserves
    /// a secondary task's existing task-specific display convention.
    pub display_name: Option<Option<String>>,
    /// Complete st2-owned tag snapshot. `None` removes an optional owned key.
    pub tags: BTreeMap<String, Option<String>>,
}

pub const AGENT_PRESENTATION_SCHEMA_TAG: &str = "agent.presentation.schema";
/// Owned-metadata schema carried by every managed PTY: immutable actor ID plus current address.
pub const AGENT_PRESENTATION_SCHEMA: &str = "2";
/// The immutable agent ID of the actor that owns this PTY.
pub const AGENT_ACTOR_ID_TAG: &str = "agent.actor.id";
/// The actor's current bus address. Removed when the subject is non-routable.
pub const AGENT_ACTOR_ADDRESS_TAG: &str = "agent.actor.address";
/// The schema-1 owned key this schema replaces. It held a *route*, which after an address cutover
/// may name a different subject entirely, so leaving it behind would strand a stale alias on the
/// session forever. It is st2-owned, so the schema-2 patch deletes it: the projection always
/// carries this key with `None`.
pub const LEGACY_AGENT_ACTOR_PATH_TAG: &str = "agent.actor.path";
pub const AGENT_DESCRIPTION_TAG: &str = "agent.presentation.description";
/// Compatibility role owned by st2 only on the canonical agent PTY.
pub const COMPATIBILITY_ROLE_TAG: &str = "role";

/// Fail-closed admission errors for runner-owned task identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskIdentityAdmissionError {
    Conflict {
        agent_id: String,
        task: String,
        declared: String,
    },
}

impl fmt::Display for TaskIdentityAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict {
                agent_id,
                task,
                declared,
            } => write!(
                formatter,
                "agent '{agent_id}' task '{task}' declares conflicting ST_AGENT '{declared}'; expected runner-owned value '{agent_id}'"
            ),
        }
    }
}

impl std::error::Error for TaskIdentityAdmissionError {}

/// Reject local active tasks whose authored `ST_AGENT` conflicts with the runner-owned agent ID.
pub fn validate_task_identities(
    specs: &[AgentSpec],
    this_host: &str,
) -> Result<(), TaskIdentityAdmissionError> {
    for spec in specs {
        if !spec.desired_state.is_running() || spec.resolved_host(this_host) != this_host {
            continue;
        }
        let agent_id = spec.agent_id(this_host);
        for task in &spec.tasks {
            if let Some(declared) = task.env.get("ST_AGENT")
                && declared != &agent_id
            {
                return Err(TaskIdentityAdmissionError::Conflict {
                    agent_id,
                    task: task.name.clone(),
                    declared: declared.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Project runner-owned identity and the supervisor source of truth into one launch target.
///
/// `ST_AGENT` carries the raw immutable agent ID and nothing else: it is consumed downstream
/// through the typed exact-ID path, so it must never be an address or a separately concatenated
/// host prefix.
fn runner_task_env(
    spec: &AgentSpec,
    task: &crate::spec::Task,
    agent_id: &str,
) -> BTreeMap<String, String> {
    let mut env = task.env.clone();
    env.insert("ST_AGENT".to_owned(), agent_id.to_owned());
    if let Some(supervisor) = &spec.supervisor {
        env.insert("ST_SUPERVISOR".to_owned(), supervisor.clone());
    } else {
        env.remove("ST_SUPERVISOR");
    }
    env
}

/// The exact owned metadata snapshot (schema 2) desired for one managed PTY.
///
/// Immutable actor ID, the current bus address, and the optional description. A non-routable
/// subject has released its address, so its owned address tag is removed rather than frozen at
/// the last route. Only the canonical compact agent task — the one whose task ID *is* the agent
/// ID — carries `role=agent` and maps `name` to native display metadata.
///
/// The snapshot also always carries [`LEGACY_AGENT_ACTOR_PATH_TAG`] with `None`. Schema 1 stored a
/// route under that key; leaving it on a session that predates this schema would strand an alias
/// that a later address cutover can point at a different subject, so the same patch that writes
/// schema 2 deletes it. Removing an owned key is idempotent: once gone, the desired and observed
/// snapshots agree and no further patch is emitted.
fn pty_presentation(
    spec: &AgentSpec,
    task: &crate::spec::Task,
    pty_id: &str,
    agent_id: &str,
    bus_address: Option<&str>,
) -> Option<PtyPresentation> {
    if task.kind != TaskKind::Pty {
        return None;
    }
    let canonical_agent = task.name == "agent" && pty_id == agent_id;
    Some(PtyPresentation {
        pty_id: pty_id.to_owned(),
        display_name: canonical_agent.then(|| match spec.name.as_ref() {
            Some(name) if name == pty_id => None,
            _ => spec.name.clone(),
        }),
        tags: BTreeMap::from([
            (
                AGENT_PRESENTATION_SCHEMA_TAG.to_owned(),
                Some(AGENT_PRESENTATION_SCHEMA.to_owned()),
            ),
            (AGENT_ACTOR_ID_TAG.to_owned(), Some(agent_id.to_owned())),
            (
                AGENT_ACTOR_ADDRESS_TAG.to_owned(),
                bus_address.map(str::to_owned),
            ),
            (LEGACY_AGENT_ACTOR_PATH_TAG.to_owned(), None),
            (AGENT_DESCRIPTION_TAG.to_owned(), spec.description.clone()),
            (
                COMPATIBILITY_ROLE_TAG.to_owned(),
                canonical_agent.then(|| "agent".to_owned()),
            ),
        ]),
    })
}

/// The owning agent's routable bus address, or `None` once the subject is non-routable.
fn routable_bus_address(spec: &AgentSpec, this_host: &str) -> Option<String> {
    (!spec.desired_state.is_retired()).then(|| spec.bus_address(this_host))
}

fn presentation_matches(desired: &PtyPresentation, observed: &ObservedPtyPresentation) -> bool {
    let display_name_matches = desired
        .display_name
        .as_ref()
        .is_none_or(|display_name| display_name == &observed.display_name);
    display_name_matches
        && desired
            .tags
            .iter()
            .all(|(key, value)| observed.tags.get(key) == value.as_ref())
}

/// Who to notify when a task under this launch crash-loops, decided once from the same catalog
/// snapshot the plan was computed from.
///
/// A declared `supervisor` is a free-form human reference, not a typed selector, so it must be
/// resolved before it can key anything. Resolving it here — rather than at the notification site —
/// means the alert path never hands a route to an exact-ID resolver, and a reference that names
/// nothing is reported instead of silently swallowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorTarget {
    /// The declaration names no supervisor: there is nobody to notify.
    Undeclared,
    /// The declared reference resolved to exactly one subject, named here by its immutable ID.
    Resolved(String),
    /// The declared reference resolved to no subject or to more than one. Carries the reference as
    /// authored so the diagnostic can name what the operator must fix.
    Unresolved(String),
}

/// Resolve a declaration's `supervisor` edge once, against one catalog generation, so the
/// crash-loop alert reaches exactly the subject the org chart says owns this agent.
///
/// The value's namespace is decided in exactly one place —
/// [`crate::supervisor_chain::supervisor_edge`] — from the CHILD's migration state, so this path,
/// the org-chart walk, DING, resync, and authoring can never disagree about an edge.
fn resolve_supervisor(
    specs: &[AgentSpec],
    book: Option<&AddressBook>,
    spec: &AgentSpec,
    this_host: &str,
) -> SupervisorTarget {
    let Some(reference) = spec.supervisor.as_deref() else {
        return SupervisorTarget::Undeclared;
    };
    let unresolved = || SupervisorTarget::Unresolved(reference.to_owned());
    // A catalog this pass cannot project into one address book cannot attribute the edge either.
    let (Some(_book), Some(edge)) = (book, supervisor_edge(specs, spec, this_host)) else {
        return unresolved();
    };
    match resolve_edge(specs, &edge, this_host) {
        Some(parent) => SupervisorTarget::Resolved(parent.agent_id(this_host)),
        None => unresolved(),
    }
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
    /// Exact derived task IDs proved live in the same inventory snapshot. Execution stops these if
    /// the canonical agent becomes terminal while applying this launch.
    pub live_derived: Vec<String>,
    /// The already-resolved crash-loop notification target for `spec`.
    pub supervisor: SupervisorTarget,
}

/// Exact live task IDs to stop because their owner retired or their derived target is ineligible.
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
    /// This host, retired → archive every inbox message, even when no session remains.
    pub settle_retirement: Vec<&'a AgentSpec>,
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
    /// In-place presentation updates for healthy managed PTYs, independent of lifecycle actions.
    pub presentation: Vec<PtyPresentation>,
    /// Declared task ids this pass PROVED alive in the inventory snapshot — the positive evidence
    /// the restart cap needs. It is deliberately not "everything we did not launch": a task can be
    /// missing from `launch` because it was never considered (its owner failed to materialize, its
    /// launch was gated, its death was debounced as a flicker), and inferring liveness from that
    /// silence credits uptime to a task nobody looked at. Excludes ids headed for `teardown`.
    pub live: Vec<String>,
}

/// Resolve one exact local task selector without mutation: either an explicit task ID or the
/// `<agent-id>.<task-name>` default. Task selection is exact-ID selection (R19) — it deliberately
/// does not fall through to human-address lookup.
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
            let agent_id = spec.agent_id(this_host);
            let qualified = format!("{agent_id}.{}", task.name);
            let runtime = task.id.clone().unwrap_or_else(|| qualified.clone());
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
    validate_task_identities(specs, this_host)?;
    let (owner, task, runtime) = resolve_task(specs, selector, this_host)?;
    let mut plan = ReconcilePlan::default();
    let book = crate::spec::address_book(specs, this_host).ok();
    if owner.desired_state.is_retired() {
        plan.settle_retirement.push(owner);
    }
    let actual = sessions.iter().find(|s| s.pty_id == runtime);
    if !owner.desired_state.is_running() {
        if let Some(s) = actual {
            if s.alive {
                plan.teardown.push(Teardown {
                    spec: owner,
                    pty_ids: vec![runtime],
                });
            } else if owner.desired_state.is_retired() || !(task.keep || owner.keep) {
                plan.gc.push(runtime);
            }
        }
        return Ok(plan);
    }
    if task.derived && !actual.is_some_and(|session| session.alive) {
        plan.held.push(runtime);
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
    let agent_id = owner.agent_id(this_host);
    let routable_address = routable_bus_address(owner, this_host);
    let env = runner_task_env(owner, task, &agent_id);
    let target = TaskTarget {
        kind: task.kind,
        pty_id: runtime.clone(),
        agent_id: agent_id.clone(),
        bus_address: owner.bus_address(this_host),
        name: task.name.clone(),
        derived: task.derived,
        launch,
        cwd: task.cwd.clone(),
        workspace: owner.workspace.clone(),
        tags: task.tags.clone(),
        env,
        keep: task.keep || owner.keep,
        presentation: pty_presentation(
            owner,
            task,
            &runtime,
            &agent_id,
            routable_address.as_deref(),
        ),
    };
    match actual {
        Some(s) if s.alive => {
            if let Some(presentation) = target.presentation.clone()
                && !s
                    .presentation
                    .as_ref()
                    .is_some_and(|observed| presentation_matches(&presentation, observed))
            {
                plan.presentation.push(presentation);
            }
            plan.live.push(runtime);
            plan.adopt.push(owner);
        }
        _ if task.lifecycle == TaskLifecycle::AdoptOnly => plan.held.push(runtime),
        Some(_) if target.keep => plan.adopt.push(owner),
        Some(_) => {
            plan.gc.push(runtime);
            plan.launch.push(Launch {
                spec: owner,
                tasks: vec![target],
                live_derived: Vec::new(),
                supervisor: resolve_supervisor(specs, book.as_ref(), owner, this_host),
            });
        }
        _ => plan.launch.push(Launch {
            spec: owner,
            tasks: vec![target],
            live_derived: Vec::new(),
            supervisor: resolve_supervisor(specs, book.as_ref(), owner, this_host),
        }),
    }
    Ok(plan)
}

/// The state of a declared task's session in the ACTUAL world.
#[derive(Clone, Copy)]
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

/// Resolve a task's on-disk id: the explicit task `id`, else `<agent-id>.<name>`. This is the
/// session name `pty` binds a socket for, so admission checks resolve it through here rather than
/// re-deriving the format. Host placement is never separately concatenated: for an unmigrated
/// declaration the agent ID already *is* the frozen `<host>.<identity>` bytes, which is why no
/// legacy task ID or socket path moves.
pub(crate) fn resolve_task_id(agent_id: &str, name: &str, explicit: Option<&str>) -> String {
    match explicit {
        Some(id) => id.to_string(),
        None => format!("{agent_id}.{name}"),
    }
}

/// Compute the reconcile plan for `specs` given observed `sessions`, filtering to `this_host`.
pub fn reconcile<'a>(
    specs: &'a [AgentSpec],
    sessions: &[Session],
    this_host: &str,
) -> Result<ReconcilePlan<'a>, TaskIdentityAdmissionError> {
    validate_task_identities(specs, this_host)?;
    let by_id: HashMap<&str, bool> = sessions
        .iter()
        .map(|s| (s.pty_id.as_str(), s.alive))
        .collect();
    let sessions_by_id: HashMap<&str, &Session> = sessions
        .iter()
        .map(|session| (session.pty_id.as_str(), session))
        .collect();
    // One book for the whole pass, so every supervisor edge this plan records and the uniqueness
    // proof behind it describe the same catalog generation.
    let book = crate::spec::address_book(specs, this_host).ok();

    let mut plan = ReconcilePlan::default();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            plan.other_host.push(spec);
            continue;
        }
        let agent_id = spec.agent_id(this_host);
        let routable_address = routable_bus_address(spec, this_host);

        if !spec.desired_state.is_running() {
            if spec.desired_state.is_retired() {
                plan.settle_retirement.push(spec);
            }
            let mut teardown_ids = Vec::new();
            for t in &spec.tasks {
                let id = resolve_task_id(&agent_id, &t.name, t.id.as_deref());
                let retain_dead = spec.desired_state.is_suspended() && (t.keep || spec.keep);
                match session_state(&by_id, &id) {
                    SessionState::Alive => teardown_ids.push(id),
                    SessionState::Dead if !retain_dead => plan.gc.push(id),
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
                let env = runner_task_env(spec, t, &agent_id);
                let pty_id = resolve_task_id(&agent_id, &t.name, t.id.as_deref());
                Some((
                    TaskTarget {
                        kind: t.kind,
                        pty_id: pty_id.clone(),
                        agent_id: agent_id.clone(),
                        bus_address: spec.bus_address(this_host),
                        name: t.name.clone(),
                        derived: t.derived,
                        launch,
                        cwd: t.cwd.clone(),
                        workspace: spec.workspace.clone(),
                        tags: t.tags.clone(),
                        env,
                        keep: t.keep || spec.keep,
                        presentation: pty_presentation(
                            spec,
                            t,
                            &pty_id,
                            &agent_id,
                            routable_address.as_deref(),
                        ),
                    },
                    t.lifecycle,
                ))
            })
            .collect();

        debug_assert!(!targets.is_empty());

        let agent_eligible = targets
            .iter()
            .find(|(target, _)| target.name == "agent" && !target.derived)
            .is_some_and(
                |(target, lifecycle)| match session_state(&by_id, &target.pty_id) {
                    SessionState::Alive => true,
                    SessionState::Dead => !target.keep && *lifecycle == TaskLifecycle::Service,
                    SessionState::Absent => *lifecycle == TaskLifecycle::Service,
                },
            );
        let mut to_launch = Vec::new();
        let mut live_derived = Vec::new();
        let mut ineligible_derived = Vec::new();
        let mut derived_cleanup = false;
        let held_before = plan.held.len();
        for (target, lifecycle) in targets {
            let state = session_state(&by_id, &target.pty_id);
            if target.derived && !agent_eligible {
                match state {
                    SessionState::Alive => {
                        ineligible_derived.push(target.pty_id);
                        derived_cleanup = true;
                    }
                    SessionState::Dead if !target.keep => {
                        plan.gc.push(target.pty_id);
                        derived_cleanup = true;
                    }
                    SessionState::Dead | SessionState::Absent => {}
                }
                continue;
            }
            match state {
                SessionState::Alive => {
                    plan.live.push(target.pty_id.clone());
                    if target.derived {
                        live_derived.push(target.pty_id.clone());
                    }
                    let actual = sessions_by_id
                        .get(target.pty_id.as_str())
                        .expect("alive state has a session");
                    if let Some(presentation) = target.presentation.clone()
                        && !actual
                            .presentation
                            .as_ref()
                            .is_some_and(|observed| presentation_matches(&presentation, observed))
                    {
                        plan.presentation.push(presentation);
                    }
                }
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
        if !ineligible_derived.is_empty() {
            plan.teardown.push(Teardown {
                spec,
                pty_ids: ineligible_derived,
            });
        }

        if to_launch.is_empty() && plan.held.len() == held_before && !derived_cleanup {
            plan.adopt.push(spec);
        } else if !to_launch.is_empty() {
            plan.launch.push(Launch {
                spec,
                tasks: to_launch,
                live_derived,
                // Resolved here, once per launched agent, so nothing downstream re-parses the
                // authored reference — and only for agents that can actually reach the park path.
                supervisor: resolve_supervisor(specs, book.as_ref(), spec, this_host),
            });
        }
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use agent_spec::spec::{AgentDesiredState, JobType, Task};
    use agent_spec::{AgentAddress, AgentId};

    use super::*;

    const ID: &str = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";

    fn task(name: &str, kind: TaskKind, id: Option<&str>) -> Task {
        Task {
            kind,
            derived: false,
            name: name.to_owned(),
            id: id.map(str::to_owned),
            command: Some("true".to_owned()),
            argv: None,
            cwd: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
            lifecycle: TaskLifecycle::Service,
        }
    }

    fn spec(tasks: Vec<Task>) -> AgentSpec {
        AgentSpec {
            id: None,
            address: None,
            identity: "worker".to_owned(),
            name: None,
            description: None,
            host: Some("dev3".to_owned()),
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            desired_state: AgentDesiredState::Running,
            keep: false,
            restart: None,
            delivery: None,
            session_driver: None,
            driver: None,
            delivery_readiness: None,
            resources: Vec::new(),
            streams: Vec::new(),
            tasks,
            path: PathBuf::from("/catalog/agents/dev3/worker/agent.kdl"),
        }
    }

    fn migrated(tasks: Vec<Task>) -> AgentSpec {
        let mut spec = spec(tasks);
        spec.id = Some(AgentId::parse(ID).unwrap());
        spec.address = Some(AgentAddress::parse("fractal.keymap.verifier").unwrap());
        spec
    }

    fn target_of<'a>(plan: &'a ReconcilePlan<'_>, pty_id: &str) -> &'a TaskTarget {
        plan.launch
            .iter()
            .flat_map(|launch| &launch.tasks)
            .find(|target| target.pty_id == pty_id)
            .unwrap_or_else(|| panic!("no launch target {pty_id}"))
    }

    /// `ST_AGENT` is consumed through the typed exact-ID path, so it must be the raw agent ID:
    /// not the mutable bus address, and not the ID with a host concatenated onto it.
    #[test]
    fn st_agent_carries_the_raw_immutable_agent_id() {
        let declared = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        let plan = reconcile(std::slice::from_ref(&declared), &[], "dev3").unwrap();

        let target = target_of(&plan, ID);
        assert_eq!(target.env.get("ST_AGENT").map(String::as_str), Some(ID));
        assert_eq!(target.agent_id, ID);
        assert_eq!(target.bus_address, "dev3.fractal.keymap.verifier");
    }

    /// An unmigrated declaration yields exactly the frozen legacy bytes, which is why no task ID
    /// or socket path moves across migration.
    #[test]
    fn an_unmigrated_declaration_keeps_its_frozen_legacy_st_agent() {
        let declared = spec(vec![task("agent", TaskKind::Pty, Some("dev3.worker"))]);
        let plan = reconcile(std::slice::from_ref(&declared), &[], "dev3").unwrap();

        assert_eq!(
            target_of(&plan, "dev3.worker")
                .env
                .get("ST_AGENT")
                .map(String::as_str),
            Some("dev3.worker")
        );
    }

    /// Every long-form named task without an explicit task ID defaults to `<agent-id>.<task-name>`
    /// — including a task named `agent`. An authored task ID stays authoritative.
    #[test]
    fn default_task_ids_are_agent_id_dot_task_name_including_a_task_named_agent() {
        let declared = migrated(vec![
            task("agent", TaskKind::Pty, None),
            task("work", TaskKind::Pty, None),
            task("authored", TaskKind::Exec, Some("chosen.by.hand")),
        ]);
        let plan = reconcile(std::slice::from_ref(&declared), &[], "dev3").unwrap();

        let mut ids = plan
            .launch
            .iter()
            .flat_map(|launch| &launch.tasks)
            .map(|target| target.pty_id.as_str())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(
            ids,
            [
                &format!("{ID}.agent"),
                &format!("{ID}.work"),
                "chosen.by.hand",
            ]
        );
    }

    /// The exact owned tag snapshot, schema 2. An absent optional value is removed rather than
    /// left stale, and the non-canonical role tag is cleared rather than omitted.
    #[test]
    fn owned_metadata_is_the_schema_two_snapshot_and_removes_absent_optionals() {
        let mut declared = migrated(vec![task("work", TaskKind::Pty, None)]);
        declared.description = None;
        let projected = pty_presentation(
            &declared,
            &declared.tasks[0],
            &format!("{ID}.work"),
            ID,
            Some("dev3.fractal.keymap.verifier"),
        )
        .unwrap();

        assert_eq!(
            projected.tags,
            BTreeMap::from([
                (
                    AGENT_PRESENTATION_SCHEMA_TAG.to_owned(),
                    Some("2".to_owned())
                ),
                (AGENT_ACTOR_ID_TAG.to_owned(), Some(ID.to_owned())),
                (
                    AGENT_ACTOR_ADDRESS_TAG.to_owned(),
                    Some("dev3.fractal.keymap.verifier".to_owned())
                ),
                (LEGACY_AGENT_ACTOR_PATH_TAG.to_owned(), None),
                (AGENT_DESCRIPTION_TAG.to_owned(), None),
                (COMPATIBILITY_ROLE_TAG.to_owned(), None),
            ])
        );
        assert_eq!(
            projected.tags[LEGACY_AGENT_ACTOR_PATH_TAG], None,
            "schema 1's actor path is deleted by this patch, never emitted alongside schema 2"
        );
        assert_eq!(
            projected.display_name, None,
            "a secondary PTY keeps its own display convention"
        );
    }

    /// A retired subject is non-routable: it keeps its ID and releases its address, so the owned
    /// address tag is removed rather than frozen at the last route.
    #[test]
    fn a_retired_subject_releases_its_owned_address_tag_but_keeps_its_id() {
        let mut declared = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        declared.desired_state = AgentDesiredState::Retired { reason: None };
        let address = routable_bus_address(&declared, "dev3");
        assert_eq!(address, None);

        let projected =
            pty_presentation(&declared, &declared.tasks[0], ID, ID, address.as_deref()).unwrap();
        assert_eq!(projected.tags[AGENT_ACTOR_ADDRESS_TAG], None);
        assert_eq!(projected.tags[AGENT_ACTOR_ID_TAG], Some(ID.to_owned()));
    }

    /// Only the canonical compact agent task — the one whose task ID *is* the agent ID — carries
    /// `role=agent` and maps `name` to native display metadata.
    #[test]
    fn only_the_canonical_compact_task_carries_role_agent_and_a_display_name() {
        let mut declared = migrated(vec![
            task("agent", TaskKind::Pty, Some(ID)),
            task("agent", TaskKind::Pty, None),
        ]);
        declared.name = Some("Keymap verifier".to_owned());
        let plan = reconcile(std::slice::from_ref(&declared), &[], "dev3").unwrap();

        let canonical = target_of(&plan, ID).presentation.as_ref().unwrap();
        assert_eq!(
            canonical.display_name,
            Some(Some("Keymap verifier".to_owned()))
        );
        assert_eq!(
            canonical.tags[COMPATIBILITY_ROLE_TAG],
            Some("agent".to_owned())
        );

        let long_form = target_of(&plan, &format!("{ID}.agent"))
            .presentation
            .as_ref()
            .unwrap();
        assert_eq!(long_form.display_name, None);
        assert_eq!(
            long_form.tags[COMPATIBILITY_ROLE_TAG], None,
            "a non-canonical PTY must have the role tag cleared"
        );
    }

    /// Projection is idempotent: an already-correct PTY produces no patch, so `pty` emits no
    /// `metadata_change` event. Unrelated observed tags never provoke one either.
    #[test]
    fn an_already_correct_pty_produces_no_presentation_patch() {
        let mut declared = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        declared.description = Some("Verifies keymaps.".to_owned());
        let observed = Session {
            pty_id: ID.to_owned(),
            alive: true,
            exit_code: None,
            presentation: Some(ObservedPtyPresentation {
                display_name: None,
                tags: BTreeMap::from([
                    (AGENT_PRESENTATION_SCHEMA_TAG.to_owned(), "2".to_owned()),
                    (AGENT_ACTOR_ID_TAG.to_owned(), ID.to_owned()),
                    (
                        AGENT_ACTOR_ADDRESS_TAG.to_owned(),
                        "dev3.fractal.keymap.verifier".to_owned(),
                    ),
                    (
                        AGENT_DESCRIPTION_TAG.to_owned(),
                        "Verifies keymaps.".to_owned(),
                    ),
                    (COMPATIBILITY_ROLE_TAG.to_owned(), "agent".to_owned()),
                    ("unrelated".to_owned(), "preserved".to_owned()),
                ]),
            }),
        };

        let plan = reconcile(
            std::slice::from_ref(&declared),
            std::slice::from_ref(&observed),
            "dev3",
        )
        .unwrap();
        assert!(
            plan.presentation.is_empty(),
            "an unchanged snapshot must emit no patch: {:?}",
            plan.presentation
        );
        assert_eq!(plan.adopt.len(), 1);

        // A stale address is the one effective delta, and it patches without touching lifecycle.
        let mut stale = observed.clone();
        let stale_tags = &mut stale.presentation.as_mut().unwrap().tags;
        stale_tags.insert(AGENT_ACTOR_ADDRESS_TAG.to_owned(), "dev3.worker".to_owned());
        let repaired = reconcile(
            std::slice::from_ref(&declared),
            std::slice::from_ref(&stale),
            "dev3",
        )
        .unwrap();
        assert_eq!(repaired.presentation.len(), 1);
        assert_eq!(
            repaired.presentation[0].tags[AGENT_ACTOR_ADDRESS_TAG],
            Some("dev3.fractal.keymap.verifier".to_owned())
        );
        assert!(repaired.launch.is_empty() && repaired.teardown.is_empty());
        assert_eq!(repaired.gc, Vec::<String>::new());
    }

    /// An address change is a pure cutover: task IDs, launch inputs, workspace, the declaration
    /// parent, and the adoption decision are all unmoved, and a healthy task is not restarted.
    #[test]
    fn an_address_change_moves_no_task_id_and_does_not_restart_a_healthy_task() {
        let before = migrated(vec![
            task("agent", TaskKind::Pty, Some(ID)),
            task("ding", TaskKind::Exec, None),
        ]);
        let mut after = before.clone();
        after.address = Some(AgentAddress::parse("renamed.elsewhere").unwrap());

        let sessions = [
            Session {
                pty_id: ID.to_owned(),
                alive: true,
                exit_code: None,
                presentation: None,
            },
            Session {
                pty_id: format!("{ID}.ding"),
                alive: true,
                exit_code: None,
                presentation: None,
            },
        ];

        let plan_before = reconcile(std::slice::from_ref(&before), &sessions, "dev3").unwrap();
        let plan_after = reconcile(std::slice::from_ref(&after), &sessions, "dev3").unwrap();

        assert_eq!(plan_before.live, plan_after.live);
        assert_eq!(plan_before.live, vec![ID.to_owned(), format!("{ID}.ding")]);
        assert_eq!(plan_after.launch.len(), 0, "no relaunch");
        assert_eq!(plan_after.teardown.len(), 0, "no replacement");
        assert_eq!(plan_after.gc, Vec::<String>::new());
        assert_eq!(plan_after.adopt.len(), 1, "the healthy agent is adopted");
        assert_eq!(after.path, before.path, "the state anchor is unmoved");

        // Only the projected route differs, and only in presentation.
        let selected_before = reconcile_selected(std::slice::from_ref(&before), &[], "dev3", ID)
            .unwrap();
        let selected_after =
            reconcile_selected(std::slice::from_ref(&after), &[], "dev3", ID).unwrap();
        let launch_before = &selected_before.launch[0].tasks[0];
        let launch_after = &selected_after.launch[0].tasks[0];
        assert_eq!(launch_before.pty_id, launch_after.pty_id);
        assert_eq!(launch_before.agent_id, launch_after.agent_id);
        assert_eq!(launch_before.env, launch_after.env);
        assert_eq!(launch_before.launch, launch_after.launch);
        assert_eq!(launch_before.workspace, launch_after.workspace);
        assert_eq!(launch_after.bus_address, "dev3.renamed.elsewhere");
    }

    /// A declared `ST_AGENT` that is not the runner-owned agent ID refuses before any write.
    #[test]
    fn a_declared_st_agent_that_is_not_the_agent_id_refuses() {
        let mut declared = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        declared.tasks[0]
            .env
            .insert("ST_AGENT".to_owned(), "dev3.fractal.keymap.verifier".to_owned());

        let error = validate_task_identities(std::slice::from_ref(&declared), "dev3")
            .expect_err("an address in ST_AGENT is not the runner-owned selector");
        assert_eq!(
            error,
            TaskIdentityAdmissionError::Conflict {
                agent_id: ID.to_owned(),
                task: "agent".to_owned(),
                declared: "dev3.fractal.keymap.verifier".to_owned(),
            }
        );

        declared.tasks[0]
            .env
            .insert("ST_AGENT".to_owned(), ID.to_owned());
        assert!(validate_task_identities(std::slice::from_ref(&declared), "dev3").is_ok());
    }

    /// A session tagged under schema 1 carries `agent.actor.path`, which is a ROUTE: after an
    /// address cutover those bytes can name a different subject. The first schema-2 patch must
    /// delete that owned key, leave unrelated tags alone, and then stay idempotent.
    #[test]
    fn the_first_schema_two_patch_deletes_a_stale_schema_one_actor_path() {
        let mut declared = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        declared.description = Some("Verifies keymaps.".to_owned());

        // Exactly what a schema-1 writer left behind, plus somebody else's tag.
        let mut observed_tags = BTreeMap::from([
            (AGENT_PRESENTATION_SCHEMA_TAG.to_owned(), "1".to_owned()),
            (
                LEGACY_AGENT_ACTOR_PATH_TAG.to_owned(),
                "dev3.worker".to_owned(),
            ),
            (
                AGENT_DESCRIPTION_TAG.to_owned(),
                "Verifies keymaps.".to_owned(),
            ),
            (COMPATIBILITY_ROLE_TAG.to_owned(), "agent".to_owned()),
            ("unrelated".to_owned(), "preserved".to_owned()),
        ]);
        let session = |tags: &BTreeMap<String, String>| Session {
            pty_id: ID.to_owned(),
            alive: true,
            exit_code: None,
            presentation: Some(ObservedPtyPresentation {
                display_name: None,
                tags: tags.clone(),
            }),
        };

        let plan = reconcile(
            std::slice::from_ref(&declared),
            &[session(&observed_tags)],
            "dev3",
        )
        .unwrap();
        assert_eq!(plan.presentation.len(), 1);
        let patch = &plan.presentation[0];
        assert_eq!(
            patch.tags[LEGACY_AGENT_ACTOR_PATH_TAG], None,
            "the stale schema-1 route alias must be explicitly deleted"
        );
        assert_eq!(
            patch.tags[AGENT_PRESENTATION_SCHEMA_TAG],
            Some("2".to_owned())
        );
        assert_eq!(patch.tags[AGENT_ACTOR_ID_TAG], Some(ID.to_owned()));
        assert!(
            !patch.tags.contains_key("unrelated"),
            "an unrelated tag is not ours to touch: {:?}",
            patch.tags
        );

        // Apply the patch the way `pty metadata patch` would, then reconcile again.
        for (key, value) in &patch.tags {
            match value {
                Some(value) => {
                    observed_tags.insert(key.clone(), value.clone());
                }
                None => {
                    observed_tags.remove(key);
                }
            }
        }
        assert_eq!(
            observed_tags.get("unrelated").map(String::as_str),
            Some("preserved"),
            "the unrelated tag survived the patch"
        );
        assert!(!observed_tags.contains_key(LEGACY_AGENT_ACTOR_PATH_TAG));

        let settled = reconcile(
            std::slice::from_ref(&declared),
            &[session(&observed_tags)],
            "dev3",
        )
        .unwrap();
        assert!(
            settled.presentation.is_empty(),
            "the second patch must be a no-op: {:?}",
            settled.presentation
        );
    }

    /// The supervisor edge is resolved before it is stored, in the ONE namespace the child's own
    /// migration state says its `supervisor` value is written in.
    ///
    /// Migration adds a child's `id` and rewrites its references to their parents' IDs in the same
    /// atomic transition, so an explicit-`id` child's `supervisor` is an ID and an unmigrated
    /// child's is still a positional reference. Neither namespace falls back onto the other.
    #[test]
    fn a_supervisor_reference_resolves_only_in_the_childs_own_namespace() {
        const PARENT_ID: &str = "0199b8f4-8d3a-7c21-9a44-6f85b7320aaa";
        let mut parent = spec(vec![task("agent", TaskKind::Pty, Some(PARENT_ID))]);
        parent.identity = "root".to_owned();
        parent.id = Some(AgentId::parse(PARENT_ID).unwrap());
        parent.address = Some(AgentAddress::parse("org.root").unwrap());

        let target = |specs: &[AgentSpec]| {
            let book = crate::spec::address_book(specs, "dev3").ok();
            resolve_supervisor(specs, book.as_ref(), &specs[1], "dev3")
        };

        // A MIGRATED child's reference is an exact ID, and only the ID namespace answers.
        let mut migrated_child = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        migrated_child.supervisor = Some(PARENT_ID.to_owned());
        assert_eq!(
            target(&[parent.clone(), migrated_child.clone()]),
            SupervisorTarget::Resolved(PARENT_ID.to_owned())
        );

        // The parent's ADDRESS is not an ID, so a migrated child naming it does not resolve: that
        // declaration was not rewritten by migration and must be repaired, not guessed at.
        migrated_child.supervisor = Some("org.root".to_owned());
        assert_eq!(
            target(&[parent.clone(), migrated_child.clone()]),
            SupervisorTarget::Unresolved("org.root".to_owned())
        );

        // An UNMIGRATED child's reference is a legacy POSITIONAL reference: a bare `<identity>` on
        // its own host, or a qualified `<host>.<identity>`. It names a declaration slot, never a
        // mutable route — which is what keeps a retired (non-routable) or cross-host parent
        // reachable, and its qualified form is exactly that parent's frozen legacy ID.
        let mut legacy_parent = spec(vec![task("agent", TaskKind::Pty, None)]);
        legacy_parent.identity = "root".to_owned();
        let mut legacy_child = spec(vec![task("agent", TaskKind::Pty, None)]);
        legacy_child.identity = "worker".to_owned();
        for reference in ["root", "dev3.root"] {
            legacy_child.supervisor = Some(reference.to_owned());
            assert_eq!(
                target(&[legacy_parent.clone(), legacy_child.clone()]),
                SupervisorTarget::Resolved("dev3.root".to_owned()),
                "unmigrated child reference {reference}"
            );
        }

        // A migrated parent's `address` is a route, not a positional key: an unmigrated child
        // naming it was never rewritten by migration and is reported for repair, not guessed at.
        legacy_child.supervisor = Some("org.root".to_owned());
        assert_eq!(
            target(&[parent.clone(), legacy_child.clone()]),
            SupervisorTarget::Unresolved("org.root".to_owned())
        );

        // A reference that names nothing is reported, never silently treated as "no supervisor".
        legacy_child.supervisor = Some("gone".to_owned());
        assert_eq!(
            target(&[parent.clone(), legacy_child.clone()]),
            SupervisorTarget::Unresolved("gone".to_owned())
        );

        legacy_child.supervisor = None;
        assert_eq!(
            target(&[parent, legacy_child]),
            SupervisorTarget::Undeclared
        );
    }

    /// Equal bytes in the two namespaces must never collide. An unmigrated child's `supervisor`
    /// is a positional reference, so an unrelated subject whose explicit *ID* is those same bytes
    /// cannot capture the edge from the declaration that reference actually names.
    #[test]
    fn an_unrelated_subject_whose_id_equals_the_reference_cannot_capture_the_edge() {
        // The impostor: its immutable ID is literally `boss`, and it holds no such address.
        let mut impostor = spec(vec![task("agent", TaskKind::Pty, Some("boss"))]);
        impostor.identity = "impostor".to_owned();
        impostor.id = Some(AgentId::parse("boss").unwrap());
        impostor.address = Some(AgentAddress::parse("unrelated.impostor").unwrap());

        // The real parent: unmigrated, so the bare reference `boss` qualifies to its frozen
        // positional ID `dev3.boss`.
        let mut real_parent = spec(vec![task("agent", TaskKind::Pty, None)]);
        real_parent.identity = "boss".to_owned();

        let mut child = spec(vec![task("agent", TaskKind::Pty, None)]);
        child.identity = "worker".to_owned();
        child.supervisor = Some("boss".to_owned());

        let specs = vec![impostor, real_parent, child];
        let book = crate::spec::address_book(&specs, "dev3").ok();
        assert_eq!(
            resolve_supervisor(&specs, book.as_ref(), &specs[2], "dev3"),
            SupervisorTarget::Resolved("dev3.boss".to_owned()),
            "an unmigrated child's reference is positional; the subject whose explicit ID is \
             `boss` must not be able to steal the edge"
        );
    }

    /// The plan carries that resolved target on the launch, so the park path never re-parses the
    /// authored reference.
    #[test]
    fn a_launch_carries_the_resolved_supervisor_id() {
        const PARENT_ID: &str = "0199b8f4-8d3a-7c21-9a44-6f85b7320aaa";
        let mut parent = spec(vec![task("agent", TaskKind::Pty, Some(PARENT_ID))]);
        parent.identity = "root".to_owned();
        parent.id = Some(AgentId::parse(PARENT_ID).unwrap());
        parent.address = Some(AgentAddress::parse("org.root").unwrap());
        let mut child = migrated(vec![task("agent", TaskKind::Pty, Some(ID))]);
        // The child is migrated, so migration already rewrote this reference to the parent's ID.
        child.supervisor = Some(PARENT_ID.to_owned());
        let specs = vec![parent, child];

        let plan = reconcile(&specs, &[], "dev3").unwrap();
        let launched = plan
            .launch
            .iter()
            .find(|launch| launch.spec.identity == "worker")
            .expect("the child launches");
        assert_eq!(
            launched.supervisor,
            SupervisorTarget::Resolved(PARENT_ID.to_owned())
        );

        let selected = reconcile_selected(&specs, &[], "dev3", ID).unwrap();
        assert_eq!(
            selected.launch[0].supervisor,
            SupervisorTarget::Resolved(PARENT_ID.to_owned())
        );
    }
}
