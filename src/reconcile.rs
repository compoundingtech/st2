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
use kdl::KdlValue;

use crate::identity::{IdentityActivation, LegacyReason};

/// Immutable inputs captured once before generated tasks are compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCompileContext {
    catalog_root: PathBuf,
    st2_executable: PathBuf,
    /// This pass's DELTA-003 identity-activation decision, or `None` while the caller has not
    /// decided one. Undecided compiles legacy bytes: the gate is all-or-nothing, so a caller that
    /// has not proved catalog migration must not emit target-model identity.
    identity_activation: Option<IdentityActivation>,
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
            identity_activation: None,
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

    /// Bind this pass's identity-activation decision (DELTA-003 step 5).
    pub fn with_identity_activation(mut self, activation: IdentityActivation) -> Self {
        self.identity_activation = Some(activation);
        self
    }

    /// Whether this compilation writes target-model identity.
    fn activated(&self) -> bool {
        self.identity_activation
            .as_ref()
            .is_some_and(IdentityActivation::is_activated)
    }

    /// The runner-owned agent key this compilation writes for `spec`.
    fn agent_key(&self, spec: &AgentSpec, this_host: &str) -> String {
        match &self.identity_activation {
            Some(activation) => agent_key(spec, this_host, activation),
            None => spec.bus_id(this_host),
        }
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
        let bus_id = spec.bus_id(this_host);
        let agent_key = context.agent_key(spec, this_host);
        // The shared expansion in `driver` is host-qualified because it cannot see the activation
        // gate. Both runner-owned values are captured here, before the canonical task is borrowed
        // mutably, so an activated pass can re-key them below.
        let canonical_task_id = spec
            .tasks
            .iter()
            .find(|task| !task.derived && task.name == "agent")
            .map(|task| task_id_parts(&agent_key, &bus_id, task, context.activated()));
        let expansion = crate::driver::expand_driver(spec, this_host)?;
        let argv_nodes = expansion
            .nodes()
            .iter()
            .filter(|node| node.name().value() == "argv")
            .collect::<Vec<_>>();
        let [argv_node] = argv_nodes.as_slice() else {
            anyhow::bail!(
                "agent '{bus_id}' driver expansion produced {} argv nodes; expected exactly one",
                argv_nodes.len()
            );
        };
        anyhow::ensure!(
            argv_node.children().is_none()
                && argv_node.entries().iter().all(|entry| {
                    entry.name().is_none() && matches!(entry.value(), KdlValue::String(_))
                }),
            "agent '{bus_id}' driver expansion produced a non-string argv"
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
            "agent '{bus_id}' driver expansion produced an empty argv"
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
            "agent '{bus_id}' driver expansion has an unexpected {wrapper} wrapper prefix"
        );
        argv[0] = st2_executable.clone();
        argv[2] = catalog_root.clone();

        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates
            .next()
            .with_context(|| format!("agent '{bus_id}' driver has no canonical `agent` task"))?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{bus_id}' driver has more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{bus_id}' driver canonical task is not a PTY"
        );
        if let Some(canonical_task_id) = canonical_task_id.filter(|_| context.activated()) {
            anyhow::ensure!(
                argv.get(5).map(String::as_str) == Some("--identity")
                    && argv.get(7).map(String::as_str) == Some("--runtime-id"),
                "agent '{bus_id}' driver expansion has an unexpected {wrapper} identity prefix"
            );
            argv[6] = agent_key;
            argv[8] = canonical_task_id;
        }
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
        let bus_id = spec.bus_id(this_host);
        let agent_key = context.agent_key(spec, this_host);
        let activated = context.activated();
        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates.next().with_context(|| {
            format!(
                "agent '{bus_id}' selects `deliver \"{selected}\"` but has no canonical `agent` task"
            )
        })?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{bus_id}' selects `deliver \"{selected}\"` with more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{bus_id}' selects `deliver \"{selected}\"` for a non-PTY canonical task"
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
            "agent '{bus_id}' selects `deliver \"{selected}\"` with an empty canonical argv"
        );
        let runtime_id = task_id_parts(&agent_key, &bus_id, task, activated);
        let mut argv = vec![
            st2_executable.clone(),
            "--catalog".to_string(),
            catalog_root.clone(),
            "driver".to_string(),
            wrapper.to_string(),
            "--identity".to_string(),
            agent_key,
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
        // `st2 ding --identity` is a generated exact-ID consumer, not a human address parser.
        let agent_key = context.agent_key(spec, this_host);
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
                "--identity".to_string(),
                agent_key.clone(),
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
        let bus_id = spec.bus_id(this_host);
        let agent_key = context.agent_key(spec, this_host);
        let activated = context.activated();
        let mut candidates = spec
            .tasks
            .iter_mut()
            .filter(|task| !task.derived && task.name == "agent");
        let task = candidates.next().with_context(|| {
            format!(
                "agent '{bus_id}' selects `deliver \"app-server\"` but has no canonical `agent` task"
            )
        })?;
        anyhow::ensure!(
            candidates.next().is_none(),
            "agent '{bus_id}' selects `deliver \"app-server\"` with more than one canonical `agent` task"
        );
        anyhow::ensure!(
            task.kind == TaskKind::Pty,
            "agent '{bus_id}' selects `deliver \"app-server\"` for a non-PTY canonical task"
        );
        let authored = task.argv.clone().with_context(|| {
            format!(
                "agent '{bus_id}' selects `deliver \"app-server\"`; its canonical task must use structured `argv`, not shell `command`"
            )
        })?;
        anyhow::ensure!(
            !authored.is_empty(),
            "agent '{bus_id}' selects `deliver \"app-server\"` with an empty canonical argv"
        );
        anyhow::ensure!(
            !authored
                .iter()
                .any(|arg| arg == "--remote" || arg.starts_with("--remote=")),
            "agent '{bus_id}' selects `deliver \"app-server\"` but its canonical argv already declares `--remote`"
        );
        let runtime_id = task_id_parts(&agent_key, &bus_id, task, activated);
        let mut argv = vec![
            st2_executable.clone(),
            "--catalog".to_string(),
            catalog_root.clone(),
            "codex-app-server".to_string(),
            "--identity".to_string(),
            agent_key,
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
    /// Resolved task id: the explicit authored `id`, else `<agent_key>.<name>` (R26).
    pub pty_id: String,
    /// The runner-owned agent key this task belongs to — the immutable agent ID once the identity
    /// model is activated, and the host-qualified bus identity `<host>.<identity>` while legacy.
    /// Migration froze every live subject's ID to those same bytes, so activation moves nothing.
    pub agent_key: String,
    /// The subject's human-routable bus address `<host>.<address>`. Equal to `agent_key` for a
    /// subject with no explicit `address`, which is every subject before activation.
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
/// Schema 1's owned actor tag. Schema 2 replaces it with [`AGENT_ACTOR_ID_TAG`] plus
/// [`AGENT_ACTOR_ADDRESS_TAG`] and emits it as a removal, so a PTY that started under schema 1
/// does not keep a stale owned key after the cutover.
pub const AGENT_ACTOR_PATH_TAG: &str = "agent.actor.path";
/// Schema 2: the immutable, catalog-global agent ID.
pub const AGENT_ACTOR_ID_TAG: &str = "agent.actor.id";
/// Schema 2: the current, mutable bus address.
pub const AGENT_ACTOR_ADDRESS_TAG: &str = "agent.actor.address";
pub const AGENT_DESCRIPTION_TAG: &str = "agent.presentation.description";
/// Compatibility role owned by st2 only on the canonical agent PTY.
pub const COMPATIBILITY_ROLE_TAG: &str = "role";

/// The runner-owned key one subject's identity is derived from.
///
/// Legacy: the host-qualified bus identity. Activated: the explicit immutable agent ID. Migration
/// froze every live subject's ID to its former bus identity, so the two agree byte-for-byte for a
/// migrated subject and no `ST_AGENT`, task ID, socket path, or ownership key moves at activation.
///
/// Under activation an explicit `id` is always present — proving exactly that is what the gate
/// does — so its absence is unreachable rather than a silent fallback to the legacy projection.
pub fn agent_key(spec: &AgentSpec, this_host: &str, activation: &IdentityActivation) -> String {
    match activation {
        IdentityActivation::Activated => spec.id.clone().unwrap_or_else(|| {
            unreachable!(
                "identity activation admits only fully migrated catalogs, but '{}' carries no explicit agent id",
                spec.path.display()
            )
        }),
        IdentityActivation::Legacy(_) => spec.bus_id(this_host),
    }
}

/// One task's runtime ID (R26) from values captured before the task may be borrowed mutably.
fn task_id_parts(
    agent_key: &str,
    legacy_bus_id: &str,
    task: &crate::spec::Task,
    activated: bool,
) -> String {
    // Lowering runs in the Agent Spec crate, which cannot see the activation gate, so every task ID
    // it synthesizes carries the legacy host-qualified prefix: `<bus-id>` for the compact canonical
    // agent task, `<bus-id>.<name>` for everything else it names. Under activation those two shapes
    // are re-keyed onto the agent ID, which is what R26 requires. This is byte-for-byte identical
    // for every migrated subject — its frozen ID *is* the legacy bus identity — and correct for a
    // subject created after activation. An independently authored ID stays authoritative.
    if activated && let Some(id) = task.id.as_deref() {
        if !task.derived && task.name == "agent" && id == legacy_bus_id {
            return agent_key.to_owned();
        }
        if id == format!("{legacy_bus_id}.{}", task.name) {
            return format!("{agent_key}.{}", task.name);
        }
    }
    resolve_task_id(agent_key, &task.name, task.id.as_deref())
}

/// One task's runtime ID: the explicit authored ID, else `<agent-key>.<task-name>` (R26).
///
/// Every consumer that needs a task's on-disk identity — reconciliation, socket admission, task
/// inventory, resync seats — must resolve it through here so they cannot disagree about ownership.
pub fn default_task_id(
    spec: &AgentSpec,
    task: &crate::spec::Task,
    this_host: &str,
    activation: &IdentityActivation,
) -> String {
    task_id_parts(
        &agent_key(spec, this_host, activation),
        &spec.bus_id(this_host),
        task,
        activation.is_activated(),
    )
}

/// Decide the DELTA-003 identity gate once, for a caller that already holds a complete local
/// catalog view.
///
/// Anything st2 cannot *prove* migrated answers legacy: the gate is all-or-nothing, so an
/// incomplete migration transaction or a structural archive st2 cannot observe is never an
/// optimistic yes.
pub fn identity_activation(root: &Path, specs: &[AgentSpec]) -> IdentityActivation {
    if specs.is_empty() {
        // `activation_from` answers a vacuous yes for an empty subject set. Nothing has been
        // proved migrated, and a decision that outlives its (empty) view would then key a later
        // unmigrated subject as if it had an ID, so keep the gate closed.
        return IdentityActivation::Legacy(LegacyReason::CatalogNotMigrated {
            unmigrated: 0,
            first: "no local subjects".to_owned(),
        });
    }
    if crate::catalog_migrate_ids::marker_path(root).exists() {
        return IdentityActivation::Legacy(LegacyReason::MigrationIncomplete);
    }
    match crate::catalog_archive::observe(root) {
        Ok(observation) if observation.issues.is_empty() => {
            crate::identity::activation_from(specs, &observation.archived, false)
        }
        _ => IdentityActivation::Legacy(LegacyReason::CatalogNotMigrated {
            unmigrated: 1,
            first: crate::catalog_archive::archive_root(root).display().to_string(),
        }),
    }
}

/// [`identity_activation`] for a pass that discovered the catalog itself.
///
/// An unreadable declaration may itself be the unmigrated subject, so a catalog whose discovery is
/// incomplete keeps the pass legacy.
pub fn discovered_identity_activation(
    root: &Path,
    found: &crate::Discovered,
) -> IdentityActivation {
    match found.errors.first() {
        Some(error) => IdentityActivation::Legacy(LegacyReason::CatalogNotMigrated {
            unmigrated: found.errors.len(),
            first: error.path.display().to_string(),
        }),
        None => identity_activation(root, &found.specs),
    }
}

/// Fail-closed admission errors for runner-owned task identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskIdentityAdmissionError {
    Conflict {
        agent: String,
        task: String,
        declared: String,
    },
}

impl fmt::Display for TaskIdentityAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict {
                agent,
                task,
                declared,
            } => write!(
                formatter,
                "agent '{agent}' task '{task}' declares conflicting ST_AGENT '{declared}'; expected runner-owned value '{agent}'"
            ),
        }
    }
}

impl std::error::Error for TaskIdentityAdmissionError {}

/// Reject local active tasks whose authored identity conflicts with the runner-owned agent key.
pub fn validate_task_identities(
    specs: &[AgentSpec],
    this_host: &str,
    activation: &IdentityActivation,
) -> Result<(), TaskIdentityAdmissionError> {
    for spec in specs {
        if !spec.desired_state.is_running() || spec.resolved_host(this_host) != this_host {
            continue;
        }
        validate_agent_task_identity(spec, &agent_key(spec, this_host, activation))?;
    }
    Ok(())
}

/// The same rule for one subject whose runner-owned key the caller already decided.
pub fn validate_agent_task_identity(
    spec: &AgentSpec,
    agent: &str,
) -> Result<(), TaskIdentityAdmissionError> {
    for task in &spec.tasks {
        if let Some(declared) = task.env.get("ST_AGENT")
            && declared != agent
        {
            return Err(TaskIdentityAdmissionError::Conflict {
                agent: agent.to_owned(),
                task: task.name.clone(),
                declared: declared.clone(),
            });
        }
    }
    Ok(())
}

/// Project runner-owned identity and the supervisor source of truth into one launch target.
///
/// `ST_SUPERVISOR` is passed through verbatim: the migration transaction already rewrote every
/// supervisor reference to the parent's migrated ID, so under activation the declared value *is*
/// the agent ID and re-resolving it here would reintroduce address parsing on an exact selector.
fn runner_task_env(
    spec: &AgentSpec,
    task: &crate::spec::Task,
    agent_key: &str,
) -> BTreeMap<String, String> {
    let mut env = task.env.clone();
    env.insert("ST_AGENT".to_owned(), agent_key.to_owned());
    if let Some(supervisor) = &spec.supervisor {
        env.insert("ST_SUPERVISOR".to_owned(), supervisor.clone());
    } else {
        env.remove("ST_SUPERVISOR");
    }
    env
}

fn pty_presentation(
    spec: &AgentSpec,
    task: &crate::spec::Task,
    pty_id: &str,
    agent_key: &str,
    bus_address: &str,
    activation: &IdentityActivation,
) -> Option<PtyPresentation> {
    if task.kind != TaskKind::Pty {
        return None;
    }
    let canonical_agent = task.name == "agent" && pty_id == agent_key;
    let name = || match spec.name.as_ref() {
        Some(name) if name == pty_id => None,
        _ => spec.name.clone(),
    };
    if !activation.is_activated() {
        return Some(PtyPresentation {
            pty_id: pty_id.to_owned(),
            display_name: (task.name == "agent").then(name),
            tags: BTreeMap::from([
                (
                    AGENT_PRESENTATION_SCHEMA_TAG.to_owned(),
                    Some("1".to_owned()),
                ),
                (AGENT_ACTOR_PATH_TAG.to_owned(), Some(agent_key.to_owned())),
                (AGENT_DESCRIPTION_TAG.to_owned(), spec.description.clone()),
                (
                    COMPATIBILITY_ROLE_TAG.to_owned(),
                    canonical_agent.then(|| "agent".to_owned()),
                ),
            ]),
        });
    }
    Some(PtyPresentation {
        pty_id: pty_id.to_owned(),
        // R26 restricts native display metadata to the canonical compact agent task; every other
        // PTY keeps its own task-specific display convention.
        display_name: canonical_agent.then(name),
        tags: BTreeMap::from([
            (
                AGENT_PRESENTATION_SCHEMA_TAG.to_owned(),
                Some("2".to_owned()),
            ),
            (AGENT_ACTOR_ID_TAG.to_owned(), Some(agent_key.to_owned())),
            (
                AGENT_ACTOR_ADDRESS_TAG.to_owned(),
                Some(bus_address.to_owned()),
            ),
            // Schema 1's owned actor tag is retired, not inherited: leaving it behind would keep a
            // stale host-qualified value on a PTY that outlived the cutover.
            (AGENT_ACTOR_PATH_TAG.to_owned(), None),
            (AGENT_DESCRIPTION_TAG.to_owned(), spec.description.clone()),
            (
                COMPATIBILITY_ROLE_TAG.to_owned(),
                canonical_agent.then(|| "agent".to_owned()),
            ),
        ]),
    })
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

/// Resolve one exact local task selector (`<agent-key>.<task>` or explicit task id) without
/// mutation.
pub fn resolve_task<'a>(
    specs: &'a [AgentSpec],
    selector: &str,
    this_host: &str,
    activation: &IdentityActivation,
) -> anyhow::Result<(&'a AgentSpec, &'a crate::spec::Task, String)> {
    let mut matches = Vec::new();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            continue;
        }
        let key = agent_key(spec, this_host, activation);
        for task in &spec.tasks {
            let runtime = default_task_id(spec, task, this_host, activation);
            let qualified = format!("{key}.{}", task.name);
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
    activation: &IdentityActivation,
) -> anyhow::Result<ReconcilePlan<'a>> {
    validate_task_identities(specs, this_host, activation)?;
    let (owner, task, runtime) = resolve_task(specs, selector, this_host, activation)?;
    let mut plan = ReconcilePlan::default();
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
    let key = agent_key(owner, this_host, activation);
    let bus_address = owner.bus_address(this_host);
    let env = runner_task_env(owner, task, &key);
    let target = TaskTarget {
        kind: task.kind,
        pty_id: runtime.clone(),
        agent_key: key.clone(),
        bus_address: bus_address.clone(),
        name: task.name.clone(),
        derived: task.derived,
        launch,
        cwd: task.cwd.clone(),
        workspace: owner.workspace.clone(),
        tags: task.tags.clone(),
        env,
        keep: task.keep || owner.keep,
        presentation: pty_presentation(owner, task, &runtime, &key, &bus_address, activation),
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
            });
        }
        _ => plan.launch.push(Launch {
            spec: owner,
            tasks: vec![target],
            live_derived: Vec::new(),
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

/// Resolve a task's on-disk id: the explicit `id`, else `<agent-key>.<name>`. This is the session
/// name `pty` binds a socket for, so admission checks resolve it through here rather than
/// re-deriving the format. Prefer [`default_task_id`], which also applies the activated compact
/// re-key; this is the raw shape rule for a caller that already holds the key.
pub(crate) fn resolve_task_id(agent_key: &str, name: &str, explicit: Option<&str>) -> String {
    match explicit {
        Some(id) => id.to_string(),
        None => format!("{agent_key}.{name}"),
    }
}

/// Compute the reconcile plan for `specs` given observed `sessions`, filtering to `this_host`.
///
/// `activation` is decided once per pass by the caller and is never re-derived per subject: a
/// partially migrated catalog has no coherent ID namespace, so the gate is all-or-nothing.
pub fn reconcile<'a>(
    specs: &'a [AgentSpec],
    sessions: &[Session],
    this_host: &str,
    activation: &IdentityActivation,
) -> Result<ReconcilePlan<'a>, TaskIdentityAdmissionError> {
    validate_task_identities(specs, this_host, activation)?;
    let by_id: HashMap<&str, bool> = sessions
        .iter()
        .map(|s| (s.pty_id.as_str(), s.alive))
        .collect();
    let sessions_by_id: HashMap<&str, &Session> = sessions
        .iter()
        .map(|session| (session.pty_id.as_str(), session))
        .collect();

    let mut plan = ReconcilePlan::default();
    for spec in specs {
        if spec.resolved_host(this_host) != this_host {
            plan.other_host.push(spec);
            continue;
        }
        let key = agent_key(spec, this_host, activation);
        let bus_address = spec.bus_address(this_host);

        if !spec.desired_state.is_running() {
            if spec.desired_state.is_retired() {
                plan.settle_retirement.push(spec);
            }
            let mut teardown_ids = Vec::new();
            for t in &spec.tasks {
                let id = default_task_id(spec, t, this_host, activation);
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
                let env = runner_task_env(spec, t, &key);
                let pty_id = default_task_id(spec, t, this_host, activation);
                Some((
                    TaskTarget {
                        kind: t.kind,
                        pty_id: pty_id.clone(),
                        agent_key: key.clone(),
                        bus_address: bus_address.clone(),
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
                            &key,
                            &bus_address,
                            activation,
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
            });
        }
    }
    Ok(plan)
}
