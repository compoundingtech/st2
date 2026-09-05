//! Constrained, source-preserving authoring of Agent Spec presentation and desired state.
//!
//! Presentation is declaration state, not runtime identity. Every edit holds the shared persistent
//! catalog-authoring lock, rechecks the original bytes, and atomically replaces exactly one
//! canonical KDL declaration. TOML, JSON, declarations marked Nix-owned, and callers outside the
//! supplied actor relationship fail closed. `ST_AGENT` is a trusted-fleet guardrail rather than
//! authentication. The lock serializes cooperating local st2 writers; it is not a cross-host lock.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use agent_spec::spec::{
    AGENT_DESCRIPTION_MAX_CHARS, AGENT_NAME_MAX_CHARS, Resource, StreamLaunch,
    validate_desired_state_reason, validate_presentation,
};
use kdl::{KdlDocument, KdlNode};
use serde::Serialize;

use crate::catalog_lock::CatalogLock;
use crate::run::Runner as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceVersion {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl SourceVersion {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

/// A mutable presentation field with no routing or lifecycle authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PresentationField {
    Name,
    Description,
}

impl PresentationField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Description => "description",
        }
    }

    fn max_chars(self) -> usize {
        match self {
            Self::Name => AGENT_NAME_MAX_CHARS,
            Self::Description => AGENT_DESCRIPTION_MAX_CHARS,
        }
    }
}

/// Whether a request changed declaration bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorOutcome {
    Changed,
    Unchanged,
}

/// Stable machine-readable receipt from one presentation edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PresentationReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub field: PresentationField,
    pub value: Option<String>,
    pub retired: bool,
}

/// Stable authored desired-state selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredStateValue {
    Running,
    Suspended,
    Retired,
}

impl DesiredStateValue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Retired => "retired",
        }
    }
}

/// Stable machine-readable receipt from one desired-state edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DesiredStateReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub desired_state: DesiredStateValue,
    pub reason: Option<String>,
}

/// Stable machine-readable receipt from adding one agent-owned stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamAddReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub name: String,
    pub launch: Option<StreamLaunch>,
}

/// Stable machine-readable receipt from removing one agent-owned stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamRemoveReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub name: String,
}

/// Stable machine-readable receipt from adding or updating one Resource binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceAddReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub name: String,
    pub uri: String,
    pub reason: String,
    pub inactive_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<serde_json::Value>,
}

/// Stable machine-readable receipt from removing one Resource binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceRemoveReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub name: String,
}

/// Stable machine-readable receipt from relabelling one Resource binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceRenameReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub old: String,
    pub new: String,
}

/// A classified authoring refusal. `code` is stable for machine consumers.
#[derive(Debug)]
pub struct AuthorError {
    code: &'static str,
    message: String,
}

impl AuthorError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for AuthorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AuthorError {}

#[derive(Debug)]
struct AgentTarget {
    identity: String,
    source_host: String,
    source_identity: String,
    declaration: PathBuf,
    retired: bool,
}

/// Add an agent-owned stream, or prove that the identical declaration already exists.
pub fn add_stream(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
    launch: Option<StreamLaunch>,
) -> Result<StreamAddReceipt, AuthorError> {
    author_stream(
        catalog_root,
        selector,
        this_host,
        actor,
        name,
        launch.as_ref(),
        false,
    )
    .map(|(result, identity)| StreamAddReceipt {
        result,
        identity,
        name: name.to_owned(),
        launch,
    })
}

/// Remove one agent-owned stream. An already absent stream is an idempotent success.
pub fn remove_stream(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
) -> Result<StreamRemoveReceipt, AuthorError> {
    author_stream(catalog_root, selector, this_host, actor, name, None, true).map(
        |(result, identity)| StreamRemoveReceipt {
            result,
            identity,
            name: name.to_owned(),
        },
    )
}

fn author_stream(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
    launch: Option<&StreamLaunch>,
    remove: bool,
) -> Result<(AuthorOutcome, String), AuthorError> {
    let catalog_lock = CatalogLock::exclusive(catalog_root).map_err(|error| {
        AuthorError::new(
            "catalog-lock-failed",
            format!("acquire catalog-authoring lock: {error:#}"),
        )
    })?;
    let found = crate::discover_strict(catalog_root);
    if let Some(error) = found.errors.first() {
        return Err(AuthorError::new(
            "catalog-malformed",
            format!(
                "cannot prove an exact stream target while {} is malformed: {}",
                error.path.display(),
                error.message
            ),
        ));
    }
    let target = resolve_target(&found.specs, selector, this_host)?;
    let actor = actor
        .map(|actor| resolve_target(&found.specs, actor, this_host).map(|target| target.identity))
        .transpose()?;
    authorize_actor(
        &found.specs,
        &target.identity,
        this_host,
        actor.as_deref(),
        "stream-not-authorized",
    )?;
    if remove {
        let spec = found
            .specs
            .iter()
            .find(|spec| spec.path == target.declaration)
            .ok_or_else(|| {
                AuthorError::new("stream-target-lost", "resolved stream target disappeared")
            })?;
        if spec
            .streams
            .iter()
            .find(|stream| stream.name == name)
            .is_some_and(|stream| stream.launch.is_some())
        {
            let task_name = format!("{}{}", agent_spec::STREAM_TASK_PREFIX, name);
            let task = spec
                .tasks
                .iter()
                .find(|task| task.name == task_name)
                .ok_or_else(|| {
                    AuthorError::new("stream-task-missing", "launched stream has no derived task")
                })?;
            let runtime_id = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{}.{}", spec.bus_id(this_host), task.name));
            let runner = crate::run::SystemRunner::new(
                catalog_root.to_path_buf(),
                crate::run::exec_state_dir(this_host),
            );
            let live = runner
                .list_sessions()
                .map_err(|error| {
                    AuthorError::new("stream-runtime-observation-failed", error.to_string())
                })?
                .into_iter()
                .any(|session| session.alive && session.pty_id == runtime_id);
            if live {
                runner.retire(&runtime_id).map_err(|error| {
                    AuthorError::new(
                        "stream-runtime-retirement-failed",
                        format!("retire launched stream runtime {runtime_id}: {error:#}"),
                    )
                })?;
            }
        }
    }
    let result = edit_stream_declaration(
        &catalog_lock,
        catalog_root,
        &crate::catalog_transaction::retained_dir_path(catalog_lock.control())
            .map_err(|error| AuthorError::new("declaration-write-failed", error.to_string()))?,
        &target.declaration,
        &target.identity,
        &target.source_host,
        &target.source_identity,
        name,
        launch,
        remove,
        || {},
    )?;
    Ok((result, target.identity))
}

/// Declare one Resource binding, or update the binding that already carries `name`.
///
/// st2 preserves the binding for readers; it resolves nothing and grants nothing. `uri` is the
/// exact absolute identity and is stored byte for byte with no normalization.
#[allow(clippy::too_many_arguments)]
pub fn add_resource(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
    uri: &str,
    reason: &str,
    inactive_reason: Option<&str>,
) -> Result<ResourceAddReceipt, AuthorError> {
    add_resource_with_selector(
        catalog_root,
        selector,
        this_host,
        actor,
        name,
        uri,
        reason,
        inactive_reason,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn add_resource_with_selector(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
    uri: &str,
    reason: &str,
    inactive_reason: Option<&str>,
    resource_selector: Option<&serde_json::Value>,
) -> Result<ResourceAddReceipt, AuthorError> {
    author_resource(
        catalog_root,
        selector,
        this_host,
        actor,
        ResourceIntent::Upsert {
            name,
            uri,
            reason,
            inactive_reason,
            selector: resource_selector,
        },
    )
    .map(|(result, identity)| ResourceAddReceipt {
        result,
        identity,
        name: name.to_owned(),
        uri: uri.to_owned(),
        reason: reason.to_owned(),
        inactive_reason: inactive_reason.map(str::to_owned),
        selector: resource_selector.cloned(),
    })
}

/// Remove one Resource binding. An already absent binding is an idempotent success.
pub fn remove_resource(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    name: &str,
) -> Result<ResourceRemoveReceipt, AuthorError> {
    author_resource(
        catalog_root,
        selector,
        this_host,
        actor,
        ResourceIntent::Remove { name },
    )
    .map(|(result, identity)| ResourceRemoveReceipt {
        result,
        identity,
        name: name.to_owned(),
    })
}

/// Relabel one Resource binding, carrying its `uri`, `reason`, and `inactive-reason` unchanged.
///
/// An absent `old` and an already declared `new` both refuse: binding names are unique within one
/// agent, so neither request has an outcome that preserves the caller's intent.
pub fn rename_resource(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    old: &str,
    new: &str,
) -> Result<ResourceRenameReceipt, AuthorError> {
    author_resource(
        catalog_root,
        selector,
        this_host,
        actor,
        ResourceIntent::Rename { old, new },
    )
    .map(|(result, identity)| ResourceRenameReceipt {
        result,
        identity,
        old: old.to_owned(),
        new: new.to_owned(),
    })
}

/// One requested Resource-binding mutation, resolved against the declaration under the lock.
#[derive(Debug, Clone, Copy)]
enum ResourceIntent<'a> {
    Upsert {
        name: &'a str,
        uri: &'a str,
        reason: &'a str,
        inactive_reason: Option<&'a str>,
        selector: Option<&'a serde_json::Value>,
    },
    Remove {
        name: &'a str,
    },
    Rename {
        old: &'a str,
        new: &'a str,
    },
}

/// The binding state a candidate must read back as before it may be committed.
#[derive(Debug)]
struct ResourceExpectation {
    absent: Option<String>,
    present: Option<Resource>,
}

fn author_resource(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    intent: ResourceIntent<'_>,
) -> Result<(AuthorOutcome, String), AuthorError> {
    let catalog_lock = CatalogLock::exclusive(catalog_root).map_err(|error| {
        AuthorError::new(
            "catalog-lock-failed",
            format!("acquire catalog-authoring lock: {error:#}"),
        )
    })?;
    let found = crate::discover_strict(catalog_root);
    if let Some(error) = found.errors.first() {
        return Err(AuthorError::new(
            "catalog-malformed",
            format!(
                "cannot prove an exact resource target while {} is malformed: {}",
                error.path.display(),
                error.message
            ),
        ));
    }
    let target = resolve_target(&found.specs, selector, this_host)?;
    let actor = actor
        .map(|actor| resolve_target(&found.specs, actor, this_host).map(|target| target.identity))
        .transpose()?;
    authorize_actor(
        &found.specs,
        &target.identity,
        this_host,
        actor.as_deref(),
        "resource-not-authorized",
    )?;
    let result = edit_resource_declaration(
        &catalog_lock,
        catalog_root,
        &crate::catalog_transaction::retained_dir_path(catalog_lock.control())
            .map_err(|error| AuthorError::new("declaration-write-failed", error.to_string()))?,
        &target.declaration,
        &target.identity,
        &target.source_host,
        &target.source_identity,
        intent,
        || {},
    )?;
    Ok((result, target.identity))
}

/// Author one whole-agent desired state without claiming runtime convergence.
pub fn set_desired_state(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    state: DesiredStateValue,
    reason: Option<&str>,
) -> Result<DesiredStateReceipt, AuthorError> {
    match state {
        DesiredStateValue::Running if reason.is_some() => {
            return Err(AuthorError::new(
                "invalid-desired-state",
                "running desired state forbids --reason",
            ));
        }
        DesiredStateValue::Suspended | DesiredStateValue::Retired if reason.is_none() => {
            return Err(AuthorError::new(
                "invalid-desired-state",
                format!("{} desired state requires --reason", state.as_str()),
            ));
        }
        _ => {}
    }
    if let Some(reason) = reason {
        validate_desired_state_reason(reason)
            .map_err(|error| AuthorError::new("invalid-desired-state", error.to_string()))?;
    }
    let catalog_lock = CatalogLock::exclusive(catalog_root).map_err(|error| {
        AuthorError::new(
            "catalog-lock-failed",
            format!("acquire catalog-authoring lock: {error:#}"),
        )
    })?;
    let found = crate::discover(catalog_root);
    if let Some(error) = found.errors.first() {
        return Err(AuthorError::new(
            "catalog-malformed",
            format!(
                "cannot prove an exact desired-state target while {} is malformed: {}",
                error.path.display(),
                error.message
            ),
        ));
    }
    let target = resolve_target(&found.specs, selector, this_host)?;
    authorize_actor(
        &found.specs,
        &target.identity,
        this_host,
        actor,
        "desired-state-not-authorized",
    )?;
    let result = edit_desired_state_declaration(
        &catalog_lock,
        catalog_root,
        &crate::catalog_transaction::retained_dir_path(catalog_lock.control())
            .map_err(|error| AuthorError::new("declaration-write-failed", error.to_string()))?,
        &target.declaration,
        &target.identity,
        &target.source_host,
        &target.source_identity,
        state,
        reason,
        || {},
    )?;
    Ok(DesiredStateReceipt {
        result,
        identity: target.identity,
        desired_state: state,
        reason: reason.map(str::to_owned),
    })
}

/// Set or clear one presentation field for one stable Agent Spec identity.
///
/// `actor` is the caller-supplied `ST_AGENT` identity. An absent actor is the explicit operator
/// path. Within the trusted-fleet model, the guardrail limits a catalog-managed caller to itself or
/// a descendant reached through declared supervisor edges; no presentation field expands it.
pub fn set_presentation(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    actor: Option<&str>,
    field: PresentationField,
    requested: Option<&str>,
) -> Result<PresentationReceipt, AuthorError> {
    let catalog_lock = CatalogLock::exclusive(catalog_root).map_err(|error| {
        AuthorError::new(
            "catalog-lock-failed",
            format!("acquire catalog-authoring lock: {error:#}"),
        )
    })?;
    let found = crate::discover(catalog_root);
    if let Some(error) = found.errors.first() {
        return Err(AuthorError::new(
            "catalog-malformed",
            format!(
                "cannot prove an exact presentation target while {} is malformed: {}",
                error.path.display(),
                error.message
            ),
        ));
    }
    let target = resolve_target(&found.specs, selector, this_host)?;
    authorize_actor(
        &found.specs,
        &target.identity,
        this_host,
        actor,
        "presentation-not-authorized",
    )?;
    let requested = requested
        .map(|value| {
            validate_presentation(field.as_str(), Some(value), field.max_chars())
                .map(|()| value.to_owned())
                .map_err(|error| AuthorError::new("invalid-presentation", error.to_string()))
        })
        .transpose()?;
    let result = edit_declaration(
        &catalog_lock,
        catalog_root,
        &crate::catalog_transaction::retained_dir_path(catalog_lock.control())
            .map_err(|error| AuthorError::new("declaration-write-failed", error.to_string()))?,
        &target.declaration,
        &target.identity,
        &target.source_host,
        &target.source_identity,
        field,
        requested.as_deref(),
        || {},
    )?;
    Ok(PresentationReceipt {
        result,
        identity: target.identity,
        field,
        value: requested,
        retired: target.retired,
    })
}

fn resolve_target(
    specs: &[crate::AgentSpec],
    selector: &str,
    this_host: &str,
) -> Result<AgentTarget, AuthorError> {
    let exact = specs
        .iter()
        .filter(|spec| spec.bus_id(this_host) == selector)
        .collect::<Vec<_>>();
    let matches = if exact.is_empty() {
        specs
            .iter()
            .filter(|spec| spec.identity == selector)
            .collect::<Vec<_>>()
    } else {
        exact
    };
    match matches.as_slice() {
        [] => Err(AuthorError::new(
            "target-not-found",
            format!("no agent {selector:?} found in the selected catalog"),
        )),
        [spec] => Ok(AgentTarget {
            identity: spec.bus_id(this_host),
            source_host: spec.resolved_host(this_host).to_owned(),
            source_identity: spec.identity.clone(),
            declaration: spec.path.clone(),
            retired: spec.desired_state.is_retired(),
        }),
        many => {
            let mut candidates = many
                .iter()
                .map(|spec| format!("{} ({})", spec.bus_id(this_host), spec.path.display()))
                .collect::<Vec<_>>();
            candidates.sort();
            Err(AuthorError::new(
                "target-ambiguous",
                format!(
                    "agent selector {selector:?} is ambiguous: {}",
                    candidates.join(", ")
                ),
            ))
        }
    }
}

fn authorize_actor(
    specs: &[crate::AgentSpec],
    target: &str,
    this_host: &str,
    actor: Option<&str>,
    refusal_code: &'static str,
) -> Result<(), AuthorError> {
    let Some(actor) = actor else {
        return Ok(());
    };
    if actor == target {
        return Ok(());
    }
    let by_identity = specs
        .iter()
        .map(|spec| (spec.bus_id(this_host), spec))
        .collect::<BTreeMap<_, _>>();
    let mut current = target.to_owned();
    let mut visited = BTreeSet::new();
    while visited.insert(current.clone()) {
        let Some(spec) = by_identity.get(&current) else {
            break;
        };
        let Some(supervisor) = spec.supervisor.as_deref() else {
            break;
        };
        if supervisor == actor {
            return Ok(());
        }
        let same_host = format!("{}.{}", spec.resolved_host(this_host), supervisor);
        let qualified = if by_identity.contains_key(supervisor) {
            supervisor.to_owned()
        } else if by_identity.contains_key(&same_host) {
            same_host
        } else {
            supervisor.to_owned()
        };
        if qualified == actor {
            return Ok(());
        }
        current = qualified;
    }
    Err(AuthorError::new(
        refusal_code,
        format!("agent {actor:?} may edit only itself or a declared descendant, not {target:?}"),
    ))
}

#[cfg(test)]
fn edit_declaration_for_test(
    path: &Path,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    field: PresentationField,
    requested: Option<&str>,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    let control = path
        .parent()
        .expect("test declaration has a parent")
        .join(crate::catalog_lock::CONTROL_DIR);
    fs::create_dir_all(&control).expect("create test catalog control directory");
    let catalog_lock = CatalogLock::exclusive(path.parent().expect("test catalog has a parent"))
        .expect("acquire test catalog lock");
    edit_declaration(
        &catalog_lock,
        path.parent().expect("test catalog has a parent"),
        &control,
        path,
        expected_identity,
        expected_host,
        expected_agent,
        field,
        requested,
        before_commit,
    )
}

#[cfg(test)]
fn edit_desired_state_for_test(
    path: &Path,
    state: DesiredStateValue,
    reason: Option<&str>,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    let control = path
        .parent()
        .expect("test declaration has a parent")
        .join(crate::catalog_lock::CONTROL_DIR);
    fs::create_dir_all(&control).expect("create test catalog control directory");
    let catalog_lock = CatalogLock::exclusive(path.parent().expect("test catalog has a parent"))
        .expect("acquire test catalog lock");
    edit_desired_state_declaration(
        &catalog_lock,
        path.parent().expect("test catalog has a parent"),
        &control,
        path,
        "h.worker",
        "h",
        "worker",
        state,
        reason,
        before_commit,
    )
}

#[allow(clippy::too_many_arguments)]
fn edit_stream_declaration(
    catalog_lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    name: &str,
    launch: Option<&StreamLaunch>,
    remove: bool,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
        return Err(AuthorError::new(
            "unsupported-declaration-format",
            format!(
                "stream authoring requires canonical KDL, found {}",
                path.display()
            ),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    let original_version = SourceVersion::from_metadata(&metadata);
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("declaration {} is not UTF-8: {error}", path.display()),
        )
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("parsing declaration {}: {error}", path.display()),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    if is_nix_managed(target) {
        return Err(AuthorError::new(
            "nix-managed-declaration",
            format!(
                "agent {expected_identity:?} is Nix-owned; edit its Nix source instead of {}",
                path.display()
            ),
        ));
    }
    let replacement = stream_edit(text, target, name, launch, remove)?;
    let Some(replacement) = replacement else {
        return Ok(AuthorOutcome::Unchanged);
    };
    verify_stream_candidate(
        catalog,
        path,
        &replacement,
        expected_identity,
        expected_host,
        expected_agent,
        name,
        launch,
        remove,
    )?;
    atomic_replace_checked(
        catalog_lock,
        catalog,
        control,
        path,
        &original,
        original_version,
        replacement.as_bytes(),
        metadata.permissions().mode() & 0o7777,
        before_commit,
    )?;
    Ok(AuthorOutcome::Changed)
}

fn stream_edit(
    text: &str,
    target: &KdlNode,
    name: &str,
    launch: Option<&StreamLaunch>,
    remove: bool,
) -> Result<Option<String>, AuthorError> {
    let streams = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| {
            child.name().value() == "stream"
                && child.get(0).and_then(|entry| entry.as_string()) == Some(name)
        })
        .collect::<Vec<_>>();
    if streams.len() > 1 {
        return Err(AuthorError::new(
            "duplicate-stream",
            format!("target declares stream {name:?} more than once"),
        ));
    }
    if remove {
        return streams
            .first()
            .map(|node| remove_field(text, node).map(Some))
            .unwrap_or(Ok(None));
    }
    if let Some(existing) = streams.first() {
        if parsed_stream_launch(existing)? == launch.cloned() {
            return Ok(None);
        }
        return Err(AuthorError::new(
            "stream-already-exists",
            format!(
                "stream {name:?} already exists with a different launch; remove it before adding a replacement"
            ),
        ));
    }
    let authored = match launch {
        None => format!("stream {} {{}}", quoted(name)?),
        Some(StreamLaunch::Command(command)) => format!(
            "stream {} {{ command {} }}",
            quoted(name)?,
            quoted(command)?
        ),
        Some(StreamLaunch::Argv(argv)) => {
            let values = argv
                .iter()
                .map(|value| quoted(value))
                .collect::<Result<Vec<_>, _>>()?;
            format!("stream {} {{ argv {} }}", quoted(name)?, values.join(" "))
        }
    };
    insert_node(text, target, &authored).map(Some)
}

fn parsed_stream_launch(node: &KdlNode) -> Result<Option<StreamLaunch>, AuthorError> {
    let children = node
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .collect::<Vec<_>>();
    match children.as_slice() {
        [] => Ok(None),
        [child] if child.name().value() == "command" => child
            .get(0)
            .and_then(|entry| entry.as_string())
            .map(|value| Some(StreamLaunch::Command(value.to_owned())))
            .ok_or_else(|| {
                AuthorError::new("malformed-stream", "stream command must contain one string")
            }),
        [child] if child.name().value() == "argv" => {
            let argv = child
                .entries()
                .iter()
                .map(|entry| entry.value().as_string().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    AuthorError::new("malformed-stream", "stream argv values must be strings")
                })?;
            Ok(Some(StreamLaunch::Argv(argv)))
        }
        _ => Err(AuthorError::new(
            "malformed-stream",
            "stream must contain exactly one command or argv node, or be empty",
        )),
    }
}

fn verify_stream_candidate(
    catalog: &Path,
    path: &Path,
    candidate: &str,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    name: &str,
    launch: Option<&StreamLaunch>,
    removed: bool,
) -> Result<(), AuthorError> {
    let temporary = tempfile::tempdir()
        .map_err(|error| AuthorError::new("unsafe-source-edit", error.to_string()))?;
    let relative = path.strip_prefix(catalog).map_err(|_| {
        AuthorError::new(
            "unsafe-declaration-path",
            format!(
                "declaration {} is outside catalog {}",
                path.display(),
                catalog.display()
            ),
        )
    })?;
    let candidate_path = temporary.path().join(relative);
    fs::create_dir_all(
        candidate_path
            .parent()
            .expect("candidate declaration has a parent"),
    )
    .and_then(|()| fs::write(&candidate_path, candidate))
    .map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("stage stream validation: {error}"),
        )
    })?;
    let (specs, _) = agent_spec::discover_file(temporary.path(), &candidate_path)
        .map_err(|error| AuthorError::new("invalid-stream", error.to_string()))?;
    let spec = specs
        .iter()
        .find(|spec| {
            spec.identity == expected_agent && spec.bus_id(expected_host) == expected_identity
        })
        .ok_or_else(|| {
            AuthorError::new(
                "unsafe-source-edit",
                "stream candidate lost the authored agent",
            )
        })?;
    let observed = spec.streams.iter().find(|stream| stream.name == name);
    if removed && observed.is_none()
        || !removed && observed.is_some_and(|stream| stream.launch.as_ref() == launch)
    {
        Ok(())
    } else {
        Err(AuthorError::new(
            "unsafe-source-edit",
            "stream candidate did not read back as the authored intent",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn edit_resource_declaration(
    catalog_lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    intent: ResourceIntent<'_>,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
        return Err(AuthorError::new(
            "unsupported-declaration-format",
            format!(
                "resource authoring requires canonical KDL, found {}",
                path.display()
            ),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    let original_version = SourceVersion::from_metadata(&metadata);
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("declaration {} is not UTF-8: {error}", path.display()),
        )
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("parsing declaration {}: {error}", path.display()),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    if is_nix_managed(target) {
        return Err(AuthorError::new(
            "nix-managed-declaration",
            format!(
                "agent {expected_identity:?} is Nix-owned; edit its Nix source instead of {}",
                path.display()
            ),
        ));
    }
    let Some((replacement, expectation)) = resource_edit(text, target, intent)? else {
        return Ok(AuthorOutcome::Unchanged);
    };
    verify_resource_candidate(
        catalog,
        path,
        &replacement,
        expected_identity,
        expected_host,
        expected_agent,
        &expectation,
    )?;
    atomic_replace_checked(
        catalog_lock,
        catalog,
        control,
        path,
        &original,
        original_version,
        replacement.as_bytes(),
        metadata.permissions().mode() & 0o7777,
        before_commit,
    )?;
    Ok(AuthorOutcome::Changed)
}

/// Resolve one intent against the declared bindings, preserving every unrelated byte.
///
/// `Ok(None)` is the proven no-op: an unchanged upsert, an absent removal, or a self-rename. A
/// changed upsert rewrites exactly the one binding node in place, so its position, its leading
/// trivia, and every sibling binding survive.
fn resource_edit(
    text: &str,
    target: &KdlNode,
    intent: ResourceIntent<'_>,
) -> Result<Option<(String, ResourceExpectation)>, AuthorError> {
    let declared = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| child.name().value() == "resource")
        .collect::<Vec<_>>();
    let declaring = |name: &str| -> Result<Option<&KdlNode>, AuthorError> {
        let matches = declared
            .iter()
            .copied()
            .filter(|child| child.get(0).and_then(|entry| entry.as_string()) == Some(name))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(AuthorError::new(
                "duplicate-resource",
                format!("target declares resource {name:?} more than once"),
            ));
        }
        Ok(matches.first().copied())
    };
    match intent {
        ResourceIntent::Upsert {
            name,
            uri,
            reason,
            inactive_reason,
            selector,
        } => {
            let authored = declared_resource(name, uri, reason, inactive_reason, selector)?;
            let replacement = match declaring(name)? {
                Some(node) if parsed_resource(node)? == authored => return Ok(None),
                Some(node) => replace_node(text, node, &render_resource(&authored)?)?,
                None => insert_node(text, target, &render_resource(&authored)?)?,
            };
            Ok(Some((
                replacement,
                ResourceExpectation {
                    absent: None,
                    present: Some(authored),
                },
            )))
        }
        ResourceIntent::Remove { name } => {
            let Some(node) = declaring(name)? else {
                return Ok(None);
            };
            Ok(Some((
                remove_field(text, node)?,
                ResourceExpectation {
                    absent: Some(name.to_owned()),
                    present: None,
                },
            )))
        }
        ResourceIntent::Rename { old, new } => {
            let Some(node) = declaring(old)? else {
                return Err(AuthorError::new(
                    "resource-not-found",
                    format!("target declares no resource {old:?}"),
                ));
            };
            if old == new {
                return Ok(None);
            }
            if declaring(new)?.is_some() {
                return Err(AuthorError::new(
                    "resource-already-exists",
                    format!(
                        "target already declares resource {new:?}; binding names are unique within one agent"
                    ),
                ));
            }
            let carried = parsed_resource(node)?;
            let renamed = declared_resource(
                new,
                carried.uri(),
                carried.reason(),
                carried.inactive_reason(),
                carried.selector(),
            )?;
            Ok(Some((
                replace_node(text, node, &render_resource(&renamed)?)?,
                ResourceExpectation {
                    absent: Some(old.to_owned()),
                    present: Some(renamed),
                },
            )))
        }
    }
}

/// Enforce the canonical binding invariants — `agent_spec` owns them; this mints no new rule.
fn declared_resource(
    name: &str,
    uri: &str,
    reason: &str,
    inactive_reason: Option<&str>,
    selector: Option<&serde_json::Value>,
) -> Result<Resource, AuthorError> {
    let resource = match inactive_reason {
        None => Resource::new(name.to_owned(), uri.to_owned(), reason.to_owned()),
        Some(inactive_reason) => Resource::new_inactive(
            name.to_owned(),
            uri.to_owned(),
            reason.to_owned(),
            inactive_reason.to_owned(),
        ),
    }
    .map_err(|error| AuthorError::new("invalid-resource", error))?;
    Ok(match selector {
        Some(selector) => resource.with_selector(selector.clone()),
        None => resource,
    })
}

fn parsed_resource(node: &KdlNode) -> Result<Resource, AuthorError> {
    let malformed =
        |detail: &str| AuthorError::new("malformed-resource", format!("resource binding {detail}"));
    if node.children().is_some() {
        return Err(malformed("cannot have children"));
    }
    let mut name = None;
    let mut uri = None;
    let mut reason = None;
    let mut inactive_reason = None;
    let mut selector = None;
    for entry in node.entries() {
        let value = entry
            .value()
            .as_string()
            .ok_or_else(|| malformed("accepts only string values"))?;
        match entry.name().map(|name| name.value()) {
            None => {
                if name.replace(value).is_some() {
                    return Err(malformed("declares one of its fields more than once"));
                }
            }
            Some("uri") => {
                if uri.replace(value).is_some() {
                    return Err(malformed("declares one of its fields more than once"));
                }
            }
            Some("reason") => {
                if reason.replace(value).is_some() {
                    return Err(malformed("declares one of its fields more than once"));
                }
            }
            Some("inactive-reason") => {
                if inactive_reason.replace(value).is_some() {
                    return Err(malformed("declares one of its fields more than once"));
                }
            }
            Some("selector") => {
                if selector.is_some() {
                    return Err(malformed("declares one of its fields more than once"));
                }
                selector = Some(serde_json::from_str(value).map_err(|error| {
                    malformed(&format!("has invalid JSON `selector`: {error}"))
                })?);
            }
            Some(other) => return Err(malformed(&format!("has unsupported property `{other}`"))),
        }
    }
    let (Some(name), Some(uri), Some(reason)) = (name, uri, reason) else {
        return Err(malformed("needs a name, a `uri`, and a `reason`"));
    };
    declared_resource(name, uri, reason, inactive_reason, selector.as_ref())
}

fn render_resource(resource: &Resource) -> Result<String, AuthorError> {
    let mut authored = format!(
        "resource {} uri={} reason={}",
        quoted(resource.name())?,
        quoted(resource.uri())?,
        quoted(resource.reason())?
    );
    if let Some(inactive_reason) = resource.inactive_reason() {
        authored.push_str(&format!(" inactive-reason={}", quoted(inactive_reason)?));
    }
    if let Some(selector) = resource.selector() {
        authored.push_str(" selector=");
        authored.push_str(&raw_json(selector)?);
    }
    Ok(authored)
}

fn raw_json(value: &serde_json::Value) -> Result<String, AuthorError> {
    let json = serde_json::to_string(value).map_err(|error| {
        AuthorError::new(
            "invalid-resource",
            format!("serialize Resource selector as canonical JSON: {error}"),
        )
    })?;
    for hashes in 1..=json.len() + 1 {
        let fence = "#".repeat(hashes);
        if !json.contains(&format!("\"{fence}")) {
            return Ok(format!("{fence}\"{json}\"{fence}"));
        }
    }
    unreachable!("a delimiter longer than the JSON payload cannot occur in the payload")
}

/// Replace exactly one node's source span. A KDL node span carries neither the leading trivia nor
/// the trailing terminator, so the surrounding line survives untouched.
fn replace_node(text: &str, node: &KdlNode, authored: &str) -> Result<String, AuthorError> {
    let span = node.span();
    let range = span.offset()..span.offset() + span.len();
    text.get(range.clone()).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "resource binding span falls outside the declaration",
        )
    })?;
    // The span can run to the start of trailing trivia, so replacing it verbatim would glue the
    // rendered node onto a following `// comment`. Leave that separator in the source.
    let kept = text[range.clone()].trim_end_matches([' ', '\t']).len();
    let mut replacement = text.to_owned();
    replacement.replace_range(range.start..range.start + kept, authored);
    Ok(replacement)
}

fn verify_resource_candidate(
    catalog: &Path,
    path: &Path,
    candidate: &str,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    expectation: &ResourceExpectation,
) -> Result<(), AuthorError> {
    let temporary = tempfile::tempdir()
        .map_err(|error| AuthorError::new("unsafe-source-edit", error.to_string()))?;
    let relative = path.strip_prefix(catalog).map_err(|_| {
        AuthorError::new(
            "unsafe-declaration-path",
            format!(
                "declaration {} is outside catalog {}",
                path.display(),
                catalog.display()
            ),
        )
    })?;
    let candidate_path = temporary.path().join(relative);
    fs::create_dir_all(
        candidate_path
            .parent()
            .expect("candidate declaration has a parent"),
    )
    .and_then(|()| fs::write(&candidate_path, candidate))
    .map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("stage resource validation: {error}"),
        )
    })?;
    let (specs, _) = agent_spec::discover_file(temporary.path(), &candidate_path)
        .map_err(|error| AuthorError::new("invalid-resource", error.to_string()))?;
    let spec = specs
        .iter()
        .find(|spec| {
            spec.identity == expected_agent && spec.bus_id(expected_host) == expected_identity
        })
        .ok_or_else(|| {
            AuthorError::new(
                "unsafe-source-edit",
                "resource candidate lost the authored agent",
            )
        })?;
    let declares = |name: &str| {
        spec.resources
            .iter()
            .find(|resource| resource.name() == name)
    };
    if expectation
        .absent
        .as_deref()
        .is_some_and(|name| declares(name).is_some())
        || expectation
            .present
            .as_ref()
            .is_some_and(|expected| declares(expected.name()) != Some(expected))
    {
        return Err(AuthorError::new(
            "unsafe-source-edit",
            "resource candidate did not read back as the authored intent",
        ));
    }
    Ok(())
}

fn edit_declaration(
    catalog_lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    field: PresentationField,
    requested: Option<&str>,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
        return Err(AuthorError::new(
            "unsupported-declaration-format",
            format!(
                "presentation authoring requires canonical KDL, found {}",
                path.display()
            ),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    let original_version = SourceVersion::from_metadata(&metadata);
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("declaration {} is not UTF-8: {error}", path.display()),
        )
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("parsing declaration {}: {error}", path.display()),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    if is_nix_managed(target) {
        return Err(AuthorError::new(
            "nix-managed-declaration",
            format!(
                "agent {expected_identity:?} is Nix-owned; edit its Nix source instead of {}",
                path.display()
            ),
        ));
    }
    let Some(replacement) = presentation_edit(text, target, field, requested)? else {
        return Ok(AuthorOutcome::Unchanged);
    };
    verify_candidate(
        &replacement,
        expected_identity,
        expected_host,
        expected_agent,
        field,
        requested,
    )?;
    atomic_replace_checked(
        catalog_lock,
        catalog,
        control,
        path,
        &original,
        original_version,
        replacement.as_bytes(),
        metadata.permissions().mode() & 0o7777,
        before_commit,
    )?;
    Ok(AuthorOutcome::Changed)
}

#[allow(clippy::too_many_arguments)]
fn edit_desired_state_declaration(
    catalog_lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    state: DesiredStateValue,
    reason: Option<&str>,
    before_commit: impl FnOnce(),
) -> Result<AuthorOutcome, AuthorError> {
    if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
        return Err(AuthorError::new(
            "unsupported-declaration-format",
            format!(
                "desired-state authoring requires canonical KDL, found {}",
                path.display()
            ),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    let original_version = SourceVersion::from_metadata(&metadata);
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("declaration {} is not UTF-8: {error}", path.display()),
        )
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("parsing declaration {}: {error}", path.display()),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    if is_nix_managed(target) {
        return Err(AuthorError::new(
            "nix-managed-declaration",
            format!(
                "agent {expected_identity:?} is Nix-owned; edit its Nix source instead of {}",
                path.display()
            ),
        ));
    }
    let Some(replacement) = desired_state_edit(text, target, state, reason)? else {
        return Ok(AuthorOutcome::Unchanged);
    };
    verify_desired_state_candidate(
        &replacement,
        expected_identity,
        expected_host,
        expected_agent,
        state,
        reason,
    )?;
    atomic_replace_checked(
        catalog_lock,
        catalog,
        control,
        path,
        &original,
        original_version,
        replacement.as_bytes(),
        metadata.permissions().mode() & 0o7777,
        before_commit,
    )?;
    Ok(AuthorOutcome::Changed)
}

fn desired_state_edit(
    text: &str,
    target: &KdlNode,
    state: DesiredStateValue,
    reason: Option<&str>,
) -> Result<Option<String>, AuthorError> {
    let lifecycle = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| matches!(child.name().value(), "desired-state" | "retired"))
        .collect::<Vec<_>>();
    if lifecycle.len() > 1 {
        return Err(AuthorError::new(
            "duplicate-lifecycle-field",
            "target declares more than one lifecycle field",
        ));
    }
    if state == DesiredStateValue::Running {
        return lifecycle
            .first()
            .map(|node| remove_field(text, node).map(Some))
            .unwrap_or(Ok(None));
    }
    let authored = format!(
        "desired-state {} reason={}",
        quoted(state.as_str())?,
        quoted(reason.expect("validated by set_desired_state"))?
    );
    match lifecycle.as_slice() {
        [] => insert_node(text, target, &authored).map(Some),
        [node] => {
            let span = node.span();
            let range = span.offset()..span.offset() + span.len();
            if text.get(range.clone()) == Some(authored.as_str()) {
                return Ok(None);
            }
            let mut replacement = text.to_owned();
            replacement.replace_range(range, &authored);
            Ok(Some(replacement))
        }
        _ => unreachable!(),
    }
}

fn verify_desired_state_candidate(
    candidate: &str,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    state: DesiredStateValue,
    reason: Option<&str>,
) -> Result<(), AuthorError> {
    let document = KdlDocument::parse(candidate).map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("desired-state edit did not produce valid KDL: {error}"),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    let lifecycle = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| matches!(child.name().value(), "desired-state" | "retired"))
        .collect::<Vec<_>>();
    if state == DesiredStateValue::Running {
        if lifecycle.is_empty() {
            return Ok(());
        }
    } else if let [node] = lifecycle.as_slice()
        && node.name().value() == "desired-state"
        && node.get(0).and_then(|entry| entry.as_string()) == Some(state.as_str())
        && node.get("reason").and_then(|entry| entry.as_string()) == reason
    {
        return Ok(());
    }
    Err(AuthorError::new(
        "unsafe-source-edit",
        "desired-state candidate did not read back as the authored intent",
    ))
}

pub(crate) fn exact_agent_node<'a>(
    document: &'a KdlDocument,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
) -> Result<&'a KdlNode, AuthorError> {
    let agents = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "agent")
        .collect::<Vec<_>>();
    let explicit = agents
        .iter()
        .copied()
        .filter(|node| {
            let (host, identity) = agent_identity_parts(node);
            identity.as_deref() == Some(expected_agent)
                && host.as_deref().is_none_or(|host| host == expected_host)
        })
        .collect::<Vec<_>>();
    let matches = if explicit.is_empty() {
        agents
            .into_iter()
            .filter(|node| {
                let (host, identity) = agent_identity_parts(node);
                identity.is_none() && host.as_deref().is_none_or(|host| host == expected_host)
            })
            .collect::<Vec<_>>()
    } else {
        explicit
    };
    match matches.as_slice() {
        [target] => Ok(*target),
        [] => Err(AuthorError::new(
            "target-changed",
            format!("declaration no longer contains agent {expected_identity:?}"),
        )),
        _ => Err(AuthorError::new(
            "target-ambiguous",
            format!("declaration contains more than one agent {expected_identity:?}"),
        )),
    }
}

pub(crate) fn agent_identity_parts(node: &KdlNode) -> (Option<String>, Option<String>) {
    let mut identity = node
        .get(0)
        .and_then(|value| value.as_string())
        .map(str::to_owned);
    let mut host = None;
    if let Some(children) = node.children() {
        for child in children.nodes() {
            match child.name().value() {
                "identity" => {
                    identity = child
                        .get(0)
                        .and_then(|value| value.as_string())
                        .map(str::to_owned)
                        .or(identity);
                }
                "host" => {
                    host = child
                        .get(0)
                        .and_then(|value| value.as_string())
                        .map(str::to_owned);
                }
                _ => {}
            }
        }
    }
    (host, identity)
}

pub(crate) fn is_nix_managed(node: &KdlNode) -> bool {
    node.children().is_some_and(|children| {
        children
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "meta")
            .filter_map(KdlNode::children)
            .flat_map(|meta| meta.nodes())
            .filter(|child| child.name().value() == "managed-by")
            .any(|child| child.get(0).and_then(|value| value.as_string()) == Some("nix"))
    })
}

fn presentation_edit(
    text: &str,
    target: &KdlNode,
    field: PresentationField,
    requested: Option<&str>,
) -> Result<Option<String>, AuthorError> {
    let fields = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| child.name().value() == field.as_str())
        .collect::<Vec<_>>();
    match fields.as_slice() {
        [] => match requested {
            Some(value) => insert_field(text, target, field, value).map(Some),
            None => Ok(None),
        },
        [node] => match requested {
            Some(value) => replace_field(text, node, field, value),
            None => remove_field(text, node).map(Some),
        },
        _ => Err(AuthorError::new(
            "duplicate-presentation-field",
            format!("target declares `{}` more than once", field.as_str()),
        )),
    }
}

fn parse_field_value(node: &KdlNode, field: PresentationField) -> Result<&str, AuthorError> {
    if node.children().is_some() || node.entries().len() != 1 || node.entries()[0].name().is_some()
    {
        return Err(AuthorError::new(
            "malformed-presentation-field",
            format!(
                "`{}` must contain exactly one positional string",
                field.as_str()
            ),
        ));
    }
    node.get(0)
        .and_then(|value| value.as_string())
        .ok_or_else(|| {
            AuthorError::new(
                "malformed-presentation-field",
                format!("`{}` must contain a string", field.as_str()),
            )
        })
}

pub(crate) fn quoted(value: &str) -> Result<String, AuthorError> {
    serde_json::to_string(value).map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("encode presentation string for canonical KDL: {error}"),
        )
    })
}

fn replace_field(
    text: &str,
    node: &KdlNode,
    field: PresentationField,
    value: &str,
) -> Result<Option<String>, AuthorError> {
    if parse_field_value(node, field)? == value {
        return Ok(None);
    }
    let entry = &node.entries()[0];
    let span = entry.span();
    let range = span.offset()..span.offset() + span.len();
    text.get(range.clone()).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "presentation value span falls outside the declaration",
        )
    })?;
    let mut replacement = text.to_owned();
    replacement.replace_range(range, &quoted(value)?);
    Ok(Some(replacement))
}

fn insert_field(
    text: &str,
    target: &KdlNode,
    field: PresentationField,
    value: &str,
) -> Result<String, AuthorError> {
    insert_node(
        text,
        target,
        &format!("{} {}", field.as_str(), quoted(value)?),
    )
}

pub(crate) fn insert_node(text: &str, target: &KdlNode, authored: &str) -> Result<String, AuthorError> {
    let span = target.span();
    let start = span.offset();
    let end = start + span.len();
    let source = text.get(start..end).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "agent span falls outside the declaration",
        )
    })?;
    let mut replacement = text.to_owned();
    if target.children().is_none() {
        replacement.insert_str(end, &format!(" {{ {authored} }}"));
        return Ok(replacement);
    }
    if !source.ends_with('}') {
        return Err(AuthorError::new(
            "unsafe-source-shape",
            "agent child block does not end at a source-preserving insertion point",
        ));
    }
    let close = source.len() - 1;
    if let Some(newline) = source[..close].rfind('\n') {
        let closing_indent = &source[newline + 1..close];
        if !closing_indent
            .chars()
            .all(|value| matches!(value, ' ' | '\t'))
        {
            return Err(AuthorError::new(
                "unsafe-source-shape",
                "cannot preserve a non-whitespace closing-brace prefix",
            ));
        }
        let child_indent = target
            .children()
            .and_then(|children| children.nodes().first())
            .and_then(|child| line_indent(text, child.span().offset()))
            .unwrap_or_else(|| format!("{closing_indent}  "));
        replacement.insert_str(start + newline + 1, &format!("{child_indent}{authored}\n"));
        return Ok(replacement);
    }
    let before_close = &source[..close];
    let trimmed = before_close.trim_end();
    let insertion = if trimmed.ends_with('{') {
        format!(" {authored}")
    } else if trimmed.ends_with(';') {
        format!(" {authored};")
    } else {
        format!("; {authored}")
    };
    replacement.insert_str(start + trimmed.len(), &insertion);
    Ok(replacement)
}

/// Whether the remainder of a node's own line is removable trivia: blanks, optionally followed by
/// a `//` line comment. A hand-authored `resource "work" uri="…" reason="…" // why` owns that
/// comment, so deleting the binding deletes its explanation with it. `/*` is deliberately not
/// accepted — a block comment can span lines, and this only ever sees one.
fn is_line_tail_trivia(tail: &str) -> bool {
    let rest = tail.trim_start_matches([' ', '\t', '\r']);
    rest.is_empty() || rest.starts_with("//")
}

fn remove_field(text: &str, node: &KdlNode) -> Result<String, AuthorError> {
    let span = node.span();
    let start = span.offset();
    let end = start + span.len();
    text.get(start..end).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "presentation field span falls outside the declaration",
        )
    })?;
    let line_start = text[..start].rfind('\n').map_or(0, |newline| newline + 1);
    let line_end = text[end..]
        .find('\n')
        .map_or(text.len(), |newline| end + newline);
    if text[line_start..start]
        .chars()
        .all(|value| matches!(value, ' ' | '\t'))
        && is_line_tail_trivia(&text[end..line_end])
    {
        let mut replacement = text.to_owned();
        let remove_end = usize::min(line_end + usize::from(line_end < text.len()), text.len());
        replacement.replace_range(line_start..remove_end, "");
        return Ok(replacement);
    }

    // A KDL node's span excludes leading trivia. Preserve an admitted inline
    // block comment by leaving that trivia on its line while removing only the
    // lifecycle declaration and the whitespace around it. Candidate parsing
    // below remains the final guard against accepting some other unsafe prefix.
    if text[end..line_end]
        .chars()
        .all(|value| matches!(value, ' ' | '\t' | '\r'))
    {
        let before = &text[line_start..start];
        let remove_start = line_start + before.trim_end_matches([' ', '\t']).len();
        let mut replacement = text.to_owned();
        replacement.replace_range(remove_start..line_end, "");
        return Ok(replacement);
    }

    let after = &text[end..line_end];
    let after_indent = after.len() - after.trim_start_matches([' ', '\t']).len();
    let after_content = end + after_indent;
    if text[after_content..line_end].starts_with(';') {
        let mut remove_end = after_content + 1;
        while remove_end < line_end
            && text.as_bytes()[remove_end].is_ascii_whitespace()
            && text.as_bytes()[remove_end] != b'\n'
            && text.as_bytes()[remove_end] != b'\r'
        {
            remove_end += 1;
        }
        let mut replacement = text.to_owned();
        replacement.replace_range(start..remove_end, "");
        return Ok(replacement);
    }

    let before = &text[line_start..start];
    let before_content = line_start + before.trim_end_matches([' ', '\t']).len();
    let preceding = text[..before_content].chars().next_back();
    let remove_start = match preceding {
        Some(';') => before_content - 1,
        Some('{') => start,
        _ => {
            return Err(AuthorError::new(
                "unsafe-source-shape",
                "compact presentation metadata has no adjacent KDL separator",
            ));
        }
    };
    let mut replacement = text.to_owned();
    replacement.replace_range(remove_start..end, "");
    Ok(replacement)
}

pub(crate) fn line_indent(text: &str, offset: usize) -> Option<String> {
    let prefix = text.get(..offset)?;
    let start = prefix.rfind('\n').map_or(0, |newline| newline + 1);
    let indent = prefix.get(start..)?;
    indent
        .chars()
        .all(|value| matches!(value, ' ' | '\t'))
        .then(|| indent.to_owned())
}

fn verify_candidate(
    candidate: &str,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    field: PresentationField,
    expected: Option<&str>,
) -> Result<(), AuthorError> {
    let document = KdlDocument::parse(candidate).map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("presentation edit did not produce valid KDL: {error}"),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    let fields = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| child.name().value() == field.as_str())
        .collect::<Vec<_>>();
    let observed = match fields.as_slice() {
        [] => None,
        [node] => Some(parse_field_value(node, field)?),
        _ => {
            return Err(AuthorError::new(
                "unsafe-source-edit",
                format!(
                    "presentation edit produced duplicate `{}` fields",
                    field.as_str()
                ),
            ));
        }
    };
    if observed != expected {
        return Err(AuthorError::new(
            "unsafe-source-edit",
            format!(
                "presentation edit did not produce the requested `{}`",
                field.as_str()
            ),
        ));
    }
    Ok(())
}

fn atomic_replace_checked(
    catalog_lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    original: &[u8],
    original_version: SourceVersion,
    replacement: &[u8],
    mode: u32,
    before_commit: impl FnOnce(),
) -> Result<(), AuthorError> {
    let directory = path.parent().ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!("declaration path {} has no parent", path.display()),
        )
    })?;
    let mut temporary = tempfile::Builder::new()
        .prefix("agent-author-")
        .tempfile_in(control)
        .map_err(|error| {
            AuthorError::new(
                "declaration-write-failed",
                format!("staging declaration {}: {error}", path.display()),
            )
        })?;
    temporary
        .as_file_mut()
        .set_permissions(fs::Permissions::from_mode(mode))
        .and_then(|()| temporary.write_all(replacement))
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| {
            AuthorError::new(
                "declaration-write-failed",
                format!("staging declaration {}: {error}", path.display()),
            )
        })?;
    test_crash_after_temporary_write();
    before_commit();
    let current = fs::symlink_metadata(path)
        .ok()
        .filter(|metadata| metadata.file_type().is_file())
        .map(|metadata| (SourceVersion::from_metadata(&metadata), fs::read(path).ok()));
    if !matches!(current, Some((version, Some(bytes))) if version == original_version && bytes == original)
    {
        return Err(AuthorError::new(
            "source-changed",
            format!(
                "declaration {} changed while the edit was authored",
                path.display()
            ),
        ));
    }
    #[cfg(debug_assertions)]
    if std::env::var_os("ST2_TEST_AGENT_AUTHOR_FAIL_BEFORE_PUBLISH").is_some() {
        return Err(AuthorError::new(
            "declaration-write-failed",
            "injected declaration publication failure",
        ));
    }
    let generation = catalog_lock.begin_generation_commit().map_err(|error| {
        AuthorError::new(
            "declaration-write-failed",
            format!("prepare catalog generation: {error:#}"),
        )
    })?;
    if let Err(error) = crate::catalog_transaction::persist_tempfile_from_control(
        catalog_lock.control(),
        catalog,
        temporary,
        path,
    ) {
        return Err(AuthorError::new(
            "declaration-write-failed",
            format!(
                "atomically publishing declaration {}: {error}",
                path.display()
            ),
        ));
    }
    crate::catalog_transaction::open_dir_beneath(catalog, directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            AuthorError::new(
                "declaration-write-failed",
                format!(
                    "syncing declaration directory {}: {error}",
                    directory.display()
                ),
            )
        })?;
    generation.commit().map_err(|error| {
        AuthorError::new(
            "declaration-write-failed",
            format!("advance catalog generation: {error:#}"),
        )
    })?;
    Ok(())
}

#[cfg(debug_assertions)]
fn test_crash_after_temporary_write() {
    if std::env::var_os("ST2_TEST_AGENT_AUTHOR_CRASH_AFTER_TEMP").is_some() {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn test_crash_after_temporary_write() {}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, relative: &str, contents: &str) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();
        path
    }

    fn declaration(
        identity: &str,
        host: &str,
        supervisor: Option<&str>,
        managed_by: &str,
    ) -> String {
        let supervisor = supervisor
            .map(|value| format!("  supervisor {value:?}\n"))
            .unwrap_or_default();
        format!(
            "// keep this comment\nagent {identity:?} {{\n  host {host:?}\n  meta {{ managed-by {managed_by:?}; keep \"exact\" }}\n{supervisor}  command \"sleep 60\"\n}}\n"
        )
    }

    #[test]
    fn source_preserving_set_replace_idempotent_and_clear() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        let before = fs::read_to_string(&path).unwrap();

        let set = set_presentation(
            root,
            "h.worker",
            "h",
            None,
            PresentationField::Name,
            Some("Build owner"),
        )
        .unwrap();
        assert_eq!(set.result, AuthorOutcome::Changed);
        let after_set = fs::read_to_string(&path).unwrap();
        assert_eq!(after_set.matches("name \"Build owner\"").count(), 1);
        assert_eq!(after_set.replace("  name \"Build owner\"\n", ""), before);

        assert_eq!(
            set_presentation(
                root,
                "worker",
                "h",
                None,
                PresentationField::Name,
                Some("Build owner")
            )
            .unwrap()
            .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(
            set_presentation(
                root,
                "worker",
                "h",
                None,
                PresentationField::Name,
                Some("Release owner")
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            set_presentation(root, "worker", "h", None, PresentationField::Name, None)
                .unwrap()
                .result,
            AuthorOutcome::Changed
        );
        assert_eq!(fs::read_to_string(path).unwrap(), before);
    }

    #[test]
    fn source_preserving_edit_accepts_a_dotted_host_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "us.east/worker/agent.kdl",
            &declaration("worker", "us.east", None, "catalog"),
        );

        let receipt = set_presentation(
            root,
            "us.east.worker",
            "elsewhere",
            None,
            PresentationField::Name,
            Some("Build owner"),
        )
        .unwrap();

        assert_eq!(receipt.identity, "us.east.worker");
        assert!(
            fs::read_to_string(path)
                .unwrap()
                .contains("name \"Build owner\"")
        );
    }

    #[test]
    fn source_preserving_clear_accepts_a_crlf_dedicated_field() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let original = declaration("worker", "h", None, "catalog").replace('\n', "\r\n");
        let with_name = original.replace(
            "  command \"sleep 60\"",
            "  name \"Build owner\"\r\n  command \"sleep 60\"",
        );
        let path = write(root, "h/worker/agent.kdl", &with_name);

        let receipt =
            set_presentation(root, "h.worker", "h", None, PresentationField::Name, None).unwrap();

        assert_eq!(receipt.result, AuthorOutcome::Changed);
        assert_eq!(fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn self_and_supervisor_can_edit_but_sibling_and_nix_owner_cannot() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/root/agent.kdl",
            &declaration("root", "h", None, "catalog"),
        );
        write(
            root,
            "h/child/agent.kdl",
            &declaration("child", "h", Some("root"), "catalog"),
        );
        write(
            root,
            "h/sibling/agent.kdl",
            &declaration("sibling", "h", Some("root"), "catalog"),
        );
        write(
            root,
            "h/nix/agent.kdl",
            &declaration("nix", "h", Some("root"), "nix"),
        );

        set_presentation(
            root,
            "h.child",
            "h",
            Some("h.child"),
            PresentationField::Name,
            Some("self"),
        )
        .unwrap();
        set_presentation(
            root,
            "h.child",
            "h",
            Some("h.root"),
            PresentationField::Description,
            Some("supervised"),
        )
        .unwrap();
        assert_eq!(
            set_presentation(
                root,
                "h.sibling",
                "h",
                Some("h.child"),
                PresentationField::Name,
                Some("no")
            )
            .unwrap_err()
            .code(),
            "presentation-not-authorized"
        );
        assert_eq!(
            set_presentation(
                root,
                "h.nix",
                "h",
                Some("h.root"),
                PresentationField::Name,
                Some("no")
            )
            .unwrap_err()
            .code(),
            "nix-managed-declaration"
        );
    }

    #[test]
    fn stale_source_refuses_atomic_replace() {
        let temporary = tempfile::tempdir().unwrap();
        let path = write(
            temporary.path(),
            "agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        let changed = declaration("worker", "h", None, "external");
        let error = edit_declaration_for_test(
            &path,
            "h.worker",
            "h",
            "worker",
            PresentationField::Name,
            Some("Owner"),
            || fs::write(&path, &changed).unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "source-changed");
        assert_eq!(fs::read_to_string(path).unwrap(), changed);
    }

    #[test]
    fn source_version_rejects_byte_identical_aba_rewrite() {
        let temporary = tempfile::tempdir().unwrap();
        let original = declaration("worker", "h", None, "catalog");
        let path = write(temporary.path(), "agent.kdl", &original);
        let error = edit_declaration_for_test(
            &path,
            "h.worker",
            "h",
            "worker",
            PresentationField::Name,
            Some("Owner"),
            || {
                fs::write(&path, "temporary competing bytes").unwrap();
                fs::write(&path, &original).unwrap();
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), "source-changed");
        assert_eq!(fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn desired_state_authoring_refuses_a_stale_source() {
        let temporary = tempfile::tempdir().unwrap();
        let path = write(
            temporary.path(),
            "agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        let changed = declaration("worker", "h", None, "external");
        let error = edit_desired_state_for_test(
            &path,
            DesiredStateValue::Suspended,
            Some("Waiting for capacity"),
            || fs::write(&path, &changed).unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "source-changed");
        assert_eq!(fs::read_to_string(path).unwrap(), changed);
    }

    #[test]
    fn desired_state_authoring_prefers_an_explicit_target_over_an_anonymous_sibling() {
        let temporary = tempfile::tempdir().unwrap();
        let path = write(
            temporary.path(),
            "agent.kdl",
            "agent \"worker\" { host \"h\"; command \"sleep 60\" }\nagent { host \"h\"; command \"sleep 60\" }\n",
        );

        let result = edit_desired_state_for_test(
            &path,
            DesiredStateValue::Suspended,
            Some("Waiting for capacity"),
            || {},
        )
        .unwrap();

        assert_eq!(result, AuthorOutcome::Changed);
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "agent \"worker\" { host \"h\"; command \"sleep 60\"; desired-state \"suspended\" reason=\"Waiting for capacity\" }\nagent { host \"h\"; command \"sleep 60\" }\n"
        );
    }

    #[test]
    fn stream_add_supports_external_command_and_argv_and_external_remove_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        let original = fs::read_to_string(&path).unwrap();

        assert_eq!(
            add_stream(root, "h.worker", "h", Some("h.worker"), "webhook", None)
                .unwrap()
                .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            add_stream(
                root,
                "h.worker",
                "h",
                Some("h.worker"),
                "github-ci",
                Some(StreamLaunch::Command("gh watch --repo st2".to_owned())),
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            add_stream(
                root,
                "h.worker",
                "h",
                Some("h.worker"),
                "tick",
                Some(StreamLaunch::Argv(vec![
                    "tick-source".to_owned(),
                    "--daily".to_owned()
                ])),
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            add_stream(root, "h.worker", "h", Some("h.worker"), "webhook", None)
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        let authored = fs::read_to_string(&path).unwrap();
        assert!(authored.contains("stream \"webhook\" {}"));
        assert!(authored.contains("stream \"github-ci\" { command \"gh watch --repo st2\" }"));
        assert!(authored.contains("stream \"tick\" { argv \"tick-source\" \"--daily\" }"));

        assert_eq!(
            remove_stream(root, "h.worker", "h", None, "webhook")
                .unwrap()
                .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            remove_stream(root, "h.worker", "h", None, "webhook")
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        let remaining = fs::read_to_string(path).unwrap();
        assert!(!remaining.contains("stream \"webhook\""));
        assert!(remaining.contains("stream \"github-ci\""));
        assert!(remaining.contains("stream \"tick\""));
        assert_ne!(remaining, original);
    }

    #[test]
    fn stream_candidate_verification_matches_the_exact_host_agent() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "agents.kdl",
            "agent \"worker\" { host \"alpha\"; command \"sleep 60\"; stream \"existing\" {} }\nagent \"worker\" { host \"beta\"; command \"sleep 60\"; stream \"existing\" {} }\n",
        );

        assert_eq!(
            add_stream(
                root,
                "beta.worker",
                "beta",
                Some("beta.worker"),
                "webhook",
                None,
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        assert_eq!(
            remove_stream(root, "beta.worker", "beta", Some("beta.worker"), "existing",)
                .unwrap()
                .result,
            AuthorOutcome::Changed
        );

        let authored = fs::read_to_string(path).unwrap();
        let document = KdlDocument::parse(&authored).unwrap();
        let agents = document.nodes();
        assert!(agents[0].to_string().contains("stream \"existing\""));
        assert!(!agents[0].to_string().contains("stream \"webhook\""));
        assert!(!agents[1].to_string().contains("stream \"existing\""));
        assert!(agents[1].to_string().contains("stream \"webhook\""));
    }

    #[test]
    fn stream_authoring_enforces_authority_nix_ownership_and_canonical_validation() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/root/agent.kdl",
            &declaration("root", "h", None, "catalog"),
        );
        write(
            root,
            "h/child/agent.kdl",
            &declaration("child", "h", Some("root"), "catalog"),
        );
        write(
            root,
            "h/sibling/agent.kdl",
            &declaration("sibling", "h", Some("root"), "catalog"),
        );
        write(
            root,
            "h/nix/agent.kdl",
            &declaration("nix", "h", Some("root"), "nix"),
        );

        add_stream(root, "h.child", "h", Some("h.root"), "events", None).unwrap();
        assert_eq!(
            add_stream(root, "h.sibling", "h", Some("h.child"), "events", None)
                .unwrap_err()
                .code(),
            "stream-not-authorized"
        );
        assert_eq!(
            add_stream(root, "h.nix", "h", Some("h.root"), "events", None)
                .unwrap_err()
                .code(),
            "nix-managed-declaration"
        );
        assert_eq!(
            add_stream(root, "h.child", "h", None, "Bad Name", None)
                .unwrap_err()
                .code(),
            "invalid-stream"
        );
        assert_eq!(
            add_stream(
                root,
                "h.child",
                "h",
                None,
                "empty-argv",
                Some(StreamLaunch::Argv(Vec::new())),
            )
            .unwrap_err()
            .code(),
            "invalid-stream"
        );
    }

    #[test]
    fn stream_authoring_refuses_catalogs_with_concealed_declarations() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("catalog");
        let concealed = temporary.path().join("concealed");
        let declaration_path = write(
            &root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        write(
            &concealed,
            "agent.kdl",
            &declaration("shadow", "h", None, "catalog"),
        );
        symlink(&concealed, root.join("concealed-link")).unwrap();
        let original = fs::read(&declaration_path).unwrap();

        let error = add_stream(&root, "h.worker", "h", None, "events", None).unwrap_err();

        assert_eq!(error.code(), "catalog-malformed");
        assert!(
            error.to_string().contains("unobservable declaration entry"),
            "{error}"
        );
        assert_eq!(fs::read(declaration_path).unwrap(), original);
    }

    fn bound(root: &Path, identity: &str, name: &str) -> Resource {
        crate::discover(root)
            .specs
            .into_iter()
            .find(|spec| spec.identity == identity)
            .expect("catalog declares the agent")
            .resources
            .into_iter()
            .find(|resource| resource.name() == name)
            .expect("agent declares the binding")
    }

    #[test]
    fn resource_add_declares_updates_in_place_and_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );

        let added = add_resource(
            root,
            "h.worker",
            "h",
            Some("h.worker"),
            "work",
            "github-issue://example/project/123",
            "release work item",
            None,
        )
        .unwrap();
        assert_eq!(added.result, AuthorOutcome::Changed);
        assert_eq!(added.identity, "h.worker");
        assert_eq!(added.inactive_reason, None);

        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "source",
            "worktree://github.com/example/project/change",
            "primary checkout",
            None,
        )
        .unwrap();
        let two_bindings = fs::read_to_string(&path).unwrap();

        // An identical request proves the binding rather than rewriting the declaration.
        assert_eq!(
            add_resource(
                root,
                "h.worker",
                "h",
                None,
                "work",
                "github-issue://example/project/123",
                "release work item",
                None,
            )
            .unwrap()
            .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), two_bindings);

        // Re-declaring an existing name updates it in place, keeping its position and siblings.
        assert_eq!(
            add_resource(
                root,
                "h.worker",
                "h",
                None,
                "work",
                "github-issue://example/project/456",
                "follow-up work item",
                Some("superseded by the follow-up"),
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        let updated = fs::read_to_string(&path).unwrap();
        assert_eq!(updated.matches("resource \"work\"").count(), 1);
        assert!(
            updated.find("resource \"work\"").unwrap()
                < updated.find("resource \"source\"").unwrap()
        );
        assert!(updated.contains("// keep this comment"));
        assert!(updated.contains("keep \"exact\""));

        let work = bound(root, "worker", "work");
        assert_eq!(work.uri(), "github-issue://example/project/456");
        assert_eq!(work.reason(), "follow-up work item");
        assert_eq!(work.inactive_reason(), Some("superseded by the follow-up"));
        assert_eq!(
            bound(root, "worker", "source").uri(),
            "worktree://github.com/example/project/change"
        );

        // The request declares the complete binding, so an omitted inactive-reason clears it.
        assert_eq!(
            add_resource(
                root,
                "h.worker",
                "h",
                None,
                "work",
                "github-issue://example/project/456",
                "follow-up work item",
                None,
            )
            .unwrap()
            .result,
            AuthorOutcome::Changed
        );
        assert_eq!(bound(root, "worker", "work").inactive_reason(), None);
        let cleared = fs::read_to_string(&path).unwrap();
        assert!(!cleared.contains("inactive-reason"));
        assert_eq!(cleared.matches("resource \"work\"").count(), 1);
    }

    #[test]
    fn resource_add_proves_a_hand_authored_binding_without_rewriting_it() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            "agent \"worker\" {\n  host \"h\"\n  command \"sleep 60\"\n  \
             resource \"work\" reason=\"release work item\" uri=\"github-issue://example/project/123\"\n}\n",
        );
        let original = fs::read_to_string(&path).unwrap();

        // Hand-authored property order and spacing are proven, not re-rendered.
        assert_eq!(
            add_resource(
                root,
                "h.worker",
                "h",
                None,
                "work",
                "github-issue://example/project/123",
                "release work item",
                None,
            )
            .unwrap()
            .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
    }

    #[test]
    fn resource_remove_is_idempotent_and_keeps_unrelated_bindings() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "work",
            "github-issue://example/project/123",
            "release work item",
            None,
        )
        .unwrap();
        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "source",
            "worktree://github.com/example/project/change",
            "primary checkout",
            None,
        )
        .unwrap();

        let removed = remove_resource(root, "h.worker", "h", Some("h.worker"), "work").unwrap();
        assert_eq!(removed.result, AuthorOutcome::Changed);
        assert_eq!(removed.name, "work");
        let after_remove = fs::read_to_string(&path).unwrap();

        assert_eq!(
            remove_resource(root, "h.worker", "h", None, "work")
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(
            remove_resource(root, "h.worker", "h", None, "never-declared")
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), after_remove);
        assert!(!after_remove.contains("resource \"work\""));
        assert!(after_remove.contains("resource \"source\""));
        assert!(after_remove.contains("// keep this comment"));
        assert_eq!(bound(root, "worker", "source").reason(), "primary checkout");
    }

    #[test]
    fn resource_rename_carries_the_binding_and_refuses_absent_or_colliding_names() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "work",
            "github-issue://example/project/123",
            "release work item",
            Some("merged and retained for traceability"),
        )
        .unwrap();
        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "source",
            "worktree://github.com/example/project/change",
            "primary checkout",
            None,
        )
        .unwrap();
        let before = fs::read_to_string(&path).unwrap();

        assert_eq!(
            rename_resource(root, "h.worker", "h", None, "work", "work")
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), before);

        let renamed =
            rename_resource(root, "h.worker", "h", Some("h.worker"), "work", "task").unwrap();
        assert_eq!(renamed.result, AuthorOutcome::Changed);
        assert_eq!(renamed.old, "work");
        assert_eq!(renamed.new, "task");

        let task = bound(root, "worker", "task");
        assert_eq!(task.uri(), "github-issue://example/project/123");
        assert_eq!(task.reason(), "release work item");
        assert_eq!(
            task.inactive_reason(),
            Some("merged and retained for traceability")
        );
        let authored = fs::read_to_string(&path).unwrap();
        assert!(!authored.contains("resource \"work\""));
        assert!(
            authored.find("resource \"task\"").unwrap()
                < authored.find("resource \"source\"").unwrap()
        );

        assert_eq!(
            rename_resource(root, "h.worker", "h", None, "work", "elsewhere")
                .unwrap_err()
                .code(),
            "resource-not-found"
        );
        // An absent `old` refuses even when the rename would otherwise be a self-rename no-op.
        assert_eq!(
            rename_resource(root, "h.worker", "h", None, "absent", "absent")
                .unwrap_err()
                .code(),
            "resource-not-found"
        );
        assert_eq!(
            rename_resource(root, "h.worker", "h", None, "task", "source")
                .unwrap_err()
                .code(),
            "resource-already-exists"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), authored);
    }

    #[test]
    fn resource_authoring_enforces_validation_authority_and_nix_ownership() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/root/agent.kdl",
            &declaration("root", "h", None, "catalog"),
        );
        let child = write(
            root,
            "h/child/agent.kdl",
            &declaration("child", "h", Some("root"), "catalog"),
        );
        write(
            root,
            "h/sibling/agent.kdl",
            &declaration("sibling", "h", Some("root"), "catalog"),
        );
        let nix_owned = write(
            root,
            "h/nix/agent.kdl",
            &declaration("nix", "h", Some("root"), "nix"),
        );
        let untouched = fs::read_to_string(&nix_owned).unwrap();

        add_resource(
            root,
            "h.child",
            "h",
            Some("h.root"),
            "work",
            "github-issue://example/project/1",
            "supervised work item",
            None,
        )
        .unwrap();

        assert_eq!(
            add_resource(
                root,
                "h.sibling",
                "h",
                Some("h.child"),
                "work",
                "github-issue://example/project/1",
                "reaching across the fleet",
                None,
            )
            .unwrap_err()
            .code(),
            "resource-not-authorized"
        );
        assert_eq!(
            remove_resource(root, "h.sibling", "h", Some("h.child"), "work")
                .unwrap_err()
                .code(),
            "resource-not-authorized"
        );
        assert_eq!(
            add_resource(
                root,
                "h.nix",
                "h",
                Some("h.root"),
                "work",
                "github-issue://example/project/1",
                "Nix owns this declaration",
                None,
            )
            .unwrap_err()
            .code(),
            "nix-managed-declaration"
        );

        // A catalog-relative carrier path is admitted since #345, so the refusals worth pinning are
        // the ones that escape the catalog, plus empty names and empty explanations.
        for (name, uri, reason, inactive_reason) in [
            ("absolute-path", "/etc/passwd", "escapes the catalog", None),
            ("parent-escape", "../outside", "escapes the catalog", None),
            ("spaced", "issue://example/a b", "unencoded space", None),
            ("", "issue://example/1", "empty name", None),
            ("blank-reason", "issue://example/1", "", None),
            (
                "blank-inactive",
                "issue://example/1",
                "still explained",
                Some(""),
            ),
        ] {
            let error = add_resource(
                root,
                "h.child",
                "h",
                None,
                name,
                uri,
                reason,
                inactive_reason,
            )
            .unwrap_err();
            assert_eq!(error.code(), "invalid-resource", "{name}: {error}");
        }
        assert_eq!(
            fs::read_to_string(&child)
                .unwrap()
                .matches("resource ")
                .count(),
            1
        );

        // #345 widened the envelope: a catalog-relative carrier path is a valid binding uri.
        add_resource(
            root,
            "h.child",
            "h",
            None,
            "carrier",
            "carriers/goal.md",
            "Catalog-relative carrier.",
            None,
        )
        .expect("a catalog-relative carrier path is admitted");
        assert_eq!(fs::read_to_string(&nix_owned).unwrap(), untouched);
    }

    #[test]
    fn resource_uri_is_preserved_byte_for_byte() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", None, "catalog"),
        );
        let exact = "vendor+Thing://Authority.Example/Exact%20Identity?Query=A%2Fb#Frag%20Ment";

        add_resource(
            root,
            "h.worker",
            "h",
            None,
            "subject",
            exact,
            "exact vendor identity",
            None,
        )
        .unwrap();
        assert_eq!(bound(root, "worker", "subject").uri(), exact);

        // The rename path carries the identity across without normalizing it either.
        rename_resource(root, "h.worker", "h", None, "subject", "carried").unwrap();
        assert_eq!(bound(root, "worker", "carried").uri(), exact);

        // A byte-identical re-declaration is a proven no-op, not a rewrite.
        assert_eq!(
            add_resource(
                root,
                "h.worker",
                "h",
                None,
                "carried",
                exact,
                "exact vendor identity",
                None,
            )
            .unwrap()
            .result,
            AuthorOutcome::Unchanged
        );
    }
}
