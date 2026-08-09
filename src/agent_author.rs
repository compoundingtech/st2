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
    AGENT_DESCRIPTION_MAX_CHARS, AGENT_NAME_MAX_CHARS, validate_desired_state_reason,
    validate_presentation,
};
use kdl::{KdlDocument, KdlNode};
use serde::Serialize;

use crate::catalog_lock::CatalogLock;

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
        validate_desired_state_reason(reason).map_err(|error| {
            AuthorError::new("invalid-desired-state", error.to_string())
        })?;
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
                error.path.display(), error.message
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
            format!("desired-state authoring requires canonical KDL, found {}", path.display()),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new("declaration-read-failed", format!("reading declaration {}: {error}", path.display()))
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new("declaration-read-failed", format!("reading declaration {}: {error}", path.display()))
    })?;
    let original_version = SourceVersion::from_metadata(&metadata);
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new("malformed-declaration", format!("declaration {} is not UTF-8: {error}", path.display()))
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new("malformed-declaration", format!("parsing declaration {}: {error}", path.display()))
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    if is_nix_managed(target) {
        return Err(AuthorError::new(
            "nix-managed-declaration",
            format!("agent {expected_identity:?} is Nix-owned; edit its Nix source instead of {}", path.display()),
        ));
    }
    let work_removed = if state == DesiredStateValue::Retired {
        remove_work_resources(text, target).map_err(|error| {
            AuthorError::new(
                "unsafe-source-edit",
                format!(
                    "cannot remove the `resource \"work\"` node from {} for agent {expected_identity:?}: {error}; remove that node by hand and retry",
                    path.display()
                ),
            )
        })?
    } else {
        None
    };
    let replacement = if let Some(without_work) = work_removed {
        let document = KdlDocument::parse(&without_work).map_err(|error| {
            AuthorError::new(
                "unsafe-source-edit",
                format!(
                    "removing the `resource \"work\"` node from {} for agent {expected_identity:?} did not produce valid KDL: {error}; remove that node by hand and retry",
                    path.display()
                ),
            )
        })?;
        let target = exact_agent_node(
            &document,
            expected_identity,
            expected_host,
            expected_agent,
        )
        .map_err(|error| {
            AuthorError::new(
                "unsafe-source-edit",
                format!(
                    "cannot re-resolve agent {expected_identity:?} in {} after removing its `resource \"work\"` node: {error}; remove that node by hand and retry",
                    path.display()
                ),
            )
        })?;
        desired_state_edit(&without_work, target, state, reason)?.or(Some(without_work))
    } else {
        desired_state_edit(text, target, state, reason)?
    };
    let Some(replacement) = replacement else {
        return Ok(AuthorOutcome::Unchanged);
    };
    verify_desired_state_candidate(
        path,
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

fn remove_work_resources(text: &str, target: &KdlNode) -> Result<Option<String>, AuthorError> {
    let mut work_resources = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| {
            child.name().value() == "resource"
                && child.get(0).and_then(|entry| entry.as_string()) == Some("work")
        })
        .collect::<Vec<_>>();
    if work_resources.is_empty() {
        return Ok(None);
    }
    work_resources.sort_by_key(|node| std::cmp::Reverse(node.span().offset()));
    let mut replacement = text.to_owned();
    for resource in work_resources {
        replacement = remove_field(&replacement, resource)?;
    }
    Ok(Some(replacement))
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
    path: &Path,
    candidate: &str,
    expected_identity: &str,
    expected_host: &str,
    expected_agent: &str,
    state: DesiredStateValue,
    reason: Option<&str>,
) -> Result<(), AuthorError> {
    let document = KdlDocument::parse(candidate).map_err(|error| {
        AuthorError::new("unsafe-source-edit", format!("desired-state edit did not produce valid KDL: {error}"))
    })?;
    let target = exact_agent_node(&document, expected_identity, expected_host, expected_agent)?;
    let lifecycle = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| matches!(child.name().value(), "desired-state" | "retired"))
        .collect::<Vec<_>>();
    let lifecycle_matches = if state == DesiredStateValue::Running {
        lifecycle.is_empty()
    } else if let [node] = lifecycle.as_slice()
        && node.name().value() == "desired-state"
        && node.get(0).and_then(|entry| entry.as_string()) == Some(state.as_str())
        && node.get("reason").and_then(|entry| entry.as_string()) == reason
    {
        true
    } else {
        false
    };
    if !lifecycle_matches {
        return Err(AuthorError::new(
            "unsafe-source-edit",
            "desired-state candidate did not read back as the authored intent",
        ));
    }
    if state == DesiredStateValue::Retired
        && target
            .children()
            .into_iter()
            .flat_map(|children| children.nodes())
            .any(|child| {
                child.name().value() == "resource"
                    && child.get(0).and_then(|entry| entry.as_string()) == Some("work")
            })
    {
        return Err(AuthorError::new(
            "unsafe-source-edit",
            format!(
                "retirement candidate {} for agent {expected_identity:?} still contains a `resource \"work\"` node; remove that node by hand and retry",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn exact_agent_node<'a>(
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

fn agent_identity_parts(node: &KdlNode) -> (Option<String>, Option<String>) {
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

fn is_nix_managed(node: &KdlNode) -> bool {
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

fn quoted(value: &str) -> Result<String, AuthorError> {
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

fn insert_node(text: &str, target: &KdlNode, authored: &str) -> Result<String, AuthorError> {
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
        && text[end..line_end]
            .chars()
            .all(|value| matches!(value, ' ' | '\t' | '\r'))
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

fn line_indent(text: &str, offset: usize) -> Option<String> {
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
    fn retired_work_verification_error_names_the_manual_recovery() {
        let path = Path::new("catalog/agents/h/worker/agent.kdl");
        let candidate = concat!(
            "agent \"worker\" {\n",
            "  host \"h\"\n",
            "  desired-state \"retired\" reason=\"Mission complete\"\n",
            "  resource \"work\" _tag=\"github-issue\" uri=\"github-issue://example/project/216\"\n",
            "  command \"true\"\n",
            "}\n",
        );

        let error = verify_desired_state_candidate(
            path,
            candidate,
            "h.worker",
            "h",
            "worker",
            DesiredStateValue::Retired,
            Some("Mission complete"),
        )
        .unwrap_err();

        assert_eq!(error.code(), "unsafe-source-edit");
        let message = error.to_string();
        assert!(message.contains("catalog/agents/h/worker/agent.kdl"));
        assert!(message.contains("`resource \"work\"` node"));
        assert!(message.contains("remove that node by hand and retry"));
    }
}
