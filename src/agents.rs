//! The agent roster (M2.3): the data behind `st2 agents`. Enumerates the catalog's agents with their
//! presence status and retirement state, and optionally last-activity + inbox count. The JSON field
//! names, order, and null handling are a stable machine-readable contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::Serialize;

use crate::message;
use crate::status::{self, State};
use crate::{AgentSpec, Discovered, Resource, driver_diagnostic, harness_context, harness_state};

/// One roster row: everything `st2 agents [--enrich]` can report about an agent.
#[derive(Debug, Clone)]
pub struct AgentRow {
    /// The positional `<host>.<identity>` declaration key — the legacy address fallback and the
    /// bytes legacy-ID migration freezes. Not the subject's immutable identity.
    pub identity: String,
    /// Declaration source used to attribute this runtime observation.
    pub source_path: PathBuf,
    /// Effective presence (derived: stale → `unknown`, etc.).
    pub status: State,
    /// Optional display name from the Agent Spec declaration.
    pub name: Option<String>,
    /// Optional enduring responsibility boundary from the Agent Spec declaration.
    pub description: Option<String>,
    /// Whether the declaration is explicitly retired. Presence remains a separate runtime signal.
    pub retired: bool,
    /// Declarative whole-agent lifecycle intent.
    pub desired_state: String,
    /// Human-facing rationale for suspended/retired new-style declarations.
    pub desired_state_reason: Option<String>,
    /// Typed Resource bindings declared directly by the agent.
    pub resources: Vec<Resource>,
    /// Resync coverage derived from the declaration directory and each Resource URI.
    pub resource_resync: Vec<crate::resync::ResyncCoverage>,
    /// Newest activity time across inbox, archive, and status. Version 1 status uses its embedded
    /// writer timestamp; message files and legacy status use local mtime. `--enrich` only.
    pub last_activity_ms: Option<f64>,
    /// Count of canonical message files in the agent's inbox. `--enrich` only.
    pub inbox: usize,
    /// Observed harness state — the driver-owned signal of what the harness is seen doing, a third
    /// axis independent from declared presence and from desired lifecycle. `None` means no driver
    /// has ever published a record for this agent, which is different from a derived `unknown`.
    pub observed: Option<harness_state::Observed>,
    /// Current native-driver diagnostic. Absence and unreadable records remain explicit states;
    /// neither is projected as healthy.
    pub driver_diagnostic: driver_diagnostic::Observed,
    /// Harness context — how full the harness's window is, a fourth axis independent of the other
    /// three. `None` means no record exists; a record past its horizon is still reported, marked
    /// stale and carrying its age, so it survives every `observedState: unknown` derivation.
    pub context: Option<harness_context::Observed>,
    /// The catalog-global immutable agent ID: what owns this agent's runtime, durable state,
    /// graph edges, and automation. Equal to `identity` for an unmigrated declaration by
    /// construction, which is why migration moves no state.
    pub id: String,
    /// The effective host-local address: the explicit declared `address`, else `identity`.
    pub address: String,
    /// The qualified human route `<host>.<effective-address>`, or `None` for a proved
    /// non-routable subject. A retired subject releases its address but keeps its ID.
    pub bus_address: Option<String>,
}

/// Every agent in the catalog, sorted by positional declaration key, with presence + enrich data
/// computed. Read-only: walks discovered specs and each agent's resources, mutating nothing.
pub fn roster(catalog_root: &Path, this_host: &str) -> Vec<AgentRow> {
    let found = crate::discover(catalog_root);
    roster_from_discovered(&found, catalog_root, this_host)
}

/// Project a roster from one immutable discovery result. Exact selectors use this after proving
/// discovery complete so the uniqueness check and returned metadata describe the same snapshot.
pub fn roster_from_discovered(
    found: &Discovered,
    catalog_root: &Path,
    this_host: &str,
) -> Vec<AgentRow> {
    let pty_root = probe_pty_root(catalog_root);
    let profiles = crate::catalog::declared_profiles(catalog_root).unwrap_or_default();
    let profile_refresh = profiles.begin_refresh();
    let mut rows: Vec<AgentRow> = found
        .specs
        .iter()
        .filter_map(|s| {
            let agent_dir = s.path.parent()?;
            Some(AgentRow {
                identity: s.legacy_bus_identity(this_host),
                source_path: s.path.clone(),
                status: status::read_state(&status::status_path(agent_dir)),
                name: s.name.clone(),
                description: s.description.clone(),
                retired: s.desired_state.is_retired(),
                desired_state: s.desired_state.as_str().to_owned(),
                desired_state_reason: s.desired_state.reason().map(str::to_owned),
                resources: s.resources.clone(),
                resource_resync: s
                    .resources
                    .iter()
                    .map(|resource| {
                        crate::resync::resource_coverage_with_profiles(
                            agent_dir,
                            resource,
                            &profile_refresh,
                        )
                    })
                    .collect(),
                last_activity_ms: newest_activity_ms(agent_dir),
                inbox: inbox_count(agent_dir),
                observed: observed_state(s, agent_dir, &pty_root, this_host),
                driver_diagnostic: driver_diagnostic::read(&driver_diagnostic::path(agent_dir)),
                // Read independently of the state record above: the wedge case this exists for is
                // an agent whose state has gone indeterminate at 190k of a 200k window.
                context: harness_context::read(&harness_context::harness_context_path(agent_dir)),
                id: s.agent_id(this_host),
                address: s.effective_address().to_owned(),
                // Routability, not presence: a retired subject releases its effective address, so
                // it has no human route at all while keeping its ID and staying reachable by it.
                bus_address: (!s.desired_state.is_retired())
                    .then(|| s.bus_address(this_host)),
            })
        })
        .collect();
    rows.sort_by(|a, b| a.identity.cmp(&b.identity));
    rows
}

/// Read the observed-harness-state record beside `status`. The session-liveness cross-check is
/// host-local by construction: it applies only to agents this host runs, and an unreadable
/// registry downgrades nothing.
fn observed_state(
    spec: &AgentSpec,
    agent_dir: &Path,
    pty_root: &Path,
    this_host: &str,
) -> Option<harness_state::Observed> {
    let path = harness_state::harness_state_path(agent_dir);
    if spec.resolved_host(this_host) == this_host {
        let probe = |session: &str| crate::ding::session_liveness_in(pty_root, session);
        harness_state::read(&path, Some(&probe))
    } else {
        harness_state::read(&path, None)
    }
}

/// The pty registry root the probe reads: exactly the runner's own resolution, so the reader and
/// the sessions it probes can never disagree. The runner honors `PTY_ROOT` and nothing else — a
/// legacy `PTY_SESSION_DIR` here would point the probe at a directory st2-managed sessions never
/// use, turning provable deaths into indeterminate reads.
pub fn probe_pty_root(catalog_root: &Path) -> PathBuf {
    crate::run::effective_pty_root(catalog_root)
}

/// The `observedState` object inside a roster row. Vocabulary words are the record's own
/// (`as_str`), never Rust identifier spellings. The shipped fields keep their names, order, and
/// meaning; the version 3 axes are APPENDED, so a consumer pinned to the old shape reads exactly
/// what it read before.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObservedJson<'a> {
    state: &'a str,
    blocked_on: &'a str,
    input_buffer: &'a str,
    ask: &'a str,
    harness: Option<&'a str>,
    since: Option<u64>,
    reason: Option<&'a str>,
    exit: Option<&'a str>,
    /// The exact version the record declared, or `null` when the bytes declared none. This is
    /// what makes a migration's drain gate POSITIVE: "every row reads `st2.harness-state.v3`" is
    /// checkable, while "no row is still v1" is not checkable from any absence.
    schema: Option<&'a str>,
    /// Typed indeterminacy, non-null exactly when the observation is indeterminate. The
    /// authoritative field; the scalar `reason` above remains its compatibility projection.
    indeterminacy: Option<IndeterminacyJson<'a>>,
    /// The condition axis, always emitted. `absent` for versions 1 and 2, which carry no such
    /// axis — never `clear`, and no fault inferred.
    condition: ConditionJson<'a>,
    /// The tagged human-ask axis: an actual human prompt, always emitted.
    human_ask: HumanAskJson,
    /// The conversation bridge, `null` when the record states nothing about one — which is not
    /// the same as `unsupported`.
    conversation_ref: Option<ConversationRefJson<'a>>,
}

impl<'a> ObservedJson<'a> {
    fn from_row(observed: Option<&'a harness_state::Observed>) -> Option<Self> {
        observed.map(|observed| ObservedJson {
            state: observed.state.as_str(),
            blocked_on: observed.blocked_on.as_str(),
            input_buffer: observed.input_buffer.as_str(),
            ask: observed.ask.as_str(),
            harness: observed.harness.as_deref(),
            since: observed.since_ms,
            reason: observed.reason.as_deref(),
            exit: observed.exit.as_deref(),
            schema: observed.schema.as_deref(),
            indeterminacy: observed
                .indeterminacy
                .as_ref()
                .map(|why| IndeterminacyJson {
                    reason: why.reason.as_str(),
                    evidence_age_ms: why.evidence_age_ms,
                }),
            condition: ConditionJson::from_view(&observed.condition),
            human_ask: HumanAskJson::from_axis(observed.human_ask),
            conversation_ref: observed
                .conversation
                .as_ref()
                .map(ConversationRefJson::from_ref),
        })
    }
}

/// Why an observation is indeterminate, typed for consumers that must distinguish a producer bug
/// from a stale seat.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IndeterminacyJson<'a> {
    reason: &'a str,
    /// `null` when the bytes carried no usable stamp, so "no age" and "age zero" stay distinct.
    evidence_age_ms: Option<u64>,
}

/// The condition axis. One field set for every arm, so a fault never disappears as a missing key
/// and an absent axis is never mistaken for a healthy one.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConditionJson<'a> {
    kind: &'static str,
    /// `null` for a fault whose category word is outside the closed set — untyped, still routed
    /// by `recovery` — and for the non-fault arms.
    category: Option<&'static str>,
    /// Provider-namespaced and open. Diagnostic: no consumer branches on it.
    code: Option<&'a str>,
    recovery: Option<&'static str>,
    observed_at_ms: Option<u64>,
    next_observation_due_ms: Option<u64>,
    detail: Option<&'a str>,
    /// An automatic recovery past its own deadline, decided at read time on the semantic clock.
    overdue: bool,
}

impl<'a> ConditionJson<'a> {
    fn from_view(condition: &'a harness_state::ConditionView) -> Self {
        let fault = condition.fault();
        Self {
            kind: condition.kind(),
            category: fault.and_then(|fault| {
                fault
                    .category
                    .map(harness_state::FaultCategory::as_str)
            }),
            code: fault.and_then(|fault| fault.code.as_deref()),
            recovery: fault.map(|fault| fault.recovery.as_str()),
            observed_at_ms: fault.map(|fault| fault.observed_at_ms),
            next_observation_due_ms: fault.and_then(|fault| fault.next_observation_due_ms),
            detail: fault.and_then(|fault| fault.detail.as_deref()),
            overdue: fault.is_some_and(|fault| fault.overdue),
        }
    }
}

/// The tagged ask axis: `none`, `pending` with its kind, or `unknown`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HumanAskJson {
    kind: &'static str,
    ask: Option<&'static str>,
}

impl HumanAskJson {
    fn from_axis(human_ask: harness_state::HumanAsk) -> Self {
        Self {
            kind: human_ask.kind(),
            ask: human_ask.pending().map(harness_state::AskKind::as_str),
        }
    }
}

/// The conversation bridge: identity and capability only. No conversation content rides this
/// projection, because none rides the record it reads.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConversationRefJson<'a> {
    kind: &'static str,
    driver: Option<&'a str>,
    /// The provider's own conversation identity, carried opaquely.
    conversation: Option<&'a str>,
    incarnation: Option<&'a str>,
    history_mutability: Option<&'static str>,
    capability_evidence: Option<&'static str>,
    /// The finite bound through which the link was verified.
    verified_through_ms: Option<u64>,
    /// The `unavailable` arm's diagnostic reason.
    reason: Option<&'a str>,
}

impl<'a> ConversationRefJson<'a> {
    fn from_ref(conversation: &'a harness_state::ConversationRef) -> Self {
        let link = conversation.link();
        Self {
            kind: conversation.kind(),
            driver: link.map(|link| link.driver.as_str()),
            conversation: link.map(|link| link.conversation.as_str()),
            incarnation: link.map(|link| link.incarnation.as_str()),
            history_mutability: link
                .map(|link| link.history_mutability.as_str()),
            capability_evidence: link
                .map(|link| link.capability_evidence.as_str()),
            verified_through_ms: link.map(|link| link.verified_through_ms),
            reason: match conversation {
                harness_state::ConversationRef::Unavailable(reason) => reason.as_deref(),
                harness_state::ConversationRef::Unsupported
                | harness_state::ConversationRef::Linked(_) => None,
            },
        }
    }
}

/// The shared derived disposition: exactly three closed axes, computed by
/// [`harness_state::disposition`] and by nothing else. It is a ROW-level sibling of
/// `observedState` rather than a member of it, because it folds two row-level axes — the observed
/// record and the native-driver diagnostic — and nesting it would assert it derives from the
/// record alone. Downstream consumers read this instead of re-deriving urgency; the raw axes ride
/// beside it so a consumer that disagrees can see exactly what was folded.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DispositionJson {
    state: &'static str,
    attention: &'static str,
    primary_action: &'static str,
}

impl DispositionJson {
    fn from_row(row: &AgentRow) -> Self {
        let disposition = harness_state::disposition(row.observed.as_ref(), &row.driver_diagnostic);
        Self {
            state: disposition.state.as_str(),
            attention: disposition.attention.as_str(),
            primary_action: disposition.primary_action.as_str(),
        }
    }
}

/// The closed `driverDiagnostic` object. Every state uses the same field set so a malformed,
/// unsupported, or absent record remains machine-visible rather than disappearing as a healthy
/// null/default.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DriverDiagnosticJson<'a> {
    status: &'static str,
    driver: Option<&'a str>,
    stage: Option<&'static str>,
    reason: Option<&'static str>,
    source: Option<&'static str>,
    producer_version: Option<&'a str>,
    support: &'static str,
    observed_at: Option<u64>,
    evidence_age_ms: Option<u64>,
    recovery: &'static str,
}

impl<'a> DriverDiagnosticJson<'a> {
    fn from_row(observed: &'a driver_diagnostic::Observed) -> Self {
        match observed {
            driver_diagnostic::Observed::Absent => Self {
                status: observed.status(),
                driver: None,
                stage: None,
                reason: None,
                source: None,
                producer_version: None,
                support: "unknown",
                observed_at: None,
                evidence_age_ms: None,
                recovery: "publishFailureOrClearOnStageRecovery",
            },
            driver_diagnostic::Observed::Indeterminate(reason) => Self {
                status: observed.status(),
                driver: None,
                stage: None,
                reason: Some(reason.as_str()),
                source: None,
                producer_version: None,
                support: "unknown",
                observed_at: None,
                evidence_age_ms: None,
                recovery: "replaceWithValidRecordOrClearOnStageRecovery",
            },
            driver_diagnostic::Observed::Failure(failure) => Self {
                status: observed.status(),
                driver: Some(failure.driver.as_str()),
                stage: Some(failure.stage.as_str()),
                reason: Some(failure.reason.as_str()),
                source: Some(failure.source.as_str()),
                producer_version: failure.producer_version.as_deref(),
                support: failure.support.as_str(),
                observed_at: Some(failure.observed_at),
                evidence_age_ms: Some(failure.evidence_age_ms),
                recovery: "clearsOnStageRecovery",
            },
        }
    }
}

/// The `context` object inside a roster row — the fourth top-level axis. The reading projection
/// is already applied, so a consumer never re-implements staleness and never sees a record it
/// would have to age itself. Vocabulary words are the record's own (`as_str`).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextJson<'a> {
    harness: &'static str,
    used_tokens: Option<u64>,
    window_tokens: Option<u64>,
    used_percent: Option<f64>,
    model: Option<&'a str>,
    cost_usd: Option<f64>,
    session_total_tokens: Option<u64>,
    rate_limits: harness_context::RateLimits,
    rate_limited: bool,
    compactions: u64,
    last_compaction_ms: Option<u64>,
    last_compaction_trigger: Option<&'static str>,
    /// Spelled `observedAtMs` on the wire, matching the record's own field and the
    /// `sinceMs`/`writtenAtMs` convention the driver records already use. `driverDiagnostic`'s
    /// unsuffixed `observedAt` is a different record's shipped name and is not touched here.
    observed_at_ms: u64,
    age_ms: u64,
    stale: bool,
}

impl<'a> ContextJson<'a> {
    fn from_row(context: Option<&'a harness_context::Observed>) -> Option<Self> {
        context.map(|context| ContextJson {
            harness: context.harness.as_str(),
            used_tokens: context.used_tokens,
            window_tokens: context.window_tokens,
            // Carried exactly as the harness published it: never clamped here, and never divided
            // out of the operands beside it.
            used_percent: context.used_percent,
            model: context.model.as_deref(),
            cost_usd: context.cost_usd,
            session_total_tokens: context.session_total_tokens,
            rate_limits: context.rate_limits,
            rate_limited: context.is_rate_limited(),
            compactions: context.compactions,
            last_compaction_ms: context.last_compaction_ms,
            last_compaction_trigger: context
                .last_compaction_trigger
                .map(harness_context::CompactionTrigger::as_str),
            observed_at_ms: context.observed_at_ms,
            age_ms: context.age_ms,
            stale: context.stale,
        })
    }
}

#[derive(Serialize)]
struct ResourceJson<'a> {
    name: &'a str,
    uri: &'a str,
    reason: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    inactive_reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selector: Option<&'a serde_json::Value>,
    resync: &'static str,
}

fn resource_json(row: &AgentRow) -> Vec<ResourceJson<'_>> {
    row.resources
        .iter()
        .zip(&row.resource_resync)
        .map(|(resource, coverage)| ResourceJson {
            name: resource.name(),
            uri: resource.uri(),
            reason: resource.reason(),
            inactive_reason: resource.inactive_reason(),
            selector: resource.selector(),
            resync: coverage.as_str(),
        })
        .collect()
}

/// `st2 agents --json` row. Field order and names are the stable wire contract: `identity` keeps
/// its meaning — the positional `<host>.<identity>` declaration key — and the immutable-ID axis is
/// appended rather than substituted, so an existing consumer keeps reading what it read before.
#[derive(Serialize)]
struct SummaryJson<'a> {
    identity: &'a str,
    status: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
    retired: bool,
    resources: Vec<ResourceJson<'a>>,
    #[serde(rename = "desiredState")]
    desired_state: &'a str,
    #[serde(rename = "desiredStateReason")]
    desired_state_reason: Option<&'a str>,
    #[serde(rename = "observedState")]
    observed_state: Option<ObservedJson<'a>>,
    #[serde(rename = "driverDiagnostic")]
    driver_diagnostic: DriverDiagnosticJson<'a>,
    context: Option<ContextJson<'a>>,
    id: &'a str,
    address: &'a str,
    /// `null` for a proved non-routable retired subject: it has released its address, which is
    /// different from having one nobody answered.
    #[serde(rename = "busAddress")]
    bus_address: Option<&'a str>,
    /// The shared derived disposition, appended after every shipped field. A row-level sibling of
    /// `observedState`, because it folds `observedState` AND `driverDiagnostic`.
    disposition: DispositionJson,
}

/// `st2 agents --json --enrich` row (adds `lastActivity` and `inbox`).
#[derive(Serialize)]
struct EnrichedJson<'a> {
    identity: &'a str,
    status: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
    retired: bool,
    resources: Vec<ResourceJson<'a>>,
    #[serde(rename = "lastActivity")]
    last_activity: Option<f64>,
    inbox: usize,
    #[serde(rename = "desiredState")]
    desired_state: &'a str,
    #[serde(rename = "desiredStateReason")]
    desired_state_reason: Option<&'a str>,
    #[serde(rename = "observedState")]
    observed_state: Option<ObservedJson<'a>>,
    #[serde(rename = "driverDiagnostic")]
    driver_diagnostic: DriverDiagnosticJson<'a>,
    context: Option<ContextJson<'a>>,
    id: &'a str,
    address: &'a str,
    #[serde(rename = "busAddress")]
    bus_address: Option<&'a str>,
    disposition: DispositionJson,
}

/// Serialize a roster to the stable JSON emitted by `st2 agents --json [--enrich]`.
pub fn to_json(rows: &[AgentRow], enrich: bool) -> String {
    if enrich {
        let out: Vec<EnrichedJson> = rows
            .iter()
            .map(|r| EnrichedJson {
                identity: &r.identity,
                status: r.status.as_str(),
                name: r.name.as_deref(),
                description: r.description.as_deref(),
                retired: r.retired,
                resources: resource_json(r),
                desired_state: &r.desired_state,
                desired_state_reason: r.desired_state_reason.as_deref(),
                last_activity: r.last_activity_ms,
                inbox: r.inbox,
                observed_state: ObservedJson::from_row(r.observed.as_ref()),
                driver_diagnostic: DriverDiagnosticJson::from_row(&r.driver_diagnostic),
                context: ContextJson::from_row(r.context.as_ref()),
                id: &r.id,
                address: &r.address,
                bus_address: r.bus_address.as_deref(),
                disposition: DispositionJson::from_row(r),
            })
            .collect();
        serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string())
    } else {
        let out: Vec<SummaryJson> = rows
            .iter()
            .map(|r| SummaryJson {
                identity: &r.identity,
                status: r.status.as_str(),
                name: r.name.as_deref(),
                description: r.description.as_deref(),
                retired: r.retired,
                resources: resource_json(r),
                desired_state: &r.desired_state,
                desired_state_reason: r.desired_state_reason.as_deref(),
                observed_state: ObservedJson::from_row(r.observed.as_ref()),
                driver_diagnostic: DriverDiagnosticJson::from_row(&r.driver_diagnostic),
                context: ContextJson::from_row(r.context.as_ref()),
                id: &r.id,
                address: &r.address,
                bus_address: r.bus_address.as_deref(),
                disposition: DispositionJson::from_row(r),
            })
            .collect();
        serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string())
    }
}
/// Runtime-only projection used by the versioned catalog graph. Declaration identity, lifecycle,
/// and topology stay on the graph row rather than being inferred from this observation.
pub(crate) fn graph_runtime_value(row: &AgentRow) -> serde_json::Value {
    serde_json::json!({
        "presence": row.status.as_str(),
        "lastActivityMs": row.last_activity_ms,
        "inbox": row.inbox,
        "observedState": ObservedJson::from_row(row.observed.as_ref()),
        "driverDiagnostic": DriverDiagnosticJson::from_row(&row.driver_diagnostic),
        // The fourth axis, unchanged and independent: a filling window survives every
        // `observedState: unknown` derivation, which is the wedge case it exists for.
        "context": ContextJson::from_row(row.context.as_ref()),
        // The same shared derivation the roster projects, from the same function: the graph must
        // not be a second opinion about urgency.
        "disposition": DispositionJson::from_row(row),
    })
}

/// Count logically unread messages in the agent's `resources/inbox`. A same-filename archive receipt
/// suppresses and cleans a raw inbox duplicate restored by eventually-consistent sync.
fn inbox_count(agent_dir: &Path) -> usize {
    message::list_inbox(&message::inbox_dir(agent_dir))
        .map(|msgs| msgs.len())
        .unwrap_or(0)
}

/// Newest activity time across status and message state. A version 1 status contributes its origin
/// timestamp. Inbox, archive, and legacy status retain their local-mtime behavior.
fn newest_activity_ms(agent_dir: &Path) -> Option<f64> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    for dir in [
        message::inbox_dir(agent_dir),
        message::archive_dir(agent_dir),
    ] {
        if let Ok(rd) = fs::read_dir(&dir) {
            candidates.extend(rd.flatten().map(|e| e.path()));
        }
    }
    let mut newest = status::activity_time_ms(&status::status_path(agent_dir));
    for p in candidates {
        if let Ok(m) = fs::metadata(&p)
            && let Ok(t) = m.modified()
            && let Ok(duration) = t.duration_since(UNIX_EPOCH)
        {
            let timestamp = duration.as_secs_f64() * 1000.0;
            if newest.is_none_or(|current| timestamp > current) {
                newest = Some(timestamp);
            }
        }
    }
    newest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        identity: &str,
        status: State,
        name: Option<&str>,
        retired: bool,
        last: Option<f64>,
        inbox: usize,
    ) -> AgentRow {
        AgentRow {
            identity: identity.to_string(),
            source_path: PathBuf::new(),
            status,
            name: name.map(str::to_string),
            description: None,
            retired,
            desired_state: if retired { "retired" } else { "running" }.to_owned(),
            desired_state_reason: None,
            resources: Vec::new(),
            resource_resync: Vec::new(),
            last_activity_ms: last,
            inbox,
            observed: None,
            driver_diagnostic: driver_diagnostic::Observed::Absent,
            context: None,
            // An unmigrated declaration's ID is exactly its positional key; its effective address
            // is the bare identity, and only a routable subject has a bus address at all.
            id: identity.to_string(),
            address: identity
                .split_once('.')
                .map_or(identity, |(_, bare)| bare)
                .to_string(),
            bus_address: (!retired).then(|| identity.to_string()),
        }
    }

    /// Field names, order, and null handling are stable (see INVARIANTS.md).
    #[test]
    fn agents_json_has_stable_wire_shape() {
        let rows = [
            row(
                "hetz.cos-claude",
                State::Available,
                None,
                false,
                Some(1784653027733.6138),
                1,
            ),
            row("hetz.st2-claude", State::Busy, Some("owner"), true, None, 0),
        ];

        assert_eq!(
            to_json(&rows, false),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.cos-claude","address":"cos-claude","busAddress":"hetz.cos-claude","disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"desiredState":"retired","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.st2-claude","address":"st2-claude","busAddress":null,"disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}}]"#
        );
        assert_eq!(
            to_json(&rows, true),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":1,"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.cos-claude","address":"cos-claude","busAddress":"hetz.cos-claude","disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"lastActivity":null,"inbox":0,"desiredState":"retired","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.st2-claude","address":"st2-claude","busAddress":null,"disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}}]"#
        );
        // Empty roster is `[]`, not `null`.
        assert_eq!(to_json(&[], true), "[]");
    }

    #[test]
    fn agents_json_preserves_opaque_declared_resource_descriptors() {
        let mut resource_row = row("hetz.worker", State::Available, None, false, None, 0);
        resource_row.resources.push(
            Resource::new(
                "work".into(),
                "vendor+thing://authority/exact%20identity".into(),
                "Current implementation task.".into(),
            )
            .unwrap(),
        );
        resource_row
            .resource_resync
            .push(crate::resync::ResyncCoverage::Unsupported);

        assert_eq!(
            to_json(&[resource_row], false),
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[{"name":"work","uri":"vendor+thing://authority/exact%20identity","reason":"Current implementation task.","resync":"unsupported"}],"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.worker","address":"worker","busAddress":"hetz.worker","disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}}]"#
        );
    }

    /// Declared presence and observed harness state are independent axes in one payload: a
    /// declared `busy` sits beside an observed `idle` (the wedged-agent signal), and
    /// `lastActivity` keeps its existing meaning untouched by the new field.
    #[test]
    fn observed_state_joins_declared_presence_without_touching_either() {
        let mut wedged = row(
            "hetz.worker",
            State::Busy,
            None,
            false,
            Some(1784653027733.6138),
            0,
        );
        wedged.observed = Some(harness_state::Observed {
            subject: Some(harness_state::RecordSubject::BusIdentity(
                "hetz.worker".into(),
            )),
            state: harness_state::Activity::Idle,
            blocked_on: harness_state::BlockedOn::None,
            input_buffer: harness_state::InputBuffer::Empty,
            ask: harness_state::Ask::None,
            harness: Some("codex".to_string()),
            since_ms: Some(1784653000000),
            exit: None,
            reason: None,
            schema: Some(harness_state::SCHEMA_V1.to_string()),
            indeterminacy: None,
            // Versions 1 and 2 carry no condition axis: explicitly absent, never
            // `clear`, and no fault inferred from their legacy words.
            condition: harness_state::ConditionView::Absent,
            human_ask: harness_state::HumanAsk::None,
            conversation: None,
        });

        assert_eq!(
            to_json(&[wedged.clone()], false),
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null,"schema":"st2.harness-state.v1","indeterminacy":null,"condition":{"kind":"absent","category":null,"code":null,"recovery":null,"observedAtMs":null,"nextObservationDueMs":null,"detail":null,"overdue":false},"humanAsk":{"kind":"none","ask":null},"conversationRef":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.worker","address":"worker","busAddress":"hetz.worker","disposition":{"state":"idle","attention":"none","primaryAction":"none"}}]"#
        );
        assert_eq!(
            to_json(&[wedged], true),
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":0,"desiredState":"running","desiredStateReason":null,"observedState":{"state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null,"schema":"st2.harness-state.v1","indeterminacy":null,"condition":{"kind":"absent","category":null,"code":null,"recovery":null,"observedAtMs":null,"nextObservationDueMs":null,"detail":null,"overdue":false},"humanAsk":{"kind":"none","ask":null},"conversationRef":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.worker","address":"worker","busAddress":"hetz.worker","disposition":{"state":"idle","attention":"none","primaryAction":"none"}}]"#
        );

        let mut derived = row("hetz.worker", State::Available, None, false, None, 0);
        derived.observed = Some(harness_state::Observed {
            subject: None,
            state: harness_state::Activity::Unknown,
            blocked_on: harness_state::BlockedOn::Unknown,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::Unknown,
            harness: Some("codex".to_string()),
            since_ms: None,
            exit: None,
            reason: Some("session-dead".to_string()),
            schema: Some(harness_state::SCHEMA_V1.to_string()),
            indeterminacy: Some(harness_state::Indeterminacy {
                reason: "session-dead".to_string(),
                evidence_age_ms: Some(4_210),
            }),
            // Versions 1 and 2 carry no condition axis: explicitly absent, never
            // `clear`, and no fault inferred from their legacy words.
            condition: harness_state::ConditionView::Absent,
            human_ask: harness_state::HumanAsk::Unknown,
            conversation: None,
        });
        assert_eq!(
            to_json(&[derived], false),
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"state":"unknown","blockedOn":"unknown","inputBuffer":"unknown","ask":"unknown","harness":"codex","since":null,"reason":"session-dead","exit":null,"schema":"st2.harness-state.v1","indeterminacy":{"reason":"session-dead","evidenceAgeMs":4210},"condition":{"kind":"absent","category":null,"code":null,"recovery":null,"observedAtMs":null,"nextObservationDueMs":null,"detail":null,"overdue":false},"humanAsk":{"kind":"unknown","ask":null},"conversationRef":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"},"context":null,"id":"hetz.worker","address":"worker","busAddress":"hetz.worker","disposition":{"state":"unknown","attention":"none","primaryAction":"observe"}}]"#
        );
    }

    /// HC-R14/HC-R07: `context` is a fourth top-level axis, always emitted and `null` when no
    /// record exists — and it survives an `observedState` of `unknown`, which is precisely the
    /// wedge case it exists for: a runtime at 190k of a 200k window whose state has gone
    /// indeterminate. Nothing here derives one axis from another.
    #[test]
    fn context_is_a_fourth_axis_that_survives_an_indeterminate_observed_state() {
        let mut wedged = row("hetz.worker", State::Busy, None, false, None, 0);
        wedged.observed = Some(harness_state::Observed {
            subject: None,
            state: harness_state::Activity::Unknown,
            blocked_on: harness_state::BlockedOn::Unknown,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::Unknown,
            harness: Some("codex".to_string()),
            since_ms: None,
            exit: None,
            reason: Some("session-dead".to_string()),
            schema: Some(harness_state::SCHEMA_V1.to_string()),
            indeterminacy: Some(harness_state::Indeterminacy {
                reason: "session-dead".to_string(),
                evidence_age_ms: Some(4_210),
            }),
            // Versions 1 and 2 carry no condition axis: explicitly absent, never
            // `clear`, and no fault inferred from their legacy words.
            condition: harness_state::ConditionView::Absent,
            human_ask: harness_state::HumanAsk::Unknown,
            conversation: None,
        });
        wedged.context = Some(harness_context::Observed {
            subject: harness_state::RecordSubject::BusIdentity("hetz.worker".into()),
            harness: harness_context::Harness::Codex,
            used_tokens: Some(92283),
            window_tokens: Some(258400),
            used_percent: Some(33.0),
            model: None,
            cost_usd: None,
            session_total_tokens: Some(2235329),
            rate_limits: harness_context::RateLimits {
                five_hour: Some(31.0),
                seven_day: Some(55.0),
            },
            compactions: 3,
            last_compaction_ms: Some(1788000097290),
            last_compaction_trigger: Some(harness_context::CompactionTrigger::Unknown),
            observed_at_ms: 1788000100000,
            age_ms: 4210,
            stale: false,
        });

        let wire: serde_json::Value =
            serde_json::from_str(&to_json(&[wedged.clone()], false)).unwrap();
        assert_eq!(
            wire[0]["context"],
            serde_json::json!({
                "harness": "codex",
                "usedTokens": 92283,
                "windowTokens": 258400,
                "usedPercent": 33.0,
                "model": null,
                "costUsd": null,
                "sessionTotalTokens": 2235329,
                "rateLimits": {"fiveHour": 31.0, "sevenDay": 55.0},
                "rateLimited": false,
                "compactions": 3,
                "lastCompactionMs": 1788000097290u64,
                "lastCompactionTrigger": "unknown",
                "observedAtMs": 1788000100000u64,
                "ageMs": 4210,
                "stale": false
            })
        );
        // The other three axes are untouched by it, and it by them.
        assert_eq!(wire[0]["status"], "busy");
        assert_eq!(wire[0]["observedState"]["state"], "unknown");
        assert_eq!(wire[0]["driverDiagnostic"]["status"], "absent");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&to_json(&[wedged], true)).unwrap()[0]["context"]
                ["usedPercent"],
            33.0
        );

        // A percent above the window rides the wire raw, and a stale reading keeps its age.
        let mut overrun = row("hetz.pi", State::Available, None, false, None, 0);
        overrun.context = Some(harness_context::Observed {
            subject: harness_state::RecordSubject::BusIdentity("hetz.pi".into()),
            harness: harness_context::Harness::Pi,
            used_tokens: Some(23424),
            window_tokens: Some(4000),
            used_percent: Some(585.6),
            model: Some("pi-model".to_string()),
            cost_usd: Some(0.42),
            session_total_tokens: None,
            rate_limits: harness_context::RateLimits::default(),
            compactions: 0,
            last_compaction_ms: None,
            last_compaction_trigger: None,
            observed_at_ms: 1788000100000,
            age_ms: 7_200_000,
            stale: true,
        });
        let wire: serde_json::Value = serde_json::from_str(&to_json(&[overrun], false)).unwrap();
        assert_eq!(wire[0]["context"]["usedPercent"], 585.6);
        assert_eq!(wire[0]["context"]["stale"], true);
        assert_eq!(wire[0]["context"]["ageMs"], 7_200_000);
        assert_eq!(
            wire[0]["context"]["lastCompactionTrigger"],
            serde_json::Value::Null
        );
        assert_eq!(
            wire[0]["context"]["rateLimits"]["fiveHour"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn exhausted_claude_rate_limit_is_explicit_beside_active_state() {
        let mut limited = row("hetz.worker", State::Available, None, false, None, 0);
        limited.observed = Some(harness_state::Observed {
            subject: Some(harness_state::RecordSubject::BusIdentity(
                "hetz.worker".into(),
            )),
            state: harness_state::Activity::Active,
            blocked_on: harness_state::BlockedOn::None,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::None,
            harness: Some("claude".to_string()),
            since_ms: Some(1788000100000),
            exit: None,
            reason: None,
            schema: Some(harness_state::SCHEMA_V1.to_string()),
            indeterminacy: None,
            // Versions 1 and 2 carry no condition axis: explicitly absent, never
            // `clear`, and no fault inferred from their legacy words.
            condition: harness_state::ConditionView::Absent,
            human_ask: harness_state::HumanAsk::None,
            conversation: None,
        });
        limited.context = Some(harness_context::Observed {
            subject: harness_state::RecordSubject::BusIdentity("hetz.worker".into()),
            harness: harness_context::Harness::Claude,
            used_tokens: Some(194_763),
            window_tokens: Some(1_000_000),
            used_percent: Some(19.0),
            model: Some("claude-opus-5".to_string()),
            cost_usd: Some(4.7312),
            session_total_tokens: None,
            rate_limits: harness_context::RateLimits {
                five_hour: Some(100.0),
                seven_day: Some(55.0),
            },
            compactions: 0,
            last_compaction_ms: None,
            last_compaction_trigger: None,
            observed_at_ms: 1788000100000,
            age_ms: 4210,
            stale: false,
        });

        let wire: serde_json::Value =
            serde_json::from_str(&to_json(&[limited.clone()], false)).unwrap();
        assert_eq!(wire[0]["observedState"]["state"], "active");
        assert_eq!(wire[0]["context"]["rateLimits"]["fiveHour"], 100.0);
        assert_eq!(wire[0]["context"]["rateLimited"], true);

        limited.context.as_mut().unwrap().stale = true;
        let stale: serde_json::Value = serde_json::from_str(&to_json(&[limited], false)).unwrap();
        assert_eq!(stale[0]["context"]["rateLimited"], false);
    }

    #[test]
    fn driver_diagnostic_wire_exposes_failure_and_evidence_age_without_identity_payloads() {
        let mut diagnosed = row("hetz.worker", State::Available, None, false, None, 0);
        diagnosed.driver_diagnostic =
            driver_diagnostic::Observed::Failure(driver_diagnostic::Failure {
                driver: driver_diagnostic::Driver::OpenCode,
                stage: driver_diagnostic::Stage::ReadBack,
                reason: driver_diagnostic::Reason::NotDurable,
                source: driver_diagnostic::Source::MessageReadBack,
                producer_version: Some("1.18.19".to_string()),
                support: driver_diagnostic::Support::Supported,
                observed_at: 100,
                evidence_age_ms: 25,
            });
        let wire: serde_json::Value = serde_json::from_str(&to_json(&[diagnosed], false)).unwrap();
        assert_eq!(
            wire[0]["driverDiagnostic"],
            serde_json::json!({
                "status": "failure",
                "driver": "opencode",
                "stage": "readBack",
                "reason": "notDurable",
                "source": "messageReadBack",
                "producerVersion": "1.18.19",
                "support": "supported",
                "observedAt": 100,
                "evidenceAgeMs": 25,
                "recovery": "clearsOnStageRecovery"
            })
        );
        let rendered = wire[0]["driverDiagnostic"].to_string();
        for forbidden in ["prompt", "body", "filename", "sessionId", "messageId"] {
            assert!(!rendered.contains(forbidden), "{rendered}");
        }
    }

    /// The version 3 axes ride the roster wire beside the shipped fields, and the shared
    /// disposition is a ROW-level sibling computed by `harness_state::disposition` — never a
    /// second opinion assembled here. The pinned case is the hard one: a fault and a human ask at
    /// the same time, where remediation is primary and the ask must stay visible.
    #[test]
    fn the_fault_axis_and_the_shared_disposition_ride_the_roster_wire() {
        let mut faulted = row("hetz.worker", State::Busy, None, false, None, 0);
        faulted.observed = Some(harness_state::Observed {
            state: harness_state::Activity::Active,
            // The legacy pair is the projection of the tagged axis, not an independent claim.
            blocked_on: harness_state::BlockedOn::Human,
            input_buffer: harness_state::InputBuffer::Empty,
            ask: harness_state::Ask::Permission,
            harness: Some("codex".to_string()),
            since_ms: Some(1_788_000_000_000),
            exit: None,
            reason: None,
            subject: Some(harness_state::RecordSubject::AgentId(
                "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1".into(),
            )),
            schema: Some(harness_state::SCHEMA_V3.to_string()),
            indeterminacy: None,
            condition: harness_state::ConditionView::Fault(harness_state::Fault {
                category: Some(harness_state::FaultCategory::Quota),
                code: Some("codex/usage_limit_reached".to_string()),
                recovery: harness_state::Recovery::Human,
                observed_at_ms: 1_788_000_050_000,
                next_observation_due_ms: None,
                detail: Some("the five-hour window is exhausted".to_string()),
                overdue: false,
            }),
            human_ask: harness_state::HumanAsk::Pending(harness_state::AskKind::Permission),
            conversation: Some(harness_state::ConversationRef::Linked(
                harness_state::ConversationLink {
                    driver: "codex".to_string(),
                    conversation: "thread_01JXPLACEHOLDER".to_string(),
                    incarnation: "session-1".to_string(),
                    history_mutability: harness_state::HistoryMutability::Rewritable,
                    capability_evidence: harness_state::CapabilityEvidence::Probed,
                    verified_through_ms: 1_788_000_050_000,
                },
            )),
        });

        let wire: serde_json::Value =
            serde_json::from_str(&to_json(&[faulted.clone()], false)).unwrap();
        assert_eq!(
            wire[0]["observedState"]["condition"],
            serde_json::json!({
                "kind": "fault",
                "category": "quota",
                "code": "codex/usage_limit_reached",
                "recovery": "human",
                "observedAtMs": 1_788_000_050_000u64,
                "nextObservationDueMs": null,
                "detail": "the five-hour window is exhausted",
                "overdue": false
            })
        );
        assert_eq!(
            wire[0]["observedState"]["humanAsk"],
            serde_json::json!({"kind": "pending", "ask": "permission"})
        );
        assert_eq!(
            wire[0]["observedState"]["conversationRef"],
            serde_json::json!({
                "kind": "linked",
                "driver": "codex",
                "conversation": "thread_01JXPLACEHOLDER",
                "incarnation": "session-1",
                "historyMutability": "rewritable",
                "capabilityEvidence": "probed",
                "verifiedThroughMs": 1_788_000_050_000u64,
                "reason": null
            })
        );
        assert_eq!(
            wire[0]["observedState"]["schema"], "st2.harness-state.v3",
            "the exact version rides the wire so a drain gate can be positive"
        );
        // The shipped fields are untouched, and the ask stays visible beside the fault.
        assert_eq!(wire[0]["observedState"]["state"], "active");
        assert_eq!(wire[0]["observedState"]["blockedOn"], "human");
        assert_eq!(wire[0]["observedState"]["ask"], "permission");
        assert_eq!(wire[0]["observedState"]["indeterminacy"], serde_json::Value::Null);
        // Remediation is primary while the fault stands; declared presence is untouched by it.
        assert_eq!(
            wire[0]["disposition"],
            serde_json::json!({"state": "failed", "attention": "now", "primaryAction": "remediate"})
        );
        assert_eq!(wire[0]["status"], "busy");

        // The graph projects the same derivation from the same function, and `--enrich` too. The
        // appended axis must not displace one: the graph runtime keeps every key it carried, so
        // `context` — the axis that survives an indeterminate observation — is pinned here beside
        // the new one.
        faulted.context = Some(harness_context::Observed {
            subject: harness_state::RecordSubject::AgentId(
                "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1".into(),
            ),
            harness: harness_context::Harness::Codex,
            used_tokens: Some(92_283),
            window_tokens: Some(258_400),
            used_percent: Some(36.0),
            model: None,
            cost_usd: None,
            session_total_tokens: None,
            rate_limits: harness_context::RateLimits::default(),
            compactions: 0,
            last_compaction_ms: None,
            last_compaction_trigger: None,
            observed_at_ms: 1_788_000_050_000,
            age_ms: 4_210,
            stale: false,
        });
        let runtime = graph_runtime_value(&faulted);
        assert_eq!(runtime["disposition"], wire[0]["disposition"]);
        assert_eq!(runtime["context"]["usedPercent"], 36.0);
        assert_eq!(runtime["observedState"]["condition"]["kind"], "fault");
        assert_eq!(runtime["driverDiagnostic"]["status"], "absent");
        assert_eq!(runtime["presence"], "busy");
        assert_eq!(runtime["inbox"], 0);
        let enriched: serde_json::Value =
            serde_json::from_str(&to_json(&[faulted], true)).unwrap();
        assert_eq!(enriched[0]["disposition"], wire[0]["disposition"]);

        // A row nobody has ever observed is not a state: unknown, non-paging, worth observing.
        let quiet = row("hetz.quiet", State::Available, None, false, None, 0);
        let wire: serde_json::Value = serde_json::from_str(&to_json(&[quiet], false)).unwrap();
        assert_eq!(
            wire[0]["disposition"],
            serde_json::json!({"state": "unknown", "attention": "none", "primaryAction": "observe"})
        );
    }
}
