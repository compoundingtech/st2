//! The agent roster (M2.3): the data behind `st2 agents`. Enumerates the catalog's agents with their
//! presence status and retirement state, and optionally last-activity + inbox count. The JSON field
//! names, order, and null handling are a stable machine-readable contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::Serialize;

use crate::message;
use crate::status::{self, State};
use crate::{AgentSpec, Discovered, Resource, driver_diagnostic, harness_state};

/// One roster row: everything `st2 agents [--enrich]` can report about an agent.
#[derive(Debug, Clone)]
pub struct AgentRow {
    /// The bus id — `<host>.<identity>`.
    pub identity: String,
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
}

/// Every agent in the catalog, sorted by bus id, with presence + enrich data computed. Read-only:
/// walks discovered specs and each agent's resources, mutating nothing.
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
    let mut rows: Vec<AgentRow> = found
        .specs
        .iter()
        .filter_map(|s| {
            let agent_dir = s.path.parent()?;
            Some(AgentRow {
                identity: s.bus_id(this_host),
                status: status::read_state(&status::status_path(agent_dir)),
                name: s.name.clone(),
                description: s.description.clone(),
                retired: s.desired_state.is_retired(),
                desired_state: s.desired_state.as_str().to_owned(),
                desired_state_reason: s.desired_state.reason().map(str::to_owned),
                resources: s.resources.clone(),
                last_activity_ms: newest_activity_ms(agent_dir),
                inbox: inbox_count(agent_dir),
                observed: observed_state(s, agent_dir, &pty_root, this_host),
                driver_diagnostic: driver_diagnostic::read(&driver_diagnostic::path(agent_dir)),
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
/// (`as_str`), never Rust identifier spellings.
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
        })
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

/// `st2 agents --json` row. Field order and names are the stable wire contract.
#[derive(Serialize)]
struct SummaryJson<'a> {
    identity: &'a str,
    status: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
    retired: bool,
    resources: &'a [Resource],
    #[serde(rename = "desiredState")]
    desired_state: &'a str,
    #[serde(rename = "desiredStateReason")]
    desired_state_reason: Option<&'a str>,
    #[serde(rename = "observedState")]
    observed_state: Option<ObservedJson<'a>>,
    #[serde(rename = "driverDiagnostic")]
    driver_diagnostic: DriverDiagnosticJson<'a>,
}

/// `st2 agents --json --enrich` row (adds `lastActivity` and `inbox`).
#[derive(Serialize)]
struct EnrichedJson<'a> {
    identity: &'a str,
    status: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
    retired: bool,
    resources: &'a [Resource],
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
                resources: &r.resources,
                desired_state: &r.desired_state,
                desired_state_reason: r.desired_state_reason.as_deref(),
                last_activity: r.last_activity_ms,
                inbox: r.inbox,
                observed_state: ObservedJson::from_row(r.observed.as_ref()),
                driver_diagnostic: DriverDiagnosticJson::from_row(&r.driver_diagnostic),
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
                resources: &r.resources,
                desired_state: &r.desired_state,
                desired_state_reason: r.desired_state_reason.as_deref(),
                observed_state: ObservedJson::from_row(r.observed.as_ref()),
                driver_diagnostic: DriverDiagnosticJson::from_row(&r.driver_diagnostic),
            })
            .collect();
        serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string())
    }
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
            status,
            name: name.map(str::to_string),
            description: None,
            retired,
            desired_state: if retired { "retired" } else { "running" }.to_owned(),
            desired_state_reason: None,
            resources: Vec::new(),
            last_activity_ms: last,
            inbox,
            observed: None,
            driver_diagnostic: driver_diagnostic::Observed::Absent,
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
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"desiredState":"retired","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
        );
        assert_eq!(
            to_json(&rows, true),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":1,"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"lastActivity":null,"inbox":0,"desiredState":"retired","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
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

        assert_eq!(
            to_json(&[resource_row], false),
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[{"name":"work","uri":"vendor+thing://authority/exact%20identity","reason":"Current implementation task."}],"desiredState":"running","desiredStateReason":null,"observedState":null,"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
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
            state: harness_state::Activity::Idle,
            blocked_on: harness_state::BlockedOn::None,
            input_buffer: harness_state::InputBuffer::Empty,
            ask: harness_state::Ask::None,
            harness: Some("codex".to_string()),
            since_ms: Some(1784653000000),
            exit: None,
            reason: None,
        });

        assert_eq!(
            to_json(&[wedged.clone()], false),
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
        );
        assert_eq!(
            to_json(&[wedged], true),
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":0,"desiredState":"running","desiredStateReason":null,"observedState":{"state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
        );

        let mut derived = row("hetz.worker", State::Available, None, false, None, 0);
        derived.observed = Some(harness_state::Observed {
            state: harness_state::Activity::Unknown,
            blocked_on: harness_state::BlockedOn::Unknown,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::Unknown,
            harness: Some("codex".to_string()),
            since_ms: None,
            exit: None,
            reason: Some("session-dead".to_string()),
        });
        assert_eq!(
            to_json(&[derived], false),
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"state":"unknown","blockedOn":"unknown","inputBuffer":"unknown","ask":"unknown","harness":"codex","since":null,"reason":"session-dead","exit":null},"driverDiagnostic":{"status":"absent","driver":null,"stage":null,"reason":null,"source":null,"producerVersion":null,"support":"unknown","observedAt":null,"evidenceAgeMs":null,"recovery":"publishFailureOrClearOnStageRecovery"}}]"#
        );
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
}
