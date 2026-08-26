//! The agent roster (M2.3): the data behind `st2 agents`. Enumerates the catalog's agents with their
//! presence status and retirement state, and optionally last-activity + inbox count. The JSON field
//! names, order, and null handling are a stable machine-readable contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::message;
use crate::status::{self, State};
use crate::{AgentSpec, Discovered, Resource, harness_state};

/// A PTY that emitted output inside this window is running a turn. Maintained
/// harnesses stream tokens or redraw progress continuously while active and go
/// silent between turns, so one minute leaves wide margin on both sides.
///
/// Deliberately not an alias of presence or driver-record freshness: this is a
/// read-time session-activity projection with its own evidence and semantics.
const SESSION_ACTIVE_WINDOW_MS: u64 = 60_000;
const SESSION_FUTURE_SKEW_MS: u64 = 30_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PtySessionMetadata {
    last_output_at_ms: Option<u64>,
}

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
    let now_ms = unix_ms_now();
    let mut rows: Vec<AgentRow> = found
        .specs
        .iter()
        .filter_map(|s| {
            let agent_dir = s.path.parent()?;
            let bus_id = s.bus_id(this_host);
            Some(AgentRow {
                identity: bus_id.clone(),
                status: status::read_state(&status::status_path(agent_dir)),
                name: s.name.clone(),
                description: s.description.clone(),
                retired: s.desired_state.is_retired(),
                desired_state: s.desired_state.as_str().to_owned(),
                desired_state_reason: s.desired_state.reason().map(str::to_owned),
                resources: s.resources.clone(),
                last_activity_ms: newest_activity_ms(agent_dir),
                inbox: inbox_count(agent_dir),
                observed: observed_state_at(s, agent_dir, &pty_root, this_host, &bus_id, now_ms),
            })
        })
        .collect();
    rows.sort_by(|a, b| a.identity.cmp(&b.identity));
    rows
}

/// Read the same composed observation exposed by the roster, for consumers
/// such as Doctor that already hold one discovered Agent Spec.
pub fn read_observed_state(
    spec: &AgentSpec,
    agent_dir: &Path,
    pty_root: &Path,
    this_host: &str,
) -> Option<harness_state::Observed> {
    let bus_id = spec.bus_id(this_host);
    observed_state_at(spec, agent_dir, pty_root, this_host, &bus_id, unix_ms_now())
}

/// Compose the rich driver record with launcher-agnostic PTY session activity.
/// A definite, fresh driver record wins. A missing or indeterminate driver
/// record falls back to session activity on the local host. Cross-host readers
/// have neither the remote PTY registry nor its activity stamp, so they retain
/// the replicated driver record unchanged.
fn observed_state_at(
    spec: &AgentSpec,
    agent_dir: &Path,
    pty_root: &Path,
    this_host: &str,
    bus_id: &str,
    now_ms: u64,
) -> Option<harness_state::Observed> {
    let path = harness_state::harness_state_path(agent_dir);
    if spec.resolved_host(this_host) != this_host {
        return harness_state::read(&path, None);
    }

    let probe = |session: &str| crate::ding::session_liveness_in(pty_root, session);
    let driver = harness_state::read(&path, Some(&probe));
    let session = session_observation(pty_root, bus_id, now_ms);
    compose_observations(driver, session)
}

fn compose_observations(
    driver: Option<harness_state::Observed>,
    session: Option<harness_state::Observed>,
) -> Option<harness_state::Observed> {
    if driver
        .as_ref()
        .is_some_and(|observed| observed.state != harness_state::Activity::Unknown)
    {
        driver
    } else {
        session.or(driver)
    }
}

/// Read coarse activity from the canonical agent task's PTY session. The
/// runner pins that session id to the agent bus id (`reconcile`'s canonical
/// agent rule), so the mapping is computed and requires no launcher knowledge.
fn session_observation(
    pty_root: &Path,
    bus_id: &str,
    now_ms: u64,
) -> Option<harness_state::Observed> {
    if crate::ding::session_liveness_in(pty_root, bus_id) != harness_state::SessionLiveness::Alive {
        return None;
    }
    let metadata: PtySessionMetadata =
        serde_json::from_slice(&fs::read(pty_root.join(format!("{bus_id}.json"))).ok()?).ok()?;
    let last_output_at_ms = metadata.last_output_at_ms?;
    let future_skew = last_output_at_ms.saturating_sub(now_ms);
    let (state, reason) = if future_skew > SESSION_FUTURE_SKEW_MS {
        (
            harness_state::Activity::Unknown,
            Some("pty-future-skew".to_owned()),
        )
    } else if now_ms.saturating_sub(last_output_at_ms) <= SESSION_ACTIVE_WINDOW_MS {
        (harness_state::Activity::Active, None)
    } else {
        (harness_state::Activity::Idle, None)
    };
    Some(harness_state::Observed {
        fidelity: harness_state::Fidelity::Session,
        state,
        blocked_on: harness_state::BlockedOn::Unknown,
        input_buffer: harness_state::InputBuffer::Unknown,
        ask: harness_state::Ask::Unknown,
        harness: None,
        since_ms: Some(last_output_at_ms),
        exit: None,
        reason,
    })
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
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
    fidelity: &'a str,
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
            fidelity: observed.fidelity.as_str(),
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
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":null},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"desiredState":"retired","desiredStateReason":null,"observedState":null}]"#
        );
        assert_eq!(
            to_json(&rows, true),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":1,"desiredState":"running","desiredStateReason":null,"observedState":null},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"lastActivity":null,"inbox":0,"desiredState":"retired","desiredStateReason":null,"observedState":null}]"#
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
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[{"name":"work","uri":"vendor+thing://authority/exact%20identity","reason":"Current implementation task."}],"desiredState":"running","desiredStateReason":null,"observedState":null}]"#
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
            fidelity: harness_state::Fidelity::Driver,
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
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"fidelity":"driver","state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null}}]"#
        );
        assert_eq!(
            to_json(&[wedged], true),
            r#"[{"identity":"hetz.worker","status":"busy","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":0,"desiredState":"running","desiredStateReason":null,"observedState":{"fidelity":"driver","state":"idle","blockedOn":"none","inputBuffer":"empty","ask":"none","harness":"codex","since":1784653000000,"reason":null,"exit":null}}]"#
        );

        let mut derived = row("hetz.worker", State::Available, None, false, None, 0);
        derived.observed = Some(harness_state::Observed {
            fidelity: harness_state::Fidelity::Driver,
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
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null,"observedState":{"fidelity":"driver","state":"unknown","blockedOn":"unknown","inputBuffer":"unknown","ask":"unknown","harness":"codex","since":null,"reason":"session-dead","exit":null}}]"#
        );
    }

    fn write_pty_session(root: &Path, bus_id: &str, last_output_at_ms: Option<u64>) {
        fs::write(
            root.join(format!("{bus_id}.pid")),
            std::process::id().to_string(),
        )
        .unwrap();
        let metadata = match last_output_at_ms {
            Some(timestamp) => serde_json::json!({ "lastOutputAtMs": timestamp }),
            None => serde_json::json!({}),
        };
        fs::write(
            root.join(format!("{bus_id}.json")),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn session_activity_projects_active_idle_and_future_skew() {
        let root = tempfile::tempdir().unwrap();
        let bus_id = "dev3.worker";
        let now = 1_800_000_000_000;

        write_pty_session(root.path(), bus_id, Some(now - 500));
        let active = session_observation(root.path(), bus_id, now).unwrap();
        assert_eq!(active.fidelity, harness_state::Fidelity::Session);
        assert_eq!(active.state, harness_state::Activity::Active);
        assert_eq!(active.blocked_on, harness_state::BlockedOn::Unknown);
        assert_eq!(active.since_ms, Some(now - 500));

        write_pty_session(
            root.path(),
            bus_id,
            Some(now - SESSION_ACTIVE_WINDOW_MS - 1),
        );
        let idle = session_observation(root.path(), bus_id, now).unwrap();
        assert_eq!(idle.state, harness_state::Activity::Idle);
        assert_eq!(idle.reason, None);

        write_pty_session(root.path(), bus_id, Some(now + SESSION_FUTURE_SKEW_MS + 1));
        let skewed = session_observation(root.path(), bus_id, now).unwrap();
        assert_eq!(skewed.state, harness_state::Activity::Unknown);
        assert_eq!(skewed.reason.as_deref(), Some("pty-future-skew"));
    }

    #[test]
    fn session_activity_requires_both_liveness_and_an_output_stamp() {
        let root = tempfile::tempdir().unwrap();
        let bus_id = "dev3.worker";
        write_pty_session(root.path(), bus_id, None);
        assert_eq!(session_observation(root.path(), bus_id, 10_000), None);

        fs::write(root.path().join(format!("{bus_id}.pid")), "0").unwrap();
        fs::write(
            root.path().join(format!("{bus_id}.json")),
            r#"{"lastOutputAtMs":9999}"#,
        )
        .unwrap();
        assert_eq!(session_observation(root.path(), bus_id, 10_000), None);
    }

    #[test]
    fn fresh_driver_state_wins_and_indeterminate_driver_falls_back_to_session() {
        let driver = harness_state::Observed {
            fidelity: harness_state::Fidelity::Driver,
            state: harness_state::Activity::Idle,
            blocked_on: harness_state::BlockedOn::None,
            input_buffer: harness_state::InputBuffer::Empty,
            ask: harness_state::Ask::None,
            harness: Some("codex".to_owned()),
            since_ms: Some(10),
            exit: None,
            reason: None,
        };
        let session = harness_state::Observed {
            fidelity: harness_state::Fidelity::Session,
            state: harness_state::Activity::Active,
            blocked_on: harness_state::BlockedOn::Unknown,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::Unknown,
            harness: None,
            since_ms: Some(20),
            exit: None,
            reason: None,
        };

        assert_eq!(
            compose_observations(Some(driver.clone()), Some(session.clone())),
            Some(driver)
        );

        let indeterminate = harness_state::Observed {
            fidelity: harness_state::Fidelity::Driver,
            state: harness_state::Activity::Unknown,
            blocked_on: harness_state::BlockedOn::Unknown,
            input_buffer: harness_state::InputBuffer::Unknown,
            ask: harness_state::Ask::Unknown,
            harness: Some("codex".to_owned()),
            since_ms: None,
            exit: None,
            reason: Some("stale".to_owned()),
        };
        assert_eq!(
            compose_observations(Some(indeterminate), Some(session.clone())),
            Some(session)
        );
    }
}
