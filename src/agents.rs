//! The agent roster (M2.3): the data behind `st2 agents`. Enumerates the catalog's agents with their
//! presence status and retirement state, and optionally last-activity + inbox count. The JSON field
//! names, order, and null handling are a stable machine-readable contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::Serialize;

use crate::message;
use crate::status::{self, State};
use crate::{Discovered, Resource};

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
}

/// Every agent in the catalog, sorted by bus id, with presence + enrich data computed. Read-only:
/// walks discovered specs and each agent's resources, mutating nothing.
pub fn roster(catalog_root: &Path, this_host: &str) -> Vec<AgentRow> {
    let found = crate::discover(catalog_root);
    roster_from_discovered(&found, this_host)
}

/// Project a roster from one immutable discovery result. Exact selectors use this after proving
/// discovery complete so the uniqueness check and returned metadata describe the same snapshot.
pub fn roster_from_discovered(found: &Discovered, this_host: &str) -> Vec<AgentRow> {
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
            })
        })
        .collect();
    rows.sort_by(|a, b| a.identity.cmp(&b.identity));
    rows
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
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"desiredState":"running","desiredStateReason":null},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"desiredState":"retired","desiredStateReason":null}]"#
        );
        assert_eq!(
            to_json(&rows, true),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"description":null,"retired":false,"resources":[],"lastActivity":1784653027733.6138,"inbox":1,"desiredState":"running","desiredStateReason":null},{"identity":"hetz.st2-claude","status":"busy","name":"owner","description":null,"retired":true,"resources":[],"lastActivity":null,"inbox":0,"desiredState":"retired","desiredStateReason":null}]"#
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
            r#"[{"identity":"hetz.worker","status":"available","name":null,"description":null,"retired":false,"resources":[{"name":"work","uri":"vendor+thing://authority/exact%20identity","reason":"Current implementation task."}],"desiredState":"running","desiredStateReason":null}]"#
        );
    }
}
