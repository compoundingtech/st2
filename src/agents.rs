//! The agent roster (M2.3): the data behind `st2 agents`. Enumerates the catalog's agents with their
//! presence status and retirement state, and optionally last-activity + inbox count. The JSON field
//! names, order, and null handling are a stable machine-readable contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::message;
use crate::status::{self, State};

/// One roster row: everything `st2 agents [--enrich]` can report about an agent.
#[derive(Debug, Clone)]
pub struct AgentRow {
    /// The bus id — `<host>.<identity>`.
    pub identity: String,
    /// Effective presence (derived: stale → `unknown`, etc.).
    pub status: State,
    /// Optional display name (`<agent_dir>/name`), else `None`.
    pub name: Option<String>,
    /// Whether the declaration is explicitly retired. Presence remains a separate runtime signal.
    pub retired: bool,
    /// Newest mtime (unix ms) across the agent's inbox, archive, and status file; `None` if nothing
    /// has been touched. `--enrich` only.
    pub last_activity_ms: Option<f64>,
    /// Count of canonical message files in the agent's inbox. `--enrich` only.
    pub inbox: usize,
}

/// Every agent in the catalog, sorted by bus id, with presence + enrich data computed. Read-only:
/// walks discovered specs and each agent's resources, mutating nothing.
pub fn roster(catalog_root: &Path, this_host: &str) -> Vec<AgentRow> {
    let found = crate::discover(catalog_root);
    let mut rows: Vec<AgentRow> = found
        .specs
        .iter()
        .filter_map(|s| {
            let agent_dir = s.path.parent()?;
            Some(AgentRow {
                identity: s.bus_id(this_host),
                status: status::read_state(&status::status_path(agent_dir)),
                name: read_name(agent_dir),
                retired: s.retired,
                last_activity_ms: newest_mtime_ms(agent_dir),
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
    retired: bool,
}

/// `st2 agents --json --enrich` row (adds `lastActivity` and `inbox`).
#[derive(Serialize)]
struct EnrichedJson<'a> {
    identity: &'a str,
    status: &'a str,
    name: Option<&'a str>,
    retired: bool,
    #[serde(rename = "lastActivity")]
    last_activity: Option<f64>,
    inbox: usize,
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
                retired: r.retired,
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
                retired: r.retired,
            })
            .collect();
        serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string())
    }
}

/// `<agent_dir>/name` first line, if non-empty.
fn read_name(agent_dir: &Path) -> Option<String> {
    let raw = fs::read_to_string(agent_dir.join("name")).ok()?;
    let first = raw.lines().next().unwrap_or("").trim();
    (!first.is_empty()).then(|| first.to_string())
}

/// Count logically unread messages in the agent's `resources/inbox`. A same-filename archive receipt
/// suppresses and cleans a raw inbox duplicate restored by eventually-consistent sync.
fn inbox_count(agent_dir: &Path) -> usize {
    message::list_inbox(&message::inbox_dir(agent_dir))
        .map(|msgs| msgs.len())
        .unwrap_or(0)
}

/// Newest mtime (unix ms) across the agent's inbox files, archive files, and status file. `None` if
/// none of those exist.
fn newest_mtime_ms(agent_dir: &Path) -> Option<f64> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    for dir in [
        message::inbox_dir(agent_dir),
        message::archive_dir(agent_dir),
    ] {
        if let Ok(rd) = fs::read_dir(&dir) {
            candidates.extend(rd.flatten().map(|e| e.path()));
        }
    }
    candidates.push(status::status_path(agent_dir));

    let mut newest: Option<SystemTime> = None;
    for p in candidates {
        if let Ok(m) = fs::metadata(&p)
            && let Ok(t) = m.modified()
            && newest.is_none_or(|n| t > n)
        {
            newest = Some(t);
        }
    }
    newest
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64() * 1000.0)
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
            retired,
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
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"retired":false},{"identity":"hetz.st2-claude","status":"busy","name":"owner","retired":true}]"#
        );
        assert_eq!(
            to_json(&rows, true),
            r#"[{"identity":"hetz.cos-claude","status":"available","name":null,"retired":false,"lastActivity":1784653027733.6138,"inbox":1},{"identity":"hetz.st2-claude","status":"busy","name":"owner","retired":true,"lastActivity":null,"inbox":0}]"#
        );
        // Empty roster is `[]`, not `null`.
        assert_eq!(to_json(&[], true), "[]");
    }
}
