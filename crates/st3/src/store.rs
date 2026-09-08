use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::model::{
    ApplyResponse, Capability, ClaimInput, ClaimRecord, ClaimsPage, DependencySpec, DesiredSubject,
    DocumentVersion, EventRecord, IntentInput, MAX_EVAL_TIMEOUT_MS, MessageView, MissionInputKind,
    MissionOutputView, MissionResponse, MissionRevisionOperation, MissionRunDeclaration,
    MissionRunInput, MissionRunRequest, MissionRunView, MissionSpec, MissionState,
    NormalizedIntent, PlannedAction, PlanningCandidateView, PlanningPreviewView,
    PlanningSessionDeclaration, PlanningSessionView, PlanningVariantView, ReplicaBatch,
    ReplicaRange, ReplicationBatch, ReplicationResponse, ResourceObservationOutcome,
    ResourceRefreshOperation, RevisionCutover, RevisionProposalView, RevisionSubmissionView,
    RunGenerationView, RuntimeResetOperation, St3Error, StatusResponse, StepRunView, SubjectChange,
    SubjectStatus, SubscriptionSpec, WorkRequest, WorkSelector,
};

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    origin TEXT NOT NULL,
    replica_sequence INTEGER NOT NULL,
    previous_hash TEXT,
    hash TEXT NOT NULL,
    accepted_at_unix_ms TEXT NOT NULL,
    UNIQUE(origin, replica_sequence)
);

CREATE TABLE IF NOT EXISTS claims (
    store_index INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    batch_id TEXT NOT NULL REFERENCES batches(id),
    subject TEXT NOT NULL,
    kind TEXT NOT NULL,
    origin TEXT NOT NULL,
    actor TEXT,
    body TEXT NOT NULL,
    predecessors TEXT NOT NULL,
    accepted_at_unix_ms TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS claims_subject_index ON claims(subject, store_index);
CREATE INDEX IF NOT EXISTS claims_kind_index ON claims(kind, store_index);
CREATE INDEX IF NOT EXISTS claims_operation_index
ON claims(json_extract(body, '$._operation.id'))
WHERE json_extract(body, '$._operation.id') IS NOT NULL;

CREATE TABLE IF NOT EXISTS operations (
    id TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    canonical_claim_id TEXT NOT NULL REFERENCES claims(id),
    state TEXT NOT NULL CHECK(state IN ('active','conflict'))
);

CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY,
    bytes BLOB NOT NULL,
    size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
    name TEXT NOT NULL,
    hash TEXT NOT NULL REFERENCES blobs(hash),
    created_index INTEGER NOT NULL,
    binding_claim_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(name, hash)
);
CREATE INDEX IF NOT EXISTS document_latest ON documents(name, created_index DESC);

CREATE TABLE IF NOT EXISTS desired (
    subject TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    revision TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claims(id),
    body TEXT NOT NULL,
    member TEXT,
    owner_run TEXT,
    owner_generation TEXT,
    owner_step TEXT
);

CREATE TABLE IF NOT EXISTS idempotency (
    operation_id TEXT PRIMARY KEY,
    response TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mission_run_requests (
    operation_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    store_index INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_cursors (
    peer TEXT PRIMARY KEY,
    accepted_through INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_replica_cursors (
    peer TEXT NOT NULL,
    origin TEXT NOT NULL,
    accepted_through INTEGER NOT NULL,
    PRIMARY KEY(peer, origin)
);

CREATE TABLE IF NOT EXISTS capabilities (
    secret_hash TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    incarnation_id TEXT,
    expires_at_unix_ms TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mission_revisions (
    mission_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    state TEXT NOT NULL,
    body TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claims(id),
    created_index INTEGER NOT NULL,
    PRIMARY KEY(mission_id, revision)
);

CREATE TABLE IF NOT EXISTS mission_definitions (
    mission_id TEXT PRIMARY KEY,
    revision TEXT NOT NULL,
    state TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claims(id)
);

CREATE TABLE IF NOT EXISTS mission_runs (
    id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL,
    initial_revision TEXT NOT NULL,
    current_generation_id TEXT NOT NULL,
    root_revision TEXT NOT NULL,
    root_run_id TEXT NOT NULL,
    parent_step_run TEXT,
    workspace TEXT NOT NULL,
    requester TEXT NOT NULL,
    inputs TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS mission_runs_mission_index ON mission_runs(mission_id, created_at_unix_ms);

CREATE TABLE IF NOT EXISTS mission_run_deadlines (
    run_id TEXT PRIMARY KEY REFERENCES mission_runs(id),
    timeout_ms INTEGER NOT NULL,
    deadline_at_unix_ms TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_generations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES mission_runs(id),
    revision TEXT NOT NULL,
    predecessor_id TEXT,
    status TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL,
    UNIQUE(run_id, id)
);
CREATE INDEX IF NOT EXISTS run_generations_run_index ON run_generations(run_id, created_at_unix_ms);

CREATE TABLE IF NOT EXISTS step_runs (
    subject TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES mission_runs(id),
    generation_id TEXT NOT NULL REFERENCES run_generations(id),
    step_path TEXT NOT NULL,
    definition_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    assignee TEXT,
    available_to TEXT NOT NULL DEFAULT '[]',
    agentless INTEGER NOT NULL DEFAULT 1,
    title TEXT,
    goals TEXT NOT NULL,
    worker_reported INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_incarnation TEXT,
    lease_expires_at_unix_ms TEXT,
    blocked_reason TEXT,
    not_before_unix_ms TEXT,
    activated_at_unix_ms TEXT,
    readiness_epoch INTEGER NOT NULL DEFAULT 0,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL,
    constraints TEXT NOT NULL DEFAULT '[]',
    UNIQUE(generation_id, step_path)
);
CREATE INDEX IF NOT EXISTS step_runs_run_index ON step_runs(run_id, generation_id, step_path);
CREATE INDEX IF NOT EXISTS step_runs_assignee_index ON step_runs(assignee, status);
CREATE TABLE IF NOT EXISTS revision_proposals (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES mission_runs(id),
    source_generation_id TEXT NOT NULL REFERENCES run_generations(id),
    candidate_revision TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    cutover TEXT NOT NULL,
    compatible_steps TEXT NOT NULL,
    reviewers TEXT NOT NULL,
    approvals TEXT NOT NULL,
    preview_hash TEXT,
    successor_generation_id TEXT,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS revision_proposals_one_pending
ON revision_proposals(run_id) WHERE status IN ('pending-approval','draining');
CREATE TABLE IF NOT EXISTS planning_sessions (
    id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL,
    request_ref TEXT NOT NULL,
    workspace TEXT NOT NULL,
    requester TEXT NOT NULL,
    planner TEXT NOT NULL,
    status TEXT NOT NULL,
    target_run_id TEXT,
    source_generation_id TEXT,
    published_revision TEXT,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS planning_candidates (
    session_id TEXT NOT NULL REFERENCES planning_sessions(id),
    variant TEXT NOT NULL,
    revision INTEGER NOT NULL,
    markdown_ref TEXT NOT NULL,
    kdl_ref TEXT NOT NULL,
    mission_revision TEXT NOT NULL,
    submitted_at_unix_ms TEXT NOT NULL,
    PRIMARY KEY(session_id, variant, revision)
);
CREATE TABLE IF NOT EXISTS planning_previews (
    session_id TEXT NOT NULL REFERENCES planning_sessions(id),
    variant TEXT NOT NULL,
    candidate_revision INTEGER NOT NULL,
    hash TEXT NOT NULL,
    store_index INTEGER NOT NULL,
    graph TEXT NOT NULL,
    diff TEXT NOT NULL,
    mission_response TEXT NOT NULL,
    created_at_unix_ms TEXT NOT NULL,
    PRIMARY KEY(session_id, variant)
);
PRAGMA user_version = 11;
"#;

pub struct Store {
    connection: Mutex<Connection>,
    origin: String,
}

struct ChildMissionContext {
    root_revision: String,
    root_run_id: String,
    parent_step_run: String,
    default_selector: Option<WorkSelector>,
}

fn reject_old_schema(connection: &Connection) -> Result<()> {
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let table_count: u32 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        table_count == 0 || matches!(version, 10 | 11),
        "this database uses an unsupported st3 schema; start with a new state directory"
    );
    anyhow::ensure!(
        matches!(version, 0 | 10 | 11),
        "this database uses unsupported st3 schema version {version}"
    );
    Ok(())
}

fn discovered_collection_items(
    repository: &str,
    field: &str,
    previous: Option<&Value>,
    current: &Value,
) -> Vec<(String, String, Value)> {
    let Some(previous_items) = previous
        .and_then(|value| value.get(field))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    let prior_numbers = previous_items
        .iter()
        .filter_map(|item| item.get("number").and_then(Value::as_u64))
        .collect::<BTreeSet<_>>();
    let Some(current_items) = current.get(field).and_then(Value::as_array) else {
        return Vec::new();
    };
    current_items
        .iter()
        .filter_map(|item| {
            let number = item.get("number")?.as_u64()?;
            if prior_numbers.contains(&number) {
                return None;
            }
            let (segment, kind) = match field {
                "pull_requests" => ("pull-request", "vcs.pull-request"),
                "issues" => ("issue", "vcs.issue"),
                _ => return None,
            };
            let mut facts = serde_json::Map::from_iter([
                ("repository".into(), Value::String(repository.into())),
                ("number".into(), Value::from(number)),
                ("state".into(), Value::String("open".into())),
            ]);
            for name in ["url", "title"] {
                if let Some(value) = item.get(name).filter(|value| !value.is_null()) {
                    facts.insert(name.into(), value.clone());
                }
            }
            if field == "pull_requests" {
                facts.insert("draft".into(), Value::Bool(false));
                facts.insert("merged".into(), Value::Bool(false));
            }
            Some((
                format!("{repository}/{segment}/{number}"),
                kind.into(),
                Value::Object(facts),
            ))
        })
        .collect()
}

impl Store {
    pub fn open(path: &Path, origin: impl Into<String>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(path)
            .with_context(|| format!("open st3 database {}", path.display()))?;
        reject_old_schema(&connection)?;
        connection.execute_batch(SCHEMA)?;
        {
            let transaction = connection.transaction()?;
            rebuild_operations_tx(&transaction)?;
            rebuild_planning_tx(&transaction)?;
            transaction.commit()?;
        }
        Ok(Self {
            connection: Mutex::new(connection),
            origin: origin.into(),
        })
    }

    pub fn open_memory(origin: impl Into<String>) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        reject_old_schema(&connection)?;
        connection.execute_batch(SCHEMA)?;
        {
            let transaction = connection.transaction()?;
            rebuild_operations_tx(&transaction)?;
            rebuild_planning_tx(&transaction)?;
            transaction.commit()?;
        }
        Ok(Self {
            connection: Mutex::new(connection),
            origin: origin.into(),
        })
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub fn index(&self) -> Result<u64> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_index(&connection)
    }

    pub fn operation_projection_drift(&self) -> Result<Vec<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let expected = expected_operations(&connection)?;
        let mut statement = connection.prepare(
            "SELECT id, request_digest, canonical_claim_id, state FROM operations ORDER BY id",
        )?;
        let actual = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ),
                ))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut drift = Vec::new();
        for id in expected
            .keys()
            .chain(actual.keys())
            .collect::<BTreeSet<_>>()
        {
            if expected.get(id) != actual.get(id) {
                drift.push((*id).clone());
            }
        }
        Ok(drift)
    }

    pub fn rebuild_claim_projections(&self) -> Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        rebuild_operations_tx(&transaction)?;
        rebuild_planning_tx(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn cached_idempotency_response<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(key)],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|response| serde_json::from_str(&response))
            .transpose()
            .map_err(Into::into)
    }

    pub(crate) fn cache_idempotency_response<T: Serialize>(
        &self,
        key: &str,
        response: &T,
    ) -> Result<()> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection.execute(
            "INSERT OR IGNORE INTO idempotency(operation_id, response) VALUES (?1, ?2)",
            params![opaque_cache_key(key), serde_json::to_string(response)?],
        )?;
        Ok(())
    }

    pub fn mission_spec(
        &self,
        mission_id: &str,
        revision: Option<&str>,
    ) -> Result<Option<MissionSpec>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let body = if let Some(revision) = revision {
            connection
                .query_row(
                    "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
                    params![mission_id, revision],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        } else {
            connection
                .query_row(
                    "SELECT r.body FROM mission_definitions d JOIN mission_revisions r ON r.mission_id=d.mission_id AND r.revision=d.revision WHERE d.mission_id=?1",
                    [mission_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        };
        body.map(|body| serde_json::from_str(&body))
            .transpose()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_planning_session(
        &self,
        id: &str,
        mission: &str,
        request_ref: &str,
        workspace: &str,
        requester: &str,
        planner: &str,
        target_run: Option<&str>,
        source_generation: Option<&str>,
    ) -> Result<PlanningSessionView, St3Error> {
        let now = now_ms().to_string();
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .execute(
                "INSERT OR IGNORE INTO planning_sessions(id, mission_id, request_ref, workspace, requester, planner, status, target_run_id, source_generation_id, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'planning', ?7, ?8, ?9, ?9)",
                params![id, mission, request_ref, workspace, requester, planner, target_run.map(|run| run.strip_prefix("mission-run/").unwrap_or(run)), source_generation.map(generation_id_from_subject), now],
            )
            .map_err(internal)?;
        planning_session_view_tx(&connection, id)
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    "the planning session was not stored",
                )
            })
    }

    pub fn planning_session(&self, id: &str) -> Result<Option<PlanningSessionView>> {
        let id = id.strip_prefix("planning-session/").unwrap_or(id);
        let connection = self.connection.lock().expect("store mutex poisoned");
        planning_session_view_tx(&connection, id)
    }

    pub fn add_planning_candidate(
        &self,
        id: &str,
        actor: &str,
        variant: &str,
        markdown_ref: &str,
        kdl_ref: &str,
        mission_revision: &str,
    ) -> Result<PlanningSessionView, St3Error> {
        let id = id.strip_prefix("planning-session/").unwrap_or(id);
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction().map_err(internal)?;
        let (planner, status): (String, String) = transaction
            .query_row(
                "SELECT planner, status FROM planning_sessions WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    format!("planning session `{id}` does not exist"),
                )
            })?;
        if planner != normalize_actor(actor, "agent") {
            return Err(St3Error::new(
                "wrong-planner",
                format!("`{actor}` does not own planning session `{id}`"),
            ));
        }
        if !matches!(
            status.as_str(),
            "planning" | "revision-requested" | "review"
        ) {
            return Err(St3Error::new(
                "planning-session-terminal",
                format!("planning session `{id}` is {status}"),
            ));
        }
        let current = transaction
            .query_row(
                "SELECT markdown_ref, kdl_ref, mission_revision FROM planning_candidates
                 WHERE session_id=?1 AND variant=?2 ORDER BY revision DESC LIMIT 1",
                params![id, variant],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(internal)?;
        if status == "review"
            && current.as_ref().is_some_and(|(markdown, kdl, revision)| {
                markdown == markdown_ref && kdl == kdl_ref && revision == mission_revision
            })
        {
            transaction.commit().map_err(internal)?;
            drop(connection);
            return self.planning_session(id).map_err(internal)?.ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    "the planning session disappeared",
                )
            });
        }
        let revision: u32 = transaction
            .query_row(
                "SELECT COALESCE(MAX(revision), 0) + 1 FROM planning_candidates WHERE session_id=?1 AND variant=?2",
                params![id, variant],
                |row| row.get(0),
            )
            .map_err(internal)?;
        let now = now_ms().to_string();
        transaction
            .execute(
                "INSERT INTO planning_candidates(session_id, variant, revision, markdown_ref, kdl_ref, mission_revision, submitted_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![id, variant, revision, markdown_ref, kdl_ref, mission_revision, now],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "DELETE FROM planning_previews WHERE session_id=?1 AND variant=?2",
                params![id, variant],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "UPDATE planning_sessions SET status='review', updated_at_unix_ms=?2 WHERE id=?1",
                params![id, now],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        drop(connection);
        self.planning_session(id).map_err(internal)?.ok_or_else(|| {
            St3Error::new(
                "missing-planning-session",
                "the planning session disappeared",
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_planning_preview(
        &self,
        id: &str,
        variant: &str,
        candidate_revision: u32,
        hash: &str,
        graph: &str,
        diff: &str,
        mission: &MissionResponse,
    ) -> Result<PlanningSessionView, St3Error> {
        let id = id.strip_prefix("planning-session/").unwrap_or(id);
        let now = now_ms().to_string();
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .execute(
                "INSERT INTO planning_previews(session_id, variant, candidate_revision, hash, store_index, graph, diff, mission_response, created_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(session_id, variant) DO UPDATE SET candidate_revision=excluded.candidate_revision, hash=excluded.hash,
                   store_index=excluded.store_index, graph=excluded.graph, diff=excluded.diff,
                   mission_response=excluded.mission_response, created_at_unix_ms=excluded.created_at_unix_ms",
                params![
                    id,
                    variant,
                    candidate_revision,
                    hash,
                    mission.store_index,
                    graph,
                    diff,
                    serde_json::to_string(mission).map_err(internal)?,
                    now,
                ],
            )
            .map_err(internal)?;
        planning_session_view_tx(&connection, id)
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    format!("planning session `{id}` does not exist"),
                )
            })
    }

    pub fn request_planning_revision(
        &self,
        id: &str,
        actor: &str,
    ) -> Result<PlanningSessionView, St3Error> {
        self.set_planning_status(id, actor, "revision-requested", None)
    }

    pub fn finish_planning_session(
        &self,
        id: &str,
        actor: &str,
        status: &str,
        published_revision: Option<&str>,
    ) -> Result<PlanningSessionView, St3Error> {
        if !matches!(status, "approved" | "cancelled") {
            return Err(St3Error::new(
                "invalid-planning-status",
                "invalid terminal planning status",
            ));
        }
        self.set_planning_status(id, actor, status, published_revision)
    }

    fn set_planning_status(
        &self,
        id: &str,
        actor: &str,
        status: &str,
        published_revision: Option<&str>,
    ) -> Result<PlanningSessionView, St3Error> {
        let id = id.strip_prefix("planning-session/").unwrap_or(id);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let (requester, current): (String, String) = connection
            .query_row(
                "SELECT requester, status FROM planning_sessions WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    format!("planning session `{id}` does not exist"),
                )
            })?;
        if requester != normalize_actor(actor, "person") {
            return Err(St3Error::new(
                "planning-review-not-authorized",
                format!("`{actor}` cannot review planning session `{id}`"),
            ));
        }
        let transition_is_valid = match status {
            "revision-requested" => current == "review",
            "approved" => current == "review" || current == "approved",
            "cancelled" => {
                matches!(
                    current.as_str(),
                    "planning" | "review" | "revision-requested" | "cancelled"
                )
            }
            _ => false,
        };
        if !transition_is_valid {
            return Err(St3Error::new(
                "invalid-planning-transition",
                format!("planning session `{id}` cannot move from {current} to {status}"),
            ));
        }
        if current == status {
            return planning_session_view_tx(&connection, id)
                .map_err(internal)?
                .ok_or_else(|| {
                    St3Error::new(
                        "missing-planning-session",
                        "the planning session disappeared",
                    )
                });
        }
        connection
            .execute(
                "UPDATE planning_sessions SET status=?2, published_revision=?3, updated_at_unix_ms=?4 WHERE id=?1",
                params![id, status, published_revision, now_ms().to_string()],
            )
            .map_err(internal)?;
        if status == "revision-requested" {
            connection
                .execute("DELETE FROM planning_previews WHERE session_id=?1", [id])
                .map_err(internal)?;
        }
        planning_session_view_tx(&connection, id)
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-planning-session",
                    "the planning session disappeared",
                )
            })
    }

    pub fn create_mission_run(
        &self,
        request: &MissionRunRequest,
    ) -> Result<MissionRunView, St3Error> {
        self.create_mission_run_inner(request, None)
    }

    pub fn create_child_mission_run(
        &self,
        request: &MissionRunRequest,
        parent: &MissionRunView,
        parent_step_run: &str,
        default_selector: Option<&WorkSelector>,
    ) -> Result<MissionRunView, St3Error> {
        self.create_mission_run_inner(
            request,
            Some(ChildMissionContext {
                root_revision: parent.root_revision.clone(),
                root_run_id: parent
                    .root_mission_run
                    .strip_prefix("mission-run/")
                    .unwrap_or(&parent.root_mission_run)
                    .to_owned(),
                parent_step_run: normalize_step_run(parent_step_run),
                default_selector: default_selector.cloned(),
            }),
        )
    }

    fn create_mission_run_inner(
        &self,
        request: &MissionRunRequest,
        child: Option<ChildMissionContext>,
    ) -> Result<MissionRunView, St3Error> {
        let mission_id = request
            .mission
            .strip_prefix("mission/")
            .unwrap_or(&request.mission);
        let mission = self
            .mission_spec(mission_id, request.revision.as_deref())
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-mission",
                    format!("mission `{mission_id}` does not exist"),
                )
            })?;
        if mission.state != MissionState::Ready {
            return Err(St3Error::new(
                "mission-not-ready",
                format!("mission `{mission_id}` is not ready"),
            ));
        }
        let request_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&json!({
                "request": request,
                "parent": child.as_ref().map(|child| json!({
                    "root_revision": child.root_revision,
                    "root_run_id": child.root_run_id,
                    "parent_step_run": child.parent_step_run,
                    "default_selector": child.default_selector,
                })),
            }))
            .map_err(internal)?,
        ));
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some((response, stored_hash)) = connection
            .query_row(
                "SELECT i.response, r.request_hash FROM idempotency i JOIN mission_run_requests r ON r.operation_id=i.operation_id WHERE i.operation_id=?1",
                [opaque_cache_key(&request.idempotency_key)],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(internal)?
        {
            if stored_hash != request_hash {
                return Err(St3Error::new(
                    "idempotency-mismatch",
                    "the mission-run idempotency key was used with different input",
                ));
            }
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let inputs = resolve_mission_run_inputs(&transaction, &mission, &request.inputs)?;
        enforce_mission_run_capacity(&transaction, &mission)?;
        let run_id = hex::encode(Sha256::digest(
            format!("{}:{}", self.origin, request.idempotency_key).as_bytes(),
        ))[..32]
            .to_owned();
        let subject = format!("mission-run/{run_id}");
        let generation_id = hex::encode(Sha256::digest(
            format!("{}:{}:generation:1", self.origin, request.idempotency_key).as_bytes(),
        ))[..32]
            .to_owned();
        let generation_subject = format!("run-generation/{generation_id}");
        let root_revision = child
            .as_ref()
            .map(|child| child.root_revision.clone())
            .unwrap_or_else(|| mission.revision.clone());
        let root_run_id = child
            .as_ref()
            .map(|child| child.root_run_id.clone())
            .unwrap_or_else(|| run_id.clone());
        let root_mission_run = format!("mission-run/{root_run_id}");
        let parent_step_run = child.as_ref().map(|child| child.parent_step_run.clone());
        let default_selector = child
            .as_ref()
            .and_then(|child| child.default_selector.clone());
        let requester = normalize_actor(
            request.requester.as_deref().unwrap_or("person/requester"),
            "person",
        );
        let mode = request.mode.as_deref().unwrap_or("run");
        if !matches!(mode, "run" | "eval") {
            return Err(St3Error::new(
                "invalid-run-mode",
                format!("run mode `{mode}` is not registered"),
            ));
        }
        validate_mission_run_timeout(&mission, mode)?;
        let mut variables = BTreeMap::from([
            ("ST_MISSION".into(), mission.id.clone()),
            ("ST_MISSION_REVISION".into(), mission.revision.clone()),
            ("ST_MISSION_RUN".into(), run_id.clone()),
            ("ST_RUN_GENERATION".into(), generation_id.clone()),
            ("ST_REQUESTER".into(), requester.clone()),
            ("ST_ROOT_MISSION_RUN".into(), root_mission_run.clone()),
            ("ST_WORKSPACE".into(), request.workspace.clone()),
            (
                "ST_PARENT_STEP_RUN".into(),
                parent_step_run.clone().unwrap_or_default(),
            ),
        ]);
        for (name, input) in &inputs {
            variables.insert(format!("input.{name}"), input.value.clone());
        }
        let now = now_ms();
        transaction
            .execute(
                "INSERT INTO mission_runs(id, mission_id, initial_revision, current_generation_id, root_revision, root_run_id, parent_step_run, workspace, requester, inputs, mode, status, phase, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'running', 'normal', ?12, ?12)",
                params![run_id, mission.id, mission.revision, generation_id, root_revision, root_run_id, parent_step_run, request.workspace, requester, serde_json::to_string(&inputs).map_err(internal)?, mode, now.to_string()],
            )
            .map_err(internal)?;
        let deadline_at_unix_ms =
            insert_mission_deadline_tx(&transaction, &run_id, mission.timeout_ms, now)?;
        transaction
            .execute(
                "INSERT INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, NULL, 'running', ?4, 'initial mission run', ?5, ?5)",
                params![generation_id, run_id, mission.revision, requester, now.to_string()],
            )
            .map_err(internal)?;
        let mut flat = Vec::new();
        flatten_steps(&mission, default_selector.clone(), &[], &mut flat);
        for (step, selector, constraints) in flat {
            let (assignee, available_to, agentless) = interpolate_selector(&selector, &variables)?;
            let step_subject = format!("step-run/{generation_id}/{}", step.path);
            let mut step_variables = variables.clone();
            step_variables.insert("ST_STEP".into(), step.path.clone());
            step_variables.insert("ST_STEP_RUN".into(), step_subject.clone());
            step_variables.insert("ST_ATTEMPT".into(), "1".into());
            step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
            step_variables.insert(
                "ST_PARENT_STEP_RUN".into(),
                crate::mission::parent_step_path(&mission, &step.path)
                    .map(|path| format!("step-run/{generation_id}/{path}"))
                    .or_else(|| parent_step_run.clone())
                    .unwrap_or_default(),
            );
            let title = step
                .title
                .as_deref()
                .map(|value| crate::mission::interpolate(value, &step_variables))
                .transpose()?;
            let goals = interpolate_goals(&step.goals, &step_variables)?;
            let constraints = interpolate_goals(&constraints, &step_variables)?;
            transaction
                .execute(
                    "INSERT INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, created_at_unix_ms, updated_at_unix_ms, constraints)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 1, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?12)",
                    params![step_subject, run_id, generation_id, step.path, step.definition_hash, assignee, serde_json::to_string(&available_to).map_err(internal)?, agentless, title, goals, now.to_string(), constraints],
                )
                .map_err(internal)?;
        }
        let body = json!({
            "fields": {
                "status": "running",
                "mission": mission.subject,
                "revision": mission.revision,
                "initial_revision": mission.revision,
                "current_generation": generation_subject,
                "root_revision": root_revision,
                "root_mission_run": root_mission_run,
                "parent_step_run": parent_step_run,
                "default_selector": default_selector,
                "workspace": request.workspace,
                "requester": requester,
                "inputs": inputs,
                "mode": mode,
                "timeout_ms": mission.timeout_ms,
                "deadline_at_unix_ms": deadline_at_unix_ms,
            }
        });
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "mission-run.created",
            Some(&requester),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        append_claim_tx(
            &transaction,
            &self.origin,
            &generation_subject,
            "run-generation.created",
            Some(&requester),
            &json!({"fields": {
                "run": subject,
                "revision": mission.revision,
                "status": "running",
                "reason": "initial mission run"
            }}),
            &[],
            None,
        )
        .map_err(internal)?;
        let view = mission_run_view_tx(&transaction, &run_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(&request.idempotency_key),
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO mission_run_requests(operation_id, request_hash) VALUES (?1, ?2)",
                params![opaque_cache_key(&request.idempotency_key), request_hash],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn record_mission_output(
        &self,
        subject: &str,
        actor: &str,
        incarnation: Option<&str>,
        expected_mission: &str,
        mission: &MissionSpec,
        idempotency_key: &str,
    ) -> Result<MissionOutputView, St3Error> {
        let subject = normalize_step_run(subject);
        let actor = normalize_actor(actor, "agent");
        let now = now_ms();
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let current = transaction
            .query_row(
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
                 FROM step_runs WHERE subject=?1",
                [&subject],
                step_run_from_row,
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-step-run",
                    format!("step run `{subject}` does not exist"),
                )
            })?;
        if !step_generation_is_current(&transaction, &current).map_err(internal)? {
            return Err(St3Error::new(
                "stale-run-generation",
                format!("step run `{subject}` belongs to a superseded generation"),
            ));
        }
        if !mission_output_authority(&transaction, &current, &actor, incarnation, now)
            .map_err(internal)?
        {
            return Err(St3Error::new(
                "work-not-claimed",
                format!(
                    "`{actor}` does not hold an active lease for `{subject}` or its nested work"
                ),
            ));
        }
        if mission.id != expected_mission || mission.state != MissionState::Ready {
            return Err(St3Error::new(
                "wrong-mission-output",
                format!("step run `{subject}` must publish ready mission `{expected_mission}`"),
            ));
        }
        let stored = transaction
            .query_row(
                "SELECT state FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
                params![mission.id, mission.revision],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?;
        if stored.as_deref() != Some("ready") {
            return Err(St3Error::new(
                "unpublished-mission-output",
                "the exact ready mission revision is not published",
            ));
        }
        let fields: BTreeMap<String, Value> = BTreeMap::from([
            (
                "mission".into(),
                Value::String(format!("mission/{}", mission.id)),
            ),
            ("revision".into(), Value::String(mission.revision.clone())),
            (
                "step_definition".into(),
                Value::String(current.definition_hash.clone()),
            ),
            ("attempt".into(), Value::from(current.attempt)),
        ]);
        let body = json!({"fields": fields});
        let claim = append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "mission.produced",
            Some(&actor),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        let output = MissionOutputView {
            step: subject,
            mission: format!("mission/{}", mission.id),
            revision: mission.revision.clone(),
            claim_id: claim.id,
        };
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&output).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(output)
    }

    pub fn mission_output_authorized(
        &self,
        subject: &str,
        actor: &str,
        incarnation: Option<&str>,
    ) -> Result<bool> {
        let subject = normalize_step_run(subject);
        let actor = normalize_actor(actor, "agent");
        let connection = self.connection.lock().expect("store mutex poisoned");
        let current = connection
            .query_row(
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
                 FROM step_runs WHERE subject=?1",
                [&subject],
                step_run_from_row,
            )
            .optional()
            .map_err(internal)?;
        current
            .as_ref()
            .map(|current| {
                Ok(step_generation_is_current(&connection, current)?
                    && mission_output_authority(
                        &connection,
                        current,
                        &actor,
                        incarnation,
                        now_ms(),
                    )?)
            })
            .transpose()
            .map(|authorized| authorized.unwrap_or(false))
    }

    pub fn mission_output(
        &self,
        subject: &str,
        attempt: u32,
        definition_hash: &str,
    ) -> Result<Option<MissionOutputView>> {
        let subject = normalize_step_run(subject);
        let Some(claim) = self.latest_claim(&subject, Some("mission.produced"))? else {
            return Ok(None);
        };
        let fields = claim.body.get("fields").unwrap_or(&claim.body);
        if fields.get("attempt").and_then(Value::as_u64) != Some(u64::from(attempt))
            || fields.get("step_definition").and_then(Value::as_str) != Some(definition_hash)
        {
            return Ok(None);
        }
        let Some(mission) = fields.get("mission").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(revision) = fields.get("revision").and_then(Value::as_str) else {
            return Ok(None);
        };
        Ok(Some(MissionOutputView {
            step: subject,
            mission: mission.to_owned(),
            revision: revision.to_owned(),
            claim_id: claim.id,
        }))
    }

    pub fn mission_run(&self, run: &str) -> Result<Option<MissionRunView>> {
        let run = run.strip_prefix("mission-run/").unwrap_or(run);
        let connection = self.connection.lock().expect("store mutex poisoned");
        Ok(mission_run_view_tx(&connection, run).optional()?)
    }

    pub fn run_generations(&self, run: &str) -> Result<Vec<RunGenerationView>> {
        let run = run.strip_prefix("mission-run/").unwrap_or(run);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM run_generations WHERE run_id=?1 ORDER BY created_at_unix_ms, id",
        )?;
        let ids = statement
            .query_map([run], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| run_generation_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn run_generation(&self, generation: &str) -> Result<Option<RunGenerationView>> {
        let generation = generation_id_from_subject(generation);
        let connection = self.connection.lock().expect("store mutex poisoned");
        Ok(run_generation_view_tx(&connection, generation).optional()?)
    }

    pub fn descendant_run_generations(&self, generation: &str) -> Result<Vec<String>> {
        let generation = generation_id_from_subject(generation);
        let connection = self.connection.lock().expect("store mutex poisoned");
        descendant_mission_run_ids_tx(&connection, generation)?
            .into_iter()
            .map(|run| {
                connection
                    .query_row(
                        "SELECT current_generation_id FROM mission_runs WHERE id=?1",
                        [run],
                        |row| row.get::<_, String>(0),
                    )
                    .map(|id| format!("run-generation/{id}"))
                    .map_err(Into::into)
            })
            .collect()
    }

    pub fn revision_proposal_for_run(&self, run: &str) -> Result<Option<RevisionProposalView>> {
        let run = run.strip_prefix("mission-run/").unwrap_or(run);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let id = connection
            .query_row(
                "SELECT id FROM revision_proposals WHERE run_id=?1 AND status IN ('pending-approval','draining') ORDER BY created_at_unix_ms DESC LIMIT 1",
                [run],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        id.map(|id| revision_proposal_view_tx(&connection, &id))
            .transpose()
            .map_err(Into::into)
    }

    pub fn revision_proposal(&self, proposal: &str) -> Result<Option<RevisionProposalView>> {
        let proposal = proposal
            .strip_prefix("revision-proposal/")
            .unwrap_or(proposal);
        let connection = self.connection.lock().expect("store mutex poisoned");
        Ok(revision_proposal_view_tx(&connection, proposal).optional()?)
    }

    pub fn create_revision_proposal(
        &self,
        run: &str,
        mission: &MissionSpec,
        actor: &str,
        reason: &str,
        idempotency_key: &str,
    ) -> Result<RevisionProposalView, St3Error> {
        if let Some(response) = self
            .cached_idempotency_response(idempotency_key)
            .map_err(internal)?
        {
            return Ok(response);
        }
        let run_id = run.strip_prefix("mission-run/").unwrap_or(run);
        let current = self.mission_run(run_id).map_err(internal)?.ok_or_else(|| {
            St3Error::new(
                "missing-mission-run",
                format!("mission run `{run}` does not exist"),
            )
        })?;
        if !matches!(current.status.as_str(), "running" | "standing" | "blocked")
            || current.phase != "normal"
        {
            return Err(St3Error::new(
                "mission-run-not-revisable",
                format!(
                    "mission run `{run}` is {} in its {} phase",
                    current.status, current.phase
                ),
            ));
        }
        let mission_id = current
            .mission
            .strip_prefix("mission/")
            .unwrap_or(&current.mission);
        let old = self
            .mission_spec(mission_id, Some(&current.revision))
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-mission-revision",
                    "the current mission revision is unavailable",
                )
            })?;
        if mission.id != mission_id || mission.state != MissionState::Ready {
            return Err(St3Error::new(
                "wrong-mission-revision",
                format!("the proposal does not contain ready mission `{mission_id}`"),
            ));
        }
        if old.inputs != mission.inputs {
            return Err(St3Error::new(
                "run-input-mutation",
                "a run revision cannot change its input declarations",
            ));
        }
        let actor = normalize_actor(actor, "agent");
        let variables = mission_run_variables(&current, &mission.revision);
        let (compatible, reviewers) =
            analyze_mission_revision(&old, mission, &actor, &current.requester, &variables)?;
        let cutover = old.revision_cutover.clone();
        let status = if reviewers.is_empty() {
            match &cutover {
                RevisionCutover::RestartActive => {
                    return Err(St3Error::new(
                        "revision-does-not-need-proposal",
                        "this revision can cut over immediately",
                    ));
                }
                RevisionCutover::WhenIdle => "draining",
            }
        } else {
            "pending-approval"
        };
        let proposal_id = hex::encode(Sha256::digest(
            format!("{}:{}:revision-proposal", self.origin, idempotency_key).as_bytes(),
        ))[..32]
            .to_owned();
        let preview_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&json!({
                "source_generation": current.generation,
                "candidate_revision": mission.revision,
                "compatible_steps": compatible,
                "reviewers": reviewers,
                "cutover": cutover,
            }))
            .map_err(internal)?,
        ));
        let now = now_ms();
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        if connection
            .query_row(
                "SELECT 1 FROM revision_proposals WHERE run_id=?1 AND status IN ('pending-approval','draining')",
                [run_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(internal)?
            .is_some()
        {
            return Err(St3Error::new(
                "revision-proposal-already-pending",
                "the mission run already has one pending revision proposal",
            ));
        }
        let transaction = connection.transaction().map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO revision_proposals(id, run_id, source_generation_id, candidate_revision, actor, reason, status, cutover, compatible_steps, reviewers, approvals, preview_hash, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, '[]', ?11, ?12, ?12)",
                params![
                    proposal_id,
                    run_id,
                    generation_id_from_subject(&current.generation),
                    mission.revision,
                    actor,
                    reason,
                    status,
                    revision_cutover_name(&cutover),
                    serde_json::to_string(&compatible).map_err(internal)?,
                    serde_json::to_string(&reviewers).map_err(internal)?,
                    preview_hash,
                    now.to_string(),
                ],
            )
            .map_err(internal)?;
        if status == "draining" {
            transaction
                .execute(
                    "UPDATE mission_runs SET phase='revision-draining', updated_at_unix_ms=?2 WHERE id=?1",
                    params![run_id, now.to_string()],
                )
                .map_err(internal)?;
        }
        let subject = format!("revision-proposal/{proposal_id}");
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "revision-proposal.created",
            Some(&actor),
            &json!({"fields": {
                "run": current.subject,
                "source_generation": current.generation,
                "candidate_revision": mission.revision,
                "reason": reason,
                "status": status,
                "cutover": revision_cutover_name(&cutover),
                "compatible_steps": compatible,
                "reviewers": reviewers,
                "preview_hash": preview_hash,
            }}),
            &[],
            None,
        )
        .map_err(internal)?;
        let view = revision_proposal_view_tx(&transaction, &proposal_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn approve_revision_proposal(
        &self,
        proposal: &str,
        actor: &str,
        preview_hash: &str,
        idempotency_key: &str,
    ) -> Result<RevisionSubmissionView, St3Error> {
        let proposal_id = proposal
            .strip_prefix("revision-proposal/")
            .unwrap_or(proposal);
        let actor = normalize_actor(actor, "person");
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let mut proposal =
            revision_proposal_view_tx(&transaction, proposal_id).map_err(internal)?;
        if proposal.status == "applied"
            && proposal.preview_hash.as_deref() == Some(preview_hash)
            && proposal.approvals.contains(&actor)
        {
            let run = mission_run_view_tx(
                &transaction,
                proposal
                    .run
                    .strip_prefix("mission-run/")
                    .unwrap_or(&proposal.run),
            )
            .map_err(internal)?;
            let response = RevisionSubmissionView {
                status: "applied".into(),
                mission_run: run,
                proposal: Some(proposal),
            };
            transaction
                .execute(
                    "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                    params![
                        opaque_cache_key(idempotency_key),
                        serde_json::to_string(&response).map_err(internal)?
                    ],
                )
                .map_err(internal)?;
            transaction.commit().map_err(internal)?;
            return Ok(response);
        }
        if proposal.status != "pending-approval" {
            return Err(St3Error::new(
                "revision-proposal-not-reviewable",
                format!("revision proposal `{proposal_id}` is {}", proposal.status),
            ));
        }
        if proposal.preview_hash.as_deref() != Some(preview_hash) {
            return Err(St3Error::new(
                "stale-revision-preview",
                "the approval does not name the current revision preview",
            ));
        }
        if !proposal.reviewers.contains(&actor) {
            return Err(St3Error::new(
                "revision-approval-not-authorized",
                format!("`{actor}` is not a reviewer for `{proposal_id}`"),
            ));
        }
        if !proposal.approvals.contains(&actor) {
            proposal.approvals.push(actor.clone());
            proposal.approvals.sort();
        }
        let all_approved = proposal
            .reviewers
            .iter()
            .all(|reviewer| proposal.approvals.contains(reviewer));
        let status = if all_approved && matches!(proposal.cutover, RevisionCutover::WhenIdle) {
            "draining"
        } else {
            "pending-approval"
        };
        let now = now_ms();
        transaction
            .execute(
                "UPDATE revision_proposals SET approvals=?2, status=?3, updated_at_unix_ms=?4 WHERE id=?1",
                params![
                    proposal_id,
                    serde_json::to_string(&proposal.approvals).map_err(internal)?,
                    status,
                    now.to_string(),
                ],
            )
            .map_err(internal)?;
        if status == "draining" {
            transaction
                .execute(
                    "UPDATE mission_runs SET phase='revision-draining', updated_at_unix_ms=?2 WHERE id=?1",
                    params![proposal.run.strip_prefix("mission-run/").unwrap_or(&proposal.run), now.to_string()],
                )
                .map_err(internal)?;
        }
        append_claim_tx(
            &transaction,
            &self.origin,
            &proposal.subject,
            "revision-proposal.approved",
            Some(&actor),
            &json!({"fields": {"reviewer": actor, "preview_hash": preview_hash, "all_approved": all_approved}}),
            &[],
            None,
        )
        .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        drop(connection);

        if all_approved && matches!(proposal.cutover, RevisionCutover::RestartActive) {
            let run = self.finish_proposal_cutover(proposal_id, &actor, idempotency_key)?;
            let applied = self
                .revision_proposal(proposal_id)
                .map_err(internal)?
                .expect("the applied proposal exists");
            let response = RevisionSubmissionView {
                status: "applied".into(),
                mission_run: run,
                proposal: Some(applied),
            };
            let connection = self.connection.lock().expect("store mutex poisoned");
            connection
                .execute(
                    "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                    params![
                        opaque_cache_key(idempotency_key),
                        serde_json::to_string(&response).map_err(internal)?
                    ],
                )
                .map_err(internal)?;
            return Ok(response);
        }

        let proposal = self
            .revision_proposal(proposal_id)
            .map_err(internal)?
            .expect("the proposal exists");
        let run = self
            .mission_run(&proposal.run)
            .map_err(internal)?
            .expect("the proposal mission run exists");
        let response = RevisionSubmissionView {
            status: proposal.status.clone(),
            mission_run: run,
            proposal: Some(proposal),
        };
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&response).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        Ok(response)
    }

    pub fn cancel_revision_proposal(
        &self,
        proposal: &str,
        actor: &str,
        reason: Option<&str>,
        idempotency_key: &str,
    ) -> Result<RevisionProposalView, St3Error> {
        let proposal_id = proposal
            .strip_prefix("revision-proposal/")
            .unwrap_or(proposal);
        let actor = normalize_actor(
            actor,
            if actor.starts_with("person/") {
                "person"
            } else {
                "agent"
            },
        );
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let current = revision_proposal_view_tx(&transaction, proposal_id).map_err(internal)?;
        let run_id = current
            .run
            .strip_prefix("mission-run/")
            .unwrap_or(&current.run);
        let requester: String = transaction
            .query_row(
                "SELECT requester FROM mission_runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .map_err(internal)?;
        if actor != current.actor && actor != requester && !current.reviewers.contains(&actor) {
            return Err(St3Error::new(
                "revision-cancel-not-authorized",
                format!("`{actor}` cannot cancel `{proposal_id}`"),
            ));
        }
        if !matches!(current.status.as_str(), "pending-approval" | "draining") {
            return Err(St3Error::new(
                "revision-proposal-not-cancellable",
                format!("revision proposal `{proposal_id}` is {}", current.status),
            ));
        }
        let now = now_ms();
        transaction
            .execute(
                "UPDATE revision_proposals SET status='cancelled', updated_at_unix_ms=?2 WHERE id=?1",
                params![proposal_id, now.to_string()],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "UPDATE mission_runs SET phase='normal', updated_at_unix_ms=?2 WHERE id=?1 AND phase='revision-draining'",
                params![run_id, now.to_string()],
            )
            .map_err(internal)?;
        append_claim_tx(
            &transaction,
            &self.origin,
            &current.subject,
            "revision-proposal.cancelled",
            Some(&actor),
            &json!({"fields": {"status": "cancelled", "reason": reason}}),
            &[],
            None,
        )
        .map_err(internal)?;
        let view = revision_proposal_view_tx(&transaction, proposal_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn apply_drained_revision(
        &self,
        run: &str,
    ) -> Result<Option<RevisionSubmissionView>, St3Error> {
        let run_id = run.strip_prefix("mission-run/").unwrap_or(run);
        let proposal = {
            let connection = self.connection.lock().expect("store mutex poisoned");
            let active: u32 = connection
                .query_row(
                    "SELECT COUNT(*) FROM step_runs
                     WHERE generation_id=(SELECT current_generation_id FROM mission_runs WHERE id=?1)
                       AND status IN ('claimed','working','verifying')",
                    [run_id],
                    |row| row.get(0),
                )
                .map_err(internal)?;
            if active != 0 {
                return Ok(None);
            }
            let current_generation: String = connection
                .query_row(
                    "SELECT current_generation_id FROM mission_runs WHERE id=?1",
                    [run_id],
                    |row| row.get(0),
                )
                .map_err(internal)?;
            for descendant in
                descendant_mission_run_ids_tx(&connection, &current_generation).map_err(internal)?
            {
                let active: u32 = connection
                    .query_row(
                        "SELECT COUNT(*) FROM step_runs
                         WHERE run_id=?1
                           AND generation_id=(SELECT current_generation_id FROM mission_runs WHERE id=?1)
                           AND status IN ('claimed','working','verifying')",
                        [descendant],
                        |row| row.get(0),
                    )
                    .map_err(internal)?;
                if active != 0 {
                    return Ok(None);
                }
            }
            let proposal_id = connection
                .query_row(
                    "SELECT id FROM revision_proposals WHERE run_id=?1 AND status='draining' ORDER BY created_at_unix_ms LIMIT 1",
                    [run_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(internal)?;
            proposal_id
                .map(|id| revision_proposal_view_tx(&connection, &id))
                .transpose()
                .map_err(internal)?
        };
        let Some(proposal) = proposal else {
            return Ok(None);
        };
        let run = self.finish_proposal_cutover(
            &proposal.id,
            &proposal.actor,
            &format!("revision-proposal:{}:drained", proposal.id),
        )?;
        let proposal = self
            .revision_proposal(&proposal.id)
            .map_err(internal)?
            .expect("the applied proposal exists");
        Ok(Some(RevisionSubmissionView {
            status: "applied".into(),
            mission_run: run,
            proposal: Some(proposal),
        }))
    }

    fn finish_proposal_cutover(
        &self,
        proposal_id: &str,
        actor: &str,
        idempotency_key: &str,
    ) -> Result<MissionRunView, St3Error> {
        let proposal = self
            .revision_proposal(proposal_id)
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-revision-proposal",
                    format!("revision proposal `{proposal_id}` does not exist"),
                )
            })?;
        let run_id = proposal
            .run
            .strip_prefix("mission-run/")
            .unwrap_or(&proposal.run);
        let current = self.mission_run(run_id).map_err(internal)?.ok_or_else(|| {
            St3Error::new(
                "missing-mission-run",
                format!("mission run `{run_id}` does not exist"),
            )
        })?;
        if current.generation != proposal.source_generation {
            return Err(St3Error::new(
                "stale-revision-proposal",
                "the revision proposal does not target the current generation",
            ));
        }
        let mission_id = current
            .mission
            .strip_prefix("mission/")
            .unwrap_or(&current.mission);
        let mission = self
            .mission_spec(mission_id, Some(&proposal.candidate_revision))
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-mission-revision",
                    "the proposed mission revision is unavailable",
                )
            })?;
        self.adopt_approved_mission_revision(
            run_id,
            &mission,
            actor,
            &proposal.reason,
            &format!("{idempotency_key}:cutover"),
            &proposal.source_generation,
            &proposal,
        )
    }

    pub fn mission_run_for_parent_step(&self, step: &str) -> Result<Option<MissionRunView>> {
        let step = normalize_step_run(step);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let run_id = connection
            .query_row(
                "SELECT id FROM mission_runs WHERE parent_step_run=?1 ORDER BY created_at_unix_ms DESC LIMIT 1",
                [step],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        run_id
            .map(|run_id| mission_run_view_tx(&connection, &run_id))
            .transpose()
            .map_err(Into::into)
    }

    pub fn adopt_mission_revision(
        &self,
        run: &str,
        mission: &MissionSpec,
        actor: &str,
        reason: &str,
        idempotency_key: &str,
    ) -> Result<MissionRunView, St3Error> {
        self.adopt_mission_revision_inner(
            run,
            mission,
            actor,
            reason,
            idempotency_key,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn adopt_approved_mission_revision(
        &self,
        run: &str,
        mission: &MissionSpec,
        actor: &str,
        reason: &str,
        idempotency_key: &str,
        source_generation: &str,
        proposal: &RevisionProposalView,
    ) -> Result<MissionRunView, St3Error> {
        self.adopt_mission_revision_inner(
            run,
            mission,
            actor,
            reason,
            idempotency_key,
            Some(source_generation),
            true,
            Some(proposal),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn adopt_mission_revision_inner(
        &self,
        run: &str,
        mission: &MissionSpec,
        actor: &str,
        reason: &str,
        idempotency_key: &str,
        expected_generation: Option<&str>,
        protected_approved: bool,
        proposal: Option<&RevisionProposalView>,
    ) -> Result<MissionRunView, St3Error> {
        if let Some(response) = self
            .cached_idempotency_response(idempotency_key)
            .map_err(internal)?
        {
            return Ok(response);
        }
        let run_id = run.strip_prefix("mission-run/").unwrap_or(run);
        let actor = normalize_actor(actor, "agent");
        let current = self.mission_run(run_id).map_err(internal)?.ok_or_else(|| {
            St3Error::new(
                "missing-mission-run",
                format!("mission run `{run}` does not exist"),
            )
        })?;
        if expected_generation.is_some_and(|expected| {
            generation_id_from_subject(expected) != generation_id_from_subject(&current.generation)
        }) {
            return Err(St3Error::new(
                "stale-revision-proposal",
                "the revision proposal does not target the current generation",
            ));
        }
        let phase_allows_cutover = current.phase == "normal"
            || (proposal.is_some() && current.phase == "revision-draining");
        if !matches!(current.status.as_str(), "running" | "standing" | "blocked")
            || !phase_allows_cutover
        {
            return Err(St3Error::new(
                "mission-run-not-revisable",
                format!(
                    "mission run `{run}` is {} in its {} phase",
                    current.status, current.phase
                ),
            ));
        }
        let mission_id = current
            .mission
            .strip_prefix("mission/")
            .unwrap_or(&current.mission);
        if mission.id != mission_id {
            return Err(St3Error::new(
                "wrong-mission-revision",
                format!(
                    "revision `{}` does not replace mission `{mission_id}`",
                    mission.id
                ),
            ));
        }
        if mission.state != MissionState::Ready {
            return Err(St3Error::new(
                "mission-revision-not-ready",
                "a running mission can adopt only a ready revision",
            ));
        }
        let old = self
            .mission_spec(mission_id, Some(&current.revision))
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-mission-revision",
                    "the current mission revision is unavailable",
                )
            })?;
        if old.inputs != mission.inputs {
            return Err(St3Error::new(
                "run-input-mutation",
                "a run revision cannot change its input declarations",
            ));
        }

        let variables = mission_run_variables(&current, &mission.revision);
        let (compatible, reviewers) = if protected_approved {
            (compatible_step_paths(&old, mission), BTreeSet::new())
        } else {
            analyze_mission_revision(&old, mission, &actor, &current.requester, &variables)?
        };
        if !protected_approved && matches!(old.revision_cutover, RevisionCutover::WhenIdle) {
            return Err(St3Error::new(
                "revision-needs-drained-cutover",
                "the current mission revision requires a drained cutover",
            ));
        }
        if !reviewers.is_empty() && !protected_approved {
            return Err(St3Error::new(
                "revision-needs-human-approval",
                format!(
                    "the revision needs approval from {}",
                    reviewers.into_iter().collect::<Vec<_>>().join(", ")
                ),
            ));
        }

        let mut new_steps = Vec::new();
        flatten_steps(mission, None, &[], &mut new_steps);

        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let now = now_ms();
        let predecessor_id = generation_id_from_subject(&current.generation).to_owned();
        let generation_id = hex::encode(Sha256::digest(
            format!("{}:{}:generation", self.origin, idempotency_key).as_bytes(),
        ))[..32]
            .to_owned();
        let generation_subject = format!("run-generation/{generation_id}");
        let predecessor_subject = current.generation.clone();
        let mut successor_variables = variables.clone();
        successor_variables.insert("ST_RUN_GENERATION".into(), generation_id.clone());
        transaction
            .execute(
                "INSERT INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?6, ?7, ?7)",
                params![generation_id, run_id, mission.revision, predecessor_id, actor, reason, now.to_string()],
            )
            .map_err(internal)?;
        for (step, selector, constraints) in new_steps {
            let (assignee, available_to, agentless) =
                interpolate_selector(&selector, &successor_variables)?;
            let subject = format!("step-run/{generation_id}/{}", step.path);
            let mut step_variables = successor_variables.clone();
            step_variables.insert("ST_STEP".into(), step.path.clone());
            step_variables.insert("ST_STEP_RUN".into(), subject.clone());
            step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
            step_variables.insert(
                "ST_PARENT_STEP_RUN".into(),
                crate::mission::parent_step_path(mission, &step.path)
                    .map(|path| format!("step-run/{generation_id}/{path}"))
                    .or_else(|| current.parent_step_run.clone())
                    .unwrap_or_default(),
            );
            let carried = compatible
                .contains(&step.path)
                .then(|| current.steps.iter().find(|old| old.step == step.path))
                .flatten();
            let attempt = carried.map(|old| old.attempt).unwrap_or(1);
            step_variables.insert("ST_ATTEMPT".into(), attempt.to_string());
            let title = step
                .title
                .as_deref()
                .map(|value| crate::mission::interpolate(value, &step_variables))
                .transpose()?;
            let goals = interpolate_goals(&step.goals, &step_variables)?;
            let constraints = interpolate_goals(&constraints, &step_variables)?;
            let status = carried
                .map(|old| match old.status.as_str() {
                    "claimed" | "working" | "verifying" => "ready",
                    status => status,
                })
                .unwrap_or("pending");
            let worker_reported =
                carried.is_some_and(|old| old.worker_reported && status != "ready");
            let blocked_reason = carried.and_then(|old| old.blocked_reason.as_deref());
            let not_before = carried
                .and_then(|old| old.not_before_unix_ms)
                .map(|value| value.to_string());
            transaction
                .execute(
                    "INSERT INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported, blocked_reason, not_before_unix_ms, readiness_epoch, created_at_unix_ms, updated_at_unix_ms, constraints)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?17, ?18)",
                    params![subject, run_id, generation_id, step.path, step.definition_hash, status, attempt, assignee, serde_json::to_string(&available_to).map_err(internal)?, agentless, title, goals, worker_reported, blocked_reason, not_before, carried.map(|old| old.readiness_epoch).unwrap_or(0), now.to_string(), constraints],
                )
                .map_err(internal)?;
            if let Some(old) = carried {
                append_claim_tx(
                    &transaction,
                    &self.origin,
                    &subject,
                    "step-run.carried",
                    Some(&actor),
                    &json!({"fields": {
                        "source_step_run": old.subject,
                        "source_generation": predecessor_subject,
                        "status": status,
                        "attempt": attempt,
                        "worker_reported": worker_reported
                    }}),
                    &[],
                    None,
                )
                .map_err(internal)?;
            }
        }
        cancel_descendant_mission_runs_tx(
            &transaction,
            &self.origin,
            &predecessor_id,
            &actor,
            "the parent run generation was superseded",
            now,
        )?;
        transaction
            .execute(
                "UPDATE run_generations SET status='superseded', updated_at_unix_ms=?2 WHERE id=?1",
                params![predecessor_id, now.to_string()],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "UPDATE mission_runs SET current_generation_id=?2, status='running', phase='normal', updated_at_unix_ms=?3 WHERE id=?1",
                params![run_id, generation_id, now.to_string()],
            )
            .map_err(internal)?;
        append_claim_tx(
            &transaction,
            &self.origin,
            &predecessor_subject,
            "run-generation.superseded",
            Some(&actor),
            &json!({"fields": {"status": "superseded", "successor": generation_subject, "reason": reason}}),
            &[],
            None,
        )
        .map_err(internal)?;
        append_claim_tx(
            &transaction,
            &self.origin,
            &generation_subject,
            "run-generation.created",
            Some(&actor),
            &json!({"fields": {
                "run": current.subject,
                "revision": mission.revision,
                "predecessor": predecessor_subject,
                "status": "running",
                "reason": reason,
                "compatible_steps": compatible
            }}),
            &[],
            None,
        )
        .map_err(internal)?;
        if let Some(proposal) = proposal {
            let changed = transaction
                .execute(
                    "UPDATE revision_proposals
                     SET status='applied', successor_generation_id=?2, updated_at_unix_ms=?3
                     WHERE id=?1 AND source_generation_id=?4
                       AND status IN ('pending-approval','draining')",
                    params![proposal.id, generation_id, now.to_string(), predecessor_id],
                )
                .map_err(internal)?;
            if changed != 1 {
                return Err(St3Error::new(
                    "stale-revision-proposal",
                    "the revision proposal changed before the cutover",
                ));
            }
            append_claim_tx(
                &transaction,
                &self.origin,
                &proposal.subject,
                "revision-proposal.applied",
                Some(&actor),
                &json!({"fields": {
                    "status": "applied",
                    "successor_generation": generation_subject
                }}),
                &[],
                None,
            )
            .map_err(internal)?;
        }
        let view = mission_run_view_tx(&transaction, run_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn active_mission_runs(&self) -> Result<Vec<MissionRunView>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM mission_runs WHERE status IN ('running','standing','blocked') ORDER BY created_at_unix_ms",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| mission_run_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn next_active_mission_deadline(&self, origin: &str) -> Result<Option<u128>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT mission_run_deadlines.deadline_at_unix_ms
             FROM mission_run_deadlines
             JOIN mission_runs ON mission_runs.id=mission_run_deadlines.run_id
             WHERE mission_runs.status IN ('running','standing','blocked')
               AND mission_runs.phase NOT LIKE 'cleanup-%'
               AND EXISTS (
                 SELECT 1 FROM claims
                 WHERE claims.subject='mission-run/' || mission_runs.id
                   AND claims.kind='mission-run.created'
                   AND claims.origin=?1
               )",
        )?;
        let deadlines = statement
            .query_map([origin], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(deadlines
            .into_iter()
            .filter_map(|value| value.parse::<u128>().ok())
            .min())
    }

    pub fn terminate_mission_run_descendants(&self, run: &str, reason: &str) -> Result<bool> {
        let run = run.strip_prefix("mission-run/").unwrap_or(run);
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let generation: Option<String> = transaction
            .query_row(
                "SELECT current_generation_id FROM mission_runs WHERE id=?1",
                [run],
                |row| row.get(0),
            )
            .optional()?;
        let Some(generation) = generation else {
            return Ok(false);
        };
        let changed = cancel_descendant_mission_runs_tx(
            &transaction,
            &self.origin,
            &generation,
            "daemon/runtime",
            reason,
            now_ms(),
        )?;
        transaction.commit()?;
        Ok(changed)
    }

    pub fn mission_run_origin(&self, run: &str) -> Result<Option<String>> {
        let subject = if run.starts_with("mission-run/") {
            run.to_owned()
        } else {
            format!("mission-run/{run}")
        };
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT origin FROM claims WHERE subject=?1 AND kind='mission-run.created' ORDER BY store_index LIMIT 1",
                [subject],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn active_mission_runs_for_mission(&self, mission: &str) -> Result<Vec<MissionRunView>> {
        let mission = mission.strip_prefix("mission/").unwrap_or(mission);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM mission_runs
             WHERE mission_id=?1 AND status IN ('running','standing','blocked')
             ORDER BY created_at_unix_ms, id",
        )?;
        let ids = statement
            .query_map([mission], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| mission_run_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn terminal_mission_runs(&self) -> Result<Vec<MissionRunView>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM mission_runs WHERE status IN ('completed','failed','cancelled') ORDER BY created_at_unix_ms",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| mission_run_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn mission_runs_for_root(&self, root: &str) -> Result<Vec<MissionRunView>> {
        let root = root.strip_prefix("mission-run/").unwrap_or(root);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM mission_runs WHERE root_run_id=?1 ORDER BY created_at_unix_ms, id",
        )?;
        let ids = statement
            .query_map([root], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| mission_run_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn work(&self, actor: Option<&str>, include_terminal: bool) -> Result<Vec<StepRunView>> {
        let actor = actor.map(|value| normalize_actor(value, "agent"));
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                    lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
             FROM step_runs
             WHERE agentless=0
               AND (
                 ?1 IS NULL
                 OR assignee=?1
                 OR lease_owner=?1
                 OR (status='ready' AND EXISTS (
                   SELECT 1 FROM json_each(step_runs.available_to) WHERE value=?1
                 ))
               )
               AND generation_id=(SELECT current_generation_id FROM mission_runs WHERE id=step_runs.run_id)
               AND (
                 (SELECT phase FROM mission_runs WHERE id=step_runs.run_id) != 'revision-draining'
                 OR status IN ('claimed','working','verifying')
               )
               AND (?2 OR status NOT IN ('completed','failed','cancelled'))
             ORDER BY created_at_unix_ms, step_path",
        )?;
        let rows = statement.query_map(params![actor, include_terminal], step_run_from_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn step_run(&self, subject: &str) -> Result<Option<StepRunView>> {
        let subject = normalize_step_run(subject);
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
                 FROM step_runs WHERE subject=?1",
                [subject],
                step_run_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn work_action(
        &self,
        subject: &str,
        action: &str,
        request: &WorkRequest,
    ) -> Result<StepRunView, St3Error> {
        let subject = normalize_step_run(subject);
        let actor = normalize_actor(
            request.actor.as_deref().ok_or_else(|| {
                St3Error::new("missing-work-actor", "a work action needs an actor")
            })?,
            "agent",
        );
        let now = now_ms();
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(&request.idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let current = transaction
            .query_row(
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
                 FROM step_runs WHERE subject=?1",
                [&subject],
                step_run_from_row,
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| St3Error::new("missing-step-run", format!("step run `{subject}` does not exist")))?;
        let current_generation: String = transaction
            .query_row(
                "SELECT current_generation_id FROM mission_runs WHERE id=?1",
                [current
                    .run
                    .strip_prefix("mission-run/")
                    .unwrap_or(&current.run)],
                |row| row.get(0),
            )
            .map_err(internal)?;
        if generation_id_from_subject(&current.generation) != current_generation {
            return Err(St3Error::new(
                "stale-run-generation",
                format!("step run `{subject}` belongs to a superseded generation"),
            ));
        }
        let run_phase: String = transaction
            .query_row(
                "SELECT phase FROM mission_runs WHERE id=?1",
                [current
                    .run
                    .strip_prefix("mission-run/")
                    .unwrap_or(&current.run)],
                |row| row.get(0),
            )
            .map_err(internal)?;
        if action == "claim" && run_phase == "revision-draining" {
            return Err(St3Error::new(
                "run-generation-draining",
                "the current generation is draining for a revision cutover",
            ));
        }
        let eligible = current.assigned_to.as_deref() == Some(actor.as_str())
            || current
                .available_to
                .iter()
                .any(|candidate| candidate == &actor);
        if current.agentless || !eligible {
            return Err(St3Error::new(
                "work-not-available",
                format!("`{subject}` is not available to `{actor}`"),
            ));
        }
        if matches!(
            current.status.as_str(),
            "completed" | "failed" | "cancelled"
        ) {
            return Err(St3Error::new(
                "terminal-step-run",
                format!("step run `{subject}` is already {}", current.status),
            ));
        }
        let lease_valid = current
            .claim_expires_at_unix_ms
            .is_some_and(|expiry| expiry > now);
        if lease_valid && current.claimant.as_deref() != Some(actor.as_str()) {
            return Err(St3Error::new(
                "work-already-claimed",
                format!("step run `{subject}` has an active lease"),
            ));
        }
        if action != "claim"
            && (!lease_valid || current.claimant.as_deref() != Some(actor.as_str()))
        {
            return Err(St3Error::new(
                "work-not-claimed",
                format!("`{actor}` does not hold an active lease for `{subject}`"),
            ));
        }
        if lease_valid
            && current.claimant.as_deref() == Some(actor.as_str())
            && current.claim_incarnation.is_some()
            && current.claim_incarnation != request.incarnation
        {
            return Err(St3Error::new(
                "wrong-work-incarnation",
                format!("another `{actor}` incarnation holds `{subject}`"),
            ));
        }
        let effective_incarnation = current
            .claim_incarnation
            .clone()
            .or_else(|| request.incarnation.clone());
        let actor_incarnation = effective_incarnation.clone();
        let (status, worker_reported, claimant, claim_incarnation, claim_expiry) = match action {
            "claim" => {
                if current.status != "ready" && current.status != "claimed" {
                    return Err(St3Error::new(
                        "work-not-ready",
                        format!("step run `{subject}` is {}", current.status),
                    ));
                }
                (
                    "claimed",
                    current.worker_reported,
                    Some(actor.clone()),
                    effective_incarnation.clone(),
                    Some(now + 600_000),
                )
            }
            "renew" => (
                current.status.as_str(),
                current.worker_reported,
                Some(actor.clone()),
                effective_incarnation.clone(),
                Some(now + 600_000),
            ),
            "progress" => (
                "working",
                current.worker_reported,
                Some(actor.clone()),
                effective_incarnation,
                Some(now + 600_000),
            ),
            "complete" => ("verifying", true, None, None, None),
            "fail" => ("failed", current.worker_reported, None, None, None),
            "release" => ("ready", current.worker_reported, None, None, None),
            _ => {
                return Err(St3Error::new(
                    "invalid-work-action",
                    format!("work action `{action}` is not registered"),
                ));
            }
        };
        let readiness_epoch = current
            .readiness_epoch
            .saturating_add(u32::from(status == "ready" && current.status != "ready"));
        transaction
            .execute(
                "UPDATE step_runs SET status=?2, worker_reported=?3, lease_owner=?4, lease_incarnation=?5,
                        lease_expires_at_unix_ms=?6, blocked_reason=?7, readiness_epoch=?8,
                        updated_at_unix_ms=?9 WHERE subject=?1",
                params![subject, status, worker_reported, claimant, claim_incarnation, claim_expiry.map(|value| value.to_string()), request.reason, readiness_epoch, now.to_string()],
            )
            .map_err(internal)?;
        renew_nested_ancestor_leases_tx(
            &transaction,
            &current.generation,
            &current.step,
            &actor,
            actor_incarnation.as_deref(),
            now,
        )?;
        let body = json!({"fields": {
            "status": status,
            "summary": request.summary,
            "reason": request.reason,
            "worker_reported": worker_reported,
            "claimant": claimant,
            "claim_incarnation": claim_incarnation,
            "claim_expires_at_unix_ms": claim_expiry,
            "readiness_epoch": readiness_epoch
        }, "evidence": request.evidence});
        let claim_kind = match action {
            "claim" => "work.claimed",
            "renew" => "work.renewed",
            "progress" => "work.progress",
            "complete" => "work.submitted",
            "fail" => "work.failed",
            "release" => "work.released",
            _ => unreachable!("the work action was validated above"),
        };
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            claim_kind,
            Some(&actor),
            &body,
            &request.evidence,
            None,
        )
        .map_err(internal)?;
        let view = transaction.query_row(
            "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                    lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
             FROM step_runs WHERE subject=?1", [&subject], step_run_from_row).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(&request.idempotency_key),
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn set_step_state(
        &self,
        subject: &str,
        status: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let subject = normalize_step_run(subject);
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let current: Option<(String, bool, u32)> = transaction
            .query_row(
                "SELECT step_runs.status,
                        step_runs.generation_id=mission_runs.current_generation_id,
                        step_runs.readiness_epoch
                 FROM step_runs JOIN mission_runs ON mission_runs.id=step_runs.run_id
                 WHERE step_runs.subject=?1",
                [&subject],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((current, is_current, current_epoch)) = current else {
            return Ok(false);
        };
        if !is_current || current == status {
            return Ok(false);
        }
        let now = now_ms();
        let readiness_epoch = current_epoch.saturating_add(u32::from(status == "ready"));
        transaction.execute(
            "UPDATE step_runs SET status=?2, blocked_reason=?3,
                    lease_owner=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_owner END,
                    lease_incarnation=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_incarnation END,
                    lease_expires_at_unix_ms=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_expires_at_unix_ms END,
                    not_before_unix_ms=CASE WHEN ?2='ready' THEN NULL ELSE not_before_unix_ms END,
                    activated_at_unix_ms=CASE WHEN ?2='ready' THEN ?4 ELSE activated_at_unix_ms END,
                    readiness_epoch=?5, updated_at_unix_ms=?4 WHERE subject=?1",
            params![subject, status, reason, now.to_string(), readiness_epoch])?;
        let body = json!({"fields": {"status": status, "reason": reason, "readiness_epoch": readiness_epoch}});
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "step-run.state",
            None,
            &body,
            &[],
            None,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn retry_step(&self, subject: &str, reason: &str, backoff_ms: u64) -> Result<bool> {
        let subject = normalize_step_run(subject);
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let current: Option<(String, u32, bool)> = transaction
            .query_row(
                "SELECT step_runs.status, step_runs.attempt,
                        step_runs.generation_id=mission_runs.current_generation_id
                 FROM step_runs JOIN mission_runs ON mission_runs.id=step_runs.run_id
                 WHERE step_runs.subject=?1",
                [&subject],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((status, attempt, is_current)) = current else {
            return Ok(false);
        };
        if !is_current || status != "failed" {
            return Ok(false);
        }
        let now = now_ms();
        let not_before = now.saturating_add(backoff_ms as u128);
        transaction.execute(
            "UPDATE step_runs SET status='pending', attempt=?2, worker_reported=0, lease_owner=NULL,
                    lease_incarnation=NULL, lease_expires_at_unix_ms=NULL, blocked_reason=?3,
                    not_before_unix_ms=?4, activated_at_unix_ms=NULL, updated_at_unix_ms=?5 WHERE subject=?1",
            params![subject, attempt.saturating_add(1), reason, not_before.to_string(), now.to_string()])?;
        let body = json!({"fields": {"status": "pending", "attempt": attempt.saturating_add(1), "reason": reason, "not_before_unix_ms": not_before}});
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "step-run.retried",
            None,
            &body,
            &[],
            None,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn set_mission_run_state(
        &self,
        run: &str,
        status: &str,
        phase: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let run = run.strip_prefix("mission-run/").unwrap_or(run);
        let subject = format!("mission-run/{run}");
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let current: Option<(String, String)> = transaction
            .query_row(
                "SELECT status, phase FROM mission_runs WHERE id=?1",
                [run],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if current
            .as_ref()
            .is_some_and(|current| current.0 == status && current.1 == phase)
            || current.is_none()
        {
            return Ok(false);
        }
        if current.as_ref().is_some_and(|(_, current_phase)| {
            current_phase == "terminal"
                || (current_phase.starts_with("cleanup-") && phase != "terminal")
                || (current_phase == "final-cancelled"
                    && !matches!(phase, "final-cancelled" | "cleanup-cancelled" | "terminal"))
        }) {
            return Ok(false);
        }
        let now = now_ms();
        transaction.execute(
            "UPDATE mission_runs SET status=?2, phase=?3, updated_at_unix_ms=?4 WHERE id=?1",
            params![run, status, phase, now.to_string()],
        )?;
        transaction.execute(
            "UPDATE run_generations SET status=?2, updated_at_unix_ms=?3
             WHERE id=(SELECT current_generation_id FROM mission_runs WHERE id=?1)",
            params![run, status, now.to_string()],
        )?;
        let body = json!({"fields": {"status": status, "phase": phase, "reason": reason}});
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "mission-run.state",
            None,
            &body,
            &[],
            None,
        )?;
        let generation: String = transaction.query_row(
            "SELECT current_generation_id FROM mission_runs WHERE id=?1",
            [run],
            |row| row.get(0),
        )?;
        append_claim_tx(
            &transaction,
            &self.origin,
            &format!("run-generation/{generation}"),
            "run-generation.state",
            None,
            &body,
            &[],
            None,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn apply_internal(
        &self,
        intent: &NormalizedIntent,
        key: &str,
    ) -> Result<ApplyResponse, St3Error> {
        let source = IntentInput {
            kdl: String::new(),
            source_name: Some("st3 reconciler".into()),
        };
        let mission = self.mission(intent, source)?;
        if !mission.blockers.is_empty() {
            return Err(St3Error::new(
                "internal-mission-blocked",
                mission.blockers.join("; "),
            ));
        }
        let materialization = serde_json::to_vec(&(
            key,
            &mission.normalized,
            &mission.subject_tokens,
        ))
        .map_err(internal)?;
        let key = format!(
            "internal:{key}:{}",
            hex::encode(Sha256::digest(materialization))
        );
        self.apply(intent, &mission.subject_tokens, &key)
    }

    pub fn mission(
        &self,
        intent: &NormalizedIntent,
        resolved_intent: IntentInput,
    ) -> Result<MissionResponse, St3Error> {
        self.mission_at(intent, resolved_intent, None)
    }

    pub fn mission_at(
        &self,
        intent: &NormalizedIntent,
        resolved_intent: IntentInput,
        at_index: Option<u64>,
    ) -> Result<MissionResponse, St3Error> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let current_index = current_index(&connection).map_err(internal)?;
        let store_index = selected_index(current_index, at_index)?;
        let mut changes = Vec::new();
        let mut tokens = BTreeMap::new();
        let mut actions = Vec::new();
        let mut blockers = Vec::new();
        let mut warnings = Vec::new();

        for reference in &intent.document_refs {
            let Some((name, hash)) = reference.rsplit_once('@') else {
                blockers.push(format!(
                    "document `{reference}` has no selected binding; run `st3 doc put` first"
                ));
                continue;
            };
            let exists = connection
                .query_row(
                    "SELECT 1 FROM documents WHERE name = ?1 AND hash = ?2 AND created_index<=?3",
                    params![name, hash, store_index],
                    |_| Ok(()),
                )
                .optional()
                .map_err(internal)?
                .is_some();
            if !exists {
                blockers.push(format!(
                    "missing document `{reference}`; run `st3 doc put` first"
                ));
                continue;
            }
            let latest: Option<String> = connection
                .query_row(
                    "SELECT hash FROM documents WHERE name = ?1 AND created_index<=?2 ORDER BY created_index DESC LIMIT 1",
                    params![name, store_index],
                    |row| row.get(0),
                )
                .optional()
                .map_err(internal)?;
            if latest.as_deref() != Some(hash) {
                warnings.push(format!(
                    "`{reference}` is not the latest version of `{name}`; the requested version remains valid"
                ));
            }
        }

        let known = |subject: &str| -> Result<bool, St3Error> {
            if intent.subjects.contains_key(subject) {
                return Ok(true);
            }
            connection
                .query_row(
                    "SELECT 1 FROM claims WHERE subject=?1 AND store_index<=?2 LIMIT 1",
                    params![subject, store_index],
                    |_| Ok(()),
                )
                .optional()
                .map(|value| value.is_some())
                .map_err(internal)
        };
        for desired in intent.subjects.values() {
            if desired.kind == "agent" {
                for grouping in crate::graph::agent_under(&desired.desired) {
                    if grouping.agent == desired.subject {
                        warnings.push(format!(
                            "agent `{}` is grouped under itself",
                            desired.subject
                        ));
                    } else if !known(&grouping.agent)? {
                        warnings.push(format!(
                            "agent `{}` is grouped under missing agent `{}`",
                            desired.subject, grouping.agent
                        ));
                    }
                }
            }
            if desired.kind == "message"
                && let Some(to) = canonical_child_string(&desired.desired, "to")
            {
                let recipient = if to == "requester" || to.contains('/') {
                    to
                } else {
                    format!("agent/{to}")
                };
                if recipient != "requester"
                    && !recipient.starts_with("person/")
                    && !known(&recipient)?
                {
                    blockers.push(format!(
                        "message `{}` references undeclared recipient `{recipient}`",
                        desired.subject
                    ));
                }
            }
            if desired.kind == "observer"
                && let Some(observer) = crate::graph::observer_spec(&desired.desired)
                && !observer.stopped
                && !known(&observer.resource)?
            {
                warnings.push(format!(
                    "observer `{}` references missing resource `{}`",
                    desired.subject, observer.resource
                ));
            }
            if desired.kind == "subscription"
                && let Some(subscription) = crate::graph::subscription_spec(&desired.desired)
                && !subscription.stopped
            {
                if !known(&subscription.observer)? {
                    warnings.push(format!(
                        "subscription `{}` references missing observer `{}`",
                        desired.subject, subscription.observer
                    ));
                }
                if subscription.delivery == "message" && !known(&subscription.to)? {
                    warnings.push(format!(
                        "subscription `{}` has missing delivery target `{}`",
                        desired.subject, subscription.to
                    ));
                }
            }
        }
        for mission in intent.missions.values() {
            let mut selectors = mission.work_selector.iter().cloned().collect::<Vec<_>>();
            selectors.extend(
                flatten_mission_step_specs(mission)
                    .into_iter()
                    .filter_map(|step| step.work_selector.clone()),
            );
            for selector in &selectors {
                let agents: &[String] = match selector {
                    WorkSelector::Assigned { agent } => std::slice::from_ref(agent),
                    WorkSelector::Available { agents } => agents.as_slice(),
                    WorkSelector::Agentless => &[],
                };
                for agent in agents {
                    let owned_runtime =
                        agent
                            .strip_prefix("agent/${ST_MISSION_RUN}/")
                            .is_some_and(|local| {
                                mission.revision_owners.iter().any(|owner| {
                                    owner == &format!("agent/{local}")
                                        || owner
                                            .rsplit_once('.')
                                            .is_some_and(|(_, name)| name == local)
                                })
                            });
                    if !owned_runtime && !mission.revision_owners.contains(agent) && !known(agent)?
                    {
                        warnings.push(format!(
                            "mission `{}` references missing eligible agent `{agent}`",
                            mission.subject
                        ));
                    }
                }
            }
        }
        let grouping_edges = intent
            .subjects
            .values()
            .filter(|desired| desired.kind == "agent")
            .map(|desired| {
                (
                    desired.subject.clone(),
                    crate::graph::agent_under(&desired.desired)
                        .into_iter()
                        .map(|under| under.agent)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for start in grouping_edges.keys() {
            let mut frontier = vec![start.as_str()];
            let mut visited = BTreeSet::new();
            while let Some(agent) = frontier.pop() {
                if !visited.insert(agent) {
                    continue;
                }
                for parent in grouping_edges.get(agent).into_iter().flatten() {
                    if parent == start && agent != start {
                        warnings.push(format!("agent grouping for `{start}` contains a cycle"));
                    } else if grouping_edges.contains_key(parent) {
                        frontier.push(parent);
                    }
                }
            }
        }
        blockers.sort();
        blockers.dedup();
        warnings.sort();
        warnings.dedup();

        for (subject, desired) in &intent.subjects {
            let current = desired_row_at(&connection, subject, at_index).map_err(internal)?;
            let revision = desired_revision(desired);
            tokens.insert(
                subject.clone(),
                intent_leaves_at(&connection, subject, at_index).map_err(internal)?,
            );
            if current.as_ref().is_some_and(|row| row.revision == revision) {
                continue;
            }
            changes.push(SubjectChange {
                subject: subject.clone(),
                change: if current.is_some() {
                    "update"
                } else {
                    "create"
                }
                .into(),
                old_revision: current.map(|row| row.revision),
                new_revision: revision,
            });
            if desired.kind == "stop" {
                actions.push(PlannedAction {
                    subject: subject.clone(),
                    action: "stop".into(),
                    reason: "the desired state explicitly stops this member".into(),
                });
            } else if desired.member.is_some() {
                actions.push(PlannedAction {
                    subject: subject.clone(),
                    action: "observe-or-start".into(),
                    reason: "the desired member is active".into(),
                });
            }
        }
        for mission in intent.missions.values() {
            let subject = mission.subject.clone();
            let current: Option<(String, String)> = connection
                .query_row(
                    "SELECT revision, claim_id FROM mission_definitions WHERE mission_id=?1",
                    [&mission.id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(internal)?;
            tokens.insert(
                subject.clone(),
                current
                    .as_ref()
                    .map(|(_, claim)| vec![claim.clone()])
                    .unwrap_or_default(),
            );
            if current
                .as_ref()
                .is_some_and(|(revision, _)| revision == &mission.revision)
            {
                continue;
            }
            changes.push(SubjectChange {
                subject: subject.clone(),
                change: if current.is_some() {
                    "update"
                } else {
                    "create"
                }
                .into(),
                old_revision: current.map(|(revision, _)| revision),
                new_revision: mission.revision.clone(),
            });
            actions.push(PlannedAction {
                subject,
                action: "publish-mission".into(),
                reason: "the immutable mission revision is not published".into(),
            });
        }
        for declaration in intent.mission_runs.values() {
            prepare_mission_run_declaration(
                &connection,
                declaration,
                store_index,
                &mut tokens,
                &mut actions,
                &mut blockers,
                &mut warnings,
            )?;
        }
        for declaration in intent.planning_sessions.values() {
            prepare_planning_session_declaration(
                &connection,
                declaration,
                store_index,
                &mut tokens,
                &mut actions,
                &mut blockers,
            )?;
        }
        for refresh in &intent.resource_refreshes {
            tokens.entry(refresh.resource.clone()).or_insert(
                claim_ids_at(&connection, &refresh.resource, Some(store_index))
                    .map_err(internal)?
                    .into_iter()
                    .last()
                    .into_iter()
                    .collect(),
            );
            let exists = connection
                .query_row(
                    "SELECT 1 FROM desired WHERE subject=?1 AND kind='resource'",
                    [&refresh.resource],
                    |_| Ok(()),
                )
                .optional()
                .map_err(internal)?
                .is_some();
            if exists {
                match publication_operation_is_new(
                    &connection,
                    &refresh.resource,
                    "refresh",
                    &refresh.id,
                    refresh,
                ) {
                    Ok(true) => actions.push(PlannedAction {
                        subject: refresh.resource.clone(),
                        action: format!("refresh:{}", refresh.id),
                        reason: "the publication requests a fresh observation".into(),
                    }),
                    Ok(false) => {}
                    Err(error) => blockers.push(error.message),
                }
            } else {
                blockers.push(format!("resource `{}` does not exist", refresh.resource));
            }
        }

        blockers.sort();
        blockers.dedup();
        warnings.sort();
        warnings.dedup();

        Ok(MissionResponse {
            store_index,
            source_hash: intent.source_hash.clone(),
            normalized: intent.normalized.clone(),
            resolved_intent,
            changes,
            predicted_actions: actions,
            blockers,
            warnings,
            subject_tokens: tokens,
            mission_revisions: intent
                .missions
                .values()
                .map(|mission| (mission.subject.clone(), mission.revision.clone()))
                .collect(),
        })
    }

    pub fn apply(
        &self,
        intent: &NormalizedIntent,
        expected: &BTreeMap<String, Vec<String>>,
        idempotency_key: &str,
    ) -> Result<ApplyResponse, St3Error> {
        self.apply_as(intent, expected, idempotency_key, None)
    }

    pub fn apply_as(
        &self,
        intent: &NormalizedIntent,
        expected: &BTreeMap<String, Vec<String>>,
        idempotency_key: &str,
        actor: Option<&str>,
    ) -> Result<ApplyResponse, St3Error> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id = ?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        validate_documents(&transaction, &intent.document_refs)?;
        for subject in intent.subjects.keys() {
            let actual = intent_leaves_tx(&transaction, subject).map_err(internal)?;
            let expected = expected.get(subject).ok_or_else(|| {
                St3Error::new(
                    "missing-subject-token",
                    format!("apply omitted the subject token for `{subject}`"),
                )
                .with_detail("subject", subject.clone())
                .with_detail("current_heads", json!(actual.clone()))
            })?;
            if actual != *expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!("the desired state for `{subject}` changed after planning"),
                )
                .with_detail("subject", subject.clone())
                .with_detail("expected_heads", json!(expected))
                .with_detail("current_heads", json!(actual)));
            }
        }
        for mission in intent.missions.values() {
            let actual =
                mission_definition_token_tx(&transaction, &mission.id).map_err(internal)?;
            let expected = expected.get(&mission.subject).ok_or_else(|| {
                St3Error::new(
                    "missing-subject-token",
                    format!("apply omitted the subject token for `{}`", mission.subject),
                )
                .with_detail("subject", mission.subject.clone())
                .with_detail("current_heads", json!(actual.clone()))
            })?;
            if actual != *expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!(
                        "the published mission for `{}` changed after planning",
                        mission.subject
                    ),
                )
                .with_detail("subject", mission.subject.clone())
                .with_detail("expected_heads", json!(expected))
                .with_detail("current_heads", json!(actual)));
            }
        }
        for declaration in intent.mission_runs.values() {
            let actual = latest_claim_id_tx(&transaction, &declaration.subject)
                .map_err(internal)?
                .into_iter()
                .collect::<Vec<_>>();
            let expected = expected.get(&declaration.subject).ok_or_else(|| {
                St3Error::new(
                    "missing-subject-token",
                    format!(
                        "apply omitted the subject token for `{}`",
                        declaration.subject
                    ),
                )
            })?;
            if actual != *expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!(
                        "mission run `{}` changed after planning",
                        declaration.subject
                    ),
                ));
            }
        }
        for declaration in intent.planning_sessions.values() {
            let actual = latest_claim_id_tx(&transaction, &declaration.subject)
                .map_err(internal)?
                .into_iter()
                .collect::<Vec<_>>();
            let expected = expected.get(&declaration.subject).ok_or_else(|| {
                St3Error::new(
                    "missing-subject-token",
                    format!(
                        "apply omitted the subject token for `{}`",
                        declaration.subject
                    ),
                )
            })?;
            if actual != *expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!(
                        "planning session `{}` changed after planning",
                        declaration.subject
                    ),
                ));
            }
        }
        let desired_changed = intent.subjects.iter().any(|(subject, desired)| {
            current_desired_row_tx(&transaction, subject)
                .map(|current| {
                    current
                        .as_ref()
                        .is_none_or(|row| row.revision != desired_revision(desired))
                })
                .unwrap_or(true)
        });
        let missions_changed = intent.missions.values().any(|mission| {
            transaction
                .query_row(
                    "SELECT revision FROM mission_definitions WHERE mission_id=?1",
                    [&mission.id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map(|current| current.as_deref() != Some(mission.revision.as_str()))
                .unwrap_or(true)
        });
        let mut operations_changed = false;
        for declaration in intent.mission_runs.values() {
            let run_id = declaration
                .subject
                .strip_prefix("mission-run/")
                .unwrap_or(&declaration.subject);
            if declaration.creation.is_some()
                && transaction
                    .query_row("SELECT 1 FROM mission_runs WHERE id=?1", [run_id], |_| {
                        Ok(())
                    })
                    .optional()
                    .map_err(internal)?
                    .is_none()
            {
                operations_changed = true;
            }
            for revision in declaration.revisions.values() {
                operations_changed |= publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "revision",
                    &revision.id,
                    revision,
                )?;
            }
            for reset in declaration.resets.values() {
                operations_changed |= publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "reset",
                    &reset.id,
                    reset,
                )?;
            }
            for cancellation in declaration.cancellations.values() {
                operations_changed |= publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "cancellation",
                    &cancellation.id,
                    cancellation,
                )?;
            }
        }
        for declaration in intent.planning_sessions.values() {
            let id = declaration
                .subject
                .strip_prefix("planning-session/")
                .unwrap_or(&declaration.subject);
            if declaration.creation.is_some()
                && transaction
                    .query_row("SELECT 1 FROM planning_sessions WHERE id=?1", [id], |_| {
                        Ok(())
                    })
                    .optional()
                    .map_err(internal)?
                    .is_none()
            {
                operations_changed = true;
            }
            for feedback in declaration.feedback.values() {
                operations_changed |= publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "feedback",
                    &feedback.id,
                    feedback,
                )?;
            }
            for cancellation in declaration.cancellations.values() {
                operations_changed |= publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "cancellation",
                    &cancellation.id,
                    cancellation,
                )?;
            }
        }
        for refresh in &intent.resource_refreshes {
            operations_changed |= publication_operation_is_new(
                &transaction,
                &refresh.resource,
                "refresh",
                &refresh.id,
                refresh,
            )?;
        }
        let changed = desired_changed || missions_changed || operations_changed;
        if !changed {
            let store_index = current_index_tx(&transaction).map_err(internal)?;
            let mut subject_tokens = intent
                .subjects
                .keys()
                .map(|subject| {
                    intent_leaves_tx(&transaction, subject)
                        .map(|heads| (subject.clone(), heads))
                        .map_err(internal)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            for mission in intent.missions.values() {
                subject_tokens.insert(
                    mission.subject.clone(),
                    mission_definition_token_tx(&transaction, &mission.id).map_err(internal)?,
                );
            }
            for declaration in intent.mission_runs.values() {
                subject_tokens.insert(
                    declaration.subject.clone(),
                    latest_claim_id_tx(&transaction, &declaration.subject)
                        .map_err(internal)?
                        .into_iter()
                        .collect(),
                );
            }
            for declaration in intent.planning_sessions.values() {
                subject_tokens.insert(
                    declaration.subject.clone(),
                    latest_claim_id_tx(&transaction, &declaration.subject)
                        .map_err(internal)?
                        .into_iter()
                        .collect(),
                );
            }
            let response = ApplyResponse {
                changed: false,
                store_index,
                batch_id: None,
                claim_ids: Vec::new(),
                subject_tokens,
                reconcile_subjects: Vec::new(),
                resolved_kdl: String::new(),
                operations: Vec::new(),
            };
            transaction
                .execute(
                    "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                    params![
                        opaque_cache_key(idempotency_key),
                        serde_json::to_string(&response).map_err(internal)?
                    ],
                )
                .map_err(internal)?;
            transaction.commit().map_err(internal)?;
            return Ok(response);
        }
        let now = now_ms();
        let sequence = next_replica_sequence(&transaction, &self.origin).map_err(internal)?;
        let previous_hash = previous_batch_hash(&transaction, &self.origin).map_err(internal)?;
        let batch_hash = batch_header_hash(&self.origin, sequence, previous_hash.as_deref(), now)
            .map_err(internal)?;
        let batch_id = format!("batch/{}/{sequence}/{batch_hash}", self.origin);
        transaction
            .execute(
                "INSERT INTO batches(id, origin, replica_sequence, previous_hash, hash, accepted_at_unix_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![batch_id, self.origin, sequence, previous_hash, batch_hash, now.to_string()],
            )
            .map_err(internal)?;

        let mut claim_ids = Vec::new();
        let mut tokens = BTreeMap::new();
        let mut reconcile_subjects = Vec::new();
        let mut operation_receipts = Vec::new();
        for (subject, desired) in &intent.subjects {
            let revision = desired_revision(desired);
            let current = current_desired_row_tx(&transaction, subject).map_err(internal)?;
            if current.as_ref().is_some_and(|row| row.revision == revision) {
                tokens.insert(
                    subject.clone(),
                    intent_leaves_tx(&transaction, subject).map_err(internal)?,
                );
                continue;
            }
            let predecessors = intent_leaves_tx(&transaction, subject).map_err(internal)?;
            let body = serde_json::to_value(desired).map_err(internal)?;
            let claim_id = claim_hash(
                &batch_id,
                subject,
                "intent.desired",
                &self.origin,
                None,
                &body,
                &predecessors,
            )
            .map_err(internal)?;
            let store_index = insert_claim(
                &transaction,
                &claim_id,
                &batch_id,
                subject,
                "intent.desired",
                &self.origin,
                None,
                &body,
                &predecessors,
                now,
            )
            .map_err(internal)?;
            transaction
                .execute(
                    "INSERT INTO desired(subject, kind, revision, claim_id, body, member, owner_run, owner_generation, owner_step) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(subject) DO UPDATE SET kind=excluded.kind, revision=excluded.revision, claim_id=excluded.claim_id, body=excluded.body, member=excluded.member, owner_run=excluded.owner_run, owner_generation=excluded.owner_generation, owner_step=excluded.owner_step",
                    params![
                        subject,
                        desired.kind,
                        revision,
                        claim_id,
                        serde_json::to_string(&desired.desired).map_err(internal)?,
                        desired.member.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                        desired.owner_run,
                        desired.owner_generation,
                        desired.owner_step,
                    ],
                )
                .map_err(internal)?;
            insert_event(&transaction, store_index, "intent.desired", subject, &body)
                .map_err(internal)?;
            claim_ids.push(claim_id.clone());
            tokens.insert(subject.clone(), vec![claim_id]);
            reconcile_subjects.push(subject.clone());
        }
        for mission in intent.missions.values() {
            let current: Option<String> = transaction
                .query_row(
                    "SELECT revision FROM mission_definitions WHERE mission_id=?1",
                    [&mission.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(internal)?;
            if current.as_deref() == Some(mission.revision.as_str()) {
                tokens.insert(
                    mission.subject.clone(),
                    mission_definition_token_tx(&transaction, &mission.id).map_err(internal)?,
                );
                continue;
            }
            let predecessors =
                mission_definition_token_tx(&transaction, &mission.id).map_err(internal)?;
            let body = serde_json::to_value(mission).map_err(internal)?;
            let claim_id = claim_hash(
                &batch_id,
                &mission.subject,
                "mission.published",
                &self.origin,
                None,
                &body,
                &predecessors,
            )
            .map_err(internal)?;
            let store_index = insert_claim(
                &transaction,
                &claim_id,
                &batch_id,
                &mission.subject,
                "mission.published",
                &self.origin,
                None,
                &body,
                &predecessors,
                now,
            )
            .map_err(internal)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO mission_revisions(mission_id, revision, state, body, claim_id, created_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![mission.id, mission.revision, mission_state_name(&mission.state), serde_json::to_string(mission).map_err(internal)?, claim_id, store_index],
                )
                .map_err(internal)?;
            transaction
                .execute(
                    "INSERT INTO mission_definitions(mission_id, revision, state, claim_id) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(mission_id) DO UPDATE SET revision=excluded.revision, state=excluded.state, claim_id=excluded.claim_id",
                    params![mission.id, mission.revision, mission_state_name(&mission.state), claim_id],
                )
                .map_err(internal)?;
            insert_event(
                &transaction,
                store_index,
                "mission.published",
                &mission.subject,
                &body,
            )
            .map_err(internal)?;
            claim_ids.push(claim_id.clone());
            tokens.insert(mission.subject.clone(), vec![claim_id]);
        }
        for declaration in intent.mission_runs.values() {
            let created =
                create_declared_mission_run_tx(&transaction, &self.origin, declaration, &batch_id)?;
            if !created.is_empty() {
                operation_receipts.push(PlannedAction {
                    subject: declaration.subject.clone(),
                    action: "start-mission-run".into(),
                    reason: "the named mission run was created".into(),
                });
            }
            claim_ids.extend(created);
            for revision in declaration.revisions.values() {
                if !publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "revision",
                    &revision.id,
                    revision,
                )? {
                    continue;
                }
                let (ids, proposed) = adopt_declared_mission_revision_tx(
                    &transaction,
                    &self.origin,
                    declaration,
                    revision,
                    &batch_id,
                    actor,
                )?;
                claim_ids.extend(ids);
                let receipt = append_publication_operation_tx(
                    &transaction,
                    &self.origin,
                    &declaration.subject,
                    "revision",
                    &revision.id,
                    revision,
                    actor,
                    &batch_id,
                )?;
                claim_ids.push(receipt.id);
                operation_receipts.push(PlannedAction {
                    subject: declaration.subject.clone(),
                    action: format!(
                        "{}:{}",
                        if proposed {
                            "propose-revision"
                        } else {
                            "revise"
                        },
                        revision.id
                    ),
                    reason: revision.reason.clone(),
                });
                if !proposed && let Some(cancellation) = &revision.cancellation {
                    let ids = cancel_mission_run_tx(
                        &transaction,
                        &self.origin,
                        &declaration.subject,
                        &cancellation.reason,
                        &batch_id,
                        now,
                    )
                    .map_err(internal)?;
                    claim_ids.extend(ids);
                }
            }
            for reset in declaration.resets.values() {
                if !publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "reset",
                    &reset.id,
                    reset,
                )? {
                    continue;
                }
                let runtime = runtime_subject_for_run(
                    declaration
                        .subject
                        .strip_prefix("mission-run/")
                        .unwrap_or(&declaration.subject),
                    &reset.runtime,
                );
                let reset_claim = reset_declared_runtime_tx(
                    &transaction,
                    &self.origin,
                    declaration,
                    reset,
                    &runtime,
                    &batch_id,
                )?;
                claim_ids.push(reset_claim.id);
                let receipt = append_publication_operation_tx(
                    &transaction,
                    &self.origin,
                    &declaration.subject,
                    "reset",
                    &reset.id,
                    reset,
                    actor,
                    &batch_id,
                )?;
                claim_ids.push(receipt.id);
                reconcile_subjects.push(runtime.clone());
                operation_receipts.push(PlannedAction {
                    subject: runtime,
                    action: format!("reset:{}", reset.id),
                    reason: reset.reason.clone(),
                });
            }
            for cancellation in declaration.cancellations.values() {
                if !publication_operation_is_new(
                    &transaction,
                    &declaration.subject,
                    "cancellation",
                    &cancellation.id,
                    cancellation,
                )? {
                    continue;
                }
                let ids = cancel_mission_run_tx(
                    &transaction,
                    &self.origin,
                    &declaration.subject,
                    &cancellation.reason,
                    &batch_id,
                    now,
                )
                .map_err(internal)?;
                if !ids.is_empty() {
                    operation_receipts.push(PlannedAction {
                        subject: declaration.subject.clone(),
                        action: format!("cancel:{}", cancellation.id),
                        reason: cancellation.reason.clone(),
                    });
                }
                claim_ids.extend(ids);
                let receipt = append_publication_operation_tx(
                    &transaction,
                    &self.origin,
                    &declaration.subject,
                    "cancellation",
                    &cancellation.id,
                    cancellation,
                    actor,
                    &batch_id,
                )?;
                claim_ids.push(receipt.id);
            }
            tokens.insert(
                declaration.subject.clone(),
                latest_claim_id_tx(&transaction, &declaration.subject)
                    .map_err(internal)?
                    .into_iter()
                    .collect(),
            );
            reconcile_subjects.push(declaration.subject.clone());
        }
        for declaration in intent.planning_sessions.values() {
            let ids = apply_planning_session_declaration_tx(
                &transaction,
                &self.origin,
                declaration,
                &batch_id,
                actor,
                &mut operation_receipts,
            )?;
            claim_ids.extend(ids);
            tokens.insert(
                declaration.subject.clone(),
                latest_claim_id_tx(&transaction, &declaration.subject)
                    .map_err(internal)?
                    .into_iter()
                    .collect(),
            );
            reconcile_subjects.push(declaration.subject.clone());
        }
        for refresh in &intent.resource_refreshes {
            if !publication_operation_is_new(
                &transaction,
                &refresh.resource,
                "refresh",
                &refresh.id,
                refresh,
            )? {
                continue;
            }
            let ids = request_declared_resource_refresh_tx(
                &transaction,
                &self.origin,
                refresh,
                &batch_id,
            )?;
            claim_ids.extend(ids);
            let receipt = append_publication_operation_tx(
                &transaction,
                &self.origin,
                &refresh.resource,
                "refresh",
                &refresh.id,
                refresh,
                actor,
                &batch_id,
            )?;
            claim_ids.push(receipt.id);
            operation_receipts.push(PlannedAction {
                subject: refresh.resource.clone(),
                action: format!("refresh:{}", refresh.id),
                reason: "the refresh request was accepted".into(),
            });
        }
        let store_index = current_index_tx(&transaction).map_err(internal)?;
        let response = ApplyResponse {
            changed: true,
            store_index,
            batch_id: Some(batch_id),
            claim_ids,
            subject_tokens: tokens,
            reconcile_subjects,
            resolved_kdl: String::new(),
            operations: operation_receipts,
        };
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&response).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(response)
    }

    pub fn put_document(
        &self,
        name: &str,
        bytes: &[u8],
        expected_document: &Option<String>,
        idempotency_key: &str,
    ) -> Result<DocumentVersion, St3Error> {
        validate_document_name(name)?;
        if bytes.len() > 1024 * 1024 {
            return Err(St3Error::new(
                "document-too-large",
                "one document cannot exceed 1 MiB",
            ));
        }
        std::str::from_utf8(bytes).map_err(|_| {
            St3Error::new("document-not-text", "a document must contain valid UTF-8")
        })?;
        let hash = hex::encode(Sha256::digest(bytes));
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        if let Some(version) = find_document(&connection, name, &hash).map_err(internal)? {
            return Ok(version);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let current: Option<String> = transaction
            .query_row(
                "SELECT binding_claim_id FROM documents WHERE name=?1 ORDER BY created_index DESC LIMIT 1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(internal)?;
        if &current != expected_document {
            return Err(St3Error::new(
                "stale-document-token",
                format!("the selected binding for `{name}` changed before the post"),
            )
            .with_detail("subject", name.to_owned())
            .with_detail("expected_head", json!(expected_document))
            .with_detail("current_head", json!(current)));
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO blobs(hash, bytes, size) VALUES (?1, ?2, ?3)",
                params![hash, bytes, bytes.len() as u64],
            )
            .map_err(internal)?;
        let body = json!({ "name": name, "hash": hash, "size": bytes.len() });
        let record = append_claim_tx(
            &transaction,
            &self.origin,
            name,
            "doc.bound",
            None,
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO documents(name, hash, created_index, binding_claim_id) VALUES (?1, ?2, ?3, ?4)",
                params![name, hash, record.store_index, record.id],
            )
            .map_err(internal)?;
        let version = DocumentVersion {
            name: name.into(),
            hash,
            size: bytes.len() as u64,
            created_index: record.store_index,
            latest: true,
            binding_claim_id: record.id,
        };
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(idempotency_key),
                    serde_json::to_string(&version).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(version)
    }

    pub fn get_document(&self, name: &str, hash: &str) -> Result<Option<Vec<u8>>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT b.bytes FROM documents d JOIN blobs b ON b.hash=d.hash WHERE d.name=?1 AND d.hash=?2",
                params![name, hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_documents(&self, name: Option<&str>) -> Result<Vec<DocumentVersion>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let query = "SELECT d.name, d.hash, b.size, d.created_index,
                     d.created_index=(SELECT MAX(n.created_index) FROM documents n WHERE n.name=d.name)
                     ,d.binding_claim_id
                     FROM documents d JOIN blobs b ON b.hash=d.hash
                     WHERE (?1 IS NULL OR d.name=?1) ORDER BY d.name, d.created_index DESC";
        let mut statement = connection.prepare(query)?;
        let rows = statement.query_map([name], |row| {
            Ok(DocumentVersion {
                name: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
                created_index: row.get(3)?,
                latest: row.get::<_, i64>(4)? != 0,
                binding_claim_id: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn append_claim(&self, input: &ClaimInput) -> Result<ClaimRecord, St3Error> {
        self.validate_claim_input(input)?;
        let operation = claim_operation(input)?;
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some((operation_id, request_digest)) = &operation
            && let Some((stored_digest, canonical_claim, state)) =
                operation_tx(&connection, operation_id).map_err(internal)?
        {
            if state == "conflict" {
                return Err(St3Error::new(
                    "idempotency-conflict",
                    "the replicated operation has conflicting requests",
                )
                .with_detail("operation_id", operation_id.clone()));
            }
            if stored_digest != *request_digest {
                return Err(St3Error::new(
                    "idempotency-mismatch",
                    "the idempotency key already identifies a different request",
                )
                .with_detail("operation_id", operation_id.clone())
                .with_detail("stored_digest", stored_digest)
                .with_detail("request_digest", request_digest.clone()));
            }
            return claim_by_id_tx(&connection, &canonical_claim)
                .map_err(internal)?
                .ok_or_else(|| St3Error::new("internal", "the operation claim is missing"));
        }
        let transaction = connection.transaction().map_err(internal)?;
        for evidence in &input.evidence {
            let exists = transaction
                .query_row("SELECT 1 FROM claims WHERE id=?1", [evidence], |_| Ok(()))
                .optional()
                .map_err(internal)?
                .is_some();
            if !exists {
                return Err(St3Error::new(
                    "missing-evidence",
                    format!("evidence claim `{evidence}` is not stored"),
                ));
            }
        }
        if let Some(expected) = &input.expected_subject {
            let actual = latest_claim_id_tx(&transaction, &input.subject).map_err(internal)?;
            if &actual != expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!("subject `{}` changed", input.subject),
                )
                .with_detail("subject", input.subject.clone())
                .with_detail("expected_head", json!(expected))
                .with_detail("current_head", json!(actual)));
            }
        }
        let stored_fields = normalize_resource_observation(&transaction, input)?;
        validate_message_transition(&transaction, input)?;
        let predecessor = latest_claim_id_tx(&transaction, &input.subject).map_err(internal)?;
        let predecessors = predecessor.into_iter().collect::<Vec<_>>();
        let mut body = json!({
            "fields": stored_fields.as_ref().unwrap_or(&input.fields),
            "evidence": input.evidence,
        });
        if let Some((operation_id, request_digest)) = &operation {
            body.as_object_mut()
                .expect("the claim body is an object")
                .insert(
                    "_operation".into(),
                    json!({ "id": operation_id, "request_digest": request_digest }),
                );
        }
        let record = append_claim_tx(
            &transaction,
            &self.origin,
            &input.subject,
            &input.kind,
            input.actor.as_deref(),
            &body,
            &predecessors,
            None,
        )
        .map_err(claim_append_error)?;
        if operation.is_some() {
            register_operation_tx(&transaction, &record).map_err(internal)?;
        }
        transaction.commit().map_err(internal)?;
        Ok(record)
    }

    pub fn validate_claim_input(&self, input: &ClaimInput) -> Result<(), St3Error> {
        validate_claim_kind(&input.kind)?;
        validate_claim_subject(&input.subject)?;
        if let Some(actor) = &input.actor {
            validate_actor(actor)?;
        }
        validate_claim_fields(input)?;
        claim_operation(input)?;
        Ok(())
    }

    pub fn append_client_claim(&self, input: &ClaimInput) -> Result<ClaimRecord, St3Error> {
        st3_schema::registry()
            .validate_public_claim(
                &input.subject,
                &input.kind,
                &input.fields,
                input.actor.as_deref(),
            )
            .map_err(|error| St3Error::new(error.code, error.message))?;
        self.append_claim(input)
    }

    pub fn idempotent_claim(&self, key: &str) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(key)],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|response| serde_json::from_str(&response).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn operation_claim(&self, key: &str) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let operation_id = operation_id_for_key(key);
        connection
            .query_row(
                "SELECT claims.id, claims.store_index, claims.batch_id, claims.subject, claims.kind,
                        claims.origin, claims.actor, claims.body, claims.predecessors, claims.accepted_at_unix_ms
                 FROM operations JOIN claims ON claims.id=operations.canonical_claim_id
                 WHERE operations.id=?1 AND operations.state='active'",
                [operation_id],
                claim_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn status(&self, selected: Option<&str>) -> Result<StatusResponse> {
        self.status_at(selected, None, None)
    }

    pub fn status_at(
        &self,
        selected: Option<&str>,
        selected_owner_run: Option<&str>,
        at_index: Option<u64>,
    ) -> Result<StatusResponse> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let current = current_index(&connection)?;
        let store_index = selected_index(current, at_index).map_err(anyhow::Error::new)?;
        let mut subject_names = BTreeSet::new();
        if let Some(selected) = selected {
            subject_names.insert(selected.to_owned());
        } else {
            let mut statement = connection.prepare(
                "SELECT DISTINCT subject FROM claims WHERE store_index<=?1 ORDER BY subject",
            )?;
            let rows = statement.query_map([store_index], |row| row.get::<_, String>(0))?;
            subject_names.extend(rows.collect::<Result<Vec<_>, _>>()?);
        }
        let mut subjects = Vec::new();
        let mut pending_actions = Vec::new();
        for subject in subject_names {
            let desired = desired_row_at(&connection, &subject, at_index)?;
            let actual = latest_actual_at(&connection, &subject, at_index)?;
            let claims = claim_ids_at(&connection, &subject, at_index)?;
            let conflicts = desired_conflicts_at(
                &connection,
                &subject,
                desired.as_ref().map(|row| row.claim_id.as_str()),
                at_index,
            )?;
            let kind = desired.as_ref().map(|row| row.kind.clone());
            let owner_run = desired.as_ref().and_then(|row| row.owner_run.clone());
            if selected_owner_run.is_some_and(|run| owner_run.as_deref() != Some(run)) {
                continue;
            }
            let member = desired
                .as_ref()
                .and_then(|row| row.member.as_deref())
                .and_then(|value| serde_json::from_str::<crate::model::MemberSpec>(value).ok());
            let status = actual
                .as_ref()
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str);
            let unknown_claim = has_unknown_claim_at(&connection, &subject, at_index)?;
            let reachability = if unknown_claim.is_some() {
                "indeterminate".to_owned()
            } else {
                actual
                    .as_ref()
                    .and_then(|value| value.get("reachability"))
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| {
                        if actual.is_some() {
                            "reachable"
                        } else if desired.is_some() {
                            "indeterminate"
                        } else {
                            "reachable"
                        }
                    })
                    .to_owned()
            };
            let gap = match (kind.as_deref(), member.as_ref(), status) {
                (Some("stop"), _, Some("stopped" | "absent" | "exited")) => None,
                (Some("stop"), _, _) => Some("the desired state is stopped".to_owned()),
                (_, Some(_), Some("running" | "ready" | "working" | "idle")) => None,
                (_, Some(member), Some("exited"))
                    if member.restart == crate::model::RestartType::Never =>
                {
                    None
                }
                (_, Some(_), Some(value)) => Some(format!("the member is {value}")),
                (_, Some(_), None) => Some("the desired member has no actual state".to_owned()),
                _ => None,
            };
            if let Some(reason) = &gap {
                pending_actions.push(PlannedAction {
                    subject: subject.clone(),
                    action: if matches!(kind.as_deref(), Some("stop")) {
                        "stop"
                    } else {
                        "reconcile"
                    }
                    .into(),
                    reason: reason.clone(),
                });
            }
            let reason = unknown_claim
                .map(|kind| format!("claim kind `{kind}` is not registered"))
                .or_else(|| {
                    actual
                        .as_ref()
                        .and_then(|value| value.get("reason"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            let desired_value = desired
                .as_ref()
                .map(|row| serde_json::from_str(&row.body))
                .transpose()?;
            let under = desired_value
                .as_ref()
                .map(crate::graph::agent_under)
                .unwrap_or_default();
            subjects.push(SubjectStatus {
                subject,
                kind,
                desired_token: desired.as_ref().map(|row| row.claim_id.clone()),
                desired_revision: desired.as_ref().map(|row| row.revision.clone()),
                desired: desired_value,
                actual,
                conflicts,
                claims,
                owner_run,
                gap,
                reachability,
                reason,
                under,
            });
        }
        Ok(StatusResponse {
            store_index,
            subjects,
            pending_actions,
        })
    }

    pub fn events_after(&self, after: u64, subject: Option<&str>) -> Result<Vec<EventRecord>> {
        self.events_after_filtered(after, subject, None)
    }

    pub fn events_after_filtered(
        &self,
        after: u64,
        subject: Option<&str>,
        owner_run: Option<&str>,
    ) -> Result<Vec<EventRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT store_index, kind, subject, body FROM events
             WHERE store_index > ?1 AND (?2 IS NULL OR subject=?2) ORDER BY store_index",
        )?;
        let rows = statement.query_map(params![after, subject], |row| {
            let body = row.get::<_, String>(3)?;
            Ok(EventRecord {
                store_index: row.get(0)?,
                kind: row.get(1)?,
                subject: row.get(2)?,
                body: serde_json::from_str(&body).unwrap_or(Value::Null),
            })
        })?;
        rows.filter_map(|row| match row {
            Ok(event)
                if owner_run
                    .is_some_and(|run| !subject_owned_by(&connection, &event.subject, run)) =>
            {
                None
            }
            row => Some(row),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
    }

    pub fn desired_subjects(&self) -> Result<Vec<DesiredSubject>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT subject, kind, body, member, owner_run, owner_generation, owner_step FROM desired ORDER BY subject",
        )?;
        let rows = statement.query_map([], |row| {
            let subject = row.get::<_, String>(0)?;
            let kind = row.get::<_, String>(1)?;
            let desired = row.get::<_, String>(2)?;
            let member = row.get::<_, Option<String>>(3)?;
            Ok(DesiredSubject {
                subject,
                kind,
                desired: serde_json::from_str(&desired).unwrap_or(Value::Null),
                member: member.and_then(|value| serde_json::from_str(&value).ok()),
                owner_run: row.get(4)?,
                owner_generation: row.get(5)?,
                owner_step: row.get(6)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn retire_eval_owned_desired(&self, run: &str) -> Result<Vec<String>> {
        let run = if run.starts_with("mission-run/") {
            run.to_owned()
        } else {
            format!("mission-run/{run}")
        };
        let run_id = run.strip_prefix("mission-run/").unwrap_or(&run);
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let mode: Option<String> = transaction
            .query_row(
                "SELECT mode FROM mission_runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?;
        anyhow::ensure!(
            mode.as_deref() == Some("eval"),
            "only eval runs can retire their desired graph"
        );
        transaction.execute(
            "DELETE FROM desired
             WHERE owner_run IN (
               SELECT 'mission-run/' || id FROM mission_runs WHERE root_run_id=?1
             )",
            [run_id],
        )?;
        let residue = {
            let mut statement = transaction.prepare(
                "SELECT desired.subject
                 FROM desired
                 JOIN mission_runs
                   ON desired.owner_run='mission-run/' || mission_runs.id
                 WHERE mission_runs.root_run_id=?1
                 ORDER BY desired.subject",
            )?;
            statement
                .query_map([run_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        transaction.commit()?;
        Ok(residue)
    }

    pub fn eval_runtime_records(&self, run: &str) -> Result<Vec<(String, bool)>> {
        let run = if run.starts_with("mission-run/") {
            run.to_owned()
        } else {
            format!("mission-run/{run}")
        };
        let run_id = run.strip_prefix("mission-run/").unwrap_or(&run);
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mode: Option<String> = connection
            .query_row(
                "SELECT mode FROM mission_runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?;
        anyhow::ensure!(
            mode.as_deref() == Some("eval"),
            "only eval runs have disposable runtime records"
        );

        let run_subjects = {
            let mut statement = connection.prepare(
                "SELECT id FROM mission_runs WHERE root_run_id=?1 ORDER BY created_at_unix_ms, id",
            )?;
            statement
                .query_map([run_id], |row| {
                    row.get::<_, String>(0)
                        .map(|id| format!("mission-run/{id}"))
                })?
                .collect::<Result<BTreeSet<_>, _>>()?
        };
        let mut records = BTreeMap::new();
        {
            let mut statement = connection.prepare(
                "SELECT body FROM claims WHERE kind='intent.desired' ORDER BY store_index",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                let desired: DesiredSubject = serde_json::from_str(&row?)?;
                if !desired
                    .owner_run
                    .as_ref()
                    .is_some_and(|owner| run_subjects.contains(owner))
                {
                    continue;
                }
                if let Some(member) = desired.member {
                    records.insert(member.runtime_id, member.terminal);
                }
            }
        }

        let mut gate_prefixes = run_subjects
            .iter()
            .filter_map(|subject| subject.strip_prefix("mission-run/"))
            .map(|id| format!("gate-operation/mission-run.{id}/"))
            .collect::<Vec<_>>();
        {
            let mut statement = connection.prepare(
                "SELECT run_generations.id
                 FROM run_generations
                 JOIN mission_runs ON run_generations.run_id=mission_runs.id
                 WHERE mission_runs.root_run_id=?1
                 ORDER BY run_generations.created_at_unix_ms, run_generations.id",
            )?;
            let rows = statement.query_map([run_id], |row| row.get::<_, String>(0))?;
            for row in rows {
                gate_prefixes.push(format!("gate-operation/step-run.{}.", row?));
            }
        }
        {
            let mut statement = connection.prepare(
                "SELECT subject, body FROM claims WHERE kind='gate.requested' ORDER BY store_index",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (subject, body) = row?;
                if !gate_prefixes
                    .iter()
                    .any(|prefix| subject.starts_with(prefix))
                {
                    continue;
                }
                let body: Value = serde_json::from_str(&body)?;
                let fields = body.get("fields").unwrap_or(&body);
                let has_exec_runner = fields.get("runner").and_then(Value::as_str) == Some("exec")
                    || fields.get("model").and_then(Value::as_str).is_some();
                if has_exec_runner {
                    records.insert(subject.replace('/', "."), false);
                }
            }
        }
        Ok(records.into_iter().collect())
    }

    pub fn messages(
        &self,
        recipient: Option<&str>,
        include_closed: bool,
    ) -> Result<Vec<MessageView>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT DISTINCT subject FROM claims WHERE subject LIKE 'message/%' ORDER BY subject",
        )?;
        let subjects = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut output = Vec::new();
        for subject in subjects {
            let actual = latest_actual(&connection, &subject)?.unwrap_or(Value::Null);
            let desired = current_desired_row(&connection, &subject)?
                .and_then(|row| serde_json::from_str::<Value>(&row.body).ok());
            let from = actual
                .get("from")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    desired
                        .as_ref()
                        .and_then(|value| canonical_child_string(value, "from"))
                })
                .unwrap_or_else(|| "requester".into());
            let from = normalize_message_party(&from);
            let to = actual
                .get("to")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    desired
                        .as_ref()
                        .and_then(|value| canonical_child_string(value, "to"))
                })
                .unwrap_or_default();
            let to = normalize_message_party(&to);
            let content = actual
                .get("content")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    desired
                        .as_ref()
                        .and_then(|value| canonical_child_string(value, "content"))
                })
                .unwrap_or_default();
            let status = actual
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("sent")
                .to_owned();
            if recipient.is_some_and(|recipient| recipient != to)
                || (!include_closed && status == "closed")
            {
                continue;
            }
            let created_index = connection.query_row(
                "SELECT MIN(store_index) FROM claims WHERE subject=?1",
                [&subject],
                |row| row.get(0),
            )?;
            output.push(MessageView {
                subject,
                from,
                to,
                content,
                status,
                title: actual
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        desired
                            .as_ref()
                            .and_then(|value| canonical_child_string(value, "title"))
                    }),
                in_reply_to: actual
                    .get("in_reply_to")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        desired
                            .as_ref()
                            .and_then(|value| canonical_child_string(value, "in-reply-to"))
                    }),
                tags: actual
                    .get("tags")
                    .and_then(Value::as_array)
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_else(|| {
                        desired
                            .as_ref()
                            .map(|value| canonical_child_strings(value, "tag"))
                            .unwrap_or_default()
                    }),
                created_index,
            });
        }
        output.sort_by_key(|message| message.created_index);
        Ok(output)
    }

    pub fn latest_claim(&self, subject: &str, kind: Option<&str>) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let query = "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
                     FROM claims WHERE subject=?1 AND (?2 IS NULL OR kind=?2) ORDER BY store_index DESC LIMIT 1";
        connection
            .query_row(query, params![subject, kind], claim_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn gate_request_for_owner(&self, owner: &str) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
                 FROM claims WHERE kind='gate.requested'
                 AND json_extract(body, '$.fields.owner')=?1
                 ORDER BY store_index DESC LIMIT 1",
                [owner],
                claim_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn claim_by_id(&self, id: &str) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
                 FROM claims WHERE id=?1",
                [id],
                claim_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn selected_desired_token(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_desired_row(&connection, subject).map(|row| row.map(|row| row.claim_id))
    }

    pub fn selected_desired_revision(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_desired_row(&connection, subject).map(|row| row.map(|row| row.revision))
    }

    pub fn selected_desired_kind(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_desired_row(&connection, subject).map(|row| row.map(|row| row.kind))
    }

    pub fn selected_desired_origin(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT claims.origin FROM desired
                 JOIN claims ON claims.id=desired.claim_id
                 WHERE desired.subject=?1",
                [subject],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_resource_observation(
        &self,
        observer: &str,
        desired_revision: &str,
        attempt: Option<&str>,
        resource: &str,
        cursor: Option<&str>,
        facts: &Value,
        next_check_unix_ms: u128,
        subscriptions: &[(String, SubscriptionSpec)],
    ) -> Result<ResourceObservationOutcome, St3Error> {
        let operation_hash = canonical_hash(&(
            observer,
            desired_revision,
            attempt,
            cursor,
            facts,
            next_check_unix_ms,
        ))
        .map_err(internal)?;
        let idempotency_key = format!("resource-observation:{operation_hash}");
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE operation_id=?1",
                [opaque_cache_key(&idempotency_key)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let current_observer = current_desired_row_tx(&transaction, observer).map_err(internal)?;
        if current_observer
            .as_ref()
            .map(|desired| desired.revision.as_str())
            != Some(desired_revision)
        {
            return Err(St3Error::new(
                "stale-observer-revision",
                format!("observer `{observer}` changed before its observation completed"),
            ));
        }
        let mut active_subscriptions = Vec::new();
        for (subject, expected) in subscriptions {
            let Some(desired) = current_desired_row_tx(&transaction, subject).map_err(internal)?
            else {
                continue;
            };
            if desired.kind != "subscription" {
                continue;
            }
            let value = serde_json::from_str(&desired.body).map_err(internal)?;
            if crate::graph::subscription_spec(&value).as_ref() == Some(expected)
                && !expected.stopped
            {
                active_subscriptions.push((subject.clone(), expected.clone()));
            }
        }
        let normalized_observation = normalize_resource_observation(
            &transaction,
            &ClaimInput {
                subject: resource.into(),
                kind: "resource.observed".into(),
                actor: None,
                fields: BTreeMap::from([("facts".into(), facts.clone())]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            },
        )?
        .expect("a resource observation is normalized");
        let resource_kind = normalized_observation
            .get("kind")
            .cloned()
            .expect("a normalized resource observation has a kind");
        let previous = latest_actual(&transaction, resource)
            .map_err(internal)?
            .and_then(|actual| actual.get("facts").cloned());
        let baseline = previous.is_none();
        let previous_object = previous.as_ref().and_then(Value::as_object);
        let current_object = facts.as_object().ok_or_else(|| {
            St3Error::new(
                "invalid-resource-observation",
                "resource observation facts must be an object",
            )
        })?;
        let mut changed_fields = BTreeSet::new();
        for field in current_object
            .keys()
            .chain(previous_object.into_iter().flat_map(serde_json::Map::keys))
        {
            if previous_object.and_then(|object| object.get(field)) != current_object.get(field) {
                changed_fields.insert(field.clone());
            }
        }
        let changed_fields = changed_fields.into_iter().collect::<Vec<_>>();
        let now = now_ms();
        let sequence = next_replica_sequence(&transaction, &self.origin).map_err(internal)?;
        let previous_hash = previous_batch_hash(&transaction, &self.origin).map_err(internal)?;
        let batch_hash = batch_header_hash(&self.origin, sequence, previous_hash.as_deref(), now)
            .map_err(internal)?;
        let batch_id = format!("batch/{}/{sequence}/{batch_hash}", self.origin);
        transaction
            .execute(
                "INSERT INTO batches(id, origin, replica_sequence, previous_hash, hash, accepted_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    batch_id,
                    self.origin,
                    sequence,
                    previous_hash,
                    batch_hash,
                    now.to_string()
                ],
            )
            .map_err(internal)?;
        let observer_predecessors = latest_claim_id_tx(&transaction, observer)
            .map_err(internal)?
            .into_iter()
            .collect::<Vec<_>>();
        let mut observer_fields = json!({
            "status": "healthy",
            "revision": desired_revision,
            "cursor": cursor,
            "next_check_unix_ms": next_check_unix_ms.to_string(),
            "changed": baseline || !changed_fields.is_empty(),
        });
        if let Some(attempt) = attempt {
            observer_fields
                .as_object_mut()
                .expect("observer fields are an object")
                .insert("attempt".into(), Value::String(attempt.into()));
        }
        append_claim_tx(
            &transaction,
            &self.origin,
            observer,
            "observer.observed",
            None,
            &json!({"fields": observer_fields}),
            &observer_predecessors,
            Some(&batch_id),
        )
        .map_err(internal)?;
        let observation_claim = if baseline || !changed_fields.is_empty() {
            let predecessors = latest_claim_id_tx(&transaction, resource)
                .map_err(internal)?
                .into_iter()
                .collect::<Vec<_>>();
            Some(
                append_claim_tx(
                    &transaction,
                    &self.origin,
                    resource,
                    "resource.observed",
                    None,
                    &json!({"fields": {
                        "kind": resource_kind,
                        "facts": facts,
                        "observer": observer,
                        "baseline": baseline,
                        "changed_fields": changed_fields,
                    }}),
                    &predecessors,
                    Some(&batch_id),
                )
                .map_err(internal)?,
            )
        } else {
            None
        };
        let mut collection_discoveries = BTreeMap::<String, Vec<(String, String)>>::new();
        if !baseline {
            for field in ["pull_requests", "issues"] {
                for (subject, kind, item_facts) in
                    discovered_collection_items(resource, field, previous.as_ref(), facts)
                {
                    let predecessors = latest_claim_id_tx(&transaction, &subject)
                        .map_err(internal)?
                        .into_iter()
                        .collect::<Vec<_>>();
                    let evidence = observation_claim
                        .as_ref()
                        .map(|claim| vec![claim.id.clone()])
                        .unwrap_or_default();
                    let claim = append_claim_tx(
                        &transaction,
                        &self.origin,
                        &subject,
                        "resource.observed",
                        None,
                        &json!({"fields": {
                            "kind": kind,
                            "facts": item_facts,
                            "observer": observer,
                            "baseline": false,
                            "changed_fields": [field],
                        }, "evidence": evidence}),
                        &predecessors,
                        Some(&batch_id),
                    )
                    .map_err(internal)?;
                    collection_discoveries
                        .entry(field.into())
                        .or_default()
                        .push((subject, claim.id));
                }
            }
        }
        let mut available_subscriptions = BTreeSet::new();
        for (subscription_subject, subscription) in &active_subscriptions {
            let target_exists = if subscription.delivery == "message" {
                transaction
                    .query_row(
                        "SELECT 1 FROM desired WHERE subject=?1 AND kind='agent'",
                        [&subscription.to],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(internal)?
                    .is_some()
            } else {
                true
            };
            let status = if target_exists { "active" } else { "pending" };
            let current_status = latest_actual(&transaction, subscription_subject)
                .map_err(internal)?
                .and_then(|actual| {
                    actual
                        .get("state")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            if current_status.as_deref() != Some(status) {
                let predecessors = latest_claim_id_tx(&transaction, subscription_subject)
                    .map_err(internal)?
                    .into_iter()
                    .collect::<Vec<_>>();
                append_claim_tx(
                    &transaction,
                    &self.origin,
                    subscription_subject,
                    "subscription.state",
                    None,
                    &json!({"fields": {
                        "state": status,
                        "reason": (!target_exists).then_some("the delivery target is absent"),
                    }}),
                    &predecessors,
                    Some(&batch_id),
                )
                .map_err(internal)?;
            }
            if target_exists {
                available_subscriptions.insert(subscription_subject.clone());
            }
        }
        let mut message_subjects = Vec::new();
        if !baseline && !changed_fields.is_empty() {
            let evidence = observation_claim
                .as_ref()
                .map(|claim| vec![claim.id.clone()])
                .unwrap_or_default();
            for (subscription_subject, subscription) in &active_subscriptions {
                if !available_subscriptions.contains(subscription_subject) {
                    continue;
                }
                let selected = changed_fields
                    .iter()
                    .filter(|field| subscription.fields.contains(field))
                    .cloned()
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    continue;
                }
                let stable = canonical_hash(&(
                    observation_claim.as_ref().map(|claim| claim.id.as_str()),
                    subscription_subject,
                ))
                .map_err(internal)?;
                if subscription.delivery == "mission" {
                    let Some(mission) = subscription.mission.as_deref() else {
                        continue;
                    };
                    let Some(revision) = subscription.revision.as_deref() else {
                        continue;
                    };
                    let Some(resource_input) = subscription.resource_input.as_deref() else {
                        continue;
                    };
                    let Some(workspace) = subscription.workspace.as_deref() else {
                        continue;
                    };
                    let uses_collection = selected
                        .iter()
                        .any(|field| matches!(field.as_str(), "pull_requests" | "issues"));
                    let discoveries = if uses_collection {
                        selected
                            .iter()
                            .flat_map(|field| {
                                collection_discoveries
                                    .get(field)
                                    .into_iter()
                                    .flatten()
                                    .cloned()
                            })
                            .collect::<Vec<_>>()
                    } else {
                        observation_claim
                            .as_ref()
                            .map(|claim| vec![(resource.to_owned(), claim.id.clone())])
                            .unwrap_or_default()
                    };
                    for (delivery_resource, discovery) in discoveries {
                        append_claim_tx(
                            &transaction,
                            &self.origin,
                            subscription_subject,
                            "subscription.mission-requested",
                            None,
                            &json!({"fields": {
                                "mission": format!("mission/{mission}"),
                                "mission_revision": revision,
                                "resource": delivery_resource,
                                "resource_input": resource_input,
                                "workspace": workspace,
                                "discovery": discovery,
                            }, "evidence": evidence}),
                            &[],
                            Some(&batch_id),
                        )
                        .map_err(claim_append_error)?;
                    }
                    continue;
                }
                let message_subject = format!("message/resource-{}", &stable[..20]);
                let content = serde_json::to_string(&json!({
                    "resource": resource,
                    "observer": observer,
                    "changed_fields": selected,
                    "facts": facts,
                }))
                .map_err(internal)?;
                append_claim_tx(
                    &transaction,
                    &self.origin,
                    &message_subject,
                    "message.sent",
                    None,
                    &json!({"fields": {
                        "from": format!("daemon/{}", self.origin),
                        "to": subscription.to,
                        "title": format!("Resource changed: {resource}"),
                        "content": content,
                        "status": "sent",
                        "tags": ["resource-change", subscription_subject],
                    }, "evidence": evidence}),
                    &[],
                    Some(&batch_id),
                )
                .map_err(claim_append_error)?;
                message_subjects.push(message_subject);
            }
        }
        let outcome = ResourceObservationOutcome {
            baseline,
            changed_fields,
            observation_claim: observation_claim.map(|claim| claim.id),
            message_subjects,
        };
        transaction
            .execute(
                "INSERT INTO idempotency(operation_id, response) VALUES (?1, ?2)",
                params![
                    opaque_cache_key(&idempotency_key),
                    serde_json::to_string(&outcome).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(outcome)
    }

    pub fn claims_for(&self, subject: &str, kind: Option<&str>) -> Result<Vec<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let query = "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
                     FROM claims WHERE subject=?1 AND (?2 IS NULL OR kind=?2) ORDER BY store_index";
        let mut statement = connection.prepare(query)?;
        let rows = statement.query_map(params![subject, kind], claim_from_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn claims_page(
        &self,
        subject: Option<&str>,
        owner_run: Option<&str>,
        after_index: u64,
        before_index: Option<u64>,
        descending: bool,
        limit: usize,
    ) -> Result<ClaimsPage> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let order = if descending { "DESC" } else { "ASC" };
        let query = format!(
            "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
             FROM claims WHERE store_index>?1 AND (?2 IS NULL OR store_index<?2) AND (?3 IS NULL OR subject=?3) ORDER BY store_index {order}"
        );
        let mut statement = connection.prepare(&query)?;
        let rows =
            statement.query_map(params![after_index, before_index, subject], claim_from_row)?;
        let mut claims = Vec::new();
        for row in rows {
            let claim = row?;
            if owner_run.is_some_and(|run| !subject_owned_by(&connection, &claim.subject, run)) {
                continue;
            }
            claims.push(claim);
            if claims.len() > limit {
                break;
            }
        }
        let next_cursor = if claims.len() > limit {
            claims.pop();
            claims.last().map(|claim| claim.store_index)
        } else {
            None
        };
        Ok(ClaimsPage {
            claims,
            next_cursor,
        })
    }

    pub fn latest_actual_value(&self, subject: &str) -> Result<Option<Value>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        latest_actual(&connection, subject)
    }

    pub fn latest_document_hash(&self, name: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT hash FROM documents WHERE name=?1 ORDER BY created_index DESC LIMIT 1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn document_bindings(
        &self,
        references: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, String>> {
        self.document_bindings_at(references, None)
    }

    pub fn document_bindings_at(
        &self,
        references: &BTreeSet<String>,
        at_index: Option<u64>,
    ) -> Result<BTreeMap<String, String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let current = current_index(&connection)?;
        let selected = selected_index(current, at_index).map_err(anyhow::Error::new)?;
        let mut bindings = BTreeMap::new();
        for reference in references
            .iter()
            .filter(|reference| !reference.contains('@'))
        {
            if let Some(hash) = connection
                .query_row(
                    "SELECT hash FROM documents WHERE name=?1 AND created_index<=?2 ORDER BY created_index DESC LIMIT 1",
                    params![reference, selected],
                    |row| row.get(0),
                )
                .optional()?
            {
                bindings.insert(reference.clone(), hash);
            }
        }
        Ok(bindings)
    }

    pub fn latest_document_token(&self, name: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT binding_claim_id FROM documents WHERE name=?1 ORDER BY created_index DESC LIMIT 1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn issue_capability(
        &self,
        kind: &str,
        subject: &str,
        incarnation_id: Option<&str>,
        lifetime_ms: u64,
    ) -> Result<(String, u128)> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let secret = hex::encode(bytes);
        let secret_hash = hex::encode(Sha256::digest(secret.as_bytes()));
        let expires = now_ms().saturating_add(lifetime_ms as u128);
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection.execute(
            "INSERT INTO capabilities(secret_hash, kind, subject, incarnation_id, expires_at_unix_ms, used)
             VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            params![secret_hash, kind, subject, incarnation_id, expires.to_string()],
        )?;
        Ok((secret, expires))
    }

    pub fn capability(&self, secret: &str, expected_kind: &str) -> Result<Capability, St3Error> {
        let hash = hex::encode(Sha256::digest(secret.as_bytes()));
        let connection = self.connection.lock().expect("store mutex poisoned");
        let capability = connection
            .query_row(
                "SELECT kind, subject, incarnation_id, expires_at_unix_ms, used FROM capabilities WHERE secret_hash=?1",
                [&hash],
                |row| {
                    let expires = row.get::<_, String>(3)?;
                    Ok(Capability {
                        kind: row.get(0)?,
                        subject: row.get(1)?,
                        incarnation_id: row.get(2)?,
                        expires_at_unix_ms: expires.parse().unwrap_or_default(),
                        used: row.get::<_, i64>(4)? != 0,
                    })
                },
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| St3Error::new("invalid-capability", "the operation capability is not valid"))?;
        if capability.kind != expected_kind {
            return Err(St3Error::new(
                "invalid-capability",
                "the operation capability has the wrong kind",
            ));
        }
        if capability.expires_at_unix_ms < now_ms() {
            return Err(St3Error::new(
                "expired-capability",
                "the operation capability expired",
            ));
        }
        Ok(capability)
    }

    pub fn consume_capability(
        &self,
        secret: &str,
        expected_kind: &str,
    ) -> Result<Capability, St3Error> {
        let capability = self.capability(secret, expected_kind)?;
        if capability.used {
            return Ok(capability);
        }
        let hash = hex::encode(Sha256::digest(secret.as_bytes()));
        let connection = self.connection.lock().expect("store mutex poisoned");
        let changed = connection
            .execute(
                "UPDATE capabilities SET used=1 WHERE secret_hash=?1 AND used=0",
                [&hash],
            )
            .map_err(internal)?;
        if changed != 1 {
            return Err(St3Error::new(
                "used-capability",
                "the operation capability was already consumed",
            ));
        }
        Ok(Capability {
            used: false,
            ..capability
        })
    }

    pub fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        let hash = hex::encode(Sha256::digest(bytes));
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection.execute(
            "INSERT OR IGNORE INTO blobs(hash, bytes, size) VALUES (?1, ?2, ?3)",
            params![hash, bytes, bytes.len() as u64],
        )?;
        Ok(hash)
    }

    pub fn get_blob(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row("SELECT bytes FROM blobs WHERE hash=?1", [hash], |row| {
                row.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    pub fn export_replication(&self, after_sequence: u64) -> Result<ReplicationBatch> {
        let mut heads = self.replica_heads()?;
        heads.insert(self.origin.clone(), after_sequence);
        self.export_replication_for_heads(&heads)
    }

    pub fn replica_heads(&self) -> Result<BTreeMap<String, u64>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        replica_heads(&connection)
    }

    pub fn export_replication_for_heads(
        &self,
        heads: &BTreeMap<String, u64>,
    ) -> Result<ReplicationBatch> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut batches_statement = connection.prepare(
            "SELECT id, origin, replica_sequence, previous_hash, hash, accepted_at_unix_ms FROM batches
             ORDER BY origin, replica_sequence",
        )?;
        let headers = batches_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .filter_map(|row| match row {
                Ok(row) if row.2 > heads.get(&row.1).copied().unwrap_or(0) => Some(Ok(row)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .take(512)
            .collect::<Result<Vec<_>, _>>()?;
        let mut batches = Vec::new();
        let mut blobs = BTreeMap::new();
        for (id, origin, replica_sequence, previous_hash, hash, accepted_at) in headers {
            let mut claim_statement = connection.prepare(
                "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
                 FROM claims WHERE batch_id=?1 ORDER BY store_index",
            )?;
            let claims = claim_statement
                .query_map([&id], claim_from_row)?
                .collect::<Result<Vec<_>, _>>()?;
            collect_referenced_blobs(&connection, &claims, &mut blobs)?;
            batches.push(ReplicaBatch {
                id,
                origin,
                replica_sequence,
                previous_hash,
                hash,
                accepted_at_unix_ms: accepted_at.parse().unwrap_or_default(),
                claims,
            });
        }
        Ok(ReplicationBatch {
            peer: self.origin.clone(),
            replica_heads: replica_heads(&connection)?,
            batches,
            blobs,
        })
    }

    pub fn peer_cursor(&self, peer: &str) -> Result<u64> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT MAX(replica_sequence) FROM batches WHERE origin=?1",
                [peer],
                |row| row.get::<_, Option<u64>>(0),
            )
            .map(|value| value.unwrap_or(0))
            .map_err(Into::into)
    }

    pub fn import_replication(
        &self,
        relay: &str,
        input: &ReplicationBatch,
    ) -> Result<ReplicationResponse, St3Error> {
        if relay != input.peer {
            return Err(St3Error::new(
                "peer-label-mismatch",
                "the configured relay label does not match the batch label",
            ));
        }
        for (hash, bytes) in &input.blobs {
            if hex::encode(Sha256::digest(bytes)) != *hash {
                return Err(St3Error::new(
                    "blob-hash-mismatch",
                    format!("replicated blob `{hash}` failed verification"),
                ));
            }
        }
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction().map_err(internal)?;
        for (hash, bytes) in &input.blobs {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO blobs(hash, bytes, size) VALUES (?1, ?2, ?3)",
                    params![hash, bytes, bytes.len() as u64],
                )
                .map_err(internal)?;
        }
        let mut missing_ranges = Vec::new();
        let mut blocked_origins = BTreeSet::new();
        for batch in &input.batches {
            if blocked_origins.contains(&batch.origin) {
                continue;
            }
            let accepted_through = transaction
                .query_row(
                    "SELECT MAX(replica_sequence) FROM batches WHERE origin=?1",
                    [&batch.origin],
                    |row| row.get::<_, Option<u64>>(0),
                )
                .map_err(internal)?
                .unwrap_or(0);
            if batch.replica_sequence <= accepted_through {
                let stored_hash = transaction
                    .query_row(
                        "SELECT hash FROM batches WHERE origin=?1 AND replica_sequence=?2",
                        params![batch.origin, batch.replica_sequence],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(internal)?;
                if stored_hash.as_deref() != Some(batch.hash.as_str()) {
                    return Err(St3Error::new(
                        "replica-sequence-conflict",
                        format!(
                            "replica `{}` sequence {} has another hash",
                            batch.origin, batch.replica_sequence
                        ),
                    ));
                }
                continue;
            }
            if batch.replica_sequence != accepted_through.saturating_add(1) {
                missing_ranges.push(ReplicaRange {
                    origin: batch.origin.clone(),
                    from: accepted_through.saturating_add(1),
                    through: batch.replica_sequence.saturating_sub(1),
                });
                blocked_origins.insert(batch.origin.clone());
                continue;
            }
            let expected_previous = transaction
                .query_row(
                    "SELECT hash FROM batches WHERE origin=?1 ORDER BY replica_sequence DESC LIMIT 1",
                    [&batch.origin],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(internal)?;
            if expected_previous != batch.previous_hash {
                return Err(St3Error::new(
                    "broken-replica-chain",
                    format!(
                        "replica sequence {} does not cite the current chain head",
                        batch.replica_sequence
                    ),
                ));
            }
            verify_replica_batch(batch)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO batches(id, origin, replica_sequence, previous_hash, hash, accepted_at_unix_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![batch.id, batch.origin, batch.replica_sequence, batch.previous_hash, batch.hash, batch.accepted_at_unix_ms.to_string()],
                )
                .map_err(internal)?;
            for claim in &batch.claims {
                ensure_claim_blobs(&transaction, claim)?;
                let inserted = transaction
                    .execute(
                        "INSERT OR IGNORE INTO claims(id, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            claim.id,
                            claim.batch_id,
                            claim.subject,
                            claim.kind,
                            claim.origin,
                            claim.actor,
                            serde_json::to_string(&claim.body).map_err(internal)?,
                            serde_json::to_string(&claim.predecessors).map_err(internal)?,
                            claim.accepted_at_unix_ms.to_string(),
                        ],
                    )
                    .map_err(internal)?;
                if inserted != 0 {
                    let index = transaction.last_insert_rowid() as u64;
                    let effective = register_operation_tx(&transaction, claim).map_err(internal)?;
                    if effective {
                        insert_event(
                            &transaction,
                            index,
                            &claim.kind,
                            &claim.subject,
                            &claim.body,
                        )
                        .map_err(internal)?;
                    }
                    if effective && claim.kind == "intent.desired" {
                        if let Ok(desired) =
                            serde_json::from_value::<DesiredSubject>(claim.body.clone())
                        {
                            select_replicated_desired(&transaction, claim, &desired)?;
                        }
                    } else if effective && claim.kind == "doc.bound" {
                        select_replicated_document(&transaction, claim, index)?;
                    } else if effective && claim.kind == "mission.published" {
                        select_replicated_mission(&transaction, claim, index)?;
                    }
                }
            }
            transaction
                .execute(
                    "INSERT INTO peer_replica_cursors(peer, origin, accepted_through) VALUES (?1, ?2, ?3)
                     ON CONFLICT(peer, origin) DO UPDATE SET accepted_through=MAX(accepted_through, excluded.accepted_through)",
                    params![relay, batch.origin, batch.replica_sequence],
                )
                .map_err(internal)?;
        }
        rebuild_operations_tx(&transaction).map_err(internal)?;
        project_replicated_mission_runs(&transaction)?;
        rebuild_planning_tx(&transaction).map_err(internal)?;
        let accepted_heads = replica_heads(&transaction).map_err(internal)?;
        let accepted_through = accepted_heads.get(&input.peer).copied().unwrap_or(0);
        let missing_sequences = missing_ranges
            .iter()
            .filter(|range| range.origin == input.peer)
            .flat_map(|range| range.from..=range.through)
            .collect::<Vec<_>>();
        transaction
            .execute(
                "INSERT INTO peer_cursors(peer, accepted_through) VALUES (?1, ?2)
                 ON CONFLICT(peer) DO UPDATE SET accepted_through=excluded.accepted_through",
                params![relay, accepted_through],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(ReplicationResponse {
            accepted_through,
            missing_sequences,
            accepted_heads,
            missing_ranges,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_mission_run_declaration(
    connection: &Connection,
    declaration: &MissionRunDeclaration,
    store_index: u64,
    tokens: &mut BTreeMap<String, Vec<String>>,
    actions: &mut Vec<PlannedAction>,
    blockers: &mut Vec<String>,
    warnings: &mut Vec<String>,
) -> Result<(), St3Error> {
    let run_id = declaration
        .subject
        .strip_prefix("mission-run/")
        .unwrap_or(&declaration.subject);
    tokens.insert(
        declaration.subject.clone(),
        claim_ids_at(connection, &declaration.subject, Some(store_index))
            .map_err(internal)?
            .into_iter()
            .last()
            .into_iter()
            .collect(),
    );
    let current = connection
        .query_row(
            "SELECT mission_id, initial_revision, workspace, requester, status, current_generation_id,
                    inputs, mode
             FROM mission_runs WHERE id=?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()
        .map_err(internal)?;
    if let Some(creation) = &declaration.creation {
        if let Some((mission, revision, workspace, requester, _, _, inputs, mode)) = &current {
            let stored_inputs = serde_json::from_str::<BTreeMap<String, MissionRunInput>>(inputs)
                .map_err(internal)?;
            let stored_values = stored_inputs
                .into_iter()
                .map(|(name, input)| (name, input.value))
                .collect::<BTreeMap<_, _>>();
            if mission != &creation.mission
                || revision != &creation.revision
                || workspace != &creation.workspace
                || requester != &creation.requester
                || stored_values != creation.inputs
                || mode != &creation.mode
            {
                blockers.push(format!(
                    "mission run `{}` already exists with different creation fields",
                    declaration.subject
                ));
            }
        } else {
            let mission = connection
                .query_row(
                    "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
                    params![creation.mission, creation.revision],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(internal)?
                .map(|body| serde_json::from_str::<MissionSpec>(&body))
                .transpose()
                .map_err(internal)?;
            match mission {
                None => blockers.push(format!(
                    "mission `mission/{}@{}` does not exist",
                    creation.mission, creation.revision
                )),
                Some(mission) if mission.state != MissionState::Ready => blockers.push(format!(
                    "mission `mission/{}` revision `{}` is not ready",
                    creation.mission, creation.revision
                )),
                Some(mission) => {
                    if let Err(error) =
                        resolve_mission_run_inputs(connection, &mission, &creation.inputs)
                    {
                        blockers.push(error.message);
                    }
                    if let Err(error) = enforce_mission_run_capacity(connection, &mission) {
                        blockers.push(error.message);
                    }
                    actions.push(PlannedAction {
                        subject: declaration.subject.clone(),
                        action: "start-mission-run".into(),
                        reason: "the named mission run does not exist".into(),
                    });
                }
            }
        }
    }
    if current.is_none() && declaration.creation.is_none() {
        blockers.push(format!(
            "mission run `{}` does not exist",
            declaration.subject
        ));
        return Ok(());
    }
    let status = current.as_ref().map(|value| value.4.as_str());
    let current_generation = current.as_ref().map(|value| value.5.as_str());
    for revision in declaration.revisions.values() {
        if current_generation.is_some_and(|current| {
            revision
                .from_generation
                .strip_prefix("run-generation/")
                .unwrap_or(&revision.from_generation)
                != current
        }) {
            blockers.push(format!(
                "revision `{}` names a stale run generation",
                revision.id
            ));
        }
        let exists = connection
            .query_row(
                "SELECT 1 FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
                params![revision.mission, revision.revision],
                |_| Ok(()),
            )
            .optional()
            .map_err(internal)?
            .is_some();
        if !exists {
            blockers.push(format!(
                "revision `{}` names missing mission `mission/{}@{}`",
                revision.id, revision.mission, revision.revision
            ));
        }
        match publication_operation_is_new(
            connection,
            &declaration.subject,
            "revision",
            &revision.id,
            revision,
        ) {
            Ok(true) => actions.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: format!("revise:{}", revision.id),
                reason: revision.reason.clone(),
            }),
            Ok(false) => {}
            Err(error) => blockers.push(error.message),
        }
    }
    for reset in declaration.resets.values() {
        if current_generation.is_some_and(|current| {
            reset
                .from_generation
                .strip_prefix("run-generation/")
                .unwrap_or(&reset.from_generation)
                != current
        }) {
            blockers.push(format!("reset `{}` names a stale run generation", reset.id));
        }
        match publication_operation_is_new(
            connection,
            &declaration.subject,
            "reset",
            &reset.id,
            reset,
        ) {
            Ok(true) => actions.push(PlannedAction {
                subject: runtime_subject_for_run(run_id, &reset.runtime),
                action: format!("reset:{}", reset.id),
                reason: reset.reason.clone(),
            }),
            Ok(false) => {}
            Err(error) => blockers.push(error.message),
        }
    }
    for cancellation in declaration.cancellations.values() {
        let is_new = match publication_operation_is_new(
            connection,
            &declaration.subject,
            "cancellation",
            &cancellation.id,
            cancellation,
        ) {
            Ok(value) => value,
            Err(error) => {
                blockers.push(error.message);
                continue;
            }
        };
        if !is_new {
            continue;
        } else if matches!(status, Some("completed" | "failed" | "cancelled")) {
            warnings.push(format!(
                "mission run `{}` is already {}",
                declaration.subject,
                status.unwrap_or_default()
            ));
        } else {
            actions.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: format!("cancel:{}", cancellation.id),
                reason: cancellation.reason.clone(),
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn prepare_planning_session_declaration(
    connection: &Connection,
    declaration: &PlanningSessionDeclaration,
    store_index: u64,
    tokens: &mut BTreeMap<String, Vec<String>>,
    actions: &mut Vec<PlannedAction>,
    blockers: &mut Vec<String>,
) -> Result<(), St3Error> {
    let id = declaration
        .subject
        .strip_prefix("planning-session/")
        .unwrap_or(&declaration.subject);
    tokens.insert(
        declaration.subject.clone(),
        claim_ids_at(connection, &declaration.subject, Some(store_index))
            .map_err(internal)?
            .into_iter()
            .last()
            .into_iter()
            .collect(),
    );
    let current = connection
        .query_row(
            "SELECT mission_id, request_ref, workspace, requester, planner, status,
                    target_run_id, source_generation_id
             FROM planning_sessions WHERE id=?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(internal)?;
    if let Some(creation) = &declaration.creation {
        if let Some((mission, request, workspace, requester, planner, _, target, generation)) =
            &current
        {
            if mission != &creation.mission
                || request != &creation.request
                || workspace != &creation.workspace
                || requester != &creation.requester
                || planner != &crate::graph::planning_planner_subject(&declaration.subject)
                || target.as_deref()
                    != creation
                        .target_run
                        .as_deref()
                        .map(|value| value.strip_prefix("mission-run/").unwrap_or(value))
                || generation.as_deref()
                    != creation
                        .target_generation
                        .as_deref()
                        .map(|value| value.strip_prefix("run-generation/").unwrap_or(value))
            {
                blockers.push(format!(
                    "planning session `{}` already exists with different creation fields",
                    declaration.subject
                ));
            }
        } else {
            if let (Some(run), Some(generation)) =
                (&creation.target_run, &creation.target_generation)
            {
                let run_id = run.strip_prefix("mission-run/").unwrap_or(run);
                let expected_generation = generation
                    .strip_prefix("run-generation/")
                    .unwrap_or(generation);
                let actual = connection
                    .query_row(
                        "SELECT current_generation_id FROM mission_runs WHERE id=?1",
                        [run_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(internal)?;
                if actual.as_deref() != Some(expected_generation) {
                    blockers.push(format!(
                        "planning session `{}` names a stale target generation",
                        declaration.subject
                    ));
                }
            }
            actions.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: "start-planning".into(),
                reason: "the named planning session does not exist".into(),
            });
        }
    }
    if current.is_none() && declaration.creation.is_none() {
        blockers.push(format!(
            "planning session `{}` does not exist",
            declaration.subject
        ));
    }
    for feedback in declaration.feedback.values() {
        match publication_operation_is_new(
            connection,
            &declaration.subject,
            "feedback",
            &feedback.id,
            feedback,
        ) {
            Ok(true) => actions.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: format!("feedback:{}", feedback.id),
                reason: "the publication supplies planning feedback".into(),
            }),
            Ok(false) => {}
            Err(error) => blockers.push(error.message),
        }
    }
    for cancellation in declaration.cancellations.values() {
        match publication_operation_is_new(
            connection,
            &declaration.subject,
            "cancellation",
            &cancellation.id,
            cancellation,
        ) {
            Ok(true) => actions.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: format!("cancel:{}", cancellation.id),
                reason: cancellation.reason.clone(),
            }),
            Ok(false) => {}
            Err(error) => blockers.push(error.message),
        }
    }
    Ok(())
}

fn runtime_subject_for_run(run_id: &str, runtime: &str) -> String {
    let Some((kind, local)) = runtime.split_once('/') else {
        return runtime.to_owned();
    };
    if !matches!(kind, "agent" | "exec" | "pty") || local.starts_with(&format!("{run_id}/")) {
        runtime.to_owned()
    } else {
        format!("{kind}/{run_id}/{local}")
    }
}

fn create_declared_mission_run_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    declaration: &MissionRunDeclaration,
    batch_id: &str,
) -> Result<Vec<String>, St3Error> {
    let Some(creation) = &declaration.creation else {
        return Ok(Vec::new());
    };
    let run_id = declaration
        .subject
        .strip_prefix("mission-run/")
        .unwrap_or(&declaration.subject);
    let exists = transaction
        .query_row("SELECT 1 FROM mission_runs WHERE id=?1", [run_id], |_| {
            Ok(())
        })
        .optional()
        .map_err(internal)?
        .is_some();
    if exists {
        return Ok(Vec::new());
    }
    let mission: MissionSpec = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![creation.mission, creation.revision],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(internal)?
        .map(|body| serde_json::from_str(&body))
        .transpose()
        .map_err(internal)?
        .ok_or_else(|| {
            St3Error::new(
                "missing-mission",
                format!(
                    "mission `mission/{}@{}` does not exist",
                    creation.mission, creation.revision
                ),
            )
        })?;
    if mission.state != MissionState::Ready {
        return Err(St3Error::new(
            "mission-not-ready",
            format!("mission `mission/{}` is not ready", creation.mission),
        ));
    }
    validate_mission_run_timeout(&mission, &creation.mode)?;
    let inputs = resolve_mission_run_inputs(transaction, &mission, &creation.inputs)?;
    enforce_mission_run_capacity(transaction, &mission)?;
    let generation_id = Uuid::now_v7().simple().to_string();
    let generation_subject = format!("run-generation/{generation_id}");
    let root_mission_run = declaration.subject.clone();
    let mut variables = BTreeMap::from([
        ("ST_MISSION".into(), mission.id.clone()),
        ("ST_MISSION_REVISION".into(), mission.revision.clone()),
        ("ST_MISSION_RUN".into(), run_id.to_owned()),
        ("ST_RUN_GENERATION".into(), generation_id.clone()),
        ("ST_REQUESTER".into(), creation.requester.clone()),
        ("ST_ROOT_MISSION_RUN".into(), root_mission_run.clone()),
        ("ST_WORKSPACE".into(), creation.workspace.clone()),
        ("ST_PARENT_STEP_RUN".into(), String::new()),
    ]);
    for (name, input) in &inputs {
        variables.insert(format!("input.{name}"), input.value.clone());
    }
    let now = now_ms();
    transaction
        .execute(
            "INSERT INTO mission_runs(id, mission_id, initial_revision, current_generation_id, root_revision, root_run_id, parent_step_run, workspace, requester, inputs, mode, status, phase, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?3, ?1, NULL, ?5, ?6, ?7, ?8, 'running', 'normal', ?9, ?9)",
            params![
                run_id,
                mission.id,
                mission.revision,
                generation_id,
                creation.workspace,
                creation.requester,
                serde_json::to_string(&inputs).map_err(internal)?,
                creation.mode,
                now.to_string(),
            ],
        )
        .map_err(internal)?;
    let deadline_at_unix_ms =
        insert_mission_deadline_tx(transaction, run_id, mission.timeout_ms, now)?;
    transaction
        .execute(
            "INSERT INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, NULL, 'running', ?4, 'initial mission run', ?5, ?5)",
            params![
                generation_id,
                run_id,
                mission.revision,
                creation.requester,
                now.to_string()
            ],
        )
        .map_err(internal)?;
    let mut flat = Vec::new();
    flatten_steps(&mission, None, &[], &mut flat);
    for (step, selector, constraints) in flat {
        let (assignee, available_to, agentless) = interpolate_selector(&selector, &variables)?;
        let step_subject = format!("step-run/{generation_id}/{}", step.path);
        let mut step_variables = variables.clone();
        step_variables.insert("ST_STEP".into(), step.path.clone());
        step_variables.insert("ST_STEP_RUN".into(), step_subject.clone());
        step_variables.insert("ST_ATTEMPT".into(), "1".into());
        step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
        step_variables.insert(
            "ST_PARENT_STEP_RUN".into(),
            crate::mission::parent_step_path(&mission, &step.path)
                .map(|path| format!("step-run/{generation_id}/{path}"))
                .unwrap_or_default(),
        );
        let title = step
            .title
            .as_deref()
            .map(|value| crate::mission::interpolate(value, &step_variables))
            .transpose()?;
        let goals = interpolate_goals(&step.goals, &step_variables)?;
        let constraints = interpolate_goals(&constraints, &step_variables)?;
        transaction
            .execute(
                "INSERT INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, created_at_unix_ms, updated_at_unix_ms, constraints)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 1, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?12)",
                params![
                    step_subject,
                    run_id,
                    generation_id,
                    step.path,
                    step.definition_hash,
                    assignee,
                    serde_json::to_string(&available_to).map_err(internal)?,
                    agentless,
                    title,
                    goals,
                    now.to_string(),
                    constraints
                ],
            )
            .map_err(internal)?;
    }
    let body = json!({
        "fields": {
            "status": "running",
            "mission": mission.subject,
            "revision": mission.revision,
            "initial_revision": mission.revision,
            "current_generation": generation_subject,
            "root_revision": mission.revision,
            "root_mission_run": root_mission_run,
            "parent_step_run": Value::Null,
            "default_selector": Value::Null,
            "workspace": creation.workspace,
            "requester": creation.requester,
            "inputs": inputs,
            "mode": creation.mode,
            "timeout_ms": mission.timeout_ms,
            "deadline_at_unix_ms": deadline_at_unix_ms,
        }
    });
    let run_claim = append_claim_tx(
        transaction,
        origin,
        &declaration.subject,
        "mission-run.created",
        Some(&creation.requester),
        &body,
        &[],
        Some(batch_id),
    )
    .map_err(internal)?;
    let generation_claim = append_claim_tx(
        transaction,
        origin,
        &generation_subject,
        "run-generation.created",
        Some(&creation.requester),
        &json!({"fields": {
            "run": declaration.subject,
            "revision": mission.revision,
            "status": "running",
            "reason": "initial mission run"
        }}),
        &[],
        Some(batch_id),
    )
    .map_err(internal)?;
    Ok(vec![run_claim.id, generation_claim.id])
}

fn adopt_declared_mission_revision_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    declaration: &MissionRunDeclaration,
    operation: &MissionRevisionOperation,
    batch_id: &str,
    actor: Option<&str>,
) -> Result<(Vec<String>, bool), St3Error> {
    let actor = actor.ok_or_else(|| {
        St3Error::new(
            "missing-publication-actor",
            "a mission-run revision needs `--as` or ST_AGENT",
        )
    })?;
    let actor = normalize_actor_for_publication(actor);
    let run_id = declaration
        .subject
        .strip_prefix("mission-run/")
        .unwrap_or(&declaration.subject);
    let current = mission_run_view_tx(transaction, run_id).map_err(internal)?;
    if generation_id_from_subject(&current.generation)
        != generation_id_from_subject(&operation.from_generation)
    {
        return Err(St3Error::new(
            "stale-run-generation",
            format!("revision `{}` names a stale run generation", operation.id),
        ));
    }
    if !matches!(current.status.as_str(), "running" | "standing" | "blocked")
        || current.phase != "normal"
    {
        return Err(St3Error::new(
            "mission-run-not-revisable",
            format!(
                "mission run `{}` is {} in its {} phase",
                declaration.subject, current.status, current.phase
            ),
        ));
    }
    let current_mission_id = current
        .mission
        .strip_prefix("mission/")
        .unwrap_or(&current.mission);
    if operation.mission != current_mission_id {
        return Err(St3Error::new(
            "wrong-mission-revision",
            format!(
                "revision `{}` targets mission `{}` instead of `{current_mission_id}`",
                operation.id, operation.mission
            ),
        ));
    }
    let old: MissionSpec = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![current_mission_id, current.revision],
            |row| row.get::<_, String>(0),
        )
        .map_err(internal)
        .and_then(|body| serde_json::from_str(&body).map_err(internal))?;
    let next: MissionSpec = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![operation.mission, operation.revision],
            |row| row.get::<_, String>(0),
        )
        .map_err(internal)
        .and_then(|body| serde_json::from_str(&body).map_err(internal))?;
    if next.state != MissionState::Ready {
        return Err(St3Error::new(
            "mission-revision-not-ready",
            "a running mission can adopt only a ready revision",
        ));
    }
    if old.inputs != next.inputs {
        return Err(St3Error::new(
            "run-input-mutation",
            "a run revision cannot change its input declarations",
        ));
    }
    let variables = mission_run_variables(&current, &next.revision);
    let (compatible, reviewers) =
        analyze_mission_revision(&old, &next, &actor, &current.requester, &variables)?;
    if !reviewers.is_empty() || matches!(old.revision_cutover, RevisionCutover::WhenIdle) {
        if operation.cancellation.is_some() {
            return Err(St3Error::new(
                "revision-cancellation-needs-immediate-cutover",
                "a revision with a deferred or reviewed cutover cannot contain cancellation",
            ));
        }
        if transaction
            .query_row(
                "SELECT 1 FROM revision_proposals WHERE run_id=?1 AND status IN ('pending-approval','draining')",
                [run_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(internal)?
            .is_some()
        {
            return Err(St3Error::new(
                "revision-proposal-already-pending",
                "the mission run already has one pending revision proposal",
            ));
        }
        let proposal_id = hex::encode(Sha256::digest(format!(
            "st3.declared-revision-proposal.v1\0{}\0{}",
            declaration.subject, operation.id
        )))[..32]
            .to_owned();
        let cutover = old.revision_cutover.clone();
        let status = if reviewers.is_empty() {
            "draining"
        } else {
            "pending-approval"
        };
        let preview_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&json!({
                "source_generation": current.generation,
                "candidate_revision": next.revision,
                "compatible_steps": compatible,
                "reviewers": reviewers,
                "cutover": cutover,
            }))
            .map_err(internal)?,
        ));
        let now = now_ms();
        transaction
            .execute(
                "INSERT INTO revision_proposals(id, run_id, source_generation_id, candidate_revision, actor, reason, status, cutover, compatible_steps, reviewers, approvals, preview_hash, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, '[]', ?11, ?12, ?12)",
                params![
                    proposal_id,
                    run_id,
                    generation_id_from_subject(&current.generation),
                    next.revision,
                    actor,
                    operation.reason,
                    status,
                    revision_cutover_name(&cutover),
                    serde_json::to_string(&compatible).map_err(internal)?,
                    serde_json::to_string(&reviewers).map_err(internal)?,
                    preview_hash,
                    now.to_string(),
                ],
            )
            .map_err(internal)?;
        if status == "draining" {
            transaction
                .execute(
                    "UPDATE mission_runs SET phase='revision-draining', updated_at_unix_ms=?2 WHERE id=?1",
                    params![run_id, now.to_string()],
                )
                .map_err(internal)?;
        }
        let subject = format!("revision-proposal/{proposal_id}");
        let claim = append_claim_tx(
            transaction,
            origin,
            &subject,
            "revision-proposal.created",
            Some(&actor),
            &json!({"fields": {
                "run": current.subject,
                "source_generation": current.generation,
                "candidate_revision": next.revision,
                "reason": operation.reason,
                "status": status,
                "cutover": revision_cutover_name(&cutover),
                "compatible_steps": compatible,
                "reviewers": reviewers,
                "preview_hash": preview_hash,
            }}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        return Ok((vec![claim.id], true));
    }

    let predecessor_id = generation_id_from_subject(&current.generation).to_owned();
    let predecessor_subject = current.generation.clone();
    let generation_id = Uuid::now_v7().simple().to_string();
    let generation_subject = format!("run-generation/{generation_id}");
    let now = now_ms();
    transaction
        .execute(
            "INSERT INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?6, ?7, ?7)",
            params![generation_id, run_id, next.revision, predecessor_id, actor, operation.reason, now.to_string()],
        )
        .map_err(internal)?;
    let mut variables = variables;
    variables.insert("ST_RUN_GENERATION".into(), generation_id.clone());
    let mut flat = Vec::new();
    flatten_steps(&next, None, &[], &mut flat);
    let mut claim_ids = Vec::new();
    for (step, selector, constraints) in flat {
        let (assignee, available_to, agentless) = interpolate_selector(&selector, &variables)?;
        let subject = format!("step-run/{generation_id}/{}", step.path);
        let carried = compatible
            .contains(&step.path)
            .then(|| current.steps.iter().find(|old| old.step == step.path))
            .flatten();
        let attempt = carried.map(|old| old.attempt).unwrap_or(1);
        let mut step_variables = variables.clone();
        step_variables.insert("ST_STEP".into(), step.path.clone());
        step_variables.insert("ST_STEP_RUN".into(), subject.clone());
        step_variables.insert("ST_ATTEMPT".into(), attempt.to_string());
        step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
        step_variables.insert(
            "ST_PARENT_STEP_RUN".into(),
            crate::mission::parent_step_path(&next, &step.path)
                .map(|path| format!("step-run/{generation_id}/{path}"))
                .or_else(|| current.parent_step_run.clone())
                .unwrap_or_default(),
        );
        let title = step
            .title
            .as_deref()
            .map(|value| crate::mission::interpolate(value, &step_variables))
            .transpose()?;
        let goals = interpolate_goals(&step.goals, &step_variables)?;
        let constraints = interpolate_goals(&constraints, &step_variables)?;
        let status = carried
            .map(|old| match old.status.as_str() {
                "claimed" | "working" | "verifying" => "ready",
                value => value,
            })
            .unwrap_or("pending");
        let worker_reported = carried.is_some_and(|old| old.worker_reported && status != "ready");
        let blocked_reason = carried.and_then(|old| old.blocked_reason.as_deref());
        let not_before = carried
            .and_then(|old| old.not_before_unix_ms)
            .map(|value| value.to_string());
        transaction
            .execute(
                "INSERT INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported, blocked_reason, not_before_unix_ms, readiness_epoch, created_at_unix_ms, updated_at_unix_ms, constraints)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?17, ?18)",
                params![subject, run_id, generation_id, step.path, step.definition_hash, status, attempt, assignee, serde_json::to_string(&available_to).map_err(internal)?, agentless, title, goals, worker_reported, blocked_reason, not_before, carried.map(|old| old.readiness_epoch).unwrap_or(0), now.to_string(), constraints],
            )
            .map_err(internal)?;
        if let Some(old) = carried {
            let claim = append_claim_tx(
                transaction,
                origin,
                &subject,
                "step-run.carried",
                Some(&actor),
                &json!({"fields": {
                    "source_step_run": old.subject,
                    "source_generation": predecessor_subject,
                    "status": status,
                    "attempt": attempt,
                    "worker_reported": worker_reported
                }}),
                &[],
                Some(batch_id),
            )
            .map_err(internal)?;
            claim_ids.push(claim.id);
        }
    }
    cancel_descendant_mission_runs_tx(
        transaction,
        origin,
        &predecessor_id,
        &actor,
        "the parent run generation was superseded",
        now,
    )?;
    transaction
        .execute(
            "UPDATE run_generations SET status='superseded', updated_at_unix_ms=?2 WHERE id=?1",
            params![predecessor_id, now.to_string()],
        )
        .map_err(internal)?;
    transaction
        .execute(
            "UPDATE mission_runs SET current_generation_id=?2, status='running', phase='normal', updated_at_unix_ms=?3 WHERE id=?1",
            params![run_id, generation_id, now.to_string()],
        )
        .map_err(internal)?;
    let superseded = append_claim_tx(
        transaction,
        origin,
        &predecessor_subject,
        "run-generation.superseded",
        Some(&actor),
        &json!({"fields": {
            "status": "superseded",
            "successor": generation_subject,
            "reason": operation.reason,
        }}),
        &[],
        Some(batch_id),
    )
    .map_err(internal)?;
    claim_ids.push(superseded.id);
    let created = append_claim_tx(
        transaction,
        origin,
        &generation_subject,
        "run-generation.created",
        Some(&actor),
        &json!({"fields": {
            "run": current.subject,
            "revision": next.revision,
            "predecessor": predecessor_subject,
            "status": "running",
            "reason": operation.reason,
            "compatible_steps": compatible,
        }}),
        &[],
        Some(batch_id),
    )
    .map_err(internal)?;
    claim_ids.push(created.id);
    Ok((claim_ids, false))
}

fn reset_declared_runtime_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    declaration: &MissionRunDeclaration,
    operation: &RuntimeResetOperation,
    runtime: &str,
    batch_id: &str,
) -> Result<ClaimRecord, St3Error> {
    let current_generation = transaction
        .query_row(
            "SELECT current_generation_id FROM mission_runs WHERE id=?1",
            [declaration
                .subject
                .strip_prefix("mission-run/")
                .unwrap_or(&declaration.subject)],
            |row| row.get::<_, String>(0),
        )
        .map_err(internal)?;
    if generation_id_from_subject(&operation.from_generation) != current_generation {
        return Err(St3Error::new(
            "stale-run-generation",
            format!("reset `{}` names a stale run generation", operation.id),
        ));
    }
    let desired = current_desired_row_tx(transaction, runtime)
        .map_err(internal)?
        .ok_or_else(|| {
            St3Error::new(
                "runtime-not-desired",
                format!("runtime `{runtime}` has no selected desired state"),
            )
        })?;
    if !matches!(desired.kind.as_str(), "agent" | "exec" | "pty") {
        return Err(St3Error::new(
            "not-a-runtime",
            format!("subject `{runtime}` is not an agent, exec, or PTY runtime"),
        ));
    }
    let actual = latest_actual(transaction, runtime)
        .map_err(internal)?
        .ok_or_else(|| {
            St3Error::new(
                "runtime-not-incarnated",
                format!("runtime `{runtime}` has no current incarnation"),
            )
        })?;
    let incarnation = actual
        .get("fields")
        .unwrap_or(&actual)
        .get("incarnation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "runtime-not-incarnated",
                format!("runtime `{runtime}` has no current incarnation"),
            )
        })?;
    append_claim_tx(
        transaction,
        origin,
        runtime,
        "runtime.restart-window-reset",
        None,
        &json!({"fields": {
            "desired_token": desired.claim_id,
            "incarnation_id": incarnation,
            "reason": operation.reason,
        }}),
        &[],
        Some(batch_id),
    )
    .map_err(internal)
}

fn request_declared_resource_refresh_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    operation: &ResourceRefreshOperation,
    batch_id: &str,
) -> Result<Vec<String>, St3Error> {
    let desired = {
        let mut statement = transaction
            .prepare("SELECT subject, body, revision FROM desired WHERE kind='observer'")
            .map_err(internal)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(internal)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal)?
    };
    let observers = desired
        .into_iter()
        .filter_map(|(subject, body, revision)| {
            serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|desired| crate::graph::observer_spec(&desired))
                .filter(|spec| !spec.stopped && spec.resource == operation.resource)
                .map(|_| (subject, revision))
        })
        .collect::<Vec<_>>();
    if observers.is_empty() {
        return Err(St3Error::new(
            "resource-not-observed",
            format!("resource `{}` has no active observer", operation.resource),
        ));
    }
    let now = now_ms();
    let mut claims = Vec::new();
    for (observer, revision) in observers {
        let attempt = hex::encode(Sha256::digest(format!(
            "st3.resource-refresh.v1\0{}\0{}\0{}",
            operation.resource, operation.id, observer
        )));
        let claim = append_claim_tx(
            transaction,
            origin,
            &observer,
            "observer.state",
            None,
            &json!({"fields": {
                "state": "healthy",
                "reason": "published refresh",
                "revision": revision,
                "attempt": attempt,
                "next_check_unix_ms": now.to_string(),
            }}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claims.push(claim.id);
    }
    Ok(claims)
}

fn apply_planning_session_declaration_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    declaration: &PlanningSessionDeclaration,
    batch_id: &str,
    actor: Option<&str>,
    receipts: &mut Vec<PlannedAction>,
) -> Result<Vec<String>, St3Error> {
    let id = declaration
        .subject
        .strip_prefix("planning-session/")
        .unwrap_or(&declaration.subject);
    let mut claim_ids = Vec::new();
    let mut exists = transaction
        .query_row(
            "SELECT requester, status FROM planning_sessions WHERE id=?1",
            [id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(internal)?;
    if exists.is_none()
        && let Some(creation) = &declaration.creation
    {
        let planner = crate::graph::planning_planner_subject(&declaration.subject);
        transaction
            .execute(
                "INSERT INTO planning_sessions(id, mission_id, request_ref, workspace, requester, planner, status, target_run_id, source_generation_id, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'planning', ?7, ?8, ?9, ?9)",
                params![
                    id,
                    creation.mission,
                    creation.request,
                    creation.workspace,
                    creation.requester,
                    planner,
                    creation.target_run.as_deref().map(|value| value.strip_prefix("mission-run/").unwrap_or(value)),
                    creation.target_generation.as_deref().map(|value| value.strip_prefix("run-generation/").unwrap_or(value)),
                    now_ms().to_string(),
                ],
            )
            .map_err(internal)?;
        let mut fields = serde_json::Map::from_iter([
            (
                "mission".into(),
                Value::String(format!("mission/{}", creation.mission)),
            ),
            ("request".into(), Value::String(creation.request.clone())),
            ("planner".into(), Value::String(planner.clone())),
            (
                "workspace".into(),
                Value::String(creation.workspace.clone()),
            ),
            (
                "requester".into(),
                Value::String(creation.requester.clone()),
            ),
        ]);
        if let Some(run) = &creation.target_run {
            fields.insert("target_run".into(), Value::String(run.clone()));
        }
        if let Some(generation) = &creation.target_generation {
            fields.insert(
                "target_generation".into(),
                Value::String(generation.clone()),
            );
        }
        let claim = append_claim_tx(
            transaction,
            origin,
            &declaration.subject,
            "planning-session.started",
            Some(&creation.requester),
            &json!({"fields": fields}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(claim.id);
        receipts.push(PlannedAction {
            subject: declaration.subject.clone(),
            action: "start-planning".into(),
            reason: "the named planning session was created".into(),
        });
        exists = Some((creation.requester.clone(), "planning".into()));
    }
    let Some((requester, mut status)) = exists else {
        return Err(St3Error::new(
            "missing-planning-session",
            format!("planning session `{}` does not exist", declaration.subject),
        ));
    };
    let actor = actor.map(normalize_actor_for_publication);
    if (!declaration.feedback.is_empty() || !declaration.cancellations.is_empty())
        && actor.as_deref() != Some(requester.as_str())
    {
        return Err(St3Error::new(
            "planning-review-not-authorized",
            format!(
                "the publication actor cannot change planning session `{}`",
                declaration.subject
            ),
        ));
    }
    for feedback in declaration.feedback.values() {
        if !publication_operation_is_new(
            transaction,
            &declaration.subject,
            "feedback",
            &feedback.id,
            feedback,
        )? {
            continue;
        }
        if !matches!(status.as_str(), "review" | "revision-requested") {
            return Err(St3Error::new(
                "invalid-planning-transition",
                format!(
                    "planning session `{}` cannot accept feedback while {status}",
                    declaration.subject
                ),
            ));
        }
        transaction
            .execute(
                "UPDATE planning_sessions SET status='revision-requested', updated_at_unix_ms=?2 WHERE id=?1",
                params![id, now_ms().to_string()],
            )
            .map_err(internal)?;
        transaction
            .execute("DELETE FROM planning_previews WHERE session_id=?1", [id])
            .map_err(internal)?;
        let claim = append_claim_tx(
            transaction,
            origin,
            &declaration.subject,
            "planning-session.revision-requested",
            actor.as_deref(),
            &json!({"fields": {
                "feedback": feedback.document,
                "variant": feedback.variant,
            }}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(claim.id);
        let message_subject = format!(
            "message/{}",
            &hex::encode(Sha256::digest(
                format!("{}:feedback:{}", declaration.subject, feedback.id).as_bytes()
            ))[..16]
        );
        let planner = crate::graph::planning_planner_subject(&declaration.subject);
        let message = append_claim_tx(
            transaction,
            origin,
            &message_subject,
            "message.sent",
            actor.as_deref(),
            &json!({"fields": {
                "from": actor,
                "to": planner,
                "content": feedback.document,
                "status": "sent",
                "title": "Planning feedback",
                "in_reply_to": Value::Null,
                "tags": ["planning"],
            }}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(message.id);
        receipts.push(PlannedAction {
            subject: declaration.subject.clone(),
            action: format!("feedback:{}", feedback.id),
            reason: "the planning feedback was published".into(),
        });
        let receipt = append_publication_operation_tx(
            transaction,
            origin,
            &declaration.subject,
            "feedback",
            &feedback.id,
            feedback,
            actor.as_deref(),
            batch_id,
        )?;
        claim_ids.push(receipt.id);
        status = "revision-requested".into();
    }
    for cancellation in declaration.cancellations.values() {
        if !publication_operation_is_new(
            transaction,
            &declaration.subject,
            "cancellation",
            &cancellation.id,
            cancellation,
        )? {
            continue;
        }
        if status == "cancelled" {
            let receipt = append_publication_operation_tx(
                transaction,
                origin,
                &declaration.subject,
                "cancellation",
                &cancellation.id,
                cancellation,
                actor.as_deref(),
                batch_id,
            )?;
            claim_ids.push(receipt.id);
            receipts.push(PlannedAction {
                subject: declaration.subject.clone(),
                action: format!("cancel:{}", cancellation.id),
                reason: "the planning session was already cancelled".into(),
            });
            continue;
        }
        transaction
            .execute(
                "UPDATE planning_sessions SET status='cancelled', updated_at_unix_ms=?2 WHERE id=?1",
                params![id, now_ms().to_string()],
            )
            .map_err(internal)?;
        let claim = append_claim_tx(
            transaction,
            origin,
            &declaration.subject,
            "planning-session.cancelled",
            actor.as_deref(),
            &json!({"fields": {
                "reason": cancellation.reason,
                "requester": requester,
            }}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(claim.id);
        receipts.push(PlannedAction {
            subject: declaration.subject.clone(),
            action: format!("cancel:{}", cancellation.id),
            reason: cancellation.reason.clone(),
        });
        let receipt = append_publication_operation_tx(
            transaction,
            origin,
            &declaration.subject,
            "cancellation",
            &cancellation.id,
            cancellation,
            actor.as_deref(),
            batch_id,
        )?;
        claim_ids.push(receipt.id);
        status = "cancelled".into();
    }
    Ok(claim_ids)
}

fn normalize_actor_for_publication(actor: &str) -> String {
    if actor.contains('/') {
        actor.to_owned()
    } else {
        format!("person/{actor}")
    }
}

#[derive(Clone)]
struct DesiredRow {
    kind: String,
    revision: String,
    claim_id: String,
    body: String,
    member: Option<String>,
    owner_run: Option<String>,
}

fn resolve_mission_run_inputs(
    transaction: &Connection,
    mission: &MissionSpec,
    supplied: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, MissionRunInput>, St3Error> {
    let declared = mission.inputs.keys().cloned().collect::<BTreeSet<_>>();
    let provided = supplied.keys().cloned().collect::<BTreeSet<_>>();
    if declared != provided {
        let missing = declared.difference(&provided).cloned().collect::<Vec<_>>();
        let extra = provided.difference(&declared).cloned().collect::<Vec<_>>();
        return Err(St3Error::new(
            "invalid-mission-inputs",
            format!(
                "the mission inputs do not match; missing [{}]; extra [{}]",
                missing.join(", "),
                extra.join(", ")
            ),
        ));
    }
    let mut resolved = BTreeMap::new();
    for (name, declaration) in &mission.inputs {
        let value = supplied.get(name).expect("the exact input set was checked");
        let input = match declaration.kind {
            MissionInputKind::Text => MissionRunInput {
                kind: MissionInputKind::Text,
                value: value.clone(),
                subject: None,
                claim_id: None,
            },
            MissionInputKind::Resource => {
                let (subject, requested_claim) = value
                    .rsplit_once('@')
                    .map_or((value.as_str(), None), |(subject, claim)| {
                        (subject, Some(claim))
                    });
                if !subject.starts_with("resource/") {
                    return Err(St3Error::new(
                        "invalid-resource-input",
                        format!("mission input `{name}` needs a resource subject"),
                    ));
                }
                let claim_id = if let Some(claim_id) = requested_claim {
                    let exists = transaction
                        .query_row(
                            "SELECT 1 FROM claims WHERE id=?1 AND subject=?2 AND kind='resource.observed'",
                            params![claim_id, subject],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(internal)?
                        .is_some();
                    if !exists {
                        return Err(St3Error::new(
                            "missing-resource-input-version",
                            format!("mission input `{name}` references an unavailable claim"),
                        ));
                    }
                    claim_id.to_owned()
                } else {
                    transaction
                        .query_row(
                            "SELECT id FROM claims WHERE subject=?1 AND kind='resource.observed' ORDER BY store_index DESC LIMIT 1",
                            [subject],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(internal)?
                        .ok_or_else(|| {
                            St3Error::new(
                                "missing-resource-input",
                                format!(
                                    "mission input `{name}` references a resource without an observation"
                                ),
                            )
                        })?
                };
                MissionRunInput {
                    kind: MissionInputKind::Resource,
                    value: format!("{subject}@{claim_id}"),
                    subject: Some(subject.to_owned()),
                    claim_id: Some(claim_id),
                }
            }
        };
        resolved.insert(name.clone(), input);
    }
    Ok(resolved)
}

fn enforce_mission_run_capacity(
    transaction: &Connection,
    requested: &MissionSpec,
) -> Result<(), St3Error> {
    let mut statement = transaction
        .prepare(
            "SELECT mission_runs.id, run_generations.revision
             FROM mission_runs JOIN run_generations
               ON run_generations.id=mission_runs.current_generation_id
             WHERE mission_runs.mission_id=?1
               AND mission_runs.status IN ('running','standing','blocked')
             ORDER BY mission_runs.created_at_unix_ms, mission_runs.id",
        )
        .map_err(internal)?;
    let active = statement
        .query_map([&requested.id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal)?;
    let mut limit = requested.max_active_runs;
    for (_, revision) in &active {
        let body = transaction
            .query_row(
                "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
                params![requested.id, revision],
                |row| row.get::<_, String>(0),
            )
            .map_err(internal)?;
        let revision = serde_json::from_str::<MissionSpec>(&body).map_err(internal)?;
        limit = match (limit, revision.max_active_runs) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
    }
    if limit.is_some_and(|limit| active.len() >= limit as usize) {
        return Err(St3Error::new(
            "mission-run-capacity",
            format!(
                "mission `{}` reached its active run limit; active runs: {}",
                requested.id,
                active
                    .iter()
                    .map(|(id, _)| format!("mission-run/{id}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    Ok(())
}

fn current_desired_row(connection: &Connection, subject: &str) -> Result<Option<DesiredRow>> {
    connection
        .query_row(
            "SELECT kind, revision, claim_id, body, member, owner_run FROM desired WHERE subject=?1",
            [subject],
            |row| {
                Ok(DesiredRow {
                    kind: row.get(0)?,
                    revision: row.get(1)?,
                    claim_id: row.get(2)?,
                    body: row.get(3)?,
                    member: row.get(4)?,
                    owner_run: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn normalize_message_party(value: &str) -> String {
    if value.is_empty() || value == "requester" || value.contains('/') {
        value.into()
    } else {
        format!("agent/{value}")
    }
}

fn subject_owned_by(connection: &Connection, subject: &str, owner_run: &str) -> bool {
    current_desired_row(connection, subject)
        .ok()
        .flatten()
        .and_then(|row| row.owner_run)
        .is_some_and(|owner| owner == owner_run)
}

fn desired_row_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Option<DesiredRow>> {
    let Some(at_index) = at_index else {
        return current_desired_row(connection, subject);
    };
    let mut statement = connection.prepare(
        "SELECT id, body, predecessors FROM claims
         WHERE subject=?1 AND kind='intent.desired' AND store_index<=?2 ORDER BY store_index",
    )?;
    let rows = statement
        .query_map(params![subject, at_index], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let referenced = rows
        .iter()
        .flat_map(|(_, _, predecessors)| {
            serde_json::from_str::<Vec<String>>(predecessors).unwrap_or_default()
        })
        .collect::<BTreeSet<_>>();
    let mut selected = None::<(String, String, DesiredSubject)>;
    for (id, body, _) in rows
        .into_iter()
        .filter(|(id, _, _)| !referenced.contains(id))
    {
        let desired: DesiredSubject = serde_json::from_str(&body)?;
        let revision = desired_revision(&desired);
        if selected
            .as_ref()
            .is_none_or(|(current_revision, current_id, _)| {
                (revision.as_str(), id.as_str()) > (current_revision.as_str(), current_id.as_str())
            })
        {
            selected = Some((revision, id, desired));
        }
    }
    selected
        .map(|(revision, claim_id, desired)| {
            Ok(DesiredRow {
                kind: desired.kind,
                revision,
                claim_id,
                body: serde_json::to_string(&desired.desired)?,
                member: desired
                    .member
                    .map(|member| serde_json::to_string(&member))
                    .transpose()?,
                owner_run: desired.owner_run,
            })
        })
        .transpose()
}

fn current_desired_row_tx(
    transaction: &Transaction<'_>,
    subject: &str,
) -> Result<Option<DesiredRow>> {
    transaction
        .query_row(
            "SELECT kind, revision, claim_id, body, member, owner_run FROM desired WHERE subject=?1",
            [subject],
            |row| {
                Ok(DesiredRow {
                    kind: row.get(0)?,
                    revision: row.get(1)?,
                    claim_id: row.get(2)?,
                    body: row.get(3)?,
                    member: row.get(4)?,
                    owner_run: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn desired_revision(desired: &DesiredSubject) -> String {
    canonical_hash(desired).expect("desired subject serializes")
}

fn validate_documents(
    transaction: &Transaction<'_>,
    references: &BTreeSet<String>,
) -> Result<(), St3Error> {
    for reference in references {
        let (name, hash) = split_document_ref(reference)?;
        let bytes: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT b.bytes FROM documents d JOIN blobs b ON b.hash=d.hash WHERE d.name=?1 AND d.hash=?2",
                params![name, hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(internal)?;
        let Some(bytes) = bytes else {
            return Err(St3Error::new(
                "missing-document",
                format!("document `{reference}` is not stored"),
            ));
        };
        if hex::encode(Sha256::digest(&bytes)) != hash {
            return Err(St3Error::new(
                "document-hash-mismatch",
                format!("stored bytes for `{reference}` do not match the reference"),
            ));
        }
    }
    Ok(())
}

fn split_document_ref(reference: &str) -> Result<(&str, &str), St3Error> {
    reference.rsplit_once('@').ok_or_else(|| {
        St3Error::new(
            "invalid-document-reference",
            format!("document reference `{reference}` has no hash"),
        )
    })
}

fn validate_document_name(name: &str) -> Result<(), St3Error> {
    if !name.starts_with("doc/")
        || name.len() <= 4
        || name.contains('@')
        || name.contains("..")
        || name.ends_with('/')
    {
        return Err(St3Error::new(
            "invalid-document-name",
            "a document name must use `doc/NAME` and cannot contain `@` or `..`",
        ));
    }
    Ok(())
}

fn find_document(
    connection: &Connection,
    name: &str,
    hash: &str,
) -> Result<Option<DocumentVersion>> {
    connection
        .query_row(
            "SELECT d.name, d.hash, b.size, d.created_index,
             d.created_index=(SELECT MAX(n.created_index) FROM documents n WHERE n.name=d.name),
             d.binding_claim_id
             FROM documents d JOIN blobs b ON b.hash=d.hash WHERE d.name=?1 AND d.hash=?2",
            params![name, hash],
            |row| {
                Ok(DocumentVersion {
                    name: row.get(0)?,
                    hash: row.get(1)?,
                    size: row.get(2)?,
                    created_index: row.get(3)?,
                    latest: row.get::<_, i64>(4)? != 0,
                    binding_claim_id: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn validate_claim_kind(kind: &str) -> Result<(), St3Error> {
    if st3_schema::is_known_claim(kind) {
        Ok(())
    } else {
        Err(St3Error::new(
            "unknown-claim-kind",
            format!("claim kind `{kind}` is not registered in st3.v1"),
        ))
    }
}

fn validate_claim_subject(subject: &str) -> Result<(), St3Error> {
    st3_schema::registry()
        .validate_subject(subject)
        .map(|_| ())
        .map_err(|error| St3Error::new(error.code, error.message))
}

fn validate_actor(actor: &str) -> Result<(), St3Error> {
    if actor == "requester" {
        return Ok(());
    }
    validate_claim_subject(actor).map_err(|_| {
        St3Error::new(
            "invalid-claim-actor",
            "a claim actor must be `requester` or a full subject",
        )
    })
}

fn validate_claim_fields(input: &ClaimInput) -> Result<&'static st3_schema::ClaimSpec, St3Error> {
    st3_schema::registry()
        .validate_claim(&input.subject, &input.kind, &input.fields)
        .map_err(|error| St3Error::new(error.code, error.message))
}

fn validate_claim_cardinality(
    transaction: &Transaction<'_>,
    subject: &str,
    kind: &str,
    actor: Option<&str>,
    cardinality: &st3_schema::Cardinality,
) -> Result<(), St3Error> {
    let duplicate = match cardinality {
        st3_schema::Cardinality::Append | st3_schema::Cardinality::StateTransition => false,
        st3_schema::Cardinality::Once => transaction
            .query_row(
                "SELECT 1 FROM claims WHERE subject=?1 AND kind=?2 LIMIT 1",
                params![subject, kind],
                |_| Ok(()),
            )
            .optional()
            .map_err(internal)?
            .is_some(),
        st3_schema::Cardinality::OncePerActor => transaction
            .query_row(
                "SELECT 1 FROM claims WHERE subject=?1 AND kind=?2 AND actor IS ?3 LIMIT 1",
                params![subject, kind, actor],
                |_| Ok(()),
            )
            .optional()
            .map_err(internal)?
            .is_some(),
    };
    if duplicate {
        return Err(St3Error::new(
            "claim-cardinality",
            format!(
                "claim kind `{}` already exists at its allowed cardinality for `{}`",
                kind, subject
            ),
        ));
    }
    Ok(())
}

fn normalize_resource_observation(
    transaction: &Transaction<'_>,
    input: &ClaimInput,
) -> Result<Option<BTreeMap<String, Value>>, St3Error> {
    if input.kind != "resource.observed" {
        return Ok(None);
    }
    let prior_kind = latest_resource_kind(transaction, &input.subject)?;
    let desired_kind = current_desired_row_tx(transaction, &input.subject)
        .map_err(internal)?
        .filter(|desired| desired.kind == "resource")
        .and_then(|desired| serde_json::from_str::<Value>(&desired.body).ok())
        .and_then(|desired| canonical_child_string(&desired, "kind"));
    let requested_kind = input.fields.get("kind").and_then(Value::as_str);
    let kind = requested_kind
        .map(str::to_owned)
        .or_else(|| prior_kind.clone())
        .or(desired_kind)
        .ok_or_else(|| {
            St3Error::new(
                "missing-resource-kind",
                "the first resource observation needs a registered resource kind",
            )
        })?;
    if prior_kind.as_deref().is_some_and(|prior| prior != kind) {
        return Err(St3Error::new(
            "immutable-resource-kind",
            format!(
                "resource `{}` already has kind `{}`",
                input.subject,
                prior_kind.unwrap()
            ),
        ));
    }
    let facts = resource_facts(&input.fields)?;
    let spec = st3_schema::registry()
        .validate_resource_facts(&kind, &facts)
        .map_err(|error| St3Error::new(error.code, error.message))?;
    if let Some(previous) = latest_actual(transaction, &input.subject).map_err(internal)? {
        let previous = previous
            .get("facts")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| previous.as_object().cloned().unwrap_or_default());
        for (name, field) in &spec.fields {
            if field.immutable
                && let Some(value) = facts.get(name)
                && previous.get(name).is_some_and(|prior| prior != value)
            {
                return Err(St3Error::new(
                    "immutable-resource-field",
                    format!(
                        "resource field `{name}` cannot change for `{}`",
                        input.subject
                    ),
                ));
            }
        }
    }
    let mut fields = input.fields.clone();
    fields.insert("kind".into(), Value::String(kind));
    Ok(Some(fields))
}

fn resource_facts(fields: &BTreeMap<String, Value>) -> Result<BTreeMap<String, Value>, St3Error> {
    if let Some(value) = fields.get("facts") {
        return value
            .as_object()
            .map(|facts| {
                facts
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect()
            })
            .ok_or_else(|| {
                St3Error::new(
                    "invalid-resource-observation",
                    "resource observation facts must be an object",
                )
            });
    }
    const ENVELOPE: &[&str] = &[
        "kind",
        "observed_at",
        "observer",
        "baseline",
        "changed_fields",
    ];
    Ok(fields
        .iter()
        .filter(|(name, _)| !ENVELOPE.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

fn latest_resource_kind(
    transaction: &Transaction<'_>,
    subject: &str,
) -> Result<Option<String>, St3Error> {
    let bodies = {
        let mut statement = transaction
            .prepare(
                "SELECT body FROM claims WHERE subject=?1 AND kind='resource.observed' ORDER BY store_index DESC",
            )
            .map_err(internal)?;
        statement
            .query_map([subject], |row| row.get::<_, String>(0))
            .map_err(internal)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal)?
    };
    Ok(bodies.into_iter().find_map(|body| {
        serde_json::from_str::<Value>(&body)
            .ok()?
            .pointer("/fields/kind")?
            .as_str()
            .map(str::to_owned)
    }))
}

fn registered_client_claim_kind(kind: &str) -> bool {
    st3_schema::is_known_claim(kind)
}

fn opaque_cache_key(key: &str) -> String {
    format!(
        "cache/{}",
        hex::encode(Sha256::digest(
            [b"st3.idempotency-cache.v1\0".as_slice(), key.as_bytes()].concat()
        ))
    )
}

fn operation_id_for_key(key: &str) -> String {
    format!(
        "op/{}",
        hex::encode(Sha256::digest(
            [b"st3.idempotency.v1\0".as_slice(), key.as_bytes()].concat()
        ))
    )
}

fn publication_operation_key(subject: &str, action: &str, id: &str) -> String {
    operation_id_for_key(&format!("st3.publish.v1\0{subject}\0{action}\0{id}"))
}

fn publication_operation_digest<T: Serialize>(
    subject: &str,
    action: &str,
    value: &T,
) -> Result<String, St3Error> {
    canonical_hash(&("st3.publish-operation.v1", subject, action, value)).map_err(internal)
}

fn publication_operation_is_new<T: Serialize>(
    connection: &Connection,
    subject: &str,
    action: &str,
    id: &str,
    value: &T,
) -> Result<bool, St3Error> {
    let operation_id = publication_operation_key(subject, action, id);
    let digest = publication_operation_digest(subject, action, value)?;
    match operation_tx(connection, &operation_id).map_err(internal)? {
        None => Ok(true),
        Some((stored, _, state)) if state == "active" && stored == digest => Ok(false),
        Some((_, _, state)) if state == "conflict" => Err(St3Error::new(
            "idempotency-conflict",
            format!("operation `{id}` has conflicting replicated claims"),
        )),
        Some(_) => Err(St3Error::new(
            "immutable-operation-id",
            format!("operation `{id}` repeats with different content"),
        )),
    }
}

fn append_publication_operation_tx<T: Serialize>(
    transaction: &Transaction<'_>,
    origin: &str,
    subject: &str,
    action: &str,
    id: &str,
    value: &T,
    actor: Option<&str>,
    batch_id: &str,
) -> Result<ClaimRecord, St3Error> {
    let operation_id = publication_operation_key(subject, action, id);
    let request_digest = publication_operation_digest(subject, action, value)?;
    let body = json!({
        "fields": {
            "operation": id,
            "action": action,
            "status": "accepted",
        },
        "_operation": {
            "id": operation_id,
            "request_digest": request_digest,
        },
    });
    let claim = append_claim_tx(
        transaction,
        origin,
        subject,
        "publication.operation",
        actor,
        &body,
        &[],
        Some(batch_id),
    )
    .map_err(internal)?;
    register_operation_tx(transaction, &claim).map_err(internal)?;
    Ok(claim)
}

fn claim_operation(input: &ClaimInput) -> Result<Option<(String, String)>, St3Error> {
    let Some(key) = input.idempotency_key.as_deref() else {
        return Ok(None);
    };
    if key.is_empty() || key.len() > 512 || key.chars().any(char::is_control) {
        return Err(St3Error::new(
            "invalid-idempotency-key",
            "an idempotency key must contain 1 through 512 non-control characters",
        ));
    }
    let operation_id = operation_id_for_key(key);
    let mut evidence = input.evidence.clone();
    evidence.sort();
    evidence.dedup();
    let request_digest = canonical_hash(&(
        "st3.claim-request.v1",
        &input.subject,
        &input.kind,
        &input.actor,
        &input.fields,
        evidence,
        &input.expected_subject,
    ))
    .map_err(internal)?;
    Ok(Some((operation_id, request_digest)))
}

fn operation_parts(body: &Value) -> Option<(&str, &str)> {
    Some((
        body.pointer("/_operation/id")?.as_str()?,
        body.pointer("/_operation/request_digest")?.as_str()?,
    ))
}

fn operation_tx(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<(String, String, String)>> {
    connection
        .query_row(
            "SELECT request_digest, canonical_claim_id, state FROM operations WHERE id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(Into::into)
}

fn renew_nested_ancestor_leases_tx(
    transaction: &Transaction<'_>,
    generation: &str,
    step_path: &str,
    actor: &str,
    incarnation: Option<&str>,
    now: u128,
) -> std::result::Result<(), St3Error> {
    let generation = generation_id_from_subject(generation);
    let expiry = now.saturating_add(600_000);
    transaction
        .execute(
            "UPDATE step_runs
             SET lease_expires_at_unix_ms=?1, updated_at_unix_ms=?2
             WHERE generation_id=?3
               AND substr(?4, 1, length(step_path) + 1)=step_path || '/'
               AND status IN ('claimed','working','verifying')
               AND lease_owner=?5
               AND lease_incarnation IS ?6",
            params![
                expiry.to_string(),
                now.to_string(),
                generation,
                step_path,
                actor,
                incarnation
            ],
        )
        .map_err(internal)?;
    Ok(())
}

fn claim_by_id_tx(connection: &Connection, id: &str) -> Result<Option<ClaimRecord>> {
    connection
        .query_row(
            "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms FROM claims WHERE id=?1",
            [id],
            claim_from_row,
        )
        .optional()
        .map_err(Into::into)
}

fn expected_operations(
    connection: &Connection,
) -> Result<BTreeMap<String, (String, String, String)>> {
    let mut statement = connection.prepare(
        "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
         FROM claims
         WHERE json_extract(body, '$._operation.id') IS NOT NULL
         ORDER BY id",
    )?;
    let claims = statement
        .query_map([], claim_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut grouped = BTreeMap::<String, Vec<(String, String)>>::new();
    for claim in claims {
        if let Some((operation_id, request_digest)) = operation_parts(&claim.body) {
            grouped
                .entry(operation_id.to_owned())
                .or_default()
                .push((request_digest.to_owned(), claim.id));
        }
    }
    Ok(grouped
        .into_iter()
        .map(|(operation_id, mut claims)| {
            claims.sort();
            let request_digest = claims[0].0.clone();
            let state = if claims.iter().all(|(digest, _)| digest == &request_digest) {
                "active"
            } else {
                "conflict"
            };
            let canonical_claim_id = claims
                .iter()
                .filter(|(digest, _)| digest == &request_digest)
                .map(|(_, claim)| claim)
                .min()
                .expect("an operation has at least one claim")
                .clone();
            (
                operation_id,
                (request_digest, canonical_claim_id, state.into()),
            )
        })
        .collect())
}

fn rebuild_operations_tx(transaction: &Transaction<'_>) -> Result<()> {
    transaction.execute("DELETE FROM operations", [])?;
    for (id, (request_digest, canonical_claim_id, state)) in expected_operations(transaction)? {
        transaction.execute(
            "INSERT INTO operations(id, request_digest, canonical_claim_id, state) VALUES (?1, ?2, ?3, ?4)",
            params![id, request_digest, canonical_claim_id, state],
        )?;
    }
    Ok(())
}

fn rebuild_planning_tx(transaction: &Transaction<'_>) -> Result<()> {
    transaction.execute("DELETE FROM planning_previews", [])?;
    transaction.execute("DELETE FROM planning_candidates", [])?;
    transaction.execute("DELETE FROM planning_sessions", [])?;
    let mut statement = transaction.prepare(
        "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
         FROM claims WHERE kind LIKE 'planning-session.%' ORDER BY store_index",
    )?;
    let claims = statement
        .query_map([], claim_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for claim in claims {
        let id = claim
            .subject
            .strip_prefix("planning-session/")
            .context("a planning claim has an invalid subject")?;
        let fields = claim
            .body
            .get("fields")
            .and_then(Value::as_object)
            .context("a planning claim has no fields")?;
        let text = |name: &str| fields.get(name).and_then(Value::as_str);
        let accepted = claim.accepted_at_unix_ms.to_string();
        match claim.kind.as_str() {
            "planning-session.started" => {
                let mission = text("mission")
                    .context("planning-session.started has no mission")?
                    .strip_prefix("mission/")
                    .unwrap_or_else(|| text("mission").expect("the mission was checked"));
                let target_run = text("target_run")
                    .map(|value| value.strip_prefix("mission-run/").unwrap_or(value));
                let target_generation = text("target_generation")
                    .map(|value| value.strip_prefix("run-generation/").unwrap_or(value));
                transaction.execute(
                    "INSERT INTO planning_sessions(id, mission_id, request_ref, workspace, requester, planner, status, target_run_id, source_generation_id, created_at_unix_ms, updated_at_unix_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'planning', ?7, ?8, ?9, ?9)",
                    params![
                        id,
                        mission,
                        text("request").context("planning-session.started has no request")?,
                        text("workspace").context("planning-session.started has no workspace")?,
                        text("requester").or(claim.actor.as_deref()).context("planning-session.started has no requester")?,
                        text("planner").context("planning-session.started has no planner")?,
                        target_run,
                        target_generation,
                        accepted,
                    ],
                )?;
            }
            "planning-session.candidate-submitted" => {
                let variant = text("variant").unwrap_or("default");
                let revision = fields
                    .get("candidate_revision")
                    .or_else(|| fields.get("revision"))
                    .and_then(Value::as_u64)
                    .context("a planning candidate has no revision")?;
                transaction.execute(
                    "INSERT INTO planning_candidates(session_id, variant, revision, markdown_ref, kdl_ref, mission_revision, submitted_at_unix_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id,
                        variant,
                        revision,
                        text("markdown").context("a planning candidate has no Markdown")?,
                        text("kdl").context("a planning candidate has no KDL")?,
                        text("mission_revision").context("a planning candidate has no mission revision")?,
                        accepted,
                    ],
                )?;
                transaction.execute(
                    "DELETE FROM planning_previews WHERE session_id=?1 AND variant=?2",
                    params![id, variant],
                )?;
                transaction.execute(
                    "UPDATE planning_sessions SET status='review', updated_at_unix_ms=?2 WHERE id=?1",
                    params![id, accepted],
                )?;
            }
            "planning-session.previewed" => {
                let variant = text("variant").unwrap_or("default");
                let revision = fields
                    .get("candidate_revision")
                    .and_then(Value::as_u64)
                    .context("a planning preview has no candidate revision")?;
                let mission = fields
                    .get("mission")
                    .context("a planning preview has no mission response")?;
                transaction.execute(
                    "INSERT INTO planning_previews(session_id, variant, candidate_revision, hash, store_index, graph, diff, mission_response, created_at_unix_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(session_id, variant) DO UPDATE SET candidate_revision=excluded.candidate_revision, hash=excluded.hash,
                       store_index=excluded.store_index, graph=excluded.graph, diff=excluded.diff,
                       mission_response=excluded.mission_response, created_at_unix_ms=excluded.created_at_unix_ms",
                    params![
                        id,
                        variant,
                        revision,
                        text("preview_hash").context("a planning preview has no hash")?,
                        fields.get("store_index").and_then(Value::as_u64).context("a planning preview has no store index")?,
                        text("graph").context("a planning preview has no graph")?,
                        text("diff").context("a planning preview has no diff")?,
                        serde_json::to_string(mission)?,
                        accepted,
                    ],
                )?;
            }
            "planning-session.revision-requested" => {
                transaction.execute(
                    "UPDATE planning_sessions SET status='revision-requested', updated_at_unix_ms=?2 WHERE id=?1",
                    params![id, accepted],
                )?;
                transaction.execute("DELETE FROM planning_previews WHERE session_id=?1", [id])?;
            }
            "planning-session.approved" => {
                transaction.execute(
                    "UPDATE planning_sessions SET status='approved', published_revision=?2, updated_at_unix_ms=?3 WHERE id=?1",
                    params![id, text("mission_revision"), accepted],
                )?;
            }
            "planning-session.cancelled" => {
                transaction.execute(
                    "UPDATE planning_sessions SET status='cancelled', updated_at_unix_ms=?2 WHERE id=?1",
                    params![id, accepted],
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn register_operation_tx(transaction: &Transaction<'_>, claim: &ClaimRecord) -> Result<bool> {
    let Some((operation_id, request_digest)) = operation_parts(&claim.body) else {
        return Ok(true);
    };
    let current = operation_tx(transaction, operation_id)?;
    match current {
        None => {
            transaction.execute(
                "INSERT INTO operations(id, request_digest, canonical_claim_id, state) VALUES (?1, ?2, ?3, 'active')",
                params![operation_id, request_digest, claim.id],
            )?;
            Ok(true)
        }
        Some((stored_digest, canonical, state)) if stored_digest == request_digest => {
            if claim.id < canonical {
                transaction.execute(
                    "UPDATE operations SET canonical_claim_id=?2 WHERE id=?1",
                    params![operation_id, claim.id],
                )?;
            }
            Ok(state == "active" && canonical == claim.id)
        }
        Some(_) => {
            transaction.execute(
                "UPDATE operations SET state='conflict' WHERE id=?1",
                [operation_id],
            )?;
            Ok(false)
        }
    }
}

fn known_replicated_claim_kind(kind: &str) -> bool {
    matches!(
        kind,
        "intent.desired" | "doc.bound" | "mission.published" | "mission.produced"
    ) || registered_client_claim_kind(kind)
}

fn has_unknown_claim_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Option<String>> {
    let through = at_index.unwrap_or(i64::MAX as u64);
    let conflict = connection
        .query_row(
            "SELECT operations.id FROM claims JOIN operations
             ON operations.id=json_extract(claims.body, '$._operation.id')
             WHERE claims.subject=?1 AND claims.store_index<=?2 AND operations.state='conflict'
             ORDER BY operations.id LIMIT 1",
            params![subject, through],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(operation) = conflict {
        return Ok(Some(format!("idempotency-conflict:{operation}")));
    }
    let mut statement = connection.prepare(
        "SELECT DISTINCT kind FROM claims WHERE subject=?1 AND store_index<=?2 ORDER BY kind",
    )?;
    let kinds = statement
        .query_map(params![subject, through], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(kinds
        .into_iter()
        .find(|kind| !known_replicated_claim_kind(kind)))
}

fn validate_message_transition(
    transaction: &Transaction<'_>,
    input: &ClaimInput,
) -> Result<(), St3Error> {
    let requested = match input.kind.as_str() {
        "message.sent" => "sent",
        "message.delivered" => "delivered",
        "message.read" => "read",
        "message.closed" => "closed",
        _ => return Ok(()),
    };
    if !input.subject.starts_with("message/") {
        return Err(St3Error::new(
            "invalid-message-subject",
            "a message lifecycle claim needs a `message/` subject",
        ));
    }
    if input.fields.get("status").and_then(Value::as_str) != Some(requested) {
        return Err(St3Error::new(
            "invalid-message-status",
            format!("`{}` needs status `{requested}`", input.kind),
        ));
    }
    if matches!(requested, "read" | "closed") && input.actor.is_none() {
        return Err(St3Error::new(
            "missing-message-actor",
            format!("`{}` needs an actor", input.kind),
        ));
    }

    let current: Option<String> = transaction
        .query_row(
            "SELECT kind FROM claims WHERE subject=?1 AND kind IN ('message.sent','message.delivered','message.read','message.closed') ORDER BY store_index DESC LIMIT 1",
            [&input.subject],
            |row| row.get(0),
        )
        .optional()
        .map_err(internal)?;
    let current = current.as_deref().map(|kind| match kind {
        "message.sent" => "sent",
        "message.delivered" => "delivered",
        "message.read" => "read",
        "message.closed" => "closed",
        _ => unreachable!("the SQL query selects message lifecycle kinds"),
    });
    let current = if current.is_some() {
        current
    } else {
        let declared: Option<String> = transaction
            .query_row(
                "SELECT kind FROM desired WHERE subject=?1",
                [&input.subject],
                |row| row.get(0),
            )
            .optional()
            .map_err(internal)?;
        declared
            .as_deref()
            .filter(|kind| *kind == "message")
            .map(|_| "sent")
    };
    let valid = matches!(
        (current, requested),
        (None, "sent")
            | (Some("sent"), "delivered")
            | (Some("delivered"), "read")
            | (Some("read"), "closed")
    );
    if !valid {
        return Err(St3Error::new(
            "invalid-message-transition",
            format!(
                "message `{}` cannot move from `{}` to `{requested}`",
                input.subject,
                current.unwrap_or("absent")
            ),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_claim_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    subject: &str,
    kind: &str,
    actor: Option<&str>,
    body: &Value,
    predecessors: &[String],
    forced_batch: Option<&str>,
) -> Result<ClaimRecord> {
    let fields = schema_fields_for_body(kind, body)?;
    let claim_spec = st3_schema::registry()
        .validate_claim(subject, kind, &fields)
        .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?;
    validate_claim_cardinality(transaction, subject, kind, actor, &claim_spec.cardinality)
        .map_err(anyhow::Error::new)?;
    let now = now_ms();
    let batch_id = if let Some(batch) = forced_batch {
        batch.to_owned()
    } else {
        let sequence = next_replica_sequence(transaction, origin)?;
        let previous_hash = previous_batch_hash(transaction, origin)?;
        let hash = batch_header_hash(origin, sequence, previous_hash.as_deref(), now)?;
        let id = format!("batch/{origin}/{sequence}/{hash}");
        transaction.execute(
            "INSERT INTO batches(id, origin, replica_sequence, previous_hash, hash, accepted_at_unix_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, origin, sequence, previous_hash, hash, now.to_string()],
        )?;
        id
    };
    let id = claim_hash(&batch_id, subject, kind, origin, actor, body, predecessors)?;
    let store_index = insert_claim(
        transaction,
        &id,
        &batch_id,
        subject,
        kind,
        origin,
        actor,
        body,
        predecessors,
        now,
    )?;
    insert_event(transaction, store_index, kind, subject, body)?;
    Ok(ClaimRecord {
        id,
        store_index,
        batch_id,
        subject: subject.into(),
        kind: kind.into(),
        origin: origin.into(),
        actor: actor.map(str::to_owned),
        operation_id: operation_parts(body).map(|(id, _)| id.to_owned()),
        request_digest: operation_parts(body).map(|(_, digest)| digest.to_owned()),
        body: body.clone(),
        predecessors: predecessors.to_vec(),
        accepted_at_unix_ms: now,
    })
}

fn schema_fields_for_body(kind: &str, body: &Value) -> Result<BTreeMap<String, Value>> {
    if let Some(fields) = body.get("fields").and_then(Value::as_object) {
        return Ok(fields
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect());
    }
    if kind == "intent.desired" {
        let desired: DesiredSubject = serde_json::from_value(body.clone())?;
        return Ok(BTreeMap::from([
            ("kind".into(), Value::String(desired.kind.clone())),
            ("revision".into(), Value::String(desired_revision(&desired))),
            ("desired".into(), desired.desired),
        ]));
    }
    if kind == "mission.published" {
        let mission: MissionSpec = serde_json::from_value(body.clone())?;
        return Ok(BTreeMap::from([
            ("revision".into(), Value::String(mission.revision.clone())),
            ("state".into(), serde_json::to_value(&mission.state)?),
            ("body".into(), body.clone()),
        ]));
    }
    Ok(body
        .as_object()
        .context("a claim body must be an object")?
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn insert_claim(
    transaction: &Transaction<'_>,
    id: &str,
    batch_id: &str,
    subject: &str,
    kind: &str,
    origin: &str,
    actor: Option<&str>,
    body: &Value,
    predecessors: &[String],
    now: u128,
) -> Result<u64> {
    transaction.execute(
        "INSERT INTO claims(id, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            id,
            batch_id,
            subject,
            kind,
            origin,
            actor,
            serde_json::to_string(body)?,
            serde_json::to_string(predecessors)?,
            now.to_string(),
        ],
    )?;
    Ok(transaction.last_insert_rowid() as u64)
}

fn insert_event(
    transaction: &Transaction<'_>,
    store_index: u64,
    kind: &str,
    subject: &str,
    body: &Value,
) -> Result<()> {
    transaction.execute(
        "INSERT OR IGNORE INTO events(store_index, kind, subject, body) VALUES (?1, ?2, ?3, ?4)",
        params![store_index, kind, subject, serde_json::to_string(body)?],
    )?;
    Ok(())
}

fn next_replica_sequence(transaction: &Transaction<'_>, origin: &str) -> Result<u64> {
    transaction
        .query_row(
            "SELECT COALESCE(MAX(replica_sequence), 0) + 1 FROM batches WHERE origin=?1",
            [origin],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn previous_batch_hash(transaction: &Transaction<'_>, origin: &str) -> Result<Option<String>> {
    transaction
        .query_row(
            "SELECT hash FROM batches WHERE origin=?1 ORDER BY replica_sequence DESC LIMIT 1",
            [origin],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn latest_claim_id_tx(transaction: &Transaction<'_>, subject: &str) -> Result<Option<String>> {
    transaction
        .query_row(
            "SELECT id FROM claims WHERE subject=?1 ORDER BY store_index DESC LIMIT 1",
            [subject],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn current_index(connection: &Connection) -> Result<u64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(store_index), 0) FROM claims",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn replica_heads(connection: &Connection) -> Result<BTreeMap<String, u64>> {
    let mut statement = connection.prepare(
        "SELECT origin, MAX(replica_sequence) FROM batches GROUP BY origin ORDER BY origin",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

fn current_index_tx(transaction: &Transaction<'_>) -> Result<u64> {
    transaction
        .query_row(
            "SELECT COALESCE(MAX(store_index), 0) FROM claims",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn selected_index(current: u64, requested: Option<u64>) -> Result<u64, St3Error> {
    match requested {
        Some(requested) if requested > current => Err(St3Error::new(
            "invalid-snapshot-index",
            format!("snapshot index {requested} is after current index {current}"),
        )),
        Some(requested) => Ok(requested),
        None => Ok(current),
    }
}

fn latest_actual(connection: &Connection, subject: &str) -> Result<Option<Value>> {
    latest_actual_at(connection, subject, None)
}

fn latest_actual_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Option<Value>> {
    let at_index = at_index.unwrap_or(i64::MAX as u64);
    let mut statement = connection.prepare(
        "SELECT body FROM claims WHERE subject=?1 AND kind!='intent.desired' AND store_index<=?2 ORDER BY store_index",
    )?;
    let rows = statement
        .query_map(params![subject, at_index], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut merged = serde_json::Map::new();
    for body in rows {
        let value: Value = serde_json::from_str(&body)?;
        let source = value.get("fields").unwrap_or(&value);
        if let Some(fields) = source.as_object() {
            for (key, value) in fields {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(Some(Value::Object(merged)))
}

fn claim_ids_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Vec<String>> {
    let at_index = at_index.unwrap_or(i64::MAX as u64);
    let mut statement = connection.prepare(
        "SELECT id FROM claims WHERE subject=?1 AND store_index<=?2 ORDER BY store_index",
    )?;
    let rows = statement.query_map(params![subject, at_index], |row| row.get(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn desired_conflicts_at(
    connection: &Connection,
    subject: &str,
    winner: Option<&str>,
    at_index: Option<u64>,
) -> Result<Vec<String>> {
    let at_index = at_index.unwrap_or(i64::MAX as u64);
    let mut statement = connection.prepare(
        "SELECT id, predecessors FROM claims WHERE subject=?1 AND kind='intent.desired' AND store_index<=?2 ORDER BY store_index",
    )?;
    let rows = statement
        .query_map(params![subject, at_index], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let referenced = rows
        .iter()
        .flat_map(|(_, predecessors)| {
            serde_json::from_str::<Vec<String>>(predecessors).unwrap_or_default()
        })
        .collect::<BTreeSet<_>>();
    Ok(rows
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| !referenced.contains(id) && Some(id.as_str()) != winner)
        .collect())
}

fn intent_leaves_tx(transaction: &Transaction<'_>, subject: &str) -> Result<Vec<String>> {
    let mut statement = transaction.prepare(
        "SELECT id, predecessors FROM claims WHERE subject=?1 AND kind='intent.desired' ORDER BY id",
    )?;
    let rows = statement
        .query_map([subject], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let referenced = rows
        .iter()
        .flat_map(|(_, predecessors)| {
            serde_json::from_str::<Vec<String>>(predecessors).unwrap_or_default()
        })
        .collect::<BTreeSet<_>>();
    Ok(rows
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| !referenced.contains(id))
        .collect())
}

fn mission_definition_token_tx(
    transaction: &Transaction<'_>,
    mission_id: &str,
) -> Result<Vec<String>> {
    Ok(transaction
        .query_row(
            "SELECT claim_id FROM mission_definitions WHERE mission_id=?1",
            [mission_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .into_iter()
        .collect())
}

fn intent_leaves_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Vec<String>> {
    let through = at_index.unwrap_or(i64::MAX as u64);
    let mut statement = connection.prepare(
        "SELECT id, predecessors FROM claims
         WHERE subject=?1 AND kind='intent.desired' AND store_index<=?2 ORDER BY id",
    )?;
    let rows = statement
        .query_map(params![subject, through], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let referenced = rows
        .iter()
        .flat_map(|(_, predecessors)| {
            serde_json::from_str::<Vec<String>>(predecessors).unwrap_or_default()
        })
        .collect::<BTreeSet<_>>();
    Ok(rows
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| !referenced.contains(id))
        .collect())
}

fn claim_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClaimRecord> {
    let body = row.get::<_, String>(7)?;
    let predecessors = row.get::<_, String>(8)?;
    let accepted = row.get::<_, String>(9)?;
    let body = serde_json::from_str(&body).unwrap_or(Value::Null);
    Ok(ClaimRecord {
        id: row.get(0)?,
        store_index: row.get(1)?,
        batch_id: row.get(2)?,
        subject: row.get(3)?,
        kind: row.get(4)?,
        origin: row.get(5)?,
        actor: row.get(6)?,
        operation_id: operation_parts(&body).map(|(id, _)| id.to_owned()),
        request_digest: operation_parts(&body).map(|(_, digest)| digest.to_owned()),
        body,
        predecessors: serde_json::from_str(&predecessors).unwrap_or_default(),
        accepted_at_unix_ms: accepted.parse().unwrap_or_default(),
    })
}

fn canonical_hash(value: &impl Serialize) -> Result<String> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn batch_header_hash(
    origin: &str,
    sequence: u64,
    previous_hash: Option<&str>,
    accepted_at_unix_ms: u128,
) -> Result<String> {
    canonical_hash(&(
        "st3.replica-batch.v1",
        origin,
        sequence,
        previous_hash,
        accepted_at_unix_ms.to_string(),
    ))
}

fn claim_hash(
    batch_id: &str,
    subject: &str,
    kind: &str,
    origin: &str,
    actor: Option<&str>,
    body: &Value,
    predecessors: &[String],
) -> Result<String> {
    canonical_hash(&(batch_id, subject, kind, origin, actor, body, predecessors))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn insert_mission_deadline_tx(
    transaction: &Transaction<'_>,
    run_id: &str,
    timeout_ms: Option<u64>,
    started_at_unix_ms: u128,
) -> Result<Option<u128>, St3Error> {
    let Some(timeout_ms) = timeout_ms else {
        return Ok(None);
    };
    let deadline_at_unix_ms = started_at_unix_ms.saturating_add(u128::from(timeout_ms));
    transaction
        .execute(
            "INSERT INTO mission_run_deadlines(run_id, timeout_ms, deadline_at_unix_ms)
             VALUES (?1, ?2, ?3)",
            params![run_id, timeout_ms, deadline_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    Ok(Some(deadline_at_unix_ms))
}

fn validate_mission_run_timeout(mission: &MissionSpec, mode: &str) -> Result<(), St3Error> {
    if mode != "eval" {
        return Ok(());
    }
    let timeout_ms = mission.timeout_ms.ok_or_else(|| {
        St3Error::new(
            "missing-eval-timeout",
            format!("eval entry mission `{}` needs a timeout", mission.id),
        )
    })?;
    if timeout_ms > MAX_EVAL_TIMEOUT_MS {
        return Err(St3Error::new(
            "eval-timeout-too-large",
            format!(
                "eval entry mission `{}` timeout exceeds the 20 minute limit",
                mission.id
            ),
        ));
    }
    Ok(())
}

fn internal(error: impl std::fmt::Display) -> St3Error {
    St3Error::new("internal", error.to_string())
}

fn claim_append_error(error: anyhow::Error) -> St3Error {
    match error.downcast::<St3Error>() {
        Ok(error) => error,
        Err(error) => internal(error),
    }
}

fn collect_referenced_blobs(
    connection: &Connection,
    claims: &[ClaimRecord],
    output: &mut BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let mut hashes = BTreeSet::new();
    for claim in claims {
        collect_hash_fields(&claim.body, &mut hashes);
    }
    for hash in hashes {
        if let Some(bytes) = connection
            .query_row("SELECT bytes FROM blobs WHERE hash=?1", [&hash], |row| {
                row.get(0)
            })
            .optional()?
        {
            output.insert(hash, bytes);
        }
    }
    Ok(())
}

fn collect_hash_fields(value: &Value, output: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if matches!(key.as_str(), "hash" | "blob_hash" | "bundle_hash")
                    && let Some(hash) = value.as_str().filter(|hash| hash.len() == 64)
                {
                    output.insert(hash.into());
                }
                collect_hash_fields(value, output);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_hash_fields(value, output);
            }
        }
        _ => {}
    }
}

fn canonical_child_string(value: &Value, name: &str) -> Option<String> {
    value
        .get("children")?
        .as_array()?
        .iter()
        .find(|child| child.get("name").and_then(Value::as_str) == Some(name))?
        .get("arguments")?
        .as_array()?
        .first()?
        .as_str()
        .map(str::to_owned)
}

fn canonical_child_strings(value: &Value, name: &str) -> Vec<String> {
    value
        .get("children")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|child| child.get("name").and_then(Value::as_str) == Some(name))
        .filter_map(|child| {
            child
                .get("arguments")
                .and_then(Value::as_array)
                .and_then(|arguments| arguments.first())
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn ensure_claim_blobs(transaction: &Transaction<'_>, claim: &ClaimRecord) -> Result<(), St3Error> {
    let mut hashes = BTreeSet::new();
    collect_hash_fields(&claim.body, &mut hashes);
    for hash in hashes {
        let exists = transaction
            .query_row("SELECT 1 FROM blobs WHERE hash=?1", [&hash], |_| Ok(()))
            .optional()
            .map_err(internal)?
            .is_some();
        if !exists {
            return Err(St3Error::new(
                "missing-replicated-blob",
                format!("claim `{}` references missing blob `{hash}`", claim.id),
            ));
        }
    }
    Ok(())
}

fn verify_replica_batch(batch: &ReplicaBatch) -> Result<(), St3Error> {
    let expected_batch = batch_header_hash(
        &batch.origin,
        batch.replica_sequence,
        batch.previous_hash.as_deref(),
        batch.accepted_at_unix_ms,
    )
    .map_err(internal)?;
    if expected_batch != batch.hash
        || batch.id
            != format!(
                "batch/{}/{}/{}",
                batch.origin, batch.replica_sequence, batch.hash
            )
    {
        return Err(St3Error::new(
            "batch-hash-mismatch",
            format!("replicated batch `{}` failed verification", batch.id),
        ));
    }
    for claim in &batch.claims {
        if claim.batch_id != batch.id || claim.origin != batch.origin {
            return Err(St3Error::new(
                "claim-batch-mismatch",
                format!(
                    "replicated claim `{}` names another batch or origin",
                    claim.id
                ),
            ));
        }
        let expected = claim_hash(
            &claim.batch_id,
            &claim.subject,
            &claim.kind,
            &claim.origin,
            claim.actor.as_deref(),
            &claim.body,
            &claim.predecessors,
        )
        .map_err(internal)?;
        if expected != claim.id {
            return Err(St3Error::new(
                "claim-hash-mismatch",
                format!("replicated claim `{}` failed verification", claim.id),
            ));
        }
        if known_replicated_claim_kind(&claim.kind) {
            let fields = schema_fields_for_body(&claim.kind, &claim.body).map_err(|error| {
                St3Error::new(
                    "invalid-replicated-claim",
                    format!(
                        "replicated claim `{}` has an invalid body: {error}",
                        claim.id
                    ),
                )
            })?;
            st3_schema::registry()
                .validate_claim(&claim.subject, &claim.kind, &fields)
                .map_err(|error| {
                    St3Error::new(
                        "invalid-replicated-claim",
                        format!(
                            "replicated claim `{}` violates {}: {}",
                            claim.id, error.code, error.message
                        ),
                    )
                })?;
        }
    }
    Ok(())
}

fn select_replicated_desired(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
    desired: &DesiredSubject,
) -> Result<(), St3Error> {
    let current = current_desired_row_tx(transaction, &claim.subject).map_err(internal)?;
    let revision = desired_revision(desired);
    let select = if let Some(row) = &current {
        if claim_descends_from(transaction, &claim.id, &row.claim_id).map_err(internal)? {
            true
        } else if claim_descends_from(transaction, &row.claim_id, &claim.id).map_err(internal)? {
            false
        } else {
            (revision.as_str(), claim.id.as_str()) > (row.revision.as_str(), row.claim_id.as_str())
        }
    } else {
        true
    };
    if !select {
        return Ok(());
    }
    transaction
        .execute(
            "INSERT INTO desired(subject, kind, revision, claim_id, body, member, owner_run, owner_generation, owner_step) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(subject) DO UPDATE SET kind=excluded.kind, revision=excluded.revision, claim_id=excluded.claim_id, body=excluded.body, member=excluded.member, owner_run=excluded.owner_run, owner_generation=excluded.owner_generation, owner_step=excluded.owner_step",
            params![
                claim.subject,
                desired.kind,
                revision,
                claim.id,
                serde_json::to_string(&desired.desired).map_err(internal)?,
                desired.member.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                desired.owner_run,
                desired.owner_generation,
                desired.owner_step,
            ],
        )
        .map_err(internal)?;
    Ok(())
}

fn project_replicated_mission_runs(transaction: &Transaction<'_>) -> Result<(), St3Error> {
    let mut statement = transaction
        .prepare(
            "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
             FROM claims
             WHERE kind IN ('mission-run.created','mission-run.state','run-generation.created','run-generation.state','run-generation.superseded',
                            'revision-proposal.created','revision-proposal.approved','revision-proposal.cancelled','revision-proposal.applied',
                            'step-run.carried','step-run.state','step-run.retried',
                            'work.claimed','work.renewed','work.progress','work.submitted','work.failed','work.released')
             ORDER BY store_index",
        )
        .map_err(internal)?;
    let claims = statement
        .query_map([], claim_from_row)
        .map_err(internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal)?;
    drop(statement);

    for claim in claims
        .iter()
        .filter(|claim| claim.kind == "mission-run.created")
    {
        project_mission_run_created(transaction, claim)?;
    }
    for claim in claims
        .iter()
        .filter(|claim| claim.kind != "mission-run.created")
    {
        project_mission_run_update(transaction, claim)?;
    }
    Ok(())
}

fn project_mission_run_created(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
) -> Result<(), St3Error> {
    let fields = claim.body.get("fields").unwrap_or(&claim.body);
    let run_id = claim.subject.strip_prefix("mission-run/").ok_or_else(|| {
        St3Error::new(
            "invalid-mission-run-claim",
            format!("claim `{}` has an invalid mission run subject", claim.id),
        )
    })?;
    let mission_subject = fields
        .get("mission")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-mission-run-claim",
                format!("claim `{}` has no mission", claim.id),
            )
        })?;
    let mission_id = mission_subject
        .strip_prefix("mission/")
        .unwrap_or(mission_subject);
    let revision = fields
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-mission-run-claim",
                format!("claim `{}` has no mission revision", claim.id),
            )
        })?;
    let mission_body = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![mission_id, revision],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(internal)?;
    let Some(mission_body) = mission_body else {
        return Ok(());
    };
    let mission = serde_json::from_str::<MissionSpec>(&mission_body).map_err(internal)?;
    let workspace = fields
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or(".");
    let requester = fields
        .get("requester")
        .and_then(Value::as_str)
        .unwrap_or("person/requester");
    let inputs = fields
        .get("inputs")
        .cloned()
        .map(serde_json::from_value::<BTreeMap<String, MissionRunInput>>)
        .transpose()
        .map_err(internal)?
        .unwrap_or_default();
    let mode = fields.get("mode").and_then(Value::as_str).unwrap_or("run");
    let root_revision = fields
        .get("root_revision")
        .and_then(Value::as_str)
        .unwrap_or(revision);
    let root_mission_run = fields
        .get("root_mission_run")
        .and_then(Value::as_str)
        .unwrap_or(&claim.subject);
    let root_run_id = root_mission_run
        .strip_prefix("mission-run/")
        .unwrap_or(root_mission_run);
    let parent_step_run = fields.get("parent_step_run").and_then(Value::as_str);
    let default_selector = fields
        .get("default_selector")
        .filter(|value| !value.is_null())
        .cloned()
        .map(serde_json::from_value::<WorkSelector>)
        .transpose()
        .map_err(internal)?;
    let generation_subject = fields
        .get("current_generation")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-mission-run-claim",
                format!("claim `{}` has no initial generation", claim.id),
            )
        })?;
    let generation_id = generation_id_from_subject(generation_subject);
    transaction
        .execute(
            "INSERT OR IGNORE INTO mission_runs(id, mission_id, initial_revision, current_generation_id, root_revision, root_run_id, parent_step_run, workspace, requester, inputs, mode, status, phase, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'running', 'normal', ?12, ?12)",
            params![run_id, mission_id, revision, generation_id, root_revision, root_run_id, parent_step_run, workspace, requester, serde_json::to_string(&inputs).map_err(internal)?, mode, claim.accepted_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    if let (Some(timeout_ms), Some(deadline_at_unix_ms)) = (
        fields.get("timeout_ms").and_then(Value::as_u64),
        fields.get("deadline_at_unix_ms").and_then(Value::as_u64),
    ) {
        transaction
            .execute(
                "INSERT OR IGNORE INTO mission_run_deadlines(run_id, timeout_ms, deadline_at_unix_ms)
                 VALUES (?1, ?2, ?3)",
                params![run_id, timeout_ms, deadline_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
    }
    transaction
        .execute(
            "INSERT OR IGNORE INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, NULL, 'running', ?4, 'initial mission run', ?5, ?5)",
            params![generation_id, run_id, revision, requester, claim.accepted_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    let view = mission_run_view_tx(transaction, run_id).map_err(internal)?;
    let variables = mission_run_variables(&view, revision);
    let mut steps = Vec::new();
    flatten_steps(&mission, default_selector, &[], &mut steps);
    for (step, selector, constraints) in steps {
        let (assignee, available_to, agentless) = interpolate_selector(&selector, &variables)?;
        let subject = format!("step-run/{generation_id}/{}", step.path);
        let mut step_variables = variables.clone();
        step_variables.insert("ST_STEP".into(), step.path.clone());
        step_variables.insert("ST_STEP_RUN".into(), subject.clone());
        step_variables.insert("ST_ATTEMPT".into(), "1".into());
        step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
        step_variables.insert(
            "ST_PARENT_STEP_RUN".into(),
            crate::mission::parent_step_path(&mission, &step.path)
                .map(|path| format!("step-run/{generation_id}/{path}"))
                .or_else(|| parent_step_run.map(str::to_owned))
                .unwrap_or_default(),
        );
        let title = step
            .title
            .as_deref()
            .map(|value| crate::mission::interpolate(value, &step_variables))
            .transpose()?;
        let goals = interpolate_goals(&step.goals, &step_variables)?;
        let constraints = interpolate_goals(&constraints, &step_variables)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, created_at_unix_ms, updated_at_unix_ms, constraints)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 1, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?12)",
                params![subject, run_id, generation_id, step.path, step.definition_hash, assignee, serde_json::to_string(&available_to).map_err(internal)?, agentless, title, goals, claim.accepted_at_unix_ms.to_string(), constraints],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn project_mission_run_update(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
) -> Result<(), St3Error> {
    let fields = claim.body.get("fields").unwrap_or(&claim.body);
    if claim.kind.starts_with("revision-proposal.") {
        project_revision_proposal(transaction, claim, fields)?;
        return Ok(());
    }
    if claim.kind == "run-generation.created" {
        project_run_generation_created(transaction, claim)?;
        return Ok(());
    }
    if claim.kind == "run-generation.superseded" || claim.kind == "run-generation.state" {
        let Some(generation_id) = claim.subject.strip_prefix("run-generation/") else {
            return Ok(());
        };
        let status = fields
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("superseded");
        transaction
            .execute(
                "UPDATE run_generations SET status=?2, updated_at_unix_ms=?3 WHERE id=?1",
                params![generation_id, status, claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
        return Ok(());
    }
    if claim.kind == "mission-run.state" {
        let Some(run_id) = claim.subject.strip_prefix("mission-run/") else {
            return Ok(());
        };
        let Some(status) = fields.get("status").and_then(Value::as_str) else {
            return Ok(());
        };
        let phase = fields
            .get("phase")
            .and_then(Value::as_str)
            .unwrap_or("normal");
        transaction
            .execute(
                "UPDATE mission_runs SET status=?2, phase=?3, updated_at_unix_ms=?4 WHERE id=?1",
                params![run_id, status, phase, claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
        return Ok(());
    }
    if !claim.subject.starts_with("step-run/") {
        return Ok(());
    }
    if claim.kind == "step-run.retried" {
        let Some(attempt) = fields.get("attempt").and_then(Value::as_u64) else {
            return Ok(());
        };
        let not_before = fields
            .get("not_before_unix_ms")
            .and_then(Value::as_u64)
            .map(|value| value.to_string());
        transaction
            .execute(
                "UPDATE step_runs SET status='pending', attempt=?2, worker_reported=0, lease_owner=NULL,
                        lease_incarnation=NULL, lease_expires_at_unix_ms=NULL, blocked_reason=?3,
                        not_before_unix_ms=?4, activated_at_unix_ms=NULL, updated_at_unix_ms=?5 WHERE subject=?1",
                params![claim.subject, attempt, fields.get("reason").and_then(Value::as_str), not_before, claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
        return Ok(());
    }
    let Some(status) = fields.get("status").and_then(Value::as_str) else {
        return Ok(());
    };
    if claim.kind == "step-run.state" {
        transaction
            .execute(
                "UPDATE step_runs SET status=?2, blocked_reason=?3,
                        lease_owner=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_owner END,
                        lease_incarnation=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_incarnation END,
                        lease_expires_at_unix_ms=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_expires_at_unix_ms END,
                        not_before_unix_ms=CASE WHEN ?2='ready' THEN NULL ELSE not_before_unix_ms END,
                        activated_at_unix_ms=CASE WHEN ?2='ready' THEN ?4 ELSE activated_at_unix_ms END,
                        readiness_epoch=COALESCE(?5, readiness_epoch), updated_at_unix_ms=?4 WHERE subject=?1",
                params![claim.subject, status, fields.get("reason").and_then(Value::as_str), claim.accepted_at_unix_ms.to_string(), fields.get("readiness_epoch").and_then(Value::as_u64)],
            )
            .map_err(internal)?;
        return Ok(());
    }
    if claim.kind.starts_with("work.") {
        let lease_expiry = fields
            .get("claim_expires_at_unix_ms")
            .and_then(Value::as_u64)
            .map(|value| value.to_string());
        transaction
            .execute(
                "UPDATE step_runs SET status=?2, worker_reported=?3, lease_owner=?4,
                        lease_incarnation=?5, lease_expires_at_unix_ms=?6, blocked_reason=?7,
                        readiness_epoch=COALESCE(?8, readiness_epoch), updated_at_unix_ms=?9 WHERE subject=?1",
                params![
                    claim.subject,
                    status,
                    fields
                        .get("worker_reported")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    fields.get("claimant").and_then(Value::as_str),
                    fields.get("claim_incarnation").and_then(Value::as_str),
                    lease_expiry,
                    fields.get("reason").and_then(Value::as_str),
                    fields.get("readiness_epoch").and_then(Value::as_u64),
                    claim.accepted_at_unix_ms.to_string(),
                ],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn project_revision_proposal(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
    fields: &Value,
) -> Result<(), St3Error> {
    let Some(proposal_id) = claim.subject.strip_prefix("revision-proposal/") else {
        return Ok(());
    };
    let now = claim.accepted_at_unix_ms.to_string();
    if claim.kind == "revision-proposal.created" {
        let Some(run) = fields.get("run").and_then(Value::as_str) else {
            return Ok(());
        };
        let Some(source) = fields.get("source_generation").and_then(Value::as_str) else {
            return Ok(());
        };
        let Some(revision) = fields.get("candidate_revision").and_then(Value::as_str) else {
            return Ok(());
        };
        let actor = claim.actor.as_deref().unwrap_or("requester");
        let reason = fields
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let status = fields
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("pending-approval");
        let cutover = fields
            .get("cutover")
            .and_then(Value::as_str)
            .unwrap_or("restart-active");
        let compatible = fields
            .get("compatible_steps")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let reviewers = fields
            .get("reviewers")
            .cloned()
            .unwrap_or_else(|| json!([]));
        transaction
            .execute(
                "INSERT OR IGNORE INTO revision_proposals(
                   id, run_id, source_generation_id, candidate_revision, actor, reason, status,
                   cutover, compatible_steps, reviewers, approvals, preview_hash,
                   created_at_unix_ms, updated_at_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, '[]', ?11, ?12, ?12)",
                params![
                    proposal_id,
                    run.strip_prefix("mission-run/").unwrap_or(run),
                    generation_id_from_subject(source),
                    revision,
                    actor,
                    reason,
                    status,
                    cutover,
                    serde_json::to_string(&compatible).map_err(internal)?,
                    serde_json::to_string(&reviewers).map_err(internal)?,
                    fields.get("preview_hash").and_then(Value::as_str),
                    now,
                ],
            )
            .map_err(internal)?;
        if status == "draining" {
            transaction
                .execute(
                    "UPDATE mission_runs SET phase='revision-draining', updated_at_unix_ms=?2 WHERE id=?1",
                    params![run.strip_prefix("mission-run/").unwrap_or(run), now],
                )
                .map_err(internal)?;
        }
        return Ok(());
    }
    if claim.kind == "revision-proposal.approved" {
        let Some(reviewer) = fields.get("reviewer").and_then(Value::as_str) else {
            return Ok(());
        };
        let mut approvals = transaction
            .query_row(
                "SELECT approvals FROM revision_proposals WHERE id=?1",
                [proposal_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
            .map(|value| serde_json::from_str::<BTreeSet<String>>(&value))
            .transpose()
            .map_err(internal)?
            .unwrap_or_default();
        approvals.insert(reviewer.to_owned());
        let draining = fields
            .get("all_approved")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && transaction
                .query_row(
                    "SELECT cutover='when-idle' FROM revision_proposals WHERE id=?1",
                    [proposal_id],
                    |row| row.get::<_, bool>(0),
                )
                .optional()
                .map_err(internal)?
                .unwrap_or(false);
        transaction
            .execute(
                "UPDATE revision_proposals SET approvals=?2,
                   status=CASE WHEN ?3 THEN 'draining' ELSE status END,
                   updated_at_unix_ms=?4 WHERE id=?1",
                params![
                    proposal_id,
                    serde_json::to_string(&approvals).map_err(internal)?,
                    draining,
                    now
                ],
            )
            .map_err(internal)?;
        if draining {
            transaction
                .execute(
                    "UPDATE mission_runs SET phase='revision-draining', updated_at_unix_ms=?2
                     WHERE id=(SELECT run_id FROM revision_proposals WHERE id=?1)",
                    params![proposal_id, now],
                )
                .map_err(internal)?;
        }
        return Ok(());
    }
    let (status, successor) = match claim.kind.as_str() {
        "revision-proposal.cancelled" => ("cancelled", None),
        "revision-proposal.applied" => (
            "applied",
            fields
                .get("successor_generation")
                .and_then(Value::as_str)
                .map(generation_id_from_subject),
        ),
        _ => return Ok(()),
    };
    transaction
        .execute(
            "UPDATE revision_proposals SET status=?2, successor_generation_id=?3,
               updated_at_unix_ms=?4 WHERE id=?1",
            params![proposal_id, status, successor, now],
        )
        .map_err(internal)?;
    if status == "cancelled" {
        transaction
            .execute(
                "UPDATE mission_runs SET phase='normal', updated_at_unix_ms=?2
                 WHERE id=(SELECT run_id FROM revision_proposals WHERE id=?1)",
                params![proposal_id, now],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn project_run_generation_created(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
) -> Result<(), St3Error> {
    let Some(generation_id) = claim.subject.strip_prefix("run-generation/") else {
        return Ok(());
    };
    if transaction
        .query_row(
            "SELECT 1 FROM run_generations WHERE id=?1",
            [generation_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(internal)?
        .is_some()
    {
        return Ok(());
    }
    let fields = claim.body.get("fields").unwrap_or(&claim.body);
    let Some(run_subject) = fields.get("run").and_then(Value::as_str) else {
        return Ok(());
    };
    let run_id = run_subject
        .strip_prefix("mission-run/")
        .unwrap_or(run_subject);
    let Some(revision) = fields.get("revision").and_then(Value::as_str) else {
        return Ok(());
    };
    let predecessor = fields
        .get("predecessor")
        .and_then(Value::as_str)
        .map(generation_id_from_subject);
    let reason = fields
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("replicated generation");
    let current = match mission_run_view_tx(transaction, run_id) {
        Ok(current) => current,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(()),
        Err(error) => return Err(internal(error)),
    };
    let mission_id = current
        .mission
        .strip_prefix("mission/")
        .unwrap_or(&current.mission);
    let body = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![mission_id, revision],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(internal)?;
    let Some(body) = body else {
        return Ok(());
    };
    let mission = serde_json::from_str::<MissionSpec>(&body).map_err(internal)?;
    let mut variables = mission_run_variables(&current, revision);
    variables.insert("ST_RUN_GENERATION".into(), generation_id.to_owned());
    let compatible = fields
        .get("compatible_steps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    transaction
        .execute(
            "INSERT INTO run_generations(id, run_id, revision, predecessor_id, status, actor, reason, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?6, ?7, ?7)",
            params![generation_id, run_id, revision, predecessor, claim.actor, reason, claim.accepted_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    let mut new_steps = Vec::new();
    flatten_steps(&mission, None, &[], &mut new_steps);
    for (step, selector, constraints) in new_steps {
        let (assignee, available_to, agentless) = interpolate_selector(&selector, &variables)?;
        let subject = format!("step-run/{generation_id}/{}", step.path);
        let carried = compatible
            .contains(step.path.as_str())
            .then(|| current.steps.iter().find(|old| old.step == step.path))
            .flatten();
        let status = carried
            .map(|old| match old.status.as_str() {
                "claimed" | "working" | "verifying" => "ready",
                status => status,
            })
            .unwrap_or("pending");
        let attempt = carried.map(|old| old.attempt).unwrap_or(1);
        let worker_reported = carried.is_some_and(|old| old.worker_reported && status != "ready");
        let mut step_variables = variables.clone();
        step_variables.insert("ST_STEP".into(), step.path.clone());
        step_variables.insert("ST_STEP_RUN".into(), subject.clone());
        step_variables.insert("ST_ATTEMPT".into(), attempt.to_string());
        step_variables.insert("ST_ASSIGNEE".into(), assignee.clone().unwrap_or_default());
        step_variables.insert(
            "ST_PARENT_STEP_RUN".into(),
            crate::mission::parent_step_path(&mission, &step.path)
                .map(|path| format!("step-run/{generation_id}/{path}"))
                .or_else(|| current.parent_step_run.clone())
                .unwrap_or_default(),
        );
        let title = step
            .title
            .as_deref()
            .map(|value| crate::mission::interpolate(value, &step_variables))
            .transpose()?;
        let goals = interpolate_goals(&step.goals, &step_variables)?;
        let constraints = interpolate_goals(&constraints, &step_variables)?;
        transaction
            .execute(
                "INSERT INTO step_runs(subject, run_id, generation_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported, blocked_reason, not_before_unix_ms, readiness_epoch, created_at_unix_ms, updated_at_unix_ms, constraints)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?17, ?18)",
                params![subject, run_id, generation_id, step.path, step.definition_hash, status, attempt, assignee, serde_json::to_string(&available_to).map_err(internal)?, agentless, title, goals, worker_reported, carried.and_then(|old| old.blocked_reason.as_deref()), carried.and_then(|old| old.not_before_unix_ms).map(|value| value.to_string()), carried.map(|old| old.readiness_epoch).unwrap_or(0), claim.accepted_at_unix_ms.to_string(), constraints],
            )
            .map_err(internal)?;
    }
    transaction
        .execute(
            "UPDATE mission_runs SET current_generation_id=?2, status='running', phase='normal', updated_at_unix_ms=?3 WHERE id=?1",
            params![run_id, generation_id, claim.accepted_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    Ok(())
}

fn claim_descends_from(
    transaction: &Transaction<'_>,
    descendant: &str,
    ancestor: &str,
) -> Result<bool> {
    if descendant == ancestor {
        return Ok(true);
    }
    let mut pending = vec![descendant.to_owned()];
    let mut seen = BTreeSet::new();
    while let Some(claim_id) = pending.pop() {
        if !seen.insert(claim_id.clone()) {
            continue;
        }
        let predecessors = transaction
            .query_row(
                "SELECT predecessors FROM claims WHERE id=?1",
                [&claim_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(predecessors) = predecessors else {
            continue;
        };
        for predecessor in serde_json::from_str::<Vec<String>>(&predecessors)? {
            if predecessor == ancestor {
                return Ok(true);
            }
            pending.push(predecessor);
        }
    }
    Ok(false)
}

fn select_replicated_document(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
    created_index: u64,
) -> Result<(), St3Error> {
    let name = claim
        .body
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-document-claim",
                format!("replicated document claim `{}` has no name", claim.id),
            )
        })?;
    let hash = claim
        .body
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-document-claim",
                format!("replicated document claim `{}` has no hash", claim.id),
            )
        })?;
    validate_document_name(name)?;
    transaction
        .execute(
            "INSERT OR IGNORE INTO documents(name, hash, created_index, binding_claim_id) VALUES (?1, ?2, ?3, ?4)",
            params![name, hash, created_index, claim.id],
        )
        .map_err(internal)?;
    Ok(())
}

fn select_replicated_mission(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
    created_index: u64,
) -> Result<(), St3Error> {
    let mission = serde_json::from_value::<MissionSpec>(claim.body.clone()).map_err(|error| {
        St3Error::new(
            "invalid-mission-claim",
            format!(
                "replicated mission claim `{}` is invalid: {error}",
                claim.id
            ),
        )
    })?;
    if claim.subject != mission.subject || claim.subject != format!("mission/{}", mission.id) {
        return Err(St3Error::new(
            "invalid-mission-claim",
            format!(
                "replicated mission claim `{}` has mismatched identity",
                claim.id
            ),
        ));
    }
    let mut unhashed = mission.clone();
    let revision = std::mem::take(&mut unhashed.revision);
    let expected = hex::encode(Sha256::digest(
        serde_json::to_vec(&unhashed).map_err(internal)?,
    ));
    if revision != expected {
        return Err(St3Error::new(
            "invalid-mission-claim",
            format!(
                "replicated mission claim `{}` has an invalid revision",
                claim.id
            ),
        ));
    }
    transaction
        .execute(
            "INSERT OR IGNORE INTO mission_revisions(mission_id, revision, state, body, claim_id, created_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![mission.id, mission.revision, mission_state_name(&mission.state), serde_json::to_string(&mission).map_err(internal)?, claim.id, created_index],
        )
        .map_err(internal)?;
    let current: Option<(String, String)> = transaction
        .query_row(
            "SELECT revision, claim_id FROM mission_definitions WHERE mission_id=?1",
            [&mission.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(internal)?;
    let select = if let Some((current_revision, current_claim)) = current {
        if claim_descends_from(transaction, &claim.id, &current_claim).map_err(internal)? {
            true
        } else if claim_descends_from(transaction, &current_claim, &claim.id).map_err(internal)? {
            false
        } else {
            (mission.revision.as_str(), claim.id.as_str())
                > (current_revision.as_str(), current_claim.as_str())
        }
    } else {
        true
    };
    if select {
        transaction
            .execute(
                "INSERT INTO mission_definitions(mission_id, revision, state, claim_id) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(mission_id) DO UPDATE SET revision=excluded.revision, state=excluded.state, claim_id=excluded.claim_id",
                params![mission.id, mission.revision, mission_state_name(&mission.state), claim.id],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn mission_state_name(state: &MissionState) -> &'static str {
    match state {
        MissionState::Draft => "draft",
        MissionState::Ready => "ready",
        MissionState::Retired => "retired",
    }
}

fn normalize_actor(value: &str, default_kind: &str) -> String {
    if value.contains('/') {
        value.to_owned()
    } else {
        format!("{default_kind}/{value}")
    }
}

fn normalize_step_run(value: &str) -> String {
    if value.starts_with("step-run/") {
        value.to_owned()
    } else {
        format!("step-run/{value}")
    }
}

fn flatten_steps<'a>(
    mission: &'a MissionSpec,
    inherited_selector: Option<WorkSelector>,
    inherited_constraints: &[String],
    output: &mut Vec<(&'a crate::model::StepSpec, WorkSelector, Vec<String>)>,
) {
    let mut mission_constraints = inherited_constraints.to_vec();
    mission_constraints.extend(mission.constraints.clone());
    let mission_selector = mission
        .work_selector
        .clone()
        .or(inherited_selector)
        .unwrap_or(WorkSelector::Agentless);
    for id in &mission.display_order {
        let step = &mission.steps[id];
        let selector = step
            .work_selector
            .clone()
            .unwrap_or_else(|| mission_selector.clone());
        let mut constraints = mission_constraints.clone();
        constraints.extend(step.constraints.clone());
        output.push((step, selector.clone(), constraints.clone()));
        if let Some(nested) = &step.nested_mission {
            flatten_steps(nested, Some(selector), &constraints, output);
        }
    }
}

fn flatten_mission_step_specs(mission: &MissionSpec) -> Vec<&crate::model::StepSpec> {
    fn append<'a>(mission: &'a MissionSpec, output: &mut Vec<&'a crate::model::StepSpec>) {
        for id in &mission.display_order {
            let step = &mission.steps[id];
            output.push(step);
            if let Some(nested) = &step.nested_mission {
                append(nested, output);
            }
        }
    }
    let mut output = Vec::new();
    append(mission, &mut output);
    output
}

fn interpolate_selector(
    selector: &WorkSelector,
    variables: &BTreeMap<String, String>,
) -> Result<(Option<String>, Vec<String>, bool), St3Error> {
    match selector {
        WorkSelector::Assigned { agent } => Ok((
            Some(crate::mission::interpolate(agent, variables)?),
            Vec::new(),
            false,
        )),
        WorkSelector::Available { agents } => {
            let mut output = agents
                .iter()
                .map(|agent| crate::mission::interpolate(agent, variables))
                .collect::<Result<Vec<_>, _>>()?;
            output.sort();
            output.dedup();
            Ok((None, output, false))
        }
        WorkSelector::Agentless => Ok((None, Vec::new(), true)),
    }
}

pub(crate) fn mission_run_variables(
    run: &MissionRunView,
    revision: &str,
) -> BTreeMap<String, String> {
    let mut variables = BTreeMap::from([
        (
            "ST_MISSION".into(),
            run.mission
                .strip_prefix("mission/")
                .unwrap_or(&run.mission)
                .into(),
        ),
        ("ST_MISSION_REVISION".into(), revision.into()),
        ("ST_MISSION_RUN".into(), run.id.clone()),
        (
            "ST_RUN_GENERATION".into(),
            generation_id_from_subject(&run.generation).into(),
        ),
        ("ST_WORKSPACE".into(), run.workspace.clone()),
        ("ST_REQUESTER".into(), run.requester.clone()),
        ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
        (
            "ST_PARENT_STEP_RUN".into(),
            run.parent_step_run.clone().unwrap_or_default(),
        ),
        ("ST_ROOT_MISSION_RUN".into(), run.root_mission_run.clone()),
    ]);
    variables.extend(
        run.inputs
            .iter()
            .map(|(name, input)| (format!("input.{name}"), input.value.clone())),
    );
    variables
}

fn interpolate_goals(
    goals: &[String],
    variables: &BTreeMap<String, String>,
) -> Result<String, St3Error> {
    let goals = goals
        .iter()
        .map(|goal| crate::mission::interpolate(goal, variables))
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&goals).map_err(internal)
}

pub(crate) fn analyze_mission_revision(
    old: &MissionSpec,
    new: &MissionSpec,
    actor: &str,
    requester: &str,
    variables: &BTreeMap<String, String>,
) -> Result<(BTreeSet<String>, BTreeSet<String>), St3Error> {
    let old_hashes = step_hashes(old);
    let new_hashes = step_hashes(new);
    let mut changed = old_hashes
        .keys()
        .chain(new_hashes.keys())
        .filter(|path| old_hashes.get(*path) != new_hashes.get(*path))
        .cloned()
        .collect::<BTreeSet<_>>();
    if mission_header_hash(old)? != mission_header_hash(new)? {
        changed.insert(String::new());
    }
    if changed.is_empty() {
        return Err(St3Error::new(
            "unchanged-mission-revision",
            "the proposed mission revision does not change the mission",
        ));
    }

    let actor = normalize_actor(
        actor,
        if actor.starts_with("person/") {
            "person"
        } else {
            "agent"
        },
    );
    let requester = normalize_actor(requester, "person");
    let metadata = revision_metadata(old, &requester, variables)?;
    if actor != requester {
        for path in &changed {
            let meta = metadata_for_changed_path(&metadata, path);
            if !meta.owners.contains(&actor) {
                return Err(St3Error::new(
                    "revision-outside-graph-location",
                    format!(
                        "`{actor}` cannot revise `{}` from its current graph location",
                        if path.is_empty() {
                            old.id.as_str()
                        } else {
                            path
                        }
                    ),
                ));
            }
        }
    }
    let mut reviewers = BTreeSet::new();
    for path in &changed {
        reviewers.extend(
            metadata_for_changed_path(&metadata, path)
                .reviewers
                .iter()
                .cloned(),
        );
    }
    Ok((compatible_step_paths(old, new), reviewers))
}

#[derive(Clone, Default)]
struct RevisionMetadata {
    owners: BTreeSet<String>,
    reviewers: BTreeSet<String>,
}

fn revision_metadata(
    mission: &MissionSpec,
    requester: &str,
    variables: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, RevisionMetadata>, St3Error> {
    fn collect(
        mission: &MissionSpec,
        requester: &str,
        variables: &BTreeMap<String, String>,
        inherited: RevisionMetadata,
        output: &mut BTreeMap<String, RevisionMetadata>,
    ) -> Result<(), St3Error> {
        let mut mission_meta = inherited;
        for owner in &mission.revision_owners {
            mission_meta
                .owners
                .insert(crate::mission::interpolate(owner, variables)?);
        }
        if mission.revisions_human_only {
            mission_meta.reviewers.insert(
                mission
                    .revision_reviewer
                    .as_deref()
                    .unwrap_or(requester)
                    .to_owned(),
            );
        }
        output.insert(String::new(), mission_meta.clone());
        for id in &mission.display_order {
            let step = &mission.steps[id];
            let mut step_meta = mission_meta.clone();
            for owner in &step.revision_owners {
                step_meta
                    .owners
                    .insert(crate::mission::interpolate(owner, variables)?);
            }
            if step.revisions_human_only {
                step_meta.reviewers.insert(
                    step.revision_reviewer
                        .as_deref()
                        .unwrap_or(requester)
                        .to_owned(),
                );
            }
            output.insert(step.path.clone(), step_meta.clone());
            if let Some(nested) = &step.nested_mission {
                let mut nested_meta = BTreeMap::new();
                collect(nested, requester, variables, step_meta, &mut nested_meta)?;
                for (path, meta) in nested_meta {
                    if !path.is_empty() {
                        output.insert(path, meta);
                    }
                }
            }
        }
        Ok(())
    }
    let mut output = BTreeMap::new();
    collect(
        mission,
        requester,
        variables,
        RevisionMetadata::default(),
        &mut output,
    )?;
    Ok(output)
}

fn metadata_for_changed_path<'a>(
    metadata: &'a BTreeMap<String, RevisionMetadata>,
    path: &str,
) -> &'a RevisionMetadata {
    metadata
        .iter()
        .filter(|(candidate, _)| {
            candidate.is_empty()
                || path == candidate.as_str()
                || path.starts_with(&format!("{candidate}/"))
        })
        .max_by_key(|(candidate, _)| candidate.len())
        .map(|(_, meta)| meta)
        .expect("the mission metadata is always present")
}

fn step_hashes(mission: &MissionSpec) -> BTreeMap<String, String> {
    let mut flat = Vec::new();
    flatten_steps(mission, None, &[], &mut flat);
    flat.into_iter()
        .map(|(step, selector, constraints)| {
            let bytes = serde_json::to_vec(&(step.definition_hash.as_str(), selector, constraints))
                .expect("a step compatibility value serializes");
            (step.path.clone(), hex::encode(Sha256::digest(bytes)))
        })
        .collect()
}

fn mission_header_hash(mission: &MissionSpec) -> Result<String, St3Error> {
    let value = json!({
        "id": mission.id,
        "state": mission.state,
        "inputs": mission.inputs,
        "max_active_runs": mission.max_active_runs,
        "owners": mission.revision_owners,
        "human_only": mission.revisions_human_only,
        "reviewer": mission.revision_reviewer,
        "cutover": mission.revision_cutover,
        "declarations": mission.declarations_kdl,
        "work_selector": mission.work_selector,
        "completion": mission.completion,
        "goals": mission.goals,
        "constraints": mission.constraints,
        "baselines": mission.baselines,
        "products": mission.products,
        "gates": mission.gates,
    });
    serde_json::to_vec(&value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(internal)
}

fn compatible_step_paths(old: &MissionSpec, new: &MissionSpec) -> BTreeSet<String> {
    let old_hashes = step_hashes(old);
    let new_hashes = step_hashes(new);
    let dependencies = flattened_dependencies(new);
    let mut incompatible = new_hashes
        .iter()
        .filter(|(path, hash)| old_hashes.get(*path) != Some(*hash))
        .map(|(path, _)| path.clone())
        .collect::<BTreeSet<_>>();
    loop {
        let additions = dependencies
            .iter()
            .filter(|(path, dependencies)| {
                !incompatible.contains(*path)
                    && dependencies
                        .iter()
                        .any(|dependency| incompatible.contains(dependency))
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        if additions.is_empty() {
            break;
        }
        incompatible.extend(additions);
    }
    new_hashes
        .keys()
        .filter(|path| !incompatible.contains(*path))
        .cloned()
        .collect()
}

fn flattened_dependencies(mission: &MissionSpec) -> BTreeMap<String, BTreeSet<String>> {
    fn collect(
        mission: &MissionSpec,
        prefix: &str,
        parent: Option<&str>,
        output: &mut BTreeMap<String, BTreeSet<String>>,
    ) {
        for id in &mission.display_order {
            let step = &mission.steps[id];
            let mut dependencies = BTreeSet::new();
            if let Some(parent) = parent {
                dependencies.insert(parent.into());
            }
            for dependency in &step.dependencies {
                if let DependencySpec::Step { step: target, .. } = dependency {
                    dependencies.insert(if prefix.is_empty() {
                        target.clone()
                    } else {
                        format!("{prefix}/{target}")
                    });
                }
            }
            output.insert(step.path.clone(), dependencies);
            if let Some(nested) = &step.nested_mission {
                collect(
                    nested,
                    &format!("{}/{}", step.path, nested.id),
                    Some(&step.path),
                    output,
                );
            }
        }
    }
    let mut output = BTreeMap::new();
    collect(mission, "", None, &mut output);
    output
}

fn step_run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StepRunView> {
    let lease: Option<String> = row.get(14)?;
    let not_before: Option<String> = row.get(16)?;
    let created: String = row.get(17)?;
    let updated: String = row.get(18)?;
    let subject: String = row.get(0)?;
    let generation = subject
        .split('/')
        .nth(1)
        .map(|id| format!("run-generation/{id}"))
        .unwrap_or_default();
    Ok(StepRunView {
        subject,
        run: format!("mission-run/{}", row.get::<_, String>(1)?),
        generation,
        step: row.get(2)?,
        definition_hash: row.get(3)?,
        status: row.get(4)?,
        attempt: row.get::<_, u32>(5)?,
        assigned_to: row.get(6)?,
        available_to: serde_json::from_str(&row.get::<_, String>(7)?).unwrap_or_default(),
        agentless: row.get(8)?,
        title: row.get(9)?,
        goals: serde_json::from_str(&row.get::<_, String>(10)?).unwrap_or_default(),
        constraints: serde_json::from_str(&row.get::<_, String>(20)?).unwrap_or_default(),
        under: Vec::new(),
        worker_reported: row.get::<_, bool>(11)?,
        claimant: row.get(12)?,
        claim_incarnation: row.get(13)?,
        claim_expires_at_unix_ms: lease.and_then(|value| value.parse().ok()),
        readiness_epoch: row.get(19)?,
        blocked_reason: row.get(15)?,
        not_before_unix_ms: not_before.and_then(|value| value.parse().ok()),
        created_at_unix_ms: created.parse().unwrap_or(0),
        updated_at_unix_ms: updated.parse().unwrap_or(0),
    })
}

fn planning_session_view_tx(
    connection: &Connection,
    id: &str,
) -> Result<Option<PlanningSessionView>> {
    let row = connection
        .query_row(
            "SELECT mission_id, request_ref, workspace, requester, planner, status, published_revision,
                    target_run_id, source_generation_id, created_at_unix_ms, updated_at_unix_ms
             FROM planning_sessions WHERE id=?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        mission,
        request_ref,
        workspace,
        requester,
        planner,
        status,
        published_revision,
        target_run_id,
        source_generation_id,
        created,
        updated,
    )) = row
    else {
        return Ok(None);
    };
    let mut candidate_statement = connection.prepare(
        "SELECT candidate.variant, candidate.revision, candidate.markdown_ref, candidate.kdl_ref,
                candidate.mission_revision, candidate.submitted_at_unix_ms
         FROM planning_candidates candidate
         JOIN (
           SELECT variant, MAX(revision) revision FROM planning_candidates
           WHERE session_id=?1 GROUP BY variant
         ) latest ON latest.variant=candidate.variant AND latest.revision=candidate.revision
         WHERE candidate.session_id=?1 ORDER BY candidate.variant",
    )?;
    let candidates = candidate_statement
        .query_map([id], |row| {
            let variant: String = row.get(0)?;
            let submitted: String = row.get(5)?;
            Ok(PlanningCandidateView {
                variant,
                revision: row.get(1)?,
                markdown: row.get(2)?,
                kdl: row.get(3)?,
                mission: format!("mission/{mission}"),
                mission_revision: row.get(4)?,
                submitted_at_unix_ms: submitted.parse().unwrap_or(0),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut variants = Vec::new();
    for candidate in candidates {
        let preview = connection
            .query_row(
                "SELECT candidate_revision, hash, store_index, graph, diff, mission_response, created_at_unix_ms
                 FROM planning_previews WHERE session_id=?1 AND variant=?2",
                params![id, candidate.variant],
                |row| {
                    let created: String = row.get(6)?;
                    Ok(PlanningPreviewView {
                        variant: candidate.variant.clone(),
                        candidate_revision: row.get(0)?,
                        hash: row.get(1)?,
                        store_index: row.get(2)?,
                        graph: row.get(3)?,
                        diff: row.get(4)?,
                        mission: serde_json::from_str(&row.get::<_, String>(5)?).unwrap(),
                        created_at_unix_ms: created.parse().unwrap_or(0),
                    })
                },
            )
            .optional()?;
        variants.push(PlanningVariantView {
            name: candidate.variant.clone(),
            candidate,
            preview,
        });
    }
    let selected = variants
        .iter()
        .find(|variant| variant.name == "default")
        .or_else(|| variants.first());
    let candidate = selected.map(|variant| variant.candidate.clone());
    let preview = selected.and_then(|variant| variant.preview.clone());
    Ok(Some(PlanningSessionView {
        subject: format!("planning-session/{id}"),
        id: id.to_owned(),
        mission,
        request: request_ref,
        workspace,
        requester,
        planner,
        status,
        target_mission_run: target_run_id.map(|id| format!("mission-run/{id}")),
        source_generation: source_generation_id.map(|id| format!("run-generation/{id}")),
        candidate,
        preview,
        variants,
        published_revision,
        created_at_unix_ms: created.parse().unwrap_or(0),
        updated_at_unix_ms: updated.parse().unwrap_or(0),
    }))
}

fn mission_output_authority(
    connection: &Connection,
    current: &StepRunView,
    actor: &str,
    incarnation: Option<&str>,
    now: u128,
) -> Result<bool> {
    let lease_matches = |status: &str,
                         owner: Option<&str>,
                         lease_incarnation: Option<&str>,
                         expiry: Option<u128>| {
        matches!(status, "claimed" | "working")
            && owner == Some(actor)
            && expiry.is_some_and(|expiry| expiry > now)
            && lease_incarnation.is_none_or(|lease| Some(lease) == incarnation)
    };
    if lease_matches(
        &current.status,
        current.claimant.as_deref(),
        current.claim_incarnation.as_deref(),
        current.claim_expires_at_unix_ms,
    ) {
        return Ok(true);
    }

    let prefix = format!("{}/", current.step);
    let mut statement = connection.prepare(
        "SELECT status, lease_owner, lease_incarnation, lease_expires_at_unix_ms
         FROM step_runs
         WHERE run_id=?1 AND substr(step_path, 1, length(?2))=?2",
    )?;
    let leases = statement
        .query_map(
            params![
                current
                    .run
                    .strip_prefix("mission-run/")
                    .unwrap_or(&current.run),
                prefix
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(leases.into_iter().any(|(status, owner, lease, expiry)| {
        lease_matches(
            &status,
            owner.as_deref(),
            lease.as_deref(),
            expiry.and_then(|value| value.parse().ok()),
        )
    }))
}

fn descendant_mission_run_ids_tx(
    connection: &Connection,
    generation_id: &str,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare(
        "WITH RECURSIVE descendant_runs(id) AS (
           SELECT child.id
           FROM mission_runs child
           JOIN step_runs parent_step ON parent_step.subject=child.parent_step_run
           WHERE parent_step.generation_id=?1
           UNION
           SELECT child.id
           FROM mission_runs child
           JOIN step_runs parent_step ON parent_step.subject=child.parent_step_run
           JOIN descendant_runs parent_run ON parent_step.run_id=parent_run.id
         )
         SELECT id FROM descendant_runs ORDER BY id",
    )?;
    statement
        .query_map([generation_id], |row| row.get::<_, String>(0))?
        .collect()
}

fn cancel_mission_run_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    run_subject: &str,
    reason: &str,
    batch_id: &str,
    now: u128,
) -> Result<Vec<String>, St3Error> {
    let run_id = run_subject
        .strip_prefix("mission-run/")
        .unwrap_or(run_subject);
    let current = transaction
        .query_row(
            "SELECT status, phase, current_generation_id, mission_id
             FROM mission_runs WHERE id=?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(internal)?
        .ok_or_else(|| {
            St3Error::new(
                "missing-mission-run",
                format!("mission run `mission-run/{run_id}` does not exist"),
            )
        })?;
    let (status, phase, generation_id, mission_id) = current;
    if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
        return Ok(Vec::new());
    }
    let descendants =
        descendant_mission_run_ids_tx(transaction, &generation_id).map_err(internal)?;
    let mut claim_ids = Vec::new();
    for descendant in descendants.into_iter().rev() {
        claim_ids.extend(cancel_mission_run_tx(
            transaction,
            origin,
            &format!("mission-run/{descendant}"),
            reason,
            batch_id,
            now,
        )?);
    }
    let revision: String = transaction
        .query_row(
            "SELECT revision FROM run_generations WHERE id=?1",
            [&generation_id],
            |row| row.get(0),
        )
        .map_err(internal)?;
    let mission: MissionSpec = transaction
        .query_row(
            "SELECT body FROM mission_revisions WHERE mission_id=?1 AND revision=?2",
            params![mission_id, revision],
            |row| row.get::<_, String>(0),
        )
        .map_err(internal)
        .and_then(|body| serde_json::from_str(&body).map_err(internal))?;
    let final_paths = flatten_mission_step_specs(&mission)
        .into_iter()
        .filter(|step| step.finally)
        .map(|step| step.path.clone())
        .collect::<BTreeSet<_>>();
    let mut statement = transaction
        .prepare(
            "SELECT subject, step_path, lease_owner FROM step_runs
             WHERE generation_id=?1 AND status NOT IN ('completed','failed','cancelled')
             ORDER BY subject",
        )
        .map_err(internal)?;
    let steps = statement
        .query_map([&generation_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(internal)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(internal)?;
    drop(statement);
    for (subject, path, claimant) in steps {
        if final_paths.contains(&path) {
            continue;
        }
        if let Some(claimant) = claimant {
            let stable = canonical_hash(&(run_id, &generation_id, &subject, "cancelled"))
                .map_err(internal)?;
            append_claim_tx(
                transaction,
                origin,
                &format!("message/mission-cancelled-{}", &stable[..20]),
                "message.sent",
                None,
                &json!({"fields": {
                    "from": "daemon/runtime",
                    "to": claimant,
                    "title": "Mission work cancelled",
                    "content": format!("Mission run mission-run/{run_id} cancelled step {subject}. Stop this work. Reason: {reason}"),
                    "status": "sent",
                    "tags": [format!("mission-run:mission-run/{run_id}"), format!("step-run:{subject}")],
                }}),
                &[],
                Some(batch_id),
            )
            .map_err(internal)?;
        }
        transaction
            .execute(
                "UPDATE step_runs
                 SET status='cancelled', blocked_reason=?2, lease_owner=NULL,
                     lease_incarnation=NULL, lease_expires_at_unix_ms=NULL,
                     updated_at_unix_ms=?3
                 WHERE subject=?1",
                params![subject, reason, now.to_string()],
            )
            .map_err(internal)?;
        let claim = append_claim_tx(
            transaction,
            origin,
            &subject,
            "step-run.state",
            None,
            &json!({"fields": {"status": "cancelled", "reason": reason}}),
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(claim.id);
    }
    let (next_status, next_phase) = if final_paths.is_empty() {
        ("running", "cleanup-cancelled")
    } else {
        ("running", "final-cancelled")
    };
    transaction
        .execute(
            "UPDATE mission_runs SET status=?2, phase=?3, updated_at_unix_ms=?4 WHERE id=?1",
            params![run_id, next_status, next_phase, now.to_string()],
        )
        .map_err(internal)?;
    transaction
        .execute(
            "UPDATE run_generations SET status=?2, updated_at_unix_ms=?3 WHERE id=?1",
            params![generation_id, next_status, now.to_string()],
        )
        .map_err(internal)?;
    let body = json!({"fields": {
        "status": next_status,
        "phase": next_phase,
        "reason": reason,
        "previous_phase": phase,
    }});
    for (subject, kind) in [
        (format!("mission-run/{run_id}"), "mission-run.state"),
        (
            format!("run-generation/{generation_id}"),
            "run-generation.state",
        ),
    ] {
        let claim = append_claim_tx(
            transaction,
            origin,
            &subject,
            kind,
            None,
            &body,
            &[],
            Some(batch_id),
        )
        .map_err(internal)?;
        claim_ids.push(claim.id);
    }
    Ok(claim_ids)
}

fn cancel_descendant_mission_runs_tx(
    transaction: &Transaction<'_>,
    origin: &str,
    generation_id: &str,
    actor: &str,
    reason: &str,
    now: u128,
) -> Result<bool, St3Error> {
    let mut changed = false;
    for run_id in descendant_mission_run_ids_tx(transaction, generation_id).map_err(internal)? {
        let (status, child_generation): (String, String) = transaction
            .query_row(
                "SELECT status, current_generation_id FROM mission_runs WHERE id=?1",
                [&run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(internal)?;
        if !matches!(status.as_str(), "running" | "standing" | "blocked") {
            continue;
        }
        changed = true;
        let mut statement = transaction
            .prepare(
                "SELECT subject FROM step_runs
                 WHERE generation_id=?1 AND status NOT IN ('completed','failed','cancelled')
                 ORDER BY subject",
            )
            .map_err(internal)?;
        let steps = statement
            .query_map([&child_generation], |row| row.get::<_, String>(0))
            .map_err(internal)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(internal)?;
        drop(statement);
        for subject in steps {
            transaction
                .execute(
                    "UPDATE step_runs
                     SET status='cancelled', blocked_reason=?2, lease_owner=NULL,
                         lease_incarnation=NULL, lease_expires_at_unix_ms=NULL,
                         updated_at_unix_ms=?3
                     WHERE subject=?1",
                    params![subject, reason, now.to_string()],
                )
                .map_err(internal)?;
            append_claim_tx(
                transaction,
                origin,
                &subject,
                "step-run.state",
                Some(actor),
                &json!({"fields": {"status": "cancelled", "reason": reason}}),
                &[],
                None,
            )
            .map_err(internal)?;
        }
        transaction
            .execute(
                "UPDATE mission_runs
                 SET status='cancelled', phase='terminal', updated_at_unix_ms=?2
                 WHERE id=?1",
                params![run_id, now.to_string()],
            )
            .map_err(internal)?;
        transaction
            .execute(
                "UPDATE run_generations
                 SET status='cancelled', updated_at_unix_ms=?2 WHERE id=?1",
                params![child_generation, now.to_string()],
            )
            .map_err(internal)?;
        let body = json!({"fields": {
            "status": "cancelled",
            "phase": "terminal",
            "reason": reason,
        }});
        append_claim_tx(
            transaction,
            origin,
            &format!("mission-run/{run_id}"),
            "mission-run.state",
            Some(actor),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        append_claim_tx(
            transaction,
            origin,
            &format!("run-generation/{child_generation}"),
            "run-generation.state",
            Some(actor),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
    }
    Ok(changed)
}

fn mission_run_view_tx(connection: &Connection, run_id: &str) -> rusqlite::Result<MissionRunView> {
    let mut view = connection.query_row(
        "SELECT mission_runs.id, mission_runs.mission_id, mission_runs.initial_revision,
                mission_runs.current_generation_id, run_generations.revision,
                mission_runs.root_revision, mission_runs.root_run_id, mission_runs.parent_step_run,
                mission_runs.workspace, mission_runs.requester, mission_runs.inputs, mission_runs.mode,
                mission_run_deadlines.timeout_ms, mission_run_deadlines.deadline_at_unix_ms,
                mission_runs.status, mission_runs.phase, mission_runs.created_at_unix_ms,
                mission_runs.updated_at_unix_ms
         FROM mission_runs JOIN run_generations
           ON run_generations.id=mission_runs.current_generation_id
         LEFT JOIN mission_run_deadlines ON mission_run_deadlines.run_id=mission_runs.id
         WHERE mission_runs.id=?1",
        [run_id],
        |row| {
            let id: String = row.get(0)?;
            let generation_id: String = row.get(3)?;
            let root_run_id: String = row.get(6)?;
            let deadline: Option<String> = row.get(13)?;
            let created: String = row.get(16)?;
            let updated: String = row.get(17)?;
            Ok(MissionRunView {
                subject: format!("mission-run/{id}"),
                id,
                mission: format!("mission/{}", row.get::<_, String>(1)?),
                generation: format!("run-generation/{generation_id}"),
                initial_revision: row.get(2)?,
                revision: row.get(4)?,
                root_revision: row.get(5)?,
                root_mission_run: format!("mission-run/{root_run_id}"),
                parent_step_run: row.get(7)?,
                workspace: row.get(8)?,
                requester: row.get(9)?,
                inputs: serde_json::from_str(&row.get::<_, String>(10)?).unwrap_or_default(),
                mode: row.get(11)?,
                timeout_ms: row.get(12)?,
                deadline_at_unix_ms: deadline.and_then(|value| value.parse().ok()),
                status: row.get(14)?,
                phase: row.get(15)?,
                created_at_unix_ms: created.parse().unwrap_or(0),
                updated_at_unix_ms: updated.parse().unwrap_or(0),
                steps: Vec::new(),
            })
        },
    )?;
    let mut statement = connection.prepare(
        "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
         FROM step_runs WHERE generation_id=?1 ORDER BY created_at_unix_ms, step_path",
    )?;
    view.steps = statement
        .query_map(
            [generation_id_from_subject(&view.generation)],
            step_run_from_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(view)
}

fn run_generation_view_tx(
    connection: &Connection,
    generation_id: &str,
) -> rusqlite::Result<RunGenerationView> {
    let mut view = connection.query_row(
        "SELECT id, run_id, revision, predecessor_id, status, actor, reason,
                created_at_unix_ms, updated_at_unix_ms
         FROM run_generations WHERE id=?1",
        [generation_id],
        |row| {
            let id: String = row.get(0)?;
            let run_id: String = row.get(1)?;
            let predecessor: Option<String> = row.get(3)?;
            let created: String = row.get(7)?;
            let updated: String = row.get(8)?;
            Ok(RunGenerationView {
                subject: format!("run-generation/{id}"),
                id,
                run: format!("mission-run/{run_id}"),
                revision: row.get(2)?,
                predecessor: predecessor.map(|id| format!("run-generation/{id}")),
                status: row.get(4)?,
                actor: row.get(5)?,
                reason: row.get(6)?,
                created_at_unix_ms: created.parse().unwrap_or(0),
                updated_at_unix_ms: updated.parse().unwrap_or(0),
                steps: Vec::new(),
            })
        },
    )?;
    let mut statement = connection.prepare(
        "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, available_to, agentless, title, goals, worker_reported,
                lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms, readiness_epoch, constraints
         FROM step_runs WHERE generation_id=?1 ORDER BY created_at_unix_ms, step_path",
    )?;
    view.steps = statement
        .query_map([generation_id], step_run_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(view)
}

fn revision_proposal_view_tx(
    connection: &Connection,
    proposal_id: &str,
) -> rusqlite::Result<RevisionProposalView> {
    connection.query_row(
        "SELECT id, run_id, source_generation_id, candidate_revision, actor, reason, status,
                cutover, compatible_steps, reviewers, approvals, preview_hash,
                successor_generation_id, created_at_unix_ms, updated_at_unix_ms
         FROM revision_proposals WHERE id=?1",
        [proposal_id],
        |row| {
            let id: String = row.get(0)?;
            let run_id: String = row.get(1)?;
            let source: String = row.get(2)?;
            let successor: Option<String> = row.get(12)?;
            let created: String = row.get(13)?;
            let updated: String = row.get(14)?;
            Ok(RevisionProposalView {
                subject: format!("revision-proposal/{id}"),
                id,
                run: format!("mission-run/{run_id}"),
                source_generation: format!("run-generation/{source}"),
                candidate_revision: row.get(3)?,
                actor: row.get(4)?,
                reason: row.get(5)?,
                status: row.get(6)?,
                cutover: match row.get::<_, String>(7)?.as_str() {
                    "when-idle" => RevisionCutover::WhenIdle,
                    _ => RevisionCutover::RestartActive,
                },
                compatible_steps: serde_json::from_str(&row.get::<_, String>(8)?)
                    .unwrap_or_default(),
                reviewers: serde_json::from_str(&row.get::<_, String>(9)?).unwrap_or_default(),
                approvals: serde_json::from_str(&row.get::<_, String>(10)?).unwrap_or_default(),
                preview_hash: row.get(11)?,
                successor_generation: successor.map(|id| format!("run-generation/{id}")),
                created_at_unix_ms: created.parse().unwrap_or(0),
                updated_at_unix_ms: updated.parse().unwrap_or(0),
            })
        },
    )
}

fn revision_cutover_name(cutover: &RevisionCutover) -> &'static str {
    match cutover {
        RevisionCutover::RestartActive => "restart-active",
        RevisionCutover::WhenIdle => "when-idle",
    }
}

fn generation_id_from_subject(subject: &str) -> &str {
    subject.strip_prefix("run-generation/").unwrap_or(subject)
}

fn step_generation_is_current(
    connection: &Connection,
    step: &StepRunView,
) -> rusqlite::Result<bool> {
    let current: String = connection.query_row(
        "SELECT current_generation_id FROM mission_runs WHERE id=?1",
        [step.run.strip_prefix("mission-run/").unwrap_or(&step.run)],
        |row| row.get(0),
    )?;
    Ok(generation_id_from_subject(&step.generation) == current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::parse_test_intent as parse_intent;

    fn simple(command: &str) -> NormalizedIntent {
        parse_intent(
            &format!("version 2\n exec \"work\" {{ command {command:?}; restart \"never\" }} "),
            "node",
        )
        .expect("intent")
    }

    fn publish_mission(store: &Store, source: &str, key: &str) -> MissionSpec {
        let intent = crate::graph::parse_intent(source, "node").expect("mission intent");
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .expect("mission preview");
        store
            .apply(&intent, &planned.subject_tokens, key)
            .expect("mission publish");
        intent
            .missions
            .values()
            .next()
            .expect("published mission")
            .clone()
    }

    #[test]
    fn eval_cleanup_includes_descendant_mission_runtimes() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"
version 2

mission "eval/root" state="ready" timeout="1m" {
  goal "Own one isolated eval tree."
}

mission "eval/child" state="ready" {
  goal "Run one child agent."
}
"#;
        publish_mission(&store, source, "publish-eval-tree");
        let root = store
            .create_mission_run(&MissionRunRequest {
                mission: "eval/root".into(),
                revision: None,
                workspace: "/eval".into(),
                requester: Some("person/test".into()),
                mode: Some("eval".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "eval-tree-root".into(),
            })
            .unwrap();
        let child = store
            .create_child_mission_run(
                &MissionRunRequest {
                    mission: "eval/child".into(),
                    revision: None,
                    workspace: "/eval/child".into(),
                    requester: Some("daemon/test".into()),
                    mode: Some("run".into()),
                    inputs: BTreeMap::new(),
                    idempotency_key: "eval-tree-child".into(),
                },
                &root,
                "step-run/subscription/root/reviews",
                None,
            )
            .unwrap();
        let declaration = crate::graph::parse_execution_intent(
            r#"version 2
agent "worker" { workspace "/eval/child"; command "true"; restart "never" }
"#,
            "node",
            &child.id,
        )
        .unwrap();
        store
            .apply_internal(&declaration, "declare-eval-child-worker")
            .unwrap();

        assert_eq!(
            store.eval_runtime_records(&root.subject).unwrap(),
            vec![(format!("{}.worker", child.id), true)]
        );
        assert!(
            store
                .desired_subjects()
                .unwrap()
                .iter()
                .any(|desired| desired.owner_run.as_deref() == Some(child.subject.as_str()))
        );

        assert!(
            store
                .retire_eval_owned_desired(&root.subject)
                .unwrap()
                .is_empty()
        );
        assert!(store.desired_subjects().unwrap().iter().all(|desired| {
            desired.owner_run.as_deref() != Some(root.subject.as_str())
                && desired.owner_run.as_deref() != Some(child.subject.as_str())
        }));
    }

    #[test]
    fn the_store_requires_bounded_eval_deadlines_and_keeps_them_optional_elsewhere() {
        let store = Store::open_memory("node").unwrap();
        publish_mission(
            &store,
            r#"
version 2

mission "eval/missing-deadline" state="ready" {
  goal "Reject an eval without a deadline."
}

mission "eval/long-deadline" state="ready" timeout="21m" {
  goal "Reject an eval with a deadline above the limit."
}

mission "ordinary" state="ready" {
  goal "Allow an ordinary mission without a deadline."
}

mission "bounded" state="ready" timeout="1s" {
  goal "Store one absolute deadline for an ordinary mission."
}
"#,
            "publish-deadline-missions",
        );
        let request = |mission: &str, mode: &str| MissionRunRequest {
            mission: mission.into(),
            revision: None,
            workspace: "/work".into(),
            requester: Some("person/test".into()),
            mode: Some(mode.into()),
            inputs: BTreeMap::new(),
            idempotency_key: format!("deadline-{mission}-{mode}"),
        };

        assert_eq!(
            store
                .create_mission_run(&request("eval/missing-deadline", "eval"))
                .unwrap_err()
                .code,
            "missing-eval-timeout"
        );
        assert_eq!(
            store
                .create_mission_run(&request("eval/long-deadline", "eval"))
                .unwrap_err()
                .code,
            "eval-timeout-too-large"
        );

        let ordinary = store
            .create_mission_run(&request("ordinary", "run"))
            .unwrap();
        assert_eq!(ordinary.timeout_ms, None);
        assert_eq!(ordinary.deadline_at_unix_ms, None);

        let bounded = store
            .create_mission_run(&request("bounded", "run"))
            .unwrap();
        assert_eq!(bounded.timeout_ms, Some(1_000));
        assert_eq!(
            bounded.deadline_at_unix_ms,
            Some(bounded.created_at_unix_ms + 1_000)
        );
    }

    #[test]
    fn a_declarative_resource_refresh_emits_schema_valid_observer_state() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"version 2
resource "refresh/file" { kind "filesystem.file" }
observer "refresh/file" {
  resource "resource/refresh/file"
  provider "local.file"
  locator "/tmp/st3-refresh-test"
  field "status"
}
"#;
        let intent = crate::graph::parse_execution_intent(source, "node", "refresh-run").unwrap();
        let observer = intent
            .subjects
            .values()
            .find(|subject| subject.kind == "observer")
            .unwrap()
            .subject
            .clone();
        store
            .apply_internal(&intent, "declare-refresh-observer")
            .unwrap();

        let refresh = r#"version 2
resource "refresh/file" {
  refresh "manual" { timeout "1s" }
}
"#;
        let intent = crate::graph::parse_intent(refresh, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: refresh.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply_as(
                &intent,
                &planned.subject_tokens,
                "request-resource-refresh",
                Some("person/operator"),
            )
            .unwrap();

        let state = store
            .latest_claim(&observer, Some("observer.state"))
            .unwrap()
            .unwrap();
        assert!(
            state.body["fields"]["next_check_unix_ms"]
                .as_str()
                .is_some()
        );
        assert!(state.body["fields"]["attempt"].as_str().is_some());
    }

    #[test]
    fn work_views_include_ordered_inherited_constraints() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let mission = publish_mission(
            &store,
            r#"version 2
mission "guarded" state="ready" {
  goal "Complete guarded work."
  input "lane" kind="text"
  constraint "Use the ${input.lane} lane."
  step "parent" {
    agentless
    constraint "Do not push."
    mission "child" {
      goal "Inspect the child workspace."
      constraint "Keep the workspace clean."
      step "inspect" {
        constraint "Do not edit files."
      }
    }
  }
}
"#,
            "publish-guarded",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: mission.id,
                revision: Some(mission.revision),
                workspace: workspace.path().display().to_string(),
                requester: Some("person/operator".into()),
                mode: None,
                inputs: BTreeMap::from([("lane".into(), "review".into())]),
                idempotency_key: "run-guarded".into(),
            })
            .unwrap();
        let parent = run.steps.iter().find(|step| step.step == "parent").unwrap();
        assert_eq!(parent.constraints, ["Use the review lane.", "Do not push."]);
        let child = run
            .steps
            .iter()
            .find(|step| step.step.ends_with("child/inspect"))
            .unwrap();
        assert_eq!(
            child.constraints,
            [
                "Use the review lane.",
                "Do not push.",
                "Keep the workspace clean.",
                "Do not edit files."
            ]
        );
    }

    #[test]
    fn apply_is_subject_cas_and_idempotent() {
        let store = Store::open_memory("node").expect("store");
        let intent = simple("true");
        let mission = store
            .mission(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .expect("mission");
        let first = store
            .apply(&intent, &mission.subject_tokens, "one")
            .expect("apply");
        let repeated = store
            .apply(&intent, &mission.subject_tokens, "one")
            .expect("repeat");
        assert_eq!(first.batch_id, repeated.batch_id);
        let unchanged_mission = store
            .mission(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let unchanged = store
            .apply(&intent, &unchanged_mission.subject_tokens, "unchanged")
            .unwrap();
        assert!(!unchanged.changed);
        assert!(unchanged.batch_id.is_none());
        assert_eq!(unchanged.store_index, first.store_index);

        let changed = simple("false");
        let error = store
            .apply(&changed, &mission.subject_tokens, "two")
            .expect_err("stale token");
        assert_eq!(error.code, "stale-subject");
        assert_eq!(error.details["subject"], "exec/work");
        assert_eq!(error.details["expected_heads"], json!([]));
        assert_eq!(error.details["current_heads"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn an_internal_apply_replays_after_desired_state_drifts() {
        let store = Store::open_memory("node").unwrap();
        let intent = simple("true");
        let first = store.apply_internal(&intent, "materialize:test").unwrap();
        let replay = store.apply_internal(&intent, "materialize:test").unwrap();

        assert!(first.changed);
        assert!(!replay.changed);
        assert_eq!(replay.store_index, first.store_index);
        assert!(replay.batch_id.is_none());

        let stop = crate::graph::parse_test_intent(
            "version 2\nstop \"exec/work\"\n",
            "node",
        )
        .unwrap();
        assert!(store.apply_internal(&stop, "stop:test").unwrap().changed);

        let restored = store.apply_internal(&intent, "materialize:test").unwrap();
        assert!(restored.changed);
        assert_eq!(store.selected_desired_kind("exec/work").unwrap(), Some("exec".into()));
    }

    #[test]
    fn direct_publication_is_additive_atomic_and_operation_ids_are_immutable() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let mission = publish_mission(
            &store,
            "version 2\nmission \"lifecycle\" state=\"ready\" { goal \"Remain open.\" }",
            "publish-lifecycle",
        );
        let creation = format!(
            "version 2\nmission-run \"lifecycle/demo\" {{\n  mission {:?}\n  workspace {:?}\n  requester \"person/operator\"\n}}\n",
            format!("mission/lifecycle@{}", mission.revision),
            workspace.path().display().to_string(),
        );
        let intent = crate::graph::parse_intent(&creation, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: creation.clone(),
                    source_name: None,
                },
            )
            .unwrap();
        let created = store
            .apply_as(
                &intent,
                &preview.subject_tokens,
                "create-lifecycle-run",
                Some("person/operator"),
            )
            .unwrap();
        assert!(created.changed);
        assert!(
            created
                .operations
                .iter()
                .any(|operation| operation.action == "start-mission-run")
        );

        let replay_preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: creation,
                    source_name: None,
                },
            )
            .unwrap();
        let replay = store
            .apply_as(
                &intent,
                &replay_preview.subject_tokens,
                "replay-lifecycle-run",
                Some("person/operator"),
            )
            .unwrap();
        assert!(!replay.changed);

        let omitted_source =
            "version 2\nmission \"unrelated\" state=\"ready\" { goal \"Do other work.\" }\n";
        let omitted = crate::graph::parse_intent(omitted_source, "node").unwrap();
        let omitted_preview = store
            .mission(
                &omitted,
                IntentInput {
                    kdl: omitted_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        let omission = store
            .apply_as(
                &omitted,
                &omitted_preview.subject_tokens,
                "omit-existing-run",
                Some("person/operator"),
            )
            .unwrap();
        assert!(omission.changed);
        assert!(store.mission_run("lifecycle/demo").unwrap().is_some());

        let atomic_failure = r#"version 2
mission-run "lifecycle/demo" {
  cancellation "stop" { reason "stop the run" }
}
resource "missing" {
  refresh "now" { timeout "1s" }
}
"#;
        let atomic_intent = crate::graph::parse_intent(atomic_failure, "node").unwrap();
        let atomic_preview = store
            .mission(
                &atomic_intent,
                IntentInput {
                    kdl: atomic_failure.into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert!(!atomic_preview.blockers.is_empty());
        let error = store
            .apply_as(
                &atomic_intent,
                &atomic_preview.subject_tokens,
                "atomic-failure",
                Some("person/operator"),
            )
            .unwrap_err();
        assert_eq!(error.code, "resource-not-observed");
        assert_eq!(
            store.mission_run("lifecycle/demo").unwrap().unwrap().phase,
            "normal"
        );

        let cancellation = r#"version 2
mission-run "lifecycle/demo" {
  cancellation "stop" { reason "stop the run" }
}
"#;
        let cancel_intent = crate::graph::parse_intent(cancellation, "node").unwrap();
        let cancel_preview = store
            .mission(
                &cancel_intent,
                IntentInput {
                    kdl: cancellation.into(),
                    source_name: None,
                },
            )
            .unwrap();
        let cancelled = store
            .apply_as(
                &cancel_intent,
                &cancel_preview.subject_tokens,
                "cancel-lifecycle-run",
                Some("person/operator"),
            )
            .unwrap();
        assert!(cancelled.changed);
        let replay_preview = store
            .mission(
                &cancel_intent,
                IntentInput {
                    kdl: cancellation.into(),
                    source_name: None,
                },
            )
            .unwrap();
        let replay = store
            .apply_as(
                &cancel_intent,
                &replay_preview.subject_tokens,
                "replay-cancellation",
                Some("person/operator"),
            )
            .unwrap();
        assert!(!replay.changed);

        let changed_id = r#"version 2
mission-run "lifecycle/demo" {
  cancellation "stop" { reason "a different reason" }
}
"#;
        let changed_intent = crate::graph::parse_intent(changed_id, "node").unwrap();
        let changed_preview = store
            .mission(
                &changed_intent,
                IntentInput {
                    kdl: changed_id.into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert!(
            changed_preview
                .blockers
                .iter()
                .any(|blocker| blocker.contains("different content"))
        );
    }

    #[test]
    fn a_declared_planning_session_creates_its_session_and_planner_atomically() {
        let store = Store::open_memory("node").unwrap();
        let request = store
            .put_document(
                "doc/planning/release/request",
                b"Mission the release.",
                &None,
                "planning-request",
            )
            .unwrap();
        let source = format!(
            r#"version 2
planning-session "planning/release/one" {{
  mission "release"
  request "{}@{}"
  workspace "/work/release"
  requester "person/operator"
  planner "codex" {{ model "gpt-5.6-sol"; effort "medium" }}
}}
"#,
            request.name, request.hash
        );
        let intent = crate::graph::parse_intent(&source, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.clone(),
                    source_name: None,
                },
            )
            .unwrap();
        assert!(preview.blockers.is_empty(), "{:?}", preview.blockers);
        let applied = store
            .apply_as(
                &intent,
                &preview.subject_tokens,
                "planning-session-publication",
                Some("person/operator"),
            )
            .unwrap();
        let session = store
            .planning_session("planning/release/one")
            .unwrap()
            .unwrap();
        let planner =
            crate::graph::planning_planner_subject("planning-session/planning/release/one");
        assert_eq!(session.status, "planning");
        assert_eq!(session.planner, planner);
        assert!(
            applied
                .operations
                .iter()
                .any(|operation| operation.action == "start-planning")
        );
        assert!(store.selected_desired_token(&planner).unwrap().is_some());

        let replay_preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source,
                    source_name: None,
                },
            )
            .unwrap();
        let replay = store
            .apply_as(
                &intent,
                &replay_preview.subject_tokens,
                "planning-session-replay",
                Some("person/operator"),
            )
            .unwrap();
        assert!(!replay.changed);
    }

    #[test]
    fn a_declared_human_revision_creates_and_approves_one_proposal() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let initial = publish_mission(
            &store,
            r#"version 2
mission "reviewed" state="ready" revisions="human-only" {
  goal "Use the initial definition."
  step "work" { agentless }
}
"#,
            "publish-reviewed-initial",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: initial.id.clone(),
                revision: Some(initial.revision.clone()),
                workspace: workspace.path().display().to_string(),
                requester: Some("person/operator".into()),
                mode: None,
                inputs: BTreeMap::new(),
                idempotency_key: "run-reviewed".into(),
            })
            .unwrap();
        let candidate = publish_mission(
            &store,
            r#"version 2
mission "reviewed" state="ready" revisions="human-only" {
  goal "Use the reviewed definition."
  step "work" { agentless }
}
"#,
            "publish-reviewed-candidate",
        );
        let revision = format!(
            "version 2\nmission-run {:?} {{\n  revision \"review-one\" {{\n    mission {:?}\n    from {:?}\n    reason \"the definition needs review\"\n  }}\n}}\n",
            run.subject,
            format!("mission/{}@{}", candidate.id, candidate.revision),
            run.generation,
        );
        let intent = crate::graph::parse_intent(&revision, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: revision,
                    source_name: None,
                },
            )
            .unwrap();
        let applied = store
            .apply_as(
                &intent,
                &preview.subject_tokens,
                "declare-reviewed-revision",
                Some("person/operator"),
            )
            .unwrap();
        assert!(
            applied
                .operations
                .iter()
                .any(|operation| { operation.action == "propose-revision:review-one" })
        );
        let proposal = store.revision_proposal_for_run(&run.id).unwrap().unwrap();
        assert_eq!(proposal.status, "pending-approval");
        assert_eq!(proposal.reviewers, ["person/operator"]);

        let approved = store
            .approve_revision_proposal(
                &proposal.id,
                "person/operator",
                proposal.preview_hash.as_deref().unwrap(),
                "approve-reviewed-revision",
            )
            .unwrap();
        assert_eq!(approved.status, "applied");
        assert_eq!(approved.mission_run.revision, candidate.revision);
        assert_ne!(approved.mission_run.generation, run.generation);
    }

    #[test]
    fn a_mission_response_reports_the_server_normalized_revision() {
        let source = r#"
version 2

  mission "portable" state="ready" {
    goal "Keep one worker available."
     agent "worker" { workspace "."; command "true" }
  }

"#;
        let server_intent = crate::graph::parse_intent(source, "server-node").unwrap();
        let client_intent = crate::graph::parse_intent(source, "local").unwrap();
        let server_mission = server_intent.missions.values().next().unwrap();
        let client_mission = client_intent.missions.values().next().unwrap();
        assert_ne!(server_mission.revision, client_mission.revision);

        let store = Store::open_memory("server-node").unwrap();
        let response = store
            .mission(
                &server_intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert_eq!(
            response.mission_revisions["mission/portable"],
            server_mission.revision
        );
    }

    #[test]
    fn mission_inputs_are_exact_immutable_snapshots() {
        let store = Store::open_memory("node").unwrap();
        publish_mission(
            &store,
            r#"
version 2

  resource "source" { kind "custom.st3.document-source" }
  mission "inputs" state="ready" {
    input "message" kind="text"
    input "source" kind="resource"
    goal "Use the supplied values."
    step "work" { agentless; goal "Write ${input.message}."; gate "source state" { field "state" "${input.source}" is "ready" } }
  }

"#,
            "publish-input-mission",
        );
        let first = store
            .append_claim(&ClaimInput {
                subject: "resource/source".into(),
                kind: "resource.observed".into(),
                actor: None,
                fields: BTreeMap::from([("state".into(), Value::String("ready".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("source-ready".into()),
            })
            .unwrap();
        let request = MissionRunRequest {
            mission: "inputs".into(),
            revision: None,
            workspace: ".".into(),
            requester: Some("person/test".into()),
            mode: None,
            inputs: BTreeMap::from([
                ("message".into(), "hello".into()),
                ("source".into(), "resource/source".into()),
            ]),
            idempotency_key: "run-input-mission".into(),
        };
        let run = store.create_mission_run(&request).unwrap();
        assert_eq!(run.inputs["message"].value, "hello");
        assert_eq!(
            run.inputs["source"].value,
            format!("resource/source@{}", first.id)
        );
        assert_eq!(
            store.create_mission_run(&request).unwrap().subject,
            run.subject
        );
        let mut second_run = request.clone();
        second_run.idempotency_key = "run-input-mission-two".into();
        assert_eq!(
            store.create_mission_run(&second_run).unwrap_err().code,
            "mission-run-capacity"
        );

        let mut changed = request.clone();
        changed.inputs.insert("message".into(), "changed".into());
        assert_eq!(
            store.create_mission_run(&changed).unwrap_err().code,
            "idempotency-mismatch"
        );
        let mut missing = request.clone();
        missing.idempotency_key = "run-input-missing".into();
        missing.inputs.remove("source");
        assert_eq!(
            store.create_mission_run(&missing).unwrap_err().code,
            "invalid-mission-inputs"
        );
        let mut extra = request.clone();
        extra.idempotency_key = "run-input-extra".into();
        extra.inputs.insert("other".into(), "value".into());
        assert_eq!(
            store.create_mission_run(&extra).unwrap_err().code,
            "invalid-mission-inputs"
        );

        let second = store
            .append_claim(&ClaimInput {
                subject: "resource/source".into(),
                kind: "resource.observed".into(),
                actor: None,
                fields: BTreeMap::from([("state".into(), Value::String("changed".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("source-changed".into()),
            })
            .unwrap();
        assert_ne!(first.id, second.id);
        assert_eq!(
            store.mission_run(&run.id).unwrap().unwrap().inputs["source"].claim_id,
            Some(first.id)
        );
    }

    #[test]
    fn child_runs_accept_inputs_and_revisions_cannot_change_them() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"
version 2

  mission "parent" state="ready" { goal "Keep the parent run open." }
  mission "child" state="ready" {
    input "message" kind="text"
    goal "Use the supplied message."
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "publish-parent-child")
            .unwrap();
        let parent = store
            .create_mission_run(&MissionRunRequest {
                mission: "parent".into(),
                revision: None,
                workspace: ".".into(),
                requester: None,
                mode: None,
                inputs: BTreeMap::new(),
                idempotency_key: "parent-run".into(),
            })
            .unwrap();
        let child = store
            .create_child_mission_run(
                &MissionRunRequest {
                    mission: "child".into(),
                    revision: None,
                    workspace: ".".into(),
                    requester: None,
                    mode: None,
                    inputs: BTreeMap::from([("message".into(), "hello".into())]),
                    idempotency_key: "child-run".into(),
                },
                &parent,
                "step-run/test/child",
                None,
            )
            .unwrap();
        assert_eq!(child.inputs["message"].value, "hello");
        assert_eq!(child.root_mission_run, parent.subject);

        let original = publish_mission(
            &store,
            r#"
version 2

  mission "revision-input" state="ready" {
    input "message" kind="text"
    goal "Use the supplied message."
  }

"#,
            "publish-revision-input",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: original.id,
                revision: None,
                workspace: ".".into(),
                requester: None,
                mode: None,
                inputs: BTreeMap::from([("message".into(), "hello".into())]),
                idempotency_key: "revision-input-run".into(),
            })
            .unwrap();
        let changed = publish_mission(
            &store,
            r#"
version 2

  mission "revision-input" state="ready" {
    input "source" kind="resource"
    goal "Use the supplied resource."
  }

"#,
            "publish-changed-revision-input",
        );
        let revision_error = store
            .adopt_mission_revision(
                &run.id,
                &changed,
                "agent/node.worker",
                "change the input declaration",
                "adopt-changed-revision-input",
            )
            .unwrap_err();
        assert_eq!(revision_error.code, "run-input-mutation");
    }

    #[test]
    fn mission_run_capacity_uses_the_strictest_active_revision() {
        let store = Store::open_memory("node").unwrap();
        let first = publish_mission(
            &store,
            r#"version 2
 mission "bounded" state="ready" { concurrent-runs max=3; goal "Keep this run open." } "#,
            "bounded-three",
        );
        for index in 1..=2 {
            store
                .create_mission_run(&MissionRunRequest {
                    mission: "bounded".into(),
                    revision: Some(first.revision.clone()),
                    workspace: ".".into(),
                    requester: None,
                    mode: None,
                    inputs: BTreeMap::new(),
                    idempotency_key: format!("bounded-run-{index}"),
                })
                .unwrap();
        }
        let second = publish_mission(
            &store,
            r#"version 2
 mission "bounded" state="ready" { concurrent-runs max=1; goal "Keep one run open." } "#,
            "bounded-one",
        );
        assert_eq!(
            store
                .active_mission_runs_for_mission("bounded")
                .unwrap()
                .len(),
            2
        );
        let error = store
            .create_mission_run(&MissionRunRequest {
                mission: "bounded".into(),
                revision: Some(second.revision),
                workspace: ".".into(),
                requester: None,
                mode: None,
                inputs: BTreeMap::new(),
                idempotency_key: "bounded-run-three".into(),
            })
            .unwrap_err();
        assert_eq!(error.code, "mission-run-capacity");
        assert!(error.message.contains("mission-run/"));

        let unlimited = Store::open_memory("node").unwrap();
        let mission = publish_mission(
            &unlimited,
            r#"version 2
 mission "unlimited" state="ready" { concurrent-runs; goal "Allow all runs." } "#,
            "unlimited-mission",
        );
        for index in 1..=3 {
            unlimited
                .create_mission_run(&MissionRunRequest {
                    mission: "unlimited".into(),
                    revision: Some(mission.revision.clone()),
                    workspace: ".".into(),
                    requester: None,
                    mode: None,
                    inputs: BTreeMap::new(),
                    idempotency_key: format!("unlimited-run-{index}"),
                })
                .unwrap();
        }
    }

    #[test]
    fn terminal_operations_do_not_replace_member_status() {
        let store = Store::open_memory("node").expect("store");
        let subject = "pty/demo";
        store
            .append_claim(&ClaimInput {
                subject: subject.into(),
                kind: "runtime.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("running".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .expect("member observation");
        store
            .append_claim(&ClaimInput {
                subject: subject.into(),
                kind: "terminal.input.result".into(),
                actor: None,
                fields: BTreeMap::from([("result".into(), Value::String("written".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .expect("input result");

        let actual = store
            .latest_actual_value(subject)
            .expect("actual state")
            .expect("subject state");
        assert_eq!(actual["status"], "running");
        assert_eq!(actual["result"], "written");
    }

    #[test]
    fn old_document_versions_remain_valid() {
        let store = Store::open_memory("node").expect("store");
        let old = store
            .put_document("doc/task", b"old", &None, "old")
            .expect("old");
        let new = store
            .put_document(
                "doc/task",
                b"new",
                &Some(old.binding_claim_id.clone()),
                "new",
            )
            .expect("new");
        assert_ne!(old.hash, new.hash);
        let intent = parse_intent(
            &format!(
                "version 2\n person \"worker\"; message \"task\" {{ to \"person/worker\"; content \"doc/task@{}\"; }} ",
                old.hash
            ),
            "node",
        )
        .expect("intent");
        let mission = store
            .mission(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .expect("mission");
        assert!(mission.blockers.is_empty());
        assert_eq!(mission.warnings.len(), 1);
    }

    #[test]
    fn historical_mission_and_status_use_the_selected_index() {
        let store = Store::open_memory("node").expect("store");
        let first = simple("true");
        let first_mission = store
            .mission(
                &first,
                IntentInput {
                    kdl: "first".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let first_apply = store
            .apply(&first, &first_mission.subject_tokens, "first")
            .unwrap();
        let first_index = first_apply.store_index;

        let second = simple("false");
        let second_mission = store
            .mission(
                &second,
                IntentInput {
                    kdl: "second".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&second, &second_mission.subject_tokens, "second")
            .unwrap();

        let historical = store
            .status_at(Some("exec/work"), None, Some(first_index))
            .unwrap();
        assert_eq!(historical.store_index, first_index);
        assert_eq!(
            historical.subjects[0].desired_token,
            first_apply.subject_tokens["exec/work"].first().cloned()
        );
        let historical_mission = store
            .mission_at(
                &second,
                IntentInput {
                    kdl: "historical".into(),
                    source_name: None,
                },
                Some(first_index),
            )
            .unwrap();
        assert_eq!(
            historical_mission.subject_tokens["exec/work"],
            first_apply.subject_tokens["exec/work"]
        );
        assert_eq!(historical_mission.changes.len(), 1);
    }

    #[test]
    fn replication_carries_document_bytes_and_name_versions() {
        let source = Store::open_memory("source").unwrap();
        let document = source
            .put_document("doc/task", b"replicated", &None, "document")
            .unwrap();
        let batch = source.export_replication(0).unwrap();
        let target = Store::open_memory("target").unwrap();
        target.import_replication("source", &batch).unwrap();
        assert_eq!(
            target
                .get_document("doc/task", &document.hash)
                .unwrap()
                .unwrap(),
            b"replicated"
        );
    }

    #[test]
    fn a_host_document_must_exist_before_its_declaration_is_applied() {
        let store = Store::open_memory("node").unwrap();
        let bytes = b"host facts\n";
        let hash = hex::encode(sha2::Sha256::digest(bytes));
        let source = format!("version 2\nhost \"node\" {{ document \"doc/hosts/node@{hash}\" }}\n");
        let intent = crate::graph::parse_test_intent(&source, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.clone(),
                    source_name: None,
                },
            )
            .unwrap();
        let error = store
            .apply(&intent, &preview.subject_tokens, "missing-host-document")
            .unwrap_err();
        assert_eq!(error.code, "missing-document");

        store
            .put_document("doc/hosts/node", bytes, &None, "host-document")
            .unwrap();
        let preview = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source,
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &preview.subject_tokens, "apply-host-document")
            .unwrap();
    }

    #[test]
    fn replication_carries_runnable_mission_definitions() {
        let source = Store::open_memory("source").unwrap();
        let kdl = r#"version 2
 mission "remote" state="ready" { goal "Run remote work."; step "work" { } } "#;
        let intent = parse_intent(kdl, "source").unwrap();
        let planned = source
            .mission(
                &intent,
                IntentInput {
                    kdl: kdl.into(),
                    source_name: None,
                },
            )
            .unwrap();
        source
            .apply(&intent, &planned.subject_tokens, "remote-mission")
            .unwrap();

        let target = Store::open_memory("target").unwrap();
        target
            .import_replication("source", &source.export_replication(0).unwrap())
            .unwrap();
        let replicated = target.mission_spec("remote", None).unwrap().unwrap();
        assert_eq!(replicated.state, MissionState::Ready);
        let run = target
            .create_mission_run(&MissionRunRequest {
                mission: "remote".into(),
                revision: Some(replicated.revision),
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "remote-run".into(),
            })
            .unwrap();
        assert_eq!(run.steps.len(), 1);
    }

    #[test]
    fn replication_carries_mission_runs_and_remote_work_updates() {
        let controller = Store::open_memory("controller").unwrap();
        let kdl = r#"
version 2

  mission "remote-work" state="ready" {
    goal "Complete mission remote-work."
    step "work" { assigned-to "agent/worker.one" }
  }

"#;
        let intent = parse_intent(kdl, "controller").unwrap();
        let planned = controller
            .mission(
                &intent,
                IntentInput {
                    kdl: kdl.into(),
                    source_name: None,
                },
            )
            .unwrap();
        controller
            .apply(&intent, &planned.subject_tokens, "remote-work-mission")
            .unwrap();
        let run = controller
            .create_mission_run(&MissionRunRequest {
                mission: "remote-work".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "remote-work-run".into(),
            })
            .unwrap();
        let step = run.steps[0].subject.clone();
        controller.set_step_state(&step, "ready", None).unwrap();

        let worker = Store::open_memory("worker").unwrap();
        worker
            .import_replication("controller", &controller.export_replication(0).unwrap())
            .unwrap();
        let remote = worker.mission_run(&run.id).unwrap().unwrap();
        assert_eq!(remote.steps[0].status, "ready");
        for action in ["claim", "complete"] {
            worker
                .work_action(
                    &step,
                    action,
                    &WorkRequest {
                        actor: Some("agent/worker.one".into()),
                        incarnation: Some("worker-generation".into()),
                        summary: Some("the remote work is complete".into()),
                        reason: None,
                        evidence: Vec::new(),
                        idempotency_key: format!("remote-{action}"),
                    },
                )
                .unwrap();
        }

        controller
            .import_replication(
                "worker",
                &worker
                    .export_replication_for_heads(&controller.replica_heads().unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            controller.step_run(&step).unwrap().unwrap().status,
            "verifying"
        );
    }

    #[test]
    fn replication_carries_run_generation_lineage() {
        let source = Store::open_memory("source").unwrap();
        let publish = |goal: &str, key: &str| {
            let kdl = format!(
                r#"
version 2

  agent "owner" {{ workspace "."; command "true" }}
  mission "lineage" state="ready" {{
    goal "Replicate generation lineage."
    step "work" {{
      title "Work ${{ST_STEP}} in ${{ST_RUN_GENERATION}}"
      goal {goal:?}
    }}
    step "stable" {{ goal "Carry stable work." }}
  }}

"#
            );
            let intent = parse_intent(&kdl, "source").unwrap();
            let planned = source
                .mission(
                    &intent,
                    IntentInput {
                        kdl,
                        source_name: None,
                    },
                )
                .unwrap();
            source.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["lineage"].clone()
        };
        let first = publish("Use the first goal for ${ST_STEP_RUN}.", "lineage-one");
        let run = source
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "lineage-run".into(),
            })
            .unwrap();
        let stable = run.steps.iter().find(|step| step.step == "stable").unwrap();
        source
            .set_step_state(&stable.subject, "completed", None)
            .unwrap();
        let second = publish("Use the second goal for ${ST_STEP_RUN}.", "lineage-two");
        let revised = source
            .adopt_mission_revision(
                &run.id,
                &second,
                "agent/source.owner",
                "replicate a successor generation",
                "lineage-cutover",
            )
            .unwrap();

        let target = Store::open_memory("target").unwrap();
        target
            .import_replication("source", &source.export_replication(0).unwrap())
            .unwrap();
        let replicated = target.mission_run(&run.id).unwrap().unwrap();
        assert_eq!(replicated.generation, revised.generation);
        assert_eq!(replicated.revision, second.revision);
        assert_eq!(target.run_generations(&run.id).unwrap().len(), 2);
        assert_eq!(
            replicated
                .steps
                .iter()
                .find(|step| step.step == "work")
                .unwrap()
                .title,
            revised
                .steps
                .iter()
                .find(|step| step.step == "work")
                .unwrap()
                .title
        );
        assert_eq!(
            replicated
                .steps
                .iter()
                .find(|step| step.step == "work")
                .unwrap()
                .goals,
            revised
                .steps
                .iter()
                .find(|step| step.step == "work")
                .unwrap()
                .goals
        );
        assert_eq!(
            replicated
                .steps
                .iter()
                .find(|step| step.step == "stable")
                .unwrap()
                .status,
            "completed"
        );
    }

    #[test]
    fn replication_carries_revision_proposal_lifecycle() {
        let source = Store::open_memory("source").unwrap();
        let publish = |goal: &str, key: &str| {
            let kdl = format!(
                r#"
version 2

  agent "owner" {{ workspace "."; command "true" }}
  mission "proposal" state="ready" revisions="human-only" revision-reviewer="person/reviewer" {{
    goal "Replicate a revision proposal."
    step "work" {{ goal {goal:?} }}
  }}

"#
            );
            let intent = parse_intent(&kdl, "source").unwrap();
            let planned = source
                .mission(
                    &intent,
                    IntentInput {
                        kdl,
                        source_name: None,
                    },
                )
                .unwrap();
            source.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["proposal"].clone()
        };
        let first = publish("Use the first goal.", "proposal-one");
        let run = source
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/requester".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "proposal-run".into(),
            })
            .unwrap();
        let second = publish("Use the second goal.", "proposal-two");
        let proposal = source
            .create_revision_proposal(
                &run.id,
                &second,
                "agent/source.owner",
                "replicate the proposal lifecycle",
                "proposal-create",
            )
            .unwrap();
        let target = Store::open_memory("target").unwrap();
        target
            .import_replication("source", &source.export_replication(0).unwrap())
            .unwrap();
        assert_eq!(
            target
                .revision_proposal(&proposal.id)
                .unwrap()
                .unwrap()
                .status,
            "pending-approval"
        );

        source
            .approve_revision_proposal(
                &proposal.id,
                "person/reviewer",
                proposal.preview_hash.as_deref().unwrap(),
                "proposal-approve",
            )
            .unwrap();
        target
            .import_replication(
                "source",
                &source
                    .export_replication_for_heads(&target.replica_heads().unwrap())
                    .unwrap(),
            )
            .unwrap();
        let replicated = target.revision_proposal(&proposal.id).unwrap().unwrap();
        assert_eq!(replicated.status, "applied");
        assert!(replicated.approvals.contains(&"person/reviewer".into()));
        assert_eq!(
            replicated.successor_generation,
            Some(target.mission_run(&run.id).unwrap().unwrap().generation)
        );
    }

    #[test]
    fn replication_rejects_tampered_claims() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "host/source".into(),
                kind: "transport.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .unwrap();
        let mut batch = source.export_replication(0).unwrap();
        batch.batches[0].claims[0].body["fields"]["status"] = Value::String("down".into());
        let target = Store::open_memory("target").unwrap();
        let error = target
            .import_replication("source", &batch)
            .expect_err("tampering must fail");
        assert_eq!(error.code, "claim-hash-mismatch");
    }

    #[test]
    fn replication_rejects_a_registered_claim_that_violates_the_schema() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "host/source".into(),
                kind: "transport.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .unwrap();
        let mut batch = source.export_replication(0).unwrap();
        let claim = &mut batch.batches[0].claims[0];
        claim.body["fields"]["unexpected"] = Value::Bool(true);
        claim.id = claim_hash(
            &claim.batch_id,
            &claim.subject,
            &claim.kind,
            &claim.origin,
            claim.actor.as_deref(),
            &claim.body,
            &claim.predecessors,
        )
        .unwrap();

        let target = Store::open_memory("target").unwrap();
        let error = target
            .import_replication("source", &batch)
            .expect_err("a registered claim must satisfy the shared schema");
        assert_eq!(error.code, "invalid-replicated-claim");
        assert_eq!(target.index().unwrap(), 0);
    }

    #[test]
    fn an_unknown_replicated_claim_marks_its_subject_indeterminate() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "resource/future".into(),
                kind: "resource.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("kind".into(), Value::String("custom.st3.future".into())),
                    ("status".into(), Value::String("active".into())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("future".into()),
            })
            .unwrap();
        let mut batch = source.export_replication(0).unwrap();
        let claim = &mut batch.batches[0].claims[0];
        claim.kind = "future.resource-observed".into();
        claim.id = claim_hash(
            &claim.batch_id,
            &claim.subject,
            &claim.kind,
            &claim.origin,
            claim.actor.as_deref(),
            &claim.body,
            &claim.predecessors,
        )
        .unwrap();

        let target = Store::open_memory("target").unwrap();
        target.import_replication("source", &batch).unwrap();
        let status = target.status(Some("resource/future")).unwrap();
        assert_eq!(status.subjects[0].reachability, "indeterminate");
        assert!(
            status.subjects[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("future.resource-observed")
        );
    }

    #[test]
    fn replication_relays_each_origin_across_multiple_peers() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "host/source".into(),
                kind: "transport.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("source-up".into()),
            })
            .unwrap();

        let middle = Store::open_memory("middle").unwrap();
        middle
            .import_replication("source", &source.export_replication(0).unwrap())
            .unwrap();
        let relayed = middle
            .export_replication_for_heads(&BTreeMap::new())
            .unwrap();
        assert_eq!(relayed.peer, "middle");
        assert_eq!(relayed.batches[0].origin, "source");

        let target = Store::open_memory("target").unwrap();
        target.import_replication("middle", &relayed).unwrap();
        assert!(
            target
                .latest_claim("host/source", Some("transport.observed"))
                .unwrap()
                .is_some()
        );
        assert_eq!(target.replica_heads().unwrap().get("source"), Some(&1));
    }

    #[test]
    fn a_later_publish_cites_and_resolves_all_concurrent_intent_heads() {
        let left = Store::open_memory("left").unwrap();
        let initial = simple("true");
        let mission = left
            .mission(
                &initial,
                IntentInput {
                    kdl: "initial".into(),
                    source_name: None,
                },
            )
            .unwrap();
        left.apply(&initial, &mission.subject_tokens, "initial")
            .unwrap();

        let right = Store::open_memory("right").unwrap();
        right
            .import_replication("left", &left.export_replication(0).unwrap())
            .unwrap();
        let left_change = simple("false");
        let right_change = simple("printf right");
        let left_mission = left
            .mission(
                &left_change,
                IntentInput {
                    kdl: "left".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let right_mission = right
            .mission(
                &right_change,
                IntentInput {
                    kdl: "right".into(),
                    source_name: None,
                },
            )
            .unwrap();
        left.apply(&left_change, &left_mission.subject_tokens, "left")
            .unwrap();
        right
            .apply(&right_change, &right_mission.subject_tokens, "right")
            .unwrap();

        let left_heads = left.replica_heads().unwrap();
        let right_heads = right.replica_heads().unwrap();
        left.import_replication(
            "right",
            &right.export_replication_for_heads(&left_heads).unwrap(),
        )
        .unwrap();
        right
            .import_replication(
                "left",
                &left.export_replication_for_heads(&right_heads).unwrap(),
            )
            .unwrap();
        assert_eq!(
            left.status(Some("exec/work")).unwrap().subjects[0]
                .conflicts
                .len(),
            1
        );
        assert_eq!(
            right.status(Some("exec/work")).unwrap().subjects[0]
                .conflicts
                .len(),
            1
        );

        let resolved = simple("printf resolved");
        let resolution_mission = left
            .mission(
                &resolved,
                IntentInput {
                    kdl: "resolved".into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert_eq!(resolution_mission.subject_tokens["exec/work"].len(), 2);
        left.apply(&resolved, &resolution_mission.subject_tokens, "resolved")
            .unwrap();
        let right_heads = right.replica_heads().unwrap();
        right
            .import_replication(
                "left",
                &left.export_replication_for_heads(&right_heads).unwrap(),
            )
            .unwrap();
        assert!(
            left.status(Some("exec/work")).unwrap().subjects[0]
                .conflicts
                .is_empty()
        );
        assert!(
            right.status(Some("exec/work")).unwrap().subjects[0]
                .conflicts
                .is_empty()
        );
        assert_eq!(
            left.status(Some("exec/work")).unwrap().subjects[0].desired_revision,
            right.status(Some("exec/work")).unwrap().subjects[0].desired_revision
        );
    }

    #[test]
    fn capabilities_are_one_use_and_expire() {
        let store = Store::open_memory("node").unwrap();
        let (secret, _) = store
            .issue_capability("terminal", "agent/node.worker", Some("one"), 1_000)
            .unwrap();
        assert!(!store.consume_capability(&secret, "terminal").unwrap().used);
        assert!(store.consume_capability(&secret, "terminal").unwrap().used);

        let (expired, _) = store
            .issue_capability("terminal", "agent/node.worker", Some("one"), 0)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(
            store
                .consume_capability(&expired, "terminal")
                .expect_err("expired capability")
                .code,
            "expired-capability"
        );
    }

    #[test]
    fn message_lifecycle_cannot_skip_delivery_or_read() {
        let store = Store::open_memory("node").unwrap();
        let subject = "message/demo";
        let append = |kind: &str, status: &str, actor: Option<&str>, key: &str| {
            store.append_claim(&ClaimInput {
                subject: subject.into(),
                kind: kind.into(),
                actor: actor.map(str::to_owned),
                fields: BTreeMap::from([("status".into(), Value::String(status.into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(key.into()),
            })
        };
        append("message.sent", "sent", Some("requester"), "sent").unwrap();
        assert_eq!(
            append("message.closed", "closed", Some("agent/worker"), "early")
                .expect_err("close must not skip states")
                .code,
            "invalid-message-transition"
        );
        append(
            "message.delivered",
            "delivered",
            Some("agent/worker"),
            "delivered",
        )
        .unwrap();
        append("message.read", "read", Some("agent/worker"), "read").unwrap();
        append("message.closed", "closed", Some("agent/worker"), "closed").unwrap();
        assert_eq!(store.messages(None, true).unwrap()[0].status, "closed");
    }

    #[test]
    fn desired_messages_use_canonical_agent_parties_in_views() {
        let store = Store::open_memory("node").unwrap();
        let intent = crate::graph::parse_test_intent(
            r#"
version 2

  agent "mix.sup" {
    workspace "/work"
    command "sleep 60"
    restart "never"
  }
  message "kickoff" {
    from "requester"
    to "mix.sup"
    content "work"
  }

"#,
            "local",
        )
        .unwrap();
        let mission = store
            .mission(
                &intent,
                IntentInput {
                    kdl: "message".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &mission.subject_tokens, "message")
            .unwrap();

        let messages = store.messages(Some("agent/mix.sup"), true).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].from, "requester");
        assert_eq!(messages[0].to, "agent/mix.sup");
    }

    #[test]
    fn an_old_nonempty_database_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("CREATE TABLE desired(subject TEXT PRIMARY KEY)", [])
            .unwrap();
        drop(connection);

        let error = Store::open(&path, "node")
            .err()
            .expect("the old schema must be rejected");
        assert!(error.to_string().contains("unsupported st3 schema"));
    }

    #[test]
    fn schema_version_nine_requires_fresh_state_for_version_ten() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("CREATE TABLE marker(value TEXT)", [])
            .unwrap();
        connection.pragma_update(None, "user_version", 9).unwrap();
        drop(connection);

        let error = Store::open(&path, "node")
            .err()
            .expect("schema version 9 must be rejected");
        assert!(error.to_string().contains("unsupported st3 schema"));
    }

    #[test]
    fn schema_version_ten_upgrades_additively_for_mission_deadlines() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite3");
        drop(Store::open(&path, "node").unwrap());

        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DROP TABLE mission_run_deadlines", [])
            .unwrap();
        connection.pragma_update(None, "user_version", 10).unwrap();
        drop(connection);

        drop(Store::open(&path, "node").unwrap());
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
                .unwrap(),
            11
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mission_run_deadlines'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn the_current_schema_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite3");
        drop(Store::open(&path, "node").unwrap());
        Store::open(&path, "node").unwrap();
    }

    #[test]
    fn response_idempotency_caches_never_store_caller_keys() {
        let store = Store::open_memory("node").unwrap();
        let caller_key = "caller-visible-secret-key";
        store
            .cache_idempotency_response(caller_key, &json!({"ok": true}))
            .unwrap();
        assert_eq!(
            store
                .cached_idempotency_response::<Value>(caller_key)
                .unwrap(),
            Some(json!({"ok": true}))
        );
        let connection = store.connection.lock().unwrap();
        let operation_id: String = connection
            .query_row("SELECT operation_id FROM idempotency", [], |row| row.get(0))
            .unwrap();
        assert_ne!(operation_id, caller_key);
        assert_eq!(operation_id, opaque_cache_key(caller_key));
    }

    #[test]
    fn operation_projection_drift_is_detected_and_rebuilt_on_open() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite3");
        let store = Store::open(&path, "node").unwrap();
        store
            .append_client_claim(&ClaimInput {
                subject: "resource/projection".into(),
                kind: "resource.observed".into(),
                actor: None,
                fields: BTreeMap::from([(
                    "kind".into(),
                    Value::String("custom.test.projection".into()),
                )]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("projection-operation".into()),
            })
            .unwrap();
        assert!(store.operation_projection_drift().unwrap().is_empty());
        store
            .connection
            .lock()
            .unwrap()
            .execute("UPDATE operations SET state='conflict'", [])
            .unwrap();
        assert_eq!(store.operation_projection_drift().unwrap().len(), 1);
        drop(store);

        let reopened = Store::open(&path, "node").unwrap();
        assert!(reopened.operation_projection_drift().unwrap().is_empty());
    }

    #[test]
    fn work_actions_require_an_active_incarnation_bound_lease() {
        let store = Store::open_memory("node").unwrap();
        let intent = crate::graph::parse_test_intent(
            r#"
version 2

  mission "lease" state="ready" {
    goal "Complete mission lease."
    step "work" { assigned-to "agent/worker" }
  }

"#,
            "node",
        )
        .unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: "lease".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "lease-mission")
            .unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "lease".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "lease-run".into(),
            })
            .unwrap();
        let subject = &run.steps[0].subject;
        store.set_step_state(subject, "ready", None).unwrap();
        let request = |incarnation: &str, key: &str| WorkRequest {
            actor: Some("agent/node.worker".into()),
            incarnation: Some(incarnation.into()),
            summary: None,
            reason: None,
            evidence: Vec::new(),
            idempotency_key: key.into(),
        };

        let error = store
            .work_action(
                subject,
                "progress",
                &request("one", "progress-before-claim"),
            )
            .unwrap_err();
        assert_eq!(error.code, "work-not-claimed");
        store
            .work_action(subject, "claim", &request("one", "claim-one"))
            .unwrap();
        let error = store
            .work_action(subject, "progress", &request("two", "progress-two"))
            .unwrap_err();
        assert_eq!(error.code, "wrong-work-incarnation");
        store
            .work_action(subject, "progress", &request("one", "progress-one"))
            .unwrap();

        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE step_runs SET lease_expires_at_unix_ms='0' WHERE subject=?1",
                [subject],
            )
            .unwrap();
        let error = store
            .work_action(subject, "complete", &request("one", "complete-expired"))
            .unwrap_err();
        assert_eq!(error.code, "work-not-claimed");
        store
            .set_step_state(subject, "ready", Some("the worker lease expired"))
            .unwrap();
        let view = store.step_run(subject).unwrap().unwrap();
        assert!(view.claimant.is_none());
        assert!(view.claim_incarnation.is_none());
        assert!(view.claim_expires_at_unix_ms.is_none());
    }

    #[test]
    fn an_active_nested_lease_can_publish_its_parent_mission_output() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_test_intent(source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
        };
        publish(
            r#"version 2

  mission "bootstrap" state="ready" {
    goal "Complete mission bootstrap."
    step "compile" {
      assigned-to "agent/planner"
      produces-mission "project/work"
      mission "work" { goal "Complete mission work."; step "publish" { } }
    }
  }
"#,
            "nested-output-bootstrap",
        );
        publish(
            r#"version 2
 mission "project/work" state="ready" { goal "Run project work."; step "work" { } } "#,
            "nested-output-mission",
        );
        let mission = store.mission_spec("project/work", None).unwrap().unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "bootstrap".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "nested-output-run".into(),
            })
            .unwrap();
        let parent = run
            .steps
            .iter()
            .find(|step| step.step == "compile")
            .unwrap();
        let child = run
            .steps
            .iter()
            .find(|step| step.step == "compile/work/publish")
            .unwrap();
        let request = |key: &str| WorkRequest {
            actor: Some("agent/node.planner".into()),
            incarnation: Some("current".into()),
            summary: None,
            reason: None,
            evidence: Vec::new(),
            idempotency_key: key.into(),
        };
        store
            .set_step_state(&parent.subject, "ready", None)
            .unwrap();
        store
            .work_action(&parent.subject, "claim", &request("claim-parent"))
            .unwrap();
        store
            .work_action(&parent.subject, "complete", &request("complete-parent"))
            .unwrap();
        store.set_step_state(&child.subject, "ready", None).unwrap();
        store
            .work_action(&child.subject, "claim", &request("claim-child"))
            .unwrap();

        assert!(
            store
                .mission_output_authorized(&parent.subject, "agent/node.planner", Some("current"))
                .unwrap()
        );
        assert!(
            !store
                .mission_output_authorized(&parent.subject, "agent/node.planner", Some("stale"))
                .unwrap()
        );
        let output = store
            .record_mission_output(
                &parent.subject,
                "agent/node.planner",
                Some("current"),
                "project/work",
                &mission,
                "bind-nested-output",
            )
            .unwrap();
        assert_eq!(output.revision, mission.revision);
    }

    #[test]
    fn nested_work_renews_the_same_workers_ancestor_lease() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"version 2

agent "worker" { workspace "/tmp"; command "true" }

mission "nested-work" state="ready" {
  goal "Complete nested work."
  step "outer" {
    assigned-to "agent/worker"
    mission "work" {
      goal "Complete the child work."
      step "first" { }
      step "second" { depends-on { step "first" completed } }
    }
  }
}
"#;
        let intent = crate::graph::parse_test_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "nested-lease-source")
            .unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "nested-work".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "nested-lease-run".into(),
            })
            .unwrap();
        let parent = run.steps.iter().find(|step| step.step == "outer").unwrap();
        let child = run
            .steps
            .iter()
            .find(|step| step.step == "outer/work/first")
            .unwrap();
        let request = |key: &str| WorkRequest {
            actor: Some("agent/node.worker".into()),
            incarnation: Some("current".into()),
            summary: None,
            reason: None,
            evidence: Vec::new(),
            idempotency_key: key.into(),
        };
        store
            .set_step_state(&parent.subject, "ready", None)
            .unwrap();
        store
            .work_action(&parent.subject, "claim", &request("claim-parent"))
            .unwrap();
        store
            .work_action(&parent.subject, "progress", &request("progress-parent"))
            .unwrap();
        store.set_step_state(&child.subject, "ready", None).unwrap();
        store
            .work_action(&child.subject, "claim", &request("claim-child"))
            .unwrap();
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE step_runs SET lease_expires_at_unix_ms='0' WHERE subject=?1",
                [&parent.subject],
            )
            .unwrap();

        store
            .work_action(&child.subject, "complete", &request("complete-child"))
            .unwrap();

        let parent = store.step_run(&parent.subject).unwrap().unwrap();
        assert_eq!(parent.status, "working");
        assert_eq!(parent.claimant.as_deref(), Some("agent/node.worker"));
        assert_eq!(parent.claim_incarnation.as_deref(), Some("current"));
        assert!(parent.claim_expires_at_unix_ms.unwrap() > now_ms());
    }

    #[test]
    fn a_retry_records_its_backoff_boundary() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_test_intent(source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
        };
        publish(
            r#"version 2
 mission "retry" state="ready" { goal "Run retry work."; step "work" { } } "#,
            "retry-mission",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "retry".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "retry-run".into(),
            })
            .unwrap();
        let subject = &run.steps[0].subject;
        store
            .set_step_state(subject, "failed", Some("first attempt failed"))
            .unwrap();
        store.retry_step(subject, "retry policy", 60_000).unwrap();
        let view = store.step_run(subject).unwrap().unwrap();
        assert_eq!(view.status, "pending");
        assert_eq!(view.attempt, 2);
        assert!(view.not_before_unix_ms.unwrap() >= view.updated_at_unix_ms + 59_000);
    }

    #[test]
    fn a_constraint_revision_preserves_unrelated_work_and_resets_dependents() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_test_intent(source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["revision"].clone()
        };
        let first = publish(
            r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "revision" state="ready" {
    goal "Complete mission revision."
    step "owned" { assigned-to "agent/worker"; goal "First goal." }
    step "unrelated" { }
    step "join" { depends-on { step "owned" completed; step "unrelated" completed } }
  }

"#,
            "revision-one",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "revision-run".into(),
            })
            .unwrap();
        for step in &run.steps {
            store
                .set_step_state(&step.subject, "completed", None)
                .unwrap();
        }
        let second = publish(
            r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "revision" state="ready" {
    goal "Complete mission revision."
    step "owned" {
      assigned-to "agent/worker"
      goal "First goal."
      constraint "Do not push."
    }
    step "unrelated" { }
    step "join" { depends-on { step "owned" completed; step "unrelated" completed } }
  }

"#,
            "revision-two",
        );
        let revised = store
            .adopt_mission_revision(
                &run.id,
                &second,
                "agent/node.worker",
                "the work needs a new constraint",
                "adopt-revision-two",
            )
            .unwrap();
        let states = revised
            .steps
            .iter()
            .map(|step| (step.step.as_str(), step.status.as_str()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(states["owned"], "pending");
        assert_eq!(states["join"], "pending");
        assert_eq!(states["unrelated"], "completed");
        assert_eq!(
            revised
                .steps
                .iter()
                .find(|step| step.step == "owned")
                .unwrap()
                .constraints,
            ["Do not push."]
        );
        assert_eq!(revised.root_revision, run.root_revision);
        assert_eq!(revised.revision, second.revision);
    }

    #[test]
    fn a_revision_keeps_the_old_generation_and_fences_its_work() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_test_intent(source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["generation"].clone()
        };
        let first = publish(
            r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "generation" state="ready" {
    goal "Test immutable generations."
    step "active" { assigned-to "agent/worker"; goal "Keep this definition." }
    step "stable" { goal "Carry this result." }
    step "changed" { goal "Use the first definition." }
    step "dependent" {
      depends-on { step "changed" completed }
      goal "Depend on the changed step."
    }
  }

"#,
            "generation-one",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "generation-run".into(),
            })
            .unwrap();
        let active = run
            .steps
            .iter()
            .find(|step| step.step == "active")
            .unwrap()
            .subject
            .clone();
        store.set_step_state(&active, "ready", None).unwrap();
        let request = |key: &str| WorkRequest {
            actor: Some("agent/node.worker".into()),
            incarnation: Some("worker-one".into()),
            summary: None,
            reason: None,
            evidence: Vec::new(),
            idempotency_key: key.into(),
        };
        store
            .work_action(&active, "claim", &request("generation-claim"))
            .unwrap();
        store
            .work_action(&active, "progress", &request("generation-progress"))
            .unwrap();
        for path in ["stable", "changed", "dependent"] {
            let subject = &run
                .steps
                .iter()
                .find(|step| step.step == path)
                .unwrap()
                .subject;
            store.set_step_state(subject, "completed", None).unwrap();
        }
        let second = publish(
            r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "generation" state="ready" {
    goal "Test immutable generations."
    step "active" { assigned-to "agent/worker"; goal "Keep this definition." }
    step "stable" { goal "Carry this result." }
    step "changed" { goal "Use the second definition." }
    step "dependent" {
      depends-on { step "changed" completed }
      goal "Depend on the changed step."
    }
  }

"#,
            "generation-two",
        );
        let revised = store
            .adopt_mission_revision(
                &run.id,
                &second,
                "agent/node.worker",
                "the work now needs the second definition",
                "generation-cutover",
            )
            .unwrap();

        assert_ne!(revised.generation, run.generation);
        assert_eq!(revised.initial_revision, run.revision);
        let generations = store.run_generations(&run.id).unwrap();
        assert_eq!(generations.len(), 2);
        assert_eq!(generations[0].subject, run.generation);
        assert_eq!(generations[0].status, "superseded");
        assert_eq!(
            generations[0]
                .steps
                .iter()
                .find(|step| step.step == "active")
                .unwrap()
                .status,
            "working"
        );
        assert_eq!(
            generations[1].predecessor.as_deref(),
            Some(run.generation.as_str())
        );
        let states = revised
            .steps
            .iter()
            .map(|step| (step.step.as_str(), step.status.as_str()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(states["active"], "ready");
        assert_eq!(states["stable"], "completed");
        assert_eq!(states["changed"], "pending");
        assert_eq!(states["dependent"], "pending");
        assert!(
            revised
                .steps
                .iter()
                .all(|step| step.generation == revised.generation)
        );
        let error = store
            .work_action(&active, "complete", &request("stale-generation-complete"))
            .unwrap_err();
        assert_eq!(error.code, "stale-run-generation");
        assert!(!store.set_step_state(&active, "completed", None).unwrap());
        assert_eq!(store.step_run(&active).unwrap().unwrap().status, "working");
        let retried = store
            .adopt_mission_revision(
                &run.id,
                &second,
                "agent/node.worker",
                "the work now needs the second definition",
                "generation-cutover",
            )
            .unwrap();
        assert_eq!(retried.generation, revised.generation);
    }

    #[test]
    fn assigned_work_does_not_grant_revision_authority() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_test_intent(source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["authority"].clone()
        };
        let first = publish(
            r#"
version 2

  mission "authority" state="ready" {
    goal "Keep revision authority structural."
    step "work" { assigned-to "agent/node.worker"; goal "Use the first goal." }
  }

"#,
            "authority-one",
        );
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "authority-run".into(),
            })
            .unwrap();
        let escalated = publish(
            r#"
version 2

  mission "authority" state="ready" {
    goal "Keep revision authority structural."
    step "work" {
      assigned-to "agent/node.worker"
      goal "Use the second goal."
       agent "worker" { workspace "."; command "true" }
    }
  }

"#,
            "authority-two",
        );
        let error = store
            .adopt_mission_revision(
                &run.id,
                &escalated,
                "agent/node.worker",
                "grant authority in the candidate graph",
                "authority-cutover",
            )
            .unwrap_err();
        assert_eq!(error.code, "revision-outside-graph-location");
    }

    #[test]
    fn a_mission_selector_or_completion_change_restarts_carried_work() {
        let parse = |selector: &str, completion: &str| {
            let source = format!(
                r#"
version 2

  mission "contract" state="ready" {{
    goal "Use the current mission contract."
    {selector}
    {completion}
    step "work" {{ }}
  }}

"#
            );
            parse_intent(&source, "node").unwrap().missions["contract"].clone()
        };
        let old = parse(
            "assigned-to \"agent/node.one\"",
            "completion { when \"all-steps-exhausted\" }",
        );
        let selector_changed = parse(
            "assigned-to \"agent/node.two\"",
            "completion { when \"all-steps-exhausted\" }",
        );
        let completion_changed = parse("assigned-to \"agent/node.one\"", "");
        let variables = BTreeMap::new();
        for candidate in [&selector_changed, &completion_changed] {
            let (compatible, _) = analyze_mission_revision(
                &old,
                candidate,
                "person/requester",
                "person/requester",
                &variables,
            )
            .unwrap();
            if std::ptr::eq(candidate, &selector_changed) {
                assert!(compatible.is_empty());
            } else {
                assert_eq!(compatible, BTreeSet::from(["work".into()]));
            }
        }
    }

    #[test]
    fn all_affected_human_reviewers_approve_the_exact_revision_preview() {
        let store = Store::open_memory("node").unwrap();
        let publish = |goal: &str, key: &str| {
            let source = format!(
                r#"
version 2

  mission "protected" state="ready" revisions="human-only" revision-reviewer="person/mission-reviewer" {{
    goal "Test protected revision."
     agent "worker" {{ workspace "."; command "true" }}
    step "work" revisions="human-only" revision-reviewer="person/step-reviewer" {{
      assigned-to "agent/worker"
      goal {goal:?}
    }}
  }}

"#
            );
            let intent = crate::graph::parse_test_intent(&source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source,
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["protected"].clone()
        };
        let first = publish("Use the first goal.", "protected-one");
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/requester".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "protected-run".into(),
            })
            .unwrap();
        let second = publish("Use the second goal.", "protected-two");
        let proposal = store
            .create_revision_proposal(
                &run.id,
                &second,
                "agent/node.worker",
                "the first goal is incomplete",
                "protected-proposal",
            )
            .unwrap();
        assert_eq!(proposal.status, "pending-approval");
        assert_eq!(
            proposal.reviewers,
            vec!["person/mission-reviewer", "person/step-reviewer"]
        );
        let error = store
            .approve_revision_proposal(
                &proposal.id,
                "person/mission-reviewer",
                "wrong-preview",
                "protected-wrong-preview",
            )
            .unwrap_err();
        assert_eq!(error.code, "stale-revision-preview");
        let first_approval = store
            .approve_revision_proposal(
                &proposal.id,
                "person/mission-reviewer",
                proposal.preview_hash.as_deref().unwrap(),
                "protected-first-approval",
            )
            .unwrap();
        assert_eq!(first_approval.status, "pending-approval");
        assert_eq!(first_approval.mission_run.generation, run.generation);
        let applied = store
            .approve_revision_proposal(
                &proposal.id,
                "person/step-reviewer",
                proposal.preview_hash.as_deref().unwrap(),
                "protected-final-approval",
            )
            .unwrap();
        assert_eq!(applied.status, "applied");
        assert_ne!(applied.mission_run.generation, run.generation);
        assert_eq!(
            applied
                .proposal
                .as_ref()
                .unwrap()
                .successor_generation
                .as_deref(),
            Some(applied.mission_run.generation.as_str())
        );
        let retried = store
            .approve_revision_proposal(
                &proposal.id,
                "person/step-reviewer",
                proposal.preview_hash.as_deref().unwrap(),
                "protected-final-approval-retry",
            )
            .unwrap();
        assert_eq!(
            retried.mission_run.generation,
            applied.mission_run.generation
        );
    }

    #[test]
    fn cancelling_a_revision_proposal_reopens_the_run_for_one_replacement() {
        let store = Store::open_memory("node").unwrap();
        let publish = |goal: &str, key: &str| {
            let source = format!(
                r#"
version 2

  mission "protected" state="ready" revisions="human-only" {{
    goal "Test revision cancellation."
    step "work" {{ goal {goal:?} }}
  }}

"#
            );
            let intent = crate::graph::parse_test_intent(&source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source,
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["protected"].clone()
        };
        let first = publish("Use the first goal.", "cancel-one");
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/requester".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "cancel-run".into(),
            })
            .unwrap();
        let second = publish("Use the second goal.", "cancel-two");
        let proposal = store
            .create_revision_proposal(
                &run.id,
                &second,
                "person/requester",
                "the first goal is incomplete",
                "cancel-proposal",
            )
            .unwrap();

        let error = store
            .create_revision_proposal(
                &run.id,
                &second,
                "person/requester",
                "replace the pending proposal",
                "cancel-second-pending",
            )
            .unwrap_err();
        assert_eq!(error.code, "revision-proposal-already-pending");
        let error = store
            .cancel_revision_proposal(
                &proposal.id,
                "agent/node.outsider",
                Some("not authorized"),
                "cancel-unauthorized",
            )
            .unwrap_err();
        assert_eq!(error.code, "revision-cancel-not-authorized");

        let cancelled = store
            .cancel_revision_proposal(
                &proposal.id,
                "person/requester",
                Some("the replacement needs another edit"),
                "cancel-authorized",
            )
            .unwrap();
        assert_eq!(cancelled.status, "cancelled");
        assert_eq!(store.mission_run(&run.id).unwrap().unwrap().phase, "normal");
        assert_eq!(store.run_generations(&run.id).unwrap().len(), 1);

        let replacement = store
            .create_revision_proposal(
                &run.id,
                &second,
                "person/requester",
                "the edited replacement is ready",
                "cancel-replacement",
            )
            .unwrap();
        assert_eq!(replacement.status, "pending-approval");
        assert_ne!(replacement.id, proposal.id);
    }

    #[test]
    fn a_when_idle_revision_drains_without_polling_for_new_work() {
        let store = Store::open_memory("node").unwrap();
        let publish = |cutover: Option<&str>, goal: &str, key: &str| {
            let cutover = cutover
                .map(|value| format!(" revision-cutover={value:?}"))
                .unwrap_or_default();
            let source = format!(
                r#"
version 2

  agent "worker" {{ workspace "."; command "true" }}
  mission "drain" state="ready"{cutover} {{
    goal "Test a drained cutover."
    step "active" {{ assigned-to "agent/worker"; goal "Keep active work." }}
    step "waiting" {{ assigned-to "agent/worker"; goal {goal:?} }}
  }}

"#
            );
            let intent = crate::graph::parse_test_intent(&source, "node").unwrap();
            let planned = store
                .mission(
                    &intent,
                    IntentInput {
                        kdl: source,
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.missions["drain"].clone()
        };
        let first = publish(Some("when-idle"), "Use the first goal.", "drain-one");
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "drain-run".into(),
            })
            .unwrap();
        let active = run.steps.iter().find(|step| step.step == "active").unwrap();
        let waiting = run
            .steps
            .iter()
            .find(|step| step.step == "waiting")
            .unwrap();
        store
            .set_step_state(&active.subject, "ready", None)
            .unwrap();
        store
            .set_step_state(&waiting.subject, "ready", None)
            .unwrap();
        let request = |key: &str| WorkRequest {
            actor: Some("agent/node.worker".into()),
            incarnation: Some("worker-one".into()),
            summary: None,
            reason: None,
            evidence: Vec::new(),
            idempotency_key: key.into(),
        };
        let child_source = r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "drain-child" state="ready" {
    goal "Keep descendant work in the idle boundary."
    step "active" { assigned-to "agent/worker" }
  }

"#;
        let child_intent = crate::graph::parse_test_intent(child_source, "node").unwrap();
        let child_planned = store
            .mission(
                &child_intent,
                IntentInput {
                    kdl: child_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&child_intent, &child_planned.subject_tokens, "drain-child")
            .unwrap();
        let child = store
            .create_child_mission_run(
                &MissionRunRequest {
                    mission: "drain-child".into(),
                    revision: None,
                    workspace: "/tmp".into(),
                    requester: Some("person/test".into()),
                    mode: Some("run".into()),
                    inputs: BTreeMap::new(),
                    idempotency_key: "drain-child-run".into(),
                },
                &run,
                &waiting.subject,
                None,
            )
            .unwrap();
        let child_active = &child.steps[0];
        store
            .set_step_state(&child_active.subject, "ready", None)
            .unwrap();
        store
            .work_action(
                &child_active.subject,
                "claim",
                &request("drain-child-claim"),
            )
            .unwrap();
        store
            .work_action(
                &child_active.subject,
                "progress",
                &request("drain-child-progress"),
            )
            .unwrap();
        store
            .work_action(&active.subject, "claim", &request("drain-claim"))
            .unwrap();
        store
            .work_action(&active.subject, "progress", &request("drain-progress"))
            .unwrap();
        let second = publish(Some("restart-active"), "Use the second goal.", "drain-two");
        let error = store
            .adopt_mission_revision(
                &run.id,
                &second,
                "agent/node.worker",
                "skip the current cutover rule",
                "drain-direct-adopt",
            )
            .unwrap_err();
        assert_eq!(error.code, "revision-needs-drained-cutover");
        let proposal = store
            .create_revision_proposal(
                &run.id,
                &second,
                "agent/node.worker",
                "wait for active work",
                "drain-proposal",
            )
            .unwrap();
        assert_eq!(proposal.status, "draining");
        assert_eq!(proposal.cutover, RevisionCutover::WhenIdle);
        assert_eq!(
            store.mission_run(&run.id).unwrap().unwrap().phase,
            "revision-draining"
        );
        assert_eq!(store.work(None, false).unwrap().len(), 2);
        let error = store
            .work_action(&waiting.subject, "claim", &request("drain-blocked-claim"))
            .unwrap_err();
        assert_eq!(error.code, "run-generation-draining");
        assert!(store.apply_drained_revision(&run.id).unwrap().is_none());
        store
            .work_action(&active.subject, "complete", &request("drain-complete"))
            .unwrap();
        assert!(store.apply_drained_revision(&run.id).unwrap().is_none());
        store
            .set_step_state(&active.subject, "completed", None)
            .unwrap();
        assert!(store.apply_drained_revision(&run.id).unwrap().is_none());
        store
            .work_action(
                &child_active.subject,
                "complete",
                &request("drain-child-complete"),
            )
            .unwrap();
        store
            .set_step_state(&child_active.subject, "completed", None)
            .unwrap();
        let applied = store
            .apply_drained_revision(&run.id)
            .unwrap()
            .expect("the completion event permits cutover");
        assert_eq!(applied.status, "applied");
        assert_eq!(applied.mission_run.phase, "normal");
        assert_ne!(applied.mission_run.generation, run.generation);
    }

    #[test]
    fn graph_cancellation_enters_final_work_and_is_idempotent() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"
version 2

  agent "worker" { workspace "."; command "true" }
  mission "cancel" state="ready" {
    goal "Cancel this mission through the graph."
    completion { when "all-steps-exhausted" }
    step "work" { assigned-to "agent/node.worker" }
    finally { step "cleanup" { agentless } }
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "publish-cancel-mission")
            .unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "cancel".into(),
                revision: None,
                workspace: ".".into(),
                requester: Some("person/test".into()),
                mode: None,
                inputs: BTreeMap::new(),
                idempotency_key: "run-cancel-mission".into(),
            })
            .unwrap();
        store
            .append_claim(&ClaimInput {
                subject: "agent/node.worker".into(),
                kind: "harness.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("state".into(), Value::String("ready".into())),
                    (
                        "incarnation_id".into(),
                        Value::String("worker-incarnation".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("cancel-worker-ready".into()),
            })
            .unwrap();
        store
            .set_step_state(
                &run.steps
                    .iter()
                    .find(|step| step.step == "work")
                    .unwrap()
                    .subject,
                "ready",
                None,
            )
            .unwrap();
        store
            .work_action(
                &run.steps
                    .iter()
                    .find(|step| step.step == "work")
                    .unwrap()
                    .subject,
                "claim",
                &WorkRequest {
                    actor: Some("agent/node.worker".into()),
                    incarnation: Some("worker-incarnation".into()),
                    summary: None,
                    reason: None,
                    evidence: Vec::new(),
                    idempotency_key: "claim-cancel-work".into(),
                },
            )
            .unwrap();
        let child_source = r#"
version 2

  mission "cancel-child" state="ready" {
    goal "Cancel with the parent run."
    step "child-work" { agentless }
  }

"#;
        let child_intent = parse_intent(child_source, "node").unwrap();
        let child_planned = store
            .mission(
                &child_intent,
                IntentInput {
                    kdl: child_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(
                &child_intent,
                &child_planned.subject_tokens,
                "publish-cancel-child",
            )
            .unwrap();
        let child = store
            .create_child_mission_run(
                &MissionRunRequest {
                    mission: "cancel-child".into(),
                    revision: None,
                    workspace: ".".into(),
                    requester: Some("person/test".into()),
                    mode: None,
                    inputs: BTreeMap::new(),
                    idempotency_key: "run-cancel-child".into(),
                },
                &run,
                &run.steps[0].subject,
                None,
            )
            .unwrap();
        let cancellation = format!(
            "version 2\n mission-run {:?} {{ cancellation \"operator\" {{ reason \"the test cancelled the run\" }} }} \n",
            run.subject
        );
        let intent = parse_intent(&cancellation, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: cancellation,
                    source_name: None,
                },
            )
            .unwrap();
        let first = store
            .apply(&intent, &planned.subject_tokens, "cancel-mission-run")
            .unwrap();
        let repeated = store
            .apply(&intent, &planned.subject_tokens, "cancel-mission-run")
            .unwrap();
        assert_eq!(first.batch_id, repeated.batch_id);
        let cancelled = store.mission_run(&run.id).unwrap().unwrap();
        assert_eq!(cancelled.status, "running");
        assert_eq!(cancelled.phase, "final-cancelled");
        assert_eq!(
            store.mission_run(&child.id).unwrap().unwrap().phase,
            "cleanup-cancelled"
        );
        assert!(
            !store
                .set_mission_run_state(
                    &child.id,
                    "standing",
                    "normal",
                    Some("a stale evaluator tried to reopen the run"),
                )
                .unwrap()
        );
        assert_eq!(
            store.mission_run(&child.id).unwrap().unwrap().phase,
            "cleanup-cancelled"
        );
        assert!(
            !store
                .set_mission_run_state(
                    &run.id,
                    "standing",
                    "normal",
                    Some("a stale evaluator tried to reopen the run"),
                )
                .unwrap()
        );
        assert_eq!(
            store.mission_run(&run.id).unwrap().unwrap().phase,
            "final-cancelled"
        );
        assert_eq!(
            cancelled
                .steps
                .iter()
                .find(|step| step.step == "work")
                .unwrap()
                .status,
            "cancelled"
        );
        assert_eq!(
            cancelled
                .steps
                .iter()
                .find(|step| step.step == "cleanup")
                .unwrap()
                .status,
            "pending"
        );
        let cancellation_message = store
            .messages(None, true)
            .unwrap()
            .into_iter()
            .find(|message| message.title.as_deref() == Some("Mission work cancelled"))
            .expect("the claimant receives a cancellation message");
        assert_eq!(cancellation_message.from, "daemon/runtime");
        assert_eq!(cancellation_message.to, "agent/node.worker");
    }

    #[test]
    fn preview_recognizes_a_run_scoped_selector_for_a_declared_agent() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"version 2
mission "work" state="ready" {
  goal "Complete the available work."
  agent "worker" {
    identity "fleet.worker"
    workspace "."
    command "true"
  }
  step "do-work" {
    assigned-to "agent/${ST_MISSION_RUN}/fleet.worker"
  }
}"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert!(
            planned
                .warnings
                .iter()
                .all(|warning| !warning.contains("missing eligible agent")),
            "{:?}",
            planned.warnings
        );
    }

    #[test]
    fn repository_collections_create_one_typed_resource_for_each_new_item() {
        let previous = json!({"pull_requests": [], "issues": []});
        let current = json!({
            "pull_requests": [{"number": 7, "url": "https://example.test/pull/7", "title": "Ready"}],
            "issues": [{"number": 8, "url": "https://example.test/issues/8", "title": "Bug"}],
        });
        let pulls = discovered_collection_items(
            "resource/github/acme/demo",
            "pull_requests",
            Some(&previous),
            &current,
        );
        assert_eq!(pulls.len(), 1);
        assert_eq!(pulls[0].0, "resource/github/acme/demo/pull-request/7");
        assert_eq!(pulls[0].1, "vcs.pull-request");
        assert_eq!(pulls[0].2["repository"], "resource/github/acme/demo");
        assert_eq!(pulls[0].2["draft"], false);

        let issues = discovered_collection_items(
            "resource/github/acme/demo",
            "issues",
            Some(&previous),
            &current,
        );
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].0, "resource/github/acme/demo/issue/8");
        assert_eq!(issues[0].1, "vcs.issue");
        assert_eq!(issues[0].2["repository"], "resource/github/acme/demo");
    }

    #[test]
    fn resource_observations_establish_a_baseline_and_send_selected_changes_once() {
        let store = Store::open_memory("node").unwrap();
        let source = r#"
version 2

  agent "one" { workspace "."; command "true" }
  agent "two" { workspace "."; command "true" }
  resource "github/acme/demo/pull/1" { kind "vcs.pull-request" }
  observer "github/acme/demo/pull/1" {
    resource "resource/github/acme/demo/pull/1"
    provider "github.pull-request"
    locator "acme/demo#1"
    field "head"
    field "state"
    field "checks"
  }
  subscription "one" {
    observer "observer/github/acme/demo/pull/1"
    to "agent/node.one"
    on "state"
    delivery "message"
  }
  subscription "two" {
    observer "observer/github/acme/demo/pull/1"
    to "agent/node.two"
    on "state"
    delivery "message"
  }
  subscription "missing" {
    observer "observer/github/acme/demo/pull/1"
    to "agent/node.missing"
    on "state"
    delivery "message"
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert!(planned.blockers.is_empty());
        assert!(
            planned
                .warnings
                .iter()
                .any(|warning| warning.contains("agent/node.missing"))
        );
        store
            .apply(&intent, &planned.subject_tokens, "publish-watch")
            .unwrap();
        let subscriptions = store
            .desired_subjects()
            .unwrap()
            .into_iter()
            .filter(|desired| desired.kind == "subscription")
            .map(|desired| {
                let spec = crate::graph::subscription_spec(&desired.desired).unwrap();
                (desired.subject, spec)
            })
            .collect::<Vec<_>>();
        let observer_revision = store
            .selected_desired_revision("observer/github/acme/demo/pull/1")
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .record_resource_observation(
                    "observer/github/acme/demo/pull/1",
                    "stale-revision",
                    None,
                    "resource/github/acme/demo/pull/1",
                    Some("stale-cursor"),
                    &json!({"head": "stale"}),
                    50,
                    &subscriptions,
                )
                .unwrap_err()
                .code,
            "stale-observer-revision"
        );
        let baseline = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-one"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "open",
                    "checks": ["pending"]
                }),
                100,
                &subscriptions,
            )
            .unwrap();
        assert!(baseline.baseline);
        assert!(baseline.message_subjects.is_empty());
        assert_eq!(
            store
                .claims_for(
                    "resource/github/acme/demo/pull/1",
                    Some("resource.observed")
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .latest_actual_value("subscription/missing")
                .unwrap()
                .unwrap()["state"],
            "pending"
        );
        let unchanged = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-one"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "open",
                    "checks": ["pending"]
                }),
                200,
                &subscriptions,
            )
            .unwrap();
        assert!(unchanged.changed_fields.is_empty());
        assert!(unchanged.observation_claim.is_none());
        let changed = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-two"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "closed",
                    "checks": ["pending"]
                }),
                300,
                &subscriptions,
            )
            .unwrap();
        assert_eq!(changed.changed_fields, ["state"]);
        assert_eq!(changed.message_subjects.len(), 2);
        let retry = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-two"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "closed",
                    "checks": ["pending"]
                }),
                300,
                &subscriptions,
            )
            .unwrap();
        assert_eq!(retry.message_subjects, changed.message_subjects);
        assert_eq!(store.messages(None, true).unwrap().len(), 2);
        let stop_source = "version 2\n subscription \"one\" { stop } \n";
        let stop_intent = parse_intent(stop_source, "node").unwrap();
        let stop_mission = store
            .mission(
                &stop_intent,
                IntentInput {
                    kdl: stop_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(
                &stop_intent,
                &stop_mission.subject_tokens,
                "stop-one-subscription",
            )
            .unwrap();
        let after_stop = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-after-stop"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "open",
                    "checks": ["pending"]
                }),
                350,
                &subscriptions,
            )
            .unwrap();
        assert_eq!(after_stop.message_subjects.len(), 1);
        assert_eq!(store.messages(None, true).unwrap().len(), 3);
        let unselected = store
            .record_resource_observation(
                "observer/github/acme/demo/pull/1",
                &observer_revision,
                None,
                "resource/github/acme/demo/pull/1",
                Some("cursor-three"),
                &json!({
                    "head": "resource/github/acme/demo/ref/a",
                    "state": "open",
                    "checks": ["passing"]
                }),
                400,
                &subscriptions,
            )
            .unwrap();
        assert_eq!(unselected.changed_fields, ["checks"]);
        assert!(unselected.message_subjects.is_empty());
    }

    #[test]
    fn a_graph_wide_idempotency_key_returns_the_original_claim() {
        let store = Store::open_memory("node").unwrap();
        let request = ClaimInput {
            subject: "custom/test/fact".into(),
            kind: "custom.test.observed".into(),
            actor: Some("person/tester".into()),
            fields: BTreeMap::from([("value".into(), json!(1))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some("one-operation".into()),
        };
        let first = store.append_claim(&request).unwrap();
        let retry = store.append_claim(&request).unwrap();
        assert_eq!(retry.id, first.id);
        assert_eq!(retry.operation_id, first.operation_id);
        assert_eq!(store.claims_for(&request.subject, None).unwrap().len(), 1);

        let mut changed = request.clone();
        changed.fields.insert("value".into(), json!(2));
        let error = store.append_claim(&changed).unwrap_err();
        assert_eq!(error.code, "idempotency-mismatch");
    }

    #[test]
    fn matching_replica_operations_converge_without_a_second_event() {
        let left = Store::open_memory("left").unwrap();
        let right = Store::open_memory("right").unwrap();
        let request = ClaimInput {
            subject: "custom/test/fact".into(),
            kind: "custom.test.observed".into(),
            actor: None,
            fields: BTreeMap::from([("value".into(), json!(1))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some("replicated-operation".into()),
        };
        let left_claim = left.append_claim(&request).unwrap();
        let right_claim = right.append_claim(&request).unwrap();
        assert_ne!(left_claim.id, right_claim.id);
        assert_eq!(left_claim.operation_id, right_claim.operation_id);

        left.import_replication("right", &right.export_replication(0).unwrap())
            .unwrap();
        let retry = left.append_claim(&request).unwrap();
        assert_eq!(
            retry.id,
            std::cmp::min(left_claim.id.clone(), right_claim.id.clone())
        );
        assert_eq!(left.events_after(0, None).unwrap().len(), 1);
    }

    #[test]
    fn conflicting_replica_operations_make_the_subject_indeterminate() {
        let left = Store::open_memory("left").unwrap();
        let right = Store::open_memory("right").unwrap();
        let request = |value| ClaimInput {
            subject: "custom/test/fact".into(),
            kind: "custom.test.observed".into(),
            actor: None,
            fields: BTreeMap::from([("value".into(), json!(value))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some("conflicting-operation".into()),
        };
        left.append_claim(&request(1)).unwrap();
        right.append_claim(&request(2)).unwrap();
        left.import_replication("right", &right.export_replication(0).unwrap())
            .unwrap();
        assert!(left.operation_projection_drift().unwrap().is_empty());

        let error = left.append_claim(&request(1)).unwrap_err();
        assert_eq!(error.code, "idempotency-conflict");
        let status = left.status(Some("custom/test/fact")).unwrap();
        assert_eq!(status.subjects[0].reachability, "indeterminate");
        assert!(
            status.subjects[0]
                .reason
                .as_deref()
                .is_some_and(|value| value.contains("idempotency-conflict"))
        );
    }

    #[test]
    fn public_claims_cannot_impersonate_system_producers() {
        let store = Store::open_memory("node").unwrap();
        let error = store
            .append_client_claim(&ClaimInput {
                subject: "host/peer".into(),
                kind: "transport.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .unwrap_err();
        assert_eq!(error.code, "claim-write-forbidden");
    }

    #[test]
    fn an_agent_can_change_its_account_without_account_existence_coupling() {
        let store = Store::open_memory("node").unwrap();
        let association = |account: &str, actor: &str| ClaimInput {
            subject: "agent/run/worker".into(),
            kind: "agent.account".into(),
            actor: Some(actor.into()),
            fields: BTreeMap::from([("account".into(), Value::String(account.into()))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        };

        store
            .append_client_claim(&association(
                "account/claude/not-declared",
                "agent/run/worker",
            ))
            .unwrap();
        store
            .append_client_claim(&association("account/claude/team-a", "agent/run/worker"))
            .unwrap();
        assert_eq!(
            store
                .latest_actual_value("agent/run/worker")
                .unwrap()
                .unwrap()["account"],
            "account/claude/team-a"
        );
        assert_eq!(
            store
                .append_client_claim(&association("account/claude/team-b", "agent/run/other",))
                .unwrap_err()
                .code,
            "claim-write-forbidden"
        );
    }

    #[test]
    fn once_cardinality_rejects_a_second_local_claim() {
        let store = Store::open_memory("node").unwrap();
        let request = ClaimInput {
            subject: "mission-run/one".into(),
            kind: "mission-run.created".into(),
            actor: None,
            fields: BTreeMap::new(),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        };
        store.append_claim(&request).unwrap();
        assert_eq!(
            store.append_claim(&request).unwrap_err().code,
            "claim-cardinality"
        );
    }

    #[test]
    fn replicated_once_claims_preserve_concurrent_history() {
        let left = Store::open_memory("left").unwrap();
        let right = Store::open_memory("right").unwrap();
        for (store, verdict) in [(&left, "pass"), (&right, "fail")] {
            store
                .append_claim(&ClaimInput {
                    subject: "mission-run/shared-eval".into(),
                    kind: "eval.verdict".into(),
                    actor: None,
                    fields: BTreeMap::from([("verdict".into(), Value::String(verdict.into()))]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                })
                .unwrap();
        }

        let target = Store::open_memory("target").unwrap();
        target
            .import_replication("left", &left.export_replication(0).unwrap())
            .unwrap();
        target
            .import_replication("right", &right.export_replication(0).unwrap())
            .unwrap();
        assert_eq!(
            target
                .claims_for("mission-run/shared-eval", Some("eval.verdict"))
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn resource_kind_and_immutable_facts_are_enforced() {
        let store = Store::open_memory("node").unwrap();
        let observed = |fields| ClaimInput {
            subject: "resource/commit".into(),
            kind: "resource.observed".into(),
            actor: None,
            fields,
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        };
        store
            .append_client_claim(&observed(BTreeMap::from([
                ("kind".into(), Value::String("vcs.commit".into())),
                ("sha".into(), Value::String("abc".into())),
                ("state".into(), Value::String("published".into())),
            ])))
            .unwrap();
        let second = store
            .append_client_claim(&observed(BTreeMap::from([(
                "state".into(),
                Value::String("verified".into()),
            )])))
            .unwrap();
        assert_eq!(second.body["fields"]["kind"], "vcs.commit");
        assert_eq!(
            store
                .append_client_claim(&observed(BTreeMap::from([(
                    "sha".into(),
                    Value::String("def".into()),
                )])))
                .unwrap_err()
                .code,
            "immutable-resource-field"
        );
        assert_eq!(
            store
                .append_client_claim(&observed(BTreeMap::from([(
                    "kind".into(),
                    Value::String("ci.run".into()),
                )])))
                .unwrap_err()
                .code,
            "immutable-resource-kind"
        );
    }
}
