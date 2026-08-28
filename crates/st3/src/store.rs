use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::model::{
    ApplyResponse, Capability, ChangePolicy, ClaimInput, ClaimRecord, ClaimsPage, DependencySpec,
    DesiredSubject, DocumentVersion, EventRecord, IntentInput, MessageView, NormalizedIntent,
    PlanResponse, PlanRunRequest, PlanRunView, PlanSpec, PlanState, PlannedAction, ReplicaBatch,
    ReplicaRange, ReplicationBatch, ReplicationResponse, St3Error, StatusResponse, StepRunView,
    SubjectChange, SubjectStatus, WorkRequest,
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
    activation TEXT,
    scopes TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    response TEXT NOT NULL
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

CREATE TABLE IF NOT EXISTS plan_revisions (
    plan_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    state TEXT NOT NULL,
    body TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claims(id),
    created_index INTEGER NOT NULL,
    PRIMARY KEY(plan_id, revision)
);

CREATE TABLE IF NOT EXISTS plan_definitions (
    plan_id TEXT PRIMARY KEY,
    revision TEXT NOT NULL,
    state TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claims(id)
);

CREATE TABLE IF NOT EXISTS plan_runs (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    root_revision TEXT NOT NULL,
    workspace TEXT NOT NULL,
    requester TEXT NOT NULL,
    run_scope TEXT,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS plan_runs_plan_index ON plan_runs(plan_id, created_at_unix_ms);

CREATE TABLE IF NOT EXISTS step_runs (
    subject TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES plan_runs(id),
    step_path TEXT NOT NULL,
    definition_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    assignee TEXT,
    title TEXT,
    goal TEXT,
    worker_reported INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_incarnation TEXT,
    lease_expires_at_unix_ms TEXT,
    blocked_reason TEXT,
    not_before_unix_ms TEXT,
    activated_at_unix_ms TEXT,
    created_at_unix_ms TEXT NOT NULL,
    updated_at_unix_ms TEXT NOT NULL,
    UNIQUE(run_id, step_path)
);
CREATE INDEX IF NOT EXISTS step_runs_run_index ON step_runs(run_id, step_path);
CREATE INDEX IF NOT EXISTS step_runs_assignee_index ON step_runs(assignee, status);
PRAGMA user_version = 3;
"#;

pub struct Store {
    connection: Mutex<Connection>,
    origin: String,
}

fn reject_old_schema(connection: &Connection) -> Result<()> {
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let table_count: u32 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        table_count == 0 || version == 3,
        "this database uses an unsupported pre-plan st3 schema; start with a new state directory"
    );
    anyhow::ensure!(
        version == 0 || version == 3,
        "this database uses unsupported st3 schema version {version}"
    );
    Ok(())
}

impl Store {
    pub fn open(path: &Path, origin: impl Into<String>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("open st3 database {}", path.display()))?;
        reject_old_schema(&connection)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
            origin: origin.into(),
        })
    }

    pub fn open_memory(origin: impl Into<String>) -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        reject_old_schema(&connection)?;
        connection.execute_batch(SCHEMA)?;
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

    pub fn plan_spec(&self, plan_id: &str, revision: Option<&str>) -> Result<Option<PlanSpec>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let body = if let Some(revision) = revision {
            connection
                .query_row(
                    "SELECT body FROM plan_revisions WHERE plan_id=?1 AND revision=?2",
                    params![plan_id, revision],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        } else {
            connection
                .query_row(
                    "SELECT r.body FROM plan_definitions d JOIN plan_revisions r ON r.plan_id=d.plan_id AND r.revision=d.revision WHERE d.plan_id=?1",
                    [plan_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        };
        body.map(|body| serde_json::from_str(&body))
            .transpose()
            .map_err(Into::into)
    }

    pub fn create_plan_run(&self, request: &PlanRunRequest) -> Result<PlanRunView, St3Error> {
        let plan_id = request.plan.strip_prefix("plan/").unwrap_or(&request.plan);
        let plan = self
            .plan_spec(plan_id, request.revision.as_deref())
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new("missing-plan", format!("plan `{plan_id}` does not exist"))
            })?;
        if plan.state != PlanState::Ready {
            return Err(St3Error::new(
                "plan-not-ready",
                format!("plan `{plan_id}` is not ready"),
            ));
        }
        let run_id = hex::encode(Sha256::digest(
            format!("{}:{}", self.origin, request.idempotency_key).as_bytes(),
        ))[..32]
            .to_owned();
        let subject = format!("plan-run/{run_id}");
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
        let mut variables = BTreeMap::from([
            ("PLAN".into(), plan.id.clone()),
            ("PLAN_REVISION".into(), plan.revision.clone()),
            ("PLAN_RUN".into(), run_id.clone()),
            ("REQUESTER".into(), requester.clone()),
            ("ROOT_PLAN_RUN".into(), subject.clone()),
        ]);
        let run_scope = plan
            .scope_template
            .as_deref()
            .map(|scope| crate::plan::interpolate(scope, &variables))
            .transpose()?;
        if let Some(scope) = &run_scope {
            variables.insert("RUN_SCOPE".into(), scope.clone());
        }
        let now = now_ms();
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE key=?1",
                [&request.idempotency_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO plan_runs(id, plan_id, revision, root_revision, workspace, requester, run_scope, mode, status, phase, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'running', 'normal', ?9, ?9)",
                params![run_id, plan.id, plan.revision, plan.revision, request.workspace, requester, run_scope, mode, now.to_string()],
            )
            .map_err(internal)?;
        let mut flat = Vec::new();
        flatten_steps(&plan, None, &mut flat);
        for (step, inherited_assignee) in flat {
            let assignee = step.assigned_to.as_ref().or(inherited_assignee.as_ref());
            let assignee = assignee
                .map(|value| crate::plan::interpolate(value, &variables))
                .transpose()?;
            let step_subject = format!("step-run/{run_id}/{}", step.path);
            transaction
                .execute(
                    "INSERT INTO step_runs(subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, created_at_unix_ms, updated_at_unix_ms)
                     VALUES (?1, ?2, ?3, ?4, 'pending', 1, ?5, ?6, ?7, ?8, ?8)",
                    params![step_subject, run_id, step.path, step.definition_hash, assignee, step.title, step.goal, now.to_string()],
                )
                .map_err(internal)?;
        }
        let body = json!({
            "fields": {
                "status": "running",
                "plan": plan.subject,
                "revision": plan.revision,
                "workspace": request.workspace,
                "requester": requester,
                "run_scope": run_scope,
                "mode": mode,
            }
        });
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "plan-run.created",
            Some(&requester),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        let view = plan_run_view_tx(&transaction, &run_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                params![
                    request.idempotency_key,
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn plan_run(&self, run: &str) -> Result<Option<PlanRunView>> {
        let run = run.strip_prefix("plan-run/").unwrap_or(run);
        let connection = self.connection.lock().expect("store mutex poisoned");
        Ok(plan_run_view_tx(&connection, run).optional()?)
    }

    pub fn plan_run_for_scope(&self, scope: &str) -> Result<Option<PlanRunView>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let run_id = connection
            .query_row(
                "SELECT id FROM plan_runs WHERE run_scope=?1 ORDER BY created_at_unix_ms DESC LIMIT 1",
                [scope],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        run_id
            .map(|run_id| plan_run_view_tx(&connection, &run_id))
            .transpose()
            .map_err(Into::into)
    }

    pub fn adopt_plan_revision(
        &self,
        run: &str,
        plan: &PlanSpec,
        actor: &str,
        reason: &str,
        idempotency_key: &str,
    ) -> Result<PlanRunView, St3Error> {
        let run_id = run.strip_prefix("plan-run/").unwrap_or(run);
        let actor = normalize_actor(
            actor,
            if matches!(plan.change_policy, ChangePolicy::HumanReview) {
                "person"
            } else {
                "agent"
            },
        );
        let current = self.plan_run(run_id).map_err(internal)?.ok_or_else(|| {
            St3Error::new(
                "missing-plan-run",
                format!("plan run `{run}` does not exist"),
            )
        })?;
        if !matches!(current.status.as_str(), "running" | "blocked") || current.phase != "normal" {
            return Err(St3Error::new(
                "plan-run-not-revisable",
                format!(
                    "plan run `{run}` is {} in its {} phase",
                    current.status, current.phase
                ),
            ));
        }
        let plan_id = current.plan.strip_prefix("plan/").unwrap_or(&current.plan);
        if plan.id != plan_id {
            return Err(St3Error::new(
                "wrong-plan-revision",
                format!("revision `{}` does not replace plan `{plan_id}`", plan.id),
            ));
        }
        if plan.state != PlanState::Ready {
            return Err(St3Error::new(
                "plan-revision-not-ready",
                "a running plan can adopt only a ready revision",
            ));
        }
        let old = self
            .plan_spec(plan_id, Some(&current.revision))
            .map_err(internal)?
            .ok_or_else(|| {
                St3Error::new(
                    "missing-plan-revision",
                    "the current plan revision is unavailable",
                )
            })?;
        if old.scope_template != plan.scope_template
            || old.change_policy != plan.change_policy
            || old.change_authority != plan.change_authority
        {
            return Err(St3Error::new(
                "change-policy-mutation",
                "a run revision cannot change its scope, change policy, or authority",
            ));
        }

        let variables = plan_run_variables(&current, &plan.revision);
        authorize_plan_revision(&old, plan, &actor, &variables)?;

        let mut old_steps = Vec::new();
        flatten_steps(&old, None, &mut old_steps);
        let old_hashes = old_steps
            .into_iter()
            .map(|(step, _)| (step.path.clone(), step.definition_hash.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut new_steps = Vec::new();
        flatten_steps(plan, None, &mut new_steps);
        let dependencies = flattened_dependencies(plan);
        let mut reset = new_steps
            .iter()
            .filter(|(step, _)| old_hashes.get(&step.path) != Some(&step.definition_hash))
            .map(|(step, _)| step.path.clone())
            .collect::<BTreeSet<_>>();
        loop {
            let additions = dependencies
                .iter()
                .filter(|(path, deps)| {
                    !reset.contains(*path)
                        && deps.iter().any(|dependency| reset.contains(dependency))
                })
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            if additions.is_empty() {
                break;
            }
            reset.extend(additions);
        }

        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE key=?1",
                [idempotency_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
        }
        let transaction = connection.transaction().map_err(internal)?;
        let retained = new_steps
            .iter()
            .map(|(step, _)| step.path.clone())
            .collect::<BTreeSet<_>>();
        let existing = current
            .steps
            .iter()
            .map(|step| step.step.clone())
            .collect::<Vec<_>>();
        for path in existing.into_iter().filter(|path| !retained.contains(path)) {
            transaction
                .execute(
                    "DELETE FROM step_runs WHERE run_id=?1 AND step_path=?2",
                    params![run_id, path],
                )
                .map_err(internal)?;
        }
        let now = now_ms();
        for (step, inherited_assignee) in new_steps {
            let assignee = step
                .assigned_to
                .as_ref()
                .or(inherited_assignee.as_ref())
                .map(|value| crate::plan::interpolate(value, &variables))
                .transpose()?;
            let subject = format!("step-run/{run_id}/{}", step.path);
            if reset.contains(&step.path) {
                transaction
                    .execute(
                        "INSERT INTO step_runs(subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported, created_at_unix_ms, updated_at_unix_ms)
                         VALUES (?1, ?2, ?3, ?4, 'pending', 1, ?5, ?6, ?7, 0, ?8, ?8)
                         ON CONFLICT(subject) DO UPDATE SET definition_hash=excluded.definition_hash, status='pending', attempt=1,
                           assignee=excluded.assignee, title=excluded.title, goal=excluded.goal, worker_reported=0,
                           lease_owner=NULL, lease_incarnation=NULL, lease_expires_at_unix_ms=NULL,
                           blocked_reason=?9, not_before_unix_ms=NULL, activated_at_unix_ms=NULL, updated_at_unix_ms=?8",
                        params![subject, run_id, step.path, step.definition_hash, assignee, step.title, step.goal, now.to_string(), reason],
                    )
                    .map_err(internal)?;
            }
        }
        transaction
            .execute(
                "UPDATE plan_runs SET revision=?2, status='running', phase='normal', updated_at_unix_ms=?3 WHERE id=?1",
                params![run_id, plan.revision, now.to_string()],
            )
            .map_err(internal)?;
        let body = json!({"fields": {"revision": plan.revision, "replaces": current.revision, "reason": reason}});
        append_claim_tx(
            &transaction,
            &self.origin,
            &current.subject,
            "plan-run.revised",
            Some(&actor),
            &body,
            &[],
            None,
        )
        .map_err(internal)?;
        let view = plan_run_view_tx(&transaction, run_id).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                params![
                    idempotency_key,
                    serde_json::to_string(&view).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        transaction.commit().map_err(internal)?;
        Ok(view)
    }

    pub fn active_plan_runs(&self) -> Result<Vec<PlanRunView>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id FROM plan_runs WHERE status IN ('running','blocked') ORDER BY created_at_unix_ms",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| plan_run_view_tx(&connection, &id).map_err(Into::into))
            .collect()
    }

    pub fn work(&self, assignee: Option<&str>, include_terminal: bool) -> Result<Vec<StepRunView>> {
        let assignee = assignee.map(|value| normalize_actor(value, "agent"));
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported,
                    lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms
             FROM step_runs
             WHERE (?1 IS NULL OR assignee=?1)
               AND (?2 OR status NOT IN ('completed','failed','cancelled'))
             ORDER BY created_at_unix_ms, step_path",
        )?;
        let rows = statement.query_map(params![assignee, include_terminal], step_run_from_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn step_run(&self, subject: &str) -> Result<Option<StepRunView>> {
        let subject = normalize_step_run(subject);
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms
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
                "SELECT response FROM idempotency WHERE key=?1",
                [&request.idempotency_key],
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
                "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported,
                        lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms
                 FROM step_runs WHERE subject=?1",
                [&subject],
                step_run_from_row,
            )
            .optional()
            .map_err(internal)?
            .ok_or_else(|| St3Error::new("missing-step-run", format!("step run `{subject}` does not exist")))?;
        if current.assignee.as_deref() != Some(actor.as_str()) {
            return Err(St3Error::new(
                "wrong-assignee",
                format!("`{actor}` is not assigned to `{subject}`"),
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
            .lease_expires_at_unix_ms
            .is_some_and(|expiry| expiry > now);
        if lease_valid && current.lease_owner.as_deref() != Some(actor.as_str()) {
            return Err(St3Error::new(
                "work-already-claimed",
                format!("step run `{subject}` has an active lease"),
            ));
        }
        if action != "claim"
            && (!lease_valid || current.lease_owner.as_deref() != Some(actor.as_str()))
        {
            return Err(St3Error::new(
                "work-not-claimed",
                format!("`{actor}` does not hold an active lease for `{subject}`"),
            ));
        }
        if lease_valid
            && current.lease_owner.as_deref() == Some(actor.as_str())
            && current.lease_incarnation.is_some()
            && current.lease_incarnation != request.incarnation
        {
            return Err(St3Error::new(
                "wrong-work-incarnation",
                format!("another `{actor}` incarnation holds `{subject}`"),
            ));
        }
        let effective_incarnation = current
            .lease_incarnation
            .clone()
            .or_else(|| request.incarnation.clone());
        let (status, worker_reported, lease_owner, lease_incarnation, lease_expiry) = match action {
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
        transaction
            .execute(
                "UPDATE step_runs SET status=?2, worker_reported=?3, lease_owner=?4, lease_incarnation=?5,
                        lease_expires_at_unix_ms=?6, blocked_reason=?7, updated_at_unix_ms=?8 WHERE subject=?1",
                params![subject, status, worker_reported, lease_owner, lease_incarnation, lease_expiry.map(|value| value.to_string()), request.reason, now.to_string()],
            )
            .map_err(internal)?;
        let body = json!({"fields": {
            "status": status,
            "summary": request.summary,
            "reason": request.reason,
            "worker_reported": worker_reported,
            "lease_owner": lease_owner,
            "lease_incarnation": lease_incarnation,
            "lease_expires_at_unix_ms": lease_expiry
        }, "evidence": request.evidence});
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            &format!("work.{action}"),
            Some(&actor),
            &body,
            &request.evidence,
            None,
        )
        .map_err(internal)?;
        let view = transaction.query_row(
            "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported,
                    lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms
             FROM step_runs WHERE subject=?1", [&subject], step_run_from_row).map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                params![
                    request.idempotency_key,
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
        let current: Option<String> = transaction
            .query_row(
                "SELECT status FROM step_runs WHERE subject=?1",
                [&subject],
                |row| row.get(0),
            )
            .optional()?;
        if current.as_deref() == Some(status) || current.is_none() {
            return Ok(false);
        }
        let now = now_ms();
        transaction.execute(
            "UPDATE step_runs SET status=?2, blocked_reason=?3,
                    lease_owner=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_owner END,
                    lease_incarnation=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_incarnation END,
                    lease_expires_at_unix_ms=CASE WHEN ?2 IN ('ready','completed','failed','cancelled') THEN NULL ELSE lease_expires_at_unix_ms END,
                    not_before_unix_ms=CASE WHEN ?2='ready' THEN NULL ELSE not_before_unix_ms END,
                    activated_at_unix_ms=CASE WHEN ?2='ready' THEN ?4 ELSE activated_at_unix_ms END,
                    updated_at_unix_ms=?4 WHERE subject=?1",
            params![subject, status, reason, now.to_string()])?;
        let body = json!({"fields": {"status": status, "reason": reason}});
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
        let current: Option<(String, u32)> = transaction
            .query_row(
                "SELECT status, attempt FROM step_runs WHERE subject=?1",
                [&subject],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((status, attempt)) = current else {
            return Ok(false);
        };
        if status != "failed" {
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
            "step-run.retry",
            None,
            &body,
            &[],
            None,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn set_plan_run_state(
        &self,
        run: &str,
        status: &str,
        phase: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let run = run.strip_prefix("plan-run/").unwrap_or(run);
        let subject = format!("plan-run/{run}");
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let transaction = connection.transaction()?;
        let current: Option<(String, String)> = transaction
            .query_row(
                "SELECT status, phase FROM plan_runs WHERE id=?1",
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
        let now = now_ms();
        transaction.execute(
            "UPDATE plan_runs SET status=?2, phase=?3, updated_at_unix_ms=?4 WHERE id=?1",
            params![run, status, phase, now.to_string()],
        )?;
        let body = json!({"fields": {"status": status, "phase": phase, "reason": reason}});
        append_claim_tx(
            &transaction,
            &self.origin,
            &subject,
            "plan-run.state",
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
        let plan = self.plan(intent, source)?;
        if !plan.blockers.is_empty() {
            return Err(St3Error::new(
                "internal-plan-blocked",
                plan.blockers.join("; "),
            ));
        }
        self.apply(intent, &plan.subject_tokens, key)
    }

    pub fn plan(
        &self,
        intent: &NormalizedIntent,
        resolved_intent: IntentInput,
    ) -> Result<PlanResponse, St3Error> {
        self.plan_at(intent, resolved_intent, None)
    }

    pub fn plan_at(
        &self,
        intent: &NormalizedIntent,
        resolved_intent: IntentInput,
        at_index: Option<u64>,
    ) -> Result<PlanResponse, St3Error> {
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
            if desired.kind == "link"
                && let Some(link) = crate::graph::link_spec(&desired.desired)
            {
                for endpoint in [link.from, link.to] {
                    if !known(&endpoint)? {
                        blockers.push(format!(
                            "link `{}` references undeclared subject `{endpoint}`",
                            desired.subject
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
                if recipient != "requester" && !known(&recipient)? {
                    blockers.push(format!(
                        "message `{}` references undeclared recipient `{recipient}`",
                        desired.subject
                    ));
                }
            }
            if desired.kind == "schedule"
                && let Some(schedule) = crate::graph::schedule_spec(&desired.desired, &self.origin)
                && let Some(message) = schedule.message
            {
                let recipient = if message.to == "requester" || message.to.contains('/') {
                    message.to
                } else {
                    format!("agent/{}", message.to)
                };
                if recipient != "requester" && !known(&recipient)? {
                    blockers.push(format!(
                        "schedule `{}` references undeclared recipient `{recipient}`",
                        desired.subject
                    ));
                }
            }
        }
        for checkpoint in &intent.checkpoints {
            for judge in &checkpoint.judges {
                let subject = match judge {
                    crate::model::JudgeSpec::Exists { subject }
                    | crate::model::JudgeSpec::Empty { subject }
                    | crate::model::JudgeSpec::Field { subject, .. }
                    | crate::model::JudgeSpec::Has { subject, .. }
                    | crate::model::JudgeSpec::Lacks { subject, .. } => Some(subject.as_str()),
                    _ => None,
                };
                if let Some(subject) = subject
                    && !subject.starts_with("file/")
                    && !subject.starts_with("doc/")
                    && !known(subject)?
                {
                    blockers.push(format!(
                        "checkpoint `{}` references undeclared subject `{subject}`",
                        checkpoint.name
                    ));
                }
            }
        }
        blockers.sort();
        blockers.dedup();

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
            } else if desired.member.is_some() && desired.activation.is_none() {
                actions.push(PlannedAction {
                    subject: subject.clone(),
                    action: "observe-or-start".into(),
                    reason: "the desired member is active".into(),
                });
            }
        }
        for plan in intent.plans.values() {
            let subject = plan.subject.clone();
            let current: Option<(String, String)> = connection
                .query_row(
                    "SELECT revision, claim_id FROM plan_definitions WHERE plan_id=?1",
                    [&plan.id],
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
                .is_some_and(|(revision, _)| revision == &plan.revision)
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
                new_revision: plan.revision.clone(),
            });
            actions.push(PlannedAction {
                subject,
                action: "publish-plan".into(),
                reason: "the immutable plan revision is not published".into(),
            });
        }

        Ok(PlanResponse {
            store_index,
            source_hash: intent.source_hash.clone(),
            normalized: intent.normalized.clone(),
            resolved_intent,
            changes,
            predicted_actions: actions,
            blockers,
            warnings,
            subject_tokens: tokens,
        })
    }

    pub fn apply(
        &self,
        intent: &NormalizedIntent,
        expected: &BTreeMap<String, Vec<String>>,
        idempotency_key: &str,
    ) -> Result<ApplyResponse, St3Error> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(response) = connection
            .query_row(
                "SELECT response FROM idempotency WHERE key = ?1",
                [idempotency_key],
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
        for plan in intent.plans.values() {
            let actual = plan_definition_token_tx(&transaction, &plan.id).map_err(internal)?;
            let expected = expected.get(&plan.subject).ok_or_else(|| {
                St3Error::new(
                    "missing-subject-token",
                    format!("apply omitted the subject token for `{}`", plan.subject),
                )
                .with_detail("subject", plan.subject.clone())
                .with_detail("current_heads", json!(actual.clone()))
            })?;
            if actual != *expected {
                return Err(St3Error::new(
                    "stale-subject",
                    format!(
                        "the published plan for `{}` changed after planning",
                        plan.subject
                    ),
                )
                .with_detail("subject", plan.subject.clone())
                .with_detail("expected_heads", json!(expected))
                .with_detail("current_heads", json!(actual)));
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
        let plans_changed = intent.plans.values().any(|plan| {
            transaction
                .query_row(
                    "SELECT revision FROM plan_definitions WHERE plan_id=?1",
                    [&plan.id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map(|current| current.as_deref() != Some(plan.revision.as_str()))
                .unwrap_or(true)
        });
        let changed = desired_changed || plans_changed;
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
            for plan in intent.plans.values() {
                subject_tokens.insert(
                    plan.subject.clone(),
                    plan_definition_token_tx(&transaction, &plan.id).map_err(internal)?,
                );
            }
            let response = ApplyResponse {
                changed: false,
                store_index,
                batch_id: None,
                claim_ids: Vec::new(),
                subject_tokens,
                reconcile_subjects: Vec::new(),
            };
            transaction
                .execute(
                    "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                    params![
                        idempotency_key,
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
                    "INSERT INTO desired(subject, kind, revision, claim_id, body, member, activation, scopes) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(subject) DO UPDATE SET kind=excluded.kind, revision=excluded.revision, claim_id=excluded.claim_id, body=excluded.body, member=excluded.member, activation=excluded.activation, scopes=excluded.scopes",
                    params![
                        subject,
                        desired.kind,
                        revision,
                        claim_id,
                        serde_json::to_string(&desired.desired).map_err(internal)?,
                        desired.member.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                        desired.activation.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                        serde_json::to_string(&desired.scopes).map_err(internal)?,
                    ],
                )
                .map_err(internal)?;
            insert_event(&transaction, store_index, "intent.desired", subject, &body)
                .map_err(internal)?;
            claim_ids.push(claim_id.clone());
            tokens.insert(subject.clone(), vec![claim_id]);
            reconcile_subjects.push(subject.clone());
        }
        for plan in intent.plans.values() {
            let current: Option<String> = transaction
                .query_row(
                    "SELECT revision FROM plan_definitions WHERE plan_id=?1",
                    [&plan.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(internal)?;
            if current.as_deref() == Some(plan.revision.as_str()) {
                tokens.insert(
                    plan.subject.clone(),
                    plan_definition_token_tx(&transaction, &plan.id).map_err(internal)?,
                );
                continue;
            }
            let predecessors =
                plan_definition_token_tx(&transaction, &plan.id).map_err(internal)?;
            let body = serde_json::to_value(plan).map_err(internal)?;
            let claim_id = claim_hash(
                &batch_id,
                &plan.subject,
                "plan.published",
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
                &plan.subject,
                "plan.published",
                &self.origin,
                None,
                &body,
                &predecessors,
                now,
            )
            .map_err(internal)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO plan_revisions(plan_id, revision, state, body, claim_id, created_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![plan.id, plan.revision, plan_state_name(&plan.state), serde_json::to_string(plan).map_err(internal)?, claim_id, store_index],
                )
                .map_err(internal)?;
            transaction
                .execute(
                    "INSERT INTO plan_definitions(plan_id, revision, state, claim_id) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(plan_id) DO UPDATE SET revision=excluded.revision, state=excluded.state, claim_id=excluded.claim_id",
                    params![plan.id, plan.revision, plan_state_name(&plan.state), claim_id],
                )
                .map_err(internal)?;
            insert_event(
                &transaction,
                store_index,
                "plan.published",
                &plan.subject,
                &body,
            )
            .map_err(internal)?;
            claim_ids.push(claim_id.clone());
            tokens.insert(plan.subject.clone(), vec![claim_id]);
        }
        let store_index = current_index_tx(&transaction).map_err(internal)?;
        let response = ApplyResponse {
            changed: true,
            store_index,
            batch_id: Some(batch_id),
            claim_ids,
            subject_tokens: tokens,
            reconcile_subjects,
        };
        transaction
            .execute(
                "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                params![
                    idempotency_key,
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
                "SELECT response FROM idempotency WHERE key=?1",
                [idempotency_key],
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
            "document.bound",
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
                "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                params![
                    idempotency_key,
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
        validate_claim_kind(&input.kind)?;
        validate_claim_subject(&input.subject)?;
        if let Some(actor) = &input.actor {
            validate_actor(actor)?;
        }
        validate_claim_fields(input)?;
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        if let Some(key) = &input.idempotency_key
            && let Some(response) = connection
                .query_row(
                    "SELECT response FROM idempotency WHERE key=?1",
                    [key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(internal)?
        {
            return serde_json::from_str(&response).map_err(internal);
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
        validate_message_transition(&transaction, input)?;
        let predecessor = latest_claim_id_tx(&transaction, &input.subject).map_err(internal)?;
        let predecessors = predecessor.into_iter().collect::<Vec<_>>();
        let body = json!({
            "fields": input.fields,
            "evidence": input.evidence,
        });
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
        .map_err(internal)?;
        if let Some(key) = &input.idempotency_key {
            transaction
                .execute(
                    "INSERT INTO idempotency(key, response) VALUES (?1, ?2)",
                    params![key, serde_json::to_string(&record).map_err(internal)?],
                )
                .map_err(internal)?;
        }
        transaction.commit().map_err(internal)?;
        Ok(record)
    }

    pub fn idempotent_claim(&self, key: &str) -> Result<Option<ClaimRecord>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection
            .query_row(
                "SELECT response FROM idempotency WHERE key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|response| serde_json::from_str(&response).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn status(&self, selected: Option<&str>) -> Result<StatusResponse> {
        self.status_at(selected, None, None)
    }

    pub fn status_at(
        &self,
        selected: Option<&str>,
        selected_scope: Option<&str>,
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
            let scopes = desired
                .as_ref()
                .and_then(|row| serde_json::from_str::<BTreeSet<String>>(&row.scopes).ok())
                .map(|values| values.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            if selected_scope.is_some_and(|scope| {
                subject != scope && !scopes.iter().any(|candidate| candidate == scope)
            }) {
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
                (Some("stop" | "scope-stop"), _, Some("stopped" | "absent" | "exited")) => None,
                (Some("stop" | "scope-stop"), _, _) => {
                    Some("the desired state is stopped".to_owned())
                }
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
                    action: if matches!(kind.as_deref(), Some("stop" | "scope-stop")) {
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
            subjects.push(SubjectStatus {
                subject,
                kind,
                desired_token: desired.as_ref().map(|row| row.claim_id.clone()),
                desired_revision: desired.as_ref().map(|row| row.revision.clone()),
                desired: desired
                    .as_ref()
                    .map(|row| serde_json::from_str(&row.body))
                    .transpose()?,
                actual,
                conflicts,
                claims,
                scopes,
                gap,
                reachability,
                reason,
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
        scope: Option<&str>,
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
                if scope
                    .is_some_and(|scope| !subject_in_scope(&connection, &event.subject, scope)) =>
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
            "SELECT subject, kind, body, member, activation, scopes FROM desired ORDER BY subject",
        )?;
        let rows = statement.query_map([], |row| {
            let subject = row.get::<_, String>(0)?;
            let kind = row.get::<_, String>(1)?;
            let desired = row.get::<_, String>(2)?;
            let member = row.get::<_, Option<String>>(3)?;
            let activation = row.get::<_, Option<String>>(4)?;
            let scopes = row.get::<_, String>(5)?;
            Ok(DesiredSubject {
                subject,
                kind,
                desired: serde_json::from_str(&desired).unwrap_or(Value::Null),
                member: member.and_then(|value| serde_json::from_str(&value).ok()),
                activation: activation.and_then(|value| serde_json::from_str(&value).ok()),
                scopes: serde_json::from_str(&scopes).unwrap_or_default(),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
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
                    .map(str::to_owned),
                in_reply_to: actual
                    .get("in_reply_to")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
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
                    .unwrap_or_default(),
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

    pub fn selected_desired_token(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_desired_row(&connection, subject).map(|row| row.map(|row| row.claim_id))
    }

    pub fn selected_desired_revision(&self, subject: &str) -> Result<Option<String>> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        current_desired_row(&connection, subject).map(|row| row.map(|row| row.revision))
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
        scope: Option<&str>,
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
            if scope.is_some_and(|scope| !subject_in_scope(&connection, &claim.subject, scope)) {
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

    pub fn checkpoint_reached_ordinal(&self, sequence: &str) -> Result<Option<u32>> {
        let Some(claim) = self.latest_claim(sequence, Some("checkpoint.reached"))? else {
            return Ok(None);
        };
        Ok(claim
            .body
            .pointer("/fields/ordinal")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok()))
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
                    insert_event(
                        &transaction,
                        index,
                        &claim.kind,
                        &claim.subject,
                        &claim.body,
                    )
                    .map_err(internal)?;
                    if claim.kind == "intent.desired" {
                        if let Ok(desired) =
                            serde_json::from_value::<DesiredSubject>(claim.body.clone())
                        {
                            select_replicated_desired(&transaction, claim, &desired)?;
                        }
                    } else if claim.kind == "document.bound" {
                        select_replicated_document(&transaction, claim, index)?;
                    } else if claim.kind == "plan.published" {
                        select_replicated_plan(&transaction, claim, index)?;
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
        project_replicated_plan_runs(&transaction)?;
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

#[derive(Clone)]
struct DesiredRow {
    kind: String,
    revision: String,
    claim_id: String,
    body: String,
    member: Option<String>,
    scopes: String,
}

fn current_desired_row(connection: &Connection, subject: &str) -> Result<Option<DesiredRow>> {
    connection
        .query_row(
            "SELECT kind, revision, claim_id, body, member, scopes FROM desired WHERE subject=?1",
            [subject],
            |row| {
                Ok(DesiredRow {
                    kind: row.get(0)?,
                    revision: row.get(1)?,
                    claim_id: row.get(2)?,
                    body: row.get(3)?,
                    member: row.get(4)?,
                    scopes: row.get(5)?,
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

fn subject_in_scope(connection: &Connection, subject: &str, scope: &str) -> bool {
    if subject == scope {
        return true;
    }
    current_desired_row(connection, subject)
        .ok()
        .flatten()
        .and_then(|row| serde_json::from_str::<BTreeSet<String>>(&row.scopes).ok())
        .is_some_and(|scopes| scopes.contains(scope))
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
                scopes: serde_json::to_string(&desired.scopes)?,
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
            "SELECT kind, revision, claim_id, body, member, scopes FROM desired WHERE subject=?1",
            [subject],
            |row| {
                Ok(DesiredRow {
                    kind: row.get(0)?,
                    revision: row.get(1)?,
                    claim_id: row.get(2)?,
                    body: row.get(3)?,
                    member: row.get(4)?,
                    scopes: row.get(5)?,
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
    if registered_client_claim_kind(kind) {
        Ok(())
    } else {
        Err(St3Error::new(
            "unknown-claim-kind",
            format!("claim kind `{kind}` is not registered in st3.v1"),
        ))
    }
}

fn validate_claim_subject(subject: &str) -> Result<(), St3Error> {
    if subject.is_empty()
        || subject.len() > 512
        || !subject.contains('/')
        || subject.chars().any(char::is_whitespace)
        || subject.chars().any(char::is_control)
    {
        return Err(St3Error::new(
            "invalid-claim-subject",
            "a claim subject must be a bounded full subject without whitespace",
        ));
    }
    Ok(())
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

fn validate_claim_fields(input: &ClaimInput) -> Result<(), St3Error> {
    let enum_field = |name: &str, allowed: &[&str]| -> Result<(), St3Error> {
        let Some(value) = input.fields.get(name) else {
            return Err(St3Error::new(
                "missing-claim-field",
                format!("claim kind `{}` needs field `{name}`", input.kind),
            ));
        };
        let Some(value) = value.as_str() else {
            return Err(St3Error::new(
                "invalid-claim-field",
                format!("claim field `{name}` must be a string"),
            ));
        };
        if !allowed.contains(&value) {
            return Err(St3Error::new(
                "invalid-claim-field",
                format!("claim field `{name}` has invalid value `{value}`"),
            ));
        }
        Ok(())
    };
    match input.kind.as_str() {
        "context.clear.requested" | "session.signal.requested" => {
            enum_field("status", &["requested"])?
        }
        "context.clear.result" | "session.signal.result" => {
            enum_field("status", &["succeeded", "failed"])?
        }
        "account.quota" => enum_field("quota", &["available", "limited", "exhausted", "unknown"])?,
        "eval.verdict" => enum_field("verdict", &["pass", "fail", "void"])?,
        "judgement.result" | "judge.result" => enum_field("verdict", &["pass", "fail"])?,
        "presence.observed" => enum_field("presence", &["available", "busy", "dnd", "offline"])?,
        "reachability.changed" => enum_field(
            "reachability",
            &["reachable", "unreachable", "indeterminate"],
        )?,
        "review.decision" => enum_field("decision", &["approved", "rejected", "revise"])?,
        "transport.peer" => enum_field("status", &["up", "down", "unknown"])?,
        _ => {}
    }
    Ok(())
}

fn registered_client_claim_kind(kind: &str) -> bool {
    const REGISTERED: &[&str] = &[
        "account.quota",
        "action.completed",
        "action.failed",
        "action.requested",
        "action.result",
        "actor.action-observed",
        "agent.account",
        "checkpoint.active",
        "checkpoint.failed",
        "checkpoint.reached",
        "clock.adjusted",
        "cutover.ready",
        "clock.reached",
        "clock.wake.requested",
        "clock.wake.cancel.requested",
        "context.clear.requested",
        "context.clear.result",
        "daemon.started",
        "deadline.reached",
        "eval.verdict",
        "gate.observed",
        "harness.activity",
        "harness.cleared",
        "harness.compacted",
        "harness.error",
        "harness.idle",
        "harness.ready",
        "harness.turn-usage",
        "harness.work-started",
        "judge.result",
        "judgement.result",
        "judgement.requested",
        "member.observed",
        "member.launch",
        "member.restart-reset",
        "message.accepted",
        "message.closed",
        "message.delivered",
        "message.sent",
        "plan-run.created",
        "plan-run.revised",
        "plan-run.state",
        "plan.published",
        "presence.observed",
        "pid.observed",
        "reachability.changed",
        "replication.accepted",
        "resource.binding",
        "resource.file-observed",
        "resource.session-bound",
        "review.decision",
        "session.signal.requested",
        "session.signal.result",
        "step-run.state",
        "step-run.retry",
        "scope.members",
        "supervision.decision",
        "terminal.input.requested",
        "terminal.input.result",
        "transport.peer",
        "work.claim",
        "work.renew",
        "work.progress",
        "work.complete",
        "work.fail",
        "work.release",
    ];
    REGISTERED.contains(&kind)
}

fn known_replicated_claim_kind(kind: &str) -> bool {
    matches!(kind, "intent.desired" | "document.bound" | "plan.published")
        || registered_client_claim_kind(kind)
}

fn has_unknown_claim_at(
    connection: &Connection,
    subject: &str,
    at_index: Option<u64>,
) -> Result<Option<String>> {
    let through = at_index.unwrap_or(i64::MAX as u64);
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
        "message.accepted" => "accepted",
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
    if matches!(requested, "accepted" | "closed") && input.actor.is_none() {
        return Err(St3Error::new(
            "missing-message-actor",
            format!("`{}` needs an actor", input.kind),
        ));
    }

    let current: Option<String> = transaction
        .query_row(
            "SELECT kind FROM claims WHERE subject=?1 AND kind IN ('message.sent','message.delivered','message.accepted','message.closed') ORDER BY store_index DESC LIMIT 1",
            [&input.subject],
            |row| row.get(0),
        )
        .optional()
        .map_err(internal)?;
    let current = current.as_deref().map(|kind| match kind {
        "message.sent" => "sent",
        "message.delivered" => "delivered",
        "message.accepted" => "accepted",
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
            | (Some("delivered"), "accepted")
            | (Some("accepted"), "closed")
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
        body: body.clone(),
        predecessors: predecessors.to_vec(),
        accepted_at_unix_ms: now,
    })
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

fn plan_definition_token_tx(transaction: &Transaction<'_>, plan_id: &str) -> Result<Vec<String>> {
    Ok(transaction
        .query_row(
            "SELECT claim_id FROM plan_definitions WHERE plan_id=?1",
            [plan_id],
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
    Ok(ClaimRecord {
        id: row.get(0)?,
        store_index: row.get(1)?,
        batch_id: row.get(2)?,
        subject: row.get(3)?,
        kind: row.get(4)?,
        origin: row.get(5)?,
        actor: row.get(6)?,
        body: serde_json::from_str(&body).unwrap_or(Value::Null),
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

fn internal(error: impl std::fmt::Display) -> St3Error {
    St3Error::new("internal", error.to_string())
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
            "INSERT INTO desired(subject, kind, revision, claim_id, body, member, activation, scopes) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(subject) DO UPDATE SET kind=excluded.kind, revision=excluded.revision, claim_id=excluded.claim_id, body=excluded.body, member=excluded.member, activation=excluded.activation, scopes=excluded.scopes",
            params![
                claim.subject,
                desired.kind,
                revision,
                claim.id,
                serde_json::to_string(&desired.desired).map_err(internal)?,
                desired.member.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                desired.activation.as_ref().map(serde_json::to_string).transpose().map_err(internal)?,
                serde_json::to_string(&desired.scopes).map_err(internal)?,
            ],
        )
        .map_err(internal)?;
    Ok(())
}

fn project_replicated_plan_runs(transaction: &Transaction<'_>) -> Result<(), St3Error> {
    let mut statement = transaction
        .prepare(
            "SELECT id, store_index, batch_id, subject, kind, origin, actor, body, predecessors, accepted_at_unix_ms
             FROM claims
             WHERE kind IN ('plan-run.created','plan-run.revised','plan-run.state','step-run.state','step-run.retry',
                            'work.claim','work.renew','work.progress','work.complete','work.fail','work.release')
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
        .filter(|claim| claim.kind == "plan-run.created")
    {
        project_plan_run_created(transaction, claim)?;
    }
    for claim in claims
        .iter()
        .filter(|claim| claim.kind != "plan-run.created")
    {
        project_plan_run_update(transaction, claim)?;
    }
    Ok(())
}

fn project_plan_run_created(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
) -> Result<(), St3Error> {
    let fields = claim.body.get("fields").unwrap_or(&claim.body);
    let run_id = claim.subject.strip_prefix("plan-run/").ok_or_else(|| {
        St3Error::new(
            "invalid-plan-run-claim",
            format!("claim `{}` has an invalid plan run subject", claim.id),
        )
    })?;
    let plan_subject = fields.get("plan").and_then(Value::as_str).ok_or_else(|| {
        St3Error::new(
            "invalid-plan-run-claim",
            format!("claim `{}` has no plan", claim.id),
        )
    })?;
    let plan_id = plan_subject.strip_prefix("plan/").unwrap_or(plan_subject);
    let revision = fields
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            St3Error::new(
                "invalid-plan-run-claim",
                format!("claim `{}` has no plan revision", claim.id),
            )
        })?;
    let plan_body = transaction
        .query_row(
            "SELECT body FROM plan_revisions WHERE plan_id=?1 AND revision=?2",
            params![plan_id, revision],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(internal)?;
    let Some(plan_body) = plan_body else {
        return Ok(());
    };
    let plan = serde_json::from_str::<PlanSpec>(&plan_body).map_err(internal)?;
    let workspace = fields
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or(".");
    let requester = fields
        .get("requester")
        .and_then(Value::as_str)
        .unwrap_or("person/requester");
    let run_scope = fields.get("run_scope").and_then(Value::as_str);
    let mode = fields.get("mode").and_then(Value::as_str).unwrap_or("run");
    transaction
        .execute(
            "INSERT OR IGNORE INTO plan_runs(id, plan_id, revision, root_revision, workspace, requester, run_scope, mode, status, phase, created_at_unix_ms, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, ?7, 'running', 'normal', ?8, ?8)",
            params![run_id, plan_id, revision, workspace, requester, run_scope, mode, claim.accepted_at_unix_ms.to_string()],
        )
        .map_err(internal)?;
    let view = plan_run_view_tx(transaction, run_id).map_err(internal)?;
    let variables = plan_run_variables(&view, revision);
    let mut steps = Vec::new();
    flatten_steps(&plan, None, &mut steps);
    for (step, inherited_assignee) in steps {
        let assignee = step
            .assigned_to
            .as_ref()
            .or(inherited_assignee.as_ref())
            .map(|value| crate::plan::interpolate(value, &variables))
            .transpose()?;
        let subject = format!("step-run/{run_id}/{}", step.path);
        transaction
            .execute(
                "INSERT OR IGNORE INTO step_runs(subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, 'pending', 1, ?5, ?6, ?7, ?8, ?8)",
                params![subject, run_id, step.path, step.definition_hash, assignee, step.title, step.goal, claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn project_plan_run_update(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
) -> Result<(), St3Error> {
    let fields = claim.body.get("fields").unwrap_or(&claim.body);
    if claim.kind == "plan-run.revised" {
        let Some(run_id) = claim.subject.strip_prefix("plan-run/") else {
            return Ok(());
        };
        let Some(revision) = fields.get("revision").and_then(Value::as_str) else {
            return Ok(());
        };
        project_plan_revision(
            transaction,
            run_id,
            revision,
            fields.get("reason").and_then(Value::as_str),
            claim.accepted_at_unix_ms,
        )?;
        return Ok(());
    }
    if claim.kind == "plan-run.state" {
        let Some(run_id) = claim.subject.strip_prefix("plan-run/") else {
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
                "UPDATE plan_runs SET status=?2, phase=?3, updated_at_unix_ms=?4 WHERE id=?1",
                params![run_id, status, phase, claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
        return Ok(());
    }
    if !claim.subject.starts_with("step-run/") {
        return Ok(());
    }
    if claim.kind == "step-run.retry" {
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
                        updated_at_unix_ms=?4 WHERE subject=?1",
                params![claim.subject, status, fields.get("reason").and_then(Value::as_str), claim.accepted_at_unix_ms.to_string()],
            )
            .map_err(internal)?;
        return Ok(());
    }
    if claim.kind.starts_with("work.") {
        let lease_expiry = fields
            .get("lease_expires_at_unix_ms")
            .and_then(Value::as_u64)
            .map(|value| value.to_string());
        transaction
            .execute(
                "UPDATE step_runs SET status=?2, worker_reported=?3, lease_owner=?4,
                        lease_incarnation=?5, lease_expires_at_unix_ms=?6, blocked_reason=?7,
                        updated_at_unix_ms=?8 WHERE subject=?1",
                params![
                    claim.subject,
                    status,
                    fields
                        .get("worker_reported")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    fields.get("lease_owner").and_then(Value::as_str),
                    fields.get("lease_incarnation").and_then(Value::as_str),
                    lease_expiry,
                    fields.get("reason").and_then(Value::as_str),
                    claim.accepted_at_unix_ms.to_string(),
                ],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn project_plan_revision(
    transaction: &Transaction<'_>,
    run_id: &str,
    revision: &str,
    reason: Option<&str>,
    now: u128,
) -> Result<(), St3Error> {
    let current = match plan_run_view_tx(transaction, run_id) {
        Ok(current) => current,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(()),
        Err(error) => return Err(internal(error)),
    };
    let plan_id = current.plan.strip_prefix("plan/").unwrap_or(&current.plan);
    let body = transaction
        .query_row(
            "SELECT body FROM plan_revisions WHERE plan_id=?1 AND revision=?2",
            params![plan_id, revision],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(internal)?;
    let Some(body) = body else {
        return Ok(());
    };
    let plan = serde_json::from_str::<PlanSpec>(&body).map_err(internal)?;
    let variables = plan_run_variables(&current, revision);
    let old_hashes = current
        .steps
        .iter()
        .map(|step| (step.step.clone(), step.definition_hash.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut new_steps = Vec::new();
    flatten_steps(&plan, None, &mut new_steps);
    let dependencies = flattened_dependencies(&plan);
    let mut reset = new_steps
        .iter()
        .filter(|(step, _)| old_hashes.get(&step.path) != Some(&step.definition_hash))
        .map(|(step, _)| step.path.clone())
        .collect::<BTreeSet<_>>();
    loop {
        let additions = dependencies
            .iter()
            .filter(|(path, dependencies)| {
                !reset.contains(*path)
                    && dependencies
                        .iter()
                        .any(|dependency| reset.contains(dependency))
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        if additions.is_empty() {
            break;
        }
        reset.extend(additions);
    }
    let retained = new_steps
        .iter()
        .map(|(step, _)| step.path.clone())
        .collect::<BTreeSet<_>>();
    for path in old_hashes.keys().filter(|path| !retained.contains(*path)) {
        transaction
            .execute(
                "DELETE FROM step_runs WHERE run_id=?1 AND step_path=?2",
                params![run_id, path],
            )
            .map_err(internal)?;
    }
    for (step, inherited_assignee) in new_steps {
        if !reset.contains(&step.path) {
            continue;
        }
        let assignee = step
            .assigned_to
            .as_ref()
            .or(inherited_assignee.as_ref())
            .map(|value| crate::plan::interpolate(value, &variables))
            .transpose()?;
        let subject = format!("step-run/{run_id}/{}", step.path);
        transaction
            .execute(
                "INSERT INTO step_runs(subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported, created_at_unix_ms, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, 'pending', 1, ?5, ?6, ?7, 0, ?8, ?8)
                 ON CONFLICT(subject) DO UPDATE SET definition_hash=excluded.definition_hash, status='pending', attempt=1,
                   assignee=excluded.assignee, title=excluded.title, goal=excluded.goal, worker_reported=0,
                   lease_owner=NULL, lease_incarnation=NULL, lease_expires_at_unix_ms=NULL,
                   blocked_reason=?9, not_before_unix_ms=NULL, activated_at_unix_ms=NULL, updated_at_unix_ms=?8",
                params![subject, run_id, step.path, step.definition_hash, assignee, step.title, step.goal, now.to_string(), reason],
            )
            .map_err(internal)?;
    }
    transaction
        .execute(
            "UPDATE plan_runs SET revision=?2, status='running', phase='normal', updated_at_unix_ms=?3 WHERE id=?1",
            params![run_id, revision, now.to_string()],
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

fn select_replicated_plan(
    transaction: &Transaction<'_>,
    claim: &ClaimRecord,
    created_index: u64,
) -> Result<(), St3Error> {
    let plan = serde_json::from_value::<PlanSpec>(claim.body.clone()).map_err(|error| {
        St3Error::new(
            "invalid-plan-claim",
            format!("replicated plan claim `{}` is invalid: {error}", claim.id),
        )
    })?;
    if claim.subject != plan.subject || claim.subject != format!("plan/{}", plan.id) {
        return Err(St3Error::new(
            "invalid-plan-claim",
            format!(
                "replicated plan claim `{}` has mismatched identity",
                claim.id
            ),
        ));
    }
    let mut unhashed = plan.clone();
    let revision = std::mem::take(&mut unhashed.revision);
    let expected = hex::encode(Sha256::digest(
        serde_json::to_vec(&unhashed).map_err(internal)?,
    ));
    if revision != expected {
        return Err(St3Error::new(
            "invalid-plan-claim",
            format!(
                "replicated plan claim `{}` has an invalid revision",
                claim.id
            ),
        ));
    }
    transaction
        .execute(
            "INSERT OR IGNORE INTO plan_revisions(plan_id, revision, state, body, claim_id, created_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![plan.id, plan.revision, plan_state_name(&plan.state), serde_json::to_string(&plan).map_err(internal)?, claim.id, created_index],
        )
        .map_err(internal)?;
    let current: Option<(String, String)> = transaction
        .query_row(
            "SELECT revision, claim_id FROM plan_definitions WHERE plan_id=?1",
            [&plan.id],
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
            (plan.revision.as_str(), claim.id.as_str())
                > (current_revision.as_str(), current_claim.as_str())
        }
    } else {
        true
    };
    if select {
        transaction
            .execute(
                "INSERT INTO plan_definitions(plan_id, revision, state, claim_id) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(plan_id) DO UPDATE SET revision=excluded.revision, state=excluded.state, claim_id=excluded.claim_id",
                params![plan.id, plan.revision, plan_state_name(&plan.state), claim.id],
            )
            .map_err(internal)?;
    }
    Ok(())
}

fn plan_state_name(state: &PlanState) -> &'static str {
    match state {
        PlanState::Draft => "draft",
        PlanState::Ready => "ready",
        PlanState::Retired => "retired",
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
    plan: &'a PlanSpec,
    inherited_assignee: Option<String>,
    output: &mut Vec<(&'a crate::model::StepSpec, Option<String>)>,
) {
    for id in &plan.display_order {
        let step = &plan.steps[id];
        output.push((step, inherited_assignee.clone()));
        if let Some(nested) = &step.nested_plan {
            let next = step
                .assigned_to
                .clone()
                .or_else(|| inherited_assignee.clone());
            flatten_steps(nested, next, output);
        }
    }
}

pub(crate) fn plan_run_variables(run: &PlanRunView, revision: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "PLAN".into(),
            run.plan.strip_prefix("plan/").unwrap_or(&run.plan).into(),
        ),
        ("PLAN_REVISION".into(), revision.into()),
        ("PLAN_RUN".into(), run.id.clone()),
        (
            "RUN_SCOPE".into(),
            run.run_scope.clone().unwrap_or_default(),
        ),
        ("WORKSPACE".into(), run.workspace.clone()),
        ("REQUESTER".into(), run.requester.clone()),
        ("ROOT_PLAN_RUN".into(), run.subject.clone()),
    ])
}

pub(crate) fn authorize_plan_revision(
    old: &PlanSpec,
    new: &PlanSpec,
    actor: &str,
    variables: &BTreeMap<String, String>,
) -> Result<(), St3Error> {
    match old.change_policy {
        ChangePolicy::Supervisor | ChangePolicy::HumanReview => {
            let default_kind = if matches!(old.change_policy, ChangePolicy::HumanReview) {
                "person"
            } else {
                "agent"
            };
            let authority = old.change_authority.as_deref().ok_or_else(|| {
                St3Error::new(
                    "missing-change-authority",
                    "the plan has no change authority",
                )
            })?;
            if normalize_actor(authority, default_kind) != actor {
                return Err(St3Error::new(
                    "revision-not-authorized",
                    format!("`{actor}` is not the plan change authority"),
                ));
            }
        }
        ChangePolicy::Agent => {
            let hashes = |plan: &PlanSpec| {
                let mut flat = Vec::new();
                flatten_steps(plan, None, &mut flat);
                flat.into_iter()
                    .map(|(step, _)| (step.path.clone(), step.definition_hash.clone()))
                    .collect::<BTreeMap<_, _>>()
            };
            let old_hashes = hashes(old);
            let new_hashes = hashes(new);
            let changed = old_hashes
                .keys()
                .chain(new_hashes.keys())
                .filter(|path| old_hashes.get(*path) != new_hashes.get(*path))
                .cloned()
                .collect::<BTreeSet<_>>();
            if changed.is_empty() {
                return Err(St3Error::new(
                    "unchanged-plan-revision",
                    "the proposed plan revision does not change a step",
                ));
            }
            let mut owned = Vec::new();
            for plan in [old, new] {
                let mut flat = Vec::new();
                flatten_steps(plan, None, &mut flat);
                for (step, inherited) in flat {
                    let assignee = step.assigned_to.as_ref().or(inherited.as_ref());
                    if assignee
                        .map(|value| crate::plan::interpolate(value, variables))
                        .transpose()?
                        .as_deref()
                        == Some(actor)
                    {
                        owned.push(step.path.clone());
                    }
                }
            }
            if changed.iter().any(|path| {
                !owned
                    .iter()
                    .any(|root| path == root || path.starts_with(&format!("{root}/")))
            }) {
                return Err(St3Error::new(
                    "revision-outside-assignment",
                    format!("`{actor}` can revise only its assigned step subtree"),
                ));
            }
        }
    }
    Ok(())
}

fn flattened_dependencies(plan: &PlanSpec) -> BTreeMap<String, BTreeSet<String>> {
    fn collect(
        plan: &PlanSpec,
        prefix: &str,
        parent: Option<&str>,
        output: &mut BTreeMap<String, BTreeSet<String>>,
    ) {
        for id in &plan.display_order {
            let step = &plan.steps[id];
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
            if let Some(nested) = &step.nested_plan {
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
    collect(plan, "", None, &mut output);
    output
}

fn step_run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StepRunView> {
    let lease: Option<String> = row.get(12)?;
    let not_before: Option<String> = row.get(14)?;
    let created: String = row.get(15)?;
    let updated: String = row.get(16)?;
    Ok(StepRunView {
        subject: row.get(0)?,
        run: format!("plan-run/{}", row.get::<_, String>(1)?),
        step: row.get(2)?,
        definition_hash: row.get(3)?,
        status: row.get(4)?,
        attempt: row.get::<_, u32>(5)?,
        assignee: row.get(6)?,
        title: row.get(7)?,
        goal: row.get(8)?,
        worker_reported: row.get::<_, bool>(9)?,
        lease_owner: row.get(10)?,
        lease_incarnation: row.get(11)?,
        lease_expires_at_unix_ms: lease.and_then(|value| value.parse().ok()),
        blocked_reason: row.get(13)?,
        not_before_unix_ms: not_before.and_then(|value| value.parse().ok()),
        created_at_unix_ms: created.parse().unwrap_or(0),
        updated_at_unix_ms: updated.parse().unwrap_or(0),
    })
}

fn plan_run_view_tx(connection: &Connection, run_id: &str) -> rusqlite::Result<PlanRunView> {
    let mut view = connection.query_row(
        "SELECT id, plan_id, revision, root_revision, workspace, requester, run_scope, mode, status, phase,
                created_at_unix_ms, updated_at_unix_ms FROM plan_runs WHERE id=?1",
        [run_id],
        |row| {
            let id: String = row.get(0)?;
            let created: String = row.get(10)?;
            let updated: String = row.get(11)?;
            Ok(PlanRunView {
                subject: format!("plan-run/{id}"),
                id,
                plan: format!("plan/{}", row.get::<_, String>(1)?),
                revision: row.get(2)?,
                root_revision: row.get(3)?,
                workspace: row.get(4)?,
                requester: row.get(5)?,
                run_scope: row.get(6)?,
                mode: row.get(7)?,
                status: row.get(8)?,
                phase: row.get(9)?,
                created_at_unix_ms: created.parse().unwrap_or(0),
                updated_at_unix_ms: updated.parse().unwrap_or(0),
                steps: Vec::new(),
            })
        },
    )?;
    let mut statement = connection.prepare(
        "SELECT subject, run_id, step_path, definition_hash, status, attempt, assignee, title, goal, worker_reported,
                lease_owner, lease_incarnation, lease_expires_at_unix_ms, blocked_reason, not_before_unix_ms, created_at_unix_ms, updated_at_unix_ms
         FROM step_runs WHERE run_id=?1 ORDER BY created_at_unix_ms, step_path",
    )?;
    view.steps = statement
        .query_map([run_id], step_run_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::parse_intent;

    fn simple(command: &str) -> NormalizedIntent {
        parse_intent(
            &format!("version 2\nsubgraph {{ exec \"work\" {{ command {command:?}; restart \"never\" }} }}"),
            "node",
        )
        .expect("intent")
    }

    #[test]
    fn apply_is_subject_cas_and_idempotent() {
        let store = Store::open_memory("node").expect("store");
        let intent = simple("true");
        let plan = store
            .plan(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .expect("plan");
        let first = store
            .apply(&intent, &plan.subject_tokens, "one")
            .expect("apply");
        let repeated = store
            .apply(&intent, &plan.subject_tokens, "one")
            .expect("repeat");
        assert_eq!(first.batch_id, repeated.batch_id);
        let unchanged_plan = store
            .plan(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let unchanged = store
            .apply(&intent, &unchanged_plan.subject_tokens, "unchanged")
            .unwrap();
        assert!(!unchanged.changed);
        assert!(unchanged.batch_id.is_none());
        assert_eq!(unchanged.store_index, first.store_index);

        let changed = simple("false");
        let error = store
            .apply(&changed, &plan.subject_tokens, "two")
            .expect_err("stale token");
        assert_eq!(error.code, "stale-subject");
        assert_eq!(error.details["subject"], "exec/work");
        assert_eq!(error.details["expected_heads"], json!([]));
        assert_eq!(error.details["current_heads"].as_array().unwrap().len(), 1);
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
                "version 2\nsubgraph {{ person \"worker\"; message \"task\" {{ to \"person/worker\"; content \"doc/task@{}\"; }} }}",
                old.hash
            ),
            "node",
        )
        .expect("intent");
        let plan = store
            .plan(
                &intent,
                IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .expect("plan");
        assert!(plan.blockers.is_empty());
        assert_eq!(plan.warnings.len(), 1);
    }

    #[test]
    fn historical_plan_and_status_use_the_selected_index() {
        let store = Store::open_memory("node").expect("store");
        let first = simple("true");
        let first_plan = store
            .plan(
                &first,
                IntentInput {
                    kdl: "first".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let first_apply = store
            .apply(&first, &first_plan.subject_tokens, "first")
            .unwrap();
        let first_index = first_apply.store_index;

        let second = simple("false");
        let second_plan = store
            .plan(
                &second,
                IntentInput {
                    kdl: "second".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&second, &second_plan.subject_tokens, "second")
            .unwrap();

        let historical = store
            .status_at(Some("exec/work"), None, Some(first_index))
            .unwrap();
        assert_eq!(historical.store_index, first_index);
        assert_eq!(
            historical.subjects[0].desired_token,
            first_apply.subject_tokens["exec/work"].first().cloned()
        );
        let historical_plan = store
            .plan_at(
                &second,
                IntentInput {
                    kdl: "historical".into(),
                    source_name: None,
                },
                Some(first_index),
            )
            .unwrap();
        assert_eq!(
            historical_plan.subject_tokens["exec/work"],
            first_apply.subject_tokens["exec/work"]
        );
        assert_eq!(historical_plan.changes.len(), 1);
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
    fn replication_carries_runnable_plan_definitions() {
        let source = Store::open_memory("source").unwrap();
        let kdl = r#"version 2
subgraph { plan "remote" state="ready" { step "work" { } } }"#;
        let intent = parse_intent(kdl, "source").unwrap();
        let planned = source
            .plan(
                &intent,
                IntentInput {
                    kdl: kdl.into(),
                    source_name: None,
                },
            )
            .unwrap();
        source
            .apply(&intent, &planned.subject_tokens, "remote-plan")
            .unwrap();

        let target = Store::open_memory("target").unwrap();
        target
            .import_replication("source", &source.export_replication(0).unwrap())
            .unwrap();
        let replicated = target.plan_spec("remote", None).unwrap().unwrap();
        assert_eq!(replicated.state, PlanState::Ready);
        let run = target
            .create_plan_run(&PlanRunRequest {
                plan: "remote".into(),
                revision: Some(replicated.revision),
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "remote-run".into(),
            })
            .unwrap();
        assert_eq!(run.steps.len(), 1);
    }

    #[test]
    fn replication_carries_plan_runs_and_remote_work_updates() {
        let controller = Store::open_memory("controller").unwrap();
        let kdl = r#"
version 2
subgraph {
  plan "remote-work" state="ready" {
    step "work" { assigned-to "agent/worker.one" }
  }
}
"#;
        let intent = parse_intent(kdl, "controller").unwrap();
        let planned = controller
            .plan(
                &intent,
                IntentInput {
                    kdl: kdl.into(),
                    source_name: None,
                },
            )
            .unwrap();
        controller
            .apply(&intent, &planned.subject_tokens, "remote-work-plan")
            .unwrap();
        let run = controller
            .create_plan_run(&PlanRunRequest {
                plan: "remote-work".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "remote-work-run".into(),
            })
            .unwrap();
        let step = run.steps[0].subject.clone();
        controller.set_step_state(&step, "ready", None).unwrap();

        let worker = Store::open_memory("worker").unwrap();
        worker
            .import_replication("controller", &controller.export_replication(0).unwrap())
            .unwrap();
        let remote = worker.plan_run(&run.id).unwrap().unwrap();
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
    fn replication_rejects_tampered_claims() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "host/source".into(),
                kind: "transport.peer".into(),
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
    fn an_unknown_replicated_claim_marks_its_subject_indeterminate() {
        let source = Store::open_memory("source").unwrap();
        source
            .append_claim(&ClaimInput {
                subject: "resource/future".into(),
                kind: "resource.binding".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("active".into()))]),
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
                kind: "transport.peer".into(),
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
                .latest_claim("host/source", Some("transport.peer"))
                .unwrap()
                .is_some()
        );
        assert_eq!(target.replica_heads().unwrap().get("source"), Some(&1));
    }

    #[test]
    fn a_later_publish_cites_and_resolves_all_concurrent_intent_heads() {
        let left = Store::open_memory("left").unwrap();
        let initial = simple("true");
        let plan = left
            .plan(
                &initial,
                IntentInput {
                    kdl: "initial".into(),
                    source_name: None,
                },
            )
            .unwrap();
        left.apply(&initial, &plan.subject_tokens, "initial")
            .unwrap();

        let right = Store::open_memory("right").unwrap();
        right
            .import_replication("left", &left.export_replication(0).unwrap())
            .unwrap();
        let left_change = simple("false");
        let right_change = simple("printf right");
        let left_plan = left
            .plan(
                &left_change,
                IntentInput {
                    kdl: "left".into(),
                    source_name: None,
                },
            )
            .unwrap();
        let right_plan = right
            .plan(
                &right_change,
                IntentInput {
                    kdl: "right".into(),
                    source_name: None,
                },
            )
            .unwrap();
        left.apply(&left_change, &left_plan.subject_tokens, "left")
            .unwrap();
        right
            .apply(&right_change, &right_plan.subject_tokens, "right")
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
        let resolution_plan = left
            .plan(
                &resolved,
                IntentInput {
                    kdl: "resolved".into(),
                    source_name: None,
                },
            )
            .unwrap();
        assert_eq!(resolution_plan.subject_tokens["exec/work"].len(), 2);
        left.apply(&resolved, &resolution_plan.subject_tokens, "resolved")
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
    fn message_lifecycle_cannot_skip_delivery_or_acceptance() {
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
        append(
            "message.accepted",
            "accepted",
            Some("agent/worker"),
            "accepted",
        )
        .unwrap();
        append("message.closed", "closed", Some("agent/worker"), "closed").unwrap();
        assert_eq!(store.messages(None, true).unwrap()[0].status, "closed");
    }

    #[test]
    fn desired_messages_use_canonical_agent_parties_in_views() {
        let store = Store::open_memory("node").unwrap();
        let intent = crate::graph::parse_intent(
            r#"
version 2
subgraph {
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
}
"#,
            "local",
        )
        .unwrap();
        let plan = store
            .plan(
                &intent,
                IntentInput {
                    kdl: "message".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &plan.subject_tokens, "message")
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
        assert!(
            error
                .to_string()
                .contains("unsupported pre-plan st3 schema")
        );
    }

    #[test]
    fn work_actions_require_an_active_incarnation_bound_lease() {
        let store = Store::open_memory("node").unwrap();
        let intent = crate::graph::parse_intent(
            r#"
version 2
subgraph {
  plan "lease" state="ready" {
    step "work" { assigned-to "agent/worker" }
  }
}
"#,
            "node",
        )
        .unwrap();
        let planned = store
            .plan(
                &intent,
                IntentInput {
                    kdl: "lease".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "lease-plan")
            .unwrap();
        let run = store
            .create_plan_run(&PlanRunRequest {
                plan: "lease".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
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
        assert!(view.lease_owner.is_none());
        assert!(view.lease_incarnation.is_none());
        assert!(view.lease_expires_at_unix_ms.is_none());
    }

    #[test]
    fn a_retry_records_its_backoff_boundary() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_intent(source, "node").unwrap();
            let planned = store
                .plan(
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
subgraph { plan "retry" state="ready" { step "work" { } } }"#,
            "retry-plan",
        );
        let run = store
            .create_plan_run(&PlanRunRequest {
                plan: "retry".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
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
    fn an_authorized_revision_preserves_unrelated_work_and_resets_dependents() {
        let store = Store::open_memory("node").unwrap();
        let publish = |source: &str, key: &str| {
            let intent = crate::graph::parse_intent(source, "node").unwrap();
            let planned = store
                .plan(
                    &intent,
                    IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent.plans["revision"].clone()
        };
        let first = publish(
            r#"
version 2
subgraph {
  plan "revision" state="ready" {
    step "owned" { assigned-to "agent/worker"; goal "First goal." }
    step "unrelated" { }
    step "join" { depends-on { step "owned" completed; step "unrelated" completed } }
  }
}
"#,
            "revision-one",
        );
        let run = store
            .create_plan_run(&PlanRunRequest {
                plan: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
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
subgraph {
  plan "revision" state="ready" {
    step "owned" { assigned-to "agent/worker"; goal "A corrected goal." }
    step "unrelated" { }
    step "join" { depends-on { step "owned" completed; step "unrelated" completed } }
  }
}
"#,
            "revision-two",
        );
        let revised = store
            .adopt_plan_revision(
                &run.id,
                &second,
                "agent/node.worker",
                "the first goal was incomplete",
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
        assert_eq!(revised.root_revision, run.root_revision);
        assert_eq!(revised.revision, second.revision);
    }
}
