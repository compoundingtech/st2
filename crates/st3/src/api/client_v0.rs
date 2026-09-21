use super::*;
use axum::http::header::AUTHORIZATION;

const LOCAL_ACTOR: &str = "person/local/session/unix";
const ALL_SCOPES: &[&str] = &[
    "read.projections",
    "terminal.read",
    "terminal.control",
    "control.attention",
    "control.messages",
    "control.launches",
    "control.missions",
    "control.work",
    "control.runtimes",
    "control.pairing",
];
const ACTIONS: &[&str] = &[
    "attention.resolve",
    "message.send",
    "message.read",
    "message.close",
    "launch.create",
    "launch.revise",
    "launch.preview",
    "launch.approve",
    "launch.cancel",
    "mission.start",
    "mission.revise",
    "mission.approve-revision",
    "mission.cancel",
    "work.claim",
    "work.renew",
    "work.progress",
    "work.complete",
    "work.fail",
    "work.release",
    "work.publish-mission",
    "runtime.stop",
    "runtime.restart",
    "runtime.reset",
    "runtime.context-clear",
    "runtime.signal",
    "terminal.input",
    "terminal.resize",
    "terminal.attach",
    "terminal.detach",
    "pairing.revoke",
];
const AVAILABLE_ACTIONS: &[&str] = &[
    "attention.resolve",
    "message.send",
    "message.read",
    "message.close",
    "launch.create",
    "launch.revise",
    "launch.preview",
    "launch.approve",
    "launch.cancel",
    "mission.start",
    "mission.cancel",
    "work.claim",
    "work.renew",
    "work.progress",
    "work.complete",
    "work.fail",
    "work.release",
    "runtime.context-clear",
    "runtime.signal",
    "terminal.input",
    "terminal.resize",
    "terminal.attach",
    "terminal.detach",
    "pairing.revoke",
];

#[derive(Clone, Debug)]
pub(super) struct ClientSession {
    pub(super) actor: String,
    pub(super) transport: &'static str,
    scopes: std::collections::BTreeSet<String>,
}

impl ClientSession {
    fn local() -> Self {
        Self {
            actor: LOCAL_ACTOR.into(),
            transport: "unix",
            scopes: ALL_SCOPES.iter().map(|scope| (*scope).to_owned()).collect(),
        }
    }

    fn allows(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }
}

pub(super) fn capabilities(session: &ClientSession) -> Vec<Value> {
    let mut capabilities = ALL_SCOPES
        .iter()
        .map(|scope| {
            json!({
                "id": scope,
                "version": 0,
                "state": if session.allows(scope) { "granted" } else { "ungranted" }
            })
        })
        .collect::<Vec<_>>();
    capabilities.extend(ACTIONS.iter().map(|action| {
        let scope = action_scope(action).expect("registered client action has a scope");
        let state = if !AVAILABLE_ACTIONS.contains(action) {
            "unavailable"
        } else if session.allows(scope) {
            "granted"
        } else {
            "ungranted"
        };
        json!({ "id": action, "version": 0, "state": state })
    }));
    capabilities
}

fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::FORBIDDEN,
        code: "forbidden".into(),
        message: message.into(),
        details: serde_json::Map::new(),
    }
}

fn validation(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code: "validation-failed".into(),
        message: message.into(),
        details: serde_json::Map::new(),
    }
}

fn stale(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "stale-fence".into(),
        message: message.into(),
        details: serde_json::Map::new(),
    }
}

fn credential_digest(credential: &str) -> String {
    hex::encode(Sha256::digest(credential.as_bytes()))
}

pub(super) fn authenticate(
    state: &AppState,
    request: &Request<Body>,
) -> Result<ClientSession, ApiError> {
    let Some(value) = request.headers().get(AUTHORIZATION) else {
        return Ok(ClientSession::local());
    };
    let value = value
        .to_str()
        .map_err(|_| forbidden("the client authorization header is malformed"))?;
    let credential = value
        .strip_prefix("Bearer ")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| forbidden("the client authorization scheme must be Bearer"))?;
    let digest = credential_digest(credential);
    let page = state
        .store
        .claims_page(None, None, 0, None, true, 10_000)
        .map_err(ApiError::internal)?;
    let paired = page.claims.iter().find(|claim| {
        claim.kind == "custom.client.pairing-completed"
            && claim
                .body
                .pointer("/fields/credential_hash")
                .and_then(Value::as_str)
                == Some(digest.as_str())
    });
    let Some(paired) = paired else {
        return Err(forbidden("the client credential is unknown or expired"));
    };
    let revoked = page.claims.iter().any(|claim| {
        claim.subject == paired.subject
            && claim.kind == "custom.client.pairing-revoked"
            && claim.store_index > paired.store_index
    });
    let expires_at = paired
        .body
        .pointer("/fields/expires_at_unix_ms")
        .and_then(Value::as_u64)
        .map(u128::from)
        .unwrap_or_default();
    if revoked || expires_at <= client_now_ms() {
        return Err(forbidden("the client credential was revoked or expired"));
    }
    let actor = paired
        .body
        .pointer("/fields/session_actor")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::internal("a paired client has no derived actor"))?;
    let scopes = paired
        .body
        .pointer("/fields/scopes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let session = ClientSession {
        actor: actor.into(),
        transport: "fabric-loopback",
        scopes,
    };
    if request.method() == axum::http::Method::GET {
        let scope = if request.uri().path().starts_with("/v1/client/terminals/") {
            "terminal.read"
        } else {
            "read.projections"
        };
        require_scope(&session, scope)?;
    }
    Ok(session)
}

fn require_scope(session: &ClientSession, scope: &str) -> Result<(), ApiError> {
    if session.allows(scope) {
        Ok(())
    } else {
        Err(forbidden(format!(
            "the authenticated client session is not granted `{scope}`"
        )))
    }
}

fn mission_resources(store: &Store) -> anyhow::Result<Vec<Value>> {
    let claims = store.claims_page(None, None, 0, None, false, 100_000)?;
    let mut runs = std::collections::BTreeSet::new();
    for claim in claims.claims {
        if claim.subject.starts_with("mission-run/") {
            runs.insert(claim.subject);
        }
    }
    let mut missions = BTreeMap::<String, Vec<MissionRunView>>::new();
    for run in runs {
        if let Some(run) = store.mission_run(&run)? {
            missions.entry(run.mission.clone()).or_default().push(run);
        }
    }
    let mut values = missions
        .into_iter()
        .map(|(mission, mut runs)| {
            runs.sort_by_key(|run| run.created_at_unix_ms);
            let latest = runs.last().expect("mission has at least one run");
            let state = if runs.iter().any(|run| run.status == "running") {
                "running"
            } else if runs.iter().any(|run| run.status == "standing") {
                "standing"
            } else {
                match latest.status.as_str() {
                    "completed" => "completed",
                    "failed" => "failed",
                    "cancelled" => "cancelled",
                    _ => "ready",
                }
            };
            let run_generations = runs
                .iter()
                .map(|run| (run.subject.clone(), Value::String(run.generation.clone())))
                .collect::<serde_json::Map<_, _>>();
            json!({
                "id": mission,
                "kind": "mission",
                "revision": latest.revision,
                "updated_at": client_timestamp(latest.updated_at_unix_ms),
                "title": latest.mission.strip_prefix("mission/").unwrap_or(&latest.mission),
                "state": state,
                "mission_revision": latest.revision,
                "runs": runs.into_iter().map(|run| run.subject).collect::<Vec<_>>(),
                "run_generations": run_generations,
                "operational": { "layer": "current", "actionable": true, "reasons": [] }
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right["updated_at"]
            .as_str()
            .cmp(&left["updated_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(values)
}

fn runtime_resources(store: &Store, history: bool, at: &str) -> anyhow::Result<Vec<Value>> {
    let status = if history {
        store.status_history(None, None, None)?
    } else {
        store.status(None)?
    };
    let mut values = Vec::new();
    for subject in status.subjects {
        if !matches!(subject.kind.as_deref(), Some("agent" | "exec" | "pty")) {
            continue;
        }
        let fields = subject
            .actual
            .as_ref()
            .map(|actual| actual.get("fields").unwrap_or(actual));
        let Some(runtime_id) = fields
            .and_then(|fields| fields.get("runtime_id"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let observed = fields
            .and_then(|fields| fields.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("pending");
        let state = match observed {
            "ready" | "working" | "idle" => "running",
            "absent" => "stopped",
            other @ ("pending" | "starting" | "running" | "stopping" | "stopped" | "exited"
            | "failed" | "unreachable") => other,
            _ => "pending",
        };
        values.push(json!({
            "id": format!("runtime/{runtime_id}"),
            "kind": "runtime",
            "revision": subject.desired_revision.clone().or(subject.desired_token.clone()).unwrap_or_else(|| runtime_id.into()),
            "updated_at": at,
            "runtime_kind": match subject.kind.as_deref() { Some("pty") => "terminal", Some("exec") => "exec", _ => "agent" },
            "owner_id": subject.subject,
            "state": state,
            "runtime_id": runtime_id,
            "incarnation_id": fields.and_then(|fields| fields.get("incarnation_id")).and_then(Value::as_str),
            "desired_revision": subject.desired_revision.or(subject.desired_token).unwrap_or_else(|| runtime_id.into()),
            "operational": subject.projection
        }));
    }
    values.sort_by(|left, right| {
        left["owner_id"]
            .as_str()
            .cmp(&right["owner_id"].as_str())
            .then_with(|| {
                left["runtime_kind"]
                    .as_str()
                    .cmp(&right["runtime_kind"].as_str())
            })
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(values)
}

fn operation_resources(state: &AppState, at: &str) -> Result<Vec<Value>, ApiError> {
    let report = doctor_report(state)?.0;
    let mut values = report
        .checks
        .into_iter()
        .map(|check| {
            let digest = hex::encode(Sha256::digest(check.name.as_bytes()));
            json!({
                "id": format!("operation/diagnostic-{}", &digest[..16]),
                "kind": "operation",
                "revision": format!("{}:{}", check.name, check.status),
                "updated_at": at,
                "component": match check.name.as_str() { "replication" => "transport", "runtime-drift" | "runtime-ownership" | "pty-runtime" | "driver-readiness" => "runtime", _ => "daemon" },
                "severity": match check.status.as_str() { "fail" => "critical", "warn" => "warning", _ => "info" },
                "state": match check.status.as_str() { "fail" => "failed", "warn" => "degraded", _ => "healthy" },
                "summary": check.message,
                "targets": [],
                "operational": { "layer": "current", "actionable": false, "reasons": ["diagnostic"] }
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        let severity = |value: &Value| match value["severity"].as_str() {
            Some("critical") => 0,
            Some("warning") => 1,
            _ => 2,
        };
        severity(left)
            .cmp(&severity(right))
            .then_with(|| left["component"].as_str().cmp(&right["component"].as_str()))
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(values)
}

pub(super) async fn missions(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    require_scope(&session, "read.projections")?;
    client_page(
        &state,
        &snapshot,
        "missions",
        mission_resources(&state.store).map_err(ApiError::internal)?,
        &query,
    )
    .map(Json)
}

pub(super) async fn mission_detail(
    State(state): State<AppState>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "read.projections")?;
    client_detail(
        mission_resources(&state.store).map_err(ApiError::internal)?,
        "mission",
        &id,
    )
}

pub(super) async fn runtimes(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    require_scope(&session, "read.projections")?;
    let items = runtime_resources(&state.store, query.history, &snapshot.created_at)
        .map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "runtimes", items, &query).map(Json)
}

pub(super) async fn runtime_detail(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "read.projections")?;
    client_detail(
        runtime_resources(&state.store, true, &snapshot.created_at).map_err(ApiError::internal)?,
        "runtime",
        &id,
    )
}

pub(super) async fn operations(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    require_scope(&session, "read.projections")?;
    let items = operation_resources(&state, &snapshot.created_at)?;
    client_page(&state, &snapshot, "operations", items, &query).map(Json)
}

pub(super) async fn operation_detail(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "read.projections")?;
    client_detail(
        operation_resources(&state, &snapshot.created_at)?,
        "operation",
        &id,
    )
}

pub(super) fn timeline_value(
    state: &AppState,
    snapshot: &ClientSnapshot,
    session: &ClientSession,
    id: &str,
    requested_limit: Option<usize>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "read.projections")?;
    let session_id = client_detail_id("session", &id);
    let resource = client_session_resources(&state.store, true, &snapshot.created_at)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|item| item["id"] == session_id)
        .ok_or_else(|| ApiError::not_found(format!("session `{session_id}` does not exist")))?;
    let status = resource["state"].as_str().unwrap_or("waiting");
    let limit = requested_limit
        .unwrap_or(CLIENT_DEFAULT_PAGE_ITEMS)
        .clamp(1, CLIENT_MAX_PAGE_ITEMS);
    let item = json!({
        "id": format!("timeline-entry/{}/0", session_id.trim_start_matches("session/")),
        "sequence": 0,
        "revision": 1,
        "timestamp": resource["updated_at"],
        "role": "system",
        "type": "status",
        "final": status != "running" && status != "waiting",
        "body": { "status": status }
    });
    Ok(Json(json!({
        "kind": "timeline-page",
        "session_id": session_id,
        "items": [item],
        "page": { "limit": limit, "has_more": false, "next_cursor": null, "cursor_expires_at": null }
    })))
}

#[derive(Default, Deserialize)]
pub(super) struct EventsQuery {
    after: Option<String>,
    limit: Option<usize>,
    wait_ms: Option<u64>,
}

fn decode_event_cursor(node: &str, cursor: Option<&str>) -> Result<u64, ApiError> {
    let Some(cursor) = cursor else { return Ok(0) };
    let mut parts = cursor.split('/');
    if parts.next() != Some("event-cursor") || parts.next() != Some(node) {
        return Err(ApiError {
            status: StatusCode::GONE,
            code: "cursor-gap".into(),
            message: "the event cursor belongs to another host or retention epoch".into(),
            details: serde_json::Map::from_iter([("full_resync".into(), Value::Bool(true))]),
        });
    }
    parts
        .next()
        .and_then(|value| value.parse().ok())
        .filter(|_| parts.next().is_none())
        .ok_or_else(|| validation("the event cursor is malformed"))
}

pub(super) async fn events(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "read.projections")?;
    let after = decode_event_cursor(&state.node, query.after.as_deref())?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(query.wait_ms.unwrap_or(0).min(30_000));
    let records = loop {
        let records = state
            .store
            .events_after(after, None)
            .map_err(ApiError::internal)?;
        if !records.is_empty() || tokio::time::Instant::now() >= deadline {
            break records;
        }
        let mut changed = state.event_notify.subscribe();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(remaining, changed.changed())
            .await
            .is_err()
        {
            break Vec::new();
        }
    };
    let has_more = records.len() > limit;
    let records = records.into_iter().take(limit).collect::<Vec<_>>();
    let resume = records
        .last()
        .map(|record| record.store_index)
        .unwrap_or(after);
    let items = records
        .into_iter()
        .map(|record| {
            let previous = format!(
                "event-cursor/{}/{}",
                state.node,
                record.store_index.saturating_sub(1)
            );
            let next = format!("event-cursor/{}/{}", state.node, record.store_index);
            json!({
                "id": format!("projection-event/{}/{}", state.node, record.store_index),
                "epoch": state.node,
                "sequence": record.store_index,
                "previous_cursor": previous,
                "next_cursor": next,
                "timestamp": snapshot.created_at,
                "type": "upsert",
                "resource_ids": [record.subject],
                "snapshot_id": snapshot.id,
                "body": { "claim_kind": record.kind, "projection": record.body }
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "kind": "event-page",
        "oldest_cursor": format!("event-cursor/{}/0", state.node),
        "resume_cursor": format!("event-cursor/{}/{resume}", state.node),
        "items": items,
        "has_more": has_more
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PairingBegin {
    api_version: String,
    device_name: String,
}

pub(super) async fn pairing_begin(
    State(state): State<AppState>,
    Extension(session): Extension<ClientSession>,
    Json(request): Json<PairingBegin>,
) -> Result<Json<Value>, ApiError> {
    if session.transport != "unix" {
        return Err(forbidden("pairing can only begin on the local Unix API"));
    }
    if request.api_version != CLIENT_API_VERSION
        || request.device_name.trim().is_empty()
        || request.device_name.len() > 120
    {
        return Err(validation(
            "the pairing request has an invalid version or device name",
        ));
    }
    let mut random = [0_u8; 10];
    getrandom::fill(&mut random).map_err(ApiError::internal)?;
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let code = random[..8]
        .iter()
        .map(|byte| ALPHABET[*byte as usize % ALPHABET.len()] as char)
        .collect::<String>();
    let stable = hex::encode(Sha256::digest(
        format!("{}:{code}", request.device_name).as_bytes(),
    ));
    let pairing_id = format!("pairing/{}", &stable[..24]);
    let subject = format!("custom/client/pairing-{}", &stable[..24]);
    let expires_at = client_now_ms() + 300_000;
    state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: "custom.client.pairing-begun".into(),
            actor: Some(session.actor),
            fields: BTreeMap::from([
                ("pairing_id".into(), Value::String(pairing_id.clone())),
                ("device_name".into(), Value::String(request.device_name)),
                ("code_hash".into(), Value::String(credential_digest(&code))),
                ("expires_at_unix_ms".into(), json!(expires_at)),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(
        json!({ "kind": "pairing-challenge", "pairing_id": pairing_id, "code": code, "expires_at": client_timestamp(expires_at) }),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PairingComplete {
    api_version: String,
    code: String,
    device_public_key: String,
}

pub(super) async fn pairing_complete(
    State(state): State<AppState>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PairingComplete>,
) -> Result<Json<Value>, ApiError> {
    if request.api_version != CLIENT_API_VERSION || request.device_public_key.len() < 32 {
        return Err(validation(
            "the pairing completion has an invalid version or public key",
        ));
    }
    let pairing_id = client_detail_id("pairing", &id);
    let claims = state
        .store
        .claims_page(None, None, 0, None, true, 10_000)
        .map_err(ApiError::internal)?;
    let begun = claims
        .claims
        .iter()
        .find(|claim| {
            claim.kind == "custom.client.pairing-begun"
                && claim
                    .body
                    .pointer("/fields/pairing_id")
                    .and_then(Value::as_str)
                    == Some(pairing_id.as_str())
        })
        .ok_or_else(|| ApiError::not_found(format!("pairing `{pairing_id}` does not exist")))?;
    let used = claims.claims.iter().any(|claim| {
        claim.subject == begun.subject && claim.kind == "custom.client.pairing-completed"
    });
    let valid_code = begun
        .body
        .pointer("/fields/code_hash")
        .and_then(Value::as_str)
        == Some(credential_digest(&request.code).as_str());
    let expires = begun
        .body
        .pointer("/fields/expires_at_unix_ms")
        .and_then(Value::as_u64)
        .map(u128::from)
        .unwrap_or_default();
    if used || !valid_code || expires <= client_now_ms() {
        return Err(forbidden(
            "the pairing code is invalid, expired, or already used",
        ));
    }
    let mut secret = [0_u8; 32];
    getrandom::fill(&mut secret).map_err(ApiError::internal)?;
    let credential = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let device_hash = hex::encode(Sha256::digest(request.device_public_key.as_bytes()));
    let device_id = format!("device/{}", &device_hash[..24]);
    let actor_suffix = &device_hash[..16];
    let session_actor = format!("person/local/session/{actor_suffix}");
    let expires_at = client_now_ms() + 30 * 24 * 60 * 60 * 1_000;
    let scopes = vec!["read.projections", "terminal.read"];
    state
        .store
        .append_claim(&ClaimInput {
            subject: begun.subject.clone(),
            kind: "custom.client.pairing-completed".into(),
            actor: Some(session.actor),
            fields: BTreeMap::from([
                ("pairing_id".into(), Value::String(pairing_id)),
                ("device_id".into(), Value::String(device_id.clone())),
                ("session_actor".into(), Value::String(session_actor.clone())),
                (
                    "credential_hash".into(),
                    Value::String(credential_digest(&credential)),
                ),
                (
                    "device_public_key".into(),
                    Value::String(request.device_public_key),
                ),
                ("scopes".into(), json!(scopes)),
                ("expires_at_unix_ms".into(), json!(expires_at)),
            ]),
            evidence: vec![begun.id.clone()],
            expected_subject: None,
            idempotency_key: None,
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(
        json!({ "kind": "paired-session", "device_id": device_id, "session_actor": session_actor, "credential": credential, "scopes": scopes, "expires_at": client_timestamp(expires_at) }),
    ))
}

fn terminal_subject(id: &str) -> String {
    id.strip_prefix("terminal/").unwrap_or(id).to_owned()
}

pub(super) async fn terminal_screen(
    State(state): State<AppState>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "terminal.read")?;
    let subject = terminal_subject(&id);
    let live = live_session(&state, &subject, None)?;
    if !live.terminal {
        return Err(validation(
            "the requested runtime does not expose a terminal",
        ));
    }
    let screen = st_runtime::PtyRuntime::new(state.pty_root.clone())
        .with_binary(state.pty_binary.to_string_lossy())
        .screen(&live.runtime_id)
        .map_err(ApiError::internal)?;
    let mut lines = screen.lines().take(200).enumerate().map(|(row, text)| json!({ "row": row, "text": text, "redacted": false, "truncated": text.len() > 4096 })).collect::<Vec<_>>();
    if lines.is_empty() {
        lines.push(json!({ "row": 0, "text": "", "redacted": false, "truncated": false }));
    }
    let rows = lines.len().max(1);
    let columns = lines
        .iter()
        .filter_map(|line| line["text"].as_str().map(str::chars).map(Iterator::count))
        .max()
        .unwrap_or(1)
        .max(1);
    Ok(Json(json!({
        "kind": "terminal-screen", "terminal_id": client_detail_id("terminal", &id),
        "runtime_incarnation": live.incarnation_id, "rows": rows, "columns": columns,
        "cursor": { "row": rows - 1, "column": 0, "visible": true }, "title": live.runtime_id,
        "lines": lines, "next_sequence": state.store.index().map_err(ApiError::internal)?, "truncated": screen.lines().count() > 200
    })))
}

#[derive(Default, Deserialize)]
pub(super) struct TerminalStreamQuery {
    after: Option<u64>,
    incarnation: Option<String>,
}

pub(super) async fn terminal_stream(
    State(state): State<AppState>,
    Extension(session): Extension<ClientSession>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<TerminalStreamQuery>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&session, "terminal.read")?;
    let subject = terminal_subject(&id);
    let live = live_session(&state, &subject, query.incarnation.as_deref())?;
    let sequence = state.store.index().map_err(ApiError::internal)?;
    let mut frames = Vec::new();
    if query.after.is_none() || query.after.is_some_and(|after| after > sequence) {
        let screen = terminal_screen(
            State(state.clone()),
            Extension(session),
            AxumPath(id.clone()),
        )
        .await?
        .0;
        frames.push(json!({
            "id": format!("terminal-frame/{}/{}", id.trim_start_matches("terminal/"), sequence),
            "terminal_id": client_detail_id("terminal", &id), "runtime_incarnation": live.incarnation_id,
            "sequence": sequence, "type": "resync", "timestamp": client_timestamp(client_now_ms()), "body": { "screen": screen }
        }));
    }
    Ok(Json(
        json!({ "kind": "terminal-frame-page", "terminal_id": client_detail_id("terminal", &id), "runtime_incarnation": live.incarnation_id, "frames": frames, "resume_sequence": sequence.saturating_add(1) }),
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fence {
    snapshot_id: String,
    #[serde(default)]
    subject_revisions: BTreeMap<String, String>,
    mission_generation: Option<String>,
    step_definition: Option<String>,
    attempt: Option<u32>,
    readiness_epoch: Option<u64>,
    runtime_incarnation: Option<String>,
    terminal_sequence: Option<u64>,
    preview_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActionRequest {
    api_version: String,
    id: String,
    #[serde(rename = "type")]
    action_type: String,
    idempotency_key: String,
    fence: Fence,
    parameters: Value,
}

fn action_scope(action: &str) -> Option<&'static str> {
    Some(match action.split_once('.')?.0 {
        "attention" => "control.attention",
        "message" => "control.messages",
        "launch" => "control.launches",
        "mission" => "control.missions",
        "work" => "control.work",
        "runtime" => "control.runtimes",
        "terminal" => {
            if matches!(action, "terminal.attach" | "terminal.detach") {
                "terminal.read"
            } else {
                "terminal.control"
            }
        }
        "pairing" => "control.pairing",
        _ => return None,
    })
}

fn parameter_string(parameters: &Value, key: &str) -> Result<String, ApiError> {
    parameters
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| validation(format!("action parameters require `{key}`")))
}

fn contains_identity_selector(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            matches!(
                key.as_str(),
                "actor" | "credential" | "fleet_secret" | "shared_secret"
            ) || contains_identity_selector(value)
        }),
        Value::Array(values) => values.iter().any(contains_identity_selector),
        _ => false,
    }
}

fn validate_work_fence(state: &AppState, target: &str, fence: &Fence) -> Result<(), ApiError> {
    let work = state
        .store
        .work(None, true)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|work| work.subject == target || work.subject == client_detail_id("step-run", target))
        .ok_or_else(|| ApiError::not_found(format!("work `{target}` does not exist")))?;
    if fence.mission_generation.as_deref() != Some(work.generation.as_str())
        || fence.step_definition.as_deref() != Some(work.definition_hash.as_str())
        || fence.attempt != Some(work.attempt)
        || fence.readiness_epoch != Some(u64::from(work.readiness_epoch))
        || fence
            .runtime_incarnation
            .as_deref()
            .is_some_and(|incarnation| work.claim_incarnation.as_deref() != Some(incarnation))
    {
        return Err(stale(format!(
            "the execution fence for `{}` is stale",
            work.subject
        )));
    }
    Ok(())
}

fn validate_launch_fence(state: &AppState, target: &str, fence: &Fence) -> Result<(), ApiError> {
    let launch_id = launch_session_id(target);
    let launch = state
        .store
        .planning_session(launch_id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("launch `{target}` does not exist")))?;
    let resource_id = format!("launch/{}", launch.id);
    let expected = format!("launch/{}", launch.updated_at_unix_ms);
    if fence.subject_revisions.get(&resource_id) != Some(&expected) {
        return Err(stale(format!(
            "the launch revision fence for `{resource_id}` is stale"
        )));
    }
    Ok(())
}

fn validate_fence(
    state: &AppState,
    _snapshot: &ClientSnapshot,
    fence: &Fence,
) -> Result<(), ApiError> {
    let parsed = fence
        .snapshot_id
        .strip_prefix("snapshot/")
        .and_then(|value| value.rsplit_once('/'))
        .and_then(|(host_and_index, _fingerprint)| host_and_index.rsplit_once('/'));
    let current_index = state.store.index().map_err(ApiError::internal)?;
    let expected_host = state.node.replace(char::is_whitespace, "-");
    if !parsed.is_some_and(|(host, index)| {
        host == expected_host && index.parse::<u64>().ok() == Some(current_index)
    }) {
        return Err(stale(
            "the client snapshot changed before the action was submitted",
        ));
    }
    for (subject, revision) in &fence.subject_revisions {
        let current = if let Some(id) = subject.strip_prefix("launch/") {
            state
                .store
                .planning_session(id)
                .map_err(ApiError::internal)?
                .map(|launch| format!("launch/{}", launch.updated_at_unix_ms))
        } else {
            state
                .store
                .claims_for(subject, None)
                .map_err(ApiError::internal)?
                .last()
                .map(|claim| claim.id.clone())
        };
        if current.as_deref() != Some(revision) {
            return Err(stale(format!(
                "the revision fence for `{subject}` is stale"
            )));
        }
    }
    Ok(())
}

async fn dispatch_action(
    state: &AppState,
    session: &ClientSession,
    request: &ActionRequest,
) -> Result<Vec<String>, ApiError> {
    let p = &request.parameters;
    match request.action_type.as_str() {
        "attention.resolve" => {
            let target = parameter_string(p, "attention_id")?;
            let result = resolve_attention(
                State(state.clone()),
                AxumPath(target),
                Json(AttentionResolveRequest {
                    outcome: parameter_string(p, "outcome")?,
                    reason: p.get("reason").and_then(Value::as_str).map(str::to_owned),
                    actor: session.actor.clone(),
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "message.send" => {
            let result = send_message(
                State(state.clone()),
                Json(MessageSendRequest {
                    idempotency_key: request.idempotency_key.clone(),
                    from: session.actor.clone(),
                    to: parameter_string(p, "to")?,
                    content: parameter_string(p, "content")?,
                    title: p.get("title").and_then(Value::as_str).map(str::to_owned),
                    in_reply_to: p
                        .get("in_reply_to")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    tags: p
                        .get("tags")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "message.read" | "message.close" => {
            let target = parameter_string(p, "target_id")?;
            let lifecycle = if request.action_type == "message.read" {
                "read"
            } else {
                "closed"
            };
            let result = post_message_claim(
                State(state.clone()),
                AxumPath(target),
                Json(MessageLifecycleRequest {
                    lifecycle: lifecycle.into(),
                    actor: Some(session.actor.clone()),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "launch.create" => {
            let target = p
                .get("target")
                .and_then(Value::as_object)
                .ok_or_else(|| validation("launch creation requires a typed target"))?;
            let target_type = target
                .get("type")
                .and_then(Value::as_str)
                .ok_or_else(|| validation("launch target requires `type`"))?;
            let (mission, run, workspace) = match target_type {
                "new-mission" => (
                    target
                        .get("mission_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            validation("a new-mission launch target requires `mission_id`")
                        })?
                        .trim_start_matches("mission/")
                        .to_owned(),
                    None,
                    target
                        .get("workspace")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            validation("a new-mission launch target requires `workspace`")
                        })?
                        .to_owned(),
                ),
                "mission-run" => {
                    let run_id = target
                        .get("mission_run_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            validation("a mission-run launch target requires `mission_run_id`")
                        })?;
                    let current = state
                        .store
                        .mission_run(run_id)
                        .map_err(ApiError::internal)?
                        .ok_or_else(|| {
                            ApiError::not_found(format!("mission run `{run_id}` does not exist"))
                        })?;
                    let generation = target
                        .get("generation_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            validation("a mission-run launch target requires `generation_id`")
                        })?;
                    if generation != current.generation
                        || request.fence.mission_generation.as_deref()
                            != Some(current.generation.as_str())
                    {
                        return Err(stale("the launch target generation is stale"));
                    }
                    (
                        current.mission.trim_start_matches("mission/").to_owned(),
                        Some(current.subject),
                        current.workspace,
                    )
                }
                _ => return Err(validation("launch target type is invalid")),
            };
            let launch = start_planning_session(
                State(state.clone()),
                Json(PlanningSessionStartRequest {
                    mission,
                    run,
                    request: parameter_string(p, "request")?.into_bytes(),
                    workspace,
                    requester: Some(session.actor.clone()),
                    model: None,
                    effort: None,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![format!("launch/{}", launch.id)])
        }
        "launch.revise" => {
            let launch_id = parameter_string(p, "launch_id")?;
            validate_launch_fence(state, &launch_id, &request.fence)?;
            let launch = revise_planning_session(
                State(state.clone()),
                AxumPath(launch_session_id(&launch_id).to_owned()),
                Json(PlanningRevisionRequest {
                    actor: session.actor.clone(),
                    feedback: parameter_string(p, "feedback")?.into_bytes(),
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![format!("launch/{}", launch.id)])
        }
        "launch.preview" => {
            let launch_id = parameter_string(p, "launch_id")?;
            validate_launch_fence(state, &launch_id, &request.fence)?;
            let variant_id = parameter_string(p, "variant_id")?;
            let variant = variant_id.rsplit('/').next().unwrap_or(&variant_id);
            let launch = preview_named_planning_candidate(
                State(state.clone()),
                AxumPath((launch_session_id(&launch_id).to_owned(), variant.to_owned())),
            )
            .await?
            .0;
            Ok(vec![
                format!("launch/{}", launch.id),
                format!("launch-variant/{}/{}", launch.id, variant),
            ])
        }
        "launch.approve" => {
            let launch_id = parameter_string(p, "launch_id")?;
            validate_launch_fence(state, &launch_id, &request.fence)?;
            let requested_variant = parameter_string(p, "variant_id")?;
            let requested_variant = requested_variant
                .rsplit('/')
                .next()
                .unwrap_or(&requested_variant);
            let launch = state
                .store
                .planning_session(launch_session_id(&launch_id))
                .map_err(ApiError::internal)?
                .ok_or_else(|| {
                    ApiError::not_found(format!("launch `{launch_id}` does not exist"))
                })?;
            if launch
                .candidate
                .as_ref()
                .map(|candidate| candidate.variant.as_str())
                != Some(requested_variant)
            {
                return Err(stale("the selected launch variant is stale"));
            }
            let launch = approve_planning_session(
                State(state.clone()),
                AxumPath(launch_session_id(&launch_id).to_owned()),
                Json(PlanningApprovalRequest {
                    actor: session.actor.clone(),
                    preview_hash: request.fence.preview_token.clone().ok_or_else(|| {
                        validation("launch approval requires a preview token fence")
                    })?,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![format!("launch/{}", launch.id)])
        }
        "launch.cancel" => {
            let launch_id = parameter_string(p, "target_id")?;
            validate_launch_fence(state, &launch_id, &request.fence)?;
            let launch = cancel_planning_session(
                State(state.clone()),
                AxumPath(launch_session_id(&launch_id).to_owned()),
                Json(PlanningCancelRequest {
                    actor: session.actor.clone(),
                    reason: p.get("reason").and_then(Value::as_str).map(str::to_owned),
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![format!("launch/{}", launch.id)])
        }
        action @ ("work.claim" | "work.renew" | "work.progress" | "work.complete" | "work.fail"
        | "work.release") => {
            let target = parameter_string(p, "target_id")?;
            validate_work_fence(state, &target, &request.fence)?;
            let result = state
                .store
                .work_action(
                    &target,
                    action.trim_start_matches("work."),
                    &WorkRequest {
                        actor: Some(session.actor.clone()),
                        incarnation: request.fence.runtime_incarnation.clone(),
                        summary: p.get("summary").and_then(Value::as_str).map(str::to_owned),
                        reason: p.get("reason").and_then(Value::as_str).map(str::to_owned),
                        evidence: p
                            .get("evidence")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect(),
                        idempotency_key: request.idempotency_key.clone(),
                    },
                )
                .map_err(ApiError::bad)?;
            signal_changed(state);
            Ok(vec![result.subject])
        }
        "mission.start" => {
            let inputs = p
                .get("inputs")
                .and_then(Value::as_object)
                .ok_or_else(|| validation("mission start requires an `inputs` object"))?
                .iter()
                .map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.clone(), value.to_owned()))
                        .ok_or_else(|| validation("mission input values must be strings"))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let result = start_mission_run(
                State(state.clone()),
                Json(MissionRunRequest {
                    mission: parameter_string(p, "mission_id")?,
                    revision: None,
                    workspace: parameter_string(p, "workspace")?,
                    requester: Some(session.actor.clone()),
                    mode: Some("run".into()),
                    inputs,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "mission.cancel" => {
            let target = parameter_string(p, "target_id")?;
            let current = state
                .store
                .mission_run(&target)
                .map_err(ApiError::internal)?
                .ok_or_else(|| {
                    ApiError::not_found(format!("mission run `{target}` does not exist"))
                })?;
            if request.fence.mission_generation.as_deref() != Some(current.generation.as_str()) {
                return Err(stale("the mission generation fence is stale"));
            }
            state
                .store
                .set_mission_run_state(
                    &current.subject,
                    "cancelled",
                    "terminal",
                    p.get("reason").and_then(Value::as_str),
                )
                .map_err(ApiError::internal)?;
            signal_changed(state);
            Ok(vec![current.subject])
        }
        "terminal.input" => {
            let mode = match parameter_string(p, "mode")?.as_str() {
                "line" => SessionInputMode::Line,
                "raw" => SessionInputMode::Raw,
                "key" => SessionInputMode::Key,
                _ => return Err(validation("terminal input mode is invalid")),
            };
            let target = terminal_subject(&parameter_string(p, "terminal_id")?);
            let result = input_session(
                State(state.clone()),
                AxumPath(target),
                Json(SessionInputRequest {
                    expected_incarnation: request.fence.runtime_incarnation.clone().ok_or_else(
                        || validation("terminal input requires a runtime incarnation fence"),
                    )?,
                    mode,
                    value: parameter_string(p, "value")?,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "terminal.resize" => {
            let target = terminal_subject(&parameter_string(p, "terminal_id")?);
            let rows = p
                .get("rows")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or_else(|| validation("terminal resize rows must fit a positive u16"))?;
            let columns = p
                .get("columns")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or_else(|| validation("terminal resize columns must fit a positive u16"))?;
            let live = live_session(state, &target, request.fence.runtime_incarnation.as_deref())?;
            if !live.terminal {
                return Err(validation("terminal resize requires a terminal runtime"));
            }
            let socket = state.pty_root.join(format!("{}.sock", live.runtime_id));
            let runtime_id = live.runtime_id.clone();
            tokio::task::spawn_blocking(move || {
                let stream = std::os::unix::net::UnixStream::connect(&socket)?;
                let mut connection = pty_core::client::SessionConnection::attach_over(
                    stream,
                    &runtime_id,
                    rows,
                    columns,
                    Some(Duration::from_secs(2)),
                )?;
                connection.resize(rows, columns);
                connection.disconnect();
                anyhow::Ok(())
            })
            .await
            .map_err(ApiError::internal)?
            .map_err(ApiError::internal)?;
            Ok(vec![client_detail_id(
                "terminal",
                &parameter_string(p, "terminal_id")?,
            )])
        }
        "runtime.context-clear" => {
            let target = terminal_subject(&parameter_string(p, "target_id")?);
            let result = clear_context(
                State(state.clone()),
                AxumPath(target),
                Json(ContextClearRequest {
                    expected_incarnation: request.fence.runtime_incarnation.clone().ok_or_else(
                        || validation("runtime control requires an incarnation fence"),
                    )?,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "runtime.signal" => {
            let target = terminal_subject(&parameter_string(p, "target_id")?);
            let result = signal_session(
                State(state.clone()),
                AxumPath(target),
                Json(SessionSignalRequest {
                    expected_incarnation: request.fence.runtime_incarnation.clone().ok_or_else(
                        || validation("runtime control requires an incarnation fence"),
                    )?,
                    signal: parameter_string(p, "signal")?,
                    idempotency_key: request.idempotency_key.clone(),
                }),
            )
            .await?
            .0;
            Ok(vec![result.subject])
        }
        "terminal.attach" | "terminal.detach" => {
            let target = parameter_string(p, "target_id")?;
            let subject = terminal_subject(&target);
            live_session(
                state,
                &subject,
                request.fence.runtime_incarnation.as_deref(),
            )?;
            Ok(vec![target])
        }
        "pairing.revoke" => {
            let device = parameter_string(p, "target_id")?;
            let claims = state
                .store
                .claims_page(None, None, 0, None, true, 10_000)
                .map_err(ApiError::internal)?;
            let paired = claims
                .claims
                .iter()
                .find(|claim| {
                    claim.kind == "custom.client.pairing-completed"
                        && claim
                            .body
                            .pointer("/fields/device_id")
                            .and_then(Value::as_str)
                            == Some(device.as_str())
                })
                .ok_or_else(|| {
                    ApiError::not_found(format!("paired device `{device}` does not exist"))
                })?;
            state
                .store
                .append_claim(&ClaimInput {
                    subject: paired.subject.clone(),
                    kind: "custom.client.pairing-revoked".into(),
                    actor: Some(session.actor.clone()),
                    fields: BTreeMap::from([("device_id".into(), Value::String(device.clone()))]),
                    evidence: vec![paired.id.clone()],
                    expected_subject: None,
                    idempotency_key: Some(request.idempotency_key.clone()),
                })
                .map_err(ApiError::bad)?;
            signal_changed(state);
            Ok(vec![device])
        }
        _ => Err(ApiError {
            status: StatusCode::NOT_IMPLEMENTED,
            code: "unsupported-capability".into(),
            message: format!(
                "action `{}` is declared but is not available on this daemon",
                request.action_type
            ),
            details: serde_json::Map::new(),
        }),
    }
}

pub(super) async fn action(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<ClientSession>,
    Json(request): Json<ActionRequest>,
) -> Result<Json<Value>, ApiError> {
    if request.api_version != CLIENT_API_VERSION
        || !request.id.starts_with("action/")
        || !(16..=256).contains(&request.idempotency_key.len())
    {
        return Err(validation(
            "the action version, ID, or idempotency key is invalid",
        ));
    }
    if contains_identity_selector(&request.parameters) {
        return Err(validation(
            "client actions cannot select an actor, credential, or fleet secret",
        ));
    }
    let scope = action_scope(&request.action_type)
        .ok_or_else(|| validation("the action type is unknown"))?;
    require_scope(&session, scope)?;
    let encoded = serde_json::to_vec(&request).map_err(ApiError::internal)?;
    let request_digest = hex::encode(Sha256::digest(&encoded));
    let receipt_digest = hex::encode(Sha256::digest(
        format!("{}:{}", session.actor, request.idempotency_key).as_bytes(),
    ));
    let receipt_subject = format!("custom/client/action-{}", &receipt_digest[..32]);
    if let Some(receipt) = state
        .store
        .claims_for(&receipt_subject, Some("custom.client.action-result"))
        .map_err(ApiError::internal)?
        .last()
    {
        let old_digest = receipt
            .body
            .pointer("/fields/request_digest")
            .and_then(Value::as_str);
        if old_digest != Some(request_digest.as_str()) {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                code: "idempotency-conflict".into(),
                message: "the idempotency key was already used for a different action".into(),
                details: serde_json::Map::new(),
            });
        }
        let mut result = receipt
            .body
            .pointer("/fields/result")
            .cloned()
            .ok_or_else(|| ApiError::internal("the client action receipt has no result"))?;
        result["snapshot_id"] = Value::String(new_client_snapshot(&state).id);
        return Ok(Json(result));
    }
    validate_fence(&state, &snapshot, &request.fence)?;
    if request.action_type.starts_with("terminal.")
        && request.fence.terminal_sequence != Some(state.store.index().map_err(ApiError::internal)?)
    {
        return Err(stale("the terminal sequence fence is stale"));
    }
    let affected = dispatch_action(&state, &session, &request).await?;
    let operation_id = format!("operation/client-{}", &request_digest[..24]);
    let mut result = json!({ "kind": "action-result", "action_id": request.id, "operation_id": operation_id, "status": "completed", "affected_ids": affected });
    state
        .store
        .append_claim(&ClaimInput {
            subject: receipt_subject,
            kind: "custom.client.action-result".into(),
            actor: Some(session.actor),
            fields: BTreeMap::from([
                ("request_digest".into(), Value::String(request_digest)),
                ("result".into(), result.clone()),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    result["snapshot_id"] = Value::String(new_client_snapshot(&state).id);
    Ok(Json(result))
}
