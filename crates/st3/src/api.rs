use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Extension, Path as AxumPath, Query, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{Notify, watch};
use tower::ServiceExt as _;

use crate::archive::hydrate_eval;
use crate::graph::{parse_intent, resolve_document_references};
use crate::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, AttentionItemView, AttentionRequest,
    AttentionRequestView, AttentionResolveRequest, ClaimInput, ClaimRecord, ClaimsPage,
    ClientPageInfo, ClientResourcePage, ContextClearRequest, DoctorCheck, DoctorReport,
    DocumentPutRequest, DocumentVersion, EvalStartRequest, EvalStartResponse, EvalStatus,
    EventRecord, GateResultRequest, HumanReviewView, LaunchApproveAndStartRequest,
    LaunchApproveAndStartView, LaunchDecisionAnswerRequest, LaunchDecisionRequest,
    LaunchStartRequest, MAX_EVAL_TIMEOUT_MS, MessageLifecycleRequest, MessageSendRequest,
    MessageView, MissionOutputView, MissionProductionRequest, MissionRequest, MissionResponse,
    MissionRevisionRequest, MissionRunRequest, MissionRunView, OperationalRepairApplyRequest,
    OperationalRepairPlan, OperationalRepairResult, PlanningApprovalRequest, PlanningCancelRequest,
    PlanningCandidateSubmitRequest, PlanningProposalRequest, PlanningRevisionRequest,
    PlanningSessionStartRequest, PlanningSessionView, QuickAgentRequest, QuickAgentResponse,
    ReplicaRecordView, ReplicationExportRequest, ReplicationExportResponse,
    ReplicationPeerFailureRequest, ReplicationReceiveRequest, ReplicationReceiveResponse,
    ReplicationRepairRequest, ReplicationStatus, ResourceUnwatchRequest, ResourceWatchRequest,
    ResourceWatchView, ReviewRequest, RevisionApprovalRequest, RevisionCancelRequest,
    RevisionCutover, RevisionProposalView, RevisionSubmissionView, RunGenerationView,
    SessionControlResponse, SessionInputMode, SessionInputRequest, SessionLogChunk, SessionScreen,
    SessionSignalRequest, St3Error, StatusResponse, StepRunView, WorkRequest, WorkWakeRequest,
};
use crate::store::Store;

mod client_v0;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub notify: Arc<Notify>,
    pub event_notify: watch::Sender<u64>,
    pub node: String,
    pub state_dir: std::path::PathBuf,
    pub pty_root: std::path::PathBuf,
    pub pty_binary: std::path::PathBuf,
    pub fleet_id: Option<String>,
    pub configured_peers: Vec<String>,
}

const CLIENT_API_VERSION: &str = "st3.client.v0";
const CLIENT_PROJECTION_VERSION: &str = "client-projection.v0";
const CLIENT_DEFAULT_PAGE_ITEMS: usize = 50;
const CLIENT_MAX_PAGE_ITEMS: usize = 200;
const CLIENT_PAGE_TTL_MS: u128 = 15 * 60 * 1_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClientSnapshot {
    id: String,
    host_id: String,
    store_index: u64,
    projection_version: String,
    created_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ClientListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    #[serde(default)]
    history: bool,
    person: Option<String>,
    actor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClientPageCursor {
    snapshot: ClientSnapshot,
    collection: String,
    offset: usize,
    limit: usize,
    history: bool,
    person: Option<String>,
    actor: Option<String>,
    expires_at_unix_ms: u128,
}

fn signal_changed(state: &AppState) {
    state.notify.notify_one();
    state
        .event_notify
        .send_modify(|generation| *generation = generation.saturating_add(1));
    let _ = fs::write(
        state.state_dir.join("replication.wake"),
        format!("{}\n", uuid::Uuid::now_v7()),
    );
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
    details: serde_json::Map<String, Value>,
}

impl ApiError {
    fn bad(error: St3Error) -> Self {
        let status = match error.code {
            "stale-subject"
            | "missing-subject-token"
            | "stale-document-token"
            | "stale-incarnation"
            | "stale-launch-preview" => StatusCode::CONFLICT,
            "internal" => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        };
        Self {
            status,
            code: error.code.into(),
            message: error.message,
            details: error.details,
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal".into(),
            message: error.to_string(),
            details: serde_json::Map::new(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not-found".into(),
            message: message.into(),
            details: serde_json::Map::new(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "code": self.code, "message": self.message, "details": self.details })),
        )
            .into_response()
    }
}

pub fn router(state: AppState) -> Router {
    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/client/capabilities", get(client_capabilities))
        .route("/v1/client/attention", get(client_attention))
        .route("/v1/client/attention/{*id}", get(client_attention_detail))
        .route("/v1/client/messages", get(client_messages))
        .route("/v1/client/messages/{*id}", get(client_messages_detail))
        .route("/v1/client/launches", get(client_launches))
        .route(
            "/v1/client/launches/{id}/variants",
            get(client_launch_variants),
        )
        .route(
            "/v1/client/launches/{id}/variants/{variant}",
            get(client_launch_variant_detail),
        )
        .route(
            "/v1/client/launches/{id}/decisions",
            get(client_launch_decisions),
        )
        .route(
            "/v1/client/launches/{id}/decisions/{decision}",
            get(client_launch_decision_detail),
        )
        .route(
            "/v1/client/launches/{id}/approvals",
            get(client_launch_approvals),
        )
        .route(
            "/v1/client/launches/{id}/approvals/{approval}",
            get(client_launch_approval_detail),
        )
        .route("/v1/client/launches/{id}", get(client_launches_detail))
        .route("/v1/client/work", get(client_work))
        .route("/v1/client/work/{*id}", get(client_work_detail))
        .route("/v1/client/agents", get(client_agents))
        .route("/v1/client/agents/{*id}", get(client_agents_detail))
        .route("/v1/client/history", get(client_history))
        .route("/v1/client/history/{*id}", get(client_history_detail))
        .route("/v1/client/sessions", get(client_sessions))
        .route("/v1/client/sessions/{*id}", get(client_sessions_detail))
        .route("/v1/client/missions", get(client_v0::missions))
        .route("/v1/client/missions/{*id}", get(client_v0::mission_detail))
        .route("/v1/client/runtimes", get(client_v0::runtimes))
        .route("/v1/client/runtimes/{*id}", get(client_v0::runtime_detail))
        .route("/v1/client/operations", get(client_v0::operations))
        .route(
            "/v1/client/operations/{*id}",
            get(client_v0::operation_detail),
        )
        .route("/v1/client/events", get(client_v0::events))
        .route("/v1/client/actions", post(client_v0::action))
        .route("/v1/client/pairings", post(client_v0::pairing_begin))
        .route(
            "/v1/client/pairings/{id}/complete",
            post(client_v0::pairing_complete),
        )
        .route(
            "/v1/client/terminals/{id}/screen",
            get(client_v0::terminal_screen),
        )
        .route(
            "/v1/client/terminals/{id}/stream",
            get(client_v0::terminal_stream),
        )
        .route("/v1/schema", get(schema))
        .route("/v1/intent/mission", post(mission))
        .route("/v1/intent/apply", post(apply))
        .route("/v1/missions/{id}", get(get_mission))
        .route("/v1/launches/{id}", get(get_planning_session))
        .route("/v1/launches/{id}/submit", post(submit_planning_candidate))
        .route(
            "/v1/launches/{id}/variants/{variant}/submit",
            post(submit_named_planning_candidate),
        )
        .route(
            "/v1/launches/{id}/preview",
            post(preview_planning_candidate),
        )
        .route(
            "/v1/launches/{id}/variants/{variant}/preview",
            post(preview_named_planning_candidate),
        )
        .route(
            "/v1/launches/{id}/variants/{left}/compare/{right}",
            get(compare_planning_variants),
        )
        .route(
            "/v1/launches/{id}/variants/{variant}/propose",
            post(propose_planning_variant),
        )
        .route("/v1/launches/{id}/approve", post(approve_planning_session))
        .route(
            "/v1/launches/{id}/approve-and-launch",
            post(approve_and_start_launch),
        )
        .route("/v1/launches/{id}/start", post(start_approved_launch))
        .route("/v1/launches/{id}/decisions", post(request_launch_decision))
        .route(
            "/v1/launches/{id}/decisions/{decision}/answer",
            post(answer_launch_decision),
        )
        .route("/v1/launches", post(start_planning_session))
        .route("/v1/launches/{id}/revise", post(revise_planning_session))
        .route("/v1/launches/{id}/cancel", post(cancel_planning_session))
        .route("/v1/documents", get(list_documents).post(put_document))
        .route("/v1/documents/content", get(get_document))
        .route("/v1/claims", get(list_claims).post(post_claim))
        .route("/v1/claims/by-id/{id}", get(get_claim))
        .route("/v1/reviews", get(list_reviews))
        .route("/v1/reviews/{*subject}", post(post_review))
        .route("/v1/attention", get(list_attention).post(request_attention))
        .route("/v1/attention/resolve/{*subject}", post(resolve_attention))
        .route("/v1/messages", get(list_messages).post(send_message))
        .route("/v1/messages/{message_id}/claims", post(post_message_claim))
        .route("/v1/messages/read/{*subject}", get(read_message))
        .route("/v1/status", get(status))
        .route("/v1/events", get(events))
        .route("/v1/doctor", get(doctor))
        .route("/v1/repair", get(operational_repair_plan))
        .route("/v1/repair/apply", post(apply_operational_repair))
        .route("/v1/replication/status", get(replication_status))
        .route("/v1/replication/records", get(replication_records))
        .route("/v1/replication/records/{*record}", get(replication_record))
        .route("/v1/replication/repair", post(repair_replication_record))
        .route(
            "/v1/internal/replication/export",
            post(replication_export).layer(DefaultBodyLimit::max(crate::peer::MAX_EXCHANGE_BYTES)),
        )
        .route(
            "/v1/internal/replication/receive",
            post(replication_receive).layer(DefaultBodyLimit::max(crate::peer::MAX_EXCHANGE_BYTES)),
        )
        .route(
            "/v1/internal/replication/peer-failure",
            post(replication_peer_failure),
        )
        .route("/v1/internal/replication-wake", post(replication_wake))
        .route("/v1/evals", post(start_eval))
        .route("/v1/evals/{*run}", get(get_eval))
        .route("/v1/mission-runs", get(list_mission_runs))
        .route(
            "/v1/mission-runs/{run}/generations",
            get(list_run_generations),
        )
        .route(
            "/v1/mission-runs/{run}/revision-proposal",
            get(get_run_revision_proposal),
        )
        .route("/v1/mission-runs/{run}/revision", post(revise_mission_run))
        .route("/v1/mission-runs/{run}", get(get_mission_run))
        .route("/v1/run-generations/{generation}", get(get_run_generation))
        .route(
            "/v1/revision-proposals/{proposal}",
            get(get_revision_proposal),
        )
        .route(
            "/v1/revision-proposals/{proposal}/approve",
            post(approve_revision_proposal),
        )
        .route(
            "/v1/revision-proposals/{proposal}/cancel",
            post(cancel_revision_proposal),
        )
        .route("/v1/work", get(list_work))
        .route("/v1/work/mission/{*subject}", post(publish_work_mission))
        .route("/v1/work/wake/{*subject}", post(wake_work))
        .route("/v1/work/{action}/{*subject}", post(post_work_action))
        .route("/v1/gate-results", post(post_gate_result))
        .route("/v1/sessions", get(list_sessions))
        .route("/v1/sessions/{subject}/context/clear", post(clear_context))
        .route("/v1/sessions/{subject}/signal", post(signal_session))
        .route("/v1/sessions/input/{*subject}", post(input_session))
        .route("/v1/sessions/logs/{*subject}", get(logs_session))
        .route("/v1/sessions/screen/{*subject}", get(screen_session))
        .route("/v1/sessions/attach/{*subject}", post(attach_session))
        .route("/v1/sessions/{subject}/attach", post(attach_session))
        .route("/v1/sessions/terminal/{*subject}", get(terminal_session));
    #[cfg(test)]
    let app = app
        .route("/v1/resource-watches", post(watch_resource))
        .route(
            "/v1/resource-watches/{*subscription}",
            post(unwatch_resource),
        )
        .route("/v1/resources/refresh/{*resource}", post(refresh_resource))
        .route("/v1/runtimes/reset/{*subject}", post(reset_runtime))
        .route("/v1/claude", post(quick_claude))
        .route("/v1/codex", post(quick_codex))
        .route("/v1/mission-runs", post(start_mission_run));
    app.layer(from_fn_with_state(state.clone(), response_envelope))
        .with_state(state)
}

async fn schema() -> Json<Value> {
    let registry = st3_schema::registry();
    Json(json!({
        "schema": registry.name,
        "digest": registry.digest(),
        "subjects": registry.subjects,
        "resources": registry.resources,
        "claims": registry.claims,
    }))
}

async fn response_envelope(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let client_request = request.uri().path().starts_with("/v1/client/");
    let client_authentication = client_request
        .then(|| client_v0::authenticate(&state, &request))
        .transpose();
    if let Ok(Some(session)) = &client_authentication {
        request.extensions_mut().insert(session.clone());
    }
    let client_snapshot = client_request.then(|| {
        request
            .uri()
            .query()
            .and_then(|query| {
                query.split('&').find_map(|pair| {
                    let (name, value) = pair.split_once('=')?;
                    (name == "cursor").then(|| {
                        urlencoding::decode(value)
                            .map(|value| value.into_owned())
                            .unwrap_or_else(|_| value.to_owned())
                    })
                })
            })
            .and_then(|cursor| decode_client_cursor(&cursor).ok())
            .map(|cursor| cursor.snapshot)
            .unwrap_or_else(|| new_client_snapshot(&state))
    });
    if let Some(snapshot) = &client_snapshot {
        request.extensions_mut().insert(snapshot.clone());
    }
    let response = match client_authentication {
        Ok(_) => next.run(request).await,
        Err(error) => error.into_response(),
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS
        || !response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return response;
    }
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    let raw = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes).unwrap_or_else(|error| {
            json!({
                "code": "invalid-server-json",
                "message": error.to_string(),
            })
        }),
        Err(error) => json!({
            "code": "response-read-failed",
            "message": error.to_string(),
        }),
    };
    let store_index = state.store.index().unwrap_or_default();
    let request_id = if client_request {
        format!("request/{}", new_request_id())
    } else {
        new_request_id()
    };
    let envelope = if client_request && status.is_success() {
        json!({
            "api_version": CLIENT_API_VERSION,
            "request_id": request_id,
            "snapshot": client_snapshot.unwrap_or_else(|| new_client_snapshot(&state)),
            "value": raw,
        })
    } else if client_request {
        json!({
            "api_version": CLIENT_API_VERSION,
            "error_version": "st3.client.error.v0",
            "request_id": request_id,
            "code": client_error_code(raw.get("code").and_then(Value::as_str)),
            "message": raw.get("message").and_then(Value::as_str).unwrap_or("the request failed"),
            "retryable": matches!(status, StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE),
            "details": raw.get("details").cloned().unwrap_or_else(|| json!({})),
        })
    } else if status.is_success() {
        json!({
            "api_version": "st3.v1",
            "request_id": request_id,
            "snapshot_host": state.node,
            "store_index": store_index,
            "value": raw,
        })
    } else {
        json!({
            "api_version": "st3.v1",
            "request_id": request_id,
            "snapshot_host": state.node,
            "store_index": store_index,
            "code": raw.get("code").and_then(Value::as_str).unwrap_or("request-failed"),
            "message": raw.get("message").and_then(Value::as_str).unwrap_or("the request failed"),
            "details": raw.get("details").cloned().unwrap_or_else(|| json!({})),
        })
    };
    let body = serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec());
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(body))
}

fn new_request_id() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        let fallback = format!(
            "{}:{}:{:?}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            std::thread::current().id()
        );
        bytes.copy_from_slice(&Sha256::digest(fallback.as_bytes())[..16]);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes(bytes[0..4].try_into().expect("four bytes")),
        u16::from_be_bytes(bytes[4..6].try_into().expect("two bytes")),
        u16::from_be_bytes(bytes[6..8].try_into().expect("two bytes")),
        u16::from_be_bytes(bytes[8..10].try_into().expect("two bytes")),
        u64::from_be_bytes([
            0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ])
    )
}

fn client_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn client_timestamp(unix_ms: u128) -> String {
    let unix_ms = i64::try_from(unix_ms).unwrap_or(i64::MAX);
    chrono::DateTime::from_timestamp_millis(unix_ms)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn client_snapshot_time(snapshot: &ClientSnapshot) -> u128 {
    chrono::DateTime::parse_from_rfc3339(&snapshot.created_at)
        .map(|time| u128::try_from(time.timestamp_millis()).unwrap_or_default())
        .unwrap_or_default()
}

fn new_client_snapshot(state: &AppState) -> ClientSnapshot {
    let store_index = state.store.index().unwrap_or_default();
    let created_at = client_timestamp(client_now_ms());
    let fingerprint = hex::encode(Sha256::digest(
        format!(
            "{CLIENT_PROJECTION_VERSION}:{}:{store_index}:{created_at}",
            state.node
        )
        .as_bytes(),
    ));
    ClientSnapshot {
        id: format!(
            "snapshot/{}/{store_index}/{}",
            state.node.replace(char::is_whitespace, "-"),
            &fingerprint[..16]
        ),
        host_id: format!("host/{}", state.node.replace(char::is_whitespace, "-")),
        store_index,
        projection_version: CLIENT_PROJECTION_VERSION.into(),
        created_at,
    }
}

fn client_error_code(code: Option<&str>) -> String {
    match code.unwrap_or("internal") {
        "not-found"
        | "forbidden"
        | "unsupported-capability"
        | "validation-failed"
        | "idempotency-conflict"
        | "stale-fence"
        | "cursor-gap"
        | "page-cursor-expired"
        | "rate-limited"
        | "internal" => code.unwrap_or("internal").to_owned(),
        _ => "internal".into(),
    }
}

fn encode_client_cursor(cursor: &ClientPageCursor) -> Result<String, ApiError> {
    let encoded = serde_json::to_vec(cursor).map_err(ApiError::internal)?;
    Ok(format!(
        "page/{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded)
    ))
}

fn decode_client_cursor(cursor: &str) -> Result<ClientPageCursor, ApiError> {
    let encoded = cursor.strip_prefix("page/").ok_or_else(|| ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code: "validation-failed".into(),
        message: "the page cursor is malformed".into(),
        details: serde_json::Map::new(),
    })?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "validation-failed".into(),
            message: "the page cursor is malformed".into(),
            details: serde_json::Map::new(),
        })?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code: "validation-failed".into(),
        message: "the page cursor is malformed".into(),
        details: serde_json::Map::new(),
    })
}

fn client_page_expired(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::GONE,
        code: "page-cursor-expired".into(),
        message: message.into(),
        details: serde_json::Map::new(),
    }
}

fn client_page(
    state: &AppState,
    snapshot: &ClientSnapshot,
    collection: &str,
    items: Vec<Value>,
    query: &ClientListQuery,
) -> Result<ClientResourcePage, ApiError> {
    let requested_limit = query
        .limit
        .unwrap_or(CLIENT_DEFAULT_PAGE_ITEMS)
        .clamp(1, CLIENT_MAX_PAGE_ITEMS);
    let (offset, limit, expires_at_unix_ms) = if let Some(cursor) = &query.cursor {
        let cursor = decode_client_cursor(cursor)?;
        if cursor.collection != collection
            || cursor.history != query.history
            || cursor.person != query.person
            || cursor.actor != query.actor
            || query
                .limit
                .is_some_and(|limit| limit.clamp(1, CLIENT_MAX_PAGE_ITEMS) != cursor.limit)
            || cursor.snapshot.id != snapshot.id
            || cursor.snapshot.store_index != snapshot.store_index
        {
            return Err(client_page_expired(
                "the page cursor does not match this collection, snapshot, or filter",
            ));
        }
        if client_now_ms() > cursor.expires_at_unix_ms {
            return Err(client_page_expired("the page cursor expired"));
        }
        (cursor.offset, cursor.limit, cursor.expires_at_unix_ms)
    } else {
        (0, requested_limit, client_now_ms() + CLIENT_PAGE_TTL_MS)
    };
    if state.store.index().map_err(ApiError::internal)? != snapshot.store_index {
        return Err(client_page_expired(
            "the snapshot changed; restart pagination from the first page",
        ));
    }
    let end = offset.saturating_add(limit).min(items.len());
    let page_items = items.get(offset..end).unwrap_or_default().to_vec();
    let has_more = end < items.len();
    let next_cursor = if has_more {
        Some(encode_client_cursor(&ClientPageCursor {
            snapshot: snapshot.clone(),
            collection: collection.into(),
            offset: end,
            limit,
            history: query.history,
            person: query.person.clone(),
            actor: query.actor.clone(),
            expires_at_unix_ms,
        })?)
    } else {
        None
    };
    Ok(ClientResourcePage {
        kind: "page".into(),
        collection: collection.into(),
        items: page_items,
        page: ClientPageInfo {
            limit,
            has_more,
            next_cursor,
            cursor_expires_at: Some(client_timestamp(expires_at_unix_ms)),
        },
    })
}

fn client_detail_id(kind: &str, id: &str) -> String {
    if id.starts_with(&format!("{kind}/")) {
        id.to_owned()
    } else {
        format!("{kind}/{id}")
    }
}

fn client_detail(items: Vec<Value>, kind: &str, id: &str) -> Result<Json<Value>, ApiError> {
    let id = client_detail_id(kind, id);
    items
        .into_iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(id.as_str()))
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("{kind} `{id}` does not exist")))
}

async fn client_capabilities(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<client_v0::ClientSession>,
) -> Json<Value> {
    let cursor = format!("event-cursor/{}/{}", state.node, snapshot.store_index);
    let capabilities = client_v0::capabilities(&session);
    Json(json!({
        "kind": "capabilities",
        "session_actor": session.actor,
        "transport": session.transport,
        "capabilities": capabilities,
        "limits": {
            "max_page_items": CLIENT_MAX_PAGE_ITEMS,
            "max_event_items": 500,
            "max_response_bytes": 1_048_576,
            "max_wait_ms": 30_000
        },
        "event_cursor": cursor,
        "oldest_event_cursor": format!("event-cursor/{}/0", state.node),
        "schemas": [
            "../client-v0/schemas/client-v0.schema.json",
            "../client-v0/schemas/operations.json"
        ]
    }))
}

fn client_work_resources(
    store: &Store,
    actor: Option<&str>,
    history: bool,
    snapshot_unix_ms: u128,
) -> anyhow::Result<Vec<Value>> {
    let mut work = if history {
        store.work_history_at_snapshot(actor, snapshot_unix_ms)?
    } else {
        store.work_at_snapshot(actor, false, snapshot_unix_ms)?
    };
    work.sort_by(|left, right| {
        let priority = |status: &str| match status {
            "ready" => 0,
            "claimed" | "working" => 1,
            "verifying" => 2,
            "blocked" => 3,
            "pending" | "waiting" => 4,
            _ => 5,
        };
        priority(&left.status)
            .cmp(&priority(&right.status))
            .then_with(|| left.readiness_epoch.cmp(&right.readiness_epoch))
            .then_with(|| left.step.cmp(&right.step))
            .then_with(|| left.subject.cmp(&right.subject))
    });
    work.into_iter()
        .map(|work| {
            let operational = store.work_annotation(&work)?;
            let state = match work.status.as_str() {
                "pending" => "waiting",
                "working" => "claimed",
                other => other,
            };
            Ok(json!({
                "id": work.subject,
                "kind": "work",
                "revision": work.definition_hash,
                "updated_at": client_timestamp(work.updated_at_unix_ms),
                "mission_run_id": work.run,
                "generation_id": work.generation,
                "definition_id": work.definition_hash,
                "path": work.step,
                "state": state,
                "attempt": work.attempt,
                "readiness_epoch": work.readiness_epoch,
                "claimant": work.claimant,
                "claim_incarnation": work.claim_incarnation,
                "claim_expires_at_unix_ms": work.claim_expires_at_unix_ms,
                "execution_started_at_unix_ms": work.execution_started_at_unix_ms,
                "execution_elapsed_ms": work.execution_elapsed_ms,
                "timeout_ms": work.timeout_ms,
                "goals": work.goals,
                "constraints": work.constraints,
                "operational": operational
            }))
        })
        .collect()
}

fn client_agent_resources(store: &Store, history: bool, at: &str) -> anyhow::Result<Vec<Value>> {
    let status = if history {
        store.status_history(None, None, None)?
    } else {
        store.status(None)?
    };
    let mut agents = status
        .subjects
        .into_iter()
        .filter(|subject| {
            subject.subject.starts_with("agent/") || subject.kind.as_deref() == Some("agent")
        })
        .map(|subject| {
            let fields = subject
                .actual
                .as_ref()
                .map(|actual| actual.get("fields").unwrap_or(actual));
            let observed = fields
                .and_then(|fields| fields.get("status"))
                .and_then(Value::as_str);
            let state = match observed {
                Some("running" | "ready" | "working" | "idle") => "running",
                Some("starting" | "pending") => "starting",
                Some("waiting") => "waiting",
                Some("failed") => "failed",
                Some("stopped" | "exited" | "absent") => "stopped",
                _ if subject.desired.is_some() => "desired",
                _ => "stopped",
            };
            let runtime_ids = fields
                .and_then(|fields| fields.get("runtime_id"))
                .and_then(Value::as_str)
                .map(|runtime| vec![format!("runtime/{runtime}")])
                .unwrap_or_default();
            let name = subject
                .desired
                .as_ref()
                .and_then(|desired| desired.get("display_name"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    subject
                        .subject
                        .strip_prefix("agent/")
                        .unwrap_or(&subject.subject)
                })
                .to_owned();
            let revision = subject
                .desired_revision
                .clone()
                .or_else(|| subject.claims.last().cloned())
                .unwrap_or_else(|| format!("agent/{}", subject.subject));
            let value = json!({
                "id": subject.subject,
                "kind": "agent",
                "revision": revision,
                "updated_at": at,
                "name": name,
                "state": state,
                "reachability": subject.reachability,
                "runtime_ids": runtime_ids,
                "operational": subject.projection
            });
            (name, value)
        })
        .collect::<Vec<_>>();
    agents.sort_by(|(left_name, left), (right_name, right)| {
        left_name
            .cmp(right_name)
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(agents.into_iter().map(|(_, value)| value).collect())
}

fn client_session_resources(store: &Store, history: bool, at: &str) -> anyhow::Result<Vec<Value>> {
    let status = if history {
        store.status_history(None, None, None)?
    } else {
        store.status(None)?
    };
    let mut sessions = Vec::new();
    for subject in status.subjects.into_iter().filter(|subject| {
        subject.subject.starts_with("agent/") || subject.kind.as_deref() == Some("agent")
    }) {
        let fields = subject
            .actual
            .as_ref()
            .map(|actual| actual.get("fields").unwrap_or(actual));
        let incarnation = fields
            .and_then(|fields| fields.get("incarnation_id"))
            .and_then(Value::as_str)
            .or(subject.projection.runtime_incarnation.as_deref());
        let runtime = fields
            .and_then(|fields| fields.get("runtime_id"))
            .and_then(Value::as_str);
        let Some(identity) = incarnation.or(runtime) else {
            continue;
        };
        let digest = hex::encode(Sha256::digest(
            format!("{}:{identity}", subject.subject).as_bytes(),
        ));
        let observed = fields
            .and_then(|fields| fields.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("waiting");
        let state = match observed {
            "running" | "ready" | "working" | "idle" => "running",
            "starting" | "pending" | "waiting" => "waiting",
            "failed" => "failed",
            "cancelled" => "cancelled",
            "stopped" | "exited" | "absent" => "completed",
            _ => "waiting",
        };
        let started = fields
            .and_then(|fields| fields.get("started_at_unix_ms"))
            .and_then(|value| value.as_u64().map(u128::from))
            .map(client_timestamp)
            .unwrap_or_else(|| at.to_owned());
        sessions.push(json!({
            "id": format!("session/{}", &digest[..24]),
            "kind": "session",
            "revision": identity,
            "updated_at": at,
            "owner_id": subject.subject,
            "state": state,
            "started_at": started,
            "ended_at": if state == "completed" || state == "failed" || state == "cancelled" { Some(at) } else { None },
            "timeline_cursor": format!("timeline-cursor/{}/0", &digest[..24]),
            "runtime_incarnation": incarnation,
            "operational": subject.projection
        }));
    }
    sessions.sort_by(|left, right| {
        right["updated_at"]
            .as_str()
            .cmp(&left["updated_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(sessions)
}

fn attention_resource_id(subject: &str) -> String {
    if subject.starts_with("attention/") {
        subject.to_owned()
    } else {
        let digest = hex::encode(Sha256::digest(subject.as_bytes()));
        format!("attention/{}", &digest[..24])
    }
}

fn client_attention_resources(
    store: &Store,
    person: Option<&str>,
    history: bool,
) -> anyhow::Result<Vec<Value>> {
    let current = store.attention_items(person)?;
    let current_subjects = current
        .iter()
        .map(|item| item.subject.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut resources = BTreeMap::new();
    for item in &current {
        let id = attention_resource_id(&item.subject);
        resources.insert(
            id.clone(),
            json!({
                "id": id,
                "kind": "attention",
                "revision": item.subject,
                "updated_at": client_timestamp(item.requested_at_unix_ms),
                "title": item.title,
                "detail": item.detail,
                "priority": if item.kind == "fault" { "high" } else { "normal" },
                "state": "open",
                "requested_at": client_timestamp(item.requested_at_unix_ms),
                "targets": item.targets,
                "actions": if item.kind == "fault" { vec!["attention.resolve"] } else { Vec::<&str>::new() },
                "operational": { "layer": "current", "actionable": true, "reasons": [] }
            }),
        );
    }
    if history {
        for request in store.attention_requests(person, true)? {
            let current = current_subjects.contains(request.subject.as_str());
            let mut reasons = Vec::new();
            if request.status != "pending" {
                reasons.push("resolved");
            } else if !current {
                reasons.push("superseded");
            }
            let id = attention_resource_id(&request.subject);
            resources.insert(
                id.clone(),
                json!({
                    "id": id,
                    "kind": "attention",
                    "revision": request.request,
                    "updated_at": client_timestamp(request.resolved_at_unix_ms.unwrap_or(request.requested_at_unix_ms)),
                    "title": request.title,
                    "detail": request.reason,
                    "priority": match request.severity.as_str() { "critical" => "critical", "error" => "high", "warning" => "normal", _ => "low" },
                    "state": if request.status == "pending" { "open" } else { "resolved" },
                    "requested_at": client_timestamp(request.requested_at_unix_ms),
                    "targets": request.targets,
                    "actions": if current { vec!["attention.resolve"] } else { Vec::<&str>::new() },
                    "operational": { "layer": if current { "current" } else { "history" }, "actionable": current, "reasons": reasons }
                }),
            );
        }
    }
    let mut resources = resources.into_values().collect::<Vec<_>>();
    let priority = |value: &Value| match value["priority"].as_str() {
        Some("critical") => 0,
        Some("high") => 1,
        Some("normal") => 2,
        _ => 3,
    };
    resources.sort_by(|left, right| {
        priority(left)
            .cmp(&priority(right))
            .then_with(|| {
                left["requested_at"]
                    .as_str()
                    .cmp(&right["requested_at"].as_str())
            })
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(resources)
}

fn client_message_resources(
    store: &Store,
    person: Option<&str>,
    history: bool,
) -> anyhow::Result<Vec<Value>> {
    let current_ids = store
        .operational_messages(person, false)?
        .into_iter()
        .map(|message| message.subject)
        .collect::<std::collections::BTreeSet<_>>();
    let messages = store.operational_messages(person, history)?;
    let mut resources = Vec::new();
    for message in messages {
        let claims = store.claims_for(&message.subject, None)?;
        let first = claims.first();
        let last = claims.last();
        let sent_at = first
            .map(|claim| claim.accepted_at_unix_ms)
            .unwrap_or_default();
        let updated_at = last
            .map(|claim| claim.accepted_at_unix_ms)
            .unwrap_or(sent_at);
        let mut reasons = Vec::new();
        if message.status == "closed" {
            reasons.push("closed");
        } else if !current_ids.contains(&message.subject) {
            reasons.push("superseded");
        }
        let current = reasons.is_empty();
        resources.push(json!({
            "id": message.subject,
            "kind": "message",
            "revision": last.map(|claim| claim.id.as_str()).unwrap_or("message/unknown"),
            "updated_at": client_timestamp(updated_at),
            "from": message.from,
            "to": message.to,
            "title": message.title,
            "content": message.content,
            "state": message.status,
            "sent_at": client_timestamp(sent_at),
            "in_reply_to": message.in_reply_to,
            "tags": message.tags,
            "operational": { "layer": if current { "current" } else { "history" }, "actionable": current, "reasons": reasons }
        }));
    }
    resources.sort_by(|left, right| {
        right["sent_at"]
            .as_str()
            .cmp(&left["sent_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(resources)
}

fn launch_session_id(id: &str) -> &str {
    id.strip_prefix("launch/")
        .or_else(|| id.strip_prefix("planning-session/"))
        .unwrap_or(id)
}

fn client_launch_decision_resources(
    store: &Store,
    session: &PlanningSessionView,
) -> anyhow::Result<Vec<Value>> {
    let claims = store.claims_for(&session.subject, None)?;
    let answers = claims
        .iter()
        .filter(|claim| claim.kind == "planning-session.question-answered")
        .filter_map(|claim| {
            let id = claim.body.pointer("/fields/decision_id")?.as_str()?;
            Some((id.to_owned(), claim))
        })
        .collect::<BTreeMap<_, _>>();
    let mut decisions = claims
        .iter()
        .filter(|claim| claim.kind == "planning-session.question-requested")
        .filter_map(|claim| {
            let fields = claim.body.get("fields")?;
            let id = fields.get("decision_id")?.as_str()?;
            let answer = answers.get(id);
            Some(json!({
                "id": id,
                "kind": "launch-decision",
                "revision": answer.map(|answer| answer.id.as_str()).unwrap_or(claim.id.as_str()),
                "updated_at": client_timestamp(answer.map(|answer| answer.accepted_at_unix_ms).unwrap_or(claim.accepted_at_unix_ms)),
                "launch_id": format!("launch/{}", session.id),
                "question": fields.get("question").cloned().unwrap_or(Value::Null),
                "state": if answer.is_some() { "answered" } else { "open" },
                "requested_at": client_timestamp(claim.accepted_at_unix_ms),
                "choices": fields.get("choices").cloned().unwrap_or_else(|| json!([])),
                "answer": answer.and_then(|answer| answer.body.pointer("/fields/answer")).cloned(),
                "operational": { "layer": if answer.is_some() { "history" } else { "current" }, "actionable": answer.is_none(), "reasons": if answer.is_some() { vec!["answered"] } else { Vec::<&str>::new() } }
            }))
        })
        .collect::<Vec<_>>();
    decisions.sort_by(|left, right| {
        left["requested_at"]
            .as_str()
            .cmp(&right["requested_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(decisions)
}

fn launch_diagnostics(blockers: &[String], warnings: &[String]) -> Vec<Value> {
    blockers
        .iter()
        .map(|message| json!({"code": "preview-blocker", "severity": "error", "message": message, "path": null}))
        .chain(warnings.iter().map(|message| {
            json!({"code": "preview-warning", "severity": "warning", "message": message, "path": null})
        }))
        .collect()
}

fn client_launch_diagnostics(preview: &crate::model::PlanningPreviewView) -> Vec<Value> {
    launch_diagnostics(&preview.mission.blockers, &preview.mission.warnings)
}

fn client_safe_json(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("declarations_kdl");
            object.remove("kdl");
            object.remove("markdown");
            for value in object.values_mut() {
                client_safe_json(value);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(client_safe_json),
        _ => {}
    }
}

fn canonical_json(value: &Value, output: &mut String) -> anyhow::Result<()> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => output.push_str(&serde_json::to_string(value)?),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                canonical_json(value, output)?;
            }
            output.push(']');
        }
        Value::Object(object) => {
            output.push('{');
            let mut fields = object.iter().collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (name, value)) in fields.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(name)?);
                output.push(':');
                canonical_json(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn launch_preview_token(
    session: &PlanningSessionView,
    variant: &crate::model::PlanningVariantView,
    normalized_mission: &Value,
    diagnostics: &[Value],
) -> anyhow::Result<String> {
    launch_preview_token_values(
        &session.id,
        &variant.name,
        variant.candidate.revision,
        session.source_generation.as_deref(),
        normalized_mission,
        diagnostics,
    )
}

fn launch_preview_token_values(
    launch_id: &str,
    variant: &str,
    candidate_revision: u32,
    target_generation: Option<&str>,
    normalized_mission: &Value,
    diagnostics: &[Value],
) -> anyhow::Result<String> {
    let input = json!({
        "api_version": CLIENT_API_VERSION,
        "launch_id": format!("launch/{launch_id}"),
        "variant_id": format!("launch-variant/{launch_id}/{variant}"),
        "candidate_revision": candidate_revision,
        "target_generation": target_generation,
        "normalized_mission": normalized_mission,
        "diagnostics": diagnostics,
    });
    let mut canonical = String::new();
    canonical_json(&input, &mut canonical)?;
    Ok(format!(
        "lpv0:{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    ))
}

fn launch_visualization(
    mission: &crate::model::MissionSpec,
    session: &PlanningSessionView,
    preview: &crate::model::PlanningPreviewView,
) -> Value {
    let nodes = mission.display_order.iter().filter_map(|id| mission.steps.get(id)).map(|step| {
        json!({
            "id": format!("step/{}", step.path),
            "kind": "step",
            "label": step.title.as_deref().unwrap_or(&step.id),
            "path": step.path,
            "goals": step.goals,
            "constraints": step.constraints,
            "assignment": step.work_selector,
            "timeout_ms": step.timeout_ms,
            "retry": step.retry,
            "gates": step.gates,
            "loop": step.loop_spec,
            "resources": step.documents,
            "source_references": step.documents,
            "runtime": { "attempt": null, "lease": null, "progress": null, "blockers": [], "attention": [], "errors": [], "cursor": null }
        })
    }).collect::<Vec<_>>();
    let edges = mission
        .steps
        .values()
        .flat_map(|step| {
            step.dependencies
                .iter()
                .map(move |dependency| match dependency {
                    crate::model::DependencySpec::Step {
                        step: dependency, ..
                    } => json!({
                        "id": format!("edge/{}/{}", dependency, step.path),
                        "kind": "dependency",
                        "from": format!("step/{dependency}"),
                        "to": format!("step/{}", step.path),
                        "gate": dependency,
                    }),
                    crate::model::DependencySpec::Predicate { .. } => json!({
                        "id": format!("edge/predicate/{}", step.path),
                        "kind": "gate",
                        "from": null,
                        "to": format!("step/{}", step.path),
                        "gate": dependency,
                    }),
                })
        })
        .collect::<Vec<_>>();
    let timeline = mission
        .display_order
        .iter()
        .enumerate()
        .filter_map(|(ordinal, id)| mission.steps.get(id).map(|step| (ordinal, step)))
        .map(|(ordinal, step)| {
            json!({
                "id": format!("timeline/{}", step.path),
                "node": format!("step/{}", step.path),
                "ordinal": ordinal,
                "dependencies": step.dependencies,
                "timeout_ms": step.timeout_ms,
            })
        })
        .collect::<Vec<_>>();
    let mut lanes = BTreeMap::<String, Vec<String>>::new();
    for step in mission.steps.values() {
        let assignment = step
            .work_selector
            .as_ref()
            .map(|selector| serde_json::to_string(selector).unwrap_or_else(|_| "assigned".into()))
            .unwrap_or_else(|| "unassigned".into());
        lanes
            .entry(assignment)
            .or_default()
            .push(format!("step/{}", step.path));
    }
    json!({
        "version": "st3.visualization.v0",
        "views": ["graph", "timeline", "swimlane", "revision", "risk", "live-progress"],
        "mission": format!("mission/{}", mission.id),
        "nodes": nodes,
        "edges": edges,
        "groups": mission.steps.values().filter(|step| step.queue.is_some() || step.nested_mission.is_some()).map(|step| json!({
            "id": step.queue.clone().unwrap_or_else(|| step.path.clone()),
            "kind": if step.queue.is_some() { "queue" } else { "nested-mission" },
            "members": [format!("step/{}", step.path)]
        })).collect::<Vec<_>>(),
        "timeline": { "entries": timeline },
        "swimlanes": lanes.into_iter().enumerate().map(|(ordinal, (assignment, nodes))| json!({
            "id": format!("lane/{ordinal}"), "assignment": assignment, "nodes": nodes
        })).collect::<Vec<_>>(),
        "goals": mission.goals,
        "constraints": mission.constraints,
        "gates": mission.gates,
        "resources": mission.products,
        "revision": { "candidate": mission.revision, "target_generation": session.source_generation },
        "risk": { "blockers": preview.mission.blockers, "warnings": preview.mission.warnings, "gates": mission.gates },
        "live_progress": { "state": session.status, "cursor": null, "updated_at": client_timestamp(session.updated_at_unix_ms) }
    })
}

fn client_launch_variant_resources(
    state: &AppState,
    session: &PlanningSessionView,
) -> anyhow::Result<Vec<Value>> {
    let mut resources = Vec::new();
    for (ordinal, variant) in session.variants.iter().enumerate() {
        let (normalized, diagnostics, visualization, structured_diff) = if let Some(preview) =
            &variant.preview
        {
            let intent = parse_intent(&preview.mission.resolved_intent.kdl, &state.node)
                .map_err(|error| anyhow::anyhow!(error.message))?;
            let mission = intent
                .missions
                .get(&session.mission)
                .ok_or_else(|| anyhow::anyhow!("preview mission is missing"))?;
            let mut normalized = serde_json::to_value(mission)?;
            client_safe_json(&mut normalized);
            (
                normalized,
                client_launch_diagnostics(preview),
                launch_visualization(mission, session, preview),
                json!({"changes": preview.mission.changes, "predicted_actions": preview.mission.predicted_actions}),
            )
        } else {
            (
                json!({}),
                Vec::new(),
                json!({"version": "st3.visualization.v0", "views": [], "nodes": [], "edges": [], "groups": [], "timeline": {"entries": []}, "swimlanes": []}),
                json!({"changes": [], "predicted_actions": []}),
            )
        };
        let preview_token = variant
            .preview
            .as_ref()
            .map(|_| launch_preview_token(session, variant, &normalized, &diagnostics))
            .transpose()?;
        let blocked = diagnostics
            .iter()
            .any(|diagnostic| diagnostic["severity"] == "error");
        let approved = session.status == "approved"
            && session.candidate.as_ref().is_some_and(|candidate| {
                candidate.variant == variant.name
                    && candidate.revision == variant.candidate.revision
            });
        resources.push(json!({
            "id": format!("launch-variant/{}/{}", session.id, variant.name),
            "kind": "launch-variant",
            "revision": format!("launch-variant-revision/{}/{}/{}", session.id, variant.name, variant.candidate.revision),
            "updated_at": client_timestamp(variant.preview.as_ref().map(|preview| preview.created_at_unix_ms).unwrap_or(variant.candidate.submitted_at_unix_ms)),
            "launch_id": format!("launch/{}", session.id),
            "ordinal": ordinal,
            "candidate_revision": variant.candidate.revision,
            "status": if approved { "approved" } else if blocked { "blocked" } else if variant.preview.is_some() { "previewable" } else { "draft" },
            "target_generation": session.source_generation,
            "normalized_mission": normalized,
            "diagnostics": diagnostics,
            "preview_token": preview_token,
            "structured_diff": structured_diff,
            "visualization": visualization,
            "operational": { "layer": if approved { "history" } else { "current" }, "actionable": !approved && !blocked, "reasons": if blocked { vec!["blocked"] } else if approved { vec!["approved"] } else { Vec::<&str>::new() } }
        }));
    }
    Ok(resources)
}

fn client_launch_approval_resources(
    state: &AppState,
    session: &PlanningSessionView,
) -> anyhow::Result<Vec<Value>> {
    let variants = client_launch_variant_resources(state, session)?;
    let tokens = variants
        .into_iter()
        .filter_map(|variant| {
            Some((
                variant["candidate_revision"].as_u64()?,
                (
                    variant["preview_token"].as_str()?.to_owned(),
                    variant["id"].as_str()?.to_owned(),
                ),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let claims = state.store.claims_for(&session.subject, None)?;
    let mut approvals = claims
        .into_iter()
        .filter(|claim| claim.kind == "planning-session.approved")
        .filter_map(|claim| {
            let revision = claim.body.pointer("/fields/candidate_revision")?.as_u64()?;
            let (token, variant_id) = tokens.get(&revision)?;
            let reviewer = claim
                .body
                .pointer("/fields/requester")
                .and_then(Value::as_str)
                .or(claim.actor.as_deref())?;
            Some(json!({
                "id": format!("launch-approval/{}/{}", session.id, revision),
                "kind": "launch-approval",
                "revision": claim.id,
                "updated_at": client_timestamp(claim.accepted_at_unix_ms),
                "launch_id": format!("launch/{}", session.id),
                "variant_id": variant_id,
                "reviewer": reviewer,
                "state": "approved",
                "preview_token": token,
                "decided_at": client_timestamp(claim.accepted_at_unix_ms),
                "operational": { "layer": "history", "actionable": false, "reasons": ["approved"] }
            }))
        })
        .collect::<Vec<_>>();
    approvals.sort_by(|left, right| {
        left["decided_at"]
            .as_str()
            .cmp(&right["decided_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(approvals)
}

fn client_launch_resources(store: &Store, history: bool) -> anyhow::Result<Vec<Value>> {
    let sessions = store.planning_sessions(history)?;
    let mut resources = sessions
        .into_iter()
        .map(|session| {
            let phase = match session.status.as_str() {
                "planning" | "revision-requested" => "authoring",
                "review" => "review",
                "approved" => "approved",
                "cancelled" => "cancelled",
                _ => "failed",
            };
            let historical = matches!(phase, "approved" | "cancelled" | "failed");
            let target = match (&session.target_mission_run, &session.source_generation) {
                (Some(run), Some(generation)) => json!({
                    "type": "mission-run",
                    "mission_run_id": run,
                    "generation_id": generation
                }),
                _ => json!({ "type": "new-mission" }),
            };
            let decisions = client_launch_decision_resources(store, &session)?;
            let approval_ids = store.claims_for(&session.subject, None)?.into_iter().filter(|claim| claim.kind == "planning-session.approved").filter_map(|claim| claim.body.pointer("/fields/candidate_revision").and_then(Value::as_u64).map(|revision| format!("launch-approval/{}/{revision}", session.id))).collect::<Vec<_>>();
            Ok(json!({
                "id": format!("launch/{}", session.id),
                "kind": "launch",
                "revision": format!("launch/{}", session.updated_at_unix_ms),
                "updated_at": client_timestamp(session.updated_at_unix_ms),
                "title": session.mission,
                "phase": phase,
                "request": session.request,
                "target": target,
                "variants": session.variants.iter().map(|variant| format!("launch-variant/{}/{}", session.id, variant.name)).collect::<Vec<_>>(),
                "decisions": decisions.iter().filter_map(|decision| decision["id"].as_str()).collect::<Vec<_>>(),
                "approvals": approval_ids,
                "operational": { "layer": if historical { "history" } else { "current" }, "actionable": !historical, "reasons": if historical { vec![phase] } else { Vec::<&str>::new() } }
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    resources.sort_by(|left, right| {
        right["updated_at"]
            .as_str()
            .cmp(&left["updated_at"].as_str())
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    Ok(resources)
}

fn client_history_resources(store: &Store) -> anyhow::Result<Vec<Value>> {
    let index = store.index()?;
    let page = store.claims_page(None, None, 0, None, true, index as usize + 1)?;
    Ok(page
        .claims
        .into_iter()
        .map(|claim| {
            let mut targets = vec![claim.subject.clone()];
            if let Some(actor) = &claim.actor
                && actor.contains('/')
                && !actor.chars().any(char::is_whitespace)
            {
                targets.push(actor.clone());
            }
            json!({
                "id": format!("history/{}", claim.store_index),
                "kind": "history",
                "revision": claim.id,
                "updated_at": client_timestamp(claim.accepted_at_unix_ms),
                "event_type": claim.kind,
                "occurred_at": client_timestamp(claim.accepted_at_unix_ms),
                "store_index": claim.store_index,
                "summary": format!("{} on {}", claim.kind, claim.subject),
                "targets": targets,
                "operational": { "layer": "history", "actionable": false, "reasons": ["audit"] }
            })
        })
        .collect())
}

async fn client_work(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let store = state.store.clone();
    let actor = query.actor.clone();
    let history = query.history;
    let snapshot_unix_ms = client_snapshot_time(&snapshot);
    let items = blocking_store(move || {
        client_work_resources(&store, actor.as_deref(), history, snapshot_unix_ms)
    })
    .await?;
    client_page(&state, &snapshot, "work", items, &query).map(Json)
}

async fn client_work_detail(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_work_resources(
            &state.store,
            query.actor.as_deref(),
            query.history,
            client_snapshot_time(&snapshot),
        )
        .map_err(ApiError::internal)?,
        "work",
        &id,
    )
}

async fn client_agents(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_agent_resources(&state.store, query.history, &snapshot.created_at)
        .map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "agents", items, &query).map(Json)
}

async fn client_agents_detail(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_agent_resources(&state.store, query.history, &snapshot.created_at)
            .map_err(ApiError::internal)?,
        "agent",
        &id,
    )
}

async fn client_sessions(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_session_resources(&state.store, query.history, &snapshot.created_at)
        .map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "sessions", items, &query).map(Json)
}

async fn client_sessions_detail(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Extension(session): Extension<client_v0::ClientSession>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    if let Some(id) = id.strip_suffix("/timeline") {
        return client_v0::timeline_value(&state, &snapshot, &session, id, query.limit);
    }
    client_detail(
        client_session_resources(&state.store, query.history, &snapshot.created_at)
            .map_err(ApiError::internal)?,
        "session",
        &id,
    )
}

async fn client_attention(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_attention_resources(&state.store, query.person.as_deref(), query.history)
        .map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "attention", items, &query).map(Json)
}

async fn client_attention_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_attention_resources(&state.store, query.person.as_deref(), query.history)
            .map_err(ApiError::internal)?,
        "attention",
        &id,
    )
}

async fn client_messages(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_message_resources(&state.store, query.person.as_deref(), query.history)
        .map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "messages", items, &query).map(Json)
}

async fn client_messages_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_message_resources(&state.store, query.person.as_deref(), query.history)
            .map_err(ApiError::internal)?,
        "message",
        &id,
    )
}

async fn client_launches(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_launch_resources(&state.store, query.history).map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "launches", items, &query).map(Json)
}

async fn client_launches_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_launch_resources(&state.store, query.history).map_err(ApiError::internal)?,
        "launch",
        &id,
    )
}

fn client_launch_session(state: &AppState, id: &str) -> Result<PlanningSessionView, ApiError> {
    state
        .store
        .planning_session(launch_session_id(id))
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("launch `{id}` does not exist")))
}

async fn client_launch_variants(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    let items = client_launch_variant_resources(&state, &session).map_err(ApiError::internal)?;
    client_page(
        &state,
        &snapshot,
        &format!("launches/{id}/variants"),
        items,
        &query,
    )
    .map(Json)
}

async fn client_launch_variant_detail(
    State(state): State<AppState>,
    AxumPath((id, variant)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    client_detail(
        client_launch_variant_resources(&state, &session).map_err(ApiError::internal)?,
        "launch-variant",
        &format!("{}/{variant}", session.id),
    )
}

async fn client_launch_decisions(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    let items =
        client_launch_decision_resources(&state.store, &session).map_err(ApiError::internal)?;
    client_page(
        &state,
        &snapshot,
        &format!("launches/{id}/decisions"),
        items,
        &query,
    )
    .map(Json)
}

async fn client_launch_decision_detail(
    State(state): State<AppState>,
    AxumPath((id, decision)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    client_detail(
        client_launch_decision_resources(&state.store, &session).map_err(ApiError::internal)?,
        "launch-decision",
        &decision,
    )
}

async fn client_launch_approvals(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    let items = client_launch_approval_resources(&state, &session).map_err(ApiError::internal)?;
    client_page(
        &state,
        &snapshot,
        &format!("launches/{id}/approvals"),
        items,
        &query,
    )
    .map(Json)
}

async fn client_launch_approval_detail(
    State(state): State<AppState>,
    AxumPath((id, approval)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let session = client_launch_session(&state, &id)?;
    client_detail(
        client_launch_approval_resources(&state, &session).map_err(ApiError::internal)?,
        "launch-approval",
        &format!("{}/{approval}", session.id),
    )
}

async fn client_history(
    State(state): State<AppState>,
    Extension(snapshot): Extension<ClientSnapshot>,
    Query(query): Query<ClientListQuery>,
) -> Result<Json<ClientResourcePage>, ApiError> {
    let items = client_history_resources(&state.store).map_err(ApiError::internal)?;
    client_page(&state, &snapshot, "history", items, &query).map(Json)
}

async fn client_history_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    client_detail(
        client_history_resources(&state.store).map_err(ApiError::internal)?,
        "history",
        &id,
    )
}

async fn blocking_store<T, F>(operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::internal)
}

async fn blocking_action<T, F>(operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, St3Error> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::bad)
}

pub async fn serve_unix(socket: &Path, app: Router) -> anyhow::Result<()> {
    if let Some(parent) = socket.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(socket)?;
    fs::set_permissions(socket, fs::Permissions::from_mode(0o600))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let app = app.clone();
                async move { app.oneshot(request.map(Body::new)).await }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
    }
}

pub async fn serve_tcp(address: &str, app: Router) -> anyhow::Result<()> {
    let address = address.parse::<std::net::SocketAddr>()?;
    anyhow::ensure!(
        address.ip().is_loopback(),
        "the peer listener must bind to a loopback address"
    );
    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({
        "status": "ready",
        "node": state.node,
        "version": env!("CARGO_PKG_VERSION"),
        "isolation": isolation_name(st_runtime::isolation_mode()),
        "store_index": state.store.index().map_err(ApiError::internal)?,
        "security": "trusted-network-no-tls-no-acls",
    })))
}

fn isolation_name(mode: st_runtime::Isolation) -> &'static str {
    match mode {
        st_runtime::Isolation::Scope => "scope",
        st_runtime::Isolation::Detached => "detached",
        st_runtime::Isolation::DegradedDetached => "degraded-detached",
    }
}

async fn doctor(State(state): State<AppState>) -> Result<Json<DoctorReport>, ApiError> {
    tokio::task::spawn_blocking(move || doctor_report(&state))
        .await
        .map_err(ApiError::internal)?
}

fn doctor_report(state: &AppState) -> Result<Json<DoctorReport>, ApiError> {
    let mut checks = Vec::new();
    match state.store.index() {
        Ok(index) => checks.push(DoctorCheck {
            name: "claim-store".into(),
            status: "pass".into(),
            message: format!("the claim store is readable at index {index}"),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "claim-store".into(),
            status: "fail".into(),
            message: error.to_string(),
        }),
    }
    match state.store.operation_projection_drift() {
        Ok(drift) if drift.is_empty() => checks.push(DoctorCheck {
            name: "operation-projection".into(),
            status: "pass".into(),
            message: "the operation projection matches the claim log".into(),
        }),
        Ok(drift) => checks.push(DoctorCheck {
            name: "operation-projection".into(),
            status: "fail".into(),
            message: format!("operation projection drift: {}", drift.join(", ")),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "operation-projection".into(),
            status: "fail".into(),
            message: error.to_string(),
        }),
    }
    match state.store.operational_repair_plan() {
        Ok(plan) if plan.items.is_empty() => checks.push(DoctorCheck {
            name: "operational-repair".into(),
            status: "pass".into(),
            message: "the graph has no repairable operational contradictions".into(),
        }),
        Ok(plan) => checks.push(DoctorCheck {
            name: "operational-repair".into(),
            status: "warn".into(),
            message: format!(
                "{} graph-authorized repairs are available; inspect `st3 repair dry-run` token {}",
                plan.items.len(),
                plan.token
            ),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "operational-repair".into(),
            status: "fail".into(),
            message: format!("could not compute the operational repair plan: {error}"),
        }),
    }
    match tempfile::Builder::new()
        .prefix(".st3-doctor-")
        .tempfile_in(&state.state_dir)
    {
        Ok(_) => checks.push(DoctorCheck {
            name: "state-directory".into(),
            status: "pass".into(),
            message: format!("{} is a writable directory", state.state_dir.display()),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "state-directory".into(),
            status: "fail".into(),
            message: format!("cannot write {}: {error}", state.state_dir.display()),
        }),
    }
    let desired = state.store.desired_subjects().map_err(ApiError::internal)?;
    let terminal_required = desired.iter().any(|subject| {
        subject
            .member
            .as_ref()
            .is_some_and(|member| member.terminal)
    });
    let pty_snapshot = st_runtime::PtyRuntime::new(state.pty_root.clone())
        .with_binary(state.pty_binary.to_string_lossy())
        .snapshot();
    match &pty_snapshot {
        Ok(items) => checks.push(DoctorCheck {
            name: "pty-runtime".into(),
            status: "pass".into(),
            message: format!("the PTY runtime returned {} sessions", items.len()),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "pty-runtime".into(),
            status: if terminal_required { "fail" } else { "warn" }.into(),
            message: error.to_string(),
        }),
    }
    let isolation = st_runtime::isolation_mode();
    checks.push(DoctorCheck {
        name: "process-isolation".into(),
        status: if isolation == st_runtime::Isolation::DegradedDetached {
            "warn"
        } else {
            "pass"
        }
        .into(),
        message: match isolation {
            st_runtime::Isolation::Scope => "Linux tasks use transient systemd user scopes".into(),
            st_runtime::Isolation::Detached => {
                "tasks use detached process groups on this platform".into()
            }
            st_runtime::Isolation::DegradedDetached => {
                "systemd user scopes are unavailable; a daemon restart can stop tasks".into()
            }
        },
    });
    let mut owners = BTreeMap::<String, Vec<String>>::new();
    for subject in &desired {
        if let Some(member) = &subject.member {
            owners
                .entry(member.runtime_id.clone())
                .or_default()
                .push(subject.subject.clone());
        }
    }
    let duplicates = owners
        .into_iter()
        .filter(|(_, subjects)| subjects.len() > 1)
        .map(|(runtime, subjects)| format!("{runtime}: {}", subjects.join(", ")))
        .collect::<Vec<_>>();
    checks.push(DoctorCheck {
        name: "runtime-ownership".into(),
        status: if duplicates.is_empty() {
            "pass"
        } else {
            "fail"
        }
        .into(),
        message: if duplicates.is_empty() {
            "each desired member has a unique runtime ID".into()
        } else {
            format!("duplicate runtime owners: {}", duplicates.join("; "))
        },
    });
    let status = state.store.status(None).map_err(ApiError::internal)?;
    let mut desired_runtime_ids = desired
        .iter()
        .filter_map(|subject| {
            subject
                .member
                .as_ref()
                .map(|member| member.runtime_id.clone())
        })
        .collect::<std::collections::BTreeSet<_>>();
    for subject in &status.subjects {
        if subject.desired.is_none() {
            continue;
        }
        if let Some(runtime_id) = subject
            .actual
            .as_ref()
            .map(|actual| actual.get("fields").unwrap_or(actual))
            .and_then(|fields| fields.get("runtime_id"))
            .and_then(Value::as_str)
        {
            desired_runtime_ids.insert(runtime_id.into());
        }
    }
    let mut unowned = pty_snapshot
        .as_ref()
        .map(|items| {
            items
                .iter()
                .filter(|item| !desired_runtime_ids.contains(&item.name))
                .map(|item| format!("PTY {}", item.name))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let exec_directory = state.state_dir.join("exec");
    if let Ok(entries) = fs::read_dir(&exec_directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(runtime_id) = name.strip_suffix(".json") else {
                continue;
            };
            if !desired_runtime_ids.contains(runtime_id) {
                unowned.push(format!("exec {runtime_id}"));
            }
        }
    }
    checks.push(DoctorCheck {
        name: "runtime-drift".into(),
        status: if unowned.is_empty() { "pass" } else { "warn" }.into(),
        message: if unowned.is_empty() {
            "the runtime has no unowned sessions or current records".into()
        } else {
            format!("unowned runtime state: {}", unowned.join(", "))
        },
    });
    let driver_gaps = status
        .subjects
        .iter()
        .filter(|subject| {
            desired.iter().any(|desired| {
                desired.subject == subject.subject
                    && desired
                        .member
                        .as_ref()
                        .and_then(|member| member.driver.as_ref())
                        .is_some()
            }) && (!subject
                .harness
                .as_ref()
                .is_some_and(crate::model::CurrentHarnessView::is_ready)
                || subject.gap.is_some())
        })
        .map(|subject| {
            format!(
                "{}: {}",
                subject.subject,
                subject
                    .harness
                    .as_ref()
                    .and_then(|harness| harness.reason.as_deref())
                    .or(subject.gap.as_deref())
                    .unwrap_or("the current harness incarnation is not ready")
            )
        })
        .collect::<Vec<_>>();
    checks.push(DoctorCheck {
        name: "driver-readiness".into(),
        status: if driver_gaps.is_empty() {
            "pass"
        } else {
            "warn"
        }
        .into(),
        message: if driver_gaps.is_empty() {
            "all desired native drivers have a ready current harness incarnation".into()
        } else {
            driver_gaps.join("; ")
        },
    });
    match state.store.replication_status(
        state.fleet_id.is_some(),
        state.fleet_id.as_deref(),
        &state.configured_peers,
    ) {
        Ok(replication) if !replication.configured => checks.push(DoctorCheck {
            name: "replication".into(),
            status: "pass".into(),
            message: "this node is intentionally local-only".into(),
        }),
        Ok(replication) => {
            let unavailable = replication
                .peers
                .iter()
                .filter(|peer| peer.status != "up")
                .map(|peer| format!("{}={}", peer.peer, peer.status))
                .collect::<Vec<_>>();
            let unresolved = replication.invalid_records + replication.unknown_records;
            let status = if replication.unhealthy_projections != 0 {
                "fail"
            } else if !unavailable.is_empty() || unresolved != 0 {
                "warn"
            } else {
                "pass"
            };
            checks.push(DoctorCheck {
                name: "replication".into(),
                status: status.into(),
                message: format!(
                    "{} envelopes; {} unresolved records; {} unhealthy projections; peers {}",
                    replication.received_envelopes,
                    unresolved,
                    replication.unhealthy_projections,
                    if unavailable.is_empty() {
                        "up".into()
                    } else {
                        unavailable.join(", ")
                    }
                ),
            });
        }
        Err(error) => checks.push(DoctorCheck {
            name: "replication".into(),
            status: "fail".into(),
            message: error.to_string(),
        }),
    }
    let report_status = if checks.iter().any(|check| check.status == "fail") {
        "fail"
    } else if checks.iter().any(|check| check.status == "warn") {
        "warn"
    } else {
        "pass"
    };
    Ok(Json(DoctorReport {
        status: report_status.into(),
        checks,
    }))
}

async fn operational_repair_plan(
    State(state): State<AppState>,
) -> Result<Json<OperationalRepairPlan>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.operational_repair_plan())
        .await
        .map(Json)
}

async fn apply_operational_repair(
    State(state): State<AppState>,
    Json(request): Json<OperationalRepairApplyRequest>,
) -> Result<Json<OperationalRepairResult>, ApiError> {
    let store = state.store.clone();
    let result = blocking_action(move || store.apply_operational_repair(&request.token)).await?;
    if result.applied != 0 {
        signal_changed(&state);
    }
    Ok(Json(result))
}

async fn replication_status(
    State(state): State<AppState>,
) -> Result<Json<ReplicationStatus>, ApiError> {
    let store = state.store.clone();
    let configured = state.fleet_id.is_some();
    let fleet = state.fleet_id.clone();
    let peers = state.configured_peers.clone();
    blocking_store(move || store.replication_status(configured, fleet.as_deref(), &peers))
        .await
        .map(Json)
}

#[derive(Deserialize)]
struct ReplicationRecordsQuery {
    #[serde(default = "default_true")]
    unresolved: bool,
}

fn default_true() -> bool {
    true
}

async fn replication_records(
    State(state): State<AppState>,
    Query(query): Query<ReplicationRecordsQuery>,
) -> Result<Json<Vec<ReplicaRecordView>>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.replica_records(query.unresolved))
        .await
        .map(Json)
}

async fn replication_record(
    State(state): State<AppState>,
    AxumPath(record): AxumPath<String>,
) -> Result<Json<ReplicaRecordView>, ApiError> {
    let record = if record.starts_with("record/") {
        record
    } else {
        format!("record/{record}")
    };
    let store = state.store.clone();
    let record_for_read = record.clone();
    blocking_store(move || store.replica_record(&record_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("replica record `{record}` does not exist")))
}

async fn repair_replication_record(
    State(state): State<AppState>,
    Json(request): Json<ReplicationRepairRequest>,
) -> Result<Json<ClaimRecord>, ApiError> {
    let claim = state
        .store
        .repair_replica_record(
            &request.record_ref,
            &request.replacement_claim_id,
            &request.reason,
            &request.actor,
            &request.idempotency_key,
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(claim))
}

async fn replication_export(
    State(state): State<AppState>,
    Json(request): Json<ReplicationExportRequest>,
) -> Result<Json<ReplicationExportResponse>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || {
        let exchange = if request.summary_only {
            store.export_replication_summary(&request.fleet_id)?
        } else {
            store.export_replication_exchange(&request.fleet_id, &request.inventory)?
        };
        Ok(Json(ReplicationExportResponse {
            exchange,
            store_index: store.index()?,
        }))
    })
    .await
}

async fn replication_receive(
    State(state): State<AppState>,
    Json(request): Json<ReplicationReceiveRequest>,
) -> Result<Json<ReplicationReceiveResponse>, ApiError> {
    let store = state.store.clone();
    let response = blocking_action(move || {
        let receipt = store.receive_replication_exchange(
            &request.peer,
            &request.fleet_id,
            &request.exchange,
        )?;
        let (admission, repairs, projected) = if replication_receive_has_new_data(receipt.received)
        {
            let admission = store
                .validate_replication_backlog()
                .map_err(|error| St3Error::new("internal", error.to_string()))?;
            let repairs = store
                .apply_replication_repairs()
                .map_err(|error| St3Error::new("internal", error.to_string()))?;
            let projected = store
                .project_replication_backlog()
                .map_err(|error| St3Error::new("internal", error.to_string()))?;
            (admission, repairs, projected)
        } else {
            (Default::default(), 0, true)
        };
        let changed = projected && (admission.changed || repairs != 0);
        let store_index = store
            .index()
            .map_err(|error| St3Error::new("internal", error.to_string()))?;
        Ok(ReplicationReceiveResponse {
            receipt,
            changed,
            store_index,
        })
    })
    .await?;
    if response.changed {
        state.notify.notify_one();
        state
            .event_notify
            .send_modify(|generation| *generation = generation.saturating_add(1));
    }
    Ok(Json(response))
}

fn replication_receive_has_new_data(received: usize) -> bool {
    received != 0
}

async fn replication_peer_failure(
    State(state): State<AppState>,
    Json(request): Json<ReplicationPeerFailureRequest>,
) -> Result<Json<Value>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || {
        store.record_peer_failure(&request.peer, &request.status, &request.error)?;
        Ok(Json(json!({ "recorded": true })))
    })
    .await
}

async fn replication_wake(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let admission = state
        .store
        .validate_replication_backlog()
        .map_err(ApiError::internal)?;
    let repairs = state
        .store
        .apply_replication_repairs()
        .map_err(ApiError::internal)?;
    let projected = state
        .store
        .project_replication_backlog()
        .map_err(ApiError::internal)?;
    if projected && (admission.changed || repairs != 0) {
        signal_changed(&state);
    }
    Ok(Json(json!({
        "admitted": admission.valid,
        "unknown": admission.unknown,
        "invalid": admission.invalid,
        "repairs": repairs,
        "projected": projected,
    })))
}

async fn start_planning_session(
    State(state): State<AppState>,
    Json(request): Json<PlanningSessionStartRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let request_text = std::str::from_utf8(&request.request).map_err(|_| {
        ApiError::bad(St3Error::new(
            "launch-request-not-text",
            "a launch request must contain valid UTF-8",
        ))
    })?;
    if request_text.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-launch-request",
            "a launch request cannot be empty",
        )));
    }
    let target_run = request
        .run
        .as_deref()
        .map(|run| state.store.mission_run(run))
        .transpose()
        .map_err(ApiError::internal)?
        .flatten();
    if request.run.is_some() && target_run.is_none() {
        return Err(ApiError::not_found("the target mission run does not exist"));
    }
    if let Some(run) = &target_run
        && (!matches!(run.status.as_str(), "running" | "blocked") || run.phase != "normal")
    {
        return Err(ApiError::bad(St3Error::new(
            "mission-run-not-revisable",
            format!(
                "mission run `{}` is {} in its {} phase",
                run.subject, run.status, run.phase
            ),
        )));
    }
    let mission_id = target_run
        .as_ref()
        .map(|run| {
            run.mission
                .strip_prefix("mission/")
                .unwrap_or(&run.mission)
                .to_owned()
        })
        .unwrap_or_else(|| request.mission.clone());
    crate::mission::validate_mission_id(&mission_id).map_err(ApiError::bad)?;
    let id = hex::encode(Sha256::digest(request.idempotency_key.as_bytes()))[..24].to_owned();
    let request_name = format!("doc/planning/{id}/request");
    let request_document = state
        .store
        .put_document(
            &request_name,
            &request.request,
            &None,
            &format!("{}:request", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    let requester =
        normalize_planning_reviewer(request.requester.as_deref().unwrap_or("person/requester"));
    let planner_alias = format!("agent/planner.{}", &id[..10]);
    let request_reference = format!("{}@{}", request_document.name, request_document.hash);
    let context_reference = if let Some(run) = &target_run {
        let mission = state
            .store
            .mission_spec(&mission_id, Some(&run.revision))
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::internal("the target mission revision is unavailable"))?;
        let context = serde_json::to_vec_pretty(&json!({"mission_run": run, "mission": mission}))
            .map_err(ApiError::internal)?;
        let document = state
            .store
            .put_document(
                &format!("doc/planning/{id}/run-context"),
                &context,
                &None,
                &format!("{}:run-context", request.idempotency_key),
            )
            .map_err(ApiError::bad)?;
        Some(format!("{}@{}", document.name, document.hash))
    } else {
        None
    };
    let expected_subject = state
        .store
        .selected_desired_token(&planner_alias)
        .map_err(ApiError::internal)?
        .into_iter()
        .collect();
    let prompt = format!(
        "You are the durable Codex planner for launch {id}. Use `st3 message ls`, read and archive the native Small Talk request, and use `st3 doc get` for each immutable document reference. Write one Markdown mission and one complete version 2 KDL mission. The KDL mission ID must be `{mission_id}` and its state must be ready. You can submit named variants with `st3 launch submit {id} --variant NAME --markdown FILE --kdl FILE`. Use temporary files outside the workspace, and remove them after submission. Do not change the workspace. Do not publish or run the mission. Stay available for revision messages until approval or cancellation."
    );
    let planner = quick_agent(
        &state,
        QuickAgentRequest {
            subject: planner_alias,
            worktree: request.workspace.clone(),
            model: request.model,
            effort: request.effort,
            prompt: Some(prompt),
            arguments: vec![
                "--dangerously-bypass-approvals-and-sandbox".into(),
                "--dangerously-bypass-hook-trust".into(),
            ],
            expected_subject,
            idempotency_key: format!("{}:planner", request.idempotency_key),
        },
        "codex",
    )
    .await?;
    let session = state
        .store
        .create_planning_session(
            &id,
            &mission_id,
            &request_reference,
            &request.workspace,
            &requester,
            &planner.subject,
            target_run.as_ref().map(|run| run.subject.as_str()),
            target_run.as_ref().map(|run| run.generation.as_str()),
        )
        .map_err(ApiError::bad)?;
    send_planning_message(
        &state,
        &format!("planning-request:{id}"),
        &requester,
        &planner.subject,
        &context_reference
            .as_ref()
            .map(|context| format!("{request_reference}\n{context}"))
            .unwrap_or_else(|| request_reference.clone()),
        "Launch request",
    )?;
    let mut started_fields = BTreeMap::from([
        (
            "mission".into(),
            Value::String(format!("mission/{}", session.mission)),
        ),
        ("request".into(), Value::String(request_reference)),
        ("planner".into(), Value::String(session.planner.clone())),
        ("workspace".into(), Value::String(session.workspace.clone())),
        ("requester".into(), Value::String(session.requester.clone())),
    ]);
    if let Some(run) = &session.target_mission_run {
        started_fields.insert("target_run".into(), Value::String(run.clone()));
    }
    if let Some(generation) = &session.source_generation {
        started_fields.insert(
            "target_generation".into(),
            Value::String(generation.clone()),
        );
    }
    record_planning_event(
        &state,
        &session,
        "planning-session.started",
        Some(&requester),
        started_fields,
        &format!("{}:started", request.idempotency_key),
    )?;
    signal_changed(&state);
    Ok(Json(session))
}

async fn get_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let store = state.store.clone();
    let id_for_read = id.clone();
    blocking_store(move || store.planning_session(&id_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("launch `{id}` does not exist")))
}

async fn submit_planning_candidate(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningCandidateSubmitRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    submit_planning_variant(state, id, "default".into(), request).await
}

async fn submit_named_planning_candidate(
    State(state): State<AppState>,
    AxumPath((id, variant)): AxumPath<(String, String)>,
    Json(request): Json<PlanningCandidateSubmitRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    submit_planning_variant(state, id, variant, request).await
}

async fn submit_planning_variant(
    state: AppState,
    id: String,
    variant: String,
    request: PlanningCandidateSubmitRequest,
) -> Result<Json<PlanningSessionView>, ApiError> {
    validate_planning_variant(&variant)?;
    let session = required_planning_session(&state, &id)?;
    let markdown = std::str::from_utf8(&request.markdown).map_err(|_| {
        ApiError::bad(St3Error::new(
            "launch-markdown-not-text",
            "planning Markdown must contain valid UTF-8",
        ))
    })?;
    if markdown.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-launch-markdown",
            "planning Markdown cannot be empty",
        )));
    }
    let kdl = std::str::from_utf8(&request.kdl).map_err(|_| {
        ApiError::bad(St3Error::new(
            "launch-kdl-not-text",
            "planning KDL must contain valid UTF-8",
        ))
    })?;
    let (intent, _) = mission_source(&state, kdl, None)?;
    if !intent.subjects.is_empty()
        || intent.missions.len() != 1
        || !intent.missions.contains_key(&session.mission)
    {
        return Err(ApiError::bad(St3Error::new(
            "wrong-launch-mission",
            format!(
                "a candidate must contain only ready mission `{}` and no immediate desired state",
                session.mission
            ),
        )));
    }
    let mission = &intent.missions[&session.mission];
    if mission.state != crate::model::MissionState::Ready {
        return Err(ApiError::bad(St3Error::new(
            "launch-mission-not-ready",
            "a planning candidate must contain a ready mission",
        )));
    }
    let mut content_hasher = Sha256::new();
    content_hasher.update(b"st3.planning-candidate.v1\0");
    content_hasher.update((request.markdown.len() as u64).to_be_bytes());
    content_hasher.update(&request.markdown);
    content_hasher.update((request.kdl.len() as u64).to_be_bytes());
    content_hasher.update(&request.kdl);
    let content_hash = hex::encode(content_hasher.finalize());
    let markdown_name = format!(
        "doc/planning/{}/candidate/{content_hash}/markdown",
        session.id
    );
    let kdl_name = format!("doc/planning/{}/candidate/{content_hash}/kdl", session.id);
    let markdown_document = state
        .store
        .put_document(
            &markdown_name,
            &request.markdown,
            &None,
            &format!("planning-candidate:{}:{content_hash}:markdown", session.id),
        )
        .map_err(ApiError::bad)?;
    let kdl_document = state
        .store
        .put_document(
            &kdl_name,
            &request.kdl,
            &None,
            &format!("planning-candidate:{}:{content_hash}:kdl", session.id),
        )
        .map_err(ApiError::bad)?;
    let response = state
        .store
        .add_planning_candidate(
            &session.id,
            &request.actor,
            &variant,
            &format!("{}@{}", markdown_document.name, markdown_document.hash),
            &format!("{}@{}", kdl_document.name, kdl_document.hash),
            &mission.revision,
        )
        .map_err(ApiError::bad)?;
    let candidate = response
        .variants
        .iter()
        .find(|candidate| candidate.name == variant)
        .map(|variant| &variant.candidate)
        .expect("the submitted planning candidate is visible");
    record_planning_event(
        &state,
        &response,
        "planning-session.candidate-submitted",
        Some(&request.actor),
        BTreeMap::from([
            ("variant".into(), Value::String(variant.clone())),
            ("candidate_revision".into(), Value::from(candidate.revision)),
            ("markdown".into(), Value::String(candidate.markdown.clone())),
            ("kdl".into(), Value::String(candidate.kdl.clone())),
            (
                "mission_revision".into(),
                Value::String(candidate.mission_revision.clone()),
            ),
        ]),
        &format!(
            "planning-candidate:{}:{variant}:{}",
            response.id, candidate.revision
        ),
    )?;
    signal_changed(&state);
    preview_planning_variant(state, id, variant).await
}

async fn preview_planning_candidate(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    preview_planning_variant(state, id, "default".into()).await
}

async fn preview_named_planning_candidate(
    State(state): State<AppState>,
    AxumPath((id, variant)): AxumPath<(String, String)>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    preview_planning_variant(state, id, variant).await
}

async fn preview_planning_variant(
    state: AppState,
    id: String,
    variant: String,
) -> Result<Json<PlanningSessionView>, ApiError> {
    validate_planning_variant(&variant)?;
    let session = required_planning_session(&state, &id)?;
    if session.status != "review" {
        return Err(ApiError::bad(St3Error::new(
            "launch-not-reviewable",
            format!("launch `{}` is {}", session.id, session.status),
        )));
    }
    let candidate = session
        .variants
        .iter()
        .find(|candidate| candidate.name == variant)
        .map(|variant| &variant.candidate)
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-launch-candidate",
                "the launch has no candidate",
            ))
        })?;
    let kdl = planning_document_text(&state, &candidate.kdl)?;
    let (intent, mission_response) = mission_source(&state, &kdl, None)?;
    let mission = &intent.missions[&session.mission];
    let graph = render_planning_graph(mission);
    let diff = render_planning_diff(&mission_response);
    let mut normalized = serde_json::to_value(mission).map_err(ApiError::internal)?;
    client_safe_json(&mut normalized);
    let diagnostics = launch_diagnostics(&mission_response.blockers, &mission_response.warnings);
    let hash = launch_preview_token_values(
        &session.id,
        &variant,
        candidate.revision,
        session.source_generation.as_deref(),
        &normalized,
        &diagnostics,
    )
    .map_err(ApiError::internal)?;
    if session
        .variants
        .iter()
        .find(|candidate| candidate.name == variant)
        .and_then(|variant| variant.preview.as_ref())
        .is_some_and(|preview| {
            preview.candidate_revision == candidate.revision && preview.hash == hash
        })
    {
        return Ok(Json(session));
    }
    let response = state
        .store
        .save_planning_preview(
            &session.id,
            &variant,
            candidate.revision,
            &hash,
            &graph,
            &diff,
            &mission_response,
        )
        .map_err(ApiError::bad)?;
    let preview = response
        .variants
        .iter()
        .find(|candidate| candidate.name == variant)
        .and_then(|variant| variant.preview.as_ref())
        .expect("the saved launch preview is visible");
    record_planning_event(
        &state,
        &response,
        "planning-session.previewed",
        Some(&response.requester),
        BTreeMap::from([
            ("variant".into(), Value::String(variant.clone())),
            (
                "candidate_revision".into(),
                Value::from(preview.candidate_revision),
            ),
            ("preview_hash".into(), Value::String(preview.hash.clone())),
            ("store_index".into(), Value::from(preview.store_index)),
            ("graph".into(), Value::String(preview.graph.clone())),
            ("diff".into(), Value::String(preview.diff.clone())),
            (
                "mission".into(),
                serde_json::to_value(&preview.mission).map_err(ApiError::internal)?,
            ),
        ]),
        &format!("planning-preview:{}:{}", response.id, preview.hash),
    )?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn compare_planning_variants(
    State(state): State<AppState>,
    AxumPath((id, left, right)): AxumPath<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    let variant = |name: &str| {
        session
            .variants
            .iter()
            .find(|variant| variant.name == name)
            .ok_or_else(|| ApiError::not_found(format!("launch variant `{name}` does not exist")))
    };
    let left = variant(&left)?;
    let right = variant(&right)?;
    Ok(Json(json!({
        "session": session.subject,
        "source_generation": session.source_generation,
        "left": left,
        "right": right,
    })))
}

async fn propose_planning_variant(
    State(state): State<AppState>,
    AxumPath((id, variant)): AxumPath<(String, String)>,
    Json(request): Json<PlanningProposalRequest>,
) -> Result<Json<RevisionSubmissionView>, ApiError> {
    if let Some(cached) = cached_revision_submission(
        &state,
        &format!("{}:cutover", request.idempotency_key),
        &format!("{}:proposal", request.idempotency_key),
    )? {
        return Ok(Json(cached));
    }
    let session = required_planning_session(&state, &id)?;
    let run_subject = session.target_mission_run.as_deref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "launch-has-no-run",
            "this launch creates a new mission and cannot propose a run revision",
        ))
    })?;
    let current = state
        .store
        .mission_run(run_subject)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("the target mission run does not exist"))?;
    if session.source_generation.as_deref() != Some(current.generation.as_str()) {
        return Err(ApiError::bad(St3Error::new(
            "stale-launch-generation",
            "the launch variants target a superseded generation",
        )));
    }
    let variant = session
        .variants
        .iter()
        .find(|candidate| candidate.name == variant)
        .ok_or_else(|| ApiError::not_found(format!("launch variant `{variant}` does not exist")))?;
    let preview = variant.preview.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-launch-preview",
            "preview the named variant before proposal",
        ))
    })?;
    if !preview.mission.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "launch-preview-blocked",
            preview.mission.blockers.join("; "),
        )));
    }
    let intent =
        parse_intent(&preview.mission.resolved_intent.kdl, &state.node).map_err(ApiError::bad)?;
    let mission = &intent.missions[&session.mission];
    let old = state
        .store
        .mission_spec(&session.mission, Some(&current.revision))
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::internal("the current mission revision is unavailable"))?;
    let (_, reviewers) = crate::store::analyze_mission_revision(
        &old,
        mission,
        &request.actor,
        &current.requester,
        &crate::store::mission_run_variables(&current, &mission.revision),
    )
    .map_err(ApiError::bad)?;
    state
        .store
        .apply(
            &intent,
            &preview.mission.subject_tokens,
            &format!("{}:publish", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    let result =
        if reviewers.is_empty() && matches!(old.revision_cutover, RevisionCutover::RestartActive) {
            RevisionSubmissionView {
                status: "applied".into(),
                mission_run: state
                    .store
                    .adopt_mission_revision(
                        run_subject,
                        mission,
                        &request.actor,
                        &request.reason,
                        &format!("{}:cutover", request.idempotency_key),
                    )
                    .map_err(ApiError::bad)?,
                proposal: None,
            }
        } else {
            let proposal = state
                .store
                .create_revision_proposal(
                    run_subject,
                    mission,
                    &request.actor,
                    &request.reason,
                    &format!("{}:proposal", request.idempotency_key),
                )
                .map_err(ApiError::bad)?;
            RevisionSubmissionView {
                status: proposal.status.clone(),
                mission_run: state
                    .store
                    .mission_run(run_subject)
                    .map_err(ApiError::internal)?
                    .expect("the target mission run exists"),
                proposal: Some(proposal),
            }
        };
    signal_changed(&state);
    Ok(Json(result))
}

fn validate_planning_variant(variant: &str) -> Result<(), ApiError> {
    if variant.is_empty()
        || variant.len() > 80
        || !variant
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ApiError::bad(St3Error::new(
            "invalid-launch-variant",
            "a launch variant must use 1 through 80 letters, digits, dots, dashes, or underscores",
        )));
    }
    Ok(())
}

async fn revise_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningRevisionRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    if request.feedback.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-launch-feedback",
            "launch feedback cannot be empty",
        )));
    }
    std::str::from_utf8(&request.feedback).map_err(|_| {
        ApiError::bad(St3Error::new(
            "launch-feedback-not-text",
            "launch feedback must contain valid UTF-8",
        ))
    })?;
    let feedback_hash = hex::encode(Sha256::digest(&request.feedback));
    let name = format!("doc/planning/{}/feedback/{feedback_hash}", session.id);
    let document = state
        .store
        .put_document(
            &name,
            &request.feedback,
            &None,
            &format!("{}:feedback", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    let response = state
        .store
        .request_planning_revision(&session.id, &request.actor)
        .map_err(ApiError::bad)?;
    record_planning_event(
        &state,
        &response,
        "planning-session.revision-requested",
        Some(&request.actor),
        BTreeMap::from([(
            "feedback".into(),
            Value::String(format!("{}@{}", document.name, document.hash)),
        )]),
        &format!("{}:revision-requested", request.idempotency_key),
    )?;
    send_planning_message(
        &state,
        &format!("planning-revision:{}:{feedback_hash}", session.id),
        &request.actor,
        &session.planner,
        &format!("{}@{}", document.name, document.hash),
        "Launch revision requested",
    )?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn approve_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningApprovalRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    if session.status != "review" && session.status != "approved" {
        return Err(ApiError::bad(St3Error::new(
            "launch-not-reviewable",
            format!("launch `{}` is {}", session.id, session.status),
        )));
    }
    let candidate = session.candidate.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-launch-candidate",
            "the launch has no candidate",
        ))
    })?;
    let preview = session.preview.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-launch-preview",
            "preview the candidate before approval",
        ))
    })?;
    let variant = session
        .variants
        .iter()
        .find(|variant| variant.name == candidate.variant)
        .ok_or_else(|| ApiError::internal("the selected launch variant is unavailable"))?;
    let projected =
        client_launch_variant_resources(&state, &session).map_err(ApiError::internal)?;
    let approval_token = projected
        .iter()
        .find(|resource| {
            resource["id"] == format!("launch-variant/{}/{}", session.id, variant.name)
        })
        .and_then(|resource| resource["preview_token"].as_str())
        .ok_or_else(|| ApiError::internal("the selected launch preview has no approval token"))?;
    if (preview.hash != request.preview_hash && approval_token != request.preview_hash)
        || preview.candidate_revision != candidate.revision
    {
        return Err(ApiError::bad(St3Error::new(
            "stale-launch-preview",
            "the approval does not name the current preview",
        )));
    }
    if session.status == "approved" {
        if session.published_revision.as_deref() == Some(candidate.mission_revision.as_str()) {
            let approval_key = format!("planning-approval:{}:{}", session.id, preview.hash);
            stop_planning_agent(&state, &session.planner, &approval_key)?;
            signal_changed(&state);
            return Ok(Json(session));
        }
        return Err(ApiError::internal(format!(
            "launch `{}` has inconsistent approved revision state",
            session.id
        )));
    }
    if !preview.mission.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "launch-preview-blocked",
            preview.mission.blockers.join("; "),
        )));
    }
    let target = if let Some(run_subject) = session.target_mission_run.as_deref() {
        let current = state
            .store
            .mission_run(run_subject)
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::not_found("the target mission run does not exist"))?;
        if session.source_generation.as_deref() != Some(current.generation.as_str()) {
            return Err(ApiError::bad(St3Error::new(
                "stale-launch-generation",
                "the launch approval targets a superseded generation",
            )));
        }
        Some((run_subject.to_owned(), current))
    } else {
        None
    };
    let intent =
        parse_intent(&preview.mission.resolved_intent.kdl, &state.node).map_err(ApiError::bad)?;
    let approval_key = format!("planning-approval:{}:{}", session.id, preview.hash);
    state
        .store
        .apply(
            &intent,
            &preview.mission.subject_tokens,
            &format!("{approval_key}:publish"),
        )
        .map_err(ApiError::bad)?;
    if let Some((run_subject, current)) = target {
        let mission = &intent.missions[&session.mission];
        let old = state
            .store
            .mission_spec(&session.mission, Some(&current.revision))
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::internal("the current mission revision is unavailable"))?;
        let (_, reviewers) = crate::store::analyze_mission_revision(
            &old,
            mission,
            &request.actor,
            &current.requester,
            &crate::store::mission_run_variables(&current, &mission.revision),
        )
        .map_err(ApiError::bad)?;
        if reviewers.is_empty() && matches!(old.revision_cutover, RevisionCutover::RestartActive) {
            state
                .store
                .adopt_mission_revision(
                    &run_subject,
                    mission,
                    &request.actor,
                    "the requester approved the launch revision",
                    &format!("{approval_key}:cutover"),
                )
                .map_err(ApiError::bad)?;
        } else {
            let proposal = state
                .store
                .create_revision_proposal(
                    &run_subject,
                    mission,
                    &request.actor,
                    "the requester approved the launch revision",
                    &format!("{approval_key}:proposal"),
                )
                .map_err(ApiError::bad)?;
            if proposal.reviewers.contains(&request.actor) {
                let revision_preview = proposal
                    .preview_hash
                    .as_deref()
                    .ok_or_else(|| ApiError::internal("the revision proposal has no preview"))?;
                state
                    .store
                    .approve_revision_proposal(
                        &proposal.subject,
                        &request.actor,
                        revision_preview,
                        &format!("{approval_key}:revision-approval"),
                    )
                    .map_err(ApiError::bad)?;
            }
        }
    }
    let response = state
        .store
        .finish_planning_session(
            &session.id,
            &request.actor,
            "approved",
            Some(&candidate.mission_revision),
        )
        .map_err(ApiError::bad)?;
    record_planning_event(
        &state,
        &response,
        "planning-session.approved",
        Some(&request.actor),
        BTreeMap::from([
            ("variant".into(), Value::String(candidate.variant.clone())),
            ("candidate_revision".into(), Value::from(candidate.revision)),
            ("preview_hash".into(), Value::String(preview.hash.clone())),
            (
                "preview_token".into(),
                Value::String(approval_token.to_owned()),
            ),
            (
                "mission_revision".into(),
                Value::String(candidate.mission_revision.clone()),
            ),
            ("markdown".into(), Value::String(candidate.markdown.clone())),
            ("kdl".into(), Value::String(candidate.kdl.clone())),
            (
                "requester".into(),
                Value::String(response.requester.clone()),
            ),
        ]),
        &format!("{approval_key}:event"),
    )?;
    stop_planning_agent(&state, &session.planner, &approval_key)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn start_approved_launch(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<LaunchStartRequest>,
) -> Result<Json<MissionRunView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    if session.status != "approved" {
        return Err(ApiError::bad(St3Error::new(
            "launch-not-approved",
            format!("launch `{}` is {}", session.id, session.status),
        )));
    }
    if session.target_mission_run.is_some() {
        return Err(ApiError::bad(St3Error::new(
            "launch-target-already-running",
            "a launch targeting an existing run is applied in place and cannot start another run",
        )));
    }
    let revision = session.published_revision.clone().ok_or_else(|| {
        ApiError::internal(format!(
            "approved launch `{}` has no published revision",
            session.id
        ))
    })?;
    require_agent_mission_authority(&state, &request.actor, "start", &session.mission)?;
    let response = state
        .store
        .create_mission_run(&MissionRunRequest {
            mission: session.mission,
            revision: Some(revision),
            workspace: request.workspace,
            requester: Some(normalize_message_party(&request.actor)),
            mode: None,
            inputs: request.inputs,
            idempotency_key: format!("launch-start:{}:{}", session.id, request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn approve_and_start_launch(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<LaunchApproveAndStartRequest>,
) -> Result<Json<LaunchApproveAndStartView>, ApiError> {
    let Json(launch) = approve_planning_session(
        State(state.clone()),
        AxumPath(id.clone()),
        Json(PlanningApprovalRequest {
            actor: request.actor.clone(),
            preview_hash: request.preview_hash,
            idempotency_key: format!("{}:approve", request.idempotency_key),
        }),
    )
    .await?;
    // Approval is intentionally committed before start. If runtime creation fails, the durable
    // approved launch remains recoverable through this same idempotent request or `/start`.
    let Json(mission_run) = start_approved_launch(
        State(state),
        AxumPath(id),
        Json(LaunchStartRequest {
            actor: request.actor,
            workspace: request.workspace,
            inputs: request.inputs,
            idempotency_key: format!("{}:start", request.idempotency_key),
        }),
    )
    .await?;
    Ok(Json(LaunchApproveAndStartView {
        launch,
        mission_run,
    }))
}

async fn request_launch_decision(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<LaunchDecisionRequest>,
) -> Result<Json<Value>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    if normalize_message_party(&request.actor) != session.planner {
        return Err(ApiError::bad(St3Error::new(
            "launch-decision-not-authorized",
            "only the launch planner can request a decision",
        )));
    }
    let question = request.question.trim();
    if question.is_empty() || question.len() > 2_000 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-launch-question",
            "a launch question must contain 1 through 2000 bytes",
        )));
    }
    let choices = request
        .choices
        .iter()
        .map(|choice| choice.trim().to_owned())
        .collect::<Vec<_>>();
    let unique = choices.iter().collect::<std::collections::BTreeSet<_>>();
    if choices.is_empty()
        || choices.len() > 20
        || choices.iter().any(String::is_empty)
        || unique.len() != choices.len()
    {
        return Err(ApiError::bad(St3Error::new(
            "invalid-launch-choices",
            "a launch question needs 1 through 20 distinct non-empty choices",
        )));
    }
    let digest = hex::encode(Sha256::digest(request.idempotency_key.as_bytes()));
    let decision_id = format!("launch-decision/{}", &digest[..32]);
    record_planning_event(
        &state,
        &session,
        "planning-session.question-requested",
        Some(&request.actor),
        BTreeMap::from([
            ("decision_id".into(), Value::String(decision_id.clone())),
            ("revision".into(), Value::from(1)),
            ("question".into(), Value::String(question.to_owned())),
            (
                "choices".into(),
                serde_json::to_value(&choices).map_err(ApiError::internal)?,
            ),
            ("requester".into(), Value::String(session.requester.clone())),
            ("planner".into(), Value::String(session.planner.clone())),
        ]),
        &format!("launch-question:{}:{}", session.id, request.idempotency_key),
    )?;
    send_planning_message(
        &state,
        &format!("launch-question-message:{}:{decision_id}", session.id),
        &session.planner,
        &session.requester,
        &format!(
            "{decision_id}\n{question}\nChoices: {}",
            choices.join(" | ")
        ),
        "Launch decision requested",
    )?;
    signal_changed(&state);
    let value = client_launch_decision_resources(&state.store, &session)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|value| value["id"] == decision_id)
        .ok_or_else(|| ApiError::internal("the recorded launch decision is unavailable"))?;
    Ok(Json(value))
}

async fn answer_launch_decision(
    State(state): State<AppState>,
    AxumPath((id, decision)): AxumPath<(String, String)>,
    Json(request): Json<LaunchDecisionAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    let decision_id = client_detail_id("launch-decision", &decision);
    let decisions =
        client_launch_decision_resources(&state.store, &session).map_err(ApiError::internal)?;
    let current = decisions
        .iter()
        .find(|value| value["id"] == decision_id)
        .ok_or_else(|| {
            ApiError::not_found(format!("launch decision `{decision_id}` does not exist"))
        })?;
    if request.expected_revision != 1 {
        return Err(ApiError::bad(St3Error::new(
            "stale-launch-decision",
            "the launch decision revision changed",
        )));
    }
    if current["state"] == "answered" {
        if current["answer"].as_str() == Some(request.answer.as_str()) {
            return Ok(Json(current.clone()));
        }
        return Err(ApiError::bad(St3Error::new(
            "launch-decision-immutable",
            "a launch decision answer is immutable",
        )));
    }
    let choices = current["choices"].as_array().cloned().unwrap_or_default();
    if !choices
        .iter()
        .any(|choice| choice.as_str() == Some(request.answer.as_str()))
    {
        return Err(ApiError::bad(St3Error::new(
            "invalid-launch-answer",
            "the answer must equal one of the question choices",
        )));
    }
    record_planning_event(
        &state,
        &session,
        "planning-session.question-answered",
        Some(&request.actor),
        BTreeMap::from([
            ("decision_id".into(), Value::String(decision_id.clone())),
            (
                "expected_revision".into(),
                Value::from(request.expected_revision),
            ),
            ("answer".into(), Value::String(request.answer.clone())),
            ("requester".into(), Value::String(session.requester.clone())),
        ]),
        &format!("launch-answer:{}:{}", session.id, request.idempotency_key),
    )?;
    send_planning_message(
        &state,
        &format!("launch-answer-message:{}:{decision_id}", session.id),
        &session.requester,
        &session.planner,
        &format!("{decision_id}\nAnswer: {}", request.answer),
        "Launch decision answered",
    )?;
    close_planning_message(
        &state,
        &format!("launch-question-message:{}:{decision_id}", session.id),
        &session.requester,
        &request.idempotency_key,
    )?;
    signal_changed(&state);
    let value = client_launch_decision_resources(&state.store, &session)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|value| value["id"] == decision_id)
        .ok_or_else(|| ApiError::internal("the answered launch decision is unavailable"))?;
    Ok(Json(value))
}

async fn cancel_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningCancelRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    if session.status == "cancelled" {
        stop_planning_agent(&state, &session.planner, &request.idempotency_key)?;
        signal_changed(&state);
        return Ok(Json(session));
    }
    let response = state
        .store
        .finish_planning_session(&session.id, &request.actor, "cancelled", None)
        .map_err(ApiError::bad)?;
    record_planning_event(
        &state,
        &response,
        "planning-session.cancelled",
        Some(&request.actor),
        BTreeMap::from([
            (
                "reason".into(),
                request
                    .reason
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "requester".into(),
                Value::String(response.requester.clone()),
            ),
        ]),
        &format!("{}:cancelled", request.idempotency_key),
    )?;
    stop_planning_agent(&state, &session.planner, &request.idempotency_key)?;
    signal_changed(&state);
    Ok(Json(response))
}

fn required_planning_session(state: &AppState, id: &str) -> Result<PlanningSessionView, ApiError> {
    let id = launch_session_id(id);
    state
        .store
        .planning_session(id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("launch `{id}` does not exist")))
}

fn normalize_planning_reviewer(value: &str) -> String {
    if value.contains('/') {
        value.to_owned()
    } else {
        format!("person/{value}")
    }
}

fn authorize_planning_reviewer(session: &PlanningSessionView, actor: &str) -> Result<(), ApiError> {
    if normalize_planning_reviewer(actor) == session.requester {
        return Ok(());
    }
    Err(ApiError::bad(St3Error::new(
        "launch-review-not-authorized",
        format!("`{actor}` cannot review launch `{}`", session.id),
    )))
}

fn record_planning_event(
    state: &AppState,
    session: &PlanningSessionView,
    kind: &str,
    actor: Option<&str>,
    fields: BTreeMap<String, Value>,
    idempotency_key: &str,
) -> Result<(), ApiError> {
    state
        .store
        .append_claim(&ClaimInput {
            subject: session.subject.clone(),
            kind: kind.into(),
            actor: actor.map(normalize_message_party),
            fields,
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(idempotency_key.into()),
        })
        .map_err(ApiError::bad)?;
    state
        .store
        .rebuild_claim_projections()
        .map_err(ApiError::internal)?;
    Ok(())
}

fn planning_document_text(state: &AppState, reference: &str) -> Result<String, ApiError> {
    let (name, hash) = reference.rsplit_once('@').ok_or_else(|| {
        ApiError::internal(format!(
            "planning document reference `{reference}` is invalid"
        ))
    })?;
    let bytes = state
        .store
        .get_document(name, hash)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::internal(format!("planning document `{reference}` is missing")))?;
    String::from_utf8(bytes).map_err(ApiError::internal)
}

fn mission_source(
    state: &AppState,
    kdl: &str,
    at_index: Option<u64>,
) -> Result<(crate::model::NormalizedIntent, MissionResponse), ApiError> {
    let initial = parse_intent(kdl, &state.node).map_err(ApiError::bad)?;
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, at_index)
        .map_err(ApiError::internal)?;
    let resolved_kdl = resolve_document_references(kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let response = state
        .store
        .mission_at(
            &intent,
            crate::model::IntentInput {
                kdl: resolved_kdl,
                source_name: Some("planning candidate".into()),
            },
            at_index,
        )
        .map_err(ApiError::bad)?;
    Ok((intent, response))
}

fn render_planning_graph(mission: &crate::model::MissionSpec) -> String {
    fn append(mission: &crate::model::MissionSpec, indent: &str, lines: &mut Vec<String>) {
        for id in &mission.display_order {
            let step = &mission.steps[id];
            let dependencies = step
                .dependencies
                .iter()
                .filter_map(|dependency| match dependency {
                    crate::model::DependencySpec::Step { step, .. } => Some(step.as_str()),
                    crate::model::DependencySpec::Predicate { .. } => None,
                })
                .collect::<Vec<_>>();
            let suffix = if dependencies.is_empty() {
                "root".into()
            } else {
                format!("after {}", dependencies.join(", "))
            };
            let queue = step
                .queue
                .as_deref()
                .zip(step.queue_position)
                .map(|(queue, position)| format!(" · queue {queue} #{position}"))
                .unwrap_or_default();
            lines.push(format!("{indent}{} [{suffix}]{queue}", step.path));
            for product in &step.products {
                lines.push(format!("{indent}  produces {}", product.subject));
            }
            for gate in &step.gates {
                lines.push(format!("{indent}  gate {}", crate::graph::gate_name(gate)));
            }
            if let Some(nested) = &step.nested_mission {
                append(nested, &format!("{indent}  "), lines);
            }
        }
    }
    let mut lines = vec![format!("mission/{}", mission.id)];
    for baseline in &mission.baselines {
        lines.push(format!("  baseline {}", baseline.name));
    }
    for product in &mission.products {
        lines.push(format!("  produces {}", product.subject));
    }
    for gate in &mission.gates {
        lines.push(format!("  gate {}", crate::graph::gate_name(gate)));
    }
    append(mission, "  ", &mut lines);
    lines.join("\n")
}

fn render_planning_diff(mission: &MissionResponse) -> String {
    if mission.changes.is_empty() {
        return "No graph changes.".into();
    }
    mission
        .changes
        .iter()
        .map(|change| format!("{} {}", change.change, change.subject))
        .collect::<Vec<_>>()
        .join("\n")
}

fn send_planning_message(
    state: &AppState,
    key: &str,
    from: &str,
    to: &str,
    content: &str,
    title: &str,
) -> Result<(), ApiError> {
    let subject = format!(
        "message/{}",
        &hex::encode(Sha256::digest(key.as_bytes()))[..16]
    );
    state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: "message.sent".into(),
            actor: Some(normalize_message_party(from)),
            fields: BTreeMap::from([
                ("from".into(), Value::String(normalize_message_party(from))),
                ("to".into(), Value::String(normalize_message_party(to))),
                ("content".into(), Value::String(content.into())),
                ("status".into(), Value::String("sent".into())),
                ("title".into(), Value::String(title.into())),
                ("in_reply_to".into(), Value::Null),
                (
                    "tags".into(),
                    Value::Array(vec![Value::String("launch".into())]),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(key.into()),
        })
        .map_err(ApiError::bad)?;
    Ok(())
}

fn close_planning_message(
    state: &AppState,
    message_key: &str,
    actor: &str,
    operation_key: &str,
) -> Result<(), ApiError> {
    let subject = format!(
        "message/{}",
        &hex::encode(Sha256::digest(message_key.as_bytes()))[..16]
    );
    for (kind, status) in [
        ("message.delivered", "delivered"),
        ("message.read", "read"),
        ("message.closed", "closed"),
    ] {
        state
            .store
            .append_claim(&ClaimInput {
                subject: subject.clone(),
                kind: kind.into(),
                actor: Some(normalize_message_party(actor)),
                fields: BTreeMap::from([("status".into(), Value::String(status.into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("{operation_key}:{status}")),
            })
            .map_err(ApiError::bad)?;
    }
    Ok(())
}

fn stop_planning_agent(state: &AppState, planner: &str, key: &str) -> Result<(), ApiError> {
    let local_agent = planner
        .strip_prefix("agent/")
        .and_then(|value| value.split_once('/').map(|(_, local)| local))
        .unwrap_or(planner);
    let standing_mission = format!("mission/standing/{local_agent}");
    let cancellation = state
        .store
        .active_mission_runs()
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|run| run.mission == standing_mission)
        .map(|run| run.subject)
        .map(|run| {
            format!(
                "mission-run {:?} {{ cancellation \"planning-session-ended\" {{ reason \"the launch ended\" }} }}\n",
                run
            )
        })
        .unwrap_or_default();
    let kdl = if cancellation.is_empty() {
        format!("version 2\nstop {planner:?}\n")
    } else {
        format!("version 2\n{cancellation}")
    };
    let intent = crate::graph::parse_internal_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    state
        .store
        .apply_internal(&intent, &format!("{key}:stop-planner"))
        .map_err(ApiError::bad)?;
    Ok(())
}

async fn mission(
    State(state): State<AppState>,
    Json(request): Json<MissionRequest>,
) -> Result<Json<MissionResponse>, ApiError> {
    let initial = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, request.at_index)
        .map_err(ApiError::internal)?;
    let resolved_kdl =
        resolve_document_references(&request.intent.kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let resolved = crate::model::IntentInput {
        kdl: resolved_kdl,
        source_name: request.intent.source_name,
    };
    state
        .store
        .mission_at(&intent, resolved, request.at_index)
        .map(Json)
        .map_err(ApiError::bad)
}

async fn apply(
    State(state): State<AppState>,
    Json(request): Json<ApplyRequest>,
) -> Result<Json<ApplyResponse>, ApiError> {
    let actor = request.actor.as_deref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-publication-actor",
            "publication needs `--as` or ST_AGENT",
        ))
    })?;
    let intent = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    if normalized_agent_actor(actor).is_some() && !intent.missions.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "agent-mission-publication-route",
            "an agent must publish a mission through `st3 work publish-mission` or `st3 work revise`",
        )));
    }
    for declaration in intent.mission_runs.values() {
        if let Some(creation) = &declaration.creation {
            require_agent_mission_authority(&state, actor, "start", &creation.mission)?;
        }
        for revision in declaration.revisions.values() {
            require_agent_mission_authority(&state, actor, "revise", &revision.mission)?;
        }
    }
    let mut response = state
        .store
        .apply_as(
            &intent,
            &request.expected_subjects,
            &request.idempotency_key,
            Some(actor),
        )
        .map_err(ApiError::bad)?;
    response.resolved_kdl = request.intent.kdl;
    signal_changed(&state);
    Ok(Json(response))
}

async fn get_mission(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<crate::model::MissionSpec>, ApiError> {
    let id = id.strip_prefix("mission/").unwrap_or(&id).to_owned();
    let store = state.store.clone();
    let id_for_read = id.clone();
    blocking_store(move || store.mission_spec(&id_for_read, None))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("mission `mission/{id}` does not exist")))
}

async fn put_document(
    State(state): State<AppState>,
    Json(request): Json<DocumentPutRequest>,
) -> Result<Json<DocumentVersion>, ApiError> {
    let response = state
        .store
        .put_document(
            &request.name,
            &request.bytes,
            &request.expected_document,
            &request.idempotency_key,
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn watch_resource(
    State(state): State<AppState>,
    Json(request): Json<ResourceWatchRequest>,
) -> Result<Json<ResourceWatchView>, ApiError> {
    if request.fields.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "missing-subscription-field",
            "a resource watch needs at least one field",
        )));
    }
    let target = request.to.ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-subscription-target",
            "a resource watch needs a delivery target",
        ))
    })?;
    let target = normalize_message_party(&target);
    let (resource_name, resource_kind) = match request.provider.as_str() {
        "github.pull-request" => {
            for field in &request.fields {
                if !matches!(field.as_str(), "head" | "state" | "review" | "checks") {
                    return Err(ApiError::bad(St3Error::new(
                        "invalid-subscription-field",
                        format!("GitHub pull request provider does not support field `{field}`"),
                    )));
                }
            }
            let (repository, number) = request.locator.rsplit_once('#').ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a GitHub pull request locator needs OWNER/REPO#NUMBER",
                ))
            })?;
            let (owner, repository) = repository.split_once('/').ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a GitHub pull request locator needs OWNER/REPO#NUMBER",
                ))
            })?;
            number.parse::<u64>().map_err(|_| {
                ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a GitHub pull request number must be an integer",
                ))
            })?;
            (
                format!("github/{owner}/{repository}/pull/{number}"),
                "vcs.pull-request",
            )
        }
        "github.repository" => {
            for field in &request.fields {
                if !matches!(field.as_str(), "pull_requests" | "issues") {
                    return Err(ApiError::bad(St3Error::new(
                        "invalid-subscription-field",
                        format!("GitHub repository provider does not support field `{field}`"),
                    )));
                }
            }
            let (owner, repository) = request.locator.split_once('/').ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a GitHub repository locator needs OWNER/REPO",
                ))
            })?;
            if owner.is_empty() || repository.is_empty() || repository.contains('/') {
                return Err(ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a GitHub repository locator needs OWNER/REPO",
                )));
            }
            (format!("github/{owner}/{repository}"), "vcs.repository")
        }
        "local.file" => {
            let path = std::path::Path::new(&request.locator);
            if !path.is_absolute() {
                return Err(ApiError::bad(St3Error::new(
                    "invalid-resource-locator",
                    "a local file locator must be an absolute path",
                )));
            }
            for field in &request.fields {
                if !matches!(
                    field.as_str(),
                    "status" | "path" | "content_hash" | "size" | "mode" | "reason"
                ) {
                    return Err(ApiError::bad(St3Error::new(
                        "invalid-subscription-field",
                        format!("local file provider does not support field `{field}`"),
                    )));
                }
            }
            let hash = hex::encode(Sha256::digest(request.locator.as_bytes()));
            (
                format!("local-file/{}/{}", state.node, &hash[..24]),
                "filesystem.file",
            )
        }
        provider => {
            return Err(ApiError::bad(St3Error::new(
                "unsupported-capability",
                format!("resource provider `{provider}` is not registered"),
            )));
        }
    };
    let selected_fields = request
        .fields
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let stable = serde_json::to_vec(&json!({
        "provider": request.provider,
        "locator": request.locator,
        "fields": selected_fields,
        "target": target,
        "delivery": "message",
    }))
    .map_err(ApiError::internal)?;
    let subscription_hash = hex::encode(Sha256::digest(stable));
    let mission_id = format!(
        "resource-watch/{resource_name}/{}",
        &subscription_hash[..16]
    );
    let quote = |value: &str| serde_json::to_string(value).expect("a string serializes");
    let observer_fields = selected_fields
        .iter()
        .map(|field| format!("        field {}\n", quote(field)))
        .collect::<String>();
    let subscription_fields = selected_fields
        .iter()
        .map(|field| format!("        on {}\n", quote(field)))
        .collect::<String>();
    let kdl = format!(
        "version 2\nresource {} {{\n  kind {resource_kind:?}\n}}\nmission {} state=\"ready\" {{\n  goal \"Observe one resource and send its selected changes.\"\n  observer \"watch\" {{\n    resource {}\n    provider {}\n    locator {}\n{observer_fields}  }}\n  subscription \"watch\" {{\n    observer \"observer/watch\"\n    to {}\n{subscription_fields}    delivery \"message\"\n  }}\n}}\n",
        quote(&resource_name),
        quote(&mission_id),
        quote(&format!("resource/{resource_name}")),
        quote(&request.provider),
        quote(&request.locator),
        quote(&target),
    );
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let planned = state
        .store
        .mission(
            &intent,
            crate::model::IntentInput {
                kdl,
                source_name: Some("resource watch".into()),
            },
        )
        .map_err(ApiError::bad)?;
    state
        .store
        .apply(&intent, &planned.subject_tokens, &request.idempotency_key)
        .map_err(ApiError::bad)?;
    let run = state
        .store
        .create_mission_run(&MissionRunRequest {
            mission: mission_id,
            revision: None,
            workspace: ".".into(),
            requester: Some(target.clone()),
            mode: Some("run".into()),
            inputs: BTreeMap::new(),
            idempotency_key: format!("{}:run", request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(ResourceWatchView {
        resource: format!("resource/{resource_name}"),
        observer: format!("observer/{}/watch", run.id),
        subscription: format!("subscription/{}/watch", run.id),
    }))
}

async fn unwatch_resource(
    State(state): State<AppState>,
    AxumPath(subscription): AxumPath<String>,
    Json(request): Json<ResourceUnwatchRequest>,
) -> Result<Json<Value>, ApiError> {
    let subscription = format!(
        "subscription/{}",
        subscription
            .strip_prefix("subscription/")
            .unwrap_or(&subscription)
    );
    let run = state
        .store
        .desired_subjects()
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|subject| subject.subject == subscription)
        .and_then(|subject| subject.owner_run)
        .or_else(|| {
            subscription
                .strip_prefix("subscription/")
                .and_then(|value| value.split('/').next())
                .map(|run| format!("mission-run/{run}"))
        })
        .ok_or_else(|| {
            ApiError::not_found(format!("subscription `{subscription}` does not exist"))
        })?;
    let kdl = format!(
        "version 2\nmission-run {run:?} {{ cancellation \"resource-watch-stopped\" {{ reason \"the resource watch stopped\" }} }}\n"
    );
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let planned = state
        .store
        .mission(
            &intent,
            crate::model::IntentInput {
                kdl,
                source_name: Some("resource unwatch".into()),
            },
        )
        .map_err(ApiError::bad)?;
    state
        .store
        .apply(&intent, &planned.subject_tokens, &request.idempotency_key)
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(json!({
        "subscription": subscription,
        "status": "stopped",
        "actor": request.actor,
    })))
}

async fn refresh_resource(
    State(state): State<AppState>,
    AxumPath(resource): AxumPath<String>,
    Json(request): Json<crate::model::ResourceRefreshRequest>,
) -> Result<Json<crate::model::ResourceRefreshView>, ApiError> {
    if request.timeout_ms == 0 || request.timeout_ms > 3_600_000 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-refresh-timeout",
            "a resource refresh timeout must be between 1 ms and 1 hour",
        )));
    }
    let resource = if resource.starts_with("resource/") {
        resource
    } else {
        format!("resource/{resource}")
    };
    let observers = state
        .store
        .desired_subjects()
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|subject| subject.kind == "observer")
        .filter_map(|subject| {
            crate::graph::observer_spec(&subject.desired)
                .filter(|spec| !spec.stopped && spec.resource == resource)
                .map(|spec| (subject.subject, spec))
        })
        .collect::<Vec<_>>();
    if observers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "resource-not-observed",
            format!("resource `{resource}` has no active observer"),
        )));
    }
    let attempts = observers
        .iter()
        .map(|(observer, _)| {
            let stable = format!(
                "st3.resource-refresh.v1\0{}\0{observer}",
                request.idempotency_key
            );
            (observer.clone(), hex::encode(Sha256::digest(stable)))
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(completed) = completed_resource_refresh(&state.store, &resource, &attempts)? {
        return Ok(Json(completed));
    }
    for (observer, _) in &observers {
        let revision = state
            .store
            .selected_desired_revision(observer)
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::internal(format!("observer `{observer}` has no revision")))?;
        state
            .store
            .append_claim(&ClaimInput {
                subject: observer.clone(),
                kind: "observer.refresh-requested".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("revision".into(), Value::String(revision)),
                    ("attempt".into(), Value::String(attempts[observer].clone())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("{}:{observer}", request.idempotency_key)),
            })
            .map_err(ApiError::bad)?;
    }
    signal_changed(&state);
    let wait = async {
        let mut event_changed = state.event_notify.subscribe();
        loop {
            if let Some(completed) = completed_resource_refresh(&state.store, &resource, &attempts)?
            {
                return Ok(completed);
            }
            event_changed.changed().await.map_err(ApiError::internal)?;
        }
    };
    tokio::time::timeout(Duration::from_millis(request.timeout_ms), wait)
        .await
        .map_err(|_| {
            ApiError::bad(St3Error::new(
                "resource-refresh-timeout",
                format!("resource `{resource}` did not finish its refresh before the timeout"),
            ))
        })?
        .map(Json)
}

fn completed_resource_refresh(
    store: &Store,
    resource: &str,
    attempts: &BTreeMap<String, String>,
) -> Result<Option<crate::model::ResourceRefreshView>, ApiError> {
    let mut changed = false;
    let mut completed_at_index = 0;
    for (observer, attempt) in attempts {
        let completed = store
            .claims_for(observer, Some("observer.observed"))
            .map_err(ApiError::internal)?
            .into_iter()
            .rev()
            .find(|claim| {
                claim
                    .body
                    .pointer("/fields/attempt")
                    .and_then(Value::as_str)
                    == Some(attempt.as_str())
            });
        if let Some(completed) = completed {
            changed |= completed
                .body
                .pointer("/fields/changed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            completed_at_index = completed_at_index.max(completed.store_index);
            continue;
        }
        let failed = store
            .claims_for(observer, Some("observer.state"))
            .map_err(ApiError::internal)?
            .into_iter()
            .rev()
            .find(|claim| {
                claim
                    .body
                    .pointer("/fields/attempt")
                    .and_then(Value::as_str)
                    == Some(attempt.as_str())
                    && claim.body.pointer("/fields/state").and_then(Value::as_str)
                        == Some("unreachable")
            });
        if let Some(failed) = failed {
            let reason = failed
                .body
                .pointer("/fields/reason")
                .and_then(Value::as_str)
                .unwrap_or("the observer failed");
            return Err(ApiError::bad(St3Error::new(
                "resource-refresh-failed",
                format!("observer `{observer}` could not refresh `{resource}`: {reason}"),
            )));
        }
        return Ok(None);
    }
    Ok(Some(crate::model::ResourceRefreshView {
        resource: resource.into(),
        observers: attempts.keys().cloned().collect(),
        changed,
        completed_at_index,
    }))
}

#[derive(Deserialize)]
struct DocumentQuery {
    name: Option<String>,
}

async fn list_documents(
    State(state): State<AppState>,
    Query(query): Query<DocumentQuery>,
) -> Result<Json<Vec<DocumentVersion>>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.list_documents(query.name.as_deref()))
        .await
        .map(Json)
}

#[derive(Deserialize)]
struct DocumentContentQuery {
    reference: String,
}

#[derive(Serialize)]
struct DocumentContent {
    reference: String,
    bytes: Vec<u8>,
}

async fn get_document(
    State(state): State<AppState>,
    Query(query): Query<DocumentContentQuery>,
) -> Result<Json<DocumentContent>, ApiError> {
    let (name, hash) = query.reference.rsplit_once('@').ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "invalid-document-reference",
            "a document reference needs `@HASH`",
        ))
    })?;
    let store = state.store.clone();
    let name = name.to_owned();
    let hash = hash.to_owned();
    let bytes = blocking_store(move || store.get_document(&name, &hash))
        .await?
        .ok_or_else(|| {
            ApiError::not_found(format!("document `{}` is not stored", query.reference))
        })?;
    Ok(Json(DocumentContent {
        reference: query.reference,
        bytes,
    }))
}

async fn post_claim(
    State(state): State<AppState>,
    Json(request): Json<ClaimInput>,
) -> Result<Json<ClaimRecord>, ApiError> {
    let (response, appended) = state
        .store
        .append_client_claim_outcome(&request)
        .map_err(ApiError::bad)?;
    if appended {
        signal_changed(&state);
    }
    Ok(Json(response))
}

async fn get_claim(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ClaimRecord>, ApiError> {
    let store = state.store.clone();
    let id_for_read = id.clone();
    blocking_store(move || store.claim_by_id(&id_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("claim `{id}` does not exist")))
}

#[derive(Deserialize)]
struct ClaimsQuery {
    subject: Option<String>,
    owner_run: Option<String>,
    #[serde(default, alias = "after")]
    after_index: u64,
    #[serde(alias = "before")]
    before_index: Option<u64>,
    #[serde(default)]
    order: ClaimsOrder,
    #[serde(default = "default_claim_limit")]
    limit: usize,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ClaimsOrder {
    #[default]
    Asc,
    Desc,
}

fn default_claim_limit() -> usize {
    100
}

async fn list_claims(
    State(state): State<AppState>,
    Query(query): Query<ClaimsQuery>,
) -> Result<Json<ClaimsPage>, ApiError> {
    if query.limit == 0 || query.limit > 500 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-page-limit",
            "a claim page limit must be between 1 and 500",
        )));
    }
    let store = state.store.clone();
    blocking_store(move || {
        store.claims_page(
            query.subject.as_deref(),
            query.owner_run.as_deref(),
            query.after_index,
            query.before_index,
            matches!(query.order, ClaimsOrder::Desc),
            query.limit,
        )
    })
    .await
    .map(Json)
}

#[derive(Default, Deserialize)]
struct ReviewsQuery {
    reviewer: Option<String>,
}

async fn list_reviews(
    State(state): State<AppState>,
    Query(query): Query<ReviewsQuery>,
) -> Result<Json<Vec<HumanReviewView>>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.pending_human_reviews(query.reviewer.as_deref()))
        .await
        .map(Json)
}

#[derive(Default, Deserialize)]
struct AttentionQuery {
    person: Option<String>,
}

async fn list_attention(
    State(state): State<AppState>,
    Query(query): Query<AttentionQuery>,
) -> Result<Json<Vec<AttentionItemView>>, ApiError> {
    let person = query.person.map(|person| {
        if person.contains('/') {
            person
        } else {
            format!("person/{person}")
        }
    });
    if person
        .as_deref()
        .is_some_and(|person| !person.starts_with("person/"))
    {
        return Err(ApiError::bad(St3Error::new(
            "invalid-attention-person",
            "an attention filter must name a person subject",
        )));
    }
    let store = state.store.clone();
    blocking_store(move || store.attention_items(person.as_deref()))
        .await
        .map(Json)
}

async fn request_attention(
    State(state): State<AppState>,
    Json(request): Json<AttentionRequest>,
) -> Result<Json<AttentionRequestView>, ApiError> {
    let id = hex::encode(Sha256::digest(request.idempotency_key.as_bytes()));
    let subject = format!("attention/{}", &id[..32]);
    let response = state
        .store
        .request_attention(&subject, &request)
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn resolve_attention(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<AttentionResolveRequest>,
) -> Result<Json<AttentionRequestView>, ApiError> {
    let response = state
        .store
        .resolve_attention(&subject, &request)
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn post_review(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<ReviewRequest>,
) -> Result<Json<ClaimRecord>, ApiError> {
    if !matches!(request.decision.as_str(), "approved" | "rejected") {
        return Err(ApiError::bad(St3Error::new(
            "invalid-review-decision",
            "a review decision must be approved or rejected",
        )));
    }
    let subject = if subject.starts_with("resource/")
        || subject.starts_with("step-run/")
        || subject.starts_with("mission-run/")
    {
        subject
    } else {
        format!("step-run/{subject}")
    };
    let actor = request.actor.map(|actor| {
        if actor.contains('/') {
            actor
        } else {
            format!("person/{actor}")
        }
    });
    let review_request = if subject.starts_with("step-run/") || subject.starts_with("mission-run/")
    {
        let pending = state
            .store
            .pending_human_reviews(None)
            .map_err(ApiError::internal)?
            .into_iter()
            .find(|review| review.owner == subject)
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "review-not-requested",
                    format!("`{subject}` has no pending human review"),
                ))
            })?;
        let review_request = state
            .store
            .claim_by_id(&pending.request)
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::internal("the pending review request does not exist"))?;
        let reviewer = review_request
            .body
            .pointer("/fields/reviewer")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("a human review request has no reviewer"))?;
        if actor.as_deref() != Some(reviewer) {
            return Err(ApiError::bad(St3Error::new(
                "wrong-reviewer",
                format!("the pending review requires `{reviewer}`"),
            )));
        }
        Some(review_request)
    } else {
        None
    };
    let verdict = if request.decision == "approved" {
        "pass"
    } else {
        "fail"
    };
    let mut fields = BTreeMap::from([
        ("verdict".into(), Value::String(verdict.into())),
        (
            "reason".into(),
            request.reason.map(Value::String).unwrap_or(Value::Null),
        ),
    ]);
    if let Some(review_request) = &review_request {
        fields.insert("request".into(), Value::String(review_request.id.clone()));
    }
    let response = state
        .store
        .append_claim(&ClaimInput {
            subject: review_request
                .as_ref()
                .map(|request| request.subject.clone())
                .unwrap_or_else(|| subject.clone()),
            kind: "gate.result".into(),
            actor,
            fields,
            evidence: review_request
                .iter()
                .map(|request| request.id.clone())
                .collect(),
            expected_subject: request.expected_subject,
            idempotency_key: None,
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn send_message(
    State(state): State<AppState>,
    Json(request): Json<MessageSendRequest>,
) -> Result<Json<MessageView>, ApiError> {
    if request.content.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-message",
            "a message needs nonempty content",
        )));
    }
    if request.content.len() > 4096 && !request.content.starts_with("doc/") {
        return Err(ApiError::bad(St3Error::new(
            "message-too-large",
            "an inline message cannot exceed 4 KiB; post a document first",
        )));
    }
    if request.content.starts_with("doc/") {
        let (name, hash) = request.content.rsplit_once('@').ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "unpinned-document-reference",
                "a message document reference needs `doc/NAME@HASH`",
            ))
        })?;
        if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ApiError::bad(St3Error::new(
                "invalid-document-reference",
                "a message document reference has an invalid hash",
            )));
        }
        if state
            .store
            .get_document(name, hash)
            .map_err(ApiError::internal)?
            .is_none()
        {
            return Err(ApiError::bad(St3Error::new(
                "missing-document",
                format!("message document `{}` is not stored", request.content),
            )));
        }
    }
    let from = normalize_message_party(&request.from);
    let to = normalize_message_party(&request.to);
    let id = hex::encode(Sha256::digest(request.idempotency_key.as_bytes()))[..16].to_owned();
    let subject = format!("message/{id}");
    let fields = BTreeMap::from([
        ("from".into(), Value::String(from.clone())),
        ("to".into(), Value::String(to.clone())),
        ("content".into(), Value::String(request.content.clone())),
        ("status".into(), Value::String("sent".into())),
        (
            "title".into(),
            request
                .title
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "in_reply_to".into(),
            request
                .in_reply_to
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "tags".into(),
            Value::Array(request.tags.iter().cloned().map(Value::String).collect()),
        ),
    ]);
    let record = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "message.sent".into(),
            actor: Some(from.clone()),
            fields,
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(MessageView {
        subject,
        from,
        to,
        content: request.content,
        status: "sent".into(),
        title: request.title,
        in_reply_to: request.in_reply_to,
        tags: request.tags,
        created_index: record.store_index,
    }))
}

#[derive(Deserialize)]
struct MessagesQuery {
    to: Option<String>,
    #[serde(default)]
    include_closed: bool,
}

async fn list_messages(
    State(state): State<AppState>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<Vec<MessageView>>, ApiError> {
    let recipient = query.to.as_deref().map(normalize_message_party);
    let store = state.store.clone();
    blocking_store(move || store.messages(recipient.as_deref(), query.include_closed))
        .await
        .map(Json)
}

async fn read_message(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
) -> Result<Json<MessageView>, ApiError> {
    let subject = if subject.starts_with("message/") {
        subject
    } else {
        format!("message/{subject}")
    };
    let store = state.store.clone();
    blocking_store(move || store.messages(None, true))
        .await?
        .into_iter()
        .find(|message| message.subject == subject)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("message `{subject}` does not exist")))
}

async fn post_message_claim(
    State(state): State<AppState>,
    AxumPath(message_id): AxumPath<String>,
    Json(request): Json<MessageLifecycleRequest>,
) -> Result<Json<ClaimRecord>, ApiError> {
    let subject = message_subject(&message_id);
    let kind = match request.lifecycle.as_str() {
        "delivered" => "message.delivered",
        "read" => "message.read",
        "closed" => "message.closed",
        other => {
            return Err(ApiError::bad(St3Error::new(
                "invalid-message-lifecycle",
                format!("message lifecycle `{other}` is not registered"),
            )));
        }
    };
    let message = state
        .store
        .messages(None, true)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|message| message.subject == subject)
        .ok_or_else(|| ApiError::not_found(format!("message `{subject}` does not exist")))?;
    let actor = request.actor.ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-message-actor",
            "message lifecycle transitions require the recipient actor",
        ))
    })?;
    let actor = normalize_message_party(&actor);
    if actor != message.to {
        return Err(ApiError::bad(St3Error::new(
            "wrong-message-recipient",
            format!(
                "message `{subject}` belongs to `{}`, not `{actor}`",
                message.to
            ),
        )));
    }
    let record = state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: kind.into(),
            actor: Some(actor),
            fields: BTreeMap::from([("status".into(), Value::String(request.lifecycle))]),
            evidence: request.evidence,
            expected_subject: request.expected_subject,
            idempotency_key: Some(request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(record))
}

fn message_subject(value: &str) -> String {
    if value.starts_with("message/") {
        value.to_owned()
    } else {
        format!("message/{value}")
    }
}

#[derive(Deserialize)]
struct StatusQuery {
    subject: Option<String>,
    owner_run: Option<String>,
    at_index: Option<u64>,
    #[serde(default)]
    history: bool,
}

async fn status(
    State(state): State<AppState>,
    Query(query): Query<StatusQuery>,
) -> Result<Json<StatusResponse>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || {
        if query.history {
            store.status_history(
                query.subject.as_deref(),
                query.owner_run.as_deref(),
                query.at_index,
            )
        } else {
            store.status_at(
                query.subject.as_deref(),
                query.owner_run.as_deref(),
                query.at_index,
            )
        }
    })
    .await
    .map(Json)
}

async fn list_sessions(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::model::SubjectStatus>>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.terminal_statuses())
        .await
        .map(Json)
}

async fn reset_runtime(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<crate::model::RuntimeResetRequest>,
) -> Result<Json<crate::model::RuntimeResetView>, ApiError> {
    if request.reason.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "missing-reset-reason",
            "runtime reset needs a reason",
        )));
    }
    let status = state
        .store
        .status_at(Some(&subject), None, None)
        .map_err(ApiError::internal)?;
    let runtime = status
        .subjects
        .into_iter()
        .find(|item| item.subject == subject)
        .ok_or_else(|| ApiError::not_found(format!("runtime `{subject}` does not exist")))?;
    if !matches!(runtime.kind.as_deref(), Some("agent" | "exec" | "pty")) {
        return Err(ApiError::bad(St3Error::new(
            "not-a-runtime",
            format!("subject `{subject}` is not an agent, exec, or PTY runtime"),
        )));
    }
    let desired_token = runtime.desired_token.ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "runtime-not-desired",
            format!("runtime `{subject}` has no selected desired state"),
        ))
    })?;
    let subject_head = runtime.claims.last().cloned();
    let incarnation_id = runtime
        .actual
        .as_ref()
        .map(|actual| actual.get("fields").unwrap_or(actual))
        .and_then(|actual| actual.get("incarnation_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "runtime-not-incarnated",
                format!("runtime `{subject}` has no current incarnation"),
            ))
        })?;
    let fields = BTreeMap::from([
        ("desired_token".into(), Value::String(desired_token.clone())),
        (
            "incarnation_id".into(),
            Value::String(incarnation_id.clone()),
        ),
        ("reason".into(), Value::String(request.reason)),
    ]);
    let claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "runtime.restart-window-reset".into(),
            actor: None,
            fields,
            evidence: Vec::new(),
            expected_subject: Some(subject_head),
            idempotency_key: Some(request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(crate::model::RuntimeResetView {
        subject,
        desired_token,
        incarnation_id,
        reset_claim: claim.id,
    }))
}

#[derive(Deserialize)]
struct EventQuery {
    #[serde(default, alias = "after_index")]
    after: u64,
    subject: Option<String>,
    owner_run: Option<String>,
    wait: Option<bool>,
    timeout_ms: Option<u64>,
}

async fn events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<EventRecord>>, ApiError> {
    let read = |state: &AppState| {
        let store = state.store.clone();
        let subject = query.subject.clone();
        let owner_run = query.owner_run.clone();
        async move {
            blocking_store(move || {
                store.events_after_filtered(query.after, subject.as_deref(), owner_run.as_deref())
            })
            .await
        }
    };
    let current = read(&state).await?;
    if !current.is_empty() || query.wait == Some(false) {
        return Ok(Json(current));
    }
    let wait = async {
        let mut event_changed = state.event_notify.subscribe();
        loop {
            let current = read(&state).await?;
            if !current.is_empty() {
                return Ok(current);
            }
            event_changed.changed().await.map_err(ApiError::internal)?;
        }
    };
    let timeout_ms = query.timeout_ms.unwrap_or(30_000).clamp(10, 30_000);
    match tokio::time::timeout(Duration::from_millis(timeout_ms), wait).await {
        Ok(result) => result.map(Json),
        Err(_) => Ok(Json(Vec::new())),
    }
}

async fn quick_claude(
    State(state): State<AppState>,
    Json(request): Json<QuickAgentRequest>,
) -> Result<Json<QuickAgentResponse>, ApiError> {
    quick_agent(&state, request, "claude").await.map(Json)
}

async fn quick_codex(
    State(state): State<AppState>,
    Json(request): Json<QuickAgentRequest>,
) -> Result<Json<QuickAgentResponse>, ApiError> {
    quick_agent(&state, request, "codex").await.map(Json)
}

async fn quick_agent(
    state: &AppState,
    request: QuickAgentRequest,
    driver: &str,
) -> Result<QuickAgentResponse, ApiError> {
    let response_key = format!("quick-agent-response:{}", request.idempotency_key);
    if let Some(response) = state
        .store
        .cached_idempotency_response(&response_key)
        .map_err(ApiError::internal)?
    {
        return Ok(response);
    }
    let bus_id = request
        .subject
        .strip_prefix("agent/")
        .unwrap_or(&request.subject);
    let mut driver_body = String::new();
    if let Some(model) = &request.model {
        driver_body.push_str(&format!("model {model:?}\n"));
    }
    if let Some(effort) = &request.effort {
        driver_body.push_str(&format!("effort {effort:?}\n"));
    }
    if driver == "claude" {
        driver_body.push_str("dev-channels #true\n");
    }
    if !request.arguments.is_empty() {
        driver_body.push_str("args");
        for argument in &request.arguments {
            driver_body.push_str(&format!(" {argument:?}"));
        }
        driver_body.push('\n');
    }
    if let Some(prompt) = &request.prompt {
        driver_body.push_str(&format!("prompt {prompt:?}\n"));
    }
    let mission_id = format!("standing/{bus_id}");
    let kdl = format!(
        "version 2\nmission {mission_id:?} state=\"ready\" {{\n  goal \"Keep the agent ready for work and conversation.\"\n  agent {bus_id:?} {{\n    identity {bus_id:?}\n    workspace {:?}\n    harness {driver:?} {{\n{driver_body}    }}\n  }}\n}}\n",
        request.worktree
    );
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let mission =
        intent.missions.get(&mission_id).cloned().ok_or_else(|| {
            ApiError::internal("quick agent normalization lost its standing mission")
        })?;
    let planned = state
        .store
        .mission(
            &intent,
            crate::model::IntentInput {
                kdl: kdl.clone(),
                source_name: Some(format!("quick {driver}")),
            },
        )
        .map_err(ApiError::bad)?;
    state
        .store
        .apply(&intent, &planned.subject_tokens, &request.idempotency_key)
        .map_err(ApiError::bad)?;
    let mut active = state
        .store
        .active_mission_runs()
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|run| run.mission == format!("mission/{mission_id}"))
        .collect::<Vec<_>>();
    if active.len() > 1 {
        return Err(ApiError::bad(St3Error::new(
            "conflicting-standing-runs",
            format!("standing mission `mission/{mission_id}` has more than one active run"),
        )));
    }
    let run = if let Some(current) = active.pop() {
        if current.revision == mission.revision {
            current
        } else {
            state
                .store
                .adopt_mission_revision(
                    &current.subject,
                    &mission,
                    "person/requester",
                    "the quick agent configuration changed",
                    &format!("{}:standing-revision", request.idempotency_key),
                )
                .map_err(ApiError::bad)?
        }
    } else {
        state
            .store
            .create_mission_run(&crate::model::MissionRunRequest {
                mission: mission_id.clone(),
                revision: None,
                workspace: request.worktree.clone(),
                requester: Some("person/requester".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: format!("{}:standing-run", request.idempotency_key),
            })
            .map_err(ApiError::bad)?
    };
    let agent_subject = format!("agent/{}/{bus_id}", run.id);
    let actual_agent_token = state
        .store
        .selected_desired_token(&agent_subject)
        .map_err(ApiError::internal)?
        .into_iter()
        .collect::<Vec<_>>();
    if actual_agent_token != request.expected_subject {
        return Err(ApiError::bad(St3Error::new(
            "stale-subject",
            format!("the desired state for `{agent_subject}` changed"),
        )));
    }
    signal_changed(state);
    let runtime_id = format!("{}.{}", run.id.replace('/', "."), bus_id.replace('/', "."));
    let harness = state
        .store
        .current_harness(&agent_subject)
        .map_err(ApiError::internal)?;
    let ready = harness
        .as_ref()
        .is_some_and(crate::model::CurrentHarnessView::is_ready);
    let response = QuickAgentResponse {
        subject: agent_subject,
        mission: format!("mission/{mission_id}"),
        mission_run: run.subject,
        generation: run.generation,
        runtime_id,
        event_cursor: state.store.index().map_err(ApiError::internal)?,
        incarnation_id: harness.map(|harness| harness.incarnation_id),
        ready,
    };
    state
        .store
        .cache_idempotency_response(&response_key, &response)
        .map_err(ApiError::internal)?;
    Ok(response)
}

async fn start_eval(
    State(state): State<AppState>,
    Json(request): Json<EvalStartRequest>,
) -> Result<Json<EvalStartResponse>, ApiError> {
    let actual_hash = hex::encode(Sha256::digest(&request.bundle));
    if actual_hash != request.bundle_hash {
        return Err(ApiError::bad(St3Error::new(
            "eval-bundle-hash-mismatch",
            "the eval bundle bytes do not match the supplied hash",
        )));
    }
    state
        .store
        .put_blob(&request.bundle)
        .map_err(ApiError::internal)?;
    let nonce = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let workspace = state
        .state_dir
        .join("evals")
        .join(&request.bundle_hash[..16])
        .join(nonce.to_string());
    hydrate_eval(&request.bundle, &workspace).map_err(ApiError::internal)?;
    let eval_file = workspace.join("eval.kdl");
    let kdl = fs::read_to_string(&eval_file)
        .map_err(|error| ApiError::internal(format!("read {}: {error}", eval_file.display())))?;
    let kdl = kdl.replace("${EVAL_ROOT}", &workspace.to_string_lossy());
    let mut intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let ready = intent
        .missions
        .values()
        .filter(|mission| mission.state == crate::model::MissionState::Ready)
        .collect::<Vec<_>>();
    let named_entry = format!("eval/{}", request.name);
    let entry = ready
        .iter()
        .copied()
        .find(|mission| mission.id == named_entry)
        .or_else(|| (ready.len() == 1).then_some(ready[0]))
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "invalid-eval-mission-count",
                format!(
                    "an eval with helper missions needs one ready entry mission named `{named_entry}`"
                ),
            ))
        })?;
    let timeout_ms = entry.timeout_ms.ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-eval-timeout",
            format!("eval entry mission `{}` needs a timeout", entry.id),
        ))
    })?;
    if timeout_ms > MAX_EVAL_TIMEOUT_MS {
        return Err(ApiError::bad(St3Error::new(
            "eval-timeout-too-large",
            format!(
                "eval entry mission `{}` timeout exceeds the 20 minute limit",
                entry.id
            ),
        )));
    }
    let entry_id = entry.id.clone();
    let entry_revision = entry.revision.clone();
    let idempotency_key = format!("eval:{}:{}:{nonce}", request.name, request.bundle_hash);
    let owner_run = state
        .store
        .mission_run_subject_for_idempotency_key(&idempotency_key);
    scope_eval_desired_subjects(
        &mut intent,
        &state.store.desired_subjects().map_err(ApiError::internal)?,
        &owner_run,
    )
    .map_err(ApiError::bad)?;
    stage_eval_documents(&state, &workspace, &intent).map_err(ApiError::bad)?;
    let mission = state
        .store
        .mission(
            &intent,
            crate::model::IntentInput {
                kdl: kdl.clone(),
                source_name: Some(eval_file.display().to_string()),
            },
        )
        .map_err(ApiError::bad)?;
    if !mission.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "eval-blocked",
            mission.blockers.join("; "),
        )));
    }
    let applied = state
        .store
        .apply(
            &intent,
            &mission.subject_tokens,
            &format!(
                "eval:{}:{}:{nonce}:apply",
                request.name, request.bundle_hash
            ),
        )
        .map_err(ApiError::bad)?;
    let run = match state.store.create_mission_run(&MissionRunRequest {
        mission: entry_id,
        revision: Some(entry_revision),
        workspace: workspace.to_string_lossy().into_owned(),
        requester: Some("person/eval-requester".into()),
        mode: Some("eval".into()),
        inputs: request.inputs,
        idempotency_key,
    }) {
        Ok(run) => run,
        Err(error) => {
            state
                .store
                .discard_desired_owned_by(&owner_run)
                .map_err(ApiError::internal)?;
            return Err(ApiError::bad(error));
        }
    };
    signal_changed(&state);
    Ok(Json(EvalStartResponse {
        event_cursor: applied
            .store_index
            .max(state.store.index().map_err(ApiError::internal)?),
        mission_run: run.subject,
    }))
}

fn scope_eval_desired_subjects(
    intent: &mut crate::model::NormalizedIntent,
    current: &[crate::model::DesiredSubject],
    owner_run: &str,
) -> Result<(), St3Error> {
    let selected = current
        .iter()
        .map(|subject| (subject.subject.as_str(), subject))
        .collect::<BTreeMap<_, _>>();
    let mut shared = Vec::new();
    for (subject, desired) in &mut intent.subjects {
        let Some(current) = selected.get(subject.as_str()) else {
            desired.owner_run = Some(owner_run.to_owned());
            continue;
        };
        if *current == desired {
            shared.push(subject.clone());
            continue;
        }
        return Err(St3Error::new(
            "eval-desired-conflict",
            format!(
                "eval declaration `{subject}` conflicts with the selected desired state; use a run-owned declaration or an isolated daemon"
            ),
        ));
    }
    for subject in shared {
        intent.subjects.remove(&subject);
    }
    Ok(())
}

async fn get_eval(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
) -> Result<Json<EvalStatus>, ApiError> {
    let store = state.store.clone();
    let run_for_read = run.clone();
    let run = blocking_store(move || store.mission_run(&run_for_read))
        .await?
        .ok_or_else(|| ApiError::not_found(format!("eval mission run `{run}` does not exist")))?;
    let store = state.store.clone();
    let run_subject = run.subject.clone();
    let verdict_claim =
        blocking_store(move || store.latest_claim(&run_subject, Some("eval.verdict"))).await?;
    let verdict = verdict_claim
        .as_ref()
        .and_then(|claim| claim.body.pointer("/fields/verdict"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let cleanup = if run.phase == "terminal" && verdict.is_some() {
        "complete"
    } else if run.phase == "final"
        || run.phase == "final-cancelled"
        || run.phase.starts_with("cleanup-")
    {
        "cleaning"
    } else {
        "pending"
    };
    let active_steps = run
        .steps
        .iter()
        .filter(|step| {
            matches!(
                step.status.as_str(),
                "ready" | "claimed" | "working" | "verifying" | "blocked"
            )
        })
        .map(|step| step.step.clone())
        .collect();
    Ok(Json(EvalStatus {
        mission_run: run.subject,
        lifecycle: run.status,
        phase: run.phase,
        active_steps,
        verdict,
        cleanup: cleanup.into(),
        store_index: state.store.index().map_err(ApiError::internal)?,
    }))
}

async fn start_mission_run(
    State(state): State<AppState>,
    Json(request): Json<MissionRunRequest>,
) -> Result<Json<MissionRunView>, ApiError> {
    let requester = request.requester.as_deref().unwrap_or("person/requester");
    require_agent_mission_authority(&state, requester, "start", &request.mission)?;
    let response = state
        .store
        .create_mission_run(&request)
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

#[derive(Deserialize)]
struct MissionRunQuery {
    root: Option<String>,
    mission: Option<String>,
}

async fn list_mission_runs(
    State(state): State<AppState>,
    Query(query): Query<MissionRunQuery>,
) -> Result<Json<Vec<MissionRunView>>, ApiError> {
    let store = state.store.clone();
    match (query.root, query.mission) {
        (Some(root), None) => blocking_store(move || store.mission_runs_for_root(&root))
            .await
            .map(Json),
        (None, Some(mission)) => {
            blocking_store(move || store.active_mission_runs_for_mission(&mission))
                .await
                .map(Json)
        }
        _ => Err(ApiError::bad(St3Error::new(
            "invalid-mission-run-query",
            "select exactly one mission or root mission run",
        ))),
    }
}

async fn get_mission_run(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
) -> Result<Json<MissionRunView>, ApiError> {
    let store = state.store.clone();
    let run_for_read = run.clone();
    blocking_store(move || store.mission_run(&run_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("mission run `{run}` does not exist")))
}

async fn revise_mission_run(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
    Json(request): Json<MissionRevisionRequest>,
) -> Result<Json<RevisionSubmissionView>, ApiError> {
    if let Some(cached) = cached_revision_submission(
        &state,
        &format!("{}:adopt", request.idempotency_key),
        &format!("{}:propose", request.idempotency_key),
    )? {
        return Ok(Json(cached));
    }
    let current = state
        .store
        .mission_run(&run)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("mission run `{run}` does not exist")))?;
    let initial = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    if initial.missions.len() != 1 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-mission-revision-intent",
            "a run revision must contain exactly one mission",
        )));
    }
    if !initial.subjects.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "invalid-mission-revision-intent",
            "a run revision can contain only its mission",
        )));
    }
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, None)
        .map_err(ApiError::internal)?;
    let resolved_kdl =
        resolve_document_references(&request.intent.kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let replacement = intent
        .missions
        .values()
        .next()
        .expect("one mission was checked");
    let mission_id = current
        .mission
        .strip_prefix("mission/")
        .unwrap_or(&current.mission);
    if replacement.id != mission_id {
        return Err(ApiError::bad(St3Error::new(
            "wrong-mission-revision",
            format!(
                "revision `{}` does not replace mission `{mission_id}`",
                replacement.id
            ),
        )));
    }
    let old = state
        .store
        .mission_spec(mission_id, Some(&current.revision))
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-mission-revision",
                "the current mission revision is unavailable",
            ))
        })?;
    if old.inputs != replacement.inputs {
        return Err(ApiError::bad(St3Error::new(
            "run-input-mutation",
            "a run revision cannot change its input declarations",
        )));
    }
    let actor = if request.actor.contains('/') {
        request.actor.clone()
    } else {
        format!("agent/{}", request.actor)
    };
    require_agent_mission_authority(&state, &actor, "revise", mission_id)?;
    let (_, reviewers) = crate::store::analyze_mission_revision(
        &old,
        replacement,
        &actor,
        &current.requester,
        &crate::store::mission_run_variables(&current, &replacement.revision),
    )
    .map_err(ApiError::bad)?;
    let mut publication = intent.clone();
    publication.subjects.clear();
    let planned = state
        .store
        .mission(
            &publication,
            crate::model::IntentInput {
                kdl: resolved_kdl,
                source_name: request.intent.source_name,
            },
        )
        .map_err(ApiError::bad)?;
    if !planned.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "mission-revision-blocked",
            planned.blockers.join("; "),
        )));
    }
    state
        .store
        .apply(
            &publication,
            &planned.subject_tokens,
            &format!("{}:publish", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    let revised =
        if reviewers.is_empty() && matches!(old.revision_cutover, RevisionCutover::RestartActive) {
            let mission_run = state
                .store
                .adopt_mission_revision(
                    &run,
                    replacement,
                    &actor,
                    &request.reason,
                    &format!("{}:adopt", request.idempotency_key),
                )
                .map_err(ApiError::bad)?;
            RevisionSubmissionView {
                status: "applied".into(),
                mission_run,
                proposal: None,
            }
        } else {
            let proposal = state
                .store
                .create_revision_proposal(
                    &run,
                    replacement,
                    &actor,
                    &request.reason,
                    &format!("{}:propose", request.idempotency_key),
                )
                .map_err(ApiError::bad)?;
            RevisionSubmissionView {
                status: proposal.status.clone(),
                mission_run: state
                    .store
                    .mission_run(&run)
                    .map_err(ApiError::internal)?
                    .expect("the revised mission run exists"),
                proposal: Some(proposal),
            }
        };
    signal_changed(&state);
    Ok(Json(revised))
}

fn cached_revision_submission(
    state: &AppState,
    direct_key: &str,
    proposal_key: &str,
) -> Result<Option<RevisionSubmissionView>, ApiError> {
    if let Some(mission_run) = state
        .store
        .cached_idempotency_response::<MissionRunView>(direct_key)
        .map_err(ApiError::internal)?
    {
        return Ok(Some(RevisionSubmissionView {
            status: "applied".into(),
            mission_run,
            proposal: None,
        }));
    }
    let Some(cached) = state
        .store
        .cached_idempotency_response::<RevisionProposalView>(proposal_key)
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    let proposal = state
        .store
        .revision_proposal(&cached.id)
        .map_err(ApiError::internal)?
        .unwrap_or(cached);
    let mission_run = state
        .store
        .mission_run(&proposal.run)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::internal("the proposal mission run is unavailable"))?;
    Ok(Some(RevisionSubmissionView {
        status: proposal.status.clone(),
        mission_run,
        proposal: Some(proposal),
    }))
}

async fn list_run_generations(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
) -> Result<Json<Vec<RunGenerationView>>, ApiError> {
    let store = state.store.clone();
    blocking_store(move || store.run_generations(&run))
        .await
        .map(Json)
}

async fn get_run_generation(
    State(state): State<AppState>,
    AxumPath(generation): AxumPath<String>,
) -> Result<Json<RunGenerationView>, ApiError> {
    let store = state.store.clone();
    let generation_for_read = generation.clone();
    blocking_store(move || store.run_generation(&generation_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("run generation `{generation}` does not exist")))
}

async fn get_run_revision_proposal(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
) -> Result<Json<RevisionProposalView>, ApiError> {
    let store = state.store.clone();
    let run_for_read = run.clone();
    blocking_store(move || store.revision_proposal_for_run(&run_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "mission run `{run}` has no pending revision proposal"
            ))
        })
}

async fn get_revision_proposal(
    State(state): State<AppState>,
    AxumPath(proposal): AxumPath<String>,
) -> Result<Json<RevisionProposalView>, ApiError> {
    let store = state.store.clone();
    let proposal_for_read = proposal.clone();
    blocking_store(move || store.revision_proposal(&proposal_for_read))
        .await?
        .map(Json)
        .ok_or_else(|| {
            ApiError::not_found(format!("revision proposal `{proposal}` does not exist"))
        })
}

async fn approve_revision_proposal(
    State(state): State<AppState>,
    AxumPath(proposal): AxumPath<String>,
    Json(request): Json<RevisionApprovalRequest>,
) -> Result<Json<RevisionSubmissionView>, ApiError> {
    let result = state
        .store
        .approve_revision_proposal(
            &proposal,
            &request.actor,
            &request.preview_hash,
            &request.idempotency_key,
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(result))
}

async fn cancel_revision_proposal(
    State(state): State<AppState>,
    AxumPath(proposal): AxumPath<String>,
    Json(request): Json<RevisionCancelRequest>,
) -> Result<Json<RevisionProposalView>, ApiError> {
    let result = state
        .store
        .cancel_revision_proposal(
            &proposal,
            &request.actor,
            request.reason.as_deref(),
            &request.idempotency_key,
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(result))
}

#[derive(Deserialize)]
struct WorkQuery {
    actor: Option<String>,
    #[serde(default)]
    include_terminal: bool,
}

async fn list_work(
    State(state): State<AppState>,
    Query(query): Query<WorkQuery>,
) -> Result<Json<Vec<StepRunView>>, ApiError> {
    let store = state.store.clone();
    let (mut work, desired) = blocking_store(move || {
        Ok((
            store.work(query.actor.as_deref(), query.include_terminal)?,
            store.desired_subjects()?,
        ))
    })
    .await?;
    let agents = desired
        .into_iter()
        .filter(|subject| subject.kind == "agent")
        .map(|subject| (subject.subject, crate::graph::agent_under(&subject.desired)))
        .collect::<BTreeMap<_, _>>();
    for step in &mut work {
        if let Some(actor) = step
            .claimant
            .as_ref()
            .or(step.assigned_to.as_ref())
            .or_else(|| (step.available_to.len() == 1).then(|| &step.available_to[0]))
        {
            step.under = agents.get(actor).cloned().unwrap_or_default();
        }
    }
    Ok(Json(work))
}

async fn wake_work(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<WorkWakeRequest>,
) -> Result<Json<MessageView>, ApiError> {
    if request.reason.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "missing-wake-reason",
            "a manual work wake needs a reason",
        )));
    }
    if let Some(existing) = state
        .store
        .operation_claim(&request.idempotency_key)
        .map_err(ApiError::internal)?
    {
        let message = state
            .store
            .messages(None, true)
            .map_err(ApiError::internal)?
            .into_iter()
            .find(|message| message.subject == existing.subject)
            .ok_or_else(|| ApiError::internal("the wake operation message is unavailable"))?;
        return Ok(Json(message));
    }
    let step = state
        .store
        .step_run(&subject)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("step run `{subject}` does not exist")))?;
    if step.status != "ready" {
        return Err(ApiError::bad(St3Error::new(
            "work-not-ready",
            format!(
                "step run `{}` is `{}`, not ready",
                step.subject, step.status
            ),
        )));
    }
    let agent = step.assigned_to.as_deref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "work-has-no-assignee",
            format!("step run `{}` has no exact assignee to wake", step.subject),
        ))
    })?;
    let harness = state
        .store
        .current_harness(agent)
        .map_err(ApiError::internal)?
        .filter(crate::model::CurrentHarnessView::is_ready)
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "assignee-harness-not-ready",
                format!("assignee `{agent}` has no ready current harness"),
            ))
        })?;
    let attempt = step
        .wake
        .as_ref()
        .map(|wake| wake.attempts)
        .unwrap_or_default()
        .saturating_add(1);
    let message = crate::reconcile::append_work_wake_message(
        &state.store,
        &step,
        agent,
        &harness.incarnation_id,
        attempt,
        "manual",
        &request.actor,
        &request.reason,
        request.idempotency_key,
    )
    .map_err(ApiError::internal)?;
    signal_changed(&state);
    Ok(Json(message))
}

async fn publish_work_mission(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<MissionProductionRequest>,
) -> Result<Json<MissionOutputView>, ApiError> {
    let step = state
        .store
        .step_run(&subject)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("step run `{subject}` does not exist")))?;
    let actor = if request.actor.contains('/') {
        request.actor.clone()
    } else {
        format!("agent/{}", request.actor)
    };
    if !state
        .store
        .mission_output_authorized(&step.subject, &actor, request.incarnation.as_deref())
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::bad(St3Error::new(
            "work-not-claimed",
            format!(
                "`{actor}` does not hold the mission-producing step `{}` or its nested work",
                step.subject
            ),
        )));
    }
    let run = state
        .store
        .mission_run(&step.run)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("mission run `{}` does not exist", step.run)))?;
    let root = state
        .store
        .mission_spec(
            run.mission.strip_prefix("mission/").unwrap_or(&run.mission),
            Some(&run.revision),
        )
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-mission-revision",
                "the running mission revision is unavailable",
            ))
        })?;
    let definition = crate::mission::find_step(&root, &step.step).ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-step-definition",
            format!("step `{}` is absent from its mission revision", step.step),
        ))
    })?;
    let expected_mission = definition.produces_mission.as_deref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "step-does-not-produce-mission",
            format!("step `{}` does not declare produces-mission", step.step),
        ))
    })?;
    require_agent_mission_authority(&state, &actor, "publish", expected_mission)?;

    let initial = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    if initial.missions.len() != 1 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-mission-output-intent",
            "a mission output must contain exactly one mission",
        )));
    }
    if !initial.subjects.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "invalid-mission-output-intent",
            "a mission output can contain only its mission",
        )));
    }
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, None)
        .map_err(ApiError::internal)?;
    let resolved_kdl =
        resolve_document_references(&request.intent.kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let mission = intent
        .missions
        .values()
        .next()
        .expect("one mission was checked");
    if mission.id != expected_mission || mission.state != crate::model::MissionState::Ready {
        return Err(ApiError::bad(St3Error::new(
            "wrong-mission-output",
            format!(
                "step `{}` must publish ready mission `{expected_mission}`",
                step.step
            ),
        )));
    }
    let mut publication = intent.clone();
    publication.subjects.clear();
    let planned = state
        .store
        .mission(
            &publication,
            crate::model::IntentInput {
                kdl: resolved_kdl,
                source_name: request.intent.source_name,
            },
        )
        .map_err(ApiError::bad)?;
    if !planned.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "mission-output-blocked",
            planned.blockers.join("; "),
        )));
    }
    state
        .store
        .apply(
            &publication,
            &planned.subject_tokens,
            &format!("{}:publish", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    let output = state
        .store
        .record_mission_output(
            &step.subject,
            &actor,
            request.incarnation.as_deref(),
            expected_mission,
            mission,
            &format!("{}:bind", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(output))
}

fn require_agent_mission_authority(
    state: &AppState,
    actor: &str,
    action: &str,
    mission: &str,
) -> Result<(), ApiError> {
    let Some(actor) = normalized_agent_actor(actor) else {
        return Ok(());
    };
    let mission = mission.strip_prefix("mission/").unwrap_or(mission);
    let desired = state
        .store
        .desired_subjects()
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|desired| desired.kind == "agent" && desired.subject == actor)
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-agent-mission-authority",
                format!("`{actor}` has no current desired agent declaration"),
            ))
        })?;
    let authority = crate::graph::agent_mission_authority(&desired.desired);
    if authority.allows(action, mission) {
        Ok(())
    } else {
        Err(ApiError::bad(St3Error::new(
            "mission-authority-denied",
            format!("`{actor}` cannot {action} mission `{mission}`"),
        )))
    }
}

fn normalized_agent_actor(actor: &str) -> Option<String> {
    if actor.starts_with("person/") || actor.starts_with("daemon/") || actor.starts_with("system/")
    {
        None
    } else if actor.starts_with("agent/") {
        Some(actor.to_owned())
    } else {
        Some(format!("agent/{actor}"))
    }
}

async fn post_work_action(
    State(state): State<AppState>,
    AxumPath((action, subject)): AxumPath<(String, String)>,
    Json(request): Json<WorkRequest>,
) -> Result<Json<StepRunView>, ApiError> {
    let store = state.store.clone();
    let (mut response, desired) = blocking_action(move || {
        let response = store.work_action(&subject, &action, &request)?;
        let desired = store.desired_subjects().map_err(|error| {
            St3Error::new("store-read-failed", format!("read desired agents: {error}"))
        })?;
        Ok((response, desired))
    })
    .await?;
    if let Some(assignee) = response
        .claimant
        .as_ref()
        .or(response.assigned_to.as_ref())
        .or_else(|| (response.available_to.len() == 1).then(|| &response.available_to[0]))
    {
        response.under = desired
            .into_iter()
            .filter(|subject| subject.kind == "agent")
            .map(|subject| (subject.subject, crate::graph::agent_under(&subject.desired)))
            .collect::<BTreeMap<_, _>>()
            .get(assignee)
            .cloned()
            .unwrap_or_default();
    }
    signal_changed(&state);
    Ok(Json(response))
}

fn stage_eval_documents(
    state: &AppState,
    workspace: &Path,
    intent: &crate::model::NormalizedIntent,
) -> Result<(), St3Error> {
    for reference in &intent.document_refs {
        let (name, hash) = reference.rsplit_once('@').ok_or_else(|| {
            St3Error::new(
                "invalid-document-reference",
                format!("eval document reference `{reference}` has no hash"),
            )
        })?;
        if state
            .store
            .get_document(name, hash)
            .map_err(|error| St3Error::new("internal", error.to_string()))?
            .is_some()
        {
            continue;
        }
        let path = workspace.join(".st3-documents").join(hash);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            St3Error::new(
                "missing-eval-document",
                format!(
                    "eval document `{reference}` is absent from the store and {} is not staged: {error}",
                    path.display()
                ),
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(St3Error::new(
                "invalid-eval-document",
                format!(
                    "staged eval document {} is not a regular file",
                    path.display()
                ),
            ));
        }
        let bytes = fs::read(&path).map_err(|error| {
            St3Error::new(
                "invalid-eval-document",
                format!("read staged eval document {}: {error}", path.display()),
            )
        })?;
        if hex::encode(Sha256::digest(&bytes)) != hash {
            return Err(St3Error::new(
                "eval-document-hash-mismatch",
                format!(
                    "staged eval document {} does not match `{reference}`",
                    path.display()
                ),
            ));
        }
        let expected = state
            .store
            .latest_document_token(name)
            .map_err(|error| St3Error::new("internal", error.to_string()))?;
        state.store.put_document(
            name,
            &bytes,
            &expected,
            &format!("eval-document:{name}:{hash}"),
        )?;
    }
    Ok(())
}

struct LiveSession {
    runtime_id: String,
    incarnation_id: String,
    terminal: bool,
    driver: Option<String>,
}

fn live_session(
    state: &AppState,
    subject: &str,
    expected_incarnation: Option<&str>,
) -> Result<LiveSession, ApiError> {
    let status = state
        .store
        .status(Some(subject))
        .map_err(ApiError::internal)?;
    let actual = status
        .subjects
        .first()
        .and_then(|item| item.actual.as_ref())
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no live session")))?;
    let fields = actual.get("fields").unwrap_or(actual);
    let runtime_id = fields
        .get("runtime_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no runtime")))?;
    let incarnation_id = fields
        .get("incarnation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no incarnation")))?;
    if expected_incarnation.is_some_and(|expected| incarnation_id != expected) {
        return Err(ApiError::bad(St3Error::new(
            "stale-incarnation",
            format!("subject `{subject}` changed incarnation"),
        )));
    }
    let member = state
        .store
        .desired_subjects()
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|desired| desired.subject == subject)
        .and_then(|desired| desired.member);
    let terminal = member
        .as_ref()
        .map(|member| member.terminal)
        .or_else(|| fields.get("terminal").and_then(Value::as_bool))
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no runtime kind")))?;
    Ok(LiveSession {
        runtime_id: runtime_id.into(),
        incarnation_id: incarnation_id.into(),
        terminal,
        driver: member.and_then(|member| member.driver),
    })
}

#[derive(Deserialize)]
struct SessionLogQuery {
    #[serde(default)]
    after: u64,
    #[serde(default = "default_log_limit")]
    limit: usize,
    #[serde(default)]
    previous: bool,
    #[serde(default)]
    wait: bool,
}

fn default_log_limit() -> usize {
    64 * 1024
}

async fn logs_session(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Query(query): Query<SessionLogQuery>,
) -> Result<Json<SessionLogChunk>, ApiError> {
    if query.limit == 0 || query.limit > 64 * 1024 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-log-limit",
            "a log chunk limit must be between 1 and 65536 bytes",
        )));
    }
    let session = live_session(&state, &subject, None)?;
    if session.terminal {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "terminal sessions expose a screen instead of an exec log",
        )));
    }
    let runtime =
        st_runtime::ExecRuntime::new(state.state_dir.join("exec"), state.state_dir.join("logs"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut generation = if query.previous {
            runtime
                .previous_generation(&session.runtime_id)
                .map_err(ApiError::internal)?
        } else {
            match runtime
                .observe(&session.runtime_id)
                .map_err(ApiError::internal)?
            {
                Some(st_runtime::ExecObservation::Running(generation))
                | Some(st_runtime::ExecObservation::Exited(generation)) => Some(generation),
                Some(st_runtime::ExecObservation::Indeterminate(reason)) => {
                    return Err(ApiError::internal(reason));
                }
                None => None,
            }
        }
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "subject `{subject}` has no {}exec log generation",
                if query.previous { "previous " } else { "" }
            ))
        })?;
        if !query.previous && generation.generation_id != session.incarnation_id {
            return Err(ApiError::bad(St3Error::new(
                "stale-incarnation",
                format!("subject `{subject}` changed exec generation"),
            )));
        }
        let log = runtime
            .read_log_bytes(&session.runtime_id, query.previous)
            .map_err(ApiError::internal)?
            .unwrap_or_default();
        let start = usize::try_from(query.after)
            .unwrap_or(usize::MAX)
            .min(log.len());
        let end = start.saturating_add(query.limit).min(log.len());
        let running = if query.previous {
            false
        } else {
            match runtime
                .observe(&session.runtime_id)
                .map_err(ApiError::internal)?
            {
                Some(st_runtime::ExecObservation::Running(latest)) => {
                    generation = latest;
                    true
                }
                Some(st_runtime::ExecObservation::Exited(latest)) => {
                    generation = latest;
                    false
                }
                Some(st_runtime::ExecObservation::Indeterminate(reason)) => {
                    return Err(ApiError::internal(reason));
                }
                None => false,
            }
        };
        if end > start || !query.wait || !running || tokio::time::Instant::now() >= deadline {
            return Ok(Json(SessionLogChunk {
                subject,
                runtime_id: session.runtime_id,
                generation_id: generation.generation_id,
                previous: query.previous,
                start_offset: start as u64,
                next_offset: end as u64,
                data_base64: base64::engine::general_purpose::STANDARD.encode(&log[start..end]),
                eof: end == log.len() && !running,
                status: if running { "running" } else { "exited" }.into(),
                exit_code: generation.exit_code,
                exit_signal: generation.exit_signal,
            }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn screen_session(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
) -> Result<Json<SessionScreen>, ApiError> {
    let session = live_session(&state, &subject, None)?;
    if !session.terminal {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "an exec session has a log instead of a terminal screen",
        )));
    }
    let screen = st_runtime::PtyRuntime::new(state.pty_root.clone())
        .with_binary(state.pty_binary.to_string_lossy())
        .screen(&session.runtime_id)
        .map_err(ApiError::internal)?;
    Ok(Json(SessionScreen {
        subject,
        runtime_id: session.runtime_id,
        incarnation_id: session.incarnation_id,
        screen,
    }))
}

async fn input_session(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<SessionInputRequest>,
) -> Result<Json<SessionControlResponse>, ApiError> {
    let result_key = format!(
        "session-control-result:input:{subject}:{}",
        request.idempotency_key
    );
    if let Some(result) = state
        .store
        .idempotent_claim(&result_key)
        .map_err(ApiError::internal)?
    {
        return Ok(Json(session_control_response(&subject, &result)));
    }
    let session = live_session(&state, &subject, Some(&request.expected_incarnation))?;
    if !session.terminal {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "session input requires a terminal session",
        )));
    }
    let bytes = match request.mode {
        SessionInputMode::Raw => base64::engine::general_purpose::STANDARD
            .decode(&request.value)
            .map_err(|error| {
                ApiError::bad(St3Error::new(
                    "invalid-session-input",
                    format!("raw terminal input is not valid base64: {error}"),
                ))
            })?,
        SessionInputMode::Line | SessionInputMode::Key => request.value.as_bytes().to_vec(),
    };
    let mode = match request.mode {
        SessionInputMode::Line => "line",
        SessionInputMode::Raw => "raw",
        SessionInputMode::Key => "key",
    };
    let request_key = format!(
        "session-control-request:input:{subject}:{}",
        request.idempotency_key
    );
    if let Some(prior) = state
        .store
        .operation_claim(&request_key)
        .map_err(ApiError::internal)?
    {
        return finish_session_control(
            &state,
            &subject,
            "terminal.input.result",
            &result_key,
            &prior,
            &session,
            Err(anyhow::anyhow!(
                "the input request committed before an outcome; st3 will not repeat it"
            )),
        );
    }
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "terminal.input.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("mode".into(), Value::String(mode.into())),
                (
                    "sha256".into(),
                    Value::String(hex::encode(Sha256::digest(&bytes))),
                ),
                ("byte_count".into(), Value::from(bytes.len() as u64)),
                (
                    "runtime_id".into(),
                    Value::String(session.runtime_id.clone()),
                ),
                (
                    "incarnation_id".into(),
                    Value::String(session.incarnation_id.clone()),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(request_key),
        })
        .map_err(ApiError::bad)?;
    let runtime = st_runtime::PtyRuntime::new(state.pty_root.clone())
        .with_binary(state.pty_binary.to_string_lossy());
    let effect = match request.mode {
        SessionInputMode::Line => runtime.send_line_if(
            &session.runtime_id,
            &request.value,
            Some(&session.incarnation_id),
        ),
        SessionInputMode::Raw => {
            runtime.send_raw_if(&session.runtime_id, &bytes, Some(&session.incarnation_id))
        }
        SessionInputMode::Key => runtime.send_key_if(
            &session.runtime_id,
            &request.value,
            Some(&session.incarnation_id),
        ),
    };
    finish_session_control(
        &state,
        &subject,
        "terminal.input.result",
        &result_key,
        &request_claim,
        &session,
        effect,
    )
}

async fn clear_context(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<ContextClearRequest>,
) -> Result<Json<SessionControlResponse>, ApiError> {
    let result_key = format!(
        "session-control-result:context-clear:{subject}:{}",
        request.idempotency_key
    );
    if let Some(result) = state
        .store
        .idempotent_claim(&result_key)
        .map_err(ApiError::internal)?
    {
        return Ok(Json(session_control_response(&subject, &result)));
    }
    let session = live_session(&state, &subject, Some(&request.expected_incarnation))?;
    if !session.terminal || session.driver.as_deref() != Some("claude") {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "context clear requires a terminal Claude driver in st3 v1",
        )));
    }
    let request_key = format!(
        "session-control-request:context-clear:{subject}:{}",
        request.idempotency_key
    );
    if let Some(prior) = state
        .store
        .operation_claim(&request_key)
        .map_err(ApiError::internal)?
    {
        return finish_session_control(
            &state,
            &subject,
            "harness.context-clear.result",
            &result_key,
            &prior,
            &session,
            Ok(()),
        );
    }
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "harness.context-clear.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                (
                    "runtime_id".into(),
                    Value::String(session.runtime_id.clone()),
                ),
                (
                    "incarnation_id".into(),
                    Value::String(session.incarnation_id.clone()),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(request_key),
        })
        .map_err(ApiError::bad)?;
    let effect = st_runtime::PtyRuntime::new(state.pty_root.clone())
        .with_binary(state.pty_binary.to_string_lossy())
        .send_line_if(&session.runtime_id, "/clear", Some(&session.incarnation_id));
    finish_session_control(
        &state,
        &subject,
        "harness.context-clear.result",
        &result_key,
        &request_claim,
        &session,
        effect,
    )
}

async fn signal_session(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<SessionSignalRequest>,
) -> Result<Json<SessionControlResponse>, ApiError> {
    let signal = match request.signal.as_str() {
        "interrupt" => libc::SIGINT,
        "hangup" => libc::SIGHUP,
        "user-1" => libc::SIGUSR1,
        "user-2" => libc::SIGUSR2,
        other => {
            return Err(ApiError::bad(St3Error::new(
                "invalid-session-signal",
                format!("session signal `{other}` is not registered"),
            )));
        }
    };
    let result_key = format!(
        "session-control-result:signal:{subject}:{}",
        request.idempotency_key
    );
    if let Some(result) = state
        .store
        .idempotent_claim(&result_key)
        .map_err(ApiError::internal)?
    {
        return Ok(Json(session_control_response(&subject, &result)));
    }
    let session = live_session(&state, &subject, Some(&request.expected_incarnation))?;
    let request_key = format!(
        "session-control-request:signal:{subject}:{}",
        request.idempotency_key
    );
    if let Some(prior) = state
        .store
        .operation_claim(&request_key)
        .map_err(ApiError::internal)?
    {
        return finish_session_control(
            &state,
            &subject,
            "runtime.action.succeeded",
            &result_key,
            &prior,
            &session,
            Err(anyhow::anyhow!(
                "the signal request committed before an outcome; st3 will not repeat it"
            )),
        );
    }
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "runtime.action.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("action".into(), Value::String("signal".into())),
                ("operation".into(), Value::String(result_key.clone())),
                ("signal".into(), Value::String(request.signal)),
                (
                    "runtime_id".into(),
                    Value::String(session.runtime_id.clone()),
                ),
                (
                    "incarnation_id".into(),
                    Value::String(session.incarnation_id.clone()),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(request_key),
        })
        .map_err(ApiError::bad)?;
    let effect = if session.terminal {
        st_runtime::PtyRuntime::new(state.pty_root.clone())
            .with_binary(state.pty_binary.to_string_lossy())
            .signal_if(&session.runtime_id, Some(&session.incarnation_id), signal)
    } else {
        st_runtime::ExecRuntime::new(state.state_dir.join("exec"), state.state_dir.join("logs"))
            .signal_if(&session.runtime_id, Some(&session.incarnation_id), signal)
    };
    finish_session_control(
        &state,
        &subject,
        "runtime.action.succeeded",
        &result_key,
        &request_claim,
        &session,
        effect,
    )
}

fn finish_session_control(
    state: &AppState,
    subject: &str,
    kind: &str,
    result_key: &str,
    request: &ClaimRecord,
    session: &LiveSession,
    effect: anyhow::Result<()>,
) -> Result<Json<SessionControlResponse>, ApiError> {
    let succeeded = effect.is_ok();
    let mut reason = effect.as_ref().err().map(ToString::to_string);
    let mut fields = BTreeMap::from([
        (
            "runtime_id".into(),
            Value::String(session.runtime_id.clone()),
        ),
        (
            "incarnation_id".into(),
            Value::String(session.incarnation_id.clone()),
        ),
    ]);
    match kind {
        "terminal.input.result" => {
            fields.insert(
                "result".into(),
                Value::String(if succeeded { "written" } else { "failed" }.into()),
            );
        }
        "harness.context-clear.result" => {
            let result = if succeeded { "indeterminate" } else { "failed" };
            if succeeded {
                reason = Some(
                    "the clear command was sent, but no new context epoch was observed".into(),
                );
            }
            fields.insert("result".into(), Value::String(result.into()));
        }
        _ => {
            fields.insert(
                "operation_status".into(),
                Value::String(if succeeded { "succeeded" } else { "failed" }.into()),
            );
        }
    }
    fields.insert(
        "reason".into(),
        reason.clone().map(Value::String).unwrap_or(Value::Null),
    );
    let claim_kind = if kind == "runtime.action.succeeded" && !succeeded {
        "runtime.action.failed"
    } else {
        kind
    };
    let result = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.into(),
            kind: claim_kind.into(),
            actor: Some("requester".into()),
            fields,
            evidence: vec![request.id.clone()],
            expected_subject: None,
            idempotency_key: Some(result_key.into()),
        })
        .map_err(ApiError::bad)?;
    signal_changed(state);
    if !succeeded && let Some(reason) = reason {
        let code = if reason.contains("changed incarnation") {
            "stale-incarnation"
        } else {
            "control-action-failed"
        };
        return Err(ApiError::bad(St3Error::new(code, reason)));
    }
    Ok(Json(SessionControlResponse {
        subject: subject.into(),
        request_claim_id: request.id.clone(),
        result_claim_id: result.id,
        event_cursor: result.store_index,
    }))
}

fn session_control_response(subject: &str, result: &ClaimRecord) -> SessionControlResponse {
    SessionControlResponse {
        subject: subject.into(),
        request_claim_id: result
            .body
            .pointer("/evidence/0")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        result_claim_id: result.id.clone(),
        event_cursor: result.store_index,
    }
}

async fn attach_session(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(_request): Json<AttachRequest>,
) -> Result<Json<Attachment>, ApiError> {
    let session = live_session(&state, &subject, None)?;
    if !session.terminal {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "terminal attachment requires a terminal session",
        )));
    }
    let (capability, expires_at_unix_ms) = state
        .store
        .issue_capability("terminal", &subject, Some(&session.incarnation_id), 30_000)
        .map_err(ApiError::internal)?;
    Ok(Json(Attachment {
        websocket_path: format!(
            "/v1/sessions/terminal/{}?capability={}",
            urlencoding::encode(&subject),
            capability
        ),
        subject,
        runtime_id: session.runtime_id,
        incarnation_id: Some(session.incarnation_id),
        capability,
        expires_at_unix_ms,
    }))
}

async fn post_gate_result(
    State(state): State<AppState>,
    Json(request): Json<GateResultRequest>,
) -> Result<Json<ClaimRecord>, ApiError> {
    if !matches!(request.verdict.as_str(), "pass" | "fail") {
        return Err(ApiError::bad(St3Error::new(
            "invalid-gate-result",
            "a gate-result verdict must be pass or fail",
        )));
    }
    let capability = state
        .store
        .capability(&request.operation_capability, "gate-result")
        .map_err(ApiError::bad)?;
    if capability.used {
        let prior = state
            .store
            .latest_claim(&capability.subject, Some("gate.result"))
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "used-capability",
                    "the gate-result capability was already consumed",
                ))
            })?;
        let same = prior
            .body
            .pointer("/fields/verdict")
            .and_then(Value::as_str)
            == Some(request.verdict.as_str())
            && prior.body.pointer("/fields/reason").and_then(Value::as_str)
                == Some(request.reason.as_str());
        if same {
            return Ok(Json(prior));
        }
        return Err(ApiError::bad(St3Error::new(
            "used-capability",
            "the gate-result capability was already used for another verdict",
        )));
    }
    let input = ClaimInput {
        subject: capability.subject.clone(),
        kind: "gate.result".into(),
        actor: None,
        fields: BTreeMap::from([
            ("verdict".into(), Value::String(request.verdict.clone())),
            ("reason".into(), Value::String(request.reason.clone())),
        ]),
        evidence: request.evidence.clone(),
        expected_subject: None,
        idempotency_key: Some(request.idempotency_key.clone()),
    };
    state
        .store
        .validate_claim_input(&input)
        .map_err(ApiError::bad)?;
    for evidence in &input.evidence {
        if state
            .store
            .claim_by_id(evidence)
            .map_err(ApiError::internal)?
            .is_none()
        {
            return Err(ApiError::bad(St3Error::new(
                "missing-evidence",
                format!("evidence claim `{evidence}` is not stored"),
            )));
        }
    }
    let capability = state
        .store
        .consume_capability(&request.operation_capability, "gate-result")
        .map_err(ApiError::bad)?;
    if capability.used {
        let prior = state
            .store
            .latest_claim(&capability.subject, Some("gate.result"))
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "used-capability",
                    "the gate-result capability was already consumed",
                ))
            })?;
        let same = prior
            .body
            .pointer("/fields/verdict")
            .and_then(Value::as_str)
            == Some(request.verdict.as_str())
            && prior.body.pointer("/fields/reason").and_then(Value::as_str)
                == Some(request.reason.as_str());
        if same {
            return Ok(Json(prior));
        }
        return Err(ApiError::bad(St3Error::new(
            "used-capability",
            "the gate-result capability was already used for another verdict",
        )));
    }
    let response = state.store.append_claim(&input).map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

#[derive(Deserialize)]
struct TerminalQuery {
    capability: String,
}

async fn terminal_session(
    websocket: WebSocketUpgrade,
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Query(query): Query<TerminalQuery>,
) -> Result<Response, ApiError> {
    let capability = state
        .store
        .consume_capability(&query.capability, "terminal")
        .map_err(ApiError::bad)?;
    if capability.used || capability.subject != subject {
        return Err(ApiError::bad(St3Error::new(
            "invalid-capability",
            "the terminal capability is used or names another subject",
        )));
    }
    let status = state
        .store
        .status(Some(&subject))
        .map_err(ApiError::internal)?;
    let actual = status
        .subjects
        .first()
        .and_then(|item| item.actual.as_ref())
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no live session")))?;
    let fields = actual.get("fields").unwrap_or(actual);
    let runtime_id = fields
        .get("runtime_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no runtime")))?
        .to_owned();
    let current_incarnation = fields
        .get("incarnation_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if capability.incarnation_id != current_incarnation {
        return Err(ApiError::bad(St3Error::new(
            "stale-incarnation",
            "the terminal incarnation changed before attachment",
        )));
    }
    Ok(websocket
        .protocols(["st3.terminal.v1"])
        .on_upgrade(move |socket| terminal_proxy(socket, state.pty_root, runtime_id)))
}

async fn terminal_proxy(socket: WebSocket, pty_root: std::path::PathBuf, runtime_id: String) {
    let stream =
        match tokio::net::UnixStream::connect(pty_root.join(format!("{runtime_id}.sock"))).await {
            Ok(stream) => stream,
            Err(_) => return,
        };
    let (mut writer, mut reader) = socket.split();
    let (mut output_stream, mut input_stream) = stream.into_split();
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut bytes = vec![0_u8; 8192];
    loop {
        tokio::select! {
            read = output_stream.read(&mut bytes) => {
                match read {
                    Ok(0) | Err(_) => break,
                    Ok(count) if writer
                        .send(WsMessage::Binary(bytes[..count].to_vec().into()))
                        .await
                        .is_err() => break,
                    Ok(_) => {}
                }
            }
            message = reader.next() => {
                let payload = match message {
                    Some(Ok(WsMessage::Binary(bytes))) => bytes.to_vec(),
                    Some(Ok(WsMessage::Text(text))) => text.as_bytes().to_vec(),
                    Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(WsMessage::Ping(_))) | Some(Ok(WsMessage::Pong(_))) => continue,
                };
                if input_stream.write_all(&payload).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn normalize_message_party(value: &str) -> String {
    if value == "requester" {
        "person/requester".into()
    } else if value.contains('/') {
        value.into()
    } else {
        format!("agent/{value}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::path::PathBuf;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

    fn state(root: &Path) -> AppState {
        AppState {
            store: Arc::new(Store::open_memory("node").unwrap()),
            notify: Arc::new(Notify::new()),
            event_notify: watch::channel(0_u64).0,
            node: "node".into(),
            state_dir: root.to_path_buf(),
            pty_root: root.join("pty"),
            pty_binary: PathBuf::from("pty"),
            fleet_id: None,
            configured_peers: Vec::new(),
        }
    }

    #[test]
    fn replication_heartbeats_do_not_process_the_backlog() {
        assert!(!replication_receive_has_new_data(0));
        assert!(replication_receive_has_new_data(1));
    }

    fn materialize_run_agents(state: &AppState, run: &MissionRunView) {
        let mission = state
            .store
            .mission_spec(
                run.mission.strip_prefix("mission/").unwrap_or(&run.mission),
                Some(&run.revision),
            )
            .unwrap()
            .unwrap();
        let Some(source) = mission.declarations_kdl.as_deref() else {
            return;
        };
        let intent = crate::graph::parse_execution_intent(source, &state.node, &run.id).unwrap();
        state
            .store
            .apply_internal(&intent, &format!("test-materialize:{}", run.id))
            .unwrap();
    }

    fn apply_request(state: &AppState, source: &str, actor: &str, key: &str) -> ApplyRequest {
        let intent = parse_intent(source, &state.node).unwrap();
        let preview = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        ApplyRequest {
            intent: crate::model::IntentInput {
                kdl: source.into(),
                source_name: None,
            },
            expected_subjects: preview.subject_tokens,
            idempotency_key: key.into(),
            actor: Some(actor.into()),
        }
    }

    #[tokio::test]
    async fn an_event_waiter_cannot_consume_the_reconciler_signal() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let mut event = state.event_notify.subscribe();

        signal_changed(&state);

        tokio::time::timeout(Duration::from_millis(50), event.changed())
            .await
            .expect("the event waiter did not wake")
            .expect("the event sender closed");
        tokio::time::timeout(Duration::from_millis(50), state.notify.notified())
            .await
            .expect("the reconciler signal was lost");
    }

    #[tokio::test]
    async fn an_event_wait_ignores_a_wake_without_a_matching_event() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let waiter_state = state.clone();
        let waiter = tokio::spawn(async move {
            events(
                State(waiter_state),
                Query(EventQuery {
                    after: 0,
                    subject: None,
                    owner_run: None,
                    wait: Some(true),
                    timeout_ms: None,
                }),
            )
            .await
            .unwrap()
            .0
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        signal_changed(&state);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished(), "an empty wake ended the event wait");

        state
            .store
            .append_claim(&ClaimInput {
                subject: "custom/test/event".into(),
                kind: "custom.test.recorded".into(),
                actor: None,
                fields: BTreeMap::new(),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .unwrap();
        signal_changed(&state);
        let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the event wait did not finish")
            .unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].kind, "custom.test.recorded");
    }

    #[tokio::test]
    async fn health_and_doctor_report_runtime_metadata() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let (status, health) = get_request(app.clone(), "/v1/health").await;
        assert_eq!(status, StatusCode::OK, "{health}");
        assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
        assert!(health["isolation"].is_string());

        let (status, doctor) = get_request(app, "/v1/doctor").await;
        assert_eq!(status, StatusCode::OK, "{doctor}");
        assert!(doctor["status"].is_string());
        assert!(
            doctor["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|check| check["name"] == "runtime-ownership")
        );
    }

    #[tokio::test]
    async fn replication_receive_accepts_a_request_above_axums_default_body_limit() {
        let root = tempfile::tempdir().unwrap();
        let value = json!({
            "peer": "source",
            "fleet_id": "fleet",
            "exchange": {
                "peer": "source",
                "fleet_id": "fleet",
                "schema_digest": st3_schema::registry().digest(),
                "authority_digest": "",
                "graph_digest": "",
                "inventory": { "digest": "", "envelopes": [] },
                "envelopes": []
            }
        });
        let mut body = serde_json::to_vec(&value).unwrap();
        body.resize(2 * 1024 * 1024 + 1, b' ');
        let response = router(state(root.path()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/replication/receive")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn replication_export_accepts_a_request_above_axums_default_body_limit() {
        let root = tempfile::tempdir().unwrap();
        let value = json!({
            "fleet_id": "fleet",
            "inventory": { "digest": "", "envelopes": [] },
            "summary_only": false
        });
        let mut body = serde_json::to_vec(&value).unwrap();
        body.resize(2 * 1024 * 1024 + 1, b' ');
        let response = router(state(root.path()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/replication/export")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn api_rejects_the_removed_plan_routes() {
        let root = tempfile::tempdir().unwrap();
        for path in ["/v1/plans/example", "/v1/plan-runs/example"] {
            let response = router(state(root.path()))
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn runtime_reset_is_bound_to_the_selected_desire_and_incarnation() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let source = format!(
            r#"
version 2

  agent "resettable" {{
    workspace {workspace:?}
    command "true"
    restart "on-failure"
  }}

"#,
            workspace = root.path().display().to_string()
        );
        let intent = crate::graph::parse_execution_intent(&source, "node", "reset-run").unwrap();
        store
            .apply_internal(&intent, "runtime-reset-desire")
            .unwrap();
        let subject = intent
            .subjects
            .values()
            .find(|subject| subject.kind == "agent")
            .unwrap()
            .subject
            .clone();
        let desired_token = store.selected_desired_token(&subject).unwrap().unwrap();
        store
            .append_claim(&ClaimInput {
                subject: subject.clone(),
                kind: "runtime.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("status".into(), Value::String("running".into())),
                    (
                        "runtime_id".into(),
                        Value::String("resettable-runtime".into()),
                    ),
                    (
                        "incarnation_id".into(),
                        Value::String("incarnation-one".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("runtime-reset-observed".into()),
            })
            .unwrap();

        let app = router(state);
        let (status, reset) = json_request(
            app,
            &format!("/v1/runtimes/reset/{}", urlencoding::encode(&subject)),
            serde_json::to_value(crate::model::RuntimeResetRequest {
                reason: "clear the failed restart window".into(),
                idempotency_key: "runtime-reset-request".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reset}");
        assert_eq!(reset["subject"], subject);
        assert_eq!(reset["desired_token"], desired_token);
        assert_eq!(reset["incarnation_id"], "incarnation-one");
        let claim = store
            .latest_claim(&subject, Some("runtime.restart-window-reset"))
            .unwrap()
            .unwrap();
        assert_eq!(claim.body["fields"]["desired_token"], desired_token);
        assert_eq!(claim.body["fields"]["incarnation_id"], "incarnation-one");
        assert_eq!(
            claim.body["fields"]["reason"],
            "clear the failed restart window"
        );
        assert!(claim.body.get("actor").is_none());
    }

    #[tokio::test]
    async fn resource_refresh_waits_for_its_observation_and_reports_unchanged_success() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let source = format!(
            r#"
version 2

  resource "refresh/file" {{ kind "filesystem.file" }}
  observer "refresh/file" {{
    resource "resource/refresh/file"
    provider "local.file"
    locator {locator:?}
    field "status"
    field "content_hash"
  }}

"#,
            locator = root.path().join("watched.txt").display().to_string()
        );
        let intent = crate::graph::parse_execution_intent(&source, "node", "refresh-run").unwrap();
        store
            .apply_internal(&intent, "resource-refresh-desire")
            .unwrap();
        let observer = intent
            .subjects
            .values()
            .find(|subject| subject.kind == "observer")
            .unwrap()
            .subject
            .clone();
        let resource = intent
            .subjects
            .values()
            .find(|subject| subject.kind == "resource")
            .unwrap()
            .subject
            .clone();
        let revision = store.selected_desired_revision(&observer).unwrap().unwrap();
        let app = router(state.clone());

        let mut refresh_started = state.event_notify.subscribe();
        let first_app = app.clone();
        let refresh_path = format!("/v1/resources/refresh/{}", urlencoding::encode(&resource));
        let refresh_request = serde_json::to_value(crate::model::ResourceRefreshRequest {
            timeout_ms: 1_000,
            idempotency_key: "resource-refresh-first".into(),
        })
        .unwrap();
        let first =
            tokio::spawn(
                async move { json_request(first_app, &refresh_path, refresh_request).await },
            );
        tokio::time::timeout(Duration::from_secs(1), refresh_started.changed())
            .await
            .expect("the first refresh did not start")
            .expect("the event sender closed");
        let first_attempt = store
            .latest_claim(&observer, Some("observer.refresh-requested"))
            .unwrap()
            .unwrap()
            .body["fields"]["attempt"]
            .as_str()
            .unwrap()
            .to_owned();
        let facts = json!({
            "status": "ready",
            "path": root.path().join("watched.txt").display().to_string(),
            "content_hash": "hash-one",
        });
        store
            .record_resource_observation(
                &observer,
                &revision,
                Some("an-unrelated-attempt"),
                &resource,
                Some("unrelated-cursor"),
                &json!({
                    "status": "ready",
                    "path": root.path().join("watched.txt").display().to_string(),
                    "content_hash": "unrelated-hash",
                }),
                1,
                &[],
            )
            .unwrap();
        signal_changed(&state);
        tokio::task::yield_now().await;
        assert!(!first.is_finished());
        store
            .record_resource_observation(
                &observer,
                &revision,
                Some(&first_attempt),
                &resource,
                Some("cursor-one"),
                &facts,
                1,
                &[],
            )
            .unwrap();
        signal_changed(&state);
        let (status, first) = first.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["resource"], resource);
        assert_eq!(first["observers"], json!([observer.clone()]));
        assert_eq!(first["changed"], true);

        let mut refresh_started = state.event_notify.subscribe();
        let refresh_path = format!("/v1/resources/refresh/{}", urlencoding::encode(&resource));
        let refresh_request = serde_json::to_value(crate::model::ResourceRefreshRequest {
            timeout_ms: 1_000,
            idempotency_key: "resource-refresh-second".into(),
        })
        .unwrap();
        let second =
            tokio::spawn(async move { json_request(app, &refresh_path, refresh_request).await });
        tokio::time::timeout(Duration::from_secs(1), refresh_started.changed())
            .await
            .expect("the second refresh did not start")
            .expect("the event sender closed");
        let second_attempt = store
            .latest_claim(&observer, Some("observer.refresh-requested"))
            .unwrap()
            .unwrap()
            .body["fields"]["attempt"]
            .as_str()
            .unwrap()
            .to_owned();
        store
            .record_resource_observation(
                &observer,
                &revision,
                Some(&second_attempt),
                &resource,
                Some("cursor-two"),
                &facts,
                2,
                &[],
            )
            .unwrap();
        signal_changed(&state);
        let (status, second) = second.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_eq!(second["changed"], false);
        assert!(
            second["completed_at_index"].as_u64().unwrap()
                > first["completed_at_index"].as_u64().unwrap()
        );
        assert_eq!(
            store
                .claims_for(&resource, Some("resource.observed"))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .claims_for(&observer, Some("observer.observed"))
                .unwrap()
                .len(),
            3
        );
    }

    async fn json_request(app: Router, path: &str, value: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&value).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let envelope: Value = serde_json::from_slice(&bytes).unwrap();
        if path.starts_with("/v1/client/") {
            assert_eq!(envelope["api_version"], CLIENT_API_VERSION);
            assert_eq!(envelope["snapshot"]["host_id"], "host/node");
        } else {
            assert_eq!(envelope["api_version"], "st3.v1");
            assert_eq!(envelope["snapshot_host"], "node");
        }
        assert!(envelope["request_id"].as_str().unwrap().contains('-'));
        if path.starts_with("/v1/client/") {
            assert!(envelope["snapshot"]["store_index"].is_u64());
        } else {
            assert!(envelope["store_index"].is_u64());
        }
        let value = if status.is_success() {
            envelope["value"].clone()
        } else {
            envelope
        };
        (status, value)
    }

    async fn get_request(app: Router, path: &str) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let envelope: Value = serde_json::from_slice(&bytes).unwrap();
        if path.starts_with("/v1/client/") {
            assert_eq!(envelope["api_version"], CLIENT_API_VERSION);
            assert_eq!(envelope["snapshot"]["host_id"], "host/node");
        } else {
            assert_eq!(envelope["api_version"], "st3.v1");
            assert_eq!(envelope["snapshot_host"], "node");
        }
        assert!(envelope["request_id"].as_str().unwrap().contains('-'));
        if path.starts_with("/v1/client/") {
            assert!(envelope["snapshot"]["store_index"].is_u64());
        } else {
            assert!(envelope["store_index"].is_u64());
        }
        let value = if status.is_success() {
            envelope["value"].clone()
        } else {
            envelope
        };
        (status, value)
    }

    #[tokio::test]
    async fn mission_resolves_a_bare_document_to_immutable_bytes() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let version = state
            .store
            .put_document("doc/task", b"hello", &None, "document")
            .unwrap();
        let app = router(state);
        let (status, body) = json_request(
            app.clone(),
            "/v1/intent/mission",
            serde_json::to_value(MissionRequest {
                intent: crate::model::IntentInput {
                    kdl: r#"version 2
 message "task" { to "worker"; content "doc/task" } "#
                        .into(),
                    source_name: None,
                },
                at_index: None,
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.pointer("/resolved_intent/kdl")
                .and_then(Value::as_str)
                .unwrap()
                .contains(&format!("doc/task@{}", version.hash))
        );
    }

    #[tokio::test]
    async fn preview_and_publish_reject_invalid_nested_declarations_without_a_write() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);
        let before = store.index().unwrap();
        let kdl = r#"version 2
mission "invalid-message" state="ready" {
  goal "Reject this declaration before the mission runs."
  step "send" {
    agentless
    message "notice" {
      to "agent/worker"
      subject "This field is not valid."
      body "This field is not valid."
    }
  }
}"#;

        let (preview_status, preview_error) = json_request(
            app.clone(),
            "/v1/intent/mission",
            serde_json::to_value(MissionRequest {
                intent: crate::model::IntentInput {
                    kdl: kdl.into(),
                    source_name: Some("invalid-message.kdl".into()),
                },
                at_index: None,
            })
            .unwrap(),
        )
        .await;
        assert_eq!(preview_status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(preview_error["code"], "unknown-child");
        assert_eq!(store.index().unwrap(), before);

        let (publish_status, publish_error) = json_request(
            app,
            "/v1/intent/apply",
            serde_json::to_value(ApplyRequest {
                intent: crate::model::IntentInput {
                    kdl: kdl.into(),
                    source_name: Some("invalid-message.kdl".into()),
                },
                expected_subjects: BTreeMap::new(),
                idempotency_key: "reject-invalid-nested-message".into(),
                actor: Some("person/nathan".into()),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(publish_status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(publish_error["code"], "unknown-child");
        assert_eq!(store.index().unwrap(), before);
    }

    #[tokio::test]
    async fn planning_requires_an_exact_preview_and_publishes_without_a_run() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let marker = workspace.join("marker.txt");
        fs::write(&marker, "unchanged\n").unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);

        let (status, started) = json_request(
            app.clone(),
            "/v1/launches",
            serde_json::to_value(PlanningSessionStartRequest {
                mission: "planned/work".into(),
                run: None,
                request: b"Mission a two-step release without changing this workspace.".to_vec(),
                workspace: workspace.display().to_string(),
                requester: Some("nathan".into()),
                model: Some("gpt-5.6-sol".into()),
                effort: Some("medium".into()),
                idempotency_key: "planning-session-test".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{started}");
        let session = started["id"].as_str().unwrap();
        let planner = started["planner"].as_str().unwrap();
        assert!(planner.ends_with(&format!("/planner.{}", &session[..10])));
        assert_eq!(started["requester"], "person/nathan");
        let request_reference = started["request"].as_str().unwrap();
        assert!(request_reference.starts_with("doc/planning/"));
        let (request_name, request_hash) = request_reference.rsplit_once('@').unwrap();
        assert_eq!(
            store
                .get_document(request_name, request_hash)
                .unwrap()
                .unwrap(),
            b"Mission a two-step release without changing this workspace."
        );
        let standing = store
            .active_mission_runs()
            .unwrap()
            .into_iter()
            .find(|run| run.steps.is_empty() && run.status == "running")
            .unwrap();
        let standing_mission = store
            .mission_spec(
                standing.mission.trim_start_matches("mission/"),
                Some(&standing.revision),
            )
            .unwrap()
            .unwrap();
        let standing_intent = crate::graph::parse_execution_intent(
            standing_mission.declarations_kdl.as_ref().unwrap(),
            "node",
            &standing.id,
        )
        .unwrap();
        let planner_launch = standing_intent
            .subjects
            .get(planner)
            .unwrap()
            .member
            .as_ref()
            .unwrap()
            .launch
            .clone();
        assert!(matches!(
            &planner_launch,
            crate::model::LaunchSpec::Argv(arguments)
                if arguments.iter().any(|argument| argument == "--dangerously-bypass-approvals-and-sandbox")
                    && arguments.iter().any(|argument| argument == "--dangerously-bypass-hook-trust")
        ));
        assert!(store.mission_spec("planned/work", None).unwrap().is_none());
        assert_eq!(store.active_mission_runs().unwrap().len(), 1);

        let first = br#"
version 2

  mission "planned/work" state="ready" {
    goal "Publish the planned result."
    step "inspect" { goal "Inspect the source." }
    step "change" {
      goal "Make the approved change."
      depends-on { step "inspect" completed }
    }
  }

"#;
        let mut side_effect = br#"
version 2
  agent "side-effect" {
    workspace "/tmp"
    harness "codex" { prompt "This must never be published." }
  }
"#
        .to_vec();
        side_effect.extend_from_slice(&first[b"version 2\n".len()..]);
        let (status, rejected) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Mission with a side effect".to_vec(),
                kdl: side_effect,
                idempotency_key: "planning-candidate-side-effect".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rejected}");
        assert_eq!(rejected["code"], "runtime-outside-mission");
        assert!(
            store
                .desired_subjects()
                .unwrap()
                .into_iter()
                .all(|desired| desired.subject != "agent/node.side-effect")
        );

        let (status, submitted) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Mission\n\n1. Inspect.\n2. Change.\n".to_vec(),
                kdl: first.to_vec(),
                idempotency_key: "planning-candidate-one".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{submitted}");
        assert_eq!(submitted["candidate"]["revision"], 1);
        assert!(store.mission_spec("planned/work", None).unwrap().is_none());
        let (status, resubmitted) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Mission\n\n1. Inspect.\n2. Change.\n".to_vec(),
                kdl: first.to_vec(),
                idempotency_key: "planning-candidate-one-network-retry".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resubmitted}");
        assert_eq!(resubmitted["candidate"]["revision"], 1);
        assert_eq!(
            store
                .claims_for(
                    &format!("planning-session/{session}"),
                    Some("planning-session.candidate-submitted"),
                )
                .unwrap()
                .len(),
            1
        );

        let (status, previewed) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/preview"),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{previewed}");
        let first_hash = previewed["preview"]["hash"].as_str().unwrap().to_owned();
        let graph = previewed["preview"]["graph"].as_str().unwrap();
        assert!(graph.contains("inspect [root]"), "{graph}");
        assert!(graph.contains("change [after inspect]"), "{graph}");
        assert!(
            previewed["preview"]["diff"]
                .as_str()
                .unwrap()
                .contains("mission/planned/work")
        );
        let (_, attention) = get_request(app.clone(), "/v1/attention?person=nathan").await;
        assert_eq!(attention.as_array().unwrap().len(), 1);
        assert_eq!(attention[0]["kind"], "launch-approval");
        assert_eq!(
            attention[0]["subject"],
            format!("planning-session/{session}")
        );

        let (status, revised) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/revise"),
            serde_json::to_value(PlanningRevisionRequest {
                actor: "person/nathan".into(),
                feedback: b"Add a verification step.".to_vec(),
                idempotency_key: "planning-revision-one".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revised}");
        assert_eq!(revised["status"], "revision-requested");
        assert!(revised.get("preview").is_none());
        assert!(store.mission_spec("planned/work", None).unwrap().is_none());
        let (_, attention) = get_request(app.clone(), "/v1/attention?person=nathan").await;
        assert_eq!(attention, json!([]));

        let second = br#"
version 2

  mission "planned/work" state="ready" {
    goal "Publish the planned and verified result."
    step "inspect" { goal "Inspect the source." }
    step "change" {
      goal "Make the approved change."
      depends-on { step "inspect" completed }
    }
    step "verify" {
      goal "Verify the result."
      depends-on { step "change" completed }
    }
  }

"#;
        let (status, resubmitted) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Mission\n\n1. Inspect.\n2. Change.\n3. Verify.\n".to_vec(),
                kdl: second.to_vec(),
                idempotency_key: "planning-candidate-two".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resubmitted}");
        assert_eq!(resubmitted["candidate"]["revision"], 2);

        let (status, previewed) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/preview"),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{previewed}");
        let current_hash = previewed["preview"]["hash"].as_str().unwrap().to_owned();
        assert_ne!(current_hash, first_hash);
        let (status, variants) = get_request(
            app.clone(),
            &format!("/v1/client/launches/{session}/variants"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{variants}");
        let variant = &variants["items"][0];
        let preview_token = variant["preview_token"].as_str().unwrap().to_owned();
        assert!(preview_token.starts_with("lpv0:"));
        assert_eq!(current_hash, preview_token);
        assert_eq!(variant["normalized_mission"]["id"], "planned/work");
        assert!(
            !serde_json::to_string(&variant["normalized_mission"])
                .unwrap()
                .contains("declarations_kdl")
        );
        assert_eq!(
            variant["visualization"]["views"],
            json!([
                "graph",
                "timeline",
                "swimlane",
                "revision",
                "risk",
                "live-progress"
            ])
        );
        assert_eq!(
            variant["visualization"]["timeline"]["entries"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            variant["visualization"]["swimlanes"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            variant["structured_diff"]["changes"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let (status, decision) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/decisions"),
            serde_json::to_value(LaunchDecisionRequest {
                actor: planner.into(),
                question: "Which verification level?".into(),
                choices: vec!["focused".into(), "full".into()],
                idempotency_key: "launch-decision-verification".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{decision}");
        let decision_id = decision["id"].as_str().unwrap();
        let decision_path = decision_id.trim_start_matches("launch-decision/");
        let (status, answered) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/decisions/{decision_path}/answer"),
            serde_json::to_value(LaunchDecisionAnswerRequest {
                actor: "person/nathan".into(),
                answer: "full".into(),
                expected_revision: 1,
                idempotency_key: "launch-decision-verification-answer".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{answered}");
        assert_eq!(answered["state"], "answered");
        assert_eq!(answered["answer"], "full");
        let (status, immutable) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/decisions/{decision_path}/answer"),
            serde_json::to_value(LaunchDecisionAnswerRequest {
                actor: "person/nathan".into(),
                answer: "focused".into(),
                expected_revision: 1,
                idempotency_key: "launch-decision-verification-conflict".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{immutable}");
        assert_eq!(immutable["code"], "launch-decision-immutable");
        let (_, attention) = get_request(app.clone(), "/v1/attention?person=nathan").await;
        assert_eq!(attention.as_array().unwrap().len(), 1);
        assert_eq!(attention[0]["kind"], "launch-approval");

        let (status, unauthorized) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/intruder".into(),
                preview_hash: current_hash.clone(),
                idempotency_key: "planning-approve-unauthorized".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{unauthorized}");
        assert_eq!(unauthorized["code"], "launch-review-not-authorized");
        assert!(store.mission_spec("planned/work", None).unwrap().is_none());

        let (status, stale) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/nathan".into(),
                preview_hash: first_hash,
                idempotency_key: "planning-approve-stale".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{stale}");
        assert_eq!(stale["code"], "stale-launch-preview");
        assert!(store.mission_spec("planned/work", None).unwrap().is_none());

        let (status, approved) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/nathan".into(),
                preview_hash: preview_token.clone(),
                idempotency_key: "planning-approve-current".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(approved["status"], "approved");
        assert_eq!(
            approved["published_revision"],
            approved["candidate"]["mission_revision"]
        );
        assert!(store.mission_spec("planned/work", None).unwrap().is_some());
        let active = store.active_mission_runs().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].phase, "cleanup-cancelled");
        assert_eq!(fs::read_to_string(&marker).unwrap(), "unchanged\n");
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
        let (_, attention) = get_request(app.clone(), "/v1/attention?person=nathan").await;
        assert_eq!(attention, json!([]));

        let documents = store
            .latest_claim(
                &format!("planning-session/{session}"),
                Some("planning-session.approved"),
            )
            .unwrap()
            .expect("the published mission does not link its documents");
        for field in ["markdown", "kdl"] {
            let reference = documents
                .body
                .pointer(&format!("/fields/{field}"))
                .and_then(Value::as_str)
                .unwrap();
            let (name, hash) = reference.rsplit_once('@').unwrap();
            assert!(store.get_document(name, hash).unwrap().is_some());
        }
        assert!(
            store
                .desired_subjects()
                .unwrap()
                .into_iter()
                .all(|desired| desired.subject != planner)
        );

        let (status, approved_again) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/nathan".into(),
                preview_hash: approved["preview"]["hash"].as_str().unwrap().into(),
                idempotency_key: "planning-approve-retry".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{approved_again}");
        assert_eq!(approved_again["status"], "approved");
        let (status, started_run) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/approve-and-launch"),
            serde_json::to_value(LaunchApproveAndStartRequest {
                actor: "person/nathan".into(),
                preview_hash: preview_token.clone(),
                workspace: workspace.display().to_string(),
                inputs: BTreeMap::new(),
                idempotency_key: "approved-launch-combined".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{started_run}");
        assert_eq!(
            started_run["mission_run"]["revision"],
            approved["published_revision"]
        );
        assert_eq!(started_run["launch"]["status"], "approved");
        let (status, started_again) = json_request(
            app,
            &format!("/v1/launches/{session}/approve-and-launch"),
            serde_json::to_value(LaunchApproveAndStartRequest {
                actor: "person/nathan".into(),
                preview_hash: preview_token,
                workspace: workspace.display().to_string(),
                inputs: BTreeMap::new(),
                idempotency_key: "approved-launch-combined".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{started_again}");
        assert_eq!(
            started_again["mission_run"]["subject"],
            started_run["mission_run"]["subject"]
        );
        assert_eq!(
            store
                .claims_for("mission/planned/work", Some("mission.published"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .claims_for(
                    &format!("planning-session/{session}"),
                    Some("planning-session.approved"),
                )
                .unwrap()
                .len(),
            1
        );
        for kind in [
            "planning-session.started",
            "planning-session.candidate-submitted",
            "planning-session.previewed",
            "planning-session.revision-requested",
            "planning-session.approved",
        ] {
            assert!(
                !store
                    .claims_for(&format!("planning-session/{session}"), Some(kind))
                    .unwrap()
                    .is_empty(),
                "missing {kind}"
            );
        }
        let started_event = store
            .claims_for(
                &format!("planning-session/{session}"),
                Some("planning-session.started"),
            )
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            started_event
                .body
                .pointer("/fields/request")
                .and_then(Value::as_str),
            Some(request_reference)
        );
        let before = serde_json::to_value(store.planning_session(session).unwrap()).unwrap();
        store.rebuild_claim_projections().unwrap();
        let after = serde_json::to_value(store.planning_session(session).unwrap()).unwrap();
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn declarative_planning_approval_stops_its_session_planner() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let request = store
            .put_document(
                "doc/planning/direct/request",
                b"Mission one inspection.",
                &None,
                "direct-planning-request",
            )
            .unwrap();
        let session = "planning/direct/one";
        let source = format!(
            r#"version 2
planning-session {session:?} {{
  mission "planned/direct"
  request {:?}
  workspace {:?}
  requester "person/operator"
  planner "codex" {{ model "gpt-5.6-sol"; effort "medium" }}
}}
"#,
            format!("{}@{}", request.name, request.hash),
            workspace.display().to_string(),
        );
        let intent = parse_intent(&source, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source,
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply_as(
                &intent,
                &preview.subject_tokens,
                "direct-planning-session",
                Some("person/operator"),
            )
            .unwrap();
        let planner =
            crate::graph::planning_planner_subject(&format!("planning-session/{session}"));
        assert_eq!(
            store.selected_desired_kind(&planner).unwrap().as_deref(),
            Some("agent")
        );

        let app = router(state);
        let encoded_session = urlencoding::encode(session);
        let candidate = br#"version 2
mission "planned/direct" state="ready" {
  goal "Inspect the release."
  step "inspect" { goal "Inspect the release input." }
}
"#;
        let (status, submitted) = json_request(
            app.clone(),
            &format!("/v1/launches/{encoded_session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.clone(),
                markdown: b"# Mission\n\nInspect the release input.\n".to_vec(),
                kdl: candidate.to_vec(),
                idempotency_key: "direct-planning-candidate".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{submitted}");
        let (status, previewed) = json_request(
            app.clone(),
            &format!("/v1/launches/{encoded_session}/preview"),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{previewed}");
        let preview_hash = previewed["preview"]["hash"].as_str().unwrap();
        let (status, approved) = json_request(
            app,
            &format!("/v1/launches/{encoded_session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/operator".into(),
                preview_hash: preview_hash.into(),
                idempotency_key: "direct-planning-approval".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(approved["status"], "approved");
        assert_eq!(
            store.selected_desired_kind(&planner).unwrap().as_deref(),
            Some("stop")
        );
    }

    #[tokio::test]
    async fn one_targeted_planning_approval_also_approves_the_run_revision() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let initial = r#"version 2
mission "targeted" state="ready" revisions="human-only" {
  goal "Use the initial mission."
  step "work" { agentless }
}
"#;
        let intent = parse_intent(initial, "node").unwrap();
        let preview = store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: initial.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &preview.subject_tokens, "targeted-mission")
            .unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "targeted".into(),
                revision: None,
                workspace: workspace.display().to_string(),
                requester: Some("person/operator".into()),
                mode: None,
                inputs: BTreeMap::new(),
                idempotency_key: "targeted-run".into(),
            })
            .unwrap();
        let app = router(state);
        let (status, session) = json_request(
            app.clone(),
            "/v1/launches",
            serde_json::to_value(PlanningSessionStartRequest {
                mission: "ignored".into(),
                run: Some(run.subject.clone()),
                request: b"Add the final verification.".to_vec(),
                workspace: workspace.display().to_string(),
                requester: Some("person/operator".into()),
                model: None,
                effort: None,
                idempotency_key: "targeted-session".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{session}");
        let session_id = session["id"].as_str().unwrap();
        let planner = session["planner"].as_str().unwrap();
        let candidate = br#"version 2
mission "targeted" state="ready" revisions="human-only" {
  goal "Use the reviewed mission."
  step "work" { agentless }
  step "verify" {
    agentless
    depends-on { step "work" completed }
  }
}
"#;
        let (status, submitted) = json_request(
            app.clone(),
            &format!("/v1/launches/{session_id}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Targeted mission\n".to_vec(),
                kdl: candidate.to_vec(),
                idempotency_key: "targeted-candidate".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{submitted}");
        let preview_hash = submitted["preview"]["hash"].as_str().unwrap();
        let (status, approved) = json_request(
            app,
            &format!("/v1/launches/{session_id}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/operator".into(),
                preview_hash: preview_hash.into(),
                idempotency_key: "targeted-approval".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(approved["status"], "approved");
        let revised = store.mission_run(&run.id).unwrap().unwrap();
        assert_ne!(revised.generation, run.generation);

        let approvals = store
            .events_after(0, None)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == "revision-proposal.approved")
            .count();
        assert_eq!(approvals, 1);
    }

    #[tokio::test]
    async fn planning_compares_named_variants_and_proposes_from_one_generation() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let source = r#"
version 2

  mission "variants" state="ready" {
    goal "Use the initial mission."
    step "work" { goal "Use the initial goal." }
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &planned.subject_tokens, "variant-initial-mission")
            .unwrap();
        let run = store
            .create_mission_run(&MissionRunRequest {
                mission: "variants".into(),
                revision: None,
                workspace: workspace.display().to_string(),
                requester: Some("person/nathan".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "variant-run".into(),
            })
            .unwrap();
        let app = router(state);
        let (status, started) = json_request(
            app.clone(),
            "/v1/launches",
            serde_json::to_value(PlanningSessionStartRequest {
                mission: "ignored-when-run-is-present".into(),
                run: Some(run.subject.clone()),
                request: b"Compare a compact mission with an extended mission.".to_vec(),
                workspace: workspace.display().to_string(),
                requester: Some("person/nathan".into()),
                model: None,
                effort: None,
                idempotency_key: "variant-session".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{started}");
        assert_eq!(started["target_mission_run"], run.subject);
        assert_eq!(started["source_generation"], run.generation);
        let session = started["id"].as_str().unwrap();
        let planner = started["planner"].as_str().unwrap();
        let compact = br#"
version 2

  mission "variants" state="ready" {
    goal "Use the compact mission."
    step "work" { goal "Use the compact goal." }
  }

"#;
        let extended = br#"
version 2

  mission "variants" state="ready" {
    goal "Use the extended mission."
    step "work" { goal "Use the extended goal." }
    step "verify" {
      depends-on { step "work" completed }
      goal "Verify the extended result."
    }
  }

"#;
        for (index, (name, kdl)) in [
            ("compact", compact.as_slice()),
            ("extended", extended.as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            let (status, submitted) = json_request(
                app.clone(),
                &format!("/v1/launches/{session}/variants/{name}/submit"),
                serde_json::to_value(PlanningCandidateSubmitRequest {
                    actor: planner.into(),
                    markdown: format!("# {name} mission\n").into_bytes(),
                    kdl: kdl.to_vec(),
                    idempotency_key: format!("variant-submit-{name}"),
                })
                .unwrap(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{submitted}");
            assert_eq!(submitted["variants"].as_array().unwrap().len(), index + 1);
            let (status, previewed) = json_request(
                app.clone(),
                &format!("/v1/launches/{session}/variants/{name}/preview"),
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{previewed}");
        }
        assert_eq!(
            store
                .claims_for(
                    &format!("planning-session/{session}"),
                    Some("planning-session.candidate-submitted"),
                )
                .unwrap()
                .len(),
            2
        );
        let (status, compared) = get_request(
            app.clone(),
            &format!("/v1/launches/{session}/variants/compact/compare/extended"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{compared}");
        assert_eq!(compared["left"]["name"], "compact");
        assert_eq!(compared["right"]["name"], "extended");

        let (status, proposed) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/variants/extended/propose"),
            serde_json::to_value(PlanningProposalRequest {
                actor: "person/nathan".into(),
                reason: "the extended variant has the required check".into(),
                idempotency_key: "variant-propose-extended".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{proposed}");
        assert_eq!(proposed["status"], "applied");
        assert_ne!(proposed["mission_run"]["generation"], run.generation);

        let (status, retried) = json_request(
            app.clone(),
            &format!("/v1/launches/{session}/variants/extended/propose"),
            serde_json::to_value(PlanningProposalRequest {
                actor: "person/nathan".into(),
                reason: "the extended variant has the required check".into(),
                idempotency_key: "variant-propose-extended".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retried}");
        assert_eq!(
            retried["mission_run"]["generation"],
            proposed["mission_run"]["generation"]
        );

        let (status, stale) = json_request(
            app,
            &format!("/v1/launches/{session}/variants/compact/propose"),
            serde_json::to_value(PlanningProposalRequest {
                actor: "person/nathan".into(),
                reason: "try the stale compact variant".into(),
                idempotency_key: "variant-propose-compact".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{stale}");
        assert_eq!(stale["code"], "stale-launch-generation");
    }

    #[tokio::test]
    async fn message_ingress_rejects_empty_content_before_it_enters_the_fifo() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let (status, body) = json_request(
            app,
            "/v1/messages",
            serde_json::to_value(MessageSendRequest {
                idempotency_key: "empty-message".into(),
                from: "agent/sender".into(),
                to: "agent/receiver".into(),
                content: " \n".into(),
                title: None,
                in_reply_to: None,
                tags: Vec::new(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], "empty-message");
    }

    #[tokio::test]
    async fn a_runtime_work_message_uses_a_valid_daemon_sender() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let (status, body) = json_request(
            app,
            "/v1/messages",
            serde_json::to_value(MessageSendRequest {
                idempotency_key: "runtime-work-message".into(),
                from: "daemon/runtime".into(),
                to: "agent/worker".into(),
                content: "A durable mission step is ready.".into(),
                title: Some("Mission step ready".into()),
                in_reply_to: None,
                tags: vec!["st3-work:step-run/run/build@1@1@incarnation".into()],
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["from"], "daemon/runtime");
        assert_eq!(body["to"], "agent/worker");
    }

    #[tokio::test]
    async fn a_requester_message_uses_a_full_person_subject() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let (status, body) = json_request(
            app,
            "/v1/messages",
            serde_json::to_value(MessageSendRequest {
                idempotency_key: "requester-message".into(),
                from: "requester".into(),
                to: "agent/worker".into(),
                content: "Please do the work.".into(),
                title: None,
                in_reply_to: None,
                tags: Vec::new(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["from"], "person/requester");
    }

    #[tokio::test]
    async fn message_lifecycle_requires_the_exact_recipient_actor() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let (status, message) = json_request(
            app.clone(),
            "/v1/messages",
            serde_json::to_value(MessageSendRequest {
                idempotency_key: "recipient-authority-message".into(),
                from: "agent/sender".into(),
                to: "person/receiver".into(),
                content: "Please review this.".into(),
                title: None,
                in_reply_to: None,
                tags: Vec::new(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{message}");
        let message_id = message["subject"]
            .as_str()
            .unwrap()
            .trim_start_matches("message/");
        let path = format!("/v1/messages/{message_id}/claims");

        let (status, missing) = json_request(
            app.clone(),
            &path,
            serde_json::to_value(MessageLifecycleRequest {
                lifecycle: "read".into(),
                actor: None,
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: "missing-message-actor".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{missing}");
        assert_eq!(missing["code"], "missing-message-actor");

        let (status, wrong) = json_request(
            app.clone(),
            &path,
            serde_json::to_value(MessageLifecycleRequest {
                lifecycle: "read".into(),
                actor: Some("person/intruder".into()),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: "wrong-message-actor".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{wrong}");
        assert_eq!(wrong["code"], "wrong-message-recipient");

        let (status, read) = json_request(
            app.clone(),
            &path,
            serde_json::to_value(MessageLifecycleRequest {
                lifecycle: "delivered".into(),
                actor: Some("person/receiver".into()),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: "right-message-actor".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{read}");
        assert_eq!(read["actor"], "person/receiver");

        let legacy = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages/close/obsolete")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn eval_upload_posts_its_staged_documents_before_apply() {
        let root = tempfile::tempdir().unwrap();
        let eval_dir = tempfile::tempdir().unwrap();
        let bytes = b"hello from the eval";
        let hash = hex::encode(Sha256::digest(bytes));
        fs::create_dir_all(eval_dir.path().join(".st3-documents")).unwrap();
        fs::write(eval_dir.path().join(".st3-documents").join(&hash), bytes).unwrap();
        fs::write(
            eval_dir.path().join("eval.kdl"),
            format!(
                r#"
version 2

  mission "eval/demo" state="ready" timeout="2m" {{
    goal "Complete mission eval/demo."
    baseline "document-content" {{ has "doc/evals/demo/task@{hash}" "hello" }}
    step "document" {{
      agentless
      title "The document exists"

        message "task" {{ to "person/worker"; content "doc/evals/demo/task@{hash}" }}

    }}
    completion {{ when "all-steps-exhausted" }}
  }}

"#
            ),
        )
        .unwrap();
        let bundle = crate::archive::archive_eval(eval_dir.path()).unwrap();
        let bundle_hash = hex::encode(Sha256::digest(&bundle));
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);
        let (status, body) = json_request(
            app.clone(),
            "/v1/evals",
            serde_json::to_value(EvalStartRequest {
                name: "demo".into(),
                bundle_hash,
                bundle,
                inputs: BTreeMap::new(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["mission_run"]
                .as_str()
                .unwrap()
                .starts_with("mission-run/")
        );
        let eval_run = body["mission_run"].as_str().unwrap();
        let (status, eval_status) = get_request(
            app.clone(),
            &format!("/v1/evals/{}", urlencoding::encode(eval_run)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{eval_status}");
        assert_eq!(eval_status["mission_run"], body["mission_run"]);
        assert_eq!(eval_status["lifecycle"], "running");
        assert!(eval_status.get("active_checkpoint").is_none());
        let root = body["mission_run"].as_str().unwrap();
        let (status, mission_runs) = get_request(
            app,
            &format!("/v1/mission-runs?root={}", urlencoding::encode(root)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{mission_runs}");
        assert_eq!(mission_runs.as_array().unwrap().len(), 1);
        assert_eq!(mission_runs[0]["subject"], root);
        assert_eq!(
            store
                .get_document("doc/evals/demo/task", &hash)
                .unwrap()
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn the_same_eval_bundle_can_run_again_with_a_new_workspace() {
        let root = tempfile::tempdir().unwrap();
        let eval_dir = tempfile::tempdir().unwrap();
        fs::write(
            eval_dir.path().join("eval.kdl"),
            r#"version 2
mission "eval/repeat" state="ready" timeout="2m" {
  concurrent-runs max=2
  goal "Run the same archived eval in a fresh workspace."
  completion { when "all-steps-exhausted" }
  step "done" {
    agentless
    gate "the current eval workspace exists" {
      exec "test -d '${EVAL_ROOT}'"
      host "local"
      workspace "${EVAL_ROOT}"
      time-limit "1m"
    }
  }
}
"#,
        )
        .unwrap();
        let bundle = crate::archive::archive_eval(eval_dir.path()).unwrap();
        let bundle_hash = hex::encode(Sha256::digest(&bundle));
        let app = router(state(root.path()));

        let request = || {
            serde_json::to_value(EvalStartRequest {
                name: "repeat".into(),
                bundle_hash: bundle_hash.clone(),
                bundle: bundle.clone(),
                inputs: BTreeMap::new(),
            })
            .unwrap()
        };
        let (first_status, first) = json_request(app.clone(), "/v1/evals", request()).await;
        let (second_status, second) = json_request(app, "/v1/evals", request()).await;

        assert_eq!(first_status, StatusCode::OK, "{first}");
        assert_eq!(second_status, StatusCode::OK, "{second}");
        assert_ne!(first["mission_run"], second["mission_run"]);
    }

    #[tokio::test]
    async fn eval_entries_require_a_bounded_twenty_minute_timeout() {
        for (timeout, code) in [
            ("", "missing-eval-timeout"),
            (" timeout=\"21m\"", "eval-timeout-too-large"),
        ] {
            let root = tempfile::tempdir().unwrap();
            let eval_dir = tempfile::tempdir().unwrap();
            fs::write(
                eval_dir.path().join("eval.kdl"),
                format!(
                    r#"version 2
mission "eval/deadline" state="ready"{timeout} {{
  goal "Prove the eval deadline is explicit and bounded."
  completion {{ when "all-steps-exhausted" }}
  step "done" {{ agentless }}
}}
"#
                ),
            )
            .unwrap();
            let bundle = crate::archive::archive_eval(eval_dir.path()).unwrap();
            let bundle_hash = hex::encode(Sha256::digest(&bundle));
            let app = router(state(root.path()));

            let (status, body) = json_request(
                app,
                "/v1/evals",
                serde_json::to_value(EvalStartRequest {
                    name: "deadline".into(),
                    bundle_hash,
                    bundle,
                    inputs: BTreeMap::new(),
                })
                .unwrap(),
            )
            .await;

            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
            assert_eq!(body["code"], code, "{body}");
        }
    }

    #[test]
    fn eval_fixtures_are_owned_without_replacing_selected_desired_state() {
        let current = parse_intent(
            r#"version 2
host "node" { document "doc/hosts/production@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
"#,
            "node",
        )
        .unwrap()
        .subjects
        .into_values()
        .collect::<Vec<_>>();
        let mut conflicting = parse_intent(
            r#"version 2
host "local" { document "doc/hosts/eval@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
"#,
            "node",
        )
        .unwrap();

        let error = scope_eval_desired_subjects(&mut conflicting, &current, "mission-run/eval-run")
            .unwrap_err();
        assert_eq!(error.code, "eval-desired-conflict");
        assert_eq!(current[0].subject, "host/node");
        assert_eq!(
            current[0].desired["children"][0]["arguments"][0],
            "doc/hosts/production@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let mut fixture = parse_intent(
            r#"version 2
resource "eval/fixture" { kind "filesystem.file" }
"#,
            "node",
        )
        .unwrap();
        scope_eval_desired_subjects(&mut fixture, &current, "mission-run/eval-run").unwrap();
        assert_eq!(
            fixture.subjects["resource/eval/fixture"]
                .owner_run
                .as_deref(),
            Some("mission-run/eval-run")
        );
    }

    #[test]
    fn eval_reuses_an_identical_selected_declaration_without_owning_it() {
        let mut intent = parse_intent(
            r#"version 2
resource "shared" { kind "filesystem.file" }
"#,
            "node",
        )
        .unwrap();
        let current = intent.subjects.values().cloned().collect::<Vec<_>>();

        scope_eval_desired_subjects(&mut intent, &current, "mission-run/eval-run").unwrap();

        assert!(intent.subjects.is_empty());
    }

    #[tokio::test]
    async fn an_eval_cannot_replace_selected_host_metadata() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let production = state
            .store
            .put_document(
                "doc/hosts/node",
                b"Production host facts.\n",
                &None,
                "production-host",
            )
            .unwrap();
        let source = format!(
            "version 2\nhost \"node\" {{ document \"doc/hosts/node@{}\" }}\n",
            production.hash
        );
        let intent = parse_intent(&source, "node").unwrap();
        let preview = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source,
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &preview.subject_tokens, "publish-production-host")
            .unwrap();

        let eval_dir = tempfile::tempdir().unwrap();
        let eval_host = b"Eval host facts.\n";
        let eval_hash = hex::encode(Sha256::digest(eval_host));
        fs::create_dir_all(eval_dir.path().join(".st3-documents")).unwrap();
        fs::write(
            eval_dir.path().join(".st3-documents").join(&eval_hash),
            eval_host,
        )
        .unwrap();
        fs::write(
            eval_dir.path().join("eval.kdl"),
            format!(
                r#"version 2
host "local" {{ document "doc/hosts/eval@{eval_hash}" }}
mission "eval/host-conflict" state="ready" timeout="1m" {{
  goal "Do not replace production host metadata."
  completion {{ when "all-steps-exhausted" }}
  step "done" {{ agentless }}
}}
"#
            ),
        )
        .unwrap();
        let bundle = crate::archive::archive_eval(eval_dir.path()).unwrap();
        let bundle_hash = hex::encode(Sha256::digest(&bundle));
        let app = router(state.clone());

        let (status, body) = json_request(
            app,
            "/v1/evals",
            serde_json::to_value(EvalStartRequest {
                name: "host-conflict".into(),
                bundle_hash,
                bundle,
                inputs: BTreeMap::new(),
            })
            .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "eval-desired-conflict");
        let selected = state
            .store
            .desired_subjects()
            .unwrap()
            .into_iter()
            .find(|desired| desired.subject == "host/node")
            .unwrap();
        assert_eq!(
            selected.desired["children"][0]["arguments"][0],
            format!("doc/hosts/node@{}", production.hash)
        );
    }

    #[tokio::test]
    async fn an_eval_can_publish_ready_helper_missions() {
        let root = tempfile::tempdir().unwrap();
        let eval_dir = tempfile::tempdir().unwrap();
        fs::write(
            eval_dir.path().join("eval.kdl"),
            r#"
version 2

mission "eval/demo/helper" state="ready" {
  goal "Supply one helper mission."
  completion { when "all-steps-exhausted" }
  step "done" { agentless }
}

mission "eval/demo" state="ready" timeout="2m" {
  goal "Run the eval entry mission."
  completion { when "all-steps-exhausted" }
  step "done" { agentless }
}
"#,
        )
        .unwrap();
        let bundle = crate::archive::archive_eval(eval_dir.path()).unwrap();
        let bundle_hash = hex::encode(Sha256::digest(&bundle));
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);

        let (status, body) = json_request(
            app,
            "/v1/evals",
            serde_json::to_value(EvalStartRequest {
                name: "demo".into(),
                bundle_hash,
                bundle,
                inputs: BTreeMap::new(),
            })
            .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let run = store
            .mission_run(body["mission_run"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(run.mission, "mission/eval/demo");
    }

    #[tokio::test]
    async fn manual_work_wake_is_idempotent_and_uses_the_durable_driver_inbox() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2

mission "wake" state="ready" {
  goal "Exercise manual work wake."
  agent "worker" { workspace "/tmp"; command "true" }
  step "work" { assigned-to "agent/${ST_MISSION_RUN}/worker" }
}
"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "manual-wake-source")
            .unwrap();
        let run = state
            .store
            .create_mission_run(&MissionRunRequest {
                mission: "wake".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "manual-wake-run".into(),
            })
            .unwrap();
        materialize_run_agents(&state, &run);
        let step = &run.steps[0];
        let agent = format!("agent/{}/worker", run.id);
        state
            .store
            .set_step_state(&step.subject, "ready", None)
            .unwrap();
        state
            .store
            .append_claim(&ClaimInput {
                subject: agent.clone(),
                kind: "runtime.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("status".into(), Value::String("running".into())),
                    ("runtime_id".into(), Value::String("wake-worker".into())),
                    (
                        "incarnation_id".into(),
                        Value::String("wake-incarnation".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("manual-wake-runtime".into()),
            })
            .unwrap();
        state
            .store
            .append_claim(&ClaimInput {
                subject: agent.clone(),
                kind: "harness.observed".into(),
                actor: Some(agent.clone()),
                fields: BTreeMap::from([
                    ("state".into(), Value::String("idle".into())),
                    ("driver".into(), Value::String("codex".into())),
                    (
                        "incarnation_id".into(),
                        Value::String("wake-incarnation".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("manual-wake-harness".into()),
            })
            .unwrap();

        let app = router(state.clone());
        let request = serde_json::to_value(WorkWakeRequest {
            actor: "person/operator".into(),
            reason: "the operator requested another delivery".into(),
            idempotency_key: "manual-wake-request".into(),
        })
        .unwrap();
        let path = format!("/v1/work/wake/{}", urlencoding::encode(&step.subject));
        let (status, first) = json_request(app.clone(), &path, request.clone()).await;
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["status"], "sent");
        assert!(
            first["tags"]
                .as_array()
                .unwrap()
                .contains(&Value::String("st3-wake-source:manual".into()))
        );
        let (status, repeated) = json_request(app, &path, request).await;
        assert_eq!(status, StatusCode::OK, "{repeated}");
        assert_eq!(repeated["subject"], first["subject"]);
        assert_eq!(state.store.messages(Some(&agent), true).unwrap().len(), 1);
        let projected = state.store.step_run(&step.subject).unwrap().unwrap();
        let wake = projected.wake.expect("wake projection");
        assert_eq!(wake.attempts, 1);
        assert_eq!(wake.assignee_state, "idle");
    }

    #[tokio::test]
    async fn a_mission_revision_requires_current_agent_authority() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2

  mission "revision" state="ready" {
    goal "Complete mission revision."
     agent "sup" {
       workspace "."
       command "true"
       mission-authority { revise "revision" }
     }
    step "work" { goal "First goal." }
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "revision-mission-one")
            .unwrap();
        let run = state
            .store
            .create_mission_run(&MissionRunRequest {
                mission: "revision".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "revision-run".into(),
            })
            .unwrap();
        materialize_run_agents(&state, &run);
        let actor = format!("agent/{}/sup", run.id);
        let app = router(state.clone());
        let replacement = r#"
version 2

  mission "revision" state="ready" {
    goal "Complete mission revision."
     agent "sup" {
       workspace "."
       command "true"
       mission-authority { revise "revision" }
     }
    step "work" { goal "Corrected goal." }
  }

"#;
        let (status, revised) = json_request(
            app,
            &format!("/v1/mission-runs/{}/revision", run.id),
            serde_json::to_value(MissionRevisionRequest {
                intent: crate::model::IntentInput {
                    kdl: replacement.into(),
                    source_name: None,
                },
                actor,
                reason: "the first goal was incomplete".into(),
                idempotency_key: "revision-two".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revised}");
        assert_eq!(revised["status"], "applied");
        assert_ne!(revised["mission_run"]["revision"], run.revision);
        assert_eq!(revised["mission_run"]["root_revision"], run.root_revision);
        assert_eq!(revised["mission_run"]["steps"][0]["status"], "pending");
        assert_eq!(state.store.desired_subjects().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn agent_start_authority_is_action_specific_and_cannot_be_self_granted() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2

mission "authority-host" state="ready" {
  goal "Keep two authority test agents available."
  agent "starter" {
    workspace "."
    command "true"
    mission-authority { start "authority-target" }
  }
  agent "publisher" {
    workspace "."
    command "true"
    mission-authority { publish "authority-target" }
  }
}

mission "authority-target" state="ready" {
  concurrent-runs
  goal "Keep each authorized test run open."
}
"#;
        let intent = parse_intent(source, "node").unwrap();
        let preview = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &preview.subject_tokens, "authority-host")
            .unwrap();
        let host = state
            .store
            .create_mission_run(&MissionRunRequest {
                mission: "authority-host".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "authority-host-run".into(),
            })
            .unwrap();
        materialize_run_agents(&state, &host);
        let starter = format!("agent/{}/starter", host.id);
        let publisher = format!("agent/{}/publisher", host.id);
        let target = intent.missions["authority-target"].revision.clone();
        let declaration = |id: &str, requester: &str| {
            format!(
                "version 2\nmission-run {id:?} {{\n  mission {:?}\n  workspace {:?}\n  requester {requester:?}\n}}\n",
                format!("mission/authority-target@{target}"),
                root.path().display().to_string(),
            )
        };
        let app = router(state.clone());

        let allowed = declaration("authority-target/allowed", &starter);
        let (status, body) = json_request(
            app.clone(),
            "/v1/intent/apply",
            serde_json::to_value(apply_request(
                &state,
                &allowed,
                &starter,
                "authority-start-allowed",
            ))
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let denied = declaration("authority-target/denied", &publisher);
        let (status, body) = json_request(
            app.clone(),
            "/v1/intent/apply",
            serde_json::to_value(apply_request(
                &state,
                &denied,
                &publisher,
                "authority-start-denied",
            ))
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "mission-authority-denied");

        let self_grant = r#"version 2
mission "authority-self-grant" state="ready" {
  goal "Reject this generic agent publication."
  agent "self" {
    workspace "."
    command "true"
    mission-authority { publish "authority-self-grant" }
  }
}
"#;
        let (status, body) = json_request(
            app,
            "/v1/intent/apply",
            serde_json::to_value(ApplyRequest {
                intent: crate::model::IntentInput {
                    kdl: self_grant.into(),
                    source_name: None,
                },
                expected_subjects: BTreeMap::new(),
                idempotency_key: "authority-self-grant".into(),
                actor: Some(starter),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "agent-mission-publication-route");
    }

    #[tokio::test]
    async fn a_claimed_step_publishes_one_attempt_bound_ready_mission() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2

  mission "bootstrap" state="ready" {
    goal "Complete mission bootstrap."
    agent "planner" {
      workspace "."
      command "true"
      mission-authority { publish "project/*" }
    }
    step "compile" {
      assigned-to "agent/${ST_MISSION_RUN}/planner"
      produces-mission "project/work"
    }
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "publish-bootstrap-output")
            .unwrap();
        let run = state
            .store
            .create_mission_run(&MissionRunRequest {
                mission: "bootstrap".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "run-bootstrap-output".into(),
            })
            .unwrap();
        materialize_run_agents(&state, &run);
        let actor = format!("agent/{}/planner", run.id);
        let step = run.steps[0].clone();
        state
            .store
            .set_step_state(&step.subject, "ready", None)
            .unwrap();
        let produced = r#"
version 2

  mission "project/work" state="ready" {
    goal "Complete mission project/work."
    step "inspect" { title "Inspect the project" }
    step "implement" { depends-on { step "inspect" completed } }
  }

"#;
        let app = router(state.clone());
        let (status, output) = json_request(
            app.clone(),
            &format!("/v1/work/mission/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(MissionProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: produced.into(),
                    source_name: Some("generated.kdl".into()),
                },
                actor: actor.clone(),
                incarnation: Some("test".into()),
                idempotency_key: "reject-unclaimed-output".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{output}");
        assert_eq!(output["code"], "work-not-claimed");
        state
            .store
            .work_action(
                &step.subject,
                "claim",
                &WorkRequest {
                    actor: Some(actor.clone()),
                    incarnation: Some("test".into()),
                    summary: None,
                    reason: None,
                    evidence: Vec::new(),
                    idempotency_key: "claim-bootstrap-output".into(),
                },
            )
            .unwrap();
        let wrong = r#"
version 2

  mission "project/other" state="ready" { goal "Complete mission project/other."; goal "Complete mission project/other."; step "inspect" { } }

"#;
        let (status, output) = json_request(
            app.clone(),
            &format!("/v1/work/mission/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(MissionProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: wrong.into(),
                    source_name: Some("wrong.kdl".into()),
                },
                actor: actor.clone(),
                incarnation: Some("test".into()),
                idempotency_key: "reject-wrong-output".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{output}");
        assert_eq!(output["code"], "wrong-mission-output");
        let (status, output) = json_request(
            app,
            &format!("/v1/work/mission/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(MissionProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: produced.into(),
                    source_name: Some("generated.kdl".into()),
                },
                actor,
                incarnation: Some("test".into()),
                idempotency_key: "publish-produced-work".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{output}");
        assert_eq!(output["mission"], "mission/project/work");
        let revision = output["revision"].as_str().unwrap();
        assert_eq!(revision.len(), 64);
        assert!(
            state
                .store
                .mission_spec("project/work", Some(revision))
                .unwrap()
                .is_some()
        );
        let bound = state
            .store
            .mission_output(&step.subject, 1, &step.definition_hash)
            .unwrap()
            .expect("attempt-bound mission output");
        assert_eq!(bound.revision, revision);
        assert_eq!(bound.claim_id, output["claim_id"]);
    }

    #[tokio::test]
    async fn claims_endpoint_returns_bounded_cursor_pages() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        for (subject, key) in [("host/one", "one"), ("host/two", "two")] {
            state
                .store
                .append_claim(&ClaimInput {
                    subject: subject.into(),
                    kind: "transport.observed".into(),
                    actor: None,
                    fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: Some(key.into()),
                })
                .unwrap();
        }
        let app = router(state);
        let (status, first) = get_request(app.clone(), "/v1/claims?limit=1").await;
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["claims"].as_array().unwrap().len(), 1);
        let cursor = first["next_cursor"].as_u64().unwrap();
        let (status, second) = get_request(
            app.clone(),
            &format!("/v1/claims?limit=1&after_index={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_eq!(second["claims"].as_array().unwrap().len(), 1);
        assert!(second["next_cursor"].is_null());

        let (status, descending) =
            get_request(app, "/v1/claims?limit=1&order=desc&before_index=3").await;
        assert_eq!(status, StatusCode::OK, "{descending}");
        assert_eq!(
            descending["claims"][0]["subject"].as_str(),
            Some("host/two")
        );
    }

    #[tokio::test]
    async fn public_claim_admission_and_idempotency_use_the_exported_schema() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let reconcile_notify = state.notify.clone();
        let app = router(state);

        let (status, schema) = get_request(app.clone(), "/v1/schema").await;
        assert_eq!(status, StatusCode::OK, "{schema}");
        assert_eq!(schema["schema"], st3_schema::SCHEMA_NAME);
        assert_eq!(schema["digest"], st3_schema::registry().digest());

        let forbidden = serde_json::to_value(ClaimInput {
            subject: "host/node".into(),
            kind: "transport.observed".into(),
            actor: None,
            fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some("caller-transport".into()),
        })
        .unwrap();
        let (status, body) = json_request(app.clone(), "/v1/claims", forbidden).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "claim-write-forbidden");

        let resource = ClaimInput {
            subject: "resource/example".into(),
            kind: "resource.observed".into(),
            actor: Some("person/nathan".into()),
            fields: BTreeMap::from([
                (
                    "kind".into(),
                    Value::String("custom.example.measurement".into()),
                ),
                ("value".into(), Value::from(1)),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some("caller-resource".into()),
        };
        let (status, first) = json_request(
            app.clone(),
            "/v1/claims",
            serde_json::to_value(&resource).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{first}");
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            reconcile_notify.notified(),
        )
        .await
        .expect("a new claim must wake reconciliation");
        let (status, retry) = json_request(
            app.clone(),
            "/v1/claims",
            serde_json::to_value(&resource).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retry}");
        assert_eq!(retry["id"], first["id"]);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(25),
                reconcile_notify.notified(),
            )
            .await
            .is_err(),
            "an idempotent claim replay must not wake reconciliation"
        );

        let mut changed = resource;
        changed.fields.insert("value".into(), Value::from(2));
        let (status, mismatch) =
            json_request(app, "/v1/claims", serde_json::to_value(changed).unwrap()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{mismatch}");
        assert_eq!(mismatch["code"], "idempotency-mismatch");
    }

    #[tokio::test]
    async fn an_invalid_gate_result_does_not_consume_its_capability() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let subject = "gate-operation/test/result";
        let (capability, _) = store
            .issue_capability("gate-result", subject, None, 60_000)
            .unwrap();
        let app = router(state);
        let request = |idempotency_key: String| {
            serde_json::to_value(GateResultRequest {
                operation_capability: capability.clone(),
                verdict: "pass".into(),
                reason: "the evidence passes".into(),
                evidence: Vec::new(),
                idempotency_key,
            })
            .unwrap()
        };

        let (status, invalid) =
            json_request(app.clone(), "/v1/gate-results", request("x".repeat(513))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{invalid}");
        assert_eq!(invalid["code"], "invalid-idempotency-key");
        assert!(!store.capability(&capability, "gate-result").unwrap().used);

        let (status, accepted) =
            json_request(app, "/v1/gate-results", request("valid-gate-result".into())).await;
        assert_eq!(status, StatusCode::OK, "{accepted}");
        assert_eq!(accepted["body"]["fields"]["verdict"], "pass");
        assert!(store.capability(&capability, "gate-result").unwrap().used);
    }

    #[tokio::test]
    async fn human_review_list_and_decisions_use_exact_current_requests() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2

  mission "review-api" state="ready" {
    goal "Complete mission review-api."
    step "approval" { gate "human-review" type="human" { reviewer "person/nathan" } }
  }

"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .mission(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "review-api-mission")
            .unwrap();
        let run = state
            .store
            .create_mission_run(&MissionRunRequest {
                mission: "review-api".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                inputs: BTreeMap::new(),
                idempotency_key: "review-api-run".into(),
            })
            .unwrap();
        let step = &run.steps[0];
        let request_fields = |owner: String, definition: String, operation: &str| {
            BTreeMap::from([
                ("owner".into(), Value::String(owner)),
                ("reviewer".into(), Value::String("person/nathan".into())),
                (
                    "question".into(),
                    Value::String("Is the candidate ready?".into()),
                ),
                (
                    "review_targets".into(),
                    Value::Array(vec![
                        Value::String("resource/review/candidate".into()),
                        Value::String("doc/review/report@abc".into()),
                    ]),
                ),
                (
                    "decisions".into(),
                    Value::Array(vec![
                        Value::String("approved".into()),
                        Value::String("rejected".into()),
                    ]),
                ),
                ("operation".into(), Value::String(operation.into())),
                (
                    "mission_revision".into(),
                    Value::String(run.revision.clone()),
                ),
                ("step_definition".into(), Value::String(definition)),
                ("attempt".into(), Value::from(step.attempt)),
            ])
        };
        let step_request = state
            .store
            .append_claim(&ClaimInput {
                subject: "gate-operation/review-api/approval".into(),
                kind: "gate.requested".into(),
                actor: None,
                fields: request_fields(
                    step.subject.clone(),
                    step.definition_hash.clone(),
                    "gate-operation/review-api/approval",
                ),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("step-review-request".into()),
            })
            .unwrap();
        let mission_request = state
            .store
            .append_claim(&ClaimInput {
                subject: "gate-operation/review-api/mission".into(),
                kind: "gate.requested".into(),
                actor: None,
                fields: request_fields(
                    run.subject.clone(),
                    run.revision.clone(),
                    "gate-operation/review-api/mission",
                ),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("mission-review-request".into()),
            })
            .unwrap();
        let store = state.store.clone();
        let app = router(state);
        let (status, listed) = get_request(app.clone(), "/v1/reviews").await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(listed.as_array().unwrap().len(), 2);
        assert_eq!(listed[0]["request"], step_request.id);
        assert_eq!(listed[0]["owner"], step.subject);
        assert_eq!(listed[0]["step"], "approval");
        assert_eq!(listed[0]["review_targets"].as_array().unwrap().len(), 2);
        assert_eq!(listed[1]["request"], mission_request.id);
        assert_eq!(listed[1]["owner"], run.subject);
        assert!(listed[1].get("step").is_none());

        let (status, selected) =
            get_request(app.clone(), "/v1/reviews?reviewer=person%2Fnathan").await;
        assert_eq!(status, StatusCode::OK, "{selected}");
        assert_eq!(selected.as_array().unwrap().len(), 2);
        let (status, attention) =
            get_request(app.clone(), "/v1/attention?person=person%2Fnathan").await;
        assert_eq!(status, StatusCode::OK, "{attention}");
        assert_eq!(attention.as_array().unwrap().len(), 2);
        assert!(
            attention
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["kind"] == "human-gate")
        );

        let (status, filtered) =
            get_request(app.clone(), "/v1/reviews?reviewer=person%2Fsomeone-else").await;
        assert_eq!(status, StatusCode::OK, "{filtered}");
        assert_eq!(filtered, json!([]));

        store
            .append_claim(&ClaimInput {
                subject: step_request.subject.clone(),
                kind: "gate.result".into(),
                actor: Some("person/someone-else".into()),
                fields: BTreeMap::from([
                    ("verdict".into(), Value::String("pass".into())),
                    ("request".into(), Value::String(step_request.id.clone())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("wrong-review-result".into()),
            })
            .unwrap();
        store
            .append_claim(&ClaimInput {
                subject: step_request.subject.clone(),
                kind: "gate.result".into(),
                actor: Some("person/nathan".into()),
                fields: BTreeMap::from([("verdict".into(), Value::String("pass".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("unbound-review-result".into()),
            })
            .unwrap();
        let (_, still_pending) = get_request(app.clone(), "/v1/reviews").await;
        assert_eq!(still_pending.as_array().unwrap().len(), 2);

        let body = |actor: &str| {
            serde_json::to_value(ReviewRequest {
                decision: "approved".into(),
                reason: None,
                actor: Some(actor.into()),
                expected_subject: None,
            })
            .unwrap()
        };
        let (status, rejected) = json_request(
            app.clone(),
            &format!("/v1/reviews/{}", run.subject),
            body("person/someone-else"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rejected}");
        assert_eq!(rejected["code"], "wrong-reviewer");

        let (status, accepted_mission) = json_request(
            app.clone(),
            &format!("/v1/reviews/{}", run.subject),
            body("person/nathan"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{accepted_mission}");
        assert_eq!(
            accepted_mission["body"]["fields"]["request"],
            mission_request.id
        );
        assert_eq!(accepted_mission["body"]["fields"]["verdict"], "pass");

        let reject = serde_json::to_value(ReviewRequest {
            decision: "rejected".into(),
            reason: Some("the evidence is incomplete".into()),
            actor: Some("person/nathan".into()),
            expected_subject: None,
        })
        .unwrap();
        let (status, accepted_step) = json_request(
            app.clone(),
            &format!("/v1/reviews/{}", step.subject),
            reject,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{accepted_step}");
        assert_eq!(accepted_step["body"]["fields"]["request"], step_request.id);
        assert_eq!(accepted_step["body"]["fields"]["verdict"], "fail");
        assert_eq!(accepted_step["body"]["evidence"][0], step_request.id);

        let (_, empty) = get_request(app, "/v1/reviews?reviewer=person%2Fnathan").await;
        assert_eq!(empty, json!([]));
    }

    #[tokio::test]
    async fn attention_routes_request_filter_and_resolve_one_fault() {
        let root = tempfile::tempdir().unwrap();
        let app = router(state(root.path()));
        let request = serde_json::to_value(AttentionRequest {
            reviewer: "nathan".into(),
            title: "Fabric needs review".into(),
            reason: "The queue did not recover.".into(),
            severity: "error".into(),
            targets: vec!["resource/fabric/queue".into()],
            actor: "agent/fabric/worker".into(),
            idempotency_key: "api-attention-fabric".into(),
        })
        .unwrap();
        let (status, created) = json_request(app.clone(), "/v1/attention", request).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        assert_eq!(created["reviewer"], "person/nathan");
        assert_eq!(created["status"], "pending");

        let (status, selected) = get_request(app.clone(), "/v1/attention?person=nathan").await;
        assert_eq!(status, StatusCode::OK, "{selected}");
        assert_eq!(selected.as_array().unwrap().len(), 1);
        assert_eq!(selected[0]["kind"], "fault");
        let (_, filtered) =
            get_request(app.clone(), "/v1/attention?person=person%2Fsomeone-else").await;
        assert_eq!(filtered, json!([]));

        let subject = created["subject"].as_str().unwrap();
        let wrong = serde_json::to_value(AttentionResolveRequest {
            outcome: "resolved".into(),
            reason: None,
            actor: "person/someone-else".into(),
            idempotency_key: "api-attention-wrong".into(),
        })
        .unwrap();
        let (status, rejected) = json_request(
            app.clone(),
            &format!("/v1/attention/resolve/{}", urlencoding::encode(subject)),
            wrong,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rejected}");
        assert_eq!(rejected["code"], "wrong-attention-reviewer");

        let resolution = serde_json::to_value(AttentionResolveRequest {
            outcome: "resolved".into(),
            reason: Some("The queue recovered.".into()),
            actor: "person/nathan".into(),
            idempotency_key: "api-attention-resolve".into(),
        })
        .unwrap();
        let (status, resolved) = json_request(
            app.clone(),
            &format!("/v1/attention/resolve/{}", urlencoding::encode(subject)),
            resolution,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolved}");
        assert_eq!(resolved["status"], "resolved");
        let (_, empty) = get_request(app, "/v1/attention?person=nathan").await;
        assert_eq!(empty, json!([]));
    }

    #[tokio::test]
    async fn quick_claude_publishes_the_native_driver_and_attach_is_incarnation_bound() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);
        let request = QuickAgentRequest {
            subject: "agent/node.quick".into(),
            worktree: root.path().display().to_string(),
            model: Some("test-model".into()),
            effort: Some("high".into()),
            prompt: None,
            arguments: Vec::new(),
            expected_subject: Vec::new(),
            idempotency_key: "quick-claude".into(),
        };
        let (status, created) = json_request(
            app.clone(),
            "/v1/claude",
            serde_json::to_value(&request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        assert_eq!(
            created["subject"],
            format!(
                "agent/{}/node.quick",
                created["mission_run"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("mission-run/")
            )
        );
        let agent_subject = created["subject"].as_str().unwrap().to_owned();
        assert_eq!(created["mission"], "mission/standing/node.quick");
        assert!(
            created["mission_run"]
                .as_str()
                .unwrap()
                .starts_with("mission-run/")
        );
        assert!(
            created["generation"]
                .as_str()
                .unwrap()
                .starts_with("run-generation/")
        );
        let standing = store
            .active_mission_runs()
            .unwrap()
            .into_iter()
            .find(|run| run.subject == created["mission_run"])
            .unwrap();
        let mission = store
            .mission_spec("standing/node.quick", Some(&standing.revision))
            .unwrap()
            .unwrap();
        let intent = crate::graph::parse_execution_intent(
            mission.declarations_kdl.as_ref().unwrap(),
            "node",
            &standing.id,
        )
        .unwrap();
        store
            .apply_internal(&intent, "test-materialize-standing-agent")
            .unwrap();
        let (status, repeated) = json_request(
            app.clone(),
            "/v1/claude",
            serde_json::to_value(&request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{repeated}");
        assert_eq!(repeated, created);
        assert_eq!(
            store
                .active_mission_runs()
                .unwrap()
                .into_iter()
                .filter(|run| run.mission == "mission/standing/node.quick")
                .count(),
            1
        );
        let desired = store.desired_subjects().unwrap();
        let member = desired
            .iter()
            .find(|subject| subject.subject == agent_subject)
            .unwrap()
            .member
            .as_ref()
            .unwrap();
        assert_eq!(member.driver.as_deref(), Some("claude"));
        assert!(matches!(
            &member.launch,
            crate::model::LaunchSpec::Argv(argv)
                if argv.windows(2).any(|pair| pair == ["driver", "claude"])
        ));
        assert!(matches!(
            &member.launch,
            crate::model::LaunchSpec::Argv(argv)
                if argv.last().map(String::as_str) == Some(crate::boot::BOOT_PROMPT)
        ));

        let mut revised = request.clone();
        revised.model = Some("revised-model".into());
        revised.expected_subject = store
            .selected_desired_token(&agent_subject)
            .unwrap()
            .into_iter()
            .collect();
        revised.idempotency_key = "quick-claude-revised".into();
        let (status, revised_response) = json_request(
            app.clone(),
            "/v1/claude",
            serde_json::to_value(&revised).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revised_response}");
        assert_eq!(revised_response["mission_run"], created["mission_run"]);
        assert_ne!(revised_response["generation"], created["generation"]);
        assert_eq!(
            store
                .active_mission_runs()
                .unwrap()
                .into_iter()
                .filter(|run| run.mission == "mission/standing/node.quick")
                .count(),
            1
        );

        store
            .append_claim(&ClaimInput {
                subject: agent_subject.clone(),
                kind: "runtime.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("status".into(), Value::String("running".into())),
                    (
                        "runtime_id".into(),
                        Value::String(revised_response["runtime_id"].as_str().unwrap().into()),
                    ),
                    (
                        "incarnation_id".into(),
                        Value::String("generation-one".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("quick-running".into()),
            })
            .unwrap();
        let (status, attachment) = json_request(
            app.clone(),
            &format!(
                "/v1/sessions/attach/{}",
                urlencoding::encode(&agent_subject)
            ),
            serde_json::to_value(AttachRequest::default()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{attachment}");
        assert_eq!(attachment["incarnation_id"], "generation-one");
        assert!(
            attachment["websocket_path"]
                .as_str()
                .unwrap()
                .contains("capability=")
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_app = app.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, server_app).await.unwrap();
        });
        let mut request = format!(
            "ws://{address}{}",
            attachment["websocket_path"].as_str().unwrap()
        )
        .into_client_request()
        .unwrap();
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "st3.terminal.v1".parse().unwrap());
        let (mut websocket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(
            response.headers().get("sec-websocket-protocol").unwrap(),
            "st3.terminal.v1"
        );
        let _ = websocket.close(None).await;
        server.abort();

        let (status, error) = json_request(
            app,
            &format!(
                "/v1/sessions/{}/context/clear",
                urlencoding::encode(&agent_subject)
            ),
            serde_json::to_value(ContextClearRequest {
                expected_incarnation: "generation-old".into(),
                idempotency_key: "clear-old".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(error["code"], "stale-incarnation");
    }

    #[tokio::test]
    async fn resource_watch_is_idempotent_and_unwatch_stops_only_the_subscription() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let store = state.store.clone();
        let app = router(state);
        let request = ResourceWatchRequest {
            provider: "github.pull-request".into(),
            locator: "compoundingtech/st2#403".into(),
            fields: vec!["state".into(), "head".into()],
            to: Some("agent/node.watcher".into()),
            idempotency_key: "watch-403".into(),
        };
        let (status, first) = json_request(
            app.clone(),
            "/v1/resource-watches",
            serde_json::to_value(&request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{first}");
        let (status, retry) = json_request(
            app.clone(),
            "/v1/resource-watches",
            serde_json::to_value(&request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retry}");
        assert_eq!(retry, first);
        assert_eq!(
            store
                .desired_subjects()
                .unwrap()
                .into_iter()
                .filter(|desired| matches!(
                    desired.kind.as_str(),
                    "resource" | "observer" | "subscription"
                ))
                .count(),
            1
        );
        let second_request = ResourceWatchRequest {
            provider: "github.pull-request".into(),
            locator: "compoundingtech/st2#403".into(),
            fields: vec!["checks".into()],
            to: Some("agent/node.second-watcher".into()),
            idempotency_key: "watch-403-checks".into(),
        };
        let (status, second) = json_request(
            app.clone(),
            "/v1/resource-watches",
            serde_json::to_value(&second_request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_ne!(second["subscription"], first["subscription"]);
        assert_eq!(store.active_mission_runs().unwrap().len(), 2);
        let subscription = first["subscription"].as_str().unwrap();
        let (status, stopped) = json_request(
            app,
            &format!("/v1/resource-watches/{}", urlencoding::encode(subscription)),
            serde_json::to_value(ResourceUnwatchRequest {
                actor: Some("agent/node.watcher".into()),
                idempotency_key: "unwatch-403".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{stopped}");
        let run_id = subscription
            .strip_prefix("subscription/")
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        let run = store.mission_run(run_id).unwrap().unwrap();
        assert_eq!(run.status, "running");
        assert_eq!(run.phase, "cleanup-cancelled");
        assert_eq!(store.active_mission_runs().unwrap().len(), 2);
    }
}
