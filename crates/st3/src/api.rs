use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt as _, StreamExt as _};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::Notify;
use tower::ServiceExt as _;

use crate::archive::hydrate_cell;
use crate::graph::{parse_intent, resolve_document_references};
use crate::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, ClaimInput, ClaimRecord, ClaimsPage,
    ContextClearRequest, DocumentPutRequest, DocumentVersion, EvalStartRequest, EvalStartResponse,
    EvalStatus, EventRecord, JudgementRequest, MessageLifecycleRequest, MessageSendRequest,
    MessageView, PlanRequest, PlanResponse, QuickAgentRequest, QuickAgentResponse,
    ReplicationBatch, ReplicationQuery, ReplicationResponse, ReviewRequest, SessionControlResponse,
    SessionSignalRequest, St3Error, StatusResponse,
};
use crate::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub notify: Arc<Notify>,
    pub node: String,
    pub state_dir: std::path::PathBuf,
    pub pty_root: std::path::PathBuf,
    pub trusted_peers: std::collections::BTreeSet<String>,
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
            | "stale-incarnation" => StatusCode::CONFLICT,
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
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/intent/plan", post(plan))
        .route("/v1/intent/apply", post(apply))
        .route("/v1/documents", get(list_documents).post(put_document))
        .route("/v1/documents/content", get(get_document))
        .route("/v1/claims", get(list_claims).post(post_claim))
        .route("/v1/reviews/{*subject}", post(post_review))
        .route("/v1/messages", get(list_messages).post(send_message))
        .route("/v1/messages/{message_id}/claims", post(post_message_claim))
        .route("/v1/messages/close/{*subject}", post(close_message))
        .route("/v1/messages/read/{*subject}", get(read_message))
        .route("/v1/status", get(status))
        .route("/v1/events", get(events))
        .route("/v1/claude", post(quick_claude))
        .route("/v1/codex", post(quick_codex))
        .route("/v1/evals", post(start_eval))
        .route("/v1/evals/{*scope}", get(get_eval))
        .route("/v1/judgements", post(post_judgement))
        .route("/v1/sessions/{subject}/context/clear", post(clear_context))
        .route("/v1/sessions/{subject}/signal", post(signal_session))
        .route("/v1/sessions/attach/{*subject}", post(attach_session))
        .route("/v1/sessions/{subject}/attach", post(attach_session))
        .route("/v1/sessions/terminal/{*subject}", get(terminal_session))
        .route("/v1/peer/export", get(export_peer))
        .route("/v1/peer/cursor", get(peer_cursor))
        .route("/v1/peer/claims", post(import_peer))
        .route("/v1/peer/claims/query", post(query_peer))
        .layer(from_fn_with_state(state.clone(), response_envelope))
        .with_state(state)
}

async fn response_envelope(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let response = next.run(request).await;
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
    let request_id = new_request_id();
    let envelope = if status.is_success() {
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
    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({
        "status": "ready",
        "node": state.node,
        "store_index": state.store.index().map_err(ApiError::internal)?,
        "security": "trusted-network-no-tls-no-acls",
    })))
}

async fn plan(
    State(state): State<AppState>,
    Json(request): Json<PlanRequest>,
) -> Result<Json<PlanResponse>, ApiError> {
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
        .plan_at(&intent, resolved, request.at_index)
        .map(Json)
        .map_err(ApiError::bad)
}

async fn apply(
    State(state): State<AppState>,
    Json(request): Json<ApplyRequest>,
) -> Result<Json<ApplyResponse>, ApiError> {
    let intent = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    let response = state
        .store
        .apply(
            &intent,
            &request.expected_subjects,
            &request.idempotency_key,
        )
        .map_err(ApiError::bad)?;
    state.notify.notify_one();
    Ok(Json(response))
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
    state.notify.notify_waiters();
    Ok(Json(response))
}

#[derive(Deserialize)]
struct DocumentQuery {
    name: Option<String>,
}

async fn list_documents(
    State(state): State<AppState>,
    Query(query): Query<DocumentQuery>,
) -> Result<Json<Vec<DocumentVersion>>, ApiError> {
    state
        .store
        .list_documents(query.name.as_deref())
        .map(Json)
        .map_err(ApiError::internal)
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
    let bytes = state
        .store
        .get_document(name, hash)
        .map_err(ApiError::internal)?
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
    let response = state.store.append_claim(&request).map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    Ok(Json(response))
}

#[derive(Deserialize)]
struct ClaimsQuery {
    subject: Option<String>,
    scope: Option<String>,
    #[serde(default, alias = "after")]
    after_index: u64,
    #[serde(default = "default_claim_limit")]
    limit: usize,
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
    state
        .store
        .claims_page(
            query.subject.as_deref(),
            query.scope.as_deref(),
            query.after_index,
            query.limit,
        )
        .map(Json)
        .map_err(ApiError::internal)
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
    let subject = if subject.starts_with("resource/") {
        subject
    } else {
        format!("resource/{subject}")
    };
    let actor = request.actor.map(|actor| {
        if actor.contains('/') {
            actor
        } else {
            format!("person/{actor}")
        }
    });
    let fields = BTreeMap::from([
        ("decision".into(), Value::String(request.decision)),
        (
            "reason".into(),
            request.reason.map(Value::String).unwrap_or(Value::Null),
        ),
    ]);
    let response = state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: "review.decision".into(),
            actor,
            fields,
            evidence: Vec::new(),
            expected_subject: request.expected_subject,
            idempotency_key: None,
        })
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    Ok(Json(response))
}

async fn send_message(
    State(state): State<AppState>,
    Json(request): Json<MessageSendRequest>,
) -> Result<Json<MessageView>, ApiError> {
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
    state.notify.notify_waiters();
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
    state
        .store
        .messages(recipient.as_deref(), query.include_closed)
        .map(Json)
        .map_err(ApiError::internal)
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
    state
        .store
        .messages(None, true)
        .map_err(ApiError::internal)?
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
        "accepted" => "message.accepted",
        "closed" => "message.closed",
        other => {
            return Err(ApiError::bad(St3Error::new(
                "invalid-message-lifecycle",
                format!("message lifecycle `{other}` is not registered"),
            )));
        }
    };
    let actor = if let Some(actor) = request.actor {
        Some(normalize_message_party(&actor))
    } else {
        state
            .store
            .messages(None, true)
            .map_err(ApiError::internal)?
            .into_iter()
            .find(|message| message.subject == subject)
            .map(|message| message.to)
    };
    let record = state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: kind.into(),
            actor,
            fields: BTreeMap::from([("status".into(), Value::String(request.lifecycle))]),
            evidence: request.evidence,
            expected_subject: request.expected_subject,
            idempotency_key: Some(request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    Ok(Json(record))
}

async fn close_message(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
) -> Result<Json<ClaimRecord>, ApiError> {
    let subject = message_subject(&subject);
    let idempotency_key = format!("message-close:{subject}");
    let actor = state
        .store
        .messages(None, true)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|message| message.subject == subject)
        .map(|message| message.to);
    let response = state
        .store
        .append_claim(&ClaimInput {
            subject,
            kind: "message.closed".into(),
            actor,
            fields: BTreeMap::from([("status".into(), Value::String("closed".into()))]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(idempotency_key),
        })
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    Ok(Json(response))
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
    scope: Option<String>,
    at_index: Option<u64>,
}

async fn status(
    State(state): State<AppState>,
    Query(query): Query<StatusQuery>,
) -> Result<Json<StatusResponse>, ApiError> {
    state
        .store
        .status_at(
            query.subject.as_deref(),
            query.scope.as_deref(),
            query.at_index,
        )
        .map(Json)
        .map_err(ApiError::internal)
}

#[derive(Deserialize)]
struct EventQuery {
    #[serde(default, alias = "after_index")]
    after: u64,
    subject: Option<String>,
    scope: Option<String>,
}

async fn events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<EventRecord>>, ApiError> {
    let current = state
        .store
        .events_after_filtered(
            query.after,
            query.subject.as_deref(),
            query.scope.as_deref(),
        )
        .map_err(ApiError::internal)?;
    if !current.is_empty() {
        return Ok(Json(current));
    }
    let _ = tokio::time::timeout(Duration::from_secs(30), state.notify.notified()).await;
    state
        .store
        .events_after_filtered(
            query.after,
            query.subject.as_deref(),
            query.scope.as_deref(),
        )
        .map(Json)
        .map_err(ApiError::internal)
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
    driver_body.push_str(
        "prompt \"Assist the user in this worktree. Use st3 message ls, read, reply, and archive for graph messages.\"\n",
    );
    let kdl = format!(
        "subgraph {{\n  agent {bus_id:?} {{\n    identity {bus_id:?}\n    workspace {:?}\n    {driver} {{\n{driver_body}    }}\n  }}\n}}\n",
        request.worktree
    );
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let expected_subjects =
        BTreeMap::from([(request.subject.clone(), request.expected_subject.clone())]);
    let applied = state
        .store
        .apply(&intent, &expected_subjects, &request.idempotency_key)
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    let runtime_id = intent
        .subjects
        .get(&request.subject)
        .and_then(|subject| subject.member.as_ref())
        .map(|member| member.runtime_id.clone())
        .ok_or_else(|| ApiError::internal("quick agent normalization lost its member"))?;
    let ready = state
        .store
        .latest_claim(&request.subject, Some("harness.ready"))
        .map_err(ApiError::internal)?
        .is_some();
    Ok(QuickAgentResponse {
        subject: request.subject,
        runtime_id,
        event_cursor: applied.store_index,
        incarnation_id: None,
        ready,
    })
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
    let workspace = state
        .state_dir
        .join("evals")
        .join(&request.bundle_hash[..16]);
    hydrate_cell(&request.bundle, &workspace).map_err(ApiError::internal)?;
    let eval_file = workspace.join("eval.kdl");
    let kdl = fs::read_to_string(&eval_file)
        .map_err(|error| ApiError::internal(format!("read {}: {error}", eval_file.display())))?;
    let kdl = kdl.replace("${EVAL_ROOT}", &workspace.to_string_lossy());
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    stage_eval_documents(&state, &workspace, &intent).map_err(ApiError::bad)?;
    let plan = state
        .store
        .plan(
            &intent,
            crate::model::IntentInput {
                kdl: kdl.clone(),
                source_name: Some(eval_file.display().to_string()),
            },
        )
        .map_err(ApiError::bad)?;
    if !plan.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "eval-blocked",
            plan.blockers.join("; "),
        )));
    }
    let applied = state
        .store
        .apply(
            &intent,
            &plan.subject_tokens,
            &format!("eval:{}:{}", request.name, request.bundle_hash),
        )
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    let scope = intent
        .subjects
        .values()
        .find(|subject| subject.kind == "scope")
        .map(|subject| subject.subject.clone())
        .unwrap_or_else(|| format!("scope/eval/{}", request.name));
    Ok(Json(EvalStartResponse {
        scope,
        event_cursor: applied.store_index,
    }))
}

async fn get_eval(
    State(state): State<AppState>,
    AxumPath(scope): AxumPath<String>,
) -> Result<Json<EvalStatus>, ApiError> {
    let scope = if scope.starts_with("scope/") {
        scope
    } else {
        format!("scope/{scope}")
    };
    let status = state
        .store
        .status_at(None, Some(&scope), None)
        .map_err(ApiError::internal)?;
    if status.subjects.is_empty() {
        return Err(ApiError::not_found(format!(
            "eval scope `{scope}` does not exist"
        )));
    }
    let scope_actual = status
        .subjects
        .iter()
        .find(|subject| subject.subject == scope)
        .and_then(|subject| subject.actual.as_ref());
    let verdict = scope_actual
        .and_then(|actual| actual.get("verdict"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let cleanup = if scope_actual
        .and_then(|actual| actual.get("status"))
        .and_then(Value::as_str)
        == Some("stopped")
    {
        "complete"
    } else {
        "pending"
    };
    let active_checkpoint = status
        .subjects
        .iter()
        .filter(|subject| subject.kind.as_deref() == Some("checkpoint-stage"))
        .filter_map(|subject| {
            let ordinal = subject.actual.as_ref()?.get("ordinal")?.as_u64()?;
            let name = subject.desired.as_ref()?.get("name")?.as_str()?.to_owned();
            Some((ordinal, name))
        })
        .max_by_key(|(ordinal, _)| *ordinal)
        .map(|(_, name)| name);
    let lifecycle = if verdict.is_none() {
        "running"
    } else if cleanup == "complete" {
        "complete"
    } else {
        "cleaning"
    };
    Ok(Json(EvalStatus {
        scope,
        lifecycle: lifecycle.into(),
        active_checkpoint,
        verdict,
        cleanup: cleanup.into(),
        store_index: status.store_index,
    }))
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
    expected_incarnation: &str,
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
    if incarnation_id != expected_incarnation {
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
        .and_then(|desired| desired.member)
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no member intent")))?;
    Ok(LiveSession {
        runtime_id: runtime_id.into(),
        incarnation_id: incarnation_id.into(),
        terminal: member.terminal,
        driver: member.driver,
    })
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
    let session = live_session(&state, &subject, &request.expected_incarnation)?;
    if !session.terminal || session.driver.as_deref() != Some("claude") {
        return Err(ApiError::bad(St3Error::new(
            "unsupported-capability",
            "context clear requires a terminal Claude driver in st3 v1",
        )));
    }
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "context.clear.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("status".into(), Value::String("requested".into())),
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
            idempotency_key: Some(format!(
                "session-control-request:context-clear:{subject}:{}",
                request.idempotency_key
            )),
        })
        .map_err(ApiError::bad)?;
    let effect = st_runtime::PtyRuntime::new(state.pty_root.clone()).send_line_if(
        &session.runtime_id,
        "/clear",
        Some(&session.incarnation_id),
    );
    finish_session_control(
        &state,
        &subject,
        "context.clear.result",
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
    let session = live_session(&state, &subject, &request.expected_incarnation)?;
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "session.signal.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("status".into(), Value::String("requested".into())),
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
            idempotency_key: Some(format!(
                "session-control-request:signal:{subject}:{}",
                request.idempotency_key
            )),
        })
        .map_err(ApiError::bad)?;
    let effect = if session.terminal {
        st_runtime::PtyRuntime::new(state.pty_root.clone()).signal_if(
            &session.runtime_id,
            Some(&session.incarnation_id),
            signal,
        )
    } else {
        st_runtime::ExecRuntime::new(state.state_dir.join("exec"), state.state_dir.join("logs"))
            .signal_if(&session.runtime_id, Some(&session.incarnation_id), signal)
    };
    finish_session_control(
        &state,
        &subject,
        "session.signal.result",
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
    let status = if effect.is_ok() {
        "succeeded"
    } else {
        "failed"
    };
    let reason = effect.as_ref().err().map(ToString::to_string);
    let result = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.into(),
            kind: kind.into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("status".into(), Value::String(status.into())),
                (
                    "runtime_id".into(),
                    Value::String(session.runtime_id.clone()),
                ),
                (
                    "incarnation_id".into(),
                    Value::String(session.incarnation_id.clone()),
                ),
                (
                    "reason".into(),
                    reason.clone().map(Value::String).unwrap_or(Value::Null),
                ),
            ]),
            evidence: vec![request.id.clone()],
            expected_subject: None,
            idempotency_key: Some(result_key.into()),
        })
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    if let Some(reason) = reason {
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
    let status = state
        .store
        .status(Some(&subject))
        .map_err(ApiError::internal)?;
    let actual = status
        .subjects
        .first()
        .and_then(|subject| subject.actual.as_ref())
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no live session")))?;
    let fields = actual.get("fields").unwrap_or(actual);
    let runtime_id = fields
        .get("runtime_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::not_found(format!("subject `{subject}` has no runtime")))?;
    let incarnation_id = fields
        .get("incarnation_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let (capability, expires_at_unix_ms) = state
        .store
        .issue_capability("terminal", &subject, incarnation_id.as_deref(), 30_000)
        .map_err(ApiError::internal)?;
    Ok(Json(Attachment {
        websocket_path: format!(
            "/v1/sessions/terminal/{}?capability={}",
            urlencoding::encode(&subject),
            capability
        ),
        subject,
        runtime_id: runtime_id.into(),
        incarnation_id,
        capability,
        expires_at_unix_ms,
    }))
}

async fn post_judgement(
    State(state): State<AppState>,
    Json(request): Json<JudgementRequest>,
) -> Result<Json<ClaimRecord>, ApiError> {
    if !matches!(request.verdict.as_str(), "pass" | "fail") {
        return Err(ApiError::bad(St3Error::new(
            "invalid-judgement",
            "a judgement verdict must be pass or fail",
        )));
    }
    let capability = state
        .store
        .consume_capability(&request.operation_capability, "judgement")
        .map_err(ApiError::bad)?;
    if capability.used {
        let prior = state
            .store
            .latest_claim(&capability.subject, Some("judgement.result"))
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "used-capability",
                    "the judgement capability was already consumed",
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
            "the judgement capability was already used for another verdict",
        )));
    }
    let response = state
        .store
        .append_claim(&ClaimInput {
            subject: capability.subject,
            kind: "judgement.result".into(),
            actor: None,
            fields: BTreeMap::from([
                ("verdict".into(), Value::String(request.verdict)),
                ("reason".into(), Value::String(request.reason)),
            ]),
            evidence: request.evidence,
            expected_subject: None,
            idempotency_key: Some(request.idempotency_key),
        })
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
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
    let current_incarnation = fields.get("incarnation_id").and_then(Value::as_str);
    if capability.incarnation_id.as_deref() != current_incarnation {
        return Err(ApiError::bad(St3Error::new(
            "stale-incarnation",
            "the terminal incarnation changed before attachment",
        )));
    }
    Ok(websocket.on_upgrade(move |socket| terminal_proxy(socket, state, subject, runtime_id)))
}

async fn terminal_proxy(socket: WebSocket, state: AppState, subject: String, runtime_id: String) {
    let mut output = match tokio::process::Command::new("pty")
        .env("PTY_ROOT", &state.pty_root)
        .args(["peek", "-f", &runtime_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return,
    };
    let Some(mut output_stream) = output.stdout.take() else {
        return;
    };
    let (mut writer, mut reader) = socket.split();
    let store = state.store.clone();
    let input_subject = subject.clone();
    let input_runtime = runtime_id.clone();
    let pty_root = state.pty_root.clone();
    let input = tokio::spawn(async move {
        let runtime = st_runtime::PtyRuntime::new(pty_root);
        let mut sequence = 0_u64;
        while let Some(Ok(message)) = reader.next().await {
            let bytes = match message {
                WsMessage::Binary(bytes) => bytes.to_vec(),
                WsMessage::Text(text) => text.as_bytes().to_vec(),
                WsMessage::Close(_) => break,
                WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
            };
            sequence = sequence.saturating_add(1);
            let byte_hash = hex::encode(Sha256::digest(&bytes));
            let Ok(blob_hash) = store.put_blob(&bytes) else {
                break;
            };
            let request = store.append_claim(&ClaimInput {
                subject: input_subject.clone(),
                kind: "terminal.input.requested".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("sequence".into(), Value::from(sequence)),
                    ("byte_hash".into(), Value::String(byte_hash)),
                    ("blob_hash".into(), Value::String(blob_hash)),
                    ("runtime_id".into(), Value::String(input_runtime.clone())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!(
                    "terminal:{input_subject}:{input_runtime}:{sequence}"
                )),
            });
            let Ok(request) = request else {
                break;
            };
            if runtime.send_raw(&input_runtime, &bytes).is_err() {
                break;
            }
            let _ = store.append_claim(&ClaimInput {
                subject: input_subject.clone(),
                kind: "terminal.input.result".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("sequence".into(), Value::from(sequence)),
                    ("status".into(), Value::String("written".into())),
                ]),
                evidence: vec![request.id],
                expected_subject: None,
                idempotency_key: Some(format!(
                    "terminal-result:{input_subject}:{input_runtime}:{sequence}"
                )),
            });
        }
    });
    use tokio::io::AsyncReadExt as _;
    let mut bytes = vec![0_u8; 8192];
    loop {
        match output_stream.read(&mut bytes).await {
            Ok(0) | Err(_) => break,
            Ok(count)
                if writer
                    .send(WsMessage::Binary(bytes[..count].to_vec().into()))
                    .await
                    .is_err() =>
            {
                break;
            }
            Ok(_) => {}
        }
    }
    input.abort();
    let _ = output.kill().await;
}

#[derive(Deserialize)]
struct PeerExportQuery {
    #[serde(default)]
    after: u64,
}

async fn export_peer(
    State(state): State<AppState>,
    Query(query): Query<PeerExportQuery>,
) -> Result<Json<ReplicationBatch>, ApiError> {
    state
        .store
        .export_replication(query.after)
        .map(Json)
        .map_err(ApiError::internal)
}

async fn import_peer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(batch): Json<ReplicationBatch>,
) -> Result<Json<ReplicationResponse>, ApiError> {
    let relay = headers
        .get("x-st3-peer")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(&batch.peer);
    if !state.trusted_peers.contains(relay) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "unconfigured-peer".into(),
            message: format!("peer `{relay}` is not configured as trusted"),
            details: serde_json::Map::new(),
        });
    }
    let response = state
        .store
        .import_replication(relay, &batch)
        .map_err(ApiError::bad)?;
    state.notify.notify_waiters();
    Ok(Json(response))
}

async fn query_peer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(query): Json<ReplicationQuery>,
) -> Result<Json<ReplicationBatch>, ApiError> {
    let relay = headers
        .get("x-st3-peer")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError {
            status: StatusCode::FORBIDDEN,
            code: "unconfigured-peer".into(),
            message: "the replication query has no configured peer label".into(),
            details: serde_json::Map::new(),
        })?;
    if !state.trusted_peers.contains(relay) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "unconfigured-peer".into(),
            message: format!("peer `{relay}` is not configured as trusted"),
            details: serde_json::Map::new(),
        });
    }
    state
        .store
        .export_replication_for_heads(&query.replica_heads)
        .map(Json)
        .map_err(ApiError::internal)
}

#[derive(Deserialize)]
struct PeerCursorQuery {
    peer: String,
}

async fn peer_cursor(
    State(state): State<AppState>,
    Query(query): Query<PeerCursorQuery>,
) -> Result<Json<Value>, ApiError> {
    if query.peer != state.node && !state.trusted_peers.contains(&query.peer) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "unconfigured-peer".into(),
            message: format!("peer `{}` is not configured as trusted", query.peer),
            details: serde_json::Map::new(),
        });
    }
    Ok(Json(json!({
        "peer": query.peer,
        "accepted_through": state.store.peer_cursor(&query.peer).map_err(ApiError::internal)?,
    })))
}

fn normalize_message_party(value: &str) -> String {
    if value == "requester" || value.contains('/') {
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

    fn state(root: &Path) -> AppState {
        AppState {
            store: Arc::new(Store::open_memory("node").unwrap()),
            notify: Arc::new(Notify::new()),
            node: "node".into(),
            state_dir: root.to_path_buf(),
            pty_root: root.join("pty"),
            trusted_peers: Default::default(),
        }
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
        assert_eq!(envelope["api_version"], "st3.v1");
        assert_eq!(envelope["snapshot_host"], "node");
        assert!(envelope["request_id"].as_str().unwrap().contains('-'));
        assert!(envelope["store_index"].is_u64());
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
        assert_eq!(envelope["api_version"], "st3.v1");
        assert_eq!(envelope["snapshot_host"], "node");
        assert!(envelope["request_id"].as_str().unwrap().contains('-'));
        assert!(envelope["store_index"].is_u64());
        let value = if status.is_success() {
            envelope["value"].clone()
        } else {
            envelope
        };
        (status, value)
    }

    #[tokio::test]
    async fn plan_resolves_a_bare_document_to_immutable_bytes() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let version = state
            .store
            .put_document("doc/task", b"hello", &None, "document")
            .unwrap();
        let app = router(state);
        let (status, body) = json_request(
            app.clone(),
            "/v1/intent/plan",
            serde_json::to_value(PlanRequest {
                intent: crate::model::IntentInput {
                    kdl: r#"subgraph { message "task" { to "worker"; content "doc/task" } }"#
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
    async fn eval_upload_posts_its_staged_documents_before_apply() {
        let root = tempfile::tempdir().unwrap();
        let cell = tempfile::tempdir().unwrap();
        let bytes = b"hello from the eval";
        let hash = hex::encode(Sha256::digest(bytes));
        fs::create_dir_all(cell.path().join(".st3-documents")).unwrap();
        fs::write(cell.path().join(".st3-documents").join(&hash), bytes).unwrap();
        fs::write(
            cell.path().join("eval.kdl"),
            format!(
                r#"
subgraph {{
  person "worker"
  checkpoints "eval/demo" scope="scope/eval/demo" {{
    checkpoint "The document exists" {{
      subgraph {{
        message "task" {{ to "person/worker"; content "doc/evals/demo/task@{hash}" }}
      }}
      judges {{ has "doc/evals/demo/task@{hash}" "hello" }}
    }}
    checkpoint "The temporary eval scope is empty" {{
      subgraph {{ scope "eval/demo" {{ stop }} }}
      judges {{ empty "scope/eval/demo" }}
    }}
  }}
}}
"#
            ),
        )
        .unwrap();
        let bundle = crate::archive::archive_cell(cell.path()).unwrap();
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
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["scope"], "scope/eval/demo");
        assert_eq!(
            store
                .get_document("doc/evals/demo/task", &hash)
                .unwrap()
                .unwrap(),
            bytes
        );
        let (status, eval) = get_request(app, "/v1/evals/eval/demo").await;
        assert_eq!(status, StatusCode::OK, "{eval}");
        assert_eq!(eval["scope"], "scope/eval/demo");
        assert_eq!(eval["lifecycle"], "running");
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
                    kind: "transport.peer".into(),
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
        let (status, second) =
            get_request(app, &format!("/v1/claims?limit=1&after_index={cursor}")).await;
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_eq!(second["claims"].as_array().unwrap().len(), 1);
        assert!(second["next_cursor"].is_null());
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
            expected_subject: Vec::new(),
            idempotency_key: "quick-claude".into(),
        };
        let (status, created) = json_request(
            app.clone(),
            "/v1/claude",
            serde_json::to_value(request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        assert_eq!(created["subject"], "agent/node.quick");
        let desired = store.desired_subjects().unwrap();
        let member = desired
            .iter()
            .find(|subject| subject.subject == "agent/node.quick")
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

        store
            .append_claim(&ClaimInput {
                subject: "agent/node.quick".into(),
                kind: "member.observed".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("status".into(), Value::String("running".into())),
                    ("runtime_id".into(), Value::String("node.quick".into())),
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
            "/v1/sessions/attach/agent/node.quick",
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

        let (status, error) = json_request(
            app,
            "/v1/sessions/agent%2Fnode.quick/context/clear",
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
}
