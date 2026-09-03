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
use base64::Engine as _;
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

use crate::archive::hydrate_eval;
use crate::graph::{parse_intent, resolve_document_references};
use crate::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, ClaimInput, ClaimRecord, ClaimsPage,
    ContextClearRequest, DoctorCheck, DoctorReport, DocumentPutRequest, DocumentVersion,
    EvalStartRequest, EvalStartResponse, EvalStatus, EventRecord, GateResultRequest,
    MessageLifecycleRequest, MessageSendRequest, MessageView, PlanOutputView,
    PlanProductionRequest, PlanRequest, PlanResponse, PlanRevisionRequest, PlanRunRequest,
    PlanRunView, PlanningApprovalRequest, PlanningCancelRequest, PlanningCandidateSubmitRequest,
    PlanningRevisionRequest, PlanningSessionStartRequest, PlanningSessionView, QuickAgentRequest,
    QuickAgentResponse, ReplicationBatch, ReplicationQuery, ReplicationResponse, ReviewRequest,
    SessionControlResponse, SessionInputMode, SessionInputRequest, SessionLogChunk, SessionScreen,
    SessionSignalRequest, St3Error, StatusResponse, StepRunView, WorkRequest,
};
use crate::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub notify: Arc<Notify>,
    pub event_notify: Arc<Notify>,
    pub node: String,
    pub state_dir: std::path::PathBuf,
    pub pty_root: std::path::PathBuf,
    pub trusted_peers: std::collections::BTreeSet<String>,
}

fn signal_changed(state: &AppState) {
    state.notify.notify_one();
    state.event_notify.notify_waiters();
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
            | "stale-planning-preview" => StatusCode::CONFLICT,
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
        .route("/v1/planning-sessions", post(start_planning_session))
        .route("/v1/planning-sessions/{id}", get(get_planning_session))
        .route(
            "/v1/planning-sessions/{id}/submit",
            post(submit_planning_candidate),
        )
        .route(
            "/v1/planning-sessions/{id}/preview",
            post(preview_planning_candidate),
        )
        .route(
            "/v1/planning-sessions/{id}/revise",
            post(revise_planning_session),
        )
        .route(
            "/v1/planning-sessions/{id}/approve",
            post(approve_planning_session),
        )
        .route(
            "/v1/planning-sessions/{id}/cancel",
            post(cancel_planning_session),
        )
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
        .route("/v1/doctor", get(doctor))
        .route("/v1/claude", post(quick_claude))
        .route("/v1/codex", post(quick_codex))
        .route("/v1/evals", post(start_eval))
        .route("/v1/evals/{*scope}", get(get_eval))
        .route("/v1/plan-runs", get(list_plan_runs).post(start_plan_run))
        .route("/v1/plan-runs/{run}/revision", post(revise_plan_run))
        .route("/v1/plan-runs/{run}", get(get_plan_run))
        .route("/v1/work", get(list_work))
        .route("/v1/work/plan/{*subject}", post(publish_work_plan))
        .route("/v1/work/{action}/{*subject}", post(post_work_action))
        .route("/v1/gate-results", post(post_gate_result))
        .route("/v1/sessions/{subject}/context/clear", post(clear_context))
        .route("/v1/sessions/{subject}/signal", post(signal_session))
        .route("/v1/sessions/input/{*subject}", post(input_session))
        .route("/v1/sessions/logs/{*subject}", get(logs_session))
        .route("/v1/sessions/screen/{*subject}", get(screen_session))
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
    let pty_snapshot = st_runtime::PtyRuntime::new(state.pty_root.clone()).snapshot();
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
            }) && subject.gap.is_some()
        })
        .map(|subject| {
            format!(
                "{}: {}",
                subject.subject,
                subject.gap.as_deref().unwrap_or("not ready")
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
            "all desired native drivers have no current graph gap".into()
        } else {
            driver_gaps.join("; ")
        },
    });
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

async fn start_planning_session(
    State(state): State<AppState>,
    Json(request): Json<PlanningSessionStartRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let request_text = std::str::from_utf8(&request.request).map_err(|_| {
        ApiError::bad(St3Error::new(
            "planning-request-not-text",
            "a planning request must contain valid UTF-8",
        ))
    })?;
    if request_text.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-planning-request",
            "a planning request cannot be empty",
        )));
    }
    crate::plan::validate_plan_id(&request.plan).map_err(ApiError::bad)?;
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
    let planner = format!("agent/{}.planner.{}", state.node, &id[..10]);
    let request_reference = format!("{}@{}", request_document.name, request_document.hash);
    let session = state
        .store
        .create_planning_session(
            &id,
            &request.plan,
            &request_reference,
            &request.workspace,
            &requester,
            &planner,
        )
        .map_err(ApiError::bad)?;
    let expected_subject = state
        .store
        .selected_desired_token(&planner)
        .map_err(ApiError::internal)?
        .into_iter()
        .collect();
    let prompt = format!(
        "You are the durable Codex planner for planning session {id}. Use `st3 message ls`, read and archive the native Small Talk request, and use `st3 doc get` to read the immutable document reference in the message content. Write one Markdown plan and one complete version 2 KDL plan. The KDL plan ID must be `{}` and its state must be ready. Submit both files with `st3 plan submit {id} --markdown FILE --kdl FILE`. Use temporary files outside the workspace, and remove them after submission. Do not change the workspace. Do not publish or run the plan. Stay available for revision messages until approval or cancellation.",
        request.plan
    );
    quick_agent(
        &state,
        QuickAgentRequest {
            subject: planner.clone(),
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
    send_planning_message(
        &state,
        &format!("planning-request:{id}"),
        &requester,
        &planner,
        &request_reference,
        "Planning request",
    )?;
    record_planning_event(
        &state,
        &session,
        "planning-session.started",
        Some(&requester),
        BTreeMap::from([
            (
                "plan".into(),
                Value::String(format!("plan/{}", session.plan)),
            ),
            ("request".into(), Value::String(request_reference)),
            ("planner".into(), Value::String(session.planner.clone())),
            ("workspace".into(), Value::String(session.workspace.clone())),
        ]),
        &format!("{}:started", request.idempotency_key),
    )?;
    signal_changed(&state);
    Ok(Json(session))
}

async fn get_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    state
        .store
        .planning_session(&id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("planning session `{id}` does not exist")))
}

async fn submit_planning_candidate(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningCandidateSubmitRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    let markdown = std::str::from_utf8(&request.markdown).map_err(|_| {
        ApiError::bad(St3Error::new(
            "planning-markdown-not-text",
            "planning Markdown must contain valid UTF-8",
        ))
    })?;
    if markdown.trim().is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "empty-planning-markdown",
            "planning Markdown cannot be empty",
        )));
    }
    let kdl = std::str::from_utf8(&request.kdl).map_err(|_| {
        ApiError::bad(St3Error::new(
            "planning-kdl-not-text",
            "planning KDL must contain valid UTF-8",
        ))
    })?;
    let (intent, _) = plan_source(&state, kdl, None)?;
    if !intent.subjects.is_empty()
        || !intent.checkpoints.is_empty()
        || intent.plans.len() != 1
        || !intent.plans.contains_key(&session.plan)
    {
        return Err(ApiError::bad(St3Error::new(
            "wrong-planning-plan",
            format!(
                "a candidate must contain only ready plan `{}` and no immediate desired state",
                session.plan
            ),
        )));
    }
    let plan = &intent.plans[&session.plan];
    if plan.state != crate::model::PlanState::Ready {
        return Err(ApiError::bad(St3Error::new(
            "planning-plan-not-ready",
            "a planning candidate must contain a ready plan",
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
            &format!("{}@{}", markdown_document.name, markdown_document.hash),
            &format!("{}@{}", kdl_document.name, kdl_document.hash),
            &plan.revision,
        )
        .map_err(ApiError::bad)?;
    let candidate = response
        .candidate
        .as_ref()
        .expect("the submitted planning candidate is visible");
    record_planning_event(
        &state,
        &response,
        "planning-session.candidate-submitted",
        Some(&request.actor),
        BTreeMap::from([
            ("candidate_revision".into(), Value::from(candidate.revision)),
            ("markdown".into(), Value::String(candidate.markdown.clone())),
            ("kdl".into(), Value::String(candidate.kdl.clone())),
            (
                "plan_revision".into(),
                Value::String(candidate.plan_revision.clone()),
            ),
        ]),
        &format!("planning-candidate:{}:{}", response.id, candidate.revision),
    )?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn preview_planning_candidate(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    if session.status != "review" {
        return Err(ApiError::bad(St3Error::new(
            "planning-session-not-reviewable",
            format!("planning session `{}` is {}", session.id, session.status),
        )));
    }
    let candidate = session.candidate.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-planning-candidate",
            "the planning session has no candidate",
        ))
    })?;
    let kdl = planning_document_text(&state, &candidate.kdl)?;
    let (intent, plan_response) = plan_source(&state, &kdl, None)?;
    let plan = &intent.plans[&session.plan];
    let graph = render_planning_graph(plan);
    let diff = render_planning_diff(&plan_response);
    let hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&json!({
            "candidate_revision": candidate.revision,
            "markdown": candidate.markdown,
            "kdl": candidate.kdl,
            "plan_revision": candidate.plan_revision,
            "graph": graph,
            "diff": diff,
            "plan": plan_response,
        }))
        .map_err(ApiError::internal)?,
    ));
    let response = state
        .store
        .save_planning_preview(
            &session.id,
            candidate.revision,
            &hash,
            &graph,
            &diff,
            &plan_response,
        )
        .map_err(ApiError::bad)?;
    let preview = response
        .preview
        .as_ref()
        .expect("the saved planning preview is visible");
    record_planning_event(
        &state,
        &response,
        "planning-session.previewed",
        Some(&response.requester),
        BTreeMap::from([
            (
                "candidate_revision".into(),
                Value::from(preview.candidate_revision),
            ),
            ("preview_hash".into(), Value::String(preview.hash.clone())),
            ("store_index".into(), Value::from(preview.store_index)),
        ]),
        &format!("planning-preview:{}:{}", response.id, preview.hash),
    )?;
    signal_changed(&state);
    Ok(Json(response))
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
            "empty-planning-feedback",
            "planning feedback cannot be empty",
        )));
    }
    std::str::from_utf8(&request.feedback).map_err(|_| {
        ApiError::bad(St3Error::new(
            "planning-feedback-not-text",
            "planning feedback must contain valid UTF-8",
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
        "Planning revision requested",
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
            "planning-session-not-reviewable",
            format!("planning session `{}` is {}", session.id, session.status),
        )));
    }
    let candidate = session.candidate.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-planning-candidate",
            "the planning session has no candidate",
        ))
    })?;
    let preview = session.preview.as_ref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-planning-preview",
            "preview the candidate before approval",
        ))
    })?;
    if preview.hash != request.preview_hash || preview.candidate_revision != candidate.revision {
        return Err(ApiError::bad(St3Error::new(
            "stale-planning-preview",
            "the approval does not name the current preview",
        )));
    }
    if session.status == "approved" {
        if session.published_revision.as_deref() == Some(candidate.plan_revision.as_str()) {
            return Ok(Json(session));
        }
        return Err(ApiError::internal(format!(
            "planning session `{}` has inconsistent approved revision state",
            session.id
        )));
    }
    if !preview.plan.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "planning-preview-blocked",
            preview.plan.blockers.join("; "),
        )));
    }
    let intent =
        parse_intent(&preview.plan.resolved_intent.kdl, &state.node).map_err(ApiError::bad)?;
    let approval_key = format!("planning-approval:{}:{}", session.id, preview.hash);
    state
        .store
        .apply(
            &intent,
            &preview.plan.subject_tokens,
            &format!("{approval_key}:publish"),
        )
        .map_err(ApiError::bad)?;
    state
        .store
        .append_claim(&ClaimInput {
            subject: format!("plan/{}", session.plan),
            kind: "plan.documents".into(),
            actor: Some(normalize_message_party(&request.actor)),
            fields: BTreeMap::from([
                (
                    "planning_session".into(),
                    Value::String(session.subject.clone()),
                ),
                ("markdown".into(), Value::String(candidate.markdown.clone())),
                ("kdl".into(), Value::String(candidate.kdl.clone())),
                (
                    "revision".into(),
                    Value::String(candidate.plan_revision.clone()),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(format!("{approval_key}:documents")),
        })
        .map_err(ApiError::bad)?;
    let response = state
        .store
        .finish_planning_session(
            &session.id,
            &request.actor,
            "approved",
            Some(&candidate.plan_revision),
        )
        .map_err(ApiError::bad)?;
    record_planning_event(
        &state,
        &response,
        "planning-session.approved",
        Some(&request.actor),
        BTreeMap::from([
            (
                "preview_hash".into(),
                Value::String(request.preview_hash.clone()),
            ),
            (
                "plan_revision".into(),
                Value::String(candidate.plan_revision.clone()),
            ),
            ("markdown".into(), Value::String(candidate.markdown.clone())),
            ("kdl".into(), Value::String(candidate.kdl.clone())),
        ]),
        &format!("{approval_key}:event"),
    )?;
    stop_planning_agent(&state, &session.planner, &approval_key)?;
    signal_changed(&state);
    Ok(Json(response))
}

async fn cancel_planning_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<PlanningCancelRequest>,
) -> Result<Json<PlanningSessionView>, ApiError> {
    let session = required_planning_session(&state, &id)?;
    authorize_planning_reviewer(&session, &request.actor)?;
    if session.status == "cancelled" {
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
        BTreeMap::from([(
            "reason".into(),
            request
                .reason
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        )]),
        &format!("{}:cancelled", request.idempotency_key),
    )?;
    stop_planning_agent(&state, &session.planner, &request.idempotency_key)?;
    signal_changed(&state);
    Ok(Json(response))
}

fn required_planning_session(state: &AppState, id: &str) -> Result<PlanningSessionView, ApiError> {
    state
        .store
        .planning_session(id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("planning session `{id}` does not exist")))
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
        "planning-review-not-authorized",
        format!("`{actor}` cannot review planning session `{}`", session.id),
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

fn plan_source(
    state: &AppState,
    kdl: &str,
    at_index: Option<u64>,
) -> Result<(crate::model::NormalizedIntent, PlanResponse), ApiError> {
    let initial = parse_intent(kdl, &state.node).map_err(ApiError::bad)?;
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, at_index)
        .map_err(ApiError::internal)?;
    let resolved_kdl = resolve_document_references(kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let response = state
        .store
        .plan_at(
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

fn render_planning_graph(plan: &crate::model::PlanSpec) -> String {
    fn append(plan: &crate::model::PlanSpec, indent: &str, lines: &mut Vec<String>) {
        for id in &plan.display_order {
            let step = &plan.steps[id];
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
            lines.push(format!("{indent}{} [{}]", step.path, suffix));
            for product in &step.products {
                lines.push(format!("{indent}  produces {}", product.subject));
            }
            for gate in &step.gates {
                lines.push(format!("{indent}  gate {}", crate::graph::gate_name(gate)));
            }
            if let Some(nested) = &step.nested_plan {
                append(nested, &format!("{indent}  "), lines);
            }
        }
    }
    let mut lines = vec![format!("plan/{}", plan.id)];
    for baseline in &plan.baselines {
        lines.push(format!("  baseline {}", baseline.name));
    }
    for product in &plan.products {
        lines.push(format!("  produces {}", product.subject));
    }
    for gate in &plan.gates {
        lines.push(format!("  gate {}", crate::graph::gate_name(gate)));
    }
    append(plan, "  ", &mut lines);
    lines.join("\n")
}

fn render_planning_diff(plan: &PlanResponse) -> String {
    if plan.changes.is_empty() {
        return "No graph changes.".into();
    }
    plan.changes
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
                    Value::Array(vec![Value::String("planning".into())]),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(key.into()),
        })
        .map_err(ApiError::bad)?;
    Ok(())
}

fn stop_planning_agent(state: &AppState, planner: &str, key: &str) -> Result<(), ApiError> {
    let kdl = format!("version 2\nsubgraph {{ stop {planner:?} }}\n");
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    state
        .store
        .apply_internal(&intent, &format!("{key}:stop-planner"))
        .map_err(ApiError::bad)?;
    Ok(())
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
    signal_changed(&state);
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
    signal_changed(&state);
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
    signal_changed(&state);
    Ok(Json(response))
}

#[derive(Deserialize)]
struct ClaimsQuery {
    subject: Option<String>,
    scope: Option<String>,
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
    state
        .store
        .claims_page(
            query.subject.as_deref(),
            query.scope.as_deref(),
            query.after_index,
            query.before_index,
            matches!(query.order, ClaimsOrder::Desc),
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
    if !matches!(
        request.decision.as_str(),
        "approved" | "rejected" | "revise"
    ) {
        return Err(ApiError::bad(St3Error::new(
            "invalid-review-decision",
            "a review decision must be approved, rejected, or revise",
        )));
    }
    let subject = if subject.starts_with("resource/") || subject.starts_with("step-run/") {
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
    let review_request = if subject.starts_with("step-run/") {
        let review_request = state
            .store
            .latest_claim(&subject, Some("review.requested"))
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "review-not-requested",
                    format!("step run `{subject}` has no pending human review"),
                ))
            })?;
        let step = state
            .store
            .step_run(&subject)
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::bad(St3Error::new(
                    "unknown-step-run",
                    format!("step run `{subject}` does not exist"),
                ))
            })?;
        let run = state
            .store
            .plan_run(&step.run)
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::internal(format!("plan run `{}` does not exist", step.run)))?;
        let request_is_current = review_request
            .body
            .pointer("/fields/plan_revision")
            .and_then(Value::as_str)
            == Some(run.revision.as_str())
            && review_request
                .body
                .pointer("/fields/step_definition")
                .and_then(Value::as_str)
                == Some(step.definition_hash.as_str())
            && review_request
                .body
                .pointer("/fields/attempt")
                .and_then(Value::as_u64)
                == Some(u64::from(step.attempt));
        if !request_is_current {
            return Err(ApiError::bad(St3Error::new(
                "stale-review-request",
                "the human review request does not match the current step attempt",
            )));
        }
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
    let decision = request.decision.clone();
    let mut fields = BTreeMap::from([
        ("decision".into(), Value::String(request.decision)),
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
            subject: subject.clone(),
            kind: "review.decision".into(),
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
    if decision == "revise" && subject.starts_with("step-run/") {
        state
            .store
            .set_step_state(
                &subject,
                "blocked",
                Some("the human reviewer requested a plan revision"),
            )
            .map_err(ApiError::internal)?;
    }
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
    signal_changed(&state);
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
    signal_changed(&state);
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
    let notified = state.event_notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
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
    let _ = tokio::time::timeout(Duration::from_secs(30), notified).await;
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
    let prompt = request.prompt.as_deref().unwrap_or(
        "Assist the user in this worktree. Use st3 message ls, read, reply, and archive for Small Talk messages.",
    );
    if !request.arguments.is_empty() {
        driver_body.push_str("args");
        for argument in &request.arguments {
            driver_body.push_str(&format!(" {argument:?}"));
        }
        driver_body.push('\n');
    }
    driver_body.push_str(&format!("prompt {prompt:?}\n"));
    let kdl = format!(
        "version 2\nsubgraph {{\n  agent {bus_id:?} {{\n    identity {bus_id:?}\n    workspace {:?}\n    harness {driver:?} {{\n{driver_body}    }}\n  }}\n}}\n",
        request.worktree
    );
    let intent = parse_intent(&kdl, &state.node).map_err(ApiError::bad)?;
    let expected_subjects =
        BTreeMap::from([(request.subject.clone(), request.expected_subject.clone())]);
    let applied = state
        .store
        .apply(&intent, &expected_subjects, &request.idempotency_key)
        .map_err(ApiError::bad)?;
    signal_changed(state);
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
    let ready = intent
        .plans
        .values()
        .filter(|plan| plan.state == crate::model::PlanState::Ready)
        .collect::<Vec<_>>();
    if ready.len() != 1 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-eval-plan-count",
            "an eval must contain exactly one ready plan",
        )));
    }
    let run = state
        .store
        .create_plan_run(&PlanRunRequest {
            plan: ready[0].id.clone(),
            revision: Some(ready[0].revision.clone()),
            workspace: workspace.to_string_lossy().into_owned(),
            requester: Some("person/eval-requester".into()),
            mode: Some("eval".into()),
            idempotency_key: format!("eval:{}:{}:{nonce}", request.name, request.bundle_hash),
        })
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    let scope = run
        .run_scope
        .clone()
        .unwrap_or_else(|| format!("scope/eval/{}/{}", request.name, run.id));
    Ok(Json(EvalStartResponse {
        scope,
        event_cursor: applied
            .store_index
            .max(state.store.index().map_err(ApiError::internal)?),
        plan_run: Some(run.subject),
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
    let run = state
        .store
        .plan_run_for_scope(&scope)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("eval scope `{scope}` does not exist")))?;
    let verdict = match run.status.as_str() {
        "completed" => Some("pass".into()),
        "failed" => Some("fail".into()),
        "cancelled" => Some("void".into()),
        _ => None,
    };
    let cleanup = match run.phase.as_str() {
        "final" => "cleaning",
        "terminal" => "complete",
        _ => "pending",
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
        scope,
        plan_run: run.subject,
        lifecycle: run.status,
        phase: run.phase,
        active_steps,
        verdict,
        cleanup: cleanup.into(),
        store_index: state.store.index().map_err(ApiError::internal)?,
    }))
}

async fn start_plan_run(
    State(state): State<AppState>,
    Json(request): Json<PlanRunRequest>,
) -> Result<Json<PlanRunView>, ApiError> {
    let response = state
        .store
        .create_plan_run(&request)
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(response))
}

#[derive(Deserialize)]
struct PlanRunQuery {
    root: String,
}

async fn list_plan_runs(
    State(state): State<AppState>,
    Query(query): Query<PlanRunQuery>,
) -> Result<Json<Vec<PlanRunView>>, ApiError> {
    state
        .store
        .plan_runs_for_root(&query.root)
        .map(Json)
        .map_err(ApiError::internal)
}

async fn get_plan_run(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
) -> Result<Json<PlanRunView>, ApiError> {
    state
        .store
        .plan_run(&run)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("plan run `{run}` does not exist")))
}

async fn revise_plan_run(
    State(state): State<AppState>,
    AxumPath(run): AxumPath<String>,
    Json(request): Json<PlanRevisionRequest>,
) -> Result<Json<PlanRunView>, ApiError> {
    let current = state
        .store
        .plan_run(&run)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("plan run `{run}` does not exist")))?;
    let initial = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    if initial.plans.len() != 1 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-plan-revision-intent",
            "a run revision must contain exactly one plan",
        )));
    }
    let initial_plan = initial.plans.values().next().expect("one plan was checked");
    let allowed_scope = initial_plan.scope_template.as_deref();
    if initial.subjects.iter().any(|(subject, desired)| {
        Some(subject.as_str()) != allowed_scope || desired.kind != "scope"
    }) {
        return Err(ApiError::bad(St3Error::new(
            "invalid-plan-revision-intent",
            "a run revision can contain only its plan and its enclosing scope",
        )));
    }
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, None)
        .map_err(ApiError::internal)?;
    let resolved_kdl =
        resolve_document_references(&request.intent.kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let replacement = intent.plans.values().next().expect("one plan was checked");
    let plan_id = current.plan.strip_prefix("plan/").unwrap_or(&current.plan);
    if replacement.id != plan_id {
        return Err(ApiError::bad(St3Error::new(
            "wrong-plan-revision",
            format!(
                "revision `{}` does not replace plan `{plan_id}`",
                replacement.id
            ),
        )));
    }
    let old = state
        .store
        .plan_spec(plan_id, Some(&current.revision))
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-plan-revision",
                "the current plan revision is unavailable",
            ))
        })?;
    if old.scope_template != replacement.scope_template
        || old.change_policy != replacement.change_policy
        || old.change_authority != replacement.change_authority
    {
        return Err(ApiError::bad(St3Error::new(
            "change-policy-mutation",
            "a run revision cannot change its scope, change policy, or authority",
        )));
    }
    let default_kind = if matches!(old.change_policy, crate::model::ChangePolicy::HumanReview) {
        "person"
    } else {
        "agent"
    };
    let actor = if request.actor.contains('/') {
        request.actor.clone()
    } else {
        format!("{default_kind}/{}", request.actor)
    };
    crate::store::authorize_plan_revision(
        &old,
        replacement,
        &actor,
        &crate::store::plan_run_variables(&current, &replacement.revision),
    )
    .map_err(ApiError::bad)?;
    let mut publication = intent.clone();
    publication.subjects.clear();
    let planned = state
        .store
        .plan(
            &publication,
            crate::model::IntentInput {
                kdl: resolved_kdl,
                source_name: request.intent.source_name,
            },
        )
        .map_err(ApiError::bad)?;
    if !planned.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "plan-revision-blocked",
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
    let revised = state
        .store
        .adopt_plan_revision(
            &run,
            replacement,
            &actor,
            &request.reason,
            &format!("{}:adopt", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(revised))
}

#[derive(Deserialize)]
struct WorkQuery {
    assignee: Option<String>,
    #[serde(default)]
    include_terminal: bool,
}

async fn list_work(
    State(state): State<AppState>,
    Query(query): Query<WorkQuery>,
) -> Result<Json<Vec<StepRunView>>, ApiError> {
    let mut work = state
        .store
        .work(query.assignee.as_deref(), query.include_terminal)
        .map_err(ApiError::internal)?;
    let agents = desired_agent_grouping(&state)?;
    for step in &mut work {
        if let Some(assignee) = &step.assignee {
            step.under = agents.get(assignee).cloned().unwrap_or_default();
        }
    }
    Ok(Json(work))
}

fn desired_agent_grouping(
    state: &AppState,
) -> Result<BTreeMap<String, Vec<crate::model::UnderSpec>>, ApiError> {
    state
        .store
        .desired_subjects()
        .map_err(ApiError::internal)
        .map(|subjects| {
            subjects
                .into_iter()
                .filter(|subject| subject.kind == "agent")
                .map(|subject| (subject.subject, crate::graph::agent_under(&subject.desired)))
                .collect()
        })
}

async fn publish_work_plan(
    State(state): State<AppState>,
    AxumPath(subject): AxumPath<String>,
    Json(request): Json<PlanProductionRequest>,
) -> Result<Json<PlanOutputView>, ApiError> {
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
        .plan_output_authorized(&step.subject, &actor, request.incarnation.as_deref())
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::bad(St3Error::new(
            "work-not-claimed",
            format!(
                "`{actor}` does not hold the plan-producing step `{}` or its nested work",
                step.subject
            ),
        )));
    }
    let run = state
        .store
        .plan_run(&step.run)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("plan run `{}` does not exist", step.run)))?;
    let root = state
        .store
        .plan_spec(
            run.plan.strip_prefix("plan/").unwrap_or(&run.plan),
            Some(&run.revision),
        )
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::bad(St3Error::new(
                "missing-plan-revision",
                "the running plan revision is unavailable",
            ))
        })?;
    let definition = crate::plan::find_step(&root, &step.step).ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "missing-step-definition",
            format!("step `{}` is absent from its plan revision", step.step),
        ))
    })?;
    let expected_plan = definition.produces_plan.as_deref().ok_or_else(|| {
        ApiError::bad(St3Error::new(
            "step-does-not-produce-plan",
            format!("step `{}` does not declare produces-plan", step.step),
        ))
    })?;

    let initial = parse_intent(&request.intent.kdl, &state.node).map_err(ApiError::bad)?;
    if initial.plans.len() != 1 {
        return Err(ApiError::bad(St3Error::new(
            "invalid-plan-output-intent",
            "a plan output must contain exactly one plan",
        )));
    }
    let initial_plan = initial.plans.values().next().expect("one plan was checked");
    let allowed_scope = initial_plan.scope_template.as_deref();
    if initial
        .subjects
        .iter()
        .any(|(name, desired)| Some(name.as_str()) != allowed_scope || desired.kind != "scope")
    {
        return Err(ApiError::bad(St3Error::new(
            "invalid-plan-output-intent",
            "a plan output can contain only its plan and enclosing scope",
        )));
    }
    let bindings = state
        .store
        .document_bindings_at(&initial.document_refs, None)
        .map_err(ApiError::internal)?;
    let resolved_kdl =
        resolve_document_references(&request.intent.kdl, &bindings).map_err(ApiError::bad)?;
    let intent = parse_intent(&resolved_kdl, &state.node).map_err(ApiError::bad)?;
    let plan = intent.plans.values().next().expect("one plan was checked");
    if plan.id != expected_plan || plan.state != crate::model::PlanState::Ready {
        return Err(ApiError::bad(St3Error::new(
            "wrong-plan-output",
            format!(
                "step `{}` must publish ready plan `{expected_plan}`",
                step.step
            ),
        )));
    }
    let mut publication = intent.clone();
    publication.subjects.clear();
    let planned = state
        .store
        .plan(
            &publication,
            crate::model::IntentInput {
                kdl: resolved_kdl,
                source_name: request.intent.source_name,
            },
        )
        .map_err(ApiError::bad)?;
    if !planned.blockers.is_empty() {
        return Err(ApiError::bad(St3Error::new(
            "plan-output-blocked",
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
        .record_plan_output(
            &step.subject,
            &actor,
            request.incarnation.as_deref(),
            expected_plan,
            plan,
            &format!("{}:bind", request.idempotency_key),
        )
        .map_err(ApiError::bad)?;
    signal_changed(&state);
    Ok(Json(output))
}

async fn post_work_action(
    State(state): State<AppState>,
    AxumPath((action, subject)): AxumPath<(String, String)>,
    Json(request): Json<WorkRequest>,
) -> Result<Json<StepRunView>, ApiError> {
    let mut response = state
        .store
        .work_action(&subject, &action, &request)
        .map_err(ApiError::bad)?;
    if let Some(assignee) = &response.assignee {
        response.under = desired_agent_grouping(&state)?
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
    let blob_hash = state.store.put_blob(&bytes).map_err(ApiError::internal)?;
    let mode = match request.mode {
        SessionInputMode::Line => "line",
        SessionInputMode::Raw => "raw",
        SessionInputMode::Key => "key",
    };
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "terminal.input.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("mode".into(), Value::String(mode.into())),
                ("blob_hash".into(), Value::String(blob_hash)),
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
                "session-control-request:input:{subject}:{}",
                request.idempotency_key
            )),
        })
        .map_err(ApiError::bad)?;
    let runtime = st_runtime::PtyRuntime::new(state.pty_root.clone());
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
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "context.clear.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("operation_status".into(), Value::String("requested".into())),
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
    let session = live_session(&state, &subject, Some(&request.expected_incarnation))?;
    let request_claim = state
        .store
        .append_claim(&ClaimInput {
            subject: subject.clone(),
            kind: "session.signal.requested".into(),
            actor: Some("requester".into()),
            fields: BTreeMap::from([
                ("operation_status".into(), Value::String("requested".into())),
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
                ("operation_status".into(), Value::String(status.into())),
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
    signal_changed(state);
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
    let response = state
        .store
        .append_claim(&ClaimInput {
            subject: capability.subject,
            kind: "gate.result".into(),
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
    Ok(websocket.on_upgrade(move |socket| {
        terminal_proxy(socket, state, subject, runtime_id, current_incarnation)
    }))
}

async fn terminal_proxy(
    socket: WebSocket,
    state: AppState,
    subject: String,
    runtime_id: String,
    incarnation_id: Option<String>,
) {
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
            if runtime
                .send_raw_if(&input_runtime, &bytes, incarnation_id.as_deref())
                .is_err()
            {
                break;
            }
            let _ = store.append_claim(&ClaimInput {
                subject: input_subject.clone(),
                kind: "terminal.input.result".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("sequence".into(), Value::from(sequence)),
                    ("operation_status".into(), Value::String("written".into())),
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
    signal_changed(&state);
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
            event_notify: Arc::new(Notify::new()),
            node: "node".into(),
            state_dir: root.to_path_buf(),
            pty_root: root.join("pty"),
            trusted_peers: Default::default(),
        }
    }

    #[tokio::test]
    async fn an_event_waiter_cannot_consume_the_reconciler_signal() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let event = state.event_notify.notified();
        tokio::pin!(event);
        event.as_mut().enable();

        signal_changed(&state);

        tokio::time::timeout(Duration::from_millis(50), event)
            .await
            .expect("the event waiter did not wake");
        tokio::time::timeout(Duration::from_millis(50), state.notify.notified())
            .await
            .expect("the reconciler signal was lost");
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
                    kdl: r#"version 2
subgraph { message "task" { to "worker"; content "doc/task" } }"#
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
            "/v1/planning-sessions",
            serde_json::to_value(PlanningSessionStartRequest {
                plan: "planned/work".into(),
                request: b"Plan a two-step release without changing this workspace.".to_vec(),
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
        assert_eq!(started["requester"], "person/nathan");
        let request_reference = started["request"].as_str().unwrap();
        assert!(request_reference.starts_with("doc/planning/"));
        let (request_name, request_hash) = request_reference.rsplit_once('@').unwrap();
        assert_eq!(
            store
                .get_document(request_name, request_hash)
                .unwrap()
                .unwrap(),
            b"Plan a two-step release without changing this workspace."
        );
        let planner_launch = store
            .desired_subjects()
            .unwrap()
            .into_iter()
            .find(|desired| desired.subject == planner)
            .unwrap()
            .member
            .unwrap()
            .launch;
        assert!(matches!(
            &planner_launch,
            crate::model::LaunchSpec::Argv(arguments)
                if arguments.iter().any(|argument| argument == "--dangerously-bypass-approvals-and-sandbox")
                    && arguments.iter().any(|argument| argument == "--dangerously-bypass-hook-trust")
        ));
        assert!(store.plan_spec("planned/work", None).unwrap().is_none());
        assert!(store.active_plan_runs().unwrap().is_empty());

        let first = br#"
version 2
subgraph {
  plan "planned/work" state="ready" {
    goal "Publish the planned result."
    step "inspect" { goal "Inspect the source." }
    step "change" {
      goal "Make the approved change."
      depends-on { step "inspect" completed }
    }
  }
}
"#;
        let mut side_effect = br#"
version 2
subgraph {
  agent "side-effect" {
    workspace "/tmp"
    harness "codex" { prompt "This must never be published." }
  }
"#
        .to_vec();
        side_effect.extend_from_slice(&first[b"version 2\nsubgraph {\n".len()..]);
        let (status, rejected) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Plan with a side effect".to_vec(),
                kdl: side_effect,
                idempotency_key: "planning-candidate-side-effect".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rejected}");
        assert_eq!(rejected["code"], "wrong-planning-plan");
        assert!(
            store
                .desired_subjects()
                .unwrap()
                .into_iter()
                .all(|desired| desired.subject != "agent/node.side-effect")
        );

        let (status, submitted) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Plan\n\n1. Inspect.\n2. Change.\n".to_vec(),
                kdl: first.to_vec(),
                idempotency_key: "planning-candidate-one".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{submitted}");
        assert_eq!(submitted["candidate"]["revision"], 1);
        assert!(store.plan_spec("planned/work", None).unwrap().is_none());
        let (status, resubmitted) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Plan\n\n1. Inspect.\n2. Change.\n".to_vec(),
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
            &format!("/v1/planning-sessions/{session}/preview"),
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
                .contains("plan/planned/work")
        );

        let (status, revised) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/revise"),
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
        assert!(store.plan_spec("planned/work", None).unwrap().is_none());

        let second = br#"
version 2
subgraph {
  plan "planned/work" state="ready" {
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
}
"#;
        let (status, resubmitted) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/submit"),
            serde_json::to_value(PlanningCandidateSubmitRequest {
                actor: planner.into(),
                markdown: b"# Plan\n\n1. Inspect.\n2. Change.\n3. Verify.\n".to_vec(),
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
            &format!("/v1/planning-sessions/{session}/preview"),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{previewed}");
        let current_hash = previewed["preview"]["hash"].as_str().unwrap().to_owned();
        assert_ne!(current_hash, first_hash);

        let (status, unauthorized) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/intruder".into(),
                preview_hash: current_hash.clone(),
                idempotency_key: "planning-approve-unauthorized".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{unauthorized}");
        assert_eq!(unauthorized["code"], "planning-review-not-authorized");
        assert!(store.plan_spec("planned/work", None).unwrap().is_none());

        let (status, stale) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/nathan".into(),
                preview_hash: first_hash,
                idempotency_key: "planning-approve-stale".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{stale}");
        assert_eq!(stale["code"], "stale-planning-preview");
        assert!(store.plan_spec("planned/work", None).unwrap().is_none());

        let (status, approved) = json_request(
            app.clone(),
            &format!("/v1/planning-sessions/{session}/approve"),
            serde_json::to_value(PlanningApprovalRequest {
                actor: "person/nathan".into(),
                preview_hash: current_hash,
                idempotency_key: "planning-approve-current".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(approved["status"], "approved");
        assert_eq!(
            approved["published_revision"],
            approved["candidate"]["plan_revision"]
        );
        assert!(store.plan_spec("planned/work", None).unwrap().is_some());
        assert!(store.active_plan_runs().unwrap().is_empty());
        assert_eq!(fs::read_to_string(&marker).unwrap(), "unchanged\n");
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);

        let documents = store
            .latest_claim("plan/planned/work", Some("plan.documents"))
            .unwrap()
            .expect("the published plan does not link its documents");
        for field in ["markdown", "kdl"] {
            let reference = documents
                .body
                .pointer(&format!("/fields/{field}"))
                .and_then(Value::as_str)
                .unwrap();
            let (name, hash) = reference.rsplit_once('@').unwrap();
            assert!(store.get_document(name, hash).unwrap().is_some());
        }
        assert_eq!(
            store
                .desired_subjects()
                .unwrap()
                .into_iter()
                .find(|desired| desired.subject == planner)
                .unwrap()
                .kind,
            "stop"
        );

        let (status, approved_again) = json_request(
            app,
            &format!("/v1/planning-sessions/{session}/approve"),
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
        assert_eq!(
            store
                .claims_for("plan/planned/work", Some("plan.published"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .claims_for("plan/planned/work", Some("plan.documents"))
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
subgraph {{
  scope "eval/demo/${{ST_PLAN_RUN}}" retention="temporary" change-policy="agent" {{
    plan "eval/demo" state="ready" {{
      goal "Complete plan eval/demo."
      baseline "document-content" {{ has "doc/evals/demo/task@{hash}" "hello" }}
      step "document" {{
        title "The document exists"
      subgraph {{
        message "task" {{ to "person/worker"; content "doc/evals/demo/task@{hash}" }}
      }}
      }}
      step "cleanup" finally=#true {{
        subgraph {{ scope "eval/demo/${{ST_PLAN_RUN}}" {{ stop }} }}
        gate "scope-empty" {{ empty "scope/eval/demo/${{ST_PLAN_RUN}}" }}
      }}
    }}
  }}
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
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["scope"]
                .as_str()
                .unwrap()
                .starts_with("scope/eval/demo/")
        );
        assert!(body["plan_run"].as_str().unwrap().starts_with("plan-run/"));
        let eval_scope = body["scope"].as_str().unwrap();
        let (status, eval_status) = get_request(
            app.clone(),
            &format!("/v1/evals/{}", urlencoding::encode(eval_scope)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{eval_status}");
        assert_eq!(eval_status["plan_run"], body["plan_run"]);
        assert_eq!(eval_status["lifecycle"], "running");
        assert!(eval_status.get("active_checkpoint").is_none());
        let root = body["plan_run"].as_str().unwrap();
        let (status, plan_runs) = get_request(
            app,
            &format!("/v1/plan-runs?root={}", urlencoding::encode(root)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{plan_runs}");
        assert_eq!(plan_runs.as_array().unwrap().len(), 1);
        assert_eq!(plan_runs[0]["subject"], root);
        assert_eq!(
            store
                .get_document("doc/evals/demo/task", &hash)
                .unwrap()
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn a_scoped_plan_revision_publishes_without_reapplying_the_scope() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2
subgraph {
  scope "revision/${ST_PLAN_RUN}" change-policy="supervisor" change-authority="agent/sup" {
    plan "revision" state="ready" {
      goal "Complete plan revision."
      step "work" { goal "First goal." }
    }
  }
}
"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "revision-plan-one")
            .unwrap();
        let run = state
            .store
            .create_plan_run(&PlanRunRequest {
                plan: "revision".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "revision-run".into(),
            })
            .unwrap();
        let app = router(state.clone());
        let replacement = r#"
version 2
subgraph {
  scope "revision/${ST_PLAN_RUN}" change-policy="supervisor" change-authority="agent/sup" {
    plan "revision" state="ready" {
      goal "Complete plan revision."
      step "work" { goal "Corrected goal." }
    }
  }
}
"#;
        let (status, revised) = json_request(
            app,
            &format!("/v1/plan-runs/{}/revision", run.id),
            serde_json::to_value(PlanRevisionRequest {
                intent: crate::model::IntentInput {
                    kdl: replacement.into(),
                    source_name: None,
                },
                actor: "agent/sup".into(),
                reason: "the first goal was incomplete".into(),
                idempotency_key: "revision-two".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revised}");
        assert_ne!(revised["revision"], run.revision);
        assert_eq!(revised["root_revision"], run.root_revision);
        assert_eq!(revised["steps"][0]["status"], "pending");
        assert_eq!(
            state
                .store
                .claims_for("scope/revision/${ST_PLAN_RUN}", Some("intent.desired"))
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn a_claimed_step_publishes_one_attempt_bound_ready_plan() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2
subgraph {
  plan "bootstrap" state="ready" {
    goal "Complete plan bootstrap."
    step "compile" {
      assigned-to "agent/planner"
      produces-plan "project/work"
    }
  }
}
"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .plan(
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
            .create_plan_run(&PlanRunRequest {
                plan: "bootstrap".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-bootstrap-output".into(),
            })
            .unwrap();
        let step = run.steps[0].clone();
        state
            .store
            .set_step_state(&step.subject, "ready", None)
            .unwrap();
        let produced = r#"
version 2
subgraph {
  plan "project/work" state="ready" {
    goal "Complete plan project/work."
    step "inspect" { title "Inspect the project" }
    step "implement" { depends-on { step "inspect" completed } }
  }
}
"#;
        let app = router(state.clone());
        let (status, output) = json_request(
            app.clone(),
            &format!("/v1/work/plan/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(PlanProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: produced.into(),
                    source_name: Some("generated.kdl".into()),
                },
                actor: "agent/node.planner".into(),
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
                    actor: Some("agent/node.planner".into()),
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
subgraph {
  plan "project/other" state="ready" { goal "Complete plan project/other."; goal "Complete plan project/other."; step "inspect" { } }
}
"#;
        let (status, output) = json_request(
            app.clone(),
            &format!("/v1/work/plan/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(PlanProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: wrong.into(),
                    source_name: Some("wrong.kdl".into()),
                },
                actor: "agent/node.planner".into(),
                incarnation: Some("test".into()),
                idempotency_key: "reject-wrong-output".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{output}");
        assert_eq!(output["code"], "wrong-plan-output");
        let (status, output) = json_request(
            app,
            &format!("/v1/work/plan/{}", urlencoding::encode(&step.subject)),
            serde_json::to_value(PlanProductionRequest {
                intent: crate::model::IntentInput {
                    kdl: produced.into(),
                    source_name: Some("generated.kdl".into()),
                },
                actor: "agent/node.planner".into(),
                incarnation: Some("test".into()),
                idempotency_key: "publish-produced-work".into(),
            })
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{output}");
        assert_eq!(output["plan"], "plan/project/work");
        let revision = output["revision"].as_str().unwrap();
        assert_eq!(revision.len(), 64);
        assert!(
            state
                .store
                .plan_spec("project/work", Some(revision))
                .unwrap()
                .is_some()
        );
        let bound = state
            .store
            .plan_output(&step.subject, 1, &step.definition_hash)
            .unwrap()
            .expect("attempt-bound plan output");
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
    async fn a_step_review_decision_binds_to_its_pending_request() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let source = r#"
version 2
subgraph {
  plan "review-api" state="ready" {
    goal "Complete plan review-api."
    step "approval" { gate "human-review" type="human" { reviewer "person/nathan" } }
  }
}
"#;
        let intent = parse_intent(source, "node").unwrap();
        let planned = state
            .store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        state
            .store
            .apply(&intent, &planned.subject_tokens, "review-api-plan")
            .unwrap();
        let run = state
            .store
            .create_plan_run(&PlanRunRequest {
                plan: "review-api".into(),
                revision: None,
                workspace: root.path().display().to_string(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "review-api-run".into(),
            })
            .unwrap();
        let step = &run.steps[0];
        let request = state
            .store
            .append_claim(&ClaimInput {
                subject: step.subject.clone(),
                kind: "review.requested".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("reviewer".into(), Value::String("person/nathan".into())),
                    ("plan_revision".into(), Value::String(run.revision.clone())),
                    (
                        "step_definition".into(),
                        Value::String(step.definition_hash.clone()),
                    ),
                    ("attempt".into(), Value::from(step.attempt)),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("review-request".into()),
            })
            .unwrap();
        let app = router(state);
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
            &format!("/v1/reviews/{}", step.subject),
            body("person/someone-else"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rejected}");
        assert_eq!(rejected["code"], "wrong-reviewer");

        let (status, accepted) = json_request(
            app,
            &format!("/v1/reviews/{}", step.subject),
            body("person/nathan"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{accepted}");
        assert_eq!(accepted["body"]["fields"]["request"], request.id);
        assert_eq!(accepted["body"]["evidence"][0], request.id);
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
