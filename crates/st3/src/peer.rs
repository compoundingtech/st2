use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use hmac::{Hmac, Mac as _};
use notify::Watcher as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::client::Client;
use crate::config::{Config, PeerConfig};
use crate::model::{
    ApiResponse, ReplicationExchange, ReplicationExportRequest, ReplicationExportResponse,
    ReplicationInventory, ReplicationPeerFailureRequest, ReplicationReceiveRequest,
    ReplicationReceiveResponse,
};
#[cfg(test)]
use crate::store::Store;

const PROTOCOL: &str = "st3-replication-v1";
const EXCHANGE_PATH: &str = "/v1/peer/exchange";
const HEADER_FLEET: &str = "x-st3-fleet";
const HEADER_NODE: &str = "x-st3-node";
const HEADER_BODY: &str = "x-st3-body-sha256";
const HEADER_SIGNATURE: &str = "x-st3-signature";
const HEADER_REQUEST: &str = "x-st3-request-digest";
pub(crate) const MAX_EXCHANGE_BYTES: usize = 64 * 1024 * 1024;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct FleetAuth {
    fleet_id: String,
    secret: Arc<Vec<u8>>,
}

impl FleetAuth {
    pub fn load(fleet_id: &str, path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path)
            .with_context(|| format!("inspect the fleet secret {}", path.display()))?;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "the fleet secret must not grant group or other permissions"
        );
        let bytes =
            fs::read(path).with_context(|| format!("read the fleet secret {}", path.display()))?;
        let hexadecimal = std::str::from_utf8(&bytes)
            .ok()
            .map(str::trim)
            .filter(|value| {
                value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
        let secret = match hexadecimal {
            Some(value) => hex::decode(value).context("decode the hexadecimal fleet secret")?,
            None => bytes,
        };
        anyhow::ensure!(secret.len() == 32, "the fleet secret must contain 32 bytes");
        Ok(Self {
            fleet_id: fleet_id.into(),
            secret: Arc::new(secret),
        })
    }

    #[cfg(test)]
    fn test(fleet_id: &str, secret: &[u8]) -> Self {
        Self {
            fleet_id: fleet_id.into(),
            secret: Arc::new(secret.to_vec()),
        }
    }

    pub fn fleet_id(&self) -> &str {
        &self.fleet_id
    }

    fn body_digest(body: &[u8]) -> String {
        hex::encode(Sha256::digest(body))
    }

    fn signature(
        &self,
        method: &str,
        path: &str,
        node: &str,
        body_digest: &str,
        request_digest: Option<&str>,
    ) -> String {
        let canonical = format!(
            "{PROTOCOL}\n{method}\n{path}\n{}\n{node}\n{body_digest}\n{}",
            self.fleet_id,
            request_digest.unwrap_or_default()
        );
        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts a secret of any length");
        mac.update(canonical.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn request_headers(&self, node: &str, body: &[u8]) -> Result<HeaderMap> {
        let digest = Self::body_digest(body);
        let signature = self.signature("POST", EXCHANGE_PATH, node, &digest, None);
        headers(&self.fleet_id, node, &digest, &signature, None)
    }

    fn response_headers(&self, node: &str, body: &[u8], request_digest: &str) -> Result<HeaderMap> {
        let digest = Self::body_digest(body);
        let signature = self.signature(
            "RESPONSE",
            EXCHANGE_PATH,
            node,
            &digest,
            Some(request_digest),
        );
        headers(
            &self.fleet_id,
            node,
            &digest,
            &signature,
            Some(request_digest),
        )
    }

    fn verify(
        &self,
        headers: &HeaderMap,
        method: &str,
        path: &str,
        body: &[u8],
        expected_node: Option<&str>,
        request_digest: Option<&str>,
    ) -> Result<String> {
        let field = |name: &str| -> Result<&str> {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .with_context(|| format!("the signed message has no valid {name} header"))
        };
        anyhow::ensure!(
            field(HEADER_FLEET)? == self.fleet_id,
            "the fleet ID does not match"
        );
        let node = field(HEADER_NODE)?;
        if let Some(expected) = expected_node {
            anyhow::ensure!(
                node == expected,
                "the signed node does not match the configured peer"
            );
        }
        let digest = Self::body_digest(body);
        anyhow::ensure!(
            field(HEADER_BODY)? == digest,
            "the signed body digest does not match"
        );
        if let Some(expected) = request_digest {
            anyhow::ensure!(
                field(HEADER_REQUEST)? == expected,
                "the response does not bind to this request"
            );
        }
        let signature = hex::decode(field(HEADER_SIGNATURE)?)
            .context("the replication signature is not hexadecimal")?;
        let canonical = format!(
            "{PROTOCOL}\n{method}\n{path}\n{}\n{node}\n{digest}\n{}",
            self.fleet_id,
            request_digest.unwrap_or_default()
        );
        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts a secret of any length");
        mac.update(canonical.as_bytes());
        mac.verify_slice(&signature)
            .context("the replication signature does not match")?;
        Ok(node.into())
    }
}

fn headers(
    fleet: &str,
    node: &str,
    digest: &str,
    signature: &str,
    request_digest: Option<&str>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        (HEADER_FLEET, fleet),
        (HEADER_NODE, node),
        (HEADER_BODY, digest),
        (HEADER_SIGNATURE, signature),
    ] {
        headers.insert(name, HeaderValue::from_str(value)?);
    }
    if let Some(request_digest) = request_digest {
        headers.insert(HEADER_REQUEST, HeaderValue::from_str(request_digest)?);
    }
    Ok(headers)
}

#[derive(Clone)]
struct PeerState {
    backend: PeerBackend,
    node: String,
    auth: FleetAuth,
    peers: BTreeSet<String>,
    main_socket: PathBuf,
    outbound_notify: watch::Sender<u64>,
}

#[derive(Clone)]
enum PeerBackend {
    Main(Client),
    #[cfg(test)]
    Local(Arc<Store>),
}

impl PeerBackend {
    async fn export(
        &self,
        fleet_id: &str,
        inventory: &ReplicationInventory,
        summary_only: bool,
    ) -> Result<ReplicationExportResponse> {
        match self {
            Self::Main(client) => {
                client
                    .post(
                        "/v1/internal/replication/export",
                        &ReplicationExportRequest {
                            fleet_id: fleet_id.to_owned(),
                            inventory: inventory.clone(),
                            summary_only,
                        },
                    )
                    .await
            }
            #[cfg(test)]
            Self::Local(store) => {
                let exchange = if summary_only {
                    store.export_replication_summary(fleet_id)?
                } else {
                    store.export_replication_exchange(fleet_id, inventory)?
                };
                Ok(ReplicationExportResponse {
                    exchange,
                    store_index: store.index()?,
                })
            }
        }
    }

    async fn receive(
        &self,
        peer: &str,
        fleet_id: &str,
        exchange: &ReplicationExchange,
    ) -> Result<ReplicationReceiveResponse> {
        match self {
            Self::Main(client) => {
                client
                    .post(
                        "/v1/internal/replication/receive",
                        &ReplicationReceiveRequest {
                            peer: peer.to_owned(),
                            fleet_id: fleet_id.to_owned(),
                            exchange: exchange.clone(),
                        },
                    )
                    .await
            }
            #[cfg(test)]
            Self::Local(store) => {
                let receipt = store
                    .receive_replication_exchange(peer, fleet_id, exchange)
                    .map_err(anyhow::Error::msg)?;
                let admission = store.validate_replication_backlog()?;
                let repairs = store.apply_replication_repairs()?;
                let projected = store.project_replication_backlog()?;
                Ok(ReplicationReceiveResponse {
                    receipt,
                    changed: projected && (admission.changed || repairs != 0),
                    store_index: store.index()?,
                })
            }
        }
    }

    async fn record_failure(&self, peer: &str, status: &str, error: &str) -> Result<()> {
        match self {
            Self::Main(client) => {
                let _: serde_json::Value = client
                    .post(
                        "/v1/internal/replication/peer-failure",
                        &ReplicationPeerFailureRequest {
                            peer: peer.to_owned(),
                            status: status.to_owned(),
                            error: error.to_owned(),
                        },
                    )
                    .await?;
                Ok(())
            }
            #[cfg(test)]
            Self::Local(store) => store.record_peer_failure(peer, status, error),
        }
    }
}

pub async fn run_worker(config: Config) -> Result<()> {
    config.validate()?;
    let fleet_id = config
        .fleet_id
        .as_deref()
        .context("the replication worker needs fleet_id")?;
    let secret_file = config
        .shared_secret_file
        .as_deref()
        .context("the replication worker needs shared_secret_file")?;
    let address = config
        .peer_listen
        .as_deref()
        .context("the replication worker needs peer_listen")?;
    let auth = FleetAuth::load(fleet_id, secret_file)?;
    wait_for_main_daemon(&config.socket).await;
    let backend = PeerBackend::Main(Client::unix(config.socket.clone()));
    let (notify, _notify_receiver) = watch::channel(0_u64);
    let wake_file = config.state_dir.join("replication.wake");
    if !wake_file.exists() {
        fs::write(&wake_file, b"worker-start\n")?;
    }
    let watcher_notify = notify.clone();
    let mut database_watcher =
        notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            if event.is_ok() {
                watcher_notify.send_modify(|generation| *generation = generation.saturating_add(1));
            }
        })?;
    database_watcher.watch(&wake_file, notify::RecursiveMode::NonRecursive)?;
    let state = PeerState {
        backend: backend.clone(),
        node: config.node.clone(),
        auth: auth.clone(),
        peers: config.peers.iter().map(|peer| peer.name.clone()).collect(),
        main_socket: config.socket.clone(),
        outbound_notify: notify.clone(),
    };
    start_outbound(
        backend,
        config.node.clone(),
        config.peers,
        auth,
        config.socket,
        notify,
    );
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind the replication listener at {address}"))?;
    let app = peer_router(state);
    axum::serve(listener, app).await?;
    Ok(())
}

fn peer_router(state: PeerState) -> Router {
    Router::new()
        .route(EXCHANGE_PATH, post(receive_exchange))
        .layer(DefaultBodyLimit::max(MAX_EXCHANGE_BYTES))
        .with_state(state)
}

async fn wait_for_main_daemon(socket: &Path) {
    let client = Client::unix(socket);
    loop {
        if client.get::<serde_json::Value>("/v1/health").await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn start_outbound(
    backend: PeerBackend,
    node: String,
    peers: Vec<PeerConfig>,
    auth: FleetAuth,
    main_socket: PathBuf,
    notify: watch::Sender<u64>,
) {
    for peer in peers {
        let backend = backend.clone();
        let node = node.clone();
        let auth = auth.clone();
        let main_socket = main_socket.clone();
        let mut notify = notify.subscribe();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match exchange(&backend, &node, &peer, &auth, &main_socket).await {
                    Ok(_) => {
                        backoff = Duration::from_secs(1);
                        tokio::select! {
                            _ = notify.changed() => {}
                            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                        }
                    }
                    Err(error) => {
                        let status = if error.to_string().contains("signature")
                            || error.to_string().contains("fleet")
                        {
                            "auth-failed"
                        } else {
                            "down"
                        };
                        let _ = backend
                            .record_failure(&peer.name, status, &error.to_string())
                            .await;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        });
    }
}

async fn receive_exchange(
    State(state): State<PeerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let relay = match state
        .auth
        .verify(&headers, "POST", EXCHANGE_PATH, &body, None, None)
    {
        Ok(relay) => relay,
        Err(error) => {
            return (
                StatusCode::UNAUTHORIZED,
                format!("replication authentication failed: {error:#}"),
            )
                .into_response();
        }
    };
    let request_digest = FleetAuth::body_digest(&body);
    let result = async {
        anyhow::ensure!(state.peers.contains(&relay), "the peer is not configured");
        let request: ReplicationExchange =
            serde_json::from_slice(&body).context("decode the replication exchange")?;
        let received = state
            .backend
            .receive(&relay, state.auth.fleet_id(), &request)
            .await?;
        if received.changed {
            wake_main(&state.main_socket).await;
        }
        if received.receipt.received != 0 {
            state
                .outbound_notify
                .send_modify(|generation| *generation = generation.saturating_add(1));
        }
        let response = state
            .backend
            .export(state.auth.fleet_id(), &request.inventory, false)
            .await?;
        signed_response(
            &state,
            &request_digest,
            response.store_index,
            response.exchange,
        )
    }
    .await;
    match result {
        Ok(response) => response,
        Err(error) => {
            let message = format!("replication request failed: {error:#}");
            let _ = state.backend.record_failure(&relay, "down", &message).await;
            signed_error_response(
                &state,
                &request_digest,
                0,
                StatusCode::UNPROCESSABLE_ENTITY,
                &message,
            )
            .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, message).into_response())
        }
    }
}

fn signed_response<T: Serialize>(
    state: &PeerState,
    request_digest: &str,
    store_index: u64,
    value: T,
) -> Result<Response> {
    let envelope = ApiResponse {
        api_version: "st3.v1".into(),
        request_id: uuid::Uuid::now_v7().to_string(),
        snapshot_host: state.node.clone(),
        store_index,
        value,
    };
    let body = serde_json::to_vec(&envelope)?;
    let headers = state
        .auth
        .response_headers(&state.node, &body, request_digest)?;
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response.headers_mut().extend(headers);
    Ok(response)
}

fn signed_error_response(
    state: &PeerState,
    request_digest: &str,
    store_index: u64,
    status: StatusCode,
    message: &str,
) -> Result<Response> {
    let envelope = ApiResponse {
        api_version: "st3.v1".into(),
        request_id: uuid::Uuid::now_v7().to_string(),
        snapshot_host: state.node.clone(),
        store_index,
        value: serde_json::json!({
            "code": "replication-request-failed",
            "message": message,
        }),
    };
    let body = serde_json::to_vec(&envelope)?;
    let headers = state
        .auth
        .response_headers(&state.node, &body, request_digest)?;
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response.headers_mut().extend(headers);
    Ok(response)
}

async fn exchange(
    backend: &PeerBackend,
    node: &str,
    peer: &PeerConfig,
    auth: &FleetAuth,
    main_socket: &Path,
) -> Result<bool> {
    let first = backend
        .export(auth.fleet_id(), &ReplicationInventory::default(), true)
        .await?
        .exchange;
    let local_digest = first.inventory.digest.clone();
    let query = ReplicationExchange {
        envelopes: Vec::new(),
        ..first
    };
    let remote = post_signed(peer, node, auth, &query).await?;
    let different = remote.inventory.digest != local_digest;
    let pulled = !remote.envelopes.is_empty();
    let received = backend
        .receive(&peer.name, auth.fleet_id(), &remote)
        .await?;
    if received.changed {
        wake_main(main_socket).await;
    }
    let mut pushed = false;
    let mut pulled_follow_up = false;
    if different {
        let push = backend
            .export(auth.fleet_id(), &remote.inventory, false)
            .await?
            .exchange;
        pushed = !push.envelopes.is_empty();
        let response = post_signed(peer, node, auth, &push).await?;
        pulled_follow_up = !response.envelopes.is_empty();
        let received = backend
            .receive(&peer.name, auth.fleet_id(), &response)
            .await?;
        if received.changed {
            wake_main(main_socket).await;
        }
    }
    Ok(pulled || pulled_follow_up || pushed)
}

async fn post_signed(
    peer: &PeerConfig,
    node: &str,
    auth: &FleetAuth,
    exchange: &ReplicationExchange,
) -> Result<ReplicationExchange> {
    let body = serde_json::to_vec(exchange)?;
    let request_digest = FleetAuth::body_digest(&body);
    let headers = auth.request_headers(node, &body)?;
    let endpoint = format!("{}{}", peer.url.trim_end_matches('/'), EXCHANGE_PATH);
    let response = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(120))
        .build()?
        .post(&endpoint)
        .headers(headers)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .with_context(|| {
            format!(
                "replication endpoint `{endpoint}` failed during its 120 second exchange limit; retry peer {}",
                peer.name
            )
        })?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.bytes().await?.to_vec();
    auth.verify(
        &headers,
        "RESPONSE",
        EXCHANGE_PATH,
        &bytes,
        Some(&peer.name),
        Some(&request_digest),
    )?;
    anyhow::ensure!(
        status.is_success(),
        "peer {} returned {status}: {}",
        peer.name,
        String::from_utf8_lossy(&bytes)
    );
    let response: ApiResponse<ReplicationExchange> =
        serde_json::from_slice(&bytes).context("decode the signed peer response")?;
    anyhow::ensure!(
        response.api_version == "st3.v1",
        "the peer API version differs"
    );
    Ok(response.value)
}

async fn wake_main(socket: &Path) {
    if !socket.exists() {
        return;
    }
    let client = Client::unix(socket.to_path_buf());
    let _ = client
        .post::<_, serde_json::Value>("/v1/internal/replication-wake", &serde_json::json!({}))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ClaimInput;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use tower::ServiceExt as _;

    #[test]
    fn signed_messages_detect_tampering_and_wrong_fleets() {
        let auth = FleetAuth::test("1f91ca65-7793-48cc-866e-ac15690130e1", &[7; 32]);
        let body = br#"{"hello":"fleet"}"#;
        let headers = auth.request_headers("node-a", body).unwrap();
        assert!(
            auth.verify(&headers, "POST", EXCHANGE_PATH, body, Some("node-a"), None)
                .is_ok()
        );
        assert!(
            auth.verify(
                &headers,
                "POST",
                EXCHANGE_PATH,
                br#"{"hello":"other"}"#,
                Some("node-a"),
                None
            )
            .is_err()
        );
        let other = FleetAuth::test("48608b46-bf75-442a-a462-787085dc574e", &[7; 32]);
        assert!(
            other
                .verify(&headers, "POST", EXCHANGE_PATH, body, Some("node-a"), None)
                .is_err()
        );
    }

    #[test]
    fn response_signatures_bind_to_one_request() {
        let auth = FleetAuth::test("1f91ca65-7793-48cc-866e-ac15690130e1", &[9; 32]);
        let body = b"response";
        let headers = auth.response_headers("node-b", body, "request-a").unwrap();
        assert!(
            auth.verify(
                &headers,
                "RESPONSE",
                EXCHANGE_PATH,
                body,
                Some("node-b"),
                Some("request-a")
            )
            .is_ok()
        );
        assert!(
            auth.verify(
                &headers,
                "RESPONSE",
                EXCHANGE_PATH,
                body,
                Some("node-b"),
                Some("request-b")
            )
            .is_err()
        );
    }

    #[test]
    fn fleet_secret_loading_accepts_private_raw_or_hex_files_only() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("fleet-secret");
        fs::write(&path, [3_u8; 32]).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        FleetAuth::load("1f91ca65-7793-48cc-866e-ac15690130e1", &path).unwrap();

        fs::write(&path, format!("{}\n", hex::encode([4_u8; 32]))).unwrap();
        FleetAuth::load("1f91ca65-7793-48cc-866e-ac15690130e1", &path).unwrap();

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(FleetAuth::load("1f91ca65-7793-48cc-866e-ac15690130e1", &path).is_err());
    }

    #[tokio::test]
    async fn an_authenticated_failure_has_a_request_bound_signature() {
        let auth = FleetAuth::test("1f91ca65-7793-48cc-866e-ac15690130e1", &[5; 32]);
        let body = Bytes::from_static(b"not json");
        let request_digest = FleetAuth::body_digest(&body);
        let response = receive_exchange(
            State(PeerState {
                backend: PeerBackend::Local(Arc::new(Store::open_memory("target").unwrap())),
                node: "target".into(),
                auth: auth.clone(),
                peers: BTreeSet::from(["source".into()]),
                main_socket: PathBuf::from("/no/such/socket"),
                outbound_notify: watch::channel(0_u64).0,
            }),
            auth.request_headers("source", &body).unwrap(),
            body,
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        auth.verify(
            &headers,
            "RESPONSE",
            EXCHANGE_PATH,
            &bytes,
            Some("target"),
            Some(&request_digest),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn the_peer_route_accepts_an_exchange_above_axums_default_body_limit() {
        let fleet = "1f91ca65-7793-48cc-866e-ac15690130e1";
        let auth = FleetAuth::test(fleet, &[5; 32]);
        let state = PeerState {
            backend: PeerBackend::Local(Arc::new(Store::open_memory("target").unwrap())),
            node: "target".into(),
            auth: auth.clone(),
            peers: BTreeSet::from(["source".into()]),
            main_socket: PathBuf::from("/no/such/socket"),
            outbound_notify: watch::channel(0_u64).0,
        };
        let exchange = ReplicationExchange {
            peer: "source".into(),
            fleet_id: fleet.into(),
            schema_digest: st3_schema::registry().digest(),
            authority_digest: String::new(),
            graph_digest: String::new(),
            inventory: ReplicationInventory::default(),
            envelopes: Vec::new(),
        };
        let mut body = serde_json::to_vec(&exchange).unwrap();
        body.resize(2 * 1024 * 1024 + 1, b' ');
        let request_digest = FleetAuth::body_digest(&body);
        let mut request = Request::builder()
            .method("POST")
            .uri(EXCHANGE_PATH)
            .body(Body::from(body.clone()))
            .unwrap();
        request
            .headers_mut()
            .extend(auth.request_headers("source", &body).unwrap());
        let response = peer_router(state).oneshot(request).await.unwrap();
        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let headers = response.headers().clone();
        let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        auth.verify(
            &headers,
            "RESPONSE",
            EXCHANGE_PATH,
            &response_body,
            Some("target"),
            Some(&request_digest),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn signed_peer_exchange_moves_new_authority_in_both_directions() {
        let fleet = "1f91ca65-7793-48cc-866e-ac15690130e1";
        let auth = FleetAuth::test(fleet, &[6; 32]);
        let source = Arc::new(Store::open_memory("source").unwrap());
        let target = Arc::new(Store::open_memory("target").unwrap());
        source.bind_fleet(fleet).unwrap();
        target.bind_fleet(fleet).unwrap();
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

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = PeerState {
            backend: PeerBackend::Local(target.clone()),
            node: "target".into(),
            auth: auth.clone(),
            peers: BTreeSet::from(["source".into()]),
            main_socket: PathBuf::from("/no/such/socket"),
            outbound_notify: watch::channel(0_u64).0,
        };
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route(EXCHANGE_PATH, post(receive_exchange))
                    .with_state(state),
            )
            .into_future(),
        );
        let peer = PeerConfig {
            name: "target".into(),
            url: format!("http://{address}"),
        };
        exchange(
            &PeerBackend::Local(source.clone()),
            "source",
            &peer,
            &auth,
            Path::new("/no/such/socket"),
        )
        .await
        .unwrap();
        assert!(
            target
                .latest_claim("host/source", Some("transport.observed"))
                .unwrap()
                .is_some()
        );

        target
            .append_claim(&ClaimInput {
                subject: "host/target".into(),
                kind: "transport.observed".into(),
                actor: None,
                fields: BTreeMap::from([("status".into(), Value::String("up".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("target-up".into()),
            })
            .unwrap();
        exchange(
            &PeerBackend::Local(source.clone()),
            "source",
            &peer,
            &auth,
            Path::new("/no/such/socket"),
        )
        .await
        .unwrap();
        assert!(
            source
                .latest_claim("host/target", Some("transport.observed"))
                .unwrap()
                .is_some()
        );
        let source_status = source.replication_status(true, Some(fleet), &[]).unwrap();
        let target_status = target.replication_status(true, Some(fleet), &[]).unwrap();
        assert_eq!(
            source_status.authority_digest,
            target_status.authority_digest
        );
        let summary = source.export_replication_summary(fleet).unwrap();
        assert!(summary.inventory.envelopes.is_empty());
        assert!(!summary.inventory.digest.is_empty());
        let converged = target
            .export_replication_exchange(fleet, &summary.inventory)
            .unwrap();
        assert!(converged.inventory.envelopes.is_empty());
        assert!(converged.envelopes.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn the_worker_uses_the_main_daemon_as_the_only_database_writer() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("st3.sock");
        let fleet = "1f91ca65-7793-48cc-866e-ac15690130e1";
        let store = Arc::new(Store::open(&root.path().join("claims.sqlite3"), "target").unwrap());
        store.bind_fleet(fleet).unwrap();
        let app = crate::api::router(crate::api::AppState {
            store: store.clone(),
            notify: Arc::new(tokio::sync::Notify::new()),
            event_notify: watch::channel(0_u64).0,
            node: "target".into(),
            state_dir: root.path().to_path_buf(),
            pty_root: root.path().join("pty"),
            pty_binary: std::path::PathBuf::from("pty"),
            fleet_id: Some(fleet.into()),
            configured_peers: vec!["source".into()],
        });
        let server_socket = socket.clone();
        let server = tokio::spawn(async move {
            crate::api::serve_unix(&server_socket, app).await.unwrap();
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(socket.exists(), "the main daemon socket did not start");

        let source = Store::open_memory("source").unwrap();
        source.bind_fleet(fleet).unwrap();
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
        let exchange = source
            .export_replication_exchange(fleet, &ReplicationInventory::default())
            .unwrap();
        let backend = PeerBackend::Main(Client::unix(socket));
        let received = backend.receive("source", fleet, &exchange).await.unwrap();
        assert!(received.changed);
        assert!(received.receipt.received > 0);
        let exported = backend
            .export(fleet, &ReplicationInventory::default(), false)
            .await
            .unwrap();
        assert!(!exported.exchange.inventory.envelopes.is_empty());
        let summary = backend
            .export(fleet, &ReplicationInventory::default(), true)
            .await
            .unwrap();
        assert!(summary.exchange.inventory.envelopes.is_empty());
        assert!(!summary.exchange.inventory.digest.is_empty());
        backend
            .record_failure("source", "down", "test outage")
            .await
            .unwrap();
        assert!(
            store
                .latest_claim("host/source", Some("transport.observed"))
                .unwrap()
                .is_some()
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_duplicate_inbound_exchange_does_not_wake_outbound_replication() {
        let fleet = "1f91ca65-7793-48cc-866e-ac15690130e1";
        let auth = FleetAuth::test(fleet, &[7; 32]);
        let source = Store::open_memory("source").unwrap();
        let target = Arc::new(Store::open_memory("target").unwrap());
        source.bind_fleet(fleet).unwrap();
        target.bind_fleet(fleet).unwrap();
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
        let exchange = source
            .export_replication_exchange(fleet, &ReplicationInventory::default())
            .unwrap();
        let body = Bytes::from(serde_json::to_vec(&exchange).unwrap());
        let (outbound_notify, mut outbound_wake) = watch::channel(0_u64);
        let state = PeerState {
            backend: PeerBackend::Local(target),
            node: "target".into(),
            auth: auth.clone(),
            peers: BTreeSet::from(["source".into()]),
            main_socket: PathBuf::from("/no/such/socket"),
            outbound_notify,
        };

        let first = receive_exchange(
            State(state.clone()),
            auth.request_headers("source", &body).unwrap(),
            body.clone(),
        )
        .await;
        assert!(first.status().is_success());
        outbound_wake.changed().await.unwrap();
        let _ = outbound_wake.borrow_and_update();

        let duplicate = receive_exchange(
            State(state.clone()),
            auth.request_headers("source", &body).unwrap(),
            body,
        )
        .await;
        assert!(duplicate.status().is_success());
        assert!(
            !outbound_wake.has_changed().unwrap(),
            "a duplicate receipt must not start a replication echo loop"
        );
    }
}
