//! Typed `st3.client.v0` client. This crate never parses CLI or harness output.

mod contract;
mod generated;
pub use contract::*;
pub use generated::*;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub enum Endpoint {
    Unix(PathBuf),
    FabricLoopback(String),
}

#[derive(Clone, Debug)]
pub struct Client {
    endpoint: Endpoint,
    credential: Option<String>,
    http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("st3 client API error {0:?}: {1}")]
    Api(ErrorCode, String, ErrorEnvelope),
    #[error("st3 client transport error: {0}")]
    Transport(String),
    #[error("st3 client protocol error: {0}")]
    Protocol(String),
}

impl Client {
    pub fn unix(path: impl AsRef<Path>) -> Self {
        Self {
            endpoint: Endpoint::Unix(path.as_ref().to_owned()),
            credential: None,
            http: reqwest::Client::new(),
        }
    }

    pub fn fabric_loopback(base_url: impl Into<String>, credential: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::FabricLoopback(base_url.into().trim_end_matches('/').to_owned()),
            credential: Some(credential.into()),
            http: reqwest::Client::new(),
        }
    }

    pub async fn capabilities(&self) -> Result<Envelope<Capabilities>, ClientError> {
        self.get("/v1/client/capabilities").await
    }
    pub async fn list(
        &self,
        collection: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        history: bool,
    ) -> Result<Envelope<Page>, ClientError> {
        let mut query = Vec::new();
        if let Some(cursor) = cursor {
            query.push(format!("cursor={}", percent_encode(cursor)));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if history {
            query.push("history=true".into());
        }
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        self.get(&format!("/v1/client/{collection}{suffix}")).await
    }
    pub async fn resource(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Envelope<Resource>, ClientError> {
        let id = if collection == "launches" {
            id.trim_start_matches("launch/")
        } else {
            id
        };
        self.get(&format!("/v1/client/{collection}/{id}")).await
    }
    pub async fn timeline(
        &self,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<Envelope<TimelinePage>, ClientError> {
        let suffix = limit
            .map(|limit| format!("?limit={limit}"))
            .unwrap_or_default();
        self.get(&format!(
            "/v1/client/sessions/{session_id}/timeline{suffix}"
        ))
        .await
    }
    pub async fn events(
        &self,
        after: Option<&str>,
        limit: Option<usize>,
        wait_ms: Option<u64>,
    ) -> Result<Envelope<EventPage>, ClientError> {
        let mut query = Vec::new();
        if let Some(after) = after {
            query.push(format!("after={}", percent_encode(after)));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(wait_ms) = wait_ms {
            query.push(format!("wait_ms={wait_ms}"));
        }
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        self.get(&format!("/v1/client/events{suffix}")).await
    }
    pub async fn action(
        &self,
        request: &ActionRequest,
    ) -> Result<Envelope<ActionResult>, ClientError> {
        self.post("/v1/client/actions", request).await
    }
    pub async fn pairing_begin(
        &self,
        request: &PairingBegin,
    ) -> Result<Envelope<PairingChallenge>, ClientError> {
        self.post("/v1/client/pairings", request).await
    }
    pub async fn pairing_complete(
        &self,
        pairing_id: &str,
        request: &PairingComplete,
    ) -> Result<Envelope<PairedSession>, ClientError> {
        self.post(
            &format!(
                "/v1/client/pairings/{}/complete",
                pairing_id.trim_start_matches("pairing/")
            ),
            request,
        )
        .await
    }
    pub async fn terminal_screen(
        &self,
        terminal_id: &str,
    ) -> Result<Envelope<TerminalScreen>, ClientError> {
        self.get(&format!(
            "/v1/client/terminals/{}/screen",
            terminal_id.trim_start_matches("terminal/")
        ))
        .await
    }
    pub async fn terminal_frames(
        &self,
        terminal_id: &str,
        after: Option<u64>,
        incarnation: Option<&str>,
    ) -> Result<Envelope<TerminalFramePage>, ClientError> {
        let mut query = Vec::new();
        if let Some(after) = after {
            query.push(format!("after={after}"));
        }
        if let Some(incarnation) = incarnation {
            query.push(format!("incarnation={}", percent_encode(incarnation)));
        }
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        self.get(&format!(
            "/v1/client/terminals/{}/stream{suffix}",
            terminal_id.trim_start_matches("terminal/")
        ))
        .await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request(Method::GET, path, None).await
    }
    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        value: &impl Serialize,
    ) -> Result<T, ClientError> {
        let body =
            serde_json::to_vec(value).map_err(|error| ClientError::Protocol(error.to_string()))?;
        self.request(Method::POST, path, Some(body)).await
    }
    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, ClientError> {
        let (status, bytes) = match &self.endpoint {
            Endpoint::Unix(socket) => {
                unix_request(socket, method, path, body, self.credential.as_deref()).await?
            }
            Endpoint::FabricLoopback(base) => {
                let mut request = self.http.request(method, format!("{base}{path}"));
                if let Some(credential) = &self.credential {
                    request = request.bearer_auth(credential);
                }
                if let Some(body) = body {
                    request = request
                        .header("content-type", "application/json")
                        .body(body);
                }
                let response = request
                    .send()
                    .await
                    .map_err(|error| ClientError::Transport(error.to_string()))?;
                let status = response.status().as_u16();
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| ClientError::Transport(error.to_string()))?
                    .to_vec();
                (status, bytes)
            }
        };
        if !(200..300).contains(&status) {
            let error: ErrorEnvelope = serde_json::from_slice(&bytes)
                .map_err(|decode| ClientError::Protocol(format!("HTTP {status}: {decode}")))?;
            return Err(ClientError::Api(
                error.code.clone(),
                error.message.clone(),
                error,
            ));
        }
        serde_json::from_slice(&bytes).map_err(|error| ClientError::Protocol(error.to_string()))
    }
}

async fn unix_request(
    socket: &Path,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    credential: Option<&str>,
) -> Result<(u16, Vec<u8>), ClientError> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost");
    if let Some(credential) = credential {
        builder = builder.header("authorization", format!("Bearer {credential}"));
    }
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let response = sender
        .send_request(
            builder
                .body(Full::new(Bytes::from(body.unwrap_or_default())))
                .map_err(|error| ClientError::Protocol(error.to_string()))?,
        )
        .await
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    let status = response.status().as_u16();
    let bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|error| ClientError::Transport(error.to_string()))?
        .to_bytes()
        .to_vec();
    Ok((status, bytes))
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
                vec![byte as char]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}
