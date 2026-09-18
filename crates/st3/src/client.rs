use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use pty_core::client::{AttachParams, ClientIo, attach};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

use crate::model::{ApiErrorResponse, ApiResponse};

#[derive(Clone, Debug)]
pub enum Endpoint {
    Unix(PathBuf),
    Http(String),
}

impl Endpoint {
    pub fn parse(value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        if value.starts_with("http://") || value.starts_with("https://") {
            Self::Http(value.trim_end_matches('/').into())
        } else if let Some(path) = value.strip_prefix("unix://") {
            Self::Unix(PathBuf::from(format!("/{path}").replace("//", "/")))
        } else {
            Self::Unix(PathBuf::from(value))
        }
    }
}

#[derive(Clone)]
pub struct Client {
    endpoint: Endpoint,
    http: reqwest::Client,
    deadlines: ClientDeadlines,
}

#[derive(Clone, Copy)]
struct ClientDeadlines {
    connect: Duration,
    request: Duration,
    bulk: Duration,
    event: Duration,
    terminal_handshake: Duration,
}

impl Default for ClientDeadlines {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(3),
            request: Duration::from_secs(15),
            bulk: Duration::from_secs(120),
            event: Duration::from_secs(35),
            terminal_handshake: Duration::from_secs(10),
        }
    }
}

impl Client {
    pub fn new(endpoint: Endpoint) -> Self {
        let deadlines = ClientDeadlines::default();
        Self {
            endpoint,
            http: reqwest::Client::builder()
                .connect_timeout(deadlines.connect)
                .build()
                .expect("the st3 HTTP client configuration is valid"),
            deadlines,
        }
    }

    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::new(Endpoint::Unix(path.into()))
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request::<(), T>("GET", path, None).await
    }

    pub async fn post<I: Serialize, O: DeserializeOwned>(&self, path: &str, body: &I) -> Result<O> {
        self.request("POST", path, Some(body)).await
    }

    pub async fn request<I: Serialize, O: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<&I>,
    ) -> Result<O> {
        let bytes = body
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let deadline = request_deadline(path, self.deadlines);
        let response = match &self.endpoint {
            Endpoint::Unix(socket) => {
                unix_request(
                    socket,
                    method,
                    path,
                    &bytes,
                    self.deadlines.connect,
                    deadline,
                )
                .await?
            }
            Endpoint::Http(base) => {
                let started = tokio::time::Instant::now();
                let url = format!("{base}{path}");
                let request = match method {
                    "GET" => self.http.get(&url),
                    "POST" => self.http.post(&url).body(bytes),
                    other => anyhow::bail!("unsupported HTTP method {other}"),
                }
                .header("content-type", "application/json")
                .header("connection", "close");
                let endpoint = url.clone();
                let response = tokio::time::timeout(deadline, request.send())
                    .await
                    .map_err(|_| deadline_error(&endpoint, "request", deadline))?
                    .with_context(|| format!("connect to the st3 API endpoint {endpoint}"))?;
                let status = response.status();
                let remaining = deadline.saturating_sub(started.elapsed());
                let bytes = tokio::time::timeout(remaining, response.bytes())
                    .await
                    .map_err(|_| deadline_error(&endpoint, "response", deadline))??
                    .to_vec();
                if !status.is_success() {
                    return Err(api_error(status.as_u16(), &bytes));
                }
                bytes
            }
        };
        let response: ApiResponse<O> =
            serde_json::from_slice(&response).context("decode the st3 API response envelope")?;
        anyhow::ensure!(
            response.api_version == "st3.v1",
            "the st3 API returned unsupported version {}",
            response.api_version
        );
        Ok(response.value)
    }

    pub async fn proxy_terminal(&self, name: &str, path: &str) -> Result<i32> {
        match &self.endpoint {
            Endpoint::Unix(socket) => {
                let endpoint = socket.display().to_string();
                let stream = tokio::time::timeout(
                    self.deadlines.connect,
                    tokio::net::UnixStream::connect(socket),
                )
                .await
                .map_err(|_| deadline_error(&endpoint, "connect", self.deadlines.connect))?
                .with_context(|| format!("connect to the st3 API at {endpoint}"))?;
                let request = terminal_request(&format!("ws://localhost{path}"))?;
                let (websocket, _) = tokio::time::timeout(
                    self.deadlines.terminal_handshake,
                    tokio_tungstenite::client_async(request, stream),
                )
                .await
                .map_err(|_| {
                    deadline_error(
                        &endpoint,
                        "terminal WebSocket handshake",
                        self.deadlines.terminal_handshake,
                    )
                })??;
                proxy_websocket(name, websocket).await
            }
            Endpoint::Http(base) => {
                let base = base
                    .strip_prefix("http://")
                    .map(|value| format!("ws://{value}"))
                    .or_else(|| {
                        base.strip_prefix("https://")
                            .map(|value| format!("wss://{value}"))
                    })
                    .context("a terminal endpoint must use http or https")?;
                let request = terminal_request(&format!("{base}{path}"))?;
                let endpoint = format!("{base}{path}");
                let (websocket, _) = tokio::time::timeout(
                    self.deadlines.terminal_handshake,
                    tokio_tungstenite::connect_async(request),
                )
                .await
                .map_err(|_| {
                    deadline_error(
                        &endpoint,
                        "terminal WebSocket handshake",
                        self.deadlines.terminal_handshake,
                    )
                })??;
                proxy_websocket(name, websocket).await
            }
        }
    }
}

fn terminal_request(url: &str) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "st3.terminal.v1"
            .parse()
            .expect("the protocol header is valid"),
    );
    Ok(request)
}

async fn proxy_websocket<S>(
    name: &str,
    websocket: tokio_tungstenite::WebSocketStream<S>,
) -> Result<i32>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (client_stream, bridge_stream) = StdUnixStream::pair()?;
    bridge_stream.set_nonblocking(true)?;
    let bridge_stream = tokio::net::UnixStream::from_std(bridge_stream)?;
    let bridge = tokio::spawn(bridge_terminal_protocol(websocket, bridge_stream));
    let name = name.to_owned();
    let outcome = tokio::task::spawn_blocking(move || {
        attach(
            AttachParams::new(&name, client_stream),
            &ClientIo::default(),
        )
    })
    .await
    .context("join the terminal client")?;
    bridge.abort();
    Ok(outcome.exit_code())
}

async fn bridge_terminal_protocol<S>(
    websocket: tokio_tungstenite::WebSocketStream<S>,
    stream: tokio::net::UnixStream,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut writer, mut reader) = websocket.split();
    let (mut input, mut output) = stream.into_split();
    let mut bytes = vec![0_u8; 65_536];
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    loop {
        tokio::select! {
            read = input.read(&mut bytes) => {
                let count = read?;
                if count == 0 {
                    writer.close().await?;
                    break;
                }
                writer.send(tokio_tungstenite::tungstenite::Message::Binary(bytes[..count].to_vec().into())).await?;
            }
            message = reader.next() => {
                match message {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                        output.write_all(&bytes).await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        output.write_all(text.as_bytes()).await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                }
            }
        }
    }
    Ok(())
}

async fn unix_request(
    socket: &Path,
    method: &str,
    path: &str,
    body: &[u8],
    connect_deadline: Duration,
    deadline: Duration,
) -> Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let started = tokio::time::Instant::now();
    let endpoint = socket.display().to_string();
    let mut stream = tokio::time::timeout(
        connect_deadline.min(deadline),
        tokio::net::UnixStream::connect(socket),
    )
    .await
    .map_err(|_| deadline_error(&endpoint, "connect", connect_deadline))?
    .with_context(|| {
        format!(
            "connect to the st3 API at {}; run `st3 up` first",
            socket.display()
        )
    })?;
    let remaining = deadline.saturating_sub(started.elapsed());
    let response = tokio::time::timeout(remaining, async {
        stream
            .write_all(
                format!(
                    "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        stream.write_all(body).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    })
    .await
    .map_err(|_| deadline_error(&endpoint, "request and response", deadline))??;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("the st3 API returned an incomplete HTTP response")?;
    let header = std::str::from_utf8(&response[..header_end])?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .context("the st3 API returned an invalid HTTP status")?;
    let mut body = response[(header_end + 4)..].to_vec();
    if header
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        body = decode_chunked(&body)?;
    }
    if !(200..300).contains(&status) {
        return Err(api_error(status, &body));
    }
    Ok(body)
}

fn request_deadline(path: &str, deadlines: ClientDeadlines) -> Duration {
    if path.starts_with("/v1/internal/replication/export")
        || path.starts_with("/v1/internal/replication/receive")
    {
        deadlines.bulk
    } else if path.starts_with("/v1/events?") && path.contains("wait=true") {
        deadlines.event
    } else {
        deadlines.request
    }
}

fn deadline_error(endpoint: &str, phase: &str, deadline: Duration) -> anyhow::Error {
    anyhow::anyhow!(
        "st3 API endpoint `{endpoint}` exceeded the {phase} limit of {} ms; the service may be busy or unavailable, retry the command",
        deadline.as_millis()
    )
}

fn decode_chunked(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut cursor = 0;
    let mut output = Vec::new();
    loop {
        let end = bytes[cursor..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| cursor + position)
            .context("invalid chunked API response")?;
        let length = usize::from_str_radix(std::str::from_utf8(&bytes[cursor..end])?.trim(), 16)?;
        cursor = end + 2;
        if length == 0 {
            break;
        }
        anyhow::ensure!(
            cursor + length <= bytes.len(),
            "truncated chunked API response"
        );
        output.extend_from_slice(&bytes[cursor..cursor + length]);
        cursor += length + 2;
    }
    Ok(output)
}

fn api_error(status: u16, bytes: &[u8]) -> anyhow::Error {
    if let Ok(error) = serde_json::from_slice::<ApiErrorResponse>(bytes) {
        return anyhow::anyhow!(
            "st3 API returned {status} {}: {}",
            error.code,
            error.message
        );
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes)
        && let Some(message) = value.get("message").and_then(|value| value.as_str())
    {
        return anyhow::anyhow!("st3 API returned {status}: {message}");
    }
    anyhow::anyhow!(
        "st3 API returned {status}: {}",
        String::from_utf8_lossy(bytes).trim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use tokio::net::UnixListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    fn test_api() -> Router {
        Router::new().route(
            "/v1/test",
            get(|| async { Json(test_envelope(json!({"method": "get"}))) })
                .post(|Json(body): Json<Value>| async move { Json(test_envelope(body)) }),
        )
    }

    fn test_envelope(value: Value) -> ApiResponse<Value> {
        ApiResponse {
            api_version: "st3.v1".into(),
            request_id: "test-request".into(),
            snapshot_host: "test-node".into(),
            store_index: 1,
            value,
        }
    }

    fn fast_client(endpoint: Endpoint) -> Client {
        let deadlines = ClientDeadlines {
            connect: Duration::from_millis(20),
            request: Duration::from_millis(30),
            bulk: Duration::from_millis(40),
            event: Duration::from_millis(50),
            terminal_handshake: Duration::from_millis(30),
        };
        Client {
            endpoint,
            http: reqwest::Client::builder()
                .connect_timeout(deadlines.connect)
                .build()
                .unwrap(),
            deadlines,
        }
    }

    #[test]
    fn request_classes_have_distinct_bounded_deadlines() {
        let deadlines = ClientDeadlines::default();
        assert_eq!(
            request_deadline("/v1/status", deadlines),
            Duration::from_secs(15)
        );
        assert_eq!(
            request_deadline("/v1/events?wait=true&timeout_ms=30000", deadlines),
            Duration::from_secs(35)
        );
        assert_eq!(
            request_deadline("/v1/internal/replication/export", deadlines),
            Duration::from_secs(120)
        );
    }

    #[tokio::test]
    async fn a_stalled_unix_response_fails_with_a_retryable_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("st3.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let error = fast_client(Endpoint::Unix(socket))
            .get::<Value>("/v1/status")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("request and response"));
        assert!(error.contains("retry the command"));
        server.abort();
    }

    #[tokio::test]
    async fn a_stalled_http_response_fails_with_a_retryable_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let error = fast_client(Endpoint::Http(format!("http://{address}")))
            .get::<Value>("/v1/status")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("request"));
        assert!(error.contains("retry the command"));
        server.abort();
    }

    async fn assert_request_transports(client: Client) {
        let get: Value = client.get("/v1/test").await.unwrap();
        assert_eq!(get, json!({"method": "get"}));
        let post: Value = client
            .post("/v1/test", &json!({"method": "post"}))
            .await
            .unwrap();
        assert_eq!(post, json!({"method": "post"}));
    }

    #[tokio::test]
    async fn the_unix_transport_completes_real_get_and_post_requests() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("st3.sock");
        let server_socket = socket.clone();
        let server = tokio::spawn(async move {
            crate::api::serve_unix(&server_socket, test_api())
                .await
                .unwrap();
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(socket.exists(), "the Unix API socket did not start");
        assert_request_transports(Client::unix(&socket)).await;
        server.abort();
    }

    #[tokio::test]
    async fn the_http_transport_completes_real_get_and_post_requests() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, test_api()).await.unwrap();
        });
        assert_request_transports(Client::new(Endpoint::Http(format!("http://{address}")))).await;
        server.abort();
    }

    #[tokio::test]
    async fn the_unix_terminal_transport_completes_a_real_websocket_handshake() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("st3.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let observed = Arc::new(Mutex::new(None));
        let server_observed = observed.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &Request, mut response: Response| {
                    *server_observed.lock().unwrap() = Some((
                        request.uri().path().to_owned(),
                        request
                            .headers()
                            .get("Sec-WebSocket-Protocol")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                    ));
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", "st3.terminal.v1".parse().unwrap());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            websocket.close(None).await.unwrap();
        });

        Client::unix(&socket)
            .proxy_terminal("demo", "/v1/pty/agent%2Fdemo/attach")
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some((
                "/v1/pty/agent%2Fdemo/attach".into(),
                Some("st3.terminal.v1".into())
            ))
        );
    }

    #[tokio::test]
    async fn the_http_terminal_transport_completes_a_real_websocket_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let observed = Arc::new(Mutex::new(None));
        let server_observed = observed.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &Request, mut response: Response| {
                    *server_observed.lock().unwrap() = Some((
                        request.uri().path().to_owned(),
                        request
                            .headers()
                            .get("Sec-WebSocket-Protocol")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                    ));
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", "st3.terminal.v1".parse().unwrap());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            websocket.close(None).await.unwrap();
        });

        Client::new(Endpoint::Http(format!("http://{address}")))
            .proxy_terminal("demo", "/v1/pty/agent%2Fdemo/attach")
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some((
                "/v1/pty/agent%2Fdemo/attach".into(),
                Some("st3.terminal.v1".into())
            ))
        );
    }
}
