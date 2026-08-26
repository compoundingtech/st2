use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use serde::de::DeserializeOwned;

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
}

impl Client {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            http: reqwest::Client::new(),
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
        let response = match &self.endpoint {
            Endpoint::Unix(socket) => {
                let socket = socket.clone();
                let method = method.to_owned();
                let path = path.to_owned();
                tokio::task::spawn_blocking(move || unix_request(&socket, &method, &path, &bytes))
                    .await??
            }
            Endpoint::Http(base) => {
                let url = format!("{base}{path}");
                let request = match method {
                    "GET" => self.http.get(url),
                    "POST" => self.http.post(url).body(bytes),
                    other => anyhow::bail!("unsupported HTTP method {other}"),
                }
                .header("content-type", "application/json")
                .header("connection", "close");
                let response = request.send().await.context("connect to the st3 API")?;
                let status = response.status();
                let bytes = response.bytes().await?.to_vec();
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

    pub async fn proxy_terminal(&self, path: &str) -> Result<()> {
        match &self.endpoint {
            Endpoint::Unix(socket) => {
                let stream = tokio::net::UnixStream::connect(socket)
                    .await
                    .with_context(|| format!("connect to the st3 API at {}", socket.display()))?;
                let request = tokio_tungstenite::tungstenite::http::Request::builder()
                    .uri(format!("ws://localhost{path}"))
                    .header("Host", "localhost")
                    .header("Sec-WebSocket-Protocol", "st3.terminal.v1")
                    .body(())?;
                let (websocket, _) = tokio_tungstenite::client_async(request, stream).await?;
                proxy_websocket(websocket).await
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
                let (websocket, _) =
                    tokio_tungstenite::connect_async(format!("{base}{path}")).await?;
                proxy_websocket(websocket).await
            }
        }
    }
}

async fn proxy_websocket<S>(websocket: tokio_tungstenite::WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let _raw = RawTerminal::enter();
    let (mut writer, mut reader) = websocket.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut input = vec![0_u8; 4096];
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    loop {
        tokio::select! {
            read = stdin.read(&mut input) => {
                let count = read?;
                if count == 0 {
                    writer.close().await?;
                    break;
                }
                writer.send(tokio_tungstenite::tungstenite::Message::Binary(input[..count].to_vec().into())).await?;
            }
            message = reader.next() => {
                match message {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                        stdout.write_all(&bytes).await?;
                        stdout.flush().await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        stdout.write_all(text.as_bytes()).await?;
                        stdout.flush().await?;
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

struct RawTerminal(Option<String>);

impl RawTerminal {
    fn enter() -> Self {
        let state = std::process::Command::new("stty")
            .arg("-g")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|value| value.trim().to_owned());
        if state.is_some() {
            let _ = std::process::Command::new("stty")
                .args(["raw", "-echo"])
                .status();
        }
        Self(state)
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        if let Some(state) = &self.0 {
            let _ = std::process::Command::new("stty").arg(state).status();
        }
    }
}

fn unix_request(socket: &Path, method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>> {
    let mut stream = UnixStream::connect(socket).with_context(|| {
        format!(
            "connect to the st3 API at {}; run `st3 up` first",
            socket.display()
        )
    })?;
    stream.write_all(
        format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
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
