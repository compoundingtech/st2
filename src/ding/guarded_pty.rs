use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const STATUS_PACKET: u8 = 7;
const GUARDED_DATA_PACKET: u8 = 9;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(super) struct PtyActivity {
    pub(super) state: String,
    pub(super) generation: String,
    #[serde(rename = "producerEpoch")]
    pub(super) producer_epoch: Option<String>,
    pub(super) sequence: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(super) struct PtySnapshot {
    pub(super) name: String,
    pub(super) generation: String,
    #[serde(rename = "ioRevision")]
    pub(super) io_revision: u64,
    pub(super) activity: PtyActivity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GuardedSend {
    Sent {
        revision: u64,
    },
    Conflict {
        actual_generation: String,
        actual_revision: u64,
        error: String,
    },
}

pub(super) trait GuardedPty {
    fn snapshot(&self) -> anyhow::Result<PtySnapshot>;
    fn compare_and_send(
        &self,
        generation: &str,
        revision: u64,
        bytes: &str,
    ) -> anyhow::Result<GuardedSend>;
    fn session_alive(&self) -> bool;
}

pub(super) struct SocketPty {
    session: String,
    root: PathBuf,
}

impl SocketPty {
    pub(super) fn new(session: impl Into<String>) -> anyhow::Result<Self> {
        let session = session.into();
        validate_session(&session)?;
        Ok(Self {
            session,
            root: super::pty_session_dir(),
        })
    }

    #[cfg(test)]
    fn with_root(session: impl Into<String>, root: PathBuf) -> Self {
        Self {
            session: session.into(),
            root,
        }
    }

    fn request(&self, packet_type: u8, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        let path = self.root.join(format!("{}.sock", self.session));
        let mut stream = UnixStream::connect(&path).map_err(|error| {
            anyhow::anyhow!("connecting to PTY socket {}: {error}", path.display())
        })?;
        stream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
        write_packet(&mut stream, packet_type, payload)?;
        let (response_type, response) = read_packet(&mut stream)?;
        if response_type != packet_type {
            anyhow::bail!("PTY returned packet type {response_type}, expected {packet_type}");
        }
        Ok(response)
    }
}

impl GuardedPty for SocketPty {
    fn snapshot(&self) -> anyhow::Result<PtySnapshot> {
        let response = self.request(STATUS_PACKET, &[])?;
        let snapshot: PtySnapshot = serde_json::from_slice(&response)
            .map_err(|error| anyhow::anyhow!("parsing PTY STATUS response: {error}"))?;
        if snapshot.name != self.session {
            anyhow::bail!(
                "PTY STATUS session mismatch: expected `{}`, got `{}`",
                self.session,
                snapshot.name
            );
        }
        Ok(snapshot)
    }

    fn compare_and_send(
        &self,
        generation: &str,
        revision: u64,
        bytes: &str,
    ) -> anyhow::Result<GuardedSend> {
        #[derive(Serialize)]
        struct Request<'a> {
            generation: &'a str,
            #[serde(rename = "ioRevision")]
            io_revision: u64,
            data: &'a str,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Response {
            ok: bool,
            generation: String,
            #[serde(rename = "ioRevision")]
            io_revision: u64,
            error: Option<String>,
        }

        let payload = serde_json::to_vec(&Request {
            generation,
            io_revision: revision,
            data: bytes,
        })?;
        let response: Response =
            serde_json::from_slice(&self.request(GUARDED_DATA_PACKET, &payload)?)
                .map_err(|error| anyhow::anyhow!("parsing PTY guarded-send response: {error}"))?;
        if response.ok {
            if response.generation != generation || response.io_revision <= revision {
                anyhow::bail!("PTY guarded-send success returned an invalid generation/revision");
            }
            Ok(GuardedSend::Sent {
                revision: response.io_revision,
            })
        } else {
            Ok(GuardedSend::Conflict {
                actual_generation: response.generation,
                actual_revision: response.io_revision,
                error: response
                    .error
                    .unwrap_or_else(|| "guard rejected".to_string()),
            })
        }
    }

    fn session_alive(&self) -> bool {
        super::session_alive(&self.session)
    }
}

fn validate_session(session: &str) -> anyhow::Result<()> {
    if session.is_empty()
        || session.len() > 255
        || !session
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        anyhow::bail!(
            "invalid PTY session `{session}`: expected 1-255 letters, digits, dots, dashes, or underscores"
        );
    }
    Ok(())
}

fn write_packet(stream: &mut UnixStream, packet_type: u8, payload: &[u8]) -> anyhow::Result<()> {
    let length = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("PTY packet payload is too large"))?;
    let mut header = [0_u8; 5];
    header[0] = packet_type;
    header[1..].copy_from_slice(&length.to_be_bytes());
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    Ok(())
}

fn read_packet(stream: &mut UnixStream) -> anyhow::Result<(u8, Vec<u8>)> {
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header[1..].try_into().expect("four-byte length")) as usize;
    if length > MAX_RESPONSE_BYTES {
        anyhow::bail!("PTY response exceeds {MAX_RESPONSE_BYTES} bytes");
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;
    Ok((header[0], payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::thread;

    fn serve_once(
        listener: &UnixListener,
        expected_type: u8,
        response: serde_json::Value,
    ) -> Vec<u8> {
        let (mut stream, _) = listener.accept().unwrap();
        let (packet_type, payload) = read_packet(&mut stream).unwrap();
        assert_eq!(packet_type, expected_type);
        write_packet(
            &mut stream,
            expected_type,
            &serde_json::to_vec(&response).unwrap(),
        )
        .unwrap();
        payload
    }

    #[test]
    fn reads_exact_activity_snapshot_and_sends_generation_revision_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("host.agent.sock");
        let listener = UnixListener::bind(socket).unwrap();
        let root = tmp.path().to_path_buf();
        let server = thread::spawn(move || {
            assert!(
                serve_once(
                    &listener,
                    STATUS_PACKET,
                    serde_json::json!({
                        "name": "host.agent",
                        "generation": "generation-a",
                        "ioRevision": 41,
                        "activity": {
                            "state": "idle",
                            "generation": "generation-a",
                            "producerEpoch": "epoch-a",
                            "sequence": 7
                        }
                    }),
                )
                .is_empty()
            );
            let payload = serve_once(
                &listener,
                GUARDED_DATA_PACKET,
                serde_json::json!({
                    "ok": true,
                    "generation": "generation-a",
                    "ioRevision": 42
                }),
            );
            let request: serde_json::Value = serde_json::from_slice(&payload).unwrap();
            assert_eq!(
                request,
                serde_json::json!({
                    "generation": "generation-a",
                    "ioRevision": 41,
                    "data": "notice"
                })
            );
        });

        let pty = SocketPty::with_root("host.agent", root);
        let snapshot = pty.snapshot().unwrap();
        assert_eq!(snapshot.activity.producer_epoch.as_deref(), Some("epoch-a"));
        assert_eq!(snapshot.activity.sequence, 7);
        assert_eq!(
            pty.compare_and_send("generation-a", 41, "notice").unwrap(),
            GuardedSend::Sent { revision: 42 }
        );
        server.join().unwrap();
    }

    #[test]
    fn guard_conflict_is_typed_and_never_reinterpreted_as_success() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("host.agent.sock");
        let listener = UnixListener::bind(socket).unwrap();
        let root = tmp.path().to_path_buf();
        let server = thread::spawn(move || {
            let _ = serve_once(
                &listener,
                GUARDED_DATA_PACKET,
                serde_json::json!({
                    "ok": false,
                    "generation": "generation-b",
                    "ioRevision": 9,
                    "error": "daemon generation mismatch"
                }),
            );
        });
        let pty = SocketPty::with_root("host.agent", root);
        assert_eq!(
            pty.compare_and_send("generation-a", 8, "notice").unwrap(),
            GuardedSend::Conflict {
                actual_generation: "generation-b".to_string(),
                actual_revision: 9,
                error: "daemon generation mismatch".to_string(),
            }
        );
        server.join().unwrap();
    }

    #[test]
    fn session_names_cannot_escape_the_pty_root() {
        assert!(SocketPty::new("../other").is_err());
        assert!(SocketPty::new("host.agent").is_ok());
    }
}
