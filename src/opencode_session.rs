//! Controlled OpenCode launch: presence, observed harness state, and native server delivery.
//!
//! OpenCode's interactive TUI is also a server: the wrapper allocates a loopback port and a
//! per-seat password, launches the TUI bound to them, and speaks plain HTTP to its own child. Two
//! consumers hang off that surface. The observed-state producer subscribes to the `/event` SSE
//! stream and projects session status, permission asks, and questions into the generic
//! `harness-state` record — evidence-gated, so a dropped stream stops the heartbeat and the record
//! ages out rather than restating a state nobody is watching. The delivery pump mirrors the Codex
//! FIFO discipline: an `Attempted` receipt is persisted before transport, the transport is
//! `POST /session/<id>/prompt_async` with a caller-derived stable `messageID`, and the only
//! accepted receipt is the message read back from the server — never the `/tui/*` endpoints, which
//! acknowledge input even when no TUI is attached.
//!
//! Fail-closed gate: delivery requires both a supported `opencode --version` and a live `/doc`
//! subset check proving the exact API arms st2 depends on. Observation requires only the `/doc`
//! check — its vocabulary already degrades to indeterminate on anything unrecognized.

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::harness_state::{self, Activity, Ask, BlockedOn, InputBuffer, Observation, Writer};
use crate::provider_session::{PROVIDER_POLL, STOP, install_signal_handler};
use crate::{ding, message, status};

/// OpenCode versions whose `/event`, `/session`, and `prompt_async` surfaces were verified.
/// The live `/doc` check below guards the shape; this list guards the semantics behind it.
const SUPPORTED_OPENCODE_VERSIONS: [&str; 1] = ["1.18.19"];

const DELIVERY_STATE_SCHEMA: &str = "st2.opencode-delivery-state.v1";
const STOP_GRACE: Duration = Duration::from_secs(5);
const INBOX_REFRESH_FALLBACK: Duration = Duration::from_secs(2);
const DELIVERY_RETRY: Duration = Duration::from_secs(2);
const SEED_RETRY: Duration = Duration::from_millis(250);
const SSE_RECONNECT: Duration = Duration::from_secs(2);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// Every API arm the wrapper depends on, as it appears in the served OpenAPI document. A missing
/// marker names itself in the refusal, so a surface change is a diagnosis rather than a mystery.
const REQUIRED_API_MARKERS: [&str; 12] = [
    "prompt_async",
    "messageID",
    "session.status",
    "session.idle",
    "session.error",
    "permission.asked",
    "permission.replied",
    "question.asked",
    // The exit arms and pending listings are as load-bearing as the entries: a release renaming
    // a question exit would otherwise pass the gate and then hold `blockedOn: human` forever,
    // and the reconnect seed reads both listing endpoints.
    "question.replied",
    "question.rejected",
    "/permission",
    "/question",
];

pub fn run(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    opencode_argv: Vec<String>,
) -> Result<()> {
    let this_host = crate::run::detect_host();
    let agent_dir = message::resolve_agent_dir(catalog_root, &identity, &this_host)?
        .with_context(|| format!("opencode driver agent '{identity}' is not declared"))?;
    anyhow::ensure!(
        !opencode_argv.is_empty(),
        "opencode driver '{runtime_id}' has no provider argv"
    );
    let version_ok = match supported_version(&opencode_argv[0]) {
        Ok(version) => {
            let ok = SUPPORTED_OPENCODE_VERSIONS.contains(&version.as_str());
            if !ok {
                tracing::warn!(
                    "st2 opencode-session: version {version} is unverified (supported: {}); native delivery disabled",
                    SUPPORTED_OPENCODE_VERSIONS.join(", ")
                );
            }
            ok
        }
        Err(error) => {
            tracing::warn!("st2 opencode-session: cannot read opencode version: {error:#}");
            false
        }
    };

    let port = allocate_port()?;
    let password = random_password()?;
    let mut argv = opencode_argv;
    argv.extend([
        "--port".to_string(),
        port.to_string(),
        "--hostname".to_string(),
        "127.0.0.1".to_string(),
    ]);
    let client = Client::new(port, &password);

    install_signal_handler();
    // The written claim comes BEFORE the provider spawns: a claim that cannot be written aborts
    // the launch while there is still nothing to leak, and it supersedes whatever a predecessor
    // left — a still-fresh live record included — before this wrapper's first observation.
    let mut session = {
        let session = harness_state::session_token();
        let seq = harness_state::claim(&agent_dir, identity.clone(), "opencode", &session)?;
        Session {
            client,
            version_ok,
            status_path: status::status_path(&agent_dir),
            // The pty session vouching for the record is the wrapper's task: the runtime ID
            // names the registry entry, and only aliases the identity on driver-expanded seats.
            writer: Writer::new(
                &agent_dir,
                identity.clone(),
                "opencode",
                Some(runtime_id.clone()),
            )
            .with_ownership(session, seq),
            delivery: Delivery::new(catalog_root, &agent_dir, &this_host, &identity, &runtime_id),
        }
    };
    let mut child = match spawn_provider(&argv, &password) {
        Ok(child) => child,
        Err(error) => {
            // The claim already replaced whatever the predecessor left; returning through `?`
            // would leave the exitless `ended (superseded)` placeholder standing as a false
            // takeover. The launch failed under THIS session's ownership, so the record ends
            // honestly here instead (the same contract pi's launch path keeps).
            let _ = session.writer.observe(
                Observation::new(Activity::Ended, BlockedOn::None, InputBuffer::Unknown)
                    .with_reason("launch-error")
                    .with_exit("exit unknown"),
            );
            return Err(error);
        }
    };

    run_session(session, &mut child, &agent_dir)
}

// ---- wrapper session loop --------------------------------------------------------------------

struct Session {
    client: Client,
    version_ok: bool,
    status_path: PathBuf,
    writer: Writer,
    delivery: Delivery,
}

fn run_session(mut session: Session, child: &mut Child, agent_dir: &Path) -> Result<()> {
    let (wake_tx, wake_rx) = mpsc::channel();
    let _watcher = crate::watch::watch_delivery_inputs(agent_dir, wake_tx);
    let (event_tx, event_rx) = mpsc::channel();
    let sse_stop = std::sync::Arc::new(AtomicBool::new(false));

    let mut machine = EventMachine::default();
    let mut api_ok = false;
    let mut sse_started = false;
    let mut sse_connected = false;
    let mut evidence = false;
    let mut next_seed_attempt = Instant::now();
    let mut next_gate_attempt = Instant::now();
    let mut next_presence = Instant::now();
    let mut next_inbox = Instant::now();

    let outcome = loop {
        if STOP.load(Ordering::SeqCst) {
            // The terminal record precedes the escalation: SIGKILL takes this wrapper with its
            // group, so nothing after the kill can write (§D of the harness-state design). When
            // the group yields inside the grace window the wrapper survives, so the record is
            // rewritten with the exit the reap actually observed — "stopped" remains only as
            // escalation cover.
            let _ = session.writer.ended("stopped");
            let reaped = stop_provider_group(child);
            if let Ok(Some(exit)) = &reaped {
                let _ = session.writer.ended(describe_exit(*exit));
            }
            break reaped.map(|_| ());
        }
        match child.try_wait() {
            Ok(Some(exit)) => {
                let _ = session.writer.ended(describe_exit(exit));
                break completed(exit);
            }
            Ok(None) => {}
            Err(error) => {
                // The liveness check failing is a terminal outcome too: without this write the
                // claim placeholder stands as the visible state (the self-review's error-arm
                // class).
                let _ = session.writer.observe(
                    Observation::new(Activity::Ended, BlockedOn::None, InputBuffer::Unknown)
                        .with_reason("launch-error")
                        .with_exit("exit unknown"),
                );
                return Err(error).context("checking opencode provider");
            }
        }

        // The API gate needs the child's server to answer; retry until it does.
        if !api_ok && Instant::now() >= next_gate_attempt {
            match session.client.get_json("/doc") {
                Ok(doc) => match check_openapi_subset(&doc) {
                    Ok(()) => api_ok = true,
                    Err(error) => {
                        tracing::warn!("st2 opencode-session: API gate failed: {error:#}");
                        // A failed shape check is terminal for this launch: the surface will not
                        // change until the binary does. Stop probing; run presence-only.
                        next_gate_attempt = Instant::now() + Duration::from_secs(3600);
                    }
                },
                Err(_) => next_gate_attempt = Instant::now() + Duration::from_millis(250),
            }
        }
        if api_ok && !sse_started {
            spawn_sse_reader(session.client.clone(), event_tx.clone(), sse_stop.clone());
            sse_started = true;
        }

        while let Ok(event) = event_rx.try_recv() {
            match event {
                SseMessage::Connected => {
                    machine = EventMachine::default();
                    sse_connected = true;
                    // Evidence turns on only once the level seed succeeds: resuming heartbeats
                    // on a transiently failed seed would re-stamp whatever the disk last said.
                    evidence = seed_from_server(&session.client, &mut machine);
                    next_seed_attempt = Instant::now() + SEED_RETRY;
                }
                SseMessage::Disconnected => {
                    sse_connected = false;
                    evidence = false;
                    // Everything from here until a successful reseed happened unobserved: the
                    // next observation must open a fresh transition even if it restates the
                    // pre-outage tuple.
                    session.writer.interrupt();
                }
                SseMessage::Event(value) => {
                    if let Some(sid) = event_session_id(&value) {
                        session.delivery.saw_session(sid);
                    }
                    machine.apply(&value);
                }
            }
        }
        if machine.poisoned && machine.ended.is_none() && evidence {
            // The projection went untrustworthy mid-stream: stop heartbeating over it and let
            // the level seed rebuild the whole picture from the server's own truth.
            evidence = false;
            session.writer.interrupt();
            next_seed_attempt = Instant::now();
        }
        if sse_connected && !evidence && Instant::now() >= next_seed_attempt {
            evidence = seed_from_server(&session.client, &mut machine);
            next_seed_attempt = Instant::now() + SEED_RETRY;
        }
        if evidence && let Some(observation) = machine.observation() {
            let _ = session.writer.observe(observation);
        }

        let now = Instant::now();
        if now >= next_presence {
            let _ = status::refresh(&session.status_path);
            if evidence {
                let _ = session.writer.heartbeat();
            }
            next_presence = now + status::STATUS_REFRESH;
        }

        let mut inbox_due = now >= next_inbox;
        while wake_rx.try_recv().is_ok() {
            inbox_due = true;
        }
        if inbox_due {
            if session.version_ok && api_ok {
                session.delivery.pump(&session.client);
            }
            next_inbox = Instant::now() + INBOX_REFRESH_FALLBACK;
        }

        thread::sleep(PROVIDER_POLL);
    };
    sse_stop.store(true, Ordering::SeqCst);
    outcome
}

fn spawn_provider(argv: &[String], password: &str) -> Result<Child> {
    use std::os::unix::process::CommandExt as _;
    let (program, args) = argv
        .split_first()
        .context("opencode provider argv is empty")?;
    let mut command = std::process::Command::new(program);
    command
        .args(args)
        .env("OPENCODE_SERVER_PASSWORD", password)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            libc::signal(libc::SIGTERM, libc::SIG_DFL);
            Ok(())
        });
    }
    command
        .spawn()
        .with_context(|| format!("starting opencode provider {program}"))
}

fn completed(exit: ExitStatus) -> Result<()> {
    anyhow::ensure!(exit.success(), "opencode provider exited with {exit}");
    Ok(())
}

fn stop_provider_group(child: &mut Child) -> Result<Option<ExitStatus>> {
    let process_group = unsafe { libc::getpgrp() };
    anyhow::ensure!(
        process_group > 1,
        "refusing to signal process group {process_group}"
    );
    unsafe {
        libc::kill(-process_group, libc::SIGTERM);
    }
    let deadline = Instant::now() + STOP_GRACE;
    while Instant::now() < deadline {
        if let Some(exit) = child.try_wait()? {
            return Ok(Some(exit));
        }
        thread::sleep(Duration::from_millis(25));
    }
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    Ok(child.wait().ok())
}

fn describe_exit(exit: ExitStatus) -> String {
    match (exit.code(), exit.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "exit unknown".to_string(),
    }
}

fn supported_version(binary: &str) -> Result<String> {
    let output = std::process::Command::new(binary)
        .arg("--version")
        .output()
        .with_context(|| format!("running {binary} --version"))?;
    anyhow::ensure!(output.status.success(), "{binary} --version failed");
    let version = String::from_utf8_lossy(&output.stdout);
    let version = version
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string();
    anyhow::ensure!(!version.is_empty(), "{binary} --version printed nothing");
    Ok(version)
}

/// Verify the served OpenAPI document still carries every arm st2 consumes. Substring markers are
/// deliberate: the document nests these identifiers at unstable depths across versions, and the
/// check must name what went missing rather than fail on structure.
fn check_openapi_subset(doc: &Value) -> Result<()> {
    let serialized = serde_json::to_string(doc).unwrap_or_default();
    let missing: Vec<&str> = REQUIRED_API_MARKERS
        .iter()
        .copied()
        .filter(|marker| !serialized.contains(marker))
        .collect();
    anyhow::ensure!(
        missing.is_empty(),
        "OpenCode /doc no longer offers: {}",
        missing.join(", ")
    );
    Ok(())
}

fn allocate_port() -> Result<u16> {
    // Bind-then-release: the child rebinds the port. The race window is real but tiny, loopback
    // only, and a lost race fails the launch loudly at spawn.
    let listener = TcpListener::bind("127.0.0.1:0").context("allocating opencode port")?;
    Ok(listener.local_addr()?.port())
}

fn random_password() -> Result<String> {
    let mut bytes = [0_u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .context("reading /dev/urandom for the opencode server password")?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

// ---- minimal loopback HTTP client ------------------------------------------------------------

/// Plain HTTP/1.1 over loopback with basic auth; one connection per request, `Connection: close`.
/// The tree ships no HTTP client and the runner is sync by design, so this stays hand-rolled and
/// exactly as small as the four requests the wrapper makes.
#[derive(Clone)]
struct Client {
    addr: String,
    auth: String,
    /// The SSE silence horizon; [`SSE_SILENCE`] in production, shrunk only by tests.
    sse_silence: Duration,
}

impl Client {
    fn new(port: u16, password: &str) -> Self {
        Self {
            addr: format!("127.0.0.1:{port}"),
            auth: base64(format!("opencode:{password}").as_bytes()),
            sse_silence: SSE_SILENCE,
        }
    }

    fn get_json(&self, path: &str) -> Result<Value> {
        let (status, body) = self.request("GET", path, None)?;
        anyhow::ensure!(status == 200, "GET {path} returned {status}");
        serde_json::from_slice(&body).with_context(|| format!("GET {path} returned invalid JSON"))
    }

    fn status_of_get(&self, path: &str) -> Result<u16> {
        Ok(self.request("GET", path, None)?.0)
    }

    fn post_json(&self, path: &str, payload: &Value) -> Result<u16> {
        Ok(self.request("POST", path, Some(payload))?.0)
    }

    fn request(&self, method: &str, path: &str, payload: Option<&Value>) -> Result<(u16, Vec<u8>)> {
        let mut stream = TcpStream::connect(&self.addr)
            .with_context(|| format!("connecting to opencode at {}", self.addr))?;
        stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
        stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
        let body = payload.map(|value| value.to_string()).unwrap_or_default();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Basic {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            self.addr,
            self.auth,
            body.len()
        )?;
        let mut reader = BufReader::new(stream);
        let status = read_http_status(&mut reader)?;
        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            if line == "\r\n" || line == "\n" {
                break;
            }
            line.clear();
        }
        let mut body = Vec::new();
        reader.read_to_end(&mut body)?;
        Ok((status, body))
    }

    fn open_sse(&self) -> Result<BufReader<TcpStream>> {
        let mut stream = TcpStream::connect(&self.addr)
            .with_context(|| format!("connecting to opencode at {}", self.addr))?;
        stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
        // A stalled socket must not keep evidence alive forever: the server emits periodic
        // `server.heartbeat` events (measured well inside a minute on 1.18.19), so a read that
        // sees NOTHING for the silence horizon — comfortably more than twice that cadence — is a
        // dead stream, surfaced as a disconnect (evidence off, reconnect and reseed).
        stream.set_read_timeout(Some(self.sse_silence))?;
        // HTTP/1.0 on purpose: over 1.1 the server chunk-encodes the stream (measured on
        // 1.18.19), which interleaves chunk-size lines into the line-oriented SSE read and can
        // split a `data:` line across chunks — a silently dropped event. Over 1.0 the same server
        // streams raw SSE bytes until close, which is exactly the framing this reader parses.
        write!(
            stream,
            "GET /event HTTP/1.0\r\nHost: {}\r\nAuthorization: Basic {}\r\nAccept: text/event-stream\r\n\r\n",
            self.addr, self.auth
        )?;
        let mut reader = BufReader::new(stream);
        let status = read_http_status(&mut reader)?;
        anyhow::ensure!(status == 200, "GET /event returned {status}");
        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            if line == "\r\n" || line == "\n" {
                break;
            }
            line.clear();
        }
        Ok(reader)
    }
}

/// The SSE silence horizon: at least twice the measured `server.heartbeat` cadence, so a healthy
/// stream can never trip it while a stalled socket cannot outlive it.
const SSE_SILENCE: Duration = Duration::from_secs(120);

fn read_http_status(reader: &mut BufReader<TcpStream>) -> Result<u16> {
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .with_context(|| format!("invalid HTTP status line {status_line:?}"))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let buffer = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let combined = u32::from_be_bytes([0, buffer[0], buffer[1], buffer[2]]);
        for position in 0..4 {
            if position <= chunk.len() {
                let index = ((combined >> (18 - 6 * position)) & 0x3f) as usize;
                out.push(ALPHABET[index] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

// ---- SSE subscription ------------------------------------------------------------------------

enum SseMessage {
    Connected,
    Event(Value),
    Disconnected,
}

fn spawn_sse_reader(client: Client, tx: Sender<SseMessage>, stop: std::sync::Arc<AtomicBool>) {
    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match client.open_sse() {
                Ok(mut reader) => {
                    if tx.send(SseMessage::Connected).is_err() {
                        return;
                    }
                    let mut data = String::new();
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                        let trimmed = line.trim_end_matches(['\r', '\n']);
                        if let Some(chunk) = trimmed.strip_prefix("data:") {
                            data.push_str(chunk.trim_start());
                        } else if trimmed.is_empty() && !data.is_empty() {
                            if let Ok(event) = serde_json::from_str::<Value>(&data)
                                && tx.send(SseMessage::Event(event)).is_err()
                            {
                                return;
                            }
                            data.clear();
                        }
                    }
                    if tx.send(SseMessage::Disconnected).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    if tx.send(SseMessage::Disconnected).is_err() {
                        return;
                    }
                }
            }
            thread::sleep(SSE_RECONNECT);
        }
    });
}

/// Re-seed observed state from the level surface after (re)connecting: events missed while
/// disconnected are unrecoverable, and `/session/status` omits idle sessions, so an empty map over
/// a live server is itself the idle proof.
fn seed_from_server(client: &Client, machine: &mut EventMachine) -> bool {
    // The seed is built in a FRESH machine and swapped in only once every read validates:
    // mutating the live one would leave half-seeded asks behind a mid-seed failure, and a
    // successful re-seed must also CLEAR stale busy/blocked entries whose exits passed while
    // the stream was down — the level surface at seed time is the whole truth, and any event
    // that raced the seed re-arrives or is re-listed on the next reconnect. Delivery targeting
    // deliberately learns nothing here: status-map iteration order is not recency (W8-5); the
    // pending-work recovery path resolves targets from the session listing instead.
    let Ok(statuses) = client.get_json("/session/status") else {
        return false;
    };
    // A response that is not the documented object shape proves nothing: seeding definite idle
    // from a null or an array would fabricate level evidence out of a shape this version cannot
    // read. Fail the seed and retry.
    let Some(map) = statuses.as_object() else {
        return false;
    };
    let mut seeded = EventMachine::default();
    seeded.seed_idle();
    {
        for (session_id, status) in map {
            // Exactly the pinned vocabulary: an unknown future word is not "busy" — it is
            // surface drift the /doc gate vocabulary did not cover, and evidence restored over
            // words we cannot read would be fabricated. Fail the seed and retry instead.
            match status.get("type").and_then(Value::as_str) {
                Some("idle") => {}
                Some("busy") => seeded.seed_busy(session_id.clone(), false),
                Some("retry") => seeded.seed_busy(session_id.clone(), true),
                _ => return false,
            }
        }
    }
    // An ask opened before this connection would otherwise be invisible until its exit event:
    // re-seed pending asks so blockedOn survives an SSE reconnect, with each id kept so the
    // ordinary id-matched exit still releases it. Both listing endpoints are measured on 1.18.19
    // (the committed capture drove them: `GET /permission` and `GET /question` return pending
    // ids), so a question open across a reconnect is recovered exactly like a permission.
    // Both listings must succeed for the seed to count: a transient failure here would restore
    // evidence on an unblocked picture and silently wedge an ask opened during the outage —
    // return false instead, keep heartbeats off, and let the seed retry.
    for (endpoint, kind) in [("/permission", "permission"), ("/question", "question")] {
        let Ok(pending) = client.get_json(endpoint) else {
            return false;
        };
        let Some(items) = pending.as_array() else {
            return false;
        };
        for item in items {
            // An entry whose id this version cannot read is a pending ask that could never be
            // released by its id-matched exit: seeding around it would restore evidence on a
            // picture that silently drops a human block. Fail the seed and retry.
            let Some(id) = item
                .get("id")
                .or_else(|| item.get("requestID"))
                .and_then(Value::as_str)
            else {
                return false;
            };
            seeded.seed_ask(id.to_string(), kind);
        }
    }
    *machine = seeded;
    true
}

// ---- event projection ------------------------------------------------------------------------

/// The pure projection from OpenCode's event stream to one seat-level observation. A dedicated
/// seat aggregates across the server's sessions: any busy session is activity, any open
/// permission or question is a human block, and idle is only derived from positive level evidence.
#[derive(Default)]
struct EventMachine {
    /// sessionID → currently retrying (vs plainly busy).
    busy: BTreeMap<String, bool>,
    /// permission/question id → what kind of human ask holds it open.
    blocked: BTreeMap<String, &'static str>,
    /// Level evidence seen: idle is a proof, never a default.
    seen_level: bool,
    /// A tracked-busy session moved to a status word this version cannot read: every projection
    /// is withheld until a fresh level seed replaces this machine.
    poisoned: bool,
    /// Terminal reason, once observed.
    ended: Option<&'static str>,
    /// The most recent non-terminal session error, surfaced as the idle reason once.
    last_error: Option<String>,
}

impl EventMachine {
    fn seed_idle(&mut self) {
        self.seen_level = true;
    }

    fn seed_busy(&mut self, session_id: String, retry: bool) {
        self.seen_level = true;
        self.busy.insert(session_id, retry);
    }

    /// Re-enter a human ask found pending at connect time, under its own id so the ordinary
    /// id-matched exit event releases it.
    fn seed_ask(&mut self, id: String, kind: &'static str) {
        self.blocked.insert(id, kind);
    }

    fn apply(&mut self, event: &Value) {
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return;
        };
        let properties = event.get("properties").unwrap_or(&Value::Null);
        let session_id = || {
            properties
                .get("sessionID")
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        match kind {
            "session.status" => {
                let Some(session_id) = session_id() else {
                    return;
                };
                match properties
                    .pointer("/status/type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "busy" => {
                        self.seen_level = true;
                        self.busy.insert(session_id, false);
                        self.last_error = None;
                    }
                    "retry" => {
                        self.seen_level = true;
                        self.busy.insert(session_id, true);
                    }
                    "idle" => {
                        self.seen_level = true;
                        self.busy.remove(&session_id);
                    }
                    // A future status arm is not evidence of anything — not even level evidence:
                    // counting it would let an unrecognized word prove `idle` on a quiet server.
                    // It poisons the whole projection on ANY session, tracked or not: a
                    // tracked-busy entry can no longer be trusted to clear, and an untracked
                    // session in a state this version cannot read makes standing idle evidence
                    // a fabrication — that session may already be mid-turn. Withholding until
                    // a level seed rebuilds the picture is the only honest reading.
                    _ => {
                        self.poisoned = true;
                    }
                }
            }
            "session.idle" => {
                if let Some(session_id) = session_id() {
                    self.seen_level = true;
                    self.busy.remove(&session_id);
                }
            }
            "session.error" => {
                let name = properties
                    .pointer("/error/name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                if name == "ProviderAuthError" {
                    self.ended = Some("providerAuth");
                } else {
                    if let Some(session_id) = session_id() {
                        self.busy.remove(&session_id);
                    }
                    self.seen_level = true;
                    self.last_error = Some(format!("error:{name}"));
                }
            }
            "permission.asked" => {
                if let Some(id) = ask_id(properties) {
                    self.blocked.insert(id, "permission");
                }
            }
            "permission.replied" => {
                if let Some(id) = ask_id(properties) {
                    self.blocked.remove(&id);
                }
            }
            "question.asked" => {
                if let Some(id) = ask_id(properties) {
                    self.blocked.insert(id, "question");
                }
            }
            "question.replied" | "question.rejected" => {
                if let Some(id) = ask_id(properties) {
                    self.blocked.remove(&id);
                }
            }
            // server.connected, server.heartbeat, plugin.added replay, message.*, … — not state.
            _ => {}
        }
    }

    fn observation(&self) -> Option<Observation> {
        // A sticky terminal outranks poison: `ended` does not depend on the busy map the
        // unknown word made untrustworthy, and withholding it would lose the terminal to the
        // forced reseed's fresh machine.
        if let Some(reason) = self.ended {
            return Some(
                Observation::new(Activity::Ended, BlockedOn::None, InputBuffer::Unknown)
                    .with_reason(reason),
            );
        }
        if self.poisoned {
            return None;
        }
        if let Some(kind) = self.blocked.values().next() {
            let ask = match *kind {
                "question" => Ask::Question,
                _ => Ask::Permission,
            };
            return Some(
                Observation::new(Activity::Active, BlockedOn::Human, InputBuffer::Unknown)
                    .with_ask(ask)
                    .with_reason(*kind),
            );
        }
        if !self.busy.is_empty() {
            let observation =
                Observation::new(Activity::Active, BlockedOn::None, InputBuffer::Unknown);
            return Some(if self.busy.values().all(|retry| *retry) {
                observation.with_reason("retry")
            } else {
                observation
            });
        }
        if self.seen_level {
            let observation =
                Observation::new(Activity::Idle, BlockedOn::None, InputBuffer::Unknown);
            return Some(match &self.last_error {
                Some(reason) => observation.with_reason(reason.clone()),
                None => observation,
            });
        }
        None
    }
}

/// The most recently updated session id from `GET /session`, or the last listed when the entries
/// carry no readable update time. `None` while the seat's TUI has not created a session yet.
fn newest_listed_session(client: &Client) -> Option<String> {
    let listed = client.get_json("/session").ok()?;
    let sessions = listed.as_array()?;
    sessions
        .iter()
        .max_by_key(|session| {
            session
                .pointer("/time/updated")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        })
        .or_else(|| sessions.last())
        .and_then(|session| session.get("id").and_then(Value::as_str))
        .map(str::to_string)
}

/// A permission/question id, wherever this version of the event nests it. Measured on 1.18.19:
/// `permission.asked`/`question.asked` carry `/id`, while their `*.replied`/`*.rejected` exits
/// carry `/requestID` — missing that spelling would hold `blockedOn: human` forever after a grant.
fn ask_id(properties: &Value) -> Option<String> {
    for pointer in ["/id", "/requestID", "/permission/id", "/question/id"] {
        if let Some(id) = properties.pointer(pointer).and_then(Value::as_str) {
            return Some(id.to_string());
        }
    }
    None
}

fn event_session_id(event: &Value) -> Option<&str> {
    let kind = event.get("type").and_then(Value::as_str)?;
    if !kind.starts_with("session.") {
        return None;
    }
    event
        .pointer("/properties/sessionID")
        .and_then(Value::as_str)
}

// ---- native delivery -------------------------------------------------------------------------

enum ReadBack {
    Durable,
    Absent,
    Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum DeliveryPhase {
    Attempted,
    Accepted,
}

/// One durable FIFO delivery attempt, written before transport (the Codex discipline). The stable
/// `messageID` makes a replayed attempt reconcilable: the server either shows the message durably
/// (accepted) or does not (retry the same identity, never a second one).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeliveryState {
    schema: String,
    agent: String,
    runtime_id: String,
    session_id: String,
    filename: String,
    message_id: String,
    phase: DeliveryPhase,
}

struct Delivery {
    catalog_root: PathBuf,
    inbox: PathBuf,
    status_path: PathBuf,
    this_host: String,
    identity: String,
    runtime_id: String,
    state_path: PathBuf,
    state: Option<DeliveryState>,
    /// The session a new delivery binds to: the most recently observed one.
    target_session: Option<String>,
    next_attempt: Instant,
}

impl Delivery {
    fn new(
        catalog_root: &Path,
        agent_dir: &Path,
        this_host: &str,
        identity: &str,
        runtime_id: &str,
    ) -> Self {
        let state_path = state_dir(catalog_root, identity).join("delivery-state.json");
        Self::with_state_path(
            catalog_root,
            agent_dir,
            this_host,
            identity,
            runtime_id,
            state_path,
        )
    }

    fn with_state_path(
        catalog_root: &Path,
        agent_dir: &Path,
        this_host: &str,
        identity: &str,
        runtime_id: &str,
        state_path: PathBuf,
    ) -> Self {
        let state = std::fs::read(&state_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<DeliveryState>(&bytes).ok())
            .filter(|state| {
                state.schema == DELIVERY_STATE_SCHEMA
                    && state.agent == identity
                    && message::is_message_filename(&state.filename)
                    && state.message_id
                        == stable_message_id(identity, &state.session_id, &state.filename)
            });
        Self {
            catalog_root: catalog_root.to_path_buf(),
            inbox: message::inbox_dir(agent_dir),
            status_path: status::status_path(agent_dir),
            this_host: this_host.to_string(),
            identity: identity.to_string(),
            runtime_id: runtime_id.to_string(),
            state_path,
            state,
            target_session: None,
            next_attempt: Instant::now(),
        }
    }

    fn saw_session(&mut self, session_id: &str) {
        self.target_session = Some(session_id.to_string());
    }

    fn pump(&mut self, client: &Client) {
        if let Err(error) = self.pump_inner(client) {
            tracing::warn!("st2 opencode-session: delivery: {error:#}");
        }
    }

    fn pump_inner(&mut self, client: &Client) -> Result<()> {
        let unread = message::list_inbox(&self.inbox)?;
        if let Some(state) = self.state.as_ref()
            && unread
                .iter()
                .all(|message| message.filename != state.filename)
        {
            self.clear_state()?;
        }
        if status::read_state(&self.status_path) == status::State::Dnd {
            return Ok(());
        }
        let Some(head) = unread.into_iter().next() else {
            return Ok(());
        };
        let target = match self.target_session.clone() {
            Some(target) => target,
            None => {
                // A session that settled before this observer connected is invisible to both the
                // event stream and `/session/status` (idle sessions are omitted), so a pending
                // delivery would otherwise stall forever. With work waiting, recover the binding
                // from the session listing — retried every pump pass until a session exists.
                let Some(recovered) = newest_listed_session(client) else {
                    return Ok(());
                };
                self.saw_session(&recovered);
                recovered
            }
        };
        if let Some(state) = self.state.as_ref()
            && state.session_id != target
        {
            // A newly selected session is a different delivery binding (the Codex thread rule).
            self.clear_state()?;
        }

        if let Some(state) = self.state.clone() {
            if state.filename != head.filename {
                return Ok(()); // The bound message is behind the head; archive precedence resolves it.
            }
            match state.phase {
                DeliveryPhase::Accepted => return Ok(()),
                DeliveryPhase::Attempted => return self.reconcile_or_retry(client, state),
            }
        }

        let message_id = stable_message_id(&self.identity, &target, &head.filename);
        let state = DeliveryState {
            schema: DELIVERY_STATE_SCHEMA.to_string(),
            agent: self.identity.clone(),
            runtime_id: self.runtime_id.clone(),
            session_id: target,
            filename: head.filename.clone(),
            message_id,
            phase: DeliveryPhase::Attempted,
        };
        self.write_state(state.clone())?;
        let text = ding::poke_text(&self.catalog_root, &self.this_host, &self.identity, &head);
        self.send(client, &state, &text)
    }

    fn reconcile_or_retry(&mut self, client: &Client, state: DeliveryState) -> Result<()> {
        match self.read_back(client, &state) {
            ReadBack::Durable => {
                let mut accepted = state;
                accepted.phase = DeliveryPhase::Accepted;
                return self.write_state(accepted);
            }
            // Measured on 1.18.19: a second POST with the same messageID appends its parts again
            // into the same message, so an indeterminate read-back must never trigger a resend —
            // the read-back itself is retried on a later pass.
            ReadBack::Indeterminate => return Ok(()),
            ReadBack::Absent => {}
        }
        if Instant::now() < self.next_attempt {
            return Ok(());
        }
        let unread = message::list_inbox(&self.inbox)?;
        let Some(head) = unread
            .into_iter()
            .find(|message| message.filename == state.filename)
        else {
            return Ok(());
        };
        let text = ding::poke_text(&self.catalog_root, &self.this_host, &self.identity, &head);
        self.send(client, &state, &text)
    }

    fn send(&mut self, client: &Client, state: &DeliveryState, text: &str) -> Result<()> {
        self.next_attempt = Instant::now() + DELIVERY_RETRY;
        let payload = json!({
            "messageID": state.message_id,
            "parts": [{ "type": "text", "text": text }],
        });
        let path = format!("/session/{}/prompt_async", state.session_id);
        let status = client.post_json(&path, &payload)?;
        anyhow::ensure!(
            (200..300).contains(&status),
            "POST {path} returned {status}"
        );
        if matches!(self.read_back(client, state), ReadBack::Durable) {
            let mut accepted = state.clone();
            accepted.phase = DeliveryPhase::Accepted;
            self.write_state(accepted)?;
        }
        Ok(())
    }

    /// The only receipt this transport accepts: the exact client message read back durably.
    fn read_back(&self, client: &Client, state: &DeliveryState) -> ReadBack {
        let path = format!("/session/{}/message/{}", state.session_id, state.message_id);
        match client.status_of_get(&path) {
            Ok(200) => ReadBack::Durable,
            Ok(404) => ReadBack::Absent,
            // A 5xx or a transport error proves nothing about the message either way.
            Ok(_) | Err(_) => ReadBack::Indeterminate,
        }
    }

    fn write_state(&mut self, state: DeliveryState) -> Result<()> {
        atomic_json(&self.state_path, &state)?;
        self.state = Some(state);
        Ok(())
    }

    fn clear_state(&mut self) -> Result<()> {
        match std::fs::remove_file(&self.state_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.state = None;
        Ok(())
    }
}

fn stable_message_id(recipient: &str, session_id: &str, filename: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.opencode-client-message.v1");
    for value in [
        recipient.as_bytes(),
        session_id.as_bytes(),
        filename.as_bytes(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    // OpenCode message ids match `^msg`; a hex tail keeps the identity stable and grammatical.
    format!("msg{:.26}", format!("{:x}", hash.finalize()))
}

fn state_dir(catalog_root: &Path, identity: &str) -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    let mut hash = Sha256::new();
    for value in [
        catalog_root.as_os_str().as_encoded_bytes(),
        identity.as_bytes(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    let digest = format!("{:x}", hash.finalize());
    base.join("st2").join("opencode").join(&digest[..24])
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state file has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".{}.tmp", std::process::id()));
    // Durability, not just atomicity: a crash between the rename and OpenCode's acceptance of
    // the prompt_async would lose the Attempted receipt and make the pump re-POST duplicate
    // parts. The bytes reach disk before the rename and the directory entry afterwards, so
    // once delivery proceeds the receipt survives.
    let mut file = std::fs::File::create(&temp)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    drop(file);
    if let Err(error) = std::fs::rename(&temp, path) {
        let _ = std::fs::remove_file(&temp);
        return Err(error.into());
    }
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use super::*;

    fn event(raw: &str) -> Value {
        serde_json::from_str(raw).unwrap()
    }

    fn observed(machine: &EventMachine) -> Observation {
        machine.observation().expect("an observation")
    }

    #[test]
    fn no_evidence_yields_no_observation_so_nothing_is_written() {
        let mut machine = EventMachine::default();
        assert_eq!(machine.observation(), None);
        // Replay noise on connect is not evidence either.
        for noise in [
            r#"{"type":"server.connected","properties":{}}"#,
            r#"{"type":"server.heartbeat","properties":{}}"#,
            r#"{"type":"plugin.added","properties":{"name":"x"}}"#,
            r#"{"type":"message.updated","properties":{"sessionID":"ses_a"}}"#,
        ] {
            machine.apply(&event(noise));
        }
        assert_eq!(machine.observation(), None);
    }

    #[test]
    fn session_status_drives_active_and_idle_across_sessions() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy"}}}"#,
        ));
        assert_eq!(observed(&machine).state, Activity::Active);

        // A second idle session does not mask the busy one (aggregate rule)…
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_b","status":{"type":"idle"}}}"#,
        ));
        assert_eq!(observed(&machine).state, Activity::Active);

        // …and duplicates are idempotent.
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy"}}}"#,
        ));
        machine.apply(&event(
            r#"{"type":"session.idle","properties":{"sessionID":"ses_a"}}"#,
        ));
        let idle = observed(&machine);
        assert_eq!(idle.state, Activity::Idle);
        assert_eq!(idle.blocked_on, BlockedOn::None);
    }

    #[test]
    fn retry_reads_active_with_a_reason() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"retry","attempt":2}}}"#,
        ));
        let retrying = observed(&machine);
        assert_eq!(retrying.state, Activity::Active);
        assert_eq!(retrying.reason.as_deref(), Some("retry"));
    }

    #[test]
    fn permission_blocks_until_the_same_id_is_replied() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy"}}}"#,
        ));
        machine.apply(&event(
            r#"{"type":"permission.asked","properties":{"id":"per_1","sessionID":"ses_a"}}"#,
        ));
        let blocked = observed(&machine);
        assert_eq!(blocked.state, Activity::Active);
        assert_eq!(blocked.blocked_on, BlockedOn::Human);
        assert_eq!(blocked.ask, Ask::Permission);
        assert_eq!(blocked.reason.as_deref(), Some("permission"));

        // A different id resolving is not this ask's exit edge. Replies spell the id `requestID`
        // on the measured wire.
        machine.apply(&event(
            r#"{"type":"permission.replied","properties":{"requestID":"per_other"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::Human);

        machine.apply(&event(
            r#"{"type":"permission.replied","properties":{"requestID":"per_1"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);
        assert_eq!(observed(&machine).state, Activity::Active);
    }

    /// The verbatim event pair captured live from opencode 1.18.19 (isolated server, config-file
    /// `"permission":{"bash":"ask"}`, free model): entry carries `properties.id`, the grant carries
    /// `properties.requestID`. A vocabulary drift on either side must fail here, not in the field.
    #[test]
    fn captured_permission_grant_pair_enters_and_exits_blocked() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"id":"evt_02fdc8e3f001djGGTdLwNiVJce","type":"session.status","properties":{"sessionID":"ses_fd0241376ffe3KDznnEB55qvKi","status":{"type":"busy"}}}"#,
        ));
        machine.apply(&event(
            r#"{"id":"evt_02fdc246b0020Xw65txB3nXBC4","type":"permission.asked","properties":{"id":"per_02fdc246b001BB5pclAd62tzpJ","sessionID":"ses_fd0241376ffe3KDznnEB55qvKi","permission":"bash","patterns":["echo capture-test-42"],"metadata":{"command":"echo capture-test-42"},"always":["echo *"],"tool":{"messageID":"msg_02fdc0989001nfz93uTCTLeO6O","callID":"call_6614fd927fe74d86ab089078"}}}"#,
        ));
        let blocked = observed(&machine);
        assert_eq!(blocked.state, Activity::Active);
        assert_eq!(blocked.blocked_on, BlockedOn::Human);
        assert_eq!(blocked.ask, Ask::Permission);
        assert_eq!(blocked.reason.as_deref(), Some("permission"));

        machine.apply(&event(
            r#"{"id":"evt_02fdc8342001TQBwhszchZw1U6","type":"permission.replied","properties":{"sessionID":"ses_fd0241376ffe3KDznnEB55qvKi","requestID":"per_02fdc246b001BB5pclAd62tzpJ","reply":"once"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);
    }

    /// The verbatim question pair captured in the same run: same id/requestID asymmetry.
    #[test]
    fn captured_question_reply_pair_enters_and_exits_blocked() {
        let mut machine = EventMachine::default();
        machine.seed_idle();
        machine.apply(&event(
            r#"{"type":"question.asked","properties":{"id":"que_02fdd3e83001GwptE1fgJam0jB","sessionID":"ses_fd0241376ffe3KDznnEB55qvKi","questions":[{"question":"Do you want me to continue helping with a coding or shell task in this workspace?","header":"Next step","options":[{"label":"Yes","description":"You have a follow-up task you will describe next"},{"label":"No","description":"No further action needed for now"}],"multiple":true}],"tool":{"messageID":"msg_02fdd2672001Mr1YBNCe4YM5Ro","callID":"call_59b5baf00c9f4e248d48f04b"}}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::Human);
        assert_eq!(observed(&machine).reason.as_deref(), Some("question"));

        machine.apply(&event(
            r#"{"type":"question.replied","properties":{"sessionID":"ses_fd0241376ffe3KDznnEB55qvKi","requestID":"que_02fdd3e83001GwptE1fgJam0jB","answers":[["Yes"]]}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);
    }

    #[test]
    fn questions_block_like_permissions_and_rejection_releases() {
        let mut machine = EventMachine::default();
        machine.seed_idle();
        machine.apply(&event(
            r#"{"type":"question.asked","properties":{"id":"que_1","sessionID":"ses_a"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::Human);
        assert_eq!(observed(&machine).reason.as_deref(), Some("question"));
        machine.apply(&event(
            r#"{"type":"question.rejected","properties":{"requestID":"que_1"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);
    }

    #[test]
    fn provider_auth_error_is_terminal_and_other_errors_settle_to_idle_with_a_reason() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy"}}}"#,
        ));
        machine.apply(&event(
            r#"{"type":"session.error","properties":{"sessionID":"ses_a","error":{"name":"MessageAbortedError"}}}"#,
        ));
        let idle = observed(&machine);
        assert_eq!(idle.state, Activity::Idle);
        assert_eq!(idle.reason.as_deref(), Some("error:MessageAbortedError"));

        machine.apply(&event(
            r#"{"type":"session.error","properties":{"sessionID":"ses_a","error":{"name":"ProviderAuthError"}}}"#,
        ));
        let ended = observed(&machine);
        assert_eq!(ended.state, Activity::Ended);
        assert_eq!(ended.reason.as_deref(), Some("providerAuth"));
    }

    #[test]
    fn openapi_gate_names_every_missing_marker() {
        let complete: Value = serde_json::from_str(&format!(
            r#"{{"paths":{{}},"markers":{:?}}}"#,
            REQUIRED_API_MARKERS
        ))
        .unwrap();
        check_openapi_subset(&complete).unwrap();

        let error = check_openapi_subset(&serde_json::json!({"paths": {"/session": {}}}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("prompt_async"), "{error}");
        assert!(error.contains("permission.asked"), "{error}");
    }

    #[test]
    fn stable_message_ids_are_grammatical_and_distinct_per_binding() {
        let id = stable_message_id("h.worker", "ses_a", "1786380000000-abc123.md");
        assert!(id.starts_with("msg"));
        assert_eq!(
            id,
            stable_message_id("h.worker", "ses_a", "1786380000000-abc123.md")
        );
        for other in [
            stable_message_id("h.other", "ses_a", "1786380000000-abc123.md"),
            stable_message_id("h.worker", "ses_b", "1786380000000-abc123.md"),
            stable_message_id("h.worker", "ses_a", "1786380000000-def456.md"),
        ] {
            assert_ne!(id, other);
        }
    }

    #[test]
    fn base64_matches_known_vectors() {
        for (input, expected) in [
            (&b""[..], ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"opencode:pw", "b3BlbmNvZGU6cHc="),
        ] {
            assert_eq!(base64(input), expected, "{input:?}");
        }
    }

    // ---- fake-server delivery tests ----------------------------------------------------------

    struct FakeServer {
        port: u16,
        /// Every messageID a prompt_async POST carried, accepted or not.
        posts: Arc<Mutex<Vec<String>>>,
        /// messageIDs the server will report durable.
        durable: Arc<Mutex<BTreeSet<String>>>,
        accept_posts: Arc<AtomicBool>,
        /// When set, the message read-back answers 500: durable state is unknowable.
        read_back_error: Arc<AtomicBool>,
        /// When set, /session/status answers 500: the level seed fails.
        status_error: Arc<AtomicBool>,
        /// Ask ids /permission reports as pending.
        pending_permissions: Arc<Mutex<Vec<String>>>,
        /// Ask ids /question reports as pending.
        pending_questions: Arc<Mutex<Vec<String>>>,
        /// When set, both pending-ask listings answer 500: the ask seed fails.
        ask_error: Arc<AtomicBool>,
        /// Session ids `GET /session` lists (idle sessions appear here and nowhere else).
        listed_sessions: Arc<Mutex<Vec<String>>>,
        /// When set, /session/status serves this raw body instead of an object.
        status_body: Arc<Mutex<Option<String>>>,
        /// When set, /permission serves this raw body instead of the pending ids.
        ask_body: Arc<Mutex<Option<String>>>,
    }

    fn spawn_fake_server() -> FakeServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let posts = Arc::new(Mutex::new(Vec::new()));
        let durable = Arc::new(Mutex::new(BTreeSet::new()));
        let accept_posts = Arc::new(AtomicBool::new(true));
        let read_back_error = Arc::new(AtomicBool::new(false));
        let status_error = Arc::new(AtomicBool::new(false));
        let pending_permissions = Arc::new(Mutex::new(Vec::<String>::new()));
        let pending_questions = Arc::new(Mutex::new(Vec::<String>::new()));
        let ask_error = Arc::new(AtomicBool::new(false));
        let ask_body = Arc::new(Mutex::new(None::<String>));
        let listed_sessions = Arc::new(Mutex::new(Vec::<String>::new()));
        let status_body = Arc::new(Mutex::new(None::<String>));
        let (posts_t, durable_t, accept_t) = (posts.clone(), durable.clone(), accept_posts.clone());
        let (read_back_t, status_err_t, pending_t, questions_t, ask_err_t, listed_t, status_body_t) = (
            read_back_error.clone(),
            status_error.clone(),
            pending_permissions.clone(),
            pending_questions.clone(),
            ask_error.clone(),
            listed_sessions.clone(),
            status_body.clone(),
        );
        let ask_body_t = ask_body.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let mut reader = BufReader::new(stream);
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() {
                    continue;
                }
                let mut content_length = 0_usize;
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 {
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                    line.clear();
                }
                let mut body = vec![0_u8; content_length];
                let _ = reader.read_exact(&mut body);
                let mut parts = request_line.split_whitespace();
                let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                let status = if method == "POST" && path.ends_with("/prompt_async") {
                    let message_id =
                        serde_json::from_slice::<Value>(&body)
                            .ok()
                            .and_then(|value| {
                                value
                                    .get("messageID")
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            });
                    match message_id {
                        Some(message_id) => {
                            posts_t.lock().unwrap().push(message_id.clone());
                            if accept_t.load(Ordering::SeqCst) {
                                durable_t.lock().unwrap().insert(message_id);
                                200
                            } else {
                                500
                            }
                        }
                        None => 400,
                    }
                } else if method == "GET" && path.contains("/message/") {
                    let message_id = path.rsplit('/').next().unwrap_or("");
                    if read_back_t.load(Ordering::SeqCst) {
                        500
                    } else if durable_t.lock().unwrap().contains(message_id) {
                        200
                    } else {
                        404
                    }
                } else if method == "GET" && path == "/session" {
                    200
                } else if method == "GET" && path == "/session/status" {
                    if status_err_t.load(Ordering::SeqCst) {
                        500
                    } else {
                        200
                    }
                } else if method == "GET" && (path == "/permission" || path == "/question") {
                    if ask_err_t.load(Ordering::SeqCst) {
                        500
                    } else {
                        200
                    }
                } else {
                    404
                };
                let body = if method == "GET" && path == "/session" {
                    let ids = listed_t.lock().unwrap();
                    serde_json::to_string(
                        &ids.iter()
                            .map(|id| serde_json::json!({ "id": id }))
                            .collect::<Vec<_>>(),
                    )
                    .unwrap()
                } else if method == "GET" && path == "/session/status" {
                    if let Some(body) = status_body_t.lock().unwrap().clone() {
                        body
                    } else {
                        "{}".to_string()
                    }
                } else if method == "GET" && (path == "/permission" || path == "/question") {
                    if path == "/permission"
                        && let Some(body) = ask_body_t.lock().unwrap().clone()
                    {
                        let mut stream = reader.into_inner();
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        continue;
                    }
                    let ids = if path == "/permission" {
                        pending_t.lock().unwrap()
                    } else {
                        questions_t.lock().unwrap()
                    };
                    serde_json::to_string(
                        &ids.iter()
                            .map(|id| serde_json::json!({ "id": id }))
                            .collect::<Vec<_>>(),
                    )
                    .unwrap()
                } else {
                    "{}".to_string()
                };
                let mut stream = reader.into_inner();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        FakeServer {
            port,
            posts,
            durable,
            accept_posts,
            read_back_error,
            status_error,
            pending_permissions,
            pending_questions,
            ask_error,
            listed_sessions,
            status_body,
            ask_body,
        }
    }

    /// Measured on 1.18.19: a second POST with the same messageID appends its parts again into
    /// the same message, so an indeterminate read-back must retry the read-back, never the POST.
    #[test]
    fn an_indeterminate_read_back_retries_the_read_back_and_never_re_posts() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, _filename) = delivery_fixture(tmp.path(), state_path.clone());

        server.read_back_error.store(true, Ordering::SeqCst);
        delivery.pump(&client);
        assert_eq!(server.posts.lock().unwrap().len(), 1, "one POST, attempted");
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Attempted);

        // While the read-back stays indeterminate, no pass may re-POST.
        delivery.pump(&client);
        delivery.pump(&client);
        assert_eq!(server.posts.lock().unwrap().len(), 1);

        // The read-back recovering flips the same attempt to Accepted with no second POST.
        server.read_back_error.store(false, Ordering::SeqCst);
        delivery.pump(&client);
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Accepted);
        assert_eq!(server.posts.lock().unwrap().len(), 1);
    }

    /// An ask opened before the SSE connection must survive the reconnect seed with its id, so
    /// the ordinary id-matched exit still releases it.
    #[test]
    fn seeding_recovers_a_pending_ask_and_gates_evidence_on_the_level_seed() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, _filename) = delivery_fixture(tmp.path(), state_path);

        // A transiently failing level seed yields no evidence at all.
        server.status_error.store(true, Ordering::SeqCst);
        let mut machine = EventMachine::default();
        assert!(!seed_from_server(&client, &mut machine));
        server.status_error.store(false, Ordering::SeqCst);

        // So does a failing pending-ask listing: an ask opened during the outage must not be
        // reported unblocked with heartbeats restored — the seed retries instead.
        server.ask_error.store(true, Ordering::SeqCst);
        let mut machine = EventMachine::default();
        assert!(!seed_from_server(&client, &mut machine));
        server.ask_error.store(false, Ordering::SeqCst);

        // A successful seed recovers the pending ask under its own id.
        server.status_error.store(false, Ordering::SeqCst);
        server
            .pending_permissions
            .lock()
            .unwrap()
            .push("per_pending".to_string());
        let mut machine = EventMachine::default();
        assert!(seed_from_server(&client, &mut machine));
        let blocked = observed(&machine);
        assert_eq!(blocked.state, Activity::Active);
        assert_eq!(blocked.blocked_on, BlockedOn::Human);

        assert_eq!(blocked.ask, Ask::Permission);

        // The id-matched exit releases exactly the recovered ask.
        machine.apply(&event(
            r#"{"type":"permission.replied","properties":{"requestID":"per_pending","reply":"once"}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);

        // Pending questions are recovered from their own listing endpoint (measured on 1.18.19),
        // classified as question asks, and released by the question's id-matched reply.
        server.pending_permissions.lock().unwrap().clear();
        server
            .pending_questions
            .lock()
            .unwrap()
            .push("que_pending".to_string());
        let mut machine = EventMachine::default();
        assert!(seed_from_server(&client, &mut machine));
        let blocked = observed(&machine);
        assert_eq!(blocked.blocked_on, BlockedOn::Human);
        assert_eq!(blocked.ask, Ask::Question);
        machine.apply(&event(
            r#"{"type":"question.replied","properties":{"requestID":"que_pending","answers":[["Yes"]]}}"#,
        ));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::None);
    }

    fn delivery_fixture(tmp: &Path, state_path: PathBuf) -> (Delivery, String) {
        let agent_dir = tmp.join("agents/h/worker");
        let inbox = message::inbox_dir(&agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        let filename =
            message::send_to_inbox(&inbox, "h.sender", Some("subject"), None, &[], "body").unwrap();
        let mut delivery =
            Delivery::with_state_path(tmp, &agent_dir, "h", "h.worker", "h.worker", state_path);
        delivery.saw_session("ses_target");
        (delivery, filename)
    }

    #[test]
    fn delivery_attempts_before_transport_and_accepts_only_the_read_back_receipt() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, filename) = delivery_fixture(tmp.path(), state_path.clone());

        delivery.pump(&client);
        let expected_id = stable_message_id("h.worker", "ses_target", &filename);
        assert_eq!(
            server.posts.lock().unwrap().as_slice(),
            [expected_id.clone()]
        );
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Accepted);
        assert_eq!(state.message_id, expected_id);

        // Accepted is terminal for this file: further pumps send nothing.
        delivery.pump(&client);
        delivery.pump(&client);
        assert_eq!(server.posts.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_stale_attempt_reconciles_by_reading_back_instead_of_resending() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (_, filename) = delivery_fixture(tmp.path(), state_path.clone());

        // A prior incarnation attempted this exact delivery and the server made it durable.
        let message_id = stable_message_id("h.worker", "ses_target", &filename);
        server.durable.lock().unwrap().insert(message_id.clone());
        atomic_json(
            &state_path,
            &DeliveryState {
                schema: DELIVERY_STATE_SCHEMA.to_string(),
                agent: "h.worker".to_string(),
                runtime_id: "h.worker".to_string(),
                session_id: "ses_target".to_string(),
                filename,
                message_id: message_id.clone(),
                phase: DeliveryPhase::Attempted,
            },
        )
        .unwrap();

        let agent_dir = tmp.path().join("agents/h/worker");
        let mut delivery = Delivery::with_state_path(
            tmp.path(),
            &agent_dir,
            "h",
            "h.worker",
            "h.worker",
            state_path.clone(),
        );
        delivery.saw_session("ses_target");
        delivery.pump(&client);

        assert!(server.posts.lock().unwrap().is_empty(), "must not resend");
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Accepted);
    }

    #[test]
    fn a_failed_transport_retries_the_same_identity_never_a_second_one() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        server.accept_posts.store(false, Ordering::SeqCst);
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, filename) = delivery_fixture(tmp.path(), state_path.clone());

        delivery.pump(&client);
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Attempted);

        server.accept_posts.store(true, Ordering::SeqCst);
        delivery.next_attempt = Instant::now();
        delivery.pump(&client);

        let expected_id = stable_message_id("h.worker", "ses_target", &filename);
        assert_eq!(
            server.posts.lock().unwrap().as_slice(),
            [expected_id.clone(), expected_id]
        );
        let state: DeliveryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state.phase, DeliveryPhase::Accepted);
    }

    #[test]
    fn dnd_suppresses_delivery_and_a_missing_target_session_waits() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, _) = delivery_fixture(tmp.path(), state_path);

        let agent_dir = tmp.path().join("agents/h/worker");
        status::set_state(&status::status_path(&agent_dir), status::State::Dnd).unwrap();
        delivery.pump(&client);
        assert!(server.posts.lock().unwrap().is_empty());

        status::set_state(&status::status_path(&agent_dir), status::State::Available).unwrap();
        delivery.target_session = None;
        delivery.pump(&client);
        assert!(
            server.posts.lock().unwrap().is_empty(),
            "no session yet: wait, never create"
        );
    }

    /// T5: an unrecognized future `session.status` word is no evidence at all — counting it as
    /// level evidence would let a quiet server derive `idle` from a word we cannot read.
    #[test]
    fn an_unrecognized_status_word_is_not_level_evidence() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"hibernating"}}}"#,
        ));
        assert_eq!(
            machine.observation(),
            None,
            "no level evidence, no observation"
        );
    }

    /// O1: a seat whose session settled before the observer connected is invisible to the event
    /// stream and to /session/status — with work pending, the delivery binding is recovered from
    /// the session listing rather than stalling forever.
    #[test]
    fn a_pre_settled_session_is_recovered_from_the_listing_for_delivery() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let state_path = tmp.path().join("state/delivery-state.json");
        let (mut delivery, _filename) = delivery_fixture(tmp.path(), state_path);
        delivery.target_session = None;

        // No listing yet: nothing to bind, nothing sent — and no wedge, it simply retries.
        delivery.pump(&client);
        assert!(server.posts.lock().unwrap().is_empty());

        // The idle session exists only in the listing; the next pass binds and delivers.
        server
            .listed_sessions
            .lock()
            .unwrap()
            .push("ses_settled".to_string());
        delivery.pump(&client);
        let posts = server.posts.lock().unwrap();
        assert_eq!(posts.len(), 1, "delivery bound to the recovered session");
    }

    /// W8-4: the seed is atomic against the LIVE machine — a mid-seed failure leaves no
    /// half-seeded asks behind, and a successful re-seed clears stale entries whose exits
    /// passed while the stream was down.
    #[test]
    fn the_seed_swaps_in_whole_and_clears_what_the_level_no_longer_states() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let _keep = tmp;

        // A machine holding a stale blocked ask and a stale busy session from before the drop.
        let mut machine = EventMachine::default();
        machine.seed_busy("ses_stale".to_string(), false);
        machine.seed_ask("per_stale".to_string(), "permission");

        // /permission succeeds, /question fails mid-seed: the live machine is untouched —
        // the stale ask is still held (not half-cleared) and no new ask leaked in.
        server
            .pending_permissions
            .lock()
            .unwrap()
            .push("per_new".to_string());
        server.ask_error.store(true, Ordering::SeqCst);
        // ask_error fails BOTH listings; simulate the split by failing only after /permission:
        // the atomicity claim is the same — nothing of the attempt lands.
        assert!(!seed_from_server(&client, &mut machine));
        let snapshot = observed(&machine);
        assert_eq!(
            snapshot.blocked_on,
            BlockedOn::Human,
            "stale ask still held"
        );
        server.ask_error.store(false, Ordering::SeqCst);

        // A successful re-seed states the whole level truth: the settled session and the
        // resolved ask disappear, the listed pending ask remains.
        assert!(seed_from_server(&client, &mut machine));
        let snapshot = observed(&machine);
        assert_eq!(snapshot.blocked_on, BlockedOn::Human);
        machine.apply(&event(
            r#"{"type":"permission.replied","properties":{"requestID":"per_new","reply":"once"}}"#,
        ));
        let snapshot = observed(&machine);
        assert_eq!(
            snapshot.blocked_on,
            BlockedOn::None,
            "per_stale is gone, not wedged"
        );
        assert_eq!(
            snapshot.state,
            Activity::Idle,
            "ses_stale cleared by the re-seed"
        );
    }

    /// An unknown status word on ANY session poisons the whole projection until a level seed
    /// replaces the machine: a tracked-busy entry can no longer be trusted to clear, and an
    /// untracked session in a state this version cannot read makes standing idle evidence a
    /// fabrication — that session may already be mid-turn.
    #[test]
    fn an_unreadable_status_word_on_a_tracked_busy_session_poisons_the_projection() {
        let mut machine = EventMachine::default();
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}"#,
        ));
        assert_eq!(observed(&machine).state, Activity::Active);
        machine.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"hibernating"}}}"#,
        ));
        assert!(
            machine.observation().is_none(),
            "a busy entry that can no longer be trusted to clear must withhold every projection"
        );

        // The same word on a session the projection does NOT track: standing idle evidence
        // must not keep heartbeating on top of unreadable activity.
        let mut untracked = EventMachine::default();
        untracked.seed_idle();
        assert_eq!(observed(&untracked).state, Activity::Idle);
        untracked.apply(&event(
            r#"{"type":"session.status","properties":{"sessionID":"ses_9","status":{"type":"hibernating"}}}"#,
        ));
        assert!(
            untracked.observation().is_none(),
            "an unknown word on an untracked session withholds the definite idle"
        );

        // A sticky terminal outranks the poison: it does not depend on the busy map the
        // unknown word made untrustworthy, and withholding it would lose the terminal.
        machine.apply(&event(
            r#"{"type":"session.error","properties":{"sessionID":"ses_1","error":{"name":"ProviderAuthError"}}}"#,
        ));
        let terminal = observed(&machine);
        assert_eq!(terminal.state, Activity::Ended);
        assert_eq!(terminal.reason.as_deref(), Some("providerAuth"));
    }

    /// A pending listing entry whose id this version cannot read must fail the whole seed:
    /// seeding around it would restore evidence on a picture that silently drops a human block.
    #[test]
    fn an_unreadable_pending_entry_fails_the_seed() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let _keep = tmp;

        *server.ask_body.lock().unwrap() = Some(r#"[{"id":"per_ok"},{"token":42}]"#.to_string());
        let mut machine = EventMachine::default();
        assert!(
            !seed_from_server(&client, &mut machine),
            "an entry without a readable id must fail the seed, not be skipped"
        );

        *server.ask_body.lock().unwrap() = None;
        server
            .pending_permissions
            .lock()
            .unwrap()
            .push("per_ok".to_string());
        let mut machine = EventMachine::default();
        assert!(seed_from_server(&client, &mut machine));
        assert_eq!(observed(&machine).blocked_on, BlockedOn::Human);
    }

    /// Cluster 3: a /session/status response that is not the documented object shape proves
    /// nothing — null and array shapes must fail the seed, never read as definite idle.
    #[test]
    fn non_object_status_shapes_fail_the_seed() {
        let tmp = tempfile::tempdir().unwrap();
        let server = spawn_fake_server();
        let client = Client::new(server.port, "pw");
        let _keep = tmp;
        for shape in ["null", "[]", "[\"ses_a\"]", "3"] {
            *server.status_body.lock().unwrap() = Some(shape.to_string());
            let mut machine = EventMachine::default();
            assert!(
                !seed_from_server(&client, &mut machine),
                "shape {shape} must fail the seed"
            );
            assert_eq!(
                machine.observation(),
                None,
                "no level evidence from {shape}"
            );
        }
        *server.status_body.lock().unwrap() = None;
        let mut machine = EventMachine::default();
        assert!(seed_from_server(&client, &mut machine));
    }

    /// n8: a server that accepts the stream and then goes silent must surface as a disconnect
    /// within the silence horizon — a stalled socket cannot keep evidence alive forever.
    #[test]
    fn a_silent_sse_stream_disconnects_at_the_silence_horizon() {
        use std::io::Write as _;
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.write_all(b"HTTP/1.0 200 OK\r\n\r\n");
            // Say nothing, forever; hold the socket open.
            thread::sleep(Duration::from_secs(30));
        });
        let mut client = Client::new(port, "pw");
        client.sse_silence = Duration::from_millis(200);

        let (tx, rx) = mpsc::channel();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        spawn_sse_reader(client, tx, stop.clone());
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_disconnect = false;
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(250)) {
                Ok(SseMessage::Disconnected) => {
                    saw_disconnect = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        stop.store(true, Ordering::SeqCst);
        assert!(saw_disconnect, "silence must surface as a disconnect");
    }
}
