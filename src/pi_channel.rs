//! Minimal pi channel watcher.
//!
//! The inbox is the durable source of truth. This process keeps only an ephemeral set of filenames
//! delivered during its current lifetime; a restart scans the inbox again. It is spawned by the
//! shipped pi extension and owned by it over stdio, so EOF on stdin is the session-lifetime
//! boundary. The outer pi session wrapper owns presence, because pi can outlive a failed extension.
//!
//! The wire is newline-delimited JSON in both directions. st2 — not the extension — decides how a
//! delivered message is handed to the agent, so the delivery mode travels on the frame: changing
//! that policy is a Rust change, not a redeploy of a TypeScript asset.

use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::{context, harness_state, message};

const POLL: Duration = Duration::from_millis(250);

/// How old durable working state may be and still be restored, matching the lifecycle hooks'
/// `ST_REHYDRATE_STALE_S` default. Stale state is worse than none: it describes a world the agent
/// has already left.
const CONTEXT_MAX_AGE: Duration = Duration::from_secs(86_400);

/// The boot instruction every maintained harness restores verbatim. It is the shipped bus contract:
/// declare presence, then drain the inbox.
const RITUAL: &str = "Run the st2 boot ritual now: set your status to available, then drain your \
inbox by reading, acting on, replying when useful, and archiving each handled message. Before \
resuming or starting work, set your status to busy; set available only when yielding or ready for \
new work.";

/// The wire version the shipped extension is written against. A mismatch is the extension's to
/// refuse: st2 never guesses what an older asset understands.
pub const PROTOCOL: u32 = 1;

/// How pi is asked to hand one delivered message to the agent.
///
/// `steer` is the only value st2 currently emits. It is the earliest point at which pi accepts
/// input without discarding the running turn, and the same choice the Codex native path makes when
/// it routes an active turn to a steer. Holding the message until the agent settles instead would
/// defer delivery inside pi, where st2 cannot see it, which is what the "`busy` delivers
/// immediately" rule exists to prevent. Per-message selection is tracked in #277.
///
/// The boundary is finer than "after the current turn's tool calls" suggests. Measured against a
/// live provider: each tool call and its result form their own assistant message, so a steer sent
/// during a four-step job landed on the *first* tool-result boundary — in the same millisecond as
/// that result, after waiting out only the remainder of the one in-flight call. Steer latency is
/// therefore bounded by a single tool call's duration, not by the length of the job. What is not
/// guaranteed is that displaced work resumes: the model chose to continue, once, on one model.
const DELIVER_AS: &str = "steer";

fn channel_content(subject: Option<&str>, body: &str) -> String {
    match subject.filter(|value| !value.is_empty()) {
        Some(subject) => format!("Subject: {subject}\n\n{body}"),
        None => body.to_owned(),
    }
}

/// The harness-specific facts the shared channel loop needs: which env names carry the wrapper's
/// exported ownership triple, and what label goes on records and errors.
pub struct ChannelKind {
    pub label: &'static str,
    pub runtime_id_env: &'static str,
    pub session_env: &'static str,
    pub seq_env: &'static str,
}

const PI_KIND: ChannelKind = ChannelKind {
    label: "pi",
    runtime_id_env: crate::pi_session::CHANNEL_RUNTIME_ID,
    session_env: crate::pi_session::CHANNEL_SESSION,
    seq_env: crate::pi_session::CHANNEL_SEQ,
};

const OMP_KIND: ChannelKind = ChannelKind {
    label: "omp",
    runtime_id_env: crate::omp_session::CHANNEL_RUNTIME_ID,
    session_env: crate::omp_session::CHANNEL_SESSION,
    seq_env: crate::omp_session::CHANNEL_SEQ,
};

/// Run the pi native message channel over stdio.
pub fn run(catalog_root: &Path, identity: &str) -> Result<()> {
    run_for(catalog_root, identity, &PI_KIND)
}

/// Run the omp native message channel over stdio (the omp extension's child).
pub fn run_omp(catalog_root: &Path, identity: &str) -> Result<()> {
    run_for(catalog_root, identity, &OMP_KIND)
}

fn run_for(catalog_root: &Path, identity: &str, kind: &ChannelKind) -> Result<()> {
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("{} channel agent '{identity}' is not declared", kind.label))?;
    let inbox = message::inbox_dir(&agent_dir);
    // Composed here rather than in the extension: what a restarted agent is told is st2's contract,
    // not the asset's, and the Codex and Claude hooks compose the same three blocks in bash.
    let session_context = session_context(&agent_dir, identity);
    let (input_tx, input_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            if input_tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    write_json(
        &mut stdout,
        &json!({
            "type": "hello",
            "protocol": PROTOCOL,
            "identity": identity,
            "sessionContext": session_context,
        }),
    )?;
    stdout.flush()?;
    // The channel owns the live half of observed harness state: it is the one process that sees
    // the harness's own turn events, and its stdio connection to the extension is the evidence
    // that those events are still being watched. The terminal half belongs to the outer session
    // wrapper, which alone sees the provider die.
    // The pty session vouching for the record is the wrapper's task: its runtime ID arrives in
    // the channel environment, and only aliases the identity on driver-expanded seats.
    let pty_session = std::env::var(kind.runtime_id_env)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| identity.to_string());
    // The wrapper mints the session token; adopting it makes the wrapper's terminal record own
    // this channel's live records (so a queued frame after `ended` is suppressed) while a
    // predecessor incarnation's records are foreign: the first frame opens a fresh transition
    // and a predecessor's terminal record never silences this session.
    let mut writer =
        harness_state::Writer::new(&agent_dir, identity, kind.label, Some(pty_session));
    if let Ok(session) = std::env::var(kind.session_env)
        && !session.is_empty()
    {
        // Full adopted ownership when the wrapper exported it: the claimed sequence gives the
        // token a direction, so a straggler channel from a superseded session is refused.
        writer = match std::env::var(kind.seq_env)
            .ok()
            .and_then(|seq| seq.parse::<u64>().ok())
        {
            Some(seq) => writer.with_ownership(session, seq),
            None => writer.with_session(session),
        };
    }
    writer.interrupt();
    channel_loop(
        &input_rx,
        &mut stdout,
        &inbox,
        &mut writer,
        identity,
        kind.label,
        POLL,
        harness_state::HARNESS_STATE_REFRESH,
    )
}

/// The channel's steady state: forward inbox entries out, fold extension frames in, and keep the
/// observed-state heartbeat exactly as fresh as the stdio connection that justifies it. EOF is the
/// session-lifetime boundary — the loop then returns without writing anything, so the record ages
/// to `unknown` rather than asserting a state nobody is watching.
fn channel_loop(
    input: &Receiver<io::Result<String>>,
    out: &mut impl Write,
    inbox: &Path,
    writer: &mut harness_state::Writer,
    identity: &str,
    label: &str,
    poll: Duration,
    heartbeat_every: Duration,
) -> Result<()> {
    let mut delivered = HashSet::new();
    let mut next_heartbeat = Instant::now() + heartbeat_every;
    loop {
        match input.recv_timeout(poll) {
            Ok(line) => {
                let line = line.with_context(|| format!("reading {label} channel input"))?;
                if line.trim().is_empty() {
                    continue;
                }
                // Frames from the extension are otherwise observational. An unknown *or malformed*
                // frame is dropped rather than fatal: a newer asset, or one line of stray output,
                // must not be able to take the channel down and stall an inbox. A failed record
                // write degrades the same way — delivery never depends on observability.
                if let Some(observation) = serde_json::from_str::<Value>(&line)
                    .ok()
                    .as_ref()
                    .and_then(state_observation)
                    // A queued live frame must never overwrite the wrapper's terminal record:
                    // the channel and the wrapper are separate processes, so the flock alone
                    // serializes but does not order their writes.
                    && let Err(error) = writer.observe_unless_ended(observation)
                {
                    tracing::warn!("st2 {label} channel: recording observed state failed: {error}");
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            // pi's extension owns this child over stdio. EOF is the session-lifetime boundary, so
            // do not leave a detached watcher behind.
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
        let now = Instant::now();
        if now >= next_heartbeat {
            if let Err(error) = writer.heartbeat() {
                tracing::warn!("st2 {label} channel: refreshing observed state failed: {error}");
            }
            next_heartbeat = now + heartbeat_every;
        }
        for msg in message::list_inbox(inbox)? {
            if delivered.insert(msg.filename.clone()) {
                write_json(out, &message_frame(msg, identity))?;
            }
        }
        out.flush()?;
        thread::sleep(poll);
    }
}

/// The observed-state frame the shipped extension emits on the harness's own turn boundaries.
/// Only positively recognized words become observations: an unrecognized state word is dropped
/// like any other unknown frame, so a newer asset cannot make this channel record something it
/// cannot vouch for. pi offers no waiting-on-a-human signal, so pi frames never carry
/// `blockedOn`; the omp extension does (`tool_approval_requested`/`_resolved`), and its optional
/// axes are parsed here for both channels — a frame without them decodes exactly as before.
fn state_observation(frame: &Value) -> Option<harness_state::Observation> {
    if frame.get("type").and_then(Value::as_str) != Some("state") {
        return None;
    }
    let state = match frame.get("state").and_then(Value::as_str)? {
        "active" => harness_state::Activity::Active,
        "idle" => harness_state::Activity::Idle,
        _ => return None,
    };
    let blocked_on = match frame.get("blockedOn").and_then(Value::as_str) {
        Some("human") => harness_state::BlockedOn::Human,
        _ => harness_state::BlockedOn::None,
    };
    let mut observation =
        harness_state::Observation::new(state, blocked_on, harness_state::InputBuffer::Unknown);
    if blocked_on == harness_state::BlockedOn::Human
        && let Some(ask) = frame.get("ask").and_then(Value::as_str)
    {
        observation = observation.with_ask(parse_ask(ask));
    }
    if let Some(reason) = frame.get("reason").and_then(Value::as_str) {
        observation = observation.with_reason(reason);
    }
    Some(observation)
}

/// The machine-readable ask word on a blocked frame. An unrecognized word decodes as unknown —
/// indeterminate, never silently reclassified — matching the record's own decode rule.
fn parse_ask(word: &str) -> harness_state::Ask {
    match word {
        "permission" => harness_state::Ask::Permission,
        "question" => harness_state::Ask::Question,
        "review" => harness_state::Ask::Review,
        _ => harness_state::Ask::Unknown,
    }
}

/// What a starting or restarting pi session is told about its own durable state.
///
/// pi has no session-start hook, so this is the payload that stands in for
/// `$ST_HOOKS/codex-session-start.sh`. The three blocks and their order are deliberately identical
/// to that script's, so a persona written against one harness reads the same on pi. Empty when
/// there is nothing to restore and no unread work, so a fresh agent is told nothing but its ritual.
fn session_context(agent_dir: &Path, identity: &str) -> String {
    let mut blocks = Vec::new();
    let state = context::read_now_fresh(&context::context_dir(agent_dir), CONTEXT_MAX_AGE);
    if !state.trim().is_empty() {
        blocks.push(format!(
            "<context source=\"st2/context/now.md\" agent=\"{identity}\">\n{}\n</context>",
            state.trim_end()
        ));
    }
    blocks.push(RITUAL.to_string());
    let unread = message::list_inbox(&message::inbox_dir(agent_dir)).unwrap_or_default();
    if !unread.is_empty() {
        let mut lines = vec![format!("## st2 inbox ({} unread)", unread.len())];
        lines.extend(unread.iter().map(|msg| {
            let from = msg.from.as_deref().unwrap_or("unknown");
            match msg.subject.as_deref() {
                Some(subject) => format!("- {}  {from}  Subject: {subject}", msg.filename),
                None => format!("- {}  {from}", msg.filename),
            }
        }));
        blocks.push(lines.join("\n"));
    }
    blocks.join("\n\n")
}

/// One inbox entry as the frame the extension hands to pi.
///
/// The envelope matches the Claude channel's exactly, so a persona written against one native
/// harness reads the same on the other.
fn message_frame(msg: message::Message, identity: &str) -> Value {
    let content = channel_content(msg.subject.as_deref(), &msg.body);
    json!({"type":"message","deliverAs":DELIVER_AS,"content":content,"meta":{
        "from": msg.from,
        "messageFilename": msg.filename,
        "threadFilename": msg.in_reply_to.unwrap_or_else(|| msg.filename.clone()),
        "identity": identity
    }})
}

fn write_json(out: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the two words pi's own turn boundaries can vouch for become observations. Everything
    /// else — other frame types, unknown state words, missing fields — is dropped, so a newer
    /// extension asset cannot push this channel into recording something it cannot prove.
    #[test]
    fn only_recognized_state_frames_become_observations() {
        let active = state_observation(&json!({"type":"state","state":"active"})).unwrap();
        assert_eq!(active.state, harness_state::Activity::Active);
        assert_eq!(active.blocked_on, harness_state::BlockedOn::None);
        assert_eq!(active.input_buffer, harness_state::InputBuffer::Unknown);
        assert_eq!(
            state_observation(&json!({"type":"state","state":"idle"}))
                .unwrap()
                .state,
            harness_state::Activity::Idle
        );

        for frame in [
            json!({"type":"state","state":"child"}),
            json!({"type":"state","state":"unknown"}),
            json!({"type":"state"}),
            json!({"type":"delivered","state":"active"}),
            json!({"state":"active"}),
        ] {
            assert_eq!(state_observation(&frame), None, "frame: {frame}");
        }
    }

    /// The omp extension's approval frames carry the blocked-on-human axis pi never emits. The
    /// optional axes must decode for either channel's frames, an unrecognized ask word decodes
    /// unknown (never silently reclassified), and a blocked frame without the extras decodes as
    /// before.
    #[test]
    fn blocked_frames_carry_the_human_axes() {
        let blocked = state_observation(&json!({
            "type":"state","state":"active","blockedOn":"human",
            "ask":"permission","reason":"bash"
        }))
        .unwrap();
        assert_eq!(blocked.blocked_on, harness_state::BlockedOn::Human);
        assert_eq!(blocked.ask, harness_state::Ask::Permission);
        assert_eq!(blocked.reason.as_deref(), Some("bash"));

        let unknown_ask = state_observation(&json!({
            "type":"state","state":"active","blockedOn":"human",
            "ask":"sacrifice"
        }))
        .unwrap();
        assert_eq!(unknown_ask.blocked_on, harness_state::BlockedOn::Human);
        assert_eq!(unknown_ask.ask, harness_state::Ask::Unknown);

        let plain = state_observation(&json!({"type":"state","state":"idle"})).unwrap();
        assert_eq!(plain.blocked_on, harness_state::BlockedOn::None);
        assert_eq!(plain.ask, harness_state::Ask::None);
    }

    /// The stdio connection is the evidence. While it lives, the record's heartbeat advances
    /// without a new observation; when it ends, the loop returns having written nothing more, so
    /// the last state is left to age to `unknown` instead of being asserted or terminated — the
    /// terminal record belongs to the session wrapper, which sees the provider die.
    #[test]
    fn heartbeats_while_connected_then_leaves_the_record_to_age_on_eof() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let record = harness_state::harness_state_path(agent_dir);
        let mut writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()));
        let (tx, rx) = mpsc::channel();

        tx.send(Ok(r#"{"type":"state","state":"active"}"#.to_string()))
            .unwrap();
        let disconnect = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            drop(tx);
        });
        let mut out = Vec::new();
        channel_loop(
            &rx,
            &mut out,
            &message::inbox_dir(agent_dir),
            &mut writer,
            "h.worker",
            "pi",
            Duration::from_millis(2),
            Duration::from_millis(5),
        )
        .unwrap();

        disconnect.join().unwrap();

        let raw: Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
        assert_eq!(raw["state"], "active", "EOF must not rewrite the state");
        assert!(
            raw["writtenAtMs"].as_u64().unwrap() > raw["sinceMs"].as_u64().unwrap(),
            "a heartbeat re-stamped the record while the connection lived: {raw}"
        );
        let after_eof = std::fs::read(&record).unwrap();
        thread::sleep(Duration::from_millis(15));
        assert_eq!(
            std::fs::read(&record).unwrap(),
            after_eof,
            "nothing may write after the connection is gone"
        );
    }

    /// The wrapper's terminal record is the incarnation's last word: a live frame the extension
    /// queued before dying must not resurrect the session after the wrapper reaped it.
    #[test]
    fn a_queued_live_frame_never_overwrites_the_wrappers_terminal_record() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let record = harness_state::harness_state_path(agent_dir);
        // The wrapper mints the session token and the channel adopts it — that sharing is what
        // makes the wrapper's terminal record this session's last word.
        let session = harness_state::session_token();
        let mut channel_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()))
                .with_session(session.clone());
        let mut wrapper_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()))
                .with_session(session);
        wrapper_writer.ended("signal 9").unwrap();
        let terminal = std::fs::read(&record).unwrap();

        let (tx, rx) = mpsc::channel();
        tx.send(Ok(r#"{"type":"state","state":"idle"}"#.to_string()))
            .unwrap();
        drop(tx);
        let mut out = Vec::new();
        channel_loop(
            &rx,
            &mut out,
            &message::inbox_dir(agent_dir),
            &mut channel_writer,
            "h.worker",
            "pi",
            Duration::from_millis(2),
            Duration::from_millis(5),
        )
        .unwrap();

        assert_eq!(std::fs::read(&record).unwrap(), terminal);
    }

    #[test]
    fn channel_content_reuses_the_claude_channel_envelope() {
        assert_eq!(
            channel_content(Some("subject"), "body"),
            "Subject: subject\n\nbody"
        );
        assert_eq!(channel_content(None, "body"), "body");
        assert_eq!(channel_content(Some(""), "body"), "body");
    }

    /// A restarting pi agent has to be told the same three things the Codex and Claude session-start
    /// hooks tell theirs, in the same order — otherwise "restart" means something different per
    /// harness.
    #[test]
    fn session_context_restores_state_ritual_and_unread_work_in_hook_order() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        context::write_now(
            &context::context_dir(agent_dir),
            "Mid-migration on shard 3.",
        )
        .unwrap();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        std::fs::write(
            inbox.join("1787042542238-xex2t4.md"),
            "---\nfrom: h.supervisor\nsubject: deploy check\n---\nVerify staging.\n",
        )
        .unwrap();

        let restored = session_context(agent_dir, "h.worker");

        let state = restored
            .find("Mid-migration on shard 3.")
            .expect("state restored");
        let ritual = restored
            .find("Run the st2 boot ritual")
            .expect("ritual present");
        let unread = restored
            .find("## st2 inbox (1 unread)")
            .expect("unread listed");
        assert!(state < ritual && ritual < unread, "{restored}");
        assert!(
            restored.contains("<context source=\"st2/context/now.md\" agent=\"h.worker\">"),
            "{restored}"
        );
        assert!(restored.contains("Subject: deploy check"), "{restored}");
    }

    /// A fresh agent has no durable state and no mail. It must still get its ritual, and must not be
    /// handed an empty `<context>` envelope describing nothing.
    #[test]
    fn a_fresh_agent_is_told_only_its_ritual() {
        let tmp = tempfile::tempdir().unwrap();

        let restored = session_context(tmp.path(), "h.worker");

        assert!(
            restored.starts_with("Run the st2 boot ritual"),
            "{restored}"
        );
        assert!(!restored.contains("<context"), "{restored}");
        assert!(!restored.contains("st2 inbox"), "{restored}");
    }

    /// The extension is not allowed to choose how a message lands, so the mode has to be on the
    /// frame st2 writes. Pinning it here is what makes #277 a one-place change.
    #[test]
    fn st2_owns_the_delivery_mode_on_the_wire() {
        let frame = message_frame(
            message::Message {
                filename: "1787042542238-xex2t4.md".into(),
                ts_ms: 1_787_042_542_238,
                from: Some("h.supervisor".into()),
                subject: Some("deploy check".into()),
                in_reply_to: None,
                tags: Vec::new(),
                priority: None,
                idempotency_key: None,
                stream: None,
                event_id: None,
                event_key: None,
                body: "Please verify the staging deploy.".into(),
            },
            "h.worker",
        );

        assert_eq!(frame["type"], "message");
        assert_eq!(frame["deliverAs"], "steer");
        assert_eq!(
            frame["content"],
            "Subject: deploy check\n\nPlease verify the staging deploy."
        );
        assert_eq!(frame["meta"]["identity"], "h.worker");
        // An unthreaded message threads on itself, matching the Claude channel.
        assert_eq!(
            frame["meta"]["threadFilename"],
            frame["meta"]["messageFilename"]
        );
    }
}
