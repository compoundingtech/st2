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
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::{context, message};

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

pub fn run(catalog_root: &Path, identity: &str) -> Result<()> {
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("pi channel agent '{identity}' is not declared"))?;
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
    let mut delivered = HashSet::new();
    loop {
        match input_rx.recv_timeout(POLL) {
            Ok(line) => {
                let line = line.context("reading pi channel input")?;
                if line.trim().is_empty() {
                    continue;
                }
                // Frames from the extension are observational today. An unknown *or malformed*
                // frame is dropped rather than fatal: a newer asset, or one line of stray output,
                // must not be able to take the channel down and stall an inbox.
                let _ = serde_json::from_str::<Value>(&line);
            }
            Err(RecvTimeoutError::Timeout) => {}
            // pi's extension owns this child over stdio. EOF is the session-lifetime boundary, so
            // do not leave a detached watcher behind.
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
        for msg in message::list_inbox(&inbox)? {
            if delivered.insert(msg.filename.clone()) {
                write_json(&mut stdout, &message_frame(msg, identity))?;
            }
        }
        stdout.flush()?;
        thread::sleep(POLL);
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
        context::write_now(&context::context_dir(agent_dir), "Mid-migration on shard 3.").unwrap();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        std::fs::write(
            inbox.join("1787042542238-xex2t4.md"),
            "---\nfrom: h.supervisor\nsubject: deploy check\n---\nVerify staging.\n",
        )
        .unwrap();

        let restored = session_context(agent_dir, "h.worker");

        let state = restored.find("Mid-migration on shard 3.").expect("state restored");
        let ritual = restored.find("Run the st2 boot ritual").expect("ritual present");
        let unread = restored.find("## st2 inbox (1 unread)").expect("unread listed");
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

        assert!(restored.starts_with("Run the st2 boot ritual"), "{restored}");
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
