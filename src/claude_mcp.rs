//! Claude's MCP-native delivery channel.
//!
//! The inbox is the durable work source. The declaration-owned delivery ledger is the durable
//! transport authority; Claude's notification write and flush provide no delivery evidence. The
//! outer Claude session wrapper owns presence because Claude can close this child before the
//! session ends.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::native_channel::{DeliveryPump, channel_content, delivery_correlation, write_json};
use crate::{delivery_ledger, message};

const POLL: Duration = Duration::from_millis(250);

fn delivery_ledger_path_in(agent_dir: &Path) -> PathBuf {
    agent_dir.join(delivery_ledger::LEDGER_FILE)
}

fn delivery_correlate() -> impl Fn(&str, &str) -> String + Send + Sync + 'static {
    delivery_correlation
}

fn message_frame(
    msg: message::Message,
    identity: &str,
    permit: &delivery_ledger::Permit,
) -> Result<Value> {
    let content = format!(
        "{}\n{}",
        delivery_ledger::marker(&msg.filename, permit.token())?,
        channel_content(msg.subject.as_deref(), &msg.body)
    );
    let thread_filename = msg
        .in_reply_to
        .clone()
        .unwrap_or_else(|| msg.filename.clone());
    Ok(
        json!({"jsonrpc":"2.0","method":"notifications/claude/channel","params":{
            "content":content,
            "meta":{"from":msg.from,"messageFilename":msg.filename,"threadFilename":thread_filename,"identity":identity}
        }}),
    )
}

/// Read this Claude seat's delivery state without recovering or writing it.
pub fn observe_delivery(
    catalog_root: &Path,
    identity: &str,
) -> Result<delivery_ledger::Observation> {
    let agent_dir =
        message::resolve_declared_dir(catalog_root, identity, &crate::run::detect_host())?
            .with_context(|| format!("delivery ledger agent '{identity}' is not declared"))?;
    delivery_ledger::observe(
        &delivery_ledger_path_in(&agent_dir),
        delivery_ledger::Harness::Claude.profile(),
        identity,
        &delivery_correlate(),
    )
}

/// Record exact operator absence without invoking the Claude transport.
pub fn operator_refuse(
    catalog_root: &Path,
    identity: &str,
    refusal: &delivery_ledger::OperatorRefusal,
) -> Result<delivery_ledger::OperatorOutcome> {
    let agent_dir =
        message::resolve_declared_dir(catalog_root, identity, &crate::run::detect_host())?
            .with_context(|| format!("delivery ledger agent '{identity}' is not declared"))?;
    if message::archive_receipt_exists(&agent_dir, &refusal.fence.filename)? {
        return Ok(delivery_ledger::OperatorOutcome::ArchiveSettled {
            filename: refusal.fence.filename.clone(),
        });
    }
    delivery_ledger::Ledger::for_operator(
        &delivery_ledger_path_in(&agent_dir),
        delivery_ledger::Harness::Claude.profile(),
        identity,
        delivery_correlate(),
    )
    .operator_refuse(refusal)
}

pub fn run(catalog_root: &Path, identity: &str) -> Result<()> {
    let agent_dir =
        message::resolve_declared_dir(catalog_root, identity, &crate::run::detect_host())?
            .with_context(|| format!("Claude MCP agent '{identity}' is not declared"))?;
    let inbox = message::inbox_dir(&agent_dir);
    let (input_tx, input_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            if input_tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut delivery = DeliveryPump::open(
        &agent_dir,
        identity,
        identity,
        delivery_ledger::Harness::Claude,
        message_frame,
    );
    let mut initialized = false;
    loop {
        match input_rx.recv_timeout(POLL) {
            Ok(line) => {
                let line = line.context("reading Claude MCP input")?;
                if line.trim().is_empty() {
                    continue;
                }
                let request: Value =
                    serde_json::from_str(&line).context("decoding Claude MCP JSON")?;
                match request.get("method").and_then(Value::as_str) {
                    Some("initialize") => {
                        let id = request.get("id").cloned().unwrap_or(Value::Null);
                        write_json(
                            &mut stdout,
                            &json!({"jsonrpc":"2.0","id":id,"result":{
                                "protocolVersion": request.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),
                                "capabilities":{"tools":{},"experimental":{"claude/channel":{}}},
                                "serverInfo":{"name":"st2","version":env!("CARGO_PKG_VERSION")}
                            }}),
                        )?;
                    }
                    Some("notifications/initialized") => {
                        initialized = true;
                    }
                    Some("tools/list") | Some("resources/list") | Some("prompts/list") => {
                        if let Some(id) = request.get("id") {
                            let field = if request["method"] == "tools/list" {
                                "tools"
                            } else if request["method"] == "resources/list" {
                                "resources"
                            } else {
                                "prompts"
                            };
                            write_json(
                                &mut stdout,
                                &json!({"jsonrpc":"2.0","id":id,"result":{field:[]}}),
                            )?;
                        }
                    }
                    Some("ping") => {
                        if let Some(id) = request.get("id") {
                            write_json(&mut stdout, &json!({"jsonrpc":"2.0","id":id,"result":{}}))?;
                        }
                    }
                    _ => {}
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            // Claude owns this child over stdio. EOF is the session-lifetime
            // boundary, so do not leave a detached watcher behind.
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
        if initialized {
            delivery.pump(&mut stdout, message::list_inbox(&inbox)?, identity)?;
        }

        stdout.flush()?;
        thread::sleep(POLL);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const FIRST: &str = "1787042542238-xex2t4.md";
    const SECOND: &str = "1787042542239-abc123.md";

    fn test_message(filename: &str) -> message::Message {
        message::Message {
            filename: filename.into(),
            ts_ms: filename[..13].parse().unwrap(),
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
        }
    }

    fn output_frames(out: &[u8]) -> Vec<Value> {
        String::from_utf8_lossy(out)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn claude_attempt_only_delivery_is_transactional_and_restart_safe() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        let unread = vec![test_message(FIRST), test_message(SECOND)];

        let mut first_process = DeliveryPump::open(
            agent_dir,
            "h.worker",
            "h.worker",
            delivery_ledger::Harness::Claude,
            message_frame,
        );
        let mut first_out = Vec::new();
        first_process
            .pump(&mut first_out, unread.clone(), "h.worker")
            .unwrap();
        let first_snapshot = first_process.ledger.snapshot().unwrap();
        let first_attempt = first_snapshot.attempt(FIRST).unwrap().clone();
        assert_eq!(first_snapshot.attempts().len(), 1);
        assert_eq!(first_attempt.phase, delivery_ledger::Phase::Attempted);
        assert_eq!(first_attempt.binding, "h.worker");
        let frames = output_frames(&first_out);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["method"], "notifications/claude/channel");
        assert_eq!(
            frames[0]["params"]["content"],
            format!(
                "{}\nSubject: deploy check\n\nPlease verify the staging deploy.",
                delivery_ledger::marker(FIRST, first_attempt.token).unwrap()
            )
        );

        let mut restarted = DeliveryPump::open(
            agent_dir,
            "h.worker",
            "h.worker",
            delivery_ledger::Harness::Claude,
            message_frame,
        );
        let mut restart_out = Vec::new();
        restarted
            .pump(&mut restart_out, unread.clone(), "h.worker")
            .unwrap();
        assert!(restart_out.is_empty());

        let mut missing_out = Vec::new();
        restarted
            .pump(&mut missing_out, vec![test_message(SECOND)], "h.worker")
            .unwrap();
        let missing_snapshot = restarted.ledger.snapshot().unwrap();
        assert!(missing_out.is_empty());
        assert!(missing_snapshot.attempt(FIRST).is_some());
        assert!(missing_snapshot.attempt(SECOND).is_none());

        let mut restored_out = Vec::new();
        restarted
            .pump(&mut restored_out, unread.clone(), "h.worker")
            .unwrap();
        assert!(restored_out.is_empty());

        let archive = message::archive_dir(agent_dir);
        std::fs::create_dir_all(&archive).unwrap();
        std::fs::write(archive.join(FIRST), b"durable recipient receipt").unwrap();
        let stale_unread = unread.clone();
        let mut after_archive = Vec::new();
        restarted
            .pump(&mut after_archive, stale_unread, "h.worker")
            .unwrap();
        let second_snapshot = restarted.ledger.snapshot().unwrap();
        assert!(second_snapshot.attempt(FIRST).is_none());
        let second_attempt = second_snapshot.attempt(SECOND).unwrap().clone();
        assert_eq!(output_frames(&after_archive).len(), 1);

        let audit = delivery_ledger::OperatorAudit::new(
            delivery_ledger::OperatorSource::Operator,
            unsafe { libc::geteuid() },
            None,
            None,
            "marker absent from provider history",
            second_snapshot.digest(),
            1,
        )
        .unwrap();
        assert!(matches!(
            restarted
                .ledger
                .operator_refuse(&delivery_ledger::OperatorRefusal {
                    fence: second_attempt.fence(),
                    audit,
                })
                .unwrap(),
            delivery_ledger::OperatorOutcome::Applied(_)
        ));

        let mut retry_process = DeliveryPump::open(
            agent_dir,
            "h.worker",
            "h.worker",
            delivery_ledger::Harness::Claude,
            message_frame,
        );
        let mut retry_out = Vec::new();
        retry_process
            .pump(&mut retry_out, vec![test_message(SECOND)], "h.worker")
            .unwrap();
        let replacement = retry_process
            .ledger
            .snapshot()
            .unwrap()
            .attempt(SECOND)
            .unwrap()
            .clone();
        assert_ne!(replacement.token, second_attempt.token);
        assert_eq!(replacement.binding, "h.worker");
        assert_eq!(output_frames(&retry_out).len(), 1);
        retry_process
            .pump(&mut retry_out, vec![test_message(SECOND)], "h.worker")
            .unwrap();
        assert_eq!(output_frames(&retry_out).len(), 1);
    }
}
