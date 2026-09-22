//! Provider-native Claude delivery over the documented stream-JSON conversation boundary.
//!
//! The interactive MCP channel only proved that bytes reached a plugin child. It could report
//! delivery while Claude remained idle forever. This driver keeps one print-mode process open,
//! writes user turns to its stream-JSON stdin, and advances the shared delivery ledger only when
//! `--replay-user-messages` echoes the exact submitted turn. The graph bridge reads that durable
//! receipt; copying a message into the inbox or writing it to stdin is never delivery.

use std::collections::BTreeSet;
use std::fs;
use std::io::{BufRead as _, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::delivery_ledger::{self, Begin, Correlation, Evidence, NegativeReceipt, Phase};
use crate::harness_state::{self, Activity, BlockedOn, InputBuffer, Observation};
use crate::harness_timeline::{EntryType, Role};
use crate::provider_session::{
    STOP, SessionObserver, describe_exit, install_signal_handler, stop_provider_group,
};
use crate::{ding, harness_timeline, message, pretrust, run};

const POLL: Duration = Duration::from_millis(100);
const INBOX_REFRESH: Duration = Duration::from_millis(250);
const SESSION_FILE: &str = "claude-session-id";

#[derive(Debug)]
struct PendingDelivery {
    filename: String,
    text: String,
}

/// Run Claude with explicit private state paths.
pub fn run_controlled_paths(
    catalog_root: &Path,
    state_dir: &Path,
    agent_dir: &Path,
    identity: String,
    runtime_id: String,
    claude_argv: Vec<String>,
) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    fs::create_dir_all(agent_dir)?;
    let inbox = message::inbox_dir(agent_dir);
    fs::create_dir_all(&inbox)?;
    fs::create_dir_all(message::archive_dir(agent_dir))?;
    let workspace = std::env::current_dir().context("reading the Claude driver workspace")?;
    pretrust::pretrust_claude(std::slice::from_ref(&workspace))
        .with_context(|| format!("admitting Claude driver workspace {}", workspace.display()))?;

    let (executable, args, boot_prompt) = stream_argv(state_dir, claude_argv)?;
    install_signal_handler();
    let observer = SessionObserver::new(agent_dir, &identity, "claude", &runtime_id)?;
    let mut state_writer =
        harness_state::Writer::new(agent_dir, &identity, "claude", Some(runtime_id.clone()))
            .with_ownership(observer.session().to_string(), observer.seq());
    let mut timeline =
        harness_timeline::Writer::new(agent_dir, "claude", observer.session().to_string());

    let mut child = Command::new(&executable)
        .args(&args)
        .env(crate::claude_session::RUNTIME_ID_ENV, &runtime_id)
        .env(crate::claude_session::SESSION_ENV, observer.session())
        .env(
            crate::claude_session::SESSION_SEQ_ENV,
            observer.seq().to_string(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("starting Claude stream driver with {executable}"))?;
    let mut stdin = child
        .stdin
        .take()
        .context("Claude stream stdin is unavailable")?;
    let stdout = child
        .stdout
        .take()
        .context("Claude stream stdout is unavailable")?;
    let (line_tx, line_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });

    write_user_turn(&mut stdin, &boot_prompt)?;
    state_writer.observe(Observation::new(
        Activity::Active,
        BlockedOn::None,
        InputBuffer::Empty,
    ))?;

    let ledger_path = state_dir.join(delivery_ledger::LEDGER_FILE);
    let mut binding = read_session_id(state_dir).unwrap_or_else(|| "pending-session".into());
    let mut ledger = open_ledger(&ledger_path, &identity, &runtime_id);
    let mut boot_replay = Some(boot_prompt);
    let mut pending: Option<PendingDelivery> = None;
    let mut next_inbox_refresh = Instant::now();
    let mut output_sequence = 0_u64;

    loop {
        if STOP.load(Ordering::SeqCst) {
            // Stream mode owns its child directly instead of going through run_provider's poll
            // loop, so it must honor the shared stop edge itself. Without this check a terminal
            // hangup kills only the supporting PTY daemon and leaves Claude's stream process
            // group behind while the supervisor starts a successor.
            drop(stdin);
            stop_provider_group(&mut child, Some(&observer))?;
            return Ok(());
        }
        while let Ok(line) = line_rx.try_recv() {
            let line = line.context("reading Claude stream output")?;
            if line.trim().is_empty() {
                continue;
            }
            let frame: Value = serde_json::from_str(&line)
                .with_context(|| format!("decoding Claude stream frame: {line}"))?;
            output_sequence = output_sequence.saturating_add(1);
            if frame.get("type").and_then(Value::as_str) == Some("system")
                && frame.get("subtype").and_then(Value::as_str) == Some("init")
                && let Some(session) = frame.get("session_id").and_then(Value::as_str)
            {
                persist_session_id(state_dir, session)?;
                if binding != session {
                    binding = session.to_owned();
                    ledger.rebind(&binding)?;
                }
                timeline.append(
                    format!("system-init-{output_sequence}"),
                    Role::System,
                    EntryType::Status,
                    json!({"status": "ready"}),
                    true,
                )?;
            }

            if let Some(text) = replayed_user_text(&frame) {
                timeline.append(
                    format!("user-replay-{output_sequence}"),
                    Role::User,
                    EntryType::Message,
                    json!({"text": text}),
                    true,
                )?;
                state_writer.observe(Observation::new(
                    Activity::Active,
                    BlockedOn::None,
                    InputBuffer::Empty,
                ))?;
                if boot_replay.as_deref() == Some(text) {
                    boot_replay = None;
                } else if pending
                    .as_ref()
                    .is_some_and(|delivery| delivery.text == text)
                {
                    let delivered = pending.take().expect("the matching delivery exists");
                    ledger.record(&delivered.filename, Evidence::Consumed)?;
                }
            }

            if frame.get("type").and_then(Value::as_str) == Some("assistant") {
                timeline.append(
                    format!("assistant-{output_sequence}"),
                    Role::Assistant,
                    EntryType::Message,
                    frame
                        .pointer("/message/content")
                        .cloned()
                        .map(|content| json!({"content": content}))
                        .unwrap_or_else(|| json!({"content": []})),
                    frame
                        .pointer("/message/stop_reason")
                        .is_some_and(|value| !value.is_null()),
                )?;
            }

            if frame.get("type").and_then(Value::as_str) == Some("result") {
                let error = frame.get("is_error").and_then(Value::as_bool) == Some(true);
                let reason = error.then(|| claude_error_reason(&frame));
                let mut observation =
                    Observation::new(Activity::Idle, BlockedOn::None, InputBuffer::Empty);
                if let Some(reason) = reason {
                    observation = observation.with_reason(reason);
                }
                state_writer.observe(observation)?;
                timeline.append(
                    format!("result-{output_sequence}"),
                    Role::System,
                    if error { EntryType::Error } else { EntryType::Status },
                    if error {
                        json!({"message": frame.get("result").and_then(Value::as_str).unwrap_or("Claude turn failed")})
                    } else {
                        json!({"status": "idle"})
                    },
                    true,
                )?;
                if let Some(usage) = frame.get("usage").cloned() {
                    timeline.append(
                        format!("usage-{output_sequence}"),
                        Role::System,
                        EntryType::Usage,
                        usage,
                        true,
                    )?;
                }
            }
        }

        if Instant::now() >= next_inbox_refresh {
            let unread = message::list_inbox(&inbox)?;
            let unread_names = unread
                .iter()
                .map(|item| item.filename.as_str())
                .collect::<BTreeSet<_>>();
            ledger.prune(|filename| unread_names.contains(filename))?;
            if pending.is_none()
                && boot_replay.is_none()
                && binding != "pending-session"
                && let Some(next) = unread
                    .into_iter()
                    .find(|item| !ledger.settled(&item.filename))
            {
                // A prior incarnation that died after its write but before the replay is
                // ambiguous. At-least-once delivery deliberately retries it; the stable PING
                // id and shared boot contract make duplicate application idempotent.
                if ledger.entry(&next.filename).is_some_and(|entry| {
                    entry.phase == Phase::Attempted
                        && entry.incarnation.as_deref() != Some(observer.session())
                }) {
                    ledger.negative(
                        &next.filename,
                        NegativeReceipt::RetryAfterAbandonedIncarnation,
                    )?;
                }
                if ledger.retry(&next.filename) == delivery_ledger::RetryDecision::Retry {
                    let text = ding::poke_text(catalog_root, &run::detect_host(), &identity, &next);
                    ledger.begin(Begin {
                        filename: next.filename.clone(),
                        binding: binding.clone(),
                        correlation: Correlation::native(stable_correlation(
                            &identity,
                            &binding,
                            &next.filename,
                        )),
                        incarnation: Some(observer.session().to_string()),
                    })?;
                    write_user_turn(&mut stdin, &text)?;
                    pending = Some(PendingDelivery {
                        filename: next.filename,
                        text,
                    });
                }
            }
            next_inbox_refresh = Instant::now() + INBOX_REFRESH;
        }

        if let Some(exit) = child.try_wait().context("polling Claude stream process")? {
            let label = describe_exit(exit);
            observer.ended(&label);
            anyhow::ensure!(exit.success(), "Claude stream process exited with {exit}");
            return Ok(());
        }
        thread::sleep(POLL);
    }
}

/// Read the exact inbox files whose user turns Claude replayed.
pub fn consumed_delivery_filenames(
    state_dir: &Path,
    identity: &str,
    runtime_id: &str,
) -> Result<BTreeSet<String>> {
    let path = state_dir.join(delivery_ledger::LEDGER_FILE);
    if !path.is_file() {
        return Ok(BTreeSet::new());
    }
    let ledger = open_ledger(&path, identity, runtime_id);
    if let Some(reason) = ledger.quarantined() {
        anyhow::bail!("Claude delivery receipt ledger is quarantined: {reason}");
    }
    Ok(ledger
        .entries()
        .iter()
        .filter(|entry| entry.phase == Phase::Consumed)
        .map(|entry| entry.filename.clone())
        .collect())
}

fn open_ledger(path: &Path, identity: &str, runtime_id: &str) -> delivery_ledger::Ledger {
    delivery_ledger::Ledger::open(
        path,
        delivery_ledger::Harness::Claude.profile(),
        identity,
        runtime_id,
        |binding, filename| stable_correlation(identity, binding, filename),
    )
}

fn stable_correlation(identity: &str, binding: &str, filename: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st3.claude-stream-user-message.v1");
    for value in [identity, binding, filename] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("st3:{:x}", hash.finalize())
}

fn stream_argv(state_dir: &Path, mut argv: Vec<String>) -> Result<(String, Vec<String>, String)> {
    anyhow::ensure!(!argv.is_empty(), "Claude stream argv is empty");
    let executable = argv.remove(0);
    let boot_prompt = argv
        .pop()
        .context("Claude stream argv has no boot prompt")?;
    let mut filtered = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if matches!(
            argv[index].as_str(),
            "--channels" | "--input-format" | "--output-format"
        ) {
            index += 2;
            continue;
        }
        if argv[index].starts_with("--channels=")
            || argv[index].starts_with("--input-format=")
            || argv[index].starts_with("--output-format=")
            || matches!(
                argv[index].as_str(),
                "-p" | "--print" | "--verbose" | "--replay-user-messages" | "--include-hook-events"
            )
        {
            index += 1;
            continue;
        }
        filtered.push(argv[index].clone());
        index += 1;
    }
    for required in [
        "-p",
        "--verbose",
        "--replay-user-messages",
        "--include-hook-events",
    ] {
        if !filtered.iter().any(|value| value == required) {
            filtered.push(required.into());
        }
    }
    filtered.extend([
        "--input-format".into(),
        "stream-json".into(),
        "--output-format".into(),
        "stream-json".into(),
    ]);
    if !filtered
        .iter()
        .any(|value| matches!(value.as_str(), "--resume" | "-c" | "--continue"))
        && let Some(session) = read_session_id(state_dir)
    {
        filtered.extend(["--resume".into(), session]);
    }
    Ok((executable, filtered, boot_prompt))
}

fn write_user_turn(stdin: &mut impl Write, text: &str) -> Result<()> {
    serde_json::to_writer(
        &mut *stdin,
        &json!({
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]}
        }),
    )?;
    stdin.write_all(b"\n")?;
    stdin.flush()?;
    Ok(())
}

fn replayed_user_text(frame: &Value) -> Option<&str> {
    (frame.get("type").and_then(Value::as_str) == Some("user")
        && frame.get("isReplay").and_then(Value::as_bool) == Some(true))
    .then(|| {
        frame
            .pointer("/message/content/0/text")
            .and_then(Value::as_str)
    })
    .flatten()
}

fn claude_error_reason(frame: &Value) -> String {
    let text = frame
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if text.contains("capacity") || text.contains("overloaded") || text.contains("rate limit") {
        "providerCapacity".into()
    } else {
        "providerError".into()
    }
}

fn read_session_id(state_dir: &Path) -> Option<String> {
    fs::read_to_string(state_dir.join(SESSION_FILE))
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn persist_session_id(state_dir: &Path, session: &str) -> Result<()> {
    let temporary = state_dir.join(format!(".{SESSION_FILE}.tmp"));
    fs::write(&temporary, format!("{session}\n"))?;
    fs::rename(temporary, state_dir.join(SESSION_FILE))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_argv_removes_the_channel_and_resumes_the_saved_session() {
        let root = tempfile::tempdir().unwrap();
        persist_session_id(root.path(), "session-1").unwrap();
        let (program, args, prompt) = stream_argv(
            root.path(),
            vec![
                "claude".into(),
                "--channels".into(),
                "plugin:st3-channel@st3".into(),
                "--input-format=text".into(),
                "--output-format".into(),
                "text".into(),
                "--print".into(),
                "--model".into(),
                "sonnet".into(),
                "boot".into(),
            ],
        )
        .unwrap();
        assert_eq!(program, "claude");
        assert_eq!(prompt, "boot");
        assert!(!args.iter().any(|arg| arg == "--channels"));
        assert!(!args.iter().any(|arg| arg == "text"));
        assert_eq!(args.iter().filter(|arg| *arg == "-p").count(), 1);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--resume", "session-1"])
        );
        assert_eq!(
            args.windows(2)
                .filter(|pair| *pair == ["--input-format", "stream-json"])
                .count(),
            1
        );
    }

    #[test]
    fn only_an_exact_replayed_user_message_is_a_receipt() {
        let replay = json!({
            "type": "user",
            "isReplay": true,
            "message": {"content": [{"type": "text", "text": "hello"}]}
        });
        assert_eq!(replayed_user_text(&replay), Some("hello"));
        let mut unacknowledged = replay;
        unacknowledged["isReplay"] = Value::Bool(false);
        assert_eq!(replayed_user_text(&unacknowledged), None);
    }
}
