use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};

use sha2::{Digest as _, Sha256};
use st2::event::{self, EventReceiptStatus, RING_CAPACITY};
use st2::message;

fn declare_agent(root: &Path, desired: &str, streams: &str) -> PathBuf {
    let directory = root.join("agents/hetz/worker");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"hetz\"\n  desired-state {desired}\n  command \"agent\"\n{streams}}}\n"
        ),
    )
    .unwrap();
    directory
}

fn emit(root: &Path, id: &str, key: Option<&str>, supersede: bool) -> event::EventReceipt {
    event::emit(
        root,
        "hetz",
        "hetz.worker",
        "gh-ci",
        id,
        key,
        Some(&format!("CI {id}")),
        &format!("{{\"id\":\"{id}\"}}"),
        supersede,
    )
    .unwrap()
}

#[test]
fn stable_event_identity_publishes_exactly_one_canonical_message() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");

    let first = emit(catalog.path(), "run-812", None, false);
    let replay = emit(catalog.path(), "run-812", None, false);

    assert_eq!(first.status, EventReceiptStatus::Created);
    assert_eq!(replay.status, EventReceiptStatus::Deduplicated);
    assert_eq!(first.filename, replay.filename);
    let inbox = message::list_inbox(&message::inbox_dir(&agent)).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].from.as_deref(), Some("hetz.worker/gh-ci"));
    assert_eq!(inbox[0].stream.as_deref(), Some("gh-ci"));
    assert_eq!(inbox[0].event_id.as_deref(), Some("run-812"));
    assert!(!agent.join("resources/sent").exists());
}

#[test]
fn concurrent_replays_publish_exactly_one_event() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let root = Arc::new(catalog.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(12));
    let threads = (0..12)
        .map(|_| {
            let root = Arc::clone(&root);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                emit(&root, "delivery-1", None, false)
            })
        })
        .collect::<Vec<_>>();
    let receipts = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.status == EventReceiptStatus::Created)
            .count(),
        1
    );
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].filename == pair[1].filename)
    );
    assert_eq!(
        message::list_inbox(&message::inbox_dir(&agent))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn conflicting_reuse_and_undeclared_or_suspended_ingress_fail_closed() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    emit(catalog.path(), "same", None, false);
    let conflict = event::emit(
        catalog.path(),
        "hetz",
        "hetz.worker",
        "gh-ci",
        "same",
        None,
        Some("different"),
        "different",
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(
        conflict.contains("reused with different content"),
        "{conflict}"
    );
    let undeclared = event::emit(
        catalog.path(),
        "hetz",
        "hetz.worker",
        "other",
        "1",
        None,
        None,
        "x",
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(
        undeclared.contains("does not declare stream 'other'"),
        "{undeclared}"
    );

    fs::write(
        agent.join("agent.kdl"),
        "agent \"worker\" {\n  host \"hetz\"\n  desired-state \"suspended\" reason=\"hold\"\n  command \"agent\"\n  stream \"gh-ci\" {}\n}\n",
    )
    .unwrap();
    let suspended = event::emit(
        catalog.path(),
        "hetz",
        "hetz.worker",
        "gh-ci",
        "2",
        None,
        None,
        "x",
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(suspended.contains("eyes are closed"), "{suspended}");
    assert_eq!(
        message::list_inbox(&message::inbox_dir(&agent))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn supersede_collapses_only_the_matching_key_and_preserves_archive_receipts() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let pr_1_old = emit(catalog.path(), "pr1-fail", Some("pr-1"), true);
    let pr_2 = emit(catalog.path(), "pr2-fail", Some("pr-2"), true);
    let pr_1_new = emit(catalog.path(), "pr1-pass", Some("pr-1"), true);

    let inbox = message::inbox_dir(&agent);
    let archive = message::archive_dir(&agent);
    assert!(!inbox.join(&pr_1_old.filename).exists());
    assert!(archive.join(&pr_1_old.filename).exists());
    assert!(inbox.join(&pr_2.filename).exists());
    assert!(inbox.join(&pr_1_new.filename).exists());
    assert_eq!(
        pr_1_new.superseded.as_deref(),
        Some(pr_1_old.filename.as_str())
    );
    let replay = emit(catalog.path(), "pr1-fail", Some("pr-1"), true);
    assert_eq!(replay.status, EventReceiptStatus::Deduplicated);
    assert!(!inbox.join(&pr_1_old.filename).exists());
}

#[test]
fn keyless_supersede_replaces_the_stream_wide_head() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let old = emit(catalog.path(), "old", None, true);
    let new = emit(catalog.path(), "new", None, true);
    assert_eq!(new.superseded.as_deref(), Some(old.filename.as_str()));
    assert!(!message::inbox_dir(&agent).join(old.filename).exists());
}

#[test]
fn crash_replay_honors_an_archive_receipt_and_never_restores_the_inbox_copy() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let filename = "1784649988123-proof1.md";
    let rendered = event::render_event(
        "hetz.worker/gh-ci",
        Some("CI archived"),
        "gh-ci",
        "archived",
        None,
        "payload",
    );
    let archive = message::archive_dir(&agent);
    fs::create_dir_all(&archive).unwrap();
    fs::write(archive.join(filename), &rendered).unwrap();
    let state_dir = agent.join("resources/streams/gh-ci");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "stream": "gh-ci",
            "recipient": "hetz.worker",
            "pending": {
                "eventId": "archived",
                "filename": filename,
                "key": null,
                "renderedSha256": format!("{:x}", Sha256::digest(rendered.as_bytes())),
                "supersede": false
            },
            "recent": []
        }))
        .unwrap(),
    )
    .unwrap();

    let receipt = event::emit(
        catalog.path(),
        "hetz",
        "hetz.worker",
        "gh-ci",
        "archived",
        None,
        Some("CI archived"),
        "payload",
        false,
    )
    .unwrap();
    assert_eq!(receipt.status, EventReceiptStatus::Deduplicated);
    assert!(!message::inbox_dir(&agent).join(filename).exists());
    assert!(archive.join(filename).exists());
}

#[test]
fn subject_frontmatter_injection_is_refused_before_any_write() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let error = event::emit(
        catalog.path(),
        "hetz",
        "hetz.worker",
        "gh-ci",
        "safe-id",
        None,
        Some("safe\nevent-id: forged"),
        "body",
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("event subject"), "{error}");
    assert!(!message::inbox_dir(&agent).exists());
    assert!(!agent.join("resources/streams").exists());
}

#[test]
fn stream_state_is_bounded_and_forgets_only_beyond_its_honest_horizon() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let first = emit(catalog.path(), "event-0", None, false);
    for index in 1..=RING_CAPACITY {
        emit(catalog.path(), &format!("event-{index}"), None, false);
    }
    let state = fs::read_to_string(agent.join("resources/streams/gh-ci/state.json")).unwrap();
    let state: serde_json::Value = serde_json::from_str(&state).unwrap();
    assert_eq!(state["recent"].as_array().unwrap().len(), RING_CAPACITY);
    let replay = emit(catalog.path(), "event-0", None, false);
    assert_eq!(replay.status, EventReceiptStatus::Created);
    assert_ne!(replay.filename, first.filename);
}

#[test]
fn event_emit_cli_returns_a_stable_json_receipt_and_ding_marks_the_record() {
    let catalog = tempfile::tempdir().unwrap();
    let agent = declare_agent(catalog.path(), "\"running\"", "  stream \"gh-ci\" {}\n");
    let mut child = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            catalog.path().to_str().unwrap(),
            "event",
            "emit",
            "hetz.worker",
            "--stream",
            "gh-ci",
            "--event-id",
            "cli-1",
            "--subject",
            "CLI proof",
            "--host",
            "hetz",
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write as _;
    child.stdin.take().unwrap().write_all(b"payload").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["status"], "created");
    assert_eq!(receipt["recipient"], "hetz.worker");
    let message = message::list_inbox(&message::inbox_dir(&agent))
        .unwrap()
        .remove(0);
    let ding = st2::ding::poke_text(catalog.path(), "hetz", "hetz.worker", &message);
    assert!(
        ding.starts_with("[DING] » hetz.worker/gh-ci: CLI proof"),
        "{ding}"
    );
}
