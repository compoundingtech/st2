//! CLI coverage for message-list filters and output modes.

use std::fs;
use std::path::Path;
use std::process::Command;

fn write_message(inbox: &Path, ts_ms: u64, suffix: &str, from: &str) {
    fs::create_dir_all(inbox).unwrap();
    fs::write(
        inbox.join(format!("{ts_ms:013}-{suffix}.md")),
        format!("---\nfrom: {from}\nsubject: test\n---\nbody\n"),
    )
    .unwrap();
}

fn write_agent(root: &Path, identity: &str) {
    let directory = root.join("h").join(identity);
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("agent.kdl"),
        format!(
            "agent \"{identity}\" {{\n  identity \"{identity}\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" {{ command \"x\" }}\n}}\n"
        ),
    )
    .unwrap();
}

fn list_identity(root: &Path, identity: &str, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["message", "ls", identity, "--root"])
        .arg(root)
        .args(["--host", "h"])
        .args(extra)
        .output()
        .unwrap()
}

fn list(root: &Path, extra: &[&str]) -> std::process::Output {
    list_identity(root, "bob", extra)
}

fn send(root: &Path, body: &str, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["message", "send", "bob", "--root"])
        .arg(root)
        .args(["--host", "h", "--as", "alice", "--message", body])
        .args(extra)
        .output()
        .unwrap()
}

fn archive(root: &Path, filename: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["message", "archive", "bob", filename, "--root"])
        .arg(root)
        .args(["--host", "h", "--as", "alice"])
        .output()
        .unwrap()
}

#[test]
fn ordinary_send_keeps_its_bytes_output_and_storage_path() {
    let temporary = tempfile::tempdir().unwrap();
    let output = send(
        temporary.path(),
        "ordinary body",
        &["--subject", "ordinary"],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let filename = String::from_utf8(output.stdout).unwrap().trim().to_string();
    assert!(st2::message::is_message_filename(&filename));
    assert_eq!(
        fs::read_to_string(temporary.path().join("bob/inbox").join(filename)).unwrap(),
        "---\nfrom: alice\nsubject: ordinary\n---\nordinary body\n"
    );
    assert!(!temporary.path().join("bob/message-receipts").exists());
}

#[test]
fn idempotent_send_returns_stable_json_across_inbox_and_archive_retries() {
    let temporary = tempfile::tempdir().unwrap();
    let flags = [
        "--subject",
        "daily check",
        "--source",
        "systemd:daily",
        "--event-id",
        "2026-07-31",
        "--json",
    ];
    let first = send(temporary.path(), "first body", &flags);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first["recipient"], "bob");
    assert_eq!(first["outcome"], "created");
    assert_eq!(first["receiptId"], first["filename"]);
    let filename = first["filename"].as_str().unwrap();

    let retry = send(temporary.path(), "changed body must not win", &flags);
    let retry: serde_json::Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(retry["outcome"], "deduplicated");
    assert_eq!(retry["filename"], first["filename"]);
    let message = st2::message::read_msg(&temporary.path().join("bob/inbox"), filename).unwrap();
    assert_eq!(message.body.trim_end(), "first body");
    assert_eq!(message.source.as_deref(), Some("systemd:daily"));
    assert_eq!(message.event_id.as_deref(), Some("2026-07-31"));
    let listed = list(temporary.path(), &["--count"]);
    assert!(listed.status.success());
    assert_eq!(String::from_utf8_lossy(&listed.stdout).trim(), "1");

    let archived = archive(temporary.path(), filename);
    assert!(
        archived.status.success(),
        "{}",
        String::from_utf8_lossy(&archived.stderr)
    );
    let retry = send(temporary.path(), "another changed body", &flags);
    let retry: serde_json::Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(retry["outcome"], "deduplicated");
    assert_eq!(retry["filename"], first["filename"]);
    assert!(
        st2::message::list_dir(&temporary.path().join("bob/inbox"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        st2::message::list_dir(&temporary.path().join("bob/archive"))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn idempotency_flags_are_paired_and_different_keys_do_not_deduplicate() {
    let temporary = tempfile::tempdir().unwrap();
    let incomplete = send(temporary.path(), "body", &["--source", "producer"]);
    assert!(!incomplete.status.success());
    assert!(!temporary.path().join("bob/inbox").exists());

    let first = send(
        temporary.path(),
        "one",
        &["--source", "producer", "--event-id", "one", "--json"],
    );
    let second = send(
        temporary.path(),
        "two",
        &["--source", "producer", "--event-id", "two", "--json"],
    );
    let first: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    let second: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(first["outcome"], "created");
    assert_eq!(second["outcome"], "created");
    assert_ne!(first["filename"], second["filename"]);
    assert_eq!(
        st2::message::list_dir(&temporary.path().join("bob/inbox"))
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn since_is_strict_and_composes_with_other_list_filters() {
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("bob/inbox");
    write_message(&inbox, 1_700_000_000_000, "aaaaaa", "alice");
    write_message(&inbox, 1_700_000_000_001, "bbbbbb", "alice");
    write_message(&inbox, 1_700_000_000_002, "cccccc", "carol");

    let out = list(tmp.path(), &["--since", "1700000000000", "--json"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["ts"], 1_700_000_000_001_u64);
    assert_eq!(rows[1]["ts"], 1_700_000_000_002_u64);

    // Equality is excluded, and --since composes with the existing sender/count filters.
    let out = list(
        tmp.path(),
        &["--since", "1700000000001", "--from", "alice", "--count"],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "0");
}

#[test]
fn archived_filename_wins_over_a_restored_raw_inbox_copy_in_all_list_modes() {
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("bob/inbox");
    let archive = tmp.path().join("bob/archive");
    let filename = "1700000000000-aaaaaa.md";
    write_message(&inbox, 1_700_000_000_000, "aaaaaa", "alice");
    fs::create_dir_all(&archive).unwrap();
    fs::copy(inbox.join(filename), archive.join(filename)).unwrap();

    let out = list(tmp.path(), &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("# 0 messages in bob inbox"));
    assert!(
        !inbox.join(filename).exists(),
        "listing must clean the inbox file shadowed by the archive receipt"
    );

    let out = list(tmp.path(), &["--count"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "0");

    let out = list(tmp.path(), &["--json"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "[]");

    // The receipt remains visible when the operator explicitly lists the archive.
    let out = list(tmp.path(), &["--archive", "--count"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "1");
}

#[test]
fn an_unknown_catalog_identity_fails_before_every_output_mode_reads_a_box() {
    let tmp = tempfile::tempdir().unwrap();
    write_agent(tmp.path(), "alice");

    // A populated flat box must not turn an unknown catalog identity into a valid result.
    write_message(
        &tmp.path().join("missing/inbox"),
        1_700_000_000_000,
        "aaaaaa",
        "alice",
    );
    write_message(
        &tmp.path().join("missing/archive"),
        1_700_000_000_001,
        "bbbbbb",
        "alice",
    );

    for extra in [
        vec![],
        vec!["--json"],
        vec!["--count"],
        vec!["--archive"],
        vec!["--archive", "--json"],
        vec!["--archive", "--count"],
    ] {
        let out = list_identity(tmp.path(), "missing", &extra);
        assert!(!out.status.success(), "mode {extra:?} must fail");
        assert!(
            out.stdout.is_empty(),
            "mode {extra:?} must fail before rendering output"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("no agent 'missing' found in catalog"));
        assert!(stderr.contains(&tmp.path().display().to_string()));
    }
}

#[test]
fn known_empty_native_and_catalog_less_flat_boxes_remain_valid() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path(), "alice");
    for extra in [
        vec![],
        vec!["--json"],
        vec!["--count"],
        vec!["--archive", "--count"],
    ] {
        let out = list_identity(catalog.path(), "alice", &extra);
        assert!(
            out.status.success(),
            "known native mode {extra:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let flat = tempfile::tempdir().unwrap();
    write_message(
        &flat.path().join("bob/inbox"),
        1_700_000_000_000,
        "aaaaaa",
        "alice",
    );
    let out = list(flat.path(), &["--count"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "1");
}

#[test]
fn orphan_mode_explicitly_reads_raw_flat_inbox_and_archive() {
    let tmp = tempfile::tempdir().unwrap();
    write_agent(tmp.path(), "alice");
    write_message(
        &tmp.path().join("missing/inbox"),
        1_700_000_000_000,
        "aaaaaa",
        "alice",
    );
    write_message(
        &tmp.path().join("missing/archive"),
        1_700_000_000_001,
        "bbbbbb",
        "alice",
    );

    let inbox = list_identity(tmp.path(), "missing", &["--orphan", "--json"]);
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inbox.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let archive = list_identity(tmp.path(), "missing", &["--orphan", "--archive", "--count"]);
    assert!(
        archive.status.success(),
        "{}",
        String::from_utf8_lossy(&archive.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&archive.stdout).trim(), "1");
}

#[test]
fn malformed_catalog_declarations_disable_implicit_flat_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let declaration = tmp.path().join("h/broken/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(&declaration, "agent \"broken\" { this is not valid").unwrap();
    write_message(
        &tmp.path().join("missing/inbox"),
        1_700_000_000_000,
        "aaaaaa",
        "alice",
    );

    for extra in [
        vec![],
        vec!["--json"],
        vec!["--count"],
        vec!["--archive"],
        vec!["--archive", "--json"],
        vec!["--archive", "--count"],
    ] {
        let out = list_identity(tmp.path(), "missing", &extra);
        assert!(!out.status.success(), "mode {extra:?} must fail");
        assert!(
            out.stdout.is_empty(),
            "mode {extra:?} must fail before rendering output"
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("no agent 'missing' found in catalog")
        );
    }
}
