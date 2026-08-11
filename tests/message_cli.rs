//! CLI coverage for message-list filters and output modes.

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};

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

#[test]
fn delivery_is_a_read_only_bounded_body_view() {
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("bob/inbox");
    write_message(&inbox, 1_700_000_000_000, "aaaaaa", "alice");

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["message", "delivery", "bob", "--root"])
        .arg(tmp.path())
        .args(["--host", "h"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("st2.inbox-delivery.v1"));
    assert!(text.contains("1700000000000-aaaaaa.md"));
    assert!(text.contains(r#""body":"body\n""#));
    assert!(inbox.join("1700000000000-aaaaaa.md").is_file());
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
fn send_routes_only_by_stable_identity_in_a_catalog_and_preserves_catalogless_bus() {
    let send = |root: &Path, recipient: &str, root_flag: &str, external: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
        command
            .args(["message", "send", recipient, root_flag])
            .arg(root)
            .args(["--host", "h", "--as", "h.sender"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(identity) = external {
            command.env("ST2_EVAL_REQUESTER", identity);
        }
        let mut child = command.spawn().unwrap();
        child.stdin.take().unwrap().write_all(b"work\n").unwrap();
        child.wait_with_output().unwrap()
    };

    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path(), "worker");
    let declaration = catalog.path().join("h/worker/agent.kdl");
    fs::write(
        &declaration,
        fs::read_to_string(&declaration).unwrap().replace(
            "  type \"service\"\n",
            "  type \"service\"\n  name \"Shared Worker\"\n",
        ),
    )
    .unwrap();

    let display = send(catalog.path(), "Shared Worker", "--catalog", None);
    assert!(!display.status.success());
    assert!(
        String::from_utf8_lossy(&display.stderr)
            .contains("no agent 'Shared Worker' found in catalog")
    );
    assert!(!catalog.path().join("Shared Worker").exists());

    let stable = send(catalog.path(), "h.worker", "--catalog", None);
    assert!(
        stable.status.success(),
        "{}",
        String::from_utf8_lossy(&stable.stderr)
    );
    assert_eq!(
        fs::read_dir(catalog.path().join("h/worker/resources/inbox"))
            .unwrap()
            .count(),
        1
    );

    fs::create_dir_all(catalog.path().join("requester/inbox")).unwrap();
    assert!(!send(catalog.path(), "requester", "--catalog", None).status.success());
    assert!(!send(catalog.path(), "requester", "--catalog", Some("other"))
        .status
        .success());
    let external = send(catalog.path(), "requester", "--catalog", Some("requester"));
    assert!(
        external.status.success(),
        "{}",
        String::from_utf8_lossy(&external.stderr)
    );
    assert_eq!(
        fs::read_dir(catalog.path().join("requester/inbox"))
            .unwrap()
            .count(),
        1
    );

    let flat = tempfile::tempdir().unwrap();
    let raw = send(flat.path(), "requester", "--root", None);
    assert!(
        raw.status.success(),
        "{}",
        String::from_utf8_lossy(&raw.stderr)
    );
    assert_eq!(
        fs::read_dir(flat.path().join("requester/inbox"))
            .unwrap()
            .count(),
        1
    );
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
