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

fn list(root: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["message", "ls", "bob", "--root"])
        .arg(root)
        .args(["--host", "h"])
        .args(extra)
        .output()
        .unwrap()
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
