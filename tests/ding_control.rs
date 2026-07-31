use std::fs;
use std::path::Path;
use std::process::Command;

use st2::message::{inbox_dir, send_to_inbox};

fn st2() -> Command {
    Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

#[test]
fn hook_owned_cli_records_only_exact_unread_filenames() {
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agents/host/worker");
    write(
        tmp.path(),
        "agents/host/worker/agent.kdl",
        r#"agent "worker" { host "host"; command "agent"; ding }"#,
    );
    let inbox = inbox_dir(&agent_dir);
    let filename = send_to_inbox(&inbox, "sender", Some("work"), None, &[], "body").unwrap();

    let output = st2()
        .arg("--catalog")
        .arg(tmp.path())
        .args([
            "ding-control",
            "--identity",
            "host.worker",
            "--host",
            "host",
            "hook-owned",
            "--message",
            &filename,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt = Path::new(std::str::from_utf8(&output.stdout).unwrap().trim());
    assert!(receipt.is_file());
    let value: serde_json::Value = serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
    assert_eq!(value["kind"], "hook-owned");
    assert_eq!(value["messages"], serde_json::json!([filename]));

    let rejected = st2()
        .arg("--catalog")
        .arg(tmp.path())
        .args([
            "ding-control",
            "--identity",
            "host.worker",
            "--host",
            "host",
            "hook-owned",
            "--message",
            "1785000000000-abc123.md",
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("not currently unread"));
}
