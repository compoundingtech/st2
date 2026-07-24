#![cfg(unix)]

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use st2::{context, message};

struct Fixture {
    _tmp: tempfile::TempDir,
    catalog: PathBuf,
    context: PathBuf,
    inbox: PathBuf,
    bin: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let agent = catalog.join("agents/Silber/cos");
        let context = context::context_dir(&agent);
        let inbox = message::inbox_dir(&agent);
        fs::create_dir_all(&context).unwrap();
        fs::create_dir_all(&inbox).unwrap();
        fs::write(
            agent.join("agent.kdl"),
            r#"agent "cos" {
  host "Silber"
  workspace "/tmp"
  env { ST_AGENT "Silber.cos" }
  command "codex"
  ding
}"#,
        )
        .unwrap();

        let bin = tmp.path().join("bin");
        fs::create_dir_all(&bin).unwrap();
        symlink(env!("CARGO_BIN_EXE_st2"), bin.join("st2")).unwrap();
        let state = tmp.path().join("state");

        Self {
            _tmp: tmp,
            catalog,
            context,
            inbox,
            bin,
            state,
        }
    }

    fn run(&self, script: &str) -> Output {
        let current_path = std::env::var("PATH").unwrap_or_default();
        Command::new("/bin/bash")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("hooks")
                    .join(script),
            )
            .env("PATH", format!("{}:{current_path}", self.bin.display()))
            .env("ST_ROOT", &self.catalog)
            .env("CATALOG", &self.catalog)
            .env("ST_AGENT", "Silber.cos")
            .env("XDG_STATE_HOME", &self.state)
            .output()
            .unwrap()
    }
}

fn jq_available() -> bool {
    Command::new("jq")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

#[test]
fn session_start_emits_current_codex_context_envelope() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Codex hook");
        return;
    }
    let fixture = Fixture::new();
    context::write_now(&fixture.context, "working on the materializer\n").unwrap();
    message::send_to_inbox(
        &fixture.inbox,
        "Silber.worker",
        Some("status"),
        None,
        &[],
        "done",
    )
    .unwrap();

    let output = fixture.run("codex-session-start.sh");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["hookSpecificOutput"]["hookEventName"], "SessionStart");
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(additional.contains("working on the materializer"));
    assert!(additional.contains("## st2 inbox (1 unread)"));
    assert!(additional.contains("Run the st2 boot ritual"));
}

#[test]
fn pre_compact_stubs_only_empty_context_and_never_clobbers_real_state() {
    let fixture = Fixture::new();
    let output = fixture.run("codex-pre-compact.sh");
    assert!(output.status.success());
    let stub = context::read(&fixture.context, context::View::Now);
    assert!(stub.contains("pre-compact stub"));

    context::write_now(&fixture.context, "real checkpoint\n").unwrap();
    let output = fixture.run("codex-pre-compact.sh");
    assert!(output.status.success());
    assert_eq!(
        context::read(&fixture.context, context::View::Now),
        "real checkpoint\n"
    );
}

#[test]
fn stop_uses_since_cursor_and_emits_only_new_messages() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Codex hook");
        return;
    }
    let fixture = Fixture::new();
    message::send_to_inbox(
        &fixture.inbox,
        "Silber.worker",
        Some("first"),
        None,
        &[],
        "one",
    )
    .unwrap();

    let first = fixture.run("codex-stop.sh");
    assert!(first.status.success());
    let json: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(json["hookSpecificOutput"]["hookEventName"], "Stop");
    assert!(
        json["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap()
            .contains("first")
    );

    let quiet = fixture.run("codex-stop.sh");
    assert!(quiet.status.success());
    assert!(quiet.stdout.is_empty());

    std::thread::sleep(Duration::from_millis(3));
    message::send_to_inbox(
        &fixture.inbox,
        "Silber.worker",
        Some("second"),
        None,
        &[],
        "two",
    )
    .unwrap();
    let second = fixture.run("codex-stop.sh");
    let json: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(additional.contains("second"));
    assert!(!additional.contains("first"));
}
