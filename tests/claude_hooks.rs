#![cfg(unix)]

//! The Claude hooks, exercised as the harness runs them. These assert the channel the payload
//! arrives on, not merely that a payload was produced: the defect these cover shipped a hook that
//! emitted a byte-perfect payload on a channel the model never sees, so "the hook printed it" and
//! "the model received it" are exactly the two things that must not be conflated.

use std::fs;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use st2::context;

fn bash() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|dir| dir.join("bash"))
        .find(|path| path.is_file())
        .expect("bash is available on PATH")
}

fn jq_available() -> bool {
    Command::new("jq")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

struct Fixture {
    _tmp: tempfile::TempDir,
    catalog: PathBuf,
    context: PathBuf,
    bin: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let agent = catalog.join("agents/Silber/cos");
        let context = context::context_dir(&agent);
        fs::create_dir_all(&context).unwrap();
        fs::write(
            agent.join("agent.kdl"),
            r#"agent "cos" {
  host "Silber"
  workspace "/tmp"
  env { ST_AGENT "Silber.cos" }
  command "claude"
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
            bin,
            state,
        }
    }

    fn run(&self, script: &str) -> Output {
        self.run_with(script, &[])
    }

    /// `overrides` are applied last, so a test can drop `PATH` entries or retune staleness.
    fn run_with(&self, script: &str, overrides: &[(&str, &str)]) -> Output {
        self.run_with_input(script, overrides, "")
    }

    fn run_with_input(&self, script: &str, overrides: &[(&str, &str)], input: &str) -> Output {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("hooks")
            .join(script);
        let current_path = std::env::var("PATH").unwrap_or_default();
        let mut command = Command::new(bash());
        command
            .arg(script)
            .env("PATH", format!("{}:{current_path}", self.bin.display()))
            .env("ST_ROOT", &self.catalog)
            .env("CATALOG", &self.catalog)
            .env("ST_AGENT", "Silber.cos")
            .env("XDG_STATE_HOME", &self.state);
        for (key, value) in overrides {
            command.env(key, value);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }

    fn stop_failure_record(&self) -> PathBuf {
        self.state
            .join("st2/hook-events/stop-failure/Silber.cos.jsonl")
    }
}

/// The payload must arrive as `additionalContext` on stdout. An earlier revision of this hook wrote
/// the identical bytes to stderr and exited 2, which the harness renders as a hook error and never
/// hands to the model — so `stderr.is_empty()` and `status == 0` are the load-bearing assertions
/// here, not the presence of the text.
#[test]
fn session_start_delivers_context_on_the_supported_channel_and_never_on_stderr() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    context::write_now(&fixture.context, "rehydration canary PELICAN-7742\n").unwrap();

    let output = fixture.run("claude-session-start.sh");

    assert!(
        output.status.success(),
        "the hook must exit 0; exit 2 is rendered as a hook error and discarded. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "nothing may go to stderr — it is the channel the model cannot see. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["hookSpecificOutput"]["hookEventName"], "SessionStart");
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(additional.contains("rehydration canary PELICAN-7742"));
    assert!(additional.contains(r#"<context source="st2/context/now.md" agent="Silber.cos">"#));
    assert!(additional.contains("Run the st2 boot ritual"));
    assert!(additional.contains("set your status to busy"));
}

#[test]
fn session_start_delivers_context_larger_than_the_platform_argument_limit() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    let large_context = "x".repeat(256 * 1024);
    context::write_now(&fixture.context, &large_context).unwrap();

    let output = fixture.run("claude-session-start.sh");

    assert!(
        output.status.success(),
        "large durable context must not cross an argv boundary. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(
        additional.contains(&large_context),
        "the complete durable context must reach Claude without truncation"
    );
}

/// Missing durable state is an ordinary cold start: the ritual still has to reach the model, and the
/// context envelope must be absent rather than empty.
#[test]
fn session_start_still_delivers_the_ritual_when_no_context_exists() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();

    let output = fixture.run("claude-session-start.sh");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(additional.contains("Run the st2 boot ritual"));
    assert!(
        !additional.contains("<context"),
        "no state means no envelope, not an empty one: {additional}"
    );
}

/// `--fresh-within` is a staleness gate, so context older than the window must not be restored —
/// otherwise a replacement session resumes an objective its predecessor already abandoned.
#[test]
fn session_start_omits_context_that_fails_the_staleness_gate() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    context::write_now(&fixture.context, "stale objective\n").unwrap();

    let output = fixture.run_with("claude-session-start.sh", &[("ST_REHYDRATE_STALE_S", "0")]);

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let additional = json["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(!additional.contains("stale objective"), "{additional}");
    assert!(additional.contains("Run the st2 boot ritual"));
}

/// Fail-open: lifecycle enrichment must never prevent Claude from starting.
#[test]
fn session_start_fails_open_without_required_commands() {
    let fixture = Fixture::new();

    let output = fixture.run_with("claude-session-start.sh", &[("PATH", "")]);

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn stop_failure_appends_a_private_redacted_record_without_a_supervisor() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    let payload = r#"{
  "session_id": "session-safe",
  "hook_event_name": "StopFailure",
  "error": "authentication_failed",
  "error_details": "Login expired: Bearer bearer-secret-1234567890 and sk-ant-api03-abcdefghijklmnop",
  "last_assistant_message": "API Error: Login expired with abcdef0123456789abcdef0123456789abcdef0123456789",
  "api_key": "secret-key-value",
  "headers": {"X-Api-Key": "header-secret-value"},
  "nested": {
    "authorization": "Bearer nested-secret-1234567890",
    "safe": "retry in 60 seconds"
  }
}"#;

    let output = fixture.run_with_input("claude-stop-failure.sh", &[], payload);

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());

    let record_path = fixture.stop_failure_record();
    let contents = fs::read_to_string(&record_path).unwrap();
    let lines = contents.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "one hook call must append one JSONL line");
    let record: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(record["schema"], 1);
    assert_eq!(record["event"], "StopFailure");
    assert_eq!(record["identity"], "Silber.cos");
    assert_eq!(record["error_type"], "authentication_failed");
    assert!(
        record["timestamp"]
            .as_str()
            .is_some_and(|value| value.ends_with('Z'))
    );
    assert_eq!(record["payload"]["session_id"], "session-safe");
    assert_eq!(record["payload"]["error"], "authentication_failed");
    assert_eq!(record["payload"]["api_key"], "[REDACTED]");
    assert_eq!(record["payload"]["headers"]["X-Api-Key"], "[REDACTED]");
    assert_eq!(record["payload"]["nested"]["authorization"], "[REDACTED]");
    assert_eq!(record["payload"]["nested"]["safe"], "retry in 60 seconds");
    assert!(
        record["payload"]["error_details"]
            .as_str()
            .unwrap()
            .contains("[REDACTED]")
    );
    assert!(!contents.contains("bearer-secret"));
    assert!(!contents.contains("sk-ant-api03"));
    assert!(!contents.contains("secret-key-value"));
    assert!(!contents.contains("header-secret-value"));
    assert!(!contents.contains("nested-secret"));
    assert!(!contents.contains("abcdef0123456789abcdef"));
    assert_eq!(
        fs::metadata(record_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let status = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["status", "Silber.cos", "--root"])
        .arg(&fixture.catalog)
        .output()
        .unwrap();
    assert!(status.status.success());
    assert_eq!(String::from_utf8(status.stdout).unwrap(), "offline\n");
}

#[test]
fn stop_failure_records_old_and_new_error_fields_before_reaction_filtering() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();

    let first = fixture.run_with_input(
        "claude-stop-failure.sh",
        &[],
        r#"{"hook_event_name":"StopFailure","error_type":"max_output_tokens"}"#,
    );
    let second = fixture.run_with_input(
        "claude-stop-failure.sh",
        &[],
        r#"{"hook_event_name":"StopFailure","error":"overloaded"}"#,
    );

    assert!(first.status.success());
    assert!(second.status.success());
    let contents = fs::read_to_string(fixture.stop_failure_record()).unwrap();
    let records = contents
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["error_type"], "max_output_tokens");
    assert_eq!(records[1]["error_type"], "overloaded");
}

#[test]
fn stop_failure_never_copies_an_invalid_raw_payload_to_disk() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    let raw = "not-json Bearer raw-secret-1234567890";

    let output = fixture.run_with_input("claude-stop-failure.sh", &[], raw);

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    let contents = fs::read_to_string(fixture.stop_failure_record()).unwrap();
    let record: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    assert_eq!(record["error_type"], "unknown");
    assert_eq!(record["payload"], serde_json::Value::Null);
    assert_eq!(record["payload_error"], "invalid_json");
    assert!(!contents.contains("raw-secret"));
}

#[test]
fn stop_failure_remains_fail_open_when_the_record_path_is_unwritable() {
    if !jq_available() {
        eprintln!("SKIP: jq is required by the shipped Claude hook");
        return;
    }
    let fixture = Fixture::new();
    let blocked = fixture._tmp.path().join("blocked-state");
    fs::write(&blocked, "not a directory").unwrap();

    let output = fixture.run_with_input(
        "claude-stop-failure.sh",
        &[("XDG_STATE_HOME", blocked.to_str().unwrap())],
        r#"{"hook_event_name":"StopFailure","error":"server_error"}"#,
    );

    assert!(
        output.status.success(),
        "record failure must not block Claude. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());

    let status = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["status", "Silber.cos", "--root"])
        .arg(&fixture.catalog)
        .output()
        .unwrap();
    assert!(status.status.success());
    assert_eq!(String::from_utf8(status.stdout).unwrap(), "away\n");
}
