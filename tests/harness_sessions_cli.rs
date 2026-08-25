use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn write_agent(catalog: &Path, host: &str, identity: &str, body: &str) {
    let path = catalog
        .join("agents")
        .join(host)
        .join(identity)
        .join("agent.kdl");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        format!("agent \"{identity}\" {{\n  host \"{host}\"\n{body}\n}}\n"),
    )
    .unwrap();
}

fn claude_body(workspace: &Path) -> String {
    format!(
        "  workspace {:?}\n  claude {{\n    prompt \"boot\"\n  }}",
        workspace.display().to_string()
    )
}

fn codex_body(workspace: &Path) -> String {
    format!(
        "  workspace {:?}\n  codex {{\n    prompt \"boot\"\n  }}",
        workspace.display().to_string()
    )
}

fn project_dir(home: &Path, workspace: &Path) -> PathBuf {
    let key = workspace
        .display()
        .to_string()
        .chars()
        .map(|character| match character {
            '/' | '.' => '-',
            other => other,
        })
        .collect::<String>();
    home.join(".claude/projects").join(key)
}

fn sessions(catalog: &Path, home: &Path, identity: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(catalog)
        .args(["harness", "sessions", "--identity", identity, "--json"])
        .env("HOME", home)
        .env_remove("CATALOG")
        .output()
        .unwrap()
}

#[test]
fn claude_inventory_returns_only_approved_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work.example/repo");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &claude_body(&workspace));
    let project = project_dir(&home, &workspace);
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join("session-1.jsonl"),
        concat!(
            "{\"type\":\"permission-mode\",\"sessionId\":\"session-1\",",
            "\"permissionMode\":\"default\"}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-1\",",
            "\"timestamp\":\"2026-08-25T10:00:00.000Z\",",
            "\"message\":{\"content\":\"must-not-leak\"}}\n",
            "{\"type\":\"permission-mode\",\"sessionId\":\"session-1\",",
            "\"permissionMode\":\"bypassPermissions\"}\n",
            "{\"type\":\"last-prompt\",\"sessionId\":\"session-1\",",
            "\"prompt\":\"must-not-leak-either\"}\n"
        ),
    )
    .unwrap();

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let repeated = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(repeated.status.success());
    assert_eq!(output.stdout, repeated.stdout, "unchanged evidence drifted");
    let raw = String::from_utf8(output.stdout).unwrap();
    assert!(!raw.contains("must-not-leak"), "{raw}");
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["schema"], "st2.harness-sessions.v1");
    assert_eq!(
        value["catalog"],
        catalog.canonicalize().unwrap().to_string_lossy().as_ref()
    );
    assert_eq!(value["host"], host);
    assert_eq!(value["identity"], format!("{host}.worker"));
    assert_eq!(value["driver"], "claude");
    assert_eq!(value["workspace"], workspace.to_string_lossy().as_ref());
    assert_eq!(value["complete"], true);
    assert_eq!(value["errors"], serde_json::json!([]));
    assert_eq!(value["sessions"].as_array().unwrap().len(), 1);
    let session = &value["sessions"][0];
    assert_eq!(session["sessionId"], "session-1");
    assert!(session["modifiedAt"].as_str().unwrap().ends_with('Z'));
    assert!(session["sizeBytes"].as_u64().unwrap() > 0);
    assert_eq!(session["permissionMode"]["index"], 3);
    assert_eq!(session["permissionMode"]["sessionId"], "session-1");
    assert_eq!(session["permissionMode"]["value"], "bypassPermissions");
    assert_eq!(session["lastRecord"]["index"], 4);
    assert_eq!(session["lastRecord"]["type"], "last-prompt");
    assert!(session["lastRecord"]["timestamp"].is_null());
}

#[test]
fn valid_empty_project_directory_is_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &claude_body(&workspace));
    fs::create_dir_all(project_dir(&home, &workspace)).unwrap();

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], true);
    assert_eq!(value["sessions"], serde_json::json!([]));
}

#[test]
fn lossy_workspace_collision_refuses_attribution() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let dotted = tmp.path().join("same.part");
    let slashed = tmp.path().join("same/part");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "one", &claude_body(&dotted));
    write_agent(&catalog, &host, "two", &claude_body(&slashed));

    let output = sessions(&catalog, &home, &format!("{host}.one"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["sessions"].as_array().unwrap().is_empty());
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains(&format!("{host}.two"))
    );
}

#[test]
fn another_claude_seats_default_cwd_participates_in_collision_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let host = st2::detect_host();
    let fallback = catalog.join("agents").join(&host).join("two");
    write_agent(&catalog, &host, "one", &claude_body(&fallback));
    write_agent(
        &catalog,
        &host,
        "two",
        "  claude {\n    prompt \"boot\"\n  }",
    );

    let output = sessions(&catalog, &home, &format!("{host}.one"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains(&format!("{host}.two"))
    );
}

#[test]
fn unsupported_native_driver_returns_an_incomplete_envelope() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &codex_body(&workspace));

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["driver"], "codex");
    assert_eq!(value["complete"], false);
    assert!(value["errors"][0].as_str().unwrap().contains("unsupported"));
}

#[test]
fn declaration_without_a_native_driver_has_no_read_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(
        &catalog,
        &host,
        "worker",
        &format!(
            "  workspace {:?}\n  deliver \"mcp\"\n  argv \"claude\" \"boot\"",
            workspace.display().to_string()
        ),
    );

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["driver"].is_null());
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("no native driver")
    );
}

#[test]
fn claude_driver_without_a_workspace_cannot_select_files() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let host = st2::detect_host();
    write_agent(
        &catalog,
        &host,
        "worker",
        "  claude {\n    prompt \"boot\"\n  }",
    );

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["driver"], "claude");
    assert!(value["workspace"].is_null());
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("no declared workspace")
    );
}

#[test]
fn a_remote_agent_cannot_authorize_local_file_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let local_host = st2::detect_host();
    let remote_host = format!("remote-{local_host}");
    write_agent(&catalog, &remote_host, "worker", &claude_body(&workspace));

    let output = sessions(&catalog, &home, &format!("{remote_host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["errors"][0].as_str().unwrap().contains("nonlocal"));
}

#[test]
fn missing_provider_directory_is_incomplete_not_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &claude_body(&workspace));

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["sessions"].as_array().unwrap().is_empty());
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("inspect Claude session directory")
    );
}

#[test]
fn malformed_jsonl_returns_no_false_complete_signal() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &claude_body(&workspace));
    let project = project_dir(&home, &workspace);
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("broken.jsonl"), "{not-json}\n").unwrap();

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["errors"][0].as_str().unwrap().contains("valid JSON"));
}

#[test]
fn symlinked_jsonl_is_an_unsafe_file_type() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let home = tmp.path().join("home");
    let workspace = tmp.path().join("work");
    let host = st2::detect_host();
    write_agent(&catalog, &host, "worker", &claude_body(&workspace));
    let project = project_dir(&home, &workspace);
    fs::create_dir_all(&project).unwrap();
    let outside = tmp.path().join("outside.jsonl");
    fs::write(&outside, "{\"type\":\"user\"}\n").unwrap();
    symlink(&outside, project.join("session.jsonl")).unwrap();

    let output = sessions(&catalog, &home, &format!("{host}.worker"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("real regular file")
    );
}

#[test]
fn json_is_required_in_v1() {
    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["harness", "sessions", "--identity", "host.worker"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires --json"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
