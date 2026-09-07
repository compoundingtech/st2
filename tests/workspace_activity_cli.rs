use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn fixture(pty_json: &str) -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let pty_root = tmp.path().join("pty-root");
    let bin = tmp.path().join("bin");
    fs::create_dir_all(catalog.join("agents/h/worker")).unwrap();
    fs::create_dir(&workspace).unwrap();
    fs::create_dir(&pty_root).unwrap();
    fs::create_dir(&bin).unwrap();
    fs::write(
        catalog.join("catalog.kdl"),
        format!(
            "catalog {{ pty-root {:?} }}\n",
            pty_root.display().to_string()
        ),
    )
    .unwrap();
    fs::write(
        catalog.join("agents/h/worker/agent.kdl"),
        format!(
            "agent \"worker\" {{\n host \"h\"\n workspace {:?}\n pty \"agent\" {{ id \"h.worker\"; argv \"agent-bin\" }}\n}}\n",
            workspace.display().to_string()
        ),
    )
    .unwrap();
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\n",
            pty_json.replace('\'', "'\"'\"'")
        ),
    );
    (tmp, catalog, workspace, bin)
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn snapshot_with_ttl(catalog: &Path, bin: &Path, state: &Path, ttl: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "workspace-activity",
            "--host",
            "h",
            "--ttl",
            ttl,
            "--json",
            "--catalog",
        ])
        .arg(catalog)
        .env("PATH", bin)
        .env("XDG_STATE_HOME", state)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap()
}

fn snapshot(catalog: &Path, bin: &Path, state: &Path) -> Output {
    snapshot_with_ttl(catalog, bin, state, "30")
}

#[test]
fn reports_sorted_active_workspace_claim_without_mutating_state() {
    let (tmp, catalog, workspace, bin) = fixture(
        r#"[{"name":"h.worker","status":"running","pid":77,"createdAt":"2026-08-13T10:00:00.000Z"}]"#,
    );
    let output = snapshot(&catalog, &bin, &tmp.path().join("state"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schemaVersion"], "st2.workspace-activity.v1");
    assert_eq!(value["producer"], "st2");
    assert_eq!(value["complete"], true);
    assert_eq!(
        value["claims"][0]["workspace"],
        workspace.display().to_string()
    );
    assert_eq!(
        value["claims"][0]["agents"],
        serde_json::json!(["h.worker"])
    );
    assert_eq!(
        value["claims"][0]["activeRuntimeIds"],
        serde_json::json!(["h.worker"])
    );
    assert_eq!(value["claims"][0]["active"], true);
    assert!(value["capturedAt"].as_str().unwrap().ends_with('Z'));
    assert!(value["expiresAt"].as_str().unwrap().ends_with('Z'));
    assert!(!tmp.path().join("state").exists());
}

#[test]
fn runtime_observer_failure_prints_incomplete_envelope_and_fails() {
    let (tmp, catalog, _workspace, bin) = fixture("not-json");
    let output = snapshot(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(!value["errors"].as_array().unwrap().is_empty());
}

#[test]
fn accepts_ttl_boundaries() {
    let (tmp, catalog, _workspace, bin) = fixture("[]");
    for ttl in ["1", "300"] {
        let output = snapshot_with_ttl(&catalog, &bin, &tmp.path().join("state"), ttl);
        assert!(
            output.status.success(),
            "TTL {ttl} was unexpectedly rejected"
        );
    }
}

#[test]
fn rejects_zero_and_overlong_ttls_with_an_incomplete_envelope() {
    let (tmp, catalog, _workspace, bin) = fixture("[]");
    for ttl in ["0", "301"] {
        let output = snapshot_with_ttl(&catalog, &bin, &tmp.path().join("state"), ttl);
        assert!(!output.status.success(), "TTL {ttl} unexpectedly succeeded");
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["complete"], false);
        assert_eq!(value["expiresAt"], value["capturedAt"]);
        assert!(
            value["errors"][0]
                .as_str()
                .unwrap()
                .contains("TTL must be between")
        );
    }
}

#[test]
fn catalog_discovery_failure_prints_incomplete_envelope_and_fails() {
    let (tmp, catalog, _workspace, bin) = fixture("[]");
    fs::write(
        catalog.join("agents/h/worker/agent.kdl"),
        "not valid kdl {{",
    )
    .unwrap();

    let output = snapshot(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("catalog file")
    );
}

#[test]
fn duplicate_runtime_ids_print_incomplete_envelope_and_fail() {
    let (tmp, catalog, workspace, bin) = fixture("[]");
    let duplicate_dir = catalog.join("agents/h/duplicate");
    fs::create_dir_all(&duplicate_dir).unwrap();
    fs::write(
        duplicate_dir.join("agent.kdl"),
        format!(
            "agent \"duplicate\" {{\n host \"h\"\n workspace {:?}\n pty \"agent\" {{ id \"h.worker\"; argv \"agent-bin\" }}\n}}\n",
            workspace.display().to_string()
        ),
    )
    .unwrap();

    let output = snapshot(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error.as_str().unwrap().contains("duplicate runtime id"))
    );
}

#[test]
fn relative_workspace_is_resolved_from_the_declaring_spec_directory() {
    let (tmp, catalog, workspace, bin) = fixture("[]");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    let relative_workspace = declaration.parent().unwrap().join("relative-workspace");
    fs::create_dir(&relative_workspace).unwrap();
    let contents = fs::read_to_string(&declaration).unwrap().replace(
        &format!("workspace {:?}", workspace.display().to_string()),
        "workspace \"relative-workspace\"",
    );
    fs::write(declaration, contents).unwrap();

    let output = snapshot(&catalog, &bin, &tmp.path().join("state"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value["claims"][0]["workspace"],
        relative_workspace.display().to_string()
    );
}
