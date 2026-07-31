use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

fn fixture(pty_json: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let pty_root = tmp.path().join("pty-root");
    let bin = tmp.path().join("bin");
    fs::create_dir_all(catalog.join("agents/h/worker")).unwrap();
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
        r#"
agent "worker" {
  host "h"
  pty "agent" {
    id "h.worker"
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#,
    )
    .unwrap();
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\n",
            pty_json.replace('\'', "'\"'\"'")
        ),
    );
    (tmp, catalog, bin)
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut mode = fs::metadata(path).unwrap().permissions();
    mode.set_mode(0o755);
    fs::set_permissions(path, mode).unwrap();
}

fn tasks(catalog: &Path, bin: &Path, state: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--json", "--catalog"])
        .arg(catalog)
        .env("PATH", bin)
        .env("XDG_STATE_HOME", state)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap()
}

fn real_tasks(catalog: &Path, state: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--json", "--catalog"])
        .arg(catalog)
        .env("XDG_STATE_HOME", state)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap()
}

#[test]
fn tasks_cli_emits_stable_complete_generation_without_mutation() {
    let (tmp, catalog, bin) = fixture(
        r#"[{"name":"h.worker","status":"running","pid":77,"createdAt":"2026-07-31T10:00:00.000Z"},{"name":"human.scratch","status":"running","pid":88,"createdAt":"2026-07-31T10:00:01.000Z"}]"#,
    );
    let agent = catalog.join("agents/h/worker/agent.kdl");
    let catalog_config = catalog.join("catalog.kdl");
    let before_agent = fs::read(&agent).unwrap();
    let before_catalog = fs::read(&catalog_config).unwrap();
    let state = tmp.path().join("state");
    let first = tasks(&catalog, &bin, &state);
    let second = tasks(&catalog, &bin, &state);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(first.stdout, second.stdout, "unchanged generation drifted");
    let value: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(value["schema"], "st2.task-inventory.v1");
    assert_eq!(value["complete"], true);
    assert_eq!(value["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(value["tasks"][0]["agent"], "h.worker");
    assert_eq!(value["tasks"][0]["task"], "agent");
    assert_eq!(value["tasks"][0]["runtimeId"], "h.worker");
    assert_eq!(value["tasks"][0]["runtime"]["pid"], 77);
    assert_eq!(
        value["tasks"][0]["runtime"]["createdAt"],
        "2026-07-31T10:00:00.000Z"
    );
    assert!(
        value["tasks"][0]["runtime"]["generationId"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(fs::read(agent).unwrap(), before_agent);
    assert_eq!(fs::read(catalog_config).unwrap(), before_catalog);
    assert!(!state.exists(), "read-only inventory created runtime state");
}

#[test]
fn incomplete_or_malformed_pty_generation_is_json_nonzero_and_never_absent() {
    for pty_json in [
        r#"[{"name":"h.worker","status":"running","pid":77}]"#,
        r#"[{"name":"h.worker","status":"running","pid":0,"createdAt":"not-a-time"}]"#,
        "not-json",
    ] {
        let (tmp, catalog, bin) = fixture(pty_json);
        let output = tasks(&catalog, &bin, &tmp.path().join("state"));
        assert!(!output.status.success());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["complete"], false);
        assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
        assert_ne!(value["tasks"][0]["runtime"]["state"], "absent");
    }
}

#[test]
fn failed_and_timed_out_pty_commands_are_typed_incomplete() {
    let (tmp, catalog, bin) = fixture("[]");
    write_executable(
        &bin.join("pty"),
        "#!/bin/sh\nprintf 'fixture failure' >&2\nexit 7\n",
    );
    let failed = tasks(&catalog, &bin, &tmp.path().join("failed-state"));
    assert!(!failed.status.success());
    let failed_json: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(failed_json["complete"], false);
    assert!(
        failed_json["errors"][0]
            .as_str()
            .unwrap()
            .contains("fixture failure")
    );

    let sleep = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|dir| dir.join("sleep"))
        .find(|path| path.is_file())
        .expect("sleep executable on the test PATH");
    write_executable(
        &bin.join("pty"),
        &format!("#!/bin/sh\nexec {:?} 30\n", sleep.display().to_string()),
    );
    let started = std::time::Instant::now();
    let timed_out = tasks(&catalog, &bin, &tmp.path().join("timeout-state"));
    assert!(!timed_out.status.success());
    assert!(started.elapsed() < Duration::from_secs(10));
    let timeout_json: serde_json::Value = serde_json::from_slice(&timed_out.stdout).unwrap();
    assert_eq!(timeout_json["complete"], false);
    assert!(
        timeout_json["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| {
                error
                    .as_str()
                    .is_some_and(|error| error.contains("timed out"))
            })
    );
}

#[test]
fn legacy_exec_generation_is_joined_read_only_without_calling_pty() {
    let (tmp, catalog, bin) = fixture("[]");
    fs::write(
        catalog.join("agents/h/worker/agent.kdl"),
        r#"
agent "worker" {
  host "h"
  exec "ding" {
    command "sleep 30"
  }
}
"#,
    )
    .unwrap();
    write_executable(&bin.join("pty"), "#!/bin/sh\nexit 99\n");
    std::thread::sleep(Duration::from_millis(20));
    let state = tmp.path().join("state");
    let exec_state = state.join("st2/h/exec");
    fs::create_dir_all(&exec_state).unwrap();
    let pid_file = exec_state.join("h.worker.ding.pid");
    let legacy = std::process::id().to_string();
    fs::write(&pid_file, &legacy).unwrap();
    let before_entries = fs::read_dir(&exec_state)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();

    let output = tasks(&catalog, &bin, &state);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], true);
    assert_eq!(value["tasks"][0]["kind"], "exec");
    assert_eq!(value["tasks"][0]["runtime"]["state"], "running");
    assert_eq!(
        value["tasks"][0]["runtime"]["pid"],
        u64::from(std::process::id())
    );
    assert_eq!(fs::read_to_string(&pid_file).unwrap(), legacy);
    let after_entries = fs::read_dir(&exec_state)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        after_entries, before_entries,
        "inventory changed exec state"
    );
}

#[test]
fn missing_pty_root_is_positive_absence_without_creation_or_invocation() {
    let (tmp, catalog, bin) = fixture("[]");
    let missing = tmp.path().join("missing-root");
    fs::write(
        catalog.join("catalog.kdl"),
        format!(
            "catalog {{ pty-root {:?} }}\n",
            missing.display().to_string()
        ),
    )
    .unwrap();
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\ntouch {:?}\nexit 99\n",
            tmp.path().join("PTY-WAS-CALLED").display().to_string()
        ),
    );
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["tasks"][0]["runtime"]["state"], "absent");
    assert!(!missing.exists());
    assert!(!tmp.path().join("PTY-WAS-CALLED").exists());
}

#[test]
fn catalog_semantic_drift_during_observation_fails_closed() {
    let (tmp, catalog, bin) = fixture("[]");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    let replacement = r#"agent "worker" {
  host "h"
  pty "replacement" { id "h.replacement"; argv "agent-bin" }
}
"#;
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nprintf '%s' '{}' > {:?}\nprintf '%s\\n' '[]'\n",
            replacement.replace('\'', "'\"'\"'"),
            declaration.display().to_string()
        ),
    );
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["errors"].as_array().unwrap().iter().any(|error| {
        error
            .as_str()
            .is_some_and(|error| error.contains("changed during task observation"))
    }));
}

#[test]
fn missing_catalog_and_missing_json_flag_fail_explicitly() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing-catalog");
    let missing_output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--json", "--catalog"])
        .arg(&missing)
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap();
    assert!(!missing_output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&missing_output.stdout).unwrap();
    assert_eq!(value["schema"], "st2.task-inventory.v1");
    assert_eq!(value["complete"], false);
    assert!(value["tasks"].as_array().unwrap().is_empty());

    let (fixture_tmp, catalog, bin) = fixture("[]");
    let no_json = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--catalog"])
        .arg(&catalog)
        .env("PATH", bin)
        .env("XDG_STATE_HOME", fixture_tmp.path().join("state"))
        .output()
        .unwrap();
    assert!(!no_json.status.success());
    assert!(String::from_utf8_lossy(&no_json.stderr).contains("requires --json"));
}

struct RealPtyCleanup {
    root: PathBuf,
    id: &'static str,
}

impl Drop for RealPtyCleanup {
    fn drop(&mut self) {
        for verb in ["kill", "rm"] {
            let _ = Command::new("pty")
                .args([verb, self.id])
                .env("PTY_ROOT", &self.root)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            std::thread::sleep(Duration::from_millis(300));
        }
    }
}

fn remove_real_pty(root: &Path, id: &str) {
    let killed = Command::new("pty")
        .args(["kill", id])
        .env("PTY_ROOT", root)
        .output()
        .unwrap();
    assert!(
        killed.status.success(),
        "{}",
        String::from_utf8_lossy(&killed.stderr)
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let removed = Command::new("pty")
            .args(["rm", id])
            .env("PTY_ROOT", root)
            .output()
            .unwrap();
        if removed.status.success() {
            return;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn packaged_tasks_tracks_real_pty_generation_replacement() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let pty_root = tmp.path().join("pty");
    fs::create_dir_all(catalog.join("agents/h/worker")).unwrap();
    fs::create_dir(&pty_root).unwrap();
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
        r#"agent "worker" {
  host "h"
  pty "agent" { id "h.worker"; argv "agent-bin" }
}
"#,
    )
    .unwrap();
    let _cleanup = RealPtyCleanup {
        root: pty_root.clone(),
        id: "h.worker",
    };
    let launch = || {
        Command::new("pty")
            .args(["run", "-d", "--id", "h.worker", "--", "sleep", "120"])
            .env("PTY_ROOT", &pty_root)
            .output()
            .unwrap()
    };
    let first_launch = launch();
    assert!(
        first_launch.status.success(),
        "{}",
        String::from_utf8_lossy(&first_launch.stderr)
    );
    let state = tmp.path().join("state");
    let first = real_tasks(&catalog, &state);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    let first_generation = first_json["tasks"][0]["runtime"]["generationId"]
        .as_str()
        .unwrap()
        .to_owned();

    remove_real_pty(&pty_root, "h.worker");
    let second_launch = launch();
    assert!(
        second_launch.status.success(),
        "{}",
        String::from_utf8_lossy(&second_launch.stderr)
    );
    let second = real_tasks(&catalog, &state);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_ne!(
        second_json["tasks"][0]["runtime"]["generationId"]
            .as_str()
            .unwrap(),
        first_generation
    );
}
