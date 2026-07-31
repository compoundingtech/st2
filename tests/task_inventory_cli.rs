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
    let fake = bin.join("pty");
    fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\n",
            pty_json.replace('\'', "'\"'\"'")
        ),
    )
    .unwrap();
    let mut mode = fs::metadata(&fake).unwrap().permissions();
    mode.set_mode(0o755);
    fs::set_permissions(&fake, mode).unwrap();
    (tmp, catalog, bin)
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

fn tasks_selected(catalog: &Path, bin: &Path, state: &Path, desired_state: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "tasks",
            "--host",
            "h",
            "--desired-state",
            desired_state,
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
        assert!(
            std::time::Instant::now() < deadline,
            "PTY {id:?} never became removable: {}",
            String::from_utf8_lossy(&removed.stderr)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(target_os = "linux")]
fn wait_for_catalog_lock_block(child: &mut std::process::Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let wait_channel =
            fs::read_to_string(format!("/proc/{}/wchan", child.id())).unwrap_or_default();
        if wait_channel.contains("locks_lock_inode_wait") {
            return;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "inventory exited before waiting on the catalog lock"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "inventory never reached the catalog lock wait; wchan={wait_channel:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(target_os = "macos")]
fn wait_for_catalog_lock_block(child: &mut std::process::Child) {
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        child.try_wait().unwrap().is_none(),
        "inventory bypassed the catalog shared lock"
    );
}

#[test]
fn tasks_cli_emits_stable_complete_generation_without_doctor_prose() {
    let (tmp, catalog, bin) = fixture(
        r#"[{"name":"h.worker","status":"running","pid":77,"createdAt":"2026-07-31T10:00:00.000Z"},{"name":"human.scratch","status":"running","pid":88,"createdAt":"2026-07-31T10:00:01.000Z"}]"#,
    );
    let first = tasks(&catalog, &bin, &tmp.path().join("state"));
    let second = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(first.stdout, second.stdout, "unchanged generation drifted");
    let value: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(value["schema"], "st2.task-inventory.v1");
    assert_eq!(
        value["selection"]["desiredStates"],
        serde_json::json!(["running", "absent"])
    );
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
}

#[test]
fn desired_state_is_closed_and_running_scope_excludes_retired_before_observation() {
    let (tmp, catalog, bin) = fixture(
        r#"[{"name":"h.worker","status":"running","pid":77,"createdAt":"2026-07-31T10:00:00.000Z"}]"#,
    );
    fs::create_dir_all(catalog.join("agents/h/retired")).unwrap();
    fs::write(
        catalog.join("agents/h/retired/agent.kdl"),
        r#"
agent "retired" {
  host "h"
  retired #true
  pty "agent" {
    id "h.retired"
  }
}
"#,
    )
    .unwrap();
    let running = tasks_selected(&catalog, &bin, &tmp.path().join("state"), "running");
    assert!(
        running.status.success(),
        "{}",
        String::from_utf8_lossy(&running.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&running.stdout).unwrap();
    assert_eq!(
        value["selection"]["desiredStates"],
        serde_json::json!(["running"])
    );
    assert_eq!(value["complete"], true);
    assert_eq!(value["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(value["tasks"][0]["runtimeId"], "h.worker");

    let invalid = tasks_selected(&catalog, &bin, &tmp.path().join("state"), "stopped");
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("possible values: running, absent"));
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
    let first = real_tasks(&catalog, &tmp.path().join("state"));
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
    let second = real_tasks(&catalog, &tmp.path().join("state"));
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    let second_generation = second_json["tasks"][0]["runtime"]["generationId"]
        .as_str()
        .unwrap();
    assert_ne!(second_generation, first_generation);
}

#[test]
fn incomplete_generation_is_json_nonzero_and_never_absent() {
    let (tmp, catalog, bin) = fixture(r#"[{"name":"h.worker","status":"running","pid":77}]"#);
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
    assert_ne!(value["tasks"][0]["runtime"]["state"], "absent");
}

#[test]
fn malformed_pty_generation_is_indeterminate() {
    let (tmp, catalog, bin) =
        fixture(r#"[{"name":"h.worker","status":"running","pid":0,"createdAt":"not-a-time"}]"#);
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
    assert!(value["tasks"][0]["runtime"]["generationId"].is_null());
}

#[test]
fn malformed_pty_json_is_typed_incomplete() {
    let (tmp, catalog, bin) = fixture("not-json");
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["errors"][0].as_str().unwrap().contains("parsing"));
    assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
}

#[test]
fn failed_pty_command_is_typed_incomplete() {
    let (tmp, catalog, bin) = fixture("[]");
    fs::write(
        bin.join("pty"),
        "#!/bin/sh\nprintf 'fixture failure' >&2\nexit 7\n",
    )
    .unwrap();
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("fixture failure")
    );
    assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
}

#[test]
fn timed_out_pty_command_is_typed_incomplete() {
    let (tmp, catalog, bin) = fixture("[]");
    let sleep = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|dir| dir.join("sleep"))
        .find(|path| path.is_file())
        .expect("sleep executable on the test PATH");
    fs::write(
        bin.join("pty"),
        format!("#!/bin/sh\nexec {:?} 30\n", sleep.display().to_string()),
    )
    .unwrap();
    let started = std::time::Instant::now();
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "outer timeout did not bound the PTY probe"
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(
        value["errors"].as_array().unwrap().iter().any(|error| error
            .as_str()
            .is_some_and(|error| error.contains("timed out"))),
        "{}",
        value["errors"]
    );
    assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
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
    fs::write(bin.join("pty"), "#!/bin/sh\nexit 99\n").unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let state = tmp.path().join("state");
    let exec_state = state.join("st2/h/exec");
    fs::create_dir_all(&exec_state).unwrap();
    let pid_file = exec_state.join("h.worker.ding.pid");
    let legacy = std::process::id().to_string();
    fs::write(&pid_file, &legacy).unwrap();

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
    assert_eq!(
        fs::read_to_string(pid_file).unwrap(),
        legacy,
        "inventory must not migrate legacy state"
    );
}

#[test]
fn missing_pty_root_is_positive_absence_without_creation_or_pty_invocation() {
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
    fs::write(
        bin.join("pty"),
        format!(
            "#!/bin/sh\ntouch {:?}\nexit 99\n",
            tmp.path().join("PTY-WAS-CALLED").display().to_string()
        ),
    )
    .unwrap();
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["tasks"][0]["runtime"]["state"], "absent");
    assert!(!missing.exists());
    assert!(!tmp.path().join("PTY-WAS-CALLED").exists());
}

#[test]
fn incomplete_apply_marker_returns_typed_incomplete_envelope() {
    let (tmp, catalog, bin) = fixture("[]");
    fs::create_dir_all(catalog.join(".st2")).unwrap();
    fs::write(catalog.join(".st2/catalog-apply-incomplete"), "{}").unwrap();
    let output = tasks(&catalog, &bin, &tmp.path().join("state"));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["complete"], false);
    assert!(value["tasks"].as_array().unwrap().is_empty());
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("catalog apply is incomplete")
    );
}

#[test]
fn task_inventory_serializes_behind_catalog_exclusive_writer() {
    let (tmp, catalog, bin) = fixture("[]");
    let gate = st2::CatalogLock::exclusive(&catalog).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--json", "--catalog"])
        .arg(&catalog)
        .env("PATH", &bin)
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_catalog_lock_block(&mut child);
    assert!(
        child.try_wait().unwrap().is_none(),
        "inventory bypassed the catalog shared lock"
    );
    fs::write(
        catalog.join("agents/h/worker/agent.kdl"),
        r#"
agent "worker" {
  host "h"
  pty "replacement" {
    id "h.replacement"
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#,
    )
    .unwrap();
    drop(gate);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(value["tasks"][0]["task"], "replacement");
    assert_eq!(value["tasks"][0]["runtimeId"], "h.replacement");
}

#[test]
fn missing_catalog_still_returns_the_typed_incomplete_envelope() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing-catalog");
    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["tasks", "--host", "h", "--json", "--catalog"])
        .arg(&missing)
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema"], "st2.task-inventory.v1");
    assert_eq!(value["complete"], false);
    assert!(value["tasks"].as_array().unwrap().is_empty());
    assert!(
        value["errors"][0]
            .as_str()
            .unwrap()
            .contains("canonicalize")
    );
}
