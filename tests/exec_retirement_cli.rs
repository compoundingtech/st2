use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Mutex;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

static LIVE_E2E: Mutex<()> = Mutex::new(());

const LEGACY_PARTITION_HASH_DOMAIN: &[u8] = b"st2.exec-retirement-legacy-partition.v1\0";

fn legacy_partition_sha256(partition: &Value) -> String {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TypedLegacyTask<'a> {
        runtime_id: &'a str,
        agent: &'a str,
        task: &'a str,
        desired_state: &'a str,
    }

    let typed = partition
        .as_array()
        .unwrap()
        .iter()
        .map(|task| TypedLegacyTask {
            runtime_id: task["runtimeId"].as_str().unwrap(),
            agent: task["agent"].as_str().unwrap(),
            task: task["task"].as_str().unwrap(),
            desired_state: task["desiredState"].as_str().unwrap(),
        })
        .collect::<Vec<_>>();
    let mut hash = Sha256::new();
    hash.update(LEGACY_PARTITION_HASH_DOMAIN);
    let mut canonical = serde_json::to_vec(&typed).unwrap();
    canonical.push(b'\n');
    hash.update(canonical);
    format!("{:x}", hash.finalize())
}

fn st2() -> Command {
    Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn fixture(root: &Path) -> (PathBuf, PathBuf, String) {
    let catalog = root.join("catalog");
    let xdg = root.join("xdg");
    fs::create_dir_all(&catalog).unwrap();
    fs::create_dir_all(&xdg).unwrap();
    fs::write(
        catalog.join("catalog.kdl"),
        format!(
            "catalog {{ pty-root \"{}\" }}\n",
            root.join("pty").display()
        ),
    )
    .unwrap();
    let snapshot = root.join("snapshot");
    let output = st2()
        .args(["--catalog"])
        .arg(&catalog)
        .args(["catalog", "snapshot", "--output"])
        .arg(&snapshot)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let root_sha = serde_json::from_slice::<Value>(&output.stdout).unwrap()["rootSha256"]
        .as_str()
        .unwrap()
        .to_string();
    (catalog, xdg, root_sha)
}

fn catalog_sha(root: &Path, catalog: &Path, name: &str) -> String {
    let snapshot = root.join(name);
    let output = st2()
        .args(["--catalog"])
        .arg(catalog)
        .args(["catalog", "snapshot", "--output"])
        .arg(snapshot)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice::<Value>(&output.stdout).unwrap()["rootSha256"]
        .as_str()
        .unwrap()
        .to_string()
}

fn declare_canonical_ding(catalog: &Path, identity: &str, retired: bool) {
    let spec = catalog.join(format!("agents/testhost/{identity}/agent.kdl"));
    fs::create_dir_all(spec.parent().unwrap()).unwrap();
    fs::write(
        spec,
        format!(
            "agent \"{identity}\" {{\n  host \"testhost\"\n  type \"service\"\n  retired \
             #{retired}\n  command \"true\"\n  ding\n}}\n"
        ),
    )
    .unwrap();
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn prepend_path(bin: &Path) -> String {
    format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

fn pid_alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

struct LiveCleanup {
    catalog: PathBuf,
    xdg: PathBuf,
    path: String,
    sentinel: Child,
}

impl Drop for LiveCleanup {
    fn drop(&mut self) {
        let _ = st2()
            .arg("--catalog")
            .arg(&self.catalog)
            .args(["down", "--host", "testhost"])
            .env("XDG_STATE_HOME", &self.xdg)
            .env("PATH", &self.path)
            .output();
        let _ = self.sentinel.kill();
        let _ = self.sentinel.wait();
    }
}

#[test]
fn legacy_set_stale_prepare_apply_and_replay_are_exact() {
    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _) = fixture(temp.path());
    declare_canonical_ding(&catalog, "alpha", false);
    declare_canonical_ding(&catalog, "beta", true);
    let root_sha = catalog_sha(temp.path(), &catalog, "partition-snapshot");
    let state = xdg.join("st2/testhost/exec");
    fs::create_dir_all(&state).unwrap();
    let first = state.join("testhost.alpha.ding.pid");
    let second = state.join("testhost.beta.ding.pid");
    fs::write(&first, "2000000000\n").unwrap();
    fs::write(&second, "1999999999\n").unwrap();
    let plan = temp.path().join("retirement-plan.json");

    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--legacy-set",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        prepare.status.success(),
        "{}",
        String::from_utf8_lossy(&prepare.stderr)
    );
    let prepared: Value = serde_json::from_slice(&prepare.stdout).unwrap();
    let plan_sha = prepared["planSha256"].as_str().unwrap();
    let plan_json: Value = serde_json::from_slice(&fs::read(&plan).unwrap()).unwrap();
    let partition_sha = legacy_partition_sha256(&plan_json["legacyPartition"]);
    assert_eq!(prepared["targets"], 2);
    assert_eq!(prepared["legacyPartitionSha256"], partition_sha);
    assert!(plan.is_file());
    assert!(
        first.is_file() && second.is_file(),
        "prepare is runtime-read-only"
    );

    let apply_args = [
        "exec",
        "retirement",
        "apply",
        "--plan",
        plan.to_str().unwrap(),
        "--expect-plan-sha256",
        plan_sha,
        "--json",
    ];
    let applied = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let receipt: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(receipt["schema"], "st2.exec-retirement.v1");
    assert_eq!(receipt["forwardOnlyStarted"], true);
    assert_eq!(receipt["legacyPartitionSha256"], partition_sha);
    assert_eq!(
        receipt["legacyPartition"][0]["desiredState"],
        "running-ding"
    );
    assert_eq!(
        receipt["legacyPartition"][1]["desiredState"],
        "absent-retired"
    );
    assert_eq!(receipt["targets"].as_array().unwrap().len(), 2);
    assert!(
        receipt["targets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|target| target["disposition"] == "stale-record-only")
    );
    assert!(!first.exists() && !second.exists());

    let replay = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(replay.status.success());
    assert_eq!(
        replay.stdout, applied.stdout,
        "replay returns the exact stored receipt"
    );

    let transaction = fs::read_dir(state.join(".retirements"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let receipt_path = transaction.join("receipt.json");
    let mut tampered: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    tampered["legacyPartitionSha256"] = Value::String("0".repeat(64));
    fs::write(&receipt_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    let rejected = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr)
            .contains("stored retirement receipt does not bind the exact completed transaction")
    );
}

#[test]
fn legacy_set_rejects_a_foreign_strict_record_without_a_plan() {
    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, root_sha) = fixture(temp.path());
    let state = xdg.join("st2/testhost/exec");
    fs::create_dir_all(&state).unwrap();
    fs::write(state.join("testhost.legacy.ding.pid"), "2000000000\n").unwrap();
    fs::write(
        state.join("testhost.foreign.ding.pid"),
        "{\"schema\":\"foreign\"}\n",
    )
    .unwrap();
    let plan = temp.path().join("rejected-plan.json");

    let output = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--legacy-set",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["schema"], "st2.exec-retirement-error.v1");
    assert!(!plan.exists());
    assert!(state.join("testhost.legacy.ding.pid").exists());
    assert!(state.join("testhost.foreign.ding.pid").exists());
}

#[test]
fn crash_after_record_rename_resumes_from_the_exact_private_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _) = fixture(temp.path());
    declare_canonical_ding(&catalog, "crash", false);
    let root_sha = catalog_sha(temp.path(), &catalog, "crash-snapshot");
    let state = xdg.join("st2/testhost/exec");
    fs::create_dir_all(&state).unwrap();
    let record = state.join("testhost.crash.ding.pid");
    fs::write(&record, "2000000000\n").unwrap();
    let before = fs::metadata(&record).unwrap();
    let plan = temp.path().join("crash-plan.json");
    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--legacy-set",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(prepare.status.success());
    let prepared: Value = serde_json::from_slice(&prepare.stdout).unwrap();
    let plan_sha = prepared["planSha256"].as_str().unwrap();
    let request_sha =
        serde_json::from_slice::<Value>(&fs::read(&plan).unwrap()).unwrap()["requestSha256"]
            .as_str()
            .unwrap()
            .to_string();
    let apply_args = [
        "exec",
        "retirement",
        "apply",
        "--plan",
        plan.to_str().unwrap(),
        "--expect-plan-sha256",
        plan_sha,
        "--json",
    ];
    let crashed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .env("ST2_TEST_EXEC_RETIREMENT_CRASH_AT", "after-record-rename")
        .output()
        .unwrap();
    assert!(
        !crashed.status.success(),
        "failpoint did not terminate apply"
    );
    let transaction = state.join(".retirements").join(request_sha);
    let slot = transaction.join("records").join("testhost.crash.ding.pid");
    assert!(!record.exists());
    assert_eq!(fs::read(&slot).unwrap(), b"2000000000\n");
    assert_eq!(
        std::os::unix::fs::MetadataExt::ino(&fs::metadata(&slot).unwrap()),
        std::os::unix::fs::MetadataExt::ino(&before)
    );
    let journal: Value =
        serde_json::from_slice(&fs::read(transaction.join("journal.json")).unwrap()).unwrap();
    assert_eq!(journal["items"]["testhost.crash.ding"]["phase"], "prepared");
    assert_eq!(journal["forwardOnlyStarted"], true);

    let resumed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let receipt: Value = serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(receipt["targets"][0]["recordAfter"]["inode"], before.ino());
}

#[cfg(target_os = "linux")]
#[test]
fn strict_v2_live_retirement_drains_only_its_exact_scope() {
    let _live_guard = LIVE_E2E.lock().unwrap();
    if std::env::var_os("XDG_RUNTIME_DIR").is_none()
        || !Command::new("systemd-run")
            .args(["--user", "--version"])
            .output()
            .is_ok_and(|output| output.status.success())
    {
        panic!("live exact-retirement E2E requires a systemd user manager");
    }

    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _root_sha) = fixture(temp.path());
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("pty"),
        "#!/bin/sh\n[ \"$1\" = list ] && printf '[]\\n' && exit 0\nexit 97\n",
    );
    let path = prepend_path(&bin);
    let agent = catalog.join("agents/testhost/retirer/agent.kdl");
    fs::create_dir_all(agent.parent().unwrap()).unwrap();
    fs::write(
        &agent,
        r#"
agent "retirer" {
  host "testhost"
  type "service"
  exec "ding" {
    id "testhost.retirer.ding"
    command "sleep 120 & wait"
  }
}
"#,
    )
    .unwrap();

    // The declaration changed after fixture() took its initial digest.
    let root_sha = catalog_sha(temp.path(), &catalog, "live-snapshot");

    let sentinel = Command::new("sleep").arg("120").spawn().unwrap();
    let sentinel_pid = sentinel.id() as i32;
    let _cleanup = LiveCleanup {
        catalog: catalog.clone(),
        xdg: xdg.clone(),
        path: path.clone(),
        sentinel,
    };
    let boot = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(["up", "--host", "testhost", "--once"])
        .env("XDG_STATE_HOME", &xdg)
        .env("PATH", &path)
        .env_remove("PTY_ROOT")
        .env_remove("PTY_SESSION_DIR")
        .output()
        .unwrap();
    assert!(
        boot.status.success(),
        "{}",
        String::from_utf8_lossy(&boot.stderr)
    );
    let record = xdg.join("st2/testhost/exec/testhost.retirer.ding.pid");
    let generation: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    assert_eq!(generation["schema"], "st2.exec-generation.v2");
    let leader = generation["pid"].as_i64().unwrap() as i32;

    let plan = temp.path().join("live-retirement-plan.json");
    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--id",
            "testhost.retirer.ding",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        prepare.status.success(),
        "{}",
        String::from_utf8_lossy(&prepare.stderr)
    );
    let plan_sha = serde_json::from_slice::<Value>(&prepare.stdout).unwrap()["planSha256"]
        .as_str()
        .unwrap()
        .to_string();

    let apply = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "apply",
            "--plan",
            plan.to_str().unwrap(),
            "--expect-plan-sha256",
            &plan_sha,
            "--json",
        ])
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        apply.status.success(),
        "{}",
        String::from_utf8_lossy(&apply.stderr)
    );
    let receipt: Value = serde_json::from_slice(&apply.stdout).unwrap();
    assert_eq!(receipt["targets"][0]["disposition"], "cgroup-retired");
    assert!(
        receipt["targets"][0]["membership"]
            .as_array()
            .is_some_and(|members| members.len() >= 2),
        "{}",
        receipt
    );
    assert!(!pid_alive(leader), "exact leader survived cgroup.kill");
    assert!(pid_alive(sentinel_pid), "unrelated sentinel was signalled");
    assert!(!record.exists(), "generation record was not retired");
}

#[cfg(target_os = "linux")]
#[test]
fn crash_after_cgroup_freeze_resumes_forward_without_touching_a_sentinel() {
    let _live_guard = LIVE_E2E.lock().unwrap();
    if std::env::var_os("XDG_RUNTIME_DIR").is_none()
        || !Command::new("systemd-run")
            .args(["--user", "--version"])
            .output()
            .is_ok_and(|output| output.status.success())
    {
        panic!("live exact-retirement E2E requires a systemd user manager");
    }
    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _) = fixture(temp.path());
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("pty"),
        "#!/bin/sh\n[ \"$1\" = list ] && printf '[]\\n' && exit 0\nexit 97\n",
    );
    let path = prepend_path(&bin);
    let agent = catalog.join("agents/testhost/freeze/agent.kdl");
    fs::create_dir_all(agent.parent().unwrap()).unwrap();
    fs::write(
        &agent,
        r#"
agent "freeze" {
  host "testhost"
  type "service"
  exec "ding" {
    id "testhost.freeze.ding"
    command "sleep 120"
  }
}
"#,
    )
    .unwrap();
    let root_sha = catalog_sha(temp.path(), &catalog, "freeze-snapshot");
    let sentinel = Command::new("sleep").arg("120").spawn().unwrap();
    let sentinel_pid = sentinel.id() as i32;
    let _cleanup = LiveCleanup {
        catalog: catalog.clone(),
        xdg: xdg.clone(),
        path: path.clone(),
        sentinel,
    };
    let boot = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(["up", "--host", "testhost", "--once"])
        .env("XDG_STATE_HOME", &xdg)
        .env("PATH", &path)
        .env_remove("PTY_ROOT")
        .env_remove("PTY_SESSION_DIR")
        .output()
        .unwrap();
    assert!(boot.status.success());
    let record = xdg.join("st2/testhost/exec/testhost.freeze.ding.pid");
    let generation: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    let leader = generation["pid"].as_i64().unwrap() as i32;
    let cgroup_path = generation["isolation"]["cgroupPath"].as_str().unwrap();
    let plan = temp.path().join("freeze-plan.json");
    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--id",
            "testhost.freeze.ding",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(prepare.status.success());
    let plan_sha = serde_json::from_slice::<Value>(&prepare.stdout).unwrap()["planSha256"]
        .as_str()
        .unwrap()
        .to_string();
    let apply_args = [
        "exec",
        "retirement",
        "apply",
        "--plan",
        plan.to_str().unwrap(),
        "--expect-plan-sha256",
        &plan_sha,
        "--json",
    ];
    let crashed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .env("ST2_TEST_EXEC_RETIREMENT_CRASH_AT", "after-cgroup-freeze")
        .output()
        .unwrap();
    assert!(!crashed.status.success());
    let events = fs::read_to_string(
        Path::new("/sys/fs/cgroup")
            .join(cgroup_path.trim_start_matches('/'))
            .join("cgroup.events"),
    )
    .unwrap();
    assert!(events.lines().any(|line| line == "frozen 1"));
    assert!(record.exists());
    assert!(pid_alive(leader));
    assert!(pid_alive(sentinel_pid));

    let resumed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let receipt: Value = serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(receipt["targets"][0]["freezeObserved"], true);
    assert!(!pid_alive(leader));
    assert!(pid_alive(sentinel_pid));
}

#[cfg(target_os = "linux")]
#[test]
fn frozen_leaderless_resume_uses_only_durable_membership() {
    let _live_guard = LIVE_E2E.lock().unwrap();
    if std::env::var_os("XDG_RUNTIME_DIR").is_none()
        || !Command::new("systemd-run")
            .args(["--user", "--version"])
            .output()
            .is_ok_and(|output| output.status.success())
    {
        panic!("live exact-retirement E2E requires a systemd user manager");
    }
    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _) = fixture(temp.path());
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("pty"),
        "#!/bin/sh\n[ \"$1\" = list ] && printf '[]\\n' && exit 0\nexit 97\n",
    );
    let path = prepend_path(&bin);
    let agent = catalog.join("agents/testhost/leaderless/agent.kdl");
    fs::create_dir_all(agent.parent().unwrap()).unwrap();
    fs::write(
        &agent,
        r#"
agent "leaderless" {
  host "testhost"
  type "service"
  exec "ding" {
    id "testhost.leaderless.ding"
    command "sleep 120 & wait"
  }
}
"#,
    )
    .unwrap();
    let root_sha = catalog_sha(temp.path(), &catalog, "leaderless-snapshot");
    let sentinel = Command::new("sleep").arg("120").spawn().unwrap();
    let sentinel_pid = sentinel.id() as i32;
    let _cleanup = LiveCleanup {
        catalog: catalog.clone(),
        xdg: xdg.clone(),
        path: path.clone(),
        sentinel,
    };
    let boot = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(["up", "--host", "testhost", "--once"])
        .env("XDG_STATE_HOME", &xdg)
        .env("PATH", &path)
        .env_remove("PTY_ROOT")
        .env_remove("PTY_SESSION_DIR")
        .output()
        .unwrap();
    assert!(boot.status.success());
    let record = xdg.join("st2/testhost/exec/testhost.leaderless.ding.pid");
    let generation: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    let leader = generation["pid"].as_i64().unwrap() as i32;
    let plan = temp.path().join("leaderless-plan.json");
    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--id",
            "testhost.leaderless.ding",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(prepare.status.success());
    let plan_sha = serde_json::from_slice::<Value>(&prepare.stdout).unwrap()["planSha256"]
        .as_str()
        .unwrap()
        .to_string();
    let apply_args = [
        "exec",
        "retirement",
        "apply",
        "--plan",
        plan.to_str().unwrap(),
        "--expect-plan-sha256",
        &plan_sha,
        "--json",
    ];
    let crashed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .env(
            "ST2_TEST_EXEC_RETIREMENT_CRASH_AT",
            "after-membership-journal",
        )
        .output()
        .unwrap();
    assert!(!crashed.status.success());
    let plan_json: Value = serde_json::from_slice(&fs::read(&plan).unwrap()).unwrap();
    let transaction = xdg
        .join("st2/testhost/exec/.retirements")
        .join(plan_json["requestSha256"].as_str().unwrap());
    let journal: Value =
        serde_json::from_slice(&fs::read(transaction.join("journal.json")).unwrap()).unwrap();
    let durable_members = journal["items"]["testhost.leaderless.ding"]["membership"]
        .as_array()
        .unwrap()
        .clone();
    assert!(durable_members.len() >= 2);
    assert_eq!(
        journal["items"]["testhost.leaderless.ding"]["phase"],
        "frozen"
    );
    unsafe {
        libc::kill(leader, libc::SIGKILL);
    }
    for _ in 0..100 {
        if !pid_alive(leader) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(!pid_alive(leader), "frozen leader did not accept SIGKILL");

    let resumed = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(apply_args)
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let receipt: Value = serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(
        receipt["targets"][0]["membership"].as_array().unwrap(),
        durable_members.as_slice()
    );
    assert!(pid_alive(sentinel_pid));
}

#[cfg(target_os = "linux")]
#[test]
fn legacy_numeric_live_ding_uses_scope_authority_without_a_generation_id() {
    let _live_guard = LIVE_E2E.lock().unwrap();
    if std::env::var_os("XDG_RUNTIME_DIR").is_none()
        || !Command::new("systemd-run")
            .args(["--user", "--version"])
            .output()
            .is_ok_and(|output| output.status.success())
    {
        panic!("live legacy-retirement E2E requires a systemd user manager");
    }

    let temp = tempfile::tempdir().unwrap();
    let (catalog, xdg, _) = fixture(temp.path());
    declare_canonical_ding(&catalog, "legacy", false);
    let root_sha = catalog_sha(temp.path(), &catalog, "legacy-live-snapshot");
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("pty"),
        "#!/bin/sh\ncase \"$1\" in\n  --help) exit 0 ;;\n  list) printf '[]\\n' ;;\n  run) exit 0 ;;\n  *) exit 97 ;;\nesac\n",
    );
    symlink(env!("CARGO_BIN_EXE_st2"), bin.join("st2")).unwrap();
    let path = prepend_path(&bin);
    let pty_root = temp.path().join("pty");
    fs::create_dir_all(&pty_root).unwrap();
    fs::write(
        pty_root.join("testhost.legacy.pid"),
        format!("{}\n", std::process::id()),
    )
    .unwrap();
    let sentinel = Command::new("sleep").arg("120").spawn().unwrap();
    let sentinel_pid = sentinel.id() as i32;
    let _cleanup = LiveCleanup {
        catalog: catalog.clone(),
        xdg: xdg.clone(),
        path: path.clone(),
        sentinel,
    };
    let boot = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args(["up", "--host", "testhost", "--once"])
        .env("XDG_STATE_HOME", &xdg)
        .env("PATH", &path)
        .env_remove("PTY_ROOT")
        .env_remove("PTY_SESSION_DIR")
        .output()
        .unwrap();
    assert!(
        boot.status.success(),
        "{}",
        String::from_utf8_lossy(&boot.stderr)
    );
    let record = xdg.join("st2/testhost/exec/testhost.legacy.ding.pid");
    let generation: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    assert_eq!(generation["schema"], "st2.exec-generation.v2");
    let leader = generation["pid"].as_i64().unwrap() as i32;
    assert!(pid_alive(leader), "legacy Ding exited before retirement");
    fs::write(&record, format!("{leader}\n")).unwrap();

    let plan = temp.path().join("legacy-live-plan.json");
    let prepare = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--legacy-set",
            "--expect-catalog-sha256",
            &root_sha,
            "--output",
        ])
        .arg(&plan)
        .arg("--json")
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        prepare.status.success(),
        "{}",
        String::from_utf8_lossy(&prepare.stderr)
    );
    let plan_sha = serde_json::from_slice::<Value>(&prepare.stdout).unwrap()["planSha256"]
        .as_str()
        .unwrap()
        .to_string();
    let apply = st2()
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "exec",
            "retirement",
            "apply",
            "--plan",
            plan.to_str().unwrap(),
            "--expect-plan-sha256",
            &plan_sha,
            "--json",
        ])
        .env("XDG_STATE_HOME", &xdg)
        .output()
        .unwrap();
    assert!(
        apply.status.success(),
        "{}",
        String::from_utf8_lossy(&apply.stderr)
    );
    let receipt: Value = serde_json::from_slice(&apply.stdout).unwrap();
    assert_eq!(
        receipt["legacyPartition"][0]["desiredState"],
        "running-ding"
    );
    assert_eq!(receipt["targets"][0]["authorityKind"], "legacy-scope-v1");
    assert!(receipt["targets"][0]["generationId"].is_null());
    assert_eq!(receipt["targets"][0]["disposition"], "cgroup-retired");
    assert!(!pid_alive(leader));
    assert!(pid_alive(sentinel_pid));
}
