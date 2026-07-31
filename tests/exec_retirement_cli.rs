use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Mutex;

use serde_json::Value;

static LIVE_E2E: Mutex<()> = Mutex::new(());

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
fn prepare_cli_hard_cuts_legacy_set_and_requires_one_exact_id() {
    let help = st2()
        .args(["exec", "retirement", "prepare", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("--id <ID>"));
    assert!(!help.contains("--legacy-set"));

    let rejected = st2()
        .args([
            "exec",
            "retirement",
            "prepare",
            "--host",
            "testhost",
            "--legacy-set",
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    let error = String::from_utf8(rejected.stderr).unwrap();
    assert!(error.contains("unexpected argument '--legacy-set'"));

    let source = include_str!("../src/exec_retirement.rs");
    for removed in [
        "derive_legacy_successor_partition",
        "classify_numeric_pid",
        "classify_live_legacy_scope",
        "systemd_scope_witness",
        "capture_legacy_process_witness",
        "verify_legacy_scope_witness",
        "prove_stale_at_apply",
    ] {
        assert!(
            !source.contains(removed),
            "dead authority returned: {removed}"
        );
    }
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
