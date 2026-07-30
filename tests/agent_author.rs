use std::fs;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use st2::exec_backend::ExecBackend;
use st2::host_lock::process_alive;
use st2::{Runner, Session, TaskTarget};

struct LiveChild(Child);

impl Drop for LiveChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn declaration(identity: &str, host: &str, retired: bool) -> String {
    format!(
        "agent \"{identity}\" {{\n  host \"{host}\"\n  retired #{retired}\n  command \"sleep 300\"\n}}\n"
    )
}

fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["--catalog", root.to_str().unwrap(), "agent", "name"])
        .args(args)
        .output()
        .unwrap()
}

fn alive(child: &LiveChild) -> bool {
    unsafe { libc::kill(child.0.id() as i32, 0) == 0 }
}

fn task_ids(root: &Path) -> Vec<String> {
    let found = st2::discover(root);
    st2::reconcile(&found.specs, &[], "h")
        .launch
        .iter()
        .flat_map(|launch| launch.tasks.iter())
        .map(|task| task.pty_id.clone())
        .collect()
}

struct ExecOnlyRunner {
    backend: ExecBackend,
    task_ids: Vec<String>,
}

impl ExecOnlyRunner {
    fn new(state: &Path, catalog: &Path, task_ids: &[&str]) -> Self {
        Self {
            backend: ExecBackend::new(state.to_path_buf(), catalog.to_path_buf()),
            task_ids: task_ids.iter().map(|value| (*value).to_string()).collect(),
        }
    }
}

impl Runner for ExecOnlyRunner {
    fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        self.backend.list()
    }

    fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
        self.backend.spawn(target, spec_dir)
    }

    fn kill(&self, task_id: &str) -> anyhow::Result<()> {
        self.backend.kill(task_id)
    }

    fn reap_for_restart(&self, task_id: &str) -> anyhow::Result<()> {
        self.backend.reap_for_restart(task_id)
    }

    fn remove(&self, task_id: &str) -> anyhow::Result<()> {
        self.backend.remove(task_id)
    }
}

impl Drop for ExecOnlyRunner {
    fn drop(&mut self) {
        for task_id in &self.task_ids {
            let _ = self.backend.kill(task_id);
            let _ = self.backend.remove(task_id);
        }
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}

#[test]
fn cli_sets_lists_idempotently_replaces_and_clears_without_runtime_or_state_mutation() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let agent_kdl = declaration("worker", "h", false);
    write(root, "h/worker/agent.kdl", &agent_kdl);
    write(root, "h/worker/status", "busy\n");
    write(
        root,
        "h/worker/resources/inbox/1000000000000-aaaaaa.md",
        "---\nfrom: h.sender\n---\nunread\n",
    );
    write(
        root,
        "h/worker/resources/context/now.md",
        "durable context\n",
    );
    write(root, "h/worker/resources/archive/receipt.md", "handled\n");
    write(
        root,
        "h/worker/resources/links/work",
        "https://example.invalid/work\n",
    );
    let initial_task_ids = task_ids(root);
    let live = LiveChild(Command::new("sleep").arg("300").spawn().unwrap());

    let set = run(root, &["h.worker", "Build owner", "--json", "--host", "h"]);
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&set.stdout).unwrap();
    assert_eq!(receipt["result"], "changed");
    assert_eq!(receipt["identity"], "h.worker");
    assert_eq!(receipt["name"], "Build owner");
    assert_eq!(receipt["retired"], false);
    assert!(
        alive(&live),
        "display-name authoring must not signal live work"
    );

    let roster = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            root.to_str().unwrap(),
            "agents",
            "--host",
            "h",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(roster.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&roster.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.worker");
    assert_eq!(rows[0]["name"], "Build owner");

    let human = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["--catalog", root.to_str().unwrap(), "agents", "--host", "h"])
        .output()
        .unwrap();
    assert!(human.status.success());
    assert_eq!(
        String::from_utf8(human.stdout).unwrap(),
        "h.worker\tbusy\tBuild owner\n"
    );

    let replace = run(
        root,
        &["h.worker", "Release owner", "--json", "--host", "h"],
    );
    assert!(replace.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replace.stdout).unwrap()["result"],
        "changed"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/name")).unwrap(),
        "Release owner\n"
    );

    let repeat = run(root, &["worker", "Release owner", "--json", "--host", "h"]);
    assert!(repeat.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repeat.stdout).unwrap()["result"],
        "unchanged"
    );

    let clear = run(root, &["h.worker", "--clear", "--json", "--host", "h"]);
    assert!(clear.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&clear.stdout).unwrap()["name"],
        serde_json::Value::Null
    );
    assert!(!root.join("h/worker/name").exists());
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        agent_kdl
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/resources/context/now.md")).unwrap(),
        "durable context\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/resources/archive/receipt.md")).unwrap(),
        "handled\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/resources/inbox/1000000000000-aaaaaa.md")).unwrap(),
        "---\nfrom: h.sender\n---\nunread\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/resources/links/work")).unwrap(),
        "https://example.invalid/work\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/status")).unwrap(),
        "busy\n"
    );
    assert_eq!(task_ids(root), initial_task_ids);
    assert!(alive(&live));
}

#[test]
fn cli_reports_ambiguous_missing_invalid_and_retired_remote_targets() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "h", false),
    );
    write(
        root,
        "remote/worker/agent.kdl",
        &declaration("worker", "remote", true),
    );

    for (args, code) in [
        (
            vec!["worker", "owner", "--json", "--host", "h"],
            "target-ambiguous",
        ),
        (
            vec!["missing", "owner", "--json", "--host", "h"],
            "target-not-found",
        ),
        (
            vec!["h.worker", " leading", "--json", "--host", "h"],
            "invalid-display-name",
        ),
    ] {
        let output = run(root, &args);
        assert!(!output.status.success());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["result"], "error");
        assert_eq!(receipt["code"], code);
        assert_eq!(receipt["identity"], args[0]);
    }

    let remote = run(
        root,
        &["remote.worker", "Retired owner", "--json", "--host", "h"],
    );
    assert!(
        remote.status.success(),
        "{}",
        String::from_utf8_lossy(&remote.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&remote.stdout).unwrap();
    assert_eq!(receipt["identity"], "remote.worker");
    assert_eq!(receipt["retired"], true);
    assert_eq!(
        fs::read_to_string(root.join("remote/worker/name")).unwrap(),
        "Retired owner\n"
    );
}

#[test]
fn cli_retirement_is_source_preserving_and_reports_runtime_as_unobserved() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let declaration = root.join("unusual/folder/seat.kdl");
    let original = r#"// authored source
agent "worker" {
  host "h"
  role "builder" // stays byte-identical
  command "sleep 300"
}
"#;
    write(root, "unusual/folder/seat.kdl", original);
    write(root, "unusual/folder/status", "available\n");
    write(root, "unusual/folder/resources/context/now.md", "durable\n");

    let authored = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            root.to_str().unwrap(),
            "agent",
            "retire",
            "h.worker",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        authored.status.success(),
        "{}",
        String::from_utf8_lossy(&authored.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&authored.stdout).unwrap();
    assert_eq!(receipt["result"], "authored");
    assert_eq!(receipt["identity"], "h.worker");
    assert_eq!(receipt["retired"], true);
    assert_eq!(receipt["runtimeRetirement"], "not-observed");
    assert_eq!(
        fs::read_to_string(&declaration).unwrap(),
        r#"// authored source
agent "worker" {
  host "h"
  role "builder" // stays byte-identical
  command "sleep 300"
  retired #true
}
"#
    );
    assert_eq!(
        fs::read_to_string(root.join("unusual/folder/status")).unwrap(),
        "available\n"
    );
    assert_eq!(
        fs::read_to_string(root.join("unusual/folder/resources/context/now.md")).unwrap(),
        "durable\n"
    );
    assert!(!root.join("exec").exists());

    let unchanged = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            root.to_str().unwrap(),
            "agent",
            "retire",
            "h.worker",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(unchanged.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unchanged.stdout).unwrap()["result"],
        "unchanged"
    );
}

#[test]
fn retirement_tears_down_only_the_selected_live_task_and_never_relaunches_it() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "fleet.kdl",
        r#"agent "one" {
  host "h"
  exec "task" { command "sleep 300" }
}
agent "two" {
  host "h"
  exec "task" { command "sleep 300" }
}
"#,
    );
    let state = temporary.path().join("exec-state");
    let runner = ExecOnlyRunner::new(&state, root, &["h.one.task", "h.two.task"]);

    let launched = st2::up_once(root, "h", &runner).unwrap();
    assert!(launched.errors.is_empty(), "{:?}", launched.errors);
    assert_eq!(launched.launched, ["h.one.task", "h.two.task"]);
    let one_pid: i32 = fs::read_to_string(state.join("h.one.task.pid"))
        .unwrap()
        .parse()
        .unwrap();
    let two_pid: i32 = fs::read_to_string(state.join("h.two.task.pid"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(process_alive(one_pid));
    assert!(process_alive(two_pid));

    let receipt = st2::agent_author::retire_agent(root, "h.one", "h").unwrap();
    assert_eq!(receipt.result, st2::agent_author::RetireOutcome::Authored);
    assert_eq!(
        receipt.runtime_retirement,
        st2::agent_author::RuntimeRetirement::NotObserved
    );
    let torn_down = st2::up_once(root, "h", &runner).unwrap();
    assert!(torn_down.errors.is_empty(), "{:?}", torn_down.errors);
    assert_eq!(torn_down.torn_down, ["h.one.task"]);
    assert!(torn_down.launched.is_empty());
    assert!(wait_until(|| {
        runner
            .list_sessions()
            .unwrap()
            .iter()
            .any(|session| session.pty_id == "h.one.task" && !session.alive)
    }));
    assert!(process_alive(two_pid), "unselected task was disrupted");

    let cleanup = st2::up_once(root, "h", &runner).unwrap();
    assert!(cleanup.errors.is_empty(), "{:?}", cleanup.errors);
    assert_eq!(cleanup.gc, ["h.one.task"]);
    assert!(cleanup.launched.is_empty());
    assert!(!state.join("h.one.task.pid").exists());
    assert_eq!(
        fs::read_to_string(state.join("h.two.task.pid"))
            .unwrap()
            .parse::<i32>()
            .unwrap(),
        two_pid
    );

    let stable = st2::up_once(root, "h", &runner).unwrap();
    assert!(stable.errors.is_empty(), "{:?}", stable.errors);
    assert!(stable.launched.is_empty());
    assert!(!state.join("h.one.task.pid").exists());
    assert!(process_alive(two_pid));
}
