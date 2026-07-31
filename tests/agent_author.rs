use std::fs;
use std::path::Path;
use std::process::{Child, Command};

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
