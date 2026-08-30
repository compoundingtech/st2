use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

static RUNTIME_TEST: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn write_agent(root: &Path) {
    let directory = root.join("agents/hetz/worker");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("agent.kdl"),
        "agent \"worker\" {\n  host \"hetz\"\n  command \"agent\"\n}\n",
    )
    .unwrap();
    st2::event::publish_owner_binding_in_state_root_for_test(root, "hetz", &root.join("state"))
        .unwrap();
}

fn configure_st2_command(command: &mut Command, root: &Path) {
    command
        .arg("--catalog")
        .arg(root)
        .env("PTY_ROOT", root.join("pty"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("ST2_TEST_EVENT_HOST", "hetz")
        .env_remove("ST_AGENT");
}

fn st2_command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
    configure_st2_command(&mut command, root);
    command
}

fn st2(root: &Path, args: &[&str]) -> std::process::Output {
    st2_command(root).args(args).output().unwrap()
}

fn up_once(root: &Path) {
    let output = st2(root, &["up", "--once", "--host", "hetz"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn pty_ids(root: &Path) -> Vec<(String, String)> {
    let output = Command::new("pty")
        .args(["list", "--json"])
        .env("PTY_ROOT", root.join("pty"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    rows.as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["name"].as_str().unwrap().to_owned(),
                row["status"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn task_runtime_state(root: &Path, id: &str) -> Option<String> {
    let output = st2(root, &["tasks", "--host", "hetz", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let inventory: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    inventory["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|task| {
            (task["runtimeId"] == id).then(|| task["runtime"]["state"].as_str().unwrap().to_owned())
        })
}

fn runtime_is_running(root: &Path, id: &str) -> bool {
    task_runtime_state(root, id).as_deref() == Some("running")
}

fn assert_runtime_is_running(root: &Path, id: &str) {
    let state = task_runtime_state(root, id);
    assert!(
        state.as_deref() == Some("running"),
        "{id} is not running: {state:?}"
    );
}

fn exec_record_exists(root: &Path, id: &str) -> bool {
    root.join("state/st2/hetz/exec")
        .join(format!("{id}.pid"))
        .exists()
}

fn tree_snapshot(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    fn collect(root: &Path, directory: &Path, output: &mut Vec<(PathBuf, Option<Vec<u8>>)>) {
        let mut entries = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if path.is_dir() {
                output.push((relative, None));
                collect(root, &path, output);
            } else {
                output.push((relative, Some(fs::read(&path).unwrap())));
            }
        }
    }

    let mut output = Vec::new();
    collect(root, root, &mut output);
    output
}

#[test]
fn a_runtime_run_leaves_inherited_production_roots_byte_identical() {
    let production_pty = tempfile::tempdir().unwrap();
    let production_state = tempfile::tempdir().unwrap();
    fs::create_dir_all(production_pty.path().join("nested")).unwrap();
    fs::write(
        production_pty.path().join("nested/sentinel"),
        b"production pty",
    )
    .unwrap();
    fs::write(
        production_state.path().join("sentinel"),
        b"production state",
    )
    .unwrap();
    let pty_before = tree_snapshot(production_pty.path());
    let state_before = tree_snapshot(production_state.path());

    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
    command
        .env("PTY_ROOT", production_pty.path())
        .env("XDG_STATE_HOME", production_state.path());
    configure_st2_command(&mut command, catalog.path());
    let output = command
        .args(["up", "--once", "--host", "hetz"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        pty_ids(catalog.path())
            .iter()
            .any(|(name, _)| name == "hetz.worker")
    );
    assert_eq!(tree_snapshot(production_pty.path()), pty_before);
    assert_eq!(tree_snapshot(production_state.path()), state_before);
}

#[test]
fn stream_add_emit_and_rm_are_one_real_cli_workflow() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());

    let add = st2(
        catalog.path(),
        &[
            "stream",
            "add",
            "webhook",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--json",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&add.stdout).unwrap();
    assert_eq!(receipt["result"], "changed");
    assert_eq!(receipt["name"], "webhook");
    assert!(receipt["launch"].is_null());

    let emit = st2(
        catalog.path(),
        &[
            "event",
            "emit",
            "hetz.worker",
            "--stream",
            "webhook",
            "--event-id",
            "delivery-1",
            "--message",
            "payload",
            "--host",
            "hetz",
            "--json",
        ],
    );
    assert!(
        emit.status.success(),
        "{}",
        String::from_utf8_lossy(&emit.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&emit.stdout).unwrap();
    assert_eq!(receipt["status"], "created");

    let remove = st2(
        catalog.path(),
        &[
            "stream",
            "rm",
            "webhook",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--json",
        ],
    );
    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&remove.stdout).unwrap();
    assert_eq!(receipt["result"], "changed");
    let declaration =
        fs::read_to_string(catalog.path().join("agents/hetz/worker/agent.kdl")).unwrap();
    assert!(!declaration.contains("stream \"webhook\""));
}

#[test]
fn a_direct_adapter_launch_executes_the_exact_event_cli_contract() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    let binary = env!("CARGO_BIN_EXE_st2");
    let add = st2(
        catalog.path(),
        &[
            "stream",
            "add",
            "adapter",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--",
            binary,
            "--catalog",
            catalog.path().to_str().unwrap(),
            "event",
            "emit",
            "hetz.worker",
            "--stream",
            "adapter",
            "--event-id",
            "adapter-1",
            "--message",
            "from-adapter",
            "--host",
            "hetz",
            "--json",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );

    let spec = st2::discover(catalog.path()).specs.remove(0);
    let adapter = spec
        .tasks
        .iter()
        .find(|task| task.name == "stream-adapter")
        .unwrap();
    assert!(adapter.derived);
    let argv = adapter.argv.as_ref().unwrap();
    let output = Command::new(&argv[0])
        .args(&argv[1..])
        .env("PTY_ROOT", catalog.path().join("pty"))
        .env("XDG_STATE_HOME", catalog.path().join("state"))
        .env("ST2_TEST_EVENT_HOST", "hetz")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["status"], "created");
    assert_eq!(
        st2::message::list_inbox(&st2::message::inbox_dir(
            &catalog.path().join("agents/hetz/worker")
        ))
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn direct_adapter_argv_preserves_spaces_and_metacharacters_exactly() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    let expected = [
        "/bin/example adapter",
        "argument with spaces",
        "$HOME",
        "$(never-executed)",
        "semi;colon",
        "quote\"and\\slash",
        "--looks-like-a-flag",
    ];
    let mut args = vec![
        "stream",
        "add",
        "exact-argv",
        "--agent",
        "hetz.worker",
        "--host",
        "hetz",
        "--",
    ];
    args.extend(expected);

    let add = st2(catalog.path(), &args);

    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    let spec = st2::discover(catalog.path()).specs.remove(0);
    let stream = spec
        .streams
        .iter()
        .find(|stream| stream.name == "exact-argv")
        .unwrap();
    assert_eq!(
        stream.launch,
        Some(st2::spec::StreamLaunch::Argv(
            expected.iter().map(|value| (*value).to_owned()).collect()
        ))
    );
}

#[test]
fn command_and_direct_argv_are_mutually_exclusive() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());

    let add = st2(
        catalog.path(),
        &[
            "stream",
            "add",
            "ambiguous",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--command",
            "echo shell",
            "--",
            "/bin/echo",
            "direct",
        ],
    );

    assert!(!add.status.success());
    assert!(
        String::from_utf8_lossy(&add.stderr).contains("cannot be used with"),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
}

#[test]
fn launched_stream_removal_retires_runtime_before_source_publication() {
    let _runtime = RUNTIME_TEST.lock().unwrap();
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    let add = st2(
        catalog.path(),
        &[
            "stream",
            "add",
            "live",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--command",
            "sleep 60",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    for _ in 0..3 {
        up_once(catalog.path());
        if runtime_is_running(catalog.path(), "hetz.worker.stream-live") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_runtime_is_running(catalog.path(), "hetz.worker.stream-live");

    let remove = st2(
        catalog.path(),
        &[
            "stream",
            "rm",
            "live",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
        ],
    );

    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
    let mut retired = false;
    for _ in 0..100 {
        retired = !exec_record_exists(catalog.path(), "hetz.worker.stream-live");
        if retired {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        retired,
        "stream adapter remained alive after lifecycle-first removal"
    );
    assert!(
        !fs::read_to_string(catalog.path().join("agents/hetz/worker/agent.kdl"))
            .unwrap()
            .contains("stream \"live\"")
    );
}

#[test]
fn launched_stream_removal_escalates_an_ignoring_adapter_before_forgetting_it() {
    let _runtime = RUNTIME_TEST.lock().unwrap();
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    let add = st2(
        catalog.path(),
        &[
            "stream",
            "add",
            "stubborn",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
            "--command",
            "trap '' TERM; while :; do sleep 1; done",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    up_once(catalog.path());
    let runtime_id = "hetz.worker.stream-stubborn";
    for _ in 0..100 {
        if runtime_is_running(catalog.path(), runtime_id) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_runtime_is_running(catalog.path(), runtime_id);

    let started = std::time::Instant::now();
    let remove = st2(
        catalog.path(),
        &[
            "stream",
            "rm",
            "stubborn",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
        ],
    );
    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(started.elapsed() >= std::time::Duration::from_secs(2));
    assert!(
        !exec_record_exists(catalog.path(), runtime_id),
        "retirement must not erase the record until the ignoring process group exits"
    );
    assert!(
        !fs::read_to_string(catalog.path().join("agents/hetz/worker/agent.kdl"))
            .unwrap()
            .contains("stream \"stubborn\"")
    );
}

#[test]
fn failed_source_publish_after_stop_keeps_declaration_relaunchable() {
    let _runtime = RUNTIME_TEST.lock().unwrap();
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    assert!(
        st2(
            catalog.path(),
            &[
                "stream",
                "add",
                "live",
                "--agent",
                "hetz.worker",
                "--host",
                "hetz",
                "--command",
                "sleep 60"
            ]
        )
        .status
        .success()
    );
    up_once(catalog.path());
    let failed = st2_command(catalog.path())
        .args([
            "stream",
            "rm",
            "live",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
        ])
        .env("ST2_TEST_AGENT_AUTHOR_FAIL_BEFORE_PUBLISH", "1")
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(
        fs::read_to_string(catalog.path().join("agents/hetz/worker/agent.kdl"))
            .unwrap()
            .contains("stream \"live\"")
    );

    for _ in 0..3 {
        up_once(catalog.path());
        if runtime_is_running(catalog.path(), "hetz.worker.stream-live") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_runtime_is_running(catalog.path(), "hetz.worker.stream-live");
    assert!(
        st2(
            catalog.path(),
            &[
                "stream",
                "rm",
                "live",
                "--agent",
                "hetz.worker",
                "--host",
                "hetz"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn external_stream_removal_performs_no_runtime_operation() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());
    assert!(
        st2(
            catalog.path(),
            &[
                "stream",
                "add",
                "external",
                "--agent",
                "hetz.worker",
                "--host",
                "hetz"
            ]
        )
        .status
        .success()
    );

    let remove = st2_command(catalog.path())
        .args([
            "stream",
            "rm",
            "external",
            "--agent",
            "hetz.worker",
            "--host",
            "hetz",
        ])
        .env("PATH", "")
        .output()
        .unwrap();

    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
}

#[test]
fn a_bare_actor_can_self_author_on_the_selected_host() {
    let catalog = tempfile::tempdir().unwrap();
    write_agent(catalog.path());

    let add = st2(
        catalog.path(),
        &[
            "stream", "add", "webhook", "--as", "worker", "--host", "hetz",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );

    let remove = st2(
        catalog.path(),
        &[
            "stream", "rm", "webhook", "--as", "worker", "--host", "hetz",
        ],
    );
    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
}
