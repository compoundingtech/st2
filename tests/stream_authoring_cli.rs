use std::fs;
use std::path::Path;
use std::process::Command;

fn write_agent(root: &Path) {
    let directory = root.join("agents/hetz/worker");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("agent.kdl"),
        "agent \"worker\" {\n  host \"hetz\"\n  command \"agent\"\n}\n",
    )
    .unwrap();
}

fn st2(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(root)
        .args(args)
        .env_remove("ST_AGENT")
        .output()
        .unwrap()
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
    let output = Command::new(&argv[0]).args(&argv[1..]).output().unwrap();
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
