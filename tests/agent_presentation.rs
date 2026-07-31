use std::fs;
use std::fs::OpenOptions;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::Path;
use std::process::{Command, Stdio};

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn declaration(identity: &str, supervisor: Option<&str>, managed_by: &str) -> String {
    let supervisor = supervisor
        .map(|value| format!("  supervisor {value:?}\n"))
        .unwrap_or_default();
    format!(
        "// unrelated comment\nagent {identity:?} {{\n  host \"h\"\n  meta {{ managed-by {managed_by:?}; keep \"unchanged\" }}\n{supervisor}  command \"sleep 300\"\n}}\n"
    )
}

fn run(root: &Path, command: &str, args: &[&str], actor: Option<&str>) -> std::process::Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_st2"));
    process
        .args(["--catalog", root.to_str().unwrap(), command])
        .args(args)
        .env_remove("ST_AGENT");
    if let Some(actor) = actor {
        process.env("ST_AGENT", actor);
    }
    process.output().unwrap()
}

#[test]
fn cli_sets_replaces_and_clears_fields_without_changing_identity_or_other_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = declaration("worker", None, "catalog");
    write(root, "h/worker/agent.kdl", &initial);
    write(root, "h/worker/name", "obsolete sibling authority\n");

    for (command, field, value) in [
        ("rename", "name", "Build owner"),
        ("describe", "description", "Own build delivery"),
    ] {
        let output = run(
            root,
            command,
            &["h.worker", value, "--host", "h", "--json"],
            None,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["result"], "changed");
        assert_eq!(receipt["identity"], "h.worker");
        assert_eq!(receipt["field"], field);
        assert_eq!(receipt["value"], value);
    }

    let found = st2::discover(root);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let spec = &found.specs[0];
    assert_eq!(spec.identity, "worker");
    assert_eq!(spec.name.as_deref(), Some("Build owner"));
    assert_eq!(spec.description.as_deref(), Some("Own build delivery"));
    assert_eq!(
        fs::read_to_string(root.join("h/worker/name")).unwrap(),
        "obsolete sibling authority\n",
        "the retired sibling source is ignored, not rewritten or consulted"
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
    assert_eq!(rows[0]["description"], "Own build delivery");

    let repeat = run(
        root,
        "rename",
        &["worker", "Build owner", "--host", "h", "--json"],
        None,
    );
    assert!(repeat.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repeat.stdout).unwrap()["result"],
        "unchanged"
    );

    for command in ["rename", "describe"] {
        let clear = run(
            root,
            command,
            &["h.worker", "--clear", "--host", "h", "--json"],
            None,
        );
        assert!(clear.status.success());
    }
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        initial
    );
    let cleared_roster = Command::new(env!("CARGO_BIN_EXE_st2"))
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
    assert!(cleared_roster.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&cleared_roster.stdout).unwrap();
    assert!(rows[0]["name"].is_null(), "retired sibling name file was consulted");
    assert!(rows[0]["description"].is_null());
}

#[test]
fn cli_rejects_unicode_line_and_paragraph_separators_for_both_fields() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = declaration("worker", None, "catalog");
    write(root, "h/worker/agent.kdl", &initial);

    for command in ["rename", "describe"] {
        for separator in ['\u{2028}', '\u{2029}'] {
            let value = format!("left{separator}right");
            let output = run(
                root,
                command,
                &["h.worker", &value, "--host", "h", "--json"],
                None,
            );
            assert!(
                !output.status.success(),
                "accepted {command} U+{:04X}",
                separator as u32
            );
            let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(receipt["result"], "error");
            assert_eq!(receipt["code"], "invalid-presentation");
        }
    }

    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        initial
    );
}

#[test]
fn cli_enforces_agent_authority_and_nix_and_format_refusals() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/root/agent.kdl",
        &declaration("root", None, "catalog"),
    );
    write(
        root,
        "h/child/agent.kdl",
        &declaration("child", Some("root"), "catalog"),
    );
    write(
        root,
        "h/sibling/agent.kdl",
        &declaration("sibling", Some("root"), "catalog"),
    );
    write(
        root,
        "h/nix/agent.kdl",
        &declaration("nix", Some("root"), "nix"),
    );
    write(
        root,
        "h/json/agent.json",
        r#"{"identity":"json","host":"h","command":"sleep 300"}"#,
    );

    for (command, target, actor, code) in [
        (
            "rename",
            "h.sibling",
            Some("h.child"),
            "presentation-not-authorized",
        ),
        (
            "describe",
            "h.nix",
            Some("h.root"),
            "nix-managed-declaration",
        ),
        ("describe", "h.json", None, "unsupported-declaration-format"),
    ] {
        let output = run(
            root,
            command,
            &[target, "refused", "--host", "h", "--json"],
            actor,
        );
        assert!(!output.status.success());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["result"], "error");
        assert_eq!(receipt["code"], code);
    }

    let allowed = run(
        root,
        "describe",
        &["h.child", "Owned by root", "--host", "h", "--json"],
        Some("h.root"),
    );
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
}

#[test]
fn concurrent_cli_writers_serialize_without_losing_either_field() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", None, "catalog"),
    );
    fs::create_dir(root.join(".st2")).unwrap();
    let lock_path = root.join(".st2/presentation-authoring.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(&lock_path)
        .unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);
    let inode = fs::metadata(&lock_path).unwrap().ino();

    let spawn = |command: &str, value: &str| {
        Command::new(env!("CARGO_BIN_EXE_st2"))
            .args([
                "--catalog",
                root.to_str().unwrap(),
                command,
                "h.worker",
                value,
                "--host",
                "h",
                "--json",
            ])
            .env_remove("ST_AGENT")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let rename = spawn("rename", "Build owner");
    let describe = spawn("describe", "Own build delivery");
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) }, 0);

    for output in [
        rename.wait_with_output().unwrap(),
        describe.wait_with_output().unwrap(),
    ] {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let found = st2::discover(root);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(found.specs[0].name.as_deref(), Some("Build owner"));
    assert_eq!(
        found.specs[0].description.as_deref(),
        Some("Own build delivery")
    );
    assert_eq!(fs::metadata(lock_path).unwrap().ino(), inode);
}

#[test]
fn presentation_lock_refuses_a_symlinked_control_directory() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("catalog");
    let outside = temporary.path().join("outside");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&outside).unwrap();
    write(
        &root,
        "h/worker/agent.kdl",
        &declaration("worker", None, "catalog"),
    );
    symlink(&outside, root.join(".st2")).unwrap();

    let output = run(
        &root,
        "rename",
        &["h.worker", "Owner", "--host", "h", "--json"],
        None,
    );
    assert!(!output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["code"], "presentation-lock-failed");
    assert!(!outside.join("presentation-authoring.lock").exists());
}

#[test]
fn presentation_lock_refuses_a_symlinked_lock_file() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("catalog");
    let outside = temporary.path().join("outside-lock");
    fs::create_dir(&root).unwrap();
    fs::create_dir(root.join(".st2")).unwrap();
    fs::write(&outside, "unchanged").unwrap();
    write(
        &root,
        "h/worker/agent.kdl",
        &declaration("worker", None, "catalog"),
    );
    symlink(&outside, root.join(".st2/presentation-authoring.lock")).unwrap();

    let output = run(
        &root,
        "describe",
        &["h.worker", "Owner", "--host", "h", "--json"],
        None,
    );
    assert!(!output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["code"], "presentation-lock-failed");
    assert_eq!(fs::read_to_string(outside).unwrap(), "unchanged");
}
