use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn failing_pty_path() -> tempfile::TempDir {
    let bin = tempfile::tempdir().unwrap();
    executable(&bin.path().join("pty"), "#!/bin/sh\nexit 42\n");
    bin
}

fn assert_skipped_once_exits_nonzero(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        !output.status.success(),
        "a skipped --once pass reported success\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("list sessions (pass skipped)")
            && stderr.contains("one-shot reconcile pass was skipped"),
        "missing skipped-pass receipt:\n{stderr}"
    );
}

#[test]
fn catalog_up_once_exits_nonzero_when_the_pass_is_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = failing_pty_path();
    let agent = tmp.path().join("catalog/agents/h/worker/agent.kdl");
    fs::create_dir_all(agent.parent().unwrap()).unwrap();
    fs::write(
        &agent,
        "agent \"worker\" { host \"h\"; command \"true\" }\n",
    )
    .unwrap();

    assert_skipped_once_exits_nonzero(
        Command::new(env!("CARGO_BIN_EXE_st2"))
            .arg("up")
            .arg("--catalog")
            .arg(tmp.path().join("catalog"))
            .args(["--host", "h", "--once"])
            .env("PATH", bin.path())
            .env("XDG_STATE_HOME", tmp.path().join("state")),
    );
}

#[test]
fn spec_up_once_exits_nonzero_when_the_pass_is_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = failing_pty_path();
    let spec = tmp.path().join("fleet.kdl");
    fs::write(
        &spec,
        "host \"h\"\nteam \"fleet\" { agent \"worker\" { command \"true\" } }\n",
    )
    .unwrap();

    assert_skipped_once_exits_nonzero(
        Command::new(env!("CARGO_BIN_EXE_st2"))
            .arg("up")
            .arg(&spec)
            .args(["--host", "h", "--once"])
            .env("PATH", bin.path())
            .env("XDG_STATE_HOME", tmp.path().join("state")),
    );
}

#[test]
fn catalog_up_once_accepts_a_slow_bounded_fleet_census() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let declaration = catalog.join("agents/h/gone/agent.kdl");
    let bin = tmp.path().join("bin");
    let sessions = tmp.path().join("sessions.json");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::create_dir_all(&bin).unwrap();
    fs::write(
        declaration,
        "agent \"gone\" { host \"h\"; retired #true; command \"true\" }\n",
    )
    .unwrap();
    let fleet = (0..45)
        .map(|index| {
            serde_json::json!({
                "name": format!("seat-{index}"),
                "status": "running",
                "exit_code": null
            })
        })
        .collect::<Vec<_>>();
    fs::write(&sessions, serde_json::to_vec(&fleet).unwrap()).unwrap();
    executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nif [ \"$1\" = list ]; then sleep 3; cat '{}'; fi\n",
            sessions.display()
        ),
    );
    let path = std::env::join_paths(
        std::iter::once(bin.as_path().to_path_buf())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();

    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("up")
        .arg("--catalog")
        .arg(&catalog)
        .args(["--host", "h", "--once"])
        .env("PATH", path)
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "a bounded fleet census skipped the reconcile pass\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(started.elapsed() >= Duration::from_secs(3));
}
