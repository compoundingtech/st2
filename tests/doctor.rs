#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command};

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn retired_catalog(root: &Path) -> Child {
    let declaration = root.join("agents/h/gone/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        declaration,
        "agent \"gone\" { host \"h\"; retired #true; command \"true\" }\n",
    )
    .unwrap();
    let owner = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    fs::write(root.join(".st2.h.lock"), format!("{}\n", owner.id())).unwrap();
    owner
}

fn doctor(catalog: &Path, bin: &Path, state: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("doctor")
        .arg("--catalog")
        .arg(catalog)
        .args(["--host", "h"])
        .env("PATH", bin)
        .env("XDG_STATE_HOME", state)
        .env("PTY_ROOT", state.join("pty"))
        .output()
        .unwrap()
}

#[test]
fn retired_declaration_is_healthy_when_tasks_and_presence_are_absent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let bin = tmp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    executable(
        &bin.join("pty"),
        "#!/bin/sh\nif [ \"$1\" = list ]; then printf '[]\\n'; fi\n",
    );
    let mut owner = retired_catalog(&catalog);

    let output = doctor(&catalog, &bin, &tmp.path().join("state"));
    let _ = owner.kill();
    let _ = owner.wait();
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("✓ h.gone retirement complete (all declared tasks absent)"),
        "{stdout}"
    );
    assert!(
        !stdout.contains("h.gone presence"),
        "retired declarations must not require presence:\n{stdout}"
    );
}

#[test]
fn retired_declaration_is_unhealthy_while_a_declared_task_is_alive() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let bin = tmp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    executable(
        &bin.join("pty"),
        "#!/bin/sh\nif [ \"$1\" = list ]; then printf '[{\"name\":\"h.gone\",\"status\":\"running\"}]\\n'; fi\n",
    );
    let mut owner = retired_catalog(&catalog);

    let output = doctor(&catalog, &bin, &tmp.path().join("state"));
    let _ = owner.kill();
    let _ = owner.wait();
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        !output.status.success(),
        "retirement leak passed doctor:\n{stdout}"
    );
    assert!(
        stdout.contains(
            "✗ h.gone retirement complete (all declared tasks absent) — still present: h.gone (alive)"
        ),
        "{stdout}"
    );
    assert!(
        !stdout.contains("h.gone presence"),
        "retired declarations must not require presence:\n{stdout}"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("1 problem(s) found"),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn retired_declaration_is_unhealthy_while_a_dead_task_record_remains() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let bin = tmp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    executable(
        &bin.join("pty"),
        "#!/bin/sh\nif [ \"$1\" = list ]; then printf '[{\"name\":\"h.gone\",\"status\":\"exited\"}]\\n'; fi\n",
    );
    let mut owner = retired_catalog(&catalog);

    let output = doctor(&catalog, &bin, &tmp.path().join("state"));
    let _ = owner.kill();
    let _ = owner.wait();
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        !output.status.success(),
        "retirement residue passed doctor:\n{stdout}"
    );
    assert!(
        stdout.contains(
            "✗ h.gone retirement complete (all declared tasks absent) — still present: h.gone (dead)"
        ),
        "{stdout}"
    );
    assert!(
        !stdout.contains("h.gone presence"),
        "retired declarations must not require presence:\n{stdout}"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("1 problem(s) found"),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
