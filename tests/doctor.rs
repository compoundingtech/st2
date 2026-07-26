//! `st2 doctor` is the obvious thing to wire into a health gate, so each ✓ has to mean what it says.
//! This pins the presence check: a status file that was never written is not a fresh one. Both cases
//! run against an identical catalog and differ only in whether that file exists.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// A `PATH` holding only `st2` and an inert `pty` shim.
fn bin() -> tempfile::TempDir {
    let bin = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_st2"), bin.path().join("st2")).unwrap();
    executable(
        &bin.path().join("pty"),
        "#!/bin/sh\nif [ \"$1\" = \"list\" ]; then printf '[]\\n'; fi\nexit 0\n",
    );
    bin
}

/// A one-agent catalog whose host-lock is owned by this (live) test process, so doctor's supervisor
/// check passes without spawning anything — the `st2` child has a different pid, so it reads the lock
/// as held by someone else.
fn catalog(root: &Path) {
    let declaration = root.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        declaration,
        "agent \"worker\" {\n  host \"h\"\n  command \"true\"\n}\n",
    )
    .unwrap();
    fs::write(root.join(".st2.h.lock"), format!("{}\n", std::process::id())).unwrap();
}

/// Doctor's stdout for a fresh catalog, optionally seeded with a presence file.
fn doctor_stdout(root: &Path, presence: Option<&str>) -> String {
    catalog(root);
    if let Some(state) = presence {
        fs::write(root.join("agents/h/worker/status"), format!("{state}\n")).unwrap();
    }
    let bin = bin();
    let out = Command::new(bin.path().join("st2"))
        .args(["doctor", "--catalog"])
        .arg(root)
        .args(["--host", "h"])
        .env("PATH", bin.path())
        .env("PTY_ROOT", root.join("pty"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn doctor_does_not_call_a_never_written_presence_fresh() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    let stdout = doctor_stdout(&root, None);

    assert!(
        !root.join("agents/h/worker/status").exists(),
        "precondition: the agent has no status file"
    );
    assert!(
        !stdout.contains("presence fresh"),
        "a presence file that was never written must not be reported as fresh:\n{stdout}"
    );
    assert!(
        stdout.contains("✗ h.worker presence written"),
        "the missing presence file must be reported as a problem:\n{stdout}"
    );
}

#[test]
fn doctor_accepts_a_deliberately_offline_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    // `offline` is a settable state — deliberately offline is healthy. Only *absent* is not.
    let stdout = doctor_stdout(&root, Some("offline"));

    assert!(
        stdout.contains("✓ h.worker presence fresh (is `offline`)"),
        "a written `offline` presence is fresh:\n{stdout}"
    );
}
