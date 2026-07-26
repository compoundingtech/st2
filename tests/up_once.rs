//! `st2 up --once` is a one-shot boot step (a unit's `ExecStart`, a deploy hook, a CI gate), so its
//! exit status is the only signal a caller reads. This pins that a pass which never happened does not
//! report success.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// A `PATH` holding only `st2` and a `pty` shim whose `list` either works or fails.
fn bin_with_pty(list_fails: bool) -> tempfile::TempDir {
    let bin = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_st2"), bin.path().join("st2")).unwrap();
    let pty = if list_fails {
        "#!/bin/sh\nif [ \"$1\" = \"list\" ]; then echo 'simulated failure' >&2; exit 1; fi\nexit 0\n"
    } else {
        "#!/bin/sh\nif [ \"$1\" = \"list\" ]; then printf '[]\\n'; fi\nexit 0\n"
    };
    executable(&bin.path().join("pty"), pty);
    bin
}

fn catalog(root: &Path) {
    let declaration = root.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        declaration,
        "agent \"worker\" {\n  host \"h\"\n  command \"true\"\n}\n",
    )
    .unwrap();
}

fn up_once(bin: &Path, state: &Path, root: &Path) -> std::process::Output {
    Command::new(bin.join("st2"))
        .env("PATH", bin)
        .env("XDG_STATE_HOME", state)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .args(["up", "--catalog"])
        .arg(root)
        .args(["--host", "h", "--once"])
        .output()
        .unwrap()
}

#[test]
fn up_once_fails_when_the_reconcile_pass_was_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    catalog(&root);
    let bin = bin_with_pty(true);

    let out = up_once(bin.path(), &tmp.path().join("state"), &root);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        stderr.contains("pass skipped"),
        "expected the skipped-pass error on stderr, got: {stderr}"
    );
    assert!(
        !out.status.success(),
        "a pass that never ran must not exit 0 — nothing was launched"
    );
}

#[test]
fn up_once_succeeds_when_the_pass_ran() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    catalog(&root);
    let bin = bin_with_pty(false);

    let out = up_once(bin.path(), &tmp.path().join("state"), &root);
    assert!(
        out.status.success(),
        "a pass that ran must exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
