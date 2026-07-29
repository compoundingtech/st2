//! `st2 init` — minimal, idempotent host-local catalog initialization.

use std::path::Path;
use std::process::{Command, Output};

const INITIAL: &str = "host {\n  pty-root \"$CATALOG/pty\"\n}\n";

fn st2(args: &[&str], state: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(args)
        .env("HOME", state)
        .env("XDG_STATE_HOME", state.join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap()
}

#[test]
fn init_creates_only_the_matching_host_config_and_is_byte_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("new/catalog");
    let catalog_arg = catalog.to_str().unwrap();

    let first = st2(
        &["init", "--catalog", catalog_arg, "--host", "local"],
        tmp.path(),
    );
    assert!(
        first.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let config = catalog.join("agents/local/config.kdl");
    assert_eq!(std::fs::read_to_string(&config).unwrap(), INITIAL);
    assert!(
        !catalog.join("catalog.kdl").exists(),
        "init must not generate a global machine-specific fallback"
    );

    let before = std::fs::read(&config).unwrap();
    let second = st2(
        &["init", "--catalog", catalog_arg, "--host", "local"],
        tmp.path(),
    );
    assert!(second.status.success());
    assert_eq!(std::fs::read(&config).unwrap(), before);
    assert!(
        String::from_utf8_lossy(&second.stdout).contains("(unchanged)"),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
}

#[test]
fn init_preserves_an_existing_valid_author_override() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let config = catalog.join("agents/local/config.kdl");
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    let authored = "host {\n  pty-root \"/machine-local/registry\"\n}\n";
    std::fs::write(&config, authored).unwrap();

    let out = st2(
        &[
            "init",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "local",
        ],
        tmp.path(),
    );
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read_to_string(&config).unwrap(), authored);
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("/machine-local/registry"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

#[test]
fn init_rejects_an_existing_invalid_file_without_overwriting_it() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let config = catalog.join("agents/local/config.kdl");
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    let invalid = b"host { pty_root \"wrong\" }\n";
    std::fs::write(&config, invalid).unwrap();

    let out = st2(
        &[
            "init",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "local",
        ],
        tmp.path(),
    );
    assert!(!out.status.success());
    assert_eq!(std::fs::read(&config).unwrap(), invalid);
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unknown host field 'pty_root'"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn init_rejects_a_host_that_is_not_one_safe_path_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let out = st2(
        &[
            "init",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "../escape",
        ],
        tmp.path(),
    );
    assert!(!out.status.success());
    assert!(!tmp.path().join("escape/config.kdl").exists());
}
