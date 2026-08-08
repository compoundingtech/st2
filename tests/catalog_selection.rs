//! Shared catalog selection across the CLI: explicit global `--catalog`, inherited `$CATALOG`, then
//! `${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`. Legacy positional/`--root` forms
//! remain covered by the command-specific integration suites.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use sha2::{Digest as _, Sha256};

fn write_agent(catalog: &Path, host: &str, identity: &str) {
    let dir = catalog.join("agents").join(host).join(identity);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("agent.kdl"),
        format!("agent \"{identity}\" {{ host \"{host}\"; command \"true\" }}\n"),
    )
    .unwrap();
}

fn agents(extra: &[&str], catalog_env: Option<&Path>, xdg_state: &Path) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_st2"));
    cmd.args(["agents", "--host", "h", "--json"])
        .args(extra)
        .env("XDG_STATE_HOME", xdg_state);
    match catalog_env {
        Some(path) => {
            cmd.env("CATALOG", path);
        }
        None => {
            cmd.env_remove("CATALOG");
        }
    }
    cmd.output().unwrap()
}

#[test]
fn agents_defaults_to_the_standard_xdg_catalog() {
    let state = tempfile::tempdir().unwrap();
    let catalog = state.path().join("st2/default/catalog");
    write_agent(&catalog, "h", "default-seat");

    let out = agents(&[], None, state.path());
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.default-seat");
}

#[test]
fn agents_defaults_to_home_local_state_without_xdg_state_home() {
    let home = tempfile::tempdir().unwrap();
    let catalog = home.path().join(".local/state/st2/default/catalog");
    write_agent(&catalog, "h", "home-seat");

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["agents", "--host", "h", "--json"])
        .env("HOME", home.path())
        .env_remove("XDG_STATE_HOME")
        .env_remove("CATALOG")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.home-seat");
}

#[test]
fn catalog_environment_overrides_the_standard_default() {
    let state = tempfile::tempdir().unwrap();
    let ambient = tempfile::tempdir().unwrap();
    write_agent(ambient.path(), "h", "ambient-seat");

    let out = agents(&[], Some(ambient.path()), state.path());
    assert!(out.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.ambient-seat");
}

#[test]
fn global_catalog_flag_overrides_the_environment_from_a_subcommand() {
    let state = tempfile::tempdir().unwrap();
    let ambient = tempfile::tempdir().unwrap();
    let selected = tempfile::tempdir().unwrap();
    write_agent(ambient.path(), "h", "ambient-seat");
    write_agent(selected.path(), "h", "selected-seat");

    // Global flags are accepted after the subcommand too; this is the ergonomic `st2 agents
    // --catalog …` spelling even though the option is implemented once at the CLI root.
    let selected_arg = selected.path().to_str().unwrap();
    let out = agents(
        &["--catalog", selected_arg],
        Some(ambient.path()),
        state.path(),
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.selected-seat");
}

#[test]
fn agent_publish_can_target_only_the_global_catalog_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let spec = tmp.path().join("agent.kdl");
    fs::create_dir_all(&catalog).unwrap();
    fs::write(
        &spec,
        "agent \"worker\" {\n  host \"h\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let input_sha256 = format!("{:x}", Sha256::digest(fs::read(&spec).unwrap()));

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(&catalog)
        .args(["agent", "publish", "--spec"])
        .arg(&spec)
        .args(["--input-sha256", &input_sha256])
        .arg("--expect-absent")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(catalog.join("agents/h/worker/agent.kdl").is_file());
}

#[test]
fn catalog_aliases_share_the_canonical_reader_lock_domain() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let alias = tmp.path().join("catalog-alias");
    let spec = catalog.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(spec.parent().unwrap()).unwrap();
    fs::write(
        &spec,
        "agent \"worker\" {\n  host \"h\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(&catalog, &alias).unwrap();

    for args in [["ls"].as_slice(), ["agents", "--json"].as_slice()] {
        let out = Command::new(env!("CARGO_BIN_EXE_st2"))
            .args(args)
            .arg("--catalog")
            .arg(&alias)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{} failed through alias: {}",
            args[0],
            String::from_utf8_lossy(&out.stderr)
        );
    }
    assert!(catalog.join(".st2/catalog-authoring.lock").is_file());
    assert!(!alias.join(".st2/catalog-authoring.lock").is_symlink());
}

#[test]
fn catalog_initialization_adds_the_control_directory_to_the_local_git_exclusion() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    write_agent(&catalog, "h", "worker");
    let initialized = Command::new("git")
        .args(["init", "-q"])
        .arg(&catalog)
        .status()
        .unwrap();
    assert!(initialized.success());
    let exclude = catalog.join(".git/info/exclude");
    fs::write(&exclude, "existing-pattern").unwrap();

    let catalog_arg = catalog.to_str().unwrap();
    for _ in 0..2 {
        let output = agents(&["--catalog", catalog_arg], None, &tmp.path().join("state"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert_eq!(
        fs::read_to_string(exclude).unwrap(),
        "existing-pattern\n.st2/\n"
    );
    let ignored = Command::new("git")
        .args(["-C"])
        .arg(&catalog)
        .args(["check-ignore", "-q", ".st2/catalog-authoring.lock"])
        .status()
        .unwrap();
    assert!(ignored.success());
}

#[test]
fn a_copied_incomplete_marker_blocks_destination_catalog_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    write_agent(&catalog, "h", "worker");
    let catalog_arg = catalog.to_str().unwrap();
    let initialized = agents(&["--catalog", catalog_arg], None, &tmp.path().join("state"));
    assert!(initialized.status.success());
    fs::write(
        catalog.join(".st2/catalog-apply-incomplete"),
        "copied from another host\n",
    )
    .unwrap();

    let blocked = agents(&["--catalog", catalog_arg], None, &tmp.path().join("state"));
    assert!(!blocked.status.success());
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("catalog apply is incomplete"),
        "{}",
        String::from_utf8_lossy(&blocked.stderr)
    );
}
