//! Shared catalog selection across the CLI: explicit global `--catalog`, inherited `$CATALOG`, then
//! `${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`. Legacy positional/`--root` forms
//! remain covered by the command-specific integration suites.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

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
fn compile_agent_can_target_only_the_global_catalog_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let persona = tmp.path().join("persona.md");
    fs::create_dir_all(&workspace).unwrap();
    fs::write(&persona, "# worker\n").unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(&catalog)
        .args([
            "compile-agent",
            "--identity",
            "worker",
            "--host",
            "h",
            "--harness",
            "codex",
            "--dir",
        ])
        .arg(&workspace)
        .arg("--persona")
        .arg(&persona)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(catalog.join("agents/h/worker/agent.kdl").is_file());
}
