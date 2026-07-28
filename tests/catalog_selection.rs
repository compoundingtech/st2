//! Shared catalog selection across the CLI: explicit global `--catalog`, inherited `$CATALOG`, then
//! `${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`. Legacy positional/`--root` forms
//! remain covered by the command-specific integration suites.
//!
//! `down` is the one deliberate exception: teardown names its target or refuses.

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

/// Teardown is the only verb that ends tasks, so it does not inherit its target from the shared
/// selection: both fallbacks are ambient (st2 exports `CATALOG` into every task it launches, and the
/// standard default is derived from `$HOME`), which would make a forgotten argument resolve to a live
/// fleet instead of failing. Hermetic: `HOME`/`XDG_STATE_HOME` are temp dirs and the bus env vars are
/// removed, so a regression here kills a temp catalog's (empty) session set, never a real one.
#[test]
fn down_refuses_an_inferred_target_but_takes_a_named_one() {
    let home = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let ambient = tempfile::tempdir().unwrap();
    write_agent(&state.path().join("st2/default/catalog"), "h", "default-seat");
    write_agent(ambient.path(), "h", "ambient-seat");

    let down = |catalog_env: Option<&Path>, extra: &[&str]| -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_st2"));
        cmd.args(["down", "--host", "h"])
            .args(extra)
            .env("HOME", home.path())
            .env("XDG_STATE_HOME", state.path())
            .env_remove("ST_ROOT")
            .env_remove("PTY_ROOT");
        match catalog_env {
            Some(path) => cmd.env("CATALOG", path),
            None => cmd.env_remove("CATALOG"),
        };
        cmd.output().unwrap()
    };

    // The standard default catalog is not a teardown target…
    let out = down(None, &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "down tore down an inferred catalog:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        stderr.contains("refusing to tear down an inferred catalog"),
        "no refusal:\n{stderr}"
    );
    assert!(
        stderr.contains(
            state
                .path()
                .join("st2/default/catalog")
                .to_str()
                .unwrap()
        ),
        "the refusal must name what it would have torn down:\n{stderr}"
    );

    // …and neither is an inherited `$CATALOG`, which st2 itself exports into every managed task.
    let out = down(Some(ambient.path()), &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "down tore down an inherited $CATALOG");
    assert!(
        stderr.contains(ambient.path().to_str().unwrap()),
        "the refusal must name the inherited catalog:\n{stderr}"
    );

    // A named target is accepted — from argv, over an inherited $CATALOG pointing elsewhere.
    let named = tempfile::tempdir().unwrap();
    let out = down(Some(ambient.path()), &["--catalog", named.path().to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("refusing to tear down"),
        "a named target was refused:\n{stderr}"
    );
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
