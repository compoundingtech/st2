#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

fn pin_hooks(mut command: Command, hooks_root: &Path) -> Command {
    command.env("ST_HOOKS", hooks_root);
    command
}

fn command(hooks_root: &Path) -> Command {
    pin_hooks(Command::new(env!("CARGO_BIN_EXE_st2")), hooks_root)
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn hooks_cli_is_explicit_receipted_idempotent_and_verify_only() {
    let tmp = tempfile::tempdir().unwrap();
    let hooks_root = tmp.path().join("hooks");

    let missing = command(&hooks_root)
        .args(["hooks", "verify"])
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(
        !hooks_root.exists(),
        "read-only verification must not create the hook root"
    );

    let install = command(&hooks_root)
        .args(["hooks", "install"])
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    let receipt_path = hooks_root.join("current.json");
    let first_receipt = fs::read(&receipt_path).unwrap();
    let receipt: serde_json::Value = serde_json::from_slice(&first_receipt).unwrap();
    let relative = receipt["directory"].as_str().unwrap();
    let set_dir = hooks_root.join(relative);
    assert!(relative.starts_with("sets/sha256-"), "{relative}");
    assert!(!hooks_root.join("codex-stop.sh").exists());
    assert!(set_dir.join("codex-stop.sh").is_file());

    let verify = command(&hooks_root)
        .args(["hooks", "verify"])
        .output()
        .unwrap();
    assert!(
        verify.status.success(),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let reinstall = command(&hooks_root)
        .args(["hooks", "install"])
        .output()
        .unwrap();
    assert!(reinstall.status.success());
    assert_eq!(fs::read(&receipt_path).unwrap(), first_receipt);

    fs::write(set_dir.join("codex-stop.sh"), "#!/bin/sh\nexit 1\n").unwrap();
    let mismatch = command(&hooks_root)
        .args(["hooks", "verify"])
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    assert!(
        String::from_utf8_lossy(&mismatch.stderr).contains("content mismatch"),
        "{}",
        String::from_utf8_lossy(&mismatch.stderr)
    );
    let no_implicit_repair = command(&hooks_root)
        .args(["hooks", "install"])
        .output()
        .unwrap();
    assert!(!no_implicit_repair.status.success());
    assert_eq!(
        fs::read_to_string(set_dir.join("codex-stop.sh")).unwrap(),
        "#!/bin/sh\nexit 1\n",
        "an immutable set mismatch must never be silently rewritten"
    );
}

#[test]
fn fixture_pin_overrides_an_inherited_hooks_root_without_escape() {
    let tmp = tempfile::tempdir().unwrap();
    let inherited = tmp.path().join("inherited-live-shaped-root");
    let fixture = tmp.path().join("fixture-hooks");
    let mut inherited_command = Command::new(env!("CARGO_BIN_EXE_st2"));
    inherited_command.env("ST_HOOKS", &inherited);

    let output = pin_hooks(inherited_command, &fixture)
        .args(["hooks", "install"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !inherited.exists(),
        "an inherited ST_HOOKS must be overwritten before the subprocess starts"
    );
    assert!(fixture.join("current.json").is_file());
    assert!(fixture.join("sets").is_dir());
}

#[test]
fn codex_materialization_verifies_before_writing_and_renders_a_versioned_path() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::create_dir_all(&workspace).unwrap();
    fs::write(
        &declaration,
        format!(
            r####"agent "worker" {{
  host "h"
  workspace "{}"
  env {{ ST_AGENT "h.worker" }}
  command "exec codex"
  render {{
    json-upsert ".codex/hooks.json" #"""
{{"hooks":{{"Stop":[{{"hooks":[{{"type":"command","command":"$ST_HOOKS/codex-stop.sh"}}]}}]}}}}
"""#
  }}
}}
"####,
            workspace.display()
        ),
    )
    .unwrap();

    let blocked = command(&hooks_root)
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--materialize-only"])
        .output()
        .unwrap();
    assert!(!blocked.status.success());
    assert!(!workspace.join(".codex/hooks.json").exists());
    assert!(
        !hooks_root.exists(),
        "materialization verification must not install hooks"
    );

    assert!(command(&hooks_root)
        .args(["hooks", "install"])
        .status()
        .unwrap()
        .success());
    let materialized = command(&hooks_root)
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--materialize-only"])
        .output()
        .unwrap();
    assert!(
        materialized.status.success(),
        "{}",
        String::from_utf8_lossy(&materialized.stderr)
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(workspace.join(".codex/hooks.json")).unwrap()).unwrap();
    let hook = settings["hooks"]["Stop"][0]["hooks"][0]["command"]
        .as_str()
        .unwrap();
    assert!(hook.starts_with(&format!("{}/sets/sha256-", hooks_root.display())));
    assert!(hook.ends_with("/codex-stop.sh"));
}

#[test]
fn up_once_suppresses_a_new_codex_agent_without_mutating_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    let bin = tmp.path().join("bin");
    let pty_log = tmp.path().join("pty.log");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&bin).unwrap();
    fs::write(
        &declaration,
        format!(
            "agent \"worker\" {{\n  host \"h\"\n  workspace \"{}\"\n  \
             env {{ ST_AGENT \"h.worker\" }}\n  command \"exec codex\"\n  \
             render {{ file \"hook-proof\" \"must-not-write\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1\" = list ]; then printf '[]\\n'; fi\n",
            pty_log.display()
        ),
    );

    let output = command(&hooks_root)
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--once"])
        .env("PATH", &bin)
        .output()
        .unwrap();
    assert!(output.status.success());
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(report.contains("launch suppressed"), "{report}");
    assert!(
        !fs::read_to_string(&pty_log)
            .unwrap_or_default()
            .lines()
            .any(|line| line.starts_with("run ")),
        "the affected Codex pty must not launch"
    );
    assert!(
        !workspace.join("hook-proof").exists(),
        "hook verification must happen before any Codex workspace materialization"
    );
    assert!(
        !hooks_root.exists(),
        "ordinary up must never create or rewrite the hook root"
    );
}

#[test]
fn missing_hooks_do_not_rewrite_or_stop_an_already_live_codex_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    let bin = tmp.path().join("bin");
    let pty_log = tmp.path().join("pty.log");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&bin).unwrap();
    fs::write(
        &declaration,
        format!(
            "agent \"worker\" {{\n  host \"h\"\n  workspace \"{}\"\n  \
             env {{ ST_AGENT \"h.worker\" }}\n  command \"exec codex\"\n  \
             render {{ file \"hook-proof\" \"must-not-write\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    write_executable(
        &bin.join("pty"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1\" = list ]; then printf '[{{\"name\":\"h.worker\",\"status\":\"running\"}}]\\n'; fi\n",
            pty_log.display()
        ),
    );

    let output = command(&hooks_root)
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--once"])
        .env("PATH", &bin)
        .output()
        .unwrap();
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output.status.success(), "{report}");
    assert!(report.contains("materialization deferred"), "{report}");
    assert!(report.contains("adopted (1): worker"), "{report}");
    assert!(
        !workspace.join("hook-proof").exists(),
        "an already-live Codex workspace must remain untouched"
    );
    let pty_actions = fs::read_to_string(&pty_log).unwrap_or_default();
    assert_eq!(
        pty_actions.lines().collect::<Vec<_>>(),
        ["list --json"],
        "the existing Codex session must be adopted without run, kill, or remove"
    );
    assert!(!hooks_root.exists());
}
