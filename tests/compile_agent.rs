//! `st2 compile-agent` is an experimental convenience generator. These tests prove it emits native
//! declarations, leaves workspaces untouched until materialization, and has no removed CLI aliases.

use std::fs;
use std::process::Command;

use st2::discover;

#[test]
fn canonical_hand_authored_examples_parse() {
    for (name, text) in [
        ("codex", include_str!("../examples/native/agent-codex.kdl")),
        (
            "claude",
            include_str!("../examples/native/agent-claude.kdl"),
        ),
    ] {
        let catalog = tempfile::tempdir().unwrap();
        let declaration = catalog.path().join("agents/h/seat/agent.kdl");
        fs::create_dir_all(declaration.parent().unwrap()).unwrap();
        fs::write(&declaration, text).unwrap();
        let found = discover(catalog.path());
        assert!(
            found.errors.is_empty(),
            "{name} compact example failed to parse: {:?}",
            found.errors
        );
        assert_eq!(found.specs.len(), 1);
        assert_eq!(found.specs[0].tasks.len(), 2);
    }
}

#[test]
fn compile_agent_generates_claude_then_materializes_verbatim_persona() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    let state = tmp.path().join("state");
    let persona = tmp.path().join("supervisor.md");
    let persona_body = "# Supervisor\n\nCoordinate the task.\n";
    fs::create_dir_all(&workspace).unwrap();
    fs::write(&persona, persona_body).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("compile-agent")
        .arg(&catalog)
        .args([
            "--role",
            "supervisor",
            "--identity",
            "sup",
            "--host",
            "h",
            "--harness",
            "claude",
            "--supervisor",
            "lead",
            "--extra-arg=--verbose",
        ])
        .arg("--dir")
        .arg(&workspace)
        .arg("--persona")
        .arg(&persona)
        .env("ST_HOOKS", &hooks_root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let declaration = catalog.join("agents/h/sup/agent.kdl");
    let kdl = fs::read_to_string(&declaration).unwrap();
    assert!(kdl.starts_with("// Experimental output from `st2 compile-agent`."));
    assert!(kdl.contains("supervisor \"lead\""));
    assert!(kdl.contains("\"--verbose\""));
    assert!(kdl.contains("argv \"claude\""));
    assert!(!kdl.contains("command #\"exec claude"));
    assert!(kdl.contains("set status busy"));
    assert!(!kdl.contains("ST_SUPERVISOR"));
    assert!(!workspace.join(".st2/PERSONA.md").exists());

    let found = discover(&catalog);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let spec = found
        .specs
        .iter()
        .find(|spec| spec.identity == "sup")
        .unwrap();
    assert_eq!(spec.host.as_deref(), Some("h"));
    assert_eq!(spec.role.as_deref(), Some("supervisor"));
    assert_eq!(spec.supervisor.as_deref(), Some("lead"));
    assert_eq!(
        spec.tasks
            .iter()
            .find(|task| task.name == "agent")
            .unwrap()
            .argv
            .as_ref()
            .unwrap()[0],
        "claude"
    );

    let validate = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("validate")
        .arg(&catalog)
        .env("ST_HOOKS", &hooks_root)
        .output()
        .unwrap();
    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stdout)
    );
    let install = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["hooks", "install"])
        .env("ST_HOOKS", &hooks_root)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    let materialize = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--materialize-only"])
        .env("ST_HOOKS", &hooks_root)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();
    assert!(
        materialize.status.success(),
        "{}",
        String::from_utf8_lossy(&materialize.stderr)
    );

    assert_eq!(
        fs::read_to_string(workspace.join(".st2/PERSONA.md")).unwrap(),
        persona_body
    );
    let loader = fs::read_to_string(workspace.join(".claude/rules/st2.md")).unwrap();
    assert!(loader.contains("PERSONA.md"));
    let bus = fs::read_to_string(workspace.join(".st2/bus.md")).unwrap();
    assert!(bus.contains("## Status discipline"));
    assert!(bus.contains("Set `busy` immediately before actively executing"));
    assert!(bus.contains("does not suppress notifications merely because you are `busy`"));
    assert!(bus.contains("abandoned hold ages to `unknown` after 15 minutes"));
    let hooks: serde_json::Value =
        serde_json::from_slice(&fs::read(workspace.join(".claude/settings.local.json")).unwrap())
            .unwrap();
    assert!(
        hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .ends_with("/claude-session-start.sh")
    );
}

#[test]
fn compile_agent_generates_codex_then_materializes_composed_agents_md() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let state = tmp.path().join("state");
    let hooks_root = tmp.path().join("hooks");
    let persona = tmp.path().join("worker.md");
    let persona_body = "# Codex worker\n\nOwn the scoped repository.\n";
    fs::create_dir_all(&workspace).unwrap();
    fs::write(&persona, persona_body).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("compile-agent")
        .arg(&catalog)
        .args([
            "--role",
            "worker",
            "--identity",
            "worker",
            "--host",
            "h",
            "--harness",
            "codex",
        ])
        .arg("--dir")
        .arg(&workspace)
        .arg("--persona")
        .arg(&persona)
        .env("ST_HOOKS", &hooks_root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!workspace.join("AGENTS.md").exists());

    let install = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["hooks", "install"])
        .env("ST_HOOKS", &hooks_root)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    let materialize = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("up")
        .arg(&catalog)
        .args(["--host", "h", "--materialize-only"])
        .env("ST_HOOKS", &hooks_root)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();
    assert!(
        materialize.status.success(),
        "{}",
        String::from_utf8_lossy(&materialize.stderr)
    );

    let agents = fs::read_to_string(workspace.join("AGENTS.md")).unwrap();
    assert!(agents.starts_with(persona_body));
    assert!(agents.contains("# st2 bus instructions"));
    assert!(agents.contains("## Status discipline"));
    assert!(agents.contains("Set `busy` immediately before actively executing"));
    assert!(agents.contains("does not suppress notifications merely because you are `busy`"));
    assert!(agents.contains("abandoned hold ages to `unknown` after 15 minutes"));
    let hooks: serde_json::Value =
        serde_json::from_slice(&fs::read(workspace.join(".codex/hooks.json")).unwrap()).unwrap();
    assert!(
        hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .ends_with("/codex-session-start.sh")
    );
    assert!(
        hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("/sets/sha256-"),
        "rendered hook references must select one immutable hook set"
    );

    let kdl = fs::read_to_string(catalog.join("agents/h/worker/agent.kdl")).unwrap();
    assert!(kdl.contains("argv \"codex\""));
    assert!(!kdl.contains("exec codex"));
    assert!(!kdl.contains("\"-c\""));
    assert!(!kdl.contains("projects="));
    assert!(kdl.contains("set status busy"));
    assert!(kdl.contains("--dangerously-bypass-hook-trust"));
    assert!(kdl.contains("json-upsert \".codex/hooks.json\""));
}

#[test]
fn compile_agent_opt_in_codex_trust_roundtrips_workspace_bytes_through_toml_and_kdl() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp
        .path()
        .join("workspace with 'single \"# double and \\backslash");
    let hooks_root = tmp.path().join("hooks");
    let persona = tmp.path().join("worker.md");
    fs::create_dir_all(&workspace).unwrap();
    fs::write(&persona, "# Worker\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("compile-agent")
        .arg(&catalog)
        .args([
            "--role",
            "worker",
            "--identity",
            "worker",
            "--host",
            "h",
            "--harness",
            "codex",
            "--trust-workspace",
        ])
        .arg("--dir")
        .arg(&workspace)
        .arg("--persona")
        .arg(&persona)
        .env("ST_HOOKS", &hooks_root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let found = discover(&catalog);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let spec = found
        .specs
        .iter()
        .find(|spec| spec.identity == "worker")
        .unwrap();
    let workspace_text = workspace.to_str().unwrap();
    assert_eq!(spec.workspace.as_deref(), Some(workspace_text));
    let argv = spec
        .tasks
        .iter()
        .find(|task| task.name == "agent")
        .and_then(|task| task.argv.as_deref())
        .unwrap();
    let report = st2::validate::validate(&catalog);
    assert_eq!(
        report.errors(),
        0,
        "generated output must validate: {:?}",
        report.issues
    );

    assert_eq!(argv.first().map(String::as_str), Some("codex"));
    assert_eq!(argv.get(1).map(String::as_str), Some("-c"));
    let config = argv.get(2).unwrap();
    let config = config.parse::<toml::Table>().unwrap();
    let projects = config["projects"].as_table().unwrap();
    let (key, project) = projects.iter().next().unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(key.as_bytes(), workspace_text.as_bytes());
    assert_eq!(
        project["trust_level"].as_str(),
        Some("trusted"),
        "argv: {argv:?}"
    );
}

#[test]
fn compile_agent_rejects_workspace_trust_for_non_codex_before_writing() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let persona = tmp.path().join("worker.md");
    fs::create_dir_all(&workspace).unwrap();
    fs::write(&persona, "# Worker\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("compile-agent")
        .arg(&catalog)
        .args([
            "--identity",
            "worker",
            "--host",
            "h",
            "--harness",
            "claude",
            "--trust-workspace",
        ])
        .arg("--dir")
        .arg(&workspace)
        .arg("--persona")
        .arg(&persona)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--trust-workspace requires --harness codex"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !catalog.exists(),
        "a rejected trust/harness combination wrote catalog output"
    );
}

#[test]
fn removed_generator_aliases_are_unknown_commands() {
    let tmp = tempfile::tempdir().unwrap();
    let hooks_root = tmp.path().join("hooks");
    let removed = [
        ["a", "dd"].concat(),
        ["com", "pile"].concat(),
        ["ren", "der"].concat(),
        ["build", "-agent"].concat(),
        ["render", "-agent"].concat(),
        ["re", "move"].concat(),
    ];
    for removed in removed {
        let output = Command::new(env!("CARGO_BIN_EXE_st2"))
            .arg(&removed)
            .env("ST_HOOKS", &hooks_root)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{removed} unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("unrecognized subcommand"),
            "{removed}: {stderr}"
        );
    }
}
