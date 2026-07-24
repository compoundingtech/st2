//! M3.1 integration: the render→run round-trip. Render an IR agent, then prove the rendered catalog
//! is exactly what st2 RUN accepts — `discover` parses it into a valid, RUNNABLE spec carrying the
//! behavior-neutral wiring — and that the persona overlay lands in the agent's workspace. This is the
//! M3.1 acceptance (render → st2 up → agent alive, proven at the discovery boundary the runner uses).

use std::fs;
use std::process::Command;

use st2::discover;
use st2::render::{parse_ir, render_agent};

#[test]
fn canonical_compact_examples_parse() {
    for (name, text) in [
        (
            "codex",
            include_str!("../examples/format/agent-codex.kdl"),
        ),
        (
            "claude",
            include_str!("../examples/format/agent-claude.kdl"),
        ),
    ] {
        let catalog = tempfile::tempdir().unwrap();
        let declaration = catalog
            .path()
            .join("agents/<host>/<identity>/agent.kdl");
        fs::create_dir_all(declaration.parent().unwrap()).unwrap();
        fs::write(&declaration, text).unwrap();
        let found = discover(catalog.path());
        assert!(
            found.errors.is_empty(),
            "{name} compact example failed to parse: {:?}",
            found.errors
        );
        assert_eq!(
            found.specs.len(),
            1,
            "{name} example should declare one agent"
        );
        assert_eq!(
            found.specs[0].tasks.len(),
            2,
            "{name} example should lower to command + ding tasks"
        );
    }
}

#[test]
fn render_produces_a_runnable_catalog_and_workspace_overlay() {
    let tmp = tempfile::tempdir().unwrap();
    let ir_dir = tmp.path().join("ir");
    let catalog = tmp.path().join("catalog");
    let ws = tmp.path().join("ws"); // the agent's workspace
    let personas = ir_dir.join("personas");
    fs::create_dir_all(&personas).unwrap();
    fs::create_dir_all(&ws).unwrap();
    fs::write(
        personas.join("worker.md"),
        "# Worker persona\nDo the work.\n",
    )
    .unwrap();

    let ir_text = format!(
        r#"
        agent "fabric-claude" {{
          host "silber"
          role "worker"
          harness "claude"
          model "opus"
          workspace "{ws}"
          supervisor "cos"
        }}
        "#,
        ws = ws.display()
    );
    let agents = parse_ir(&ir_text).unwrap();
    render_agent(&agents[0], &catalog, &personas).unwrap();

    // 1) discover parses the rendered catalog into a valid, RUNNABLE spec (what `st2 up` consumes).
    let found = discover(&catalog);
    assert!(
        found.errors.is_empty(),
        "rendered catalog has discovery errors: {:?}",
        found.errors
    );
    let spec = found
        .specs
        .iter()
        .find(|s| s.identity == "fabric-claude")
        .expect("rendered agent is discovered");
    assert!(spec.is_runnable(), "rendered agent must be runnable");
    assert_eq!(spec.host.as_deref(), Some("silber"));

    // 2) the agent pty task carries convoy's exact command + the $CATALOG-rooted bus env.
    let agent = spec
        .tasks
        .iter()
        .find(|t| t.name == "agent")
        .expect("agent task");
    let cmd = agent.command.as_deref().unwrap();
    assert!(
        cmd.contains("exec claude --permission-mode bypassPermissions --model 'opus'"),
        "unexpected agent command: {cmd}"
    );
    assert_eq!(
        agent.env.get("ST_AGENT").map(String::as_str),
        Some("silber.fabric-claude")
    );
    assert_eq!(
        agent.env.get("ST_ROOT").map(String::as_str),
        Some("$CATALOG/smalltalk")
    );
    assert_eq!(
        agent.env.get("PTY_ROOT").map(String::as_str),
        Some("$CATALOG/pty")
    );

    // 3) the ding task targets the smalltalk bus (session id vs bus id are distinct).
    let ding = spec
        .tasks
        .iter()
        .find(|t| t.name == "ding")
        .expect("ding task");
    assert_eq!(
        ding.command.as_deref(),
        Some("st ding silber.fabric --identity silber.fabric-claude --root $CATALOG/smalltalk")
    );

    // 4) the persona overlay landed in the WORKSPACE (Claude Code loads .claude/rules from cwd).
    assert_eq!(
        fs::read_to_string(ws.join(".convoy/PERSONA.md")).unwrap(),
        "# Worker persona\nDo the work.\n"
    );
    assert!(
        fs::read_to_string(ws.join(".claude/rules/convoy.md"))
            .unwrap()
            .contains("@../../.convoy/PERSONA.md")
    );

    // 5) the smalltalk bus member folder exists (behavior-neutral bus presence).
    assert!(
        catalog
            .join("smalltalk/silber.fabric-claude/inbox")
            .is_dir()
    );
    assert!(
        catalog
            .join("smalltalk/silber.fabric-claude/archive")
            .is_dir()
    );

    // 6) NO permissions block → NO .claude/settings.json (open/bypassPermissions, the swap-safe
    //    default; behavior-neutral with convoy which renders no permissions).
    assert!(
        !ws.join(".claude/settings.json").exists(),
        "no-block agent must not clamp permissions"
    );
}

/// A DECLARED permissions block renders the SIMPLE tool-level mechanism (M3.2): a PreToolUse
/// `permissions.sh` that BLOCKS disallowed tools (deny, never prompt), registered by a hook-only
/// `settings.json`. The full-model seam (path scopes, shims, ask-routing) is parsed, not yet rendered.
#[test]
fn render_permissions_block_emits_a_tool_gate_hook() {
    let tmp = tempfile::tempdir().unwrap();
    let ir_dir = tmp.path().join("ir");
    let catalog = tmp.path().join("catalog");
    let ws = tmp.path().join("ws");
    let personas = ir_dir.join("personas");
    fs::create_dir_all(&personas).unwrap();
    fs::create_dir_all(&ws).unwrap();
    fs::write(personas.join("worker.md"), "# w\n").unwrap();

    let ir = format!(
        r#"agent "x-claude" {{
            host "h"; role "worker"; harness "claude"; workspace "{ws}"
            permissions {{
              tools {{ allow "Read" "Edit" "Grep"; deny "WebSearch" }}
              spawn #false
              shim "gh" {{ scope-repo "$WORKSPACE" }}
            }}
          }}"#,
        ws = ws.display()
    );
    let agents = parse_ir(&ir).unwrap();
    render_agent(&agents[0], &catalog, &personas).unwrap();

    // settings.json ONLY registers the hook — no permissions allow/ask block (which would prompt).
    let v: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(ws.join(".claude/settings.json")).unwrap())
            .unwrap();
    assert_eq!(v["hooks"]["PreToolUse"][0]["hooks"][0]["type"], "command");
    assert!(
        v.get("permissions").is_none(),
        "no allowlist that would prompt an autonomous agent"
    );

    // The hook enforces allow/deny (spawn #false → deny Agent) and blocks WITHOUT prompting.
    let hook = fs::read_to_string(ws.join(".claude/hooks/permissions.sh")).unwrap();
    assert!(hook.contains(r#"ALLOW="Read Edit Grep""#));
    assert!(hook.contains("WebSearch") && hook.contains("Agent")); // deny + spawn#false
    assert!(hook.contains(r#""permissionDecision":"deny""#) && !hook.contains(r#""ask""#));

    // Deferred: no shim bin/, no PATH env (full model, not now).
    assert!(!ws.join("bin/gh").exists(), "shims are the deferred seam");
    let spec = discover(&catalog)
        .specs
        .into_iter()
        .find(|s| s.identity == "x-claude")
        .unwrap();
    let agent = spec.tasks.iter().find(|t| t.name == "agent").unwrap();
    assert!(
        !agent.env.contains_key("PATH"),
        "no shim PATH yet (deferred)"
    );
}

/// The rendered PreToolUse hook actually ENFORCES the tool policy when run: an allowed tool passes,
/// an unlisted/denied/spawn tool is blocked via `deny` (never a prompt — exit 0), and every call is
/// recorded. This is the useful-today guarantee: you can restrict an autonomous agent's tools and it
/// never hangs.
#[test]
fn rendered_permissions_hook_enforces_the_tool_policy_when_run() {
    let tmp = tempfile::tempdir().unwrap();
    let ir_dir = tmp.path().join("ir");
    let catalog = tmp.path().join("catalog");
    let ws = tmp.path().join("ws");
    let personas = ir_dir.join("personas");
    fs::create_dir_all(&personas).unwrap();
    fs::create_dir_all(&ws).unwrap();
    fs::write(personas.join("worker.md"), "# w\n").unwrap();

    let ir = format!(
        r#"agent "x-claude" {{ host "h"; role "worker"; harness "claude"; workspace "{ws}"
             permissions {{ tools {{ allow "Read" "Edit"; deny "WebSearch" }}; spawn #false }} }}"#,
        ws = ws.display()
    );
    render_agent(&parse_ir(&ir).unwrap()[0], &catalog, &personas).unwrap();
    let hook = ws.join(".claude/hooks/permissions.sh");

    // Run the hook with a tool call on stdin; return (stdout, exit_code).
    let run = |tool: &str| -> (String, i32) {
        let mut c = Command::new(&hook);
        c.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut child = c.spawn().unwrap();
        use std::io::Write;
        write!(
            child.stdin.take().unwrap(),
            r#"{{"tool_name":"{tool}","tool_input":{{}}}}"#
        )
        .unwrap();
        let out = child.wait_with_output().unwrap();
        (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            out.status.code().unwrap_or(-1),
        )
    };

    // Allowed → passes through (no decision), exit 0.
    let (allowed, code) = run("Read");
    assert_eq!(code, 0);
    assert!(
        !allowed.contains("permissionDecision"),
        "allowed tool must pass: {allowed}"
    );

    // Not in the allow-list, an explicit deny, and the spawn tool → all BLOCKED via deny, exit 0
    // (never a prompt — an autonomous pty can't answer one).
    for blocked in ["Bash", "WebSearch", "Agent"] {
        let (out, code) = run(blocked);
        assert_eq!(
            code, 0,
            "{blocked}: hook must exit 0 (deny via JSON, not a prompt)"
        );
        assert!(
            out.contains(r#""permissionDecision":"deny""#),
            "{blocked} must be denied: {out}"
        );
    }

    // Every call was recorded to the events log.
    assert_eq!(
        fs::read_to_string(ws.join(".convoy/events/pretooluse.jsonl"))
            .unwrap()
            .lines()
            .count(),
        4
    );
}

/// `st2 compile-agent` GENERATES one compact agent declaration + vendored templates. The workspace
/// stays untouched until the materialize phase executes the declaration.
#[test]
fn render_agent_from_flags_produces_a_runnable_catalog_with_verbatim_persona() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();
    // A composed persona FILE, as an eval's compose-persona.sh would write it (SHA-pinned content).
    let persona = tmp.path().join("gb-sup.md");
    let persona_body = "# gb-sup — eval SUPERVISOR\n\nYou coordinate the debug task. (composed)\n";
    fs::write(&persona, persona_body).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("render-agent")
        .arg(&catalog)
        .args([
            "--role",
            "supervisor",
            "--identity",
            "gb-sup",
            "--host",
            "gbpilot",
            "--harness",
            "claude",
        ])
        .arg("--dir")
        .arg(&ws)
        .arg("--persona")
        .arg(&persona)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "render-agent failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The rendered catalog is RUNNABLE — discover parses it into a valid spec at the runner boundary.
    let found = discover(&catalog);
    assert!(
        found.errors.is_empty(),
        "discover errors: {:?}",
        found.errors
    );
    let spec = found
        .specs
        .iter()
        .find(|s| s.identity == "gb-sup")
        .expect("gb-sup rendered");
    assert_eq!(spec.host.as_deref(), Some("gbpilot"));

    assert!(
        !ws.join(".st2/PERSONA.md").exists(),
        "compile-agent must not materialize the workspace"
    );
    let validate = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["validate"])
        .arg(&catalog)
        .output()
        .unwrap();
    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stdout)
    );
    let materialize = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up"])
        .arg(&catalog)
        .args(["--host", "gbpilot", "--materialize-only"])
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .output()
        .unwrap();
    assert!(
        materialize.status.success(),
        "{}",
        String::from_utf8_lossy(&materialize.stderr)
    );

    // The composed persona FILE is vendored then installed VERBATIM by the declarative copy.
    let overlay = fs::read_to_string(ws.join(".st2/PERSONA.md")).unwrap();
    assert_eq!(overlay, persona_body, "persona was not installed verbatim");
    let loader = fs::read_to_string(ws.join(".claude/rules/st2.md")).unwrap();
    assert!(
        loader.contains("PERSONA.md"),
        "loader does not @-import the persona overlay"
    );
    let hooks: serde_json::Value =
        serde_json::from_slice(&fs::read(ws.join(".claude/settings.local.json")).unwrap()).unwrap();
    assert!(
        hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .ends_with("/claude-session-start.sh")
    );
}

/// Codex reads AGENTS.md from cwd, so compile-agent pre-composes the persona and st2 bus contract
/// into one catalog-owned template and declares the current Codex lifecycle hooks.
#[test]
fn render_agent_codex_installs_the_persona_as_verbatim_agents_md() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();
    let persona = tmp.path().join("cx.md");
    let persona_body = "# codex seat\n\nYou own ZORPKIT-9000. Codeword FALCONRIDGE.\n";
    fs::write(&persona, persona_body).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("render-agent")
        .arg(&catalog)
        .args([
            "--role",
            "worker",
            "--identity",
            "cx",
            "--host",
            "h",
            "--harness",
            "codex",
        ])
        .arg("--dir")
        .arg(&ws)
        .arg("--persona")
        .arg(&persona)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "render-agent codex failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(!ws.join("AGENTS.md").exists());
    let materialize = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up"])
        .arg(&catalog)
        .args(["--host", "h", "--materialize-only"])
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .output()
        .unwrap();
    assert!(
        materialize.status.success(),
        "{}",
        String::from_utf8_lossy(&materialize.stderr)
    );

    let agents = fs::read_to_string(ws.join("AGENTS.md")).unwrap();
    assert!(agents.starts_with(persona_body));
    assert!(agents.contains("# st2 bus instructions"));
    let hooks: serde_json::Value =
        serde_json::from_slice(&fs::read(ws.join(".codex/hooks.json")).unwrap()).unwrap();
    assert!(
        hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .ends_with("/codex-session-start.sh")
    );

    // The compact catalog runs Codex with both unattended runtime and vetted-hook trust bypasses.
    let kdl = fs::read_to_string(catalog.join("agents/h/cx/agent.kdl")).unwrap();
    assert!(kdl.contains("exec codex"), "codex agent should exec codex");
    assert!(kdl.contains("--dangerously-bypass-hook-trust"));
    assert!(kdl.contains("json-upsert \".codex/hooks.json\""));
}
