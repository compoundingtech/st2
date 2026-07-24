//! M3 NEUTRALITY gate — the swap-safety proof. Render the SAME agent through convoy's renderer AND
//! `st2 render`, into throwaway temp dirs (nothing touches a real repo), and DIFF the wiring. This is
//! the load-bearing guarantee of M3: a rendered agent must boot + wire identically to a convoy one.
//! By-construction is not sufficient — a single transcription slip (a flag, a path segment, an
//! ordering) survives it and only a diff catches it — so this is a real test, not a claim.
//!
//! Byte-identical: the `exec claude …` command, the ding command (modulo `$CATALOG`), `ST_AGENT`, the
//! `.claude/rules/convoy.md` loader, `PERSONA.md`, `DING-BUS.md`. The one honest non-byte difference is
//! st2's `$CATALOG/…` indirection (R11) vs convoy's absolute `<net>/…` — asserted as equivalence after
//! substituting `$CATALOG` for the net dir. Deferred (extend this diff when they land): the
//! `settings.local.json` boot hooks and the M3.2 permissions `.claude/` artifacts.
//!
//! Requires `convoy` on PATH (like the pty gate). Missing convoy is a HARD FAILURE — a neutrality gate
//! that silently skips proves nothing — unless `ST2_ALLOW_CONVOY_SKIP` is set on a box without convoy.

use std::fs;
use std::path::Path;
use std::process::Command;

use st2::discover;
use st2::render::{parse_ir, render_agent};

fn convoy_available() -> bool {
    Command::new("convoy").arg("--help").output().map(|o| o.status.success()).unwrap_or(false)
}

fn convoy_gate() -> bool {
    if convoy_available() {
        return true;
    }
    assert!(
        std::env::var_os("ST2_ALLOW_CONVOY_SKIP").is_some(),
        "render neutrality gate: `convoy` is not on PATH, so st2/convoy render-neutrality is UNPROVEN. \
         Install convoy (required in CI/gating), or set ST2_ALLOW_CONVOY_SKIP=1 to skip on a box \
         without it."
    );
    eprintln!("SKIP render_neutrality: `convoy` not on PATH (ST2_ALLOW_CONVOY_SKIP set)");
    false
}

/// The command + env + ding, extracted from convoy's rendered `pty.toml`.
struct ConvoyWiring {
    command: String,
    st_agent: String,
    st_root: String,
    pty_root: String,
    ding_command: String,
    /// The on-disk pty session ids — st2's reconcile ADOPTION KEY. Must match or a runner swap
    /// (convoy→st2) cold-launches a duplicate instead of adopting the live session.
    agent_id: String,
    ding_id: String,
}

fn read_convoy_wiring(pty_toml: &Path) -> ConvoyWiring {
    let v: toml::Value = toml::from_str(&fs::read_to_string(pty_toml).unwrap()).unwrap();
    let claude = &v["sessions"]["claude"];
    let ding = &v["sessions"]["ding"];
    let s = |x: &toml::Value| x.as_str().unwrap().to_string();
    ConvoyWiring {
        command: s(&claude["command"]),
        st_agent: s(&claude["env"]["ST_AGENT"]),
        st_root: s(&claude["env"]["ST_ROOT"]),
        pty_root: s(&claude["env"]["PTY_ROOT"]),
        ding_command: s(&ding["command"]),
        agent_id: s(&claude["id"]),
        ding_id: s(&ding["id"]),
    }
}

#[test]
fn st2_render_is_behavior_neutral_with_convoy() {
    if !convoy_gate() {
        return;
    }

    // ── shared inputs: the SAME agent, expressed for each renderer ──────────────────────────────
    let tmp = tempfile::tempdir().unwrap();
    let personas = tmp.path().join("personas");
    fs::create_dir_all(&personas).unwrap();
    let persona_body = "# Worker persona\nDo the work you are handed.\n";
    fs::write(personas.join("worker.md"), persona_body).unwrap();

    // ── convoy render (isolated: temp net + temp workspace + temp personas) ─────────────────────
    let net = tmp.path().join("net");
    let cws = tmp.path().join("convoy-ws");
    fs::create_dir_all(net.join("catalog")).unwrap();
    fs::create_dir_all(&cws).unwrap();
    fs::write(
        net.join("catalog").join("fabric-claude.toml"),
        format!(
            "identity = \"fabric-claude\"\nrole = \"worker\"\nhost = \"silber\"\nworkspace = \"{}\"\n\
             harness = \"claude\"\nmodel = \"opus\"\ntransport = \"ding\"\n",
            cws.display()
        ),
    )
    .unwrap();
    let out = Command::new("convoy")
        .args(["render", "fabric-claude", "--network"])
        .arg(&net)
        .arg("--dir")
        .arg(&cws)
        .env("CONVOY_PERSONAS_DIR", &personas)
        .output()
        .unwrap();
    assert!(out.status.success(), "convoy render failed: {}", String::from_utf8_lossy(&out.stderr));
    let convoy = read_convoy_wiring(&cws.join(".convoy/pty.toml"));

    // ── st2 render the same agent (temp catalog + temp workspace) ───────────────────────────────
    let catalog = tmp.path().join("catalog");
    let sws = tmp.path().join("st2-ws");
    fs::create_dir_all(&sws).unwrap();
    let ir = format!(
        r#"agent "fabric-claude" {{ host "silber"; role "worker"; harness "claude"; model "opus"; workspace "{}" }}"#,
        sws.display()
    );
    let agents = parse_ir(&ir).unwrap();
    render_agent(&agents[0], &catalog, &personas).unwrap();

    // Pull st2's wiring back out through the runner's own discovery (what `st2 up` would consume).
    let found = discover(&catalog);
    let spec = found.specs.iter().find(|s| s.identity == "fabric-claude").unwrap();
    let agent = spec.tasks.iter().find(|t| t.name == "agent").unwrap();
    let ding = spec.tasks.iter().find(|t| t.name == "ding").unwrap();
    let st2_command = agent.command.clone().unwrap();
    let st2_ding = ding.command.clone().unwrap();

    // ── DIFF ────────────────────────────────────────────────────────────────────────────────────
    // The command and ST_AGENT are BYTE-IDENTICAL (no paths).
    assert_eq!(st2_command, convoy.command, "agent command must be byte-identical to convoy");
    assert_eq!(agent.env.get("ST_AGENT").unwrap(), &convoy.st_agent, "ST_AGENT must match convoy");

    // Env/ding paths: convoy is absolute `<net>/…`; st2 is `$CATALOG/…`. Assert equivalence by
    // substituting `$CATALOG` for convoy's net dir — same relative wiring.
    let net_str = net.to_string_lossy();
    assert_eq!(
        agent.env.get("ST_ROOT").unwrap().as_str(),
        convoy.st_root.replace(net_str.as_ref(), "$CATALOG"),
        "ST_ROOT must equal convoy's (modulo $CATALOG)"
    );
    assert_eq!(
        agent.env.get("PTY_ROOT").unwrap().as_str(),
        convoy.pty_root.replace(net_str.as_ref(), "$CATALOG"),
        "PTY_ROOT must equal convoy's (modulo $CATALOG)"
    );
    assert_eq!(
        st2_ding,
        convoy.ding_command.replace(net_str.as_ref(), "$CATALOG"),
        "ding command must equal convoy's (modulo $CATALOG)"
    );

    // ADOPTION KEY — the pty session id st2's reconcile matches against `pty list`. It MUST equal
    // convoy's on-disk session id (no paths, byte-identical), or a convoy→st2 runner swap finds its
    // declared id absent and COLD-LAUNCHES a duplicate instead of adopting the live session (the
    // sub-swap-1 "double-run" failure). This is the field the rest of the neutrality diff does not
    // cover — the `displayName` (`<host>.<short>-<harness>`) is human-facing and NOT the key.
    assert_eq!(
        agent.id.as_deref(), Some(convoy.agent_id.as_str()),
        "agent session id (adoption key) must equal convoy's — else st2 up won't adopt the live agent"
    );
    assert_eq!(
        ding.id.as_deref(), Some(convoy.ding_id.as_str()),
        "ding session id (adoption key) must equal convoy's — else st2 up won't adopt the live ding"
    );

    // Overlay files must be BYTE-identical (no paths inside these).
    let read = |ws: &Path, rel: &str| fs::read_to_string(ws.join(rel)).unwrap();
    assert_eq!(read(&sws, ".convoy/PERSONA.md"), read(&cws, ".convoy/PERSONA.md"), "PERSONA.md differs");
    assert_eq!(read(&sws, ".convoy/DING-BUS.md"), read(&cws, ".convoy/DING-BUS.md"), "DING-BUS.md differs");
    assert_eq!(
        read(&sws, ".claude/rules/convoy.md"),
        read(&cws, ".claude/rules/convoy.md"),
        ".claude/rules/convoy.md loader differs"
    );

    // settings.local.json boot hooks: structurally equal (JSON key order is irrelevant; the hook
    // COMMAND strings must match — both resolve the same smalltalk hook scripts + ST_BIN).
    let sj: serde_json::Value = serde_json::from_str(&read(&sws, ".claude/settings.local.json")).unwrap();
    let cj: serde_json::Value = serde_json::from_str(&read(&cws, ".claude/settings.local.json")).unwrap();
    assert_eq!(sj, cj, "settings.local.json boot hooks differ from convoy");

    // Permissions dimension (M3.2): this agent declares NO permissions block, so NEITHER renderer
    // emits a permissions `.claude/settings.json` — the agent stays open/bypassPermissions. (A
    // declared block is net-new to st2; convoy has none, so there is nothing to diff there.)
    assert!(!sws.join(".claude/settings.json").exists(), "st2 must not clamp a no-block agent");
    assert!(!cws.join(".claude/settings.json").exists(), "convoy emits no permissions settings.json");
}
