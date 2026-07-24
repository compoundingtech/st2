//! RENDER (M3) — compile a higher-level IR into the concrete VRS catalog st2 RUN executes.
//!
//! M3.1 is the behavior-neutral SPINE: an st2-rendered agent boots and wires IDENTICALLY to a
//! convoy-rendered one — same `exec claude` command, same `ST_AGENT`/`ST_ROOT`/`PTY_ROOT` env, same
//! `st ding` on the smalltalk bus, same persona-via-`.claude/rules` overlay. Only the manifest format
//! (`agent.kdl`) and the runner (st2) change; the agent's runtime behavior does not. That is what
//! lets M3 swap the *renderer* without touching the fleet.
//!
//! Wiring is grounded in convoy's renderer (the reference). Deferred: permissions → `.claude/`
//! (M3.2), the vendored `DING-BUS.md` template + the `settings.local.json` hooks (additive overlay,
//! needs convoy's exact template strings for byte-neutrality), and the smalltalk→native-bus cutover
//! (M6). This module emits NO permissions.

use std::fs;
use std::path::Path;

use kdl::KdlDocument;

/// Convoy's boot prompt for every non-CoS role (verbatim; apostrophe-free by design so it is
/// single-quote-safe on the command line).
const BOOT_WORKER: &str = "You just cold-started. Run your boot ritual now: set your st status to available and drain your inbox (read, act on, and archive each message). Then stand by for work delivered via ding.";
/// Convoy's boot prompt for `chief-of-staff` (verbatim).
const BOOT_COS: &str = "You just cold-started. Run your boot ritual (set status available, drain inbox). If this is a fresh network with no populated private cos repo, run your first-run interview now, per your persona.";

/// The harness-suffix set convoy strips to derive the pty session id from the bus identity.
const HARNESS_SUFFIXES: &[&str] = &["-claude", "-codex", "-opencode", "-pi"];

/// The ding-mode bus instructions — vendored BYTE-FOR-BYTE from convoy's rendered `DING-BUS.md`, so a
/// rendered agent reads identical operating instructions (behavior-neutral). The neutrality test
/// diffs this against convoy's fresh render; a drift there means update this vendored copy.
const DING_BUS_MD: &str = include_str!("../templates/DING-BUS.md");

/// A parsed IR agent declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrAgent {
    /// The agent block name — the full identity, e.g. `fabric-claude` (keeps the harness suffix).
    pub identity: String,
    pub host: String,
    pub role: String,
    pub harness: String,
    pub model: Option<String>,
    /// Persona file basename — materialized from `<personas>/<persona>.md`.
    pub persona: String,
    pub workspace: String,
    pub supervisor: Option<String>,
    /// A declared `permissions{}` block, or `None`. **None = open (`bypassPermissions`)** — the
    /// swap-safe default: an autonomous pty can't answer a permission prompt, so nothing is clamped
    /// (M3.2). A declared block renders allow/ask/deny + hook + shims.
    pub permissions: Option<IrPermissions>,
    /// Extra harness (claude/codex) CLI args spliced into the agent command BEFORE the boot prompt —
    /// e.g. `--plugin-dir <dir>`. The st2 analog of convoy's per-seat pty.toml command edit; empty for
    /// a normal agent (so the command stays byte-neutral with convoy). Each is one argv token,
    /// shell-quoted at render.
    pub extra_args: Vec<String>,
}

/// A declared permissions block.
///
/// **M3.2 renders the SIMPLE, useful-today thing: tool-level allow/deny, ENFORCED BY THE HOOK.** The
/// agent stays `bypassPermissions` (autonomous — it must never hang on a prompt), and the PreToolUse
/// hook BLOCKS a disallowed tool via `permissionDecision: deny` (block, never prompt), so both an
/// allow-list and a deny-list are safe. `tools_allow`/`tools_deny` + `spawn` are what M3.2 renders.
///
/// **TODO — the full model EXTENDS this later (not built now; seam left):** the path read/write
/// scopes (`read`/`write`), the top-level `ask` path scopes + ask→human/supervisor routing (spec
/// DQ5), curated shims (`bin/gh`), and a deny-floor / restrictive `defaultMode` for true
/// bounded-by-default. Those fields are PARSED here (the seam) but not yet rendered.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IrPermissions {
    /// `tools { allow … }` — tool names allowed (allow-list: anything not listed is blocked). RENDERED.
    pub tools_allow: Vec<String>,
    /// `tools { deny … }` — tool names blocked. RENDERED.
    pub tools_deny: Vec<String>,
    /// `spawn #false` denies the subagent tool. Default `true`. RENDERED (folds into deny).
    pub spawn: bool,
    /// `tools { ask … }` — SEAM (ask-routing / DQ5), not yet rendered.
    pub tools_ask: Vec<String>,
    /// `read { … }` path scopes — SEAM, not yet rendered.
    pub read: Vec<String>,
    /// `write { … }` path scopes — SEAM, not yet rendered.
    pub write: Vec<String>,
    /// top-level `ask { … }` path scopes — SEAM, not yet rendered.
    pub ask_paths: Vec<String>,
    /// `shim "<tool>" { scope-repo … }` — SEAM (curated shims), not yet rendered.
    pub shims: Vec<Shim>,
}

/// A curated command shim — SEAM for the full model; not yet rendered (M3.2 defers shims).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shim {
    pub name: String,
    pub scope_repo: Option<String>,
}

impl IrAgent {
    /// The pty session id / catalog agent dir segment: `<host>.<agentShort>` (harness suffix stripped).
    pub fn session_id(&self) -> String {
        format!("{}.{}", self.host, agent_short(&self.identity))
    }

    /// The bus id `ST_AGENT`: `<host>.<identity>` (keeps the harness suffix).
    pub fn bus_id(&self) -> String {
        format!("{}.{}", self.host, self.identity)
    }

    /// The `pty "agent"` command — convoy's exact `exec <harness> …` wiring. `bypassPermissions` is
    /// the standing posture for every role; the model rides through verbatim (no alias translation).
    pub fn agent_command(&self) -> anyhow::Result<String> {
        let boot = boot_prompt(&self.role);
        let model = self.model.as_deref().map(|m| format!(" --model '{m}'")).unwrap_or_default();
        // Extra harness args (e.g. `--plugin-dir <dir>`) go BEFORE the boot prompt, shell-quoted so a
        // path with spaces/metacharacters stays one argv token. Empty → byte-neutral with convoy.
        let extra: String = self.extra_args.iter().map(|a| format!(" {}", shell_single_quote(a))).collect();
        match self.harness.as_str() {
            "claude" => Ok(format!("exec claude --permission-mode bypassPermissions{model}{extra} '{boot}'")),
            // codex takes no --permission-mode; it uses bypass-approvals+sandbox instead.
            "codex" => Ok(format!("exec codex --dangerously-bypass-approvals-and-sandbox{model}{extra} '{boot}'")),
            other => anyhow::bail!("render: harness '{other}' not supported yet (M3.1: claude, codex)"),
        }
    }

    /// The `exec "ding"` command — `st ding` on the smalltalk bus (behavior-neutral; the native
    /// `st2 ding` / unified-inbox cutover is M6). `$CATALOG` is expanded by st2 at spawn.
    pub fn ding_command(&self) -> String {
        format!("st ding {} --identity {} --root $CATALOG/smalltalk", self.session_id(), self.bus_id())
    }
}

/// POSIX single-quote a token so it survives `sh -c` as one argv element (a path with spaces/`$`/`;`
/// stays intact): wrap in `'…'`, and render any embedded `'` as `'\''`.
fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Strip a trailing harness suffix (`-claude`/`-codex`/…) to get the session-name form of an identity.
fn agent_short(identity: &str) -> &str {
    for suf in HARNESS_SUFFIXES {
        if let Some(stem) = identity.strip_suffix(suf) {
            return stem;
        }
    }
    identity
}

fn boot_prompt(role: &str) -> &'static str {
    if role == "chief-of-staff" { BOOT_COS } else { BOOT_WORKER }
}

/// Parse an IR document (KDL) into its agent declarations. A `permissions{}` child is ignored (M3.2).
pub fn parse_ir(text: &str) -> anyhow::Result<Vec<IrAgent>> {
    let doc = KdlDocument::parse(text).map_err(|e| anyhow::anyhow!("IR KDL parse error: {e}"))?;
    let mut out = Vec::new();
    for node in doc.nodes() {
        if node.name().value() != "agent" {
            continue;
        }
        let identity = node
            .get(0)
            .and_then(|v| v.as_string())
            .map(String::from)
            .ok_or_else(|| anyhow::anyhow!("IR: an `agent` node needs a name (the identity)"))?;

        let (mut host, mut role, mut harness, mut model, mut persona, mut workspace, mut supervisor) =
            (None, None, None, None, None, None, None);
        let mut permissions = None;
        if let Some(children) = node.children() {
            for c in children.nodes() {
                let val = c.get(0).and_then(|v| v.as_string()).map(String::from);
                match c.name().value() {
                    "host" => host = val,
                    "role" => role = val,
                    "harness" => harness = val,
                    "model" => model = val,
                    "persona" => persona = val,
                    "workspace" => workspace = val,
                    "supervisor" => supervisor = val,
                    "permissions" => permissions = Some(parse_permissions(c)),
                    _ => {}
                }
            }
        }
        let need = |f: Option<String>, name: &str| {
            f.ok_or_else(|| anyhow::anyhow!("IR agent '{identity}': missing required field `{name}`"))
        };
        let role = need(role, "role")?;
        // persona defaults to the role's persona file when unspecified (convoy: baseFile(role)).
        let persona = persona.unwrap_or_else(|| role.clone());
        out.push(IrAgent {
            host: need(host, "host")?,
            harness: need(harness, "harness")?,
            workspace: need(workspace, "workspace")?,
            role,
            model,
            persona,
            supervisor,
            permissions,
            identity,
            extra_args: Vec::new(), // IR has no extra-args node today; the imperative render-agent sets them
        });
    }
    Ok(out)
}

/// Parse a `permissions{}` block. `spawn` defaults to `true` (allowed).
fn parse_permissions(node: &kdl::KdlNode) -> IrPermissions {
    let mut p = IrPermissions { spawn: true, ..Default::default() };
    let Some(children) = node.children() else { return p };
    for c in children.nodes() {
        match c.name().value() {
            "tools" => {
                if let Some(tc) = c.children() {
                    for t in tc.nodes() {
                        let names = positional_strings(t);
                        match t.name().value() {
                            "allow" => p.tools_allow = names,
                            "deny" => p.tools_deny = names,
                            "ask" => p.tools_ask = names,
                            _ => {}
                        }
                    }
                }
            }
            "read" => p.read = path_scopes(c),
            "write" => p.write = path_scopes(c),
            "ask" => p.ask_paths = path_scopes(c),
            "spawn" => p.spawn = c.get(0).and_then(|v| v.as_bool()).unwrap_or(true),
            "shim" => {
                if let Some(name) = c.get(0).and_then(|v| v.as_string()) {
                    let scope_repo = c.children().and_then(|sc| {
                        sc.nodes()
                            .iter()
                            .find(|n| n.name().value() == "scope-repo")
                            .and_then(|n| n.get(0).and_then(|v| v.as_string()).map(String::from))
                    });
                    p.shims.push(Shim { name: name.to_string(), scope_repo });
                }
            }
            _ => {}
        }
    }
    p
}

/// A node's positional (unnamed) string arguments — e.g. `allow "Read" "Edit"` → `["Read","Edit"]`.
fn positional_strings(node: &kdl::KdlNode) -> Vec<String> {
    node.entries()
        .iter()
        .filter(|e| e.name().is_none())
        .filter_map(|e| e.value().as_string().map(String::from))
        .collect()
}

/// Path scopes accept either positional args (`read "a" "b"`) or child-node names (`read { "a"; "b" }`).
fn path_scopes(node: &kdl::KdlNode) -> Vec<String> {
    let args = positional_strings(node);
    if !args.is_empty() {
        return args;
    }
    node.children()
        .map(|ch| ch.nodes().iter().map(|n| n.name().value().to_string()).collect())
        .unwrap_or_default()
}

/// Render one IR agent's `agent.kdl` (the manifest st2 RUN reads). Behavior-neutral wiring; no
/// permissions (M3.2). `env` values are `$CATALOG`-rooted (R11) and expand to the same paths convoy
/// uses at spawn.
pub fn render_agent_kdl(ir: &IrAgent) -> anyhow::Result<String> {
    let bus = ir.bus_id();
    let session = ir.session_id();
    let command = ir.agent_command()?;
    let ding = ir.ding_command();

    let mut s = String::new();
    s.push_str("// Rendered by `st2 render` from IR — behavior-neutral with convoy. Do not edit; re-render.\n");
    s.push_str(&format!("agent {} {{\n", kdl_str(&ir.identity)));
    s.push_str(&format!("  identity {}\n", kdl_str(&ir.identity)));
    s.push_str(&format!("  host {}\n", kdl_str(&ir.host)));
    s.push_str("  type \"service\"\n");
    s.push_str(&format!("  workspace {}\n", kdl_str(&ir.workspace)));
    if let Some(sup) = &ir.supervisor {
        s.push_str(&format!("  supervisor {}\n", kdl_str(sup)));
    }
    // The agent process.
    s.push_str("  pty \"agent\" {\n");
    s.push_str(&format!("    id {}\n", kdl_str(&session)));
    s.push_str(&format!("    command {}\n", kdl_raw(&command)));
    s.push_str("    tags role=\"agent\"\n");
    s.push_str(&render_env(&bus));
    s.push_str("  }\n");
    // The ding sidecar.
    s.push_str("  exec \"ding\" {\n");
    s.push_str(&format!("    id {}\n", kdl_str(&format!("{session}.ding"))));
    s.push_str(&format!("    command {}\n", kdl_raw(&ding)));
    s.push_str(&render_env(&bus));
    s.push_str("  }\n");
    s.push_str("}\n");
    Ok(s)
}

/// The shared env block — bus wiring that always wins (convoy parity): identity + the smalltalk/pty
/// roots, `$CATALOG`-rooted. (A `PATH` shim-dir prepend belongs here when shims land — deferred.)
fn render_env(bus_id: &str) -> String {
    format!(
        "    env {{\n      ST_AGENT {}\n      ST_ROOT \"$CATALOG/smalltalk\"\n      PTY_ROOT \"$CATALOG/pty\"\n    }}\n",
        kdl_str(bus_id)
    )
}

/// A plain KDL double-quoted string (values here are identities/paths — no embedded quotes).
fn kdl_str(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A KDL raw string `#"…"#` for command lines (which contain single quotes) — avoids escaping.
fn kdl_raw(s: &str) -> String {
    format!("#\"{s}\"#")
}

/// The PreToolUse tool-gate hook template. `__ALLOW__`/`__DENY__` are replaced with this agent's
/// space-separated tool lists. It BLOCKS a disallowed tool via `permissionDecision: deny` — which
/// blocks WITHOUT prompting, so an autonomous `bypassPermissions` agent never hangs — and records
/// every call to `.convoy/events/`. allow-list = block anything not listed; deny-list = block those.
/// jq-free (tool_name pulled with sed). Simple tool-level perms; the full model extends it later.
const PERM_HOOK_TEMPLATE: &str = r#"#!/usr/bin/env bash
# Rendered by `st2 render` (M3.2, simple tool-level perms). PreToolUse gate: BLOCK a disallowed tool
# (deny, never prompt — safe for an autonomous bypassPermissions agent) + record to .convoy/events/.
# TODO (extends later): path read/write scopes, shims, ask->human routing (DQ5), deny-floor.
set -eu
ALLOW="__ALLOW__"
DENY="__DENY__"
input="$(cat)"
tool="$(printf '%s' "$input" | sed -n 's/.*"tool_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
dir="$(cd "$(dirname "$0")/../.." && pwd)/.convoy/events"
mkdir -p "$dir" 2>/dev/null || true
printf '%s\n' "$input" >> "$dir/pretooluse.jsonl" 2>/dev/null || true
blocked=0
for d in $DENY; do [ "$tool" = "$d" ] && blocked=1; done
if [ -n "$ALLOW" ]; then
  ok=0; for a in $ALLOW; do [ "$tool" = "$a" ] && ok=1; done
  [ "$ok" = 0 ] && blocked=1
fi
if [ "$blocked" = 1 ]; then
  printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"tool %s is not permitted for this agent"}}\n' "$tool"
fi
exit 0
"#;

/// Bake this agent's allow/deny tool lists into the PreToolUse gate hook.
fn render_permissions_hook(allow: &[String], deny: &[String]) -> String {
    PERM_HOOK_TEMPLATE.replace("__ALLOW__", &allow.join(" ")).replace("__DENY__", &deny.join(" "))
}

/// A minimal `.claude/settings.json` that ONLY registers the PreToolUse hook — no `permissions`
/// allow/ask block (that would PROMPT; the agent stays `bypassPermissions` and the hook does the
/// blocking without prompting). `hook_cmd` is the absolute hook path.
fn render_settings_json(hook_cmd: &str) -> String {
    let settings = serde_json::json!({
        "hooks": { "PreToolUse": [ { "hooks": [ { "type": "command", "command": hook_cmd } ] } ] }
    });
    serde_json::to_string_pretty(&settings).unwrap_or_else(|_| "{}".to_string())
}

/// The full render of one IR agent: writes the catalog `agent.kdl`, materializes the persona overlay
/// into the workspace, and creates the smalltalk bus member dirs. Returns the agent dir written.
pub fn render_agent(
    ir: &IrAgent,
    catalog_root: &Path,
    personas_dir: &Path,
) -> anyhow::Result<std::path::PathBuf> {
    // 1) The manifest: <catalog>/<host>/<identity>/agent.kdl (discover slurps it).
    let agent_dir = catalog_root.join(&ir.host).join(&ir.identity);
    fs::create_dir_all(&agent_dir)?;
    fs::write(agent_dir.join("agent.kdl"), render_agent_kdl(ir)?)?;

    // 2) The persona overlay in the WORKSPACE (Claude Code loads .claude/rules from cwd; git-excluded
    //    exactly like convoy so it never touches tracked state).
    write_persona_overlay(ir, personas_dir)?;

    // 3) Permissions overlay (M3.2) — ONLY when a block is declared. No block → nothing written →
    //    open/bypassPermissions (the swap-safe default; behavior-neutral with convoy, which emits none).
    if let Some(perms) = &ir.permissions {
        write_permissions_overlay(ir, perms)?;
    }

    // 4) The smalltalk bus member folder (behavior-neutral bus presence): inbox + archive.
    let member = catalog_root.join("smalltalk").join(ir.bus_id());
    fs::create_dir_all(member.join("inbox"))?;
    fs::create_dir_all(member.join("archive"))?;

    Ok(agent_dir)
}

/// Materialize `<workspace>/.convoy/PERSONA.md` and append its `@`-import to
/// `<workspace>/.claude/rules/convoy.md` (Claude Code auto-loads `.claude/rules/*.md`; the `@`-path is
/// relative to that file). Both are added to `.git/info/exclude`. Convoy-parity mechanism.
fn write_persona_overlay(ir: &IrAgent, personas_dir: &Path) -> anyhow::Result<()> {
    let ws = Path::new(&ir.workspace);
    let convoy_dir = ws.join(".convoy");
    fs::create_dir_all(&convoy_dir)?;

    let persona_src = personas_dir.join(format!("{}.md", ir.persona));
    if persona_src.exists() {
        fs::copy(&persona_src, convoy_dir.join("PERSONA.md"))?;
    } else {
        anyhow::bail!(
            "render: persona '{}' not found at {}",
            ir.persona,
            persona_src.display()
        );
    }
    // The ding-bus instructions (vendored, byte-identical to convoy).
    fs::write(convoy_dir.join("DING-BUS.md"), DING_BUS_MD)?;

    // The boot hooks (SessionStart re-drain, PreCompact, StopFailure) — convoy-parity so a rendered
    // agent re-runs its boot ritual on cold start etc. Skipped (with a warning) if `st` isn't
    // resolvable, since the hook commands reference smalltalk's hook scripts.
    let claude_dir = ws.join(".claude");
    fs::create_dir_all(&claude_dir)?;
    match resolve_smalltalk_hooks() {
        Some((st_bin, hooks)) => {
            fs::write(claude_dir.join("settings.local.json"), render_settings_local_json(&st_bin, &hooks))?;
        }
        None => eprintln!(
            "st2 render: `st` not on PATH — skipping settings.local.json boot hooks for '{}'",
            ir.bus_id()
        ),
    }

    // The loader Claude Code auto-reads from cwd — append both `@`-imports (convoy-parity order).
    let rules_dir = ws.join(".claude").join("rules");
    fs::create_dir_all(&rules_dir)?;
    let loader = rules_dir.join("convoy.md");
    let mut content = fs::read_to_string(&loader).unwrap_or_default();
    for import in ["@../../.convoy/PERSONA.md", "@../../.convoy/DING-BUS.md"] {
        if !content.contains(import) {
            if !content.is_empty() && !content.ends_with('\n') {
                content.push('\n');
            }
            content.push_str(import);
            content.push('\n');
        }
    }
    fs::write(&loader, content)?;

    let mut excludes = vec![".convoy/", ".claude/rules/convoy.md", ".claude/settings.local.json"];
    // Codex reads AGENTS.md from cwd — it does NOT load `.claude/rules` or `.convoy/PERSONA.md`. So a
    // codex seat gets its brief ONLY via AGENTS.md: install the composed persona there, VERBATIM (the
    // `--persona` contract that preserves the eval's SHA-pinned composition byte-for-byte). The claude
    // overlay is still written (harmless; convoy-parity) — codex just doesn't read it.
    if ir.harness == "codex" {
        fs::copy(&persona_src, ws.join("AGENTS.md"))?;
        excludes.push("AGENTS.md");
    }
    exclude_from_git(ws, &excludes);
    Ok(())
}

/// Render a declared permissions block (M3.2, simple tool-level perms) into the workspace overlay: a
/// `.claude/hooks/permissions.sh` PreToolUse gate that blocks disallowed tools (allow/deny + spawn),
/// and a `.claude/settings.json` that registers it. Both git-excluded. Nothing to restrict (no
/// allow-list, no deny, spawn allowed) → nothing written (stays fully open). The path scopes, shims,
/// and ask-routing on `p` are the seam for the full model — parsed, not yet rendered.
fn write_permissions_overlay(ir: &IrAgent, p: &IrPermissions) -> anyhow::Result<()> {
    // Effective deny = declared tool denies + the subagent tool when spawn is off.
    let mut deny = p.tools_deny.clone();
    if !p.spawn && !deny.iter().any(|d| d == "Agent") {
        deny.push("Agent".to_string());
    }
    if p.tools_allow.is_empty() && deny.is_empty() {
        return Ok(()); // declared block, but nothing restrictive → open, no hook needed
    }

    let ws = Path::new(&ir.workspace);
    let hooks_dir = ws.join(".claude").join("hooks");
    fs::create_dir_all(&hooks_dir)?;
    let hook_path = hooks_dir.join("permissions.sh");
    fs::write(&hook_path, render_permissions_hook(&p.tools_allow, &deny))?;
    set_executable(&hook_path);

    fs::write(ws.join(".claude").join("settings.json"), render_settings_json(&hook_path.to_string_lossy()))?;

    exclude_from_git(ws, &[".claude/settings.json", ".claude/hooks/"]);
    Ok(())
}

/// Resolve `st` on PATH → `(<abs st binary>, <smalltalk hooks dir>)`, mirroring how convoy finds its
/// smalltalk hook scripts. `None` if `st` isn't on PATH or the hooks dir isn't there.
fn resolve_smalltalk_hooks() -> Option<(String, std::path::PathBuf)> {
    let path = std::env::var_os("PATH")?;
    let st = std::env::split_paths(&path).map(|d| d.join("st")).find(|p| p.is_file())?;
    let st_real = fs::canonicalize(&st).ok()?;
    let hooks = st_real.parent()?.parent()?.join("examples/claude-code/hooks"); // <smalltalk>/bin/st → hooks
    hooks
        .join("session-start.sh")
        .is_file()
        .then(|| (st_real.to_string_lossy().into_owned(), hooks))
}

/// The boot-hooks `settings.local.json` — convoy-parity `SessionStart`/`PreCompact`/`StopFailure`
/// hooks that run smalltalk's hook scripts. (Neutrality is asserted structurally — parsed JSON
/// equality — not byte order.)
fn render_settings_local_json(st_bin: &str, hooks: &Path) -> String {
    let cmd = |name: &str| format!("ST_BIN={st_bin} {}", hooks.join(name).display());
    let settings = serde_json::json!({
        "$schema": "https://json.schemastore.org/claude-code-settings.json",
        "hooks": {
            "SessionStart": [{ "hooks": [{ "type": "command", "async": true, "asyncRewake": true, "command": cmd("session-start.sh") }] }],
            "PreCompact": [{ "hooks": [{ "type": "command", "command": cmd("pre-compact.sh") }] }],
            "StopFailure": [{ "hooks": [{ "type": "command", "command": cmd("stop-failure.sh") }] }]
        }
    });
    serde_json::to_string_pretty(&settings).unwrap_or_default()
}

/// Best-effort `chmod +x`.
fn set_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = fs::metadata(path) {
        let mut perms = meta.permissions();
        perms.set_mode(perms.mode() | 0o755);
        let _ = fs::set_permissions(path, perms);
    }
}

/// Best-effort append to `<workspace>/.git/info/exclude` (only inside a git repo) so the composed
/// overlay never shows up in the agent repo's `git status`. Idempotent.
fn exclude_from_git(workspace: &Path, patterns: &[&str]) {
    let exclude = workspace.join(".git").join("info").join("exclude");
    let Some(parent) = exclude.parent() else { return };
    if !parent.exists() {
        return; // not a git repo (no .git/info) — nothing to exclude
    }
    let mut content = fs::read_to_string(&exclude).unwrap_or_default();
    let mut changed = false;
    for p in patterns {
        if !content.lines().any(|l| l.trim() == *p) {
            if !content.is_empty() && !content.ends_with('\n') {
                content.push('\n');
            }
            content.push_str(p);
            content.push('\n');
            changed = true;
        }
    }
    if changed {
        let _ = fs::write(&exclude, content);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker() -> IrAgent {
        IrAgent {
            identity: "fabric-claude".into(),
            host: "silber".into(),
            role: "worker".into(),
            harness: "claude".into(),
            model: Some("opus".into()),
            persona: "worker".into(),
            workspace: "/repos/fabric".into(),
            supervisor: Some("cos".into()),
            permissions: None,
            extra_args: Vec::new(),
        }
    }

    #[test]
    fn ids_strip_harness_suffix_but_bus_id_keeps_it() {
        let a = worker();
        assert_eq!(a.session_id(), "silber.fabric"); // suffix stripped
        assert_eq!(a.bus_id(), "silber.fabric-claude"); // suffix kept
    }

    #[test]
    fn agent_command_matches_convoy_claude_wiring() {
        let a = worker();
        assert_eq!(
            a.agent_command().unwrap(),
            "exec claude --permission-mode bypassPermissions --model 'opus' 'You just cold-started. Run your boot ritual now: set your st status to available and drain your inbox (read, act on, and archive each message). Then stand by for work delivered via ding.'"
        );
        // No model → no --model flag.
        let mut b = worker();
        b.model = None;
        assert!(b.agent_command().unwrap().starts_with("exec claude --permission-mode bypassPermissions 'You just"));
        // CoS gets the CoS boot prompt.
        let mut c = worker();
        c.role = "chief-of-staff".into();
        assert!(c.agent_command().unwrap().contains("first-run interview"));
    }

    #[test]
    fn extra_args_splice_before_the_boot_prompt_shell_quoted() {
        // The skill-inheritance case: --plugin-dir <dir> injected into a seat's claude command.
        let mut a = worker();
        a.extra_args = vec!["--plugin-dir".into(), "/repos/evals/plugins".into()];
        let cmd = a.agent_command().unwrap();
        assert!(
            cmd.starts_with("exec claude --permission-mode bypassPermissions --model 'opus' '--plugin-dir' '/repos/evals/plugins' 'You just"),
            "extra args go after the model flag and before the boot prompt, each shell-quoted: {cmd}"
        );
        // A path with a single quote stays one argv token (POSIX '\'' escaping).
        let mut b = worker();
        b.extra_args = vec!["--plugin-dir".into(), "/a b/it's".into()];
        assert!(b.agent_command().unwrap().contains(r"'/a b/it'\''s'"));
        // Empty extra_args → byte-identical to convoy (no stray space).
        let mut c = worker();
        c.extra_args = Vec::new();
        assert!(!c.agent_command().unwrap().contains("  "));
        // codex leg splices the same way.
        let mut d = worker();
        d.harness = "codex".into();
        d.extra_args = vec!["--cd".into(), "/x".into()];
        assert!(d.agent_command().unwrap().contains("--dangerously-bypass-approvals-and-sandbox --model 'opus' '--cd' '/x' '"));
    }

    #[test]
    fn ding_command_targets_the_smalltalk_bus() {
        assert_eq!(
            worker().ding_command(),
            "st ding silber.fabric --identity silber.fabric-claude --root $CATALOG/smalltalk"
        );
    }

    #[test]
    fn parse_ir_reads_fields_and_defaults_persona_to_role() {
        let ir = r#"
            agent "fabric-claude" {
              host "silber"
              role "worker"
              harness "claude"
              model "opus"
              workspace "/repos/fabric"
              supervisor "cos"
              permissions { spawn #false }
            }
        "#;
        let agents = parse_ir(ir).unwrap();
        assert_eq!(agents.len(), 1);
        let a = &agents[0];
        assert_eq!(a.identity, "fabric-claude");
        assert_eq!(a.host, "silber");
        assert_eq!(a.model.as_deref(), Some("opus"));
        assert_eq!(a.persona, "worker"); // defaulted to role
        assert_eq!(a.supervisor.as_deref(), Some("cos"));
    }

    #[test]
    fn parse_ir_reads_a_permissions_block() {
        let ir = r#"
            agent "x-claude" {
              host "h"; role "worker"; harness "claude"; workspace "/w"
              permissions {
                tools { allow "Read" "Edit"; deny "WebSearch"; ask "Bash" }
                read  { "$WORKSPACE/**"; "$CATALOG/**" }
                write { "$WORKSPACE/**" }
                spawn #false
                shim "gh" { scope-repo "$WORKSPACE" }
              }
            }
        "#;
        let p = parse_ir(ir).unwrap()[0].permissions.clone().unwrap();
        // Rendered dimensions:
        assert_eq!(p.tools_allow, ["Read", "Edit"]);
        assert_eq!(p.tools_deny, ["WebSearch"]);
        assert!(!p.spawn);
        // Seam dimensions (parsed, not yet rendered):
        assert_eq!(p.tools_ask, ["Bash"]);
        assert_eq!(p.read, ["$WORKSPACE/**", "$CATALOG/**"]);
        assert_eq!(p.write, ["$WORKSPACE/**"]);
        assert_eq!(p.shims.len(), 1);
    }

    #[test]
    fn permissions_hook_blocks_disallowed_tools_without_prompting() {
        // spawn #false folds into deny; allow-list means anything unlisted is blocked.
        let hook = render_permissions_hook(&["Read".into(), "Edit".into()], &["WebSearch".into(), "Agent".into()]);
        assert!(hook.contains(r#"ALLOW="Read Edit""#));
        assert!(hook.contains(r#"DENY="WebSearch Agent""#));
        // It DENIES (blocks, never asks — an autonomous pty can't answer a prompt).
        assert!(hook.contains(r#""permissionDecision":"deny""#));
        assert!(!hook.contains("\"ask\""), "must never prompt");
        // settings.json ONLY registers the hook — no permissions allow/ask block.
        let settings: serde_json::Value =
            serde_json::from_str(&render_settings_json("/w/.claude/hooks/permissions.sh")).unwrap();
        assert_eq!(settings["hooks"]["PreToolUse"][0]["hooks"][0]["type"], "command");
        assert!(settings.get("permissions").is_none(), "no allowlist that would prompt");
    }

    #[test]
    fn rendered_agent_kdl_carries_the_neutral_wiring() {
        let kdl = render_agent_kdl(&worker()).unwrap();
        assert!(kdl.contains("agent \"fabric-claude\""));
        assert!(kdl.contains("id \"silber.fabric\""));
        assert!(kdl.contains("--permission-mode bypassPermissions"));
        assert!(kdl.contains("ST_AGENT \"silber.fabric-claude\""));
        assert!(kdl.contains("ST_ROOT \"$CATALOG/smalltalk\""));
        assert!(kdl.contains("PTY_ROOT \"$CATALOG/pty\""));
        assert!(kdl.contains("st ding silber.fabric --identity silber.fabric-claude"));
    }
}
