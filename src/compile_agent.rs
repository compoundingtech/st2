//! Experimental compact-agent generator.
//!
//! Hand-authored `agent.kdl` is the canonical st2 interface. This module is a generation aid: it
//! writes one native declaration plus catalog-owned templates, but it never materializes a workspace.

use std::fs;
use std::path::Path;

const BUS_ST2_MD: &str = include_str!("../templates/bus.st2.md");

#[derive(Debug, Clone)]
pub struct AgentInput {
    pub identity: String,
    pub host: String,
    pub role: String,
    pub harness: String,
    pub model: Option<String>,
    pub workspace: String,
    pub supervisor: Option<String>,
    pub extra_args: Vec<String>,
}

impl AgentInput {
    fn bus_id(&self) -> String {
        format!("{}.{}", self.host, self.identity)
    }

    fn argv(&self) -> anyhow::Result<Vec<String>> {
        let boot = if self.role == "chief-of-staff" {
            "You just cold-started. Read AGENTS.md and run your st2 boot ritual now: set status available and drain your inbox. Before resuming or starting work, set status busy; set available only when yielding or ready for new work. Resume unfinished durable work immediately; stand by for work delivered via ding only when no unfinished durable work remains."
        } else {
            "You just cold-started. Run your st2 boot ritual now: set status available and drain your inbox. Before resuming or starting work, set status busy; set available only when yielding or ready for new work. Resume unfinished durable work immediately; stand by for work delivered via ding only when no unfinished durable work remains."
        };
        let mut argv = match self.harness.as_str() {
            "claude" => vec![
                "claude".to_string(),
                "--permission-mode".to_string(),
                "bypassPermissions".to_string(),
            ],
            "codex" => vec![
                "codex".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--dangerously-bypass-hook-trust".to_string(),
            ],
            other => anyhow::bail!(
                "compile-agent: harness '{other}' is not supported (expected claude or codex)"
            ),
        };
        if let Some(model) = &self.model {
            argv.extend(["--model".to_string(), model.clone()]);
        }
        argv.extend(self.extra_args.iter().cloned());
        argv.push(boot.to_string());
        Ok(argv)
    }
}

fn kdl_str(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn kdl_multiline_raw(value: &str) -> String {
    format!("#\"\"\"\n{value}\n\"\"\"#")
}

fn hook_json(harness: &str) -> serde_json::Value {
    match harness {
        "codex" => serde_json::json!({
            "hooks": {
                "SessionStart": [{
                    "hooks": [{
                        "type": "command",
                        "command": "$ST_HOOKS/codex-session-start.sh",
                        "timeout": 5,
                        "statusMessage": "Restoring st2 context"
                    }]
                }],
                "PreCompact": [{
                    "hooks": [{
                        "type": "command",
                        "command": "$ST_HOOKS/codex-pre-compact.sh",
                        "timeout": 5,
                        "statusMessage": "Checkpointing st2 context"
                    }]
                }],
                "Stop": [{
                    "hooks": [{
                        "type": "command",
                        "command": "$ST_HOOKS/codex-stop.sh",
                        "timeout": 5
                    }]
                }]
            }
        }),
        "claude" => serde_json::json!({
            "$schema": "https://json.schemastore.org/claude-code-settings.json",
            "hooks": {
                "SessionStart": [{
                    "hooks": [{
                        "type": "command",
                        "async": true,
                        "asyncRewake": true,
                        "command": "$ST_HOOKS/claude-session-start.sh"
                    }]
                }],
                "PreCompact": [{
                    "hooks": [{
                        "type": "command",
                        "command": "$ST_HOOKS/claude-pre-compact.sh"
                    }]
                }],
                "StopFailure": [{
                    "hooks": [{
                        "type": "command",
                        "command": "$ST_HOOKS/claude-stop-failure.sh"
                    }]
                }]
            }
        }),
        _ => serde_json::Value::Null,
    }
}

/// Generate one compact declaration and its catalog-owned templates.
pub fn compile_agent(
    input: &AgentInput,
    catalog_root: &Path,
    persona_file: &Path,
) -> anyhow::Result<std::path::PathBuf> {
    if !matches!(input.harness.as_str(), "claude" | "codex") {
        anyhow::bail!(
            "compile-agent: harness '{}' is not supported (expected claude or codex)",
            input.harness
        );
    }
    let persona = fs::read_to_string(persona_file).map_err(|error| {
        anyhow::anyhow!(
            "reading persona {} for '{}': {error}",
            persona_file.display(),
            input.identity
        )
    })?;

    let templates = catalog_root.join("_templates");
    fs::create_dir_all(&templates)?;
    let prefix = format!("{}.{}", input.host, input.identity);
    let persona_name = format!("{prefix}.persona.md");
    let agents_name = format!("{prefix}.AGENTS.md");
    let bus_name = "bus.st2.md";
    fs::write(templates.join(&persona_name), &persona)?;
    fs::write(templates.join(bus_name), BUS_ST2_MD)?;
    if input.harness == "codex" {
        let mut agents = persona;
        if !agents.ends_with('\n') {
            agents.push('\n');
        }
        agents.push_str("\n---\n\n");
        agents.push_str(BUS_ST2_MD);
        fs::write(templates.join(&agents_name), agents)?;
    }

    let argv = input.argv()?;
    let hooks = serde_json::to_string_pretty(&hook_json(&input.harness))?;
    let mut kdl = String::new();
    kdl.push_str(
        "// Experimental output from `st2 compile-agent`. Inspect this file and every render target before use.\n",
    );
    kdl.push_str(&format!("agent {} {{\n", kdl_str(&input.identity)));
    kdl.push_str(&format!("  host {}\n", kdl_str(&input.host)));
    kdl.push_str(&format!("  role {}\n", kdl_str(&input.role)));
    kdl.push_str(&format!("  workspace {}\n", kdl_str(&input.workspace)));
    if let Some(supervisor) = &input.supervisor {
        kdl.push_str(&format!("  supervisor {}\n", kdl_str(supervisor)));
    }
    kdl.push_str("  env {\n");
    kdl.push_str(&format!("    ST_AGENT {}\n", kdl_str(&input.bus_id())));
    kdl.push_str("  }\n");
    kdl.push_str("  argv");
    for arg in argv {
        kdl.push(' ');
        kdl.push_str(&kdl_str(&arg));
    }
    kdl.push('\n');
    kdl.push_str("  ding\n\n");
    kdl.push_str("  render {\n");
    if input.harness == "codex" {
        kdl.push_str(&format!(
            "    copy {} \"AGENTS.md\"\n",
            kdl_str(&format!("_templates/{agents_name}"))
        ));
        kdl.push_str(&format!(
            "    json-upsert \".codex/hooks.json\" {}\n",
            kdl_multiline_raw(&hooks)
        ));
        kdl.push_str("    git-exclude \"AGENTS.md\" \".codex/hooks.json\"\n");
    } else {
        kdl.push_str(&format!(
            "    copy {} \".st2/PERSONA.md\"\n",
            kdl_str(&format!("_templates/{persona_name}"))
        ));
        kdl.push_str(&format!(
            "    copy {} \".st2/bus.md\"\n",
            kdl_str(&format!("_templates/{bus_name}"))
        ));
        kdl.push_str("    ensure-line \".claude/rules/st2.md\" \"@../../.st2/PERSONA.md\"\n");
        kdl.push_str("    ensure-line \".claude/rules/st2.md\" \"@../../.st2/bus.md\"\n");
        kdl.push_str(&format!(
            "    json-upsert \".claude/settings.local.json\" {}\n",
            kdl_multiline_raw(&hooks)
        ));
        kdl.push_str(
            "    git-exclude \".st2/\" \".claude/rules/st2.md\" \".claude/settings.local.json\"\n",
        );
    }
    kdl.push_str("  }\n");
    kdl.push_str("}\n");

    let agent_dir = catalog_root
        .join("agents")
        .join(&input.host)
        .join(&input.identity);
    fs::create_dir_all(&agent_dir)?;
    fs::write(agent_dir.join("agent.kdl"), kdl)?;
    for kind in ["inbox", "archive", "context", "links"] {
        fs::create_dir_all(agent_dir.join("resources").join(kind))?;
    }
    Ok(agent_dir)
}
