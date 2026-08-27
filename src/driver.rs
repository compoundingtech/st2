//! Pure typed-driver expansion into ordinary hand-authorable Agent Spec KDL primitives.
//!
//! Expansion does not read files, inspect a harness, mutate a declaration, or execute a process.
//! Print, reconcile, and materialization use this same expansion.

use agent_spec::spec::{AgentSpec, ClaudeDriver, CodexDriver, Driver, OpenCodeDriver, PiDriver};
use anyhow::{Context, Result};
use kdl::{KdlDocument, KdlEntry, KdlNode};

const ST2: &str = "st2";
const CATALOG: &str = "$CATALOG";

/// Reject two launch sources before expansion, task compilation, or workspace writes.
pub(crate) fn ensure_single_source(spec: &AgentSpec) -> Result<()> {
    anyhow::ensure!(
        spec.driver.is_none() || spec.delivery.is_none(),
        "agent '{}' declares both a driver block and `deliver`; choose one launch source",
        spec.identity
    );
    Ok(())
}

/// Expand one typed driver into KDL nodes that can be written inside an `agent {}` block.
pub fn expand_driver(spec: &AgentSpec, this_host: &str) -> Result<KdlDocument> {
    ensure_single_source(spec)?;
    let driver = spec
        .driver
        .as_ref()
        .with_context(|| format!("agent '{}' has no driver block", spec.identity))?;
    anyhow::ensure!(
        spec.host.is_some() || !this_host.is_empty(),
        "agent '{}' has no host and driver expansion received no host fallback",
        spec.identity
    );
    let bus_id = spec.bus_id(this_host);
    let mut output = match driver {
        Driver::Claude(driver) => expand_claude(driver, &bus_id)?,
        Driver::Codex(driver) => expand_codex(driver, &bus_id),
        Driver::Pi(driver) => expand_pi(driver, &bus_id),
        Driver::OpenCode(driver) => expand_opencode(driver, &bus_id),
    };
    output.autoformat();
    Ok(output)
}

fn expand_codex(driver: &CodexDriver, bus_id: &str) -> KdlDocument {
    let mut provider = vec!["codex".to_string()];
    if let Some(model) = &driver.model {
        provider.extend(["--model".to_string(), model.clone()]);
    }
    if let Some(effort) = &driver.effort {
        provider.extend(["-c".to_string(), format!("model_reasoning_effort={effort}")]);
    }
    provider.extend(driver.args.iter().cloned());
    provider.push(driver.prompt.clone());

    let mut argv = vec![
        ST2.to_string(),
        "--catalog".to_string(),
        CATALOG.to_string(),
        "driver".to_string(),
        "codex".to_string(),
        "--identity".to_string(),
        bus_id.to_string(),
        "--runtime-id".to_string(),
        bus_id.to_string(),
        "--".to_string(),
    ];
    argv.extend(provider);
    document([node("argv", argv)])
}

/// pi needs no rendered configuration file: its channel is an extension, and the wrapper injects
/// that extension from this binary's verified hook set rather than writing a machine-local path
/// into the declaration. `-a` accepts the workspace for this run only, which is why no pi analogue
/// of [`crate::pretrust`] exists — nothing in the operator's ambient pi config is mutated.
fn expand_pi(driver: &PiDriver, bus_id: &str) -> KdlDocument {
    let mut provider = vec!["pi".to_string(), "-a".to_string()];
    if let Some(model) = &driver.model {
        provider.extend(["--model".to_string(), model.clone()]);
    }
    if let Some(effort) = &driver.effort {
        provider.extend(["--thinking".to_string(), effort.clone()]);
    }
    provider.extend(driver.args.iter().cloned());
    provider.push(driver.prompt.clone());

    let mut argv = vec![
        ST2.to_string(),
        "--catalog".to_string(),
        CATALOG.to_string(),
        "driver".to_string(),
        "pi-session".to_string(),
        "--identity".to_string(),
        bus_id.to_string(),
        "--runtime-id".to_string(),
        bus_id.to_string(),
        "--".to_string(),
    ];
    argv.extend(provider);
    document([node("argv", argv)])
}

/// OpenCode's server surface is wrapper-owned runtime state: the wrapper allocates the port and
/// password at launch, so the expansion stays a pure declaration with no machine-local values.
fn expand_opencode(driver: &OpenCodeDriver, bus_id: &str) -> KdlDocument {
    let mut provider = vec!["opencode".to_string()];
    if let Some(model) = &driver.model {
        provider.extend(["--model".to_string(), model.clone()]);
    }
    provider.extend(driver.args.iter().cloned());
    // Unlike the sibling harnesses, OpenCode's positional argument is a project directory; the
    // startup prompt is a named flag.
    provider.extend(["--prompt".to_string(), driver.prompt.clone()]);

    let mut argv = vec![
        ST2.to_string(),
        "--catalog".to_string(),
        CATALOG.to_string(),
        "driver".to_string(),
        "opencode-session".to_string(),
        "--identity".to_string(),
        bus_id.to_string(),
        "--runtime-id".to_string(),
        bus_id.to_string(),
        "--".to_string(),
    ];
    argv.extend(provider);
    document([node("argv", argv)])
}

fn expand_claude(driver: &ClaudeDriver, bus_id: &str) -> Result<KdlDocument> {
    // The same registration a hand-authored seat carries: without it a driver-declared
    // seat has no observed-state producer and no lifecycle hooks at all.
    let settings = serde_json::to_string_pretty(&crate::hooks::claude_settings_registration())?;
    let mut render = KdlNode::new("render");
    render.set_children(document([{
        // Hook arrays join whatever the workspace already declares: replacement would clobber
        // user-registered hooks on every materialization, and union is idempotent.
        let mut upsert = node(
            "json-upsert",
            vec![".claude/settings.local.json".to_string(), settings],
        );
        upsert
            .entries_mut()
            .push(KdlEntry::new_prop("arrays", "union"));
        upsert
    }]));

    let mut provider = vec!["claude".to_string()];
    if let Some(model) = &driver.model {
        provider.extend(["--model".to_string(), model.clone()]);
    }
    if let Some(effort) = &driver.effort {
        provider.extend(["--effort".to_string(), effort.clone()]);
    }
    if driver.dev_channels {
        provider.extend([
            "--channels".to_string(),
            crate::claude_channel::CHANNEL.to_string(),
        ]);
    }
    provider.extend(driver.args.iter().cloned());
    provider.push(driver.prompt.clone());

    let mut argv = vec![
        ST2.to_string(),
        "--catalog".to_string(),
        CATALOG.to_string(),
        "driver".to_string(),
        "claude-session".to_string(),
        "--identity".to_string(),
        bus_id.to_string(),
        "--runtime-id".to_string(),
        bus_id.to_string(),
        "--".to_string(),
    ];
    argv.extend(provider);
    Ok(document([render, node("argv", argv)]))
}

fn node(name: &str, args: Vec<String>) -> KdlNode {
    let mut node = KdlNode::new(name);
    node.entries_mut()
        .extend(args.into_iter().map(KdlEntry::new));
    node
}

fn document<const N: usize>(nodes: [KdlNode; N]) -> KdlDocument {
    let mut document = KdlDocument::new();
    document.nodes_mut().extend(nodes);
    document
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use agent_spec::spec::{AgentDesiredState, JobType};
    use kdl::KdlValue;

    use super::*;

    fn spec(driver: Driver) -> AgentSpec {
        AgentSpec {
            identity: "worker".into(),
            name: None,
            description: None,
            host: Some("host".into()),
            role: None,
            job_type: JobType::Service,
            workspace: Some("/work".into()),
            supervisor: None,
            desired_state: AgentDesiredState::Running,
            keep: false,
            restart: None,
            delivery: None,
            driver: Some(driver),
            resources: Vec::new(),
            streams: Vec::new(),
            tasks: Vec::new(),
            path: PathBuf::from("/catalog/agents/host/worker/agent.kdl"),
        }
    }

    fn strings(node: &KdlNode) -> Vec<&str> {
        node.entries()
            .iter()
            .filter_map(|entry| match entry.value() {
                KdlValue::String(value) => Some(value.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn codex_expands_to_one_plain_argv_with_typed_fields_before_verbatim_args() {
        let output = expand_driver(
            &spec(Driver::Codex(CodexDriver {
                model: Some("gpt-5.6-sol".into()),
                effort: Some("xhigh".into()),
                prompt: "Start work.".into(),
                args: vec!["--model".into(), "override".into()],
            })),
            "unused",
        )
        .unwrap();

        assert_eq!(output.nodes().len(), 1);
        assert_eq!(
            strings(output.get("argv").unwrap()),
            [
                "st2",
                "--catalog",
                "$CATALOG",
                "driver",
                "codex",
                "--identity",
                "host.worker",
                "--runtime-id",
                "host.worker",
                "--",
                "codex",
                "--model",
                "gpt-5.6-sol",
                "-c",
                "model_reasoning_effort=xhigh",
                "--model",
                "override",
                "Start work."
            ]
        );
    }

    #[test]
    fn opencode_expands_to_the_session_wrapper_with_a_prompt_flag() {
        let output = expand_driver(
            &spec(Driver::OpenCode(OpenCodeDriver {
                model: Some("anthropic/claude-opus-5".into()),
                prompt: "Start work.".into(),
                args: vec!["--agent".into(), "build".into()],
            })),
            "unused",
        )
        .unwrap();

        assert_eq!(output.nodes().len(), 1);
        assert_eq!(
            strings(output.get("argv").unwrap()),
            [
                "st2",
                "--catalog",
                "$CATALOG",
                "driver",
                "opencode-session",
                "--identity",
                "host.worker",
                "--runtime-id",
                "host.worker",
                "--",
                "opencode",
                "--model",
                "anthropic/claude-opus-5",
                "--agent",
                "build",
                "--prompt",
                "Start work."
            ]
        );
    }

    #[test]
    fn claude_expands_to_a_channel_render_and_session_owned_launch() {
        let output = expand_driver(
            &spec(Driver::Claude(ClaudeDriver {
                model: Some("opus".into()),
                effort: Some("xhigh".into()),
                dev_channels: true,
                prompt: "Start work.".into(),
                args: vec!["--model".into(), "override".into()],
            })),
            "unused",
        )
        .unwrap();

        assert_eq!(output.nodes().len(), 2);
        let render = output.get("render").unwrap();
        let upserts: Vec<&KdlNode> = render
            .children()
            .unwrap()
            .nodes()
            .iter()
            .filter(|node| node.name().value() == "json-upsert")
            .collect();
        assert_eq!(upserts.len(), 1);
        let settings = strings(upserts[0]);
        assert_eq!(settings[0], ".claude/settings.local.json");
        let settings: serde_json::Value = serde_json::from_str(settings[1]).unwrap();
        assert_eq!(settings, crate::hooks::claude_settings_registration());
        let argv = strings(output.get("argv").unwrap());
        assert_eq!(
            &argv[..17],
            &[
                "st2",
                "--catalog",
                "$CATALOG",
                "driver",
                "claude-session",
                "--identity",
                "host.worker",
                "--runtime-id",
                "host.worker",
                "--",
                "claude",
                "--model",
                "opus",
                "--effort",
                "xhigh",
                "--channels",
                "plugin:st2-channel@st2",
            ]
        );
        assert_eq!(&argv[17..], &["--model", "override", "Start work."]);
    }
}
