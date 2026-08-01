//! KDL job parsing — lower a canonical VRS `agent.kdl` into the same [`RawSpec`] the TOML/JSON path
//! uses (spec.md §2). KDL is the canonical format. This walks the node tree by hand (KDL is not
//! serde-native) and fills a [`RawSpec`], which then flows through the shared identity/host
//! resolution. A file may hold more than one `agent` node.
//!
//! Only runner-normative fields plus metadata `role` are read; render-only fields (`harness`,
//! `model`, `persona`, `permissions`, `transport`, `strategy`) and the inert `meta{}` block are
//! ignored.

use crate::declared::{DeclaredDocument, DeclaredNode, DeclaredValue};
use crate::spec::{RawResource, RawRestart, RawSpec, RawTask};

/// Lower an already parsed declaration document into the runner's raw representation.
pub(crate) fn lower_declared_document(document: &DeclaredDocument) -> anyhow::Result<Vec<RawSpec>> {
    document
        .agents
        .iter()
        .map(|agent| agent_node_to_raw(&agent.node))
        .collect()
}

/// First positional (unnamed) argument of a node, as a string.
fn arg_string(node: &DeclaredNode) -> Option<String> {
    node.argument(0)
        .and_then(DeclaredValue::as_str)
        .map(String::from)
}

/// Every positional argument of a node, as strings.
fn argv(node: &DeclaredNode) -> anyhow::Result<Vec<String>> {
    node.arguments()
        .map(|value| {
            value
                .as_str()
                .map(String::from)
                .ok_or_else(|| anyhow::anyhow!("`argv` accepts only string arguments"))
        })
        .collect()
}

/// First positional argument as a bool (`#true`/`#false`), defaulting to `false`.
fn arg_bool(node: &DeclaredNode) -> bool {
    node.argument(0)
        .and_then(DeclaredValue::as_bool)
        .unwrap_or(false)
}

/// First positional argument as an integer.
fn arg_u32(node: &DeclaredNode) -> Option<u32> {
    node.argument(0)
        .and_then(DeclaredValue::as_integer)
        .and_then(|i| u32::try_from(i).ok())
}

fn agent_node_to_raw(node: &DeclaredNode) -> anyhow::Result<RawSpec> {
    let mut raw = RawSpec {
        identity: arg_string(node), // agent "<identity>" — may be overridden by an `identity` child
        ..Default::default()
    };

    // Environment is an agent-level scope in the compact format and cascades into explicit legacy
    // tasks too. Parse it first so declaration order does not change semantics.
    for child in &node.children {
        if child.name == "env" {
            raw.env.extend(env_node_to_raw(child));
        }
    }

    for child in &node.children {
        match child.name.as_str() {
            "identity" => raw.identity = arg_string(child).or(raw.identity),
            "name" => parse_presentation(child, "name", &mut raw.name)?,
            "description" => parse_presentation(child, "description", &mut raw.description)?,
            "host" => raw.host = arg_string(child),
            "role" => raw.role = arg_string(child),
            "type" => raw.job_type = arg_string(child),
            "workspace" => raw.workspace = arg_string(child),
            "supervisor" => raw.supervisor = arg_string(child),
            "retired" => raw.retired = arg_bool(child),
            "keep" => raw.keep = arg_bool(child),
            "lifecycle" => raw.lifecycle = arg_string(child),
            "restart" => raw.restart = Some(restart_node_to_raw(child)),
            "resource" => {
                let (name, resource) = resource_node_to_raw(child)?;
                raw.resource.insert(name, resource)?;
            }
            "command" => raw.command = arg_string(child),
            "argv" => raw.argv = Some(argv(child)?),
            "ding" => raw.ding = true,
            "env" => {}
            "pty" => {
                if let Some(name) = arg_string(child) {
                    raw.pty.insert(name, task_node_to_raw(child)?);
                }
            }
            "exec" => {
                if let Some(name) = arg_string(child) {
                    raw.exec.insert(name, task_node_to_raw(child)?);
                }
            }
            // meta, harness, model, persona, permissions, transport, strategy, … — ignored.
            _ => {}
        }
    }
    if raw.ding && raw.exec.contains_key("ding") {
        anyhow::bail!(
            "agent '{}' declares both compact `ding` and `exec \"ding\"`; choose one form",
            raw.identity.as_deref().unwrap_or("<unnamed>")
        );
    }
    Ok(raw)
}

fn parse_presentation(
    node: &DeclaredNode,
    field: &str,
    destination: &mut Option<String>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        destination.is_none(),
        "agent declares `{field}` more than once"
    );
    anyhow::ensure!(
        node.children.is_empty() && node.entries.len() == 1 && node.entries[0].name.is_none(),
        "agent `{field}` must contain exactly one positional string"
    );
    let value = node
        .argument(0)
        .and_then(DeclaredValue::as_str)
        .ok_or_else(|| anyhow::anyhow!("agent `{field}` must contain a string"))?;
    *destination = Some(value.to_owned());
    Ok(())
}

fn resource_node_to_raw(node: &DeclaredNode) -> anyhow::Result<(String, RawResource)> {
    if !node.children.is_empty() {
        anyhow::bail!("resource binding cannot have children");
    }

    let mut name = None;
    let mut tag = None;
    let mut uri = None;
    let mut relation = None;
    let mut reason = None;
    for entry in &node.entries {
        let Some(property) = entry.name.as_deref() else {
            if name.is_some() {
                anyhow::bail!("resource binding accepts exactly one positional name");
            }
            name = entry.value.as_str().map(String::from);
            if name.is_none() {
                anyhow::bail!("resource binding needs a string name");
            }
            continue;
        };

        let value = entry.value.as_str().map(String::from);
        match property {
            "_tag" => {
                if tag.is_some() {
                    anyhow::bail!("resource binding has duplicate `_tag`");
                }
                tag = value;
                if tag.is_none() {
                    anyhow::bail!("resource binding needs string `_tag`");
                }
            }
            "uri" => {
                if uri.is_some() {
                    anyhow::bail!("resource binding has duplicate `uri`");
                }
                uri = value;
                if uri.is_none() {
                    anyhow::bail!("resource binding needs string `uri`");
                }
            }
            "relation" => {
                if relation.is_some() {
                    anyhow::bail!("resource binding has duplicate `relation`");
                }
                relation = value;
                if relation.is_none() {
                    anyhow::bail!("resource binding needs string `relation`");
                }
            }
            "reason" => {
                if reason.is_some() {
                    anyhow::bail!("resource binding has duplicate `reason`");
                }
                reason = value;
                if reason.is_none() {
                    anyhow::bail!("resource binding needs string `reason`");
                }
            }
            other => anyhow::bail!("resource binding has unsupported property `{other}`"),
        }
    }

    Ok((
        name.ok_or_else(|| anyhow::anyhow!("resource binding needs a string name"))?,
        RawResource {
            tag: tag.ok_or_else(|| anyhow::anyhow!("resource binding needs string `_tag`"))?,
            uri: uri.ok_or_else(|| anyhow::anyhow!("resource binding needs string `uri`"))?,
            relation,
            reason,
        },
    ))
}

fn restart_node_to_raw(node: &DeclaredNode) -> RawRestart {
    let mut r = RawRestart::default();
    for child in &node.children {
        match child.name.as_str() {
            "attempts" => r.attempts = arg_u32(child),
            "interval" => r.interval = arg_string(child),
            "delay" => r.delay = arg_string(child),
            "mode" => r.mode = arg_string(child),
            _ => {}
        }
    }
    r
}

fn task_node_to_raw(node: &DeclaredNode) -> anyhow::Result<RawTask> {
    let mut t = RawTask::default();
    for child in &node.children {
        match child.name.as_str() {
            "id" => t.id = arg_string(child),
            "command" => t.command = arg_string(child),
            "argv" => t.argv = Some(argv(child)?),
            "cwd" => t.cwd = arg_string(child),
            "keep" => t.keep = arg_bool(child),
            "lifecycle" => t.lifecycle = arg_string(child),
            // `tags role="agent" "st.network"="$CATALOG"` — properties on the node.
            "tags" => {
                for entry in &child.entries {
                    if let (Some(name), Some(val)) = (entry.name.as_deref(), entry.value.as_str()) {
                        t.tags.insert(name.to_string(), val.to_string());
                    }
                }
            }
            // `env { KEY "value"; … }` — child nodes, name = key, first arg = value.
            "env" => {
                t.env.extend(env_node_to_raw(child));
            }
            _ => {}
        }
    }
    Ok(t)
}

fn env_node_to_raw(node: &DeclaredNode) -> std::collections::BTreeMap<String, String> {
    let mut env = std::collections::BTreeMap::new();
    for child in &node.children {
        if let Some(value) = arg_string(child) {
            env.insert(child.name.clone(), value);
        }
    }
    env
}
