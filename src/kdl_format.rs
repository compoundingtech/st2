//! KDL job parsing — lower a canonical VRS `agent.kdl` into the same [`RawSpec`] the TOML/JSON path
//! uses (spec.md §2). KDL is the canonical format. This walks the node tree by hand (KDL is not
//! serde-native) and fills a [`RawSpec`], which then flows through the shared identity/host
//! resolution. A file may hold more than one `agent` node.
//!
//! Only runner-normative fields are read; render-only fields (`harness`, `model`, `role`, `persona`,
//! `permissions`, `transport`, `strategy`) and the inert `meta{}` block are ignored.

use kdl::{KdlDocument, KdlNode};

use crate::spec::{RawRestart, RawSpec, RawTask};

/// Parse a KDL document into zero or more raw specs (one per top-level `agent` node).
pub(crate) fn parse_kdl(text: &str) -> anyhow::Result<Vec<RawSpec>> {
    let doc = KdlDocument::parse(text).map_err(|e| anyhow::anyhow!("KDL parse error: {e}"))?;
    let mut specs = Vec::new();
    for node in doc.nodes() {
        if node.name().value() == "agent" {
            specs.push(agent_node_to_raw(node)?);
        }
    }
    Ok(specs)
}

/// First positional (unnamed) argument of a node, as a string.
fn arg_string(node: &KdlNode) -> Option<String> {
    node.get(0).and_then(|v| v.as_string()).map(String::from)
}

/// First positional argument as a bool (`#true`/`#false`), defaulting to `false`.
fn arg_bool(node: &KdlNode) -> bool {
    node.get(0).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// First positional argument as an integer.
fn arg_u32(node: &KdlNode) -> Option<u32> {
    node.get(0)
        .and_then(|v| v.as_integer())
        .and_then(|i| u32::try_from(i).ok())
}

fn agent_node_to_raw(node: &KdlNode) -> anyhow::Result<RawSpec> {
    let mut raw = RawSpec {
        identity: arg_string(node), // agent "<identity>" — may be overridden by an `identity` child
        ..Default::default()
    };

    let Some(children) = node.children() else {
        return Ok(raw);
    };

    // Environment is an agent-level scope in the compact format and cascades into explicit legacy
    // tasks too. Parse it first so declaration order does not change semantics.
    for child in children.nodes() {
        if child.name().value() == "env" {
            raw.env.extend(env_node_to_raw(child));
        }
    }

    for child in children.nodes() {
        match child.name().value() {
            "identity" => raw.identity = arg_string(child).or(raw.identity),
            "host" => raw.host = arg_string(child),
            "role" => raw.role = arg_string(child),
            "type" => raw.job_type = arg_string(child),
            "workspace" => raw.workspace = arg_string(child),
            "supervisor" => raw.supervisor = arg_string(child),
            "retired" => raw.retired = arg_bool(child),
            "keep" => raw.keep = arg_bool(child),
            "restart" => raw.restart = Some(restart_node_to_raw(child)),
            "command" => raw.command = arg_string(child),
            "ding" => raw.ding = true,
            "delivery" => raw.delivery = arg_string(child),
            "env" => {}
            "pty" => {
                if let Some(name) = arg_string(child) {
                    raw.pty.insert(name, task_node_to_raw(child));
                }
            }
            "exec" => {
                if let Some(name) = arg_string(child) {
                    raw.exec.insert(name, task_node_to_raw(child));
                }
            }
            // meta, harness, model, role, persona, permissions, transport, strategy, … — ignored.
            _ => {}
        }
    }
    if raw.command.is_some() && raw.pty.contains_key("agent") {
        anyhow::bail!(
            "agent '{}' declares both compact `command` and `pty \"agent\"`; choose one form",
            raw.identity.as_deref().unwrap_or("<unnamed>")
        );
    }
    if raw.ding && raw.exec.contains_key("ding") {
        anyhow::bail!(
            "agent '{}' declares both compact `ding` and `exec \"ding\"`; choose one form",
            raw.identity.as_deref().unwrap_or("<unnamed>")
        );
    }
    Ok(raw)
}

fn restart_node_to_raw(node: &KdlNode) -> RawRestart {
    let mut r = RawRestart::default();
    let Some(children) = node.children() else {
        return r;
    };
    for child in children.nodes() {
        match child.name().value() {
            "attempts" => r.attempts = arg_u32(child),
            "interval" => r.interval = arg_string(child),
            "delay" => r.delay = arg_string(child),
            "mode" => r.mode = arg_string(child),
            _ => {}
        }
    }
    r
}

fn task_node_to_raw(node: &KdlNode) -> RawTask {
    let mut t = RawTask::default();
    let Some(children) = node.children() else {
        return t;
    };

    for child in children.nodes() {
        match child.name().value() {
            "id" => t.id = arg_string(child),
            "command" => t.command = arg_string(child),
            "cwd" => t.cwd = arg_string(child),
            "keep" => t.keep = arg_bool(child),
            // `tags role="agent" "st.network"="$CATALOG"` — properties on the node.
            "tags" => {
                for entry in child.entries() {
                    if let (Some(name), Some(val)) = (entry.name(), entry.value().as_string()) {
                        t.tags.insert(name.value().to_string(), val.to_string());
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
    t
}

fn env_node_to_raw(node: &KdlNode) -> std::collections::BTreeMap<String, String> {
    let mut env = std::collections::BTreeMap::new();
    if let Some(children) = node.children() {
        for child in children.nodes() {
            if let Some(value) = arg_string(child) {
                env.insert(child.name().value().to_string(), value);
            }
        }
    }
    env
}
