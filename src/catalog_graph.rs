//! One fail-closed, versioned observation of the catalog declaration graph and agent runtime rows.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use agent_spec::DeclaredValue;
use agent_spec::discovery::{Declared, DiscoveredDeclaration, path_defaults};
use agent_spec::spec::AgentSpec;
use anyhow::{Context, Result};
use serde::Serialize;

pub const CATALOG_GRAPH_SCHEMA: &str = "st2.catalog-graph.v1";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogGraph {
    pub schema: &'static str,
    pub agent_spec_revision: &'static str,
    pub complete: bool,
    pub roots: GraphRoots,
    pub agents: Vec<GraphAgent>,
    pub declarations: Vec<GraphDeclaration>,
    pub conflicts: Vec<GraphConflict>,
    pub issues: Vec<GraphIssue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphRoots {
    pub catalog: PathBuf,
    pub st_root: PathBuf,
    pub pty_root: PathBuf,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphAgent {
    pub id: String,
    pub identity: String,
    pub host: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub supervisor: Option<String>,
    /// Agent Spec `role`, named for the consumer concept it supplies.
    pub persona: Option<String>,
    pub workspace: Option<String>,
    pub resolved_workspace: Option<PathBuf>,
    pub session_driver: Option<String>,
    pub desired_state: String,
    pub desired_state_reason: Option<String>,
    pub source: GraphSource,
    pub resources: Vec<GraphResource>,
    pub runtime: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSource {
    pub path: String,
    pub declared_identity: Option<String>,
    pub declared_host: Option<String>,
    pub identity_provenance: &'static str,
    pub host_provenance: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphResource {
    pub name: String,
    pub uri: String,
    pub reason: String,
    pub inactive_reason: Option<String>,
    pub selector: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphDeclaration {
    pub path: String,
    pub format: String,
    pub status: &'static str,
    pub agents: Vec<PartialAgent>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialAgent {
    pub identity: Option<String>,
    pub host: Option<String>,
    pub supervisor: Option<String>,
    pub persona: Option<String>,
    pub workspace: Option<String>,
    pub session_driver: Option<String>,
    pub desired_state: Option<String>,
    pub desired_state_reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphConflict {
    pub kind: &'static str,
    pub identity: String,
    pub paths: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphIssue {
    pub severity: &'static str,
    pub code: &'static str,
    pub path: String,
    pub agent: Option<String>,
    pub message: String,
}

/// Observe the declaration graph and runtime rows under one shared catalog-authoring fence.
pub fn snapshot(root: &Path, this_host: &str) -> Result<CatalogGraph> {
    let root = root
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", root.display()))?;
    let _lock = crate::CatalogLock::shared(&root)
        .context("acquire shared catalog-authoring lock for catalog graph")?;
    let found = crate::discover_strict(&root);
    let report = crate::validate::validate_discovered(&root, Some(this_host), &found);

    let mut runtime_by_path: BTreeMap<PathBuf, Vec<crate::agents::AgentRow>> = BTreeMap::new();
    for row in crate::agents::roster_from_discovered(&found, &root, this_host) {
        runtime_by_path
            .entry(row.source_path.clone())
            .or_default()
            .push(row);
    }

    let mut agents = found
        .specs
        .iter()
        .map(|spec| graph_agent(&root, this_host, &found.declarations, spec, &mut runtime_by_path))
        .collect::<Vec<_>>();
    agents.sort_by(|left, right| left.id.cmp(&right.id).then(left.source.path.cmp(&right.source.path)));

    let error_paths = report
        .issues
        .iter()
        .filter(|issue| issue.severity == crate::validate::Severity::Error)
        .map(|issue| issue.path.as_str())
        .collect::<BTreeSet<_>>();
    let declarations = found
        .declarations
        .iter()
        .map(|declaration| graph_declaration(&root, declaration, &error_paths))
        .collect();

    let conflicts = duplicate_identity_conflicts(&root, this_host, &found.specs);
    let complete = report.errors() == 0;
    let issues = report
        .issues
        .into_iter()
        .map(|issue| GraphIssue {
            severity: issue.severity.tag(),
            code: issue.code,
            path: issue.path,
            agent: issue.agent,
            message: issue.message,
        })
        .collect();

    Ok(CatalogGraph {
        schema: CATALOG_GRAPH_SCHEMA,
        agent_spec_revision: agent_spec::AGENT_SPEC_REVISION,
        complete,
        roots: GraphRoots {
            catalog: root.clone(),
            st_root: root.clone(),
            pty_root: crate::run::effective_pty_root(&root),
        },
        agents,
        declarations,
        conflicts,
        issues,
    })
}

fn graph_agent(
    root: &Path,
    this_host: &str,
    declarations: &[DiscoveredDeclaration],
    spec: &AgentSpec,
    runtime_by_path: &mut BTreeMap<PathBuf, Vec<crate::agents::AgentRow>>,
) -> GraphAgent {
    let source_declaration = declarations.iter().find(|entry| entry.path == spec.path);
    let (path_identity, path_host) = path_defaults(root, &spec.path);
    let raw = source_declaration.and_then(|entry| match_declared(&entry.agents, spec, path_identity.as_deref()));
    let id = spec.bus_id(this_host);
    let runtime = runtime_by_path
        .get_mut(&spec.path)
        .and_then(|rows| rows.iter().position(|row| row.identity == id).map(|index| rows.remove(index)))
        .map(|row| crate::agents::graph_runtime_value(&row))
        .unwrap_or(serde_json::Value::Null);
    let resolved_workspace = spec.workspace.as_deref().and_then(|workspace| {
        crate::expand::resolve_spec_path(
            workspace,
            root,
            spec.path.parent().unwrap_or(root),
        )
        .ok()
    });
    let session_driver = spec
        .session_driver
        .map(|driver| driver.as_str().to_owned())
        .or_else(|| spec.driver.as_ref().map(|driver| driver.name().to_owned()));

    GraphAgent {
        id,
        identity: spec.identity.clone(),
        host: spec.resolved_host(this_host).to_owned(),
        name: spec.name.clone(),
        description: spec.description.clone(),
        supervisor: spec.supervisor.clone(),
        persona: spec.role.clone(),
        workspace: spec.workspace.clone(),
        resolved_workspace,
        session_driver,
        desired_state: spec.desired_state.as_str().to_owned(),
        desired_state_reason: spec.desired_state.reason().map(str::to_owned),
        source: GraphSource {
            path: relative(root, &spec.path),
            declared_identity: raw.and_then(|declared| declared.identity.clone()),
            declared_host: raw.and_then(|declared| declared.host.clone()),
            identity_provenance: if raw.is_some_and(|declared| declared.identity.is_some()) {
                "declaration"
            } else {
                "path"
            },
            host_provenance: if raw.is_some_and(|declared| declared.host.is_some()) {
                "declaration"
            } else if path_host.is_some() {
                "path"
            } else {
                "runtimeDefault"
            },
        },
        resources: spec
            .resources
            .iter()
            .map(|resource| GraphResource {
                name: resource.name().to_owned(),
                uri: resource.uri().to_owned(),
                reason: resource.reason().to_owned(),
                inactive_reason: resource.inactive_reason().map(str::to_owned),
                selector: resource.selector().cloned(),
            })
            .collect(),
        runtime,
    }
}

fn match_declared<'a>(
    declared: &'a [Declared],
    spec: &AgentSpec,
    path_identity: Option<&str>,
) -> Option<&'a Declared> {
    declared
        .iter()
        .find(|raw| raw.identity.as_deref() == Some(spec.identity.as_str()))
        .or_else(|| {
            (path_identity == Some(spec.identity.as_str()))
                .then(|| declared.iter().find(|raw| raw.identity.is_none()))
                .flatten()
        })
}

fn graph_declaration<'a>(
    root: &Path,
    declaration: &DiscoveredDeclaration,
    error_paths: &BTreeSet<&'a str>,
) -> GraphDeclaration {
    let path = relative(root, &declaration.path);
    let agents = declaration
        .parse
        .as_ref()
        .and_then(|parse| parse.document.as_ref())
        .map(|document| {
            document
                .agents
                .iter()
                .map(|agent| {
                    let desired = agent.field("desired-state");
                    PartialAgent {
                        identity: agent.identity().and_then(DeclaredValue::as_str).map(str::to_owned),
                        host: declared_field(agent, "host"),
                        supervisor: declared_field(agent, "supervisor"),
                        persona: declared_field(agent, "role"),
                        workspace: declared_field(agent, "workspace"),
                        session_driver: declared_field(agent, "session-driver"),
                        desired_state: desired
                            .and_then(|node| node.argument(0))
                            .and_then(DeclaredValue::as_str)
                            .map(str::to_owned),
                        desired_state_reason: desired
                            .and_then(|node| node.property("reason"))
                            .and_then(DeclaredValue::as_str)
                            .map(str::to_owned),
                    }
                })
                .collect()
        })
        .unwrap_or_else(|| {
            declaration
                .agents
                .iter()
                .map(|agent| PartialAgent {
                    identity: agent.identity.clone(),
                    host: agent.host.clone(),
                    supervisor: None,
                    persona: None,
                    workspace: None,
                    session_driver: None,
                    desired_state: None,
                    desired_state_reason: None,
                })
                .collect()
        });
    GraphDeclaration {
        format: declaration
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("")
            .to_owned(),
        status: if error_paths.contains(path.as_str()) {
            "invalid"
        } else {
            "valid"
        },
        path,
        agents,
    }
}

fn declared_field(agent: &agent_spec::DeclaredAgent, name: &str) -> Option<String> {
    agent
        .field(name)
        .and_then(|node| node.argument(0))
        .and_then(DeclaredValue::as_str)
        .map(str::to_owned)
}

fn duplicate_identity_conflicts(
    root: &Path,
    this_host: &str,
    specs: &[AgentSpec],
) -> Vec<GraphConflict> {
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for spec in specs {
        grouped
            .entry(spec.bus_id(this_host))
            .or_default()
            .push(relative(root, &spec.path));
    }
    grouped
        .into_iter()
        .filter_map(|(identity, mut paths)| {
            if paths.len() < 2 {
                return None;
            }
            paths.sort();
            Some(GraphConflict {
                kind: "duplicateIdentity",
                identity,
                paths,
            })
        })
        .collect()
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).display().to_string()
}
