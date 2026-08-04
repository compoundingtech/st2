//! Provider-neutral Agent Spec presentation projected into durable catalog state.

use std::path::{Path, PathBuf};

use agent_spec::spec::AgentSpec;
use anyhow::{Context, Result};
use serde::Serialize;

pub const PRESENTATION_SCHEMA: &str = "st2.agent-presentation.v1";
pub const PRESENTATION_FILE: &str = "presentation.json";

#[derive(Debug, Serialize)]
struct PresentationSnapshot<'a> {
    schema: &'static str,
    host: &'a str,
    identity: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
}

pub fn presentation_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join(PRESENTATION_FILE)
}

fn publish(spec: &AgentSpec, this_host: &str) -> Result<()> {
    let agent_dir = spec
        .path
        .parent()
        .context("Agent Spec path has no parent directory")?;
    let snapshot = PresentationSnapshot {
        schema: PRESENTATION_SCHEMA,
        host: spec.resolved_host(this_host),
        identity: &spec.identity,
        name: spec.name.as_deref(),
        description: spec.description.as_deref(),
    };
    let mut bytes = serde_json::to_vec_pretty(&snapshot)?;
    bytes.push(b'\n');
    crate::state_projection::write_atomic_if_changed(&presentation_path(agent_dir), &bytes)?;
    Ok(())
}

/// Publish every selected host-local Agent Spec independently. Failures are returned as diagnostics
/// and never acquire lifecycle authority over another agent or over task launch/adoption.
pub(crate) fn publish_local(specs: &[AgentSpec], this_host: &str) -> Vec<String> {
    specs
        .iter()
        .filter(|spec| !spec.retired && spec.resolved_host(this_host) == this_host)
        .filter_map(|spec| {
            publish(spec, this_host).err().map(|error| {
                format!(
                    "publish presentation snapshot for {}: {error:#}",
                    spec.bus_id(this_host)
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_spec::spec::{JobType, Task};
    use std::fs;

    fn spec(path: PathBuf) -> AgentSpec {
        AgentSpec {
            identity: "worker".into(),
            name: Some("Release / build \\ owner".into()),
            description: None,
            host: Some("host".into()),
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            retired: false,
            keep: false,
            restart: None,
            resources: Vec::new(),
            tasks: Vec::<Task>::new(),
            path,
        }
    }

    #[test]
    fn snapshot_is_versioned_deterministic_and_explicitly_nullable() {
        let temporary = tempfile::tempdir().unwrap();
        let agent_dir = temporary.path().join("agents/host/worker");
        fs::create_dir_all(&agent_dir).unwrap();
        let spec = spec(agent_dir.join("agent.kdl"));

        assert!(publish_local(std::slice::from_ref(&spec), "host").is_empty());
        assert_eq!(
            fs::read_to_string(presentation_path(&agent_dir)).unwrap(),
            concat!(
                "{\n",
                "  \"schema\": \"st2.agent-presentation.v1\",\n",
                "  \"host\": \"host\",\n",
                "  \"identity\": \"worker\",\n",
                "  \"name\": \"Release / build \\\\ owner\",\n",
                "  \"description\": null\n",
                "}\n"
            )
        );
    }

    #[test]
    fn only_the_selected_host_publishes_its_snapshot() {
        let temporary = tempfile::tempdir().unwrap();
        let agent_dir = temporary.path().join("agents/other/worker");
        fs::create_dir_all(&agent_dir).unwrap();
        let mut spec = spec(agent_dir.join("agent.kdl"));
        spec.host = Some("other".into());

        assert!(publish_local(std::slice::from_ref(&spec), "host").is_empty());
        assert!(!presentation_path(&agent_dir).exists());
    }
}
