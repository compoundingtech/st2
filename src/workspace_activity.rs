//! Short-lived, read-only workspace activity snapshots.
//!
//! The snapshot joins explicit Agent Spec `workspace` declarations to the same PTY/exec generation
//! observers used by `st2 tasks --json`. It is evidence for a cleanup planner, not cleanup authority:
//! st2 never deletes a workspace and a consumer must reject incomplete or expired snapshots.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Serialize;

use crate::task_inventory::{DesiredRuntime, ObservedState, RuntimeObservation, RuntimeObserver};
use crate::{SystemRunner, discover, exec_state_dir};

pub const SCHEMA_VERSION: &str = "st2.workspace-activity.v1";
pub const PRODUCER: &str = "st2";
pub const MIN_TTL: Duration = Duration::from_secs(1);
pub const MAX_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceActivitySnapshot {
    schema_version: &'static str,
    producer: &'static str,
    epoch: Epoch,
    captured_at: String,
    expires_at: String,
    complete: bool,
    errors: Vec<String>,
    claims: Vec<WorkspaceClaim>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Epoch {
    catalog: PathBuf,
    host: String,
    catalog_generation: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceClaim {
    workspace: PathBuf,
    agents: Vec<String>,
    active_runtime_ids: Vec<String>,
    active: bool,
}

impl WorkspaceActivitySnapshot {
    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("workspace activity snapshot is serializable")
    }

    fn incomplete(
        catalog: PathBuf,
        host: String,
        now: SystemTime,
        ttl: Duration,
        error: String,
    ) -> Self {
        let captured_at = timestamp(now);
        let expires_at = timestamp(now.checked_add(ttl).unwrap_or(now));
        Self {
            schema_version: SCHEMA_VERSION,
            producer: PRODUCER,
            epoch: Epoch {
                catalog,
                host,
                catalog_generation: None,
            },
            captured_at,
            expires_at,
            complete: false,
            errors: vec![error],
            claims: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct DeclaredWorkspace {
    path: PathBuf,
    agent: String,
    runtimes: Vec<DesiredRuntime>,
}

/// Capture one bounded workspace-activity observation. All failures are represented in the JSON
/// envelope and make the command fail after printing it, so absence can never be inferred from an
/// unavailable catalog or runtime backend.
pub fn snapshot(root: &Path, host: &str, ttl: Duration) -> WorkspaceActivitySnapshot {
    snapshot_at(root, host, ttl, SystemTime::now())
}

fn snapshot_at(
    root: &Path,
    host: &str,
    ttl: Duration,
    now: SystemTime,
) -> WorkspaceActivitySnapshot {
    if !(MIN_TTL..=MAX_TTL).contains(&ttl) {
        return WorkspaceActivitySnapshot::incomplete(
            root.to_path_buf(),
            host.to_owned(),
            now,
            Duration::ZERO,
            format!(
                "snapshot TTL must be between {} and {} seconds",
                MIN_TTL.as_secs(),
                MAX_TTL.as_secs()
            ),
        );
    }
    let catalog = match root.canonicalize() {
        Ok(path) => path,
        Err(error) => {
            return WorkspaceActivitySnapshot::incomplete(
                root.to_path_buf(),
                host.to_owned(),
                now,
                ttl,
                format!("canonicalize catalog {}: {error}", root.display()),
            );
        }
    };
    let before = match crate::catalog_lock::read_fence(&catalog) {
        Ok(fence) => fence,
        Err(error) => {
            return WorkspaceActivitySnapshot::incomplete(
                catalog,
                host.to_owned(),
                now,
                ttl,
                error.to_string(),
            );
        }
    };
    let found = discover(&catalog);
    let mut errors = found
        .errors
        .iter()
        .map(|error| format!("catalog file {}: {}", error.path.display(), error.message))
        .collect::<Vec<_>>();
    let mut declared = Vec::new();
    for spec in &found.specs {
        if spec.resolved_host(host) != host {
            continue;
        }
        let Some(raw_workspace) = spec.workspace.as_deref() else {
            continue;
        };
        let expanded = crate::expand::expand_catalog(raw_workspace, &catalog);
        let spec_dir = spec.path.parent().unwrap_or(&catalog);
        let path = match spec_dir.join(&expanded).canonicalize() {
            Ok(path) => path,
            Err(error) => {
                errors.push(format!(
                    "canonicalize workspace {expanded:?} for {}: {error}",
                    spec.bus_id(host)
                ));
                continue;
            }
        };
        let bus_id = spec.bus_id(host);
        let runtimes = spec
            .tasks
            .iter()
            .filter(|task| {
                !spec.desired_state.is_running() || task.command.is_some() || task.argv.is_some()
            })
            .map(|task| DesiredRuntime {
                runtime_id: task
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("{bus_id}.{}", task.name)),
                kind: task.kind,
            })
            .collect();
        declared.push(DeclaredWorkspace {
            path,
            agent: bus_id,
            runtimes,
        });
    }
    let desired = declared
        .iter()
        .flat_map(|workspace| workspace.runtimes.iter().cloned())
        .collect::<Vec<_>>();
    let mut runtime_owners = BTreeMap::<String, usize>::new();
    for runtime in &desired {
        *runtime_owners
            .entry(runtime.runtime_id.clone())
            .or_default() += 1;
    }
    for (runtime_id, owners) in runtime_owners {
        if owners > 1 {
            errors.push(format!(
                "duplicate runtime id {runtime_id:?} is declared {owners} times"
            ));
        }
    }
    let runner = SystemRunner::new(catalog.clone(), exec_state_dir(host));
    let observed = runner.observe(&desired);
    errors.extend(observed.errors.iter().cloned());
    if !observed.complete && observed.errors.is_empty() {
        errors.push("runtime observer reported an incomplete batch".into());
    }
    let desired_ids = desired
        .iter()
        .map(|runtime| runtime.runtime_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut observations = BTreeMap::<String, RuntimeObservation>::new();
    for observation in observed.observations {
        if !desired_ids.contains(observation.runtime_id.as_str()) {
            errors.push(format!(
                "runtime observer returned undeclared id {:?}",
                observation.runtime_id
            ));
            continue;
        }
        let runtime_id = observation.runtime_id.clone();
        if observations
            .insert(runtime_id.clone(), observation)
            .is_some()
        {
            errors.push(format!(
                "runtime observer returned duplicate id {runtime_id:?}"
            ));
        }
    }
    let mut claims = BTreeMap::<PathBuf, (BTreeSet<String>, BTreeSet<String>)>::new();
    for workspace in declared {
        let claim = claims.entry(workspace.path).or_default();
        claim.0.insert(workspace.agent);
        for runtime in workspace.runtimes {
            match observations.get(&runtime.runtime_id).map(|row| &row.state) {
                Some(ObservedState::Running(_)) => {
                    claim.1.insert(runtime.runtime_id);
                }
                Some(ObservedState::Indeterminate(error)) => errors.push(error.clone()),
                None if !observed.complete => errors.push(format!(
                    "runtime observation incomplete for {:?}",
                    runtime.runtime_id
                )),
                _ => {}
            }
        }
    }
    let after_found = discover(&catalog);
    if !crate::task_inventory::same_discovery(&found, &after_found) {
        errors.push("catalog declarations changed during workspace activity observation".into());
    }
    match crate::catalog_lock::read_fence(&catalog) {
        Ok(after) if after == before => {}
        Ok(_) => {
            errors.push("catalog generation changed during workspace activity observation".into())
        }
        Err(error) => errors.push(error.to_string()),
    }
    errors.sort();
    errors.dedup();
    let claims = claims
        .into_iter()
        .map(|(workspace, (agents, active_runtime_ids))| {
            let active_runtime_ids = active_runtime_ids.into_iter().collect::<Vec<_>>();
            WorkspaceClaim {
                workspace,
                agents: agents.into_iter().collect(),
                active: !active_runtime_ids.is_empty(),
                active_runtime_ids,
            }
        })
        .collect();
    let captured_at = timestamp(now);
    let expires_at = timestamp(now.checked_add(ttl).unwrap_or(now));
    WorkspaceActivitySnapshot {
        schema_version: SCHEMA_VERSION,
        producer: PRODUCER,
        epoch: Epoch {
            catalog,
            host: host.to_owned(),
            catalog_generation: before.generation(),
        },
        captured_at,
        expires_at,
        complete: errors.is_empty() && observed.complete,
        errors,
        claims,
    }
}

fn timestamp(time: SystemTime) -> String {
    crate::exec_backend::rfc3339_utc(time).unwrap_or_else(|_| "1970-01-01T00:00:00.000Z".into())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::snapshot_at;

    #[test]
    fn expiry_is_derived_from_the_capture_time_and_ttl() {
        let snapshot = snapshot_at(
            std::path::Path::new("/definitely/missing/st2-catalog"),
            "host",
            Duration::from_secs(30),
            UNIX_EPOCH + Duration::from_secs(86_400),
        );

        assert_eq!(snapshot.captured_at, "1970-01-02T00:00:00.000Z");
        assert_eq!(snapshot.expires_at, "1970-01-02T00:00:30.000Z");
    }
}
