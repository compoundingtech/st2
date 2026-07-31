//! Typed, read-only desired-task/runtime inventory.
//!
//! This is the machine boundary for an adoption-only supervisor cutover.  It
//! deliberately does not expose a reconcile plan and cannot mutate runtime
//! state: declaration discovery and runtime observation are joined into one
//! complete-or-indeterminate receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use agent_spec::spec::{TaskKind, TaskLifecycle};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::Discovered;

pub const TASK_INVENTORY_SCHEMA: &str = "st2.task-inventory.v1";

/// Optional closed desired-state selection. Omission means both states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DesiredStateSelection {
    Running,
    Absent,
}

impl DesiredStateSelection {
    fn matches(self, retired: bool) -> bool {
        matches!(
            (self, retired),
            (Self::Running, false) | (Self::Absent, true)
        )
    }

    fn wire_name(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Absent => "absent",
        }
    }
}

/// Opaque identity over one backend's stable process-generation evidence.
pub(crate) fn generation_id(
    backend: &str,
    runtime_id: &str,
    pid: u32,
    created_at: &str,
    start_time_ticks: Option<u64>,
) -> String {
    let mut hash = Sha256::new();
    let pid = pid.to_be_bytes();
    let start_time_ticks = start_time_ticks.unwrap_or(0).to_be_bytes();
    for value in [
        backend.as_bytes(),
        runtime_id.as_bytes(),
        pid.as_slice(),
        created_at.as_bytes(),
        start_time_ticks.as_slice(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    format!("sha256:{:x}", hash.finalize())
}

pub(crate) fn is_rfc3339_utc_millis(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return false;
    }
    let digit_ranges = [0..4, 5..7, 8..10, 11..13, 14..16, 17..19, 20..23];
    if digit_ranges
        .into_iter()
        .flatten()
        .any(|index| !bytes[index].is_ascii_digit())
    {
        return false;
    }
    let number = |range: std::ops::Range<usize>| value[range].parse::<u32>().unwrap_or(u32::MAX);
    let month = number(5..7);
    let day = number(8..10);
    let year = number(0..4);
    let hour = number(11..13);
    let minute = number(14..16);
    let second = number(17..19);
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    year > 0 && (1..=days).contains(&day) && hour <= 23 && minute <= 59 && second <= 60
}

/// One positively identified live runtime generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeGeneration {
    pid: u32,
    /// Backend-issued RFC3339 UTC creation time.
    created_at: String,
    /// Opaque generation identity derived from stable backend evidence.
    generation_id: String,
}

impl RuntimeGeneration {
    pub fn new(pid: u32, created_at: String, generation_id: String) -> Result<Self, String> {
        if pid == 0 {
            return Err("runtime pid must be positive".into());
        }
        if !is_rfc3339_utc_millis(&created_at) {
            return Err(
                "runtime createdAt is not a valid RFC3339 UTC millisecond timestamp".into(),
            );
        }
        if generation_id.is_empty() {
            return Err("runtime generationId is empty".into());
        }
        Ok(Self {
            pid,
            created_at,
            generation_id,
        })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }
}

/// Closed runtime observation. Invalid state/generation/error products are unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedState {
    Running(RuntimeGeneration),
    Exited,
    Vanished,
    Absent,
    Indeterminate(String),
}

/// One backend observation keyed by the exact declared runtime id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservation {
    pub runtime_id: String,
    pub state: ObservedState,
}

/// One coherent observation attempt. Any backend uncertainty makes the batch
/// incomplete; callers must not infer absence from its missing rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservationBatch {
    pub complete: bool,
    pub observations: Vec<RuntimeObservation>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredRuntime {
    pub runtime_id: String,
    pub kind: TaskKind,
}

/// Read-only backend boundary used by the CLI and by deterministic tests.
pub trait RuntimeObserver {
    fn observe(&self, desired: &[DesiredRuntime]) -> ObservationBatch;
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskInventory {
    schema: &'static str,
    catalog: PathBuf,
    host: String,
    selection: TaskSelection,
    complete: bool,
    errors: Vec<String>,
    tasks: Vec<TaskRow>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskSelection {
    desired_states: Vec<&'static str>,
}

impl TaskInventory {
    pub fn incomplete(
        catalog: PathBuf,
        host: String,
        error: String,
        selection: Option<DesiredStateSelection>,
    ) -> Self {
        Self {
            schema: TASK_INVENTORY_SCHEMA,
            catalog,
            host,
            selection: selection_receipt(selection),
            complete: false,
            errors: vec![error],
            tasks: Vec::new(),
        }
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("task inventory contains only serializable values")
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskRow {
    agent: String,
    task: String,
    runtime_id: String,
    kind: &'static str,
    lifecycle: &'static str,
    retired: bool,
    desired_state: &'static str,
    runtime: RuntimeJson,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeJson {
    state: &'static str,
    pid: Option<u32>,
    created_at: Option<String>,
    generation_id: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
struct DesiredTask {
    agent: String,
    task: String,
    runtime_id: String,
    kind: TaskKind,
    lifecycle: TaskLifecycle,
    retired: bool,
}

/// Join one discovered declaration snapshot to one runtime snapshot.
///
/// The caller holds the catalog shared lock across both discovery and this
/// observation. `catalog` must already be canonicalized.
pub fn inventory(
    catalog: &Path,
    host: &str,
    found: &Discovered,
    observer: &dyn RuntimeObserver,
    selection: Option<DesiredStateSelection>,
) -> TaskInventory {
    let mut errors = found
        .errors
        .iter()
        .map(|error| format!("catalog file {}: {}", error.path.display(), error.message))
        .collect::<Vec<_>>();
    let mut desired = Vec::new();
    let mut runtime_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for spec in &found.specs {
        if spec.resolved_host(host) != host {
            continue;
        }
        let bus_id = spec.bus_id(host);
        for task in &spec.tasks {
            // Active declaration-only metadata has no desired runtime. Retired tasks remain in the
            // inventory even without launch material so stale generations cannot disappear from a
            // complete teardown/cutover receipt.
            if !spec.retired && task.command.is_none() && task.argv.is_none() {
                continue;
            }
            let runtime_id = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{bus_id}.{}", task.name));
            runtime_owners
                .entry(runtime_id.clone())
                .or_default()
                .push(format!("{bus_id}/{}", task.name));
            desired.push(DesiredTask {
                agent: bus_id.clone(),
                task: task.name.clone(),
                runtime_id,
                kind: task.kind,
                lifecycle: task.lifecycle,
                retired: spec.retired,
            });
        }
    }

    for (runtime_id, owners) in &runtime_owners {
        if owners.len() > 1 {
            errors.push(format!(
                "duplicate runtime id {runtime_id:?} is declared by {}",
                owners.join(", ")
            ));
        }
    }

    // Catalog/identity/duplicate admission above is global. Desired-state selection is applied
    // only after those declaration errors are known, and before any runtime backend observation.
    desired.retain(|task| selection.is_none_or(|selection| selection.matches(task.retired)));
    let desired_runtimes = desired
        .iter()
        .map(|task| DesiredRuntime {
            runtime_id: task.runtime_id.clone(),
            kind: task.kind,
        })
        .collect::<Vec<_>>();
    let desired_ids = desired_runtimes
        .iter()
        .map(|runtime| runtime.runtime_id.as_str())
        .collect::<BTreeSet<_>>();
    let observed = observer.observe(&desired_runtimes);
    for error in &observed.errors {
        push_error(&mut errors, error.clone());
    }
    if !observed.complete && observed.errors.is_empty() {
        push_error(
            &mut errors,
            "runtime observer reported an incomplete batch".into(),
        );
    }
    let mut by_id: BTreeMap<String, RuntimeObservation> = BTreeMap::new();
    let mut duplicate_observations = BTreeSet::new();
    for observation in observed.observations {
        if !desired_ids.contains(observation.runtime_id.as_str()) {
            push_error(
                &mut errors,
                format!(
                    "runtime observer returned undeclared id {:?}",
                    observation.runtime_id
                ),
            );
            continue;
        }
        if let ObservedState::Indeterminate(error) = &observation.state {
            push_error(&mut errors, error.clone());
        }
        let runtime_id = observation.runtime_id.clone();
        if by_id.insert(runtime_id.clone(), observation).is_some() {
            duplicate_observations.insert(runtime_id);
        }
    }
    for runtime_id in duplicate_observations {
        errors.push(format!(
            "runtime observer returned duplicate id {runtime_id:?}"
        ));
        by_id.insert(
            runtime_id.clone(),
            RuntimeObservation {
                runtime_id,
                state: ObservedState::Indeterminate("duplicate runtime observation".into()),
            },
        );
    }

    let observation_complete = observed.complete && observed.errors.is_empty();
    desired.sort_by(|a, b| {
        (&a.agent, &a.task, &a.runtime_id).cmp(&(&b.agent, &b.task, &b.runtime_id))
    });
    let tasks = desired
        .into_iter()
        .map(|task| {
            let observation = by_id.remove(&task.runtime_id).unwrap_or_else(|| {
                if observation_complete {
                    RuntimeObservation {
                        runtime_id: task.runtime_id.clone(),
                        state: ObservedState::Absent,
                    }
                } else {
                    RuntimeObservation {
                        runtime_id: task.runtime_id.clone(),
                        state: ObservedState::Indeterminate(
                            "runtime observation incomplete".into(),
                        ),
                    }
                }
            });
            let (state, pid, created_at, generation_id, error) = match observation.state {
                ObservedState::Running(generation) => (
                    "running",
                    Some(generation.pid),
                    Some(generation.created_at),
                    Some(generation.generation_id),
                    None,
                ),
                ObservedState::Exited => ("exited", None, None, None, None),
                ObservedState::Vanished => ("vanished", None, None, None, None),
                ObservedState::Absent => ("absent", None, None, None, None),
                ObservedState::Indeterminate(error) => {
                    ("indeterminate", None, None, None, Some(error))
                }
            };
            TaskRow {
                agent: task.agent,
                task: task.task,
                runtime_id: task.runtime_id,
                kind: match task.kind {
                    TaskKind::Pty => "pty",
                    TaskKind::Exec => "exec",
                },
                lifecycle: match task.lifecycle {
                    TaskLifecycle::Service => "service",
                    TaskLifecycle::AdoptOnly => "adopt-only",
                },
                retired: task.retired,
                desired_state: if task.retired { "absent" } else { "running" },
                runtime: RuntimeJson {
                    state,
                    pid,
                    created_at,
                    generation_id,
                    error,
                },
            }
        })
        .collect();

    TaskInventory {
        schema: TASK_INVENTORY_SCHEMA,
        catalog: catalog.to_path_buf(),
        host: host.to_owned(),
        selection: selection_receipt(selection),
        complete: errors.is_empty() && observation_complete,
        errors,
        tasks,
    }
}

fn selection_receipt(selection: Option<DesiredStateSelection>) -> TaskSelection {
    TaskSelection {
        desired_states: match selection {
            Some(selection) => vec![selection.wire_name()],
            None => vec!["running", "absent"],
        },
    }
}

fn push_error(errors: &mut Vec<String>, error: String) {
    if !errors.contains(&error) {
        errors.push(error);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;

    use serde_json::Value;

    use super::*;

    #[derive(Clone)]
    struct FixedObserver(ObservationBatch);

    impl RuntimeObserver for FixedObserver {
        fn observe(&self, _desired: &[DesiredRuntime]) -> ObservationBatch {
            self.0.clone()
        }
    }

    struct RecordingObserver {
        desired: RefCell<Vec<String>>,
        batch: ObservationBatch,
    }

    impl RuntimeObserver for RecordingObserver {
        fn observe(&self, desired: &[DesiredRuntime]) -> ObservationBatch {
            *self.desired.borrow_mut() = desired
                .iter()
                .map(|runtime| runtime.runtime_id.clone())
                .collect();
            self.batch.clone()
        }
    }

    fn write_agent(catalog: &Path, host: &str, identity: &str, body: &str) {
        let dir = catalog.join("agents").join(host).join(identity);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent.kdl"),
            format!("agent \"{identity}\" {{ host \"{host}\"; {body} }}\n"),
        )
        .unwrap();
    }

    fn json(catalog: &Path, host: &str, observer: ObservationBatch) -> serde_json::Value {
        let found = crate::discover(catalog);
        serde_json::from_str(
            &inventory(catalog, host, &found, &FixedObserver(observer), None).to_json(),
        )
        .unwrap()
    }

    fn running(id: &str, pid: u32) -> RuntimeObservation {
        RuntimeObservation {
            runtime_id: id.into(),
            state: ObservedState::Running(
                RuntimeGeneration::new(
                    pid,
                    "2026-07-31T10:00:00.000Z".into(),
                    format!("sha256:g-{pid}"),
                )
                .unwrap(),
            ),
        }
    }

    #[test]
    fn stable_wire_shape_maps_pty_exec_explicit_default_and_ignores_foreign_host() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"
              pty "agent" {
                id "h.worker"
                lifecycle "adopt-only"
                argv "agent-bin"
              }
              exec "ding" { argv "st2" "ding" }
            "#,
        );
        write_agent(
            tmp.path(),
            "other",
            "foreign",
            r#"pty "agent" { argv "must-not-appear" }"#,
        );
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![running("h.worker", 11), running("h.worker.ding", 12)],
                errors: vec![],
            },
        );
        assert_eq!(value["schema"], TASK_INVENTORY_SCHEMA);
        assert_eq!(value["catalog"], tmp.path().to_str().unwrap());
        assert_eq!(value["host"], "h");
        assert_eq!(
            value["selection"]["desiredStates"],
            serde_json::json!(["running", "absent"])
        );
        assert_eq!(value["complete"], true);
        assert_eq!(value["errors"], Value::Array(vec![]));
        assert_eq!(value["tasks"].as_array().unwrap().len(), 2);
        assert_eq!(
            value["tasks"][0],
            serde_json::json!({
                "agent": "h.worker",
                "task": "agent",
                "runtimeId": "h.worker",
                "kind": "pty",
                "lifecycle": "adopt-only",
                "retired": false,
                "desiredState": "running",
                "runtime": {
                    "state": "running",
                    "pid": 11,
                    "createdAt": "2026-07-31T10:00:00.000Z",
                    "generationId": "sha256:g-11",
                    "error": null
                }
            })
        );
        assert_eq!(value["tasks"][1]["runtimeId"], "h.worker.ding");
        assert_eq!(value["tasks"][1]["kind"], "exec");
        assert_eq!(value["tasks"][1]["lifecycle"], "service");
        assert!(
            value["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["agent"] != "other.foreign")
        );
    }

    #[test]
    fn complete_missing_runtime_is_positive_absence_but_incomplete_is_indeterminate() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { id "h.worker"; argv "agent-bin" }"#,
        );

        let absent = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![],
                errors: vec![],
            },
        );
        assert_eq!(absent["complete"], true);
        assert_eq!(absent["tasks"][0]["runtime"]["state"], "absent");
        assert_eq!(absent["tasks"][0]["runtime"]["pid"], Value::Null);

        let indeterminate = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: false,
                observations: vec![],
                errors: vec!["pty timed out".into()],
            },
        );
        assert_eq!(indeterminate["complete"], false);
        assert_eq!(
            indeterminate["tasks"][0]["runtime"]["state"],
            "indeterminate"
        );
        assert_eq!(
            indeterminate["tasks"][0]["runtime"]["error"],
            "runtime observation incomplete"
        );
    }

    #[test]
    fn retired_task_desires_absence_without_erasing_observed_generation() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "old",
            r#"retired #true; pty "agent" { id "h.old" }"#,
        );
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![running("h.old", 42)],
                errors: vec![],
            },
        );
        assert_eq!(value["tasks"][0]["retired"], true);
        assert_eq!(value["tasks"][0]["desiredState"], "absent");
        assert_eq!(value["tasks"][0]["runtime"]["state"], "running");
        assert_eq!(value["tasks"][0]["runtime"]["pid"], 42);
    }

    #[test]
    fn running_selection_filters_before_observation_and_serializes_scope() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "active",
            r#"pty "agent" { id "h.active"; argv "agent-bin" }"#,
        );
        write_agent(
            tmp.path(),
            "h",
            "retired",
            r#"retired #true; pty "agent" { id "h.retired" }"#,
        );
        let observer = RecordingObserver {
            desired: RefCell::new(Vec::new()),
            batch: ObservationBatch {
                complete: true,
                observations: vec![running("h.active", 7)],
                errors: vec![],
            },
        };
        let found = crate::discover(tmp.path());
        let value: Value = serde_json::from_str(
            &inventory(
                tmp.path(),
                "h",
                &found,
                &observer,
                Some(DesiredStateSelection::Running),
            )
            .to_json(),
        )
        .unwrap();
        assert_eq!(&*observer.desired.borrow(), &["h.active"]);
        assert_eq!(
            value["selection"]["desiredStates"],
            serde_json::json!(["running"])
        );
        assert_eq!(value["complete"], true);
        assert_eq!(value["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(value["tasks"][0]["runtimeId"], "h.active");
    }

    #[test]
    fn selection_keeps_global_errors_and_selected_runtime_uncertainty() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "active",
            r#"pty "agent" { id "shared"; argv "agent-bin" }"#,
        );
        write_agent(
            tmp.path(),
            "h",
            "retired",
            r#"retired #true; pty "agent" { id "shared" }"#,
        );
        let found = crate::discover(tmp.path());
        let value: Value = serde_json::from_str(
            &inventory(
                tmp.path(),
                "h",
                &found,
                &FixedObserver(ObservationBatch {
                    complete: true,
                    observations: vec![RuntimeObservation {
                        runtime_id: "shared".into(),
                        state: ObservedState::Indeterminate("selected runtime unreadable".into()),
                    }],
                    errors: vec![],
                }),
                Some(DesiredStateSelection::Running),
            )
            .to_json(),
        )
        .unwrap();
        assert_eq!(value["complete"], false);
        assert!(
            value["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|error| error.as_str().unwrap().contains("duplicate runtime id"))
        );
        assert!(
            value["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|error| error.as_str().unwrap().contains("unreadable"))
        );
    }

    #[test]
    fn duplicate_observation_is_indeterminate_and_non_complete() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { id "h.worker"; argv "agent-bin" }"#,
        );
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![running("h.worker", 1), running("h.worker", 2)],
                errors: vec![],
            },
        );
        assert_eq!(value["complete"], false);
        assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
        assert!(
            value["errors"][0]
                .as_str()
                .unwrap()
                .contains("duplicate id")
        );
    }

    #[test]
    fn observer_cannot_claim_complete_running_without_generation_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { argv "agent-bin" }"#,
        );
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![RuntimeObservation {
                    runtime_id: "h.worker.agent".into(),
                    state: ObservedState::Indeterminate(
                        "running runtime lacks complete generation evidence".into(),
                    ),
                }],
                errors: vec![],
            },
        );
        assert_eq!(value["complete"], false);
        assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
        assert!(
            value["errors"][0]
                .as_str()
                .unwrap()
                .contains("lacks complete generation")
        );
    }

    #[test]
    fn generation_constructor_rejects_invalid_evidence() {
        assert!(!is_rfc3339_utc_millis("2026-02-31T10:00:00.000Z"));
        assert!(!is_rfc3339_utc_millis("2025-02-29T10:00:00.000Z"));
        assert!(is_rfc3339_utc_millis("2024-02-29T10:00:00.000Z"));

        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { argv "agent-bin" }"#,
        );
        assert!(
            RuntimeGeneration::new(7, "2026-02-31T10:00:00.000Z".into(), "sha256:g-7".into())
                .is_err()
        );
        assert!(
            RuntimeGeneration::new(0, "2026-07-31T10:00:00.000Z".into(), "sha256:g-0".into())
                .is_err()
        );
        let observation = RuntimeObservation {
            runtime_id: "h.worker.agent".into(),
            state: ObservedState::Indeterminate("backend evidence is invalid".into()),
        };
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![observation],
                errors: vec![],
            },
        );
        assert_eq!(value["complete"], false);
        assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
    }

    #[test]
    fn closed_runtime_states_serialize_exactly() {
        let tmp = tempfile::tempdir().unwrap();
        for identity in ["a", "b", "c"] {
            write_agent(
                tmp.path(),
                "h",
                identity,
                &format!(r#"pty "agent" {{ id "h.{identity}"; argv "x" }}"#),
            );
        }
        let value = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: false,
                observations: vec![
                    RuntimeObservation {
                        runtime_id: "h.a".into(),
                        state: ObservedState::Exited,
                    },
                    RuntimeObservation {
                        runtime_id: "h.b".into(),
                        state: ObservedState::Vanished,
                    },
                    RuntimeObservation {
                        runtime_id: "h.c".into(),
                        state: ObservedState::Indeterminate("unreadable".into()),
                    },
                ],
                errors: vec!["one runtime unreadable".into()],
            },
        );
        assert_eq!(value["tasks"][0]["runtime"]["state"], "exited");
        assert_eq!(value["tasks"][1]["runtime"]["state"], "vanished");
        assert_eq!(value["tasks"][2]["runtime"]["state"], "indeterminate");
    }
}
