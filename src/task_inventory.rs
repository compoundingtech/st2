//! Typed, read-only desired-task/runtime inventory.
//!
//! This is a diagnostic and automation boundary. It deliberately does not
//! expose a reconcile plan and cannot mutate catalog or runtime state.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use agent_spec::spec::{AgentSpec, RestartMode, Task, TaskKind, TaskLifecycle};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::Discovered;

pub const TASK_INVENTORY_SCHEMA: &str = "st2.task-inventory.v1";
pub(crate) const LAUNCH_GENERATION_TAG: &str = "st2.launch_generation";

pub(crate) fn valid_launch_generation(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value["sha256:".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn digest<'a>(domain: &str, fields: impl IntoIterator<Item = (&'a str, Vec<u8>)>) -> String {
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    for (name, value) in fields {
        hash.update((name.len() as u64).to_be_bytes());
        hash.update(name.as_bytes());
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    format!("sha256:{:x}", hash.finalize())
}

fn push_optional(
    fields: &mut Vec<(&'static str, Vec<u8>)>,
    name: &'static str,
    value: Option<&str>,
) {
    let mut encoded = vec![u8::from(value.is_some())];
    encoded.extend_from_slice(value.unwrap_or_default().as_bytes());
    fields.push((name, encoded));
}

/// Semantic identity of one accepted declaration. Source path, comments, and
/// formatting are deliberately excluded; every parsed declaration field is included.
pub fn declaration_revision(spec: &AgentSpec) -> String {
    let mut fields = Vec::new();
    fields.push(("identity", spec.identity.as_bytes().to_vec()));
    push_optional(&mut fields, "host", spec.host.as_deref());
    push_optional(&mut fields, "role", spec.role.as_deref());
    fields.push(("jobType", b"service".to_vec()));
    push_optional(&mut fields, "workspace", spec.workspace.as_deref());
    push_optional(&mut fields, "supervisor", spec.supervisor.as_deref());
    fields.push(("retired", vec![u8::from(spec.retired)]));
    fields.push(("keep", vec![u8::from(spec.keep)]));
    match &spec.restart {
        Some(restart) => {
            fields.push(("restart.present", vec![1]));
            fields.push(("restart.attempts", restart.attempts.to_be_bytes().to_vec()));
            fields.push((
                "restart.interval",
                restart.interval.as_nanos().to_be_bytes().to_vec(),
            ));
            fields.push((
                "restart.delay",
                restart.delay.as_nanos().to_be_bytes().to_vec(),
            ));
            fields.push((
                "restart.mode",
                match restart.mode {
                    RestartMode::Fail => b"fail".to_vec(),
                    RestartMode::Delay => b"delay".to_vec(),
                },
            ));
        }
        None => fields.push(("restart.present", vec![0])),
    }
    for resource in &spec.resources {
        fields.push(("resource.name", resource.name().as_bytes().to_vec()));
        fields.push(("resource.tag", resource.tag().as_bytes().to_vec()));
        fields.push(("resource.uri", resource.uri().as_bytes().to_vec()));
    }
    for task in &spec.tasks {
        fields.push(("task.name", task.name.as_bytes().to_vec()));
        fields.push((
            "task.kind",
            match task.kind {
                TaskKind::Pty => b"pty".to_vec(),
                TaskKind::Exec => b"exec".to_vec(),
            },
        ));
        fields.push(("task.derived", vec![u8::from(task.derived)]));
        push_optional(&mut fields, "task.id", task.id.as_deref());
        push_optional(&mut fields, "task.command", task.command.as_deref());
        match &task.argv {
            Some(argv) => {
                fields.push(("task.argv.present", vec![1]));
                for arg in argv {
                    fields.push(("task.argv", arg.as_bytes().to_vec()));
                }
            }
            None => fields.push(("task.argv.present", vec![0])),
        }
        push_optional(&mut fields, "task.cwd", task.cwd.as_deref());
        for (name, value) in &task.tags {
            fields.push(("task.tag.name", name.as_bytes().to_vec()));
            fields.push(("task.tag.value", value.as_bytes().to_vec()));
        }
        for (name, value) in &task.env {
            fields.push(("task.env.name", name.as_bytes().to_vec()));
            fields.push(("task.env.value", value.as_bytes().to_vec()));
        }
        fields.push(("task.keep", vec![u8::from(task.keep)]));
        fields.push((
            "task.lifecycle",
            match task.lifecycle {
                TaskLifecycle::Service => b"service".to_vec(),
                TaskLifecycle::AdoptOnly => b"adopt-only".to_vec(),
            },
        ));
    }
    digest("st2.declaration-revision.v1", fields)
}

/// Desired launch generation over the effective process-creation and supervision contract.
/// Resource bindings and descriptive agent metadata are intentionally absent.
pub fn desired_launch_generation(
    spec: &AgentSpec,
    task: &Task,
    host: &str,
    catalog: &Path,
) -> Option<String> {
    let target = crate::reconcile::task_target(spec, task, host)?;
    let spec_dir = spec.path.parent().unwrap_or_else(|| Path::new("."));
    Some(launch_generation_for_target(&target, spec_dir, catalog))
}

pub(crate) fn launch_generation_for_target(
    target: &crate::reconcile::TaskTarget,
    spec_dir: &Path,
    catalog: &Path,
) -> String {
    let (launch_kind, launch_args) = match &target.launch {
        crate::reconcile::TaskLaunch::Shell(command) => ("shell", vec![command.clone()]),
        crate::reconcile::TaskLaunch::Argv(argv) => ("argv", argv.clone()),
    };
    let cwd = crate::run::resolve_task_cwd(target, spec_dir, catalog);
    let mut fields = vec![
        ("runtimeId", target.pty_id.as_bytes().to_vec()),
        (
            "kind",
            match target.kind {
                TaskKind::Pty => b"pty".to_vec(),
                TaskKind::Exec => b"exec".to_vec(),
            },
        ),
        ("launch.kind", launch_kind.as_bytes().to_vec()),
        ("cwd", cwd.as_os_str().as_encoded_bytes().to_vec()),
        ("catalog", catalog.as_os_str().as_encoded_bytes().to_vec()),
        (
            "restart.attempts",
            target.restart.attempts.to_be_bytes().to_vec(),
        ),
        (
            "restart.interval",
            target.restart.interval.as_nanos().to_be_bytes().to_vec(),
        ),
        (
            "restart.delay",
            target.restart.delay.as_nanos().to_be_bytes().to_vec(),
        ),
        (
            "restart.mode",
            match target.restart.mode {
                RestartMode::Fail => b"fail".to_vec(),
                RestartMode::Delay => b"delay".to_vec(),
            },
        ),
        (
            "lifecycle",
            match target.lifecycle {
                TaskLifecycle::Service => b"service".to_vec(),
                TaskLifecycle::AdoptOnly => b"adopt-only".to_vec(),
            },
        ),
        ("keep", vec![u8::from(target.keep)]),
    ];
    for arg in launch_args {
        fields.push((
            "launch.arg",
            if launch_kind == "argv" {
                crate::expand::expand_catalog(&arg, catalog).into_bytes()
            } else {
                arg.into_bytes()
            },
        ));
    }
    let mut env = BTreeMap::from([
        ("CATALOG".to_owned(), catalog.display().to_string()),
        ("ST_ROOT".to_owned(), catalog.display().to_string()),
        (
            "PTY_ROOT".to_owned(),
            crate::run::effective_pty_root(catalog)
                .display()
                .to_string(),
        ),
    ]);
    if target.kind == TaskKind::Pty {
        env.insert("TERM".to_owned(), "xterm-256color".to_owned());
    }
    if let Ok(path) = crate::hooks::hooks_root() {
        env.insert("ST_HOOKS".to_owned(), path.display().to_string());
    }
    for (name, value) in &target.env {
        let value = if name == "PTY_ROOT" {
            crate::run::effective_pty_root(catalog)
                .display()
                .to_string()
        } else {
            crate::expand::expand_catalog(value, catalog)
        };
        env.insert(name.clone(), value);
    }
    for (name, value) in env {
        fields.push(("env.name", name.into_bytes()));
        fields.push(("env.value", value.into_bytes()));
    }
    if target.kind == TaskKind::Pty {
        for (name, value) in &target.tags {
            fields.push(("tag.name", name.as_bytes().to_vec()));
            fields.push((
                "tag.value",
                crate::expand::expand_catalog(value, catalog).into_bytes(),
            ));
        }
    }
    digest("st2.launch-generation.v1", fields)
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

const LAUNCH_BINDING_SCHEMA: &str = "st2.launch-binding.v1";

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchBinding {
    schema: String,
    runtime_id: String,
    runtime_generation: String,
    launch_generation: String,
}

/// Machine-local binding from an exact backend generation to the launch
/// contract st2 used to create it. A binding is trusted only while both IDs match.
pub(crate) struct LaunchBindingStore {
    root: PathBuf,
}

impl LaunchBindingStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path(&self, runtime_id: &str) -> PathBuf {
        let key = digest(
            "st2.launch-binding-key.v1",
            [("runtimeId", runtime_id.as_bytes().to_vec())],
        );
        self.root.join(format!("{}.json", &key["sha256:".len()..]))
    }

    pub fn read(
        &self,
        runtime_id: &str,
        runtime_generation: &str,
    ) -> anyhow::Result<Option<String>> {
        let path = self.path(runtime_id);
        let raw = match fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let binding: LaunchBinding = serde_json::from_slice(&raw)?;
        if binding.schema != LAUNCH_BINDING_SCHEMA || binding.runtime_id != runtime_id {
            anyhow::bail!("invalid launch binding {}", path.display());
        }
        Ok((binding.runtime_generation == runtime_generation).then_some(binding.launch_generation))
    }

    pub fn write(
        &self,
        runtime_id: &str,
        runtime_generation: &str,
        launch_generation: &str,
    ) -> anyhow::Result<()> {
        fs::create_dir_all(&self.root)?;
        let mut file = tempfile::NamedTempFile::new_in(&self.root)?;
        serde_json::to_writer(
            &mut file,
            &LaunchBinding {
                schema: LAUNCH_BINDING_SCHEMA.to_owned(),
                runtime_id: runtime_id.to_owned(),
                runtime_generation: runtime_generation.to_owned(),
                launch_generation: launch_generation.to_owned(),
            },
        )?;
        file.as_file_mut().sync_all()?;
        file.persist(self.path(runtime_id))
            .map_err(|error| error.error)?;
        Ok(())
    }

    pub fn remove(&self, runtime_id: &str) -> anyhow::Result<()> {
        match fs::remove_file(self.path(runtime_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
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
    /// Backend-issued or conservatively derived RFC3339 UTC creation time.
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
    /// Launch generation bound to this exact runtime generation when st2 created it.
    pub running_launch_generation: Option<String>,
}

/// One coherent backend observation attempt. Any backend uncertainty makes the
/// batch incomplete; callers must not infer absence from its missing rows.
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

/// Read-only backend boundary used by the CLI and deterministic tests.
pub trait RuntimeObserver {
    fn observe(&self, desired: &[DesiredRuntime]) -> ObservationBatch;
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementReceipt {
    schema: &'static str,
    task: String,
    pub previous_runtime_generation: String,
    pub running_runtime_generation: String,
    desired_launch_generation: String,
    running_launch_generation: String,
    pub launch_generation_state: &'static str,
}

impl ReplacementReceipt {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("replacement receipt contains serializable values")
    }
}

fn observe_running(
    observer: &dyn RuntimeObserver,
    desired: &DesiredRuntime,
) -> anyhow::Result<Option<(RuntimeGeneration, Option<String>)>> {
    let batch = observer.observe(std::slice::from_ref(desired));
    if !batch.complete || !batch.errors.is_empty() {
        anyhow::bail!(
            "runtime observation incomplete: {}",
            batch.errors.join("; ")
        );
    }
    if batch.observations.len() > 1 {
        anyhow::bail!("runtime observation returned duplicate task rows");
    }
    match batch.observations.into_iter().next() {
        None
        | Some(RuntimeObservation {
            state: ObservedState::Absent | ObservedState::Exited | ObservedState::Vanished,
            ..
        }) => Ok(None),
        Some(RuntimeObservation {
            state: ObservedState::Running(generation),
            running_launch_generation,
            ..
        }) => Ok(Some((generation, running_launch_generation))),
        Some(RuntimeObservation {
            state: ObservedState::Indeterminate(error),
            ..
        }) => anyhow::bail!("runtime observation indeterminate: {error}"),
    }
}

/// Replace one healthy task only when its exact current runtime generation
/// still matches the caller's observation. Ordinary publication never calls this path.
pub fn replace_task<R>(
    catalog: &Path,
    host: &str,
    selector: &str,
    expected_running_generation: &str,
    runner: &R,
) -> anyhow::Result<ReplacementReceipt>
where
    R: crate::run::Runner + RuntimeObserver,
{
    let found = crate::discover(catalog);
    if !found.errors.is_empty() || !found.warnings.is_empty() {
        anyhow::bail!("catalog is not strict-clean; refusing replacement");
    }
    let (spec, task, runtime_id) = crate::reconcile::resolve_task(&found.specs, selector, host)?;
    if spec.retired {
        anyhow::bail!("task {runtime_id:?} belongs to a retired declaration");
    }
    if task.lifecycle != TaskLifecycle::Service {
        anyhow::bail!("task {runtime_id:?} is adopt-only and cannot be replaced");
    }
    let target = crate::reconcile::task_target(spec, task, host)
        .ok_or_else(|| anyhow::anyhow!("task {runtime_id:?} has no launch contract"))?;
    let desired = DesiredRuntime {
        runtime_id: runtime_id.clone(),
        kind: task.kind,
    };
    let Some((observed, _)) = observe_running(runner, &desired)? else {
        anyhow::bail!("task {runtime_id:?} has no healthy running generation");
    };
    if observed.generation_id() != expected_running_generation {
        anyhow::bail!(
            "expected running generation {expected_running_generation:?}, observed {:?}",
            observed.generation_id()
        );
    }

    let after = crate::discover(catalog);
    if !same_discovery(&found, &after) {
        anyhow::bail!("catalog declarations changed during replacement admission");
    }
    let Some((rechecked, _)) = observe_running(runner, &desired)? else {
        anyhow::bail!("task {runtime_id:?} stopped before replacement");
    };
    if rechecked.generation_id() != expected_running_generation {
        anyhow::bail!(
            "expected running generation changed before replacement: expected {expected_running_generation:?}, observed {:?}",
            rechecked.generation_id()
        );
    }

    runner.kill(&runtime_id)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match observe_running(runner, &desired)? {
            None => break,
            Some((current, _)) if current.generation_id() != expected_running_generation => {
                anyhow::bail!(
                    "a different generation {:?} appeared while replacing {runtime_id:?}",
                    current.generation_id()
                );
            }
            Some(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Some(_) => anyhow::bail!("task {runtime_id:?} did not stop within 10 seconds"),
        }
    }

    runner.reap_for_restart(&runtime_id)?;
    let spec_dir = spec.path.parent().unwrap_or_else(|| Path::new("."));
    runner.spawn(&target, spec_dir)?;
    let Some((running, running_launch_generation)) = observe_running(runner, &desired)? else {
        anyhow::bail!("replacement for {runtime_id:?} did not produce a running generation");
    };
    if running.generation_id() == expected_running_generation {
        anyhow::bail!("replacement for {runtime_id:?} reused the prior runtime generation");
    }
    let desired_launch_generation = launch_generation_for_target(&target, spec_dir, catalog);
    let running_launch_generation = running_launch_generation.ok_or_else(|| {
        anyhow::anyhow!("replacement for {runtime_id:?} has no launch-generation binding")
    })?;
    let launch_generation_state = if desired_launch_generation == running_launch_generation {
        "converged"
    } else {
        "drifted"
    };
    Ok(ReplacementReceipt {
        schema: "st2.task-replacement.v1",
        task: runtime_id,
        previous_runtime_generation: expected_running_generation.to_owned(),
        running_runtime_generation: running.generation_id().to_owned(),
        desired_launch_generation,
        running_launch_generation,
        launch_generation_state,
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskInventory {
    schema: &'static str,
    catalog: PathBuf,
    host: String,
    complete: bool,
    errors: Vec<String>,
    tasks: Vec<TaskRow>,
}

impl TaskInventory {
    pub fn incomplete(catalog: PathBuf, host: String, error: String) -> Self {
        Self {
            schema: TASK_INVENTORY_SCHEMA,
            catalog,
            host,
            complete: false,
            errors: vec![error],
            tasks: Vec::new(),
        }
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn mark_incomplete(&mut self, error: impl Into<String>) {
        push_error(&mut self.errors, error.into());
        self.complete = false;
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
    declaration_revision: String,
    desired_launch_generation: Option<String>,
    running_launch_generation: Option<String>,
    launch_generation_state: &'static str,
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
    declaration_revision: String,
    desired_launch_generation: Option<String>,
}

/// Whether two deterministic discovery passes describe the same semantic catalog.
///
/// This detects observed declaration drift without claiming writer serialization.
/// A change that normalizes to the same declaration is intentionally equivalent.
pub fn same_discovery(left: &Discovered, right: &Discovered) -> bool {
    left.specs == right.specs && left.warnings == right.warnings && left.errors == right.errors
}

/// Join one discovered declaration observation to one runtime observation.
///
/// `catalog` must already be canonicalized. The caller is responsible for
/// checking that declaration semantics did not drift across runtime observation.
pub fn inventory(
    catalog: &Path,
    host: &str,
    found: &Discovered,
    observer: &dyn RuntimeObserver,
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
        let revision = declaration_revision(spec);
        for task in &spec.tasks {
            // Active declaration-only metadata has no desired runtime. Retired tasks remain in the
            // inventory even without launch material so stale generations stay visible.
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
                declaration_revision: revision.clone(),
                desired_launch_generation: desired_launch_generation(spec, task, host, catalog),
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
                running_launch_generation: None,
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
                        running_launch_generation: None,
                    }
                } else {
                    RuntimeObservation {
                        runtime_id: task.runtime_id.clone(),
                        state: ObservedState::Indeterminate(
                            "runtime observation incomplete".into(),
                        ),
                        running_launch_generation: None,
                    }
                }
            });
            let running_launch_generation = observation.running_launch_generation;
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
            let launch_generation_state = match (
                state,
                task.desired_launch_generation.as_deref(),
                running_launch_generation.as_deref(),
            ) {
                ("running", Some(desired), Some(running)) if desired == running => "converged",
                ("running", Some(_), Some(_)) => "drifted",
                ("running", _, _) => "unknown",
                ("absent", _, _) => "absent",
                _ => "not-running",
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
                declaration_revision: task.declaration_revision,
                desired_launch_generation: task.desired_launch_generation,
                running_launch_generation,
                launch_generation_state,
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
        complete: errors.is_empty() && observation_complete,
        errors,
        tasks,
    }
}

fn push_error(errors: &mut Vec<String>, error: String) {
    if !errors.contains(&error) {
        errors.push(error);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::fs;

    use serde_json::Value;

    use super::*;
    use crate::reconcile::{Session, TaskTarget};
    use crate::run::Runner;

    #[derive(Clone)]
    struct FixedObserver(ObservationBatch);

    impl RuntimeObserver for FixedObserver {
        fn observe(&self, _desired: &[DesiredRuntime]) -> ObservationBatch {
            self.0.clone()
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
        serde_json::from_str(&inventory(catalog, host, &found, &FixedObserver(observer)).to_json())
            .unwrap()
    }

    fn running(id: &str, pid: u32, running_launch_generation: Option<&str>) -> RuntimeObservation {
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
            running_launch_generation: running_launch_generation.map(str::to_owned),
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
                observations: vec![
                    running("h.worker", 11, None),
                    running("h.worker.ding", 12, None),
                ],
                errors: vec![],
            },
        );
        assert_eq!(value["schema"], TASK_INVENTORY_SCHEMA);
        assert_eq!(value["catalog"], tmp.path().to_str().unwrap());
        assert_eq!(value["host"], "h");
        assert_eq!(value["complete"], true);
        assert_eq!(value["errors"], Value::Array(vec![]));
        assert_eq!(value["tasks"].as_array().unwrap().len(), 2);
        let declaration_revision = value["tasks"][0]["declarationRevision"].clone();
        let desired_launch_generation = value["tasks"][0]["desiredLaunchGeneration"].clone();
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
                "declarationRevision": declaration_revision,
                "desiredLaunchGeneration": desired_launch_generation,
                "runningLaunchGeneration": null,
                "launchGenerationState": "unknown",
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
    fn complete_missing_runtime_is_absent_but_incomplete_is_indeterminate() {
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
                observations: vec![running("h.old", 42, None)],
                errors: vec![],
            },
        );
        assert_eq!(value["tasks"][0]["retired"], true);
        assert_eq!(value["tasks"][0]["desiredState"], "absent");
        assert_eq!(value["tasks"][0]["runtime"]["state"], "running");
        assert_eq!(value["tasks"][0]["runtime"]["pid"], 42);
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
                observations: vec![running("h.worker", 1, None), running("h.worker", 2, None)],
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
    fn invalid_running_evidence_and_timestamp_fail_closed() {
        assert!(!is_rfc3339_utc_millis("2026-02-31T10:00:00.000Z"));
        assert!(!is_rfc3339_utc_millis("2025-02-29T10:00:00.000Z"));
        assert!(is_rfc3339_utc_millis("2024-02-29T10:00:00.000Z"));
        assert!(
            RuntimeGeneration::new(0, "2026-07-31T10:00:00.000Z".into(), "sha256:g".into())
                .is_err()
        );

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
                    running_launch_generation: None,
                }],
                errors: vec![],
            },
        );
        assert_eq!(value["complete"], false);
        assert_eq!(value["tasks"][0]["runtime"]["state"], "indeterminate");
    }

    #[test]
    fn declaration_and_launch_generations_separate_resource_edits_from_launch_drift() {
        let tmp = tempfile::tempdir().unwrap();
        let declaration = |resource: &str, executable: &str| {
            format!(
                r#"
                  resource "notes" _tag="file" uri="{resource}"
                  restart {{ attempts 3; interval "60s"; delay "0s"; mode "delay" }}
                  pty "agent" {{ id "h.worker"; argv "{executable}" }}
                "#
            )
        };
        write_agent(
            tmp.path(),
            "h",
            "worker",
            &declaration("file:///notes/one", "agent-v1"),
        );
        let first = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                ..ObservationBatch::default()
            },
        );
        let first_revision = first["tasks"][0]["declarationRevision"]
            .as_str()
            .unwrap()
            .to_owned();
        let first_launch = first["tasks"][0]["desiredLaunchGeneration"]
            .as_str()
            .unwrap()
            .to_owned();

        write_agent(
            tmp.path(),
            "h",
            "worker",
            &declaration("file:///notes/two", "agent-v1"),
        );
        let resource_edit = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                ..ObservationBatch::default()
            },
        );
        assert_ne!(
            resource_edit["tasks"][0]["declarationRevision"],
            first_revision
        );
        assert_eq!(
            resource_edit["tasks"][0]["desiredLaunchGeneration"],
            first_launch
        );

        write_agent(
            tmp.path(),
            "h",
            "worker",
            &declaration("file:///notes/two", "agent-v2"),
        );
        let launch_edit = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                ..ObservationBatch::default()
            },
        );
        assert_ne!(
            launch_edit["tasks"][0]["desiredLaunchGeneration"],
            first_launch
        );

        write_agent(
            tmp.path(),
            "h",
            "worker",
            &declaration("file:///notes/one", "agent-v1"),
        );
        let converged = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![running("h.worker", 11, Some(&first_launch))],
                errors: vec![],
            },
        );
        assert_eq!(converged["tasks"][0]["launchGenerationState"], "converged");
        assert_eq!(
            converged["tasks"][0]["runningLaunchGeneration"],
            first_launch
        );

        let drifted = json(
            tmp.path(),
            "h",
            ObservationBatch {
                complete: true,
                observations: vec![running("h.worker", 11, Some("sha256:older-launch"))],
                errors: vec![],
            },
        );
        assert_eq!(drifted["tasks"][0]["launchGenerationState"], "drifted");
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
                        running_launch_generation: None,
                    },
                    RuntimeObservation {
                        runtime_id: "h.b".into(),
                        state: ObservedState::Vanished,
                        running_launch_generation: None,
                    },
                    RuntimeObservation {
                        runtime_id: "h.c".into(),
                        state: ObservedState::Indeterminate("unreadable".into()),
                        running_launch_generation: None,
                    },
                ],
                errors: vec!["one runtime unreadable".into()],
            },
        );
        assert_eq!(value["tasks"][0]["runtime"]["state"], "exited");
        assert_eq!(value["tasks"][1]["runtime"]["state"], "vanished");
        assert_eq!(value["tasks"][2]["runtime"]["state"], "indeterminate");
    }

    #[test]
    fn semantic_discovery_drift_is_detected_but_comment_only_edits_are_equivalent() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(tmp.path(), "h", "worker", r#"pty "agent" { argv "first" }"#);
        let first = crate::discover(tmp.path());
        let path = tmp.path().join("agents/h/worker/agent.kdl");
        let original = fs::read_to_string(&path).unwrap();
        fs::write(&path, format!("// comment\n{original}")).unwrap();
        let comment_only = crate::discover(tmp.path());
        assert!(same_discovery(&first, &comment_only));
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { argv "second" }"#,
        );
        let changed = crate::discover(tmp.path());
        assert!(!same_discovery(&first, &changed));
    }

    struct ReplacingRunner {
        catalog: PathBuf,
        generation: RefCell<Option<(RuntimeGeneration, Option<String>)>>,
        killed: Cell<usize>,
        spawned: Cell<usize>,
    }

    impl RuntimeObserver for ReplacingRunner {
        fn observe(&self, desired: &[DesiredRuntime]) -> ObservationBatch {
            let observations = self
                .generation
                .borrow()
                .clone()
                .into_iter()
                .map(
                    |(generation, running_launch_generation)| RuntimeObservation {
                        runtime_id: desired[0].runtime_id.clone(),
                        state: ObservedState::Running(generation),
                        running_launch_generation,
                    },
                )
                .collect();
            ObservationBatch {
                complete: true,
                observations,
                errors: vec![],
            }
        }
    }

    impl Runner for ReplacingRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
            Ok(vec![])
        }

        fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
            self.spawned.set(self.spawned.get() + 1);
            let launch_generation = launch_generation_for_target(target, spec_dir, &self.catalog);
            self.generation.replace(Some((
                RuntimeGeneration::new(12, "2026-07-31T10:01:00.000Z".into(), "sha256:g-12".into())
                    .unwrap(),
                Some(launch_generation),
            )));
            Ok(())
        }

        fn kill(&self, _runtime_id: &str) -> anyhow::Result<()> {
            self.killed.set(self.killed.get() + 1);
            self.generation.replace(None);
            Ok(())
        }

        fn reap_for_restart(&self, _runtime_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove(&self, _runtime_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn explicit_replacement_is_fenced_by_the_expected_running_generation() {
        let tmp = tempfile::tempdir().unwrap();
        write_agent(
            tmp.path(),
            "h",
            "worker",
            r#"pty "agent" { id "h.worker"; argv "agent-v2" }"#,
        );
        let runner = ReplacingRunner {
            catalog: tmp.path().to_path_buf(),
            generation: RefCell::new(Some((
                RuntimeGeneration::new(11, "2026-07-31T10:00:00.000Z".into(), "sha256:g-11".into())
                    .unwrap(),
                Some("sha256:old-launch".into()),
            ))),
            killed: Cell::new(0),
            spawned: Cell::new(0),
        };

        let stale =
            replace_task(tmp.path(), "h", "h.worker", "sha256:not-current", &runner).unwrap_err();
        assert!(stale.to_string().contains("expected running generation"));
        assert_eq!(runner.killed.get(), 0);
        assert_eq!(runner.spawned.get(), 0);

        let receipt = replace_task(tmp.path(), "h", "h.worker", "sha256:g-11", &runner).unwrap();
        assert_eq!(receipt.previous_runtime_generation, "sha256:g-11");
        assert_eq!(receipt.running_runtime_generation, "sha256:g-12");
        assert_eq!(receipt.launch_generation_state, "converged");
        assert_eq!(runner.killed.get(), 1);
        assert_eq!(runner.spawned.get(), 1);
    }
}
