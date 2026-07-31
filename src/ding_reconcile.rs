//! Exact, forward-only adoption of the successor Ding exec set during cutover.
//!
//! The capability in this module can observe and spawn only Ding execs. It intentionally cannot
//! kill, reap, remove, garbage-collect, or address provider PTYs.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::cutover_admission::{
    AdmissionError, AdmissionResult, CanonicalCatalog, GateId, SuccessorDingObservation,
};
use crate::exec_backend::{
    ExecBackend, ExecCutoverBinding, ExecGeneration, ExecGenerationObservation,
};
use crate::reconcile::{TaskLaunch, TaskTarget};
use crate::spec::{TaskKind, TaskLifecycle};

pub const DING_RECONCILE_RECEIPT_SCHEMA: &str = "st2.cutover-ding-reconcile-receipt.v1";
const DING_JOURNAL_SCHEMA: &str = "st2.cutover-ding-journal.v1";
const MAX_DINGS: usize = 256;
const MAX_ARGV: usize = 32;
const MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DingReconcileAction {
    pub generation_id: String,
    pub desired: Vec<DingDesiredExec>,
    pub desired_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DingDesiredExec {
    pub runtime_id: String,
    pub canonical_argv: Vec<String>,
    pub canonical_cwd: PathBuf,
    pub canonical_env: BTreeMap<String, String>,
    pub launch_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DingReconcileReceipt {
    pub schema: String,
    pub gate_id: String,
    pub action_index: usize,
    pub generation_id: String,
    pub desired_sha256: String,
    pub runtime_ids: Vec<String>,
    pub exec_generation_ids: Vec<String>,
    pub observed_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DingJournal {
    schema: String,
    gate_id: String,
    action_index: usize,
    generation_id: String,
    desired_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DingJournalEntry {
    runtime_id: String,
    launch_sha256: String,
    exec_generation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DingObservation {
    pub generation: ExecGeneration,
    pub alive: bool,
}

/// Deliberately capability-poor backend used only by cutover Ding reconciliation.
pub(crate) trait DingExecBackend {
    fn observe_ding(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>>;
    fn spawn_bound_ding(
        &self,
        desired: &DingDesiredExec,
        binding: ExecCutoverBinding,
    ) -> AdmissionResult<DingObservation>;
}

pub(crate) struct SystemDingExecBackend {
    inner: ExecBackend,
}

/// Capability-poor, read-only System exec observer used by provider-fleet proof.
///
/// Unlike [`SystemDingExecBackend`], this type has no spawn/kill/reap/remove surface.
pub(crate) struct SystemDingPartitionObserver {
    inner: ExecBackend,
    catalog: PathBuf,
}

impl SystemDingPartitionObserver {
    pub(crate) fn new(state_dir: PathBuf, catalog: PathBuf) -> Self {
        Self {
            inner: ExecBackend::new(state_dir, catalog.clone()),
            catalog,
        }
    }

    fn validate_catalog(&self, catalog: &CanonicalCatalog) -> AdmissionResult<()> {
        let observer_catalog = self.catalog.canonicalize().map_err(|error| {
            AdmissionError::io(
                format!(
                    "canonicalize Ding partition observer catalog {}",
                    self.catalog.display()
                ),
                error,
            )
        })?;
        if observer_catalog != catalog.as_path() {
            return Err(AdmissionError::Conflict(
                "Ding partition observer is bound to a different canonical catalog".to_owned(),
            ));
        }
        Ok(())
    }
}

pub(crate) trait DingGenerationReader {
    fn validate_catalog(&self, _catalog: &CanonicalCatalog) -> AdmissionResult<()> {
        Ok(())
    }

    fn observe_generation(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>>;
}

impl DingGenerationReader for SystemDingPartitionObserver {
    fn validate_catalog(&self, catalog: &CanonicalCatalog) -> AdmissionResult<()> {
        SystemDingPartitionObserver::validate_catalog(self, catalog)
    }

    fn observe_generation(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>> {
        match self
            .inner
            .observe_generation_optional(runtime_id)
            .map_err(|error| {
                AdmissionError::Invalid(format!(
                    "observe successor Ding generation {runtime_id:?}: {error:#}"
                ))
            })? {
            None => Ok(None),
            Some(ExecGenerationObservation::Known { generation, alive }) => {
                Ok(Some(DingObservation { generation, alive }))
            }
            Some(ExecGenerationObservation::Indeterminate { reason, .. }) => {
                Err(AdmissionError::Conflict(format!(
                    "successor Ding generation {runtime_id:?} is indeterminate: {reason}"
                )))
            }
        }
    }
}

impl SystemDingExecBackend {
    pub(crate) fn new(state_dir: PathBuf, catalog: PathBuf) -> Self {
        Self {
            inner: ExecBackend::new(state_dir, catalog),
        }
    }
}

impl DingExecBackend for SystemDingExecBackend {
    fn observe_ding(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>> {
        match self
            .inner
            .observe_generation_optional(runtime_id)
            .map_err(|error| {
                AdmissionError::Invalid(format!("observe Ding {runtime_id}: {error:#}"))
            })? {
            None => Ok(None),
            Some(ExecGenerationObservation::Known { generation, alive }) => {
                Ok(Some(DingObservation { generation, alive }))
            }
            Some(ExecGenerationObservation::Indeterminate { reason, .. }) => {
                Err(AdmissionError::Conflict(format!(
                    "Ding generation {runtime_id:?} is indeterminate: {reason}"
                )))
            }
        }
    }

    fn spawn_bound_ding(
        &self,
        desired: &DingDesiredExec,
        binding: ExecCutoverBinding,
    ) -> AdmissionResult<DingObservation> {
        let target = TaskTarget {
            kind: TaskKind::Exec,
            pty_id: desired.runtime_id.clone(),
            bus_id: desired.runtime_id.trim_end_matches(".ding").to_owned(),
            name: "ding".to_owned(),
            launch: TaskLaunch::Argv(desired.canonical_argv.clone()),
            cwd: Some(desired.canonical_cwd.display().to_string()),
            workspace: None,
            tags: BTreeMap::new(),
            env: desired.canonical_env.clone(),
            keep: false,
            presentation: None,
        };
        let generation = self
            .inner
            .spawn_cutover_ding(&target, Path::new("/"), binding)
            .map_err(|error| {
                AdmissionError::Invalid(format!(
                    "spawn exact cutover Ding {:?}: {error:#}",
                    desired.runtime_id
                ))
            })?;
        Ok(DingObservation {
            generation,
            alive: true,
        })
    }
}

pub(crate) fn validate_action(action: &DingReconcileAction) -> AdmissionResult<()> {
    safe_component("Ding generation id", &action.generation_id)?;
    if action.desired.is_empty() || action.desired.len() > MAX_DINGS {
        return Err(AdmissionError::Invalid(format!(
            "Ding desired set must contain 1..={MAX_DINGS} entries"
        )));
    }
    let mut prior = None;
    for desired in &action.desired {
        validate_desired(desired)?;
        if prior
            .as_deref()
            .is_some_and(|value| value >= desired.runtime_id.as_str())
        {
            return Err(AdmissionError::Invalid(
                "Ding desired set must be strictly ordered by unique runtime id".to_owned(),
            ));
        }
        prior = Some(desired.runtime_id.clone());
    }
    let expected = desired_set_sha256(&action.desired)?;
    if action.desired_sha256 != expected {
        return Err(AdmissionError::Invalid(format!(
            "Ding desired sha256 differs from its canonical set: expected {expected}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_receipt(
    action: &DingReconcileAction,
    gate_id: &str,
    action_index: usize,
    receipt: &DingReconcileReceipt,
) -> AdmissionResult<()> {
    if receipt.schema != DING_RECONCILE_RECEIPT_SCHEMA
        || receipt.gate_id != gate_id
        || receipt.action_index != action_index
        || receipt.generation_id != action.generation_id
        || receipt.desired_sha256 != action.desired_sha256
        || receipt.runtime_ids
            != action
                .desired
                .iter()
                .map(|desired| desired.runtime_id.clone())
                .collect::<Vec<_>>()
        || receipt.exec_generation_ids.len() != action.desired.len()
        || receipt
            .exec_generation_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != receipt.exec_generation_ids.len()
        || receipt.observed_sha256
            != observation_sha256(&receipt.runtime_ids, &receipt.exec_generation_ids)
    {
        return Err(AdmissionError::Invalid(
            "Ding reconciliation receipt does not prove the exact desired generation set"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn derive_and_validate_catalog(
    catalog: &Path,
    host: &str,
    action: &DingReconcileAction,
) -> AdmissionResult<()> {
    validate_ding_action_preflight(catalog, catalog, host, action)
}

pub(crate) fn validate_ding_action_preflight(
    declaration_root: &Path,
    logical_catalog: &Path,
    host: &str,
    action: &DingReconcileAction,
) -> AdmissionResult<()> {
    let discovered = crate::discover(declaration_root);
    if !discovered.errors.is_empty() || !discovered.warnings.is_empty() {
        return Err(AdmissionError::Conflict(
            "Ding reconciliation requires warning-free catalog discovery".to_owned(),
        ));
    }
    let mut derived = Vec::new();
    for spec in &discovered.specs {
        if spec.resolved_host(host) != host || spec.retired {
            continue;
        }
        let bus_id = spec.bus_id(host);
        for task in &spec.tasks {
            if task.kind != TaskKind::Exec || task.name != "ding" {
                continue;
            }
            if task.lifecycle == TaskLifecycle::AdoptOnly {
                return Err(AdmissionError::Invalid(format!(
                    "successor Ding {} cannot be adopt-only",
                    task.id.clone().unwrap_or_else(|| format!("{bus_id}.ding"))
                )));
            }
            let runtime_id = task.id.clone().unwrap_or_else(|| format!("{bus_id}.ding"));
            let canonical_argv = canonical_ding_argv(task, &bus_id, logical_catalog, &runtime_id)?;
            let relative_spec = spec.path.strip_prefix(declaration_root).map_err(|_| {
                AdmissionError::Invalid("Ding declaration escaped its root".to_owned())
            })?;
            let spec_dir = logical_catalog
                .join(relative_spec)
                .parent()
                .unwrap_or(logical_catalog)
                .to_path_buf();
            let cwd = match task.cwd.as_deref().or(spec.workspace.as_deref()) {
                Some(value) => spec_dir.join(crate::expand::expand_catalog(value, logical_catalog)),
                None => spec_dir,
            }
            .canonicalize()
            .map_err(|error| {
                AdmissionError::io(format!("canonicalize Ding cwd for {runtime_id}"), error)
            })?;
            let mut env = task
                .env
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        crate::expand::expand_catalog(value, logical_catalog),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if let Some(supervisor) = &spec.supervisor {
                env.insert("ST_SUPERVISOR".to_owned(), supervisor.clone());
            } else {
                env.remove("ST_SUPERVISOR");
            }
            let mut desired = DingDesiredExec {
                runtime_id,
                canonical_argv,
                canonical_cwd: cwd,
                canonical_env: env,
                launch_sha256: String::new(),
            };
            desired.launch_sha256 = launch_sha256(&desired)?;
            derived.push(desired);
        }
    }
    derived.sort_by(|left, right| left.runtime_id.cmp(&right.runtime_id));
    if derived != action.desired {
        return Err(AdmissionError::Conflict(
            "precommitted Ding desired set differs from exact current catalog declarations"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn observe_successor_partition(
    catalog: &CanonicalCatalog,
    cutover_dir: &File,
    host: &str,
    gate_id: &GateId,
    action_index: usize,
    action: &DingReconcileAction,
    reader: &dyn DingGenerationReader,
) -> AdmissionResult<Vec<SuccessorDingObservation>> {
    validate_action(action)?;
    derive_and_validate_catalog(catalog.as_path(), host, action)?;
    reader.validate_catalog(catalog)?;

    let observed = action
        .desired
        .iter()
        .map(|desired| {
            reader
                .observe_generation(&desired.runtime_id)
                .map(|observation| (desired, observation))
        })
        .collect::<AdmissionResult<Vec<_>>>()?;

    let journal_dir =
        open_existing_journal_dir(cutover_dir, gate_id.as_str(), &action.generation_id)?;
    if journal_dir.is_none() {
        if observed.iter().any(|(_, generation)| generation.is_some()) {
            return Err(AdmissionError::Conflict(
                "successor Ding generation exists without its retained cutover journal".to_owned(),
            ));
        }
        return Ok(action
            .desired
            .iter()
            .map(|desired| SuccessorDingObservation::Absent {
                runtime_id: desired.runtime_id.clone(),
            })
            .collect());
    }
    let journal_dir = journal_dir.expect("checked");
    let journal_file = open_regular_at(&journal_dir, "journal.json")?;
    let journal: DingJournal = read_canonical(&journal_file)?;
    let expected_header = DingJournal {
        schema: DING_JOURNAL_SCHEMA.to_owned(),
        gate_id: gate_id.as_str().to_owned(),
        action_index,
        generation_id: action.generation_id.clone(),
        desired_sha256: action.desired_sha256.clone(),
    };
    if journal != expected_header {
        return Err(AdmissionError::Conflict(
            "successor Ding journal header is foreign to the exact transaction action".to_owned(),
        ));
    }
    validate_exact_journal_names(&journal_dir, action)?;
    let header_bytes = canonical_bytes(&journal)?;
    let mut partition = Vec::with_capacity(action.desired.len());
    let mut live_generations = BTreeSet::new();
    for (desired, generation) in observed {
        let entry_name = format!("{}.json", desired.runtime_id);
        let entry = match open_regular_at(&journal_dir, &entry_name) {
            Ok(file) => Some(read_canonical::<DingJournalEntry>(&file)?),
            Err(AdmissionError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        if let Some(entry) = &entry
            && (entry.runtime_id != desired.runtime_id
                || entry.launch_sha256 != desired.launch_sha256)
        {
            return Err(AdmissionError::Conflict(format!(
                "successor Ding journal entry {:?} is foreign",
                desired.runtime_id
            )));
        }
        match generation {
            None => {
                if entry
                    .as_ref()
                    .is_some_and(|entry| entry.exec_generation_id.is_some())
                {
                    return Err(AdmissionError::Conflict(format!(
                        "successor Ding {:?} has a completed journal entry but no generation record",
                        desired.runtime_id
                    )));
                }
                partition.push(SuccessorDingObservation::Absent {
                    runtime_id: desired.runtime_id.clone(),
                });
            }
            Some(observation) => {
                let binding = ExecCutoverBinding {
                    gate_id: gate_id.as_str().to_owned(),
                    action_index,
                    ding_generation_id: action.generation_id.clone(),
                    launch_sha256: desired.launch_sha256.clone(),
                };
                verify_observation(desired, &binding, &observation)?;
                let entry = entry.ok_or_else(|| {
                    AdmissionError::Conflict(format!(
                        "live successor Ding {:?} has no retained journal entry",
                        desired.runtime_id
                    ))
                })?;
                if entry.exec_generation_id.as_deref()
                    != Some(observation.generation.generation_id.as_str())
                    || !live_generations.insert(observation.generation.generation_id.clone())
                {
                    return Err(AdmissionError::Conflict(format!(
                        "successor Ding {:?} generation is missing, duplicated, or differs from its journal",
                        desired.runtime_id
                    )));
                }
                let entry_bytes = canonical_bytes(&entry)?;
                partition.push(SuccessorDingObservation::JournalBound {
                    runtime_id: desired.runtime_id.clone(),
                    gate_id: gate_id.clone(),
                    action_index,
                    ding_generation_id: action.generation_id.clone(),
                    launch_sha256: desired.launch_sha256.clone(),
                    journal_sha256: journal_binding_sha256(&header_bytes, &entry_bytes),
                });
            }
        }
    }
    Ok(partition)
}

fn open_existing_journal_dir(
    cutover_dir: &File,
    gate_id: &str,
    generation_id: &str,
) -> AdmissionResult<Option<File>> {
    let Some(ding) = open_optional_directory_at(cutover_dir, "ding")? else {
        return Ok(None);
    };
    validate_optional_single_directory_name(&ding, gate_id, "Ding gate")?;
    let Some(gate) = open_optional_directory_at(&ding, gate_id)? else {
        return Ok(None);
    };
    validate_optional_single_directory_name(&gate, generation_id, "Ding generation")?;
    open_optional_directory_at(&gate, generation_id)
}

fn open_optional_directory_at(parent: &File, name: &str) -> AdmissionResult<Option<File>> {
    match open_directory_at(parent, name) {
        Ok(directory) => Ok(Some(directory)),
        Err(AdmissionError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn validate_optional_single_directory_name(
    directory: &File,
    expected: &str,
    label: &str,
) -> AdmissionResult<()> {
    for name in directory_entry_names(directory)? {
        if name != expected {
            return Err(AdmissionError::Conflict(format!(
                "unexpected {label} journal namespace {name:?}"
            )));
        }
    }
    Ok(())
}

fn open_directory_at(parent: &File, name: &str) -> AdmissionResult<File> {
    safe_component("Ding journal directory", name)?;
    let name = c_name(name)?;
    // SAFETY: parent is a retained directory descriptor and name is NUL terminated.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    if fd < 0 {
        return Err(AdmissionError::io(
            "open retained Ding journal directory",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: openat returned one newly owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn validate_exact_journal_names(
    journal_dir: &File,
    action: &DingReconcileAction,
) -> AdmissionResult<()> {
    let mut allowed = action
        .desired
        .iter()
        .map(|desired| format!("{}.json", desired.runtime_id))
        .collect::<BTreeSet<_>>();
    allowed.insert("journal.json".to_owned());
    for name in directory_entry_names(journal_dir)? {
        if !allowed.contains(&name) {
            return Err(AdmissionError::Conflict(format!(
                "unexpected successor Ding journal entry {name:?}"
            )));
        }
    }
    Ok(())
}

fn validate_mutating_journal_namespace(
    cutover_dir: &File,
    gate_id: &str,
    action: &DingReconcileAction,
) -> AdmissionResult<()> {
    let Some(ding) = open_optional_directory_at(cutover_dir, "ding")? else {
        return Ok(());
    };
    validate_optional_single_directory_name(&ding, gate_id, "Ding gate")?;
    let Some(gate) = open_optional_directory_at(&ding, gate_id)? else {
        return Ok(());
    };
    validate_optional_single_directory_name(&gate, &action.generation_id, "Ding generation")?;
    let Some(journal) = open_optional_directory_at(&gate, &action.generation_id)? else {
        return Ok(());
    };
    validate_exact_journal_names(&journal, action)
}

fn directory_entry_names(directory: &File) -> AdmissionResult<Vec<String>> {
    // SAFETY: dup returns an independently owned descriptor on success.
    let duplicate = unsafe { libc::dup(directory.as_raw_fd()) };
    if duplicate < 0 {
        return Err(AdmissionError::io(
            "duplicate retained Ding journal directory",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: fdopendir takes ownership of the duplicated descriptor.
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        // SAFETY: fdopendir failed and did not take ownership.
        unsafe { libc::close(duplicate) };
        return Err(AdmissionError::io(
            "open retained Ding journal directory stream",
            std::io::Error::last_os_error(),
        ));
    }
    let mut names = Vec::new();
    loop {
        // SAFETY: stream is valid until closed below.
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            break;
        }
        // SAFETY: d_name is NUL terminated for the lifetime of this dirent.
        let raw = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        let name = raw.to_str().map_err(|_| {
            AdmissionError::Invalid("Ding journal filename is not UTF-8".to_owned())
        })?;
        if name != "." && name != ".." {
            names.push(name.to_owned());
        }
    }
    // SAFETY: stream was created by fdopendir and is closed exactly once.
    if unsafe { libc::closedir(stream) } != 0 {
        return Err(AdmissionError::io(
            "close retained Ding journal directory stream",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(names)
}

fn journal_binding_sha256(header: &[u8], entry: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.cutover-ding-journal-binding.v1\0");
    for bytes in [header, entry] {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    format!("{:x}", hash.finalize())
}

pub(crate) fn reconcile(
    catalog: &Path,
    cutover_dir: &File,
    host: &str,
    gate_id: &str,
    action_index: usize,
    action: &DingReconcileAction,
    backend: &dyn DingExecBackend,
) -> AdmissionResult<DingReconcileReceipt> {
    validate_action(action)?;
    derive_and_validate_catalog(catalog, host, action)?;
    validate_mutating_journal_namespace(cutover_dir, gate_id, action)?;
    let ding_dir = open_or_create_dir_at(cutover_dir, "ding")?;
    let gate_dir = open_or_create_dir_at(&ding_dir, gate_id)?;
    let journal_dir = open_or_create_dir_at(&gate_dir, &action.generation_id)?;
    let journal = DingJournal {
        schema: DING_JOURNAL_SCHEMA.to_owned(),
        gate_id: gate_id.to_owned(),
        action_index,
        generation_id: action.generation_id.clone(),
        desired_sha256: action.desired_sha256.clone(),
    };
    create_or_verify(&journal_dir, "journal.json", &journal)?;

    let mut exec_generation_ids = Vec::with_capacity(action.desired.len());
    for desired in &action.desired {
        let entry_name = format!("{}.json", desired.runtime_id);
        let mut entry = DingJournalEntry {
            runtime_id: desired.runtime_id.clone(),
            launch_sha256: desired.launch_sha256.clone(),
            exec_generation_id: None,
        };
        entry = match open_regular_at(&journal_dir, &entry_name) {
            Ok(file) => read_canonical(&file)?,
            Err(AdmissionError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                create_or_verify(&journal_dir, &entry_name, &entry)?;
                entry
            }
            Err(error) => return Err(error),
        };
        if entry.runtime_id != desired.runtime_id || entry.launch_sha256 != desired.launch_sha256 {
            return Err(AdmissionError::Conflict(format!(
                "Ding journal entry {:?} changed",
                desired.runtime_id
            )));
        }
        let binding = ExecCutoverBinding {
            gate_id: gate_id.to_owned(),
            action_index,
            ding_generation_id: action.generation_id.clone(),
            launch_sha256: desired.launch_sha256.clone(),
        };
        let observed = match backend.observe_ding(&desired.runtime_id)? {
            Some(observed) => observed,
            None if entry.exec_generation_id.is_none() => {
                backend.spawn_bound_ding(desired, binding.clone())?
            }
            None => {
                return Err(AdmissionError::Conflict(format!(
                    "completed Ding journal entry {:?} has no generation",
                    desired.runtime_id
                )));
            }
        };
        verify_observation(desired, &binding, &observed)?;
        if let Some(recorded) = &entry.exec_generation_id {
            if recorded != &observed.generation.generation_id {
                return Err(AdmissionError::Conflict(format!(
                    "Ding generation {:?} changed after journal completion",
                    desired.runtime_id
                )));
            }
        } else {
            entry.exec_generation_id = Some(observed.generation.generation_id.clone());
            atomic_replace(&journal_dir, &entry_name, &entry)?;
        }
        exec_generation_ids.push(observed.generation.generation_id);
    }

    // Final read-back is the proof: one strictly ordered desired set, one exact bound live
    // generation per entry, and no duplicate generation ids.
    let mut unique = BTreeSet::new();
    for (desired, expected_generation) in action.desired.iter().zip(&exec_generation_ids) {
        let observed = backend.observe_ding(&desired.runtime_id)?.ok_or_else(|| {
            AdmissionError::Conflict(format!(
                "Ding {:?} vanished before final receipt",
                desired.runtime_id
            ))
        })?;
        let binding = ExecCutoverBinding {
            gate_id: gate_id.to_owned(),
            action_index,
            ding_generation_id: action.generation_id.clone(),
            launch_sha256: desired.launch_sha256.clone(),
        };
        verify_observation(desired, &binding, &observed)?;
        if &observed.generation.generation_id != expected_generation
            || !unique.insert(expected_generation.clone())
        {
            return Err(AdmissionError::Conflict(
                "Ding final read-back found a changed or duplicate generation".to_owned(),
            ));
        }
    }
    let runtime_ids = action
        .desired
        .iter()
        .map(|desired| desired.runtime_id.clone())
        .collect::<Vec<_>>();
    let observed_sha256 = observation_sha256(&runtime_ids, &exec_generation_ids);
    Ok(DingReconcileReceipt {
        schema: DING_RECONCILE_RECEIPT_SCHEMA.to_owned(),
        gate_id: gate_id.to_owned(),
        action_index,
        generation_id: action.generation_id.clone(),
        desired_sha256: action.desired_sha256.clone(),
        runtime_ids,
        exec_generation_ids,
        observed_sha256,
    })
}

fn verify_observation(
    desired: &DingDesiredExec,
    binding: &ExecCutoverBinding,
    observed: &DingObservation,
) -> AdmissionResult<()> {
    if !observed.alive || observed.generation.cutover.as_ref() != Some(binding) {
        return Err(AdmissionError::Conflict(format!(
            "existing Ding {:?} is not one live exact cutover-bound generation",
            desired.runtime_id
        )));
    }
    Ok(())
}

fn canonical_ding_argv(
    task: &crate::spec::Task,
    bus_id: &str,
    catalog: &Path,
    runtime_id: &str,
) -> AdmissionResult<Vec<String>> {
    let expected = vec![
        "st2".to_owned(),
        "ding".to_owned(),
        "--identity".to_owned(),
        bus_id.to_owned(),
        "--root".to_owned(),
        "$ST_ROOT".to_owned(),
    ];
    match (&task.command, &task.argv) {
        (Some(command), None)
            if command == &format!("st2 ding --identity {bus_id} --root $ST_ROOT") => {}
        (None, Some(argv)) if argv == &expected => {}
        _ => {
            return Err(AdmissionError::Invalid(format!(
                "Ding {runtime_id:?} is not the closed canonical launch"
            )));
        }
    }
    Ok(expected
        .into_iter()
        .map(|argument| {
            if argument == "$ST_ROOT" {
                catalog.display().to_string()
            } else {
                crate::expand::expand_catalog(&argument, catalog)
            }
        })
        .collect())
}

fn validate_desired(desired: &DingDesiredExec) -> AdmissionResult<()> {
    safe_component("Ding runtime id", &desired.runtime_id)?;
    if desired.canonical_argv.is_empty() || desired.canonical_argv.len() > MAX_ARGV {
        return Err(AdmissionError::Invalid(
            "Ding canonical argv is empty or too large".to_owned(),
        ));
    }
    if desired
        .canonical_argv
        .iter()
        .any(|value| value.len() > MAX_BYTES || value.as_bytes().contains(&0))
        || desired.canonical_env.len() > 64
        || desired
            .canonical_env
            .iter()
            .any(|(key, value)| key.len() > 128 || value.len() > MAX_BYTES)
        || !desired.canonical_cwd.is_absolute()
    {
        return Err(AdmissionError::Invalid(
            "Ding launch exceeds canonical argv/cwd/env bounds".to_owned(),
        ));
    }
    let expected = launch_sha256(desired)?;
    if desired.launch_sha256 != expected {
        return Err(AdmissionError::Invalid(format!(
            "Ding launch sha256 differs from canonical launch: expected {expected}"
        )));
    }
    Ok(())
}

fn safe_component(label: &str, value: &str) -> AdmissionResult<()> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} is not one bounded safe component"
        )));
    }
    Ok(())
}

pub fn launch_sha256(desired: &DingDesiredExec) -> AdmissionResult<String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Launch<'a> {
        runtime_id: &'a str,
        canonical_argv: &'a [String],
        canonical_cwd: &'a Path,
        canonical_env: &'a BTreeMap<String, String>,
    }
    hash_json(
        b"st2.cutover-ding-launch.v1\0",
        &Launch {
            runtime_id: &desired.runtime_id,
            canonical_argv: &desired.canonical_argv,
            canonical_cwd: &desired.canonical_cwd,
            canonical_env: &desired.canonical_env,
        },
    )
}

pub fn desired_set_sha256(desired: &[DingDesiredExec]) -> AdmissionResult<String> {
    hash_json(b"st2.cutover-ding-desired.v1\0", &desired)
}

pub(crate) fn observation_sha256(runtime_ids: &[String], generation_ids: &[String]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.cutover-ding-observation.v1\0");
    for values in [runtime_ids, generation_ids] {
        hash.update((values.len() as u64).to_be_bytes());
        for value in values {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
    }
    format!("{:x}", hash.finalize())
}

fn hash_json<T: Serialize + ?Sized>(domain: &[u8], value: &T) -> AdmissionResult<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| AdmissionError::Invalid(format!("serialize Ding evidence: {error}")))?;
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn canonical_bytes<T: Serialize>(value: &T) -> AdmissionResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| AdmissionError::Invalid(format!("serialize Ding journal: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn create_or_verify<T>(directory: &File, name: &str, expected: &T) -> AdmissionResult<()>
where
    T: Serialize + for<'de> Deserialize<'de> + PartialEq,
{
    match open_regular_at(directory, name) {
        Ok(file) => {
            if read_canonical::<T>(&file)? != *expected {
                return Err(AdmissionError::Conflict(format!(
                    "Ding journal entry differs from immutable intent: {name}"
                )));
            }
            Ok(())
        }
        Err(AdmissionError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            let bytes = canonical_bytes(expected)?;
            let mut file = create_regular_at(directory, name)?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| AdmissionError::io(format!("persist {name}"), error))?;
            directory
                .sync_all()
                .map_err(|error| AdmissionError::io("sync Ding journal directory", error))
        }
        Err(error) => Err(error),
    }
}

fn read_canonical<T: for<'de> Deserialize<'de> + Serialize>(file: &File) -> AdmissionResult<T> {
    let mut file = file
        .try_clone()
        .map_err(|error| AdmissionError::io("clone Ding journal entry", error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| AdmissionError::io("seek Ding journal entry", error))?;
    let mut bytes = Vec::new();
    file.take(64 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| AdmissionError::io("read Ding journal entry", error))?;
    if bytes.len() > 64 * 1024 {
        return Err(AdmissionError::Invalid(
            "Ding journal entry exceeds size bound".to_owned(),
        ));
    }
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| AdmissionError::Invalid(format!("parse Ding journal: {error}")))?;
    if canonical_bytes(&value)? != bytes {
        return Err(AdmissionError::Invalid(
            "Ding journal entry is not canonical JSON".to_owned(),
        ));
    }
    Ok(value)
}

fn atomic_replace<T: Serialize>(directory: &File, name: &str, value: &T) -> AdmissionResult<()> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let stage = format!(
        ".ding-journal-{}-{}.tmp",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut temp = create_regular_at(directory, &stage)?;
    temp.write_all(&canonical_bytes(value)?)
        .and_then(|_| temp.sync_all())
        .map_err(|error| AdmissionError::io("persist Ding journal stage", error))?;
    rename_at(directory, &stage, name)?;
    directory
        .sync_all()
        .map_err(|error| AdmissionError::io("sync Ding journal directory", error))
}

fn c_name(name: &str) -> AdmissionResult<CString> {
    CString::new(name).map_err(|_| AdmissionError::Invalid("Ding path contains NUL".to_owned()))
}

fn open_or_create_dir_at(parent: &File, name: &str) -> AdmissionResult<File> {
    safe_component("Ding journal directory", name)?;
    let name_c = c_name(name)?;
    let open = || unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name_c.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    let mut fd = open();
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(AdmissionError::io(
                format!("open Ding directory {name}"),
                error,
            ));
        }
        let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) };
        if created != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(AdmissionError::io(
                    format!("create Ding directory {name}"),
                    error,
                ));
            }
        } else {
            parent
                .sync_all()
                .map_err(|error| AdmissionError::io("sync Ding directory parent", error))?;
        }
        fd = open();
    }
    if fd < 0 {
        return Err(AdmissionError::io(
            format!("open Ding directory {name}"),
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: openat returned one newly-owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn open_regular_at(directory: &File, name: &str) -> AdmissionResult<File> {
    let name = c_name(name)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(AdmissionError::io(
            "open Ding journal entry",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: openat returned one newly-owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn create_regular_at(directory: &File, name: &str) -> AdmissionResult<File> {
    let name = c_name(name)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(AdmissionError::io(
            "create Ding journal entry",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: openat returned one newly-owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn rename_at(directory: &File, from: &str, to: &str) -> AdmissionResult<()> {
    let from = c_name(from)?;
    let to = c_name(to)?;
    let result = unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            from.as_ptr(),
            directory.as_raw_fd(),
            to.as_ptr(),
        )
    };
    if result != 0 {
        return Err(AdmissionError::io(
            "replace Ding journal entry",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::fs;

    #[derive(Clone, Copy)]
    enum Failure {
        None,
        BeforeSpawn,
        AfterSpawn,
    }

    struct FakeBackend {
        observed: RefCell<BTreeMap<String, DingObservation>>,
        spawns: Cell<usize>,
        failure: Cell<Failure>,
    }

    impl FakeBackend {
        fn new(failure: Failure) -> Self {
            Self {
                observed: RefCell::new(BTreeMap::new()),
                spawns: Cell::new(0),
                failure: Cell::new(failure),
            }
        }
    }

    impl DingExecBackend for FakeBackend {
        fn observe_ding(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>> {
            Ok(self.observed.borrow().get(runtime_id).cloned())
        }

        fn spawn_bound_ding(
            &self,
            desired: &DingDesiredExec,
            binding: ExecCutoverBinding,
        ) -> AdmissionResult<DingObservation> {
            let failure = self.failure.replace(Failure::None);
            if matches!(failure, Failure::BeforeSpawn) {
                return Err(AdmissionError::Conflict(
                    "injected pre-spawn crash".to_owned(),
                ));
            }
            self.spawns.set(self.spawns.get() + 1);
            let observation = DingObservation {
                generation: ExecGeneration {
                    schema: "st2.exec-generation.v3".to_owned(),
                    pid: 1000 + self.spawns.get() as u32,
                    created_at: "2026-07-31T00:00:00.000Z".to_owned(),
                    start_time_ticks: 42 + self.spawns.get() as u64,
                    generation_id: format!("exec-generation-{}", self.spawns.get()),
                    isolation: None,
                    cutover: Some(binding),
                },
                alive: true,
            };
            self.observed
                .borrow_mut()
                .insert(desired.runtime_id.clone(), observation.clone());
            if matches!(failure, Failure::AfterSpawn) {
                return Err(AdmissionError::Conflict(
                    "injected post-spawn pre-journal crash".to_owned(),
                ));
            }
            Ok(observation)
        }
    }

    impl DingGenerationReader for FakeBackend {
        fn observe_generation(&self, runtime_id: &str) -> AdmissionResult<Option<DingObservation>> {
            Ok(self.observed.borrow().get(runtime_id).cloned())
        }
    }

    fn fixture() -> (tempfile::TempDir, File, DingReconcileAction) {
        let catalog = tempfile::tempdir().unwrap();
        let workspace = catalog.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let declaration = format!(
            r#"agent "worker" {{
  identity "worker"
  host "host"
  workspace {:?}
  exec "ding" {{
    id "host.worker.ding"
    argv "st2" "ding" "--identity" "host.worker" "--root" "$ST_ROOT"
    env {{ ST_AGENT "host.worker" }}
  }}
}}
"#,
            workspace
        );
        let declaration_path = catalog.path().join("agents/host/worker/agent.kdl");
        fs::create_dir_all(declaration_path.parent().unwrap()).unwrap();
        fs::write(declaration_path, declaration).unwrap();
        let mut desired = DingDesiredExec {
            runtime_id: "host.worker.ding".to_owned(),
            canonical_argv: vec![
                "st2".to_owned(),
                "ding".to_owned(),
                "--identity".to_owned(),
                "host.worker".to_owned(),
                "--root".to_owned(),
                catalog.path().display().to_string(),
            ],
            canonical_cwd: workspace.canonicalize().unwrap(),
            canonical_env: BTreeMap::from([("ST_AGENT".to_owned(), "host.worker".to_owned())]),
            launch_sha256: String::new(),
        };
        desired.launch_sha256 = launch_sha256(&desired).unwrap();
        let action = DingReconcileAction {
            generation_id: "ding-generation-1".to_owned(),
            desired: vec![desired],
            desired_sha256: String::new(),
        };
        let action = DingReconcileAction {
            desired_sha256: desired_set_sha256(&action.desired).unwrap(),
            ..action
        };
        let cutover = catalog.path().join(".st2/cutover");
        fs::create_dir_all(&cutover).unwrap();
        (catalog, File::open(cutover).unwrap(), action)
    }

    #[test]
    fn post_spawn_crash_adopts_exact_bound_generation_without_duplicate() {
        let (catalog, cutover, action) = fixture();
        let backend = FakeBackend::new(Failure::AfterSpawn);
        assert!(
            reconcile(
                catalog.path(),
                &cutover,
                "host",
                "gate-1",
                3,
                &action,
                &backend,
            )
            .is_err()
        );
        assert_eq!(backend.spawns.get(), 1);
        let first = reconcile(
            catalog.path(),
            &cutover,
            "host",
            "gate-1",
            3,
            &action,
            &backend,
        )
        .unwrap();
        let replay = reconcile(
            catalog.path(),
            &cutover,
            "host",
            "gate-1",
            3,
            &action,
            &backend,
        )
        .unwrap();
        assert_eq!(backend.spawns.get(), 1);
        assert_eq!(first, replay);
    }

    #[test]
    fn pre_spawn_crash_retries_once_and_foreign_unbound_generation_never_spawns() {
        let (catalog, cutover, action) = fixture();
        let backend = FakeBackend::new(Failure::BeforeSpawn);
        assert!(
            reconcile(
                catalog.path(),
                &cutover,
                "host",
                "gate-1",
                3,
                &action,
                &backend,
            )
            .is_err()
        );
        assert_eq!(backend.spawns.get(), 0);
        reconcile(
            catalog.path(),
            &cutover,
            "host",
            "gate-1",
            3,
            &action,
            &backend,
        )
        .unwrap();
        assert_eq!(backend.spawns.get(), 1);

        let (catalog, cutover, action) = fixture();
        let backend = FakeBackend::new(Failure::None);
        backend.observed.borrow_mut().insert(
            action.desired[0].runtime_id.clone(),
            DingObservation {
                generation: ExecGeneration {
                    schema: "st2.exec-generation.v2".to_owned(),
                    pid: 9,
                    created_at: "2026-07-31T00:00:00.000Z".to_owned(),
                    start_time_ticks: 9,
                    generation_id: "foreign".to_owned(),
                    isolation: None,
                    cutover: None,
                },
                alive: true,
            },
        );
        assert!(
            reconcile(
                catalog.path(),
                &cutover,
                "host",
                "gate-1",
                3,
                &action,
                &backend,
            )
            .is_err()
        );
        assert_eq!(backend.spawns.get(), 0);
    }

    #[test]
    fn desired_set_rejects_duplicates_reordering_and_catalog_drift() {
        let (catalog, _cutover, action) = fixture();
        let mut duplicate = action.clone();
        duplicate.desired.push(duplicate.desired[0].clone());
        duplicate.desired_sha256 = desired_set_sha256(&duplicate.desired).unwrap();
        assert!(validate_action(&duplicate).is_err());

        let mut drift = action.clone();
        drift.desired[0]
            .canonical_env
            .insert("ST_AGENT".to_owned(), "wrong.worker".to_owned());
        drift.desired[0].launch_sha256 = launch_sha256(&drift.desired[0]).unwrap();
        drift.desired_sha256 = desired_set_sha256(&drift.desired).unwrap();
        assert!(derive_and_validate_catalog(catalog.path(), "host", &drift).is_err());
    }

    #[test]
    fn read_only_partition_proves_positive_absence_without_creating_a_journal() {
        let (catalog, cutover, action) = fixture();
        let canonical = CanonicalCatalog::open(catalog.path()).unwrap();
        let backend = FakeBackend::new(Failure::None);
        let observed = observe_successor_partition(
            &canonical,
            &cutover,
            "host",
            &GateId::parse("gate-1").unwrap(),
            3,
            &action,
            &backend,
        )
        .unwrap();
        assert_eq!(
            observed,
            vec![SuccessorDingObservation::Absent {
                runtime_id: action.desired[0].runtime_id.clone()
            }]
        );
        assert_eq!(backend.spawns.get(), 0);
        assert!(!catalog.path().join(".st2/cutover/ding").exists());
    }

    #[test]
    fn read_only_partition_accepts_only_exact_live_journal_binding() {
        let (catalog, cutover, action) = fixture();
        let canonical = CanonicalCatalog::open(catalog.path()).unwrap();
        let backend = FakeBackend::new(Failure::None);
        reconcile(
            catalog.path(),
            &cutover,
            "host",
            "gate-1",
            3,
            &action,
            &backend,
        )
        .unwrap();
        let spawned = backend.spawns.get();
        let observed = observe_successor_partition(
            &canonical,
            &cutover,
            "host",
            &GateId::parse("gate-1").unwrap(),
            3,
            &action,
            &backend,
        )
        .unwrap();
        assert!(matches!(
            &observed[0],
            SuccessorDingObservation::JournalBound {
                runtime_id,
                gate_id,
                action_index: 3,
                ding_generation_id,
                launch_sha256,
                journal_sha256,
            } if runtime_id == &action.desired[0].runtime_id
                && gate_id.as_str() == "gate-1"
                && ding_generation_id == &action.generation_id
                && launch_sha256 == &action.desired[0].launch_sha256
                && journal_sha256.len() == 64
        ));
        assert_eq!(backend.spawns.get(), spawned, "observation is read-only");

        backend
            .observed
            .borrow_mut()
            .get_mut(&action.desired[0].runtime_id)
            .unwrap()
            .generation
            .cutover
            .as_mut()
            .unwrap()
            .action_index = 4;
        assert!(
            observe_successor_partition(
                &canonical,
                &cutover,
                "host",
                &GateId::parse("gate-1").unwrap(),
                3,
                &action,
                &backend,
            )
            .is_err()
        );
        assert_eq!(backend.spawns.get(), spawned);
    }

    #[test]
    fn read_only_partition_rejects_live_generation_without_journal() {
        let (catalog, cutover, action) = fixture();
        let canonical = CanonicalCatalog::open(catalog.path()).unwrap();
        let backend = FakeBackend::new(Failure::None);
        let binding = ExecCutoverBinding {
            gate_id: "gate-1".to_owned(),
            action_index: 3,
            ding_generation_id: action.generation_id.clone(),
            launch_sha256: action.desired[0].launch_sha256.clone(),
        };
        backend.observed.borrow_mut().insert(
            action.desired[0].runtime_id.clone(),
            DingObservation {
                generation: ExecGeneration {
                    schema: "st2.exec-generation.v3".to_owned(),
                    pid: 7,
                    created_at: "2026-07-31T00:00:00.000Z".to_owned(),
                    start_time_ticks: 7,
                    generation_id: "orphan-generation".to_owned(),
                    isolation: None,
                    cutover: Some(binding),
                },
                alive: true,
            },
        );
        assert!(
            observe_successor_partition(
                &canonical,
                &cutover,
                "host",
                &GateId::parse("gate-1").unwrap(),
                3,
                &action,
                &backend,
            )
            .is_err()
        );
        assert_eq!(backend.spawns.get(), 0);
    }

    #[test]
    fn read_only_partition_rejects_foreign_names_across_the_full_journal_census() {
        for foreign_level in ["gate", "generation", "entry"] {
            let (catalog, cutover, action) = fixture();
            let canonical = CanonicalCatalog::open(catalog.path()).unwrap();
            let backend = FakeBackend::new(Failure::None);
            let ding = catalog.path().join(".st2/cutover/ding");
            match foreign_level {
                "gate" => {
                    fs::create_dir_all(ding.join("foreign-gate")).unwrap();
                }
                "generation" => {
                    fs::create_dir_all(ding.join("gate-1/foreign-generation")).unwrap();
                }
                "entry" => {
                    reconcile(
                        catalog.path(),
                        &cutover,
                        "host",
                        "gate-1",
                        3,
                        &action,
                        &backend,
                    )
                    .unwrap();
                    fs::write(
                        ding.join("gate-1")
                            .join(&action.generation_id)
                            .join("foreign.json"),
                        b"{}\n",
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                observe_successor_partition(
                    &canonical,
                    &cutover,
                    "host",
                    &GateId::parse("gate-1").unwrap(),
                    3,
                    &action,
                    &backend,
                )
                .is_err(),
                "foreign {foreign_level} name was accepted"
            );
        }
    }

    #[test]
    fn mutating_reconcile_rejects_foreign_namespace_before_journal_or_spawn_side_effects() {
        for foreign_level in ["gate", "generation", "entry"] {
            let (catalog, cutover, action) = fixture();
            let backend = FakeBackend::new(Failure::None);
            let ding = catalog.path().join(".st2/cutover/ding");
            let target = ding.join("gate-1").join(&action.generation_id);
            let foreign = match foreign_level {
                "gate" => ding.join("foreign-gate"),
                "generation" => ding.join("gate-1/foreign-generation"),
                "entry" => target.join("foreign.json"),
                _ => unreachable!(),
            };
            if foreign_level == "entry" {
                fs::create_dir_all(&target).unwrap();
                fs::write(&foreign, b"foreign\n").unwrap();
            } else {
                fs::create_dir_all(&foreign).unwrap();
            }
            assert!(
                reconcile(
                    catalog.path(),
                    &cutover,
                    "host",
                    "gate-1",
                    3,
                    &action,
                    &backend,
                )
                .is_err(),
                "foreign {foreign_level} namespace was accepted"
            );
            assert_eq!(backend.spawns.get(), 0);
            assert!(foreign.exists(), "foreign evidence was mutated");
            assert!(
                !target.join("journal.json").exists(),
                "journal was created before namespace rejection"
            );
        }
    }

    #[test]
    #[ignore = "isolated live user-systemd crash recovery; run explicitly with target/debug on PATH"]
    fn live_systemd_spawn_before_record_recovers_exactly_once() {
        use std::process::{Command, Stdio};

        assert_eq!(crate::isolate::mode(), crate::isolate::Isolation::Scope);
        let mut random = [0_u8; 16];
        File::open("/dev/urandom")
            .unwrap()
            .read_exact(&mut random)
            .unwrap();
        let nonce = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let identity = format!("worker-{nonce}");
        let runtime_id = format!("host.{identity}.ding");
        let gate_id = format!("gate-{nonce}");
        let generation_id = format!("generation-{nonce}");
        let catalog = tempfile::tempdir().unwrap();
        let workspace = catalog.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let pty_root = catalog.path().join("pty");
        fs::create_dir_all(&pty_root).unwrap();
        struct PtyRootGuard(Option<std::ffi::OsString>);
        impl Drop for PtyRootGuard {
            fn drop(&mut self) {
                if let Some(original) = self.0.take() {
                    unsafe { std::env::set_var("PTY_ROOT", original) };
                } else {
                    unsafe { std::env::remove_var("PTY_ROOT") };
                }
            }
        }
        let _pty_root_guard = PtyRootGuard(std::env::var_os("PTY_ROOT"));
        unsafe { std::env::set_var("PTY_ROOT", &pty_root) };
        fs::write(
            pty_root.join(format!("host.{identity}.pid")),
            std::process::id().to_string(),
        )
        .unwrap();
        let declaration = format!(
            r#"agent "{identity}" {{
  identity "{identity}"
  host "host"
  workspace {:?}
  exec "ding" {{
    id "{runtime_id}"
    argv "st2" "ding" "--identity" "host.{identity}" "--root" "$ST_ROOT"
    env {{ ST_AGENT "host.{identity}" }}
  }}
}}
"#,
            workspace
        );
        let declaration_path = catalog
            .path()
            .join(format!("agents/host/{identity}/agent.kdl"));
        fs::create_dir_all(declaration_path.parent().unwrap()).unwrap();
        fs::write(declaration_path, declaration).unwrap();
        let mut desired = DingDesiredExec {
            runtime_id: runtime_id.clone(),
            canonical_argv: vec![
                "st2".to_owned(),
                "ding".to_owned(),
                "--identity".to_owned(),
                format!("host.{identity}"),
                "--root".to_owned(),
                catalog.path().display().to_string(),
            ],
            canonical_cwd: workspace.canonicalize().unwrap(),
            canonical_env: BTreeMap::from([("ST_AGENT".to_owned(), format!("host.{identity}"))]),
            launch_sha256: String::new(),
        };
        desired.launch_sha256 = launch_sha256(&desired).unwrap();
        let binding = ExecCutoverBinding {
            gate_id: gate_id.clone(),
            action_index: 0,
            ding_generation_id: generation_id.clone(),
            launch_sha256: desired.launch_sha256.clone(),
        };
        let unit = crate::exec_backend::cutover_scope_unit(&runtime_id, &binding);
        let collision = Command::new("systemctl")
            .args(["--user", "show", &unit, "--property=LoadState", "--value"])
            .stderr(Stdio::null())
            .output()
            .unwrap();
        let load_state = String::from_utf8(collision.stdout).unwrap();
        assert!(
            !collision.status.success()
                || load_state.trim().is_empty()
                || load_state.trim() == "not-found",
            "cryptographically unique live-test unit already exists: {unit} ({})",
            load_state.trim()
        );
        struct ExactUnitGuard(String);
        impl Drop for ExactUnitGuard {
            fn drop(&mut self) {
                let _ = Command::new("systemctl")
                    .args(["--user", "stop", &self.0])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
        let _guard = ExactUnitGuard(unit.clone());
        let desired = vec![desired];
        let action = DingReconcileAction {
            generation_id,
            desired_sha256: desired_set_sha256(&desired).unwrap(),
            desired,
        };
        let cutover_path = catalog.path().join(".st2/cutover");
        fs::create_dir_all(&cutover_path).unwrap();
        let cutover = File::open(&cutover_path).unwrap();
        let state = catalog.path().join("state");
        let backend = SystemDingExecBackend::new(state.clone(), catalog.path().to_path_buf());

        unsafe { std::env::set_var("ST2_TEST_CUTOVER_DING_CRASH_AFTER_SCOPE", &runtime_id) };
        let first = reconcile(
            catalog.path(),
            &cutover,
            "host",
            &gate_id,
            0,
            &action,
            &backend,
        );
        unsafe { std::env::remove_var("ST2_TEST_CUTOVER_DING_CRASH_AFTER_SCOPE") };
        assert!(first.is_err(), "injected crash boundary must interrupt");
        assert!(
            !state.join(format!("{runtime_id}.pid")).exists(),
            "crash boundary must precede v3 publication"
        );

        let receipt = reconcile(
            catalog.path(),
            &cutover,
            "host",
            &gate_id,
            0,
            &action,
            &backend,
        )
        .unwrap();
        let replay = reconcile(
            catalog.path(),
            &cutover,
            "host",
            &gate_id,
            0,
            &action,
            &backend,
        )
        .unwrap();
        assert_eq!(receipt, replay);
        let control_group = Command::new("systemctl")
            .args([
                "--user",
                "show",
                &unit,
                "--property=ControlGroup",
                "--value",
            ])
            .output()
            .unwrap();
        assert!(control_group.status.success());
        let group = String::from_utf8(control_group.stdout)
            .unwrap()
            .trim()
            .to_owned();
        let members = fs::read_to_string(
            Path::new("/sys/fs/cgroup")
                .join(group.trim_start_matches('/'))
                .join("cgroup.procs"),
        )
        .unwrap();
        assert_eq!(
            members
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count(),
            1,
            "exact deterministic scope must contain one successor Ding and no orphan/duplicate"
        );
        let generation: ExecGeneration =
            serde_json::from_slice(&fs::read(state.join(format!("{runtime_id}.pid"))).unwrap())
                .unwrap();
        assert_eq!(generation.cutover.as_ref(), Some(&binding));
    }
}
