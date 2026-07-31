//! Crash-recoverable retirement of exact strict-v2 exec generations.
//!
//! This module is deliberately not a compatibility implementation of `ExecBackend::remove`.
//! Preparation accepts one live strict-v2 generation, takes an exclusive state-namespace lock,
//! and publishes an immutable plan. Apply accepts only that plan, re-censuses the namespace, pins
//! the exact leader with a pidfd, securely opens its dedicated leaf cgroup-v2 scope, freezes and
//! revalidates every member, uses `cgroup.kill`, proves the scope empty, and retires the record.
//!
//! A record is never unlinked.  It is moved with `renameat2(RENAME_NOREPLACE)` into a private slot,
//! then the moved inode and bytes are verified.  A raced move is rolled back without replacement;
//! if that rollback conflicts, both names are preserved and the journal records the conflict.
//! There is no numeric-PID/process-group, PTY, path-unlink, or whole-directory fallback.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsStr};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result as AnyResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const PLAN_SCHEMA: &str = "st2.exec-retirement-plan.v1";
const PREPARATION_SCHEMA: &str = "st2.exec-retirement-preparation.v1";
const JOURNAL_SCHEMA: &str = "st2.exec-retirement-journal.v1";
const RECEIPT_SCHEMA: &str = "st2.exec-retirement.v1";
const REQUEST_HASH_DOMAIN: &[u8] = b"st2.exec-retirement-request.v1\0";
const CENSUS_HASH_DOMAIN: &[u8] = b"st2.exec-state-census.v1\0";
const RECORD_HASH_DOMAIN: &[u8] = b"st2.exec-record.v1\0";
const LEGACY_PARTITION_HASH_DOMAIN: &[u8] = b"st2.exec-retirement-legacy-partition.v1\0";
const CONTROL_DIR: &str = ".retirements";
const LOCK_FILE: &str = ".exec-retirement.lock";
const PLAN_FILE: &str = "plan.json";
const JOURNAL_FILE: &str = "journal.json";
const RECEIPT_FILE: &str = "receipt.json";
const SLOT_DIR: &str = "records";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const CGROUP2_SUPER_MAGIC: libc::c_long = 0x6367_7270;
const WAIT_LIMIT: Duration = Duration::from_secs(10);

type Result<T> = AnyResult<T>;

pub type RetirementResult<T> = std::result::Result<T, RetirementError>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetirementErrorCode {
    Unsupported,
    Authority,
    Conflict,
    Recoverable,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetirementError {
    pub code: RetirementErrorCode,
    pub message: String,
}

impl fmt::Display for RetirementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for RetirementError {}

#[derive(Debug)]
struct TaggedRetirementError {
    code: RetirementErrorCode,
    message: String,
}

impl fmt::Display for TaggedRetirementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TaggedRetirementError {}

fn tagged(code: RetirementErrorCode, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(TaggedRetirementError {
        code,
        message: message.into(),
    })
}

impl RetirementErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Authority => "authority",
            Self::Conflict => "conflict",
            Self::Recoverable => "recoverable",
        }
    }
}

fn public_error(error: anyhow::Error) -> RetirementError {
    let message = format!("{error:#}");
    let code = if error.downcast_ref::<RecoverableApplyError>().is_some() {
        RetirementErrorCode::Recoverable
    } else if let Some(tagged) = error.downcast_ref::<TaggedRetirementError>() {
        tagged.code
    } else {
        RetirementErrorCode::Authority
    };
    RetirementError { code, message }
}

/// A preparation targets one exact strict-v2 runtime generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetirementSelector {
    Id(String),
}

#[derive(Clone, Debug)]
pub struct RetirementPrepareRequest {
    pub catalog: PathBuf,
    pub host: String,
    pub selector: RetirementSelector,
    pub expect_catalog_sha256: String,
    pub output: PathBuf,
}

#[derive(Clone, Debug)]
pub struct RetirementApplyRequest {
    pub catalog: PathBuf,
    pub plan: PathBuf,
    /// Caller-held digest returned by [`prepare`].
    pub expect_plan_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetirementPreparation {
    pub schema: &'static str,
    pub plan_sha256: String,
    pub catalog: PathBuf,
    pub host: String,
    pub catalog_sha256: String,
    pub state_dir: PathBuf,
    pub output: PathBuf,
    pub census_sha256: String,
    pub legacy_partition_sha256: String,
    pub targets: usize,
}

impl RetirementPreparation {
    pub fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }

    pub fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn legacy_partition_sha256(&self) -> &str {
        &self.legacy_partition_sha256
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetirementPlan {
    schema: String,
    request_sha256: String,
    catalog: PathBuf,
    host: String,
    catalog_sha256: String,
    state_dir_device: u64,
    state_dir_inode: u64,
    census_sha256: String,
    census: Vec<CensusEntry>,
    selection: SelectionWire,
    legacy_partition: Option<Vec<LegacySuccessorTask>>,
    targets: Vec<PlannedTarget>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind", deny_unknown_fields)]
enum SelectionWire {
    Id { runtime_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CensusEntry {
    name: String,
    device: u64,
    inode: u64,
    length: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PlannedTarget {
    runtime_id: String,
    generation_id: Option<String>,
    authority_kind: RetirementAuthorityKind,
    record: RecordEvidence,
    classification: PlannedClassification,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetirementAuthorityKind {
    StrictGenerationV2,
    LegacyScopeV1,
    StaleRecordOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuccessorDesiredState {
    RunningDing,
    AbsentRetired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySuccessorTask {
    pub runtime_id: String,
    pub agent: String,
    pub task: String,
    pub desired_state: SuccessorDesiredState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "state", deny_unknown_fields)]
enum PlannedClassification {
    Live {
        pid: i32,
        start_time_ticks: u64,
        scope_unit: String,
        cgroup_path: String,
        cgroup_device: u64,
        cgroup_inode: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyScopeWitness {
    pub scope_id: String,
    pub control_group: String,
    pub invocation_id: String,
    pub active_enter_timestamp_monotonic: u64,
    pub slice: String,
    pub member: LegacyProcessWitness,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyProcessWitness {
    pub pid: i32,
    pub start_time_ticks: u64,
    pub uid: u32,
    pub executable: PathBuf,
    pub executable_device: u64,
    pub executable_inode: u64,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub cwd_device: u64,
    pub cwd_inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecordEvidence {
    relative_path: String,
    device: u64,
    inode: u64,
    length: u64,
    modified_unix_ns: i128,
    sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ItemPhase {
    Prepared,
    MutationAuthorized,
    Frozen,
    Killed,
    RecordRetired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaderlessFreezeAction {
    Freeze,
    AlreadyFrozen,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum JournalStatus {
    Prepared,
    Applying,
    Completed,
    Conflict,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetirementJournal {
    schema: String,
    request_sha256: String,
    plan_sha256: String,
    status: JournalStatus,
    forward_only_started: bool,
    items: BTreeMap<String, JournalItem>,
    conflict: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JournalItem {
    phase: ItemPhase,
    membership: Vec<MemberEvidence>,
    freeze_observed: bool,
    cgroup_outcome: Option<CgroupRetirementOutcome>,
}

#[derive(Debug)]
struct RecoverableApplyError(String);

impl fmt::Display for RecoverableApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RecoverableApplyError {}

fn recoverable(error: impl fmt::Display, operation: &str) -> anyhow::Error {
    anyhow::Error::new(RecoverableApplyError(format!("{operation}: {error}")))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecRetirementReceipt {
    pub schema: String,
    pub request_sha256: String,
    pub plan_sha256: String,
    pub catalog: PathBuf,
    pub host: String,
    pub catalog_sha256: String,
    pub state_dir_device: u64,
    pub state_dir_inode: u64,
    pub journal_schema: String,
    pub journal_sha256: String,
    pub journal_status: String,
    pub status: ExecRetirementStatus,
    pub completed_at_unix_ms: u128,
    pub census_sha256: String,
    pub forward_only_started: bool,
    pub legacy_partition_sha256: String,
    pub legacy_partition: Option<Vec<LegacySuccessorTask>>,
    pub targets: Vec<RetiredTarget>,
}

impl ExecRetirementReceipt {
    pub fn canonical_sha256(&self) -> RetirementResult<String> {
        canonical_json(self)
            .map(|bytes| sha256(&bytes))
            .map_err(public_error)
    }

    pub fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }

    pub fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn legacy_partition_sha256(&self) -> &str {
        &self.legacy_partition_sha256
    }

    pub fn forward_only_started(&self) -> bool {
        self.forward_only_started
    }
}

/// The public apply result. The older internal name remains as an alias while the CLI wiring lands.
pub type RetirementApplyReceipt = ExecRetirementReceipt;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecRetirementStatus {
    Completed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetiredTarget {
    pub runtime_id: String,
    pub generation_id: Option<String>,
    pub authority_kind: RetirementAuthorityKind,
    pub disposition: RetiredDisposition,
    pub pid: i32,
    pub start_time_ticks: Option<u64>,
    pub cgroup_path: Option<String>,
    pub scope_unit: Option<String>,
    pub cgroup_device: Option<u64>,
    pub cgroup_inode: Option<u64>,
    pub legacy_scope: Option<LegacyScopeWitness>,
    pub membership: Vec<MemberEvidence>,
    pub freeze_observed: bool,
    pub cgroup_outcome: Option<CgroupRetirementOutcome>,
    pub durable_phase: String,
    pub record_before: RetiredRecordEvidence,
    pub record_after: RetiredRecordEvidence,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CgroupRetirementOutcome {
    KillApplied,
    AlreadyEmpty,
    ScopeCollected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetiredDisposition {
    StaleRecordOnly,
    CgroupRetired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberEvidence {
    pub pid: i32,
    pub start_time_ticks: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetiredRecordEvidence {
    pub relative_path: String,
    pub device: u64,
    pub inode: u64,
    pub length: u64,
    pub modified_unix_ns: i128,
    pub sha256: String,
}

struct StateLock {
    state_dir: File,
    _control_dir: File,
    _lock: File,
    path: PathBuf,
}

impl StateLock {
    fn acquire(path: &Path) -> Result<Self> {
        let path = path
            .canonicalize()
            .with_context(|| format!("canonicalize exec state directory {}", path.display()))?;
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect exec state directory {}", path.display()))?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "exec state root is not a real directory: {}",
            path.display()
        );
        let state_dir = open_dir_nofollow(&path)?;
        let lock = openat_create_regular(
            &state_dir,
            OsStr::new(LOCK_FILE),
            libc::O_RDWR | libc::O_CREAT,
            0o600,
        )
        .with_context(|| format!("open exec retirement lock in {}", path.display()))?;
        let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .context("acquire exec retirement namespace lock");
        }
        let control_dir = ensure_control_dir(&state_dir)?;
        Ok(Self {
            state_dir,
            _control_dir: control_dir,
            _lock: lock,
            path,
        })
    }

    fn capability_path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from(format!("/proc/self/fd/{}", self.state_dir.as_raw_fd()))
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.path.clone()
        }
    }
}

/// Capture and durably publish an immutable retirement plan.
pub fn prepare(request: RetirementPrepareRequest) -> RetirementResult<RetirementPreparation> {
    prepare_inner(request).map_err(public_error)
}

fn prepare_inner(request: RetirementPrepareRequest) -> Result<RetirementPreparation> {
    validate_sha256(&request.expect_catalog_sha256)?;
    validate_host_component(&request.host)?;
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    let ownership = crate::host_lock::HostOwnership::acquire(&catalog, &request.host)
        .with_context(|| format!("claim retirement host ownership for {:?}", request.host))?;
    let admission = crate::cutover_admission::RuntimeMutationAdmission::ordinary(&ownership)
        .context("admit ordinary exec retirement")?;
    prepare_admitted(request, &admission.permission())
}

/// Prepare a retirement while the caller retains exact runtime-mutation authority.
///
/// This is the composable seam used by a cutover transaction: it neither acquires another host
/// lock nor re-enters catalog admission. The request is bound to the token before state locking,
/// control-directory creation, or plan publication.
pub(crate) fn prepare_admitted(
    request: RetirementPrepareRequest,
    permission: &crate::cutover_admission::RuntimeMutate<'_>,
) -> Result<RetirementPreparation> {
    validate_sha256(&request.expect_catalog_sha256)?;
    validate_host_component(&request.host)?;
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    ensure_permission_matches(permission, &catalog, &request.host)?;
    let catalog_sha256 = crate::catalog_transaction::declaration_root_sha256_locked(&catalog)?;
    anyhow::ensure!(
        catalog_sha256 == request.expect_catalog_sha256,
        "catalog declaration root changed: expected {}, found {}",
        request.expect_catalog_sha256,
        catalog_sha256
    );
    let state_dir = crate::run::exec_state_dir(&request.host);
    let locked = StateLock::acquire(&state_dir)?;
    let output = absolute_output(&request.output)?;
    anyhow::ensure!(
        !output.starts_with(&locked.path) && !output.starts_with(&catalog),
        "retirement plan output must be outside the exec state directory and catalog"
    );
    let metadata = locked.state_dir.metadata()?;
    let census = census_state_namespace(&locked)?;
    let census_sha256 = hash_census(&census);
    let legacy_partition = None;
    let legacy_partition_sha256 = hash_legacy_partition(legacy_partition.as_deref())?;
    let mut targets = Vec::new();
    for entry in &census {
        let Some(runtime_id) = entry.name.strip_suffix(".pid") else {
            continue;
        };
        if !safe_runtime_id(runtime_id) {
            anyhow::bail!("unsafe exec runtime id in state namespace: {runtime_id:?}");
        }
        let RetirementSelector::Id(selected) = &request.selector;
        let selected = selected == runtime_id;
        if !selected {
            continue;
        }
        let record = open_record(&locked, runtime_id)?;
        let raw = read_exact_record(&record.file)?;
        let generation: crate::exec_backend::ExecGeneration = serde_json::from_slice(&raw)
            .map_err(|_| {
                tagged(
                    RetirementErrorCode::Unsupported,
                    format!(
                        "runtime {runtime_id:?} is not a strict-v2 generation; predecessor records \
                         are read-only"
                    ),
                )
            })?;
        crate::exec_backend::validate_generation(runtime_id, &generation)
            .map_err(anyhow::Error::msg)?;
        if generation.schema != crate::exec_backend::EXEC_GENERATION_SCHEMA_V2 {
            return Err(tagged(
                RetirementErrorCode::Unsupported,
                format!("runtime {runtime_id:?} is not a strict-v2 generation and is read-only"),
            ));
        }
        let classification = classify_strict_generation(&generation)?;
        targets.push(PlannedTarget {
            runtime_id: runtime_id.to_string(),
            generation_id: Some(generation.generation_id),
            authority_kind: RetirementAuthorityKind::StrictGenerationV2,
            record: record.evidence,
            classification,
        });
    }
    targets.sort_by(|a, b| a.runtime_id.cmp(&b.runtime_id));

    let RetirementSelector::Id(runtime_id) = request.selector;
    anyhow::ensure!(
        safe_runtime_id(&runtime_id),
        "unsafe runtime id {runtime_id:?}"
    );
    targets.retain(|target| target.runtime_id == runtime_id);
    anyhow::ensure!(
        targets.len() == 1,
        "runtime {runtime_id:?} is not exactly one strict-v2 generation"
    );
    let selection = SelectionWire::Id { runtime_id };
    let request_sha256 = hash_request(
        &selection,
        &census_sha256,
        legacy_partition.as_deref(),
        &targets,
    )?;
    let plan = RetirementPlan {
        schema: PLAN_SCHEMA.to_string(),
        request_sha256: request_sha256.clone(),
        catalog: catalog.clone(),
        host: request.host.clone(),
        catalog_sha256: catalog_sha256.clone(),
        state_dir_device: metadata.dev(),
        state_dir_inode: metadata.ino(),
        census_sha256,
        census,
        selection,
        legacy_partition,
        targets,
    };
    let plan_bytes = canonical_json(&plan)?;
    let plan_sha256 = sha256(&plan_bytes);
    let transaction = transaction_dir(&locked.capability_path(), &request_sha256);
    create_or_validate_transaction(&transaction, &plan_bytes, &plan_sha256)?;
    publish_or_validate_plan_output(&output, &plan_bytes, &plan_sha256)?;
    let journal = RetirementJournal {
        schema: JOURNAL_SCHEMA.to_string(),
        request_sha256: request_sha256.clone(),
        plan_sha256: plan_sha256.clone(),
        status: JournalStatus::Prepared,
        forward_only_started: false,
        items: plan
            .targets
            .iter()
            .map(|target| {
                (
                    target.runtime_id.clone(),
                    JournalItem {
                        phase: ItemPhase::Prepared,
                        membership: Vec::new(),
                        freeze_observed: false,
                        cgroup_outcome: None,
                    },
                )
            })
            .collect(),
        conflict: None,
    };
    create_or_validate_journal(&transaction, &journal)?;
    sync_dir(&locked.capability_path().join(CONTROL_DIR))?;
    Ok(RetirementPreparation {
        schema: PREPARATION_SCHEMA,
        plan_sha256,
        catalog,
        host: request.host,
        catalog_sha256,
        state_dir: locked.path,
        output,
        census_sha256: plan.census_sha256,
        legacy_partition_sha256,
        targets: plan.targets.len(),
    })
}

fn ensure_permission_matches(
    permission: &crate::cutover_admission::RuntimeMutate<'_>,
    catalog: &Path,
    host: &str,
) -> Result<()> {
    anyhow::ensure!(
        permission.catalog().as_path() == catalog,
        "runtime mutation authority belongs to catalog {}, not {}",
        permission.catalog().as_path().display(),
        catalog.display()
    );
    anyhow::ensure!(
        permission.host().as_str() == host,
        "runtime mutation authority belongs to host {:?}, not {:?}",
        permission.host().as_str(),
        host
    );
    Ok(())
}

/// Apply or resume the immutable plan selected by `expect_plan_sha256`.
///
/// Expected race/conflict refusals are returned as errors after the durable journal is updated.
/// A completed repeated request returns the byte-identical stored receipt.
pub fn apply(request: RetirementApplyRequest) -> RetirementResult<RetirementApplyReceipt> {
    apply_inner(request).map_err(public_error)
}

fn apply_inner(request: RetirementApplyRequest) -> Result<RetirementApplyReceipt> {
    validate_sha256(&request.expect_plan_sha256)?;
    let plan_bytes = read_regular_nofollow(&request.plan)?;
    anyhow::ensure!(
        sha256(&plan_bytes) == request.expect_plan_sha256,
        "retirement plan digest does not match caller expectation"
    );
    let plan: RetirementPlan =
        serde_json::from_slice(&plan_bytes).context("decode exec retirement plan")?;
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    anyhow::ensure!(
        catalog == plan.catalog,
        "retirement plan belongs to catalog {}, not {}",
        plan.catalog.display(),
        catalog.display()
    );
    let ownership = crate::host_lock::HostOwnership::acquire(&catalog, &plan.host)
        .with_context(|| format!("claim retirement host ownership for {:?}", plan.host))?;
    let admission = crate::cutover_admission::RuntimeMutationAdmission::ordinary(&ownership)
        .context("admit ordinary exec retirement")?;
    apply_admitted(request, &admission.permission())
}

/// Apply or resume a retirement while the caller retains exact runtime-mutation authority.
///
/// The caller-held token makes this safe to compose inside a durable cutover without attempting
/// to reacquire either the host lock or the ordinary catalog admission lock.
pub(crate) fn apply_admitted(
    request: RetirementApplyRequest,
    permission: &crate::cutover_admission::RuntimeMutate<'_>,
) -> Result<RetirementApplyReceipt> {
    validate_sha256(&request.expect_plan_sha256)?;
    let plan_bytes = read_regular_nofollow(&request.plan)?;
    anyhow::ensure!(
        sha256(&plan_bytes) == request.expect_plan_sha256,
        "retirement plan digest does not match caller expectation"
    );
    let plan: RetirementPlan =
        serde_json::from_slice(&plan_bytes).context("decode exec retirement plan")?;
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    anyhow::ensure!(
        catalog == plan.catalog,
        "retirement plan belongs to catalog {}, not {}",
        plan.catalog.display(),
        catalog.display()
    );
    ensure_permission_matches(permission, &catalog, &plan.host)?;
    let catalog_sha256 = crate::catalog_transaction::declaration_root_sha256_locked(&catalog)?;
    anyhow::ensure!(
        catalog_sha256 == plan.catalog_sha256,
        "catalog declaration root changed: plan {}, current {}",
        plan.catalog_sha256,
        catalog_sha256
    );
    let state_dir = crate::run::exec_state_dir(&plan.host);
    let locked = StateLock::acquire(&state_dir)?;
    let transaction =
        find_transaction_by_plan(&locked.capability_path(), &request.expect_plan_sha256)?;
    let internal_plan_bytes = read_regular_nofollow(&transaction.join(PLAN_FILE))?;
    anyhow::ensure!(
        internal_plan_bytes == plan_bytes,
        "private transaction plan differs from caller-held plan"
    );
    validate_plan(&locked, &plan, &request.expect_plan_sha256)?;
    let mut journal: RetirementJournal = read_json(&transaction.join(JOURNAL_FILE))?;
    validate_journal(&plan, &request.expect_plan_sha256, &journal)?;
    if let Some(receipt) =
        read_optional_json::<RetirementApplyReceipt>(&transaction.join(RECEIPT_FILE))?
    {
        anyhow::ensure!(
            receipt.schema == RECEIPT_SCHEMA
                && receipt.plan_sha256 == request.expect_plan_sha256
                && receipt.request_sha256 == plan.request_sha256
                && receipt.catalog == plan.catalog
                && receipt.host == plan.host
                && receipt.catalog_sha256 == plan.catalog_sha256
                && receipt.state_dir_device == plan.state_dir_device
                && receipt.state_dir_inode == plan.state_dir_inode
                && receipt.journal_schema == JOURNAL_SCHEMA
                && receipt.journal_status == "completed"
                && receipt.census_sha256 == plan.census_sha256
                && receipt.forward_only_started
                && receipt.legacy_partition_sha256
                    == hash_legacy_partition(plan.legacy_partition.as_deref())?
                && receipt.legacy_partition == plan.legacy_partition
                && journal.status == JournalStatus::Completed
                && journal.conflict.is_none()
                && journal.forward_only_started
                && receipt.journal_sha256 == sha256(&canonical_json(&journal)?)
                && receipt_targets_bind_plan(&receipt, &plan, &journal, &transaction,)?,
            "stored retirement receipt does not bind the exact completed transaction"
        );
        return Ok(receipt);
    }
    anyhow::ensure!(
        journal.status != JournalStatus::Conflict,
        "retirement transaction is in conflict: {}",
        journal.conflict.as_deref().unwrap_or("unspecified")
    );

    // First reconcile the only legitimate journal lag: renameat2 completed but the subsequent
    // journal write did not. The exact private inode/bytes are the recovery authority.
    reconcile_moved_records(&locked, &transaction, &plan, &mut journal)?;
    // Every invocation, including crash recovery, proves the complete remaining namespace before
    // it can resume another irreversible phase.
    verify_remaining_namespace(&locked, &plan, &journal)?;
    if journal.status == JournalStatus::Prepared {
        journal.status = JournalStatus::Applying;
        write_journal(&transaction, &journal)?;
    }

    let mut retired = Vec::new();
    for target in &plan.targets {
        let phase = journal
            .items
            .get(&target.runtime_id)
            .context("retirement journal omitted a plan target")?
            .phase;
        match apply_target(&locked, &transaction, &plan, target, phase, &mut journal) {
            Ok(receipt) => retired.push(receipt),
            Err(error) => {
                if error.downcast_ref::<RecoverableApplyError>().is_some() {
                    journal.status = JournalStatus::Applying;
                    journal.conflict = None;
                    let _ = write_journal(&transaction, &journal);
                } else {
                    journal.status = JournalStatus::Conflict;
                    journal.conflict = Some(format!("{:#}", error));
                    write_journal(&transaction, &journal)?;
                    return Err(tagged(
                        RetirementErrorCode::Conflict,
                        format!("retirement transaction conflicted: {error:#}"),
                    ));
                }
                return Err(error);
            }
        }
    }
    verify_remaining_namespace(&locked, &plan, &journal)?;
    journal.status = JournalStatus::Completed;
    journal.conflict = None;
    write_journal(&transaction, &journal)?;
    test_checkpoint("after-completed-journal");
    let journal_sha256 = sha256(&canonical_json(&journal)?);
    let legacy_partition_sha256 = hash_legacy_partition(plan.legacy_partition.as_deref())?;
    let receipt = ExecRetirementReceipt {
        schema: RECEIPT_SCHEMA.to_string(),
        request_sha256: plan.request_sha256,
        plan_sha256: request.expect_plan_sha256,
        catalog: plan.catalog,
        host: plan.host,
        catalog_sha256: plan.catalog_sha256,
        state_dir_device: plan.state_dir_device,
        state_dir_inode: plan.state_dir_inode,
        journal_schema: JOURNAL_SCHEMA.to_string(),
        journal_sha256,
        journal_status: "completed".to_string(),
        status: ExecRetirementStatus::Completed,
        completed_at_unix_ms: unix_ms()?,
        census_sha256: plan.census_sha256,
        forward_only_started: journal.forward_only_started,
        legacy_partition_sha256,
        legacy_partition: plan.legacy_partition,
        targets: retired,
    };
    publish_create_only_json(&transaction.join(RECEIPT_FILE), &receipt)?;
    sync_dir(&transaction)?;
    Ok(receipt)
}

fn receipt_targets_bind_plan(
    receipt: &RetirementApplyReceipt,
    plan: &RetirementPlan,
    journal: &RetirementJournal,
    transaction: &Path,
) -> Result<bool> {
    if receipt.targets.len() != plan.targets.len() {
        return Ok(false);
    }
    for (actual, expected) in receipt.targets.iter().zip(&plan.targets) {
        let Some(item) = journal.items.get(&expected.runtime_id) else {
            return Ok(false);
        };
        if item.phase != ItemPhase::RecordRetired {
            return Ok(false);
        }
        let slot_relative = format!("{SLOT_DIR}/{}.pid", expected.runtime_id);
        let after = verify_retired_slot(&transaction.join(&slot_relative), &expected.record)?;
        if actual != &retired_target(expected, item, after, slot_relative) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn enter_forward_only(transaction: &Path, journal: &mut RetirementJournal) -> Result<()> {
    if !journal.forward_only_started {
        journal.forward_only_started = true;
        journal.status = JournalStatus::Applying;
        write_journal(transaction, journal)?;
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn test_checkpoint(point: &str) {
    if std::env::var("ST2_TEST_EXEC_RETIREMENT_CRASH_AT").as_deref() == Ok(point) {
        std::process::abort();
    }
    if std::env::var("ST2_TEST_EXEC_RETIREMENT_PAUSE_AT").as_deref() != Ok(point) {
        return;
    }
    if let Some(ready) = std::env::var_os("ST2_TEST_EXEC_RETIREMENT_READY") {
        let _ = fs::write(ready, format!("{point}\n"));
    }
    let release = std::env::var_os("ST2_TEST_EXEC_RETIREMENT_RELEASE")
        .map(PathBuf::from)
        .expect("paused retirement test omitted release path");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !release.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting to release retirement checkpoint {point}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(debug_assertions))]
fn test_checkpoint(_point: &str) {}

fn reconcile_moved_records(
    locked: &StateLock,
    transaction: &Path,
    plan: &RetirementPlan,
    journal: &mut RetirementJournal,
) -> Result<()> {
    let mut changed = false;
    for target in &plan.targets {
        let item = journal
            .items
            .get(&target.runtime_id)
            .context("retirement journal omitted target state")?;
        if item.phase == ItemPhase::RecordRetired {
            continue;
        }
        let source = locked.capability_path().join(&target.record.relative_path);
        let slot = transaction
            .join(SLOT_DIR)
            .join(format!("{}.pid", target.runtime_id));
        let source_exists = path_exists_nofollow(&source)?;
        let slot_exists = path_exists_nofollow(&slot)?;
        match (source_exists, slot_exists) {
            (true, false) => {}
            (false, true) => {
                if matches!(&target.classification, PlannedClassification::Live { .. }) {
                    anyhow::ensure!(
                        item.phase == ItemPhase::Killed,
                        "live record moved before a durable killed phase"
                    );
                }
                verify_retired_slot(&slot, &target.record)?;
                journal
                    .items
                    .get_mut(&target.runtime_id)
                    .context("retirement journal omitted target state")?
                    .phase = ItemPhase::RecordRetired;
                changed = true;
            }
            (true, true) => {
                return Err(tagged(
                    RetirementErrorCode::Conflict,
                    format!(
                        "source and private slot both exist for {:?}",
                        target.runtime_id
                    ),
                ));
            }
            (false, false) => {
                return Err(tagged(
                    RetirementErrorCode::Conflict,
                    format!(
                        "source and private slot are both absent for {:?}",
                        target.runtime_id
                    ),
                ));
            }
        }
    }
    if changed {
        write_journal(transaction, journal)?;
    }
    Ok(())
}

fn apply_target(
    locked: &StateLock,
    transaction: &Path,
    plan: &RetirementPlan,
    target: &PlannedTarget,
    phase: ItemPhase,
    journal: &mut RetirementJournal,
) -> Result<RetiredTarget> {
    let slot_relative = format!("{SLOT_DIR}/{}.pid", target.runtime_id);
    let slot = transaction.join(&slot_relative);
    let source = locked.capability_path().join(&target.record.relative_path);
    let source_exists = path_exists_nofollow(&source)?;
    let slot_exists = path_exists_nofollow(&slot)?;
    let mut membership = journal
        .items
        .get(&target.runtime_id)
        .context("retirement journal omitted target state")?
        .membership
        .clone();
    if phase == ItemPhase::RecordRetired || (!source_exists && slot_exists) {
        let after = verify_retired_slot(&slot, &target.record)?;
        journal
            .items
            .get_mut(&target.runtime_id)
            .context("retirement journal omitted target state")?
            .phase = ItemPhase::RecordRetired;
        write_journal(transaction, journal)?;
        let item = journal
            .items
            .get(&target.runtime_id)
            .context("retirement journal omitted target state")?;
        return Ok(retired_target(target, item, after, slot_relative));
    }
    anyhow::ensure!(
        source_exists && !slot_exists,
        "source/slot state is not a lossless retirement state for {:?}",
        target.runtime_id
    );
    verify_remaining_namespace(locked, plan, journal)?;
    // Bind and retain the exact record before the first cgroup control write. A record race can
    // never be discovered only after its process boundary has already been mutated.
    let record = open_record(locked, &target.runtime_id)?;
    anyhow::ensure!(
        record.evidence == target.record,
        "exec generation record changed before retirement"
    );

    match &target.classification {
        PlannedClassification::Live {
            pid,
            start_time_ticks,
            scope_unit,
            cgroup_path,
            cgroup_device,
            cgroup_inode,
            ..
        } if matches!(
            phase,
            ItemPhase::Prepared | ItemPhase::MutationAuthorized | ItemPhase::Frozen
        ) =>
        {
            #[cfg(not(target_os = "linux"))]
            return Err(tagged(
                RetirementErrorCode::Unsupported,
                "live exec retirement requires Linux pidfd and cgroup v2",
            ));
            #[cfg(target_os = "linux")]
            {
                match pidfd_open(*pid) {
                    Ok(leader) => {
                        verify_systemd_scope(scope_unit, cgroup_path)?;
                        anyhow::ensure!(
                            process_start_time_ticks(*pid)? == *start_time_ticks,
                            "planned exec leader generation changed"
                        );
                        let mut cgroup =
                            CgroupHandle::open(cgroup_path, *cgroup_device, *cgroup_inode)?;
                        test_checkpoint("before-first-runtime-mutation");
                        verify_open_record_binding(locked, &record, &target.record)?;
                        verify_systemd_scope(scope_unit, cgroup_path)?;
                        anyhow::ensure!(
                            process_start_time_ticks(*pid)? == *start_time_ticks
                                && process_cgroup_path(*pid)? == *cgroup_path
                                && cgroup.contains_pid(*pid)?,
                            "planned exec authority changed at the mutation boundary"
                        );
                        enter_forward_only(transaction, journal)?;
                        journal
                            .items
                            .get_mut(&target.runtime_id)
                            .context("retirement journal omitted target state")?
                            .phase = ItemPhase::MutationAuthorized;
                        write_journal(transaction, journal).map_err(|error| {
                            recoverable(error, "persist exact mutation authorization")
                        })?;
                        test_checkpoint("after-mutation-authorized-journal");
                        cgroup
                            .freeze()
                            .map_err(|error| recoverable(error, "freeze exact exec cgroup"))?;
                        test_checkpoint("after-cgroup-freeze");
                        let item = journal
                            .items
                            .get_mut(&target.runtime_id)
                            .context("retirement journal omitted target state")?;
                        item.phase = ItemPhase::Frozen;
                        item.freeze_observed = true;
                        write_journal(transaction, journal).map_err(|error| {
                            recoverable(error, "persist frozen retirement phase")
                        })?;
                        let pinned = cgroup.revalidate_members(*pid, *start_time_ticks, leader)?;
                        membership = pinned.evidence.clone();
                        journal
                            .items
                            .get_mut(&target.runtime_id)
                            .context("retirement journal omitted target state")?
                            .membership = membership.clone();
                        write_journal(transaction, journal).map_err(|error| {
                            recoverable(error, "persist pinned cgroup membership")
                        })?;
                        test_checkpoint("after-membership-journal");
                        cgroup.kill_all(&pinned).map_err(|error| {
                            recoverable(format!("{error:#}"), "kill exact exec cgroup")
                        })?;
                        test_checkpoint("after-cgroup-kill");
                        journal
                            .items
                            .get_mut(&target.runtime_id)
                            .context("retirement journal omitted target state")?
                            .cgroup_outcome = Some(CgroupRetirementOutcome::KillApplied);
                    }
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::ESRCH) | Some(libc::ENOENT)
                        ) && leaderless_recovery_phase(phase) =>
                    {
                        match CgroupHandle::open(cgroup_path, *cgroup_device, *cgroup_inode) {
                            Ok(mut cgroup) => {
                                verify_systemd_scope(scope_unit, cgroup_path)?;
                                if !cgroup.is_empty()? {
                                    let frozen = read_event(&cgroup.events_file, "frozen")?;
                                    match leaderless_freeze_action(phase, frozen.as_deref())? {
                                        LeaderlessFreezeAction::Freeze => {
                                            cgroup.freeze().map_err(|error| {
                                            recoverable(
                                                error,
                                                "freeze authorized leaderless exact exec cgroup",
                                            )
                                        })?;
                                            let item =
                                                journal.items.get_mut(&target.runtime_id).context(
                                                    "retirement journal omitted target state",
                                                )?;
                                            item.phase = ItemPhase::Frozen;
                                            item.freeze_observed = true;
                                            write_journal(transaction, journal).map_err(
                                                |error| {
                                                    recoverable(
                                                        error,
                                                        "persist leaderless frozen retirement \
                                                         phase",
                                                    )
                                                },
                                            )?;
                                        }
                                        LeaderlessFreezeAction::AlreadyFrozen => {
                                            let item =
                                                journal.items.get_mut(&target.runtime_id).context(
                                                    "retirement journal omitted target state",
                                                )?;
                                            if phase == ItemPhase::MutationAuthorized {
                                                item.phase = ItemPhase::Frozen;
                                                item.freeze_observed = true;
                                                write_journal(transaction, journal).map_err(
                                                    |error| {
                                                        recoverable(
                                                            error,
                                                            "persist observed leaderless frozen \
                                                             retirement phase",
                                                        )
                                                    },
                                                )?;
                                            } else {
                                                anyhow::ensure!(
                                                    item.freeze_observed,
                                                    "durable Frozen phase omitted freeze \
                                                     observation"
                                                );
                                            }
                                        }
                                    }
                                    let pinned = cgroup.revalidate_all_members()?;
                                    if membership.is_empty() {
                                        membership = pinned.evidence.clone();
                                        journal
                                            .items
                                            .get_mut(&target.runtime_id)
                                            .context("retirement journal omitted target state")?
                                            .membership = membership.clone();
                                        write_journal(transaction, journal).map_err(|error| {
                                            recoverable(
                                                error,
                                                "persist leaderless frozen membership",
                                            )
                                        })?;
                                    } else {
                                        anyhow::ensure!(
                                            pinned
                                                .evidence
                                                .iter()
                                                .all(|member| membership.contains(member)),
                                            "leaderless frozen membership added or changed a \
                                             generation outside durable evidence"
                                        );
                                    }
                                    cgroup.kill_all(&pinned).map_err(|error| {
                                        recoverable(error, "kill leaderless exact exec cgroup")
                                    })?;
                                    journal
                                        .items
                                        .get_mut(&target.runtime_id)
                                        .context("retirement journal omitted target state")?
                                        .cgroup_outcome =
                                        Some(CgroupRetirementOutcome::KillApplied);
                                } else {
                                    journal
                                        .items
                                        .get_mut(&target.runtime_id)
                                        .context("retirement journal omitted target state")?
                                        .cgroup_outcome =
                                        Some(CgroupRetirementOutcome::AlreadyEmpty);
                                }
                            }
                            Err(open_error) if anyhow_is_not_found(&open_error) => {
                                anyhow::ensure!(
                                    systemd_control_group(scope_unit)?.is_none(),
                                    "planned cgroup disappeared while systemd still reports scope \
                                    {scope_unit}"
                                );
                                journal
                                    .items
                                    .get_mut(&target.runtime_id)
                                    .context("retirement journal omitted target state")?
                                    .cgroup_outcome = Some(CgroupRetirementOutcome::ScopeCollected);
                            }
                            Err(open_error) => return Err(open_error),
                        }
                    }
                    Err(error) => {
                        return Err(error).context("pin planned exec leader with pidfd");
                    }
                }
                journal
                    .items
                    .get_mut(&target.runtime_id)
                    .context("retirement journal omitted target state")?
                    .phase = ItemPhase::Killed;
                write_journal(transaction, journal)
                    .map_err(|error| recoverable(error, "persist killed retirement phase"))?;
            }
        }
        PlannedClassification::Live {
            pid,
            start_time_ticks,
            scope_unit,
            cgroup_path,
            cgroup_device,
            cgroup_inode,
            ..
        } => {
            // Recovery after the kill boundary: the exact leader must be gone and the original
            // scope must either be absent or still be the same empty inode.
            anyhow::ensure!(
                generation_is_absent(*pid, *start_time_ticks),
                "planned exec generation remains live after durable killed phase"
            );
            verify_scope_empty_or_collected(
                scope_unit,
                cgroup_path,
                *cgroup_device,
                *cgroup_inode,
            )?;
            anyhow::ensure!(
                journal
                    .items
                    .get(&target.runtime_id)
                    .and_then(|item| item.cgroup_outcome)
                    .is_some(),
                "durable killed phase omitted its cgroup retirement outcome"
            );
        }
    }

    enter_forward_only(transaction, journal)?;
    lossless_move_record(locked, &record.file, &record.evidence, &slot)?;
    test_checkpoint("after-record-rename");
    journal
        .items
        .get_mut(&target.runtime_id)
        .context("retirement journal omitted target state")?
        .phase = ItemPhase::RecordRetired;
    write_journal(transaction, journal)?;
    let after = verify_retired_slot(&slot, &target.record)?;
    let item = journal
        .items
        .get(&target.runtime_id)
        .context("retirement journal omitted target state")?;
    Ok(retired_target(target, item, after, slot_relative))
}

fn leaderless_recovery_phase(phase: ItemPhase) -> bool {
    matches!(phase, ItemPhase::MutationAuthorized | ItemPhase::Frozen)
}

fn leaderless_freeze_action(
    phase: ItemPhase,
    frozen: Option<&str>,
) -> Result<LeaderlessFreezeAction> {
    match (phase, frozen) {
        (ItemPhase::MutationAuthorized, Some("0")) => Ok(LeaderlessFreezeAction::Freeze),
        (ItemPhase::MutationAuthorized | ItemPhase::Frozen, Some("1")) => {
            Ok(LeaderlessFreezeAction::AlreadyFrozen)
        }
        _ => anyhow::bail!("leaderless recovery found an invalid exact-scope freeze state"),
    }
}

fn retired_target(
    target: &PlannedTarget,
    item: &JournalItem,
    after: RecordEvidence,
    slot_relative: String,
) -> RetiredTarget {
    let before = public_record(&target.record);
    let after = RetiredRecordEvidence {
        relative_path: slot_relative,
        device: after.device,
        inode: after.inode,
        length: after.length,
        modified_unix_ns: after.modified_unix_ns,
        sha256: after.sha256,
    };
    match &target.classification {
        PlannedClassification::Live {
            pid,
            start_time_ticks,
            scope_unit,
            cgroup_path,
            cgroup_device,
            cgroup_inode,
            ..
        } => RetiredTarget {
            runtime_id: target.runtime_id.clone(),
            generation_id: target.generation_id.clone(),
            authority_kind: target.authority_kind,
            disposition: RetiredDisposition::CgroupRetired,
            pid: *pid,
            start_time_ticks: Some(*start_time_ticks),
            scope_unit: Some(scope_unit.clone()),
            cgroup_path: Some(cgroup_path.clone()),
            cgroup_device: Some(*cgroup_device),
            cgroup_inode: Some(*cgroup_inode),
            legacy_scope: None,
            membership: item.membership.clone(),
            freeze_observed: item.freeze_observed,
            cgroup_outcome: item.cgroup_outcome,
            durable_phase: "record-retired".to_string(),
            record_before: before,
            record_after: after,
        },
    }
}

fn public_record(record: &RecordEvidence) -> RetiredRecordEvidence {
    RetiredRecordEvidence {
        relative_path: record.relative_path.clone(),
        device: record.device,
        inode: record.inode,
        length: record.length,
        modified_unix_ns: record.modified_unix_ns,
        sha256: record.sha256.clone(),
    }
}

struct OpenRecord {
    file: File,
    evidence: RecordEvidence,
}

fn open_record(locked: &StateLock, runtime_id: &str) -> Result<OpenRecord> {
    anyhow::ensure!(
        safe_runtime_id(runtime_id),
        "unsafe runtime id {runtime_id:?}"
    );
    let name = format!("{runtime_id}.pid");
    let file = openat_regular_nofollow(&locked.state_dir, OsStr::new(&name))
        .with_context(|| format!("open exact exec record {name}"))?;
    let metadata = file.metadata()?;
    let bytes = read_exact_record(&file)?;
    Ok(OpenRecord {
        file,
        evidence: RecordEvidence {
            relative_path: name,
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_unix_ns: metadata_mtime_ns(&metadata),
            sha256: record_sha256(&bytes),
        },
    })
}

fn verify_open_record_binding(
    locked: &StateLock,
    held: &OpenRecord,
    expected: &RecordEvidence,
) -> Result<()> {
    let current = stat_path_nofollow(&locked.capability_path().join(&expected.relative_path))?;
    let held_metadata = held.file.metadata()?;
    anyhow::ensure!(
        held.evidence == *expected
            && current == *expected
            && held_metadata.dev() == expected.device
            && held_metadata.ino() == expected.inode,
        "exact exec record changed at the retirement mutation boundary"
    );
    Ok(())
}

fn classify_strict_generation(
    generation: &crate::exec_backend::ExecGeneration,
) -> Result<PlannedClassification> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = generation;
        return Err(tagged(
            RetirementErrorCode::Unsupported,
            "strict exec retirement requires Linux",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        let pid = generation.pid as i32;
        let isolation = generation
            .isolation
            .as_ref()
            .context("strict v2 generation omitted its isolation capability")?;
        match pidfd_open(pid) {
            Ok(_leader) => {
                if process_start_time_ticks(pid).ok() != Some(generation.start_time_ticks) {
                    anyhow::bail!(
                        "strict v2 generation is no longer live at its recorded process generation"
                    );
                }
                anyhow::ensure!(
                    process_cgroup_path(pid)? == isolation.cgroup_path,
                    "strict generation leader left its recorded cgroup"
                );
                verify_systemd_scope(&isolation.unit, &isolation.cgroup_path)?;
                let cgroup = CgroupHandle::open(
                    &isolation.cgroup_path,
                    isolation.cgroup_device,
                    isolation.cgroup_inode,
                )?;
                anyhow::ensure!(
                    cgroup.contains_pid(pid)?,
                    "strict generation leader is absent from its recorded cgroup"
                );
                Ok(PlannedClassification::Live {
                    pid,
                    start_time_ticks: generation.start_time_ticks,
                    scope_unit: isolation.unit.clone(),
                    cgroup_path: isolation.cgroup_path.clone(),
                    cgroup_device: isolation.cgroup_device,
                    cgroup_inode: isolation.cgroup_inode,
                })
            }
            Err(error)
                if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) =>
            {
                Err(anyhow::anyhow!(
                    "strict v2 generation leader is absent; general retirement cannot perform \
                     record-only retirement"
                ))
            }
            Err(error) => Err(error).context("open pidfd for strict exec generation"),
        }
    }
}

struct CgroupHandle {
    dir: File,
    freeze_file: File,
    kill_file: File,
    events_file: File,
    procs_file: File,
    path: String,
}

struct PinnedMembership {
    evidence: Vec<MemberEvidence>,
    _pidfds: Vec<File>,
    _leader_pidfd: Option<File>,
}

impl CgroupHandle {
    #[cfg(target_os = "linux")]
    fn open(path: &str, device: u64, inode: u64) -> Result<Self> {
        Self::open_inner(path, Some((device, inode)))
    }

    #[cfg(not(target_os = "linux"))]
    fn open(_path: &str, _device: u64, _inode: u64) -> Result<Self> {
        Err(tagged(
            RetirementErrorCode::Unsupported,
            "cgroup-v2 exec retirement requires Linux",
        ))
    }

    #[cfg(target_os = "linux")]
    fn open_inner(path: &str, expected: Option<(u64, u64)>) -> Result<Self> {
        anyhow::ensure!(path.starts_with('/'), "cgroup path is not absolute");
        let mut components = Path::new(path).components();
        anyhow::ensure!(
            matches!(components.next(), Some(Component::RootDir))
                && components.all(|component| matches!(component, Component::Normal(_))),
            "cgroup path contains a non-root/non-normal component"
        );
        let root = open_dir_nofollow(Path::new(CGROUP_ROOT))?;
        ensure_cgroup2(&root)?;
        let relative = path
            .strip_prefix('/')
            .context("cgroup path omitted root prefix")?;
        let dir = openat2_beneath(&root, OsStr::new(relative))
            .with_context(|| format!("securely open exact cgroup {path:?}"))?;
        let metadata = dir.metadata()?;
        if let Some((device, inode)) = expected {
            anyhow::ensure!(
                metadata.dev() == device && metadata.ino() == inode,
                "cgroup inode changed since preparation"
            );
        }
        let leaf = Path::new(path)
            .file_name()
            .and_then(OsStr::to_str)
            .context("cgroup scope has no UTF-8 leaf name")?;
        anyhow::ensure!(
            leaf.starts_with("st2-") && leaf.ends_with(".scope"),
            "cgroup is not a dedicated st2 scope: {leaf:?}"
        );
        ensure_leaf_cgroup(&dir)?;
        Ok(Self {
            freeze_file: openat_file(&dir, OsStr::new("cgroup.freeze"), libc::O_RDWR)?,
            kill_file: openat_file(&dir, OsStr::new("cgroup.kill"), libc::O_WRONLY)?,
            events_file: openat_file(&dir, OsStr::new("cgroup.events"), libc::O_RDONLY)?,
            procs_file: openat_file(&dir, OsStr::new("cgroup.procs"), libc::O_RDONLY)?,
            dir,
            path: path.to_string(),
        })
    }

    fn contains_pid(&self, pid: i32) -> Result<bool> {
        Ok(read_pids(&self.procs_file)?.contains(&pid))
    }

    fn freeze(&mut self) -> Result<()> {
        write_control(&mut self.freeze_file, b"1")?;
        wait_events(&self.events_file, "frozen", "1")
            .context("wait for exact exec cgroup to freeze")
    }

    #[cfg(target_os = "linux")]
    fn revalidate_members(
        &mut self,
        leader_pid: i32,
        leader_start: u64,
        leader_pidfd: File,
    ) -> Result<PinnedMembership> {
        ensure_pidfd_live(&leader_pidfd)?;
        anyhow::ensure!(
            process_start_time_ticks(leader_pid)? == leader_start,
            "leader generation changed after cgroup freeze"
        );
        anyhow::ensure!(
            process_cgroup_path(leader_pid)? == self.path,
            "leader left the planned cgroup"
        );
        ensure_leaf_cgroup(&self.dir)?;
        let first = read_pids(&self.procs_file)?;
        anyhow::ensure!(
            first.contains(&leader_pid),
            "frozen cgroup omitted its leader"
        );
        let mut guards = Vec::new();
        let mut evidence = Vec::new();
        for pid in &first {
            let pidfd = pidfd_open(*pid)
                .with_context(|| format!("pin frozen cgroup member {pid} with pidfd"))?;
            let start_time_ticks = process_start_time_ticks(*pid)?;
            anyhow::ensure!(
                process_cgroup_path(*pid)? == self.path,
                "frozen member {pid} does not resolve to the planned cgroup"
            );
            evidence.push(MemberEvidence {
                pid: *pid,
                start_time_ticks,
            });
            guards.push(pidfd);
        }
        let second = read_pids(&self.procs_file)?;
        anyhow::ensure!(
            first == second,
            "cgroup membership changed while frozen and pinned"
        );
        // The returned capability owns every pidfd; the caller must retain it through cgroup.kill.
        for guard in &guards {
            ensure_pidfd_live(guard)?;
        }
        evidence.sort_by_key(|member| member.pid);
        Ok(PinnedMembership {
            evidence,
            _pidfds: guards,
            _leader_pidfd: Some(leader_pidfd),
        })
    }

    #[cfg(target_os = "linux")]
    fn revalidate_all_members(&mut self) -> Result<PinnedMembership> {
        ensure_leaf_cgroup(&self.dir)?;
        let first = read_pids(&self.procs_file)?;
        anyhow::ensure!(!first.is_empty(), "frozen scope has no members to recover");
        let mut guards = Vec::new();
        let mut evidence = Vec::new();
        for pid in &first {
            let pidfd = pidfd_open(*pid)
                .with_context(|| format!("pin leaderless frozen cgroup member {pid}"))?;
            let start_time_ticks = process_start_time_ticks(*pid)?;
            anyhow::ensure!(
                process_cgroup_path(*pid)? == self.path,
                "leaderless frozen member {pid} left the exact cgroup"
            );
            ensure_pidfd_live(&pidfd)?;
            evidence.push(MemberEvidence {
                pid: *pid,
                start_time_ticks,
            });
            guards.push(pidfd);
        }
        anyhow::ensure!(
            first == read_pids(&self.procs_file)?,
            "leaderless frozen cgroup membership changed while pinned"
        );
        evidence.sort_by_key(|member| member.pid);
        Ok(PinnedMembership {
            evidence,
            _pidfds: guards,
            _leader_pidfd: None,
        })
    }

    #[cfg(target_os = "linux")]
    fn kill_all(&mut self, pinned: &PinnedMembership) -> Result<()> {
        // Re-read the leaf membership at the last possible point. Frozen tasks cannot fork, and
        // cgroup.kill is the kernel's atomic descendant kill primitive.
        let current = read_pids(&self.procs_file)?;
        let expected = pinned
            .evidence
            .iter()
            .map(|member| member.pid)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            current == expected,
            "frozen cgroup membership changed before cgroup.kill"
        );
        write_control(&mut self.kill_file, b"1")?;
        match wait_events(&self.events_file, "populated", "0") {
            Ok(()) => Ok(()),
            // A transient `--collect` scope can disappear immediately after its final task dies.
            // The kernel cannot remove a populated cgroup, so a dead retained control FD is an
            // equally strong empty proof after the exact cgroup.kill write succeeded.
            Err(error)
                if anyhow_has_errno(&error, libc::ENODEV)
                    || anyhow_has_errno(&error, libc::ENOENT) =>
            {
                Ok(())
            }
            Err(error) => Err(error).context("prove exact exec cgroup empty after cgroup.kill"),
        }
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(read_event(&self.events_file, "populated")?.as_deref() == Some("0"))
    }
}

#[cfg(target_os = "linux")]
fn openat2_beneath(parent: &File, path: &OsStr) -> Result<File> {
    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    const RESOLVE_NO_XDEV: u64 = 0x01;
    const RESOLVE_NO_SYMLINKS: u64 = 0x04;
    const RESOLVE_BENEATH: u64 = 0x08;

    let path = CString::new(path.as_bytes()).context("cgroup path contains NUL")?;
    let how = OpenHow {
        flags: (libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_NO_XDEV | RESOLVE_NO_SYMLINKS | RESOLVE_BENEATH,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            path.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        ) as libc::c_int
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("openat2 exact cgroup");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn lossless_move_record(
    locked: &StateLock,
    held: &File,
    expected: &RecordEvidence,
    slot: &Path,
) -> Result<()> {
    let source = locked.capability_path().join(&expected.relative_path);
    let current = stat_path_nofollow(&source)?;
    anyhow::ensure!(
        current.device == expected.device
            && current.inode == expected.inode
            && current.sha256 == expected.sha256,
        "source record changed before lossless retirement move"
    );
    let slot_parent = slot.parent().context("retirement slot has no parent")?;
    ensure_real_dir(slot_parent)?;
    let slot_parent_fd = open_dir_nofollow(slot_parent)?;
    let slot_name = slot
        .file_name()
        .context("retirement slot has no filename")?;
    renameat_noreplace(
        &locked.state_dir,
        OsStr::new(&expected.relative_path),
        &slot_parent_fd,
        slot_name,
    )
    .with_context(|| format!("move exact record into private slot {}", slot.display()))?;
    locked.state_dir.sync_all()?;
    sync_dir(slot_parent)?;

    let moved = stat_path_nofollow(slot);
    let valid = moved.as_ref().is_ok_and(|moved| {
        moved.device == expected.device
            && moved.inode == expected.inode
            && moved.length == expected.length
            && moved.modified_unix_ns == expected.modified_unix_ns
            && moved.sha256 == expected.sha256
    }) && held.metadata().is_ok_and(|metadata| {
        metadata.dev() == expected.device && metadata.ino() == expected.inode
    });
    if valid {
        return Ok(());
    }

    // Preserve the raced object: rollback is itself no-replace. If a successor already occupies the
    // source name, the moved object remains in the private slot and both objects survive.
    match renameat_noreplace(
        &slot_parent_fd,
        slot_name,
        &locked.state_dir,
        OsStr::new(&expected.relative_path),
    ) {
        Ok(()) => {
            let _ = locked.state_dir.sync_all();
            let _ = sync_dir(slot_parent);
            anyhow::bail!("retired record inode/bytes mismatch; raced object restored")
        }
        Err(rollback) => anyhow::bail!(
            "retired record inode/bytes mismatch and no-replace rollback conflicted; \
             both names preserved: {rollback}"
        ),
    }
}

fn verify_retired_slot(path: &Path, expected: &RecordEvidence) -> Result<RecordEvidence> {
    let evidence = stat_path_nofollow(path)?;
    anyhow::ensure!(
        evidence.device == expected.device
            && evidence.inode == expected.inode
            && evidence.length == expected.length
            && evidence.modified_unix_ns == expected.modified_unix_ns
            && evidence.sha256 == expected.sha256,
        "private retirement slot does not contain the planned record"
    );
    Ok(evidence)
}

fn census_state_namespace(locked: &StateLock) -> Result<Vec<CensusEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(locked.capability_path())
        .with_context(|| format!("census exec state namespace {}", locked.path.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .context("exec state namespace contains a non-UTF-8 name")?;
        if matches!(name, LOCK_FILE | CONTROL_DIR) {
            continue;
        }
        let file = openat_regular_nofollow(&locked.state_dir, OsStr::new(name))?;
        let metadata = file.metadata()?;
        anyhow::ensure!(
            metadata.is_file(),
            "exec state namespace contains unsupported entry {name:?}"
        );
        let bytes = read_bounded(&file, 1024 * 1024)?;
        entries.push(CensusEntry {
            name: name.to_string(),
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            sha256: sha256(&bytes),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

fn verify_remaining_namespace(
    locked: &StateLock,
    plan: &RetirementPlan,
    journal: &RetirementJournal,
) -> Result<()> {
    let retired = journal
        .items
        .iter()
        .filter(|(_, item)| item.phase == ItemPhase::RecordRetired)
        .map(|(runtime_id, _)| format!("{runtime_id}.pid"))
        .collect::<BTreeSet<_>>();
    let expected = plan
        .census
        .iter()
        .filter(|entry| !retired.contains(&entry.name))
        .cloned()
        .collect::<Vec<_>>();
    let current = census_state_namespace(locked)?;
    anyhow::ensure!(
        current == expected,
        "exec state namespace differs from the exact remaining prepared census"
    );
    Ok(())
}

fn path_exists_nofollow(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "retirement path is not a real regular file: {}",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_plan(locked: &StateLock, plan: &RetirementPlan, plan_sha256: &str) -> Result<()> {
    anyhow::ensure!(
        plan.schema == PLAN_SCHEMA,
        "unsupported retirement plan schema"
    );
    anyhow::ensure!(
        !plan.targets.is_empty(),
        "retirement plan contains no targets"
    );
    let metadata = locked.state_dir.metadata()?;
    anyhow::ensure!(
        metadata.dev() == plan.state_dir_device && metadata.ino() == plan.state_dir_inode,
        "exec state directory identity changed since preparation"
    );
    let encoded = canonical_json(plan)?;
    anyhow::ensure!(
        sha256(&encoded) == plan_sha256,
        "decoded retirement plan does not re-encode to its caller-held digest"
    );
    anyhow::ensure!(
        hash_census(&plan.census) == plan.census_sha256,
        "retirement plan census digest is invalid"
    );
    let mut ids = BTreeSet::new();
    for target in &plan.targets {
        anyhow::ensure!(
            safe_runtime_id(&target.runtime_id) && ids.insert(&target.runtime_id),
            "retirement plan has an unsafe or duplicate runtime id"
        );
        anyhow::ensure!(
            target.record.relative_path == format!("{}.pid", target.runtime_id),
            "retirement record path does not match runtime id"
        );
        let SelectionWire::Id { runtime_id } = &plan.selection;
        anyhow::ensure!(
            matches!(target.classification, PlannedClassification::Live { .. })
                && runtime_id == &target.runtime_id
                && target.generation_id.is_some()
                && target.authority_kind == RetirementAuthorityKind::StrictGenerationV2,
            "general retirement plan does not bind one live strict-v2 generation"
        );
    }
    anyhow::ensure!(
        plan.legacy_partition.is_none(),
        "successor retirement plans cannot carry predecessor partition authority"
    );
    Ok(())
}

fn validate_journal(
    plan: &RetirementPlan,
    plan_sha256: &str,
    journal: &RetirementJournal,
) -> Result<()> {
    anyhow::ensure!(
        journal.schema == JOURNAL_SCHEMA
            && journal.request_sha256 == plan.request_sha256
            && journal.plan_sha256 == plan_sha256,
        "retirement journal does not bind the selected plan"
    );
    let expected = plan
        .targets
        .iter()
        .map(|target| target.runtime_id.as_str())
        .collect::<BTreeSet<_>>();
    let actual = journal
        .items
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(actual == expected, "retirement journal target set changed");
    for target in &plan.targets {
        let item = journal
            .items
            .get(&target.runtime_id)
            .context("retirement journal omitted target state")?;
        if matches!(
            item.phase,
            ItemPhase::Frozen | ItemPhase::Killed | ItemPhase::RecordRetired
        ) {
            anyhow::ensure!(
                item.freeze_observed,
                "live retirement phase omitted its freeze observation"
            );
        }
        if matches!(item.phase, ItemPhase::Killed | ItemPhase::RecordRetired) {
            anyhow::ensure!(
                item.cgroup_outcome.is_some(),
                "live killed phase omitted its cgroup outcome"
            );
        }
    }
    let advanced = journal
        .items
        .values()
        .any(|item| item.phase != ItemPhase::Prepared);
    anyhow::ensure!(
        !advanced || journal.forward_only_started,
        "retirement journal advanced without its forward-only boundary"
    );
    anyhow::ensure!(
        journal.status != JournalStatus::Completed || journal.forward_only_started,
        "completed retirement journal omitted its forward-only boundary"
    );
    Ok(())
}

fn hash_request(
    selection: &SelectionWire,
    census_sha256: &str,
    legacy_partition: Option<&[LegacySuccessorTask]>,
    targets: &[PlannedTarget],
) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(REQUEST_HASH_DOMAIN);
    hash.update(canonical_json(selection)?);
    hash.update(census_sha256.as_bytes());
    hash.update(canonical_json(&legacy_partition)?);
    hash.update(canonical_json(targets)?);
    Ok(format!("{:x}", hash.finalize()))
}

fn hash_legacy_partition(legacy_partition: Option<&[LegacySuccessorTask]>) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(LEGACY_PARTITION_HASH_DOMAIN);
    hash.update(canonical_json(&legacy_partition)?);
    Ok(format!("{:x}", hash.finalize()))
}

fn hash_census(census: &[CensusEntry]) -> String {
    let mut hash = Sha256::new();
    hash.update(CENSUS_HASH_DOMAIN);
    for entry in census {
        hash.update((entry.name.len() as u64).to_be_bytes());
        hash.update(entry.name.as_bytes());
        hash.update(entry.device.to_be_bytes());
        hash.update(entry.inode.to_be_bytes());
        hash.update(entry.length.to_be_bytes());
        hash.update(entry.sha256.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn record_sha256(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(RECORD_HASH_DOMAIN);
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> i128 {
    i128::from(metadata.mtime())
        .saturating_mul(1_000_000_000)
        .saturating_add(i128::from(metadata.mtime_nsec()))
}

fn canonical_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn safe_runtime_id(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'.' | b'-'))
}

fn validate_host_component(host: &str) -> Result<()> {
    anyhow::ensure!(
        !host.is_empty()
            && host != "."
            && host != ".."
            && !host.starts_with('.')
            && host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')),
        "host must be one safe path component"
    );
    Ok(())
}

fn transaction_dir(state_dir: &Path, request_sha256: &str) -> PathBuf {
    state_dir.join(CONTROL_DIR).join(request_sha256)
}

fn absolute_output(output: &Path) -> Result<PathBuf> {
    let absolute = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let parent = absolute
        .parent()
        .context("retirement plan output has no parent")?
        .canonicalize()
        .with_context(|| {
            format!(
                "canonicalize retirement plan output parent {}",
                absolute.display()
            )
        })?;
    Ok(parent.join(
        absolute
            .file_name()
            .context("retirement plan output has no filename")?,
    ))
}

fn publish_or_validate_plan_output(path: &Path, bytes: &[u8], digest: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "retirement plan output is not a real regular file"
            );
            let existing = read_regular_nofollow(path)?;
            anyhow::ensure!(
                existing == bytes && sha256(&existing) == digest,
                "retirement plan output already exists with different bytes"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            publish_create_only_bytes(path, bytes)?;
        }
        Err(error) => return Err(error).context("inspect retirement plan output"),
    }
    Ok(())
}

fn create_or_validate_transaction(
    transaction: &Path,
    plan_bytes: &[u8],
    plan_sha256: &str,
) -> Result<()> {
    match fs::create_dir(transaction) {
        Ok(()) => {
            fs::create_dir(transaction.join(SLOT_DIR))?;
            publish_create_only_bytes(&transaction.join(PLAN_FILE), plan_bytes)?;
            sync_dir(transaction)?;
            sync_dir(transaction.parent().context("transaction has no parent")?)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure_real_dir(transaction)?;
            ensure_real_dir(&transaction.join(SLOT_DIR))?;
            let existing = read_regular_nofollow(&transaction.join(PLAN_FILE))?;
            anyhow::ensure!(
                sha256(&existing) == plan_sha256 && existing == plan_bytes,
                "existing transaction does not contain the exact prepared plan"
            );
        }
        Err(error) => return Err(error).context("create exec retirement transaction"),
    }
    Ok(())
}

fn create_or_validate_journal(transaction: &Path, journal: &RetirementJournal) -> Result<()> {
    let path = transaction.join(JOURNAL_FILE);
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            let existing: RetirementJournal = read_json(&path)?;
            anyhow::ensure!(
                existing.request_sha256 == journal.request_sha256
                    && existing.plan_sha256 == journal.plan_sha256,
                "existing retirement journal belongs to a different request"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            publish_create_only_json(&path, journal)?;
        }
        Err(error) => return Err(error).context("inspect retirement journal"),
    }
    Ok(())
}

fn find_transaction_by_plan(state_dir: &Path, plan_sha256: &str) -> Result<PathBuf> {
    let control = state_dir.join(CONTROL_DIR);
    let mut matches = Vec::new();
    for entry in fs::read_dir(&control)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "retirement control namespace contains a non-directory entry"
        );
        let plan = entry.path().join(PLAN_FILE);
        if let Ok(bytes) = read_regular_nofollow(&plan)
            && sha256(&bytes) == plan_sha256
        {
            matches.push(entry.path());
        }
    }
    anyhow::ensure!(
        matches.len() == 1,
        "expected exactly one retirement transaction for plan {plan_sha256}, found {}",
        matches.len()
    );
    Ok(matches.remove(0))
}

fn write_journal(transaction: &Path, journal: &RetirementJournal) -> Result<()> {
    atomic_replace_json(&transaction.join(JOURNAL_FILE), journal)?;
    sync_dir(transaction)
}

fn ensure_control_dir(state_fd: &File) -> Result<File> {
    let name = CString::new(CONTROL_DIR).expect("static control directory has no NUL");
    let result = unsafe { libc::mkdirat(state_fd.as_raw_fd(), name.as_ptr(), 0o700) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error).context("create retirement control directory");
        }
    }
    let control = openat(
        state_fd,
        OsStr::new(CONTROL_DIR),
        libc::O_RDONLY | libc::O_DIRECTORY,
    )?;
    state_fd.sync_all()?;
    Ok(control)
}

fn ensure_real_dir(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "expected a real directory: {}",
        path.display()
    );
    Ok(())
}

fn publish_create_only_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    publish_create_only_bytes(path, &canonical_json(value)?)
}

fn publish_create_only_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("published file has no parent")?;
    let mut temp = tempfile::Builder::new()
        .prefix(".exec-retirement-publish.")
        .tempfile_in(parent)?;
    temp.as_file_mut().write_all(bytes)?;
    temp.as_file().sync_all()?;
    rename_noreplace(temp.path(), path)?;
    sync_dir(parent)
}

fn atomic_replace_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("journal has no parent")?;
    let temp = parent.join(format!(
        ".journal-{}-{}.tmp",
        std::process::id(),
        unix_ms()?
    ));
    publish_create_only_bytes(&temp, &canonical_json(value)?)?;
    fs::rename(&temp, path)?;
    sync_dir(parent)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(&read_regular_nofollow(path)?)
        .with_context(|| format!("decode {}", path.display()))
}

fn read_optional_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>> {
    match read_regular_nofollow(path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?,
        )),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn read_regular_nofollow(path: &Path) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "not a regular file: {}",
        path.display()
    );
    read_bounded(&file, 8 * 1024 * 1024)
}

fn read_exact_record(file: &File) -> Result<Vec<u8>> {
    read_bounded(file, 1024 * 1024)
}

fn read_bounded(file: &File, limit: u64) -> Result<Vec<u8>> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= limit,
        "file exceeds bounded read limit"
    );
    Ok(bytes)
}

fn stat_path_nofollow(path: &Path) -> Result<RecordEvidence> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(metadata.is_file(), "record is not a regular file");
    let bytes = read_exact_record(&file)?;
    Ok(RecordEvidence {
        relative_path: path
            .file_name()
            .and_then(OsStr::to_str)
            .context("record path has no UTF-8 leaf")?
            .to_string(),
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified_unix_ns: metadata_mtime_ns(&metadata),
        sha256: record_sha256(&bytes),
    })
}

fn open_dir_nofollow(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)?;
    Ok(file)
}

fn openat_regular_nofollow(parent: &File, name: &OsStr) -> Result<File> {
    let file = openat(parent, name, libc::O_RDONLY)?;
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "opened object is not a regular file"
    );
    Ok(file)
}

fn openat_file(parent: &File, name: &OsStr, flags: libc::c_int) -> Result<File> {
    openat(parent, name, flags)
}

fn openat_create_regular(
    parent: &File,
    name: &OsStr,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> Result<File> {
    let name = CString::new(name.as_bytes()).context("path component contains NUL")?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("openat create no-follow");
    }
    let file = unsafe { File::from_raw_fd(fd) };
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "opened object is not a regular file"
    );
    Ok(file)
}

fn openat(parent: &File, name: &OsStr, flags: libc::c_int) -> Result<File> {
    let name = CString::new(name.as_bytes()).context("path component contains NUL")?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("openat no-follow");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn ensure_cgroup2(file: &File) -> Result<()> {
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    let result = unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("fstatfs cgroup root");
    }
    let stat = unsafe { stat.assume_init() };
    anyhow::ensure!(
        stat.f_type as libc::c_long == CGROUP2_SUPER_MAGIC,
        "cgroup root is not cgroup v2"
    );
    Ok(())
}

fn ensure_leaf_cgroup(dir: &File) -> Result<()> {
    let proc_path = format!("/proc/self/fd/{}", dir.as_raw_fd());
    for entry in fs::read_dir(proc_path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        anyhow::ensure!(
            !metadata.is_dir(),
            "planned st2 scope has a child cgroup; leaf authority required"
        );
    }
    Ok(())
}

fn read_pids(file: &File) -> Result<Vec<i32>> {
    let raw = read_control(file)?;
    let mut pids = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .parse::<i32>()
                .context("parse cgroup member pid")
        })
        .collect::<Result<Vec<_>>>()?;
    pids.sort_unstable();
    pids.dedup();
    Ok(pids)
}

fn read_control(file: &File) -> Result<String> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut value = String::new();
    file.take(1024 * 1024).read_to_string(&mut value)?;
    Ok(value)
}

fn write_control(file: &mut File, value: &[u8]) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(value)?;
    Ok(())
}

fn read_event(file: &File, key: &str) -> Result<Option<String>> {
    Ok(read_control(file)?.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().to_string())
    }))
}

fn wait_events(file: &File, key: &str, expected: &str) -> Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        if read_event(file, key)?.as_deref() == Some(expected) {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for cgroup.events {key}={expected}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(target_os = "linux")]
fn pidfd_open(pid: i32) -> std::io::Result<File> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as libc::c_int };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(not(target_os = "linux"))]
fn pidfd_open(_pid: i32) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "pidfd requires Linux",
    ))
}

#[cfg(target_os = "linux")]
fn ensure_pidfd_live(pidfd: &File) -> Result<()> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            0,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("revalidate live pidfd");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn process_start_time_ticks(pid: i32) -> Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let after_comm = stat.rsplit_once(") ").context("malformed proc stat")?.1;
    after_comm
        .split_whitespace()
        .nth(19)
        .context("proc stat omitted starttime")?
        .parse()
        .context("parse process starttime")
}

#[cfg(target_os = "linux")]
fn process_cgroup_path(pid: i32) -> Result<String> {
    let raw = fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    let mut matches = raw
        .lines()
        .filter_map(|line| line.strip_prefix("0::"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matches.len() == 1,
        "process does not have exactly one unified cgroup-v2 path"
    );
    Ok(matches.remove(0))
}

#[cfg(target_os = "linux")]
fn verify_systemd_scope(unit: &str, expected_cgroup: &str) -> Result<()> {
    let control_group = systemd_control_group(unit)?
        .with_context(|| format!("systemd does not acknowledge exact scope {unit}"))?;
    anyhow::ensure!(
        control_group == expected_cgroup,
        "systemd scope {unit} resolves to {control_group:?}, expected {expected_cgroup:?}"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn systemd_control_group(unit: &str) -> Result<Option<String>> {
    let output = Command::new("systemctl")
        .args(["--user", "show", "--property=ControlGroup", "--value", unit])
        .output()
        .with_context(|| format!("query systemd scope authority {unit}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    let control_group = String::from_utf8(output.stdout)
        .context("systemd ControlGroup is not UTF-8")?
        .trim()
        .to_string();
    Ok((!control_group.is_empty()).then_some(control_group))
}

#[cfg(target_os = "linux")]
fn verify_scope_empty_or_collected(
    unit: &str,
    cgroup_path: &str,
    device: u64,
    inode: u64,
) -> Result<()> {
    match CgroupHandle::open(cgroup_path, device, inode) {
        Ok(cgroup) => {
            verify_systemd_scope(unit, cgroup_path)?;
            anyhow::ensure!(cgroup.is_empty()?, "planned cgroup is not empty after kill");
        }
        Err(error) if anyhow_is_not_found(&error) => {
            anyhow::ensure!(
                systemd_control_group(unit)?.is_none(),
                "planned cgroup path disappeared while systemd still reports scope {unit}"
            );
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn anyhow_is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    })
}

fn anyhow_has_errno(error: &anyhow::Error, errno: i32) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(errno))
    })
}

fn generation_is_absent(pid: i32, start_time_ticks: u64) -> bool {
    #[cfg(target_os = "linux")]
    {
        pidfd_open(pid).is_err() || process_start_time_ticks(pid).ok() != Some(start_time_ticks)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, start_time_ticks);
        false
    }
}

fn rename_noreplace(source: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let source = CString::new(source.as_os_str().as_bytes()).unwrap();
        let target = CString::new(target.as_os_str().as_bytes()).unwrap();
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                target.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (source, target);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lossless record retirement requires renameat2(RENAME_NOREPLACE)",
        ))
    }
}

fn renameat_noreplace(
    source_dir: &File,
    source: &OsStr,
    target_dir: &File,
    target: &OsStr,
) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let source = CString::new(source.as_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let target = CString::new(target.as_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let result = unsafe {
            libc::renameat2(
                source_dir.as_raw_fd(),
                source.as_ptr(),
                target_dir.as_raw_fd(),
                target.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (source_dir, source, target_dir, target);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lossless record retirement requires renameat2(RENAME_NOREPLACE)",
        ))
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    open_dir_nofollow(path)?.sync_all()?;
    Ok(())
}

fn validate_sha256(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected lowercase SHA-256"
    );
    Ok(())
}

fn unix_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_record(path: &Path, bytes: &[u8]) -> (File, RecordEvidence) {
        fs::write(path, bytes).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let evidence = RecordEvidence {
            relative_path: path.file_name().unwrap().to_str().unwrap().to_string(),
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_unix_ns: metadata_mtime_ns(&metadata),
            sha256: record_sha256(bytes),
        };
        (file, evidence)
    }

    #[test]
    fn lossless_record_move_preserves_exact_inode_and_bytes() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("host.demo.ding.pid");
        let slot = root.path().join("private/records/host.demo.ding.pid");
        fs::create_dir_all(slot.parent().unwrap()).unwrap();
        let bytes = b"2000000000\n";
        let (held, expected) = write_record(&source, bytes);
        let locked = StateLock::acquire(root.path()).unwrap();

        lossless_move_record(&locked, &held, &expected, &slot).unwrap();

        assert!(!source.exists());
        let after = verify_retired_slot(&slot, &expected).unwrap();
        assert_eq!(after.device, expected.device);
        assert_eq!(after.inode, expected.inode);
        assert_eq!(fs::read(slot).unwrap(), bytes);
    }

    #[test]
    fn occupied_private_slot_preserves_both_objects() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("host.demo.ding.pid");
        let slot = root.path().join("private/records/host.demo.ding.pid");
        fs::create_dir_all(slot.parent().unwrap()).unwrap();
        let source_bytes = b"2000000000\n";
        let slot_bytes = b"foreign\n";
        let (held, expected) = write_record(&source, source_bytes);
        fs::write(&slot, slot_bytes).unwrap();
        let locked = StateLock::acquire(root.path()).unwrap();

        let error = lossless_move_record(&locked, &held, &expected, &slot).unwrap_err();

        assert!(error.to_string().contains("private slot"));
        assert_eq!(fs::read(source).unwrap(), source_bytes);
        assert_eq!(fs::read(slot).unwrap(), slot_bytes);
    }

    #[test]
    fn runtime_ids_are_single_safe_path_components() {
        assert!(safe_runtime_id("dev3.demo.ding"));
        assert!(safe_runtime_id("st2-a:b_c-1"));
        for unsafe_id in ["", ".hidden", "../escape", "a/b", "a b", "a\nb"] {
            assert!(!safe_runtime_id(unsafe_id), "{unsafe_id:?}");
        }
    }

    #[test]
    fn legacy_partition_digest_binds_none_and_typed_contents() {
        let mut none = Sha256::new();
        none.update(LEGACY_PARTITION_HASH_DOMAIN);
        none.update(b"null\n");
        assert_eq!(
            hash_legacy_partition(None).unwrap(),
            format!("{:x}", none.finalize())
        );

        let partition = [LegacySuccessorTask {
            runtime_id: "dev3.demo.ding".to_owned(),
            agent: "dev3.demo".to_owned(),
            task: "ding".to_owned(),
            desired_state: SuccessorDesiredState::RunningDing,
        }];
        assert_ne!(
            hash_legacy_partition(None).unwrap(),
            hash_legacy_partition(Some(&partition)).unwrap()
        );
    }

    #[test]
    fn leaderless_recovery_is_scoped_to_durable_per_item_mutation_boundary() {
        assert!(!leaderless_recovery_phase(ItemPhase::Prepared));
        assert!(leaderless_recovery_phase(ItemPhase::MutationAuthorized));
        assert!(leaderless_recovery_phase(ItemPhase::Frozen));
        assert!(!leaderless_recovery_phase(ItemPhase::Killed));
        assert!(!leaderless_recovery_phase(ItemPhase::RecordRetired));
        assert_eq!(
            leaderless_freeze_action(ItemPhase::MutationAuthorized, Some("0")).unwrap(),
            LeaderlessFreezeAction::Freeze
        );
        assert_eq!(
            leaderless_freeze_action(ItemPhase::MutationAuthorized, Some("1")).unwrap(),
            LeaderlessFreezeAction::AlreadyFrozen
        );
        assert_eq!(
            leaderless_freeze_action(ItemPhase::Frozen, Some("1")).unwrap(),
            LeaderlessFreezeAction::AlreadyFrozen
        );
        assert!(leaderless_freeze_action(ItemPhase::Frozen, Some("0")).is_err());
        assert!(leaderless_freeze_action(ItemPhase::Prepared, Some("0")).is_err());
    }

    #[test]
    fn runtime_permission_is_bound_to_exact_catalog_and_host() {
        let root = tempfile::tempdir().unwrap();
        let expected_catalog = root.path().join("expected");
        let other_catalog = root.path().join("other");
        fs::create_dir_all(&expected_catalog).unwrap();
        fs::create_dir_all(&other_catalog).unwrap();
        let ownership =
            crate::host_lock::HostOwnership::acquire(&expected_catalog, "expected-host").unwrap();
        let admission =
            crate::cutover_admission::RuntimeMutationAdmission::ordinary(&ownership).unwrap();
        let permission = admission.permission();
        let expected_catalog = expected_catalog.canonicalize().unwrap();
        let other_catalog = other_catalog.canonicalize().unwrap();

        ensure_permission_matches(&permission, &expected_catalog, "expected-host").unwrap();
        assert!(
            ensure_permission_matches(&permission, &other_catalog, "expected-host")
                .unwrap_err()
                .to_string()
                .contains("belongs to catalog")
        );
        assert!(
            ensure_permission_matches(&permission, &expected_catalog, "other-host")
                .unwrap_err()
                .to_string()
                .contains("belongs to host")
        );
    }
}
