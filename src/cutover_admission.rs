//! Durable, global admission for operations that can conflict with a catalog cutover.
//!
//! The durable fence and host ownership are deliberately different authorities. Beginning a
//! cutover first publishes the singleton fence while holding the catalog lock, releases that lock,
//! and only then claims `HostOwnership` followed by the catalog lock. No path waits for host
//! ownership while retaining the catalog lock.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use agent_spec::spec::{TaskKind, TaskLifecycle};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::catalog_lock::{CONTROL_DIR, CatalogLock};
use crate::catalog_transaction::{
    ApplyRequest, ApplyResult, apply_admitted, declaration_root_sha256_locked, prepare_apply,
};
use crate::ding_reconcile::{self, DingExecBackend, DingReconcileAction, DingReconcileReceipt};
use crate::host_lock::HostOwnership;

pub const CUTOVER_TRANSACTION_SCHEMA: &str = "st2.cutover-transaction.v4";
pub const MUTATION_BUSY_SCHEMA: &str = "st2.mutation-busy.v1";
pub const PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA: &str =
    "st2.cutover-predecessor-retirement-evidence.v1";
pub const EXTERNAL_CHECKPOINT_EVIDENCE_SCHEMA: &str = "st2.cutover-external-checkpoint-evidence.v1";
pub const PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA: &str =
    "st2.cutover-provider-fleet-proof-evidence.v1";

const CUTOVER_DIR: &str = "cutover";
const ACTIVE_MARKER: &str = "active.json";
const HISTORY_DIR: &str = "history";
const MAX_MARKER_BYTES: u64 = 1024 * 1024;
const MAX_RECEIPT_BYTES: usize = 64 * 1024;
const MAX_ID_BYTES: usize = 128;
const MAX_ARGV_ITEMS: usize = 256;
const MAX_ARG_BYTES: usize = 16 * 1024;
const MAX_ACTIONS: usize = 64;

/// Debug-only live fault boundary shared with the cutover systemd E2E.
///
/// The sentinel contract intentionally matches the CLI boundary: both variables are required, the
/// sentinel must already be a hardened file in a private `/tmp` child, and the process publishes
/// its exact pid and boundary before stopping itself for an external SIGKILL.
#[cfg(all(debug_assertions, target_os = "linux"))]
fn maybe_pause_at_cutover_test_boundary(boundary: &str) -> AdmissionResult<()> {
    const BOUNDARY_ENV: &str = "ST2_TEST_CUTOVER_BOUNDARY";
    const SENTINEL_ENV: &str = "ST2_TEST_CUTOVER_SENTINEL";
    let (Some(requested_boundary), Some(sentinel)) = (
        std::env::var_os(BOUNDARY_ENV),
        std::env::var_os(SENTINEL_ENV),
    ) else {
        if std::env::var_os(BOUNDARY_ENV).is_some() || std::env::var_os(SENTINEL_ENV).is_some() {
            return Err(AdmissionError::Invalid(format!(
                "{BOUNDARY_ENV} and {SENTINEL_ENV} must be supplied together for a cutover test boundary"
            )));
        }
        return Ok(());
    };
    if requested_boundary != boundary {
        return Ok(());
    }
    let sentinel = PathBuf::from(sentinel);
    if !sentinel.is_absolute() {
        return Err(AdmissionError::Invalid(format!(
            "{SENTINEL_ENV} must be absolute"
        )));
    }
    let link_metadata = match fs::symlink_metadata(&sentinel) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(AdmissionError::io(
                format!("inspect cutover test sentinel {}", sentinel.display()),
                error,
            ));
        }
    };
    let canonical = sentinel.canonicalize().map_err(|error| {
        AdmissionError::io(
            format!("canonicalize cutover test sentinel {}", sentinel.display()),
            error,
        )
    })?;
    if canonical != sentinel
        || !link_metadata.is_file()
        || link_metadata.file_type().is_symlink()
        || link_metadata.nlink() != 1
        || link_metadata.uid() != unsafe { libc::geteuid() }
        || link_metadata.permissions().mode() & 0o022 != 0
    {
        return Err(AdmissionError::Invalid(
            "cutover test sentinel must be a canonical, singly linked, current-user regular file not writable by group or world".to_owned(),
        ));
    }
    let parent = sentinel
        .parent()
        .ok_or_else(|| AdmissionError::Invalid("cutover test sentinel has no parent".to_owned()))?;
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        AdmissionError::io(
            format!("inspect cutover test directory {}", parent.display()),
            error,
        )
    })?;
    if parent.parent() != Some(Path::new("/tmp"))
        || !parent_metadata.is_dir()
        || parent_metadata.file_type().is_symlink()
        || parent_metadata.uid() != unsafe { libc::geteuid() }
        || parent_metadata.permissions().mode() & 0o077 != 0
    {
        return Err(AdmissionError::Invalid(
            "cutover test sentinel must be inside a private current-user temporary directory directly under /tmp".to_owned(),
        ));
    }
    let phase = sentinel.with_extension("phase");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&phase)
        .map_err(|error| {
            AdmissionError::io(
                format!("create cutover test phase {}", phase.display()),
                error,
            )
        })?;
    writeln!(output, "{} {boundary}", std::process::id())
        .map_err(|error| AdmissionError::io("write cutover test phase", error))?;
    output
        .sync_all()
        .map_err(|error| AdmissionError::io("sync cutover test phase", error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| AdmissionError::io("sync cutover test phase directory", error))?;
    // SAFETY: this debug-only, explicitly armed boundary deliberately suspends the candidate so
    // the live user-systemd test can SIGKILL this exact process.
    if unsafe { libc::raise(libc::SIGSTOP) } != 0 {
        return Err(AdmissionError::io(
            "raise SIGSTOP at cutover test boundary",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(not(all(debug_assertions, target_os = "linux")))]
fn maybe_pause_at_cutover_test_boundary(_boundary: &str) -> AdmissionResult<()> {
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct CanonicalCatalog(PathBuf);

impl CanonicalCatalog {
    pub fn open(path: impl AsRef<Path>) -> AdmissionResult<Self> {
        let requested = path.as_ref();
        let canonical = requested.canonicalize().map_err(|error| {
            AdmissionError::io(
                format!("canonicalize catalog {}", requested.display()),
                error,
            )
        })?;
        let metadata = fs::symlink_metadata(&canonical).map_err(|error| {
            AdmissionError::io(format!("inspect catalog {}", canonical.display()), error)
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(AdmissionError::Invalid(format!(
                "catalog is not a real directory: {}",
                canonical.display()
            )));
        }
        Ok(Self(canonical))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct HostId(String);

impl HostId {
    pub fn parse(host: impl Into<String>) -> AdmissionResult<Self> {
        let host = host.into();
        validate_component("host", &host)?;
        Ok(Self(host))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for HostId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct GateId(String);

impl GateId {
    pub fn parse(id: impl Into<String>) -> AdmissionResult<Self> {
        let id = id.into();
        validate_component("gate id", &id)?;
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for GateId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn validate_component(label: &str, value: &str) -> AdmissionResult<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.starts_with('.')
        || value.len() > MAX_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be one safe path component of at most {MAX_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationBusy {
    pub schema: &'static str,
    pub catalog: PathBuf,
    pub requested_host: Option<HostId>,
    pub active_marker: PathBuf,
    pub active_host: Option<HostId>,
    pub gate_id: Option<GateId>,
    pub reason: BusyReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BusyReason {
    ActiveCutover,
    MalformedActiveMarker,
    UnknownActiveEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationAdmission {
    Available,
    Busy(MutationBusy),
}

pub fn probe_mutation_admission(
    catalog: &CanonicalCatalog,
    requested_host: Option<&HostId>,
) -> AdmissionResult<MutationAdmission> {
    match reject_active_gate(catalog, requested_host) {
        Ok(()) => Ok(MutationAdmission::Available),
        Err(AdmissionError::Busy(busy)) => Ok(MutationAdmission::Busy(busy)),
        Err(error) => Err(error),
    }
}

#[derive(Debug)]
pub enum AdmissionError {
    Busy(MutationBusy),
    Invalid(String),
    Conflict(String),
    Io {
        context: String,
        source: std::io::Error,
    },
}

pub type AdmissionResult<T> = Result<T, AdmissionError>;

impl AdmissionError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy(busy) => write!(
                output,
                "mutation busy at {} ({:?})",
                busy.active_marker.display(),
                busy.reason
            ),
            Self::Invalid(message) | Self::Conflict(message) => output.write_str(message),
            Self::Io { context, source } => write!(output, "{context}: {source}"),
        }
    }
}

impl std::error::Error for AdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub struct RuntimeMutationAdmission<'owner> {
    _ownership: &'owner HostOwnership,
    catalog: CanonicalCatalog,
    host: HostId,
    _catalog_lock: CatalogLock,
}

impl<'owner> RuntimeMutationAdmission<'owner> {
    pub fn ordinary(ownership: &'owner HostOwnership) -> AdmissionResult<Self> {
        let catalog = CanonicalCatalog::open(ownership.catalog())?;
        let host = HostId::parse(ownership.host().to_owned())?;
        let catalog_lock = CatalogLock::shared(catalog.as_path()).map_err(|error| {
            AdmissionError::Invalid(format!("acquire shared catalog lock: {error:#}"))
        })?;
        reject_active_gate(&catalog, Some(&host))?;
        Ok(Self {
            _ownership: ownership,
            catalog,
            host,
            _catalog_lock: catalog_lock,
        })
    }

    pub fn permission(&self) -> RuntimeMutate<'_> {
        RuntimeMutate {
            catalog: &self.catalog,
            host: &self.host,
            _source: RuntimeAuthoritySource::Ordinary,
        }
    }
}

pub struct RuntimeMutate<'a> {
    catalog: &'a CanonicalCatalog,
    host: &'a HostId,
    _source: RuntimeAuthoritySource,
}

enum RuntimeAuthoritySource {
    Ordinary,
}

impl RuntimeMutate<'_> {
    pub fn catalog(&self) -> &CanonicalCatalog {
        self.catalog
    }

    pub fn host(&self) -> &HostId {
        self.host
    }
}

pub fn admit_catalog_publish<'a>(
    catalog: &'a CanonicalCatalog,
    held_exclusive_catalog_lock: &'a CatalogLock,
) -> AdmissionResult<CatalogPublish<'a>> {
    if !held_exclusive_catalog_lock.is_exclusive_for(catalog.as_path()) {
        return Err(AdmissionError::Invalid(
            "catalog publication requires an exclusive lock for the same canonical catalog"
                .to_owned(),
        ));
    }
    reject_active_gate(catalog, None)?;
    Ok(CatalogPublish {
        catalog,
        _catalog_lock: held_exclusive_catalog_lock,
    })
}

pub struct CatalogPublish<'a> {
    catalog: &'a CanonicalCatalog,
    _catalog_lock: &'a CatalogLock,
}

impl CatalogPublish<'_> {
    pub fn catalog(&self) -> &CanonicalCatalog {
        self.catalog
    }
}

#[derive(Debug, Clone)]
pub struct BeginCutover {
    pub catalog: CanonicalCatalog,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
    pub source_catalog_sha256: String,
    pub program: Vec<CutoverAction>,
    pub predecessor_retirement: PredecessorRetirementEvidence,
}

#[derive(Debug, Clone)]
pub struct ResumeCutover {
    pub catalog: CanonicalCatalog,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CutoverAction {
    CatalogTransition(CatalogTransition),
    ExternalCheckpoint {
        kind: ExternalCheckpointKind,
        input_sha256: String,
    },
    DingReconcile(DingReconcileAction),
    ProviderFleetProof(ProviderFleetProofAction),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogTransition {
    pub before_sha256: String,
    pub after_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalCheckpointKind {
    Cleanup,
    FinalProof,
    BusContinuity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderFleetProofAction {
    pub providers: Vec<ProviderFleetEntry>,
    pub providers_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderFleetEntry {
    pub identity: String,
    pub host: HostId,
    pub provider: String,
    pub account: String,
    pub persona: String,
    pub workspace: PathBuf,
    pub prompt: LaunchPromptAuthority,
    pub canonical_argv: Vec<String>,
    pub argv_sha256: String,
    pub profile_sha256: String,
    pub harness: String,
    pub model: String,
    pub effort: String,
    pub mode: String,
    pub boot_contract: String,
    pub launch_generation_id: String,
    pub runtime_generation_id: String,
    pub trajectory_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaunchPromptAuthority {
    pub runtime_profile_path: PathBuf,
    pub runtime_profile_sha256: String,
    pub persona_prompt_path: PathBuf,
    pub persona_prompt_sha256: String,
    pub launch_receipt_path: PathBuf,
    pub launch_receipt_sha256: String,
    pub injection_kind: PromptInjectionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PromptInjectionKind {
    ClaudeAppendSystemPromptFile,
    CodexDeveloperInstructions,
    OpencodeSystemPromptFile,
    PiAppendSystemPromptFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PredecessorRetiredDing {
    pub runtime_id: String,
    pub agent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PredecessorRetirementEvidence {
    pub schema: String,
    pub receipt_sha256: String,
    pub plan_sha256: String,
    pub catalog_sha256: String,
    pub host: HostId,
    pub census_sha256: String,
    pub journal_sha256: String,
    pub legacy_partition_sha256: String,
    pub legacy_partition: Vec<PredecessorRetiredDing>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalCheckpointEvidence {
    pub receipt_sha256: String,
    pub receipt: ExternalCheckpointReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalCheckpointReceipt {
    pub schema: String,
    pub canonical_catalog: PathBuf,
    pub catalog_device: u64,
    pub catalog_inode: u64,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
    pub action_index: usize,
    pub kind: ExternalCheckpointKind,
    pub input_sha256: String,
    pub payload: ExternalCheckpointPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ExternalCheckpointPayload {
    Cleanup {
        manifest_sha256: String,
        result_sha256: String,
    },
    FinalProof {
        final_catalog_sha256: String,
        providers_sha256: String,
        launch_receipts_sha256: String,
        ding_partition_sha256: String,
        ding_reconcile_sha256: String,
        validation_sha256: String,
        runtime_inventory_sha256: String,
    },
    BusContinuity {
        bus_id: String,
        probe_sha256: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderTaskStatus {
    Running,
    Absent,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderTaskObservation {
    pub identity: String,
    pub status: ProviderTaskStatus,
    pub runtime_generation_id: Option<String>,
    pub prompt: Option<LaunchPromptAuthority>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SuccessorDingObservation {
    Absent {
        runtime_id: String,
    },
    JournalBound {
        runtime_id: String,
        gate_id: GateId,
        action_index: usize,
        ding_generation_id: String,
        launch_sha256: String,
        journal_sha256: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderFleetSnapshot {
    pub authored_providers: Vec<ProviderTaskObservation>,
    pub successor_dings: Vec<SuccessorDingObservation>,
}

pub trait ProviderFleetObserver {
    fn observe_provider_rows(
        &self,
        catalog: &CanonicalCatalog,
        host: &HostId,
    ) -> AdmissionResult<Vec<ProviderTaskObservation>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderFleetProofEvidence {
    pub schema: String,
    pub providers_sha256: String,
    pub launch_receipts_sha256: String,
    pub ding_partition_sha256: String,
    pub result_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompletedCheckpoint {
    pub action_index: usize,
    pub evidence: ExternalCheckpointEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompletedDingReconcile {
    pub action_index: usize,
    pub receipt: DingReconcileReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverMarker {
    pub schema: String,
    pub canonical_catalog: PathBuf,
    pub catalog_device: u64,
    pub catalog_inode: u64,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
    pub source_catalog_sha256: String,
    pub program: Vec<CutoverAction>,
    pub cursor: usize,
    pub predecessor_retirement: PredecessorRetirementEvidence,
    pub completed_checkpoints: Vec<CompletedCheckpoint>,
    pub completed_ding_reconciles: Vec<CompletedDingReconcile>,
    pub provider_fleet_proof: Option<ProviderFleetProofEvidence>,
    pub finalized: bool,
}

pub enum ResumeOutcome {
    Active(CutoverTransaction),
    Finalized(FinalizedWithOwnership),
}

pub enum BeginOutcome {
    Claimed(CutoverTransaction),
    Fenced(PendingFence),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFence {
    pub catalog: CanonicalCatalog,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
    pub active_path: PathBuf,
}

impl PendingFence {
    /// Wait under the already durable fence until the retiring supervisor releases host ownership.
    ///
    /// This is an unbounded ownership wait by design. The candidate service remains alive (and is
    /// restarted by systemd if it crashes) rather than publishing a fence and exiting idle.
    pub fn wait_for_ownership(self) -> AdmissionResult<CutoverTransaction> {
        loop {
            match CutoverTransaction::claim_active(
                self.catalog.clone(),
                self.host.clone(),
                self.gate_id.clone(),
                self.request_sha256.clone(),
            ) {
                Ok(transaction) => return Ok(transaction),
                Err(AdmissionError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedCutover {
    pub history_path: PathBuf,
    pub marker: CutoverMarker,
}

pub struct FinalizedWithOwnership {
    pub finalized: FinalizedCutover,
    ownership: HostOwnership,
    readiness: Option<SuccessorReadiness>,
}

/// Final cutover authority retained until the successor supervisor has entered its loop.
pub struct SuccessorReadiness {
    catalog: CanonicalCatalog,
    active_path: PathBuf,
    marker: CutoverMarker,
    marker_bytes: Vec<u8>,
    _catalog_lock: CatalogLock,
}

impl FinalizedWithOwnership {
    pub fn into_successor_parts(
        self,
    ) -> (FinalizedCutover, HostOwnership, Option<SuccessorReadiness>) {
        (self.finalized, self.ownership, self.readiness)
    }
}

impl SuccessorReadiness {
    /// Archive the active fence only after the successor supervisor has entered its loop.
    pub fn supervisor_entered(self) -> AdmissionResult<()> {
        let (observed, bytes) = read_marker_with_bytes(&self.catalog, &self.active_path)?;
        if observed != self.marker || bytes != self.marker_bytes || !observed.finalized {
            return Err(AdmissionError::Conflict(
                "finalized active fence changed before successor readiness".to_owned(),
            ));
        }
        fs::remove_file(&self.active_path).map_err(|error| {
            AdmissionError::io(
                format!(
                    "archive active cutover marker after successor readiness {}",
                    self.active_path.display()
                ),
                error,
            )
        })?;
        sync_dir(
            self.active_path
                .parent()
                .ok_or_else(|| AdmissionError::Invalid("active fence has no parent".to_owned()))?,
        )
    }
}

pub struct CutoverTransaction {
    catalog: CanonicalCatalog,
    host: HostId,
    active_path: PathBuf,
    marker: CutoverMarker,
    marker_bytes: Vec<u8>,
    authority: CatalogAuthority,
    host_ownership: Option<HostOwnership>,
    catalog_lock: Option<CatalogLock>,
}

/// Retained directory and marker descriptors bind a transaction to one catalog incarnation.
///
/// The same-UID raw-write adversary remains outside the cooperative lock model, but every
/// transaction step detects path replacement and byte/inode drift before publishing its next
/// durable state.
struct CatalogAuthority {
    root: File,
    control: File,
    cutover: File,
    active: File,
    root_identity: (u64, u64),
    control_identity: (u64, u64),
    cutover_identity: (u64, u64),
    active_identity: (u64, u64),
}

impl CatalogAuthority {
    fn open(
        catalog: &CanonicalCatalog,
        active_path: &Path,
        marker: &CutoverMarker,
    ) -> AdmissionResult<Self> {
        let root = open_directory(catalog.as_path())?;
        let control_path = catalog.as_path().join(CONTROL_DIR);
        let control = open_directory(&control_path)?;
        let cutover_path = control_path.join(CUTOVER_DIR);
        let cutover = open_directory(&cutover_path)?;
        let active = open_regular(active_path)?;
        let root_identity = file_identity(&root)?;
        if root_identity != (marker.catalog_device, marker.catalog_inode) {
            return Err(AdmissionError::Conflict(
                "catalog incarnation does not match durable fence authority".to_owned(),
            ));
        }
        Ok(Self {
            root_identity,
            control_identity: file_identity(&control)?,
            cutover_identity: file_identity(&cutover)?,
            active_identity: file_identity(&active)?,
            root,
            control,
            cutover,
            active,
        })
    }

    fn revalidate(
        &self,
        catalog: &CanonicalCatalog,
        marker: &CutoverMarker,
    ) -> AdmissionResult<()> {
        if self.root_identity != (marker.catalog_device, marker.catalog_inode)
            || path_identity(catalog.as_path())? != self.root_identity
            || path_identity(&catalog.as_path().join(CONTROL_DIR))? != self.control_identity
            || path_identity(&catalog.as_path().join(CONTROL_DIR).join(CUTOVER_DIR))?
                != self.cutover_identity
            || file_identity(&self.root)? != self.root_identity
            || file_identity(&self.control)? != self.control_identity
            || file_identity(&self.cutover)? != self.cutover_identity
        {
            return Err(AdmissionError::Conflict(
                "catalog or cutover directory incarnation changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn read_active_bytes(&self) -> AdmissionResult<Vec<u8>> {
        let active_path_identity = fstatat_identity(&self.cutover, ACTIVE_MARKER)?;
        if active_path_identity != self.active_identity
            || file_identity(&self.active)? != self.active_identity
        {
            return Err(AdmissionError::Conflict(
                "active marker inode changed before compare-and-swap".to_owned(),
            ));
        }
        let mut file = self
            .active
            .try_clone()
            .map_err(|error| AdmissionError::io("clone retained active marker", error))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|error| AdmissionError::io("seek retained active marker", error))?;
        let mut bytes = Vec::new();
        file.take(MAX_MARKER_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| AdmissionError::io("read retained active marker", error))?;
        if bytes.len() as u64 > MAX_MARKER_BYTES {
            return Err(AdmissionError::Invalid(
                "retained active marker exceeds size bound".to_owned(),
            ));
        }
        Ok(bytes)
    }

    fn reopen_active(
        &mut self,
        catalog: &CanonicalCatalog,
        active_path: &Path,
        marker: &CutoverMarker,
    ) -> AdmissionResult<()> {
        self.revalidate(catalog, marker)?;
        let active = open_regular(active_path)?;
        let identity = file_identity(&active)?;
        if fstatat_identity(&self.cutover, ACTIVE_MARKER)? != identity {
            return Err(AdmissionError::Conflict(
                "replacement active marker is not the retained cutover entry".to_owned(),
            ));
        }
        self.active = active;
        self.active_identity = identity;
        Ok(())
    }
}

fn open_directory(path: &Path) -> AdmissionResult<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(|error| {
            AdmissionError::io(format!("open retained directory {}", path.display()), error)
        })
}

fn open_regular(path: &Path) -> AdmissionResult<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            AdmissionError::io(format!("open retained file {}", path.display()), error)
        })
}

fn file_identity(file: &File) -> AdmissionResult<(u64, u64)> {
    let metadata = file
        .metadata()
        .map_err(|error| AdmissionError::io("inspect retained descriptor", error))?;
    Ok((metadata.dev(), metadata.ino()))
}

fn path_identity(path: &Path) -> AdmissionResult<(u64, u64)> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AdmissionError::io(format!("inspect path identity {}", path.display()), error)
    })?;
    if metadata.file_type().is_symlink() {
        return Err(AdmissionError::Conflict(format!(
            "authority path became a symlink: {}",
            path.display()
        )));
    }
    Ok((metadata.dev(), metadata.ino()))
}

fn fstatat_identity(directory: &File, name: &str) -> AdmissionResult<(u64, u64)> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let name = CString::new(name)
        .map_err(|_| AdmissionError::Invalid("authority entry contains NUL".to_owned()))?;
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the retained directory descriptor and C string are valid and stat points to
    // writable storage initialized by a successful fstatat call.
    let result = unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(AdmissionError::io(
            format!("inspect retained cutover entry {name:?}"),
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: fstatat succeeded and initialized the value.
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(AdmissionError::Conflict(
            "retained active marker is not a regular file".to_owned(),
        ));
    }
    Ok((stat.st_dev, stat.st_ino))
}

impl CutoverTransaction {
    /// Publish the fence first, then release catalog EX before attempting host ownership.
    pub fn begin(request: BeginCutover) -> AdmissionResult<BeginOutcome> {
        validate_sha256("request sha256", &request.request_sha256)?;
        validate_sha256("source catalog sha256", &request.source_catalog_sha256)?;
        validate_program(&request.source_catalog_sha256, &request.program)?;
        let paths = ensure_cutover_dirs(&request.catalog)?;
        let active_path = active_marker_path(&paths);
        let history_path = history_marker_path(&paths, &request.host, &request.gate_id)?;

        let marker = CutoverMarker {
            schema: CUTOVER_TRANSACTION_SCHEMA.to_owned(),
            canonical_catalog: request.catalog.as_path().to_path_buf(),
            catalog_device: fs::metadata(request.catalog.as_path())
                .map_err(|error| AdmissionError::io("inspect catalog identity", error))?
                .dev(),
            catalog_inode: fs::metadata(request.catalog.as_path())
                .map_err(|error| AdmissionError::io("inspect catalog identity", error))?
                .ino(),
            host: request.host.clone(),
            gate_id: request.gate_id.clone(),
            request_sha256: request.request_sha256,
            source_catalog_sha256: request.source_catalog_sha256,
            program: request.program,
            cursor: 0,
            predecessor_retirement: request.predecessor_retirement,
            completed_checkpoints: Vec::new(),
            completed_ding_reconciles: Vec::new(),
            provider_fleet_proof: None,
            finalized: false,
        };
        validate_marker(&request.catalog, &request.host, &marker)?;

        {
            let catalog_lock =
                CatalogLock::exclusive(request.catalog.as_path()).map_err(|error| {
                    AdmissionError::Invalid(format!("acquire exclusive catalog lock: {error:#}"))
                })?;
            let _ = &catalog_lock;
            reject_active_gate(&request.catalog, Some(&request.host))?;
            reject_history_collision(&history_path)?;
            let observed =
                declaration_root_sha256_locked(request.catalog.as_path()).map_err(|error| {
                    AdmissionError::Invalid(format!(
                        "compute source declaration-root digest: {error:#}"
                    ))
                })?;
            if observed != marker.source_catalog_sha256 {
                return Err(AdmissionError::Conflict(format!(
                    "source catalog digest compare-and-swap failed: expected {}, found {observed}",
                    marker.source_catalog_sha256
                )));
            }
            publish_create_only(&paths.cutover, &active_path, &marker)?;
        }

        match Self::claim_active(
            request.catalog.clone(),
            request.host.clone(),
            marker.gate_id.clone(),
            marker.request_sha256.clone(),
        ) {
            Ok(transaction) => Ok(BeginOutcome::Claimed(transaction)),
            Err(AdmissionError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::WouldBlock =>
            {
                Ok(BeginOutcome::Fenced(PendingFence {
                    catalog: request.catalog,
                    host: request.host,
                    gate_id: marker.gate_id,
                    request_sha256: marker.request_sha256,
                    active_path,
                }))
            }
            Err(error) => Err(error),
        }
    }

    pub fn resume(request: ResumeCutover) -> AdmissionResult<ResumeOutcome> {
        validate_sha256("request sha256", &request.request_sha256)?;
        let host_ownership =
            HostOwnership::acquire(request.catalog.as_path(), request.host.as_str()).map_err(
                |error| {
                    AdmissionError::io(
                        format!("acquire cutover host lock for {}", request.host.as_str()),
                        error,
                    )
                },
            )?;
        let catalog_lock = CatalogLock::exclusive(request.catalog.as_path()).map_err(|error| {
            AdmissionError::Invalid(format!("acquire exclusive catalog lock: {error:#}"))
        })?;
        let paths = ensure_cutover_dirs(&request.catalog)?;
        let active_path = active_marker_path(&paths);
        match fs::symlink_metadata(&active_path) {
            Ok(_) => {
                let (marker, marker_bytes) =
                    read_marker_with_bytes(&request.catalog, &active_path)?;
                verify_resume_authority(&request, &marker)?;
                validate_resume_catalog_digest(&request.catalog, &marker)?;
                if marker.finalized {
                    let history_path =
                        history_marker_path(&paths, &request.host, &request.gate_id)?;
                    ensure_exact_finalized_history(&request.catalog, &history_path, &marker_bytes)?;
                    return Ok(ResumeOutcome::Finalized(FinalizedWithOwnership {
                        finalized: FinalizedCutover {
                            history_path,
                            marker: marker.clone(),
                        },
                        ownership: host_ownership,
                        readiness: Some(SuccessorReadiness {
                            catalog: request.catalog,
                            active_path,
                            marker,
                            marker_bytes,
                            _catalog_lock: catalog_lock,
                        }),
                    }));
                }
                let authority = CatalogAuthority::open(&request.catalog, &active_path, &marker)?;
                Ok(ResumeOutcome::Active(Self {
                    catalog: request.catalog,
                    host: request.host,
                    active_path,
                    marker,
                    marker_bytes,
                    authority,
                    host_ownership: Some(host_ownership),
                    catalog_lock: Some(catalog_lock),
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let history_path = history_marker_path(&paths, &request.host, &request.gate_id)?;
                let (marker, _) = read_marker_with_bytes(&request.catalog, &history_path)
                    .map_err(|error| {
                        AdmissionError::Conflict(format!(
                            "active fence absent and exact finalized history unavailable at {}: {error}",
                            history_path.display()
                        ))
                    })?;
                verify_resume_authority(&request, &marker)?;
                if !marker.finalized {
                    return Err(AdmissionError::Conflict(
                        "exact history record is not finalized".to_owned(),
                    ));
                }
                Ok(ResumeOutcome::Finalized(FinalizedWithOwnership {
                    finalized: FinalizedCutover {
                        history_path,
                        marker,
                    },
                    ownership: host_ownership,
                    readiness: None,
                }))
            }
            Err(error) => Err(AdmissionError::io(
                format!("inspect active cutover marker {}", active_path.display()),
                error,
            )),
        }
    }

    /// Inspect exact finalized history without acquiring host ownership.
    ///
    /// This is the idempotent post-cutover read path: the successor may retain the host lock
    /// indefinitely, so a repeated driver invocation must not block trying to reacquire it merely
    /// to prove that its exact request already finalized. `None` means the active transaction still
    /// exists or the exact history record is absent; malformed or mismatched history is an error.
    pub fn inspect_finalized(request: ResumeCutover) -> AdmissionResult<Option<FinalizedCutover>> {
        validate_sha256("request sha256", &request.request_sha256)?;
        let _catalog_lock = CatalogLock::shared(request.catalog.as_path()).map_err(|error| {
            AdmissionError::Invalid(format!("acquire shared catalog lock: {error:#}"))
        })?;
        let cutover = request
            .catalog
            .as_path()
            .join(CONTROL_DIR)
            .join(CUTOVER_DIR);
        let active_path = cutover.join(ACTIVE_MARKER);
        match fs::symlink_metadata(&active_path) {
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(AdmissionError::io(
                    format!("inspect active cutover marker {}", active_path.display()),
                    error,
                ));
            }
        }
        let history_path = cutover
            .join(HISTORY_DIR)
            .join(request.host.as_str())
            .join(format!("{}.json", request.gate_id.as_str()));
        match fs::symlink_metadata(&history_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(AdmissionError::io(
                    format!("inspect cutover history {}", history_path.display()),
                    error,
                ));
            }
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(AdmissionError::Conflict(format!(
                    "exact cutover history is not a regular file: {}",
                    history_path.display()
                )));
            }
        }
        let (marker, _) = read_marker_with_bytes(&request.catalog, &history_path)?;
        verify_resume_authority(&request, &marker)?;
        if !marker.finalized {
            return Err(AdmissionError::Conflict(
                "exact history record is not finalized".to_owned(),
            ));
        }
        Ok(Some(FinalizedCutover {
            history_path,
            marker,
        }))
    }

    /// Reacquire successor ownership from exact finalized history without reopening transaction
    /// authority.
    ///
    /// `None` means no exact finalized history exists and the caller may attempt a fresh begin.
    /// Once history is observed, this rechecks active absence and the same history bytes while
    /// retaining host ownership plus catalog EX. A mismatched/corrupt history record is always an
    /// error; it never falls through to begin.
    pub fn reacquire_finalized_successor(
        request: ResumeCutover,
    ) -> AdmissionResult<Option<FinalizedWithOwnership>> {
        let Some(_) = Self::inspect_finalized(request.clone())? else {
            return Ok(None);
        };
        let host_ownership =
            HostOwnership::acquire(request.catalog.as_path(), request.host.as_str()).map_err(
                |error| {
                    AdmissionError::io(
                        format!(
                            "reacquire finalized successor host lock for {}",
                            request.host.as_str()
                        ),
                        error,
                    )
                },
            )?;
        let _catalog_lock = CatalogLock::exclusive(request.catalog.as_path()).map_err(|error| {
            AdmissionError::Invalid(format!("acquire exclusive catalog lock: {error:#}"))
        })?;
        let paths = ensure_cutover_dirs(&request.catalog)?;
        let active_path = active_marker_path(&paths);
        match fs::symlink_metadata(&active_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(AdmissionError::Conflict(
                    "active cutover appeared while reacquiring finalized successor".to_owned(),
                ));
            }
            Err(error) => {
                return Err(AdmissionError::io(
                    format!("inspect active cutover marker {}", active_path.display()),
                    error,
                ));
            }
        }
        let history_path = history_marker_path(&paths, &request.host, &request.gate_id)?;
        let (marker, _) = read_marker_with_bytes(&request.catalog, &history_path)?;
        verify_resume_authority(&request, &marker)?;
        if !marker.finalized {
            return Err(AdmissionError::Conflict(
                "exact history record is not finalized".to_owned(),
            ));
        }
        Ok(Some(FinalizedWithOwnership {
            finalized: FinalizedCutover {
                history_path,
                marker,
            },
            ownership: host_ownership,
            readiness: None,
        }))
    }

    fn claim_active(
        catalog: CanonicalCatalog,
        host: HostId,
        gate_id: GateId,
        request_sha256: String,
    ) -> AdmissionResult<Self> {
        let host_ownership =
            HostOwnership::acquire(catalog.as_path(), host.as_str()).map_err(|error| {
                AdmissionError::io(
                    format!("acquire cutover host lock for {}", host.as_str()),
                    error,
                )
            })?;
        let catalog_lock = CatalogLock::exclusive(catalog.as_path()).map_err(|error| {
            AdmissionError::Invalid(format!("acquire exclusive catalog lock: {error:#}"))
        })?;
        let paths = ensure_cutover_dirs(&catalog)?;
        let active_path = active_marker_path(&paths);
        let (marker, marker_bytes) = read_marker_with_bytes(&catalog, &active_path)?;
        let request = ResumeCutover {
            catalog: catalog.clone(),
            host: host.clone(),
            gate_id,
            request_sha256,
        };
        verify_resume_authority(&request, &marker)?;
        validate_resume_catalog_digest(&catalog, &marker)?;
        let authority = CatalogAuthority::open(&catalog, &active_path, &marker)?;
        Ok(Self {
            catalog,
            host: host.clone(),
            active_path,
            marker,
            marker_bytes,
            authority,
            host_ownership: Some(host_ownership),
            catalog_lock: Some(catalog_lock),
        })
    }

    pub fn marker(&self) -> &CutoverMarker {
        &self.marker
    }

    pub fn permission(&mut self) -> Transaction<'_> {
        Transaction { transaction: self }
    }

    fn persist(&mut self, next: CutoverMarker) -> AdmissionResult<()> {
        self.authority.revalidate(&self.catalog, &self.marker)?;
        let observed_bytes = self.authority.read_active_bytes()?;
        let observed: CutoverMarker = serde_json::from_slice(&observed_bytes).map_err(|error| {
            AdmissionError::Invalid(format!("parse retained active marker: {error}"))
        })?;
        if observed != self.marker || observed_bytes != self.marker_bytes {
            return Err(AdmissionError::Conflict(format!(
                "cutover marker compare-and-swap failed at {}",
                self.active_path.display()
            )));
        }
        validate_marker(&self.catalog, &self.host, &next)?;
        let next_bytes = replace_durable(
            &self.authority.cutover,
            self.active_path.parent().ok_or_else(|| {
                AdmissionError::Invalid("cutover marker has no parent".to_owned())
            })?,
            &self.active_path,
            &next,
        )?;
        self.marker = next;
        self.marker_bytes = next_bytes;
        self.authority
            .reopen_active(&self.catalog, &self.active_path, &self.marker)?;
        Ok(())
    }
}

pub struct Transaction<'a> {
    transaction: &'a mut CutoverTransaction,
}

/// Opaque retained catalog writer seam for the exact next catalog-transition action.
pub(crate) struct TransactionCatalog<'a> {
    catalog: &'a CanonicalCatalog,
    _catalog_lock: &'a CatalogLock,
}

impl TransactionCatalog<'_> {
    pub(crate) fn catalog(&self) -> &CanonicalCatalog {
        self.catalog
    }
}

impl Transaction<'_> {
    pub fn marker(&self) -> &CutoverMarker {
        &self.transaction.marker
    }

    pub(crate) fn catalog_transition_authority(
        &self,
        expected_index: usize,
    ) -> AdmissionResult<TransactionCatalog<'_>> {
        self.expect_cursor(expected_index)?;
        if !matches!(
            self.transaction.marker.program.get(expected_index),
            Some(CutoverAction::CatalogTransition(_))
        ) {
            return Err(self.wrong_action("catalog-transition"));
        }
        Ok(TransactionCatalog {
            catalog: &self.transaction.catalog,
            _catalog_lock: self.transaction.catalog_lock.as_ref().ok_or_else(|| {
                AdmissionError::Conflict(
                    "cutover catalog authority was already released".to_owned(),
                )
            })?,
        })
    }

    pub fn apply_catalog_transition_once(
        &mut self,
        expected_index: usize,
        request: ApplyRequest,
    ) -> AdmissionResult<Option<ApplyResult>> {
        let prepared = prepare_apply(request).map_err(|error| {
            AdmissionError::Conflict(format!("prepare catalog apply: {error:#}"))
        })?;
        let mut result = None;
        self.catalog_transition_once(expected_index, |authority| {
            result =
                Some(apply_admitted(prepared, authority).map_err(|error| {
                    AdmissionError::Conflict(format!("catalog apply: {error:#}"))
                })?);
            Ok(())
        })?;
        Ok(result)
    }

    fn catalog_transition_once<F>(
        &mut self,
        expected_index: usize,
        mutate: F,
    ) -> AdmissionResult<()>
    where
        F: FnOnce(&TransactionCatalog<'_>) -> AdmissionResult<()>,
    {
        self.expect_cursor(expected_index)?;
        let transition = match self.transaction.marker.program.get(expected_index) {
            Some(CutoverAction::CatalogTransition(transition)) => transition.clone(),
            _ => return Err(self.wrong_action("catalog-transition")),
        };
        let observed_before = declaration_root_sha256_locked(self.transaction.catalog.as_path())
            .map_err(|error| {
                AdmissionError::Invalid(format!(
                    "compute declaration-root digest before transition: {error:#}"
                ))
            })?;
        if observed_before == transition.after_sha256 {
            // The declaration mutation committed before the prior process advanced the cursor.
        } else if observed_before == transition.before_sha256 {
            let authority = self.catalog_transition_authority(expected_index)?;
            mutate(&authority)?;
            let observed_after = declaration_root_sha256_locked(self.transaction.catalog.as_path())
                .map_err(|error| {
                    AdmissionError::Invalid(format!(
                        "compute declaration-root digest after transition: {error:#}"
                    ))
                })?;
            if observed_after != transition.after_sha256 {
                return Err(AdmissionError::Conflict(format!(
                    "catalog transition produced unexpected digest: expected {}, found {observed_after}",
                    transition.after_sha256
                )));
            }
        } else {
            return Err(AdmissionError::Conflict(format!(
                "catalog digest compare-and-swap failed: expected {} or crash-recovery digest {}, found {observed_before}",
                transition.before_sha256, transition.after_sha256
            )));
        }
        self.advance_cursor()
    }

    pub fn record_external_checkpoint(
        &mut self,
        expected_index: usize,
        receipt_bytes: &[u8],
    ) -> AdmissionResult<()> {
        self.expect_cursor(expected_index)?;
        let (expected_kind, expected_input_sha256) =
            match self.transaction.marker.program.get(expected_index) {
                Some(CutoverAction::ExternalCheckpoint { kind, input_sha256 }) => {
                    (*kind, input_sha256.as_str())
                }
                _ => return Err(self.wrong_action("external-checkpoint")),
            };
        let evidence = parse_external_receipt(
            &self.transaction.catalog,
            &self.transaction.marker,
            expected_index,
            expected_kind,
            expected_input_sha256,
            receipt_bytes,
        )?;
        let mut next = self.transaction.marker.clone();
        next.completed_checkpoints.push(CompletedCheckpoint {
            action_index: expected_index,
            evidence,
        });
        next.cursor += 1;
        self.transaction.persist(next)
    }

    /// Reconcile the immutable successor Ding set through a capability-poor exec backend.
    pub(crate) fn reconcile_dings_once(
        &mut self,
        expected_index: usize,
        backend: &dyn DingExecBackend,
    ) -> AdmissionResult<DingReconcileReceipt> {
        self.expect_cursor(expected_index)?;
        let action = match self.transaction.marker.program.get(expected_index) {
            Some(CutoverAction::DingReconcile(action)) => action.clone(),
            _ => return Err(self.wrong_action("ding-reconcile")),
        };
        let receipt = ding_reconcile::reconcile(
            self.transaction.catalog.as_path(),
            &self.transaction.authority.cutover,
            self.transaction.host.as_str(),
            self.transaction.marker.gate_id.as_str(),
            expected_index,
            &action,
            backend,
        )?;
        let mut completed = self.transaction.marker.clone();
        completed
            .completed_ding_reconciles
            .push(CompletedDingReconcile {
                action_index: expected_index,
                receipt: receipt.clone(),
            });
        completed.cursor += 1;
        self.transaction.persist(completed)?;
        Ok(receipt)
    }

    /// Prove every authored local provider task is the exact live launch-scoped generation.
    ///
    /// Catalog declarations, rather than observer assertions, define the provider inventory and
    /// immutable launch tuple. The observer can only report live generation and prompt evidence.
    /// Any refusal occurs before marker persistence and therefore has zero durable side effects.
    pub(crate) fn prove_provider_fleet_once(
        &mut self,
        expected_index: usize,
        observer: &dyn ProviderFleetObserver,
        ding_reader: &dyn ding_reconcile::DingGenerationReader,
    ) -> AdmissionResult<ProviderFleetProofEvidence> {
        self.expect_cursor(expected_index)?;
        let action = match self.transaction.marker.program.get(expected_index) {
            Some(CutoverAction::ProviderFleetProof(action)) => action.clone(),
            _ => return Err(self.wrong_action("provider-fleet-proof")),
        };
        validate_provider_inventory_from_catalog(
            &self.transaction.catalog,
            &self.transaction.host,
            &action,
            &catalog_digest_at_cursor(&self.transaction.marker, self.transaction.marker.cursor),
        )?;
        let ding = match self.transaction.marker.program.get(expected_index + 1) {
            Some(CutoverAction::DingReconcile(action)) => action,
            _ => {
                return Err(AdmissionError::Invalid(
                    "provider fleet proof must be followed by its exact Ding reconciliation"
                        .to_owned(),
                ));
            }
        };
        let authored_providers =
            observer.observe_provider_rows(&self.transaction.catalog, &self.transaction.host)?;
        let successor_dings = ding_reconcile::observe_successor_partition(
            &self.transaction.catalog,
            &self.transaction.authority.cutover,
            self.transaction.host.as_str(),
            &self.transaction.marker.gate_id,
            expected_index + 1,
            ding,
            ding_reader,
        )?;
        let snapshot = ProviderFleetSnapshot {
            authored_providers,
            successor_dings,
        };
        validate_provider_snapshot(
            &action,
            ding,
            &self.transaction.marker.gate_id,
            expected_index + 1,
            &snapshot,
        )?;
        let ding_partition_sha256 = successor_ding_partition_sha256(&snapshot.successor_dings)?;
        let result_sha256 = provider_fleet_result_sha256(&snapshot)?;
        let completion = ProviderFleetProofEvidence {
            schema: PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA.to_owned(),
            providers_sha256: action.providers_sha256.clone(),
            launch_receipts_sha256: provider_launch_receipts_sha256(&action.providers)?,
            ding_partition_sha256,
            result_sha256,
        };
        validate_provider_fleet_proof(&action, &completion)?;
        let mut completed = self.transaction.marker.clone();
        completed.provider_fleet_proof = Some(completion.clone());
        completed.cursor += 1;
        self.transaction.persist(completed)?;
        Ok(completion)
    }

    pub fn finalize(self) -> AdmissionResult<FinalizedWithOwnership> {
        let marker = &self.transaction.marker;
        if marker.cursor != marker.program.len() {
            return Err(AdmissionError::Conflict(
                "cannot finalize before the exact action program is complete".to_owned(),
            ));
        }
        if marker.provider_fleet_proof.is_none() {
            return Err(AdmissionError::Conflict(
                "cannot finalize without exact provider fleet proof".to_owned(),
            ));
        }
        for kind in [
            ExternalCheckpointKind::FinalProof,
            ExternalCheckpointKind::BusContinuity,
        ] {
            if !marker
                .completed_checkpoints
                .iter()
                .any(|checkpoint| checkpoint.evidence.receipt.kind == kind)
            {
                return Err(AdmissionError::Conflict(format!(
                    "cannot finalize without {kind:?} evidence"
                )));
            }
        }
        let expected_final = catalog_digest_at_cursor(marker, marker.program.len());
        let observed = declaration_root_sha256_locked(self.transaction.catalog.as_path()).map_err(
            |error| {
                AdmissionError::Invalid(format!("compute final declaration-root digest: {error:#}"))
            },
        )?;
        if observed != expected_final {
            return Err(AdmissionError::Conflict(format!(
                "final catalog digest compare-and-swap failed: expected {expected_final}, found {observed}"
            )));
        }

        let mut finalized = marker.clone();
        finalized.finalized = true;
        let finalized_bytes = canonical_json(&finalized)?;
        let paths = ensure_cutover_dirs(&self.transaction.catalog)?;
        let history_path = history_marker_path(
            &paths,
            &self.transaction.host,
            &self.transaction.marker.gate_id,
        )?;
        // History becomes durable before active.json can claim finalization. A crash at the
        // following exact boundary leaves an unfinalized active transaction plus byte-exact
        // finalized history; replay republishes the same history and then performs the active CAS.
        // The inverse state (finalized active without history) is therefore unreachable for new
        // writers and repaired by `resume` for compatibility with an interrupted older writer.
        ensure_exact_finalized_history(&self.transaction.catalog, &history_path, &finalized_bytes)?;
        maybe_pause_at_cutover_test_boundary("after-finalized-history-before-active-finalized")?;
        self.transaction.persist(finalized)?;
        if self.transaction.marker_bytes != finalized_bytes {
            return Err(AdmissionError::Conflict(
                "finalized active marker differs from exact durable history".to_owned(),
            ));
        }
        let ownership = self.transaction.host_ownership.take().ok_or_else(|| {
            AdmissionError::Conflict("cutover host ownership was already transferred".to_owned())
        })?;
        let catalog_lock = self.transaction.catalog_lock.take().ok_or_else(|| {
            AdmissionError::Conflict("cutover catalog authority was already transferred".to_owned())
        })?;
        Ok(FinalizedWithOwnership {
            finalized: FinalizedCutover {
                history_path,
                marker: self.transaction.marker.clone(),
            },
            ownership,
            readiness: Some(SuccessorReadiness {
                catalog: self.transaction.catalog.clone(),
                active_path: self.transaction.active_path.clone(),
                marker: self.transaction.marker.clone(),
                marker_bytes: self.transaction.marker_bytes.clone(),
                _catalog_lock: catalog_lock,
            }),
        })
    }

    fn expect_cursor(&self, expected: usize) -> AdmissionResult<()> {
        if self.transaction.marker.cursor != expected {
            return Err(AdmissionError::Conflict(format!(
                "action cursor compare-and-swap failed: expected {expected}, found {}",
                self.transaction.marker.cursor
            )));
        }
        Ok(())
    }

    fn wrong_action(&self, expected: &str) -> AdmissionError {
        AdmissionError::Conflict(format!(
            "action {} is not the exact next {expected} action",
            self.transaction.marker.cursor
        ))
    }

    fn advance_cursor(&mut self) -> AdmissionResult<()> {
        let mut next = self.transaction.marker.clone();
        next.cursor += 1;
        self.transaction.persist(next)
    }
}

#[derive(Debug)]
struct CutoverPaths {
    cutover: PathBuf,
    history: PathBuf,
}

fn ensure_cutover_dirs(catalog: &CanonicalCatalog) -> AdmissionResult<CutoverPaths> {
    let control = catalog.as_path().join(CONTROL_DIR);
    ensure_real_directory(&control, catalog.as_path())?;
    let cutover = control.join(CUTOVER_DIR);
    ensure_real_directory(&cutover, &control)?;
    let history = cutover.join(HISTORY_DIR);
    ensure_real_directory(&history, &cutover)?;
    Ok(CutoverPaths { cutover, history })
}

fn ensure_real_directory(path: &Path, parent: &Path) -> AdmissionResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(AdmissionError::Invalid(format!(
            "cutover control path is not a real directory: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => sync_dir(parent),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path).map_err(|inspect| {
                    AdmissionError::io(
                        format!("inspect raced cutover directory {}", path.display()),
                        inspect,
                    )
                })?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    Ok(())
                } else {
                    Err(AdmissionError::Invalid(format!(
                        "cutover control path is not a real directory: {}",
                        path.display()
                    )))
                }
            }
            Err(error) => Err(AdmissionError::io(
                format!("create cutover directory {}", path.display()),
                error,
            )),
        },
        Err(error) => Err(AdmissionError::io(
            format!("inspect cutover directory {}", path.display()),
            error,
        )),
    }
}

fn active_marker_path(paths: &CutoverPaths) -> PathBuf {
    paths.cutover.join(ACTIVE_MARKER)
}

fn history_marker_path(
    paths: &CutoverPaths,
    host: &HostId,
    gate_id: &GateId,
) -> AdmissionResult<PathBuf> {
    let host_dir = paths.history.join(host.as_str());
    ensure_real_directory(&host_dir, &paths.history)?;
    Ok(host_dir.join(format!("{}.json", gate_id.as_str())))
}

fn reject_history_collision(path: &Path) -> AdmissionResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AdmissionError::io(
            format!("inspect cutover history {}", path.display()),
            error,
        )),
        Ok(_) => Err(AdmissionError::Conflict(format!(
            "cutover history already exists at {}",
            path.display()
        ))),
    }
}

fn reject_active_gate(
    catalog: &CanonicalCatalog,
    requested_host: Option<&HostId>,
) -> AdmissionResult<()> {
    let active = catalog
        .as_path()
        .join(CONTROL_DIR)
        .join(CUTOVER_DIR)
        .join(ACTIVE_MARKER);
    match fs::symlink_metadata(&active) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(AdmissionError::io(
                format!("inspect active cutover entry {}", active.display()),
                error,
            ));
        }
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(busy(
                catalog,
                requested_host,
                active,
                None,
                None,
                BusyReason::UnknownActiveEntry,
            ));
        }
    }
    let marker = match read_marker(catalog, &active) {
        Ok(marker) => marker,
        Err(_) => {
            return Err(busy(
                catalog,
                requested_host,
                active,
                None,
                None,
                BusyReason::MalformedActiveMarker,
            ));
        }
    };
    if validate_marker(catalog, &marker.host, &marker).is_err() {
        return Err(busy(
            catalog,
            requested_host,
            active,
            Some(marker.host),
            None,
            BusyReason::MalformedActiveMarker,
        ));
    }
    Err(busy(
        catalog,
        requested_host,
        active,
        Some(marker.host),
        Some(marker.gate_id),
        BusyReason::ActiveCutover,
    ))
}

fn busy(
    catalog: &CanonicalCatalog,
    requested_host: Option<&HostId>,
    active_marker: PathBuf,
    active_host: Option<HostId>,
    gate_id: Option<GateId>,
    reason: BusyReason,
) -> AdmissionError {
    AdmissionError::Busy(MutationBusy {
        schema: MUTATION_BUSY_SCHEMA,
        catalog: catalog.as_path().to_path_buf(),
        requested_host: requested_host.cloned(),
        active_marker,
        active_host,
        gate_id,
        reason,
    })
}

fn verify_resume_authority(request: &ResumeCutover, marker: &CutoverMarker) -> AdmissionResult<()> {
    validate_marker(&request.catalog, &request.host, marker)?;
    if marker.gate_id != request.gate_id || marker.request_sha256 != request.request_sha256 {
        return Err(AdmissionError::Conflict(
            "cutover authority does not match the exact gate and request".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_program(source: &str, program: &[CutoverAction]) -> AdmissionResult<()> {
    if program.is_empty() || program.len() > MAX_ACTIONS {
        return Err(AdmissionError::Invalid(format!(
            "cutover program must contain 1..={MAX_ACTIONS} actions"
        )));
    }
    let mut catalog_digest = source;
    let mut ding_count = 0;
    let mut adoption_count = 0;
    let mut final_proof_count = 0;
    let mut bus_count = 0;
    for action in program {
        match action {
            CutoverAction::CatalogTransition(transition) => {
                validate_sha256(
                    "catalog transition before sha256",
                    &transition.before_sha256,
                )?;
                validate_sha256("catalog transition after sha256", &transition.after_sha256)?;
                if transition.before_sha256 != catalog_digest
                    || transition.after_sha256 == transition.before_sha256
                {
                    return Err(AdmissionError::Invalid(
                        "catalog transition program is not exact and contiguous".to_owned(),
                    ));
                }
                catalog_digest = &transition.after_sha256;
            }
            CutoverAction::ExternalCheckpoint { kind, input_sha256 } => {
                validate_sha256("external checkpoint input sha256", input_sha256)?;
                match kind {
                    ExternalCheckpointKind::FinalProof => final_proof_count += 1,
                    ExternalCheckpointKind::BusContinuity => bus_count += 1,
                    ExternalCheckpointKind::Cleanup => {}
                }
            }
            CutoverAction::DingReconcile(action) => {
                ding_count += 1;
                ding_reconcile::validate_action(action)?;
            }
            CutoverAction::ProviderFleetProof(action) => {
                adoption_count += 1;
                validate_provider_fleet_action(action)?;
            }
        }
    }
    if ding_count != 1 || adoption_count != 1 || final_proof_count != 1 || bus_count != 1 {
        return Err(AdmissionError::Invalid(
            "cutover program needs exactly one Ding reconciliation, adoption proof, final-proof, and bus-continuity action".to_owned(),
        ));
    }
    let ding_index = program
        .iter()
        .position(|action| matches!(action, CutoverAction::DingReconcile(_)))
        .expect("Ding count checked");
    let adoption_index = program
        .iter()
        .position(|action| matches!(action, CutoverAction::ProviderFleetProof(_)))
        .expect("adoption count checked");
    if ding_index != adoption_index + 1 {
        return Err(AdmissionError::Invalid(
            "Ding reconciliation must be the exact next action after provider fleet proof"
                .to_owned(),
        ));
    }
    if program[..adoption_index]
        .iter()
        .filter(|action| matches!(action, CutoverAction::CatalogTransition(_)))
        .count()
        != program
            .iter()
            .filter(|action| matches!(action, CutoverAction::CatalogTransition(_)))
            .count()
    {
        return Err(AdmissionError::Invalid(
            "every catalog transition must precede provider fleet proof".to_owned(),
        ));
    }
    let bus_index = program
        .iter()
        .position(|action| {
            matches!(
                action,
                CutoverAction::ExternalCheckpoint {
                    kind: ExternalCheckpointKind::BusContinuity,
                    ..
                }
            )
        })
        .expect("bus count checked");
    if bus_index <= ding_index {
        return Err(AdmissionError::Invalid(
            "bus-continuity must follow Ding reconciliation".to_owned(),
        ));
    }
    if !matches!(
        program.last(),
        Some(CutoverAction::ExternalCheckpoint {
            kind: ExternalCheckpointKind::FinalProof,
            ..
        })
    ) {
        return Err(AdmissionError::Invalid(
            "final-proof must be the last cutover action".to_owned(),
        ));
    }
    Ok(())
}

fn validate_provider_fleet_action(action: &ProviderFleetProofAction) -> AdmissionResult<()> {
    if action.providers.is_empty() || action.providers.len() > MAX_ACTIONS {
        return Err(AdmissionError::Invalid(format!(
            "provider fleet must contain 1..={MAX_ACTIONS} entries"
        )));
    }
    let mut ordered = action.providers.clone();
    ordered.sort();
    if ordered != action.providers
        || action
            .providers
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
    {
        return Err(AdmissionError::Invalid(
            "provider fleet entries must be uniquely identity-ordered".to_owned(),
        ));
    }
    for entry in &action.providers {
        validate_provider_entry(entry)?;
    }
    validate_sha256("provider fleet sha256", &action.providers_sha256)?;
    let observed = provider_entries_sha256(&action.providers)?;
    if action.providers_sha256 != observed {
        return Err(AdmissionError::Invalid(format!(
            "provider fleet digest mismatch: expected {observed}"
        )));
    }
    Ok(())
}

fn validate_provider_entry(entry: &ProviderFleetEntry) -> AdmissionResult<()> {
    validate_component("provider identity", &entry.identity)?;
    validate_component("provider host", entry.host.as_str())?;
    validate_component("provider", &entry.provider)?;
    validate_component("provider account", &entry.account)?;
    validate_component("provider persona", &entry.persona)?;
    validate_component("runtime generation id", &entry.runtime_generation_id)?;
    validate_component("Axe launch generation id", &entry.launch_generation_id)?;
    if !entry.workspace.is_absolute()
        || entry.workspace.as_os_str().as_encoded_bytes().len() > MAX_ARG_BYTES
    {
        return Err(AdmissionError::Invalid(
            "provider workspace must be an absolute bounded path".to_owned(),
        ));
    }
    validate_prompt_authority(&entry.workspace, &entry.prompt)?;
    if entry.canonical_argv.is_empty() || entry.canonical_argv.len() > MAX_ARGV_ITEMS {
        return Err(AdmissionError::Invalid(format!(
            "provider canonical argv must contain 1..={MAX_ARGV_ITEMS} items"
        )));
    }
    for argument in &entry.canonical_argv {
        if argument.as_bytes().len() > MAX_ARG_BYTES || argument.as_bytes().contains(&0) {
            return Err(AdmissionError::Invalid(format!(
                "provider argv items must contain at most {MAX_ARG_BYTES} bytes and no NUL"
            )));
        }
    }
    validate_sha256("provider argv sha256", &entry.argv_sha256)?;
    let observed_argv_sha256 = candidate_argv_sha256(&entry.canonical_argv);
    if entry.argv_sha256 != observed_argv_sha256 {
        return Err(AdmissionError::Invalid(format!(
            "provider argv digest does not match canonical argv: expected {observed_argv_sha256}"
        )));
    }
    validate_sha256("provider profile sha256", &entry.profile_sha256)?;
    validate_sha256("provider trajectory sha256", &entry.trajectory_sha256)?;
    for (label, value) in [
        ("provider harness", &entry.harness),
        ("provider model", &entry.model),
        ("provider effort", &entry.effort),
        ("provider mode", &entry.mode),
        ("provider boot contract", &entry.boot_contract),
    ] {
        if value.is_empty() || value.len() > MAX_ID_BYTES {
            return Err(AdmissionError::Invalid(format!(
                "{label} must contain 1..={MAX_ID_BYTES} bytes"
            )));
        }
    }
    let expected_trajectory = provider_trajectory_sha256(entry)?;
    if entry.trajectory_sha256 != expected_trajectory {
        return Err(AdmissionError::Invalid(format!(
            "provider trajectory digest mismatch: expected {expected_trajectory}"
        )));
    }
    Ok(())
}

fn validate_prompt_authority(
    workspace: &Path,
    prompt: &LaunchPromptAuthority,
) -> AdmissionResult<()> {
    for (label, path) in [
        ("runtime profile", &prompt.runtime_profile_path),
        ("persona prompt", &prompt.persona_prompt_path),
        ("Axe launch receipt", &prompt.launch_receipt_path),
    ] {
        if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > MAX_ARG_BYTES {
            return Err(AdmissionError::Invalid(format!(
                "{label} path must be absolute and bounded"
            )));
        }
    }
    for (label, digest) in [
        ("runtime profile sha256", &prompt.runtime_profile_sha256),
        ("persona prompt sha256", &prompt.persona_prompt_sha256),
        ("Axe launch receipt sha256", &prompt.launch_receipt_sha256),
    ] {
        validate_sha256(label, digest)?;
    }
    let legacy = workspace.join(".st2").join("PERSONA.md");
    if prompt.persona_prompt_path == legacy
        || prompt.launch_receipt_path == legacy
        || prompt.runtime_profile_path.starts_with(workspace)
        || prompt.persona_prompt_path.starts_with(workspace)
        || prompt.launch_receipt_path.starts_with(workspace)
    {
        return Err(AdmissionError::Invalid(
            "launch prompt authority must be external to the workspace and never use workspace/.st2/PERSONA.md".to_owned(),
        ));
    }
    Ok(())
}

pub fn candidate_argv_sha256(argv: &[String]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.candidate-argv.v1\0");
    hash.update((argv.len() as u64).to_be_bytes());
    for argument in argv {
        hash.update((argument.len() as u64).to_be_bytes());
        hash.update(argument.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

pub fn provider_entries_sha256(entries: &[ProviderFleetEntry]) -> AdmissionResult<String> {
    let bytes = canonical_json(entries)?;
    let mut hash = Sha256::new();
    hash.update(b"st2.cutover-provider-fleet.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

pub fn provider_launch_receipts_sha256(entries: &[ProviderFleetEntry]) -> AdmissionResult<String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LaunchReceiptBinding<'a> {
        identity: &'a str,
        path: &'a Path,
        sha256: &'a str,
    }
    let bindings = entries
        .iter()
        .map(|entry| LaunchReceiptBinding {
            identity: &entry.identity,
            path: &entry.prompt.launch_receipt_path,
            sha256: &entry.prompt.launch_receipt_sha256,
        })
        .collect::<Vec<_>>();
    let bytes = canonical_json(&bindings)?;
    let mut hash = Sha256::new();
    hash.update(b"st2.cutover-provider-launch-receipts.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

pub fn provider_trajectory_sha256(entry: &ProviderFleetEntry) -> AdmissionResult<String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Trajectory<'a> {
        provider: &'a str,
        harness: &'a str,
        model: &'a str,
        effort: &'a str,
        persona: &'a str,
        mode: &'a str,
        boot_contract: &'a str,
    }
    let bytes = serde_json::to_vec(&Trajectory {
        provider: &entry.provider,
        harness: &entry.harness,
        model: &entry.model,
        effort: &entry.effort,
        persona: &entry.persona,
        mode: &entry.mode,
        boot_contract: &entry.boot_contract,
    })
    .map_err(|error| {
        AdmissionError::Invalid(format!("serialize Axe provider trajectory: {error}"))
    })?;
    let mut hash = Sha256::new();
    hash.update(b"axe.agent-launch-trajectory.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn validate_provider_inventory_from_catalog(
    catalog: &CanonicalCatalog,
    host: &HostId,
    action: &ProviderFleetProofAction,
    expected_catalog_sha256: &str,
) -> AdmissionResult<()> {
    let before = declaration_root_sha256_locked(catalog.as_path()).map_err(|error| {
        AdmissionError::Invalid(format!("compute provider proof catalog digest: {error:#}"))
    })?;
    if before != expected_catalog_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "provider proof catalog digest mismatch: expected {expected_catalog_sha256}, found {before}"
        )));
    }
    let found = crate::discover(catalog.as_path());
    if !found.errors.is_empty() {
        return Err(AdmissionError::Invalid(format!(
            "provider catalog discovery is not exact: {}",
            found
                .errors
                .iter()
                .map(|error| format!("{}: {}", error.path.display(), error.message))
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
    let authored = found
        .specs
        .iter()
        .filter(|spec| spec.resolved_host(host.as_str()) == host.as_str() && !spec.retired)
        .flat_map(|spec| {
            spec.tasks
                .iter()
                .filter(|task| {
                    task.name == "agent"
                        && !task.derived
                        && task.kind == TaskKind::Pty
                        && task.lifecycle == TaskLifecycle::AdoptOnly
                })
                .map(move |task| (spec, task))
        })
        .collect::<Vec<_>>();
    if authored.len() != action.providers.len() {
        return Err(AdmissionError::Conflict(format!(
            "authored local provider inventory mismatch: catalog has {}, action has {}",
            authored.len(),
            action.providers.len()
        )));
    }
    for entry in &action.providers {
        let matches = authored
            .iter()
            .filter(|(spec, _)| spec.identity == entry.identity)
            .collect::<Vec<_>>();
        let [(spec, task)] = matches.as_slice() else {
            return Err(AdmissionError::Conflict(format!(
                "provider identity {:?} is missing or non-unique in authored local inventory",
                entry.identity
            )));
        };
        if entry.host != *host {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} host does not match transaction host",
                entry.identity
            )));
        }
        let spec_dir = spec.path.parent().unwrap_or_else(|| Path::new("."));
        let workspace = spec
            .workspace
            .as_deref()
            .map(|workspace| {
                spec_dir.join(crate::expand::expand_catalog(workspace, catalog.as_path()))
            })
            .ok_or_else(|| {
                AdmissionError::Conflict(format!(
                    "provider {:?} has no authored workspace",
                    entry.identity
                ))
            })?;
        if workspace != entry.workspace {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} workspace mismatch",
                entry.identity
            )));
        }
        let argv = match (&task.command, &task.argv) {
            (None, Some(argv)) => argv
                .iter()
                .map(|argument| crate::expand::expand_catalog(argument, catalog.as_path()))
                .collect::<Vec<_>>(),
            _ => {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} must have one structured argv",
                    entry.identity
                )));
            }
        };
        if argv != entry.canonical_argv {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} argv mismatch",
                entry.identity
            )));
        }
        for (flag, expected) in [
            ("--persona", entry.persona.as_str()),
            ("--harness", entry.harness.as_str()),
            ("--model", entry.model.as_str()),
            ("--effort", entry.effort.as_str()),
            ("--mode", entry.mode.as_str()),
            ("--boot", entry.boot_contract.as_str()),
        ] {
            let observed = exact_argv_axis(&argv, flag)?;
            if observed != expected {
                return Err(AdmissionError::Conflict(format!(
                    "provider {:?} {flag} mismatch: expected {expected:?}, found {observed:?}",
                    entry.identity
                )));
            }
        }
        let persona = task
            .env
            .get("AGENT_PERSONA")
            .map(|value| crate::expand::expand_catalog(value, catalog.as_path()));
        if persona.as_deref() != Some(entry.persona.as_str()) {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} AGENT_PERSONA mismatch",
                entry.identity
            )));
        }
        let profile = task
            .env
            .get("AGENT_RUNTIME_PROFILE")
            .map(|value| PathBuf::from(crate::expand::expand_catalog(value, catalog.as_path())))
            .ok_or_else(|| {
                AdmissionError::Conflict(format!(
                    "provider {:?} AGENT_RUNTIME_PROFILE is absent",
                    entry.identity
                ))
            })?;
        if profile != entry.prompt.runtime_profile_path {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} runtime profile path mismatch",
                entry.identity
            )));
        }
        let profile_sha256 = bounded_regular_file_sha256(&profile, "provider runtime profile")?;
        if profile_sha256 != entry.profile_sha256
            || profile_sha256 != entry.prompt.runtime_profile_sha256
        {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} runtime profile digest mismatch",
                entry.identity
            )));
        }
        validate_legacy_prompt_absence(spec, task, &entry.workspace)?;
    }
    let after = declaration_root_sha256_locked(catalog.as_path()).map_err(|error| {
        AdmissionError::Invalid(format!(
            "recompute provider proof catalog digest: {error:#}"
        ))
    })?;
    if after != before {
        return Err(AdmissionError::Conflict(
            "provider declaration snapshot changed during proof".to_owned(),
        ));
    }
    Ok(())
}

fn validate_legacy_prompt_absence(
    spec: &agent_spec::spec::AgentSpec,
    task: &agent_spec::spec::Task,
    workspace: &Path,
) -> AdmissionResult<()> {
    let legacy = workspace.join(".st2").join("PERSONA.md");
    match fs::symlink_metadata(&legacy) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AdmissionError::io(
                format!("inspect legacy workspace prompt {}", legacy.display()),
                error,
            ));
        }
        Ok(_) => {
            return Err(AdmissionError::Conflict(format!(
                "legacy workspace prompt exists at {}",
                legacy.display()
            )));
        }
    }
    let declaration = fs::read(&spec.path).map_err(|error| {
        AdmissionError::io(
            format!("read provider declaration {}", spec.path.display()),
            error,
        )
    })?;
    if declaration.len() > MAX_RECEIPT_BYTES {
        return Err(AdmissionError::Invalid(format!(
            "provider declaration exceeds {MAX_RECEIPT_BYTES} byte proof bound"
        )));
    }
    let legacy_relative = b".st2/PERSONA.md";
    if declaration
        .windows(legacy_relative.len())
        .any(|window| window == legacy_relative)
        || task
            .argv
            .iter()
            .flatten()
            .any(|argument| argument.contains(".st2/PERSONA.md"))
        || task
            .command
            .as_deref()
            .is_some_and(|command| command.contains(".st2/PERSONA.md"))
        || task.env.iter().any(|(key, value)| {
            key.contains(".st2/PERSONA.md") || value.contains(".st2/PERSONA.md")
        })
    {
        return Err(AdmissionError::Conflict(format!(
            "provider {:?} declaration or launch retains the workspace PERSONA loader",
            spec.identity
        )));
    }
    Ok(())
}

fn exact_argv_axis<'a>(argv: &'a [String], flag: &str) -> AdmissionResult<&'a str> {
    let inline = format!("{flag}=");
    let mut values = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if argv[index] == flag {
            let value = argv.get(index + 1).ok_or_else(|| {
                AdmissionError::Invalid(format!("provider argv {flag} has no value"))
            })?;
            if value.starts_with("--") {
                return Err(AdmissionError::Invalid(format!(
                    "provider argv {flag} has no value"
                )));
            }
            values.push(value.as_str());
            index += 2;
            continue;
        }
        if let Some(value) = argv[index].strip_prefix(&inline) {
            if value.is_empty() {
                return Err(AdmissionError::Invalid(format!(
                    "provider argv {flag} has no value"
                )));
            }
            values.push(value);
        }
        index += 1;
    }
    match values.as_slice() {
        [value] => Ok(*value),
        [] => Err(AdmissionError::Invalid(format!(
            "provider argv omits {flag}"
        ))),
        _ => Err(AdmissionError::Invalid(format!(
            "provider argv repeats {flag}"
        ))),
    }
}

fn bounded_regular_file_sha256(path: &Path, label: &str) -> AdmissionResult<String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AdmissionError::io(format!("inspect {label} {}", path.display()), error)
    })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_RECEIPT_BYTES as u64
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be a real regular file of at most {MAX_RECEIPT_BYTES} bytes"
        )));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| AdmissionError::io(format!("open {label} {}", path.display()), error))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_RECEIPT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| AdmissionError::io(format!("read {label} {}", path.display()), error))?;
    if bytes.len() > MAX_RECEIPT_BYTES {
        return Err(AdmissionError::Invalid(format!(
            "{label} exceeds size bound"
        )));
    }
    Ok(sha256_bytes(&bytes))
}

fn validate_provider_snapshot(
    action: &ProviderFleetProofAction,
    ding: &DingReconcileAction,
    gate_id: &GateId,
    ding_action_index: usize,
    snapshot: &ProviderFleetSnapshot,
) -> AdmissionResult<()> {
    if snapshot.authored_providers.len() != action.providers.len() {
        return Err(AdmissionError::Conflict(
            "provider observation is missing or has extra authored tasks".to_owned(),
        ));
    }
    for entry in &action.providers {
        let matches = snapshot
            .authored_providers
            .iter()
            .filter(|observed| observed.identity == entry.identity)
            .collect::<Vec<_>>();
        let [observed] = matches.as_slice() else {
            return Err(AdmissionError::Conflict(format!(
                "provider observation for {:?} is missing or duplicated",
                entry.identity
            )));
        };
        if observed.status != ProviderTaskStatus::Running
            || observed.runtime_generation_id.as_deref()
                != Some(entry.runtime_generation_id.as_str())
            || observed.prompt.as_ref() != Some(&entry.prompt)
        {
            return Err(AdmissionError::Conflict(format!(
                "provider {:?} is not the exact running launch-scoped trajectory",
                entry.identity
            )));
        }
    }
    if snapshot.successor_dings.len() != ding.desired.len() {
        return Err(AdmissionError::Conflict(
            "successor Ding observation is not a bijection with DingReconcile authority".to_owned(),
        ));
    }
    for desired in &ding.desired {
        let matches = snapshot
            .successor_dings
            .iter()
            .filter(|observed| match observed {
                SuccessorDingObservation::Absent { runtime_id }
                | SuccessorDingObservation::JournalBound { runtime_id, .. } => {
                    runtime_id == &desired.runtime_id
                }
            })
            .collect::<Vec<_>>();
        let [observed] = matches.as_slice() else {
            return Err(AdmissionError::Conflict(format!(
                "successor Ding {:?} is missing or duplicated",
                desired.runtime_id
            )));
        };
        if let SuccessorDingObservation::JournalBound {
            gate_id: observed_gate,
            action_index: observed_index,
            ding_generation_id,
            launch_sha256,
            journal_sha256,
            ..
        } = observed
        {
            validate_sha256("successor Ding journal sha256", journal_sha256)?;
            if observed_gate != gate_id
                || *observed_index != ding_action_index
                || ding_generation_id != &ding.generation_id
                || launch_sha256 != &desired.launch_sha256
            {
                return Err(AdmissionError::Conflict(format!(
                    "successor Ding {:?} is not exact journal-bound authority",
                    desired.runtime_id
                )));
            }
        }
    }
    Ok(())
}

fn successor_ding_partition_sha256(
    observations: &[SuccessorDingObservation],
) -> AdmissionResult<String> {
    let mut ordered = observations.to_vec();
    ordered.sort_by(|left, right| successor_ding_id(left).cmp(successor_ding_id(right)));
    let bytes = canonical_json(&ordered)?;
    let mut hash = Sha256::new();
    hash.update(b"st2.successor-ding-partition.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn provider_fleet_result_sha256(snapshot: &ProviderFleetSnapshot) -> AdmissionResult<String> {
    let mut canonical = snapshot.clone();
    canonical
        .authored_providers
        .sort_by(|left, right| left.identity.cmp(&right.identity));
    canonical
        .successor_dings
        .sort_by(|left, right| successor_ding_id(left).cmp(successor_ding_id(right)));
    let bytes = canonical_json(&canonical)?;
    let mut hash = Sha256::new();
    hash.update(b"st2.provider-fleet-proof-result.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn successor_ding_id(observation: &SuccessorDingObservation) -> &str {
    match observation {
        SuccessorDingObservation::Absent { runtime_id }
        | SuccessorDingObservation::JournalBound { runtime_id, .. } => runtime_id,
    }
}

pub(crate) fn validate_predecessor_retirement_evidence(
    host: &HostId,
    source_catalog_sha256: &str,
    evidence: &PredecessorRetirementEvidence,
) -> AdmissionResult<()> {
    if evidence.schema != PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA || &evidence.host != host {
        return Err(AdmissionError::Invalid(
            "predecessor retirement evidence has wrong schema or host authority".to_owned(),
        ));
    }
    for (label, digest) in [
        (
            "predecessor retirement receipt sha256",
            &evidence.receipt_sha256,
        ),
        ("predecessor retirement plan sha256", &evidence.plan_sha256),
        (
            "predecessor retirement catalog sha256",
            &evidence.catalog_sha256,
        ),
        (
            "predecessor retirement census sha256",
            &evidence.census_sha256,
        ),
        (
            "predecessor retirement journal sha256",
            &evidence.journal_sha256,
        ),
        (
            "predecessor retirement legacy partition sha256",
            &evidence.legacy_partition_sha256,
        ),
    ] {
        validate_sha256(label, digest)?;
    }
    if evidence.catalog_sha256 != source_catalog_sha256 {
        return Err(AdmissionError::Conflict(
            "predecessor retirement receipt does not bind the source catalog digest".to_owned(),
        ));
    }
    if evidence.legacy_partition.len() > 4096 {
        return Err(AdmissionError::Invalid(
            "predecessor retirement partition exceeds the marker bound".to_owned(),
        ));
    }
    let mut previous = None;
    for ding in &evidence.legacy_partition {
        if ding.agent.is_empty()
            || ding.agent.len() > MAX_ID_BYTES
            || ding.runtime_id.len() > MAX_ID_BYTES
            || ding.runtime_id != format!("{}.ding", ding.agent)
            || !ding.agent.starts_with(&format!("{}.", host.as_str()))
            || !ding
                .agent
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(AdmissionError::Invalid(
                "predecessor retirement partition contains a non-local or non-Ding identity"
                    .to_owned(),
            ));
        }
        if previous.is_some_and(|prior: &str| prior >= ding.runtime_id.as_str()) {
            return Err(AdmissionError::Invalid(
                "predecessor retirement partition must be strictly ordered and unique".to_owned(),
            ));
        }
        previous = Some(ding.runtime_id.as_str());
    }
    Ok(())
}

fn parse_external_receipt(
    catalog: &CanonicalCatalog,
    marker: &CutoverMarker,
    expected_index: usize,
    expected_kind: ExternalCheckpointKind,
    expected_input_sha256: &str,
    receipt_bytes: &[u8],
) -> AdmissionResult<ExternalCheckpointEvidence> {
    if receipt_bytes.is_empty() || receipt_bytes.len() > MAX_RECEIPT_BYTES {
        return Err(AdmissionError::Invalid(format!(
            "external checkpoint receipt must contain 1..={MAX_RECEIPT_BYTES} bytes"
        )));
    }
    let receipt: ExternalCheckpointReceipt =
        serde_json::from_slice(receipt_bytes).map_err(|error| {
            AdmissionError::Invalid(format!("parse external checkpoint receipt: {error}"))
        })?;
    if canonical_json(&receipt)? != receipt_bytes {
        return Err(AdmissionError::Invalid(
            "external checkpoint receipt bytes are not canonical JSON".to_owned(),
        ));
    }
    validate_external_receipt(
        catalog,
        marker,
        expected_index,
        expected_kind,
        expected_input_sha256,
        &receipt,
    )?;
    Ok(ExternalCheckpointEvidence {
        receipt_sha256: sha256_bytes(receipt_bytes),
        receipt,
    })
}

fn validate_external_receipt(
    catalog: &CanonicalCatalog,
    marker: &CutoverMarker,
    expected_index: usize,
    expected_kind: ExternalCheckpointKind,
    expected_input_sha256: &str,
    receipt: &ExternalCheckpointReceipt,
) -> AdmissionResult<()> {
    if receipt.schema != EXTERNAL_CHECKPOINT_EVIDENCE_SCHEMA
        || receipt.canonical_catalog != catalog.as_path()
        || (receipt.catalog_device, receipt.catalog_inode)
            != (marker.catalog_device, marker.catalog_inode)
        || receipt.host != marker.host
        || receipt.gate_id != marker.gate_id
        || receipt.request_sha256 != marker.request_sha256
        || receipt.action_index != expected_index
        || receipt.kind != expected_kind
        || receipt.input_sha256 != expected_input_sha256
    {
        return Err(AdmissionError::Invalid(
            "external checkpoint receipt does not match the exact catalog incarnation, transaction, and action"
                .to_owned(),
        ));
    }
    validate_sha256(
        "external checkpoint receipt request sha256",
        &receipt.request_sha256,
    )?;
    validate_sha256(
        "external checkpoint receipt input sha256",
        &receipt.input_sha256,
    )?;
    match (&receipt.kind, &receipt.payload) {
        (
            ExternalCheckpointKind::Cleanup,
            ExternalCheckpointPayload::Cleanup {
                manifest_sha256,
                result_sha256,
            },
        ) => {
            validate_sha256("cleanup manifest sha256", manifest_sha256)?;
            validate_sha256("cleanup result sha256", result_sha256)
        }
        (
            ExternalCheckpointKind::FinalProof,
            ExternalCheckpointPayload::FinalProof {
                final_catalog_sha256,
                providers_sha256,
                launch_receipts_sha256,
                ding_partition_sha256,
                ding_reconcile_sha256,
                validation_sha256,
                runtime_inventory_sha256,
            },
        ) => {
            validate_sha256("final proof catalog sha256", final_catalog_sha256)?;
            validate_sha256("final proof provider fleet sha256", providers_sha256)?;
            validate_sha256("final proof launch receipts sha256", launch_receipts_sha256)?;
            validate_sha256("final proof Ding partition sha256", ding_partition_sha256)?;
            validate_sha256("final proof Ding reconcile sha256", ding_reconcile_sha256)?;
            validate_sha256("final proof validation sha256", validation_sha256)?;
            validate_sha256(
                "final proof runtime inventory sha256",
                runtime_inventory_sha256,
            )?;
            let expected_catalog = catalog_digest_at_cursor(marker, expected_index);
            let fleet = marker.provider_fleet_proof.as_ref().ok_or_else(|| {
                AdmissionError::Conflict(
                    "final proof requires durable provider fleet evidence".to_owned(),
                )
            })?;
            let provider_action = marker
                .program
                .iter()
                .find_map(|action| match action {
                    CutoverAction::ProviderFleetProof(action) => Some(action),
                    _ => None,
                })
                .ok_or_else(|| {
                    AdmissionError::Invalid(
                        "final proof program has no provider fleet action".to_owned(),
                    )
                })?;
            let expected_launch_receipts =
                provider_launch_receipts_sha256(&provider_action.providers)?;
            let [ding] = marker.completed_ding_reconciles.as_slice() else {
                return Err(AdmissionError::Conflict(
                    "final proof requires exactly one durable Ding reconciliation".to_owned(),
                ));
            };
            let expected_ding_reconcile = sha256_bytes(&canonical_json(&ding.receipt)?);
            if final_catalog_sha256 != &expected_catalog
                || providers_sha256 != &fleet.providers_sha256
                || launch_receipts_sha256 != &expected_launch_receipts
                || launch_receipts_sha256 != &fleet.launch_receipts_sha256
                || ding_partition_sha256 != &fleet.ding_partition_sha256
                || ding_reconcile_sha256 != &expected_ding_reconcile
                || runtime_inventory_sha256 != &fleet.result_sha256
            {
                return Err(AdmissionError::Conflict(
                    "final proof does not bind the exact final catalog, full provider fleet, launch receipts, and Ding set"
                        .to_owned(),
                ));
            }
            Ok(())
        }
        (
            ExternalCheckpointKind::BusContinuity,
            ExternalCheckpointPayload::BusContinuity {
                bus_id,
                probe_sha256,
            },
        ) => {
            validate_component("bus id", bus_id)?;
            validate_sha256("bus continuity probe sha256", probe_sha256)
        }
        _ => Err(AdmissionError::Invalid(
            "external checkpoint receipt payload does not match its action kind".to_owned(),
        )),
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_provider_fleet_proof(
    action: &ProviderFleetProofAction,
    evidence: &ProviderFleetProofEvidence,
) -> AdmissionResult<()> {
    if evidence.schema != PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA
        || evidence.providers_sha256 != action.providers_sha256
    {
        return Err(AdmissionError::Invalid(
            "provider fleet proof does not bind the immutable fleet".to_owned(),
        ));
    }
    validate_sha256(
        "provider fleet proof provider digest",
        &evidence.providers_sha256,
    )?;
    validate_sha256(
        "provider fleet proof Ding partition digest",
        &evidence.ding_partition_sha256,
    )?;
    validate_sha256(
        "provider fleet proof result sha256",
        &evidence.result_sha256,
    )
}

fn validate_marker(
    catalog: &CanonicalCatalog,
    address_host: &HostId,
    marker: &CutoverMarker,
) -> AdmissionResult<()> {
    if marker.schema != CUTOVER_TRANSACTION_SCHEMA
        || marker.canonical_catalog != catalog.as_path()
        || &marker.host != address_host
    {
        return Err(AdmissionError::Invalid(
            "cutover marker schema or canonical authority mismatch".to_owned(),
        ));
    }
    let root_identity = path_identity(catalog.as_path())?;
    if root_identity != (marker.catalog_device, marker.catalog_inode) {
        return Err(AdmissionError::Conflict(
            "cutover marker belongs to a different catalog incarnation".to_owned(),
        ));
    }
    HostId::parse(marker.host.as_str().to_owned())?;
    GateId::parse(marker.gate_id.as_str().to_owned())?;
    validate_sha256("request sha256", &marker.request_sha256)?;
    validate_sha256("source catalog sha256", &marker.source_catalog_sha256)?;
    validate_program(&marker.source_catalog_sha256, &marker.program)?;
    if marker.cursor > marker.program.len() {
        return Err(AdmissionError::Invalid(
            "action cursor exceeds immutable program".to_owned(),
        ));
    }
    validate_predecessor_retirement_evidence(
        &marker.host,
        &marker.source_catalog_sha256,
        &marker.predecessor_retirement,
    )?;
    for checkpoint in &marker.completed_checkpoints {
        if checkpoint.action_index >= marker.cursor {
            return Err(AdmissionError::Invalid(
                "checkpoint evidence lies at or after the action cursor".to_owned(),
            ));
        }
        let (kind, input_sha256) = match marker.program.get(checkpoint.action_index) {
            Some(CutoverAction::ExternalCheckpoint { kind, input_sha256 }) => {
                (*kind, input_sha256.as_str())
            }
            _ => {
                return Err(AdmissionError::Invalid(
                    "checkpoint evidence does not address a checkpoint action".to_owned(),
                ));
            }
        };
        validate_sha256(
            "external checkpoint receipt sha256",
            &checkpoint.evidence.receipt_sha256,
        )?;
        validate_external_receipt(
            catalog,
            marker,
            checkpoint.action_index,
            kind,
            input_sha256,
            &checkpoint.evidence.receipt,
        )?;
        let receipt_bytes = canonical_json(&checkpoint.evidence.receipt)?;
        if checkpoint.evidence.receipt_sha256 != sha256_bytes(&receipt_bytes) {
            return Err(AdmissionError::Invalid(
                "stored external checkpoint receipt sha256 does not match its canonical bytes"
                    .to_owned(),
            ));
        }
    }
    let completed_checkpoint_count = marker.program[..marker.cursor]
        .iter()
        .filter(|action| matches!(action, CutoverAction::ExternalCheckpoint { .. }))
        .count();
    if marker.completed_checkpoints.len() != completed_checkpoint_count {
        return Err(AdmissionError::Invalid(
            "checkpoint evidence is not complete and one-to-one with the cursor".to_owned(),
        ));
    }
    for completed in &marker.completed_ding_reconciles {
        if completed.action_index >= marker.cursor {
            return Err(AdmissionError::Invalid(
                "Ding reconciliation evidence lies at or after the action cursor".to_owned(),
            ));
        }
        let action = match marker.program.get(completed.action_index) {
            Some(CutoverAction::DingReconcile(action)) => action,
            _ => {
                return Err(AdmissionError::Invalid(
                    "Ding reconciliation evidence does not address a Ding action".to_owned(),
                ));
            }
        };
        ding_reconcile::validate_receipt(
            action,
            marker.gate_id.as_str(),
            completed.action_index,
            &completed.receipt,
        )?;
    }
    let completed_ding_count = marker.program[..marker.cursor]
        .iter()
        .filter(|action| matches!(action, CutoverAction::DingReconcile(_)))
        .count();
    if marker.completed_ding_reconciles.len() != completed_ding_count {
        return Err(AdmissionError::Invalid(
            "Ding reconciliation evidence is not complete and one-to-one with the cursor"
                .to_owned(),
        ));
    }
    let adoption_index = marker
        .program
        .iter()
        .position(|action| matches!(action, CutoverAction::ProviderFleetProof(_)))
        .expect("validated program has a provider fleet proof");
    let adoption_action = match &marker.program[adoption_index] {
        CutoverAction::ProviderFleetProof(action) => action,
        _ => unreachable!(),
    };
    if let Some(proof) = &marker.provider_fleet_proof {
        validate_provider_fleet_proof(adoption_action, proof)?;
        if marker.cursor <= adoption_index {
            return Err(AdmissionError::Invalid(
                "provider fleet proof exists before cursor completion".to_owned(),
            ));
        }
    } else if marker.cursor > adoption_index {
        return Err(AdmissionError::Invalid(
            "cursor passed provider fleet proof without evidence".to_owned(),
        ));
    }
    if marker.finalized
        && (marker.cursor != marker.program.len() || marker.provider_fleet_proof.is_none())
    {
        return Err(AdmissionError::Invalid(
            "finalized marker is not completely successful".to_owned(),
        ));
    }
    Ok(())
}

fn catalog_digest_at_cursor(marker: &CutoverMarker, cursor: usize) -> String {
    let mut digest = marker.source_catalog_sha256.clone();
    for action in &marker.program[..cursor] {
        if let CutoverAction::CatalogTransition(transition) = action {
            digest.clone_from(&transition.after_sha256);
        }
    }
    digest
}

fn validate_resume_catalog_digest(
    catalog: &CanonicalCatalog,
    marker: &CutoverMarker,
) -> AdmissionResult<()> {
    let implied = catalog_digest_at_cursor(marker, marker.cursor);
    let crash_after = match marker.program.get(marker.cursor) {
        Some(CutoverAction::CatalogTransition(transition)) => {
            Some(transition.after_sha256.as_str())
        }
        _ => None,
    };
    let observed = declaration_root_sha256_locked(catalog.as_path()).map_err(|error| {
        AdmissionError::Invalid(format!(
            "compute declaration-root digest while resuming cutover: {error:#}"
        ))
    })?;
    if observed != implied && crash_after != Some(observed.as_str()) {
        return Err(AdmissionError::Conflict(format!(
            "resume catalog digest is neither cursor-implied `{implied}` nor the exact uncompleted transition result; found `{observed}`"
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, digest: &str) -> AdmissionResult<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn read_marker(catalog: &CanonicalCatalog, path: &Path) -> AdmissionResult<CutoverMarker> {
    read_marker_with_bytes(catalog, path).map(|(marker, _)| marker)
}

fn read_marker_with_bytes(
    catalog: &CanonicalCatalog,
    path: &Path,
) -> AdmissionResult<(CutoverMarker, Vec<u8>)> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AdmissionError::io(format!("inspect cutover marker {}", path.display()), error)
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AdmissionError::Invalid(format!(
            "cutover marker is not a real regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > MAX_MARKER_BYTES {
        return Err(AdmissionError::Invalid(format!(
            "cutover marker exceeds {MAX_MARKER_BYTES} bytes: {}",
            path.display()
        )));
    }
    let file = File::open(path).map_err(|error| {
        AdmissionError::io(format!("read cutover marker {}", path.display()), error)
    })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            AdmissionError::io(format!("read cutover marker {}", path.display()), error)
        })?;
    if bytes.len() as u64 > MAX_MARKER_BYTES {
        return Err(AdmissionError::Invalid(format!(
            "cutover marker exceeds {MAX_MARKER_BYTES} bytes: {}",
            path.display()
        )));
    }
    let marker: CutoverMarker = serde_json::from_slice(&bytes).map_err(|error| {
        AdmissionError::Invalid(format!("parse cutover marker {}: {error}", path.display()))
    })?;
    if marker.canonical_catalog != catalog.as_path() {
        return Err(AdmissionError::Invalid(format!(
            "cutover marker catalog mismatch at {}",
            path.display()
        )));
    }
    Ok((marker, bytes))
}

fn publish_create_only(
    temp_parent: &Path,
    target: &Path,
    marker: &CutoverMarker,
) -> AdmissionResult<Vec<u8>> {
    let bytes = canonical_json(marker)?;
    publish_bytes_create_only(temp_parent, target, &bytes)?;
    Ok(bytes)
}

/// Publish or verify the immutable finalized record before active authority can be finalized.
///
/// Create-only publication makes concurrent or stale history fail closed. Existing history is
/// accepted only when its validated marker bytes are exactly identical.
fn ensure_exact_finalized_history(
    catalog: &CanonicalCatalog,
    history_path: &Path,
    finalized_bytes: &[u8],
) -> AdmissionResult<()> {
    match fs::symlink_metadata(history_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => publish_bytes_create_only(
            history_path
                .parent()
                .ok_or_else(|| AdmissionError::Invalid("history path has no parent".to_owned()))?,
            history_path,
            finalized_bytes,
        ),
        Ok(_) => {
            let (marker, bytes) = read_marker_with_bytes(catalog, history_path)?;
            if !marker.finalized || bytes != finalized_bytes {
                return Err(AdmissionError::Conflict(format!(
                    "cutover history collision at {}",
                    history_path.display()
                )));
            }
            Ok(())
        }
        Err(error) => Err(AdmissionError::io(
            format!("inspect cutover history {}", history_path.display()),
            error,
        )),
    }
}

fn publish_bytes_create_only(
    temp_parent: &Path,
    target: &Path,
    bytes: &[u8],
) -> AdmissionResult<()> {
    let mut temp = tempfile::Builder::new()
        .prefix(".cutover-marker-")
        .tempfile_in(temp_parent)
        .map_err(|error| AdmissionError::io("create cutover marker stage", error))?;
    temp.as_file_mut()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| AdmissionError::io("set cutover marker permissions", error))?;
    temp.write_all(bytes)
        .map_err(|error| AdmissionError::io("write cutover marker stage", error))?;
    temp.as_file()
        .sync_all()
        .map_err(|error| AdmissionError::io("sync cutover marker stage", error))?;
    fs::hard_link(temp.path(), target).map_err(|error| {
        AdmissionError::io(
            format!("publish create-only cutover marker {}", target.display()),
            error,
        )
    })?;
    sync_dir(
        target.parent().ok_or_else(|| {
            AdmissionError::Invalid("cutover marker target has no parent".to_owned())
        })?,
    )
}

fn replace_durable(
    retained_stage_parent: &File,
    stage_parent: &Path,
    target: &Path,
    marker: &CutoverMarker,
) -> AdmissionResult<Vec<u8>> {
    let bytes = canonical_json(marker)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".cutover-marker-update-")
        .tempfile_in(stage_parent)
        .map_err(|error| AdmissionError::io("create cutover marker update", error))?;
    temp.as_file_mut()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| AdmissionError::io("set cutover marker permissions", error))?;
    temp.write_all(&bytes)
        .map_err(|error| AdmissionError::io("write cutover marker update", error))?;
    temp.as_file()
        .sync_all()
        .map_err(|error| AdmissionError::io("sync cutover marker update", error))?;
    let source_name = temp
        .path()
        .file_name()
        .ok_or_else(|| AdmissionError::Invalid("marker stage has no filename".to_owned()))?;
    let target_name = target
        .file_name()
        .ok_or_else(|| AdmissionError::Invalid("marker target has no filename".to_owned()))?;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    let source_name = CString::new(source_name.as_bytes())
        .map_err(|_| AdmissionError::Invalid("marker stage filename contains NUL".to_owned()))?;
    let target_name = CString::new(target_name.as_bytes())
        .map_err(|_| AdmissionError::Invalid("marker target filename contains NUL".to_owned()))?;
    // SAFETY: both names are NUL-terminated and the retained directory descriptor remains valid.
    let result = unsafe {
        libc::renameat(
            retained_stage_parent.as_raw_fd(),
            source_name.as_ptr(),
            retained_stage_parent.as_raw_fd(),
            target_name.as_ptr(),
        )
    };
    if result != 0 {
        return Err(AdmissionError::io(
            "replace cutover marker through retained directory",
            std::io::Error::last_os_error(),
        ));
    }
    retained_stage_parent
        .sync_all()
        .map_err(|error| AdmissionError::io("sync retained cutover directory", error))?;
    Ok(bytes)
}

fn canonical_json<T: Serialize + ?Sized>(value: &T) -> AdmissionResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| AdmissionError::Invalid(format!("serialize cutover marker: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn sync_dir(path: &Path) -> AdmissionResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| AdmissionError::io(format!("sync directory {}", path.display()), error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    fn fixture() -> (tempfile::TempDir, CanonicalCatalog, HostId) {
        let root = tempfile::tempdir().unwrap();
        let catalog = CanonicalCatalog::open(root.path()).unwrap();
        let host = HostId::parse("test-host").unwrap();
        (root, catalog, host)
    }

    fn provider_entry(identity: &str) -> ProviderFleetEntry {
        let canonical_argv = vec![
            "axe".to_owned(),
            "agent".to_owned(),
            "exec".to_owned(),
            "--persona".to_owned(),
            "worker".to_owned(),
            "--harness".to_owned(),
            "codex".to_owned(),
            "--model".to_owned(),
            "gpt-5".to_owned(),
            "--effort".to_owned(),
            "high".to_owned(),
            "--mode".to_owned(),
            "interactive".to_owned(),
            "--boot".to_owned(),
            "fresh".to_owned(),
        ];
        let mut entry = ProviderFleetEntry {
            identity: identity.to_owned(),
            host: HostId::parse("test-host").unwrap(),
            provider: "openai".to_owned(),
            account: "account-2".to_owned(),
            persona: "worker".to_owned(),
            workspace: PathBuf::from("/work/catalog"),
            prompt: LaunchPromptAuthority {
                runtime_profile_path: PathBuf::from("/nix/store/profile.json"),
                runtime_profile_sha256: B.to_owned(),
                persona_prompt_path: PathBuf::from("/nix/store/personas/worker.md"),
                persona_prompt_sha256: C.to_owned(),
                launch_receipt_path: PathBuf::from("/run/st2/launch-receipts/worker.json"),
                launch_receipt_sha256: D.to_owned(),
                injection_kind: PromptInjectionKind::CodexDeveloperInstructions,
            },
            argv_sha256: candidate_argv_sha256(&canonical_argv),
            canonical_argv,
            profile_sha256: B.to_owned(),
            harness: "codex".to_owned(),
            model: "gpt-5".to_owned(),
            effort: "high".to_owned(),
            mode: "interactive".to_owned(),
            boot_contract: "fresh".to_owned(),
            launch_generation_id: format!("launch-{identity}"),
            runtime_generation_id: format!("generation-{identity}"),
            trajectory_sha256: String::new(),
        };
        entry.trajectory_sha256 = provider_trajectory_sha256(&entry).unwrap();
        entry
    }

    fn candidate() -> ProviderFleetProofAction {
        let providers = vec![provider_entry("dev3.worker-7")];
        ProviderFleetProofAction {
            providers_sha256: provider_entries_sha256(&providers).unwrap(),
            providers,
        }
    }

    fn ding_action() -> DingReconcileAction {
        let mut desired = crate::ding_reconcile::DingDesiredExec {
            runtime_id: "test-host.worker.ding".to_owned(),
            canonical_argv: vec!["st2".to_owned(), "ding".to_owned()],
            canonical_cwd: PathBuf::from("/work/catalog"),
            canonical_env: std::collections::BTreeMap::new(),
            launch_sha256: String::new(),
        };
        desired.launch_sha256 = crate::ding_reconcile::launch_sha256(&desired).unwrap();
        let desired = vec![desired];
        DingReconcileAction {
            generation_id: "ding-generation-7".to_owned(),
            desired_sha256: crate::ding_reconcile::desired_set_sha256(&desired).unwrap(),
            desired,
        }
    }

    fn program(source: &str, after: &str) -> Vec<CutoverAction> {
        vec![
            CutoverAction::CatalogTransition(CatalogTransition {
                before_sha256: source.to_owned(),
                after_sha256: after.to_owned(),
            }),
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::Cleanup,
                input_sha256: A.to_owned(),
            },
            CutoverAction::ProviderFleetProof(candidate()),
            CutoverAction::DingReconcile(ding_action()),
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::BusContinuity,
                input_sha256: C.to_owned(),
            },
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::FinalProof,
                input_sha256: B.to_owned(),
            },
        ]
    }

    fn digests(catalog: &CanonicalCatalog) -> (String, String) {
        let source = declaration_root_sha256_locked(catalog.as_path()).unwrap();
        let config = catalog.as_path().join(crate::catalog::CONFIG_FILE);
        fs::write(&config, b"candidate = true\n").unwrap();
        let after = declaration_root_sha256_locked(catalog.as_path()).unwrap();
        fs::remove_file(config).unwrap();
        (source, after)
    }

    fn begin(catalog: CanonicalCatalog, host: HostId) -> (CutoverTransaction, String, String) {
        let (source, after) = digests(&catalog);
        let outcome = CutoverTransaction::begin(BeginCutover {
            catalog,
            host: host.clone(),
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
            source_catalog_sha256: source.clone(),
            program: program(&source, &after),
            predecessor_retirement: predecessor_evidence(&source, &host),
        })
        .unwrap();
        let BeginOutcome::Claimed(transaction) = outcome else {
            panic!("host is free");
        };
        (transaction, source, after)
    }

    fn predecessor_evidence(catalog_digest: &str, host: &HostId) -> PredecessorRetirementEvidence {
        PredecessorRetirementEvidence {
            schema: PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA.to_owned(),
            receipt_sha256: A.to_owned(),
            plan_sha256: B.to_owned(),
            catalog_sha256: catalog_digest.to_owned(),
            host: host.clone(),
            census_sha256: C.to_owned(),
            journal_sha256: D.to_owned(),
            legacy_partition_sha256: C.to_owned(),
            legacy_partition: vec![PredecessorRetiredDing {
                runtime_id: format!("{}.legacy.ding", host.as_str()),
                agent: format!("{}.legacy", host.as_str()),
            }],
        }
    }

    fn checkpoint(
        cutover: &CutoverTransaction,
        action_index: usize,
        kind: ExternalCheckpointKind,
    ) -> Vec<u8> {
        let input_sha256 = match &cutover.marker.program[action_index] {
            CutoverAction::ExternalCheckpoint { input_sha256, .. } => input_sha256.clone(),
            _ => panic!("test checkpoint index"),
        };
        let payload = match kind {
            ExternalCheckpointKind::Cleanup => ExternalCheckpointPayload::Cleanup {
                manifest_sha256: C.to_owned(),
                result_sha256: D.to_owned(),
            },
            ExternalCheckpointKind::FinalProof => ExternalCheckpointPayload::FinalProof {
                final_catalog_sha256: declaration_root_sha256_locked(cutover.catalog.as_path())
                    .unwrap(),
                providers_sha256: cutover
                    .marker
                    .provider_fleet_proof
                    .as_ref()
                    .unwrap()
                    .providers_sha256
                    .clone(),
                launch_receipts_sha256: cutover
                    .marker
                    .provider_fleet_proof
                    .as_ref()
                    .unwrap()
                    .launch_receipts_sha256
                    .clone(),
                ding_partition_sha256: cutover
                    .marker
                    .provider_fleet_proof
                    .as_ref()
                    .unwrap()
                    .ding_partition_sha256
                    .clone(),
                ding_reconcile_sha256: sha256_bytes(
                    &canonical_json(
                        &cutover.marker.completed_ding_reconciles.as_slice()[0].receipt,
                    )
                    .unwrap(),
                ),
                validation_sha256: C.to_owned(),
                runtime_inventory_sha256: cutover
                    .marker
                    .provider_fleet_proof
                    .as_ref()
                    .unwrap()
                    .result_sha256
                    .clone(),
            },
            ExternalCheckpointKind::BusContinuity => ExternalCheckpointPayload::BusContinuity {
                bus_id: "catalog-bus".to_owned(),
                probe_sha256: D.to_owned(),
            },
        };
        canonical_json(&ExternalCheckpointReceipt {
            schema: EXTERNAL_CHECKPOINT_EVIDENCE_SCHEMA.to_owned(),
            canonical_catalog: cutover.marker.canonical_catalog.clone(),
            catalog_device: cutover.marker.catalog_device,
            catalog_inode: cutover.marker.catalog_inode,
            host: cutover.marker.host.clone(),
            gate_id: cutover.marker.gate_id.clone(),
            request_sha256: cutover.marker.request_sha256.clone(),
            action_index,
            kind,
            input_sha256,
            payload,
        })
        .unwrap()
    }

    fn complete_adoption_and_ding(cutover: &mut CutoverTransaction) {
        let adoption_index = cutover.marker.cursor;
        let adoption = match &cutover.marker.program[adoption_index] {
            CutoverAction::ProviderFleetProof(action) => action,
            _ => panic!("test adoption index"),
        };
        let mut adopted = cutover.marker.clone();
        adopted.provider_fleet_proof = Some(ProviderFleetProofEvidence {
            schema: PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA.to_owned(),
            providers_sha256: adoption.providers_sha256.clone(),
            launch_receipts_sha256: provider_launch_receipts_sha256(&adoption.providers).unwrap(),
            ding_partition_sha256: C.to_owned(),
            result_sha256: D.to_owned(),
        });
        adopted.cursor += 1;
        cutover.persist(adopted).unwrap();

        let ding_index = cutover.marker.cursor;
        let ding = match &cutover.marker.program[ding_index] {
            CutoverAction::DingReconcile(action) => action,
            _ => panic!("test Ding index"),
        };
        let runtime_ids = ding
            .desired
            .iter()
            .map(|desired| desired.runtime_id.clone())
            .collect::<Vec<_>>();
        let generation_ids = runtime_ids
            .iter()
            .enumerate()
            .map(|(index, _)| format!("exec-generation-{index}"))
            .collect::<Vec<_>>();
        let receipt = DingReconcileReceipt {
            schema: crate::ding_reconcile::DING_RECONCILE_RECEIPT_SCHEMA.to_owned(),
            gate_id: cutover.marker.gate_id.as_str().to_owned(),
            action_index: ding_index,
            generation_id: ding.generation_id.clone(),
            desired_sha256: ding.desired_sha256.clone(),
            runtime_ids: runtime_ids.clone(),
            exec_generation_ids: generation_ids.clone(),
            observed_sha256: crate::ding_reconcile::observation_sha256(
                &runtime_ids,
                &generation_ids,
            ),
        };
        let mut reconciled = cutover.marker.clone();
        reconciled
            .completed_ding_reconciles
            .push(CompletedDingReconcile {
                action_index: ding_index,
                receipt,
            });
        reconciled.cursor += 1;
        cutover.persist(reconciled).unwrap();
    }

    fn record_checkpoint(
        cutover: &mut CutoverTransaction,
        action_index: usize,
        kind: ExternalCheckpointKind,
    ) {
        let bytes = checkpoint(cutover, action_index, kind);
        cutover
            .permission()
            .record_external_checkpoint(action_index, &bytes)
            .unwrap();
    }

    #[test]
    fn fence_is_published_before_host_claim_and_survives_claim_failure() {
        let (_root, catalog, host) = fixture();
        let ownership = HostOwnership::acquire(catalog.as_path(), host.as_str()).unwrap();
        let (source, after) = digests(&catalog);
        let outcome = CutoverTransaction::begin(BeginCutover {
            catalog: catalog.clone(),
            host: host.clone(),
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
            source_catalog_sha256: source.clone(),
            program: program(
                &declaration_root_sha256_locked(catalog.as_path()).unwrap(),
                &after,
            ),
            predecessor_retirement: predecessor_evidence(&source, &host),
        })
        .unwrap();
        let BeginOutcome::Fenced(pending) = outcome else {
            panic!("held host ownership must leave a durable pending fence");
        };
        assert!(matches!(
            probe_mutation_admission(&catalog, Some(&host)).unwrap(),
            MutationAdmission::Busy(MutationBusy {
                reason: BusyReason::ActiveCutover,
                ..
            })
        ));
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(ownership);
        });
        let claimed = pending.wait_for_ownership().unwrap();
        assert_eq!(claimed.marker().cursor, 0);
    }

    #[test]
    fn publication_proof_is_bound_to_the_same_canonical_catalog() {
        let (_root_a, catalog_a, _host) = fixture();
        let (_root_b, catalog_b, _) = fixture();
        let lock = CatalogLock::exclusive(catalog_a.as_path()).unwrap();
        assert!(admit_catalog_publish(&catalog_a, &lock).is_ok());
        assert!(matches!(
            admit_catalog_publish(&catalog_b, &lock),
            Err(AdmissionError::Invalid(_))
        ));
    }

    #[test]
    fn program_requires_adoption_proof_before_ding_reconciliation() {
        let mut actions = program(A, B);
        let adoption = actions
            .iter()
            .position(|action| matches!(action, CutoverAction::ProviderFleetProof(_)))
            .unwrap();
        let ding = actions
            .iter()
            .position(|action| matches!(action, CutoverAction::DingReconcile(_)))
            .unwrap();
        actions.swap(adoption, ding);
        assert!(validate_program(A, &actions).is_err());
    }

    #[test]
    fn program_refuses_every_catalog_transition_after_provider_fleet_proof() {
        let mut actions = program(A, B);
        let adoption = actions
            .iter()
            .position(|action| matches!(action, CutoverAction::ProviderFleetProof(_)))
            .unwrap();
        actions.insert(
            adoption + 2,
            CutoverAction::CatalogTransition(CatalogTransition {
                before_sha256: B.to_owned(),
                after_sha256: D.to_owned(),
            }),
        );
        assert!(
            validate_program(A, &actions)
                .unwrap_err()
                .to_string()
                .contains("must precede provider fleet proof")
        );
    }

    fn running_observation(entry: &ProviderFleetEntry) -> ProviderTaskObservation {
        ProviderTaskObservation {
            identity: entry.identity.clone(),
            status: ProviderTaskStatus::Running,
            runtime_generation_id: Some(entry.runtime_generation_id.clone()),
            prompt: Some(entry.prompt.clone()),
        }
    }

    #[test]
    fn full_provider_fleet_rejects_non_primary_drift_and_legacy_prompt_authority() {
        let mut providers = vec![
            provider_entry("dev3.worker-2"),
            provider_entry("dev3.worker-1"),
        ];
        providers.sort();
        let action = ProviderFleetProofAction {
            providers_sha256: provider_entries_sha256(&providers).unwrap(),
            providers: providers.clone(),
        };
        validate_provider_fleet_action(&action).unwrap();
        let ding = ding_action();
        let mut snapshot = ProviderFleetSnapshot {
            authored_providers: providers.iter().map(running_observation).collect(),
            successor_dings: vec![SuccessorDingObservation::Absent {
                runtime_id: ding.desired[0].runtime_id.clone(),
            }],
        };
        let gate = GateId::parse("gate-1").unwrap();
        validate_provider_snapshot(&action, &ding, &gate, 3, &snapshot).unwrap();

        snapshot.authored_providers[1].runtime_generation_id =
            Some("drifted-non-primary-generation".to_owned());
        assert!(validate_provider_snapshot(&action, &ding, &gate, 3, &snapshot).is_err());
        snapshot.authored_providers[1] = running_observation(&providers[1]);
        snapshot.authored_providers[1].status = ProviderTaskStatus::Stopped;
        assert!(validate_provider_snapshot(&action, &ding, &gate, 3, &snapshot).is_err());
        snapshot.authored_providers[1] = running_observation(&providers[1]);
        snapshot.authored_providers[1]
            .prompt
            .as_mut()
            .unwrap()
            .persona_prompt_sha256 = B.to_owned();
        assert!(validate_provider_snapshot(&action, &ding, &gate, 3, &snapshot).is_err());
    }

    #[test]
    fn launch_prompt_authority_rejects_workspace_persona_path() {
        let mut entry = provider_entry("dev3.worker-1");
        entry.prompt.persona_prompt_path = entry.workspace.join(".st2/PERSONA.md");
        entry.trajectory_sha256 = provider_trajectory_sha256(&entry).unwrap();
        assert!(validate_provider_entry(&entry).is_err());
    }

    #[test]
    fn axe_trajectory_digest_contains_only_the_seven_stable_axes() {
        let entry = provider_entry("dev3.worker-1");
        let expected = provider_trajectory_sha256(&entry).unwrap();
        let mut per_run = entry.clone();
        per_run.identity = "dev3.worker-99".to_owned();
        per_run.account = "another-account".to_owned();
        per_run.runtime_generation_id = "another-runtime".to_owned();
        per_run.launch_generation_id = "another-launch".to_owned();
        per_run.workspace = PathBuf::from("/another/workspace");
        assert_eq!(provider_trajectory_sha256(&per_run).unwrap(), expected);
        per_run.mode = "batch".to_owned();
        assert_ne!(provider_trajectory_sha256(&per_run).unwrap(), expected);
    }

    #[test]
    fn legacy_workspace_persona_file_and_loader_are_proven_absent_from_source() {
        use std::collections::BTreeMap;

        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let declaration = root.path().join("agent.kdl");
        fs::write(&declaration, b"agent \"worker\" {}\n").unwrap();
        let task = agent_spec::spec::Task {
            kind: TaskKind::Pty,
            derived: false,
            name: "agent".to_owned(),
            id: None,
            command: None,
            argv: Some(vec!["axe".to_owned()]),
            cwd: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
            lifecycle: TaskLifecycle::AdoptOnly,
        };
        let spec = agent_spec::spec::AgentSpec {
            identity: "worker".to_owned(),
            name: None,
            description: None,
            host: Some("test-host".to_owned()),
            role: None,
            job_type: agent_spec::spec::JobType::Service,
            workspace: Some(workspace.display().to_string()),
            supervisor: None,
            retired: false,
            keep: false,
            restart: None,
            resources: Vec::new(),
            tasks: vec![task.clone()],
            path: declaration.clone(),
        };
        validate_legacy_prompt_absence(&spec, &task, &workspace).unwrap();

        fs::create_dir_all(workspace.join(".st2")).unwrap();
        fs::write(workspace.join(".st2/PERSONA.md"), b"legacy").unwrap();
        assert!(validate_legacy_prompt_absence(&spec, &task, &workspace).is_err());
        fs::remove_file(workspace.join(".st2/PERSONA.md")).unwrap();
        fs::write(
            &declaration,
            b"render { file \".st2/PERSONA.md\" \"legacy\" }\n",
        )
        .unwrap();
        assert!(validate_legacy_prompt_absence(&spec, &task, &workspace).is_err());
    }

    #[test]
    fn successor_dings_are_a_separate_absent_or_exact_journal_bound_partition() {
        let providers = vec![provider_entry("dev3.worker-1")];
        let action = ProviderFleetProofAction {
            providers_sha256: provider_entries_sha256(&providers).unwrap(),
            providers: providers.clone(),
        };
        let ding = ding_action();
        let providers = providers.iter().map(running_observation).collect();
        let absent = ProviderFleetSnapshot {
            authored_providers: providers,
            successor_dings: vec![SuccessorDingObservation::Absent {
                runtime_id: ding.desired[0].runtime_id.clone(),
            }],
        };
        let gate = GateId::parse("gate-1").unwrap();
        validate_provider_snapshot(&action, &ding, &gate, 3, &absent).unwrap();

        let exact = ProviderFleetSnapshot {
            authored_providers: absent.authored_providers.clone(),
            successor_dings: vec![SuccessorDingObservation::JournalBound {
                runtime_id: ding.desired[0].runtime_id.clone(),
                gate_id: gate.clone(),
                action_index: 3,
                ding_generation_id: ding.generation_id.clone(),
                launch_sha256: ding.desired[0].launch_sha256.clone(),
                journal_sha256: A.to_owned(),
            }],
        };
        validate_provider_snapshot(&action, &ding, &gate, 3, &exact).unwrap();
        let mut wrong = exact;
        let SuccessorDingObservation::JournalBound {
            ding_generation_id, ..
        } = &mut wrong.successor_dings[0]
        else {
            unreachable!()
        };
        *ding_generation_id = "foreign-generation".to_owned();
        assert!(validate_provider_snapshot(&action, &ding, &gate, 3, &wrong).is_err());
        wrong
            .successor_dings
            .push(SuccessorDingObservation::Absent {
                runtime_id: "extra.ding".to_owned(),
            });
        assert!(validate_provider_snapshot(&action, &ding, &gate, 3, &wrong).is_err());
    }

    #[test]
    fn action_program_enforces_exact_interleaving_and_typed_evidence() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, _source, after) = begin(catalog.clone(), host.clone());
        let premature = checkpoint(&cutover, 1, ExternalCheckpointKind::Cleanup);
        assert!(
            cutover
                .permission()
                .record_external_checkpoint(0, &premature)
                .is_err()
        );
        cutover
            .permission()
            .catalog_transition_once(0, |_| {
                fs::write(
                    catalog.as_path().join(crate::catalog::CONFIG_FILE),
                    b"candidate = true\n",
                )
                .map_err(|error| AdmissionError::io("write candidate catalog", error))
            })
            .unwrap();
        assert_eq!(cutover.marker().cursor, 1);
        assert_eq!(
            declaration_root_sha256_locked(catalog.as_path()).unwrap(),
            after
        );
        let mut wrong_receipt: ExternalCheckpointReceipt =
            serde_json::from_slice(&checkpoint(&cutover, 1, ExternalCheckpointKind::Cleanup))
                .unwrap();
        wrong_receipt.request_sha256 = D.to_owned();
        assert!(
            cutover
                .permission()
                .record_external_checkpoint(1, &canonical_json(&wrong_receipt).unwrap())
                .is_err()
        );
        assert_eq!(cutover.marker().cursor, 1);
        record_checkpoint(&mut cutover, 1, ExternalCheckpointKind::Cleanup);
        let stored = &cutover.marker().completed_checkpoints[0].evidence;
        assert_eq!(
            stored.receipt_sha256,
            sha256_bytes(&canonical_json(&stored.receipt).unwrap())
        );
        complete_adoption_and_ding(&mut cutover);
        record_checkpoint(&mut cutover, 4, ExternalCheckpointKind::BusContinuity);
        let mut arbitrary: ExternalCheckpointReceipt =
            serde_json::from_slice(&checkpoint(&cutover, 5, ExternalCheckpointKind::FinalProof))
                .unwrap();
        let ExternalCheckpointPayload::FinalProof {
            final_catalog_sha256,
            ..
        } = &mut arbitrary.payload
        else {
            unreachable!()
        };
        *final_catalog_sha256 = A.to_owned();
        assert!(
            cutover
                .permission()
                .record_external_checkpoint(5, &canonical_json(&arbitrary).unwrap())
                .unwrap_err()
                .to_string()
                .contains("does not bind the exact final catalog")
        );
        record_checkpoint(&mut cutover, 5, ExternalCheckpointKind::FinalProof);
        let finalized = cutover.permission().finalize().unwrap();
        assert!(finalized.finalized.history_path.exists());
        assert!(finalized.finalized.marker.finalized);
    }

    #[test]
    fn resume_accepts_exact_uncompleted_transition_after_digest_and_advances_without_reinvoke() {
        let (_root, catalog, host) = fixture();
        let (cutover, _source, _after) = begin(catalog.clone(), host.clone());
        fs::write(
            catalog.as_path().join(crate::catalog::CONFIG_FILE),
            b"candidate = true\n",
        )
        .unwrap();
        drop(cutover);
        let ResumeOutcome::Active(mut resumed) = CutoverTransaction::resume(ResumeCutover {
            catalog,
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .unwrap() else {
            panic!("expected active transaction");
        };
        let invoked = Cell::new(false);
        resumed
            .permission()
            .catalog_transition_once(0, |_| {
                invoked.set(true);
                Ok(())
            })
            .unwrap();
        assert!(!invoked.get());
        assert_eq!(resumed.marker().cursor, 1);
    }

    #[test]
    fn finalized_history_has_unambiguous_nested_address_and_typed_recovery() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, _source, _) = begin(catalog.clone(), host.clone());
        cutover
            .permission()
            .catalog_transition_once(0, |_| {
                fs::write(
                    catalog.as_path().join(crate::catalog::CONFIG_FILE),
                    b"candidate = true\n",
                )
                .map_err(|error| AdmissionError::io("write candidate catalog", error))
            })
            .unwrap();
        record_checkpoint(&mut cutover, 1, ExternalCheckpointKind::Cleanup);
        complete_adoption_and_ding(&mut cutover);
        record_checkpoint(&mut cutover, 4, ExternalCheckpointKind::BusContinuity);
        record_checkpoint(&mut cutover, 5, ExternalCheckpointKind::FinalProof);
        let finalized = cutover.permission().finalize().unwrap();
        assert!(
            finalized
                .finalized
                .history_path
                .ends_with(Path::new("test-host").join("gate-1.json"))
        );
        let finalized_path = finalized.finalized.history_path.clone();
        assert!(
            catalog
                .as_path()
                .join(CONTROL_DIR)
                .join(CUTOVER_DIR)
                .join(ACTIVE_MARKER)
                .exists(),
            "active gate stays present until successor loop readiness"
        );
        drop(finalized);
        let ResumeOutcome::Finalized(recovered_before_readiness) =
            CutoverTransaction::resume(ResumeCutover {
                catalog: catalog.clone(),
                host: host.clone(),
                gate_id: GateId::parse("gate-1").unwrap(),
                request_sha256: A.to_owned(),
            })
            .unwrap()
        else {
            panic!("restart before readiness must recover the finalized active fence");
        };
        let (_, ownership, readiness) = recovered_before_readiness.into_successor_parts();
        readiness.unwrap().supervisor_entered().unwrap();
        let inspected = CutoverTransaction::inspect_finalized(ResumeCutover {
            catalog: catalog.clone(),
            host: host.clone(),
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .unwrap()
        .expect("finalized history must remain inspectable while successor owns the host");
        assert_eq!(inspected.history_path, finalized_path);
        drop(ownership);
        let recovered = CutoverTransaction::reacquire_finalized_successor(ResumeCutover {
            catalog: catalog.clone(),
            host: host.clone(),
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .unwrap()
        .expect("expected history-only successor recovery");
        assert_eq!(recovered.finalized.history_path, finalized_path);
        assert!(
            HostOwnership::acquire(catalog.as_path(), host.as_str()).is_err(),
            "concurrent finalized replay cannot duplicate successor ownership"
        );
        let (_, recovered_ownership, readiness) = recovered.into_successor_parts();
        assert!(readiness.is_none());
        drop(recovered_ownership);

        let mut mismatched = inspected.marker.clone();
        mismatched.request_sha256 = D.to_owned();
        fs::write(&finalized_path, canonical_json(&mismatched).unwrap()).unwrap();
        let error = match CutoverTransaction::reacquire_finalized_successor(ResumeCutover {
            catalog: catalog.clone(),
            host: host.clone(),
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        }) {
            Err(error) => error,
            Ok(_) => panic!("mismatched history must be rejected"),
        };
        assert!(
            matches!(
                error,
                AdmissionError::Invalid(_) | AdmissionError::Conflict(_)
            ),
            "mismatched history must fail as typed validation/conflict"
        );
        assert!(
            !catalog
                .as_path()
                .join(CONTROL_DIR)
                .join(CUTOVER_DIR)
                .join(ACTIVE_MARKER)
                .exists(),
            "mismatched history must never reopen predecessor authority"
        );
    }

    #[test]
    fn finalized_active_without_history_is_repaired_before_successor_recovery() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, _source, _) = begin(catalog.clone(), host.clone());
        cutover
            .permission()
            .catalog_transition_once(0, |_| {
                fs::write(
                    catalog.as_path().join(crate::catalog::CONFIG_FILE),
                    b"candidate = true\n",
                )
                .map_err(|error| AdmissionError::io("write candidate catalog", error))
            })
            .unwrap();
        record_checkpoint(&mut cutover, 1, ExternalCheckpointKind::Cleanup);
        complete_adoption_and_ding(&mut cutover);
        record_checkpoint(&mut cutover, 4, ExternalCheckpointKind::BusContinuity);
        record_checkpoint(&mut cutover, 5, ExternalCheckpointKind::FinalProof);
        let finalized = cutover.permission().finalize().unwrap();
        let history_path = finalized.finalized.history_path.clone();
        let expected_bytes = canonical_json(&finalized.finalized.marker).unwrap();

        // Model the only legacy bad window: an older writer persisted finalized active authority
        // and died before publishing history. New writers cannot create this state because they
        // publish history first.
        fs::remove_file(&history_path).unwrap();
        sync_dir(history_path.parent().unwrap()).unwrap();
        drop(finalized);

        let ResumeOutcome::Finalized(recovered) = CutoverTransaction::resume(ResumeCutover {
            catalog: catalog.clone(),
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .unwrap() else {
            panic!("finalized active authority must repair its missing exact history");
        };
        assert_eq!(fs::read(&history_path).unwrap(), expected_bytes);
        assert!(recovered.finalized.marker.finalized);
        assert!(recovered.readiness.is_some());
    }

    #[test]
    fn history_collision_refuses_before_active_marker_can_be_finalized() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, _source, _) = begin(catalog.clone(), host.clone());
        cutover
            .permission()
            .catalog_transition_once(0, |_| {
                fs::write(
                    catalog.as_path().join(crate::catalog::CONFIG_FILE),
                    b"candidate = true\n",
                )
                .map_err(|error| AdmissionError::io("write candidate catalog", error))
            })
            .unwrap();
        record_checkpoint(&mut cutover, 1, ExternalCheckpointKind::Cleanup);
        complete_adoption_and_ding(&mut cutover);
        record_checkpoint(&mut cutover, 4, ExternalCheckpointKind::BusContinuity);
        record_checkpoint(&mut cutover, 5, ExternalCheckpointKind::FinalProof);

        let paths = ensure_cutover_dirs(&catalog).unwrap();
        let history_path =
            history_marker_path(&paths, &host, &GateId::parse("gate-1").unwrap()).unwrap();
        let mut collision = cutover.marker().clone();
        collision.finalized = true;
        collision.request_sha256 = D.to_owned();
        publish_bytes_create_only(
            history_path.parent().unwrap(),
            &history_path,
            &canonical_json(&collision).unwrap(),
        )
        .unwrap();

        let error = match cutover.permission().finalize() {
            Err(error) => error,
            Ok(_) => panic!("mismatched history must refuse finalization"),
        };
        assert!(error.to_string().contains("history collision"));
        let (active, _) =
            read_marker_with_bytes(&catalog, &paths.cutover.join(ACTIVE_MARKER)).unwrap();
        assert!(
            !active.finalized,
            "history must become exact before active authority can claim finalization"
        );
    }

    #[test]
    fn ids_marker_bounds_and_active_entries_fail_closed() {
        assert!(HostId::parse("x".repeat(MAX_ID_BYTES + 1)).is_err());
        assert!(GateId::parse("gate/one").is_err());
        let mut invalid_candidate = candidate();
        invalid_candidate.providers[0]
            .canonical_argv
            .push("--different".to_owned());
        assert!(validate_provider_fleet_action(&invalid_candidate).is_err());
        let (_root, catalog, host) = fixture();
        let paths = ensure_cutover_dirs(&catalog).unwrap();
        fs::write(active_marker_path(&paths), b"{bad json").unwrap();
        assert!(matches!(
            probe_mutation_admission(&catalog, Some(&host)).unwrap(),
            MutationAdmission::Busy(MutationBusy {
                reason: BusyReason::MalformedActiveMarker,
                ..
            })
        ));
    }
}
