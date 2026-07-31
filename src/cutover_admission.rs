//! Durable admission for the operations that can conflict with a host cutover.
//!
//! The ordinary runtime and catalog-authoring lock domains remain separate. A durable cutover
//! marker bridges them without turning either advisory lock into crash authority:
//!
//! - ordinary host mutation acquires `HostLock`, then `CatalogLock::shared`, then checks the gate;
//! - catalog publication already holds `CatalogLock::exclusive` and checks the same global gate;
//! - a cutover acquires `HostLock`, then `CatalogLock::exclusive`, and creates or resumes one
//!   durable marker.
//!
//! Active markers are never reclaimed by PID, age, or mtime. Finalization moves the complete
//! marker to history without replacement; it never unlinks the only durable transaction record.

use std::fmt;
use std::fs::{self, File};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::catalog_lock::{CONTROL_DIR, CatalogLock};
use crate::catalog_transaction::declaration_root_sha256_locked;
use crate::host_lock::HostOwnership;

pub const CUTOVER_TRANSACTION_SCHEMA: &str = "st2.cutover-transaction.v1";
pub const MUTATION_BUSY_SCHEMA: &str = "st2.mutation-busy.v1";
const CUTOVER_DIR: &str = "cutover";
const ACTIVE_MARKER: &str = "active.json";
const HISTORY_DIR: &str = "history";
const MAX_MARKER_BYTES: u64 = 1024 * 1024;

/// A real, symlink-resolved catalog directory.
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

/// A host identifier safe for use as exactly one filename component.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct HostId(String);

impl HostId {
    pub fn parse(host: impl Into<String>) -> AdmissionResult<Self> {
        let host = host.into();
        if host.is_empty()
            || host == "."
            || host == ".."
            || host.starts_with('.')
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(AdmissionError::Invalid(
                "host must be one safe path component".to_owned(),
            ));
        }
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
        let host = String::deserialize(deserializer)?;
        Self::parse(host).map_err(serde::de::Error::custom)
    }
}

/// A caller-chosen stable transaction identifier safe for a history filename.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct GateId(String);

impl GateId {
    pub fn parse(id: impl Into<String>) -> AdmissionResult<Self> {
        let id = id.into();
        if id.is_empty()
            || id == "."
            || id == ".."
            || id.starts_with('.')
            || id.len() > 128
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(AdmissionError::Invalid(
                "gate id must be one safe path component of at most 128 bytes".to_owned(),
            ));
        }
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
        let id = String::deserialize(deserializer)?;
        Self::parse(id).map_err(serde::de::Error::custom)
    }
}

/// The serializable, machine-readable refusal returned at a durable gate.
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

/// A read-only, typed view of the durable mutation gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationAdmission {
    Available,
    Busy(MutationBusy),
}

/// Inspect the durable gate without acquiring mutation authority or changing catalog state.
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
    fn io(context: impl Into<String>, source: std::io::Error) -> Self {
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

/// Short-lived admission for one ordinary runtime mutation.
///
/// [`HostOwnership`] is retained independently for a resident supervisor. This guard borrows that
/// ownership and retains the shared catalog lock only for one reconcile/materialize/teardown pass.
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

/// Non-forgeable proof that one runtime mutation is admitted.
pub struct RuntimeMutate<'a> {
    catalog: &'a CanonicalCatalog,
    host: &'a HostId,
    _source: RuntimeAuthoritySource,
}

enum RuntimeAuthoritySource {
    Ordinary,
    Transaction,
}

impl RuntimeMutate<'_> {
    pub fn catalog(&self) -> &CanonicalCatalog {
        self.catalog
    }

    pub fn host(&self) -> &HostId {
        self.host
    }
}

/// Check catalog publication admission while the caller retains its exclusive catalog lock.
///
/// `held_exclusive_catalog_lock` is deliberately required in the API. `CatalogLock` does not
/// expose its mode, so the caller is responsible for passing the exclusive guard it just acquired.
pub fn admit_catalog_publish<'a>(
    catalog: &'a CanonicalCatalog,
    held_exclusive_catalog_lock: &'a CatalogLock,
) -> AdmissionResult<CatalogPublish<'a>> {
    if !held_exclusive_catalog_lock.is_exclusive() {
        return Err(AdmissionError::Invalid(
            "catalog publication requires an exclusive catalog lock".to_owned(),
        ));
    }
    reject_active_gate(catalog, None)?;
    Ok(CatalogPublish {
        catalog,
        _catalog_lock: held_exclusive_catalog_lock,
    })
}

/// Proof that an exclusive catalog publisher passed every active host gate.
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
    /// The complete, ordered declaration-root transition program. It cannot be extended later.
    pub catalog_transitions: Vec<CatalogTransition>,
}

#[derive(Debug, Clone)]
pub struct ResumeCutover {
    pub catalog: CanonicalCatalog,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverMarker {
    pub schema: String,
    pub canonical_catalog: PathBuf,
    pub host: HostId,
    pub gate_id: GateId,
    pub request_sha256: String,
    pub source_catalog_sha256: String,
    pub phase: CutoverPhase,
    pub forward_only: bool,
    pub retirement_plan_sha256: Option<String>,
    pub retirement_receipt_sha256: Option<String>,
    pub catalog_transitions: Vec<CatalogTransition>,
    pub catalog_transitions_completed: usize,
    pub candidate_started: bool,
    pub candidate_completed: Option<CandidateCompletion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CutoverPhase {
    Begun,
    RetirementPlanRecorded,
    ForwardOnlyStarted,
    RetirementReceiptRecorded,
    CatalogTransitioned,
    CandidateStarted,
    CandidateCompleted,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogTransition {
    pub before_sha256: String,
    pub after_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CandidateCompletion {
    Succeeded { result_sha256: String },
    Failed { result_sha256: String },
}

impl CandidateCompletion {
    fn validate(&self) -> AdmissionResult<()> {
        match self {
            Self::Succeeded { result_sha256 } | Self::Failed { result_sha256 } => {
                validate_sha256("candidate result sha256", result_sha256)
            }
        }
    }
}

/// The durable cutover authority plus the retained host/exclusive-catalog locks.
pub struct CutoverTransaction {
    catalog: CanonicalCatalog,
    host: HostId,
    active_path: PathBuf,
    marker: CutoverMarker,
    marker_bytes: Vec<u8>,
    _host_ownership: HostOwnership,
    _catalog_lock: CatalogLock,
}

impl CutoverTransaction {
    pub fn begin(request: BeginCutover) -> AdmissionResult<Self> {
        validate_sha256("request sha256", &request.request_sha256)?;
        validate_sha256("source catalog sha256", &request.source_catalog_sha256)?;
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
        reject_active_gate(&request.catalog, Some(&request.host))?;
        validate_transition_program(
            &request.source_catalog_sha256,
            &request.catalog_transitions,
        )?;
        let observed_source =
            declaration_root_sha256_locked(request.catalog.as_path()).map_err(|error| {
                AdmissionError::Invalid(format!(
                    "compute source declaration-root digest: {error:#}"
                ))
            })?;
        if observed_source != request.source_catalog_sha256 {
            return Err(AdmissionError::Conflict(format!(
                "source catalog digest compare-and-swap failed: expected {}, found {observed_source}",
                request.source_catalog_sha256
            )));
        }
        let paths = ensure_cutover_dirs(&request.catalog)?;
        let active_path = active_marker_path(&paths);
        let history_path = history_marker_path(&paths, &request.host, &request.gate_id);
        reject_history_collision(&history_path)?;
        let marker = CutoverMarker {
            schema: CUTOVER_TRANSACTION_SCHEMA.to_owned(),
            canonical_catalog: request.catalog.as_path().to_path_buf(),
            host: request.host.clone(),
            gate_id: request.gate_id,
            request_sha256: request.request_sha256,
            source_catalog_sha256: request.source_catalog_sha256,
            phase: CutoverPhase::Begun,
            forward_only: false,
            retirement_plan_sha256: None,
            retirement_receipt_sha256: None,
            catalog_transitions: request.catalog_transitions,
            catalog_transitions_completed: 0,
            candidate_started: false,
            candidate_completed: None,
        };
        let marker_bytes = publish_create_only(&paths.cutover, &active_path, &marker)?;
        Ok(Self {
            catalog: request.catalog,
            host: request.host,
            active_path,
            marker,
            marker_bytes,
            _host_ownership: host_ownership,
            _catalog_lock: catalog_lock,
        })
    }

    pub fn resume(request: ResumeCutover) -> AdmissionResult<Self> {
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
        let (marker, marker_bytes) = read_marker_with_bytes(&request.catalog, &active_path)?;
        validate_marker(&request.catalog, &request.host, &marker)?;
        if marker.gate_id != request.gate_id || marker.request_sha256 != request.request_sha256 {
            return Err(AdmissionError::Conflict(format!(
                "active cutover authority mismatch at {}",
                active_path.display()
            )));
        }
        validate_resume_catalog_digest(&request.catalog, &marker)?;
        Ok(Self {
            catalog: request.catalog,
            host: request.host,
            active_path,
            marker,
            marker_bytes,
            _host_ownership: host_ownership,
            _catalog_lock: catalog_lock,
        })
    }

    pub fn marker(&self) -> &CutoverMarker {
        &self.marker
    }

    pub fn permission(&mut self) -> Transaction<'_> {
        Transaction { transaction: self }
    }

    fn persist(&mut self, next: CutoverMarker) -> AdmissionResult<()> {
        let (observed, observed_bytes) =
            read_marker_with_bytes(&self.catalog, &self.active_path)?;
        if observed != self.marker || observed_bytes != self.marker_bytes {
            return Err(AdmissionError::Conflict(format!(
                "cutover marker compare-and-swap failed at {}",
                self.active_path.display()
            )));
        }
        validate_marker(&self.catalog, &self.host, &next)?;
        let next_bytes = replace_durable(
            &self
                .active_path
                .parent()
                .ok_or_else(|| {
                    AdmissionError::Invalid("cutover marker has no parent directory".to_owned())
                })?
                .to_path_buf(),
            &self.active_path,
            &next,
        )?;
        self.marker = next;
        self.marker_bytes = next_bytes;
        Ok(())
    }
}

/// Typed authority for exact, durable cutover state transitions.
pub struct Transaction<'a> {
    transaction: &'a mut CutoverTransaction,
}

impl Transaction<'_> {
    pub fn marker(&self) -> &CutoverMarker {
        &self.transaction.marker
    }

    pub fn record_retirement_plan(
        &mut self,
        expected_phase: CutoverPhase,
        plan_sha256: String,
    ) -> AdmissionResult<()> {
        self.expect_phase(expected_phase, CutoverPhase::Begun)?;
        validate_sha256("retirement plan sha256", &plan_sha256)?;
        let mut next = self.transaction.marker.clone();
        next.retirement_plan_sha256 = Some(plan_sha256);
        next.phase = CutoverPhase::RetirementPlanRecorded;
        self.transaction.persist(next)
    }

    pub fn record_retirement_receipt(
        &mut self,
        expected_phase: CutoverPhase,
        expected_plan_sha256: &str,
        receipt_sha256: String,
    ) -> AdmissionResult<()> {
        self.expect_phase(expected_phase, CutoverPhase::ForwardOnlyStarted)?;
        if self.transaction.marker.retirement_plan_sha256.as_deref() != Some(expected_plan_sha256) {
            return Err(AdmissionError::Conflict(
                "retirement plan digest compare-and-swap failed".to_owned(),
            ));
        }
        validate_sha256("retirement receipt sha256", &receipt_sha256)?;
        let mut next = self.transaction.marker.clone();
        next.retirement_receipt_sha256 = Some(receipt_sha256);
        next.phase = CutoverPhase::RetirementReceiptRecorded;
        self.transaction.persist(next)
    }

    /// Durably cross the irreversible boundary before invoking retirement apply.
    pub fn start_forward_only(
        &mut self,
        expected_phase: CutoverPhase,
        expected_plan_sha256: &str,
    ) -> AdmissionResult<()> {
        self.expect_phase(expected_phase, CutoverPhase::RetirementPlanRecorded)?;
        if self.transaction.marker.retirement_plan_sha256.as_deref()
            != Some(expected_plan_sha256)
        {
            return Err(AdmissionError::Conflict(
                "retirement plan digest compare-and-swap failed".to_owned(),
            ));
        }
        let mut next = self.transaction.marker.clone();
        next.forward_only = true;
        next.phase = CutoverPhase::ForwardOnlyStarted;
        self.transaction.persist(next)
    }

    /// Complete one pre-recorded exact declaration-root transition.
    ///
    /// The declaration mutation runs in-process while this transaction retains the exclusive
    /// catalog lock. Before invocation the observed root must be the pre-recorded `beforeSha256`.
    /// After invocation it must be the pre-recorded `afterSha256`. If a process crashed after the
    /// mutation but before the marker update, resume observes the exact after digest and advances
    /// without invoking the closure twice.
    pub fn catalog_transition_once<F>(
        &mut self,
        expected_phase: CutoverPhase,
        expected_index: usize,
        expected_current_sha256: &str,
        expected_after_sha256: &str,
        mutate: F,
    ) -> AdmissionResult<()>
    where
        F: FnOnce() -> AdmissionResult<()>,
    {
        let current_phase = self.transaction.marker.phase;
        if current_phase != expected_phase
            || !matches!(
                current_phase,
                CutoverPhase::RetirementReceiptRecorded | CutoverPhase::CatalogTransitioned
            )
        {
            return Err(AdmissionError::Conflict(format!(
                "catalog transition phase compare-and-swap failed: expected {expected_phase:?}, found {current_phase:?}"
            )));
        }
        validate_sha256("expected current catalog sha256", expected_current_sha256)?;
        validate_sha256("expected after catalog sha256", expected_after_sha256)?;
        let index = self.transaction.marker.catalog_transitions_completed;
        if index != expected_index {
            return Err(AdmissionError::Conflict(format!(
                "catalog transition index compare-and-swap failed: expected {expected_index}, found {index}"
            )));
        }
        let programmed = self
            .transaction
            .marker
            .catalog_transitions
            .get(index)
            .ok_or_else(|| {
                AdmissionError::Conflict(
                    "catalog transition program is already complete".to_owned(),
                )
            })?;
        if programmed.before_sha256 != expected_current_sha256
            || programmed.after_sha256 != expected_after_sha256
        {
            return Err(AdmissionError::Conflict(format!(
                "catalog transition authority mismatch at index {index}"
            )));
        }
        let observed_before =
            declaration_root_sha256_locked(self.transaction.catalog.as_path()).map_err(|error| {
                AdmissionError::Invalid(format!(
                    "compute declaration-root digest before transition: {error:#}"
                ))
            })?;
        if observed_before == expected_after_sha256 {
            // Recovery after the catalog mutation became durable but before the gate cursor did.
        } else if observed_before == expected_current_sha256 {
            mutate()?;
            let observed_after =
                declaration_root_sha256_locked(self.transaction.catalog.as_path()).map_err(
                    |error| {
                        AdmissionError::Invalid(format!(
                            "compute declaration-root digest after transition: {error:#}"
                        ))
                    },
                )?;
            if observed_after != expected_after_sha256 {
                return Err(AdmissionError::Conflict(format!(
                    "catalog transition produced unexpected digest: expected {expected_after_sha256}, found {observed_after}"
                )));
            }
        } else {
            return Err(AdmissionError::Conflict(format!(
                "catalog digest compare-and-swap failed: expected {expected_current_sha256} or crash-recovery digest {expected_after_sha256}, found {observed_before}"
            )));
        }
        let mut next = self.transaction.marker.clone();
        next.catalog_transitions_completed += 1;
        next.phase = CutoverPhase::CatalogTransitioned;
        self.transaction.persist(next)
    }

    /// Invoke the candidate exactly once.
    ///
    /// `candidateStarted` is persisted before the closure runs. A crash in the closure therefore
    /// resumes as a typed conflict and never guesses that retry is safe. Once the closure returns,
    /// its success or failure digest is durably persisted and subsequent calls only return it.
    pub fn candidate_once<F>(
        &mut self,
        expected_phase: CutoverPhase,
        invoke: F,
    ) -> AdmissionResult<CandidateCompletion>
    where
        F: FnOnce(&RuntimeMutate<'_>) -> CandidateCompletion,
    {
        if let Some(completed) = &self.transaction.marker.candidate_completed {
            return Ok(completed.clone());
        }
        if self.transaction.marker.candidate_started {
            return Err(AdmissionError::Conflict(
                "candidate was durably started but has no completion; refusing a second invocation"
                    .to_owned(),
            ));
        }
        self.expect_phase(expected_phase, CutoverPhase::CatalogTransitioned)?;
        if self.transaction.marker.catalog_transitions_completed
            != self.transaction.marker.catalog_transitions.len()
        {
            return Err(AdmissionError::Conflict(
                "candidate cannot start before every pre-recorded catalog transition completes"
                    .to_owned(),
            ));
        }
        let mut started = self.transaction.marker.clone();
        started.candidate_started = true;
        started.phase = CutoverPhase::CandidateStarted;
        self.transaction.persist(started)?;

        let permission = RuntimeMutate {
            catalog: &self.transaction.catalog,
            host: &self.transaction.host,
            _source: RuntimeAuthoritySource::Transaction,
        };
        let completion = invoke(&permission);
        completion.validate()?;
        let mut completed = self.transaction.marker.clone();
        completed.candidate_completed = Some(completion.clone());
        completed.phase = CutoverPhase::CandidateCompleted;
        self.transaction.persist(completed)?;
        Ok(completion)
    }

    /// Losslessly move a completed marker from active authority to immutable history.
    pub fn finalize(self, expected_phase: CutoverPhase) -> AdmissionResult<PathBuf> {
        let actual = self.transaction.marker.phase;
        if actual != expected_phase
            || !matches!(
                expected_phase,
                CutoverPhase::CandidateCompleted | CutoverPhase::Finalized
            )
        {
            return Err(AdmissionError::Conflict(format!(
                "finalize phase compare-and-swap failed: expected {expected_phase:?}, found {actual:?}"
            )));
        }
        if !matches!(
            self.transaction.marker.candidate_completed,
            Some(CandidateCompletion::Succeeded { .. })
        ) {
            return Err(AdmissionError::Conflict(
                "a failed or incomplete candidate cannot finalize the cutover".to_owned(),
            ));
        }
        if actual == CutoverPhase::CandidateCompleted {
            let mut finalized = self.transaction.marker.clone();
            finalized.phase = CutoverPhase::Finalized;
            self.transaction.persist(finalized)?;
        }

        let paths = ensure_cutover_dirs(&self.transaction.catalog)?;
        let history_path = history_marker_path(
            &paths,
            &self.transaction.host,
            &self.transaction.marker.gate_id,
        );
        match fs::symlink_metadata(&history_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                publish_bytes_create_only(
                    &paths.cutover,
                    &history_path,
                    &self.transaction.marker_bytes,
                )?;
            }
            Ok(_) => {
                let (_, history_bytes) =
                    read_marker_with_bytes(&self.transaction.catalog, &history_path)?;
                if history_bytes != self.transaction.marker_bytes {
                    return Err(AdmissionError::Conflict(format!(
                        "cutover history collision at {}",
                        history_path.display()
                    )));
                }
            }
            Err(error) => {
                return Err(AdmissionError::io(
                    format!("inspect cutover history {}", history_path.display()),
                    error,
                ));
            }
        }
        fs::remove_file(&self.transaction.active_path).map_err(|error| {
            AdmissionError::io(
                format!(
                    "retire active cutover marker {}",
                    self.transaction.active_path.display()
                ),
                error,
            )
        })?;
        sync_dir(&paths.cutover)?;
        Ok(history_path)
    }

    fn expect_phase(
        &self,
        supplied_expected: CutoverPhase,
        required: CutoverPhase,
    ) -> AdmissionResult<()> {
        let actual = self.transaction.marker.phase;
        if supplied_expected != required || actual != supplied_expected {
            return Err(AdmissionError::Conflict(format!(
                "cutover phase compare-and-swap failed: required {required:?}, expected {supplied_expected:?}, found {actual:?}"
            )));
        }
        Ok(())
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
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(AdmissionError::Invalid(format!(
                    "cutover control path is not a real directory: {}",
                    path.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(path).map_err(|inspect| {
                        AdmissionError::io(
                            format!("inspect raced cutover directory {}", path.display()),
                            inspect,
                        )
                    })?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(AdmissionError::Invalid(format!(
                            "cutover control path is not a real directory: {}",
                            path.display()
                        )));
                    }
                }
                Err(error) => {
                    return Err(AdmissionError::io(
                        format!("create cutover directory {}", path.display()),
                        error,
                    ));
                }
            }
            sync_dir(parent)?;
        }
        Err(error) => {
            return Err(AdmissionError::io(
                format!("inspect cutover directory {}", path.display()),
                error,
            ));
        }
    }
    Ok(())
}

fn active_marker_path(paths: &CutoverPaths) -> PathBuf {
    paths.cutover.join(ACTIVE_MARKER)
}

fn history_marker_path(paths: &CutoverPaths, host: &HostId, gate_id: &GateId) -> PathBuf {
    paths
        .history
        .join(format!("{}-{}.json", host.as_str(), gate_id.as_str()))
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
                format!("inspect active cutover directory {}", active.display()),
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

fn validate_resume_catalog_digest(
    catalog: &CanonicalCatalog,
    marker: &CutoverMarker,
) -> AdmissionResult<()> {
    let expected = if marker.catalog_transitions_completed == 0 {
        &marker.source_catalog_sha256
    } else {
        &marker.catalog_transitions[marker.catalog_transitions_completed - 1].after_sha256
    };
    let observed = declaration_root_sha256_locked(catalog.as_path()).map_err(|error| {
        AdmissionError::Invalid(format!(
            "compute declaration-root digest while resuming cutover: {error:#}"
        ))
    })?;
    if observed != *expected {
        return Err(AdmissionError::Conflict(format!(
            "resume catalog digest compare-and-swap failed at transition cursor {}: expected {expected}, found {observed}",
            marker.catalog_transitions_completed
        )));
    }
    Ok(())
}

fn validate_marker(
    catalog: &CanonicalCatalog,
    filename_host: &HostId,
    marker: &CutoverMarker,
) -> AdmissionResult<()> {
    if marker.schema != CUTOVER_TRANSACTION_SCHEMA {
        return Err(AdmissionError::Invalid(format!(
            "unknown cutover marker schema `{}`",
            marker.schema
        )));
    }
    if marker.canonical_catalog != catalog.as_path() || &marker.host != filename_host {
        return Err(AdmissionError::Invalid(
            "cutover marker authority does not match its canonical address".to_owned(),
        ));
    }
    GateId::parse(marker.gate_id.as_str().to_owned())?;
    validate_sha256("request sha256", &marker.request_sha256)?;
    validate_sha256("source catalog sha256", &marker.source_catalog_sha256)?;
    if let Some(digest) = &marker.retirement_plan_sha256 {
        validate_sha256("retirement plan sha256", digest)?;
    }
    if let Some(digest) = &marker.retirement_receipt_sha256 {
        validate_sha256("retirement receipt sha256", digest)?;
    }
    validate_transition_program(
        &marker.source_catalog_sha256,
        &marker.catalog_transitions,
    )?;
    if marker.catalog_transitions_completed > marker.catalog_transitions.len() {
        return Err(AdmissionError::Invalid(
            "completed catalog transition cursor exceeds the pre-recorded program".to_owned(),
        ));
    }
    if marker.candidate_completed.is_some() && !marker.candidate_started {
        return Err(AdmissionError::Invalid(
            "candidate completion exists without durable candidate start".to_owned(),
        ));
    }
    if let Some(completion) = &marker.candidate_completed {
        completion.validate()?;
    }
    let retirement_complete = marker.retirement_plan_sha256.is_some()
        && marker.retirement_receipt_sha256.is_some()
        && marker.forward_only;
    let phase_shape_valid = match marker.phase {
        CutoverPhase::Begun => {
            marker.retirement_plan_sha256.is_none()
                && marker.retirement_receipt_sha256.is_none()
                && marker.catalog_transitions_completed == 0
                && !marker.candidate_started
                && !marker.forward_only
        }
        CutoverPhase::RetirementPlanRecorded => {
            marker.retirement_plan_sha256.is_some()
                && marker.retirement_receipt_sha256.is_none()
                && marker.catalog_transitions_completed == 0
                && !marker.candidate_started
                && !marker.forward_only
        }
        CutoverPhase::ForwardOnlyStarted => {
            marker.retirement_plan_sha256.is_some()
                && marker.retirement_receipt_sha256.is_none()
                && marker.catalog_transitions_completed == 0
                && !marker.candidate_started
                && marker.forward_only
        }
        CutoverPhase::RetirementReceiptRecorded => {
            marker.retirement_plan_sha256.is_some()
                && marker.retirement_receipt_sha256.is_some()
                && marker.catalog_transitions_completed == 0
                && !marker.candidate_started
                && marker.forward_only
        }
        CutoverPhase::CatalogTransitioned => {
            marker.catalog_transitions_completed > 0
                && !marker.candidate_started
                && marker.retirement_receipt_sha256.is_some()
                && retirement_complete
        }
        CutoverPhase::CandidateStarted => {
            marker.catalog_transitions_completed == marker.catalog_transitions.len()
                && marker.candidate_started
                && marker.candidate_completed.is_none()
                && retirement_complete
        }
        CutoverPhase::CandidateCompleted | CutoverPhase::Finalized => {
            marker.catalog_transitions_completed == marker.catalog_transitions.len()
                && marker.candidate_started
                && marker.candidate_completed.is_some()
                && retirement_complete
                && (marker.phase != CutoverPhase::Finalized
                    || matches!(
                        marker.candidate_completed,
                        Some(CandidateCompletion::Succeeded { .. })
                    ))
        }
    };
    if !phase_shape_valid {
        return Err(AdmissionError::Invalid(
            "cutover marker fields do not match its closed phase".to_owned(),
        ));
    }
    Ok(())
}

fn validate_transition_program(
    source_catalog_sha256: &str,
    transitions: &[CatalogTransition],
) -> AdmissionResult<()> {
    if transitions.is_empty() {
        return Err(AdmissionError::Invalid(
            "cutover needs at least one pre-recorded catalog transition".to_owned(),
        ));
    }
    let mut current = source_catalog_sha256;
    for transition in transitions {
        validate_sha256(
            "catalog transition before sha256",
            &transition.before_sha256,
        )?;
        validate_sha256("catalog transition after sha256", &transition.after_sha256)?;
        if transition.before_sha256 != current
            || transition.after_sha256 == transition.before_sha256
        {
            return Err(AdmissionError::Invalid(
                "catalog transition program is not exact and contiguous".to_owned(),
            ));
        }
        current = &transition.after_sha256;
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

fn publish_create_only(
    temp_parent: &Path,
    target: &Path,
    marker: &CutoverMarker,
) -> AdmissionResult<Vec<u8>> {
    let bytes = canonical_json(marker)?;
    publish_bytes_create_only(temp_parent, target, &bytes)?;
    Ok(bytes)
}

fn publish_bytes_create_only(
    temp_parent: &Path,
    target: &Path,
    bytes: &[u8],
) -> AdmissionResult<()> {
    let mut temp = tempfile::Builder::new()
        .prefix(".cutover-marker-")
        .tempfile_in(temp_parent)
        .map_err(|error| {
            AdmissionError::io(
                format!("create cutover marker stage in {}", temp_parent.display()),
                error,
            )
        })?;
    temp.as_file_mut()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| AdmissionError::io("set cutover marker permissions", error))?;
    temp.write_all(&bytes)
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
    let target_parent = target.parent().ok_or_else(|| {
        AdmissionError::Invalid("cutover marker target has no parent directory".to_owned())
    })?;
    sync_dir(target_parent)
}

fn replace_durable(
    stage_parent: &Path,
    target: &Path,
    marker: &CutoverMarker,
) -> AdmissionResult<Vec<u8>> {
    let target_parent = target.parent().ok_or_else(|| {
        AdmissionError::Invalid("cutover marker has no parent directory".to_owned())
    })?;
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
    temp.persist(target)
        .map_err(|error| AdmissionError::io("replace cutover marker", error.error))?;
    sync_dir(target_parent)?;
    Ok(bytes)
}

fn canonical_json<T: Serialize>(value: &T) -> AdmissionResult<Vec<u8>> {
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
    const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    fn fixture() -> (tempfile::TempDir, CanonicalCatalog, HostId) {
        let root = tempfile::tempdir().unwrap();
        let catalog = CanonicalCatalog::open(root.path()).unwrap();
        let host = HostId::parse("test-host").unwrap();
        (root, catalog, host)
    }

    fn transition_program(catalog: &CanonicalCatalog) -> (String, String) {
        let source = declaration_root_sha256_locked(catalog.as_path()).unwrap();
        let config = catalog.as_path().join(crate::catalog::CONFIG_FILE);
        fs::write(&config, b"candidate = true\n").unwrap();
        let after = declaration_root_sha256_locked(catalog.as_path()).unwrap();
        fs::remove_file(config).unwrap();
        assert_ne!(source, after);
        (source, after)
    }

    fn begin(
        catalog: CanonicalCatalog,
        host: HostId,
    ) -> (CutoverTransaction, String, String) {
        let (source, after) = transition_program(&catalog);
        let transaction = CutoverTransaction::begin(BeginCutover {
            catalog,
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
            source_catalog_sha256: source.clone(),
            catalog_transitions: vec![CatalogTransition {
                before_sha256: source.clone(),
                after_sha256: after.clone(),
            }],
        })
        .unwrap();
        (transaction, source, after)
    }

    fn advance_through_catalog_transition(
        cutover: &mut CutoverTransaction,
        catalog: &CanonicalCatalog,
        source: &str,
        after: &str,
    ) {
        let mut transaction = cutover.permission();
        transaction
            .record_retirement_plan(CutoverPhase::Begun, C.to_owned())
            .unwrap();
        transaction
            .start_forward_only(CutoverPhase::RetirementPlanRecorded, C)
            .unwrap();
        transaction
            .record_retirement_receipt(CutoverPhase::ForwardOnlyStarted, C, D.to_owned())
            .unwrap();
        transaction
            .catalog_transition_once(
                CutoverPhase::RetirementReceiptRecorded,
                0,
                source,
                after,
                || {
                    fs::write(
                        catalog.as_path().join(crate::catalog::CONFIG_FILE),
                        b"candidate = true\n",
                    )
                    .map_err(|error| AdmissionError::io("write candidate catalog", error))
                },
            )
            .unwrap();
    }

    #[test]
    fn one_active_gate_blocks_ordinary_mutation_for_every_host() {
        let (_root, catalog, host) = fixture();
        let (transaction, _, _) = begin(catalog.clone(), host.clone());
        let error = HostOwnership::acquire(catalog.as_path(), host.as_str())
            .err()
            .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

        drop(transaction);
        let ownership = HostOwnership::acquire(catalog.as_path(), "other-host").unwrap();
        let error = RuntimeMutationAdmission::ordinary(&ownership).err().unwrap();
        assert!(matches!(
            error,
            AdmissionError::Busy(MutationBusy {
                reason: BusyReason::ActiveCutover,
                ..
            })
        ));
    }

    #[test]
    fn durable_gate_survives_lock_release_and_returns_typed_busy_json() {
        let (_root, catalog, host) = fixture();
        drop(begin(catalog.clone(), host.clone()));

        let ownership = HostOwnership::acquire(catalog.as_path(), host.as_str()).unwrap();
        let error = RuntimeMutationAdmission::ordinary(&ownership).err().unwrap();
        let AdmissionError::Busy(busy) = error else {
            panic!("expected durable busy gate");
        };
        assert_eq!(busy.schema, MUTATION_BUSY_SCHEMA);
        assert_eq!(busy.reason, BusyReason::ActiveCutover);
        let json = serde_json::to_value(busy).unwrap();
        assert_eq!(json["schema"], MUTATION_BUSY_SCHEMA);

        let exclusive = CatalogLock::exclusive(catalog.as_path()).unwrap();
        let error = admit_catalog_publish(&catalog, &exclusive).err().unwrap();
        assert!(matches!(
            error,
            AdmissionError::Busy(MutationBusy {
                reason: BusyReason::ActiveCutover,
                ..
            })
        ));

        assert!(matches!(
            probe_mutation_admission(&catalog, None).unwrap(),
            MutationAdmission::Busy(MutationBusy {
                reason: BusyReason::ActiveCutover,
                ..
            })
        ));
    }

    #[test]
    fn catalog_publish_authority_requires_exclusive_lock() {
        let (_root, catalog, _host) = fixture();
        let shared = CatalogLock::shared(catalog.as_path()).unwrap();
        let error = admit_catalog_publish(&catalog, &shared).err().unwrap();
        assert!(matches!(error, AdmissionError::Invalid(_)));
    }

    #[test]
    fn malformed_and_unknown_active_entries_fail_closed_without_time_reclaim() {
        let (_root, catalog, host) = fixture();
        let paths = ensure_cutover_dirs(&catalog).unwrap();
        let active = active_marker_path(&paths);
        fs::write(&active, b"{not json").unwrap();
        let ownership = HostOwnership::acquire(catalog.as_path(), host.as_str()).unwrap();
        let error = RuntimeMutationAdmission::ordinary(&ownership).err().unwrap();
        assert!(matches!(
            error,
            AdmissionError::Busy(MutationBusy {
                reason: BusyReason::MalformedActiveMarker,
                ..
            })
        ));

        fs::remove_file(&active).unwrap();
        fs::create_dir(&active).unwrap();
        let exclusive = CatalogLock::exclusive(catalog.as_path()).unwrap();
        let error = admit_catalog_publish(&catalog, &exclusive).err().unwrap();
        assert!(matches!(
            error,
            AdmissionError::Busy(MutationBusy {
                reason: BusyReason::UnknownActiveEntry,
                ..
            })
        ));
    }

    #[test]
    fn transition_methods_are_exact_compare_and_swap_and_history_is_lossless() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, source, after) = begin(catalog.clone(), host);
        let history_path = {
            let mut transaction = cutover.permission();
            transaction
                .record_retirement_plan(CutoverPhase::Begun, C.to_owned())
                .unwrap();
            transaction
                .start_forward_only(CutoverPhase::RetirementPlanRecorded, C)
                .unwrap();
            assert!(
                transaction
                    .record_retirement_receipt(
                        CutoverPhase::ForwardOnlyStarted,
                        D,
                        D.to_owned(),
                    )
                    .is_err()
            );
            transaction
                .record_retirement_receipt(CutoverPhase::ForwardOnlyStarted, C, D.to_owned())
                .unwrap();
            assert!(
                transaction
                    .catalog_transition_once(
                        CutoverPhase::RetirementReceiptRecorded,
                        0,
                        A,
                        &after,
                        || Ok(()),
                    )
                    .is_err()
            );
            transaction
                .catalog_transition_once(
                    CutoverPhase::RetirementReceiptRecorded,
                    0,
                    &source,
                    &after,
                    || {
                        fs::write(
                            catalog.as_path().join(crate::catalog::CONFIG_FILE),
                            b"candidate = true\n",
                        )
                        .map_err(|error| AdmissionError::io("write candidate catalog", error))
                    },
                )
                .unwrap();
            let calls = Cell::new(0);
            let completion = transaction
                .candidate_once(CutoverPhase::CatalogTransitioned, |_| {
                    calls.set(calls.get() + 1);
                    CandidateCompletion::Succeeded {
                        result_sha256: D.to_owned(),
                    }
                })
                .unwrap();
            assert_eq!(calls.get(), 1);
            assert!(matches!(completion, CandidateCompletion::Succeeded { .. }));
            let replay = transaction
                .candidate_once(CutoverPhase::CandidateCompleted, |_| {
                    calls.set(calls.get() + 1);
                    CandidateCompletion::Failed {
                        result_sha256: A.to_owned(),
                    }
                })
                .unwrap();
            assert_eq!(calls.get(), 1, "completed candidate is never reinvoked");
            assert_eq!(replay, completion);
            transaction
                .finalize(CutoverPhase::CandidateCompleted)
                .unwrap()
        };
        assert!(history_path.exists());
        assert!(
            !catalog
                .as_path()
                .join(CONTROL_DIR)
                .join(CUTOVER_DIR)
                .join(ACTIVE_MARKER)
                .exists()
        );
        let history: CutoverMarker =
            serde_json::from_slice(&fs::read(history_path).unwrap()).unwrap();
        assert_eq!(history.phase, CutoverPhase::Finalized);
    }

    #[test]
    fn started_without_completion_never_invokes_candidate_again_after_resume() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, source, after) = begin(catalog.clone(), host.clone());
        {
            let mut transaction = cutover.permission();
            transaction
                .record_retirement_plan(CutoverPhase::Begun, C.to_owned())
                .unwrap();
            transaction
                .start_forward_only(CutoverPhase::RetirementPlanRecorded, C)
                .unwrap();
            transaction
                .record_retirement_receipt(CutoverPhase::ForwardOnlyStarted, C, D.to_owned())
                .unwrap();
            transaction
                .catalog_transition_once(
                    CutoverPhase::RetirementReceiptRecorded,
                    0,
                    &source,
                    &after,
                    || {
                    fs::write(
                        catalog.as_path().join(crate::catalog::CONFIG_FILE),
                        b"candidate = true\n",
                    )
                    .map_err(|error| AdmissionError::io("write candidate catalog", error))
                    },
                )
                .unwrap();
        }
        let mut started = cutover.marker.clone();
        started.candidate_started = true;
        started.phase = CutoverPhase::CandidateStarted;
        cutover.persist(started).unwrap();
        drop(cutover);

        let mut resumed = CutoverTransaction::resume(ResumeCutover {
            catalog,
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .unwrap();
        let called = Cell::new(false);
        let error = resumed
            .permission()
            .candidate_once(CutoverPhase::CandidateStarted, |_| {
                called.set(true);
                CandidateCompletion::Succeeded {
                    result_sha256: D.to_owned(),
                }
            })
            .unwrap_err();
        assert!(!called.get());
        assert!(matches!(error, AdmissionError::Conflict(_)));
    }

    #[test]
    fn begin_rejects_an_existing_history_address() {
        let (_root, catalog, host) = fixture();
        let (source, after) = transition_program(&catalog);
        let paths = ensure_cutover_dirs(&catalog).unwrap();
        let history =
            history_marker_path(&paths, &host, &GateId::parse("gate-1").unwrap());
        fs::write(history, b"immutable prior history\n").unwrap();
        let error = CutoverTransaction::begin(BeginCutover {
            catalog,
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
            source_catalog_sha256: source.clone(),
            catalog_transitions: vec![CatalogTransition {
                before_sha256: source,
                after_sha256: after,
            }],
        })
        .err()
        .unwrap();
        assert!(matches!(error, AdmissionError::Conflict(_)));
    }

    #[test]
    fn resume_rejects_a_catalog_digest_not_implied_by_the_cursor() {
        let (_root, catalog, host) = fixture();
        drop(begin(catalog.clone(), host.clone()));
        fs::write(
            catalog.as_path().join(crate::catalog::CONFIG_FILE),
            b"unrecorded = true\n",
        )
        .unwrap();
        let error = CutoverTransaction::resume(ResumeCutover {
            catalog,
            host,
            gate_id: GateId::parse("gate-1").unwrap(),
            request_sha256: A.to_owned(),
        })
        .err()
        .unwrap();
        assert!(matches!(error, AdmissionError::Conflict(_)));
    }

    #[test]
    fn marker_compare_and_swap_is_byte_exact() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, _, _) = begin(catalog, host);
        let mut changed = cutover.marker_bytes.clone();
        changed.push(b' ');
        fs::write(&cutover.active_path, changed).unwrap();
        let error = cutover
            .permission()
            .record_retirement_plan(CutoverPhase::Begun, C.to_owned())
            .unwrap_err();
        assert!(matches!(error, AdmissionError::Conflict(_)));
    }

    #[test]
    fn oversized_marker_is_a_typed_fail_closed_probe() {
        let (_root, catalog, host) = fixture();
        let paths = ensure_cutover_dirs(&catalog).unwrap();
        fs::write(
            active_marker_path(&paths),
            vec![b' '; MAX_MARKER_BYTES as usize + 1],
        )
        .unwrap();
        assert!(matches!(
            probe_mutation_admission(&catalog, Some(&host)).unwrap(),
            MutationAdmission::Busy(MutationBusy {
                reason: BusyReason::MalformedActiveMarker,
                ..
            })
        ));
    }

    #[test]
    fn failed_candidate_remains_active_and_cannot_finalize() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, source, after) = begin(catalog.clone(), host);
        advance_through_catalog_transition(&mut cutover, &catalog, &source, &after);
        cutover
            .permission()
            .candidate_once(CutoverPhase::CatalogTransitioned, |_| {
                CandidateCompletion::Failed {
                    result_sha256: D.to_owned(),
                }
            })
            .unwrap();
        let active_path = cutover.active_path.clone();
        let error = cutover
            .permission()
            .finalize(CutoverPhase::CandidateCompleted)
            .unwrap_err();
        assert!(matches!(error, AdmissionError::Conflict(_)));
        assert!(active_path.exists());
    }

    #[test]
    fn finalize_replays_an_exact_history_publish_without_replacement() {
        let (_root, catalog, host) = fixture();
        let (mut cutover, source, after) = begin(catalog.clone(), host.clone());
        advance_through_catalog_transition(&mut cutover, &catalog, &source, &after);
        cutover
            .permission()
            .candidate_once(CutoverPhase::CatalogTransitioned, |_| {
                CandidateCompletion::Succeeded {
                    result_sha256: D.to_owned(),
                }
            })
            .unwrap();
        let mut finalized = cutover.marker.clone();
        finalized.phase = CutoverPhase::Finalized;
        cutover.persist(finalized).unwrap();
        let paths = ensure_cutover_dirs(&catalog).unwrap();
        let history = history_marker_path(&paths, &host, &cutover.marker.gate_id);
        publish_bytes_create_only(&paths.cutover, &history, &cutover.marker_bytes).unwrap();

        let finalized_path = cutover
            .permission()
            .finalize(CutoverPhase::Finalized)
            .unwrap();
        assert_eq!(finalized_path, history);
        assert!(!active_marker_path(&paths).exists());
        assert_eq!(fs::read(finalized_path).unwrap(), cutover.marker_bytes);
    }

    #[test]
    fn resume_requires_exact_gate_and_request_authority() {
        let (_root, catalog, host) = fixture();
        drop(begin(catalog.clone(), host.clone()));
        let wrong = CutoverTransaction::resume(ResumeCutover {
            catalog,
            host,
            gate_id: GateId::parse("other-gate").unwrap(),
            request_sha256: A.to_owned(),
        })
        .err()
        .unwrap();
        assert!(matches!(wrong, AdmissionError::Conflict(_)));
    }

    #[test]
    fn ids_and_digests_are_closed_values() {
        assert!(HostId::parse("../host").is_err());
        assert!(HostId::parse(".hidden").is_err());
        assert!(GateId::parse("gate/one").is_err());
        assert!(validate_sha256("digest", &"A".repeat(64)).is_err());
        assert!(validate_sha256("digest", "abc").is_err());
    }
}
