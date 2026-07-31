//! File-backed driver for one exact cutover transaction.
//!
//! The request file is an immutable authority manifest. Its canonical bytes bind the catalog,
//! host, gate, source declaration digest, complete action program, and retirement selector before
//! the durable fence is published. The retirement output, prepared catalog inputs, and checkpoint
//! receipt output paths are canonical request data. Prepared content is precommitted by digest;
//! checkpoint receipt facts are necessarily observed later, then schema-validated and hashed into
//! the durable transaction marker before its cursor advances.

use std::fmt;
use std::fs::OpenOptions;
use std::io::Read as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::catalog_transaction::{ApplyMode, ApplyRequest, ApplyResult};
use crate::cutover_admission::{
    AdmissionError, AdmissionResult, BeginCutover, BeginOutcome, CutoverAction, CutoverMarker,
    CutoverTransaction, ExternalCheckpointKind, FinalizedCutover, FinalizedWithOwnership, GateId,
    HostId, MutationAdmission, MutationBusy, PendingFence, ProviderFleetProofAction,
    ProviderFleetProofEvidence, ResumeCutover, ResumeOutcome, probe_mutation_admission,
};
use crate::ding_reconcile::{
    DingExecBackend, DingGenerationReader, DingReconcileAction, SystemDingExecBackend,
    SystemDingPartitionObserver,
};
use crate::exec_retirement::{
    RetirementApplyReceipt, RetirementApplyRequest, RetirementPreparation,
    RetirementPrepareRequest, RetirementSelector,
};
use crate::run::{ProviderFleetRuntimeObserver, Runner};

pub const CUTOVER_REQUEST_SCHEMA: &str = "st2.cutover-request.v1";
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
const MAX_CHECKPOINT_RECEIPT_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverRequest {
    pub schema: String,
    pub canonical_catalog: PathBuf,
    pub host: HostId,
    pub gate_id: GateId,
    pub source_catalog_sha256: String,
    pub program: Vec<CutoverAction>,
    pub retirement: CutoverRetirement,
    pub catalog_inputs: Vec<CutoverCatalogInput>,
    pub checkpoint_inputs: Vec<CutoverCheckpointInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CutoverRetirementSelector {
    Id { runtime_id: String },
    LegacySet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverRetirement {
    pub selector: CutoverRetirementSelector,
    pub plan_output: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverCatalogInput {
    pub action_index: usize,
    pub prepared: PathBuf,
    pub expect_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverCheckpointInput {
    pub action_index: usize,
    pub receipt: PathBuf,
}

impl CutoverRetirementSelector {
    fn to_retirement_selector(&self) -> RetirementSelector {
        match self {
            Self::Id { runtime_id } => RetirementSelector::Id(runtime_id.clone()),
            Self::LegacySet => RetirementSelector::LegacySet,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedCutoverRequest {
    request: CutoverRequest,
    request_sha256: String,
    source_path: PathBuf,
}

impl LoadedCutoverRequest {
    /// Read a bounded regular file without following a final symlink, check its caller-held digest,
    /// and require byte-for-byte canonical JSON.
    pub fn load(path: impl AsRef<Path>, expect_request_sha256: &str) -> AdmissionResult<Self> {
        validate_sha256("expected cutover request sha256", expect_request_sha256)?;
        let path = path.as_ref();
        let bytes = read_regular_nofollow(path, MAX_REQUEST_BYTES, "cutover request")?;
        let observed = sha256(&bytes);
        if observed != expect_request_sha256 {
            return Err(AdmissionError::Conflict(format!(
                "cutover request digest mismatch: expected {expect_request_sha256}, found {observed}"
            )));
        }
        Self::from_canonical_bytes(path.to_path_buf(), bytes, observed)
    }

    pub fn request(&self) -> &CutoverRequest {
        &self.request
    }

    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn begin(&self) -> AdmissionResult<DriverBeginOutcome> {
        let outcome = CutoverTransaction::begin(BeginCutover {
            catalog: crate::cutover_admission::CanonicalCatalog::open(
                &self.request.canonical_catalog,
            )?,
            host: self.request.host.clone(),
            gate_id: self.request.gate_id.clone(),
            request_sha256: self.request_sha256.clone(),
            source_catalog_sha256: self.request.source_catalog_sha256.clone(),
            program: self.request.program.clone(),
        })?;
        match outcome {
            BeginOutcome::Claimed(transaction) => Ok(DriverBeginOutcome::Claimed(
                CutoverDriver::bind(self.clone(), transaction)?,
            )),
            BeginOutcome::Fenced(fence) => Ok(DriverBeginOutcome::Fenced(fence)),
        }
    }

    pub fn resume(&self) -> AdmissionResult<DriverResumeOutcome> {
        let outcome = CutoverTransaction::resume(ResumeCutover {
            catalog: crate::cutover_admission::CanonicalCatalog::open(
                &self.request.canonical_catalog,
            )?,
            host: self.request.host.clone(),
            gate_id: self.request.gate_id.clone(),
            request_sha256: self.request_sha256.clone(),
        })?;
        match outcome {
            ResumeOutcome::Active(transaction) => Ok(DriverResumeOutcome::Active(
                CutoverDriver::bind(self.clone(), transaction)?,
            )),
            ResumeOutcome::Finalized(finalized) => {
                verify_authority(&self.request, &self.request_sha256, &finalized.marker)?;
                Ok(DriverResumeOutcome::Finalized(finalized))
            }
        }
    }

    /// Begin or resume the exact request and execute every internally available action.
    ///
    /// The driver stops only at a durable external-evidence boundary. Reinvocation with the same
    /// request bytes and digest resumes from the marker cursor.
    pub fn run(&self, runner: &dyn Runner) -> AdmissionResult<DriverRunOutcome> {
        let catalog =
            crate::cutover_admission::CanonicalCatalog::open(&self.request.canonical_catalog)?;
        let resume = ResumeCutover {
            catalog: catalog.clone(),
            host: self.request.host.clone(),
            gate_id: self.request.gate_id.clone(),
            request_sha256: self.request_sha256.clone(),
        };
        if let Some(finalized) = CutoverTransaction::inspect_finalized(resume.clone())? {
            verify_authority(&self.request, &self.request_sha256, &finalized.marker)?;
            return Ok(DriverRunOutcome::Finalized(finalized));
        }
        let active = self
            .request
            .canonical_catalog
            .join(".st2/cutover/active.json");
        let active_state = std::fs::symlink_metadata(&active);
        let opened = match active_state {
            Ok(_) => match self.resume() {
                Ok(DriverResumeOutcome::Active(driver)) => OpenedDriver::Active(driver),
                Ok(DriverResumeOutcome::Finalized(finalized)) => {
                    return Ok(DriverRunOutcome::Finalized(finalized));
                }
                Err(resume_error) => {
                    let catalog = crate::cutover_admission::CanonicalCatalog::open(
                        &self.request.canonical_catalog,
                    )?;
                    match probe_mutation_admission(&catalog, Some(&self.request.host))? {
                        MutationAdmission::Busy(busy) => {
                            return Ok(DriverRunOutcome::Fenced(DriverFence::Active(busy)));
                        }
                        MutationAdmission::Available => return Err(resume_error),
                    }
                }
            },
            Err(active_error) if active_error.kind() == std::io::ErrorKind::NotFound => {
                match self.begin() {
                    Ok(DriverBeginOutcome::Claimed(driver)) => OpenedDriver::Active(driver),
                    Ok(DriverBeginOutcome::Fenced(fence)) => {
                        return Ok(DriverRunOutcome::Fenced(DriverFence::Pending(fence)));
                    }
                    Err(AdmissionError::Busy(busy)) => {
                        return Ok(DriverRunOutcome::Fenced(DriverFence::Active(busy)));
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => {
                return Err(AdmissionError::Io {
                    context: format!("inspect active cutover marker {}", active.display()),
                    source: error,
                });
            }
        };
        let OpenedDriver::Active(driver) = opened;
        let exec_state_dir = crate::run::exec_state_dir(self.request.host.as_str());
        let ding_backend = SystemDingExecBackend::new(
            exec_state_dir.clone(),
            self.request.canonical_catalog.clone(),
        );
        let ding_reader = SystemDingPartitionObserver::new(
            exec_state_dir,
            self.request.canonical_catalog.clone(),
        );
        driver.run_to_boundary(runner, &ding_backend, &ding_reader)
    }

    fn from_canonical_bytes(
        source_path: PathBuf,
        bytes: Vec<u8>,
        request_sha256: String,
    ) -> AdmissionResult<Self> {
        let request: CutoverRequest = serde_json::from_slice(&bytes).map_err(|error| {
            AdmissionError::Invalid(format!("parse canonical cutover request: {error}"))
        })?;
        validate_request(&request)?;
        let canonical = canonical_request_bytes(&request)?;
        if canonical != bytes {
            return Err(AdmissionError::Invalid(
                "cutover request is not byte-for-byte canonical JSON".to_owned(),
            ));
        }
        Ok(Self {
            request,
            request_sha256,
            source_path,
        })
    }
}

pub enum DriverBeginOutcome {
    Claimed(CutoverDriver),
    Fenced(PendingFence),
}

pub enum DriverResumeOutcome {
    Active(CutoverDriver),
    Finalized(FinalizedCutover),
}

enum OpenedDriver {
    Active(CutoverDriver),
}

pub enum DriverFence {
    Active(MutationBusy),
    Pending(PendingFence),
}

pub enum DriverRunOutcome {
    Completed {
        finalized: FinalizedWithOwnership,
        provider_fleet_proof: Option<ProviderFleetProofEvidence>,
    },
    Finalized(FinalizedCutover),
    Fenced(DriverFence),
    NeedsCheckpoint {
        action_index: usize,
        kind: ExternalCheckpointKind,
        input_sha256: String,
        receipt: PathBuf,
    },
}

pub struct CutoverDriver {
    request: LoadedCutoverRequest,
    transaction: CutoverTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DriverStep {
    Catalog(CutoverCatalogInput),
    Checkpoint {
        kind: ExternalCheckpointKind,
        input_sha256: String,
        input: CutoverCheckpointInput,
    },
    DingReconcile,
    ProviderFleetProof,
    Finalize,
}

impl CutoverDriver {
    fn bind(
        request: LoadedCutoverRequest,
        transaction: CutoverTransaction,
    ) -> AdmissionResult<Self> {
        verify_authority(
            &request.request,
            &request.request_sha256,
            transaction.marker(),
        )?;
        Ok(Self {
            request,
            transaction,
        })
    }

    pub fn marker(&self) -> &CutoverMarker {
        self.transaction.marker()
    }

    fn run_to_boundary(
        mut self,
        runner: &dyn Runner,
        ding_backend: &dyn DingExecBackend,
        ding_reader: &dyn DingGenerationReader,
    ) -> AdmissionResult<DriverRunOutcome> {
        if self.transaction.marker().retirement_receipt.is_none() {
            if self.transaction.marker().retirement_plan.is_none() {
                self.prepare_retirement(self.request.request.retirement.plan_output.clone())?;
            }
            let plan_sha256 = self
                .transaction
                .marker()
                .retirement_plan
                .as_ref()
                .expect("preparation records retirement plan evidence")
                .plan_sha256
                .clone();
            self.apply_retirement(
                self.request.request.retirement.plan_output.clone(),
                plan_sha256,
            )?;
        }

        let mut provider_fleet_proof = None;
        loop {
            let index = self.transaction.marker().cursor;
            match classify_next(
                &self.request.request,
                &self.request.request_sha256,
                self.transaction.marker(),
            )? {
                DriverStep::Catalog(input) => {
                    self.apply_next_catalog(ApplyRequest {
                        catalog: self.request.request.canonical_catalog.clone(),
                        mode: ApplyMode::Prepared {
                            prepared: input.prepared,
                            expect_sha256: input.expect_sha256,
                        },
                    })?;
                }
                DriverStep::Checkpoint {
                    kind,
                    input_sha256,
                    input,
                } => match std::fs::symlink_metadata(&input.receipt) {
                    Ok(_) => self.record_observed_checkpoint(&input.receipt)?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(DriverRunOutcome::NeedsCheckpoint {
                            action_index: index,
                            kind,
                            input_sha256,
                            receipt: input.receipt.clone(),
                        });
                    }
                    Err(error) => {
                        return Err(AdmissionError::Io {
                            context: format!(
                                "inspect checkpoint receipt {}",
                                input.receipt.display()
                            ),
                            source: error,
                        });
                    }
                },
                DriverStep::DingReconcile => {
                    self.transaction
                        .permission()
                        .reconcile_dings_once(index, ding_backend)?;
                }
                DriverStep::ProviderFleetProof => {
                    let action = self.next_provider_fleet_proof()?.clone();
                    self.next_ding_reconcile_after_provider_fleet()?;
                    let observer = ProviderFleetRuntimeObserver::new(&action, runner);
                    provider_fleet_proof =
                        Some(self.transaction.permission().prove_provider_fleet_once(
                            index,
                            &observer,
                            ding_reader,
                        )?);
                }
                DriverStep::Finalize => break,
            }
        }

        let finalized = self.finalize()?;
        Ok(DriverRunOutcome::Completed {
            finalized,
            provider_fleet_proof,
        })
    }

    pub fn prepare_retirement(
        &mut self,
        output: impl Into<PathBuf>,
    ) -> AdmissionResult<RetirementPreparation> {
        self.verify()?;
        self.transaction
            .permission()
            .prepare_retirement_once(RetirementPrepareRequest {
                catalog: self.request.request.canonical_catalog.clone(),
                host: self.request.request.host.as_str().to_owned(),
                selector: self
                    .request
                    .request
                    .retirement
                    .selector
                    .to_retirement_selector(),
                expect_catalog_sha256: self.request.request.source_catalog_sha256.clone(),
                output: output.into(),
            })
    }

    pub fn apply_retirement(
        &mut self,
        plan: impl Into<PathBuf>,
        expect_plan_sha256: impl Into<String>,
    ) -> AdmissionResult<RetirementApplyReceipt> {
        self.verify()?;
        self.transaction
            .permission()
            .apply_retirement_once(RetirementApplyRequest {
                catalog: self.request.request.canonical_catalog.clone(),
                plan: plan.into(),
                expect_plan_sha256: expect_plan_sha256.into(),
            })
    }

    /// Apply the exact catalog transition at the durable cursor.
    pub fn apply_next_catalog(
        &mut self,
        request: ApplyRequest,
    ) -> AdmissionResult<Option<ApplyResult>> {
        self.verify()?;
        let supplied = crate::cutover_admission::CanonicalCatalog::open(&request.catalog)?;
        if supplied.as_path() != self.request.request.canonical_catalog {
            return Err(AdmissionError::Conflict(format!(
                "catalog apply belongs to {}, not request catalog {}",
                supplied.as_path().display(),
                self.request.request.canonical_catalog.display()
            )));
        }
        let index = self.transaction.marker().cursor;
        self.transaction
            .permission()
            .apply_catalog_transition_once(index, request)
    }

    /// Record the exact checkpoint at the durable cursor after checking the raw receipt bytes
    /// against a caller-held digest.
    pub fn record_next_checkpoint(
        &mut self,
        receipt: impl AsRef<Path>,
        expect_receipt_sha256: &str,
    ) -> AdmissionResult<()> {
        self.verify()?;
        validate_sha256("expected checkpoint receipt sha256", expect_receipt_sha256)?;
        let bytes = read_regular_nofollow(
            receipt.as_ref(),
            MAX_CHECKPOINT_RECEIPT_BYTES,
            "checkpoint receipt",
        )?;
        let observed = sha256(&bytes);
        if observed != expect_receipt_sha256 {
            return Err(AdmissionError::Conflict(format!(
                "checkpoint receipt digest mismatch: expected {expect_receipt_sha256}, found {observed}"
            )));
        }
        let index = self.transaction.marker().cursor;
        self.transaction
            .permission()
            .record_external_checkpoint(index, &bytes)
    }

    fn record_observed_checkpoint(&mut self, receipt: &Path) -> AdmissionResult<()> {
        self.verify()?;
        let bytes =
            read_regular_nofollow(receipt, MAX_CHECKPOINT_RECEIPT_BYTES, "checkpoint receipt")?;
        let index = self.transaction.marker().cursor;
        self.transaction
            .permission()
            .record_external_checkpoint(index, &bytes)
    }

    /// Return the immutable provider-fleet proof only when it is the exact next action.
    pub fn next_provider_fleet_proof(&self) -> AdmissionResult<&ProviderFleetProofAction> {
        self.verify()?;
        let marker = self.transaction.marker();
        match marker.program.get(marker.cursor) {
            Some(CutoverAction::ProviderFleetProof(action)) => Ok(action),
            _ => Err(AdmissionError::Conflict(format!(
                "action {} is not the exact next provider-fleet-proof action",
                marker.cursor
            ))),
        }
    }

    fn next_ding_reconcile_after_provider_fleet(&self) -> AdmissionResult<&DingReconcileAction> {
        let marker = self.transaction.marker();
        match marker.program.get(marker.cursor + 1) {
            Some(CutoverAction::DingReconcile(action)) => Ok(action),
            _ => Err(AdmissionError::Conflict(
                "provider-fleet proof is not followed by exact Ding reconciliation".to_owned(),
            )),
        }
    }

    pub fn finalize(mut self) -> AdmissionResult<FinalizedWithOwnership> {
        self.verify()?;
        self.transaction.permission().finalize()
    }

    fn verify(&self) -> AdmissionResult<()> {
        verify_authority(
            &self.request.request,
            &self.request.request_sha256,
            self.transaction.marker(),
        )
    }
}

fn classify_next(
    request: &CutoverRequest,
    request_sha256: &str,
    marker: &CutoverMarker,
) -> AdmissionResult<DriverStep> {
    verify_authority(request, request_sha256, marker)?;
    let Some(action) = marker.program.get(marker.cursor) else {
        return if marker.cursor == marker.program.len() {
            Ok(DriverStep::Finalize)
        } else {
            Err(AdmissionError::Conflict(
                "cutover cursor exceeds the exact request program".to_owned(),
            ))
        };
    };
    match action {
        CutoverAction::CatalogTransition(_) => {
            let input = request
                .catalog_inputs
                .iter()
                .find(|input| input.action_index == marker.cursor)
                .cloned()
                .ok_or_else(|| {
                    AdmissionError::Invalid(format!(
                        "request has no prepared catalog input for action {}",
                        marker.cursor
                    ))
                })?;
            Ok(DriverStep::Catalog(input))
        }
        CutoverAction::ExternalCheckpoint { kind, input_sha256 } => {
            let input = request
                .checkpoint_inputs
                .iter()
                .find(|input| input.action_index == marker.cursor)
                .cloned()
                .ok_or_else(|| {
                    AdmissionError::Invalid(format!(
                        "request has no checkpoint receipt input for action {}",
                        marker.cursor
                    ))
                })?;
            Ok(DriverStep::Checkpoint {
                kind: *kind,
                input_sha256: input_sha256.clone(),
                input,
            })
        }
        CutoverAction::DingReconcile(_) => Ok(DriverStep::DingReconcile),
        CutoverAction::ProviderFleetProof(_) => Ok(DriverStep::ProviderFleetProof),
    }
}

pub fn canonical_request_bytes(request: &CutoverRequest) -> AdmissionResult<Vec<u8>> {
    validate_request(request)?;
    serde_json::to_vec(request)
        .map_err(|error| AdmissionError::Invalid(format!("encode cutover request: {error}")))
}

pub fn canonical_request_sha256(request: &CutoverRequest) -> AdmissionResult<String> {
    canonical_request_bytes(request).map(|bytes| sha256(&bytes))
}

fn validate_request(request: &CutoverRequest) -> AdmissionResult<()> {
    if request.schema != CUTOVER_REQUEST_SCHEMA {
        return Err(AdmissionError::Invalid(format!(
            "cutover request schema must be {CUTOVER_REQUEST_SCHEMA}"
        )));
    }
    validate_sha256(
        "cutover source catalog sha256",
        &request.source_catalog_sha256,
    )?;
    crate::cutover_admission::validate_program(&request.source_catalog_sha256, &request.program)?;
    let canonical = crate::cutover_admission::CanonicalCatalog::open(&request.canonical_catalog)?;
    if canonical.as_path() != request.canonical_catalog {
        return Err(AdmissionError::Invalid(format!(
            "cutover request catalog path is not canonical: {}",
            request.canonical_catalog.display()
        )));
    }
    if let CutoverRetirementSelector::Id { runtime_id } = &request.retirement.selector {
        if runtime_id.is_empty()
            || runtime_id.len() > 128
            || !runtime_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(AdmissionError::Invalid(
                "retirement runtime id must be one safe component of at most 128 bytes".to_owned(),
            ));
        }
    }
    validate_absolute_artifact_path("retirement plan output", &request.retirement.plan_output)?;
    validate_exact_inputs(request)?;
    Ok(())
}

fn validate_exact_inputs(request: &CutoverRequest) -> AdmissionResult<()> {
    let mut catalog_indexes = std::collections::BTreeSet::new();
    for input in &request.catalog_inputs {
        validate_sha256("prepared catalog input sha256", &input.expect_sha256)?;
        validate_absolute_artifact_path("prepared catalog input", &input.prepared)?;
        if !catalog_indexes.insert(input.action_index)
            || !matches!(
                request.program.get(input.action_index),
                Some(CutoverAction::CatalogTransition(_))
            )
        {
            return Err(AdmissionError::Invalid(format!(
                "catalog input {} must map exactly once to a catalog-transition action",
                input.action_index
            )));
        }
    }
    let expected_catalog = request
        .program
        .iter()
        .filter(|action| matches!(action, CutoverAction::CatalogTransition(_)))
        .count();
    if catalog_indexes.len() != expected_catalog {
        return Err(AdmissionError::Invalid(
            "every catalog-transition action requires exactly one prepared input".to_owned(),
        ));
    }

    let mut checkpoint_indexes = std::collections::BTreeSet::new();
    for input in &request.checkpoint_inputs {
        validate_absolute_artifact_path("checkpoint receipt", &input.receipt)?;
        if !checkpoint_indexes.insert(input.action_index)
            || !matches!(
                request.program.get(input.action_index),
                Some(CutoverAction::ExternalCheckpoint { .. })
            )
        {
            return Err(AdmissionError::Invalid(format!(
                "checkpoint input {} must map exactly once to an external-checkpoint action",
                input.action_index
            )));
        }
    }
    let expected_checkpoints = request
        .program
        .iter()
        .filter(|action| matches!(action, CutoverAction::ExternalCheckpoint { .. }))
        .count();
    if checkpoint_indexes.len() != expected_checkpoints {
        return Err(AdmissionError::Invalid(
            "every external-checkpoint action requires exactly one receipt input".to_owned(),
        ));
    }
    Ok(())
}

fn validate_absolute_artifact_path(label: &str, path: &Path) -> AdmissionResult<()> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be an absolute normalized path"
        )));
    }
    Ok(())
}

fn verify_authority(
    request: &CutoverRequest,
    request_sha256: &str,
    marker: &CutoverMarker,
) -> AdmissionResult<()> {
    if marker.request_sha256 != request_sha256
        || marker.canonical_catalog != request.canonical_catalog
        || marker.host != request.host
        || marker.gate_id != request.gate_id
        || marker.source_catalog_sha256 != request.source_catalog_sha256
        || marker.program != request.program
    {
        return Err(AdmissionError::Conflict(
            "cutover transaction does not match the exact request authority".to_owned(),
        ));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> AdmissionResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_regular_nofollow(path: &Path, max_bytes: u64, label: &str) -> AdmissionResult<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| AdmissionError::Io {
            context: format!("open {label} {}", path.display()),
            source: error,
        })?;
    let metadata = file.metadata().map_err(|error| AdmissionError::Io {
        context: format!("inspect {label} {}", path.display()),
        source: error,
    })?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be a singly linked regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > max_bytes {
        return Err(AdmissionError::Invalid(format!(
            "{label} exceeds {max_bytes} bytes: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| AdmissionError::Io {
            context: format!("read {label} {}", path.display()),
            source: error,
        })?;
    if bytes.len() as u64 > max_bytes {
        return Err(AdmissionError::Invalid(format!(
            "{label} exceeds {max_bytes} bytes: {}",
            path.display()
        )));
    }
    Ok(bytes)
}

impl fmt::Debug for CutoverDriver {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("CutoverDriver")
            .field("request", &self.request)
            .field("marker", self.transaction.marker())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::symlink;

    use tempfile::TempDir;

    use super::*;
    use crate::cutover_admission::CutoverMarker;
    use crate::ding_reconcile::{DingDesiredExec, DingReconcileAction};

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn request(catalog: &Path) -> CutoverRequest {
        let argv = vec![
            "/nix/store/axe/bin/axe".to_owned(),
            "agent".to_owned(),
            "launch".to_owned(),
            "--persona".to_owned(),
            "worker".to_owned(),
            "--harness".to_owned(),
            "codex".to_owned(),
            "--model".to_owned(),
            "gpt-5".to_owned(),
            "--effort".to_owned(),
            "high".to_owned(),
            "--mode".to_owned(),
            "managed-unattended".to_owned(),
            "--boot".to_owned(),
            "managed-v1".to_owned(),
        ];
        let mut ding = DingDesiredExec {
            runtime_id: "worker-a.ding".to_owned(),
            canonical_argv: vec!["st2".to_owned(), "ding".to_owned()],
            canonical_cwd: catalog.to_path_buf(),
            canonical_env: BTreeMap::new(),
            launch_sha256: String::new(),
        };
        ding.launch_sha256 = crate::ding_reconcile::launch_sha256(&ding).unwrap();
        let dings = vec![ding];
        let mut provider = crate::cutover_admission::ProviderFleetEntry {
            identity: "worker-a".to_owned(),
            host: HostId::parse("host-a").unwrap(),
            provider: "codex".to_owned(),
            account: "account-a".to_owned(),
            persona: "worker".to_owned(),
            workspace: catalog.to_path_buf(),
            prompt: crate::cutover_admission::LaunchPromptAuthority {
                runtime_profile_path: PathBuf::from("/nix/store/profile.json"),
                runtime_profile_sha256: digest(3),
                persona_prompt_path: PathBuf::from("/nix/store/personas/worker.md"),
                persona_prompt_sha256: digest(4),
                launch_receipt_path: PathBuf::from("/run/axe/receipts/worker.json"),
                launch_receipt_sha256: digest(5),
                injection_kind:
                    crate::cutover_admission::PromptInjectionKind::CodexDeveloperInstructions,
            },
            canonical_argv: argv.clone(),
            argv_sha256: crate::cutover_admission::candidate_argv_sha256(&argv),
            profile_sha256: digest(3),
            harness: "codex".to_owned(),
            model: "gpt-5".to_owned(),
            effort: "high".to_owned(),
            mode: "managed-unattended".to_owned(),
            boot_contract: "managed-v1".to_owned(),
            launch_generation_id: "axe-generation-a".to_owned(),
            runtime_generation_id: "generation-a".to_owned(),
            trajectory_sha256: String::new(),
        };
        provider.trajectory_sha256 =
            crate::cutover_admission::provider_trajectory_sha256(&provider).unwrap();
        let providers = vec![provider];
        CutoverRequest {
            schema: CUTOVER_REQUEST_SCHEMA.to_owned(),
            canonical_catalog: catalog.to_path_buf(),
            host: HostId::parse("host-a").unwrap(),
            gate_id: GateId::parse("gate-a").unwrap(),
            source_catalog_sha256: digest(1),
            program: vec![
                CutoverAction::ProviderFleetProof(ProviderFleetProofAction {
                    providers_sha256: crate::cutover_admission::provider_entries_sha256(&providers)
                        .unwrap(),
                    providers,
                }),
                CutoverAction::DingReconcile(DingReconcileAction {
                    generation_id: "ding-generation-a".to_owned(),
                    desired_sha256: crate::ding_reconcile::desired_set_sha256(&dings).unwrap(),
                    desired: dings,
                }),
                CutoverAction::ExternalCheckpoint {
                    kind: crate::cutover_admission::ExternalCheckpointKind::FinalProof,
                    input_sha256: digest(4),
                },
                CutoverAction::ExternalCheckpoint {
                    kind: crate::cutover_admission::ExternalCheckpointKind::BusContinuity,
                    input_sha256: digest(5),
                },
            ],
            retirement: CutoverRetirement {
                selector: CutoverRetirementSelector::Id {
                    runtime_id: "42".to_owned(),
                },
                plan_output: catalog.join("retirement-plan.json"),
            },
            catalog_inputs: Vec::new(),
            checkpoint_inputs: vec![
                CutoverCheckpointInput {
                    action_index: 2,
                    receipt: catalog.join("final-proof.json"),
                },
                CutoverCheckpointInput {
                    action_index: 3,
                    receipt: catalog.join("bus-continuity.json"),
                },
            ],
        }
    }

    fn marker(request: &CutoverRequest, request_sha256: &str) -> CutoverMarker {
        let metadata = fs::metadata(&request.canonical_catalog).unwrap();
        CutoverMarker {
            schema: crate::cutover_admission::CUTOVER_TRANSACTION_SCHEMA.to_owned(),
            canonical_catalog: request.canonical_catalog.clone(),
            catalog_device: metadata.dev(),
            catalog_inode: metadata.ino(),
            host: request.host.clone(),
            gate_id: request.gate_id.clone(),
            request_sha256: request_sha256.to_owned(),
            source_catalog_sha256: request.source_catalog_sha256.clone(),
            program: request.program.clone(),
            cursor: 0,
            retirement_plan: None,
            forward_only_started: false,
            retirement_receipt: None,
            completed_checkpoints: Vec::new(),
            completed_ding_reconciles: Vec::new(),
            provider_fleet_proof: None,
            finalized: false,
        }
    }

    #[test]
    fn canonical_request_round_trips_with_caller_held_digest() {
        let temp = TempDir::new().unwrap();
        let request = request(temp.path());
        let bytes = canonical_request_bytes(&request).unwrap();
        let digest = sha256(&bytes);
        let path = temp.path().join("request.json");
        fs::write(&path, bytes).unwrap();

        let loaded = LoadedCutoverRequest::load(&path, &digest).unwrap();
        assert_eq!(loaded.request(), &request);
        assert_eq!(loaded.request_sha256(), digest);
    }

    #[test]
    fn request_rejects_wrong_digest_noncanonical_bytes_and_symlink() {
        let temp = TempDir::new().unwrap();
        let request = request(temp.path());
        let canonical = canonical_request_bytes(&request).unwrap();
        let path = temp.path().join("request.json");
        fs::write(&path, &canonical).unwrap();

        let error = LoadedCutoverRequest::load(&path, &digest(9)).unwrap_err();
        assert!(error.to_string().contains("digest mismatch"));

        let pretty = serde_json::to_vec_pretty(&request).unwrap();
        fs::write(&path, &pretty).unwrap();
        let error = LoadedCutoverRequest::load(&path, &sha256(&pretty)).unwrap_err();
        assert!(error.to_string().contains("not byte-for-byte canonical"));

        fs::write(&path, &canonical).unwrap();
        let link = temp.path().join("request-link.json");
        symlink(&path, &link).unwrap();
        let error = LoadedCutoverRequest::load(&link, &sha256(&canonical)).unwrap_err();
        assert!(error.to_string().contains("open cutover request"));
    }

    #[test]
    fn request_rejects_noncanonical_catalog_and_uncommitted_selector() {
        let temp = TempDir::new().unwrap();
        let alias = temp.path().join("alias");
        symlink(temp.path(), &alias).unwrap();
        let mut request = request(&alias);
        assert!(
            canonical_request_bytes(&request)
                .unwrap_err()
                .to_string()
                .contains("not canonical")
        );

        request.canonical_catalog = temp.path().to_path_buf();
        request.retirement.selector = CutoverRetirementSelector::Id {
            runtime_id: "../42".to_owned(),
        };
        assert!(
            canonical_request_bytes(&request)
                .unwrap_err()
                .to_string()
                .contains("safe component")
        );
    }

    #[test]
    fn transaction_binding_rejects_wrong_catalog_host_gate_source_program_and_digest() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        let request = request(left.path());
        let request_sha256 = canonical_request_sha256(&request).unwrap();
        let exact = marker(&request, &request_sha256);
        verify_authority(&request, &request_sha256, &exact).unwrap();

        let mut wrong = exact.clone();
        wrong.canonical_catalog = right.path().to_path_buf();
        assert!(verify_authority(&request, &request_sha256, &wrong).is_err());
        wrong = exact.clone();
        wrong.host = HostId::parse("host-b").unwrap();
        assert!(verify_authority(&request, &request_sha256, &wrong).is_err());
        wrong = exact.clone();
        wrong.gate_id = GateId::parse("gate-b").unwrap();
        assert!(verify_authority(&request, &request_sha256, &wrong).is_err());
        wrong = exact.clone();
        wrong.source_catalog_sha256 = digest(2);
        assert!(verify_authority(&request, &request_sha256, &wrong).is_err());
        wrong = exact.clone();
        wrong.program.push(CutoverAction::ExternalCheckpoint {
            kind: crate::cutover_admission::ExternalCheckpointKind::Cleanup,
            input_sha256: digest(3),
        });
        assert!(verify_authority(&request, &request_sha256, &wrong).is_err());
        assert!(verify_authority(&request, &digest(4), &exact).is_err());
    }

    #[test]
    fn request_requires_one_exact_artifact_for_each_external_action() {
        let temp = TempDir::new().unwrap();
        let mut request = request(temp.path());
        request.program.insert(
            0,
            CutoverAction::CatalogTransition(crate::cutover_admission::CatalogTransition {
                before_sha256: digest(1),
                after_sha256: digest(9),
            }),
        );
        for input in &mut request.checkpoint_inputs {
            input.action_index += 1;
        }
        request.catalog_inputs = vec![CutoverCatalogInput {
            action_index: 0,
            prepared: temp.path().join("prepared"),
            expect_sha256: digest(6),
        }];
        canonical_request_bytes(&request).unwrap();

        let mut missing = request.clone();
        missing.catalog_inputs.clear();
        assert!(
            canonical_request_bytes(&missing)
                .unwrap_err()
                .to_string()
                .contains("every catalog-transition")
        );

        let mut wrong_kind = request.clone();
        wrong_kind.checkpoint_inputs[0].action_index = 0;
        assert!(
            canonical_request_bytes(&wrong_kind)
                .unwrap_err()
                .to_string()
                .contains("external-checkpoint")
        );

        let mut duplicate = request;
        duplicate
            .catalog_inputs
            .push(duplicate.catalog_inputs[0].clone());
        assert!(
            canonical_request_bytes(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("exactly once")
        );
    }

    #[test]
    fn run_state_dispatches_adoption_ding_checkpoints_then_finalize() {
        let temp = TempDir::new().unwrap();
        let request = request(temp.path());
        let request_sha256 = canonical_request_sha256(&request).unwrap();
        let mut marker = marker(&request, &request_sha256);

        assert_eq!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::ProviderFleetProof
        );
        marker.cursor = 1;
        assert_eq!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::DingReconcile
        );
        marker.cursor = 2;
        assert!(matches!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::Checkpoint {
                kind: ExternalCheckpointKind::FinalProof,
                ..
            }
        ));
        marker.cursor = 3;
        assert!(matches!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::Checkpoint {
                kind: ExternalCheckpointKind::BusContinuity,
                ..
            }
        ));
        marker.cursor = 4;
        assert_eq!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::Finalize
        );

        assert!(classify_next(&request, &digest(9), &marker).is_err());
        marker.cursor = 5;
        assert!(
            classify_next(&request, &request_sha256, &marker)
                .unwrap_err()
                .to_string()
                .contains("cursor exceeds")
        );
    }
}
