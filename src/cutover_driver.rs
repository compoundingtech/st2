//! File-backed driver for one exact cutover transaction.
//!
//! The request file is an immutable authority manifest. Its canonical bytes bind the catalog,
//! host, gate, source declaration digest, complete action program, and completed predecessor
//! retirement receipt before the durable fence is published. Prepared catalog inputs and
//! checkpoint receipt output paths are canonical request data. Prepared content is precommitted by
//! digest; checkpoint receipt facts are necessarily observed later, then schema-validated and
//! hashed into the durable transaction marker before its cursor advances.

use std::fmt;
use std::fs::OpenOptions;
use std::io::Read as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::catalog_transaction::{ApplyMode, ApplyRequest, ApplyResult, prepare_apply};
use crate::cutover_admission::{
    AdmissionError, AdmissionResult, BeginCutover, BeginOutcome, CutoverAction, CutoverMarker,
    CutoverTransaction, ExternalCheckpointKind, FinalizedWithOwnership, GateId, HostId,
    MutationAdmission, MutationBusy, PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA, PendingFence,
    PredecessorRetiredDing, PredecessorRetirementEvidence, ProviderFleetProofAction,
    ProviderFleetProofEvidence, ResumeCutover, ResumeOutcome, probe_mutation_admission,
    validate_predecessor_retirement_evidence,
};
use crate::ding_reconcile::{
    DingExecBackend, DingGenerationReader, DingReconcileAction, SystemDingExecBackend,
    SystemDingPartitionObserver,
};
use crate::exec_retirement::{
    ExecRetirementReceipt, ExecRetirementStatus, RetirementAuthorityKind, SuccessorDesiredState,
};
use crate::run::{ProviderFleetRuntimeObserver, Runner};

pub const CUTOVER_REQUEST_SCHEMA: &str = "st2.cutover-request.v2";
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
const MAX_CHECKPOINT_RECEIPT_BYTES: u64 = 64 * 1024;
const MAX_PREDECESSOR_RETIREMENT_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverRequest {
    pub schema: String,
    pub canonical_catalog: PathBuf,
    pub host: HostId,
    pub gate_id: GateId,
    pub source_catalog_sha256: String,
    pub program: Vec<CutoverAction>,
    pub predecessor_retirement: CutoverPredecessorRetirementInput,
    pub catalog_inputs: Vec<CutoverCatalogInput>,
    pub checkpoint_inputs: Vec<CutoverCheckpointInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CutoverPredecessorRetirementInput {
    pub receipt: PathBuf,
    pub expect_sha256: String,
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
        let predecessor_retirement = self.preflight_evidence()?;
        let outcome = CutoverTransaction::begin(BeginCutover {
            catalog: crate::cutover_admission::CanonicalCatalog::open(
                &self.request.canonical_catalog,
            )?,
            host: self.request.host.clone(),
            gate_id: self.request.gate_id.clone(),
            request_sha256: self.request_sha256.clone(),
            source_catalog_sha256: self.request.source_catalog_sha256.clone(),
            program: self.request.program.clone(),
            predecessor_retirement,
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
                verify_authority(
                    &self.request,
                    &self.request_sha256,
                    &finalized.finalized.marker,
                )?;
                Ok(DriverResumeOutcome::Finalized(finalized))
            }
        }
    }

    /// Begin or resume the exact request and execute every internally available action.
    ///
    /// The driver stops only at a durable external-evidence boundary. Reinvocation with the same
    /// request bytes and digest resumes from the marker cursor.
    pub fn run(&self, runner: &dyn Runner) -> AdmissionResult<DriverRunOutcome> {
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
                let replay = ResumeCutover {
                    catalog: crate::cutover_admission::CanonicalCatalog::open(
                        &self.request.canonical_catalog,
                    )?,
                    host: self.request.host.clone(),
                    gate_id: self.request.gate_id.clone(),
                    request_sha256: self.request_sha256.clone(),
                };
                if let Some(finalized) = CutoverTransaction::reacquire_finalized_successor(replay)?
                {
                    verify_authority(
                        &self.request,
                        &self.request_sha256,
                        &finalized.finalized.marker,
                    )?;
                    return Ok(DriverRunOutcome::Finalized(finalized));
                }
                match self.begin() {
                    Ok(DriverBeginOutcome::Claimed(driver)) => OpenedDriver::Active(driver),
                    Ok(DriverBeginOutcome::Fenced(fence)) => {
                        let transaction = fence.wait_for_ownership()?;
                        OpenedDriver::Active(CutoverDriver::bind(self.clone(), transaction)?)
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

    pub fn preflight(&self) -> AdmissionResult<()> {
        self.preflight_evidence().map(drop)
    }

    fn preflight_evidence(&self) -> AdmissionResult<PredecessorRetirementEvidence> {
        preflight_artifacts(&self.request)
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
    Finalized(FinalizedWithOwnership),
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
    Finalized(FinalizedWithOwnership),
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
    validate_absolute_artifact_path(
        "predecessor retirement receipt",
        &request.predecessor_retirement.receipt,
    )?;
    validate_sha256(
        "predecessor retirement receipt sha256",
        &request.predecessor_retirement.expect_sha256,
    )?;
    validate_exact_inputs(request)?;
    Ok(())
}

fn preflight_artifacts(request: &CutoverRequest) -> AdmissionResult<PredecessorRetirementEvidence> {
    preflight_artifacts_with_state(request, &crate::run::exec_state_dir(request.host.as_str()))
}

fn preflight_artifacts_with_state(
    request: &CutoverRequest,
    state_dir: &Path,
) -> AdmissionResult<PredecessorRetirementEvidence> {
    validate_request(request)?;
    let catalog = crate::cutover_admission::CanonicalCatalog::open(&request.canonical_catalog)?;
    let receipt_path = canonical_artifact_path(
        "predecessor retirement receipt",
        &request.predecessor_retirement.receipt,
    )?;
    if receipt_path.starts_with(catalog.as_path()) || receipt_path.starts_with(state_dir) {
        return Err(AdmissionError::Invalid(
            "predecessor retirement receipt must be outside the catalog and exec state directory"
                .to_owned(),
        ));
    }
    let receipt_bytes = read_regular_nofollow(
        &receipt_path,
        MAX_PREDECESSOR_RETIREMENT_RECEIPT_BYTES,
        "predecessor retirement receipt",
    )?;
    let predecessor_retirement = parse_predecessor_retirement_receipt(request, &receipt_bytes)?;
    let mut outputs = std::collections::BTreeSet::new();
    let mut prospective_catalog = None;
    outputs.insert(receipt_path);
    for input in &request.checkpoint_inputs {
        let output = preflight_output(
            "checkpoint receipt",
            &input.receipt,
            catalog.as_path(),
            state_dir,
        )?;
        if !outputs.insert(output) {
            return Err(AdmissionError::Invalid(
                "cutover output artifact paths must be pairwise distinct".to_owned(),
            ));
        }
    }
    for input in &request.catalog_inputs {
        let transition = match request.program.get(input.action_index) {
            Some(CutoverAction::CatalogTransition(transition)) => transition,
            _ => {
                return Err(AdmissionError::Invalid(format!(
                    "catalog input {} does not map to a catalog transition",
                    input.action_index
                )));
            }
        };
        if input.expect_sha256 != transition.before_sha256 {
            return Err(AdmissionError::Invalid(format!(
                "catalog input {} expectation does not match its transition before digest",
                input.action_index
            )));
        }
        let prepared = prepare_apply(ApplyRequest {
            catalog: request.canonical_catalog.clone(),
            mode: ApplyMode::Prepared {
                prepared: input.prepared.clone(),
                expect_sha256: input.expect_sha256.clone(),
            },
        })
        .map_err(|error| {
            AdmissionError::Invalid(format!(
                "preflight prepared catalog input {}: {error:#}",
                input.action_index
            ))
        })?;
        if prepared.desired_root_sha256() != Some(transition.after_sha256.as_str()) {
            return Err(AdmissionError::Invalid(format!(
                "prepared catalog input {} does not produce its transition after digest",
                input.action_index
            )));
        }
        if prospective_catalog
            .as_ref()
            .is_none_or(|(index, _, _)| input.action_index > *index)
        {
            prospective_catalog = Some((
                input.action_index,
                input.prepared.canonicalize().map_err(|error| {
                    AdmissionError::Invalid(format!(
                        "canonicalize prepared prospective catalog {}: {error}",
                        input.prepared.display()
                    ))
                })?,
                transition.after_sha256.clone(),
            ));
        }
    }
    let (declaration_root, expected_sha256) = prospective_catalog
        .as_ref()
        .map(|(_, root, digest)| (root.as_path(), digest.as_str()))
        .unwrap_or((catalog.as_path(), request.source_catalog_sha256.as_str()));
    for action in &request.program {
        match action {
            CutoverAction::ProviderFleetProof(action) => {
                crate::run::validate_provider_action_preflight(
                    declaration_root,
                    catalog.as_path(),
                    &request.host,
                    action,
                    expected_sha256,
                )?;
            }
            CutoverAction::DingReconcile(action) => {
                crate::ding_reconcile::validate_ding_action_preflight(
                    declaration_root,
                    catalog.as_path(),
                    request.host.as_str(),
                    action,
                )?;
            }
            _ => {}
        }
    }
    Ok(predecessor_retirement)
}

fn canonical_artifact_path(label: &str, path: &Path) -> AdmissionResult<PathBuf> {
    validate_absolute_artifact_path(label, path)?;
    let parent = path
        .parent()
        .ok_or_else(|| AdmissionError::Invalid(format!("{label} has no parent")))?
        .canonicalize()
        .map_err(|error| {
            AdmissionError::Invalid(format!(
                "canonicalize {label} parent {}: {error}",
                path.display()
            ))
        })?;
    Ok(parent.join(
        path.file_name()
            .ok_or_else(|| AdmissionError::Invalid(format!("{label} has no filename")))?,
    ))
}

fn parse_predecessor_retirement_receipt(
    request: &CutoverRequest,
    bytes: &[u8],
) -> AdmissionResult<PredecessorRetirementEvidence> {
    let observed_sha256 = sha256(bytes);
    if observed_sha256 != request.predecessor_retirement.expect_sha256 {
        return Err(AdmissionError::Conflict(format!(
            "predecessor retirement receipt digest mismatch: expected {}, found {observed_sha256}",
            request.predecessor_retirement.expect_sha256
        )));
    }
    let receipt: ExecRetirementReceipt = serde_json::from_slice(bytes).map_err(|error| {
        AdmissionError::Invalid(format!("parse predecessor retirement receipt: {error}"))
    })?;
    let mut canonical = serde_json::to_vec(&receipt).map_err(|error| {
        AdmissionError::Invalid(format!("encode predecessor retirement receipt: {error}"))
    })?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(AdmissionError::Invalid(
            "predecessor retirement receipt is not byte-for-byte canonical JSON".to_owned(),
        ));
    }
    if receipt.schema != "st2.exec-retirement.v1"
        || receipt.status != ExecRetirementStatus::Completed
        || receipt.journal_schema != "st2.exec-retirement-journal.v1"
        || receipt.journal_status != "completed"
        || !receipt.forward_only_started
        || receipt.catalog != request.canonical_catalog
        || receipt.host != request.host.as_str()
        || receipt.catalog_sha256 != request.source_catalog_sha256
    {
        return Err(AdmissionError::Invalid(
            "predecessor retirement receipt is not the exact completed source-catalog transaction"
                .to_owned(),
        ));
    }
    for (label, digest) in [
        (
            "predecessor retirement request sha256",
            &receipt.request_sha256,
        ),
        ("predecessor retirement plan sha256", &receipt.plan_sha256),
        (
            "predecessor retirement catalog sha256",
            &receipt.catalog_sha256,
        ),
        (
            "predecessor retirement census sha256",
            &receipt.census_sha256,
        ),
        (
            "predecessor retirement journal sha256",
            &receipt.journal_sha256,
        ),
        (
            "predecessor retirement legacy partition sha256",
            &receipt.legacy_partition_sha256,
        ),
    ] {
        validate_sha256(label, digest)?;
    }
    let partition = receipt.legacy_partition.as_ref().ok_or_else(|| {
        AdmissionError::Invalid(
            "predecessor retirement receipt omits the complete legacy partition".to_owned(),
        )
    })?;
    let mut partition_hash = Sha256::new();
    partition_hash.update(b"st2.exec-retirement-legacy-partition.v1\0");
    let mut partition_bytes = serde_json::to_vec(&receipt.legacy_partition).map_err(|error| {
        AdmissionError::Invalid(format!("encode predecessor retirement partition: {error}"))
    })?;
    partition_bytes.push(b'\n');
    partition_hash.update(partition_bytes);
    let observed_partition_sha256 = format!("{:x}", partition_hash.finalize());
    if observed_partition_sha256 != receipt.legacy_partition_sha256 {
        return Err(AdmissionError::Invalid(
            "predecessor retirement receipt has an invalid legacy partition digest".to_owned(),
        ));
    }

    let mut projected = Vec::with_capacity(partition.len());
    let mut partition_ids = std::collections::BTreeSet::new();
    let mut previous_runtime_id = None;
    for row in partition {
        if row.task != "ding"
            || row.desired_state != SuccessorDesiredState::AbsentRetired
            || row.runtime_id != format!("{}.ding", row.agent)
            || !row
                .agent
                .starts_with(&format!("{}.", request.host.as_str()))
            || !partition_ids.insert(row.runtime_id.as_str())
            || previous_runtime_id.is_some_and(|previous: &str| previous >= row.runtime_id.as_str())
        {
            return Err(AdmissionError::Invalid(
                "predecessor retirement partition is not an exact all-retired local Ding set"
                    .to_owned(),
            ));
        }
        projected.push(PredecessorRetiredDing {
            runtime_id: row.runtime_id.clone(),
            agent: row.agent.clone(),
        });
        previous_runtime_id = Some(row.runtime_id.as_str());
    }

    let mut target_ids = std::collections::BTreeSet::new();
    let mut previous_target_id = None;
    for target in &receipt.targets {
        if target.generation_id.is_some()
            || !matches!(
                target.authority_kind,
                RetirementAuthorityKind::LegacyScopeV1 | RetirementAuthorityKind::StaleRecordOnly
            )
            || target.durable_phase != "record-retired"
            || target.record_before.relative_path != format!("{}.pid", target.runtime_id)
            || !target_ids.insert(target.runtime_id.as_str())
            || previous_target_id
                .is_some_and(|previous: &str| previous >= target.runtime_id.as_str())
        {
            return Err(AdmissionError::Invalid(
                "predecessor retirement target is not exact completed legacy authority".to_owned(),
            ));
        }
        previous_target_id = Some(target.runtime_id.as_str());
    }
    if target_ids != partition_ids || receipt.targets.len() != partition.len() {
        return Err(AdmissionError::Invalid(
            "predecessor retirement targets and all-retired Ding partition are not a bijection"
                .to_owned(),
        ));
    }

    let evidence = PredecessorRetirementEvidence {
        schema: PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA.to_owned(),
        receipt_sha256: observed_sha256,
        plan_sha256: receipt.plan_sha256,
        catalog_sha256: receipt.catalog_sha256,
        host: request.host.clone(),
        census_sha256: receipt.census_sha256,
        journal_sha256: receipt.journal_sha256,
        legacy_partition_sha256: receipt.legacy_partition_sha256,
        legacy_partition: projected,
    };
    validate_predecessor_retirement_evidence(
        &request.host,
        &request.source_catalog_sha256,
        &evidence,
    )?;
    Ok(evidence)
}

fn preflight_output(
    label: &str,
    path: &Path,
    catalog: &Path,
    state_dir: &Path,
) -> AdmissionResult<PathBuf> {
    validate_absolute_artifact_path(label, path)?;
    let parent = path
        .parent()
        .ok_or_else(|| AdmissionError::Invalid(format!("{label} has no parent")))?;
    let parent = parent.canonicalize().map_err(|error| {
        AdmissionError::Invalid(format!(
            "canonicalize {label} parent {}: {error}",
            parent.display()
        ))
    })?;
    let metadata = std::fs::symlink_metadata(&parent).map_err(|error| AdmissionError::Io {
        context: format!("inspect {label} parent {}", parent.display()),
        source: error,
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AdmissionError::Invalid(format!(
            "{label} parent is not a real directory: {}",
            parent.display()
        )));
    }
    let resolved = parent.join(
        path.file_name()
            .ok_or_else(|| AdmissionError::Invalid(format!("{label} has no filename")))?,
    );
    if resolved.starts_with(catalog) || resolved.starts_with(state_dir) {
        return Err(AdmissionError::Invalid(format!(
            "{label} must be outside the catalog and exec state directory"
        )));
    }
    match std::fs::symlink_metadata(&resolved) {
        Ok(metadata)
            if metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.nlink() == 1 => {}
        Ok(_) => {
            return Err(AdmissionError::Invalid(format!(
                "{label} is not a singly linked regular file: {}",
                resolved.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AdmissionError::Io {
                context: format!("inspect {label} {}", resolved.display()),
                source: error,
            });
        }
    }
    Ok(resolved)
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
        if let Some(CutoverAction::CatalogTransition(transition)) =
            request.program.get(input.action_index)
            && input.expect_sha256 != transition.before_sha256
        {
            return Err(AdmissionError::Invalid(format!(
                "catalog input {} expectation must equal its transition before digest",
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
        || marker.predecessor_retirement.receipt_sha256
            != request.predecessor_retirement.expect_sha256
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

    fn predecessor_receipt(catalog: &Path, host: &str, catalog_sha256: &str) -> Vec<u8> {
        use crate::exec_retirement::{
            ExecRetirementReceipt, ExecRetirementStatus, LegacySuccessorTask, RetiredDisposition,
            RetiredRecordEvidence, RetiredTarget,
        };

        let partition = vec![LegacySuccessorTask {
            runtime_id: format!("{host}.legacy.ding"),
            agent: format!("{host}.legacy"),
            task: "ding".to_owned(),
            desired_state: SuccessorDesiredState::AbsentRetired,
        }];
        let legacy_partition = Some(partition);
        let mut partition_bytes = serde_json::to_vec(&legacy_partition).unwrap();
        partition_bytes.push(b'\n');
        let mut partition_hash = Sha256::new();
        partition_hash.update(b"st2.exec-retirement-legacy-partition.v1\0");
        partition_hash.update(partition_bytes);
        let record = RetiredRecordEvidence {
            relative_path: format!("{host}.legacy.ding.pid"),
            device: 1,
            inode: 2,
            length: 3,
            modified_unix_ns: 4,
            sha256: digest(4),
        };
        let receipt = ExecRetirementReceipt {
            schema: "st2.exec-retirement.v1".to_owned(),
            request_sha256: digest(1),
            plan_sha256: digest(2),
            catalog: catalog.to_path_buf(),
            host: host.to_owned(),
            catalog_sha256: catalog_sha256.to_owned(),
            state_dir_device: 1,
            state_dir_inode: 2,
            journal_schema: "st2.exec-retirement-journal.v1".to_owned(),
            journal_sha256: digest(3),
            journal_status: "completed".to_owned(),
            status: ExecRetirementStatus::Completed,
            completed_at_unix_ms: 1,
            census_sha256: digest(5),
            forward_only_started: true,
            legacy_partition_sha256: format!("{:x}", partition_hash.finalize()),
            legacy_partition,
            targets: vec![RetiredTarget {
                runtime_id: format!("{host}.legacy.ding"),
                generation_id: None,
                authority_kind: RetirementAuthorityKind::StaleRecordOnly,
                disposition: RetiredDisposition::StaleRecordOnly,
                pid: 42,
                start_time_ticks: None,
                cgroup_path: None,
                scope_unit: None,
                cgroup_device: None,
                cgroup_inode: None,
                legacy_scope: None,
                membership: Vec::new(),
                freeze_observed: false,
                cgroup_outcome: None,
                durable_phase: "record-retired".to_owned(),
                record_before: record.clone(),
                record_after: RetiredRecordEvidence {
                    relative_path: ".retirements/record".to_owned(),
                    ..record
                },
            }],
        };
        let mut bytes = serde_json::to_vec(&receipt).unwrap();
        bytes.push(b'\n');
        bytes
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
                    kind: crate::cutover_admission::ExternalCheckpointKind::BusContinuity,
                    input_sha256: digest(5),
                },
                CutoverAction::ExternalCheckpoint {
                    kind: crate::cutover_admission::ExternalCheckpointKind::FinalProof,
                    input_sha256: digest(4),
                },
            ],
            predecessor_retirement: CutoverPredecessorRetirementInput {
                receipt: catalog
                    .parent()
                    .unwrap_or(catalog)
                    .join("predecessor-retirement.json"),
                expect_sha256: digest(9),
            },
            catalog_inputs: Vec::new(),
            checkpoint_inputs: vec![
                CutoverCheckpointInput {
                    action_index: 2,
                    receipt: catalog.join("bus-continuity.json"),
                },
                CutoverCheckpointInput {
                    action_index: 3,
                    receipt: catalog.join("final-proof.json"),
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
            predecessor_retirement: PredecessorRetirementEvidence {
                schema: PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA.to_owned(),
                receipt_sha256: request.predecessor_retirement.expect_sha256.clone(),
                plan_sha256: digest(8),
                catalog_sha256: request.source_catalog_sha256.clone(),
                host: request.host.clone(),
                census_sha256: digest(7),
                journal_sha256: digest(6),
                legacy_partition_sha256: digest(5),
                legacy_partition: vec![PredecessorRetiredDing {
                    runtime_id: format!("{}.legacy.ding", request.host.as_str()),
                    agent: format!("{}.legacy", request.host.as_str()),
                }],
            },
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
    fn predecessor_receipt_rejects_reordered_but_rehashed_partition() {
        let temp = TempDir::new().unwrap();
        let mut request = request(temp.path());
        let bytes = predecessor_receipt(
            temp.path(),
            request.host.as_str(),
            &request.source_catalog_sha256,
        );
        let mut receipt: ExecRetirementReceipt = serde_json::from_slice(&bytes).unwrap();
        let partition = receipt.legacy_partition.as_mut().unwrap();
        let mut earlier = partition[0].clone();
        earlier.runtime_id = format!("{}.alpha.ding", request.host.as_str());
        earlier.agent = format!("{}.alpha", request.host.as_str());
        partition.push(earlier);
        let mut target = receipt.targets[0].clone();
        target.runtime_id = format!("{}.alpha.ding", request.host.as_str());
        target.record_before.relative_path = format!("{}.pid", target.runtime_id);
        receipt.targets.push(target);
        let mut partition_bytes = serde_json::to_vec(&receipt.legacy_partition).unwrap();
        partition_bytes.push(b'\n');
        let mut partition_hash = Sha256::new();
        partition_hash.update(b"st2.exec-retirement-legacy-partition.v1\0");
        partition_hash.update(partition_bytes);
        receipt.legacy_partition_sha256 = format!("{:x}", partition_hash.finalize());
        let mut reordered = serde_json::to_vec(&receipt).unwrap();
        reordered.push(b'\n');
        request.predecessor_retirement.expect_sha256 = sha256(&reordered);

        assert!(
            parse_predecessor_retirement_receipt(&request, &reordered)
                .unwrap_err()
                .to_string()
                .contains("all-retired local Ding set")
        );
    }

    #[test]
    fn request_rejects_noncanonical_catalog_and_uncommitted_receipt_digest() {
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
        request.predecessor_retirement.expect_sha256 = "not-a-digest".to_owned();
        assert!(
            canonical_request_bytes(&request)
                .unwrap_err()
                .to_string()
                .contains("64 lowercase hexadecimal")
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
            expect_sha256: digest(1),
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
    fn invalid_predecessor_receipts_fail_preflight_without_publishing_a_fence() {
        let root = TempDir::new().unwrap();
        let catalog = root.path().join("catalog");
        let state = root.path().join("state/st2/host-a/exec");
        fs::create_dir(&catalog).unwrap();
        fs::create_dir_all(&state).unwrap();
        let mut base = request(&catalog);
        let receipt = predecessor_receipt(
            &base.canonical_catalog,
            base.host.as_str(),
            &base.source_catalog_sha256,
        );
        let receipt_path = root.path().join("predecessor-retirement.json");
        fs::write(&receipt_path, &receipt).unwrap();
        base.predecessor_retirement.receipt = receipt_path;
        base.predecessor_retirement.expect_sha256 = sha256(&receipt);
        base.checkpoint_inputs[0].receipt = root.path().join("bus.json");
        base.checkpoint_inputs[1].receipt = root.path().join("final.json");

        for invalid in [
            catalog.join("receipt.json"),
            state.join("receipt.json"),
            root.path().join("missing-parent/receipt.json"),
        ] {
            let mut request = base.clone();
            request.predecessor_retirement.receipt = invalid;
            assert!(preflight_artifacts_with_state(&request, &state).is_err());
            assert!(
                !catalog.join(".st2/cutover/active.json").exists(),
                "preflight refusal must precede durable fence publication"
            );
        }
    }

    #[test]
    fn tampered_predecessor_receipt_refuses_before_fence_publication() {
        let root = TempDir::new().unwrap();
        let catalog = root.path().join("catalog");
        fs::create_dir(&catalog).unwrap();
        let mut request = request(&catalog);
        request.source_catalog_sha256 =
            crate::catalog_transaction::declaration_root_sha256_locked(&catalog).unwrap();
        let receipt = predecessor_receipt(
            &request.canonical_catalog,
            request.host.as_str(),
            &request.source_catalog_sha256,
        );
        let receipt_path = root.path().join("predecessor-retirement.json");
        fs::write(&receipt_path, &receipt).unwrap();
        request.predecessor_retirement = CutoverPredecessorRetirementInput {
            receipt: receipt_path.clone(),
            expect_sha256: sha256(&receipt),
        };
        let request_bytes = canonical_request_bytes(&request).unwrap();
        let request_path = root.path().join("request.json");
        fs::write(&request_path, &request_bytes).unwrap();
        fs::write(&receipt_path, b"{}\n").unwrap();

        let loaded = LoadedCutoverRequest::load(&request_path, &sha256(&request_bytes)).unwrap();
        let error = match loaded.begin() {
            Err(error) => error,
            Ok(_) => panic!("tampered receipt must not publish a cutover fence"),
        };
        assert!(error.to_string().contains("receipt digest mismatch"));
        assert!(!catalog.join(".st2/cutover/active.json").exists());
    }

    #[test]
    fn deterministic_provider_and_ding_mismatches_refuse_before_fence_publication() {
        for mismatch in ["provider", "ding", "prepared-final"] {
            let root = TempDir::new().unwrap();
            let catalog = root.path().join("catalog");
            fs::create_dir(&catalog).unwrap();
            let mut request = request(&catalog);
            let workspace = root.path().join("workspace");
            fs::create_dir(&workspace).unwrap();
            let ding_workspace = root.path().join("ding-workspace");
            fs::create_dir(&ding_workspace).unwrap();
            let provider = request
                .program
                .iter_mut()
                .find_map(|action| match action {
                    CutoverAction::ProviderFleetProof(action) => Some(&mut action.providers[0]),
                    _ => None,
                })
                .unwrap();
            provider.workspace = workspace.clone();
            provider.trajectory_sha256 =
                crate::cutover_admission::provider_trajectory_sha256(provider).unwrap();
            let CutoverAction::ProviderFleetProof(action) = &mut request.program[0] else {
                unreachable!()
            };
            action.providers_sha256 =
                crate::cutover_admission::provider_entries_sha256(&action.providers).unwrap();
            let CutoverAction::DingReconcile(action) = &mut request.program[1] else {
                unreachable!()
            };
            action.desired[0].canonical_cwd = ding_workspace.canonicalize().unwrap();
            action.desired[0].launch_sha256 =
                crate::ding_reconcile::launch_sha256(&action.desired[0]).unwrap();
            action.desired_sha256 =
                crate::ding_reconcile::desired_set_sha256(&action.desired).unwrap();
            let provider_argv = match &request.program[0] {
                CutoverAction::ProviderFleetProof(action) => {
                    action.providers[0].canonical_argv.clone()
                }
                _ => unreachable!(),
            };
            let argv = provider_argv
                .iter()
                .map(|argument| format!("{argument:?}"))
                .collect::<Vec<_>>()
                .join(" ");
            let declaration = format!(
                r#"agent "worker-a" {{
  identity "worker-a"
  host "host-a"
  workspace {workspace:?}
  pty "agent" {{
    lifecycle "adopt-only"
    argv {argv}
    env {{
      AGENT_PERSONA "worker"
      AGENT_RUNTIME_PROFILE "/nix/store/profile.json"
    }}
  }}
  exec "ding" {{
    id "worker-a.ding"
    argv "st2" "ding" "--identity" "host-a.worker-a" "--root" "$ST_ROOT"
    cwd {ding_cwd:?}
  }}
}}
"#,
                workspace = workspace,
                ding_cwd = ding_workspace,
            );
            let declaration_path = catalog.join("agents/host-a/worker-a/agent.kdl");
            fs::create_dir_all(declaration_path.parent().unwrap()).unwrap();
            fs::write(&declaration_path, &declaration).unwrap();
            request.source_catalog_sha256 =
                crate::catalog_transaction::declaration_root_sha256_locked(&catalog).unwrap();
            let receipt = predecessor_receipt(
                &request.canonical_catalog,
                request.host.as_str(),
                &request.source_catalog_sha256,
            );
            let receipt_path = root.path().join("predecessor-retirement.json");
            fs::write(&receipt_path, &receipt).unwrap();
            request.predecessor_retirement = CutoverPredecessorRetirementInput {
                receipt: receipt_path,
                expect_sha256: sha256(&receipt),
            };
            request.checkpoint_inputs[0].receipt = root.path().join("bus.json");
            request.checkpoint_inputs[1].receipt = root.path().join("final.json");
            if mismatch == "prepared-final" {
                let prepared = root.path().join("prepared");
                let prepared_declaration = prepared.join("agents/host-a/worker-a/agent.kdl");
                fs::create_dir_all(prepared_declaration.parent().unwrap()).unwrap();
                fs::write(
                    prepared_declaration,
                    declaration.replace("\"gpt-5\"", "\"opus\""),
                )
                .unwrap();
                let after =
                    crate::catalog_transaction::declaration_root_sha256_locked(&prepared).unwrap();
                request.program.insert(
                    0,
                    CutoverAction::CatalogTransition(crate::cutover_admission::CatalogTransition {
                        before_sha256: request.source_catalog_sha256.clone(),
                        after_sha256: after,
                    }),
                );
                for input in &mut request.checkpoint_inputs {
                    input.action_index += 1;
                }
                request.catalog_inputs.push(CutoverCatalogInput {
                    action_index: 0,
                    prepared,
                    expect_sha256: request.source_catalog_sha256.clone(),
                });
            }
            if mismatch == "provider" {
                let action = request
                    .program
                    .iter_mut()
                    .find_map(|action| match action {
                        CutoverAction::ProviderFleetProof(action) => Some(action),
                        _ => None,
                    })
                    .unwrap();
                action.providers[0].model = "foreign-model".to_owned();
                action.providers[0].trajectory_sha256 =
                    crate::cutover_admission::provider_trajectory_sha256(&action.providers[0])
                        .unwrap();
                action.providers_sha256 =
                    crate::cutover_admission::provider_entries_sha256(&action.providers).unwrap();
            }
            if mismatch == "ding" {
                let action = request
                    .program
                    .iter_mut()
                    .find_map(|action| match action {
                        CutoverAction::DingReconcile(action) => Some(action),
                        _ => None,
                    })
                    .unwrap();
                action.desired[0]
                    .canonical_env
                    .insert("FOREIGN".to_owned(), "1".to_owned());
                action.desired[0].launch_sha256 =
                    crate::ding_reconcile::launch_sha256(&action.desired[0]).unwrap();
                action.desired_sha256 =
                    crate::ding_reconcile::desired_set_sha256(&action.desired).unwrap();
            }
            let bytes = canonical_request_bytes(&request).unwrap();
            let request_path = root.path().join("request.json");
            fs::write(&request_path, &bytes).unwrap();
            let loaded = LoadedCutoverRequest::load(&request_path, &sha256(&bytes)).unwrap();
            assert!(
                loaded.begin().is_err(),
                "{mismatch} mismatch published a cutover"
            );
            assert!(
                !catalog.join(".st2/cutover/active.json").exists(),
                "{mismatch} mismatch published an active fence"
            );
        }
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
                kind: ExternalCheckpointKind::BusContinuity,
                ..
            }
        ));
        marker.cursor = 3;
        assert!(matches!(
            classify_next(&request, &request_sha256, &marker).unwrap(),
            DriverStep::Checkpoint {
                kind: ExternalCheckpointKind::FinalProof,
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
