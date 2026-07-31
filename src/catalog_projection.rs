//! Deterministic control-plane migration projections from one retained catalog snapshot.
//!
//! This policy layer is a child of `catalog_transaction`, so it composes with the exact same
//! no-follow capture, declaration projection, validation, hashing, and atomic publication
//! machinery without exposing a second filesystem model.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{
    DeclarationProjection, ProjectionSource, absolute_path, canonical_real_dir,
    canonical_real_dir_no_alias, capture_prepared_catalog, capture_projection_bundle,
    hash_projection, materialize_projection, project, rename_noreplace, sync_dir, sync_tree_dirs,
    validate_sha256,
};

const PROJECTION_SCHEMA: &str = "st2.catalog-projection.v1";
const PROJECTION_BUNDLE_HASH_DOMAIN: &[u8] = b"st2.catalog-projection-bundle.v1\0";

#[derive(Debug)]
pub struct CatalogProjectionRequest {
    /// Logical catalog address whose path semantics the retained snapshot captured.
    pub catalog: PathBuf,
    /// A retained canonical snapshot produced by `catalog snapshot`.
    pub snapshot: PathBuf,
    /// The exact declaration-root digest returned when the snapshot was captured.
    pub expect_sha256: String,
    /// Create-only atomic bundle destination.
    pub output: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogProjectionResult {
    pub schema: &'static str,
    pub output: PathBuf,
    /// Domain-separated digest of `receipt.json` plus the three child projection roots.
    /// The sibling `bundle.sha256` file is deliberately excluded to avoid self-reference.
    pub bundle_sha256: String,
    pub receipt: CatalogProjectionReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionReceipt {
    pub schema: String,
    pub logical_catalog: PathBuf,
    pub source_root_sha256: String,
    pub admission: CatalogProjectionAdmission,
    pub service: CatalogProjectionArtifact,
    pub adopt_only: CatalogProjectionArtifact,
    pub provider_witness: CatalogProjectionArtifact,
    pub equality: CatalogProjectionEquality,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionAdmission {
    /// Whether ordinary `catalog apply` full admission can accept these exact bytes now.
    pub apply_admissible: bool,
    /// Exact projection-only exceptions. Version 1 permits only `render-owner-conflict`.
    pub exceptions: Vec<CatalogProjectionAdmissionException>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionAdmissionException {
    pub code: String,
    pub count: usize,
    pub evidence: Vec<CatalogProjectionAdmissionEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionAdmissionEvidence {
    pub path: String,
    pub agent: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionArtifact {
    /// Path relative to the atomic projection bundle.
    pub path: String,
    pub root_sha256: String,
    pub entries: usize,
    /// Every Agent identity, including agents without a provider PTY.
    pub agent_ids: Vec<String>,
    /// Agent identities carrying at least one provider PTY, including retired agents.
    pub provider_agent_ids: Vec<String>,
    /// Every provider PTY task, including retired desired-absent tasks.
    pub provider_task_ids: Vec<String>,
    /// Provider PTY tasks whose agents are not retired and are desired running.
    pub active_provider_task_ids: Vec<String>,
    /// Provider PTY tasks whose agents are retired and are desired absent.
    pub retired_absent_provider_task_ids: Vec<String>,
    pub exec_task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProjectionEquality {
    pub service_to_adopt_agent_ids: bool,
    pub service_to_witness_agent_ids: bool,
    pub service_to_adopt_provider_agent_ids: bool,
    pub service_to_adopt_provider_task_ids: bool,
    pub service_to_adopt_active_provider_task_ids: bool,
    pub service_to_adopt_retired_provider_task_ids: bool,
    pub service_active_to_witness_provider_task_ids: bool,
}

#[derive(Debug, Clone, Copy)]
enum ProjectionKind {
    AdoptOnly,
    ProviderWitness,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum CatalogProjectionChild {
    Service,
    AdoptOnly,
}

#[derive(Debug)]
pub struct CatalogProjectionApplyRequest {
    pub catalog: PathBuf,
    pub bundle: PathBuf,
    pub child: CatalogProjectionChild,
    pub expect_bundle_sha256: String,
    pub expect_sha256: String,
}

/// Derive one atomic, restart-safe control-plane migration bundle from one retained snapshot.
///
/// The source is captured once through retained no-follow descriptors. All three children and the
/// typed receipt are computed from that one immutable capture; this command never opens a live
/// catalog or mutates runtime state.
pub fn project_catalog(request: CatalogProjectionRequest) -> Result<CatalogProjectionResult> {
    validate_sha256(&request.expect_sha256)?;
    let logical_catalog = canonical_real_dir(&request.catalog, "logical catalog")?;
    let snapshot_input = absolute_path(&request.snapshot)?;
    let snapshot = canonical_real_dir_no_alias(&request.snapshot, "catalog snapshot")?;
    anyhow::ensure!(
        snapshot_input == snapshot,
        "catalog snapshot path must be its canonical absolute path: {}",
        request.snapshot.display()
    );
    let output = absolute_path(&request.output)?;
    anyhow::ensure!(
        !output.starts_with(&snapshot),
        "projection output must be outside the snapshot: {}",
        output.display()
    );
    anyhow::ensure!(
        !output.starts_with(&logical_catalog),
        "projection output resolves inside the logical catalog: {}",
        output.display()
    );
    let parent = output.parent().context("projection output has no parent")?;
    let parent = canonical_real_dir(parent, "projection output parent")?;
    let output = parent.join(
        output
            .file_name()
            .context("projection output has no final path component")?,
    );
    anyhow::ensure!(
        !output.starts_with(&snapshot),
        "projection output resolves inside the snapshot: {}",
        output.display()
    );
    anyhow::ensure!(
        !output.starts_with(&logical_catalog),
        "projection output resolves inside the logical catalog: {}",
        output.display()
    );
    match fs::symlink_metadata(&output) {
        Ok(_) => anyhow::bail!(
            "projection output already exists (projection bundles are create-only): {}",
            output.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect projection output {}", output.display()));
        }
    }

    let captured = tempfile::tempdir().context("create retained snapshot capture")?;
    capture_prepared_catalog(&snapshot, captured.path())?;
    let service = project(
        captured.path(),
        ProjectionSource::Prepared,
        &logical_catalog,
    )
    .context("validate retained service snapshot")?;
    anyhow::ensure!(
        service.root_sha256 == request.expect_sha256,
        "catalog snapshot root sha256 mismatch: expected {}, found {}",
        request.expect_sha256,
        service.root_sha256
    );
    let admission = validate_projectable_catalog(captured.path(), false)
        .context("validate retained service snapshot")?;

    let adopt_only = transform_provider_projection(&service, ProjectionKind::AdoptOnly)?;
    let provider_witness =
        transform_provider_projection(&service, ProjectionKind::ProviderWitness)?;

    let stage = tempfile::Builder::new()
        .prefix(".st2-catalog-projection-")
        .tempdir_in(&parent)
        .with_context(|| format!("create projection stage in {}", parent.display()))?;
    let service_path = stage.path().join("service");
    let adopt_path = stage.path().join("adopt-only");
    let witness_path = stage.path().join("provider-witness");
    fs::create_dir(&service_path)?;
    fs::create_dir(&adopt_path)?;
    fs::create_dir(&witness_path)?;
    materialize_projection(&service, &service_path)?;
    materialize_projection(&adopt_only, &adopt_path)?;
    materialize_projection(&provider_witness, &witness_path)?;
    let intended_roots = (
        service.root_sha256.clone(),
        adopt_only.root_sha256.clone(),
        provider_witness.root_sha256.clone(),
    );

    // Attest the actual private-stage bytes, not merely the in-memory values intended for them.
    let service = project(&service_path, ProjectionSource::Prepared, &logical_catalog)
        .context("re-project materialized service bytes")?;
    let adopt_only = project(&adopt_path, ProjectionSource::Prepared, &logical_catalog)
        .context("re-project materialized adopt-only bytes")?;
    let provider_witness = project(&witness_path, ProjectionSource::Prepared, &logical_catalog)
        .context("re-project materialized provider-witness bytes")?;
    anyhow::ensure!(
        (
            &service.root_sha256,
            &adopt_only.root_sha256,
            &provider_witness.root_sha256,
        ) == (&intended_roots.0, &intended_roots.1, &intended_roots.2),
        "materialized projection bytes differ from the derived projections"
    );

    let service_admission = validate_projectable_catalog(&service_path, false)
        .context("validate materialized service projection")?;
    let adopt_admission = validate_projectable_catalog(&adopt_path, false)
        .context("validate materialized adopt-only projection")?;
    anyhow::ensure!(
        admission == service_admission && admission == adopt_admission,
        "provider projection changed catalog admission evidence"
    );
    validate_projectable_catalog(&witness_path, true)
        .context("validate materialized provider-only witness projection")?;

    let service_artifact = projection_artifact(&service_path, "service", &service)?;
    let adopt_artifact = projection_artifact(&adopt_path, "adopt-only", &adopt_only)?;
    let witness_artifact =
        projection_artifact(&witness_path, "provider-witness", &provider_witness)?;
    let equality = CatalogProjectionEquality {
        service_to_adopt_agent_ids: service_artifact.agent_ids == adopt_artifact.agent_ids,
        service_to_witness_agent_ids: service_artifact.agent_ids == witness_artifact.agent_ids,
        service_to_adopt_provider_agent_ids: service_artifact.provider_agent_ids
            == adopt_artifact.provider_agent_ids,
        service_to_adopt_provider_task_ids: service_artifact.provider_task_ids
            == adopt_artifact.provider_task_ids,
        service_to_adopt_active_provider_task_ids: service_artifact.active_provider_task_ids
            == adopt_artifact.active_provider_task_ids,
        service_to_adopt_retired_provider_task_ids: service_artifact
            .retired_absent_provider_task_ids
            == adopt_artifact.retired_absent_provider_task_ids,
        service_active_to_witness_provider_task_ids: service_artifact.active_provider_task_ids
            == witness_artifact.provider_task_ids,
    };
    anyhow::ensure!(
        equality.service_to_adopt_agent_ids
            && equality.service_to_witness_agent_ids
            && equality.service_to_adopt_provider_agent_ids
            && equality.service_to_adopt_provider_task_ids
            && equality.service_to_adopt_active_provider_task_ids
            && equality.service_to_adopt_retired_provider_task_ids
            && equality.service_active_to_witness_provider_task_ids,
        "provider projection changed agent or provider task identity"
    );
    let mut partition = service_artifact.active_provider_task_ids.clone();
    partition.extend(
        service_artifact
            .retired_absent_provider_task_ids
            .iter()
            .cloned(),
    );
    partition.sort();
    anyhow::ensure!(
        partition == service_artifact.provider_task_ids,
        "active and retired provider task ids do not exactly partition the source"
    );
    anyhow::ensure!(
        witness_artifact.retired_absent_provider_task_ids.is_empty(),
        "provider witness retained a retired provider task"
    );
    anyhow::ensure!(
        adopt_artifact.exec_task_ids == service_artifact.exec_task_ids,
        "adopt-only projection changed exec task identity"
    );
    anyhow::ensure!(
        witness_artifact.exec_task_ids.is_empty(),
        "provider-only witness contains exec tasks"
    );

    let receipt = CatalogProjectionReceipt {
        schema: PROJECTION_SCHEMA.to_owned(),
        logical_catalog,
        source_root_sha256: service.root_sha256.clone(),
        admission,
        service: service_artifact,
        adopt_only: adopt_artifact,
        provider_witness: witness_artifact,
        equality,
    };
    let mut receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
    receipt_bytes.push(b'\n');
    write_new_file(&stage.path().join("receipt.json"), &receipt_bytes)?;
    let bundle_sha256 = hash_projection_bundle(&receipt, &receipt_bytes);
    write_new_file(
        &stage.path().join("bundle.sha256"),
        format!("{bundle_sha256}\n").as_bytes(),
    )?;
    sync_tree_dirs(stage.path())?;
    rename_noreplace(&stage.keep(), &output)
        .with_context(|| format!("publish projection bundle {}", output.display()))?;
    sync_dir(&parent)?;

    Ok(CatalogProjectionResult {
        schema: PROJECTION_SCHEMA,
        output,
        bundle_sha256,
        receipt,
    })
}

/// Verify a projection bundle's typed receipt, logical target, outer digest, and selected
/// materialized child before delegating to the ordinary full-admission/CAS apply transaction.
pub fn apply_projection_bundle(
    request: CatalogProjectionApplyRequest,
) -> Result<super::ApplyResult> {
    validate_sha256(&request.expect_sha256)?;
    validate_sha256(&request.expect_bundle_sha256)?;
    let logical_catalog = canonical_real_dir(&request.catalog, "catalog")?;
    let bundle_input = absolute_path(&request.bundle)?;
    let bundle = canonical_real_dir_no_alias(&request.bundle, "projection bundle")?;
    anyhow::ensure!(
        bundle_input == bundle,
        "projection bundle path must be its canonical absolute path: {}",
        request.bundle.display()
    );
    anyhow::ensure!(
        !bundle.starts_with(&logical_catalog),
        "projection bundle must be outside the live catalog: {}",
        bundle.display()
    );

    // One retained no-follow capture is the sole authority below this point. The caller-held
    // bundle digest detects any concurrent or pre-existing rewrite of the public bundle, while
    // ordinary apply later captures only this private, stable child.
    let captured_bundle = tempfile::tempdir().context("create projection-bundle capture root")?;
    capture_projection_bundle(&bundle, captured_bundle.path())
        .context("capture projection bundle through retained descriptors")?;
    let bundle = captured_bundle.path();
    let receipt_bytes = read_real_file(&bundle.join("receipt.json"), "projection receipt")?;
    let receipt: CatalogProjectionReceipt =
        serde_json::from_slice(&receipt_bytes).context("parse projection receipt")?;
    anyhow::ensure!(
        receipt.schema == PROJECTION_SCHEMA,
        "unsupported projection receipt schema: {}",
        receipt.schema
    );
    anyhow::ensure!(
        receipt.logical_catalog == logical_catalog,
        "projection receipt logical catalog {} does not match apply target {}",
        receipt.logical_catalog.display(),
        logical_catalog.display()
    );
    validate_sha256(&receipt.source_root_sha256)
        .context("validate projection receipt source root sha256")?;
    anyhow::ensure!(
        receipt.source_root_sha256 == request.expect_sha256,
        "projection receipt source root {} does not match expected live root {}",
        receipt.source_root_sha256,
        request.expect_sha256
    );
    let recorded_bundle_hash = String::from_utf8(read_real_file(
        &bundle.join("bundle.sha256"),
        "projection bundle digest",
    )?)
    .context("projection bundle digest is not UTF-8")?;
    let recorded_bundle_hash = recorded_bundle_hash.trim();
    validate_sha256(recorded_bundle_hash)?;
    let actual_bundle_hash = hash_projection_bundle(&receipt, &receipt_bytes);
    anyhow::ensure!(
        recorded_bundle_hash == actual_bundle_hash,
        "projection bundle sha256 mismatch: expected {recorded_bundle_hash}, found {actual_bundle_hash}"
    );
    anyhow::ensure!(
        actual_bundle_hash == request.expect_bundle_sha256,
        "projection bundle sha256 mismatch: caller expected {}, found {actual_bundle_hash}",
        request.expect_bundle_sha256
    );
    anyhow::ensure!(
        receipt.service.root_sha256 == receipt.source_root_sha256,
        "projection service root does not match receipt source root"
    );

    let (relative, artifact) = match request.child {
        CatalogProjectionChild::Service => ("service", &receipt.service),
        CatalogProjectionChild::AdoptOnly => ("adopt-only", &receipt.adopt_only),
    };
    anyhow::ensure!(
        artifact.path == relative,
        "projection receipt child path mismatch: expected {relative}, found {}",
        artifact.path
    );
    verify_projection_child(
        &bundle.join("service"),
        "service",
        &receipt.service,
        &logical_catalog,
        false,
    )?;
    verify_projection_child(
        &bundle.join("adopt-only"),
        "adopt-only",
        &receipt.adopt_only,
        &logical_catalog,
        false,
    )?;
    verify_projection_child(
        &bundle.join("provider-witness"),
        "provider-witness",
        &receipt.provider_witness,
        &logical_catalog,
        true,
    )?;
    let prepared = bundle.join(relative);

    super::apply(super::ApplyRequest {
        catalog: logical_catalog,
        mode: super::ApplyMode::Prepared {
            prepared,
            expect_sha256: request.expect_sha256,
        },
    })
}

fn verify_projection_child(
    prepared: &Path,
    relative: &'static str,
    artifact: &CatalogProjectionArtifact,
    logical_catalog: &Path,
    allow_identity_witnesses: bool,
) -> Result<()> {
    anyhow::ensure!(
        artifact.path == relative,
        "projection receipt child path mismatch: expected {relative}, found {}",
        artifact.path
    );
    let verified = project(prepared, ProjectionSource::Prepared, logical_catalog)
        .with_context(|| format!("verify projection child {relative}"))?;
    anyhow::ensure!(
        verified.root_sha256 == artifact.root_sha256 && verified.entries() == artifact.entries,
        "projection child {relative} does not match its receipt"
    );
    validate_projectable_catalog(prepared, allow_identity_witnesses)
        .with_context(|| format!("admit projection child {relative}"))?;
    let verified_artifact = projection_artifact(prepared, relative, &verified)?;
    anyhow::ensure!(
        verified_artifact.agent_ids == artifact.agent_ids
            && verified_artifact.provider_agent_ids == artifact.provider_agent_ids
            && verified_artifact.provider_task_ids == artifact.provider_task_ids
            && verified_artifact.active_provider_task_ids == artifact.active_provider_task_ids
            && verified_artifact.retired_absent_provider_task_ids
                == artifact.retired_absent_provider_task_ids
            && verified_artifact.exec_task_ids == artifact.exec_task_ids,
        "projection child {relative} identities do not match its receipt"
    );
    Ok(())
}

fn transform_provider_projection(
    source: &DeclarationProjection,
    kind: ProjectionKind,
) -> Result<DeclarationProjection> {
    let mut projection = source.clone();
    for (relative, file) in &mut projection.files {
        if !is_canonical_agent_spec_path(relative) {
            continue;
        }
        let text = std::str::from_utf8(&file.bytes)
            .with_context(|| format!("canonical Agent Spec is not UTF-8: {relative}"))?;
        let mut document = KdlDocument::parse(text)
            .map_err(|error| anyhow::anyhow!("parse canonical Agent Spec {relative}: {error}"))?;
        let agent_indexes = document
            .nodes()
            .iter()
            .enumerate()
            .filter(|(_, node)| node.name().value() == "agent")
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            agent_indexes.len() == 1,
            "canonical Agent Spec must contain exactly one agent: {relative}"
        );
        transform_agent_node(&mut document.nodes_mut()[agent_indexes[0]], kind)
            .with_context(|| format!("project canonical Agent Spec {relative}"))?;
        document.autoformat();
        file.bytes = document.to_string().into_bytes();
    }
    projection.root_sha256 = hash_projection(&projection.files, &projection.workspace_dirs);
    Ok(projection)
}

fn is_canonical_agent_spec_path(relative: &str) -> bool {
    let components = relative.split('/').collect::<Vec<_>>();
    components.len() == 4 && components[0] == "agents" && components[3] == "agent.kdl"
}

fn transform_agent_node(agent: &mut KdlNode, kind: ProjectionKind) -> Result<()> {
    let children = agent
        .children_mut()
        .as_mut()
        .context("canonical agent needs a child block")?;
    let compact_provider_count = children
        .nodes()
        .iter()
        .filter(|node| matches!(node.name().value(), "command" | "argv"))
        .count();
    anyhow::ensure!(
        compact_provider_count <= 1,
        "compact agent may declare only one of command or argv"
    );
    let retired_nodes = children
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "retired")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        retired_nodes.len() <= 1,
        "agent declares retired more than once"
    );
    let retired = retired_nodes
        .first()
        .and_then(|node| node.get(0))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    if matches!(kind, ProjectionKind::ProviderWitness) {
        children.nodes_mut().retain(|node| {
            !matches!(node.name().value(), "exec" | "ding")
                && !(retired
                    && matches!(
                        node.name().value(),
                        "command" | "argv" | "pty" | "lifecycle"
                    ))
        });
    }
    if compact_provider_count == 1 && !(retired && matches!(kind, ProjectionKind::ProviderWitness))
    {
        set_lifecycle(children.nodes_mut(), "adopt-only")?;
    }
    for task in children
        .nodes_mut()
        .iter_mut()
        .filter(|node| node.name().value() == "pty")
    {
        let task_children = task
            .children_mut()
            .as_mut()
            .context("pty task needs a child block")?;
        set_lifecycle(task_children.nodes_mut(), "adopt-only")?;
    }
    Ok(())
}

fn set_lifecycle(nodes: &mut Vec<KdlNode>, value: &str) -> Result<()> {
    let indexes = nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.name().value() == "lifecycle")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    anyhow::ensure!(indexes.len() <= 1, "task declares lifecycle more than once");
    if let Some(index) = indexes.first().copied() {
        let node = &mut nodes[index];
        anyhow::ensure!(
            node.children().is_none(),
            "lifecycle cannot have a child block"
        );
        node.entries_mut().clear();
        node.entries_mut().push(KdlEntry::new(value));
    } else {
        let mut lifecycle = KdlNode::new("lifecycle");
        lifecycle.entries_mut().push(KdlEntry::new(value));
        nodes.push(lifecycle);
    }
    Ok(())
}

/// Projection admission is deliberately narrower than ordinary apply admission. It permits the
/// exact pre-existing render-ownership conflict class as explicit evidence, never as a silent
/// green. Provider witnesses additionally contain intentional identity/resource-only agents after
/// runnable work was projected away.
fn validate_projectable_catalog(
    root: &Path,
    allow_identity_witnesses: bool,
) -> Result<CatalogProjectionAdmission> {
    let found = crate::discover(root);
    let mut hosts = BTreeSet::new();
    for spec in &found.specs {
        let host = spec
            .host
            .as_deref()
            .context("canonical declaration is missing explicit host")?;
        hosts.insert(host.to_string());
    }
    let mut issues = BTreeSet::new();
    let report = crate::validate::validate(root);
    issues.extend(
        report
            .issues
            .iter()
            .filter(|issue| issue.severity == crate::validate::Severity::Error)
            .map(issue_evidence),
    );
    for host in hosts {
        let report = crate::validate::validate_for_host(root, &host);
        issues.extend(
            report
                .issues
                .iter()
                .filter(|issue| issue.severity == crate::validate::Severity::Error)
                .map(issue_evidence),
        );
    }
    let rejected = issues
        .iter()
        .filter(|(code, _)| {
            *code != "render-owner-conflict"
                && !(allow_identity_witnesses && *code == "not-runnable")
        })
        .map(|(code, evidence)| format!("{} [{code}]: {}", evidence.path, evidence.message))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        rejected.is_empty(),
        "catalog fails projection admission:\n{}",
        rejected.join("\n")
    );
    let evidence = issues
        .into_iter()
        .filter(|(code, _)| *code == "render-owner-conflict")
        .map(|(_, evidence)| evidence)
        .collect::<Vec<_>>();
    let exceptions = if evidence.is_empty() {
        Vec::new()
    } else {
        vec![CatalogProjectionAdmissionException {
            code: "render-owner-conflict".to_owned(),
            count: evidence.len(),
            evidence,
        }]
    };
    Ok(CatalogProjectionAdmission {
        apply_admissible: exceptions.is_empty(),
        exceptions,
    })
}

fn issue_evidence(
    issue: &crate::validate::Issue,
) -> (&'static str, CatalogProjectionAdmissionEvidence) {
    (
        issue.code,
        CatalogProjectionAdmissionEvidence {
            path: issue.path.clone(),
            agent: issue.agent.clone(),
            message: issue.message.clone(),
        },
    )
}

fn projection_artifact(
    root: &Path,
    relative_path: &'static str,
    projection: &DeclarationProjection,
) -> Result<CatalogProjectionArtifact> {
    let discovered = crate::discover(root);
    anyhow::ensure!(
        discovered.errors.is_empty() && discovered.warnings.is_empty(),
        "projection discovery is not clean: {} errors, {} warnings",
        discovered.errors.len(),
        discovered.warnings.len()
    );
    let mut agent_ids = Vec::new();
    let mut provider_agent_ids = BTreeSet::new();
    let mut provider_task_ids = Vec::new();
    let mut active_provider_task_ids = Vec::new();
    let mut retired_absent_provider_task_ids = Vec::new();
    let mut exec_task_ids = Vec::new();
    for spec in &discovered.specs {
        let host = spec
            .host
            .as_deref()
            .context("projected Agent Spec lacks explicit host")?;
        let agent_id = spec.bus_id(host);
        agent_ids.push(agent_id.clone());
        for task in &spec.tasks {
            let task_id = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{agent_id}.{}", task.name));
            match task.kind {
                agent_spec::spec::TaskKind::Pty => {
                    provider_agent_ids.insert(agent_id.clone());
                    provider_task_ids.push(task_id.clone());
                    if !spec.retired {
                        active_provider_task_ids.push(task_id);
                    } else {
                        retired_absent_provider_task_ids.push(task_id);
                    }
                    anyhow::ensure!(
                        task.lifecycle == agent_spec::spec::TaskLifecycle::AdoptOnly
                            || relative_path == "service",
                        "{relative_path} provider task is not adopt-only"
                    );
                }
                agent_spec::spec::TaskKind::Exec => exec_task_ids.push(task_id),
            }
        }
    }
    agent_ids.sort();
    provider_task_ids.sort();
    active_provider_task_ids.sort();
    retired_absent_provider_task_ids.sort();
    exec_task_ids.sort();
    anyhow::ensure!(
        adjacent_unique(&agent_ids)
            && adjacent_unique(&provider_task_ids)
            && adjacent_unique(&exec_task_ids),
        "projection contains duplicate agent or task identity"
    );
    Ok(CatalogProjectionArtifact {
        path: relative_path.to_owned(),
        root_sha256: projection.root_sha256.clone(),
        entries: projection.entries(),
        agent_ids,
        provider_agent_ids: provider_agent_ids.into_iter().collect(),
        provider_task_ids,
        active_provider_task_ids,
        retired_absent_provider_task_ids,
        exec_task_ids,
    })
}

fn adjacent_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] != pair[1])
}

fn hash_projection_bundle(receipt: &CatalogProjectionReceipt, receipt_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PROJECTION_BUNDLE_HASH_DOMAIN);
    for (name, digest) in [
        ("service", receipt.service.root_sha256.as_str()),
        ("adopt-only", receipt.adopt_only.root_sha256.as_str()),
        (
            "provider-witness",
            receipt.provider_witness.root_sha256.as_str(),
        ),
    ] {
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update(digest.as_bytes());
    }
    hasher.update((receipt_bytes.len() as u64).to_be_bytes());
    hasher.update(receipt_bytes);
    format!("{:x}", hasher.finalize())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o644)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    output.write_all(bytes)?;
    output.sync_all()?;
    Ok(())
}

fn read_real_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a real regular file: {}",
        path.display()
    );
    fs::read(path).with_context(|| format!("read {label} {}", path.display()))
}
