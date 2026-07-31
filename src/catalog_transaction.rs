//! Canonical declaration snapshots and crash-recoverable whole-catalog application.
//!
//! This module owns the declaration projection shared by bulk readers and writers. Runtime state
//! is never copied, hashed, removed, or locked behind this transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::catalog_lock::{APPLY_MARKER, CONTROL_DIR, CatalogLock};
use crate::cutover_admission::{
    CanonicalCatalog, CatalogPublish, TransactionCatalog, admit_catalog_publish,
};

mod authority_seal {
    pub trait Sealed {}
}

/// Retained, non-forgeable authority for a declaration-plane mutation.
///
/// This trait is crate-private and sealed in this writer module. Ordinary callers can only obtain
/// it through `admit_catalog_publish`; the cutover transaction can later add its own opaque token
/// here without adding a boolean/force bypass or reacquiring the catalog lock.
pub(crate) trait RetainedCatalogMutationAuthority: authority_seal::Sealed {
    fn catalog(&self) -> &CanonicalCatalog;
}

impl authority_seal::Sealed for CatalogPublish<'_> {}

impl RetainedCatalogMutationAuthority for CatalogPublish<'_> {
    fn catalog(&self) -> &CanonicalCatalog {
        self.catalog()
    }
}

impl authority_seal::Sealed for TransactionCatalog<'_> {}

impl RetainedCatalogMutationAuthority for TransactionCatalog<'_> {
    fn catalog(&self) -> &CanonicalCatalog {
        self.catalog()
    }
}

const SNAPSHOT_SCHEMA: &str = "st2.catalog-snapshot.v1";
const APPLY_SCHEMA: &str = "st2.catalog-apply.v1";
const MARKER_SCHEMA: &str = "st2.catalog-apply-incomplete.v1";
const HASH_DOMAIN: &[u8] = b"st2.catalog-declaration-root.v1\0";
const STAGE_PREFIX: &str = "catalog-apply-stage-";
const TEMPLATE_MAX_DEPTH: usize = 8;
const TEMPLATE_MAX_FILES: usize = 256;
const TEMPLATE_MAX_FILE_BYTES: u64 = 1024 * 1024;
const TEMPLATE_MAX_TOTAL_BYTES: u64 = 32 * 1024 * 1024;
const PROJECTION_BUNDLE_MAX_DEPTH: usize = 16;
const PROJECTION_BUNDLE_MAX_FILES: usize = 4096;
const PROJECTION_BUNDLE_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const PROJECTION_BUNDLE_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug)]
pub struct SnapshotRequest {
    pub catalog: PathBuf,
    pub output: PathBuf,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResult {
    pub schema: &'static str,
    pub status: SnapshotStatus,
    pub catalog: PathBuf,
    pub output: PathBuf,
    pub root_sha256: String,
    pub entries: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotStatus {
    Created,
    Unchanged,
}

#[path = "catalog_projection.rs"]
mod projection;
pub use projection::{
    CatalogProjectionAdmission, CatalogProjectionAdmissionEvidence,
    CatalogProjectionAdmissionException, CatalogProjectionApplyRequest, CatalogProjectionArtifact,
    CatalogProjectionChild, CatalogProjectionEquality, CatalogProjectionReceipt,
    CatalogProjectionRequest, CatalogProjectionResult, apply_projection_bundle, project_catalog,
};

#[derive(Debug)]
pub struct ApplyRequest {
    pub catalog: PathBuf,
    pub mode: ApplyMode,
}

#[derive(Debug)]
pub enum ApplyMode {
    Prepared {
        prepared: PathBuf,
        expect_sha256: String,
    },
    Resume,
}

pub(crate) struct PreparedCatalogApply {
    catalog: CanonicalCatalog,
    prepared_input: Option<(PathBuf, String, DeclarationProjection)>,
}

impl PreparedCatalogApply {
    pub(crate) fn desired_root_sha256(&self) -> Option<&str> {
        self.prepared_input
            .as_ref()
            .map(|(_, _, projection)| projection.root_sha256.as_str())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyResult {
    pub schema: &'static str,
    pub status: ApplyStatus,
    pub catalog: PathBuf,
    pub prepared: Option<PathBuf>,
    pub before_sha256: String,
    pub after_sha256: String,
    pub entries: usize,
    pub recovered: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplyStatus {
    Applied,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectedFile {
    bytes: Vec<u8>,
    executable: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DeclarationProjection {
    files: BTreeMap<String, ProjectedFile>,
    workspace_dirs: BTreeSet<String>,
    root_sha256: String,
}

impl DeclarationProjection {
    pub(crate) fn entries(&self) -> usize {
        self.files.len()
    }
}

/// Recompute the canonical declaration-root digest while the caller retains a
/// [`CatalogLock`] for `catalog`.
///
/// Lifecycle transactions use this instead of duplicating the projection
/// algorithm. The lock is intentionally supplied by the caller so it can be
/// held continuously from authority validation through the runtime mutation.
pub fn declaration_root_sha256_locked(catalog: &Path) -> Result<String> {
    let catalog = canonical_real_dir(catalog, "catalog")?;
    let projection = project(&catalog, ProjectionSource::Current, &catalog)?;
    validate_live_workspace_facts(&catalog, &projection.workspace_dirs)?;
    Ok(projection.root_sha256)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionSource {
    Current,
    Prepared,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplyMarker {
    schema: String,
    stage_name: String,
    expected_root_sha256: String,
    prepared_root_sha256: String,
    original_paths: Vec<String>,
}

/// Capture one coherent declaration plane under the shared catalog-authoring lock.
pub fn snapshot(request: SnapshotRequest) -> Result<SnapshotResult> {
    let catalog = canonical_real_dir(&request.catalog, "catalog")?;
    let output = absolute_path(&request.output)?;
    anyhow::ensure!(
        !output.starts_with(&catalog),
        "snapshot output must be outside the catalog: {}",
        output.display()
    );
    let parent = output.parent().context("snapshot output has no parent")?;
    let parent = canonical_real_dir(parent, "snapshot output parent")?;
    let output = parent.join(
        output
            .file_name()
            .context("snapshot output has no final path component")?,
    );

    let _lock = CatalogLock::shared(&catalog)?;
    let projection = project(&catalog, ProjectionSource::Current, &catalog)?;
    validate_live_workspace_facts(&catalog, &projection.workspace_dirs)?;
    match fs::symlink_metadata(&output) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "snapshot output is not a real directory: {}",
                output.display()
            );
            let existing = project(&output, ProjectionSource::Prepared, &catalog)?;
            anyhow::ensure!(
                existing.root_sha256 == projection.root_sha256,
                "snapshot output already exists with root sha256 {}, expected {}",
                existing.root_sha256,
                projection.root_sha256
            );
            return Ok(SnapshotResult {
                schema: SNAPSHOT_SCHEMA,
                status: SnapshotStatus::Unchanged,
                catalog,
                output,
                entries: projection.entries(),
                root_sha256: projection.root_sha256,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect snapshot output {}", output.display()));
        }
    }

    let stage = tempfile::Builder::new()
        .prefix(".st2-catalog-snapshot-")
        .tempdir_in(&parent)
        .with_context(|| format!("create snapshot stage in {}", parent.display()))?;
    materialize_projection(&projection, stage.path())?;
    rename_noreplace(&stage.keep(), &output)
        .with_context(|| format!("publish snapshot {}", output.display()))?;
    sync_dir(&parent)?;

    Ok(SnapshotResult {
        schema: SNAPSHOT_SCHEMA,
        status: SnapshotStatus::Created,
        catalog,
        output,
        entries: projection.entries(),
        root_sha256: projection.root_sha256,
    })
}

/// Apply a complete prepared declaration plane under one exclusive transaction.
pub fn apply(request: ApplyRequest) -> Result<ApplyResult> {
    let prepared = prepare_apply(request)?;
    let catalog = prepared.catalog.as_path();
    let lock = CatalogLock::exclusive_for_catalog_apply(catalog)?;
    let authority_catalog = prepared.catalog.clone();
    let publication = admit_catalog_publish(&authority_catalog, &lock)?;
    apply_admitted(prepared, &publication)
}

/// Capture and validate caller-owned prepared bytes before acquiring catalog mutation authority.
///
/// This step never creates or changes a path in the live catalog.
pub(crate) fn prepare_apply(request: ApplyRequest) -> Result<PreparedCatalogApply> {
    let catalog = canonical_real_dir(&request.catalog, "catalog")?;
    let canonical_catalog = CanonicalCatalog::open(&catalog)?;
    let prepared_input = match request.mode {
        ApplyMode::Prepared {
            prepared,
            expect_sha256,
        } => {
            validate_sha256(&expect_sha256)?;
            let prepared = canonical_real_dir_no_alias(&prepared, "prepared catalog")?;
            anyhow::ensure!(
                !prepared.starts_with(&catalog),
                "prepared catalog must be outside the live catalog: {}",
                prepared.display()
            );
            let captured = tempfile::tempdir().context("create prepared-catalog capture root")?;
            capture_prepared_catalog(&prepared, captured.path())?;
            let desired = project(captured.path(), ProjectionSource::Prepared, &catalog)?;
            Some((prepared, expect_sha256, desired))
        }
        ApplyMode::Resume => None,
    };
    Ok(PreparedCatalogApply {
        catalog: canonical_catalog,
        prepared_input,
    })
}

/// Apply a previously captured declaration plane while retaining exact catalog authority.
pub(crate) fn apply_admitted(
    prepared: PreparedCatalogApply,
    authority: &impl RetainedCatalogMutationAuthority,
) -> Result<ApplyResult> {
    anyhow::ensure!(
        authority.catalog() == &prepared.catalog,
        "catalog mutation authority is bound to {}, not {}",
        authority.catalog().as_path().display(),
        prepared.catalog.as_path().display()
    );
    let catalog = prepared.catalog.as_path().to_path_buf();
    let prepared_input = prepared.prepared_input;
    let marker_path = catalog.join(CONTROL_DIR).join(APPLY_MARKER);
    let existing_marker = read_marker_optional(&marker_path)?;
    let recovered = existing_marker.is_some();
    let (prepared, expect_sha256, desired, marker) = match (prepared_input, existing_marker) {
        (Some(_), Some(_)) => {
            anyhow::bail!("catalog apply is incomplete; recover only with `catalog apply --resume`")
        }
        (Some((prepared, expect_sha256, desired)), None) => {
            (Some(prepared), expect_sha256, desired, None)
        }
        (None, Some(marker)) => {
            validate_marker(&marker)?;
            let stage_path = catalog.join(CONTROL_DIR).join(&marker.stage_name);
            let staged = project(&stage_path, ProjectionSource::Prepared, &catalog)
                .context("validate durable recovery stage")?;
            anyhow::ensure!(
                staged.root_sha256 == marker.prepared_root_sha256,
                "durable recovery stage hash mismatch: expected {}, found {}",
                marker.prepared_root_sha256,
                staged.root_sha256
            );
            (
                None,
                marker.expected_root_sha256.clone(),
                staged,
                Some(marker),
            )
        }
        (None, None) => anyhow::bail!("catalog apply --resume requires an incomplete apply marker"),
    };

    validate_live_workspace_facts(&catalog, &desired.workspace_dirs)?;
    // Admission reads exact durable/captured declaration bytes. Catalog-contained workspace facts
    // are mirrored as empty directories; their live content is never copied or hashed.
    let admission = tempfile::tempdir().context("create prepared-catalog admission root")?;
    materialize_projection(&desired, admission.path())?;
    validate_full_catalog(admission.path())?;
    let desired_config = crate::catalog::load(admission.path())?;
    validate_external_pty_root(&catalog, &desired_config)?;

    let stage_name = stage_name(&desired.root_sha256);
    let stage_path = catalog.join(CONTROL_DIR).join(&stage_name);
    let (before_sha256, original_paths, current) = if let Some(marker) = marker {
        anyhow::ensure!(
            marker.stage_name == stage_name,
            "incomplete catalog apply stage name does not match its prepared root"
        );
        (expect_sha256.clone(), marker.original_paths, None)
    } else {
        let current = project_excluding(
            &catalog,
            ProjectionSource::Current,
            &catalog,
            &desired.workspace_dirs,
        )?;
        let live_config = crate::catalog::load(&catalog)?;
        let same_pty_root = effective_pty_root(&catalog, &live_config)
            == effective_pty_root(&catalog, &desired_config);
        if current.root_sha256 == desired.root_sha256 && same_pty_root {
            return Ok(ApplyResult {
                schema: APPLY_SCHEMA,
                status: ApplyStatus::Unchanged,
                catalog,
                prepared,
                before_sha256: current.root_sha256.clone(),
                entries: desired.entries(),
                after_sha256: desired.root_sha256,
                recovered: false,
            });
        }
        anyhow::ensure!(
            same_pty_root,
            "catalog apply v1 refuses an effective pty-root change"
        );
        anyhow::ensure!(
            current.root_sha256 == expect_sha256,
            "catalog apply precondition failed: expected sha256 {}, found {}",
            expect_sha256,
            current.root_sha256
        );
        let original_paths = current.files.keys().cloned().collect::<Vec<_>>();
        ensure_durable_stage(&catalog, &stage_path, &desired)?;
        write_marker(
            &catalog,
            &ApplyMarker {
                schema: MARKER_SCHEMA.to_string(),
                stage_name: stage_name.clone(),
                expected_root_sha256: expect_sha256.clone(),
                prepared_root_sha256: desired.root_sha256.clone(),
                original_paths: original_paths.clone(),
            },
        )?;
        test_checkpoint("marker-created");
        (current.root_sha256.clone(), original_paths, Some(current))
    };

    let staged = project(&stage_path, ProjectionSource::Prepared, &catalog)?;
    apply_projection(&catalog, &original_paths, current.as_ref(), &staged)?;
    test_checkpoint("before-verify");
    let verified = project(&catalog, ProjectionSource::Current, &catalog)?;
    anyhow::ensure!(
        verified.root_sha256 == staged.root_sha256,
        "catalog apply verification failed: expected {}, found {}",
        staged.root_sha256,
        verified.root_sha256
    );
    validate_full_catalog(&catalog).context("validate applied live catalog")?;
    sync_dir(&catalog)?;
    test_checkpoint("before-clear");
    fs::remove_file(&marker_path)
        .with_context(|| format!("clear catalog apply marker {}", marker_path.display()))?;
    sync_dir(&catalog.join(CONTROL_DIR))?;
    let _ = fs::remove_dir_all(&stage_path);
    let _ = sync_dir(&catalog.join(CONTROL_DIR));

    Ok(ApplyResult {
        schema: APPLY_SCHEMA,
        status: ApplyStatus::Applied,
        catalog,
        prepared,
        before_sha256,
        entries: verified.entries(),
        after_sha256: verified.root_sha256,
        recovered,
    })
}

/// Full structural and host-scoped validation for a complete prospective catalog.
pub(crate) fn validate_full_catalog(root: &Path) -> Result<()> {
    let found = crate::discover(root);
    let mut hosts = BTreeSet::new();
    for spec in &found.specs {
        let host = spec
            .host
            .as_deref()
            .context("canonical declaration is missing explicit host")?;
        hosts.insert(host.to_string());
    }
    let mut errors = BTreeSet::new();
    let report = crate::validate::validate(root);
    errors.extend(
        report
            .issues
            .iter()
            .filter(|issue| issue.severity == crate::validate::Severity::Error)
            .map(format_issue),
    );
    for host in hosts {
        let report = crate::validate::validate_for_host(root, &host);
        errors.extend(
            report
                .issues
                .iter()
                .filter(|issue| issue.severity == crate::validate::Severity::Error)
                .map(format_issue),
        );
    }
    anyhow::ensure!(
        errors.is_empty(),
        "catalog fails full validation:\n{}",
        errors.into_iter().collect::<Vec<_>>().join("\n")
    );
    Ok(())
}

fn format_issue(issue: &crate::validate::Issue) -> String {
    format!("{} [{}]: {}", issue.path, issue.code, issue.message)
}

fn project(
    root: &Path,
    source: ProjectionSource,
    logical_catalog: &Path,
) -> Result<DeclarationProjection> {
    project_excluding(root, source, logical_catalog, &BTreeSet::new())
}

fn project_excluding(
    root: &Path,
    source: ProjectionSource,
    logical_catalog: &Path,
    additional_workspace_dirs: &BTreeSet<String>,
) -> Result<DeclarationProjection> {
    let metadata = fs::symlink_metadata(root)?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "projection root is not a real directory: {}",
        root.display()
    );
    let mut files = BTreeMap::new();
    add_optional_regular(root, &root.join(crate::catalog::CONFIG_FILE), &mut files)?;
    let spec_paths = collect_canonical_specs(root, source, &mut files)?;
    let discovered = crate::discover(root);
    let mut specs = Vec::new();
    for path in &spec_paths {
        let declared = crate::discovery::parse_declared(path)
            .with_context(|| format!("parse canonical declaration {}", path.display()))?;
        anyhow::ensure!(
            declared.len() == 1,
            "canonical declaration must contain exactly one agent: {}",
            path.display()
        );
        let relative = path.strip_prefix(root)?;
        let components = normal_components(relative)?;
        let expected_host = components[1].as_str();
        let expected_identity = components[2].as_str();
        anyhow::ensure!(
            declared[0].host.as_deref() == Some(expected_host)
                && declared[0].identity.as_deref() == Some(expected_identity),
            "canonical declaration needs explicit host '{}' and identity '{}': {}",
            expected_host,
            expected_identity,
            path.display()
        );
        let matching = discovered
            .specs
            .iter()
            .filter(|spec| spec.path == *path)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            matching.len() == 1,
            "canonical declaration did not lower to exactly one agent: {}",
            path.display()
        );
        specs.push(matching[0].clone());
    }

    let workspace_dirs = catalog_workspace_dirs(root, logical_catalog, &specs)?;
    let mut scan_exclusions = workspace_dirs.clone();
    scan_exclusions.extend(additional_workspace_dirs.iter().cloned());
    for spec in &specs {
        let bundle = spec.path.parent().context("canonical spec has no bundle")?;
        collect_bundle_files(root, bundle, bundle, source, &scan_exclusions, &mut files)?;
    }
    collect_templates(root, &mut files)?;

    for spec in &specs {
        let host = spec.host.as_deref().context("explicit host disappeared")?;
        for input in crate::materialize::catalog_owned_render_inputs(root, spec, host)? {
            let relative = normalized_relative(root, &input)?;
            let in_bundle = spec
                .path
                .parent()
                .is_some_and(|parent| input.starts_with(parent));
            anyhow::ensure!(
                in_bundle || relative.starts_with("_templates/"),
                "catalog-owned render input must be inside its agent bundle or _templates: {}",
                input.display()
            );
            add_regular(root, &input, &mut files)?;
        }
    }

    if source == ProjectionSource::Prepared {
        validate_prepared_workspace_facts(root, &workspace_dirs)?;
        reject_unprojected_entries(root, &files, &workspace_dirs)?;
    }
    let root_sha256 = hash_projection(&files, &workspace_dirs);
    Ok(DeclarationProjection {
        files,
        workspace_dirs,
        root_sha256,
    })
}

fn collect_canonical_specs(
    root: &Path,
    source: ProjectionSource,
    files: &mut BTreeMap<String, ProjectedFile>,
) -> Result<Vec<PathBuf>> {
    let agents = root.join("agents");
    let Some(host_entries) = read_real_dir_optional(&agents)? else {
        return Ok(Vec::new());
    };
    let mut specs = Vec::new();
    for host_entry in host_entries {
        let host_path = host_entry.path();
        ensure_safe_component(&host_entry.file_name(), "host")?;
        let host_metadata = fs::symlink_metadata(&host_path)?;
        if host_metadata.is_file() && !host_metadata.file_type().is_symlink() {
            continue;
        }
        anyhow::ensure!(
            host_metadata.is_dir() && !host_metadata.file_type().is_symlink(),
            "canonical host address is not a real directory: {}",
            host_path.display()
        );
        for identity_entry in sorted_entries(&host_path)? {
            let identity_path = identity_entry.path();
            ensure_safe_component(&identity_entry.file_name(), "identity")?;
            let identity_metadata = fs::symlink_metadata(&identity_path)?;
            if identity_metadata.is_file() && !identity_metadata.file_type().is_symlink() {
                continue;
            }
            anyhow::ensure!(
                identity_metadata.is_dir() && !identity_metadata.file_type().is_symlink(),
                "canonical identity address is not a real directory: {}",
                identity_path.display()
            );
            let spec = identity_path.join("agent.kdl");
            match fs::symlink_metadata(&spec) {
                Ok(metadata) => {
                    anyhow::ensure!(
                        metadata.is_file() && !metadata.file_type().is_symlink(),
                        "canonical spec is not a real regular file: {}",
                        spec.display()
                    );
                    add_regular(root, &spec, files)?;
                    specs.push(spec);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if source == ProjectionSource::Prepared {
                        reject_state_children(&identity_path)?;
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspect canonical spec {}", spec.display()));
                }
            }
        }
    }
    specs.sort();
    Ok(specs)
}

fn collect_bundle_files(
    root: &Path,
    bundle_root: &Path,
    dir: &Path,
    source: ProjectionSource,
    workspace_dirs: &BTreeSet<String>,
    files: &mut BTreeMap<String, ProjectedFile>,
) -> Result<()> {
    for entry in sorted_entries(dir)? {
        let path = entry.path();
        let relative = normalized_relative(root, &path)?;
        let relative_to_bundle = path.strip_prefix(bundle_root)?;
        let first = relative_to_bundle.components().next();
        let name = entry.file_name();
        let name_text = name.to_str().context("bundle path is not UTF-8")?;
        let canonical_workspace = first.is_some()
            && relative_to_bundle.components().count() == 1
            && name_text == ".workspace";
        if canonical_workspace || workspace_dirs.contains(&relative) {
            continue;
        }
        let state = matches!(name_text, "resources" | "archive" | "inbox" | "status")
            || name_text.starts_with(".status.tmp-");
        if first.is_some() && relative_to_bundle.components().count() == 1 && state {
            if source == ProjectionSource::Prepared {
                anyhow::bail!(
                    "prepared catalog contains state-plane path: {}",
                    path.display()
                );
            }
            continue;
        }
        anyhow::ensure!(
            !matches!(name_text, ".git" | ".st2"),
            "bundle contains reserved control path: {}",
            path.display()
        );
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            collect_bundle_files(root, bundle_root, &path, source, workspace_dirs, files)?;
        } else if metadata.is_file() && !metadata.file_type().is_symlink() {
            add_regular(root, &path, files)?;
        } else {
            anyhow::bail!(
                "catalog declaration plane contains symlink or special entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn catalog_workspace_dirs(
    root: &Path,
    logical_catalog: &Path,
    specs: &[agent_spec::spec::AgentSpec],
) -> Result<BTreeSet<String>> {
    let mut facts = BTreeSet::new();
    for spec in specs {
        let bundle = spec
            .path
            .parent()
            .context("canonical spec has no bundle directory")?;
        let bundle_relative = normalized_relative(root, bundle)?;
        let logical_bundle = logical_catalog.join(&bundle_relative);
        let expected = format!("{bundle_relative}/.workspace");
        let paths = spec
            .workspace
            .iter()
            .map(String::as_str)
            .chain(spec.tasks.iter().filter_map(|task| task.cwd.as_deref()));
        for raw in paths {
            let resolved = crate::expand::resolve_spec_path(raw, logical_catalog, &logical_bundle)?;
            if resolved.starts_with(logical_catalog) {
                let relative = normalized_relative(logical_catalog, &resolved)?;
                anyhow::ensure!(
                    relative == expected,
                    "catalog-contained workspace/cwd must use canonical {}: {}",
                    expected,
                    resolved.display()
                );
                facts.insert(relative);
            }
        }
    }
    Ok(facts)
}

fn validate_prepared_workspace_facts(root: &Path, facts: &BTreeSet<String>) -> Result<()> {
    for relative in facts {
        let path = root.join(relative);
        ensure_real_dir_chain_present(root, &path, "prepared workspace fact")?;
        anyhow::ensure!(
            sorted_entries(&path)?.is_empty(),
            "prepared workspace fact must be an empty directory: {}",
            path.display()
        );
    }
    Ok(())
}

fn collect_templates(root: &Path, files: &mut BTreeMap<String, ProjectedFile>) -> Result<()> {
    let templates = root.join("_templates");
    let Some(_) = read_real_dir_optional(&templates)? else {
        return Ok(());
    };
    let mut count = 0usize;
    let mut total = 0u64;
    collect_template_dir(
        root, &templates, &templates, 0, &mut count, &mut total, files,
    )
}

fn collect_template_dir(
    root: &Path,
    templates: &Path,
    dir: &Path,
    depth: usize,
    count: &mut usize,
    total: &mut u64,
    files: &mut BTreeMap<String, ProjectedFile>,
) -> Result<()> {
    anyhow::ensure!(
        depth <= TEMPLATE_MAX_DEPTH,
        "_templates exceeds maximum depth {TEMPLATE_MAX_DEPTH}"
    );
    for entry in sorted_entries(dir)? {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_str().context("_templates path is not UTF-8")?;
        anyhow::ensure!(
            !matches!(
                name,
                ".git" | ".st2" | "pty" | "resources" | "archive" | "inbox" | "status"
            ),
            "_templates contains reserved control/state path: {}",
            path.display()
        );
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            collect_template_dir(root, templates, &path, depth + 1, count, total, files)?;
        } else if metadata.is_file() && !metadata.file_type().is_symlink() {
            let relative_depth = path.strip_prefix(templates)?.components().count();
            anyhow::ensure!(
                relative_depth <= TEMPLATE_MAX_DEPTH,
                "_templates file exceeds maximum depth {TEMPLATE_MAX_DEPTH}: {}",
                path.display()
            );
            anyhow::ensure!(
                metadata.nlink() == 1,
                "_templates file must not be hard-linked: {}",
                path.display()
            );
            anyhow::ensure!(
                metadata.len() <= TEMPLATE_MAX_FILE_BYTES,
                "_templates file exceeds {TEMPLATE_MAX_FILE_BYTES} bytes: {}",
                path.display()
            );
            *count += 1;
            *total = total.saturating_add(metadata.len());
            anyhow::ensure!(
                *count <= TEMPLATE_MAX_FILES,
                "_templates exceeds {TEMPLATE_MAX_FILES} files"
            );
            anyhow::ensure!(
                *total <= TEMPLATE_MAX_TOTAL_BYTES,
                "_templates exceeds {TEMPLATE_MAX_TOTAL_BYTES} total bytes"
            );
            add_regular(root, &path, files)?;
        } else {
            anyhow::bail!(
                "_templates contains a symlink or special entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn reject_state_children(identity_path: &Path) -> Result<()> {
    for entry in sorted_entries(identity_path)? {
        let name = entry.file_name();
        let name = name.to_str().context("identity path is not UTF-8")?;
        if matches!(name, "resources" | "archive" | "inbox" | "status")
            || name.starts_with(".status.tmp-")
        {
            anyhow::bail!(
                "prepared catalog contains state-plane path: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn add_optional_regular(
    root: &Path,
    path: &Path,
    files: &mut BTreeMap<String, ProjectedFile>,
) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => add_regular(root, path, files),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn add_regular(
    root: &Path,
    path: &Path,
    files: &mut BTreeMap<String, ProjectedFile>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "declaration input is not a real regular file: {}",
        path.display()
    );
    let relative = normalized_relative(root, path)?;
    let bytes =
        fs::read(path).with_context(|| format!("read declaration input {}", path.display()))?;
    let executable = metadata.permissions().mode() & 0o111 != 0;
    files.insert(relative, ProjectedFile { bytes, executable });
    Ok(())
}

fn reject_unprojected_entries(
    root: &Path,
    files: &BTreeMap<String, ProjectedFile>,
    workspace_dirs: &BTreeSet<String>,
) -> Result<()> {
    let mut allowed_dirs = BTreeSet::new();
    for path in files.keys() {
        let mut parent = Path::new(path).parent();
        while let Some(value) = parent {
            if value.as_os_str().is_empty() {
                break;
            }
            allowed_dirs.insert(value.to_string_lossy().to_string());
            parent = value.parent();
        }
    }
    for path in workspace_dirs {
        let mut value = Some(Path::new(path));
        while let Some(dir) = value {
            if dir.as_os_str().is_empty() {
                break;
            }
            allowed_dirs.insert(dir.to_string_lossy().to_string());
            value = dir.parent();
        }
    }
    reject_unprojected_recursive(root, root, files, &allowed_dirs)
}

fn reject_unprojected_recursive(
    root: &Path,
    dir: &Path,
    files: &BTreeMap<String, ProjectedFile>,
    allowed_dirs: &BTreeSet<String>,
) -> Result<()> {
    for entry in sorted_entries(dir)? {
        let path = entry.path();
        let relative = normalized_relative(root, &path)?;
        let metadata = fs::symlink_metadata(&path)?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "prepared catalog contains a symlink: {}",
            path.display()
        );
        if metadata.is_dir() {
            anyhow::ensure!(
                allowed_dirs.contains(&relative),
                "prepared catalog contains unprojected directory: {}",
                path.display()
            );
            reject_unprojected_recursive(root, &path, files, allowed_dirs)?;
        } else if metadata.is_file() {
            anyhow::ensure!(
                files.contains_key(&relative),
                "prepared catalog contains unprojected file: {}",
                path.display()
            );
        } else {
            anyhow::bail!(
                "prepared catalog contains a special entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn hash_projection(
    files: &BTreeMap<String, ProjectedFile>,
    workspace_dirs: &BTreeSet<String>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(HASH_DOMAIN);
    for (path, file) in files {
        hasher.update([1]);
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update([u8::from(file.executable)]);
        hasher.update((file.bytes.len() as u64).to_be_bytes());
        hasher.update(&file.bytes);
    }
    for path in workspace_dirs {
        hasher.update([2]);
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn materialize_projection(projection: &DeclarationProjection, root: &Path) -> Result<()> {
    for (relative, file) in &projection.files {
        let target = root.join(relative);
        let parent = target.parent().context("projected file has no parent")?;
        ensure_real_dir_chain(root, parent)?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(if file.executable { 0o755 } else { 0o644 })
            .open(&target)
            .with_context(|| format!("create projected file {}", target.display()))?;
        output.write_all(&file.bytes)?;
        output.sync_all()?;
    }
    for relative in &projection.workspace_dirs {
        ensure_real_dir_chain(root, &root.join(relative))?;
    }
    sync_tree_dirs(root)
}

fn ensure_durable_stage(
    catalog: &Path,
    stage_path: &Path,
    desired: &DeclarationProjection,
) -> Result<()> {
    match fs::symlink_metadata(stage_path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "catalog apply stage is not a real directory: {}",
                stage_path.display()
            );
            let existing = project(stage_path, ProjectionSource::Prepared, catalog)?;
            anyhow::ensure!(
                existing.root_sha256 == desired.root_sha256
                    && existing.workspace_dirs == desired.workspace_dirs,
                "existing catalog apply stage has unexpected root sha256"
            );
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let control = catalog.join(CONTROL_DIR);
    let stage = tempfile::Builder::new()
        .prefix(".catalog-apply-stage-")
        .tempdir_in(&control)?;
    materialize_projection(desired, stage.path())?;
    rename_noreplace(&stage.keep(), stage_path)?;
    sync_dir(&control)
}

fn apply_projection(
    catalog: &Path,
    original_paths: &[String],
    current: Option<&DeclarationProjection>,
    desired: &DeclarationProjection,
) -> Result<()> {
    let atomically_created = create_new_identity_bundles(catalog, desired)?;
    let mut stale = original_paths
        .iter()
        .filter(|path| !desired.files.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    stale.sort_by_key(|path| (is_canonical_agent_spec(path), path.clone()));
    for path in stale {
        let target = catalog.join(&path);
        let metadata = match fs::symlink_metadata(&target) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "stale declaration is not a real regular file: {}",
            target.display()
        );
        fs::remove_file(&target)
            .with_context(|| format!("remove stale declaration {}", target.display()))?;
        sync_dir(target.parent().context("stale declaration has no parent")?)?;
        if is_canonical_agent_spec(&path) {
            test_checkpoint("deleted-spec");
        }
        test_checkpoint("mid-delete");
    }
    for (path, desired_file) in &desired.files {
        if current.is_some_and(|current| current.files.get(path) == Some(desired_file))
            || atomically_created
                .iter()
                .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
        {
            continue;
        }
        atomic_replace_file(catalog, path, desired_file)?;
        test_checkpoint("mid-write");
    }
    Ok(())
}

fn is_canonical_agent_spec(path: &str) -> bool {
    let components = path.split('/').collect::<Vec<_>>();
    components.len() == 4 && components[0] == "agents" && components[3] == "agent.kdl"
}

fn create_new_identity_bundles(
    catalog: &Path,
    desired: &DeclarationProjection,
) -> Result<BTreeSet<String>> {
    let mut created = BTreeSet::new();
    for path in desired.files.keys() {
        let components = path.split('/').collect::<Vec<_>>();
        if components.len() != 4 || components[0] != "agents" || components[3] != "agent.kdl" {
            continue;
        }
        let prefix = components[..3].join("/");
        let target = catalog.join(&prefix);
        match fs::symlink_metadata(&target) {
            Ok(metadata) => {
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "canonical identity path is not a real directory: {}",
                    target.display()
                );
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let host = target
            .parent()
            .context("identity target has no host parent")?;
        ensure_real_dir_chain(catalog, host)?;
        let stage = tempfile::Builder::new()
            .prefix(".catalog-apply-identity-")
            .tempdir_in(host)?;
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o755))?;
        for (candidate, file) in &desired.files {
            let Some(relative) = candidate.strip_prefix(&format!("{prefix}/")) else {
                continue;
            };
            let destination = stage.path().join(relative);
            let parent = destination.parent().context("bundle file has no parent")?;
            ensure_real_dir_chain(stage.path(), parent)?;
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(if file.executable { 0o755 } else { 0o644 })
                .open(&destination)?;
            output.write_all(&file.bytes)?;
            output.sync_all()?;
        }
        sync_tree_dirs(stage.path())?;
        rename_noreplace(&stage.keep(), &target)
            .with_context(|| format!("publish canonical identity bundle {}", target.display()))?;
        sync_dir(host)?;
        created.insert(prefix);
        test_checkpoint("mid-write");
    }
    Ok(created)
}

fn atomic_replace_file(catalog: &Path, relative: &str, file: &ProjectedFile) -> Result<()> {
    let target = catalog.join(relative);
    let parent = target
        .parent()
        .context("declaration target has no parent")?;
    ensure_real_dir_chain(catalog, parent)?;
    match fs::symlink_metadata(&target) {
        Ok(metadata) => anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "declaration target is not a real regular file: {}",
            target.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut temp = tempfile::Builder::new()
        .prefix(".catalog-apply-file-")
        .tempfile_in(parent)?;
    temp.as_file_mut()
        .set_permissions(fs::Permissions::from_mode(if file.executable {
            0o755
        } else {
            0o644
        }))?;
    temp.write_all(&file.bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(&target)
        .map_err(|error| error.error)
        .with_context(|| format!("replace declaration {}", target.display()))?;
    sync_dir(parent)
}

fn write_marker(catalog: &Path, marker: &ApplyMarker) -> Result<()> {
    let control = catalog.join(CONTROL_DIR);
    let target = control.join(APPLY_MARKER);
    let mut bytes = serde_json::to_vec(marker)?;
    bytes.push(b'\n');
    let mut temp = tempfile::Builder::new()
        .prefix(".catalog-apply-incomplete-")
        .tempfile_in(&control)?;
    temp.write_all(&bytes)?;
    temp.as_file().sync_all()?;
    fs::hard_link(temp.path(), &target)
        .with_context(|| format!("publish catalog apply marker {}", target.display()))?;
    temp.close()?;
    sync_dir(&control)
}

fn read_marker_optional(path: &Path) -> Result<Option<ApplyMarker>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "catalog apply marker is not a real regular file: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let bytes = fs::read(path)?;
    let marker = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse catalog apply marker {}", path.display()))?;
    Ok(Some(marker))
}

fn validate_marker(marker: &ApplyMarker) -> Result<()> {
    anyhow::ensure!(
        marker.schema == MARKER_SCHEMA,
        "unsupported catalog apply marker schema '{}'",
        marker.schema
    );
    validate_sha256(&marker.prepared_root_sha256)?;
    validate_sha256(&marker.expected_root_sha256)?;
    anyhow::ensure!(
        marker
            .original_paths
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "catalog apply marker originalPaths must be strictly sorted and unique"
    );
    for path in &marker.original_paths {
        validate_declaration_leaf_path(path)?;
    }
    let components = normal_components(Path::new(&marker.stage_name))?;
    anyhow::ensure!(
        components.len() == 1 && marker.stage_name == stage_name(&marker.prepared_root_sha256),
        "catalog apply marker has an unsafe or non-canonical stage name"
    );
    Ok(())
}

fn validate_declaration_leaf_path(path: &str) -> Result<()> {
    let components = path.split('/').collect::<Vec<_>>();
    anyhow::ensure!(
        !components.is_empty()
            && components
                .iter()
                .all(|component| !component.is_empty() && !matches!(*component, "." | "..")),
        "catalog apply marker contains an unsafe declaration path"
    );
    anyhow::ensure!(
        !components
            .iter()
            .any(|component| matches!(*component, ".git" | ".st2"))
            && components.first().copied() != Some("pty"),
        "catalog apply marker contains a reserved declaration path"
    );
    let canonical_bundle = components.len() >= 4 && components[0] == "agents";
    let template = components.len() >= 2 && components[0] == "_templates";
    anyhow::ensure!(
        path == crate::catalog::CONFIG_FILE || canonical_bundle || template,
        "catalog apply marker contains an unowned declaration path"
    );
    if canonical_bundle {
        anyhow::ensure!(
            !matches!(
                components[3],
                ".workspace" | "resources" | "archive" | "inbox" | "status"
            ) && !components[3].starts_with(".status.tmp-"),
            "catalog apply marker contains a workspace or state-plane path"
        );
    }
    Ok(())
}

fn effective_pty_root(live_catalog: &Path, config: &crate::catalog::CatalogConfig) -> PathBuf {
    match &config.pty_root {
        Some(declared) => live_catalog.join(crate::expand::expand_catalog(declared, live_catalog)),
        None => live_catalog.join("pty"),
    }
}

fn validate_live_workspace_facts(catalog: &Path, facts: &BTreeSet<String>) -> Result<()> {
    for relative in facts {
        let path = catalog.join(relative);
        ensure_real_dir_chain_present(catalog, &path, "live workspace fact")?;
    }
    Ok(())
}

fn validate_external_pty_root(
    catalog: &Path,
    config: &crate::catalog::CatalogConfig,
) -> Result<()> {
    anyhow::ensure!(
        config.pty_root.is_some(),
        "catalog apply v1 requires an explicit external pty-root"
    );
    let pty_root = lexical_absolute(&effective_pty_root(catalog, config))?;
    anyhow::ensure!(
        !pty_root.starts_with(catalog),
        "catalog apply v1 requires pty-root outside the catalog: {}",
        pty_root.display()
    );
    Ok(())
}

fn lexical_absolute(path: &Path) -> Result<PathBuf> {
    anyhow::ensure!(
        path.is_absolute(),
        "path is not absolute: {}",
        path.display()
    );
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(name) => normalized.push(name),
            Component::Prefix(_) => anyhow::bail!("unsupported path prefix: {}", path.display()),
        }
    }
    Ok(normalized)
}

fn stage_name(root_sha256: &str) -> String {
    format!("{STAGE_PREFIX}{root_sha256}")
}

fn validate_sha256(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "expected sha256 must be exactly 64 hexadecimal characters"
    );
    anyhow::ensure!(
        value.bytes().all(|byte| !byte.is_ascii_uppercase()),
        "expected sha256 must use lowercase hexadecimal"
    );
    Ok(())
}

fn canonical_real_dir(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalize {label} {}", path.display()))?;
    ensure_real_dir(&canonical, label)?;
    Ok(canonical)
}

fn canonical_real_dir_no_alias(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a real directory: {}",
        path.display()
    );
    canonical_real_dir(path, label)
}

fn ensure_real_dir(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a real directory: {}",
        path.display()
    );
    Ok(())
}

fn read_real_dir_optional(path: &Path) -> Result<Option<Vec<fs::DirEntry>>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "catalog path is not a real directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    Ok(Some(sorted_entries(path)?))
}

fn sorted_entries(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("read directory {}", path.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn ensure_safe_component(value: &std::ffi::OsStr, label: &str) -> Result<()> {
    let value = value
        .to_str()
        .with_context(|| format!("{label} is not UTF-8"))?;
    anyhow::ensure!(
        !value.is_empty() && !matches!(value, "." | ".." | ".git" | ".st2"),
        "{label} is not one safe path component"
    );
    Ok(())
}

fn normalized_relative(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("path escapes projection root: {}", path.display()))?;
    let components = normal_components(relative)?;
    anyhow::ensure!(!components.is_empty(), "projection cannot contain its root");
    Ok(components.join("/"))
}

fn normal_components(path: &Path) -> Result<Vec<String>> {
    path.components()
        .map(|component| match component {
            Component::Normal(value) => value
                .to_str()
                .map(String::from)
                .context("catalog path is not UTF-8"),
            _ => anyhow::bail!("catalog path contains an unsafe component"),
        })
        .collect()
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn ensure_real_dir_chain(root: &Path, target: &Path) -> Result<()> {
    let relative = target
        .strip_prefix(root)
        .context("directory escapes root")?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("directory contains an unsafe path component");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "path is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                fs::set_permissions(&current, fs::Permissions::from_mode(0o755))?;
                sync_dir(
                    current
                        .parent()
                        .context("created directory has no parent")?,
                )?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn ensure_real_dir_chain_present(root: &Path, target: &Path, label: &str) -> Result<()> {
    let relative = target
        .strip_prefix(root)
        .with_context(|| format!("{label} escapes catalog root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("{label} contains an unsafe path component");
        };
        current.push(name);
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("inspect {label} {}", current.display()))?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "{label} is not a real directory: {}",
            current.display()
        );
    }
    Ok(())
}

fn sync_tree_dirs(root: &Path) -> Result<()> {
    let mut dirs = Vec::new();
    collect_dirs(root, &mut dirs)?;
    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for dir in dirs {
        sync_dir(&dir)?;
    }
    Ok(())
}

fn collect_dirs(root: &Path, dirs: &mut Vec<PathBuf>) -> Result<()> {
    dirs.push(root.to_path_buf());
    for entry in sorted_entries(root)? {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            collect_dirs(&path, dirs)?;
        }
    }
    Ok(())
}

/// Copy an exact directory capability without following a source symlink at any depth.
pub(crate) fn capture_real_tree(source: &Path, destination: &Path) -> Result<()> {
    capture_tree(source, destination, CaptureMode::General)
}

/// Capture the exact closed projection-bundle shape through retained descriptors with resource
/// bounds applied before bytes are copied.
pub(crate) fn capture_projection_bundle(source: &Path, destination: &Path) -> Result<()> {
    let source = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(source)
        .context("open retained projection bundle")?;
    let names = capability_dir_entries_bounded(&source, 6)?;
    let expected = [
        "adopt-only",
        "bundle.sha256",
        "provider-witness",
        "receipt.json",
        "service",
    ];
    anyhow::ensure!(
        names.iter().map(|name| name.to_string_lossy()).eq(expected),
        "projection bundle must contain exactly service, adopt-only, provider-witness, receipt.json, and bundle.sha256"
    );
    let source_root = retained_dir_path(&source)?.canonicalize()?;
    let destination_root = destination.canonicalize()?;
    anyhow::ensure!(
        !destination_root.starts_with(&source_root),
        "projection bundle capture destination is contained by its source"
    );
    let mut budget = ProjectionBundleCaptureBudget::default();
    for name in names {
        budget.visit_entry()?;
        let input = openat_nofollow(&source, &name)?;
        let metadata = input.metadata()?;
        let target = destination.join(&name);
        let text = name.to_string_lossy();
        if matches!(text.as_ref(), "service" | "adopt-only" | "provider-witness") {
            anyhow::ensure!(
                metadata.is_dir(),
                "projection child must be a real directory"
            );
            fs::create_dir(&target)?;
            capture_projection_bundle_dir(&input, &target, 0, &mut budget)?;
        } else {
            anyhow::ensure!(
                metadata.is_file(),
                "projection receipt/digest must be a real file"
            );
            capture_projection_bundle_file(input, &target, &metadata, &mut budget)?;
        }
    }
    sync_tree_dirs(destination)
}

#[derive(Default)]
struct ProjectionBundleCaptureBudget {
    entries: usize,
    files: usize,
    bytes: u64,
}

impl ProjectionBundleCaptureBudget {
    fn visit_entry(&mut self) -> Result<()> {
        self.entries += 1;
        anyhow::ensure!(
            self.entries <= PROJECTION_BUNDLE_MAX_FILES,
            "projection bundle exceeds {PROJECTION_BUNDLE_MAX_FILES} filesystem entries"
        );
        Ok(())
    }
}

fn capture_projection_bundle_dir(
    source: &File,
    destination: &Path,
    depth: usize,
    budget: &mut ProjectionBundleCaptureBudget,
) -> Result<()> {
    anyhow::ensure!(
        depth <= PROJECTION_BUNDLE_MAX_DEPTH,
        "projection bundle exceeds maximum depth {PROJECTION_BUNDLE_MAX_DEPTH}"
    );
    let remaining = PROJECTION_BUNDLE_MAX_FILES.saturating_sub(budget.entries);
    for name in capability_dir_entries_bounded(source, remaining)? {
        budget.visit_entry()?;
        let input = openat_nofollow(source, &name)?;
        let metadata = input.metadata()?;
        let target = destination.join(&name);
        if metadata.is_dir() {
            fs::create_dir(&target)?;
            capture_projection_bundle_dir(&input, &target, depth + 1, budget)?;
        } else if metadata.is_file() {
            capture_projection_bundle_file(input, &target, &metadata, budget)?;
        } else {
            anyhow::bail!(
                "projection bundle contains a symlink or special entry: {}",
                target.display()
            );
        }
    }
    Ok(())
}

fn capture_projection_bundle_file(
    mut input: File,
    target: &Path,
    metadata: &fs::Metadata,
    budget: &mut ProjectionBundleCaptureBudget,
) -> Result<()> {
    anyhow::ensure!(
        metadata.nlink() == 1,
        "projection bundle contains a hard-linked file: {}",
        target.display()
    );
    anyhow::ensure!(
        metadata.len() <= PROJECTION_BUNDLE_MAX_FILE_BYTES,
        "projection bundle file exceeds {PROJECTION_BUNDLE_MAX_FILE_BYTES} bytes"
    );
    budget.files += 1;
    budget.bytes = budget.bytes.saturating_add(metadata.len());
    anyhow::ensure!(
        budget.files <= PROJECTION_BUNDLE_MAX_FILES,
        "projection bundle exceeds {PROJECTION_BUNDLE_MAX_FILES} files"
    );
    anyhow::ensure!(
        budget.bytes <= PROJECTION_BUNDLE_MAX_TOTAL_BYTES,
        "projection bundle exceeds {PROJECTION_BUNDLE_MAX_TOTAL_BYTES} bytes"
    );
    let executable = metadata.permissions().mode() & 0o111 != 0;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(if executable { 0o755 } else { 0o644 })
        .open(target)?;
    let copied = std::io::copy(
        &mut std::io::Read::by_ref(&mut input).take(PROJECTION_BUNDLE_MAX_FILE_BYTES + 1),
        &mut output,
    )?;
    anyhow::ensure!(
        copied == metadata.len(),
        "projection bundle file changed while captured: {}",
        target.display()
    );
    output.sync_all()?;
    Ok(())
}

fn capture_prepared_catalog(source: &Path, destination: &Path) -> Result<()> {
    capture_tree(source, destination, CaptureMode::PreparedCatalog)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CaptureMode {
    General,
    PreparedCatalog,
}

fn capture_tree(source: &Path, destination: &Path, mode: CaptureMode) -> Result<()> {
    let source = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(source)
        .context("open retained source directory")?;
    let source_root = retained_dir_path(&source)?
        .canonicalize()
        .context("resolve retained source directory")?;
    let destination_root = destination
        .canonicalize()
        .context("resolve capture destination directory")?;
    anyhow::ensure!(
        !destination_root.starts_with(&source_root),
        "capture destination {} is contained by source {}",
        destination_root.display(),
        source_root.display()
    );
    capture_dir_capability(&source, destination, destination, mode)?;
    sync_tree_dirs(destination)
}

fn retained_dir_path(dir: &File) -> Result<PathBuf> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        Ok(PathBuf::from(format!("/proc/self/fd/{}", dir.as_raw_fd())))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(PathBuf::from(format!("/dev/fd/{}", dir.as_raw_fd())))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    {
        let _ = dir;
        anyhow::bail!("retained-directory capture is unsupported on this platform")
    }
}

fn capture_dir_capability(
    source: &File,
    destination: &Path,
    capture_root: &Path,
    mode: CaptureMode,
) -> Result<()> {
    for name in capability_dir_entries(source)? {
        let mut input = openat_nofollow(source, &name)?;
        let metadata = input.metadata()?;
        let target = destination.join(&name);
        if metadata.is_dir() {
            let relative = target.strip_prefix(capture_root)?;
            if mode == CaptureMode::PreparedCatalog && is_canonical_workspace_fact(relative) {
                anyhow::ensure!(
                    capability_dir_entries(&input)?.is_empty(),
                    "prepared workspace fact must be empty: {}",
                    relative.display()
                );
                fs::create_dir(&target)?;
                fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;
                continue;
            }
            fs::create_dir(&target)?;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;
            capture_dir_capability(&input, &target, capture_root, mode)?;
        } else if metadata.is_file() {
            let relative = target.strip_prefix(capture_root)?;
            if relative.components().next().and_then(|value| match value {
                Component::Normal(name) => name.to_str(),
                _ => None,
            }) == Some("_templates")
            {
                anyhow::ensure!(
                    metadata.nlink() == 1,
                    "_templates contains a hard-linked file: {}",
                    target.display()
                );
            }
            let executable = metadata.permissions().mode() & 0o111 != 0;
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(if executable { 0o755 } else { 0o644 })
                .open(&target)?;
            std::io::copy(&mut input, &mut output)?;
            output.sync_all()?;
        } else {
            anyhow::bail!(
                "source tree contains a symlink or special entry: {}",
                target.display()
            );
        }
    }
    Ok(())
}

fn is_canonical_workspace_fact(path: &Path) -> bool {
    let components = path.components().collect::<Vec<_>>();
    components.len() == 4
        && matches!(components[0], Component::Normal(name) if name == "agents")
        && matches!(components[3], Component::Normal(name) if name == ".workspace")
}

fn openat_nofollow(parent: &File, name: &std::ffi::OsStr) -> Result<File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let name = CString::new(name.as_bytes()).context("source entry name contains NUL")?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open retained source entry");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn capability_dir_entries(dir: &File) -> Result<Vec<std::ffi::OsString>> {
    let path = retained_dir_path(dir)?;
    let mut names = fs::read_dir(&path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

fn capability_dir_entries_bounded(dir: &File, maximum: usize) -> Result<Vec<std::ffi::OsString>> {
    let path = retained_dir_path(dir)?;
    let mut names = Vec::new();
    for entry in fs::read_dir(&path)? {
        anyhow::ensure!(
            names.len() < maximum,
            "projection bundle filesystem entries exceed bounded enumeration"
        );
        names.push(entry?.file_name());
    }
    names.sort();
    Ok(names)
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

pub(crate) fn rename_noreplace(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "source contains NUL")
    })?;
    let target = CString::new(target.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "target contains NUL")
    })?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    let result = {
        let _ = (source, target);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "atomic no-replace directory rename is unsupported on this platform",
        ));
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(debug_assertions)]
fn test_checkpoint(point: &str) {
    if std::env::var("ST2_TEST_CATALOG_APPLY_PAUSE_AT").as_deref() == Ok(point)
        && let (Ok(ready), Ok(release)) = (
            std::env::var("ST2_TEST_CATALOG_APPLY_READY"),
            std::env::var("ST2_TEST_CATALOG_APPLY_RELEASE"),
        )
    {
        let _ = fs::write(&ready, point);
        while !Path::new(&release).exists() {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    if std::env::var("ST2_TEST_CATALOG_APPLY_CRASH_AT").as_deref() == Ok(point) {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn test_checkpoint(_point: &str) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_directory_facts_are_typed_into_the_projection_hash() {
        let files = BTreeMap::new();
        let no_facts = BTreeSet::new();
        let facts = BTreeSet::from(["agents/host/worker/.workspace".to_string()]);
        assert_ne!(
            hash_projection(&files, &no_facts),
            hash_projection(&files, &facts)
        );
    }

    #[test]
    fn admitted_apply_rejects_authority_for_another_catalog_before_marker_access() {
        let temp = tempfile::tempdir().unwrap();
        let authorized_catalog = temp.path().join("authorized");
        let requested_catalog = temp.path().join("requested");
        fs::create_dir(&authorized_catalog).unwrap();
        fs::create_dir(&requested_catalog).unwrap();

        let prepared = prepare_apply(ApplyRequest {
            catalog: requested_catalog.clone(),
            mode: ApplyMode::Resume,
        })
        .unwrap();
        let canonical = CanonicalCatalog::open(&authorized_catalog).unwrap();
        let lock = CatalogLock::exclusive(&authorized_catalog).unwrap();
        let authority = admit_catalog_publish(&canonical, &lock).unwrap();
        let error = apply_admitted(prepared, &authority).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("catalog mutation authority is bound to")
        );
        assert!(!requested_catalog.join(CONTROL_DIR).exists());
    }
}
