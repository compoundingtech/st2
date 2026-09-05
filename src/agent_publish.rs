//! Transactional publication of one canonical Agent Spec into a live catalog.

use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use agent_spec::discovery::parse_declared;
use agent_spec::{DeclaredValue, parse_declared_document};
use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::catalog_lock::CatalogLock;
use crate::catalog_transaction::sync_dir;
use crate::{AgentAddress, AgentId};

const SCHEMA: &str = "st2.agent-publish.v2";
const DIGEST_SCHEMA: &str = "st2.agent-source-digest.v1";
const BUNDLE_DIGEST_DOMAIN: &[u8] = b"st2.agent-publish-bundle.v1\0";

/// A classified publication refusal. `code` is stable for machine consumers.
#[derive(Debug)]
pub struct PublishRefusal {
    pub code: &'static str,
    pub message: String,
}

impl PublishRefusal {
    fn new(code: &'static str, message: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self {
            code,
            message: message.into(),
        })
    }
}

impl std::fmt::Display for PublishRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for PublishRefusal {}

/// A publication that would change an existing subject's immutable ID.
pub const IMMUTABLE_AGENT_ID: &str = "immutable-agent-id";

/// A creating publication that did not mint an explicit generated ID.
pub const CREATION_REQUIRES_GENERATED_ID: &str = "creation-requires-generated-id";

#[derive(Debug, Clone)]
pub enum PublishSource {
    Spec(PathBuf),
    Bundle(PathBuf),
}

#[derive(Debug, Clone)]
pub enum PublishExpectation {
    Absent,
    Sha256(String),
}

#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub catalog: PathBuf,
    pub source: PublishSource,
    pub expectation: PublishExpectation,
    pub input_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PublishStatus {
    Published,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishResult {
    pub schema: &'static str,
    pub policy_profile: &'static str,
    pub agent_spec_revision: &'static str,
    pub status: PublishStatus,
    /// The published subject's catalog-global immutable ID: the ownership key every automation,
    /// durable edge, and task ID uses.
    pub agent_id: String,
    /// How a human reaches the published subject right now, or `None` for a retired subject that
    /// released its address.
    pub bus_address: Option<String>,
    pub path: PathBuf,
    pub input_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_sha256: Option<String>,
    pub after_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateKind {
    Spec,
    Bundle,
}

#[derive(Debug)]
struct Candidate {
    stage: tempfile::TempDir,
    kind: CandidateKind,
    bytes: Vec<u8>,
    host: String,
    /// The positional declaration key. Also the legacy address fallback — never the ID.
    identity: String,
    /// Explicit `id` as declared. `None` is an unmigrated legacy candidate whose ID is its frozen
    /// legacy bus identity.
    id: Option<AgentId>,
    /// Explicit `address` as declared. `None` falls back to `identity`.
    address: Option<AgentAddress>,
    /// Whether ordinary address routing may reach the candidate once published.
    routable: bool,
    input_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Spec,
    Bundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDigest {
    pub schema: &'static str,
    pub kind: SourceKind,
    pub sha256: String,
}

/// The staging name `axe agent check` requires in place at
/// `<catalog>/agents/<host>/<identity>/agent.kdl.candidate`.
const CANDIDATE_SPEC_FILE_NAME: &str = "agent.kdl.candidate";

/// Whether `path` names a canonical KDL declaration source.
///
/// The gate exists to keep legacy TOML/JSON declarations out of publication, so it reads the
/// file name rather than the bytes. `agent.kdl.candidate` is accepted because the prescribed
/// authoring workflow validates a candidate under exactly that name; rejecting it forced a
/// second copy of the same bytes to exist during publication.
fn is_canonical_kdl_spec_name(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name == CANDIDATE_SPEC_FILE_NAME
        || path.extension().and_then(|value| value.to_str()) == Some("kdl")
}

impl Candidate {
    fn stage_in(parent: &Path, source: PublishSource) -> Result<Self> {
        let stage = tempfile::Builder::new()
            .prefix("agent-publish-")
            .tempdir_in(parent)
            .with_context(|| format!("stage publication in {}", parent.display()))?;
        let kind = match &source {
            PublishSource::Spec(path) => {
                let mut input = open_regular_nofollow(path)
                    .with_context(|| format!("open candidate spec {}", path.display()))?;
                let metadata = input.metadata()?;
                anyhow::ensure!(
                    metadata.is_file(),
                    "candidate spec is not a regular file: {}",
                    path.display()
                );
                let mut bytes = Vec::new();
                input.read_to_end(&mut bytes)?;
                write_synced(&stage.path().join("agent.kdl"), &bytes)?;
                CandidateKind::Spec
            }
            PublishSource::Bundle(path) => {
                crate::catalog_transaction::capture_real_tree(path, stage.path())
                    .with_context(|| format!("capture bundle {}", path.display()))?;
                File::open(stage.path())?.sync_all()?;
                CandidateKind::Bundle
            }
        };
        let spec_path = stage.path().join("agent.kdl");
        if let PublishSource::Spec(path) = &source {
            anyhow::ensure!(
                is_canonical_kdl_spec_name(path),
                "spec source must be named `*.kdl` or `{CANDIDATE_SPEC_FILE_NAME}`, found {}",
                path.display()
            );
        }
        let metadata = fs::symlink_metadata(&spec_path)
            .with_context(|| format!("read candidate spec {}", spec_path.display()))?;
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "candidate spec is not a regular file: {}",
            spec_path.display()
        );
        let bytes = fs::read(&spec_path)
            .with_context(|| format!("read candidate spec {}", spec_path.display()))?;
        let text = std::str::from_utf8(&bytes).context("candidate Agent Spec is not UTF-8")?;
        let parsed = parse_declared_document(&spec_path, text);
        anyhow::ensure!(
            parsed.is_valid(),
            "candidate fails strict declaration parsing:\n{}",
            parsed
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.severity == agent_spec::DeclaredSeverity::Error)
                .map(|diagnostic| format!(
                    "{}:{}:{} [{}]: {}",
                    diagnostic.source.display(),
                    diagnostic.span.line,
                    diagnostic.span.column,
                    diagnostic.code,
                    diagnostic.message
                ))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let document = parsed
            .document
            .context("candidate Agent Spec has no parsed declaration")?;
        anyhow::ensure!(
            document.nodes.len() == 1 && document.agents.len() == 1,
            "candidate must contain exactly one top-level `agent` node"
        );
        let declared = &document.agents[0];
        let host = declared
            .field("host")
            .and_then(|field| field.argument(0))
            .and_then(DeclaredValue::as_str)
            .filter(|value| !value.is_empty())
            .context("candidate must declare a non-empty explicit host")?;
        let identity = declared
            .identity()
            .and_then(DeclaredValue::as_str)
            .filter(|value| !value.is_empty())
            .context("candidate must declare a non-empty explicit identity")?;
        validate_component("host", host)?;
        validate_component("identity", identity)?;
        // Lower the already strictly parsed candidate so `id`, `address`, and desired state come
        // from the one canonical Agent Spec lowering rather than a second hand-rolled reader.
        let (specs, _) = agent_spec::discover_file(stage.path(), &spec_path)
            .context("lower the candidate Agent Spec")?;
        let spec = match specs.as_slice() {
            [spec] => spec,
            _ => anyhow::bail!("candidate must lower to exactly one Agent Spec"),
        };
        let input_sha256 = match kind {
            CandidateKind::Spec => sha256(&bytes),
            CandidateKind::Bundle => bundle_sha256(stage.path())?,
        };
        Ok(Self {
            id: spec.id.clone(),
            address: spec.address.clone(),
            routable: !spec.desired_state.is_retired(),
            stage,
            kind,
            bytes,
            host: host.to_string(),
            identity: identity.to_string(),
            input_sha256,
        })
    }

    /// The positional declaration key `<host>.<identity>`.
    fn legacy_bus_identity(&self) -> String {
        agent_spec::legacy_bus_identity(&self.host, &self.identity)
    }

    /// The catalog-global immutable ID this publication claims.
    fn agent_id(&self) -> String {
        match &self.id {
            Some(id) => id.as_str().to_owned(),
            None => self.legacy_bus_identity(),
        }
    }

    /// The human route the published subject answers on, or `None` once it is non-routable.
    fn bus_address(&self) -> Option<String> {
        let effective = match &self.address {
            Some(address) => address.as_str(),
            None => self.identity.as_str(),
        };
        self.routable
            .then(|| agent_spec::bus_address(&self.host, effective))
    }
}

/// Lower the incumbent declaration bytes under the lock and report the ID they own.
///
/// The bytes are staged at the same canonical placement they occupy in the live catalog, then read
/// through exactly the Agent Spec lowering [`Candidate::stage_in`] uses, so this mints no second
/// reader and no second precedence rule. An unmigrated incumbent yields its frozen legacy bus
/// identity — the same value it will keep after migration.
fn incumbent_agent_id(bytes: &[u8], host: &str, identity: &str) -> Result<String> {
    let staging = tempfile::tempdir().context("create incumbent lowering staging root")?;
    let directory = staging.path().join(host).join(identity);
    fs::create_dir_all(&directory).context("stage the incumbent declaration directory")?;
    let staged = directory.join("agent.kdl");
    fs::write(&staged, bytes).context("stage the incumbent declaration")?;
    let (specs, _) = agent_spec::discover_file(staging.path(), &staged)
        .context("lower the incumbent Agent Spec")?;
    match specs.as_slice() {
        [spec] => Ok(spec.agent_id(host)),
        [] => anyhow::bail!("the incumbent declaration lowers to no Agent Spec"),
        many => anyhow::bail!(
            "the incumbent declaration lowers to {} Agent Specs",
            many.len()
        ),
    }
}

/// Whether `value` is a canonical lowercase hyphenated UUIDv7.
///
/// Creation mints a brand-new subject, and a brand-new subject's ID is generated, never derived
/// from a route. Accepting a frozen-legacy-shaped ID here would let publication keep minting
/// placement-shaped IDs forever, which is exactly what decision 0015 retires.
fn is_canonical_uuid_v7(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        let expected_hyphen = matches!(index, 8 | 13 | 18 | 23);
        let is_hyphen = *byte == b'-';
        if expected_hyphen != is_hyphen {
            return false;
        }
        if !is_hyphen && !byte.is_ascii_digit() && !(b'a'..=b'f').contains(byte) {
            return false;
        }
    }
    // Version 7 in the high nibble of octet 6, RFC 9562 variant `10xx` in octet 8.
    bytes[14] == b'7' && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

pub fn digest_source(source: PublishSource) -> Result<SourceDigest> {
    let parent = tempfile::tempdir().context("create source-digest staging root")?;
    let candidate = Candidate::stage_in(parent.path(), source)?;
    Ok(SourceDigest {
        schema: DIGEST_SCHEMA,
        kind: match candidate.kind {
            CandidateKind::Spec => SourceKind::Spec,
            CandidateKind::Bundle => SourceKind::Bundle,
        },
        sha256: candidate.input_sha256,
    })
}
/// Strictly validate one unpublished candidate as an overlay on the selected live catalog.
///
/// The live declaration plane is held under its shared authoring fence and never modified. The
/// returned report uses the ordinary versioned validation vocabulary so callers do not need to
/// construct or interpret a shadow catalog themselves.
pub fn validate_candidate_for_host(
    catalog: &Path,
    source: PublishSource,
    this_host: &str,
) -> crate::validate::Report {
    let source_path = match &source {
        PublishSource::Spec(path) | PublishSource::Bundle(path) => path.display().to_string(),
    };
    let attempt = (|| -> Result<crate::validate::Report> {
        let catalog = catalog
            .canonicalize()
            .with_context(|| format!("canonicalize catalog {}", catalog.display()))?;
        let _lock = CatalogLock::shared(&catalog)?;
        let staging = tempfile::tempdir().context("create candidate validation staging root")?;
        let candidate = Candidate::stage_in(staging.path(), source)?;
        let shadow = build_overlay(&catalog, staging.path(), &candidate)?;
        Ok(crate::validate::validate_strict_for_host(
            shadow.path(),
            this_host,
        ))
    })();
    match attempt {
        Ok(report) => report,
        Err(error) => {
            let mut report = crate::validate::validate_strict_for_host(catalog, this_host);
            report.issues.push(crate::validate::Issue::error(
                "candidate-error",
                source_path,
                None,
                format!("{error:#}"),
            ));
            report
        }
    }
}

/// Publish one spec under the catalog's exclusive authoring lock.
pub fn publish(request: PublishRequest) -> Result<PublishResult> {
    validate_sha256(&request.input_sha256)?;
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    let lock = CatalogLock::exclusive(&catalog)?;
    let control = crate::catalog_transaction::retained_dir_path(lock.control())?;
    let candidate = Candidate::stage_in(&control, request.source)?;
    anyhow::ensure!(
        candidate.input_sha256 == request.input_sha256,
        "publication input precondition failed: expected sha256 {}, captured {}",
        request.input_sha256,
        candidate.input_sha256
    );
    let target_dir = catalog
        .join("agents")
        .join(&candidate.host)
        .join(&candidate.identity);
    let target_spec = target_dir.join("agent.kdl");
    validate_existing_ancestry(&catalog, &target_dir)?;
    let before = read_regular_optional(&target_spec)?;
    let same_spec = before.as_deref() == Some(candidate.bytes.as_slice());
    let before_hash = before.as_deref().map(sha256);
    let after_hash = sha256(&candidate.bytes);

    match &request.expectation {
        PublishExpectation::Absent => {
            if let Some(current) = &before {
                anyhow::ensure!(
                    current == &candidate.bytes,
                    "publish precondition failed: {} already exists with sha256 {}",
                    target_spec.display(),
                    before_hash.as_deref().unwrap_or("<unreadable>")
                );
            }
        }
        PublishExpectation::Sha256(expected) => {
            validate_sha256(expected)?;
            let actual = before_hash.as_deref().with_context(|| {
                format!(
                    "publish precondition failed: {} is absent",
                    target_spec.display()
                )
            })?;
            anyhow::ensure!(
                actual == expected,
                "publish precondition failed: expected sha256 {expected}, found {actual}"
            );
        }
    }

    if candidate.kind == CandidateKind::Bundle {
        anyhow::ensure!(
            matches!(request.expectation, PublishExpectation::Absent),
            "bundle publication is create-only; use --expect-absent"
        );
        match fs::symlink_metadata(&target_dir) {
            Ok(metadata) if before.is_some() => anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "bundle target is not a real directory: {}",
                target_dir.display()
            ),
            Ok(_) => anyhow::bail!(
                "bundle target directory already exists without agent.kdl: {}",
                target_dir.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read bundle target {}", target_dir.display()));
            }
        }
    }

    // The subject's ID is immutable, and a byte-level expectation cannot see identity: an
    // `--expect-sha256` update that matches the incumbent bytes exactly would otherwise be free to
    // re-key the subject, which F02 refuses rather than inferring a rename, a replacement, or a
    // state migration. Compare the two IDs before anything is admitted or written.
    match &before {
        Some(current) => {
            let incumbent = incumbent_agent_id(current, &candidate.host, &candidate.identity)?;
            let proposed = candidate.agent_id();
            if incumbent != proposed {
                return Err(PublishRefusal::new(
                    IMMUTABLE_AGENT_ID,
                    format!(
                        "{} already declares agent id '{incumbent}'; this candidate claims '{proposed}'. An agent ID is immutable: retire the subject and create a replacement instead of re-keying it.",
                        target_spec.display()
                    ),
                ));
            }
        }
        None => {
            // Creation, not update: a brand-new subject mints a generated ID. The frozen-legacy
            // fallback exists to read and update a subject that predates migration, never to keep
            // minting new placement-shaped IDs.
            let minted = candidate.id.as_ref().map(|id| id.as_str().to_owned());
            if !minted.as_deref().is_some_and(is_canonical_uuid_v7) {
                return Err(PublishRefusal::new(
                    CREATION_REQUIRES_GENERATED_ID,
                    match minted {
                        Some(declared) => format!(
                            "creating {} requires a generated canonical UUIDv7 `id`; '{declared}' is not one",
                            target_spec.display()
                        ),
                        None => format!(
                            "creating {} requires an explicit generated canonical UUIDv7 `id`",
                            target_spec.display()
                        ),
                    },
                ));
            }
        }
    }

    validate_overlay(&catalog, &control, &candidate)?;
    ensure_real_dir_chain(
        &catalog,
        target_dir
            .parent()
            .context("publication target has no parent")?,
    )?;

    if same_spec {
        if candidate.kind == CandidateKind::Bundle {
            anyhow::ensure!(
                bundle_projection_matches(candidate.stage.path(), &target_dir)?,
                "bundle target exists but does not contain the complete candidate payload: {}",
                target_dir.display()
            );
        }
        let verified_after =
            verify_published_spec(&catalog, &target_spec, &candidate.bytes, &after_hash)?;
        return Ok(result(
            PublishStatus::Unchanged,
            candidate.agent_id(),
            candidate.bus_address(),
            candidate.input_sha256.clone(),
            target_spec,
            before_hash,
            verified_after,
        ));
    }

    test_before_publication();
    let generation = lock.begin_generation_commit()?;
    let agent_id = candidate.agent_id();
    let bus_address = candidate.bus_address();
    let input_sha256 = candidate.input_sha256.clone();
    match candidate.kind {
        CandidateKind::Spec => {
            ensure_real_dir_chain(&catalog, &target_dir)?;
            atomic_write_spec(
                lock.control(),
                &catalog,
                &target_spec,
                &candidate.bytes,
                before.is_some(),
            )?;
        }
        CandidateKind::Bundle => {
            atomic_publish_staged_bundle(lock.control(), &catalog, candidate.stage, &target_dir)?;
        }
    }
    test_after_publication_before_readback();
    let verified_after =
        verify_published_spec(&catalog, &target_spec, &candidate.bytes, &after_hash)?;
    generation.commit()?;
    Ok(result(
        PublishStatus::Published,
        agent_id,
        bus_address,
        input_sha256,
        target_spec,
        before_hash,
        verified_after,
    ))
}

fn result(
    status: PublishStatus,
    agent_id: String,
    bus_address: Option<String>,
    input_sha256: String,
    path: PathBuf,
    before_sha256: Option<String>,
    after_sha256: String,
) -> PublishResult {
    PublishResult {
        schema: SCHEMA,
        policy_profile: crate::validate::CORE_CATALOG_POLICY_PROFILE,
        agent_spec_revision: agent_spec::AGENT_SPEC_REVISION,
        status,
        agent_id,
        bus_address,
        path,
        input_sha256,
        before_sha256,
        after_sha256,
    }
}

fn verify_published_spec(
    catalog: &Path,
    target: &Path,
    expected_bytes: &[u8],
    expected_sha256: &str,
) -> Result<String> {
    let observed = read_regular_beneath(catalog, target)
        .with_context(|| format!("read back published Agent Spec {}", target.display()))?;
    let observed_sha256 = sha256(&observed);
    anyhow::ensure!(
        observed_sha256 == expected_sha256 && observed == expected_bytes,
        "published Agent Spec readback mismatch: expected sha256 {expected_sha256}, found {observed_sha256}"
    );
    crate::catalog_transaction::validate_full_catalog(
        catalog,
        &crate::catalog_archive::archived_subjects(catalog)?,
    )
        .context("published catalog fails locked core/catalog re-admission")?;
    Ok(observed_sha256)
}

fn read_regular_beneath(catalog: &Path, target: &Path) -> Result<Vec<u8>> {
    let parent = target
        .parent()
        .context("published Agent Spec has no parent")?;
    let parent = crate::catalog_transaction::open_dir_beneath(catalog, parent)?;
    let leaf = target
        .file_name()
        .context("published Agent Spec has no file name")?;
    let leaf = CString::new(leaf.as_bytes()).context("published Agent Spec name contains NUL")?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            leaf.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).context("open published Agent Spec readback");
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "published Agent Spec readback is not a regular file"
    );
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn validate_component(field: &str, value: &str) -> Result<()> {
    let mut components = Path::new(value).components();
    anyhow::ensure!(
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none(),
        "{field} must be one safe path component"
    );
    anyhow::ensure!(
        !matches!(value, ".git" | ".st2"),
        "{field} uses a reserved catalog control name"
    );
    Ok(())
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

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_regular_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "publication target is not a regular file: {}",
                path.display()
            );
            Ok(Some(
                fs::read(path).with_context(|| format!("read {}", path.display()))?,
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

/// Admit the candidate as an overlay on the complete prospective catalog.
///
/// Agent IDs are catalog-global across the live plane and the structural archive, so the overlay
/// is proved against both: publishing a spec whose ID an archived subject still holds must fail
/// even though that subject is undiscoverable.
fn validate_overlay(catalog: &Path, control: &Path, candidate: &Candidate) -> Result<()> {
    let shadow = build_overlay(catalog, control, candidate)?;
    let archived = crate::catalog_archive::archived_subjects(catalog)?;
    crate::catalog_transaction::validate_full_catalog(shadow.path(), &archived)
        .context("candidate fails full-catalog validation")
}

fn build_overlay(
    catalog: &Path,
    staging_parent: &Path,
    candidate: &Candidate,
) -> Result<tempfile::TempDir> {
    let shadow = tempfile::Builder::new()
        .prefix("catalog-admission-")
        .tempdir_in(staging_parent)
        .with_context(|| format!("create validation shadow in {}", staging_parent.display()))?;
    let live_target = catalog
        .join("agents")
        .join(&candidate.host)
        .join(&candidate.identity);
    copy_filtered_catalog(catalog, shadow.path(), catalog, &live_target)?;
    let target = shadow
        .path()
        .join("agents")
        .join(&candidate.host)
        .join(&candidate.identity);
    fs::create_dir_all(&target)
        .with_context(|| format!("create validation overlay {}", target.display()))?;
    match candidate.kind {
        CandidateKind::Spec => fs::write(target.join("agent.kdl"), &candidate.bytes)
            .context("write candidate into validation shadow")?,
        CandidateKind::Bundle => overlay_tree(candidate.stage.path(), &target)?,
    }
    Ok(shadow)
}

fn copy_filtered_catalog(
    source: &Path,
    destination: &Path,
    catalog: &Path,
    prospective_target: &Path,
) -> Result<()> {
    let declaration_parent = source == prospective_target || is_declaration_parent(source)?;
    for entry in fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let from = entry.path();
        let name = entry.file_name();
        let name_text = name.to_string_lossy();
        if from == catalog.join("pty")
            || matches!(name_text.as_ref(), ".git" | ".st2")
            || (declaration_parent
                && matches!(
                    name_text.as_ref(),
                    "resources" | "archive" | "inbox" | "status"
                )
                || declaration_parent && name_text.starts_with(".status.tmp-"))
        {
            continue;
        }
        let to = destination.join(name);
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.is_dir() {
            fs::create_dir(&to)?;
            copy_filtered_catalog(&from, &to, catalog, prospective_target)?;
        } else if metadata.is_file() {
            fs::copy(&from, &to)?;
        } else if metadata.file_type().is_symlink() {
            symlink(fs::read_link(&from)?, &to)?;
        } else {
            anyhow::bail!("unsupported catalog entry type: {}", from.display());
        }
    }
    Ok(())
}

fn is_declaration_parent(path: &Path) -> Result<bool> {
    // These are independent catalog children discovered while constructing the post-write shadow,
    // not a reparse of `Candidate`: each file must be admitted before its adjacent state is pruned.
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry?;
        let candidate = entry.path();
        let extension = candidate.extension().and_then(|value| value.to_str());
        if !matches!(extension, Some("kdl" | "toml" | "json")) {
            continue;
        }
        if candidate.file_stem().and_then(|value| value.to_str()) == Some("agent") {
            return Ok(true);
        }
        let metadata = match fs::symlink_metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.is_file()
            && !metadata.file_type().is_symlink()
            && parse_declared(&candidate).is_ok_and(|declared| !declared.is_empty())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn overlay_tree(source: &Path, destination: &Path) -> Result<()> {
    for entry in fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.is_dir() {
            fs::create_dir_all(&to)?;
            overlay_tree(&from, &to)?;
        } else if metadata.is_file() {
            fs::copy(&from, &to)?;
        } else {
            anyhow::bail!("unsupported staged bundle entry type: {}", from.display());
        }
    }
    Ok(())
}

fn open_regular_nofollow(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open regular file {}", path.display()))
}

fn bundle_sha256(root: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(BUNDLE_DIGEST_DOMAIN);
    hash_bundle_dir(root, root, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

fn hash_bundle_dir(root: &Path, dir: &Path, hasher: &mut Sha256) -> Result<()> {
    for entry in sorted_entries(dir)? {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)?
            .to_str()
            .context("bundle path is not UTF-8")?
            .replace(std::path::MAIN_SEPARATOR, "/");
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            hash_record(hasher, b'd', &relative, false, &[]);
            hash_bundle_dir(root, &path, hasher)?;
        } else if metadata.is_file() && !metadata.file_type().is_symlink() {
            hash_record(
                hasher,
                b'f',
                &relative,
                metadata.permissions().mode() & 0o111 != 0,
                &fs::read(&path)?,
            );
        } else {
            anyhow::bail!(
                "staged bundle contains a symlink or special entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn hash_record(hasher: &mut Sha256, kind: u8, path: &str, executable: bool, bytes: &[u8]) {
    hasher.update([kind]);
    hasher.update((path.len() as u64).to_be_bytes());
    hasher.update(path.as_bytes());
    hasher.update([u8::from(executable)]);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn sorted_entries(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn ensure_real_dir_chain(catalog: &Path, target: &Path) -> Result<()> {
    let relative = target
        .strip_prefix(catalog)
        .context("publication target escapes catalog")?;
    let mut current = catalog.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("publication target contains an unsafe path component");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "publication path is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("create {}", current.display()))?;
                sync_dir(
                    current
                        .parent()
                        .context("created directory has no parent")?,
                )?;
            }
            Err(error) => return Err(error).with_context(|| format!("read {}", current.display())),
        }
    }
    Ok(())
}

fn atomic_write_spec(
    control_file: &File,
    catalog: &Path,
    target: &Path,
    bytes: &[u8],
    replace: bool,
) -> Result<()> {
    let parent = target.parent().context("spec target has no parent")?;
    let control = crate::catalog_transaction::retained_dir_path(control_file)?;
    let mut temp = tempfile::Builder::new()
        .prefix("agent-publish-leaf-")
        .tempfile_in(&control)
        .with_context(|| format!("create temporary spec in {}", control.display()))?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    test_crash_after_temporary_write();
    if replace {
        crate::catalog_transaction::persist_tempfile_from_control(
            control_file,
            catalog,
            temp,
            target,
        )
        .with_context(|| format!("replace {}", target.display()))?;
    } else {
        crate::catalog_transaction::link_tempfile_from_control(
            control_file,
            catalog,
            &temp,
            target,
        )
        .with_context(|| format!("publish {}", target.display()))?;
        temp.close()?;
    }
    crate::catalog_transaction::open_dir_beneath(catalog, parent)?
        .sync_all()
        .map_err(Into::into)
}

#[cfg(debug_assertions)]
fn test_crash_after_temporary_write() {
    if std::env::var_os("ST2_TEST_AGENT_PUBLISH_CRASH_AFTER_TEMP").is_some() {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn test_crash_after_temporary_write() {}

#[cfg(debug_assertions)]
fn test_before_publication() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_AGENT_PUBLISH_READY"),
        std::env::var("ST2_TEST_AGENT_PUBLISH_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_before_publication() {}

#[cfg(debug_assertions)]
fn test_after_publication_before_readback() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_AGENT_PUBLISH_READBACK_READY"),
        std::env::var("ST2_TEST_AGENT_PUBLISH_READBACK_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_after_publication_before_readback() {}

fn atomic_publish_staged_bundle(
    control: &File,
    catalog: &Path,
    stage: tempfile::TempDir,
    target: &Path,
) -> Result<()> {
    let parent = target.parent().context("bundle target has no parent")?;
    crate::catalog_transaction::rename_noreplace_between_dirs(
        control,
        catalog,
        stage.path(),
        target,
    )
    .with_context(|| format!("publish bundle {}", target.display()))?;
    crate::catalog_transaction::open_dir_beneath(catalog, parent)?
        .sync_all()
        .map_err(Into::into)
}

fn bundle_projection_matches(source: &Path, target: &Path) -> Result<bool> {
    for entry in fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.is_dir() {
            let target_metadata = match fs::symlink_metadata(&to) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            };
            if !target_metadata.is_dir() || target_metadata.file_type().is_symlink() {
                return Ok(false);
            }
            if !bundle_projection_matches(&from, &to)? {
                return Ok(false);
            }
        } else if metadata.is_file() {
            let target_metadata = match fs::symlink_metadata(&to) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            };
            if !target_metadata.is_file()
                || target_metadata.file_type().is_symlink()
                || target_metadata.permissions().mode() & 0o7777
                    != metadata.permissions().mode() & 0o7777
            {
                return Ok(false);
            }
            match fs::read(&to) {
                Ok(bytes) if bytes == fs::read(&from)? => {}
                Ok(_) => return Ok(false),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        } else {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_existing_ancestry(catalog: &Path, target: &Path) -> Result<()> {
    let relative = target
        .strip_prefix(catalog)
        .context("publication target escapes catalog")?;
    let mut current = catalog.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("publication target contains an unsafe path component");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "publication path is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).with_context(|| format!("read {}", current.display())),
        }
    }
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
