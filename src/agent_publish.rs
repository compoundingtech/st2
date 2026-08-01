//! Transactional publication of one canonical Agent Spec into a live catalog.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};

use agent_spec::discovery::parse_declared;
use anyhow::{Context, Result};
use kdl::KdlDocument;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::catalog_lock::CatalogLock;

const SCHEMA: &str = "st2.agent-publish.v1";

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
    pub status: PublishStatus,
    pub bus_id: String,
    pub path: PathBuf,
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
    identity: String,
}

impl Candidate {
    fn stage(catalog: &Path, source: PublishSource) -> Result<Self> {
        let control = catalog.join(crate::catalog_lock::CONTROL_DIR);
        let stage = tempfile::Builder::new()
            .prefix("agent-publish-")
            .tempdir_in(&control)
            .with_context(|| format!("stage publication in {}", control.display()))?;
        let kind = match &source {
            PublishSource::Spec(path) => {
                let metadata = fs::symlink_metadata(path)
                    .with_context(|| format!("read candidate spec {}", path.display()))?;
                anyhow::ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "candidate spec is not a regular file: {}",
                    path.display()
                );
                let bytes = fs::read(path)
                    .with_context(|| format!("read candidate spec {}", path.display()))?;
                write_synced(&stage.path().join("agent.kdl"), &bytes)?;
                CandidateKind::Spec
            }
            PublishSource::Bundle(path) => {
                let metadata = fs::symlink_metadata(path)
                    .with_context(|| format!("read bundle {}", path.display()))?;
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "bundle is not a real directory: {}",
                    path.display()
                );
                validate_bundle_tree(path)?;
                copy_tree(path, stage.path(), true)?;
                File::open(stage.path())?.sync_all()?;
                CandidateKind::Bundle
            }
        };
        let spec_path = stage.path().join("agent.kdl");
        anyhow::ensure!(
            match &source {
                PublishSource::Spec(path) =>
                    path.extension().and_then(|value| value.to_str()) == Some("kdl"),
                PublishSource::Bundle(_) => true,
            },
            "published spec must be canonical KDL"
        );
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
        let document = KdlDocument::parse(text)
            .map_err(|error| anyhow::anyhow!("KDL parse error: {error}"))?;
        anyhow::ensure!(
            document.nodes().len() == 1 && document.nodes()[0].name().value() == "agent",
            "candidate must contain exactly one top-level `agent` node"
        );
        let declared = parse_declared(&spec_path)
            .with_context(|| format!("parse candidate Agent Spec {}", spec_path.display()))?;
        anyhow::ensure!(
            declared.len() == 1,
            "candidate must declare exactly one agent (found {})",
            declared.len()
        );
        let host = declared[0]
            .host
            .as_deref()
            .filter(|value| !value.is_empty())
            .context("candidate must declare a non-empty explicit host")?;
        let identity = declared[0]
            .identity
            .as_deref()
            .filter(|value| !value.is_empty())
            .context("candidate must declare a non-empty explicit identity")?;
        validate_component("host", host)?;
        validate_component("identity", identity)?;
        Ok(Self {
            stage,
            kind,
            bytes,
            host: host.to_string(),
            identity: identity.to_string(),
        })
    }

    fn bus_id(&self) -> String {
        format!("{}.{}", self.host, self.identity)
    }
}

/// Publish one spec under the catalog's exclusive authoring lock.
pub fn publish(request: PublishRequest) -> Result<PublishResult> {
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    let _lock = CatalogLock::exclusive(&catalog)?;
    let candidate = Candidate::stage(&catalog, request.source)?;
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

    validate_overlay(&catalog, &candidate)?;
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
        return Ok(result(
            PublishStatus::Unchanged,
            &candidate,
            target_spec,
            before_hash,
            after_hash,
        ));
    }

    match candidate.kind {
        CandidateKind::Spec => {
            ensure_real_dir_chain(&catalog, &target_dir)?;
            atomic_write_spec(&target_spec, &candidate.bytes, before.is_some())?;
        }
        CandidateKind::Bundle => {
            let result = result(
                PublishStatus::Published,
                &candidate,
                target_spec,
                before_hash,
                after_hash,
            );
            atomic_publish_staged_bundle(candidate.stage, &target_dir)?;
            return Ok(result);
        }
    }
    Ok(result(
        PublishStatus::Published,
        &candidate,
        target_spec,
        before_hash,
        after_hash,
    ))
}

fn result(
    status: PublishStatus,
    candidate: &Candidate,
    path: PathBuf,
    before_sha256: Option<String>,
    after_sha256: String,
) -> PublishResult {
    PublishResult {
        schema: SCHEMA,
        status,
        bus_id: candidate.bus_id(),
        path,
        before_sha256,
        after_sha256,
    }
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

fn validate_overlay(catalog: &Path, candidate: &Candidate) -> Result<()> {
    let control = catalog.join(crate::catalog_lock::CONTROL_DIR);
    let shadow = tempfile::Builder::new()
        .prefix("catalog-admission-")
        .tempdir_in(&control)
        .with_context(|| format!("create validation shadow in {}", control.display()))?;
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

    let found = crate::discovery::discover(shadow.path());
    let hosts: BTreeSet<_> = found
        .specs
        .iter()
        .filter_map(|spec| spec.host.clone())
        .collect();
    let mut errors = BTreeSet::new();
    for host in hosts {
        let report = crate::validate::validate_for_host(shadow.path(), &host);
        errors.extend(
            report
                .issues
                .iter()
                .filter(|issue| issue.severity == crate::validate::Severity::Error)
                .map(|issue| format!("{} [{}]: {}", issue.path, issue.code, issue.message)),
        );
    }
    anyhow::ensure!(
        errors.is_empty(),
        "candidate fails full-catalog validation:\n{}",
        errors.into_iter().collect::<Vec<_>>().join("\n")
    );
    Ok(())
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

fn validate_bundle_tree(root: &Path) -> Result<()> {
    anyhow::ensure!(
        root.join("agent.kdl").is_file(),
        "bundle must contain agent.kdl at its root"
    );
    for entry in fs::read_dir(root).with_context(|| format!("read bundle {}", root.display()))? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            validate_bundle_tree_entries(&path)?;
        } else {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "bundle contains a non-regular entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn validate_bundle_tree_entries(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("read bundle {}", root.display()))? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            validate_bundle_tree_entries(&path)?;
        } else {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "bundle contains a non-regular entry: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path, sync: bool) -> Result<()> {
    for entry in fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.is_dir() {
            fs::create_dir(&to)?;
            copy_tree(&from, &to, sync)?;
            if sync {
                File::open(&to)?.sync_all()?;
            }
        } else if metadata.is_file() {
            fs::copy(&from, &to)?;
            if sync {
                OpenOptions::new().read(true).open(&to)?.sync_all()?;
            }
        } else {
            anyhow::bail!("unsupported bundle entry type: {}", from.display());
        }
    }
    Ok(())
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

fn atomic_write_spec(target: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    let parent = target.parent().context("spec target has no parent")?;
    let mut temp = tempfile::Builder::new()
        .prefix(".agent.kdl.publish-")
        .tempfile_in(parent)
        .with_context(|| format!("create temporary spec in {}", parent.display()))?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    if replace {
        temp.persist(target)
            .map_err(|error| error.error)
            .with_context(|| format!("replace {}", target.display()))?;
    } else {
        fs::hard_link(temp.path(), target)
            .with_context(|| format!("publish {}", target.display()))?;
        temp.close()?;
    }
    sync_dir(parent)
}

fn atomic_publish_staged_bundle(stage: tempfile::TempDir, target: &Path) -> Result<()> {
    let parent = target.parent().context("bundle target has no parent")?;
    rename_noreplace(&stage.keep(), target)
        .with_context(|| format!("publish bundle {}", target.display()))?;
    sync_dir(parent)
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

fn rename_noreplace(source: &Path, target: &Path) -> std::io::Result<()> {
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

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}
