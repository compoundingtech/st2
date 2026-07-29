//! Experimental content-addressed Agent Spec catalog.
//!
//! Immutable declarations and commits live below `.st2/catalog-v1`. A single
//! mutable root head selects one complete catalog snapshot. No `agent.kdl`
//! projection is created: `AgentSpec::path` points at immutable source bytes,
//! while `AgentSpec::agent_dir` points at stable mutable state.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const SCHEMA: u32 = 1;
static TEMP_SERIAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSpec {
    pub object: String,
    pub host: String,
    pub identity: String,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefCommit {
    pub schema: u32,
    pub parent: Option<String>,
    pub host: String,
    pub identity: String,
    pub manager: String,
    pub target_object: Option<String>,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefHead {
    pub commit: String,
    pub value: RefCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceBindingCommit {
    pub schema: u32,
    pub parent: Option<String>,
    pub host: String,
    pub identity: String,
    pub manager: String,
    pub state_relative: PathBuf,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBinding {
    pub commit: String,
    pub value: ResourceBindingCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeatAdmission {
    pub schema: u32,
    pub host: String,
    pub identity: String,
    pub ref_commit: String,
    pub resource_binding_commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionObject {
    pub digest: String,
    pub value: SeatAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRootCommit {
    pub schema: u32,
    pub parent: Option<String>,
    pub manager: String,
    /// Bus id (`host.identity`) to immutable SeatAdmission digest.
    pub admissions: BTreeMap<String, String>,
    /// Exact seat updates requested by this operation, for unambiguous replay.
    pub updates: BTreeMap<String, String>,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRoot {
    pub commit: String,
    pub value: CatalogRootCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionSelection {
    pub ref_commit: String,
    pub resource_binding_commit: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedSeat {
    pub root_commit: String,
    pub admission: String,
    pub ref_commit: String,
    pub resource_binding_commit: String,
    pub spec_object: String,
    pub spec: crate::spec::AgentSpec,
}

pub struct CatalogStore {
    catalog_root: PathBuf,
    store_root: PathBuf,
}

impl CatalogStore {
    pub fn new(catalog_root: impl Into<PathBuf>) -> Self {
        let catalog_root = catalog_root.into();
        let store_root = catalog_root.join(".st2").join("catalog-v1");
        Self {
            catalog_root,
            store_root,
        }
    }

    /// Parse and identify one exact KDL declaration without capturing referenced inputs.
    pub fn prepare(&self, bytes: &[u8]) -> Result<PreparedSpec> {
        let text = std::str::from_utf8(bytes).context("Agent Spec is not UTF-8")?;
        let mut raws = crate::kdl_format::parse_kdl(text)?;
        if raws.len() != 1 {
            bail!(
                "a catalog object must contain exactly one agent declaration (found {})",
                raws.len()
            );
        }
        let raw = raws.pop().expect("length checked");
        let identity = raw
            .identity
            .clone()
            .filter(|value| !value.is_empty())
            .context("content-addressed declaration must carry identity in content")?;
        let host = raw
            .host
            .clone()
            .filter(|value| !value.is_empty())
            .context("content-addressed declaration must carry host in content")?;
        if !raw.looks_like_spec() {
            bail!("object is not an Agent Spec");
        }
        let spec = raw.into_agent_spec(
            identity.clone(),
            Some(host.clone()),
            PathBuf::from("<prepared-agent.kdl>"),
        );
        if !spec.is_runnable() && !spec.retired {
            bail!("active Agent Spec is not runnable");
        }
        Ok(PreparedSpec {
            object: digest("st2-agent-spec-object-v1", bytes),
            host,
            identity,
            bytes: bytes.to_vec(),
        })
    }

    /// Import exact bytes. An existing object is verified and never overwritten.
    pub fn import(&self, prepared: &PreparedSpec) -> Result<PathBuf> {
        let path = self.object_path(&prepared.object)?;
        publish_immutable(&path, &prepared.bytes)?;
        if fs::read(&path)? != prepared.bytes {
            bail!("immutable object collision at {}", path.display());
        }
        Ok(path)
    }

    /// Per-seat ref CAS. `expected = None` is create-only.
    pub fn compare_and_set_ref(
        &self,
        host: &str,
        identity: &str,
        expected: Option<&str>,
        manager: &str,
        target_object: Option<&str>,
        operation_id: &str,
    ) -> Result<RefHead> {
        validate_component("host", host)?;
        validate_component("identity", identity)?;
        validate_component("manager", manager)?;
        validate_operation(operation_id)?;
        validate_optional_digest(expected)?;
        if let Some(object) = target_object
            && !self.object_path(object)?.is_file()
        {
            bail!("target object '{object}' has not been imported");
        }
        let _guard = FileLock::acquire(&self.ref_lock_path(host, identity))?;
        let current = self.read_ref_head(host, identity)?;
        if let Some(head) = &current
            && head.value.operation_id == operation_id
            && head.value.manager == manager
        {
            if head.value.parent.as_deref() == expected
                && head.value.target_object.as_deref() == target_object
            {
                return Ok(head.clone());
            }
            bail!("operation id replay does not match the original ref request");
        }
        if current.as_ref().map(|head| head.commit.as_str()) != expected {
            bail!(
                "ref CAS conflict: expected {:?}, found {:?}",
                expected,
                current.as_ref().map(|head| head.commit.as_str())
            );
        }
        if let Some(head) = &current
            && head.value.manager != manager
        {
            bail!(
                "manager conflict: ref is owned by '{}', caller is '{}'",
                head.value.manager,
                manager
            );
        }
        let value = RefCommit {
            schema: SCHEMA,
            parent: current.as_ref().map(|head| head.commit.clone()),
            host: host.to_string(),
            identity: identity.to_string(),
            manager: manager.to_string(),
            target_object: target_object.map(str::to_string),
            operation_id: operation_id.to_string(),
        };
        let bytes = encoded(&value)?;
        let commit = digest("st2-agent-spec-ref-commit-v1", &bytes);
        publish_immutable(&self.ref_commit_path(&commit)?, &bytes)?;
        write_atomic(
            &self.ref_head_path(host, identity),
            format!("{commit}\n").as_bytes(),
        )?;
        Ok(RefHead { commit, value })
    }

    pub fn read_ref_head(&self, host: &str, identity: &str) -> Result<Option<RefHead>> {
        let Some(commit) = read_head_value(&self.ref_head_path(host, identity))? else {
            return Ok(None);
        };
        Ok(Some(self.read_ref_commit(&commit)?))
    }

    pub fn commit_resource_binding(
        &self,
        parent: Option<&str>,
        host: &str,
        identity: &str,
        manager: &str,
        state_relative: &Path,
        operation_id: &str,
    ) -> Result<ResourceBinding> {
        validate_component("host", host)?;
        validate_component("identity", identity)?;
        validate_component("manager", manager)?;
        validate_relative_state_path(state_relative)?;
        validate_state_path_custody(&self.catalog_root, state_relative)?;
        validate_operation(operation_id)?;
        validate_optional_digest(parent)?;
        if let Some(parent) = parent {
            let previous = self.read_resource_binding(parent)?;
            if previous.value.host != host || previous.value.identity != identity {
                bail!("resource binding parent belongs to a different seat");
            }
            if previous.value.manager != manager {
                bail!("resource binding manager conflict");
            }
        }
        let value = ResourceBindingCommit {
            schema: SCHEMA,
            parent: parent.map(str::to_string),
            host: host.to_string(),
            identity: identity.to_string(),
            manager: manager.to_string(),
            state_relative: state_relative.to_path_buf(),
            operation_id: operation_id.to_string(),
        };
        let bytes = encoded(&value)?;
        let commit = digest("st2-resource-binding-commit-v1", &bytes);
        publish_immutable(&self.binding_path(&commit)?, &bytes)?;
        Ok(ResourceBinding { commit, value })
    }

    /// Atomically update one seat in the selected complete catalog.
    pub fn admit(
        &self,
        expected_root: Option<&str>,
        manager: &str,
        selection: AdmissionSelection,
        operation_id: &str,
    ) -> Result<CatalogRoot> {
        self.admit_many(expected_root, manager, &[selection], operation_id)
    }

    /// Atomically update multiple seats in one complete catalog root.
    pub fn admit_many(
        &self,
        expected_root: Option<&str>,
        manager: &str,
        selections: &[AdmissionSelection],
        operation_id: &str,
    ) -> Result<CatalogRoot> {
        self.admit_many_with_hook(expected_root, manager, selections, operation_id, |_| Ok(()))
    }

    fn admit_many_with_hook<F>(
        &self,
        expected_root: Option<&str>,
        manager: &str,
        selections: &[AdmissionSelection],
        operation_id: &str,
        mut hook: F,
    ) -> Result<CatalogRoot>
    where
        F: FnMut(RootPublishPoint) -> Result<()>,
    {
        validate_component("manager", manager)?;
        validate_operation(operation_id)?;
        validate_optional_digest(expected_root)?;
        if selections.is_empty() {
            bail!("at least one seat selection is required");
        }
        let mut updates = BTreeMap::new();
        for selection in selections {
            let admission = self.prepare_admission(selection, manager)?;
            let bus_id = format!("{}.{}", admission.value.host, admission.value.identity);
            if updates
                .insert(bus_id.clone(), admission.digest.clone())
                .is_some()
            {
                bail!("duplicate seat '{bus_id}' in publication");
            }
            let bytes = encoded(&admission.value)?;
            publish_immutable(&self.admission_path(&admission.digest)?, &bytes)?;
        }
        hook(RootPublishPoint::AdmissionsPersisted)?;

        let _guard = FileLock::acquire(&self.root_lock_path())?;
        let current = self.read_root()?;
        if let Some(root) = &current
            && root.value.operation_id == operation_id
            && root.value.manager == manager
        {
            if root.value.parent.as_deref() == expected_root && root.value.updates == updates {
                return Ok(root.clone());
            }
            bail!("operation id replay does not match the original root request");
        }
        if current.as_ref().map(|root| root.commit.as_str()) != expected_root {
            bail!(
                "catalog root CAS conflict: expected {:?}, found {:?}",
                expected_root,
                current.as_ref().map(|root| root.commit.as_str())
            );
        }
        let mut admissions = current
            .as_ref()
            .map(|root| root.value.admissions.clone())
            .unwrap_or_default();
        admissions.extend(updates.clone());
        let value = CatalogRootCommit {
            schema: SCHEMA,
            parent: current.as_ref().map(|root| root.commit.clone()),
            manager: manager.to_string(),
            admissions,
            updates,
            operation_id: operation_id.to_string(),
        };

        // The complete prospective graph is validated before it becomes visible.
        self.resolve_root_value("<prospective>", &value)?;
        let bytes = encoded(&value)?;
        let commit = digest("st2-catalog-root-commit-v1", &bytes);
        publish_immutable(&self.root_commit_path(&commit)?, &bytes)?;
        hook(RootPublishPoint::RootPersisted)?;
        write_atomic(&self.root_head_path(), format!("{commit}\n").as_bytes())?;
        hook(RootPublishPoint::HeadPublished)?;
        Ok(CatalogRoot { commit, value })
    }

    pub fn read_root(&self) -> Result<Option<CatalogRoot>> {
        let Some(commit) = read_head_value(&self.root_head_path())? else {
            return Ok(None);
        };
        let bytes = fs::read(self.root_commit_path(&commit)?)
            .with_context(|| format!("catalog root references missing commit '{commit}'"))?;
        let value: CatalogRootCommit = serde_json::from_slice(&bytes)?;
        verify_digest("st2-catalog-root-commit-v1", &commit, &value)?;
        validate_optional_digest(value.parent.as_deref())?;
        for value in value.admissions.values().chain(value.updates.values()) {
            validate_digest(value)?;
        }
        Ok(Some(CatalogRoot { commit, value }))
    }

    pub fn resolve_root(&self) -> Result<Vec<ResolvedSeat>> {
        Ok(self
            .inspect_snapshot()?
            .map(|(_, seats)| seats)
            .unwrap_or_default())
    }

    /// Capture one selected root and resolve exactly that immutable snapshot.
    pub fn inspect_snapshot(&self) -> Result<Option<(CatalogRoot, Vec<ResolvedSeat>)>> {
        self.inspect_snapshot_with_hook(|| Ok(()))
    }

    fn inspect_snapshot_with_hook<F>(
        &self,
        hook: F,
    ) -> Result<Option<(CatalogRoot, Vec<ResolvedSeat>)>>
    where
        F: FnOnce() -> Result<()>,
    {
        let Some(root) = self.read_root()? else {
            return Ok(None);
        };
        hook()?;
        let seats = self.resolve_root_value(&root.commit, &root.value)?;
        Ok(Some((root, seats)))
    }

    fn resolve_root_value(
        &self,
        root_commit: &str,
        root: &CatalogRootCommit,
    ) -> Result<Vec<ResolvedSeat>> {
        if root.schema != SCHEMA {
            bail!("unsupported catalog root schema {}", root.schema);
        }
        let mut resolved = Vec::with_capacity(root.admissions.len());
        let mut state_paths = BTreeMap::<PathBuf, String>::new();
        for (bus_id, admission_digest) in &root.admissions {
            let admission = self.read_admission(admission_digest)?;
            let expected_bus_id = format!("{}.{}", admission.value.host, admission.value.identity);
            if *bus_id != expected_bus_id {
                bail!(
                    "catalog root key '{bus_id}' does not match admission seat '{expected_bus_id}'"
                );
            }
            let spec_ref = self.read_ref_commit(&admission.value.ref_commit)?;
            let binding = self.read_resource_binding(&admission.value.resource_binding_commit)?;
            validate_join(&admission.value, &spec_ref.value, &binding.value)?;
            validate_state_path_custody(&self.catalog_root, &binding.value.state_relative)?;
            if let Some(other) =
                state_paths.insert(binding.value.state_relative.clone(), bus_id.clone())
            {
                bail!(
                    "seats '{other}' and '{bus_id}' share resource state path '{}'",
                    binding.value.state_relative.display()
                );
            }
            let object = spec_ref
                .value
                .target_object
                .as_deref()
                .context("admission references a tombstoned ref commit")?;
            let object_path = self.object_path(object)?;
            let bytes = fs::read(&object_path)
                .with_context(|| format!("admission references missing object '{object}'"))?;
            if digest("st2-agent-spec-object-v1", &bytes) != object {
                bail!("Agent Spec object digest mismatch for '{object}'");
            }
            let text = std::str::from_utf8(&bytes).context("Agent Spec object is not UTF-8")?;
            let mut raws = crate::kdl_format::parse_kdl(text)?;
            if raws.len() != 1 {
                bail!("admitted Agent Spec must contain exactly one declaration");
            }
            let raw = raws.pop().expect("length checked");
            if raw.identity.as_deref() != Some(&admission.value.identity)
                || raw.host.as_deref() != Some(&admission.value.host)
            {
                bail!("Agent Spec content does not match admission seat '{bus_id}'");
            }
            let mut spec = raw.into_agent_spec(
                admission.value.identity.clone(),
                Some(admission.value.host.clone()),
                object_path,
            );
            if !spec.is_runnable() && !spec.retired {
                bail!("active admitted Agent Spec '{bus_id}' is not runnable");
            }
            spec.agent_dir = self.catalog_root.join(&binding.value.state_relative);
            resolved.push(ResolvedSeat {
                root_commit: root_commit.to_string(),
                admission: admission.digest,
                ref_commit: spec_ref.commit,
                resource_binding_commit: binding.commit,
                spec_object: object.to_string(),
                spec,
            });
        }
        Ok(resolved)
    }

    fn prepare_admission(
        &self,
        selection: &AdmissionSelection,
        manager: &str,
    ) -> Result<AdmissionObject> {
        let spec_ref = self.read_ref_commit(&selection.ref_commit)?;
        let binding = self.read_resource_binding(&selection.resource_binding_commit)?;
        if spec_ref.value.manager != manager || binding.value.manager != manager {
            bail!(
                "manager '{manager}' cannot admit seat commits owned by '{}'",
                spec_ref.value.manager
            );
        }
        let value = SeatAdmission {
            schema: SCHEMA,
            host: spec_ref.value.host.clone(),
            identity: spec_ref.value.identity.clone(),
            ref_commit: selection.ref_commit.clone(),
            resource_binding_commit: selection.resource_binding_commit.clone(),
        };
        validate_join(&value, &spec_ref.value, &binding.value)?;
        if spec_ref.value.target_object.is_none() {
            bail!("a tombstoned ref commit cannot be admitted");
        }
        let bytes = encoded(&value)?;
        Ok(AdmissionObject {
            digest: digest("st2-seat-admission-v1", &bytes),
            value,
        })
    }

    fn read_ref_commit(&self, commit: &str) -> Result<RefHead> {
        let bytes = fs::read(self.ref_commit_path(commit)?)
            .with_context(|| format!("missing ref commit '{commit}'"))?;
        let value: RefCommit = serde_json::from_slice(&bytes)?;
        verify_digest("st2-agent-spec-ref-commit-v1", commit, &value)?;
        validate_optional_digest(value.parent.as_deref())?;
        validate_optional_digest(value.target_object.as_deref())?;
        Ok(RefHead {
            commit: commit.to_string(),
            value,
        })
    }

    fn read_resource_binding(&self, commit: &str) -> Result<ResourceBinding> {
        let bytes = fs::read(self.binding_path(commit)?)
            .with_context(|| format!("missing resource binding commit '{commit}'"))?;
        let value: ResourceBindingCommit = serde_json::from_slice(&bytes)?;
        verify_digest("st2-resource-binding-commit-v1", commit, &value)?;
        validate_optional_digest(value.parent.as_deref())?;
        validate_relative_state_path(&value.state_relative)?;
        Ok(ResourceBinding {
            commit: commit.to_string(),
            value,
        })
    }

    fn read_admission(&self, digest_value: &str) -> Result<AdmissionObject> {
        let bytes = fs::read(self.admission_path(digest_value)?)
            .with_context(|| format!("missing SeatAdmission '{digest_value}'"))?;
        let value: SeatAdmission = serde_json::from_slice(&bytes)?;
        verify_digest("st2-seat-admission-v1", digest_value, &value)?;
        validate_digest(&value.ref_commit)?;
        validate_digest(&value.resource_binding_commit)?;
        Ok(AdmissionObject {
            digest: digest_value.to_string(),
            value,
        })
    }

    fn object_path(&self, value: &str) -> Result<PathBuf> {
        validate_digest(value)?;
        Ok(self
            .store_root
            .join("objects")
            .join(format!("{value}.agent.kdl")))
    }

    fn ref_commit_path(&self, value: &str) -> Result<PathBuf> {
        validate_digest(value)?;
        Ok(self
            .store_root
            .join("ref-commits")
            .join(format!("{value}.json")))
    }

    fn ref_head_path(&self, host: &str, identity: &str) -> PathBuf {
        self.store_root
            .join("refs")
            .join(host)
            .join(identity)
            .join("head")
    }

    fn ref_lock_path(&self, host: &str, identity: &str) -> PathBuf {
        self.store_root
            .join("locks")
            .join("refs")
            .join(host)
            .join(format!("{identity}.lock"))
    }

    fn binding_path(&self, value: &str) -> Result<PathBuf> {
        validate_digest(value)?;
        Ok(self
            .store_root
            .join("resource-bindings")
            .join(format!("{value}.json")))
    }

    fn admission_path(&self, value: &str) -> Result<PathBuf> {
        validate_digest(value)?;
        Ok(self
            .store_root
            .join("admissions")
            .join(format!("{value}.json")))
    }

    fn root_commit_path(&self, value: &str) -> Result<PathBuf> {
        validate_digest(value)?;
        Ok(self
            .store_root
            .join("root-commits")
            .join(format!("{value}.json")))
    }

    fn root_head_path(&self) -> PathBuf {
        self.store_root.join("root")
    }

    fn root_lock_path(&self) -> PathBuf {
        self.store_root.join("locks").join("root.lock")
    }
}

fn validate_join(
    admission: &SeatAdmission,
    spec_ref: &RefCommit,
    binding: &ResourceBindingCommit,
) -> Result<()> {
    if admission.schema != SCHEMA || spec_ref.schema != SCHEMA || binding.schema != SCHEMA {
        bail!("unsupported catalog object schema");
    }
    if spec_ref.host != admission.host
        || spec_ref.identity != admission.identity
        || binding.host != admission.host
        || binding.identity != admission.identity
    {
        bail!("SeatAdmission contains a cross-seat join");
    }
    if spec_ref.manager != binding.manager {
        bail!("SeatAdmission joins commits owned by different managers");
    }
    Ok(())
}

fn validate_component(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        bail!("invalid {label} '{value}'");
    }
    Ok(())
}

fn validate_operation(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("operation id must not be empty");
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256-") else {
        bail!("invalid digest '{value}': expected sha256-<64 lowercase hex>");
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("invalid digest '{value}': expected sha256-<64 lowercase hex>");
    }
    Ok(())
}

fn validate_optional_digest(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_digest(value)?;
    }
    Ok(())
}

fn validate_relative_state_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("resource state path must be a non-empty relative path");
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!(
            "resource state path '{}' must contain only normal relative components",
            path.display()
        );
    }
    if path
        .components()
        .next()
        .is_some_and(|component| component.as_os_str() == ".st2")
    {
        bail!("resource state path must not be under reserved .st2");
    }
    Ok(())
}

fn validate_state_path_custody(catalog_root: &Path, relative: &Path) -> Result<()> {
    validate_relative_state_path(relative)?;
    let mut current = catalog_root.to_path_buf();
    for component in std::iter::once(None).chain(
        relative
            .components()
            .map(|component| Some(component.as_os_str())),
    ) {
        if let Some(component) = component {
            current.push(component);
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "resource state path crosses symlink component {}",
                    current.display()
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspecting state path {}", current.display()));
            }
        }
    }
    Ok(())
}

fn digest(domain: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    format!("sha256-{:x}", hasher.finalize())
}

fn encoded<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn verify_digest<T: Serialize>(domain: &str, expected: &str, value: &T) -> Result<()> {
    if digest(domain, &encoded(value)?) != expected {
        bail!("digest mismatch for '{expected}'");
    }
    Ok(())
}

fn read_head_value(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(raw) => {
            let value = raw.trim();
            if value.is_empty() {
                bail!("empty catalog head at {}", path.display());
            }
            validate_digest(value)?;
            Ok(Some(value.to_string()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn temp_path(target: &Path) -> PathBuf {
    let serial = TEMP_SERIAL.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("value");
    target.with_file_name(format!(".{name}.tmp-{}-{serial}", std::process::id()))
}

fn publish_immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        return if fs::read(path)? == bytes {
            Ok(())
        } else {
            bail!("refusing to replace immutable artifact {}", path.display())
        };
    }
    let tmp = temp_path(path);
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::hard_link(&tmp, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(path)? != bytes {
                    bail!("immutable publication collision at {}", path.display());
                }
            }
            Err(error) => return Err(error.into()),
        }
        sync_parent(path)?;
        Ok(())
    })();
    let _ = fs::remove_file(&tmp);
    result
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("atomic target has no parent")?;
    fs::create_dir_all(parent)?;
    let tmp = temp_path(path);
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        sync_parent(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

struct FileLock(File);

impl FileLock {
    fn acquire(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self(file))
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootPublishPoint {
    AdmissionsPersisted,
    RootPersisted,
    HeadPublished,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    fn spec(identity: &str, command: &str) -> Vec<u8> {
        format!("agent \"{identity}\" {{\n  host \"h\"\n  command \"{command}\"\n}}\n").into_bytes()
    }

    fn publish_seat(
        store: &CatalogStore,
        identity: &str,
        command: &str,
        operation: &str,
    ) -> (RefHead, ResourceBinding) {
        publish_seat_as(store, identity, command, "eval", operation)
    }

    fn publish_seat_as(
        store: &CatalogStore,
        identity: &str,
        command: &str,
        manager: &str,
        operation: &str,
    ) -> (RefHead, ResourceBinding) {
        let prepared = store.prepare(&spec(identity, command)).unwrap();
        store.import(&prepared).unwrap();
        let spec_ref = store
            .compare_and_set_ref(
                "h",
                identity,
                None,
                manager,
                Some(&prepared.object),
                &format!("{operation}:ref"),
            )
            .unwrap();
        let binding = store
            .commit_resource_binding(
                None,
                "h",
                identity,
                manager,
                Path::new(&format!("agents/h/{identity}")),
                &format!("{operation}:binding"),
            )
            .unwrap();
        (spec_ref, binding)
    }

    #[test]
    fn exact_bytes_resolve_with_stable_mutable_agent_state_without_projection() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let bytes = format!(
            "agent \"worker\" {{\n  host \"h\"\n  workspace \"{}\"\n  command \"sleep 10\"\n  render {{ file \".st2/proof\" \"ok\" }}\n}}\n",
            workspace.display()
        )
        .into_bytes();
        let store = CatalogStore::new(tmp.path());
        let prepared = store.prepare(&bytes).unwrap();
        let object_path = store.import(&prepared).unwrap();
        let spec_ref = store
            .compare_and_set_ref(
                "h",
                "worker",
                None,
                "dynamic",
                Some(&prepared.object),
                "op:ref",
            )
            .unwrap();
        let binding = store
            .commit_resource_binding(
                None,
                "h",
                "worker",
                "dynamic",
                Path::new("agents/h/worker"),
                "op:binding",
            )
            .unwrap();
        store
            .admit(
                None,
                "dynamic",
                AdmissionSelection {
                    ref_commit: spec_ref.commit,
                    resource_binding_commit: binding.commit,
                },
                "op:root",
            )
            .unwrap();

        let seats = store.resolve_root().unwrap();
        assert_eq!(seats.len(), 1);
        let seat = &seats[0];
        assert_eq!(seat.spec.path, object_path);
        assert_eq!(seat.spec.agent_dir, tmp.path().join("agents/h/worker"));
        assert!(!seat.spec.agent_dir.join("agent.kdl").exists());

        let inbox = crate::message::inbox_dir(&seat.spec.agent_dir);
        crate::message::send_to_inbox(&inbox, "h.sender", None, None, &[], "hello").unwrap();
        assert_eq!(
            crate::message::list_inbox(&inbox).unwrap()[0].body,
            "hello\n"
        );
        let context = crate::context::context_dir(&seat.spec.agent_dir);
        crate::context::write_now(&context, "working").unwrap();
        assert_eq!(
            crate::context::read(&context, crate::context::View::Now),
            "working"
        );
        let status = crate::status::status_path(&seat.spec.agent_dir);
        crate::status::set_state(&status, crate::status::State::Available).unwrap();
        assert_eq!(
            crate::status::read_state(&status),
            crate::status::State::Available
        );
        let before = fs::read(&object_path).unwrap();
        crate::materialize::materialize_agent(tmp.path(), &seat.spec, "h").unwrap();
        assert_eq!(
            fs::read_to_string(workspace.join(".st2/proof")).unwrap(),
            "ok"
        );
        assert_eq!(fs::read(&object_path).unwrap(), before);
    }

    #[test]
    fn multi_seat_root_is_atomic_and_validates_the_complete_graph() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        let (a_ref, a_binding) = publish_seat(&store, "a", "sleep 1", "a");
        let (b_ref, b_binding) = publish_seat(&store, "b", "sleep 2", "b");
        let root = store
            .admit_many(
                None,
                "eval",
                &[
                    AdmissionSelection {
                        ref_commit: a_ref.commit,
                        resource_binding_commit: a_binding.commit,
                    },
                    AdmissionSelection {
                        ref_commit: b_ref.commit,
                        resource_binding_commit: b_binding.commit,
                    },
                ],
                "root-ab",
            )
            .unwrap();
        assert_eq!(root.value.admissions.len(), 2);
        let seats = store.resolve_root().unwrap();
        assert_eq!(
            seats
                .iter()
                .map(|seat| seat.spec.identity.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );

        // A cross-seat ref/binding pair fails before root visibility changes.
        assert!(
            store
                .admit(
                    Some(&root.commit),
                    "eval",
                    AdmissionSelection {
                        ref_commit: seats[0].ref_commit.clone(),
                        resource_binding_commit: seats[1].resource_binding_commit.clone(),
                    },
                    "invalid",
                )
                .is_err()
        );
        assert_eq!(store.read_root().unwrap().unwrap(), root);
    }

    #[test]
    fn mixed_managers_update_only_owned_seats_and_preserve_foreign_admissions() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        let (nix_ref, nix_binding) = publish_seat_as(&store, "static", "sleep 1", "nix", "nix");
        let nix_selection = AdmissionSelection {
            ref_commit: nix_ref.commit,
            resource_binding_commit: nix_binding.commit,
        };
        let nix_root = store
            .admit(None, "nix", nix_selection.clone(), "root-nix")
            .unwrap();
        let static_admission = nix_root.value.admissions["h.static"].clone();

        let (dynamic_ref, dynamic_binding) =
            publish_seat_as(&store, "dynamic", "sleep 2", "dynamic", "dynamic");
        let mixed_root = store
            .admit(
                Some(&nix_root.commit),
                "dynamic",
                AdmissionSelection {
                    ref_commit: dynamic_ref.commit,
                    resource_binding_commit: dynamic_binding.commit,
                },
                "root-dynamic",
            )
            .unwrap();
        assert_eq!(mixed_root.value.manager, "dynamic");
        assert_eq!(
            mixed_root.value.admissions["h.static"], static_admission,
            "untouched Nix admission must be preserved byte-identically"
        );
        assert!(mixed_root.value.admissions.contains_key("h.dynamic"));
        assert!(
            store
                .admit(
                    Some(&mixed_root.commit),
                    "dynamic",
                    nix_selection,
                    "foreign-seat",
                )
                .is_err()
        );
        assert_eq!(store.read_root().unwrap().unwrap(), mixed_root);
    }

    #[test]
    fn root_head_is_the_atomic_visibility_boundary_and_response_loss_replays() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        let (a_ref, binding) = publish_seat(&store, "worker", "sleep 1", "a");
        let root_a = store
            .admit(
                None,
                "eval",
                AdmissionSelection {
                    ref_commit: a_ref.commit.clone(),
                    resource_binding_commit: binding.commit.clone(),
                },
                "root-a",
            )
            .unwrap();
        let b = store.prepare(&spec("worker", "sleep 2")).unwrap();
        store.import(&b).unwrap();
        let b_ref = store
            .compare_and_set_ref(
                "h",
                "worker",
                Some(&a_ref.commit),
                "eval",
                Some(&b.object),
                "b:ref",
            )
            .unwrap();
        let selection = AdmissionSelection {
            ref_commit: b_ref.commit,
            resource_binding_commit: binding.commit,
        };

        let before_head = store.admit_many_with_hook(
            Some(&root_a.commit),
            "eval",
            std::slice::from_ref(&selection),
            "root-b-before",
            |point| {
                if point == RootPublishPoint::RootPersisted {
                    bail!("simulated crash");
                }
                Ok(())
            },
        );
        assert!(before_head.is_err());
        assert_eq!(store.read_root().unwrap().unwrap(), root_a);

        let after_head = store.admit_many_with_hook(
            Some(&root_a.commit),
            "eval",
            std::slice::from_ref(&selection),
            "root-b",
            |point| {
                if point == RootPublishPoint::HeadPublished {
                    bail!("simulated response loss");
                }
                Ok(())
            },
        );
        assert!(after_head.is_err());
        let visible = store.read_root().unwrap().unwrap();
        let replayed = store
            .admit_many(
                Some(&root_a.commit),
                "eval",
                &[selection.clone()],
                "root-b",
            )
            .unwrap();
        assert_eq!(replayed, visible);
        assert!(
            store
                .admit_many(Some(&visible.commit), "eval", &[selection], "root-b",)
                .is_err(),
            "same operation id with a different expected parent is not a replay"
        );
    }

    #[test]
    fn ref_cas_has_one_winner_and_manager_fencing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        let a = store.prepare(&spec("worker", "sleep 1")).unwrap();
        let b = store.prepare(&spec("worker", "sleep 2")).unwrap();
        store.import(&a).unwrap();
        store.import(&b).unwrap();
        let first = store
            .compare_and_set_ref("h", "worker", None, "nix", Some(&a.object), "first")
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let attempts = [a.object, b.object]
            .into_iter()
            .enumerate()
            .map(|(index, object)| {
                let barrier = Arc::clone(&barrier);
                let root = tmp.path().to_path_buf();
                let expected = first.commit.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    CatalogStore::new(root).compare_and_set_ref(
                        "h",
                        "worker",
                        Some(&expected),
                        "nix",
                        Some(&object),
                        &format!("race-{index}"),
                    )
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = attempts
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let current = store.read_ref_head("h", "worker").unwrap().unwrap();
        assert!(
            store
                .compare_and_set_ref(
                    "h",
                    "worker",
                    Some(&current.commit),
                    "nix",
                    current.value.target_object.as_deref(),
                    &current.value.operation_id,
                )
                .is_err(),
            "same operation id with a different expected parent is not a replay"
        );
        assert!(
            store
                .compare_and_set_ref("h", "worker", Some(&current.commit), "other", None, "steal",)
                .is_err()
        );
    }

    #[test]
    fn digest_grammar_is_checked_before_any_digest_derived_path() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        for invalid in [
            "../escape",
            "sha256-../../escape",
            "sha256-ABCDEF",
            "sha256-abc",
            "md5-0000000000000000000000000000000000000000000000000000000000000000",
        ] {
            assert!(store.object_path(invalid).is_err());
            assert!(store.ref_commit_path(invalid).is_err());
            assert!(store.binding_path(invalid).is_err());
            assert!(store.admission_path(invalid).is_err());
            assert!(store.root_commit_path(invalid).is_err());
        }
        assert!(
            store
                .compare_and_set_ref(
                    "h",
                    "worker",
                    None,
                    "eval",
                    Some("../../escape"),
                    "bad-target",
                )
                .is_err()
        );
        assert!(
            store
                .commit_resource_binding(
                    Some("../../escape"),
                    "h",
                    "worker",
                    "eval",
                    Path::new("agents/h/worker"),
                    "bad-parent",
                )
                .is_err()
        );
        write_atomic(&store.root_head_path(), b"../../escape\n").unwrap();
        assert!(store.read_root().is_err());
        assert!(!tmp.path().join("escape").exists());
    }

    #[test]
    fn state_roots_are_unique_non_reserved_and_do_not_cross_symlinks() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        assert!(
            store
                .commit_resource_binding(
                    None,
                    "h",
                    "reserved",
                    "eval",
                    Path::new(".st2/agent-state"),
                    "reserved",
                )
                .is_err()
        );
        fs::create_dir_all(tmp.path().join("real-agents")).unwrap();
        symlink(
            tmp.path().join("real-agents"),
            tmp.path().join("linked-agents"),
        )
        .unwrap();
        assert!(
            store
                .commit_resource_binding(
                    None,
                    "h",
                    "linked",
                    "eval",
                    Path::new("linked-agents/h/linked"),
                    "linked",
                )
                .is_err()
        );

        let (a_ref, _) = publish_seat(&store, "a", "sleep 1", "a");
        let (b_ref, _) = publish_seat(&store, "b", "sleep 2", "b");
        let a_binding = store
            .commit_resource_binding(
                None,
                "h",
                "a",
                "eval",
                Path::new("shared-state"),
                "a-shared",
            )
            .unwrap();
        let b_binding = store
            .commit_resource_binding(
                None,
                "h",
                "b",
                "eval",
                Path::new("shared-state"),
                "b-shared",
            )
            .unwrap();
        assert!(
            store
                .admit_many(
                    None,
                    "eval",
                    &[
                        AdmissionSelection {
                            ref_commit: a_ref.commit,
                            resource_binding_commit: a_binding.commit,
                        },
                        AdmissionSelection {
                            ref_commit: b_ref.commit,
                            resource_binding_commit: b_binding.commit,
                        },
                    ],
                    "duplicate-state",
                )
                .is_err()
        );
        assert!(store.read_root().unwrap().is_none());
    }

    #[test]
    fn inspect_resolves_the_single_root_snapshot_it_captured() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CatalogStore::new(tmp.path());
        let (a_ref, binding) = publish_seat(&store, "worker", "sleep 1", "a");
        let root_a = store
            .admit(
                None,
                "eval",
                AdmissionSelection {
                    ref_commit: a_ref.commit.clone(),
                    resource_binding_commit: binding.commit.clone(),
                },
                "root-a",
            )
            .unwrap();
        let b = store.prepare(&spec("worker", "sleep 2")).unwrap();
        store.import(&b).unwrap();
        let b_ref = store
            .compare_and_set_ref(
                "h",
                "worker",
                Some(&a_ref.commit),
                "eval",
                Some(&b.object),
                "b-ref",
            )
            .unwrap();
        let selection = AdmissionSelection {
            ref_commit: b_ref.commit,
            resource_binding_commit: binding.commit,
        };

        let (captured, seats) = store
            .inspect_snapshot_with_hook(|| {
                store.admit(Some(&root_a.commit), "eval", selection, "root-b")?;
                Ok(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(captured, root_a);
        assert_eq!(seats[0].root_commit, root_a.commit);
        assert!(
            seats[0]
                .spec
                .tasks
                .iter()
                .any(|task| task.command.as_deref() == Some("sleep 1"))
        );
        assert_ne!(store.read_root().unwrap().unwrap().commit, captured.commit);
    }
}
