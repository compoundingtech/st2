//! Explicit, receipt-bearing installation of the lifecycle hooks shipped with st2.
//!
//! Hook scripts are published as immutable content-addressed sets. Ordinary supervision resolves
//! and verifies the set embedded in its own binary, but never writes it. Only `st2 hooks install`
//! publishes a set and atomically selects it with `current.json`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CODEX_SESSION_START: &[u8] = include_bytes!("../hooks/codex-session-start.sh");
const CODEX_PRE_COMPACT: &[u8] = include_bytes!("../hooks/codex-pre-compact.sh");
const CODEX_STOP: &[u8] = include_bytes!("../hooks/codex-stop.sh");
const CLAUDE_SESSION_START: &[u8] = include_bytes!("../hooks/claude-session-start.sh");
const CLAUDE_PRE_COMPACT: &[u8] = include_bytes!("../hooks/claude-pre-compact.sh");
const CLAUDE_STOP_FAILURE: &[u8] = include_bytes!("../hooks/claude-stop-failure.sh");

const SCHEMA: u32 = 1;
const RECEIPT_FILE: &str = "current.json";
const SET_MANIFEST_FILE: &str = "manifest.json";
const SETS_DIR: &str = "sets";
const HOOKS: [(&str, &[u8]); 6] = [
    ("codex-session-start.sh", CODEX_SESSION_START),
    ("codex-pre-compact.sh", CODEX_PRE_COMPACT),
    ("codex-stop.sh", CODEX_STOP),
    ("claude-session-start.sh", CLAUDE_SESSION_START),
    ("claude-pre-compact.sh", CLAUDE_PRE_COMPACT),
    ("claude-stop-failure.sh", CLAUDE_STOP_FAILURE),
];
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HookManifest {
    schema: u32,
    hookset: String,
    directory: String,
    files: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstallReceipt {
    #[serde(flatten)]
    manifest: HookManifest,
    st2_version: String,
    st2_git_sha: String,
    source_commit_unix: u64,
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

fn expected_manifest() -> HookManifest {
    let files = HOOKS
        .iter()
        .map(|(name, bytes)| ((*name).to_string(), format!("sha256:{}", sha256(bytes))))
        .collect::<BTreeMap<_, _>>();
    let encoded = serde_json::to_vec(&files).expect("the built-in hook manifest is serializable");
    let hookset = format!("sha256-{}", sha256(&encoded));
    HookManifest {
        schema: SCHEMA,
        directory: format!("{SETS_DIR}/{hookset}"),
        hookset,
        files,
    }
}

fn expected_receipt() -> InstallReceipt {
    InstallReceipt {
        manifest: expected_manifest(),
        st2_version: env!("CARGO_PKG_VERSION").to_string(),
        st2_git_sha: env!("ST2_GIT_SHA_FULL").to_string(),
        source_commit_unix: env!("ST2_GIT_COMMIT_UNIX").parse().unwrap_or(0),
    }
}

/// Stable content identity of this binary's embedded hook set.
pub fn hookset_id() -> String {
    expected_manifest().hookset
}

/// Install-owned hook root. `$ST_HOOKS` can pin a scratch or custom state layout; otherwise use
/// `$XDG_STATE_HOME/st2/hooks` or `~/.local/state/st2/hooks`.
pub fn hooks_root() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ST_HOOKS").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    let state = match std::env::var_os("XDG_STATE_HOME").filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(
            std::env::var_os("HOME").context("HOME is not set and XDG_STATE_HOME is unset")?,
        )
        .join(".local/state"),
    };
    Ok(state.join("st2/hooks"))
}

/// The immutable, versioned directory this binary expects rendered hook settings to use.
/// Resolving the path never creates or changes anything.
pub fn versioned_hooks_dir() -> Result<PathBuf> {
    Ok(versioned_hooks_dir_at(&hooks_root()?))
}

/// The immutable hook-set directory beneath an explicit root.
pub fn versioned_hooks_dir_at(root: &Path) -> PathBuf {
    root.join(expected_manifest().directory)
}

/// Whether this host has a Codex agent whose rendered settings and next launch need this hook set.
pub fn required_by_codex(specs: &[crate::spec::AgentSpec], this_host: &str) -> bool {
    specs
        .iter()
        .any(|spec| required_by_codex_agent(spec, this_host))
}

/// Whether one local declaration owns a Codex agent task.
pub fn required_by_codex_agent(spec: &crate::spec::AgentSpec, this_host: &str) -> bool {
    spec.host.as_deref().is_none_or(|host| host == this_host)
        && spec.tasks.iter().any(|task| {
            task.name == "agent"
                && task
                    .command
                    .as_deref()
                    .is_some_and(crate::shepherd::command_invokes_codex)
        })
}

fn receipt_path(root: &Path) -> PathBuf {
    root.join(RECEIPT_FILE)
}

fn read_receipt(root: &Path) -> Result<InstallReceipt> {
    let path = receipt_path(root);
    let bytes =
        fs::read(&path).with_context(|| format!("reading hook receipt {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing hook receipt {}", path.display()))
}

fn manifest_bytes(manifest: &HookManifest) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(manifest).context("serializing hook manifest")?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn receipt_bytes(receipt: &InstallReceipt) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(receipt).context("serializing hook receipt")?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn verify_set(root: &Path, manifest: &HookManifest) -> Result<PathBuf> {
    let expected = expected_manifest();
    if manifest != &expected {
        anyhow::bail!(
            "active hook receipt selects '{}', but this st2 requires '{}'",
            manifest.hookset,
            expected.hookset
        );
    }
    let dir = root.join(&manifest.directory);
    let manifest_path = dir.join(SET_MANIFEST_FILE);
    let installed_manifest = fs::read(&manifest_path)
        .with_context(|| format!("reading hook-set manifest {}", manifest_path.display()))?;
    if installed_manifest != manifest_bytes(manifest)? {
        anyhow::bail!("hook-set manifest mismatch at {}", manifest_path.display());
    }
    for (name, expected_bytes) in HOOKS {
        let path = dir.join(name);
        let actual = fs::read(&path)
            .with_context(|| format!("reading installed hook {}", path.display()))?;
        if actual != expected_bytes {
            anyhow::bail!("installed hook content mismatch at {}", path.display());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if fs::metadata(&path)?.permissions().mode() & 0o111 == 0 {
                anyhow::bail!("installed hook is not executable: {}", path.display());
            }
        }
    }
    Ok(dir)
}

/// Verify that the atomic receipt selects this binary's complete, byte-exact hook set.
/// This function is read-only.
pub fn verify_installed() -> Result<PathBuf> {
    verify_installed_at(&hooks_root()?)
}

/// Read-only verification beneath an explicit hook root.
pub fn verify_installed_at(root: &Path) -> Result<PathBuf> {
    let receipt = read_receipt(root)?;
    verify_set(root, &receipt.manifest)
}

fn numeric_version(version: &str) -> Option<Vec<u64>> {
    let core = version.split_once('-').map_or(version, |(core, _)| core);
    core.split('.').map(|part| part.parse().ok()).collect()
}

fn compare_versions(left: &str, right: &str) -> Option<Ordering> {
    let mut left = numeric_version(left)?;
    let mut right = numeric_version(right)?;
    let width = left.len().max(right.len());
    left.resize(width, 0);
    right.resize(width, 0);
    Some(left.cmp(&right))
}

fn check_replacement(
    current: &InstallReceipt,
    candidate: &InstallReceipt,
    allow_downgrade: bool,
) -> Result<()> {
    if current.manifest.hookset == candidate.manifest.hookset {
        return Ok(());
    }
    let source_order =
        compare_versions(&current.st2_version, &candidate.st2_version).map(|version_order| {
            version_order.then_with(|| {
                current
                    .source_commit_unix
                    .cmp(&candidate.source_commit_unix)
            })
        });
    if allow_downgrade || source_order == Some(Ordering::Less) {
        return Ok(());
    }
    let reason = match source_order {
        Some(Ordering::Greater) => "the installed receipt is newer",
        Some(Ordering::Equal) => "the two different hook sets have the same source order",
        Some(Ordering::Less) => unreachable!(),
        None => "the installed receipt's version cannot be ordered",
    };
    anyhow::bail!(
        "refusing to replace hook set '{}' with '{}' because {reason}; \
         rerun with --allow-downgrade only for an intentional rollback",
        current.manifest.hookset,
        candidate.manifest.hookset
    )
}

fn temp_path(parent: &Path, label: &str) -> PathBuf {
    let serial = TEMP_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    parent.join(format!(".{label}.tmp-{}-{serial}", std::process::id()))
}

fn write_set(root: &Path, receipt: &InstallReceipt) -> Result<PathBuf> {
    let sets = root.join(SETS_DIR);
    fs::create_dir_all(&sets).with_context(|| format!("creating {}", sets.display()))?;
    let final_dir = root.join(&receipt.manifest.directory);
    if final_dir.exists() {
        return verify_set(root, &receipt.manifest).with_context(|| {
            format!(
                "refusing to replace corrupt immutable hook set {}",
                final_dir.display()
            )
        });
    }

    let tmp = temp_path(&sets, "hookset");
    fs::create_dir(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let result = (|| -> Result<()> {
        for (name, bytes) in HOOKS {
            let path = tmp.join(name);
            fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
                    .with_context(|| format!("making {} executable", path.display()))?;
            }
        }
        fs::write(
            tmp.join(SET_MANIFEST_FILE),
            manifest_bytes(&receipt.manifest)?,
        )
        .context("writing hook-set manifest")?;
        fs::rename(&tmp, &final_dir)
            .with_context(|| format!("atomically publishing hook set {}", final_dir.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&tmp);
    }
    result?;
    verify_set(root, &receipt.manifest)
}

fn write_receipt(root: &Path, receipt: &InstallReceipt) -> Result<()> {
    let target = receipt_path(root);
    let tmp = temp_path(root, RECEIPT_FILE);
    fs::write(&tmp, receipt_bytes(receipt)?)
        .with_context(|| format!("writing temporary hook receipt {}", tmp.display()))?;
    if let Err(error) = fs::rename(&tmp, &target) {
        let _ = fs::remove_file(&tmp);
        return Err(error)
            .with_context(|| format!("atomically selecting hook set in {}", target.display()));
    }
    Ok(())
}

/// Explicitly publish this binary's immutable hook set and atomically select it.
pub fn install(allow_downgrade: bool) -> Result<PathBuf> {
    install_at(&hooks_root()?, allow_downgrade)
}

/// Explicit installer beneath a provided root. Ordinary `up` and materialization paths never call it.
pub fn install_at(root: &Path, allow_downgrade: bool) -> Result<PathBuf> {
    fs::create_dir_all(root).with_context(|| format!("creating hook root {}", root.display()))?;
    let candidate = expected_receipt();
    match read_receipt(root) {
        Ok(current) => {
            check_replacement(&current, &candidate, allow_downgrade)?;
            if current.manifest.hookset == candidate.manifest.hookset {
                return verify_set(root, &current.manifest);
            }
        }
        Err(error) if receipt_path(root).exists() && !allow_downgrade => {
            return Err(error).context(
                "refusing to replace an unreadable hook receipt without --allow-downgrade",
            );
        }
        Err(_) => {}
    }
    let dir = write_set(root, &candidate)?;
    write_receipt(root, &candidate)?;
    verify_installed_at(root)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_install_is_idempotent_receipted_and_executable() {
        let tmp = tempfile::tempdir().unwrap();
        let first = install_at(tmp.path(), false).unwrap();
        let receipt = fs::read(receipt_path(tmp.path())).unwrap();
        let second = install_at(tmp.path(), false).unwrap();
        assert_eq!(first, second);
        assert_eq!(fs::read(receipt_path(tmp.path())).unwrap(), receipt);
        assert_eq!(verify_installed_at(tmp.path()).unwrap(), first);
        assert!(first.starts_with(tmp.path().join(SETS_DIR)));
        for (name, expected) in HOOKS {
            let path = first.join(name);
            assert_eq!(fs::read(&path).unwrap(), expected);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_ne!(fs::metadata(path).unwrap().permissions().mode() & 0o111, 0);
            }
        }
    }

    #[test]
    fn verification_rejects_missing_stale_partial_and_mismatched_state() {
        let missing = tempfile::tempdir().unwrap();
        assert!(verify_installed_at(missing.path()).is_err());

        let stale = tempfile::tempdir().unwrap();
        install_at(stale.path(), false).unwrap();
        let mut receipt = read_receipt(stale.path()).unwrap();
        receipt.manifest.hookset = "sha256-stale".into();
        fs::write(receipt_path(stale.path()), receipt_bytes(&receipt).unwrap()).unwrap();
        assert!(verify_installed_at(stale.path())
            .unwrap_err()
            .to_string()
            .contains("requires"));

        let partial = tempfile::tempdir().unwrap();
        let partial_dir = install_at(partial.path(), false).unwrap();
        fs::remove_file(partial_dir.join("codex-stop.sh")).unwrap();
        assert!(verify_installed_at(partial.path())
            .unwrap_err()
            .to_string()
            .contains("codex-stop.sh"));

        let mismatch = tempfile::tempdir().unwrap();
        let mismatch_dir = install_at(mismatch.path(), false).unwrap();
        fs::write(mismatch_dir.join("codex-stop.sh"), b"wrong\n").unwrap();
        assert!(verify_installed_at(mismatch.path())
            .unwrap_err()
            .to_string()
            .contains("content mismatch"));
    }

    #[test]
    fn an_older_binary_needs_explicit_downgrade_authority() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path()).unwrap();
        let mut newer = expected_receipt();
        newer.manifest.hookset = "sha256-newer".into();
        newer.manifest.directory = "sets/sha256-newer".into();
        newer.source_commit_unix = newer.source_commit_unix.saturating_add(1);
        fs::write(receipt_path(tmp.path()), receipt_bytes(&newer).unwrap()).unwrap();

        let error = install_at(tmp.path(), false).unwrap_err().to_string();
        assert!(error.contains("--allow-downgrade"), "{error}");
        assert!(install_at(tmp.path(), true).is_ok());
        assert_eq!(
            read_receipt(tmp.path()).unwrap().manifest.hookset,
            hookset_id()
        );
    }

    #[test]
    fn a_partial_unreceipted_set_is_never_selected_or_rewritten() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = versioned_hooks_dir_at(tmp.path());
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("codex-stop.sh"), b"partial\n").unwrap();

        let error = install_at(tmp.path(), false).unwrap_err().to_string();

        assert!(error.contains("corrupt immutable hook set"), "{error}");
        assert!(!receipt_path(tmp.path()).exists());
        assert_eq!(
            fs::read(dir.join("codex-stop.sh")).unwrap(),
            b"partial\n",
            "the explicit installer must not rewrite a partial content-addressed set"
        );
    }
}
