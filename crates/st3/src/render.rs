use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context as _, Result};
use serde_json::Value;
use sha2::Digest as _;

use crate::model::DesiredSubject;
use crate::store::Store;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlannedWrite {
    destination: PathBuf,
    bytes: Vec<u8>,
    mode: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RenderReceipt {
    pub destination: String,
    pub sha256: String,
    pub mode: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenderResult {
    pub warnings: Vec<String>,
    pub receipts: Vec<RenderReceipt>,
}

pub fn apply(store: &Store, desired: &Value, workspace: &Path) -> Result<RenderResult> {
    let Some(render) = children(desired)
        .iter()
        .find(|child| name(child) == Some("render"))
    else {
        return Ok(RenderResult::default());
    };
    anyhow::ensure!(
        workspace.is_dir(),
        "render workspace {} does not exist",
        workspace.display()
    );
    let (writes, warnings) = prepare_render(store, render, workspace)?;
    commit_transaction(&writes)?;
    Ok(RenderResult {
        warnings,
        receipts: writes
            .into_iter()
            .map(|write| RenderReceipt {
                destination: write.destination.to_string_lossy().into_owned(),
                sha256: hex::encode(sha2::Sha256::digest(&write.bytes)),
                mode: write.mode,
            })
            .collect(),
    })
}

fn prepare_render(
    store: &Store,
    render: &Value,
    workspace: &Path,
) -> Result<(Vec<PlannedWrite>, Vec<String>)> {
    let mut writes = Vec::new();
    let mut warnings = Vec::new();
    for operation in children(render) {
        let operation_name = name(operation).context("render operation has no name")?;
        if operation_name == "git-exclude" && !workspace.join(".git/info").is_dir() {
            warnings.push(format!(
                "skip git-exclude because {} has no .git/info directory",
                workspace.display()
            ));
            continue;
        }
        let arguments = arguments(operation);
        let (path, bytes) = match operation_name {
            "copy" => {
                anyhow::ensure!(
                    arguments.len() == 2,
                    "render copy needs source and destination"
                );
                let source = arguments[0]
                    .as_str()
                    .context("render copy source is not text")?;
                let bytes = if source.starts_with("doc/") {
                    let (name, hash) = source
                        .rsplit_once('@')
                        .context("render document source needs @HASH")?;
                    store
                        .get_document(name, hash)?
                        .with_context(|| format!("render document `{source}` is missing"))?
                } else {
                    fs::read(source).with_context(|| format!("read render source {source}"))?
                };
                (
                    arguments[1].as_str().context("destination is not text")?,
                    bytes,
                )
            }
            "file" => {
                anyhow::ensure!(
                    !arguments.is_empty() && arguments.len() <= 2,
                    "render file has invalid arguments"
                );
                let content = arguments
                    .get(1)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| child_string(operation, "content"))
                    .context("render file has no content")?;
                (
                    arguments[0]
                        .as_str()
                        .context("render file destination is not text")?,
                    content.into_bytes(),
                )
            }
            "json-upsert" => {
                anyhow::ensure!(
                    !arguments.is_empty() && arguments.len() <= 2,
                    "json-upsert has invalid arguments"
                );
                let path = arguments[0]
                    .as_str()
                    .context("json-upsert destination is not text")?;
                let destination = destination(workspace, path)?;
                let content = arguments
                    .get(1)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| child_string(operation, "content"))
                    .context("json-upsert has no content")?;
                let patch: Value = serde_json::from_str(&content)
                    .context("json-upsert content is invalid JSON")?;
                anyhow::ensure!(patch.is_object(), "json-upsert content must be an object");
                let mut current = match fs::read_to_string(&destination) {
                    Ok(value) => serde_json::from_str(&value).with_context(|| {
                        format!("parse existing JSON {}", destination.display())
                    })?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Value::Object(Default::default())
                    }
                    Err(error) => return Err(error.into()),
                };
                merge_json(
                    &mut current,
                    patch,
                    property(operation, "arrays").and_then(Value::as_str) == Some("union"),
                );
                let mut bytes = serde_json::to_vec_pretty(&current)?;
                bytes.push(b'\n');
                (path, bytes)
            }
            "ensure-line" | "git-exclude" => {
                let path = if operation_name == "git-exclude" {
                    ".git/info/exclude"
                } else {
                    anyhow::ensure!(
                        arguments.len() == 2,
                        "ensure-line needs destination and line"
                    );
                    arguments[0]
                        .as_str()
                        .context("ensure-line destination is not text")?
                };
                let destination = destination(workspace, path)?;
                let mut current = match fs::read_to_string(&destination) {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                    Err(error) => return Err(error.into()),
                };
                let lines = if operation_name == "git-exclude" {
                    arguments
                } else {
                    &arguments[1..]
                };
                for value in lines {
                    let line = value.as_str().context("render line is not text")?;
                    if !current.lines().any(|existing| existing == line) {
                        if !current.is_empty() && !current.ends_with('\n') {
                            current.push('\n');
                        }
                        current.push_str(line);
                        current.push('\n');
                    }
                }
                (path, current.into_bytes())
            }
            other => anyhow::bail!("unknown render operation `{other}`"),
        };
        let destination = destination(workspace, path)?;
        let mode = if executable(operation) { 0o755 } else { 0o644 };
        ensure_tracked_file_is_unchanged(workspace, &destination, &bytes)?;
        if let Some(existing) = writes
            .iter()
            .find(|write: &&PlannedWrite| write.destination == destination)
        {
            anyhow::ensure!(
                existing.bytes == bytes && existing.mode == mode,
                "render operations disagree about {}",
                destination.display()
            );
            continue;
        }
        writes.push(PlannedWrite {
            destination,
            bytes,
            mode,
        });
    }
    Ok((writes, warnings))
}

pub fn apply_all(
    store: &Store,
    desired: &[&DesiredSubject],
    host: &str,
) -> Result<BTreeMap<String, RenderResult>> {
    let mut owners = BTreeMap::<PathBuf, (String, PlannedWrite)>::new();
    let mut results = BTreeMap::new();
    for subject in desired {
        let Some(member) = subject.member.as_ref().filter(|member| member.host == host) else {
            continue;
        };
        let Some(render) = children(&subject.desired)
            .iter()
            .find(|child| name(child) == Some("render"))
        else {
            continue;
        };
        let workspace = Path::new(&member.workspace);
        if !workspace.exists() && !member.workspace_create {
            anyhow::bail!("workspace {} does not exist", workspace.display());
        }
        let (writes, warnings) = prepare_render(store, render, workspace)?;
        let receipts = writes
            .iter()
            .map(|write| RenderReceipt {
                destination: write.destination.to_string_lossy().into_owned(),
                sha256: hex::encode(sha2::Sha256::digest(&write.bytes)),
                mode: write.mode,
            })
            .collect();
        results.insert(subject.subject.clone(), RenderResult { warnings, receipts });
        for write in writes {
            if let Some((owner, existing)) = owners.get(&write.destination) {
                anyhow::ensure!(
                    existing.bytes == write.bytes && existing.mode == write.mode,
                    "render owners `{owner}` and `{}` disagree about {}",
                    subject.subject,
                    write.destination.display()
                );
            } else {
                owners.insert(write.destination.clone(), (subject.subject.clone(), write));
            }
        }
    }
    let writes = owners
        .into_values()
        .map(|(_, write)| write)
        .collect::<Vec<_>>();
    commit_transaction(&writes)?;
    Ok(results)
}

fn ensure_tracked_file_is_unchanged(
    workspace: &Path,
    destination: &Path,
    bytes: &[u8],
) -> Result<()> {
    let Ok(relative) = destination.strip_prefix(workspace) else {
        return Ok(());
    };
    let tracked = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["ls-files", "--error-unmatch", "--"])
        .arg(relative)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .with_context(|| format!("check whether {} is tracked", destination.display()))?
        .success();
    if tracked {
        let current = fs::read(destination).with_context(|| {
            format!("read tracked render destination {}", destination.display())
        })?;
        anyhow::ensure!(
            current == bytes,
            "render refuses to change tracked file {}",
            destination.display()
        );
    }
    Ok(())
}

fn commit_transaction(writes: &[PlannedWrite]) -> Result<()> {
    let changes = writes
        .iter()
        .filter(|write| {
            let bytes_match = fs::read(&write.destination).is_ok_and(|bytes| bytes == write.bytes);
            let mode_matches = fs::metadata(&write.destination)
                .is_ok_and(|metadata| metadata.permissions().mode() & 0o7777 == write.mode);
            !bytes_match || !mode_matches
        })
        .collect::<Vec<_>>();
    let mut originals = Vec::with_capacity(changes.len());
    for write in &changes {
        let bytes = match fs::read(&write.destination) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("read render destination {}", write.destination.display())
                });
            }
        };
        let mode = match fs::metadata(&write.destination) {
            Ok(metadata) => Some(metadata.permissions().mode() & 0o7777),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("read render mode for {}", write.destination.display())
                });
            }
        };
        originals.push((write.destination.clone(), bytes, mode));
    }
    let mut committed = 0;
    for write in changes {
        if let Err(error) = atomic_write_mode(&write.destination, &write.bytes, write.mode) {
            let mut rollback_errors = Vec::new();
            for (path, bytes, mode) in originals[..committed].iter().rev() {
                let rollback = if let Some(bytes) = bytes {
                    atomic_write_mode(path, bytes, mode.unwrap_or(0o644))
                } else {
                    fs::remove_file(path).map_err(anyhow::Error::from)
                };
                if let Err(rollback) = rollback {
                    rollback_errors.push(format!("{}: {rollback}", path.display()));
                }
            }
            if !rollback_errors.is_empty() {
                anyhow::bail!(
                    "render commit failed: {error}; rollback failed: {}",
                    rollback_errors.join("; ")
                );
            }
            return Err(error);
        }
        committed += 1;
    }
    Ok(())
}

fn atomic_write_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let parent = path.parent().context("render destination has no parent")?;
    fs::create_dir_all(parent)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_extension(format!("st3-tmp-{}-{sequence}", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(mode))?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn destination(workspace: &Path, value: &str) -> Result<PathBuf> {
    let destination = PathBuf::from(value);
    anyhow::ensure!(
        !destination.is_absolute(),
        "render destination `{value}` must be relative to its workspace"
    );
    anyhow::ensure!(
        !destination
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)),
        "render destination `{value}` escapes its workspace"
    );
    let mut checked = workspace.to_path_buf();
    for component in destination.components() {
        checked.push(component);
        match fs::symlink_metadata(&checked) {
            Ok(metadata) => anyhow::ensure!(
                !metadata.file_type().is_symlink(),
                "render destination `{value}` crosses symbolic link {}",
                checked.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect render destination component {}", checked.display())
                });
            }
        }
    }
    Ok(workspace.join(destination))
}

fn merge_json(current: &mut Value, patch: Value, union_arrays: bool) {
    match (current, patch) {
        (Value::Object(current), Value::Object(patch)) => {
            for (key, value) in patch {
                if let Some(existing) = current.get_mut(&key) {
                    merge_json(existing, value, union_arrays);
                } else {
                    current.insert(key, value);
                }
            }
        }
        (Value::Array(current), Value::Array(patch)) if union_arrays => {
            for value in patch {
                if !current.contains(&value) {
                    current.push(value);
                }
            }
        }
        (current, patch) => *current = patch,
    }
}

fn name(node: &Value) -> Option<&str> {
    node.get("name").and_then(Value::as_str)
}

fn children(node: &Value) -> &[Value] {
    node.get("children")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn arguments(node: &Value) -> &[Value] {
    node.get("arguments")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn property<'a>(node: &'a Value, name: &str) -> Option<&'a Value> {
    node.get("properties")?.get(name)
}

fn executable(node: &Value) -> bool {
    property(node, "executable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn child_string(node: &Value, child_name: &str) -> Option<String> {
    children(node)
        .iter()
        .find(|child| name(child) == Some(child_name))?
        .get("arguments")?
        .as_array()?
        .first()?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_document_copy_uses_exact_version_and_mode() {
        let store = Store::open_memory("node").unwrap();
        let document = store
            .put_document("doc/script", b"echo ok\n", &None, "script")
            .unwrap();
        let desired = serde_json::json!({
            "name": "agent",
            "children": [{
                "name": "render",
                "children": [{
                    "name": "copy",
                    "arguments": [format!("doc/script@{}", document.hash), "bin/run"],
                    "properties": { "executable": true }
                }]
            }]
        });
        let workspace = tempfile::tempdir().unwrap();
        let result = apply(&store, &desired, workspace.path()).unwrap();
        assert_eq!(result.receipts.len(), 1);
        assert_eq!(result.receipts[0].mode, 0o755);
        assert_eq!(
            fs::read(workspace.path().join("bin/run")).unwrap(),
            b"echo ok\n"
        );
        assert_eq!(
            fs::metadata(workspace.path().join("bin/run"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[test]
    fn a_failed_commit_restores_every_prior_destination() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join("block"), "not a directory").unwrap();
        let desired = serde_json::json!({
            "children": [{
                "name": "render",
                "children": [
                    { "name": "file", "arguments": ["first", "new"] },
                    { "name": "file", "arguments": ["block/second", "new"] }
                ]
            }]
        });

        assert!(apply(&store, &desired, workspace.path()).is_err());
        assert!(!workspace.path().join("first").exists());
        assert_eq!(
            fs::read_to_string(workspace.path().join("block")).unwrap(),
            "not a directory"
        );
    }

    #[test]
    fn graph_render_rejects_two_owners_before_it_writes() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let source = format!(
            r#"version 2

  agent "one" {{ workspace {:?}; command "true"; render {{ file "shared" "one" }} }}
  agent "two" {{ workspace {:?}; command "true"; render {{ file "shared" "two" }} }}
"#,
            workspace.path().display().to_string(),
            workspace.path().display().to_string(),
        );
        let intent = crate::graph::parse_test_intent(&source, "node").unwrap();
        let desired = intent.subjects.values().collect::<Vec<_>>();

        let error = apply_all(&store, &desired, "node").unwrap_err();
        assert!(error.to_string().contains("disagree"));
        assert!(!workspace.path().join("shared").exists());
    }

    #[test]
    fn render_refuses_to_change_a_tracked_file() {
        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(workspace.path())
            .status()
            .unwrap();
        fs::write(workspace.path().join("tracked"), "original\n").unwrap();
        Command::new("git")
            .args(["add", "tracked"])
            .current_dir(workspace.path())
            .status()
            .unwrap();
        let desired = serde_json::json!({
            "children": [{
                "name": "render",
                "children": [{ "name": "file", "arguments": ["tracked", "changed\n"] }]
            }]
        });

        let error = apply(&store, &desired, workspace.path()).unwrap_err();
        assert!(error.to_string().contains("tracked file"));
        assert_eq!(
            fs::read_to_string(workspace.path().join("tracked")).unwrap(),
            "original\n"
        );
    }

    #[test]
    fn render_rejects_a_symbolic_link_escape() {
        use std::os::unix::fs::symlink;

        let store = Store::open_memory("node").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), workspace.path().join("outside")).unwrap();
        let desired = serde_json::json!({
            "children": [{
                "name": "render",
                "children": [{ "name": "file", "arguments": ["outside/file", "escaped"] }]
            }]
        });

        let error = apply(&store, &desired, workspace.path()).unwrap_err();
        assert!(error.to_string().contains("crosses symbolic link"));
        assert!(!outside.path().join("file").exists());
    }

    #[test]
    fn a_failed_atomic_write_removes_its_temporary_file() {
        let workspace = tempfile::tempdir().unwrap();
        let destination = workspace.path().join("occupied");
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("child"), "keep").unwrap();

        assert!(atomic_write_mode(&destination, b"new", 0o644).is_err());
        assert!(
            fs::read_dir(workspace.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains("st3-tmp"))
        );
    }
}
