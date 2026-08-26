use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde_json::Value;

use crate::store::Store;

pub fn apply(store: &Store, desired: &Value, workspace: &Path) -> Result<Vec<String>> {
    let Some(render) = children(desired)
        .iter()
        .find(|child| name(child) == Some("render"))
    else {
        return Ok(Vec::new());
    };
    fs::create_dir_all(workspace)?;
    let mut warnings = Vec::new();
    for operation in children(render) {
        match name(operation) {
            Some("copy") => copy(store, operation, workspace)?,
            Some("file") => write_inline(operation, workspace)?,
            Some("json-upsert") => json_upsert(operation, workspace)?,
            Some("ensure-line") => ensure_line(operation, workspace)?,
            Some("git-exclude") => {
                if let Err(error) = git_exclude(operation, workspace) {
                    warnings.push(error.to_string());
                }
            }
            Some(other) => anyhow::bail!("unknown render operation `{other}`"),
            None => anyhow::bail!("render operation has no name"),
        }
    }
    Ok(warnings)
}

fn copy(store: &Store, operation: &Value, workspace: &Path) -> Result<()> {
    let arguments = arguments(operation);
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
    let destination = destination(
        workspace,
        arguments[1].as_str().context("destination is not text")?,
    )?;
    atomic_write(&destination, &bytes, executable(operation))
}

fn write_inline(operation: &Value, workspace: &Path) -> Result<()> {
    let arguments = arguments(operation);
    anyhow::ensure!(
        !arguments.is_empty() && arguments.len() <= 2,
        "render file has invalid arguments"
    );
    let destination = destination(
        workspace,
        arguments[0]
            .as_str()
            .context("render file destination is not text")?,
    )?;
    let content = if let Some(value) = arguments.get(1).and_then(Value::as_str) {
        value.to_owned()
    } else {
        child_string(operation, "content").context("render file has no content")?
    };
    atomic_write(&destination, content.as_bytes(), executable(operation))
}

fn json_upsert(operation: &Value, workspace: &Path) -> Result<()> {
    let arguments = arguments(operation);
    anyhow::ensure!(
        !arguments.is_empty() && arguments.len() <= 2,
        "json-upsert has invalid arguments"
    );
    let destination = destination(
        workspace,
        arguments[0]
            .as_str()
            .context("json-upsert destination is not text")?,
    )?;
    let content = if let Some(value) = arguments.get(1).and_then(Value::as_str) {
        value.to_owned()
    } else {
        child_string(operation, "content").context("json-upsert has no content")?
    };
    let patch: Value =
        serde_json::from_str(&content).context("json-upsert content is invalid JSON")?;
    anyhow::ensure!(patch.is_object(), "json-upsert content must be an object");
    let mut current = match fs::read_to_string(&destination) {
        Ok(value) => serde_json::from_str(&value)
            .with_context(|| format!("parse existing JSON {}", destination.display()))?,
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
    atomic_write(&destination, &bytes, executable(operation))
}

fn ensure_line(operation: &Value, workspace: &Path) -> Result<()> {
    let arguments = arguments(operation);
    anyhow::ensure!(
        arguments.len() == 2,
        "ensure-line needs destination and line"
    );
    let destination = destination(
        workspace,
        arguments[0]
            .as_str()
            .context("ensure-line destination is not text")?,
    )?;
    let line = arguments[1]
        .as_str()
        .context("ensure-line value is not text")?;
    let mut current = match fs::read_to_string(&destination) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    if !current.lines().any(|existing| existing == line) {
        if !current.is_empty() && !current.ends_with('\n') {
            current.push('\n');
        }
        current.push_str(line);
        current.push('\n');
    }
    atomic_write(&destination, current.as_bytes(), executable(operation))
}

fn git_exclude(operation: &Value, workspace: &Path) -> Result<()> {
    let destination = workspace.join(".git/info/exclude");
    let mut current = match fs::read_to_string(&destination) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    for argument in arguments(operation) {
        let line = argument.as_str().context("git-exclude path is not text")?;
        if !current.lines().any(|existing| existing == line) {
            if !current.is_empty() && !current.ends_with('\n') {
                current.push('\n');
            }
            current.push_str(line);
            current.push('\n');
        }
    }
    atomic_write(&destination, current.as_bytes(), false)
}

fn atomic_write(path: &Path, bytes: &[u8], executable: bool) -> Result<()> {
    let parent = path.parent().context("render destination has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("st3-tmp-{}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(if executable { 0o755 } else { 0o644 })
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::set_permissions(
        &temporary,
        fs::Permissions::from_mode(if executable { 0o755 } else { 0o644 }),
    )?;
    fs::rename(&temporary, path)?;
    Ok(())
}

fn destination(workspace: &Path, value: &str) -> Result<PathBuf> {
    let destination = PathBuf::from(value);
    if destination.is_absolute() {
        return Ok(destination);
    }
    anyhow::ensure!(
        !destination
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)),
        "render destination `{value}` escapes its workspace"
    );
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
        apply(&store, &desired, workspace.path()).unwrap();
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
}
