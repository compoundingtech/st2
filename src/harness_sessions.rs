//! Read-only, metadata-only inspection of provider-owned harness session files.
//!
//! The selected native driver declaration is the authority boundary. This module accepts no
//! arbitrary session path and never returns prompt, message, tool, or attachment content.

use std::fs::{self, OpenOptions};
use std::io::{BufRead as _, BufReader, Read as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::{AgentSpec, Driver};

pub const HARNESS_SESSIONS_SCHEMA: &str = "st2.harness-sessions.v1";

/// One fail-closed inspection result.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessSessions {
    schema: &'static str,
    catalog: PathBuf,
    host: String,
    identity: String,
    driver: Option<String>,
    workspace: Option<PathBuf>,
    complete: bool,
    errors: Vec<String>,
    sessions: Vec<HarnessSession>,
}

impl HarnessSessions {
    pub fn incomplete(
        catalog: PathBuf,
        host: String,
        identity: String,
        error: impl Into<String>,
    ) -> Self {
        Self {
            schema: HARNESS_SESSIONS_SCHEMA,
            catalog,
            host,
            identity,
            driver: None,
            workspace: None,
            complete: false,
            errors: vec![error.into()],
            sessions: Vec::new(),
        }
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    /// Inspection errors are metadata-only diagnostics. Callers use them to explain why an
    /// incomplete inventory cannot support a supervision decision.
    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    /// The newest safe activity marker. The inventory is sorted newest first, so this does not
    /// expose provider content or make a second filesystem observation.
    pub fn newest_activity(&self) -> Option<HarnessActivity<'_>> {
        self.sessions.first().map(|session| HarnessActivity {
            modified_at_nanos: session.modified_at_nanos,
            last_record_type: session
                .last_record
                .as_ref()
                .map(|record| record.record_type.as_str()),
        })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("harness session inventory contains serializable paths")
    }

    fn push_error(&mut self, error: impl Into<String>) {
        let error = error.into();
        if !self.errors.contains(&error) {
            self.errors.push(error);
        }
        self.complete = false;
    }
}

/// The minimum provider-owned session metadata that supervision needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessActivity<'a> {
    /// Unix time in nanoseconds from the inspected file-prefix snapshot.
    pub modified_at_nanos: u128,
    /// The final provider record type, when the JSONL file contains a record.
    pub last_record_type: Option<&'a str>,
}

/// Safe metadata from one provider-owned JSONL file.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HarnessSession {
    session_id: String,
    path: PathBuf,
    /// RFC3339 UTC milliseconds from the inspected file-prefix snapshot.
    modified_at: String,
    /// Full precision used only for deterministic mtime ordering.
    #[serde(skip)]
    modified_at_nanos: u128,
    size_bytes: u64,
    permission_mode: Option<PermissionModeRecord>,
    last_record: Option<LastRecord>,
}

/// The latest standalone Claude permission-mode record.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionModeRecord {
    index: u64,
    session_id: String,
    value: String,
}

/// The raw final record position and metadata. A provider record can omit its timestamp.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LastRecord {
    index: u64,
    #[serde(rename = "type")]
    record_type: String,
    timestamp: Option<String>,
}

/// The provider fields that v1 may inspect. Serde skips every content-bearing field without
/// allocating it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaudeRecordMetadata {
    #[serde(rename = "type")]
    record_type: String,
    timestamp: Option<String>,
    session_id: Option<String>,
    permission_mode: Option<String>,
}

/// Inspect one exact local agent through its native driver declaration.
pub fn inspect(catalog: &Path, host: &str, identity: &str, home: &Path) -> HarnessSessions {
    let catalog = match catalog.canonicalize() {
        Ok(catalog) => catalog,
        Err(error) => {
            return HarnessSessions::incomplete(
                catalog.to_path_buf(),
                host.to_owned(),
                identity.to_owned(),
                format!("canonicalize catalog {}: {error}", catalog.display()),
            );
        }
    };
    let mut result = HarnessSessions {
        schema: HARNESS_SESSIONS_SCHEMA,
        catalog: catalog.clone(),
        host: host.to_owned(),
        identity: identity.to_owned(),
        driver: None,
        workspace: None,
        complete: true,
        errors: Vec::new(),
        sessions: Vec::new(),
    };

    let before = match crate::catalog_lock::read_fence(&catalog) {
        Ok(fence) => fence,
        Err(error) => {
            result.push_error(error.to_string());
            return result;
        }
    };
    let found = crate::discover_strict(&catalog);
    for error in &found.errors {
        result.push_error(format!(
            "catalog file {}: {}",
            error.path.display(),
            error.message
        ));
    }
    match crate::catalog_lock::read_fence(&catalog) {
        Ok(after) if after == before => {}
        Ok(_) => result.push_error("catalog generation changed during agent discovery"),
        Err(error) => result.push_error(error.to_string()),
    }
    if !result.complete {
        return result;
    }

    let matches = found
        .specs
        .iter()
        .filter(|spec| spec.bus_id(host) == identity)
        .cloned()
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        result.push_error(format!(
            "expected exactly one Agent Spec with identity `{identity}`, found {}",
            matches.len()
        ));
        return result;
    }
    let spec = &matches[0];
    if spec.resolved_host(host) != host {
        result.push_error(format!(
            "Agent Spec `{identity}` belongs to nonlocal host `{}`",
            spec.resolved_host(host)
        ));
        return result;
    }
    let Some(driver) = spec.driver.as_ref() else {
        result.push_error(format!(
            "Agent Spec `{identity}` has no native driver declaration"
        ));
        return result;
    };
    result.driver = Some(driver.name().to_owned());
    if !matches!(driver, Driver::Claude(_)) {
        result.push_error(format!(
            "driver `{}` is unsupported by harness session inspection v1",
            driver.name()
        ));
        return result;
    }

    let workspace = match resolved_workspace(spec, &catalog, host) {
        Ok(workspace) => workspace,
        Err(error) => {
            result.push_error(format!("{error:#}"));
            return result;
        }
    };
    result.workspace = Some(workspace.clone());
    let project_key = match claude_project_key(&workspace) {
        Ok(key) => key,
        Err(error) => {
            result.push_error(error.to_string());
            return result;
        }
    };

    for other in found.specs.iter().filter(|other| {
        other.resolved_host(host) == host && matches!(other.driver, Some(Driver::Claude(_)))
    }) {
        if other.bus_id(host) == identity {
            continue;
        }
        let other_workspace = match effective_workspace(other, &catalog, host) {
            Ok(workspace) => workspace,
            Err(error) => {
                result.push_error(format!(
                    "cannot prove Claude workspace ownership because `{}` does not resolve: {error}",
                    other.bus_id(host)
                ));
                continue;
            }
        };
        match claude_project_key(&other_workspace) {
            Ok(other_key) if other_key == project_key => result.push_error(format!(
                "Claude session directory is shared with Agent Spec `{}`",
                other.bus_id(host)
            )),
            Ok(_) => {}
            Err(error) => result.push_error(format!(
                "cannot prove Claude workspace ownership for `{}`: {error}",
                other.bus_id(host)
            )),
        }
    }
    if !result.complete {
        return result;
    }

    if !home.is_absolute() {
        result.push_error(format!("HOME is not absolute: {}", home.display()));
        return result;
    }
    let session_dir = home.join(".claude/projects").join(project_key);
    match inspect_claude_dir(&session_dir) {
        Ok(sessions) => result.sessions = sessions,
        Err(error) => result.push_error(format!("{error:#}")),
    }

    let after = crate::discover_strict(&catalog);
    if !crate::task_inventory::same_discovery(&found, &after) {
        result.push_error("catalog declarations changed during harness session inspection");
    }
    match crate::catalog_lock::read_fence(&catalog) {
        Ok(after) if after == before => {}
        Ok(_) => result.push_error("catalog generation changed during harness session inspection"),
        Err(error) => result.push_error(error.to_string()),
    }
    result
}

fn resolved_workspace(spec: &AgentSpec, catalog: &Path, host: &str) -> Result<PathBuf> {
    let workspace = spec.workspace.as_deref().with_context(|| {
        format!(
            "Agent Spec `{}` has no declared workspace",
            spec.bus_id(host)
        )
    })?;
    let spec_dir = spec
        .path
        .parent()
        .context("Agent Spec path has no parent directory")?;
    crate::expand::resolve_spec_path(workspace, catalog, spec_dir)
        .with_context(|| format!("resolve declared workspace `{workspace}`"))
}

/// Resolve another Claude seat's actual cwd for collision detection. The selected seat still
/// requires an explicit workspace because only that declaration grants the requested read.
fn effective_workspace(spec: &AgentSpec, catalog: &Path, host: &str) -> Result<PathBuf> {
    match spec.workspace.as_deref() {
        Some(_) => resolved_workspace(spec, catalog, host),
        None => spec
            .path
            .parent()
            .map(Path::to_path_buf)
            .context("Agent Spec path has no parent directory"),
    }
}

fn claude_project_key(workspace: &Path) -> Result<String> {
    let workspace = workspace
        .to_str()
        .with_context(|| format!("workspace is not UTF-8: {}", workspace.display()))?;
    Ok(workspace
        .chars()
        .map(|character| match character {
            '/' | '.' => '-',
            other => other,
        })
        .collect())
}

fn inspect_claude_dir(path: &Path) -> Result<Vec<HarnessSession>> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect Claude session directory {}", path.display()))?;
    anyhow::ensure!(
        before.is_dir() && !before.file_type().is_symlink(),
        "Claude session directory is not a real directory: {}",
        path.display()
    );

    let paths = claude_jsonl_paths(path)?;
    let mut sessions = Vec::with_capacity(paths.len());
    for session_path in &paths {
        sessions.push(inspect_claude_file(session_path)?);
    }
    sessions.sort_by(|left, right| {
        right
            .modified_at_nanos
            .cmp(&left.modified_at_nanos)
            .then_with(|| left.session_id.cmp(&right.session_id))
    });

    let after_paths = claude_jsonl_paths(path)?;
    anyhow::ensure!(
        paths == after_paths,
        "Claude session file set changed during inspection: {}",
        path.display()
    );
    let after = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect Claude session directory {}", path.display()))?;
    anyhow::ensure!(
        after.is_dir() && !after.file_type().is_symlink() && same_file(&before, &after),
        "Claude session directory changed during inspection: {}",
        path.display()
    );
    Ok(sessions)
}

fn claude_jsonl_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)
        .with_context(|| format!("read Claude session directory {}", path.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "read an entry from Claude session directory {}",
                path.display()
            )
        })?;
        let entry_path = entry.path();
        if entry_path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspect Claude session entry {}", entry_path.display()))?;
        anyhow::ensure!(
            file_type.is_file() && !file_type.is_symlink(),
            "Claude session entry is not a real regular file: {}",
            entry_path.display()
        );
        paths.push(entry_path);
    }
    paths.sort();
    Ok(paths)
}

fn inspect_claude_file(path: &Path) -> Result<HarnessSession> {
    let session_id = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .with_context(|| {
            format!(
                "Claude session filename has no UTF-8 ID: {}",
                path.display()
            )
        })?
        .to_owned();
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open Claude session file {}", path.display()))?;
    let before = file
        .metadata()
        .with_context(|| format!("inspect Claude session file {}", path.display()))?;
    anyhow::ensure!(
        before.is_file(),
        "Claude session entry is not a regular file: {}",
        path.display()
    );
    let size_bytes = before.len();
    let modified = before
        .modified()
        .with_context(|| format!("read Claude session mtime {}", path.display()))?;
    let modified_at_nanos = system_time_nanos(modified)?;
    let modified_at = crate::exec_backend::rfc3339_utc(modified)
        .context("format Claude session mtime as RFC3339 UTC")?;

    let mut snapshot = file.by_ref().take(size_bytes);
    let (permission_mode, last_record) = parse_claude_records(&mut snapshot, &session_id)
        .with_context(|| format!("parse Claude session file {}", path.display()))?;
    anyhow::ensure!(
        snapshot.limit() == 0,
        "Claude session file shortened during inspection: {}",
        path.display()
    );

    let after = file
        .metadata()
        .with_context(|| format!("reinspect Claude session file {}", path.display()))?;
    anyhow::ensure!(
        after.len() >= size_bytes,
        "Claude session file shortened during inspection: {}",
        path.display()
    );
    if after.len() == size_bytes {
        anyhow::ensure!(
            before.mtime() == after.mtime() && before.mtime_nsec() == after.mtime_nsec(),
            "Claude session file changed in place during inspection: {}",
            path.display()
        );
    }
    let current = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect Claude session path {}", path.display()))?;
    anyhow::ensure!(
        current.is_file() && !current.file_type().is_symlink() && same_file(&before, &current),
        "Claude session file was replaced during inspection: {}",
        path.display()
    );

    Ok(HarnessSession {
        session_id,
        path: path.to_path_buf(),
        modified_at,
        modified_at_nanos,
        size_bytes,
        permission_mode,
        last_record,
    })
}

fn parse_claude_records(
    reader: impl std::io::Read,
    expected_session_id: &str,
) -> Result<(Option<PermissionModeRecord>, Option<LastRecord>)> {
    let mut permission_mode = None;
    let mut last_record = None;
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let mut index = 0_u64;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .context("read JSONL record")?;
        if read == 0 {
            break;
        }
        index += 1;
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        anyhow::ensure!(!line.is_empty(), "record {index} is empty");
        let record: ClaudeRecordMetadata = serde_json::from_slice(&line)
            .with_context(|| format!("record {index} is not valid JSON metadata"))?;
        last_record = Some(LastRecord {
            index,
            record_type: record.record_type.clone(),
            timestamp: record.timestamp,
        });

        if record.record_type == "permission-mode" {
            let session_id = record
                .session_id
                .as_deref()
                .with_context(|| format!("permission-mode record {index} has no sessionId"))?;
            anyhow::ensure!(
                session_id == expected_session_id,
                "permission-mode record {index} belongs to session `{session_id}`, not `{expected_session_id}`"
            );
            let value = record
                .permission_mode
                .as_deref()
                .with_context(|| format!("permission-mode record {index} has no permissionMode"))?;
            permission_mode = Some(PermissionModeRecord {
                index,
                session_id: session_id.to_owned(),
                value: value.to_owned(),
            });
        }
    }
    Ok((permission_mode, last_record))
}

fn system_time_nanos(time: std::time::SystemTime) -> Result<u128> {
    Ok(time
        .duration_since(UNIX_EPOCH)
        .context("file mtime predates the Unix epoch")?
        .as_nanos())
}

fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_key_replaces_slashes_and_dots_only() {
        assert_eq!(
            claude_project_key(Path::new("/work/example.com/a-b")).unwrap(),
            "-work-example-com-a-b"
        );
    }

    #[test]
    fn parser_keeps_null_tail_time_and_latest_permission_mode() {
        let bytes = br#"{"type":"permission-mode","sessionId":"s","permissionMode":"default"}
{"type":"assistant","sessionId":"s","timestamp":"2026-08-25T10:00:00.000Z","message":{"content":"secret"}}
{"type":"permission-mode","sessionId":"s","permissionMode":"bypassPermissions"}
{"type":"last-prompt","sessionId":"s","prompt":"secret"}
"#;
        let (permission, last) = parse_claude_records(bytes.as_slice(), "s").unwrap();
        let permission = permission.unwrap();
        assert_eq!(permission.index, 3);
        assert_eq!(permission.value, "bypassPermissions");
        let last = last.unwrap();
        assert_eq!(last.index, 4);
        assert_eq!(last.record_type, "last-prompt");
        assert_eq!(last.timestamp, None);
    }

    #[test]
    fn parser_refuses_a_borrowable_non_string_timestamp() {
        let error = parse_claude_records(
            br#"{"type":"assistant","timestamp":7}
"#
            .as_slice(),
            "s",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("not valid JSON metadata"),
            "{error:#}"
        );
    }

    #[test]
    fn parser_refuses_permission_mode_from_another_session() {
        let error = parse_claude_records(
            br#"{"type":"permission-mode","sessionId":"other","permissionMode":"default"}
"#
            .as_slice(),
            "expected",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("belongs to session"),
            "{error:#}"
        );
    }

    #[test]
    fn parser_refuses_two_records_on_one_jsonl_line() {
        let error = parse_claude_records(
            br#"{"type":"user"}{"type":"assistant"}
"#
            .as_slice(),
            "s",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("not valid JSON metadata"),
            "{error:#}"
        );
    }
}
