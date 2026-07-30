//! Explicit authoring operations over one exact catalog agent.
//!
//! These operations change catalog-owned presentation/lifecycle state, never a running task.
//! Target resolution is content-based and refuses ambiguity rather than depending on folder names.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use kdl::{KdlDocument, KdlNode};
use serde::Serialize;

/// Display names are one human-facing line, bounded independently from stable identity.
pub const DISPLAY_NAME_MAX_CHARS: usize = 160;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Whether an authoring request changed bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorOutcome {
    Changed,
    Unchanged,
}

/// Stable machine-readable receipt from display-name authoring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DisplayNameReceipt {
    pub result: AuthorOutcome,
    pub identity: String,
    pub name: Option<String>,
    pub retired: bool,
}

/// Whether retirement intent was newly authored or already present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RetireOutcome {
    Authored,
    Unchanged,
}

/// Retirement authoring does not inspect or control the selected host's runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRetirement {
    NotObserved,
}

/// Stable machine-readable receipt from source-preserving retirement authoring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetireReceipt {
    pub result: RetireOutcome,
    pub identity: String,
    pub retired: bool,
    pub runtime_retirement: RuntimeRetirement,
}

/// A classified authoring failure. `code` is stable for machine consumers.
#[derive(Debug)]
pub struct AuthorError {
    code: &'static str,
    message: String,
}

impl AuthorError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for AuthorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AuthorError {}

#[derive(Debug)]
struct AgentTarget {
    identity: String,
    declaration: PathBuf,
    directory: PathBuf,
    retired: bool,
}

/// Set or clear the optional display name for one exact catalog agent.
///
/// A qualified bus identity wins over a bare-identity match. Bare identities are accepted only
/// when unique across the selected catalog. Retired and explicitly remote declarations remain
/// authorable because the adjacent `name` file is presentation metadata, not runtime control.
pub fn set_display_name(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
    requested: Option<&str>,
) -> Result<DisplayNameReceipt, AuthorError> {
    let target = resolve_target(catalog_root, selector, this_host)?;
    let requested = requested.map(validate_display_name).transpose()?;
    let path = target.directory.join("name");

    let result = match requested.as_deref() {
        Some(name) => set_name_file(&path, name)?,
        None => clear_name_file(&path)?,
    };

    Ok(DisplayNameReceipt {
        result,
        identity: target.identity,
        name: requested,
        retired: target.retired,
    })
}

/// Author `retired #true` in one exact canonical KDL declaration.
///
/// This changes desired lifecycle intent only. It never moves the declaration, inspects runtime
/// state, reconciles, or claims teardown completion; a running supervisor observes the next
/// snapshot and owns teardown.
pub fn retire_agent(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
) -> Result<RetireReceipt, AuthorError> {
    let _writer_fence = DirectoryWriterFence::acquire(catalog_root)?;
    let target = resolve_retire_target(catalog_root, selector, this_host)?;
    let result = retire_declaration_under_fence(&target.declaration, &target.identity, || {})?;
    Ok(RetireReceipt {
        result,
        identity: target.identity,
        retired: true,
        runtime_retirement: RuntimeRetirement::NotObserved,
    })
}

fn resolve_retire_target(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
) -> Result<AgentTarget, AuthorError> {
    let found = crate::discover(catalog_root);
    if let Some(error) = found.errors.first() {
        return Err(AuthorError::new(
            "catalog-malformed",
            format!(
                "cannot prove an exact retirement target while {} is malformed: {}",
                error.path.display(),
                error.message
            ),
        ));
    }
    let exact = found
        .specs
        .iter()
        .filter(|spec| spec.bus_id(this_host) == selector)
        .collect::<Vec<_>>();
    if exact.is_empty() {
        if found.specs.iter().any(|spec| spec.identity == selector) {
            return Err(AuthorError::new(
                "exact-target-required",
                format!(
                    "retirement requires an exact '<host>.<identity>' selector, not '{selector}'"
                ),
            ));
        }
        return Err(AuthorError::new(
            "target-not-found",
            format!(
                "no agent '{selector}' found in catalog {}",
                catalog_root.display()
            ),
        ));
    }
    if exact.len() > 1 {
        let mut candidates = exact
            .iter()
            .map(|spec| spec.path.display().to_string())
            .collect::<Vec<_>>();
        candidates.sort();
        return Err(AuthorError::new(
            "target-ambiguous",
            format!(
                "agent selector '{selector}' is declared more than once: {}",
                candidates.join(", ")
            ),
        ));
    }
    let spec = exact[0];
    let directory = spec.path.parent().map(Path::to_path_buf).ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!("agent '{selector}' has no declaration parent"),
        )
    })?;
    Ok(AgentTarget {
        identity: spec.bus_id(this_host),
        declaration: spec.path.clone(),
        directory,
        retired: spec.retired,
    })
}

fn resolve_target(
    catalog_root: &Path,
    selector: &str,
    this_host: &str,
) -> Result<AgentTarget, AuthorError> {
    let found = crate::discover(catalog_root);
    let exact = found
        .specs
        .iter()
        .filter(|spec| spec.bus_id(this_host) == selector)
        .collect::<Vec<_>>();
    let matches = if exact.is_empty() {
        found
            .specs
            .iter()
            .filter(|spec| spec.identity == selector)
            .collect::<Vec<_>>()
    } else {
        exact
    };

    if matches.is_empty() {
        return Err(AuthorError::new(
            "target-not-found",
            format!(
                "no agent '{selector}' found in catalog {}",
                catalog_root.display()
            ),
        ));
    }
    if matches.len() > 1 {
        let mut candidates = matches
            .iter()
            .map(|spec| format!("{} ({})", spec.bus_id(this_host), spec.path.display()))
            .collect::<Vec<_>>();
        candidates.sort();
        return Err(AuthorError::new(
            "target-ambiguous",
            format!(
                "agent selector '{selector}' is ambiguous: {}",
                candidates.join(", ")
            ),
        ));
    }

    let spec = matches[0];
    let directory = spec.path.parent().map(Path::to_path_buf).ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!(
                "agent '{}' has no declaration parent",
                spec.bus_id(this_host)
            ),
        )
    })?;
    Ok(AgentTarget {
        identity: spec.bus_id(this_host),
        declaration: spec.path.clone(),
        directory,
        retired: spec.retired,
    })
}

#[cfg(test)]
fn retire_declaration(
    path: &Path,
    expected_identity: &str,
    before_commit: impl FnOnce(),
) -> Result<RetireOutcome, AuthorError> {
    let directory = path.parent().ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!("declaration path {} has no parent", path.display()),
        )
    })?;
    let _writer_fence = DirectoryWriterFence::acquire(directory)?;
    retire_declaration_under_fence(path, expected_identity, before_commit)
}

fn retire_declaration_under_fence(
    path: &Path,
    expected_identity: &str,
    before_commit: impl FnOnce(),
) -> Result<RetireOutcome, AuthorError> {
    if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
        return Err(AuthorError::new(
            "unsupported-declaration-format",
            format!(
                "retirement authoring requires canonical KDL, found {}",
                path.display()
            ),
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(AuthorError::new(
            "unsafe-declaration-path",
            format!("refusing non-regular declaration path {}", path.display()),
        ));
    }
    let original = fs::read(path).map_err(|error| {
        AuthorError::new(
            "declaration-read-failed",
            format!("reading declaration {}: {error}", path.display()),
        )
    })?;
    let text = std::str::from_utf8(&original).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("declaration {} is not UTF-8: {error}", path.display()),
        )
    })?;
    let document = KdlDocument::parse(text).map_err(|error| {
        AuthorError::new(
            "malformed-declaration",
            format!("parsing declaration {}: {error}", path.display()),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity)?;
    let replacement = retirement_edit(text, target)?;
    let Some(replacement) = replacement else {
        return Ok(RetireOutcome::Unchanged);
    };
    verify_retired_candidate(&replacement, expected_identity)?;
    atomic_replace_checked(
        path,
        &original,
        replacement.as_bytes(),
        metadata.permissions().mode() & 0o7777,
        before_commit,
    )?;
    Ok(RetireOutcome::Authored)
}

fn exact_agent_node<'a>(
    document: &'a KdlDocument,
    expected_identity: &str,
) -> Result<&'a KdlNode, AuthorError> {
    let matches = document
        .nodes()
        .iter()
        .filter(|node| {
            node.name().value() == "agent"
                && explicit_agent_identity(node).as_deref() == Some(expected_identity)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [target] => Ok(*target),
        [] => Err(AuthorError::new(
            "target-changed",
            format!("declaration no longer contains explicit agent '{expected_identity}'"),
        )),
        _ => Err(AuthorError::new(
            "target-ambiguous",
            format!("declaration contains more than one explicit agent '{expected_identity}'"),
        )),
    }
}

fn explicit_agent_identity(node: &KdlNode) -> Option<String> {
    let mut identity = node
        .get(0)
        .and_then(|value| value.as_string())
        .map(str::to_string);
    let mut host = None;
    for child in node.children()?.nodes() {
        match child.name().value() {
            "identity" => {
                identity = child
                    .get(0)
                    .and_then(|value| value.as_string())
                    .map(str::to_string)
                    .or(identity);
            }
            "host" => {
                host = child
                    .get(0)
                    .and_then(|value| value.as_string())
                    .map(str::to_string);
            }
            _ => {}
        }
    }
    Some(format!("{}.{}", host?, identity?))
}

fn retirement_edit(text: &str, target: &KdlNode) -> Result<Option<String>, AuthorError> {
    let retired = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| child.name().value() == "retired")
        .collect::<Vec<_>>();
    match retired.as_slice() {
        [] => insert_retirement(text, target).map(Some),
        [node] => replace_false_retirement(text, node),
        _ => Err(AuthorError::new(
            "malformed-retirement-intent",
            "target declares `retired` more than once",
        )),
    }
}

fn replace_false_retirement(text: &str, retired: &KdlNode) -> Result<Option<String>, AuthorError> {
    if retired.children().is_some() || retired.entries().len() != 1 {
        return Err(AuthorError::new(
            "malformed-retirement-intent",
            "`retired` must contain exactly one #true or #false argument",
        ));
    }
    let entry = &retired.entries()[0];
    if entry.name().is_some() {
        return Err(AuthorError::new(
            "malformed-retirement-intent",
            "`retired` must use one positional boolean argument",
        ));
    }
    let value = entry.value().as_bool().ok_or_else(|| {
        AuthorError::new(
            "malformed-retirement-intent",
            "`retired` must contain #true or #false",
        )
    })?;
    let span = entry.span();
    let range = span.offset()..span.offset() + span.len();
    let source = text.get(range.clone()).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "retirement value span falls outside the declaration",
        )
    })?;
    let expected = if value { "#true" } else { "#false" };
    if source != expected {
        return Err(AuthorError::new(
            "unsupported-retirement-syntax",
            format!("retirement value must use canonical `{expected}` syntax"),
        ));
    }
    if value {
        return Ok(None);
    }
    let mut replacement = text.to_string();
    replacement.replace_range(range, "#true");
    Ok(Some(replacement))
}

struct DirectoryWriterFence(fs::File);

impl DirectoryWriterFence {
    fn acquire(directory: &Path) -> Result<Self, AuthorError> {
        let file = fs::File::open(directory).map_err(|error| {
            AuthorError::new(
                "writer-fence-failed",
                format!(
                    "opening declaration directory {} for writer fencing: {error}",
                    directory.display()
                ),
            )
        })?;
        // SAFETY: `file` owns a valid descriptor for the duration of the returned guard.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            let code = if error.kind() == std::io::ErrorKind::WouldBlock {
                "writer-collision"
            } else {
                "writer-fence-failed"
            };
            return Err(AuthorError::new(
                code,
                format!(
                    "acquiring declaration writer fence for {}: {error}",
                    directory.display()
                ),
            ));
        }
        Ok(Self(file))
    }
}

impl Drop for DirectoryWriterFence {
    fn drop(&mut self) {
        // SAFETY: the guard still owns this valid descriptor. Closing it is also an unlock
        // backstop, so a Drop-time unlock error is intentionally ignored.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn insert_retirement(text: &str, target: &KdlNode) -> Result<String, AuthorError> {
    let span = target.span();
    let start = span.offset();
    let end = start + span.len();
    let source = text.get(start..end).ok_or_else(|| {
        AuthorError::new(
            "malformed-declaration",
            "agent span falls outside the declaration",
        )
    })?;
    let mut replacement = text.to_string();

    if target.children().is_none() {
        replacement.insert_str(end, " { retired #true }");
        return Ok(replacement);
    }
    if !source.ends_with('}') {
        return Err(AuthorError::new(
            "unsafe-source-shape",
            "agent child block does not end at a source-preserving insertion point",
        ));
    }
    let close = source.len() - 1;
    if let Some(newline) = source[..close].rfind('\n') {
        let closing_indent = &source[newline + 1..close];
        if !closing_indent
            .chars()
            .all(|value| matches!(value, ' ' | '\t'))
        {
            return Err(AuthorError::new(
                "unsafe-source-shape",
                "cannot preserve a non-whitespace closing-brace prefix",
            ));
        }
        let child_indent = target
            .children()
            .and_then(|children| children.nodes().first())
            .and_then(|child| line_indent(text, child.span().offset()))
            .unwrap_or_else(|| format!("{closing_indent}  "));
        replacement.insert_str(
            start + newline + 1,
            &format!("{child_indent}retired #true\n"),
        );
        return Ok(replacement);
    }

    let before_close = &source[..close];
    let trimmed = before_close.trim_end();
    let insertion = if trimmed.ends_with('{') {
        " retired #true"
    } else if trimmed.ends_with(';') {
        " retired #true;"
    } else {
        "; retired #true"
    };
    replacement.insert_str(start + trimmed.len(), insertion);
    Ok(replacement)
}

fn line_indent(text: &str, offset: usize) -> Option<String> {
    let prefix = text.get(..offset)?;
    let start = prefix.rfind('\n').map_or(0, |newline| newline + 1);
    let indent = prefix.get(start..)?;
    indent
        .chars()
        .all(|value| matches!(value, ' ' | '\t'))
        .then(|| indent.to_string())
}

fn verify_retired_candidate(candidate: &str, expected_identity: &str) -> Result<(), AuthorError> {
    let document = KdlDocument::parse(candidate).map_err(|error| {
        AuthorError::new(
            "unsafe-source-edit",
            format!("retirement edit did not produce valid KDL: {error}"),
        )
    })?;
    let target = exact_agent_node(&document, expected_identity)?;
    let retired = target
        .children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .filter(|child| child.name().value() == "retired")
        .collect::<Vec<_>>();
    if retired.len() != 1
        || retired[0].entries().len() != 1
        || retired[0].get(0).and_then(|value| value.as_bool()) != Some(true)
    {
        return Err(AuthorError::new(
            "unsafe-source-edit",
            "retirement edit did not author exactly one `retired #true` intent",
        ));
    }
    Ok(())
}

fn atomic_replace_checked(
    path: &Path,
    original: &[u8],
    replacement: &[u8],
    mode: u32,
    before_commit: impl FnOnce(),
) -> Result<(), AuthorError> {
    let directory = path.parent().ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!("declaration path {} has no parent", path.display()),
        )
    })?;
    let temporary = directory.join(format!(
        ".agent.kdl.retire-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&temporary)?;
        file.write_all(replacement)?;
        file.sync_all()
    })();
    if let Err(error) = write {
        let _ = fs::remove_file(&temporary);
        return Err(AuthorError::new(
            "declaration-write-failed",
            format!("staging retirement declaration {}: {error}", path.display()),
        ));
    }

    before_commit();
    let current = fs::symlink_metadata(path)
        .ok()
        .filter(|metadata| metadata.file_type().is_file())
        .and_then(|_| fs::read(path).ok());
    if current.as_deref() != Some(original) {
        let _ = fs::remove_file(&temporary);
        return Err(AuthorError::new(
            "source-changed",
            format!(
                "declaration {} changed while retirement was being authored",
                path.display()
            ),
        ));
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(AuthorError::new(
            "declaration-write-failed",
            format!(
                "atomically publishing retirement declaration {}: {error}",
                path.display()
            ),
        ));
    }
    fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            AuthorError::new(
                "declaration-write-failed",
                format!(
                    "syncing declaration directory {}: {error}",
                    directory.display()
                ),
            )
        })
}

fn validate_display_name(raw: &str) -> Result<String, AuthorError> {
    if raw.is_empty() {
        return Err(AuthorError::new(
            "invalid-display-name",
            "display name cannot be empty; use --clear to remove it",
        ));
    }
    if raw.trim() != raw {
        return Err(AuthorError::new(
            "invalid-display-name",
            "display name cannot begin or end with whitespace",
        ));
    }
    if raw.chars().any(char::is_control) {
        return Err(AuthorError::new(
            "invalid-display-name",
            "display name must be one printable line without control characters",
        ));
    }
    if raw.chars().count() > DISPLAY_NAME_MAX_CHARS {
        return Err(AuthorError::new(
            "invalid-display-name",
            format!("display name exceeds the {DISPLAY_NAME_MAX_CHARS}-character limit"),
        ));
    }
    Ok(raw.to_string())
}

fn existing_regular_file(path: &Path) -> Result<bool, AuthorError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(AuthorError::new(
            "unsafe-name-path",
            format!("refusing non-regular display-name path {}", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(AuthorError::new(
            "name-read-failed",
            format!("reading display-name path {}: {error}", path.display()),
        )),
    }
}

fn exact_name_file(path: &Path, expected: &str) -> Result<bool, AuthorError> {
    if !existing_regular_file(path)? {
        return Ok(false);
    }
    let raw = fs::read(path).map_err(|error| {
        AuthorError::new(
            "name-read-failed",
            format!("reading display name {}: {error}", path.display()),
        )
    })?;
    Ok(raw == format!("{expected}\n").as_bytes())
}

fn set_name_file(path: &Path, name: &str) -> Result<AuthorOutcome, AuthorError> {
    if exact_name_file(path, name)? {
        return Ok(AuthorOutcome::Unchanged);
    }
    let directory = path.parent().ok_or_else(|| {
        AuthorError::new(
            "invalid-target",
            format!("display-name path {} has no parent", path.display()),
        )
    })?;
    let temporary = directory.join(format!(
        ".name.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        writeln!(file, "{name}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)
    })();
    if let Err(error) = write {
        let _ = fs::remove_file(&temporary);
        return Err(AuthorError::new(
            "name-write-failed",
            format!(
                "atomically writing display name {}: {error}",
                path.display()
            ),
        ));
    }
    Ok(AuthorOutcome::Changed)
}

fn clear_name_file(path: &Path) -> Result<AuthorOutcome, AuthorError> {
    if !existing_regular_file(path)? {
        return Ok(AuthorOutcome::Unchanged);
    }
    fs::remove_file(path).map_err(|error| {
        AuthorError::new(
            "name-write-failed",
            format!("clearing display name {}: {error}", path.display()),
        )
    })?;
    Ok(AuthorOutcome::Changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_ids(root: &Path) -> Vec<String> {
        let found = crate::discover(root);
        crate::reconcile(&found.specs, &[], "h")
            .launch
            .iter()
            .flat_map(|launch| launch.tasks.iter())
            .map(|task| task.pty_id.clone())
            .collect()
    }

    fn write(root: &Path, relative: &str, contents: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    fn declaration(identity: &str, host: &str, retired: bool) -> String {
        format!(
            "agent \"{identity}\" {{\n  host \"{host}\"\n  retired #{retired}\n  command \"sleep 60\"\n}}\n"
        )
    }

    #[test]
    fn set_replace_idempotent_and_clear_touch_only_the_name_file() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let declaration = declaration("worker", "h", false);
        write(root, "h/worker/agent.kdl", &declaration);
        write(root, "h/worker/resources/context/now.md", "durable\n");
        let initial_task_ids = task_ids(root);

        let set = set_display_name(root, "h.worker", "h", Some("Build owner")).unwrap();
        assert_eq!(set.result, AuthorOutcome::Changed);
        assert_eq!(set.name.as_deref(), Some("Build owner"));
        assert_eq!(
            fs::read_to_string(root.join("h/worker/name")).unwrap(),
            "Build owner\n"
        );
        assert_eq!(
            set_display_name(root, "worker", "h", Some("Build owner"))
                .unwrap()
                .result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(
            fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
            declaration
        );
        assert_eq!(
            fs::read_to_string(root.join("h/worker/resources/context/now.md")).unwrap(),
            "durable\n"
        );
        assert_eq!(task_ids(root), initial_task_ids);

        assert_eq!(
            set_display_name(root, "worker", "h", None).unwrap().result,
            AuthorOutcome::Changed
        );
        assert!(!root.join("h/worker/name").exists());
        assert_eq!(
            set_display_name(root, "worker", "h", None).unwrap().result,
            AuthorOutcome::Unchanged
        );
        assert_eq!(task_ids(root), initial_task_ids);
    }

    #[test]
    fn qualified_identity_resolves_ambiguity_and_retired_remote_agents_are_authorable() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", false),
        );
        write(
            root,
            "remote/worker/agent.kdl",
            &declaration("worker", "remote", true),
        );

        let error = set_display_name(root, "worker", "h", Some("ambiguous")).unwrap_err();
        assert_eq!(error.code(), "target-ambiguous");

        let remote = set_display_name(root, "remote.worker", "h", Some("Retired owner")).unwrap();
        assert_eq!(remote.identity, "remote.worker");
        assert!(remote.retired);
        assert_eq!(
            fs::read_to_string(root.join("remote/worker/name")).unwrap(),
            "Retired owner\n"
        );
        assert!(!root.join("h/worker/name").exists());
    }

    #[test]
    fn invalid_names_and_unsafe_paths_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/worker/agent.kdl",
            &declaration("worker", "h", false),
        );

        for invalid in ["", " leading", "trailing ", "two\nlines", "tab\tname"] {
            let error = set_display_name(root, "worker", "h", Some(invalid)).unwrap_err();
            assert_eq!(error.code(), "invalid-display-name");
        }
        let too_long = "x".repeat(DISPLAY_NAME_MAX_CHARS + 1);
        assert_eq!(
            set_display_name(root, "worker", "h", Some(&too_long))
                .unwrap_err()
                .code(),
            "invalid-display-name"
        );

        fs::create_dir(root.join("h/worker/name")).unwrap();
        assert_eq!(
            set_display_name(root, "worker", "h", Some("safe"))
                .unwrap_err()
                .code(),
            "unsafe-name-path"
        );
    }

    #[test]
    fn retirement_replaces_only_the_selected_false_value_and_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let path = root.join("fleet.kdl");
        let original = r#"// fleet heading
agent "other" {
  host "h"
  retired #false // unrelated intent
  command "sleep 60"
}

agent "worker" { // selected source stays put
  host "h"
  role "builder" // preserve this comment
  retired #false // only these six bytes change
  command "sleep 60"
}
"#;
        fs::write(&path, original).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();

        assert_eq!(
            retire_declaration(&path, "h.worker", || {}).unwrap(),
            RetireOutcome::Authored
        );
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            original.replacen(
                "  retired #false // only these six bytes change",
                "  retired #true // only these six bytes change",
                1
            )
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        assert_eq!(
            retire_declaration(&path, "h.worker", || {}).unwrap(),
            RetireOutcome::Unchanged
        );

        let already = root.join("already.kdl");
        let already_source = "agent \"done\" { host \"h\"; retired #true; command \"sleep 60\" }\n";
        fs::write(&already, already_source).unwrap();
        assert_eq!(
            retire_declaration(&already, "h.done", || {}).unwrap(),
            RetireOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(already).unwrap(), already_source);
    }

    #[test]
    fn retirement_inserts_omitted_intent_without_reformatting_multiline_or_inline_source() {
        let temporary = tempfile::tempdir().unwrap();
        let multiline = temporary.path().join("multiline.kdl");
        let multiline_original = r#"agent "worker" {
  host "h"
  // lifecycle intent belongs above this comment too
  command "sleep 60"
}
"#;
        fs::write(&multiline, multiline_original).unwrap();
        retire_declaration(&multiline, "h.worker", || {}).unwrap();
        assert_eq!(
            fs::read_to_string(&multiline).unwrap(),
            r#"agent "worker" {
  host "h"
  // lifecycle intent belongs above this comment too
  command "sleep 60"
  retired #true
}
"#
        );

        let inline = temporary.path().join("inline.kdl");
        fs::write(
            &inline,
            r#"agent "inline" { host "h"; command "sleep 60" }"#,
        )
        .unwrap();
        retire_declaration(&inline, "h.inline", || {}).unwrap();
        assert_eq!(
            fs::read_to_string(&inline).unwrap(),
            r#"agent "inline" { host "h"; command "sleep 60"; retired #true }"#
        );
    }

    #[test]
    fn retirement_refuses_malformed_intent_and_a_source_change_before_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let malformed = temporary.path().join("malformed.kdl");
        fs::write(
            &malformed,
            "agent \"worker\" { host \"h\"; retired \"yes\"; command \"sleep 60\" }\n",
        )
        .unwrap();
        assert_eq!(
            retire_declaration(&malformed, "h.worker", || {})
                .unwrap_err()
                .code(),
            "malformed-retirement-intent"
        );

        let raced = temporary.path().join("raced.kdl");
        let original = "agent \"worker\" { host \"h\"; retired #false; command \"sleep 60\" }\n";
        let concurrent =
            "agent \"worker\" { host \"h\"; retired #false; role \"new\"; command \"sleep 60\" }\n";
        fs::write(&raced, original).unwrap();
        let error = retire_declaration(&raced, "h.worker", || {
            fs::write(&raced, concurrent).unwrap();
        })
        .unwrap_err();
        assert_eq!(error.code(), "source-changed");
        assert_eq!(fs::read_to_string(&raced).unwrap(), concurrent);
        assert!(
            fs::read_dir(temporary.path())
                .unwrap()
                .flatten()
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".agent.kdl.retire-"))
        );
    }

    #[test]
    fn retirement_requires_canonical_kdl_and_refuses_duplicate_targets() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "h/worker/agent.toml",
            "identity = \"worker\"\nhost = \"h\"\ncommand = \"sleep 60\"\n",
        );
        assert_eq!(
            retire_agent(root, "h.worker", "h").unwrap_err().code(),
            "unsupported-declaration-format"
        );

        fs::remove_file(root.join("h/worker/agent.toml")).unwrap();
        write(root, "first/agent.kdl", &declaration("worker", "h", false));
        write(root, "second/agent.kdl", &declaration("worker", "h", false));
        assert_eq!(
            retire_agent(root, "h.worker", "h").unwrap_err().code(),
            "target-ambiguous"
        );
    }

    #[test]
    fn retirement_requires_an_exact_selector_and_refuses_a_writer_collision() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "unusual/place/seat.kdl",
            &declaration("worker", "h", false),
        );

        assert_eq!(
            retire_agent(root, "worker", "h").unwrap_err().code(),
            "exact-target-required"
        );

        let _held = DirectoryWriterFence::acquire(root).unwrap();
        assert_eq!(
            retire_agent(root, "h.worker", "h").unwrap_err().code(),
            "writer-collision"
        );
        assert_eq!(
            fs::read_to_string(root.join("unusual/place/seat.kdl")).unwrap(),
            declaration("worker", "h", false)
        );
    }
}
