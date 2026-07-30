//! Explicit authoring operations over one exact catalog agent.
//!
//! These operations change catalog-owned presentation/lifecycle state, never a running task.
//! Target resolution is content-based and refuses ambiguity rather than depending on folder names.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
        directory,
        retired: spec.retired,
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
}
