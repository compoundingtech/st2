//! Discovery — slurp a unified catalog+inbox folder and lower every agent spec it contains.
//!
//! The folder is *both* the catalog and the inboxes: one directory per agent holding its spec plus
//! its `inbox/`/`archive/`. Discovery walks `<root>` recursively, parses every `*.{toml,json,kdl}`
//! that looks like a spec, and resolves each spec's `identity`/`host` with the gist's precedence:
//! **content wins, the path supplies defaults, a mismatch is a warning** (a convention, not a
//! constraint). Malformed files are collected as errors rather than halting the walk — one bad edit
//! must not wedge the whole reconcile.

use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::spec::{AgentSpec, RawSpec};

/// The result of walking a catalog folder. Sorted + deterministic.
#[derive(Debug, Default)]
pub struct Discovered {
    /// Every spec that parsed and resolved an identity, sorted by path.
    pub specs: Vec<AgentSpec>,
    /// Non-fatal notes (identity/host path↔content mismatches, formats not yet supported).
    pub warnings: Vec<String>,
    /// Files that looked like specs but failed to parse/resolve — surfaced, never silently dropped.
    pub errors: Vec<SpecError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    pub path: PathBuf,
    pub message: String,
}

/// Extensions discovery will attempt to parse as specs.
const SPEC_EXTS: [&str; 3] = ["toml", "json", "kdl"];

/// Walk `root` recursively and lower every agent spec found. Returns empty (no error) when `root`
/// does not exist yet — a fresh, un-seeded folder is a valid state.
pub fn discover(root: &Path) -> Discovered {
    let mut out = Discovered::default();
    let mut files = Vec::new();
    collect_spec_files(root, root, &mut files);
    files.sort();
    for path in files {
        match load_specs(root, &path) {
            Ok((specs, warnings)) => {
                out.specs.extend(specs);
                out.warnings.extend(warnings);
            }
            Err(e) => out.errors.push(SpecError {
                path,
                message: e.to_string(),
            }),
        }
    }
    out
}

/// Recursively gather candidate spec files, skipping dotfiles/dotdirs (`.git`, hidden config), the
/// top-level `pty/` runtime registry, and anything that isn't one of [`SPEC_EXTS`]. `pty` session
/// metadata includes JSON that can resemble an agent spec; it is runner state, never catalog input.
/// Unreadable directories are skipped, not fatal.
fn collect_spec_files(root: &Path, dir: &Path, acc: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue; // skip hidden files and directories
        }
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_dir() {
            if path == root.join("pty")
                || name == "resources"
                || name == "archive"
                || name == "inbox"
            {
                continue;
            }
            collect_spec_files(root, &path, acc);
        } else if ft.is_file()
            && let Some(ext) = path.extension().and_then(|e| e.to_str())
            && SPEC_EXTS.contains(&ext)
        {
            acc.push(path);
        }
    }
}

/// Parse a spec file into its raw (pre-resolution) shape — one per `agent` node for KDL, 0-or-1 for
/// TOML/JSON. Non-spec extensions yield an empty vec. Shared by discovery and `validate` (which needs
/// the *raw* `type` string before it is normalized away, to catch a typo'd `type = "srvice"`).
pub(crate) fn parse_raw_file(path: &Path) -> anyhow::Result<Vec<RawSpec>> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let text = fs::read_to_string(path)?;
    Ok(match ext {
        "toml" => {
            vec![toml::from_str(&text).map_err(|e| anyhow::anyhow!("TOML parse error: {e}"))?]
        }
        "json" => vec![
            serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("JSON parse error: {e}"))?,
        ],
        "kdl" => crate::kdl_format::parse_kdl(&text)?,
        _ => Vec::new(),
    })
}

/// Parse one file into `(specs, warnings)`. TOML/JSON yield 0-or-1 spec; KDL yields one per `agent`
/// node. Non-spec files yield an empty vec; a malformed file is an `Err` (collected, never fatal).
fn load_specs(root: &Path, path: &Path) -> anyhow::Result<(Vec<AgentSpec>, Vec<String>)> {
    let raws = parse_raw_file(path)?;

    let mut specs = Vec::new();
    let mut warnings = Vec::new();
    for raw in raws {
        if let Some((spec, mut warns)) = resolve_spec(root, path, raw)? {
            specs.push(spec);
            warnings.append(&mut warns);
        }
    }
    Ok((specs, warnings))
}

/// Apply the gist's identity/host precedence to one raw spec: content wins, path supplies defaults, a
/// mismatch warns. Returns `None` for a non-spec (no agent signal); `Err` when it looks like a spec
/// but no identity can be resolved from content or path.
fn resolve_spec(
    root: &Path,
    path: &Path,
    raw: RawSpec,
) -> anyhow::Result<Option<(AgentSpec, Vec<String>)>> {
    if !raw.looks_like_spec() {
        return Ok(None); // random config in the tree — not an agent spec
    }

    let (path_identity, path_host) = path_defaults(root, path);
    let mut warnings = Vec::new();

    let identity = match (raw.identity.clone(), path_identity) {
        (Some(c), Some(p)) if c != p => {
            warnings.push(format!(
                "{}: identity mismatch — content '{c}' vs path '{p}'; using content",
                path.display()
            ));
            c
        }
        (Some(c), _) => c,
        (None, Some(p)) => p,
        (None, None) => {
            anyhow::bail!(
                "{}: spec has no identity in content or path",
                path.display()
            )
        }
    };

    let host = match (raw.host.clone(), path_host) {
        (Some(c), Some(p)) if c != p => {
            warnings.push(format!(
                "{}: host mismatch — content '{c}' vs path '{p}'; using content",
                path.display()
            ));
            Some(c)
        }
        (Some(c), _) => Some(c),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    };

    let spec = raw.into_agent_spec(identity, host, path.to_path_buf());
    Ok(Some((spec, warnings)))
}

/// Derive `(identity, host)` defaults from the file's path under `root`.
///
/// The canonical layout is `<root>/<host>/<identity>/agent.<ext>` (a generic `agent` filename), but
/// a file named for the agent (`<root>/[host/]<identity>.<ext>`) works too. The rule: a generic
/// `agent` stem takes identity from its parent dir and host from its grandparent; any other stem is
/// itself the identity and takes host from its parent dir. Content still overrides both.
///
/// Exposed for `validate`, which re-derives the path defaults to reconstruct an identity/host
/// path↔content mismatch structurally (the resolved [`AgentSpec`] no longer records which source won).
pub fn path_defaults(root: &Path, file: &Path) -> (Option<String>, Option<String>) {
    let rel = file.strip_prefix(root).unwrap_or(file);
    let stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let dirs: Vec<String> = rel
        .parent()
        .map(|p| {
            p.components()
                .filter_map(|c| match c {
                    Component::Normal(s) => s.to_str().map(|s| s.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    if stem == "agent" {
        // Generic filename: identity = last dir, host = second-to-last dir.
        let identity = dirs.last().cloned();
        let host = if dirs.len() >= 2 {
            Some(dirs[dirs.len() - 2].clone())
        } else {
            None
        };
        (identity, host)
    } else {
        // File named for the agent: identity = stem, host = its parent dir (if any).
        let identity = if stem.is_empty() {
            None
        } else {
            Some(stem.to_string())
        };
        let host = dirs.last().cloned();
        (identity, host)
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    fn pd(root: &str, file: &str) -> (Option<String>, Option<String>) {
        path_defaults(Path::new(root), Path::new(file))
    }

    #[test]
    fn canonical_layout_host_identity_agent() {
        assert_eq!(
            pd("/cat", "/cat/hetz/st2-claude/agent.kdl"),
            (Some("st2-claude".into()), Some("hetz".into()))
        );
    }

    #[test]
    fn identity_folder_generic_name_no_host() {
        assert_eq!(
            pd("/cat", "/cat/fabric-claude/agent.toml"),
            (Some("fabric-claude".into()), None)
        );
    }

    #[test]
    fn flat_file_named_for_agent() {
        assert_eq!(
            pd("/cat", "/cat/fabric-claude.toml"),
            (Some("fabric-claude".into()), None)
        );
    }

    #[test]
    fn agent_named_file_inside_host_folder() {
        assert_eq!(
            pd("/cat", "/cat/hetz/fabric-claude.toml"),
            (Some("fabric-claude".into()), Some("hetz".into()))
        );
    }
}
