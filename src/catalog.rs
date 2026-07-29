//! Session-registry declarations for one catalog.
//!
//! The portable shared fallback lives at `<catalog>/catalog.kdl`; the selected host may override it
//! at `<catalog>/agents/<host>/config.kdl`:
//!
//! ```kdl
//! catalog {
//!   pty-root "$CATALOG/../shared-pty"
//! }
//! ```
//!
//! Neither declaration is an agent spec: discovery reserves the exact host-config path, and
//! `eval_spec::parse_spec` rejects `catalog` as a top-level team node.

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use kdl::KdlDocument;

/// The catalog-level declaration, read from the catalog root.
pub const CONFIG_FILE: &str = "catalog.kdl";
/// The matching host's declaration at `<catalog>/agents/<host>/config.kdl`.
pub const HOST_CONFIG_FILE: &str = "config.kdl";

/// What `<catalog>/catalog.kdl` declares. An absent file leaves every field `None`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CatalogConfig {
    /// The `pty` session registry holding this catalog's tasks. Relative values anchor at the
    /// catalog root; `$VAR`/`$CATALOG` are expanded at use.
    pub pty_root: Option<String>,
}

/// `<catalog>/catalog.kdl`.
pub fn config_path(catalog_root: &Path) -> PathBuf {
    catalog_root.join(CONFIG_FILE)
}

/// Reject a host selector that could escape `<catalog>/agents/<host>`.
pub fn validate_host(host: &str) -> anyhow::Result<()> {
    let mut components = Path::new(host).components();
    if host.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        anyhow::bail!("host must be one non-empty path segment, got '{host}'");
    }
    Ok(())
}

/// `<catalog>/agents/<host>/config.kdl`.
pub fn host_config_path(catalog_root: &Path, host: &str) -> anyhow::Result<PathBuf> {
    validate_host(host)?;
    Ok(catalog_root
        .join("agents")
        .join(host)
        .join(HOST_CONFIG_FILE))
}

/// Every declared host config, sorted by host.
pub fn host_config_paths(catalog_root: &Path) -> Vec<(String, PathBuf)> {
    let mut paths = Vec::new();
    let Ok(entries) = std::fs::read_dir(catalog_root.join("agents")) else {
        return paths;
    };
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Some(host) = entry.file_name().to_str().map(String::from) else {
            continue;
        };
        let path = entry.path().join(HOST_CONFIG_FILE);
        if path.is_file() {
            paths.push((host, path));
        }
    }
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    paths
}

fn parse_pty_root(
    node: &kdl::KdlNode,
    block: &str,
    exact_shape: bool,
) -> anyhow::Result<Option<String>> {
    if exact_shape && !node.entries().is_empty() {
        anyhow::bail!("{block} block takes no arguments or properties");
    }
    let mut pty_root = None;
    let Some(children) = node.children() else {
        return Ok(pty_root);
    };
    for child in children.nodes() {
        match child.name().value() {
            "pty-root" => {
                if exact_shape && pty_root.is_some() {
                    anyhow::bail!("pty-root declared more than once in {block} block");
                }
                let value = if exact_shape {
                    let entries = child.entries();
                    (entries.len() == 1
                        && entries[0].name().is_none()
                        && child.children().is_none())
                    .then(|| entries[0].value().as_string())
                    .flatten()
                } else {
                    // Preserve the established root-catalog parser: its first positional value
                    // wins even if a renderer carried extra node metadata.
                    child.get(0).and_then(|entry| entry.as_string())
                }
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "pty-root needs exactly one non-empty path, e.g. pty-root \"$CATALOG/pty\""
                    )
                })?;
                pty_root = Some(value.to_string());
            }
            other => anyhow::bail!("unknown {block} field '{other}' (expected pty-root)"),
        }
    }
    Ok(pty_root)
}

/// Parse a catalog declaration.
///
/// An unknown child of `catalog{}` is an error rather than ignored: a typo'd `pty_root` would
/// silently resolve back to `<catalog>/pty` and reappear as a live agent whose task reads dead —
/// exactly the split registry this declaration exists to prevent. Its value set is closed, so the
/// lint cannot fire on a render-only field st2 ignores by design.
///
/// Top-level nodes other than `catalog` are left alone: the same file may legitimately hold `agent`
/// nodes, which discovery owns.
pub fn parse(text: &str) -> anyhow::Result<CatalogConfig> {
    let doc = KdlDocument::parse(text).map_err(|e| anyhow::anyhow!("KDL parse error: {e}"))?;
    let mut config = CatalogConfig::default();
    let mut seen = false;

    for node in doc.nodes().iter().filter(|n| n.name().value() == "catalog") {
        if seen {
            anyhow::bail!("catalog block declared more than once");
        }
        seen = true;
        config.pty_root = parse_pty_root(node, "catalog", false)?;
    }
    Ok(config)
}

/// Parse the closed `host { pty-root "…" }` declaration used only by
/// `<catalog>/agents/<host>/config.kdl`. The folder is the host key; the node takes no host argument.
pub fn parse_host(text: &str) -> anyhow::Result<CatalogConfig> {
    let doc = KdlDocument::parse(text).map_err(|e| anyhow::anyhow!("KDL parse error: {e}"))?;
    if doc.nodes().len() != 1 || doc.nodes()[0].name().value() != "host" {
        anyhow::bail!("host config needs exactly one `host {{ ... }}` block");
    }
    let pty_root = parse_pty_root(&doc.nodes()[0], "host", true)?
        .ok_or_else(|| anyhow::anyhow!("host block needs pty-root"))?;
    Ok(CatalogConfig {
        pty_root: Some(pty_root),
    })
}

/// Read `<catalog>/catalog.kdl`. A missing file is the default declaration, not an error.
pub fn load(catalog_root: &Path) -> anyhow::Result<CatalogConfig> {
    match std::fs::read_to_string(config_path(catalog_root)) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CatalogConfig::default()),
        Err(e) => Err(e.into()),
    }
}

/// Read the exact matching host declaration. A missing file means this precedence layer is absent.
pub fn load_host(catalog_root: &Path, host: &str) -> anyhow::Result<Option<CatalogConfig>> {
    let path = host_config_path(catalog_root, host)?;
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_host(&text).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Which precedence layer selected a session registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PtyRootSource {
    Ambient,
    HostConfig,
    CatalogConfig,
    NativeDefault,
}

impl PtyRootSource {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ambient => "ambient PTY_ROOT",
            Self::HostConfig => "matching host config",
            Self::CatalogConfig => "shared catalog config",
            Self::NativeDefault => "native default",
        }
    }
}

/// The selected registry and the layer that selected it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyRootResolution {
    pub path: PathBuf,
    pub source: PtyRootSource,
}

fn declared_pty_root(catalog_root: &Path, value: &str) -> PathBuf {
    catalog_root.join(crate::expand::expand_catalog(value, catalog_root))
}

/// Resolve the registry for one selected host.
///
/// Stronger layers short-circuit before weaker files are read, so an explicit ephemeral override is
/// not blocked by an unused malformed config, and a valid host config is not blocked by a malformed
/// shared fallback.
pub fn resolve_pty_root(
    catalog_root: &Path,
    host: &str,
    ambient: Option<&OsStr>,
) -> anyhow::Result<PtyRootResolution> {
    validate_host(host)?;
    if let Some(value) = ambient.filter(|value| !value.is_empty()) {
        return Ok(PtyRootResolution {
            path: PathBuf::from(value),
            source: PtyRootSource::Ambient,
        });
    }
    if let Some(config) = load_host(catalog_root, host)? {
        return Ok(PtyRootResolution {
            path: declared_pty_root(
                catalog_root,
                config
                    .pty_root
                    .as_deref()
                    .expect("closed host config requires pty-root"),
            ),
            source: PtyRootSource::HostConfig,
        });
    }
    if let Some(value) = load(catalog_root)?.pty_root {
        return Ok(PtyRootResolution {
            path: declared_pty_root(catalog_root, &value),
            source: PtyRootSource::CatalogConfig,
        });
    }
    Ok(PtyRootResolution {
        path: catalog_root.join("pty"),
        source: PtyRootSource::NativeDefault,
    })
}

/// The legacy root-catalog fallback: root `pty-root` if declared, else `<catalog>/pty`.
///
/// Host-aware commands use [`resolve_pty_root`] and pin that result into their runtime environment;
/// this helper remains for library callers that have no host selector.
///
/// A malformed declaration falls back to the default instead of failing: this runs on every spawn,
/// list, and kill, including teardown, and inventing a root there is worse than using the native
/// one. `st2 validate` is where a bad declaration is reported.
pub fn pty_root(catalog_root: &Path) -> PathBuf {
    match load(catalog_root).ok().and_then(|c| c.pty_root) {
        // Join, so a relative declaration anchors at the catalog instead of the caller's cwd.
        Some(declared) => declared_pty_root(catalog_root, &declared),
        None => catalog_root.join("pty"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_undeclared_catalog_keeps_the_native_root() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(load(tmp.path()).unwrap(), CatalogConfig::default());
        assert_eq!(pty_root(tmp.path()), tmp.path().join("pty"));

        // A file that declares other things, but no pty root.
        std::fs::write(config_path(tmp.path()), "agent \"a\" { command \"true\" }\n").unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("pty"));
    }

    #[test]
    fn a_declared_root_is_expanded_and_anchored_at_the_catalog() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            config_path(tmp.path()),
            "catalog {\n  pty-root \"/run/agents/pty\"\n}\n",
        )
        .unwrap();
        assert_eq!(pty_root(tmp.path()), PathBuf::from("/run/agents/pty"));

        std::fs::write(config_path(tmp.path()), "catalog { pty-root \"$CATALOG/../shared\" }\n")
            .unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("../shared"));

        // A relative value belongs to the catalog, never to the caller's cwd.
        std::fs::write(config_path(tmp.path()), "catalog { pty-root \"registry\" }\n").unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("registry"));
    }

    #[test]
    fn a_mistyped_declaration_is_an_error_not_a_silent_default() {
        assert!(parse("catalog { pty_root \"/run/agents/pty\" }").is_err());
        assert!(parse("catalog { pty-root }").is_err());
        assert!(parse("catalog { pty-root \"\" }").is_err());
        assert!(parse("catalog { pty-root \"/a\" }\ncatalog { pty-root \"/b\" }").is_err());
        assert!(parse("this is (not kdl").is_err());

        // Reported by `st2 validate`; the runtime path stays on the native root.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(config_path(tmp.path()), "catalog { pty_root \"/run/agents/pty\" }\n")
            .unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("pty"));
    }

    #[test]
    fn host_config_is_closed_and_uses_its_folder_as_the_only_host_key() {
        assert_eq!(
            parse_host("host { pty-root \"$CATALOG/pty\" }\n").unwrap(),
            CatalogConfig {
                pty_root: Some("$CATALOG/pty".into())
            }
        );
        assert!(parse_host("host \"local\" { pty-root \"pty\" }\n").is_err());
        assert!(parse_host("catalog { pty-root \"pty\" }\n").is_err());
        assert!(parse_host("config { pty-root \"pty\" }\n").is_err());
        assert!(parse_host("host { pty_root \"pty\" }\n").is_err());
        assert!(parse_host("host { pty-root \"a\"; pty-root \"b\" }\n").is_err());
        assert!(parse_host("host {}\n").is_err());
    }

    #[test]
    fn pty_root_precedence_short_circuits_unused_lower_layers() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let host_path = host_config_path(root, "local").unwrap();
        std::fs::create_dir_all(host_path.parent().unwrap()).unwrap();

        // Strongest: an explicit ephemeral override does not read malformed lower config.
        std::fs::write(&host_path, "not kdl").unwrap();
        std::fs::write(config_path(root), "also not kdl").unwrap();
        assert_eq!(
            resolve_pty_root(root, "local", Some(OsStr::new("/tmp/eval-pty"))).unwrap(),
            PtyRootResolution {
                path: PathBuf::from("/tmp/eval-pty"),
                source: PtyRootSource::Ambient,
            }
        );

        // Matching host: a valid host root isolates the malformed shared fallback.
        std::fs::write(&host_path, "host { pty-root \"$CATALOG/host-pty\" }\n").unwrap();
        assert_eq!(
            resolve_pty_root(root, "local", None).unwrap(),
            PtyRootResolution {
                path: root.join("host-pty"),
                source: PtyRootSource::HostConfig,
            }
        );

        // Shared fallback, then native default.
        std::fs::remove_file(&host_path).unwrap();
        std::fs::write(config_path(root), "catalog { pty-root \"shared-pty\" }\n").unwrap();
        assert_eq!(
            resolve_pty_root(root, "local", None).unwrap(),
            PtyRootResolution {
                path: root.join("shared-pty"),
                source: PtyRootSource::CatalogConfig,
            }
        );
        std::fs::remove_file(config_path(root)).unwrap();
        assert_eq!(
            resolve_pty_root(root, "local", None).unwrap(),
            PtyRootResolution {
                path: root.join("pty"),
                source: PtyRootSource::NativeDefault,
            }
        );
    }

    #[test]
    fn malformed_needed_layer_fails_closed_and_host_cannot_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let host_path = host_config_path(root, "local").unwrap();
        std::fs::create_dir_all(host_path.parent().unwrap()).unwrap();
        std::fs::write(&host_path, "host { pty_root \"wrong\" }\n").unwrap();
        assert!(resolve_pty_root(root, "local", None).is_err());

        std::fs::remove_file(&host_path).unwrap();
        std::fs::write(config_path(root), "catalog { pty_root \"wrong\" }\n").unwrap();
        assert!(resolve_pty_root(root, "local", None).is_err());
        assert!(resolve_pty_root(root, "../local", None).is_err());
        assert!(host_config_path(root, "a/b").is_err());
        assert!(host_config_path(root, "").is_err());
    }
}
