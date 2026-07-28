//! The catalog's own declaration — `<catalog>/catalog.kdl`.
//!
//! Every other file in a catalog describes an agent; this one describes the folder. Only the session
//! registry is declarable today:
//!
//! ```kdl
//! catalog {
//!   pty-root "/run/agents/pty"
//! }
//! ```
//!
//! It is deliberately not a spec. `catalog` is not an `agent` node, so discovery lowers nothing from
//! it, and `eval_spec::parse_spec` rejects `catalog` as a top-level node, so a catalog that declares
//! a root is still dispatched as a catalog and never mistaken for a single-file team spec.

use std::path::{Path, PathBuf};

use kdl::KdlDocument;

/// The catalog-level declaration, read from the catalog root.
pub const CONFIG_FILE: &str = "catalog.kdl";

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
        let Some(children) = node.children() else {
            continue;
        };
        for child in children.nodes() {
            match child.name().value() {
                "pty-root" => {
                    let value = child
                        .get(0)
                        .and_then(|v| v.as_string())
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            anyhow::anyhow!("pty-root needs a non-empty path, e.g. pty-root \"/run/agents/pty\"")
                        })?;
                    config.pty_root = Some(value.to_string());
                }
                other => anyhow::bail!("unknown catalog field '{other}' (expected pty-root)"),
            }
        }
    }
    Ok(config)
}

/// Read `<catalog>/catalog.kdl`. A missing file is the default declaration, not an error.
pub fn load(catalog_root: &Path) -> anyhow::Result<CatalogConfig> {
    match std::fs::read_to_string(config_path(catalog_root)) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CatalogConfig::default()),
        Err(e) => Err(e.into()),
    }
}

/// The session registry the CATALOG itself declares: `pty-root` if it declares one, else the native
/// `<catalog>/pty`. This is what `st2 env`/`st2 pty`/`st2 shell` hand to bus-aware tools, so those
/// describe the catalog rather than whatever registry the caller happens to be standing in.
///
/// A malformed declaration falls back to the default instead of failing: this runs on every spawn,
/// list, and kill, including teardown, and inventing a root there is worse than using the native
/// one. `st2 validate` is where a bad declaration is reported.
pub fn pty_root(catalog_root: &Path) -> PathBuf {
    match load(catalog_root).ok().and_then(|c| c.pty_root) {
        // Join, so a relative declaration anchors at the catalog instead of the caller's cwd.
        Some(declared) => catalog_root.join(crate::expand::expand_catalog(&declared, catalog_root)),
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
}
