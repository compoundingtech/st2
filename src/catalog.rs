//! The catalog's own declaration — `<catalog>/catalog.kdl`.
//!
//! Every other file in a catalog describes an agent; this one describes the folder. Two things
//! are declarable today: the session registry and resource profiles —
//!
//! ```kdl
//! catalog {
//!   pty-root "/run/agents/pty"
//! }
//!
//! // One wasm resolver per URI scheme; `class` (optional, default coalesced) decides how
//! // carriers resolved through the profile notify. Paths anchor at the catalog root.
//! profile "dev.schickling.agent-goal" {
//!   wasm "resolvers/goal.wasm"
//!   class "immediate"
//! }
//! ```
//!
//! It is deliberately not a spec. `catalog` and `profile` are not `agent` nodes, so discovery
//! lowers nothing from them, and `eval_spec::parse_spec` rejects both as top-level nodes, so a
//! catalog that declares them is still dispatched as a catalog and never mistaken for a
//! single-file team spec.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use agent_spec::profile::{ProfileClass, ResourceProfile, ResourceProfileRegistry};
use kdl::KdlDocument;
/// The catalog-level declaration, read from the catalog root.
pub const CONFIG_FILE: &str = "catalog.kdl";
/// One declared resource profile: `profile "<scheme>" { wasm "<path>" class "..." }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredProfile {
    /// The URI scheme this profile resolves.
    pub scheme: String,
    /// Path of the resolver `.wasm`, anchored at the catalog root when relative.
    pub wasm: String,
    /// How carriers resolved through this profile notify; defaults to coalesced.
    pub class: ProfileClass,
}

/// What `<catalog>/catalog.kdl` declares. An absent file leaves every field empty.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CatalogConfig {
    /// The `pty` session registry holding this catalog's tasks. Relative values anchor at the
    /// catalog root; `$VAR`/`$CATALOG` are expanded at use.
    pub pty_root: Option<String>,
    /// Resource profiles in declaration order.
    pub profiles: Vec<DeclaredProfile>,
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
/// Top-level nodes other than `catalog` and `profile` are left alone: the same file may
/// legitimately hold `agent` nodes, which discovery owns.
pub fn parse(text: &str) -> anyhow::Result<CatalogConfig> {
    let doc = KdlDocument::parse(text).map_err(|e| anyhow::anyhow!("KDL parse error: {e}"))?;
    let mut config = CatalogConfig::default();
    let mut seen_catalog = false;
    let mut seen_schemes = BTreeSet::new();

    for node in doc.nodes() {
        match node.name().value() {
            "catalog" => {
                if seen_catalog {
                    anyhow::bail!("catalog block declared more than once");
                }
                seen_catalog = true;
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
                                    anyhow::anyhow!(
                                        "pty-root needs a non-empty path, e.g. pty-root \"/run/agents/pty\""
                                    )
                                })?;
                            config.pty_root = Some(value.to_string());
                        }
                        other => {
                            anyhow::bail!("unknown catalog field '{other}' (expected pty-root)")
                        }
                    }
                }
            }
            "profile" => {
                let profile = parse_profile(node)?;
                if !seen_schemes.insert(profile.scheme.clone()) {
                    anyhow::bail!(
                        "profile '{}' declared more than once",
                        profile.scheme
                    );
                }
                config.profiles.push(profile);
            }
            _ => {}
        }
    }
    Ok(config)
}

fn parse_profile(node: &kdl::KdlNode) -> anyhow::Result<DeclaredProfile> {
    if node.entries().len() != 1 {
        anyhow::bail!("profile takes exactly one quoted URI scheme and no properties");
    }
    let scheme = node
        .get(0)
        .and_then(|v| v.as_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "profile needs a non-empty URI scheme, e.g. \
                 profile \"dev.example.goal\" {{ wasm \"resolvers/goal.wasm\" }}"
            )
        })?;
    let scheme_ok = !scheme.contains('/')
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    if !scheme_ok {
        anyhow::bail!("profile '{scheme}' is not a valid URI scheme");
    }

    let Some(children) = node.children() else {
        anyhow::bail!("profile '{scheme}' needs a wasm child naming its resolver module");
    };
    let mut wasm: Option<String> = None;
    let mut class = ProfileClass::Coalesced;
    let mut seen_class = false;
    for child in children.nodes() {
        // KDL folds `wasm "a.wasm" class "immediate"` written without separators into ONE node
        // with extra positional entries — reject anything beyond the single expected argument
        // so a run-on line fails loudly instead of parsing as something else.
        if child.entries().len() != 1 {
            anyhow::bail!(
                "profile '{scheme}': '{}' takes exactly one quoted value",
                child.name().value()
            );
        }
        match child.name().value() {
            "wasm" => {
                if wasm.is_some() {
                    anyhow::bail!("profile '{scheme}' declares wasm more than once");
                }
                let value = child
                    .get(0)
                    .and_then(|v| v.as_string())
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "profile '{scheme}' needs a non-empty module path, e.g. \
                             wasm \"resolvers/goal.wasm\""
                        )
                    })?;
                wasm = Some(value.to_string());
            }
            "class" => {
                if seen_class {
                    anyhow::bail!("profile '{scheme}' declares class more than once");
                }
                seen_class = true;
                let value = child.get(0).and_then(|v| v.as_string()).unwrap_or("");
                class = ProfileClass::parse(value).ok_or_else(|| {
                    anyhow::anyhow!(
                        "profile '{scheme}': unknown class '{value}' (expected immediate, coalesced, or silent)"
                    )
                })?;
            }
            other => anyhow::bail!(
                "unknown profile field '{other}' in profile '{scheme}' (expected wasm or class)"
            ),
        }
    }
    let Some(wasm) = wasm else {
        anyhow::bail!("profile '{scheme}' needs a wasm child naming its resolver module");
    };
    Ok(DeclaredProfile {
        scheme: scheme.to_owned(),
        wasm,
        class,
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

/// The resource profiles the CATALOG itself declares, as an injectable registry for the resync
/// supervisor. Relative `wasm` paths anchor at the catalog root; `$CATALOG`/`$VAR` expand like
/// every catalog-anchored declaration.
///
/// Unlike [`pty_root`], a malformed declaration is an ERROR here, not a fallback: profile blocks
/// gate watchability of agent resources, and silently dropping one would hide the misconfiguration
/// behind "nothing fires". `st2 up` surfaces this before spawning; `st2 validate` reports it.
pub fn declared_profiles(catalog_root: &Path) -> anyhow::Result<ResourceProfileRegistry> {
    let config = load(catalog_root)?;
    Ok(ResourceProfileRegistry::empty().with_profiles(config.profiles.into_iter().map(
        |declared| {
            let expanded =
                PathBuf::from(crate::expand::expand_catalog(&declared.wasm, catalog_root));
            let module = if expanded.is_absolute() {
                expanded
            } else {
                // Join, so a relative declaration anchors at the catalog instead of the cwd.
                catalog_root.join(expanded)
            };
            ResourceProfile::wasm(declared.scheme, module, declared.class)
        },
    )))
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
        std::fs::write(
            config_path(tmp.path()),
            "agent \"a\" { command \"true\" }\n",
        )
        .unwrap();
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

        std::fs::write(
            config_path(tmp.path()),
            "catalog { pty-root \"$CATALOG/../shared\" }\n",
        )
        .unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("../shared"));

        // A relative value belongs to the catalog, never to the caller's cwd.
        std::fs::write(
            config_path(tmp.path()),
            "catalog { pty-root \"registry\" }\n",
        )
        .unwrap();
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
        std::fs::write(
            config_path(tmp.path()),
            "catalog { pty_root \"/run/agents/pty\" }\n",
        )
        .unwrap();
        assert_eq!(pty_root(tmp.path()), tmp.path().join("pty"));
    }

    #[test]
    fn profile_blocks_parse_with_default_and_declared_classes() {
        let config = parse(
            r#"
            profile "dev.example.goal" { wasm "resolvers/goal.wasm" }
            profile "dev.example.tree" {
              wasm "/abs/resolvers/tree.wasm"
              class "silent"
            }
            "#,
        )
        .unwrap();
        assert_eq!(
            config.profiles,
            vec![
                DeclaredProfile {
                    scheme: "dev.example.goal".into(),
                    wasm: "resolvers/goal.wasm".into(),
                    class: ProfileClass::Coalesced,
                },
                DeclaredProfile {
                    scheme: "dev.example.tree".into(),
                    wasm: "/abs/resolvers/tree.wasm".into(),
                    class: ProfileClass::Silent,
                },
            ]
        );
    }

    #[test]
    fn malformed_profile_blocks_fail_validation_loudly() {
        let loud = [
            // missing scheme / module
            r#"profile { wasm "x.wasm" }"#,
            r#"profile "dev.x" { class "immediate" }"#,
            r#"profile "dev.x""#,
            r#"profile "dev.x" "extra" { wasm "x.wasm" }"#,
            r#"profile "dev.x" class="silent" { wasm "x.wasm" }"#,
            // invalid class value and duplicate fields
            r#"profile "dev.x" { wasm "x.wasm" class "goal" }"#,
            r#"profile "dev.x" { wasm "a.wasm" wasm "b.wasm" }"#,
            r#"profile "dev.x" { wasm "x.wasm" class "immediate" class "silent" }"#,
            // unknown field, duplicate scheme, scheme that is not a scheme
            r#"profile "dev.x" { template "x.md" }"#,
            r#"profile "dev.a" { wasm "a.wasm" }
               profile "dev.a" { wasm "b.wasm" }"#,
            r#"profile "not a scheme" { wasm "x.wasm" }"#,
        ];
        for text in loud {
            assert!(parse(text).is_err(), "expected error for: {text}");
        }
    }

    #[test]
    fn declared_profiles_anchor_relative_modules_at_the_catalog_root() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            config_path(tmp.path()),
            "profile \"dev.example.goal\" {\n  wasm \"$CATALOG/resolvers/goal.wasm\"\n}\n",
        )
        .unwrap();
        let registry = declared_profiles(tmp.path()).unwrap();
        let profile = registry.get("dev.example.goal").unwrap();
        assert_eq!(
            profile.module(),
            Some(tmp.path().join("resolvers/goal.wasm").as_path())
        );

        // A malformed block propagates instead of falling back to an empty registry.
        std::fs::write(
            config_path(tmp.path()),
            "profile \"dev.example.goal\" { class \"bogus\" }\n",
        )
        .unwrap();
        assert!(declared_profiles(tmp.path()).is_err());
    }
}
