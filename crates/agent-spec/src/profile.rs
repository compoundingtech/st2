//! Resource profiles: scheme-level resolution semantics for Agent Spec resource URIs.
//!
//! The spec treats resource URIs as opaque identities — st2 preserves them but resolves nothing.
//! A *resource profile* closes that gap: a registry maps a URI scheme to a resolver. Per decision
//! Q10 every profile is a **wasm module** — there is no declarative template tier. A `.wasm`
//! binary exporting `resolve` (see [`crate::profile_wasm`] for the ABI) is run under wasmtime by
//! [`crate::profile_wasm::WasmResolver`] with fuel and memory limits, and each profile declares
//! the notification [`ProfileClass`] its resolved carriers use.
//!
//! All wasm machinery lives behind the `wasm-resolver` feature. Without it the registry still
//! parses and holds profiles; resolution of a registered scheme reports that the feature is
//! unavailable, which callers fold into "unwatchable" like any other contained resolver failure.
//!
//! The registry is injectable so a catalog can extend or override the built-in set.

use std::collections::BTreeMap;
#[cfg(feature = "wasm-resolver")]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(feature = "wasm-resolver")]
use std::sync::Arc;

#[cfg(feature = "wasm-resolver")]
use parking_lot::Mutex;

#[cfg(feature = "wasm-resolver")]
use sha2::{Digest as _, Sha256};

/// Scheme of the standing-seat goal carrier: `dev.schickling.agent-goal://<host>/<identity>`.
/// The authority names a logical host and identity; a resolver module decides what the URI
/// denotes on this seat (the demo guest: `<agent_dir>/resources/goal.md`).
pub const AGENT_GOAL_SCHEME: &str = "dev.schickling.agent-goal";

/// Maximum resolver module bytes admitted by both catalog transactions and the wasm runtime.
pub const DEFAULT_MODULE_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// How carriers resolved through one profile notify (`RESYNC-R04`). Declared alongside the
/// resolver module instead of sniffed from path basenames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileClass {
    /// Emit as soon as a content transition is observed.
    Immediate,
    /// Collect transitions into a short coalescing window before emitting.
    Coalesced,
    /// Never watch, never emit: the store is agent-authored state.
    Silent,
}

impl ProfileClass {
    /// Parse the catalog spelling of a class (`immediate|coalesced|silent`).
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "immediate" => Some(Self::Immediate),
            "coalesced" => Some(Self::Coalesced),
            "silent" => Some(Self::Silent),
            _ => None,
        }
    }
}

impl std::fmt::Display for ProfileClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Immediate => "immediate",
            Self::Coalesced => "coalesced",
            Self::Silent => "silent",
        })
    }
}

/// One profile entry: a scheme and how its URIs gain a local denotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceProfile {
    pub scheme: String,
    pub source: ProfileSource,
}

/// Where one profile's denotation comes from. Wasm-only per Q10.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileSource {
    /// A wasm module implementing the `resolve` protocol (see [`crate::profile_wasm`] for the
    /// ABI), plus the notification class every carrier it resolves carries. Compilation results
    /// (success or failure) are cached by exact declared path spellings, normalized path identity,
    /// admission policy, and file identity; instances remain per-resolution.
    Wasm {
        module: PathBuf,
        class: ProfileClass,
        containment_root: Option<PathBuf>,
    },
}

impl ResourceProfile {
    /// Wasm-module profile pointing at a `.wasm` file on disk.
    pub fn wasm(
        scheme: impl Into<String>,
        module: impl Into<PathBuf>,
        class: ProfileClass,
    ) -> Self {
        Self {
            scheme: scheme.into(),
            source: ProfileSource::Wasm {
                module: module.into(),
                class,
                containment_root: None,
            },
        }
    }

    /// Wasm-module profile whose module must be opened beneath one trusted directory without
    /// following symlinks in any relative path component.
    pub fn wasm_contained(
        scheme: impl Into<String>,
        containment_root: impl Into<PathBuf>,
        relative_module: impl AsRef<Path>,
        class: ProfileClass,
    ) -> Self {
        let containment_root = containment_root.into();
        Self {
            scheme: scheme.into(),
            source: ProfileSource::Wasm {
                module: containment_root.join(relative_module),
                class,
                containment_root: Some(containment_root),
            },
        }
    }

    /// The declared class for carriers this profile resolves.
    pub fn class(&self) -> ProfileClass {
        match &self.source {
            ProfileSource::Wasm { class, .. } => *class,
        }
    }

    /// The module path when this profile is wasm-backed (always, today).
    pub fn module(&self) -> Option<&Path> {
        match &self.source {
            ProfileSource::Wasm { module, .. } => Some(module),
        }
    }
}

/// One successful resolution through a profile: the local denotation, the host confinement root
/// that every later read must honor, and the declared class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub path: PathBuf,
    pub containment_root: PathBuf,
    pub class: ProfileClass,
}

#[cfg(feature = "wasm-resolver")]
const WASM_CACHE_CAPACITY: usize = 32;

#[cfg(feature = "wasm-resolver")]
#[derive(Clone, PartialEq, Eq)]
struct ModuleIdentity {
    digest: [u8; 32],
    len: u64,
    modified: Option<std::time::SystemTime>,
    created: Option<std::time::SystemTime>,
    readonly: bool,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    mode: u32,
    #[cfg(unix)]
    uid: u32,
    #[cfg(unix)]
    gid: u32,
    #[cfg(unix)]
    ctime: i64,
    #[cfg(unix)]
    ctime_nsec: i64,
}

#[cfg(feature = "wasm-resolver")]
impl ModuleIdentity {
    fn of(snapshot: &crate::profile_wasm::ModuleSnapshot) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;

        let metadata = &snapshot.metadata;
        Self {
            digest: Sha256::digest(&snapshot.bytes).into(),
            len: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            readonly: metadata.permissions().readonly(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
            #[cfg(unix)]
            mode: metadata.mode(),
            #[cfg(unix)]
            uid: metadata.uid(),
            #[cfg(unix)]
            gid: metadata.gid(),
            #[cfg(unix)]
            ctime: metadata.ctime(),
            #[cfg(unix)]
            ctime_nsec: metadata.ctime_nsec(),
        }
    }
}
#[cfg(feature = "wasm-resolver")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ModuleCacheKey {
    normalized_module: PathBuf,
    declared_module: PathBuf,
    normalized_containment_root: Option<PathBuf>,
    declared_containment_root: Option<PathBuf>,
}

#[cfg(feature = "wasm-resolver")]
impl ModuleCacheKey {
    fn new(module: &Path, containment_root: Option<&Path>) -> Self {
        Self {
            normalized_module: normalize_cache_path(module),
            declared_module: module.to_path_buf(),
            normalized_containment_root: containment_root.map(normalize_cache_path),
            declared_containment_root: containment_root.map(Path::to_path_buf),
        }
    }
}

#[cfg(feature = "wasm-resolver")]
fn normalize_cache_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                normalized.push(component.as_os_str());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if matches!(
                    normalized.components().next_back(),
                    Some(std::path::Component::Normal(_))
                ) {
                    normalized.pop();
                } else if !path.is_absolute() {
                    normalized.push(component.as_os_str());
                }
            }
            std::path::Component::Normal(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(feature = "wasm-resolver")]
#[derive(Clone)]
struct CachedModule {
    identity: ModuleIdentity,
    result: Result<Arc<crate::profile_wasm::WasmResolver>, String>,
    last_used: u64,
}

#[cfg(feature = "wasm-resolver")]
#[derive(Default)]
struct WasmCache {
    modules: HashMap<ModuleCacheKey, CachedModule>,
    clock: u64,
    #[cfg(test)]
    compile_attempts: u64,
    #[cfg(test)]
    snapshot_attempts: u64,
}

/// One resolution pass over a registry. Module snapshots, including read and compile failures, are
/// shared by every binding using the same declared path spelling and admission policy in the
/// pass; a later pass snapshots each used key again so replacement invalidation remains
/// observable.
pub struct ResourceProfileRefresh<'a> {
    registry: &'a ResourceProfileRegistry,
    #[cfg(feature = "wasm-resolver")]
    modules: Mutex<HashMap<ModuleCacheKey, Result<Arc<crate::profile_wasm::WasmResolver>, String>>>,
}

impl ResourceProfileRefresh<'_> {
    /// Look up a declared profile without resolving it.
    pub fn get(&self, scheme: &str) -> Option<&ResourceProfile> {
        self.registry.get(scheme)
    }

    /// Resolve one binding against the registry definitions captured by this refresh.
    pub fn try_resolve(&self, agent_dir: &Path, uri: &str) -> Result<Option<Resolution>, String> {
        let Some((scheme, _)) = uri.split_once(':') else {
            return Ok(None);
        };
        if !is_uri_scheme(scheme) {
            return Ok(None);
        }
        let Some(profile) = self.registry.profiles.get(scheme) else {
            return Ok(None);
        };
        let ProfileSource::Wasm {
            module,
            class,
            containment_root,
        } = &profile.source;

        #[cfg(not(feature = "wasm-resolver"))]
        {
            let _ = (module, class, containment_root, agent_dir);
            Err("profile resolver unavailable: st2 was built without the `wasm-resolver` feature"
                .to_owned())
        }
        #[cfg(feature = "wasm-resolver")]
        {
            let key = ModuleCacheKey::new(module, containment_root.as_deref());
            let resolver = {
                let mut modules = self.modules.lock();
                if let Some(result) = modules.get(&key) {
                    result.clone()
                } else {
                    let result =
                        self.registry
                            .compiled(&key, module, containment_root.as_deref());
                    modules.insert(key, result.clone());
                    result
                }
            }?;
            let contained = resolver
                .resolve_contained(uri, agent_dir)
                .map_err(|error| error.to_string())?;
            Ok(Some(Resolution {
                path: contained.path,
                containment_root: contained.root,
                class: *class,
            }))
        }
    }
}

#[cfg(feature = "wasm-resolver")]
impl WasmCache {
    fn next_clock(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn get(
        &mut self,
        key: &ModuleCacheKey,
        identity: &ModuleIdentity,
    ) -> Option<Result<Arc<crate::profile_wasm::WasmResolver>, String>> {
        let now = self.next_clock();
        let cached = self.modules.get_mut(key)?;
        if cached.identity != *identity {
            return None;
        }
        cached.last_used = now;
        Some(cached.result.clone())
    }

    fn insert(
        &mut self,
        key: ModuleCacheKey,
        identity: ModuleIdentity,
        result: Result<Arc<crate::profile_wasm::WasmResolver>, String>,
    ) {
        if !self.modules.contains_key(&key) && self.modules.len() >= WASM_CACHE_CAPACITY {
            if let Some(oldest) = self
                .modules
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(key, _)| key.clone())
            {
                self.modules.remove(&oldest);
            }
        }
        let last_used = self.next_clock();
        self.modules.insert(
            key,
            CachedModule {
                identity,
                result,
                last_used,
            },
        );
    }
}

/// Scheme -> resolver registry. Lookup is exact-scheme; unregistered schemes stay opaque.
#[derive(Clone)]
pub struct ResourceProfileRegistry {
    profiles: BTreeMap<String, ResourceProfile>,
    /// Bounded compiled-module outcomes keyed by path and file identity. Shared across clones so
    /// repeated and concurrent resolution coalesces successful and failed compilation attempts.
    #[cfg(feature = "wasm-resolver")]
    wasm_cache: Arc<Mutex<WasmCache>>,
}

impl PartialEq for ResourceProfileRegistry {
    fn eq(&self, other: &Self) -> bool {
        self.profiles == other.profiles
    }
}

impl std::fmt::Debug for ResourceProfileRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Compiled resolvers and cached compile failures are summarized instead of derived.
        #[cfg(feature = "wasm-resolver")]
        let modules = self.wasm_cache.lock().modules.len();
        let mut out = f.debug_struct("ResourceProfileRegistry");
        out.field("profiles", &self.profiles);
        #[cfg(feature = "wasm-resolver")]
        out.field("wasm_cache_entries", &modules);
        out.finish()
    }
}

impl Default for ResourceProfileRegistry {
    fn default() -> Self {
        Self::builtin()
    }
}

impl ResourceProfileRegistry {
    /// The built-in set shipped with st2. Profiles are declared, not presumed: today the built-in
    /// set is empty, so every scheme-URI stays unwatchable until a catalog declares a profile for
    /// it (deny-by-default).
    pub fn builtin() -> Self {
        Self::empty()
    }

    pub fn empty() -> Self {
        Self {
            profiles: BTreeMap::new(),
            #[cfg(feature = "wasm-resolver")]
            wasm_cache: Arc::new(Mutex::new(WasmCache::default())),
        }
    }

    /// Insert (or replace) one profile; returns `self` for chaining.
    pub fn with_profile(mut self, profile: ResourceProfile) -> Self {
        self.profiles.insert(profile.scheme.clone(), profile);
        self
    }

    /// Insert (or replace) many profiles; returns `self` for chaining.
    pub fn with_profiles(
        mut self,
        profiles: impl IntoIterator<Item = ResourceProfile>,
    ) -> Self {
        for profile in profiles {
            self.profiles.insert(profile.scheme.clone(), profile);
        }
        self
    }

    /// Replace definitions from another registry while preserving this registry's compiled cache.
    pub fn replace_definitions(&mut self, registry: Self) {
        self.profiles = registry.profiles;
    }

    /// Start one resolution pass. Every unique normalized module path and admission policy is read
    /// and fingerprinted at most once in the pass, even when many bindings or schemes share it.
    pub fn begin_refresh(&self) -> ResourceProfileRefresh<'_> {
        ResourceProfileRefresh {
            registry: self,
            #[cfg(feature = "wasm-resolver")]
            modules: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, scheme: &str) -> Option<&ResourceProfile> {
        self.profiles.get(scheme)
    }

    /// Resolve `uri` when its scheme has a registered profile. No scheme or an unregistered
    /// scheme is not this registry's business (`Ok(None)`); the caller's legacy local-path rules
    /// apply there.
    ///
    /// A failing wasm resolver (trap, exhausted fuel, malformed output, missing feature) is
    /// contained and reported as `None`: the URI stays unwatchable, the supervisor survives. Use
    /// [`ResourceProfileRegistry::try_resolve`] when the failure reason matters.
    pub fn resolve(&self, agent_dir: &Path, uri: &str) -> Option<Resolution> {
        self.try_resolve(agent_dir, uri).ok().flatten()
    }

    /// Like [`Self::resolve`] but surfaces failures for observability:
    ///
    /// - `Ok(None)` — no URI scheme, or the scheme has no registered profile;
    /// - `Err(_)` — the scheme IS registered but resolution failed (wasm trap, malformed output,
    ///   or built without the `wasm-resolver` feature).
    pub fn try_resolve(&self, agent_dir: &Path, uri: &str) -> Result<Option<Resolution>, String> {
        self.begin_refresh().try_resolve(agent_dir, uri)
    }

    #[cfg(feature = "wasm-resolver")]
    fn compiled(
        &self,
        key: &ModuleCacheKey,
        module_path: &Path,
        containment_root: Option<&Path>,
    ) -> Result<Arc<crate::profile_wasm::WasmResolver>, String> {
        #[cfg(test)]
        {
            self.wasm_cache.lock().snapshot_attempts += 1;
        }
        let snapshot = crate::profile_wasm::read_module_snapshot(
            module_path,
            containment_root,
            DEFAULT_MODULE_LIMIT_BYTES,
        )
        .map_err(|error| error.to_string())?;
        let identity = ModuleIdentity::of(&snapshot);
        let mut cache = self.wasm_cache.lock();
        if let Some(cached) = cache.get(key, &identity) {
            return cached;
        }
        #[cfg(test)]
        {
            cache.compile_attempts += 1;
        }
        // Compilation stays under the cache mutex: clones and concurrent subscribers coalesce
        // the same identity into one attempt, including one cached failure.
        let result = crate::profile_wasm::WasmResolver::from_bytes(&snapshot.bytes)
            .map(Arc::new)
            .map_err(|error| error.to_string());
        cache.insert(key.clone(), identity, result.clone());
        result
    }
}

/// RFC 3986 scheme characters: the same test `st2::resync` uses to decide whether a URI string
/// carries a scheme at all. Shared so both sides agree on where profiles take over.
fn is_uri_scheme(scheme: &str) -> bool {
    scheme
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
        && !scheme.contains('/')
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_profile(class: ProfileClass) -> ResourceProfile {
        ResourceProfile::wasm(
            AGENT_GOAL_SCHEME,
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/demo_resolver.wasm"),
            class,
        )
    }
    #[cfg(feature = "wasm-resolver")]
    fn write_any_uri_resolver(path: &Path) {
        std::fs::write(
            path,
            r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (data (i32.const 8) "{\22path\22:\22resources/goal.md\22,\22class\22:\22goal\22}")
  (func (export "resolve") (param i32 i32 i32 i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 8)) (i64.const 32))
      (i64.extend_i32_u (i32.const 43))))
)"#,
        )
        .unwrap();
    }

    #[test]
    fn classes_round_trip_through_their_catalog_spelling() {
        for (text, expected) in [
            ("immediate", ProfileClass::Immediate),
            ("coalesced", ProfileClass::Coalesced),
            ("silent", ProfileClass::Silent),
        ] {
            assert_eq!(ProfileClass::parse(text), Some(expected));
            assert_eq!(expected.to_string(), text);
        }
        assert_eq!(ProfileClass::parse("goal"), None);
        assert_eq!(ProfileClass::parse(""), None);
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn registered_scheme_resolves_with_the_declared_class() {
        let registry = ResourceProfileRegistry::empty().with_profile(demo_profile(ProfileClass::Immediate));
        assert_eq!(
            registry.resolve(Path::new("/cat/agents/dev3/janitor"), "dev.schickling.agent-goal://dev3/janitor"),
            Some(Resolution {
                path: PathBuf::from("/cat/agents/dev3/janitor/resources/goal.md"),
                class: ProfileClass::Immediate,
                containment_root: PathBuf::from("/cat/agents/dev3/janitor"),
            })
        );
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn authority_and_path_are_identity_not_location() {
        // A foreign host's identity still denotes the local seat's own goal carrier.
        let registry = ResourceProfileRegistry::empty().with_profile(demo_profile(ProfileClass::Coalesced));
        assert!(registry
            .resolve(Path::new("/here"), "dev.schickling.agent-goal://elsewhere/x")
            .is_some());
    }

    #[test]
    fn unknown_schemes_relative_paths_and_file_uris_are_not_registry_business() {
        let registry = ResourceProfileRegistry::empty().with_profile(demo_profile(ProfileClass::Immediate));
        assert_eq!(registry.resolve(Path::new("/a"), "worktree://repo/main"), None);
        assert_eq!(registry.resolve(Path::new("/a"), "http://x/y"), None);
        assert_eq!(registry.resolve(Path::new("/a"), "resources/goal.md"), None);
        assert_eq!(registry.resolve(Path::new("/a"), "file:///etc/x"), None);
    }

    #[test]
    fn injected_profiles_replace_the_builtin_for_the_same_scheme() {
        let builtin = ResourceProfileRegistry::builtin();
        assert_eq!(builtin.get(AGENT_GOAL_SCHEME), None, "builtin set is empty");
        let injected = builtin.with_profiles([demo_profile(ProfileClass::Silent)]);
        assert_eq!(
            injected.get(AGENT_GOAL_SCHEME).map(ResourceProfile::class),
            Some(ProfileClass::Silent)
        );
        assert_eq!(
            ResourceProfileRegistry::default(),
            ResourceProfileRegistry::builtin()
        );
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn broken_module_fails_contained_with_the_reason_surfaced() {
        let temp = tempfile::tempdir().unwrap();
        let broken = temp.path().join("broken.wasm");
        std::fs::write(&broken, b"this is not a wasm module").unwrap();
        let registry = ResourceProfileRegistry::empty()
            .with_profile(ResourceProfile::wasm("doomed", &broken, ProfileClass::Immediate));
        assert_eq!(registry.resolve(Path::new("/a"), "doomed://x"), None);
        let reason = registry.try_resolve(Path::new("/a"), "doomed://x").unwrap_err();
        assert!(reason.contains("instantiation failed"), "{reason}");
        // Unregistered schemes are `Ok(None)` even in a registry that also holds failures.
        assert_eq!(registry.try_resolve(Path::new("/a"), "other://x").unwrap(), None);
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn failed_compilation_is_coalesced_and_invalidated_by_file_identity() {
        const URI: &str = "dev.schickling.agent-goal://host/agent";
        let temp = tempfile::tempdir().unwrap();
        let module = temp.path().join("resolver.wasm");
        std::fs::write(&module, b"this is not a wasm module").unwrap();
        let registry = ResourceProfileRegistry::empty().with_profile(ResourceProfile::wasm(
            AGENT_GOAL_SCHEME,
            &module,
            ProfileClass::Immediate,
        ));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let registry = registry.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    registry.try_resolve(Path::new("/agent"), URI)
                })
            })
            .collect::<Vec<_>>();
        let attempts = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(attempts.iter().all(Result::is_err));
        {
            let cache = registry.wasm_cache.lock();
            assert_eq!(
                cache.compile_attempts, 1,
                "concurrent subscribers compile one unchanged bad identity once"
            );
            assert_eq!(cache.modules.len(), 1);
            let key = ModuleCacheKey::new(&module, None);
            assert!(cache.modules[&key].result.is_err());
        }
        assert!(
            registry
                .try_resolve(Path::new("/agent"), URI)
                .is_err()
        );
        assert_eq!(
            registry.wasm_cache.lock().compile_attempts,
            1,
            "later refreshes reuse the cached failure"
        );
        assert_eq!(registry.wasm_cache.lock().snapshot_attempts, 9);

        let original_permissions = std::fs::metadata(&module).unwrap().permissions();
        let mut changed_permissions = original_permissions.clone();
        changed_permissions.set_readonly(!original_permissions.readonly());
        std::fs::set_permissions(&module, changed_permissions).unwrap();
        assert!(
            registry
                .try_resolve(Path::new("/agent"), URI)
                .is_err()
        );
        assert_eq!(
            registry.wasm_cache.lock().compile_attempts,
            2,
            "metadata identity changes invalidate the cached failure"
        );

        std::fs::set_permissions(&module, original_permissions).unwrap();
        std::fs::write(
            &module,
            include_bytes!("../tests/fixtures/demo_resolver.wasm"),
        )
        .unwrap();
        assert!(
            registry
                .try_resolve(Path::new("/agent"), URI)
                .unwrap()
                .is_some(),
            "new module bytes are compiled after replacement"
        );
        assert_eq!(registry.wasm_cache.lock().compile_attempts, 3);
        assert!(
            registry
                .try_resolve(Path::new("/agent"), URI)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            registry.wasm_cache.lock().compile_attempts,
            3,
            "the successful replacement is cached too"
        );

        assert_eq!(
            registry.wasm_cache.lock().snapshot_attempts,
            12,
            "each direct resolution is its own refresh"
        );
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn one_refresh_shares_module_snapshot_and_errors_across_many_bindings() {
        const URI: &str = "shared://host/agent";
        let temp = tempfile::tempdir().unwrap();
        let module = temp.path().join("resolver.wasm");
        std::fs::write(&module, b"not a wasm module").unwrap();
        let registry = ResourceProfileRegistry::empty().with_profile(ResourceProfile::wasm(
            "shared",
            &module,
            ProfileClass::Immediate,
        ));

        let refresh = registry.begin_refresh();
        let errors = (0..100)
            .map(|index| {
                refresh
                    .try_resolve(&temp.path().join(format!("agent-{index}")), URI)
                    .unwrap_err()
            })
            .collect::<Vec<_>>();
        assert!(errors.windows(2).all(|pair| pair[0] == pair[1]));
        let cache = registry.wasm_cache.lock();
        assert_eq!(cache.snapshot_attempts, 1);
        assert_eq!(cache.compile_attempts, 1);
    }

    #[test]
    #[cfg(all(feature = "wasm-resolver", unix))]
    fn cache_normalization_does_not_change_the_path_used_for_admission() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("child")).unwrap();
        symlink(outside.path().join("child"), root.path().join("link")).unwrap();
        write_any_uri_resolver(&root.path().join("resolver.wasm"));
        let declared = root.path().join("link/../resolver.wasm");
        let registry = ResourceProfileRegistry::empty().with_profile(ResourceProfile::wasm(
            "external",
            &declared,
            ProfileClass::Immediate,
        ));

        assert!(
            registry
                .try_resolve(Path::new("/agent"), "external://host/agent")
                .is_err(),
            "admission must use the declared path, which resolves through the symlink outside root"
        );
        assert_eq!(
            ModuleCacheKey::new(&declared, None).normalized_module,
            root.path().join("resolver.wasm")
        );
    }

    #[test]
    #[cfg(all(feature = "wasm-resolver", unix))]
    fn refresh_cache_distinguishes_symlink_sensitive_module_spellings() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("child")).unwrap();
        symlink(outside.path().join("child"), root.path().join("link")).unwrap();
        std::fs::write(outside.path().join("resolver.wasm"), b"not a wasm module").unwrap();
        write_any_uri_resolver(&root.path().join("resolver.wasm"));
        let indirect = root.path().join("link/../resolver.wasm");
        let direct = root.path().join("resolver.wasm");
        let registry = ResourceProfileRegistry::empty().with_profiles([
            ResourceProfile::wasm("indirect", &indirect, ProfileClass::Immediate),
            ResourceProfile::wasm("direct", &direct, ProfileClass::Immediate),
        ]);

        let refresh = registry.begin_refresh();
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "indirect://host/agent")
                .is_err()
        );
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "direct://host/agent")
                .unwrap()
                .is_some(),
            "the direct module must be opened instead of reusing the indirect failure"
        );
        assert_eq!(registry.wasm_cache.lock().snapshot_attempts, 2);
    }

    #[test]
    #[cfg(all(feature = "wasm-resolver", unix))]
    fn refresh_cache_distinguishes_symlink_sensitive_containment_roots() {
        use std::os::unix::fs::symlink;

        let catalog = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("child")).unwrap();
        symlink(
            outside.path().join("child"),
            catalog.path().join("link"),
        )
        .unwrap();
        write_any_uri_resolver(&outside.path().join("resolver.wasm"));
        let indirect_root = catalog.path().join("link/..");
        let registry = ResourceProfileRegistry::empty().with_profiles([
            ResourceProfile::wasm_contained(
                "indirect-root",
                &indirect_root,
                Path::new("resolver.wasm"),
                ProfileClass::Immediate,
            ),
            ResourceProfile::wasm_contained(
                "direct-root",
                catalog.path(),
                Path::new("link/../resolver.wasm"),
                ProfileClass::Immediate,
            ),
        ]);

        let refresh = registry.begin_refresh();
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "indirect-root://host/agent")
                .unwrap()
                .is_some()
        );
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "direct-root://host/agent")
                .is_err(),
            "the direct root must enforce its own descriptor-relative admission"
        );
        assert_eq!(registry.wasm_cache.lock().snapshot_attempts, 2);
    }

    #[test]
    #[cfg(all(feature = "wasm-resolver", unix))]
    fn refresh_cache_cannot_reuse_external_admission_for_a_contained_module() {
        use std::os::unix::fs::symlink;

        let catalog = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let linked_directory = catalog.path().join("link");
        symlink(external.path(), &linked_directory).unwrap();
        let module = external.path().join("resolver.wasm");
        write_any_uri_resolver(&module);
        let shared_spelling = linked_directory.join("resolver.wasm");
        let registry = ResourceProfileRegistry::empty().with_profiles([
            ResourceProfile::wasm("external", &shared_spelling, ProfileClass::Immediate),
            ResourceProfile::wasm_contained(
                "contained",
                catalog.path(),
                Path::new("link/resolver.wasm"),
                ProfileClass::Immediate,
            ),
        ]);

        let refresh = registry.begin_refresh();
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "external://host/agent")
                .unwrap()
                .is_some()
        );
        assert!(
            refresh
                .try_resolve(Path::new("/agent"), "contained://host/agent")
                .is_err(),
            "contained admission must re-open the descriptor-relative path and reject the symlink"
        );
        let cache = registry.wasm_cache.lock();
        assert_eq!(cache.snapshot_attempts, 2);
        assert_eq!(cache.compile_attempts, 1);
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn registry_clones_share_compilation_only_with_the_same_admission_policy() {
        let catalog = tempfile::tempdir().unwrap();
        let module = catalog.path().join("resolver.wasm");
        write_any_uri_resolver(&module);
        let registry = ResourceProfileRegistry::empty().with_profiles([
            ResourceProfile::wasm("external", &module, ProfileClass::Immediate),
            ResourceProfile::wasm_contained(
                "contained",
                catalog.path(),
                Path::new("resolver.wasm"),
                ProfileClass::Immediate,
            ),
        ]);
        let clone = registry.clone();

        for current in [&registry, &clone] {
            assert!(
                current
                    .try_resolve(Path::new("/agent"), "external://host/agent")
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(registry.wasm_cache.lock().compile_attempts, 1);

        for current in [&registry, &clone] {
            assert!(
                current
                    .try_resolve(Path::new("/agent"), "contained://host/agent")
                    .unwrap()
                    .is_some()
            );
        }
        let cache = registry.wasm_cache.lock();
        assert_eq!(
            cache.compile_attempts, 2,
            "external and descriptor-relative admission retain distinct cache entries"
        );
        assert_eq!(cache.modules.len(), 2);
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn a_later_refresh_retries_a_replaced_module_and_reuses_the_new_identity() {
        const URI: &str = "dev.schickling.agent-goal://host/agent";
        let temp = tempfile::tempdir().unwrap();
        let module = temp.path().join("resolver.wasm");
        std::fs::write(&module, b"not a wasm module").unwrap();
        let registry = ResourceProfileRegistry::empty().with_profile(ResourceProfile::wasm(
            AGENT_GOAL_SCHEME,
            &module,
            ProfileClass::Immediate,
        ));

        let first = registry.begin_refresh();
        assert!(first.try_resolve(temp.path(), URI).is_err());
        std::fs::write(
            &module,
            include_bytes!("../tests/fixtures/demo_resolver.wasm"),
        )
        .unwrap();
        assert!(
            first.try_resolve(temp.path(), URI).is_err(),
            "one pass shares one immutable module outcome"
        );

        let replacement = registry.begin_refresh();
        assert!(
            replacement.try_resolve(temp.path(), URI).unwrap().is_some(),
            "the next pass snapshots and compiles the replacement"
        );
        assert!(replacement.try_resolve(temp.path(), URI).unwrap().is_some());
        let cache = registry.wasm_cache.lock();
        assert_eq!(cache.snapshot_attempts, 2);
        assert_eq!(cache.compile_attempts, 2);
    }

    #[test]
    #[cfg(feature = "wasm-resolver")]
    fn compiled_module_cache_is_lru_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let mut registry = ResourceProfileRegistry::empty();
        for index in 0..WASM_CACHE_CAPACITY + 4 {
            let scheme = format!("broken{index}");
            let module = temp.path().join(format!("{scheme}.wasm"));
            std::fs::write(&module, format!("not wasm {index}")).unwrap();
            registry = registry.with_profile(ResourceProfile::wasm(
                &scheme,
                &module,
                ProfileClass::Immediate,
            ));
        }
        for index in 0..WASM_CACHE_CAPACITY + 4 {
            let uri = format!("broken{index}://host/agent");
            assert!(registry.try_resolve(Path::new("/agent"), &uri).is_err());
        }
        let cache = registry.wasm_cache.lock();
        assert_eq!(cache.modules.len(), WASM_CACHE_CAPACITY);
        assert_eq!(
            cache.compile_attempts as usize,
            WASM_CACHE_CAPACITY + 4
        );
    }
}
