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
    /// (success or failure) are cached by path and file identity; instances remain per-resolution.
    Wasm { module: PathBuf, class: ProfileClass },
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
#[derive(Clone)]
struct CachedModule {
    identity: ModuleIdentity,
    result: Result<Arc<crate::profile_wasm::WasmResolver>, String>,
    last_used: u64,
}

#[cfg(feature = "wasm-resolver")]
#[derive(Default)]
struct WasmCache {
    modules: HashMap<PathBuf, CachedModule>,
    clock: u64,
    #[cfg(test)]
    compile_attempts: u64,
}

#[cfg(feature = "wasm-resolver")]
impl WasmCache {
    fn next_clock(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn get(
        &mut self,
        path: &Path,
        identity: &ModuleIdentity,
    ) -> Option<Result<Arc<crate::profile_wasm::WasmResolver>, String>> {
        let now = self.next_clock();
        let cached = self.modules.get_mut(path)?;
        if cached.identity != *identity {
            return None;
        }
        cached.last_used = now;
        Some(cached.result.clone())
    }

    fn insert(
        &mut self,
        path: PathBuf,
        identity: ModuleIdentity,
        result: Result<Arc<crate::profile_wasm::WasmResolver>, String>,
    ) {
        if !self.modules.contains_key(&path) && self.modules.len() >= WASM_CACHE_CAPACITY {
            if let Some(oldest) = self
                .modules
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(path, _)| path.clone())
            {
                self.modules.remove(&oldest);
            }
        }
        let last_used = self.next_clock();
        self.modules.insert(
            path,
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
        let Some((scheme, _)) = uri.split_once(':') else {
            return Ok(None);
        };
        if !is_uri_scheme(scheme) {
            return Ok(None);
        }
        let Some(profile) = self.profiles.get(scheme) else {
            return Ok(None);
        };
        let ProfileSource::Wasm { module, class } = &profile.source;

        #[cfg(not(feature = "wasm-resolver"))]
        {
            let _ = (module, class, agent_dir);
            Err("profile resolver unavailable: st2 was built without the `wasm-resolver` feature"
                .to_owned())
        }
        #[cfg(feature = "wasm-resolver")]
        {
            let resolver = self.compiled(module)?;
            let contained = resolver
                .resolve_contained(uri, agent_dir)
                .map_err(|e| e.to_string())?;
            Ok(Some(Resolution {
                path: contained.path,
                containment_root: contained.root,
                class: *class,
            }))
        }
    }

    #[cfg(feature = "wasm-resolver")]
    fn compiled(
        &self,
        module_path: &Path,
    ) -> Result<Arc<crate::profile_wasm::WasmResolver>, String> {
        let snapshot = crate::profile_wasm::read_module_snapshot(
            module_path,
            crate::profile_wasm::DEFAULT_MODULE_LIMIT_BYTES,
        )
        .map_err(|error| error.to_string())?;
        let identity = ModuleIdentity::of(&snapshot);
        let mut cache = self.wasm_cache.lock();
        if let Some(cached) = cache.get(module_path, &identity) {
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
        cache.insert(module_path.to_path_buf(), identity, result.clone());
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
            assert!(cache.modules[&module].result.is_err());
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
