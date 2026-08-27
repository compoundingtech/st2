//! Out-of-process-style isolation via an in-process wasm sandbox: [`WasmResolver`] runs a
//! resource-profile resolver as a core wasm module under wasmtime.
//!
//! ABI (deliberately minimal, no WASI, no component model):
//!
//! - guest exports `memory`, `alloc(len: i32) -> i32`, and
//!   `resolve(uri_ptr: i32, uri_len: i32, dir_ptr: i32, dir_len: i32) -> i64`.
//! - the host copies `uri` and `agent_dir` into linear memory through `alloc`; the return value
//!   packs `(ptr << 32) | len` of a UTF-8 JSON document `{"path": "...", "class": "..."}`.
//! - the agent directory reaches the guest as a second argument (a global would need mutable
//!   globals + import plumbing for no gain).
//!
//! Containment story: traps, fuel exhaustion, and memory-limit breaches are caught here and
//! surfaced as typed errors; they never unwind into the supervisor. Semantic containment
//! (the guest returning *which* path to watch) stays the host's job — see
//! [`WasmResolver::resolve_contained`].

use serde::Deserialize;
use wasmtime::{
    Config, Engine, Instance, Memory, Module, Store, StoreLimits, StoreLimitsBuilder, Trap,
    TypedFunc,
};

/// Default per-call compute budget in wasmtime fuel units (~metered wasm instructions).
pub const DEFAULT_FUEL_PER_CALL: u64 = 5_000_000;
/// Default cap on guest linear-memory growth. Breaching it traps instead of OOMing the host.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
/// Default cap on elements in each guest table. The ABI itself requires no table.
pub const DEFAULT_TABLE_ELEMENT_LIMIT: usize = 10_000;
/// Maximum resolver module bytes admitted before Wasmtime validation and compilation.
pub use crate::profile::DEFAULT_MODULE_LIMIT_BYTES;

/// One resolution result as produced by a wasm resolver module.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WasmResolution {
    /// Local path the profile denotes, relative to the agent directory.
    pub path: String,
    /// Free-form classification carried alongside the denotation (e.g. `goal`).
    #[serde(default)]
    pub class: String,
}

/// A resolver-selected path plus the host root that must confine every later read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainedPath {
    pub path: std::path::PathBuf,
    pub root: std::path::PathBuf,
}

/// Why a wasm-backed resolution failed. Every variant is contained: the host survives.
#[derive(Debug)]
pub enum WasmResolveError {
    /// The module failed to load or instantiate.
    Instantiation(String),
    /// The guest trapped (unreachable, stack overflow, out-of-bounds access, …).
    Trap(Trap),
    /// The guest exceeded its per-call fuel budget (e.g. infinite loop).
    FuelExhausted,
    /// The guest returned a pointer/length pair that does not denote readable UTF-8 JSON.
    BadReturn(String),
    /// The guest omitted one of the required exports.
    MissingExport(&'static str),
}

impl std::fmt::Display for WasmResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Instantiation(e) => write!(f, "module instantiation failed: {e:#}"),
            Self::Trap(t) => write!(f, "guest trapped: {t}"),
            Self::FuelExhausted => write!(f, "guest exceeded its fuel budget"),
            Self::BadReturn(e) => write!(f, "guest returned malformed output: {e:#}"),
            Self::MissingExport(name) => write!(f, "guest does not export `{name}`"),
        }
    }
}

impl std::error::Error for WasmResolveError {}

/// A compiled-but-not-instantiated wasm resolver module. Cheap to clone (engine + module are
/// interned); instances are created per resolution unless a caller pools them explicitly.
pub struct WasmResolver {
    engine: Engine,
    module: Module,
    fuel_per_call: u64,
    memory_limit: usize,
}

impl Clone for WasmResolver {
    fn clone(&self) -> Self {
        // Engine and Module are cheap Arc-backed clones; wasmtime shares compiled code per
        // engine, so no recompilation happens here.
        Self {
            engine: self.engine.clone(),
            module: self.module.clone(),
            fuel_per_call: self.fuel_per_call,
            memory_limit: self.memory_limit,
        }
    }
}

impl WasmResolver {
    /// Compile a `.wasm` module from disk with default budgets.
    pub fn load(path: &std::path::Path) -> Result<Self, WasmResolveError> {
        let snapshot = read_module_snapshot(path, DEFAULT_MODULE_LIMIT_BYTES)?;
        Self::from_bytes(&snapshot.bytes)
    }

    /// Compile already-bounded module bytes. The profile registry uses this after fingerprinting
    /// one immutable read so cache identity and compiled code describe the same byte sequence.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, WasmResolveError> {
        if bytes.len() > DEFAULT_MODULE_LIMIT_BYTES {
            return Err(WasmResolveError::Instantiation(format!(
                "resolver module exceeds the {DEFAULT_MODULE_LIMIT_BYTES}-byte limit"
            )));
        }
        let engine = engine();
        let module = Module::new(&engine, bytes)
            .map_err(|e| WasmResolveError::Instantiation(e.to_string()))?;
        Ok(Self {
            engine,
            module,
            fuel_per_call: DEFAULT_FUEL_PER_CALL,
            memory_limit: DEFAULT_MEMORY_LIMIT_BYTES,
        })
    }

    /// Compile an in-memory binary (tests use this with inline WAT).
    pub fn from_wat(wat: &str) -> Result<Self, WasmResolveError> {
        let engine = engine();
        let module =
            Module::new(&engine, wat).map_err(|e| WasmResolveError::Instantiation(e.to_string()))?;
        Ok(Self {
            engine,
            module,
            fuel_per_call: DEFAULT_FUEL_PER_CALL,
            memory_limit: DEFAULT_MEMORY_LIMIT_BYTES,
        })
    }

    /// Override the per-call fuel budget.
    pub fn with_fuel_per_call(mut self, fuel: u64) -> Self {
        self.fuel_per_call = fuel;
        self
    }

    /// Override the guest linear-memory cap.
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Instantiate a fresh instance. Measured separately from calls in benchmarks because it is
    /// the dominant warm/cold split for wasm plugins.
    pub fn instantiate(&self) -> Result<WasmInstance, WasmResolveError> {
        WasmInstance::new(self)
    }

    /// Resolve once with one fresh fuel allowance shared by instantiation and the call.
    pub fn resolve_once(
        &self,
        uri: &str,
        agent_dir: &str,
    ) -> Result<WasmResolution, WasmResolveError> {
        self.instantiate()?.resolve(uri, agent_dir)
    }

    /// Resolve with semantic containment: the returned path must stay inside `agent_dir`.
    /// This is the boundary the wasm sandbox cannot enforce by itself — the guest chooses what
    /// string to return, so the host decides which strings it will act on. The returned root must
    /// also confine every later carrier read because nonexistent path components can change.
    pub fn resolve_contained(
        &self,
        uri: &str,
        agent_dir: &std::path::Path,
    ) -> Result<ContainedPath, WasmResolveError> {
        let resolution = self.resolve_once(uri, &agent_dir.to_string_lossy())?;
        if resolution.path.is_empty() {
            return Err(WasmResolveError::BadReturn(
                "resolver returned an empty path".to_owned(),
            ));
        }
        let root = normalize(agent_dir);
        let path = normalize(&root.join(&resolution.path));
        if !path.starts_with(&root) {
            return Err(WasmResolveError::BadReturn(format!(
                "resolver escaped the agent directory: {:?}",
                resolution.path
            )));
        }
        if path == root {
            return Err(WasmResolveError::BadReturn(
                "resolver path resolves to the agent directory".to_owned(),
            ));
        }
        reject_symlink_components(&root, &path)?;
        Ok(ContainedPath { path, root })
    }
}

pub(crate) struct ModuleSnapshot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) metadata: std::fs::Metadata,
}

pub(crate) fn read_module_snapshot(
    path: &std::path::Path,
    limit: usize,
) -> Result<ModuleSnapshot, WasmResolveError> {
    use std::io::Read as _;

    let file = open_module_file(path)?;
    let declared_len = file
        .metadata()
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?
        .len();
    if declared_len > limit as u64 {
        return Err(WasmResolveError::Instantiation(format!(
            "resolver module is {declared_len} bytes; limit is {limit} bytes"
        )));
    }
    let mut bytes = Vec::with_capacity(declared_len as usize);
    (&file)
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?;
    if bytes.len() > limit {
        return Err(WasmResolveError::Instantiation(format!(
            "resolver module exceeds the {limit}-byte limit"
        )));
    }
    let metadata = file
        .metadata()
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?;
    Ok(ModuleSnapshot { bytes, metadata })
}

#[cfg(unix)]
fn open_module_file(path: &std::path::Path) -> Result<std::fs::File, WasmResolveError> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?;
    if !file
        .metadata()
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?
        .file_type()
        .is_file()
    {
        return Err(WasmResolveError::Instantiation(
            "resolver module is not a regular file".to_owned(),
        ));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_module_file(path: &std::path::Path) -> Result<std::fs::File, WasmResolveError> {
    let file = std::fs::File::open(path)
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?;
    if !file
        .metadata()
        .map_err(|error| WasmResolveError::Instantiation(error.to_string()))?
        .file_type()
        .is_file()
    {
        return Err(WasmResolveError::Instantiation(
            "resolver module is not a regular file".to_owned(),
        ));
    }
    Ok(file)
}

fn reject_symlink_components(
    agent_dir: &std::path::Path,
    path: &std::path::Path,
) -> Result<(), WasmResolveError> {
    let mut current = agent_dir.to_path_buf();
    for component in path
        .strip_prefix(agent_dir)
        .expect("contained resolver path has the agent directory as its prefix")
        .components()
    {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(WasmResolveError::BadReturn(format!(
                    "resolver path crosses a symlink inside the agent directory: {current:?}"
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(WasmResolveError::BadReturn(format!(
                    "resolver path component cannot be inspected: {current:?}: {error}"
                )));
            }
        }
    }
    Ok(())
}

fn engine() -> Engine {
    let mut config = Config::new();
    config.consume_fuel(true);
    Engine::new(&config).expect("wasmtime engine configuration is valid")
}

fn classify_wasmtime_error(err: wasmtime::Error) -> WasmResolveError {
    match err.downcast_ref::<Trap>() {
        Some(Trap::OutOfFuel) => WasmResolveError::FuelExhausted,
        Some(trap) => WasmResolveError::Trap(*trap),
        None => WasmResolveError::Instantiation(err.to_string()),
    }
}

/// Lexical normalization only: resolvers name files inside the agent dir; nothing requires them
/// to exist yet.
fn normalize(path: &std::path::Path) -> std::path::PathBuf {
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

struct GuestFuncs {
    alloc: TypedFunc<i32, i32>,
    resolve: TypedFunc<(i32, i32, i32, i32), i64>,
    memory: Memory,
}

/// A live guest instance with its own store, fuel budget, and memory limit.
pub struct WasmInstance {
    store: Store<StoreLimits>,
    funcs: GuestFuncs,
    fuel_per_call: u64,
    first_call_uses_start_fuel: bool,
}

impl WasmInstance {
    fn new(resolver: &WasmResolver) -> Result<Self, WasmResolveError> {
        let mut store = Store::new(
            &resolver.engine,
            StoreLimitsBuilder::new()
                .memory_size(resolver.memory_limit)
                .table_elements(DEFAULT_TABLE_ELEMENT_LIMIT)
                .memories(1)
                .tables(4)
                .build(),
        );
        // Route instance/table/memory growth through the per-instance limits configured above.
        store.limiter(|limits| limits);
        store
            .set_fuel(resolver.fuel_per_call)
            .map_err(|e| WasmResolveError::Instantiation(e.to_string()))?;

        // No WASI, no imports: the demo protocol is closed. Import-requiring modules fail here,
        // which is itself containment (an untrusted module cannot reach the host environment).
        let instance = Instance::new(&mut store, &resolver.module, &[])
            .map_err(classify_wasmtime_error)?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|_| WasmResolveError::MissingExport("alloc"))?;
        let resolve = instance
            .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, "resolve")
            .map_err(|_| WasmResolveError::MissingExport("resolve"))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(WasmResolveError::MissingExport("memory"))?;
        Ok(Self {
            store,
            funcs: GuestFuncs { alloc, resolve, memory },
            fuel_per_call: resolver.fuel_per_call,
            first_call_uses_start_fuel: true,
        })
    }

    fn charge_fuel(&mut self) -> Result<(), WasmResolveError> {
        self.store
            .set_fuel(self.fuel_per_call)
            .map_err(|e| WasmResolveError::Instantiation(e.to_string()))
    }

    fn classify_call_error(&self, err: wasmtime::Error) -> WasmResolveError {
        classify_wasmtime_error(err)
    }

    /// Copy inputs into linear memory and run one metered `resolve` call. The first call consumes
    /// the fuel left after the module start function; later calls each receive one fresh budget.
    pub fn resolve(
        &mut self,
        uri: &str,
        agent_dir: &str,
    ) -> Result<WasmResolution, WasmResolveError> {
        if self.first_call_uses_start_fuel {
            self.first_call_uses_start_fuel = false;
        } else {
            self.charge_fuel()?;
        }
        let uri_ptr = self.write_guest_bytes(uri.as_bytes())?;
        let dir_ptr = self.write_guest_bytes(agent_dir.as_bytes())?;

        let packed = self
            .funcs
            .resolve
            .call(
                &mut self.store,
                (uri_ptr, uri.len() as i32, dir_ptr, agent_dir.len() as i32),
            )
            .map_err(|e| self.classify_call_error(e))?;

        let ret_ptr = (packed >> 32) as u32 as usize;
        let ret_len = (packed as u32) as usize;
        let bytes = self.read_guest_bytes(ret_ptr, ret_len)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|e| WasmResolveError::BadReturn(format!("return payload is not UTF-8: {e}")))?;
        serde_json::from_str(text).map_err(|e| WasmResolveError::BadReturn(e.to_string()))
    }

    fn write_guest_bytes(&mut self, bytes: &[u8]) -> Result<i32, WasmResolveError> {
        let ptr = self
            .funcs
            .alloc
            .call(&mut self.store, bytes.len() as i32)
            .map_err(|e| self.classify_call_error(e))?;
        let end = ptr as u32 as usize + bytes.len();
        let mem_end = self.funcs.memory.data_size(&self.store);
        if ptr < 0 || end > mem_end {
            return Err(WasmResolveError::BadReturn(
                "guest allocator handed out out-of-bounds memory".to_string(),
            ));
        }
        self.funcs
            .memory
            .write(&mut self.store, ptr as usize, bytes)
            .map_err(|e| WasmResolveError::BadReturn(format!("host write into guest memory failed: {e}")))?;
        Ok(ptr)
    }

    fn read_guest_bytes(&self, ptr: usize, len: usize) -> Result<&[u8], WasmResolveError> {
        let end = ptr.checked_add(len).ok_or_else(|| {
            WasmResolveError::BadReturn("return length overflows".to_string())
        })?;
        self.funcs
            .memory
            .data(&self.store)
            .get(ptr..end)
            .ok_or_else(|| {
                WasmResolveError::BadReturn(
                    "return pointer/length outside linear memory".to_string(),
                )
            })
    }
}
