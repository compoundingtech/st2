//! `st2 service install|status|uninstall` — install `st2 up` as a **systemd-user** unit so the
//! supervisor comes back on boot and on crash.
//!
//! **Linux-only, by design (the maintainer).** The Mac stays MANUAL — the maintainer runs `st2 up` themselves there
//! (TCC: a launchd-owned process can't inherit his GUI/keychain trust), so we deliberately do NOT
//! ship a launchd path like fabric does. On macOS (or anything non-systemd) this bails loud.
//!
//! A service restart is safe because st2 spawns each task in its own transient scope
//! (`systemd-run --user --scope`, see `isolate.rs`) —
//! a SIBLING of this unit, not a child. So stopping/restarting `st2.service` reaps only the
//! supervisor loop; the agents survive in their scopes and a fresh supervisor ADOPTS them
//! (tests/nomad_survival.rs + tests/transport_isolation.rs). That is the whole reason this unit is
//! now a safe thing to install.

use std::{
    env,
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    os::{
        fd::AsRawFd as _,
        unix::fs::{MetadataExt as _, OpenOptionsExt as _},
    },
    process::Command,
};

use anyhow::{Context, Result, bail};

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "st2.service";
/// 1 GiB is generous headroom for an I/O-light sync supervisor (folder-watch + sleep + shell-outs);
/// the agents themselves live in sibling scopes and are NOT bounded by this.
pub const DEFAULT_MEMORY_MAX_MB: u64 = 1024;
pub const CUTOVER_RESTART_SEC: u64 = 2;
pub const CUTOVER_CANDIDATE_ENV: &str = "ST2_CUTOVER_CANDIDATE_UNIT";
#[cfg(debug_assertions)]
pub const CUTOVER_TEST_ORDINARY_UNIT_ENV: &str = "ST2_TEST_CUTOVER_ORDINARY_UNIT";

/// Everything the unit's `ExecStart` needs, resolved from the invoking environment.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// Absolute path to the `st2` binary (`env::current_exe()` at install time).
    exe: PathBuf,
    /// Absolute catalog (or spec-file) path handed to `st2 up`.
    catalog: PathBuf,
    /// Baked `--host`; `None` lets `st2 up` auto-detect the hostname (matching a manual `st2 up`).
    host: Option<String>,
    /// Explicit command search path inherited by st2 and every task it launches. A systemd user
    /// manager normally has only system directories on PATH, while pty/Codex commonly live under
    /// user-local or version-manager directories.
    path: String,
    /// Optional machine-local pty registry. This is deliberately independent of the synced catalog:
    /// an explicit adoption can use an existing registry without syncing pid/socket state.
    pty_root: Option<PathBuf>,
    memory_max_mb: u64,
}

#[derive(Debug, Clone)]
pub struct CutoverCandidateServiceSpec {
    pub exe: PathBuf,
    pub catalog: PathBuf,
    pub request: PathBuf,
    pub request_sha256: String,
    pub host: String,
    pub gate_id: String,
    pub unit_name: String,
    #[cfg(debug_assertions)]
    test_ordinary_unit: Option<String>,
}

impl CutoverCandidateServiceSpec {
    pub fn new(
        exe: PathBuf,
        catalog: PathBuf,
        request: PathBuf,
        request_sha256: String,
        host: String,
        gate_id: String,
    ) -> Result<Self> {
        if !exe.is_absolute() || !catalog.is_absolute() || !request.is_absolute() {
            bail!("cutover candidate executable, catalog, and request paths must be absolute");
        }
        if request_sha256.len() != 64
            || !request_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("cutover candidate request sha256 must be lowercase hexadecimal");
        }
        let unit_name = format!("st2-cutover-{request_sha256}.service");
        #[cfg(debug_assertions)]
        let test_ordinary_unit = std::env::var(CUTOVER_TEST_ORDINARY_UNIT_ENV)
            .ok()
            .map(|unit| validate_test_ordinary_unit(&unit).map(|()| unit))
            .transpose()?;
        Ok(Self {
            exe,
            catalog,
            request,
            request_sha256,
            host,
            gate_id,
            unit_name,
            #[cfg(debug_assertions)]
            test_ordinary_unit,
        })
    }

    #[cfg(debug_assertions)]
    pub fn with_test_ordinary_unit(mut self, unit: String) -> Result<Self> {
        validate_test_ordinary_unit(&unit)?;
        self.test_ordinary_unit = Some(unit);
        Ok(self)
    }

    fn program_arguments(&self) -> Vec<String> {
        vec![
            self.exe.display().to_string(),
            "--catalog".to_owned(),
            self.catalog.display().to_string(),
            "cutover".to_owned(),
            "run".to_owned(),
            "--request".to_owned(),
            self.request.display().to_string(),
            "--expect-request-sha256".to_owned(),
            self.request_sha256.clone(),
        ]
    }
}

pub fn render_cutover_candidate_unit(spec: &CutoverCandidateServiceSpec) -> String {
    let exec_start = spec
        .program_arguments()
        .iter()
        .map(|arg| systemd_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    #[cfg(debug_assertions)]
    let test_environment = spec
        .test_ordinary_unit
        .as_ref()
        .map(|unit| {
            format!(
                "Environment={}\n",
                systemd_quote_arg(&format!("{CUTOVER_TEST_ORDINARY_UNIT_ENV}={unit}"))
            )
        })
        .unwrap_or_default();
    #[cfg(not(debug_assertions))]
    let test_environment = "";
    format!(
        "[Unit]\n\
Description=st2 exact catalog cutover candidate\n\
After=default.target\n\
\n\
[Service]\n\
Type=simple\n\
Environment={}\n\
{test_environment}\
ExecStart={exec_start}\n\
Restart=always\n\
RestartSec={}s\n\
WorkingDirectory={}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        systemd_quote_arg(&format!("{CUTOVER_CANDIDATE_ENV}={}", spec.unit_name)),
        CUTOVER_RESTART_SEC,
        systemd_quote_arg(&spec.catalog.display().to_string()),
    )
}

pub fn install_cutover_candidate(spec: &CutoverCandidateServiceSpec) -> Result<PathBuf> {
    install_cutover_candidate_systemd(spec)
}

#[cfg(target_os = "linux")]
const CUTOVER_CANDIDATE_PROPERTIES: [&str; 10] = [
    "MainPID",
    "ActiveState",
    "LoadState",
    "Restart",
    "RestartUSec",
    "FragmentPath",
    "DropInPaths",
    "NeedDaemonReload",
    "UnitFileState",
    "Transient",
];

#[cfg(target_os = "linux")]
fn read_cutover_candidate_properties(unit_name: &str) -> Result<BTreeMap<String, String>> {
    let properties = CUTOVER_CANDIDATE_PROPERTIES.join(",");
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            unit_name,
            &format!("--property={properties}"),
        ])
        .output()
        .context("inspect loaded cutover candidate unit")?;
    if !output.status.success() {
        bail!("systemd refused loaded cutover candidate unit inspection");
    }
    let output =
        String::from_utf8(output.stdout).context("candidate unit properties are not UTF-8")?;
    parse_cutover_candidate_properties(&output)
}

#[cfg(target_os = "linux")]
fn parse_cutover_candidate_properties(output: &str) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for line in output.lines() {
        let (property, value) = line
            .split_once('=')
            .with_context(|| format!("malformed candidate unit property: {line:?}"))?;
        if !CUTOVER_CANDIDATE_PROPERTIES.contains(&property) {
            bail!("unexpected candidate unit property {property}");
        }
        if parsed
            .insert(property.to_owned(), value.to_owned())
            .is_some()
        {
            bail!("duplicate candidate unit property {property}");
        }
    }
    for property in CUTOVER_CANDIDATE_PROPERTIES {
        if !parsed.contains_key(property) {
            bail!("candidate unit property {property} is missing");
        }
    }
    Ok(parsed)
}

#[cfg(target_os = "linux")]
fn validate_cutover_candidate_properties(
    expected_fragment: &Path,
    expected_pid: Option<u32>,
    properties: &BTreeMap<String, String>,
) -> Result<()> {
    let observed = |property: &str| {
        properties
            .get(property)
            .map(String::as_str)
            .with_context(|| format!("candidate unit property {property} is missing"))
    };
    for (property, expected) in [
        ("ActiveState", "active".to_owned()),
        ("LoadState", "loaded".to_owned()),
        ("Restart", "always".to_owned()),
        ("RestartUSec", format!("{}s", CUTOVER_RESTART_SEC)),
        ("NeedDaemonReload", "no".to_owned()),
        ("UnitFileState", "enabled".to_owned()),
        ("Transient", "no".to_owned()),
    ] {
        let value = observed(property)?;
        if value != expected {
            bail!("candidate unit {property} mismatch: expected {expected}, found {value}");
        }
    }
    let main_pid = observed("MainPID")?;
    match expected_pid {
        Some(expected) if main_pid != expected.to_string() => {
            bail!("candidate unit MainPID mismatch: expected {expected}, found {main_pid}");
        }
        Some(_) => {}
        None => {
            let parsed = main_pid
                .parse::<u32>()
                .context("candidate unit MainPID is not an unsigned integer")?;
            if parsed == 0 {
                bail!("candidate unit MainPID must identify a live candidate process");
            }
        }
    }
    let drop_ins = observed("DropInPaths")?;
    if !drop_ins.is_empty() {
        bail!("candidate unit has unapproved drop-ins: {drop_ins}");
    }
    let fragment = PathBuf::from(observed("FragmentPath")?);
    if !fragment.is_absolute() {
        bail!("candidate unit FragmentPath is not absolute");
    }
    let fragment = fragment.canonicalize().with_context(|| {
        format!(
            "canonicalize loaded candidate fragment {}",
            fragment.display()
        )
    })?;
    if fragment != expected_fragment {
        bail!(
            "candidate unit FragmentPath mismatch: expected {}, found {}",
            expected_fragment.display(),
            fragment.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn validate_cutover_candidate_process(spec: &CutoverCandidateServiceSpec) -> Result<()> {
    let observed = env::var(CUTOVER_CANDIDATE_ENV)
        .context("cutover run must execute from its dedicated candidate systemd service")?;
    if observed != spec.unit_name {
        bail!(
            "cutover candidate unit mismatch: expected {}, found {observed}",
            spec.unit_name
        );
    }
    env::var_os("INVOCATION_ID")
        .context("cutover candidate is not running under a systemd service invocation")?;
    validate_cutover_candidate_loaded_artifact(spec, Some(std::process::id()))
}

#[cfg(target_os = "linux")]
fn validate_cutover_candidate_loaded_artifact(
    spec: &CutoverCandidateServiceSpec,
    expected_pid: Option<u32>,
) -> Result<()> {
    let path = systemd_user_unit_path_named(&spec.unit_name)?;
    let mut unit_file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&path)
        .with_context(|| format!("open cutover candidate unit {}", path.display()))?;
    let metadata = unit_file
        .metadata()
        .with_context(|| format!("inspect cutover candidate unit {}", path.display()))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!(
            "cutover candidate unit is not a real regular file: {}",
            path.display()
        );
    }
    let expected_bytes = render_cutover_candidate_unit(spec).into_bytes();
    if metadata.len() != expected_bytes.len() as u64 {
        bail!(
            "installed cutover candidate unit differs from the exact request artifact: {}",
            path.display()
        );
    }
    let expected_fragment = path
        .canonicalize()
        .with_context(|| format!("canonicalize cutover candidate unit {}", path.display()))?;
    let before = read_cutover_candidate_properties(&spec.unit_name)?;
    validate_cutover_candidate_properties(&expected_fragment, expected_pid, &before)?;
    let mut bytes = Vec::new();
    (&mut unit_file)
        .take(expected_bytes.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!(
                "read cutover candidate unit {}",
                expected_fragment.display()
            )
        })?;
    if bytes != expected_bytes {
        bail!(
            "installed cutover candidate unit differs from the exact request artifact: {}",
            expected_fragment.display()
        );
    }
    let after = read_cutover_candidate_properties(&spec.unit_name)?;
    if before != after {
        bail!("candidate unit properties changed during exact artifact validation");
    }
    validate_cutover_candidate_properties(&expected_fragment, expected_pid, &after)?;
    let current = fs::symlink_metadata(&path)
        .with_context(|| format!("reinspect cutover candidate unit {}", path.display()))?;
    if !current.is_file()
        || current.file_type().is_symlink()
        || (current.dev(), current.ino()) != (metadata.dev(), metadata.ino())
    {
        bail!("candidate unit file identity changed during exact artifact validation");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn validate_cutover_candidate_process(_spec: &CutoverCandidateServiceSpec) -> Result<()> {
    unsupported()
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        catalog: impl Into<PathBuf>,
        host: Option<String>,
        path: impl Into<String>,
        pty_root: Option<PathBuf>,
        memory_max_mb: u64,
    ) -> Result<Self> {
        if memory_max_mb == 0 {
            bail!("--memory-max-mb must be greater than zero");
        }
        let path = path.into();
        if path.is_empty() {
            bail!("service PATH cannot be empty");
        }
        if pty_root.as_ref().is_some_and(|root| !root.is_absolute()) {
            bail!("--pty-root must be an absolute path");
        }
        Ok(Self {
            exe: exe.into(),
            catalog: catalog.into(),
            host,
            path,
            pty_root,
            memory_max_mb,
        })
    }

    /// The `ExecStart` argv: `<st2> up --catalog <catalog> [--host <h>]`.
    fn program_arguments(&self) -> Vec<String> {
        let mut args = vec![
            self.exe.display().to_string(),
            "up".to_string(),
            "--catalog".to_string(),
            self.catalog.display().to_string(),
        ];
        if let Some(host) = &self.host {
            args.push("--host".to_string());
            args.push(host.clone());
        }
        args
    }

    /// Read-only durable-cutover admission check run immediately before each supervisor start.
    fn preflight_arguments(&self) -> Vec<String> {
        let mut args = vec![
            self.exe.display().to_string(),
            "--catalog".to_string(),
            self.catalog.display().to_string(),
            "cutover".to_string(),
            "status".to_string(),
        ];
        if let Some(host) = &self.host {
            args.push("--host".to_string());
            args.push(host.clone());
        }
        args.push("--json".to_string());
        args
    }
}

/// `st2 service install [--catalog <catalog>] [--host H] [--pty-root PATH]
/// [--memory-max-mb N]`.
pub fn install(
    catalog: &Path,
    host: Option<String>,
    pty_root: Option<PathBuf>,
    memory_max_mb: u64,
) -> Result<()> {
    let exe = env::current_exe().context("failed to resolve the current st2 executable")?;
    let path = service_path(&exe)?;
    // A systemd unit runs from no shell and no cwd — the catalog MUST be absolute, and it must exist
    // now (you install the service against an existing catalog, not a future one).
    let catalog = catalog.canonicalize().with_context(|| {
        format!(
            "catalog {} does not exist — create it before installing the service",
            catalog.display()
        )
    })?;
    let pty_root = pty_root
        .map(|root| {
            root.canonicalize()
                .with_context(|| format!("pty root {} does not exist", root.display()))
        })
        .transpose()?;
    let _topology_lock = lock_service_topology()?;
    reject_installed_cutover_successor()?;
    require_service_mutation_admission(&catalog, host.as_deref())?;
    let spec = ServiceSpec::new(exe, &catalog, host, path, pty_root, memory_max_mb)?;

    install_systemd_user(&spec)?;

    println!("installed");
    println!("catalog\t{}", catalog.display());
    match &spec.host {
        Some(h) => println!("host\t{h}"),
        None => println!("host\t(auto-detected at runtime)"),
    }
    match &spec.pty_root {
        Some(root) => println!("pty-root\t{}", root.display()),
        None => println!("pty-root\t{}/pty (catalog default)", catalog.display()),
    }
    println!("memory-max-mb\t{memory_max_mb}");
    Ok(())
}

/// Persist the invoking PATH in the unit, prepending the installed st2 directory when necessary.
/// This makes the unit independent of systemd's sparse manager environment while retaining the
/// exact version-manager/user-local directories the operator verified at install time.
fn service_path(exe: &Path) -> Result<String> {
    let ambient = env::var_os("PATH").context("PATH is not set")?;
    let mut entries: Vec<PathBuf> = env::split_paths(&ambient).collect();
    if let Some(parent) = exe.parent()
        && !entries.iter().any(|entry| entry == parent)
    {
        entries.insert(0, parent.to_path_buf());
    }
    env::join_paths(entries)
        .context("service PATH contains an unsupported byte")
        .map(|path| path.to_string_lossy().into_owned())
}

/// `st2 service status` — show the unit's systemd status.
pub fn status() -> Result<()> {
    status_systemd_user()
}

/// `st2 service uninstall` — stop, disable, and remove the unit. Idempotent.
pub fn uninstall(catalog: &Path) -> Result<()> {
    let catalog = catalog.canonicalize().with_context(|| {
        format!(
            "catalog {} does not exist — select the installed service catalog before uninstalling",
            catalog.display()
        )
    })?;
    require_service_mutation_admission(&catalog, None)?;
    uninstall_systemd_user()?;
    println!("uninstalled");
    Ok(())
}

fn require_service_mutation_admission(catalog: &Path, host: Option<&str>) -> Result<()> {
    let catalog = crate::cutover_admission::CanonicalCatalog::open(catalog)?;
    let host = crate::cutover_admission::HostId::parse(
        host.map(ToOwned::to_owned)
            .unwrap_or_else(crate::detect_host),
    )?;
    match crate::cutover_admission::probe_mutation_admission(&catalog, Some(&host))? {
        crate::cutover_admission::MutationAdmission::Available => Ok(()),
        crate::cutover_admission::MutationAdmission::Busy(busy) => bail!(
            "service mutation refused: {}",
            serde_json::to_string(&busy)?
        ),
    }
}

#[cfg(target_os = "linux")]
fn install_systemd_user(spec: &ServiceSpec) -> Result<()> {
    let unit_path = systemd_user_unit_path()?;
    if let Some(parent) = unit_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&unit_path, render_systemd_user_unit(spec))
        .with_context(|| format!("failed to write {}", unit_path.display()))?;

    // daemon-reload → enable (boot) → restart (start now, idempotent over a running instance).
    run_command("systemctl", &["--user", "daemon-reload"])?;
    run_command("systemctl", &["--user", "enable", SERVICE_NAME])?;
    run_command("systemctl", &["--user", "restart", SERVICE_NAME])?;
    println!("unit\t{}", unit_path.display());
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_cutover_candidate_systemd(spec: &CutoverCandidateServiceSpec) -> Result<PathBuf> {
    let unit_path = systemd_user_unit_path_named(&spec.unit_name)?;
    let unit_dir = unit_path.parent().context("unit has no parent")?;
    fs::create_dir_all(unit_dir)
        .with_context(|| format!("create systemd user unit directory {}", unit_dir.display()))?;
    File::open(unit_dir)?.sync_all()?;
    let topology_lock = lock_service_topology_in(unit_dir)?;
    publish_cutover_candidate_unit_locked(spec, unit_dir, &topology_lock)?;
    run_command("systemctl", &["--user", "daemon-reload"])?;
    run_command("systemctl", &["--user", "enable", "--now", &spec.unit_name])?;
    validate_cutover_candidate_topology_locked(spec, unit_dir, &topology_lock)?;
    validate_cutover_candidate_loaded_artifact(spec, None)?;
    drop(topology_lock);
    Ok(unit_path)
}

#[cfg(all(target_os = "linux", test))]
fn publish_cutover_candidate_unit(
    spec: &CutoverCandidateServiceSpec,
    unit_dir: &Path,
) -> Result<PathBuf> {
    fs::create_dir_all(unit_dir)
        .with_context(|| format!("create systemd user unit directory {}", unit_dir.display()))?;
    File::open(unit_dir)?.sync_all()?;
    let topology_lock = lock_service_topology_in(unit_dir)?;
    publish_cutover_candidate_unit_locked(spec, unit_dir, &topology_lock)
}

#[cfg(target_os = "linux")]
fn reject_other_cutover_candidate_locked(
    spec: &CutoverCandidateServiceSpec,
    unit_dir: &Path,
    topology_lock: &ServiceTopologyLock,
) -> Result<()> {
    topology_lock.validate_for(unit_dir)?;
    for entry in fs::read_dir(unit_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("st2-cutover-") && name.ends_with(".service") && name != spec.unit_name
        {
            bail!(
                "a different durable cutover successor unit already exists: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn publish_cutover_candidate_unit_locked(
    spec: &CutoverCandidateServiceSpec,
    unit_dir: &Path,
    topology_lock: &ServiceTopologyLock,
) -> Result<PathBuf> {
    reject_other_cutover_candidate_locked(spec, unit_dir, topology_lock)?;
    let unit_path = unit_dir.join(&spec.unit_name);
    let bytes = render_cutover_candidate_unit(spec).into_bytes();
    let mut stage = tempfile::Builder::new()
        .prefix(".st2-cutover-unit-")
        .tempfile_in(unit_dir)?;
    stage.as_file_mut().write_all(&bytes)?;
    stage.as_file().sync_all()?;
    match stage.persist_noclobber(&unit_path) {
        Ok(_) => File::open(unit_dir)?.sync_all()?,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(&unit_path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                bail!(
                    "cutover candidate unit is not a real regular file: {}",
                    unit_path.display()
                );
            }
            let existing = fs::read(&unit_path)?;
            if existing != bytes {
                bail!(
                    "cutover candidate unit exists with different bytes: {}",
                    unit_path.display()
                );
            }
        }
        Err(error) => return Err(error.error).context("publish cutover candidate unit"),
    }
    Ok(unit_path)
}

#[cfg(target_os = "linux")]
fn validate_cutover_candidate_topology_locked(
    spec: &CutoverCandidateServiceSpec,
    unit_dir: &Path,
    topology_lock: &ServiceTopologyLock,
) -> Result<()> {
    reject_other_cutover_candidate_locked(spec, unit_dir, topology_lock)?;
    let unit_path = unit_dir.join(&spec.unit_name);
    let metadata = fs::symlink_metadata(&unit_path)
        .with_context(|| format!("inspect durable candidate unit {}", unit_path.display()))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!(
            "cutover candidate unit is not a real regular file: {}",
            unit_path.display()
        );
    }
    if fs::read(&unit_path)? != render_cutover_candidate_unit(spec).as_bytes() {
        bail!(
            "installed cutover candidate unit differs from the exact request artifact: {}",
            unit_path.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct ServiceTopologyLock {
    _file: File,
    unit_dir: PathBuf,
}

#[cfg(target_os = "linux")]
impl ServiceTopologyLock {
    fn validate_for(&self, unit_dir: &Path) -> Result<()> {
        let expected = self.unit_dir.canonicalize()?;
        let observed = unit_dir.canonicalize()?;
        if observed != expected {
            bail!(
                "service topology lock belongs to {}, not {}",
                expected.display(),
                observed.display()
            );
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn lock_service_topology() -> Result<ServiceTopologyLock> {
    let unit = systemd_user_unit_path()?;
    let dir = unit.parent().context("systemd user unit has no parent")?;
    fs::create_dir_all(dir)?;
    lock_service_topology_in(dir)
}

#[cfg(target_os = "linux")]
fn lock_service_topology_in(unit_dir: &Path) -> Result<ServiceTopologyLock> {
    let lock_path = unit_dir.join(".st2-cutover-install.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(&lock_path)?;
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("lock service topology");
    }
    Ok(ServiceTopologyLock {
        _file: lock,
        unit_dir: unit_dir.canonicalize()?,
    })
}

#[cfg(not(target_os = "linux"))]
fn lock_service_topology() -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn retire_ordinary_supervisor_for_cutover() -> Result<()> {
    #[cfg(debug_assertions)]
    let service_name = match env::var(CUTOVER_TEST_ORDINARY_UNIT_ENV) {
        Ok(unit) => {
            validate_test_ordinary_unit(&unit)?;
            unit
        }
        Err(env::VarError::NotPresent) => SERVICE_NAME.to_owned(),
        Err(error) => return Err(error).context("read cutover test ordinary unit"),
    };
    #[cfg(not(debug_assertions))]
    let service_name = SERVICE_NAME.to_owned();
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", &service_name])
        .status();
    let ordinary = systemd_user_unit_path_named(&service_name)?;
    if ordinary.exists() {
        fs::remove_file(&ordinary)
            .with_context(|| format!("remove competing supervisor {}", ordinary.display()))?;
        File::open(ordinary.parent().context("ordinary unit has no parent")?)?.sync_all()?;
    }
    run_command("systemctl", &["--user", "daemon-reload"])
}

#[cfg(debug_assertions)]
fn validate_test_ordinary_unit(unit: &str) -> Result<()> {
    if !unit.starts_with("st2-cutover-e2e-ordinary-")
        || !unit.ends_with(".service")
        || unit.len() > 160
        || !unit
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!(
            "{CUTOVER_TEST_ORDINARY_UNIT_ENV} must be a safe st2-cutover-e2e-ordinary-*.service name"
        );
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn retire_ordinary_supervisor_for_cutover() -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn reject_installed_cutover_successor() -> Result<()> {
    let dir = systemd_user_unit_path()?
        .parent()
        .context("systemd user unit has no parent")?
        .to_path_buf();
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect systemd user units"),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("st2-cutover-") && name.ends_with(".service") {
            bail!(
                "ordinary st2.service cannot compete with durable cutover successor {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn reject_installed_cutover_successor() -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn install_cutover_candidate_systemd(_spec: &CutoverCandidateServiceSpec) -> Result<PathBuf> {
    unsupported()?;
    unreachable!()
}

#[cfg(not(target_os = "linux"))]
fn install_systemd_user(_spec: &ServiceSpec) -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn status_systemd_user() -> Result<()> {
    run_command(
        "systemctl",
        &["--user", "status", SERVICE_NAME, "--no-pager"],
    )
}

#[cfg(not(target_os = "linux"))]
fn status_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(target_os = "linux")]
fn uninstall_systemd_user() -> Result<()> {
    // Best-effort stop+disable (ignore "not loaded"), then remove the unit and reload.
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", SERVICE_NAME])
        .status();
    let unit_path = systemd_user_unit_path()?;
    if unit_path.exists() {
        fs::remove_file(&unit_path)
            .with_context(|| format!("failed to remove {}", unit_path.display()))?;
    }
    run_command("systemctl", &["--user", "daemon-reload"])?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn uninstall_systemd_user() -> Result<()> {
    unsupported()
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> Result<()> {
    bail!(
        "st2 service is Linux/systemd-user only (headless hosts like hetz). On macOS, run \
         `st2 up --catalog <catalog>` yourself — the Mac stays manual for TCC reasons (a launchd-owned \
         process can't inherit your GUI/keychain trust)."
    )
}

#[cfg(target_os = "linux")]
fn run_command(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {program} {}", args.join(" ")))?;
    if !status.success() {
        bail!("{program} {} failed with status {status}", args.join(" "));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

#[cfg(target_os = "linux")]
fn systemd_user_unit_path() -> Result<PathBuf> {
    let base = match env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => home_dir()?.join(".config"),
    };
    Ok(base.join("systemd/user").join(SERVICE_NAME))
}

fn systemd_user_unit_path_named(name: &str) -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let base = match env::var_os("XDG_CONFIG_HOME") {
            Some(path) => PathBuf::from(path),
            None => home_dir()?.join(".config"),
        };
        Ok(base.join("systemd/user").join(name))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = name;
        unsupported()?;
        unreachable!()
    }
}

/// Render the systemd-user unit. Pure (no I/O) so it is unit-testable on any OS.
pub fn render_systemd_user_unit(spec: &ServiceSpec) -> String {
    let exec_start_pre = spec
        .preflight_arguments()
        .iter()
        .map(|arg| systemd_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let exec_start = spec
        .program_arguments()
        .iter()
        .map(|arg| systemd_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
Description=st2 supervisor (st2 up)\n\
After=network.target\n\
\n\
[Service]\n\
Type=simple\n\
Environment={}\n\
{}\
ExecStartPre={exec_start_pre}\n\
ExecStart={exec_start}\n\
Restart=on-failure\n\
RestartSec=5s\n\
MemoryMax={}M\n\
WorkingDirectory={}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        systemd_quote_arg(&format!("PATH={}", spec.path)),
        spec.pty_root
            .as_ref()
            .map(|root| format!(
                "Environment={}\n",
                systemd_quote_arg(&format!("PTY_ROOT={}", root.display()))
            ))
            .unwrap_or_default(),
        spec.memory_max_mb,
        systemd_quote_arg(&spec.catalog.display().to_string())
    )
}

/// Quote an argument for a systemd `ExecStart` line: bare if it is all "safe" chars, else
/// double-quoted with systemd's escapes (`\`, `"`, `$`→`$$`, `%`→`%%`). Mirrors fabric.
fn systemd_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && arg.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'.' | b'_' | b':' | b'-' | b'+' | b'=')
        })
    {
        return arg.to_string();
    }

    let mut quoted = String::from("\"");
    for ch in arg.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '$' => quoted.push_str("$$"),
            '%' => quoted.push_str("%%"),
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_runs_st2_up_with_restart_and_memory_limit() -> Result<()> {
        let spec = ServiceSpec::new(
            "/home/user/.cargo/bin/st2",
            "/home/user/catalog",
            None,
            "/home/user/.cargo/bin:/home/user/.local/bin:/usr/bin",
            None,
            DEFAULT_MEMORY_MAX_MB,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(
            unit.contains("ExecStart=/home/user/.cargo/bin/st2 up --catalog /home/user/catalog")
        );
        assert!(unit.contains(
            "ExecStartPre=/home/user/.cargo/bin/st2 --catalog /home/user/catalog cutover status --json"
        ));
        // No --host baked when unset → st2 up auto-detects, same as a manual run.
        assert!(!unit.contains("--host"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("RestartSec=5s"));
        assert!(unit.contains("MemoryMax=1024M"));
        assert!(unit.contains("WorkingDirectory=/home/user/catalog"));
        assert!(
            unit.contains("Environment=PATH=/home/user/.cargo/bin:/home/user/.local/bin:/usr/bin")
        );
        assert!(!unit.contains("Environment=PTY_ROOT="));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(unit.contains("Description=st2 supervisor (st2 up)"));
        Ok(())
    }

    #[test]
    fn unit_bakes_host_when_provided() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/st2",
            "/srv/catalog",
            Some("hetz".to_string()),
            "/usr/local/bin:/usr/bin",
            Some(PathBuf::from("/srv/legacy-pty")),
            512,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(
            unit.contains("ExecStart=/usr/local/bin/st2 up --catalog /srv/catalog --host hetz")
        );
        assert!(unit.contains(
            "ExecStartPre=/usr/local/bin/st2 --catalog /srv/catalog cutover status --host hetz --json"
        ));
        assert!(unit.contains("MemoryMax=512M"));
        assert!(unit.contains("Environment=PTY_ROOT=/srv/legacy-pty"));
        Ok(())
    }

    #[test]
    fn cutover_candidate_unit_is_exact_and_restart_always() -> Result<()> {
        let spec = CutoverCandidateServiceSpec::new(
            PathBuf::from("/nix/store/st2/bin/st2"),
            PathBuf::from("/srv/catalog"),
            PathBuf::from("/srv/requests/cutover.json"),
            "a".repeat(64),
            "hetz".to_owned(),
            "gate-7".to_owned(),
        )?;
        let unit = render_cutover_candidate_unit(&spec);
        assert_eq!(
            spec.unit_name,
            format!("st2-cutover-{}.service", "a".repeat(64))
        );
        assert!(unit.contains(
            "ExecStart=/nix/store/st2/bin/st2 --catalog /srv/catalog cutover run --request /srv/requests/cutover.json --expect-request-sha256 "
        ));
        assert!(unit.contains("Restart=always"));
        assert!(unit.contains("RestartSec=2s"));
        assert!(unit.contains("Environment=ST2_CUTOVER_CANDIDATE_UNIT=st2-cutover-"));
        assert!(!unit.contains("ExecStartPre="));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn candidate_properties(fragment: &Path) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("MainPID".to_owned(), "4242".to_owned()),
            ("ActiveState".to_owned(), "active".to_owned()),
            ("LoadState".to_owned(), "loaded".to_owned()),
            ("Restart".to_owned(), "always".to_owned()),
            ("RestartUSec".to_owned(), "2s".to_owned()),
            ("FragmentPath".to_owned(), fragment.display().to_string()),
            ("DropInPaths".to_owned(), String::new()),
            ("NeedDaemonReload".to_owned(), "no".to_owned()),
            ("UnitFileState".to_owned(), "enabled".to_owned()),
            ("Transient".to_owned(), "no".to_owned()),
        ])
    }

    #[cfg(target_os = "linux")]
    fn candidate_fragment_fixture() -> Result<(tempfile::TempDir, PathBuf)> {
        let root = tempfile::tempdir()?;
        let fragment = root.path().join("st2-cutover-test.service");
        fs::write(&fragment, b"[Service]\nExecStart=/bin/true\n")?;
        Ok((root, fragment.canonicalize()?))
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_properties_accept_only_the_exact_durable_fragment() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        let output = candidate_properties(&fragment)
            .into_iter()
            .map(|(property, value)| format!("{property}={value}"))
            .collect::<Vec<_>>()
            .join("\n");
        let properties = parse_cutover_candidate_properties(&output)?;
        validate_cutover_candidate_properties(&fragment, Some(4242), &properties)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_rejects_runtime_fragment_shadow() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        let shadow_root = tempfile::tempdir()?;
        let shadow = shadow_root.path().join("st2-cutover-test.service");
        fs::write(&shadow, b"[Service]\nExecStart=/bin/true\n")?;
        let error = validate_cutover_candidate_properties(
            &fragment,
            Some(4242),
            &candidate_properties(&shadow),
        )
        .unwrap_err();
        assert!(error.to_string().contains("FragmentPath mismatch"));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_rejects_transient_unit() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        let mut properties = candidate_properties(&fragment);
        properties.insert("Transient".to_owned(), "yes".to_owned());
        let error =
            validate_cutover_candidate_properties(&fragment, Some(4242), &properties).unwrap_err();
        assert!(error.to_string().contains("Transient mismatch"));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_rejects_unapproved_drop_in() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        let mut properties = candidate_properties(&fragment);
        properties.insert(
            "DropInPaths".to_owned(),
            "/run/user/1000/systemd/user/st2-cutover-test.service.d/override.conf".to_owned(),
        );
        let error =
            validate_cutover_candidate_properties(&fragment, Some(4242), &properties).unwrap_err();
        assert!(error.to_string().contains("unapproved drop-ins"));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_rejects_stale_daemon_reload_state() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        let mut properties = candidate_properties(&fragment);
        properties.insert("NeedDaemonReload".to_owned(), "yes".to_owned());
        let error =
            validate_cutover_candidate_properties(&fragment, Some(4242), &properties).unwrap_err();
        assert!(error.to_string().contains("NeedDaemonReload mismatch"));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn loaded_candidate_rejects_nonpersistent_enablement() -> Result<()> {
        let (_root, fragment) = candidate_fragment_fixture()?;
        for state in ["disabled", "enabled-runtime", "transient"] {
            let mut properties = candidate_properties(&fragment);
            properties.insert("UnitFileState".to_owned(), state.to_owned());
            let error = validate_cutover_candidate_properties(&fragment, Some(4242), &properties)
                .unwrap_err();
            assert!(
                error.to_string().contains("UnitFileState mismatch"),
                "{state}: {error:#}"
            );
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cutover_candidate_publication_is_idempotent_and_excludes_other_requests() -> Result<()> {
        let root = tempfile::tempdir()?;
        let spec = CutoverCandidateServiceSpec::new(
            PathBuf::from("/nix/store/st2/bin/st2"),
            PathBuf::from("/srv/catalog"),
            PathBuf::from("/srv/requests/cutover.json"),
            "a".repeat(64),
            "hetz".to_owned(),
            "gate-7".to_owned(),
        )?;
        let left = spec.clone();
        let right = spec.clone();
        let dir_left = root.path().to_path_buf();
        let dir_right = root.path().to_path_buf();
        let one =
            std::thread::spawn(move || publish_cutover_candidate_unit(&left, &dir_left).unwrap());
        let two =
            std::thread::spawn(move || publish_cutover_candidate_unit(&right, &dir_right).unwrap());
        assert_eq!(one.join().unwrap(), two.join().unwrap());
        publish_cutover_candidate_unit(&spec, root.path())?;

        let other = CutoverCandidateServiceSpec::new(
            spec.exe.clone(),
            spec.catalog.clone(),
            spec.request.clone(),
            "b".repeat(64),
            spec.host.clone(),
            spec.gate_id.clone(),
        )?;
        let error = publish_cutover_candidate_unit(&other, root.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("different durable cutover successor")
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn post_enable_topology_validation_rejects_a_competing_candidate() -> Result<()> {
        let root = tempfile::tempdir()?;
        let spec = CutoverCandidateServiceSpec::new(
            PathBuf::from("/nix/store/st2/bin/st2"),
            PathBuf::from("/srv/catalog"),
            PathBuf::from("/srv/requests/cutover.json"),
            "a".repeat(64),
            "hetz".to_owned(),
            "gate-7".to_owned(),
        )?;
        publish_cutover_candidate_unit(&spec, root.path())?;
        let competing = root
            .path()
            .join(format!("st2-cutover-{}.service", "b".repeat(64)));
        fs::write(&competing, b"[Service]\nExecStart=/bin/false\n")?;

        let topology_lock = lock_service_topology_in(root.path())?;
        let error = validate_cutover_candidate_topology_locked(&spec, root.path(), &topology_lock)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("different durable cutover successor")
        );
        Ok(())
    }

    #[test]
    fn unit_quotes_paths_with_spaces_and_escapes_specifiers() -> Result<()> {
        let spec = ServiceSpec::new(
            "/opt/st2 tools/st2",
            "/srv/cat 100%",
            None,
            "/opt/st2 tools:/usr/bin",
            Some(PathBuf::from("/srv/pty 100%")),
            256,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(unit.contains("ExecStart=\"/opt/st2 tools/st2\" up --catalog \"/srv/cat 100%%\""));
        assert!(unit.contains(
            "ExecStartPre=\"/opt/st2 tools/st2\" --catalog \"/srv/cat 100%%\" cutover status --json"
        ));
        assert!(unit.contains("WorkingDirectory=\"/srv/cat 100%%\""));
        assert!(unit.contains("Environment=\"PATH=/opt/st2 tools:/usr/bin\""));
        assert!(unit.contains("Environment=\"PTY_ROOT=/srv/pty 100%%\""));
        Ok(())
    }

    #[test]
    fn zero_memory_max_is_rejected() {
        let err = ServiceSpec::new("/bin/st2", "/cat", None, "/bin", None, 0).unwrap_err();
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn empty_path_and_relative_pty_root_are_rejected() {
        let err = ServiceSpec::new("/bin/st2", "/cat", None, "", None, 1).unwrap_err();
        assert!(err.to_string().contains("PATH cannot be empty"));

        let err = ServiceSpec::new(
            "/bin/st2",
            "/cat",
            None,
            "/bin",
            Some(PathBuf::from("relative")),
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("absolute"));
    }
}
