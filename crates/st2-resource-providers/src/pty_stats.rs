use std::collections::BTreeMap;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;

use st2_resource_wasip2::{CapabilityModule, InvocationStore, ObservationRequest};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/pty-stats",
        world: "pty-stats-observer",
    });
}

use bindings::compoundingtech::st2_pty_stats::pty_stats::{
    ExitStatus, Host, Outcome, PtyStatsError, Scope,
};

const IMPORT_NAME: &str = "compoundingtech:st2-pty-stats/pty-stats@0.1.0";
const MAX_STDOUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PtyStatsScope {
    All,
    Session(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyStatsConfig {
    pub executable: PathBuf,
    pub cwd: PathBuf,
    pub scope: PtyStatsScope,
    pub deadline: Duration,
}

impl PtyStatsConfig {
    pub fn resolve(
        executable: impl AsRef<Path>,
        cwd: impl Into<PathBuf>,
        scope: PtyStatsScope,
        deadline: Duration,
    ) -> Result<Self, &'static str> {
        if deadline.is_zero() || deadline > Duration::from_secs(60) {
            return Err("PTY stats deadline is invalid");
        }
        let executable =
            resolve_executable(executable.as_ref()).ok_or("PTY executable is unavailable")?;
        let cwd = cwd.into();
        if !cwd.is_absolute() {
            return Err("PTY stats cwd must be absolute");
        }
        if let PtyStatsScope::Session(session) = &scope
            && (session.is_empty() || session.len() > 512 || session.contains('\0'))
        {
            return Err("PTY session scope is invalid");
        }
        Ok(Self {
            executable,
            cwd,
            scope,
            deadline,
        })
    }
}

#[derive(Clone)]
pub struct PtyStatsModule {
    config: PtyStatsConfig,
    pending: Arc<Mutex<BTreeMap<u64, Arc<InvocationControl>>>>,
}

impl PtyStatsModule {
    pub fn new(config: PtyStatsConfig) -> Self {
        Self {
            config,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Reserve the exact cancellation identity that the executor will hand to `begin`.
    pub fn prepare(&self, invocation_id: u64) -> PtyStatsCancellation {
        let control = Arc::new(InvocationControl::new());
        self.pending
            .lock()
            .insert(invocation_id, Arc::clone(&control));
        PtyStatsCancellation { control }
    }

    pub fn discard_prepared(&self, invocation_id: u64) -> bool {
        self.pending.lock().remove(&invocation_id).is_some()
    }
}


#[derive(Clone)]
pub struct PtyStatsCancellation {
    control: Arc<InvocationControl>,
}

impl PtyStatsCancellation {
    pub fn cancel(&self) -> bool {
        self.control.terminate(Termination::Cancelled)
    }
}

pub struct PtyStatsInvocation {
    config: PtyStatsConfig,
    control: Arc<InvocationControl>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Termination {
    None = 0,
    Cancelled = 1,
    TimedOut = 2,
}

struct InvocationControl {
    termination: AtomicU8,
    child: Mutex<ChildOwnership>,
}

enum ChildOwnership {
    Pending,
    Live(i32),
    Reaping,
    Reaped,
}

impl InvocationControl {
    fn new() -> Self {
        Self {
            termination: AtomicU8::new(Termination::None as u8),
            child: Mutex::new(ChildOwnership::Pending),
        }
    }

    fn termination(&self) -> Termination {
        match self.termination.load(Ordering::Acquire) {
            1 => Termination::Cancelled,
            2 => Termination::TimedOut,
            _ => Termination::None,
        }
    }

    fn install(&self, process_group: i32) {
        let mut ownership = self.child.lock();
        debug_assert!(matches!(*ownership, ChildOwnership::Pending));
        *ownership = ChildOwnership::Live(process_group);
        if self.termination() != Termination::None {
            let _ = kill_process_group(process_group);
        }
    }

    fn terminate(&self, reason: Termination) -> bool {
        let changed = self
            .termination
            .compare_exchange(
                Termination::None as u8,
                reason as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if changed {
            if let ChildOwnership::Live(process_group) = *self.child.lock() {
                let _ = kill_process_group(process_group);
            }
        }
        changed
    }

    fn wait_and_reap(
        &self,
        child: &mut std::process::Child,
    ) -> std::io::Result<std::process::ExitStatus> {
        wait_without_reaping(child.id())?;
        let mut ownership = self.child.lock();
        debug_assert!(matches!(*ownership, ChildOwnership::Live(_)));
        *ownership = ChildOwnership::Reaping;
        let status = child.wait();
        *ownership = ChildOwnership::Reaped;
        status
    }

    fn kill_and_reap(&self, child: &mut std::process::Child) {
        let mut ownership = self.child.lock();
        if let ChildOwnership::Live(process_group) = *ownership {
            let _ = kill_process_group(process_group);
        }
        *ownership = ChildOwnership::Reaping;
        let _ = child.wait();
        *ownership = ChildOwnership::Reaped;
    }
}


impl CapabilityModule for PtyStatsModule {
    type Invocation = PtyStatsInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::PtyStatsObserver::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, request: &ObservationRequest) -> Self::Invocation {
        let control = self
            .pending
            .lock()
            .remove(&request.invocation_id)
            .unwrap_or_else(|| Arc::new(InvocationControl::new()));
        PtyStatsInvocation {
            config: self.config.clone(),
            control,
        }
    }
}

impl Host for InvocationStore<PtyStatsInvocation> {
    fn get(&mut self, scope: Scope) -> Result<Outcome, PtyStatsError> {
        self.capability_mut().get(scope)
    }
}

impl PtyStatsInvocation {
    fn get(&mut self, scope: Scope) -> Result<Outcome, PtyStatsError> {
        if !scope_matches(&self.config.scope, &scope) {
            return Err(PtyStatsError::Denied);
        }
        if self.control.termination() == Termination::Cancelled {
            return Err(PtyStatsError::Cancelled);
        }
        let mut command = Command::new(&self.config.executable);
        command
            .arg("stats")
            .arg("--json")
            .current_dir(&self.config.cwd)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let PtyStatsScope::Session(session) = &self.config.scope {
            command.arg(session);
        }
        // SAFETY: this runs in the freshly-forked child before exec and calls only async-signal-safe
        // setpgid. The dedicated process group is the cancellation/reaping boundary.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let mut child = command.spawn().map_err(|_| PtyStatsError::Unavailable)?;
        self.control.install(child.id() as i32);
        let Some(stdout) = child.stdout.take() else {
            self.control.kill_and_reap(&mut child);
            return Err(PtyStatsError::Unavailable);
        };
        let Some(stderr) = child.stderr.take() else {
            self.control.kill_and_reap(&mut child);
            return Err(PtyStatsError::Unavailable);
        };
        let stdout_reader = match thread::Builder::new()
            .name("st2-pty-stats-stdout".into())
            .spawn(move || drain_bounded(stdout, MAX_STDOUT_BYTES))
        {
            Ok(reader) => reader,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                return Err(PtyStatsError::Unavailable);
            }
        };
        let stderr_reader = match thread::Builder::new()
            .name("st2-pty-stats-stderr".into())
            .spawn(move || drain_bounded(stderr, MAX_STDERR_BYTES))
        {
            Ok(reader) => reader,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                let _ = stdout_reader.join();
                return Err(PtyStatsError::Unavailable);
            }
        };
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let deadline = self.config.deadline;
        let deadline_control = Arc::clone(&self.control);
        let timer = match thread::Builder::new()
            .name("st2-pty-stats-deadline".into())
            .spawn(move || {
                if completed_rx.recv_timeout(deadline).is_err() {
                    deadline_control.terminate(Termination::TimedOut);
                }
            })
        {
            Ok(timer) => timer,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(PtyStatsError::Unavailable);
            }
        };
        let status = match self.control.wait_and_reap(&mut child) {
            Ok(status) => status,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                let _ = completed_tx.send(());
                let _ = timer.join();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(PtyStatsError::Unavailable);
            }
        };
        let _ = completed_tx.send(());
        let _ = timer.join();
        let (stdout, stdout_truncated) = stdout_reader
            .join()
            .map_err(|_| PtyStatsError::Unavailable)??;
        let (stderr, stderr_truncated) = stderr_reader
            .join()
            .map_err(|_| PtyStatsError::Unavailable)??;
        match self.control.termination() {
            Termination::Cancelled => return Err(PtyStatsError::Cancelled),
            Termination::TimedOut => return Err(PtyStatsError::DeadlineExceeded),
            Termination::None => {}
        }
        if stdout_truncated || stderr_truncated {
            return Err(PtyStatsError::ResourceExhausted);
        }
        let exit = status.code().map_or_else(
            || ExitStatus::Signal(status.signal().unwrap_or(0)),
            ExitStatus::Code,
        );
        Ok(Outcome {
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
            exit,
        })
    }
}

fn scope_matches(configured: &PtyStatsScope, requested: &Scope) -> bool {
    match (configured, requested) {
        (PtyStatsScope::All, Scope::All) => true,
        (PtyStatsScope::Session(configured), Scope::Session(requested)) => configured == requested,
        _ => false,
    }
}

fn drain_bounded(
    mut input: impl std::io::Read,
    limit: usize,
) -> Result<(Vec<u8>, bool), PtyStatsError> {
    let mut retained = Vec::with_capacity(limit.min(64 * 1024));
    let mut truncated = false;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = input.read(&mut buffer).map_err(|_| PtyStatsError::Unavailable)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(retained.len());
        retained
            .write_all(&buffer[..read.min(remaining)])
            .map_err(|_| PtyStatsError::Unavailable)?;
        truncated |= read > remaining;
    }
    Ok((retained, truncated))
}

fn kill_process_group(process_group: i32) -> bool {
    // SAFETY: negative pid addresses the process group created by pre_exec; SIGKILL is required to
    // make the deadline a hard bound even when the provider subprocess ignores graceful signals.
    unsafe { libc::kill(-process_group, libc::SIGKILL) == 0 }
}

fn wait_without_reaping(pid: u32) -> std::io::Result<()> {
    loop {
        // SAFETY: `info` is initialized for the kernel, and WNOWAIT deliberately keeps the child
        // waitable so its process-group identity cannot be recycled before ownership is fenced.
        let result = unsafe {
            let mut info = std::mem::zeroed::<libc::siginfo_t>();
            libc::waitid(
                libc::P_PID,
                pid,
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
fn resolve_executable(executable: &Path) -> Option<PathBuf> {
    if executable.is_absolute() {
        return executable_is_runnable(executable).then(|| executable.to_path_buf());
    }
    let validation_cwd = std::env::current_dir().ok()?;
    let search_path = std::env::var_os("PATH");
    resolve_executable_at(
        executable,
        &validation_cwd,
        search_path.as_deref(),
    )
}

fn resolve_executable_at(
    executable: &Path,
    validation_cwd: &Path,
    search_path: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    debug_assert!(validation_cwd.is_absolute());
    if executable.components().count() > 1 {
        let candidate = validation_cwd.join(executable);
        return executable_is_runnable(&candidate).then_some(candidate);
    }
    let search_path = search_path?;
    std::env::split_paths(search_path)
        .map(|directory| {
            let directory = if directory.is_absolute() {
                directory
            } else {
                validation_cwd.join(directory)
            };
            directory.join(executable)
        })
        .find(|candidate| executable_is_runnable(candidate))
}

fn executable_is_runnable(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| {
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    #[test]
    fn discarded_preparation_does_not_leak_pending_invocation_state() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("pty-fixture");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let module = PtyStatsModule::new(
            PtyStatsConfig::resolve(
                &executable,
                temporary.path().to_path_buf(),
                PtyStatsScope::All,
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        let _cancellation = module.prepare(42);
        assert_eq!(module.pending.lock().len(), 1);
        assert!(module.discard_prepared(42));
        assert!(module.pending.lock().is_empty());
    }

    use super::*;

    fn write_executable(path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    fn invoke(config: PtyStatsConfig) -> Outcome {
        PtyStatsInvocation {
            config,
            control: Arc::new(InvocationControl::new()),
        }
        .get(Scope::All)
        .unwrap()
    }

    #[test]
    fn slash_relative_executable_stays_bound_to_validation_cwd() {
        let validation_cwd = std::env::current_dir().unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("st2-pty-resolution-")
            .tempdir_in(&validation_cwd)
            .unwrap();
        let configured_cwd = temporary.path().join("configured");
        std::fs::create_dir_all(temporary.path().join("tools")).unwrap();
        let validated_executable = temporary.path().join("tools/pty-stats");
        write_executable(
            &validated_executable,
            "#!/bin/sh\nprintf 'validated\\n'\n",
        );
        let relative_executable = validated_executable.strip_prefix(&validation_cwd).unwrap();
        let rebound_executable = configured_cwd.join(relative_executable);
        std::fs::create_dir_all(rebound_executable.parent().unwrap()).unwrap();
        write_executable(&rebound_executable, "#!/bin/sh\nprintf 'rebound\\n'\n");

        let config = PtyStatsConfig::resolve(
            relative_executable,
            configured_cwd,
            PtyStatsScope::All,
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(config.executable.is_absolute());
        assert_eq!(config.executable, validated_executable);
        assert_eq!(invoke(config).stdout, b"validated\n");
    }

    #[test]
    fn relative_path_entry_stays_bound_to_validation_cwd() {
        let temporary = tempfile::tempdir().unwrap();
        let validation_cwd = temporary.path().join("validation");
        let configured_cwd = temporary.path().join("configured");
        std::fs::create_dir_all(validation_cwd.join("bin")).unwrap();
        std::fs::create_dir_all(configured_cwd.join("bin")).unwrap();
        write_executable(
            &validation_cwd.join("bin/pty-stats"),
            "#!/bin/sh\nprintf 'validated-path\\n'\n",
        );
        write_executable(
            &configured_cwd.join("bin/pty-stats"),
            "#!/bin/sh\nprintf 'rebound-path\\n'\n",
        );

        let executable = resolve_executable_at(
            Path::new("pty-stats"),
            &validation_cwd,
            Some(std::ffi::OsStr::new("bin")),
        )
        .unwrap();
        assert!(executable.is_absolute());
        assert_eq!(executable, validation_cwd.join("bin/pty-stats"));
        let outcome = invoke(PtyStatsConfig {
            executable,
            cwd: configured_cwd,
            scope: PtyStatsScope::All,
            deadline: Duration::from_secs(1),
        });
        assert_eq!(outcome.stdout, b"validated-path\n");
    }

    #[test]
    fn fixed_command_deadline_kills_and_reaps_the_process_group() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("blocked-pty");
        let fifo = temporary.path().join("block");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: the pathname is a live NUL-terminated byte string owned for the call.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        std::fs::write(
            &executable,
            "#!/bin/sh\nexec 3< \"$PWD/block\"\nread value <&3\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let config = PtyStatsConfig::resolve(
            &executable,
            temporary.path(),
            PtyStatsScope::All,
            Duration::from_millis(100),
        )
        .unwrap();
        let control = Arc::new(InvocationControl::new());
        let mut invocation = PtyStatsInvocation {
            config,
            control: Arc::clone(&control),
        };
        assert!(matches!(
            invocation.get(Scope::All),
            Err(PtyStatsError::DeadlineExceeded)
        ));
        assert!(matches!(*control.child.lock(), ChildOwnership::Reaped));
    }

    #[test]
    #[ignore = "requires packaged pty: cargo test -p st2-resource-providers pty_stats_live_json -- --ignored"]
    fn pty_stats_live_json() {
        let config = PtyStatsConfig::resolve(
            "pty",
            "/",
            PtyStatsScope::All,
            Duration::from_secs(10),
        )
        .unwrap();
        let mut invocation = PtyStatsInvocation {
            config,
            control: Arc::new(InvocationControl::new()),
        };
        let outcome = invocation.get(Scope::All).unwrap();
        assert!(matches!(outcome.exit, ExitStatus::Code(0)));
        serde_json::from_slice::<serde_json::Value>(&outcome.stdout).unwrap();
    }
}
