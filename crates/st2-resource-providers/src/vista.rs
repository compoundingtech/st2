use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use st2_resource_wasip2::{
    CapabilityContext, CapabilityModule, InterruptionReason,
    InvocationControl as ExecutorInvocationControl, InvocationStore,
};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/vista",
        world: "vista-provider",
    });
}

use bindings::compoundingtech::st2_vista::vista::{
    ArtifactRequest, ExitStatus, Host, Outcome, VistaError,
};

const IMPORT_NAME: &str = "compoundingtech:st2-vista/vista@0.1.0";
const MAX_STDOUT_BYTES: usize = 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_VERSION: u64 = 9_999_999_999_999_999_999;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VistaConfig {
    pub executable: PathBuf,
    pub cwd: PathBuf,
    pub deadline: Duration,
}

impl VistaConfig {
    pub fn resolve(
        executable: impl AsRef<Path>,
        cwd: impl Into<PathBuf>,
        deadline: Duration,
    ) -> Result<Self, &'static str> {
        if deadline.is_zero() || deadline > Duration::from_secs(60) {
            return Err("Vista deadline is invalid");
        }
        let executable =
            resolve_executable(executable.as_ref()).ok_or("Vista executable is unavailable")?;
        let cwd = cwd.into();
        if !cwd.is_absolute() {
            return Err("Vista cwd must be absolute");
        }
        Ok(Self {
            executable,
            cwd,
            deadline,
        })
    }
}

#[derive(Clone)]
pub struct VistaModule {
    config: VistaConfig,
}

impl VistaModule {
    pub fn new(config: VistaConfig) -> Self {
        Self { config }
    }
}

pub struct VistaInvocation {
    config: VistaConfig,
    control: Arc<ProcessControl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Termination {
    None = 0,
    Cancelled = 1,
    TimedOut = 2,
}

struct ProcessControl {
    termination: AtomicU8,
    child: Mutex<ChildOwnership>,
    #[cfg(not(test))]
    invocation: ExecutorInvocationControl,
    #[cfg(test)]
    invocation: Option<ExecutorInvocationControl>,
}

enum ChildOwnership {
    Pending,
    Live(i32),
    Reaping,
    Reaped,
}

impl ProcessControl {
    fn new(invocation: ExecutorInvocationControl) -> Self {
        Self {
            termination: AtomicU8::new(Termination::None as u8),
            child: Mutex::new(ChildOwnership::Pending),
            #[cfg(not(test))]
            invocation,
            #[cfg(test)]
            invocation: Some(invocation),
        }
    }

    #[cfg(test)]
    fn detached() -> Self {
        Self {
            termination: AtomicU8::new(Termination::None as u8),
            child: Mutex::new(ChildOwnership::Pending),
            invocation: None,
        }
    }

    fn termination(&self) -> Termination {
        match self.termination.load(Ordering::Acquire) {
            1 => Termination::Cancelled,
            2 => Termination::TimedOut,
            _ => match self.executor_interruption() {
                Some(InterruptionReason::Cancelled) => Termination::Cancelled,
                Some(InterruptionReason::TimedOut) => Termination::TimedOut,
                None => Termination::None,
            },
        }
    }

    fn executor_interruption(&self) -> Option<InterruptionReason> {
        #[cfg(not(test))]
        {
            self.invocation.interruption_reason()
        }
        #[cfg(test)]
        {
            self.invocation
                .as_ref()
                .and_then(ExecutorInvocationControl::interruption_reason)
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
        if changed
            && let ChildOwnership::Live(process_group) = *self.child.lock()
        {
            let _ = kill_process_group(process_group);
        }
        changed
    }

    fn synchronize_interruption(&self) -> Termination {
        let reason = self.termination();
        if reason != Termination::None {
            self.terminate(reason);
        }
        reason
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

impl CapabilityModule for VistaModule {
    type Invocation = VistaInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::VistaProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, context: CapabilityContext<'_>) -> Self::Invocation {
        VistaInvocation {
            config: self.config.clone(),
            control: Arc::new(ProcessControl::new(context.control().clone())),
        }
    }
}

impl Host for InvocationStore<VistaInvocation> {
    fn get(&mut self, request: ArtifactRequest) -> Result<Outcome, VistaError> {
        self.capability_mut().get(request)
    }
}

impl VistaInvocation {
    fn get(&mut self, request: ArtifactRequest) -> Result<Outcome, VistaError> {
        if !request_is_valid(&request) {
            return Err(VistaError::Denied);
        }
        match self.control.termination() {
            Termination::Cancelled => return Err(VistaError::Cancelled),
            Termination::TimedOut => return Err(VistaError::DeadlineExceeded),
            Termination::None => {}
        }

        let mut command = Command::new(&self.config.executable);
        command
            .arg("artifact")
            .arg("get")
            .arg(&request.slug)
            .arg(format!("v{}", request.version))
            .arg("--output")
            .arg("json")
            .current_dir(&self.config.cwd)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
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
        let mut child = command.spawn().map_err(|_| VistaError::Unavailable)?;
        self.control.install(child.id() as i32);
        let Some(stdout) = child.stdout.take() else {
            self.control.kill_and_reap(&mut child);
            return Err(VistaError::Unavailable);
        };
        let Some(stderr) = child.stderr.take() else {
            self.control.kill_and_reap(&mut child);
            return Err(VistaError::Unavailable);
        };
        let stdout_reader = match thread::Builder::new()
            .name("st2-vista-stdout".into())
            .spawn(move || drain_bounded(stdout, MAX_STDOUT_BYTES))
        {
            Ok(reader) => reader,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                return Err(VistaError::Unavailable);
            }
        };
        let stderr_reader = match thread::Builder::new()
            .name("st2-vista-stderr".into())
            .spawn(move || drain_bounded(stderr, MAX_STDERR_BYTES))
        {
            Ok(reader) => reader,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                let _ = stdout_reader.join();
                return Err(VistaError::Unavailable);
            }
        };
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let deadline = Instant::now() + self.config.deadline;
        let deadline_control = Arc::clone(&self.control);
        let timer = match thread::Builder::new()
            .name("st2-vista-deadline".into())
            .spawn(move || loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    deadline_control.terminate(Termination::TimedOut);
                    return;
                }
                match completed_rx.recv_timeout(remaining.min(Duration::from_millis(10))) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                if deadline_control.synchronize_interruption() != Termination::None {
                    return;
                }
            })
        {
            Ok(timer) => timer,
            Err(_) => {
                self.control.kill_and_reap(&mut child);
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(VistaError::Unavailable);
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
                return Err(VistaError::Unavailable);
            }
        };
        let _ = completed_tx.send(());
        let _ = timer.join();
        let (stdout, stdout_truncated) = stdout_reader
            .join()
            .map_err(|_| VistaError::Unavailable)??;
        let (stderr, stderr_truncated) = stderr_reader
            .join()
            .map_err(|_| VistaError::Unavailable)??;
        match self.control.termination() {
            Termination::Cancelled => return Err(VistaError::Cancelled),
            Termination::TimedOut => return Err(VistaError::DeadlineExceeded),
            Termination::None => {}
        }
        if stdout_truncated || stderr_truncated {
            return Err(VistaError::ResourceExhausted);
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

fn request_is_valid(request: &ArtifactRequest) -> bool {
    valid_slug(&request.slug) && (1..=MAX_VERSION).contains(&request.version)
}

fn valid_slug(slug: &str) -> bool {
    !slug.is_empty()
        && slug.len() <= 128
        && slug.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || (byte == b'-' && index > 0)
        })
        && !slug.ends_with('-')
        && !slug.contains("--")
}

fn drain_bounded(
    mut input: impl std::io::Read,
    limit: usize,
) -> Result<(Vec<u8>, bool), VistaError> {
    let mut retained = Vec::with_capacity(limit.min(64 * 1024));
    let mut truncated = false;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|_| VistaError::Unavailable)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(retained.len());
        retained
            .write_all(&buffer[..read.min(remaining)])
            .map_err(|_| VistaError::Unavailable)?;
        truncated |= read > remaining;
    }
    Ok((retained, truncated))
}

fn kill_process_group(process_group: i32) -> bool {
    // SAFETY: negative pid addresses the process group created by pre_exec; SIGKILL makes the
    // deadline a hard bound even when the provider subprocess ignores graceful signals.
    unsafe { libc::kill(-process_group, libc::SIGKILL) == 0 }
}

fn wait_without_reaping(pid: u32) -> std::io::Result<()> {
    loop {
        // SAFETY: `info` is initialized for the kernel, and WNOWAIT keeps the child waitable so its
        // process-group identity cannot be recycled before ownership is fenced.
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
    resolve_executable_at(executable, &validation_cwd, search_path.as_deref())
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

    use super::*;

    fn write_executable(path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    fn config(executable: &Path, cwd: &Path, deadline: Duration) -> VistaConfig {
        VistaConfig::resolve(executable, cwd.to_path_buf(), deadline).unwrap()
    }

    fn request(slug: &str, version: u64) -> ArtifactRequest {
        ArtifactRequest {
            slug: slug.into(),
            version,
        }
    }

    #[test]
    fn fixed_command_uses_exact_read_only_argv() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("vista");
        write_executable(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' \"$#|$1|$2|$3|$4|$5|$6\"\n",
        );
        let mut invocation = VistaInvocation {
            config: config(&executable, temporary.path(), Duration::from_secs(1)),
            control: Arc::new(ProcessControl::detached()),
        };
        let outcome = invocation.get(request("release-notes", 7)).unwrap();
        assert_eq!(
            outcome.stdout,
            b"6|artifact|get|release-notes|v7|--output|json\n"
        );
    }

    #[test]
    fn invalid_identity_is_denied_before_spawn() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("vista");
        let marker = temporary.path().join("spawned");
        write_executable(&executable, "#!/bin/sh\ntouch \"$PWD/spawned\"\n");
        let mut invocation = VistaInvocation {
            config: config(&executable, temporary.path(), Duration::from_secs(1)),
            control: Arc::new(ProcessControl::detached()),
        };
        for denied in [
            request("", 1),
            request("-leading", 1),
            request("trailing-", 1),
            request("two--dashes", 1),
            request("Upper", 1),
            request("valid", 0),
            request("valid", MAX_VERSION + 1),
        ] {
            assert!(matches!(invocation.get(denied), Err(VistaError::Denied)));
        }
        assert!(!marker.exists());
    }

    #[test]
    fn cancelled_before_spawn_is_effect_free() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("vista");
        let marker = temporary.path().join("spawned");
        write_executable(&executable, "#!/bin/sh\ntouch \"$PWD/spawned\"\n");
        let control = Arc::new(ProcessControl::detached());
        control.terminate(Termination::Cancelled);
        let mut invocation = VistaInvocation {
            config: config(&executable, temporary.path(), Duration::from_secs(1)),
            control,
        };
        assert!(matches!(
            invocation.get(request("valid", 1)),
            Err(VistaError::Cancelled)
        ));
        assert!(!marker.exists());
    }

    #[test]
    fn fixed_command_deadline_kills_and_reaps_the_process_group() {
        let temporary = tempfile::tempdir().unwrap();
        let executable = temporary.path().join("vista");
        let fifo = temporary.path().join("block");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: the pathname is a live NUL-terminated byte string owned for the call.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        write_executable(
            &executable,
            "#!/bin/sh\nexec 3< \"$PWD/block\"\nread value <&3\n",
        );
        let control = Arc::new(ProcessControl::detached());
        let mut invocation = VistaInvocation {
            config: config(&executable, temporary.path(), Duration::from_millis(100)),
            control: Arc::clone(&control),
        };
        assert!(matches!(
            invocation.get(request("valid", 1)),
            Err(VistaError::DeadlineExceeded)
        ));
        assert!(matches!(*control.child.lock(), ChildOwnership::Reaped));
    }
}
