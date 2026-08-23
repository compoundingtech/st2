//! The launch-and-hold-presence body shared by every interactive provider wrapper.
//!
//! An interactive harness cannot be trusted to keep its own control channel alive for the whole
//! session: Claude may close its stdio MCP child after startup, and a pi extension is only as
//! durable as the pi process that loaded it. The wrapper that owns the PTY launch is the one
//! process whose lifetime is exactly the session's, so presence is refreshed from here while that
//! exact child remains alive. Each harness module keeps only what is genuinely harness-specific:
//! how its provider argv is assembled and what environment the harness needs to reach st2 back.

use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

use crate::{harness_state, status};

pub(crate) const PROVIDER_POLL: Duration = Duration::from_millis(250);
const STOP_GRACE: Duration = Duration::from_secs(5);

pub(crate) static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_signal: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

extern "C" fn on_interrupt_signal(_signal: libc::c_int) {}

pub(crate) fn install_signal_handler() {
    STOP.store(false, Ordering::SeqCst);
    let handler = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    let interrupt = on_interrupt_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        // The terminal also sends SIGINT to this wrapper. Keep the wrapper alive while the
        // provider handles that interactive interrupt itself.
        libc::signal(libc::SIGINT, interrupt);
    }
}

/// How one provider session ended, as the wrapper saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderOutcome {
    /// The child exited on its own, with this status.
    Exited(ExitStatus),
    /// The wrapper stopped its process group; the reaped status when the group yielded inside the
    /// grace window. `None` means the SIGKILL escalation ran — and since that kill targets the
    /// wrapper's own group, code after it usually never runs at all.
    Stopped(Option<ExitStatus>),
}

/// The observed-harness-state handle a wrapper threads through its poll loop. Every operation
/// constructs a fresh writer over the on-disk record, so the wrapper re-stamps or terminates
/// whatever state a hook process wrote in between and never clobbers a fresher observation.
pub(crate) struct SessionObserver {
    agent_dir: PathBuf,
    identity: String,
    harness: &'static str,
    pty_session: String,
    session_start_ms: u64,
}

impl SessionObserver {
    /// `pty_session` is the wrapper's runtime/task ID — the registry entry whose liveness vouches
    /// for the record. The session start is pinned once here so every per-operation fresh writer
    /// agrees where this session began: a predecessor session's record stays heartbeat-ineligible
    /// until something of this session is observed.
    pub(crate) fn new(
        agent_dir: &Path,
        identity: &str,
        harness: &'static str,
        pty_session: &str,
    ) -> Self {
        Self {
            agent_dir: agent_dir.to_path_buf(),
            identity: identity.to_string(),
            harness,
            pty_session: pty_session.to_string(),
            session_start_ms: crate::message::now_ms(),
        }
    }

    fn writer(&self) -> harness_state::Writer {
        harness_state::Writer::new(
            &self.agent_dir,
            &self.identity,
            self.harness,
            Some(self.pty_session.clone()),
        )
        .session_started_at(self.session_start_ms)
    }

    /// Re-stamp whatever live state is on disk. The wrapper's evidence is the provider child it is
    /// polling, so this is called only while that child is alive.
    pub(crate) fn heartbeat(&self) {
        let _ = self.writer().heartbeat();
    }

    /// Best-effort terminal record; observation must never turn a clean teardown into an error.
    pub(crate) fn ended(&self, exit: &str) {
        let _ = self.writer().ended(exit);
    }
}

fn describe_exit(exit: ExitStatus) -> String {
    match (exit.code(), exit.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "exit unknown".to_string(),
    }
}

/// Run one interactive provider in this wrapper's terminal process group, refreshing presence on
/// `refresh_interval` for exactly as long as the spawned child lives. Fails on a nonzero exit;
/// wrappers that need the exit itself use [`run_provider_observed`]. With an observer, the
/// terminal record lands on every exit path this process survives.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_provider(
    provider: &str,
    status_path: &Path,
    argv: &[String],
    env: &[(String, String)],
    refresh_interval: Duration,
    poll: Duration,
    stop: &AtomicBool,
    observed: Option<&SessionObserver>,
) -> Result<()> {
    match run_provider_observed(
        provider,
        status_path,
        argv,
        env,
        refresh_interval,
        poll,
        stop,
        observed,
    )? {
        ProviderOutcome::Exited(exit) => {
            if let Some(observed) = observed {
                observed.ended(&describe_exit(exit));
            }
            completed_provider(provider, exit)
        }
        ProviderOutcome::Stopped(_) => Ok(()),
    }
}

/// [`run_provider`], but reporting how the session ended instead of judging it, so a wrapper can
/// record its own terminal observation before deciding what the exit means. The stop path still
/// writes the observer's terminal record in-line, because after SIGKILL escalation no caller code
/// is guaranteed to run.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_provider_observed(
    provider: &str,
    status_path: &Path,
    argv: &[String],
    env: &[(String, String)],
    refresh_interval: Duration,
    poll: Duration,
    stop: &AtomicBool,
    observed: Option<&SessionObserver>,
) -> Result<ProviderOutcome> {
    let (program, args) = argv
        .split_first()
        .with_context(|| format!("{provider} provider argv is empty"))?;
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (key, value) in env {
        command.env(key, value);
    }
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            libc::signal(libc::SIGTERM, libc::SIG_DFL);
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("starting {provider} provider {program}"))?;
    let mut next_refresh = Instant::now();
    loop {
        if stop.load(Ordering::SeqCst) {
            return stop_provider_group(&mut child, observed).map(ProviderOutcome::Stopped);
        }
        if let Some(exit) = child
            .try_wait()
            .with_context(|| format!("checking {provider} provider"))?
        {
            return Ok(ProviderOutcome::Exited(exit));
        }
        let now = Instant::now();
        if now >= next_refresh {
            let _ = status::refresh(status_path);
            if let Some(observed) = observed {
                observed.heartbeat();
            }
            next_refresh = now + refresh_interval;
        }
        thread::sleep(poll.min(next_refresh.saturating_duration_since(Instant::now())));
    }
}

fn completed_provider(provider: &str, exit: ExitStatus) -> Result<()> {
    anyhow::ensure!(exit.success(), "{provider} provider exited with {exit}");
    Ok(())
}

fn stop_provider_group(
    child: &mut Child,
    observed: Option<&SessionObserver>,
) -> Result<Option<ExitStatus>> {
    let process_group = unsafe { libc::getpgrp() };
    anyhow::ensure!(
        process_group > 1,
        "refusing to signal process group {process_group}"
    );
    unsafe {
        libc::kill(-process_group, libc::SIGTERM);
    }
    let deadline = Instant::now() + STOP_GRACE;
    while Instant::now() < deadline {
        if let Some(exit) = child.try_wait()? {
            if let Some(observed) = observed {
                observed.ended(&describe_exit(exit));
            }
            return Ok(Some(exit));
        }
        thread::sleep(Duration::from_millis(25));
    }
    // The escalation SIGKILLs this wrapper's own process group, so the wrapper dies with the
    // provider and nothing after the kill is guaranteed to run. The terminal record must land
    // first: a liveness record that stops being written is still being read.
    if let Some(observed) = observed {
        observed.ended("signal 9");
    }
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    Ok(child.wait().ok())
}
