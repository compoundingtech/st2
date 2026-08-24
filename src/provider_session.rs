//! The launch-and-hold-presence body shared by every interactive provider wrapper.
//!
//! An interactive harness cannot be trusted to keep its own control channel alive for the whole
//! session: Claude may close its stdio MCP child after startup, and a pi extension is only as
//! durable as the pi process that loaded it. The wrapper that owns the PTY launch is the one
//! process whose lifetime is exactly the session's, so presence is refreshed from here while that
//! exact child remains alive. Each harness module keeps only what is genuinely harness-specific:
//! how its provider argv is assembled and what environment the harness needs to reach st2 back.

use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

use crate::status;

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

/// Run one interactive provider in this wrapper's terminal process group, refreshing presence on
/// `refresh_interval` for exactly as long as the spawned child lives. Fails on a nonzero exit;
/// wrappers that need the exit itself use [`run_provider_observed`].
pub(crate) fn run_provider(
    provider: &str,
    status_path: &Path,
    argv: &[String],
    env: &[(String, String)],
    refresh_interval: Duration,
    poll: Duration,
    stop: &AtomicBool,
) -> Result<()> {
    match run_provider_observed(
        provider,
        status_path,
        argv,
        env,
        refresh_interval,
        poll,
        stop,
    )? {
        ProviderOutcome::Exited(exit) => completed_provider(provider, exit),
        ProviderOutcome::Stopped(_) => Ok(()),
    }
}

/// [`run_provider`], but reporting how the session ended instead of judging it, so a wrapper can
/// record a terminal observation before deciding what the exit means.
pub(crate) fn run_provider_observed(
    provider: &str,
    status_path: &Path,
    argv: &[String],
    env: &[(String, String)],
    refresh_interval: Duration,
    poll: Duration,
    stop: &AtomicBool,
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
            return stop_provider_group(&mut child).map(ProviderOutcome::Stopped);
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
            next_refresh = now + refresh_interval;
        }
        thread::sleep(poll.min(next_refresh.saturating_duration_since(Instant::now())));
    }
}

fn completed_provider(provider: &str, exit: ExitStatus) -> Result<()> {
    anyhow::ensure!(exit.success(), "{provider} provider exited with {exit}");
    Ok(())
}

fn stop_provider_group(child: &mut Child) -> Result<Option<ExitStatus>> {
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
            return Ok(Some(exit));
        }
        thread::sleep(Duration::from_millis(25));
    }
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    Ok(child.wait().ok())
}
