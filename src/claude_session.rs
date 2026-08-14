//! Controlled Claude launch with a session-owned presence lease.
//!
//! Claude can close its stdio MCP child after startup. That child cannot prove that the interactive
//! provider still lives. This wrapper launches the provider and refreshes presence while that exact
//! child remains alive. It uses the provider's existing terminal process group.

use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

use crate::{message, status};

const PROVIDER_POLL: Duration = Duration::from_millis(250);
const STOP_GRACE: Duration = Duration::from_secs(5);

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_signal: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

extern "C" fn on_interrupt_signal(_signal: libc::c_int) {}

fn install_signal_handler() {
    STOP.store(false, Ordering::SeqCst);
    let handler = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    let interrupt = on_interrupt_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        // The terminal also sends SIGINT to this wrapper. Keep the wrapper alive while Claude
        // handles that interactive interrupt itself.
        libc::signal(libc::SIGINT, interrupt);
    }
}

/// Run one interactive Claude provider and maintain its presence until it exits.
pub fn run(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    claude_argv: Vec<String>,
) -> Result<()> {
    let agent_dir =
        message::resolve_agent_dir(catalog_root, &identity, &crate::run::detect_host())?
            .with_context(|| format!("Claude driver agent '{identity}' is not declared"))?;
    anyhow::ensure!(
        !claude_argv.is_empty(),
        "Claude driver '{runtime_id}' has no provider argv"
    );
    install_signal_handler();
    run_provider(
        &status::status_path(&agent_dir),
        &claude_argv,
        status::STATUS_REFRESH,
        PROVIDER_POLL,
        &STOP,
    )
    .with_context(|| format!("running Claude driver '{runtime_id}'"))
}

fn run_provider(
    status_path: &Path,
    argv: &[String],
    refresh_interval: Duration,
    poll: Duration,
    stop: &AtomicBool,
) -> Result<()> {
    let (program, args) = argv
        .split_first()
        .context("Claude provider argv is empty")?;
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            libc::signal(libc::SIGTERM, libc::SIG_DFL);
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("starting Claude provider {program}"))?;
    let mut next_refresh = Instant::now();
    loop {
        if stop.load(Ordering::SeqCst) {
            return stop_provider_group(&mut child);
        }
        if let Some(exit) = child.try_wait().context("checking Claude provider")? {
            return completed_provider(exit);
        }
        let now = Instant::now();
        if now >= next_refresh {
            let _ = status::refresh(status_path);
            next_refresh = now + refresh_interval;
        }
        thread::sleep(poll.min(next_refresh.saturating_duration_since(Instant::now())));
    }
}

fn completed_provider(exit: ExitStatus) -> Result<()> {
    anyhow::ensure!(exit.success(), "Claude provider exited with {exit}");
    Ok(())
}

fn stop_provider_group(child: &mut Child) -> Result<()> {
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
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    let _ = child.wait();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn idle_provider_refreshes_presence_without_mcp_input() {
        let tmp = tempfile::tempdir().unwrap();
        let presence = status::status_path(tmp.path());
        status::set_state(&presence, status::State::Available).unwrap();
        let before = fs::read_to_string(&presence).unwrap();
        let stop = AtomicBool::new(false);

        run_provider(
            &presence,
            &["sh".into(), "-c".into(), "sleep 0.12".into()],
            Duration::from_millis(25),
            Duration::from_millis(5),
            &stop,
        )
        .unwrap();

        let after = fs::read_to_string(&presence).unwrap();
        assert_ne!(after, before);
        assert_eq!(status::read_state(&presence), status::State::Available);
    }
}
