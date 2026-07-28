//! Native inbox-to-terminal DING delivery.
//!
//! Every maintained harness uses the same fail-closed transport: positively identify an empty
//! composer, bracketed-paste the normalized notice without Return, and submit only after two exact
//! safe-composer observations. A human draft, active turn, modal, changed composer, unreadable
//! screen, or bounded observation timeout never receives Return.
//!
//! Once a paste command starts, the sidecar owns that payload and retries by inspection only. It
//! never pastes the same notice again until the exact staged payload has disappeared or changed.
//! This preserves FIFO/archive behavior without letting a command timeout create duplicate text.
//! Startup can adopt an exact staged recovery or backlog notice before coalescing remaining unread
//! work into one generic recovery DING. `busy` never suppresses a notification; fresh `dnd` does.

use std::collections::{HashSet, VecDeque};
use std::io::{Read as _, Seek as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

mod composer;
#[cfg(test)]
mod fixtures;
mod harness;

use crate::message::{self, Message};
use composer::{ComposerState, classify_composer};
use crate::status;

const BRACKETED_PASTE_START: &str = "\x1b[200~";
const BRACKETED_PASTE_END: &str = "\x1b[201~";
const SUBJECT_MAX_CHARS: usize = 160;
const SENDER_MAX_CHARS: usize = 80;
const RECOVERY_POKE: &str = "[DING] unread st2 messages remain; check your inbox";
// Must exceed face607's bounded 0.5s delivery delay plus PTY/Node startup overhead; otherwise a
// successful pane write is misreported as a timeout and retried, duplicating the owned payload.
const PTY_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
#[allow(dead_code)]
const COMPOSER_OBSERVATION_WINDOW: Duration = Duration::from_millis(450);
#[allow(dead_code)]
const COMPOSER_OBSERVATION_POLL: Duration = Duration::from_millis(10);
/// A human or active turn can keep a staged notice unsafe for minutes. Retrying `pty peek` every
/// inbox poll creates a short-lived child for each attempt, so keep the correctness fallback but
/// bound that descendant churn independently of the filesystem poll cadence.
const DELIVERY_RETRY_BACKOFF: Duration = Duration::from_secs(15);

/// The `<rand6>` of a `<unix-ms>-<rand6>.md` filename — the stable id an agent dedups re-pokes on.
/// Falls back to the `.md`-stripped stem for anything off-grammar.
pub fn poke_id(filename: &str) -> &str {
    if message::is_message_filename(filename) {
        // `13 digits` + `-` = 14 bytes of prefix, then the 6 rand chars.
        &filename[14..20]
    } else {
        filename.strip_suffix(".md").unwrap_or(filename)
    }
}

/// Convert arbitrary text into one printable line.
///
/// Every control or whitespace run becomes at most one ordinary space. Removing terminal control
/// bytes before bracketed-paste framing makes it impossible for an untrusted field to inject the
/// closing marker.
fn normalize_line(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let mut pending_space = false;

    for ch in input.chars() {
        if ch.is_control() || ch.is_whitespace() {
            pending_space = !normalized.is_empty();
            continue;
        }
        if pending_space {
            normalized.push(' ');
            pending_space = false;
        }
        normalized.push(ch);
    }

    normalized
}

fn normalize_field(value: Option<&str>, fallback: &str, max_chars: usize) -> String {
    let normalized = normalize_line(value.unwrap_or_default());
    let bounded: String = normalized.chars().take(max_chars).collect();
    let bounded = bounded.trim_end();
    if bounded.is_empty() {
        fallback.to_string()
    } else {
        bounded.to_string()
    }
}

/// The `[DING] …` line an agent sees for one newly arrived message. Consumers must key on the
/// prefix and stable id rather than descriptive words. Subject and sender are bounded, normalized
/// untrusted fields; the stable id and inbox instruction are never truncated.
pub fn poke_text(msg: &Message) -> String {
    let subject = normalize_field(msg.subject.as_deref(), "(no subject)", SUBJECT_MAX_CHARS);
    let from = normalize_field(msg.from.as_deref(), "unknown", SENDER_MAX_CHARS);
    format!(
        "[DING] new st2 message: [id:{}] {subject} (from {from}); check your inbox",
        poke_id(&msg.filename)
    )
}

fn bracketed_paste(text: &str) -> String {
    let normalized = normalize_line(text);
    format!("{BRACKETED_PASTE_START}{normalized}{BRACKETED_PASTE_END}")
}

/// Bracketed-paste one normalized notice without Return.
pub fn pty_stage_args(session: &str, text: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--seq".into(),
        bracketed_paste(text),
    ]
}

/// Submit a composer that two immediately adjacent inspections proved contains the exact notice.
pub fn pty_submit_args(session: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--seq".into(),
        "key:return".into(),
    ]
}

/// Recovery delivery: one bounded PTY transaction containing paste and Return.
pub fn pty_delivery_args(session: &str, text: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--with-delay".into(),
        "0.5".into(),
        "--seq".into(),
        bracketed_paste(text),
        "--seq".into(),
        "key:return".into(),
    ]
}

/// One delivery attempt either submitted the notice, owns a paste that must be retried by
/// inspection only, or performed no input because the target was not positively safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PokeOutcome {
    Delivered,
    Staged,
    Deferred,
}

/// How DING delivers a poke and checks liveness, abstracted so the watch loop is testable without a
/// real `pty`.
pub trait Poker {
    fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome>;
    fn retry_staged(&self, _text: &str) -> anyhow::Result<PokeOutcome> {
        Ok(PokeOutcome::Deferred)
    }
    fn adopt_staged(&self, _candidates: &[String]) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    fn session_alive(&self) -> bool;
}

/// Production [`Poker`]: shells out to the sibling `pty` binary and probes its pidfile for liveness.
pub struct PtyPoker {
    bin: String,
    session: String,
}

impl PtyPoker {
    pub fn new(session: impl Into<String>) -> Self {
        Self {
            bin: "pty".to_string(),
            session: session.into(),
        }
    }

    fn run(&self, args: Vec<String>, operation: &str) -> anyhow::Result<()> {
        let out = output_with_timeout(Command::new(&self.bin).args(args), PTY_COMMAND_TIMEOUT)?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty {operation} {}` failed: {}",
                self.session,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn peek(&self) -> anyhow::Result<String> {
        let out = output_with_timeout(
            Command::new(&self.bin).args(["peek", self.session.as_str()]),
            PTY_COMMAND_TIMEOUT,
        )?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty peek {}` failed: {}",
                self.session,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        String::from_utf8(out.stdout).map_err(|error| {
            anyhow::anyhow!("`pty peek {}` returned non-UTF-8: {error}", self.session)
        })
    }

    /// Central production safety path shared by inbox DING and any caller that needs to record an
    /// attempt immediately before the only command containing Return.
    pub fn poke_with(
        &self,
        text: &str,
        before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<PokeOutcome> {
        // Recovery contract: PTY-alive delivery is transport-first. Keep one owned payload,
        // stage it once, wait a bounded interval, then submit exactly once; pane heuristics remain
        // diagnostic only and must not silently suppress messaging.
        self.run(pty_delivery_args(&self.session, text), "send")
            .map_err(|error| anyhow::anyhow!("staging DING payload: {error}"))?;
        before_submit()?;
        Ok(PokeOutcome::Delivered)
    }
}

impl Poker for PtyPoker {
    fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
        self.poke_with(text, &mut || Ok(()))
    }

    fn retry_staged(&self, text: &str) -> anyhow::Result<PokeOutcome> {
        let _ = text;
        self.run(pty_submit_args(&self.session), "send")
            .map_err(|error| anyhow::anyhow!("submitting staged DING payload: {error}"))?;
        Ok(PokeOutcome::Delivered)
    }

    fn adopt_staged(&self, candidates: &[String]) -> anyhow::Result<Option<String>> {
        let screen = self.peek()?;
        Ok(exact_staged_candidate(&screen, candidates))
    }

    fn session_alive(&self) -> bool {
        session_alive(&self.session)
    }
}

/// Run a non-interactive child with bounded output capture. Temporary files keep an escaped
/// descendant that inherited stdout/stderr from blocking cleanup after the direct child times out.
fn output_with_timeout(command: &mut Command, timeout: Duration) -> anyhow::Result<Output> {
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut child = command.spawn()?;
    let pid = child.id() as i32;
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            let _ = child.kill();
            thread::spawn(move || {
                let _ = child.wait();
            });
            anyhow::bail!("timed out after {:.1}s", timeout.as_secs_f64());
        }
        thread::sleep(Duration::from_millis(10));
    };
    stdout.rewind()?;
    stderr.rewind()?;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    stdout.read_to_end(&mut stdout_bytes)?;
    stderr.read_to_end(&mut stderr_bytes)?;
    Ok(Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    })
}

fn exact_staged_candidate(screen: &str, candidates: &[String]) -> Option<String> {
    candidates.iter().find_map(|candidate| {
        matches!(
            classify_composer(screen, candidate),
            ComposerState::ExactSafe | ComposerState::ExactBlocked
        )
        .then(|| candidate.clone())
    })
}

#[allow(dead_code)]
fn observed_poke(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    stage: &mut dyn FnMut() -> anyhow::Result<()>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    observed_poke_with_window(
        text,
        peek,
        stage,
        submit,
        poll,
        before_submit,
        COMPOSER_OBSERVATION_WINDOW,
    )
}

/// Two-phase DING delivery with injected operations for deterministic regression tests.
fn observed_poke_with_window(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    stage: &mut dyn FnMut() -> anyhow::Result<()>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    observation_window: Duration,
) -> anyhow::Result<PokeOutcome> {
    match classify_composer(&peek()?, text) {
        ComposerState::ExactSafe => {
            return submit_after_final_observation(text, peek, submit, before_submit);
        }
        ComposerState::ExactBlocked => return Ok(PokeOutcome::Staged),
        ComposerState::EmptySafe => {}
        ComposerState::Changed | ComposerState::Ambiguous => {
            return Ok(PokeOutcome::Deferred);
        }
    }

    // Once this command starts, success is ambiguous on any error or timeout: the paste may already
    // have reached the TUI. Preserve ownership and let retry_staged inspect instead of re-pasting.
    if let Err(error) = stage() {
        eprintln!("st2 ding: paste command became ambiguous; retaining staged ownership: {error}");
        return Ok(PokeOutcome::Staged);
    }

    let deadline = Instant::now() + observation_window;
    loop {
        let screen = match peek() {
            Ok(screen) => screen,
            Err(error) => {
                eprintln!(
                    "st2 ding: post-paste observation failed; retaining staged ownership: {error}"
                );
                return Ok(PokeOutcome::Staged);
            }
        };
        match classify_composer(&screen, text) {
            ComposerState::ExactSafe => {
                return submit_after_final_observation(text, peek, submit, before_submit);
            }
            ComposerState::ExactBlocked => return Ok(PokeOutcome::Staged),
            ComposerState::Changed => return Ok(PokeOutcome::Deferred),
            ComposerState::EmptySafe | ComposerState::Ambiguous => {}
        }
        if Instant::now() >= deadline {
            return Ok(PokeOutcome::Staged);
        }
        poll();
    }
}

/// The final observation is intentionally adjacent to the bare-Return operation. Any change or
/// uncertainty after the first exact observation prevents submission.
fn submit_after_final_observation(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    let screen = match peek() {
        Ok(screen) => screen,
        Err(error) => {
            eprintln!(
                "st2 ding: final composer observation failed; retaining staged ownership: {error}"
            );
            return Ok(PokeOutcome::Staged);
        }
    };
    match classify_composer(&screen, text) {
        ComposerState::ExactSafe => {}
        ComposerState::ExactBlocked | ComposerState::Ambiguous => {
            return Ok(PokeOutcome::Staged);
        }
        ComposerState::EmptySafe | ComposerState::Changed => {
            return Ok(PokeOutcome::Deferred);
        }
    }
    if let Err(error) = before_submit() {
        eprintln!("st2 ding: pre-submit receipt failed; retaining staged ownership: {error}");
        return Ok(PokeOutcome::Staged);
    }
    if let Err(error) = submit() {
        eprintln!("st2 ding: Return command became ambiguous; retaining staged ownership: {error}");
        return Ok(PokeOutcome::Staged);
    }
    Ok(PokeOutcome::Delivered)
}

/// Inspect-only retry for a payload whose paste command already started.
#[allow(dead_code)]
fn observed_retry_staged(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    let screen = match peek() {
        Ok(screen) => screen,
        Err(error) => {
            eprintln!("st2 ding: staged retry observation failed; retaining ownership: {error}");
            return Ok(PokeOutcome::Staged);
        }
    };
    match classify_composer(&screen, text) {
        ComposerState::ExactSafe => {
            submit_after_final_observation(text, peek, submit, before_submit)
        }
        ComposerState::ExactBlocked | ComposerState::Ambiguous => Ok(PokeOutcome::Staged),
        ComposerState::EmptySafe | ComposerState::Changed => Ok(PokeOutcome::Deferred),
    }
}

/// `<pty-session-dir>/<session>.pid` + `kill(pid, 0)`; any miss means gone. This mirrors the
/// session registry's own liveness probe without forking `pty`.
pub fn session_alive(session: &str) -> bool {
    let pidfile = pty_session_dir().join(format!("{session}.pid"));
    let Ok(raw) = std::fs::read_to_string(&pidfile) else {
        return false;
    };
    let Ok(pid) = raw.trim().parse::<i32>() else {
        return false;
    };
    // Signal 0 probes existence and permission without delivering a signal.
    pid > 0 && unsafe { libc::kill(pid, 0) == 0 }
}

/// The `pty` session registry dir. This must mirror the sibling tool's resolution order.
fn pty_session_dir() -> PathBuf {
    for var in ["PTY_ROOT", "PTY_SESSION_DIR"] {
        if let Ok(directory) = std::env::var(var)
            && !directory.is_empty()
        {
            return PathBuf::from(directory);
        }
    }
    home_dir().join(".local").join("state").join("pty")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// Refuse to start if the `pty` binary is unreachable.
pub fn probe_pty_on_path() -> anyhow::Result<()> {
    match output_with_timeout(Command::new("pty").arg("--help"), PTY_COMMAND_TIMEOUT) {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => anyhow::bail!("`pty --help` exited {}", out.status),
        Err(error) => anyhow::bail!("`pty` not runnable on PATH: {error}"),
    }
}

/// Logically unread messages in `inbox_dir` not in `seen`, in send order, while updating `seen` to
/// exactly the current unread set. A same-named archive receipt suppresses and cleans a restored raw
/// inbox copy. The first call returns the whole backlog; the sidecar coalesces it into one recovery
/// notice.
pub fn new_arrivals(inbox_dir: &Path, seen: &mut HashSet<String>) -> Vec<Message> {
    let messages = message::list_inbox(inbox_dir).unwrap_or_default();
    let current: HashSet<&str> = messages
        .iter()
        .map(|message| message.filename.as_str())
        .collect();
    seen.retain(|filename| current.contains(filename.as_str()));
    messages
        .into_iter()
        .filter(|message| seen.insert(message.filename.clone()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingNotice {
    Recovery {
        startup: HashSet<String>,
        in_inbox: bool,
        staged_text: Option<String>,
    },
    Message {
        message: Message,
        in_inbox: bool,
        staged_text: Option<String>,
    },
    /// Exact composer text adopted on sidecar startup. The ordinary recovery notice remains behind
    /// it so any other unread backlog is still coalesced after this owned payload resolves.
    Adopted { staged_text: Option<String> },
}

impl PendingNotice {
    fn text(&self) -> String {
        match self {
            Self::Recovery { .. } => RECOVERY_POKE.to_string(),
            Self::Message { message, .. } => poke_text(message),
            Self::Adopted {
                staged_text: Some(text),
            } => text.clone(),
            Self::Adopted { staged_text: None } => String::new(),
        }
    }

    fn staged_text(&self) -> Option<&str> {
        match self {
            Self::Recovery { staged_text, .. }
            | Self::Message { staged_text, .. }
            | Self::Adopted { staged_text } => staged_text.as_deref(),
        }
    }

    fn set_staged_text(&mut self, value: Option<String>) {
        match self {
            Self::Recovery { staged_text, .. }
            | Self::Message { staged_text, .. }
            | Self::Adopted { staged_text } => *staged_text = value,
        }
    }

    fn in_inbox(&self) -> bool {
        match self {
            Self::Recovery { in_inbox, .. } | Self::Message { in_inbox, .. } => *in_inbox,
            Self::Adopted { .. } => false,
        }
    }

    fn adopted(text: String) -> Self {
        Self::Adopted {
            staged_text: Some(text),
        }
    }

    fn message(message: Message) -> Self {
        Self::Message {
            message,
            in_inbox: true,
            staged_text: None,
        }
    }
}

/// Consecutive liveness misses tolerated after a session has first been observed alive.
const SESSION_GONE_DEBOUNCE_MISSES: u32 = 3;

#[derive(Default)]
struct SessionWatch {
    /// A startup miss is not terminal: the sidecar may register before its target session.
    seen_alive: bool,
    misses: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum WatchStep {
    Poll,
    Gone,
}

impl SessionWatch {
    fn step(&mut self, alive: bool) -> WatchStep {
        if alive {
            self.seen_alive = true;
            self.misses = 0;
            return WatchStep::Poll;
        }
        if !self.seen_alive {
            return WatchStep::Poll;
        }
        self.misses += 1;
        if self.misses >= SESSION_GONE_DEBOUNCE_MISSES {
            WatchStep::Gone
        } else {
            WatchStep::Poll
        }
    }
}

/// Tunables for the watch loop.
pub struct DingConfig {
    /// Fallback poll cadence and liveness-check cadence.
    pub poll: Duration,
    /// Presence mtime refresh cadence while the target session is alive.
    pub status_refresh: Duration,
}

impl Default for DingConfig {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(1000),
            status_refresh: status::STATUS_REFRESH,
        }
    }
}

/// Watch `inbox_dir` and notify until stopped or the target session is gone.
///
/// Existing unread contents become one generic recovery DING. New arrivals remain FIFO-queued
/// across fresh `dnd` status and transport failures; `busy` does not suppress delivery. Archive
/// receipts prune queued work before delivery.
pub fn run_ding(
    inbox_dir: &Path,
    status_path: Option<&Path>,
    poker: &dyn Poker,
    config: &DingConfig,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    // Arm the watcher before seeding. The timer remains the correctness fallback if watching fails.
    let (tx, rx) = channel::<()>();
    let watch_at = if inbox_dir.exists() {
        inbox_dir
    } else {
        inbox_dir.parent().unwrap_or(inbox_dir)
    };
    let _watcher = crate::watch::watch_recursive_mutations(watch_at, tx);

    let mut seen = HashSet::new();
    let backlog = new_arrivals(inbox_dir, &mut seen);
    let mut startup_candidates = (!backlog.is_empty()).then(|| {
        std::iter::once(RECOVERY_POKE.to_string())
            .chain(backlog.iter().map(poke_text))
            .collect::<Vec<_>>()
    });
    let mut pending = VecDeque::new();
    if !backlog.is_empty() {
        pending.push_back(PendingNotice::Recovery {
            startup: backlog
                .iter()
                .map(|message| message.filename.clone())
                .collect(),
            in_inbox: true,
            staged_text: None,
        });
    }
    eprintln!(
        "st2 ding: ready — found {} existing unread message(s){}; watching for new arrivals.",
        backlog.len(),
        if backlog.is_empty() {
            ""
        } else {
            " and queued one recovery notice"
        }
    );

    let mut watch = SessionWatch::default();
    let mut logged_waiting = false;
    let mut last_refresh: Option<Instant> = None;
    let mut next_delivery_attempt: Option<Instant> = None;

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        let alive = poker.session_alive();
        if watch.step(alive) == WatchStep::Gone {
            eprintln!("st2 ding: target pty session is gone — exiting.");
            break;
        }

        if alive {
            if let Some(path) = status_path
                && last_refresh.is_none_or(|instant| instant.elapsed() >= config.status_refresh)
            {
                let _ = status::refresh(path);
                last_refresh = Some(Instant::now());
            }

            pending.extend(
                new_arrivals(inbox_dir, &mut seen)
                    .into_iter()
                    .map(PendingNotice::message),
            );
            prune_archived_pending(inbox_dir, &mut pending);

            let delivery_due =
                next_delivery_attempt.is_none_or(|deadline| Instant::now() >= deadline);
            if delivery_due && !delivery_suppressed(status_path) {
                if let Some(candidates) = startup_candidates.as_ref() {
                    match poker.adopt_staged(candidates) {
                        Ok(Some(text)) => {
                            if text == RECOVERY_POKE {
                                if let Some(recovery) = pending
                                    .iter_mut()
                                    .find(|notice| matches!(notice, PendingNotice::Recovery { .. }))
                                {
                                    recovery.set_staged_text(Some(text));
                                }
                            } else {
                                pending.push_front(PendingNotice::adopted(text));
                            }
                            startup_candidates = None;
                        }
                        Ok(None) => startup_candidates = None,
                        Err(error) => {
                            eprintln!("st2 ding: startup staged-notice adoption failed: {error}")
                        }
                    }
                }
                flush_pending(status_path, &mut pending, poker);
                next_delivery_attempt = (startup_candidates.is_some() || !pending.is_empty())
                    .then(|| Instant::now() + DELIVERY_RETRY_BACKOFF);
            }
        } else if !watch.seen_alive && !logged_waiting {
            eprintln!(
                "st2 ding: target pty session not yet registered; waiting before enabling exit-when-gone."
            );
            logged_waiting = true;
        }

        match rx.recv_timeout(config.poll) {
            Ok(()) => drain(&rx),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                thread::sleep(config.poll);
            }
        }
    }

    Ok(())
}

fn prune_archived_pending(inbox_dir: &Path, pending: &mut VecDeque<PendingNotice>) {
    let Ok(current) = message::list_inbox(inbox_dir) else {
        return;
    };
    let filenames: HashSet<&str> = current
        .iter()
        .map(|message| message.filename.as_str())
        .collect();
    for notice in pending.iter_mut() {
        match notice {
            PendingNotice::Recovery {
                startup, in_inbox, ..
            } => {
                *in_inbox = startup
                    .iter()
                    .any(|filename| filenames.contains(filename.as_str()));
            }
            PendingNotice::Message {
                message, in_inbox, ..
            } => {
                *in_inbox = filenames.contains(message.filename.as_str());
            }
            PendingNotice::Adopted { .. } => {}
        }
    }
    // A paste that already started stays owned across an archive race. It is never pasted again;
    // the inspect-only retry either submits the exact safe payload or proves ownership disappeared.
    pending.retain(|notice| notice.staged_text().is_some() || notice.in_inbox());
}

fn delivery_suppressed(status_path: Option<&Path>) -> bool {
    status_path.is_some_and(|path| status::read_state(path) == status::State::Dnd)
}

fn flush_pending(
    status_path: Option<&Path>,
    pending: &mut VecDeque<PendingNotice>,
    poker: &dyn Poker,
) {
    if delivery_suppressed(status_path) {
        return;
    }

    while let Some(notice) = pending.front_mut() {
        let staged = notice.staged_text().map(str::to_string);
        let was_staged = staged.is_some();
        let text = staged.unwrap_or_else(|| notice.text());
        let outcome = if was_staged {
            poker.retry_staged(&text)
        } else {
            poker.poke(&text)
        };
        match outcome {
            Ok(PokeOutcome::Delivered) => {
                pending.pop_front();
            }
            Ok(PokeOutcome::Staged) => {
                notice.set_staged_text(Some(text));
                break;
            }
            Ok(PokeOutcome::Deferred) if was_staged => {
                // The exact owned payload disappeared or changed. Adopted startup text has the
                // generic recovery notice behind it, while unread ordinary work may make one later
                // fresh guarded attempt. Archived work is done.
                notice.set_staged_text(None);
                if notice.in_inbox() {
                    break;
                }
                pending.pop_front();
            }
            Ok(PokeOutcome::Deferred) => break,
            Err(error) => {
                eprintln!("st2 ding: {error}");
                break;
            }
        }
    }
}

/// Set by SIGINT/SIGTERM so `st2 ding` exits cleanly when st2 tears the sidecar down.
static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_signal: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn install_signal_handler() {
    let handler = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
}

/// Boot and run the sidecar, refreshing presence while the target pty is alive.
pub fn serve(
    inbox_dir: &Path,
    status_path: &Path,
    session: &str,
    config: &DingConfig,
) -> anyhow::Result<()> {
    probe_pty_on_path()?;
    install_signal_handler();
    run_ding(
        inbox_dir,
        Some(status_path),
        &PtyPoker::new(session),
        config,
        &STOP,
    )
}

fn drain(rx: &Receiver<()>) {
    while rx.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::message::{archive_dir, archive_msg, inbox_dir, send_to_inbox};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    fn msg(filename: &str, from: &str, subject: Option<&str>) -> Message {
        Message {
            filename: filename.to_string(),
            ts_ms: filename
                .split_once('-')
                .and_then(|(timestamp, _)| timestamp.parse().ok())
                .unwrap_or_default(),
            from: Some(from.to_string()),
            subject: subject.map(str::to_string),
            in_reply_to: None,
            tags: vec![],
            priority: None,
            body: String::new(),
        }
    }

    #[derive(Default)]
    struct RecordingPoker {
        alive: AtomicBool,
        defer: AtomicBool,
        probes: AtomicUsize,
        failures: Mutex<usize>,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingPoker {
        fn live() -> Self {
            Self {
                alive: AtomicBool::new(true),
                ..Default::default()
            }
        }
    }

    impl Poker for RecordingPoker {
        fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
            self.calls.lock().unwrap().push(text.to_string());
            if self.defer.load(Ordering::SeqCst) {
                return Ok(PokeOutcome::Deferred);
            }
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                anyhow::bail!("injected send failure");
            }
            Ok(PokeOutcome::Delivered)
        }

        fn session_alive(&self) -> bool {
            self.probes.fetch_add(1, Ordering::SeqCst);
            self.alive.load(Ordering::SeqCst)
        }
    }

    struct OwnershipPoker {
        pokes: Mutex<Vec<String>>,
        retries: Mutex<Vec<String>>,
        poke_outcomes: Mutex<VecDeque<PokeOutcome>>,
        retry_outcomes: Mutex<VecDeque<PokeOutcome>>,
    }

    impl Poker for OwnershipPoker {
        fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
            self.pokes.lock().unwrap().push(text.to_string());
            Ok(self
                .poke_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected fresh poke"))
        }

        fn retry_staged(&self, text: &str) -> anyhow::Result<PokeOutcome> {
            self.retries.lock().unwrap().push(text.to_string());
            Ok(self
                .retry_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected staged retry"))
        }

        fn session_alive(&self) -> bool {
            true
        }
    }

    #[test]
    fn poke_id_extracts_rand6() {
        assert_eq!(poke_id("1785070000000-abc123.md"), "abc123");
        assert_eq!(poke_id("notes.md"), "notes");
    }

    #[test]
    fn poke_text_normalizes_and_bounds_untrusted_fields() {
        assert_eq!(
            poke_text(&msg("1785070000000-abc123.md", "alice", Some("deploy?"))),
            "[DING] new st2 message: [id:abc123] deploy? (from alice); check your inbox"
        );
        assert_eq!(
            poke_text(&Message {
                from: None,
                subject: None,
                ..msg("1785070000000-def456.md", "", None)
            }),
            "[DING] new st2 message: [id:def456] (no subject) (from unknown); check your inbox"
        );

        let subject = format!("{}\nignored", "s".repeat(SUBJECT_MAX_CHARS + 20));
        let sender = format!("{}\tignored", "f".repeat(SENDER_MAX_CHARS + 20));
        let text = poke_text(&msg("1785070000000-ghi789.md", &sender, Some(&subject)));
        assert!(text.contains(&"s".repeat(SUBJECT_MAX_CHARS)));
        assert!(!text.contains(&"s".repeat(SUBJECT_MAX_CHARS + 1)));
        assert!(text.contains(&"f".repeat(SENDER_MAX_CHARS)));
        assert!(!text.contains(&"f".repeat(SENDER_MAX_CHARS + 1)));
        assert!(text.ends_with("; check your inbox"));
        assert!(text.contains("[id:ghi789]"));
    }

    #[test]
    fn malicious_controls_cannot_escape_the_single_paste_frame() {
        let message = msg(
            "1785070000000-k0ygwh.md",
            "attacker\x1b[201~\r\u{009b}2J",
            Some("line one\n\tline two\x1b[201~key:return"),
        );
        let text = poke_text(&message);
        assert!(!text.chars().any(char::is_control));
        assert!(!text.contains("  "));
        assert!(text.contains("[id:k0ygwh]"));
        assert!(text.ends_with("; check your inbox"));

        let direct = format!("{text}\x1b[201~\nsecond line");
        let args = pty_stage_args("seat", &direct);
        let framed = &args[3];
        assert_eq!(framed.matches(BRACKETED_PASTE_START).count(), 1);
        assert_eq!(framed.matches(BRACKETED_PASTE_END).count(), 1);
        let inner = framed
            .strip_prefix(BRACKETED_PASTE_START)
            .unwrap()
            .strip_suffix(BRACKETED_PASTE_END)
            .unwrap();
        assert!(!inner.chars().any(char::is_control));
        assert!(inner.ends_with("[201~ second line"));
    }

    #[test]
    fn pty_stage_and_submit_are_separate_exact_sequences() {
        assert_eq!(
            pty_stage_args("my-session", "hello\nworld"),
            vec![
                "send",
                "my-session",
                "--seq",
                "\x1b[200~hello world\x1b[201~",
            ]
        );
        assert_eq!(
            pty_submit_args("my-session"),
            vec!["send", "my-session", "--seq", "key:return"]
        );
        assert!(!pty_stage_args("my-session", "hello").contains(&"key:return".to_string()));
    }

    #[test]
    fn pty_delivery_uses_face607_delay_order_and_seconds() {
        assert_eq!(
            pty_delivery_args("s", "hello"),
            vec![
                "send",
                "s",
                "--with-delay",
                "0.5",
                "--seq",
                "\x1b[200~hello\x1b[201~",
                "--seq",
                "key:return"
            ]
        );
    }

    #[test]
    fn startup_adopts_only_an_exact_recovery_or_backlog_composer() {
        let recovery = RECOVERY_POKE.to_string();
        let backlog =
            "[DING] new st2 message: [id:abc123] seeded (from cos); check your inbox".to_string();
        let candidates = vec![recovery.clone(), backlog.clone()];
        assert_eq!(
            exact_staged_candidate(&staged_codex_screen(&backlog), &candidates),
            Some(backlog.clone())
        );
        assert_eq!(
            exact_staged_candidate(&staged_claude_screen(&recovery), &candidates),
            Some(recovery)
        );
        assert_eq!(
            exact_staged_candidate(&human_codex_screen(), &candidates),
            None
        );
    }

    #[test]
    fn paste_then_two_exact_observations_precede_return() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] ordered (from cos); check your inbox";
        let screens = RefCell::new(VecDeque::from([
            idle_codex_screen(),
            staged_codex_screen(text),
            staged_codex_screen(text),
        ]));
        let actions = RefCell::new(Vec::new());
        let outcome = observed_poke_with_window(
            text,
            &mut || {
                actions.borrow_mut().push("peek");
                Ok(screens.borrow_mut().pop_front().unwrap())
            },
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || actions.borrow_mut().push("poll"),
            &mut || {
                actions.borrow_mut().push("receipt");
                Ok(())
            },
            Duration::from_millis(10),
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Delivered);
        assert_eq!(
            *actions.borrow(),
            ["peek", "paste", "peek", "peek", "receipt", "return"]
        );
    }

    #[test]
    fn changed_modal_ambiguous_and_bounded_timeout_never_return() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] guarded (from cos); check your inbox";
        for screen in [
            human_codex_screen(),
            format!("Create a plan?\r\n{}", staged_codex_screen(text)),
            "unrecognized renderer".to_string(),
        ] {
            let actions = RefCell::new(Vec::new());
            let outcome = observed_poke_with_window(
                text,
                &mut || {
                    actions.borrow_mut().push("peek");
                    Ok(screen.clone())
                },
                &mut || {
                    actions.borrow_mut().push("paste");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap();
            assert_ne!(outcome, PokeOutcome::Delivered);
            assert!(!actions.borrow().contains(&"return"));
            assert!(!actions.borrow().contains(&"paste"));
        }

        let screens = RefCell::new(VecDeque::from([idle_claude_screen(), idle_claude_screen()]));
        let actions = RefCell::new(Vec::new());
        let outcome = observed_poke_with_window(
            text,
            &mut || {
                actions.borrow_mut().push("peek");
                Ok(screens.borrow_mut().pop_front().unwrap())
            },
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || Ok(()),
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Staged);
        assert_eq!(*actions.borrow(), ["peek", "paste", "peek"]);
    }

    #[test]
    fn final_observation_change_and_staged_retry_are_fail_closed() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] final race (from cos); check your inbox";
        let screens = RefCell::new(VecDeque::from([
            idle_codex_screen(),
            staged_codex_screen(text),
            human_codex_screen(),
        ]));
        let actions = RefCell::new(Vec::new());
        let outcome = observed_poke_with_window(
            text,
            &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || Ok(()),
            Duration::from_millis(10),
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Deferred);
        assert_eq!(*actions.borrow(), ["paste"]);

        let retry_screens = RefCell::new(VecDeque::from([
            staged_claude_screen(text),
            staged_claude_screen(text),
        ]));
        let retry_actions = RefCell::new(Vec::new());
        assert_eq!(
            observed_retry_staged(
                text,
                &mut || Ok(retry_screens.borrow_mut().pop_front().unwrap()),
                &mut || {
                    retry_actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || Ok(()),
            )
            .unwrap(),
            PokeOutcome::Delivered
        );
        assert_eq!(*retry_actions.borrow(), ["return"]);
    }

    #[test]
    fn pty_commands_have_a_real_outer_timeout() {
        let started = Instant::now();
        let error = output_with_timeout(
            Command::new("sh").args(["-c", "sleep 2"]),
            Duration::from_millis(30),
        )
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn session_watch_has_startup_grace_debounce_and_live_reset() {
        let mut watch = SessionWatch::default();
        for _ in 0..10 {
            assert_eq!(watch.step(false), WatchStep::Poll);
        }
        assert_eq!(watch.step(true), WatchStep::Poll);
        assert_eq!(watch.step(false), WatchStep::Poll);
        assert_eq!(watch.step(false), WatchStep::Poll);
        assert_eq!(watch.step(true), WatchStep::Poll);
        assert_eq!(watch.step(false), WatchStep::Poll);
        assert_eq!(watch.step(false), WatchStep::Poll);
        assert_eq!(watch.step(false), WatchStep::Gone);
    }

    #[test]
    fn new_arrivals_is_fifo_and_archive_receipts_prevent_reding() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let archive = archive_dir(agent.path());
        let first = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "one").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let second = send_to_inbox(&inbox, "bob", Some("second"), None, &[], "two").unwrap();

        let mut seen = HashSet::new();
        let initial = new_arrivals(&inbox, &mut seen);
        assert_eq!(
            initial
                .iter()
                .map(|message| message.filename.as_str())
                .collect::<Vec<_>>(),
            [first.as_str(), second.as_str()]
        );
        assert!(new_arrivals(&inbox, &mut seen).is_empty());

        archive_msg(&inbox, &archive, &first).unwrap();
        std::fs::copy(archive.join(&first), inbox.join(&first)).unwrap();
        assert!(
            new_arrivals(&inbox, &mut seen).is_empty(),
            "an archive receipt must suppress a restored inbox copy"
        );

        std::thread::sleep(Duration::from_millis(2));
        let third = send_to_inbox(&inbox, "carol", Some("third"), None, &[], "three").unwrap();
        assert_eq!(
            new_arrivals(&inbox, &mut seen)
                .into_iter()
                .map(|message| message.filename)
                .collect::<Vec<_>>(),
            [third]
        );
    }

    #[test]
    fn staged_ownership_survives_archive_and_never_repastes() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let archive = archive_dir(agent.path());
        let filename = send_to_inbox(&inbox, "cos", Some("owned"), None, &[], "body").unwrap();
        let message = message::list_inbox(&inbox).unwrap().pop().unwrap();
        let expected = poke_text(&message);
        let mut pending = VecDeque::from([PendingNotice::message(message)]);
        let poker = OwnershipPoker {
            pokes: Mutex::new(Vec::new()),
            retries: Mutex::new(Vec::new()),
            poke_outcomes: Mutex::new(VecDeque::from([PokeOutcome::Staged])),
            retry_outcomes: Mutex::new(VecDeque::from([
                PokeOutcome::Staged,
                PokeOutcome::Delivered,
            ])),
        };

        flush_pending(None, &mut pending, &poker);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].staged_text(), Some(expected.as_str()));

        archive_msg(&inbox, &archive, &filename).unwrap();
        prune_archived_pending(&inbox, &mut pending);
        assert_eq!(
            pending.len(),
            1,
            "an already-started paste remains inspection-owned across archive"
        );

        flush_pending(None, &mut pending, &poker);
        assert_eq!(pending.len(), 1);
        flush_pending(None, &mut pending, &poker);
        assert!(pending.is_empty());
        assert_eq!(poker.pokes.lock().unwrap().as_slice(), [expected.as_str()]);
        assert_eq!(
            poker.retries.lock().unwrap().as_slice(),
            [expected.as_str(), expected.as_str()]
        );
    }

    #[test]
    fn pending_delivery_ignores_busy_but_respects_fresh_dnd_archive_and_retry() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let archive = archive_dir(agent.path());
        let status_path = status::status_path(agent.path());
        let first = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "one").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let second = send_to_inbox(&inbox, "bob", Some("second"), None, &[], "two").unwrap();
        let mut pending: VecDeque<PendingNotice> = message::list_inbox(&inbox)
            .unwrap()
            .into_iter()
            .map(PendingNotice::message)
            .collect();
        let poker = RecordingPoker::live();

        archive_msg(&inbox, &archive, &first).unwrap();
        prune_archived_pending(&inbox, &mut pending);
        assert_eq!(
            pending
                .iter()
                .filter_map(|notice| match notice {
                    PendingNotice::Message { message, .. } => Some(message.filename.as_str()),
                    PendingNotice::Recovery { .. } | PendingNotice::Adopted { .. } => None,
                })
                .collect::<Vec<_>>(),
            [second.as_str()]
        );

        *poker.failures.lock().unwrap() = 1;
        status::set_state(&status_path, status::State::Busy).unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert_eq!(pending.len(), 1, "a failed head remains queued");
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert!(pending.is_empty());

        let third = send_to_inbox(&inbox, "carol", Some("third"), None, &[], "three").unwrap();
        pending.extend(
            message::list_inbox(&inbox)
                .unwrap()
                .into_iter()
                .filter(|message| message.filename == third)
                .map(PendingNotice::message),
        );
        status::set_state(&status_path, status::State::Dnd).unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert_eq!(pending.len(), 1, "fresh dnd suppresses delivery");

        let stale = std::time::SystemTime::now() - status::STATUS_STALE - Duration::from_secs(1);
        std::fs::File::open(&status_path)
            .unwrap()
            .set_modified(stale)
            .unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert!(
            pending.is_empty(),
            "stale dnd reads unknown and no longer suppresses"
        );

        let calls = poker.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], calls[1], "the failed FIFO head retries first");
        assert!(calls[0].contains("second"));
        assert!(calls[2].contains("third"));
    }

    #[test]
    fn startup_recovery_notice_retries_in_memory() {
        let poker = RecordingPoker::live();
        *poker.failures.lock().unwrap() = 1;
        let mut pending = VecDeque::from([PendingNotice::Recovery {
            startup: HashSet::from(["1785070000000-abc123.md".to_string()]),
            in_inbox: true,
            staged_text: None,
        }]);

        flush_pending(None, &mut pending, &poker);
        assert_eq!(pending.len(), 1);
        flush_pending(None, &mut pending, &poker);
        assert!(pending.is_empty());

        let calls = poker.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1], "the failed FIFO head retries first");
        assert_eq!(calls[0], RECOVERY_POKE);
    }

    #[test]
    fn startup_backlog_gets_one_generic_recovery_then_new_arrivals_poke() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let status_path = status::status_path(agent.path());
        status::set_state(&status_path, status::State::Available).unwrap();
        send_to_inbox(&inbox, "old", Some("seeded"), None, &[], "old").unwrap();

        let poker = RecordingPoker::live();
        let stop = AtomicBool::new(false);
        let config = DingConfig {
            poll: Duration::from_millis(5),
            status_refresh: Duration::from_secs(60),
        };

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let deadline = Instant::now() + Duration::from_secs(3);
                while poker.probes.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                    std::thread::yield_now();
                }
                send_to_inbox(&inbox, "new", Some("post-start"), None, &[], "new").unwrap();
                while poker.calls.lock().unwrap().len() < 2 && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(2));
                }
                stop.store(true, Ordering::SeqCst);
            });

            run_ding(&inbox, Some(&status_path), &poker, &config, &stop).unwrap();
        });

        let calls = poker.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], RECOVERY_POKE);
        assert!(calls[1].contains("post-start"));
        assert!(!calls.iter().any(|call| call.contains("seeded")));
    }

    #[test]
    fn deferred_delivery_backoff_bounds_short_lived_pty_attempts() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let status_path = status::status_path(agent.path());
        status::set_state(&status_path, status::State::Available).unwrap();
        send_to_inbox(&inbox, "alice", Some("active"), None, &[], "active").unwrap();

        let poker = RecordingPoker::live();
        poker.defer.store(true, Ordering::SeqCst);
        let stop = AtomicBool::new(false);
        let config = DingConfig {
            poll: Duration::from_millis(20),
            status_refresh: Duration::from_secs(60),
        };

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(250));
                stop.store(true, Ordering::SeqCst);
            });
            run_ding(&inbox, Some(&status_path), &poker, &config, &stop).unwrap();
        });

        assert_eq!(
            poker.calls.lock().unwrap().len(),
            1,
            "an unsafe composer must not respawn pty peek/send children every inbox poll"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn idle_ding_does_not_spin_on_its_own_inbox_reads() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        std::fs::create_dir_all(&inbox).unwrap();
        let status_path = status::status_path(agent.path());
        status::set_state(&status_path, status::State::Available).unwrap();

        let poker = RecordingPoker::live();
        let stop = AtomicBool::new(false);
        let config = DingConfig {
            poll: Duration::from_millis(20),
            status_refresh: Duration::from_secs(60),
        };
        let started = Instant::now();

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(150));
                stop.store(true, Ordering::SeqCst);
            });
            run_ding(&inbox, Some(&status_path), &poker, &config, &stop).unwrap();
        });

        assert!(started.elapsed() >= Duration::from_millis(140));
        assert!(
            poker.probes.load(Ordering::SeqCst) <= 20,
            "idle DING must sleep at its configured cadence"
        );
    }
}
