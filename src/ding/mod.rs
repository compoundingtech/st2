//! Native inbox-to-terminal DING delivery.
//!
//! Fresh delivery preserves one bounded production transport containing the normalized
//! bracketed-paste, a 0.5 second delay, and Return. Once that transaction has started, any command
//! or receipt ambiguity retains staged ownership. A later bare-Return retry is allowed only after
//! two adjacent adapter observations prove the exact retained composer is safe.
//!
//! Once a paste command starts, the sidecar owns that payload and retries by inspection only. It
//! never pastes the same notice again while that transport attempt remains owned.
//! PTY or Return success is not delivery: a harness adapter must positively classify the expected
//! notice text in its submitted-prompt or queued-message pattern while the live composer is empty.
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
mod harness;

use crate::message::{self, Message};
use crate::status;
use composer::{ComposerState, classify_composer, classify_receipt};
use harness::ReceiptState;

const BRACKETED_PASTE_START: &str = "\x1b[200~";
const BRACKETED_PASTE_END: &str = "\x1b[201~";
const SUBJECT_MAX_CHARS: usize = 160;
const SENDER_MAX_CHARS: usize = 80;
const RECOVERY_POKE: &str = "[DING] unread st2 messages remain; check your inbox";
// Must exceed face607's bounded 0.5s delivery delay plus PTY/Node startup overhead; otherwise a
// successful pane write is misreported as a timeout and retried, duplicating the owned payload.
const PTY_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const COMPOSER_OBSERVATION_WINDOW: Duration = Duration::from_millis(450);
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

/// Recovery transport: one bounded PTY transaction containing paste and Return.
///
/// A successful command proves only that the PTY accepted the input sequence. [`PokeOutcome::Delivered`]
/// still requires a separate harness receipt for the exact notice.
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

/// One delivery attempt either has positive harness acceptance for the exact notice, owns an
/// ambiguous or retained payload that must be retried by inspection only, or performed no input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PokeOutcome {
    Delivered,
    Staged,
    /// A maintained adapter positively proved that the exact staged notice is absent. Queue state
    /// decides whether an archive receipt makes that proof sufficient to relinquish ownership.
    NotRetained,
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

    /// Central production path shared by inbox DING and any caller that needs to record an attempt
    /// immediately before the command containing Return.
    pub fn poke_with(
        &self,
        text: &str,
        before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<PokeOutcome> {
        transport_and_observe_with_window(
            text,
            &mut || self.run(pty_delivery_args(&self.session, text), "send"),
            &mut || self.peek(),
            &mut || thread::sleep(COMPOSER_OBSERVATION_POLL),
            before_submit,
            COMPOSER_OBSERVATION_WINDOW,
        )
    }
}

impl Poker for PtyPoker {
    fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
        self.poke_with(text, &mut || Ok(()))
    }

    fn retry_staged(&self, text: &str) -> anyhow::Result<PokeOutcome> {
        retry_staged_with_window(
            text,
            &mut || self.peek(),
            &mut || self.run(pty_submit_args(&self.session), "send"),
            &mut || thread::sleep(COMPOSER_OBSERVATION_POLL),
            &mut || Ok(()),
            COMPOSER_OBSERVATION_WINDOW,
        )
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

fn transport_and_observe_with_window(
    text: &str,
    transport: &mut dyn FnMut() -> anyhow::Result<()>,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    observation_window: Duration,
) -> anyhow::Result<PokeOutcome> {
    before_submit()?;
    // Preserve the accepted transport-first transaction. Once it starts, any command or
    // observation failure is ambiguous: the paste may have landed even if Return did not.
    if let Err(error) = transport() {
        eprintln!("st2 ding: DING transport became ambiguous; retaining staged ownership: {error}");
        return Ok(PokeOutcome::Staged);
    }
    observe_receipt_with_window(text, peek, poll, observation_window)
}

/// Observe one bounded post-submit window. PTY success, disappearance, and generic screen change
/// are not receipts; only the adapter's positive accepted-pattern classification completes
/// delivery.
fn observe_receipt_with_window(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    poll: &mut dyn FnMut(),
    observation_window: Duration,
) -> anyhow::Result<PokeOutcome> {
    let deadline = Instant::now() + observation_window;
    loop {
        let screen = match peek() {
            Ok(screen) => screen,
            Err(error) => {
                eprintln!(
                    "st2 ding: post-submit receipt observation failed; retaining staged ownership: {error}"
                );
                return Ok(PokeOutcome::Staged);
            }
        };
        if classify_receipt(&screen, text) == ReceiptState::Accepted {
            return Ok(PokeOutcome::Delivered);
        }
        if Instant::now() >= deadline {
            return Ok(PokeOutcome::Staged);
        }
        poll();
    }
}

/// Inspect-only retry for a transport-owned payload. It never pastes; one bare Return is allowed
/// only after two adjacent adapter observations prove the exact retained composer is safe.
fn retry_staged_with_window(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    observation_window: Duration,
) -> anyhow::Result<PokeOutcome> {
    let screen = match peek() {
        Ok(screen) => screen,
        Err(error) => {
            eprintln!("st2 ding: staged retry observation failed; retaining ownership: {error}");
            return Ok(PokeOutcome::Staged);
        }
    };
    match classify_receipt(&screen, text) {
        ReceiptState::Accepted => Ok(PokeOutcome::Delivered),
        ReceiptState::RetainedSafe => submit_retained_after_final_observation(
            text,
            peek,
            submit,
            poll,
            before_submit,
            observation_window,
        ),
        ReceiptState::NotRetained => Ok(PokeOutcome::NotRetained),
        ReceiptState::RetainedBlocked | ReceiptState::Unproven => Ok(PokeOutcome::Staged),
    }
}

fn submit_retained_after_final_observation(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    observation_window: Duration,
) -> anyhow::Result<PokeOutcome> {
    let screen = match peek() {
        Ok(screen) => screen,
        Err(error) => {
            eprintln!(
                "st2 ding: final retained-composer observation failed; retaining ownership: {error}"
            );
            return Ok(PokeOutcome::Staged);
        }
    };
    match classify_receipt(&screen, text) {
        ReceiptState::Accepted => return Ok(PokeOutcome::Delivered),
        ReceiptState::RetainedSafe => {}
        ReceiptState::NotRetained => return Ok(PokeOutcome::NotRetained),
        ReceiptState::RetainedBlocked | ReceiptState::Unproven => {
            return Ok(PokeOutcome::Staged);
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
    observe_receipt_with_window(text, peek, poll, observation_window)
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
            return submit_after_final_observation(
                text,
                peek,
                submit,
                poll,
                before_submit,
                observation_window,
            );
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
                return submit_after_final_observation(
                    text,
                    peek,
                    submit,
                    poll,
                    before_submit,
                    observation_window,
                );
            }
            ComposerState::ExactBlocked => return Ok(PokeOutcome::Staged),
            ComposerState::Changed => return Ok(PokeOutcome::Staged),
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
    poll: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    observation_window: Duration,
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
            return Ok(PokeOutcome::Staged);
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
    observe_receipt_with_window(text, peek, poll, observation_window)
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

    fn is_archived(&self) -> bool {
        match self {
            Self::Recovery { in_inbox, .. } | Self::Message { in_inbox, .. } => !*in_inbox,
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
    /// Presence heartbeat refresh cadence while the target session is alive.
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
            Ok(PokeOutcome::NotRetained) if was_staged && notice.is_archived() => {
                pending.pop_front();
            }
            Ok(PokeOutcome::NotRetained) => {
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

    fn idle_codex_screen() -> String {
        "\x1b[1m›\x1b[1C\x1b[22;2mFind and fix a bug in @filename\r\n\r\n\
         \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
            .to_string()
    }

    fn idle_codex_screen_with_home_relative_cwd() -> String {
        idle_codex_screen().replace(" · /workspace", " · ~/Code/st2")
    }

    fn staged_codex_screen(text: &str) -> String {
        let rendered = text.replace(' ', "\x1b[1C");
        format!(
            "\x1b[1m›\x1b[1C\x1b[0m{rendered}\r\n\r\n\
             \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
        )
    }

    fn human_codex_screen() -> String {
        staged_codex_screen("please keep my half-written draft")
    }

    fn accepted_codex_screen(text: &str) -> String {
        format!(
            "{}\r\n\r\n{}",
            staged_codex_screen(text),
            idle_codex_screen()
        )
    }

    fn queued_codex_screen(text: &str) -> String {
        format!(
            "Messages to be submitted after next tool call:\r\n{text}\r\n\r\n{}",
            idle_codex_screen()
        )
    }

    fn claude_rule() -> String {
        "─".repeat(80)
    }

    fn idle_claude_screen() -> String {
        let rule = claude_rule();
        format!(
            "Claude Code v2.1.220\r\n{rule}\r\n❯\u{00a0}Try \"write a test for validate.rs\"\r\n\
             {rule}\r\n  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents"
        )
    }

    fn idle_claude_screen_without_hint() -> String {
        idle_claude_screen().replace(" (shift+tab to cycle)", "")
    }

    fn staged_claude_screen(text: &str) -> String {
        assert!(text.is_ascii());
        let rule = claude_rule();
        let composer = if text.len() <= 77 {
            format!("❯\u{00a0}{text}")
        } else {
            let (first, continuation) = text.split_at(77);
            format!("❯\u{00a0}{first}\r\n  {continuation}")
        };
        format!(
            "Claude Code v2.1.220\r\n{rule}\r\n{composer}\r\n{rule}\r\n\
             ⏵⏵ bypass permissions on (shift+tab to cycle)"
        )
    }

    /// A pane that has been used: the startup banner has scrolled out of the peeked viewport, the
    /// composer is empty rather than showing a rotating placeholder, and the footer omits the
    /// conditional `(shift+tab to cycle)` hint while keeping the permission-mode indicator.
    fn mature_claude_screen(composer: &str) -> String {
        let rule = claude_rule();
        let footer = "  ⏵⏵ bypass permissions on · PR #42 · 2 shells · ← 1 agent";
        format!("  an earlier turn\r\n{rule}\r\n{composer}\r\n{rule}\r\n{footer}")
    }

    fn mature_idle_claude_screen() -> String {
        mature_claude_screen("❯\u{00a0}")
    }

    fn mature_idle_claude_screen_with_hint() -> String {
        mature_idle_claude_screen().replace(
            "⏵⏵ bypass permissions on",
            "⏵⏵ bypass permissions on (shift+tab to cycle)",
        )
    }

    /// The same used pane in accept-edits mode. `permissions on` is specific to the bypass footer,
    /// so no accept-edits or auto pane is positively idle to this classifier.
    fn mature_idle_accept_edits_claude_screen() -> String {
        mature_idle_claude_screen().replace("⏵⏵ bypass permissions on", "⏵⏵ accept edits on")
    }

    fn mature_staged_claude_screen(text: &str) -> String {
        mature_claude_screen(&format!("❯\u{00a0}{text}"))
    }

    /// The same used pane with an in-flight turn: a spinner status line above the composer. Every
    /// frame below was observed on a real 2.1.220 pane; the glyph animates and the elapsed timer is
    /// not always rendered, so both variations appear here.
    fn mid_turn_claude_screen(status: &str, composer: &str) -> String {
        let rule = claude_rule();
        let footer = "  ⏵⏵ bypass permissions on · PR #42 · 2 shells · ← 1 agent";
        format!("  an earlier turn\r\n{status}\r\n{rule}\r\n{composer}\r\n{rule}\r\n{footer}")
    }

    /// Status lines for an ACTIVE turn — Return must never be sent.
    const ACTIVE_TURN_STATUS: [&str; 4] = [
        "✻ Frolicking… (3m 35s · ↓ 6.9k tokens)",
        "✽ Schlepping…",
        "· Metamorphosing…",
        "✶ Schlepping… (9s · ↓ 296 tokens · thinking with high effort)",
    ];

    /// Status lines for a FINISHED turn — these sit above every genuinely idle composer, so
    /// treating them as blocked would stop delivery entirely.
    const FINISHED_TURN_STATUS: [&str; 4] = [
        "✻ Brewed for 5s",
        "✻ Crunched for 7s",
        "✻ Cogitated for 11s · 1 shell still running",
        "✻ Baked for 3s · 1 shell still running",
    ];

    /// A live Claude composer with a stale Codex composer above it in scrollback, preceded by
    /// escape-heavy output. The escapes inflate the Codex byte offset far past the Claude
    /// composer's row, which is what makes the two locators' units observably disagree.
    fn live_claude_below_escape_heavy_codex_transcript() -> String {
        let padding =
            "\x1b[1;32m\x1b[38;5;204mpadding with lots of escapes\x1b[0m\x1b[0m\r\n".repeat(10);
        let codex = staged_codex_screen("a stale pasted codex draft");
        let filler = "\x1b[1;32mmore padding\x1b[0m\r\n".repeat(6);
        format!(
            "{padding}{codex}\r\n{filler}{}",
            mature_idle_claude_screen()
        )
    }

    /// A Codex pane whose scrollback holds a captured Claude screen — two ruled lines around a `❯`
    /// row plus a Claude idle footer — above the live, ANSI-detected Codex composer. Capturing and
    /// pasting pane text is routine, so this shape is not exotic.
    fn codex_screen_below_claude_transcript(transcript_row: &str, codex: &str) -> String {
        let rule = claude_rule();
        format!(
            "  scrollback: a pasted Claude pane\r\n{rule}\r\n❯\u{00a0}{transcript_row}\r\n{rule}\r\n\
             \u{0020} ⏵⏵ bypass permissions on (shift+tab to cycle)\r\n\r\n{codex}"
        )
    }

    /// An in-flight Claude turn ships no interrupt hint, so the composer being empty is not proof
    /// that Return is safe — the pane is working. The finished-turn line looks almost identical and
    /// sits above every genuinely idle composer, so both directions are pinned here.
    #[test]
    fn an_in_flight_turn_blocks_return_but_a_finished_one_does_not() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";

        for status in ACTIVE_TURN_STATUS {
            // Empty composer mid-turn: positively empty, but not safe.
            assert_ne!(
                classify_composer(&mid_turn_claude_screen(status, "❯\u{00a0}"), expected),
                ComposerState::EmptySafe,
                "active turn must not be EmptySafe: {status}"
            );
            // The notice already staged mid-turn: exact, but Return is still withheld.
            assert_eq!(
                classify_composer(
                    &mid_turn_claude_screen(status, &format!("❯\u{00a0}{expected}")),
                    expected
                ),
                ComposerState::ExactBlocked,
                "active turn must be ExactBlocked: {status}"
            );
        }

        // A finished turn is the normal idle screen. Blocking on it would stop delivery forever.
        for status in FINISHED_TURN_STATUS {
            assert_eq!(
                classify_composer(&mid_turn_claude_screen(status, "❯\u{00a0}"), expected),
                ComposerState::EmptySafe,
                "finished turn must stay deliverable: {status}"
            );
            assert_eq!(
                classify_composer(
                    &mid_turn_claude_screen(status, &format!("❯\u{00a0}{expected}")),
                    expected
                ),
                ComposerState::ExactSafe,
                "finished turn must stay submittable: {status}"
            );
        }
    }

    /// The two locators do not natively work in the same units. Codex is matched with `rfind` over
    /// the raw screen, so it reports a **byte offset** inflated by every escape sequence above it;
    /// Claude is matched over stripped lines, so it reports a **row**. Comparing those directly
    /// picks Codex almost always, since an offset dwarfs a row — including when the live composer
    /// is Claude's and the Codex match is stale scrollback. Both must be normalized to a row.
    ///
    /// On the screen below, measured: the Codex composer sits at byte offset 560 but row 10, while
    /// the live Claude composer is row 20. Comparing row against offset picks Codex, so this would
    /// classify from a pasted draft instead of the real composer.
    #[test]
    fn composer_positions_are_compared_as_rows_not_raw_byte_offsets() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";
        assert_eq!(
            classify_composer(&live_claude_below_escape_heavy_codex_transcript(), expected),
            ComposerState::EmptySafe
        );
    }

    /// Normalizing the Codex offset means counting newlines in the *stripped* prefix, which is only
    /// faithful if stripping preserves them. It does for well-formed input. It does not for an
    /// unterminated sequence: the CSI scanner runs until a byte in `0x40..=0x7e` and `\n` is `0x0a`,
    /// so it eats newlines, and an unterminated OSC consumes to the end of input. Both are recorded
    /// here so a future change to `strip_ansi` cannot silently shift every row.
    #[test]
    fn stripping_preserves_newlines_for_well_formed_sequences_only() {
        let nl = |text: &str| composer::strip_ansi(text).matches('\n').count();

        assert_eq!(
            nl("\x1b[1;32mone\x1b[0m\r\n\x1b[2Ctwo\x1b[0m\r\n\x1b[1mthree\x1b[0m\r\n"),
            3
        );
        // Unterminated CSI: the newline is consumed while hunting for a final byte.
        assert_eq!(nl("before\r\n\x1b[999999\r\nafter\r\n"), 2);
        // Unterminated OSC: everything to the end of input is consumed.
        assert_eq!(nl("before\r\n\x1b]0;no terminator\r\nafter\r\n"), 1);
    }

    /// Scrollback that merely looks like a composer must never outrank the live one. The paste and
    /// the Return always go to the pane's real bottom composer, so misreading transcript text as
    /// "idle" or "already staged" is a wrong positive: it can type into, or submit, a human draft.
    #[test]
    fn transcript_composers_never_outrank_the_live_bottom_composer() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";

        // The live Codex composer holds a human draft in both cases, so both must stay `Changed`.
        // An empty transcript row would otherwise read as positively-empty and allow the paste.
        assert_eq!(
            classify_composer(
                &codex_screen_below_claude_transcript("", &human_codex_screen()),
                expected
            ),
            ComposerState::Changed
        );
        // A transcript row holding the exact notice is the worse case: it would otherwise satisfy
        // the two adjacent exact observations and send a bare Return to the draft.
        assert_eq!(
            classify_composer(
                &codex_screen_below_claude_transcript(expected, &human_codex_screen()),
                expected
            ),
            ComposerState::Changed
        );

        // The rule is positional, not a Codex preference: a genuine Claude pane whose scrollback
        // shows a captured Codex composer still classifies from its own live Claude composer.
        assert_eq!(
            classify_composer(
                &format!(
                    "{}\r\n{}",
                    staged_codex_screen("a stale pasted codex draft"),
                    mature_idle_claude_screen()
                ),
                expected
            ),
            ComposerState::EmptySafe
        );
    }

    #[test]
    fn maintained_composer_classifiers_require_exact_idle_state() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";
        assert_eq!(
            classify_composer(&idle_codex_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&idle_codex_screen_with_home_relative_cwd(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&staged_codex_screen(expected), expected),
            ComposerState::ExactSafe
        );
        assert_eq!(
            classify_composer(&human_codex_screen(), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!("Create a plan?\r\n{}", staged_codex_screen(expected)),
                expected
            ),
            ComposerState::ExactBlocked
        );

        assert_eq!(
            classify_composer(&idle_claude_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&idle_claude_screen_without_hint(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&staged_claude_screen(expected), expected),
            ComposerState::ExactSafe
        );
        assert_eq!(
            classify_composer(&staged_claude_screen("a changed human composer"), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!("Esc to interrupt\r\n{}", staged_claude_screen(expected)),
                expected
            ),
            ComposerState::ExactBlocked
        );
        // A used pane must classify exactly like a fresh one: neither the scrolled-away banner, the
        // empty composer, nor the missing cycle hint is evidence that Return is unsafe.
        assert_eq!(
            classify_composer(&mature_idle_claude_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&mature_idle_claude_screen_with_hint(), expected),
            ComposerState::EmptySafe
        );
        // Only the bypass footer carries `permissions on`, so an otherwise identical accept-edits
        // pane is never positively idle. It stays unsubmitted rather than being proven safe.
        assert_eq!(
            classify_composer(&mature_idle_accept_edits_claude_screen(), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(&mature_staged_claude_screen(expected), expected),
            ComposerState::ExactSafe
        );
        // The same pane shape must still fail closed on a human draft and on an active turn.
        assert_eq!(
            classify_composer(
                &mature_staged_claude_screen("a changed human composer"),
                expected
            ),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!(
                    "Esc to interrupt\r\n{}",
                    mature_staged_claude_screen(expected)
                ),
                expected
            ),
            ComposerState::ExactBlocked
        );

        assert_eq!(
            classify_composer("unknown terminal pixels", expected),
            ComposerState::Ambiguous
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
            accepted_codex_screen(text),
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
            ["peek", "paste", "peek", "peek", "receipt", "return", "peek"]
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
        assert_eq!(outcome, PokeOutcome::Staged);
        assert_eq!(*actions.borrow(), ["paste"]);

        let retry_screens = RefCell::new(VecDeque::from([
            staged_claude_screen(text),
            staged_claude_screen(text),
            format!("❯\u{00a0}{text}\r\n{}", idle_claude_screen()),
        ]));
        let retry_actions = RefCell::new(Vec::new());
        assert_eq!(
            retry_staged_with_window(
                text,
                &mut || Ok(retry_screens.borrow_mut().pop_front().unwrap()),
                &mut || {
                    retry_actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap(),
            PokeOutcome::Delivered
        );
        assert_eq!(*retry_actions.borrow(), ["return"]);
    }

    #[test]
    fn successful_transport_with_retained_or_unproven_pixels_is_not_delivered() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] receipt truth (from cos); check your inbox";
        for screen in [
            staged_codex_screen(text),
            idle_codex_screen(),
            human_codex_screen(),
            "unknown renderer".to_string(),
        ] {
            let actions = RefCell::new(Vec::new());
            let outcome = transport_and_observe_with_window(
                text,
                &mut || {
                    actions.borrow_mut().push("transport");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("peek");
                    Ok(screen.clone())
                },
                &mut || actions.borrow_mut().push("poll"),
                &mut || {
                    actions.borrow_mut().push("before-submit");
                    Ok(())
                },
                Duration::ZERO,
            )
            .unwrap();
            assert_eq!(outcome, PokeOutcome::Staged);
            assert_eq!(
                *actions.borrow(),
                ["before-submit", "transport", "peek"],
                "transport success alone must never become Delivered"
            );
        }
    }

    #[test]
    fn ambiguous_transport_receipt_and_retry_errors_retain_staged_ownership() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] error truth (from cos); check your inbox";

        let actions = RefCell::new(Vec::new());
        assert_eq!(
            transport_and_observe_with_window(
                text,
                &mut || {
                    actions.borrow_mut().push("transport");
                    anyhow::bail!("ambiguous transport")
                },
                &mut || {
                    actions.borrow_mut().push("peek");
                    Ok(idle_codex_screen())
                },
                &mut || {},
                &mut || {
                    actions.borrow_mut().push("before-submit");
                    Ok(())
                },
                Duration::ZERO,
            )
            .unwrap(),
            PokeOutcome::Staged
        );
        assert_eq!(*actions.borrow(), ["before-submit", "transport"]);

        assert_eq!(
            transport_and_observe_with_window(
                text,
                &mut || Ok(()),
                &mut || anyhow::bail!("unreadable receipt"),
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap(),
            PokeOutcome::Staged
        );

        let screens = RefCell::new(VecDeque::from([
            staged_codex_screen(text),
            staged_codex_screen(text),
        ]));
        let submits = RefCell::new(0);
        assert_eq!(
            retry_staged_with_window(
                text,
                &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
                &mut || {
                    *submits.borrow_mut() += 1;
                    anyhow::bail!("ambiguous Return")
                },
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap(),
            PokeOutcome::Staged
        );
        assert_eq!(*submits.borrow(), 1);
    }

    #[test]
    fn adapter_recognized_notice_with_an_empty_live_composer_is_a_positive_receipt() {
        let text = "[DING] new st2 message: [id:abc123] receipt truth (from cos); check your inbox";
        assert_eq!(
            classify_receipt(&queued_codex_screen(text), text),
            ReceiptState::Accepted
        );
        assert_eq!(
            classify_receipt(&accepted_codex_screen(text), text),
            ReceiptState::Accepted
        );
        assert_eq!(
            classify_receipt(&staged_codex_screen(text), text),
            ReceiptState::RetainedSafe
        );
        assert_eq!(
            classify_receipt(
                &format!("ordinary transcript: {text}\r\n{}", idle_codex_screen()),
                text
            ),
            ReceiptState::NotRetained,
            "notice text outside an adapter-recognized accepted pattern is not a receipt"
        );
        assert_eq!(
            classify_receipt(
                &format!("old receipt: {text}\r\n{}", human_codex_screen()),
                text
            ),
            ReceiptState::NotRetained,
            "a parsed changed live composer positively excludes the exact owned notice"
        );
        assert_eq!(
            classify_receipt(&idle_codex_screen(), text),
            ReceiptState::NotRetained
        );
        assert_eq!(
            classify_receipt("unknown renderer", text),
            ReceiptState::Unproven,
            "an unrecognized screen cannot prove that the owned notice disappeared"
        );

        assert_eq!(
            classify_receipt(
                &format!("❯\u{00a0}{text}\r\n{}", idle_claude_screen()),
                text
            ),
            ReceiptState::Accepted
        );
        assert_eq!(
            classify_receipt(&staged_claude_screen(text), text),
            ReceiptState::RetainedSafe
        );
        assert_eq!(
            classify_receipt(
                &format!("ordinary transcript: {text}\r\n{}", idle_claude_screen()),
                text
            ),
            ReceiptState::NotRetained
        );
        assert_eq!(
            classify_receipt(&idle_claude_screen(), text),
            ReceiptState::NotRetained
        );
    }

    #[test]
    fn unsupported_composer_wraps_are_unproven_receipts() {
        let text = "[DING] new st2 message: [id:abc123] receipt truth (from cos); check your inbox";
        let (first, continuation) = text.split_at(32);
        let codex = format!(
            "\x1b[1m›\x1b[1C\x1b[0m{first}\r\n  {continuation}\r\n\r\n\
             \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
        );
        let rule = claude_rule();
        let claude = format!(
            "Claude Code v2.1.220\r\n{rule}\r\n❯\u{00a0}{first}\r\n  {continuation}\r\n\
             {rule}\r\n⏵⏵ bypass permissions on (shift+tab to cycle)"
        );

        assert_eq!(
            (
                classify_receipt(&codex, text),
                classify_receipt(&claude, text),
            ),
            (ReceiptState::Unproven, ReceiptState::Unproven),
            "unsupported wraps cannot prove that the notice disappeared"
        );
        assert_eq!(
            classify_receipt(&human_codex_screen(), text),
            ReceiptState::NotRetained
        );
        assert_eq!(
            classify_receipt(&staged_claude_screen("a changed human composer"), text),
            ReceiptState::NotRetained
        );
    }

    #[test]
    fn staged_retry_submits_only_retained_safe_and_requires_a_receipt() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] retry truth (from cos); check your inbox";

        let retained = RefCell::new(VecDeque::from([
            staged_codex_screen(text),
            staged_codex_screen(text),
            staged_codex_screen(text),
        ]));
        let submits = RefCell::new(0);
        let outcome = retry_staged_with_window(
            text,
            &mut || Ok(retained.borrow_mut().pop_front().unwrap()),
            &mut || {
                *submits.borrow_mut() += 1;
                Ok(())
            },
            &mut || {},
            &mut || Ok(()),
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Staged);
        assert_eq!(*submits.borrow(), 1);

        for (screen, expected) in [
            (
                format!("Create a plan?\r\n{}", staged_codex_screen(text)),
                PokeOutcome::Staged,
            ),
            (idle_codex_screen(), PokeOutcome::NotRetained),
            (human_codex_screen(), PokeOutcome::NotRetained),
            ("unknown renderer".to_string(), PokeOutcome::Staged),
        ] {
            let submits = RefCell::new(0);
            let outcome = retry_staged_with_window(
                text,
                &mut || Ok(screen.clone()),
                &mut || {
                    *submits.borrow_mut() += 1;
                    Ok(())
                },
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap();
            assert_eq!(outcome, expected);
            assert_eq!(
                *submits.borrow(),
                0,
                "blocked or unproven receipts must not receive Return"
            );
        }

        let accepted = queued_codex_screen(text);
        let submits = RefCell::new(0);
        assert_eq!(
            retry_staged_with_window(
                text,
                &mut || Ok(accepted.clone()),
                &mut || {
                    *submits.borrow_mut() += 1;
                    Ok(())
                },
                &mut || {},
                &mut || Ok(()),
                Duration::ZERO,
            )
            .unwrap(),
            PokeOutcome::Delivered
        );
        assert_eq!(*submits.borrow(), 0);
    }

    #[test]
    fn staged_retry_keeps_unproven_and_retained_blocked_owned() {
        use std::cell::RefCell;

        let text = "[DING] new st2 message: [id:abc123] retry truth (from cos); check your inbox";
        for screen in [
            "unknown renderer".to_string(),
            format!("Create a plan?\r\n{}", staged_codex_screen(text)),
        ] {
            let submits = RefCell::new(0);
            assert_eq!(
                retry_staged_with_window(
                    text,
                    &mut || Ok(screen.clone()),
                    &mut || {
                        *submits.borrow_mut() += 1;
                        Ok(())
                    },
                    &mut || {},
                    &mut || Ok(()),
                    Duration::ZERO,
                )
                .unwrap(),
                PokeOutcome::Staged
            );
            assert_eq!(*submits.borrow(), 0);
        }
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
    fn archived_not_retained_releases_fifo_without_repasting_owned_notice() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let archive = archive_dir(agent.path());
        let first = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "one").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let second = send_to_inbox(&inbox, "bob", Some("second"), None, &[], "two").unwrap();
        let mut pending: VecDeque<PendingNotice> = message::list_inbox(&inbox)
            .unwrap()
            .into_iter()
            .map(PendingNotice::message)
            .collect();
        let first_text = pending[0].text();
        let second_text = pending[1].text();
        let poker = OwnershipPoker {
            pokes: Mutex::new(Vec::new()),
            retries: Mutex::new(Vec::new()),
            poke_outcomes: Mutex::new(VecDeque::from([
                PokeOutcome::Staged,
                PokeOutcome::Delivered,
            ])),
            retry_outcomes: Mutex::new(VecDeque::from([PokeOutcome::NotRetained])),
        };

        flush_pending(None, &mut pending, &poker);
        archive_msg(&inbox, &archive, &first).unwrap();
        prune_archived_pending(&inbox, &mut pending);
        flush_pending(None, &mut pending, &poker);

        assert!(pending.is_empty());
        assert_eq!(
            poker.pokes.lock().unwrap().as_slice(),
            [first_text.as_str(), second_text.as_str()]
        );
        assert_eq!(poker.retries.lock().unwrap().as_slice(), [first_text]);
        assert!(!inbox.join(first).exists());
        assert!(inbox.join(second).exists());
    }

    #[test]
    fn unread_not_retained_keeps_fifo_ownership_without_repasting() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        send_to_inbox(&inbox, "alice", Some("first"), None, &[], "one").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        send_to_inbox(&inbox, "bob", Some("second"), None, &[], "two").unwrap();
        let mut pending: VecDeque<PendingNotice> = message::list_inbox(&inbox)
            .unwrap()
            .into_iter()
            .map(PendingNotice::message)
            .collect();
        let first_text = pending[0].text();
        let poker = OwnershipPoker {
            pokes: Mutex::new(Vec::new()),
            retries: Mutex::new(Vec::new()),
            poke_outcomes: Mutex::new(VecDeque::from([
                PokeOutcome::Staged,
                PokeOutcome::Delivered,
            ])),
            retry_outcomes: Mutex::new(VecDeque::from([PokeOutcome::NotRetained])),
        };

        flush_pending(None, &mut pending, &poker);
        flush_pending(None, &mut pending, &poker);

        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].staged_text(), Some(first_text.as_str()));
        assert_eq!(
            poker.pokes.lock().unwrap().as_slice(),
            [first_text.as_str()]
        );
        assert_eq!(poker.retries.lock().unwrap().as_slice(), [first_text]);
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

        let stale_ms = crate::message::now_ms()
            - u64::try_from((status::STATUS_STALE + Duration::from_secs(1)).as_millis()).unwrap();
        std::fs::write(&status_path, format!("dnd\nv1 {stale_ms}\n")).unwrap();
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
