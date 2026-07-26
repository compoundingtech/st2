//! The ding sidecar (M2.2): a native watcher over an agent's `resources/inbox` that pokes the agent's
//! pty on every new message arrival.
//!
//! The notice carries a stable `[DING]` prefix and message id:
//! `[DING] new st2 message: [id:<rand6>] <subject> (from <sender>); check your inbox`.
//!
//! Delivery is deliberately safer than the old unconditional text-then-Enter sequence. For a Codex
//! pane, DING bracketed-pastes the notice without Enter, waits for the TUI to settle, peeks again,
//! and submits only when the exact notice is still in an otherwise-idle composer. One exact idle
//! `Create a plan? … esc dismiss` prompt has a bounded recovery: after a second identical
//! inspection, DING sends only Escape, re-inspects, and still sends Return only for the exact staged
//! notice. Every other modal, an active turn, a human draft, or an unreadable pane defers the message
//! in memory; `busy` and `dnd` status do likewise. The inbox file remains the durable source of truth
//! while deferred. This closes the race where Codex opened a choice modal during the 0.5-second gap
//! and an unconditional trailing Return selected the modal's default action.
//!
//! It DOES run the brief-023 presence refresh: while the agent's pty is alive it re-writes the
//! agent's own `status` file on a cadence (preserving the value) so a healthy-but-idle agent never
//! rots to `unknown` — the exact failure that read the whole fleet `unknown` for 45 min. Once the pty
//! is gone the refresh stops (and the ding exits), so a dead agent correctly ages into `unknown`.
//!
//! Startup does NOT re-poke the existing backlog (the agent's boot ritual drains that) — only messages
//! that arrive after the ding starts are poked. A periodic full-backlog re-scan (for messages that
//! landed while the ding was down) is part of that deferred set.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use crate::message::{self, Message};
use crate::status;

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

/// The `[DING] …` line an agent sees for one newly arrived message. Consumers must key on the prefix
/// and stable id rather than descriptive words. Empty/missing `subject` and `from` degrade to
/// `(no subject)` / `unknown`.
pub fn poke_text(msg: &Message) -> String {
    let subject = msg
        .subject
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("(no subject)");
    let from = msg
        .from
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown");
    format!(
        "[DING] new st2 message: [id:{}] {subject} (from {from}); check your inbox",
        poke_id(&msg.filename)
    )
}

/// The unguarded `pty send` argv for an explicitly non-Codex pane: text, then `key:return`, with a
/// `0.5s` gap.
/// Codex instead uses [`pty_stage_args`] followed by a guarded [`pty_submit_args`].
pub fn pty_send_args(session: &str, text: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--with-delay".into(),
        "0.5".into(),
        "--seq".into(),
        text.into(),
        "--seq".into(),
        "key:return".into(),
    ]
}

/// Bracketed-paste a DING notice without submitting it. Keeping Enter out of this command is the
/// safety boundary: Codex can open a modal after the text lands without that modal receiving Return.
pub fn pty_stage_args(session: &str, text: &str) -> Vec<String> {
    vec!["send".into(), session.into(), "--paste".into(), text.into()]
}

/// Submit text that a post-stage pane inspection proved is still the exact DING notice.
pub fn pty_submit_args(session: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--seq".into(),
        "key:return".into(),
    ]
}

/// Dismiss only a separately recognized Codex plan prompt. Escape and Return are intentionally
/// separate PTY operations so dismissing a modal can never also activate its selected action.
pub fn pty_dismiss_plan_modal_args(session: &str) -> Vec<String> {
    vec![
        "send".into(),
        session.into(),
        "--seq".into(),
        "key:escape".into(),
    ]
}

/// One delivery attempt either reached the target or was deliberately deferred because the target
/// pane/status was not safe. Deferred work stays in the in-memory queue and the durable inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PokeOutcome {
    Delivered,
    Deferred,
}

/// How a target terminal accepts a poke. Unknown or unrendered agent commands default to the
/// fail-closed Codex guard; only a positively identified legacy harness gets the old combined
/// text-plus-Return injection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeliveryMode {
    #[default]
    CodexGuarded,
    Legacy,
}

impl DeliveryMode {
    /// Select the legacy path only for the exact Claude command shape emitted by the renderer (or an
    /// absolute Claude binary path). A missing, wrapped, or unfamiliar command remains guarded.
    pub fn for_agent_command(command: Option<&str>) -> Self {
        if command.is_some_and(|command| command_invokes(command, "claude")) {
            Self::Legacy
        } else {
            Self::CodexGuarded
        }
    }
}

fn command_invokes(command: &str, expected: &str) -> bool {
    let command = command.trim();
    let command = command
        .strip_prefix("exec ")
        .unwrap_or(command)
        .trim_start();
    let Some(program) = command.split_ascii_whitespace().next() else {
        return false;
    };
    Path::new(program)
        .file_name()
        .is_some_and(|name| name == expected)
}

/// How the ding delivers a poke and checks liveness — abstracted so the watch loop is testable
/// without a real `pty`. Production shells out to the `pty` binary.
pub trait Poker {
    /// Try to inject and submit `text`. Unsafe pane state returns [`PokeOutcome::Deferred`] without
    /// sending Return; transport/inspection failures are errors and are retried by the watch loop.
    fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome>;
    /// On sidecar startup, recover at most one notice whose visible stable DING id names exactly one
    /// message in the seeded unread backlog. The default is inert for fake and legacy pokers.
    fn recover_seeded(&self, _backlog: &[Message]) -> anyhow::Result<PokeOutcome> {
        Ok(PokeOutcome::Deferred)
    }
    /// True while the target pty session still exists — the ding exits cleanly once it's gone.
    fn session_alive(&self) -> bool;
}

/// Production [`Poker`]: shells out to the sibling `pty` binary and probes its pidfile for liveness.
pub struct PtyPoker {
    bin: String,
    session: String,
    mode: DeliveryMode,
}

impl PtyPoker {
    /// Construct a fail-closed poker. This is the safe default for Codex and for an unknown target.
    pub fn new(session: impl Into<String>) -> Self {
        Self {
            bin: "pty".to_string(),
            session: session.into(),
            mode: DeliveryMode::CodexGuarded,
        }
    }

    /// Construct a poker for a positively identified legacy terminal harness.
    pub fn new_legacy(session: impl Into<String>) -> Self {
        Self {
            bin: "pty".to_string(),
            session: session.into(),
            mode: DeliveryMode::Legacy,
        }
    }

    fn run(&self, args: Vec<String>, operation: &str) -> anyhow::Result<()> {
        let out = Command::new(&self.bin).args(args).output()?;
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
        let out = Command::new(&self.bin)
            .args(["peek", self.session.as_str()])
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty peek {}` failed: {}",
                self.session,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        String::from_utf8(out.stdout)
            .map_err(|e| anyhow::anyhow!("`pty peek {}` returned non-UTF-8: {e}", self.session))
    }

    /// Central production safety path shared by inbox DING and scheduled shepherd prompts.
    ///
    /// `before_submit` runs after the final safe-pane inspection and immediately before the only
    /// command containing Return. Shepherd uses it to persist its attempt without consuming
    /// backoff for a neutral pane deferral; inbox DING passes a no-op.
    pub fn poke_with(
        &self,
        text: &str,
        before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<PokeOutcome> {
        if self.mode == DeliveryMode::Legacy {
            return legacy_poke(
                &mut || self.run(pty_send_args(&self.session, text), "send"),
                before_submit,
            );
        }
        guarded_poke(
            text,
            &mut || self.peek(),
            &mut || self.run(pty_stage_args(&self.session, text), "send"),
            &mut || {
                self.run(
                    pty_dismiss_plan_modal_args(&self.session),
                    "send plan-modal dismissal to",
                )
            },
            &mut || self.run(pty_submit_args(&self.session), "send"),
            &mut || thread::sleep(Duration::from_millis(500)),
            before_submit,
        )
    }
}

impl Poker for PtyPoker {
    fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
        self.poke_with(text, &mut || Ok(()))
    }

    fn recover_seeded(&self, backlog: &[Message]) -> anyhow::Result<PokeOutcome> {
        if self.mode != DeliveryMode::CodexGuarded || backlog.is_empty() {
            return Ok(PokeOutcome::Deferred);
        }
        let screen = self.peek()?;
        let Some(text) = exact_staged_backlog_notice(&screen, backlog) else {
            return Ok(PokeOutcome::Deferred);
        };
        self.poke_with(&text, &mut || Ok(()))
    }

    fn session_alive(&self) -> bool {
        session_alive(&self.session)
    }
}

/// What the rendered target pane says is safe to do with one exact notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneState {
    /// Codex is idle and its composer contains only a dim placeholder.
    Idle,
    /// The exact expected DING notice is staged in the Codex composer, with no modal/active turn.
    Staged,
    /// The exact notice is staged under the one Codex plan prompt that explicitly permits Escape.
    DismissiblePlanModal,
    /// Codex is active, modal, holding human input, or otherwise ambiguous. Never send Return.
    Blocked,
    /// No recognized Codex state was found. Guarded delivery fails closed.
    Unknown,
}

enum CodexComposer {
    Empty,
    Typed(String),
}

fn legacy_poke(
    send: &mut dyn FnMut() -> anyhow::Result<()>,
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    before_submit()?;
    send()?;
    Ok(PokeOutcome::Delivered)
}

/// Guarded two-phase delivery with injected PTY operations for deterministic regression tests.
fn guarded_poke(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    stage: &mut dyn FnMut() -> anyhow::Result<()>,
    dismiss_plan_modal: &mut dyn FnMut() -> anyhow::Result<()>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    settle: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    match classify_pane(&peek()?, text) {
        PaneState::Blocked => Ok(PokeOutcome::Deferred),
        PaneState::DismissiblePlanModal => recover_plan_modal(
            text,
            peek,
            stage,
            dismiss_plan_modal,
            submit,
            settle,
            before_submit,
        ),
        PaneState::Staged => {
            before_submit()?;
            submit()?;
            Ok(PokeOutcome::Delivered)
        }
        PaneState::Idle => {
            stage()?;
            settle();
            match classify_pane(&peek()?, text) {
                PaneState::Staged => {
                    before_submit()?;
                    submit()?;
                    Ok(PokeOutcome::Delivered)
                }
                PaneState::DismissiblePlanModal => recover_plan_modal(
                    text,
                    peek,
                    stage,
                    dismiss_plan_modal,
                    submit,
                    settle,
                    before_submit,
                ),
                // This includes the incident window: a Codex model-choice modal appeared after
                // paste but before Return. Leave the exact notice staged and retry when safe.
                PaneState::Blocked | PaneState::Idle | PaneState::Unknown => {
                    Ok(PokeOutcome::Deferred)
                }
            }
        }
        // A transiently blank or partially rendered Codex pane can contain no recognizable
        // signature. The configured Codex/unknown path still fails closed.
        PaneState::Unknown => Ok(PokeOutcome::Deferred),
    }
}

/// Recover one stable, exact Codex plan prompt without polling or ever sending Return to the modal.
///
/// The modal must survive a second inspection before Escape. After dismissal, an exact staged
/// notice can submit immediately; an empty composer can be restaged once. Any other state defers.
fn recover_plan_modal(
    text: &str,
    peek: &mut dyn FnMut() -> anyhow::Result<String>,
    stage: &mut dyn FnMut() -> anyhow::Result<()>,
    dismiss_plan_modal: &mut dyn FnMut() -> anyhow::Result<()>,
    submit: &mut dyn FnMut() -> anyhow::Result<()>,
    settle: &mut dyn FnMut(),
    before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<PokeOutcome> {
    settle();
    if classify_pane(&peek()?, text) != PaneState::DismissiblePlanModal {
        return Ok(PokeOutcome::Deferred);
    }

    dismiss_plan_modal()?;
    settle();
    match classify_pane(&peek()?, text) {
        PaneState::Staged => {
            before_submit()?;
            submit()?;
            Ok(PokeOutcome::Delivered)
        }
        PaneState::Idle => {
            stage()?;
            settle();
            if classify_pane(&peek()?, text) != PaneState::Staged {
                return Ok(PokeOutcome::Deferred);
            }
            before_submit()?;
            submit()?;
            Ok(PokeOutcome::Delivered)
        }
        PaneState::DismissiblePlanModal | PaneState::Blocked | PaneState::Unknown => {
            Ok(PokeOutcome::Deferred)
        }
    }
}

const CODEX_EMPTY_COMPOSERS: [&str; 3] = [
    "\x1b[1;22m›\x1b[1C\x1b[22;2m",
    "\x1b[1m›\x1b[1C\x1b[22;2m",
    "\x1b[1m›\x1b[22m \x1b[2m",
];
const CODEX_TYPED_COMPOSERS: [&str; 4] = [
    "\x1b[1;22m›\x1b[1C\x1b[0m",
    "\x1b[1;2m› \x1b[0m",
    "\x1b[1m›\x1b[1C\x1b[0m",
    "\x1b[1m›\x1b[22m ",
];

/// Fail-closed Codex pane classifier.
///
/// The ANSI distinction is load-bearing: Codex renders an empty placeholder dim (`22;2m`) and real
/// composer input normally (`0m`). Plain text alone cannot distinguish a human draft from the
/// rotating placeholder, and Codex keeps the empty composer visible while a turn is running.
fn classify_pane(screen: &str, expected: &str) -> PaneState {
    let plain = strip_ansi(screen);
    let known_other_modal = plain.contains("Our systems are thinking a bit more")
        || plain.contains("Retry with a faster model")
        || looks_like_choice_menu(&plain);
    let active = plain.contains("Working (")
        || plain.contains("esc to interrupt")
        || plain.contains("Messages to be submitted after next tool call")
        || plain.contains("press esc to interrupt and send");
    if active {
        return PaneState::Blocked;
    }

    let composer = located_bottom_codex_composer(screen);
    let live_plan_modal = composer
        .as_ref()
        .map(|(start, _)| looks_like_dismissible_plan_modal(&strip_ansi(&screen[*start..])))
        .unwrap_or_else(|| looks_like_dismissible_plan_modal(&plain));
    if let Some((_, CodexComposer::Typed(input))) = &composer {
        let exact_notice = collapse_whitespace(input) == collapse_whitespace(expected);
        if exact_notice && !known_other_modal && live_plan_modal {
            return PaneState::DismissiblePlanModal;
        }
    }
    if known_other_modal || live_plan_modal {
        return PaneState::Blocked;
    }

    match composer {
        Some((_, CodexComposer::Empty)) => return PaneState::Idle,
        Some((_, CodexComposer::Typed(input))) => {
            return if collapse_whitespace(&input) == collapse_whitespace(expected) {
                PaneState::Staged
            } else {
                PaneState::Blocked
            };
        }
        None => {}
    }

    // A Codex footer/prompt/modal signature that we don't understand is not permission to press
    // Return. This makes future Codex TUI states safe by default.
    if plain.contains("gpt-")
        || plain.contains("Codex")
        || plain.lines().any(|line| line.trim_start().starts_with('›'))
    {
        PaneState::Blocked
    } else {
        PaneState::Unknown
    }
}

fn looks_like_dismissible_plan_modal(plain: &str) -> bool {
    collapse_whitespace(plain).contains("Create a plan? shift+tab use Plan mode esc dismiss")
}

/// Select one seeded unread notice only when the bottom composer is a DING whose stable id names
/// exactly one backlog message. Descriptive words are deliberately non-authoritative across rolling
/// binary versions; the entire visible composer becomes the exact expected text for the subsequent
/// guarded re-inspection. Ambiguity still fails closed.
fn exact_staged_backlog_notice(screen: &str, backlog: &[Message]) -> Option<String> {
    let (_, CodexComposer::Typed(input)) = located_bottom_codex_composer(screen)? else {
        return None;
    };
    if input
        .chars()
        .any(|ch| ch.is_control() && !ch.is_whitespace())
    {
        return None;
    }
    let visible = collapse_whitespace(&input);
    let (id, visible_tail) = visible_staged_ding_parts(&visible)?;
    if !matches!(
        classify_pane(screen, &visible),
        PaneState::Staged | PaneState::DismissiblePlanModal
    ) {
        return None;
    }

    let mut matches = backlog.iter().filter(|msg| poke_id(&msg.filename) == id);
    let message = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    let marker = format!("[id:{id}]");
    let expected = poke_text(message);
    let (_, expected_tail) = expected.split_once(&marker)?;
    (collapse_whitespace(visible_tail) == collapse_whitespace(expected_tail)).then_some(visible)
}

fn visible_staged_ding_parts(visible: &str) -> Option<(&str, &str)> {
    let rest = visible.strip_prefix("[DING] ")?;
    let (description, after_marker) = rest.split_once("[id:")?;
    let (id, suffix) = after_marker.split_once(']')?;
    if description.trim().is_empty()
        || id.len() != 6
        // Match the durable reader grammar, not only the narrower alphabet used for new ids.
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
        || suffix.contains("[id:")
    {
        return None;
    }
    Some((id, suffix))
}

fn looks_like_choice_menu(plain: &str) -> bool {
    let mut first = false;
    let mut later = false;
    for line in plain.lines().map(str::trim_start) {
        first |= line.starts_with("› 1.") || line.starts_with("> 1.");
        later |= line.starts_with("2.") || line.starts_with("3.");
    }
    first && later
}

fn located_bottom_codex_composer(screen: &str) -> Option<(usize, CodexComposer)> {
    let empty = CODEX_EMPTY_COMPOSERS
        .iter()
        .filter_map(|marker| {
            screen
                .rfind(marker)
                .map(|start| (start, marker.len(), true))
        })
        .max_by_key(|(start, _, _)| *start);
    let typed = CODEX_TYPED_COMPOSERS
        .iter()
        .filter_map(|marker| {
            screen
                .rfind(marker)
                .map(|start| (start, marker.len(), false))
        })
        .max_by_key(|(start, _, _)| *start);
    // The current Linux typed prefix is also the prefix of its empty-placeholder form. When both
    // begin at the same bottom-most composer, the longer exact empty marker is authoritative.
    let (start, marker_len, empty) = match (empty, typed) {
        (Some(empty), Some(typed)) if empty.0 >= typed.0 => empty,
        (_, Some(typed)) => typed,
        (Some(empty), None) => empty,
        (None, None) => return None,
    };
    if empty {
        return Some((start, CodexComposer::Empty));
    }
    let tail = &screen[start + marker_len..];
    let input = tail
        .split_once("\r\n \x1b[")
        .or_else(|| tail.split_once("\n \x1b["))
        .or_else(|| tail.split_once("\r\n\r\n"))
        .or_else(|| tail.split_once("\n\n"))
        .map(|(input, _)| input)
        .unwrap_or(tail);
    Some((start, CodexComposer::Typed(strip_ansi(input))))
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Strip the CSI/OSC sequences emitted by `pty peek` while preserving rendered text.
fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != 0x1b {
            let ch = input[i..].chars().next().expect("valid UTF-8 boundary");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        i += 1;
        if i >= bytes.len() {
            break;
        }
        match bytes[i] {
            b'[' => {
                i += 1;
                let params_start = i;
                let mut final_byte = None;
                while i < bytes.len() {
                    let byte = bytes[i];
                    i += 1;
                    if (0x40..=0x7e).contains(&byte) {
                        final_byte = Some(byte);
                        break;
                    }
                }
                // `pty peek` may encode visible spaces as a bounded cursor-forward CSI instead of
                // literal bytes. Preserve that rendered text for exact staged-composer comparison;
                // unsupported/private/huge sequences still disappear and therefore fail closed.
                if final_byte == Some(b'C') {
                    let params = &bytes[params_start..i.saturating_sub(1)];
                    let width = if params.is_empty() {
                        Some(1)
                    } else if params.iter().all(u8::is_ascii_digit) {
                        std::str::from_utf8(params)
                            .ok()
                            .and_then(|value| value.parse::<usize>().ok())
                            .map(|value| value.max(1))
                    } else {
                        None
                    };
                    if let Some(width) = width.filter(|width| *width <= 512) {
                        for _ in 0..width {
                            out.push(' ');
                        }
                    }
                }
            }
            b']' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == 0x07 {
                        i += 1;
                        break;
                    }
                    if bytes[i] == 0x1b && bytes.get(i + 1) == Some(&b'\\') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    out
}

/// `<pty-session-dir>/<session>.pid` + `kill(pid, 0)`; any miss → gone. Cheap (no `pty` fork) and it
/// agrees with how `pty` itself decides a session is live.
pub fn session_alive(session: &str) -> bool {
    let pidfile = pty_session_dir().join(format!("{session}.pid"));
    let Ok(raw) = std::fs::read_to_string(&pidfile) else {
        return false;
    };
    let Ok(pid) = raw.trim().parse::<i32>() else {
        return false;
    };
    // signal 0 = existence + permission probe, no signal delivered.
    pid > 0 && unsafe { libc::kill(pid, 0) == 0 }
}

/// The `pty` session registry dir — MUST mirror pty-rust's `registry::session_dir()` resolution
/// (`$PTY_ROOT`, else the deprecated `$PTY_SESSION_DIR`, else `~/.local/state/pty`), or the pidfile
/// probe looks in the wrong place.
fn pty_session_dir() -> PathBuf {
    for var in ["PTY_ROOT", "PTY_SESSION_DIR"] {
        if let Ok(d) = std::env::var(var)
            && !d.is_empty()
        {
            return PathBuf::from(d);
        }
    }
    home_dir().join(".local").join("state").join("pty")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// Refuse to start if the `pty` binary isn't reachable — a ding that can't reach `pty` delivers
/// nothing, so fail loudly at boot instead of silently dropping every poke.
pub fn probe_pty_on_path() -> anyhow::Result<()> {
    match Command::new("pty").arg("--help").output() {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => anyhow::bail!("`pty --help` exited {}", out.status),
        Err(e) => anyhow::bail!("`pty` not runnable on PATH: {e}"),
    }
}

/// The logically unread messages in `inbox_dir` not in `seen`, in send order — updating `seen` to
/// exactly that set. A same-named sibling archive receipt suppresses a raw inbox copy restored by an
/// eventually-consistent sync, so archive remove/reappear races never re-poke. The first call seeds
/// `seen` and returns the whole backlog; callers discard that to get push-only-on-new semantics.
pub fn new_arrivals(inbox_dir: &Path, seen: &mut HashSet<String>) -> Vec<Message> {
    let msgs = message::list_inbox(inbox_dir).unwrap_or_default(); // sorted by (ts_ms, filename)
    let current: HashSet<&str> = msgs.iter().map(|m| m.filename.as_str()).collect();
    seen.retain(|f| current.contains(f.as_str()));
    msgs.into_iter()
        .filter(|m| seen.insert(m.filename.clone()))
        .collect()
}

/// Consecutive liveness misses tolerated before the ding gives up on a session it has seen alive
/// Debounces a transient probe blip from killing the sidecar.
const SESSION_GONE_DEBOUNCE_MISSES: u32 = 3;

/// The startup-grace + debounce state for the session-liveness watch. Kept as a pure decision (fed one
/// probe result at a time) so it is unit-testable without a real pty or timers.
#[derive(Default)]
struct SessionWatch {
    /// The target must be seen alive at least once before a miss can ever mean "gone" — otherwise a
    /// launch race (ding up before the agent pty registers its pidfile) would kill the sidecar.
    seen_alive: bool,
    /// Consecutive misses since the last time it was seen alive.
    misses: u32,
}

/// Whether the watch loop should keep polling or exit because the session is gone.
#[derive(Debug, PartialEq, Eq)]
enum WatchStep {
    Poll,
    Gone,
}

impl SessionWatch {
    /// Feed one liveness probe result. `Gone` only after the session has been seen alive and then
    /// missed [`SESSION_GONE_DEBOUNCE_MISSES`] times in a row; a live probe resets the miss counter.
    fn step(&mut self, alive: bool) -> WatchStep {
        if alive {
            self.seen_alive = true;
            self.misses = 0;
            return WatchStep::Poll;
        }
        if !self.seen_alive {
            return WatchStep::Poll; // startup grace — still waiting for the target to register
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
    /// Fallback poll cadence — also how often liveness is re-checked. Folder-watch events reconcile
    /// immediately regardless; the poll is the always-on safety net (like the supervisor loop).
    pub poll: Duration,
    /// How often to re-write the agent's `status` to bump its mtime (while the pty is alive), so a
    /// live agent stays out of the `unknown` staleness window.
    pub status_refresh: Duration,
    /// Pane-delivery strategy. Defaults to fail-closed Codex guarding.
    pub delivery_mode: DeliveryMode,
}

impl Default for DingConfig {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(1000),
            status_refresh: status::STATUS_REFRESH,
            delivery_mode: DeliveryMode::CodexGuarded,
        }
    }
}

/// Watch `inbox_dir` and poke `poker` on each new arrival until `stop` is set or the target session is
/// gone. Existing inbox contents are seeded as already-seen (no startup poke storm). On the first
/// live/available pass only, one seeded message may resume if a visible bottom-composer DING has a
/// stable id naming exactly one unread backlog file. Unsafe pane or `busy`/`dnd` status defers in FIFO
/// order; transport failures retain the head for retry. A message archived while deferred is pruned
/// before delivery.
pub fn run_ding(
    inbox_dir: &Path,
    status_path: Option<&Path>,
    poker: &dyn Poker,
    config: &DingConfig,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    // Arm the watcher BEFORE seeding: a file that lands in the gap between the seed scan and the watch
    // starting still fires an event we process next pass, so nothing slips through. Watch the inbox
    // if it exists, else its parent (`resources/`) so we still catch the inbox dir being created.
    // Best-effort — the poll timer is the real correctness guarantee.
    let (tx, rx) = channel::<()>();
    let watch_at = if inbox_dir.exists() {
        inbox_dir
    } else {
        inbox_dir.parent().unwrap_or(inbox_dir)
    };
    let _watcher = watch_dir(watch_at, tx);

    // Seed: everything already in the inbox is already-seen (no startup poke storm; the agent's boot
    // ritual drains that backlog). Only arrivals AFTER this are poked.
    let mut seen = HashSet::new();
    let backlog = new_arrivals(inbox_dir, &mut seen);
    eprintln!(
        "st2 ding: ready — seeded {} existing message(s) as already-seen; watching for new arrivals.",
        backlog.len()
    );
    let mut seeded_recovery = Some(backlog);

    let mut watch = SessionWatch::default();
    let mut logged_waiting = false;
    let mut last_refresh: Option<Instant> = None;
    let mut pending = VecDeque::new();
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
            // Presence refresh (brief-023): while the pty is up, bump the agent's status mtime on a
            // cadence, preserving its value, so a live-but-idle agent doesn't age into `unknown`.
            if let Some(sp) = status_path
                && last_refresh.is_none_or(|t| t.elapsed() >= config.status_refresh)
            {
                let _ = status::refresh(sp); // best-effort — a hiccup must not kill the sidecar
                last_refresh = Some(Instant::now());
            }
            if !delivery_suppressed(status_path)
                && let Some(mut backlog) = seeded_recovery.take()
            {
                retain_current_messages(inbox_dir, &mut backlog);
                match poker.recover_seeded(&backlog) {
                    Ok(PokeOutcome::Delivered) => {
                        eprintln!(
                            "st2 ding: resumed one exact staged message from the seeded inbox."
                        );
                    }
                    Ok(PokeOutcome::Deferred) => {}
                    Err(error) => {
                        eprintln!("st2 ding: seeded staged-message recovery failed: {error}")
                    }
                }
            }
            // Only scan once the target is up: arrivals during the startup race stay unseen and are
            // queued on the first live pass, so nothing is lost to the launch gap.
            for msg in new_arrivals(inbox_dir, &mut seen) {
                pending.push_back(msg);
            }
            prune_archived_pending(inbox_dir, &mut pending);
            flush_pending(status_path, &mut pending, poker);
        } else if !watch.seen_alive && !logged_waiting {
            eprintln!(
                "st2 ding: target pty session not yet registered; waiting before enabling exit-when-gone."
            );
            logged_waiting = true;
        }
        // Wait for a folder change or the poll timeout, then coalesce a burst into one scan.
        match rx.recv_timeout(config.poll) {
            Ok(()) => drain(&rx),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                // Watcher never started (timer-only) — the recv returns immediately, so sleep to keep
                // the poll cadence instead of busy-looping.
                std::thread::sleep(config.poll);
            }
        }
    }
    Ok(())
}

fn retain_current_messages(inbox_dir: &Path, messages: &mut Vec<Message>) {
    let Ok(current) = message::list_inbox(inbox_dir) else {
        messages.clear();
        return;
    };
    let filenames: HashSet<&str> = current.iter().map(|msg| msg.filename.as_str()).collect();
    messages.retain(|msg| filenames.contains(msg.filename.as_str()));
}

fn prune_archived_pending(inbox_dir: &Path, pending: &mut VecDeque<Message>) {
    let Ok(current) = message::list_inbox(inbox_dir) else {
        return;
    };
    let filenames: HashSet<&str> = current.iter().map(|msg| msg.filename.as_str()).collect();
    pending.retain(|msg| filenames.contains(msg.filename.as_str()));
}

fn delivery_suppressed(status_path: Option<&Path>) -> bool {
    status_path.is_some_and(|path| {
        matches!(
            status::read_state(path),
            status::State::Busy | status::State::Dnd
        )
    })
}

fn flush_pending(status_path: Option<&Path>, pending: &mut VecDeque<Message>, poker: &dyn Poker) {
    if delivery_suppressed(status_path) {
        return;
    }

    while let Some(msg) = pending.front() {
        match poker.poke(&poke_text(msg)) {
            Ok(PokeOutcome::Delivered) => {
                pending.pop_front();
            }
            Ok(PokeOutcome::Deferred) => break,
            Err(e) => {
                eprintln!("st2 ding: {e}");
                break;
            }
        }
    }
}

/// Set by SIGINT/SIGTERM so `st2 ding` exits cleanly when st2 tears the sidecar down.
static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn install_signal_handler() {
    let handler = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
}

/// Boot and run the sidecar: refuse if `pty` is unreachable, install the stop-signal handler, then
/// watch `inbox_dir` and poke `session` until interrupted or the session is gone, refreshing the
/// agent's presence at `status_path` while the pty is alive.
pub fn serve(
    inbox_dir: &Path,
    status_path: &Path,
    session: &str,
    config: &DingConfig,
) -> anyhow::Result<()> {
    probe_pty_on_path()?;
    install_signal_handler();
    let poker = match config.delivery_mode {
        DeliveryMode::CodexGuarded => PtyPoker::new(session),
        DeliveryMode::Legacy => PtyPoker::new_legacy(session),
    };
    run_ding(inbox_dir, Some(status_path), &poker, config, &STOP)
}

/// Best-effort recursive watcher: sends `()` on any change under `dir`. `None` (timer-only fallback)
/// if the platform or path can't be watched.
fn watch_dir(dir: &Path, tx: std::sync::mpsc::Sender<()>) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_ok() {
            let _ = tx.send(());
        }
    })
    .ok()?;
    watcher.watch(dir, RecursiveMode::Recursive).ok()?;
    Some(watcher)
}

fn drain(rx: &Receiver<()>) {
    while rx.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::send_to_inbox;

    fn msg(filename: &str, from: &str, subject: Option<&str>) -> Message {
        Message {
            filename: filename.to_string(),
            ts_ms: 0,
            from: Some(from.to_string()),
            subject: subject.map(str::to_string),
            in_reply_to: None,
            tags: vec![],
            priority: None,
            body: String::new(),
        }
    }

    #[test]
    fn poke_id_extracts_rand6() {
        assert_eq!(poke_id("1714826789012-x9k4mz.md"), "x9k4mz");
        assert_eq!(poke_id("1714826789012-abciou.md"), "abciou"); // full grammar
        assert_eq!(poke_id("notes.md"), "notes"); // off-grammar fallback
    }

    #[test]
    fn poke_text_shape_and_defaults() {
        assert_eq!(
            poke_text(&msg("1714826789012-x9k4mz.md", "alice", Some("deploy?"))),
            "[DING] new st2 message: [id:x9k4mz] deploy? (from alice); check your inbox"
        );
        // Empty/missing subject + from degrade to stable defaults.
        let mut m = msg("1714826789012-x9k4mz.md", "", Some(""));
        m.from = None;
        assert_eq!(
            poke_text(&m),
            "[DING] new st2 message: [id:x9k4mz] (no subject) (from unknown); check your inbox"
        );
    }

    #[test]
    fn pty_send_args_wire_shape() {
        // Retained only for a pane with no Codex signature.
        let args = pty_send_args("my-session", "[DING] hi");
        assert_eq!(
            args,
            vec![
                "send",
                "my-session",
                "--with-delay",
                "0.5",
                "--seq",
                "[DING] hi",
                "--seq",
                "key:return"
            ]
        );
    }

    #[test]
    fn codex_stage_never_contains_return_and_submit_is_bare_return() {
        assert_eq!(
            pty_stage_args("my-session", "[DING] hi"),
            vec!["send", "my-session", "--paste", "[DING] hi"]
        );
        assert_eq!(
            pty_dismiss_plan_modal_args("my-session"),
            vec!["send", "my-session", "--seq", "key:escape"]
        );
        assert_eq!(
            pty_submit_args("my-session"),
            vec!["send", "my-session", "--seq", "key:return"]
        );
    }

    fn idle_codex_screen() -> String {
        "\x1b[1;22m›\x1b[1C\x1b[22;2mExplain this codebase\r\n\r\n\
         \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
            .to_string()
    }

    fn staged_codex_screen(text: &str) -> String {
        format!(
            "\x1b[1;22m›\x1b[1C\x1b[0m{text}\r\n\r\n\
             \x1b[2C\x1b[2mtab to queue message\x1b[0m"
        )
    }

    fn current_macos_idle_codex_screen() -> String {
        "\x1b[1m›\x1b[1C\x1b[22;2mImprove documentation in @filename\r\n\r\n\
         \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
            .to_string()
    }

    fn current_macos_staged_codex_screen(text: &str) -> String {
        let text = text.replace(' ', "\x1b[1C");
        format!(
            "\x1b[1m›\x1b[1C\x1b[0m{text}\r\n\r\n\
             \x1b[2Cgpt-5.6-sol xhigh · /workspace"
        )
    }

    fn current_linux_idle_codex_screen() -> String {
        "\x1b[48;5;234m \x1b[79X\r\n\
         \x1b[1m›\x1b[22m \x1b[2mImplement {feature}\x1b[59X\r\n\
         \x1b[22m \x1b[79X\r\n"
            .to_string()
    }

    fn current_linux_staged_codex_screen(text: &str) -> String {
        format!(
            "\x1b[48;5;234m \x1b[79X\r\n\
             \x1b[1m›\x1b[22m {text}\x1b[48X\r\n\
             \x20\x1b[79X\r\n\
             \x1b[0m\x1b[2C\x1b[2mtab to queue message\x1b[0m"
        )
    }

    fn modal_codex_screen(text: &str) -> String {
        format!(
            "\x1b[1;2m› \x1b[0m{text}\r\n\r\n\r\n\
             \x1b[2C\x1b[1mOur systems are thinking a bit more about this request before responding.\r\n\
             \x1b[2C\x1b[22;2mHang tight or retry with a faster model for a quicker response.\r\n\r\n\
             \x1b[0m› 1. Retry with a faster model\r\n\
             \x1b[2C2. Dismiss and keep waiting\r\n\
             \x1b[2C3. Learn more\r\n"
        )
    }

    fn plan_modal_codex_screen(text: &str) -> String {
        format!(
            "\x1b[1m›\x1b[1C\x1b[0m{text}\r\n\r\n\
             \x1b[1mCreate a plan?\x1b[0m\r\n\
             \x1b[2mshift+tab use Plan mode   esc dismiss\x1b[0m\r\n"
        )
    }

    #[test]
    fn codex_choice_modal_blocks_return_even_when_ding_text_is_staged() {
        let expected = "[DING] new st2 message: [id:k0ygwh] safety (from cos); check your inbox";
        assert_eq!(
            classify_pane(&modal_codex_screen(expected), expected),
            PaneState::Blocked
        );
    }

    #[test]
    fn only_exact_staged_notice_under_current_plan_modal_is_dismissible() {
        let expected = "[DING] new st2 message: [id:k0ygwh] safety (from cos); check your inbox";
        assert_eq!(
            classify_pane(&plan_modal_codex_screen(expected), expected),
            PaneState::DismissiblePlanModal
        );
        assert_eq!(
            classify_pane(
                &plan_modal_codex_screen("half-written human request"),
                expected
            ),
            PaneState::Blocked
        );
        assert_eq!(
            classify_pane(
                &format!(
                    "• Working (2s • esc to interrupt)\r\n{}",
                    plan_modal_codex_screen(expected)
                ),
                expected
            ),
            PaneState::Blocked
        );

        let historical = format!(
            "{}\r\n{}",
            plan_modal_codex_screen("old system notice"),
            staged_codex_screen(expected)
        );
        assert_eq!(
            classify_pane(&historical, expected),
            PaneState::Staged,
            "a historical plan prompt above the bottom composer is not a live modal"
        );
    }

    #[test]
    fn seeded_recovery_matches_exactly_one_visible_unread_notice() {
        let exact = msg("1714826789012-k0ygwh.md", "cos", Some("safety"));
        let other = msg("1714826789013-abc123.md", "alice", Some("other"));
        let expected = poke_text(&exact);
        let backlog = [other.clone(), exact.clone()];

        assert_eq!(
            exact_staged_backlog_notice(&plan_modal_codex_screen(&expected), &backlog),
            Some(expected.clone())
        );
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&expected), &backlog),
            Some(expected.clone())
        );
        let rolling_notice = format!(
            "[DING] new {} message: [id:k0ygwh] safety (from cos); check your inbox",
            ["small", "talk"].concat()
        );
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&rolling_notice), &backlog),
            Some(rolling_notice.clone()),
            "only the description before the stable id may differ across the rolling window"
        );
        assert_eq!(
            exact_staged_backlog_notice(&plan_modal_codex_screen(&rolling_notice), &backlog),
            Some(rolling_notice.clone()),
            "the rolling form remains recoverable under the exact stable plan modal"
        );
        assert_eq!(
            exact_staged_backlog_notice(
                &staged_codex_screen("half-written human request"),
                &backlog
            ),
            None
        );
        assert_eq!(
            exact_staged_backlog_notice(&modal_codex_screen(&expected), &backlog),
            None,
            "a generic choice modal remains blocked"
        );
        assert_eq!(
            exact_staged_backlog_notice(&idle_codex_screen(), &backlog),
            None,
            "an idle restart never replays seeded inbox work"
        );
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&expected), &[]),
            None,
            "an archived or otherwise absent inbox message cannot recover"
        );
        assert_eq!(
            exact_staged_backlog_notice(
                &staged_codex_screen(&expected),
                std::slice::from_ref(&other)
            ),
            None,
            "visible text must still be an unread inbox message"
        );
        assert_eq!(
            exact_staged_backlog_notice(
                &staged_codex_screen(
                    "[DING] changed wording [id:zzzzzz] but no unread file has this id"
                ),
                &backlog
            ),
            None
        );
        for edited in [
            format!("extra {rolling_notice}"),
            rolling_notice.replace("safety", "human-edited subject"),
            rolling_notice.replace("(from cos)", "(from mallory)"),
            format!("{rolling_notice} extra tail"),
            rolling_notice.replace("[id:k0ygwh]", "[id:ABC123]"),
            rolling_notice.replace("new ", "new \u{0}"),
        ] {
            assert_eq!(
                exact_staged_backlog_notice(&staged_codex_screen(&edited), &backlog),
                None,
                "human-edited or ambiguous wire text must fail closed: {edited:?}"
            );
        }
        assert_eq!(
            exact_staged_backlog_notice(
                &staged_codex_screen("[DING] ambiguous [id:k0ygwh] second marker [id:abc123]"),
                &backlog
            ),
            None
        );
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&expected), &[exact.clone(), exact]),
            None,
            "an ambiguous duplicate match fails closed"
        );

        let mut defaults = msg("1714826789014-def456.md", "", None);
        defaults.from = None;
        let default_rolling =
            "[DING] rolling words [id:def456] (no subject) (from unknown); check your inbox";
        assert_eq!(
            exact_staged_backlog_notice(
                &current_linux_staged_codex_screen(default_rolling),
                &[defaults]
            ),
            Some(default_rolling.to_string()),
            "message-derived defaults are part of the authoritative tail"
        );

        let accepted_legacy_id = msg("1714826789015-iiiiii.md", "cos", Some("legacy id"));
        let accepted_legacy_notice =
            "[DING] rolling words [id:iiiiii] legacy id (from cos); check your inbox";
        assert_eq!(
            exact_staged_backlog_notice(
                &staged_codex_screen(accepted_legacy_notice),
                &[accepted_legacy_id]
            ),
            Some(accepted_legacy_notice.to_string()),
            "recovery follows the full durable reader grammar, not only the generator subset"
        );
    }

    #[test]
    fn codex_idle_staged_and_human_typing_states_are_distinct() {
        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let human = staged_codex_screen("half-written human request");
        let working = format!(
            "• Working (42s • esc to interrupt)\r\n\r\n{}",
            staged_codex_screen(expected)
        );

        assert_eq!(
            classify_pane(&idle_codex_screen(), expected),
            PaneState::Idle
        );
        assert_eq!(
            classify_pane(&staged_codex_screen(expected), expected),
            PaneState::Staged
        );
        assert_eq!(classify_pane(&human, expected), PaneState::Blocked);
        assert_eq!(classify_pane(&working, expected), PaneState::Blocked);
    }

    #[test]
    fn current_codex_renderer_variants_preserve_idle_staged_and_human_distinctions() {
        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";

        for idle in [
            current_macos_idle_codex_screen(),
            current_linux_idle_codex_screen(),
        ] {
            assert_eq!(classify_pane(&idle, expected), PaneState::Idle);
        }
        for (renderer, staged) in [
            ("macOS", current_macos_staged_codex_screen(expected)),
            ("Linux", current_linux_staged_codex_screen(expected)),
        ] {
            assert_eq!(
                classify_pane(&staged, expected),
                PaneState::Staged,
                "{renderer} staged composer"
            );
        }
        for human in [
            current_macos_staged_codex_screen("half-written human request"),
            current_linux_staged_codex_screen("half-written human request"),
        ] {
            assert_eq!(classify_pane(&human, expected), PaneState::Blocked);
        }
    }

    #[test]
    fn ansi_cursor_forward_is_rendered_as_bounded_spaces() {
        assert_eq!(
            strip_ansi("one\x1b[1Ctwo\x1b[Cthree\x1b[2Cfour"),
            "one two three  four"
        );
        assert_eq!(
            strip_ansi("one\x1b[513Ctwo"),
            "onetwo",
            "huge cursor motion is not expanded"
        );
        assert_eq!(
            strip_ansi("one\x1b[?1Ctwo"),
            "onetwo",
            "private cursor modes are not interpreted as composer spaces"
        );
    }

    #[test]
    fn bottom_most_composer_marker_wins_over_historical_typed_content() {
        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let screen = format!(
            "{}\r\n{}",
            staged_codex_screen("old human prompt"),
            idle_codex_screen()
        );

        assert_eq!(classify_pane(&screen, expected), PaneState::Idle);
    }

    #[test]
    fn post_paste_modal_or_composer_mismatch_defers_without_return_then_submits_when_safe() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        for blocked_after_paste in [
            modal_codex_screen(expected),
            staged_codex_screen("human changed the composer"),
        ] {
            let screens = RefCell::new(VecDeque::from([idle_codex_screen(), blocked_after_paste]));
            let actions = RefCell::new(Vec::new());
            let before = Cell::new(0);
            let outcome = guarded_poke(
                expected,
                &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
                &mut || {
                    actions.borrow_mut().push("paste");
                    Ok(())
                },
                &mut || unreachable!("generic modal must not be dismissed"),
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || {
                    before.set(before.get() + 1);
                    Ok(())
                },
            )
            .unwrap();

            assert_eq!(outcome, PokeOutcome::Deferred);
            assert_eq!(actions.borrow().as_slice(), ["paste"]);
            assert_eq!(before.get(), 0, "no durable attempt before a safe submit");

            let mut safe_screen = VecDeque::from([staged_codex_screen(expected)]);
            let outcome = guarded_poke(
                expected,
                &mut || Ok(safe_screen.pop_front().unwrap()),
                &mut || unreachable!("the exact text is already staged"),
                &mut || unreachable!("there is no plan modal"),
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || {
                    before.set(before.get() + 1);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(outcome, PokeOutcome::Delivered);
            assert_eq!(actions.borrow().as_slice(), ["paste", "return"]);
            assert_eq!(before.get(), 1);
        }

        let peek_count = Cell::new(0);
        let actions = RefCell::new(Vec::new());
        let before = Cell::new(0);
        let outcome = guarded_poke(
            expected,
            &mut || {
                let count = peek_count.get();
                peek_count.set(count + 1);
                if count == 0 {
                    Ok(idle_codex_screen())
                } else {
                    anyhow::bail!("post-paste peek unavailable")
                }
            },
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || unreachable!("an unreadable screen must not dismiss a modal"),
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        );
        assert!(outcome.is_err(), "a failed re-peek must fail closed");
        assert_eq!(actions.borrow().as_slice(), ["paste"]);
        assert_eq!(before.get(), 0);
    }

    #[test]
    fn stable_plan_modal_dismisses_with_escape_then_delivers_exact_notice() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let screens = RefCell::new(VecDeque::from([
            idle_codex_screen(),
            plan_modal_codex_screen(expected),
            plan_modal_codex_screen(expected),
            staged_codex_screen(expected),
        ]));
        let actions = RefCell::new(Vec::new());
        let before = Cell::new(0);
        let outcome = guarded_poke(
            expected,
            &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("escape");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(outcome, PokeOutcome::Delivered);
        assert_eq!(actions.borrow().as_slice(), ["paste", "escape", "return"]);
        assert_eq!(before.get(), 1);
    }

    #[test]
    fn changed_plan_modal_defers_without_escape_or_return() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let screens = RefCell::new(VecDeque::from([
            plan_modal_codex_screen(expected),
            staged_codex_screen(expected),
        ]));
        let actions = RefCell::new(Vec::new());
        let before = Cell::new(0);
        let outcome = guarded_poke(
            expected,
            &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
            &mut || unreachable!("the exact text is already staged"),
            &mut || {
                actions.borrow_mut().push("escape");
                Ok(())
            },
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(outcome, PokeOutcome::Deferred);
        assert!(actions.borrow().is_empty());
        assert_eq!(before.get(), 0);
    }

    #[test]
    fn plan_modal_escape_can_restage_one_empty_composer_but_never_a_changed_draft() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        for (after_escape, expected_actions, expected_outcome) in [
            (
                VecDeque::from([
                    plan_modal_codex_screen(expected),
                    plan_modal_codex_screen(expected),
                    idle_codex_screen(),
                    staged_codex_screen(expected),
                ]),
                vec!["escape", "paste", "return"],
                PokeOutcome::Delivered,
            ),
            (
                VecDeque::from([
                    plan_modal_codex_screen(expected),
                    plan_modal_codex_screen(expected),
                    staged_codex_screen("human changed the composer"),
                ]),
                vec!["escape"],
                PokeOutcome::Deferred,
            ),
            (
                VecDeque::from([
                    plan_modal_codex_screen(expected),
                    plan_modal_codex_screen(expected),
                    plan_modal_codex_screen(expected),
                ]),
                vec!["escape"],
                PokeOutcome::Deferred,
            ),
            (
                VecDeque::from([
                    plan_modal_codex_screen(expected),
                    plan_modal_codex_screen(expected),
                    modal_codex_screen(expected),
                ]),
                vec!["escape"],
                PokeOutcome::Deferred,
            ),
        ] {
            let screens = RefCell::new(after_escape);
            let actions = RefCell::new(Vec::new());
            let before = Cell::new(0);
            let outcome = guarded_poke(
                expected,
                &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
                &mut || {
                    actions.borrow_mut().push("paste");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("escape");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || {
                    before.set(before.get() + 1);
                    Ok(())
                },
            )
            .unwrap();

            assert_eq!(outcome, expected_outcome);
            assert_eq!(actions.into_inner(), expected_actions);
            assert_eq!(
                before.get(),
                usize::from(expected_outcome == PokeOutcome::Delivered)
            );
        }
    }

    #[test]
    fn pre_paste_modal_active_draft_and_peek_failure_fail_closed() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let screens = [
            modal_codex_screen(expected),
            format!(
                "• Working (2s • esc to interrupt)\r\n{}",
                idle_codex_screen()
            ),
            staged_codex_screen("human draft"),
        ];
        for screen in screens {
            let actions = RefCell::new(Vec::new());
            let before = Cell::new(0);
            let outcome = guarded_poke(
                expected,
                &mut || Ok(screen.clone()),
                &mut || {
                    actions.borrow_mut().push("paste");
                    Ok(())
                },
                &mut || unreachable!("unsafe panes must not be dismissed"),
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || {
                    before.set(before.get() + 1);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(outcome, PokeOutcome::Deferred);
            assert!(actions.borrow().is_empty());
            assert_eq!(before.get(), 0);
        }

        let actions = RefCell::new(Vec::new());
        assert!(
            guarded_poke(
                expected,
                &mut || anyhow::bail!("peek unavailable"),
                &mut || {
                    actions.borrow_mut().push("paste");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("escape");
                    Ok(())
                },
                &mut || {
                    actions.borrow_mut().push("return");
                    Ok(())
                },
                &mut || {},
                &mut || Ok(()),
            )
            .is_err()
        );
        assert!(actions.borrow().is_empty());
    }

    #[test]
    fn ordinary_idle_codex_poke_round_trips_paste_repeek_then_return() {
        use std::cell::{Cell, RefCell};

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let screens = RefCell::new(VecDeque::from([
            idle_codex_screen(),
            staged_codex_screen(expected),
        ]));
        let actions = RefCell::new(Vec::new());
        let before = Cell::new(0);
        let outcome = guarded_poke(
            expected,
            &mut || Ok(screens.borrow_mut().pop_front().unwrap()),
            &mut || {
                actions.borrow_mut().push("paste");
                Ok(())
            },
            &mut || unreachable!("there is no plan modal"),
            &mut || {
                actions.borrow_mut().push("return");
                Ok(())
            },
            &mut || {},
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(outcome, PokeOutcome::Delivered);
        assert_eq!(actions.borrow().as_slice(), ["paste", "return"]);
        assert_eq!(before.get(), 1);
    }

    #[test]
    fn ambiguous_screen_fails_closed_unless_legacy_harness_is_explicit() {
        use std::cell::{Cell, RefCell};

        assert_eq!(
            DeliveryMode::for_agent_command(None),
            DeliveryMode::CodexGuarded
        );
        assert_eq!(
            DeliveryMode::for_agent_command(Some("exec codex --model gpt-5.6-sol")),
            DeliveryMode::CodexGuarded
        );
        assert_eq!(
            DeliveryMode::for_agent_command(Some("exec /opt/bin/claude --dangerously-skip")),
            DeliveryMode::Legacy
        );
        assert_eq!(
            DeliveryMode::for_agent_command(Some("sh -c 'claude'")),
            DeliveryMode::CodexGuarded,
            "wrapped or unfamiliar commands cannot opt into unconditional Return"
        );

        let expected = "[DING] new st2 message: [id:abc123] hi (from alice); check your inbox";
        let actions = RefCell::new(Vec::new());
        let before = Cell::new(0);
        let outcome = guarded_poke(
            expected,
            &mut || Ok(String::new()),
            &mut || unreachable!("ambiguous screen must not stage"),
            &mut || unreachable!("ambiguous screen must not dismiss a modal"),
            &mut || unreachable!("ambiguous screen must not submit"),
            &mut || {},
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Deferred);
        assert!(actions.borrow().is_empty());
        assert_eq!(before.get(), 0);

        let outcome = legacy_poke(
            &mut || {
                actions.borrow_mut().push("legacy");
                Ok(())
            },
            &mut || {
                before.set(before.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outcome, PokeOutcome::Delivered);
        assert_eq!(actions.borrow().as_slice(), ["legacy"]);
        assert_eq!(before.get(), 1);
    }

    #[test]
    fn session_watch_startup_grace_never_exits_before_seen_alive() {
        let mut w = SessionWatch::default();
        // A launch race: probed dead many times before the target registers — must NOT exit.
        for _ in 0..10 {
            assert_eq!(w.step(false), WatchStep::Poll);
        }
        assert_eq!(w.step(true), WatchStep::Poll); // finally registers
    }

    #[test]
    fn session_watch_debounces_then_exits_after_seen_alive() {
        let mut w = SessionWatch::default();
        assert_eq!(w.step(true), WatchStep::Poll); // seen alive
        assert_eq!(w.step(false), WatchStep::Poll); // miss 1 — debounced
        assert_eq!(w.step(false), WatchStep::Poll); // miss 2 — debounced
        assert_eq!(w.step(false), WatchStep::Gone); // miss 3 — gone
    }

    #[test]
    fn session_watch_live_probe_resets_the_miss_counter() {
        let mut w = SessionWatch::default();
        w.step(true);
        w.step(false);
        w.step(false); // 2 misses
        assert_eq!(w.step(true), WatchStep::Poll); // recovered → reset
        assert_eq!(w.step(false), WatchStep::Poll); // needs a fresh 3
        assert_eq!(w.step(false), WatchStep::Poll);
        assert_eq!(w.step(false), WatchStep::Gone);
    }

    /// While the pty is alive, the ding refreshes the agent's presence (brief-023) — here a missing
    /// status is written to `available`. Backs the presence-liveness invariant (see INVARIANTS.md).
    #[test]
    fn run_ding_refreshes_presence_while_alive() {
        struct AlivePoker;
        impl Poker for AlivePoker {
            fn poke(&self, _: &str) -> anyhow::Result<PokeOutcome> {
                Ok(PokeOutcome::Delivered)
            }
            fn session_alive(&self) -> bool {
                true
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("resources").join("inbox");
        let sp = crate::status::status_path(tmp.path()); // missing initially
        let stop = AtomicBool::new(false);
        let config = DingConfig {
            poll: Duration::from_millis(5),
            status_refresh: Duration::from_millis(0),
            delivery_mode: DeliveryMode::CodexGuarded,
        };

        std::thread::scope(|s| {
            s.spawn(|| {
                // Let the loop take at least one live pass, then stop it.
                std::thread::sleep(Duration::from_millis(60));
                stop.store(true, Ordering::SeqCst);
            });
            run_ding(&inbox, Some(&sp), &AlivePoker, &config, &stop).unwrap();
        });

        assert_eq!(
            crate::status::read_state(&sp),
            crate::status::State::Available
        );
    }

    #[test]
    fn startup_recovers_seeded_backlog_once_only_after_dnd_clears() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::{Barrier, Mutex, mpsc};

        struct RecoveryPoker {
            recoveries: Mutex<Vec<Vec<String>>>,
            ordinary_pokes: Mutex<Vec<String>>,
            live_probes: AtomicUsize,
            second_probe: Barrier,
            recovered: mpsc::SyncSender<()>,
        }
        impl Poker for RecoveryPoker {
            fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
                self.ordinary_pokes.lock().unwrap().push(text.to_string());
                Ok(PokeOutcome::Delivered)
            }
            fn recover_seeded(&self, backlog: &[Message]) -> anyhow::Result<PokeOutcome> {
                self.recoveries
                    .lock()
                    .unwrap()
                    .push(backlog.iter().map(|msg| msg.filename.clone()).collect());
                self.recovered.send(()).unwrap();
                Ok(PokeOutcome::Delivered)
            }
            fn session_alive(&self) -> bool {
                if self.live_probes.fetch_add(1, Ordering::SeqCst) == 1 {
                    // The test thread now knows one complete dnd-gated pass happened. Hold the
                    // second pass here until it changes status to available.
                    self.second_probe.wait();
                    self.second_probe.wait();
                }
                true
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("resources").join("inbox");
        let status_path = crate::status::status_path(tmp.path());
        crate::status::set_state(&status_path, crate::status::State::Dnd).unwrap();
        let selected = send_to_inbox(&inbox, "alice", Some("seeded"), None, &[], "hi").unwrap();
        let silent = send_to_inbox(&inbox, "bob", Some("also seeded"), None, &[], "yo").unwrap();
        let mut expected_seed_order = vec![selected, silent];
        expected_seed_order.sort();
        let stop = AtomicBool::new(false);
        let (recovered_tx, recovered_rx) = mpsc::sync_channel(1);
        let poker = RecoveryPoker {
            recoveries: Mutex::new(Vec::new()),
            ordinary_pokes: Mutex::new(Vec::new()),
            live_probes: AtomicUsize::new(0),
            second_probe: Barrier::new(2),
            recovered: recovered_tx,
        };
        let config = DingConfig {
            poll: Duration::from_millis(1),
            status_refresh: Duration::from_secs(60),
            delivery_mode: DeliveryMode::CodexGuarded,
        };

        std::thread::scope(|scope| {
            let thread_poker = &poker;
            let thread_status_path = &status_path;
            let thread_stop = &stop;
            scope.spawn(move || {
                thread_poker.second_probe.wait();
                assert!(
                    thread_poker.recoveries.lock().unwrap().is_empty(),
                    "dnd must suppress seeded recovery on the first complete pass"
                );
                crate::status::set_state(thread_status_path, crate::status::State::Available)
                    .unwrap();
                thread_poker.second_probe.wait();
                recovered_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("available pass did not recover seeded backlog");
                thread_stop.store(true, Ordering::SeqCst);
            });
            run_ding(&inbox, Some(&status_path), &poker, &config, &stop).unwrap();
        });

        assert_eq!(
            poker.recoveries.lock().unwrap().as_slice(),
            [expected_seed_order],
            "the complete seeded backlog is offered for exact matching once"
        );
        assert!(
            poker.ordinary_pokes.lock().unwrap().is_empty(),
            "startup recovery never replays the backlog as ordinary arrivals"
        );
    }

    #[test]
    fn archived_seeded_message_is_not_a_recovery_candidate() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("resources").join("inbox");
        let archive = tmp.path().join("resources").join("archive");
        let filename = send_to_inbox(&inbox, "alice", Some("seeded"), None, &[], "hi").unwrap();
        let before = message::list_inbox(&inbox).unwrap();
        let visible = poke_text(&before[0]);
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&visible), &before),
            Some(visible.clone())
        );

        message::archive_msg(&inbox, &archive, &filename).unwrap();
        let after = message::list_inbox(&inbox).unwrap();
        assert!(after.is_empty());
        assert_eq!(
            exact_staged_backlog_notice(&staged_codex_screen(&visible), &after),
            None
        );
    }

    #[test]
    fn deferred_fifo_delivers_once_when_safe_and_dnd_is_untouched() {
        use std::sync::Mutex;

        struct ScriptedPoker {
            outcomes: Mutex<VecDeque<PokeOutcome>>,
            calls: Mutex<Vec<String>>,
        }
        impl Poker for ScriptedPoker {
            fn poke(&self, text: &str) -> anyhow::Result<PokeOutcome> {
                self.calls.lock().unwrap().push(text.to_string());
                Ok(self
                    .outcomes
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(PokeOutcome::Delivered))
            }
            fn session_alive(&self) -> bool {
                true
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let sp = crate::status::status_path(tmp.path());
        crate::status::set_state(&sp, crate::status::State::Dnd).unwrap();
        let first = msg("1700000000000-abc123.md", "alice", Some("first"));
        let second = msg("1700000000001-def456.md", "bob", Some("second"));
        let first_text = poke_text(&first);
        let second_text = poke_text(&second);
        let mut pending = VecDeque::from([first, second]);
        let poker = ScriptedPoker {
            outcomes: Mutex::new(VecDeque::from([
                PokeOutcome::Deferred,
                PokeOutcome::Delivered,
                PokeOutcome::Delivered,
            ])),
            calls: Mutex::new(Vec::new()),
        };

        flush_pending(Some(&sp), &mut pending, &poker);
        assert_eq!(pending.len(), 2);
        assert!(poker.calls.lock().unwrap().is_empty());
        assert_eq!(
            crate::status::read_state(&sp),
            crate::status::State::Dnd,
            "delivery gating must not alter dnd"
        );

        crate::status::set_state(&sp, crate::status::State::Available).unwrap();
        flush_pending(Some(&sp), &mut pending, &poker);
        assert_eq!(pending.len(), 2, "unsafe head remains queued");
        {
            let calls = poker.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0], first_text,
                "the later arrival cannot pass a deferred head"
            );
        }

        flush_pending(Some(&sp), &mut pending, &poker);
        assert!(pending.is_empty(), "both messages deliver once safe");
        flush_pending(Some(&sp), &mut pending, &poker);
        assert_eq!(
            poker.calls.lock().unwrap().as_slice(),
            [first_text.clone(), first_text, second_text],
            "the head retries once, the tail follows, and delivered work is never repeated"
        );
    }

    #[test]
    fn new_arrivals_seeds_then_detects_and_prunes() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let f1 = send_to_inbox(&inbox, "alice", Some("one"), None, &[], "hi").unwrap();

        // Seed: the pre-existing message is already-seen, so it is NOT a fresh arrival.
        let mut seen = HashSet::new();
        assert_eq!(new_arrivals(&inbox, &mut seen).len(), 1); // first call returns the backlog…
        assert!(new_arrivals(&inbox, &mut seen).is_empty()); // …and now nothing is new

        // A genuinely new message is detected exactly once.
        let f2 = send_to_inbox(&inbox, "bob", Some("two"), None, &[], "yo").unwrap();
        let fresh = new_arrivals(&inbox, &mut seen);
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].filename, f2);
        assert!(new_arrivals(&inbox, &mut seen).is_empty());

        // Archiving f1 out of the inbox prunes it from `seen` (no re-poke if a name ever recurs), and
        // f2 stays seen.
        std::fs::remove_file(inbox.join(&f1)).unwrap();
        assert!(new_arrivals(&inbox, &mut seen).is_empty());
        assert!(seen.contains(&f2) && !seen.contains(&f1));
    }

    #[test]
    fn archived_receipt_suppresses_remove_reappear_and_restart_pokes() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("resources").join("inbox");
        let archive = tmp.path().join("resources").join("archive");
        let filename = send_to_inbox(&inbox, "alice", Some("once"), None, &[], "hi").unwrap();

        let mut seen = HashSet::new();
        assert_eq!(new_arrivals(&inbox, &mut seen).len(), 1);
        crate::message::archive_msg(&inbox, &archive, &filename).unwrap();
        let bytes = std::fs::read(archive.join(&filename)).unwrap();

        // The normal removal prunes the filename from the running ding's volatile set.
        assert!(new_arrivals(&inbox, &mut seen).is_empty());
        assert!(!seen.contains(&filename));

        // A stale replica restores, disappears, and restores again. The durable receipt wins every
        // time, including after a ding restart with a brand-new in-memory `seen` set.
        std::fs::create_dir_all(&inbox).unwrap();
        std::fs::write(inbox.join(&filename), &bytes).unwrap();
        assert!(new_arrivals(&inbox, &mut seen).is_empty());
        std::fs::remove_file(inbox.join(&filename)).unwrap();
        assert!(new_arrivals(&inbox, &mut seen).is_empty());
        std::fs::write(inbox.join(&filename), &bytes).unwrap();
        assert!(new_arrivals(&inbox, &mut seen).is_empty());

        let mut restarted_seen = HashSet::new();
        assert!(new_arrivals(&inbox, &mut restarted_seen).is_empty());
        assert!(restarted_seen.is_empty());
    }
}
