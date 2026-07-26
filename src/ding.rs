//! Native inbox-to-terminal DING delivery.
//!
//! Every harness uses one transport:
//! `pty send <session> --with-delay 0.5 --seq <bracketed-paste notice> --seq key:return`.
//! The fixed delay is based on observed paste-settling behavior. It reduces early-submit races but
//! cannot guarantee modal safety: a terminal UI can still open a modal before Return arrives.
//!
//! The sidecar deliberately does not inspect or classify terminal pixels. It retains only the
//! transport-independent contracts: normalized single-line payloads, FIFO delivery, archive-receipt
//! deduplication, `busy`/`dnd` gating, presence refresh, and target-session liveness. Startup seeds
//! the existing inbox without poking it; the agent's boot ritual owns backlog draining.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use crate::message::{self, Message};
use crate::status;

const BRACKETED_PASTE_START: &str = "\x1b[200~";
const BRACKETED_PASTE_END: &str = "\x1b[201~";
const SUBJECT_MAX_CHARS: usize = 160;
const SENDER_MAX_CHARS: usize = 80;

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

/// Exact harness-neutral `pty send` argv: one bracketed-paste sequence, one fixed delay, then Return.
pub fn pty_send_args(session: &str, text: &str) -> Vec<String> {
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

/// How DING delivers a poke and checks liveness — abstracted so the watch loop is testable without
/// a real `pty`.
pub trait Poker {
    fn poke(&self, text: &str) -> anyhow::Result<()>;
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

    fn run(&self, args: Vec<String>) -> anyhow::Result<()> {
        let out = Command::new(&self.bin).args(args).output()?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty send {}` failed: {}",
                self.session,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    /// Central production path shared by inbox DING and scheduled shepherd prompts.
    ///
    /// Shepherd uses `before_submit` to durably record its attempt immediately before the one
    /// terminal command. Inbox delivery passes a no-op.
    pub fn poke_with(
        &self,
        text: &str,
        before_submit: &mut dyn FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        before_submit()?;
        self.run(pty_send_args(&self.session, text))
    }
}

impl Poker for PtyPoker {
    fn poke(&self, text: &str) -> anyhow::Result<()> {
        self.poke_with(text, &mut || Ok(()))
    }

    fn session_alive(&self) -> bool {
        session_alive(&self.session)
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
    match Command::new("pty").arg("--help").output() {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => anyhow::bail!("`pty --help` exited {}", out.status),
        Err(error) => anyhow::bail!("`pty` not runnable on PATH: {error}"),
    }
}

/// Logically unread messages in `inbox_dir` not in `seen`, in send order, while updating `seen` to
/// exactly the current unread set. A same-named archive receipt suppresses a restored raw inbox copy.
/// The first call returns the whole backlog; the sidecar discards it for push-only-on-new behavior.
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

/// Watch `inbox_dir` and poke each post-start arrival until stopped or the target session is gone.
///
/// Existing inbox contents are seeded without a poke. New arrivals remain FIFO-queued across
/// `busy`/`dnd` status and transport failures. Archive receipts prune queued work before delivery.
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
    let _watcher = watch_dir(watch_at, tx);

    let mut seen = HashSet::new();
    let backlog = new_arrivals(inbox_dir, &mut seen);
    eprintln!(
        "st2 ding: ready — seeded {} existing message(s) as already-seen; watching for new arrivals.",
        backlog.len()
    );

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
            if let Some(path) = status_path
                && last_refresh.is_none_or(|instant| instant.elapsed() >= config.status_refresh)
            {
                let _ = status::refresh(path);
                last_refresh = Some(Instant::now());
            }

            pending.extend(new_arrivals(inbox_dir, &mut seen));
            prune_archived_pending(inbox_dir, &mut pending);
            flush_pending(status_path, &mut pending, poker);
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

fn prune_archived_pending(inbox_dir: &Path, pending: &mut VecDeque<Message>) {
    let Ok(current) = message::list_inbox(inbox_dir) else {
        return;
    };
    let filenames: HashSet<&str> = current
        .iter()
        .map(|message| message.filename.as_str())
        .collect();
    pending.retain(|message| filenames.contains(message.filename.as_str()));
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

    while let Some(message) = pending.front() {
        if let Err(error) = poker.poke(&poke_text(message)) {
            eprintln!("st2 ding: {error}");
            break;
        }
        pending.pop_front();
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

/// Best-effort recursive watcher. `None` means timer-only fallback.
fn watch_dir(dir: &Path, tx: std::sync::mpsc::Sender<()>) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if result.is_ok() {
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
        fn poke(&self, text: &str) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(text.to_string());
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                anyhow::bail!("injected send failure");
            }
            Ok(())
        }

        fn session_alive(&self) -> bool {
            self.probes.fetch_add(1, Ordering::SeqCst);
            self.alive.load(Ordering::SeqCst)
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
        let args = pty_send_args("seat", &direct);
        let framed = &args[5];
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
    fn pty_send_is_one_exact_delayed_sequence_without_inspection_or_escape() {
        assert_eq!(
            pty_send_args("my-session", "hello\nworld"),
            vec![
                "send",
                "my-session",
                "--with-delay",
                "0.5",
                "--seq",
                "\x1b[200~hello world\x1b[201~",
                "--seq",
                "key:return",
            ]
        );

        let source = include_str!("ding.rs");
        for removed in [
            ["pty ", "peek"].concat(),
            ["key:", "escape"].concat(),
            ["Delivery", "Mode"].concat(),
            ["classify_", "pane"].concat(),
            ["recover_", "seeded"].concat(),
            ["Co", "dex"].concat(),
            ["Clau", "de"].concat(),
        ] {
            assert!(
                !source.contains(&removed),
                "renderer-specific path returned: {removed}"
            );
        }
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
    fn pending_delivery_respects_status_fifo_archive_and_transport_retry() {
        let agent = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(agent.path());
        let archive = archive_dir(agent.path());
        let status_path = status::status_path(agent.path());
        let first = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "one").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let second = send_to_inbox(&inbox, "bob", Some("second"), None, &[], "two").unwrap();
        let mut pending: VecDeque<Message> =
            message::list_inbox(&inbox).unwrap().into_iter().collect();
        let poker = RecordingPoker::live();

        status::set_state(&status_path, status::State::Busy).unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        status::set_state(&status_path, status::State::Dnd).unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert!(poker.calls.lock().unwrap().is_empty());

        archive_msg(&inbox, &archive, &first).unwrap();
        prune_archived_pending(&inbox, &mut pending);
        assert_eq!(
            pending
                .iter()
                .map(|message| message.filename.as_str())
                .collect::<Vec<_>>(),
            [second.as_str()]
        );

        *poker.failures.lock().unwrap() = 1;
        status::set_state(&status_path, status::State::Available).unwrap();
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert_eq!(pending.len(), 1, "a failed head remains queued");
        flush_pending(Some(&status_path), &mut pending, &poker);
        assert!(pending.is_empty());

        let calls = poker.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].contains("[id:"));
        assert_eq!(calls[0], calls[1], "the failed FIFO head retries first");
    }

    #[test]
    fn startup_backlog_is_silent_and_only_post_start_arrivals_poke() {
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
                while poker.calls.lock().unwrap().is_empty() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(2));
                }
                stop.store(true, Ordering::SeqCst);
            });

            run_ding(&inbox, Some(&status_path), &poker, &config, &stop).unwrap();
        });

        let calls = poker.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("post-start"));
        assert!(!calls[0].contains("seeded"));
    }
}
