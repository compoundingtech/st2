//! The ding sidecar (M2.2): a native watcher over an agent's `resources/inbox` that pokes the agent's
//! pty on every new message arrival — st2's replacement for smalltalk's external `st ding`.
//!
//! It is wire-identical to the thing it replaces, so a running agent's `[DING]` handling is unchanged:
//! the same `[DING] new smalltalk message: [id:<rand6>] <subject> (from <sender>); check your inbox`
//! line, injected with the same `pty send <session> --with-delay 0.5 --seq <text> --seq key:return`
//! (text first, a beat, then Enter — so a bracketed-paste TUI commits the text before the return).
//!
//! **Scope (M2.2) is push-on-arrival: watch → poke.** The heavier smalltalk-ding behaviors are
//! deliberately deferred as we build toward the evals gate:
//!   - status-aware buffering (hold while busy/dnd, flush on available) — needs the M2.3 status
//!     projection to exist first;
//!   - the pane-typing guard (peek before injecting so we don't clobber a human mid-keystroke);
//!   - the periodic tidy / drift summary.
//!
//! It DOES run the brief-023 presence refresh: while the agent's pty is alive it re-writes the
//! agent's own `status` file on a cadence (preserving the value) so a healthy-but-idle agent never
//! rots to `unknown` — the exact failure that read the whole fleet `unknown` for 45 min. Once the pty
//! is gone the refresh stops (and the ding exits), so a dead agent correctly ages into `unknown`.
//!
//! Startup does NOT re-poke the existing backlog (the agent's boot ritual drains that) — only messages
//! that arrive after the ding starts are poked. A periodic full-backlog re-scan (for messages that
//! landed while the ding was down) is part of that deferred set.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use crate::message::{self, Message};
use crate::status;

/// The `<rand6>` of a `<unix-ms>-<rand6>.md` filename — the stable id an agent dedups re-pokes on.
/// Falls back to the `.md`-stripped stem for anything off-grammar (mirrors smalltalk's `ding_id`).
pub fn poke_id(filename: &str) -> &str {
    if message::is_message_filename(filename) {
        // `13 digits` + `-` = 14 bytes of prefix, then the 6 rand chars.
        &filename[14..20]
    } else {
        filename.strip_suffix(".md").unwrap_or(filename)
    }
}

/// The `[DING] …` line an agent sees in its terminal for one newly-arrived message. Byte-for-byte the
/// format smalltalk emits and every agent's `DING-BUS.md` pattern-matches. Empty/missing `subject`
/// and `from` degrade to `(no subject)` / `unknown`.
pub fn poke_text(msg: &Message) -> String {
    let subject = msg.subject.as_deref().filter(|s| !s.is_empty()).unwrap_or("(no subject)");
    let from = msg.from.as_deref().filter(|s| !s.is_empty()).unwrap_or("unknown");
    format!(
        "[DING] new smalltalk message: [id:{}] {subject} (from {from}); check your inbox",
        poke_id(&msg.filename)
    )
}

/// The `pty send` argv for one poke: the text as a `--seq` chunk, then `key:return`, with a `0.5s`
/// gap so the terminal commits the text before the Enter. Split out for wire-shape testing.
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

/// How the ding delivers a poke and checks liveness — abstracted so the watch loop is testable
/// without a real `pty`. Production shells out to the `pty` binary.
pub trait Poker {
    /// Inject `text` (then Enter) into the target pty session.
    fn poke(&self, text: &str) -> anyhow::Result<()>;
    /// True while the target pty session still exists — the ding exits cleanly once it's gone.
    fn session_alive(&self) -> bool;
}

/// Production [`Poker`]: shells out to the sibling `pty` binary and probes its pidfile for liveness.
pub struct PtyPoker {
    bin: String,
    session: String,
}

impl PtyPoker {
    pub fn new(session: impl Into<String>) -> Self {
        Self { bin: "pty".to_string(), session: session.into() }
    }
}

impl Poker for PtyPoker {
    fn poke(&self, text: &str) -> anyhow::Result<()> {
        let out = Command::new(&self.bin).args(pty_send_args(&self.session, text)).output()?;
        if !out.status.success() {
            anyhow::bail!(
                "`pty send {}` failed: {}",
                self.session,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn session_alive(&self) -> bool {
        session_alive(&self.session)
    }
}

/// `<pty-session-dir>/<session>.pid` + `kill(pid, 0)`; any miss → gone. Cheap (no `pty` fork) and it
/// agrees with how `pty` itself decides a session is live.
pub fn session_alive(session: &str) -> bool {
    let pidfile = pty_session_dir().join(format!("{session}.pid"));
    let Ok(raw) = std::fs::read_to_string(&pidfile) else { return false };
    let Ok(pid) = raw.trim().parse::<i32>() else { return false };
    // signal 0 = existence + permission probe, no signal delivered.
    pid > 0 && unsafe { libc::kill(pid, 0) == 0 }
}

/// The `pty` session registry dir — MUST mirror pty-rust's `registry::session_dir()` resolution
/// (`$PTY_ROOT`, else the deprecated `$PTY_SESSION_DIR`, else `~/.local/state/pty`), or the pidfile
/// probe looks in the wrong place. Under convoy, `$PTY_ROOT` points into the network's state dir.
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
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"))
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

/// The messages in `inbox_dir` not in `seen`, in send order — updating `seen` to exactly the inbox's
/// current contents (so archived/removed files drop out and never re-poke). The first call seeds
/// `seen` and returns the whole backlog; callers discard that to get push-only-on-new semantics.
pub fn new_arrivals(inbox_dir: &Path, seen: &mut HashSet<String>) -> Vec<Message> {
    let msgs = message::list_dir(inbox_dir).unwrap_or_default(); // sorted by (ts_ms, filename)
    let current: HashSet<&str> = msgs.iter().map(|m| m.filename.as_str()).collect();
    seen.retain(|f| current.contains(f.as_str()));
    msgs.into_iter().filter(|m| seen.insert(m.filename.clone())).collect()
}

/// Consecutive liveness misses tolerated before the ding gives up on a session it has seen alive
/// (matches smalltalk-ding). Debounces a transient probe blip from killing the sidecar.
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
        if self.misses >= SESSION_GONE_DEBOUNCE_MISSES { WatchStep::Gone } else { WatchStep::Poll }
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
}

impl Default for DingConfig {
    fn default() -> Self {
        Self { poll: Duration::from_millis(1000), status_refresh: status::STATUS_REFRESH }
    }
}

/// Watch `inbox_dir` and poke `poker` on each new arrival until `stop` is set or the target session is
/// gone. Existing inbox contents are seeded as already-seen (no startup poke storm). Poke failures are
/// non-fatal (logged to stderr) — a transient `pty send` hiccup must not kill the sidecar.
pub fn run_ding(
    inbox_dir: &Path,
    status_path: Option<&Path>,
    poker: &dyn Poker,
    config: &DingConfig,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    // Arm the watcher BEFORE seeding: a file that lands in the gap between the seed scan and the watch
    // starting still fires an event we process next pass, so nothing slips through (mirrors
    // smalltalk-ding). Watch the inbox if it exists, else its parent (`resources/`) so we still catch
    // the inbox dir being created. Best-effort — the poll timer is the real correctness guarantee.
    let (tx, rx) = channel::<()>();
    let watch_at = if inbox_dir.exists() { inbox_dir } else { inbox_dir.parent().unwrap_or(inbox_dir) };
    let _watcher = watch_dir(watch_at, tx);

    // Seed: everything already in the inbox is already-seen (no startup poke storm; the agent's boot
    // ritual drains that backlog). Only arrivals AFTER this are poked.
    let mut seen = HashSet::new();
    let backlog = new_arrivals(inbox_dir, &mut seen);
    eprintln!(
        "st2 ding: ready — seeded {} existing message(s) as already-seen; watching for new arrivals.",
        backlog.len()
    );

    let mut watch = SessionWatch::default();
    let mut logged_waiting = false;
    let mut last_refresh: Option<Instant> = None;
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
            // Only poke once the target is up: arrivals during the startup race stay unseen (we skip
            // the scan) and are poked on the first live pass, so nothing is lost to the launch gap.
            for msg in new_arrivals(inbox_dir, &mut seen) {
                let text = poke_text(&msg);
                if let Err(e) = poker.poke(&text) {
                    eprintln!("st2 ding: {e}");
                }
            }
        } else if !watch.seen_alive && !logged_waiting {
            eprintln!("st2 ding: target pty session not yet registered; waiting before enabling exit-when-gone.");
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
    let poker = PtyPoker::new(session.to_string());
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
            "[DING] new smalltalk message: [id:x9k4mz] deploy? (from alice); check your inbox"
        );
        // Empty/missing subject + from degrade to the smalltalk defaults.
        let mut m = msg("1714826789012-x9k4mz.md", "", Some(""));
        m.from = None;
        assert_eq!(
            poke_text(&m),
            "[DING] new smalltalk message: [id:x9k4mz] (no subject) (from unknown); check your inbox"
        );
    }

    #[test]
    fn pty_send_args_wire_shape() {
        // Must match smalltalk-ding byte-for-byte: text `--seq`, then `key:return`, gapped 0.5s.
        let args = pty_send_args("my-session", "[DING] hi");
        assert_eq!(
            args,
            vec!["send", "my-session", "--with-delay", "0.5", "--seq", "[DING] hi", "--seq", "key:return"]
        );
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
            fn poke(&self, _: &str) -> anyhow::Result<()> {
                Ok(())
            }
            fn session_alive(&self) -> bool {
                true
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("resources").join("inbox");
        let sp = crate::status::status_path(tmp.path()); // missing initially
        let stop = AtomicBool::new(false);
        let config = DingConfig { poll: Duration::from_millis(5), status_refresh: Duration::from_millis(0) };

        std::thread::scope(|s| {
            s.spawn(|| {
                // Let the loop take at least one live pass, then stop it.
                std::thread::sleep(Duration::from_millis(60));
                stop.store(true, Ordering::SeqCst);
            });
            run_ding(&inbox, Some(&sp), &AlivePoker, &config, &stop).unwrap();
        });

        assert_eq!(crate::status::read_state(&sp), crate::status::State::Available);
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
}
