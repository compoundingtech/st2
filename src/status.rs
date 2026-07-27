//! Native per-agent presence status.
//!
//! A `status` file (sibling of `agent.kdl` in the agent's dir) holds exactly one word: one of the
//! settable states. `unknown` is DERIVED, never written — a status whose mtime is older than
//! [`STATUS_STALE`] reads as `unknown` regardless of its contents, so a crashed/gone agent stops
//! reading as its last set value. Reads are permissive (missing → `offline`, corrupt → `offline`).
//! Writes are atomic (tmp + rename) so a concurrent reader never sees a partial file. The ding
//! periodically re-writes an agent's own non-DND status to bump the mtime *preserving the value* so
//! a healthy-but-idle agent never rots to `unknown` (the failure that read the whole fleet `unknown`
//! for 45 min). `dnd` is intentionally not refreshed: an abandoned hold ages to `unknown` after the
//! same stale window. The vocabulary, stale window, and file semantics are stable.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// A status file older than this reads as `unknown` no matter its contents.
pub const STATUS_STALE: Duration = Duration::from_secs(15 * 60);
/// How often a live agent's status should be refreshed to stay inside the stale window — 5 min gives
/// a 3× safety margin (two missed refreshes before `unknown`).
pub const STATUS_REFRESH: Duration = Duration::from_secs(5 * 60);

/// Presence state. `Unknown` is derived from staleness and is never written to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Offline,
    Available,
    Busy,
    Away,
    Dnd,
    Unknown,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Offline => "offline",
            State::Available => "available",
            State::Busy => "busy",
            State::Away => "away",
            State::Dnd => "dnd",
            State::Unknown => "unknown",
        }
    }

    /// Parse a *settable* state word — everything except the derived `unknown`. This is the `--set`
    /// validator: a user can never write `unknown` directly.
    pub fn parse_settable(s: &str) -> Option<State> {
        match s {
            "offline" => Some(State::Offline),
            "available" => Some(State::Available),
            "busy" => Some(State::Busy),
            "away" => Some(State::Away),
            "dnd" => Some(State::Dnd),
            _ => None,
        }
    }

    /// Parse any valid state word, including `unknown` — used when reading a file's contents.
    fn parse_any(s: &str) -> Option<State> {
        Self::parse_settable(s).or(if s == "unknown" { Some(State::Unknown) } else { None })
    }
}

/// An agent's presence file: `<agent_dir>/status`.
pub fn status_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("status")
}

/// Read an agent's effective presence. Order matters: missing file → `offline`;
/// mtime older than [`STATUS_STALE`] → `unknown` (regardless of contents); unreadable → `offline`;
/// else the first line's word if valid, else `offline` (never trust a corrupt file).
pub fn read_state(status_path: &Path) -> State {
    let meta = match fs::metadata(status_path) {
        Ok(m) => m,
        Err(_) => return State::Offline, // missing
    };
    if let Ok(mtime) = meta.modified()
        && let Ok(age) = SystemTime::now().duration_since(mtime)
        && age > STATUS_STALE
    {
        return State::Unknown; // stale → not trustworthy
    }
    let raw = match fs::read_to_string(status_path) {
        Ok(r) => r,
        Err(_) => return State::Offline,
    };
    let first = raw.lines().next().unwrap_or("").trim();
    State::parse_any(first).unwrap_or(State::Offline)
}

/// Set an agent's presence to a settable `state`, atomically (tmp sibling + rename), writing
/// `<state>\n`. Creates the agent dir if missing.
pub fn set_state(status_path: &Path, state: State) -> anyhow::Result<()> {
    write_atomic(status_path, state.as_str())
}

/// Outcome of a [`refresh`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// File present + a valid settable state → re-wrote the same value, mtime bumped.
    Refreshed,
    /// File recorded `dnd` → left untouched so an abandoned hold ages out.
    LeftDnd,
    /// File missing → wrote `available` (the sensible default for a connected agent).
    WroteDefault,
    /// File present but its contents aren't a settable state → left untouched.
    LeftCorrupt,
    /// File recorded `unknown` (which we never write) → left untouched.
    LeftUnknown,
    /// A read or write failed — best-effort; caller decides whether to log.
    Error,
}

/// Bump an agent's status mtime so a live-but-idle agent never rots to `unknown`, preserving the
/// recorded value. Missing → write `available`. A valid non-DND value → re-write the same value.
/// `dnd`, `unknown`, or corrupt → leave untouched (never renew a hold or invent a value). Atomic.
pub fn refresh(status_path: &Path) -> RefreshOutcome {
    if !status_path.exists() {
        return match write_atomic(status_path, State::Available.as_str()) {
            Ok(()) => RefreshOutcome::WroteDefault,
            Err(_) => RefreshOutcome::Error,
        };
    }
    let raw = match fs::read_to_string(status_path) {
        Ok(r) => r,
        Err(_) => return RefreshOutcome::Error,
    };
    let first = raw.lines().next().unwrap_or("").trim();
    if first == "unknown" {
        return RefreshOutcome::LeftUnknown;
    }
    if first == "dnd" {
        return RefreshOutcome::LeftDnd;
    }
    match State::parse_settable(first) {
        Some(s) => match write_atomic(status_path, s.as_str()) {
            Ok(()) => RefreshOutcome::Refreshed,
            Err(_) => RefreshOutcome::Error,
        },
        None => RefreshOutcome::LeftCorrupt,
    }
}

/// Atomic write: a temp sibling + rename, so a concurrent reader sees either the old bytes or the new
/// bytes, never a partial file.
fn write_atomic(path: &Path, value: &str) -> anyhow::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let tmp = dir.join(tmp_name());
    fs::write(&tmp, format!("{value}\n"))?;
    // rename over the target — atomic on the same filesystem.
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp); // best-effort cleanup
        return Err(e.into());
    }
    Ok(())
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A per-write-unique temp filename (pid + a process-local counter — no collisions within a process,
/// and the pid separates processes).
fn tmp_name() -> String {
    format!(".status.tmp-{}-{}", std::process::id(), TMP_COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as Dur;

    #[test]
    fn missing_is_offline() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read_state(&status_path(tmp.path())), State::Offline);
    }

    #[test]
    fn set_then_read_roundtrips_each_settable_state() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        for st in [State::Offline, State::Available, State::Busy, State::Away, State::Dnd] {
            set_state(&sp, st).unwrap();
            assert_eq!(read_state(&sp), st);
            // file content is exactly `<state>\n` (wire format).
            assert_eq!(fs::read_to_string(&sp).unwrap(), format!("{}\n", st.as_str()));
        }
    }

    #[test]
    fn unknown_is_not_settable() {
        assert!(State::parse_settable("unknown").is_none());
        assert!(State::parse_settable("available").is_some());
        assert!(State::parse_settable("bogus").is_none());
    }

    #[test]
    fn corrupt_contents_read_as_offline() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        fs::write(&sp, "garbage\n").unwrap();
        assert_eq!(read_state(&sp), State::Offline);
    }

    #[test]
    fn stale_mtime_reads_as_unknown_regardless_of_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        set_state(&sp, State::Busy).unwrap();
        // Backdate the mtime past the stale window.
        let old = SystemTime::now() - STATUS_STALE - Dur::from_secs(60);
        let f = fs::File::open(&sp).unwrap();
        f.set_modified(old).unwrap();
        assert_eq!(read_state(&sp), State::Unknown);
    }

    #[test]
    fn refresh_preserves_value_and_bumps_mtime() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        set_state(&sp, State::Busy).unwrap();
        // Backdate → would read unknown…
        let old = SystemTime::now() - STATUS_STALE - Dur::from_secs(60);
        fs::File::open(&sp).unwrap().set_modified(old).unwrap();
        assert_eq!(read_state(&sp), State::Unknown);
        // …refresh keeps `busy` but bumps mtime, so it reads busy again.
        assert_eq!(refresh(&sp), RefreshOutcome::Refreshed);
        assert_eq!(read_state(&sp), State::Busy);
    }

    #[test]
    fn refresh_missing_writes_available_default() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        assert_eq!(refresh(&sp), RefreshOutcome::WroteDefault);
        assert_eq!(read_state(&sp), State::Available);
    }

    #[test]
    fn refresh_leaves_dnd_to_age_out() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        set_state(&sp, State::Dnd).unwrap();
        let before = fs::metadata(&sp).unwrap().modified().unwrap();
        std::thread::sleep(Dur::from_millis(5));

        assert_eq!(refresh(&sp), RefreshOutcome::LeftDnd);
        assert_eq!(fs::metadata(&sp).unwrap().modified().unwrap(), before);
        assert_eq!(read_state(&sp), State::Dnd);
    }

    #[test]
    fn refresh_leaves_corrupt_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        fs::write(&sp, "garbage\n").unwrap();
        assert_eq!(refresh(&sp), RefreshOutcome::LeftCorrupt);
        assert_eq!(fs::read_to_string(&sp).unwrap(), "garbage\n"); // untouched
    }
}
