//! Native per-agent presence status.
//!
//! A `status` file is a sibling of `agent.kdl` in the agent directory. The first line holds one
//! settable state. The optional second line holds `updated-at-unix-ms <timestamp>`. Old readers use
//! the first line and file mtime. New readers use the content timestamp when present and accept the
//! old bare state during a mixed-fleet upgrade. Thus, each heartbeat changes the replicated bytes.
//! `unknown` is derived and is never written. Reads are permissive: missing and corrupt files become
//! `offline`. Writes are atomic, so a reader never sees a partial file. A live session refreshes its
//! non-DND status. `dnd` is not refreshed, so an abandoned hold ages to `unknown`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A status record older than this reads as `unknown` no matter which state it contains.
pub const STATUS_STALE: Duration = Duration::from_secs(15 * 60);
/// A live session refreshes presence every five minutes. This permits two missed writes before the
/// 15-minute stale limit and produces 288 replicated writes each day.
pub const STATUS_REFRESH: Duration = Duration::from_secs(5 * 60);

const UPDATED_AT_PREFIX: &str = "updated-at-unix-ms ";

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
        Self::parse_settable(s).or(if s == "unknown" {
            Some(State::Unknown)
        } else {
            None
        })
    }
}

/// An agent's presence file: `<agent_dir>/status`.
pub fn status_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("status")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StatusRecord {
    state: State,
    updated_at_unix_ms: Option<u128>,
}

/// Read an agent's effective presence. A timestamped record uses its content timestamp. A legacy
/// bare state uses mtime. Missing, unreadable, and corrupt files read as `offline`.
pub fn read_state(status_path: &Path) -> State {
    read_state_at(status_path, SystemTime::now())
}

fn read_state_at(status_path: &Path, now: SystemTime) -> State {
    let meta = match fs::metadata(status_path) {
        Ok(m) => m,
        Err(_) => return State::Offline, // missing
    };
    let raw = match fs::read_to_string(status_path) {
        Ok(r) => r,
        Err(_) => return State::Offline,
    };
    let Some(record) = parse_record(&raw) else {
        return State::Offline;
    };
    let stale = match record.updated_at_unix_ms {
        Some(updated_at) => content_timestamp_is_stale(now, updated_at),
        None => meta
            .modified()
            .ok()
            .and_then(|mtime| now.duration_since(mtime).ok())
            .is_some_and(|age| age >= STATUS_STALE),
    };
    if stale { State::Unknown } else { record.state }
}

fn parse_record(raw: &str) -> Option<StatusRecord> {
    let mut lines = raw.lines();
    let state = State::parse_any(lines.next().unwrap_or("").trim())?;
    let updated_at_unix_ms = match lines.next() {
        None => None,
        Some(line) => {
            let value = line.trim().strip_prefix(UPDATED_AT_PREFIX)?;
            Some(value.parse().ok()?)
        }
    };
    if lines.next().is_some() {
        return None;
    }
    Some(StatusRecord {
        state,
        updated_at_unix_ms,
    })
}

fn content_timestamp_is_stale(now: SystemTime, updated_at_unix_ms: u128) -> bool {
    now.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| duration.as_millis().checked_sub(updated_at_unix_ms))
        .is_some_and(|age_ms| age_ms >= STATUS_STALE.as_millis())
}

/// Set an agent's presence atomically. The first line remains compatible with old readers. The
/// second line gives content-based synchronizers a changed value and gives new readers freshness.
pub fn set_state(status_path: &Path, state: State) -> anyhow::Result<()> {
    set_state_at(status_path, state, SystemTime::now())
}

fn set_state_at(status_path: &Path, state: State, now: SystemTime) -> anyhow::Result<()> {
    write_atomic(status_path, &encode_record(state, now)?)
}

fn encode_record(state: State, now: SystemTime) -> anyhow::Result<String> {
    let updated_at_unix_ms = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("presence timestamp is before the Unix epoch"))?
        .as_millis();
    Ok(format!(
        "{}\n{UPDATED_AT_PREFIX}{updated_at_unix_ms}\n",
        state.as_str()
    ))
}

/// Outcome of a [`refresh`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// A valid settable state received a new content timestamp.
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

/// Write a new content timestamp while preserving the state. Missing becomes `available`. `dnd`,
/// `unknown`, and corrupt records stay unchanged. The write is atomic.
pub fn refresh(status_path: &Path) -> RefreshOutcome {
    refresh_at(status_path, SystemTime::now())
}

fn refresh_at(status_path: &Path, now: SystemTime) -> RefreshOutcome {
    if !status_path.exists() {
        return match set_state_at(status_path, State::Available, now) {
            Ok(()) => RefreshOutcome::WroteDefault,
            Err(_) => RefreshOutcome::Error,
        };
    }
    let raw = match fs::read_to_string(status_path) {
        Ok(r) => r,
        Err(_) => return RefreshOutcome::Error,
    };
    let Some(record) = parse_record(&raw) else {
        return RefreshOutcome::LeftCorrupt;
    };
    if record.state == State::Unknown {
        return RefreshOutcome::LeftUnknown;
    }
    if record.state == State::Dnd {
        return RefreshOutcome::LeftDnd;
    }
    match set_state_at(status_path, record.state, now) {
        Ok(()) => RefreshOutcome::Refreshed,
        Err(_) => RefreshOutcome::Error,
    }
}

/// Atomic write: a temp sibling + rename, so a concurrent reader sees either the old bytes or the new
/// bytes, never a partial file.
fn write_atomic(path: &Path, value: &str) -> anyhow::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let tmp = dir.join(tmp_name());
    fs::write(&tmp, value)?;
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
    format!(
        ".status.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as Dur;

    fn at(unix_ms: u64) -> SystemTime {
        UNIX_EPOCH + Dur::from_millis(unix_ms)
    }

    #[test]
    fn missing_is_offline() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read_state(&status_path(tmp.path())), State::Offline);
    }

    #[test]
    fn set_then_read_roundtrips_each_settable_state() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        let written_at = 1_786_741_730_761;
        for st in [
            State::Offline,
            State::Available,
            State::Busy,
            State::Away,
            State::Dnd,
        ] {
            set_state_at(&sp, st, at(written_at)).unwrap();
            assert_eq!(read_state_at(&sp, at(written_at + 1)), st);
            assert_eq!(
                fs::read_to_string(&sp).unwrap(),
                format!("{}\nupdated-at-unix-ms {written_at}\n", st.as_str())
            );
        }
    }

    #[test]
    fn timestamped_status_keeps_the_old_reader_first_line() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        set_state_at(&sp, State::Available, at(1_786_741_730_761)).unwrap();

        let raw = fs::read_to_string(&sp).unwrap();
        assert_eq!(raw.lines().next(), Some("available"));
        assert_eq!(
            State::parse_any(raw.lines().next().unwrap()),
            Some(State::Available)
        );
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

        fs::write(&sp, "available\nupdated-at-unix-ms nope\n").unwrap();
        assert_eq!(read_state(&sp), State::Offline);
    }

    #[test]
    fn legacy_bare_word_uses_mtime_during_a_mixed_fleet_upgrade() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        fs::write(&sp, "busy\n").unwrap();
        let old = SystemTime::now() - STATUS_STALE - Dur::from_secs(60);
        let f = fs::File::open(&sp).unwrap();
        f.set_modified(old).unwrap();
        assert_eq!(read_state(&sp), State::Unknown);
    }

    #[test]
    fn timestamped_content_controls_freshness_after_replication() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        let now = SystemTime::now();
        let old = SystemTime::now() - STATUS_STALE - Dur::from_secs(60);

        set_state_at(&sp, State::Available, now).unwrap();
        fs::File::open(&sp).unwrap().set_modified(old).unwrap();
        assert_eq!(read_state(&sp), State::Available);

        set_state_at(&sp, State::Busy, old).unwrap();
        fs::File::open(&sp)
            .unwrap()
            .set_modified(SystemTime::now())
            .unwrap();
        assert_eq!(read_state(&sp), State::Unknown);
    }

    #[test]
    fn refresh_preserves_value_and_changes_the_replicated_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        let first = at(1_786_741_000_000);
        let second = first + STATUS_REFRESH;
        set_state_at(&sp, State::Busy, first).unwrap();
        let before = fs::read_to_string(&sp).unwrap();

        assert_eq!(refresh_at(&sp, second), RefreshOutcome::Refreshed);

        let after = fs::read_to_string(&sp).unwrap();
        assert_ne!(after, before);
        assert_eq!(
            parse_record(&after).unwrap().updated_at_unix_ms,
            Some(1_786_741_300_000)
        );
        assert_eq!(read_state_at(&sp, second), State::Busy);
    }

    #[test]
    fn writer_and_replica_carry_the_same_fresh_timestamp_while_idle() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = tmp.path().join("writer-status");
        let replica = tmp.path().join("replica-status");
        let first = at(1_786_741_000_000);
        let second = first + STATUS_REFRESH;
        set_state_at(&writer, State::Available, first).unwrap();
        fs::write(&replica, fs::read(&writer).unwrap()).unwrap();

        assert_eq!(refresh_at(&writer, second), RefreshOutcome::Refreshed);
        fs::write(&replica, fs::read(&writer).unwrap()).unwrap();

        let writer_raw = fs::read_to_string(&writer).unwrap();
        let replica_raw = fs::read_to_string(&replica).unwrap();
        assert_eq!(replica_raw, writer_raw);
        assert_eq!(
            parse_record(&writer_raw).unwrap().updated_at_unix_ms,
            Some(1_786_741_300_000)
        );
        let observed = second + STATUS_REFRESH - Dur::from_millis(1);
        assert_eq!(read_state_at(&writer, observed), State::Available);
        assert_eq!(read_state_at(&replica, observed), State::Available);
    }

    #[test]
    fn refresh_missing_writes_available_default() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        let now = at(1_786_741_730_761);
        assert_eq!(refresh_at(&sp, now), RefreshOutcome::WroteDefault);
        assert_eq!(read_state_at(&sp, now), State::Available);
        assert_eq!(
            fs::read_to_string(&sp).unwrap(),
            "available\nupdated-at-unix-ms 1786741730761\n"
        );
    }

    #[test]
    fn refresh_leaves_dnd_to_age_out() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = status_path(tmp.path());
        let written_at = at(1_786_741_000_000);
        set_state_at(&sp, State::Dnd, written_at).unwrap();
        let before = fs::read_to_string(&sp).unwrap();

        assert_eq!(
            refresh_at(&sp, written_at + STATUS_REFRESH),
            RefreshOutcome::LeftDnd
        );
        assert_eq!(fs::read_to_string(&sp).unwrap(), before);
        assert_eq!(read_state_at(&sp, written_at), State::Dnd);
        assert_eq!(
            read_state_at(&sp, written_at + STATUS_STALE),
            State::Unknown
        );
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
