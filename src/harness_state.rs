//! Observed harness state: the driver-owned record of what a harness is seen doing.
//!
//! A `harness-state` file (sibling of `status` in the agent's dir) carries the latest observation a
//! session wrapper made of its provider: whether the harness is working, blocked on a human, or
//! ended, plus what its input buffer holds. This is the *observed* axis; `status` remains the
//! *declared* one, and neither speaks for the other. The record is written only by the driver
//! wrapper that owns the live session, on state transitions plus a slow heartbeat, and it follows
//! the presence record's transport discipline: an embedded origin timestamp (never file mtime),
//! atomic tmp+rename writes, byte-distinct content on every write, and a derived-only `unknown` —
//! a writer that loses sight of its harness stops heartbeating and lets the record age out rather
//! than refreshing a state it can no longer prove.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A valid observation at least this old reads as `unknown`. Deliberately its own constant rather
/// than an alias of [`crate::status::STATUS_STALE`]: retuning presence must not silently retune
/// observed harness state.
pub const HARNESS_STATE_STALE: Duration = Duration::from_secs(15 * 60);
/// How often a live writer re-stamps a record it still has evidence for — the presence cadence, so
/// wrappers piggyback on the wakeup they already own.
pub const HARNESS_STATE_REFRESH: Duration = Duration::from_secs(5 * 60);
/// Maximum accepted positive difference between the writer's UTC clock and the reader's clock.
pub const HARNESS_STATE_FUTURE_SKEW: Duration = Duration::from_secs(60);

const SCHEMA: &str = "st2.harness-state.v1";

/// What the harness is observed doing. `Child` is reserved: it is part of the contract so a v1
/// reader decodes it, but no producer emits it yet (the screen observer that would have was cut).
/// `Unknown` is DERIVED — staleness, malformation, or a dead session — and is never written; there
/// is no constructor path from missing evidence to `Idle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Activity {
    Idle,
    Active,
    Child,
    /// The session ended or reached a terminal error; nothing further will be observed from this
    /// incarnation without intervention. Unlike the live states, a fresh `Ended` survives the
    /// session-liveness cross-check: a terminal record is *supposed* to outlive its writer.
    Ended,
    #[serde(other)]
    Unknown,
}

impl Activity {
    pub fn as_str(self) -> &'static str {
        match self {
            Activity::Idle => "idle",
            Activity::Active => "active",
            Activity::Child => "child",
            Activity::Ended => "ended",
            Activity::Unknown => "unknown",
        }
    }
}

/// Who the harness is waiting on. `Human` means the model is stopped and a person is the thing
/// that restarts it (a permission prompt, a review, a question) — neither working nor merely idle.
/// Unrecognized future values decode as `Unknown` (indeterminate), never as `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BlockedOn {
    None,
    Human,
    #[serde(other)]
    Unknown,
}

impl BlockedOn {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockedOn::None => "none",
            BlockedOn::Human => "human",
            BlockedOn::Unknown => "unknown",
        }
    }
}

/// What the harness's composer holds. `Unknown` is writable on this axis: "I cannot see the
/// composer" is itself the observation most producers make.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InputBuffer {
    Empty,
    Nonempty,
    #[serde(other)]
    Unknown,
}

impl InputBuffer {
    pub fn as_str(self) -> &'static str {
        match self {
            InputBuffer::Empty => "empty",
            InputBuffer::Nonempty => "nonempty",
            InputBuffer::Unknown => "unknown",
        }
    }
}

/// The durable record. Additive-tolerant on read (no `deny_unknown_fields`): a reader pinned to an
/// older crate may be older than the writer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    schema: String,
    agent: String,
    harness: String,
    state: Activity,
    blocked_on: BlockedOn,
    input_buffer: InputBuffer,
    /// Diagnostic only. No consumer branches on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// `Ended` only: the exit outcome, e.g. `exit 0` or `signal 9`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit: Option<String>,
    /// The pty session whose liveness vouches for the live states. Same-host readers cross-check
    /// it; a record whose session is provably dead reads `unknown` even while fresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty_session: Option<String>,
    /// When the current state was entered. Survives heartbeat re-stamps.
    since_ms: u64,
    /// The heartbeat: when the writer last held evidence for this state.
    written_at_ms: u64,
    /// Monotonic transition counter. Keeps every write byte-distinct and leaves room for a
    /// compatible transition history later.
    transitions: u64,
}

/// The observed-state file: `<agent_dir>/harness-state`.
pub fn harness_state_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("harness-state")
}

/// One observation as a producer states it: everything except the derived pieces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub state: Activity,
    pub blocked_on: BlockedOn,
    pub input_buffer: InputBuffer,
    pub reason: Option<String>,
    pub exit: Option<String>,
}

impl Observation {
    pub fn new(state: Activity, blocked_on: BlockedOn, input_buffer: InputBuffer) -> Self {
        Self {
            state,
            blocked_on,
            input_buffer,
            reason: None,
            exit: None,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_exit(mut self, exit: impl Into<String>) -> Self {
        self.exit = Some(exit.into());
        self
    }
}

/// The writer a session wrapper owns. One writer per live session; it coalesces identical
/// observations, stamps transitions, and re-stamps the heartbeat on the presence cadence. The
/// caller's rule for indeterminacy: when evidence is lost (the observer no longer sees its
/// harness), call nothing — never heartbeat a state you cannot see, and never write `unknown`.
pub struct Writer {
    path: PathBuf,
    agent: String,
    harness: &'static str,
    pty_session: Option<String>,
    current: Option<Record>,
}

impl Writer {
    /// A writer continues the transition counter of any readable predecessor record so restarts
    /// keep writes byte-distinct; an unreadable predecessor starts the counter fresh.
    pub fn new(
        agent_dir: &Path,
        agent: impl Into<String>,
        harness: &'static str,
        pty_session: Option<String>,
    ) -> Self {
        let path = harness_state_path(agent_dir);
        let current = read_record(&path);
        Self {
            path,
            agent: agent.into(),
            harness,
            pty_session,
            current,
        }
    }

    /// Record an observation. Identical consecutive observations coalesce into a heartbeat
    /// re-stamp; a genuine change writes a new transition with a fresh `since`. `Unknown` state is
    /// derived and cannot be written.
    pub fn observe(&mut self, observation: Observation) -> anyhow::Result<()> {
        anyhow::ensure!(
            observation.state != Activity::Unknown,
            "unknown is derived and cannot be written"
        );
        let now_ms = crate::message::now_ms();
        let unchanged = self.current.as_ref().is_some_and(|current| {
            current.state == observation.state
                && current.blocked_on == observation.blocked_on
                && current.input_buffer == observation.input_buffer
                && current.reason == observation.reason
                && current.exit == observation.exit
        });
        let (since_ms, transitions) = match (&self.current, unchanged) {
            (Some(current), true) => (current.since_ms, current.transitions),
            (Some(current), false) => (now_ms, current.transitions.saturating_add(1)),
            (None, _) => (now_ms, 0),
        };
        let record = Record {
            schema: SCHEMA.to_string(),
            agent: self.agent.clone(),
            harness: self.harness.to_string(),
            state: observation.state,
            blocked_on: observation.blocked_on,
            input_buffer: observation.input_buffer,
            reason: observation.reason,
            exit: observation.exit,
            pty_session: self.pty_session.clone(),
            since_ms,
            written_at_ms: now_ms,
            transitions,
        };
        write_record(&self.path, &record)?;
        self.current = Some(record);
        Ok(())
    }

    /// Re-stamp the heartbeat for a state the writer still has evidence for. A writer that has not
    /// observed anything yet, or whose last write was terminal, has nothing to keep fresh.
    pub fn heartbeat(&mut self) -> anyhow::Result<()> {
        let Some(current) = self.current.as_mut() else {
            return Ok(());
        };
        if current.state == Activity::Ended {
            return Ok(());
        }
        current.written_at_ms = crate::message::now_ms();
        write_record(&self.path, current)
    }

    /// Write the terminal record for this session. Idempotent-shaped: callers on racing teardown
    /// paths may both call it.
    pub fn ended(&mut self, exit: impl Into<String>) -> anyhow::Result<()> {
        self.observe(
            Observation::new(Activity::Ended, BlockedOn::None, InputBuffer::Unknown)
                .with_exit(exit),
        )
    }
}

/// The derived view a consumer reads. `state` already folds in staleness, future skew,
/// malformation, and (when a probe is supplied) session liveness; `reason` names which derivation
/// produced an `unknown`, so no absence is silent.
#[derive(Debug, Clone, PartialEq)]
pub struct Observed {
    pub state: Activity,
    pub blocked_on: BlockedOn,
    pub input_buffer: InputBuffer,
    pub harness: Option<String>,
    pub since_ms: Option<u64>,
    pub exit: Option<String>,
    pub reason: Option<String>,
}

impl Observed {
    fn indeterminate(reason: &str, harness: Option<String>) -> Self {
        // The single constructor for an indeterminate observation: every absence routes here, so
        // no path can derive `idle` — or anything else — from missing evidence.
        Self {
            state: Activity::Unknown,
            blocked_on: BlockedOn::Unknown,
            input_buffer: InputBuffer::Unknown,
            harness,
            since_ms: None,
            exit: None,
            reason: Some(reason.to_string()),
        }
    }
}

/// Result of a same-host session-liveness probe. `Indeterminate` (an unreadable registry, e.g. a
/// reader without the session dir the writer used) must not downgrade anything: unprovable
/// evidence is never reported as death.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLiveness {
    Alive,
    Dead,
    Indeterminate,
}

/// Read an agent's observed harness state. `None` means no record exists — no driver has ever
/// observed this agent, which is different from `unknown`. `probe` is the optional same-host
/// liveness cross-check for the record's pty session; pass `None` for cross-host reads.
pub fn read(
    path: &Path,
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
) -> Option<Observed> {
    let raw = fs::read(path).ok()?;
    Some(read_raw_at(&raw, probe, crate::message::now_ms()))
}

fn read_raw_at(
    raw: &[u8],
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
    now_ms: u64,
) -> Observed {
    let Ok(record) = serde_json::from_slice::<Record>(raw) else {
        return Observed::indeterminate("malformed-record", None);
    };
    let harness = Some(record.harness.clone());
    if record.written_at_ms > now_ms {
        if record.written_at_ms - now_ms > duration_ms(HARNESS_STATE_FUTURE_SKEW) {
            return Observed::indeterminate("future-skew", harness);
        }
    } else if now_ms - record.written_at_ms >= duration_ms(HARNESS_STATE_STALE) {
        return Observed::indeterminate("stale", harness);
    }
    if record.state == Activity::Unknown {
        // A literal `unknown` is never written by this crate; treat one like malformation.
        return Observed::indeterminate("literal-unknown", harness);
    }
    if record.state != Activity::Ended
        && let (Some(probe), Some(session)) = (probe, record.pty_session.as_deref())
        && probe(session) == SessionLiveness::Dead
    {
        return Observed::indeterminate("session-dead", harness);
    }
    Observed {
        state: record.state,
        blocked_on: record.blocked_on,
        input_buffer: record.input_buffer,
        harness,
        since_ms: Some(record.since_ms),
        exit: record.exit,
        reason: record.reason,
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn read_record(path: &Path) -> Option<Record> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn write_record(path: &Path, record: &Record) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let tmp = dir.join(tmp_name());
    fs::write(&tmp, &bytes)?;
    // rename over the target — atomic on the same filesystem.
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp); // best-effort cleanup
        return Err(e.into());
    }
    Ok(())
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_name() -> String {
    format!(
        ".harness-state.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(dir: &Path) -> Writer {
        Writer::new(dir, "hetz.worker", "codex", Some("worker".to_string()))
    }

    fn active() -> Observation {
        Observation::new(Activity::Active, BlockedOn::None, InputBuffer::Unknown)
    }

    #[test]
    fn missing_record_reads_as_none_not_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read(&harness_state_path(tmp.path()), None), None);
    }

    #[test]
    fn observe_then_read_roundtrips_every_writable_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = writer(tmp.path());
        for (state, blocked, buffer) in [
            (Activity::Idle, BlockedOn::None, InputBuffer::Empty),
            (Activity::Active, BlockedOn::Human, InputBuffer::Unknown),
            (Activity::Child, BlockedOn::None, InputBuffer::Nonempty),
            (Activity::Ended, BlockedOn::None, InputBuffer::Unknown),
        ] {
            writer
                .observe(Observation::new(state, blocked, buffer))
                .unwrap();
            let observed = read(&harness_state_path(tmp.path()), None).unwrap();
            assert_eq!(observed.state, state);
            assert_eq!(observed.blocked_on, blocked);
            assert_eq!(observed.input_buffer, buffer);
            assert_eq!(observed.harness.as_deref(), Some("codex"));
            assert!(observed.since_ms.is_some());
        }
    }

    #[test]
    fn unknown_state_is_derived_and_cannot_be_written() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = writer(tmp.path());
        assert!(writer
            .observe(Observation::new(
                Activity::Unknown,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .is_err());
        assert_eq!(read(&harness_state_path(tmp.path()), None), None);
    }

    #[test]
    fn every_write_is_byte_distinct() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let first = fs::read(&path).unwrap();

        // A coalesced identical observation still re-stamps the heartbeat…
        std::thread::sleep(Duration::from_millis(2));
        writer.observe(active()).unwrap();
        let second = fs::read(&path).unwrap();
        assert_ne!(first, second, "identical observation must re-stamp bytes");

        // …and an explicit heartbeat does the same.
        std::thread::sleep(Duration::from_millis(2));
        writer.heartbeat().unwrap();
        let third = fs::read(&path).unwrap();
        assert_ne!(second, third, "heartbeat must change bytes");
    }

    #[test]
    fn identical_observations_coalesce_without_a_new_transition() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let entered: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        writer.observe(active()).unwrap();
        let restated: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restated.since_ms, entered.since_ms, "since survives restating");
        assert_eq!(restated.transitions, entered.transitions);

        writer
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        let transitioned: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(transitioned.transitions, entered.transitions + 1);
        assert!(transitioned.since_ms >= restated.since_ms);
    }

    #[test]
    fn restart_continues_the_transition_counter() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut first = writer(tmp.path());
        first.observe(active()).unwrap();
        first
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        drop(first);

        let mut second = writer(tmp.path());
        second.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.transitions, 2);
    }

    #[test]
    fn staleness_and_future_skew_derive_unknown_with_distinct_reasons() {
        let now_ms = 2_000_000_000_u64;
        let stale_ms = duration_ms(HARNESS_STATE_STALE);
        let skew_ms = duration_ms(HARNESS_STATE_FUTURE_SKEW);
        let raw = |written_at_ms: u64| {
            serde_json::to_vec(&Record {
                schema: SCHEMA.to_string(),
                agent: "hetz.worker".to_string(),
                harness: "codex".to_string(),
                state: Activity::Active,
                blocked_on: BlockedOn::None,
                input_buffer: InputBuffer::Unknown,
                reason: None,
                exit: None,
                pty_session: None,
                since_ms: written_at_ms,
                written_at_ms,
                transitions: 0,
            })
            .unwrap()
        };

        let fresh = read_raw_at(&raw(now_ms - stale_ms + 1), None, now_ms);
        assert_eq!(fresh.state, Activity::Active);

        let stale = read_raw_at(&raw(now_ms - stale_ms), None, now_ms);
        assert_eq!(stale.state, Activity::Unknown);
        assert_eq!(stale.reason.as_deref(), Some("stale"));
        assert_eq!(stale.blocked_on, BlockedOn::Unknown);

        let bounded_future = read_raw_at(&raw(now_ms + skew_ms), None, now_ms);
        assert_eq!(bounded_future.state, Activity::Active);

        let excessive_future = read_raw_at(&raw(now_ms + skew_ms + 1), None, now_ms);
        assert_eq!(excessive_future.state, Activity::Unknown);
        assert_eq!(excessive_future.reason.as_deref(), Some("future-skew"));
    }

    #[test]
    fn malformed_record_is_unknown_without_mtime_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        for raw in [&b"garbage"[..], b"{}", b"{\"schema\":\"st2.harness-state.v1\"}"] {
            fs::write(&path, raw).unwrap();
            let observed = read(&path, None).unwrap();
            assert_eq!(observed.state, Activity::Unknown);
            assert_eq!(observed.reason.as_deref(), Some("malformed-record"));
        }
    }

    #[test]
    fn a_dead_session_reads_unknown_even_while_fresh_but_ended_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        let dead: &dyn Fn(&str) -> SessionLiveness = &|_| SessionLiveness::Dead;
        let indeterminate: &dyn Fn(&str) -> SessionLiveness = &|_| SessionLiveness::Indeterminate;

        writer.observe(active()).unwrap();
        let observed = read(&path, Some(dead)).unwrap();
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("session-dead"));
        // An unreadable registry proves nothing and downgrades nothing.
        assert_eq!(read(&path, Some(indeterminate)).unwrap().state, Activity::Active);

        writer.ended("signal 9").unwrap();
        let observed = read(&path, Some(dead)).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("signal 9"));
    }

    #[test]
    fn heartbeat_re_stamps_only_live_states_and_never_resurrects_ended() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        // Nothing observed yet: heartbeat is a no-op, not a default write.
        writer.heartbeat().unwrap();
        assert_eq!(read(&path, None), None);

        writer.ended("exit 0").unwrap();
        let terminal = fs::read(&path).unwrap();
        writer.heartbeat().unwrap();
        assert_eq!(fs::read(&path).unwrap(), terminal, "ended is never re-stamped");
    }

    #[test]
    fn future_vocabulary_degrades_to_indeterminate_not_none() {
        // A v2 writer's new words must not decode as anything definite in a v1 reader.
        let raw = br#"{"schema":"st2.harness-state.v2","agent":"hetz.worker","harness":"codex","state":"hibernating","blockedOn":"robot","inputBuffer":"overflowing","sinceMs":1,"writtenAtMs":9999999999999,"transitions":3,"novelField":true}"#;
        let observed = read_raw_at(raw, None, 9_999_999_999_999);
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("literal-unknown"));

        // And on a fresh record with a known state, unknown axis words stay indeterminate.
        let raw = br#"{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"robot","inputBuffer":"overflowing","sinceMs":1,"writtenAtMs":9999999999999,"transitions":3}"#;
        let observed = read_raw_at(raw, None, 9_999_999_999_999);
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(observed.blocked_on, BlockedOn::Unknown);
        assert_eq!(observed.input_buffer, InputBuffer::Unknown);
    }
}
