use std::io::{BufRead as _, BufReader, Read as _};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;

const MAX_EVENT_BYTES: usize = 64 * 1024;
const MAX_LEASE: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub(super) enum AdapterSignal {
    Line(String),
    Error(String),
    Eof,
}

/// One explicitly configured, long-running generic activity adapter.
///
/// Its argv is launched directly. Core introduces no shell, arguments, or environment. Provider-native
/// interpretation stays in that external process; core consumes only its bounded JSONL stdout.
pub(super) struct ExternalAdapter {
    child: Child,
    signals: Receiver<AdapterSignal>,
}

impl ExternalAdapter {
    pub(super) fn spawn(argv: &[String], wake: Sender<()>) -> anyhow::Result<Self> {
        let (program, arguments) = argv
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("DING adapter argv cannot be empty"))?;
        let mut child = Command::new(program)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| anyhow::anyhow!("starting DING adapter `{program}`: {error}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("DING adapter stdout was not piped"))?;
        let (tx, signals) = channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut bytes = Vec::new();
                match (&mut reader)
                    .take((MAX_EVENT_BYTES + 1) as u64)
                    .read_until(b'\n', &mut bytes)
                {
                    Ok(0) => {
                        let _ = tx.send(AdapterSignal::Eof);
                        let _ = wake.send(());
                        break;
                    }
                    Ok(_) if bytes.len() > MAX_EVENT_BYTES => {
                        let _ = tx.send(AdapterSignal::Error(format!(
                            "adapter event exceeds {MAX_EVENT_BYTES} bytes"
                        )));
                        let _ = wake.send(());
                        break;
                    }
                    Ok(_) => {
                        while matches!(bytes.last(), Some(b'\n' | b'\r')) {
                            bytes.pop();
                        }
                        match String::from_utf8(bytes) {
                            Ok(line) if !line.is_empty() => {
                                let _ = tx.send(AdapterSignal::Line(line));
                            }
                            Ok(_) => {
                                let _ = tx.send(AdapterSignal::Error(
                                    "adapter emitted an empty event".to_string(),
                                ));
                            }
                            Err(error) => {
                                let _ = tx.send(AdapterSignal::Error(format!(
                                    "adapter event is not UTF-8: {error}"
                                )));
                            }
                        }
                        let _ = wake.send(());
                    }
                    Err(error) => {
                        let _ = tx.send(AdapterSignal::Error(format!(
                            "reading adapter stdout: {error}"
                        )));
                        let _ = wake.send(());
                        break;
                    }
                }
            }
        });
        Ok(Self { child, signals })
    }

    pub(super) fn try_recv(&self) -> Option<AdapterSignal> {
        self.signals.try_recv().ok()
    }
}

impl Drop for ExternalAdapter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ActivityState {
    Idle,
    Active,
    Child,
    Unknown,
}

impl ActivityState {
    pub(super) fn pty_name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active => "active",
            Self::Child => "child_command",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum InputBufferState {
    Empty,
    Nonempty,
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityEvent {
    v: u8,
    kind: String,
    session: String,
    incarnation: String,
    generation: String,
    sequence: u64,
    state: ActivityState,
    #[serde(rename = "inputBuffer")]
    input_buffer: InputBufferState,
    #[serde(rename = "validForMs")]
    valid_for_ms: u64,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct IdleLease {
    pub(super) session: String,
    pub(super) incarnation: String,
    pub(super) generation: String,
    pub(super) sequence: u64,
    pub(super) state: ActivityState,
    expires_at: Instant,
}

impl IdleLease {
    pub(super) fn is_fresh(&self, now: Instant) -> bool {
        now < self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldReason {
    AdapterUnavailable,
    AdapterError,
    TupleChanged,
    Active,
    Child,
    Unknown,
    InputNonempty,
    InputUnknown,
    Stale,
}

impl HoldReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::AdapterUnavailable => "adapter-unavailable",
            Self::AdapterError => "adapter-error",
            Self::TupleChanged => "adapter-tuple-changed",
            Self::Active => "activity-active",
            Self::Child => "activity-child",
            Self::Unknown => "activity-unknown",
            Self::InputNonempty => "input-buffer-nonempty",
            Self::InputUnknown => "input-buffer-unknown",
            Self::Stale => "activity-stale",
        }
    }
}

/// Current fail-closed adapter evidence. The first event anchors an incarnation/generation tuple.
/// A tuple change is observed but cannot authorize delivery until a later event confirms it.
#[derive(Debug)]
pub(super) struct AdapterState {
    session: String,
    last_sequence: Option<u64>,
    tuple: Option<(String, String)>,
    last_state: Option<ActivityState>,
    last_input_buffer: Option<InputBufferState>,
    lease: Option<IdleLease>,
    faulted: bool,
    tuple_changed: bool,
}

impl AdapterState {
    pub(super) fn new(session: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            last_sequence: None,
            tuple: None,
            last_state: None,
            last_input_buffer: None,
            lease: None,
            faulted: false,
            tuple_changed: false,
        }
    }

    pub(super) fn apply_line(&mut self, line: &str, received_at: Instant) -> anyhow::Result<()> {
        let event: ActivityEvent = match serde_json::from_str(line) {
            Ok(event) => event,
            Err(error) => {
                return self.fail(format!("invalid adapter JSONL event: {error}"));
            }
        };
        self.apply(event, received_at)
    }

    fn apply(&mut self, event: ActivityEvent, received_at: Instant) -> anyhow::Result<()> {
        if event.v != 1 {
            return self.fail(format!("unsupported adapter protocol version {}", event.v));
        }
        if event.kind != "activity" {
            return self.fail(format!("unsupported adapter event kind `{}`", event.kind));
        }
        if event.session != self.session {
            return self.fail(format!(
                "adapter session mismatch: expected `{}`, got `{}`",
                self.session, event.session
            ));
        }
        if event.incarnation.is_empty() || event.generation.is_empty() {
            return self.fail("adapter incarnation and generation must be non-empty".to_string());
        }
        let next_tuple = (event.incarnation.clone(), event.generation.clone());
        let tuple_changed = self
            .tuple
            .as_ref()
            .is_some_and(|current| current != &next_tuple);
        if event.sequence == 0
            || (!tuple_changed
                && self
                    .last_sequence
                    .is_some_and(|previous| event.sequence <= previous))
        {
            return self.fail(format!(
                "adapter sequence {} is not strictly newer than {:?}",
                event.sequence, self.last_sequence
            ));
        }
        if event.valid_for_ms == 0 {
            return self.fail("adapter validForMs must be greater than zero".to_string());
        }

        let _opaque_reason = event.reason.as_deref();
        self.last_sequence = Some(event.sequence);
        self.faulted = false;
        self.last_state = Some(event.state);
        self.last_input_buffer = Some(event.input_buffer);
        if tuple_changed {
            self.tuple = Some(next_tuple);
            self.lease = None;
            self.tuple_changed = true;
            return Ok(());
        }
        self.tuple = Some(next_tuple);
        self.tuple_changed = false;
        self.lease = None;
        if event.state == ActivityState::Idle && event.input_buffer == InputBufferState::Empty {
            let duration = Duration::from_millis(event.valid_for_ms).min(MAX_LEASE);
            self.lease = Some(IdleLease {
                session: event.session,
                incarnation: event.incarnation,
                generation: event.generation,
                sequence: event.sequence,
                state: event.state,
                expires_at: received_at + duration,
            });
        }
        Ok(())
    }

    fn fail(&mut self, message: String) -> anyhow::Result<()> {
        self.invalidate();
        anyhow::bail!(message)
    }

    pub(super) fn invalidate(&mut self) {
        self.lease = None;
        self.faulted = true;
    }

    pub(super) fn authority(&self, now: Instant) -> Result<&IdleLease, HoldReason> {
        if self.faulted {
            return Err(HoldReason::AdapterError);
        }
        if self.tuple_changed {
            return Err(HoldReason::TupleChanged);
        }
        if let Some(lease) = self.lease.as_ref() {
            return lease
                .is_fresh(now)
                .then_some(lease)
                .ok_or(HoldReason::Stale);
        }
        match (self.last_state, self.last_input_buffer) {
            (None, _) => Err(HoldReason::AdapterUnavailable),
            (Some(ActivityState::Active), _) => Err(HoldReason::Active),
            (Some(ActivityState::Child), _) => Err(HoldReason::Child),
            (Some(ActivityState::Unknown), _) => Err(HoldReason::Unknown),
            (Some(ActivityState::Idle), Some(InputBufferState::Nonempty)) => {
                Err(HoldReason::InputNonempty)
            }
            (Some(ActivityState::Idle), Some(InputBufferState::Unknown)) => {
                Err(HoldReason::InputUnknown)
            }
            (Some(ActivityState::Idle), Some(InputBufferState::Empty)) => Err(HoldReason::Stale),
            (Some(ActivityState::Idle), None) => Err(HoldReason::InputUnknown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    fn event(sequence: u64, state: &str, input_buffer: &str, valid_for_ms: u64) -> String {
        format!(
            r#"{{"v":1,"kind":"activity","session":"host.agent","incarnation":"epoch-a","generation":"generation-a","sequence":{sequence},"state":"{state}","inputBuffer":"{input_buffer}","validForMs":{valid_for_ms},"reason":"opaque"}}"#
        )
    }

    #[test]
    fn only_fresh_idle_and_empty_authorizes() {
        let now = Instant::now();
        let mut state = AdapterState::new("host.agent");
        state
            .apply_line(&event(1, "idle", "empty", 250), now)
            .unwrap();
        let lease = state.authority(now).unwrap();
        assert_eq!(lease.incarnation, "epoch-a");
        assert_eq!(lease.generation, "generation-a");
        assert_eq!(lease.sequence, 1);
        assert!(matches!(
            state.authority(now + Duration::from_millis(250)),
            Err(HoldReason::Stale)
        ));
    }

    #[test]
    fn every_non_idle_or_nonempty_input_state_holds() {
        for (sequence, activity, input, expected) in [
            (1, "active", "empty", HoldReason::Active),
            (2, "child", "empty", HoldReason::Child),
            (3, "unknown", "empty", HoldReason::Unknown),
            (4, "idle", "nonempty", HoldReason::InputNonempty),
            (5, "idle", "unknown", HoldReason::InputUnknown),
        ] {
            let mut state = AdapterState::new("host.agent");
            state
                .apply_line(&event(sequence, activity, input, 250), Instant::now())
                .unwrap();
            assert!(matches!(state.authority(Instant::now()), Err(reason) if reason == expected));
        }
    }

    #[test]
    fn malformed_identity_sequence_and_tuple_changes_fail_closed() {
        let now = Instant::now();
        let mut state = AdapterState::new("host.agent");
        state
            .apply_line(&event(1, "idle", "empty", 250), now)
            .unwrap();
        assert!(
            state
                .apply_line(&event(1, "idle", "empty", 250), now)
                .is_err()
        );
        assert!(matches!(
            state.authority(now),
            Err(HoldReason::AdapterError)
        ));

        let wrong = event(2, "idle", "empty", 250).replace("host.agent", "other.agent");
        assert!(state.apply_line(&wrong, now).is_err());

        state
            .apply_line(&event(3, "idle", "empty", 250), now)
            .unwrap();
        let changed = event(1, "idle", "empty", 250)
            .replace("epoch-a", "epoch-b")
            .replace("generation-a", "generation-b");
        state.apply_line(&changed, now).unwrap();
        assert!(matches!(
            state.authority(now),
            Err(HoldReason::TupleChanged)
        ));
        let confirmed = event(2, "idle", "empty", 250)
            .replace("epoch-a", "epoch-b")
            .replace("generation-a", "generation-b");
        state.apply_line(&confirmed, now).unwrap();
        assert!(state.authority(now).is_ok());
    }

    #[test]
    fn unknown_fields_and_event_kinds_are_rejected() {
        let mut state = AdapterState::new("host.agent");
        let extra = event(1, "idle", "empty", 250).replace(
            r#","reason":"opaque"}"#,
            r#","reason":"opaque","provider":"forbidden"}"#,
        );
        assert!(state.apply_line(&extra, Instant::now()).is_err());
        let other = event(2, "idle", "empty", 250).replace("activity", "turn-boundary");
        assert!(state.apply_line(&other, Instant::now()).is_err());
    }

    #[test]
    fn external_adapter_uses_exact_direct_argv_and_wakes_on_jsonl() {
        let payload = event(1, "idle", "empty", 250);
        let (wake_tx, wake_rx) = channel();
        let adapter = ExternalAdapter::spawn(
            &[
                "sh".to_string(),
                "-c".to_string(),
                "printf '%s\\n' \"$1\"".to_string(),
                "sh".to_string(),
                payload.clone(),
            ],
            wake_tx,
        )
        .unwrap();
        wake_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let signal = loop {
            if let Some(signal) = adapter.try_recv() {
                break signal;
            }
            thread::yield_now();
        };
        assert!(matches!(signal, AdapterSignal::Line(line) if line == payload));
        assert!(ExternalAdapter::spawn(&[], channel::<()>().0).is_err());
    }
}
