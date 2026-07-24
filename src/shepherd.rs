//! Scheduled machine-root health sweeps.
//!
//! A shepherd pass is deliberately separate from inbox delivery: every cadence bucket, the one
//! active local Codex `role=root` is poked with a local health-sweep prompt. The state machine below
//! owns the complete decision, durable backoff, delivery latch, and fault-reporting lifecycle. Its
//! clock, persistent store, poker, and reporter are injected so the same production pass used by
//! `up --once` and the supervisor loop can be driven across multiple passes and process restarts.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::ding::Poker as DingPoker;
use crate::spec::AgentSpec;

pub(crate) const SHEPHERD_PROMPT: &str = "[ST2 LOCAL TICK] Run the scheduled local machine-root \
health sweep: inspect catalog/service/fabric/PTY state, safe drift, and report incidents. This is \
not an inbox event and must not poll the inbox.";

const CADENCE_SECS: u64 = 2 * 60 * 60;
const ATTEMPT_BACKOFF_SECS: u64 = 5 * 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ShepherdKey {
    host: String,
    root: String,
}

impl ShepherdKey {
    fn new(host: impl Into<String>, root: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            root: root.into(),
        }
    }

    fn bus_id(&self) -> String {
        format!("{}.{}", self.host, self.root)
    }
}

impl fmt::Display for ShepherdKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.host, self.root)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PersistedState {
    attempt_at: Option<u64>,
    delivered_bucket: Option<u64>,
}

pub(crate) trait Clock {
    fn now_secs(&self) -> u64;
}

pub(crate) trait Store {
    fn is_dnd(&mut self, agent_dir: &Path) -> bool;
    fn load(&mut self, key: &ShepherdKey) -> anyhow::Result<PersistedState>;
    fn save(&mut self, key: &ShepherdKey, state: PersistedState) -> anyhow::Result<()>;
}

pub(crate) trait Poker {
    fn poke(&mut self, session: &str, text: &str) -> anyhow::Result<()>;
}

pub(crate) trait Reporter {
    fn report(&mut self, fault: &Fault);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fault {
    MissingRoot { host: String },
    DuplicateRoots { host: String, roots: Vec<String> },
    NonCodexRoot { key: ShepherdKey },
    StateRead { key: ShepherdKey, error: String },
    AttemptWrite { key: ShepherdKey, error: String },
    Poke { key: ShepherdKey, error: String },
    DeliveredWrite { key: ShepherdKey, error: String },
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRoot { host } => {
                write!(f, "disabled on {host}: no active local role=root")
            }
            Self::DuplicateRoots { host, roots } => write!(
                f,
                "disabled on {host}: expected one active local role=root, found {} ({})",
                roots.len(),
                roots.join(", ")
            ),
            Self::NonCodexRoot { key } => {
                write!(
                    f,
                    "disabled for {key}: local role=root is not an exact Codex command"
                )
            }
            Self::StateRead { key, error } => {
                write!(f, "cannot read durable state for {key}: {error}")
            }
            Self::AttemptWrite { key, error } => {
                write!(
                    f,
                    "cannot durably record attempt for {key}; poke suppressed: {error}"
                )
            }
            Self::Poke { key, error } => write!(f, "poke failed for {key}: {error}"),
            Self::DeliveredWrite { key, error } => write!(
                f,
                "poke reached {key}, but delivered state could not be persisted: {error}"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Dnd,
    Delivered,
    Backoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Delivered {
        key: ShepherdKey,
        bucket: u64,
    },
    Skipped {
        key: ShepherdKey,
        reason: SkipReason,
    },
    Fault(Fault),
}

/// Process-local state that must survive every pass of a supervisor loop.
///
/// `delivered` closes the duplicate-poke hole when a successful poke is followed by a failed
/// delivered-state write. `last_fault` reports a stable fault once, clears after a non-fault outcome,
/// and reports again only after the observed state has changed.
#[derive(Debug, Default)]
pub(crate) struct Runtime {
    delivered: HashSet<(ShepherdKey, u64)>,
    last_fault: Option<Fault>,
}

impl Runtime {
    fn observe(&mut self, outcome: &Outcome, reporter: &mut dyn Reporter) {
        match outcome {
            Outcome::Fault(fault) => {
                if self.last_fault.as_ref() != Some(fault) {
                    reporter.report(fault);
                }
                self.last_fault = Some(fault.clone());
            }
            Outcome::Delivered { .. } | Outcome::Skipped { .. } => {
                self.last_fault = None;
            }
        }
    }
}

#[derive(Debug)]
struct Target {
    key: ShepherdKey,
    agent_dir: PathBuf,
}

/// Select the shepherd target without reading time, status, or filesystem state.
///
/// Cardinality is evaluated before harness classification so an accidental second machine root is
/// always surfaced, even if only one of the two happens to invoke Codex.
fn select_target(specs: &[AgentSpec], this_host: &str) -> Result<Target, Fault> {
    let mut roots: Vec<&AgentSpec> = specs
        .iter()
        .filter(|spec| {
            !spec.retired
                && spec.resolved_host(this_host) == this_host
                && spec.role.as_deref() == Some("root")
        })
        .collect();
    roots.sort_by(|left, right| left.identity.cmp(&right.identity));

    match roots.as_slice() {
        [] => Err(Fault::MissingRoot {
            host: this_host.to_string(),
        }),
        [spec] => {
            if !spec
                .tasks
                .iter()
                .filter(|task| task.name == "agent")
                .filter_map(|task| task.command.as_deref())
                .any(command_invokes_codex)
            {
                return Err(Fault::NonCodexRoot {
                    key: ShepherdKey::new(this_host, &spec.identity),
                });
            }
            Ok(Target {
                key: ShepherdKey::new(this_host, &spec.identity),
                agent_dir: spec
                    .path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf(),
            })
        }
        _ => Err(Fault::DuplicateRoots {
            host: this_host.to_string(),
            roots: roots.iter().map(|spec| spec.identity.clone()).collect(),
        }),
    }
}

/// Run one complete shepherd state transition.
///
/// The ordering is the safety contract: load state, honor delivery/backoff, durably save the attempt,
/// poke, latch the successful delivery in memory, then durably save the delivered bucket.
pub(crate) fn run_pass(
    specs: &[AgentSpec],
    this_host: &str,
    runtime: &mut Runtime,
    clock: &dyn Clock,
    store: &mut dyn Store,
    poker: &mut dyn Poker,
    reporter: &mut dyn Reporter,
) -> Outcome {
    let now = clock.now_secs();
    let bucket = now / CADENCE_SECS;
    runtime
        .delivered
        .retain(|(_, delivered_bucket)| *delivered_bucket == bucket);

    let outcome = (|| {
        let target = select_target(specs, this_host)?;
        let key = target.key;

        if store.is_dnd(&target.agent_dir) {
            return Ok(Outcome::Skipped {
                key,
                reason: SkipReason::Dnd,
            });
        }
        if runtime.delivered.contains(&(key.clone(), bucket)) {
            return Ok(Outcome::Skipped {
                key,
                reason: SkipReason::Delivered,
            });
        }

        let mut state = store.load(&key).map_err(|error| Fault::StateRead {
            key: key.clone(),
            error: error.to_string(),
        })?;
        if state.delivered_bucket == Some(bucket) {
            return Ok(Outcome::Skipped {
                key,
                reason: SkipReason::Delivered,
            });
        }
        if state
            .attempt_at
            .is_some_and(|attempt| now.saturating_sub(attempt) < ATTEMPT_BACKOFF_SECS)
        {
            return Ok(Outcome::Skipped {
                key,
                reason: SkipReason::Backoff,
            });
        }

        state.attempt_at = Some(now);
        store
            .save(&key, state)
            .map_err(|error| Fault::AttemptWrite {
                key: key.clone(),
                error: error.to_string(),
            })?;

        poker
            .poke(&key.bus_id(), SHEPHERD_PROMPT)
            .map_err(|error| Fault::Poke {
                key: key.clone(),
                error: error.to_string(),
            })?;

        // Latch before the second durable write: a successful poke must never be duplicated by this
        // runtime merely because persisting its delivery failed.
        runtime.delivered.insert((key.clone(), bucket));
        state.delivered_bucket = Some(bucket);
        store
            .save(&key, state)
            .map_err(|error| Fault::DeliveredWrite {
                key: key.clone(),
                error: error.to_string(),
            })?;

        Ok(Outcome::Delivered { key, bucket })
    })()
    .unwrap_or_else(Outcome::Fault);

    runtime.observe(&outcome, reporter);
    outcome
}

/// Recognize the exact command shape st2's renderers emit, while accepting an absolute Codex binary
/// path in an operator-authored spec. Arbitrary shell pipelines and incidental arguments are not
/// guessed through.
pub(crate) fn command_invokes_codex(command: &str) -> bool {
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
        .is_some_and(|name| name == "codex")
}

pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub(crate) struct FilesystemStore {
    state_dir: PathBuf,
}

impl FilesystemStore {
    pub(crate) fn new(catalog_root: &Path) -> Self {
        Self {
            state_dir: catalog_root.join(".st2-shepherd-state"),
        }
    }

    fn path(&self, key: &ShepherdKey) -> PathBuf {
        self.state_dir
            .join(encode_component(&key.host))
            .join(format!("{}.json", encode_component(&key.root)))
    }
}

impl Store for FilesystemStore {
    fn is_dnd(&mut self, agent_dir: &Path) -> bool {
        crate::status::read_state(&crate::status::status_path(agent_dir))
            == crate::status::State::Dnd
    }

    fn load(&mut self, key: &ShepherdKey) -> anyhow::Result<PersistedState> {
        let path = self.path(key);
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PersistedState::default());
            }
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))
    }

    fn save(&mut self, key: &ShepherdKey, state: PersistedState) -> anyhow::Result<()> {
        let path = self.path(key);
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
        let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
        tmp_name.push(format!(
            ".tmp-{}-{}",
            std::process::id(),
            SHEPHERD_TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let tmp = parent.join(tmp_name);
        let raw = serde_json::to_vec(&state).context("serializing shepherd state")?;
        std::fs::write(&tmp, raw).with_context(|| format!("writing {}", tmp.display()))?;
        if let Err(error) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(error)
                .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()));
        }
        Ok(())
    }
}

static SHEPHERD_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Encode one untrusted identity component into a single path component without collisions or path
/// traversal. `%` itself is encoded, so the representation is reversible.
fn encode_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

pub(crate) struct PtyPoker;

impl Poker for PtyPoker {
    fn poke(&mut self, session: &str, text: &str) -> anyhow::Result<()> {
        crate::ding::PtyPoker::new(session).poke(text)
    }
}

pub(crate) struct StderrReporter;

impl Reporter for StderrReporter {
    fn report(&mut self, fault: &Fault) {
        eprintln!("st2: shepherd {fault}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, HashMap};
    use std::rc::Rc;

    use crate::spec::{JobType, Task, TaskKind};

    struct FakeClock(u64);

    impl Clock for FakeClock {
        fn now_secs(&self) -> u64 {
            self.0
        }
    }

    #[derive(Default)]
    struct FakeStore {
        states: HashMap<ShepherdKey, PersistedState>,
        dnd: bool,
        fail_attempt_writes: usize,
        fail_delivered_writes: usize,
        events: Rc<RefCell<Vec<String>>>,
    }

    impl Store for FakeStore {
        fn is_dnd(&mut self, _agent_dir: &Path) -> bool {
            self.dnd
        }

        fn load(&mut self, key: &ShepherdKey) -> anyhow::Result<PersistedState> {
            Ok(self.states.get(key).copied().unwrap_or_default())
        }

        fn save(&mut self, key: &ShepherdKey, state: PersistedState) -> anyhow::Result<()> {
            if state.delivered_bucket.is_some() {
                self.events.borrow_mut().push("save-delivered".into());
                if self.fail_delivered_writes > 0 {
                    self.fail_delivered_writes -= 1;
                    anyhow::bail!("delivered write fault");
                }
            } else {
                self.events.borrow_mut().push("save-attempt".into());
                if self.fail_attempt_writes > 0 {
                    self.fail_attempt_writes -= 1;
                    anyhow::bail!("attempt write fault");
                }
            }
            self.states.insert(key.clone(), state);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakePoker {
        calls: Vec<String>,
        failures: usize,
        events: Rc<RefCell<Vec<String>>>,
    }

    impl Poker for FakePoker {
        fn poke(&mut self, session: &str, text: &str) -> anyhow::Result<()> {
            self.events.borrow_mut().push("poke".into());
            self.calls.push(format!("{session}: {text}"));
            if self.failures > 0 {
                self.failures -= 1;
                anyhow::bail!("poke fault");
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeReporter(Vec<Fault>);

    impl Reporter for FakeReporter {
        fn report(&mut self, fault: &Fault) {
            self.0.push(fault.clone());
        }
    }

    fn root(identity: &str, command: &str) -> AgentSpec {
        AgentSpec {
            identity: identity.into(),
            host: Some("node".into()),
            role: Some("root".into()),
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            retired: false,
            keep: false,
            restart: None,
            tasks: vec![Task {
                kind: TaskKind::Pty,
                name: "agent".into(),
                id: None,
                command: Some(command.into()),
                cwd: None,
                tags: BTreeMap::new(),
                env: BTreeMap::new(),
                keep: false,
            }],
            path: PathBuf::from(format!("/catalog/node/{identity}/agent.kdl")),
        }
    }

    fn pass(
        specs: &[AgentSpec],
        now: u64,
        runtime: &mut Runtime,
        store: &mut FakeStore,
        poker: &mut FakePoker,
        reporter: &mut FakeReporter,
    ) -> Outcome {
        run_pass(
            specs,
            "node",
            runtime,
            &FakeClock(now),
            store,
            poker,
            reporter,
        )
    }

    #[test]
    fn shepherd_pass_delivers_exactly_once_and_dedupes_repeat_passes() {
        let specs = [root("root", "exec codex --model gpt-5")];
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut store = FakeStore {
            events: Rc::clone(&events),
            ..Default::default()
        };
        let mut poker = FakePoker {
            events: Rc::clone(&events),
            ..Default::default()
        };
        let mut runtime = Runtime::default();
        let mut reporter = FakeReporter::default();

        assert!(matches!(
            pass(
                &specs,
                CADENCE_SECS * 7,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { bucket: 7, .. }
        ));
        assert_eq!(
            events.borrow().as_slice(),
            ["save-attempt", "poke", "save-delivered"],
            "the attempt must be durable before the poke"
        );

        assert!(matches!(
            pass(
                &specs,
                CADENCE_SECS * 7 + ATTEMPT_BACKOFF_SECS + 1,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Delivered,
                ..
            }
        ));
        assert_eq!(poker.calls.len(), 1);
        assert!(reporter.0.is_empty());

        assert!(matches!(
            pass(
                &specs,
                CADENCE_SECS * 8,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { bucket: 8, .. }
        ));
        assert_eq!(poker.calls.len(), 2, "the next cadence bucket is due");
    }

    #[test]
    fn shepherd_fresh_runtime_uses_persisted_delivery_and_identity_replacement_is_due() {
        let old = [root("old-root", "/opt/bin/codex --model gpt-5")];
        let replacement = [root("new-root", "codex --model gpt-5")];
        let now = CADENCE_SECS * 9;
        let mut store = FakeStore::default();
        let mut poker = FakePoker::default();
        let mut reporter = FakeReporter::default();

        assert!(matches!(
            pass(
                &old,
                now,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { .. }
        ));
        assert!(matches!(
            pass(
                &old,
                now + ATTEMPT_BACKOFF_SECS + 1,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Delivered,
                ..
            }
        ));
        assert!(matches!(
            pass(
                &replacement,
                now + ATTEMPT_BACKOFF_SECS + 1,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { .. }
        ));
        assert_eq!(poker.calls.len(), 2);
        assert!(poker.calls[0].starts_with("node.old-root:"));
        assert!(poker.calls[1].starts_with("node.new-root:"));
    }

    #[test]
    fn shepherd_dnd_then_available_delivers_without_consuming_the_bucket() {
        let specs = [root("root", "exec codex")];
        let mut runtime = Runtime::default();
        let mut store = FakeStore {
            dnd: true,
            ..Default::default()
        };
        let mut poker = FakePoker::default();
        let mut reporter = FakeReporter::default();
        let now = CADENCE_SECS * 2;

        assert!(matches!(
            pass(
                &specs,
                now,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Dnd,
                ..
            }
        ));
        store.dnd = false;
        assert!(matches!(
            pass(
                &specs,
                now + 1,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { .. }
        ));
        assert_eq!(poker.calls.len(), 1);
    }

    #[test]
    fn shepherd_selection_faults_report_once_until_the_state_changes() {
        let codex = root("root", "exec codex");
        let other = root("other-root", "exec codex");
        let non_codex = root("root", "exec claude");
        let mut runtime = Runtime::default();
        let mut store = FakeStore::default();
        let mut poker = FakePoker::default();
        let mut reporter = FakeReporter::default();

        for _ in 0..2 {
            assert!(matches!(
                pass(&[], 1, &mut runtime, &mut store, &mut poker, &mut reporter),
                Outcome::Fault(Fault::MissingRoot { .. })
            ));
        }
        assert_eq!(reporter.0.len(), 1);

        for _ in 0..2 {
            assert!(matches!(
                pass(
                    &[codex.clone(), other.clone()],
                    1,
                    &mut runtime,
                    &mut store,
                    &mut poker,
                    &mut reporter
                ),
                Outcome::Fault(Fault::DuplicateRoots { .. })
            ));
        }
        assert_eq!(reporter.0.len(), 2);

        for _ in 0..2 {
            assert!(matches!(
                pass(
                    std::slice::from_ref(&non_codex),
                    1,
                    &mut runtime,
                    &mut store,
                    &mut poker,
                    &mut reporter
                ),
                Outcome::Fault(Fault::NonCodexRoot { .. })
            ));
        }
        assert_eq!(reporter.0.len(), 3);

        // A healthy pass clears the fault latch. The same missing-root fault is reportable again
        // after that real state transition.
        assert!(matches!(
            pass(
                std::slice::from_ref(&codex),
                1,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Delivered { .. }
        ));
        assert!(matches!(
            pass(&[], 1, &mut runtime, &mut store, &mut poker, &mut reporter),
            Outcome::Fault(Fault::MissingRoot { .. })
        ));
        assert_eq!(reporter.0.len(), 4);
    }

    #[test]
    fn shepherd_failed_poke_is_backed_off_across_a_fresh_runtime() {
        let specs = [root("root", "exec codex")];
        let now = CADENCE_SECS * 4;
        let mut store = FakeStore::default();
        let mut poker = FakePoker {
            failures: 2,
            ..Default::default()
        };
        let mut reporter = FakeReporter::default();

        assert!(matches!(
            pass(
                &specs,
                now,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Fault(Fault::Poke { .. })
        ));
        assert!(matches!(
            pass(
                &specs,
                now + ATTEMPT_BACKOFF_SECS - 1,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Backoff,
                ..
            }
        ));
        assert_eq!(poker.calls.len(), 1);

        assert!(matches!(
            pass(
                &specs,
                now + ATTEMPT_BACKOFF_SECS,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Fault(Fault::Poke { .. })
        ));
        assert_eq!(poker.calls.len(), 2);
    }

    #[test]
    fn shepherd_attempt_write_failure_fails_closed_before_poke() {
        let specs = [root("root", "exec codex")];
        let mut runtime = Runtime::default();
        let mut store = FakeStore {
            fail_attempt_writes: 2,
            ..Default::default()
        };
        let mut poker = FakePoker::default();
        let mut reporter = FakeReporter::default();

        for _ in 0..2 {
            assert!(matches!(
                pass(
                    &specs,
                    1,
                    &mut runtime,
                    &mut store,
                    &mut poker,
                    &mut reporter
                ),
                Outcome::Fault(Fault::AttemptWrite { .. })
            ));
        }
        assert!(poker.calls.is_empty());
        assert_eq!(
            reporter.0.len(),
            1,
            "an unchanged write fault is reported once per runtime"
        );
    }

    #[test]
    fn shepherd_delivered_write_failure_uses_the_runtime_latch() {
        let specs = [root("root", "exec codex")];
        let now = CADENCE_SECS * 5;
        let mut runtime = Runtime::default();
        let mut store = FakeStore {
            fail_delivered_writes: 1,
            ..Default::default()
        };
        let mut poker = FakePoker::default();
        let mut reporter = FakeReporter::default();

        assert!(matches!(
            pass(
                &specs,
                now,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Fault(Fault::DeliveredWrite { .. })
        ));
        assert!(matches!(
            pass(
                &specs,
                now + ATTEMPT_BACKOFF_SECS + 1,
                &mut runtime,
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Delivered,
                ..
            }
        ));
        assert_eq!(poker.calls.len(), 1);
        assert_eq!(reporter.0.len(), 1);

        // A process restart loses the in-memory success latch, but the durable attempt still
        // throttles an immediate duplicate.
        assert!(matches!(
            pass(
                &specs,
                now + ATTEMPT_BACKOFF_SECS - 1,
                &mut Runtime::default(),
                &mut store,
                &mut poker,
                &mut reporter
            ),
            Outcome::Skipped {
                reason: SkipReason::Backoff,
                ..
            }
        ));
        assert_eq!(poker.calls.len(), 1);
    }

    #[test]
    fn filesystem_store_persists_independent_host_and_root_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut first = FilesystemStore::new(tmp.path());
        let left = ShepherdKey::new("host/a", "root.one");
        let right = ShepherdKey::new("host/a", "root/two");
        first
            .save(
                &left,
                PersistedState {
                    attempt_at: Some(7),
                    delivered_bucket: Some(3),
                },
            )
            .unwrap();
        first
            .save(
                &right,
                PersistedState {
                    attempt_at: Some(9),
                    delivered_bucket: None,
                },
            )
            .unwrap();

        let mut restarted = FilesystemStore::new(tmp.path());
        assert_eq!(
            restarted.load(&left).unwrap(),
            PersistedState {
                attempt_at: Some(7),
                delivered_bucket: Some(3)
            }
        );
        assert_eq!(
            restarted.load(&right).unwrap(),
            PersistedState {
                attempt_at: Some(9),
                delivered_bucket: None
            }
        );
    }

    #[test]
    fn codex_command_classification_is_exact() {
        assert!(command_invokes_codex(
            "exec codex --dangerously-bypass-approvals-and-sandbox 'x'"
        ));
        assert!(command_invokes_codex("/opt/bin/codex --model x"));
        assert!(!command_invokes_codex("echo codex"));
        assert!(!command_invokes_codex("claude codex"));
        assert!(!command_invokes_codex(
            "exec /opt/bin/codex-wrapper --model x"
        ));
        assert!(!SHEPHERD_PROMPT.contains("Check your inbox"));
        assert!(SHEPHERD_PROMPT.contains("not an inbox event"));

        let mut wrong_agent = root("root", "exec claude");
        wrong_agent.tasks.push(Task {
            kind: TaskKind::Exec,
            name: "audit".into(),
            id: None,
            command: Some("exec codex".into()),
            cwd: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
        });
        assert!(matches!(
            select_target(&[wrong_agent], "node"),
            Err(Fault::NonCodexRoot { .. })
        ));
    }
}
