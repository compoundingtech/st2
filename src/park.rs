//! The parked set's on-disk projection, and the operator's unpark request channel.
//!
//! [`FlappingCap`](crate::flapping::FlappingCap) is the source of truth for which tasks are parked,
//! and it lives in the supervisor's memory. Everything else — `st2 tasks` above all — is a separate,
//! read-only process, which is why a parked task used to show `desired: running`, not running, and
//! `error: null`: the fault existed, but only inside a process nobody was reading. This module is how
//! that in-memory truth becomes legible without becoming a second source of it.
//!
//! Two directions, one writer each. Conflating them is the tempting mistake:
//!
//! * The **projection** (`parked/`) is written by the supervisor and read by everyone. A missing or
//!   absent marker means *not parked*. Nothing else may write here.
//! * The **request** (`unpark/`) is written by `st2 unpark` and consumed by the supervisor. A missing
//!   request means *nobody asked*.
//!
//! Were the projection also the request — "no marker, therefore unpark" — then a wiped state dir, a
//! failed write, or a supervisor and CLI disagreeing about `XDG_STATE_HOME` would all silently mean
//! *release every parked task on this host*. Each direction instead fails towards inaction.
//!
//! A marker is stamped with the generation of the supervisor that wrote it, and a marker whose
//! supervisor is gone reads as **not parked**. That is not leniency: R31 scopes parking to one
//! supervisor run, so no run means no park. It also keeps `st2 tasks` quiet and zero-exit on a host
//! where st2 simply is not running. The generation is `(pid, start-time)` rather than a bare pid,
//! because a recycled pid would otherwise resurrect a park from a supervisor that died days ago.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// Wire schema of one park marker.
pub const PARK_SCHEMA: &str = "st2.park.v1";

/// The machine-local park projection dir for a host: `$XDG_STATE_HOME/st2/<host>/parked`. A sibling
/// of the exec runner state, and equally not synced — a park belongs to one host's supervisor run.
pub fn park_dir(host: &str) -> PathBuf {
    crate::run::exec_state_dir(host).with_file_name("parked")
}

/// The operator's unpark request dir: `$XDG_STATE_HOME/st2/<host>/unpark`.
pub fn unpark_request_dir(host: &str) -> PathBuf {
    crate::run::exec_state_dir(host).with_file_name("unpark")
}

/// One parked task, as published by the supervisor that parked it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParkRecord {
    pub schema: String,
    pub runtime_id: String,
    /// The supervisor that parked it, identified by pid *and* start time. Both must match a live
    /// process for the marker to count — a pid alone is reuse-unsafe.
    pub supervisor_pid: u32,
    pub supervisor_start_time_ticks: u64,
    /// When the park was published, RFC3339 UTC with milliseconds.
    pub parked_at: String,
    /// Why, in the operator's terms.
    pub reason: String,
}

/// What the projection says about one desired task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkState {
    /// Positively not parked: no marker, or a marker owned by a supervisor that is gone.
    NotParked,
    Parked(ParkRecord),
    /// The marker exists but could not be believed. Unreadable evidence is not absence.
    Indeterminate(String),
}

/// One coherent read of the projection. `complete` false means some marker could not be believed, not
/// that some task is parked — a parked task is a *known* fault and keeps the batch complete.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParkBatch {
    pub complete: bool,
    pub states: BTreeMap<String, ParkState>,
    pub errors: Vec<String>,
}

impl ParkBatch {
    pub fn state(&self, runtime_id: &str) -> &ParkState {
        self.states.get(runtime_id).unwrap_or(&ParkState::NotParked)
    }
}

/// Read-only boundary over the projection, so the inventory can be driven deterministically in tests
/// without a real supervisor or a real state dir.
pub trait ParkObserver {
    fn observe(&self, desired: &[String]) -> ParkBatch;
}

/// The real projection reader, rooted at a host's park dir.
#[derive(Debug, Clone)]
pub struct DirParkObserver {
    dir: PathBuf,
}

impl DirParkObserver {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn for_host(host: &str) -> Self {
        Self::new(park_dir(host))
    }
}

impl ParkObserver for DirParkObserver {
    fn observe(&self, desired: &[String]) -> ParkBatch {
        let mut batch = ParkBatch {
            complete: true,
            ..Default::default()
        };
        for runtime_id in desired {
            let state = match read_marker(&self.dir, runtime_id) {
                Ok(state) => state,
                Err(error) => {
                    batch.complete = false;
                    let detail = format!("park marker for {runtime_id:?}: {error}");
                    batch.errors.push(detail.clone());
                    ParkState::Indeterminate(detail)
                }
            };
            batch.states.insert(runtime_id.clone(), state);
        }
        batch
    }
}

/// A projection with nothing in it — the honest state of a host whose supervisor has parked nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoParks;

impl ParkObserver for NoParks {
    fn observe(&self, desired: &[String]) -> ParkBatch {
        ParkBatch {
            complete: true,
            states: desired
                .iter()
                .map(|id| (id.clone(), ParkState::NotParked))
                .collect(),
            errors: Vec::new(),
        }
    }
}

fn marker_path(dir: &Path, runtime_id: &str) -> PathBuf {
    dir.join(format!("{runtime_id}.json"))
}

fn read_marker(dir: &Path, runtime_id: &str) -> anyhow::Result<ParkState> {
    let path = marker_path(dir, runtime_id);
    let raw = match fs::read(&path) {
        Ok(raw) => raw,
        // A missing marker — or a missing dir entirely — is positively "not parked". A host whose
        // supervisor never parked anything has no projection dir at all, and must not be an error.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ParkState::NotParked);
        }
        Err(error) => anyhow::bail!("reading {}: {error}", path.display()),
    };
    let record: ParkRecord =
        serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))?;
    if record.schema != PARK_SCHEMA {
        anyhow::bail!(
            "unknown schema {:?} in {} (expected {PARK_SCHEMA})",
            record.schema,
            path.display()
        );
    }
    if record.runtime_id != runtime_id {
        anyhow::bail!(
            "{} claims runtime id {:?}",
            path.display(),
            record.runtime_id
        );
    }
    if crate::exec_backend::process_start_time_ticks(record.supervisor_pid as i32).ok()
        != Some(record.supervisor_start_time_ticks)
    {
        // The supervisor that parked it is gone. R31 scopes a park to one supervisor run, so this is
        // a positive "not parked", not an unknown — and it keeps `st2 tasks` zero-exit on a host
        // where st2 is not running at all.
        return Ok(ParkState::NotParked);
    }
    Ok(ParkState::Parked(record))
}

/// The supervisor's writer for the projection. Owns the dir exclusively.
#[derive(Debug, Clone)]
pub struct ParkProjection {
    dir: PathBuf,
    supervisor_pid: u32,
    supervisor_start_time_ticks: u64,
}

impl ParkProjection {
    /// Stamp the projection with *this* process's generation. Fails only if the supervisor cannot
    /// identify itself, which would make every marker it wrote unbelievable anyway.
    pub fn current(dir: PathBuf) -> anyhow::Result<Self> {
        let supervisor_pid = std::process::id();
        let supervisor_start_time_ticks =
            crate::exec_backend::process_start_time_ticks(supervisor_pid as i32)
                .context("identifying this supervisor's process generation")?;
        Ok(Self {
            dir,
            supervisor_pid,
            supervisor_start_time_ticks,
        })
    }

    pub fn for_host(host: &str) -> anyhow::Result<Self> {
        Self::current(park_dir(host))
    }

    /// Republish the projection to exactly `parked`, returning any non-fatal errors. Markers for ids
    /// that are no longer parked are removed, so a recovered task stops reporting a fault without
    /// anyone having to remember to clean up after it.
    pub fn publish(&self, parked: &BTreeSet<String>, reason: &str) -> Vec<String> {
        let mut errors = Vec::new();
        if parked.is_empty() && !self.dir.exists() {
            return errors;
        }
        if let Err(error) = fs::create_dir_all(&self.dir) {
            errors.push(format!(
                "creating park projection {}: {error}",
                self.dir.display()
            ));
            return errors;
        }
        for runtime_id in parked {
            if let Err(error) = self.write_marker(runtime_id, reason) {
                errors.push(format!("publishing park for {runtime_id:?}: {error}"));
            }
        }
        match fs::read_dir(&self.dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    let Some(runtime_id) = name.strip_suffix(".json") else {
                        continue;
                    };
                    if parked.contains(runtime_id) {
                        continue;
                    }
                    if let Err(error) = fs::remove_file(entry.path()) {
                        errors.push(format!("clearing stale park {runtime_id:?}: {error}"));
                    }
                }
            }
            Err(error) => errors.push(format!(
                "listing park projection {}: {error}",
                self.dir.display()
            )),
        }
        errors
    }

    fn write_marker(&self, runtime_id: &str, reason: &str) -> anyhow::Result<()> {
        let path = marker_path(&self.dir, runtime_id);
        // An already-published park keeps its original `parkedAt`: rewriting it every pass would make
        // "since when" read as "since a moment ago" forever, which is the field an operator uses to
        // tell a fresh crash-loop from one that has been down all day.
        if let Ok(raw) = fs::read(&path)
            && let Ok(existing) = serde_json::from_slice::<ParkRecord>(&raw)
            && existing.runtime_id == runtime_id
            && existing.supervisor_pid == self.supervisor_pid
            && existing.supervisor_start_time_ticks == self.supervisor_start_time_ticks
        {
            return Ok(());
        }
        let record = ParkRecord {
            schema: PARK_SCHEMA.to_string(),
            runtime_id: runtime_id.to_string(),
            supervisor_pid: self.supervisor_pid,
            supervisor_start_time_ticks: self.supervisor_start_time_ticks,
            parked_at: crate::exec_backend::rfc3339_utc(std::time::SystemTime::now())
                .context("timestamping park")?,
            reason: reason.to_string(),
        };
        write_json_atomically(&path, &record, ".park.")
    }
}

fn write_json_atomically<T: Serialize>(
    path: &Path,
    value: &T,
    temp_prefix: &str,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
    fs::create_dir_all(parent)?;
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    let mut temp = tempfile::Builder::new()
        .prefix(temp_prefix)
        .tempfile_in(parent)?;
    temp.write_all(&bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("publishing {}", path.display()))?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Reject anything that is not a plain filename, so an operator's typo (or a hostile argument) cannot
/// steer a request write outside the request dir. The projection side needs no equivalent: those ids
/// come from validated declarations, never from a command line.
fn validate_request_id(runtime_id: &str) -> anyhow::Result<()> {
    let mut components = Path::new(runtime_id).components();
    let single = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    );
    anyhow::ensure!(
        single && !runtime_id.is_empty() && !runtime_id.starts_with('.'),
        "{runtime_id:?} is not a valid task id"
    );
    Ok(())
}

/// Ask the supervisor to unpark one task. Returns the request path. Writing a request that already
/// exists is not an error — the operator asked twice for the same thing.
pub fn request_unpark(dir: &Path, runtime_id: &str) -> anyhow::Result<PathBuf> {
    validate_request_id(runtime_id)?;
    let path = dir.join(runtime_id);
    fs::create_dir_all(dir)?;
    fs::File::create(&path).with_context(|| format!("writing {}", path.display()))?;
    fs::File::open(dir)?.sync_all()?;
    Ok(path)
}

/// Consume every pending unpark request, returning the requested ids and any non-fatal errors. Each
/// request is removed as it is taken, so one request produces exactly one recovery attempt: leaving it
/// behind would relaunch the task on every subsequent pass, defeating the restart policy entirely.
pub fn take_unpark_requests(dir: &Path) -> (Vec<String>, Vec<String>) {
    let mut ids = Vec::new();
    let mut errors = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return (ids, errors),
        Err(error) => {
            errors.push(format!(
                "listing unpark requests {}: {error}",
                dir.display()
            ));
            return (ids, errors);
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => ids.push(name.to_string()),
            // Another pass took it first. One request, one attempt — do not act on it twice.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => errors.push(format!("taking unpark request {name:?}: {error}")),
        }
    }
    ids.sort();
    (ids, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(dir: &Path) -> ParkProjection {
        ParkProjection::current(dir.to_path_buf()).expect("this process has a generation")
    }

    fn parked(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    fn desired(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    #[test]
    fn published_parks_are_readable_and_clear_when_the_task_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let projection = projection(dir.path());
        let observer = DirParkObserver::new(dir.path().to_path_buf());

        assert!(projection.publish(&parked(&["a", "b"]), "crash-looped").is_empty());
        let batch = observer.observe(&desired(&["a", "b", "healthy"]));
        assert!(batch.complete, "a parked task is a known fault, not missing evidence");
        assert!(batch.errors.is_empty());
        let ParkState::Parked(record) = batch.state("a") else {
            panic!("'a' was published as parked but does not read back as parked");
        };
        assert_eq!(record.runtime_id, "a");
        assert_eq!(record.reason, "crash-looped");
        assert_eq!(record.supervisor_pid, std::process::id());
        assert!(matches!(batch.state("b"), ParkState::Parked(_)));
        assert_eq!(batch.state("healthy"), &ParkState::NotParked);

        // 'a' recovers: republishing without it must retract its marker, not leave a fault standing.
        assert!(projection.publish(&parked(&["b"]), "crash-looped").is_empty());
        let batch = observer.observe(&desired(&["a", "b"]));
        assert_eq!(batch.state("a"), &ParkState::NotParked);
        assert!(matches!(batch.state("b"), ParkState::Parked(_)));
        assert!(batch.complete);
    }

    /// A park belongs to one supervisor run (R31). A marker left behind by a supervisor that has since
    /// died describes a park that no longer exists, and reporting it would make `st2 tasks` claim a
    /// fault on every host where st2 merely is not running.
    ///
    /// The pid used here is this process's own with a *wrong* start time, which is exactly the
    /// pid-reuse shape: the pid is live, so a bare `kill -0` check would believe the marker.
    #[test]
    fn a_marker_from_a_dead_supervisor_reads_as_not_parked() {
        let dir = tempfile::tempdir().unwrap();
        let live = projection(dir.path());
        assert!(live.publish(&parked(&["a"]), "crash-looped").is_empty());

        let path = marker_path(dir.path(), "a");
        let mut record: ParkRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        record.supervisor_start_time_ticks = record.supervisor_start_time_ticks.wrapping_add(1);
        write_json_atomically(&path, &record, ".park.").unwrap();

        let batch = DirParkObserver::new(dir.path().to_path_buf()).observe(&desired(&["a"]));
        assert_eq!(
            batch.state("a"),
            &ParkState::NotParked,
            "a park outlived the supervisor run it belongs to"
        );
        assert!(batch.complete, "a stale marker is a positive absence, not an unknown");
        assert!(batch.errors.is_empty());
    }

    /// A host that has never parked anything has no projection dir. That must read as "nothing is
    /// parked" and stay complete — otherwise every `st2 tasks` on a quiet host exits non-zero.
    #[test]
    fn an_absent_projection_is_positively_empty() {
        let dir = tempfile::tempdir().unwrap();
        let observer = DirParkObserver::new(dir.path().join("never-created"));
        let batch = observer.observe(&desired(&["a", "b"]));
        assert!(batch.complete);
        assert!(batch.errors.is_empty());
        assert_eq!(batch.state("a"), &ParkState::NotParked);
    }

    /// Unreadable evidence is not absence. A marker that exists but cannot be believed makes the batch
    /// incomplete, matching the rest of the diagnostic surface's fail-closed posture.
    #[test]
    fn an_unbelievable_marker_is_indeterminate_not_absent() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path()).unwrap();
        fs::write(marker_path(dir.path(), "garbage"), b"{not json").unwrap();
        fs::write(
            marker_path(dir.path(), "wrong-schema"),
            br#"{"schema":"st2.park.v99","runtimeId":"wrong-schema","supervisorPid":1,"supervisorStartTimeTicks":1,"parkedAt":"2026-01-01T00:00:00.000Z","reason":"x"}"#,
        )
        .unwrap();

        let batch =
            DirParkObserver::new(dir.path().to_path_buf()).observe(&desired(&["garbage", "wrong-schema"]));
        assert!(!batch.complete);
        assert_eq!(batch.errors.len(), 2);
        assert!(matches!(batch.state("garbage"), ParkState::Indeterminate(_)));
        assert!(matches!(batch.state("wrong-schema"), ParkState::Indeterminate(_)));
    }

    /// The park's age is what tells a fresh crash-loop from one that has been down all day, so a pass
    /// that re-publishes an existing park must not restamp it.
    #[test]
    fn republishing_an_existing_park_preserves_when_it_happened() {
        let dir = tempfile::tempdir().unwrap();
        let projection = projection(dir.path());
        projection.publish(&parked(&["a"]), "crash-looped");
        let first = fs::read(marker_path(dir.path(), "a")).unwrap();
        projection.publish(&parked(&["a"]), "crash-looped");
        assert_eq!(first, fs::read(marker_path(dir.path(), "a")).unwrap());
    }

    #[test]
    fn a_request_is_consumed_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        request_unpark(dir.path(), "b").unwrap();
        request_unpark(dir.path(), "a").unwrap();
        // Asking twice for the same task is one request, not two.
        request_unpark(dir.path(), "a").unwrap();

        let (ids, errors) = take_unpark_requests(dir.path());
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
        assert!(errors.is_empty());

        // Taking is consuming: a leftover request would relaunch the task on every later pass and
        // quietly disable the restart policy it was parked by.
        let (ids, errors) = take_unpark_requests(dir.path());
        assert!(ids.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn a_missing_request_dir_is_no_requests() {
        let dir = tempfile::tempdir().unwrap();
        let (ids, errors) = take_unpark_requests(&dir.path().join("never-created"));
        assert!(ids.is_empty());
        assert!(errors.is_empty());
    }

    /// The request dir is the one place an operator's argument becomes a path, so it is the one place
    /// that has to refuse a traversal rather than write outside itself.
    #[test]
    fn a_request_id_cannot_escape_its_dir() {
        let dir = tempfile::tempdir().unwrap();
        for bad in ["../escaped", "sub/nested", "/absolute", "", ".", "..", ".hidden"] {
            assert!(
                request_unpark(dir.path(), bad).is_err(),
                "{bad:?} was accepted as a task id"
            );
        }
        assert!(request_unpark(dir.path(), "dev3.agent.ding").is_ok());
    }
}
