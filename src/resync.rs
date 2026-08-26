//! Resync events: supervisor-emitted notifications when a live agent's declared resource carriers
//! change on disk ([`06-resync`](../docs/vrs/06-resync/spec.md)).
//!
//! Delivery rides the declared-stream ingress (`st2::event::emit`) through the built-in reserved
//! `resync` stream that exists on every running agent without declaration. Watching is
//! deny-by-default: one non-recursive watch per distinct parent directory of the resolved
//! watchable carriers, so whole-file replacement by rename stays visible through the surviving
//! directory inode. Digest state is seeded silently when a reconcile pass installs a watch set;
//! only a content transition emits.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use notify::Watcher as _;
use sha2::{Digest as _, Sha256};

use agent_spec::spec::AgentSpec;

/// The built-in stream every running agent accepts without declaring it.
pub const RESYNC_STREAM: &str = "resync";

/// Provisional coalescing windows (`RESYNC-T02`): tuned by observed notification volume.
const IMMEDIATE_WINDOW: Duration = Duration::from_millis(500);
const COALESCED_WINDOW: Duration = Duration::from_secs(5);

/// How a carrier notifies (`RESYNC-R04`). Silent carriers never reach the watch set:
/// [`classify`] excludes them, so nothing about them is observed or emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CarrierClass {
    Immediate,
    Coalesced,
}

impl CarrierClass {
    fn window(self) -> Duration {
        match self {
            CarrierClass::Immediate => IMMEDIATE_WINDOW,
            CarrierClass::Coalesced => COALESCED_WINDOW,
        }
    }
}

/// One watchable local carrier: binding label, absolute path, notification class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchableCarrier {
    pub label: String,
    pub path: PathBuf,
    pub class: CarrierClass,
}

/// The watchable carriers of one agent, keyed by its bus id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWatchSet {
    pub bus_id: String,
    pub carriers: Vec<WatchableCarrier>,
}

/// Resolve one spec's watch set: the declaration file plus every active resource binding whose
/// URI denotes a local file (`RESYNC-R01`). Bindings with an inactive reason are skipped;
/// schemes without a local denotation and silent stores are simply absent.
pub fn watch_set_for(spec: &AgentSpec, this_host: &str) -> AgentWatchSet {
    let agent_dir = spec.path.parent().unwrap_or(Path::new("."));
    let mut carriers = vec![WatchableCarrier {
        label: "declaration".to_owned(),
        path: spec.path.clone(),
        class: CarrierClass::Immediate,
    }];
    for resource in &spec.resources {
        if resource.inactive_reason().is_some() {
            continue;
        }
        let Some(path) = resolve_local_path(agent_dir, resource.uri()) else {
            continue;
        };
        let Some(class) = classify(agent_dir, resource.name(), &path) else {
            continue;
        };
        carriers.push(WatchableCarrier {
            label: resource.name().to_owned(),
            path,
            class,
        });
    }
    // The supervisor's resolved logical host — not the OS hostname — decides the bus id, so an
    // agent supervised under `st2 up --host <alias>` without an explicit declaration host still
    // produces a recipient `resolve_stream` can resolve.
    AgentWatchSet {
        bus_id: spec.bus_id(this_host),
        carriers,
    }
}

/// A URI denotes a local file when it is an absolute `file://` URI or a scheme-less
/// catalog-relative path resolved against the agent directory. Anything else has no local
/// denotation.
fn resolve_local_path(agent_dir: &Path, uri: &str) -> Option<PathBuf> {
    if let Some(rest) = uri.strip_prefix("file://") {
        let path = PathBuf::from(rest);
        return path.is_absolute().then_some(path);
    }
    let has_uri_scheme = uri.split_once(':').is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && !scheme.contains('/')
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    });
    if has_uri_scheme {
        return None;
    }
    Some(agent_dir.join(uri))
}

/// Class defaults (`RESYNC-R04`): goal carriers are immediate; stores the agent itself authors
/// are silent (None); everything else is coalesced. The declaration carrier is immediate by
/// construction in [`watch_set_for`].
fn classify(agent_dir: &Path, binding_name: &str, path: &Path) -> Option<CarrierClass> {
    let agent_relative = path.strip_prefix(agent_dir).ok();
    let authored_store = agent_relative.is_some_and(|rel| {
        rel.starts_with("resources/context")
            || rel.starts_with("resources/decisions")
            || rel.starts_with("resources/friction")
    });
    if authored_store {
        return None;
    }
    let goal = binding_name == "goal" || path.file_name().is_some_and(|n| n == "goal.md");
    Some(if goal {
        CarrierClass::Immediate
    } else {
        CarrierClass::Coalesced
    })
}

// ---- Supervisor side ---------------------------------------------------------------------------

enum Msg {
    WatchSet(Vec<AgentWatchSet>),
    Mutations(Vec<PathBuf>),
    /// Explicit stop: the worker's own watcher holds the last `Sender`, so `Disconnected`
    /// would stay unreachable while `join` waits.
    Shutdown,
}

/// Handle for the resync worker thread. Dropping it disconnects the mailbox and joins the worker.
pub struct ResyncSupervisor {
    tx: Option<Sender<Msg>>,
    handle: Option<JoinHandle<()>>,
}

impl ResyncSupervisor {
    /// Spawn the worker. Watch installation itself is best-effort per refresh; a failure degrades
    /// to timer-driven digest polling over the watch set rather than losing the capability.
    pub fn spawn(root: PathBuf, this_host: String) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
        let forward = tx.clone();
        let handle = std::thread::Builder::new()
            .name("resync".to_owned())
            .spawn(move || worker_loop(root, this_host, rx, forward))
            .ok();
        Self {
            tx: Some(tx),
            handle,
        }
    }
    /// Replace the worker's watch set from the pass's already-discovered specs. Called once per
    /// reconcile pass; new carriers are seeded silently, removed ones are dropped.
    pub fn refresh(&self, specs: &[AgentSpec], this_host: &str) {
        let sets = specs
            .iter()
            .filter(|spec| spec.resolved_host(this_host) == this_host)
            .filter(|spec| spec.desired_state.is_running())
            .map(|spec| watch_set_for(spec, this_host))
            .collect();
        if let Some(tx) = &self.tx {
            let _ = tx.send(Msg::WatchSet(sets));
        }
    }
}

impl Drop for ResyncSupervisor {
    fn drop(&mut self) {
        // The worker's own watcher holds the last Sender, so dropping the handle alone would
        // leave `Disconnected` unreachable and join blocked until a recv timeout fires.
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Msg::Shutdown);
            drop(tx);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// ---- Worker side --------------------------------------------------------------------------------

struct Entry {
    bus_id: String,
    label: String,
    class: CarrierClass,
    digest: Option<String>,
    dirty: bool,
}

#[cfg(unix)]
type DirIdentity = (u64, u64);
#[cfg(not(unix))]
type DirIdentity = ();

fn dir_identity(path: &Path) -> Option<DirIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(path).ok().map(|_| ())
    }
}

fn is_mutation(event: &notify::Event) -> bool {
    use notify::event::*;
    !matches!(
        event.kind,
        EventKind::Access(_) | EventKind::Other | EventKind::Any
    )
}

struct Worker {
    root: PathBuf,
    this_host: String,
    carriers: BTreeMap<PathBuf, Vec<Entry>>,
    deadlines: BTreeMap<CarrierClass, Instant>,
    watched: BTreeMap<PathBuf, Option<DirIdentity>>,
    /// `None` degrades to digest polling at refresh cadence (each reconcile pass) instead of
    /// evented watching — diagnosed once by the absence of immediacy, never a hard error.
    watcher: Option<notify::RecommendedWatcher>,
}

fn make_watcher(forward: Sender<Msg>) -> Option<notify::RecommendedWatcher> {
    notify::RecommendedWatcher::new(
        move |result: notify::Result<notify::Event>| {
            if let Ok(event) = result {
                if is_mutation(&event) {
                    let _ = forward.send(Msg::Mutations(event.paths.clone()));
                }
            }
        },
        notify::Config::default().with_follow_symlinks(false),
    )
    .ok()
}

fn worker_loop(root: PathBuf, this_host: String, rx: Receiver<Msg>, forward: Sender<Msg>) {
    let watcher = make_watcher(forward);
    let mut worker = Worker {
        root,
        this_host,
        carriers: BTreeMap::new(),
        deadlines: BTreeMap::new(),
        watched: BTreeMap::new(),
        watcher,
    };
    loop {
        let timeout = worker
            .deadlines
            .values()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .min()
            .unwrap_or(Duration::from_secs(3600));
        match rx.recv_timeout(timeout) {
            Ok(Msg::WatchSet(sets)) => worker.apply_watch_sets(sets),
            Ok(Msg::Mutations(paths)) => worker.mark_mutated(paths),
            Ok(Msg::Shutdown) => break,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        worker.flush_due(Instant::now());
    }
}

fn rebuild_carriers(
    mut previous: BTreeMap<PathBuf, Vec<Entry>>,
    sets: Vec<AgentWatchSet>,
) -> BTreeMap<PathBuf, Vec<Entry>> {
    let mut next: BTreeMap<PathBuf, Vec<Entry>> = BTreeMap::new();
    for set in sets {
        for carrier in set.carriers {
            // Several agents may bind the same local file: every `(bus_id, label)`
            // subscription at a path is retained, not replaced (`RESYNC-R01`). Subscribers
            // already on record keep their digest and pending dirty state: a reconcile pass
            // can land between a mutation and its flush window, and reseeding here would
            // silently erase that event (`RESYNC-R03` protects the baseline, not mid-flight
            // transitions). Only genuinely unknown subscriptions seed silently.
            let existing = previous.entry(carrier.path.clone()).or_default();
            let retained = existing
                .iter()
                .position(|entry| {
                    entry.bus_id == set.bus_id
                        && entry.label == carrier.label
                        && entry.class == carrier.class
                })
                .map(|index| existing.remove(index));
            let entry = retained.unwrap_or_else(|| Entry {
                bus_id: set.bus_id.clone(),
                label: carrier.label.clone(),
                class: carrier.class,
                digest: read_digest(&carrier.path),
                dirty: false,
            });
            next.entry(carrier.path).or_default().push(entry);
        }
    }
    next
}

impl Worker {
    fn apply_watch_sets(&mut self, sets: Vec<AgentWatchSet>) {
        let mut next = rebuild_carriers(std::mem::take(&mut self.carriers), sets);
        if self.watcher.is_none() {
            self.diff_emit(&mut next);
        }
        self.carriers = next;
        // Diff paths that were blind before registering newly recovered parents; otherwise the
        // new watch suppresses polling of mutations that happened during the blind interval.
        self.poll_unwatched();
        self.refresh_watches();
    }

    /// Digest-diff every entry in `next`, emitting observed transitions. Seeding stays silent
    /// because each baseline was captured when its subscription was created.
    fn diff_emit(&mut self, next: &mut BTreeMap<PathBuf, Vec<Entry>>) {
        let paths: Vec<PathBuf> = next.keys().cloned().collect();
        for path in paths {
            let Some(entries) = next.get_mut(&path) else {
                continue;
            };
            let root = self.root.clone();
            let this_host = self.this_host.clone();
            for entry in entries.iter_mut() {
                let Some(new_digest) = read_digest(&path) else {
                    continue;
                };
                if entry.digest.as_deref() == Some(new_digest.as_str()) {
                    continue;
                }
                let old = entry.digest.as_deref();
                if emit_resync(
                    &root,
                    &this_host,
                    &entry.bus_id,
                    &entry.label,
                    &path,
                    old,
                    &new_digest,
                ) {
                    entry.digest = Some(new_digest);
                }
            }
        }
    }

    /// Poll carriers whose parent directory carries no registered watch right now.
    fn poll_unwatched(&mut self) {
        let unwatched: Vec<PathBuf> = self
            .carriers
            .keys()
            .filter(|path| {
                !self
                    .watched
                    .contains_key(path.parent().unwrap_or(Path::new(".")))
            })
            .cloned()
            .collect();
        for path in unwatched {
            self.flush_path(&path);
        }
    }

    /// Re-register watches so every distinct parent directory of the current watch set is covered,
    /// dropping directories that left the set or were replaced (identity change). A replaced watch
    /// stays blind until the next pass rebuilds it — bounded by the reconcile interval, the same
    /// tradeoff `CatalogDeclarationWatcher` accepts for declarations.
    fn refresh_watches(&mut self) {
        let mut desired: Vec<PathBuf> = Vec::new();
        for path in self.carriers.keys() {
            if let Some(parent) = path.parent() {
                desired.push(parent.to_path_buf());
            }
        }
        desired.sort();
        desired.dedup();
        self.watched.retain(|dir, identity| {
            let wanted = desired.contains(dir);
            let current = dir_identity(dir);
            let stale = !wanted || *identity != current;
            if stale {
                if let Some(watcher) = self.watcher.as_mut() {
                    let _ = watcher.unwatch(dir);
                }
            }
            !stale
        });
        for dir in desired {
            if self.watched.contains_key(&dir) {
                continue;
            }
            let identity = dir_identity(&dir);
            let Some(watcher) = self.watcher.as_mut() else {
                // Degraded mode: apply_watch_sets diffs digests at refresh cadence instead.
                break;
            };
            if watcher
                .watch(&dir, notify::RecursiveMode::NonRecursive)
                .is_ok()
            {
                self.watched.insert(dir, identity);
            }
        }
    }

    fn mark_mutated(&mut self, paths: Vec<PathBuf>) {
        let now = Instant::now();
        let mut extend = false;
        for path in paths {
            // A created or renamed directory may be a carrier's parent that did not exist at
            // refresh time and so carries no watch of its own. Its creation surfaces on the
            // nearest watched ancestor; carriers beneath it must be re-dirtied because their own
            // creation events landed inside the unwatched subtree.
            let subtree = self
                .carriers
                .keys()
                .any(|carrier| carrier.starts_with(&path) && *carrier != path);
            if subtree {
                extend = true;
            }
            let mut dirty_here: Vec<CarrierClass> = Vec::new();
            for (carrier, entries) in &mut self.carriers {
                let hit = *carrier == path || (subtree && carrier.starts_with(&path));
                if !hit {
                    continue;
                }
                for entry in entries.iter_mut() {
                    if !entry.dirty {
                        let deadline = now + entry.class.window();
                        self.deadlines
                            .entry(entry.class)
                            .and_modify(|existing| *existing = (*existing).min(deadline))
                            .or_insert(deadline);
                    }
                    entry.dirty = true;
                    dirty_here.push(entry.class);
                }
            }
            drop(dirty_here);
        }
        if extend {
            self.refresh_watches();
        }
    }

    fn flush_due(&mut self, now: Instant) {
        let due: Vec<CarrierClass> = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(class, _)| *class)
            .collect();
        for class in due {
            self.deadlines.remove(&class);
            let targets: Vec<PathBuf> = self
                .carriers
                .iter()
                .filter(|(_, entries)| {
                    entries
                        .iter()
                        .any(|entry| entry.dirty && entry.class == class)
                })
                .map(|(path, _)| path.clone())
                .collect();
            for path in targets {
                self.flush_path(&path);
            }
        }
    }

    /// Flush every subscriber of one path: diff digests, emit transitions, and retain failed
    /// publications for retry with the same event identity.
    fn flush_path(&mut self, path: &Path) {
        let Some(entries) = self.carriers.get_mut(path) else {
            return;
        };
        let mut retries = Vec::new();
        for entry in entries.iter_mut() {
            entry.dirty = false;
            let Some(new_digest) = read_digest(path) else {
                // Unreadable right now (deleted mid-window): stay quiet and keep the previous
                // digest, so a later reappearance still counts as a change.
                continue;
            };
            if entry.digest.as_deref() == Some(new_digest.as_str()) {
                continue;
            }
            if emit_resync(
                &self.root,
                &self.this_host,
                &entry.bus_id,
                &entry.label,
                path,
                entry.digest.as_deref(),
                &new_digest,
            ) {
                entry.digest = Some(new_digest);
            } else {
                entry.dirty = true;
                retries.push(entry.class);
            }
        }
        let now = Instant::now();
        for class in retries {
            let deadline = now + class.window();
            self.deadlines
                .entry(class)
                .and_modify(|existing| *existing = (*existing).min(deadline))
                .or_insert(deadline);
        }
    }
}

/// Hash the canonical rendered transition body so one `(stream, event-id)` can never identify
/// different bindings, paths, or old/new digest transitions.
fn transition_identity(body: &str) -> String {
    format!("{:x}", Sha256::digest(body.as_bytes()))
}

/// One superseded resync event through the unchanged stream ingress (`RESYNC-R06`).
fn emit_resync(
    root: &Path,
    this_host: &str,
    bus_id: &str,
    label: &str,
    path: &Path,
    old: Option<&str>,
    new_digest: &str,
) -> bool {
    let subject = format!("resource {label} changed");
    let body = render_body(label, path, old, new_digest);
    let event_id = transition_identity(&body);
    match crate::event::emit(
        root,
        this_host,
        bus_id,
        RESYNC_STREAM,
        &event_id,
        Some(label),
        Some(subject.as_str()),
        &body,
        true,
    ) {
        Ok(_) => true,
        Err(error) => {
            eprintln!(
                "st2: resync emit for '{}' failed: {error:#}",
                path.display()
            );
            false
        }
    }
}

fn read_digest(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(format!("{:x}", Sha256::digest(&bytes)))
}

fn render_body(label: &str, path: &Path, old: Option<&str>, new: &str) -> String {
    format!(
        "resource `{label}` changed\n\nbinding: {label}\npath: {}\nold: {}\nnew: {new}\n",
        path.display(),
        old.unwrap_or("unknown"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discover(catalog: &Path) -> AgentSpec {
        let found = crate::discover_strict(catalog);
        eprintln!("discovery errors: {:?}", found.errors);
        found.specs.into_iter().next().unwrap()
    }

    #[test]
    fn watch_set_covers_declaration_and_local_bindings_only() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("agents/hetz/worker");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("agent.kdl"),
            r#"agent "worker" {
  host "hetz"
  command "true"
  resource "goal" uri="resources/goal.md" reason="Mission."
  resource "issue" uri="github-issue://org/repo/41" reason="Task."
}"#,
        )
        .unwrap();
        let set = watch_set_for(&discover(tmp.path()), "hetz");
        assert_eq!(set.bus_id, "hetz.worker");
        let mut labels: Vec<&str> = set.carriers.iter().map(|c| c.label.as_str()).collect();
        labels.sort();
        assert_eq!(labels, vec!["declaration", "goal"]);
        let goal = set.carriers.iter().find(|c| c.label == "goal").unwrap();
        assert_eq!(goal.class, CarrierClass::Immediate);
        assert_eq!(goal.path, dir.join("resources/goal.md"));
    }

    #[test]
    fn bus_id_uses_the_supervisor_host_not_the_os_hostname() {
        let tmp = tempfile::tempdir().unwrap();
        // A root-level declaration file supplies neither content nor path host: host stays
        // None, so the supervisor's logical alias must decide the recipient.
        std::fs::write(
            tmp.path().join("worker.kdl"),
            r#"agent "worker" {
  command "true"
}"#,
        )
        .unwrap();
        let spec = discover(tmp.path());
        assert_eq!(watch_set_for(&spec, "alias").bus_id, "alias.worker");
        assert_eq!(watch_set_for(&spec, "other").bus_id, "other.worker");
    }

    #[test]
    fn shared_path_refresh_preserves_every_subscription_state() {
        let shared = PathBuf::from("/shared/resource.md");
        let previous = BTreeMap::from([(
            shared.clone(),
            vec![
                Entry {
                    bus_id: "host.alpha".to_owned(),
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    digest: Some("alpha-before".to_owned()),
                    dirty: true,
                },
                Entry {
                    bus_id: "host.beta".to_owned(),
                    label: "spec".to_owned(),
                    class: CarrierClass::Coalesced,
                    digest: Some("beta-before".to_owned()),
                    dirty: true,
                },
            ],
        )]);
        let sets = vec![
            AgentWatchSet {
                bus_id: "host.alpha".to_owned(),
                carriers: vec![WatchableCarrier {
                    label: "goal".to_owned(),
                    path: shared.clone(),
                    class: CarrierClass::Immediate,
                }],
            },
            AgentWatchSet {
                bus_id: "host.beta".to_owned(),
                carriers: vec![WatchableCarrier {
                    label: "spec".to_owned(),
                    path: shared.clone(),
                    class: CarrierClass::Coalesced,
                }],
            },
        ];

        let rebuilt = rebuild_carriers(previous, sets);
        let entries = rebuilt.get(&shared).expect("shared path remains watched");
        assert_eq!(entries.len(), 2);
        for (bus_id, digest) in [
            ("host.alpha", "alpha-before"),
            ("host.beta", "beta-before"),
        ] {
            let entry = entries
                .iter()
                .find(|entry| entry.bus_id == bus_id)
                .expect("subscriber remains present");
            assert_eq!(entry.digest.as_deref(), Some(digest));
            assert!(entry.dirty, "pending mutation remains pending for {bus_id}");
        }
    }

    #[test]
    fn failed_emit_retains_digest_and_schedules_the_same_transition_for_retry() {
        let root = tempfile::tempdir().unwrap();
        let carrier = root.path().join("carrier.md");
        std::fs::write(&carrier, "new bytes").unwrap();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(
                carrier.clone(),
                vec![Entry {
                    bus_id: "host.missing".to_owned(),
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    digest: Some("old-digest".to_owned()),
                    dirty: true,
                }],
            )]),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };

        worker.flush_path(&carrier);
        let entry = &worker.carriers[&carrier][0];
        assert_eq!(entry.digest.as_deref(), Some("old-digest"));
        assert!(entry.dirty);
        assert!(worker.deadlines.contains_key(&CarrierClass::Immediate));
    }

    #[test]
    fn transition_identity_covers_every_rendered_transition_dimension() {
        let baseline = render_body(
            "goal",
            Path::new("/agent/goal.md"),
            Some("old-digest"),
            "new-digest",
        );
        assert_eq!(
            transition_identity(&baseline),
            transition_identity(&baseline),
            "replaying one canonical body must reproduce its identity"
        );

        for (dimension, changed) in [
            (
                "binding",
                render_body(
                    "spec",
                    Path::new("/agent/goal.md"),
                    Some("old-digest"),
                    "new-digest",
                ),
            ),
            (
                "path",
                render_body(
                    "goal",
                    Path::new("/other/goal.md"),
                    Some("old-digest"),
                    "new-digest",
                ),
            ),
            (
                "old digest",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    Some("other-old"),
                    "new-digest",
                ),
            ),
            (
                "seeded old state",
                render_body("goal", Path::new("/agent/goal.md"), None, "new-digest"),
            ),
            (
                "new digest",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    Some("old-digest"),
                    "other-new",
                ),
            ),
        ] {
            assert_ne!(
                transition_identity(&baseline),
                transition_identity(&changed),
                "changing {dimension} must change the event identity"
            );
        }
    }

    #[test]
    fn local_path_resolution_accepts_only_file_uris_and_schemeless_paths() {
        let agent_dir = Path::new("/cat/agents/hetz/w");
        assert_eq!(
            resolve_local_path(agent_dir, "file:///etc/demo.kdl"),
            Some(PathBuf::from("/etc/demo.kdl"))
        );
        assert_eq!(
            resolve_local_path(agent_dir, "resources/journal.md"),
            Some(agent_dir.join("resources/journal.md"))
        );
        assert_eq!(resolve_local_path(agent_dir, "http://x/y"), None);
        assert_eq!(resolve_local_path(agent_dir, "worktree://repo/main"), None);
    }

    #[test]
    fn classification_is_goal_immediate_stores_silent_other_coalesced() {
        let agent_dir = Path::new("/cat/agents/hetz/w");
        assert_eq!(
            classify(agent_dir, "mission", &agent_dir.join("resources/goal.md")),
            Some(CarrierClass::Immediate),
            "basename goal.md is immediate regardless of binding name"
        );
        assert_eq!(
            classify(
                agent_dir,
                "journal",
                &agent_dir.join("resources/context/journal.md")
            ),
            None,
            "agent-authored stores are silent and excluded from the watch set"
        );
        assert_eq!(
            classify(
                agent_dir,
                "decision-log",
                &agent_dir.join("resources/decisions/x.md")
            ),
            None
        );
        assert_eq!(
            classify(agent_dir, "spec", Path::new("/etc/demo/spec.md")),
            Some(CarrierClass::Coalesced)
        );
    }
}
