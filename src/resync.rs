//! Resync events: supervisor-emitted notifications when a live agent's declared resource carriers
//! change on disk ([`06-resync`](../docs/vrs/06-resync/spec.md)).
//!
//! Delivery rides the event-stream machinery through a crate-internal admission for the built-in
//! reserved `resync` stream that exists on every running agent without declaration. Public event
//! ingress remains declaration-gated. Watching is
//! deny-by-default: one non-recursive watch per distinct parent directory of the resolved
//! watchable carriers, so whole-file replacement by rename stays visible through the surviving
//! directory inode. Digest state is seeded silently when a reconcile pass installs a watch set;
//! only a content transition emits.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use notify::Watcher as _;
use sha2::{Digest as _, Sha256};
use agent_spec::profile::{
    ProfileClass, ResourceProfileRefresh, ResourceProfileRegistry,
};
use agent_spec::spec::{AgentSpec, decode_percent_path};

/// The reserved stream used only by the supervisor's crate-internal resync publisher.
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
/// Catalog-observable resync coverage for one declared Resource binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncCoverage {
    Immediate,
    Coalesced,
    Silent,
    Unsupported,
    Inactive,
}

impl ResyncCoverage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Coalesced => "coalesced",
            Self::Silent => "silent",
            Self::Unsupported => "unsupported",
            Self::Inactive => "inactive",
        }
    }

    fn carrier_class(self) -> Option<CarrierClass> {
        match self {
            Self::Immediate => Some(CarrierClass::Immediate),
            Self::Coalesced => Some(CarrierClass::Coalesced),
            Self::Silent | Self::Unsupported | Self::Inactive => None,
        }
    }
}


/// One watchable local carrier: binding label, absolute path, notification class, and an optional
/// host root that must confine every read of a resolver-selected path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchableCarrier {
    pub label: String,
    pub path: PathBuf,
    pub class: CarrierClass,
    pub containment_root: Option<PathBuf>,
}

/// The watchable carriers of one agent, keyed by its declaration path with current routing IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWatchSet {
    pub declaration_path: PathBuf,
    pub bus_id: String,
    pub seat_id: Option<String>,
    pub carriers: Vec<WatchableCarrier>,
}

/// Resolve one spec's watch set: the declaration file plus every active resource binding whose
/// URI denotes a local file (`RESYNC-R01`) — directly (`file://`, catalog-relative) or through a
/// declared resource profile for a scheme URI. Bindings with an inactive reason are skipped;
/// schemes without a local denotation and silent carriers are simply absent. A failing profile
/// resolver is contained: its binding is skipped, the rest of the set survives.
pub fn watch_set_for(
    spec: &AgentSpec,
    this_host: &str,
    profiles: &ResourceProfileRegistry,
) -> AgentWatchSet {
    watch_set_for_in_catalog(spec, std::slice::from_ref(spec), this_host, profiles)
}

/// [`watch_set_for`] with the catalog view a `notify-chain` profile needs to reach the carriers
/// this agent's `supervisor` ancestors declare. Without the other specs, chain carriers cannot be
/// resolved and only the agent's own carriers are produced.
pub fn watch_set_for_in_catalog(
    spec: &AgentSpec,
    specs: &[AgentSpec],
    this_host: &str,
    profiles: &ResourceProfileRegistry,
) -> AgentWatchSet {
    let refresh = profiles.begin_refresh();
    resolve_watch_set(spec, specs, this_host, &refresh).0
}

fn resolve_watch_set(
    spec: &AgentSpec,
    specs: &[AgentSpec],
    this_host: &str,
    profiles: &ResourceProfileRefresh<'_>,
) -> (AgentWatchSet, Vec<String>) {
    let declaration_path = lexical_clean(&spec.path);
    let agent_dir = declaration_path.parent().unwrap_or(Path::new("."));
    let mut carriers = vec![WatchableCarrier {
        label: "declaration".to_owned(),
        path: declaration_path.clone(),
        class: CarrierClass::Immediate,
        containment_root: None,
    }];
    let mut diagnostics = Vec::new();
    for resource in &spec.resources {
        if resource.inactive_reason().is_some() {
            continue;
        }
        // Silent profiles carry no observable transition, so never compile or execute their
        // untrusted resolver merely to discard its result.
        let registered_profile = resource
            .uri()
            .split_once(':')
            .and_then(|(scheme, _)| profiles.get(scheme));
        if registered_profile.is_some_and(|profile| profile.class() == ProfileClass::Silent) {
            continue;
        }
        // Declared profile schemes resolve through their wasm module; the declared class governs
        // notification instead of the local-path defaults.
        match profiles.try_resolve(agent_dir, resource.uri()) {
            Ok(Some(resolution)) => {
                let Some(class) = carrier_class(resolution.class) else {
                    continue;
                };
                carriers.push(WatchableCarrier {
                    label: resource.name().to_owned(),
                    path: resolution.path,
                    class,
                    containment_root: Some(resolution.containment_root),
                });
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                diagnostics.push(format!(
                    "resync profile for {} resource '{}': {error}; binding is unwatchable",
                    spec.bus_id(this_host),
                    resource.name()
                ));
                continue;
            }
        }
        let Some(class) = resource_coverage(agent_dir, resource).carrier_class() else {
            continue;
        };
        let path = resolve_local_path(agent_dir, resource.uri())
            .expect("watchable coverage must have a local path");
        carriers.push(WatchableCarrier {
            label: resource.name().to_owned(),
            path,
            class,
            containment_root: None,
        });
    }
    append_chain_carriers(
        spec,
        specs,
        this_host,
        profiles,
        &mut carriers,
        &mut diagnostics,
    );
    // The supervisor's resolved logical host — not the OS hostname — decides the bus id, so an
    // agent supervised under `st2 up --host <alias>` without an explicit declaration host still
    // produces a recipient `resolve_stream` can resolve.
    (
        AgentWatchSet {
            declaration_path,
            bus_id: spec.bus_id(this_host),
            seat_id: spec.tasks.iter().find(|task| task.name == "agent").map(|task| {
                task.id
                    .clone()
                    .unwrap_or_else(|| format!("{}.{}", spec.bus_id(this_host), task.name))
            }),
            carriers,
        },
        diagnostics,
    )
}

/// A URI denotes a local file when it is an absolute, authority-free `file://` URI or a
/// scheme-less catalog-relative path resolved against the agent directory. URI scheme syntax is
/// parsed before the `file` scheme name is matched ASCII-case-insensitively, as required by RFC
/// 3986. Unsupported file URI authorities, query/fragment components,
/// malformed escapes, encoded path separators, and encoded parent components have no local
/// denotation.
/// Carriers this agent's `supervisor` ancestors declare through a `notify-chain` profile.
///
/// A profile whose layers compose along the supervisor edge leaves every descendant's effective
/// view dependent on carriers the descendant does not own. Resync notifies a carrier's owner, so
/// without this the descendant is never told its view changed.
///
/// The walk deliberately reuses each ancestor's OWN declared URI rather than synthesizing one:
/// st2 does not own any profile's URI grammar, and a resolver is free to ignore the authority
/// component entirely, so a synthesized subject would be a guess. Resolving the ancestor's
/// declaration against the ancestor's directory is the identical call the ancestor's own
/// subscription makes, which is what keeps containment unchanged — the guest is still only ever
/// asked to resolve one agent's URI against that agent's own directory.
///
/// Matching is by profile scheme, never by binding label: labels are agent-local and replaceable,
/// so keying on them would silently drop a layer whose owner renamed its binding.
fn append_chain_carriers(
    spec: &AgentSpec,
    specs: &[AgentSpec],
    this_host: &str,
    profiles: &ResourceProfileRefresh<'_>,
    carriers: &mut Vec<WatchableCarrier>,
    diagnostics: &mut Vec<String>,
) {
    let chain_schemes: Vec<&str> = spec
        .resources
        .iter()
        .filter(|resource| resource.inactive_reason().is_none())
        .filter_map(|resource| resource.uri().split_once(':').map(|(scheme, _)| scheme))
        .filter(|scheme| profiles.get(scheme).is_some_and(|p| p.notify_chain()))
        .collect();
    if chain_schemes.is_empty() {
        return;
    }

    let ancestors = match crate::supervisor_chain::ancestors(specs, spec, this_host) {
        Ok(ancestors) => ancestors,
        Err(error) => {
            diagnostics.push(format!(
                "resync notify-chain for {}: supervisor chain is unwalkable ({error:?}); \
                 ancestor carriers are unwatchable",
                spec.bus_id(this_host)
            ));
            return;
        }
    };

    for ancestor in ancestors {
        // Skip and continue, never sever: a retired ancestor contributes no layer, but its own
        // ancestors still do. `is_retired` normalizes both declaration spellings.
        if ancestor.desired_state.is_retired() {
            continue;
        }
        let ancestor_declaration = lexical_clean(&ancestor.path);
        let ancestor_dir = ancestor_declaration.parent().unwrap_or(Path::new("."));
        let ancestor_bus_id = ancestor.bus_id(this_host);
        for resource in &ancestor.resources {
            if resource.inactive_reason().is_some() {
                continue;
            }
            let Some((scheme, _)) = resource.uri().split_once(':') else {
                continue;
            };
            if !chain_schemes.contains(&scheme) {
                continue;
            }
            match profiles.try_resolve(ancestor_dir, resource.uri()) {
                Ok(Some(resolution)) => {
                    let Some(class) = carrier_class(resolution.class) else {
                        continue;
                    };
                    carriers.push(WatchableCarrier {
                        // Qualifying by owner keeps each ancestor's layer on its own supersession
                        // key, so a burst on one ancestor cannot collapse another's event.
                        label: format!("{}@{ancestor_bus_id}", resource.name()),
                        path: resolution.path,
                        class,
                        containment_root: Some(resolution.containment_root),
                    });
                }
                Ok(None) => {}
                Err(error) => diagnostics.push(format!(
                    "resync notify-chain for {}: ancestor {ancestor_bus_id} resource '{}': \
                     {error}; that ancestor layer is unwatchable",
                    spec.bus_id(this_host),
                    resource.name()
                )),
            }
        }
    }
}

fn resolve_local_path(agent_dir: &Path, uri: &str) -> Option<PathBuf> {
    if let Some((scheme, scheme_specific)) = uri.split_once(':').filter(|(scheme, _)| {
        !scheme.is_empty()
            && !scheme.contains('/')
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    }) {
        if !scheme.eq_ignore_ascii_case("file") {
            return None;
        }
        let encoded_path = scheme_specific.strip_prefix("//")?;
        if !encoded_path.starts_with('/')
            || encoded_path.starts_with("//")
            || encoded_path.contains(['?', '#'])
        {
            return None;
        }
        let path = PathBuf::from(decode_percent_path(encoded_path).ok()?);
        return path.is_absolute().then(|| lexical_clean(&path));
    }
    let path = PathBuf::from(decode_percent_path(uri).ok()?);
    Some(lexical_clean(&agent_dir.join(path)))
}
/// Resolve the externally visible resync coverage for one Resource binding.
pub fn resource_coverage(agent_dir: &Path, resource: &agent_spec::spec::Resource) -> ResyncCoverage {
    if resource.inactive_reason().is_some() {
        return ResyncCoverage::Inactive;
    }
    let Some(path) = resolve_local_path(agent_dir, resource.uri()) else {
        return ResyncCoverage::Unsupported;
    };
    match classify(agent_dir, resource.name(), &path) {
        Some(CarrierClass::Immediate) => ResyncCoverage::Immediate,
        Some(CarrierClass::Coalesced) => ResyncCoverage::Coalesced,
        None => ResyncCoverage::Silent,
    }
}


/// Remove `.` and `..` components lexically. This deliberately does not inspect the filesystem:
/// classification follows the authored path structure without resolving symlinks.
fn lexical_clean(path: &Path) -> PathBuf {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                clean.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                let can_pop = matches!(clean.components().next_back(), Some(Component::Normal(_)));
                if can_pop {
                    clean.pop();
                } else if !clean.has_root() {
                    clean.push(component.as_os_str());
                }
            }
        }
    }
    clean
}

/// Class defaults for carriers resolved WITHOUT a declared profile (`RESYNC-R04`): goal carriers
/// are immediate; stores the agent itself authors are silent (None); everything else is coalesced.
/// The declaration carrier is immediate by construction in [`watch_set_for`]. Profile-resolved
/// carriers skip this sniffing entirely — their class is what the catalog declares.
fn classify(agent_dir: &Path, binding_name: &str, normalized_path: &Path) -> Option<CarrierClass> {
    let agent_relative = normalized_path.strip_prefix(agent_dir).ok();
    let authored_store = agent_relative.is_some_and(|rel| {
        rel.starts_with("resources/context")
            || rel.starts_with("resources/decisions")
            || rel.starts_with("resources/friction")
    });
    if authored_store {
        return None;
    }
    let goal =
        binding_name == "goal" || normalized_path.file_name().is_some_and(|n| n == "goal.md");
    Some(if goal {
        CarrierClass::Immediate
    } else {
        CarrierClass::Coalesced
    })
}

/// A declared profile class maps onto carrier notification: silent profiles are excluded from
/// the watch set exactly like sniffed agent-authored stores.
fn carrier_class(class: ProfileClass) -> Option<CarrierClass> {
    match class {
        ProfileClass::Immediate => Some(CarrierClass::Immediate),
        ProfileClass::Coalesced => Some(CarrierClass::Coalesced),
        ProfileClass::Silent => None,
    }
}

/// Resolve coverage with the catalog's declared profile registry. Silent profiles report their
/// declared class without executing a guest; other registered schemes are watchable only when
/// their resolver succeeds.
pub fn resource_coverage_with_profiles(
    agent_dir: &Path,
    resource: &agent_spec::spec::Resource,
    profiles: &ResourceProfileRefresh<'_>,
) -> ResyncCoverage {
    if resource.inactive_reason().is_some() {
        return ResyncCoverage::Inactive;
    }
    let registered = resource
        .uri()
        .split_once(':')
        .and_then(|(scheme, _)| profiles.get(scheme));
    let Some(profile) = registered else {
        return resource_coverage(agent_dir, resource);
    };
    if profile.class() == ProfileClass::Silent {
        return ResyncCoverage::Silent;
    }
    match profiles.try_resolve(agent_dir, resource.uri()) {
        Ok(Some(resolution)) => match resolution.class {
            ProfileClass::Immediate => ResyncCoverage::Immediate,
            ProfileClass::Coalesced => ResyncCoverage::Coalesced,
            ProfileClass::Silent => ResyncCoverage::Silent,
        },
        Ok(None) | Err(_) => ResyncCoverage::Unsupported,
    }
}

// ---- Supervisor side ---------------------------------------------------------------------------

struct WatchRefresh {
    sets: Vec<AgentWatchSet>,
    malformed_declarations: BTreeSet<PathBuf>,
    live_task_ids: BTreeSet<String>,
}

enum Msg {
    WatchSet(WatchRefresh),
    Install(AgentWatchSet, Sender<()>),
    Deactivate(String, Sender<()>),
    Mutations(Vec<PathBuf>),
    Rescan,
    /// Explicit stop: the worker's own watcher holds the last `Sender`, so `Disconnected`
    /// would stay unreachable while `join` waits.
    Shutdown,
}

/// Handle for the resync worker thread. Dropping it disconnects the mailbox and joins the worker.
pub struct ResyncSupervisor {
    tx: Option<Sender<Msg>>,
    handle: Option<JoinHandle<()>>,
    /// Scheme resolution semantics for resource URIs. Definitions update under the same lock as
    /// watch-set construction, while the registry retains its bounded compiled-module cache.
    profiles: std::sync::Mutex<ResourceProfileRegistry>,
}

impl ResyncSupervisor {
    /// Spawn the worker over the built-in profile set. Watch installation itself is best-effort
    /// per refresh; a failure degrades to timer-driven digest polling over the watch set rather
    /// than losing the capability.
    pub fn spawn(root: PathBuf, this_host: String) -> Self {
        Self::with_profiles(root, this_host, ResourceProfileRegistry::builtin())
    }

    /// Spawn with an explicit resource-profile registry — the injection point the `up` loop uses
    /// to hand over the catalog's declared profiles.
    pub fn with_profiles(
        root: PathBuf,
        this_host: String,
        profiles: ResourceProfileRegistry,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
        let forward = tx.clone();
        let handle = std::thread::Builder::new()
            .name("resync".to_owned())
            .spawn(move || worker_loop(root, this_host, rx, forward))
            .ok();
        Self {
            tx: Some(tx),
            handle,
            profiles: std::sync::Mutex::new(profiles),
        }
    }
    /// Replace the worker's active watch set using two deliberately distinct catalog views.
    ///
    /// `catalog_specs` contains every valid discovered declaration and is used only to resolve
    /// topology such as supervisor chains. `live_subscription_specs` contains canonical seats
    /// proven live by this pass and is the only source of active subscriptions. A malformed
    /// declaration retains its prior subscription only while its canonical seat is observed alive;
    /// contained profile failures reach the reconcile report.
    #[must_use = "resolver diagnostics must be surfaced by the reconcile caller"]
    pub fn refresh(
        &self,
        catalog_specs: &[AgentSpec],
        live_subscription_specs: &[AgentSpec],
        this_host: &str,
        sessions: &[crate::reconcile::Session],
        malformed_declarations: &[PathBuf],
    ) -> Vec<String> {
        let profiles = self
            .profiles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.refresh_with_registry(
            &profiles,
            catalog_specs,
            live_subscription_specs,
            this_host,
            sessions,
            malformed_declarations,
        )
    }

    /// Atomically replace the profile definitions and the watch set for one reconcile pass.
    ///
    /// Replacement keeps the supervisor's bounded compiled-module cache but gives this pass a
    /// fresh module-snapshot scope shared by all bindings.
    #[must_use = "resolver diagnostics must be surfaced by the reconcile caller"]
    pub fn refresh_with_profiles(
        &self,
        profiles: ResourceProfileRegistry,
        catalog_specs: &[AgentSpec],
        live_subscription_specs: &[AgentSpec],
        this_host: &str,
        sessions: &[crate::reconcile::Session],
        malformed_declarations: &[PathBuf],
    ) -> Vec<String> {
        let mut current = self
            .profiles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.replace_definitions(profiles);
        self.refresh_with_registry(
            &current,
            catalog_specs,
            live_subscription_specs,
            this_host,
            sessions,
            malformed_declarations,
        )
    }

    fn refresh_with_registry(
        &self,
        profiles: &ResourceProfileRegistry,
        catalog_specs: &[AgentSpec],
        live_subscription_specs: &[AgentSpec],
        this_host: &str,
        sessions: &[crate::reconcile::Session],
        malformed_declarations: &[PathBuf],
    ) -> Vec<String> {
        let mut diagnostics = Vec::new();
        let refresh_profiles = profiles.begin_refresh();
        let sets = live_subscription_specs
            .iter()
            .filter(|spec| spec.resolved_host(this_host) == this_host)
            .filter(|spec| spec.desired_state.is_running())
            .map(|spec| {
                let (set, mut failures) =
                    resolve_watch_set(spec, catalog_specs, this_host, &refresh_profiles);
                diagnostics.append(&mut failures);
                set
            })
            .collect();
        let refresh = WatchRefresh {
            sets,
            malformed_declarations: malformed_declarations
                .iter()
                .map(|path| lexical_clean(path))
                .collect(),
            live_task_ids: sessions
                .iter()
                .filter(|session| session.alive)
                .map(|session| session.pty_id.clone())
                .collect(),
        };
        if let Some(tx) = &self.tx {
            let _ = tx.send(Msg::WatchSet(refresh));
        }
        diagnostics
    }

    /// Synchronously install one newly proven-live canonical seat before reconciliation advances
    /// to another launch target. The acknowledgement closes the gap between a successful spawn and
    /// the worker's silent baseline seed; later full refreshes still own removals and malformed
    /// declaration retention. Profile and supervisor-chain resolution use the supervisor's current
    /// registry and the pass's complete catalog view, and return contained resolver failures for the
    /// reconcile report.
    pub fn install_live(
        &self,
        spec: &AgentSpec,
        specs: &[AgentSpec],
        this_host: &str,
    ) -> Vec<String> {
        if spec.resolved_host(this_host) != this_host || !spec.desired_state.is_running() {
            return Vec::new();
        }
        let (set, diagnostics) = {
            let profiles = self
                .profiles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            resolve_watch_set(spec, specs, this_host, &profiles.begin_refresh())
        };
        let (ack_tx, ack_rx) = channel();
        if self
            .tx
            .as_ref()
            .is_some_and(|tx| tx.send(Msg::Install(set, ack_tx)).is_ok())
        {
            let _ = ack_rx.recv();
        }
        diagnostics
    }

    /// Synchronously remove a canonical seat's active subscriptions before relaunch work begins.
    /// Sequence floors remain retained so a later successful install cannot reuse an occurrence.
    pub fn deactivate(&self, spec: &AgentSpec, this_host: &str) {
        let (ack_tx, ack_rx) = channel();
        if self
            .tx
            .as_ref()
            .is_some_and(|tx| tx.send(Msg::Deactivate(spec.bus_id(this_host), ack_tx)).is_ok())
        {
            let _ = ack_rx.recv();
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum CarrierState {
    Present(String),
    Missing,
}

impl CarrierState {
    fn render(&self) -> &str {
        match self {
            Self::Present(digest) => digest,
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingTransition {
    binding: String,
    path: PathBuf,
    old_state: CarrierState,
    new_state: CarrierState,
    body: String,
    event_id: String,
}

impl PendingTransition {
    fn new(
        binding: &str,
        path: &Path,
        old_state: &CarrierState,
        new_state: &CarrierState,
        incarnation: crate::event::StreamOwnerIncarnation,
        sequence: u64,
    ) -> Self {
        let occurrence = incarnation.occurrence_token(sequence);
        let body = render_body(binding, path, old_state, new_state, &occurrence);
        let event_id = transition_identity(&body);
        Self {
            binding: binding.to_owned(),
            path: path.to_path_buf(),
            old_state: old_state.clone(),
            new_state: new_state.clone(),
            body,
            event_id,
        }
    }
}

struct Entry {
    bus_id: String,
    seat_id: Option<String>,
    label: String,
    class: CarrierClass,
    containment_root: Option<PathBuf>,
    state: Option<CarrierState>,
    /// Last occurrence sequence reserved by this retained subscription. Sequence zero is the
    /// silent seeded state; only capturing a new immutable transition advances it.
    occurrence_sequence: u64,
    /// Immutable publication snapshot retained after a failed emit. Metadata refresh may change
    /// the current route, path, or class, but a reserved event identity must keep its exact bytes.
    pending_transition: Option<PendingTransition>,
    dirty: bool,
}
/// Recipient-scoped subscription identity. A declaration or carrier may move without changing
/// the event deduplication namespace, while a bus-id change intentionally starts a new namespace.
type SubscriptionIdentity = (String, String);

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
    /// Last sequence reserved for each recipient/binding identity seen by this supervisor.
    /// Inactive subscriptions stay here so a reinstall cannot collide with an earlier occurrence.
    /// Its lifetime is the worker's and its cardinality is bounded by identities observed there.
    subscription_sequences: BTreeMap<SubscriptionIdentity, u64>,
    deadlines: BTreeMap<CarrierClass, Instant>,
    watched: BTreeMap<PathBuf, Option<DirIdentity>>,
    /// `None` degrades to digest polling at refresh cadence (each reconcile pass) instead of
    /// evented watching — diagnosed once by the absence of immediacy, never a hard error.
    watcher: Option<notify::RecommendedWatcher>,
}

fn forward_watch_result(forward: &Sender<Msg>, result: notify::Result<notify::Event>) {
    match result {
        Ok(event) if is_mutation(&event) => {
            let _ = forward.send(Msg::Mutations(event.paths));
        }
        Ok(_) => {}
        Err(_) => {
            // A backend error can mean events were dropped. Re-read every subscribed carrier:
            // digest equality suppresses false positives while changed bytes still notify.
            let _ = forward.send(Msg::Rescan);
        }
    }
}

fn make_watcher(forward: Sender<Msg>) -> Option<notify::RecommendedWatcher> {
    notify::RecommendedWatcher::new(
        move |result| forward_watch_result(&forward, result),
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
        subscription_sequences: BTreeMap::new(),
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
            Ok(Msg::WatchSet(refresh)) => worker.apply_watch_sets(refresh),
            Ok(Msg::Install(set, ack)) => {
                worker.install_watch_set(set);
                let _ = ack.send(());
            }
            Ok(Msg::Deactivate(bus_id, ack)) => {
                worker.deactivate_watch_set(&bus_id);
                let _ = ack.send(());
            }
            Ok(Msg::Mutations(paths)) => worker.mark_mutated(paths),
            Ok(Msg::Rescan) => worker.rescan_all(),
            Ok(Msg::Shutdown) => break,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        worker.flush_due(Instant::now());
    }
}

fn take_retained_entry(
    previous: &mut BTreeMap<PathBuf, Vec<Entry>>,
    bus_id: &str,
    label: &str,
) -> Option<Entry> {
    for entries in previous.values_mut() {
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.bus_id == bus_id && entry.label == label)
        {
            return Some(entries.remove(index));
        }
    }
    None
}

fn rebuild_carriers(
    mut previous: BTreeMap<PathBuf, Vec<Entry>>,
    refresh: WatchRefresh,
    subscription_sequences: &BTreeMap<SubscriptionIdentity, u64>,
) -> BTreeMap<PathBuf, Vec<Entry>> {
    let mut next: BTreeMap<PathBuf, Vec<Entry>> = BTreeMap::new();
    for set in refresh.sets {
        for carrier in set.carriers {
            // The canonical recipient and binding label identify one subscription across
            // declaration and carrier relocation. Rebuild every retained entry from the current
            // declaration while carrying its baseline and immutable delivery snapshot. Looking
            // across path buckets also lets a binding's re-resolved path/class/containment
            // metadata become current. A bus-id change intentionally seeds a new
            // recipient-scoped namespace.
            let identity = (set.bus_id.clone(), carrier.label.clone());
            let retained = take_retained_entry(&mut previous, &set.bus_id, &carrier.label);
            let (state, occurrence_sequence, pending_transition, dirty) = retained.map_or_else(
                || {
                    let (state, dirty) =
                        match read_state(&carrier.path, carrier.containment_root.as_deref()) {
                            Ok(state) => (Some(state), false),
                            Err(error) => {
                                diagnose_read_error(&carrier.path, &error);
                                (None, true)
                            }
                        };
                    (
                        state,
                        subscription_sequences.get(&identity).copied().unwrap_or(0),
                        None,
                        dirty,
                    )
                },
                |entry| {
                    (
                        entry.state,
                        entry.occurrence_sequence,
                        entry.pending_transition,
                        entry.dirty,
                    )
                },
            );
            let entry = Entry {
                bus_id: set.bus_id.clone(),
                seat_id: set.seat_id.clone(),
                label: carrier.label.clone(),
                class: carrier.class,
                containment_root: carrier.containment_root.clone(),
                state,
                occurrence_sequence,
                pending_transition,
                dirty,
            };
            next.entry(carrier.path).or_default().push(entry);
        }
    }
    // Strict discovery omits a malformed declaration entirely. Preserve only its declaration
    // subscription, and only when the exact canonical seat task is still observed alive.
    for (path, entries) in previous {
        if !refresh.malformed_declarations.contains(&path) {
            continue;
        }
        let retained = entries
            .into_iter()
            .filter(|entry| {
                entry.label == "declaration"
                    && entry
                        .seat_id
                        .as_ref()
                        .is_some_and(|seat_id| refresh.live_task_ids.contains(seat_id))
            })
            .collect::<Vec<_>>();
        if !retained.is_empty() {
            next.entry(path).or_default().extend(retained);
        }
    }
    next
}

impl Worker {
    fn prepare_carrier_update(
        &mut self,
        previous: &BTreeMap<PathBuf, Vec<Entry>>,
    ) -> BTreeMap<SubscriptionIdentity, PathBuf> {
        // The active entries can disappear entirely while an agent is suspended. Retain only the
        // scalar sequence floor per recipient/binding identity for this supervisor lifetime;
        // watches and carrier baselines remain exclusively in the active carrier map.
        for entry in previous.values().flatten() {
            let identity = (entry.bus_id.clone(), entry.label.clone());
            self.subscription_sequences
                .entry(identity)
                .and_modify(|sequence| *sequence = (*sequence).max(entry.occurrence_sequence))
                .or_insert(entry.occurrence_sequence);
        }
        previous
            .iter()
            .flat_map(|(path, entries)| {
                entries.iter().map(|entry| {
                    (
                        (entry.bus_id.clone(), entry.label.clone()),
                        path.clone(),
                    )
                })
            })
            .collect()
    }

    fn finish_carrier_update(
        &mut self,
        previous_paths: &BTreeMap<SubscriptionIdentity, PathBuf>,
    ) {
        let rebound_paths = self
            .carriers
            .iter()
            .flat_map(|(path, entries)| {
                entries.iter().filter_map(move |entry| {
                    previous_paths
                        .get(&(entry.bus_id.clone(), entry.label.clone()))
                        .filter(|previous_path| *previous_path != path)
                        .map(|_| path.clone())
                })
            })
            .collect::<BTreeSet<_>>();

        // A retained dirty entry may move notification class during metadata refresh. Remove a
        // deadline only when no dirty subscriber remains in that class, and schedule every newly
        // represented class so no dirty transition is stranded under its old class deadline.
        self.reconcile_dirty_deadlines(Instant::now());

        // Rebindings within an already watched parent produce no filesystem event. Diff their new
        // paths explicitly before watch refresh so the old baseline transitions immediately.
        self.poll_paths(rebound_paths.into_iter().collect());
        // Diff paths that were blind before registering newly recovered parents; otherwise the
        // new watch suppresses polling of mutations that happened during the blind interval.
        self.poll_unwatched();
        let registered = self.refresh_watches();
        // Registration closes the event gap first; this second digest pass covers writes between
        // the pre-registration poll and watch installation.
        self.poll_registered(&registered);
    }

    fn apply_watch_sets(&mut self, refresh: WatchRefresh) {
        let previous = std::mem::take(&mut self.carriers);
        let previous_paths = self.prepare_carrier_update(&previous);
        self.carriers = rebuild_carriers(previous, refresh, &self.subscription_sequences);
        self.finish_carrier_update(&previous_paths);
    }

    fn install_watch_set(&mut self, set: AgentWatchSet) {
        let replaced_bus_id = set.bus_id.clone();
        let previous = std::mem::take(&mut self.carriers);
        let previous_paths = self.prepare_carrier_update(&previous);
        let mut retained = BTreeMap::<PathBuf, Vec<Entry>>::new();
        let mut unaffected = BTreeMap::<PathBuf, Vec<Entry>>::new();
        for (path, entries) in previous {
            let (matching, other): (Vec<_>, Vec<_>) = entries
                .into_iter()
                .partition(|entry| entry.bus_id == replaced_bus_id);
            if !matching.is_empty() {
                retained.insert(path.clone(), matching);
            }
            if !other.is_empty() {
                unaffected.insert(path, other);
            }
        }
        let replacement = rebuild_carriers(
            retained,
            WatchRefresh {
                sets: vec![set],
                malformed_declarations: BTreeSet::new(),
                live_task_ids: BTreeSet::new(),
            },
            &self.subscription_sequences,
        );
        for (path, entries) in replacement {
            unaffected.entry(path).or_default().extend(entries);
        }
        self.carriers = unaffected;
        self.finish_carrier_update(&previous_paths);
    }

    fn deactivate_watch_set(&mut self, bus_id: &str) {
        let previous = std::mem::take(&mut self.carriers);
        let previous_paths = self.prepare_carrier_update(&previous);
        self.carriers = previous
            .into_iter()
            .filter_map(|(path, entries)| {
                let retained = entries
                    .into_iter()
                    .filter(|entry| entry.bus_id != bus_id)
                    .collect::<Vec<_>>();
                (!retained.is_empty()).then_some((path, retained))
            })
            .collect();
        self.finish_carrier_update(&previous_paths);
    }

    fn reconcile_dirty_deadlines(&mut self, now: Instant) {
        let dirty_classes = self
            .carriers
            .values()
            .flatten()
            .filter(|entry| entry.dirty)
            .map(|entry| entry.class)
            .collect::<BTreeSet<_>>();
        self.deadlines
            .retain(|class, _| dirty_classes.contains(class));
        for class in dirty_classes {
            self.deadlines
                .entry(class)
                .or_insert_with(|| now + class.window());
        }
    }

    /// Polling observes transitions through the same class deadlines as filesystem events. It
    /// never emits directly: this preserves coalescing and lets `flush_path` replay pending
    /// transitions before considering newer carrier state.
    fn poll_paths(&mut self, paths: Vec<PathBuf>) {
        let now = Instant::now();
        for path in paths {
            let Some(entries) = self.carriers.get_mut(&path) else {
                continue;
            };
            for entry in entries {
                let changed = entry.pending_transition.is_some()
                    || match read_state(&path, entry.containment_root.as_deref()) {
                        Ok(observed) => entry.state.as_ref() != Some(&observed),
                        Err(error) => {
                            diagnose_read_error(&path, &error);
                            true
                        }
                    };
                if !changed {
                    continue;
                }
                if !entry.dirty {
                    let deadline = now + entry.class.window();
                    self.deadlines
                        .entry(entry.class)
                        .and_modify(|existing| *existing = (*existing).min(deadline))
                        .or_insert(deadline);
                }
                entry.dirty = true;
            }
        }
    }

    /// Poll carriers whose parent directory carries no registered watch right now.
    fn poll_unwatched(&mut self) {
        let unwatched = self
            .carriers
            .keys()
            .filter(|path| {
                !self
                    .watched
                    .contains_key(path.parent().unwrap_or(Path::new(".")))
            })
            .cloned()
            .collect();
        self.poll_paths(unwatched);
    }

    fn poll_registered(&mut self, directories: &[PathBuf]) {
        let paths = self
            .carriers
            .keys()
            .filter(|path| {
                path.parent()
                    .is_some_and(|parent| directories.iter().any(|dir| dir == parent))
            })
            .cloned()
            .collect();
        self.poll_paths(paths);
    }

    fn rescan_all(&mut self) {
        self.poll_paths(self.carriers.keys().cloned().collect());
    }

    /// Re-register watches so every distinct parent directory of the current watch set is covered,
    /// dropping directories that left the set or were replaced (identity change). A replaced watch
    /// stays blind until the next pass rebuilds it — bounded by the reconcile interval, the same
    /// tradeoff `CatalogDeclarationWatcher` accepts for declarations.
    fn refresh_watches(&mut self) -> Vec<PathBuf> {
        let mut registered = Vec::new();
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
                registered.push(dir.clone());
                self.watched.insert(dir, identity);
            }
        }
        registered
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
            let registered = self.refresh_watches();
            self.poll_registered(&registered);
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
                self.flush_path(&path, Some(class));
            }
        }
    }

    /// Flush subscribers of one path whose class is due, or every subscriber for fallback polls:
    /// diff carrier states, emit transitions, and retain failed publications for retry with the
    /// same event identity.
    fn flush_path(&mut self, path: &Path, due_class: Option<CarrierClass>) {
        let occurrence_incarnation =
            crate::event::current_stream_owner_incarnation(&self.root, &self.this_host).ok();
        let Some(entries) = self.carriers.get_mut(path) else {
            return;
        };
        let mut retries = Vec::new();
        for entry in entries.iter_mut() {
            if due_class.is_some_and(|class| entry.class != class) {
                continue;
            }
            entry.dirty = false;
            let observed = read_state(path, entry.containment_root.as_deref());

            if let Some(pending) = entry.pending_transition.as_ref() {
                if emit_resync(&self.root, &self.this_host, &entry.bus_id, pending) {
                    let completed = entry
                        .pending_transition
                        .take()
                        .expect("pending transition was just observed");
                    entry.state = Some(completed.new_state);
                    // The current carrier may have advanced or rebound while the immutable
                    // transition was pending. Complete it first, then schedule current state.
                    match observed {
                        Ok(observed) if entry.state.as_ref() != Some(&observed) => {
                            entry.dirty = true;
                            retries.push(entry.class);
                        }
                        Err(error) => {
                            diagnose_read_error(path, &error);
                            entry.dirty = true;
                            retries.push(entry.class);
                        }
                        Ok(_) => {}
                    }
                } else {
                    if let Err(error) = observed {
                        diagnose_read_error(path, &error);
                    }
                    entry.dirty = true;
                    retries.push(entry.class);
                }
                continue;
            }

            let target_state = match observed {
                Ok(state) => state,
                Err(error) => {
                    diagnose_read_error(path, &error);
                    entry.dirty = true;
                    retries.push(entry.class);
                    continue;
                }
            };
            let Some(old_state) = entry.state.as_ref() else {
                // A subscription first observed during a transient read failure has no proven
                // baseline. Seed the first successful observation silently.
                entry.state = Some(target_state);
                continue;
            };
            if old_state == &target_state {
                continue;
            }
            let (Some(incarnation), Some(sequence)) = (
                occurrence_incarnation,
                entry.occurrence_sequence.checked_add(1),
            ) else {
                // An event cannot reserve a stable occurrence without a current owner incarnation
                // or after sequence exhaustion. Keep the transition uncaptured and retry later.
                entry.dirty = true;
                retries.push(entry.class);
                continue;
            };
            let transition = PendingTransition::new(
                &entry.label,
                path,
                old_state,
                &target_state,
                incarnation,
                sequence,
            );
            entry.occurrence_sequence = sequence;
            if emit_resync(&self.root, &self.this_host, &entry.bus_id, &transition) {
                entry.state = Some(target_state);
            } else {
                entry.pending_transition = Some(transition);
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
/// different bindings, paths, digest transitions, or captured occurrences.
fn transition_identity(body: &str) -> String {
    format!("{:x}", Sha256::digest(body.as_bytes()))
}

/// One superseded resync event through the supervisor-only built-in admission (`RESYNC-R06`).
fn emit_resync(
    root: &Path,
    this_host: &str,
    bus_id: &str,
    transition: &PendingTransition,
) -> bool {
    let subject = format!("resource {} changed", transition.binding);
    match crate::event::emit_builtin_resync(
        root,
        this_host,
        bus_id,
        &transition.event_id,
        Some(&transition.binding),
        Some(subject.as_str()),
        &transition.body,
        true,
    ) {
        Ok(_) => true,
        Err(error) => {
            eprintln!(
                "st2: resync emit for '{}' failed: {error:#}",
                transition.path.display()
            );
            false
        }
    }
}

fn read_state(path: &Path, containment_root: Option<&Path>) -> std::io::Result<CarrierState> {
    match containment_root {
        Some(root) => read_confined(path, root),
        None => read_regular(path),
    }
}

fn diagnose_read_error(path: &Path, error: &std::io::Error) {
    eprintln!(
        "st2: resync read for '{}' failed transiently; retrying: {error}",
        path.display()
    );
}

fn hash_reader(mut file: std::fs::File) -> std::io::Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(unix)]
fn classify_open_error(error: std::io::Error) -> std::io::Result<CarrierState> {
    match error.raw_os_error() {
        Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP) => Ok(CarrierState::Missing),
        _ => Err(error),
    }
}

#[cfg(not(unix))]
fn classify_open_error(error: std::io::Error) -> std::io::Result<CarrierState> {
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(CarrierState::Missing)
    } else {
        Err(error)
    }
}

#[cfg(unix)]
fn read_regular(path: &Path) -> std::io::Result<CarrierState> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) => return classify_open_error(error),
    };
    if !file.metadata()?.file_type().is_file() {
        return Ok(CarrierState::Missing);
    }
    hash_reader(file).map(CarrierState::Present)
}

#[cfg(not(unix))]
fn read_regular(path: &Path) -> std::io::Result<CarrierState> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) => return classify_open_error(error),
    };
    if !file.metadata()?.file_type().is_file() {
        return Ok(CarrierState::Missing);
    }
    hash_reader(file).map(CarrierState::Present)
}

#[cfg(unix)]
fn read_confined(path: &Path, root: &Path) -> std::io::Result<CarrierState> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;

    let invalid_path = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "carrier path is outside its confinement root or has an unsafe component",
        )
    };
    let relative = path.strip_prefix(root).map_err(|_| invalid_path())?;
    let mut components = relative.components().peekable();
    if components.peek().is_none() {
        return Ok(CarrierState::Missing);
    }

    // Open the confinement root component-by-component from the filesystem root. `O_NOFOLLOW`
    // on one full pathname protects only its final component; descriptor-relative traversal
    // protects every ancestor from symlink replacement as well.
    let mut root_components = root.components();
    if root_components.next() != Some(std::path::Component::RootDir) {
        return Err(invalid_path());
    }
    let slash = CString::new("/").map_err(|_| invalid_path())?;
    // SAFETY: `slash` is NUL-terminated and the returned descriptor is checked before ownership.
    let filesystem_root = unsafe {
        libc::open(
            slash.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if filesystem_root < 0 {
        return classify_open_error(std::io::Error::last_os_error());
    }
    // SAFETY: `filesystem_root` is newly owned after the non-negative check.
    let mut directory = unsafe { OwnedFd::from_raw_fd(filesystem_root) };
    for component in root_components {
        let std::path::Component::Normal(name) = component else {
            return Err(invalid_path());
        };
        let name = CString::new(name.as_bytes()).map_err(|_| invalid_path())?;
        let flags = libc::O_RDONLY
            | libc::O_DIRECTORY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK;
        // SAFETY: the live directory descriptor and NUL-terminated component are valid.
        let opened = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if opened < 0 {
            return classify_open_error(std::io::Error::last_os_error());
        }
        // SAFETY: `opened` is newly owned after the non-negative check.
        directory = unsafe { OwnedFd::from_raw_fd(opened) };
    }

    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(invalid_path());
        };
        let name = CString::new(name.as_bytes()).map_err(|_| invalid_path())?;
        let last = components.peek().is_none();
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if last { 0 } else { libc::O_DIRECTORY };
        // SAFETY: both the live directory descriptor and NUL-terminated component are valid;
        // `O_NOFOLLOW` makes each lookup fail closed if that component is replaced by a symlink.
        let opened = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if opened < 0 {
            return classify_open_error(std::io::Error::last_os_error());
        }
        // SAFETY: `opened` is a newly-owned descriptor after the non-negative check above.
        let opened = unsafe { OwnedFd::from_raw_fd(opened) };
        if last {
            let file = std::fs::File::from(opened);
            if !file.metadata()?.file_type().is_file() {
                return Ok(CarrierState::Missing);
            }
            return hash_reader(file).map(CarrierState::Present);
        }
        directory = opened;
    }
    Ok(CarrierState::Missing)
}

#[cfg(not(unix))]
fn read_confined(_path: &Path, _root: &Path) -> std::io::Result<CarrierState> {
    // No std API can atomically enforce no-follow traversal. Fail closed on unsupported hosts.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-follow reads are unavailable",
    ))
}

fn render_body(
    label: &str,
    path: &Path,
    old: &CarrierState,
    new: &CarrierState,
    occurrence: &str,
) -> String {
    format!(
        "resource `{label}` changed\n\nbinding: {label}\npath: {}\nold: {}\nnew: {}\noccurrence: {occurrence}\n",
        path.display(),
        old.render(),
        new.render(),
    )
}
#[cfg(test)]
mod tests {
    use super::*;

    fn refresh_for(sets: Vec<AgentWatchSet>) -> WatchRefresh {
        WatchRefresh {
            sets,
            malformed_declarations: BTreeSet::new(),
            live_task_ids: BTreeSet::new(),
        }
    }

    fn owner_incarnation(seed: u64) -> crate::event::StreamOwnerIncarnation {
        crate::event::StreamOwnerIncarnation::for_test(seed, seed + 1, 42, seed + 2)
    }

    fn discover(catalog: &Path) -> AgentSpec {
        let found = crate::discover_strict(catalog);
        eprintln!("discovery errors: {:?}", found.errors);
        found.specs.into_iter().next().unwrap()
    }

    fn resync_inbox_event(agent_dir: &Path) -> String {
        std::fs::read_dir(agent_dir.join("resources/inbox"))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
            .find(|event| event.lines().any(|line| line == "stream: resync"))
            .expect("current resync inbox event")
    }
    fn resync_inbox_events(agent_dir: &Path) -> Vec<String> {
        std::fs::read_dir(agent_dir.join("resources/inbox"))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
            .filter(|event| event.lines().any(|line| line == "stream: resync"))
            .collect()
    }

    fn event_field(event: &str, field: &str) -> String {
        event
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}: ")))
            .unwrap_or_else(|| panic!("missing {field} in event"))
            .to_owned()
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
  resource "journal" uri="resources/context/journal.md" reason="Memory."
  resource "issue" uri="github-issue://org/repo/41" reason="Task."
  resource "old" uri="resources/old.md" reason="History." inactive-reason="No longer used."
}"#,
        )
        .unwrap();
        let spec = discover(tmp.path());
        let set = watch_set_for(&spec, "hetz", &Default::default());
        assert_eq!(set.bus_id, "hetz.worker");
        let mut labels: Vec<&str> = set.carriers.iter().map(|c| c.label.as_str()).collect();
        labels.sort();
        assert_eq!(labels, vec!["declaration", "goal"]);
        let goal = set.carriers.iter().find(|c| c.label == "goal").unwrap();
        assert_eq!(goal.class, CarrierClass::Immediate);
        assert_eq!(goal.path, dir.join("resources/goal.md"));
        let coverage = spec
            .resources
            .iter()
            .map(|resource| (resource.name(), resource_coverage(&dir, resource)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(coverage["goal"], ResyncCoverage::Immediate);
        assert_eq!(coverage["journal"], ResyncCoverage::Silent);
        assert_eq!(coverage["issue"], ResyncCoverage::Unsupported);
        assert_eq!(coverage["old"], ResyncCoverage::Inactive);
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
        assert_eq!(
            watch_set_for(&spec, "alias", &Default::default()).bus_id,
            "alias.worker"
        );
        assert_eq!(
            watch_set_for(&spec, "other", &Default::default()).bus_id,
            "other.worker"
        );
    }

    #[test]
    fn lexical_paths_drive_store_classification_and_containment() {
        let agent_dir = Path::new("/catalog/agents/host/worker");

        let silent = resolve_local_path(
            agent_dir,
            "file:///catalog/agents/host/worker/resources/tmp/../context/./journal.md",
        )
        .unwrap();
        assert_eq!(
            silent,
            agent_dir.join("resources/context/journal.md"),
            "file URI dot segments are removed before classification"
        );
        assert_eq!(classify(agent_dir, "journal", &silent), None);

        let goal = resolve_local_path(agent_dir, "resources/context/.././goal.md").unwrap();
        assert_eq!(goal, agent_dir.join("resources/goal.md"));
        assert_eq!(
            classify(agent_dir, "notes", &goal),
            Some(CarrierClass::Immediate)
        );

        let escaped =
            resolve_local_path(agent_dir, "resources/context/../../outside/notes.md").unwrap();
        assert_eq!(escaped, agent_dir.join("outside/notes.md"));
        assert_eq!(
            classify(agent_dir, "notes", &escaped),
            Some(CarrierClass::Coalesced),
            "a lexical escape from an authored store is not silent"
        );
        assert_eq!(
            classify(
                agent_dir,
                "notes",
                Path::new("/catalog/agents/host/worker-copy/resources/context/notes.md"),
            ),
            Some(CarrierClass::Coalesced),
            "path-prefix siblings are not contained by the agent directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn classification_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path().join("agent");
        std::fs::create_dir_all(agent_dir.join("resources/context")).unwrap();
        std::os::unix::fs::symlink(
            agent_dir.join("resources/context"),
            agent_dir.join("resources/linked"),
        )
        .unwrap();

        let linked = resolve_local_path(&agent_dir, "resources/linked/journal.md").unwrap();
        assert_eq!(
            classify(&agent_dir, "journal", &linked),
            Some(CarrierClass::Coalesced),
            "classification is lexical and must not canonicalize through the symlink"
        );
    }

    #[test]
    fn shared_path_refresh_preserves_every_subscription_state() {
        let shared = PathBuf::from("/shared/resource.md");
        let previous = BTreeMap::from([(
            shared.clone(),
            vec![
                Entry {
                    bus_id: "host.alpha".to_owned(),
                    seat_id: None,
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: Some(CarrierState::Present("alpha-before".to_owned())),
                    occurrence_sequence: 4,
                    pending_transition: None,
                    dirty: true,
                },
                Entry {
                    bus_id: "host.beta".to_owned(),
                    seat_id: None,
                    label: "spec".to_owned(),
                    class: CarrierClass::Coalesced,
                    containment_root: None,
                    state: Some(CarrierState::Present("beta-before".to_owned())),
                    occurrence_sequence: 9,
                    dirty: true,
                    pending_transition: None,
                },
            ],
        )]);
        let sets = vec![
            AgentWatchSet {
                declaration_path: PathBuf::from("/catalog/alpha/agent.kdl"),
                bus_id: "host.alpha".to_owned(),
                seat_id: None,
                carriers: vec![WatchableCarrier {
                    label: "goal".to_owned(),
                    path: shared.clone(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                }],
            },
            AgentWatchSet {
                declaration_path: PathBuf::from("/catalog/beta/agent.kdl"),
                bus_id: "host.beta".to_owned(),
                seat_id: None,
                carriers: vec![WatchableCarrier {
                    label: "spec".to_owned(),
                    path: shared.clone(),
                    class: CarrierClass::Coalesced,
                    containment_root: None,
                }],
            },
        ];

        let rebuilt = rebuild_carriers(previous, refresh_for(sets), &BTreeMap::new());
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
            assert_eq!(
                entry.state,
                Some(CarrierState::Present(digest.to_owned()))
            );
            assert!(entry.dirty, "pending mutation remains pending for {bus_id}");
        }
    }

    #[test]
    fn retained_subscription_uses_current_seat_path_and_class() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/alias/worker");
        std::fs::create_dir_all(agent_dir.join("resources")).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "alias"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let goal = agent_dir.join("resources/goal.md");
        std::fs::write(&goal, "current bytes").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "alias").unwrap();

        let mut current = watch_set_for(
            &discover(root.path()),
            "alias",
            &ResourceProfileRegistry::empty(),
        );
        current.seat_id = Some("current-seat".to_owned());
        let old_path = agent_dir.join("resources/old-goal.md");
        let previous = BTreeMap::from([(
            old_path.clone(),
            vec![Entry {
                bus_id: "alias.worker".to_owned(),
                seat_id: Some("stale-seat".to_owned()),
                label: "goal".to_owned(),
                class: CarrierClass::Coalesced,
                containment_root: None,
                state: Some(CarrierState::Present("old-digest".to_owned())),
                occurrence_sequence: 3,
                pending_transition: None,
                dirty: true,
            }],
        )]);

        let rebuilt = rebuild_carriers(previous, refresh_for(vec![current]), &BTreeMap::new());
        assert!(!rebuilt.contains_key(&old_path));
        let entry = rebuilt[&goal]
            .iter()
            .find(|entry| entry.label == "goal")
            .expect("the goal subscription remains pending at its current path");
        assert_eq!(entry.bus_id, "alias.worker");
        assert_eq!(entry.seat_id.as_deref(), Some("current-seat"));
        assert_eq!(entry.class, CarrierClass::Immediate);
        assert_eq!(
            entry.state,
            Some(CarrierState::Present("old-digest".to_owned()))
        );
        assert!(entry.dirty);

        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "alias".to_owned(),
            carriers: rebuilt,
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.flush_path(&goal, Some(CarrierClass::Immediate));

        let entry = worker.carriers[&goal]
            .iter()
            .find(|entry| entry.label == "goal")
            .unwrap();
        assert!(!entry.dirty, "the current recipient accepted the transition");
        assert!(entry.pending_transition.is_none());
        let events = std::fs::read_dir(agent_dir.join("resources/inbox"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 1, "the event routes to the current bus id");
    }

    #[test]
    fn pending_retry_keeps_its_original_snapshot_across_path_rebinding() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/alias/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        let old_path = resources.join("old-goal.md");
        let current_path = resources.join("current-goal.md");
        std::fs::write(&old_path, "pending bytes").unwrap();
        std::fs::write(&current_path, "current rebound bytes").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "alias").unwrap();
        let declaration = agent_dir.join("agent.kdl");
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "alias".to_owned(),
            carriers: BTreeMap::from([(
                old_path.clone(),
                vec![Entry {
                    bus_id: "alias.worker".to_owned(),
                    seat_id: None,
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: Some(CarrierState::Present("old-digest".to_owned())),
                    occurrence_sequence: 0,
                    pending_transition: None,
                    dirty: true,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };

        worker.flush_path(&old_path, None);
        let pending = worker.carriers[&old_path][0]
            .pending_transition
            .clone()
            .expect("failed emit retains an immutable transition");
        std::fs::write(
            &declaration,
            r#"agent "worker" {
  host "alias"
  command "agent"
  resource "goal" uri="resources/current-goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let current =
            watch_set_for(&discover(root.path()), "alias", &ResourceProfileRegistry::empty());

        worker.apply_watch_sets(refresh_for(vec![current]));
        worker.flush_due(Instant::now() + IMMEDIATE_WINDOW + Duration::from_secs(1));

        let event = std::fs::read_dir(resources.join("inbox"))
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .find(|body| body.contains("stream: resync"))
            .expect("the retry routes through the refreshed recipient");
        assert!(event.contains(&format!("event-id: {}", pending.event_id)), "{event}");
        assert!(event.contains(&pending.body), "{event}");
        assert!(pending.body.contains(&format!("path: {}", old_path.display())));
        assert!(
            !pending
                .body
                .contains(&format!("path: {}", current_path.display())),
            "rebinding must not rewrite bytes reserved under the pending event identity"
        );
        let entry = &worker.carriers[&current_path][0];
        assert_eq!(entry.state.as_ref(), Some(&pending.new_state));
        assert!(entry.pending_transition.is_none());
        assert!(
            entry.dirty,
            "current rebound bytes are queued only after the pending snapshot completes"
        );
    }

    #[test]
    fn same_directory_path_rebinding_diffs_the_new_path_without_a_filesystem_event() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("resources");
        std::fs::create_dir_all(&parent).unwrap();
        let old_path = parent.join("old.md");
        let new_path = parent.join("new.md");
        std::fs::write(&old_path, "old bytes").unwrap();
        std::fs::write(&new_path, "new bytes").unwrap();
        let declaration = root.path().join("agent.kdl");
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(
                old_path.clone(),
                vec![Entry {
                    bus_id: "host.worker".to_owned(),
                    seat_id: None,
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: read_state(&old_path, None).ok(),
                    occurrence_sequence: 0,
                    pending_transition: None,
                    dirty: false,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::from([(parent.clone(), dir_identity(&parent))]),
            watcher: None,
        };

        worker.apply_watch_sets(refresh_for(vec![AgentWatchSet {
            declaration_path: declaration,
            bus_id: "host.worker".to_owned(),
            seat_id: None,
            carriers: vec![WatchableCarrier {
                label: "goal".to_owned(),
                path: new_path.clone(),
                class: CarrierClass::Immediate,
                containment_root: None,
            }],
        }]));

        assert!(!worker.carriers.contains_key(&old_path));
        assert!(worker.carriers[&new_path][0].dirty);
        assert!(
            worker.deadlines.contains_key(&CarrierClass::Immediate),
            "metadata refresh must enqueue the rebound digest even though its parent stayed watched"
        );
    }

    #[test]
    fn dirty_entry_deadline_migrates_when_refresh_changes_notification_class() {
        let root = tempfile::tempdir().unwrap();
        let carrier = root.path().join("carrier.md");
        std::fs::write(&carrier, "same bytes").unwrap();
        let declaration = root.path().join("agent.kdl");
        let old_deadline = Instant::now();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(
                carrier.clone(),
                vec![Entry {
                    bus_id: "host.worker".to_owned(),
                    seat_id: None,
                    label: "spec".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: read_state(&carrier, None).ok(),
                    occurrence_sequence: 0,
                    pending_transition: None,
                    dirty: true,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::from([(CarrierClass::Immediate, old_deadline)]),
            watched: BTreeMap::new(),
            watcher: None,
        };
        let refresh = |class| {
            refresh_for(vec![AgentWatchSet {
                declaration_path: declaration.clone(),
                bus_id: "host.worker".to_owned(),
                seat_id: None,
                carriers: vec![WatchableCarrier {
                    label: "spec".to_owned(),
                    path: carrier.clone(),
                    class,
                    containment_root: None,
                }],
            }])
        };

        worker.apply_watch_sets(refresh(CarrierClass::Coalesced));
        assert!(!worker.deadlines.contains_key(&CarrierClass::Immediate));
        assert!(
            worker.deadlines[&CarrierClass::Coalesced] >= old_deadline + COALESCED_WINDOW,
            "immediate-to-coalesced migration receives the new class window"
        );

        let coalesced_deadline = worker.deadlines[&CarrierClass::Coalesced];
        worker.apply_watch_sets(refresh(CarrierClass::Immediate));
        assert!(!worker.deadlines.contains_key(&CarrierClass::Coalesced));
        assert!(
            worker.deadlines[&CarrierClass::Immediate] < coalesced_deadline,
            "coalesced-to-immediate migration is rescheduled under the shorter window"
        );
        assert!(worker.carriers[&carrier][0].dirty);
    }

    #[test]
    fn malformed_declaration_retains_only_an_observed_live_seat_subscription() {
        let declaration = PathBuf::from("/catalog/agents/hetz/worker/agent.kdl");
        let previous = || {
            BTreeMap::from([(
                declaration.clone(),
                vec![Entry {
                    bus_id: "hetz.worker".to_owned(),
                    seat_id: Some("custom-worker-seat".to_owned()),
                    label: "declaration".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: Some(CarrierState::Present("before".to_owned())),
                    occurrence_sequence: 1,
                    pending_transition: Some(PendingTransition::new(
                        "declaration",
                        &declaration,
                        &CarrierState::Present("before".to_owned()),
                        &CarrierState::Present("corrected".to_owned()),
                        owner_incarnation(1),
                        1,
                    )),
                    dirty: true,
                }],
            )])
        };
        let malformed_declarations = BTreeSet::from([declaration.clone()]);

        let retained = rebuild_carriers(
            previous(),
            WatchRefresh {
                sets: Vec::new(),
                malformed_declarations: malformed_declarations.clone(),
                live_task_ids: BTreeSet::from(["custom-worker-seat".to_owned()]),
            },
            &BTreeMap::new(),
        );
        let entry = &retained[&declaration][0];
        assert_eq!(
            entry.state,
            Some(CarrierState::Present("before".to_owned()))
        );
        assert_eq!(
            entry
                .pending_transition
                .as_ref()
                .map(|pending| pending.new_state.clone()),
            Some(CarrierState::Present("corrected".to_owned()))
        );

        let dropped = rebuild_carriers(
            previous(),
            WatchRefresh {
                sets: Vec::new(),
                malformed_declarations,
                live_task_ids: BTreeSet::new(),
            },
            &BTreeMap::new(),
        );
        assert!(
            dropped.is_empty(),
            "a malformed declaration must not retain a watch after its exact seat is no longer live"
        );
    }

    #[test]
    fn degraded_poll_replays_a_pending_transition_before_newer_bytes() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/hetz/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "hetz"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "newer live bytes").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "hetz").unwrap();

        let set = watch_set_for(&discover(root.path()), "hetz", &Default::default());
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "hetz".to_owned(),
            carriers: BTreeMap::from([(
                carrier.clone(),
                vec![Entry {
                    bus_id: "hetz.worker".to_owned(),
                    seat_id: set.seat_id.clone(),
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: Some(CarrierState::Present("old-digest".to_owned())),
                    occurrence_sequence: 1,
                    pending_transition: Some(PendingTransition::new(
                        "goal",
                        &carrier,
                        &CarrierState::Present("old-digest".to_owned()),
                        &CarrierState::Present("pending-target".to_owned()),
                        owner_incarnation(1),
                        1,
                    )),
                    dirty: false,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        let now = Instant::now();
        worker.apply_watch_sets(refresh_for(vec![set]));
        assert!(
            !resources.join("inbox").exists(),
            "degraded polling must schedule rather than emit during refresh"
        );
        worker.flush_due(now + IMMEDIATE_WINDOW + Duration::from_secs(1));

        let event = std::fs::read_dir(resources.join("inbox"))
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .find(|body| body.contains("stream: resync"))
            .expect("pending transition is replayed");
        assert!(event.contains("old: old-digest"), "{event}");
        assert!(event.contains("new: pending-target"), "{event}");
        let entry = &worker.carriers[&carrier][0];
        assert_eq!(
            entry.state,
            Some(CarrierState::Present("pending-target".to_owned()))
        );
        assert!(entry.pending_transition.is_none());
        assert!(
            entry.dirty,
            "newer live bytes are scheduled only after the pending transition completes"
        );
    }

    #[test]
    fn fallback_polling_preserves_the_coalesced_window() {
        let root = tempfile::tempdir().unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let carrier = root.path().join("carrier.md");
        std::fs::write(&carrier, "before").unwrap();
        let baseline = read_state(&carrier, None).ok();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(
                carrier.clone(),
                vec![Entry {
                    bus_id: "host.missing".to_owned(),
                    seat_id: None,
                    label: "spec".to_owned(),
                    class: CarrierClass::Coalesced,
                    containment_root: None,
                    state: baseline.clone(),
                    occurrence_sequence: 0,
                    pending_transition: None,
                    dirty: false,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        std::fs::write(&carrier, "after").unwrap();
        let now = Instant::now();
        worker.apply_watch_sets(refresh_for(vec![AgentWatchSet {
            declaration_path: PathBuf::from("/catalog/missing/agent.kdl"),
            bus_id: "host.missing".to_owned(),
            seat_id: None,
            carriers: vec![WatchableCarrier {
                label: "spec".to_owned(),
                path: carrier.clone(),
                class: CarrierClass::Coalesced,
                containment_root: None,
            }],
        }]));

        worker.flush_due(now + IMMEDIATE_WINDOW + Duration::from_secs(1));
        let entry = &worker.carriers[&carrier][0];
        assert_eq!(entry.state, baseline);
        assert!(entry.pending_transition.is_none(), "coalesced emit ran too early");

        worker.flush_due(now + COALESCED_WINDOW + Duration::from_secs(1));
        assert!(
            worker.carriers[&carrier][0].pending_transition.is_some(),
            "the coalesced transition must be attempted after its full window"
        );
    }

    #[test]
    fn notify_backend_error_rescans_every_carrier_digest() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/hetz/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "hetz"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let goal = resources.join("goal.md");
        std::fs::write(&goal, "before\n").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "hetz").unwrap();

        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "hetz".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.apply_watch_sets(refresh_for(vec![watch_set_for(
            &discover(root.path()),
            "hetz",
            &Default::default(),
        )]));
        std::fs::write(&goal, "after\n").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        forward_watch_result(&tx, Err(notify::Error::generic("backend dropped events")));
        match rx.recv().unwrap() {
            Msg::Rescan => worker.rescan_all(),
            _ => panic!("a notify backend error must request a full digest rescan"),
        }
        worker.flush_due(Instant::now() + IMMEDIATE_WINDOW);

        let inbox = resources.join("inbox");
        let events = std::fs::read_dir(inbox)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert!(events[0].contains("stream: resync"));
        assert!(events[0].contains("binding: goal"));
    }

    #[cfg(unix)]
    #[test]
    fn digesting_a_fifo_fails_without_blocking_the_worker() {
        use std::os::unix::ffi::OsStrExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let fifo = tmp.path().join("carrier.fifo");
        let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: the path is NUL-terminated and points into the live temp directory.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        assert_eq!(read_state(&fifo, None).unwrap(), CarrierState::Missing);
        assert_eq!(
            read_state(&fifo, Some(tmp.path())).unwrap(),
            CarrierState::Missing,
            "confined carrier reads must reject a FIFO without blocking too"
        );
    }

    #[test]
    fn due_flush_only_clears_subscribers_of_the_due_class() {
        let root = tempfile::tempdir().unwrap();
        let carrier = root.path().join("carrier.md");
        std::fs::write(&carrier, "same bytes").unwrap();
        let state = read_state(&carrier, None).ok();
        let entries = [CarrierClass::Immediate, CarrierClass::Coalesced]
            .into_iter()
            .map(|class| Entry {
                bus_id: "host.worker".to_owned(),
                seat_id: None,
                label: format!("{class:?}"),
                class,
                containment_root: None,
                state: state.clone(),
                occurrence_sequence: 0,
                pending_transition: None,
                dirty: true,
            })
            .collect();
        let now = Instant::now();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(carrier, entries)]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::from([(CarrierClass::Immediate, now)]),
            watched: BTreeMap::new(),
            watcher: None,
        };

        worker.flush_due(now);
        let entries = worker.carriers.values().next().unwrap();
        assert!(!entries[0].dirty);
        assert!(entries[1].dirty, "coalesced subscriber must wait for its own deadline");
    }

    #[cfg(unix)]
    #[test]
    fn confined_read_refuses_a_symlink_created_after_resolution() {
        let agent_dir = tempfile::tempdir().expect("agent directory");
        let resources = agent_dir.path().join("resources");
        std::fs::create_dir(&resources).expect("resources directory");
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "inside").expect("inside carrier");
        assert!(
            matches!(
                read_state(&carrier, Some(agent_dir.path())),
                Ok(CarrierState::Present(_))
            ),
            "ordinary files beneath the admitted root remain readable"
        );

        std::fs::remove_file(&carrier).expect("remove inside carrier");
        let outside = tempfile::NamedTempFile::new().expect("outside carrier");
        std::fs::write(outside.path(), "external").expect("outside bytes");
        std::os::unix::fs::symlink(outside.path(), &carrier)
            .expect("replace absent carrier with an external symlink");
        assert_eq!(
            read_state(&carrier, Some(agent_dir.path())).unwrap(),
            CarrierState::Missing
        );

        std::fs::remove_file(&carrier).expect("remove final symlink");
        std::fs::remove_dir(&resources).expect("remove resources directory");
        let outside_dir = tempfile::tempdir().expect("outside directory");
        std::fs::write(outside_dir.path().join("goal.md"), "external").expect("outside carrier");
        std::os::unix::fs::symlink(outside_dir.path(), &resources)
            .expect("replace absent ancestor with an external symlink");
        assert_eq!(
            read_state(&carrier, Some(agent_dir.path())).unwrap(),
            CarrierState::Missing
        );
    }

    #[cfg(unix)]
    #[test]
    fn confined_read_refuses_a_symlinked_confinement_root_ancestor() {
        let temp = tempfile::tempdir().expect("outer directory");
        let real_root = temp.path().join("real/agent");
        std::fs::create_dir_all(&real_root).expect("real agent directory");
        std::fs::write(real_root.join("goal.md"), "outside admitted ancestry")
            .expect("carrier bytes");
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(temp.path().join("real"), &alias)
            .expect("symlinked root ancestor");
        let admitted_root = alias.join("agent");
        assert_eq!(
            read_state(&admitted_root.join("goal.md"), Some(&admitted_root)).unwrap(),
            CarrierState::Missing,
            "every component of the confinement root must be opened without following symlinks"
        );
    }
    #[test]
    fn deletion_and_same_byte_recreation_are_distinct_carrier_transitions() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/host/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "host"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "same bytes").unwrap();
        let CarrierState::Present(original_digest) = read_state(&carrier, None).unwrap() else {
            panic!("regular carrier has a digest");
        };
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let set = watch_set_for(&discover(root.path()), "host", &Default::default());
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.apply_watch_sets(refresh_for(vec![set]));

        std::fs::remove_file(&carrier).unwrap();
        worker.flush_path(&carrier, None);
        let deletion = resync_inbox_events(&agent_dir);
        assert_eq!(deletion.len(), 1);
        assert_eq!(event_field(&deletion[0], "old"), original_digest);
        assert_eq!(event_field(&deletion[0], "new"), "missing");
        assert!(event_field(&deletion[0], "occurrence").ends_with(":1"));
        assert_eq!(
            worker.carriers[&carrier][0].state,
            Some(CarrierState::Missing)
        );

        worker.flush_path(&carrier, None);
        assert_eq!(
            resync_inbox_events(&agent_dir).len(),
            1,
            "repeated missing observations are silent"
        );

        std::fs::write(&carrier, "same bytes").unwrap();
        worker.flush_path(&carrier, None);
        let events = resync_inbox_events(&agent_dir);
        assert_eq!(
            events.len(),
            1,
            "creation supersedes the tombstone under the binding key"
        );
        let creation = &events[0];
        assert_eq!(event_field(creation, "old"), "missing");
        assert!(event_field(creation, "occurrence").ends_with(":2"));
    }

    #[test]
    #[cfg(unix)]
    fn transient_permission_error_retries_without_emitting_a_tombstone() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/host/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "host"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "before").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let set = watch_set_for(&discover(root.path()), "host", &Default::default());
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.apply_watch_sets(refresh_for(vec![set]));
        let baseline = worker.carriers[&carrier][0].state.clone();

        let original_permissions = std::fs::metadata(&carrier).unwrap().permissions();
        std::fs::set_permissions(&carrier, std::fs::Permissions::from_mode(0)).unwrap();
        worker.flush_path(&carrier, None);
        let entry = &worker.carriers[&carrier][0];
        assert_eq!(entry.state, baseline);
        assert!(entry.pending_transition.is_none());
        assert!(entry.dirty);
        assert!(!resources.join("inbox").exists());

        std::fs::set_permissions(&carrier, original_permissions).unwrap();
        std::fs::write(&carrier, "after").unwrap();
        worker.flush_due(Instant::now() + IMMEDIATE_WINDOW + Duration::from_secs(1));
        let event = resync_inbox_event(&agent_dir);
        assert_ne!(event_field(&event, "new"), "missing");
    }

    #[test]
    #[cfg(unix)]
    fn initial_transient_read_failure_schedules_a_baseline_retry() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/host/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "host"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "baseline").unwrap();
        let original_permissions = std::fs::metadata(&carrier).unwrap().permissions();
        std::fs::set_permissions(&carrier, std::fs::Permissions::from_mode(0)).unwrap();
        let set = watch_set_for(&discover(root.path()), "host", &Default::default());
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::from([(resources.clone(), dir_identity(&resources))]),
            watcher: None,
        };

        worker.apply_watch_sets(refresh_for(vec![set]));
        let entry = worker.carriers[&carrier]
            .iter()
            .find(|entry| entry.label == "goal")
            .unwrap();
        assert_eq!(entry.state, None);
        assert!(entry.dirty);
        assert!(worker.deadlines.contains_key(&CarrierClass::Immediate));

        std::fs::set_permissions(&carrier, original_permissions).unwrap();
        worker.flush_due(Instant::now() + IMMEDIATE_WINDOW + Duration::from_secs(1));
        let entry = worker.carriers[&carrier]
            .iter()
            .find(|entry| entry.label == "goal")
            .unwrap();
        assert!(matches!(entry.state, Some(CarrierState::Present(_))));
        assert!(!entry.dirty);
        assert!(!resources.join("inbox").exists());
    }

    #[test]
    fn reinstalled_subscription_keeps_occurrence_identity_without_an_active_watch() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/host/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "host"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let carrier = resources.join("goal.md");
        std::fs::write(&carrier, "A").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let set = watch_set_for(&discover(root.path()), "host", &Default::default());
        let seen_subscription_count = set.carriers.len();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.apply_watch_sets(refresh_for(vec![set.clone()]));

        std::fs::write(&carrier, "B").unwrap();
        worker.flush_path(&carrier, None);
        let before_suspend = resync_inbox_event(&agent_dir);

        // Suspension removes every carrier and watch, while retaining one scalar sequence floor
        // for each declaration/binding identity seen during this supervisor incarnation.
        worker
            .watched
            .insert(resources.clone(), dir_identity(&resources));
        worker.apply_watch_sets(refresh_for(Vec::new()));
        assert!(worker.carriers.is_empty());
        assert!(worker.watched.is_empty());
        assert_eq!(
            worker.subscription_sequences.len(),
            seen_subscription_count
        );

        std::fs::write(&carrier, "A").unwrap();
        worker.apply_watch_sets(refresh_for(vec![set]));
        let resumed = worker.carriers[&carrier]
            .iter()
            .find(|entry| entry.label == "goal")
            .unwrap();
        assert_eq!(resumed.occurrence_sequence, 1);
        std::fs::write(&carrier, "B").unwrap();
        worker.flush_path(&carrier, None);
        let after_resume = resync_inbox_event(&agent_dir);

        assert_eq!(
            event_field(&before_suspend, "old"),
            event_field(&after_resume, "old")
        );
        assert_eq!(
            event_field(&before_suspend, "new"),
            event_field(&after_resume, "new")
        );
        assert_ne!(
            event_field(&before_suspend, "event-id"),
            event_field(&after_resume, "event-id"),
            "the post-resume A→B occurrence must not deduplicate against the pre-suspend one"
        );
        assert!(event_field(&before_suspend, "occurrence").ends_with(":1"));
        assert!(event_field(&after_resume, "occurrence").ends_with(":2"));
    }

    #[test]
    fn relocated_subscription_keeps_occurrence_sequence_in_the_recipient_namespace() {
        let root = tempfile::tempdir().unwrap();
        let agent_dir = root.path().join("agents/host/worker");
        let resources = agent_dir.join("resources");
        std::fs::create_dir_all(&resources).unwrap();
        std::fs::write(
            agent_dir.join("agent.kdl"),
            r#"agent "worker" {
  host "host"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
        )
        .unwrap();
        let original_carrier = resources.join("goal.md");
        let relocated_carrier = resources.join("relocated-goal.md");
        std::fs::write(&original_carrier, "A").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let set =
            watch_set_for(&discover(root.path()), "host", &ResourceProfileRegistry::empty());
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::new(),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        worker.apply_watch_sets(refresh_for(vec![set.clone()]));

        std::fs::write(&original_carrier, "B").unwrap();
        worker.flush_path(&original_carrier, None);
        let first_a_to_b = resync_inbox_event(&agent_dir);

        std::fs::write(&relocated_carrier, "A").unwrap();
        let mut relocated = set;
        relocated.declaration_path = agent_dir.join("relocated/agent.kdl");
        for carrier in &mut relocated.carriers {
            if carrier.label == "declaration" {
                carrier.path = relocated.declaration_path.clone();
            } else if carrier.label == "goal" {
                carrier.path = relocated_carrier.clone();
            }
        }
        worker.apply_watch_sets(refresh_for(vec![relocated.clone()]));
        let rebound = worker.carriers[&relocated_carrier]
            .iter()
            .find(|entry| entry.label == "goal")
            .unwrap();
        assert_eq!(rebound.occurrence_sequence, 1);
        assert_eq!(
            rebound.state,
            read_state(&original_carrier, None).ok()
        );

        worker.flush_path(&relocated_carrier, None);
        let back_to_a = resync_inbox_event(&agent_dir);
        assert_eq!(event_field(&back_to_a, "old"), event_field(&first_a_to_b, "new"));
        assert_eq!(event_field(&back_to_a, "new"), event_field(&first_a_to_b, "old"));
        assert!(event_field(&back_to_a, "occurrence").ends_with(":2"));

        std::fs::write(&relocated_carrier, "B").unwrap();
        worker.flush_path(&relocated_carrier, None);
        let second_a_to_b = resync_inbox_event(&agent_dir);
        assert_eq!(
            event_field(&first_a_to_b, "old"),
            event_field(&second_a_to_b, "old")
        );
        assert_eq!(
            event_field(&first_a_to_b, "new"),
            event_field(&second_a_to_b, "new")
        );
        assert_ne!(
            event_field(&first_a_to_b, "event-id"),
            event_field(&second_a_to_b, "event-id")
        );
        assert!(event_field(&first_a_to_b, "occurrence").ends_with(":1"));
        assert!(event_field(&second_a_to_b, "occurrence").ends_with(":3"));

        relocated.bus_id = "host.replacement".to_owned();
        worker.apply_watch_sets(refresh_for(vec![relocated]));
        assert_eq!(
            worker.carriers[&relocated_carrier]
                .iter()
                .find(|entry| entry.label == "goal")
                .unwrap()
                .occurrence_sequence,
            0,
            "a different recipient starts a distinct deduplication namespace"
        );
    }

    #[test]
    fn subscribers_advance_occurrence_sequences_independently() {
        let root = tempfile::tempdir().unwrap();
        let carrier = root.path().join("shared.md");
        std::fs::write(&carrier, "new bytes").unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let entries = ["host.alpha", "host.beta"]
            .into_iter()
            .map(|bus_id| Entry {
                bus_id: bus_id.to_owned(),
                seat_id: None,
                label: "goal".to_owned(),
                class: CarrierClass::Immediate,
                containment_root: None,
                state: Some(CarrierState::Present("old-digest".to_owned())),
                occurrence_sequence: 0,
                pending_transition: None,
                dirty: true,
            })
            .collect();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(carrier.clone(), entries)]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };

        worker.flush_path(&carrier, None);

        let entries = &worker.carriers[&carrier];
        assert_eq!(entries[0].occurrence_sequence, 1);
        assert_eq!(entries[1].occurrence_sequence, 1);
        assert_eq!(
            event_field(&entries[0].pending_transition.as_ref().unwrap().body, "occurrence"),
            event_field(&entries[1].pending_transition.as_ref().unwrap().body, "occurrence"),
            "one subscriber must not consume sequence numbers from another"
        );
    }

    #[test]
    fn supervisor_restart_incarnation_changes_the_occurrence_namespace() {
        let first = PendingTransition::new(
            "goal",
            Path::new("/agent/goal.md"),
            &CarrierState::Present("old".to_owned()),
            &CarrierState::Present("new".to_owned()),
            owner_incarnation(1),
            1,
        );
        let restarted = PendingTransition::new(
            "goal",
            Path::new("/agent/goal.md"),
            &CarrierState::Present("old".to_owned()),
            &CarrierState::Present("new".to_owned()),
            owner_incarnation(2),
            1,
        );

        assert_ne!(first.body, restarted.body);
        assert_ne!(first.event_id, restarted.event_id);
    }

    #[test]
    fn failed_tombstone_emit_retains_present_state_and_immutable_retry_snapshot() {
        let root = tempfile::tempdir().unwrap();
        crate::event::publish_owner_binding_for_test(root.path(), "host").unwrap();
        let carrier = root.path().join("carrier.md");
        std::fs::write(&carrier, "old bytes").unwrap();
        let mut worker = Worker {
            root: root.path().to_path_buf(),
            this_host: "host".to_owned(),
            carriers: BTreeMap::from([(
                carrier.clone(),
                vec![Entry {
                    bus_id: "host.missing".to_owned(),
                    seat_id: None,
                    label: "goal".to_owned(),
                    class: CarrierClass::Immediate,
                    containment_root: None,
                    state: Some(CarrierState::Present("old-digest".to_owned())),
                    occurrence_sequence: 0,
                    pending_transition: None,
                    dirty: true,
                }],
            )]),
            subscription_sequences: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            watched: BTreeMap::new(),
            watcher: None,
        };
        std::fs::remove_file(&carrier).unwrap();

        worker.flush_path(&carrier, None);
        let pending_transition = worker.carriers[&carrier][0]
            .pending_transition
            .clone()
            .expect("failed transition snapshot is retained");
        assert_eq!(pending_transition.new_state, CarrierState::Missing);
        assert!(pending_transition.body.contains("old: old-digest"));
        assert!(pending_transition.body.contains("new: missing"));
        assert_eq!(worker.carriers[&carrier][0].occurrence_sequence, 1);
        std::fs::write(&carrier, "old bytes").unwrap();
        worker.flush_path(&carrier, None);
        let entry = &worker.carriers[&carrier][0];
        assert_eq!(entry.occurrence_sequence, 1);
        assert_eq!(
            entry.state,
            Some(CarrierState::Present("old-digest".to_owned()))
        );
        assert_eq!(
            entry.pending_transition.as_ref(),
            Some(&pending_transition),
            "a retry must replay the tombstone snapshot even after the carrier is recreated"
        );
        assert!(entry.dirty);
        assert!(worker.deadlines.contains_key(&CarrierClass::Immediate));
    }

    #[test]
    fn transition_identity_covers_every_rendered_transition_dimension() {
        let old = CarrierState::Present("old-digest".to_owned());
        let new = CarrierState::Present("new-digest".to_owned());
        let baseline = render_body(
            "goal",
            Path::new("/agent/goal.md"),
            &old,
            &new,
            "v1:1:2:42:3:1",
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
                    &old,
                    &new,
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "path",
                render_body(
                    "goal",
                    Path::new("/other/goal.md"),
                    &old,
                    &new,
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "old state",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    &CarrierState::Present("other-old".to_owned()),
                    &new,
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "missing old state",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    &CarrierState::Missing,
                    &new,
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "new state",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    &old,
                    &CarrierState::Present("other-new".to_owned()),
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "missing new state",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    &old,
                    &CarrierState::Missing,
                    "v1:1:2:42:3:1",
                ),
            ),
            (
                "occurrence",
                render_body(
                    "goal",
                    Path::new("/agent/goal.md"),
                    &old,
                    &new,
                    "v1:1:2:42:3:2",
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
    fn local_path_resolution_parses_supported_file_uris_without_uri_metadata_bytes() {
        let agent_dir = Path::new("/cat/agents/hetz/w");
        assert_eq!(
            resolve_local_path(agent_dir, "file:///etc/demo.kdl"),
            Some(PathBuf::from("/etc/demo.kdl"))
        );
        for scheme in ["file", "FILE", "FiLe"] {
            assert_eq!(
                resolve_local_path(agent_dir, &format!("{scheme}:///etc/demo.kdl")),
                Some(PathBuf::from("/etc/demo.kdl"))
            );
        }
        assert_eq!(
            resolve_local_path(agent_dir, "file:///tmp/with%20space/%E2%82%AC.md"),
            Some(PathBuf::from("/tmp/with space/€.md"))
        );
        assert_eq!(
            resolve_local_path(agent_dir, "file:///tmp/literal%3Fmark"),
            Some(PathBuf::from("/tmp/literal?mark"))
        );
        assert_eq!(
            resolve_local_path(agent_dir, "file:///"),
            Some(PathBuf::from("/"))
        );
        assert_eq!(
            resolve_local_path(agent_dir, "resources/journal.md"),
            Some(agent_dir.join("resources/journal.md"))
        );
        assert_eq!(
            resolve_local_path(
                agent_dir,
                "resources/with%20space/%E2%82%AC-journal.md"
            ),
            Some(agent_dir.join("resources/with space/€-journal.md"))
        );

        for unsupported in [
            "file://authority/etc/demo.kdl",
            "file:////authority/etc/demo.kdl",
            "file:///etc/demo.kdl?revision=2",
            "file:///etc/demo.kdl#section",
            "file:///tmp/encoded%2Fseparator",
            "file:///tmp/encoded%2fseparator",
            "file:///tmp/encoded%5Cseparator",
            "file:///tmp/bad%escape",
            "file:///tmp/%2E%2E/escape",
            "file:///tmp/a%00b",
            "file:///tmp/%FF.md",
            "resources/encoded%2Fseparator",
            "resources/encoded%5Cseparator",
            "resources/%2E%2E/outside.md",
            "resources/bad%escape",
            "resources/a%00b",
            "resources/%FF.md",
            "http://x/y",
            "worktree://repo/main",
            "GitHub-Issue://org/repo/41",
            "ProFiLe:opaque",
        ] {
            assert_eq!(
                resolve_local_path(agent_dir, unsupported),
                None,
                "{unsupported} must not become filesystem bytes"
            );
        }
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

    #[test]
    fn profile_classes_map_onto_carrier_notification() {
        assert_eq!(carrier_class(ProfileClass::Immediate), Some(CarrierClass::Immediate));
        assert_eq!(carrier_class(ProfileClass::Coalesced), Some(CarrierClass::Coalesced));
        assert_eq!(
            carrier_class(ProfileClass::Silent),
            None,
            "silent profiles are excluded from the watch set like sniffed authored stores"
        );
    }

    #[test]
    fn registered_profile_failures_are_reported_while_other_bindings_survive() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("agents/hetz/worker");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("agent.kdl"),
            r#"agent "worker" {
  host "hetz"
  command "true"
  resource "goal" uri="dev.schickling.agent-goal://hetz/worker" reason="Mission."
  resource "issue" uri="worktree://repo/main" reason="Opaque scheme."
}"#,
        )
        .unwrap();

        let broken = tmp.path().join("broken.wasm");
        std::fs::write(&broken, b"not a module").unwrap();
        let profiles = ResourceProfileRegistry::empty().with_profile(
            agent_spec::ResourceProfile::wasm(
                "dev.schickling.agent-goal",
                &broken,
                ProfileClass::Coalesced,
            ),
        );
        let refresh = profiles.begin_refresh();
        let spec = discover(tmp.path());
        let (set, diagnostics) =
            resolve_watch_set(&spec, std::slice::from_ref(&spec), "hetz", &refresh);
        assert!(!set.carriers.iter().any(|c| c.label == "goal"));
        assert!(set.carriers.iter().any(|c| c.label == "declaration"));
        assert!(!set.carriers.iter().any(|c| c.label == "issue"));
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].contains("resource 'goal'"));
        assert!(diagnostics[0].contains("unwatchable"));
        assert_eq!(
            resource_coverage_with_profiles(&dir, &spec.resources[0], &refresh),
            ResyncCoverage::Unsupported
        );
    }

    #[test]
    fn silent_profile_skips_its_resolver_entirely() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("agents/hetz/worker");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("agent.kdl"),
            r#"agent "worker" {
  host "hetz"
  command "true"
  resource "goal" uri="dev.schickling.agent-goal://hetz/worker" reason="Mission."
}"#,
        )
        .unwrap();
        let missing = tmp.path().join("must-not-load.wasm");
        let profiles = ResourceProfileRegistry::empty().with_profile(
            agent_spec::ResourceProfile::wasm(
                "dev.schickling.agent-goal",
                missing,
                ProfileClass::Silent,
            ),
        );
        let refresh = profiles.begin_refresh();
        let spec = discover(tmp.path());
        let (set, diagnostics) =
            resolve_watch_set(&spec, std::slice::from_ref(&spec), "hetz", &refresh);
        assert!(!set.carriers.iter().any(|carrier| carrier.label == "goal"));
        assert!(
            diagnostics.is_empty(),
            "a silent profile must not execute its missing resolver: {diagnostics:?}"
        );
        assert_eq!(
            resource_coverage_with_profiles(&dir, &spec.resources[0], &refresh),
            ResyncCoverage::Silent
        );
    }
}
