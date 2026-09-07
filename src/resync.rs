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
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use notify::Watcher as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use agent_spec::profile::{
    ProfileClass, ResourceProfileRefresh, ResourceProfileRegistry,
};
use agent_spec::spec::{AgentSpec, Resource, decode_percent_path};

use crate::resource_profile::{MAX_FACTS, MAX_FACT_KEY_BYTES, ResourceFact};

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
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeclarationSummary {
    bindings: BTreeMap<String, String>,
    complete: bool,
}

fn declaration_summary(spec: &AgentSpec) -> DeclarationSummary {
    let mut bindings = spec
        .resources
        .iter()
        .map(|resource| {
            (
                resource.name().to_owned(),
                declaration_resource_digest(resource),
            )
        })
        .collect::<Vec<_>>();
    bindings.sort_by(|left, right| left.0.cmp(&right.0));
    let complete = bindings.len() <= MAX_FACTS
        && bindings
            .iter()
            .all(|(label, _)| label.len() <= MAX_FACT_KEY_BYTES);
    DeclarationSummary {
        bindings: bindings.into_iter().take(MAX_FACTS).collect(),
        complete,
    }
}

fn declaration_resource_digest(resource: &Resource) -> String {
    let mut digest = Sha256::new();
    digest.update(b"st2.resync.resource-declaration.v1\0");
    update_digest_field(&mut digest, resource.uri().as_bytes());
    update_digest_field(&mut digest, resource.reason().as_bytes());
    match resource.inactive_reason() {
        Some(reason) => {
            digest.update([1]);
            update_digest_field(&mut digest, reason.as_bytes());
        }
        None => digest.update([0]),
    }
    let selector = serde_json::to_vec(&resource.selector())
        .expect("a parsed JSON selector always serializes");
    update_digest_field(&mut digest, &selector);
    format!("{:x}", digest.finalize())
}

fn update_digest_field(digest: &mut Sha256, value: &[u8]) {
    let length = u64::try_from(value.len()).expect("declaration fields fit in u64");
    digest.update(length.to_be_bytes());
    digest.update(value);
}

/// The watchable carriers of one agent, keyed by its declaration path with current routing IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWatchSet {
    pub declaration_path: PathBuf,
    pub bus_id: String,
    pub seat_id: Option<String>,
    pub carriers: Vec<WatchableCarrier>,
    declaration_summary: Option<DeclarationSummary>,
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
            declaration_summary: Some(declaration_summary(spec)),
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

/// What one publication attempt settled, and therefore what happens to its reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationOutcome {
    Published,
    /// The attempt may pass later: retry the reserved bytes on the carrier's class deadline.
    Retry,
    /// The recipient is declared but not running. Keep the reservation, schedule nothing, and
    /// re-arm it when that recipient's desired state returns to running. Dropping it instead
    /// would lose a resync the agent should see on resume.
    Parked,
    /// No retry can admit this publication. Drop the reservation after one diagnostic.
    Refused,
}

enum Msg {
    WatchSet(WatchRefresh),
    Install(AgentWatchSet, Sender<()>),
    Deactivate(String, Sender<()>),
    Mutations(Vec<PathBuf>),
    Rescan,
    /// One handed-off publication finished. Outcomes return through the worker's own mailbox so
    /// the worker remains the only writer of carrier baselines and retry deadlines.
    Emitted {
        bus_id: String,
        label: String,
        outcome: PublicationOutcome,
    },
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
    fn fact_value(&self) -> String {
        match self {
            Self::Present(digest) => digest.chars().take(12).collect(),
            Self::Missing => "missing".to_owned(),
        }
    }
}
fn digest_transition_fact(old: &CarrierState, new: &CarrierState) -> Vec<ResourceFact> {
    vec![
        ResourceFact::transition("digest", Some(old.fact_value()), Some(new.fact_value()))
            .expect("short carrier digests are valid facts"),
    ]
}

fn declaration_transition_facts(
    old: Option<&DeclarationSummary>,
    new: Option<&DeclarationSummary>,
    old_state: &CarrierState,
    new_state: &CarrierState,
) -> Vec<ResourceFact> {
    let (Some(old), Some(new)) = (old, new) else {
        return digest_transition_fact(old_state, new_state);
    };
    if !old.complete || !new.complete {
        return digest_transition_fact(old_state, new_state);
    }

    let labels = old
        .bindings
        .keys()
        .chain(new.bindings.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let facts = labels
        .into_iter()
        .filter_map(|label| match (old.bindings.get(&label), new.bindings.get(&label)) {
            (None, Some(_)) => Some(ResourceFact::transition(
                label,
                None::<String>,
                Some("declared".to_owned()),
            )),
            (Some(_), None) => Some(ResourceFact::transition(
                label,
                Some("declared".to_owned()),
                None::<String>,
            )),
            (Some(before), Some(after)) if before != after => {
                Some(ResourceFact::current(label, "changed"))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>();
    match facts {
        Ok(facts) if !facts.is_empty() => facts,
        Ok(_) | Err(_) => digest_transition_fact(old_state, new_state),
    }
}

fn current_declaration_summary(root: &Path, path: &Path) -> Option<DeclarationSummary> {
    crate::discover_strict(root)
        .specs
        .into_iter()
        .find(|spec| lexical_clean(&spec.path) == path)
        .map(|spec| declaration_summary(&spec))
}


#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingTransition {
    binding: String,
    path: PathBuf,
    old_state: CarrierState,
    new_state: CarrierState,
    facts: Vec<ResourceFact>,
    topics: Vec<String>,
    body: String,
    event_id: String,
    new_declaration_summary: Option<DeclarationSummary>,
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
        Self::capture(
            binding,
            path,
            old_state,
            new_state,
            digest_transition_fact(old_state, new_state),
            vec!["content".to_owned()],
            None,
            incarnation,
            sequence,
        )
    }

    fn declaration(
        path: &Path,
        old_state: &CarrierState,
        new_state: &CarrierState,
        old_summary: Option<&DeclarationSummary>,
        new_summary: Option<DeclarationSummary>,
        incarnation: crate::event::StreamOwnerIncarnation,
        sequence: u64,
    ) -> Self {
        let facts = declaration_transition_facts(
            old_summary,
            new_summary.as_ref(),
            old_state,
            new_state,
        );
        Self::capture(
            "declaration",
            path,
            old_state,
            new_state,
            facts,
            vec!["declaration".to_owned()],
            new_summary,
            incarnation,
            sequence,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn capture(
        binding: &str,
        path: &Path,
        old_state: &CarrierState,
        new_state: &CarrierState,
        facts: Vec<ResourceFact>,
        topics: Vec<String>,
        new_declaration_summary: Option<DeclarationSummary>,
        incarnation: crate::event::StreamOwnerIncarnation,
        sequence: u64,
    ) -> Self {
        let occurrence = incarnation.occurrence_token(sequence);
        let body = render_body(binding, &topics, &facts, &occurrence);
        let event_id = transition_identity(&body);
        Self {
            binding: binding.to_owned(),
            path: path.to_path_buf(),
            old_state: old_state.clone(),
            new_state: new_state.clone(),
            facts,
            topics,
            body,
            event_id,
            new_declaration_summary,
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
    declaration_summary: Option<DeclarationSummary>,
    /// Last occurrence sequence reserved by this retained subscription. Sequence zero is the
    /// silent seeded state; only capturing a new immutable transition advances it.
    occurrence_sequence: u64,
    /// Immutable publication snapshot retained after a failed emit. Metadata refresh may change
    /// the current route, path, or class, but a reserved event identity must keep its exact bytes.
    pending_transition: Option<PendingTransition>,
    /// True while a handed-off publication for this subscription is outstanding. Publishing runs
    /// off the worker thread, so the worker must not hand off the same subscription twice or
    /// advance its baseline before the outcome returns.
    in_flight: bool,
    /// True once the recipient refused because it is not running. A parked subscription captures
    /// nothing and schedules nothing; the next refresh that carries the recipient again — which
    /// only happens while it is running — clears this and re-arms its retained reservation.
    parked: bool,
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

/// One publication handed to the emitter thread.
struct EmitJob {
    bus_id: String,
    label: String,
    transition: PendingTransition,
}

/// The handoff between the worker thread and the emitter thread.
///
/// Publishing does not run on the worker thread. A reconcile pass answers its own progress on
/// that thread — `install_live` and `deactivate` block on a worker acknowledgement — so a
/// publication executed there serializes the whole pass behind it. A publication is neither
/// bounded nor cheap: it takes the shared catalog-authoring lock, re-resolves the catalog, and
/// takes the recipient's stream lock, any of which can block on another process. Handing
/// publication to its own thread is what keeps a pass able to complete while publications are
/// pending, refused, or stuck (#431).
#[derive(Default)]
struct EmitQueue {
    state: std::sync::Mutex<EmitQueueState>,
    ready: std::sync::Condvar,
}

#[derive(Default)]
struct EmitQueueState {
    jobs: std::collections::VecDeque<EmitJob>,
    stopped: bool,
    /// Publications ever handed off. A refusal that is retried at a cadence shows up here as
    /// volume, which is what the tests for terminal-refusal classification measure.
    #[cfg(test)]
    handed_off: usize,
}

impl EmitQueue {
    fn lock(&self) -> std::sync::MutexGuard<'_, EmitQueueState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn push(&self, job: EmitJob) {
        let mut state = self.lock();
        #[cfg(test)]
        {
            state.handed_off += 1;
        }
        state.jobs.push_back(job);
        drop(state);
        self.ready.notify_one();
    }

    #[cfg(test)]
    fn handed_off(&self) -> usize {
        self.lock().handed_off
    }

    /// Drop every queued publication for one recipient. A publication already taken by the
    /// emitter is left alone: it was in flight while the subscription was still active, and its
    /// outcome is discarded by the worker when the subscription is gone.
    fn cancel(&self, bus_id: &str) {
        self.lock().jobs.retain(|job| job.bus_id != bus_id);
    }

    /// Drop every queued publication whose recipient is no longer an active subscription.
    fn retain_recipients(&self, active: &BTreeSet<String>) {
        self.lock().jobs.retain(|job| active.contains(&job.bus_id));
    }

    /// Pop one queued publication without waiting. Only the synchronous test drive uses this;
    /// the emitter thread blocks in [`Self::next`].
    #[cfg(test)]
    fn take_queued(&self) -> Option<EmitJob> {
        self.lock().jobs.pop_front()
    }

    /// Recipients of every queued publication, in queue order.
    #[cfg(test)]
    fn queued_recipients(&self) -> Vec<String> {
        self.lock()
            .jobs
            .iter()
            .map(|job| job.bus_id.clone())
            .collect()
    }

    fn stop(&self) {
        let mut state = self.lock();
        state.stopped = true;
        state.jobs.clear();
        self.ready.notify_all();
    }

    fn next(&self) -> Option<EmitJob> {
        let mut state = self.lock();
        loop {
            if state.stopped {
                return None;
            }
            if let Some(job) = state.jobs.pop_front() {
                return Some(job);
            }
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

fn emitter_loop(root: PathBuf, this_host: String, queue: Arc<EmitQueue>, outcomes: Sender<Msg>) {
    while let Some(job) = queue.next() {
        let outcome = emit_resync(&root, &this_host, &job.bus_id, &job.transition);
        if outcomes
            .send(Msg::Emitted {
                bus_id: job.bus_id,
                label: job.label,
                outcome,
            })
            .is_err()
        {
            break;
        }
    }
}

struct Worker {
    root: PathBuf,
    this_host: String,
    carriers: BTreeMap<PathBuf, Vec<Entry>>,
    /// Last sequence reserved for each recipient/binding identity seen by this supervisor.
    /// Inactive subscriptions stay here so a reinstall cannot collide with an earlier occurrence.
    /// Its lifetime is the worker's and its cardinality is bounded by identities observed there.
    subscription_sequences: BTreeMap<SubscriptionIdentity, u64>,
    /// Reservations retained for recipients that refused because they are not running. A refresh
    /// drops a suspended recipient's subscription entirely, so the reservation has to outlive the
    /// subscription to survive until that recipient resumes. Like `subscription_sequences`, this
    /// is worker-lifetime state bounded by the identities it has observed.
    parked_transitions: BTreeMap<SubscriptionIdentity, PendingTransition>,
    deadlines: BTreeMap<CarrierClass, Instant>,
    watched: BTreeMap<PathBuf, Option<DirIdentity>>,
    /// `None` degrades to digest polling at refresh cadence (each reconcile pass) instead of
    /// evented watching — diagnosed once by the absence of immediacy, never a hard error.
    watcher: Option<notify::RecommendedWatcher>,
    /// Publications handed off to the emitter thread. See [`EmitQueue`].
    emit: Arc<EmitQueue>,
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
    let emit = Arc::new(EmitQueue::default());
    let emitter = std::thread::Builder::new()
        .name("resync-emit".to_owned())
        .spawn({
            let root = root.clone();
            let this_host = this_host.clone();
            let emit = Arc::clone(&emit);
            let outcomes = forward.clone();
            move || emitter_loop(root, this_host, emit, outcomes)
        })
        .ok();
    let watcher = make_watcher(forward);
    let mut worker = Worker {
        root,
        this_host,
        carriers: BTreeMap::new(),
        subscription_sequences: BTreeMap::new(),
        parked_transitions: BTreeMap::new(),
        deadlines: BTreeMap::new(),
        watched: BTreeMap::new(),
        watcher,
        emit: Arc::clone(&emit),
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
            Ok(Msg::Emitted {
                bus_id,
                label,
                outcome,
            }) => worker.record_publication(&bus_id, &label, outcome),
            Ok(Msg::Shutdown) => break,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        worker.flush_due(Instant::now());
    }
    // Stop the emitter without joining it. A publication blocked on another process's lock must
    // not hold up supervisor teardown, and the emitter owns no worker state to hand back.
    emit.stop();
    drop(emitter);
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
    parked_transitions: &mut BTreeMap<SubscriptionIdentity, PendingTransition>,
) -> BTreeMap<PathBuf, Vec<Entry>> {
    let mut next: BTreeMap<PathBuf, Vec<Entry>> = BTreeMap::new();
    for set in refresh.sets {
        let seeded_declaration_summary = set.declaration_summary.clone();
        for carrier in set.carriers {
            // The canonical recipient and binding label identify one subscription across
            // declaration and carrier relocation. Rebuild every retained entry from the current
            // declaration while carrying its baseline and immutable delivery snapshot. Looking
            // across path buckets also lets a binding's re-resolved path/class/containment
            // metadata become current. A bus-id change intentionally seeds a new
            // recipient-scoped namespace.
            let identity = (set.bus_id.clone(), carrier.label.clone());
            // Every recipient in a refresh set is running, so this is where a reservation parked
            // against a not-running recipient re-arms.
            let restored = parked_transitions.remove(&identity);
            let retained = take_retained_entry(&mut previous, &set.bus_id, &carrier.label);
            let (
                state,
                declaration_summary,
                occurrence_sequence,
                pending_transition,
                in_flight,
                dirty,
            ) = retained.map_or_else(
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
                        (carrier.label == "declaration")
                            .then(|| seeded_declaration_summary.clone())
                            .flatten(),
                        subscription_sequences.get(&identity).copied().unwrap_or(0),
                        None,
                        false,
                        dirty,
                    )
                },
                |entry| {
                    (
                        entry.state,
                        entry.declaration_summary,
                        entry.occurrence_sequence,
                        entry.pending_transition,
                        // A rebuilt subscription is the same subscription: its outstanding
                        // publication is still outstanding, and its outcome still applies.
                        entry.in_flight,
                        entry.dirty,
                    )
                },
            );
            let restored_reservation = restored.is_some();
            let entry = Entry {
                bus_id: set.bus_id.clone(),
                seat_id: set.seat_id.clone(),
                label: carrier.label.clone(),
                class: carrier.class,
                containment_root: carrier.containment_root.clone(),
                state,
                declaration_summary,
                occurrence_sequence,
                pending_transition: pending_transition.or(restored),
                in_flight,
                parked: false,
                dirty: dirty || restored_reservation,
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
        // Registration closes the event gap first; a second digest pass then covers writes
        // between the pre-registration poll and watch installation. That pass spans the whole
        // watch set, not only the directories this refresh touched: changing the registration
        // can cost the backend its already-queued events for subscriptions that never moved
        // (see `refresh_watches`), which is the same loss a backend error means and takes the
        // same containment. Equal digests keep it silent.
        if self.refresh_watches() {
            self.rescan_all();
        }
    }

    fn apply_watch_sets(&mut self, refresh: WatchRefresh) {
        let previous = std::mem::take(&mut self.carriers);
        let previous_paths = self.prepare_carrier_update(&previous);
        self.carriers = rebuild_carriers(
            previous,
            refresh,
            &self.subscription_sequences,
            &mut self.parked_transitions,
        );
        // A subscription this refresh dropped — a suspended, retired or no-longer-live seat —
        // must not have a queued publication published afterwards.
        let active = self
            .carriers
            .values()
            .flatten()
            .map(|entry| entry.bus_id.clone())
            .collect::<BTreeSet<_>>();
        self.emit.retain_recipients(&active);
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
            &mut self.parked_transitions,
        );
        for (path, entries) in replacement {
            unaffected.entry(path).or_default().extend(entries);
        }
        self.carriers = unaffected;
        self.finish_carrier_update(&previous_paths);
    }

    fn deactivate_watch_set(&mut self, bus_id: &str) {
        // Nothing may start publishing to this recipient once the acknowledgement returns.
        self.emit.cancel(bus_id);
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
                if entry.parked {
                    // A parked recipient is not running: observing its carrier would only
                    // schedule work whose answer is already known.
                    continue;
                }
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

    fn rescan_all(&mut self) {
        self.poll_paths(self.carriers.keys().cloned().collect());
    }

    /// Re-register watches so every distinct parent directory of the current watch set is covered,
    /// dropping directories that left the set or were replaced (identity change). A replaced watch
    /// stays blind until the next pass rebuilds it — bounded by the reconcile interval, the same
    /// tradeoff `CatalogDeclarationWatcher` accepts for declarations.
    ///
    /// Reports whether the registration set actually changed, because a backend is not obliged to
    /// leave its other subscriptions undisturbed while it does. `notify`'s macOS FSEvents backend
    /// stops the single shared stream on every `watch`/`unwatch`, purges the device's pending
    /// events, and restarts at `kFSEventStreamEventIdSinceNow` — so registering one new directory
    /// destroys mutations already queued for directories that were watched the whole time. Linux
    /// inotify adds and removes descriptors on a shared fd and keeps its queue. Callers must treat
    /// a changed registration as a possible drop across the entire watch set.
    fn refresh_watches(&mut self) -> bool {
        let mut changed = false;
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
                    changed = true;
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
            // The purge is in the attempt, not the outcome: FSEvents' `watch_inner` stops and
            // restarts the stream before `append_path` can reject a directory that has since
            // gone missing. Count the attempt, exactly as the unwatch above does.
            changed = true;
            if watcher
                .watch(&dir, notify::RecursiveMode::NonRecursive)
                .is_ok()
            {
                self.watched.insert(dir, identity);
            }
        }
        changed
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
                    if entry.parked {
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
                    dirty_here.push(entry.class);
                }
            }
            drop(dirty_here);
        }
        if extend && self.refresh_watches() {
            self.rescan_all();
        }
    }

    /// Drive one flush and every publication it hands off to completion on this thread.
    ///
    /// In production the worker and the emitter are two threads of one loop; a unit test asserts
    /// on settled carrier state, so it runs both halves here in the order the loop runs them.
    #[cfg(test)]
    fn flush_path_publishing(&mut self, path: &Path, due_class: Option<CarrierClass>) {
        self.flush_path(path, due_class);
        self.drain_publications();
    }

    /// [`Self::flush_due`] with its handed-off publications driven to completion. See
    /// [`Self::flush_path_publishing`].
    #[cfg(test)]
    fn flush_due_publishing(&mut self, now: Instant) {
        self.flush_due(now);
        self.drain_publications();
    }

    #[cfg(test)]
    fn drain_publications(&mut self) {
        while let Some(job) = self.emit.take_queued() {
            let published = emit_resync(&self.root, &self.this_host, &job.bus_id, &job.transition);
            self.record_publication(&job.bus_id, &job.label, published);
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
        let observed_declaration_summary = self
            .carriers
            .get(path)
            .is_some_and(|entries| entries.iter().any(|entry| entry.label == "declaration"))
            .then(|| current_declaration_summary(&self.root, path));
        let Some(entries) = self.carriers.get_mut(path) else {
            return;
        };
        let mut retries = Vec::new();
        let mut handoffs = Vec::new();
        for entry in entries.iter_mut() {
            if due_class.is_some_and(|class| entry.class != class) {
                continue;
            }
            if entry.parked {
                // No capture and no publication while the recipient is not running. Its
                // reservation waits in `parked_transitions`; the carrier baseline is unchanged,
                // so a change during the parked window is still observed after it re-arms.
                entry.dirty = false;
                continue;
            }
            if entry.in_flight {
                // One outstanding publication per subscription. Its outcome re-reads this
                // carrier, so a transition that arrives meanwhile is observed then, not lost.
                entry.dirty = false;
                continue;
            }
            entry.dirty = false;
            let observed = read_state(path, entry.containment_root.as_deref());

            if let Some(pending) = entry.pending_transition.as_ref() {
                if let Err(error) = observed {
                    diagnose_read_error(path, &error);
                }
                entry.in_flight = true;
                handoffs.push(EmitJob {
                    bus_id: entry.bus_id.clone(),
                    label: entry.label.clone(),
                    transition: pending.clone(),
                });
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
            let transition = if entry.label == "declaration" {
                PendingTransition::declaration(
                    path,
                    old_state,
                    &target_state,
                    entry.declaration_summary.as_ref(),
                    observed_declaration_summary.clone().flatten(),
                    incarnation,
                    sequence,
                )
            } else {
                PendingTransition::new(
                    &entry.label,
                    path,
                    old_state,
                    &target_state,
                    incarnation,
                    sequence,
                )
            };
            entry.occurrence_sequence = sequence;
            entry.in_flight = true;
            handoffs.push(EmitJob {
                bus_id: entry.bus_id.clone(),
                label: entry.label.clone(),
                transition: transition.clone(),
            });
            // The captured transition stays pending until its outcome returns: its exact bytes
            // and reserved event identity are what a retry must reuse.
            entry.pending_transition = Some(transition);
        }
        // Queue outside the carrier borrow. Publishing itself happens on the emitter thread, so
        // nothing below this point waits for a catalog lock, a stream lock, or a refusal.
        for job in handoffs {
            self.emit.push(job);
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

    /// Apply one handed-off publication's outcome. The worker stays the only writer of carrier
    /// baselines and retry deadlines, so the emitter reports back here instead of touching state.
    ///
    /// The outcome decides what happens to the reservation:
    ///
    /// - `Published` completes it and advances the baseline.
    /// - `Retry` leaves it pending; re-observing the carrier re-arms its class.
    /// - `Parked` moves it out of the subscription and schedules nothing. It re-arms when the
    ///   recipient is carried by a refresh again, which only happens while it is running.
    /// - `Refused` drops it and advances the baseline anyway, because a reservation no retry can
    ///   admit must not be re-captured from the same carrier transition on the next observation.
    ///
    /// An outcome whose subscription is gone — deactivated or refreshed away while the
    /// publication was outstanding — matches nothing and is discarded.
    fn record_publication(&mut self, bus_id: &str, label: &str, outcome: PublicationOutcome) {
        let mut touched = Vec::new();
        let mut parked = Vec::new();
        for (path, entries) in &mut self.carriers {
            for entry in entries
                .iter_mut()
                .filter(|entry| entry.bus_id == bus_id && entry.label == label)
            {
                entry.in_flight = false;
                match outcome {
                    PublicationOutcome::Published | PublicationOutcome::Refused => {
                        if let Some(settled) = entry.pending_transition.take() {
                            entry.state = Some(settled.new_state);
                            if settled.binding == "declaration" {
                                entry.declaration_summary = settled.new_declaration_summary;
                            }
                        }
                    }
                    PublicationOutcome::Retry => {}
                    PublicationOutcome::Parked => {
                        entry.parked = true;
                        entry.dirty = false;
                        if let Some(reservation) = entry.pending_transition.take() {
                            parked.push((
                                (entry.bus_id.clone(), entry.label.clone()),
                                reservation,
                            ));
                        }
                    }
                }
                touched.push(path.clone());
            }
        }
        for (identity, reservation) in parked {
            self.parked_transitions.insert(identity, reservation);
        }
        self.poll_paths(touched);
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
) -> PublicationOutcome {
    let subject = crate::resource_profile_supervisor::resource_change_subject(
        &transition.binding,
        &transition.facts,
        &transition.topics,
        "content changed",
    );
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
        Ok(_) => PublicationOutcome::Published,
        Err(error) => {
            let path = transition.path.display();
            match crate::event::refusal_kind(&error) {
                Some(crate::event::RefusalKind::RecipientNotRunning) => {
                    eprintln!(
                        "st2: resync for '{path}' is parked until '{bus_id}' is running again: {error:#}"
                    );
                    PublicationOutcome::Parked
                }
                Some(crate::event::RefusalKind::Permanent) => {
                    eprintln!(
                        "st2: resync for '{path}' dropped; no retry can admit it: {error:#}"
                    );
                    PublicationOutcome::Refused
                }
                None => {
                    eprintln!("st2: resync emit for '{path}' failed: {error:#}");
                    PublicationOutcome::Retry
                }
            }
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResyncBody<'a> {
    binding: &'a str,
    topics: &'a [String],
    facts: &'a [ResourceFact],
    occurrence: &'a str,
}

fn render_body(
    binding: &str,
    topics: &[String],
    facts: &[ResourceFact],
    occurrence: &str,
) -> String {
    serde_json::to_string(&ResyncBody {
        binding,
        topics,
        facts,
        occurrence,
    })
    .expect("validated resync facts always serialize")
}
#[cfg(test)]
mod tests;
