//! Resident process supervision for observable Resource Profiles.
//!
//! The worker is the sole owner of runtime processes and their protocol state. Reconcile callers
//! submit complete desired binding sets; removals are acknowledged only after registrations have
//! been fenced and processes no longer owned by the desired generation have been stopped.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, BufReader, Write as _};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};

use agent_spec::profile::{
    ProfileCapability, ProfileDescriptor, ResourceProfileRegistry, RuntimeTopology,
};
use agent_spec::spec::AgentSpec;
use anyhow::Context as _;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::catalog::CatalogConfig;
use crate::resource_profile::{
    AcceptedOutput, BindingId, BindingRegistration, CatchUp, HostMessage, OwnerClaim,
    PublicationContract, RegistrationToken, ResourceFact, RuntimeHealthState, RuntimeIncarnation,
    RuntimeLifecycle, RuntimeMessage, RuntimeOwner, SnapshotDigest, SnapshotTarget, TopicSelection,
    MAX_PROTOCOL_LINE_BYTES, decode_runtime_line, encode_host_line,
};

const MAILBOX_CAPACITY: usize = 64;
const WRITER_CAPACITY: usize = 64;
static ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceProfileHealth {
    pub scheme: String,
    pub binding: Option<String>,
    pub state: RuntimeHealthState,
    pub detail: Option<String>,
}

#[derive(Debug)]
pub struct ResourceProfileRefreshReport {
    pub warnings: Vec<String>,
}

pub struct ResourceProfileSupervisor {
    tx: SyncSender<Msg>,
    worker: Option<JoinHandle<()>>,
    catalog_root: PathBuf,
    this_host: String,
}

impl ResourceProfileSupervisor {
    pub fn new(catalog_root: PathBuf, this_host: String) -> anyhow::Result<Self> {
        let catalog_root = lexical_absolute(&catalog_root)?;
        let (tx, rx) = mpsc::sync_channel(MAILBOX_CAPACITY);
        let worker_tx = tx.clone();
        let worker_root = catalog_root.clone();
        let worker_host = this_host.clone();
        let worker = thread::Builder::new()
            .name("st2-resource-profile".to_owned())
            .spawn(move || Worker::new(worker_root, worker_host, worker_tx).run(rx))
            .context("spawn Resource Profile supervisor")?;
        Ok(Self {
            tx,
            worker: Some(worker),
            catalog_root,
            this_host,
        })
    }

    /// Reconcile the exact set of bindings owned by canonical agent seats proven live this pass.
    /// The caller holds the catalog read lock while deriving `config`, `profiles`, and `generation`.
    pub fn refresh(
        &self,
        config: &CatalogConfig,
        profiles: &ResourceProfileRegistry,
        generation: Option<u64>,
        live_specs: &[AgentSpec],
    ) -> ResourceProfileRefreshReport {
        let (desired, mut warnings) = desired_bindings(
            &self.catalog_root,
            &self.this_host,
            config,
            profiles,
            generation,
            live_specs,
        );
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self
            .tx
            .send(Msg::Refresh {
                desired,
                reply: reply_tx,
            })
            .is_err()
        {
            warnings.push("Resource Profile supervisor worker stopped".to_owned());
        } else if let Ok(worker_warnings) = reply_rx.recv() {
            warnings.extend(worker_warnings);
        } else {
            warnings.push("Resource Profile supervisor refresh acknowledgement failed".to_owned());
        }
        ResourceProfileRefreshReport { warnings }
    }

    /// Fence one agent before its canonical seat is replaced. Completion is synchronous.
    pub fn deactivate(&self, spec: &AgentSpec) {
        let recipient = spec.bus_id(&self.this_host);
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self
            .tx
            .send(Msg::Deactivate {
                recipient,
                reply: reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.recv();
        }
    }

    pub fn health(&self) -> Vec<ResourceProfileHealth> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self.tx.send(Msg::Health { reply: reply_tx }).is_err() {
            return Vec::new();
        }
        reply_rx.recv().unwrap_or_default()
    }
}

impl Drop for ResourceProfileSupervisor {
    fn drop(&mut self) {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self.tx.send(Msg::Shutdown { reply: reply_tx }).is_ok() {
            let _ = reply_rx.recv();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug)]
enum Msg {
    Refresh {
        desired: BTreeMap<String, DesiredBinding>,
        reply: SyncSender<Vec<String>>,
    },
    Deactivate {
        recipient: String,
        reply: SyncSender<()>,
    },
    Health {
        reply: SyncSender<Vec<ResourceProfileHealth>>,
    },
    RuntimeOutput {
        key: RuntimeKey,
        owner: RuntimeOwner,
        output: Result<RuntimeMessage, String>,
    },
    RuntimeEof {
        key: RuntimeKey,
        owner: RuntimeOwner,
    },
    WriterFailed {
        key: RuntimeKey,
        owner: RuntimeOwner,
        error: String,
    },
    Shutdown {
        reply: SyncSender<()>,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct DesiredBinding {
    stable_key: String,
    catalog_root: PathBuf,
    this_host: String,
    recipient: String,
    binding_name: String,
    scheme: String,
    generation: u64,
    topology: RuntimeTopology,
    argv: Vec<String>,
    uri: String,
    selector: Value,
    descriptor: ProfileDescriptor,
    target: SnapshotTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RuntimeKey {
    Shared {
        scheme: String,
        generation: u64,
    },
    PerBinding {
        scheme: String,
        generation: u64,
        binding: String,
    },
}

impl DesiredBinding {
    fn runtime_key(&self) -> RuntimeKey {
        match self.topology {
            RuntimeTopology::Shared => RuntimeKey::Shared {
                scheme: self.scheme.clone(),
                generation: self.generation,
            },
            RuntimeTopology::PerBinding => RuntimeKey::PerBinding {
                scheme: self.scheme.clone(),
                generation: self.generation,
                binding: hash_text(&self.stable_key),
            },
        }
    }
}

fn desired_bindings(
    catalog_root: &Path,
    this_host: &str,
    config: &CatalogConfig,
    profiles: &ResourceProfileRegistry,
    generation: Option<u64>,
    live_specs: &[AgentSpec],
) -> (BTreeMap<String, DesiredBinding>, Vec<String>) {
    let catalog_generation = generation.unwrap_or(0);
    let refresh = profiles.begin_refresh();
    let mut descriptors = BTreeMap::new();
    let mut runtimes = BTreeMap::new();
    let mut generations = BTreeMap::new();
    let mut warnings = Vec::new();
    for declared in &config.profiles {
        let Some(runtime) = declared.runtime.as_ref() else {
            continue;
        };
        match refresh.try_descriptor(&declared.scheme) {
            Ok(Some(descriptor))
                if descriptor.capabilities.contains(&ProfileCapability::Observe) =>
            {
                generations.insert(
                    declared.scheme.clone(),
                    profile_generation(catalog_generation, declared, &descriptor, profiles),
                );
                descriptors.insert(declared.scheme.clone(), descriptor);
                runtimes.insert(declared.scheme.clone(), runtime.clone());
            }
            Ok(_) => warnings.push(format!(
                "Resource Profile '{}': runtime has no observable descriptor",
                declared.scheme
            )),
            Err(error) => warnings.push(format!(
                "Resource Profile '{}': descriptor unavailable: {error}",
                declared.scheme
            )),
        }
    }

    let mut desired = BTreeMap::new();
    for spec in live_specs {
        let declaration = lexical_absolute(&spec.path).unwrap_or_else(|_| spec.path.clone());
        let agent_dir = declaration.parent().unwrap_or(Path::new("/"));
        for resource in &spec.resources {
            if resource.inactive_reason().is_some() {
                continue;
            }
            let Some((scheme, _)) = resource.uri().split_once(':') else {
                continue;
            };
            let (Some(descriptor), Some(runtime), Some(generation)) = (
                descriptors.get(scheme),
                runtimes.get(scheme),
                generations.get(scheme),
            ) else {
                continue;
            };
            let selector = resource
                .selector()
                .cloned()
                .unwrap_or_else(|| descriptor.default_selector.clone());
            if let Err(error) = descriptor.validate_selector(&selector) {
                warnings.push(format!(
                    "Resource Profile '{}' binding {} resource '{}': {error}",
                    scheme,
                    spec.bus_id(this_host),
                    resource.name()
                ));
                continue;
            }
            let resolution = match refresh.try_resolve(agent_dir, resource.uri()) {
                Ok(Some(resolution)) => resolution,
                Ok(None) => {
                    warnings.push(format!(
                        "Resource Profile '{}' binding {} resource '{}': resolver returned no carrier",
                        scheme,
                        spec.bus_id(this_host),
                        resource.name()
                    ));
                    continue;
                }
                Err(error) => {
                    warnings.push(format!(
                        "Resource Profile '{}' binding {} resource '{}': {error}",
                        scheme,
                        spec.bus_id(this_host),
                        resource.name()
                    ));
                    continue;
                }
            };
            let target = match SnapshotTarget::new(
                resolution.containment_root,
                &resolution.path,
            ) {
                Ok(target) => target,
                Err(error) => {
                    warnings.push(format!(
                        "Resource Profile '{}' binding {} resource '{}': {error}",
                        scheme,
                        spec.bus_id(this_host),
                        resource.name()
                    ));
                    continue;
                }
            };
            let recipient = spec.bus_id(this_host);
            let stable_key = format!("{recipient}\0{}", resource.name());
            desired.insert(
                stable_key.clone(),
                DesiredBinding {
                    stable_key,
                    catalog_root: catalog_root.to_path_buf(),
                    this_host: this_host.to_owned(),
                    recipient,
                    binding_name: resource.name().to_owned(),
                    scheme: scheme.to_owned(),
                    generation: *generation,
                    topology: descriptor.runtime.topology,
                    argv: runtime.argv.clone(),
                    uri: resource.uri().to_owned(),
                    selector,
                    descriptor: descriptor.clone(),
                    target,
                },
            );
        }
    }
    (desired, warnings)
}

struct Worker {
    catalog_root: PathBuf,
    this_host: String,
    tx: SyncSender<Msg>,
    runtimes: BTreeMap<RuntimeKey, RuntimeProcess>,
}

impl Worker {
    fn new(catalog_root: PathBuf, this_host: String, tx: SyncSender<Msg>) -> Self {
        Self {
            catalog_root,
            this_host,
            tx,
            runtimes: BTreeMap::new(),
        }
    }

    fn run(mut self, rx: Receiver<Msg>) {
        while let Ok(message) = rx.recv() {
            match message {
                Msg::Refresh { desired, reply } => {
                    let warnings = self.reconcile(desired);
                    let _ = reply.send(warnings);
                }
                Msg::Deactivate { recipient, reply } => {
                    self.deactivate_recipient(&recipient);
                    let _ = reply.send(());
                }
                Msg::Health { reply } => {
                    let health = self
                        .runtimes
                        .values()
                        .flat_map(RuntimeProcess::health)
                        .collect();
                    let _ = reply.send(health);
                }
                Msg::RuntimeOutput { key, owner, output } => {
                    self.runtime_output(&key, &owner, output);
                }
                Msg::RuntimeEof { key, owner } => {
                    self.runtime_failed(&key, &owner, "runtime protocol reached EOF");
                }
                Msg::WriterFailed { key, owner, error } => {
                    self.runtime_failed(&key, &owner, &error);
                }
                Msg::Shutdown { reply } => {
                    self.stop_all();
                    let _ = reply.send(());
                    break;
                }
            }
        }
        self.stop_all();
    }

    fn reconcile(&mut self, desired: BTreeMap<String, DesiredBinding>) -> Vec<String> {
        let mut warnings = Vec::new();
        let desired_runtime_keys = desired
            .values()
            .map(DesiredBinding::runtime_key)
            .collect::<BTreeSet<_>>();
        let obsolete = self
            .runtimes
            .keys()
            .filter(|key| !desired_runtime_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in obsolete {
            if let Some(mut runtime) = self.runtimes.remove(&key) {
                runtime.deactivate_all();
                runtime.stop();
            }
        }

        let mut grouped: BTreeMap<RuntimeKey, Vec<DesiredBinding>> = BTreeMap::new();
        for binding in desired.into_values() {
            grouped.entry(binding.runtime_key()).or_default().push(binding);
        }
        for (key, bindings) in grouped {
            if !self.runtimes.contains_key(&key) {
                match RuntimeProcess::spawn(
                    key.clone(),
                    &bindings[0],
                    self.tx.clone(),
                    &self.catalog_root,
                    &self.this_host,
                ) {
                    Ok(runtime) => {
                        self.runtimes.insert(key.clone(), runtime);
                    }
                    Err(error) => {
                        warnings.push(format!(
                            "Resource Profile '{}': runtime spawn failed: {error:#}",
                            bindings[0].scheme
                        ));
                        continue;
                    }
                }
            }
            let Some(runtime) = self.runtimes.get_mut(&key) else {
                continue;
            };
            if let Err(error) = runtime.reconcile_bindings(bindings) {
                warnings.push(format!(
                    "Resource Profile '{}': runtime registration failed: {error:#}",
                    runtime.scheme
                ));
                if let Some(mut failed) = self.runtimes.remove(&key) {
                    failed.stop();
                }
            }
        }
        warnings
    }

    fn deactivate_recipient(&mut self, recipient: &str) {
        for runtime in self.runtimes.values_mut() {
            runtime.deactivate_recipient(recipient);
        }
        let empty = self
            .runtimes
            .iter()
            .filter_map(|(key, runtime)| runtime.bindings.is_empty().then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in empty {
            if let Some(mut runtime) = self.runtimes.remove(&key) {
                runtime.stop();
            }
        }
    }

    fn runtime_output(
        &mut self,
        key: &RuntimeKey,
        owner: &RuntimeOwner,
        output: Result<RuntimeMessage, String>,
    ) {
        let Some(runtime) = self.runtimes.get_mut(key) else {
            return;
        };
        if !owner_matches(Some(&runtime.owner), owner) {
            return;
        }
        let failure = match output {
            Ok(message) => runtime
                .accept(message, &self.catalog_root, &self.this_host)
                .err()
                .map(|error| format!("runtime output rejected: {error:#}")),
            Err(error) => Some(format!("runtime protocol error: {error}")),
        };
        if let Some(detail) = failure {
            self.runtime_failed(key, owner, &detail);
        }
    }

    fn runtime_failed(&mut self, key: &RuntimeKey, owner: &RuntimeOwner, detail: &str) {
        if !owner_matches(self.runtimes.get(key).map(|runtime| &runtime.owner), owner) {
            return;
        }
        if let Some(mut runtime) = self.runtimes.remove(key) {
            eprintln!("st2: Resource Profile '{}': {detail}", runtime.scheme);
            runtime.stop();
        }
    }

    fn stop_all(&mut self) {
        for (_, mut runtime) in std::mem::take(&mut self.runtimes) {
            runtime.deactivate_all();
            runtime.stop();
        }
    }
}

struct RuntimeProcess {
    scheme: String,
    owner: RuntimeOwner,
    lifecycle: RuntimeLifecycle,
    child: Child,
    writer: Option<SyncSender<Vec<u8>>>,
    writer_thread: Option<JoinHandle<()>>,
    reader_thread: Option<JoinHandle<()>>,
    bindings: BTreeMap<String, ActiveBinding>,
    process_health: ResourceProfileHealth,
}

struct ActiveBinding {
    desired: DesiredBinding,
    binding_id: BindingId,
    registration: RegistrationToken,
    catch_up: CatchUp,
    health: ResourceProfileHealth,
}

impl RuntimeProcess {
    fn spawn(
        key: RuntimeKey,
        sample: &DesiredBinding,
        supervisor_tx: SyncSender<Msg>,
        catalog_root: &Path,
        this_host: &str,
    ) -> anyhow::Result<Self> {
        let executable = sample
            .argv
            .first()
            .context("runtime argv is unexpectedly empty")?;
        let mut command = Command::new(executable);
        command
            .args(&sample.argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn().with_context(|| format!("spawn {executable:?}"))?;
        let stdin = child.stdin.take().context("capture runtime stdin")?;
        let stdout = child.stdout.take().context("capture runtime stdout")?;
        let sequence = ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let incarnation = RuntimeIncarnation::new(format!("{}-{sequence}", sample.generation))?;
        let claim = OwnerClaim::new(hash_text(&format!(
            "{}\0{}\0{}\0{sequence}",
            catalog_root.display(),
            this_host,
            sample.scheme
        )))?;
        let owner = RuntimeOwner::new(incarnation, claim);
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(owner.clone());

        let (writer_tx, writer_rx) = mpsc::sync_channel::<Vec<u8>>(WRITER_CAPACITY);
        let writer_key = key.clone();
        let writer_owner = owner.clone();
        let writer_supervisor = supervisor_tx.clone();
        let writer_thread = thread::Builder::new()
            .name("st2-resource-profile-stdin".to_owned())
            .spawn(move || {
                runtime_writer(stdin, writer_rx, writer_key, writer_owner, writer_supervisor)
            })?;
        let reader_key = key.clone();
        let reader_owner = owner.clone();
        let reader_thread = thread::Builder::new()
            .name("st2-resource-profile-stdout".to_owned())
            .spawn(move || runtime_reader(stdout, reader_key, reader_owner, supervisor_tx))?;

        Ok(Self {
            scheme: sample.scheme.clone(),
            owner,
            lifecycle,
            child,
            writer: Some(writer_tx),
            writer_thread: Some(writer_thread),
            reader_thread: Some(reader_thread),
            bindings: BTreeMap::new(),
            process_health: ResourceProfileHealth {
                scheme: sample.scheme.clone(),
                binding: None,
                state: RuntimeHealthState::Starting,
                detail: None,
            },
        })
    }

    fn reconcile_bindings(&mut self, desired: Vec<DesiredBinding>) -> anyhow::Result<()> {
        let desired_keys = desired
            .iter()
            .map(|binding| binding.stable_key.clone())
            .collect::<BTreeSet<_>>();
        let removed = self
            .bindings
            .keys()
            .filter(|key| !desired_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in removed {
            self.unregister(&key);
        }
        for binding in desired {
            if self
                .bindings
                .get(&binding.stable_key)
                .is_some_and(|active| active.desired == binding)
            {
                if let Some(active) = self.bindings.get_mut(&binding.stable_key) {
                    let _ = emit_pending_for(active);
                }
                continue;
            }
            self.unregister(&binding.stable_key);
            self.register(binding)?;
        }
        Ok(())
    }

    fn register(&mut self, desired: DesiredBinding) -> anyhow::Result<()> {
        let sequence = ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let binding_id = BindingId::new(hash_text(&format!(
            "{}\0{}\0{}",
            desired.stable_key, desired.generation, sequence
        )))?;
        let registration = RegistrationToken::new(hash_text(&format!(
            "{}\0{}\0{sequence}",
            self.owner.claim().as_str(), desired.stable_key
        )))?;
        let selection = selector_topics(&desired.selector)?;
        let contract = PublicationContract::new(
            desired.descriptor.snapshot.schema_id.clone(),
            desired.descriptor.snapshot.media_type.clone(),
            desired
                .descriptor
                .topics
                .iter()
                .map(|topic| topic.name.clone()),
            selection,
        )?;
        let registration_state = BindingRegistration::new(
            binding_id.clone(),
            registration.clone(),
            desired.target.clone(),
            contract,
        );
        self.lifecycle.register(&self.owner, registration_state)?;

        let state_directory = binding_state_directory(&desired)?;
        std::fs::create_dir_all(&state_directory)
            .with_context(|| format!("create {}", state_directory.display()))?;
        let mut catch_up = CatchUp::open_for_snapshot(&state_directory, &desired.target)?;
        let pending = catch_up.set_deliverable(true)?;
        let previous_digest = catch_up.state().current_snapshot_digest();
        let message = HostMessage::Register {
            owner: self.owner.clone(),
            binding_id: binding_id.clone(),
            registration: registration.clone(),
            uri: desired.uri.clone(),
            selector: desired.selector.clone(),
            carrier_path: desired.target.path(),
            previous_digest,
        };
        self.send(message)?;
        let mut active = ActiveBinding {
            health: ResourceProfileHealth {
                scheme: desired.scheme.clone(),
                binding: Some(desired.binding_name.clone()),
                state: RuntimeHealthState::Starting,
                detail: None,
            },
            desired,
            binding_id,
            registration,
            catch_up,
        };
        if pending.is_some() {
            let _ = emit_pending_for(&mut active);
        }
        self.bindings
            .insert(active.desired.stable_key.clone(), active);
        Ok(())
    }

    fn unregister(&mut self, stable_key: &str) {
        let Some(mut active) = self.bindings.remove(stable_key) else {
            return;
        };
        let _ = active.catch_up.set_deliverable(false);
        let message = HostMessage::Unregister {
            owner: self.owner.clone(),
            binding_id: active.binding_id.clone(),
            registration: active.registration.clone(),
        };
        let _ = self.send(message);
        let _ = self.lifecycle.unregister(
            &self.owner,
            &active.binding_id,
            &active.registration,
        );
    }

    fn deactivate_recipient(&mut self, recipient: &str) {
        let removed = self
            .bindings
            .iter()
            .filter_map(|(key, active)| {
                (active.desired.recipient == recipient).then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in removed {
            self.unregister(&key);
        }
    }

    fn deactivate_all(&mut self) {
        let keys = self.bindings.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.unregister(&key);
        }
    }

    fn accept(
        &mut self,
        message: RuntimeMessage,
        catalog_root: &Path,
        this_host: &str,
    ) -> anyhow::Result<()> {
        let binding_id = match &message {
            RuntimeMessage::Publish { binding_id, .. } => Some(binding_id.clone()),
            RuntimeMessage::Health { binding_id, .. } => binding_id.clone(),
        };
        match self.lifecycle.accept_output(&message)? {
            AcceptedOutput::Publication(publication) => {
                let active = self
                    .bindings
                    .values_mut()
                    .find(|active| Some(&active.binding_id) == binding_id.as_ref())
                    .context("accepted publication has no active binding")?;
                let (_, pending) = active.catch_up.publish(publication)?;
                if pending.is_some() {
                    emit_pending(catalog_root, this_host, active)?;
                }
            }
            AcceptedOutput::Health(health) => {
                if let Some(binding_id) = health.binding_id() {
                    if let Some(active) = self
                        .bindings
                        .values_mut()
                        .find(|active| &active.binding_id == binding_id)
                    {
                        active.health.state = health.state();
                        active.health.detail = health.detail().map(str::to_owned);
                    }
                } else {
                    self.process_health.state = health.state();
                    self.process_health.detail = health.detail().map(str::to_owned);
                }
            }
        }
        Ok(())
    }

    fn send(&self, message: HostMessage) -> anyhow::Result<()> {
        let line = encode_host_line(&message)?;
        let writer = self.writer.as_ref().context("runtime stdin is closed")?;
        match writer.try_send(line) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => anyhow::bail!("runtime stdin queue is full"),
            Err(TrySendError::Disconnected(_)) => anyhow::bail!("runtime stdin is disconnected"),
        }
    }

    fn health(&self) -> Vec<ResourceProfileHealth> {
        std::iter::once(self.process_health.clone())
            .chain(self.bindings.values().map(|active| active.health.clone()))
            .collect()
    }

    fn stop(&mut self) {
        self.writer.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.writer_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.reader_thread.take() {
            let _ = thread.join();
        }
    }
}

fn runtime_writer(
    mut stdin: std::process::ChildStdin,
    rx: Receiver<Vec<u8>>,
    key: RuntimeKey,
    owner: RuntimeOwner,
    supervisor: SyncSender<Msg>,
) {
    while let Ok(line) = rx.recv() {
        if let Err(error) = stdin.write_all(&line).and_then(|_| stdin.flush()) {
            let _ = supervisor.send(Msg::WriterFailed {
                key,
                owner,
                error: format!("runtime stdin failed: {error}"),
            });
            return;
        }
    }
}

fn runtime_reader(
    stdout: std::process::ChildStdout,
    key: RuntimeKey,
    owner: RuntimeOwner,
    supervisor: SyncSender<Msg>,
) {
    let mut reader = BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader) {
            Ok(Some(line)) => {
                let output = decode_runtime_line(&line).map_err(|error| error.to_string());
                if supervisor
                    .send(Msg::RuntimeOutput {
                        key: key.clone(),
                        owner: owner.clone(),
                        output,
                    })
                    .is_err()
                {
                    return;
                }
            }
            Ok(None) => {
                let _ = supervisor.send(Msg::RuntimeEof { key, owner });
                return;
            }
            Err(error) => {
                let _ = supervisor.send(Msg::RuntimeOutput {
                    key,
                    owner,
                    output: Err(error.to_string()),
                });
                return;
            }
        }
    }
}

fn read_bounded_line(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() { Ok(None) } else { Ok(Some(line)) };
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            let consumed = newline + 1;
            if line.len() + consumed > MAX_PROTOCOL_LINE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime protocol line exceeds 2 MiB",
                ));
            }
            line.extend_from_slice(&available[..consumed]);
            reader.consume(consumed);
            return Ok(Some(line));
        }
        if line.len() + available.len() >= MAX_PROTOCOL_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime protocol line exceeds 2 MiB",
            ));
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
}

fn selector_topics(selector: &Value) -> anyhow::Result<TopicSelection> {
    let topics = selector
        .as_object()
        .and_then(|object| object.get("topics"))
        .and_then(Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .map(|topic| {
                    topic
                        .as_str()
                        .map(str::to_owned)
                        .context("selector topic is not a string")
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(TopicSelection::new(topics)?)
}

fn profile_generation(
    catalog_generation: u64,
    declared: &crate::catalog::DeclaredProfile,
    descriptor: &ProfileDescriptor,
    profiles: &ResourceProfileRegistry,
) -> u64 {
    let module_identity = profiles
        .get(&declared.scheme)
        .and_then(|profile| profile.module())
        .and_then(|module| std::fs::metadata(module).ok())
        .map(|metadata| {
            format!(
                "{}:{:?}:{:?}",
                metadata.len(),
                metadata.modified().ok(),
                metadata.created().ok()
            )
        })
        .unwrap_or_default();
    let input = format!(
        "{catalog_generation}\0{}\0{}\0{}\0{}\0{:?}\0{:?}\0{module_identity}",
        declared.scheme,
        declared.wasm,
        declared.class,
        declared.notify_chain,
        declared.runtime.as_ref().map(|runtime| &runtime.argv),
        descriptor,
    );
    let digest = Sha256::digest(input.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix is eight bytes"))
}

fn emit_pending(catalog_root: &Path, this_host: &str, active: &mut ActiveBinding) -> anyhow::Result<()> {
    emit_pending_at(catalog_root, this_host, active)
}

fn emit_pending_for(active: &mut ActiveBinding) -> anyhow::Result<()> {
    let root = active.desired.catalog_root.clone();
    let host = active.desired.this_host.clone();
    emit_pending_at(&root, &host, active)
}

fn emit_pending_at(
    catalog_root: &Path,
    this_host: &str,
    active: &mut ActiveBinding,
) -> anyhow::Result<()> {
    let Some(delivery) = active.catch_up.pending_delivery() else {
        return Ok(());
    };
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Body<'a> {
        binding: &'a str,
        snapshot_digest: String,
        topics: &'a [String],
        facts: &'a [ResourceFact],
    }
    let digest = delivery.digest();
    let topics = delivery.selected_topics().to_vec();
    let facts = delivery.facts();
    let body = serde_json::to_string(&Body {
        binding: &active.desired.binding_name,
        snapshot_digest: digest.to_string(),
        topics: &topics,
        facts,
    })?;
    let event_id = publication_event_id(
        &active.desired.recipient,
        &active.desired.binding_name,
        digest,
    );
    let subject = resource_change_subject(
        &active.desired.binding_name,
        facts,
        &topics,
        "snapshot updated",
    );
    crate::event::emit_builtin_resync(
        catalog_root,
        this_host,
        &active.desired.recipient,
        &event_id,
        Some(&active.desired.binding_name),
        Some(&subject),
        &body,
        true,
    )?;
    active.catch_up.acknowledge_delivery(digest)?;
    Ok(())
}

fn publication_event_id(recipient: &str, binding: &str, digest: SnapshotDigest) -> String {
    hash_text(&format!("resource-profile\0{recipient}\0{binding}\0{digest}"))
}

pub(crate) fn resource_change_subject(
    binding: &str,
    facts: &[ResourceFact],
    topics: &[String],
    fallback: &str,
) -> String {
    const SUBJECT_MAX_SCALARS: usize = 96;
    const MAX_RENDERED_FACTS: usize = 3;

    let topic_suffix = if topics.is_empty() {
        String::new()
    } else {
        format!(" [{}]", topics.join(", "))
    };
    let base = format!("{binding} · ");
    let mut rendered = Vec::new();
    for fact in facts.iter().take(MAX_RENDERED_FACTS) {
        let fact = render_fact(fact);
        let candidate = format!("{base}{}{topic_suffix}", {
            let mut candidate_facts = rendered.clone();
            candidate_facts.push(fact.clone());
            candidate_facts.join("; ")
        });
        if candidate.chars().count() > SUBJECT_MAX_SCALARS {
            break;
        }
        rendered.push(fact);
    }

    let detail = if rendered.is_empty() {
        fallback.to_owned()
    } else {
        rendered.join("; ")
    };
    let subject = format!("{base}{detail}{topic_suffix}");
    if subject.chars().count() <= SUBJECT_MAX_SCALARS {
        return subject;
    }

    // Facts and topic names are never clipped. A pathological oversized binding or topic suffix
    // falls back to the binding and bounded generic detail; the durable body remains complete.
    let fallback = format!("{binding} · {fallback}");
    if fallback.chars().count() <= SUBJECT_MAX_SCALARS {
        fallback
    } else {
        fallback.chars().take(SUBJECT_MAX_SCALARS).collect()
    }
}

fn render_fact(fact: &ResourceFact) -> String {
    match (fact.before(), fact.after()) {
        (None, Some(Some(after))) => format!("{}={after}", fact.key()),
        (None, Some(None)) => format!("{}=removed", fact.key()),
        (Some(None), Some(Some(after))) => format!("{}=+{after}", fact.key()),
        (Some(Some(before)), Some(None)) => format!("{}=-{before}", fact.key()),
        (Some(Some(before)), Some(Some(after))) => {
            format!("{}={before}→{after}", fact.key())
        }
        (Some(None), Some(None)) => format!("{}=absent", fact.key()),
        (Some(Some(before)), None) => format!("{} was {before}", fact.key()),
        (Some(None), None) => format!("{} was absent", fact.key()),
        (None, None) => unreachable!("validated facts always carry a value"),
    }
}

fn binding_state_directory(desired: &DesiredBinding) -> anyhow::Result<PathBuf> {
    let state = lexical_absolute(&crate::run::state_root())?;
    Ok(state
        .join("st2")
        .join("resource-profiles")
        .join(hash_path(&desired.catalog_root))
        .join(hash_text(&desired.this_host))
        .join(hash_text(&format!(
            "{}\0{}\0{}",
            desired.recipient, desired.scheme, desired.binding_name
        ))))
}

fn lexical_absolute(path: &Path) -> anyhow::Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
        }
    }
    Ok(normalized)
}

fn hash_path(path: &Path) -> String {
    hash_text(&path.to_string_lossy())
}

fn owner_matches(current: Option<&RuntimeOwner>, message: &RuntimeOwner) -> bool {
    current == Some(message)
}

fn hash_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reader_accepts_one_maximal_line_and_rejects_overflow() {
        let mut maximal = vec![b'x'; MAX_PROTOCOL_LINE_BYTES - 1];
        maximal.push(b'\n');
        assert_eq!(
            read_bounded_line(&mut maximal.as_slice()).unwrap().unwrap().len(),
            MAX_PROTOCOL_LINE_BYTES
        );
        let mut overflow = vec![b'x'; MAX_PROTOCOL_LINE_BYTES];
        overflow.push(b'\n');
        assert!(read_bounded_line(&mut overflow.as_slice()).is_err());
    }

    #[test]
    fn runtime_keys_enforce_shared_and_per_binding_topology() {
        let shared_a = RuntimeKey::Shared {
            scheme: "dev.x".into(),
            generation: 7,
        };
        let shared_b = RuntimeKey::Shared {
            scheme: "dev.x".into(),
            generation: 7,
        };
        assert_eq!(shared_a, shared_b);
        let per_a = RuntimeKey::PerBinding {
            scheme: "dev.x".into(),
            generation: 7,
            binding: hash_text("a"),
        };
        let per_b = RuntimeKey::PerBinding {
            scheme: "dev.x".into(),
            generation: 7,
            binding: hash_text("b"),
        };
        assert_ne!(per_a, per_b);
    }

    #[test]
    fn stale_output_key_cannot_alias_a_hot_reloaded_generation() {
        assert_ne!(
            RuntimeKey::Shared {
                scheme: "dev.x".into(),
                generation: 1,
            },
            RuntimeKey::Shared {
                scheme: "dev.x".into(),
                generation: 2,
            }
        );
    }

    #[test]
    fn stale_owner_envelopes_cannot_target_a_replacement_with_the_same_runtime_key() {
        let old = RuntimeOwner::new(
            RuntimeIncarnation::new("incarnation-1").unwrap(),
            OwnerClaim::new("claim-1").unwrap(),
        );
        let replacement = RuntimeOwner::new(
            RuntimeIncarnation::new("incarnation-2").unwrap(),
            OwnerClaim::new("claim-2").unwrap(),
        );
        assert!(owner_matches(Some(&replacement), &replacement));
        assert!(!owner_matches(Some(&replacement), &old));
        assert!(!owner_matches(None, &old));
    }

    #[test]
    fn publication_subject_renders_ordered_facts_and_reserves_topics() {
        let facts = vec![
            ResourceFact::current("state", "ready").unwrap(),
            ResourceFact::transition("label", None::<String>, Some("bug")).unwrap(),
            ResourceFact::transition("owner", Some("alice"), Some("bob")).unwrap(),
            ResourceFact::current("fourth", "omitted").unwrap(),
        ];
        assert_eq!(
            resource_change_subject(
                "review",
                &facts,
                &["ci.failure".to_owned()],
                "snapshot updated"
            ),
            "review · state=ready; label=+bug; owner=alice→bob [ci.failure]"
        );
    }

    #[test]
    fn publication_subject_drops_low_priority_facts_atomically_within_96_scalars() {
        let facts = vec![
            ResourceFact::current("priority", "x".repeat(80)).unwrap(),
            ResourceFact::current("lower", "must-not-leapfrog").unwrap(),
        ];
        let subject = resource_change_subject(
            "review",
            &facts,
            &["selected.topic".to_owned()],
            "snapshot updated",
        );
        assert_eq!(subject, "review · snapshot updated [selected.topic]");
        assert!(subject.chars().count() <= 96);
    }

    #[test]
    fn publication_subject_without_facts_has_a_useful_compatible_fallback() {
        assert_eq!(
            resource_change_subject("review", &[], &[], "snapshot updated"),
            "review · snapshot updated"
        );
    }

    #[test]
    fn stale_protocol_failure_does_not_remove_the_replacement_process() {
        let old = RuntimeOwner::new(
            RuntimeIncarnation::new("incarnation-1").unwrap(),
            OwnerClaim::new("claim-1").unwrap(),
        );
        let replacement = RuntimeOwner::new(
            RuntimeIncarnation::new("incarnation-2").unwrap(),
            OwnerClaim::new("claim-2").unwrap(),
        );
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(replacement.clone());
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--help")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let key = RuntimeKey::Shared {
            scheme: "dev.x".into(),
            generation: 1,
        };
        let (tx, _rx) = mpsc::sync_channel(1);
        let mut worker = Worker::new(PathBuf::from("/"), "host".into(), tx);
        worker.runtimes.insert(
            key.clone(),
            RuntimeProcess {
                scheme: "dev.x".into(),
                owner: replacement,
                lifecycle,
                child,
                writer: None,
                writer_thread: None,
                reader_thread: None,
                bindings: BTreeMap::new(),
                process_health: ResourceProfileHealth {
                    scheme: "dev.x".into(),
                    binding: None,
                    state: RuntimeHealthState::Starting,
                    detail: None,
                },
            },
        );

        worker.runtime_output(&key, &old, Err("stale malformed output".into()));
        assert!(worker.runtimes.contains_key(&key));
        worker.stop_all();
    }
}
