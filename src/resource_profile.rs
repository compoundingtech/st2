//! Observable Resource Profile runtime protocol, ownership, publication, and catch-up core.
//!
//! This module deliberately stops at the supervisor integration boundary: it does not spawn a
//! runtime or enqueue resync records. It validates and fences runtime output, publishes the one
//! canonical snapshot, and exposes level-triggered delivery work for the existing event ingress.

use std::collections::{BTreeSet, HashMap};
use std::ffi::{CString, OsStr};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub use st2_resource_protocol::{
    BindingId, FactError, FactValue, HostMessage, OpaqueIdError, OwnerClaim, ProtocolError,
    RegistrationToken, ResourceFact, RuntimeHealthState, RuntimeIncarnation, RuntimeMessage,
    RuntimeOwner, SnapshotBytes, SnapshotDigest, SnapshotSizeError, MAX_FACTS, MAX_FACT_KEY_BYTES,
    MAX_FACT_VALUE_BYTES, MAX_HEALTH_DETAIL_BYTES, MAX_PROTOCOL_LINE_BYTES, MAX_SELECTOR_BYTES,
    MAX_SNAPSHOT_BYTES, decode_host_line, decode_runtime_line, encode_host_line,
    encode_runtime_line,
};

// Covers the prior state envelope plus 32 maximally sized facts after worst-case JSON escaping.
const MAX_CATCH_UP_FILE_BYTES: usize = 256 * 1024;
const CATCH_UP_FILE: &str = "resource-profile-catch-up.json";
const PUBLICATION_INTENT_FILE: &str = "resource-profile-publication-intent.json";
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSelection {
    topics: BTreeSet<String>,
}

impl TopicSelection {
    pub fn new(topics: impl IntoIterator<Item = String>) -> Result<Self, ContractError> {
        let mut normalized = BTreeSet::new();
        for topic in topics {
            if topic.is_empty() {
                return Err(ContractError::EmptyTopic);
            }
            if !normalized.insert(topic) {
                return Err(ContractError::DuplicateTopic);
            }
        }
        Ok(Self { topics: normalized })
    }

    pub fn contains(&self, topic: &str) -> bool {
        self.topics.contains(topic)
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.topics.iter().map(String::as_str)
    }

    fn select(&self, published: &[String]) -> Vec<String> {
        published
            .iter()
            .filter(|topic| self.contains(topic))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    EmptySchemaId,
    EmptyMediaType,
    EmptyTopic,
    DuplicateTopic,
    SelectedTopicNotPublished(String),
}

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySchemaId => formatter.write_str("schema id must not be empty"),
            Self::EmptyMediaType => formatter.write_str("media type must not be empty"),
            Self::EmptyTopic => formatter.write_str("topic names must not be empty"),
            Self::DuplicateTopic => formatter.write_str("topic names must be unique"),
            Self::SelectedTopicNotPublished(topic) => {
                write!(formatter, "selected topic is not published: {topic}")
            }
        }
    }
}

impl std::error::Error for ContractError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationContract {
    schema_id: String,
    media_type: String,
    published_topics: BTreeSet<String>,
    selection: TopicSelection,
}

impl PublicationContract {
    pub fn new(
        schema_id: impl Into<String>,
        media_type: impl Into<String>,
        published_topics: impl IntoIterator<Item = String>,
        selection: TopicSelection,
    ) -> Result<Self, ContractError> {
        let schema_id = schema_id.into();
        if schema_id.is_empty() {
            return Err(ContractError::EmptySchemaId);
        }
        let media_type = media_type.into();
        if media_type.is_empty() {
            return Err(ContractError::EmptyMediaType);
        }
        let mut normalized = BTreeSet::new();
        for topic in published_topics {
            if topic.is_empty() {
                return Err(ContractError::EmptyTopic);
            }
            if !normalized.insert(topic) {
                return Err(ContractError::DuplicateTopic);
            }
        }
        for topic in selection.topics() {
            if !normalized.contains(topic) {
                return Err(ContractError::SelectedTopicNotPublished(topic.to_owned()));
            }
        }
        Ok(Self {
            schema_id,
            media_type,
            published_topics: normalized,
            selection,
        })
    }

    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn published_topics(&self) -> impl Iterator<Item = &str> {
        self.published_topics.iter().map(String::as_str)
    }

    pub fn selection(&self) -> &TopicSelection {
        &self.selection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTarget {
    root: PathBuf,
    relative: PathBuf,
}

impl SnapshotTarget {
    pub fn new(root: impl Into<PathBuf>, carrier_path: impl AsRef<Path>) -> Result<Self, PathError> {
        let root = root.into();
        validate_absolute_path(&root).map_err(PathError::UnsafeRoot)?;
        let carrier_path = carrier_path.as_ref();
        let relative = if carrier_path.is_absolute() {
            carrier_path
                .strip_prefix(&root)
                .map_err(|_| PathError::EscapesRoot)?
                .to_path_buf()
        } else {
            carrier_path.to_path_buf()
        };
        validate_relative_path(&relative).map_err(PathError::UnsafeCarrier)?;
        Ok(Self { root, relative })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn relative_path(&self) -> &Path {
        &self.relative
    }

    pub fn path(&self) -> PathBuf {
        self.root.join(&self.relative)
    }

    /// Digest the currently published contained snapshot, if present.
    pub fn current_digest(&self) -> Result<Option<SnapshotDigest>, PublicationError> {
        let parent = self.relative.parent().unwrap_or_else(|| Path::new(""));
        let leaf = self
            .relative
            .file_name()
            .ok_or_else(|| PublicationError::UnsafeTarget(PathError::UnsafeCarrier("missing leaf")))?;
        let directory = match open_absolute_dir_beneath(&self.root, parent) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(PublicationError::Io(error)),
        };
        read_regular_optional_at(&directory, leaf, MAX_SNAPSHOT_BYTES)
            .map_err(|error| match error {
                BoundedReadError::TooLarge => PublicationError::ExistingSnapshotTooLarge,
                BoundedReadError::NotRegular => PublicationError::SnapshotNotRegular,
                BoundedReadError::Io(error) => PublicationError::Io(error),
            })
            .map(|bytes| bytes.map(|bytes| SnapshotDigest::of(&bytes)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    EscapesRoot,
    UnsafeRoot(&'static str),
    UnsafeCarrier(&'static str),
}

impl fmt::Display for PathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EscapesRoot => formatter.write_str("snapshot path escapes its containment root"),
            Self::UnsafeRoot(reason) => write!(formatter, "unsafe containment root: {reason}"),
            Self::UnsafeCarrier(reason) => write!(formatter, "unsafe snapshot path: {reason}"),
        }
    }
}

impl std::error::Error for PathError {}

fn validate_absolute_path(path: &Path) -> Result<(), &'static str> {
    if !path.is_absolute() {
        return Err("path is not absolute");
    }
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err("path is not lexically normalized");
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), &'static str> {
    let mut any = false;
    for component in path.components() {
        let Component::Normal(_) = component else {
            return Err("path contains a non-normal component");
        };
        any = true;
    }
    if !any {
        return Err("path must name a file beneath the root");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRegistration {
    binding_id: BindingId,
    registration: RegistrationToken,
    target: SnapshotTarget,
    contract: PublicationContract,
}

impl BindingRegistration {
    pub fn new(
        binding_id: BindingId,
        registration: RegistrationToken,
        target: SnapshotTarget,
        contract: PublicationContract,
    ) -> Self {
        Self {
            binding_id,
            registration,
            target,
            contract,
        }
    }

    pub fn binding_id(&self) -> &BindingId {
        &self.binding_id
    }

    pub fn registration(&self) -> &RegistrationToken {
        &self.registration
    }

    pub fn target(&self) -> &SnapshotTarget {
        &self.target
    }

    pub fn contract(&self) -> &PublicationContract {
        &self.contract
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationChange {
    Added,
    Replaced,
}

#[derive(Debug, Default)]
pub struct RuntimeLifecycle {
    owner: Option<RuntimeOwner>,
    bindings: HashMap<BindingId, BindingRegistration>,
}

impl RuntimeLifecycle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn owner(&self) -> Option<&RuntimeOwner> {
        self.owner.as_ref()
    }

    pub fn claim(&mut self, owner: RuntimeOwner) -> bool {
        if self.owner.as_ref() == Some(&owner) {
            return false;
        }
        self.owner = Some(owner);
        self.bindings.clear();
        true
    }

    pub fn register(
        &mut self,
        owner: &RuntimeOwner,
        registration: BindingRegistration,
    ) -> Result<RegistrationChange, FenceError> {
        self.require_owner(owner)?;
        let change = if self
            .bindings
            .insert(registration.binding_id.clone(), registration)
            .is_some()
        {
            RegistrationChange::Replaced
        } else {
            RegistrationChange::Added
        };
        Ok(change)
    }

    pub fn unregister(
        &mut self,
        owner: &RuntimeOwner,
        binding_id: &BindingId,
        registration: &RegistrationToken,
    ) -> Result<BindingRegistration, FenceError> {
        self.require_registration(owner, binding_id, registration)?;
        Ok(self
            .bindings
            .remove(binding_id)
            .expect("registration was checked immediately before removal"))
    }

    pub fn accept_output<'a>(
        &'a self,
        message: &'a RuntimeMessage,
    ) -> Result<AcceptedOutput<'a>, FenceError> {
        match message {
            RuntimeMessage::Publish {
                owner,
                binding_id,
                registration,
                schema_id,
                media_type,
                bytes,
                topics,
                facts,
                observed_at,
            } => {
                let binding = self.require_registration(owner, binding_id, registration)?;
                if schema_id != binding.contract.schema_id() {
                    return Err(FenceError::ContractMismatch { field: "schemaId" });
                }
                if media_type != binding.contract.media_type() {
                    return Err(FenceError::ContractMismatch { field: "mediaType" });
                }
                let mut unique = BTreeSet::new();
                for topic in topics {
                    if topic.is_empty() || !unique.insert(topic.as_str()) {
                        return Err(FenceError::InvalidTopics);
                    }
                    if !binding.contract.published_topics.contains(topic) {
                        return Err(FenceError::UnpublishedTopic(topic.clone()));
                    }
                }
                Ok(AcceptedOutput::Publication(AcceptedPublication {
                    target: &binding.target,
                    bytes,
                    selected_topics: binding.contract.selection.select(topics),
                    facts: facts.as_deref().unwrap_or_default(),
                    observed_at: observed_at.as_deref(),
                }))
            }
            RuntimeMessage::Health {
                owner,
                binding_id,
                registration,
                state,
                detail,
            } => {
                self.require_owner(owner)?;
                match (binding_id, registration) {
                    (Some(binding_id), Some(registration)) => {
                        self.require_registration(owner, binding_id, registration)?;
                    }
                    (None, None) => {}
                    _ => return Err(FenceError::InvalidHealthScope),
                }
                if detail
                    .as_ref()
                    .is_some_and(|detail| detail.len() > MAX_HEALTH_DETAIL_BYTES)
                {
                    return Err(FenceError::HealthDetailTooLarge);
                }
                Ok(AcceptedOutput::Health(AcceptedHealth {
                    binding_id: binding_id.as_ref(),
                    state: *state,
                    detail: detail.as_deref(),
                }))
            }
        }
    }

    fn require_owner(&self, owner: &RuntimeOwner) -> Result<(), FenceError> {
        match self.owner.as_ref() {
            None => Err(FenceError::NoOwner),
            Some(current) if current != owner => Err(FenceError::StaleOwner),
            Some(_) => Ok(()),
        }
    }

    fn require_registration(
        &self,
        owner: &RuntimeOwner,
        binding_id: &BindingId,
        registration: &RegistrationToken,
    ) -> Result<&BindingRegistration, FenceError> {
        self.require_owner(owner)?;
        let binding = self
            .bindings
            .get(binding_id)
            .ok_or(FenceError::UnknownBinding)?;
        if &binding.registration != registration {
            return Err(FenceError::StaleRegistration);
        }
        Ok(binding)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceError {
    NoOwner,
    StaleOwner,
    UnknownBinding,
    StaleRegistration,
    ContractMismatch { field: &'static str },
    UnpublishedTopic(String),
    InvalidTopics,
    InvalidHealthScope,
    HealthDetailTooLarge,
}

impl fmt::Display for FenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOwner => formatter.write_str("runtime has no current owner"),
            Self::StaleOwner => formatter.write_str("runtime output has a stale owner claim"),
            Self::UnknownBinding => formatter.write_str("runtime output names an unknown binding"),
            Self::StaleRegistration => {
                formatter.write_str("runtime output has a stale registration token")
            }
            Self::ContractMismatch { field } => {
                write!(formatter, "runtime output has a mismatched {field}")
            }
            Self::UnpublishedTopic(topic) => {
                write!(formatter, "runtime output names unpublished topic {topic}")
            }
            Self::InvalidTopics => formatter.write_str("runtime output has invalid topics"),
            Self::InvalidHealthScope => formatter.write_str("runtime health has an invalid scope"),
            Self::HealthDetailTooLarge => formatter.write_str("runtime health detail is too large"),
        }
    }
}

impl std::error::Error for FenceError {}

#[derive(Debug)]
pub enum AcceptedOutput<'a> {
    Publication(AcceptedPublication<'a>),
    Health(AcceptedHealth<'a>),
}

#[derive(Debug)]
pub struct AcceptedPublication<'a> {
    target: &'a SnapshotTarget,
    bytes: &'a SnapshotBytes,
    selected_topics: Vec<String>,
    facts: &'a [ResourceFact],
    observed_at: Option<&'a str>,
}

impl<'a> AcceptedPublication<'a> {
    pub fn target(&self) -> &SnapshotTarget {
        self.target
    }

    pub fn selected_topics(&self) -> &[String] {
        &self.selected_topics
    }

    pub fn facts(&self) -> &[ResourceFact] {
        self.facts
    }

    pub fn observed_at(&self) -> Option<&str> {
        self.observed_at
    }

    fn prepare(self) -> Result<PreparedPublication<'a>, PublicationError> {
        prepare_snapshot(
            self.target,
            self.bytes.as_slice(),
            self.selected_topics,
            self.facts.to_vec(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedHealth<'a> {
    binding_id: Option<&'a BindingId>,
    state: RuntimeHealthState,
    detail: Option<&'a str>,
}

impl<'a> AcceptedHealth<'a> {
    pub fn binding_id(&self) -> Option<&'a BindingId> {
        self.binding_id
    }

    pub fn state(&self) -> RuntimeHealthState {
        self.state
    }

    pub fn detail(&self) -> Option<&'a str> {
        self.detail
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotChange {
    First,
    Equal,
    Changed { previous: SnapshotDigest },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationOutcome {
    digest: SnapshotDigest,
    change: SnapshotChange,
    selected_topics: Vec<String>,
    facts: Vec<ResourceFact>,
}

impl PublicationOutcome {
    pub fn digest(&self) -> SnapshotDigest {
        self.digest
    }

    pub fn change(&self) -> SnapshotChange {
        self.change
    }

    pub fn selected_topics(&self) -> &[String] {
        &self.selected_topics
    }

    pub fn facts(&self) -> &[ResourceFact] {
        &self.facts
    }

    pub fn invalidating(&self) -> bool {
        self.change != SnapshotChange::Equal && !self.selected_topics.is_empty()
    }
}

#[derive(Debug)]
pub enum PublicationError {
    SnapshotTooLarge { actual: usize },
    UnsafeTarget(PathError),
    ExistingSnapshotTooLarge,
    SnapshotNotRegular,
    Io(io::Error),
}

impl fmt::Display for PublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotTooLarge { actual } => write!(
                formatter,
                "snapshot is {actual} bytes; maximum is {MAX_SNAPSHOT_BYTES}"
            ),
            Self::UnsafeTarget(error) => write!(formatter, "unsafe snapshot target: {error}"),
            Self::ExistingSnapshotTooLarge => formatter.write_str("existing snapshot is too large"),
            Self::SnapshotNotRegular => {
                formatter.write_str("snapshot target is not a real regular file")
            }
            Self::Io(error) => write!(formatter, "snapshot publication failed: {error}"),
        }
    }
}

impl std::error::Error for PublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnsafeTarget(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct PreparedPublication<'a> {
    target: &'a SnapshotTarget,
    bytes: &'a [u8],
    outcome: PublicationOutcome,
}

impl PreparedPublication<'_> {
    fn commit(&self) -> Result<(), PublicationError> {
        if self.outcome.change == SnapshotChange::Equal {
            return Ok(());
        }
        let parent = self
            .target
            .relative
            .parent()
            .unwrap_or_else(|| Path::new(""));
        let leaf = self.target.relative.file_name().ok_or_else(|| {
            PublicationError::UnsafeTarget(PathError::UnsafeCarrier("missing leaf"))
        })?;
        let directory = open_absolute_dir_beneath(&self.target.root, parent)
            .map_err(PublicationError::Io)?;
        atomic_replace_at(&directory, leaf, self.bytes).map_err(PublicationError::Io)
    }
}

fn prepare_snapshot<'a>(
    target: &'a SnapshotTarget,
    bytes: &'a [u8],
    selected_topics: Vec<String>,
    facts: Vec<ResourceFact>,
) -> Result<PreparedPublication<'a>, PublicationError> {
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(PublicationError::SnapshotTooLarge { actual: bytes.len() });
    }
    let parent = target.relative.parent().unwrap_or_else(|| Path::new(""));
    ensure_absolute_dir_beneath(&target.root, parent).map_err(PublicationError::Io)?;
    let previous = target.current_digest()?;
    let digest = SnapshotDigest::of(bytes);
    let change = match previous {
        None => SnapshotChange::First,
        Some(previous) if previous == digest => SnapshotChange::Equal,
        Some(previous) => SnapshotChange::Changed { previous },
    };
    Ok(PreparedPublication {
        target,
        bytes,
        outcome: PublicationOutcome {
            digest,
            change,
            selected_topics,
            facts,
        },
    })
}

fn publish_snapshot(
    target: &SnapshotTarget,
    bytes: &[u8],
    selected_topics: Vec<String>,
    facts: Vec<ResourceFact>,
) -> Result<PublicationOutcome, PublicationError> {
    let prepared = prepare_snapshot(target, bytes, selected_topics, facts)?;
    prepared.commit()?;
    Ok(prepared.outcome)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatchUpState {
    current_snapshot_digest: Option<SnapshotDigest>,
    last_delivered_digest: Option<SnapshotDigest>,
    pending_relevant_change: bool,
    #[serde(default)]
    pending_selected_topics: Vec<String>,
    #[serde(default)]
    pending_facts: Vec<ResourceFact>,
    deliverable: bool,
}

impl CatchUpState {
    pub fn current_snapshot_digest(&self) -> Option<SnapshotDigest> {
        self.current_snapshot_digest
    }

    pub fn last_delivered_digest(&self) -> Option<SnapshotDigest> {
        self.last_delivered_digest
    }

    pub fn pending_relevant_change(&self) -> bool {
        self.pending_relevant_change
    }

    pub fn pending_selected_topics(&self) -> &[String] {
        &self.pending_selected_topics
    }

    pub fn pending_facts(&self) -> &[ResourceFact] {
        &self.pending_facts
    }

    pub fn deliverable(&self) -> bool {
        self.deliverable
    }

    fn validate(&self) -> Result<(), CatchUpError> {
        if self.pending_relevant_change && self.current_snapshot_digest.is_none() {
            return Err(CatchUpError::InvalidState(
                "pending relevance requires a current snapshot digest",
            ));
        }
        if self.pending_relevant_change != !self.pending_selected_topics.is_empty() {
            return Err(CatchUpError::InvalidState(
                "pending relevance and selected topics disagree",
            ));
        }
        validate_persisted_topics(&self.pending_selected_topics)?;
        validate_persisted_facts(&self.pending_facts)?;
        if !self.pending_relevant_change && !self.pending_facts.is_empty() {
            return Err(CatchUpError::InvalidState(
                "pending facts require a pending relevant change",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryRequest {
    digest: SnapshotDigest,
    selected_topics: Vec<String>,
    facts: Vec<ResourceFact>,
}

impl DeliveryRequest {
    pub fn digest(&self) -> SnapshotDigest {
        self.digest
    }

    pub fn selected_topics(&self) -> &[String] {
        &self.selected_topics
    }

    pub fn facts(&self) -> &[ResourceFact] {
        &self.facts
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublicationIntent {
    digest: SnapshotDigest,
    selected_topics: Vec<String>,
    #[serde(default)]
    facts: Vec<ResourceFact>,
}

impl PublicationIntent {
    fn from_outcome(outcome: &PublicationOutcome) -> Self {
        Self {
            digest: outcome.digest,
            selected_topics: outcome.selected_topics.clone(),
            facts: outcome.facts.clone(),
        }
    }

    fn validate(&self) -> Result<(), CatchUpError> {
        validate_persisted_topics(&self.selected_topics)?;
        validate_persisted_facts(&self.facts)
    }
}

fn validate_persisted_topics(topics: &[String]) -> Result<(), CatchUpError> {
    let mut unique = BTreeSet::new();
    for topic in topics {
        if topic.is_empty() || !unique.insert(topic.as_str()) {
            return Err(CatchUpError::InvalidState(
                "persisted selected topics must be non-empty and unique",
            ));
        }
    }
    Ok(())
}
fn validate_persisted_facts(facts: &[ResourceFact]) -> Result<(), CatchUpError> {
    if facts.len() > MAX_FACTS || facts.iter().any(|fact| fact.validate().is_err()) {
        return Err(CatchUpError::InvalidState("persisted facts are invalid"));
    }
    Ok(())
}


#[derive(Debug)]
pub struct CatchUp {
    directory: File,
    state: CatchUpState,
}

impl CatchUp {
    pub fn open(state_directory: &Path) -> Result<Self, CatchUpError> {
        validate_absolute_path(state_directory)
            .map_err(CatchUpError::UnsafeStateDirectory)?;
        let directory = open_absolute_dir(state_directory).map_err(CatchUpError::Io)?;
        let state = match read_regular_optional_at(
            &directory,
            OsStr::new(CATCH_UP_FILE),
            MAX_CATCH_UP_FILE_BYTES,
        ) {
            Ok(Some(bytes)) => {
                serde_json::from_slice::<CatchUpState>(&bytes).map_err(CatchUpError::Json)?
            }
            Ok(None) => CatchUpState::default(),
            Err(BoundedReadError::TooLarge) => return Err(CatchUpError::StateTooLarge),
            Err(BoundedReadError::NotRegular) => return Err(CatchUpError::StateNotRegular),
            Err(BoundedReadError::Io(error)) => return Err(CatchUpError::Io(error)),
        };
        state.validate()?;
        Ok(Self { directory, state })
    }

    pub fn open_for_snapshot(
        state_directory: &Path,
        target: &SnapshotTarget,
    ) -> Result<Self, CatchUpError> {
        let mut catch_up = Self::open(state_directory)?;
        catch_up.reconcile_snapshot(target)?;
        Ok(catch_up)
    }

    pub fn state(&self) -> &CatchUpState {
        &self.state
    }

    pub fn publish(
        &mut self,
        publication: AcceptedPublication<'_>,
    ) -> Result<(PublicationOutcome, Option<DeliveryRequest>), PublicationTransactionError> {
        let target = publication.target;
        self.reconcile_snapshot(target)
            .map_err(PublicationTransactionError::CatchUp)?;
        let prepared = publication
            .prepare()
            .map_err(PublicationTransactionError::Publication)?;
        let outcome = prepared.outcome.clone();
        if outcome.change != SnapshotChange::Equal {
            self.write_publication_intent(&PublicationIntent::from_outcome(&outcome))
                .map_err(PublicationTransactionError::CatchUp)?;
        }
        prepared
            .commit()
            .map_err(PublicationTransactionError::Publication)?;
        let delivery = self
            .record_publication(&outcome)
            .map_err(PublicationTransactionError::CatchUp)?;
        if outcome.change != SnapshotChange::Equal {
            self.clear_publication_intent()
                .map_err(PublicationTransactionError::CatchUp)?;
        }
        Ok((outcome, delivery))
    }

    pub fn reconcile_snapshot(
        &mut self,
        target: &SnapshotTarget,
    ) -> Result<Option<DeliveryRequest>, CatchUpError> {
        let observed = target.current_digest().map_err(CatchUpError::Publication)?;
        let intent = self.read_publication_intent()?;
        let mut next = self.state.clone();

        match intent.as_ref() {
            Some(intent) if observed == Some(intent.digest) => {
                next.current_snapshot_digest = observed;
                if !intent.selected_topics.is_empty() {
                    next.pending_relevant_change = true;
                    next.pending_selected_topics = intent.selected_topics.clone();
                    next.pending_facts = intent.facts.clone();
                }
            }
            Some(_) | None => {
                if observed.is_none() && next.pending_relevant_change {
                    return Err(CatchUpError::InvalidState(
                        "a pending invalidation has no readable canonical snapshot",
                    ));
                }
                next.current_snapshot_digest = observed;
            }
        }

        if next != self.state {
            self.commit(next)?;
        }
        if intent.is_some() {
            self.clear_publication_intent()?;
        }
        Ok(self.pending_delivery())
    }

    pub fn set_deliverable(
        &mut self,
        deliverable: bool,
    ) -> Result<Option<DeliveryRequest>, CatchUpError> {
        if self.state.deliverable != deliverable {
            let mut next = self.state.clone();
            next.deliverable = deliverable;
            self.commit(next)?;
        }
        Ok(self.pending_delivery())
    }

    pub fn pending_delivery(&self) -> Option<DeliveryRequest> {
        if !self.state.deliverable || !self.state.pending_relevant_change {
            return None;
        }
        Some(DeliveryRequest {
            digest: self.state.current_snapshot_digest?,
            selected_topics: self.state.pending_selected_topics.clone(),
            facts: self.state.pending_facts.clone(),
        })
    }

    pub fn acknowledge_delivery(
        &mut self,
        digest: SnapshotDigest,
    ) -> Result<bool, CatchUpError> {
        if !self.state.pending_relevant_change
            || self.state.current_snapshot_digest != Some(digest)
        {
            return Ok(false);
        }
        let mut next = self.state.clone();
        next.last_delivered_digest = Some(digest);
        next.pending_relevant_change = false;
        next.pending_selected_topics.clear();
        next.pending_facts.clear();
        self.commit(next)?;
        Ok(true)
    }

    fn record_publication(
        &mut self,
        outcome: &PublicationOutcome,
    ) -> Result<Option<DeliveryRequest>, CatchUpError> {
        let mut next = self.state.clone();
        next.current_snapshot_digest = Some(outcome.digest);
        if outcome.invalidating() {
            next.pending_relevant_change = true;
            next.pending_selected_topics = outcome.selected_topics.clone();
            next.pending_facts = outcome.facts.clone();
        }
        self.commit(next)?;
        Ok(self.pending_delivery())
    }

    fn read_publication_intent(&self) -> Result<Option<PublicationIntent>, CatchUpError> {
        match read_regular_optional_at(
            &self.directory,
            OsStr::new(PUBLICATION_INTENT_FILE),
            MAX_CATCH_UP_FILE_BYTES,
        ) {
            Ok(Some(bytes)) => {
                let intent = serde_json::from_slice::<PublicationIntent>(&bytes)
                    .map_err(CatchUpError::Json)?;
                intent.validate()?;
                Ok(Some(intent))
            }
            Ok(None) => Ok(None),
            Err(BoundedReadError::TooLarge) => Err(CatchUpError::IntentTooLarge),
            Err(BoundedReadError::NotRegular) => Err(CatchUpError::IntentNotRegular),
            Err(BoundedReadError::Io(error)) => Err(CatchUpError::Io(error)),
        }
    }

    fn write_publication_intent(
        &self,
        intent: &PublicationIntent,
    ) -> Result<(), CatchUpError> {
        intent.validate()?;
        let mut bytes = serde_json::to_vec(intent).map_err(CatchUpError::Json)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_CATCH_UP_FILE_BYTES {
            return Err(CatchUpError::IntentTooLarge);
        }
        atomic_replace_at(
            &self.directory,
            OsStr::new(PUBLICATION_INTENT_FILE),
            &bytes,
        )
        .map_err(CatchUpError::Io)
    }

    fn clear_publication_intent(&self) -> Result<(), CatchUpError> {
        remove_optional_at(&self.directory, OsStr::new(PUBLICATION_INTENT_FILE))
            .map_err(CatchUpError::Io)
    }

    fn commit(&mut self, state: CatchUpState) -> Result<(), CatchUpError> {
        state.validate()?;
        let mut bytes = serde_json::to_vec(&state).map_err(CatchUpError::Json)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_CATCH_UP_FILE_BYTES {
            return Err(CatchUpError::StateTooLarge);
        }
        atomic_replace_at(&self.directory, OsStr::new(CATCH_UP_FILE), &bytes)
            .map_err(CatchUpError::Io)?;
        self.state = state;
        Ok(())
    }
}

#[derive(Debug)]
pub enum PublicationTransactionError {
    Publication(PublicationError),
    CatchUp(CatchUpError),
}

impl fmt::Display for PublicationTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publication(error) => fmt::Display::fmt(error, formatter),
            Self::CatchUp(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for PublicationTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::CatchUp(error) => Some(error),
        }
    }
}

#[derive(Debug)]
pub enum CatchUpError {
    UnsafeStateDirectory(&'static str),
    StateTooLarge,
    StateNotRegular,
    IntentTooLarge,
    IntentNotRegular,
    InvalidState(&'static str),
    Publication(PublicationError),
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for CatchUpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsafeStateDirectory(reason) => {
                write!(formatter, "unsafe catch-up state directory: {reason}")
            }
            Self::StateTooLarge => formatter.write_str("catch-up state file is too large"),
            Self::StateNotRegular => {
                formatter.write_str("catch-up state path is not a real regular file")
            }
            Self::IntentTooLarge => formatter.write_str("publication intent file is too large"),
            Self::IntentNotRegular => {
                formatter.write_str("publication intent path is not a real regular file")
            }
            Self::InvalidState(reason) => write!(formatter, "invalid catch-up state: {reason}"),
            Self::Publication(error) => write!(formatter, "snapshot reconciliation failed: {error}"),
            Self::Json(error) => write!(formatter, "invalid catch-up state JSON: {error}"),
            Self::Io(error) => write!(formatter, "catch-up state I/O failed: {error}"),
        }
    }
}

impl std::error::Error for CatchUpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

enum BoundedReadError {
    TooLarge,
    NotRegular,
    Io(io::Error),
}

fn open_absolute_dir(path: &Path) -> io::Result<File> {
    open_absolute_dir_beneath(path, Path::new(""))
}

fn open_absolute_dir_beneath(root: &Path, relative: &Path) -> io::Result<File> {
    validate_absolute_path(root).map_err(invalid_input)?;
    validate_empty_or_relative_path(relative).map_err(invalid_input)?;
    let slash = c"/";
    let descriptor = unsafe {
        libc::open(
            slash.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut directory = unsafe { File::from_raw_fd(descriptor) };
    for component in root
        .components()
        .chain(relative.components())
        .filter_map(|component| match component {
            Component::RootDir => None,
            Component::Normal(name) => Some(name),
            _ => None,
        })
    {
        directory = openat_directory(&directory, component)?;
    }
    Ok(directory)
}

fn ensure_absolute_dir_beneath(root: &Path, relative: &Path) -> io::Result<File> {
    validate_absolute_path(root).map_err(invalid_input)?;
    validate_empty_or_relative_path(relative).map_err(invalid_input)?;
    let mut directory = open_absolute_dir(root)?;
    for component in relative.components().filter_map(|component| match component {
        Component::Normal(name) => Some(name),
        _ => None,
    }) {
        let name = c_string(component)?;
        let result = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        directory = openat_directory(&directory, component)?;
    }
    Ok(directory)
}

fn validate_empty_or_relative_path(path: &Path) -> Result<(), &'static str> {
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err("path contains a non-normal component");
        }
    }
    Ok(())
}

fn invalid_input(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason)
}

fn openat_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    let name = c_string(name)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

fn read_regular_optional_at(
    directory: &File,
    name: &OsStr,
    maximum: usize,
) -> Result<Option<Vec<u8>>, BoundedReadError> {
    let name = c_string(name).map_err(BoundedReadError::Io)?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(BoundedReadError::Io(error));
    }
    let file = unsafe { File::from_raw_fd(descriptor) };
    let metadata = file.metadata().map_err(BoundedReadError::Io)?;
    if !metadata.is_file() {
        return Err(BoundedReadError::NotRegular);
    }
    if metadata.len() > maximum as u64 {
        return Err(BoundedReadError::TooLarge);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(BoundedReadError::Io)?;
    if bytes.len() > maximum {
        return Err(BoundedReadError::TooLarge);
    }
    Ok(Some(bytes))
}

fn atomic_replace_at(directory: &File, leaf: &OsStr, bytes: &[u8]) -> io::Result<()> {
    ensure_regular_or_absent_at(directory, leaf)?;
    let leaf = c_string(leaf)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = CString::new(format!(
        ".resource-profile.tmp-{}-{sequence}",
        std::process::id()
    ))
    .expect("generated temporary name contains no NUL");
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            temporary.as_ptr(),
            libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        let renamed = unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                temporary.as_ptr(),
                directory.as_raw_fd(),
                leaf.as_ptr(),
            )
        };
        if renamed < 0 {
            return Err(io::Error::last_os_error());
        }
        directory.sync_all()
    })();
    if result.is_err() {
        unsafe {
            libc::unlinkat(directory.as_raw_fd(), temporary.as_ptr(), 0);
        }
    }
    result
}
fn remove_optional_at(directory: &File, leaf: &OsStr) -> io::Result<()> {
    let leaf = c_string(leaf)?;
    let removed = unsafe { libc::unlinkat(directory.as_raw_fd(), leaf.as_ptr(), 0) };
    if removed < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(error);
    }
    directory.sync_all()
}


fn ensure_regular_or_absent_at(directory: &File, leaf: &OsStr) -> io::Result<()> {
    match read_regular_optional_at(directory, leaf, 0) {
        Ok(None) | Ok(Some(_)) | Err(BoundedReadError::TooLarge) => Ok(()),
        Err(BoundedReadError::NotRegular) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "target is not a regular file",
        )),
        Err(BoundedReadError::Io(error)) => Err(error),
    }
}

fn c_string(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    fn id<T>(value: &str, make: impl FnOnce(String) -> Result<T, OpaqueIdError>) -> T {
        make(value.to_owned()).unwrap()
    }

    fn owner(value: &str) -> RuntimeOwner {
        RuntimeOwner::new(
            id(value, RuntimeIncarnation::new),
            id(&format!("claim-{value}"), OwnerClaim::new),
        )
    }

    fn binding_id(value: &str) -> BindingId {
        id(value, BindingId::new)
    }

    fn token(value: &str) -> RegistrationToken {
        id(value, RegistrationToken::new)
    }

    fn target(root: &Path) -> SnapshotTarget {
        SnapshotTarget::new(fs::canonicalize(root).unwrap(), "snapshot.json").unwrap()
    }

    fn contract(selected: &[&str]) -> PublicationContract {
        PublicationContract::new(
            "schema.v1",
            "application/json",
            ["selected", "ignored"].map(str::to_owned),
            TopicSelection::new(selected.iter().map(|topic| (*topic).to_owned())).unwrap(),
        )
        .unwrap()
    }

    fn registration(root: &Path, registration: &str) -> BindingRegistration {
        BindingRegistration::new(
            binding_id("binding"),
            token(registration),
            target(root),
            contract(&["selected"]),
        )
    }

    fn publication(
        owner: RuntimeOwner,
        registration: &str,
        bytes: &[u8],
        topics: &[&str],
    ) -> RuntimeMessage {
        RuntimeMessage::Publish {
            owner,
            binding_id: binding_id("binding"),
            registration: token(registration),
            schema_id: "schema.v1".to_owned(),
            media_type: "application/json".to_owned(),
            bytes: SnapshotBytes::new(bytes.to_vec()).unwrap(),
            topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            facts: None,
            observed_at: None,
        }
    }

    fn accepted_publication<'a>(
        lifecycle: &'a RuntimeLifecycle,
        message: &'a RuntimeMessage,
    ) -> AcceptedPublication<'a> {
        match lifecycle.accept_output(message).unwrap() {
            AcceptedOutput::Publication(publication) => publication,
            AcceptedOutput::Health(_) => panic!("expected publication"),
        }
    }


    #[test]
    fn stale_owner_and_registration_are_fenced() {
        let directory = tempfile::tempdir().unwrap();
        let current = owner("current");
        let stale = owner("stale");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(current.clone());
        lifecycle
            .register(&current, registration(directory.path(), "current-token"))
            .unwrap();

        let stale_owner = publication(stale, "current-token", b"bytes", &["selected"]);
        assert!(matches!(
            lifecycle.accept_output(&stale_owner),
            Err(FenceError::StaleOwner)
        ));
        let stale_token = publication(current, "stale-token", b"bytes", &["selected"]);
        assert!(matches!(
            lifecycle.accept_output(&stale_token),
            Err(FenceError::StaleRegistration)
        ));
        assert!(!directory.path().join("snapshot.json").exists());
    }

    #[test]
    fn unregister_and_reregister_fence_every_old_token() {
        let directory = tempfile::tempdir().unwrap();
        let current = owner("current");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(current.clone());
        lifecycle
            .register(&current, registration(directory.path(), "first"))
            .unwrap();
        lifecycle
            .unregister(&current, &binding_id("binding"), &token("first"))
            .unwrap();

        let first = publication(current.clone(), "first", b"old", &["selected"]);
        assert!(matches!(
            lifecycle.accept_output(&first),
            Err(FenceError::UnknownBinding)
        ));
        lifecycle
            .register(&current, registration(directory.path(), "second"))
            .unwrap();
        assert!(matches!(
            lifecycle.accept_output(&first),
            Err(FenceError::StaleRegistration)
        ));
        let second = publication(current, "second", b"new", &["selected"]);
        assert!(matches!(
            lifecycle.accept_output(&second),
            Ok(AcceptedOutput::Publication(_))
        ));
    }

    #[test]
    fn new_owner_claim_clears_all_registrations() {
        let directory = tempfile::tempdir().unwrap();
        let first_owner = owner("first");
        let second_owner = owner("second");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(first_owner.clone());
        lifecycle
            .register(&first_owner, registration(directory.path(), "token"))
            .unwrap();
        assert!(lifecycle.claim(second_owner.clone()));

        let output = publication(second_owner, "token", b"bytes", &["selected"]);
        assert!(matches!(
            lifecycle.accept_output(&output),
            Err(FenceError::UnknownBinding)
        ));
    }

    #[test]
    fn first_publication_invalidates_but_equal_publication_is_silent() {
        let directory = tempfile::tempdir().unwrap();
        let current = owner("current");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(current.clone());
        lifecycle
            .register(&current, registration(directory.path(), "token"))
            .unwrap();
        let message = publication(current, "token", br#"{"state":1}"#, &["selected"]);
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let mut catch_up = CatchUp::open(&state_directory).unwrap();

        let (first, _) = catch_up
            .publish(accepted_publication(&lifecycle, &message))
            .unwrap();
        assert_eq!(first.change(), SnapshotChange::First);
        assert!(first.invalidating());
        assert_eq!(fs::read(directory.path().join("snapshot.json")).unwrap(), br#"{"state":1}"#);

        let (equal, _) = catch_up
            .publish(accepted_publication(&lifecycle, &message))
            .unwrap();
        assert_eq!(equal.change(), SnapshotChange::Equal);
        assert!(!equal.invalidating());
        assert_eq!(equal.digest(), first.digest());
    }

    #[test]
    fn first_publication_creates_missing_contained_parent_directories() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let target = SnapshotTarget::new(&root, "resources/github-pr/owner/repo/389.json").unwrap();
        assert_eq!(target.current_digest().unwrap(), None);

        let outcome =
            publish_snapshot(&target, b"bytes", vec!["selected".to_owned()], Vec::new()).unwrap();

        assert_eq!(outcome.change(), SnapshotChange::First);
        assert_eq!(
            fs::read(root.join("resources/github-pr/owner/repo/389.json")).unwrap(),
            b"bytes"
        );
    }

    #[test]
    fn topic_filtering_updates_snapshot_without_invalidating() {
        let directory = tempfile::tempdir().unwrap();
        let current = owner("current");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(current.clone());
        lifecycle
            .register(&current, registration(directory.path(), "token"))
            .unwrap();
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let mut catch_up = CatchUp::open(&state_directory).unwrap();
        let ignored = publication(current.clone(), "token", b"ignored", &["ignored"]);
        let (outcome, _) = catch_up
            .publish(accepted_publication(&lifecycle, &ignored))
            .unwrap();
        assert_eq!(outcome.change(), SnapshotChange::First);
        assert!(outcome.selected_topics().is_empty());
        assert!(!outcome.invalidating());

        let selected = publication(
            current,
            "token",
            b"selected",
            &["ignored", "selected"],
        );
        let (outcome, _) = catch_up
            .publish(accepted_publication(&lifecycle, &selected))
            .unwrap();
        assert!(matches!(outcome.change(), SnapshotChange::Changed { .. }));
        assert_eq!(outcome.selected_topics(), ["selected"]);
        assert!(outcome.invalidating());
    }

    #[test]
    fn unavailable_delivery_catches_up_to_the_latest_digest_and_acks_by_level() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let relevant = PublicationOutcome {
            digest: SnapshotDigest::of(b"relevant"),
            change: SnapshotChange::First,
            selected_topics: vec!["selected".to_owned()],
            facts: vec![ResourceFact::current("state", "ready").unwrap()],
        };
        let irrelevant = PublicationOutcome {
            digest: SnapshotDigest::of(b"later but irrelevant"),
            change: SnapshotChange::Changed {
                previous: relevant.digest(),
            },
            selected_topics: Vec::new(),
            facts: Vec::new(),
        };
        let mut catch_up = CatchUp::open(&state_directory).unwrap();
        assert_eq!(catch_up.record_publication(&relevant).unwrap(), None);
        assert_eq!(catch_up.record_publication(&irrelevant).unwrap(), None);
        assert!(catch_up.state().pending_relevant_change());

        let request = catch_up.set_deliverable(true).unwrap().unwrap();
        assert_eq!(request.digest(), irrelevant.digest());
        assert_eq!(request.selected_topics(), ["selected"]);
        assert_eq!(request.facts(), relevant.facts());
        assert!(!catch_up.acknowledge_delivery(relevant.digest()).unwrap());
        assert!(catch_up.state().pending_relevant_change());
        assert!(catch_up.acknowledge_delivery(irrelevant.digest()).unwrap());
        assert!(!catch_up.state().pending_relevant_change());
        assert_eq!(
            catch_up.state().last_delivered_digest(),
            Some(irrelevant.digest())
        );
    }

    #[test]
    fn catch_up_state_survives_reload_and_ignores_crash_temporary() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let outcome = PublicationOutcome {
            digest: SnapshotDigest::of(b"snapshot"),
            change: SnapshotChange::First,
            selected_topics: vec!["selected".to_owned()],
            facts: vec![ResourceFact::current("state", "ready").unwrap()],
        };
        {
            let mut catch_up = CatchUp::open(&state_directory).unwrap();
            catch_up.record_publication(&outcome).unwrap();
        }
        fs::write(
            state_directory.join(".resource-profile.tmp-crash"),
            b"not committed",
        )
        .unwrap();

        let catch_up = CatchUp::open(&state_directory).unwrap();
        assert_eq!(
            catch_up.state().current_snapshot_digest(),
            Some(outcome.digest())
        );
        assert!(catch_up.state().pending_relevant_change());
        assert_eq!(
            catch_up.state().pending_selected_topics(),
            ["selected"]
        );
        assert_eq!(catch_up.state().pending_facts(), outcome.facts());
    }

    #[test]
    fn catch_up_persists_a_maximal_worst_case_escaped_fact_envelope() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let facts = (0..MAX_FACTS)
            .map(|_| {
                ResourceFact::new(
                    "\"".repeat(MAX_FACT_KEY_BYTES),
                    FactValue::value("\"".repeat(MAX_FACT_VALUE_BYTES)),
                    FactValue::value("\\".repeat(MAX_FACT_VALUE_BYTES)),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let outcome = PublicationOutcome {
            digest: SnapshotDigest::of(b"snapshot"),
            change: SnapshotChange::First,
            selected_topics: vec!["selected".to_owned()],
            facts,
        };

        {
            let mut catch_up = CatchUp::open(&state_directory).unwrap();
            catch_up.record_publication(&outcome).unwrap();
        }
        let catch_up = CatchUp::open(&state_directory).unwrap();
        assert_eq!(catch_up.state().pending_facts(), outcome.facts());
    }

    #[test]
    fn durable_intent_recovers_relevance_before_an_equal_republish() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = fs::canonicalize(directory.path()).unwrap();
        let current = owner("current");
        let mut lifecycle = RuntimeLifecycle::new();
        lifecycle.claim(current.clone());
        lifecycle
            .register(&current, registration(directory.path(), "token"))
            .unwrap();
        let message = publication(current, "token", b"committed", &["selected"]);
        let accepted = accepted_publication(&lifecycle, &message);

        {
            let catch_up = CatchUp::open(&state_directory).unwrap();
            let prepared = accepted.prepare().unwrap();
            catch_up
                .write_publication_intent(&PublicationIntent::from_outcome(&prepared.outcome))
                .unwrap();
            prepared.commit().unwrap();
        }

        let snapshot_target = target(directory.path());
        let mut catch_up =
            CatchUp::open_for_snapshot(&state_directory, &snapshot_target).unwrap();
        assert!(catch_up.state().pending_relevant_change());
        assert_eq!(
            catch_up.state().pending_selected_topics(),
            ["selected"]
        );

        let (equal, _) = catch_up
            .publish(accepted_publication(&lifecycle, &message))
            .unwrap();
        assert_eq!(equal.change(), SnapshotChange::Equal);
        assert!(catch_up.state().pending_relevant_change());
        let request = catch_up.set_deliverable(true).unwrap().unwrap();
        assert_eq!(request.digest(), equal.digest());
        assert_eq!(request.selected_topics(), ["selected"]);
    }

    #[test]
    fn snapshot_and_state_paths_reject_escape_and_symlinks() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        assert!(matches!(
            SnapshotTarget::new(&root, "../escape"),
            Err(PathError::UnsafeCarrier(_))
        ));
        assert!(matches!(
            SnapshotTarget::new(&root, root.parent().unwrap().join("escape")),
            Err(PathError::EscapesRoot)
        ));

        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.join("linked-parent")).unwrap();
        let target = SnapshotTarget::new(&root, "linked-parent/snapshot.json").unwrap();
        assert!(matches!(
            publish_snapshot(&target, b"bytes", vec!["selected".to_owned()], Vec::new()),
            Err(PublicationError::Io(_))
        ));

        symlink(outside.path().join("state.json"), root.join(CATCH_UP_FILE)).unwrap();
        assert!(matches!(
            CatchUp::open(&root),
            Err(CatchUpError::Io(_)) | Err(CatchUpError::StateNotRegular)
        ));
    }
}
