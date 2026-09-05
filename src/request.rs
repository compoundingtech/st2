//! Idempotent service-principal request/reply transport over the native message bus.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use kdl::KdlDocument;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::message;

/// Typed-request schema versions.
///
/// In a version-1 envelope every Agent endpoint (`to` on a request, `from` on a reply) means that
/// agent's BUS ADDRESS. In version 2 it means the immutable agent ID, with the publication-time
/// route kept beside it as a display-only snapshot. A service principal's `from`/`replyTo` is its
/// own canonical route at BOTH versions — a principal is not an Agent and has no agent ID.
const REQUEST_VERSION: u32 = 1;
const REQUEST_VERSION_2: u32 = 2;

/// The single switch for the version-2 typed-request writer, a member of the same DELTA-003
/// activation cohort as [`crate::message::WRITE_MESSAGE_RECORD_VERSION_2`].
///
/// Reader-first held first: [`agent_endpoint_id`] and the version checks below accept both
/// versions, and that landed before this flipped. It is ON because `publish` resolves its
/// recipient to an immutable agent ID, and writing an ID into a schema whose `to` means a bus
/// address would leave a reader-first binary unable to validate, read, or reply to the request.
pub const WRITE_REQUEST_VERSION_2: bool = true;

const REQUEST_WRITE_VERSION: u32 = if WRITE_REQUEST_VERSION_2 {
    REQUEST_VERSION_2
} else {
    REQUEST_VERSION
};

/// Whether a typed-request reader understands this schema version.
fn supported_request_version(version: u32) -> bool {
    version == REQUEST_VERSION || version == REQUEST_VERSION_2
}

/// The immutable agent ID one durable Agent endpoint denotes.
///
/// A version-2 endpoint already IS an ID. A version-1 endpoint is a bus address whose bytes
/// migration froze as that subject's ID — unless migration reassigned them, in which case the
/// bytes denote either subject. `owner` names the agent that independently owns the row those
/// bytes were read from (the recipient of a request in its own inbox); pass `None` when nothing
/// proves ownership, and a colliding endpoint then refuses with `Ok(None)` rather than resolving
/// to whichever subject kept the bytes (`MESSAGE-R04`).
///
/// `Err` is the distinct third answer: the durable collision record exists but this binary cannot
/// read it, so no legacy endpoint can be attributed at all. That must not collapse into
/// "no collisions recorded", which would retype contested bytes into the keeper.
fn agent_endpoint_id(
    root: &Path,
    version: u32,
    endpoint: &str,
    owner: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let collisions = crate::catalog_migrate::load_legacy_id_collisions(root)?;
    Ok(message::DurableEndpoint {
        version,
        value: endpoint,
        kind: None,
        owns_row: owner.is_some(),
        owner_agent_id: owner.unwrap_or(endpoint),
    }
    .attribute(&collisions)
    .agent_id()
    .map(str::to_owned))
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePrincipal {
    pub identity: String,
    pub host: String,
    pub path: PathBuf,
}

impl ServicePrincipal {
    /// The principal's canonical bus route.
    ///
    /// A service principal is NOT an Agent: this is its own typed endpoint, it lives in a separate
    /// namespace from agent IDs, and it is never usable as one (`MESSAGE-R11`).
    pub fn bus_address(&self) -> String {
        format!("{}.{}", self.host, self.identity)
    }

    pub fn inbox(&self) -> PathBuf {
        message::inbox_dir(&self.path)
    }

    pub fn archive(&self) -> PathBuf {
        message::archive_dir(&self.path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestEnvelope {
    version: u32,
    idempotency_key: String,
    from: String,
    /// The recipient Agent. Version 2: its immutable agent ID. Version 1: its bus address.
    to: String,
    /// Version 2 only: the recipient's bus address at publication time. Display only — a released
    /// address is immediately reusable, so it is never a selector and never delivery authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_address: Option<String>,
    reply_to: String,
    tags: BTreeMap<String, String>,
    body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReplyEnvelope {
    version: u32,
    idempotency_key: String,
    request_filename: String,
    /// The replying Agent. Version 2: its immutable agent ID. Version 1: its bus address.
    from: String,
    /// Version 2 only: the replying agent's publication-time bus address. Display only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_address: Option<String>,
    tags: BTreeMap<String, String>,
    body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublicationRecord {
    version: u32,
    idempotency_key: String,
    from: String,
    to: String,
    filename: String,
    envelope: String,
    rendered_message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishReceipt {
    pub status: &'static str,
    pub idempotency_key: String,
    pub filename: String,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IncomingRequest {
    pub status: &'static str,
    pub idempotency_key: String,
    pub request_filename: String,
    pub from: String,
    pub tags: BTreeMap<String, String>,
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(
    tag = "status",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum RequestStatus {
    Pending {
        idempotency_key: String,
        request_filename: String,
    },
    Replied {
        idempotency_key: String,
        request_filename: String,
        from: String,
        tags: BTreeMap<String, String>,
        body: Value,
    },
}

pub fn discover_principals(root: &Path) -> anyhow::Result<Vec<ServicePrincipal>> {
    let principals_root = root.join("principals");
    let mut declarations = Vec::new();
    let hosts = match fs::read_dir(&principals_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    for host_entry in hosts.flatten().filter(|entry| entry.path().is_dir()) {
        let identities = fs::read_dir(host_entry.path())?;
        for identity_entry in identities.flatten().filter(|entry| entry.path().is_dir()) {
            let declaration = identity_entry.path().join("principal.kdl");
            if declaration.is_file() {
                declarations.push(declaration);
            }
        }
    }
    declarations.sort();

    let mut principals = Vec::new();
    let mut principal_addresses = HashSet::new();
    for declaration in declarations {
        let text = fs::read_to_string(&declaration)?;
        let document = KdlDocument::parse(&text).map_err(|error| {
            anyhow::anyhow!("{}: KDL parse error: {error}", declaration.display())
        })?;
        let nodes: Vec<_> = document
            .nodes()
            .iter()
            .filter(|node| node.name().value() == "principal")
            .collect();
        if nodes.len() != 1 {
            anyhow::bail!(
                "{}: expected exactly one `principal` declaration",
                declaration.display()
            );
        }
        let node = nodes[0];
        let identity = node
            .get(0)
            .and_then(|value| value.as_string())
            .context("principal identity must be the first string argument")?
            .to_string();
        let host = node
            .get("host")
            .and_then(|value| value.as_string())
            .context("principal requires string property `host`")?
            .to_string();
        let directory = declaration
            .parent()
            .context("principal declaration has no parent")?;
        let path_identity = directory.file_name().and_then(|value| value.to_str());
        let path_host = directory
            .parent()
            .and_then(|value| value.file_name())
            .and_then(|value| value.to_str());
        if path_identity != Some(identity.as_str()) || path_host != Some(host.as_str()) {
            anyhow::bail!(
                "{}: principal content `{host}.{identity}` must match `principals/<host>/<identity>/principal.kdl`",
                declaration.display()
            );
        }
        let principal = ServicePrincipal {
            identity,
            host,
            path: directory.to_path_buf(),
        };
        if !principal_addresses.insert(principal.bus_address()) {
            anyhow::bail!("duplicate service principal `{}`", principal.bus_address());
        }
        principals.push(principal);
    }
    Ok(principals)
}

pub fn resolve_principal(
    root: &Path,
    identity: &str,
    this_host: &str,
) -> anyhow::Result<ServicePrincipal> {
    let principal = discover_principals(root)?
        .into_iter()
        .find(|principal| {
            principal.bus_address() == identity
                || (principal.host == this_host && principal.identity == identity)
        })
        .with_context(|| {
            format!(
                "'{identity}' is not a declared service principal in catalog {}",
                root.display()
            )
        })?;
    // The collision that matters is a human one: a principal route and an Agent's bus ADDRESS are
    // both things a person types to reach someone, so those two namespaces must stay disjoint.
    // Agent IDs are a separate namespace and equal bytes there never collide.
    if crate::discover(root)
        .specs
        .into_iter()
        .any(|spec| spec.bus_address(this_host) == principal.bus_address())
    {
        anyhow::bail!(
            "service principal `{}` collides with an Agent Spec address",
            principal.bus_address()
        );
    }
    Ok(principal)
}

/// One Agent party to a typed request, as the durable record must carry it.
struct AgentParty {
    /// The immutable agent ID: what a version-2 record persists.
    id: String,
    /// The current bus address: what a version-1 record persists, and what a version-2 record
    /// keeps only as a display snapshot.
    address: String,
    path: PathBuf,
}

impl AgentParty {
    /// The value this schema version persists in an Agent endpoint field.
    fn endpoint(&self) -> &str {
        if WRITE_REQUEST_VERSION_2 {
            &self.id
        } else {
            &self.address
        }
    }

    /// The publication-time snapshot, written only alongside a version-2 ID.
    fn address_snapshot(&self) -> Option<String> {
        WRITE_REQUEST_VERSION_2.then(|| self.address.clone())
    }
}

/// Resolve the Agent side of a typed request.
///
/// The selector is typed because both forms genuinely occur: a CLI positional or `--agent` value
/// is an ordinary ADDRESS and must go through the address book, while `--id` and `$ST_AGENT` are
/// EXACT IDs. Forcing a positional through exact-ID lookup makes every subject that carries a
/// generated ID unreachable by the only name a person knows for it.
fn resolve_agent(
    root: &Path,
    selector: &crate::AgentSelector,
    this_host: &str,
) -> anyhow::Result<AgentParty> {
    let discovered = crate::discover(root);
    let id = crate::spec::address_book(&discovered.specs, this_host)?
        .resolve(selector)
        .map_err(|error| anyhow::anyhow!("{error} in catalog {}", root.display()))?
        .id
        .as_str()
        .to_owned();
    // Back-mapping the resolved subject to its declaration is itself a uniqueness proof. Only
    // `resolve_id` refuses `AmbiguousId`; `resolve_address` dedups its candidates BY agent ID, so
    // an address naming one of two subjects that share an ID resolves cleanly to a single Subject
    // and a first-match scan would then publish into whichever declaration discovery ordered
    // first. Both selector kinds therefore prove it here.
    let mut declarations = discovered
        .specs
        .iter()
        .filter(|spec| spec.agent_id(this_host) == id);
    let spec = declarations
        .next()
        .context("resolved subject has no declaration")?;
    if let Some(duplicate) = declarations.next() {
        anyhow::bail!(
            "agent id '{id}' is declared by more than one subject ({}, {}); refusing to guess \
             which declaration this request belongs to",
            spec.path.display(),
            duplicate.path.display()
        );
    }
    let path = spec
        .path
        .parent()
        .context("agent declaration has no parent")?
        .to_path_buf();
    Ok(AgentParty {
        id,
        address: spec.bus_address(this_host),
        path,
    })
}

/// Publish a typed request from a service principal to one Agent.
///
/// `recipient` is typed: the CLI positional is an ordinary address, `--id` is an exact ID.
/// `principal_identity` is the principal's own route — a service principal is not an Agent and
/// never enters the agent-ID namespace.
pub fn publish(
    root: &Path,
    this_host: &str,
    principal_identity: &str,
    recipient: &crate::AgentSelector,
    idempotency_key: &str,
    tags: BTreeMap<String, String>,
    body: Value,
) -> anyhow::Result<PublishReceipt> {
    require_key(idempotency_key)?;
    let principal = resolve_principal(root, principal_identity, this_host)?;
    let recipient = resolve_agent(root, recipient, this_host)?;
    let from = principal.bus_address();
    let to = recipient.endpoint().to_owned();
    let envelope = RequestEnvelope {
        version: REQUEST_WRITE_VERSION,
        idempotency_key: idempotency_key.to_string(),
        from: from.clone(),
        to: to.clone(),
        to_address: recipient.address_snapshot(),
        reply_to: from.clone(),
        tags,
        body,
    };
    let envelope = serde_json::to_string(&envelope)?;
    let record_dir = principal.path.join("resources/request-state/outgoing");
    publish_once(
        &record_dir,
        idempotency_key,
        idempotency_key,
        &from,
        // A principal has no agent ID, so the rendered `from:` route IS its canonical endpoint.
        None,
        &to,
        &envelope,
        &message::inbox_dir(&recipient.path),
        Some(&format!("request {idempotency_key}")),
        None,
        &["st2-request".to_string()],
        "request",
    )
}

/// Reply to a typed request as the Agent it was addressed to.
pub fn reply(
    root: &Path,
    this_host: &str,
    agent: &crate::AgentSelector,
    request_filename: &str,
    tags: BTreeMap<String, String>,
    body: Value,
) -> anyhow::Result<PublishReceipt> {
    let agent = resolve_agent(root, agent, this_host)?;
    let request_message = read_inbox_or_archive(&agent.path, request_filename)?;
    let request = parse_request_message(&request_message)?;
    // The record was read from THIS agent's own boxes, which is what proves it owns the row and
    // therefore what licenses attributing a colliding version-1 recipient endpoint to it.
    anyhow::ensure!(
        supported_request_version(request.version)
            && agent_endpoint_id(root, request.version, &request.to, Some(&agent.id))?.as_deref()
                == Some(agent.id.as_str()),
        "request is not addressed to agent `{}`",
        agent.id
    );
    let principal = resolve_principal(root, &request.reply_to, this_host)?;
    let from = agent.endpoint().to_owned();
    let envelope = ReplyEnvelope {
        version: REQUEST_WRITE_VERSION,
        idempotency_key: request.idempotency_key.clone(),
        request_filename: request_filename.to_string(),
        from: from.clone(),
        from_address: agent.address_snapshot(),
        tags,
        body,
    };
    let envelope = serde_json::to_string(&envelope)?;
    let state_key = format!("{}\0{}", request.reply_to, request.idempotency_key);
    let record_dir = agent.path.join("resources/request-state/replies");
    publish_once(
        &record_dir,
        &state_key,
        &request.idempotency_key,
        // The rendered `from:` line is the agent's ROUTE; `from-id:` carries the authority.
        &agent.address,
        WRITE_REQUEST_VERSION_2.then_some(agent.id.as_str()),
        &request.reply_to,
        &envelope,
        &principal.inbox(),
        Some(&format!("re: request {}", request.idempotency_key)),
        Some(request_filename),
        &["st2-request-reply".to_string()],
        "reply",
    )
}

pub fn read(
    root: &Path,
    this_host: &str,
    agent_selector: &crate::AgentSelector,
    request_filename: &str,
) -> anyhow::Result<IncomingRequest> {
    let agent = resolve_agent(root, agent_selector, this_host)?;
    let message = read_inbox_or_archive(&agent.path, request_filename)?;
    let request = parse_request_message(&message)?;
    anyhow::ensure!(
        supported_request_version(request.version)
            && agent_endpoint_id(root, request.version, &request.to, Some(&agent.id))?.as_deref()
                == Some(agent.id.as_str()),
        "request is not addressed to agent `{}`",
        agent.id
    );
    resolve_principal(root, &request.reply_to, this_host)?;
    Ok(IncomingRequest {
        status: "request",
        idempotency_key: request.idempotency_key,
        request_filename: request_filename.to_string(),
        from: request.from,
        tags: request.tags,
        body: request.body,
    })
}

pub fn status(
    root: &Path,
    this_host: &str,
    principal_identity: &str,
    idempotency_key: &str,
) -> anyhow::Result<RequestStatus> {
    let principal = resolve_principal(root, principal_identity, this_host)?;
    let record_path = record_path(
        &principal.path.join("resources/request-state/outgoing"),
        idempotency_key,
    );
    let record: PublicationRecord =
        serde_json::from_slice(&fs::read(&record_path).with_context(|| {
            format!("no published request for idempotency key `{idempotency_key}`")
        })?)?;

    // The principal owns this outgoing record, so the Agent endpoint in it is NOT the row's own
    // state owner. A colliding version-1 endpoint is therefore unattributed and matches nothing:
    // pairing it with a reply would credit whichever subject kept the legacy bytes.
    let Some(expected_agent) = agent_endpoint_id(root, record.version, &record.to, None)? else {
        anyhow::bail!(
            "request recipient `{}` is a legacy bus identity migration reassigned; \
             this record cannot prove which subject it was addressed to",
            record.to
        );
    };

    let mut replies = Vec::new();
    for directory in [principal.inbox(), principal.archive()] {
        for candidate in message::list_dir(&directory)? {
            if candidate.in_reply_to.as_deref() != Some(record.filename.as_str()) {
                continue;
            }
            let Ok(reply) = serde_json::from_str::<ReplyEnvelope>(candidate.body.trim()) else {
                continue;
            };
            // A version-2 reply's `from` is an agent ID and the frontmatter route is only a
            // display snapshot, so the frontmatter cross-check keys on `from-id` at that version.
            let frontmatter_matches = if reply.version >= REQUEST_VERSION_2 {
                candidate.from_id.as_deref() == Some(reply.from.as_str())
            } else {
                candidate.from.as_deref() == Some(reply.from.as_str())
            };
            if supported_request_version(reply.version)
                && reply.idempotency_key == idempotency_key
                && reply.request_filename == record.filename
                && agent_endpoint_id(root, reply.version, &reply.from, None)?.as_deref()
                    == Some(expected_agent.as_str())
                && frontmatter_matches
                && candidate.tags.iter().any(|tag| tag == "st2-request-reply")
            {
                replies.push(reply);
            }
        }
    }
    replies.dedup_by(|left, right| left == right);
    match replies.as_slice() {
        [] => Ok(RequestStatus::Pending {
            idempotency_key: idempotency_key.to_string(),
            request_filename: record.filename,
        }),
        [reply] => Ok(RequestStatus::Replied {
            idempotency_key: idempotency_key.to_string(),
            request_filename: record.filename,
            from: reply.from.clone(),
            tags: reply.tags.clone(),
            body: reply.body.clone(),
        }),
        _ => anyhow::bail!("conflicting replies for idempotency key `{idempotency_key}`"),
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_once(
    state_dir: &Path,
    state_key: &str,
    receipt_key: &str,
    from: &str,
    // The sender's immutable agent ID, when the sender is an Agent and the writer is on version 2.
    from_id: Option<&str>,
    to: &str,
    envelope: &str,
    inbox: &Path,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    message_tags: &[String],
    kind: &str,
) -> anyhow::Result<PublishReceipt> {
    fs::create_dir_all(state_dir)?;
    let path = record_path(state_dir, state_key);
    let mut candidate = PublicationRecord {
        version: REQUEST_WRITE_VERSION,
        idempotency_key: state_key.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        filename: message::new_filename(),
        envelope: envelope.to_string(),
        rendered_message: String::new(),
    };
    candidate.rendered_message = message::render_agent_message(
        from,
        from_id,
        subject,
        in_reply_to,
        message_tags,
        &candidate.envelope,
    );
    let serialized = serde_json::to_vec(&candidate)?;
    let created = atomic_create(&path, &serialized)?;
    let record: PublicationRecord = if created {
        candidate
    } else {
        serde_json::from_slice(&fs::read(&path)?)?
    };
    if !supported_request_version(record.version)
        || record.idempotency_key != state_key
        || record.from != from
        || record.to != to
        || record.envelope != envelope
    {
        anyhow::bail!("idempotency key reused with different {kind}");
    }
    let message_created =
        message::materialize_message_once(inbox, &record.filename, &record.rendered_message)?;
    Ok(PublishReceipt {
        status: "published",
        idempotency_key: receipt_key.to_string(),
        filename: record.filename,
        deduplicated: !created || !message_created,
    })
}

fn atomic_create(path: &Path, bytes: &[u8]) -> anyhow::Result<bool> {
    let parent = path.parent().context("state record has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".request-state.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&temporary, bytes)?;
    let result = match fs::hard_link(&temporary, path) {
        Ok(()) => Ok(true),
        Err(_) if path.is_file() => Ok(false),
        Err(error) => Err(error.into()),
    };
    let _ = fs::remove_file(temporary);
    result
}

fn record_path(directory: &Path, key: &str) -> PathBuf {
    let hash = Sha256::digest(key.as_bytes());
    directory.join(format!("{hash:x}.json"))
}

fn require_key(key: &str) -> anyhow::Result<()> {
    if key.is_empty() {
        anyhow::bail!("idempotency key must not be empty");
    }
    Ok(())
}

fn parse_request_message(message: &message::Message) -> anyhow::Result<RequestEnvelope> {
    let request: RequestEnvelope =
        serde_json::from_str(message.body.trim()).context("message is not a typed st2 request")?;
    if request.from != request.reply_to {
        anyhow::bail!("request sender and reply target differ");
    }
    if message.from.as_deref() != Some(request.from.as_str()) {
        anyhow::bail!("request frontmatter sender does not match its JSON envelope");
    }
    if !message.tags.iter().any(|tag| tag == "st2-request") {
        anyhow::bail!("request is missing the native `st2-request` transport tag");
    }
    if message.in_reply_to.is_some() {
        anyhow::bail!("request must not be a native reply");
    }
    Ok(request)
}

fn read_inbox_or_archive(agent_dir: &Path, filename: &str) -> anyhow::Result<message::Message> {
    message::read_msg(&message::inbox_dir(agent_dir), filename)
        .or_else(|_| message::read_msg(&message::archive_dir(agent_dir), filename))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentSelector;

    fn catalog() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let principal = root.join("principals/h/example-ci");
        fs::create_dir_all(&principal).unwrap();
        fs::write(
            principal.join("principal.kdl"),
            "principal \"example-ci\" host=\"h\"\n",
        )
        .unwrap();
        // A MIGRATED agent: its immutable ID is a UUID and its route is a separate mutable address.
        let agent = root.join("h/worker");
        fs::create_dir_all(&agent).unwrap();
        fs::write(
            agent.join("agent.kdl"),
            "agent \"worker\" {\n  identity \"worker\"\n  id \"worker-uuid\"\n  address \"chat\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" { command \"x\" }\n}\n",
        )
        .unwrap();
        temp
    }

    /// A CLI positional is an ordinary ADDRESS. Forcing it through exact-ID lookup made every
    /// subject carrying a generated ID unreachable by the only name a person knows for it.
    #[test]
    fn a_positional_address_resolves_a_uuid_id_agent_and_persists_the_id() {
        let temp = catalog();
        let root = temp.path();

        let by_address = resolve_agent(root, &AgentSelector::address("chat"), "h").unwrap();
        assert_eq!(by_address.id, "worker-uuid");
        assert_eq!(by_address.address, "h.chat");

        let by_qualified = resolve_agent(root, &AgentSelector::address("h.chat"), "h").unwrap();
        assert_eq!(by_qualified.id, "worker-uuid");

        let by_id = resolve_agent(root, &AgentSelector::id("worker-uuid"), "h").unwrap();
        assert_eq!(by_id.id, "worker-uuid");

        // The two namespaces stay disjoint in both directions.
        assert!(resolve_agent(root, &AgentSelector::id("chat"), "h").is_err());
        assert!(resolve_agent(root, &AgentSelector::address("worker-uuid"), "h").is_err());
    }

    /// Decision 1 for the request plane: a record that carries an immutable ID must DECLARE
    /// version 2. Under version 1 `to` means the agent's bus address, so an ID stored there is
    /// unreadable to a reader-first binary.
    #[test]
    fn publish_writes_a_version_2_envelope_carrying_the_id_and_an_address_snapshot() {
        let temp = catalog();
        let root = temp.path();
        assert!(WRITE_REQUEST_VERSION_2, "the request writer cohort is on");

        let receipt = publish(
            root,
            "h",
            "h.example-ci",
            &AgentSelector::address("chat"),
            "key-1",
            BTreeMap::new(),
            serde_json::json!({ "run": 1 }),
        )
        .unwrap();

        let delivered = message::read_msg(&message::inbox_dir(&root.join("h/worker")), &receipt.filename)
            .unwrap();
        let envelope: RequestEnvelope = serde_json::from_str(delivered.body.trim()).unwrap();
        assert_eq!(envelope.version, REQUEST_VERSION_2);
        assert_eq!(envelope.to, "worker-uuid");
        assert_eq!(envelope.to_address.as_deref(), Some("h.chat"));
        // A principal is not an Agent: both ends of its own route stay the route.
        assert_eq!(envelope.from, "h.example-ci");
        assert_eq!(envelope.reply_to, "h.example-ci");
        assert_eq!(delivered.from.as_deref(), Some("h.example-ci"));
        assert_eq!(delivered.from_id, None);

        // The agent it names can read and reply to it.
        let incoming = read(root, "h", &AgentSelector::id("worker-uuid"), &receipt.filename).unwrap();
        assert_eq!(incoming.from, "h.example-ci");
        let reply_receipt = reply(
            root,
            "h",
            &AgentSelector::address("chat"),
            &receipt.filename,
            BTreeMap::new(),
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let replied = message::read_msg(
            &message::inbox_dir(&root.join("principals/h/example-ci")),
            &reply_receipt.filename,
        )
        .unwrap();
        let reply_envelope: ReplyEnvelope = serde_json::from_str(replied.body.trim()).unwrap();
        assert_eq!(reply_envelope.version, REQUEST_VERSION_2);
        assert_eq!(reply_envelope.from, "worker-uuid");
        assert_eq!(reply_envelope.from_address.as_deref(), Some("h.chat"));
        // The rendered route is display; `from-id` is the authority the principal matches on.
        assert_eq!(replied.from.as_deref(), Some("h.chat"));
        assert_eq!(replied.from_id.as_deref(), Some("worker-uuid"));

        assert!(matches!(
            status(root, "h", "h.example-ci", "key-1").unwrap(),
            RequestStatus::Replied { from, .. } if from == "worker-uuid"
        ));
    }

    /// A version-1 endpoint whose legacy bytes migration reassigned denotes either subject. Where
    /// nothing proves ownership it must refuse, not resolve to whichever subject kept the bytes.
    #[test]
    fn a_reassigned_version_1_endpoint_is_unattributed_without_proof_of_ownership() {
        let temp = catalog();
        let root = temp.path();
        let control = crate::catalog_migrate::legacy_id_collisions_path(root);
        fs::create_dir_all(control.parent().unwrap()).unwrap();
        fs::write(
            &control,
            serde_json::to_vec(&crate::catalog_migrate::LegacyIdCollisions {
                schema: "st2.catalog-legacy-id-collisions.v1".to_owned(),
                entries: vec![crate::catalog_migrate::LegacyIdCollision {
                    legacy_bus_identity: "h.worker".to_owned(),
                    keeper: crate::AgentId::parse("h.worker").unwrap(),
                    reassigned: vec![crate::AgentId::parse("worker-uuid").unwrap()],
                }],
            })
            .unwrap(),
        )
        .unwrap();

        // No proof of ownership: refuse rather than credit the keeper.
        assert_eq!(
            agent_endpoint_id(root, REQUEST_VERSION, "h.worker", None).unwrap(),
            None
        );
        // The recipient of a request found in its OWN boxes does own the row.
        assert_eq!(
            agent_endpoint_id(root, REQUEST_VERSION, "h.worker", Some("worker-uuid"))
                .unwrap()
                .as_deref(),
            Some("worker-uuid")
        );
        // Uncontested legacy bytes are that subject's frozen ID at version 1.
        assert_eq!(
            agent_endpoint_id(root, REQUEST_VERSION, "h.other", None)
                .unwrap()
                .as_deref(),
            Some("h.other")
        );
        // A version-2 endpoint is already an ID and is never reinterpreted.
        assert_eq!(
            agent_endpoint_id(root, REQUEST_VERSION_2, "h.worker", None)
                .unwrap()
                .as_deref(),
            Some("h.worker")
        );
    }

    /// A collision record that exists but cannot be read proves nothing about which legacy bytes
    /// were contested, so attribution refuses instead of reading as "no collisions" and retyping
    /// the bytes into their apparent owner.
    #[test]
    fn an_unreadable_collision_record_refuses_legacy_attribution() {
        for body in [
            b"{ not json".to_vec(),
            serde_json::to_vec(&serde_json::json!({
                "schema": "st2.catalog-legacy-id-collisions.v2",
                "entries": []
            }))
            .unwrap(),
        ] {
            let temp = catalog();
            let root = temp.path();
            let record = crate::catalog_migrate::legacy_id_collisions_path(root);
            fs::create_dir_all(record.parent().unwrap()).unwrap();
            fs::write(&record, &body).unwrap();

            let refusal = agent_endpoint_id(root, REQUEST_VERSION, "h.worker", Some("h.worker"))
                .expect_err("an unreadable collision record must refuse");
            let rendered = format!("{refusal:#}");
            assert!(
                rendered.contains("legacy-id-collision"),
                "refusal must name the collision record: {rendered}"
            );

            // Even a version-2 endpoint, which never consults the record's contents, refuses:
            // a catalog whose collision set is unreadable cannot answer attribution at all.
            assert!(
                agent_endpoint_id(root, REQUEST_VERSION_2, "h.worker", None).is_err(),
                "an unreadable collision record must refuse at every version"
            );
        }
    }
}
