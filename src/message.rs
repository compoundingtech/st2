//! Native message bus.
//!
//! st2 owns messaging directly now: a message is a markdown file with YAML frontmatter, named
//! `<unix-ms>-<rand6>.md`, written into the recipient agent's `resources/inbox/` (VRS §5). The
//! **recipient is implied by the path**; `from` lives in the frontmatter; the filename's ms prefix is
//! the send time. Send = write the file. Archive = move inbox→archive. Nobody mutates a file after
//! creation. An archive copy is also a durable receipt: if an eventually-consistent file sync briefly
//! restores the same filename in the inbox, inbox readers suppress the duplicate and a repeated
//! archive removes it without overwriting the receipt. The on-disk grammar is stable.
//!
//! This module is location-agnostic: it operates on an inbox/agent directory a caller resolves (from
//! the catalog for VRS-native, or `$ST_ROOT` for a compat shim).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write as _};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use st2_wire::message::{EndpointKind, SentCoverage, SentMessageRow, SentMessages};

use crate::{AddressBook, AgentId, AgentSelector, ResolveError, Subject};

/// Version of the sender ledger's head, active pointer, commit nodes, and idempotency receipts.
/// Those shapes are unchanged by DELTA-003; only the message *record* gained a second version.
const SENT_VERSION: u32 = 1;
const SENT_DIR: &str = "sent";
const SENT_HEAD: &str = "index.json";
const SENT_ACTIVE: &str = "active.json";
const SENT_COMMITS: &str = "commits";
const SENT_MESSAGES: &str = "messages";
const SENT_PENDING: &str = "pending";
const SENT_LOCK: &str = ".lock";
const SENT_KEYS: &str = "keys";

/// Durable message-record schema versions (03-message spec, "State ownership").
///
/// Version 1 keeps legacy bus identities in `from`/`to`. Version 2 carries an immutable agent ID
/// plus a publication-time bus-address snapshot for an Agent endpoint, and an explicit endpoint
/// kind with a canonical typed address for a principal or external endpoint.
pub const MESSAGE_RECORD_VERSION_1: u32 = 1;
pub const MESSAGE_RECORD_VERSION_2: u32 = 2;

/// The single switch for the version-2 durable message writer, one member of the DELTA-003
/// activation cohort.
///
/// Reader-first was satisfied first: every reader in this module accepts version 1 and version 2,
/// and that landed before this flipped. It is ON because a record whose `from`/`to` carry immutable
/// agent IDs must DECLARE version 2 — under version 1 those fields mean bus identities, and storing
/// an ID there would silently misattribute the endpoint to whatever subject now answers that route.
/// Nothing writes an ID under version 1, and nothing writes a route under version 2.
///
/// Named and reversible on purpose: activation step 5 flips this together with harness-state v2,
/// harness-context v2, and PTY schema 2, and it must be revertible with them.
pub const WRITE_MESSAGE_RECORD_VERSION_2: bool = true;

const SENT_RECORD_WRITE_VERSION: u32 = if WRITE_MESSAGE_RECORD_VERSION_2 {
    MESSAGE_RECORD_VERSION_2
} else {
    MESSAGE_RECORD_VERSION_1
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SentHead {
    version: u32,
    since: u64,
    count: u64,
    tip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SentActive {
    version: u32,
    filename: String,
    record_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SentCommit {
    version: u32,
    ordinal: u64,
    previous: Option<String>,
    filename: String,
    row_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SentKey {
    version: u32,
    to: String,
    key: String,
    filename: String,
    record_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SentRecord {
    version: u32,
    filename: String,
    ts: u64,
    /// The canonical endpoint. An Agent endpoint is an immutable agent ID; a typed non-Agent
    /// endpoint is that endpoint's own canonical address. Never a mutable agent address.
    from: String,
    to: String,
    subject: Option<String>,
    in_reply_to: Option<String>,
    tags: Vec<String>,
    priority: Option<String>,
    idempotency_key: Option<String>,
    body: String,
    rendered_message: String,
    /// Version 2 only: the endpoint's bus address at publication time. Display-only, because a
    /// released address is immediately reusable and may already route to a different subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_address: Option<String>,
    /// Version 2 only: what the endpoint value is. Absent is the version-1 meaning, `agent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_kind: Option<EndpointKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_kind: Option<EndpointKind>,
}

impl SentRecord {
    fn row(&self, include_body: bool) -> SentMessageRow {
        SentMessageRow {
            filename: self.filename.clone(),
            ts: self.ts,
            to: self.to.clone(),
            subject: self.subject.clone(),
            in_reply_to: self.in_reply_to.clone(),
            tags: self.tags.clone(),
            priority: self.priority.clone(),
            idempotency_key: self.idempotency_key.clone(),
            body: include_body.then(|| self.body.clone()),
            to_address: self.to_address.clone(),
            to_kind: self.to_kind,
        }
    }

    /// This row's recipient endpoint, as a reader must interpret it. `owner_agent_id` is the
    /// current immutable ID of the subject that owns this sender ledger.
    fn recipient_endpoint<'a>(&'a self, owner_agent_id: &'a str) -> DurableEndpoint<'a> {
        DurableEndpoint {
            version: self.version,
            value: &self.to,
            kind: self.to_kind,
            // A sender-owned row's state owner is its sender, never its recipient.
            owns_row: false,
            owner_agent_id,
        }
    }

    /// This row's sender endpoint. A sender ledger row is owned by its sender, so a colliding
    /// version-1 sender endpoint resolves to that owner's migrated ID.
    ///
    /// The publication path already knows its own sender, so only the attribution proofs read
    /// this; it exists to state the ownership side of `MESSAGE-R04` in one place.
    #[cfg(test)]
    fn sender_endpoint<'a>(&'a self, owner_agent_id: &'a str) -> DurableEndpoint<'a> {
        DurableEndpoint {
            version: self.version,
            value: &self.from,
            kind: self.from_kind,
            owns_row: true,
            owner_agent_id,
        }
    }

    fn same_operation(&self, candidate: &Self) -> bool {
        self.from == candidate.from
            && self.to == candidate.to
            && self.from_kind == candidate.from_kind
            && self.to_kind == candidate.to_kind
            && self.from_address == candidate.from_address
            && self.to_address == candidate.to_address
            && self.subject == candidate.subject
            && self.in_reply_to == candidate.in_reply_to
            && self.tags == candidate.tags
            && self.priority == candidate.priority
            && self.idempotency_key == candidate.idempotency_key
            && self.body == candidate.body
            && self.rendered_message == candidate.rendered_message
    }
}

/// Accept both durable record versions, and refuse a record that mixes them.
///
/// A version-1 record carrying version-2 fields is not a tolerable older shape, it is a record
/// whose endpoint meaning is undecidable, so it fails closed like any other corruption.
fn validate_sent_record(record: &SentRecord) -> anyhow::Result<()> {
    match record.version {
        MESSAGE_RECORD_VERSION_1 => anyhow::ensure!(
            record.from_kind.is_none()
                && record.to_kind.is_none()
                && record.from_address.is_none()
                && record.to_address.is_none(),
            "version-1 sent record carries version-2 endpoint fields"
        ),
        MESSAGE_RECORD_VERSION_2 => anyhow::ensure!(
            record.from_kind.is_some() && record.to_kind.is_some(),
            "version-2 sent record is missing an explicit endpoint kind"
        ),
        version => anyhow::bail!("unsupported sent record version {version}"),
    }
    Ok(())
}

/// The durable collision metadata the legacy-ID migration transaction writes.
///
/// Migration froze each unmigrated subject's `<host>.<identity>` bytes as its agent ID, so a
/// version-1 endpoint is that subject's agent ID by construction — not a fallback, an identity.
/// The one exception is a reassigned legacy bus identity: one subject kept the bytes while an
/// archived subject received a generated ID, so those bytes in a version-1 row denote either
/// subject and cannot be retyped universally (03-message spec, `MESSAGE-R04`).
///
/// `src/catalog_migrate.rs` owns the on-disk format and implements this for its record; nothing
/// here parses that format.
pub trait LegacyCollisionIndex {
    /// True when migration reassigned these legacy `<host>.<identity>` bytes.
    fn is_reassigned(&self, legacy_bus_identity: &str) -> bool;
}

/// A catalog that has not run the legacy-ID migration reassigned nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoLegacyCollisions;

impl LegacyCollisionIndex for NoLegacyCollisions {
    fn is_reassigned(&self, _legacy_bus_identity: &str) -> bool {
        false
    }
}

/// The migration transaction's durable record, read as a collision index.
///
/// Only contested endpoints are recorded, because a non-contested one needs no record: its bytes
/// already are that subject's frozen legacy ID. `keeper` and the reassigned IDs are deliberately
/// unused here — the owner's ID comes from the state being read, never from this record, which is
/// what makes it impossible to mis-attribute an archived sender to the live keeper.
impl LegacyCollisionIndex for crate::catalog_migrate::LegacyIdCollisions {
    fn is_reassigned(&self, legacy_bus_identity: &str) -> bool {
        matches!(
            self.attribution(legacy_bus_identity),
            crate::catalog_migrate::LegacyEndpoint::Collision { .. }
        )
    }
}

impl<T: LegacyCollisionIndex + ?Sized> LegacyCollisionIndex for &T {
    fn is_reassigned(&self, legacy_bus_identity: &str) -> bool {
        (**self).is_reassigned(legacy_bus_identity)
    }
}

/// What one durable endpoint denotes, and therefore what authority it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointAttribution {
    /// A canonical immutable agent ID. Carries reply and automation authority.
    AgentId(String),
    /// A typed non-Agent endpoint's canonical address. Replyable on that transport; never an
    /// agent-automation key.
    TypedAddress { kind: EndpointKind, address: String },
    /// Colliding version-1 legacy bytes that this row's state owner does not own. Rendered as a
    /// historical address, never as the keeping subject's ID, and carrying no reply or automation
    /// authority: addressing the live replacement would deliver to the wrong subject.
    UnattributedLegacyAddress(String),
}

impl EndpointAttribution {
    /// The canonical target a reply or automation may address, or `None` when this endpoint
    /// carries no such authority.
    pub fn reply_target(&self) -> Option<&str> {
        match self {
            Self::AgentId(id) => Some(id),
            Self::TypedAddress { address, .. } => Some(address),
            Self::UnattributedLegacyAddress(_) => None,
        }
    }

    /// The immutable agent ID, when this endpoint is an attributed Agent.
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::AgentId(id) => Some(id),
            Self::TypedAddress { .. } | Self::UnattributedLegacyAddress(_) => None,
        }
    }

    /// Human rendering. An unattributed legacy endpoint is explicitly marked as historical so it
    /// is never read as a live route.
    pub fn display(&self) -> String {
        match self {
            Self::AgentId(id) => id.clone(),
            Self::TypedAddress { address, .. } => address.clone(),
            Self::UnattributedLegacyAddress(address) => format!("{address} (historical address)"),
        }
    }
}

/// One endpoint read back from a durable message record, with everything a reader needs to decide
/// its attribution.
#[derive(Debug, Clone, Copy)]
pub struct DurableEndpoint<'a> {
    /// The record's schema version.
    pub version: u32,
    /// The persisted endpoint value.
    pub value: &'a str,
    /// The persisted endpoint kind. Absent means `agent`, the only version-1 meaning.
    pub kind: Option<EndpointKind>,
    /// True when this endpoint is the row's own state owner: the sender for a sender-owned row,
    /// the recipient for an inbox row.
    pub owns_row: bool,
    /// That state owner's current immutable agent ID.
    pub owner_agent_id: &'a str,
}

impl DurableEndpoint<'_> {
    /// Attribute this endpoint against the migration's durable collision metadata.
    pub fn attribute(&self, collisions: &impl LegacyCollisionIndex) -> EndpointAttribution {
        let kind = EndpointKind::or_version_1_default(self.kind);
        if kind != EndpointKind::Agent {
            return EndpointAttribution::TypedAddress {
                kind,
                address: self.value.to_owned(),
            };
        }
        // A version-2 Agent endpoint already IS an immutable ID; there is nothing to reinterpret.
        if self.version >= MESSAGE_RECORD_VERSION_2 || !collisions.is_reassigned(self.value) {
            return EndpointAttribution::AgentId(self.value.to_owned());
        }
        if self.owns_row {
            return EndpointAttribution::AgentId(self.owner_agent_id.to_owned());
        }
        EndpointAttribution::UnattributedLegacyAddress(self.value.to_owned())
    }
}

/// The alphabet st2 *generates* `<rand6>` from — Crockford base32 (`0-9a-z` minus `i l o u`). This is
/// a strict subset of what the reader accepts: the frozen bus grammar is `[0-9a-z]{6}`, so a peer
/// may legally use i/l/o/u and [`is_message_filename`] must not reject those.
const CROCKFORD: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Parsed frontmatter + body of a message. Readers are permissive: missing/malformed frontmatter
/// still yields a readable body with `from = None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// `<unix-ms>-<rand6>.md`.
    pub filename: String,
    /// Send time in unix ms (from the filename prefix).
    pub ts_ms: u64,
    /// `from:` — the claimed sender as a human route. For an Agent sender this is the sender's bus
    /// address at publication time: display only, never authority, because an address is mutable
    /// and a released one is immediately reusable.
    pub from: Option<String>,
    /// `from-id:` — the sender's immutable agent ID, written by the version-2 message writer.
    /// This, not `from`, is the sender's identity for provenance, replies, and automation.
    pub from_id: Option<String>,
    /// `subject:`.
    pub subject: Option<String>,
    /// `in-reply-to:` — the filename of the message this replies to.
    pub in_reply_to: Option<String>,
    /// `tags:` — comma-separated.
    pub tags: Vec<String>,
    /// `priority:` — `low` | `normal` | `high`, if set.
    pub priority: Option<String>,
    /// `idempotency-key:` — the caller's optional operation identity for exact retries.
    pub idempotency_key: Option<String>,
    /// `stream:` + `event-id:` classify an ordinary inbox record as an event.
    pub stream: Option<Box<str>>,
    pub event_id: Option<Box<str>>,
    pub event_key: Option<Box<str>>,
    /// The markdown body.
    pub body: String,
}

impl Message {
    /// How this inbox row's sender must be attributed.
    ///
    /// The authoritative field is `from_id`; `from` is a mutable, immediately reusable route and is
    /// never authority. The collision-aware version-1 path is used only when the record predates
    /// `from_id`. `recipient_agent_id` is the row's own state owner — an inbox row is owned by its
    /// recipient — which is what decides whether a colliding legacy endpoint may be attributed.
    pub fn sender_attribution(
        &self,
        recipient_agent_id: &str,
        collisions: &impl LegacyCollisionIndex,
    ) -> Option<EndpointAttribution> {
        let (version, value) = match self.from_id.as_deref() {
            Some(id) => (MESSAGE_RECORD_VERSION_2, id),
            None => (MESSAGE_RECORD_VERSION_1, self.from.as_deref()?),
        };
        Some(
            DurableEndpoint {
                version,
                value,
                kind: None,
                owns_row: false,
                owner_agent_id: recipient_agent_id,
            }
            .attribute(collisions),
        )
    }

    /// The canonical endpoint a reply to this message must address.
    ///
    /// `None` means refuse: either there is no sender at all, or the sender is a colliding
    /// version-1 endpoint this row does not own, in which case replying would deliver to whichever
    /// subject kept the bytes rather than to the subject that actually wrote the message.
    pub fn reply_target(
        &self,
        recipient_agent_id: &str,
        collisions: &impl LegacyCollisionIndex,
    ) -> Option<String> {
        self.sender_attribution(recipient_agent_id, collisions)?
            .reply_target()
            .map(str::to_owned)
    }
}

/// [`Message::reply_target`] against the catalog's own durable collision metadata.
///
/// This is the accessor a reply command should call: it never reads `from`, so an address that has
/// since been released and reused cannot capture the reply.
///
/// `Err` means the collision record exists but could not be read as this schema, so no legacy
/// endpoint can be attributed at all; `Ok(None)` is the ordinary refusal (no sender, or a
/// contested version-1 endpoint this row does not own).
pub fn reply_target_in(
    catalog_root: &Path,
    message: &Message,
    recipient_agent_id: &str,
) -> anyhow::Result<Option<String>> {
    let collisions = crate::catalog_migrate::load_legacy_id_collisions(catalog_root)?;
    Ok(message.reply_target(recipient_agent_id, &collisions))
}

/// Current unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A fresh canonical `<unix-ms>-<rand6>.md` filename (the shared grammar — messages AND context
/// decision entries use it, so both sort chronologically by name).
pub fn new_filename() -> String {
    format!("{}-{}.md", now_ms(), rand6())
}

/// Six Crockford-base32 chars from `/dev/urandom` (falls back to a time-derived value).
fn rand6() -> String {
    let mut buf = [0u8; 6];
    let ok = fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .is_ok();
    if !ok {
        // Degenerate fallback — mix the ns clock so it isn't all-zeros.
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (n >> (i * 8)) as u8;
        }
    }
    buf.iter()
        .map(|b| CROCKFORD[(*b as usize) % 32] as char)
        .collect()
}

/// True if `name` matches the frozen bus grammar `^[0-9]{13}-[0-9a-z]{6}\.md$`.
/// The reader accepts the FULL `[0-9a-z]` rand6 alphabet — st2 generates a Crockford subset, but a
/// peer message may use any lowercase alnum and dropping those would lose real messages.
pub fn is_message_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".md") else {
        return false;
    };
    let Some((ts, rand)) = stem.split_once('-') else {
        return false;
    };
    ts.len() == 13
        && ts.bytes().all(|b| b.is_ascii_digit())
        && rand.len() == 6
        && rand
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

/// Render a message file's contents (frontmatter + body) for a sender that has no separate
/// immutable ID to declare — a service principal or an external requester, whose canonical
/// endpoint IS the route in `from`.
pub fn render_message(
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> String {
    render_message_with_idempotency(from, None, subject, in_reply_to, tags, body, None)
}

/// Render a message whose sender IS an Agent, so the route goes in `from:` and the authoritative
/// immutable ID goes in `from-id:`. A reader must key on `from-id`, never on the mutable route.
pub fn render_agent_message(
    from_address: &str,
    from_id: Option<&str>,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> String {
    render_message_with_idempotency(from_address, from_id, subject, in_reply_to, tags, body, None)
}

/// `from_id` is written as a separate `from-id:` line only by the version-2 writer. Keeping it out
/// of the version-1 shape is what makes the reader-first rollout byte-identical: an older reader
/// sees exactly the frontmatter it always saw.
fn render_message_with_idempotency(
    from: &str,
    from_id: Option<&str>,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> String {
    let mut s = String::from("---\n");
    s.push_str(&format!("from: {from}\n"));
    if let Some(id) = from_id {
        s.push_str(&format!("from-id: {id}\n"));
    }
    if let Some(subj) = subject {
        s.push_str(&format!("subject: {subj}\n"));
    }
    if let Some(irt) = in_reply_to {
        s.push_str(&format!("in-reply-to: {irt}\n"));
    }
    if !tags.is_empty() {
        s.push_str(&format!("tags: {}\n", tags.join(", ")));
    }
    if let Some(key) = idempotency_key {
        s.push_str(&format!("idempotency-key: {key}\n"));
    }
    s.push_str("---\n");
    s.push_str(body);
    if !body.ends_with('\n') {
        s.push('\n');
    }
    s
}

/// Parse a message file's contents into frontmatter fields + body. Permissive.
pub(crate) fn parse_message(filename: &str, contents: &str) -> Message {
    let ts_ms = filename
        .split_once('-')
        .and_then(|(ts, _)| ts.parse::<u64>().ok())
        .unwrap_or(0);

    let mut msg = Message {
        filename: filename.to_string(),
        ts_ms,
        from: None,
        from_id: None,
        subject: None,
        in_reply_to: None,
        tags: Vec::new(),
        priority: None,
        idempotency_key: None,
        stream: None,
        event_id: None,
        event_key: None,
        body: String::new(),
    };

    // Frontmatter is an opening `---` line … a closing `---` line.
    let rest = contents
        .strip_prefix("---\n")
        .or_else(|| contents.strip_prefix("---\r\n"));
    if let Some(rest) = rest
        && let Some(end) = rest.find("\n---")
    {
        let front = &rest[..end];
        // body starts after the closing `---` line
        let after = &rest[end + 1..]; // at the `---` line
        let body = after.split_once('\n').map(|x| x.1).unwrap_or("");
        for line in front.lines() {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            let v = v.trim();
            match k.trim() {
                "from" => msg.from = Some(v.to_string()),
                "from-id" => msg.from_id = Some(v.to_string()),
                "subject" => msg.subject = Some(v.to_string()),
                "in-reply-to" => msg.in_reply_to = Some(v.to_string()),
                "tags" => {
                    msg.tags = v
                        .split(',')
                        .map(|t| t.trim().to_string())
                        .filter(|t| !t.is_empty())
                        .collect()
                }
                "priority" => msg.priority = Some(v.to_string()),
                "idempotency-key" => msg.idempotency_key = Some(v.to_string()),
                "stream" => msg.stream = Some(v.into()),
                "event-id" => msg.event_id = Some(v.into()),
                "key" => msg.event_key = Some(v.into()),
                _ => {}
            }
        }
        msg.body = body.to_string();
    } else {
        // No frontmatter — the whole thing is the body.
        msg.body = contents.to_string();
    }
    msg
}

/// Send: write a new message file into `inbox_dir`, returning its filename. Creates `inbox_dir` if
/// missing; retries on the astronomically-unlikely filename collision.
///
/// The message is materialized atomically (temporary sibling + rename). A direct write under the
/// canonical name exposes an empty or partial file to concurrent readers after create/truncate but
/// before the bytes arrive; that incomplete message has no parseable sender or subject.
pub fn send_to_inbox(
    inbox_dir: &Path,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> anyhow::Result<String> {
    send_to_inbox_from_agent(inbox_dir, from, None, subject, in_reply_to, tags, body)
}

/// [`send_to_inbox`] for a sender that owns an immutable agent ID, which is written as `from-id:`.
///
/// The ledger-less delivery paths (an external mailbox recipient, or an external sender) must
/// still carry the authoritative sender identity: a reply keys on `from-id`, and for any subject
/// whose ID differs from its address — every subject created after identity migration — the
/// mutable route alone cannot resolve back to the sender.
#[allow(clippy::too_many_arguments)]
pub fn send_to_inbox_from_agent(
    inbox_dir: &Path,
    from: &str,
    from_id: Option<&str>,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> anyhow::Result<String> {
    fs::create_dir_all(inbox_dir)?;
    let contents = render_agent_message(from, from_id, subject, in_reply_to, tags, body);
    // This deliberately cannot match `is_message_filename`, so a concurrent scan ignores it.
    let tmp = inbox_dir.join(tmp_name());
    if let Err(error) = fs::write(&tmp, &contents) {
        let _ = fs::remove_file(&tmp);
        return Err(error.into());
    }
    for _ in 0..8 {
        let filename = new_filename();
        let path = inbox_dir.join(&filename);
        if !path.exists() {
            if let Err(error) = fs::rename(&tmp, &path) {
                let _ = fs::remove_file(&tmp);
                return Err(error.into());
            }
            return Ok(filename);
        }
    }
    let _ = fs::remove_file(&tmp);
    anyhow::bail!(
        "could not allocate a unique message filename in {}",
        inbox_dir.display()
    )
}

/// Atomically materialize one already-rendered canonical message filename.
///
/// Repeating the same filename and bytes is success; the same filename with different bytes is an
/// error. Request publication uses this after durably reserving its random filename, so a replay
/// finishes an interrupted send without allocating a second bus message.
pub fn materialize_message_once(
    inbox_dir: &Path,
    filename: &str,
    contents: &str,
) -> anyhow::Result<bool> {
    if !is_message_filename(filename) {
        anyhow::bail!("invalid canonical message filename `{filename}`");
    }
    fs::create_dir_all(inbox_dir)?;
    let destination = inbox_dir.join(filename);
    if destination.is_file() {
        let existing = fs::read_to_string(&destination)?;
        if existing == contents {
            return Ok(false);
        }
        anyhow::bail!("message filename collision with different bytes: {filename}");
    }
    let temporary = inbox_dir.join(tmp_name());
    let mut temporary_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)?;
    temporary_file.write_all(contents.as_bytes())?;
    temporary_file.sync_all()?;
    drop(temporary_file);
    let result = match fs::hard_link(&temporary, &destination) {
        Ok(()) => Ok(true),
        Err(_) if destination.is_file() => {
            let existing = fs::read_to_string(&destination)?;
            if existing == contents {
                Ok(false)
            } else {
                anyhow::bail!("message filename collision with different bytes: {filename}")
            }
        }
        Err(error) => Err(error.into()),
    };
    let _ = fs::remove_file(temporary);
    if result.is_ok() {
        File::open(inbox_dir)?.sync_all()?;
    }
    result
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_name() -> String {
    format!(
        ".message.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Read a canonical entry that was already returned by `read_dir`. Removing a message concurrently
/// (for example, inbox→archive) is normal: that entry vanished, it did not become an empty message.
fn read_message_contents(path: &Path) -> anyhow::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(anyhow::anyhow!("reading {}: {error}", path.display())),
    }
}

/// List the canonical messages in `dir` (inbox or archive), sorted by send time. Non-message files
/// are skipped. Frontmatter is parsed for metadata.
pub fn list_dir(dir: &Path) -> anyhow::Result<Vec<Message>> {
    let mut msgs = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(msgs),
        Err(error) => return Err(error.into()),
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_message_filename(&name) {
            continue;
        }
        let Some(contents) = read_message_contents(&entry.path())? else {
            continue;
        };
        msgs.push(parse_message(&name, &contents));
    }
    // Primary order is send time; the `<rand6>` suffix is a deterministic tiebreak so two messages
    // that land in the same millisecond still list in a stable, reproducible order (the wire format
    // can't recover true send-order within a millisecond — this at least isn't `read_dir`-arbitrary).
    msgs.sort_by(|a, b| {
        a.ts_ms
            .cmp(&b.ts_ms)
            .then_with(|| a.filename.cmp(&b.filename))
    });
    Ok(msgs)
}

/// List messages that are logically present in `inbox_dir`.
///
/// The sibling `archive/` is a durable handled-message receipt. Eventually-consistent sync can
/// briefly restore an inbox file after a local inbox→archive move but before the delete tombstone
/// converges. If the same canonical filename already exists in archive, the archived copy wins and
/// the raw inbox duplicate is not unread work. Listing also removes that duplicate so every reader
/// helps the archive receipt converge locally.
pub fn list_inbox(inbox_dir: &Path) -> anyhow::Result<Vec<Message>> {
    let archive_dir = sibling_archive_dir(inbox_dir);
    let mut unread = Vec::new();
    for message in list_dir(inbox_dir)? {
        if archive_dir.join(&message.filename).is_file() {
            remove_inbox_duplicate(&inbox_dir.join(&message.filename), &message.filename)?;
        } else {
            unread.push(message);
        }
    }
    Ok(unread)
}

/// The archive directory paired with an inbox. Both native
/// `resources/{inbox,archive}` and flat `<identity>/{inbox,archive}` layouts use sibling boxes.
fn sibling_archive_dir(inbox_dir: &Path) -> PathBuf {
    inbox_dir.parent().unwrap_or(inbox_dir).join("archive")
}

/// Read one message file from `dir`.
pub fn read_msg(dir: &Path, filename: &str) -> anyhow::Result<Message> {
    let contents = fs::read_to_string(dir.join(filename))
        .map_err(|e| anyhow::anyhow!("reading {filename}: {e}"))?;
    Ok(parse_message(filename, &contents))
}

/// An agent's inbox dir (VRS §5): `<agent_dir>/resources/inbox`.
pub fn inbox_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join("inbox")
}

/// An agent's message-archive dir: `<agent_dir>/resources/archive`.
pub fn archive_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join("archive")
}

fn sent_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join(SENT_DIR)
}

/// Read one sender's ledger without consulting recipient state.
pub fn list_sent(agent_dir: &Path, include_body: bool) -> anyhow::Result<SentMessages> {
    let root = sent_dir(agent_dir);
    let _lock = SentLock::shared(&root)?;
    list_sent_unlocked(&root, include_body)
}

/// Inspect one sender's ledger without creating sender state.
///
/// Doctor uses this read-only path. If a sender starts its first publication during the initial
/// unlocked read, the second lock check repeats the read under that sender's persistent lock.
pub fn inspect_sent(agent_dir: &Path, include_body: bool) -> anyhow::Result<SentMessages> {
    let root = sent_dir(agent_dir);
    if let Some(_lock) = SentLock::shared_existing(&root)? {
        return list_sent_unlocked(&root, include_body);
    }
    let snapshot = list_sent_unlocked(&root, include_body);
    if let Some(_lock) = SentLock::shared_existing(&root)? {
        list_sent_unlocked(&root, include_body)
    } else {
        snapshot
    }
}

fn list_sent_unlocked(root: &Path, include_body: bool) -> anyhow::Result<SentMessages> {
    let head_path = root.join(SENT_HEAD);
    let head: SentHead = match fs::read(&head_path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("reading sent head {}", head_path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            anyhow::ensure!(
                read_sent_active(root)?.is_none()
                    && read_pending_records(&root.join(SENT_PENDING))?.is_empty()
                    && read_sent_records(&root.join(SENT_MESSAGES))?.is_empty()
                    && read_sent_commits(&root.join(SENT_COMMITS))?.is_empty()
                    && read_sent_keys(&root.join(SENT_KEYS))?.is_empty(),
                "sender state exists without a sent head"
            );
            return Ok(SentMessages {
                coverage: SentCoverage::Unavailable,
                messages: Vec::new(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    validate_sent_head(&head)?;
    let pending = read_pending_records(&root.join(SENT_PENDING))?;
    anyhow::ensure!(pending.len() <= 1, "multiple pending sent intents");
    let active = read_sent_active(root)?;
    match (active.as_ref(), pending.as_slice()) {
        (None, []) | (None, [_]) => {}
        (Some(active), [record]) => anyhow::ensure!(
            active.filename == record.filename && active.record_digest == digest_json(record)?,
            "active sent intent differs from pending record"
        ),
        (Some(_), []) => {}
        _ => unreachable!(),
    }
    let committed_pending_cleanup = if active.is_none()
        && let [record] = pending.as_slice()
        && sent_record_exists(root, &record.filename)?
    {
        anyhow::ensure!(
            head_tip_commits_record(root, &head, record)?,
            "committed pending intent is missing its active marker"
        );
        true
    } else {
        false
    };

    let rows = read_sent_records(&root.join(SENT_MESSAGES))?
        .into_iter()
        .map(|record| (record.filename.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let commits = read_sent_commits(&root.join(SENT_COMMITS))?;
    let mut reachable_nodes = BTreeSet::new();
    let mut reachable_rows = BTreeSet::new();
    let mut messages = Vec::new();
    let mut digest = head.tip.clone();
    let mut ordinal = head.count;
    while let Some(current) = digest {
        let node = commits.get(&current).context("missing sent commit node")?;
        anyhow::ensure!(digest_json(node)? == current, "sent commit digest mismatch");
        anyhow::ensure!(node.ordinal == ordinal, "sent commit ordinal mismatch");
        let row = rows
            .get(&node.filename)
            .context("missing committed sent record")?;
        anyhow::ensure!(
            digest_json(row)? == node.row_digest,
            "sent row digest mismatch"
        );
        anyhow::ensure!(
            reachable_nodes.insert(current),
            "sent commit chain contains a cycle"
        );
        anyhow::ensure!(
            reachable_rows.insert(node.filename.clone()),
            "sent commit chain references one row more than once"
        );
        messages.push(row.row(include_body));
        digest = node.previous.clone();
        ordinal = ordinal
            .checked_sub(1)
            .context("sent commit chain exceeds head count")?;
    }
    anyhow::ensure!(ordinal == 0, "sent commit count/genesis mismatch");
    if let Some(active) = &active
        && pending.is_empty()
    {
        let row = rows
            .get(&active.filename)
            .context("active sent intent without pending has no committed row")?;
        anyhow::ensure!(
            reachable_rows.contains(&active.filename),
            "active sent intent without pending is not committed"
        );
        anyhow::ensure!(
            digest_json(row)? == active.record_digest,
            "active sent intent differs from committed row"
        );
    }

    let active_row = active.as_ref().and_then(|active| {
        rows.get(&active.filename)
            .filter(|_| !reachable_rows.contains(&active.filename))
    });
    if let Some(row) = active_row {
        let active = active
            .as_ref()
            .context("uncommitted sent row has no active intent")?;
        anyhow::ensure!(
            active.record_digest == digest_json(row)?,
            "active sent intent differs from uncommitted row"
        );
    }
    let explained_node = match active_row {
        Some(row) => Some(digest_json(&sent_commit(&head, row)?)?),
        None => None,
    };
    anyhow::ensure!(
        rows.keys().all(|filename| {
            reachable_rows.contains(filename)
                || active
                    .as_ref()
                    .is_some_and(|active| active.filename == *filename)
        }),
        "unexplained sent record"
    );
    anyhow::ensure!(
        commits.keys().all(|node| {
            reachable_nodes.contains(node) || explained_node.as_ref() == Some(node)
        }),
        "unexplained sent commit node"
    );
    let keys = read_sent_keys(&root.join(SENT_KEYS))?;
    let mut expected_keys = BTreeSet::new();
    for filename in &reachable_rows {
        let row = rows.get(filename).unwrap();
        if let Some(key) = &row.idempotency_key {
            let key_digest = digest_json(&(&row.to, key))?;
            if let Some(receipt) = keys.get(&key_digest) {
                anyhow::ensure!(
                    receipt.filename == row.filename && receipt.record_digest == digest_json(row)?,
                    "sent idempotency receipt differs from committed row"
                );
            } else {
                anyhow::ensure!(
                    active
                        .as_ref()
                        .is_some_and(|active| active.filename == *filename)
                        || (committed_pending_cleanup
                            && pending
                                .first()
                                .is_some_and(|record| record.filename == *filename)),
                    "committed sent row is missing its idempotency receipt"
                );
            }
            expected_keys.insert(key_digest);
        }
    }
    anyhow::ensure!(
        keys.keys().all(|digest| expected_keys.contains(digest)),
        "unexplained sent idempotency receipt"
    );

    messages.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.filename.cmp(&right.filename))
    });
    let incomplete =
        usize::from((!pending.is_empty() && !committed_pending_cleanup) || active.is_some());
    let coverage = if incomplete == 0 {
        SentCoverage::Since { since: head.since }
    } else {
        SentCoverage::Partial {
            since: head.since,
            pending: incomplete,
        }
    };
    Ok(SentMessages { coverage, messages })
}

fn read_sent_records(directory: &Path) -> anyhow::Result<Vec<SentRecord>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            anyhow::bail!("sent record filename is not UTF-8");
        };
        if name.starts_with(".message.tmp-") {
            continue;
        }
        anyhow::ensure!(name.ends_with(".json"), "unexpected sent record entry");
        let record: SentRecord = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("reading sent record {}", entry.path().display()))?;
        validate_sent_record(&record)?;
        anyhow::ensure!(
            sent_record_name(&record.filename) == name,
            "sent record filename does not match its payload"
        );
        records.push(record);
    }
    records.sort_by(|left, right| left.filename.cmp(&right.filename));
    Ok(records)
}

fn read_pending_records(directory: &Path) -> anyhow::Result<Vec<SentRecord>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            anyhow::bail!("pending sent record filename is not UTF-8");
        };
        if name.starts_with(".message.tmp-") {
            continue;
        }
        let digest = name
            .strip_suffix(".json")
            .context("unexpected pending sent record entry")?;
        anyhow::ensure!(is_sha256(digest), "invalid pending sent record digest");
        let record: SentRecord = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("reading pending sent record {}", entry.path().display()))?;
        validate_sent_record(&record)?;
        anyhow::ensure!(
            is_message_filename(&record.filename),
            "invalid sent record filename"
        );
        anyhow::ensure!(
            digest_json(&record)? == digest,
            "pending sent record digest mismatch"
        );
        records.push(record);
    }
    records.sort_by(|left, right| left.filename.cmp(&right.filename));
    Ok(records)
}

fn read_sent_active(root: &Path) -> anyhow::Result<Option<SentActive>> {
    let path = root.join(SENT_ACTIVE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let active: SentActive = serde_json::from_slice(&bytes)
        .with_context(|| format!("reading active sent intent {}", path.display()))?;
    anyhow::ensure!(
        active.version == SENT_VERSION,
        "unsupported active sent version"
    );
    anyhow::ensure!(
        is_message_filename(&active.filename),
        "invalid active sent filename"
    );
    Ok(Some(active))
}

fn read_sent_commits(directory: &Path) -> anyhow::Result<BTreeMap<String, SentCommit>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut commits = BTreeMap::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            anyhow::bail!("sent commit filename is not UTF-8");
        };
        if name.starts_with(".message.tmp-") {
            continue;
        }
        let digest = name
            .strip_suffix(".json")
            .context("unexpected sent commit entry")?;
        let node: SentCommit = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("reading sent commit {}", entry.path().display()))?;
        anyhow::ensure!(
            node.version == SENT_VERSION,
            "unsupported sent commit version"
        );
        anyhow::ensure!(
            is_message_filename(&node.filename),
            "invalid sent commit filename"
        );
        anyhow::ensure!(
            digest_json(&node)? == digest,
            "sent commit filename mismatch"
        );
        anyhow::ensure!(
            commits.insert(digest.to_string(), node).is_none(),
            "duplicate sent commit"
        );
    }
    Ok(commits)
}

fn read_sent_keys(directory: &Path) -> anyhow::Result<BTreeMap<String, SentKey>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut keys = BTreeMap::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            anyhow::bail!("sent key filename is not UTF-8");
        };
        if name.starts_with(".message.tmp-") {
            continue;
        }
        let digest = name
            .strip_suffix(".json")
            .context("unexpected sent key entry")?;
        let key: SentKey = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("reading sent key {}", entry.path().display()))?;
        anyhow::ensure!(key.version == SENT_VERSION, "unsupported sent key version");
        anyhow::ensure!(
            digest_json(&(&key.to, &key.key))? == digest,
            "sent key filename mismatch"
        );
        anyhow::ensure!(
            keys.insert(digest.to_string(), key).is_none(),
            "duplicate sent key"
        );
    }
    Ok(keys)
}

fn sent_record_name(filename: &str) -> String {
    format!("{filename}.json")
}

/// Eval-owned authority for one external flat requester mailbox. General catalog routing remains
/// declaration-only; possessing this value is the explicit exception at message call sites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalInbox {
    root: PathBuf,
    identity: String,
    inbox: PathBuf,
}

impl ExternalInbox {
    pub fn new(root: &Path, identity: &str) -> anyhow::Result<Self> {
        let mut components = Path::new(identity).components();
        let safe = matches!(components.next(), Some(Component::Normal(component)) if component == identity)
            && components.next().is_none();
        anyhow::ensure!(
            safe,
            "external requester identity must be one non-empty relative path component"
        );
        Ok(Self {
            root: root.to_path_buf(),
            identity: identity.to_owned(),
            inbox: root.join(identity).join("inbox"),
        })
    }

    pub fn provision(root: &Path, identity: &str) -> anyhow::Result<Self> {
        let external = Self::new(root, identity)?;
        fs::create_dir_all(&external.inbox).map_err(|error| {
            anyhow::anyhow!(
                "provisioning external requester {identity:?} inbox {}: {error}",
                external.inbox.display()
            )
        })?;
        Ok(external)
    }
}

/// Resolve an inbox by exact immutable agent ID. A proven catalog-less root retains the legacy
/// flat bus; inside a catalog an unknown ID always fails closed.
pub fn resolve_inbox(root: &Path, agent_id: &str, host: &str) -> anyhow::Result<PathBuf> {
    resolve_list_box(root, agent_id, host, false, false)
}

/// Resolve a normal declared inbox or one exact eval-owned external requester capability.
pub fn resolve_inbox_with_external(
    root: &Path,
    id: &str,
    host: &str,
    external: Option<&ExternalInbox>,
) -> anyhow::Result<PathBuf> {
    match resolve_inbox(root, id, host) {
        Ok(inbox) => Ok(inbox),
        Err(error) => match external {
            Some(external)
                if external.root == root && external.identity == id && external.inbox.is_dir() =>
            {
                Ok(external.inbox.clone())
            }
            _ => Err(error),
        },
    }
}

/// Archive companion to [`resolve_inbox`], with the same exact-ID and catalog-less boundaries.
pub fn resolve_archive(root: &Path, agent_id: &str, host: &str) -> anyhow::Result<PathBuf> {
    resolve_list_box(root, agent_id, host, true, false)
}

/// Resolve one box for `message ls` by exact immutable agent ID.
pub fn resolve_list_box(
    root: &Path,
    agent_id: &str,
    host: &str,
    archive: bool,
    orphan: bool,
) -> anyhow::Result<PathBuf> {
    select_list_box(root, &AgentSelector::id(agent_id), host, archive, orphan)
}

/// Resolve one box for `message ls`.
///
/// The permissive flat layout is automatic only when discovery proves that `root` is catalog-less:
/// a DING or eval seat under a catalog-less `ST_ROOT` names its box by the literal selector.
/// Once any valid or malformed declaration makes it a catalog, an absent subject is an error.
/// `orphan` is the explicit recovery path for inspecting a raw flat box inside such a root.
pub fn select_list_box(
    root: &Path,
    selector: &AgentSelector,
    host: &str,
    archive: bool,
    orphan: bool,
) -> anyhow::Result<PathBuf> {
    let id = selector.as_input();
    let flat = || {
        root.join(id)
            .join(if archive { "archive" } else { "inbox" })
    };
    if orphan {
        return Ok(flat());
    }
    if apply_incomplete(root) {
        return select_agent_dir(root, selector, host)?
            .map(|agent_dir| {
                if archive {
                    archive_dir(&agent_dir)
                } else {
                    inbox_dir(&agent_dir)
                }
            })
            .with_context(|| {
                format!("agent '{id}' is not addressable while catalog apply is incomplete")
            });
    }
    let discovered = crate::discover(root);
    let book = crate::spec::address_book(&discovered.specs, host)?;
    match book.resolve(selector) {
        Ok(subject) => {
            let agent_dir = discovered
                .specs
                .iter()
                .find(|spec| spec.agent_id(host) == subject.id.as_str())
                .and_then(|spec| spec.path.parent())
                .context("resolved subject has no identity directory")?;
            return Ok(if archive {
                archive_dir(agent_dir)
            } else {
                inbox_dir(agent_dir)
            });
        }
        Err(ResolveError::UnknownId { .. } | ResolveError::UnknownAddress { .. }) => {}
        Err(ambiguous) => return Err(anyhow::Error::new(ambiguous)),
    }

    if discovered.specs.is_empty() && discovered.errors.is_empty() {
        return Ok(flat());
    }
    anyhow::bail!("no agent '{id}' found in catalog {}", root.display())
}

/// Resolve an exact immutable agent ID to its agent folder. Returns `None` if no subject carries
/// that ID. A retired subject keeps its ID and stays reachable here.
pub fn resolve_agent_dir(
    catalog_root: &Path,
    agent_id: &str,
    this_host: &str,
) -> anyhow::Result<Option<PathBuf>> {
    select_agent_dir(catalog_root, &AgentSelector::id(agent_id), this_host)
}

/// [`resolve_agent_dir`] for an already typed selector: an exact immutable ID never falls through
/// to address lookup, and an ordinary human reference never reaches the ID namespace.
pub fn select_agent_dir(
    catalog_root: &Path,
    selector: &AgentSelector,
    this_host: &str,
) -> anyhow::Result<Option<PathBuf>> {
    Ok(resolve_agent_handle(catalog_root, selector, this_host)?.map(|agent| agent.path))
}

pub fn with_resolved_agent_dir<T>(
    catalog_root: &Path,
    agent_id: &str,
    this_host: &str,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_resolved_state_dir(catalog_root, agent_id, this_host, &[], true, operation)
}

pub fn with_selected_agent_dir<T>(
    catalog_root: &Path,
    selector: &AgentSelector,
    this_host: &str,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_selected_state_dir(catalog_root, selector, this_host, &[], true, operation)
}

/// Run an operation against one subject's durable state directory, keyed by exact agent ID.
pub fn with_resolved_state_dir<T>(
    catalog_root: &Path,
    agent_id: &str,
    this_host: &str,
    components: &[&str],
    create: bool,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_selected_state_dir(
        catalog_root,
        &AgentSelector::id(agent_id),
        this_host,
        components,
        create,
        operation,
    )
}

pub fn with_selected_state_dir<T>(
    catalog_root: &Path,
    selector: &AgentSelector,
    this_host: &str,
    components: &[&str],
    create: bool,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let identity = selector.as_input();
    match resolve_agent_handle(catalog_root, selector, this_host)? {
        Some(agent) => {
            test_capability_checkpoint();
            let path = match agent.capability.as_ref() {
                Some(capability) if components.is_empty() => {
                    crate::catalog_transaction::retained_dir_path(capability)?
                }
                Some(capability) => {
                    let directory = open_message_box(capability, components, create)?
                        .context("resolved state directory does not exist")?;
                    let path = crate::catalog_transaction::retained_dir_path(&directory)?;
                    return operation(&path);
                }
                None => components
                    .iter()
                    .fold(agent.path, |path, component| path.join(component)),
            };
            operation(&path)
        }
        None => {
            let discovered = crate::discover(catalog_root);
            anyhow::ensure!(
                crate::catalog_transaction::catalog_transition(catalog_root)?.is_none()
                    && !catalog_root.join(crate::catalog_lock::CONTROL_DIR).exists()
                    && discovered.specs.is_empty()
                    && discovered.errors.is_empty(),
                "no agent '{identity}' found in catalog {}",
                catalog_root.display()
            );
            operation(
                &components
                    .iter()
                    .fold(catalog_root.join(identity), |path, component| {
                        path.join(component)
                    }),
            )
        }
    }
}

/// Run an operation against retained, no-follow capabilities for an agent's inbox and archive.
///
/// Both directories are opened relative to the resolved agent capability, so replacing the
/// declaration directory or either message-box ancestor with a symlink cannot redirect the
/// operation outside the catalog after recipient resolution.
pub(crate) fn with_resolved_message_boxes<T>(
    catalog_root: &Path,
    agent_id: &str,
    this_host: &str,
    operation: impl FnOnce(&Path, &Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_selected_message_boxes(
        catalog_root,
        &AgentSelector::id(agent_id),
        this_host,
        operation,
    )
}

pub(crate) fn with_selected_message_boxes<T>(
    catalog_root: &Path,
    selector: &AgentSelector,
    this_host: &str,
    operation: impl FnOnce(&Path, &Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let identity = selector.as_input();
    let agent = resolve_agent_handle(catalog_root, selector, this_host)?.with_context(|| {
        format!(
            "no agent '{identity}' found in catalog {}",
            catalog_root.display()
        )
    })?;
    let capability = agent
        .capability
        .as_ref()
        .context("resolved agent has no retained directory capability")?;
    let inbox = open_message_box(capability, &["resources", "inbox"], true)?
        .context("created inbox capability is missing")?;
    let archive = open_message_box(capability, &["resources", "archive"], true)?
        .context("created archive capability is missing")?;
    operation(
        &crate::catalog_transaction::retained_dir_path(&inbox)?,
        &crate::catalog_transaction::retained_dir_path(&archive)?,
    )
}

/// Resolve one typed selector against a fenced snapshot of the catalog's address book.
///
/// Ambiguity is an error, not an absent subject: two subjects answering one human reference is a
/// catalog the caller must not silently pick from (root `spec.md`, fail-closed resolution).
fn resolve_agent_handle(
    catalog_root: &Path,
    selector: &AgentSelector,
    this_host: &str,
) -> anyhow::Result<Option<AddressableAgent>> {
    for _ in 0..3 {
        let before = address_fence(catalog_root)?;
        let candidates = addressable_agent_dirs(catalog_root, this_host, before.1.as_ref())?;
        let after = address_fence(catalog_root)?;
        if before != after {
            continue;
        }
        let book = AddressBook::new(
            candidates
                .iter()
                .map(|candidate| candidate.subject.clone())
                .collect(),
        );
        let id = match book.resolve(selector) {
            Ok(subject) => subject.id.clone(),
            Err(ResolveError::UnknownId { .. } | ResolveError::UnknownAddress { .. }) => {
                return Ok(None);
            }
            Err(ambiguous) => return Err(anyhow::Error::new(ambiguous)),
        };
        let mut matched = candidates
            .into_iter()
            .filter(|candidate| candidate.subject.id == id)
            .collect::<Vec<_>>();
        matched.sort_by(|left, right| left.path.cmp(&right.path));
        matched.dedup_by(|left, right| left.path == right.path);
        return Ok((matched.len() == 1).then(|| matched.remove(0)));
    }
    anyhow::bail!(
        "catalog address book changed repeatedly while resolving {:?}",
        selector.as_input()
    )
}

fn address_fence(
    catalog_root: &Path,
) -> anyhow::Result<(
    Option<u64>,
    Option<crate::catalog_transaction::CatalogTransition>,
)> {
    let first_generation = crate::catalog_lock::read_generation_token(catalog_root)?;
    let transition = crate::catalog_transaction::catalog_transition(catalog_root)?;
    test_address_fence_checkpoint();
    let second_generation = crate::catalog_lock::read_generation_token(catalog_root)?;
    anyhow::ensure!(
        first_generation == second_generation,
        "catalog address book changed while sampling its transition fence"
    );
    Ok((second_generation, transition))
}

#[cfg(debug_assertions)]
fn test_address_fence_checkpoint() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_ADDRESS_FENCE_READY"),
        std::env::var("ST2_TEST_ADDRESS_FENCE_RELEASE"),
    ) else {
        return;
    };
    if OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&ready)
        .is_err()
    {
        return;
    }
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_address_fence_checkpoint() {}

#[derive(Debug)]
struct AddressableAgent {
    /// The subject as the address book sees it: immutable ID, host, effective address, routability.
    subject: Subject,
    path: PathBuf,
    capability: Option<File>,
}

impl AddressableAgent {
    /// The immutable agent ID — the durable endpoint, ownership key, and automation selector.
    fn agent_id(&self) -> &str {
        self.subject.id.as_str()
    }

    /// The current human route, or `None` for a proved non-routable subject.
    fn bus_address(&self) -> Option<String> {
        self.subject.bus_address()
    }
}

fn addressable_agent_dirs(
    catalog_root: &Path,
    this_host: &str,
    transition: Option<&crate::catalog_transaction::CatalogTransition>,
) -> anyhow::Result<Vec<AddressableAgent>> {
    let Some(transition) = transition else {
        return crate::discover(catalog_root)
            .specs
            .into_iter()
            .map(|spec| {
                let path = spec
                    .path
                    .parent()
                    .context("Agent Spec has no identity directory")?
                    .to_path_buf();
                let capability = crate::catalog_transaction::open_dir_beneath(catalog_root, &path)?;
                Ok(AddressableAgent {
                    subject: spec.subject(this_host)?,
                    path,
                    capability: Some(capability),
                })
            })
            .collect();
    };
    let agents = catalog_root.join("agents");
    let metadata = match fs::symlink_metadata(&agents) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "canonical agents path is not a real directory: {}",
        agents.display()
    );
    let mut result = Vec::new();
    for host in sorted_real_entries(&agents, "host")? {
        let host_name = safe_entry_name(&host, "host")?;
        for identity in sorted_real_identity_entries(&host.path())? {
            let identity_name = safe_entry_name(&identity, "identity")?;
            let key = crate::catalog_transaction::AgentKey {
                host: host_name.clone(),
                identity: identity_name.clone(),
            };
            let path = identity.path();
            let capability = crate::catalog_transaction::open_dir_beneath(catalog_root, &path)?;
            let retained = crate::catalog_transaction::retained_dir_path(&capability)?;
            let declared = marker_subject(&retained, &key)?;
            let retained_state =
                transition.original_agents.contains(&key) && marker_state_exists(&retained)?;
            // A current declaration is authoritative even mid-transition: it is the only place the
            // subject's explicit `id` and `address` exist, and resolving a migrated subject by the
            // positional key alone would make it unreachable exactly while an apply is incomplete.
            // A retained-state-only entry has no readable declaration, so the positional key is
            // all there is — which is also the identity migration would have frozen for it.
            let subject = match (declared, retained_state) {
                (Some(subject), _) => subject,
                (None, true) => Subject {
                    id: AgentId::frozen_legacy(&key.host, &key.identity)?,
                    host: key.host,
                    effective_address: key.identity,
                    routable: true,
                },
                (None, false) => continue,
            };
            result.push(AddressableAgent {
                subject,
                path,
                capability: Some(capability),
            });
        }
    }
    Ok(result)
}

fn sorted_real_entries(dir: &Path, label: &str) -> anyhow::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in &entries {
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "canonical {label} path is not a real directory: {}",
            entry.path().display()
        );
    }
    Ok(entries)
}

fn sorted_real_identity_entries(dir: &Path) -> anyhow::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    let mut identities = Vec::with_capacity(entries.len());
    for entry in entries {
        if crate::harness_context::is_legacy_harness_context_staging_file(&entry)? {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "canonical identity path is not a real directory: {}",
            entry.path().display()
        );
        identities.push(entry);
    }
    Ok(identities)
}

fn safe_entry_name(entry: &fs::DirEntry, label: &str) -> anyhow::Result<String> {
    let value = entry
        .file_name()
        .into_string()
        .map_err(|_| anyhow::anyhow!("canonical {label} is not UTF-8"))?;
    anyhow::ensure!(safe_component(&value), "unsafe canonical {label} {value:?}");
    Ok(value)
}

/// The subject a current canonical declaration at this key declares, if any.
///
/// `None` means no declaration at that path currently claims this `<host>/<identity>` slot; the
/// caller then falls back to retained state. Returning the SUBJECT rather than a bool is what
/// keeps a migrated agent addressable by its explicit `id` while an apply is incomplete.
fn marker_subject(
    agent_dir: &Path,
    key: &crate::catalog_transaction::AgentKey,
) -> anyhow::Result<Option<Subject>> {
    let path = agent_dir.join("agent.kdl");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "canonical Agent Spec is not a real regular file: {}",
        path.display()
    );
    let declared = crate::discovery::parse_declared(&path)?;
    if declared.len() != 1
        || declared[0].host.as_deref() != Some(&key.host)
        || declared[0].identity.as_deref() != Some(key.identity.as_str())
    {
        return Ok(None);
    }
    let declared = &declared[0];
    Ok(Some(Subject {
        id: match declared.id.as_deref() {
            Some(id) => AgentId::parse(id)?,
            None => AgentId::frozen_legacy(&key.host, &key.identity)?,
        },
        host: key.host.clone(),
        effective_address: declared
            .address
            .clone()
            .unwrap_or_else(|| key.identity.clone()),
        // Mid-transition addressability exists to keep the STATE plane reachable while the
        // declaration plane is fenced, so it does not re-derive routability from desired state.
        routable: true,
    }))
}

fn marker_state_exists(agent_dir: &Path) -> anyhow::Result<bool> {
    let mut found = false;
    for name in ["resources", "archive", "inbox"] {
        let path = agent_dir.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => found = true,
            Ok(_) => anyhow::bail!(
                "agent state path is not a real directory: {}",
                path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    match fs::symlink_metadata(agent_dir.join("status")) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => found = true,
        Ok(_) => anyhow::bail!("agent status is not a real regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let resources = agent_dir.join("resources");
    if resources.is_dir() {
        for relative in ["inbox", "archive", "context", "context/decisions"] {
            match fs::symlink_metadata(resources.join(relative)) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => anyhow::bail!("agent resource path is not a real directory"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(found)
}

fn apply_incomplete(root: &Path) -> bool {
    fs::symlink_metadata(crate::catalog_lock::apply_marker_path(root)).is_ok()
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | ".." | ".git" | ".st2")
        && Path::new(value).components().count() == 1
}

/// The default subject for a reply to a message whose subject was `original`: the original prefixed
/// with `re: `, unless it already carries a (case-insensitive) `re:` prefix. `None` stays `None`.
pub fn reply_subject(original: Option<&str>) -> Option<String> {
    original.map(|s| {
        let t = s.trim_start();
        if t.len() >= 3 && t[..3].eq_ignore_ascii_case("re:") {
            s.to_string()
        } else {
            format!("re: {s}")
        }
    })
}

/// One line of a message thread: the message plus its reply depth (0 = the thread root).
#[derive(Debug, Clone)]
pub struct ThreadEntry {
    pub filename: String,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub ts_ms: u64,
    /// Reply depth from the thread root (for `--tree` indentation).
    pub depth: usize,
}

/// Collect the whole thread that `filename` belongs to, across the catalog (a two-party conversation
/// lives in BOTH agents' boxes, so scan every agent's inbox+archive). Returns the thread in reply-tree
/// pre-order with depth; the root's `in-reply-to` chain is walked up first. Empty if `filename` isn't
/// found. Cycles/dangling `in-reply-to` are handled defensively.
pub fn collect_thread(catalog_root: &Path, filename: &str) -> anyhow::Result<Vec<ThreadEntry>> {
    // Gather every message once (dedup by filename — the same file can appear in two boxes).
    let mut all: HashMap<String, Message> = HashMap::new();
    let transition = crate::catalog_transaction::catalog_transition(catalog_root)?;
    for agent in addressable_agent_dirs(catalog_root, "", transition.as_ref())? {
        if let Some(capability) = agent.capability.as_ref() {
            for components in [&["resources", "inbox"][..], &["resources", "archive"][..]] {
                let Some(dir) = open_message_box(capability, components, false)? else {
                    continue;
                };
                for message in list_dir(&crate::catalog_transaction::retained_dir_path(&dir)?)
                    .unwrap_or_default()
                {
                    all.entry(message.filename.clone()).or_insert(message);
                }
            }
        } else {
            for dir in [inbox_dir(&agent.path), archive_dir(&agent.path)] {
                for message in list_dir(&dir).unwrap_or_default() {
                    all.entry(message.filename.clone()).or_insert(message);
                }
            }
        }
    }
    if !all.contains_key(filename) {
        return Ok(Vec::new());
    }

    // Walk up `in-reply-to` to the thread root.
    let mut root = filename.to_string();
    let mut seen = HashSet::new();
    while seen.insert(root.clone()) {
        match all.get(&root).and_then(|m| m.in_reply_to.clone()) {
            Some(parent) if all.contains_key(&parent) => root = parent,
            _ => break,
        }
    }

    // Children by parent, each sorted by send time.
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for (fname, m) in &all {
        if let Some(p) = &m.in_reply_to
            && all.contains_key(p)
        {
            children.entry(p.clone()).or_default().push(fname.clone());
        }
    }
    for kids in children.values_mut() {
        kids.sort_by_key(|f| all.get(f).map(|m| m.ts_ms).unwrap_or(0));
    }

    // Pre-order DFS from the root, iterative (stack of (filename, depth)); guard against cycles.
    let mut out = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![(root, 0usize)];
    while let Some((fname, depth)) = stack.pop() {
        if !visited.insert(fname.clone()) {
            continue;
        }
        if let Some(m) = all.get(&fname) {
            out.push(ThreadEntry {
                filename: m.filename.clone(),
                from: m.from.clone(),
                subject: m.subject.clone(),
                ts_ms: m.ts_ms,
                depth,
            });
        }
        // Push children in reverse so the earliest is processed first (stack = LIFO).
        if let Some(kids) = children.get(&fname) {
            for k in kids.iter().rev() {
                stack.push((k.clone(), depth + 1));
            }
        }
    }
    Ok(out)
}

/// One resolved delivery target, with the endpoint typing a durable record must persist.
enum DeliveryEndpoint {
    Agent(AddressableAgent),
    /// An eval-owned external requester mailbox. Its canonical endpoint is its route; it has no
    /// agent ID and never gets one.
    External {
        address: String,
        inbox: PathBuf,
        archive: PathBuf,
    },
    /// A legacy flat box under a proven catalog-less root, named by the literal selector.
    Flat {
        address: String,
        inbox: PathBuf,
        archive: PathBuf,
    },
}

impl DeliveryEndpoint {
    /// The canonical value a durable record persists: an immutable agent ID for an Agent, the
    /// canonical address for a typed non-Agent endpoint.
    fn endpoint(&self) -> &str {
        match self {
            Self::Agent(agent) => agent.agent_id(),
            Self::External { address, .. } | Self::Flat { address, .. } => address,
        }
    }

    fn kind(&self) -> EndpointKind {
        match self {
            Self::Agent(_) => EndpointKind::Agent,
            Self::External { .. } | Self::Flat { .. } => EndpointKind::External,
        }
    }

    /// The publication-time bus-address snapshot, for an Agent endpoint that is routable now.
    fn address_snapshot(&self) -> Option<String> {
        match self {
            Self::Agent(agent) => agent.bus_address(),
            Self::External { .. } | Self::Flat { .. } => None,
        }
    }

    fn boxes(&self) -> anyhow::Result<(PathBuf, PathBuf)> {
        match self {
            Self::Agent(agent) => {
                let root = match agent.capability.as_ref() {
                    Some(capability) => crate::catalog_transaction::retained_dir_path(capability)?,
                    None => agent.path.clone(),
                };
                Ok((inbox_dir(&root), archive_dir(&root)))
            }
            Self::External { inbox, archive, .. } | Self::Flat { inbox, archive, .. } => {
                Ok((inbox.clone(), archive.clone()))
            }
        }
    }
}

fn catalogless(root: &Path) -> bool {
    let discovered = crate::discover(root);
    crate::catalog_transaction::catalog_transition(root)
        .map(|transition| transition.is_none())
        .unwrap_or(false)
        && !root.join(crate::catalog_lock::CONTROL_DIR).exists()
        && discovered.specs.is_empty()
        && discovered.errors.is_empty()
}

/// Resolve a recipient named by an ordinary human reference.
///
/// `--to` is a route by design, so this is the one place inside the message layer that still
/// accepts free-form human input — and it resolves through the address book to an immutable ID
/// before anything durable is written.
fn resolve_delivery_endpoint(
    root: &Path,
    recipient: &AgentSelector,
    host: &str,
    external: Option<&ExternalInbox>,
) -> anyhow::Result<DeliveryEndpoint> {
    if let Some(agent) = resolve_agent_handle(root, recipient, host)? {
        return Ok(DeliveryEndpoint::Agent(agent));
    }
    let recipient = recipient.as_input();
    if let Some(external) = external
        && external.root == root
        && external.identity == recipient
        && external.inbox.is_dir()
    {
        return Ok(DeliveryEndpoint::External {
            address: external.identity.clone(),
            inbox: external.inbox.clone(),
            archive: sibling_archive_dir(&external.inbox),
        });
    }
    if catalogless(root) {
        return Ok(DeliveryEndpoint::Flat {
            address: recipient.to_string(),
            inbox: root.join(recipient).join("inbox"),
            archive: root.join(recipient).join("archive"),
        });
    }
    anyhow::bail!("no agent '{recipient}' found in catalog {}", root.display())
}

/// The acting sender, as a durable record must persist it.
struct SenderEndpoint {
    /// Immutable agent ID, or the canonical address of a typed non-Agent sender.
    endpoint: String,
    kind: EndpointKind,
    /// Publication-time bus address, for a routable Agent sender.
    address: Option<String>,
}

impl SenderEndpoint {
    /// What the rendered `from:` frontmatter line says: an Agent's human route when it has one,
    /// otherwise the canonical endpoint itself.
    fn rendered_from(&self) -> &str {
        self.address.as_deref().unwrap_or(&self.endpoint)
    }

    /// The `from-id:` line, written only by the version-2 writer and only for an Agent.
    fn rendered_from_id(&self) -> Option<&str> {
        (WRITE_MESSAGE_RECORD_VERSION_2 && self.kind == EndpointKind::Agent)
            .then_some(self.endpoint.as_str())
    }
}

/// Send to `recipient`, an ordinary human reference, on behalf of the exact immutable agent ID
/// `from`.
#[allow(clippy::too_many_arguments)]
pub fn send_to_resolved_inbox(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
    external: Option<&ExternalInbox>,
) -> anyhow::Result<String> {
    send_to_resolved_inbox_from(
        catalog_root,
        &AgentSelector::address(recipient),
        this_host,
        &AgentSelector::id(from),
        subject,
        in_reply_to,
        tags,
        body,
        idempotency_key,
        external,
    )
}

/// [`send_to_resolved_inbox`] where both endpoints are still typed selectors.
///
/// `recipient` is typed because a reply must address the original sender's *immutable ID* — an
/// ordinary address lookup would deliver to whoever holds those route bytes now.
///
/// Both endpoints are resolved HERE, and the recipient is resolved first. That ordering is the
/// `R27` fencing contract: the recipient lookup is the one that must observe a catalog generation
/// change and refuse a stale recipient. Resolving the acting sender earlier — at the CLI boundary,
/// say — moves the first fenced read off the recipient and lets an unrelated `--as` failure
/// short-circuit the send before the fence is ever crossed.
#[allow(clippy::too_many_arguments)]
pub fn send_to_resolved_inbox_from(
    catalog_root: &Path,
    recipient: &AgentSelector,
    this_host: &str,
    from: &AgentSelector,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
    external: Option<&ExternalInbox>,
) -> anyhow::Result<String> {
    if let Some(key) = idempotency_key {
        validate_idempotency_key(key)?;
    }
    // Unpinned: `message send` takes no `--host` for the recipient, so a fully qualified
    // `<host>.<address>` must reach a subject on ANY admitted logical host, exactly as the
    // positional path did. Pinning the sending host here would make every cross-host qualified
    // recipient unresolvable.
    let recipient = resolve_delivery_endpoint(catalog_root, recipient, this_host, external)?;
    let sender = resolve_agent_handle(catalog_root, from, this_host)?;
    let from = from.as_input();
    let external_sender =
        external.is_some_and(|external| external.root == catalog_root && external.identity == from);
    if matches!(&recipient, DeliveryEndpoint::External { .. }) || external_sender {
        anyhow::ensure!(
            idempotency_key.is_none(),
            "external requester messages do not own an ordinary sent-message index"
        );
        anyhow::ensure!(
            sender.is_some() || external_sender,
            "no agent '{from}' found in catalog {}",
            catalog_root.display()
        );
        let canonical_from = match sender.as_ref() {
            Some(agent) => SenderEndpoint {
                endpoint: agent.agent_id().to_owned(),
                kind: EndpointKind::Agent,
                address: agent.bus_address(),
            },
            None => SenderEndpoint {
                endpoint: from.to_owned(),
                kind: EndpointKind::External,
                address: None,
            },
        };
        let (inbox, _) = recipient.boxes()?;
        // This path owns no sender ledger, but it must still persist the same immutable sender
        // identity the ledger path renders: a reply keys on `from-id`, never on the route.
        return send_to_inbox_from_agent(
            &inbox,
            canonical_from.rendered_from(),
            canonical_from.rendered_from_id(),
            subject,
            in_reply_to,
            tags,
            body,
        );
    }
    let (canonical_from, sender_root) = match sender.as_ref() {
        Some(agent) => {
            let path = match agent.capability.as_ref() {
                Some(capability) => crate::catalog_transaction::retained_dir_path(capability)?,
                None => agent.path.clone(),
            };
            (
                SenderEndpoint {
                    endpoint: agent.agent_id().to_owned(),
                    kind: EndpointKind::Agent,
                    address: agent.bus_address(),
                },
                path,
            )
        }
        None if catalogless(catalog_root) => (
            SenderEndpoint {
                endpoint: from.to_string(),
                kind: EndpointKind::External,
                address: None,
            },
            catalog_root.join(from),
        ),
        None => anyhow::bail!(
            "no agent '{from}' found in catalog {}",
            catalog_root.display()
        ),
    };
    test_capability_checkpoint();
    send_with_ledger(
        catalog_root,
        this_host,
        external,
        &sender_root,
        &canonical_from,
        recipient,
        subject,
        in_reply_to,
        tags,
        body,
        idempotency_key,
    )
}

#[allow(clippy::too_many_arguments)]
fn send_with_ledger(
    catalog_root: &Path,
    this_host: &str,
    external: Option<&ExternalInbox>,
    sender_root: &Path,
    from: &SenderEndpoint,
    recipient: DeliveryEndpoint,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> anyhow::Result<String> {
    let root = sent_dir(sender_root);
    let _lock = SentLock::exclusive(&root)?;
    let mut head = ensure_sent_head(&root)?;
    validate_sent_tip(&root, &head)?;
    test_send_checkpoint("coverage")?;
    let recovered = recover_active(catalog_root, this_host, external, &root, &mut head)?;

    let filename = new_filename();
    let rendered_message = render_message_with_idempotency(
        from.rendered_from(),
        from.rendered_from_id(),
        subject,
        in_reply_to,
        tags,
        body,
        idempotency_key,
    );
    let parsed = parse_message(&filename, &rendered_message);
    // Every version decision in this module funnels through SENT_RECORD_WRITE_VERSION; the typed
    // endpoint and snapshot fields exist exactly when the record claims version 2.
    let version_2 = SENT_RECORD_WRITE_VERSION == MESSAGE_RECORD_VERSION_2;
    let candidate = SentRecord {
        version: SENT_RECORD_WRITE_VERSION,
        filename,
        ts: parsed.ts_ms,
        from: from.endpoint.clone(),
        to: recipient.endpoint().to_string(),
        subject: parsed.subject,
        in_reply_to: parsed.in_reply_to,
        tags: parsed.tags,
        priority: parsed.priority,
        idempotency_key: parsed.idempotency_key,
        body: parsed.body,
        rendered_message,
        from_address: version_2.then(|| from.address.clone()).flatten(),
        to_address: version_2.then(|| recipient.address_snapshot()).flatten(),
        from_kind: version_2.then_some(from.kind),
        to_kind: version_2.then_some(recipient.kind()),
    };
    if let Some(existing) = keyed_record(&root, &candidate)? {
        return Ok(existing.filename);
    }
    let mut matching = recovered
        .iter()
        .filter(|record| record.same_operation(&candidate));
    if let Some(existing) = matching.next() {
        anyhow::ensure!(
            matching.next().is_none(),
            "multiple recovered sends match one retry"
        );
        return Ok(existing.filename.clone());
    }

    let pending = root
        .join(SENT_PENDING)
        .join(pending_record_name(&candidate)?);
    anyhow::ensure!(
        atomic_create_file(&pending, &serde_json::to_vec(&candidate)?)?,
        "new send intent already exists"
    );
    test_send_checkpoint("pending")?;
    publish_active(&root, &candidate)?;
    test_send_checkpoint("active")?;
    deliver_record(&recipient, &candidate)?;
    test_send_checkpoint("recipient")?;
    publish_sent_record(&root, &candidate)?;
    test_send_checkpoint("row")?;
    let node = publish_sent_commit(&root, &head, &candidate)?;
    test_send_checkpoint("node")?;
    head.count = node.ordinal;
    head.tip = Some(digest_json(&node)?);
    write_sent_head(&root, &head)?;
    publish_key(&root, &candidate)?;
    test_send_checkpoint("head")?;
    fs::remove_file(&pending)?;
    test_send_checkpoint("pending-cleanup")?;
    fs::remove_file(root.join(SENT_ACTIVE))?;
    test_send_checkpoint("active-cleanup")?;
    Ok(candidate.filename)
}

fn ensure_sent_head(root: &Path) -> anyhow::Result<SentHead> {
    let path = root.join(SENT_HEAD);
    let candidate = SentHead {
        version: SENT_VERSION,
        since: now_ms(),
        count: 0,
        tip: None,
    };
    if atomic_create_file(&path, &serde_json::to_vec(&candidate)?)? {
        return Ok(candidate);
    }
    let head: SentHead = serde_json::from_slice(&fs::read(&path)?)?;
    validate_sent_head(&head)?;
    Ok(head)
}

fn validate_sent_head(head: &SentHead) -> anyhow::Result<()> {
    anyhow::ensure!(
        head.version == SENT_VERSION,
        "unsupported sent head version"
    );
    anyhow::ensure!(
        (head.count == 0) == head.tip.is_none(),
        "sent head count/tip mismatch"
    );
    if let Some(tip) = &head.tip {
        anyhow::ensure!(is_sha256(tip), "invalid sent head tip");
    }
    Ok(())
}

fn validate_sent_tip(root: &Path, head: &SentHead) -> anyhow::Result<()> {
    let Some(digest) = &head.tip else {
        return Ok(());
    };
    let node_path = root.join(SENT_COMMITS).join(format!("{digest}.json"));
    let node: SentCommit = serde_json::from_slice(&fs::read(node_path)?)?;
    anyhow::ensure!(
        node.version == SENT_VERSION,
        "unsupported sent commit version"
    );
    anyhow::ensure!(
        is_message_filename(&node.filename),
        "invalid sent commit filename"
    );
    anyhow::ensure!(
        digest_json(&node)? == *digest,
        "sent commit digest mismatch"
    );
    anyhow::ensure!(node.ordinal == head.count, "sent commit ordinal mismatch");
    let row_path = root
        .join(SENT_MESSAGES)
        .join(sent_record_name(&node.filename));
    let row: SentRecord = serde_json::from_slice(&fs::read(row_path)?)?;
    validate_sent_record(&row)?;
    anyhow::ensure!(
        row.filename == node.filename,
        "sent record filename does not match payload"
    );
    anyhow::ensure!(
        digest_json(&row)? == node.row_digest,
        "sent row digest mismatch"
    );
    Ok(())
}

fn recover_active(
    catalog_root: &Path,
    this_host: &str,
    external: Option<&ExternalInbox>,
    root: &Path,
    head: &mut SentHead,
) -> anyhow::Result<Vec<SentRecord>> {
    let pending = read_pending_records(&root.join(SENT_PENDING))?;
    anyhow::ensure!(pending.len() <= 1, "multiple pending sent intents");
    let active = read_sent_active(root)?;
    if let Some(active) = &active
        && head_tip_commits(root, head, &active.filename)?
    {
        let record = read_sent_record(root, &active.filename)
            .context("committed active intent has no sender row")?;
        anyhow::ensure!(
            digest_json(&record)? == active.record_digest,
            "committed active intent differs from sender row"
        );
        publish_key(root, &record)?;
        remove_if_exists(&root.join(SENT_PENDING).join(pending_record_name(&record)?))?;
        remove_if_exists(&root.join(SENT_ACTIVE))?;
        return Ok(Vec::new());
    }
    if active.is_none()
        && let [record] = pending.as_slice()
        && sent_record_exists(root, &record.filename)?
    {
        anyhow::ensure!(
            head_tip_commits_record(root, head, record)?,
            "committed pending intent is missing its active marker"
        );
        publish_key(root, record)?;
        remove_if_exists(&root.join(SENT_PENDING).join(pending_record_name(record)?))?;
        return Ok(Vec::new());
    }
    let record = match (active, pending.as_slice()) {
        (None, []) => return Ok(Vec::new()),
        (None, [record]) => {
            anyhow::ensure!(
                !sent_record_exists(root, &record.filename)?,
                "committed pending intent is missing its active marker"
            );
            publish_active(root, record)?;
            record.clone()
        }
        (Some(active), [record]) => {
            anyhow::ensure!(
                active.filename == record.filename && active.record_digest == digest_json(record)?,
                "active sent intent differs from pending record"
            );
            record.clone()
        }
        (Some(_), []) => anyhow::bail!("active sent intent has no recoverable pending record"),
        _ => unreachable!(),
    };
    // A persisted endpoint is canonical, never a route, so recovery re-resolves it by exact ID
    // rather than re-running address lookup: an address change between the crash and the retry
    // must not redirect the interrupted delivery.
    //
    // A version-1 endpoint is only an ID by construction while its legacy bytes were never
    // reassigned. If migration contested them, this row does not own them — a sender ledger row is
    // owned by its SENDER — so the bytes denote either the archived subject that wrote the intent
    // or the live subject that kept them. Delivering to the keeper would hand a stranger a message
    // it was never sent, so an unattributed endpoint refuses instead (`MESSAGE-R04`).
    let collisions = crate::catalog_migrate::load_legacy_id_collisions(catalog_root)
        .context("read legacy-id collision metadata for an interrupted send")?;
    let attribution = record
        .recipient_endpoint(&record.from)
        .attribute(&collisions);
    let target = attribution.reply_target().with_context(|| {
        format!(
            "pending recipient '{}' is a legacy bus identity migration reassigned; \
             this interrupted send cannot prove which subject it was addressed to",
            record.to
        )
    })?;
    let recipient = resolve_delivery_endpoint(
        catalog_root,
        &AgentSelector::id(target),
        this_host,
        external,
    )?;
    anyhow::ensure!(
        recipient.endpoint() == target,
        "pending recipient identity changed"
    );
    deliver_record(&recipient, &record)?;
    publish_sent_record(root, &record)?;
    let node = publish_sent_commit(root, head, &record)?;
    head.count = node.ordinal;
    head.tip = Some(digest_json(&node)?);
    write_sent_head(root, head)?;
    publish_key(root, &record)?;
    remove_if_exists(&root.join(SENT_PENDING).join(pending_record_name(&record)?))?;
    remove_if_exists(&root.join(SENT_ACTIVE))?;
    Ok(vec![record])
}

fn publish_active(root: &Path, record: &SentRecord) -> anyhow::Result<()> {
    let active = SentActive {
        version: SENT_VERSION,
        filename: record.filename.clone(),
        record_digest: digest_json(record)?,
    };
    let path = root.join(SENT_ACTIVE);
    let bytes = serde_json::to_vec(&active)?;
    if !atomic_create_file(&path, &bytes)? {
        anyhow::ensure!(fs::read(&path)? == bytes, "active sent intent collision");
    }
    Ok(())
}

fn pending_record_name(record: &SentRecord) -> anyhow::Result<String> {
    Ok(format!("{}.json", digest_json(record)?))
}

fn sent_commit(head: &SentHead, record: &SentRecord) -> anyhow::Result<SentCommit> {
    Ok(SentCommit {
        version: SENT_VERSION,
        ordinal: head
            .count
            .checked_add(1)
            .context("sent commit count overflow")?,
        previous: head.tip.clone(),
        filename: record.filename.clone(),
        row_digest: digest_json(record)?,
    })
}

fn publish_sent_commit(
    root: &Path,
    head: &SentHead,
    record: &SentRecord,
) -> anyhow::Result<SentCommit> {
    let node = sent_commit(head, record)?;
    let digest = digest_json(&node)?;
    let path = root.join(SENT_COMMITS).join(format!("{digest}.json"));
    let bytes = serde_json::to_vec(&node)?;
    if !atomic_create_file(&path, &bytes)? {
        anyhow::ensure!(fs::read(&path)? == bytes, "sent commit collision");
    }
    Ok(node)
}

fn publish_sent_record(root: &Path, record: &SentRecord) -> anyhow::Result<()> {
    let path = root
        .join(SENT_MESSAGES)
        .join(sent_record_name(&record.filename));
    let bytes = serde_json::to_vec(record)?;
    if !atomic_create_file(&path, &bytes)? {
        anyhow::ensure!(fs::read(&path)? == bytes, "sent record collision");
    }
    Ok(())
}

fn read_sent_record(root: &Path, filename: &str) -> anyhow::Result<SentRecord> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid sent record filename"
    );
    let path = root.join(SENT_MESSAGES).join(sent_record_name(filename));
    let record: SentRecord = serde_json::from_slice(&fs::read(&path)?)
        .with_context(|| format!("reading sent record {}", path.display()))?;
    validate_sent_record(&record)?;
    anyhow::ensure!(
        record.filename == filename,
        "sent record filename does not match payload"
    );
    Ok(record)
}

fn sent_record_exists(root: &Path, filename: &str) -> anyhow::Result<bool> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid sent record filename"
    );
    let path = root.join(SENT_MESSAGES).join(sent_record_name(filename));
    match fs::metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(metadata.is_file(), "sent record path is not a file");
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn deliver_record(recipient: &DeliveryEndpoint, record: &SentRecord) -> anyhow::Result<()> {
    let (inbox, archive) = recipient.boxes()?;
    let archived = archive.join(&record.filename);
    if archived.is_file() {
        let same = match fs::read_to_string(&archived) {
            Ok(content) => content == record.rendered_message,
            // A read failure is a failed delivery too: report it before propagating so
            // message_deliveries_total{result="fail"} covers filesystem errors, not just mismatches.
            Err(error) => {
                crate::metrics::record_message_delivery(true);
                return Err(error.into());
            }
        };
        if !same {
            crate::metrics::record_message_delivery(true);
            anyhow::bail!("archived message differs from pending send {}", record.filename);
        }
        crate::metrics::record_message_delivery(false);
        return Ok(());
    }
    match materialize_message_once(&inbox, &record.filename, &record.rendered_message) {
        Ok(_) => {
            crate::metrics::record_message_delivery(false);
            Ok(())
        }
        Err(error) => {
            crate::metrics::record_message_delivery(true);
            Err(error)
        }
    }
}

fn key_path(root: &Path, to: &str, key: &str) -> anyhow::Result<PathBuf> {
    Ok(root
        .join(SENT_KEYS)
        .join(format!("{}.json", digest_json(&(to, key))?)))
}

fn publish_key(root: &Path, record: &SentRecord) -> anyhow::Result<()> {
    let Some(key) = &record.idempotency_key else {
        return Ok(());
    };
    let receipt = SentKey {
        version: SENT_VERSION,
        to: record.to.clone(),
        key: key.clone(),
        filename: record.filename.clone(),
        record_digest: digest_json(record)?,
    };
    let path = key_path(root, &record.to, key)?;
    let bytes = serde_json::to_vec(&receipt)?;
    if !atomic_create_file(&path, &bytes)? {
        anyhow::ensure!(fs::read(&path)? == bytes, "sent idempotency-key collision");
    }
    Ok(())
}

fn keyed_record(root: &Path, candidate: &SentRecord) -> anyhow::Result<Option<SentRecord>> {
    let Some(key) = candidate.idempotency_key.as_deref() else {
        return Ok(None);
    };
    let path = key_path(root, &candidate.to, key)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let receipt: SentKey = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        receipt.version == SENT_VERSION,
        "unsupported sent key version"
    );
    anyhow::ensure!(
        receipt.to == candidate.to && receipt.key == key,
        "sent key scope mismatch"
    );
    anyhow::ensure!(
        is_message_filename(&receipt.filename),
        "invalid sent key filename"
    );
    let record_path = root
        .join(SENT_MESSAGES)
        .join(sent_record_name(&receipt.filename));
    let record: SentRecord = serde_json::from_slice(&fs::read(record_path)?)?;
    validate_sent_record(&record)?;
    anyhow::ensure!(
        record.filename == receipt.filename,
        "sent key record filename mismatch"
    );
    anyhow::ensure!(
        digest_json(&record)? == receipt.record_digest,
        "sent key record mismatch"
    );
    anyhow::ensure!(
        record.same_operation(candidate),
        "message idempotency key reused with different content"
    );
    Ok(Some(record))
}

fn head_tip_commit(root: &Path, head: &SentHead) -> anyhow::Result<Option<SentCommit>> {
    let Some(digest) = &head.tip else {
        return Ok(None);
    };
    let path = root.join(SENT_COMMITS).join(format!("{digest}.json"));
    let node: SentCommit = serde_json::from_slice(&fs::read(path)?)?;
    anyhow::ensure!(
        node.version == SENT_VERSION,
        "unsupported sent commit version"
    );
    anyhow::ensure!(
        is_message_filename(&node.filename),
        "invalid sent commit filename"
    );
    anyhow::ensure!(
        digest_json(&node)? == *digest,
        "sent commit digest mismatch"
    );
    anyhow::ensure!(node.ordinal == head.count, "sent commit ordinal mismatch");
    Ok(Some(node))
}

fn head_tip_commits(root: &Path, head: &SentHead, filename: &str) -> anyhow::Result<bool> {
    Ok(head_tip_commit(root, head)?.is_some_and(|node| node.filename == filename))
}

fn head_tip_commits_record(
    root: &Path,
    head: &SentHead,
    record: &SentRecord,
) -> anyhow::Result<bool> {
    let Some(node) = head_tip_commit(root, head)? else {
        return Ok(false);
    };
    if node.filename != record.filename {
        return Ok(false);
    }
    let pending_digest = digest_json(record)?;
    anyhow::ensure!(
        node.row_digest == pending_digest,
        "committed pending intent differs from head tip"
    );
    let committed = read_sent_record(root, &record.filename)
        .context("committed pending intent has no sender row")?;
    anyhow::ensure!(
        digest_json(&committed)? == pending_digest,
        "committed pending intent differs from sender row"
    );
    Ok(true)
}

fn digest_json(value: &impl Serialize) -> anyhow::Result<String> {
    let digest = Sha256::digest(serde_json::to_vec(value)?);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn write_sent_head(root: &Path, head: &SentHead) -> anyhow::Result<()> {
    validate_sent_head(head)?;
    atomic_replace_file(&root.join(SENT_HEAD), &serde_json::to_vec(head)?)
}

fn atomic_create_file(path: &Path, bytes: &[u8]) -> anyhow::Result<bool> {
    let parent = path.parent().context("atomic file has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(tmp_name());
    fs::write(&temporary, bytes)?;
    let result = match fs::hard_link(&temporary, path) {
        Ok(()) => Ok(true),
        Err(_) if path.is_file() => Ok(false),
        Err(error) => Err(error.into()),
    };
    let _ = fs::remove_file(temporary);
    result
}

fn atomic_replace_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().context("atomic file has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(tmp_name());
    fs::write(&temporary, bytes)?;
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_idempotency_key(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control),
        "message idempotency key must be non-empty single-line text without surrounding whitespace"
    );
    Ok(())
}

struct SentLock {
    file: Option<File>,
}

impl SentLock {
    fn shared(root: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(root)?;
        Self::acquire(root, libc::LOCK_SH)
    }

    fn exclusive(root: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(root)?;
        Self::acquire(root, libc::LOCK_EX)
    }

    fn shared_existing(root: &Path) -> anyhow::Result<Option<Self>> {
        use std::os::fd::AsRawFd as _;
        let file = match OpenOptions::new().read(true).open(root.join(SENT_LOCK)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH) };
        anyhow::ensure!(result == 0, "locking sent-message ledger failed");
        Ok(Some(Self { file: Some(file) }))
    }

    fn acquire(root: &Path, operation: libc::c_int) -> anyhow::Result<Self> {
        use std::os::fd::AsRawFd as _;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(SENT_LOCK))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
        anyhow::ensure!(result == 0, "locking sent-message ledger failed");
        Ok(Self { file: Some(file) })
    }
}

impl Drop for SentLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd as _;
        if let Some(file) = &self.file {
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

#[cfg(debug_assertions)]
fn test_send_checkpoint(point: &str) -> anyhow::Result<()> {
    if std::env::var("ST2_TEST_MESSAGE_SEND_FAIL_AFTER").as_deref() == Ok(point) {
        anyhow::bail!("injected message send failure after {point}");
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn test_send_checkpoint(_point: &str) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(debug_assertions)]
fn test_capability_checkpoint() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_MESSAGE_CAPABILITY_READY"),
        std::env::var("ST2_TEST_MESSAGE_CAPABILITY_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_capability_checkpoint() {}

pub fn archive_resolved_message(
    catalog_root: &Path,
    identity: &str,
    this_host: &str,
    filename: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid message filename {filename:?}"
    );
    let agent = match resolve_agent_handle(catalog_root, &AgentSelector::id(identity), this_host)? {
        Some(agent) => agent,
        None => {
            let discovered = crate::discover(catalog_root);
            if crate::catalog_transaction::catalog_transition(catalog_root)?.is_none()
                && !catalog_root.join(crate::catalog_lock::CONTROL_DIR).exists()
                && discovered.specs.is_empty()
                && discovered.errors.is_empty()
            {
                return archive_msg(
                    &catalog_root.join(identity).join("inbox"),
                    &catalog_root.join(identity).join("archive"),
                    filename,
                );
            }
            anyhow::bail!(
                "no agent '{identity}' found in catalog {}",
                catalog_root.display()
            )
        }
    };
    if let Some(capability) = agent.capability.as_ref() {
        let inbox = open_message_box(capability, &["resources", "inbox"], false)?
            .context("message inbox does not exist")?;
        let archive = open_message_box(capability, &["resources", "archive"], true)?
            .context("created archive capability is missing")?;
        archive_msg(
            &crate::catalog_transaction::retained_dir_path(&inbox)?,
            &crate::catalog_transaction::retained_dir_path(&archive)?,
            filename,
        )
    } else {
        archive_msg(&inbox_dir(&agent.path), &archive_dir(&agent.path), filename)
    }
}

fn open_message_box(
    agent: &File,
    components: &[&str],
    create: bool,
) -> anyhow::Result<Option<File>> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd as _;
    let mut current = agent.try_clone()?;
    for component in components {
        match crate::catalog_transaction::openat_dir_nofollow(
            &current,
            std::ffi::OsStr::new(component),
        ) {
            Ok(next) => current = next,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => {
                return Ok(None);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = CString::new(*component)?;
                let result = unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o755) };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(error.into());
                    }
                }
                current = crate::catalog_transaction::openat_dir_nofollow(
                    &current,
                    std::ffi::OsStr::new(component),
                )?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(current))
}

/// Archive `filename` from inbox to archive.
///
/// Idempotency matters on an eventually-consistent bus: if archive already contains the filename,
/// it is the durable receipt and wins. Remove only a duplicate inbox copy, never overwrite the
/// archived file. A source that disappeared concurrently is also success when the receipt exists.
pub fn archive_msg(inbox_dir: &Path, archive_dir: &Path, filename: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid message filename {filename:?}"
    );
    fs::create_dir_all(archive_dir)?;
    let source = inbox_dir.join(filename);
    let receipt = archive_dir.join(filename);

    if receipt.is_file() {
        return remove_inbox_duplicate(&source, filename);
    }

    // Link first, then unlink the source: unlike rename on Unix, hard-link creation never replaces a
    // destination that raced us into existence. The two boxes are siblings on one filesystem in
    // both supported layouts, so this gives archive-wins semantics without a clobber window.
    match fs::hard_link(&source, &receipt) {
        Ok(()) => remove_inbox_duplicate(&source, filename),
        // A sync writer may create the receipt between our check and link attempt. If so, preserve
        // that receipt and clean only the duplicate source.
        Err(_) if receipt.is_file() => remove_inbox_duplicate(&source, filename),
        Err(e) => Err(anyhow::anyhow!("archiving {filename}: {e}")),
    }
}

/// Settle every canonical message still present in an agent's inbox.
///
/// Retirement invokes this on every reconciliation pass. The archive receipt remains authoritative,
/// so replay after an interrupted pass or a sync-restored inbox duplicate is idempotent.
pub fn archive_inbox(agent_dir: &Path) -> anyhow::Result<()> {
    let inbox = inbox_dir(agent_dir);
    let archive = archive_dir(agent_dir);
    for message in list_inbox(&inbox)? {
        archive_msg(&inbox, &archive, &message.filename)?;
    }
    Ok(())
}

fn remove_inbox_duplicate(source: &Path, filename: &str) -> anyhow::Result<()> {
    match fs::remove_file(source) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::anyhow!(
            "removing archived inbox duplicate {filename}: {e}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_grammar() {
        assert!(is_message_filename("1784649988123-abc23z.md"));
        assert!(is_message_filename("1784649988123-abciou.md")); // full [0-9a-z] grammar, not just Crockford
        assert!(!is_message_filename("1784649988123-abc23z.txt")); // not .md
        assert!(!is_message_filename("123-abc23z.md")); // ts not 13 digits
        assert!(!is_message_filename("1784649988123-ABC23Z.md")); // uppercase not allowed
        assert!(!is_message_filename("1784649988123-abc2.md")); // rand not 6 chars
        assert!(!is_message_filename("notes.md")); // outside message
    }

    #[test]
    fn rand6_is_six_crockford_chars() {
        let r = rand6();
        assert_eq!(r.len(), 6);
        assert!(r.bytes().all(|b| CROCKFORD.contains(&b)));
    }

    #[test]
    fn render_then_parse_roundtrips() {
        let tags = vec!["urgent".to_string(), "review".to_string()];
        let content = render_message("alice", Some("hi"), Some("123-x.md"), &tags, "hello\nworld");
        let m = parse_message("1784649988123-abc23z.md", &content);
        assert_eq!(m.from.as_deref(), Some("alice"));
        assert_eq!(m.subject.as_deref(), Some("hi"));
        assert_eq!(m.in_reply_to.as_deref(), Some("123-x.md"));
        assert_eq!(m.tags, vec!["urgent", "review"]);
        assert_eq!(m.body.trim_end(), "hello\nworld");
        assert_eq!(m.ts_ms, 1784649988123);
    }

    #[test]
    fn missing_frontmatter_is_permissive() {
        let m = parse_message("1784649988123-abc23z.md", "just a body, no frontmatter");
        assert_eq!(m.from, None);
        assert_eq!(m.body, "just a body, no frontmatter");
    }

    #[test]
    fn a_message_removed_after_enumeration_is_skipped_not_parsed_as_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1784649988123-abc23z.md");
        fs::write(
            &path,
            render_message("alice", Some("work"), None, &[], "body"),
        )
        .unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(read_message_contents(&path).unwrap(), None);
    }

    #[test]
    fn reply_subject_prefixes_once() {
        assert_eq!(reply_subject(Some("hi")).as_deref(), Some("re: hi"));
        assert_eq!(reply_subject(Some("re: hi")).as_deref(), Some("re: hi")); // no double-prefix
        assert_eq!(reply_subject(Some("RE: hi")).as_deref(), Some("RE: hi")); // case-insensitive
        assert_eq!(reply_subject(None), None);
    }

    #[test]
    fn send_list_read_archive_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let archive = tmp.path().join("archive");

        let f1 = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "hi bob").unwrap();
        // Force f2 into a strictly later millisecond so the send-order assertion below is meaningful —
        // within a single ms the `<ms>-<rand6>` wire format can't recover send order (ties break on the
        // random suffix). See `list_dir`.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let f2 = send_to_inbox(&inbox, "carol", None, Some(&f1), &[], "re: first").unwrap();
        assert!(is_message_filename(&f1) && is_message_filename(&f2));

        let listed = list_dir(&inbox).unwrap();
        assert_eq!(listed.len(), 2);
        // sorted by time — f1 before f2
        assert_eq!(listed[0].filename, f1);
        assert_eq!(listed[0].from.as_deref(), Some("alice"));
        assert_eq!(listed[1].in_reply_to.as_deref(), Some(f1.as_str()));

        let read = read_msg(&inbox, &f1).unwrap();
        assert_eq!(read.body.trim_end(), "hi bob");

        archive_msg(&inbox, &archive, &f1).unwrap();
        assert_eq!(list_dir(&inbox).unwrap().len(), 1);
        assert_eq!(list_dir(&archive).unwrap().len(), 1);
        assert!(!inbox.join(&f1).exists());
    }

    #[test]
    fn archive_receipt_suppresses_and_idempotently_cleans_a_restored_inbox_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let archive = tmp.path().join("archive");
        let filename =
            send_to_inbox(&inbox, "alice", Some("once"), None, &[], "handle once").unwrap();

        archive_msg(&inbox, &archive, &filename).unwrap();
        let receipt = fs::read(archive.join(&filename)).unwrap();

        // Simulate a sync race restoring the old Present entry after the local archive.
        fs::create_dir_all(&inbox).unwrap();
        fs::write(inbox.join(&filename), b"stale inbox replica must not win").unwrap();
        assert_eq!(
            list_dir(&inbox).unwrap().len(),
            1,
            "the raw replica is present"
        );
        assert!(
            list_inbox(&inbox).unwrap().is_empty(),
            "the archive receipt suppresses it"
        );
        assert!(
            !inbox.join(&filename).exists(),
            "listing must also clean the shadowed inbox replica"
        );

        // Repeating archive is safe after listing already removed the duplicate.
        archive_msg(&inbox, &archive, &filename).unwrap();
        assert!(!inbox.join(&filename).exists());
        assert_eq!(fs::read(archive.join(&filename)).unwrap(), receipt);

        // Source already absent + receipt present is idempotent success too.
        archive_msg(&inbox, &archive, &filename).unwrap();
        assert_eq!(fs::read(archive.join(&filename)).unwrap(), receipt);
    }

    #[test]
    fn reserved_message_materialization_is_idempotent_but_never_clobbers() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let filename = "1784649988123-abc23z.md";
        let contents = render_message("service", Some("request"), None, &[], "{}\n");

        assert!(materialize_message_once(&inbox, filename, &contents).unwrap());
        assert!(!materialize_message_once(&inbox, filename, &contents).unwrap());
        let error = materialize_message_once(&inbox, filename, "different")
            .unwrap_err()
            .to_string();
        assert!(error.contains("collision with different bytes"));
        assert_eq!(fs::read_to_string(inbox.join(filename)).unwrap(), contents);
    }

    #[cfg(unix)]
    #[test]
    fn reserved_message_temporary_symlinks_are_never_followed() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        fs::create_dir_all(&inbox).unwrap();
        let victim = tmp.path().join("victim");
        fs::write(&victim, "must remain unchanged").unwrap();
        let start = TMP_COUNTER.load(Ordering::Relaxed);
        for counter in start..start + 4096 {
            symlink(
                &victim,
                inbox.join(format!(".message.tmp-{}-{counter}", std::process::id())),
            )
            .unwrap();
        }

        let error = materialize_message_once(&inbox, "1784649988123-symlnk.md", "must not escape")
            .unwrap_err();

        assert!(!error.to_string().is_empty());
        assert_eq!(fs::read_to_string(victim).unwrap(), "must remain unchanged");
    }

    #[test]
    fn resolve_inbox_falls_back_to_the_flat_bus_when_catalog_less() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // No catalog under root → the flat bus (<root>/<id>/inbox|archive).
        assert_eq!(
            resolve_inbox(root, "mix.sup", "h").unwrap(),
            root.join("mix.sup").join("inbox")
        );
        assert_eq!(
            resolve_archive(root, "mix.sup", "h").unwrap(),
            root.join("mix.sup").join("archive")
        );
        // A discoverable native catalog agent → its resources/inbox.
        let ad = root.join("h").join("mix.sup");
        std::fs::create_dir_all(&ad).unwrap();
        std::fs::write(
            ad.join("agent.kdl"),
            "agent \"mix.sup\" {\n  identity \"mix.sup\"\n  name \"Shared Worker\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" { command \"x\" }\n}\n",
        )
        .unwrap();
        // Inside a catalog the box is reached by the subject's exact immutable ID.
        assert_eq!(
            resolve_inbox(root, "h.mix.sup", "h").unwrap(),
            ad.join("resources").join("inbox")
        );
        // An exact-ID lookup never falls through to address lookup: the bare effective address
        // `mix.sup` names this subject as a route, and that is deliberately not an ID.
        assert!(resolve_inbox(root, "mix.sup", "h").is_err());
        assert_eq!(
            select_list_box(root, &AgentSelector::address("mix.sup"), "h", false, false).unwrap(),
            ad.join("resources").join("inbox")
        );
        assert!(resolve_inbox(root, "Shared Worker", "h").is_err());

        let external = ExternalInbox::new(root, "requester").unwrap();
        assert!(resolve_inbox_with_external(root, "requester", "h", Some(&external)).is_err());

        let requester = root.join("requester").join("inbox");
        std::fs::create_dir_all(&requester).unwrap();
        assert!(resolve_inbox(root, "requester", "h").is_err());
        assert_eq!(
            resolve_inbox_with_external(root, "requester", "h", Some(&external)).unwrap(),
            requester
        );
        assert!(resolve_inbox_with_external(root, "missing", "h", Some(&external)).is_err());
    }

    /// The external-mailbox path owns no sender ledger, but a reply keys on `from-id`: without it
    /// the delivered row carries only the mutable route, and for a subject whose ID differs from
    /// its address the reply cannot resolve at all.
    #[test]
    fn an_external_mailbox_delivery_carries_the_immutable_sender_id() {
        const SENDER_ID: &str = "0199c0de-0000-7000-8000-00000000dead";
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let directory = root.join("h").join("sender");
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("agent.kdl"),
            format!(
                "agent \"sender\" {{\n  identity \"sender\"\n  id {SENDER_ID:?}\n  address \"renamed\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" {{ command \"x\" }}\n}}\n"
            ),
        )
        .unwrap();
        let external = ExternalInbox::provision(root, "requester").unwrap();

        let filename = send_to_resolved_inbox_from(
            root,
            &AgentSelector::address("requester"),
            "h",
            &AgentSelector::id(SENDER_ID),
            Some("done"),
            None,
            &[],
            "body",
            None,
            Some(&external),
        )
        .unwrap();

        let delivered = fs::read_to_string(external.inbox.join(&filename)).unwrap();
        let parsed = parse_message(&filename, &delivered);
        assert_eq!(parsed.from.as_deref(), Some("h.renamed"));
        assert_eq!(
            parsed.from_id.as_deref(),
            Some(SENDER_ID),
            "the external mailbox row must carry the immutable sender ID: {delivered}"
        );
        assert_eq!(
            reply_target_in(root, &parsed, "requester")
                .unwrap()
                .as_deref(),
            Some(SENDER_ID),
            "a reply from that mailbox must address the sender's immutable ID"
        );
    }

    fn addressable_catalog(root: &Path) -> PathBuf {
        let agent = root.join("agents/host/worker");
        fs::create_dir_all(&agent).unwrap();
        fs::write(
            agent.join("agent.kdl"),
            "agent \"worker\" { identity \"worker\"; host \"host\"; command \"true\" }\n",
        )
        .unwrap();
        agent
    }

    #[test]
    fn transition_addressability_ignores_and_preserves_exact_legacy_staging_files() {
        let root = tempfile::tempdir().unwrap();
        addressable_catalog(root.path());
        let legacy = root
            .path()
            .join("agents/host/.harness-context.tmp-123-456");
        fs::write(&legacy, b"stale legacy staging bytes").unwrap();
        let transition = crate::catalog_transaction::CatalogTransition {
            original_agents: BTreeSet::new(),
        };

        let agents = addressable_agent_dirs(root.path(), "host", Some(&transition)).unwrap();

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id(), "host.worker");
        assert_eq!(agents[0].bus_address().as_deref(), Some("host.worker"));
        assert_eq!(
            fs::read(&legacy).unwrap(),
            b"stale legacy staging bytes",
            "address resolution must not clean another process's file"
        );
    }

    /// An incomplete apply fences the DECLARATION plane, not the state plane. A migrated subject
    /// must therefore stay reachable by its explicit `id` while the marker is up — deriving the
    /// subject from the positional `<host>/<identity>` key alone made every migrated agent
    /// unaddressable exactly when its state was still supposed to be writable.
    #[test]
    fn transition_addressability_honours_a_declared_id_and_address() {
        let root = tempfile::tempdir().unwrap();
        let agent = root.path().join("agents/host/worker");
        fs::create_dir_all(&agent).unwrap();
        fs::write(
            agent.join("agent.kdl"),
            "agent \"worker\" { identity \"worker\"; id \"worker-uuid\"; address \"chat\"; host \"host\"; command \"true\" }\n",
        )
        .unwrap();
        let transition = crate::catalog_transaction::CatalogTransition {
            original_agents: BTreeSet::new(),
        };

        let agents = addressable_agent_dirs(root.path(), "host", Some(&transition)).unwrap();

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id(), "worker-uuid");
        assert_eq!(agents[0].bus_address().as_deref(), Some("host.chat"));
    }

    /// With no readable declaration, retained state is all there is — and the positional key is
    /// exactly the identity migration would have frozen for that subject.
    #[test]
    fn transition_addressability_falls_back_to_the_positional_key_for_retained_state() {
        let root = tempfile::tempdir().unwrap();
        let agent = root.path().join("agents/host/worker");
        fs::create_dir_all(agent.join("resources")).unwrap();
        let key = crate::catalog_transaction::AgentKey {
            host: "host".to_owned(),
            identity: "worker".to_owned(),
        };
        let transition = crate::catalog_transaction::CatalogTransition {
            original_agents: BTreeSet::from([key]),
        };

        let agents = addressable_agent_dirs(root.path(), "host", Some(&transition)).unwrap();

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id(), "host.worker");
    }

    #[cfg(unix)]
    #[test]
    fn transition_addressability_rejects_legacy_type_confusion_and_near_misses() {
        use std::os::unix::fs::symlink;

        for kind in ["directory", "symlink", "near-miss"] {
            let root = tempfile::tempdir().unwrap();
            addressable_catalog(root.path());
            let host = root.path().join("agents/host");
            match kind {
                "directory" => {
                    fs::create_dir(host.join(".harness-context.tmp-123-456")).unwrap();
                }
                "symlink" => {
                    symlink(
                        host.join("worker"),
                        host.join(".harness-context.tmp-123-456"),
                    )
                    .unwrap();
                }
                "near-miss" => {
                    fs::write(host.join(".harness-context.tmp-123-nope"), b"stale").unwrap();
                }
                _ => unreachable!(),
            }
            let transition = crate::catalog_transaction::CatalogTransition {
                original_agents: BTreeSet::new(),
            };
            assert!(
                addressable_agent_dirs(root.path(), "host", Some(&transition)).is_err(),
                "{kind} must not enter the reserved compatibility exception"
            );
        }
    }


    #[test]
    fn external_inbox_rejects_unsafe_or_nested_identities() {
        let tmp = tempfile::tempdir().unwrap();
        for identity in [
            "",
            ".",
            "..",
            "nested/requester",
            "../requester",
            "/requester",
        ] {
            assert!(
                ExternalInbox::new(tmp.path(), identity).is_err(),
                "accepted unsafe external identity {identity:?}"
            );
        }
    }

    /// A minimal committed record. Version-1 shaped; the version-2 tests widen it explicitly.
    fn record(from: &str, to: &str) -> SentRecord {
        SentRecord {
            version: MESSAGE_RECORD_VERSION_1,
            filename: "1785000000000-abcdef.md".to_owned(),
            ts: 1_785_000_000_000,
            from: from.to_owned(),
            to: to.to_owned(),
            subject: None,
            in_reply_to: None,
            tags: Vec::new(),
            priority: None,
            idempotency_key: None,
            body: "hello\n".to_owned(),
            rendered_message: "---\nfrom: h.sender\n---\nhello\n".to_owned(),
            from_address: None,
            to_address: None,
            from_kind: None,
            to_kind: None,
        }
    }

    /// A version-2 Agent record keeps IDs in `from`/`to` and addresses only as snapshots, so an
    /// address change can never rewrite history or retarget a reply.
    #[test]
    fn a_version_2_agent_record_round_trips_its_id_and_address_snapshot() {
        let candidate = SentRecord {
            version: MESSAGE_RECORD_VERSION_2,
            from_address: Some("dev3.verifier".to_owned()),
            to_address: Some("dev4.chat".to_owned()),
            from_kind: Some(EndpointKind::Agent),
            to_kind: Some(EndpointKind::Agent),
            ..record(
                "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1",
                "0199b8f4-b48d-75c0-baa2-5e0fe2a1f8a3",
            )
        };
        validate_sent_record(&candidate).expect("a complete version-2 record is readable");

        let json = serde_json::to_value(&candidate).unwrap();
        assert_eq!(json["version"], 2);
        assert_eq!(json["from"], "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1");
        assert_eq!(json["fromAddress"], "dev3.verifier");
        assert_eq!(json["toAddress"], "dev4.chat");
        assert_eq!(json["toKind"], "agent");
        assert_eq!(
            serde_json::from_value::<SentRecord>(json).unwrap(),
            candidate
        );

        // The snapshot is display-only: it reaches the wire row but is never the row's `to`.
        let row = candidate.row(false);
        assert_eq!(row.to, "0199b8f4-b48d-75c0-baa2-5e0fe2a1f8a3");
        assert_eq!(row.to_address.as_deref(), Some("dev4.chat"));
        assert_eq!(row.to_kind, Some(EndpointKind::Agent));
    }

    /// A principal endpoint keeps its canonical address in `to` and says so, so no reader can
    /// mistake that address for an agent ID or hand it agent automation authority.
    #[test]
    fn a_typed_non_agent_endpoint_persists_its_kind_and_canonical_address() {
        let candidate = SentRecord {
            version: MESSAGE_RECORD_VERSION_2,
            from_kind: Some(EndpointKind::Agent),
            to_kind: Some(EndpointKind::Principal),
            from_address: Some("dev3.verifier".to_owned()),
            ..record("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", "dev3.billing")
        };
        validate_sent_record(&candidate).expect("a typed principal endpoint is readable");

        let attribution = candidate
            .recipient_endpoint("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1")
            .attribute(&NoLegacyCollisions);
        assert_eq!(
            attribution,
            EndpointAttribution::TypedAddress {
                kind: EndpointKind::Principal,
                address: "dev3.billing".to_owned(),
            }
        );
        assert_eq!(attribution.reply_target(), Some("dev3.billing"));
        // Replyable on its own transport, but never an agent-automation key.
        assert_eq!(attribution.agent_id(), None);
    }

    /// Reader-first: the same reader accepts both shapes. This is the invariant that lets version-2
    /// writers be enabled later without a flag day.
    #[test]
    fn the_sent_reader_accepts_both_version_1_and_version_2_records() {
        let tmp = tempfile::tempdir().unwrap();
        let directory = tmp.path().join(SENT_MESSAGES);
        fs::create_dir_all(&directory).unwrap();

        let legacy = record("h.sender", "h.recipient");
        let modern = SentRecord {
            version: MESSAGE_RECORD_VERSION_2,
            filename: "1785000000001-bcdefg.md".to_owned(),
            from_kind: Some(EndpointKind::Agent),
            to_kind: Some(EndpointKind::Agent),
            to_address: Some("h.recipient".to_owned()),
            ..record("h.sender", "0199b8f4-b48d-75c0-baa2-5e0fe2a1f8a3")
        };
        for candidate in [&legacy, &modern] {
            fs::write(
                directory.join(sent_record_name(&candidate.filename)),
                serde_json::to_vec(candidate).unwrap(),
            )
            .unwrap();
        }

        let read = read_sent_records(&directory).expect("both versions are readable");
        assert_eq!(read, vec![legacy, modern]);
    }

    /// A record that claims version 1 but carries version-2 endpoint fields has undecidable
    /// endpoint meaning. That is corruption, not a tolerable older shape.
    #[test]
    fn a_version_1_record_carrying_version_2_fields_fails_closed() {
        let mixed = SentRecord {
            to_kind: Some(EndpointKind::Principal),
            ..record("h.sender", "h.recipient")
        };
        assert!(validate_sent_record(&mixed).is_err());

        let incomplete = SentRecord {
            version: MESSAGE_RECORD_VERSION_2,
            from_kind: Some(EndpointKind::Agent),
            ..record("h.sender", "h.recipient")
        };
        assert!(validate_sent_record(&incomplete).is_err());
    }

    /// Every legacy endpoint in this index was reassigned at migration.
    struct AllReassigned;

    impl LegacyCollisionIndex for AllReassigned {
        fn is_reassigned(&self, _legacy_bus_identity: &str) -> bool {
            true
        }
    }

    /// The `MESSAGE-R04` collision rule, both directions.
    ///
    /// `h.worker` was contested: a live subject kept those bytes and an archived subject was given
    /// a UUID. A version-1 row in the ARCHIVED subject's own ledger says `from: h.worker`, and the
    /// row's owner is that archived subject — so it must attribute to the archived subject's
    /// migrated ID, never to the live keeper that now answers to those bytes.
    #[test]
    fn a_colliding_legacy_endpoint_attributes_only_to_the_rows_own_state_owner() {
        let archived = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";
        let row = record("h.worker", "h.peer");

        assert_eq!(
            row.sender_endpoint(archived).attribute(&AllReassigned),
            EndpointAttribution::AgentId(archived.to_owned()),
            "an archived sender's own row must not be reattributed to the live keeper"
        );

        // Without a recorded collision the bytes ARE the frozen ID, so nothing is reinterpreted.
        assert_eq!(
            row.sender_endpoint(archived).attribute(&NoLegacyCollisions),
            EndpointAttribution::AgentId("h.worker".to_owned())
        );
    }

    /// The recipient of a sender-owned row is not that row's state owner, so a colliding recipient
    /// endpoint is unattributed: a historical address with no reply or automation authority.
    #[test]
    fn a_non_owner_colliding_endpoint_is_unattributed_and_carries_no_reply_authority() {
        let owner = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";
        let row = record("h.worker", "h.peer");

        let attribution = row.recipient_endpoint(owner).attribute(&AllReassigned);
        assert_eq!(
            attribution,
            EndpointAttribution::UnattributedLegacyAddress("h.peer".to_owned())
        );
        assert_eq!(
            attribution.reply_target(),
            None,
            "addressing the live replacement would deliver to the wrong subject"
        );
        assert_eq!(attribution.agent_id(), None);
        assert_eq!(attribution.display(), "h.peer (historical address)");
        assert_ne!(
            attribution.display(),
            owner,
            "the keeping subject's ID must never stand in for archived bytes"
        );
    }

    /// A version-2 Agent endpoint is already an immutable ID, so collision metadata about legacy
    /// bytes must not touch it even when those bytes happen to look contested.
    #[test]
    fn a_version_2_agent_endpoint_is_never_reinterpreted_by_collision_metadata() {
        let owner = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";
        let row = SentRecord {
            version: MESSAGE_RECORD_VERSION_2,
            from_kind: Some(EndpointKind::Agent),
            to_kind: Some(EndpointKind::Agent),
            ..record("h.worker", "h.peer")
        };
        assert_eq!(
            row.recipient_endpoint(owner).attribute(&AllReassigned),
            EndpointAttribution::AgentId("h.peer".to_owned())
        );
    }

    /// Decision 1: a record whose endpoints are immutable IDs must DECLARE version 2. The tree was
    /// briefly incoherent — IDs stored under a version whose `from`/`to` mean bus identities — and
    /// that is exactly the misattribution the schema version exists to prevent.
    #[test]
    fn the_writer_never_stores_an_id_under_a_version_1_schema() {
        assert!(
            WRITE_MESSAGE_RECORD_VERSION_2,
            "the version-2 writer cohort is on; see DELTA-003 activation step 5"
        );
        assert_eq!(SENT_RECORD_WRITE_VERSION, MESSAGE_RECORD_VERSION_2);

        // Whatever the switch says, the two must agree: version 2 iff typed endpoints are written.
        let sender = SenderEndpoint {
            endpoint: "sender-uuid".to_owned(),
            kind: EndpointKind::Agent,
            address: Some("h.sender".to_owned()),
        };
        assert_eq!(sender.rendered_from(), "h.sender");
        assert_eq!(
            sender.rendered_from_id(),
            Some("sender-uuid"),
            "an Agent sender must publish its immutable ID beside its mutable route"
        );

        let rendered = render_message_with_idempotency(
            sender.rendered_from(),
            sender.rendered_from_id(),
            None,
            None,
            &[],
            "body",
            None,
        );
        assert!(rendered.contains("from: h.sender\n"), "{rendered}");
        assert!(rendered.contains("from-id: sender-uuid\n"), "{rendered}");

        // A principal or external sender has no agent ID, so it publishes no `from-id` line.
        let principal = SenderEndpoint {
            endpoint: "h.billing".to_owned(),
            kind: EndpointKind::External,
            address: None,
        };
        assert_eq!(principal.rendered_from(), "h.billing");
        assert_eq!(principal.rendered_from_id(), None);
    }

    /// Decision 3: a reply targets the authoritative `from_id`, never the mutable `from` route.
    /// A released address is immediately reusable, so replying by route can hand the reply to a
    /// completely different subject.
    #[test]
    fn a_reply_targets_the_immutable_sender_id_not_the_reusable_route() {
        let message = Message {
            filename: "1785000000000-abcdef.md".to_owned(),
            ts_ms: 1_785_000_000_000,
            from: Some("h.chat".to_owned()),
            from_id: Some("sender-uuid".to_owned()),
            subject: None,
            in_reply_to: None,
            tags: Vec::new(),
            priority: None,
            idempotency_key: None,
            stream: None,
            event_id: None,
            event_key: None,
            body: String::new(),
        };
        assert_eq!(
            message.reply_target("recipient-uuid", &NoLegacyCollisions),
            Some("sender-uuid".to_owned()),
            "the route `h.chat` may already belong to someone else"
        );
        // Collision metadata about legacy bytes must not touch an endpoint that is already an ID.
        assert_eq!(
            message.reply_target("recipient-uuid", &AllReassigned),
            Some("sender-uuid".to_owned())
        );
    }

    /// A pre-`from_id` message falls back to the collision-aware version-1 path, and an inbox row
    /// is owned by its RECIPIENT — so a colliding sender endpoint carries no reply authority.
    #[test]
    fn a_legacy_message_replies_by_collision_aware_attribution_and_refuses_when_unattributed() {
        let legacy = Message {
            filename: "1785000000000-abcdef.md".to_owned(),
            ts_ms: 1_785_000_000_000,
            from: Some("h.worker".to_owned()),
            from_id: None,
            subject: None,
            in_reply_to: None,
            tags: Vec::new(),
            priority: None,
            idempotency_key: None,
            stream: None,
            event_id: None,
            event_key: None,
            body: String::new(),
        };
        assert_eq!(
            legacy.reply_target("recipient-uuid", &NoLegacyCollisions),
            Some("h.worker".to_owned()),
            "uncontested legacy bytes ARE the sender's frozen agent ID"
        );
        assert_eq!(
            legacy.reply_target("recipient-uuid", &AllReassigned),
            None,
            "replying would deliver to whichever subject kept the reassigned bytes"
        );
    }

    /// Interrupted-send recovery must not treat a collided version-1 recipient as an exact ID: the
    /// keeper of those bytes is a different subject from the one the send was addressed to.
    #[test]
    fn recovery_refuses_a_pending_send_whose_legacy_recipient_was_reassigned() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let keeper = catalog.join("h/worker");
        fs::create_dir_all(&keeper).unwrap();
        fs::write(
            keeper.join("agent.kdl"),
            "agent \"worker\" {\n  identity \"worker\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" { command \"x\" }\n}\n",
        )
        .unwrap();
        // Migration reassigned `h.worker`: this live subject kept the bytes, an archived subject
        // lost them. The pending intent below was written before that split.
        let control = crate::catalog_migrate::legacy_id_collisions_path(&catalog);
        fs::create_dir_all(control.parent().unwrap()).unwrap();
        fs::write(
            &control,
            serde_json::to_vec(&crate::catalog_migrate::LegacyIdCollisions {
                schema: "st2.catalog-legacy-id-collisions.v1".to_owned(),
                entries: vec![crate::catalog_migrate::LegacyIdCollision {
                    legacy_bus_identity: "h.worker".to_owned(),
                    keeper: AgentId::parse("h.worker").unwrap(),
                    reassigned: vec![AgentId::parse("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1").unwrap()],
                }],
            })
            .unwrap(),
        )
        .unwrap();

        let sender_root = tmp.path().join("sender");
        let root = sent_dir(&sender_root);
        let pending = record("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", "h.worker");
        fs::create_dir_all(root.join(SENT_PENDING)).unwrap();
        fs::write(
            root.join(SENT_PENDING).join(pending_record_name(&pending).unwrap()),
            serde_json::to_vec(&pending).unwrap(),
        )
        .unwrap();
        publish_active(&root, &pending).unwrap();
        let mut head = ensure_sent_head(&root).unwrap();

        let error = recover_active(&catalog, "h", None, &root, &mut head)
            .expect_err("a reassigned legacy recipient cannot be resolved to one subject");
        assert!(
            format!("{error:#}").contains("reassigned"),
            "unexpected refusal: {error:#}"
        );
        assert!(
            !message_dir_has_entries(&inbox_dir(&keeper)),
            "the keeper of the legacy bytes must not receive a message addressed to another subject"
        );
    }

    fn message_dir_has_entries(dir: &Path) -> bool {
        fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
    }
}
