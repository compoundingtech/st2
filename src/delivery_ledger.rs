//! Canonical durable state for native message delivery.
//!
//! The ledger records one monotone evidence chain per inbox filename. Drivers translate their
//! harness-specific observations into [`Evidence`]; this module owns persistence, phase grading,
//! claim authorization, and binding isolation. Inbox archive remains the recipient's settlement
//! authority and is reconciled through [`Ledger::prune`].
//!
//! # The transaction boundary
//!
//! [`Ledger`] holds **no cached record**. It is immutable configuration — path, lock path,
//! profile, agent, runtime id, and the transport's own correlation derivation — and every
//! operation is a short transaction that
//!
//! 1. takes the permanent sibling `delivery-ledger.lock` (`O_NOFOLLOW`, `0600`, proven regular),
//! 2. re-reads and re-validates the **exact current bytes**, so no decision is ever made against
//!    a record another process has since replaced,
//! 3. mutates in memory, publishes atomically, and releases.
//!
//! Recovery and the Q35 backfill run inside that same boundary, so a first read cannot race a
//! concurrent first read into two translations of one legacy record.
//!
//! # Authorization is a written act
//!
//! There is no split "may I retry?" / "begin" pair any more: the pair had a window between the
//! question and the answer, and two readers could both be told yes. [`Ledger::claim`] is one
//! atomic act that authorizes, mints a fresh [`AttemptToken`] from OS randomness, durably writes
//! `Attempted`, and returns **at most one** [`Permit`]. Every later positive or negative
//! operation names the exact binding, filename, correlation, and token it believes it is talking
//! about, so an operation whose attempt was pruned, retargeted, or re-claimed lands on nothing
//! ([`Landed::Stale`]) instead of landing on its successor.
//!
//! One phase can reach this ledger without this build observing anything: an attempt an earlier
//! release made and left behind. Such a phase holds exactly as far as a phase holds — no evidence
//! policy releases `Attempted`, so nothing is re-sent — and [`Attestation`] records that this
//! build never watched it, which is what makes the leftover countable and therefore removable.
//! The translation itself lives outside this module, behind the one seam in the transaction
//! loader's "no ledger file" arm.
//!
//! # Observation never repairs
//!
//! [`observe`] is the read-only surface the roster and the CLI use. It takes the *shared* side of
//! the transaction lock, so it cannot tear a concurrent mutation, and it writes nothing: it
//! reports an absent record, a readable one, or bytes this build cannot interpret, and a tokenless
//! canonical v1 row is *reported* rather than backfilled. A diagnostic that repairs what it
//! measures cannot be trusted to measure it.

use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};

use crate::flock::{self, FileLock};
use crate::message;

pub(crate) const LEDGER_SCHEMA: &str = "st2.delivery-ledger.v1";
pub(crate) const LEDGER_FILE: &str = "delivery-ledger.json";

/// The permanent sibling that serializes recovery and every mutation.
///
/// Permanent on purpose: a lock file's only content is its inode, and removing it would split the
/// lock domain while a live process still holds the old inode open.
pub(crate) const LEDGER_LOCK: &str = "delivery-ledger.lock";
pub(crate) const LEDGER_STAGING_PREFIX: &str = ".delivery-ledger.tmp-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Harness {
    Claude,
    Codex,
    Pi,
    OpenCode,
    Omp,
}

impl Harness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Pi => "pi",
            Self::OpenCode => "opencode",
            Self::Omp => "omp",
        }
    }

    /// Parse one of the five harness names.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "claude" => Ok(Self::Claude),
            "codex" => Ok(Self::Codex),
            "pi" => Ok(Self::Pi),
            "opencode" => Ok(Self::OpenCode),
            "omp" => Ok(Self::Omp),
            other => anyhow::bail!("unknown native delivery harness '{other}'"),
        }
    }

    pub const fn policy(self) -> EvidencePolicy {
        match self {
            Self::Claude | Self::Pi | Self::Omp => EvidencePolicy::AttemptOnly,
            Self::Codex => EvidencePolicy::CodexReceipts,
            Self::OpenCode => EvidencePolicy::OpenCodeReceipts,
        }
    }

    pub const fn profile(self) -> Profile {
        Profile::new(self, self.policy())
    }
}

/// The evidence vocabulary and settlement threshold a harness can honestly observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidencePolicy {
    AttemptOnly,
    CodexReceipts,
    OpenCodeReceipts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    harness: Harness,
    policy: EvidencePolicy,
}

impl Profile {
    pub const fn new(harness: Harness, policy: EvidencePolicy) -> Self {
        Self { harness, policy }
    }

    fn graded(self, evidence: Evidence) -> Result<Phase> {
        let phase = match evidence {
            Evidence::TransportAccepted => Phase::TransportAccepted,
            Evidence::Persisted => Phase::Persisted,
            Evidence::Consumed => Phase::Consumed,
        };
        anyhow::ensure!(
            self.proves(phase),
            "{} delivery evidence cannot prove phase {phase:?}",
            self.harness.as_str()
        );
        Ok(phase)
    }

    /// Whether this evidence policy has a concrete observation for `phase`.
    fn proves(self, phase: Phase) -> bool {
        match self.policy {
            EvidencePolicy::AttemptOnly => matches!(phase, Phase::Attempted),
            EvidencePolicy::CodexReceipts => matches!(
                phase,
                Phase::Attempted | Phase::TransportAccepted | Phase::Consumed
            ),
            EvidencePolicy::OpenCodeReceipts => matches!(
                phase,
                Phase::Attempted | Phase::TransportAccepted | Phase::Persisted
            ),
        }
    }

    fn releases(self, phase: Phase) -> bool {
        matches!(self.policy, EvidencePolicy::CodexReceipts) && phase >= Phase::Consumed
    }
}

/// Durable delivery evidence. Declaration order is the monotone lattice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Phase {
    /// Persisted before the transport call.
    Attempted,
    /// The transport call returned success.
    TransportAccepted,
    /// The harness durably stores the exact correlated message.
    Persisted,
    /// The model received the message.
    Consumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Correlation {
    pub value: String,
}

impl Correlation {
    pub fn native(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegativeReceipt {
    /// The harness authoritatively does not hold the correlated message.
    Absent,
    /// The transport refused this attempt.
    Rejected,
}

/// Positive evidence translated by a harness driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    TransportAccepted,
    Persisted,
    Consumed,
}

/// Whether this build observed the evidence behind a phase, or another authority asserted it.
///
/// This is provenance, not authority: no [`Retention`], [`Authorization`] or transport decision
/// reads it. What holds a carried-forward attempt is its [`Phase`] measured against the harness
/// [`Profile`] — `Profile::releases` releases only a phase that harness can actually prove, and
/// the transaction loader refuses an asserted phase the profile cannot prove at all. The field
/// exists so the fleet can *see* an unobserved phase: it is clause 2 of DELTA-006's Resolution
/// Signal (`st2 doctor` counts it per seat), and it is deleted with the record boundary that
/// produces it. Nothing here names the authority: any party that can bound an attempt this build
/// never watched asserts, and the boundary in `crate::migrations::delivery_state` is one such
/// party.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Attestation {
    /// This build graded the evidence that set the phase through [`Profile::graded`].
    Observed,
    /// Another authority asserted the phase: this build never watched the attempt it describes.
    Asserted,
}

// ---- attempt tokens ---------------------------------------------------------------------------

/// A 128-bit fence minted for exactly one permitted attempt.
///
/// Fresh randomness per permit is the whole point: a token derived from the delivery identity
/// would be *equal* across re-claims and would therefore fence nothing. Because the token rotates
/// on every claim, an operation still carrying the previous one is provably talking about an
/// attempt that no longer exists, and it lands on nothing rather than on its successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttemptToken([u8; 16]);

impl AttemptToken {
    /// Mint from OS randomness, fail-closed.
    ///
    /// A token this call could not read is an error, never a weaker fallback: a predictable fence
    /// is worse than no delivery, because it silently stops fencing while still looking like one.
    pub fn mint() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut bytes))
            .context("minting a delivery attempt token from /dev/urandom")?;
        Ok(Self(bytes))
    }

    /// The deterministic token a tokenless canonical v1 row is backfilled with — the Q35
    /// exception, and the only token in this module that is not random.
    ///
    /// Derived from the row's own exact immutable bytes, so two processes that read the same
    /// durable row derive the *same* fence and neither can fence the other out of a record they
    /// both legitimately hold. Randomness here would do the opposite: whichever process wrote
    /// last would invalidate the other's in-flight fence for an attempt nobody re-made.
    fn derived(row: &Row) -> Result<Self> {
        debug_assert!(
            row.attempt_token.is_none(),
            "a derived token is only for a tokenless row"
        );
        let bytes = serde_json::to_vec(row)
            .context("hashing a tokenless delivery ledger entry for its derived token")?;
        let mut hash = Sha256::new();
        hash.update(b"st2.delivery-attempt-token.derived.v1");
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(&bytes);
        let digest = hash.finalize();
        let mut token = [0_u8; 16];
        token.copy_from_slice(&digest[..16]);
        Ok(Self(token))
    }

    /// Exactly 32 LOWERCASE hex characters, or an error.
    ///
    /// Uppercase is refused rather than normalized: the token is a canonical spelling that
    /// appears verbatim in the ledger bytes and in the delivery marker, and a parser that
    /// accepted two spellings of one fence would make byte comparison and hex comparison
    /// disagree.
    pub fn parse(text: &str) -> Result<Self> {
        let mut bytes = [0_u8; 16];
        decode_lowercase_hex(text, &mut bytes)
            .context("delivery attempt token is not 32 lowercase hex characters")?;
        Ok(Self(bytes))
    }

    pub fn hex(self) -> String {
        hex_of(&self.0)
    }
}

impl fmt::Display for AttemptToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.hex())
    }
}

impl Serialize for AttemptToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.hex())
    }
}

impl<'de> Deserialize<'de> for AttemptToken {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// SHA-256 of the exact raw ledger bytes a transaction or observation acted on.
///
/// Exact raw bytes, not a digest of the decoded record: a reader joining ledger state to some
/// other record needs to say "these are the bytes I saw", and a digest over a re-serialization
/// would compare equal across byte-different files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LedgerDigest([u8; 32]);

impl LedgerDigest {
    /// The digest of exact raw ledger bytes.
    ///
    /// Public so a caller that has read the file — an operator command taking its own
    /// precondition, a test fixture — never hand-rolls SHA-256 beside this module and never
    /// disagrees with it about what is hashed.
    pub fn of(bytes: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(bytes);
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&hash.finalize());
        Self(digest)
    }

    /// Exactly 64 lowercase hex characters, or an error — the CLI's strict reader for a digest
    /// an operator or another record hands back.
    pub fn parse(text: &str) -> Result<Self> {
        let mut digest = [0_u8; 32];
        decode_lowercase_hex(text, &mut digest)
            .context("delivery ledger digest is not 64 lowercase hex characters")?;
        Ok(Self(digest))
    }

    pub fn hex(self) -> String {
        hex_of(&self.0)
    }
}

/// Decode `text` into exactly `out.len()` bytes, refusing anything but canonical lowercase hex.
fn decode_lowercase_hex(text: &str, out: &mut [u8]) -> Result<()> {
    anyhow::ensure!(
        text.len() == out.len() * 2
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected {} lowercase hex characters",
        out.len() * 2
    );
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)?;
    }
    Ok(())
}

fn hex_of(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

impl fmt::Display for LedgerDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.hex())
    }
}

impl Serialize for LedgerDigest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.hex())
    }
}

impl<'de> Deserialize<'de> for LedgerDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

// ---- the wire record --------------------------------------------------------------------------

/// One durable row. Private: the module's public attempt type is [`Attempt`], which cannot be
/// built without a token, so no caller can hold a half-fenced attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Row {
    filename: String,
    binding: String,
    correlation: Correlation,
    /// Absent only on a canonical v1 row written before attempt tokens existed. The transaction
    /// loader backfills a deterministic token and persists it before anything else may act on the
    /// row; every *new* mutation mints fresh randomness instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attempt_token: Option<AttemptToken>,
    phase: Phase,
    /// Whether `phase` is this build's own grading or a claim it accepted from elsewhere.
    attestation: Attestation,
    /// The runtime incarnation that made the attempt. Live evidence acknowledges only its own
    /// incarnation; history reconciliation settles attempts from earlier incarnations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    negative: Option<NegativeReceipt>,
    /// Who refused this attempt out of band, and on exactly what evidence. Present only where an
    /// operator, rather than the transport, produced the negative receipt beside it. Last on the
    /// wire so an entry no operator touched is byte-identical to one from before this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operator_audit: Option<OperatorAudit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    schema: String,
    harness: String,
    agent: String,
    runtime_id: String,
    entries: Vec<Row>,
}

/// An attempt another authority asserted, carried forward for the loader to tokenize.
///
/// The only shape the recovery seam may produce, and it carries no token deliberately: what holds
/// the delivery is the phase — no evidence policy RELEASES `Attempted`, so a carried-forward
/// attempt suppresses a duplicate while authorizing nothing — and the token is minted
/// deterministically under the transaction lock so a translation cannot choose randomness for a
/// record this build never watched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Carried(Row);

impl Carried {
    fn into_row(self) -> Row {
        self.0
    }
}

/// Build an attempt whose phase another authority asserted rather than this build observing it.
///
/// The phase it carries is still checked against the harness [`Profile`] by the transaction
/// loader, so a claim of evidence the harness cannot produce fails closed instead of being
/// written, and the phase itself is what keeps the entry holding: an [`Attestation::Asserted`]
/// entry suppresses a duplicate exactly as far as its phase does, and authorizes a claim only
/// where the profile proves that phase.
pub fn asserted(
    filename: String,
    binding: String,
    correlation: Correlation,
    phase: Phase,
    incarnation: Option<String>,
) -> Carried {
    Carried(Row {
        filename,
        binding,
        correlation,
        attempt_token: None,
        phase,
        attestation: Attestation::Asserted,
        incarnation,
        negative: None,
        operator_audit: None,
    })
}

// ---- typed views ------------------------------------------------------------------------------

/// A validated attempt: a durable row that is guaranteed to carry its fencing token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub filename: String,
    pub binding: String,
    pub correlation: Correlation,
    pub token: AttemptToken,
    pub phase: Phase,
    pub attestation: Attestation,
    pub incarnation: Option<String>,
    pub negative: Option<NegativeReceipt>,
    /// Present only where an operator, rather than the transport, produced `negative`.
    pub audit: Option<OperatorAudit>,
}

impl Attempt {
    fn of(row: &Row) -> Result<Self> {
        Ok(Self {
            filename: row.filename.clone(),
            binding: row.binding.clone(),
            correlation: row.correlation.clone(),
            token: row
                .attempt_token
                .context("delivery ledger entry has no attempt token after backfill")?,
            phase: row.phase,
            attestation: row.attestation,
            incarnation: row.incarnation.clone(),
            negative: row.negative,
            audit: row.operator_audit.clone(),
        })
    }

    /// The exact identity a later operation must name to act on this attempt.
    pub fn fence(&self) -> Fence {
        Fence {
            filename: self.filename.clone(),
            binding: self.binding.clone(),
            correlation: self.correlation.clone(),
            token: self.token,
        }
    }
}

// ---- the operator boundary --------------------------------------------------------------------

/// The most a bounded operator reason may carry into the record.
///
/// A record is not a log: the reason exists so a later reader knows why an attempt was refused,
/// and an unbounded string here would let one refusal grow the ledger without limit.
pub const OPERATOR_REASON_MAX_BYTES: usize = 200;

/// Which authority outside the transport produced a negative receipt.
///
/// One variant today. It is an enum rather than an implied "operator" because the axis is what a
/// reader needs — a later non-operator authority must be distinguishable in the record rather
/// than inferred from the absence of something.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperatorSource {
    Operator,
}

/// Who refused an attempt out of band, on what evidence, and against exactly which bytes.
///
/// Durable beside the receipt it explains, because a refusal an operator forced and a refusal the
/// transport proved are different facts and a reader that cannot tell them apart will trust the
/// wrong one. `digest` is part of the record, not only of the call: the row already carries
/// binding, filename, correlation, and token beside this audit, so the pre-mutation digest is the
/// one half of the precondition that would otherwise have no durable representation at all — and
/// "what was the operator looking at" is the first question asked of a forced receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorAudit {
    pub source: OperatorSource,
    /// The real uid of the process that asked. Identity, not authorization: the file mode is what
    /// keeps a foreign uid out.
    pub uid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// The asking process's `ST_AGENT`, where it had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub st_agent: Option<String>,
    /// Trimmed and bounded by [`OPERATOR_REASON_MAX_BYTES`].
    pub reason: String,
    /// SHA-256 of the exact ledger bytes the operator decided against, in canonical lowercase
    /// hex. Compared under the transaction lock AND retained.
    pub digest: LedgerDigest,
    /// When the operator decided, from the caller's clock. The core takes no clock so a test can
    /// pin the record byte for byte.
    pub observed_at_ms: u64,
}

impl OperatorAudit {
    /// Build an audit record, trimming and bounding the reason.
    ///
    /// Refuses an empty reason: a forced refusal with no stated cause is the one record shape a
    /// later reader can do nothing with.
    pub fn new(
        source: OperatorSource,
        uid: u32,
        pid: Option<u32>,
        st_agent: Option<String>,
        reason: &str,
        digest: LedgerDigest,
        observed_at_ms: u64,
    ) -> Result<Self> {
        let reason = reason.trim();
        anyhow::ensure!(
            !reason.is_empty(),
            "an operator delivery refusal must state a reason"
        );
        anyhow::ensure!(
            reason.len() <= OPERATOR_REASON_MAX_BYTES,
            "an operator delivery refusal reason is bounded to {OPERATOR_REASON_MAX_BYTES} bytes"
        );
        Ok(Self {
            source,
            uid,
            pid,
            st_agent,
            reason: reason.to_owned(),
            digest,
            observed_at_ms,
        })
    }
}

/// One out-of-band refusal: an operator asserting that a correlated message is ABSENT.
///
/// There is no receipt field, deliberately. An operator is granted correlated absence and nothing
/// else: [`NegativeReceipt::Rejected`] means "the transport refused this call", which only the
/// transport can honestly say, and a public field here would let a caller assert it. The fence
/// and the audit's digest are the two halves of the precondition — the token says "this attempt",
/// the digest says "and the ledger I read" — because an operator who read a snapshot, thought,
/// and then acted must not land that decision on a ledger something else has since changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorRefusal {
    pub fence: Fence,
    pub audit: OperatorAudit,
}

/// What an operator refusal did. Every non-`Applied` outcome wrote NOTHING: the ledger bytes are
/// byte-identical to what the transaction read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorOutcome {
    /// The absence and its audit landed on the exact fenced attempt.
    Applied(Attempt),
    /// The ledger changed between the operator's read and their decision.
    StaleDigest { observed: LedgerDigest },
    /// Nothing matches this exact fence: the attempt was pruned, its token has rotated, or the
    /// fence names a foreign binding or correlation.
    UnknownAttempt,
    /// The delivery already reached its harness's settlement ceiling.
    AlreadySettled(Attempt),
    /// An absence is already recorded, so re-stating it would only rewrite the audit.
    AlreadyRefused(Attempt),
    /// The recipient's canonical archive receipt settled the message before this assertion.
    ArchiveSettled { filename: String },
}

/// The exact identity of one attempt: binding, filename, correlation, and token.
///
/// All four, because each rules out a different confusion: the filename names the message, the
/// binding names the thread or session it was sent to, the correlation names the transport-level
/// message id, and the token names *this* attempt rather than any other attempt of the same three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence {
    pub filename: String,
    pub binding: String,
    pub correlation: Correlation,
    pub token: AttemptToken,
}

impl Fence {
    fn matches(&self, row: &Row) -> bool {
        row.filename == self.filename
            && row.binding == self.binding
            && row.correlation == self.correlation
            && row.attempt_token == Some(self.token)
    }
}

/// Authorization for exactly one transport of exactly one attempt.
///
/// It exists only as the return value of a [`Ledger::claim`] that already wrote `Attempted`
/// durably, so holding one is the same fact as "the ledger says this build is mid-attempt".
#[derive(Debug, Clone)]
pub struct Permit {
    attempt: Attempt,
}

impl Permit {
    pub fn attempt(&self) -> &Attempt {
        &self.attempt
    }

    pub fn fence(&self) -> Fence {
        self.attempt.fence()
    }

    pub fn filename(&self) -> &str {
        &self.attempt.filename
    }

    pub fn binding(&self) -> &str {
        &self.attempt.binding
    }

    pub fn correlation(&self) -> &str {
        &self.attempt.correlation.value
    }

    pub fn token(&self) -> AttemptToken {
        self.attempt.token
    }
}

/// The outcome of a claim. `Held` wrote nothing at all.
#[derive(Debug, Clone)]
pub enum Claim {
    Permitted(Permit),
    Held(HoldReason),
}

/// What a claim would decide, computed from a snapshot without writing anything.
///
/// The same function [`Ledger::claim`] evaluates under the lock, so the pure predicate and the
/// written act cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authorization {
    /// A claim would mint a fresh token and durably write `Attempted`.
    Permitted,
    /// A claim would write nothing.
    Held(HoldReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    Release,
    Hold(HoldReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    AmbiguousAttempt,
    UnreadReceipt,
    NegativeReceipt,
    Settled,
    /// An attempt for another filename is outstanding. Delivery is FIFO and one-at-a-time, so the
    /// outstanding attempt is the head and every later filename waits behind it.
    OutstandingHead,
    /// The attempt for this filename belongs to another binding. It may have landed on that
    /// binding, so it can neither be delivered again nor discarded by pointing the ledger
    /// somewhere else.
    ForeignBinding,
}

impl HoldReason {
    /// The bounded operator-facing word for this hold.
    ///
    /// Owned here, not at the printers: the roster, the CLI and the JSON `delivery.reason` must
    /// all say the same word for the same fact, and a vocabulary spelled at three call sites
    /// drifts on the first new variant.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AmbiguousAttempt => "ambiguousAttempt",
            Self::UnreadReceipt => "unreadReceipt",
            Self::NegativeReceipt => "negativeReceipt",
            Self::Settled => "settled",
            Self::OutstandingHead => "outstandingHead",
            Self::ForeignBinding => "foreignBinding",
        }
    }
}

/// What a token-fenced operation did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Landed {
    /// The evidence or receipt applied to the exact fenced attempt and the ledger changed.
    Recorded(Phase),
    /// The fenced attempt is already at or past this evidence; nothing changed.
    Unchanged(Phase),
    /// No attempt matches this exact fence. The attempt it describes was pruned, retargeted, or
    /// re-claimed with a fresh token, so this operation is stale and lands on nothing.
    Stale,
}

/// The outcome of pointing the ledger at a new binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retarget {
    /// Nothing belongs to another binding.
    Clean,
    /// Foreign-binding attempts were settled or authoritatively refused, so dropping them loses
    /// nothing the new binding could need.
    Dropped(usize),
    /// An ambiguous foreign-binding attempt is retained. It may have landed, so it can neither be
    /// discarded nor delivered again; archive precedence — the recipient's own act — resolves it.
    Retained(usize),
}

/// Everything a claim has to name about the delivery it wants to make.
#[derive(Debug, Clone)]
pub struct Claimant {
    pub filename: String,
    pub binding: String,
    pub correlation: Correlation,
    pub incarnation: Option<String>,
}

/// A read-only view of one transaction's exact durable bytes.
#[derive(Debug, Clone)]
pub struct Snapshot {
    profile: Profile,
    digest: LedgerDigest,
    attempts: Vec<Attempt>,
    backfilled: usize,
}

impl Snapshot {
    fn of(profile: Profile, loaded: &Loaded) -> Result<Self> {
        Ok(Self {
            profile,
            digest: loaded.digest,
            attempts: loaded
                .record
                .entries
                .iter()
                .map(Attempt::of)
                .collect::<Result<Vec<_>>>()?,
            backfilled: loaded.backfilled,
        })
    }

    /// SHA-256 of the exact raw ledger bytes this snapshot describes.
    pub fn digest(&self) -> LedgerDigest {
        self.digest
    }

    pub fn attempts(&self) -> &[Attempt] {
        &self.attempts
    }

    pub fn attempt(&self, filename: &str) -> Option<&Attempt> {
        self.attempts
            .iter()
            .find(|attempt| attempt.filename == filename)
    }

    pub fn binding(&self) -> Option<&str> {
        self.attempts
            .first()
            .map(|attempt| attempt.binding.as_str())
    }

    /// Every attempt sharing one transport-level correlation. One correlation can carry several
    /// inbox files, so one receipt can settle several attempts — each on its own monotone row.
    pub fn correlated(&self, value: &str) -> Vec<&Attempt> {
        self.attempts
            .iter()
            .filter(|attempt| attempt.correlation.value == value)
            .collect()
    }

    /// How many tokenless canonical v1 rows the transaction behind this snapshot backfilled.
    pub fn backfilled(&self) -> usize {
        self.backfilled
    }

    /// Whether the recipient still owns this filename, and why.
    pub fn retention(&self, filename: &str) -> Retention {
        match self.attempt(filename) {
            None => Retention::Release,
            Some(attempt) => retention_of(self.profile, attempt.phase, attempt.negative),
        }
    }

    /// What a claim for this exact identity would decide. Writes nothing.
    pub fn authorization(&self, binding: &str, filename: &str) -> Authorization {
        // One attempt is outstanding at a time and it is the FIFO head: an attempt bound to some
        // other filename holds every later filename until archive precedence resolves it, so a
        // message arriving out of filename order can never open a second concurrent delivery.
        if self
            .attempts
            .iter()
            .any(|attempt| attempt.filename != filename)
        {
            return Authorization::Held(HoldReason::OutstandingHead);
        }
        let Some(attempt) = self.attempt(filename) else {
            return Authorization::Permitted;
        };
        if attempt.binding != binding {
            return Authorization::Held(HoldReason::ForeignBinding);
        }
        if attempt.negative.is_some() {
            // The only authority that reopens an identity: this build observed that the attempt
            // provably did not land.
            return Authorization::Permitted;
        }
        match self.retention(filename) {
            Retention::Release => Authorization::Held(HoldReason::Settled),
            Retention::Hold(reason) => Authorization::Held(reason),
        }
    }
}

fn retention_of(profile: Profile, phase: Phase, negative: Option<NegativeReceipt>) -> Retention {
    if negative.is_some() {
        return Retention::Hold(HoldReason::NegativeReceipt);
    }
    if profile.releases(phase) {
        return Retention::Release;
    }
    if phase >= Phase::Persisted {
        return Retention::Hold(HoldReason::UnreadReceipt);
    }
    Retention::Hold(HoldReason::AmbiguousAttempt)
}

// ---- the marker renderer ----------------------------------------------------------------------

/// The tag every rendered delivery marker opens with.
const MARKER_TAG: &str = "st2-delivery";

/// Render the provider-neutral delivery marker for one attempt.
///
/// The grammar is fixed by Q30 and is deliberately a bracketed keyed form —
/// `[st2-delivery filename=<canonical> attempt=<32-lowercase-hex>]` — because the marker is
/// VISIBLE: it lands in a transcript a human reads, so each field has to say what it is, and a
/// positional colon form would be unreadable and ambiguous the moment a third field appeared.
///
/// It carries nothing but the canonical inbox filename and the exact attempt token. A marker that
/// also carried the binding, the harness, or the message text would be a second, weaker identity
/// for the same attempt, and a harness that echoed it would spill the recipient's inbox contents
/// into its own transcript.
///
/// Claude, Pi, and OMP prepend this marker through their production native channels.
pub fn marker(filename: &str, token: AttemptToken) -> Result<String> {
    anyhow::ensure!(
        message::is_message_filename(filename),
        "a delivery marker names a canonical inbox filename"
    );
    Ok(format!(
        "[{MARKER_TAG} filename={filename} attempt={token}]"
    ))
}

/// The inverse of [`marker`]. Anything that is not exactly one marker is `None`.
pub fn parse_marker(text: &str) -> Option<(String, AttemptToken)> {
    let body = text
        .trim()
        .strip_prefix('[')?
        .strip_suffix(']')?
        .strip_prefix(MARKER_TAG)?;
    let mut fields = body.split_ascii_whitespace();
    let filename = fields.next()?.strip_prefix("filename=")?;
    let token = fields.next()?.strip_prefix("attempt=")?;
    if fields.next().is_some() || !message::is_message_filename(filename) {
        return None;
    }
    Some((filename.to_owned(), AttemptToken::parse(token).ok()?))
}

// ---- read-only observation --------------------------------------------------------------------

/// What a read-only look at a ledger path found.
#[derive(Debug, Clone)]
pub enum Observation {
    /// No record at this path: this seat has never delivered through the ledger. Positively
    /// absent, never "empty" — an absent record and a record holding nothing are different facts.
    Absent,
    /// Bytes this build read and validated.
    Held(Sighting),
    /// Bytes are present and this build cannot interpret them as its own record. Never healthy,
    /// and never repaired: the digest says exactly which bytes were refused.
    Indeterminate {
        digest: LedgerDigest,
        reason: String,
    },
}

/// A validated read-only reading of one ledger.
#[derive(Debug, Clone)]
pub struct Sighting {
    /// SHA-256 of the exact raw bytes read.
    pub digest: LedgerDigest,
    pub binding: Option<String>,
    pub attempts: Vec<Sighted>,
    /// Rows still written without an attempt token (Q35). A mutation would backfill these; this
    /// reader reports them, so the fleet can watch the count reach zero and delete the arm.
    pub tokenless: usize,
    /// Rows carrying a phase this build never observed — clause 2 of DELTA-006's signal.
    pub asserted: usize,
}

/// One observed row. Its token is optional because observation never backfills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sighted {
    pub filename: String,
    pub binding: String,
    pub correlation: Correlation,
    pub token: Option<AttemptToken>,
    pub phase: Phase,
    pub attestation: Attestation,
    pub incarnation: Option<String>,
    pub negative: Option<NegativeReceipt>,
    /// Present only where an operator, rather than the transport, produced `negative`.
    pub audit: Option<OperatorAudit>,
    pub retention: Retention,
}

/// Read one ledger without recovering, backfilling, or writing anything.
///
/// `correlate` must be the transport's own derivation, so a row that does not re-derive from its
/// own binding and filename is reported as indeterminate rather than as this agent's delivery
/// state. Providers wrap this with their derivation already bound — see
/// `crate::codex_app_server::observe_delivery` and `crate::opencode_session::observe_delivery` —
/// so a joining reader never reimplements a provider's correlation.
///
/// `Err` is reserved for a failure to *look*: an unreadable directory, a lock that is not a
/// regular file. Bytes that will not decode are [`Observation::Indeterminate`], because "I hold a
/// delivery record I cannot read" is an observation, not a failed observation.
pub fn observe(
    path: &Path,
    profile: Profile,
    agent: &str,
    correlate: &dyn Fn(&str, &str) -> String,
) -> Result<Observation> {
    let lock_path = lock_path_of(path);
    // Shared, so a reader cannot tear a mutation half-published; absent, so a reader never
    // creates the lock in a tree it only inspects.
    let _guard = match flock::open(&lock_path, flock::Open::Existing) {
        Ok(file) => {
            anyhow::ensure!(
                file.metadata()?.is_file(),
                "delivery ledger lock is not a regular file: {}",
                lock_path.display()
            );
            Some(
                FileLock::hold_blocking(file, flock::Mode::Shared)
                    .with_context(|| format!("locking delivery ledger {}", lock_path.display()))?,
            )
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("opening delivery ledger lock {}", lock_path.display()));
        }
    };
    let raw = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Observation::Absent),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading delivery ledger {}", path.display()));
        }
    };
    let digest = LedgerDigest::of(&raw);
    let record = match decode(&raw, profile, agent).and_then(|record| {
        accept(&record.entries, profile, correlate)?;
        Ok(record)
    }) {
        Ok(record) => record,
        Err(reason) => {
            return Ok(Observation::Indeterminate {
                digest,
                reason: format!("{reason:#}"),
            });
        }
    };
    Ok(Observation::Held(Sighting {
        digest,
        binding: record.entries.first().map(|row| row.binding.clone()),
        tokenless: record
            .entries
            .iter()
            .filter(|row| row.attempt_token.is_none())
            .count(),
        asserted: record
            .entries
            .iter()
            .filter(|row| row.attestation == Attestation::Asserted)
            .count(),
        attempts: record
            .entries
            .iter()
            .map(|row| Sighted {
                filename: row.filename.clone(),
                binding: row.binding.clone(),
                correlation: row.correlation.clone(),
                token: row.attempt_token,
                phase: row.phase,
                attestation: row.attestation,
                incarnation: row.incarnation.clone(),
                negative: row.negative,
                audit: row.operator_audit.clone(),
                retention: retention_of(profile, row.phase, row.negative),
            })
            .collect(),
    }))
}

/// How many entries in the ledger at `path` carry a phase this build never observed.
///
/// Clause 2 of DELTA-006's Resolution Signal, and the reason [`Attestation`] is serialized at
/// all. Read-only on purpose: a transaction runs the record boundary and may write, and a
/// diagnostic must not change what it measures. A missing ledger counts zero; bytes that will
/// not parse are an error, because "I hold a delivery record I cannot read" is exactly what an
/// operator needs told.
pub(crate) fn asserted_entries(path: &Path) -> Result<usize> {
    Ok(count_rows(path)?
        .iter()
        .filter(|row| row.attestation == Attestation::Asserted)
        .count())
}

/// How many entries in the ledger at `path` carry NO attempt token.
///
/// Absent tokens only — a row this build has already tokenized, deterministically or otherwise,
/// is not counted, because it is fenceable and therefore no longer the thing the exception
/// exists for.
///
/// DELTA-007's deletion signal, and deliberately independent of
/// [`crate::migrations::delivery_state::ResolutionSignal`]: that signal is DELTA-006's and has to
/// stay resolvable — its module has to stay deletable — while a tokenless canonical row may
/// still exist somewhere. Read-only for the same reason as [`asserted_entries`].
pub fn tokenless_entries(path: &Path) -> Result<usize> {
    Ok(count_rows(path)?
        .iter()
        .filter(|row| row.attempt_token.is_none())
        .count())
}

fn count_rows(path: &Path) -> Result<Vec<Row>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading delivery ledger {}", path.display()));
        }
    };
    let record: Record = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing delivery ledger {}", path.display()))?;
    Ok(record.entries)
}

// ---- the transaction --------------------------------------------------------------------------

type Correlate = Box<dyn Fn(&str, &str) -> String + Send + Sync>;

/// Immutable configuration. Holds no record, so nothing here can go stale.
struct Config {
    path: PathBuf,
    lock_path: PathBuf,
    profile: Profile,
    agent: String,
    runtime_id: String,
    correlate: Correlate,
}

/// One transaction's view of the durable bytes.
struct Loaded {
    record: Record,
    /// SHA-256 of the exact raw bytes this transaction acts on.
    digest: LedgerDigest,
    backfilled: usize,
    /// Whether recovery or backfill produced state that must be durable before anything acts.
    dirty: bool,
}

impl Config {
    /// Take the permanent sibling lock: no-follow, owner-only, and proven to be a regular file.
    ///
    /// `O_NOFOLLOW` stops a symlink planted at the lock path from redirecting the lock domain to a
    /// file its planter also holds; the regular-file check stops a fifo or directory from turning
    /// the acquisition into a hang or a nonsense success. State-plane records live in
    /// agent-writable directories, so both are load-bearing rather than decorative.
    fn lock(&self) -> Result<FileLock> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating delivery ledger directory {}", parent.display())
            })?;
        }
        self.hold(flock::Open::Create)
    }

    /// The operator path's lock: it must ALREADY exist.
    ///
    /// Creates no directory and no lock file, so an operator command against a seat that has
    /// never delivered fails instead of bringing the protocol's own files into existence. A
    /// `NotFound` here means exactly "this ledger has never been written", which is the honest
    /// answer to "refuse this attempt".
    fn lock_existing(&self) -> Result<FileLock> {
        self.hold(flock::Open::Existing)
    }

    fn hold(&self, create: flock::Open) -> Result<FileLock> {
        let file = flock::open(&self.lock_path, create).with_context(|| {
            format!("opening delivery ledger lock {}", self.lock_path.display())
        })?;
        anyhow::ensure!(
            file.metadata()?.is_file(),
            "delivery ledger lock is not a regular file: {}",
            self.lock_path.display()
        );
        FileLock::hold_blocking(file, flock::Mode::Exclusive)
            .with_context(|| format!("locking delivery ledger {}", self.lock_path.display()))
    }

    /// Read and validate the exact current bytes.
    fn load(&self) -> Result<Loaded> {
        let raw = match fs::read(&self.path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading delivery ledger {}", self.path.display()));
            }
        };
        let mut dirty = false;
        let mut record = match &raw {
            Some(bytes) => decode(bytes, self.profile, &self.agent)?,
            // THE RECOVERY SEAM. The only statement in this module that knows any other delivery
            // record has ever existed; deleting `crate::migrations::delivery_state` deletes this
            // arm and nothing else. What it degrades to — an empty entry list — is exactly a
            // first run on a fresh seat. It runs under the transaction lock, so two first reads
            // cannot become two translations. DELETION TRIGGER: docs/vrs/.delta/DELTA-006.
            None => {
                let entries = self.recovered()?;
                dirty = !entries.is_empty();
                Record {
                    schema: LEDGER_SCHEMA.to_owned(),
                    harness: self.profile.harness.as_str().to_owned(),
                    agent: self.agent.clone(),
                    runtime_id: self.runtime_id.clone(),
                    entries,
                }
            }
        };
        // `runtimeId` is the CREATING driver's provenance and is not re-stamped on load: an
        // operator transaction opens the same record without owning the runtime, and rewriting
        // the field there would hand a reader the wrong provenance.
        // Validate BEFORE tokenizing: a row this build must refuse is refused on the bytes it
        // was given, never on bytes this build derived from them.
        accept(&record.entries, self.profile, &self.correlate)?;
        let backfilled = backfill(&mut record)?;
        dirty |= backfilled > 0;
        let digest = if dirty {
            // The bytes this transaction will act on are the ones it is about to publish, and
            // `atomic_json` publishes exactly `serde_json::to_vec`.
            LedgerDigest::of(&serde_json::to_vec(&record)?)
        } else {
            LedgerDigest::of(raw.as_deref().unwrap_or_default())
        };
        Ok(Loaded {
            record,
            digest,
            backfilled,
            dirty,
        })
    }

    fn recovered(&self) -> Result<Vec<Row>> {
        let Some(state_dir) = self.path.parent() else {
            return Ok(Vec::new());
        };
        Ok(crate::migrations::delivery_state::recover(
            state_dir,
            self.profile.harness,
            &self.agent,
            &self.correlate,
        )?
        .into_iter()
        .map(Carried::into_row)
        .collect())
    }

    /// Read the ledger for the operator path: it must already exist, be a real regular file, and
    /// be owned by this uid.
    ///
    /// `lstat`, not `stat`, so a symlink planted at the path is refused rather than followed to
    /// whatever it names. The owner check is not the mode check: `0600` says nothing to root, and
    /// the operator command runs as whoever invoked it. Absence is an ERROR here rather than an
    /// empty record, because "there is nothing to refuse" and "I created a ledger to refuse
    /// something in" are very different acts and only the first is allowed.
    fn read_owned_regular(&self) -> Result<Vec<u8>> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = fs::symlink_metadata(&self.path)
            .with_context(|| format!("reading delivery ledger {}", self.path.display()))?;
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "delivery ledger is not a real regular file: {}",
            self.path.display()
        );
        let owner = unsafe { libc::geteuid() };
        anyhow::ensure!(
            metadata.uid() == owner,
            "delivery ledger {} is owned by uid {}, not {owner}",
            self.path.display(),
            metadata.uid()
        );
        fs::read(&self.path)
            .with_context(|| format!("reading delivery ledger {}", self.path.display()))
    }

    fn persist(&self, record: &Record) -> Result<()> {
        atomic_json(&self.path, record)
            .with_context(|| format!("writing delivery ledger {}", self.path.display()))
    }
}

fn decode(bytes: &[u8], profile: Profile, agent: &str) -> Result<Record> {
    let record: Record = serde_json::from_slice(bytes).context("reading delivery ledger record")?;
    anyhow::ensure!(
        record.schema == LEDGER_SCHEMA,
        "delivery ledger has unsupported schema '{}'",
        record.schema
    );
    anyhow::ensure!(
        Harness::parse(&record.harness)? == profile.harness,
        "delivery ledger belongs to harness '{}'",
        record.harness
    );
    anyhow::ensure!(
        record.agent == agent,
        "delivery ledger belongs to a different agent"
    );
    Ok(record)
}

/// Every check a set of rows from outside this transaction must pass.
fn accept(rows: &[Row], profile: Profile, correlate: &dyn Fn(&str, &str) -> String) -> Result<()> {
    for row in rows {
        anyhow::ensure!(
            message::is_message_filename(&row.filename) && !row.binding.is_empty(),
            "delivery ledger entry has an invalid binding or filename"
        );
        anyhow::ensure!(
            row.correlation.value == correlate(&row.binding, &row.filename),
            "delivery ledger entry correlation does not match its binding"
        );
        anyhow::ensure!(
            profile.proves(row.phase),
            "delivery ledger entry records a phase {} cannot prove",
            profile.harness.as_str()
        );
    }
    anyhow::ensure!(
        rows.windows(2)
            .all(|pair| pair[0].binding == pair[1].binding),
        "delivery ledger holds entries from more than one binding"
    );
    let mut filenames = std::collections::HashSet::with_capacity(rows.len());
    anyhow::ensure!(
        rows.iter().all(|row| filenames.insert(&row.filename)),
        "delivery ledger holds two entries for one filename"
    );
    Ok(())
}

/// Q35, the one bounded compatibility exception: tokenize every canonical v1 row that predates
/// attempt tokens, deterministically, from its own exact immutable bytes.
///
/// Counted so the fleet can watch it reach zero. Every *new* mutation mints fresh randomness, so
/// this arm can only shrink.
fn backfill(record: &mut Record) -> Result<usize> {
    let mut backfilled = 0;
    for row in &mut record.entries {
        if row.attempt_token.is_none() {
            row.attempt_token = Some(AttemptToken::derived(row)?);
            backfilled += 1;
        }
    }
    Ok(backfilled)
}

/// Where a crash lands, for the tests that prove each half of a transaction is safe alone.
fn transaction_checkpoint(stage: &str) {
    #[cfg(not(test))]
    let _ = stage;
    #[cfg(test)]
    if std::env::var("ST2_DELIVERY_LEDGER_CRASH_STAGE").as_deref() == Ok(stage) {
        std::process::exit(match stage {
            "after-load-before-act" => 71,
            "after-act-before-release" => 72,
            _ => 73,
        });
    }
}

/// The canonical delivery ledger: immutable configuration plus short locked transactions.
pub struct Ledger {
    config: Config,
    /// The most recent transaction's fail-closed verdict about the durable bytes. Reported so a
    /// driver can publish its typed "transport unavailable" boundary; it authorizes nothing.
    quarantine: Option<String>,
    /// How many tokenless canonical v1 rows this ledger has backfilled (Q35).
    backfilled: usize,
}

impl Ledger {
    /// Open the canonical ledger at `path`.
    ///
    /// `correlate(binding, filename)` must be the derivation the transport uses. Opening runs one
    /// ordinary transaction — the same lock, the same validation, the same publication — so
    /// recovery and the Q35 backfill are serialized against every mutation. Invalid or unreadable
    /// state quarantines delivery rather than preventing the harness process from starting: a
    /// driver that will not start delivers nothing at all.
    pub fn open(
        path: &Path,
        profile: Profile,
        agent: &str,
        runtime_id: &str,
        correlate: impl Fn(&str, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        let mut ledger = Self::config_only(path, profile, agent, runtime_id, correlate);
        if let Err(error) = ledger.transact(|_, _| Ok(())) {
            ledger.quarantine = Some(format!("{error:#}"));
        }
        ledger
    }

    /// A ledger for the operator path: configuration ONLY, no transaction, no I/O at all.
    ///
    /// [`Ledger::open`] deliberately runs one ordinary transaction, which recovers the legacy
    /// record and performs the Q35 backfill — right for a driver, and fatal here: it would
    /// rewrite the very bytes an operator's digest precondition is taken against, so a refusal on
    /// a changed digest would still have changed the file. This constructor therefore touches
    /// nothing: it creates no directory, no lock file, and no record, and it cannot report a
    /// quarantine because it has read nothing. The only method it may run is
    /// [`Ledger::operator_refuse`], which does its own strictly ordered locking.
    ///
    /// It takes no runtime id, because an operator owns no runtime: the field is only ever used
    /// to stamp a record this path never creates.
    pub fn for_operator(
        path: &Path,
        profile: Profile,
        agent: &str,
        correlate: impl Fn(&str, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        Self::config_only(path, profile, agent, "", correlate)
    }

    fn config_only(
        path: &Path,
        profile: Profile,
        agent: &str,
        runtime_id: &str,
        correlate: impl Fn(&str, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            config: Config {
                path: path.to_path_buf(),
                lock_path: lock_path_of(path),
                profile,
                agent: agent.to_owned(),
                runtime_id: runtime_id.to_owned(),
                correlate: Box::new(correlate),
            },
            quarantine: None,
            backfilled: 0,
        }
    }

    /// The last transaction's refusal, if it had one.
    pub fn quarantined(&self) -> Option<&str> {
        self.quarantine.as_deref()
    }

    /// How many tokenless canonical v1 rows this ledger has backfilled (Q35).
    pub fn backfilled(&self) -> usize {
        self.backfilled
    }

    /// The typed read-only view, taken inside a transaction so it names exact durable bytes.
    pub fn snapshot(&mut self) -> Result<Snapshot> {
        self.transact(|config, loaded| Snapshot::of(config.profile, loaded))
    }

    /// Authorize and durably open exactly one attempt, or write nothing.
    ///
    /// This is the ONLY authorization. There is no separate "may I?" to consult first, because a
    /// separate question has a window: two readers could both be told yes and both transport. The
    /// permit returned here exists only after `Attempted` has reached the disk under the lock.
    pub fn claim(&mut self, claimant: Claimant) -> Result<Claim> {
        self.transact(move |config, loaded| {
            anyhow::ensure!(
                message::is_message_filename(&claimant.filename) && !claimant.binding.is_empty(),
                "delivery claim has an invalid binding or filename"
            );
            anyhow::ensure!(
                claimant.correlation.value
                    == (config.correlate)(&claimant.binding, &claimant.filename),
                "delivery claim correlation does not match its binding"
            );
            let snapshot = Snapshot::of(config.profile, loaded)?;
            match snapshot.authorization(&claimant.binding, &claimant.filename) {
                Authorization::Held(reason) => Ok(Claim::Held(reason)),
                Authorization::Permitted => {
                    let row = Row {
                        filename: claimant.filename,
                        binding: claimant.binding,
                        correlation: claimant.correlation,
                        attempt_token: Some(AttemptToken::mint()?),
                        // A fresh attempt. Reaching here requires either no prior row or an
                        // authoritative negative receipt proving the prior one never landed, so
                        // this is not a downgrade of evidence — it is a new attempt's floor.
                        phase: Phase::Attempted,
                        attestation: Attestation::Observed,
                        incarnation: claimant.incarnation,
                        negative: None,
                        operator_audit: None,
                    };
                    match loaded
                        .record
                        .entries
                        .iter()
                        .position(|entry| entry.filename == row.filename)
                    {
                        Some(index) => loaded.record.entries[index] = row.clone(),
                        None => loaded.record.entries.push(row.clone()),
                    }
                    Ok(Claim::Permitted(Permit {
                        attempt: Attempt::of(&row)?,
                    }))
                }
            }
        })
    }

    /// Record positive evidence against the exact fenced attempt, without allowing a downgrade.
    pub fn record(&mut self, fence: &Fence, evidence: Evidence) -> Result<Landed> {
        self.transact(|config, loaded| {
            let phase = config.profile.graded(evidence)?;
            let Some(row) = loaded
                .record
                .entries
                .iter_mut()
                .find(|row| fence.matches(row))
            else {
                return Ok(Landed::Stale);
            };
            if phase <= row.phase {
                return Ok(Landed::Unchanged(row.phase));
            }
            row.phase = phase;
            // This build graded the evidence, so the phase is no longer a carried-forward claim.
            row.attestation = Attestation::Observed;
            row.negative = None;
            row.operator_audit = None;
            Ok(Landed::Recorded(phase))
        })
    }

    /// Record an authoritative negative receipt against the exact fenced attempt.
    ///
    /// A settled entry is never reopened, and the receipt does not itself authorize anything: the
    /// next [`Ledger::claim`] reads it, mints a FRESH token, and by doing so makes this fence
    /// stale — so a late positive receipt for the refused attempt cannot land on its successor.
    ///
    /// `attestation` is left alone. It is provenance for the PHASE, and an absence is an
    /// observation about the message, not about the phase that was carried forward — so a crash
    /// between this receipt and the re-claim still counts a carried row's asserted phase
    /// truthfully in DELTA-006 instead of laundering it into an observation.
    pub fn negative(&mut self, fence: &Fence, receipt: NegativeReceipt) -> Result<Landed> {
        self.transact(|config, loaded| {
            let profile = config.profile;
            let Some(row) = loaded
                .record
                .entries
                .iter_mut()
                .find(|row| fence.matches(row))
            else {
                return Ok(Landed::Stale);
            };
            // A delivery that already reached its ceiling cannot be un-settled by a late refusal.
            if profile.releases(row.phase) || row.negative == Some(receipt) {
                return Ok(Landed::Unchanged(row.phase));
            }
            row.negative = Some(receipt);
            row.operator_audit = None;
            Ok(Landed::Recorded(row.phase))
        })
    }

    /// Record correlated ABSENCE for one attempt, on an operator's authority.
    ///
    /// The provider-neutral operator boundary, and the only place a negative receipt is written by
    /// something other than the transport. It transports NOTHING — it records that the correlated
    /// message is absent, which is exactly what re-authorizes the next [`Ledger::claim`] to mint
    /// a fresh token and send again.
    ///
    /// The receipt is [`NegativeReceipt::Absent`], hardcoded. `Rejected` means "the transport
    /// refused this call" and only the transport can honestly say that, so it is not reachable
    /// from here at all rather than reachable-and-discouraged.
    ///
    /// # Why this does not use the ordinary transaction
    ///
    /// The ordinary loader RECOVERS and BACKFILLS before an operation runs, which is right for a
    /// driver — the pump wants a tokenized, migrated record — and wrong here: it would rewrite
    /// the bytes before the operator's digest precondition had been checked, so a refusal on a
    /// changed digest would still have changed the file. This path is strictly ordered instead:
    /// lock, read the exact raw bytes, compare the digest FIRST, then decode and validate, then
    /// require a token-bearing exact fence, then mutate. An operator therefore never initializes,
    /// recovers, backfills, or creates anything, and every refusal is byte-identical. A tokenless
    /// Q35 row simply matches no fence, so it refuses until an ordinary provider mutation
    /// tokenizes it.
    pub fn operator_refuse(&mut self, refusal: &OperatorRefusal) -> Result<OperatorOutcome> {
        anyhow::ensure!(
            message::is_message_filename(&refusal.fence.filename),
            "an operator delivery refusal names a canonical inbox filename"
        );
        let guard = self.config.lock_existing()?;
        let outcome = self.refuse_locked(refusal);
        if let Err(error) = &outcome {
            self.quarantine = Some(format!("{error:#}"));
        }
        drop(guard);
        outcome
    }

    fn refuse_locked(&mut self, refusal: &OperatorRefusal) -> Result<OperatorOutcome> {
        let raw = self.config.read_owned_regular()?;
        let digest = LedgerDigest::of(&raw);
        // FIRST, before anything is decoded and long before anything could be written.
        if digest != refusal.audit.digest {
            return Ok(OperatorOutcome::StaleDigest { observed: digest });
        }
        let mut record = decode(&raw, self.config.profile, &self.config.agent)?;
        accept(&record.entries, self.config.profile, &self.config.correlate)?;
        self.quarantine = None;
        let profile = self.config.profile;
        let Some(row) = record
            .entries
            .iter_mut()
            .find(|row| refusal.fence.matches(row))
        else {
            return Ok(OperatorOutcome::UnknownAttempt);
        };
        if profile.releases(row.phase) {
            return Ok(OperatorOutcome::AlreadySettled(Attempt::of(row)?));
        }
        if row.negative == Some(NegativeReceipt::Absent) {
            return Ok(OperatorOutcome::AlreadyRefused(Attempt::of(row)?));
        }
        row.negative = Some(NegativeReceipt::Absent);
        row.operator_audit = Some(refusal.audit.clone());
        // `attestation` is provenance for the PHASE, and the operator asserted nothing about the
        // phase — it stays whatever the driver observed or carried forward. Touching it here
        // would invent DELTA-006 rows out of operator activity. What distinguishes a forced
        // absence from a proven one is `operator_audit`, which is exactly its job.
        let attempt = Attempt::of(row)?;
        self.config.persist(&record)?;
        Ok(OperatorOutcome::Applied(attempt))
    }

    /// Point the ledger at `binding`, keeping every attempt the new binding may not discard.
    ///
    /// The old `rebind` deleted every foreign row outright, which threw away exactly the rows that
    /// matter: an ambiguous attempt on the previous thread or session MAY have landed, and
    /// deleting it re-authorizes a second delivery of a message the recipient may already hold.
    /// Only a settled or authoritatively refused attempt is droppable.
    pub fn retarget(&mut self, binding: &str) -> Result<Retarget> {
        self.transact(|config, loaded| {
            let profile = config.profile;
            let mut dropped = 0_usize;
            let mut retained = 0_usize;
            loaded.record.entries.retain(|row| {
                if row.binding == binding {
                    return true;
                }
                if profile.releases(row.phase) || row.negative.is_some() {
                    dropped += 1;
                    return false;
                }
                retained += 1;
                true
            });
            Ok(match (dropped, retained) {
                (0, 0) => Retarget::Clean,
                (_, 0) => Retarget::Dropped(dropped),
                _ => Retarget::Retained(retained),
            })
        })
    }

    /// Drop each attempt the recipient has settled by archiving its file.
    ///
    /// `settled` are exact fences taken from a snapshot the caller has just read, so an attempt
    /// the caller never saw — or one whose token has rotated since — is never removed. That is
    /// what stops a stale unread listing from deleting a newer claim: the classic shape is a pump
    /// that lists the inbox, is descheduled, and comes back to delete an attempt a later pass
    /// legitimately opened for the same filename.
    pub fn prune(&mut self, settled: &[Fence]) -> Result<usize> {
        if settled.is_empty() {
            return Ok(0);
        }
        self.transact(|_, loaded| {
            let before = loaded.record.entries.len();
            loaded
                .record
                .entries
                .retain(|row| !settled.iter().any(|fence| fence.matches(row)));
            Ok(before - loaded.record.entries.len())
        })
    }

    /// One short transaction: lock, re-read exact bytes, act, publish, release.
    fn transact<T>(&mut self, act: impl FnOnce(&Config, &mut Loaded) -> Result<T>) -> Result<T> {
        let guard = self.config.lock()?;
        let mut loaded = match self.config.load() {
            Ok(loaded) => {
                self.quarantine = None;
                loaded
            }
            Err(error) => {
                // Fail closed and stay closed: the bytes are re-read next transaction, so a
                // repaired record recovers by itself and a broken one keeps refusing.
                self.quarantine = Some(format!("{error:#}"));
                return Err(error);
            }
        };
        if loaded.dirty {
            // Recovery and the Q35 tokens are durable BEFORE anything acts on the rows they
            // produced, so a crash cannot leave an act fenced against a token nobody stored.
            self.config.persist(&loaded.record)?;
            self.backfilled += loaded.backfilled;
        }
        transaction_checkpoint("after-load-before-act");
        let before = loaded.record.entries.clone();
        let outcome = act(&self.config, &mut loaded)?;
        if loaded.record.entries != before {
            self.config.persist(&loaded.record)?;
        }
        transaction_checkpoint("after-act-before-release");
        drop(guard);
        Ok(outcome)
    }
}

fn lock_path_of(path: &Path) -> PathBuf {
    path.with_file_name(LEDGER_LOCK)
}

/// Durable replacement: file bytes reach disk before rename, then the directory entry is synced.
///
/// The directory sync is STRICT — a parent that cannot be opened for it fails the publication,
/// where it used to be swallowed. A ledger whose directory entry may not survive a crash is
/// exactly the state the ledger exists to prevent being invisible, and a failure edge nothing can
/// observe is a guarantee nothing can review.
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    crate::fsatomic::replace(
        path,
        &bytes,
        crate::fsatomic::Staging::new(".delivery-ledger"),
        crate::fsatomic::Durability::FsyncFileAndDir,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

    const FILE_A: &str = "1786380000000-aaa111.md";
    const FILE_B: &str = "1786380000001-bbb222.md";

    fn correlation(binding: &str, filename: &str) -> String {
        format!("{binding}:{filename}")
    }

    fn open(dir: &Path, harness: Harness) -> Ledger {
        open_with(dir, harness.profile())
    }

    fn open_with(dir: &Path, profile: Profile) -> Ledger {
        Ledger::open(
            &dir.join(LEDGER_FILE),
            profile,
            "h.worker",
            "h.worker.runtime",
            correlation,
        )
    }

    fn claimant(binding: &str, filename: &str) -> Claimant {
        Claimant {
            filename: filename.to_owned(),
            binding: binding.to_owned(),
            correlation: Correlation::native(correlation(binding, filename)),
            incarnation: Some("incarnation-1".to_owned()),
        }
    }

    /// Claim and unwrap the permit: the ordinary "this build now owns an attempt" step.
    fn permit(ledger: &mut Ledger, binding: &str, filename: &str) -> Permit {
        match ledger.claim(claimant(binding, filename)).unwrap() {
            Claim::Permitted(permit) => permit,
            Claim::Held(reason) => panic!("expected a permit, held for {reason:?}"),
        }
    }

    fn held(ledger: &mut Ledger, binding: &str, filename: &str) -> HoldReason {
        match ledger.claim(claimant(binding, filename)).unwrap() {
            Claim::Held(reason) => reason,
            Claim::Permitted(permit) => panic!("expected a hold, permitted {:?}", permit.token()),
        }
    }

    fn seed(ledger: &mut Ledger, entries: Vec<Carried>) {
        let record = Record {
            schema: LEDGER_SCHEMA.to_owned(),
            harness: ledger.config.profile.harness.as_str().to_owned(),
            agent: ledger.config.agent.clone(),
            runtime_id: ledger.config.runtime_id.clone(),
            entries: entries.into_iter().map(Carried::into_row).collect(),
        };
        atomic_json(&ledger.config.path, &record).unwrap();
    }

    /// Every fence a snapshot holds — the caller's "delete what the recipient settled" input.
    fn fences(snapshot: &Snapshot) -> Vec<Fence> {
        snapshot.attempts().iter().map(Attempt::fence).collect()
    }

    #[test]
    fn phase_order_is_the_monotone_evidence_lattice() {
        assert!(Phase::Attempted < Phase::TransportAccepted);
        assert!(Phase::TransportAccepted < Phase::Persisted);
        assert!(Phase::Persisted < Phase::Consumed);
    }

    #[test]
    fn profiles_accept_only_evidence_their_harness_can_produce() {
        for harness in [Harness::Claude, Harness::Pi, Harness::Omp] {
            let attempt_only = harness.profile();
            assert!(attempt_only.proves(Phase::Attempted));
            assert!(attempt_only.graded(Evidence::TransportAccepted).is_err());
            assert!(attempt_only.graded(Evidence::Persisted).is_err());
            assert!(attempt_only.graded(Evidence::Consumed).is_err());
        }

        let codex = Harness::Codex.profile();
        assert!(codex.graded(Evidence::TransportAccepted).is_ok());
        assert!(codex.graded(Evidence::Persisted).is_err());
        assert!(codex.graded(Evidence::Consumed).is_ok());

        let opencode = Harness::OpenCode.profile();
        assert!(opencode.graded(Evidence::TransportAccepted).is_ok());
        assert!(opencode.graded(Evidence::Persisted).is_ok());
        assert!(opencode.graded(Evidence::Consumed).is_err());
    }

    #[test]
    fn five_harness_identities_map_onto_three_evidence_policies() {
        for (harness, name, policy) in [
            (Harness::Claude, "claude", EvidencePolicy::AttemptOnly),
            (Harness::Codex, "codex", EvidencePolicy::CodexReceipts),
            (Harness::Pi, "pi", EvidencePolicy::AttemptOnly),
            (
                Harness::OpenCode,
                "opencode",
                EvidencePolicy::OpenCodeReceipts,
            ),
            (Harness::Omp, "omp", EvidencePolicy::AttemptOnly),
        ] {
            assert_eq!(harness.as_str(), name);
            assert_eq!(Harness::parse(name).unwrap(), harness);
            assert_eq!(harness.policy(), policy);
        }
    }

    #[test]
    fn the_ledger_core_grades_by_policy_not_harness_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let profile = Profile::new(Harness::Codex, EvidencePolicy::OpenCodeReceipts);
        let mut ledger = open_with(tmp.path(), profile);
        let permit = permit(&mut ledger, "session-main", FILE_A);

        assert_eq!(
            ledger.record(&permit.fence(), Evidence::Persisted).unwrap(),
            Landed::Recorded(Phase::Persisted)
        );
        assert!(ledger.record(&permit.fence(), Evidence::Consumed).is_err());

        let bytes = fs::read(tmp.path().join(LEDGER_FILE)).unwrap();
        let record: Record = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record.harness, "codex");
    }

    #[test]
    fn attempt_only_holds_an_attempt_across_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Pi);
        permit(&mut ledger, "stable", FILE_A);

        let mut reopened = open(tmp.path(), Harness::Pi);
        assert!(reopened.quarantined().is_none());
        assert_eq!(
            held(&mut reopened, "stable", FILE_A),
            HoldReason::AmbiguousAttempt
        );
    }

    #[test]
    fn codex_and_opencode_compact_wire_bytes_are_stable() {
        let codex_dir = tempfile::tempdir().unwrap();
        let mut codex = open(codex_dir.path(), Harness::Codex);
        let codex_permit = permit(&mut codex, "thread-main", FILE_A);
        codex
            .record(&codex_permit.fence(), Evidence::Consumed)
            .unwrap();
        assert_eq!(
            String::from_utf8(fs::read(codex_dir.path().join(LEDGER_FILE)).unwrap()).unwrap(),
            format!(
                r#"{{"schema":"st2.delivery-ledger.v1","harness":"codex","agent":"h.worker","runtimeId":"h.worker.runtime","entries":[{{"filename":"1786380000000-aaa111.md","binding":"thread-main","correlation":{{"value":"thread-main:1786380000000-aaa111.md"}},"attemptToken":"{}","phase":"consumed","attestation":"observed","incarnation":"incarnation-1"}}]}}"#,
                codex_permit.token()
            )
        );

        let opencode_dir = tempfile::tempdir().unwrap();
        let mut opencode = open(opencode_dir.path(), Harness::OpenCode);
        let opencode_permit = permit(&mut opencode, "session-main", FILE_A);
        opencode
            .record(&opencode_permit.fence(), Evidence::Persisted)
            .unwrap();
        assert_eq!(
            String::from_utf8(fs::read(opencode_dir.path().join(LEDGER_FILE)).unwrap()).unwrap(),
            format!(
                r#"{{"schema":"st2.delivery-ledger.v1","harness":"opencode","agent":"h.worker","runtimeId":"h.worker.runtime","entries":[{{"filename":"1786380000000-aaa111.md","binding":"session-main","correlation":{{"value":"session-main:1786380000000-aaa111.md"}},"attemptToken":"{}","phase":"persisted","attestation":"observed","incarnation":"incarnation-1"}}]}}"#,
                opencode_permit.token()
            )
        );
    }

    /// Renamed from `begin_persists_attempted_before_transport`: the property is unchanged, but
    /// the split `retry` + `begin` pair it named is gone — one claim now both authorizes and
    /// writes, so the permit's existence IS the durability.
    #[test]
    fn claim_persists_attempted_before_it_returns_a_permit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let permit = permit(&mut ledger, "thread-main", FILE_A);
        assert_eq!(permit.attempt().phase, Phase::Attempted);

        let mut reopened = open(tmp.path(), Harness::Codex);
        assert_eq!(
            reopened.snapshot().unwrap().attempt(FILE_A).unwrap(),
            permit.attempt()
        );
        assert_eq!(
            held(&mut reopened, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );
    }

    #[test]
    fn tokens_and_digests_parse_only_canonical_lowercase_hex() {
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..16 {
            let token = AttemptToken::mint().unwrap();
            let hex = token.hex();
            assert_eq!(hex.len(), 32);
            assert_eq!(AttemptToken::parse(&hex).unwrap(), token);
            assert!(seen.insert(hex), "a minted token repeated");
        }
        let token = AttemptToken::mint().unwrap();
        for junk in [
            String::new(),
            "zz".to_owned(),
            "0".repeat(31),
            "0".repeat(33),
            "g".repeat(32),
            token.hex().to_uppercase(),
        ] {
            assert!(
                AttemptToken::parse(&junk).is_err(),
                "{junk:?} is not a token"
            );
        }

        let digest = LedgerDigest::of(b"bytes");
        assert_eq!(digest.hex().len(), 64);
        assert_eq!(LedgerDigest::parse(&digest.hex()).unwrap(), digest);
        for junk in [
            String::new(),
            "0".repeat(63),
            digest.hex().to_uppercase(),
            token.hex(),
        ] {
            assert!(
                LedgerDigest::parse(&junk).is_err(),
                "{junk:?} is not a digest"
            );
        }
    }

    /// The point of the token: an operation whose attempt no longer exists lands on NOTHING.
    #[test]
    fn a_stale_fence_lands_on_nothing_for_evidence_negatives_and_prune() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let stale = permit(&mut ledger, "thread-main", FILE_A).fence();

        // The recipient archives it, so the attempt is gone.
        assert_eq!(ledger.prune(std::slice::from_ref(&stale)).unwrap(), 1);

        // A newer pass legitimately opens the same filename again, with a fresh token.
        let fresh = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_ne!(fresh.token, stale.token);

        // Every operation still carrying the old fence is inert.
        assert_eq!(
            ledger.record(&stale, Evidence::Consumed).unwrap(),
            Landed::Stale
        );
        assert_eq!(
            ledger.negative(&stale, NegativeReceipt::Absent).unwrap(),
            Landed::Stale
        );
        assert_eq!(ledger.prune(&[stale]).unwrap(), 0);

        let snapshot = ledger.snapshot().unwrap();
        let attempt = snapshot.attempt(FILE_A).unwrap();
        assert_eq!(attempt.token, fresh.token);
        assert_eq!(attempt.phase, Phase::Attempted);
        assert!(attempt.negative.is_none());
    }

    /// A negative receipt re-authorizes the identity, and the re-claim rotates the token — so the
    /// refused attempt's own fence can no longer settle the successor it never made.
    #[test]
    fn a_negative_receipt_rotates_the_token_it_reopened() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let refused = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_eq!(
            ledger.negative(&refused, NegativeReceipt::Absent).unwrap(),
            Landed::Recorded(Phase::Attempted)
        );

        let retried = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_ne!(retried.token, refused.token);

        // A late "consumed" for the refused attempt cannot settle the retry.
        assert_eq!(
            ledger.record(&refused, Evidence::Consumed).unwrap(),
            Landed::Stale
        );
        assert_eq!(
            ledger.snapshot().unwrap().retention(FILE_A),
            Retention::Hold(HoldReason::AmbiguousAttempt)
        );

        // The retry's own receipt settles it.
        assert_eq!(
            ledger.record(&retried, Evidence::Consumed).unwrap(),
            Landed::Recorded(Phase::Consumed)
        );
        assert_eq!(
            ledger.snapshot().unwrap().retention(FILE_A),
            Retention::Release
        );
    }

    #[test]
    fn an_outstanding_head_blocks_every_later_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let head = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_B),
            HoldReason::OutstandingHead
        );

        // Even a settled head blocks: ownership is released by the recipient's archive, and the
        // ledger learns that through `prune`, not through a phase.
        ledger.record(&head, Evidence::Consumed).unwrap();
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_B),
            HoldReason::OutstandingHead
        );

        assert_eq!(ledger.prune(&[head]).unwrap(), 1);
        assert_eq!(
            permit(&mut ledger, "thread-main", FILE_B).filename(),
            FILE_B
        );
    }

    /// Renamed from `rebind_discards_receipts_from_another_binding`, and narrowed to what is
    /// actually safe: a SETTLED foreign receipt is droppable.
    #[test]
    fn retarget_drops_settled_receipts_from_another_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let old = permit(&mut ledger, "thread-old", FILE_A).fence();
        ledger.record(&old, Evidence::Consumed).unwrap();

        assert_eq!(ledger.retarget("thread-new").unwrap(), Retarget::Dropped(1));
        assert!(ledger.snapshot().unwrap().attempts().is_empty());
        assert_eq!(
            permit(&mut ledger, "thread-new", FILE_A).binding(),
            "thread-new"
        );
    }

    /// The unsafe half of the old `rebind`, now refused: an ambiguous attempt on the previous
    /// binding MAY have landed, so nothing may delete it and nothing may deliver it again.
    #[test]
    fn a_binding_change_never_discards_an_ambiguous_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let ambiguous = permit(&mut ledger, "thread-old", FILE_A).fence();

        assert_eq!(
            ledger.retarget("thread-new").unwrap(),
            Retarget::Retained(1)
        );
        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(snapshot.attempts().len(), 1);
        assert_eq!(snapshot.binding(), Some("thread-old"));
        assert_eq!(
            held(&mut ledger, "thread-new", FILE_A),
            HoldReason::ForeignBinding
        );

        // The old binding's own authoritative absence is what makes it droppable.
        ledger
            .negative(&ambiguous, NegativeReceipt::Absent)
            .unwrap();
        assert_eq!(ledger.retarget("thread-new").unwrap(), Retarget::Dropped(1));
        assert_eq!(
            permit(&mut ledger, "thread-new", FILE_A).binding(),
            "thread-new"
        );
    }

    /// Prune deletes only what the caller actually saw. The failure it bounds: a pump lists the
    /// inbox, is descheduled while another pass opens a new attempt for the same filename, and
    /// comes back to delete it.
    #[test]
    fn prune_removes_only_the_snapshot_candidates_it_still_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let observed = permit(&mut ledger, "thread-main", FILE_A).fence();

        // The stale candidate list is computed here…
        let candidates = vec![observed];
        // …and a newer claim replaces the attempt before the prune runs.
        ledger
            .negative(&candidates[0], NegativeReceipt::Absent)
            .unwrap();
        let newer = permit(&mut ledger, "thread-main", FILE_A).fence();

        assert_eq!(ledger.prune(&candidates).unwrap(), 0);
        assert_eq!(
            ledger.snapshot().unwrap().attempt(FILE_A).unwrap().token,
            newer.token,
            "a stale unread snapshot must not delete a newer claim"
        );
        assert_eq!(ledger.prune(&[newer]).unwrap(), 1);
    }

    #[test]
    fn the_snapshot_digest_is_the_exact_raw_ledger_sha256() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        permit(&mut ledger, "thread-main", FILE_A);

        let raw = fs::read(tmp.path().join(LEDGER_FILE)).unwrap();
        assert_eq!(ledger.snapshot().unwrap().digest(), LedgerDigest::of(&raw));
        assert_eq!(ledger.snapshot().unwrap().digest().hex().len(), 64);
    }

    #[test]
    fn the_marker_is_the_keyed_grammar_over_only_the_filename_and_token() {
        let token = AttemptToken::mint().unwrap();
        let rendered = marker(FILE_A, token).unwrap();
        assert_eq!(
            rendered,
            format!("[st2-delivery filename={FILE_A} attempt={token}]")
        );
        assert_eq!(parse_marker(&rendered), Some((FILE_A.to_owned(), token)));
        assert_eq!(
            parse_marker(&format!("  {rendered}\n")),
            Some((FILE_A.to_owned(), token))
        );

        assert!(marker("notes.md", token).is_err());
        for junk in [
            "",
            "[st2-delivery]",
            &format!("[st2-delivery filename={FILE_A}]"),
            &format!("st2-delivery filename={FILE_A} attempt={token}"),
            &format!("[st2-delivery attempt={token} filename={FILE_A}]"),
            &format!("[st2-delivery filename=notes.md attempt={token}]"),
            &format!("[st2-delivery filename={FILE_A} attempt=not-a-token]"),
            // Uppercase is a different spelling of the same bytes, and the token has ONE
            // canonical spelling.
            &format!(
                "[st2-delivery filename={FILE_A} attempt={}]",
                token.hex().to_uppercase()
            ),
            &format!("[st2-delivery filename={FILE_A} attempt={token} extra=1]"),
        ] {
            assert_eq!(parse_marker(junk), None, "{junk:?} is not a marker");
        }
    }

    /// The safety property the whole record boundary rests on: a carried-forward `Attempted`
    /// phase is a bound on what already happened, so it holds the delivery, and no harness
    /// profile proves `Attempted`, so it authorizes no transport. The phase does that work; the
    /// attestation only records who saw it. This build's own observation — here an authoritative
    /// absence — is what clears the hold.
    #[test]
    fn an_asserted_phase_suppresses_a_duplicate_and_authorizes_no_transport() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        seed(
            &mut ledger,
            vec![asserted(
                FILE_A.to_owned(),
                "thread-main".to_owned(),
                Correlation::native(correlation("thread-main", FILE_A)),
                Phase::Attempted,
                Some("incarnation-0".to_owned()),
            )],
        );
        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(
            snapshot.retention(FILE_A),
            Retention::Hold(HoldReason::AmbiguousAttempt)
        );
        assert_eq!(
            snapshot.authorization("thread-main", FILE_A),
            Authorization::Held(HoldReason::AmbiguousAttempt)
        );
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );

        // The seed is durable, so a restart still holds instead of re-sending.
        let mut reopened = open(tmp.path(), Harness::Codex);
        let snapshot = reopened.snapshot().unwrap();
        let carried = snapshot.attempt(FILE_A).unwrap();
        assert_eq!(carried.attestation, Attestation::Asserted);
        assert_eq!(
            held(&mut reopened, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );

        // An authoritative absence re-authorizes one transport of the same identity — with a
        // fresh token. It does NOT relabel the phase: the phase is still the one this build never
        // watched, so a crash between here and the re-claim keeps counting it truthfully.
        let fence = carried.fence();
        assert_eq!(
            reopened.negative(&fence, NegativeReceipt::Absent).unwrap(),
            Landed::Recorded(Phase::Attempted)
        );
        let snapshot = reopened.snapshot().unwrap();
        assert_eq!(
            snapshot.attempt(FILE_A).unwrap().attestation,
            Attestation::Asserted,
            "an absence observes the message, never the carried-forward phase"
        );
        assert_eq!(
            snapshot.authorization("thread-main", FILE_A),
            Authorization::Permitted
        );
        let retried = permit(&mut reopened, "thread-main", FILE_A).fence();
        assert_ne!(retried.token, fence.token);

        // Only a GRADED POSITIVE receipt relabels the phase: at that point this build has
        // actually watched the evidence the phase names.
        reopened.record(&retried, Evidence::Consumed).unwrap();
        let snapshot = reopened.snapshot().unwrap();
        assert_eq!(
            snapshot.attempt(FILE_A).unwrap().attestation,
            Attestation::Observed
        );
        assert_eq!(snapshot.retention(FILE_A), Retention::Release);
    }

    /// Q35: the one bounded compatibility exception. A tokenless canonical v1 row becomes
    /// fenceable under the transaction lock, deterministically — two processes derive the SAME
    /// token, so neither fences the other out — and it is persisted before anything else acts.
    #[test]
    fn tokenless_canonical_v1_entries_are_backfilled_deterministically_and_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        let mut ledger = open(tmp.path(), Harness::Codex);
        seed(
            &mut ledger,
            vec![asserted(
                FILE_A.to_owned(),
                "thread-main".to_owned(),
                Correlation::native(correlation("thread-main", FILE_A)),
                Phase::Attempted,
                None,
            )],
        );
        assert_eq!(tokenless_entries(&path).unwrap(), 1);

        let mut first = open(tmp.path(), Harness::Codex);
        assert_eq!(first.backfilled(), 1);
        let derived = first.snapshot().unwrap().attempt(FILE_A).unwrap().token;
        // Persisted before anything else may act on it.
        assert_eq!(tokenless_entries(&path).unwrap(), 0);
        assert!(fs::read_to_string(&path).unwrap().contains(&derived.hex()));

        // Deterministic: another process reading the same row derives the same fence.
        seed(
            &mut first,
            vec![asserted(
                FILE_A.to_owned(),
                "thread-main".to_owned(),
                Correlation::native(correlation("thread-main", FILE_A)),
                Phase::Attempted,
                None,
            )],
        );
        let mut second = open(tmp.path(), Harness::Codex);
        assert_eq!(
            second.snapshot().unwrap().attempt(FILE_A).unwrap().token,
            derived
        );

        // And a NEW mutation gets fresh randomness, never a derived token.
        let backfilled = second.snapshot().unwrap().attempt(FILE_A).unwrap().fence();
        second
            .negative(&backfilled, NegativeReceipt::Absent)
            .unwrap();
        assert_ne!(permit(&mut second, "thread-main", FILE_A).token(), derived);
        assert_eq!(
            second.backfilled(),
            1,
            "the backfill count is not re-counted"
        );
    }

    #[test]
    fn the_persisted_ledger_is_owner_only_and_leaves_no_temp_residue() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let permit = permit(&mut ledger, "thread-main", FILE_A);
        ledger
            .record(&permit.fence(), Evidence::TransportAccepted)
            .unwrap();

        for name in [LEDGER_FILE, LEDGER_LOCK] {
            let mode = fs::metadata(tmp.path().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{name} is world-readable");
        }

        let residue = fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            // By staging-name PREFIX, not by a `.tmp` suffix: the suffix is this helper's own
            // spelling, and a filter that only matches its current spelling stops testing the
            // moment the spelling changes.
            .filter(|name| name.starts_with(".delivery-ledger"))
            .collect::<Vec<_>>();
        assert!(residue.is_empty(), "temp residue left behind: {residue:?}");
    }

    /// The directory sync is strict since the fold onto `fsatomic`: a parent that cannot be
    /// opened for it fails the publication, where it used to be swallowed.
    ///
    /// Real only for a non-root uid; the hermetic gate runs as the sandbox's unprivileged build
    /// user, and a local root run skips the edge instead of asserting what root cannot observe.
    #[test]
    fn a_directory_that_cannot_be_synced_fails_the_publication() {
        use std::os::unix::fs::PermissionsExt as _;

        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("agent");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(LEDGER_FILE);
        let record = Record {
            schema: LEDGER_SCHEMA.to_owned(),
            harness: "codex".to_owned(),
            agent: "h.worker".to_owned(),
            runtime_id: "runtime".to_owned(),
            entries: Vec::new(),
        };

        // Write and traverse, but not read: staging and renaming still work, opening the
        // directory to sync it does not.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o300)).unwrap();
        let published = atomic_json(&path, &record);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            published.is_err(),
            "the ledger's directory sync is strict: {published:?}"
        );
        // The bytes did land — the rename happens before the sync — so the failure is a report
        // about durability, not about the record's contents.
        assert!(path.exists(), "the record still landed");
    }

    #[test]
    fn positive_evidence_never_downgrades() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let fence = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_eq!(
            ledger.record(&fence, Evidence::Consumed).unwrap(),
            Landed::Recorded(Phase::Consumed)
        );
        assert_eq!(
            ledger.record(&fence, Evidence::TransportAccepted).unwrap(),
            Landed::Unchanged(Phase::Consumed)
        );
        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(snapshot.attempt(FILE_A).unwrap().phase, Phase::Consumed);
        assert_eq!(snapshot.retention(FILE_A), Retention::Release);
    }

    #[test]
    fn negative_receipt_is_the_only_retry_authority() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let first = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );
        ledger.negative(&first, NegativeReceipt::Absent).unwrap();
        let second = permit(&mut ledger, "thread-main", FILE_A).fence();
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );

        // A settled delivery is never reopened by a late refusal.
        ledger.record(&second, Evidence::Consumed).unwrap();
        assert_eq!(
            ledger.negative(&second, NegativeReceipt::Rejected).unwrap(),
            Landed::Unchanged(Phase::Consumed)
        );
        assert_eq!(
            held(&mut ledger, "thread-main", FILE_A),
            HoldReason::Settled
        );
    }

    fn audit(reason: &str, digest: LedgerDigest) -> OperatorAudit {
        OperatorAudit::new(
            OperatorSource::Operator,
            1000,
            Some(4242),
            Some("h.operator".to_owned()),
            reason,
            digest,
            1_786_380_000_000,
        )
        .unwrap()
    }

    /// The operator boundary: digest AND full fence, compared under the lock that publishes, the
    /// digest RETAINED in the record, every refusal byte-identical, and the phase attestation
    /// untouched.
    #[test]
    fn an_operator_refusal_is_fenced_by_the_exact_bytes_and_the_exact_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        let mut ledger = open(tmp.path(), Harness::Codex);
        let fence = permit(&mut ledger, "thread-main", FILE_A).fence();
        // The attempt reached the transport but nothing proved the model saw it: the ordinary
        // case an operator is asked to unstick, and one whose phase provenance is `Observed`.
        ledger.record(&fence, Evidence::TransportAccepted).unwrap();
        let digest = ledger.snapshot().unwrap().digest();

        // A digest from bytes this ledger never held is stale: the operator decided against
        // something else.
        let before = fs::read(&path).unwrap();
        let stale = OperatorRefusal {
            fence: fence.clone(),
            audit: audit("stale", LedgerDigest::of(b"{}")),
        };
        assert_eq!(
            ledger.operator_refuse(&stale).unwrap(),
            OperatorOutcome::StaleDigest { observed: digest }
        );
        assert_eq!(fs::read(&path).unwrap(), before, "a refusal wrote");

        // The right filename and the right digest, but a foreign token: matches nothing.
        let foreign = OperatorRefusal {
            fence: Fence {
                token: AttemptToken::mint().unwrap(),
                ..fence.clone()
            },
            audit: audit("foreign token", digest),
        };
        assert_eq!(
            ledger.operator_refuse(&foreign).unwrap(),
            OperatorOutcome::UnknownAttempt
        );
        assert_eq!(fs::read(&path).unwrap(), before, "a refusal wrote");

        // The exact fence and the exact bytes: correlated absence lands, with its audit.
        let applied = OperatorRefusal {
            fence: fence.clone(),
            audit: audit("  operator forced after a stuck turn  ", digest),
        };
        let OperatorOutcome::Applied(attempt) = ledger.operator_refuse(&applied).unwrap() else {
            panic!("the exact fence must apply");
        };
        assert_eq!(attempt.negative, Some(NegativeReceipt::Absent));
        assert_eq!(
            attempt.attestation,
            Attestation::Observed,
            "the operator asserted an absence, not a phase: DELTA-006 must not gain a row"
        );
        assert_eq!(attempt.phase, Phase::TransportAccepted);
        let recorded = attempt.audit.unwrap();
        assert_eq!(recorded.reason, "operator forced after a stuck turn");
        assert_eq!(recorded.uid, 1000);
        assert_eq!(recorded.pid, Some(4242));
        assert_eq!(recorded.st_agent.as_deref(), Some("h.operator"));
        assert_eq!(recorded.observed_at_ms, 1_786_380_000_000);
        assert_eq!(
            recorded.digest, digest,
            "the precondition is retained, not only checked"
        );
        // And it is durable in canonical hex, so the record answers "what was the operator
        // looking at" without the caller having kept it.
        assert!(fs::read_to_string(&path).unwrap().contains(&digest.hex()));

        // Re-stating the absence changes nothing, so an operator cannot rewrite the audit of a
        // refusal already recorded.
        let repeat = OperatorRefusal {
            fence,
            audit: audit("second thoughts", ledger.snapshot().unwrap().digest()),
        };
        let refused_bytes = fs::read(&path).unwrap();
        assert!(matches!(
            ledger.operator_refuse(&repeat).unwrap(),
            OperatorOutcome::AlreadyRefused(_)
        ));
        assert_eq!(fs::read(&path).unwrap(), refused_bytes);

        // The absence reopened the identity; the retry rotates the token and, once settled, the
        // operator can no longer refuse it at all.
        let retried = permit(&mut ledger, "thread-main", FILE_A).fence();
        ledger.record(&retried, Evidence::Consumed).unwrap();
        let too_late = OperatorRefusal {
            fence: retried,
            audit: audit("too late", ledger.snapshot().unwrap().digest()),
        };
        let consumed_bytes = fs::read(&path).unwrap();
        assert!(matches!(
            ledger.operator_refuse(&too_late).unwrap(),
            OperatorOutcome::AlreadySettled(_)
        ));
        assert_eq!(fs::read(&path).unwrap(), consumed_bytes);

        // A tokenless Q35 row carries no fence, so an operator refusal matches nothing AND
        // rewrites nothing: the backfill belongs to an ordinary provider mutation, and running it
        // here would rewrite bytes the precondition was taken against.
        let carried_dir = tempfile::tempdir().unwrap();
        let carried_path = carried_dir.path().join(LEDGER_FILE);
        let mut carried = open(carried_dir.path(), Harness::Codex);
        seed(
            &mut carried,
            vec![asserted(
                FILE_A.to_owned(),
                "thread-main".to_owned(),
                Correlation::native(correlation("thread-main", FILE_A)),
                Phase::Attempted,
                None,
            )],
        );
        let tokenless_bytes = fs::read(&carried_path).unwrap();
        let guessed = OperatorRefusal {
            fence: Fence {
                filename: FILE_A.to_owned(),
                binding: "thread-main".to_owned(),
                correlation: Correlation::native(correlation("thread-main", FILE_A)),
                token: AttemptToken::mint().unwrap(),
            },
            audit: audit("tokenless", LedgerDigest::of(&tokenless_bytes)),
        };
        assert_eq!(
            carried.operator_refuse(&guessed).unwrap(),
            OperatorOutcome::UnknownAttempt
        );
        assert_eq!(
            fs::read(&carried_path).unwrap(),
            tokenless_bytes,
            "the operator path neither recovers nor backfills"
        );
        assert_eq!(tokenless_entries(&carried_path).unwrap(), 1);

        // An absent ledger is an ERROR, never an initialization: there is nothing to refuse.
        let empty = tempfile::tempdir().unwrap();
        let mut absent = open(empty.path(), Harness::Codex);
        assert!(absent.operator_refuse(&guessed).is_err());
        assert!(!empty.path().join(LEDGER_FILE).exists());

        // `for_operator` is configuration only: constructing it touches nothing, which is what
        // lets the operator path check its precondition before anything could have been written.
        let untouched = tempfile::tempdir().unwrap();
        let state = untouched.path().join("state");
        let _config_only = Ledger::for_operator(
            &state.join(LEDGER_FILE),
            Harness::Codex.profile(),
            "h.worker",
            correlation,
        );
        assert!(!state.exists(), "for_operator created a directory");

        // A reason is required, and it is bounded.
        assert!(
            OperatorAudit::new(OperatorSource::Operator, 0, None, None, "   ", digest, 0).is_err()
        );
        assert!(
            OperatorAudit::new(
                OperatorSource::Operator,
                0,
                None,
                None,
                &"x".repeat(OPERATOR_REASON_MAX_BYTES + 1),
                digest,
                0
            )
            .is_err()
        );
    }

    #[test]
    fn later_provider_evidence_clears_operator_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let first = permit(&mut ledger, "thread-main", FILE_A).fence();
        let refusal = OperatorRefusal {
            fence: first.clone(),
            audit: audit("lost response", ledger.snapshot().unwrap().digest()),
        };
        ledger.operator_refuse(&refusal).unwrap();
        ledger.record(&first, Evidence::Consumed).unwrap();
        let superseded = ledger.snapshot().unwrap().attempt(FILE_A).unwrap().clone();
        assert!(superseded.negative.is_none());
        assert!(superseded.audit.is_none());

        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let second = permit(&mut ledger, "thread-main", FILE_A).fence();
        let refusal = OperatorRefusal {
            fence: second.clone(),
            audit: audit("lost response", ledger.snapshot().unwrap().digest()),
        };
        ledger.operator_refuse(&refusal).unwrap();
        ledger.negative(&second, NegativeReceipt::Rejected).unwrap();
        let superseded = ledger.snapshot().unwrap().attempt(FILE_A).unwrap().clone();
        assert_eq!(superseded.negative, Some(NegativeReceipt::Rejected));
        assert!(superseded.audit.is_none());
    }

    #[test]
    fn the_operator_hold_vocabulary_is_bounded_and_owned_here() {
        for (reason, word) in [
            (HoldReason::AmbiguousAttempt, "ambiguousAttempt"),
            (HoldReason::UnreadReceipt, "unreadReceipt"),
            (HoldReason::NegativeReceipt, "negativeReceipt"),
            (HoldReason::Settled, "settled"),
            (HoldReason::OutstandingHead, "outstandingHead"),
            (HoldReason::ForeignBinding, "foreignBinding"),
        ] {
            assert_eq!(reason.as_str(), word);
        }
        for harness in [
            Harness::Claude,
            Harness::Codex,
            Harness::Pi,
            Harness::OpenCode,
            Harness::Omp,
        ] {
            assert_eq!(Harness::parse(harness.as_str()).unwrap(), harness);
        }
    }

    #[test]
    fn opencode_persistence_holds_until_archive() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        let fence = permit(&mut ledger, "ses-main", FILE_A).fence();
        ledger.record(&fence, Evidence::Persisted).unwrap();
        assert_eq!(
            ledger.snapshot().unwrap().retention(FILE_A),
            Retention::Hold(HoldReason::UnreadReceipt)
        );
        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(ledger.prune(&fences(&snapshot)).unwrap(), 1);
        assert!(ledger.snapshot().unwrap().attempt(FILE_A).is_none());
    }

    #[test]
    fn correlated_returns_every_matching_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        let first = permit(&mut ledger, "thread-main", FILE_A).fence();
        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(
            snapshot
                .correlated(&first.correlation.value)
                .iter()
                .map(|attempt| attempt.filename.as_str())
                .collect::<Vec<_>>(),
            [FILE_A]
        );
        assert!(
            snapshot
                .correlated(&correlation("thread-main", FILE_B))
                .is_empty()
        );
    }

    #[test]
    fn foreign_or_malformed_state_quarantines_without_rewriting() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        fs::write(&path, b"not json").unwrap();
        let mut malformed = open(tmp.path(), Harness::Codex);
        assert!(malformed.quarantined().is_some());
        // Fail closed on EVERY operation: the bytes are re-read under the lock each time, so a
        // mutation cannot slip past a stale in-memory verdict.
        assert!(malformed.claim(claimant("thread-main", FILE_A)).is_err());
        assert!(malformed.snapshot().is_err());
        assert!(malformed.retarget("thread-main").is_err());
        assert_eq!(fs::read(&path).unwrap(), b"not json");

        atomic_json(
            &path,
            &Record {
                schema: LEDGER_SCHEMA.to_owned(),
                harness: "opencode".to_owned(),
                agent: "h.worker".to_owned(),
                runtime_id: "runtime".to_owned(),
                entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(open(tmp.path(), Harness::Codex).quarantined().is_some());
    }

    #[test]
    fn every_entry_must_validate_its_own_correlation() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        atomic_json(
            &path,
            &Record {
                schema: LEDGER_SCHEMA.to_owned(),
                harness: "codex".to_owned(),
                agent: "h.worker".to_owned(),
                runtime_id: "runtime".to_owned(),
                entries: vec![Row {
                    filename: FILE_A.to_owned(),
                    binding: "thread-main".to_owned(),
                    correlation: Correlation::native("injected"),
                    attempt_token: Some(AttemptToken::mint().unwrap()),
                    phase: Phase::Attempted,
                    attestation: Attestation::Observed,
                    incarnation: None,
                    negative: None,
                    operator_audit: None,
                }],
            },
        )
        .unwrap();
        assert!(open(tmp.path(), Harness::Codex).quarantined().is_some());
    }

    #[test]
    fn non_adjacent_duplicate_filenames_are_rejected_without_rewriting() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        let row = |filename: &str| Row {
            filename: filename.to_owned(),
            binding: "thread-main".to_owned(),
            correlation: Correlation::native(correlation("thread-main", filename)),
            attempt_token: Some(AttemptToken::mint().unwrap()),
            phase: Phase::Attempted,
            attestation: Attestation::Observed,
            incarnation: None,
            negative: None,
            operator_audit: None,
        };
        let record = Record {
            schema: LEDGER_SCHEMA.to_owned(),
            harness: "codex".to_owned(),
            agent: "h.worker".to_owned(),
            runtime_id: "runtime".to_owned(),
            entries: vec![row(FILE_A), row(FILE_B), row(FILE_A)],
        };
        atomic_json(&path, &record).unwrap();
        let bytes = fs::read(&path).unwrap();

        let ledger = open(tmp.path(), Harness::Codex);
        assert!(ledger.quarantined().is_some());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn observation_distinguishes_absent_held_and_indeterminate_without_writing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LEDGER_FILE);
        let profile = Harness::Codex.profile();
        let correlate: &dyn Fn(&str, &str) -> String = &correlation;

        // Absent: nothing here, and looking creates nothing — not even the lock.
        assert!(matches!(
            observe(&path, profile, "h.worker", correlate).unwrap(),
            Observation::Absent
        ));
        assert!(!tmp.path().join(LEDGER_LOCK).exists());

        let mut ledger = open(tmp.path(), Harness::Codex);
        let fence = permit(&mut ledger, "thread-main", FILE_A).fence();
        let raw = fs::read(&path).unwrap();
        let Observation::Held(sighting) = observe(&path, profile, "h.worker", correlate).unwrap()
        else {
            panic!("a readable ledger is held");
        };
        assert_eq!(sighting.digest, LedgerDigest::of(&raw));
        assert_eq!(sighting.binding.as_deref(), Some("thread-main"));
        assert_eq!(sighting.tokenless, 0);
        assert_eq!(sighting.asserted, 0);
        assert_eq!(sighting.attempts.len(), 1);
        assert_eq!(sighting.attempts[0].token, Some(fence.token));
        assert_eq!(
            sighting.attempts[0].retention,
            Retention::Hold(HoldReason::AmbiguousAttempt)
        );

        // A tokenless canonical v1 row is REPORTED, never repaired.
        seed(
            &mut ledger,
            vec![asserted(
                FILE_A.to_owned(),
                "thread-main".to_owned(),
                Correlation::native(correlation("thread-main", FILE_A)),
                Phase::Attempted,
                None,
            )],
        );
        let before = fs::read(&path).unwrap();
        let Observation::Held(sighting) = observe(&path, profile, "h.worker", correlate).unwrap()
        else {
            panic!("a tokenless ledger is still readable");
        };
        assert_eq!(sighting.tokenless, 1);
        assert_eq!(sighting.asserted, 1);
        assert_eq!(sighting.attempts[0].token, None);
        assert_eq!(fs::read(&path).unwrap(), before, "observation wrote");

        // Indeterminate: bytes exist and this build cannot read them as its own.
        fs::write(&path, b"not json").unwrap();
        let Observation::Indeterminate { digest, reason } =
            observe(&path, profile, "h.worker", correlate).unwrap()
        else {
            panic!("unreadable bytes are indeterminate, never healthy");
        };
        assert_eq!(digest, LedgerDigest::of(b"not json"));
        assert!(!reason.is_empty());
        assert_eq!(fs::read(&path).unwrap(), b"not json");

        // A foreign harness is indeterminate for THIS profile too, not silently empty.
        atomic_json(
            &path,
            &Record {
                schema: LEDGER_SCHEMA.to_owned(),
                harness: "opencode".to_owned(),
                agent: "h.worker".to_owned(),
                runtime_id: "runtime".to_owned(),
                entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(matches!(
            observe(&path, profile, "h.worker", correlate).unwrap(),
            Observation::Indeterminate { .. }
        ));
    }

    // ---- real-process transaction proofs ------------------------------------------------------

    /// A child process, held at a barrier before it enters the transaction.
    ///
    /// POSIX advisory locks are process-scoped, so only real children can prove that two claims
    /// for one filename cannot both be authorized. Modelled on
    /// `resource_profile::tests::PublicationWorker`, including its crash-stage plumbing: no
    /// sleeps, only a stdin release and a line-oriented transcript.
    struct ClaimWorker {
        child: Child,
        input: Option<ChildStdin>,
        output: BufReader<ChildStdout>,
        transcript: String,
    }

    impl ClaimWorker {
        fn start(dir: &Path, filename: &str, binding: &str, crash_stage: Option<&str>) -> Self {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "delivery_ledger::tests::delivery_claim_process_worker",
                    "--ignored",
                    "--nocapture",
                ])
                .env("ST2_DELIVERY_LEDGER_WORKER_DIR", dir)
                .env("ST2_DELIVERY_LEDGER_WORKER_FILE", filename)
                .env("ST2_DELIVERY_LEDGER_WORKER_BINDING", binding)
                .env_remove("ST2_DELIVERY_LEDGER_CRASH_STAGE")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            if let Some(stage) = crash_stage {
                command.env("ST2_DELIVERY_LEDGER_CRASH_STAGE", stage);
            }
            let mut child = command.spawn().unwrap();
            let input = child.stdin.take().unwrap();
            let mut output = BufReader::new(child.stdout.take().unwrap());
            let mut transcript = String::new();
            loop {
                let mut line = String::new();
                assert_ne!(output.read_line(&mut line).unwrap(), 0, "{transcript}");
                transcript.push_str(&line);
                if line.contains("CLAIM-WORKER-READY") {
                    break;
                }
            }
            Self {
                child,
                input: Some(input),
                output,
                transcript,
            }
        }

        fn release(&mut self) {
            let mut input = self.input.take().unwrap();
            input.write_all(b"x").unwrap();
            drop(input);
        }

        fn finish(mut self) -> (std::process::ExitStatus, String) {
            if self.input.is_some() {
                self.release();
            }
            let status = self.child.wait().unwrap();
            self.output.read_to_string(&mut self.transcript).unwrap();
            (status, self.transcript)
        }
    }

    #[test]
    #[ignore = "subprocess entrypoint for delivery ledger transaction tests"]
    fn delivery_claim_process_worker() {
        let Some(dir) = std::env::var_os("ST2_DELIVERY_LEDGER_WORKER_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let crash_stage = std::env::var_os("ST2_DELIVERY_LEDGER_CRASH_STAGE");
        unsafe {
            std::env::remove_var("ST2_DELIVERY_LEDGER_CRASH_STAGE");
        }
        let filename = std::env::var("ST2_DELIVERY_LEDGER_WORKER_FILE").unwrap();
        let binding = std::env::var("ST2_DELIVERY_LEDGER_WORKER_BINDING").unwrap();
        println!("CLAIM-WORKER-READY");
        std::io::stdout().flush().unwrap();
        let mut release = [0_u8; 1];
        std::io::stdin().read_exact(&mut release).unwrap();
        assert_eq!(release, *b"x");
        let mut ledger = open(&dir, Harness::Codex);
        if let Some(stage) = crash_stage {
            unsafe {
                std::env::set_var("ST2_DELIVERY_LEDGER_CRASH_STAGE", stage);
            }
        }
        let outcome = ledger.claim(claimant(&binding, &filename)).unwrap();
        let rendered = match outcome {
            Claim::Permitted(permit) => format!("Permitted({})", permit.token()),
            Claim::Held(reason) => format!("Held({reason:?})"),
        };
        println!("CLAIM-WORKER-RESULT {rendered}");
        std::io::stdout().flush().unwrap();
    }

    #[test]
    fn two_real_processes_claiming_one_filename_yield_exactly_one_permit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut first = ClaimWorker::start(tmp.path(), FILE_A, "thread-main", None);
        let mut second = ClaimWorker::start(tmp.path(), FILE_A, "thread-main", None);
        first.release();
        second.release();
        let (status_a, output_a) = first.finish();
        let (status_b, output_b) = second.finish();
        assert!(status_a.success(), "{output_a}");
        assert!(status_b.success(), "{output_b}");
        let outputs = format!("{output_a}\n{output_b}");
        assert_eq!(
            outputs.matches("CLAIM-WORKER-RESULT Permitted(").count(),
            1,
            "exactly one permit for one filename: {outputs}"
        );
        assert_eq!(
            outputs
                .matches("CLAIM-WORKER-RESULT Held(AmbiguousAttempt)")
                .count(),
            1,
            "the loser sees the winner's durable attempt: {outputs}"
        );
    }

    /// The lock is what makes the re-read meaningful: a transaction that started while this test
    /// holds the lock cannot see the pre-write bytes, because it cannot start at all.
    ///
    /// Deterministic without a sleep: the child is spawned and released while the parent already
    /// holds the exclusive lock, and the parent seeds a DIFFERENT filename before releasing it. A
    /// child that had not waited would have read an empty ledger and been permitted.
    #[test]
    fn recovery_and_mutation_serialize_on_one_permanent_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let lock_path = tmp.path().join(LEDGER_LOCK);
        let guard = FileLock::hold_blocking(
            flock::open(&lock_path, flock::Open::Create).unwrap(),
            flock::Mode::Exclusive,
        )
        .unwrap();

        let mut worker = ClaimWorker::start(tmp.path(), FILE_B, "thread-main", None);
        worker.release();

        atomic_json(
            &tmp.path().join(LEDGER_FILE),
            &Record {
                schema: LEDGER_SCHEMA.to_owned(),
                harness: "codex".to_owned(),
                agent: "h.worker".to_owned(),
                runtime_id: "h.worker.runtime".to_owned(),
                entries: vec![Row {
                    filename: FILE_A.to_owned(),
                    binding: "thread-main".to_owned(),
                    correlation: Correlation::native(correlation("thread-main", FILE_A)),
                    attempt_token: Some(AttemptToken::mint().unwrap()),
                    phase: Phase::Attempted,
                    attestation: Attestation::Observed,
                    incarnation: None,
                    negative: None,
                    operator_audit: None,
                }],
            },
        )
        .unwrap();
        drop(guard);

        let (status, output) = worker.finish();
        assert!(status.success(), "{output}");
        assert!(
            output.contains("CLAIM-WORKER-RESULT Held(OutstandingHead)"),
            "the child must observe the bytes written under the lock: {output}"
        );
    }

    #[test]
    fn a_crash_inside_a_transaction_leaves_the_ledger_at_a_checkpoint() {
        // Before the act: nothing durable, so the message is still deliverable.
        let early = tempfile::tempdir().unwrap();
        let (status, output) = ClaimWorker::start(
            early.path(),
            FILE_A,
            "thread-main",
            Some("after-load-before-act"),
        )
        .finish();
        assert_eq!(status.code(), Some(71), "{output}");
        assert!(!early.path().join(LEDGER_FILE).exists());
        let mut recovered = open(early.path(), Harness::Codex);
        assert_eq!(
            permit(&mut recovered, "thread-main", FILE_A).filename(),
            FILE_A
        );

        // After the act: the attempt IS durable even though the caller never returned, so the
        // replacement holds instead of sending a message that may already have gone out.
        let late = tempfile::tempdir().unwrap();
        let (status, output) = ClaimWorker::start(
            late.path(),
            FILE_A,
            "thread-main",
            Some("after-act-before-release"),
        )
        .finish();
        assert_eq!(status.code(), Some(72), "{output}");
        let mut replacement = open(late.path(), Harness::Codex);
        let snapshot = replacement.snapshot().unwrap();
        assert_eq!(snapshot.attempt(FILE_A).unwrap().phase, Phase::Attempted);
        assert_eq!(
            held(&mut replacement, "thread-main", FILE_A),
            HoldReason::AmbiguousAttempt
        );
    }
}
