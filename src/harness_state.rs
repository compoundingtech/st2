//! Observed harness state: the driver-owned record of what a harness is seen doing.
//!
//! A `harness-state` file (sibling of `status` in the agent's dir) carries the latest observation a
//! session wrapper made of its provider: whether the harness is working, blocked on a human, or
//! ended, plus what its input buffer holds. This is the *observed* axis; `status` remains the
//! *declared* one, and neither speaks for the other. The record is written only by the owning
//! session's driver processes — the wrapper, its channel, or its hooks; one logical owner per
//! record, and nothing outside the driver writes it. Writes happen on state transitions plus a
//! slow heartbeat and follow the presence record's transport discipline: an embedded origin
//! timestamp (never file mtime), atomic tmp+rename writes serialized by a cross-process lock,
//! byte-distinct content on every write that lands, and a derived-only `unknown` — a writer that
//! loses sight of its harness stops heartbeating and lets the record age out rather than
//! refreshing a state it can no longer prove. Restating an unchanged state is free: it touches
//! the record only when the refresh cadence is due, so a chatty producer cannot flood the
//! transport.

use std::fs;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A valid observation at least this old reads as `unknown`. Deliberately its own constant rather
/// than an alias of [`crate::status::STATUS_STALE`]: retuning presence must not silently retune
/// observed harness state.
pub const HARNESS_STATE_STALE: Duration = Duration::from_secs(15 * 60);
/// How often a live writer re-stamps a record it still has evidence for — the presence cadence, so
/// wrappers piggyback on the wakeup they already own.
pub const HARNESS_STATE_REFRESH: Duration = Duration::from_secs(5 * 60);
/// Maximum accepted positive difference between the writer's UTC clock and the reader's clock.
pub const HARNESS_STATE_FUTURE_SKEW: Duration = Duration::from_secs(60);

/// Schema version 1: the record's `agent` field carries the subject's bus identity.
pub const SCHEMA_V1: &str = "st2.harness-state.v1";
/// Schema version 2: the record's `agent` field carries the subject's immutable agent ID. Nothing
/// else about the record changes — the version exists solely to make the meaning of that one
/// field decidable from the bytes, because the two namespaces are separate and a v1 reader would
/// otherwise retype a frozen legacy ID as a route.
pub const SCHEMA_V2: &str = "st2.harness-state.v2";
/// Schema version 3: the *fault-axis* record. `agent` still means the immutable agent ID and the
/// ownership envelope stays byte-compatible with version 2 — `schema`, `agent`, `harness`,
/// `ptySession`, `incarnation`, `seq`, `sinceMs`, `writtenAtMs`, `transitions` keep their
/// spelling and meaning — so the claim fence and the sequence floor can honor a version 3 record
/// without interpreting it.
///
/// What version 3 adds is the `condition` axis: whether the harness is faulted, in which closed
/// category, whose recovery is automatic or needs a human, on its own SEMANTIC clock
/// (`observedAtMs`, plus an optional `nextObservationDueMs`) rather than the transport heartbeat.
/// What it removes is the overloaded `blockedOn`: a fault is not an ask — a throttled provider
/// asks nobody anything — and an ask is an actual human prompt, carried by a tagged state that
/// says `none`, `pending(kind)`, or `unknown` without any of the three standing in for the
/// others.
pub const SCHEMA_V3: &str = "st2.harness-state.v3";

/// Whether the fault-axis writer is active. **On**: this build emits version 3.
///
/// This is the one and only writer-selection point for this record family, and version 3
/// activation SUPERSEDED the immutable-ID selector that preceded it rather than adding a second
/// switch beside it. The DELTA-003 cohort it used to gate (raw-ID `ST_AGENT`, ID-keyed runtime
/// ownership, message record version 2, PTY schema 2) is subsumed: version 3's `agent` means the
/// immutable agent ID exactly as version 2's did, so turning this off falls back to version 2 and
/// never to version 1 — the cohort cannot be un-activated by this constant.
///
/// The reader-first precondition was met before the flip and stays met after it: [`read`] accepts
/// versions 1, 2, and 3, reports which namespace each `agent` names, and projects a legacy
/// record's condition as explicitly absent rather than as health. The fence is version-independent
/// in both directions — a version 2 claim refuses a version 3 record, and a version 3 claim
/// refuses to be superseded by one — so a mixed fleet during a rollout is safe from either side.
///
/// The constant stays named so exactly one reversal point exists, and the `#[cfg(test)]`
/// [`Writer::with_emitted_schema`] seam keeps version 2 emission provable in this build: legacy
/// records must stay readable and the refused fence direction must stay testable.
pub const EMIT_SCHEMA_V3: bool = true;

/// The version this build writes. Every ownership decision below — sequence adoption, own-record
/// coalescing, heartbeat eligibility — is scoped to it: a writer owns only the shape it emits, so
/// a v2 straggler still refuses to replace a v3 record and vice versa.
const SCHEMA: &str = if EMIT_SCHEMA_V3 { SCHEMA_V3 } else { SCHEMA_V2 };

/// Whether a record's schema is one this version can interpret. Versions 1 and 2 describe the same
/// axes and differ only in the meaning of `agent`, which [`RecordSubject`] carries. Version 3
/// keeps that meaning and replaces the blocked axis with the tagged condition and ask axes, which
/// [`ConditionView`] and [`HumanAsk`] carry. A version outside the three is uninterpretable: its
/// words may be spelled exactly like these while meaning something else.
pub fn is_supported_schema(schema: &str) -> bool {
    schema == SCHEMA_V1 || schema == SCHEMA_V2 || schema == SCHEMA_V3
}

/// The subject a record names, carrying which typed namespace the value belongs to.
///
/// ID and address are separate namespaces in which equal bytes never collide, so a consumer that
/// joins records to a catalog must know which one it is holding. An unmigrated subject's two
/// values are byte-identical, which is why this distinction costs nothing during the transition
/// and everything after the first address cutover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordSubject {
    /// From a version 1 record: the writer's `<host>.<identity>` bus identity.
    BusIdentity(String),
    /// From a version 2 record: the subject's catalog-global immutable agent ID.
    AgentId(String),
}

impl RecordSubject {
    /// Build the subject from the decision the *record's own* schema discriminator already made.
    ///
    /// The caller passes the decision rather than the schema string because the two driver
    /// records version independently: `st2.harness-state.v2` and `st2.harness-context.v2` are
    /// different spellings of the same meaning, and a shared string comparison would silently
    /// read one of them as version 1.
    pub(crate) fn for_version(carries_agent_id: bool, agent: String) -> Self {
        if carries_agent_id {
            Self::AgentId(agent)
        } else {
            Self::BusIdentity(agent)
        }
    }

    /// The declared value, whichever namespace it belongs to. Diagnostics only: never compare
    /// this across namespaces.
    pub fn value(&self) -> &str {
        match self {
            Self::BusIdentity(value) | Self::AgentId(value) => value,
        }
    }

    /// The immutable agent ID, when the record proved one.
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::AgentId(id) => Some(id),
            Self::BusIdentity(_) => None,
        }
    }

    /// The bus identity, when the record proved one.
    pub fn bus_identity(&self) -> Option<&str> {
        match self {
            Self::BusIdentity(identity) => Some(identity),
            Self::AgentId(_) => None,
        }
    }
}

/// The claim-sequence floor sidecar, beside the record: claims stay monotonic even across a
/// record this version cannot parse.
const SEQ_FLOOR_NAME: &str = ".harness-state.seq";
const LOCK_NAME: &str = ".harness-state.lock";

/// What the harness is observed doing. `Child` is reserved: it is part of the contract so a v1
/// reader decodes it, but no producer emits it yet (the screen observer that would have was cut).
/// `Unknown` is DERIVED — staleness, malformation, or a dead session — and is never written; there
/// is no constructor path from missing evidence to `Idle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Activity {
    Idle,
    Active,
    Child,
    /// The session ended or reached a terminal error; nothing further will be observed from this
    /// incarnation without intervention. Unlike the live states, a fresh `Ended` survives the
    /// session-liveness cross-check: a terminal record is *supposed* to outlive its writer.
    Ended,
    #[serde(other)]
    Unknown,
}

impl Activity {
    pub fn as_str(self) -> &'static str {
        match self {
            Activity::Idle => "idle",
            Activity::Active => "active",
            Activity::Child => "child",
            Activity::Ended => "ended",
            Activity::Unknown => "unknown",
        }
    }
}

/// Who the harness is waiting on. `Human` means the model is stopped and a person is the thing
/// that restarts it (a permission prompt, a review, a question) — neither working nor merely idle.
/// Unrecognized future values decode as `Unknown` (indeterminate), never as `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BlockedOn {
    None,
    Human,
    #[serde(other)]
    Unknown,
}

impl BlockedOn {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockedOn::None => "none",
            BlockedOn::Human => "human",
            BlockedOn::Unknown => "unknown",
        }
    }
}

/// What the harness's composer holds. `Unknown` is writable on this axis: "I cannot see the
/// composer" is itself the observation most producers make.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InputBuffer {
    Empty,
    Nonempty,
    #[serde(other)]
    Unknown,
}

impl InputBuffer {
    pub fn as_str(self) -> &'static str {
        match self {
            InputBuffer::Empty => "empty",
            InputBuffer::Nonempty => "nonempty",
            InputBuffer::Unknown => "unknown",
        }
    }
}

/// What kind of human ask holds the harness, machine-readably — consumers filter on this axis
/// (`reason` stays diagnostic-only). Meaningful only while `blockedOn` is `human`; writers set
/// `none` otherwise. `Unknown` decodes future words and is never written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Ask {
    #[default]
    None,
    Permission,
    Question,
    Review,
    #[serde(other)]
    Unknown,
}

impl Ask {
    pub fn as_str(self) -> &'static str {
        match self {
            Ask::None => "none",
            Ask::Permission => "permission",
            Ask::Question => "question",
            Ask::Review => "review",
            Ask::Unknown => "unknown",
        }
    }
}

// ---------------------------------------------------------------------------
// Version 3: the fault axis, the tagged human-ask axis, the conversation
// bridge, typed indeterminacy, and the one shared derived disposition every
// consumer reads instead of deriving its own.
// ---------------------------------------------------------------------------

/// Why a harness cannot make progress, as a CLOSED category. Closed because consumers route on
/// it: a category nobody can enumerate is a category nobody can write a filter, a runbook, or an
/// alert against. Provider vocabulary lives in the open, provider-namespaced [`Fault::code`]
/// beside it and provider prose in [`Fault::detail`]; both stay diagnostic, and no consumer
/// branches on either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultCategory {
    /// Credentials were rejected: a login, token, or key must be repaired.
    Authentication,
    /// The account itself is the obstacle — suspended, unpaid, unentitled.
    Account,
    /// A usage allowance is exhausted for a window.
    Quota,
    /// Requests are being throttled while the allowance itself is intact.
    RateLimit,
    /// The provider failed on its own side: an outage, a 5xx, a withdrawn model.
    Provider,
    /// The harness has no usable context left and cannot proceed as it stands.
    Context,
    /// The seat's own declaration or environment is wrong.
    Configuration,
    /// A policy — provider-side or local — refused the work.
    Policy,
    /// st2's own driver plumbing is the fault.
    Harness,
}

impl FaultCategory {
    /// Parse a category word. `None` for anything outside the closed set, which makes the fault
    /// UNTYPED — still a fault, still routed by its recovery. Both alternatives are worse: mapping
    /// an unknown word onto a neighbouring category invents a claim, and failing the whole record
    /// would make a real fault stop paging because its label was new.
    fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "authentication" => Self::Authentication,
            "account" => Self::Account,
            "quota" => Self::Quota,
            "rateLimit" => Self::RateLimit,
            "provider" => Self::Provider,
            "context" => Self::Context,
            "configuration" => Self::Configuration,
            "policy" => Self::Policy,
            "harness" => Self::Harness,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Authentication => "authentication",
            Self::Account => "account",
            Self::Quota => "quota",
            Self::RateLimit => "rateLimit",
            Self::Provider => "provider",
            Self::Context => "context",
            Self::Configuration => "configuration",
            Self::Policy => "policy",
            Self::Harness => "harness",
        }
    }
}

/// Who or what clears a fault — the urgency axis. `Automatic` is the only value allowed to wait,
/// and only until its own deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// The harness expects to clear this itself: a throttle window, a retried request.
    Automatic,
    /// A person must act.
    Human,
    /// Nothing clears it for this incarnation.
    Terminal,
    /// The producer could not say. Never optimistic: an unsayable recovery is treated exactly like
    /// one that needs a human.
    Unknown,
}

impl Recovery {
    /// An unrecognized future word decodes as `Unknown`, which pages: the conservative direction.
    fn parse(word: &str) -> Self {
        match word {
            "automatic" => Self::Automatic,
            "human" => Self::Human,
            "terminal" => Self::Terminal,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::Human => "human",
            Self::Terminal => "terminal",
            Self::Unknown => "unknown",
        }
    }
}

/// One fault, as st2 normalizes it for every consumer. Normalization happens here, once: the
/// alternative is every reader re-deciding what an overdue automatic recovery means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fault {
    /// `None` = untyped: the record named a category outside the closed set.
    pub category: Option<FaultCategory>,
    /// Provider-namespaced and OPEN (`claude/oauth_expired`, `codex/usage_limit_reached`):
    /// diagnostic granularity underneath a routable category, never a substitute for one.
    pub code: Option<String>,
    pub recovery: Recovery,
    /// The SEMANTIC clock: when the producer observed this condition. Deliberately distinct from
    /// the record's transport `writtenAtMs`, which a heartbeat advances without observing
    /// anything new.
    pub observed_at_ms: u64,
    /// Producer-supplied, optional, and meaningful only for `automatic` recovery: by when the
    /// producer expects to have observed this fault clear itself.
    pub next_observation_due_ms: Option<u64>,
    /// Diagnostic only. No consumer branches on it.
    pub detail: Option<String>,
    /// Derived at READ time: an `automatic` recovery whose deadline has passed. Such a fault is
    /// projected with `Unknown` recovery — a recovery that missed its own deadline is no longer
    /// evidence of anything automatic — and pages until an explicit paired clear, a terminal
    /// record, a new claim, or a new incarnation replaces it.
    pub overdue: bool,
}

/// The condition axis as a consumer sees it. `Absent` is the version 1/2 projection: those
/// records carry no condition axis at all, and absence is NEVER `clear` — inferring health from a
/// record that never spoke about health is the exact mistake this axis exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionView {
    Absent,
    Clear,
    Fault(Fault),
}

impl ConditionView {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Clear => "clear",
            Self::Fault(_) => "fault",
        }
    }

    pub fn fault(&self) -> Option<&Fault> {
        match self {
            Self::Fault(fault) => Some(fault),
            Self::Absent | Self::Clear => None,
        }
    }
}

/// What kind of human prompt is pending. `Unknown` decodes future words and a `pending` state
/// whose kind the producer did not name; it never means "no ask".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskKind {
    Permission,
    Question,
    Review,
    Unknown,
}

impl AskKind {
    fn parse(word: &str) -> Self {
        match word {
            "permission" => Self::Permission,
            "question" => Self::Question,
            "review" => Self::Review,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Question => "question",
            Self::Review => "review",
            Self::Unknown => "unknown",
        }
    }
}

/// The ask axis: an ACTUAL human prompt, tagged so that "nothing is being asked", "this kind of
/// thing is being asked", and "I cannot tell" are three different statements rather than one word
/// doing all three jobs. Orthogonal to the condition axis: a fault is not an ask, and this axis
/// says nothing about faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanAsk {
    None,
    Pending(AskKind),
    Unknown,
}

impl HumanAsk {
    /// The version 1/2 projection: the legacy `blockedOn`/`ask` pair read forward into the tagged
    /// axis, inventing nothing. `blockedOn: human` is a real pending ask, an unrecognized legacy
    /// word stays indeterminate rather than becoming `none`, and no fault is inferred anywhere.
    fn from_legacy(blocked_on: BlockedOn, ask: Ask) -> Self {
        match blocked_on {
            BlockedOn::None => Self::None,
            BlockedOn::Unknown => Self::Unknown,
            BlockedOn::Human => Self::Pending(match ask {
                Ask::Permission => AskKind::Permission,
                Ask::Question => AskKind::Question,
                Ask::Review => AskKind::Review,
                // A legacy record can be blocked on a human without naming the kind — `none` is
                // the pre-axis default — so the ask is real and its kind unstated.
                Ask::None | Ask::Unknown => AskKind::Unknown,
            }),
        }
    }

    /// The legacy pair this state projects back to, so the shipped `observedState` fields keep
    /// their exact meaning for consumers pinned to them while version 3 records flow through.
    fn to_legacy(self) -> (BlockedOn, Ask) {
        match self {
            Self::None => (BlockedOn::None, Ask::None),
            Self::Unknown => (BlockedOn::Unknown, Ask::Unknown),
            Self::Pending(kind) => (
                BlockedOn::Human,
                match kind {
                    AskKind::Permission => Ask::Permission,
                    AskKind::Question => Ask::Question,
                    AskKind::Review => Ask::Review,
                    AskKind::Unknown => Ask::Unknown,
                },
            ),
        }
    }

    pub fn kind(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending(_) => "pending",
            Self::Unknown => "unknown",
        }
    }

    pub fn pending(self) -> Option<AskKind> {
        match self {
            Self::Pending(kind) => Some(kind),
            Self::None | Self::Unknown => None,
        }
    }
}

/// Whether a linked conversation's already-read history can change underneath the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryMutability {
    /// Prior turns never change: a prefix read once stays valid.
    Stable,
    /// The provider may rewrite or compact history, so a prefix read once may be gone.
    Rewritable,
    /// The producer could not say, which a consumer must treat as rewritable.
    Unknown,
}

impl HistoryMutability {
    fn parse(word: &str) -> Self {
        match word {
            "stable" => Self::Stable,
            "rewritable" => Self::Rewritable,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Rewritable => "rewritable",
            Self::Unknown => "unknown",
        }
    }
}

/// How a capability claim was established. Required beside the claim: a capability nobody can
/// attribute is a capability nobody should build on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityEvidence {
    /// The producer actually exercised the capability against the provider.
    Probed,
    /// The driver declares it from pinned knowledge of that provider version.
    Declared,
    /// Neither: the claim is unattributed.
    Unknown,
}

impl CapabilityEvidence {
    fn parse(word: &str) -> Self {
        match word {
            "probed" => Self::Probed,
            "declared" => Self::Declared,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Probed => "probed",
            Self::Declared => "declared",
            Self::Unknown => "unknown",
        }
    }
}

/// A live link to the provider's own conversation: IDENTITY and CAPABILITY only. Conversation
/// content never rides this record — nothing content-bearing is decoded from it and nothing
/// content-bearing is projected out of it — because a catalog record replicated to every reader
/// is the wrong place for a transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationLink {
    /// Which driver's namespace the opaque identity below belongs to.
    pub driver: String,
    /// The provider's own conversation identity, opaque to st2: never parsed, never joined to a
    /// catalog, never compared across drivers.
    pub conversation: String,
    /// The runtime incarnation the link was established under, so a consumer can tell a relaunch
    /// from a continuation of the same session.
    pub incarnation: String,
    pub history_mutability: HistoryMutability,
    pub capability_evidence: CapabilityEvidence,
    /// The FINITE verification bound: the instant through which this link was actually verified.
    /// Never open-ended, so a consumer ages the claim instead of trusting it forever.
    pub verified_through_ms: u64,
}

/// The conversation bridge, tagged so "there is one", "there could be one and it is not reachable
/// now", and "this driver has no such concept" stay three different answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationRef {
    /// The driver has no conversation identity to expose at all.
    Unsupported,
    /// The driver has one and it is not available right now; the reason is diagnostic.
    Unavailable(Option<String>),
    Linked(ConversationLink),
}

impl ConversationRef {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Unavailable(_) => "unavailable",
            Self::Linked(_) => "linked",
        }
    }

    pub fn link(&self) -> Option<&ConversationLink> {
        match self {
            Self::Linked(link) => Some(link),
            Self::Unsupported | Self::Unavailable(_) => None,
        }
    }
}

/// Why an observation is indeterminate, TYPED. This is the authoritative field for new consumers;
/// the legacy scalar `Observed::reason` is a compatibility projection of this same value and is
/// never computed independently. Present exactly when the observation is indeterminate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indeterminacy {
    /// The closed derivation word: which rule produced the `unknown`.
    pub reason: String,
    /// How old the record's own evidence was when it was read, whenever the bytes carried a usable
    /// stamp at all. Absent for bytes that carried none — an unreadable file, malformed JSON — so
    /// "no age" and "age zero" stay different answers.
    pub evidence_age_ms: Option<u64>,
}

/// The shared derived state: what this seat is doing, folded across the raw axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispositionState {
    Idle,
    Working,
    WaitingHuman,
    Recovering,
    Failed,
    Ended,
    Unknown,
}

impl DispositionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::WaitingHuman => "waitingHuman",
            Self::Recovering => "recovering",
            Self::Failed => "failed",
            Self::Ended => "ended",
            Self::Unknown => "unknown",
        }
    }
}

/// How soon a human is needed. `Now` is the only paging value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attention {
    None,
    Soon,
    Now,
}

impl Attention {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Soon => "soon",
            Self::Now => "now",
        }
    }
}

/// What that human would do first. Remediation outranks answering: a faulted seat cannot use an
/// answer, and its pending ask stays visible on the raw axis regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryAction {
    None,
    Answer,
    Remediate,
    Observe,
}

impl PrimaryAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Answer => "answer",
            Self::Remediate => "remediate",
            Self::Observe => "observe",
        }
    }
}

/// One derived disposition: exactly three closed axes and nothing else. Every consumer — roster,
/// catalog graph, Doctor, and any downstream presentation — reads this instead of re-deriving
/// urgency from the raw axes, because two consumers deriving independently is how one of them
/// starts paging for something the other ignores. Why it carries no reason of its own: the raw
/// axes it folded are on the wire beside it — `condition`, `indeterminacy`, `humanAsk`, and the
/// sibling driver diagnostic — so anything a reason would say is already readable at its source,
/// and a fourth field would be a second, drifting place to say it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disposition {
    pub state: DispositionState,
    pub attention: Attention,
    pub primary_action: PrimaryAction,
}

impl Disposition {
    fn new(
        state: DispositionState,
        attention: Attention,
        primary_action: PrimaryAction,
    ) -> Self {
        Self {
            state,
            attention,
            primary_action,
        }
    }
}

/// THE disposition function: pure, total, and the only place urgency is decided.
///
/// It reads the observed record's already-normalized axes plus the sibling native-driver
/// diagnostic, and it changes neither: the raw axes ride the wire beside this result, so a
/// consumer that disagrees can see exactly what was folded. It authorizes nothing — no delivery
/// path reads it, matching the standing rule that driver degradation is advisory and never
/// alters delivery.
///
/// The order below is the contract:
/// 1. No record at all: nothing was ever observed, which is not a state — unless the sibling
///    diagnostic holds a failure, the one fault st2 can prove without any observation.
/// 2. Record-level indeterminate: unknown, non-paging, worth observing.
/// 3. `ended`: terminal and never paging — a finished seat is not an emergency.
/// 4. A fault: remediation is primary. `automatic` recovery inside its own deadline is
///    `recovering`/`soon`; everything else — human, terminal, unknown, and an overdue automatic
///    recovery normalized into unknown at read time — pages.
/// 5. A native-driver diagnostic failure, which contributes exactly like a fault the harness
///    itself could not report, and outranks an ask for the same reason a fault does.
/// 6. A pending human ask: answer it.
/// 7. Otherwise the raw activity, mapped without urgency.
pub fn disposition(
    observed: Option<&Observed>,
    diagnostic: &crate::driver_diagnostic::Observed,
) -> Disposition {
    let Some(observed) = observed else {
        // Nothing was ever observed. The sibling diagnostic can still be the whole story: the
        // Claude, Codex, and omp drivers publish a diagnostic ONLY on a credential rejection, so
        // "no harness-state record beside a published rejection" is exactly the seat whose
        // provider refused it before it ever reported a state, and reporting that as merely
        // unknown would hide the one fault st2 positively holds.
        return if matches!(diagnostic, crate::driver_diagnostic::Observed::Failure(_)) {
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate,
            )
        } else {
            Disposition::new(
                DispositionState::Unknown,
                Attention::None,
                PrimaryAction::Observe,
            )
        };
    };
    if observed.state == Activity::Unknown {
        return Disposition::new(
            DispositionState::Unknown,
            Attention::None,
            PrimaryAction::Observe,
        );
    }
    if observed.state == Activity::Ended {
        return Disposition::new(
            DispositionState::Ended,
            Attention::None,
            PrimaryAction::None,
        );
    }
    if let Some(fault) = observed.condition.fault() {
        return if fault.recovery == Recovery::Automatic {
            Disposition::new(
                DispositionState::Recovering,
                Attention::Soon,
                PrimaryAction::None,
            )
        } else {
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate,
            )
        };
    }
    if matches!(diagnostic, crate::driver_diagnostic::Observed::Failure(_)) {
        return Disposition::new(
            DispositionState::Failed,
            Attention::Now,
            PrimaryAction::Remediate,
        );
    }
    match observed.human_ask {
        HumanAsk::Pending(_) => Disposition::new(
            DispositionState::WaitingHuman,
            Attention::Now,
            PrimaryAction::Answer,
        ),
        // An unsayable ask is not a summons: it cannot be answered, because nobody knows what the
        // question is. It is worth looking at, which is what `observe` means.
        HumanAsk::Unknown => Disposition::new(
            activity_state(observed.state),
            Attention::None,
            PrimaryAction::Observe,
        ),
        HumanAsk::None => Disposition::new(
            activity_state(observed.state),
            Attention::None,
            PrimaryAction::None,
        ),
    }
}

/// The activity-only mapping, used once no condition, diagnostic, or ask claims the disposition.
fn activity_state(activity: Activity) -> DispositionState {
    match activity {
        Activity::Idle => DispositionState::Idle,
        // A foreground child is the harness working through something, which is what a consumer
        // needs to know; `child` stays visible on the raw axis for anyone who cares which.
        Activity::Active | Activity::Child => DispositionState::Working,
        Activity::Ended => DispositionState::Ended,
        Activity::Unknown => DispositionState::Unknown,
    }
}

/// The durable record. Additive-tolerant on read (no `deny_unknown_fields`): a reader pinned to an
/// older crate may be older than the writer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    schema: String,
    agent: String,
    harness: String,
    state: Activity,
    blocked_on: BlockedOn,
    input_buffer: InputBuffer,
    /// The machine-readable kind of human ask while `blockedOn` is `human`; `none` otherwise.
    /// Absent in records from writers predating the axis, which defaults to `none`.
    #[serde(default)]
    ask: Ask,
    /// Diagnostic only. No consumer branches on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// `Ended` only: the exit outcome, e.g. `exit 0` or `signal 9`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit: Option<String>,
    /// The pty session whose liveness vouches for the live states. Same-host readers cross-check
    /// it; a record whose session is provably dead reads `unknown` even while fresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty_session: Option<String>,
    /// The writing session's incarnation token. Ownership is token equality, never a timestamp
    /// comparison: same-millisecond takeovers and lingering predecessor writers are both real.
    /// Empty in records from writers predating the field, which no session owns.
    #[serde(default)]
    incarnation: String,
    /// The monotonic ownership sequence. Only a session claim (a wrapper or session-boundary
    /// writer starting up) advances it, to the on-disk value plus one; every writer refuses to
    /// touch a record whose sequence is beyond its own claim, which is what gives ownership a
    /// DIRECTION — a lingering predecessor's late write cannot replace its successor's record,
    /// while the successor's claim replaces the predecessor's.
    #[serde(default)]
    seq: u64,
    /// When the current state was entered. Survives heartbeat re-stamps.
    since_ms: u64,
    /// The heartbeat: when the writer last held evidence for this state.
    written_at_ms: u64,
    /// Monotonic transition counter. Keeps every write byte-distinct and leaves room for a
    /// compatible transition history later.
    transitions: u64,
}

/// The record's file name inside an agent directory. Named rather than inlined because the
/// replication transport's include list carries it literally: see
/// [`crate::harness_context::REPLICATED_DRIVER_RECORDS`].
pub const RECORD_NAME: &str = "harness-state";

/// The observed-state file: `<agent_dir>/harness-state`.
pub fn harness_state_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join(RECORD_NAME)
}

/// One observation as a producer states it: everything except the derived pieces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub state: Activity,
    pub blocked_on: BlockedOn,
    pub input_buffer: InputBuffer,
    pub ask: Ask,
    pub reason: Option<String>,
    pub exit: Option<String>,
}

impl Observation {
    pub fn new(state: Activity, blocked_on: BlockedOn, input_buffer: InputBuffer) -> Self {
        Self {
            state,
            blocked_on,
            input_buffer,
            ask: Ask::None,
            reason: None,
            exit: None,
        }
    }

    pub fn with_ask(mut self, ask: Ask) -> Self {
        self.ask = ask;
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_exit(mut self, exit: impl Into<String>) -> Self {
        self.exit = Some(exit.into());
        self
    }
}

// ---------------------------------------------------------------------------
// The version 3 producer surface: what a driver STATES, as distinct from what a
// consumer reads. Deliberately its own vocabulary rather than the reader's:
// every read type carries something a producer must not be able to forge — the
// legacy projection `absent`, the read-time `overdue` normalization, st2's own
// rejection words on a degraded conversation link, the derived `unknown`
// activity — and a write type that could spell those is a write type that can
// fabricate them.
// ---------------------------------------------------------------------------

/// One fault as a PRODUCER states it. Everything a consumer needs to route it, time it, and age
/// it is required here, because a fault that cannot be routed or timed is not actionable and st2
/// would rather refuse the write than publish one: the closed [`FaultCategory`] (the reader
/// tolerates an untyped category from a future producer; a producer in this build has no excuse
/// for one), the recovery class, and the SEMANTIC observation instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultReport {
    pub category: FaultCategory,
    /// Provider-namespaced and OPEN (`claude/oauth_expired`), diagnostic granularity underneath a
    /// routable category. Validated as `provider/code` so the namespace is real: a bare word is
    /// how two providers' codes silently collide.
    pub code: Option<String>,
    pub recovery: Recovery,
    /// When the producer OBSERVED this condition — the semantic clock, which the transport
    /// heartbeat never advances and a restatement of the same fault never re-mints.
    pub observed_at_ms: u64,
    /// By when the producer expects to have observed an `automatic` recovery clear itself.
    pub next_observation_due_ms: Option<u64>,
    /// Diagnostic only. No consumer branches on it.
    pub detail: Option<String>,
}

impl FaultReport {
    pub fn new(category: FaultCategory, recovery: Recovery, observed_at_ms: u64) -> Self {
        Self {
            category,
            code: None,
            recovery,
            observed_at_ms,
            next_observation_due_ms: None,
            detail: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn with_observation_due(mut self, due_ms: u64) -> Self {
        self.next_observation_due_ms = Some(due_ms);
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// The pairing identity a clear must name exactly.
    pub fn key(&self) -> FaultKey {
        FaultKey {
            category: self.category,
            code: self.code.clone(),
        }
    }

    /// Whether two reports are the SAME fault, ignoring when it was observed. Everything a
    /// consumer would route, time, or read differently counts; `observedAtMs` does not, because
    /// that is precisely the field a restatement must not move.
    fn same_fault(&self, other: &Self) -> bool {
        self.category == other.category
            && self.code == other.code
            && self.recovery == other.recovery
            && self.next_observation_due_ms == other.next_observation_due_ms
            && self.detail == other.detail
    }

    /// Producer-side strictness mirroring the reader's, so a producer never writes bytes this
    /// build's own reader would reject or normalize away.
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.observed_at_ms > 0,
            "a fault carries the instant it was observed; zero is not an observation"
        );
        if let Some(code) = &self.code {
            let namespaced = code
                .split_once('/')
                .is_some_and(|(provider, rest)| !provider.is_empty() && !rest.is_empty());
            anyhow::ensure!(
                namespaced,
                "a fault code is provider-namespaced (`provider/code`): got {code:?}"
            );
        }
        if let Some(due) = self.next_observation_due_ms {
            anyhow::ensure!(
                !matches!(self.recovery, Recovery::Human | Recovery::Terminal),
                "a next-observation deadline belongs to a recovery that happens by itself, not to \
                 `{}`",
                self.recovery.as_str()
            );
            anyhow::ensure!(
                due >= self.observed_at_ms,
                "a next-observation deadline cannot precede the observation it follows"
            );
        }
        Ok(())
    }
}

/// What a paired clear names: EXACTLY the category and code of the fault it clears. A key with no
/// code matches only a fault carrying none — a clear derived from a category alone is how one
/// provider signal ("the rate limit lifted") silences a different live fault in the same
/// category.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultKey {
    pub category: FaultCategory,
    pub code: Option<String>,
}

impl FaultKey {
    pub fn new(category: FaultCategory) -> Self {
        Self {
            category,
            code: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    fn matches(&self, fault: &FaultReport) -> bool {
        self.category == fault.category && self.code == fault.code
    }
}

/// The positive observation that authorizes clearing the WHOLE condition axis. There is
/// deliberately no "assumed", "probably", or "unknown" member: an unkeyed clear is the one
/// operation that can silence a fault nobody has seen resolve, so the caller must name the
/// progress it actually witnessed. The word rides the record as the clear's diagnostic detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressProof {
    /// A turn ran to completion against the provider.
    TurnCompleted,
    /// A request the standing fault would have blocked succeeded.
    RequestSucceeded,
}

impl ProgressProof {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnCompleted => "cleared: turn completed",
            Self::RequestSucceeded => "cleared: request succeeded",
        }
    }
}

/// The condition axis as a producer states it. Three statements, and `Unchanged` is the load
/// bearing one: an activity edge (a turn started, the composer emptied) has learned NOTHING about
/// whether the provider is faulted, and a producer forced to pick `clear` there would fabricate
/// health on every frame. `Unchanged` carries the record's own condition forward untouched —
/// including a fault's semantic clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionReport {
    Unchanged,
    Clear,
    Fault(FaultReport),
}

/// A conversation link as a producer states it: the reader's [`ConversationLink`] minus the
/// incarnation, which the writing session stamps. Two sources for one fact would make a
/// disagreement with the record's own `incarnation` unfalsifiable for a reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationClaim {
    pub driver: String,
    /// The provider's own conversation identity, opaque to st2.
    pub conversation: String,
    pub history_mutability: HistoryMutability,
    pub capability_evidence: CapabilityEvidence,
    /// The FINITE verification bound; zero is not a bound.
    pub verified_through_ms: u64,
}

/// The conversation bridge as a producer states it. A separate type from the reader's
/// [`ConversationRef`] for one specific reason: `read_v3_at` degrades a half-stated link to
/// `unavailable` carrying st2's OWN rejection word, and if producers wrote the same type they
/// could emit those words as prose — after which no consumer could tell a driver's explanation
/// from st2's verdict on the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationState {
    /// This driver has no conversation identity to expose at all.
    Unsupported,
    /// It has one and it is not available now; the reason is the producer's own prose.
    Unavailable(Option<String>),
    Linked(ConversationClaim),
}

/// st2's own conversation-axis rejection words, which [`read_v3_at`] mints when it degrades a
/// half-stated link. A producer may not spell them: the whole point of the degradation word is
/// that it means "st2 refused this link", and prose that borrows it is indistinguishable.
const RESERVED_CONVERSATION_REASONS: [&str; 3] = [
    "incomplete-conversation-link",
    "unbounded-conversation-verification",
    "unsupported-conversation-kind",
];

/// One version 3 observation as a producer states it: the resolved tuple, every axis at once.
/// A producer decides all of these from one look at its harness, so it states them as one
/// statement rather than as several writes that a reader could observe half-applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub state: Activity,
    pub input_buffer: InputBuffer,
    pub condition: ConditionReport,
    /// The tagged ask. `Unknown` is WRITABLE here and means "this producer cannot see an ask
    /// surface at all" — a maintained driver with no ask channel (pi) states it positively;
    /// `None` would fabricate absence and `Pending(Unknown)` would fabricate a waiting human.
    pub ask: HumanAsk,
    /// `None` leaves the axis exactly as the record has it; `Some` states it.
    pub conversation: Option<ConversationState>,
    /// Diagnostic only. No consumer branches on it.
    pub reason: Option<String>,
    /// `Ended` only: the exit outcome.
    pub exit: Option<String>,
}

impl Frame {
    pub fn new(
        state: Activity,
        input_buffer: InputBuffer,
        condition: ConditionReport,
        ask: HumanAsk,
    ) -> Self {
        Self {
            state,
            input_buffer,
            condition,
            ask,
            conversation: None,
            reason: None,
            exit: None,
        }
    }

    pub fn with_conversation(mut self, conversation: ConversationState) -> Self {
        self.conversation = Some(conversation);
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_exit(mut self, exit: impl Into<String>) -> Self {
        self.exit = Some(exit.into());
        self
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state != Activity::Unknown,
            "unknown is derived and cannot be written"
        );
        // A terminal record is the incarnation's last word, and its exit is what makes it one.
        // An exitless `ended` is the CLAIM FENCE, which only [`claim`] writes: letting a producer
        // spell it would let any driver process forge a session boundary.
        anyhow::ensure!(
            self.state != Activity::Ended || self.exit.is_some(),
            "a terminal frame carries its exit outcome; an exitless `ended` is the claim fence, \
             which only a session claim writes"
        );
        if let ConditionReport::Fault(fault) = &self.condition {
            fault.validate()?;
        }
        match &self.conversation {
            Some(ConversationState::Linked(link)) => {
                anyhow::ensure!(
                    !link.driver.is_empty()
                        && !link.conversation.is_empty()
                        && link.verified_through_ms > 0,
                    "a linked conversation names its driver and conversation and carries a finite \
                     positive verification bound"
                );
            }
            Some(ConversationState::Unavailable(Some(reason))) => {
                anyhow::ensure!(
                    !RESERVED_CONVERSATION_REASONS.contains(&reason.as_str()),
                    "`{reason}` is st2's own rejection word for this axis and cannot be stated as \
                     a producer reason"
                );
            }
            Some(ConversationState::Unsupported | ConversationState::Unavailable(None)) | None => {}
        }
        Ok(())
    }
}

/// What a producer write did. Landing and coalescing are both SUCCESS — a restatement inside the
/// refresh cadence deliberately does not write — and everything else is a refusal the caller can
/// read, which is why this replaces the older `Ok(false)`: a producer whose every write is being
/// dropped because the record on disk speaks another version must be able to see that, and this
/// record family has exactly one store and no second place to report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    Landed,
    /// The record already said this, recently enough that a reader trusts it. Nothing was written.
    Coalesced,
    Refused(Refusal),
}

impl WriteOutcome {
    /// Whether the record now says what the caller stated — written or already there.
    pub fn accepted(&self) -> bool {
        matches!(self, Self::Landed | Self::Coalesced)
    }

    pub fn landed(&self) -> bool {
        matches!(self, Self::Landed)
    }

    pub fn refusal(&self) -> Option<&Refusal> {
        match self {
            Self::Refused(refusal) => Some(refusal),
            Self::Landed | Self::Coalesced => None,
        }
    }
}

/// Why a write did not land. Each arm is a distinct thing for a caller to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The record declares a version this writer does not emit. Only an explicit [`claim`]
    /// supersedes it.
    Schema { found: String },
    /// A later session owns the record: its ownership sequence is beyond this writer's claim.
    Superseded { on_disk_seq: u64, ours: u64 },
    /// The record carries a foreign session's token and this writer has no claim to mint one
    /// with; only [`claim`] establishes ownership.
    Unclaimed,
    /// The bytes on disk are somebody's record that this build cannot decode.
    Unreadable,
    /// This session's own exit-bearing terminal record stands: the incarnation's last word, which
    /// an opt-in live frame and every condition operation refuse to overwrite.
    Terminal,
    /// A paired clear named a fault that is not the one standing. `current` is what is.
    ConditionMismatch { current: Option<FaultKey> },
    /// The emitted version has no condition axis, and this record has one source of truth: a
    /// condition cannot be held anywhere else while the wire cannot carry it.
    ConditionUnrepresentable { schema: &'static str },
    /// A condition operation found no observation of this session's to attach to.
    Unobserved,
    /// A version 3 write whose condition axis nobody ever stated. Not writable as `absent`, so
    /// the producer must state the axis once before an activity-only frame can ride on it.
    Unstated,
    /// A condition operation found a LIVE record naming no pty session. Writing it back would
    /// restate an unfenced live state, which every same-host reader derives as `unknown`; the
    /// producer must state its observation (with its fence) instead.
    Unfenced,
}

/// The version 2 spelling of the tagged ask axis, for the projection this build still writes.
/// Every arm round-trips through [`HumanAsk::from_legacy`], which is the property that makes the
/// projection lossless rather than merely plausible.
fn project_ask_legacy(ask: HumanAsk) -> (BlockedOn, Ask) {
    match ask {
        HumanAsk::None => (BlockedOn::None, Ask::None),
        HumanAsk::Pending(AskKind::Permission) => (BlockedOn::Human, Ask::Permission),
        HumanAsk::Pending(AskKind::Question) => (BlockedOn::Human, Ask::Question),
        HumanAsk::Pending(AskKind::Review) => (BlockedOn::Human, Ask::Review),
        // A real pending ask whose KIND the producer could not classify. `blockedOn: human` with
        // `ask: none` is exactly the pre-axis legacy spelling of that, and `from_legacy` reads it
        // back as `Pending(Unknown)`; writing `ask: unknown` instead would put a derived word on
        // the wire.
        HumanAsk::Pending(AskKind::Unknown) => (BlockedOn::Human, Ask::None),
        // "This producer cannot see the ask axis at all." The only faithful legacy spelling is
        // `blockedOn: unknown`, which `from_legacy` reads back as exactly this state and which no
        // consumer reads as a waiting human. `none` would fabricate absence and `human` would
        // fabricate a queue row nobody can answer.
        HumanAsk::Unknown => (BlockedOn::Unknown, Ask::None),
    }
}

fn ask_to_wire(ask: HumanAsk) -> WireAsk {
    match ask {
        HumanAsk::None => WireAsk {
            kind: "none".to_string(),
            ask: None,
        },
        HumanAsk::Unknown => WireAsk {
            kind: "unknown".to_string(),
            ask: None,
        },
        HumanAsk::Pending(kind) => WireAsk {
            kind: "pending".to_string(),
            ask: Some(kind.as_str().to_string()),
        },
    }
}

fn conversation_to_wire(state: &ConversationState, incarnation: &str) -> WireConversation {
    let empty = WireConversation {
        kind: String::new(),
        driver: None,
        conversation: None,
        incarnation: None,
        history_mutability: None,
        capability_evidence: None,
        verified_through_ms: None,
        reason: None,
    };
    match state {
        ConversationState::Unsupported => WireConversation {
            kind: "unsupported".to_string(),
            ..empty
        },
        ConversationState::Unavailable(reason) => WireConversation {
            kind: "unavailable".to_string(),
            reason: reason.clone(),
            ..empty
        },
        ConversationState::Linked(link) => WireConversation {
            kind: "linked".to_string(),
            driver: Some(link.driver.clone()),
            conversation: Some(link.conversation.clone()),
            incarnation: Some(incarnation.to_string()),
            history_mutability: Some(link.history_mutability.as_str().to_string()),
            capability_evidence: Some(link.capability_evidence.as_str().to_string()),
            verified_through_ms: Some(link.verified_through_ms),
            ..empty
        },
    }
}

/// One mutation of the record, as the single locked path receives it. A closed set: every
/// producer entry point becomes one of these, so no entry point can quietly acquire its own
/// ownership or coalescing rules.
enum Mutation {
    Legacy {
        observation: Observation,
        skip_if_ended: bool,
    },
    Frame {
        frame: Frame,
        skip_if_ended: bool,
    },
    Condition(ConditionOp),
}

/// A mutation of the condition axis ALONE: it restates no activity, because a producer that
/// learned its provider is throttled learned nothing about whether the model is working.
enum ConditionOp {
    Raise(FaultReport),
    ClearPaired(FaultKey),
    ClearAll(ProgressProof),
}

/// The resolved tuple a write is about to state, after carry-forward. `condition: None` means the
/// axis was never stated by anyone — writable under version 2, which has no such axis, and a
/// typed refusal under version 3, which requires one.
struct Pending {
    state: Activity,
    input_buffer: InputBuffer,
    legacy_ask: (BlockedOn, Ask),
    ask: HumanAsk,
    condition: Option<Stated>,
    conversation: Option<ConversationState>,
    reason: Option<String>,
    exit: Option<String>,
    skip_if_ended: bool,
    /// The pty session to stamp: this writer's own for an observation, and the RECORD's for a
    /// condition-only write, which observed no pty at all.
    pty_session: Option<String>,
    /// Whether this write states only the condition axis. Such a write restates the activity
    /// verbatim, so it must not restart the activity's `sinceMs` — that field means "when this
    /// state was entered", and a fault is not a state change.
    condition_only: bool,
}

/// The writer a driver process owns over one agent's record. Several driver processes may
/// legitimately hold writers over the same record — a wrapper heartbeat beside hook-process
/// transitions — so every operation takes the record's cross-process lock and treats the on-disk
/// record as the authoritative current state: rename alone is atomic but not isolated, and
/// without the re-read a stale process could resurrect the state it saw before a peer's write.
/// The caller's rule for indeterminacy: when evidence is lost (the observer no longer sees its
/// harness), call nothing — never heartbeat a state you cannot see, and never write `unknown`.
pub struct Writer {
    path: PathBuf,
    lock_path: PathBuf,
    agent: String,
    harness: &'static str,
    pty_session: Option<String>,
    interrupted: bool,
    session: String,
    /// The version this writer EMITS. Initialized from [`SCHEMA`] — the build's one
    /// writer-selection point, which version 3 activation supersedes rather than sits beside — and
    /// carried per writer so version 3 serialization is provable in this build without a second
    /// production selector. Every ownership decision is scoped to it: a writer owns only the
    /// shape it emits, so a v1 straggler still refuses to replace a v2 record and vice versa.
    schema: &'static str,
    /// The ownership sequence this writer acts under. `None` = a claiming writer: it resolves at
    /// the first write — adopting the on-disk sequence when the record already carries this
    /// session's token, else claiming on-disk + 1. `Some` = adopted ownership handed down by the
    /// session's claimer (env-exported beside the token).
    claimed_seq: Option<u64>,
}

impl Writer {
    /// The transition counter continues from any readable record already on disk, so restarts and
    /// sibling writers keep writes byte-distinct; an unreadable predecessor starts the counter
    /// fresh.
    pub fn new(
        agent_dir: &Path,
        agent: impl Into<String>,
        harness: &'static str,
        pty_session: Option<String>,
    ) -> Self {
        Self {
            path: harness_state_path(agent_dir),
            lock_path: agent_dir.join(LOCK_NAME),
            agent: agent.into(),
            harness,
            pty_session,
            interrupted: false,
            session: session_token(),
            claimed_seq: None,
            schema: SCHEMA,
        }
    }

    /// Adopt an explicit session incarnation token. Sibling writer processes of one session —
    /// a wrapper beside its hook subprocesses, a channel beside its wrapper — must share one
    /// token (typically minted by the wrapper and exported through the session environment), or
    /// each writes as its own session: restatements open transitions and the wrapper can neither
    /// re-stamp nor terminally fence its siblings' records.
    pub fn with_session(mut self, token: impl Into<String>) -> Self {
        self.session = token.into();
        self
    }

    /// Adopt the full ownership a session's claimer exported — token and claimed sequence
    /// together. A writer holding adopted ownership never claims: it writes only while the
    /// on-disk record's sequence is at or below its claim, so a straggler from a superseded
    /// session is refused in both the live and the terminal path.
    pub fn with_ownership(mut self, token: impl Into<String>, seq: u64) -> Self {
        self.session = token.into();
        self.claimed_seq = Some(seq);
        self
    }

    /// Emit a different version than [`SCHEMA`]. TEST ONLY, and deliberately not a second
    /// production selector: [`EMIT_SCHEMA_V3`] replaces [`SCHEMA`] itself, which every writer
    /// reads. The seam exists because activation makes one direction of every version rule
    /// unreachable from the production writer: with [`SCHEMA`] on version 3, the legacy byte
    /// contract, the projection guarantee, and the refused fence direction (a version 2 claim
    /// over a version 3 record) can only be exercised by naming version 2 explicitly. Both are
    /// live facts — every legacy record on disk was written that way, and a rolled-back or
    /// not-yet-replaced peer still writes that way — so both stay proved in one binary.
    /// `pub(crate)` because the adapter modules own their own legacy-projection proofs and need
    /// the same seam; it is compiled out of every shipped binary.
    #[cfg(test)]
    pub(crate) fn with_emitted_schema(mut self, schema: &'static str) -> Self {
        self.schema = schema;
        self
    }

    /// [`Writer::with_emitted_schema`] in place, for a writer a constructor already handed back
    /// inside a larger driver value.
    #[cfg(test)]
    pub(crate) fn emit_schema(&mut self, schema: &'static str) {
        self.schema = schema;
    }

    /// Mark this writer's observation stream discontinuous: its evidence was lost and has since
    /// returned. The next observation opens a fresh transition even if it restates the
    /// pre-interruption tuple — continuity (`sinceMs`, the counter) must never be claimed across
    /// an interval the observer did not see.
    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    /// Hold the record's exclusive cross-process lock for one read→decide→rename cycle. The lock
    /// file is a permanent sibling; the guard releases on drop (close).
    fn locked(&self) -> anyhow::Result<fs::File> {
        lock_exclusive(&self.lock_path)
    }

    /// Record an observation. A genuine change writes a new transition with a fresh `since`. An
    /// observation identical to the on-disk record is a no-op while that record is fresh —
    /// producers may restate their state arbitrarily often (an SSE stream restates several times
    /// per second, measured) and only the refresh cadence may reach the transport — and becomes a
    /// heartbeat-equivalent re-stamp once the record is older than [`HARNESS_STATE_REFRESH`].
    /// `Unknown` state is derived and cannot be written.
    pub fn observe(&mut self, observation: Observation) -> anyhow::Result<()> {
        self.observe_inner(observation, false).map(|_wrote| ())
    }

    /// [`Writer::observe`], except a live-state frame is dropped (returning `false`) when the
    /// on-disk record is already terminal. Not a general rule — a harness may legally report
    /// activity after a terminal error, and Codex does — but a producer whose live frames and
    /// terminal record come from different processes opts in so a queued live frame can never
    /// overwrite the incarnation's last word.
    pub fn observe_unless_ended(&mut self, observation: Observation) -> anyhow::Result<bool> {
        self.observe_inner(observation, true)
    }

    fn observe_inner(
        &mut self,
        observation: Observation,
        skip_if_ended: bool,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            observation.state != Activity::Unknown,
            "unknown is derived and cannot be written"
        );
        anyhow::ensure!(
            observation.state == Activity::Ended || self.pty_session.is_some(),
            "live observations require a pty session to vouch for them"
        );
        anyhow::ensure!(
            observation.ask != Ask::Unknown,
            "unknown is derived and cannot be written"
        );
        anyhow::ensure!(
            observation.ask == Ask::None || observation.blocked_on == BlockedOn::Human,
            "an ask kind is meaningful only while blocked on a human"
        );
        Ok(self
            .mutate(Mutation::Legacy {
                observation,
                skip_if_ended,
            })?
            .accepted())
    }

    /// Whether the version this writer EMITS carries the condition axis at all. False while the
    /// build's writer is on version 2, where a stated condition is projected away rather than
    /// stored somewhere else: there is exactly one source of truth for this record, and inventing
    /// a sidecar to hold an axis the wire cannot carry would create a second one. Adapters read
    /// this to know whether [`Writer::raise_fault`] and the clears can do anything yet.
    pub fn writes_condition_axis(&self) -> bool {
        self.schema == SCHEMA_V3
    }

    /// Publish a fully resolved version 3 tuple: activity, composer, condition, ask, and the
    /// optional conversation bridge as ONE statement, which is how a producer that just decided
    /// all of them should state them. Coalescing, ownership, and the semantic clock work exactly
    /// as they do for [`Writer::observe`]; the outcome is typed, so a refusal — a foreign schema,
    /// a later session's claim, unreadable bytes — is a value the caller can act on rather than a
    /// silent `false`.
    ///
    /// While the emitted version is 2 this projects to the existing legacy bytes with exactly the
    /// existing behaviour: the tagged ask becomes the legacy pair, and the condition and
    /// conversation axes are dropped because the version 2 wire has nowhere to carry them. A
    /// condition-only change therefore coalesces (nothing in the bytes changed), which is
    /// observable through [`Writer::writes_condition_axis`].
    pub fn publish(&mut self, frame: Frame) -> anyhow::Result<WriteOutcome> {
        self.publish_inner(frame, false)
    }

    /// [`Writer::publish`], with [`Writer::observe_unless_ended`]'s opt-in: a live frame is
    /// refused once this session's own exit-bearing terminal record stands. Kept as a separate
    /// entry point because reporting activity after a terminal error is legal for some harnesses
    /// (Codex does it) and only a producer whose live frames and terminal record come from
    /// different processes wants them suppressed.
    pub fn publish_unless_ended(&mut self, frame: Frame) -> anyhow::Result<WriteOutcome> {
        self.publish_inner(frame, true)
    }

    fn publish_inner(&mut self, frame: Frame, skip_if_ended: bool) -> anyhow::Result<WriteOutcome> {
        frame.validate()?;
        anyhow::ensure!(
            frame.state == Activity::Ended || self.pty_session.is_some(),
            "live observations require a pty session to vouch for them"
        );
        self.mutate(Mutation::Frame {
            frame,
            skip_if_ended,
        })
    }

    /// Raise a fault, or replace the one that stands, WITHOUT restating the activity axis: a
    /// producer that learned its provider is throttled has learned nothing new about whether the
    /// model is working, and making it restate an activity it did not observe is how a stale
    /// activity gets refreshed by a fault. The rest of the tuple — activity, composer, ask, and
    /// the conversation bridge — is carried across from this session's own record verbatim.
    ///
    /// Replacing a fault with the SAME fault preserves its original `observedAtMs`: the semantic
    /// clock records when the condition was first observed, so restating it cannot postpone an
    /// automatic recovery's deadline.
    pub fn raise_fault(&mut self, fault: FaultReport) -> anyhow::Result<WriteOutcome> {
        fault.validate()?;
        self.mutate(Mutation::Condition(ConditionOp::Raise(fault)))
    }

    /// Clear the fault this key names, and ONLY that one. The key is the exact `(category, code)`
    /// pair — a key naming no code matches only a fault carrying none — because a clear derived
    /// from a category alone is how one provider signal ("rate limit lifted") silences a different
    /// live fault in the same category. A mismatch writes nothing and returns
    /// [`Refusal::ConditionMismatch`] carrying the fault that actually stands, so the caller can
    /// see what it failed to match instead of believing it cleared something.
    pub fn clear_fault(&mut self, key: FaultKey) -> anyhow::Result<WriteOutcome> {
        self.mutate(Mutation::Condition(ConditionOp::ClearPaired(key)))
    }

    /// Clear the WHOLE condition axis, whatever stands on it. Unlike the paired clear this needs
    /// no key, so it requires a [`ProgressProof`]: a positive observation that the harness made
    /// progress the standing fault would have prevented. The type has no "I assume so" member —
    /// the caller must name what it saw — because an unproven blanket clear is exactly how a
    /// fault that still holds stops paging.
    pub fn clear_all(&mut self, proof: ProgressProof) -> anyhow::Result<WriteOutcome> {
        self.mutate(Mutation::Condition(ConditionOp::ClearAll(proof)))
    }

    /// THE mutation path: one lock, one read of the authoritative record, one ownership decision,
    /// one byte-distinct rename. Every producer entry point above resolves to this, so ownership
    /// direction, coalescing, stamp monotonicity, and the transition counter are decided once and
    /// cannot drift between the legacy and version 3 surfaces.
    fn mutate(&mut self, mutation: Mutation) -> anyhow::Result<WriteOutcome> {
        let _lock = self.locked()?;
        let on_disk = match read_current(&self.path) {
            CurrentRecord::Absent => None,
            // Bytes this build cannot decode as EITHER writable shape are somebody's record, not
            // a virgin seat: a non-claiming writer refuses rather than restarting the sequence
            // and counter over foreign state. Only the explicit written claim supersedes.
            CurrentRecord::Unreadable => return Ok(WriteOutcome::Refused(Refusal::Unreadable)),
            CurrentRecord::Legacy(record) => Some(Snapshot::from_legacy(record)),
            CurrentRecord::V3(record) => Some(Snapshot::from_v3(record)),
        };
        // Resolve this writer's ownership sequence, then enforce its direction. A claiming
        // writer adopts the on-disk sequence when the record already carries its token (a
        // sibling wrote first) and claims on-disk + 1 otherwise; an adopted-ownership writer
        // holds whatever its session's claimer exported. Either way, a record whose sequence is
        // beyond the claim belongs to a LATER session: this writer is the straggler, and its
        // write — live or terminal — is refused rather than replacing its successor's record.
        let seq = match self.claimed_seq {
            Some(seq) => seq,
            // A token-only writer NEVER claims: it adopts the on-disk sequence when the record
            // already carries its token, starts a virgin record at one, and is refused outright
            // against a foreign token — a mixed-version straggler minting claims would fence
            // the true successor out permanently. New sequences are minted only by [`claim`],
            // the written act, and adopted from it.
            None => match on_disk.as_ref() {
                // A virgin seat's first write mints sequence one — initial ownership,
                // exactly what [`claim_locked`] establishes — so it persists the floor
                // sidecar too: if this record later goes unreadable, a replacement claim
                // must continue past it instead of colliding with this lingering writer.
                None => {
                    persist_floor(&self.path, 1);
                    1
                }
                // Adoption never crosses schemas: a record is this writer's own only when it
                // carries both this build's version and this session's token. A same-token
                // record under another version belongs to a differently-versioned writer of
                // this session, and inheriting its sequence would let this build write over a
                // meaning it did not produce.
                Some(current)
                    if current.schema == self.schema && current.incarnation == self.session =>
                {
                    current.seq
                }
                Some(_) => return Ok(WriteOutcome::Refused(Refusal::Unclaimed)),
            },
        };
        if let Some(current) = on_disk.as_ref().filter(|current| current.seq > seq) {
            return Ok(WriteOutcome::Refused(Refusal::Superseded {
                on_disk_seq: current.seq,
                ours: seq,
            }));
        }
        // A foreign schema's `seq` decodes as serde-default zero, which every claim exceeds — a
        // v1 straggler would otherwise replace a v2 record it cannot even read. Non-claiming
        // writers refuse foreign schemas outright; only the explicit written [`claim`]
        // supersedes an unsupported schema. The refusal is TYPED rather than a silent no-op: a
        // producer whose writes are all being dropped because the record on disk speaks a
        // different version must be able to see that, and this record has no second store to
        // report it through.
        if let Some(current) = on_disk
            .as_ref()
            .filter(|current| current.schema != self.schema)
        {
            return Ok(WriteOutcome::Refused(Refusal::Schema {
                found: current.schema.clone(),
            }));
        }
        self.claimed_seq = Some(seq);
        // Ownership is token equality: a record is this writer's only when it carries both this
        // version's schema and this session's incarnation. Anything else — a foreign schema, a
        // predecessor's or successor's token, the empty pre-token form — is never coalesced
        // against and never treated as this session's terminal word; a genuine observation
        // replaces it wholesale (one logical owner per record), continuing the counter for
        // byte-distinctness. Timestamps deliberately play no part: a same-millisecond takeover
        // and a lingering predecessor writer are both real and both ambiguous by clock.
        let own = on_disk
            .as_ref()
            .filter(|current| current.schema == self.schema && current.incarnation == self.session);
        // What an UNSTATED axis inherits. A claim fence is excluded: it is a session that has
        // observed nothing, so carrying its required-but-meaningless condition forward would turn
        // a fence into a health claim.
        let inherited = own.filter(|current| !current.is_fence());
        let pending = match mutation {
            Mutation::Legacy {
                observation,
                skip_if_ended,
            } => Pending {
                state: observation.state,
                input_buffer: observation.input_buffer,
                // The legacy pair rides VERBATIM: a legacy caller's bytes must not change
                // because a tagged axis now exists to re-derive them from.
                legacy_ask: (observation.blocked_on, observation.ask),
                ask: HumanAsk::from_legacy(observation.blocked_on, observation.ask),
                condition: inherited.and_then(|current| current.condition.clone()),
                conversation: inherited.and_then(|current| current.conversation.clone()),
                reason: observation.reason,
                exit: observation.exit,
                skip_if_ended,
                pty_session: self.pty_session.clone(),
                condition_only: false,
            },
            Mutation::Frame {
                frame,
                skip_if_ended,
            } => Pending {
                state: frame.state,
                input_buffer: frame.input_buffer,
                legacy_ask: project_ask_legacy(frame.ask),
                ask: frame.ask,
                condition: match frame.condition {
                    ConditionReport::Unchanged => {
                        inherited.and_then(|current| current.condition.clone())
                    }
                    ConditionReport::Clear => Some(Stated::Clear { detail: None }),
                    ConditionReport::Fault(fault) => Some(Stated::Fault(fault)),
                },
                conversation: frame
                    .conversation
                    .or_else(|| inherited.and_then(|current| current.conversation.clone())),
                reason: frame.reason,
                exit: frame.exit,
                skip_if_ended,
                pty_session: self.pty_session.clone(),
                condition_only: false,
            },
            Mutation::Condition(op) => {
                // Version 2 has nowhere to carry a condition, and this record has exactly one
                // source of truth: refuse, typed, rather than keeping the axis in a cache the
                // session's sibling processes cannot see and coalescing cannot consult.
                if !self.writes_condition_axis() {
                    return Ok(WriteOutcome::Refused(Refusal::ConditionUnrepresentable {
                        schema: self.schema,
                    }));
                }
                // A condition attaches to an OBSERVATION. This session's own record is the only
                // one that can hold it: stating a fault about a record this session never wrote
                // would be speaking for another owner's observation.
                let Some(base) = own else {
                    return Ok(WriteOutcome::Refused(Refusal::Unobserved));
                };
                if base.is_fence() {
                    return Ok(WriteOutcome::Refused(Refusal::Unobserved));
                }
                // A terminal record is the incarnation's last word, and a condition operation is
                // never the thing that writes one: rewriting an `ended` record here would
                // re-publish `ended` from a code path that observed no exit at all.
                if base.state == Activity::Ended {
                    return Ok(WriteOutcome::Refused(Refusal::Terminal));
                }
                let condition = match op {
                    ConditionOp::Raise(fault) => Stated::Fault(fault),
                    ConditionOp::ClearPaired(key) => {
                        match base.condition.as_ref().and_then(Stated::fault) {
                            Some(fault) if key.matches(fault) => Stated::Clear { detail: None },
                            current => {
                                return Ok(WriteOutcome::Refused(Refusal::ConditionMismatch {
                                    current: current.map(FaultReport::key),
                                }));
                            }
                        }
                    }
                    // The proof rides the wire as the clear's diagnostic detail — a `clear` may
                    // carry one — so an operator can see what a blanket clear was based on.
                    ConditionOp::ClearAll(proof) => Stated::Clear {
                        detail: Some(proof.as_str().to_string()),
                    },
                };
                // A condition operation restates the activity axis verbatim, INCLUDING the pty
                // session that vouches for it: the caller may be a hook process with no pty of
                // its own, and writing its own empty fence would strip a live record's only
                // liveness evidence and flip the seat to `unknown` for every same-host reader.
                let pty_session = base.pty_session.clone();
                if pty_session.is_none() && base.state != Activity::Ended {
                    return Ok(WriteOutcome::Refused(Refusal::Unfenced));
                }
                Pending {
                    state: base.state,
                    pty_session,
                    input_buffer: base.input_buffer,
                    legacy_ask: base.legacy_ask,
                    ask: base.ask,
                    condition: Some(condition),
                    conversation: base.conversation.clone(),
                    reason: base.reason.clone(),
                    exit: base.exit.clone(),
                    skip_if_ended: false,
                    // The activity did not change, so its clock does not restart. The transition
                    // counter still advances: the bytes changed, and every landed write must be
                    // byte-distinct.
                    condition_only: true,
                }
            }
        };
        if pending.skip_if_ended
            && own.is_some_and(|current| {
                // Only a REAL terminal record from this session suppresses queued live frames:
                // one carrying an exit, which every wrapper's `ended` does. The claim record —
                // this session's own `ended (superseded)` placeholder, deliberately exitless —
                // must not suppress the session's first frames, and a predecessor incarnation's
                // `ended` is history rather than this session's last word.
                current.state == Activity::Ended && current.exit.is_some()
            })
        {
            return Ok(WriteOutcome::Refused(Refusal::Terminal));
        }
        let now_ms = crate::message::now_ms();
        let unchanged =
            !self.interrupted && own.is_some_and(|current| self.same_tuple(current, &pending));
        if unchanged
            && let Some(current) = own
            // A restatement is a no-op only against a record this session already wrote (the
            // token filter above) whose stamp a reader would trust: a stamp beyond the
            // future-skew bound — a backward clock correction's leftover — would otherwise
            // read "fresh" here forever while every reader derives future-skew unknown, so it
            // falls through to the write below, whose next_stamp resets to the writer's clock.
            && current.written_at_ms <= now_ms.saturating_add(duration_ms(HARNESS_STATE_FUTURE_SKEW))
            && now_ms.saturating_sub(current.written_at_ms) < duration_ms(HARNESS_STATE_REFRESH)
        {
            return Ok(WriteOutcome::Coalesced);
        }
        // A landed write is byte-distinct even against a same-millisecond predecessor: the stamp
        // is strictly monotonic per record, at the cost of a bounded forward skew of at most one
        // millisecond per write (writes are transition-scale, so the skew never accumulates
        // meaningfully against the staleness horizon). A stamp is only ever inherited from a
        // record a reader would trust: one already past the future-skew bound is somebody's
        // garbage (or an overflow probe), and inheriting it would poison every later write —
        // the writer's own clock wins instead.
        let written_at_ms = next_stamp(
            on_disk.as_ref().map(|current| current.written_at_ms),
            now_ms,
        );
        let (since_ms, transitions) = match (own, unchanged) {
            (Some(current), true) => (current.since_ms, current.transitions),
            // A condition-only write states the same activity the record already holds, so
            // `sinceMs` — when that state was ENTERED — carries across untouched while the
            // counter still advances for byte-distinctness.
            (Some(current), false) => (
                if pending.condition_only {
                    current.since_ms
                } else {
                    written_at_ms
                },
                current.transitions.saturating_add(1),
            ),
            (None, _) => (
                written_at_ms,
                on_disk
                    .as_ref()
                    .map_or(0, |current| current.transitions.saturating_add(1)),
            ),
        };
        // The semantic clock belongs to the FAULT, not to the write that carries it: a fault
        // restated unchanged keeps the instant it was first observed. Without this an activity
        // edge under a standing fault — several per turn on an SSE producer — would re-mint
        // `observedAtMs` and an automatic recovery could outrun its own deadline forever.
        let condition = match pending.condition {
            Some(Stated::Fault(mut fault)) => {
                if let Some(Stated::Fault(before)) = own.and_then(|current| current.condition.as_ref())
                    && before.same_fault(&fault)
                {
                    fault.observed_at_ms = before.observed_at_ms;
                }
                Some(Stated::Fault(fault))
            }
            other => other,
        };
        if self.schema == SCHEMA_V3 {
            // `absent` is not writable and neither is silence: a version 3 record whose condition
            // axis nobody ever stated is not a weaker record, it is one this build's own reader
            // rejects. Refuse it as a value — the producer must state the axis once (through
            // [`Writer::publish`]) before an activity-only frame can ride on top of it.
            let Some(condition) = condition else {
                return Ok(WriteOutcome::Refused(Refusal::Unstated));
            };
            write_record_v3(
                &self.path,
                &RecordV3 {
                    schema: self.schema.to_string(),
                    agent: self.agent.clone(),
                    harness: self.harness.to_string(),
                    state: pending.state,
                    input_buffer: pending.input_buffer,
                    condition: condition.to_wire(),
                    ask: ask_to_wire(pending.ask),
                    conversation_ref: pending
                        .conversation
                        .as_ref()
                        // The link's incarnation is the WRITING session's, stamped here rather
                        // than taken from the producer: two sources for one fact make a mismatch
                        // with the record's own `incarnation` unfalsifiable for a reader.
                        .map(|state| conversation_to_wire(state, &self.session)),
                    reason: pending.reason,
                    exit: pending.exit,
                    pty_session: pending.pty_session.clone(),
                    incarnation: self.session.clone(),
                    seq,
                    since_ms,
                    written_at_ms,
                    transitions,
                },
            )?;
        } else {
            write_record(
                &self.path,
                &Record {
                    schema: self.schema.to_string(),
                    agent: self.agent.clone(),
                    harness: self.harness.to_string(),
                    state: pending.state,
                    blocked_on: pending.legacy_ask.0,
                    input_buffer: pending.input_buffer,
                    ask: pending.legacy_ask.1,
                    reason: pending.reason,
                    exit: pending.exit,
                    pty_session: pending.pty_session.clone(),
                    incarnation: self.session.clone(),
                    seq,
                    since_ms,
                    written_at_ms,
                    transitions,
                },
            )?;
        }
        self.interrupted = false;
        Ok(WriteOutcome::Landed)
    }

    /// Whether the pending write says exactly what the record already says, in the vocabulary the
    /// emitted version can carry. Under versions 1 and 2 that is the legacy comparison unchanged,
    /// so nothing version 3 adds can make a legacy restatement write.
    fn same_tuple(&self, current: &Snapshot, pending: &Pending) -> bool {
        if current.state != pending.state
            || current.input_buffer != pending.input_buffer
            || current.reason != pending.reason
            || current.exit != pending.exit
        {
            return false;
        }
        if self.schema != SCHEMA_V3 {
            return current.legacy_ask == pending.legacy_ask;
        }
        current.ask == pending.ask
            && current.conversation == pending.conversation
            && match (current.condition.as_ref(), pending.condition.as_ref()) {
                (Some(before), Some(now)) => before.same_statement(now),
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            }
    }

    /// Re-stamp the heartbeat for whatever live state is on disk. Nothing on disk means nothing
    /// to keep fresh, and a terminal record is never re-stamped. The on-disk record is
    /// authoritative: a wrapper heartbeat re-stamps the newest state, including one a hook or
    /// channel process wrote after this writer's last observation. A predecessor session's record
    /// — one written before this session started — is preserved for counter continuity but is
    /// never heartbeat-eligible: re-stamping it would keep a dead session's state fresh forever.
    /// It becomes eligible once any writer of this session observes something.
    ///
    /// A heartbeat touches the TRANSPORT stamp and nothing else. Under version 3 that is the
    /// whole point: `condition.observedAtMs` and `condition.nextObservationDueMs` are the
    /// SEMANTIC clock, and a producer that stayed alive through a throttle window must not be
    /// able to heartbeat its way out of an overdue automatic recovery. `sinceMs`, the transition
    /// counter, the condition, the ask, and the conversation bridge are all carried across
    /// byte-identically.
    pub fn heartbeat(&mut self) -> anyhow::Result<()> {
        let _lock = self.locked()?;
        let now_ms = crate::message::now_ms();
        match read_current(&self.path) {
            CurrentRecord::Legacy(mut current) => {
                // A schema this writer does not own must not be round-tripped through this
                // version's record type, and a record this *session* does not own must never be
                // kept fresh: a lingering predecessor re-stamping its successor's record would
                // keep a dead seat's state alive for cross-host readers, and a successor
                // re-stamping a predecessor's would resurrect history. Token equality decides,
                // in both directions.
                if current.schema != self.schema
                    || current.state == Activity::Ended
                    || current.incarnation != self.session
                {
                    return Ok(());
                }
                current.written_at_ms = restamp(current.written_at_ms, now_ms);
                write_record(&self.path, &current)
            }
            CurrentRecord::V3(mut current) => {
                if current.schema != self.schema
                    || current.state == Activity::Ended
                    || current.incarnation != self.session
                {
                    return Ok(());
                }
                current.written_at_ms = restamp(current.written_at_ms, now_ms);
                write_record_v3(&self.path, &current)
            }
            CurrentRecord::Absent | CurrentRecord::Unreadable => Ok(()),
        }
    }

    /// Write the terminal record for this session. Idempotent-shaped: callers on racing teardown
    /// paths may both call it.
    ///
    /// This is the LEGACY terminal path and, like every legacy call, it states no condition axis:
    /// under version 3 it carries forward whatever condition the session's own record already
    /// holds and refuses — visibly, through [`Writer::publish`]'s typed refusal vocabulary — when
    /// there is none to carry. A version 3 producer writes its terminal frame through
    /// [`Writer::publish`], where the refusal is a value rather than a dropped write.
    pub fn ended(&mut self, exit: impl Into<String>) -> anyhow::Result<()> {
        self.observe(
            Observation::new(Activity::Ended, BlockedOn::None, InputBuffer::Unknown)
                .with_exit(exit),
        )
    }
}

/// The derived view a consumer reads. `state` already folds in staleness, future skew,
/// malformation, and (when a probe is supplied) session liveness; [`Observed::indeterminacy`]
/// names which derivation produced an `unknown`, so no absence is silent. Every version projects
/// into this one type: version 3's tagged axes are carried directly, and versions 1 and 2 project
/// their legacy pair into [`Observed::human_ask`] while leaving [`Observed::condition`]
/// explicitly absent.
#[derive(Debug, Clone, PartialEq)]
pub struct Observed {
    pub state: Activity,
    pub blocked_on: BlockedOn,
    pub input_buffer: InputBuffer,
    pub ask: Ask,
    pub harness: Option<String>,
    pub since_ms: Option<u64>,
    pub exit: Option<String>,
    /// Two meanings, one field, kept for compatibility: on a definite observation it is the
    /// record's own diagnostic `reason`, and on an indeterminate one it is a projection of
    /// [`Indeterminacy::reason`] — never computed independently of it.
    pub reason: Option<String>,
    /// The subject the record names, with the namespace its version decided. `None` whenever the
    /// observation is indeterminate: an uninterpretable record proves no subject either.
    pub subject: Option<RecordSubject>,
    /// The EXACT schema the bytes declared, whenever they declared one at all — including a
    /// version this build cannot interpret, and including records that then read indeterminate.
    /// Projected because a migration needs a POSITIVE drain gate: "every row reads
    /// `st2.harness-state.v3`" is provable from this field, while "no row is still v1" is not
    /// provable from any absence.
    pub schema: Option<String>,
    /// Typed indeterminacy, present exactly when this observation is indeterminate. The
    /// authoritative field for new consumers.
    pub indeterminacy: Option<Indeterminacy>,
    /// The condition axis. `Absent` for versions 1 and 2, which have no such axis: never `clear`,
    /// and no fault is inferred from their legacy words.
    pub condition: ConditionView,
    /// The tagged ask axis — an actual human prompt. Versions 1 and 2 project their legacy
    /// `blockedOn`/`ask` pair into it.
    pub human_ask: HumanAsk,
    /// The conversation bridge, or `None` when the record states nothing about one. `None` is not
    /// `Unsupported`: silence is not a capability claim.
    pub conversation: Option<ConversationRef>,
}

impl Observed {
    fn indeterminate(reason: &str, harness: Option<String>) -> Self {
        // The single constructor for an indeterminate observation: every absence routes here, so
        // no path can derive `idle` — or anything else — from missing evidence, and the typed
        // indeterminacy and its legacy projection are minted from one value.
        Self {
            state: Activity::Unknown,
            blocked_on: BlockedOn::Unknown,
            input_buffer: InputBuffer::Unknown,
            ask: Ask::Unknown,
            harness,
            since_ms: None,
            exit: None,
            reason: Some(reason.to_string()),
            subject: None,
            schema: None,
            indeterminacy: Some(Indeterminacy {
                reason: reason.to_string(),
                evidence_age_ms: None,
            }),
            condition: ConditionView::Absent,
            human_ask: HumanAsk::Unknown,
            conversation: None,
        }
    }

    /// Attach the version the bytes declared. An indeterminate observation keeps it: the drain
    /// gate must still see the version of a record that went stale, and `unsupported-schema` is
    /// precisely the row whose declared version an operator needs.
    fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    /// Age the evidence an indeterminate observation was derived from. Only the reader knows the
    /// clock it compared against, so the age is attached here rather than guessed by a consumer.
    fn with_evidence_age(mut self, age_ms: Option<u64>) -> Self {
        if let Some(indeterminacy) = self.indeterminacy.as_mut() {
            indeterminacy.evidence_age_ms = age_ms;
        }
        self
    }
}

/// Result of a same-host session-liveness probe. `Indeterminate` (an unreadable registry, e.g. a
/// reader without the session dir the writer used) must not downgrade anything: unprovable
/// evidence is never reported as death.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLiveness {
    Alive,
    Dead,
    Indeterminate,
}

/// Read an agent's observed harness state. `None` means no record exists — no driver has ever
/// observed this agent, which is different from `unknown`. `probe` is the optional same-host
/// liveness cross-check for the record's pty session; pass `None` for cross-host reads.
pub fn read(path: &Path, probe: Option<&dyn Fn(&str) -> SessionLiveness>) -> Option<Observed> {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        // Only proven absence is absence; a record that exists but cannot be read is
        // indeterminate, never silently "no observation".
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return Some(Observed::indeterminate("unreadable-record", None)),
    };
    Some(read_raw_at(&raw, probe, crate::message::now_ms()))
}

fn read_raw_at(
    raw: &[u8],
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
    now_ms: u64,
) -> Observed {
    // The envelope is decoded FIRST and carries only what every version spells identically, so
    // the version discriminator — not a guess at which body type happens to parse — decides who
    // may interpret the bytes. The discriminator gates interpretation because a schema outside
    // the versions this build understands may spell its words like these while meaning something
    // else, so nothing definite may be derived from it.
    let Ok(envelope) = serde_json::from_slice::<Envelope>(raw) else {
        return Observed::indeterminate("malformed-record", None);
    };
    let harness = envelope.harness.clone();
    // A zero stamp is the serde default for bytes that carried none: no age, rather than an age
    // of "since the epoch".
    let evidence_age_ms = (envelope.written_at_ms > 0)
        .then(|| now_ms.saturating_sub(envelope.written_at_ms));
    if !is_supported_schema(&envelope.schema) {
        return Observed::indeterminate("unsupported-schema", harness)
            .with_schema(&envelope.schema)
            .with_evidence_age(evidence_age_ms);
    }
    if envelope.schema == SCHEMA_V3 {
        read_v3_at(raw, harness, probe, now_ms)
    } else {
        read_legacy_at(raw, harness, probe, now_ms)
    }
    .with_schema(&envelope.schema)
    .with_evidence_age(evidence_age_ms)
}

/// Versions 1 and 2: one shape, whose only difference is the namespace of `agent`.
fn read_legacy_at(
    raw: &[u8],
    harness: Option<String>,
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
    now_ms: u64,
) -> Observed {
    let Ok(record) = serde_json::from_slice::<Record>(raw) else {
        return Observed::indeterminate("malformed-record", harness);
    };
    // Within the pair only the meaning of `agent` differs, and that meaning travels typed on
    // `subject` rather than being guessed by the consumer.
    let subject = RecordSubject::for_version(record.schema == SCHEMA_V2, record.agent.clone());
    if let Err(reason) = freshness(record.written_at_ms, now_ms) {
        return Observed::indeterminate(reason, harness);
    }
    if record.state == Activity::Unknown {
        // A literal `unknown` is never written by this crate; treat one like malformation.
        return Observed::indeterminate("literal-unknown", harness);
    }
    if is_claim_placeholder(record.state, record.exit.as_deref(), record.reason.as_deref()) {
        return Observed::indeterminate("claimed", harness);
    }
    if let Err(reason) = liveness(record.state, record.pty_session.as_deref(), probe) {
        return Observed::indeterminate(reason, harness);
    }
    Observed {
        state: record.state,
        blocked_on: record.blocked_on,
        input_buffer: record.input_buffer,
        ask: record.ask,
        harness,
        since_ms: Some(record.since_ms),
        exit: record.exit,
        reason: record.reason,
        subject: Some(subject),
        schema: Some(record.schema),
        indeterminacy: None,
        // Versions 1 and 2 carry no condition axis. It is EXPLICITLY absent rather than `clear`,
        // and no fault is inferred from `blockedOn`, from a diagnostic `reason`, or from anything
        // else in these records: they never spoke about faults.
        condition: ConditionView::Absent,
        human_ask: HumanAsk::from_legacy(record.blocked_on, record.ask),
        conversation: None,
    }
}

/// Version 3, strictly. Edge validation runs BEFORE the freshness and liveness gates: a record
/// whose axes contradict each other is not a weaker observation, it is not an observation, and its
/// age says nothing about that. Every rejection carries its own reason word, so an operator can
/// tell a producer bug from a stale seat.
fn read_v3_at(
    raw: &[u8],
    harness: Option<String>,
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
    now_ms: u64,
) -> Observed {
    let Ok(record) = serde_json::from_slice::<RecordV3>(raw) else {
        return Observed::indeterminate("malformed-record", harness);
    };
    let condition = match decode_condition(&record.condition, now_ms) {
        Ok(condition) => condition,
        Err(reason) => return Observed::indeterminate(reason, harness),
    };
    let human_ask = match decode_ask(&record.ask) {
        Ok(human_ask) => human_ask,
        Err(reason) => return Observed::indeterminate(reason, harness),
    };
    // The conversation bridge is a CAPABILITY axis, not part of the observation: a producer that
    // states it badly has still told us what the harness is doing and what is wrong with it.
    // Discarding the whole record over a half-stated link would trade a real fault and a waiting
    // human for a broken side-channel, so the damage is contained to this axis, which degrades to
    // `unavailable` carrying st2's own closed rejection word — never provider prose, and never a
    // `linked` claim a consumer could act on.
    let conversation = record
        .conversation_ref
        .as_ref()
        .map(|wire| decode_conversation(wire).unwrap_or_else(|reason| {
            ConversationRef::Unavailable(Some(reason.to_string()))
        }));
    if let Err(reason) = freshness(record.written_at_ms, now_ms) {
        return Observed::indeterminate(reason, harness);
    }
    if record.state == Activity::Unknown {
        return Observed::indeterminate("literal-unknown", harness);
    }
    if is_claim_placeholder(record.state, record.exit.as_deref(), record.reason.as_deref()) {
        return Observed::indeterminate("claimed", harness);
    }
    if let Err(reason) = liveness(record.state, record.pty_session.as_deref(), probe) {
        return Observed::indeterminate(reason, harness);
    }
    // The legacy pair is projected from the tagged axis, so a consumer pinned to
    // `blockedOn`/`ask` keeps reading exactly what those fields have always meant.
    let (blocked_on, ask) = human_ask.to_legacy();
    Observed {
        state: record.state,
        blocked_on,
        input_buffer: record.input_buffer,
        ask,
        harness,
        since_ms: Some(record.since_ms),
        exit: record.exit,
        reason: record.reason,
        // Version 3 keeps version 2's meaning: `agent` is the immutable agent ID.
        subject: Some(RecordSubject::AgentId(record.agent)),
        schema: Some(record.schema),
        indeterminacy: None,
        condition,
        human_ask,
        conversation,
    }
}

/// The transport freshness gate, shared by every version: the record's own embedded stamp against
/// the reader's clock, never file mtime.
fn freshness(written_at_ms: u64, now_ms: u64) -> Result<(), &'static str> {
    if written_at_ms > now_ms {
        if written_at_ms - now_ms > duration_ms(HARNESS_STATE_FUTURE_SKEW) {
            return Err("future-skew");
        }
    } else if now_ms - written_at_ms >= duration_ms(HARNESS_STATE_STALE) {
        return Err("stale");
    }
    Ok(())
}

/// The claim placeholder is a fence, not an observation: the session wrote it at startup and has
/// observed nothing yet. Reading it as definite `ended` would flip a live seat to dead for every
/// consumer whose harness never publishes its first frame promptly — indeterminate, distinctly,
/// until the session's first real observation or the ordinary horizon.
fn is_claim_placeholder(state: Activity, exit: Option<&str>, reason: Option<&str>) -> bool {
    state == Activity::Ended && exit.is_none() && reason == Some("superseded")
}

/// The same-host session-liveness cross-check, shared by every version. A live record that names
/// no session offers nothing to check — without this rule it would stay definite through an
/// external SIGKILL for the whole staleness horizon, which is exactly the window the cross-check
/// exists to close. Writers therefore must fence live states (enforced in `observe`); a fenced
/// record whose session is provably dead is downgraded, and an unreadable registry still
/// downgrades nothing. A fresh `ended` survives: a terminal record is supposed to outlive its
/// writer.
fn liveness(
    state: Activity,
    pty_session: Option<&str>,
    probe: Option<&dyn Fn(&str) -> SessionLiveness>,
) -> Result<(), &'static str> {
    if state == Activity::Ended {
        return Ok(());
    }
    let Some(probe) = probe else {
        return Ok(());
    };
    let Some(session) = pty_session else {
        return Err("unfenced-record");
    };
    if probe(session) == SessionLiveness::Dead {
        return Err("session-dead");
    }
    Ok(())
}

/// Strict decode of the condition axis. The rejections are the point: a producer that contradicts
/// itself must be visible as a producer bug rather than averaged into a plausible-looking state.
fn decode_condition(wire: &WireCondition, now_ms: u64) -> Result<ConditionView, &'static str> {
    match wire.kind.as_str() {
        "clear" => {
            // A `clear` carrying fault evidence is two contradictory claims in one record. Picking
            // either would be a guess, and guessing toward `clear` would silence a real fault.
            if wire.category.is_some()
                || wire.code.is_some()
                || wire.recovery.is_some()
                || wire.observed_at_ms.is_some()
                || wire.next_observation_due_ms.is_some()
            {
                return Err("contradictory-clear");
            }
            Ok(ConditionView::Clear)
        }
        "fault" => {
            // Recovery and the semantic observation time are what make a fault actionable and
            // ageable; a fault without them cannot be routed or timed out, so it is not a fault.
            let recovery = Recovery::parse(wire.recovery.as_deref().ok_or("incomplete-fault")?);
            let observed_at_ms = wire.observed_at_ms.ok_or("incomplete-fault")?;
            if let Some(due) = wire.next_observation_due_ms {
                // A deadline belongs to a recovery that is supposed to happen by itself. On a
                // recovery this build RECOGNIZES as not automatic it is a producer error, not a
                // hint. On an unrecognized recovery word it is not: the record may well be an
                // automatic class this build has never heard of, and rejecting it would turn a
                // fault that pages under `unknown` recovery into a non-paging indeterminate row —
                // trading a visible fault for silence over a word we simply do not know.
                if matches!(recovery, Recovery::Human | Recovery::Terminal) {
                    return Err("misplaced-observation-due");
                }
                // An inverted deadline is incoherent whatever the recovery class: it claims the
                // next observation was already due before the condition was observed.
                if due < observed_at_ms {
                    return Err("inverted-observation-due");
                }
            }
            // The overdue decision uses the SEMANTIC clock at READ time: the transport heartbeat
            // that keeps this record fresh never moves the deadline, and an automatic recovery
            // that missed it stops being evidence of anything automatic.
            let overdue = recovery == Recovery::Automatic
                && wire.next_observation_due_ms.is_some_and(|due| now_ms > due);
            Ok(ConditionView::Fault(Fault {
                category: wire.category.as_deref().and_then(FaultCategory::parse),
                code: wire.code.clone(),
                recovery: if overdue { Recovery::Unknown } else { recovery },
                observed_at_ms,
                next_observation_due_ms: wire.next_observation_due_ms,
                detail: wire.detail.clone(),
                overdue,
            }))
        }
        // `absent` is the LEGACY projection, not a writable state: a version 3 producer with no
        // condition to report has not finished projecting its harness.
        "absent" => Err("written-absent-condition"),
        _ => Err("unsupported-condition-kind"),
    }
}

/// Strict decode of the ask axis. Unknown is a first-class member here, so a future ask kind
/// degrades to "an ask of an unstated kind" rather than failing the record: the ask itself was
/// stated, and dropping it would lose a waiting human.
fn decode_ask(wire: &WireAsk) -> Result<HumanAsk, &'static str> {
    match wire.kind.as_str() {
        "none" => {
            if wire.ask.is_some() {
                return Err("contradictory-ask");
            }
            Ok(HumanAsk::None)
        }
        "pending" => Ok(HumanAsk::Pending(
            wire.ask.as_deref().map_or(AskKind::Unknown, AskKind::parse),
        )),
        "unknown" => Ok(HumanAsk::Unknown),
        _ => Err("unsupported-ask-kind"),
    }
}

/// Strict decode of the conversation bridge, whose failures are CONTAINED to this axis by its
/// caller: an `Err` here degrades the reference to `unavailable` carrying the rejection word,
/// never the whole observation. A `linked` reference must carry every part of what
/// makes it usable — namespace, opaque identity, incarnation, an explicit mutability claim with
/// its evidence, and a finite verification bound — because a half-stated link is precisely the
/// thing a consumer would trust and should not.
fn decode_conversation(wire: &WireConversation) -> Result<ConversationRef, &'static str> {
    match wire.kind.as_str() {
        "unsupported" => Ok(ConversationRef::Unsupported),
        "unavailable" => Ok(ConversationRef::Unavailable(wire.reason.clone())),
        "linked" => Ok(ConversationRef::Linked(ConversationLink {
            driver: wire.driver.clone().ok_or("incomplete-conversation-link")?,
            conversation: wire
                .conversation
                .clone()
                .ok_or("incomplete-conversation-link")?,
            incarnation: wire
                .incarnation
                .clone()
                .ok_or("incomplete-conversation-link")?,
            history_mutability: HistoryMutability::parse(
                wire.history_mutability
                    .as_deref()
                    .ok_or("incomplete-conversation-link")?,
            ),
            capability_evidence: CapabilityEvidence::parse(
                wire.capability_evidence
                    .as_deref()
                    .ok_or("incomplete-conversation-link")?,
            ),
            // Finite and positive: an unbounded or zero bound is an unverifiable claim, which is
            // worse than an unavailable one because it looks verified.
            verified_through_ms: wire
                .verified_through_ms
                .filter(|&bound| bound > 0)
                .ok_or("unbounded-conversation-verification")?,
        })),
        _ => Err("unsupported-conversation-kind"),
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// What the record file holds, tri-state: absence, bytes this version cannot decode as its own
/// record shape, or a parsed record. Collapsing `Unreadable` into `Absent` would let a writer
/// treat an undeserializable record as a virgin seat — restarting the sequence and counter over
/// live foreign state.
enum StoredRecord {
    Absent,
    /// Bytes this build cannot decode as a legacy record, carrying the shared ownership envelope
    /// whenever they are a versioned record at all. A version 3 record is exactly that from this
    /// build's version 2 writer: uninterpretable, yet its schema fence and its ownership sequence
    /// must still be honored, which is impossible if the bytes are reported as opaque garbage.
    Unreadable(Option<Envelope>),
    Parsed(Record),
}

/// The version-independent ownership envelope. Every version of this record spells these fields
/// identically and means the same thing by them, which is what lets the claim fence, the sequence
/// floor, and stamp continuity work across a version this build cannot interpret.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope {
    schema: String,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    transitions: u64,
    #[serde(default)]
    written_at_ms: u64,
}

/// The version 3 durable record. Additive-tolerant on read like its predecessor (no
/// `deny_unknown_fields`): a reader pinned to an older crate may be older than the writer. Note
/// what is absent by construction — no `blockedOn`, and nothing content-bearing about a
/// conversation — so a legacy `Record` can never decode these bytes and a transcript can never
/// ride them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordV3 {
    schema: String,
    agent: String,
    harness: String,
    state: Activity,
    input_buffer: InputBuffer,
    condition: WireCondition,
    ask: WireAsk,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation_ref: Option<WireConversation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty_session: Option<String>,
    #[serde(default)]
    incarnation: String,
    #[serde(default)]
    seq: u64,
    since_ms: u64,
    written_at_ms: u64,
    transitions: u64,
}

/// The condition axis as it rides the record: a discriminator plus the union of every arm's
/// fields. Decoded through [`decode_condition`] rather than a serde-tagged enum, because the
/// contradictions worth rejecting — a `clear` carrying fault evidence, a fault without recovery,
/// a deadline on a recovery nobody automates — are exactly what a tolerant tagged decode would
/// silently accept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireCondition {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_observation_due_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireAsk {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ask: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireConversation {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    driver: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_mutability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    capability_evidence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_through_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

fn read_stored(path: &Path) -> StoredRecord {
    match fs::read(path) {
        // Only proven absence is absence: a file that exists but cannot be read (permissions,
        // IO) is somebody's record — treating it as a virgin seat would let a token-only write
        // or a wrapperless claim rename over live state it never saw.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => StoredRecord::Absent,
        Err(_) => StoredRecord::Unreadable(None),
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(record) => StoredRecord::Parsed(record),
            // Undecodable as this build's record shape, but still possibly a record: the envelope
            // is what the ownership protocol needs, and it is version-independent.
            Err(_) => StoredRecord::Unreadable(serde_json::from_slice(&bytes).ok()),
        },
    }
}

fn write_record(path: &Path, record: &Record) -> anyhow::Result<()> {
    // This record stages beside itself, unchanged: the sibling driver record
    // ([`crate::harness_context`]) stages outside the agent subtree because a replicated
    // temporary name becomes a durable key, and moving this one's staging is a separate change.
    let dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();
    write_json_atomic(path, record, &dir, ".harness-state")
}

/// The diagnostic detail a version 3 claim fence carries on its required condition axis. Named so
/// the producer path can recognize its own fence and refuse to carry that `clear` forward as if a
/// producer had stated it.
const CLAIM_FENCE_DETAIL: &str = "claim fence: nothing observed yet";

fn write_record_v3(path: &Path, record: &RecordV3) -> anyhow::Result<()> {
    // Same staging discipline as its predecessor: beside the record, on the record's filesystem.
    let dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();
    write_json_atomic(path, record, &dir, ".harness-state")
}

/// The record on disk decoded into whichever of the two WRITABLE shapes it is. The two shapes are
/// structurally disjoint — a legacy record has no `condition` object and a version 3 record has
/// no `blockedOn` — so this never guesses: it decodes, in the historical order, and reports what
/// parsed. `Unreadable` carries nothing on purpose: the producer path refuses those bytes
/// outright, and the ownership envelope inside them matters only to [`claim_locked`], which reads
/// it through [`read_stored`].
enum CurrentRecord {
    Absent,
    Unreadable,
    Legacy(Record),
    V3(RecordV3),
}

fn read_current(path: &Path) -> CurrentRecord {
    let bytes = match fs::read(path) {
        // Only proven absence is absence, exactly as [`read_stored`] decides it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return CurrentRecord::Absent,
        Err(_) => return CurrentRecord::Unreadable,
        Ok(bytes) => bytes,
    };
    if let Ok(record) = serde_json::from_slice::<Record>(&bytes) {
        return CurrentRecord::Legacy(record);
    }
    if let Ok(record) = serde_json::from_slice::<RecordV3>(&bytes) {
        return CurrentRecord::V3(record);
    }
    CurrentRecord::Unreadable
}

/// The condition a write actually states, after carry-forward and semantic-clock preservation.
/// Distinct from the producer's [`ConditionReport`], which additionally spells "leave this axis
/// alone", and from the reader's [`ConditionView`], which additionally spells the legacy
/// projection `absent` and carries read-time normalization no producer may forge.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Stated {
    Clear { detail: Option<String> },
    Fault(FaultReport),
}

impl Stated {
    fn to_wire(&self) -> WireCondition {
        match self {
            Self::Clear { detail } => WireCondition {
                kind: "clear".to_string(),
                category: None,
                code: None,
                recovery: None,
                observed_at_ms: None,
                next_observation_due_ms: None,
                detail: detail.clone(),
            },
            Self::Fault(fault) => WireCondition {
                kind: "fault".to_string(),
                category: Some(fault.category.as_str().to_string()),
                code: fault.code.clone(),
                recovery: Some(fault.recovery.as_str().to_string()),
                observed_at_ms: Some(fault.observed_at_ms),
                next_observation_due_ms: fault.next_observation_due_ms,
                detail: fault.detail.clone(),
            },
        }
    }

    fn fault(&self) -> Option<&FaultReport> {
        match self {
            Self::Fault(fault) => Some(fault),
            Self::Clear { .. } => None,
        }
    }

    /// Whether two statements say the same thing about the harness, IGNORING when the fault was
    /// observed: the semantic clock is when a fault started, not when it was last restated, so a
    /// producer restating one fault every SSE frame must not look like a new statement.
    fn same_statement(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Clear { detail: ours }, Self::Clear { detail: theirs }) => ours == theirs,
            (Self::Fault(ours), Self::Fault(theirs)) => ours.same_fault(theirs),
            _ => false,
        }
    }
}

/// The condition axis as the RECORD states it, or `None` when the bytes state nothing this build
/// can continue. Lenient on purpose: this is the writer's view of its own previous statement, and
/// a statement it cannot reconstruct is simply no statement to preserve.
fn stated_condition(wire: &WireCondition) -> Option<Stated> {
    match wire.kind.as_str() {
        "clear" => Some(Stated::Clear {
            detail: wire.detail.clone(),
        }),
        "fault" => Some(Stated::Fault(FaultReport {
            category: FaultCategory::parse(wire.category.as_deref()?)?,
            code: wire.code.clone(),
            recovery: Recovery::parse(wire.recovery.as_deref()?),
            observed_at_ms: wire.observed_at_ms?,
            next_observation_due_ms: wire.next_observation_due_ms,
            detail: wire.detail.clone(),
        })),
        _ => None,
    }
}

/// The conversation bridge as the RECORD states it, in the producer's write vocabulary. The
/// link's incarnation is deliberately dropped: the writing session stamps it, so a carried-forward
/// link cannot claim an incarnation the record does not have.
fn stated_conversation(wire: &WireConversation) -> Option<ConversationState> {
    match wire.kind.as_str() {
        "unsupported" => Some(ConversationState::Unsupported),
        "unavailable" => Some(ConversationState::Unavailable(wire.reason.clone())),
        "linked" => Some(ConversationState::Linked(ConversationClaim {
            driver: wire.driver.clone()?,
            conversation: wire.conversation.clone()?,
            history_mutability: HistoryMutability::parse(wire.history_mutability.as_deref()?),
            capability_evidence: CapabilityEvidence::parse(wire.capability_evidence.as_deref()?),
            verified_through_ms: wire.verified_through_ms?,
        })),
        _ => None,
    }
}

/// The on-disk record normalized to the axes a producer write must fence against and continue.
/// One type for both writable shapes, so ownership — sequence direction, token equality, stamp
/// continuity, transition counting — is decided in exactly one place whichever version is on
/// disk and whichever version this build emits.
struct Snapshot {
    schema: String,
    incarnation: String,
    seq: u64,
    transitions: u64,
    written_at_ms: u64,
    since_ms: u64,
    state: Activity,
    /// The pty session the RECORD names, which vouches for its live states. Carried so a
    /// condition-only write — whose caller may be a hook process with no pty of its own — restates
    /// the authoritative fence rather than stripping it.
    pty_session: Option<String>,
    input_buffer: InputBuffer,
    /// The legacy pair VERBATIM, so a version 2 restatement compares byte-for-byte against what
    /// the record actually holds rather than against a re-projection of it.
    legacy_ask: (BlockedOn, Ask),
    ask: HumanAsk,
    condition: Option<Stated>,
    conversation: Option<ConversationState>,
    reason: Option<String>,
    exit: Option<String>,
}

impl Snapshot {
    fn from_legacy(record: Record) -> Self {
        Self {
            schema: record.schema,
            incarnation: record.incarnation,
            seq: record.seq,
            transitions: record.transitions,
            written_at_ms: record.written_at_ms,
            since_ms: record.since_ms,
            state: record.state,
            pty_session: record.pty_session,
            input_buffer: record.input_buffer,
            legacy_ask: (record.blocked_on, record.ask),
            ask: HumanAsk::from_legacy(record.blocked_on, record.ask),
            // Versions 1 and 2 carry no condition axis at all, which is why the condition
            // operations refuse under them rather than inventing a place to keep one.
            condition: None,
            conversation: None,
            reason: record.reason,
            exit: record.exit,
        }
    }

    fn from_v3(record: RecordV3) -> Self {
        let ask = decode_ask(&record.ask).unwrap_or(HumanAsk::Unknown);
        Self {
            schema: record.schema,
            incarnation: record.incarnation,
            seq: record.seq,
            transitions: record.transitions,
            written_at_ms: record.written_at_ms,
            since_ms: record.since_ms,
            state: record.state,
            pty_session: record.pty_session,
            input_buffer: record.input_buffer,
            legacy_ask: project_ask_legacy(ask),
            ask,
            condition: stated_condition(&record.condition),
            conversation: record
                .conversation_ref
                .as_ref()
                .and_then(stated_conversation),
            reason: record.reason,
            exit: record.exit,
        }
    }

    /// Whether this record is a claim fence: a session that has claimed the seat and observed
    /// nothing. Its axes are a fence, not a statement, so nothing is carried forward from it.
    fn is_fence(&self) -> bool {
        is_claim_placeholder(self.state, self.exit.as_deref(), self.reason.as_deref())
    }
}

/// Whether a WRAPPERLESS claimer may take the record over, decided from whichever version is on
/// disk. Both writable shapes are decoded, because a hooks-only session must be able to claim a
/// version 3 record for exactly the reasons it can claim a version 2 one — bytes it cannot decode
/// would otherwise read as "somebody's live record" forever, and after the writer cutover that
/// would be every record. Split out from [`claim_wrapperless`] so eligibility is provable for a
/// version this build's own writer is not currently emitting, in either direction.
fn wrapperless_eligible(ours: &str, current: &CurrentRecord, now_ms: u64) -> bool {
    let snapshot = match current {
        CurrentRecord::Absent => return true,
        // Unreadable bytes stay ineligible for this cautious path: a claimer that cannot read what
        // it is replacing must not replace it.
        CurrentRecord::Unreadable => return false,
        CurrentRecord::Legacy(record) => Snapshot::from_legacy(record.clone()),
        CurrentRecord::V3(record) => Snapshot::from_v3(record.clone()),
    };
    let stale = now_ms.saturating_sub(snapshot.written_at_ms) >= duration_ms(HARNESS_STATE_STALE);
    let wrapperless_owner = snapshot.incarnation.is_empty()
        || snapshot.incarnation.starts_with(WRAPPERLESS_PREFIX);
    let real_terminal = snapshot.state == Activity::Ended && snapshot.exit.is_some();
    // The same one-way schema fence as the written claim, applied here so the cautious path
    // refuses (`Ok(None)`) instead of reaching `claim_locked` and erroring: a build whose writer
    // is behind the record on disk has no eligible takeover at all, however stale or orphaned
    // that record looks.
    claim_may_supersede(ours, Some(snapshot.schema.as_str()))
        && (wrapperless_owner || real_terminal || stale)
}

/// The heartbeat re-stamp, shared by both writable shapes: strictly beyond the record's own stamp
/// so every landed heartbeat is byte-distinct, and never inheriting a stamp past the trust bound.
fn restamp(previous: u64, now_ms: u64) -> u64 {
    if previous <= now_ms.saturating_add(duration_ms(HARNESS_STATE_FUTURE_SKEW)) {
        now_ms.max(previous.saturating_add(1))
    } else {
        now_ms
    }
}

/// Take one driver record's exclusive cross-process lock, held for a read→decide→rename cycle.
/// The lock file is a permanent sibling of the record and the guard releases on drop (close).
/// Shared with [`crate::harness_context`], which owns a sibling record with its own lock file:
/// the transport is common, the ownership protocol above it is not.
pub(crate) fn lock_exclusive(lock_path: &Path) -> anyhow::Result<fs::File> {
    if let Some(dir) = lock_path.parent() {
        fs::create_dir_all(dir)?;
    }
    let lock = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(lock_path)?;
    let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
    anyhow::ensure!(rc == 0, "locking {} failed", lock_path.display());
    Ok(lock)
}

/// Stage-and-rename one newline-terminated JSON record. Atomic when `staging_dir` is on the
/// record's filesystem, which every caller must ensure — `staging_dir` is explicit precisely
/// because the two driver records answer "where may a temporary name live" differently.
pub(crate) fn write_json_atomic<T: Serialize>(
    path: &Path,
    value: &T,
    staging_dir: &Path,
    tmp_prefix: &str,
) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    fs::create_dir_all(staging_dir)?;
    let tmp = staging_dir.join(format!(
        "{tmp_prefix}.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&tmp, &bytes)?;
    // rename over the target — atomic on the same filesystem.
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp); // best-effort cleanup
        return Err(e.into());
    }
    Ok(())
}

/// The per-record monotonic stamp: strictly beyond the previous on-disk stamp when that stamp is
/// inside the future-skew trust bound (a stamp beyond it is somebody's garbage or an overflow
/// probe, and inheriting it would poison every later write), and the writer's own clock
/// otherwise. Takes the stamp rather than the record so a version this build cannot interpret
/// still contributes its own continuity through the shared envelope.
fn next_stamp(previous: Option<u64>, now_ms: u64) -> u64 {
    previous
        .filter(|&previous| {
            previous <= now_ms.saturating_add(duration_ms(HARNESS_STATE_FUTURE_SKEW))
        })
        .map_or(now_ms, |previous| now_ms.max(previous.saturating_add(1)))
}

/// Ordinal of a supported schema version, or `None` for a schema this build cannot interpret.
fn schema_rank(schema: &str) -> Option<u8> {
    match schema {
        SCHEMA_V1 => Some(1),
        SCHEMA_V2 => Some(2),
        SCHEMA_V3 => Some(3),
        _ => None,
    }
}

/// Whether a claim written under `ours` may supersede the record `current`.
///
/// Takeover across supported versions is ONE-WAY. Same version is ordinary supersession. An older
/// supported version may be superseded: that is the v1 → v2 migration, and it is the only
/// direction in which the record's meaning gains information. A NEWER supported version is
/// refused, because during the coordinated writer cutover a still-running reader-first binary
/// whose writer is v1 would otherwise overwrite a migrated, ID-bearing record with bus-address
/// semantics under a higher ownership sequence — silently destroying the migrated meaning of a
/// live record, with no later act able to tell that it happened. An unsupported schema holds
/// nothing this build could preserve, so an explicit claim still supersedes it.
///
/// `ours` is a parameter rather than a read of [`SCHEMA`] so the refused direction is provable in
/// one build: the whole defect is about two builds writing different versions at the same time.
fn claim_may_supersede(ours: &str, current: Option<&str>) -> bool {
    let Some(current) = current else {
        return true;
    };
    match (schema_rank(current), schema_rank(ours)) {
        (Some(on_disk), Some(ours)) => on_disk <= ours,
        _ => true,
    }
}

/// Claim session ownership of an agent's record, as a WRITTEN act under the record's lock: the
/// takeover record supersedes whatever is on disk — `ended` with reason `superseded`, no exit,
/// the new session's token, and the next ownership sequence — and the claimed sequence is
/// returned for the wrapper to adopt and export beside its token. Writing the claim makes it
/// atomic: racing claimers serialize on the lock and mint DISTINCT sequences, and a
/// predecessor's fresh live record is superseded at relaunch even though the pty-name-based
/// probe cannot tell the sessions apart. Readers derive indeterminate (`claimed`) from the
/// fresh placeholder — a fence, not an observation — until the session's first real
/// observation replaces it.
pub fn claim(
    agent_dir: &Path,
    agent: impl Into<String>,
    harness: &'static str,
    token: &str,
) -> anyhow::Result<u64> {
    let writer = Writer::new(agent_dir, agent, harness, None);
    let _lock = writer.locked()?;
    claim_locked(&writer, token)
}

/// The claim's body, under an already-held record lock.
fn claim_locked(writer: &Writer, token: &str) -> anyhow::Result<u64> {
    // Unreadable bytes are superseded like anything else — that is exactly what the claim is
    // for — but their MEANING cannot be continued (the record's axes restart). Their OWNERSHIP
    // must survive them regardless: a claim restarting at one would sit below a lingering
    // predecessor's claim, whose next write would replace the new claim and then permanently
    // fence the new session out. Two independent mechanisms preserve it: the version-independent
    // envelope, which a record of a version this build cannot interpret still carries, and the
    // floor sidecar, written under this same lock on every claim, which survives bytes carrying
    // no envelope at all. Only both being damaged loses the floor, and that residual is
    // documented.
    let stored = read_stored(&writer.path);
    let on_disk = match &stored {
        StoredRecord::Parsed(record) => Some(record),
        StoredRecord::Absent | StoredRecord::Unreadable(_) => None,
    };
    // A record whose meaning this build cannot read but whose envelope it can. Collapsing this
    // into "nothing on disk" is how a version 2 claim would silently overwrite a migrated
    // version 3 record — the very defect the one-way fence below exists to prevent, reappearing
    // through the parse failure instead of through the comparison.
    let foreign = match &stored {
        StoredRecord::Unreadable(envelope) => envelope.as_ref(),
        StoredRecord::Absent | StoredRecord::Parsed(_) => None,
    };
    let on_disk_schema = on_disk
        .map(|record| record.schema.as_str())
        .or_else(|| foreign.map(|envelope| envelope.schema.as_str()));
    // The semantic fence, before anything is minted or written. The SEQUENCE below deliberately
    // stays monotonic across the one-way v1 → v2 migration — it is a counter, not a meaning, and
    // restarting it at one would sit below a lingering predecessor's claim and fence the new
    // session out permanently — but the record's MEANING may only ever move forward.
    anyhow::ensure!(
        claim_may_supersede(writer.schema, on_disk_schema),
        "refusing to claim a `{}` record while this build writes `{}`: \
         taking it over would downgrade a migrated record's `agent` field to a bus address",
        on_disk_schema.unwrap_or_default(),
        writer.schema
    );
    let floor_path = writer.path.with_file_name(SEQ_FLOOR_NAME);
    let floor = fs::read_to_string(&floor_path)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok());
    let previous_seq = on_disk
        .map(|record| record.seq)
        .or_else(|| foreign.map(|envelope| envelope.seq));
    let previous_stamp = on_disk
        .map(|record| record.written_at_ms)
        .or_else(|| foreign.map(|envelope| envelope.written_at_ms));
    let previous_transitions = on_disk
        .map(|record| record.transitions)
        .or_else(|| foreign.map(|envelope| envelope.transitions));
    let highest = previous_seq.max(floor);
    // A saturated sequence would mint SHARED ownership forever after: every later claim would
    // return the same MAX, and two sessions holding equal claims are exactly the ambiguity the
    // sequence exists to remove. Fail loudly; producers degrade to token-only and stay alive.
    anyhow::ensure!(
        highest.is_none_or(|seq| seq < u64::MAX),
        "ownership sequence exhausted; refusing a shared claim"
    );
    let seq = highest.map_or(1, |seq| seq.saturating_add(1));
    let now_ms = crate::message::now_ms();
    let written_at_ms = next_stamp(previous_stamp, now_ms);
    let transitions = previous_transitions.map_or(0, |transitions| transitions.saturating_add(1));
    // The fence is written in the shape this build EMITS, so the one writer-selection point
    // carries the claim with it: a version 2 claim over a version 3 record is already refused
    // above, and a build whose writer moved to version 3 must not mint a fence its own reader
    // would call malformed.
    if writer.schema == SCHEMA_V3 {
        write_record_v3(
            &writer.path,
            &RecordV3 {
                schema: writer.schema.to_string(),
                agent: writer.agent.clone(),
                harness: writer.harness.to_string(),
                state: Activity::Ended,
                input_buffer: InputBuffer::Unknown,
                // Version 3 requires the condition axis to be present, and the fence has
                // observed nothing to say about it. Its value is structurally unreachable:
                // [`is_claim_placeholder`] makes this record indeterminate (`claimed`) before any
                // consumer sees a condition, and the producer path treats a fence as an UNSTATED
                // condition rather than carrying this `clear` forward — so no health is claimed
                // here, and the detail says so in the bytes.
                condition: WireCondition {
                    kind: "clear".to_string(),
                    category: None,
                    code: None,
                    recovery: None,
                    observed_at_ms: None,
                    next_observation_due_ms: None,
                    detail: Some(CLAIM_FENCE_DETAIL.to_string()),
                },
                ask: WireAsk {
                    kind: "unknown".to_string(),
                    ask: None,
                },
                conversation_ref: None,
                reason: Some("superseded".to_string()),
                exit: None,
                pty_session: None,
                incarnation: token.to_string(),
                seq,
                since_ms: written_at_ms,
                written_at_ms,
                transitions,
            },
        )?;
    } else {
        write_record(
            &writer.path,
            &Record {
                schema: writer.schema.to_string(),
                agent: writer.agent.clone(),
                harness: writer.harness.to_string(),
                state: Activity::Ended,
                blocked_on: BlockedOn::None,
                input_buffer: InputBuffer::Unknown,
                ask: Ask::None,
                reason: Some("superseded".to_string()),
                exit: None,
                pty_session: None,
                incarnation: token.to_string(),
                seq,
                since_ms: written_at_ms,
                written_at_ms,
                transitions,
            },
        )?;
    }
    // The floor accompanies every act that establishes ownership; its own failure
    // modes must never be quiet ones.
    persist_floor(&writer.path, seq);
    // A session boundary empties the window, so the numeric sibling is removed with the same
    // act that supersedes this record (HC-R15): the new incarnation reads "no context yet"
    // rather than the previous one's 190k, which is what a crash-looping seat would otherwise
    // show for the whole hour of that record's horizon. This runs while THIS record's lock is
    // held and takes the sibling's lock inside it, so the order is state → context. That is
    // the only place the two are ever held together and `harness_context` never takes this
    // one, so the ordering is acyclic and no writer can deadlock against it. The claim stands
    // whether or not the removal succeeds, but never silently.
    if let Some(agent_dir) = writer.path.parent()
        && let Err(error) = crate::harness_context::remove(agent_dir)
    {
        tracing::warn!(
            "st2 harness-state: clearing the harness-context record for {} failed: {error}",
            agent_dir.display()
        );
    }
    Ok(seq)
}

/// Persist the sequence-floor sidecar for `seq`. The floor is the safety net for the record
/// itself going unreadable, so its own failure modes must not be quiet ones: stage-and-rename
/// keeps a torn write from corrupting the current floor, and a failed write is logged — the
/// ownership still stands (losing the floor only matters if the record later becomes
/// unreadable), but never silently.
fn persist_floor(record_path: &Path, seq: u64) {
    let floor_path = record_path.with_file_name(SEQ_FLOOR_NAME);
    let staged = floor_path.with_file_name(".harness-state.seq.tmp");
    if let Err(error) =
        fs::write(&staged, format!("{seq}\n")).and_then(|()| fs::rename(&staged, &floor_path))
    {
        tracing::warn!(
            "st2 harness-state: writing the sequence floor {} failed: {error}",
            floor_path.display()
        );
    }
}

/// The token prefix wrapperless Claude sessions derive from Claude's own session id.
pub const WRAPPERLESS_PREFIX: &str = "claude-session-";

/// A WRAPPERLESS session boundary's claim — eligibility and the written takeover as ONE act
/// under the record lock, because check-then-act across two acquisitions is a race: a
/// hooks-only SessionStart landing between a wrapper's startup reads could otherwise steal the
/// sequence the wrapper was about to export. A wrapper's claim is always legitimate — it owns
/// the seat's lifecycle — but a wrapperless claimer (a hook fired by any interactive session
/// that inherited the project-scoped registration) must not supersede live wrapper state. It
/// claims over nothing, over records no wrapper minted (fellow wrapperless tokens), over REAL
/// terminal records (exit-bearing), and over staleness — never over a live wrapper record, and
/// never over a wrapper's FRESH claim placeholder (`ended (superseded)`, exitless,
/// wrapper-shaped token): that placeholder is a session mid-startup, not an ended one, though
/// an abandoned placeholder past the staleness horizon is claimable like any orphan.
/// `Ok(None)` = ineligible; unreadable bytes are also ineligible for this cautious path.
pub fn claim_wrapperless(
    agent_dir: &Path,
    agent: impl Into<String>,
    harness: &'static str,
    token: &str,
) -> anyhow::Result<Option<u64>> {
    let writer = Writer::new(agent_dir, agent, harness, None);
    let _lock = writer.locked()?;
    let eligible = wrapperless_eligible(
        writer.schema,
        &read_current(&writer.path),
        crate::message::now_ms(),
    );
    if !eligible {
        return Ok(None);
    }
    claim_locked(&writer, token).map(Some)
}

/// A process-unique session incarnation token: pid, wall-clock, and a process-local counter.
/// Uniqueness across the writers that can actually race on one record (processes on one host)
/// is what matters; no cryptographic strength is implied or needed.
pub fn session_token() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        crate::message::now_ms(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
mod tests {
    use super::*;

    /// The LEGACY writer surface, pinned to version 2 through the emitted-schema seam.
    ///
    /// Everything reached through [`Writer::observe`] and [`Writer::ended`] is a version 2
    /// statement by construction: the legacy pair is the whole vocabulary those calls have, and
    /// version 3 refuses a write whose condition axis nobody stated. Pinning the helper is what
    /// keeps the compatibility seam honest after activation — these tests assert the bytes and
    /// the ownership rules a version 2 record still has, which every legacy record on disk and
    /// every rolled-back seat depends on. The production version 3 surface is proved through
    /// [`v3_writer`] and the producer block below, which now emits exactly what this build ships.
    fn writer(dir: &Path) -> Writer {
        Writer::new(dir, "hetz.worker", "codex", Some("worker".to_string()))
            .with_emitted_schema(SCHEMA_V2)
    }

    /// A new session arriving the way real wrappers do: a written claim, then adoption. Claim and
    /// writer emit the SAME version — a claim is written by the version its writer emits, so a
    /// mismatched pair would be refused by the schema fence rather than testing anything.
    fn takeover(dir: &Path, harness: &'static str) -> Writer {
        takeover_emitting(dir, harness, SCHEMA_V2)
    }

    fn takeover_emitting(dir: &Path, harness: &'static str, schema: &'static str) -> Writer {
        let token = session_token();
        let seq = claim_emitting(dir, "hetz.worker", harness, &token, schema).unwrap();
        Writer::new(dir, "hetz.worker", harness, Some("worker".to_string()))
            .with_emitted_schema(schema)
            .with_ownership(token, seq)
    }

    /// [`claim`] and [`claim_wrapperless`] as a build emitting `schema` would perform them: the
    /// same locked bodies and the same eligibility, with only the emitted version substituted.
    /// The REFUSED fence direction is unreachable from a build that writes the newest version, so
    /// this seam is the only way it stays provable in one binary — which is exactly what a
    /// staged rollout with an older peer still running needs proved.
    fn claim_emitting(
        dir: &Path,
        agent: &str,
        harness: &'static str,
        token: &str,
        schema: &'static str,
    ) -> anyhow::Result<u64> {
        let writer = Writer::new(dir, agent, harness, None).with_emitted_schema(schema);
        let _lock = writer.locked()?;
        claim_locked(&writer, token)
    }

    fn claim_wrapperless_emitting(
        dir: &Path,
        agent: &str,
        harness: &'static str,
        token: &str,
        schema: &'static str,
    ) -> anyhow::Result<Option<u64>> {
        let writer = Writer::new(dir, agent, harness, None).with_emitted_schema(schema);
        let _lock = writer.locked()?;
        if !wrapperless_eligible(
            writer.schema,
            &read_current(&writer.path),
            crate::message::now_ms(),
        ) {
            return Ok(None);
        }
        claim_locked(&writer, token).map(Some)
    }


    /// The version the legacy-surface block below writes. Every test that states its observation
    /// through [`Writer::observe`], [`Writer::ended`], or a written claim is a VERSION 2 test by
    /// construction: the legacy pair is the whole vocabulary those calls have, and version 3
    /// refuses a write whose condition axis nobody stated. Pinning the block keeps the
    /// compatibility contract — the bytes, the ownership rules, and the claim behaviour every
    /// legacy record on disk and every rolled-back seat depends on — proved after activation.
    const LEGACY_SCHEMA: &str = SCHEMA_V2;

    fn legacy_new(
        dir: &Path,
        agent: impl Into<String>,
        harness: &'static str,
        pty_session: Option<String>,
    ) -> Writer {
        Writer::new(dir, agent, harness, pty_session).with_emitted_schema(LEGACY_SCHEMA)
    }

    fn legacy_claim(
        dir: &Path,
        agent: impl Into<String>,
        harness: &'static str,
        token: &str,
    ) -> anyhow::Result<u64> {
        let agent: String = agent.into();
        claim_emitting(dir, &agent, harness, token, LEGACY_SCHEMA)
    }

    fn legacy_claim_wrapperless(
        dir: &Path,
        agent: impl Into<String>,
        harness: &'static str,
        token: &str,
    ) -> anyhow::Result<Option<u64>> {
        let agent: String = agent.into();
        claim_wrapperless_emitting(dir, &agent, harness, token, LEGACY_SCHEMA)
    }

    /// One record on disk under an explicit schema, with a live stamp and an orphan token, so
    /// every eligibility clause except the schema fence says "claimable".
    ///
    /// The BODY follows the schema: the two writable shapes are structurally disjoint, so a
    /// version 3 label over a legacy body is unreadable bytes rather than a version 3 record, and
    /// planting that would test the undecodable path instead of the fence.
    fn planted(dir: &Path, schema: &str, agent: &str, seq: u64) -> PathBuf {
        let path = harness_state_path(dir);
        let axes = if schema == SCHEMA_V3 {
            r#""condition":{"kind":"clear"},"ask":{"kind":"none"}"#
        } else {
            r#""blockedOn":"none","ask":"none""#
        };
        fs::write(
            &path,
            format!(
                r#"{{"schema":"{schema}","agent":"{agent}","harness":"codex","state":"ended","inputBuffer":"unknown",{axes},"exit":"exit 0","incarnation":"","seq":{seq},"sinceMs":1,"writtenAtMs":{},"transitions":4}}"#,
                crate::message::now_ms()
            ),
        )
        .unwrap();
        path
    }

    fn active() -> Observation {
        Observation::new(Activity::Active, BlockedOn::None, InputBuffer::Unknown)
    }

    #[test]
    fn missing_record_reads_as_none_not_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read(&harness_state_path(tmp.path()), None), None);
    }

    #[test]
    fn observe_then_read_roundtrips_every_writable_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = writer(tmp.path());
        for (state, blocked, buffer) in [
            (Activity::Idle, BlockedOn::None, InputBuffer::Empty),
            (Activity::Active, BlockedOn::Human, InputBuffer::Unknown),
            (Activity::Child, BlockedOn::None, InputBuffer::Nonempty),
            (Activity::Ended, BlockedOn::None, InputBuffer::Unknown),
        ] {
            writer
                .observe(Observation::new(state, blocked, buffer))
                .unwrap();
            let observed = read(&harness_state_path(tmp.path()), None).unwrap();
            assert_eq!(observed.state, state);
            assert_eq!(observed.blocked_on, blocked);
            assert_eq!(observed.input_buffer, buffer);
            assert_eq!(observed.harness.as_deref(), Some("codex"));
            assert!(observed.since_ms.is_some());
        }
    }

    #[test]
    fn unknown_state_is_derived_and_cannot_be_written() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = writer(tmp.path());
        assert!(
            writer
                .observe(Observation::new(
                    Activity::Unknown,
                    BlockedOn::None,
                    InputBuffer::Unknown,
                ))
                .is_err()
        );
        assert_eq!(read(&harness_state_path(tmp.path()), None), None);
    }

    #[test]
    fn every_landed_write_is_byte_distinct_and_fresh_restatements_do_not_write() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let first = fs::read(&path).unwrap();

        // Restating an unchanged state against a fresh record must not touch it…
        std::thread::sleep(Duration::from_millis(2));
        writer.observe(active()).unwrap();
        assert_eq!(
            first,
            fs::read(&path).unwrap(),
            "fresh identical observation must not write"
        );

        // …while an explicit heartbeat and a genuine transition each land distinct bytes.
        std::thread::sleep(Duration::from_millis(2));
        writer.heartbeat().unwrap();
        let second = fs::read(&path).unwrap();
        assert_ne!(first, second, "heartbeat must change bytes");

        std::thread::sleep(Duration::from_millis(2));
        writer
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        let third = fs::read(&path).unwrap();
        assert_ne!(second, third, "transition must change bytes");
    }

    /// The measured failure mode this guards: an SSE-fed producer restating its state ~3×/second
    /// turned into 679 byte-distinct replicated writes in 221 s. Restatements are free.
    #[test]
    fn a_chatty_producer_restating_its_state_causes_zero_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let bytes = fs::read(&path).unwrap();
        for _ in 0..300 {
            writer.observe(active()).unwrap();
        }
        assert_eq!(bytes, fs::read(&path).unwrap());
        let record: Record = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record.transitions, 0);
    }

    #[test]
    fn an_unchanged_observation_re_stamps_only_a_record_older_than_the_refresh_cadence() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        writer.observe(active()).unwrap();

        // Age this session's own record past the refresh cadence without changing its owner.
        let mut aged: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        aged.written_at_ms = crate::message::now_ms() - duration_ms(HARNESS_STATE_REFRESH) - 1;
        aged.since_ms = aged.written_at_ms;
        write_record(&path, &aged).unwrap();

        // The unchanged restatement now lands as a heartbeat-equivalent re-stamp: same state,
        // same transition, fresh stamp.
        writer.observe(active()).unwrap();
        let restamped: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restamped.transitions, aged.transitions);
        assert_eq!(restamped.since_ms, aged.since_ms);
        assert!(restamped.written_at_ms > aged.written_at_ms);
    }

    #[test]
    fn concurrent_writers_defer_to_the_on_disk_record_not_their_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut a = writer(tmp.path());
        a.observe(active()).unwrap();
        let mut b = takeover(tmp.path(), "codex");
        b.observe(Observation::new(
            Activity::Idle,
            BlockedOn::None,
            InputBuffer::Unknown,
        ))
        .unwrap();

        // A's heartbeat re-stamps the newest state on disk — it must not resurrect `active`.
        a.heartbeat().unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.state, Activity::Idle);
        assert_eq!(record.transitions, 2);

        // B's write was a later session's claim, so A is now the straggler: its re-observation
        // is refused rather than treated as a fresh takeover of its successor's record.
        a.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.state, Activity::Idle);
        assert_eq!(record.transitions, 2);
    }

    #[test]
    fn a_heartbeat_never_resurrects_a_peer_processes_terminal_record() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut a = writer(tmp.path());
        a.observe(active()).unwrap();
        let mut b = takeover(tmp.path(), "codex");
        b.ended("signal 9").unwrap();
        let terminal = fs::read(&path).unwrap();

        a.heartbeat().unwrap();
        assert_eq!(fs::read(&path).unwrap(), terminal);
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("signal 9"));
    }

    #[test]
    fn identical_observations_coalesce_without_a_new_transition() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let entered: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        writer.observe(active()).unwrap();
        let restated: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            restated.since_ms, entered.since_ms,
            "since survives restating"
        );
        assert_eq!(restated.transitions, entered.transitions);

        writer
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        let transitioned: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(transitioned.transitions, entered.transitions + 1);
        assert!(transitioned.since_ms >= restated.since_ms);
    }

    #[test]
    fn restart_continues_the_transition_counter() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut first = writer(tmp.path());
        first.observe(active()).unwrap();
        first
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        drop(first);

        let mut second = takeover(tmp.path(), "codex");
        second.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            record.transitions, 3,
            "claim and first observation continue the counter"
        );
    }

    #[test]
    fn staleness_and_future_skew_derive_unknown_with_distinct_reasons() {
        let now_ms = 2_000_000_000_u64;
        let stale_ms = duration_ms(HARNESS_STATE_STALE);
        let skew_ms = duration_ms(HARNESS_STATE_FUTURE_SKEW);
        let raw = |written_at_ms: u64| {
            serde_json::to_vec(&Record {
                schema: SCHEMA_V2.to_string(),
                agent: "hetz.worker".to_string(),
                harness: "codex".to_string(),
                state: Activity::Active,
                blocked_on: BlockedOn::None,
                input_buffer: InputBuffer::Unknown,
                ask: Ask::None,
                reason: None,
                exit: None,
                pty_session: None,
                incarnation: String::new(),
                seq: 0,
                since_ms: written_at_ms,
                written_at_ms,
                transitions: 0,
            })
            .unwrap()
        };

        let fresh = read_raw_at(&raw(now_ms - stale_ms + 1), None, now_ms);
        assert_eq!(fresh.state, Activity::Active);

        let stale = read_raw_at(&raw(now_ms - stale_ms), None, now_ms);
        assert_eq!(stale.state, Activity::Unknown);
        assert_eq!(stale.reason.as_deref(), Some("stale"));
        assert_eq!(stale.blocked_on, BlockedOn::Unknown);

        let bounded_future = read_raw_at(&raw(now_ms + skew_ms), None, now_ms);
        assert_eq!(bounded_future.state, Activity::Active);

        let excessive_future = read_raw_at(&raw(now_ms + skew_ms + 1), None, now_ms);
        assert_eq!(excessive_future.state, Activity::Unknown);
        assert_eq!(excessive_future.reason.as_deref(), Some("future-skew"));
    }

    #[test]
    fn malformed_record_is_unknown_without_mtime_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        for raw in [
            &b"garbage"[..],
            b"{}",
            b"{\"schema\":\"st2.harness-state.v1\"}",
        ] {
            fs::write(&path, raw).unwrap();
            let observed = read(&path, None).unwrap();
            assert_eq!(observed.state, Activity::Unknown);
            assert_eq!(observed.reason.as_deref(), Some("malformed-record"));
        }
    }

    #[test]
    fn a_dead_session_reads_unknown_even_while_fresh_but_ended_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        let dead: &dyn Fn(&str) -> SessionLiveness = &|_| SessionLiveness::Dead;
        let indeterminate: &dyn Fn(&str) -> SessionLiveness = &|_| SessionLiveness::Indeterminate;

        writer.observe(active()).unwrap();
        let observed = read(&path, Some(dead)).unwrap();
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("session-dead"));
        // An unreadable registry proves nothing and downgrades nothing.
        assert_eq!(
            read(&path, Some(indeterminate)).unwrap().state,
            Activity::Active
        );

        writer.ended("signal 9").unwrap();
        let observed = read(&path, Some(dead)).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("signal 9"));
    }

    #[test]
    fn heartbeat_re_stamps_only_live_states_and_never_resurrects_ended() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        // Nothing observed yet: heartbeat is a no-op, not a default write.
        writer.heartbeat().unwrap();
        assert_eq!(read(&path, None), None);

        writer.ended("exit 0").unwrap();
        let terminal = fs::read(&path).unwrap();
        writer.heartbeat().unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            terminal,
            "ended is never re-stamped"
        );
    }

    #[test]
    fn future_vocabulary_degrades_to_indeterminate_not_none() {
        // A schema outside the versions this build understands gates interpretation entirely —
        // even words spelled exactly like these must not decode as anything definite, because the
        // later version may have changed what the same spelling means.
        let raw = br#"{"schema":"st2.harness-state.v4","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"empty","sinceMs":1,"writtenAtMs":9999999999999,"transitions":3,"novelField":true}"#;
        let observed = read_raw_at(raw, None, 9_999_999_999_999);
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("unsupported-schema"));
        assert_eq!(observed.blocked_on, BlockedOn::Unknown);
        assert_eq!(
            observed.schema.as_deref(),
            Some("st2.harness-state.v4"),
            "the declared version is reported even when it cannot be interpreted, so a drain \
             gate can positively account for every row"
        );

        // And on a v1 record with a known state, unknown axis words stay indeterminate.
        let raw = br#"{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"robot","inputBuffer":"overflowing","sinceMs":1,"writtenAtMs":9999999999999,"transitions":3}"#;
        let observed = read_raw_at(raw, None, 9_999_999_999_999);
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(observed.blocked_on, BlockedOn::Unknown);
        assert_eq!(observed.input_buffer, InputBuffer::Unknown);
        assert_eq!(
            observed.human_ask,
            HumanAsk::Unknown,
            "an unrecognized legacy blocked word projects indeterminate, never `none`"
        );
    }

    /// Reader first, twice over: this version reads every record version it will ever be asked
    /// about *before* the corresponding writer exists, and each one's `agent` field is decoded in
    /// the namespace its own discriminator names. A version outside the supported set stays
    /// `unsupported-schema`, so the tolerance is exactly three versions wide rather than
    /// "anything that parses".
    #[test]
    fn both_reserved_versions_are_read_with_their_own_agent_meaning() {
        let v1 = br#"{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"empty","sinceMs":1,"writtenAtMs":1000,"transitions":3}"#;
        let observed = read_raw_at(v1, None, 1_000);
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(
            observed.subject,
            Some(RecordSubject::BusIdentity("hetz.worker".into())),
            "version 1 `agent` is the bus identity"
        );
        assert_eq!(observed.subject.as_ref().unwrap().agent_id(), None);

        // Same bytes in `agent`, different namespace — which is precisely why the version exists:
        // a frozen legacy ID and a bus identity are indistinguishable without the discriminator.
        let v2 = br#"{"schema":"st2.harness-state.v2","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"empty","sinceMs":1,"writtenAtMs":1000,"transitions":3}"#;
        let observed = read_raw_at(v2, None, 1_000);
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(
            observed.subject,
            Some(RecordSubject::AgentId("hetz.worker".into())),
            "version 2 `agent` is the immutable agent ID"
        );
        assert_eq!(observed.subject.as_ref().unwrap().bus_identity(), None);

        // Version 3 keeps version 2's namespace: the fault axis changed, the subject did not.
        let v3 = br#"{"schema":"st2.harness-state.v3","agent":"hetz.worker","harness":"codex","state":"active","inputBuffer":"empty","condition":{"kind":"clear"},"ask":{"kind":"none"},"sinceMs":1,"writtenAtMs":1000,"transitions":3}"#;
        let observed = read_raw_at(v3, None, 1_000);
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(
            observed.subject,
            Some(RecordSubject::AgentId("hetz.worker".into())),
            "version 3 `agent` is the immutable agent ID, exactly as version 2 established"
        );

        // The legacy shape is NOT accepted under the version 3 discriminator: same words, new
        // contract, and a v3 record without the condition axis has not stated the axis at all.
        let mislabeled = br#"{"schema":"st2.harness-state.v3","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"empty","sinceMs":1,"writtenAtMs":1000,"transitions":3}"#;
        let observed = read_raw_at(mislabeled, None, 1_000);
        assert_eq!(observed.reason.as_deref(), Some("malformed-record"));

        let v4 = br#"{"schema":"st2.harness-state.v4","agent":"hetz.worker","harness":"codex","state":"active","inputBuffer":"empty","condition":{"kind":"clear"},"ask":{"kind":"none"},"sinceMs":1,"writtenAtMs":1000,"transitions":3}"#;
        let observed = read_raw_at(v4, None, 1_000);
        assert_eq!(observed.reason.as_deref(), Some("unsupported-schema"));
        assert_eq!(observed.state, Activity::Unknown);
        assert!(
            observed.subject.is_none(),
            "an uninterpretable record proves no subject either"
        );
    }

    /// The production writer's own declaration, after activation: this build emits version 3, so
    /// the record it writes must DECLARE version 3 and carry the condition axis version 3's own
    /// reader requires. The `agent` meaning the immutable-ID cohort activated is carried forward
    /// unchanged — version 3's `agent` is the immutable ID exactly as version 2's was — so the
    /// misattribution the cohort exists to prevent stays prevented, and stamping that ID under
    /// version 1 remains impossible from this build.
    ///
    /// The claim fence is deliberately included: a production session's FIRST write follows a
    /// fence whose axis is not a statement, so this exercises the same bootstrap every wrapper
    /// and adapter does.
    #[test]
    fn the_writer_declares_version_three_and_still_means_an_immutable_id() {
        assert!(EMIT_SCHEMA_V3);
        assert_eq!(SCHEMA, SCHEMA_V3);

        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = takeover_emitting(tmp.path(), "codex", SCHEMA);
        writer
            .publish(Frame::new(
                Activity::Active,
                InputBuffer::Unknown,
                ConditionReport::Clear,
                HumanAsk::None,
            ))
            .unwrap();
        let record = record_json(&path);
        assert_eq!(record["schema"], SCHEMA_V3);
        assert_eq!(record["condition"]["kind"], "clear");
        assert!(
            record.get("blockedOn").is_none(),
            "version 3 replaced the overloaded axis rather than keeping both: {record}"
        );
        // And the reader hands the subject back in the ID namespace, not as a route.
        assert_eq!(
            read(&path, None).unwrap().subject,
            Some(RecordSubject::AgentId(
                record["agent"].as_str().unwrap().to_string()
            ))
        );

        // Writing version 3 does not retype the legacy records already on disk: a v1 record still
        // reads with bus-identity meaning, which is the whole point of keeping the pair readable.
        let v1 = format!(
            r#"{{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"empty","sinceMs":1,"writtenAtMs":{},"transitions":3}}"#,
            crate::message::now_ms()
        );
        fs::write(&path, v1).unwrap();
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(
            observed.subject,
            Some(RecordSubject::BusIdentity("hetz.worker".into()))
        );
        assert_eq!(
            observed.condition,
            ConditionView::Absent,
            "a legacy record's condition is explicitly absent, never health"
        );
    }

    /// The takeover fence is ONE-WAY across supported versions. The defect this pins: during the
    /// coordinated writer cutover, a still-running reader-first binary whose writer is v1 could
    /// claim a migrated v2 record and rewrite its `agent` field as a bus address under a HIGHER
    /// ownership sequence — permanently fencing out the true owner and leaving no trace that the
    /// migrated meaning was destroyed.
    #[test]
    fn a_claim_never_downgrades_a_record_to_an_older_schema() {
        let v2 = Record {
            schema: SCHEMA_V2.to_string(),
            agent: "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1".to_string(),
            harness: "codex".to_string(),
            state: Activity::Active,
            blocked_on: BlockedOn::None,
            input_buffer: InputBuffer::Unknown,
            ask: Ask::None,
            reason: None,
            exit: None,
            pty_session: Some("worker".to_string()),
            incarnation: "other".to_string(),
            seq: 3,
            since_ms: 1,
            written_at_ms: 1,
            transitions: 1,
        };
        let v1 = Record {
            schema: SCHEMA_V1.to_string(),
            agent: "hetz.worker".to_string(),
            ..v2.clone()
        };

        // Refused: a v1 writer must not take over a v2 record.
        assert!(!claim_may_supersede(SCHEMA_V1, Some(v2.schema.as_str())));
        // Allowed, and one-way: the v1 → v2 migration, plus same-version supersession.
        assert!(claim_may_supersede(SCHEMA_V2, Some(v1.schema.as_str())));
        assert!(claim_may_supersede(SCHEMA_V1, Some(v1.schema.as_str())));
        assert!(claim_may_supersede(SCHEMA_V2, Some(v2.schema.as_str())));
        // The same one-way rule now extends to version 3, which is why this build's version 2
        // writer can never take a v3 record over: the fault axis is information a v2 claim would
        // silently destroy, exactly as a v1 claim would destroy the ID namespace.
        assert!(!claim_may_supersede(SCHEMA_V2, Some(SCHEMA_V3)));
        assert!(!claim_may_supersede(SCHEMA_V1, Some(SCHEMA_V3)));
        assert!(claim_may_supersede(SCHEMA_V3, Some(SCHEMA_V2)));
        assert!(claim_may_supersede(SCHEMA_V3, Some(SCHEMA_V3)));
        // Nothing on disk, and bytes no build can interpret, hold nothing to preserve.
        assert!(claim_may_supersede(SCHEMA_V1, None));
        assert!(claim_may_supersede(SCHEMA_V1, Some("st2.harness-state.v4")));
    }

    /// This build writes v3, so the v1 → v3 migration claim is the one it exercises end-to-end:
    /// it lands exactly once, keeps the sequence monotonic across the version change (the counter
    /// is not a meaning, and restarting it would sit below a lingering predecessor's claim and
    /// fence the new session out), and leaves the record declaring the version this build emits.
    /// The claim skips no version on the way: a v1 record is superseded directly, because the
    /// fence is a comparison and not a migration ladder.
    #[test]
    fn a_production_claim_migrates_a_version_one_record_once_and_keeps_the_sequence() {
        assert_eq!(SCHEMA, SCHEMA_V3, "this build writes the newest version");
        let tmp = tempfile::tempdir().unwrap();
        let path = planted(tmp.path(), SCHEMA_V1, "hetz.worker", 7);

        let token = session_token();
        let seq = claim(tmp.path(), "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", "codex", &token).unwrap();
        assert_eq!(seq, 8, "the sequence continues past the version change");
        assert_eq!(record_json(&path)["schema"], SCHEMA_V3);
        assert_eq!(
            record_json(&path)["agent"],
            "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1"
        );

        // The claim itself is a fence, not an observation, so it reads indeterminate (`claimed`)
        // and proves no subject. The session's first real statement is what a consumer joins to a
        // catalog, and that must land in the ID namespace — and because a fence states nothing,
        // that first statement is the one that has to state the condition axis.
        assert!(read(&path, None).unwrap().subject.is_none());
        let mut owner = Writer::new(
            tmp.path(),
            "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1",
            "codex",
            Some("worker".into()),
        )
        .with_ownership(token, seq);
        assert_eq!(
            owner
                .publish(Frame::new(
                    Activity::Active,
                    InputBuffer::Unknown,
                    ConditionReport::Unchanged,
                    HumanAsk::None,
                ))
                .unwrap(),
            WriteOutcome::Refused(Refusal::Unstated),
            "the fence is not carried forward, so nothing is inherited to ride on"
        );
        assert!(
            owner
                .publish(Frame::new(
                    Activity::Active,
                    InputBuffer::Unknown,
                    ConditionReport::Clear,
                    HumanAsk::None,
                ))
                .unwrap()
                .landed()
        );
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(
            observed.subject,
            Some(RecordSubject::AgentId(
                "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1".into()
            )),
            "the migrated record names the ID namespace"
        );
    }

    /// Wrapperless eligibility respects the same fence. The planted record is exit-bearing
    /// terminal AND orphan-tokened, so every other clause votes "claimable"; only the schema
    /// decides. A same-version plant proves the test is not vacuous.
    #[test]
    fn wrapperless_eligibility_respects_the_schema_fence() {
        let same = tempfile::tempdir().unwrap();
        planted(same.path(), SCHEMA, "hetz.worker", 2);
        assert!(
            claim_wrapperless(same.path(), "hetz.worker", "codex", &session_token())
                .unwrap()
                .is_some(),
            "an orphaned terminal record of this build's own version is claimable"
        );

        // The refused direction is not reachable from a build that writes the newest version, so
        // the fence itself is asserted where it is decidable.
        assert!(!claim_may_supersede(SCHEMA_V1, Some(SCHEMA_V2)));
    }

    /// Ownership-sequence ADOPTION never crosses schemas: a same-token record under the other
    /// version is a differently-versioned writer of this session, and inheriting its sequence
    /// would let this build write over a meaning it did not produce.
    #[test]
    fn sequence_adoption_never_crosses_schemas() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let token = session_token();
        // A v1 record carrying THIS session's token: same token, wrong version.
        fs::write(
            &path,
            format!(
                r#"{{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"unknown","incarnation":"{token}","seq":5,"sinceMs":1,"writtenAtMs":{},"transitions":1}}"#,
                crate::message::now_ms()
            ),
        )
        .unwrap();
        let before = fs::read(&path).unwrap();

        let mut token_only = legacy_new(tmp.path(), "hetz.worker", "codex", Some("worker".into()));
        token_only.session = token.clone();
        assert!(
            !token_only.observe_unless_ended(active()).unwrap(),
            "a non-claiming writer must refuse a record of the other version"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "the cross-version record is left byte-identical"
        );
    }

    #[test]
    fn interrupt_forces_a_fresh_transition_even_for_a_restated_fresh_tuple() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());

        writer.observe(active()).unwrap();
        let entered: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        // Evidence lost and returned: the restated tuple must not claim continuity, overriding
        // both the coalesce branch and the fresh-restatement no-op guard.
        writer.interrupt();
        writer.observe(active()).unwrap();
        let resumed: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(resumed.transitions, entered.transitions + 1);
        assert!(resumed.since_ms >= entered.since_ms);

        // The flag clears on the write: the next restatement coalesces again.
        let bytes = fs::read(&path).unwrap();
        writer.observe(active()).unwrap();
        assert_eq!(bytes, fs::read(&path).unwrap());
    }

    /// HC-R15: the written claim that supersedes this record also removes the numeric sibling, so
    /// a new incarnation reads "no context yet" rather than the previous incarnation's fill. The
    /// wrapperless claim path shares the same body and therefore the same behaviour.
    #[test]
    fn the_relaunch_claim_removes_the_harness_context_record() {
        use crate::harness_context::{self, Harness, Reading, harness_context_path};

        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path().join("agents").join("hetz").join("worker");
        fs::create_dir_all(&agent_dir).unwrap();
        let context_path = harness_context_path(&agent_dir);

        let fill = || Reading {
            used_tokens: Some(190_000),
            window_tokens: Some(200_000),
            used_percent: Some(95.0),
            ..Reading::default()
        };
        harness_context::Writer::new(&agent_dir, "hetz.worker", Harness::Claude)
            .unwrap()
            .observe(fill())
            .unwrap();
        assert!(harness_context::read(&context_path).is_some());

        // A wrapper relaunch: claim the state record, and the sibling goes with it.
        let token = session_token();
        legacy_claim(&agent_dir, "hetz.worker", "claude", &token).unwrap();
        assert!(
            harness_context::read(&context_path).is_none(),
            "the new incarnation must read `no context yet`"
        );
        assert!(!context_path.exists());
        // The state record's own claim placeholder is untouched by the removal: it still reads
        // indeterminate-because-`claimed`, the fence the claim just wrote.
        assert_eq!(
            read(&harness_state_path(&agent_dir), None)
                .unwrap()
                .reason
                .as_deref(),
            Some("claimed")
        );

        // Claiming a seat that never had a context record is not an error.
        legacy_claim(&agent_dir, "hetz.worker", "claude", &session_token()).unwrap();

        // …and the wrapperless boundary, which routes through the same body. It is eligible only
        // over a seat no wrapper holds, so it gets its own.
        let hooks_dir = tmp.path().join("agents").join("hetz").join("hooked");
        fs::create_dir_all(&hooks_dir).unwrap();
        let hooks_context = harness_context_path(&hooks_dir);
        harness_context::Writer::new(&hooks_dir, "hetz.hooked", Harness::Claude)
            .unwrap()
            .observe(fill())
            .unwrap();
        let wrapperless = format!("{WRAPPERLESS_PREFIX}abc");
        assert!(
            legacy_claim_wrapperless(&hooks_dir, "hetz.hooked", "claude", &wrapperless)
                .unwrap()
                .is_some(),
            "the claim must actually have happened"
        );
        assert!(harness_context::read(&hooks_context).is_none());
    }

    #[test]
    fn a_predecessor_sessions_record_is_never_heartbeat_eligible() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let predecessor = Record {
            schema: SCHEMA_V2.to_string(),
            agent: "hetz.worker".to_string(),
            harness: "codex".to_string(),
            state: Activity::Active,
            blocked_on: BlockedOn::None,
            input_buffer: InputBuffer::Unknown,
            ask: Ask::None,
            reason: None,
            exit: None,
            pty_session: Some("worker".to_string()),
            incarnation: String::new(),
            seq: 0,
            since_ms: 5,
            written_at_ms: 5,
            transitions: 3,
        };
        write_record(&path, &predecessor).unwrap();
        let stale_bytes = fs::read(&path).unwrap();

        // A restarted wrapper must not keep a dead session's state fresh forever.
        let mut writer = takeover(tmp.path(), "codex");
        writer.heartbeat().unwrap();
        assert_ne!(
            fs::read(&path).unwrap(),
            stale_bytes,
            "the written claim itself supersedes the predecessor"
        );
        assert_eq!(
            read(&path, None).unwrap().reason.as_deref(),
            Some("claimed"),
            "the fresh placeholder reads indeterminate, never definite ended"
        );

        // Once this session observes something, heartbeats re-stamp again.
        writer.observe(active()).unwrap();
        let observed_bytes = fs::read(&path).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        writer.heartbeat().unwrap();
        assert_ne!(fs::read(&path).unwrap(), observed_bytes);
    }

    #[test]
    fn an_unreadable_record_is_indeterminate_not_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        fs::create_dir(&path).unwrap();
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("unreadable-record"));
    }

    #[test]
    fn observe_unless_ended_drops_live_frames_after_a_peer_terminal_record() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        // The channel and wrapper are sibling processes of ONE session and share its token —
        // that sharing is what makes the wrapper's terminal record the session's last word.
        let token = session_token();
        let mut channel = legacy_new(tmp.path(), "hetz.worker", "pi", Some("worker".into()))
            .with_session(token.clone());
        let mut wrapper =
            legacy_new(tmp.path(), "hetz.worker", "pi", Some("worker".into())).with_session(token);

        assert!(channel.observe_unless_ended(active()).unwrap());
        wrapper.ended("signal 9").unwrap();
        let terminal = fs::read(&path).unwrap();

        // A queued live frame arriving after the terminal record must not resurrect the session.
        assert!(
            !channel
                .observe_unless_ended(Observation::new(
                    Activity::Idle,
                    BlockedOn::None,
                    InputBuffer::Unknown,
                ))
                .unwrap()
        );
        assert_eq!(fs::read(&path).unwrap(), terminal);
    }

    #[test]
    fn a_new_sessions_first_observation_writes_through_a_matching_fresh_predecessor() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut predecessor = writer(tmp.path());
        predecessor.observe(active()).unwrap();
        let before: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        // The successor is a different incarnation — token inequality alone forces the
        // write-through, even inside the same millisecond. Without it, its matching first
        // observation would be a no-op and the ownership gate would then reject every heartbeat
        // while the record quietly aged out.
        let mut successor = takeover(tmp.path(), "codex");
        successor.observe(active()).unwrap();
        let claimed: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(
            claimed.written_at_ms >= before.written_at_ms,
            "takeover must write"
        );

        std::thread::sleep(Duration::from_millis(2));
        successor.heartbeat().unwrap();
        let stamped: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(
            stamped.written_at_ms > claimed.written_at_ms,
            "heartbeat must be eligible after the takeover write"
        );
    }

    #[test]
    fn a_session_writer_marked_interrupted_opens_a_fresh_transition_on_takeover() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        writer(tmp.path()).observe(active()).unwrap();
        let before: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        let mut successor = takeover(tmp.path(), "codex");
        successor.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            record.transitions,
            before.transitions + 2,
            "the written claim and the first observation each transition"
        );
        assert!(
            record.since_ms > before.since_ms,
            "sinceMs must never span a session boundary"
        );
    }

    #[test]
    fn foreign_schemas_are_never_coalesced_restamped_or_treated_as_terminal() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        // Now that the writer emits version 2, a version-1 straggler is the foreign shape: its
        // `agent` means a route, so adopting it as this writer's own record would coalesce an
        // address onto an ID.
        let foreign = br#"{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"ended","blockedOn":"none","inputBuffer":"unknown","sinceMs":5,"writtenAtMs":99999999999999,"transitions":7,"novel":true}"#;
        fs::write(&path, foreign).unwrap();

        // Heartbeat leaves a foreign record byte-identical rather than stripping its fields —
        // and a token-only writer cannot replace it either: supersession is a claim's job.
        let mut unclaimed = writer(tmp.path());
        unclaimed.heartbeat().unwrap();
        assert_eq!(fs::read(&path).unwrap(), foreign.to_vec());
        assert!(!unclaimed.observe_unless_ended(active()).unwrap());
        assert_eq!(fs::read(&path).unwrap(), foreign.to_vec());

        // A claiming session replaces it wholesale, continuing the counter for byte-distinctness.
        let mut writer = takeover(tmp.path(), "codex");
        assert!(writer.observe_unless_ended(active()).unwrap());
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.schema, SCHEMA_V2);
        assert_eq!(record.state, Activity::Active);
        assert_eq!(record.transitions, 9);
    }

    #[test]
    fn a_predecessor_terminal_record_does_not_suppress_a_new_sessions_live_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        writer(tmp.path()).ended("exit 0").unwrap();
        let terminal: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        // Deliberately no delay: a same-millisecond takeover is the ambiguous case a timestamp
        // boundary got wrong, and token inequality decides it.
        let _ = terminal;
        let mut successor = takeover(tmp.path(), "pi");
        assert!(
            successor.observe_unless_ended(active()).unwrap(),
            "a restarted seat must replace its predecessor's terminal record"
        );
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.state, Activity::Active);
    }

    #[test]
    fn live_observations_require_a_pty_session_and_unfenced_live_records_read_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut unfenced = legacy_new(tmp.path(), "hetz.worker", "codex", None);
        assert!(
            unfenced.observe(active()).is_err(),
            "live states need a fence"
        );
        unfenced.ended("exit 0").unwrap();

        // A live record that names no session offers the probe nothing to check: with a probe
        // available it is indeterminate, while the terminal record stays definite.
        let alive: &dyn Fn(&str) -> SessionLiveness = &|_| SessionLiveness::Alive;
        assert_eq!(read(&path, Some(alive)).unwrap().state, Activity::Ended);
        let live_unfenced = format!(
            r#"{{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"unknown","sinceMs":1,"writtenAtMs":{},"transitions":1}}"#,
            crate::message::now_ms()
        );
        fs::write(&path, live_unfenced).unwrap();
        let observed = read(&path, Some(alive)).unwrap();
        assert_eq!(observed.state, Activity::Unknown);
        assert_eq!(observed.reason.as_deref(), Some("unfenced-record"));
        // Without a probe (a cross-host reader) the record keeps its staleness-only semantics.
        assert_eq!(read(&path, None).unwrap().state, Activity::Active);
    }

    #[test]
    fn ask_kind_roundtrips_and_unknown_words_decode_indeterminate() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        writer
            .observe(
                Observation::new(Activity::Active, BlockedOn::Human, InputBuffer::Unknown)
                    .with_ask(Ask::Question)
                    .with_reason("question"),
            )
            .unwrap();
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.ask, Ask::Question);
        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .contains("\"ask\":\"question\"")
        );

        // A record predating the axis defaults to `none`; a future word decodes indeterminate.
        let raw: String = fs::read_to_string(&path)
            .unwrap()
            .replace("\"ask\":\"question\",", "");
        fs::write(&path, &raw).unwrap();
        assert_eq!(read(&path, None).unwrap().ask, Ask::None);
        fs::write(
            &path,
            raw.replace(
                "\"blockedOn\":\"human\"",
                "\"blockedOn\":\"human\",\"ask\":\"telepathy\"",
            ),
        )
        .unwrap();
        assert_eq!(read(&path, None).unwrap().ask, Ask::Unknown);
    }

    #[test]
    fn a_lingering_predecessor_cannot_heartbeat_its_successors_record() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut old = writer(tmp.path());
        old.observe(active()).unwrap();

        let mut successor = takeover(tmp.path(), "codex");
        successor
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        let bytes = fs::read(&path).unwrap();

        // The old wrapper outlives its replacement briefly; its heartbeat must not keep the
        // successor's record fresh — the successor's death would otherwise stay invisible to
        // cross-host readers for as long as the straggler lives.
        old.heartbeat().unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn landed_heartbeats_are_byte_distinct_and_strictly_monotonic_even_same_millisecond() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        writer.observe(active()).unwrap();

        let mut previous: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        for _ in 0..5 {
            let before_bytes = fs::read(&path).unwrap();
            writer.heartbeat().unwrap();
            let after_bytes = fs::read(&path).unwrap();
            assert_ne!(
                after_bytes, before_bytes,
                "every landed heartbeat re-stamps bytes"
            );
            let current: Record = serde_json::from_slice(&after_bytes).unwrap();
            assert!(
                current.written_at_ms > previous.written_at_ms,
                "stamps are strictly monotonic per record"
            );
            previous = current;
        }
    }

    #[test]
    fn sibling_writers_sharing_a_session_token_coalesce_and_heartbeat_each_other() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let token = session_token();
        let mut wrapper = legacy_new(tmp.path(), "hetz.worker", "claude", Some("worker".into()))
            .with_session(token.clone());
        let mut hook = legacy_new(tmp.path(), "hetz.worker", "claude", Some("worker".into()))
            .with_session(token);

        hook.observe(active()).unwrap();
        let entered: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        // A sibling's restatement coalesces (no transition churn across hook processes)…
        let mut hook2 = legacy_new(tmp.path(), "hetz.worker", "claude", Some("worker".into()))
            .with_session(entered.incarnation.clone());
        hook2.observe(active()).unwrap();
        let restated: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restated.transitions, entered.transitions);
        assert_eq!(restated.since_ms, entered.since_ms);

        // …and the wrapper's heartbeat re-stamps the sibling-written state.
        wrapper.heartbeat().unwrap();
        let stamped: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(stamped.written_at_ms > restated.written_at_ms);
        assert_eq!(stamped.state, Activity::Active);
    }

    /// Cluster A: ownership has a direction. A successor's claim replaces the predecessor's
    /// record, and the predecessor's late writes — live AND terminal — are refused, not treated
    /// as a fresh takeover.
    #[test]
    fn a_lingering_predecessors_late_writes_are_refused_after_the_successors_claim() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut predecessor = writer(tmp.path());
        predecessor.observe(active()).unwrap();

        let mut successor = takeover(tmp.path(), "codex");
        successor
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        let claimed = fs::read(&path).unwrap();

        // The straggler's live frame, its queued terminal write, and its unless-ended variant
        // all land nothing.
        predecessor.observe(active()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), claimed);
        predecessor.ended("signal 9").unwrap();
        assert_eq!(fs::read(&path).unwrap(), claimed);
        assert!(!predecessor.observe_unless_ended(active()).unwrap());
        assert_eq!(fs::read(&path).unwrap(), claimed);
        assert_eq!(read(&path, None).unwrap().state, Activity::Idle);
    }

    /// Cluster A: adopted ownership (the env-exported token+seq) writes while the disk is at or
    /// below its claim — including performing the session's first write — and is refused once a
    /// later session claims past it.
    #[test]
    fn adopted_ownership_writes_up_to_its_claim_and_is_refused_beyond_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        writer(tmp.path()).observe(active()).unwrap();

        // The wrapper writes the claim and exports it; the hook adopts the pair.
        let token = session_token();
        let seq = legacy_claim(tmp.path(), "hetz.worker", "claude", &token).unwrap();
        let mut hook = legacy_new(tmp.path(), "hetz.worker", "claude", Some("worker".into()))
            .with_ownership(token.clone(), seq);
        hook.observe(Observation::new(
            Activity::Idle,
            BlockedOn::None,
            InputBuffer::Unknown,
        ))
        .unwrap();
        assert_eq!(read(&path, None).unwrap().state, Activity::Idle);

        // A later session claims past it; the adopted writer becomes the straggler.
        let mut next = legacy_new(tmp.path(), "hetz.worker", "claude", Some("worker".into()));
        next.observe(active()).unwrap();
        let after = fs::read(&path).unwrap();
        hook.observe(Observation::new(
            Activity::Idle,
            BlockedOn::None,
            InputBuffer::Unknown,
        ))
        .unwrap();
        assert_eq!(fs::read(&path).unwrap(), after);
    }

    /// A3: a stamp beyond the future-skew bound — garbage or an overflow probe — is never
    /// inherited; the writer's own clock wins and nothing overflows.
    #[test]
    fn untrusted_future_stamps_are_reset_not_inherited() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let poisoned = format!(
            r#"{{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"unknown","ptySession":"worker","incarnation":"other","seq":3,"sinceMs":1,"writtenAtMs":{},"transitions":1}}"#,
            u64::MAX
        );
        fs::write(&path, poisoned).unwrap();

        let mut writer = takeover(tmp.path(), "codex");
        writer.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let now = crate::message::now_ms();
        assert!(record.written_at_ms <= now.saturating_add(duration_ms(HARNESS_STATE_FUTURE_SKEW)));
        assert_eq!(
            record.seq, 4,
            "the claim still advances past the poisoned record"
        );
        assert_eq!(read(&path, None).unwrap().state, Activity::Active);
    }

    /// T2: the claim is a WRITTEN act under the record lock — racing claimers mint DISTINCT
    /// sequences instead of tying, which dissolved the former dual-claim residual.
    #[test]
    fn racing_claims_serialize_on_the_lock_and_mint_distinct_sequences() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let dir = dir.clone();
                std::thread::spawn(move || {
                    legacy_claim(&dir, "hetz.worker", "codex", &session_token()).unwrap()
                })
            })
            .collect();
        let mut seqs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len(), 4, "every racing claim minted its own sequence");
    }

    /// T3: at relaunch the claim supersedes the predecessor's still-fresh live record — the
    /// pty-name-based probe cannot tell the sessions apart, so the record itself must — and the
    /// seat reads indeterminate (`claimed`) until the new session's first real observation.
    #[test]
    fn a_relaunch_claim_supersedes_a_fresh_live_predecessor_record() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        writer(tmp.path()).observe(active()).unwrap();

        let token = session_token();
        let seq = legacy_claim(tmp.path(), "hetz.worker", "codex", &token).unwrap();
        let observed = read(&path, None).unwrap();
        assert_eq!(
            observed.state,
            Activity::Unknown,
            "the fresh placeholder is a fence, not a definite ended"
        );
        assert_eq!(observed.reason.as_deref(), Some("claimed"));
        assert_eq!(observed.exit, None);

        let mut successor = legacy_new(tmp.path(), "hetz.worker", "codex", Some("worker".into()))
            .with_ownership(token, seq);
        successor.observe(active()).unwrap();
        assert_eq!(read(&path, None).unwrap().state, Activity::Active);
    }

    /// W8-6: a foreign record's serde-default sequence of zero is below every claim — but a writer
    /// of the other version must not replace a record whose `agent` it would misread. Only the
    /// written claim supersedes.
    #[test]
    fn non_claiming_writers_refuse_foreign_schemas_outright() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let v2 = br#"{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"active","blockedOn":"none","inputBuffer":"unknown","incarnation":"future","sinceMs":1,"writtenAtMs":1,"transitions":1}"#;
        fs::write(&path, v2).unwrap();

        let token = session_token();
        let mut adopted = legacy_new(tmp.path(), "hetz.worker", "codex", Some("worker".into()))
            .with_ownership(token, 5);
        adopted.observe(active()).unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            v2.to_vec(),
            "adopted writer refused"
        );
        let mut token_only = writer(tmp.path());
        token_only.observe(active()).unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            v2.to_vec(),
            "token-only writer refused"
        );

        let mut claimed = takeover(tmp.path(), "codex");
        claimed.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.schema, SCHEMA_V2, "only the written claim supersedes");
    }

    /// W8-7: a beyond-skew future stamp (a backward clock correction's leftover) must not make
    /// an unchanged restatement a no-op forever — the next restatement repairs the stamp.
    #[test]
    fn a_beyond_skew_stamp_is_repaired_by_the_next_restatement() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut writer = writer(tmp.path());
        writer.observe(active()).unwrap();

        let mut poisoned: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        poisoned.written_at_ms = crate::message::now_ms()
            + duration_ms(HARNESS_STATE_FUTURE_SKEW)
            + duration_ms(HARNESS_STATE_REFRESH);
        write_record(&path, &poisoned).unwrap();
        assert_eq!(
            read(&path, None).unwrap().reason.as_deref(),
            Some("future-skew")
        );

        writer.observe(active()).unwrap();
        let repaired = read(&path, None).unwrap();
        assert_eq!(
            repaired.state,
            Activity::Active,
            "stamp repaired to the writer's clock"
        );
    }

    /// W8-8: the ask axis is validated at the write boundary.
    #[test]
    fn wrapperless_claims_are_atomic_and_never_supersede_a_live_wrapper() {
        let tmp = tempfile::tempdir().unwrap();
        let wl =
            |token: &str| legacy_claim_wrapperless(tmp.path(), "hetz.worker", "claude", token).unwrap();
        assert!(wl("claude-session-a").is_some(), "virgin dir");

        // A wrapper's FRESH claim placeholder is a session mid-startup, not an ended one: the
        // check-and-write is one act under the lock, so the racing hooks-only SessionStart
        // cannot steal the sequence between the wrapper's read and its write.
        let wrapper_token = session_token();
        let wrapper_seq = legacy_claim(tmp.path(), "hetz.worker", "claude", &wrapper_token).unwrap();
        assert!(
            wl("claude-session-b").is_none(),
            "fresh placeholder is owned"
        );

        // A live wrapper record stays off limits; a REAL terminal record is claimable.
        let mut wrapper = legacy_new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("worker".to_string()),
        )
        .with_ownership(wrapper_token.clone(), wrapper_seq);
        wrapper.observe(active()).unwrap();
        assert!(
            wl("claude-session-c").is_none(),
            "live wrapper record is off limits"
        );
        wrapper.ended("exit 0").unwrap();
        assert!(
            wl("claude-session-d").is_some(),
            "real terminal records may be claimed"
        );
    }

    /// An abandoned placeholder — a wrapper that claimed and then died before observing —
    /// ages past the staleness horizon and becomes claimable like any orphan.
    #[test]
    fn an_abandoned_wrapper_placeholder_is_claimable_once_stale() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        legacy_claim(tmp.path(), "hetz.worker", "claude", &session_token()).unwrap();
        assert!(
            legacy_claim_wrapperless(tmp.path(), "hetz.worker", "claude", "claude-session-x")
                .unwrap()
                .is_none()
        );

        let mut aged: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        aged.written_at_ms = crate::message::now_ms() - duration_ms(HARNESS_STATE_STALE) - 1;
        write_record(&path, &aged).unwrap();
        assert!(
            legacy_claim_wrapperless(tmp.path(), "hetz.worker", "claude", "claude-session-x")
                .unwrap()
                .is_some()
        );
    }

    /// n2: a saturated on-disk sequence would mint shared ownership; the claim fails loudly
    /// instead, and unreadable bytes are never a virgin seat for non-claiming writers.
    #[test]
    fn saturated_sequences_refuse_claims_and_unreadable_bytes_refuse_writers() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let saturated = format!(
            r#"{{"schema":"st2.harness-state.v1","agent":"hetz.worker","harness":"codex","state":"idle","blockedOn":"none","inputBuffer":"unknown","ptySession":"worker","incarnation":"other","seq":{},"sinceMs":1,"writtenAtMs":1,"transitions":1}}"#,
            u64::MAX
        );
        fs::write(&path, saturated).unwrap();
        assert!(legacy_claim(tmp.path(), "hetz.worker", "codex", "t").is_err());

        fs::write(&path, b"{not json").unwrap();
        let before = fs::read(&path).unwrap();
        let mut writer = writer(tmp.path());
        writer.observe(active()).unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "non-claiming writers refuse"
        );
        let mut adopted = legacy_new(tmp.path(), "hetz.worker", "codex", Some("worker".into()))
            .with_ownership(session_token(), 7);
        adopted.observe(active()).unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "adopted writers refuse too"
        );

        // The written claim supersedes even bytes it cannot parse; sequence and counter restart.
        let token = session_token();
        let seq = legacy_claim(tmp.path(), "hetz.worker", "codex", &token).unwrap();
        assert_eq!(seq, 1);
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.transitions, 0);
    }

    /// jUUo: the sequence floor survives a record this version cannot parse — a claim after
    /// damage continues past the damaged sequence instead of restarting below a lingering
    /// predecessor, who stays fenced out.
    #[test]
    fn the_sequence_floor_keeps_claims_monotonic_across_unreadable_records() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut predecessor = takeover(tmp.path(), "codex");
        predecessor.observe(active()).unwrap();
        let damaged_seq: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        fs::write(&path, b"{corrupted").unwrap();
        let token = session_token();
        let seq = legacy_claim(tmp.path(), "hetz.worker", "codex", &token).unwrap();
        assert!(
            seq > damaged_seq.seq,
            "the floor carries the sequence past the damage ({seq} vs {})",
            damaged_seq.seq
        );

        // The lingering predecessor is below the new claim and stays refused.
        predecessor.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.incarnation, token);
        assert_eq!(record.reason.as_deref(), Some("superseded"));
    }

    /// The virgin token-only path also establishes initial ownership (sequence one), so it
    /// persists the floor sidecar too: if that first record later goes unreadable, a
    /// replacement claim continues PAST the lingering writer instead of colliding with it.
    #[test]
    fn a_virgin_token_only_write_persists_the_sequence_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut predecessor = writer(tmp.path());
        predecessor.observe(active()).unwrap();
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.seq, 1);
        let floor = fs::read_to_string(tmp.path().join(SEQ_FLOOR_NAME)).unwrap();
        assert_eq!(
            floor.trim(),
            "1",
            "the virgin write persists the floor beside its sequence"
        );

        // The record goes unreadable; the claim must continue past sequence one, and the
        // lingering token-only predecessor stays fenced out.
        fs::write(&path, b"{corrupted").unwrap();
        let token = session_token();
        let seq = legacy_claim(tmp.path(), "hetz.worker", "codex", &token).unwrap();
        assert!(
            seq > 1,
            "the persisted floor carries the claim past sequence one ({seq})"
        );
        predecessor.observe(active()).unwrap();
        let after: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(after.incarnation, token);
    }

    /// jUUq: an unreadable (not merely absent) record refuses token-only writes and wrapperless
    /// claims — a permissions failure is never a virgin seat.
    #[test]
    fn io_failures_are_unreadable_not_absent() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        writer(tmp.path()).observe(active()).unwrap();
        let live = fs::read(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

        let mut token_only = writer(tmp.path());
        token_only
            .observe(Observation::new(
                Activity::Idle,
                BlockedOn::None,
                InputBuffer::Unknown,
            ))
            .unwrap();
        assert!(
            legacy_claim_wrapperless(tmp.path(), "hetz.worker", "claude", "claude-session-x")
                .unwrap()
                .is_none(),
            "wrapperless claims refuse unreadable records"
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            live,
            "nothing renamed over the live record"
        );
    }

    /// The reader clock the golden fixtures are written against. Fixed rather than `now_ms()`, so
    /// every fixture's freshness, deadline, and evidence age is a stated number a reviewer can
    /// check by hand.
    const FIXTURE_NOW_MS: u64 = 1_788_000_000_000;
    const FIXTURE_AGENT_ID: &str = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";

    fn fixture(raw: &str) -> Observed {
        read_raw_at(raw.as_bytes(), None, FIXTURE_NOW_MS)
    }

    fn healthy() -> crate::driver_diagnostic::Observed {
        crate::driver_diagnostic::Observed::Absent
    }

    /// One version 3 record with the axes under test substituted in, fresh against
    /// [`FIXTURE_NOW_MS`], so an edge case differs from a valid record by exactly the edge.
    fn v3_raw(condition: &str, ask: &str, conversation: Option<&str>) -> String {
        let conversation =
            conversation.map_or(String::new(), |wire| format!(r#","conversationRef":{wire}"#));
        format!(
            r#"{{"schema":"st2.harness-state.v3","agent":"{FIXTURE_AGENT_ID}","harness":"codex","state":"active","inputBuffer":"empty","condition":{condition},"ask":{ask}{conversation},"ptySession":"worker","incarnation":"session-1","seq":1,"sinceMs":1,"writtenAtMs":{},"transitions":1}}"#,
            FIXTURE_NOW_MS - 1_000
        )
    }

    /// The golden wire: one fixture per shape a consumer must handle, decoded through the real
    /// reader and folded through the real disposition function. This is the table that fails when
    /// a projection quietly changes meaning — a fixture is bytes, so nothing here can pass by
    /// agreeing with the code that produced it.
    #[test]
    fn golden_fixtures_cover_every_shape_a_consumer_must_handle() {
        // Version 1: legacy, bus identity, condition EXPLICITLY absent — never `clear`.
        let v1 = fixture(include_str!("../tests/fixtures/harness-state/v1-active.json"));
        assert_eq!(v1.schema.as_deref(), Some(SCHEMA_V1));
        assert_eq!(v1.state, Activity::Active);
        assert_eq!(v1.condition, ConditionView::Absent);
        assert_eq!(v1.human_ask, HumanAsk::None);
        assert_eq!(
            v1.subject,
            Some(RecordSubject::BusIdentity("hetz.worker".into()))
        );
        assert!(v1.indeterminacy.is_none());
        assert_eq!(
            disposition(Some(&v1), &healthy()),
            Disposition::new(
                DispositionState::Working,
                Attention::None,
                PrimaryAction::None
            )
        );

        // Version 2: same axes, ID namespace, and a legacy human block projecting into the tagged
        // axis without inventing a condition.
        let v2 = fixture(include_str!("../tests/fixtures/harness-state/v2-blocked.json"));
        assert_eq!(v2.schema.as_deref(), Some(SCHEMA_V2));
        assert_eq!(
            v2.condition,
            ConditionView::Absent,
            "versions 1 and 2 never infer a condition, and absence is never `clear`"
        );
        assert_eq!(v2.human_ask, HumanAsk::Pending(AskKind::Question));
        assert_eq!(
            (v2.blocked_on, v2.ask),
            (BlockedOn::Human, Ask::Question),
            "the shipped legacy pair keeps its exact meaning beside the tagged axis"
        );
        assert_eq!(
            v2.subject,
            Some(RecordSubject::AgentId(FIXTURE_AGENT_ID.into()))
        );
        assert_eq!(
            disposition(Some(&v2), &healthy()),
            Disposition::new(
                DispositionState::WaitingHuman,
                Attention::Now,
                PrimaryAction::Answer
            )
        );

        // Version 3, clear, with a complete conversation link.
        let clear = fixture(include_str!("../tests/fixtures/harness-state/v3-clear.json"));
        assert_eq!(clear.schema.as_deref(), Some(SCHEMA_V3));
        assert_eq!(clear.condition, ConditionView::Clear);
        assert_eq!(clear.human_ask, HumanAsk::None);
        let link = clear
            .conversation
            .as_ref()
            .and_then(ConversationRef::link)
            .expect("the fixture states a linked conversation");
        assert_eq!(link.history_mutability, HistoryMutability::Rewritable);
        assert_eq!(link.capability_evidence, CapabilityEvidence::Probed);
        assert_eq!(link.verified_through_ms, FIXTURE_NOW_MS - 1_000);
        assert_eq!(
            disposition(Some(&clear), &healthy()),
            Disposition::new(
                DispositionState::Working,
                Attention::None,
                PrimaryAction::None
            )
        );

        // A human-recovery fault: pages, and remediation is what the human does.
        let human = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-fault-human.json"
        ));
        let fault = human.condition.fault().expect("the fixture states a fault");
        assert_eq!(fault.category, Some(FaultCategory::Authentication));
        assert_eq!(fault.recovery, Recovery::Human);
        assert_eq!(fault.code.as_deref(), Some("codex/unauthorized"));
        assert!(!fault.overdue);
        assert_eq!(fault.observed_at_ms, FIXTURE_NOW_MS - 1_000);
        assert_eq!(
            human.conversation.as_ref().map(ConversationRef::kind),
            Some("unavailable")
        );
        assert_eq!(
            disposition(Some(&human), &healthy()),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );

        // An automatic recovery inside its own deadline: recovering, soon, nobody acts yet.
        let waiting = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-fault-automatic-pending.json"
        ));
        let fault = waiting.condition.fault().expect("fault");
        assert_eq!(fault.recovery, Recovery::Automatic);
        assert_eq!(fault.category, Some(FaultCategory::RateLimit));
        assert!(!fault.overdue);
        assert_eq!(
            disposition(Some(&waiting), &healthy()),
            Disposition::new(
                DispositionState::Recovering,
                Attention::Soon,
                PrimaryAction::None
            )
        );

        // The same fault past its own deadline: no longer evidence of anything automatic.
        let overdue = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-fault-automatic-overdue.json"
        ));
        let fault = overdue.condition.fault().expect("fault");
        assert!(fault.overdue);
        assert_eq!(
            fault.recovery,
            Recovery::Unknown,
            "an automatic recovery that missed its own deadline is untyped, and untyped pages"
        );
        assert_eq!(
            fault.next_observation_due_ms,
            Some(FIXTURE_NOW_MS - 60_000)
        );
        assert_eq!(
            disposition(Some(&overdue), &healthy()),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );

        // Fault and ask at once: remediation is primary, and the ask stays visible on both the
        // tagged and the legacy axis rather than being swallowed by the fault.
        let both = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-fault-and-ask.json"
        ));
        assert_eq!(
            both.condition.fault().map(|fault| fault.category),
            Some(Some(FaultCategory::Provider))
        );
        assert_eq!(both.human_ask, HumanAsk::Pending(AskKind::Permission));
        assert_eq!((both.blocked_on, both.ask), (BlockedOn::Human, Ask::Permission));
        assert_eq!(
            disposition(Some(&both), &healthy()),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );

        // A malformed version 3 record: a `clear` carrying fault evidence is not a weaker
        // observation, and its declared version survives for the drain gate.
        let malformed = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-contradictory-clear.json"
        ));
        assert_eq!(malformed.state, Activity::Unknown);
        let indeterminacy = malformed
            .indeterminacy
            .as_ref()
            .expect("an indeterminate observation states why, typed");
        assert_eq!(indeterminacy.reason, "contradictory-clear");
        assert_eq!(indeterminacy.evidence_age_ms, Some(1_000));
        assert_eq!(
            malformed.reason.as_deref(),
            Some("contradictory-clear"),
            "the legacy scalar is a projection of the typed reason, never a second derivation"
        );
        assert_eq!(malformed.schema.as_deref(), Some(SCHEMA_V3));
        assert_eq!(malformed.condition, ConditionView::Absent);
        assert_eq!(
            disposition(Some(&malformed), &healthy()),
            Disposition::new(
                DispositionState::Unknown,
                Attention::None,
                PrimaryAction::Observe
            )
        );

        // An unknown future version: uninterpretable, non-paging, and still accounted for.
        let future = fixture(include_str!(
            "../tests/fixtures/harness-state/unknown-future-schema.json"
        ));
        assert_eq!(future.state, Activity::Unknown);
        assert_eq!(
            future.indeterminacy.as_ref().map(|why| why.reason.as_str()),
            Some("unsupported-schema")
        );
        assert_eq!(future.schema.as_deref(), Some("st2.harness-state.v4"));
        assert!(future.subject.is_none());
        assert_eq!(
            disposition(Some(&future), &healthy()),
            Disposition::new(
                DispositionState::Unknown,
                Attention::None,
                PrimaryAction::Observe
            )
        );
    }

    /// Strict edge validation, one case per rejection. Each pair differs from a valid record by
    /// exactly the contradiction under test, and each carries its OWN reason word: an operator
    /// must be able to tell a producer bug from a stale seat, and one bug from another.
    #[test]
    fn strict_version_three_edges_are_rejected_with_distinct_reasons() {
        let none = r#"{"kind":"none"}"#;
        for (condition, ask, expected) in [
            // A clear that carries fault evidence claims two contradictory things at once.
            (
                r#"{"kind":"clear","recovery":"human"}"#,
                none,
                "contradictory-clear",
            ),
            (
                r#"{"kind":"clear","observedAtMs":1}"#,
                none,
                "contradictory-clear",
            ),
            // A fault that cannot be routed or aged is not a fault.
            (
                r#"{"kind":"fault","category":"quota","observedAtMs":1}"#,
                none,
                "incomplete-fault",
            ),
            (
                r#"{"kind":"fault","category":"quota","recovery":"human"}"#,
                none,
                "incomplete-fault",
            ),
            // A deadline belongs only to a recovery that is supposed to happen by itself.
            (
                r#"{"kind":"fault","category":"quota","recovery":"human","observedAtMs":1,"nextObservationDueMs":2}"#,
                none,
                "misplaced-observation-due",
            ),
            (
                r#"{"kind":"fault","category":"quota","recovery":"automatic","observedAtMs":9,"nextObservationDueMs":2}"#,
                none,
                "inverted-observation-due",
            ),
            // `absent` is the legacy projection, not a state a producer may claim.
            (r#"{"kind":"absent"}"#, none, "written-absent-condition"),
            (r#"{"kind":"degraded"}"#, none, "unsupported-condition-kind"),
            // An ask that says "nothing is pending" while naming a pending kind.
            (
                r#"{"kind":"clear"}"#,
                r#"{"kind":"none","ask":"question"}"#,
                "contradictory-ask",
            ),
            (
                r#"{"kind":"clear"}"#,
                r#"{"kind":"waiting"}"#,
                "unsupported-ask-kind",
            ),
        ] {
            let observed = fixture(&v3_raw(condition, ask, None));
            assert_eq!(observed.state, Activity::Unknown, "{condition} {ask}");
            assert_eq!(
                observed.indeterminacy.as_ref().map(|why| why.reason.as_str()),
                Some(expected),
                "{condition} {ask}"
            );
            assert_eq!(
                observed.condition,
                ConditionView::Absent,
                "a rejected record proves no condition either"
            );
        }

        // What is tolerated, and why: an unknown CATEGORY leaves the fault untyped but still
        // routed by its recovery — failing the record would make a real fault stop paging because
        // its label was new — and a `pending` ask whose kind is unstated keeps the waiting human.
        let untyped = fixture(&v3_raw(
            r#"{"kind":"fault","category":"gravity","recovery":"terminal","observedAtMs":1}"#,
            none,
            None,
        ));
        let fault = untyped.condition.fault().expect("still a fault");
        assert_eq!(fault.category, None, "an unknown category is untyped");
        assert_eq!(fault.recovery, Recovery::Terminal);
        assert_eq!(
            disposition(Some(&untyped), &healthy()),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );

        let unnamed = fixture(&v3_raw(
            r#"{"kind":"clear"}"#,
            r#"{"kind":"pending"}"#,
            None,
        ));
        assert_eq!(unnamed.human_ask, HumanAsk::Pending(AskKind::Unknown));
        assert_eq!(
            disposition(Some(&unnamed), &healthy()).primary_action,
            PrimaryAction::Answer,
            "an ask of an unstated kind is still an ask"
        );

        // An unrecognized RECOVERY word is unknown, which pages — the conservative direction.
        let unrecognized = fixture(&v3_raw(
            r#"{"kind":"fault","category":"provider","recovery":"eventually","observedAtMs":1}"#,
            none,
            None,
        ));
        assert_eq!(
            unrecognized.condition.fault().map(|fault| fault.recovery),
            Some(Recovery::Unknown)
        );
        assert_eq!(
            disposition(Some(&unrecognized), &healthy()).attention,
            Attention::Now
        );

        // A deadline beside an unrecognized recovery word is NOT rejected: the record may be an
        // automatic class this build has never heard of, and rejecting it would turn a fault that
        // pages under `unknown` recovery into a non-paging indeterminate row. An inverted
        // deadline stays incoherent whatever the recovery word says.
        let deadline_on_unknown = fixture(&v3_raw(
            r#"{"kind":"fault","category":"provider","recovery":"eventually","observedAtMs":1,"nextObservationDueMs":2}"#,
            none,
            None,
        ));
        let fault = deadline_on_unknown
            .condition
            .fault()
            .expect("an unknown recovery word keeps the fault");
        assert_eq!(fault.recovery, Recovery::Unknown);
        assert_eq!(fault.next_observation_due_ms, Some(2));
        assert!(
            !fault.overdue,
            "only an automatic recovery can be overdue; unknown already pages"
        );
        assert_eq!(
            disposition(Some(&deadline_on_unknown), &healthy()),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );
        let inverted_on_unknown = fixture(&v3_raw(
            r#"{"kind":"fault","category":"provider","recovery":"eventually","observedAtMs":9,"nextObservationDueMs":2}"#,
            none,
            None,
        ));
        assert_eq!(
            inverted_on_unknown
                .indeterminacy
                .as_ref()
                .map(|why| why.reason.as_str()),
            Some("inverted-observation-due")
        );
    }

    /// The conversation bridge is identity and capability only, and a half-stated link is exactly
    /// what a consumer would trust and should not. Silence is not a capability claim either:
    /// stating nothing is `None`, which is not `unsupported`.
    #[test]
    fn the_conversation_bridge_requires_a_complete_finite_link() {
        let clear = r#"{"kind":"clear"}"#;
        let none = r#"{"kind":"none"}"#;
        let complete = format!(
            r#"{{"kind":"linked","driver":"codex","conversation":"thread_01JXPLACEHOLDER","incarnation":"session-1","historyMutability":"stable","capabilityEvidence":"declared","verifiedThroughMs":{}}}"#,
            FIXTURE_NOW_MS - 1_000
        );
        let observed = fixture(&v3_raw(clear, none, Some(&complete)));
        let link = observed
            .conversation
            .as_ref()
            .and_then(ConversationRef::link)
            .expect("complete");
        assert_eq!(link.driver, "codex");
        assert_eq!(link.conversation, "thread_01JXPLACEHOLDER");
        assert_eq!(link.history_mutability, HistoryMutability::Stable);
        assert_eq!(link.capability_evidence, CapabilityEvidence::Declared);

        for (wire, expected) in [
            // Each of the five required parts, removed one at a time.
            (
                r#"{"kind":"linked","conversation":"c","incarnation":"i","historyMutability":"stable","capabilityEvidence":"probed","verifiedThroughMs":1}"#,
                "incomplete-conversation-link",
            ),
            (
                r#"{"kind":"linked","driver":"codex","incarnation":"i","historyMutability":"stable","capabilityEvidence":"probed","verifiedThroughMs":1}"#,
                "incomplete-conversation-link",
            ),
            (
                r#"{"kind":"linked","driver":"codex","conversation":"c","historyMutability":"stable","capabilityEvidence":"probed","verifiedThroughMs":1}"#,
                "incomplete-conversation-link",
            ),
            (
                r#"{"kind":"linked","driver":"codex","conversation":"c","incarnation":"i","capabilityEvidence":"probed","verifiedThroughMs":1}"#,
                "incomplete-conversation-link",
            ),
            (
                r#"{"kind":"linked","driver":"codex","conversation":"c","incarnation":"i","historyMutability":"stable","verifiedThroughMs":1}"#,
                "incomplete-conversation-link",
            ),
            // An absent or zero verification bound is an unverifiable claim that looks verified.
            (
                r#"{"kind":"linked","driver":"codex","conversation":"c","incarnation":"i","historyMutability":"stable","capabilityEvidence":"probed"}"#,
                "unbounded-conversation-verification",
            ),
            (
                r#"{"kind":"linked","driver":"codex","conversation":"c","incarnation":"i","historyMutability":"stable","capabilityEvidence":"probed","verifiedThroughMs":0}"#,
                "unbounded-conversation-verification",
            ),
            (r#"{"kind":"maybe"}"#, "unsupported-conversation-kind"),
        ] {
            let observed = fixture(&v3_raw(clear, none, Some(wire)));
            // Only the capability axis degrades. The observation itself — activity, condition,
            // ask — is intact, because a badly stated side-channel is not evidence about the
            // harness, and discarding the record would trade a real fault for a broken link.
            assert_eq!(
                observed.conversation,
                Some(ConversationRef::Unavailable(Some(expected.to_string()))),
                "{wire}"
            );
            assert_eq!(observed.state, Activity::Active, "{wire}");
            assert_eq!(observed.condition, ConditionView::Clear, "{wire}");
            assert_eq!(observed.human_ask, HumanAsk::None, "{wire}");
            assert!(
                observed.indeterminacy.is_none(),
                "a rejected conversation reference never makes the observation indeterminate: \
                 {wire}"
            );
        }

        // The two negative capability answers are distinct from each other and from silence.
        assert_eq!(
            fixture(&v3_raw(clear, none, Some(r#"{"kind":"unsupported"}"#))).conversation,
            Some(ConversationRef::Unsupported)
        );
        assert_eq!(
            fixture(&v3_raw(
                clear,
                none,
                Some(r#"{"kind":"unavailable","reason":"no bound thread"}"#)
            ))
            .conversation,
            Some(ConversationRef::Unavailable(Some(
                "no bound thread".to_string()
            )))
        );
        assert_eq!(
            fixture(&v3_raw(clear, none, None)).conversation,
            None,
            "a record that states nothing about a conversation claims no capability"
        );
    }

    /// The two clocks are independent. The heartbeat exists to keep TRANSPORT freshness alive; it
    /// cannot move a fault's semantic observation time or its deadline, and whether a deadline has
    /// passed is decided at READ time against the reader's clock. Without this separation a seat
    /// could heartbeat its way out of an overdue recovery forever.
    #[test]
    fn the_heartbeat_moves_transport_freshness_only_and_never_the_semantic_fault_clock() {
        let observed_at = FIXTURE_NOW_MS - 10_000;
        let due = FIXTURE_NOW_MS + 1_000;
        let raw = v3_raw(
            &format!(
                r#"{{"kind":"fault","category":"rateLimit","recovery":"automatic","observedAtMs":{observed_at},"nextObservationDueMs":{due}}}"#
            ),
            r#"{"kind":"none"}"#,
            None,
        );

        let before = read_raw_at(raw.as_bytes(), None, FIXTURE_NOW_MS);
        let after = read_raw_at(raw.as_bytes(), None, due + 1);
        for observation in [&before, &after] {
            let fault = observation.condition.fault().expect("fault");
            assert_eq!(
                (fault.observed_at_ms, fault.next_observation_due_ms),
                (observed_at, Some(due)),
                "the semantic clock belongs to the record, not the reader"
            );
        }
        assert!(!before.condition.fault().unwrap().overdue);
        assert!(after.condition.fault().unwrap().overdue);
        assert_eq!(
            disposition(Some(&before), &healthy()).attention,
            Attention::Soon
        );
        assert_eq!(
            disposition(Some(&after), &healthy()).attention,
            Attention::Now,
            "the same bytes page once their own deadline has passed"
        );
    }

    /// A native-driver diagnostic failure contributes to the shared disposition — it is a fault
    /// the harness itself could not report — while changing no raw axis. And the two non-paging
    /// rules hold against it: a finished seat is not an emergency, and neither is a record nobody
    /// can interpret.
    #[test]
    fn a_driver_diagnostic_failure_contributes_without_touching_raw_axes_or_paging_the_dead() {
        let failing = crate::driver_diagnostic::Observed::Failure(crate::driver_diagnostic::Failure {
            driver: crate::driver_diagnostic::Driver::Codex,
            stage: crate::driver_diagnostic::Stage::ProviderAuth,
            reason: crate::driver_diagnostic::Reason::ProviderAuthRejected,
            source: crate::driver_diagnostic::Source::TurnResult,
            producer_version: None,
            support: crate::driver_diagnostic::Support::Supported,
            observed_at: FIXTURE_NOW_MS - 5_000,
            evidence_age_ms: 5_000,
        });

        let clear = fixture(include_str!("../tests/fixtures/harness-state/v3-clear.json"));
        assert_eq!(
            disposition(Some(&clear), &failing),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );
        assert_eq!(
            (clear.condition.clone(), clear.human_ask, clear.state),
            (ConditionView::Clear, HumanAsk::None, Activity::Active),
            "normalization folds the diagnostic in without rewriting what the record said"
        );

        // The harness's own fault outranks the diagnostic: it is the semantic axis, and the
        // diagnostic is st2's view of its own plumbing.
        let recovering = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-fault-automatic-pending.json"
        ));
        assert_eq!(
            disposition(Some(&recovering), &failing),
            Disposition::new(
                DispositionState::Recovering,
                Attention::Soon,
                PrimaryAction::None
            )
        );

        let ended = fixture(&v3_raw_ended());
        assert_eq!(
            disposition(Some(&ended), &failing),
            Disposition::new(
                DispositionState::Ended,
                Attention::None,
                PrimaryAction::None
            ),
            "a finished seat never pages"
        );

        let malformed = fixture(include_str!(
            "../tests/fixtures/harness-state/v3-contradictory-clear.json"
        ));
        assert_eq!(
            disposition(Some(&malformed), &failing),
            Disposition::new(
                DispositionState::Unknown,
                Attention::None,
                PrimaryAction::Observe
            ),
            "a record nobody can interpret never pages"
        );

        // Never observed is not a state: it is worth looking at and nothing else.
        assert_eq!(
            disposition(None, &healthy()),
            Disposition::new(
                DispositionState::Unknown,
                Attention::None,
                PrimaryAction::Observe
            )
        );

        // ...but a published rejection beside no record at all is the credential case: Claude,
        // Codex, and omp publish a diagnostic ONLY on a rejection, so this is the whole story.
        assert_eq!(
            disposition(None, &failing),
            Disposition::new(
                DispositionState::Failed,
                Attention::Now,
                PrimaryAction::Remediate
            )
        );
    }

    /// A terminal version 3 record, exit-bearing so it is not the claim placeholder.
    fn v3_raw_ended() -> String {
        format!(
            r#"{{"schema":"st2.harness-state.v3","agent":"{FIXTURE_AGENT_ID}","harness":"codex","state":"ended","inputBuffer":"unknown","condition":{{"kind":"clear"}},"ask":{{"kind":"none"}},"exit":"exit 0","incarnation":"session-1","seq":1,"sinceMs":1,"writtenAtMs":{},"transitions":2}}"#,
            FIXTURE_NOW_MS - 1_000
        )
    }

    /// The one-way fence, now driven from the side activation left behind: a build whose writer
    /// emits version 2 READS a version 3 record and refuses to touch it, by every path. This is
    /// the rollout hazard in a mixed fleet — a not-yet-replaced peer, or a rolled-back binary,
    /// running beside an activated one — and the defect it fences is subtle: a v3 record does not
    /// decode as the legacy record shape at all, so without the version-independent envelope the
    /// claim path would see "no record on disk" and rename a version 2 claim over a live migrated
    /// record, destroying its fault axis with no trace that it happened.
    ///
    /// The version 2 writer is reached through the emitted-schema seam because this build's own
    /// production writer is on version 3, where this direction is unreachable by construction.
    #[test]
    fn a_version_two_writer_never_takes_over_a_version_three_record() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let planted = format!(
            r#"{{"schema":"st2.harness-state.v3","agent":"{FIXTURE_AGENT_ID}","harness":"codex","state":"active","inputBuffer":"empty","condition":{{"kind":"clear"}},"ask":{{"kind":"none"}},"ptySession":"worker","incarnation":"","seq":9,"sinceMs":1,"writtenAtMs":{},"transitions":4}}"#,
            crate::message::now_ms()
        );
        fs::write(&path, &planted).unwrap();

        // Reading works from either side — that is what makes a staged rollout safe at all.
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.condition, ConditionView::Clear);
        assert_eq!(observed.schema.as_deref(), Some(SCHEMA_V3));

        // Writing does not: an ordinary observation refuses, the cautious wrapperless claim is
        // ineligible, and the explicit claim errors instead of downgrading the record's meaning.
        let mut token_only = writer(tmp.path());
        assert!(!token_only.observe_unless_ended(active()).unwrap());
        assert!(
            claim_wrapperless_emitting(
                tmp.path(),
                "hetz.worker",
                "codex",
                "claude-session-x",
                SCHEMA_V2
            )
            .unwrap()
            .is_none()
        );
        let error = claim_emitting(
            tmp.path(),
            "hetz.worker",
            "codex",
            &session_token(),
            SCHEMA_V2,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(SCHEMA_V3), "{error}");
        assert_eq!(
            fs::read(&path).unwrap(),
            planted.as_bytes(),
            "the version 3 record is left byte-identical by every path"
        );

        // And the other direction is open, which is what makes the fence one-way rather than a
        // deadlock: this build's own production claim supersedes it.
        assert!(claim(tmp.path(), FIXTURE_AGENT_ID, "codex", &session_token()).is_ok());
    }

    /// The other half of the envelope's job: bytes this build cannot decode as a record still
    /// carry their OWNERSHIP forward when their version is one a claim may supersede. Without
    /// this, a claim over a torn record restarts at sequence one and a lingering predecessor
    /// holding a higher sequence fences the new session out permanently.
    #[test]
    fn an_undecodable_records_envelope_still_carries_its_ownership_forward() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        // A truncated version 1 record: a real record with a decodable envelope and a body this
        // build cannot read. No floor sidecar exists, so the envelope is the only evidence.
        fs::write(
            &path,
            br#"{"schema":"st2.harness-state.v1","seq":9,"transitions":4,"writtenAtMs":1}"#,
        )
        .unwrap();

        let seq = legacy_claim(tmp.path(), "hetz.worker", "codex", &session_token()).unwrap();
        assert_eq!(
            seq, 10,
            "the ownership sequence continues past bytes this build cannot read"
        );
        let record: Record = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            record.transitions, 5,
            "and so does the counter that keeps writes byte-distinct"
        );
    }

    // -----------------------------------------------------------------------
    // The version 3 producer surface — this build's PRODUCTION surface since
    // activation. Every one of these runs the REAL locked writer path: same
    // lock, same fences, same atomic rename. `v3_writer` names the version
    // explicitly rather than relying on `SCHEMA`, so these keep proving the
    // version 3 semantics even from a build that has been rolled back (proved
    // current by `the_writer_declares_version_three_and_still_means_an_immutable_id`).
    // -----------------------------------------------------------------------

    fn v3_writer(dir: &Path) -> Writer {
        Writer::new(dir, FIXTURE_AGENT_ID, "codex", Some("worker".to_string()))
            .with_emitted_schema(SCHEMA_V3)
    }

    fn record_json(path: &Path) -> serde_json::Value {
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    const QUOTA_CODE: &str = "codex/usage_limit_reached";

    fn quota_fault(observed_at_ms: u64) -> FaultReport {
        FaultReport::new(FaultCategory::Quota, Recovery::Automatic, observed_at_ms)
            .with_code(QUOTA_CODE)
            .with_observation_due(observed_at_ms + 600_000)
    }

    fn quota_key() -> FaultKey {
        FaultKey::new(FaultCategory::Quota).with_code(QUOTA_CODE)
    }

    fn v3_active(condition: ConditionReport) -> Frame {
        Frame::new(
            Activity::Active,
            InputBuffer::Empty,
            condition,
            HumanAsk::None,
        )
    }

    /// The COMPATIBILITY guarantee, kept after activation: a writer emitting version 2 makes the
    /// producer API a projection and nothing more. The same tuple stated through the legacy API
    /// and through the version 3 API produces the same bytes — the projection coalesces against
    /// the legacy record rather than writing — the condition and conversation axes never reach
    /// the version 2 wire, and the condition operations refuse as a VALUE rather than pretending
    /// to have stored an axis the wire cannot carry. This is what a rollback gets, and what every
    /// version 2 record still on disk was written by.
    #[test]
    fn the_version_two_projection_is_byte_identical_and_carries_no_condition_axis() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let token = session_token();
        let mut legacy = writer(tmp.path()).with_session(token.clone());
        legacy
            .observe(Observation::new(
                Activity::Active,
                BlockedOn::None,
                InputBuffer::Empty,
            ))
            .unwrap();
        let before = fs::read(&path).unwrap();

        let mut producer = writer(tmp.path()).with_session(token);
        assert!(!producer.writes_condition_axis());
        assert_eq!(
            producer
                .publish(v3_active(ConditionReport::Fault(quota_fault(1_000))))
                .unwrap(),
            WriteOutcome::Coalesced,
            "the same tuple plus a fault is the same version 2 bytes"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "so the projection writes nothing"
        );
        let record = record_json(&path);
        assert_eq!(record["schema"], SCHEMA_V2);
        assert_eq!(record["blockedOn"], "none");
        assert_eq!(record["ask"], "none");
        assert!(
            record.get("condition").is_none() && record.get("conversationRef").is_none(),
            "version 2 has no condition or conversation axis and gains none"
        );
        assert_eq!(
            producer.raise_fault(quota_fault(1_000)).unwrap(),
            WriteOutcome::Refused(Refusal::ConditionUnrepresentable { schema: SCHEMA_V2 }),
            "a condition operation refuses visibly rather than caching the axis elsewhere"
        );
        assert_eq!(
            producer.clear_fault(quota_key()).unwrap(),
            WriteOutcome::Refused(Refusal::ConditionUnrepresentable { schema: SCHEMA_V2 })
        );
        assert_eq!(
            producer.clear_all(ProgressProof::TurnCompleted).unwrap(),
            WriteOutcome::Refused(Refusal::ConditionUnrepresentable { schema: SCHEMA_V2 })
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    /// What the one selector replacement buys: the same producer calls serialize strict version 3
    /// bytes that this build's own reader accepts, with the conversation link's incarnation
    /// stamped by the WRITER rather than by the producer.
    #[test]
    fn the_version_three_writer_serializes_every_stated_axis_and_reads_back() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let mut writer = v3_writer(tmp.path());
        let fault = quota_fault(now_ms - 5_000);
        assert!(
            writer
                .publish(
                    v3_active(ConditionReport::Fault(fault.clone()))
                        .with_conversation(ConversationState::Linked(ConversationClaim {
                            driver: "codex".to_string(),
                            conversation: "thread_01JXPLACEHOLDER".to_string(),
                            history_mutability: HistoryMutability::Rewritable,
                            capability_evidence: CapabilityEvidence::Probed,
                            verified_through_ms: now_ms,
                        }))
                        .with_reason("usage limit reached")
                )
                .unwrap()
                .landed()
        );

        let record = record_json(&path);
        assert_eq!(record["schema"], SCHEMA_V3);
        assert_eq!(record["agent"], FIXTURE_AGENT_ID);
        assert_eq!(record["condition"]["kind"], "fault");
        assert_eq!(record["condition"]["category"], "quota");
        assert_eq!(record["condition"]["code"], QUOTA_CODE);
        assert_eq!(record["condition"]["recovery"], "automatic");
        assert_eq!(record["condition"]["observedAtMs"], fault.observed_at_ms);
        assert_eq!(
            record["condition"]["nextObservationDueMs"],
            fault.next_observation_due_ms.unwrap()
        );
        assert_eq!(record["ask"]["kind"], "none");
        assert!(record["ask"].get("ask").is_none());
        assert_eq!(
            record["conversationRef"]["incarnation"], record["incarnation"],
            "the link belongs to the writing session, which stamps it"
        );
        assert!(
            record.get("blockedOn").is_none(),
            "version 3 removes the overloaded axis rather than carrying both"
        );

        let observed = read(&path, None).expect("a fresh version 3 record");
        assert_eq!(observed.schema.as_deref(), Some(SCHEMA_V3));
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(observed.human_ask, HumanAsk::None);
        let decoded = observed.condition.fault().expect("a fault");
        assert_eq!(decoded.category, Some(FaultCategory::Quota));
        assert_eq!(decoded.recovery, Recovery::Automatic);
        assert!(!decoded.overdue);
        assert_eq!(
            observed
                .conversation
                .as_ref()
                .and_then(ConversationRef::link)
                .map(|link| link.driver.as_str()),
            Some("codex")
        );
    }

    /// A paired clear is exactly `(category, code)`. Neither the category alone nor a sibling code
    /// in the same category clears the fault that stands, and a refused clear RETAINS it: the
    /// mismatch is reported with what actually stands so the caller cannot believe it cleared
    /// something.
    #[test]
    fn a_paired_clear_clears_only_the_exact_category_and_code_it_names() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let mut writer = v3_writer(tmp.path());
        writer.publish(v3_active(ConditionReport::Clear)).unwrap();
        assert!(writer.raise_fault(quota_fault(now_ms)).unwrap().landed());

        assert_eq!(
            writer
                .clear_fault(FaultKey::new(FaultCategory::Quota))
                .unwrap(),
            WriteOutcome::Refused(Refusal::ConditionMismatch {
                current: Some(quota_key())
            }),
            "a codeless key names a codeless fault, not this one"
        );
        assert_eq!(
            writer
                .clear_fault(FaultKey::new(FaultCategory::Quota).with_code("codex/other_limit"))
                .unwrap(),
            WriteOutcome::Refused(Refusal::ConditionMismatch {
                current: Some(quota_key())
            }),
            "and neither does a sibling code in the same category"
        );
        assert_eq!(
            writer
                .clear_fault(FaultKey::new(FaultCategory::RateLimit).with_code(QUOTA_CODE))
                .unwrap(),
            WriteOutcome::Refused(Refusal::ConditionMismatch {
                current: Some(quota_key())
            })
        );
        let retained = record_json(&path);
        assert_eq!(retained["condition"]["kind"], "fault");
        assert_eq!(retained["condition"]["code"], QUOTA_CODE);

        assert!(
            writer.clear_fault(quota_key()).unwrap().landed(),
            "the exact pair clears it"
        );
        let cleared = record_json(&path);
        assert_eq!(cleared["condition"]["kind"], "clear");
        assert!(cleared["condition"].get("category").is_none());
        assert!(cleared["condition"].get("observedAtMs").is_none());
        assert_eq!(
            read(&path, None).expect("a record").condition,
            ConditionView::Clear
        );
        assert_eq!(
            cleared["state"], "active",
            "a condition operation restates no activity it did not observe"
        );
        assert_eq!(
            writer.clear_fault(quota_key()).unwrap(),
            WriteOutcome::Refused(Refusal::ConditionMismatch { current: None }),
            "and clearing a clear axis matches nothing"
        );
    }

    /// The unkeyed clear: it takes no key, so it takes a positive proof instead, and the proof
    /// rides the record as the clear's diagnostic detail.
    #[test]
    fn clear_all_states_its_proof_and_clears_whatever_stands() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let mut writer = v3_writer(tmp.path());
        writer.publish(v3_active(ConditionReport::Clear)).unwrap();
        writer
            .raise_fault(
                FaultReport::new(FaultCategory::Authentication, Recovery::Human, now_ms)
                    .with_code("codex/oauth_expired"),
            )
            .unwrap();
        assert!(
            writer
                .clear_all(ProgressProof::RequestSucceeded)
                .unwrap()
                .landed()
        );
        let record = record_json(&path);
        assert_eq!(record["condition"]["kind"], "clear");
        assert_eq!(
            record["condition"]["detail"],
            ProgressProof::RequestSucceeded.as_str()
        );
        assert_eq!(
            read(&path, None).expect("a record").condition,
            ConditionView::Clear
        );
    }

    /// The semantic clock belongs to the fault, not to the writes that carry it: a heartbeat moves
    /// the transport stamp and NOTHING else, and restating one fault — which an SSE producer does
    /// on every activity edge — keeps the instant it was first observed. Without both, an
    /// automatic recovery could outrun its own deadline forever.
    #[test]
    fn neither_a_heartbeat_nor_a_restatement_moves_the_semantic_fault_clock() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let due_ms = now_ms + 600_000;
        let mut writer = v3_writer(tmp.path());
        writer
            .publish(v3_active(ConditionReport::Fault(quota_fault(now_ms))))
            .unwrap();
        let published = record_json(&path);

        writer.heartbeat().unwrap();
        let beaten = record_json(&path);
        assert!(
            beaten["writtenAtMs"].as_u64() > published["writtenAtMs"].as_u64(),
            "the transport stamp advances and every landed write stays byte-distinct"
        );
        assert_eq!(
            beaten["condition"], published["condition"],
            "the semantic clock and its deadline do not"
        );
        assert_eq!(beaten["sinceMs"], published["sinceMs"]);
        assert_eq!(beaten["transitions"], published["transitions"]);
        assert_eq!(beaten["ask"], published["ask"]);

        let restated = FaultReport::new(FaultCategory::Quota, Recovery::Automatic, now_ms + 30_000)
            .with_code(QUOTA_CODE)
            .with_observation_due(due_ms);
        assert!(
            writer
                .publish(Frame::new(
                    Activity::Idle,
                    InputBuffer::Empty,
                    ConditionReport::Fault(restated),
                    HumanAsk::None,
                ))
                .unwrap()
                .landed(),
            "the activity edge itself is a real transition"
        );
        let edged = record_json(&path);
        assert_eq!(edged["state"], "idle");
        assert_eq!(
            edged["condition"]["observedAtMs"].as_u64(),
            Some(now_ms),
            "restating one fault never re-mints its clock"
        );

        assert!(
            writer
                .publish(v3_active(ConditionReport::Unchanged))
                .unwrap()
                .landed()
        );
        let unchanged = record_json(&path);
        assert_eq!(unchanged["state"], "active");
        assert_eq!(
            unchanged["condition"], edged["condition"],
            "an activity-only frame carries the condition across untouched"
        );
    }

    /// Producer-side strictness mirrors the reader's, at the write boundary and BEFORE the record
    /// exists: a producer must not be able to write bytes this build's own reader rejects, and an
    /// impossible combination is a producer bug — an error — rather than a refusal to route
    /// around.
    #[test]
    fn impossible_producer_combinations_are_refused_before_anything_is_written() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let mut writer = v3_writer(tmp.path());
        let linked = |verified_through_ms| {
            ConversationState::Linked(ConversationClaim {
                driver: "codex".to_string(),
                conversation: "thread_01JXPLACEHOLDER".to_string(),
                history_mutability: HistoryMutability::Stable,
                capability_evidence: CapabilityEvidence::Declared,
                verified_through_ms,
            })
        };
        let invalid = [
            // `unknown` activity is derived from missing evidence; there is no writing path to it.
            Frame {
                state: Activity::Unknown,
                ..v3_active(ConditionReport::Clear)
            },
            // An exitless `ended` is the claim fence, which only a session claim writes.
            Frame::new(
                Activity::Ended,
                InputBuffer::Unknown,
                ConditionReport::Clear,
                HumanAsk::None,
            ),
            // A fault with no semantic observation time cannot be aged.
            v3_active(ConditionReport::Fault(FaultReport::new(
                FaultCategory::Provider,
                Recovery::Automatic,
                0,
            ))),
            // A bare code has no namespace, which is how two providers' codes collide.
            v3_active(ConditionReport::Fault(
                FaultReport::new(FaultCategory::Provider, Recovery::Automatic, now_ms)
                    .with_code("usage_limit_reached"),
            )),
            // A deadline on a recovery nobody automates is a producer error, not a hint.
            v3_active(ConditionReport::Fault(
                FaultReport::new(FaultCategory::Authentication, Recovery::Human, now_ms)
                    .with_observation_due(now_ms + 1_000),
            )),
            // An inverted deadline claims the next observation was due before the observation.
            v3_active(ConditionReport::Fault(
                FaultReport::new(FaultCategory::RateLimit, Recovery::Automatic, now_ms)
                    .with_observation_due(now_ms - 1),
            )),
            // A zero verification bound looks verified and is not.
            v3_active(ConditionReport::Clear).with_conversation(linked(0)),
            // st2's own rejection word, stated as if it were the driver's explanation.
            v3_active(ConditionReport::Clear).with_conversation(ConversationState::Unavailable(
                Some("incomplete-conversation-link".to_string()),
            )),
        ];
        for frame in invalid {
            assert!(
                writer.publish(frame.clone()).is_err(),
                "an invalid frame must not be publishable: {frame:?}"
            );
        }
        assert!(
            !path.exists(),
            "and no invalid frame reached the record at all"
        );
        // The same strictness on the condition-only path, which validates before taking the lock.
        assert!(
            writer
                .raise_fault(FaultReport::new(
                    FaultCategory::Quota,
                    Recovery::Automatic,
                    0
                ))
                .is_err()
        );
    }

    /// The producer path is the SAME fenced path: ownership direction, foreign schemas, and
    /// unreadable bytes decide exactly as they always have — except that the outcome is a typed
    /// value, so a producer whose writes are all being dropped can see why. Nothing here is a
    /// silent `Ok(false)`.
    #[test]
    fn producer_writes_honor_the_existing_fences_and_report_refusals_as_values() {
        // A foreign schema on disk: refused, named, and left untouched.
        let foreign = tempfile::tempdir().unwrap();
        let path = planted(foreign.path(), SCHEMA_V2, FIXTURE_AGENT_ID, 3);
        let planted_bytes = fs::read(&path).unwrap();
        let mut mismatched = v3_writer(foreign.path()).with_ownership(session_token(), 9);
        assert_eq!(
            mismatched.publish(v3_active(ConditionReport::Clear)).unwrap(),
            WriteOutcome::Refused(Refusal::Schema {
                found: SCHEMA_V2.to_string()
            })
        );
        assert_eq!(fs::read(&path).unwrap(), planted_bytes);

        // A later session's claim fences the straggler out, live or terminal.
        let live = tempfile::tempdir().unwrap();
        let mut predecessor = v3_writer(live.path()).with_ownership(session_token(), 1);
        assert!(
            predecessor
                .publish(v3_active(ConditionReport::Clear))
                .unwrap()
                .landed()
        );
        let mut successor = v3_writer(live.path()).with_ownership(session_token(), 2);
        assert!(
            successor
                .publish(v3_active(ConditionReport::Clear))
                .unwrap()
                .landed()
        );
        assert_eq!(
            predecessor
                .publish(v3_active(ConditionReport::Clear))
                .unwrap(),
            WriteOutcome::Refused(Refusal::Superseded {
                on_disk_seq: 2,
                ours: 1
            })
        );

        // Bytes this build cannot decode as either shape are somebody's record, never a virgin
        // seat.
        let unreadable = tempfile::tempdir().unwrap();
        fs::write(harness_state_path(unreadable.path()), b"{not json").unwrap();
        assert_eq!(
            v3_writer(unreadable.path())
                .publish(v3_active(ConditionReport::Clear))
                .unwrap(),
            WriteOutcome::Refused(Refusal::Unreadable)
        );

        // A condition needs an observation of this session's to attach to.
        let virgin = tempfile::tempdir().unwrap();
        assert_eq!(
            v3_writer(virgin.path())
                .raise_fault(quota_fault(crate::message::now_ms()))
                .unwrap(),
            WriteOutcome::Refused(Refusal::Unobserved)
        );

        // A legacy caller under version 3 states no condition axis, and `absent` is not writable:
        // the refusal is a value on the producer surface, and the legacy signature keeps its
        // exact current behaviour.
        let legacy = tempfile::tempdir().unwrap();
        let legacy_path = harness_state_path(legacy.path());
        let mut unstated = v3_writer(legacy.path());
        assert_eq!(
            unstated.mutate(Mutation::Legacy {
                observation: active(),
                skip_if_ended: false
            }).unwrap(),
            WriteOutcome::Refused(Refusal::Unstated)
        );
        assert!(unstated.observe(active()).is_ok());
        assert!(!legacy_path.exists());
    }

    /// A condition operation is never the thing that writes a terminal record: `ended` comes from
    /// the wrapper or the process-exit owner, which knows the exit, through the resolved-tuple
    /// path. A fault raised after that would re-publish `ended` from a caller that observed no
    /// exit at all, and a fence is not an observation either.
    #[test]
    fn no_condition_operation_writes_ended_and_a_fence_is_not_an_observation() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let mut writer = v3_writer(tmp.path());
        writer
            .publish(v3_active(ConditionReport::Fault(quota_fault(now_ms))))
            .unwrap();
        assert!(
            writer
                .publish(
                    Frame::new(
                        Activity::Ended,
                        InputBuffer::Unknown,
                        ConditionReport::Unchanged,
                        HumanAsk::None,
                    )
                    .with_exit("exit 0")
                )
                .unwrap()
                .landed(),
            "the exit-bearing owner writes the terminal record"
        );
        let terminal = record_json(&path);
        assert_eq!(terminal["state"], "ended");
        assert_eq!(
            terminal["condition"]["kind"], "fault",
            "and the standing condition rides along untouched"
        );

        assert_eq!(
            writer.raise_fault(quota_fault(now_ms + 1_000)).unwrap(),
            WriteOutcome::Refused(Refusal::Terminal)
        );
        assert_eq!(
            writer.clear_fault(quota_key()).unwrap(),
            WriteOutcome::Refused(Refusal::Terminal)
        );
        assert_eq!(
            writer.clear_all(ProgressProof::TurnCompleted).unwrap(),
            WriteOutcome::Refused(Refusal::Terminal)
        );
        assert_eq!(
            record_json(&path),
            terminal,
            "and none of them touched the incarnation's last word"
        );

        // The claim fence: a session that has claimed the seat and observed nothing. Its required
        // condition axis is not a statement, so nothing attaches to it and nothing is carried out
        // of it — planted rather than claimed so the fence's exact bytes are the subject, not
        // whichever version [`claim`] happens to emit.
        let claimed = tempfile::tempdir().unwrap();
        let fence_path = harness_state_path(claimed.path());
        let token = session_token();
        fs::write(
            &fence_path,
            format!(
                r#"{{"schema":"{SCHEMA_V3}","agent":"{FIXTURE_AGENT_ID}","harness":"codex","state":"ended","inputBuffer":"unknown","condition":{{"kind":"clear","detail":"{CLAIM_FENCE_DETAIL}"}},"ask":{{"kind":"unknown"}},"reason":"superseded","incarnation":"{token}","seq":4,"sinceMs":1,"writtenAtMs":{now_ms},"transitions":1}}"#
            ),
        )
        .unwrap();
        let mut fenced = v3_writer(claimed.path()).with_ownership(token, 4);
        assert_eq!(
            fenced.raise_fault(quota_fault(now_ms)).unwrap(),
            WriteOutcome::Refused(Refusal::Unobserved),
            "a fence is a claim, not an observation a fault can attach to"
        );
        assert_eq!(
            fenced.publish(v3_active(ConditionReport::Unchanged)).unwrap(),
            WriteOutcome::Refused(Refusal::Unstated),
            "and its required-but-meaningless `clear` is never carried forward as health"
        );
        assert_eq!(
            read(&fence_path, None)
                .expect("the fence")
                .indeterminacy
                .map(|indeterminacy| indeterminacy.reason),
            Some("claimed".to_string())
        );
    }

    /// OHS-R18: `none`, `pending(kind)`, and `unknown` are three DIFFERENT statements, and the
    /// third is writable. A maintained driver that positively has no observable ask surface (pi)
    /// states `unknown`: `none` would fabricate absence and `pending` would fabricate a human
    /// waiting on a queue nobody can answer. Neither projection invents `blockedOn: human`, and
    /// both round-trip.
    #[test]
    fn a_positively_unobservable_ask_is_writable_and_never_fabricates_a_waiting_human() {
        let two = tempfile::tempdir().unwrap();
        let two_path = harness_state_path(two.path());
        assert!(
            writer(two.path())
                .publish(Frame::new(
                    Activity::Active,
                    InputBuffer::Unknown,
                    ConditionReport::Clear,
                    HumanAsk::Unknown,
                ))
                .unwrap()
                .landed()
        );
        let record = record_json(&two_path);
        assert_eq!(
            record["blockedOn"], "unknown",
            "the only faithful version 2 spelling of an unobservable ask axis"
        );
        assert_eq!(
            record["ask"], "none",
            "and never the derived `unknown` ask word"
        );
        assert_eq!(
            read(&two_path, None).expect("a record").human_ask,
            HumanAsk::Unknown,
            "which reads back as exactly what was stated"
        );

        let three = tempfile::tempdir().unwrap();
        let three_path = harness_state_path(three.path());
        assert!(
            v3_writer(three.path())
                .publish(Frame::new(
                    Activity::Active,
                    InputBuffer::Unknown,
                    ConditionReport::Clear,
                    HumanAsk::Unknown,
                ))
                .unwrap()
                .landed()
        );
        assert_eq!(record_json(&three_path)["ask"]["kind"], "unknown");
        assert_eq!(
            read(&three_path, None).expect("a record").human_ask,
            HumanAsk::Unknown
        );

        // A real pending ask whose KIND is unclassified is a different statement again, and it
        // survives both projections without becoming an absence.
        let pending = tempfile::tempdir().unwrap();
        let pending_path = harness_state_path(pending.path());
        assert!(
            writer(pending.path())
                .publish(Frame::new(
                    Activity::Idle,
                    InputBuffer::Empty,
                    ConditionReport::Clear,
                    HumanAsk::Pending(AskKind::Unknown),
                ))
                .unwrap()
                .landed()
        );
        let record = record_json(&pending_path);
        assert_eq!(record["blockedOn"], "human");
        assert_eq!(record["ask"], "none");
        assert_eq!(
            read(&pending_path, None).expect("a record").human_ask,
            HumanAsk::Pending(AskKind::Unknown)
        );

        let tagged = tempfile::tempdir().unwrap();
        let tagged_path = harness_state_path(tagged.path());
        assert!(
            v3_writer(tagged.path())
                .publish(Frame::new(
                    Activity::Idle,
                    InputBuffer::Empty,
                    ConditionReport::Clear,
                    HumanAsk::Pending(AskKind::Unknown),
                ))
                .unwrap()
                .landed()
        );
        let record = record_json(&tagged_path);
        assert_eq!(record["ask"]["kind"], "pending");
        assert_eq!(record["ask"]["ask"], "unknown");
        assert_eq!(
            read(&tagged_path, None).expect("a record").human_ask,
            HumanAsk::Pending(AskKind::Unknown)
        );
    }

    /// A fault is not a state change. A condition-only write restates the activity axis verbatim,
    /// so `sinceMs` — when that state was ENTERED — must survive it, while the transition counter
    /// still advances because the bytes changed. And it must restate the RECORD's pty session:
    /// the caller is typically a hook process with no pty of its own, and stamping its own empty
    /// fence would strip a live record's only liveness evidence and flip the seat to `unknown`
    /// for every same-host reader.
    #[test]
    fn a_condition_only_write_keeps_the_activity_clock_and_the_records_own_pty_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let now_ms = crate::message::now_ms();
        let token = session_token();
        let mut wrapper = v3_writer(tmp.path()).with_session(token.clone());
        wrapper.publish(v3_active(ConditionReport::Clear)).unwrap();
        let observed = record_json(&path);
        assert_eq!(observed["ptySession"], "worker");

        // The hook process: same session, no pty of its own.
        let mut hook = Writer::new(tmp.path(), FIXTURE_AGENT_ID, "codex", None)
            .with_emitted_schema(SCHEMA_V3)
            .with_session(token);
        assert!(hook.raise_fault(quota_fault(now_ms)).unwrap().landed());
        let faulted = record_json(&path);
        assert_eq!(
            faulted["sinceMs"], observed["sinceMs"],
            "the activity did not change, so its clock did not restart"
        );
        assert_eq!(
            faulted["transitions"].as_u64(),
            observed["transitions"].as_u64().map(|count| count + 1),
            "but the bytes did, so the counter advanced"
        );
        assert!(faulted["writtenAtMs"].as_u64() > observed["writtenAtMs"].as_u64());
        assert_eq!(
            faulted["ptySession"], "worker",
            "the authoritative fence is restated, never replaced by the caller's absence"
        );
        assert_eq!(faulted["state"], "active");
        let read_back = read(&path, None).expect("a record");
        assert_eq!(
            read_back.state,
            Activity::Active,
            "so the seat stays definite instead of deriving unknown"
        );
        assert!(read_back.condition.fault().is_some());

        // The same holds for the clears.
        assert!(hook.clear_fault(quota_key()).unwrap().landed());
        let cleared = record_json(&path);
        assert_eq!(cleared["sinceMs"], observed["sinceMs"]);
        assert_eq!(cleared["ptySession"], "worker");

        // And a LIVE record that names no fence is not something a condition operation may write
        // back: an unfenced live state reads `unknown` for every same-host reader.
        let unfenced = tempfile::tempdir().unwrap();
        let unfenced_path = harness_state_path(unfenced.path());
        let orphan = session_token();
        let raw = format!(
            r#"{{"schema":"{SCHEMA_V3}","agent":"{FIXTURE_AGENT_ID}","harness":"codex","state":"active","inputBuffer":"empty","condition":{{"kind":"clear"}},"ask":{{"kind":"none"}},"incarnation":"{orphan}","seq":1,"sinceMs":1,"writtenAtMs":{now_ms},"transitions":1}}"#
        );
        fs::write(&unfenced_path, &raw).unwrap();
        let mut fenceless = Writer::new(unfenced.path(), FIXTURE_AGENT_ID, "codex", None)
            .with_emitted_schema(SCHEMA_V3)
            .with_ownership(orphan, 1);
        assert_eq!(
            fenceless.raise_fault(quota_fault(now_ms)).unwrap(),
            WriteOutcome::Refused(Refusal::Unfenced)
        );
        assert_eq!(fs::read(&unfenced_path).unwrap(), raw.into_bytes());
    }

    /// Wrapperless eligibility decodes BOTH writable shapes. A hooks-only session must be able to
    /// claim a version 3 record for exactly the reasons it can claim a version 2 one — otherwise,
    /// after the writer cutover, every record would look like undecodable bytes and no wrapperless
    /// session could ever claim again. Decided as a pure function so BOTH arms stay provable from
    /// one build whichever version its own writer emits.
    #[test]
    fn wrapperless_eligibility_decodes_version_three_records_too() {
        let now_ms = crate::message::now_ms();
        let planted = tempfile::tempdir().unwrap();
        let planted_path = harness_state_path(planted.path());
        let v3 = |incarnation: &str, state: &str, exit: &str, written_at_ms: u64| {
            let raw = format!(
                r#"{{"schema":"{SCHEMA_V3}","agent":"{FIXTURE_AGENT_ID}","harness":"claude","state":"{state}","inputBuffer":"unknown","condition":{{"kind":"clear"}},"ask":{{"kind":"none"}}{exit},"ptySession":"worker","incarnation":"{incarnation}","seq":2,"sinceMs":1,"writtenAtMs":{written_at_ms},"transitions":1}}"#
            );
            fs::write(&planted_path, &raw).unwrap();
            let current = read_current(&planted_path);
            assert!(
                matches!(current, CurrentRecord::V3(_)),
                "the fixture must decode as a version 3 record: {raw}"
            );
            current
        };

        // A live WRAPPER record is never claimable, whichever version states it.
        assert!(!wrapperless_eligible(
            SCHEMA_V3,
            &v3("wrapper-1", "active", "", now_ms),
            now_ms
        ));
        // A fellow wrapperless token is.
        assert!(wrapperless_eligible(
            SCHEMA_V3,
            &v3(
                &format!("{WRAPPERLESS_PREFIX}abc"),
                "active",
                "",
                now_ms
            ),
            now_ms
        ));
        // So is a real terminal record, and so is an abandoned one past the horizon.
        assert!(wrapperless_eligible(
            SCHEMA_V3,
            &v3("wrapper-1", "ended", r#","exit":"exit 0""#, now_ms),
            now_ms
        ));
        assert!(wrapperless_eligible(
            SCHEMA_V3,
            &v3(
                "wrapper-1",
                "active",
                "",
                now_ms - duration_ms(HARNESS_STATE_STALE) - 1
            ),
            now_ms
        ));
        // The one-way schema fence still holds from the other side: a version 2 writer — a peer
        // not yet replaced, or a rollback — has no eligible takeover of a version 3 record,
        // however orphaned it looks.
        assert!(!wrapperless_eligible(
            SCHEMA_V2,
            &v3(
                &format!("{WRAPPERLESS_PREFIX}abc"),
                "active",
                "",
                now_ms
            ),
            now_ms
        ));
        // Absence is claimable; undecodable bytes are not.
        assert!(wrapperless_eligible(
            SCHEMA_V3,
            &CurrentRecord::Absent,
            now_ms
        ));
        assert!(!wrapperless_eligible(
            SCHEMA_V3,
            &CurrentRecord::Unreadable,
            now_ms
        ));
        // And the legacy path is unchanged: a live wrapper record written by a version 2 writer
        // stays unclaimable to a version 2 claimer, and its terminal one stays claimable.
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let mut wrapper = writer(tmp.path());
        wrapper.observe(active()).unwrap();
        assert!(!wrapperless_eligible(
            SCHEMA_V2,
            &read_current(&path),
            now_ms
        ));
        wrapper.ended("exit 0").unwrap();
        assert!(wrapperless_eligible(
            SCHEMA_V2,
            &read_current(&path),
            now_ms
        ));
    }

    // -----------------------------------------------------------------------
    // Activation proofs: what changed at the flip, and what must not have.
    // -----------------------------------------------------------------------

    /// A legacy `observe` call still works after the cutover — but only on top of a stated axis,
    /// and it projects into the version 3 wire rather than into legacy bytes. This is what every
    /// unmigrated legacy call site now does: the tagged ask is derived from the legacy pair, the
    /// standing condition is carried forward untouched, and `blockedOn` is gone from the bytes.
    #[test]
    fn a_legacy_observation_projects_into_the_version_three_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let token = session_token();
        let mut producer = v3_writer(tmp.path()).with_session(token.clone());
        let fault = quota_fault(crate::message::now_ms() - 5_000);
        assert!(
            producer
                .publish(v3_active(ConditionReport::Fault(fault.clone())))
                .unwrap()
                .landed()
        );

        let mut legacy = v3_writer(tmp.path()).with_session(token);
        assert!(
            legacy
                .observe_unless_ended(
                    Observation::new(Activity::Idle, BlockedOn::Human, InputBuffer::Empty)
                        .with_ask(Ask::Permission)
                )
                .unwrap()
        );
        let record = record_json(&path);
        assert_eq!(record["schema"], SCHEMA_V3);
        assert!(
            record.get("blockedOn").is_none(),
            "the legacy pair is projected, not appended: {record}"
        );
        assert_eq!(record["ask"]["kind"], "pending");
        assert_eq!(record["ask"]["ask"], "permission");
        assert_eq!(
            record["condition"]["kind"], "fault",
            "an activity edge carries a standing fault forward and never clears it"
        );
        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Idle);
        assert_eq!(observed.human_ask, HumanAsk::Pending(AskKind::Permission));
        assert!(matches!(observed.condition, ConditionView::Fault(_)));
    }

    /// THE activation test. A wrapper is the one process that sees the provider die, and on many
    /// incarnations its terminal write is the session's FIRST write: the claim fence states
    /// nothing and is excluded from carry-forward, so version 3 refuses an unstated terminal
    /// frame. The shared wrapper path states the axis once and the record lands anyway — without
    /// it, wrapper-only `ended` would silently stop working and every such seat would age into
    /// `unknown` instead of reporting how it ended.
    #[test]
    fn a_terminal_record_lands_even_when_no_producer_ever_stated_the_condition_axis() {
        let tmp = tempfile::tempdir().unwrap();
        let path = harness_state_path(tmp.path());
        let token = session_token();
        let seq = claim(tmp.path(), FIXTURE_AGENT_ID, "pi", &token).unwrap();
        assert_eq!(record_json(&path)["schema"], SCHEMA_V3);
        assert!(
            read(&path, None).unwrap().state != Activity::Ended,
            "the fence is indeterminate, not a terminal record"
        );

        let mut wrapper =
            Writer::new(tmp.path(), FIXTURE_AGENT_ID, "pi", Some("worker".to_string()))
                .with_ownership(token, seq);
        // The legacy terminal call is exactly what refuses, which is why the wrappers moved.
        assert!(
            matches!(
                wrapper
                    .publish(
                        Frame::new(
                            Activity::Ended,
                            InputBuffer::Unknown,
                            ConditionReport::Unchanged,
                            HumanAsk::None
                        )
                        .with_exit("signal 9")
                    )
                    .unwrap(),
                WriteOutcome::Refused(Refusal::Unstated)
            ),
            "an unstated axis over a fence is refused, and that refusal is the retry's premise"
        );
        crate::provider_session::write_terminal(
            &mut wrapper,
            "signal 9",
            None,
            ConditionReport::Clear,
        )
        .unwrap();

        let observed = read(&path, None).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("signal 9"));
        assert_eq!(
            observed.condition,
            ConditionView::Clear,
            "the wrapper states the axis once rather than fabricating a fault"
        );
    }

    /// The fence from the ACTIVATED side: this build's claim supersedes every supported version,
    /// including its own, and no supported version supersedes it. Both halves matter during a
    /// staged rollout — the first is why an activated binary can adopt any seat it finds, the
    /// second is why a peer that has not been replaced cannot undo that.
    #[test]
    fn a_version_three_claim_supersedes_every_supported_version_and_is_never_superseded() {
        for found in [SCHEMA_V1, SCHEMA_V2, SCHEMA_V3] {
            assert!(
                claim_may_supersede(SCHEMA_V3, Some(found)),
                "a version 3 claim must supersede {found}"
            );
        }
        for ours in [SCHEMA_V1, SCHEMA_V2] {
            assert!(
                !claim_may_supersede(ours, Some(SCHEMA_V3)),
                "{ours} must never supersede a version 3 record"
            );
        }

        // End to end, through the real locked claim: a version 2 record on disk is superseded by
        // this build and the sequence continues across the version change.
        let tmp = tempfile::tempdir().unwrap();
        let path = planted(tmp.path(), SCHEMA_V2, FIXTURE_AGENT_ID, 5);
        let seq = claim(tmp.path(), FIXTURE_AGENT_ID, "codex", &session_token()).unwrap();
        assert_eq!(seq, 6);
        assert_eq!(record_json(&path)["schema"], SCHEMA_V3);
    }

    /// Legacy readability, after the writer moved: the frozen version 1 and version 2 fixtures
    /// still project every axis a consumer joins to, and their condition reads as explicitly
    /// ABSENT rather than as health. Nothing about the cutover may turn a record written by an
    /// older binary into a claim that its harness was fine.
    #[test]
    fn a_legacy_record_is_still_read_and_projected_after_the_writer_cutover() {
        assert_eq!(SCHEMA, SCHEMA_V3, "this is the post-cutover statement");
        for raw in [
            include_str!("../tests/fixtures/harness-state/v1-active.json"),
            include_str!("../tests/fixtures/harness-state/v2-blocked.json"),
        ] {
            let observed = fixture(raw);
            assert!(
                observed.state != Activity::Unknown,
                "a legacy record is a definite observation: {observed:?}"
            );
            assert!(observed.subject.is_some());
            assert_eq!(
                observed.condition,
                ConditionView::Absent,
                "a version without the axis states nothing on it: {observed:?}"
            );
        }
    }
}
