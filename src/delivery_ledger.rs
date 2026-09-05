//! The shared native-delivery ledger.
//!
//! Every native transport used to keep its own single-binding `delivery-state.json` holding one
//! `{binding, filename, correlation, phase in {Attempted, Accepted}}`. `Accepted` meant something
//! different in each driver — a typed completed user message on Codex (the model consumed it), a
//! `GET 200` on OpenCode (the server merely stored it) — and nothing on disk said which. This
//! module replaces that with one evidence-graded ledger:
//!
//! * **Fresh namespace.** The record lives at `<state-dir>/delivery-ledger.json` under schema
//!   [`LEDGER_SCHEMA`]. The v1 path is never written as authority again. An in-place schema bump
//!   was measured to be unrecoverable: v1's Codex loader `ensure!`s its schema and denies unknown
//!   fields, so a v2 body there refuses to start the control connection at all, and v1's OpenCode
//!   loader silently discards it and re-POSTs the same message id — which appends its parts a
//!   second time on 1.18.19.
//! * **Monotone phases.** [`Phase`] orders `attempted < transportAccepted < persisted < admitted
//!   < consumed`. A write that would lower a phase is refused, so a restart at any persistence
//!   boundary can only ever read a true lower bound of what happened.
//! * **Per-filename identity under a shared correlation.** Entries are keyed by message filename
//!   and carry the durable native correlation they were transported under. N filenames may share
//!   one correlation value, so a bounded multi-message FIFO prefix is N monotone entries rather
//!   than one record naming one file.
//! * **Honest adapter grading.** A [`Profile`] declares what its harness can actually prove and
//!   what it could prove if every signal were wired. Evidence a harness cannot honestly produce is
//!   refused rather than recorded, and OpenCode's `persisted` therefore never reads as consumption.
//! * **One-shot label-downgrading adoption.** A pre-existing v1 record is carried forward exactly
//!   once, at the evidence it actually proved: Codex `Accepted` → [`Phase::Consumed`], OpenCode
//!   `Accepted` → [`Phase::Persisted`], either `Attempted` → [`Phase::Attempted`]. Adoption is
//!   recorded as such and authorizes no transport by itself.
//! * **A rollback-readable floor.** Before the first transport of a delivery this module writes a
//!   v1-*shaped* `Attempted` record at the old path and re-asserts it while the entry is
//!   outstanding. It is never advanced, so it cannot contradict the ledger and no old binary can
//!   read it as acceptance — it is the lower bound a rolled-back binary needs in order not to
//!   re-POST a delivery this binary started.
//! * **Fail closed, never fail to start.** An unreadable or foreign ledger quarantines the pump:
//!   no transport is authorized and the reason is retained. It never propagates as a startup error,
//!   because refusing to start is strictly worse than holding.
//!
//! What this module deliberately does not own: the inbox. Archive remains the recipient agent's
//! act and the sole settlement authority (`message::archive_msg`). "Release" here means the ledger
//! stops holding FIFO ownership and drops the rollback floor — never that a file moves.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::message;

/// The ledger's own schema. Bumping this is a fresh-namespace decision, not an in-place edit.
pub const LEDGER_SCHEMA: &str = "st2.delivery-ledger.v1";
/// The ledger filename, a sibling of the v1 state file inside the same per-harness state dir.
pub const LEDGER_FILE: &str = "delivery-ledger.json";
/// The legacy v1 filename: adoption source and rollback floor, never authority.
pub const LEGACY_FILE: &str = "delivery-state.json";
/// Codex's v1 delivery-state schema.
pub const CODEX_LEGACY_SCHEMA: &str = "st2.codex-delivery-state.v1";
/// OpenCode's v1 delivery-state schema.
pub const OPENCODE_LEGACY_SCHEMA: &str = "st2.opencode-delivery-state.v1";

/// A native transport with a durable delivery record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Harness {
    Codex,
    OpenCode,
}

impl Harness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::OpenCode => "opencode",
        }
    }

    /// Fail closed on a harness this build does not know: an unrecognized name is refused rather
    /// than defaulted, because a default would grade some other adapter's evidence with these
    /// rules.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "codex" => Ok(Self::Codex),
            "opencode" => Ok(Self::OpenCode),
            other => anyhow::bail!("unknown native delivery harness '{other}'"),
        }
    }

    pub fn legacy_schema(self) -> &'static str {
        match self {
            Self::Codex => CODEX_LEGACY_SCHEMA,
            Self::OpenCode => OPENCODE_LEGACY_SCHEMA,
        }
    }

    /// The v1 field naming the thread or session a delivery was bound to.
    fn legacy_binding_key(self) -> &'static str {
        match self {
            Self::Codex => "threadId",
            Self::OpenCode => "sessionId",
        }
    }

    /// The v1 field carrying the durable correlation the transport was sent under.
    fn legacy_correlation_key(self) -> &'static str {
        match self {
            Self::Codex => "clientId",
            Self::OpenCode => "messageId",
        }
    }

    /// What v1's `Accepted` label actually proved on this harness.
    fn adopted_accepted_phase(self) -> Phase {
        match self {
            // Written only from the typed `item/completed{userMessage, clientId}` inside a turn:
            // the model received it.
            Self::Codex => Phase::Consumed,
            // Written on `GET /session/{s}/message/{m}` returning 200: the server stored it. That
            // is not scheduling, so mapping it to consumption would make the stored-but-never-
            // admitted class permanently unretryable.
            Self::OpenCode => Phase::Persisted,
        }
    }

    pub fn profile(self) -> Profile {
        match self {
            Self::Codex => Profile {
                harness: self,
                correlation: CorrelationKind::Native,
                ceiling: Phase::Consumed,
                ceiling_if_wired: Phase::Consumed,
                // The same `clientUserMessageId` re-sent blind is not a proven no-op; Codex earns
                // a retry from a `thread/resume` sweep that proves definite absence.
                idempotent_resend: false,
                retry: RetryPolicy::AtMostOnce,
            },
            Self::OpenCode => Profile {
                harness: self,
                correlation: CorrelationKind::Native,
                // What this build can observe. `session.next.prompt.admitted` is not wired, so
                // persistence is the ceiling — and because it is below what the harness could
                // prove, persistence never releases ownership.
                ceiling: Phase::Persisted,
                ceiling_if_wired: Phase::Admitted,
                // Measured on 1.18.19: a second POST with the same messageID appends its parts
                // again into the same message. Re-sending is a duplicate, not an idempotent retry.
                idempotent_resend: false,
                retry: RetryPolicy::AtMostOnce,
            },
        }
    }
}

/// What a harness's evidence can prove, and what it could prove if every signal were wired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    pub harness: Harness,
    pub correlation: CorrelationKind,
    /// The highest phase this build's evidence can actually reach.
    pub ceiling: Phase,
    /// The highest phase the harness itself could prove. When it exceeds [`Profile::ceiling`],
    /// reaching the ceiling is not a settlement — the missing signal is merely unread.
    pub ceiling_if_wired: Phase,
    /// Whether re-sending the same correlation is a measured no-op.
    pub idempotent_resend: bool,
    /// The declared policy where correlation cannot prove anything either way.
    pub retry: RetryPolicy,
}

impl Profile {
    /// Fail closed on evidence this harness cannot honestly produce.
    fn graded(&self, evidence: Evidence) -> Result<Phase> {
        let phase = match evidence {
            Evidence::TransportAccepted => Phase::TransportAccepted,
            Evidence::Persisted => Phase::Persisted,
            Evidence::Admitted => Phase::Admitted,
            Evidence::Consumed => Phase::Consumed,
        };
        anyhow::ensure!(
            self.proves(phase),
            "{} delivery evidence cannot prove phase {phase:?}",
            self.harness.as_str()
        );
        Ok(phase)
    }

    fn proves(&self, phase: Phase) -> bool {
        match self.harness {
            // Codex has a JSON-RPC result (transport) and a typed completed user message
            // (consumption). It has no storage receipt and no scheduler admission signal.
            Harness::Codex => matches!(
                phase,
                Phase::Attempted | Phase::TransportAccepted | Phase::Consumed
            ),
            // OpenCode has a POST status (transport), a durable message read-back (storage) and,
            // once wired, prompt admission. It never proves the model consumed anything.
            Harness::OpenCode => matches!(
                phase,
                Phase::Attempted | Phase::TransportAccepted | Phase::Persisted | Phase::Admitted
            ),
        }
    }
}

/// The declared policy for a harness whose correlation cannot settle a replay either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Hold and surface rather than risk a duplicate. The conservative default.
    AtMostOnce,
    /// Accept a possible duplicate rather than risk a lost delivery.
    AtLeastOnce,
}

/// The monotone delivery phase. Declaration order is the ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Phase {
    /// Durable before transport. Says only that this process was about to send.
    Attempted,
    /// The transport call itself succeeded. Says nothing about the harness's own state.
    TransportAccepted,
    /// The harness durably holds the exact correlated message. Storage, not scheduling.
    Persisted,
    /// The harness's scheduler took it as input.
    Admitted,
    /// The model received it.
    Consumed,
}

/// How durable a delivery's correlation is. Only [`CorrelationKind::Native`] can settle a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorrelationKind {
    /// A durable harness-side identity st2 chose and can re-query.
    Native,
    /// An attributable content echo. Confirms only a uniquely attributable match.
    Content,
    /// Live-only acknowledgement; nothing survives the process.
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Correlation {
    pub kind: CorrelationKind,
    pub value: String,
}

impl Correlation {
    pub fn native(value: impl Into<String>) -> Self {
        Self {
            kind: CorrelationKind::Native,
            value: value.into(),
        }
    }
}

/// An authoritative "no" about a specific attempt. The only thing besides a durable idempotent
/// correlation that may authorize a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegativeReceipt {
    /// The harness authoritatively does not hold the correlated message.
    Absent,
    /// The transport refused this attempt.
    Rejected,
}

/// Positive evidence a driver extracted from its harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    TransportAccepted,
    Persisted,
    Admitted,
    Consumed,
}

/// One message's delivery, keyed by inbox filename.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub filename: String,
    /// The thread or session this delivery is bound to. A different binding is a different
    /// delivery: its receipt may neither suppress nor acknowledge delivery to another one.
    pub binding: String,
    pub correlation: Correlation,
    pub phase: Phase,
    /// The runtime incarnation that made the attempt. Live evidence acknowledges only its own
    /// incarnation; a pre-crash attempt is settled by a history sweep, never by a live frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<String>,
    /// The v1 schema this entry was carried forward from, when it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adopted_from: Option<String>,
    /// Whether fresh evidence has authorized another transport. Adoption sets this false and never
    /// true: carrying a record forward is not evidence about the world.
    pub retry_eligible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub negative: Option<NegativeReceipt>,
    /// The exact v1-shaped `Attempted` record written at the legacy path for this entry, retained
    /// so it can be re-asserted after a restart while the entry is still outstanding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_floor: Option<Value>,
}

/// The on-disk ledger. Additive-tolerant on read: unknown fields are ignored, but an unknown
/// schema, harness, phase, or owner is not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    schema: String,
    harness: String,
    agent: String,
    runtime_id: String,
    entries: Vec<Entry>,
}

/// Whether the ledger still holds FIFO ownership of a delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// Ownership released: stop offering, drop the rollback floor. Never an archive.
    Release,
    Hold(HoldReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    /// Transport happened; nothing about the harness's state is proved yet.
    AmbiguousAttempt,
    /// The harness holds it, but the evidence that would settle it is merely unread.
    UnreadReceipt,
    /// The harness said no. The item is re-offered.
    NegativeReceipt,
    /// Carried forward from v1: enough to suppress a duplicate, never enough to retry.
    AdoptedWithoutFreshEvidence,
    /// The ledger could not be read. Nothing may be transported.
    Quarantined,
    /// Already settled: ownership was released, so nothing is owed.
    Settled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    Hold(HoldReason),
}

/// A first transport's durable pre-conditions.
#[derive(Debug, Clone)]
pub struct Begin {
    pub filename: String,
    pub binding: String,
    pub correlation: Correlation,
    pub incarnation: Option<String>,
    /// The v1-shaped rollback floor an old binary must be able to read. Build it with
    /// [`codex_floor`] or [`opencode_floor`].
    pub legacy_floor: Value,
}

pub struct Ledger {
    path: PathBuf,
    legacy_path: PathBuf,
    profile: Profile,
    record: Record,
    quarantine: Option<String>,
}

impl Ledger {
    /// Open the ledger beside `legacy_path`, adopting the v1 record exactly once if this is the
    /// first run on the new schema.
    ///
    /// `correlate(binding, filename)` recomputes the harness's durable correlation. It is the same
    /// derivation the transport uses, so a record whose correlation does not match its own binding
    /// is provably not this agent's and fails closed.
    ///
    /// Never returns an error: an unreadable record quarantines the pump instead of refusing to
    /// start, because a driver that will not start delivers nothing at all.
    pub fn open(
        legacy_path: &Path,
        profile: Profile,
        agent: &str,
        runtime_id: &str,
        correlate: impl Fn(&str, &str) -> String,
    ) -> Self {
        let mut ledger = Self {
            path: legacy_path.with_file_name(LEDGER_FILE),
            legacy_path: legacy_path.to_path_buf(),
            profile,
            record: Record {
                schema: LEDGER_SCHEMA.to_string(),
                harness: profile.harness.as_str().to_string(),
                agent: agent.to_string(),
                runtime_id: runtime_id.to_string(),
                entries: Vec::new(),
            },
            quarantine: None,
        };
        let outcome = match fs::read(&ledger.path) {
            Ok(bytes) => ledger.load(&bytes, &correlate),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ledger.adopt(&correlate)
            }
            Err(error) => Err(error).with_context(|| {
                format!("reading delivery ledger {}", ledger.path.display())
            }),
        };
        if let Err(error) = outcome {
            ledger.record.entries.clear();
            ledger.quarantine = Some(format!("{error:#}"));
        } else {
            ledger.rebind_floor_runtime_ids();
        }
        ledger
    }

    fn load(&mut self, bytes: &[u8], correlate: &impl Fn(&str, &str) -> String) -> Result<()> {
        let record: Record = serde_json::from_slice(bytes)
            .with_context(|| format!("reading delivery ledger {}", self.path.display()))?;
        anyhow::ensure!(
            record.schema == LEDGER_SCHEMA,
            "delivery ledger has unsupported schema '{}'",
            record.schema
        );
        anyhow::ensure!(
            Harness::parse(&record.harness)? == self.profile.harness,
            "delivery ledger belongs to harness '{}'",
            record.harness
        );
        // The agent identity is the durable owner; the runtime id is a mutable address. A seat
        // relaunched under a new runtime is the SAME recipient holding the same outstanding
        // deliveries, so drift rebinds the record rather than quarantining or discarding
        // evidence — discarding it is exactly how a held ambiguous delivery becomes a duplicate.
        // A rebind is bookkeeping, never evidence about the world: entries cross unchanged,
        // including the incarnation that made each attempt, so ambiguous entries stay held, a
        // live typed receipt still cannot settle a pre-crash attempt, and nothing about the
        // rebind authorizes a resend.
        anyhow::ensure!(
            record.agent == self.record.agent,
            "delivery ledger belongs to a different agent"
        );
        let owner_runtime_id = self.record.runtime_id.clone();
        for entry in &record.entries {
            Self::validate(&self.profile, entry)?;
        }
        anyhow::ensure!(
            record
                .entries
                .windows(2)
                .all(|pair| pair[0].binding == pair[1].binding),
            "delivery ledger holds entries from more than one binding"
        );
        // A bounded multi-message prefix shares ONE correlation: the durable identity its head was
        // transported under. So a correlation is anchored when SOME entry carrying it derives it
        // from its own binding and filename — requiring that of every entry would make a batch
        // unloadable, and requiring it of none would accept a value nothing here was ever sent
        // with.
        if self.profile.correlation == CorrelationKind::Native {
            for entry in &record.entries {
                anyhow::ensure!(
                    record.entries.iter().any(|anchor| {
                        anchor.correlation.value == entry.correlation.value
                            && anchor.correlation.value
                                == correlate(&anchor.binding, &anchor.filename)
                    }),
                    "delivery ledger entry correlation does not match its binding"
                );
            }
        }
        self.record = record;
        // Provenance, not a fence: the rebound runtime id needs no write of its own and rides
        // along on the next durable write this pump makes.
        self.record.runtime_id = owner_runtime_id;
        Ok(())
    }

    /// Carry the current runtime id onto every retained rollback floor.
    ///
    /// A rolled-back Codex loader compares `runtimeId`, so a floor still naming the runtime that
    /// wrote it would be refused after a relaunch and the rollback would have no lower bound at
    /// all — the duplicate class the floor exists to remove. Only that one mutable address is
    /// rewritten. `clientId`, `runtimeIncarnation`, `threadId`/`sessionId`, `filename` and the
    /// never-advanced `phase` are left exactly as written: they identify the attempt itself, and
    /// v1 revalidates the client ID against the thread and filename it still carries. The floor
    /// stays a true lower bound, and the corrected bytes land on the next per-pass re-assert,
    /// which both drivers perform before any transport.
    fn rebind_floor_runtime_ids(&mut self) {
        let runtime_id = Value::String(self.record.runtime_id.clone());
        for entry in &mut self.record.entries {
            if let Some(object) = entry.legacy_floor.as_mut().and_then(Value::as_object_mut)
                && object.get("runtimeId") != Some(&runtime_id)
            {
                object.insert("runtimeId".to_string(), runtime_id.clone());
            }
        }
    }

    /// Per-entry structural validation, independent of any correlation grouping.
    fn validate(profile: &Profile, entry: &Entry) -> Result<()> {
        anyhow::ensure!(
            message::is_message_filename(&entry.filename) && !entry.binding.is_empty(),
            "delivery ledger entry has an invalid binding or filename"
        );
        anyhow::ensure!(
            entry.correlation.kind == profile.correlation,
            "delivery ledger entry has a correlation kind this harness does not use"
        );
        anyhow::ensure!(
            profile.proves(entry.phase),
            "delivery ledger entry records a phase {} cannot prove",
            profile.harness.as_str()
        );
        Ok(())
    }

    /// One-shot carry-forward of the v1 record, at the evidence its label actually proved.
    ///
    /// Adoption runs only when no ledger exists, and persists whatever it carried — so the ledger
    /// it writes is what makes adoption happen once. With nothing to adopt it writes nothing,
    /// because a driver that never delivers should leave no record behind. A legacy record naming
    /// another schema or another agent is ignored rather than quarantined — it is not this
    /// ledger's authority — but a record of ours whose correlation contradicts its own binding
    /// fails closed.
    fn adopt(&mut self, correlate: &impl Fn(&str, &str) -> String) -> Result<()> {
        let Some(entry) = self.read_legacy(correlate)? else {
            return Ok(());
        };
        self.record.entries.push(entry);
        self.persist()
    }

    fn read_legacy(&self, correlate: &impl Fn(&str, &str) -> String) -> Result<Option<Entry>> {
        let bytes = match fs::read(&self.legacy_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading v1 delivery state {}", self.legacy_path.display())
                });
            }
        };
        let legacy: Value = match serde_json::from_slice(&bytes) {
            Ok(legacy) => legacy,
            // Unreadable bytes at the v1 path are not authority and cannot be adopted; the new
            // ledger simply starts empty rather than quarantining a working pump.
            Err(_) => return Ok(None),
        };
        let harness = self.profile.harness;
        let string = |key: &str| legacy.get(key).and_then(Value::as_str).unwrap_or_default();
        // The agent identity is the durable owner, and it is the whole ownership test here.
        //
        // The runtime id is deliberately NOT compared, for a different reason on each harness.
        // OpenCode's v1 filter never looked at it, so comparing it would be stricter than v1 and
        // would drop a record the old binary would have acted on. Codex's v1 loader did compare
        // it — and hard-errored, refusing to start — but a drifted record still describes a real
        // attempt this recipient made, so it is carried forward at the phase it proved with
        // `retryEligible: false`. Held that way, the `thread/resume` sweep can settle it or
        // refuse it BEFORE anything is sent, which is strictly better than ignoring it and
        // opening a second delivery for the same message.
        if string("schema") != harness.legacy_schema() || string("agent") != self.record.agent {
            return Ok(None);
        }
        let binding = string(harness.legacy_binding_key()).to_string();
        let filename = string("filename").to_string();
        let value = string(harness.legacy_correlation_key()).to_string();
        anyhow::ensure!(
            message::is_message_filename(&filename) && !binding.is_empty(),
            "v1 delivery state has an invalid binding or filename"
        );
        anyhow::ensure!(
            value == correlate(&binding, &filename),
            "v1 delivery state correlation does not match its binding"
        );
        let phase = match string("phase") {
            "attempted" => Phase::Attempted,
            "accepted" => harness.adopted_accepted_phase(),
            other => anyhow::bail!("v1 delivery state has an unknown phase '{other}'"),
        };
        let incarnation = legacy
            .get("runtimeIncarnation")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(Some(Entry {
            filename,
            binding,
            correlation: Correlation {
                kind: CorrelationKind::Native,
                value,
            },
            phase,
            incarnation,
            adopted_from: Some(harness.legacy_schema().to_string()),
            // Adoption is a carried-forward claim, never an observation: it can suppress a
            // duplicate but it authorizes no transport until fresh evidence arrives.
            retry_eligible: false,
            negative: None,
            // The record we adopted IS this entry's floor: it already exists at the v1 path, it is
            // v1-readable, and it is never advanced from here.
            legacy_floor: Some(legacy),
        }))
    }

    /// The reason nothing may be transported, if the record could not be read.
    pub fn quarantined(&self) -> Option<&str> {
        self.quarantine.as_deref()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.record.entries
    }

    pub fn entry(&self, filename: &str) -> Option<&Entry> {
        self.record
            .entries
            .iter()
            .find(|entry| entry.filename == filename)
    }

    /// The binding every held entry shares, if any.
    pub fn binding(&self) -> Option<&str> {
        self.record
            .entries
            .first()
            .map(|entry| entry.binding.as_str())
    }

    /// The filenames delivered under one correlation. A bounded multi-message prefix is several
    /// entries sharing one value, so one receipt settles all of them at once.
    pub fn correlated(&self, value: &str) -> Vec<String> {
        self.record
            .entries
            .iter()
            .filter(|entry| entry.correlation.value == value)
            .map(|entry| entry.filename.clone())
            .collect()
    }

    /// Durably own an attempt before transporting it. Both durable writes — the ledger entry and
    /// the v1-readable floor — complete before this call returns, and the caller transports only
    /// afterwards. A crash between them leaves either nothing, or a floor that adoption reads
    /// back as exactly this ambiguous attempt.
    pub fn begin(&mut self, begin: Begin) -> Result<Entry> {
        anyhow::ensure!(
            self.quarantine.is_none(),
            "delivery ledger is quarantined: {}",
            self.quarantine.as_deref().unwrap_or_default()
        );
        anyhow::ensure!(
            begin.correlation.kind == self.profile.correlation,
            "{} delivery cannot use this correlation kind",
            self.profile.harness.as_str()
        );
        // Deliberately not written yet: v1 holds ONE record, so the floor that belongs there is
        // the oldest outstanding attempt, which is only knowable once this entry has landed.
        let held = self
            .record
            .entries
            .iter()
            .position(|entry| entry.filename == begin.filename);
        let entry = match held {
            Some(index) => {
                let entry = &mut self.record.entries[index];
                entry.binding = begin.binding;
                entry.correlation = begin.correlation;
                entry.incarnation = begin.incarnation;
                // Monotone: a repeat attempt never lowers what was already proved.
                entry.phase = entry.phase.max(Phase::Attempted);
                // A live attempt is ambiguous again, and it supersedes the receipt that authorized
                // it — the next retry must earn its own evidence. It is also no longer a merely
                // carried-forward claim: this build transported it.
                entry.negative = None;
                entry.retry_eligible = false;
                entry.adopted_from = None;
                entry.legacy_floor = Some(begin.legacy_floor);
                entry.clone()
            }
            None => {
                let entry = Entry {
                    filename: begin.filename,
                    binding: begin.binding,
                    correlation: begin.correlation,
                    phase: Phase::Attempted,
                    incarnation: begin.incarnation,
                    adopted_from: None,
                    retry_eligible: false,
                    negative: None,
                    legacy_floor: Some(begin.legacy_floor),
                };
                self.record.entries.push(entry.clone());
                entry
            }
        };
        self.persist()?;
        // The floor lands AFTER the entry, and it is the OLDEST outstanding one: a batch must
        // leave the earliest attempt's lower bound at the v1 path, never the most recent.
        self.reassert_floor()?;
        Ok(entry)
    }

    /// Record positive evidence. Refuses evidence the harness cannot prove, and never lowers a
    /// phase. Returns the entry's phase after the write, or `None` when no such entry is held.
    pub fn record(&mut self, filename: &str, evidence: Evidence) -> Result<Option<Phase>> {
        anyhow::ensure!(
            self.quarantine.is_none(),
            "delivery ledger is quarantined: {}",
            self.quarantine.as_deref().unwrap_or_default()
        );
        let phase = self.profile.graded(evidence)?;
        let Some(index) = self
            .record
            .entries
            .iter()
            .position(|entry| entry.filename == filename)
        else {
            return Ok(None);
        };
        {
            let entry = &mut self.record.entries[index];
            if phase <= entry.phase {
                return Ok(Some(entry.phase));
            }
            entry.phase = phase;
            // Positive evidence supersedes an earlier "no" about the same attempt.
            entry.negative = None;
            entry.retry_eligible = false;
        }
        self.persist()?;
        self.settle()?;
        Ok(Some(phase))
    }

    /// Record an authoritative "no". Ignored once the entry is settled: a late refusal cannot
    /// un-consume a delivery.
    pub fn negative(&mut self, filename: &str, receipt: NegativeReceipt) -> Result<Retention> {
        anyhow::ensure!(
            self.quarantine.is_none(),
            "delivery ledger is quarantined: {}",
            self.quarantine.as_deref().unwrap_or_default()
        );
        let retention = self.retention(filename);
        if retention == Retention::Release {
            return Ok(retention);
        }
        let Some(index) = self
            .record
            .entries
            .iter()
            .position(|entry| entry.filename == filename)
        else {
            return Ok(retention);
        };
        {
            let entry = &mut self.record.entries[index];
            if entry.negative == Some(receipt) && entry.retry_eligible {
                return Ok(Retention::Hold(HoldReason::NegativeReceipt));
            }
            entry.negative = Some(receipt);
            entry.retry_eligible = true;
        }
        self.persist()?;
        Ok(Retention::Hold(HoldReason::NegativeReceipt))
    }

    /// Whether the ledger still holds FIFO ownership of `filename`.
    ///
    /// Release requires either scheduler-or-better evidence, or reaching a ceiling that is the
    /// harness's true ceiling. A ceiling below what the harness could prove is not a settlement:
    /// OpenCode `persisted` holds with [`HoldReason::UnreadReceipt`] rather than releasing.
    pub fn retention(&self, filename: &str) -> Retention {
        if self.quarantine.is_some() {
            return Retention::Hold(HoldReason::Quarantined);
        }
        let Some(entry) = self.entry(filename) else {
            return Retention::Release;
        };
        if entry.negative.is_some() {
            return Retention::Hold(HoldReason::NegativeReceipt);
        }
        if entry.phase >= Phase::Admitted {
            return Retention::Release;
        }
        if entry.phase == self.profile.ceiling
            && self.profile.ceiling == self.profile.ceiling_if_wired
        {
            return Retention::Release;
        }
        if entry.phase >= Phase::Persisted {
            return Retention::Hold(HoldReason::UnreadReceipt);
        }
        if entry.adopted_from.is_some() && !entry.retry_eligible {
            return Retention::Hold(HoldReason::AdoptedWithoutFreshEvidence);
        }
        Retention::Hold(HoldReason::AmbiguousAttempt)
    }

    /// Whether another transport of `filename` is authorized.
    pub fn retry(&self, filename: &str) -> RetryDecision {
        if self.quarantine.is_some() {
            return RetryDecision::Hold(HoldReason::Quarantined);
        }
        let Some(entry) = self.entry(filename) else {
            // Nothing is held, so nothing is being repeated: a first transport is not a retry.
            return RetryDecision::Retry;
        };
        match self.retention(filename) {
            Retention::Release => return RetryDecision::Hold(HoldReason::Settled),
            Retention::Hold(HoldReason::NegativeReceipt) => return RetryDecision::Retry,
            Retention::Hold(_) => {}
        }
        if entry.adopted_from.is_some() && !entry.retry_eligible {
            return RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence);
        }
        if entry.phase >= Phase::Persisted {
            return RetryDecision::Hold(HoldReason::UnreadReceipt);
        }
        if entry.phase > Phase::Attempted {
            // The transport call itself landed. Nothing readable proves the harness did not get
            // it, so a second send is a duplicate risk, not a recovery.
            return RetryDecision::Hold(HoldReason::AmbiguousAttempt);
        }
        match entry.correlation.kind {
            CorrelationKind::Native if self.profile.idempotent_resend => RetryDecision::Retry,
            CorrelationKind::Native => RetryDecision::Hold(HoldReason::AmbiguousAttempt),
            CorrelationKind::Content | CorrelationKind::None => match self.profile.retry {
                RetryPolicy::AtLeastOnce => RetryDecision::Retry,
                RetryPolicy::AtMostOnce => RetryDecision::Hold(HoldReason::AmbiguousAttempt),
            },
        }
    }

    /// Drop every entry whose binding is not `binding`. A newly selected thread or session is a
    /// different delivery binding, and the old one's receipt may neither suppress nor acknowledge
    /// delivery to this one.
    pub fn rebind(&mut self, binding: &str) -> Result<()> {
        let before = self.record.entries.len();
        self.record.entries.retain(|entry| entry.binding == binding);
        if self.record.entries.len() == before {
            return Ok(());
        }
        self.persist()?;
        self.settle()
    }

    /// Reconcile the ledger to what the recipient still has unread. Archive is the recipient's
    /// act and the settlement authority: an entry whose file left the inbox is released, never
    /// re-offered, and never archived from here.
    pub fn prune(&mut self, is_unread: impl Fn(&str) -> bool) -> Result<()> {
        let before = self.record.entries.len();
        self.record
            .entries
            .retain(|entry| is_unread(&entry.filename));
        if self.record.entries.len() == before {
            return Ok(());
        }
        self.persist()?;
        self.settle()
    }

    /// The floor of the OLDEST outstanding delivery. v1 holds a single record, so when several
    /// entries are outstanding the one lower bound it can carry must be the earliest attempt.
    /// Message filenames start with their unix-ms send time, so lexicographic order is FIFO
    /// order — the same grammar the bus already relies on.
    fn oldest_floor(&self) -> Option<Value> {
        self.record
            .entries
            .iter()
            .filter(|entry| entry.legacy_floor.is_some())
            .min_by(|left, right| left.filename.cmp(&right.filename))
            .and_then(|entry| entry.legacy_floor.clone())
    }

    /// Re-assert the rollback floor while an entry is outstanding. Called on every pass: a crash
    /// exactly at the floor write would otherwise leave a landing with no v1-readable lower bound.
    pub fn reassert_floor(&mut self) -> Result<()> {
        let Some(floor) = self.oldest_floor() else {
            return Ok(());
        };
        self.write_legacy(&floor)
    }

    /// Drop the floor of every released entry, then converge the v1 path: the head outstanding
    /// floor, or nothing at all.
    fn settle(&mut self) -> Result<()> {
        let released: Vec<String> = self
            .record
            .entries
            .iter()
            .filter(|entry| entry.legacy_floor.is_some())
            .filter(|entry| self.retention(&entry.filename) == Retention::Release)
            .map(|entry| entry.filename.clone())
            .collect();
        if !released.is_empty() {
            for entry in &mut self.record.entries {
                if released.contains(&entry.filename) {
                    entry.legacy_floor = None;
                }
            }
            self.persist()?;
        }
        match self.oldest_floor() {
            Some(floor) => self.write_legacy(&floor),
            None => remove_file(&self.legacy_path),
        }
    }

    /// Write the v1 rollback floor, skipping a byte-identical restatement. An outstanding
    /// delivery re-asserts its floor on every pass, and an unread message stays outstanding for
    /// as long as the recipient leaves it in the inbox — so restating must cost a read, not an
    /// fsync per pass.
    fn write_legacy(&self, floor: &Value) -> Result<()> {
        let bytes = serde_json::to_vec(floor)?;
        if fs::read(&self.legacy_path).is_ok_and(|current| current == bytes) {
            return Ok(());
        }
        atomic_bytes(&self.legacy_path, &bytes).with_context(|| {
            format!(
                "writing v1 delivery rollback floor {}",
                self.legacy_path.display()
            )
        })
    }

    fn persist(&self) -> Result<()> {
        atomic_json(&self.path, &self.record)
            .with_context(|| format!("writing delivery ledger {}", self.path.display()))
    }
}

/// Codex's v1-shaped rollback floor. The key set is exact: v1's `CodexDeliveryState` denies
/// unknown fields, so an extra or missing key makes the old binary refuse to start.
pub fn codex_floor(
    agent: &str,
    runtime_id: &str,
    runtime_incarnation: &str,
    thread_id: &str,
    filename: &str,
    client_id: &str,
) -> Value {
    json!({
        "schema": CODEX_LEGACY_SCHEMA,
        "agent": agent,
        "runtimeId": runtime_id,
        "runtimeIncarnation": runtime_incarnation,
        "threadId": thread_id,
        "filename": filename,
        "clientId": client_id,
        "phase": "attempted",
    })
}

/// OpenCode's v1-shaped rollback floor. v1 recomputes `stable_message_id(identity, session,
/// filename)` and silently discards a record that does not match, so `message_id` must be the
/// exact derived value or the floor buys nothing.
pub fn opencode_floor(
    agent: &str,
    runtime_id: &str,
    session_id: &str,
    filename: &str,
    message_id: &str,
) -> Value {
    json!({
        "schema": OPENCODE_LEGACY_SCHEMA,
        "agent": agent,
        "runtimeId": runtime_id,
        "sessionId": session_id,
        "filename": filename,
        "messageId": message_id,
        "phase": "attempted",
    })
}

/// Durability, not just atomicity: a crash between the rename and the harness's acceptance of the
/// transport would otherwise lose the receipt and let the pump re-send duplicate content. The
/// bytes reach disk before the rename and the directory entry afterwards.
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic_bytes(path, &serde_json::to_vec(value)?)
}

fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("ledger file has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".{}.tmp", std::process::id()));
    let mut file = fs::File::create(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error.into());
    }
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn remove_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent()
                && let Ok(dir) = fs::File::open(parent)
            {
                let _ = dir.sync_all();
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE_A: &str = "1786380000000-aaa111.md";
    const FILE_B: &str = "1786380000001-bbb222.md";

    /// The one derivation both the transport and the ledger use, so a record that does not match
    /// its own binding is provably not ours.
    fn correlate(binding: &str, filename: &str) -> String {
        format!("corr:{binding}:{filename}")
    }

    fn legacy_path(dir: &Path) -> PathBuf {
        dir.join("state").join(LEGACY_FILE)
    }

    fn open(dir: &Path, harness: Harness) -> Ledger {
        Ledger::open(
            &legacy_path(dir),
            harness.profile(),
            "h.worker",
            "h.worker",
            correlate,
        )
    }

    fn begin(ledger: &mut Ledger, harness: Harness, binding: &str, filename: &str) -> Entry {
        let value = correlate(binding, filename);
        let floor = match harness {
            Harness::Codex => codex_floor(
                "h.worker",
                "h.worker",
                "incarnation-1",
                binding,
                filename,
                &value,
            ),
            Harness::OpenCode => opencode_floor("h.worker", "h.worker", binding, filename, &value),
        };
        ledger
            .begin(Begin {
                filename: filename.to_string(),
                binding: binding.to_string(),
                correlation: Correlation::native(value),
                incarnation: match harness {
                    Harness::Codex => Some("incarnation-1".to_string()),
                    Harness::OpenCode => None,
                },
                legacy_floor: floor,
            })
            .unwrap()
    }

    #[test]
    fn phase_is_monotonic_and_durable_across_every_write_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        begin(&mut ledger, Harness::Codex, "thread-main", FILE_A);

        // Every boundary is re-readable, and re-reading never loses a phase.
        for evidence in [Evidence::TransportAccepted, Evidence::Consumed] {
            ledger.record(FILE_A, evidence).unwrap();
            let reopened = open(tmp.path(), Harness::Codex);
            assert_eq!(
                reopened.entry(FILE_A).unwrap().phase,
                ledger.entry(FILE_A).unwrap().phase,
                "the phase on disk is the phase in memory"
            );
        }
        assert_eq!(ledger.entry(FILE_A).unwrap().phase, Phase::Consumed);

        // A lower phase is refused, not written.
        assert_eq!(
            ledger.record(FILE_A, Evidence::TransportAccepted).unwrap(),
            Some(Phase::Consumed),
            "a late lower reading never regresses the record"
        );
        assert_eq!(
            open(tmp.path(), Harness::Codex).entry(FILE_A).unwrap().phase,
            Phase::Consumed
        );
    }

    #[test]
    fn v1_accepted_adopts_at_the_evidence_it_actually_proved() {
        // Codex wrote `Accepted` only from the typed completed user message: consumption.
        let codex = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(codex.path()),
            &json!({
                "schema": CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "runtimeIncarnation": "incarnation-0",
                "threadId": "thread-main",
                "filename": FILE_A,
                "clientId": correlate("thread-main", FILE_A),
                "phase": "accepted",
            }),
        )
        .unwrap();
        let ledger = open(codex.path(), Harness::Codex);
        assert_eq!(ledger.quarantined(), None);
        let entry = ledger.entry(FILE_A).unwrap();
        assert_eq!(entry.phase, Phase::Consumed);
        assert_eq!(entry.adopted_from.as_deref(), Some(CODEX_LEGACY_SCHEMA));
        assert_eq!(
            ledger.retention(FILE_A),
            Retention::Release,
            "a Codex acceptance is a true ceiling"
        );

        // OpenCode wrote `Accepted` on a GET 200: storage, never scheduling.
        let opencode = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(opencode.path()),
            &json!({
                "schema": OPENCODE_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "sessionId": "ses_target",
                "filename": FILE_A,
                "messageId": correlate("ses_target", FILE_A),
                "phase": "accepted",
            }),
        )
        .unwrap();
        let ledger = open(opencode.path(), Harness::OpenCode);
        let entry = ledger.entry(FILE_A).unwrap();
        assert_eq!(entry.phase, Phase::Persisted);
        assert_eq!(
            ledger.retention(FILE_A),
            Retention::Hold(HoldReason::UnreadReceipt),
            "persistence is not consumption and never releases ownership"
        );
    }

    #[test]
    fn a_runtime_id_change_rebinds_the_record_and_holds_its_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let ledger_file = legacy_path(tmp.path()).with_file_name(LEDGER_FILE);
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);

        // The seat is relaunched under a new runtime id. The agent — the durable owner — is
        // unchanged, so this is the same recipient still holding the same outstanding delivery.
        // Quarantining or discarding here is exactly how a held ambiguous delivery becomes a
        // duplicate.
        let mut rebound = Ledger::open(
            &legacy_path(tmp.path()),
            Harness::OpenCode.profile(),
            "h.worker",
            "h.worker.relaunched",
            correlate,
        );
        assert_eq!(rebound.quarantined(), None, "drift is not corruption");
        let entry = rebound
            .entry(FILE_A)
            .expect("the evidence is preserved, never discarded");
        assert_eq!(entry.phase, Phase::Attempted);
        assert_eq!(
            rebound.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AmbiguousAttempt),
            "a rebind is bookkeeping, never evidence: it authorizes no resend of its own"
        );

        // The rebound id is provenance, not a fence: it needs no write of its own and rides along
        // on the next durable write.
        rebound.record(FILE_A, Evidence::Persisted).unwrap();
        let on_disk: Value = serde_json::from_slice(&fs::read(&ledger_file).unwrap()).unwrap();
        assert_eq!(on_disk["runtimeId"], "h.worker.relaunched");

        // A different AGENT is a different recipient, and that still fails closed.
        let foreign = Ledger::open(
            &legacy_path(tmp.path()),
            Harness::OpenCode.profile(),
            "h.other",
            "h.worker",
            correlate,
        );
        assert!(
            foreign
                .quarantined()
                .is_some_and(|reason| reason.contains("different agent"))
        );
        assert!(foreign.entries().is_empty());
    }

    #[test]
    fn a_runtime_drifted_v1_record_is_adopted_so_reconciliation_can_settle_it_before_send() {
        // OpenCode: v1's filter never compared `runtimeId` — schema, agent, the filename grammar
        // and the recomputed messageID were the whole test — so adopting on a stricter rule would
        // drop a record the old binary WOULD have acted on, which is the duplicate-POST class
        // returning.
        let opencode = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(opencode.path()),
            &json!({
                "schema": OPENCODE_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker.previous",
                "sessionId": "ses_target",
                "filename": FILE_A,
                "messageId": correlate("ses_target", FILE_A),
                "phase": "attempted",
            }),
        )
        .unwrap();
        let ledger = open(opencode.path(), Harness::OpenCode);
        assert_eq!(ledger.quarantined(), None);
        assert!(
            ledger.entry(FILE_A).is_some(),
            "OpenCode's v1 loader ignored runtimeId, so adoption must too"
        );

        // Codex: v1 DID compare it, and hard-errored. But the record still describes a real
        // attempt this recipient made, so it is carried forward at the phase it proved and held —
        // ignoring it would open a second delivery for the same message, while holding it lets
        // the resume sweep settle or refuse it before anything is sent.
        let codex = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(codex.path()),
            &json!({
                "schema": CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker.previous",
                "runtimeIncarnation": "incarnation-0",
                "threadId": "thread-main",
                "filename": FILE_A,
                "clientId": correlate("thread-main", FILE_A),
                "phase": "accepted",
            }),
        )
        .unwrap();
        let mut ledger = open(codex.path(), Harness::Codex);
        assert_eq!(ledger.quarantined(), None);
        let entry = ledger
            .entry(FILE_A)
            .expect("a drifted record is adopted, never ignored");
        assert_eq!(
            entry.phase,
            Phase::Consumed,
            "adopted at the phase its label actually proved"
        );
        assert!(!entry.retry_eligible);
        assert_eq!(entry.adopted_from.as_deref(), Some(CODEX_LEGACY_SCHEMA));
        assert_eq!(entry.incarnation.as_deref(), Some("incarnation-0"));

        // An ambiguous drifted attempt is held for the sweep, and only its verdict moves it.
        let attempted = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(attempted.path()),
            &json!({
                "schema": CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker.previous",
                "runtimeIncarnation": "incarnation-0",
                "threadId": "thread-main",
                "filename": FILE_A,
                "clientId": correlate("thread-main", FILE_A),
                "phase": "attempted",
            }),
        )
        .unwrap();
        ledger = open(attempted.path(), Harness::Codex);
        assert_eq!(
            ledger.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence),
            "no send before the sweep"
        );
        ledger.negative(FILE_A, NegativeReceipt::Absent).unwrap();
        assert_eq!(
            ledger.retry(FILE_A),
            RetryDecision::Retry,
            "a resumed history proving absence is what authorizes the resend"
        );
    }

    #[test]
    fn a_rebind_rewrites_only_the_runtime_id_on_every_retained_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = legacy_path(tmp.path());
        let mut ledger = open(tmp.path(), Harness::Codex);
        begin(&mut ledger, Harness::Codex, "thread-main", FILE_A);
        let before: Value = serde_json::from_slice(&fs::read(&legacy).unwrap()).unwrap();
        assert_eq!(before["runtimeId"], "h.worker");

        // The seat is relaunched under a new runtime id. A rolled-back Codex loader compares
        // `runtimeId`, so a floor still naming the previous runtime would be refused and the
        // rollback would have no lower bound at all.
        let mut rebound = Ledger::open(
            &legacy_path(tmp.path()),
            Harness::Codex.profile(),
            "h.worker",
            "h.worker.relaunched",
            correlate,
        );
        rebound.reassert_floor().unwrap();
        let after: Value = serde_json::from_slice(&fs::read(&legacy).unwrap()).unwrap();
        assert_eq!(after["runtimeId"], "h.worker.relaunched");

        // Only that one mutable address moved. Everything identifying the attempt is untouched,
        // including the client ID v1 revalidates against the thread and filename it still carries,
        // and the never-advanced phase.
        for key in [
            "schema",
            "agent",
            "runtimeIncarnation",
            "threadId",
            "filename",
            "clientId",
            "phase",
        ] {
            assert_eq!(after[key], before[key], "{key} must not change");
        }
        assert_eq!(after["phase"], "attempted");
        assert_eq!(
            after["clientId"].as_str().unwrap(),
            correlate("thread-main", FILE_A),
            "the floor still passes v1's own client-ID recomputation"
        );
        assert_eq!(
            after.as_object().unwrap().len(),
            before.as_object().unwrap().len(),
            "no key is added or removed: v1 denies unknown fields"
        );

        // The ledger's own entry is unchanged too — a rebind is bookkeeping, not evidence.
        let entry = rebound.entry(FILE_A).unwrap();
        assert_eq!(entry.phase, Phase::Attempted);
        assert_eq!(entry.incarnation.as_deref(), Some("incarnation-1"));
        assert_eq!(entry.correlation.value, correlate("thread-main", FILE_A));
    }

    #[test]
    fn the_v1_path_holds_the_oldest_outstanding_floor_at_every_crash_position() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = legacy_path(tmp.path());
        let mut ledger = open(tmp.path(), Harness::OpenCode);

        // Two distinct filenames with two distinct floors, under one binding. The newer one is
        // begun FIRST on purpose: the answer must not depend on insertion order.
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_B);
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&legacy).unwrap()).unwrap()["filename"],
            FILE_B
        );
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);
        assert_eq!(ledger.entries().len(), 2);

        // v1 holds ONE record, so it must name the OLDEST outstanding attempt — a rolled-back
        // binary reconciles the earliest delivery, and the later one is still held by the ledger.
        let oldest = |legacy: &Path| {
            serde_json::from_slice::<Value>(&fs::read(legacy).unwrap()).unwrap()["filename"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(oldest(&legacy), FILE_A);

        // Crash position: restart. The floor still names the oldest and both entries stay held.
        let mut restarted = open(tmp.path(), Harness::OpenCode);
        fs::remove_file(&legacy).unwrap();
        restarted.reassert_floor().unwrap();
        assert_eq!(oldest(&legacy), FILE_A);
        for filename in [FILE_A, FILE_B] {
            assert_eq!(
                restarted.retry(filename),
                RetryDecision::Hold(HoldReason::AmbiguousAttempt),
                "{filename} is held, not replayed"
            );
        }

        // The recipient archives the oldest: the floor advances to the next outstanding attempt
        // and never regresses onto a released one.
        restarted.prune(|filename| filename == FILE_B).unwrap();
        assert_eq!(oldest(&legacy), FILE_B);

        // And when the last outstanding entry goes, so does the floor.
        restarted.prune(|_| false).unwrap();
        assert!(!legacy.exists());
    }

    #[test]
    fn adoption_alone_never_authorizes_a_transport() {
        let tmp = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(tmp.path()),
            &json!({
                "schema": OPENCODE_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "sessionId": "ses_target",
                "filename": FILE_A,
                "messageId": correlate("ses_target", FILE_A),
                "phase": "attempted",
            }),
        )
        .unwrap();
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        let entry = ledger.entry(FILE_A).unwrap();
        assert_eq!(entry.phase, Phase::Attempted);
        assert!(!entry.retry_eligible);
        assert_eq!(
            ledger.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence)
        );
        assert_eq!(
            ledger.retention(FILE_A),
            Retention::Hold(HoldReason::AdoptedWithoutFreshEvidence),
            "an ambiguous carried-forward attempt is held and surfaced, never replayed"
        );

        // Only fresh evidence about the world moves it: an authoritative absence.
        ledger.negative(FILE_A, NegativeReceipt::Absent).unwrap();
        assert_eq!(ledger.retry(FILE_A), RetryDecision::Retry);
    }

    #[test]
    fn adoption_happens_exactly_once_and_never_re_reads_a_stale_v1_record() {
        let tmp = tempfile::tempdir().unwrap();
        let floor = json!({
            "schema": CODEX_LEGACY_SCHEMA,
            "agent": "h.worker",
            "runtimeId": "h.worker",
            "runtimeIncarnation": "incarnation-0",
            "threadId": "thread-main",
            "filename": FILE_A,
            "clientId": correlate("thread-main", FILE_A),
            "phase": "attempted",
        });
        atomic_json(&legacy_path(tmp.path()), &floor).unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        assert_eq!(ledger.entry(FILE_A).unwrap().phase, Phase::Attempted);

        // The delivery settles and the ledger releases it, clearing the floor.
        ledger.record(FILE_A, Evidence::Consumed).unwrap();
        assert!(
            !legacy_path(tmp.path()).exists(),
            "release clears the v1 floor"
        );

        // A stale v1 record reappearing (an old binary, a restored backup) is not re-adopted: the
        // ledger file already exists, so adoption is spent.
        atomic_json(&legacy_path(tmp.path()), &floor).unwrap();
        let reopened = open(tmp.path(), Harness::Codex);
        assert_eq!(reopened.entry(FILE_A).unwrap().phase, Phase::Consumed);
    }

    #[test]
    fn a_negative_receipt_is_retained_and_re_offered() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);
        assert_eq!(
            ledger.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AmbiguousAttempt),
            "an ambiguous attempt on a non-idempotent transport is held"
        );

        ledger.negative(FILE_A, NegativeReceipt::Absent).unwrap();
        // The receipt survives the process that observed it.
        let reopened = open(tmp.path(), Harness::OpenCode);
        assert_eq!(
            reopened.entry(FILE_A).unwrap().negative,
            Some(NegativeReceipt::Absent)
        );
        assert_eq!(reopened.retry(FILE_A), RetryDecision::Retry);
        assert_eq!(
            reopened.retention(FILE_A),
            Retention::Hold(HoldReason::NegativeReceipt),
            "a refused delivery is retained, never released"
        );

        // A settled delivery cannot be un-settled by a late "no".
        let mut settled = open(tmp.path(), Harness::OpenCode);
        settled.record(FILE_A, Evidence::Admitted).unwrap();
        assert_eq!(
            settled.negative(FILE_A, NegativeReceipt::Rejected).unwrap(),
            Retention::Release
        );
        assert_eq!(settled.entry(FILE_A).unwrap().phase, Phase::Admitted);
        assert_eq!(settled.entry(FILE_A).unwrap().negative, None);
    }

    #[test]
    fn release_requires_the_harness_scheduler_or_a_true_ceiling() {
        let tmp = tempfile::tempdir().unwrap();
        let mut opencode = open(tmp.path(), Harness::OpenCode);
        begin(&mut opencode, Harness::OpenCode, "ses_target", FILE_A);
        for (evidence, expected) in [
            (
                Evidence::TransportAccepted,
                Retention::Hold(HoldReason::AmbiguousAttempt),
            ),
            (
                Evidence::Persisted,
                Retention::Hold(HoldReason::UnreadReceipt),
            ),
            (Evidence::Admitted, Retention::Release),
        ] {
            opencode.record(FILE_A, evidence).unwrap();
            assert_eq!(opencode.retention(FILE_A), expected, "{evidence:?}");
        }

        // Codex's ceiling is the harness's own ceiling, so reaching it settles.
        let codex_dir = tempfile::tempdir().unwrap();
        let mut codex = open(codex_dir.path(), Harness::Codex);
        begin(&mut codex, Harness::Codex, "thread-main", FILE_A);
        codex.record(FILE_A, Evidence::TransportAccepted).unwrap();
        assert_eq!(
            codex.retention(FILE_A),
            Retention::Hold(HoldReason::AmbiguousAttempt),
            "a transport result is not a receipt"
        );
        codex.record(FILE_A, Evidence::Consumed).unwrap();
        assert_eq!(codex.retention(FILE_A), Retention::Release);
    }

    #[test]
    fn unknown_and_dishonest_adapter_evidence_fails_closed() {
        assert!(Harness::parse("gemini").is_err());

        let tmp = tempfile::tempdir().unwrap();
        let mut opencode = open(tmp.path(), Harness::OpenCode);
        begin(&mut opencode, Harness::OpenCode, "ses_target", FILE_A);
        let error = opencode.record(FILE_A, Evidence::Consumed).unwrap_err();
        assert!(
            error.to_string().contains("cannot prove"),
            "OpenCode has no consumption signal: {error:#}"
        );
        assert_eq!(opencode.entry(FILE_A).unwrap().phase, Phase::Attempted);

        let codex_dir = tempfile::tempdir().unwrap();
        let mut codex = open(codex_dir.path(), Harness::Codex);
        begin(&mut codex, Harness::Codex, "thread-main", FILE_A);
        assert!(codex.record(FILE_A, Evidence::Persisted).is_err());
        assert!(codex.record(FILE_A, Evidence::Admitted).is_err());
    }

    #[test]
    fn an_unreadable_ledger_quarantines_the_pump_instead_of_refusing_to_start() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        begin(&mut ledger, Harness::Codex, "thread-main", FILE_A);

        // A foreign schema at the ledger path: exactly the shape an in-place bump would have left.
        atomic_json(
            &legacy_path(tmp.path()).with_file_name(LEDGER_FILE),
            &json!({
                "schema": "st2.delivery-ledger.v2",
                "harness": "codex",
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "entries": [],
            }),
        )
        .unwrap();
        let mut quarantined = open(tmp.path(), Harness::Codex);
        assert!(
            quarantined
                .quarantined()
                .is_some_and(|reason| reason.contains("unsupported schema")),
            "the reason is retained"
        );
        assert!(quarantined.entries().is_empty());
        assert_eq!(
            quarantined.retention(FILE_A),
            Retention::Hold(HoldReason::Quarantined)
        );
        assert_eq!(
            quarantined.retry(FILE_A),
            RetryDecision::Hold(HoldReason::Quarantined)
        );
        let error = quarantined
            .begin(Begin {
                filename: FILE_A.to_string(),
                binding: "thread-main".to_string(),
                correlation: Correlation::native(correlate("thread-main", FILE_A)),
                incarnation: None,
                legacy_floor: json!({}),
            })
            .unwrap_err();
        assert!(error.to_string().contains("quarantined"));
    }

    #[test]
    fn a_tampered_correlation_fails_closed_without_deleting_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        atomic_json(
            &legacy_path(tmp.path()),
            &json!({
                "schema": CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "runtimeIncarnation": "incarnation-0",
                "threadId": "thread-main",
                "filename": FILE_A,
                "clientId": "st2:tampered",
                "phase": "attempted",
            }),
        )
        .unwrap();
        let ledger = open(tmp.path(), Harness::Codex);
        assert!(
            ledger
                .quarantined()
                .is_some_and(|reason| reason.contains("does not match its binding"))
        );
        assert!(
            legacy_path(tmp.path()).exists(),
            "a record we refuse to read is not a record we may destroy"
        );
    }

    #[test]
    fn a_v1_readable_floor_precedes_the_first_transport_and_clears_only_on_release() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = legacy_path(tmp.path());
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        assert!(!legacy.exists());

        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);
        let floor: Value = serde_json::from_slice(&fs::read(&legacy).unwrap()).unwrap();
        assert_eq!(floor["schema"], OPENCODE_LEGACY_SCHEMA);
        assert_eq!(floor["phase"], "attempted");
        assert_eq!(floor["messageId"], correlate("ses_target", FILE_A));

        // The floor never advances, so it can never contradict the ledger and no old binary can
        // read it as acceptance.
        ledger.record(FILE_A, Evidence::Persisted).unwrap();
        let held: Value = serde_json::from_slice(&fs::read(&legacy).unwrap()).unwrap();
        assert_eq!(held["phase"], "attempted");
        assert_eq!(held, floor);

        // Re-asserted while outstanding: a crash exactly at the floor write must not leave a
        // landing without a v1-readable lower bound.
        fs::remove_file(&legacy).unwrap();
        ledger.reassert_floor().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&legacy).unwrap()).unwrap(),
            floor
        );

        // Cleared only at release.
        ledger.record(FILE_A, Evidence::Admitted).unwrap();
        assert!(!legacy.exists());
        ledger.reassert_floor().unwrap();
        assert!(!legacy.exists(), "a released entry re-asserts nothing");
    }

    #[test]
    fn a_bounded_multi_message_prefix_is_n_entries_under_one_correlation() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), Harness::Codex);
        // One transport, two messages: the shape a bounded FIFO prefix needs.
        let value = correlate("thread-main", FILE_A);
        for filename in [FILE_A, FILE_B] {
            ledger
                .begin(Begin {
                    filename: filename.to_string(),
                    binding: "thread-main".to_string(),
                    correlation: Correlation::native(value.clone()),
                    incarnation: Some("incarnation-1".to_string()),
                    legacy_floor: codex_floor(
                        "h.worker",
                        "h.worker",
                        "incarnation-1",
                        "thread-main",
                        FILE_A,
                        &value,
                    ),
                })
                .unwrap();
        }
        assert_eq!(ledger.correlated(&value), vec![FILE_A, FILE_B]);

        // And the batch is loadable: a shared correlation is anchored by the head it was sent
        // under, so a restart mid-batch reads both entries back instead of quarantining.
        let reopened = open(tmp.path(), Harness::Codex);
        assert_eq!(reopened.quarantined(), None);
        assert_eq!(reopened.correlated(&value), vec![FILE_A, FILE_B]);

        // One receipt settles every filename it carried, each on its own monotone entry.
        for filename in ledger.correlated(&value) {
            ledger.record(&filename, Evidence::Consumed).unwrap();
        }
        assert!(
            ledger
                .entries()
                .iter()
                .all(|entry| entry.phase == Phase::Consumed)
        );
        assert_eq!(ledger.retention(FILE_B), Retention::Release);

        // The recipient archiving one of them releases only that one.
        ledger.prune(|filename| filename == FILE_B).unwrap();
        assert_eq!(ledger.entries().len(), 1);
        assert_eq!(ledger.entries()[0].filename, FILE_B);
    }

    #[test]
    fn released_and_archived_entries_leave_no_residue() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = legacy_path(tmp.path());
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);
        assert!(legacy.exists());

        // Archive precedence — the recipient's act — releases ownership even mid-attempt.
        ledger.prune(|_| false).unwrap();
        assert!(ledger.entries().is_empty());
        assert!(!legacy.exists(), "no outstanding entry, no floor");

        // And a rebind drops the other binding's entry with its floor.
        begin(&mut ledger, Harness::OpenCode, "ses_old", FILE_A);
        ledger.rebind("ses_new").unwrap();
        assert!(ledger.entries().is_empty());
        assert!(!legacy.exists());
        assert_eq!(ledger.binding(), None);

        // The ledger file itself is retained even when empty: its existence is what makes v1
        // adoption one-shot.
        let reopened = open(tmp.path(), Harness::OpenCode);
        assert!(reopened.entries().is_empty());
        assert_eq!(reopened.quarantined(), None);
    }

    #[test]
    fn a_crash_at_any_persistence_boundary_never_duplicates_a_held_delivery() {
        // The boundaries a restart can land between, in order: floor write, ledger attempted,
        // transport, evidence write. At every one, a replacement process must hold rather than
        // re-transport, because nothing it can read proves the harness did not get the message.
        let tmp = tempfile::tempdir().unwrap();
        let legacy = legacy_path(tmp.path());

        // Crash after the floor write, before the ledger's own attempted record.
        let value = correlate("ses_target", FILE_A);
        atomic_json(
            &legacy,
            &opencode_floor("h.worker", "h.worker", "ses_target", FILE_A, &value),
        )
        .unwrap();
        let recovered = open(tmp.path(), Harness::OpenCode);
        assert_eq!(
            recovered.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence),
            "the floor is adopted as the ambiguous attempt it is"
        );

        // Crash after the ledger's attempted record, before or during transport.
        let mut ledger = open(tmp.path(), Harness::OpenCode);
        begin(&mut ledger, Harness::OpenCode, "ses_target", FILE_A);
        let recovered = open(tmp.path(), Harness::OpenCode);
        assert_eq!(
            recovered.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AmbiguousAttempt)
        );
        assert!(legacy.exists(), "the rollback floor survives the crash");

        // Crash after persistence was proved: still held, still never re-sent.
        ledger.record(FILE_A, Evidence::Persisted).unwrap();
        let recovered = open(tmp.path(), Harness::OpenCode);
        assert_eq!(recovered.entry(FILE_A).unwrap().phase, Phase::Persisted);
        assert_eq!(
            recovered.retry(FILE_A),
            RetryDecision::Hold(HoldReason::UnreadReceipt)
        );
        assert_eq!(
            recovered.retention(FILE_A),
            Retention::Hold(HoldReason::UnreadReceipt)
        );
    }
}
