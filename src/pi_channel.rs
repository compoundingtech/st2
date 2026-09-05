//! Minimal pi channel watcher.
//!
//! The inbox is the durable source of truth. This process keeps only an ephemeral set of filenames
//! delivered during its current lifetime; a restart scans the inbox again. It is spawned by the
//! shipped pi extension and owned by it over stdio, so EOF on stdin is the session-lifetime
//! boundary. The outer pi session wrapper owns presence, because pi can outlive a failed extension.
//!
//! The wire is newline-delimited JSON in both directions. st2 — not the extension — decides how a
//! delivered message is handed to the agent, so the delivery mode travels on the frame: changing
//! that policy is a Rust change, not a redeploy of a TypeScript asset.

use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::{context, driver_diagnostic, harness_context, harness_state, message};

const POLL: Duration = Duration::from_millis(250);

/// How old durable working state may be and still be restored, matching the lifecycle hooks'
/// `ST_REHYDRATE_STALE_S` default. Stale state is worse than none: it describes a world the agent
/// has already left.
const CONTEXT_MAX_AGE: Duration = Duration::from_secs(86_400);

/// The boot instruction every maintained harness restores verbatim. It is the shipped bus contract:
/// declare presence, then drain the inbox.
const RITUAL: &str = "Run the st2 boot ritual now: set your status to available, then drain your \
inbox by reading, acting on, replying when useful, and archiving each handled message. Before \
resuming or starting work, set your status to busy; set available only when yielding or ready for \
new work.";

/// The wire version the shipped extension is written against. A mismatch is the extension's to
/// refuse: st2 never guesses what an older asset understands.
pub const PROTOCOL: u32 = 1;

/// Every protocol version st2 accepts on a channel connection, offered on the hello BESIDE
/// `protocol`, which never rises. Additive by construction: an asset that does not know the field
/// ignores it, and an asset that does answers with a `client_hello` naming what it will speak.
pub const PROTOCOLS: [u32; 2] = [1, 2];

/// Last-resort durable state when compaction begins before the agent authored a checkpoint.
///
/// The stable text deliberately carries no extension-owned path or clock. Rust owns both the
/// canonical context path and its atomic writer; the file mtime supplies freshness.
const PRE_COMPACT_STUB: &str = "# now — pre-compact stub\n\n\
PreCompact fired before the model captured durable working state. Reconstruct from git status,\n\
recent commits, and the st2 inbox, then write a real checkpoint with `st2 context write`.\n";
const PRE_COMPACT_ERROR_REASON: &str = "pre-compact context recovery failed";

/// The negotiated asset's diagnostic word for a refused approval. It is prose about an ASK that
/// is over — never a condition — and it exists only in the negotiated vocabulary, so the version
/// 2 projection withholds it: a record shape readers are pinned to must not grow a novel `reason`
/// on an unblocked frame because a newer asset started narrating one.
const APPROVAL_DENIED_REASON: &str = "approvalDenied";

/// How pi is asked to hand one delivered message to the agent.
///
/// `steer` is the only value st2 currently emits. It is the earliest point at which pi accepts
/// input without discarding the running turn, and the same choice the Codex native path makes when
/// it routes an active turn to a steer. Holding the message until the agent settles instead would
/// defer delivery inside pi, where st2 cannot see it, which is what the "`busy` delivers
/// immediately" rule exists to prevent. Per-message selection is tracked in #277.
///
/// The boundary is finer than "after the current turn's tool calls" suggests. Measured against a
/// live provider: each tool call and its result form their own assistant message, so a steer sent
/// during a four-step job landed on the *first* tool-result boundary — in the same millisecond as
/// that result, after waiting out only the remainder of the one in-flight call. Steer latency is
/// therefore bounded by a single tool call's duration, not by the length of the job. What is not
/// guaranteed is that displaced work resumes: the model chose to continue, once, on one model.
const DELIVER_AS: &str = "steer";

fn channel_content(subject: Option<&str>, body: &str) -> String {
    match subject.filter(|value| !value.is_empty()) {
        Some(subject) => format!("Subject: {subject}\n\n{body}"),
        None => body.to_owned(),
    }
}

/// st2's hello: the version the asset must understand, and every version st2 would also accept.
///
/// `protocol` stays 1 forever. The hello is st2 → asset and is written before any read, so a
/// control plane that raised it unilaterally would be REFUSED by every already-loaded asset —
/// and a refusal costs that seat its mail. The offer beside it is how a newer wire is reached
/// instead: additive, ignored by an old asset, answered by a new one.
fn hello(identity: &str, session_context: &str) -> Value {
    json!({
        "type": "hello",
        "protocol": PROTOCOL,
        "protocols": PROTOCOLS,
        "identity": identity,
        "sessionContext": session_context,
    })
}

/// The harness-specific facts the shared channel loop needs: which env names carry the wrapper's
/// exported ownership triple, what label goes on records and errors, and which native-driver
/// diagnostic word — if any — this channel publishes under.
pub struct ChannelKind {
    pub label: &'static str,
    /// Which producer row of the harness-context table these numbers come from. It is the record's
    /// only discriminator, and the two kinds genuinely differ: pi's `tokens` is the last assistant
    /// message's `totalTokens`, omp's is its prompt-only `input`. A reader that knows the harness
    /// knows which arithmetic made the number.
    pub harness: harness_context::Harness,
    /// The `driver-diagnostic` driver word, present only where the shipped extension emits a typed
    /// turn result to classify. pi's does not: it has no error-classification field to forward, so
    /// this channel would have nothing but provider prose to key on and refuses to guess from it.
    pub diagnostic_driver: Option<driver_diagnostic::Driver>,
    pub runtime_id_env: &'static str,
    pub session_env: &'static str,
    pub seq_env: &'static str,
    /// What this kind's asset can say about the ask axis when it is NOT blocked on a human. omp
    /// owns both ask surfaces — the structured `ask` tool and the approval pair — so `none` is a
    /// positive observation there. pi has no ask channel at all, and `Unknown` states exactly
    /// that: `none` would fabricate absence and `Pending(Unknown)` would fabricate a waiting
    /// human.
    pub default_ask: harness_state::HumanAsk,
    /// The conversation axis this kind can state with no evidence off the wire. pi has no
    /// conversation identity to expose at all; omp demonstrably has sessions (`--no-session`,
    /// `sessionManager`), so it states NOTHING until an event exposes one — never `Unsupported`,
    /// which would be a false capability claim.
    pub conversation: Option<harness_state::ConversationState>,
}

const PI_KIND: ChannelKind = ChannelKind {
    label: "pi",
    harness: harness_context::Harness::Pi,
    diagnostic_driver: None,
    runtime_id_env: crate::pi_session::CHANNEL_RUNTIME_ID,
    session_env: crate::pi_session::CHANNEL_SESSION,
    seq_env: crate::pi_session::CHANNEL_SEQ,
    default_ask: harness_state::HumanAsk::Unknown,
    conversation: Some(harness_state::ConversationState::Unsupported),
};

const OMP_KIND: ChannelKind = ChannelKind {
    label: "omp",
    harness: harness_context::Harness::Omp,
    diagnostic_driver: Some(driver_diagnostic::Driver::Omp),
    runtime_id_env: crate::omp_session::CHANNEL_RUNTIME_ID,
    session_env: crate::omp_session::CHANNEL_SESSION,
    seq_env: crate::omp_session::CHANNEL_SEQ,
    default_ask: harness_state::HumanAsk::None,
    conversation: None,
};

/// Run the pi native message channel over stdio.
pub fn run(catalog_root: &Path, identity: &str) -> Result<()> {
    run_for(catalog_root, identity, &PI_KIND)
}

/// Run the omp native message channel over stdio (the omp extension's child).
pub fn run_omp(catalog_root: &Path, identity: &str) -> Result<()> {
    run_for(catalog_root, identity, &OMP_KIND)
}

fn run_for(catalog_root: &Path, identity: &str, kind: &ChannelKind) -> Result<()> {
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("{} channel agent '{identity}' is not declared", kind.label))?;
    let inbox = message::inbox_dir(&agent_dir);
    // Composed here rather than in the extension: what a restarted agent is told is st2's contract,
    // not the asset's, and the Codex and Claude hooks compose the same three blocks in bash.
    let session_context = session_context(&agent_dir, identity);
    let (input_tx, input_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            if input_tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    write_json(&mut stdout, &hello(identity, &session_context))?;
    stdout.flush()?;
    // The channel owns the live half of observed harness state: it is the one process that sees
    // the harness's own turn events, and its stdio connection to the extension is the evidence
    // that those events are still being watched. The terminal half belongs to the outer session
    // wrapper, which alone sees the provider die.
    // The pty session vouching for the record is the wrapper's task: its runtime ID arrives in
    // the channel environment, and only aliases the identity on driver-expanded seats.
    let pty_session = std::env::var(kind.runtime_id_env)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| identity.to_string());
    // The wrapper mints the session token; adopting it makes the wrapper's terminal record own
    // this channel's live records (so a queued frame after `ended` is suppressed) while a
    // predecessor incarnation's records are foreign: the first frame opens a fresh transition
    // and a predecessor's terminal record never silences this session.
    let wrapper_session = std::env::var(kind.session_env)
        .ok()
        .filter(|value| !value.is_empty());
    // The context record carries the same incarnation as the state record beside it, so a reader
    // can tell "this number came from the session currently running" from "this number predates
    // it". On this record the token is provenance only: nothing is fenced on it, a straggler's
    // write lands, and the next real reading overwrites it (HC-T04). Falling back to this
    // process's own token when the wrapper exported none keeps the field populated rather than
    // claiming a session it cannot name.
    let context_session = wrapper_session
        .clone()
        .unwrap_or_else(harness_state::session_token);
    let mut writer =
        harness_state::Writer::new(&agent_dir, identity, kind.label, Some(pty_session));
    if let Some(session) = wrapper_session {
        // Full adopted ownership when the wrapper exported it: the claimed sequence gives the
        // token a direction, so a straggler channel from a superseded session is refused.
        writer = match std::env::var(kind.seq_env)
            .ok()
            .and_then(|seq| seq.parse::<u64>().ok())
        {
            Some(seq) => writer.with_ownership(session, seq),
            None => writer.with_session(session),
        };
    }
    writer.interrupt();
    // The numeric axis is a sibling record with its own writer, deliberately sharing nothing with
    // the categorical one but the incarnation token: folding a token count into `Observation`
    // would make `sinceMs` reset on every turn whose numbers moved ("idle for 40 minutes" becomes
    // unrecoverable) and turn `transitions` into a turn counter.
    //
    // Failing to construct it must not cost the seat its mail. Delivery never depends on
    // observability anywhere else in this loop, and this is the one fallible construction here —
    // an agent directory with no parent has nowhere safe to stage a temporary file.
    let mut context_writer = match harness_context::Writer::new(&agent_dir, identity, kind.harness)
    {
        Ok(writer) => Some(writer.with_session(context_session)),
        Err(error) => {
            tracing::warn!(
                "st2 {} channel: harness context is unavailable: {error}",
                kind.label
            );
            None
        }
    };
    channel_loop(
        &input_rx,
        &mut stdout,
        &inbox,
        &agent_dir,
        &mut writer,
        context_writer.as_mut(),
        identity,
        kind,
        POLL,
        harness_state::HARNESS_STATE_REFRESH,
    )
}

/// The channel's steady state: forward inbox entries out, fold extension frames in, and keep the
/// observed-state heartbeat exactly as fresh as the stdio connection that justifies it. EOF is the
/// session-lifetime boundary — the loop then returns without writing anything, so the record ages
/// to `unknown` rather than asserting a state nobody is watching.
fn channel_loop(
    input: &Receiver<io::Result<String>>,
    out: &mut impl Write,
    inbox: &Path,
    agent_dir: &Path,
    writer: &mut harness_state::Writer,
    mut context_writer: Option<&mut harness_context::Writer>,
    identity: &str,
    kind: &ChannelKind,
    poll: Duration,
    heartbeat_every: Duration,
) -> Result<()> {
    let mut delivered = HashSet::new();
    let label = kind.label;
    let mut next_heartbeat = Instant::now() + heartbeat_every;
    // What this CONNECTION agreed to speak. A session replacement re-spawns this channel from a
    // possibly-replaced binary while the loaded asset is the predecessor's, so the agreement is
    // per connection and its absence is the default rather than a failure.
    let mut negotiated: Option<u32> = None;
    // The conversation identity, once the asset has forwarded one. It rides every later frame:
    // the axis has no operation of its own, and restating an activity nobody observed just to
    // carry an identity would refresh a stale state.
    let mut conversation: Option<harness_state::ConversationState> = None;
    // A fault raised before this session's first observation existed to attach it to.
    let mut deferred_fault: Option<harness_state::FaultReport> = None;
    // The adapter-owned fault that is still TRUE, tracked separately from whatever occupies the
    // record's single condition slot: a later provider fault may displace it there, and a turn
    // that completes retires the provider's fault without touching st2's own.
    let mut harness_fault: Option<harness_state::FaultReport> = None;
    loop {
        match input.recv_timeout(poll) {
            Ok(line) => {
                let line = line.with_context(|| format!("reading {label} channel input"))?;
                if line.trim().is_empty() {
                    continue;
                }
                // Frames from the extension are otherwise observational. An unknown *or malformed*
                // frame is dropped rather than fatal: a newer asset, or one line of stray output,
                // must not be able to take the channel down and stall an inbox. A failed record
                // write degrades the same way — delivery never depends on observability.
                let frame = serde_json::from_str::<Value>(&line).ok();
                // The typed turn result, decoded once: it feeds two independent records and the
                // credential edge must not depend on the categorical write landing.
                let turn = frame.as_ref().and_then(turn_result);
                if let Some(protocol) = frame.as_ref().and_then(negotiated_protocol) {
                    negotiated = Some(protocol);
                }
                // Which wire this channel writes is the WRITER's business alone: version 3 has
                // the condition axis, version 2 does not, and a record has exactly one source of
                // truth either way. Negotiation is a separate question about what the ASSET
                // promised, and it narrows two axes rather than the whole write — see
                // [`connection_frame`]. Withholding the version 3 tuple from an un-negotiated
                // peer would be strictly worse: its faults are on the same typed turn frame, so
                // a wedged seat would go unstated.
                let states_tuple = writer.writes_condition_axis();
                let promoted = negotiated == Some(PROTOCOL_CONDITION_AXIS);
                if promoted
                    && let Some(claim) = frame
                        .as_ref()
                        .and_then(|frame| conversation_claim(frame, crate::message::now_ms()))
                {
                    conversation = Some(claim);
                }
                if let Some(observation) = frame
                    .as_ref()
                    .and_then(state_observation)
                    .or_else(|| turn.as_ref().and_then(turn_observation))
                {
                    if states_tuple {
                        let mut published =
                            connection_frame(kind, observation, promoted, conversation.clone());
                        // The condition rides the SAME write as the activity it was observed
                        // with: they are one look at the harness, and correlating them across
                        // two writes is a race a reader can lose. A fault the record had nowhere
                        // to attach yet takes this frame instead of being dropped.
                        if let Some(fault) = turn
                            .as_ref()
                            .and_then(|turn| turn_fault(turn, crate::message::now_ms()))
                            .or_else(|| deferred_fault.take())
                        {
                            published.condition = harness_state::ConditionReport::Fault(fault);
                        }
                        publish_frame(writer, published, label);
                    // A queued live frame must never overwrite the wrapper's terminal record:
                    // the channel and the wrapper are separate processes, so the flock alone
                    // serializes but does not order their writes.
                    } else if let Err(error) =
                        writer.observe_unless_ended(legacy_observation(observation))
                    {
                        tracing::warn!(
                            "st2 {label} channel: recording observed state failed: {error}"
                        );
                    }
                }
                // The one positive success edge on the whole axis: a turn that reached its
                // ordinary end. Nothing else clears everything — not an activity edge, not an
                // approval resolution, not a compaction, and least of all a retry omp is about to
                // make, which sends no frame at all. And it clears only what it is evidence
                // about: an adapter-owned pre-compact failure that still holds is restated, since
                // a working provider says nothing about st2's own failed write.
                if states_tuple && matches!(turn.as_ref(), Some(TurnResult::Ordinary)) {
                    let _cleared =
                        apply_condition(writer, turn_completed_edge(harness_fault.as_ref()), label);
                }
                // The credential axis is a third record, independent of the numbers and of the
                // categorical state: a rejection stands until a turn reaches its ordinary end,
                // whatever the seat's activity does in between.
                if let Some(driver) = kind.diagnostic_driver
                    && let Some(edge) = turn.as_ref().and_then(provider_auth_edge)
                {
                    publish_provider_auth(agent_dir, driver, edge);
                }
                // The numeric axis. There is deliberately no cadence here and no heartbeat timer:
                // a producer holding no fresh reading must write nothing at all, so the record
                // ages visibly through `ageMs` instead of looking refreshed. Every frame is handed
                // to the guard, which decides bucket, compaction edge, or heartbeat.
                if let Some(context) = frame.as_ref().and_then(context_frame)
                    && let Some(context_writer) = context_writer.as_deref_mut()
                    && let Err(error) = write_context(context_writer, context)
                {
                    tracing::warn!(
                        "st2 {label} channel: recording harness context failed: {error}"
                    );
                }
                if frame.as_ref().is_some_and(|frame| {
                    frame.get("type").and_then(Value::as_str) == Some("pre_compact")
                }) {
                    match ensure_pre_compact_context(agent_dir) {
                        Err(error) => {
                            tracing::warn!(
                                "st2 {label} channel: writing pre-compact context stub failed: \
                                 {error}"
                            );
                            if states_tuple {
                                // st2's own plumbing is what broke, so this is the one fault
                                // this adapter OWNS rather than observes. It is raised without
                                // restating the activity axis: nothing was learned about whether
                                // the model is working. It is also remembered, because only the
                                // next SUCCESSFUL pre-compact edge may retire it.
                                let edge = pre_compact_edge(false, crate::message::now_ms());
                                if let ConditionEdge::Raise(fault) = &edge {
                                    harness_fault = Some(fault.clone());
                                }
                                deferred_fault = apply_condition(writer, edge, label);
                            } else {
                                let actionable = harness_state::Observation::new(
                                    harness_state::Activity::Active,
                                    harness_state::BlockedOn::None,
                                    harness_state::InputBuffer::Unknown,
                                )
                                .with_reason(PRE_COMPACT_ERROR_REASON);
                                if let Err(state_error) = writer.observe_unless_ended(actionable) {
                                    tracing::warn!(
                                        "st2 {label} channel: recording pre-compact recovery \
                                         failure failed: {state_error}"
                                    );
                                }
                            }
                        }
                        // The stub is there now, so the failure a previous edge recorded is over.
                        // The clear names the category AND the full code — never the category
                        // alone and never a blanket clear — so a standing credential rejection
                        // survives a compaction that went fine.
                        Ok(_) if states_tuple => {
                            deferred_fault = None;
                            harness_fault = None;
                            let _cleared = apply_condition(
                                writer,
                                pre_compact_edge(true, crate::message::now_ms()),
                                label,
                            );
                        }
                        Ok(_) => {}
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            // pi's extension owns this child over stdio. EOF is the session-lifetime boundary, so
            // do not leave a detached watcher behind.
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
        let now = Instant::now();
        if now >= next_heartbeat {
            if let Err(error) = writer.heartbeat() {
                tracing::warn!("st2 {label} channel: refreshing observed state failed: {error}");
            }
            next_heartbeat = now + heartbeat_every;
        }
        for msg in message::list_inbox(inbox)? {
            if delivered.insert(msg.filename.clone()) {
                write_json(out, &message_frame(msg, identity))?;
            }
        }
        out.flush()?;
        thread::sleep(poll);
    }
}

/// The observed-state frame the shipped extension emits on the harness's own turn boundaries.
/// Only positively recognized words become observations: an unrecognized state word is dropped
/// like any other unknown frame, so a newer asset cannot make this channel record something it
/// cannot vouch for. pi offers no waiting-on-a-human signal, so pi frames never carry
/// `blockedOn`; the omp extension does (`tool_approval_requested`/`_resolved`), and its optional
/// axes are parsed here for both channels — a frame without them decodes exactly as before.
fn state_observation(frame: &Value) -> Option<harness_state::Observation> {
    if frame.get("type").and_then(Value::as_str) != Some("state") {
        return None;
    }
    let state = match frame.get("state").and_then(Value::as_str)? {
        "active" => harness_state::Activity::Active,
        "idle" => harness_state::Activity::Idle,
        _ => return None,
    };
    let blocked_on = match frame.get("blockedOn").and_then(Value::as_str) {
        Some("human") => harness_state::BlockedOn::Human,
        _ => harness_state::BlockedOn::None,
    };
    let mut observation =
        harness_state::Observation::new(state, blocked_on, harness_state::InputBuffer::Unknown);
    if blocked_on == harness_state::BlockedOn::Human
        && let Some(ask) = frame.get("ask").and_then(Value::as_str)
    {
        observation = observation.with_ask(parse_ask(ask));
    }
    if let Some(reason) = frame.get("reason").and_then(Value::as_str) {
        observation = observation.with_reason(reason);
    }
    Some(observation)
}

/// The tagged ask axis as ONE kind's asset can state it.
///
/// A kind whose default is `Unknown` cannot see an ask surface at all, so nothing it emits may be
/// promoted into a positive answer — not even the `blockedOn: none` its frames carry by default,
/// which is the pre-axis spelling of "nothing to report" rather than of "no human is waiting".
fn tagged_ask(
    kind: &ChannelKind,
    blocked_on: harness_state::BlockedOn,
    ask: harness_state::Ask,
) -> harness_state::HumanAsk {
    if kind.default_ask == harness_state::HumanAsk::Unknown {
        return harness_state::HumanAsk::Unknown;
    }
    match blocked_on {
        // A real pending ask. An unnamed or unrecognized kind stays indeterminate: the ask is
        // real and its kind unstated, which is not the same as no ask.
        harness_state::BlockedOn::Human => harness_state::HumanAsk::Pending(match ask {
            harness_state::Ask::Permission => harness_state::AskKind::Permission,
            harness_state::Ask::Question => harness_state::AskKind::Question,
            harness_state::Ask::Review => harness_state::AskKind::Review,
            harness_state::Ask::None | harness_state::Ask::Unknown => {
                harness_state::AskKind::Unknown
            }
        }),
        harness_state::BlockedOn::None => kind.default_ask,
        harness_state::BlockedOn::Unknown => harness_state::HumanAsk::Unknown,
    }
}

/// The version 3 tuple one observation states for this kind.
///
/// The condition axis is `Unchanged`, always: an activity or ask edge has learned NOTHING about
/// whether the provider is faulted, and a producer forced to pick `clear` there would fabricate
/// health several times a turn. The caller replaces it only where it genuinely observed a
/// condition with the same look at the harness.
fn kind_frame(kind: &ChannelKind, observation: harness_state::Observation) -> harness_state::Frame {
    let mut frame = harness_state::Frame::new(
        observation.state,
        observation.input_buffer,
        harness_state::ConditionReport::Unchanged,
        tagged_ask(kind, observation.blocked_on, observation.ask),
    );
    if let Some(conversation) = kind.conversation.clone() {
        frame = frame.with_conversation(conversation);
    }
    if let Some(reason) = observation.reason {
        frame = frame.with_reason(reason);
    }
    if let Some(exit) = observation.exit {
        frame = frame.with_exit(exit);
    }
    frame
}

/// One condition operation, as this channel's adapters mint them.
#[derive(Debug, PartialEq)]
enum ConditionEdge {
    Raise(harness_state::FaultReport),
    ClearPaired(harness_state::FaultKey),
    ClearAll(harness_state::ProgressProof),
}

/// Report a refused write, fail-open like every other observation in this loop: delivery never
/// depends on a record landing, so a refusal is a diagnostic and never an error.
fn report_outcome(label: &str, what: &str, outcome: &harness_state::WriteOutcome) {
    if let Some(refusal) = outcome.refusal() {
        tracing::warn!("st2 {label} channel: recording {what} was refused: {refusal:?}");
    }
}

/// The one frame worth restating, and only for the one refusal that proves it is safe.
///
/// A version 3 record's condition axis is not writable as `absent`, so the FIRST activity-only
/// frame of a record whose axis nobody ever stated is refused as
/// [`harness_state::Refusal::Unstated`] — and that refusal is itself the proof that no condition
/// of this session's stands, because a standing one would have been inherited and stated. There
/// is therefore nothing to erase, and `clear` initializes the axis truthfully. Every other
/// refusal is a fact about ownership or a terminal record, which restating cannot help, and a
/// frame that already states a condition is never rewritten.
fn restate_condition(
    frame: &harness_state::Frame,
    outcome: &harness_state::WriteOutcome,
) -> Option<harness_state::Frame> {
    if !matches!(
        outcome.refusal(),
        Some(harness_state::Refusal::Unstated)
    ) || !matches!(frame.condition, harness_state::ConditionReport::Unchanged)
    {
        return None;
    }
    let mut restated = frame.clone();
    restated.condition = harness_state::ConditionReport::Clear;
    Some(restated)
}

/// Publish one version 3 tuple, initializing the condition axis if nobody ever stated it.
fn publish_frame(writer: &mut harness_state::Writer, frame: harness_state::Frame, label: &str) {
    let warn = |error: &anyhow::Error| {
        tracing::warn!("st2 {label} channel: recording observed state failed: {error}");
    };
    match writer.publish_unless_ended(frame.clone()) {
        Ok(outcome) => match restate_condition(&frame, &outcome) {
            Some(restated) => match writer.publish_unless_ended(restated) {
                Ok(outcome) => report_outcome(label, "observed state", &outcome),
                Err(error) => warn(&error),
            },
            None => report_outcome(label, "observed state", &outcome),
        },
        Err(error) => warn(&error),
    }
}

/// Apply one condition operation, returning a fault the record had nowhere to attach yet.
///
/// A condition attaches to an OBSERVATION, so a raise that arrives before this session's first
/// frame is handed back to the caller to ride the next one rather than being dropped: st2's own
/// pre-compact failure is exactly that shape.
///
/// A clear that matched nothing is the ORDINARY case and not a problem to report: most
/// compactions never failed, and most turns end with no fault standing. The writer answers with
/// what actually stands and writes nothing, which is the correct outcome.
fn apply_condition(
    writer: &mut harness_state::Writer,
    edge: ConditionEdge,
    label: &str,
) -> Option<harness_state::FaultReport> {
    let (what, deferred, outcome) = match edge {
        ConditionEdge::Raise(fault) => (
            "a condition",
            Some(fault.clone()),
            writer.raise_fault(fault),
        ),
        ConditionEdge::ClearPaired(key) => ("a condition clear", None, writer.clear_fault(key)),
        ConditionEdge::ClearAll(proof) => ("a condition clear", None, writer.clear_all(proof)),
    };
    match outcome {
        Err(error) => {
            tracing::warn!("st2 {label} channel: recording {what} failed: {error}");
            None
        }
        Ok(harness_state::WriteOutcome::Refused(harness_state::Refusal::Unobserved)) => deferred,
        Ok(harness_state::WriteOutcome::Refused(
            harness_state::Refusal::ConditionMismatch { .. },
        )) => None,
        Ok(outcome) => {
            report_outcome(label, what, &outcome);
            None
        }
    }
}

/// The observed-state reason a rejected provider credential publishes, shared verbatim with the
/// OpenCode, Codex, and Claude producers: one word for one class, whatever named it.
const PROVIDER_AUTH_REASON: &str = "providerAuth";

/// omp's own error-classification bitfield, as `errorId` carries it on the assistant message whose
/// `stopReason` is `error` (omp's `qe` flags, measured on omp 18.1.7 — see
/// `docs/vrs/06-omp-driver/.experiments/2026-09-05-omp-provider-credential-rejection.md`).
///
/// Only the five flags this classifier needs are named. Reading the field at all is what keeps st2
/// out of provider prose: omp already did the classification, and its own credential-invalidating
/// rule is exactly the conjunction below.
mod omp_error {
    /// `qe.Class` — set by every classified value, and by nothing else. Without a flag the same
    /// field carries a BARE HTTP STATUS, so a bit test that skipped this would be reading digits.
    pub const CLASSIFIED: u64 = 1 << 12;
    /// `qe.AccountPolicy` — an org or content policy refusal; measured co-occurring with
    /// `AuthFailed` on a `cyber_policy` 403, which no re-login satisfies.
    pub const ACCOUNT_POLICY: u64 = 1 << 14;
    /// `qe.Transient` — omp intends to retry; measured co-occurring with `AuthFailed` on a
    /// `CONCURRENT_LIMIT` 403, the case omp's own rule excludes by prose.
    pub const TRANSIENT: u64 = 1 << 17;
    /// `qe.UsageLimit` — an exhausted allowance; measured co-occurring with `AuthFailed` on the
    /// `You have run out of credits` 403 that wedged a live seat for 120 transitions.
    pub const USAGE_LIMIT: u64 = 1 << 19;
    /// `qe.AuthFailed` — omp's name for a refused credential, set from a 401/403 status and from
    /// its own auth-error types.
    pub const AUTH_FAILED: u64 = 1 << 24;
}

/// One `type: "turn"` frame as the shipped omp extension emits it: the typed result of a turn that
/// ACTUALLY ended. A turn omp will retry (`willContinue`) sends no frame at all, so neither
/// credential edge is ever claimed mid-turn.
enum TurnResult<'a> {
    /// The turn reached its ordinary end — positive proof the provider accepted the credential.
    /// It asserts no activity: the extension's sampled idle poll still owns that edge.
    Ordinary,
    /// The turn ended on a provider error, carrying omp's own words for it.
    ProviderError {
        reason: Option<&'a str>,
        classification: Option<u64>,
    },
}

fn turn_result(frame: &Value) -> Option<TurnResult<'_>> {
    if frame.get("type").and_then(Value::as_str) != Some("turn") {
        return None;
    }
    let Some(error) = frame.get("error") else {
        return Some(TurnResult::Ordinary);
    };
    Some(TurnResult::ProviderError {
        reason: error.get("reason").and_then(Value::as_str),
        classification: error.get("errorId").and_then(Value::as_u64),
    })
}

/// Whether omp's classification of the error that ended a turn names a REJECTED CREDENTIAL.
///
/// `AuthFailed` alone is not the answer, because omp sets it from prose that says `401`, `403`, or
/// `forbidden` as well as from a typed status — and three of the four measured 403s were capacity,
/// policy, or concurrency. The three negative flags are each a measured co-occurrence, not a
/// precaution, and together they are omp's own rule for reaching into the credential store.
/// A classification this reader cannot see at all is not a rejection: silence beats a guess.
fn provider_credential_rejected(classification: Option<u64>) -> bool {
    let Some(id) = classification else {
        return false;
    };
    id & omp_error::CLASSIFIED != 0
        && id & omp_error::AUTH_FAILED != 0
        && id & (omp_error::USAGE_LIMIT | omp_error::ACCOUNT_POLICY | omp_error::TRANSIENT) == 0
}

/// The categorical half of a typed turn result.
///
/// A provider error that ended the turn is `active`, not an idle settle: nothing is running, but
/// the seat needs an operator and a record saying `idle` would read as a healthy yield. The reason
/// is the closed `providerAuth` word for the credential class — the same word Claude, Codex, and
/// OpenCode publish — and omp's own bounded prose for every other class, which is the only place
/// a reader learns that a 403 was about credits.
fn turn_observation(result: &TurnResult<'_>) -> Option<harness_state::Observation> {
    let TurnResult::ProviderError {
        reason,
        classification,
    } = result
    else {
        return None;
    };
    let observation = harness_state::Observation::new(
        harness_state::Activity::Active,
        harness_state::BlockedOn::None,
        harness_state::InputBuffer::Unknown,
    );
    Some(if provider_credential_rejected(*classification) {
        observation.with_reason(PROVIDER_AUTH_REASON)
    } else {
        match *reason {
            Some(reason) => observation.with_reason(reason),
            None => observation,
        }
    })
}

/// What one typed turn result proves about the seat's provider credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderAuthEdge {
    Rejected,
    Accepted,
}

/// The credential edge, or `None` when the turn proves nothing about it — which must leave a
/// standing rejection alone rather than clearing it. A provider error of any other class is
/// exactly that case: a rate limit says nothing about whether the credential is still good.
fn provider_auth_edge(result: &TurnResult<'_>) -> Option<ProviderAuthEdge> {
    match result {
        TurnResult::Ordinary => Some(ProviderAuthEdge::Accepted),
        TurnResult::ProviderError { classification, .. } => {
            provider_credential_rejected(*classification).then_some(ProviderAuthEdge::Rejected)
        }
    }
}

/// Record one credential edge on the seat's native-driver diagnostic.
///
/// A fresh publisher per edge, like the Claude hook's: the on-disk fallback is what lets an
/// ordinary turn end clear a rejection, and a channel that restarted mid-session inherits the
/// predecessor's record rather than silently starting clean. Fail-open like every other
/// observation in this loop — the publisher only warns on a write it cannot land, and delivery
/// never depends on it.
fn publish_provider_auth(
    agent_dir: &Path,
    driver: driver_diagnostic::Driver,
    edge: ProviderAuthEdge,
) {
    let mut publisher = driver_diagnostic::Publisher::new(
        agent_dir,
        driver,
        // The wrapper — not the channel — owns the version gate, and it refuses the launch on an
        // unadmitted MINOR (OMP-R05), so a running channel has no version fact of its own to
        // publish and no support verdict to restate.
        None,
        driver_diagnostic::Support::Unknown,
    );
    match edge {
        ProviderAuthEdge::Rejected => publisher.publish(
            driver_diagnostic::Stage::ProviderAuth,
            driver_diagnostic::Reason::ProviderAuthRejected,
            driver_diagnostic::Source::TurnResult,
        ),
        ProviderAuthEdge::Accepted => publisher.clear(driver_diagnostic::Stage::ProviderAuth),
    }
}

/// The protocol version whose whole content is a PROMISE BY THE ASSET: that it retires a
/// never-answered ask on a turn boundary (a denied ask emits no `tool_result` at all, DQ-OMP-1)
/// and that it forwards omp's own `sessionId`. No frame changes shape; what changes is what st2 is
/// entitled to STATE — a positive `none` on the ask axis, and a linked conversation — so the
/// agreement is per CONNECTION and never per binary: a session replacement re-spawns this channel
/// from a possibly-replaced binary while the loaded asset is the predecessor's, so either version
/// may be on the other end at any time. Offered on the hello beside `protocol`, which stays 1
/// forever because an asset refuses a hello it does not understand and a refusal costs that seat
/// its mail.
const PROTOCOL_CONDITION_AXIS: u32 = 2;

/// The asset's answer to the hello's offer, or `None` for every other frame — including an answer
/// naming a version st2 never offered, which is not an agreement but a frame this channel drops
/// like any other it cannot vouch for.
fn negotiated_protocol(frame: &Value) -> Option<u32> {
    if frame.get("type").and_then(Value::as_str) != Some("client_hello") {
        return None;
    }
    let answered = frame.get("protocol").and_then(Value::as_u64)?;
    (answered == u64::from(PROTOCOL_CONDITION_AXIS)).then_some(PROTOCOL_CONDITION_AXIS)
}

/// omp's fault codes: open, provider-namespaced, and diagnostic granularity UNDERNEATH the closed
/// category beside them — one code per measured class, so a reader can tell an exhausted
/// allowance from a refused key without reading prose, and no consumer has to.
mod omp_fault {
    pub const AUTH_FAILED: &str = "omp/authFailed";
    pub const USAGE_LIMIT: &str = "omp/usageLimit";
    pub const ACCOUNT_POLICY: &str = "omp/accountPolicy";
    pub const TRANSIENT_EXHAUSTED: &str = "omp/transientExhausted";
    pub const PROVIDER_ERROR: &str = "omp/providerError";
    pub const UNCLASSIFIED: &str = "omp/unclassified";
    /// The one fault this ADAPTER owns rather than observes: st2's own last-resort pre-compact
    /// checkpoint could not be written. Nothing about omp is wrong; st2's plumbing is.
    pub const PRE_COMPACT_WRITE_FAILED: &str = "omp/preCompactContextWriteFailed";
}

/// The condition axis of one typed turn result, over the classifications measured on omp 18.1.7
/// (`docs/vrs/06-omp-driver/.experiments/2026-09-05-omp-provider-credential-rejection.md`).
///
/// Three decisions this encodes, none of which may be re-litigated silently:
///
/// * `qe.Class` gates everything. Without it the same field carries a BARE HTTP STATUS, so a bit
///   test that skipped it would be reading digits — and reading digits is how a 403 about credits
///   becomes a refused credential.
/// * The negative flags outrank `AuthFailed`, in the order they were measured co-occurring with
///   it: an exhausted allowance is `quota`, an org or content refusal is `policy`, a throttle that
///   reached a turn END is `rateLimit`. Only `Class + AuthFailed` alone is `authentication`, which
///   is exactly [`provider_credential_rejected`] — the same rule, stated once for two axes.
/// * `Recovery::Unknown`, never `Automatic`, for the throttled and unclassified rows. The turn
///   frame carries no deadline, and an automatic fault without a `nextObservationDueMs` can never
///   escalate; `Unknown` is documented as never optimistic, so it pages.
///
/// `UsageLimit` deliberately does not split `quota` from `account`: the 402 "insufficient balance"
/// and the 403 "out of credits" carry the SAME flag, and separating them would require reading
/// omp's prose (OMP-R06, OHS-R16 forbid it).
///
/// No `detail` is attached. omp's own words already ride the record's `reason` verbatim, exactly
/// as they do today, and duplicating them into the fault would make its semantic clock restart
/// every time the provider reworded the same condition.
fn turn_fault(result: &TurnResult<'_>, observed_at_ms: u64) -> Option<harness_state::FaultReport> {
    let TurnResult::ProviderError { classification, .. } = result else {
        return None;
    };
    use harness_state::{FaultCategory as Category, Recovery};
    let (category, code, recovery) = match classification {
        Some(id) if id & omp_error::CLASSIFIED != 0 => {
            let id = *id;
            if id & omp_error::USAGE_LIMIT != 0 {
                (Category::Quota, omp_fault::USAGE_LIMIT, Recovery::Human)
            } else if id & omp_error::ACCOUNT_POLICY != 0 {
                (Category::Policy, omp_fault::ACCOUNT_POLICY, Recovery::Human)
            } else if id & omp_error::TRANSIENT != 0 {
                (
                    Category::RateLimit,
                    omp_fault::TRANSIENT_EXHAUSTED,
                    Recovery::Unknown,
                )
            } else if id & omp_error::AUTH_FAILED != 0 {
                (
                    Category::Authentication,
                    omp_fault::AUTH_FAILED,
                    Recovery::Human,
                )
            } else {
                (
                    Category::Provider,
                    omp_fault::PROVIDER_ERROR,
                    Recovery::Unknown,
                )
            }
        }
        // A classification this reader cannot see is still a fault, and it stays VISIBLE: the
        // turn died between omp and the provider, which is what `provider` says, and the code
        // says st2 could not narrow it. `harness` would claim st2's own plumbing broke — a
        // different and untrue statement — and `clear` would launder a wedged seat.
        _ => (
            Category::Provider,
            omp_fault::UNCLASSIFIED,
            Recovery::Unknown,
        ),
    };
    Some(harness_state::FaultReport::new(category, recovery, observed_at_ms).with_code(code))
}

/// The version 2 projection of one observation: exactly the bytes this channel wrote before the
/// negotiated vocabulary existed.
///
/// The only new word a negotiated asset puts on an UNBLOCKED state frame is the approval-denial
/// diagnostic, and version 2 has no ask axis that makes it meaningful, so it is withheld here
/// rather than appearing as a novel `reason`. Every reason the version 2 wire already carried is
/// untouched: a blocked frame's ask prose, a turn error's own words, and the pre-compact recovery
/// reason all ride through verbatim.
fn legacy_observation(
    mut observation: harness_state::Observation,
) -> harness_state::Observation {
    if observation.blocked_on != harness_state::BlockedOn::Human
        && observation.reason.as_deref() == Some(APPROVAL_DENIED_REASON)
    {
        observation.reason = None;
    }
    observation
}

/// What one pre-compact edge does to the adapter-owned fault: a failed stub write raises it, and
/// a SUCCESSFUL one is the only thing that retires it. No amount of provider-side progress can:
/// the two facts are unrelated, which is why the retirement is a paired clear naming this exact
/// code rather than anything blanket.
fn pre_compact_edge(succeeded: bool, observed_at_ms: u64) -> ConditionEdge {
    if succeeded {
        ConditionEdge::ClearPaired(pre_compact_fault_key())
    } else {
        ConditionEdge::Raise(pre_compact_fault(observed_at_ms))
    }
}

/// What an ordinary turn end states about the condition axis.
///
/// A completed turn proves the PROVIDER accepted the credential and did the work, which is what
/// authorizes clearing a fault nobody watched resolve. It proves nothing whatsoever about st2's
/// own pre-compact write, so an adapter-owned fault that still holds is RESTATED instead of being
/// swept up by the blanket clear — and restating the same fault preserves the instant it was
/// first observed, so its semantic clock survives every turn that runs underneath it.
fn turn_completed_edge(standing: Option<&harness_state::FaultReport>) -> ConditionEdge {
    match standing {
        Some(fault) => ConditionEdge::Raise(fault.clone()),
        None => ConditionEdge::ClearAll(harness_state::ProgressProof::TurnCompleted),
    }
}

/// The tuple as it may be stated on THIS connection.
///
/// The condition axis rides the typed `turn` frame, which every protocol version sends
/// identically, so a fault is stated either way: a wedged seat must stay visible whatever the
/// asset negotiated. The conversation axis rests on a promise only a negotiated asset made — to
/// forward omp's own session id — so no wire-evidenced link is claimed without it, while a
/// kind-level capability claim (pi's `unsupported`) is a fact about the driver and stands on
/// every connection.
///
/// The ask axis is downgraded in exactly ONE direction: a positive `none` becomes `unknown`,
/// because the promise that makes absence provable — retiring a never-answered ask on a turn
/// boundary — was never made, so an un-negotiated asset's `none` could be a denied ask nobody
/// retired. A PENDING ask is preserved verbatim, kind and all: the legacy `blockedOn`/`ask` pair
/// rides the same frame under both protocols, so a waiting human is equally proven either way,
/// and a frame blocked without a nameable kind is already `Pending(Unknown)` — a human is
/// waiting and the kind is unstated. Downgrading a pending ask to `unknown` would hide the one
/// thing this axis exists to surface, and dropping it would be worse still.
fn connection_frame(
    kind: &ChannelKind,
    observation: harness_state::Observation,
    negotiated: bool,
    conversation: Option<harness_state::ConversationState>,
) -> harness_state::Frame {
    let mut frame = kind_frame(kind, observation);
    if !negotiated {
        frame.ask = match frame.ask {
            harness_state::HumanAsk::None => harness_state::HumanAsk::Unknown,
            pending => pending,
        };
        return frame;
    }
    if let Some(claim) = conversation {
        frame = frame.with_conversation(claim);
    }
    frame
}

/// st2's own pre-compact recovery write failed: `harness`, because the harness plumbing is what
/// broke, and `human`, because nothing retries it — the next compaction edge is the only thing
/// that can prove it works again.
fn pre_compact_fault(observed_at_ms: u64) -> harness_state::FaultReport {
    harness_state::FaultReport::new(
        harness_state::FaultCategory::Harness,
        harness_state::Recovery::Human,
        observed_at_ms,
    )
    .with_code(omp_fault::PRE_COMPACT_WRITE_FAILED)
}

/// The EXACT pairing key the recovered edge clears: category and full code. Never the category
/// alone and never a blanket clear — a standing `authentication`/`omp/authFailed` from a failed
/// turn must survive a compaction that went fine, and a mismatch is answered with
/// [`harness_state::Refusal::ConditionMismatch`] and no write at all, which is the ordinary case
/// here because most compactions never failed in the first place.
fn pre_compact_fault_key() -> harness_state::FaultKey {
    harness_state::FaultKey::new(harness_state::FaultCategory::Harness)
        .with_code(omp_fault::PRE_COMPACT_WRITE_FAILED)
}

/// omp's own conversation identity, off the `{"type":"conversation"}` frame a negotiated asset
/// sends the first time an event exposes one.
///
/// The evidence is `sessionId`, measured on both halves of the approval pair and identical across
/// it (18.0.9 and 18.1.2). `Probed` because it was read off a live event rather than declared
/// from typings, and `Rewritable` because omp compacts its own session store — a prefix read once
/// may be gone. Before any frame arrives the axis is OMITTED, never `Unsupported`: omp
/// demonstrably has sessions (`--no-session`, `sessionManager`), so claiming it has none would be
/// a false capability claim, while saying nothing claims nothing.
fn conversation_claim(
    frame: &Value,
    verified_through_ms: u64,
) -> Option<harness_state::ConversationState> {
    if frame.get("type").and_then(Value::as_str) != Some("conversation") {
        return None;
    }
    let conversation = frame.get("sessionId").and_then(Value::as_str)?.trim();
    // A link with no positive verification bound is refused at the write boundary, so an
    // unstampable observation is no observation.
    if conversation.is_empty() || verified_through_ms == 0 {
        return None;
    }
    Some(harness_state::ConversationState::Linked(
        harness_state::ConversationClaim {
            driver: OMP_KIND.label.to_string(),
            conversation: conversation.to_owned(),
            history_mutability: harness_state::HistoryMutability::Rewritable,
            capability_evidence: harness_state::CapabilityEvidence::Probed,
            verified_through_ms,
        },
    ))
}

/// Write the recovery stub only when durable working state is absent or whitespace-only.
///
/// The extension cannot perform this check: it owns neither the resolved agent directory nor the
/// context module's shared writer lock. The context API keeps predicate and replacement in one
/// critical section and preserves every read error except `NotFound`.
fn ensure_pre_compact_context(agent_dir: &Path) -> Result<bool> {
    context::write_now_if_blank(&context::context_dir(agent_dir), PRE_COMPACT_STUB)
}

/// One `type: "context"` frame as the shipped extension emits it: a reading, a compaction edge, or
/// both. Both halves are optional, and a frame carrying neither is not a frame — it is dropped
/// like any other unrecognized one.
type ContextFrame = (
    Option<harness_context::Reading>,
    Option<harness_context::Compaction>,
);

/// Decode a context frame (HC-R02, HC-R03, HC-R12).
///
/// The withholding discipline lives here as much as in the asset: a number this decoder cannot
/// read as a finite number is `None`, never zero and never the previous value. `usedPercent` is
/// taken exactly as the harness published it — pi and omp report a float that runs well above 100
/// on an overrun (585.6% measured), and st2 neither clamps it nor computes one of its own from a
/// window it would have had to guess at.
fn context_frame(frame: &Value) -> Option<ContextFrame> {
    if frame.get("type").and_then(Value::as_str) != Some("context") {
        return None;
    }
    let reading = frame.get("reading").and_then(Value::as_object).map(|body| {
        let tokens = |key: &str| body.get(key).and_then(token_count);
        harness_context::Reading {
            used_tokens: tokens("usedTokens"),
            window_tokens: tokens("windowTokens"),
            used_percent: body.get("usedPercent").and_then(Value::as_f64),
            model: body
                .get("model")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            cost_usd: body.get("costUsd").and_then(Value::as_f64),
            // pi and omp both withhold this in v1: obtaining it means summing every message's
            // usage, which is a producer-side accumulator whose correctness depends on having seen
            // every message. tokenlens owns lifetime accounting, and a half-observed sum would be
            // a worse answer than none.
            session_total_tokens: None,
            // Neither harness reports account-scoped rate limits.
            rate_limits: harness_context::RateLimits::default(),
        }
    });
    let compaction = frame
        .get("compaction")
        .and_then(Value::as_object)
        .map(|body| {
            let edge = harness_context::Compaction::new(compaction_trigger(
                body.get("trigger").and_then(Value::as_str),
            ));
            // The harness's own session store answers the count for both of these harnesses, so
            // the counter is durable across restarts. A frame that could not read it leaves the
            // count absent, and st2 falls back to incrementing its own — a weaker,
            // incarnation-scoped answer rather than a wrong one.
            match body.get("count").and_then(Value::as_u64) {
                Some(count) => edge.with_count(count),
                None => edge,
            }
        });
    (reading.is_some() || compaction.is_some()).then_some((reading, compaction))
}

/// A token count off the wire, tolerating a fractional one.
///
/// Both harnesses round today — pi's fallback estimator is `Math.ceil(chars / 4)` and every
/// measured reading was integral — so this is deliberately not a decode of anything observed. It
/// exists because the failure if one ever stops rounding is silent in the worst direction: a plain
/// integer parse would return `None` for `1234.75`, and the producer would WITHHOLD a reading the
/// harness actually had. Withholding is reserved for a harness saying it does not know (HC-R03);
/// spending it on a JSON number shape would make the record lie about which of those happened. The
/// percent leg already parses as a float because pi and omp genuinely emit one there; this extends
/// the same tolerance to the operands rather than leaving them stricter for no measured reason.
///
/// A negative or non-finite value is not a token count and is withheld.
fn token_count(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_f64()
            .filter(|number| number.is_finite() && *number >= 0.0)
            .map(|number| number.round() as u64)
    })
}

/// The trigger word, over the record's closed vocabulary and additive-tolerant on read: a word
/// this version does not recognize — and an edge that carries none at all, which is omp's case and
/// three of the five harnesses' — decodes as `unknown`, never as a definite trigger.
fn compaction_trigger(word: Option<&str>) -> harness_context::CompactionTrigger {
    use harness_context::CompactionTrigger as Trigger;
    match word {
        Some("manual") => Trigger::Manual,
        Some("auto") => Trigger::Auto,
        Some("threshold") => Trigger::Threshold,
        Some("overflow") => Trigger::Overflow,
        Some("idle") => Trigger::Idle,
        _ => Trigger::Unknown,
    }
}

/// Land one context frame.
///
/// A frame carrying both halves lands as ONE write, and that is load-bearing rather than an
/// optimization: a compaction edge always writes while a reading whose percent is withheld has no
/// bucket, so an edge written alone would publish the stale pre-compaction numbers beside it and
/// the null reading proving the window was emptied would not appear until the heartbeat came due.
/// pi hands us exactly that pair — measured inside its own `session_compact` handler,
/// `getContextUsage()` already reports `{tokens: null, percent: null}` there.
fn write_context(
    writer: &mut harness_context::Writer,
    (reading, compaction): ContextFrame,
) -> Result<bool> {
    match (reading, compaction) {
        (Some(reading), Some(compaction)) => writer.compacted_with(compaction, reading),
        (Some(reading), None) => writer.observe(reading),
        (None, Some(compaction)) => writer.compacted(compaction),
        (None, None) => Ok(false),
    }
}

/// The machine-readable ask word on a blocked frame. An unrecognized word decodes as unknown —
/// indeterminate, never silently reclassified — matching the record's own decode rule.
fn parse_ask(word: &str) -> harness_state::Ask {
    match word {
        "permission" => harness_state::Ask::Permission,
        "question" => harness_state::Ask::Question,
        "review" => harness_state::Ask::Review,
        _ => harness_state::Ask::Unknown,
    }
}

/// What a starting or restarting pi session is told about its own durable state.
///
/// pi has no session-start hook, so this is the payload that stands in for
/// `$ST_HOOKS/codex-session-start.sh`. The three blocks and their order are deliberately identical
/// to that script's, so a persona written against one harness reads the same on pi. Empty when
/// there is nothing to restore and no unread work, so a fresh agent is told nothing but its ritual.
fn session_context(agent_dir: &Path, identity: &str) -> String {
    let mut blocks = Vec::new();
    let state = context::read_now_fresh(&context::context_dir(agent_dir), CONTEXT_MAX_AGE);
    if !state.trim().is_empty() {
        blocks.push(format!(
            "<context source=\"st2/context/now.md\" agent=\"{identity}\">\n{}\n</context>",
            state.trim_end()
        ));
    }
    blocks.push(RITUAL.to_string());
    let unread = message::list_inbox(&message::inbox_dir(agent_dir)).unwrap_or_default();
    if !unread.is_empty() {
        let mut lines = vec![format!("## st2 inbox ({} unread)", unread.len())];
        lines.extend(unread.iter().map(|msg| {
            let from = msg.from.as_deref().unwrap_or("unknown");
            match msg.subject.as_deref() {
                Some(subject) => format!("- {}  {from}  Subject: {subject}", msg.filename),
                None => format!("- {}  {from}", msg.filename),
            }
        }));
        blocks.push(lines.join("\n"));
    }
    blocks.join("\n\n")
}

/// One inbox entry as the frame the extension hands to pi.
///
/// The envelope matches the Claude channel's exactly, so a persona written against one native
/// harness reads the same on the other.
fn message_frame(msg: message::Message, identity: &str) -> Value {
    let content = channel_content(msg.subject.as_deref(), &msg.body);
    json!({"type":"message","deliverAs":DELIVER_AS,"content":content,"meta":{
        "from": msg.from,
        "messageFilename": msg.filename,
        "threadFilename": msg.in_reply_to.unwrap_or_else(|| msg.filename.clone()),
        "identity": identity
    }})
}

fn write_json(out: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the two words pi's own turn boundaries can vouch for become observations. Everything
    /// else — other frame types, unknown state words, missing fields — is dropped, so a newer
    /// extension asset cannot push this channel into recording something it cannot prove.
    #[test]
    fn only_recognized_state_frames_become_observations() {
        let active = state_observation(&json!({"type":"state","state":"active"})).unwrap();
        assert_eq!(active.state, harness_state::Activity::Active);
        assert_eq!(active.blocked_on, harness_state::BlockedOn::None);
        assert_eq!(active.input_buffer, harness_state::InputBuffer::Unknown);
        assert_eq!(
            state_observation(&json!({"type":"state","state":"idle"}))
                .unwrap()
                .state,
            harness_state::Activity::Idle
        );

        for frame in [
            json!({"type":"state","state":"child"}),
            json!({"type":"state","state":"unknown"}),
            json!({"type":"state"}),
            json!({"type":"delivered","state":"active"}),
            json!({"state":"active"}),
        ] {
            assert_eq!(state_observation(&frame), None, "frame: {frame}");
        }
    }

    /// The omp extension's approval frames carry the blocked-on-human axis pi never emits. The
    /// optional axes must decode for either channel's frames, an unrecognized ask word decodes
    /// unknown (never silently reclassified), and a blocked frame without the extras decodes as
    /// before.
    #[test]
    fn blocked_frames_carry_the_human_axes() {
        let blocked = state_observation(&json!({
            "type":"state","state":"active","blockedOn":"human",
            "ask":"permission","reason":"bash"
        }))
        .unwrap();
        assert_eq!(blocked.blocked_on, harness_state::BlockedOn::Human);
        assert_eq!(blocked.ask, harness_state::Ask::Permission);
        assert_eq!(blocked.reason.as_deref(), Some("bash"));

        let question = state_observation(&json!({
            "type":"state","state":"active","blockedOn":"human",
            "ask":"question","reason":"Which deployment target?"
        }))
        .unwrap();
        assert_eq!(question.blocked_on, harness_state::BlockedOn::Human);
        assert_eq!(question.ask, harness_state::Ask::Question);
        assert_eq!(
            question.reason.as_deref(),
            Some("Which deployment target?")
        );

        let unknown_ask = state_observation(&json!({
            "type":"state","state":"active","blockedOn":"human",
            "ask":"sacrifice"
        }))
        .unwrap();
        assert_eq!(unknown_ask.blocked_on, harness_state::BlockedOn::Human);
        assert_eq!(unknown_ask.ask, harness_state::Ask::Unknown);

        let plain = state_observation(&json!({"type":"state","state":"idle"})).unwrap();
        assert_eq!(plain.blocked_on, harness_state::BlockedOn::None);
        assert_eq!(plain.ask, harness_state::Ask::None);
    }

    /// A pre-compaction edge creates a last-resort checkpoint only for whitespace-only state. The
    /// channel, not the TypeScript extension, resolves the durable path and performs the write.
    #[test]
    fn pre_compact_frame_writes_only_over_blank_context() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        let context_dir = context::context_dir(agent_dir);
        context::write_now(&context_dir, " \n\t").unwrap();

        let run_frame = || {
            let mut writer =
                harness_state::Writer::new(agent_dir, "h.worker", "omp", Some("h.worker".into()));
            let (tx, rx) = mpsc::channel();
            tx.send(Ok(r#"{"type":"pre_compact"}"#.to_string()))
                .unwrap();
            drop(tx);
            channel_loop(
                &rx,
                &mut Vec::new(),
                &inbox,
                agent_dir,
                &mut writer,
                None,
                "h.worker",
                &OMP_KIND,
                Duration::from_millis(1),
                Duration::from_secs(60),
            )
            .unwrap();
        };

        run_frame();
        assert_eq!(
            context::read(&context_dir, context::View::Now),
            PRE_COMPACT_STUB
        );

        let authored = "Investigating scheduler race; next run the focused repro.\n";
        context::write_now(&context_dir, authored).unwrap();
        run_frame();
        assert_eq!(
            context::read(&context_dir, context::View::Now),
            authored,
            "the recovery edge must never replace authored state"
        );

        std::fs::remove_file(context_dir.join("now.md")).unwrap();
        std::fs::write(context_dir.join("now.md"), [0xff]).unwrap();
        run_frame();
        assert_eq!(
            std::fs::read(context_dir.join("now.md")).unwrap(),
            [0xff],
            "undecodable state must not be replaced"
        );
        let raw: Value = serde_json::from_slice(
            &std::fs::read(harness_state::harness_state_path(agent_dir)).unwrap(),
        )
        .unwrap();
        assert_eq!(raw["state"], "active");
        assert_eq!(raw["reason"], PRE_COMPACT_ERROR_REASON);
    }

    /// The stdio connection is the evidence. While it lives, the record's heartbeat advances
    /// without a new observation; when it ends, the loop returns having written nothing more, so
    /// the last state is left to age to `unknown` instead of being asserted or terminated — the
    /// terminal record belongs to the session wrapper, which sees the provider die.
    #[test]
    fn heartbeats_while_connected_then_leaves_the_record_to_age_on_eof() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let record = harness_state::harness_state_path(agent_dir);
        let mut writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()));
        let (tx, rx) = mpsc::channel();

        tx.send(Ok(r#"{"type":"state","state":"active"}"#.to_string()))
            .unwrap();
        drop(tx);
        let mut out = Vec::new();
        channel_loop(
            &rx,
            &mut out,
            &message::inbox_dir(agent_dir),
            agent_dir,
            &mut writer,
            None,
            "h.worker",
            &PI_KIND,
            Duration::from_millis(2),
            Duration::ZERO,
        )
        .unwrap();

        let raw: Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
        assert_eq!(raw["state"], "active", "EOF must not rewrite the state");
        assert!(
            raw["writtenAtMs"].as_u64().unwrap() > raw["sinceMs"].as_u64().unwrap(),
            "no heartbeat re-stamped the record while the connection lived: {raw}"
        );
        assert!(
            raw.get("exit").is_none(),
            "EOF must not write a terminal record: {raw}"
        );
    }

    /// The wrapper's terminal record is the incarnation's last word: a live frame the extension
    /// queued before dying must not resurrect the session after the wrapper reaped it.
    #[test]
    fn a_queued_live_frame_never_overwrites_the_wrappers_terminal_record() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let record = harness_state::harness_state_path(agent_dir);
        // The wrapper mints the session token and the channel adopts it — that sharing is what
        // makes the wrapper's terminal record this session's last word.
        let session = harness_state::session_token();
        let mut channel_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()))
                .with_session(session.clone());
        let mut wrapper_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()))
                .with_session(session);
        wrapper_writer.ended("signal 9").unwrap();
        let terminal = std::fs::read(&record).unwrap();

        let (tx, rx) = mpsc::channel();
        tx.send(Ok(r#"{"type":"state","state":"idle"}"#.to_string()))
            .unwrap();
        drop(tx);
        let mut out = Vec::new();
        channel_loop(
            &rx,
            &mut out,
            &message::inbox_dir(agent_dir),
            agent_dir,
            &mut channel_writer,
            None,
            "h.worker",
            &PI_KIND,
            Duration::from_millis(2),
            Duration::from_millis(5),
        )
        .unwrap();

        assert_eq!(std::fs::read(&record).unwrap(), terminal);
    }

    /// Land one context frame through a real writer and read the record back.
    fn record_after(
        frames: &[Value],
        harness: harness_context::Harness,
    ) -> harness_context::Observed {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path().join("agents").join("h").join("h.worker");
        std::fs::create_dir_all(&agent_dir).unwrap();
        let mut writer = harness_context::Writer::new(&agent_dir, "h.worker", harness).unwrap();
        for frame in frames {
            if let Some(context) = context_frame(frame) {
                write_context(&mut writer, context).unwrap();
            }
        }
        harness_context::read(&harness_context::harness_context_path(&agent_dir))
            .expect("a record must have been written")
    }

    /// HC-R13, pinned to pi 0.84.2. The payload is verbatim from the credential-free pi lab: one
    /// `message_end` for an assistant message, with `getContextUsage()` and the message's own
    /// `usage` side by side.
    ///
    /// The teeth are the numerator's MEANING, not merely its value. pi's `tokens` is the last
    /// assistant message's `totalTokens` — input + output + cacheRead + cacheWrite — and the
    /// fixture carries `input` too, so a producer that started publishing the prompt figure (which
    /// is what omp's identically-shaped call returns) fails here rather than silently publishing a
    /// differently-meaning number under the same field name. The percent is carried raw at 585.625:
    /// pi reports a float that runs far above 100 when a turn overruns the window, and a producer
    /// or reader that clamped it would hide exactly the saturation this record exists to show.
    #[test]
    fn the_pi_0_84_2_fixture_pins_total_tokens_as_the_numerator() {
        const MEASURED: &str = crate::pi_session::MEASURED_CONTEXT_VERSION;
        assert_eq!(
            MEASURED, "0.84.2",
            "the fixture below was captured on this build"
        );
        // Verbatim `event.message.usage` from the same event, carried so the assertion below can
        // name the number this producer must NOT publish.
        let message_usage = json!({
            "input": 23300, "output": 25, "cacheRead": 100, "cacheWrite": 0, "reasoning": 0,
            "totalTokens": 23425,
            "cost": {"input": 0.0699, "output": 0.000375, "cacheRead": 0.00003,
                     "cacheWrite": 0.0, "total": 0.070305}
        });
        // Verbatim `ctx.getContextUsage()` on that event, as the extension forwards it.
        let frame = json!({"type": "context", "reading": {
            "usedTokens": 23425, "windowTokens": 4000, "usedPercent": 585.625,
            "model": "fake-1", "costUsd": 0.070305
        }});

        let record = record_after(&[frame], harness_context::Harness::Pi);

        assert_eq!(record.harness, harness_context::Harness::Pi);
        assert_eq!(
            record.used_tokens,
            message_usage["totalTokens"].as_u64(),
            "pi {MEASURED}: the numerator is the assistant message's totalTokens"
        );
        assert_ne!(
            record.used_tokens,
            message_usage["input"].as_u64(),
            "pi {MEASURED}: publishing the prompt figure would be omp's arithmetic under pi's tag"
        );
        assert_eq!(record.window_tokens, Some(4000));
        assert_eq!(
            record.used_percent,
            Some(585.625),
            "carried raw, never clamped"
        );
        assert_eq!(record.model.as_deref(), Some("fake-1"));
        assert_eq!(record.cost_usd, message_usage["cost"]["total"].as_f64());
        // Neither pi nor omp carries these in v1: a lifetime sum would need a producer-side
        // accumulator whose correctness depends on having seen every message, and neither reports
        // account-scoped rate limits at all.
        assert_eq!(record.session_total_tokens, None);
        assert_eq!(record.rate_limits, harness_context::RateLimits::default());
    }

    /// HC-R13 and HC-T03's second version-coupled constant, pinned to omp 18.0.9 (the same probe
    /// run reproduces it on 18.0.3, which is why the launch gate can admit the whole 18.0 minor).
    ///
    /// omp's call has pi's exact shape and a DIFFERENT meaning: `tokens` settles to the last
    /// assistant message's prompt figure — `input` plus what pi's decomposition calls `cacheRead`
    /// — never to `totalTokens`. Measured in a controlled lab whose fake provider reported prompt
    /// tokens of 900, 9,900 and 22,500, and again on a real-credential run where `tokens` read
    /// 2,065 against that message's `totalTokens` of 2,071. So omp under-reports relative to pi by
    /// output plus cache write, and this test fails if `tokens` ever stops meaning prompt-only.
    #[test]
    fn the_omp_18_0_9_fixture_pins_prompt_input_as_the_numerator() {
        const MEASURED: [&str; 2] = crate::omp_session::MEASURED_CONTEXT_VERSIONS;
        assert_eq!(
            MEASURED,
            ["18.0.9", "18.0.3"],
            "the fixture below was captured on these builds"
        );
        let message_usage = json!({
            "input": 22400, "cacheRead": 100, "cacheWrite": 0, "output": 25,
            "totalTokens": 22525,
            "cost": {"total": 0.067605}
        });
        let frame = json!({"type": "context", "reading": {
            "usedTokens": 22500, "windowTokens": 4000, "usedPercent": 562.5,
            "model": "fake-1", "costUsd": 0.067605
        }});

        let record = record_after(&[frame], harness_context::Harness::Omp);

        assert_eq!(record.harness, harness_context::Harness::Omp);
        let prompt =
            message_usage["input"].as_u64().unwrap() + message_usage["cacheRead"].as_u64().unwrap();
        assert_eq!(
            record.used_tokens,
            Some(prompt),
            "omp {MEASURED:?}: the numerator is the assistant message's prompt tokens"
        );
        assert_ne!(
            record.used_tokens,
            message_usage["totalTokens"].as_u64(),
            "omp {MEASURED:?}: publishing totalTokens would be pi's arithmetic under omp's tag, \
             over-reporting the window by output plus cache write"
        );
        assert_eq!(record.window_tokens, Some(4000));
        assert_eq!(
            record.used_percent,
            Some(562.5),
            "carried raw, never clamped"
        );
        assert_eq!(record.cost_usd, Some(0.067605));
        assert_eq!(record.session_total_tokens, None);
    }

    /// pi's honest unknown, end to end (HC-R03). Measured inside pi's own `session_compact`
    /// handler: `getContextUsage()` already reports `{tokens: null, percent: null}` there while
    /// `contextWindow` stays populated, and `getEntries()` already counts the new entry.
    ///
    /// Two things must hold and both are silent failures. The nulls must REPLACE the previous
    /// reading rather than being carried forward — an agent whose window was just emptied must not
    /// still read 90% full. And they must land in the SAME write as the edge: a compaction edge
    /// always writes while a withheld percent has no bucket, so an edge written on its own would
    /// publish the stale pre-compaction numbers beside it and the truth would wait for the
    /// heartbeat.
    #[test]
    fn a_pi_compaction_withholds_the_reading_it_emptied_in_the_same_write() {
        let before = json!({"type": "context", "reading": {
            "usedTokens": 3625, "windowTokens": 4000, "usedPercent": 90.625,
            "model": "fake-1", "costUsd": 0.010905
        }});
        // Verbatim: reason "overflow", and `getEntries()` filtered to compactions already reads 3.
        let compacted = json!({"type": "context",
            "reading": {"usedTokens": null, "windowTokens": 4000, "usedPercent": null,
                        "model": "fake-1", "costUsd": 0.010905},
            "compaction": {"trigger": "overflow", "count": 3}});

        let full = record_after(&[before.clone()], harness_context::Harness::Pi);
        assert_eq!(full.used_percent, Some(90.625));
        assert_eq!(full.compactions, 0);

        let record = record_after(&[before, compacted], harness_context::Harness::Pi);

        assert_eq!(
            record.used_tokens, None,
            "a withheld count is never the previous one"
        );
        assert_eq!(record.used_percent, None, "nor is a withheld percent");
        assert_eq!(
            record.window_tokens,
            Some(4000),
            "pi still knows its denominator"
        );
        assert_eq!(
            record.last_compaction_trigger,
            Some(harness_context::CompactionTrigger::Overflow),
            "pi is the only v1 producer that names its trigger"
        );
        assert_eq!(
            record.compactions, 3,
            "the count is the harness's own durable one, not st2 counting edges"
        );
        assert!(record.last_compaction_ms.is_some());
        // Withholding occupancy says nothing about cost, which the harness did not retract.
        assert_eq!(record.cost_usd, Some(0.010905));
    }

    /// omp's compaction edge carries no `reason` and no `willRetry` — pi 0.84.2 has both — so the
    /// trigger is `unknown`, a legitimate v1 value for three of the five harnesses. omp does name
    /// its auto-compaction "idle" and "threshold" internally, but those words are not projected
    /// onto the event and inventing one would be a claim no capture supports. Unlike pi, omp's
    /// `getContextUsage()` still answers inside the handler, so a real reading rides along.
    #[test]
    fn an_omp_compaction_yields_unknown_because_the_event_names_no_reason() {
        let compacted = json!({"type": "context",
            "reading": {"usedTokens": 8100, "windowTokens": 4000, "usedPercent": 202.5,
                        "model": "fake-1", "costUsd": null},
            "compaction": {"trigger": null, "count": 1}});

        let record = record_after(&[compacted], harness_context::Harness::Omp);

        assert_eq!(
            record.last_compaction_trigger,
            Some(harness_context::CompactionTrigger::Unknown)
        );
        assert_eq!(record.compactions, 1);
        assert_eq!(record.used_tokens, Some(8100));
        assert_eq!(record.cost_usd, None);
    }

    /// A count the extension could not read leaves st2 counting edges itself. That is a weaker
    /// answer — incarnation-scoped rather than harness-durable — and the point is that it is
    /// weaker rather than wrong: the edge still lands, with a trigger.
    #[test]
    fn an_unreadable_durable_count_degrades_to_counting_edges_not_to_losing_them() {
        let edge = json!({"type": "context", "compaction": {"trigger": "manual"}});

        let record = record_after(&[edge.clone(), edge], harness_context::Harness::Pi);

        assert_eq!(record.compactions, 2);
        assert_eq!(
            record.last_compaction_trigger,
            Some(harness_context::CompactionTrigger::Manual)
        );
    }

    /// The decoder's own fail-closed rules. A trigger word this version does not know decodes as
    /// `unknown` and never as a definite one; a frame with neither half is not a frame; and every
    /// other frame type is left to the other decoders.
    #[test]
    fn context_frames_decode_conservatively_or_not_at_all() {
        assert_eq!(
            compaction_trigger(Some("sacrifice")),
            harness_context::CompactionTrigger::Unknown
        );
        assert_eq!(
            compaction_trigger(None),
            harness_context::CompactionTrigger::Unknown
        );
        for word in ["manual", "auto", "threshold", "overflow", "idle"] {
            assert_eq!(compaction_trigger(Some(word)).as_str(), word);
        }

        for frame in [
            json!({"type": "context"}),
            json!({"type": "context", "reading": "nonsense"}),
            json!({"type": "state", "state": "idle"}),
            json!({"reading": {"usedTokens": 1}}),
        ] {
            assert_eq!(context_frame(&frame), None, "frame: {frame}");
        }

        // A reading whose every leg is withheld is still a reading: "the harness told us it does
        // not know" is an observation, and dropping it would leave a stale number looking current.
        let withheld = context_frame(&json!({"type": "context", "reading": {}})).unwrap();
        assert_eq!(withheld.0, Some(harness_context::Reading::default()));
        assert_eq!(withheld.1, None);
    }

    /// Withholding must mean "the harness said it does not know", never "the number arrived in a
    /// JSON shape this decoder was strict about". Both harnesses round today — pi's fallback
    /// estimator is `Math.ceil(chars / 4)` — so a fractional count is not something measured; the
    /// point is that if one ever stops rounding, a strict integer parse would silently discard a
    /// real reading and the record would be indistinguishable from an honest withheld one.
    #[test]
    fn a_fractional_token_count_is_a_reading_not_a_withheld_value() {
        assert_eq!(token_count(&json!(23425)), Some(23425));
        assert_eq!(token_count(&json!(1234.75)), Some(1235));
        assert_eq!(token_count(&json!(0)), Some(0));
        // Not counts: a negative, a non-finite, and a non-number are withheld.
        assert_eq!(token_count(&json!(-1)), None);
        assert_eq!(token_count(&json!("23425")), None);
        assert_eq!(token_count(&Value::Null), None);

        let (reading, _) = context_frame(&json!({"type": "context", "reading": {
            "usedTokens": 1234.75, "windowTokens": 4000, "usedPercent": 30.86
        }}))
        .unwrap();
        assert_eq!(reading.unwrap().used_tokens, Some(1235));
    }

    /// A context frame must never be able to take the channel down or stall the inbox, and the two
    /// axes must stay independent: the numeric record is not consulted by the categorical one and
    /// does not consult it.
    #[test]
    fn context_frames_and_state_frames_are_independent_axes_on_one_wire() {
        let context = json!({"type": "context", "reading": {"usedTokens": 10, "usedPercent": 1.0}});
        let state = json!({"type": "state", "state": "idle"});

        assert!(
            state_observation(&context).is_none(),
            "a context frame is not an observation"
        );
        assert!(
            context_frame(&state).is_none(),
            "a state frame is not a reading"
        );
    }

    /// HC-R13's version pin for pi. pi ships no runtime gate, so the only thing coupling this
    /// repository to a pi build is the flake check that type-checks and runtime-smokes the shipped
    /// asset. If that tarball moves without the fixture moving, the fixture would keep claiming a
    /// measurement of a build nothing in the tree uses any more — the exact silent drift HC-T03
    /// asks a fixture to bound.
    #[test]
    fn the_measured_pi_release_is_the_one_the_extension_gate_pins() {
        let flake = include_str!("../flake.nix");
        let pin = format!(
            "piVersion = \"{}\";",
            crate::pi_session::MEASURED_CONTEXT_VERSION
        );
        assert!(
            flake.contains(&pin),
            "flake.nix must pin the pi release the harness-context fixture measured ({pin})"
        );
    }

    #[test]
    fn channel_content_reuses_the_claude_channel_envelope() {
        assert_eq!(
            channel_content(Some("subject"), "body"),
            "Subject: subject\n\nbody"
        );
        assert_eq!(channel_content(None, "body"), "body");
        assert_eq!(channel_content(Some(""), "body"), "body");
    }

    /// A restarting pi agent has to be told the same three things the Codex and Claude session-start
    /// hooks tell theirs, in the same order — otherwise "restart" means something different per
    /// harness.
    #[test]
    fn session_context_restores_state_ritual_and_unread_work_in_hook_order() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        context::write_now(
            &context::context_dir(agent_dir),
            "Mid-migration on shard 3.",
        )
        .unwrap();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        std::fs::write(
            inbox.join("1787042542238-xex2t4.md"),
            "---\nfrom: h.supervisor\nsubject: deploy check\n---\nVerify staging.\n",
        )
        .unwrap();

        let restored = session_context(agent_dir, "h.worker");

        let state = restored
            .find("Mid-migration on shard 3.")
            .expect("state restored");
        let ritual = restored
            .find("Run the st2 boot ritual")
            .expect("ritual present");
        let unread = restored
            .find("## st2 inbox (1 unread)")
            .expect("unread listed");
        assert!(state < ritual && ritual < unread, "{restored}");
        assert!(
            restored.contains("<context source=\"st2/context/now.md\" agent=\"h.worker\">"),
            "{restored}"
        );
        assert!(restored.contains("Subject: deploy check"), "{restored}");
    }

    /// A fresh agent has no durable state and no mail. It must still get its ritual, and must not be
    /// handed an empty `<context>` envelope describing nothing.
    #[test]
    fn a_fresh_agent_is_told_only_its_ritual() {
        let tmp = tempfile::tempdir().unwrap();

        let restored = session_context(tmp.path(), "h.worker");

        assert!(
            restored.starts_with("Run the st2 boot ritual"),
            "{restored}"
        );
        assert!(!restored.contains("<context"), "{restored}");
        assert!(!restored.contains("st2 inbox"), "{restored}");
    }

    /// The extension is not allowed to choose how a message lands, so the mode has to be on the
    /// frame st2 writes. Pinning it here is what makes #277 a one-place change.
    #[test]
    fn st2_owns_the_delivery_mode_on_the_wire() {
        let frame = message_frame(
            message::Message {
                filename: "1787042542238-xex2t4.md".into(),
                ts_ms: 1_787_042_542_238,
                from: Some("h.supervisor".into()),
                // An unmigrated sender's immutable ID is exactly its `<host>.<identity>` bytes, so
                // the route and the ID coincide here by construction.
                from_id: Some("h.supervisor".into()),
                subject: Some("deploy check".into()),
                in_reply_to: None,
                tags: Vec::new(),
                priority: None,
                idempotency_key: None,
                stream: None,
                event_id: None,
                event_key: None,
                body: "Please verify the staging deploy.".into(),
            },
            "h.worker",
        );

        assert_eq!(frame["type"], "message");
        assert_eq!(frame["deliverAs"], "steer");
        assert_eq!(
            frame["content"],
            "Subject: deploy check\n\nPlease verify the staging deploy."
        );
        assert_eq!(frame["meta"]["identity"], "h.worker");
        // An unthreaded message threads on itself, matching the Claude channel.
        assert_eq!(
            frame["meta"]["threadFilename"],
            frame["meta"]["messageFilename"]
        );
    }

    /// The measured omp 18.1.7 classifications, one row per case in
    /// `docs/vrs/06-omp-driver/.experiments/2026-09-05-omp-provider-credential-rejection.md`.
    ///
    /// This is the oracle that keeps st2 out of provider prose. Every 4xx here reaches `AuthFailed`
    /// — omp sets it from the words `401`, `403`, and `forbidden` as readily as from a status — so
    /// a classifier that stopped at that flag would report the exhausted-credits seat that
    /// motivated this work as a refused credential and send its operator to re-login.
    #[test]
    fn only_omps_own_credential_class_becomes_provider_auth() {
        // (case, errorId, is a rejected credential)
        let cases = [
            ("401 invalid x-api-key", 0x100_1000_u64, true),
            ("401 OAuth invalid_grant", 0x100_1000, true),
            ("403 key lacks permission", 0x100_1000, true),
            ("403 run out of credits", 0x108_1000, false),
            ("403 cyber_policy", 0x100_d000, false),
            ("403 CONCURRENT_LIMIT", 0x102_1000, false),
            ("402 insufficient balance", 0x08_1000, false),
            ("429 rate limit", 0x02_1000, false),
        ];

        for (case, error_id, rejected) in cases {
            let frame = json!({
                "type": "turn",
                "error": {"reason": case, "errorId": error_id},
            });
            let result = turn_result(&frame).expect("a turn frame decodes");
            let observed = turn_observation(&result).expect("a failed turn is an observation");
            assert_eq!(
                observed.state,
                harness_state::Activity::Active,
                "a turn that died on the provider needs an operator, not an idle settle: {case}"
            );
            assert_eq!(observed.blocked_on, harness_state::BlockedOn::None);
            if rejected {
                assert_eq!(
                    observed.reason.as_deref(),
                    Some(PROVIDER_AUTH_REASON),
                    "{case}"
                );
                assert_eq!(
                    provider_auth_edge(&result),
                    Some(ProviderAuthEdge::Rejected),
                    "{case}"
                );
            } else {
                assert_eq!(
                    observed.reason.as_deref(),
                    Some(case),
                    "omp's own prose is the only place a reader learns WHICH 4xx this was: {case}"
                );
                assert_eq!(
                    provider_auth_edge(&result),
                    None,
                    "capacity, policy, and concurrency prove nothing about the credential: {case}"
                );
            }
        }

        // A turn that reached its ordinary end: omp leaves `errorId` at 0 and emits no error at
        // all, which is the only positive proof the provider accepted the credential.
        let ordinary_frame = json!({"type": "turn"});
        let ordinary = turn_result(&ordinary_frame).expect("an ordinary end decodes");
        assert!(
            turn_observation(&ordinary).is_none(),
            "the sampled idle poll still owns the settle edge"
        );
        assert_eq!(
            provider_auth_edge(&ordinary),
            Some(ProviderAuthEdge::Accepted)
        );

        // Silence beats a guess: a bare HTTP status (no `qe.Class` bit) and a missing field are
        // both "this reader cannot classify it", never "the credential is fine".
        for unclassified in [json!(403), json!(0), Value::Null] {
            let frame =
                json!({"type": "turn", "error": {"reason": "403 …", "errorId": unclassified}});
            let result = turn_result(&frame).unwrap();
            assert_eq!(provider_auth_edge(&result), None, "{unclassified}");
            assert_eq!(
                turn_observation(&result).unwrap().reason.as_deref(),
                Some("403 …")
            );
        }

        assert!(
            turn_result(&json!({"type": "state", "state": "idle"})).is_none(),
            "the categorical axis is not a turn result"
        );
    }

    /// The whole point of the record: a refused credential is durable, survives the channel process
    /// that saw it, outranks the delivery failures it causes, and is cleared by exactly one thing —
    /// a turn that reached its ordinary end.
    #[test]
    fn a_rejected_omp_credential_stands_until_a_turn_reaches_its_ordinary_end() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        let record = driver_diagnostic::path(agent_dir);

        let run = |frames: &[&str]| {
            let mut writer =
                harness_state::Writer::new(agent_dir, "h.worker", "omp", Some("h.worker".into()));
            let (tx, rx) = mpsc::channel();
            for frame in frames {
                tx.send(Ok((*frame).to_string())).unwrap();
            }
            drop(tx);
            channel_loop(
                &rx,
                &mut Vec::new(),
                &inbox,
                agent_dir,
                &mut writer,
                None,
                "h.worker",
                &OMP_KIND,
                Duration::from_millis(1),
                Duration::from_secs(60),
            )
            .unwrap();
        };

        run(&[r#"{"type":"turn","error":{"reason":"401 invalid x-api-key","errorId":16781312}}"#]);
        let driver_diagnostic::Observed::Failure(failure) = driver_diagnostic::read(&record) else {
            panic!(
                "a refused omp credential must be a failure: {:?}",
                driver_diagnostic::read(&record)
            )
        };
        assert_eq!(failure.driver, driver_diagnostic::Driver::Omp);
        assert_eq!(failure.stage, driver_diagnostic::Stage::ProviderAuth);
        assert_eq!(
            failure.reason,
            driver_diagnostic::Reason::ProviderAuthRejected
        );
        assert_eq!(failure.source, driver_diagnostic::Source::TurnResult);
        assert!(failure.producer_version.is_none());
        let state: Value = serde_json::from_slice(
            &std::fs::read(harness_state::harness_state_path(agent_dir)).unwrap(),
        )
        .unwrap();
        assert_eq!(state["state"], "active");
        assert_eq!(state["reason"], PROVIDER_AUTH_REASON);

        // A different class in a NEW channel process must not clear it, and neither must the
        // ordinary live traffic that keeps flowing while the seat is wedged.
        run(&[
            r#"{"type":"state","state":"active"}"#,
            r#"{"type":"turn","error":{"reason":"429 rate limit","errorId":135168}}"#,
            r#"{"type":"state","state":"idle"}"#,
        ]);
        assert!(
            matches!(
                driver_diagnostic::read(&record),
                driver_diagnostic::Observed::Failure(_)
            ),
            "only a turn that reached its ordinary end retires this record"
        );

        run(&[r#"{"type":"turn"}"#]);
        assert_eq!(
            driver_diagnostic::read(&record),
            driver_diagnostic::Observed::Absent,
            "the last stage recovering removes the record entirely"
        );
    }

    /// pi's extension has no error-classification field to forward, so this channel has nothing but
    /// provider prose for it — and refuses to publish a credential verdict from prose.
    #[test]
    fn the_pi_channel_publishes_no_credential_verdict() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let mut writer =
            harness_state::Writer::new(agent_dir, "h.worker", "pi", Some("h.worker".into()));
        let (tx, rx) = mpsc::channel();
        tx.send(Ok(
            r#"{"type":"turn","error":{"reason":"401 invalid x-api-key","errorId":16781312}}"#
                .to_string(),
        ))
        .unwrap();
        drop(tx);
        channel_loop(
            &rx,
            &mut Vec::new(),
            &message::inbox_dir(agent_dir),
            agent_dir,
            &mut writer,
            None,
            "h.worker",
            &PI_KIND,
            Duration::from_millis(1),
            Duration::from_secs(60),
        )
        .unwrap();

        assert_eq!(PI_KIND.diagnostic_driver, None);
        assert_eq!(
            driver_diagnostic::read(&driver_diagnostic::path(agent_dir)),
            driver_diagnostic::Observed::Absent
        );
    }

    /// One omp channel loop over a fixed incarnation, so two runs are comparable byte for byte.
    fn omp_record(frames: &[&str]) -> Value {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        let inbox = message::inbox_dir(agent_dir);
        std::fs::create_dir_all(&inbox).unwrap();
        let mut writer =
            harness_state::Writer::new(agent_dir, "h.worker", "omp", Some("h.worker".into()))
                .with_ownership("session-1", 1);
        let (tx, rx) = mpsc::channel();
        for frame in frames {
            tx.send(Ok((*frame).to_string())).unwrap();
        }
        drop(tx);
        channel_loop(
            &rx,
            &mut Vec::new(),
            &inbox,
            agent_dir,
            &mut writer,
            None,
            "h.worker",
            &OMP_KIND,
            Duration::from_millis(1),
            Duration::from_secs(60),
        )
        .unwrap();
        let mut record: Value = serde_json::from_slice(
            &std::fs::read(harness_state::harness_state_path(agent_dir)).unwrap(),
        )
        .unwrap();
        // The two clocks are the only fields two identical runs may legitimately differ on.
        for volatile in ["writtenAtMs", "sinceMs"] {
            record.as_object_mut().unwrap().remove(volatile);
        }
        record
    }

    /// The measured omp 18.1.7 table once more, now as the version 3 condition tuple: exactly one
    /// (category, code, recovery) per class, beside the credential axis it must not disturb.
    ///
    /// The two rows that motivated this whole mapping are asserted by name. `0x1081000` (403 "out
    /// of credits") is `quota` and `0x100d000` (403 `cyber_policy`) is `policy`, even though BOTH
    /// set omp's `AuthFailed` flag: a classifier that stopped at that flag would send the operator
    /// of a wedged, fully-paid seat to re-login.
    #[test]
    fn every_measured_omp_classification_maps_to_one_v3_condition() {
        use harness_state::{FaultCategory as Category, Recovery};
        let observed_at_ms = 1_787_999_000_000;
        // (case, errorId, category, code, recovery, is a rejected credential)
        let cases = [
            (
                "401 invalid x-api-key",
                0x100_1000_u64,
                Category::Authentication,
                "omp/authFailed",
                Recovery::Human,
                true,
            ),
            (
                "401 OAuth invalid_grant",
                0x100_1000,
                Category::Authentication,
                "omp/authFailed",
                Recovery::Human,
                true,
            ),
            (
                "403 key lacks permission",
                0x100_1000,
                Category::Authentication,
                "omp/authFailed",
                Recovery::Human,
                true,
            ),
            (
                "403 run out of credits",
                0x108_1000,
                Category::Quota,
                "omp/usageLimit",
                Recovery::Human,
                false,
            ),
            (
                "402 insufficient balance",
                0x08_1000,
                Category::Quota,
                "omp/usageLimit",
                Recovery::Human,
                false,
            ),
            (
                "403 cyber_policy",
                0x100_d000,
                Category::Policy,
                "omp/accountPolicy",
                Recovery::Human,
                false,
            ),
            (
                "403 CONCURRENT_LIMIT",
                0x102_1000,
                Category::RateLimit,
                "omp/transientExhausted",
                Recovery::Unknown,
                false,
            ),
            (
                "429 rate limit",
                0x02_1000,
                Category::RateLimit,
                "omp/transientExhausted",
                Recovery::Unknown,
                false,
            ),
            // The residual classified row: omp classified it and none of the flags above is set.
            // It is the shape of a provider-side failure rather than a captured case, and it must
            // stay VISIBLE rather than becoming a credential rejection by elimination.
            (
                "500 upstream failure",
                0x00_1000,
                Category::Provider,
                "omp/providerError",
                Recovery::Unknown,
                false,
            ),
        ];

        for (case, error_id, category, code, recovery, rejected) in cases {
            let frame = json!({"type":"turn","error":{"reason":case,"errorId":error_id}});
            let result = turn_result(&frame).expect("a turn frame decodes");
            let fault = turn_fault(&result, observed_at_ms).expect("a failed turn is a condition");
            assert_eq!(fault.category, category, "{case}");
            assert_eq!(fault.code.as_deref(), Some(code), "{case}");
            assert_eq!(fault.recovery, recovery, "{case}");
            assert_eq!(
                fault.observed_at_ms, observed_at_ms,
                "the SEMANTIC clock is the producer's own observation instant: {case}"
            );
            assert_eq!(
                fault.next_observation_due_ms, None,
                "the turn frame carries no deadline, so no omp fault may claim one: {case}"
            );
            assert_eq!(
                fault.detail, None,
                "omp's prose rides the record's `reason`, never the fault's clock-bearing \
                 identity: {case}"
            );
            assert_ne!(
                fault.recovery,
                Recovery::Automatic,
                "an automatic recovery with no deadline can never escalate: {case}"
            );
            // The credential axis is the SAME rule, stated once for two records, and unchanged.
            assert_eq!(
                provider_auth_edge(&result),
                rejected.then_some(ProviderAuthEdge::Rejected),
                "{case}"
            );
            assert_eq!(
                fault.category == Category::Authentication,
                rejected,
                "`authentication` and the credential edge are one rule: {case}"
            );
            // And the legacy observation is byte-for-byte what it always was.
            let observation = turn_observation(&result).expect("a failed turn is an observation");
            assert_eq!(observation.state, harness_state::Activity::Active, "{case}");
            assert_eq!(observation.blocked_on, harness_state::BlockedOn::None, "{case}");
            assert_eq!(
                observation.reason.as_deref(),
                Some(if rejected { PROVIDER_AUTH_REASON } else { case }),
                "{case}"
            );
        }

        // A turn that reached its ordinary end states no fault at all. It is the ONLY positive
        // success edge, and what it authorizes is the blanket clear — never a fault of its own.
        let ordinary_frame = json!({"type": "turn"});
        let ordinary = turn_result(&ordinary_frame).expect("an ordinary end decodes");
        assert!(
            turn_fault(&ordinary, observed_at_ms).is_none(),
            "an ordinary turn end is progress, not a condition"
        );
        assert_eq!(
            provider_auth_edge(&ordinary),
            Some(ProviderAuthEdge::Accepted)
        );
    }

    /// An error omp itself did not classify stays VISIBLE, under the most conservative category
    /// that is still true: the turn died between omp and the provider. `harness` would claim st2's
    /// own plumbing broke and a `clear` would launder a wedged seat; both are false statements.
    #[test]
    fn an_unclassified_error_id_is_a_visible_provider_fault_not_a_credential_rejection() {
        // A bare HTTP status (no `qe.Class` bit), an absent field, and a field this decoder
        // cannot read as a number are all the same thing: no classification.
        for unclassified in [json!(403), json!(0), json!(429), Value::Null, json!("403")] {
            let frame = json!({"type":"turn","error":{"reason":"403 …","errorId":unclassified}});
            let result = turn_result(&frame).expect("a turn frame decodes");
            let fault = turn_fault(&result, 7).expect("an unreadable class is still a fault");
            assert_eq!(fault.category, harness_state::FaultCategory::Provider, "{unclassified}");
            assert_eq!(fault.code.as_deref(), Some("omp/unclassified"), "{unclassified}");
            assert_eq!(fault.recovery, harness_state::Recovery::Unknown, "{unclassified}");
            assert_eq!(
                provider_auth_edge(&result),
                None,
                "silence about the class is not a verdict on the credential: {unclassified}"
            );
        }
    }

    /// The axes are independent: an activity edge, an ask, a compaction, an approval denial, the
    /// negotiation answer, and the conversation statement all state NOTHING about the condition.
    /// A producer forced to pick `clear` on any of them would fabricate health several times a
    /// turn, and a retry in flight — which omp reports by sending no frame at all — would be the
    /// worst of them.
    #[test]
    fn the_edges_that_are_not_faults_state_no_condition() {
        for frame in [
            json!({"type":"state","state":"active"}),
            json!({"type":"state","state":"idle"}),
            json!({"type":"state","state":"active","blockedOn":"human","ask":"question","reason":"Which target?"}),
            json!({"type":"state","state":"active","blockedOn":"human","ask":"permission","reason":"bash"}),
            json!({"type":"state","state":"idle","reason":"approvalDenied"}),
            json!({"type":"context","reading":{"usedPercent":42.0}}),
            json!({"type":"pre_compact"}),
            json!({"type":"client_hello","protocol":2}),
            json!({"type":"conversation","sessionId":"2f8c"}),
            json!({"type":"delivered","meta":{}}),
        ] {
            assert!(
                turn_result(&frame).is_none(),
                "only a turn result carries a condition: {frame}"
            );
        }
        // A denied approval is an interruption, not a fault: the ask is simply over, the word is
        // prose, and the condition axis is untouched.
        let denied =
            state_observation(&json!({"type":"state","state":"idle","reason":"approvalDenied"}))
                .expect("a denial resolves the ask");
        assert_eq!(denied.blocked_on, harness_state::BlockedOn::None);
        assert_eq!(denied.ask, harness_state::Ask::None);
        assert_eq!(denied.reason.as_deref(), Some("approvalDenied"));
    }

    /// The axes are independent in the WRITE, not just in the decode: every activity and ask edge
    /// carries the condition forward `Unchanged`, so a standing fault survives a whole turn of
    /// traffic. And omp's ask axis is positive: it owns both ask surfaces, so `none` is an
    /// observation rather than an absence of one.
    #[test]
    fn an_activity_edge_carries_the_condition_forward_untouched() {
        use harness_state::{AskKind, HumanAsk};
        let rows = [
            (json!({"type":"state","state":"active"}), HumanAsk::None),
            (json!({"type":"state","state":"idle"}), HumanAsk::None),
            (
                json!({"type":"state","state":"idle","reason":"approvalDenied"}),
                HumanAsk::None,
            ),
            (
                json!({"type":"state","state":"active","blockedOn":"human","ask":"question","reason":"Which target?"}),
                HumanAsk::Pending(AskKind::Question),
            ),
            (
                json!({"type":"state","state":"active","blockedOn":"human","ask":"permission","reason":"bash"}),
                HumanAsk::Pending(AskKind::Permission),
            ),
            (
                json!({"type":"state","state":"active","blockedOn":"human","ask":"sacrifice"}),
                HumanAsk::Pending(AskKind::Unknown),
            ),
        ];
        for (raw, ask) in rows {
            let observation = state_observation(&raw).expect("a state frame decodes");
            let published = kind_frame(&OMP_KIND, observation);
            assert!(
                matches!(
                    published.condition,
                    harness_state::ConditionReport::Unchanged
                ),
                "an activity edge has learned nothing about the provider: {raw}"
            );
            assert_eq!(published.ask, ask, "{raw}");
            assert_eq!(published.input_buffer, harness_state::InputBuffer::Unknown);
            assert_eq!(
                published.conversation, None,
                "the axis is stated from wire evidence only: {raw}"
            );
        }

        // A failed turn is the one row that states a condition, and it still says `active`: the
        // seat needs an operator, and `idle` would read as a healthy yield.
        let credits = json!({
            "type":"turn","error":{"reason":"403 run out of credits","errorId":17305600}
        });
        let result = turn_result(&credits).unwrap();
        let faulted = kind_frame(&OMP_KIND, turn_observation(&result).unwrap());
        assert_eq!(faulted.state, harness_state::Activity::Active);
        assert_eq!(faulted.ask, HumanAsk::None);
        assert_eq!(
            OMP_KIND.conversation, None,
            "omp never states `unsupported`: it demonstrably has sessions"
        );
        assert_eq!(OMP_KIND.default_ask, HumanAsk::None);
    }

    /// The condition axis is not writable as `absent`, so a virgin version 3 record refuses the
    /// first activity-only frame — and ONLY that refusal authorizes restating it as `clear`. Every
    /// other refusal is a fact about ownership or a terminal record that a restatement cannot fix,
    /// and a frame that already states a condition is never rewritten into one that does not.
    #[test]
    fn only_an_unstated_axis_is_restated_as_clear() {
        let observation = state_observation(&json!({"type":"state","state":"idle"})).unwrap();
        let frame = kind_frame(&OMP_KIND, observation);
        let unstated = harness_state::WriteOutcome::Refused(harness_state::Refusal::Unstated);
        let restated = restate_condition(&frame, &unstated).expect("an unstated axis is stated");
        assert!(matches!(
            restated.condition,
            harness_state::ConditionReport::Clear
        ));
        assert_eq!(restated.state, frame.state, "only the condition axis moves");
        assert_eq!(restated.ask, frame.ask);
        assert_eq!(restated.input_buffer, frame.input_buffer);
        assert_eq!(restated.conversation, frame.conversation);
        assert_eq!(restated.reason, frame.reason);

        for outcome in [
            harness_state::WriteOutcome::Landed,
            harness_state::WriteOutcome::Coalesced,
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Terminal),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Unobserved),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Unfenced),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Unclaimed),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Unreadable),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::ConditionMismatch {
                current: None,
            }),
            harness_state::WriteOutcome::Refused(harness_state::Refusal::Superseded {
                on_disk_seq: 4,
                ours: 1,
            }),
        ] {
            assert!(
                restate_condition(&frame, &outcome).is_none(),
                "nothing else authorizes stating the axis: {outcome:?}"
            );
        }

        // A frame that already carries a fault is never rewritten into one that clears it.
        let mut faulted = frame.clone();
        faulted.condition =
            harness_state::ConditionReport::Fault(pre_compact_fault(message::now_ms()));
        assert!(restate_condition(&faulted, &unstated).is_none());
    }

    /// Which wire is written is the writer's business; negotiation narrows two axes, not the
    /// write. Under version 3 an un-negotiated peer is still published — its faults ride the same
    /// typed turn frame, so withholding the tuple would hide a wedged seat — but the ask axis
    /// reads `unknown`, because the promise that makes a positive `none` provable (retiring a
    /// never-answered ask on a turn boundary) was never made, and no conversation is claimed.
    #[test]
    fn an_un_negotiated_peer_is_published_without_the_promoted_axes() {
        use harness_state::{AskKind, ConversationState, HumanAsk};
        let link = ConversationState::Linked(harness_state::ConversationClaim {
            driver: "omp".to_owned(),
            conversation: "2f8c-4d11".to_owned(),
            history_mutability: harness_state::HistoryMutability::Rewritable,
            capability_evidence: harness_state::CapabilityEvidence::Probed,
            verified_through_ms: 1_787_999_000_000,
        });
        for raw in [
            json!({"type":"state","state":"idle"}),
            json!({"type":"state","state":"active"}),
            json!({"type":"state","state":"active","blockedOn":"human","ask":"question","reason":"Which target?"}),
        ] {
            let observation = state_observation(&raw).unwrap();
            let bare = connection_frame(&OMP_KIND, observation.clone(), false, Some(link.clone()));
            let legacy_pending = observation.blocked_on == harness_state::BlockedOn::Human;
            if legacy_pending {
                assert_eq!(
                    bare.ask,
                    kind_frame(&OMP_KIND, observation.clone()).ask,
                    "a waiting human is equally proven on either protocol: {raw}"
                );
                assert!(bare.ask.pending().is_some(), "{raw}");
            } else {
                assert_eq!(
                    bare.ask,
                    HumanAsk::Unknown,
                    "only a positive `none` is unprovable without the retirement promise: {raw}"
                );
            }
            assert_eq!(
                bare.conversation, None,
                "no promise, no claimed conversation: {raw}"
            );
            assert!(
                matches!(bare.condition, harness_state::ConditionReport::Unchanged),
                "{raw}"
            );
            assert_eq!(
                bare.state,
                state_observation(&raw).unwrap().state,
                "the activity axis is stated on every connection: {raw}"
            );

            // The same frame from a negotiated asset states both promoted axes.
            let promoted = connection_frame(&OMP_KIND, observation, true, Some(link.clone()));
            assert_eq!(promoted.conversation, Some(link.clone()), "{raw}");
            assert_ne!(promoted.ask, HumanAsk::Unknown, "{raw}");
        }
        assert_eq!(
            connection_frame(
                &OMP_KIND,
                state_observation(&json!({"type":"state","state":"idle"})).unwrap(),
                true,
                None,
            )
            .ask,
            HumanAsk::None,
            "omp owns both ask surfaces, so a negotiated `none` is a positive observation"
        );
        assert_eq!(
            connection_frame(
                &OMP_KIND,
                state_observation(
                    &json!({"type":"state","state":"active","blockedOn":"human","ask":"permission"})
                )
                .unwrap(),
                true,
                None,
            )
            .ask,
            HumanAsk::Pending(AskKind::Permission)
        );
        // A blocked frame with no nameable kind is a waiting human whose question is unstated —
        // `Pending(Unknown)` — and never a dropped ask.
        assert_eq!(
            connection_frame(
                &OMP_KIND,
                state_observation(&json!({"type":"state","state":"active","blockedOn":"human"}))
                    .unwrap(),
                false,
                None,
            )
            .ask,
            HumanAsk::Pending(AskKind::Unknown)
        );

        // A fault is stated on either connection: it rides the typed turn frame, which every
        // protocol version sends identically.
        let credits = json!({"type":"turn","error":{"reason":"403 run out of credits","errorId":17305600}});
        let result = turn_result(&credits).unwrap();
        assert!(turn_fault(&result, 5).is_some());
        // A driver-level capability claim is not a negotiated axis: pi's `unsupported` is a fact
        // about pi and stands on every connection.
        assert_eq!(
            connection_frame(
                &PI_KIND,
                state_observation(&json!({"type":"state","state":"idle"})).unwrap(),
                false,
                None,
            )
            .conversation,
            Some(ConversationState::Unsupported)
        );
    }

    /// The mixed-version seat that motivates the whole downgrade rule: a version 3 record written
    /// for an UN-NEGOTIATED asset that is blocked on a human must still summon one. The ask
    /// survives the downgrade, so the shared disposition reads `waitingHuman` / `now` / `answer` —
    /// the same verdict the legacy projection of that frame produces, which is the property that
    /// makes the version 3 rollout invisible to an operator.
    #[test]
    fn an_un_negotiated_blocked_frame_still_summons_a_human() {
        let raw = json!({
            "type":"state","state":"active","blockedOn":"human","ask":"question",
            "reason":"Which deployment target?"
        });
        let observation = state_observation(&raw).unwrap();
        let published = connection_frame(&OMP_KIND, observation.clone(), false, None);
        assert_eq!(
            published.ask,
            harness_state::HumanAsk::Pending(harness_state::AskKind::Question)
        );

        // The record such a frame projects, read back through the shared disposition. Nothing is
        // faulted and no diagnostic stands: the ask alone must carry the verdict.
        let observed = harness_state::Observed {
            state: published.state,
            blocked_on: harness_state::BlockedOn::Human,
            input_buffer: published.input_buffer,
            ask: harness_state::Ask::Question,
            harness: Some("omp".to_owned()),
            since_ms: Some(message::now_ms()),
            exit: None,
            reason: published.reason.clone(),
            subject: None,
            schema: Some(harness_state::SCHEMA_V3.to_owned()),
            indeterminacy: None,
            condition: harness_state::ConditionView::Clear,
            human_ask: published.ask,
            conversation: None,
        };
        let disposition =
            harness_state::disposition(Some(&observed), &driver_diagnostic::Observed::Absent);
        assert_eq!(
            disposition.state,
            harness_state::DispositionState::WaitingHuman
        );
        assert_eq!(disposition.attention, harness_state::Attention::Now);
        assert_eq!(
            disposition.primary_action,
            harness_state::PrimaryAction::Answer
        );

        // Had the downgrade swallowed the pending ask, the same seat would read as merely worth
        // observing — nobody would be summoned.
        let muted = harness_state::Observed {
            human_ask: harness_state::HumanAsk::Unknown,
            ..observed
        };
        let muted =
            harness_state::disposition(Some(&muted), &driver_diagnostic::Observed::Absent);
        assert_ne!(
            muted.state,
            harness_state::DispositionState::WaitingHuman,
            "this is the regression the downgrade rule exists to prevent"
        );
    }

    /// The adapter-owned fault outlives provider progress. A completed turn is evidence about the
    /// PROVIDER; st2's own failed pre-compact write is a different fact, and only the next
    /// successful pre-compact edge retires it — by category and full code, never by the blanket
    /// clear that a turn authorizes.
    #[test]
    fn a_completed_turn_never_retires_the_adapter_owned_fault() {
        let mut standing: Option<harness_state::FaultReport> = None;

        // With nothing of ours standing, a completed turn clears the whole axis.
        assert_eq!(
            turn_completed_edge(standing.as_ref()),
            ConditionEdge::ClearAll(harness_state::ProgressProof::TurnCompleted)
        );

        // The stub write fails: the fault is raised AND remembered.
        let raised = pre_compact_edge(false, 1_787_999_000_000);
        let ConditionEdge::Raise(fault) = &raised else {
            panic!("a failed stub write raises: {raised:?}")
        };
        assert_eq!(fault.key(), pre_compact_fault_key());
        standing = Some(fault.clone());

        // Two ordinary turn ends later it still stands, restated rather than swept up — and
        // restating the same fault is what preserves the instant it was first observed.
        for _turn in 0..2 {
            assert_eq!(
                turn_completed_edge(standing.as_ref()),
                ConditionEdge::Raise(fault.clone()),
                "a working provider says nothing about st2's own failed write"
            );
        }
        // Even a provider fault that displaced it in the record's single condition slot does not
        // retire it: the next completed turn restates ours rather than clearing everything.
        assert_ne!(
            turn_completed_edge(standing.as_ref()),
            ConditionEdge::ClearAll(harness_state::ProgressProof::TurnCompleted)
        );

        // Only the successful edge retires it, and only by its exact key.
        let recovered = pre_compact_edge(true, 1_787_999_100_000);
        assert_eq!(
            recovered,
            ConditionEdge::ClearPaired(pre_compact_fault_key())
        );
        standing = None;
        assert_eq!(
            turn_completed_edge(standing.as_ref()),
            ConditionEdge::ClearAll(harness_state::ProgressProof::TurnCompleted)
        );
    }

    /// The negotiated vocabulary is inert on the version 2 wire in BOTH directions: the denial
    /// prose a negotiated asset narrates is withheld rather than becoming a novel `reason` on an
    /// unblocked frame, while every reason version 2 already carried rides through verbatim.
    #[test]
    fn the_denial_diagnostic_never_reaches_the_version_two_wire() {
        let denial = state_observation(&json!({
            "type":"state","state":"idle","reason":"approvalDenied"
        }))
        .unwrap();
        assert_eq!(denial.reason.as_deref(), Some(APPROVAL_DENIED_REASON));
        assert_eq!(
            legacy_observation(denial).reason, None,
            "version 2 has no ask axis that makes this word meaningful"
        );

        // Everything the version 2 wire already said keeps saying it.
        for raw in [
            json!({"type":"state","state":"active","blockedOn":"human","ask":"permission","reason":"bash"}),
            json!({"type":"state","state":"active","blockedOn":"human","ask":"question","reason":"Which target?"}),
            json!({"type":"state","state":"active","blockedOn":"human","ask":"permission","reason":"approvalDenied"}),
        ] {
            let observation = state_observation(&raw).unwrap();
            let reason = observation.reason.clone();
            assert_eq!(
                legacy_observation(observation).reason,
                reason,
                "a blocked frame's prose is untouched: {raw}"
            );
        }
        let credits = json!({"type":"turn","error":{"reason":"403 run out of credits","errorId":17305600}});
        let faulted = turn_observation(&turn_result(&credits).unwrap()).unwrap();
        assert_eq!(
            legacy_observation(faulted).reason.as_deref(),
            Some("403 run out of credits"),
            "a turn error's own words are how a reader learns which 4xx it was"
        );
        let recovery = harness_state::Observation::new(
            harness_state::Activity::Active,
            harness_state::BlockedOn::None,
            harness_state::InputBuffer::Unknown,
        )
        .with_reason(PRE_COMPACT_ERROR_REASON);
        assert_eq!(
            legacy_observation(recovery).reason.as_deref(),
            Some(PRE_COMPACT_ERROR_REASON)
        );

        // And end to end: a denial leaves a record indistinguishable from the plain idle frame it
        // resolved to, so no reader pinned to version 2 sees a new field.
        assert_eq!(
            omp_record(&[
                r#"{"type":"client_hello","protocol":2}"#,
                r#"{"type":"state","state":"idle","reason":"approvalDenied"}"#,
            ]),
            omp_record(&[r#"{"type":"state","state":"idle"}"#]),
        );
    }

    /// The adapter-owned harness fault, and the exactness of its clear. A compaction whose stub
    /// write now succeeds retires THAT fault and only that fault: a standing credential rejection
    /// must survive it, so the key names the category AND the full code — a category-wide key
    /// would be how one healthy compaction silences a wedged seat.
    #[test]
    fn the_pre_compact_fault_is_cleared_by_category_and_full_code() {
        let fault = pre_compact_fault(9);
        assert_eq!(fault.category, harness_state::FaultCategory::Harness);
        assert_eq!(fault.recovery, harness_state::Recovery::Human);
        assert_eq!(
            fault.code.as_deref(),
            Some("omp/preCompactContextWriteFailed")
        );
        assert_eq!(fault.observed_at_ms, 9);
        assert_eq!(fault.next_observation_due_ms, None);
        assert_eq!(pre_compact_fault_key(), fault.key());
        assert_ne!(
            pre_compact_fault_key(),
            harness_state::FaultKey::new(harness_state::FaultCategory::Harness),
            "a codeless key matches a codeless fault, which is not this one"
        );
        let credential = turn_fault(
            &turn_result(&json!({"type":"turn","error":{"errorId":0x100_1000}})).unwrap(),
            9,
        )
        .unwrap();
        assert_ne!(
            pre_compact_fault_key(),
            credential.key(),
            "a healthy compaction must not clear a refused credential"
        );
    }

    /// The conversation axis is populated from omp's OWN typed evidence — the `sessionId` measured
    /// on both halves of the approval pair — and from nothing else. Before one is observed the
    /// axis is omitted; it is never `Unsupported`, because omp demonstrably has sessions and
    /// claiming otherwise would be a false capability claim.
    #[test]
    fn a_conversation_is_linked_only_from_typed_session_evidence() {
        let state = conversation_claim(
            &json!({"type":"conversation","sessionId":"  2f8c-4d11  "}),
            1_787_999_000_000,
        )
        .expect("a session id is a link");
        let link = match state {
            harness_state::ConversationState::Linked(link) => link,
            other => panic!("omp states a LINK or nothing at all: {other:?}"),
        };
        assert_eq!(link.driver, "omp");
        assert_eq!(link.conversation, "2f8c-4d11");
        assert_eq!(
            link.history_mutability,
            harness_state::HistoryMutability::Rewritable,
            "omp compacts its own session store, so a prefix read once may be gone"
        );
        assert_eq!(
            link.capability_evidence,
            harness_state::CapabilityEvidence::Probed,
            "read off a live event, not declared from typings"
        );
        assert_eq!(link.verified_through_ms, 1_787_999_000_000);

        for frame in [
            json!({"type":"conversation"}),
            json!({"type":"conversation","sessionId":"   "}),
            json!({"type":"conversation","sessionId":42}),
            json!({"type":"state","state":"idle","sessionId":"2f8c"}),
        ] {
            assert!(
                conversation_claim(&frame, 1_787_999_000_000).is_none(),
                "nothing to prove, nothing to state: {frame}"
            );
        }
        assert!(
            conversation_claim(&json!({"type":"conversation","sessionId":"2f8c"}), 0).is_none(),
            "a link with no positive verification bound is refused at the write boundary"
        );
    }

    /// Negotiation. st2's hello version never rises — the asset refuses a hello it cannot read,
    /// and a refusal costs that seat its mail — so the offer is additive and the AGREEMENT is the
    /// asset's answer. Anything else, including an answer naming a version st2 never offered, is
    /// dropped like every other frame this channel cannot vouch for.
    #[test]
    fn only_an_answer_to_the_offer_negotiates_the_condition_axis() {
        assert_eq!(PROTOCOL, 1);
        assert_eq!(
            negotiated_protocol(&json!({"type":"client_hello","protocol":2})),
            Some(PROTOCOL_CONDITION_AXIS)
        );
        for frame in [
            json!({"type":"client_hello"}),
            json!({"type":"client_hello","protocol":1}),
            json!({"type":"client_hello","protocol":3}),
            json!({"type":"client_hello","protocol":"2"}),
            json!({"type":"client_hello","protocol":-2}),
            json!({"type":"state","state":"idle","protocol":2}),
            json!({"protocol":2}),
        ] {
            assert_eq!(negotiated_protocol(&frame), None, "frame: {frame}");
        }
    }

    /// While this build's writer emits version 2 the whole negotiated vocabulary is INERT: the
    /// answer and the conversation statement change no byte of the record, because the version 2
    /// wire has nowhere to carry them and this record has exactly one source of truth. That is
    /// what makes the adapter safe to land before the writer selector flips.
    #[test]
    fn a_negotiated_peers_new_frames_never_reach_the_version_two_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = harness_state::Writer::new(tmp.path(), "h.worker", "omp", Some("w".into()));
        assert_eq!(
            writer.writes_condition_axis(),
            false,
            "this test is the proof of the version 2 projection; it is meaningless once the \
             selector flips"
        );

        let legacy = omp_record(&[r#"{"type":"state","state":"active"}"#]);
        let negotiated = omp_record(&[
            r#"{"type":"client_hello","protocol":2}"#,
            r#"{"type":"conversation","sessionId":"2f8c-4d11"}"#,
            r#"{"type":"state","state":"active"}"#,
        ]);
        assert_eq!(legacy, negotiated);
        assert_eq!(legacy["schema"], harness_state::SCHEMA_V2);
        assert_eq!(legacy["state"], "active");
        assert_eq!(legacy["blockedOn"], "none");
        assert!(
            legacy.get("condition").is_none() && legacy.get("conversationRef").is_none(),
            "neither axis exists on this wire: {legacy}"
        );

        // A failed turn still publishes exactly the legacy row it always did — the condition it
        // now also implies is representable nowhere, so it changes nothing here.
        let faulted = omp_record(&[
            r#"{"type":"client_hello","protocol":2}"#,
            r#"{"type":"turn","error":{"reason":"401 invalid x-api-key","errorId":16781312}}"#,
        ]);
        assert_eq!(faulted["state"], "active");
        assert_eq!(faulted["reason"], PROVIDER_AUTH_REASON);
        assert!(faulted.get("condition").is_none());
    }

    /// The whole negotiated vocabulary respects the incarnation's last word. `src/omp_session.rs`
    /// alone writes `ended`, and every frame the extension queued before the wrapper reaped the
    /// session — the answer, an activity edge, a failed turn, the conversation statement, and the
    /// success edge that would otherwise clear everything — is refused rather than resurrecting
    /// a session nobody is watching.
    #[test]
    fn no_negotiated_frame_resurrects_the_wrappers_terminal_omp_record() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path();
        std::fs::create_dir_all(message::inbox_dir(agent_dir)).unwrap();
        let record = harness_state::harness_state_path(agent_dir);
        // The wrapper mints the token and the channel adopts it: that sharing is what makes the
        // wrapper's terminal record this session's last word rather than a foreign one.
        let session = harness_state::session_token();
        let mut channel_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "omp", Some("h.worker".into()))
                .with_session(session.clone());
        let mut wrapper_writer =
            harness_state::Writer::new(agent_dir, "h.worker", "omp", Some("h.worker".into()))
                .with_session(session);
        wrapper_writer.ended("exit 0").unwrap();
        let terminal = std::fs::read(&record).unwrap();

        let (tx, rx) = mpsc::channel();
        for frame in [
            r#"{"type":"client_hello","protocol":2}"#,
            r#"{"type":"state","state":"active"}"#,
            r#"{"type":"turn","error":{"reason":"401 invalid x-api-key","errorId":16781312}}"#,
            r#"{"type":"conversation","sessionId":"2f8c-4d11"}"#,
            r#"{"type":"turn"}"#,
        ] {
            tx.send(Ok(frame.to_string())).unwrap();
        }
        drop(tx);
        channel_loop(
            &rx,
            &mut Vec::new(),
            &message::inbox_dir(agent_dir),
            agent_dir,
            &mut channel_writer,
            None,
            "h.worker",
            &OMP_KIND,
            Duration::from_millis(2),
            Duration::from_secs(60),
        )
        .unwrap();

        assert_eq!(
            std::fs::read(&record).unwrap(),
            terminal,
            "a refused write changes no byte"
        );
    }
}
