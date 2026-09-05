//! Controlled Codex app-server launch and persistent thread ownership.
//!
//! Native delivery cannot infer a thread from cwd, process, PTY, or `thread/list`. This module
//! starts a dedicated provider daemon, initializes an observer connection before the interactive
//! client starts, and binds a typed start notification or successful resume response to the exact
//! wrapper process incarnation that owns the PTY launch. On resume, the owning TUI must first make
//! the preserved thread visible in the provider's loaded-thread inventory. Its control watcher persists
//! delivery-relevant thread and turn state. The native delivery layer selects one durable FIFO
//! inbox head and submits typed input only when that state proves an idle or one exact regular
//! active turn.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::io::AsRawFd as _;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tungstenite::{Message as WebSocketMessage, WebSocket};

use crate::{
    delivery_ledger, ding, driver_diagnostic, harness_context, harness_state, message, run, status,
};

const REQUIRED_CODEX_CLIENT_REQUESTS: &[&str] = &[
    "hooks/list",
    "initialize",
    "thread/loaded/list",
    "thread/resume",
    "turn/start",
    "turn/steer",
];
const REQUIRED_CODEX_CLIENT_NOTIFICATIONS: &[&str] = &["initialized"];
const REQUIRED_CODEX_SERVER_NOTIFICATIONS: &[&str] = &[
    "item/completed",
    "item/started",
    "thread/started",
    "thread/status/changed",
    "turn/completed",
    "turn/started",
];
// The control observer does not answer server requests. A listed request is reviewed and safe to
// ignore. An unlisted request creates a delivery hold until the thread reports a safe status.
const CLASSIFIED_CODEX_SERVER_REQUESTS: &[&str] = &[
    "account/chatgptAuthTokens/refresh",
    "applyPatchApproval",
    "attestation/generate",
    "currentTime/read",
    "execCommandApproval",
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/call",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
];
// A listed item is reviewed and safe to ignore unless `observe` handles it explicitly. An unlisted
// item creates a delivery hold until the thread reports a safe status.
const CLASSIFIED_CODEX_THREAD_ITEMS: &[&str] = &[
    "agentMessage",
    "collabAgentToolCall",
    "commandExecution",
    "contextCompaction",
    "dynamicToolCall",
    "enteredReviewMode",
    "exitedReviewMode",
    "fileChange",
    "functionCallOutput",
    "hookPrompt",
    "imageGeneration",
    "imageView",
    "mcpToolCall",
    "plan",
    "reasoning",
    "sleep",
    "subAgentActivity",
    "userMessage",
    "webSearch",
];
const RUNTIME_SCHEMA: &str = "st2.codex-runtime.v1";
const BINDING_SCHEMA: &str = "st2.codex-thread-binding.v1";
const CONTROL_STATE_SCHEMA: &str = "st2.codex-control-state.v1";
const WRAPPER_DIAGNOSTIC_SCHEMA: &str = "st2.codex-wrapper-diagnostic.v1";
const CONTROL_TUI_LOADED_REQUEST_ID: u64 = 0;
const CONTROL_SUBSCRIBE_REQUEST_ID: u64 = 1;
const FIRST_DELIVERY_REQUEST_ID: u64 = 2;
const HOOK_TRUST_PREFLIGHT_REQUEST_ID: u64 = 1;
// The inner provider result must reach the wrapper before the outer ownership wait expires.
const TUI_LOADED_TIMEOUT: Duration = Duration::from_secs(15);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_POLL: Duration = Duration::from_millis(100);
const INBOX_REFRESH_FALLBACK: Duration = Duration::from_secs(15);
const SOCKET_PATH_BUDGET: usize = 96;

struct WrapperDiagnostics {
    file: File,
    agent: String,
    runtime_id: String,
}

impl WrapperDiagnostics {
    fn open(state_dir: &Path, agent: &str, runtime_id: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(state_dir.join("wrapper.log"))?;
        Ok(Self {
            file,
            agent: agent.to_string(),
            runtime_id: runtime_id.to_string(),
        })
    }

    fn record(&mut self, stage: &str, detail: Value) -> Result<()> {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis();
        serde_json::to_writer(
            &mut self.file,
            &json!({
                "schema": WRAPPER_DIAGNOSTIC_SCHEMA,
                "unixMs": unix_ms,
                "agent": self.agent,
                "runtimeId": self.runtime_id,
                "stage": stage,
                "detail": detail,
            }),
        )?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexRuntime {
    schema: String,
    agent: String,
    runtime_id: String,
    incarnation: String,
}

impl CodexRuntime {
    fn fresh(agent: String, runtime_id: String) -> Result<Self> {
        Ok(Self {
            schema: RUNTIME_SCHEMA.to_string(),
            agent,
            runtime_id,
            incarnation: random_token()?,
        })
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexThreadBinding {
    schema: String,
    agent: String,
    runtime_id: String,
    runtime_incarnation: String,
    thread_id: String,
}

impl CodexThreadBinding {
    fn new(runtime: &CodexRuntime, thread_id: String) -> Self {
        Self {
            schema: BINDING_SCHEMA.to_string(),
            agent: runtime.agent.clone(),
            runtime_id: runtime.runtime_id.clone(),
            runtime_incarnation: runtime.incarnation.clone(),
            thread_id,
        }
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn runtime_incarnation(&self) -> &str {
        &self.runtime_incarnation
    }
}

/// The latest delivery-relevant state observed on the bound app-server control stream.
///
/// `Active` permits `turn/steer`: its turn ID came from the latest unmatched `turn/started` event.
/// `Idle` and `TerminalError` permit `turn/start`. Every `Held` state blocks delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CodexObservedState {
    AwaitingStatus,
    Idle,
    TerminalError {
        reason: CodexTerminalError,
    },
    Active {
        #[serde(rename = "turnId")]
        turn_id: String,
    },
    Held {
        reason: CodexHoldReason,
        #[serde(rename = "turnId")]
        turn_id: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CodexHoldReason {
    ActiveWithoutTurn,
    ConflictingTurn,
    Review,
    Compaction,
    UnknownProtocol,
    NotLoaded,
    SystemError,
    UnknownStatus,
    WaitingOnApproval,
    WaitingOnUserInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CodexTerminalError {
    SystemError,
    ProviderAuthRejected,
}

/// The `CodexErrorInfo` word that names a rejected provider credential.
///
/// It is the 401/invalid-credential arm of Codex's own closed error vocabulary and is distinct
/// from both quota words (`usageLimitExceeded`, `rateLimitExceeded`) — the protocol gate pins all
/// three present so a release that merged them refuses the launch instead of silently making st2
/// call an exhausted allowance a rejected credential.
const CODEX_PROVIDER_AUTH_REJECTED: &str = "unauthorized";

/// What one `turn/completed` notification proves about this thread's provider credential.
///
/// `Turn.status` is required and `Turn.error` is populated only on `failed`, so both edges come
/// from the notification st2 already consumes — no second signal, and no inference from prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexTurnOutcome {
    /// `completed` — the provider accepted the credential for this turn.
    Accepted,
    /// `failed` with `error.codexErrorInfo: unauthorized`.
    ProviderAuthRejected,
    /// `interrupted`, `inProgress`, or a failure this version does not classify: no evidence
    /// either way, so a standing rejection must stand.
    Indeterminate,
}

fn codex_turn_outcome(turn: Option<&Value>) -> CodexTurnOutcome {
    let Some(turn) = turn else {
        return CodexTurnOutcome::Indeterminate;
    };
    match turn.get("status").and_then(Value::as_str) {
        Some("completed") => CodexTurnOutcome::Accepted,
        Some("failed")
            if turn
                .pointer("/error/codexErrorInfo")
                .and_then(Value::as_str)
                == Some(CODEX_PROVIDER_AUTH_REJECTED) =>
        {
            CodexTurnOutcome::ProviderAuthRejected
        }
        _ => CodexTurnOutcome::Indeterminate,
    }
}

impl CodexObservedState {
    /// Driver-side projection into the generic observed-harness-state vocabulary (#162). `Held` is
    /// a delivery predicate — the complement of steerable — and never leaks into the published
    /// record: holds Codex positively reported as work project to `active` (with the human-blocking
    /// ones setting the blocked axis), while holds that only mean "st2 cannot currently prove
    /// anything" project to `None`, the indeterminate observation that writes nothing.
    pub fn harness_observation(&self) -> Option<harness_state::Observation> {
        use crate::harness_state::{Activity, Ask, BlockedOn, InputBuffer, Observation};
        let observation = |state, blocked_on| {
            // This producer reads the app-server control stream and cannot see the composer.
            Observation::new(state, blocked_on, InputBuffer::Unknown)
        };
        match self {
            CodexObservedState::AwaitingStatus => None,
            CodexObservedState::Idle => Some(observation(Activity::Idle, BlockedOn::None)),
            // Both terminals project to `ended`, exactly as before; the reason is what names the
            // cause. `providerAuth` is the same word OpenCode's `ProviderAuthError` already
            // publishes, so one roster consumer classifies the credential class across harnesses.
            CodexObservedState::TerminalError { reason } => Some(
                observation(Activity::Ended, BlockedOn::None).with_reason(match reason {
                    CodexTerminalError::SystemError => "systemError",
                    CodexTerminalError::ProviderAuthRejected => "providerAuth",
                }),
            ),
            CodexObservedState::Active { .. } => {
                Some(observation(Activity::Active, BlockedOn::None))
            }
            CodexObservedState::Held { reason, .. } => match reason {
                // Review's enter and exit are MODEL-emitted items inside a running turn
                // (`enteredReviewMode`/`exitedReviewMode`, released by `observe_hold_released`):
                // nothing awaits a human, so the observed record reports plain activity. The
                // delivery hold is untouched — `Held` still blocks steer — and `review` stays a
                // reserved ask word no producer emits.
                CodexHoldReason::Review => {
                    Some(observation(Activity::Active, BlockedOn::None).with_reason("review"))
                }
                CodexHoldReason::WaitingOnApproval => Some(
                    observation(Activity::Active, BlockedOn::Human)
                        .with_ask(Ask::Permission)
                        .with_reason("waitingOnApproval"),
                ),
                CodexHoldReason::WaitingOnUserInput => Some(
                    observation(Activity::Active, BlockedOn::Human)
                        .with_ask(Ask::Question)
                        .with_reason("waitingOnUserInput"),
                ),
                CodexHoldReason::Compaction => {
                    Some(observation(Activity::Active, BlockedOn::None).with_reason("compaction"))
                }
                CodexHoldReason::UnknownProtocol => Some(
                    observation(Activity::Active, BlockedOn::None).with_reason("unknownProtocol"),
                ),
                // Codex positively reported active; st2 merely cannot name a steerable turn.
                CodexHoldReason::ActiveWithoutTurn => Some(
                    observation(Activity::Active, BlockedOn::None).with_reason("activeWithoutTurn"),
                ),
                CodexHoldReason::ConflictingTurn => Some(
                    observation(Activity::Active, BlockedOn::None).with_reason("conflictingTurn"),
                ),
                CodexHoldReason::NotLoaded
                | CodexHoldReason::SystemError
                | CodexHoldReason::UnknownStatus => None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexControlState {
    schema: String,
    agent: String,
    runtime_id: String,
    runtime_incarnation: String,
    thread_id: String,
    subscribed: bool,
    observed: CodexObservedState,
}

#[derive(Debug, Clone)]
struct CodexDeliveryConfig {
    catalog_root: PathBuf,
    agent_dir: PathBuf,
    inbox: PathBuf,
    identity: String,
    this_host: String,
    supervisor: Option<String>,
    /// The codex-cli version the protocol gate admitted, carried for the native-driver
    /// diagnostic's `producerVersion`. `None` only in tests that build a config without a gate.
    producer_version: Option<String>,
}

impl CodexDeliveryConfig {
    fn resolve(catalog_root: &Path, identity: &str) -> Result<Self> {
        let this_host = run::detect_host();
        let agent_dir = message::resolve_agent_dir(catalog_root, identity, &this_host)?
            .with_context(|| {
                format!(
                    "Codex native delivery agent '{identity}' is not declared in {}",
                    catalog_root.display()
                )
            })?;
        let supervisor = crate::discover(catalog_root)
            .specs
            .into_iter()
            .find(|spec| spec.path.parent() == Some(agent_dir.as_path()))
            .and_then(|spec| spec.supervisor);
        Ok(Self {
            catalog_root: catalog_root.to_path_buf(),
            inbox: message::inbox_dir(&agent_dir),
            agent_dir,
            identity: identity.to_string(),
            this_host,
            supervisor,
            producer_version: None,
        })
    }

    fn report_protocol_rejection(&self, codex: &str, error: &anyhow::Error) {
        let Some(supervisor) = self.supervisor.as_deref() else {
            eprintln!(
                "st2 codex: agent '{}' has no supervisor for a protocol rejection report",
                self.identity
            );
            return;
        };
        let subject = format!("Codex protocol rejected: {}", self.identity);
        let body = format!(
            "st2 rejected the installed Codex app-server protocol for agent '{}'. Native delivery did not start. Codex executable: '{}'. Error: {error:#}",
            self.identity, codex
        );
        let mut key_hash = Sha256::new();
        key_hash.update(b"st2.codex-protocol-rejection.v1");
        key_hash.update(body.as_bytes());
        let idempotency_key = format!("st2.codex-protocol-rejection.v1:{:x}", key_hash.finalize());
        let tags = ["codex-protocol".to_string(), "launch-rejected".to_string()];
        if let Err(report_error) = message::send_to_resolved_inbox(
            &self.catalog_root,
            supervisor,
            &self.this_host,
            &self.identity,
            Some(&subject),
            None,
            &tags,
            &body,
            Some(&idempotency_key),
            None,
        ) {
            eprintln!(
                "st2 codex: failed to report agent '{}' protocol rejection to supervisor '{}': {report_error:#}",
                self.identity, supervisor
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodexDeliveryMethod {
    Start,
    Steer { turn_id: String },
}

#[derive(Debug, Clone)]
struct PendingCodexDelivery {
    request_id: u64,
    filename: String,
    method: CodexDeliveryMethod,
}

// One durable FIFO delivery attempt lives in the shared `crate::delivery_ledger`, which grades
// Codex's two receipts honestly: the JSON-RPC result of `turn/start`/`turn/steer` is
// `transportAccepted`, and only the exact completed typed user message — live, or found in a
// resumed thread's history — is `consumed`. Codex has no storage receipt and no scheduler
// admission signal, so it can never write the phases in between, and consumption is its true
// ceiling: reaching it releases FIFO ownership. Ordinary message archive precedence, which is the
// recipient agent's own act, still removes the inbox entry.

/// The exact codex-cli version whose Rust source settled the occupancy arithmetic below, read at
/// tag `rust-v0.151.0` (tag object `d8673cb68e349c208659b986697773d3145dbb14`) because the Nix
/// package ships a prebuilt musl tarball with no vendored source. HC-T03 calls Codex's baseline a
/// version-coupled constant — a property of a build, not of a documented contract — and HC-R13
/// bounds that with a fixture pinned to this literal, in the shape of `omp_session`'s
/// `admitted_versions_are_exactly_the_measured_set`. A codex bump that moves the numerator, the
/// denominator, or the baseline has to fail
/// [`tests::codex_context_recomputes_the_captured_reading_and_pins_its_verified_version`] rather
/// than silently publish a differently-meaning number.
///
/// Deliberately NOT a launch gate: this constant refuses nothing, it names what was measured.
/// Admitting 0.151.0 aligns the newest delivery-gated build with this measurement; a later Codex
/// admission must still re-read the version-coupled arithmetic rather than infer compatibility
/// from the unchanged literal.
pub const CODEX_CONTEXT_VERIFIED_VERSION: &str = "0.151.0";

/// Codex's `BASELINE_TOKENS`, subtracted from BOTH the numerator and the denominator of its
/// displayed occupancy: `codex-rs/protocol/src/protocol.rs:2332` and
/// `codex-rs/tui/src/token_usage.rs:9` at `rust-v0.151.0` carry the same literal with an identical
/// function body, and no configuration override exists. Its doc comment: "should capture tokens
/// that are always present in the context (e.g. system prompt and fixed tool instructions) so that
/// the percentage reflects the portion the user can influence."
const CODEX_BASELINE_TOKENS: i64 = 12_000;

/// The seven-day rate-limit window, identified by its duration because
/// `account/rateLimits/updated` names its windows `primary`/`secondary` and nothing else. 10,080
/// minutes = 7 days, and the one captured Codex rate-limit snapshot (rollout, 0.150.1) carries
/// exactly this window as `primary`. See [`CodexContextProducer::observe_rate_limits`] for why the
/// five-hour leg stays `null`.
const CODEX_SEVEN_DAY_WINDOW_MINUTES: i64 = 10_080;

/// How many recent compaction identities the dedupe retains. One compaction reaches this observer
/// as both `item/started` and `item/completed` — and possibly also as the deprecated
/// `thread/compacted` — so counting the edge naively counts one compaction twice or three times. A
/// last-key-only memory would still miscount an interleaving (`started(A)`, `started(B)`,
/// `completed(A)`), which a small ring closes for the same cost.
const CODEX_COMPACTION_MEMORY: usize = 4;

/// Codex's own occupancy arithmetic, mirrored rather than re-derived: the published number is
/// exactly `100 −` the "N% context left" the operator reads in the Codex footer.
///
/// `codex-rs/tui/src/token_usage.rs:43` (and its protocol twin):
///
/// ```text
/// if context_window <= BASELINE_TOKENS { return 0; }
/// effective = context_window - BASELINE_TOKENS
/// used      = (last.total_tokens - BASELINE_TOKENS).max(0)
/// remaining = (effective - used).max(0)
/// ((remaining / effective) * 100).clamp(0,100).round()
/// ```
///
/// Three things this deliberately does NOT do:
///
/// - It does not round the *used* percentage. Rounding `used/effective` and rounding
///   `remaining/effective` disagree on a half — effective 200, used 101 gives 51 one way and 50
///   the other — and only the mirrored order satisfies the spec's "equals `100 −` Codex's
///   displayed '% context left'".
/// - It does not use `total`, which is cumulative session spend. Against the captured window a
///   `total`-based percent reads 100 where the true occupancy is 33.
/// - It does not use `last.inputTokens`, which gives ~36 against the same capture — close enough
///   to look right and wrong by construction.
///
/// The one divergence from the source: where Codex returns `0` remaining for a window at or below
/// the baseline, mirroring blindly would publish "100% used" for a window it cannot normalize. st2
/// withholds instead (HC-R02, HC-R03) — a saturation the harness never displayed is fabricated,
/// not observed.
///
/// The result cannot exceed 100: Codex's `remaining` is floored at zero, so an occupancy above the
/// effective window saturates in the harness's own arithmetic before st2 ever sees it. That is a
/// property of mirroring Codex, not a clamp of st2's — the record still carries what a producer
/// computes, unclamped (HC-R02), and the harnesses that can report an overrun are the ones
/// publishing a float of their own.
fn codex_used_percent(window_tokens: Option<i64>, last_total_tokens: i64) -> Option<f64> {
    let window = window_tokens?;
    if window <= CODEX_BASELINE_TOKENS {
        return None;
    }
    let effective = window - CODEX_BASELINE_TOKENS;
    let used = (last_total_tokens - CODEX_BASELINE_TOKENS).max(0);
    let remaining = (effective - used).max(0);
    let remaining_percent = ((remaining as f64 / effective as f64) * 100.0)
        .clamp(0.0, 100.0)
        .round();
    Some(100.0 - remaining_percent)
}

/// One compaction's identity as this observer can name it. The item events carry a stable item id
/// alongside the turn; the deprecated `thread/compacted` notification carries only the turn, so its
/// key collapses with any item key in the same turn rather than counting beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexCompactionKey {
    turn_id: String,
    item_id: Option<String>,
}

impl CodexCompactionKey {
    /// Whether these two names describe the same compaction. Two distinct item ids in one turn are
    /// two compactions; a turn-only name in a turn already counted is the same one under its other
    /// spelling.
    fn same_compaction(&self, other: &Self) -> bool {
        self.turn_id == other.turn_id
            && match (&self.item_id, &other.item_id) {
                (Some(mine), Some(theirs)) => mine == theirs,
                _ => true,
            }
    }
}

/// The Codex half of the harness-context record (HC-R11).
///
/// It owns a [`harness_context::Writer`] beside the harness-state writer, sharing the wrapper's
/// incarnation so both records name the same session as their provenance. It holds no guard of its
/// own: `thread/tokenUsage/updated` arrives once per model response — roughly 10–15 per turn, and
/// replayed to a newly attached connection on resume — and every one of them is handed to
/// [`harness_context::Writer::observe`], whose quantization is the only thing deciding what lands.
/// A second guard here would make the write policy per-harness, which HC-R09 exists to prevent.
///
/// The only state it carries between notifications is what it cannot recover from the next one:
/// the account-scoped rate-limit windows (a separate notification with no reading behind it) and
/// the identities of recently counted compactions.
struct CodexContextProducer {
    writer: harness_context::Writer,
    /// Last-known account-scoped windows. `account/rateLimits/updated` is documented as a *sparse
    /// rolling update* whose absent fields do not clear a previously observed value, so the last
    /// known windows ride along with the next reading instead of blanking it.
    rate_limits: harness_context::RateLimits,
    counted_compactions: VecDeque<CodexCompactionKey>,
}

impl CodexContextProducer {
    fn new(writer: harness_context::Writer) -> Self {
        Self {
            writer,
            rate_limits: harness_context::RateLimits::default(),
            counted_compactions: VecDeque::new(),
        }
    }

    /// Project one inbound control frame onto the context record, returning whether a write landed.
    ///
    /// Every unknown method, foreign thread, and malformed payload is ignored rather than failed:
    /// this is observability riding a delivery socket, and a frame this producer cannot read must
    /// not disturb the frame the delivery loop can.
    fn observe(&mut self, message: &Value, thread_id: &str) -> Result<bool> {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Ok(false);
        };
        match method {
            "thread/tokenUsage/updated" => {
                if message.pointer("/params/threadId").and_then(Value::as_str) != Some(thread_id) {
                    return Ok(false);
                }
                let Some(reading) = self.token_usage_reading(message) else {
                    return Ok(false);
                };
                self.writer.observe(reading)
            }
            // Account-scoped and thread-free (HC-T06): it repeats across every runtime sharing the
            // account, carries no occupancy, and therefore never writes on its own. It is held and
            // published by the next reading.
            "account/rateLimits/updated" => {
                self.observe_rate_limits(message);
                Ok(false)
            }
            "item/started" | "item/completed" => {
                if message.pointer("/params/threadId").and_then(Value::as_str) != Some(thread_id)
                    || message.pointer("/params/item/type").and_then(Value::as_str)
                        != Some("contextCompaction")
                {
                    return Ok(false);
                }
                let (Some(turn_id), Some(item_id)) = (
                    message.pointer("/params/turnId").and_then(Value::as_str),
                    message.pointer("/params/item/id").and_then(Value::as_str),
                ) else {
                    return Ok(false);
                };
                self.compacted(CodexCompactionKey {
                    turn_id: turn_id.to_string(),
                    item_id: Some(item_id.to_string()),
                })
            }
            // Deprecated in the protocol in favour of the item ("Deprecated: Use
            // `ContextCompaction` item type instead") and unobserved on 0.150.1. Handled anyway,
            // and deduped against the item, because a harness emitting both must still count one
            // compaction.
            "thread/compacted" => {
                if message.pointer("/params/threadId").and_then(Value::as_str) != Some(thread_id) {
                    return Ok(false);
                }
                let Some(turn_id) = message.pointer("/params/turnId").and_then(Value::as_str)
                else {
                    return Ok(false);
                };
                self.compacted(CodexCompactionKey {
                    turn_id: turn_id.to_string(),
                    item_id: None,
                })
            }
            _ => Ok(false),
        }
    }

    /// The reading a `thread/tokenUsage/updated` carries, in Codex's own arithmetic.
    ///
    /// `usedTokens` and `windowTokens` are the harness's raw operands and are published as they
    /// arrive — a window at or below the baseline is still a window the harness reported, even
    /// where it cannot produce a percent. `model` and `costUsd` are `null` because the channel
    /// carries neither: the app-server `Thread` object has `modelProvider` and no model identifier,
    /// and Codex reports no session cost anywhere in the protocol (HC-R16).
    fn token_usage_reading(&self, message: &Value) -> Option<harness_context::Reading> {
        let last_total = message
            .pointer("/params/tokenUsage/last/totalTokens")
            .and_then(Value::as_i64)?;
        let window = message
            .pointer("/params/tokenUsage/modelContextWindow")
            .and_then(Value::as_i64)
            .filter(|window| *window > 0);
        Some(harness_context::Reading {
            used_tokens: u64::try_from(last_total).ok(),
            window_tokens: window.and_then(|window| u64::try_from(window).ok()),
            used_percent: codex_used_percent(window, last_total),
            model: None,
            cost_usd: None,
            // Cumulative lifetime spend and never occupancy (HC-R16): the captured session read
            // 2,235,329 against a 258,400-token window.
            session_total_tokens: message
                .pointer("/params/tokenUsage/total/totalTokens")
                .and_then(Value::as_i64)
                .and_then(|total| u64::try_from(total).ok()),
            rate_limits: self.rate_limits,
        })
    }

    /// Merge a sparse rate-limit update into the last-known windows.
    ///
    /// Codex names its windows `primary` and `secondary` and identifies them only by
    /// `windowDurationMins`, so the join is by duration. Only the seven-day window is carried: the
    /// single captured Codex rate-limit snapshot (0.150.1) contains one window, `primary`, at
    /// 10,080 minutes. No 300-minute window and no `secondary` was ever observed on this harness,
    /// so mapping one onto `fiveHour` would be inference dressed as a measurement — and this
    /// record's whole point is that its numbers were seen. `fiveHour` therefore stays `null` for
    /// Codex until a capture shows the window; admitting it is a one-line change beside the
    /// capture that justifies it.
    fn observe_rate_limits(&mut self, message: &Value) {
        for window in ["primary", "secondary"] {
            let Some(snapshot) = message.pointer(&format!("/params/rateLimits/{window}")) else {
                continue;
            };
            if snapshot.get("windowDurationMins").and_then(Value::as_i64)
                == Some(CODEX_SEVEN_DAY_WINDOW_MINUTES)
                && let Some(used) = snapshot.get("usedPercent").and_then(Value::as_f64)
            {
                self.rate_limits.seven_day = Some(used);
            }
        }
    }

    /// Count one compaction edge unless this compaction was already counted under another of its
    /// spellings. The count is incarnation-scoped: Codex publishes an edge and nothing else, so st2
    /// does the counting and the relaunch claim's record removal resets it (HC-R12, HC-R15). The
    /// trigger is `unknown` because `ContextCompactionThreadItem` carries `id` and `type` and no
    /// reason at all.
    fn compacted(&mut self, key: CodexCompactionKey) -> Result<bool> {
        if self
            .counted_compactions
            .iter()
            .any(|counted| counted.same_compaction(&key))
        {
            return Ok(false);
        }
        self.counted_compactions.push_back(key);
        while self.counted_compactions.len() > CODEX_COMPACTION_MEMORY {
            self.counted_compactions.pop_front();
        }
        self.writer.compacted(harness_context::Compaction::new(
            harness_context::CompactionTrigger::Unknown,
        ))
    }
}

#[derive(Debug, Clone)]
struct RejectedCodexDelivery {
    filename: String,
    observed: CodexObservedState,
}

struct CodexInboxDelivery {
    config: CodexDeliveryConfig,
    runtime: CodexRuntime,
    wake: Receiver<()>,
    _watcher: Option<notify::RecommendedWatcher>,
    next_inbox_refresh: Instant,
    next_presence_refresh: Instant,
    head: Option<message::Message>,
    suppressed: bool,
    ledger: delivery_ledger::Ledger,
    pending: Option<PendingCodexDelivery>,
    rejected: Option<RejectedCodexDelivery>,
    next_request_id: u64,
    harness_writer: harness_state::Writer,
    /// Whether the latest projection carried evidence. Indeterminate observations write nothing
    /// and stop the heartbeat, so a state the pump can no longer see ages out instead of staying
    /// artificially fresh.
    harness_evidence: bool,
    /// A projected transition whose write failed, retried on the next pump pass before any
    /// heartbeat may re-stamp the contradicted on-disk state.
    pending_observation: Option<harness_state::Observation>,
    /// The numeric axis's producer, beside the categorical one. `None` only where the record has
    /// nowhere safe to stage — observability never blocks a launch.
    context: Option<CodexContextProducer>,
    /// The native-driver boundary record. Codex publishes exactly one stage on it — the provider
    /// credential — because every earlier boundary is already fail-closed at admission: an
    /// incompatible protocol refuses the launch instead of degrading into an observation.
    diagnostics: driver_diagnostic::Publisher,
}

impl CodexInboxDelivery {
    fn new(
        config: CodexDeliveryConfig,
        legacy_path: PathBuf,
        runtime: CodexRuntime,
    ) -> Result<Self> {
        fs::create_dir_all(&config.inbox).with_context(|| {
            format!(
                "creating Codex native delivery inbox {}",
                config.inbox.display()
            )
        })?;
        let (wake_tx, wake) = mpsc::channel();
        // Scoped to inbox + status: this pump's own process group writes runtime records (presence
        // refreshes, harness-state transitions) into the same agent dir, and those must not wake it.
        let watcher = crate::watch::watch_delivery_inputs(&config.agent_dir, wake_tx);
        // The ledger's authority is `delivery-ledger.json`; the v1 path beside it is the one-shot
        // adoption source and the rollback floor, never read as authority again. Codex's v1
        // `Accepted` was written only from the typed completed user message, so it adopts as
        // `consumed` — the one adoption that may suppress a duplicate on its own evidence.
        let identity = config.identity.clone();
        let ledger = delivery_ledger::Ledger::open(
            &legacy_path,
            delivery_ledger::Harness::Codex.profile(),
            &config.identity,
            runtime.runtime_id(),
            |thread, filename| stable_client_user_message_id(&identity, thread, filename),
        );
        // The pty session whose liveness vouches for the record is the wrapper's task: the
        // runtime ID names the pty registry entry, and only aliases the identity on
        // driver-expanded seats — a hand-authored seat may declare a different task ID.
        // The session token is the runtime incarnation the wrapper already minted: the pump and
        // the wrapper's terminal writer are the same session and must own the same records. The
        // claim is a WRITTEN act — it atomically supersedes whatever a predecessor left,
        // including a still-fresh live record the pty-name probe cannot distinguish.
        // Observability must never kill the launch: a claim that cannot be written degrades to
        // a token-only writer (refused by records it does not own, so it can only under-report)
        // with a warning, and delivery proceeds.
        let harness_writer = {
            let writer = harness_state::Writer::new(
                &config.agent_dir,
                config.identity.clone(),
                "codex",
                Some(runtime.runtime_id().to_string()),
            );
            match harness_state::claim(
                &config.agent_dir,
                config.identity.clone(),
                "codex",
                runtime.incarnation(),
            ) {
                Ok(claimed_seq) => writer.with_ownership(runtime.incarnation(), claimed_seq),
                Err(error) => {
                    tracing::warn!(
                        "st2 codex: observed-state claim failed; degrading to token-only: {error:#}"
                    );
                    writer.with_session(runtime.incarnation())
                }
            }
        };
        // The numeric record's writer, owned beside the state record's and carrying the same
        // incarnation so both name one session as their provenance. It takes no claim and no
        // sequence: HC-T04 leaves the numbers unfenced on purpose, because the worst a straggler
        // can publish here is a reading older than the reader thinks — which `observedAtMs`
        // already says — rather than a live state that is not live.
        let context = match harness_context::Writer::new(
            &config.agent_dir,
            config.identity.clone(),
            harness_context::Harness::Codex,
        ) {
            Ok(writer) => Some(CodexContextProducer::new(
                writer.with_session(runtime.incarnation()),
            )),
            Err(error) => {
                tracing::warn!(
                    "st2 codex: harness-context writer unavailable; context stays unpublished: {error:#}"
                );
                None
            }
        };
        // The record belongs to this incarnation: the protocol gate already admitted the version
        // it names, so `support` is a measured fact rather than a probe result.
        let diagnostics = driver_diagnostic::Publisher::new(
            &config.agent_dir,
            driver_diagnostic::Driver::Codex,
            config.producer_version.clone(),
            driver_diagnostic::Support::Supported,
        );
        Ok(Self {
            config,
            runtime,
            wake,
            _watcher: watcher,
            next_inbox_refresh: Instant::now(),
            next_presence_refresh: Instant::now(),
            head: None,
            suppressed: false,
            ledger,
            pending: None,
            rejected: None,
            next_request_id: FIRST_DELIVERY_REQUEST_ID,
            harness_writer,
            harness_evidence: false,
            pending_observation: None,
            context,
            diagnostics,
        })
    }

    /// Publish the generic observed-harness-state projection of a control-state change. Best-effort
    /// like the presence refresh: a failed record write must not disturb delivery — but it must
    /// not count as evidence either. A transition whose write failed is retained as pending and
    /// retried before any heartbeat, so a stale on-disk state is never kept fresh in
    /// contradiction of the latest observation.
    fn observe_harness(&mut self, observed: &CodexObservedState) {
        match observed.harness_observation() {
            Some(observation) => self.publish_observation(observation),
            None => {
                // Evidence lost: stop heartbeating, drop anything pending (it predates the gap),
                // and mark the stream discontinuous so a state restated after the gap opens a
                // fresh transition instead of claiming continuity across an unobserved interval.
                self.harness_evidence = false;
                self.pending_observation = None;
                self.harness_writer.interrupt();
            }
        }
    }

    /// Hand one inbound control frame to the context producer. Best-effort in the same sense as the
    /// presence refresh and the state projection: a record write that fails must not disturb
    /// delivery. Unlike the state record there is nothing to retain and retry — the next model
    /// response carries another reading, and the record ages visibly through `ageMs` until it
    /// lands (HC-R06, HC-T05).
    fn observe_context(&mut self, message: &Value, thread_id: &str) {
        if let Some(context) = self.context.as_mut()
            && let Err(error) = context.observe(message, thread_id)
        {
            tracing::warn!("st2 codex: harness-context write failed: {error:#}");
        }
    }

    /// Record what one inbound frame proves about this thread's provider credential.
    ///
    /// Frame-level like [`Self::observe_context`] and for the same reason: the credential is its
    /// own axis, and the earliest-boundary projection inside the publisher — not this call site —
    /// decides what a reader sees. Both edges come from `turn/completed`: a turn that reached
    /// `completed` is positive proof the account was accepted, and a `failed` turn whose typed
    /// error names `unauthorized` is the rejection. Anything else leaves a standing rejection
    /// alone; only positive evidence clears it.
    fn observe_provider_auth(&mut self, message: &Value, thread_id: &str) {
        if message.get("method").and_then(Value::as_str) != Some("turn/completed")
            || message.pointer("/params/threadId").and_then(Value::as_str) != Some(thread_id)
        {
            return;
        }
        match codex_turn_outcome(message.pointer("/params/turn")) {
            CodexTurnOutcome::ProviderAuthRejected => self.diagnostics.publish(
                driver_diagnostic::Stage::ProviderAuth,
                driver_diagnostic::Reason::ProviderAuthRejected,
                driver_diagnostic::Source::TurnResult,
            ),
            CodexTurnOutcome::Accepted => self
                .diagnostics
                .clear(driver_diagnostic::Stage::ProviderAuth),
            CodexTurnOutcome::Indeterminate => {}
        }
    }

    fn publish_observation(&mut self, observation: harness_state::Observation) {
        match self.harness_writer.observe(observation.clone()) {
            Ok(()) => {
                self.harness_evidence = true;
                self.pending_observation = None;
            }
            Err(_) => {
                self.harness_evidence = false;
                self.harness_writer.interrupt();
                self.pending_observation = Some(observation);
            }
        }
    }

    /// Reconcile the ledger to what the recipient still has unread, then re-assert the rollback
    /// floor. Archive precedence is the recipient agent's act and the only settlement authority:
    /// an entry whose file left the inbox releases ownership, and this pump never moves a file.
    fn reconcile_inbox(&mut self, unread: &[message::Message]) -> Result<()> {
        self.ledger
            .prune(|filename| unread.iter().any(|message| message.filename == filename))?;
        // Re-asserted on every pass while an entry is outstanding: a crash exactly at the floor
        // write would otherwise leave a landing with no v1-readable lower bound.
        self.ledger.reassert_floor()
    }

    fn refresh_if_due(&mut self) -> Result<()> {
        let now = Instant::now();
        // A pending transition retries on EVERY pump pass — its write failed once and the
        // on-disk record contradicts the latest observation until it lands; only the heartbeat
        // is presence-cadence work.
        if let Some(pending) = self.pending_observation.clone() {
            self.publish_observation(pending);
        }
        if now >= self.next_presence_refresh {
            // This wrapper owns the live provider session. It therefore owns the presence lease.
            // Preserve busy or available, and let dnd age out.
            let _ = status::refresh(&status::status_path(&self.config.agent_dir));
            if self.harness_evidence {
                let _ = self.harness_writer.heartbeat();
            }
            self.next_presence_refresh = now + status::STATUS_REFRESH;
        }
        let mut due = now >= self.next_inbox_refresh;
        while self.wake.try_recv().is_ok() {
            due = true;
        }
        if !due {
            return Ok(());
        }
        let unread = message::list_inbox(&self.config.inbox)?;
        self.reconcile_inbox(&unread)?;
        if self.rejected.as_ref().is_some_and(|rejected| {
            unread
                .iter()
                .all(|message| message.filename != rejected.filename)
        }) {
            self.rejected = None;
        }
        self.head = unread.into_iter().next();
        self.suppressed =
            status::read_state(&status::status_path(&self.config.agent_dir)) == status::State::Dnd;
        self.next_inbox_refresh = Instant::now() + INBOX_REFRESH_FALLBACK;
        Ok(())
    }

    fn maybe_request(&mut self, state: &CodexControlState) -> Result<Option<Value>> {
        self.refresh_if_due()?;
        if self.pending.is_some() || !state.subscribed || self.suppressed {
            return Ok(None);
        }
        // Fail closed: an unreadable ledger holds and surfaces rather than guessing. It never
        // refuses to start — a control connection that will not start delivers nothing at all.
        // The operator-visible surface is the existing typed boundary — the transport is
        // unavailable — and the raw reason stays in tracing, so no unbounded prose reaches the
        // record. Restating it is coalesced by the publisher, so a held pass costs no write.
        if let Some(reason) = self.ledger.quarantined().map(str::to_string) {
            tracing::warn!("st2 codex: delivery ledger is quarantined: {reason}");
            self.diagnostics.publish(
                driver_diagnostic::Stage::Delivery,
                driver_diagnostic::Reason::DeliveryUnavailable,
                driver_diagnostic::Source::PromptTransport,
            );
            return Ok(None);
        }
        // A newly selected thread is a different delivery binding. An old binding's receipt must
        // neither suppress nor acknowledge delivery to this thread.
        if self
            .ledger
            .binding()
            .is_some_and(|binding| binding != state.thread_id())
        {
            self.ledger.rebind(state.thread_id())?;
        }
        let Some(head) = self.head.clone() else {
            return Ok(None);
        };
        if self.rejected.as_ref().is_some_and(|rejected| {
            rejected.filename == head.filename && rejected.observed == state.observed
        }) {
            return Ok(None);
        }
        // Exactly one delivery is outstanding at a time on this transport: an entry bound to some
        // other file holds the pump until archive precedence resolves it, so a message arriving
        // out of filename order can never open a second concurrent delivery.
        if !self.ledger.entries().is_empty() && self.ledger.entry(&head.filename).is_none() {
            return Ok(None);
        }
        // An attempt this pump already owns is held until evidence settles or refuses it. Only an
        // authoritative "no" — a rejected request, or a resumed history proving the client ID
        // never landed — authorizes sending the same identity again; a carried-forward v1 record
        // never does.
        if self.ledger.retry(&head.filename) != delivery_ledger::RetryDecision::Retry {
            return Ok(None);
        }
        let method = match &state.observed {
            CodexObservedState::Idle | CodexObservedState::TerminalError { .. } => {
                CodexDeliveryMethod::Start
            }
            CodexObservedState::Active { turn_id } => CodexDeliveryMethod::Steer {
                turn_id: turn_id.clone(),
            },
            CodexObservedState::AwaitingStatus | CodexObservedState::Held { .. } => {
                return Ok(None);
            }
        };
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Codex delivery request ID overflow")?;
        let client_id =
            stable_client_user_message_id(&self.config.identity, state.thread_id(), &head.filename);
        let filename = head.filename.clone();
        let text = ding::poke_text(
            &self.config.catalog_root,
            &self.config.this_host,
            &self.config.identity,
            &head,
        );
        let request =
            codex_delivery_request(request_id, state.thread_id(), &client_id, &text, &method);
        // Durable ownership before transport: the v1-readable floor first, then the ledger's own
        // `attempted`.
        self.ledger.begin(delivery_ledger::Begin {
            filename: filename.clone(),
            binding: state.thread_id().to_string(),
            correlation: delivery_ledger::Correlation::native(client_id.clone()),
            // Codex's typed receipt is a live frame, so an attempt is acknowledged only by the
            // incarnation that made it; an older one is settled by the resume sweep instead.
            incarnation: Some(self.runtime.incarnation().to_string()),
            legacy_floor: delivery_ledger::codex_floor(
                &self.config.identity,
                self.runtime.runtime_id(),
                self.runtime.incarnation(),
                state.thread_id(),
                &filename,
                &client_id,
            ),
        })?;
        self.pending = Some(PendingCodexDelivery {
            request_id,
            filename,
            method,
        });
        Ok(Some(request))
    }

    fn accept_response(&mut self, message: &Value, observed: &CodexObservedState) -> Result<bool> {
        let Some(pending) = self.pending.as_ref() else {
            return Ok(false);
        };
        if message.get("method").is_some()
            || message.get("id") != Some(&Value::from(pending.request_id))
        {
            return Ok(false);
        }
        let pending = self
            .pending
            .take()
            .context("Codex delivery is not pending")?;
        if message.get("error").is_some() {
            // The request itself was refused: an authoritative negative acknowledgement about
            // this attempt, and the only thing that re-authorizes the same client ID here. A
            // delivery that already reached its ceiling cannot be un-settled by a late error.
            self.ledger.negative(
                &pending.filename,
                delivery_ledger::NegativeReceipt::Rejected,
            )?;
            self.rejected = Some(RejectedCodexDelivery {
                filename: pending.filename,
                observed: observed.clone(),
            });
            return Ok(true);
        }
        match &pending.method {
            CodexDeliveryMethod::Start => {
                required_string(message, "/result/turn/id", "turn/start response")?;
            }
            CodexDeliveryMethod::Steer { turn_id } => {
                let returned = required_string(message, "/result/turnId", "turn/steer response")?;
                anyhow::ensure!(
                    returned == turn_id,
                    "Codex turn/steer response returned a different turn"
                );
            }
        }
        // The request returned a well-formed result. That is a fact about the call, never about
        // the model, so it grades no higher than `transportAccepted`.
        self.ledger.record(
            &pending.filename,
            delivery_ledger::Evidence::TransportAccepted,
        )?;
        self.rejected = None;
        Ok(true)
    }

    fn accept_typed_receipt(&mut self, message: &Value, state: &CodexControlState) -> Result<bool> {
        if message.get("method").and_then(Value::as_str) != Some("item/completed")
            || message.pointer("/params/item/type").and_then(Value::as_str) != Some("userMessage")
        {
            return Ok(false);
        }
        let Some(client_id) = message
            .pointer("/params/item/clientId")
            .and_then(Value::as_str)
        else {
            return Ok(false);
        };
        if message.pointer("/params/threadId").and_then(Value::as_str) != Some(state.thread_id())
            || state.runtime_incarnation != self.runtime.incarnation()
        {
            return Ok(false);
        }
        // One correlation may carry several inbox files, so one typed receipt settles every entry
        // it delivered — each on its own monotone entry.
        let settled: Vec<String> = self
            .ledger
            .correlated(client_id)
            .into_iter()
            .filter(|filename| {
                self.ledger.entry(filename).is_some_and(|entry| {
                    entry.binding == state.thread_id()
                        && entry.incarnation.as_deref() == Some(self.runtime.incarnation())
                })
            })
            .collect();
        if settled.is_empty() {
            return Ok(false);
        }
        for filename in &settled {
            self.ledger
                .record(filename, delivery_ledger::Evidence::Consumed)?;
        }
        Ok(true)
    }

    /// Reconcile a pre-crash attempt against the typed history returned by `thread/resume` before
    /// the same client ID can be sent again.
    fn reconcile_resume(&mut self, message: &Value, state: &CodexControlState) -> Result<()> {
        if message.get("error").is_some() {
            return Ok(());
        }
        let unsettled: Vec<(String, String)> = self
            .ledger
            .entries()
            .iter()
            .filter(|entry| {
                entry.binding == state.thread_id()
                    && entry.phase < delivery_ledger::Phase::Consumed
            })
            .map(|entry| (entry.filename.clone(), entry.correlation.value.clone()))
            .collect();
        if unsettled.is_empty() {
            return Ok(());
        }
        let turns = message
            .pointer("/result/thread/turns")
            .and_then(Value::as_array)
            .context(
                "Codex thread/resume response has no typed turn history for delivery recovery",
            )?;
        for (filename, client_id) in unsettled {
            let accepted = turns.iter().any(|turn| {
                turn.get("items")
                    .and_then(Value::as_array)
                    .is_some_and(|items| {
                        items.iter().any(|item| {
                            item.get("type").and_then(Value::as_str) == Some("userMessage")
                                && item.get("clientId").and_then(Value::as_str)
                                    == Some(client_id.as_str())
                        })
                    })
            });
            if accepted {
                self.ledger
                    .record(&filename, delivery_ledger::Evidence::Consumed)?;
            } else {
                // An authoritative resumed history without the client ID proves the pre-crash
                // attempt never landed. That absence is the receipt — retained, not erased —
                // and only it may authorize sending the same stable ID again.
                self.ledger
                    .negative(&filename, delivery_ledger::NegativeReceipt::Absent)?;
            }
        }
        Ok(())
    }
}

fn stable_client_user_message_id(recipient: &str, thread_id: &str, filename: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.codex-client-user-message.v1");
    for value in [
        recipient.as_bytes(),
        thread_id.as_bytes(),
        filename.as_bytes(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    format!("st2:{:x}", hash.finalize())
}

fn codex_delivery_request(
    request_id: u64,
    thread_id: &str,
    client_id: &str,
    text: &str,
    method: &CodexDeliveryMethod,
) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "clientUserMessageId": client_id,
        "input": [{ "type": "text", "text": text, "text_elements": [] }]
    });
    let method_name = match method {
        CodexDeliveryMethod::Start => "turn/start",
        CodexDeliveryMethod::Steer { turn_id } => {
            params["expectedTurnId"] = Value::String(turn_id.clone());
            "turn/steer"
        }
    };
    json!({ "method": method_name, "id": request_id, "params": params })
}

enum SubscriptionAcceptance {
    Accepted { changed: bool },
    Deferred,
}

impl CodexControlState {
    fn new(runtime: &CodexRuntime, thread_id: String) -> Self {
        Self {
            schema: CONTROL_STATE_SCHEMA.to_string(),
            agent: runtime.agent.clone(),
            runtime_id: runtime.runtime_id.clone(),
            runtime_incarnation: runtime.incarnation.clone(),
            thread_id,
            subscribed: false,
            observed: CodexObservedState::AwaitingStatus,
        }
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn observed(&self) -> &CodexObservedState {
        &self.observed
    }

    pub fn subscribed(&self) -> bool {
        self.subscribed
    }

    fn accept_subscription(&mut self, message: &Value) -> Result<SubscriptionAcceptance> {
        if let Some(error) = message.get("error") {
            let code = error.get("code").and_then(Value::as_i64);
            let detail = error.get("message").and_then(Value::as_str);
            if code == Some(-32600)
                && detail
                    .is_some_and(|detail| detail.starts_with("no rollout found for thread id "))
            {
                return Ok(SubscriptionAcceptance::Deferred);
            }
            anyhow::bail!("Codex app-server rejected control thread/resume: {error}");
        }
        anyhow::ensure!(
            message.get("result").is_some(),
            "Codex control thread/resume response has no result"
        );
        let thread_id = required_string(message, "/result/thread/id", "thread/resume response")?;
        anyhow::ensure!(
            thread_id == self.thread_id,
            "Codex control thread/resume returned a different thread"
        );
        let status = required_string(
            message,
            "/result/thread/status/type",
            "thread/resume response",
        )?;
        let blocked = human_blocking_flag(message.pointer("/result/thread/status"));
        let before = (self.subscribed, self.observed.clone());
        self.subscribed = true;
        self.observe_thread_status(status, blocked);
        Ok(SubscriptionAcceptance::Accepted {
            changed: (self.subscribed, self.observed.clone()) != before,
        })
    }

    fn observe(&mut self, message: &Value) -> Result<bool> {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Ok(false);
        };
        let before = self.observed.clone();
        match method {
            "thread/started" => {
                let thread_id = required_string(message, "/params/thread/id", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let status = required_string(message, "/params/thread/status/type", method)?;
                let blocked = human_blocking_flag(message.pointer("/params/thread/status"));
                self.observe_thread_status(status, blocked);
            }
            "thread/status/changed" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let status = required_string(message, "/params/status/type", method)?;
                let blocked = human_blocking_flag(message.pointer("/params/status"));
                self.observe_thread_status(status, blocked);
            }
            "turn/started" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let turn_id = required_string(message, "/params/turn/id", method)?.to_string();
                self.observe_turn_started(turn_id);
            }
            "turn/completed" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let turn_id = required_string(message, "/params/turn/id", method)?;
                let outcome = codex_turn_outcome(message.pointer("/params/turn"));
                self.observe_turn_completed(turn_id, outcome);
            }
            "item/started" | "item/completed" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let item_type = required_string(message, "/params/item/type", method)?;
                // The admitted `ThreadItem` schema has only three variants that change
                // steerability. Every other classified item reports work inside a turn that the
                // turn and thread status already model, so it is ignored on purpose. A later
                // protocol item that gates or releases input must be added here explicitly.
                // Silently dropping one is how `exitedReviewMode` stayed unmatched. Review is
                // also the only hold that the protocol ends with a typed item of its own:
                // `contextCompaction` has no exit item, so both of its lifecycle edges keep
                // holding until the thread proves otherwise.
                let (reason, released) = match item_type {
                    "enteredReviewMode" => (CodexHoldReason::Review, false),
                    "exitedReviewMode" => (CodexHoldReason::Review, true),
                    "contextCompaction" => (CodexHoldReason::Compaction, false),
                    _ if CLASSIFIED_CODEX_THREAD_ITEMS.contains(&item_type) => return Ok(false),
                    _ => (CodexHoldReason::UnknownProtocol, false),
                };
                let turn_id = required_string(message, "/params/turnId", method)?;
                if released {
                    self.observe_hold_released(turn_id, reason);
                } else {
                    self.observe_non_steerable(turn_id, reason);
                }
            }
            _ if message.get("id").is_some()
                && !CLASSIFIED_CODEX_SERVER_REQUESTS.contains(&method) =>
            {
                self.observe_unknown_protocol();
            }
            _ => return Ok(false),
        }
        Ok(self.observed != before)
    }

    fn observe_thread_status(&mut self, status: &str, blocked: Option<CodexHoldReason>) {
        self.observed = match status {
            "idle" => CodexObservedState::Idle,
            "active" => match (&self.observed, blocked) {
                // A human-blocking flag holds the exact turn already proven active. Clearing it
                // releases that same turn, because no second `turn/started` arrives mid-turn.
                (CodexObservedState::Active { turn_id }, Some(reason)) => {
                    CodexObservedState::Held {
                        reason,
                        turn_id: Some(turn_id.clone()),
                    }
                }
                (
                    CodexObservedState::Held {
                        reason:
                            CodexHoldReason::WaitingOnApproval | CodexHoldReason::WaitingOnUserInput,
                        turn_id,
                    },
                    Some(reason),
                ) => CodexObservedState::Held {
                    reason,
                    turn_id: turn_id.clone(),
                },
                (
                    CodexObservedState::Held {
                        reason:
                            CodexHoldReason::WaitingOnApproval | CodexHoldReason::WaitingOnUserInput,
                        turn_id: Some(turn_id),
                    },
                    None,
                ) => CodexObservedState::Active {
                    turn_id: turn_id.clone(),
                },
                // A more specific hold outranks the flag: its turn ID still tracks the lifecycle.
                (
                    CodexObservedState::Active { .. }
                    | CodexObservedState::Held {
                        reason:
                            CodexHoldReason::Review
                            | CodexHoldReason::Compaction
                            | CodexHoldReason::UnknownProtocol
                            | CodexHoldReason::ConflictingTurn,
                        ..
                    },
                    _,
                ) => self.observed.clone(),
                // Flagged without a known turn: still a hold, but it names what it waits on.
                (_, Some(reason)) => CodexObservedState::Held {
                    reason,
                    turn_id: None,
                },
                (_, None) => CodexObservedState::Held {
                    reason: CodexHoldReason::ActiveWithoutTurn,
                    turn_id: None,
                },
            },
            "notLoaded" => CodexObservedState::Held {
                reason: CodexHoldReason::NotLoaded,
                turn_id: None,
            },
            "systemError" => CodexObservedState::Held {
                reason: CodexHoldReason::SystemError,
                turn_id: None,
            },
            _ => CodexObservedState::Held {
                reason: CodexHoldReason::UnknownStatus,
                turn_id: None,
            },
        };
    }

    fn observe_turn_started(&mut self, turn_id: String) {
        self.observed = match &self.observed {
            CodexObservedState::Active { turn_id: current } if current == &turn_id => {
                self.observed.clone()
            }
            CodexObservedState::Held {
                reason:
                    reason @ (CodexHoldReason::Review
                    | CodexHoldReason::Compaction
                    | CodexHoldReason::UnknownProtocol),
                ..
            } => CodexObservedState::Held {
                reason: *reason,
                turn_id: Some(turn_id),
            },
            CodexObservedState::Active { .. }
            | CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                ..
            } => CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                turn_id: None,
            },
            _ => CodexObservedState::Active { turn_id },
        };
    }

    fn observe_turn_completed(&mut self, turn_id: &str, outcome: CodexTurnOutcome) {
        // A failed turn whose typed error names a rejected credential is a SEAT-level fact: the
        // account was refused, which does not depend on which turn st2 believed live. It is
        // therefore settled before the turn-identity match below, and it outranks a plain
        // `systemError` terminal because it names the same failure's cause. Delivery semantics are
        // untouched: every `TerminalError` already permits `turn/start`.
        if outcome == CodexTurnOutcome::ProviderAuthRejected {
            self.observed = CodexObservedState::TerminalError {
                reason: CodexTerminalError::ProviderAuthRejected,
            };
            return;
        }
        self.observed = match &self.observed {
            CodexObservedState::Idle => CodexObservedState::Idle,
            CodexObservedState::TerminalError { .. } => self.observed.clone(),
            CodexObservedState::Active { turn_id: current } if current == turn_id => {
                CodexObservedState::Idle
            }
            CodexObservedState::AwaitingStatus
            | CodexObservedState::Held {
                reason: CodexHoldReason::ActiveWithoutTurn,
                ..
            } => CodexObservedState::Idle,
            // Every other hold is owned by a signal that is not the turn lifecycle. A completion
            // is not evidence that a review or a compaction ended, that the thread reloaded, that
            // a reported system error cleared, or that the human a turn was waiting on has
            // answered, so it does not speak for them. Only the signal that minted the hold
            // releases it: the waiting-on-human holds are minted from `activeFlags` on a thread
            // status and are cleared by the next thread status that omits the flag.
            CodexObservedState::Held {
                reason:
                    CodexHoldReason::Review
                    | CodexHoldReason::Compaction
                    | CodexHoldReason::UnknownProtocol
                    | CodexHoldReason::ConflictingTurn
                    | CodexHoldReason::WaitingOnApproval
                    | CodexHoldReason::WaitingOnUserInput
                    | CodexHoldReason::NotLoaded
                    | CodexHoldReason::UnknownStatus,
                ..
            } => self.observed.clone(),
            CodexObservedState::Held {
                reason: CodexHoldReason::SystemError,
                ..
            } => CodexObservedState::TerminalError {
                reason: CodexTerminalError::SystemError,
            },
            // A completion for a turn other than the one believed live is the only evidence here
            // that two turns exist. This match stays exhaustive so a new observed state cannot
            // silently arrive as a conflict it never was.
            CodexObservedState::Active { .. } => CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                turn_id: None,
            },
        };
    }

    fn observe_non_steerable(&mut self, turn_id: &str, reason: CodexHoldReason) {
        self.observed = match &self.observed {
            CodexObservedState::Active { turn_id: current } if current == turn_id => {
                CodexObservedState::Held {
                    reason,
                    turn_id: Some(turn_id.to_string()),
                }
            }
            CodexObservedState::Held {
                reason: current_reason,
                ..
            } if current_reason == &reason
                && matches!(
                    reason,
                    CodexHoldReason::Review
                        | CodexHoldReason::Compaction
                        | CodexHoldReason::UnknownProtocol
                ) =>
            {
                self.observed.clone()
            }
            _ if matches!(
                reason,
                CodexHoldReason::Review
                    | CodexHoldReason::Compaction
                    | CodexHoldReason::UnknownProtocol
            ) =>
            {
                CodexObservedState::Held {
                    reason,
                    turn_id: Some(turn_id.to_string()),
                }
            }
            _ => CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                turn_id: None,
            },
        };
    }

    /// A typed hold-exit item releases only the hold it ends, and only for the exact turn that
    /// hold carries. The exit arrives inside a running turn, so the honest result is that turn
    /// active again rather than idle. Anything else — a different hold reason, a hold bound to
    /// another turn, or no hold at all — is not evidence about this state, so it is left alone:
    /// an exit must never invent an active turn or release a hold it did not end.
    fn observe_hold_released(&mut self, turn_id: &str, reason: CodexHoldReason) {
        let CodexObservedState::Held {
            reason: current_reason,
            turn_id: held_turn_id,
        } = &self.observed
        else {
            return;
        };
        if *current_reason != reason || held_turn_id.as_deref() != Some(turn_id) {
            return;
        }
        self.observed = CodexObservedState::Active {
            turn_id: turn_id.to_string(),
        };
    }

    fn observe_unknown_protocol(&mut self) {
        if matches!(self.observed, CodexObservedState::TerminalError { .. }) {
            return;
        }
        let turn_id = match &self.observed {
            CodexObservedState::Active { turn_id }
            | CodexObservedState::Held {
                turn_id: Some(turn_id),
                ..
            } => Some(turn_id.clone()),
            _ => None,
        };
        self.observed = CodexObservedState::Held {
            reason: CodexHoldReason::UnknownProtocol,
            turn_id,
        };
    }
}

/// Read the delivery-relevant part of `ThreadStatus.activeFlags`: the first flag that says this
/// thread is blocked on a human rather than on the model.
///
/// The startup gate requires `activeFlags` on the `active` arm of `ThreadStatus`. A missing or
/// malformed runtime array reads as no flag instead of killing the control watcher. The startup
/// gate rejects an unclassified flag before launch.
fn human_blocking_flag(status: Option<&Value>) -> Option<CodexHoldReason> {
    status?
        .get("activeFlags")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .find_map(|flag| match flag {
            "waitingOnApproval" => Some(CodexHoldReason::WaitingOnApproval),
            "waitingOnUserInput" => Some(CodexHoldReason::WaitingOnUserInput),
            _ => None,
        })
}

fn required_string<'a>(message: &'a Value, pointer: &str, method: &str) -> Result<&'a str> {
    message
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{method} has no non-empty {pointer}"))
}

/// Run one authored Codex argv behind a dedicated app server and initialized control connection.
pub fn run_controlled(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    codex_argv: Vec<String>,
) -> Result<()> {
    anyhow::ensure!(
        !codex_argv.is_empty(),
        "Codex controlled launch argv is empty"
    );
    let mut delivery = CodexDeliveryConfig::resolve(catalog_root, &identity)?;
    match ensure_supported_protocol(&codex_argv[0]) {
        // The admitted version is the one fact the gate learns that outlives it: the diagnostic
        // record names the producer it was measured against, exactly as the OpenCode driver does.
        Ok(version) => delivery.producer_version = Some(version),
        Err(error) => {
            delivery.report_protocol_rejection(&codex_argv[0], &error);
            return Err(error);
        }
    }

    let state_dir = state_dir(catalog_root, &identity);
    secure_dir(&state_dir)?;
    let _owner_lock = acquire_owner_lock(&state_dir)?;
    let mut diagnostics = WrapperDiagnostics::open(&state_dir, &identity, &runtime_id)?;
    diagnostics.record("ownerAcquired", json!({}))?;

    let result = run_controlled_owned(
        catalog_root,
        &state_dir,
        identity,
        runtime_id,
        codex_argv,
        delivery,
        &mut diagnostics,
    );
    match result {
        Ok(()) => {
            diagnostics.record("completed", json!({}))?;
            Ok(())
        }
        Err(error) => {
            let error_text = format!("{error:#}");
            if let Err(diagnostic_error) =
                diagnostics.record("failed", json!({ "error": error_text }))
            {
                return Err(error).context(format!(
                    "persisting Codex wrapper failure diagnostic: {diagnostic_error:#}"
                ));
            }
            Err(error)
        }
    }
}

fn run_controlled_owned(
    catalog_root: &Path,
    state_dir: &Path,
    identity: String,
    runtime_id: String,
    codex_argv: Vec<String>,
    delivery: CodexDeliveryConfig,
    diagnostics: &mut WrapperDiagnostics,
) -> Result<()> {
    // Installed before ANY child exists — the hook-trust preflight spawns a detached app-server
    // first, and a SIGTERM landing in that window must set the stop flag its connect loop polls
    // rather than killing this wrapper around a leaked server and a stale socket. (Installing
    // resets the flag, so this must also run exactly once per launch.)
    crate::provider_session::install_signal_handler();
    let binding_path = state_dir.join("binding.json");
    let resume_thread = load_resume_thread(&binding_path, &identity, &runtime_id)?;

    let socket_path = socket_path(catalog_root, &identity)?;
    let socket_dir = socket_path
        .parent()
        .context("Codex app-server socket has no parent")?;
    secure_dir(socket_dir)?;
    prepare_socket_for_launch(&socket_path)?;

    // Publish a new incarnation only after this process holds the owner lock and has proved that no
    // older daemon is live. A rejected second owner must not invalidate the first owner's binding.
    let runtime = CodexRuntime::fresh(identity, runtime_id)?;
    atomic_json(&state_dir.join("runtime.json"), &runtime)?;
    diagnostics.record(
        "runtimePublished",
        json!({
            "runtimeIncarnation": runtime.incarnation(),
            "resumeSelected": resume_thread.is_some(),
        }),
    )?;

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(state_dir.join("app-server.log"))?;
    let endpoint = format!("unix://{}", socket_path.display());
    let mut server_args = controlled_app_server_args(&endpoint, &codex_argv[1..])?;
    if resume_thread.is_some() && authored_bypasses_hook_trust(&codex_argv[1..])? {
        let hook_cwd = controlled_hook_cwd(&codex_argv[1..])?;
        if let Some(projection) = preflight_hook_trust(
            &codex_argv[0],
            &server_args,
            &socket_path,
            &hook_cwd,
            &log,
            diagnostics,
        )? {
            insert_app_server_config_override(&mut server_args, projection.override_value)?;
        }
    }
    diagnostics.record("appServerStarting", json!({}))?;
    let mut server_command = Command::new(&codex_argv[0]);
    server_command
        .args(server_args)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    let mut server = spawn_process_group(&mut server_command, Some(&socket_path))
        .with_context(|| format!("starting {} app-server", codex_argv[0]))?;
    let result = diagnostics
        .record("appServerStarted", json!({ "pid": server.id() }))
        .and_then(|_| {
            run_connected(
                server.child_mut(),
                &socket_path,
                &runtime,
                &codex_argv,
                resume_thread.as_deref(),
                delivery,
                diagnostics,
            )
        });
    server.terminate();
    result
}

fn prepare_socket_for_launch(socket_path: &Path) -> Result<()> {
    match fs::symlink_metadata(socket_path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Codex app-server path already exists and is not a socket: {}",
                socket_path.display()
            );
            match UnixStream::connect(socket_path) {
                Ok(_) => anyhow::bail!(
                    "Codex app-server socket {} is already live; refusing a second control owner",
                    socket_path.display()
                ),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    fs::remove_file(socket_path).with_context(|| {
                        format!("removing stale Codex socket {}", socket_path.display())
                    })?;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "checking existing Codex socket {} before launch",
                            socket_path.display()
                        )
                    });
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("checking Codex socket path {}", socket_path.display()));
        }
    }
    Ok(())
}

fn run_connected(
    server: &mut Child,
    socket_path: &Path,
    runtime: &CodexRuntime,
    codex_argv: &[String],
    resume_thread: Option<&str>,
    delivery: CodexDeliveryConfig,
    diagnostics: &mut WrapperDiagnostics,
) -> Result<()> {
    // The stop handler is installed by run_controlled_owned before any spawn (the preflight's
    // detached app-server included); re-installing here would RESET a stop flag raised during
    // startup, so this function only relies on it.
    let state_dir = state_dir(&delivery.catalog_root, &delivery.identity);
    let endpoint = format!("unix://{}", socket_path.display());
    let tui_args = controlled_tui_args(&endpoint, &codex_argv[1..], resume_thread)?;
    let expected_resume =
        expected_resume_thread(&codex_argv[1..], resume_thread)?.map(str::to_owned);
    diagnostics.record("waitingForControlSocket", json!({ "pid": server.id() }))?;
    // A stop during startup ends the launch before anything was observed: no TUI exists, the
    // caller reaps the app-server, and this session leaves no record — its predecessor's ages
    // out on its own.
    let Some(control) = connect_control(server, socket_path, STARTUP_TIMEOUT)? else {
        diagnostics.record("stoppedDuringStartup", json!({ "phase": "connect" }))?;
        return Ok(());
    };
    diagnostics.record("controlSocketConnected", json!({}))?;
    let shutdown = control.try_clone()?;
    if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
        diagnostics.record("stoppedDuringStartup", json!({ "phase": "initialize" }))?;
        let _ = shutdown.shutdown(Shutdown::Both);
        return Ok(());
    }
    // The initialize wait itself polls the stop flag between short socket timeouts and returns
    // None on a stop; the recheck below covers a stop raised in the remaining gaps.
    let Some(websocket) = initialize_control(control)? else {
        diagnostics.record("stoppedDuringStartup", json!({ "phase": "initialize" }))?;
        let _ = shutdown.shutdown(Shutdown::Both);
        return Ok(());
    };
    if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
        diagnostics.record("stoppedDuringStartup", json!({ "phase": "initialized" }))?;
        let _ = shutdown.shutdown(Shutdown::Both);
        return Ok(());
    }
    diagnostics.record("controlInitialized", json!({}))?;
    let (events_tx, events_rx) = mpsc::channel();
    let binding_path = state_dir.join("binding.json");
    let control_state_path = state_dir.join("control-state.json");
    let runtime_for_reader = runtime.clone();
    let (mut resume_ready_tx, resume_ready_rx) = if expected_resume.is_some() {
        let (tx, rx) = mpsc::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let harness_agent_dir = delivery.agent_dir.clone();
    let harness_identity = delivery.identity.clone();
    let event_thread = thread::spawn(move || {
        let resume = expected_resume
            .as_deref()
            .zip(resume_ready_rx)
            .map(|(thread_id, ready)| ControlResume {
                thread_id,
                ready,
                tui_loaded_timeout: TUI_LOADED_TIMEOUT,
            });
        pump_control(
            websocket,
            &binding_path,
            &control_state_path,
            &runtime_for_reader,
            resume,
            Some(delivery),
            events_tx,
        )
    });

    // A fresh initialized observer reads before this child can issue thread/start. A resumed
    // observer waits on the gate below, then proves through thread/loaded/list that the TUI issued
    // its own resume. Only after that typed observation may control send its redundant resume.
    // Insert the remote endpoint as a global Codex option and preserve every authored argument
    // after the provider executable.
    let mut tui_command = Command::new(&codex_argv[0]);
    tui_command.args(tui_args);
    let mut tui = match tui_command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(tui) => tui,
        Err(error) => {
            drop(resume_ready_tx);
            let _ = shutdown.shutdown(Shutdown::Both);
            let _ = event_thread.join();
            // The claim already wrote its ended(superseded) placeholder; leaving that as the
            // last word would read as "another session took over". The launch failure is this
            // session's real terminal outcome — token-only adoption resolves to the claim's
            // sequence, since the claim put this token on disk.
            let mut writer = harness_state::Writer::new(
                &harness_agent_dir,
                harness_identity.clone(),
                "codex",
                Some(runtime.runtime_id().to_string()),
            )
            .with_session(runtime.incarnation());
            let _ = writer.observe(
                harness_state::Observation::new(
                    harness_state::Activity::Ended,
                    harness_state::BlockedOn::None,
                    harness_state::InputBuffer::Unknown,
                )
                .with_reason("launch-error")
                .with_exit("exit unknown"),
            );
            return Err(error)
                .with_context(|| format!("starting controlled {} TUI", codex_argv[0]));
        }
    };
    let result = (|| -> Result<TuiEnd> {
        diagnostics.record("tuiStarted", json!({ "pid": tui.id() }))?;
        if let Some(ready) = resume_ready_tx.take() {
            ready
                .send(())
                .context("starting Codex control resume after the TUI launched")?;
        }
        diagnostics.record("waitingForThreadBinding", json!({ "pid": tui.id() }))?;
        match wait_for_binding(&mut tui, &events_rx, STARTUP_TIMEOUT, diagnostics)? {
            BindingWait::Bound => {
                diagnostics.record("threadBound", json!({ "pid": tui.id() }))?;
                monitor_bound_tui(&mut tui, &events_rx)
            }
            BindingWait::Stopped => {
                terminate_child(&mut tui);
                Ok(TuiEnd::Stopped(tui.try_wait().ok().flatten()))
            }
        }
    })();
    if result.is_err() {
        terminate_child(&mut tui);
    }
    drop(resume_ready_tx);
    let _ = shutdown.shutdown(Shutdown::Both);
    let _ = event_thread.join();
    // The pump is gone, so nothing can observe this session again: publish the terminal
    // observation with the outcome the wrapper actually saw, before any staleness horizon.
    // Consumers must not branch on `reason`, so the observed exit always lands in `exit`.
    // Same incarnation as the pump's writer, adopting by token: the pump's written claim (or any
    // of its writes) put this token on disk, so token-only adoption resolves to this session's
    // claimed sequence — and the terminal record fences exactly the records this session wrote.
    let mut harness_writer = harness_state::Writer::new(
        &harness_agent_dir,
        harness_identity.clone(),
        "codex",
        Some(runtime.runtime_id().to_string()),
    )
    .with_session(runtime.incarnation());
    let _ = match &result {
        Ok(TuiEnd::Exited(status)) => harness_writer.ended(describe_tui_exit(Some(*status))),
        Ok(TuiEnd::Stopped(status)) => harness_writer.ended(describe_tui_exit(*status)),
        Err(error) => {
            let observed_exit = tui.try_wait().ok().flatten();
            harness_writer.observe(
                harness_state::Observation::new(
                    harness_state::Activity::Ended,
                    harness_state::BlockedOn::None,
                    harness_state::InputBuffer::Unknown,
                )
                .with_exit(describe_tui_exit(observed_exit))
                .with_reason(format!("{error}")),
            )
        }
    };
    match result {
        Ok(TuiEnd::Exited(status)) => completed_tui(status),
        // The wrapper stopped its own session: not a failure, mirroring the shared wrapper body.
        Ok(TuiEnd::Stopped(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

/// How the controlled TUI session came to an end, as the monitor saw it.
enum TuiEnd {
    /// The TUI exited on its own with this status.
    Exited(ExitStatus),
    /// The wrapper's stop flag ended the session; the reaped status when one was observable.
    Stopped(Option<ExitStatus>),
}

fn describe_tui_exit(status: Option<ExitStatus>) -> String {
    match status.map(|status| (status.code(), status.signal())) {
        Some((Some(code), _)) => format!("exit {code}"),
        Some((None, Some(signal))) => format!("signal {signal}"),
        _ => "exit unknown".to_string(),
    }
}

/// Start app-server with the authored global configuration inputs that its CLI supports.
///
/// Project trust, strict parsing, and feature selection affect config and hook loading in the
/// server process. Passing them only to the remote TUI silently creates two different effective
/// configurations. TUI-only policy, model, workspace, authentication, and prompt arguments stay
/// on the TUI command.
fn controlled_app_server_args(endpoint: &str, authored_args: &[String]) -> Result<Vec<String>> {
    let boundary = interactive_root_prefix_end(authored_args)?;
    let mut args = vec!["app-server".to_string()];
    let mut index = 0;
    while index < boundary {
        let argument = authored_args[index].as_str();
        if matches!(argument, "-c" | "--config" | "--enable" | "--disable") {
            args.push(argument.to_string());
            args.push(authored_args[index + 1].clone());
            index += 2;
            continue;
        }
        if argument == "--strict-config"
            || argument.starts_with("--config=")
            || argument.starts_with("--enable=")
            || argument.starts_with("--disable=")
            || (argument.starts_with("-c") && argument.len() > 2)
        {
            args.push(argument.to_string());
            index += 1;
            continue;
        }
        if matches!(
            argument,
            "--oss"
                | "--dangerously-bypass-approvals-and-sandbox"
                | "--dangerously-bypass-hook-trust"
                | "--search"
                | "--no-alt-screen"
        ) {
            index += 1;
            continue;
        }
        if matches!(argument, "-i" | "--image")
            || argument.starts_with("-i=")
            || argument.starts_with("--image=")
        {
            break;
        }
        let exact_value_option = matches!(
            argument,
            "--remote-auth-token-env"
                | "-m"
                | "--model"
                | "--local-provider"
                | "-p"
                | "--profile"
                | "-s"
                | "--sandbox"
                | "-C"
                | "--cd"
                | "--add-dir"
                | "-a"
                | "--ask-for-approval"
        );
        index += if exact_value_option { 2 } else { 1 };
    }
    args.extend(["--listen".to_string(), endpoint.to_string()]);
    Ok(args)
}

fn authored_bypasses_hook_trust(authored_args: &[String]) -> Result<bool> {
    let boundary = interactive_root_prefix_end(authored_args)?;
    Ok(authored_args[..boundary]
        .iter()
        .any(|argument| argument == "--dangerously-bypass-hook-trust"))
}

/// Resolve the workspace whose non-managed hooks the remote TUI reviews before a resume.
///
/// st2 starts the wrapper in the declared workspace. An explicit Codex `--cd`/`-C` overrides it,
/// and the last occurrence wins just as the provider CLI does. The path must already exist because
/// both project-layer discovery and remote resume require a real directory.
fn controlled_hook_cwd(authored_args: &[String]) -> Result<PathBuf> {
    let boundary = interactive_root_prefix_end(authored_args)?;
    let mut selected = std::env::current_dir().context("reading controlled Codex workspace")?;
    let mut index = 0;
    while index < boundary {
        let argument = authored_args[index].as_str();
        if matches!(argument, "-C" | "--cd") {
            selected = PathBuf::from(&authored_args[index + 1]);
            index += 2;
            continue;
        }
        if let Some(value) = argument.strip_prefix("--cd=") {
            selected = PathBuf::from(value);
        } else if let Some(value) = argument.strip_prefix("-C")
            && !value.is_empty()
        {
            selected = PathBuf::from(value);
        }
        index += if matches!(
            argument,
            "-c" | "--config"
                | "--enable"
                | "--disable"
                | "--remote-auth-token-env"
                | "-m"
                | "--model"
                | "--local-provider"
                | "-p"
                | "--profile"
                | "-s"
                | "--sandbox"
                | "--add-dir"
                | "-a"
                | "--ask-for-approval"
        ) {
            2
        } else {
            1
        };
    }
    if selected.is_relative() {
        selected = std::env::current_dir()
            .context("reading controlled Codex workspace")?
            .join(selected);
    }
    fs::canonicalize(&selected).with_context(|| {
        format!(
            "resolving controlled Codex workspace {}",
            selected.display()
        )
    })
}

#[derive(Debug)]
struct HookTrustProjection {
    override_value: String,
    count: usize,
}

/// Codex 0.145/0.146 deliberately ignores the hook-trust bypass for startup review on every
/// persistent remote resume. Before the owning TUI starts, ask the same exact provider binary for
/// its typed hook keys and hashes, then project those hashes into the final app-server's session
/// flags. This implements the authored one-invocation bypass without writing persisted trust.
fn preflight_hook_trust(
    codex: &str,
    server_args: &[String],
    socket_path: &Path,
    cwd: &Path,
    log: &File,
    diagnostics: &mut WrapperDiagnostics,
) -> Result<Option<HookTrustProjection>> {
    diagnostics.record("hookTrustPreflightStarting", json!({}))?;
    let mut server_command = Command::new(codex);
    server_command
        .args(server_args)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log.try_clone()?);
    let mut server = spawn_process_group(&mut server_command, Some(socket_path))
        .with_context(|| format!("starting {codex} hook-trust preflight app-server"))?;
    let result = diagnostics
        .record("hookTrustPreflightStarted", json!({ "pid": server.id() }))
        .and_then(|_| {
            let Some(control) = connect_control(server.child_mut(), socket_path, STARTUP_TIMEOUT)?
            else {
                // Stop requested mid-preflight: skip the projection — the launch proceeds to the
                // connect stage, whose own stop check exits gracefully before the TUI starts.
                return Ok(None);
            };
            let Some(mut websocket) = initialize_control(control)? else {
                return Ok(None);
            };
            query_hook_trust_projection(&mut websocket, cwd)
        });
    server.terminate();
    let projection = result?;
    diagnostics.record(
        "hookTrustPreflightComplete",
        json!({ "projectedHookCount": projection.as_ref().map_or(0, |value| value.count) }),
    )?;
    Ok(projection)
}

fn query_hook_trust_projection(
    websocket: &mut WebSocket<UnixStream>,
    cwd: &Path,
) -> Result<Option<HookTrustProjection>> {
    write_json_message(
        websocket,
        &json!({
            "method": "hooks/list",
            "id": HOOK_TRUST_PREFLIGHT_REQUEST_ID,
            "params": { "cwds": [cwd.to_string_lossy()] },
        }),
    )?;
    websocket.get_ref().set_read_timeout(Some(CONTROL_POLL))?;
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let response = loop {
        match read_startup_message(websocket, deadline)? {
            StartupRead::Message(message)
                if message.get("id") == Some(&Value::from(HOOK_TRUST_PREFLIGHT_REQUEST_ID)) =>
            {
                break message;
            }
            StartupRead::Message(_) => continue,
            // A stop mid-preflight skips the projection; the launch's own stop checks exit
            // gracefully before the real server spawns anything further.
            StartupRead::Stopped => return Ok(None),
            StartupRead::Closed => {
                anyhow::bail!("Codex app-server closed during hook-trust preflight")
            }
        }
    };
    if let Some(error) = response.get("error") {
        anyhow::bail!("Codex app-server rejected hooks/list preflight: {error}");
    }
    hook_trust_projection_from_response(&response, cwd)
}

fn hook_trust_projection_from_response(
    response: &Value,
    cwd: &Path,
) -> Result<Option<HookTrustProjection>> {
    let data = response
        .pointer("/result/data")
        .and_then(Value::as_array)
        .context("Codex hooks/list preflight response has no typed data")?;
    anyhow::ensure!(
        data.len() == 1,
        "Codex hooks/list preflight returned {} cwd entries instead of one",
        data.len()
    );
    let entry = &data[0];
    anyhow::ensure!(
        entry.get("cwd").and_then(Value::as_str) == Some(cwd.to_string_lossy().as_ref()),
        "Codex hooks/list preflight returned a different cwd"
    );
    let hooks = entry
        .get("hooks")
        .and_then(Value::as_array)
        .context("Codex hooks/list preflight cwd entry has no typed hooks")?;
    let mut projected = BTreeMap::new();
    for hook in hooks {
        let status = hook
            .get("trustStatus")
            .and_then(Value::as_str)
            .context("Codex hooks/list preflight hook has no trustStatus")?;
        match status {
            "trusted" | "managed" => continue,
            "untrusted" | "modified" => {}
            other => {
                anyhow::bail!("Codex hooks/list preflight returned unknown trustStatus '{other}'")
            }
        }
        anyhow::ensure!(
            hook.get("isManaged").and_then(Value::as_bool) == Some(false),
            "Codex hooks/list preflight returned a managed hook requiring trust"
        );
        let key = hook
            .get("key")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("Codex hooks/list preflight hook has no non-empty key")?;
        let current_hash = hook
            .get("currentHash")
            .and_then(Value::as_str)
            .filter(|value| value.starts_with("sha256:") && value.len() > "sha256:".len())
            .context("Codex hooks/list preflight hook has no typed currentHash")?;
        if let Some(previous) = projected.insert(key.to_string(), current_hash.to_string()) {
            anyhow::ensure!(
                previous == current_hash,
                "Codex hooks/list preflight returned conflicting hashes for one hook key"
            );
        }
    }
    if projected.is_empty() {
        return Ok(None);
    }

    let mut state = toml::Table::new();
    for (key, current_hash) in projected {
        let mut trust = toml::Table::new();
        trust.insert(
            "trusted_hash".to_string(),
            toml::Value::String(current_hash),
        );
        state.insert(key, toml::Value::Table(trust));
    }
    Ok(Some(HookTrustProjection {
        count: state.len(),
        override_value: format!("hooks.state={}", toml::Value::Table(state)),
    }))
}

fn insert_app_server_config_override(
    server_args: &mut Vec<String>,
    override_value: String,
) -> Result<()> {
    let listen = server_args
        .iter()
        .position(|argument| argument == "--listen")
        .context("controlled Codex app-server argv has no --listen boundary")?;
    server_args.splice(listen..listen, ["-c".to_string(), override_value]);
    Ok(())
}

fn controlled_tui_args(
    endpoint: &str,
    authored_args: &[String],
    resume_thread: Option<&str>,
) -> Result<Vec<String>> {
    let mut args = vec!["--remote".to_string(), endpoint.to_string()];
    let Some(thread_id) = resume_thread else {
        args.extend_from_slice(authored_args);
        return Ok(args);
    };
    let Some(insertion) = resume_insertion_index(authored_args)? else {
        args.extend_from_slice(authored_args);
        return Ok(args);
    };
    args.push("resume".to_string());
    // Codex models these flags on the `resume` command as well as the root command. Keep them
    // before SESSION_ID so clap does not treat a following flag as the optional prompt.
    args.extend_from_slice(&authored_args[..insertion]);
    args.push(thread_id.to_string());
    args.extend_from_slice(&authored_args[insertion..]);
    Ok(args)
}

/// A saved binding constrains the watcher only when st2 inserted that resume selection.
///
/// An authored `resume` or `fork` command owns its own selection. The watcher binds the first typed
/// event from that command instead of rejecting it because it differs from an older saved binding.
fn expected_resume_thread<'a>(
    authored_args: &[String],
    resume_thread: Option<&'a str>,
) -> Result<Option<&'a str>> {
    let Some(thread_id) = resume_thread else {
        return Ok(None);
    };
    Ok(resume_insertion_index(authored_args)?
        .is_some()
        .then_some(thread_id))
}

/// Find where a supported Codex interactive argv begins its prompt or subcommand.
///
/// Automatic resume must insert `resume <thread>` after global options and before the authored
/// prompt. Unknown options fail closed because guessing can turn an option value into a prompt or a
/// prompt into a session selector. `--image` is variadic, so automatic resume requires an explicit
/// `--` boundary when that option is present.
fn resume_insertion_index(authored_args: &[String]) -> Result<Option<usize>> {
    let insertion = interactive_root_prefix_end(authored_args)?;
    if authored_args
        .get(insertion)
        .is_some_and(|argument| matches!(argument.as_str(), "resume" | "fork"))
    {
        Ok(None)
    } else {
        Ok(Some(insertion))
    }
}

fn interactive_root_prefix_end(authored_args: &[String]) -> Result<usize> {
    let delimiter = authored_args.iter().position(|arg| arg == "--");
    let mut index = 0;
    while index < authored_args.len() {
        let argument = authored_args[index].as_str();
        if argument == "--" {
            return Ok(index);
        }
        if !argument.starts_with('-') || argument == "-" {
            return Ok(index);
        }

        if matches!(
            argument,
            "--strict-config"
                | "--oss"
                | "--dangerously-bypass-approvals-and-sandbox"
                | "--dangerously-bypass-hook-trust"
                | "--search"
                | "--no-alt-screen"
        ) {
            index += 1;
            continue;
        }
        anyhow::ensure!(
            !matches!(argument, "-h" | "--help" | "-V" | "--version"),
            "cannot automatically resume a Codex help or version invocation"
        );

        let exact_value_option = matches!(
            argument,
            "-c" | "--config"
                | "--enable"
                | "--disable"
                | "--remote-auth-token-env"
                | "-m"
                | "--model"
                | "--local-provider"
                | "-p"
                | "--profile"
                | "-s"
                | "--sandbox"
                | "-C"
                | "--cd"
                | "--add-dir"
                | "-a"
                | "--ask-for-approval"
        );
        if exact_value_option {
            anyhow::ensure!(
                index + 1 < authored_args.len(),
                "Codex option '{argument}' has no value"
            );
            index += 2;
            continue;
        }
        if matches!(argument, "-i" | "--image")
            || argument.starts_with("-i=")
            || argument.starts_with("--image=")
        {
            let boundary = delimiter.context(
                "automatic Codex resume with variadic --image requires an explicit `--` prompt boundary",
            )?;
            return Ok(boundary);
        }

        let long_value = [
            "--config=",
            "--enable=",
            "--disable=",
            "--remote-auth-token-env=",
            "--model=",
            "--local-provider=",
            "--profile=",
            "--sandbox=",
            "--cd=",
            "--add-dir=",
            "--ask-for-approval=",
        ]
        .iter()
        .any(|prefix| argument.starts_with(prefix));
        let short_value = ["-c", "-m", "-p", "-s", "-C", "-a"]
            .iter()
            .any(|prefix| argument.starts_with(prefix) && argument.len() > prefix.len());
        anyhow::ensure!(
            long_value || short_value,
            "cannot automatically resume through unknown Codex option '{}'",
            diagnostic_option_name(argument)
        );
        index += 1;
    }
    Ok(authored_args.len())
}

fn diagnostic_option_name(argument: &str) -> String {
    if let Some((name, _)) = argument.split_once('=') {
        return name.to_string();
    }
    if argument.starts_with("--") {
        return argument.to_string();
    }
    argument.chars().take(2).collect()
}

fn connect_control(
    server: &mut Child,
    socket_path: &Path,
    timeout: Duration,
) -> Result<Option<UnixStream>> {
    let deadline = Instant::now() + timeout;
    loop {
        // st2's stop path may fire before the control socket ever connects; without this check
        // the wrapper would sit out the whole startup timeout with SIGTERM already delivered.
        if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(None);
        }
        match UnixStream::connect(socket_path) {
            Ok(stream) => return Ok(Some(stream)),
            Err(error) if Instant::now() < deadline => {
                if let Some(status) = server.try_wait()? {
                    anyhow::bail!("Codex app-server exited before control connected: {status}");
                }
                if error.kind() != std::io::ErrorKind::NotFound
                    && error.kind() != std::io::ErrorKind::ConnectionRefused
                {
                    return Err(error).with_context(|| {
                        format!("connecting Codex control socket {}", socket_path.display())
                    });
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Codex control socket {} was not ready within {}s",
                        socket_path.display(),
                        timeout.as_secs()
                    )
                });
            }
        }
    }
}

/// `Ok(None)` = a stop was raised mid-initialize; the caller exits gracefully.
fn initialize_control(stream: UnixStream) -> Result<Option<WebSocket<UnixStream>>> {
    // Nonblocking handshake reads produce resumable `Interrupted` states. This
    // avoids treating unrelated process signals as fatal socket I/O while
    // retaining a bounded stop-check cadence during a silent handshake.
    stream.set_nonblocking(true)?;
    let handshake_deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut pending = tungstenite::client("ws://localhost/", stream);
    let (mut websocket, response) = loop {
        match pending {
            Ok(done) => break done,
            Err(tungstenite::HandshakeError::Interrupted(resumable)) => {
                if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
                    return Ok(None);
                }
                anyhow::ensure!(
                    Instant::now() < handshake_deadline,
                    "Codex WebSocket handshake timed out"
                );
                std::thread::sleep(CONTROL_POLL);
                pending = resumable.handshake();
            }
            Err(tungstenite::HandshakeError::Failure(error)) => {
                if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
                    return Ok(None);
                }
                anyhow::bail!("Codex WebSocket handshake failed: {error}")
            }
        }
    };
    websocket.get_mut().set_nonblocking(false)?;
    websocket.get_mut().set_read_timeout(Some(CONTROL_POLL))?;
    anyhow::ensure!(
        response.status().as_u16() == 101,
        "Codex WebSocket handshake returned {}",
        response.status()
    );
    write_json_message(
        &mut websocket,
        &json!({
            "method": "initialize",
            "id": 0,
            "params": {
                "clientInfo": {
                    "name": "st2",
                    "title": "st2",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": { "experimentalApi": true }
            }
        }),
    )?;

    // Short socket timeouts make the stop flag observable through the up-to-30s wait; the
    // startup timeout is restored below so later control reads keep their semantics.
    websocket.get_ref().set_read_timeout(Some(CONTROL_POLL))?;
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        let message = match read_startup_message(&mut websocket, deadline)? {
            StartupRead::Message(message) => message,
            StartupRead::Stopped => return Ok(None),
            StartupRead::Closed => {
                anyhow::bail!("Codex app-server closed the control connection during initialize")
            }
        };
        if message.get("id") != Some(&Value::from(0)) {
            continue;
        }
        if let Some(error) = message.get("error") {
            anyhow::bail!("Codex app-server rejected initialize: {error}");
        }
        anyhow::ensure!(
            message.get("result").is_some(),
            "Codex app-server initialize response has no result"
        );
        break;
    }
    websocket
        .get_ref()
        .set_read_timeout(Some(STARTUP_TIMEOUT))?;
    write_json_message(
        &mut websocket,
        &json!({ "method": "initialized", "params": {} }),
    )?;
    websocket.get_ref().set_read_timeout(None)?;
    Ok(Some(websocket))
}

/// Wait until the owning TUI has loaded the preserved thread before this control connection
/// subscribes with its own `thread/resume` request.
///
/// Process creation is not ownership evidence. If control resumes immediately after spawn, it can
/// win the cold resume and create the session before the TUI has attached, so a successful control
/// response would not prove that the TUI consumed its authored prompt. `thread/loaded/list` is a
/// typed observation of the TUI's progress and is available in every admitted Codex version.
fn wait_for_tui_loaded_thread(
    websocket: &mut WebSocket<UnixStream>,
    expected_thread_id: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        eprintln!("codex control: requesting TUI-loaded thread list");
        write_json_message(
            websocket,
            &json!({
                "method": "thread/loaded/list",
                "id": CONTROL_TUI_LOADED_REQUEST_ID,
                "params": {},
            }),
        )?;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            anyhow::ensure!(
                remaining >= Duration::from_millis(1),
                "controlled Codex TUI did not load preserved thread {expected_thread_id} before control resume"
            );
            websocket
                .get_ref()
                .set_read_timeout(Some(remaining.min(CONTROL_POLL)))?;
            let message = match poll_json_message(websocket)
                .context("polling Codex TUI-loaded response")?
            {
                ControlRead::Message(message) => message,
                ControlRead::Timeout => continue,
                ControlRead::Closed => anyhow::bail!(
                    "Codex app-server closed the control connection while waiting for the TUI to load preserved thread {expected_thread_id}"
                ),
            };
            if message.get("id") != Some(&Value::from(CONTROL_TUI_LOADED_REQUEST_ID)) {
                continue;
            }
            if let Some(error) = message.get("error") {
                anyhow::bail!("Codex app-server rejected thread/loaded/list: {error}");
            }
            let loaded = message
                .pointer("/result/data")
                .and_then(Value::as_array)
                .context("Codex thread/loaded/list response has no typed data")?;
            let contains_expected = loaded.iter().try_fold(false, |found, thread_id| {
                let thread_id = thread_id
                    .as_str()
                    .context("Codex thread/loaded/list returned a non-string thread id")?;
                Ok::<_, anyhow::Error>(found || thread_id == expected_thread_id)
            })?;
            if contains_expected {
                return Ok(());
            }
            break;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        anyhow::ensure!(
            !remaining.is_zero(),
            "controlled Codex TUI did not load preserved thread {expected_thread_id} before control resume"
        );
        thread::sleep(remaining.min(CONTROL_POLL));
    }
}

#[derive(Debug)]
enum ControlEvent {
    TuiThreadLoaded(Sender<()>),
    Bound,
    Observed,
    Closed,
    Failed(String),
}

struct ControlResume<'a> {
    thread_id: &'a str,
    ready: Receiver<()>,
    tui_loaded_timeout: Duration,
}

fn pump_control(
    mut websocket: WebSocket<UnixStream>,
    binding_path: &Path,
    control_state_path: &Path,
    runtime: &CodexRuntime,
    resume: Option<ControlResume<'_>>,
    delivery: Option<CodexDeliveryConfig>,
    events: Sender<ControlEvent>,
) {
    let result = (|| -> Result<()> {
        let (expected_resume, resume_ready, tui_loaded_timeout) = match resume {
            Some(resume) => (
                Some(resume.thread_id),
                Some(resume.ready),
                resume.tui_loaded_timeout,
            ),
            None => (None, None, TUI_LOADED_TIMEOUT),
        };
        let mut control_state: Option<CodexControlState> = None;
        let mut subscription_pending = false;
        let mut peer_closed = false;
        let delivery_state_path = control_state_path.with_file_name(delivery_ledger::LEGACY_FILE);
        let mut delivery = delivery
            .map(|config| {
                CodexInboxDelivery::new(config, delivery_state_path.clone(), runtime.clone())
            })
            .transpose()
            .context("initializing Codex inbox delivery")?;
        if let Some(thread_id) = expected_resume {
            resume_ready
                .context("saved Codex binding has no TUI-start gate")?
                .recv()
                .context("controlled Codex TUI ended before control resume")?;
            wait_for_tui_loaded_thread(&mut websocket, thread_id, tui_loaded_timeout)
                .context("waiting for Codex TUI thread load")?;
            let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
            eprintln!("codex control: emitting TuiThreadLoaded");
            events
                .send(ControlEvent::TuiThreadLoaded(diagnostic_tx))
                .context("recording that the Codex TUI loaded the preserved thread")?;
            diagnostic_rx
                .recv()
                .context("waiting for the Codex TUI-loaded diagnostic before control resume")?;
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/resume",
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "params": { "threadId": thread_id }
                }),
            )
            .context("sending Codex thread resume request")?;
            subscription_pending = true;
        }
        loop {
            if !peer_closed {
                if let Err(error) = websocket.get_ref().set_read_timeout(Some(CONTROL_POLL)) {
                    if error.kind() == std::io::ErrorKind::InvalidInput {
                        // Darwin can reject setsockopt after the peer has closed
                        // the Unix socket. Keep reading: buffered WebSocket
                        // frames must be processed before EOF is reported.
                        peer_closed = true;
                        let _ = websocket.get_ref().set_read_timeout(None);
                    } else {
                        return Err(error).context("setting Codex control poll timeout");
                    }
                }
            }
            let message =
                match poll_json_message(&mut websocket).context("polling Codex control socket")? {
                    ControlRead::Message(message) => Some(message),
                    ControlRead::Timeout => None,
                    ControlRead::Closed => {
                        let _ = events.send(ControlEvent::Closed);
                        return Ok(());
                    }
                };
            let Some(message) = message else {
                if let (Some(state), Some(delivery)) = (control_state.as_ref(), delivery.as_mut())
                    && let Some(request) = delivery.maybe_request(state)?
                {
                    write_json_message(&mut websocket, &request)
                        .context("sending Codex delivery request")?;
                }
                continue;
            };
            if control_state.is_none() {
                if let Some(thread_id) = expected_resume {
                    if message.get("method").is_some()
                        || message.get("id") != Some(&Value::from(CONTROL_SUBSCRIBE_REQUEST_ID))
                    {
                        // The one notification worth reading before the resume response lands. The
                        // app-server replays `thread/tokenUsage/updated` to a newly attached
                        // connection, and the resumed thread still holds its context — so dropping
                        // it here would leave a seat that resumes and then waits for work reading
                        // `context: null` against a full window, with nothing to correct it until
                        // the next model response. The claim this construction already made removed
                        // the predecessor's record, so there is nothing else to fall back on.
                        //
                        // Ordering-agnostic on purpose: if the replay arrives after the response
                        // the loop below already sees it and this call reads nothing. A duplicate
                        // reading costs nothing — the bucket guard skips it.
                        //
                        // The fresh-binding path below needs no such call: there is no thread id
                        // until `binding_candidate` names one, and a thread starting now has no
                        // history to replay.
                        if let Some(delivery) = delivery.as_mut() {
                            delivery.observe_context(&message, thread_id);
                        }
                        continue;
                    }
                    anyhow::ensure!(
                        subscription_pending,
                        "Codex control received an unexpected initial thread/resume response"
                    );
                    subscription_pending = false;
                    let mut bound = CodexControlState::new(runtime, thread_id.to_string());
                    match bound
                        .accept_subscription(&message)
                        .context("accepting Codex resume subscription")?
                    {
                        SubscriptionAcceptance::Accepted { .. } => {
                            if let Some(delivery) = delivery.as_mut() {
                                delivery
                                    .reconcile_resume(&message, &bound)
                                    .context("reconciling Codex resume delivery")?;
                            }
                        }
                        SubscriptionAcceptance::Deferred => anyhow::bail!(
                            "saved Codex resume binding has no persisted rollout for thread {thread_id}"
                        ),
                    }
                    atomic_json(
                        binding_path,
                        &CodexThreadBinding::new(runtime, thread_id.to_string()),
                    )
                    .context("persisting Codex resume binding")?;
                    atomic_json(control_state_path, &bound)
                        .context("persisting Codex control state")?;
                    if let Some(delivery) = delivery.as_mut() {
                        delivery.observe_harness(&bound.observed);
                    }
                    control_state = Some(bound);
                    let _ = events.send(ControlEvent::Bound);
                    continue;
                }

                let Some(thread_id) = binding_candidate(&message)
                    .context("reading Codex thread binding candidate")?
                else {
                    continue;
                };
                atomic_json(
                    binding_path,
                    &CodexThreadBinding::new(runtime, thread_id.to_string()),
                )
                .context("persisting Codex fresh binding")?;
                let mut bound = CodexControlState::new(runtime, thread_id.to_string());
                // A fresh control client that observes the owning TUI's `thread/started`
                // notification is already subscribed to that thread's broadcasts. Before its
                // first turn there is no persisted rollout for a redundant `thread/resume`.
                bound.subscribed = true;
                atomic_json(control_state_path, &bound)
                    .context("persisting Codex fresh control state")?;
                if let Some(delivery) = delivery.as_mut() {
                    delivery.observe_harness(&bound.observed);
                }
                control_state = Some(bound);
                let _ = events.send(ControlEvent::Bound);
            }

            let state = control_state
                .as_mut()
                .context("Codex control state is unbound")?;
            // The context record's whole input, taken before the delivery and state branches
            // because none of them reads a token count and every one of them may `continue`.
            //
            // Deliberately after binding: both unbound paths above skip any frame carrying a
            // `method`, so a `thread/tokenUsage/updated` replayed to a freshly attached connection
            // ahead of the resume response is dropped. The consequence is bounded — Codex emits
            // another reading on the next model response, roughly 10-15 per turn — and the record
            // is honest about the gap through `ageMs` meanwhile, which is cheaper than teaching the
            // binding handshake to hold observability frames it has no state to attribute yet.
            if let Some(delivery) = delivery.as_mut() {
                delivery.observe_context(&message, state.thread_id());
                // The credential axis, taken here for the same reason: it reads a typed turn
                // result no branch below looks at, and every one of them may `continue`.
                delivery.observe_provider_auth(&message, state.thread_id());
            }
            let delivery_response = match delivery.as_mut() {
                Some(delivery) => {
                    delivery
                        .accept_response(&message, &state.observed)
                        .context("accepting Codex delivery response")?
                        || delivery
                            .accept_typed_receipt(&message, state)
                            .context("accepting Codex typed receipt")?
                }
                None => false,
            };
            let changed = if delivery_response {
                false
            } else if message.get("method").is_none()
                && message.get("id") == Some(&Value::from(CONTROL_SUBSCRIBE_REQUEST_ID))
            {
                anyhow::ensure!(
                    subscription_pending,
                    "Codex control received an unexpected thread/resume response"
                );
                subscription_pending = false;
                match state
                    .accept_subscription(&message)
                    .context("accepting Codex subscription")?
                {
                    SubscriptionAcceptance::Accepted { changed } => {
                        if let Some(delivery) = delivery.as_mut() {
                            delivery
                                .reconcile_resume(&message, state)
                                .context("reconciling Codex subscription delivery")?;
                        }
                        changed
                    }
                    SubscriptionAcceptance::Deferred => false,
                }
            } else {
                state
                    .observe(&message)
                    .context("observing Codex control event")?
            };
            if changed {
                atomic_json(control_state_path, state)
                    .context("persisting Codex observed control state")?;
                if let Some(delivery) = delivery.as_mut() {
                    delivery.observe_harness(&state.observed);
                }
                let _ = events.send(ControlEvent::Observed);
            }
            if !state.subscribed
                && !subscription_pending
                && subscription_candidate(&message, state.thread_id())
            {
                write_json_message(
                    &mut websocket,
                    &json!({
                        "method": "thread/resume",
                        "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                        "params": { "threadId": state.thread_id }
                    }),
                )
                .context("sending Codex subscription request")?;
                subscription_pending = true;
            }
            if let Some(delivery) = delivery.as_mut()
                && let Some(request) = delivery.maybe_request(state)?
            {
                write_json_message(&mut websocket, &request)
                    .context("sending Codex delivery request")?;
            }
        }
    })();
    if let Err(error) = result {
        let _ = events.send(ControlEvent::Failed(format!("{error:#}")));
    }
}

fn subscription_candidate(message: &Value, thread_id: &str) -> bool {
    match message.get("method").and_then(Value::as_str) {
        Some("thread/started") => {
            message.pointer("/params/thread/id").and_then(Value::as_str) == Some(thread_id)
                && matches!(
                    message
                        .pointer("/params/thread/status/type")
                        .and_then(Value::as_str),
                    Some("idle" | "active")
                )
        }
        Some("thread/status/changed") => {
            message.pointer("/params/threadId").and_then(Value::as_str) == Some(thread_id)
                && matches!(
                    message
                        .pointer("/params/status/type")
                        .and_then(Value::as_str),
                    Some("idle" | "active")
                )
        }
        _ => false,
    }
}

fn binding_candidate(message: &Value) -> Result<Option<&str>> {
    match message.get("method").and_then(Value::as_str) {
        Some("thread/started") => {
            let thread_id = required_string(message, "/params/thread/id", "thread/started")?;
            Ok(Some(thread_id))
        }
        _ => Ok(None),
    }
}

/// How the binding wait ended: the thread bound, or st2's stop flag ended the session first.
enum BindingWait {
    Bound,
    Stopped,
}

fn wait_for_binding(
    tui: &mut Child,
    events: &Receiver<ControlEvent>,
    timeout: Duration,
    diagnostics: &mut WrapperDiagnostics,
) -> Result<BindingWait> {
    let deadline = Instant::now() + timeout;
    loop {
        if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(BindingWait::Stopped);
        }
        if let Some(status) = tui.try_wait()? {
            anyhow::bail!("controlled Codex TUI exited before thread binding: {status}");
        }
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(CONTROL_POLL);
        if wait.is_zero() {
            anyhow::bail!(
                "controlled Codex TUI did not establish typed thread ownership within {}s",
                timeout.as_secs()
            );
        }
        match events.recv_timeout(wait) {
            Ok(ControlEvent::TuiThreadLoaded(acknowledge)) => {
                diagnostics.record("tuiThreadLoaded", json!({ "pid": tui.id() }))?;
                let _ = acknowledge.send(());
            }
            Ok(ControlEvent::Bound) => return Ok(BindingWait::Bound),
            Ok(ControlEvent::Observed) => {}
            Ok(ControlEvent::Closed) => {
                anyhow::bail!("Codex control connection closed before thread binding")
            }
            Ok(ControlEvent::Failed(error)) => {
                anyhow::bail!("Codex control failed before thread binding: {error}")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("Codex control observer ended before thread binding")
            }
        }
    }
}

fn monitor_bound_tui(tui: &mut Child, events: &Receiver<ControlEvent>) -> Result<TuiEnd> {
    loop {
        if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
            // st2's stop path: end the session and return through the ordinary terminal-write
            // path so the record carries the observed outcome before the wrapper exits.
            terminate_child(tui);
            return Ok(TuiEnd::Stopped(tui.try_wait().ok().flatten()));
        }
        if let Some(status) = tui.try_wait()? {
            return Ok(TuiEnd::Exited(status));
        }
        match events.recv_timeout(CONTROL_POLL) {
            Ok(ControlEvent::TuiThreadLoaded(acknowledge)) => {
                let _ = acknowledge.send(());
            }
            Ok(ControlEvent::Bound) => {}
            Ok(ControlEvent::Observed) => {}
            Ok(ControlEvent::Closed) => {
                anyhow::bail!("Codex control connection closed while the TUI was live")
            }
            Ok(ControlEvent::Failed(error)) => {
                anyhow::bail!("Codex control failed while the TUI was live: {error}")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("Codex control observer ended while the TUI was live")
            }
        }
    }
}

fn completed_tui(status: ExitStatus) -> Result<()> {
    anyhow::ensure!(
        status.success(),
        "controlled Codex TUI exited with {status}"
    );
    Ok(())
}

struct CodexProtocolSchemas {
    protocol: Value,
    client_requests: Value,
    client_notifications: Value,
    server_requests: Value,
    server_notifications: Value,
}

/// Admit the installed Codex app-server protocol, returning the version that passed.
fn ensure_supported_protocol(codex: &str) -> Result<String> {
    let version = codex_version(codex)?;
    let generated = tempfile::Builder::new()
        .prefix("st2-codex-protocol-")
        .tempdir()
        .context("creating a temporary Codex protocol schema directory")?;
    let output = Command::new(codex)
        .args([
            "app-server",
            "generate-json-schema",
            "--experimental",
            "--out",
        ])
        .arg(generated.path())
        .output()
        .with_context(|| format!("generating the Codex app-server schema from {codex}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{codex} app-server schema generation failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let read_schema = |name: &str| -> Result<Value> {
        let path = generated.path().join(name);
        let bytes =
            fs::read(&path).with_context(|| format!("reading generated Codex schema {name}"))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing generated Codex schema {name}"))
    };
    let schemas = CodexProtocolSchemas {
        protocol: read_schema("codex_app_server_protocol.v2.schemas.json")?,
        client_requests: read_schema("ClientRequest.json")?,
        client_notifications: read_schema("ClientNotification.json")?,
        server_requests: read_schema("ServerRequest.json")?,
        server_notifications: read_schema("ServerNotification.json")?,
    };
    verify_codex_protocol_schemas(&schemas)
        .with_context(|| format!("Codex app-server schema from {version} is incompatible"))?;
    Ok(version)
}

fn codex_version(codex: &str) -> Result<String> {
    let mut attempt_index = 0;
    let output = loop {
        let attempt = Command::new(codex).arg("--version").output();
        match attempt {
            Ok(output) => break output,
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) && attempt_index + 1 < 5 => {
                // Some Linux filesystems briefly retain writer exclusion after a binary install.
                // Retry only this transient error and keep every other launch error immediate.
                attempt_index += 1;
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => {
                return Err(error).with_context(|| format!("reading Codex version from {codex}"));
            }
        }
    };
    anyhow::ensure!(
        output.status.success(),
        "{codex} --version failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let actual = String::from_utf8(output.stdout)
        .context("Codex version output is not UTF-8")?
        .trim()
        .to_string();
    anyhow::ensure!(!actual.is_empty(), "{codex} --version printed nothing");
    Ok(actual)
}

fn verify_codex_protocol_schemas(schemas: &CodexProtocolSchemas) -> Result<()> {
    let definitions = schemas
        .protocol
        .get("definitions")
        .and_then(Value::as_object)
        .context("aggregate schema has no definitions object")?;

    require_methods(
        &schemas.client_requests,
        REQUIRED_CODEX_CLIENT_REQUESTS,
        "client request",
    )?;
    require_methods(
        &schemas.client_notifications,
        REQUIRED_CODEX_CLIENT_NOTIFICATIONS,
        "client notification",
    )?;
    require_methods(
        &schemas.server_notifications,
        REQUIRED_CODEX_SERVER_NOTIFICATIONS,
        "server notification",
    )?;
    schema_methods(&schemas.server_requests, "server request")?;

    let status_variants = schema_variants(definitions, "ThreadStatus", "type")?;
    for status in ["notLoaded", "idle", "systemError", "active"] {
        anyhow::ensure!(
            status_variants.contains_key(status),
            "ThreadStatus has no '{status}' variant"
        );
    }
    let active = status_variants
        .get("active")
        .context("ThreadStatus has no active variant")?;
    let active_flags =
        required_property(definitions, active, "activeFlags", "ThreadStatus.active")?;
    let active_flag = require_array(definitions, active_flags, "ThreadStatus.activeFlags")?;
    anyhow::ensure!(
        active_flag == schema_definition(definitions, "ThreadActiveFlag")?,
        "ThreadStatus.activeFlags does not contain ThreadActiveFlag"
    );
    let actual_active_flags = schema_enum(definitions, "ThreadActiveFlag")?;
    anyhow::ensure!(
        actual_active_flags == string_set(&["waitingOnApproval", "waitingOnUserInput"]),
        "ThreadActiveFlag changed: {}",
        actual_active_flags
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ")
    );

    let item_variants = schema_variants(definitions, "ThreadItem", "type")?;
    for item in [
        "contextCompaction",
        "enteredReviewMode",
        "exitedReviewMode",
        "userMessage",
    ] {
        anyhow::ensure!(
            item_variants.contains_key(item),
            "ThreadItem has no '{item}' variant"
        );
    }
    let user_message = item_variants
        .get("userMessage")
        .context("ThreadItem has no userMessage variant")?;
    require_property_type(
        definitions,
        user_message,
        "clientId",
        "string",
        false,
        "ThreadItem.userMessage",
    )?;

    let user_input_variants = schema_variants(definitions, "UserInput", "type")?;
    let text_input = user_input_variants
        .get("text")
        .context("UserInput has no text variant")?;
    require_property_type(
        definitions,
        text_input,
        "text",
        "string",
        true,
        "UserInput.text",
    )?;
    let text_elements = property(definitions, text_input, "text_elements", "UserInput.text")?;
    require_array(definitions, text_elements, "UserInput.text.text_elements")?;

    for (definition, path) in [
        ("ClientInfo", &["name"][..]),
        ("ClientInfo", &["version"][..]),
        ("Thread", &["id"][..]),
        ("Turn", &["id"][..]),
        ("ThreadStatusChangedNotification", &["threadId"][..]),
        ("TurnStartedNotification", &["threadId"][..]),
        ("TurnStartedNotification", &["turn", "id"][..]),
        ("TurnCompletedNotification", &["threadId"][..]),
        ("TurnCompletedNotification", &["turn", "id"][..]),
        ("ItemStartedNotification", &["threadId"][..]),
        ("ItemStartedNotification", &["turnId"][..]),
        ("ItemCompletedNotification", &["threadId"][..]),
        ("ItemCompletedNotification", &["turnId"][..]),
        ("ThreadStartedNotification", &["thread", "id"][..]),
        ("ThreadResumeParams", &["threadId"][..]),
        ("ThreadResumeResponse", &["thread", "id"][..]),
        ("TurnStartParams", &["threadId"][..]),
        ("TurnStartResponse", &["turn", "id"][..]),
        ("TurnSteerParams", &["threadId"][..]),
        ("TurnSteerParams", &["expectedTurnId"][..]),
        ("TurnSteerResponse", &["turnId"][..]),
    ] {
        let schema = required_schema_path(definitions, definition, path)?;
        require_type(
            definitions,
            schema,
            "string",
            &format!("{definition}.{}", path.join(".")),
        )?;
    }

    require_property_type(
        definitions,
        schema_definition(definitions, "ClientInfo")?,
        "title",
        "string",
        false,
        "ClientInfo",
    )?;
    require_property_type(
        definitions,
        schema_definition(definitions, "InitializeCapabilities")?,
        "experimentalApi",
        "boolean",
        false,
        "InitializeCapabilities",
    )?;
    required_schema_path(definitions, "InitializeParams", &["clientInfo"])?;

    let thread_status = required_schema_path(definitions, "Thread", &["status"])?;
    anyhow::ensure!(
        thread_status == schema_definition(definitions, "ThreadStatus")?,
        "Thread.status does not use ThreadStatus"
    );
    let resume_status =
        required_schema_path(definitions, "ThreadResumeResponse", &["thread", "status"])?;
    anyhow::ensure!(
        resume_status == schema_definition(definitions, "ThreadStatus")?,
        "ThreadResumeResponse.thread.status does not use ThreadStatus"
    );
    let started_status = required_schema_path(
        definitions,
        "ThreadStartedNotification",
        &["thread", "status"],
    )?;
    anyhow::ensure!(
        started_status == schema_definition(definitions, "ThreadStatus")?,
        "ThreadStartedNotification.thread.status does not use ThreadStatus"
    );
    let changed_status =
        required_schema_path(definitions, "ThreadStatusChangedNotification", &["status"])?;
    anyhow::ensure!(
        changed_status == schema_definition(definitions, "ThreadStatus")?,
        "ThreadStatusChangedNotification.status does not use ThreadStatus"
    );

    let turns = required_schema_path(definitions, "Thread", &["turns"])?;
    let turn = require_array(definitions, turns, "Thread.turns")?;
    anyhow::ensure!(
        turn == schema_definition(definitions, "Turn")?,
        "Thread.turns does not contain Turn"
    );
    let items = required_schema_path(definitions, "Turn", &["items"])?;
    let item = require_array(definitions, items, "Turn.items")?;
    anyhow::ensure!(
        item == schema_definition(definitions, "ThreadItem")?,
        "Turn.items does not contain ThreadItem"
    );
    // The typed turn result the provider-credential classifier reads. A release that renames the
    // status word, drops the failure's typed error, or merges the credential arm into a quota arm
    // must refuse the launch rather than let st2 silently stop classifying rejections — or, worse,
    // report an exhausted allowance as a rejected credential.
    let turn_status = required_schema_path(definitions, "Turn", &["status"])?;
    anyhow::ensure!(
        turn_status == schema_definition(definitions, "TurnStatus")?,
        "Turn.status does not use TurnStatus"
    );
    let turn_statuses = schema_enum(definitions, "TurnStatus")?;
    for status in ["completed", "failed"] {
        anyhow::ensure!(
            turn_statuses.contains(status),
            "TurnStatus has no '{status}' variant"
        );
    }
    let turn_error = nullable_schema(
        definitions,
        property(
            definitions,
            schema_definition(definitions, "Turn")?,
            "error",
            "Turn",
        )?,
        "Turn.error",
    )?;
    anyhow::ensure!(
        turn_error == schema_definition(definitions, "TurnError")?,
        "Turn.error does not use TurnError"
    );
    let error_info = nullable_schema(
        definitions,
        property(
            definitions,
            schema_definition(definitions, "TurnError")?,
            "codexErrorInfo",
            "TurnError",
        )?,
        "TurnError.codexErrorInfo",
    )?;
    anyhow::ensure!(
        error_info == schema_definition(definitions, "CodexErrorInfo")?,
        "TurnError.codexErrorInfo does not use CodexErrorInfo"
    );
    let error_words = schema_variant_words(definitions, "CodexErrorInfo")?;
    for word in [
        CODEX_PROVIDER_AUTH_REJECTED,
        "rateLimitExceeded",
        "usageLimitExceeded",
    ] {
        anyhow::ensure!(
            error_words.contains(word),
            "CodexErrorInfo has no '{word}' word"
        );
    }
    for notification in ["ItemStartedNotification", "ItemCompletedNotification"] {
        let item = required_schema_path(definitions, notification, &["item"])?;
        anyhow::ensure!(
            item == schema_definition(definitions, "ThreadItem")?,
            "{notification}.item does not use ThreadItem"
        );
    }

    for params in ["TurnStartParams", "TurnSteerParams"] {
        let input = required_schema_path(definitions, params, &["input"])?;
        let input_item = require_array(definitions, input, &format!("{params}.input"))?;
        anyhow::ensure!(
            input_item == schema_definition(definitions, "UserInput")?,
            "{params}.input does not contain UserInput"
        );
        require_property_type(
            definitions,
            schema_definition(definitions, params)?,
            "clientUserMessageId",
            "string",
            false,
            params,
        )?;
    }
    let loaded = required_schema_path(definitions, "ThreadLoadedListResponse", &["data"])?;
    let loaded_item = require_array(definitions, loaded, "ThreadLoadedListResponse.data")?;
    require_type(
        definitions,
        loaded_item,
        "string",
        "ThreadLoadedListResponse.data item",
    )?;
    let hook_cwds = property(
        definitions,
        schema_definition(definitions, "HooksListParams")?,
        "cwds",
        "HooksListParams",
    )?;
    let hook_cwd = require_array(definitions, hook_cwds, "HooksListParams.cwds")?;
    require_type(definitions, hook_cwd, "string", "HooksListParams.cwds item")?;
    verify_hook_schema(definitions)?;
    Ok(())
}

fn verify_hook_schema(definitions: &serde_json::Map<String, Value>) -> Result<()> {
    let data = required_schema_path(definitions, "HooksListResponse", &["data"])?;
    let entry = require_array(definitions, data, "HooksListResponse.data")?;
    anyhow::ensure!(
        entry == schema_definition(definitions, "HooksListEntry")?,
        "HooksListResponse.data does not contain HooksListEntry"
    );
    let hooks = required_schema_path(definitions, "HooksListEntry", &["hooks"])?;
    let hook = require_array(definitions, hooks, "HooksListEntry.hooks")?;
    anyhow::ensure!(
        hook == schema_definition(definitions, "HookMetadata")?,
        "HooksListEntry.hooks does not contain HookMetadata"
    );
    for (property, expected_type) in [
        ("currentHash", "string"),
        ("isManaged", "boolean"),
        ("key", "string"),
    ] {
        require_property_type(
            definitions,
            schema_definition(definitions, "HookMetadata")?,
            property,
            expected_type,
            true,
            "HookMetadata",
        )?;
    }
    let trust_status = required_schema_path(definitions, "HookMetadata", &["trustStatus"])?;
    anyhow::ensure!(
        trust_status == schema_definition(definitions, "HookTrustStatus")?,
        "HookMetadata.trustStatus does not use HookTrustStatus"
    );
    let statuses = schema_enum(definitions, "HookTrustStatus")?;
    anyhow::ensure!(
        statuses == string_set(&["managed", "modified", "trusted", "untrusted"]),
        "HookTrustStatus changed: {}",
        statuses.into_iter().collect::<Vec<_>>().join(", ")
    );
    Ok(())
}

fn string_set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn schema_methods(schema: &Value, label: &str) -> Result<BTreeSet<String>> {
    let arms = schema
        .get("oneOf")
        .and_then(Value::as_array)
        .with_context(|| format!("{label} schema has no oneOf array"))?;
    let mut methods = BTreeSet::new();
    for arm in arms {
        let required = arm
            .get("required")
            .and_then(Value::as_array)
            .with_context(|| format!("{label} arm has no required array"))?;
        anyhow::ensure!(
            required
                .iter()
                .any(|value| value.as_str() == Some("method")),
            "{label} arm does not require method"
        );
        let values = arm
            .pointer("/properties/method/enum")
            .and_then(Value::as_array)
            .with_context(|| format!("{label} arm has no method enum"))?;
        anyhow::ensure!(values.len() == 1, "{label} arm method enum is not exact");
        let method = values[0]
            .as_str()
            .with_context(|| format!("{label} arm method is not a string"))?;
        anyhow::ensure!(
            methods.insert(method.to_string()),
            "{label} method '{method}' is duplicated"
        );
    }
    Ok(methods)
}

fn require_methods(schema: &Value, required: &[&str], label: &str) -> Result<()> {
    let methods = schema_methods(schema, label)?;
    let missing = string_set(required)
        .difference(&methods)
        .cloned()
        .collect::<Vec<_>>();
    anyhow::ensure!(
        missing.is_empty(),
        "missing {label} methods: {}",
        missing.join(", ")
    );
    Ok(())
}

fn schema_definition<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    name: &str,
) -> Result<&'a Value> {
    definitions
        .get(name)
        .with_context(|| format!("aggregate schema has no {name} definition"))
}

fn resolve_schema<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    mut schema: &'a Value,
) -> Result<&'a Value> {
    for _ in 0..16 {
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            let name = reference
                .strip_prefix("#/definitions/")
                .with_context(|| format!("unsupported schema reference '{reference}'"))?;
            schema = schema_definition(definitions, name)?;
            continue;
        }
        if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
            anyhow::ensure!(all_of.len() == 1, "schema allOf is not a single reference");
            schema = &all_of[0];
            continue;
        }
        return Ok(schema);
    }
    anyhow::bail!("schema reference depth exceeds 16")
}

/// Resolve `anyOf: [T, null]` — the shape the Codex generator emits for an optional typed field —
/// to `T`. A field that is not exactly one typed arm beside `null` is refused rather than guessed.
fn nullable_schema<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    schema: &'a Value,
    label: &str,
) -> Result<&'a Value> {
    let schema = resolve_schema(definitions, schema)?;
    let arms = schema
        .get("anyOf")
        .and_then(Value::as_array)
        .with_context(|| format!("{label} is not a nullable schema"))?;
    let mut typed = arms
        .iter()
        .filter(|arm| arm.get("type").and_then(Value::as_str) != Some("null"));
    let only = typed
        .next()
        .with_context(|| format!("{label} has no typed arm"))?;
    anyhow::ensure!(
        typed.next().is_none(),
        "{label} has more than one typed arm"
    );
    resolve_schema(definitions, only)
}

/// Every unit word of a `oneOf` union that mixes a string enum with data-carrying object arms —
/// the shape `CodexErrorInfo` has. Only the enum arms carry words st2 can match on.
fn schema_variant_words(
    definitions: &serde_json::Map<String, Value>,
    definition: &str,
) -> Result<BTreeSet<String>> {
    let arms = schema_definition(definitions, definition)?
        .get("oneOf")
        .and_then(Value::as_array)
        .with_context(|| format!("{definition} has no oneOf variants"))?;
    let mut words = BTreeSet::new();
    for arm in arms {
        let Some(values) = resolve_schema(definitions, arm)?
            .get("enum")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for value in values {
            let word = value
                .as_str()
                .with_context(|| format!("{definition} has a non-string enum value"))?;
            words.insert(word.to_string());
        }
    }
    anyhow::ensure!(!words.is_empty(), "{definition} has no enum words");
    Ok(words)
}

fn schema_variants<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    definition: &str,
    discriminator: &str,
) -> Result<BTreeMap<String, &'a Value>> {
    let schema = schema_definition(definitions, definition)?;
    let variants = schema
        .get("oneOf")
        .and_then(Value::as_array)
        .with_context(|| format!("{definition} has no oneOf variants"))?;
    let mut found = BTreeMap::new();
    for variant in variants {
        let variant = resolve_schema(definitions, variant)?;
        let required = variant
            .get("required")
            .and_then(Value::as_array)
            .with_context(|| format!("{definition} variant has no required array"))?;
        anyhow::ensure!(
            required
                .iter()
                .any(|value| value.as_str() == Some(discriminator)),
            "{definition} variant does not require {discriminator}"
        );
        let values = variant
            .pointer(&format!("/properties/{discriminator}/enum"))
            .and_then(Value::as_array)
            .with_context(|| format!("{definition} variant has no {discriminator} enum"))?;
        anyhow::ensure!(
            values.len() == 1,
            "{definition} variant discriminator is not exact"
        );
        let value = values[0]
            .as_str()
            .with_context(|| format!("{definition} discriminator is not a string"))?;
        anyhow::ensure!(
            found.insert(value.to_string(), variant).is_none(),
            "{definition} discriminator '{value}' is duplicated"
        );
    }
    Ok(found)
}

fn schema_enum(
    definitions: &serde_json::Map<String, Value>,
    definition: &str,
) -> Result<BTreeSet<String>> {
    let values = schema_definition(definitions, definition)?
        .get("enum")
        .and_then(Value::as_array)
        .with_context(|| format!("{definition} has no enum"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .with_context(|| format!("{definition} has a non-string enum value"))
        })
        .collect()
}

fn property<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    schema: &'a Value,
    name: &str,
    label: &str,
) -> Result<&'a Value> {
    let schema = resolve_schema(definitions, schema)?;
    let property = schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get(name))
        .with_context(|| format!("{label} has no {name} property"))?;
    resolve_schema(definitions, property)
}

fn required_property<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    schema: &'a Value,
    name: &str,
    label: &str,
) -> Result<&'a Value> {
    let schema = resolve_schema(definitions, schema)?;
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .with_context(|| format!("{label} has no required array"))?;
    anyhow::ensure!(
        required.iter().any(|value| value.as_str() == Some(name)),
        "{label} does not require {name}"
    );
    property(definitions, schema, name, label)
}

fn required_schema_path<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    definition: &str,
    path: &[&str],
) -> Result<&'a Value> {
    let mut schema = schema_definition(definitions, definition)?;
    let mut label = definition.to_string();
    for component in path {
        schema = required_property(definitions, schema, component, &label)?;
        label.push('.');
        label.push_str(component);
    }
    Ok(schema)
}

fn require_property_type(
    definitions: &serde_json::Map<String, Value>,
    schema: &Value,
    property_name: &str,
    expected_type: &str,
    required: bool,
    label: &str,
) -> Result<()> {
    let property = if required {
        required_property(definitions, schema, property_name, label)?
    } else {
        property(definitions, schema, property_name, label)?
    };
    require_type(
        definitions,
        property,
        expected_type,
        &format!("{label}.{property_name}"),
    )
}

fn require_type(
    definitions: &serde_json::Map<String, Value>,
    schema: &Value,
    expected: &str,
    label: &str,
) -> Result<()> {
    let schema = resolve_schema(definitions, schema)?;
    let matches = match schema.get("type") {
        Some(Value::String(actual)) => actual == expected,
        Some(Value::Array(actual)) => actual.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    };
    anyhow::ensure!(matches, "{label} does not accept {expected}");
    Ok(())
}

fn require_array<'a>(
    definitions: &'a serde_json::Map<String, Value>,
    schema: &'a Value,
    label: &str,
) -> Result<&'a Value> {
    let schema = resolve_schema(definitions, schema)?;
    require_type(definitions, schema, "array", label)?;
    let items = schema
        .get("items")
        .with_context(|| format!("{label} has no item schema"))?;
    resolve_schema(definitions, items)
}

pub fn state_dir(catalog_root: &Path, identity: &str) -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    state_dir_in(&base, catalog_root, identity)
}

fn state_dir_in(base: &Path, catalog_root: &Path, identity: &str) -> PathBuf {
    base.join("st2")
        .join("codex")
        .join(runtime_key(catalog_root, identity))
}

fn socket_path(catalog_root: &Path, identity: &str) -> Result<PathBuf> {
    let key = runtime_key(catalog_root, identity);
    let preferred = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .map(|base| base.join("st2-codex").join(format!("{key}.sock")));
    if let Some(path) = preferred
        && path.as_os_str().as_bytes().len() <= SOCKET_PATH_BUDGET
    {
        return Ok(path);
    }
    let path = PathBuf::from("/tmp")
        .join(format!("st2-{}", unsafe { libc::geteuid() }))
        .join("codex")
        .join(format!("{key}.sock"));
    anyhow::ensure!(
        path.as_os_str().as_bytes().len() <= SOCKET_PATH_BUDGET,
        "Codex app-server socket path is too long: {}",
        path.display()
    );
    Ok(path)
}

fn runtime_key(catalog_root: &Path, identity: &str) -> String {
    let mut hash = Sha256::new();
    for value in [catalog_root.as_os_str().as_bytes(), identity.as_bytes()] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    let digest = format!("{:x}", hash.finalize());
    digest[..24].to_string()
}

fn secure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn acquire_owner_lock(state_dir: &Path) -> Result<File> {
    let path = state_dir.join("owner.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&path)
        .with_context(|| format!("opening Codex runtime owner lock {}", path.display()))?;
    // SAFETY: `file` owns this descriptor until the returned guard is dropped. `flock` does not
    // access Rust memory, and closing the descriptor releases the process-scoped lock after crash.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("Codex runtime already has an owner at {}", path.display()));
    }
    Ok(file)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state file has no parent")?;
    secure_dir(parent)?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        random_token()?
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

pub fn load_current_binding(
    path: &Path,
    runtime: &CodexRuntime,
) -> Result<Option<CodexThreadBinding>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let binding: CodexThreadBinding = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        binding.schema == BINDING_SCHEMA,
        "unsupported Codex binding schema"
    );
    anyhow::ensure!(
        binding.agent == runtime.agent
            && binding.runtime_id == runtime.runtime_id
            && binding.runtime_incarnation == runtime.incarnation,
        "Codex thread binding belongs to a different runtime incarnation"
    );
    Ok(Some(binding))
}

pub fn load_current_control_state(
    path: &Path,
    runtime: &CodexRuntime,
    binding: &CodexThreadBinding,
) -> Result<Option<CodexControlState>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let state: CodexControlState = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        state.schema == CONTROL_STATE_SCHEMA,
        "unsupported Codex control-state schema"
    );
    anyhow::ensure!(
        state.agent == runtime.agent
            && state.runtime_id == runtime.runtime_id
            && state.runtime_incarnation == runtime.incarnation
            && state.thread_id == binding.thread_id,
        "Codex control state belongs to a different runtime binding"
    );
    Ok(Some(state))
}

fn load_resume_thread(path: &Path, agent: &str, runtime_id: &str) -> Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let binding: CodexThreadBinding = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        binding.schema == BINDING_SCHEMA,
        "unsupported Codex binding schema"
    );
    anyhow::ensure!(
        binding.agent == agent && binding.runtime_id == runtime_id,
        "Codex resume binding belongs to a different agent runtime"
    );
    anyhow::ensure!(
        !binding.thread_id.is_empty(),
        "Codex resume binding has an empty thread id"
    );
    Ok(Some(binding.thread_id))
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_json_message(websocket: &mut WebSocket<UnixStream>, value: &Value) -> Result<()> {
    websocket.send(WebSocketMessage::Text(value.to_string().into()))?;
    Ok(())
}

/// One startup-phase read: polls the stop flag between short socket timeouts so a stop raised
/// during a slow handshake peer cannot sit out the full startup timeout (the caller sets a
/// short socket read timeout first).
enum StartupRead {
    Message(Value),
    Stopped,
    Closed,
}

fn read_startup_message(
    websocket: &mut WebSocket<UnixStream>,
    deadline: Instant,
) -> Result<StartupRead> {
    loop {
        if crate::provider_session::STOP.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(StartupRead::Stopped);
        }
        let message = match websocket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Ok(StartupRead::Closed);
            }
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "Codex app-server startup read timed out"
                );
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match message {
            WebSocketMessage::Text(text) => {
                let value = serde_json::from_str(&text)
                    .context("decoding Codex app-server WebSocket JSON")?;
                return Ok(StartupRead::Message(value));
            }
            WebSocketMessage::Close(_) => return Ok(StartupRead::Closed),
            WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_) => continue,
            WebSocketMessage::Binary(_) | WebSocketMessage::Frame(_) => {
                anyhow::bail!("Codex app-server sent a non-text WebSocket message")
            }
        }
    }
}

#[cfg(test)] // Production startup reads moved to the stop-aware read_startup_message.
fn read_json_message(websocket: &mut WebSocket<UnixStream>) -> Result<Option<Value>> {
    // Darwin reports a timed Unix-socket read as EAGAIN/EWOULDBLOCK.  During
    // handshake the peer may briefly be descheduled; treat that transient as
    // retryable instead of turning scheduler timing into a protocol failure.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let message = match websocket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Ok(None);
            }
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match message {
            WebSocketMessage::Text(text) => {
                let value = serde_json::from_str(&text)
                    .context("decoding Codex app-server WebSocket JSON")?;
                return Ok(Some(value));
            }
            WebSocketMessage::Close(_) => return Ok(None),
            WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_) => continue,
            WebSocketMessage::Binary(_) | WebSocketMessage::Frame(_) => {
                anyhow::bail!("Codex app-server sent a non-text WebSocket message")
            }
        }
    }
}

enum ControlRead {
    Message(Value),
    Timeout,
    Closed,
}

fn poll_json_message(websocket: &mut WebSocket<UnixStream>) -> Result<ControlRead> {
    loop {
        let message = match websocket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Ok(ControlRead::Closed);
            }
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(ControlRead::Timeout);
            }
            Err(error) => return Err(error.into()),
        };
        match message {
            WebSocketMessage::Text(text) => {
                let value = serde_json::from_str(&text)
                    .context("decoding Codex app-server WebSocket JSON")?;
                return Ok(ControlRead::Message(value));
            }
            WebSocketMessage::Close(_) => return Ok(ControlRead::Closed),
            WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_) => continue,
            WebSocketMessage::Binary(_) | WebSocketMessage::Frame(_) => {
                anyhow::bail!("Codex app-server sent a non-text WebSocket message")
            }
        }
    }
}

/// One app-server process group and the write end of its wrapper-liveness channel.
///
/// A watchdog in the dedicated process group owns the read end. The watchdog kills only that
/// group if this wrapper disappears without running Rust cleanup. Its membership also prevents
/// the operating system from reusing the group ID before cleanup.
struct OwnedProcessGroup {
    child: Child,
    watchdog: Child,
    owner_write: Option<UnixStream>,
    socket_path: Option<PathBuf>,
    active: bool,
}

impl OwnedProcessGroup {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    fn terminate(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let process_group = self.watchdog.id() as i32;
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = self.watchdog.kill();
        let _ = self.watchdog.wait();
        if let Some(socket_path) = self.socket_path.as_deref() {
            let _ = fs::remove_file(socket_path);
        }
        self.owner_write.take();
    }
}

impl Drop for OwnedProcessGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn set_close_on_exec(fd: libc::c_int) -> std::io::Result<()> {
    let mut flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    flags |= libc::FD_CLOEXEC;
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Spawn a provider launcher in an isolated, wrapper-owned process group.
///
/// Explicit cleanup covers normal returns and Rust errors. The in-group watchdog covers wrapper
/// crashes, SIGKILL, and supervisor teardown. The watchdog holds the group ID until cleanup, so a
/// stale PID can never identify a process group that belongs to another live owner. A crash can
/// leave one dead socket file; the next launch proves that it has no listener and removes it.
fn spawn_process_group(
    command: &mut Command,
    socket_path: Option<&Path>,
) -> std::io::Result<OwnedProcessGroup> {
    let (watchdog_read, owner_write) = UnixStream::pair()?;
    set_close_on_exec(owner_write.as_raw_fd())?;
    let owner_write_fd = owner_write.as_raw_fd();
    let mut watchdog_command = Command::new("/bin/sh");
    watchdog_command
        .arg("-c")
        .arg("IFS= read -r ignored; kill -KILL 0")
        .arg("st2-codex-watchdog")
        .stdin(Stdio::from(std::os::fd::OwnedFd::from(watchdog_read)))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        watchdog_command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut watchdog = watchdog_command.spawn()?;
    let watchdog_process_group = watchdog.id() as i32;
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, watchdog_process_group) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            libc::close(owner_write_fd);
            Ok(())
        });
    }
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            drop(owner_write);
            unsafe {
                libc::kill(-watchdog_process_group, libc::SIGKILL);
            }
            let _ = watchdog.kill();
            let _ = watchdog.wait();
            return Err(error);
        }
    };
    Ok(OwnedProcessGroup {
        child,
        watchdog,
        owner_write: Some(owner_write),
        socket_path: socket_path.map(Path::to_path_buf),
        active: true,
    })
}

fn terminate_child(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stop flag is process-global, so every test that exercises a reader of it —
    /// [`initialize_control`] above all — holds this lock against the one test that flips
    /// the flag: parallel readers would otherwise observe the raised flag and fail their
    /// `no stop raised in tests` expectations.
    fn stop_flag_tests() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
            std::sync::LazyLock::new(std::sync::Mutex::default);
        match LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn a_stop_during_the_websocket_handshake_ends_startup_gracefully() {
        let _stop_exclusive = stop_flag_tests();
        let tmp = tempfile::tempdir().unwrap();
        let socket_path = tmp.path().join("control.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let silent_server = std::thread::spawn(move || listener.accept().map(|(stream, _)| stream));
        let stream = UnixStream::connect(&socket_path).unwrap();
        let stopper = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(300));
            crate::provider_session::STOP.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let started = Instant::now();
        let result = initialize_control(stream);
        // Join before resetting: on an early failure return the stopper has not fired yet,
        // and resetting first would let it re-poison the global flag for every later test.
        stopper.join().unwrap();
        crate::provider_session::STOP.store(false, std::sync::atomic::Ordering::SeqCst);
        let _held_open = silent_server.join().unwrap().unwrap();
        assert!(
            result.unwrap().is_none(),
            "a stop while the server sits silent mid-handshake must return the graceful None"
        );
        assert!(
            started.elapsed() < STARTUP_TIMEOUT,
            "the stop must unblock the handshake well before the startup timeout"
        );
    }
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;

    #[cfg(target_os = "linux")]
    fn linux_process_state(pid: i32) -> Option<char> {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()?
            .rsplit_once(") ")?
            .1
            .chars()
            .next()
    }

    fn process_can_retain_cleanup_resources(pid: i32) -> bool {
        #[cfg(target_os = "linux")]
        if linux_process_state(pid) == Some('Z') {
            return false;
        }
        crate::host_lock::process_alive(pid)
    }

    fn object_schema(required: &[&str], properties: &[(&str, Value)]) -> Value {
        json!({
            "type": "object",
            "required": required,
            "properties": properties
                .iter()
                .map(|(name, schema)| ((*name).to_string(), schema.clone()))
                .collect::<serde_json::Map<String, Value>>()
        })
    }

    fn reference(name: &str) -> Value {
        json!({ "$ref": format!("#/definitions/{name}") })
    }

    fn array_of(items: Value) -> Value {
        json!({ "type": "array", "items": items })
    }

    fn tagged_variant(name: &str, required: &[&str], properties: &[(&str, Value)]) -> Value {
        let mut all_required = vec!["type"];
        all_required.extend(required);
        let mut all_properties = vec![("type", json!({ "type": "string", "enum": [name] }))];
        all_properties.extend(properties.iter().cloned());
        object_schema(&all_required, &all_properties)
    }

    fn method_schema(methods: &[&str]) -> Value {
        json!({
            "oneOf": methods
                .iter()
                .map(|method| object_schema(
                    &["method"],
                    &[("method", json!({ "type": "string", "enum": [method] }))],
                ))
                .collect::<Vec<_>>()
        })
    }

    fn compatible_protocol_schemas() -> CodexProtocolSchemas {
        let mut definitions = serde_json::Map::new();
        definitions.insert(
            "ThreadActiveFlag".into(),
            json!({
                "type": "string",
                "enum": ["waitingOnApproval", "waitingOnUserInput"]
            }),
        );
        definitions.insert(
            "ThreadStatus".into(),
            json!({
                "oneOf": [
                    tagged_variant("notLoaded", &[], &[]),
                    tagged_variant("idle", &[], &[]),
                    tagged_variant("systemError", &[], &[]),
                    tagged_variant(
                        "active",
                        &["activeFlags"],
                        &[("activeFlags", array_of(reference("ThreadActiveFlag")))],
                    )
                ]
            }),
        );
        definitions.insert(
            "ThreadItem".into(),
            json!({
                "oneOf": [
                    tagged_variant("contextCompaction", &[], &[]),
                    tagged_variant("enteredReviewMode", &[], &[]),
                    tagged_variant("exitedReviewMode", &[], &[]),
                    tagged_variant(
                        "userMessage",
                        &[],
                        &[("clientId", json!({ "type": ["string", "null"] }))],
                    )
                ]
            }),
        );
        definitions.insert("TextElement".into(), object_schema(&[], &[]));
        definitions.insert(
            "UserInput".into(),
            json!({
                "oneOf": [tagged_variant(
                    "text",
                    &["text"],
                    &[
                        ("text", json!({ "type": "string" })),
                        ("text_elements", array_of(reference("TextElement"))),
                    ],
                )]
            }),
        );
        definitions.insert(
            "ClientInfo".into(),
            object_schema(
                &["name", "version"],
                &[
                    ("name", json!({ "type": "string" })),
                    ("title", json!({ "type": ["string", "null"] })),
                    ("version", json!({ "type": "string" })),
                ],
            ),
        );
        definitions.insert(
            "InitializeCapabilities".into(),
            object_schema(&[], &[("experimentalApi", json!({ "type": "boolean" }))]),
        );
        definitions.insert(
            "InitializeParams".into(),
            object_schema(
                &["clientInfo"],
                &[
                    ("clientInfo", reference("ClientInfo")),
                    ("capabilities", reference("InitializeCapabilities")),
                ],
            ),
        );
        definitions.insert(
            "Thread".into(),
            object_schema(
                &["id", "status", "turns"],
                &[
                    ("id", json!({ "type": "string" })),
                    ("status", reference("ThreadStatus")),
                    ("turns", array_of(reference("Turn"))),
                ],
            ),
        );
        definitions.insert(
            "Turn".into(),
            object_schema(
                &["id", "items", "status"],
                &[
                    ("id", json!({ "type": "string" })),
                    ("items", array_of(reference("ThreadItem"))),
                    ("status", reference("TurnStatus")),
                    (
                        "error",
                        json!({ "anyOf": [reference("TurnError"), { "type": "null" }] }),
                    ),
                ],
            ),
        );
        definitions.insert(
            "TurnStatus".into(),
            json!({
                "type": "string",
                "enum": ["completed", "interrupted", "failed", "inProgress"]
            }),
        );
        definitions.insert(
            "TurnError".into(),
            object_schema(
                &["message"],
                &[
                    ("message", json!({ "type": "string" })),
                    (
                        "codexErrorInfo",
                        json!({ "anyOf": [reference("CodexErrorInfo"), { "type": "null" }] }),
                    ),
                ],
            ),
        );
        definitions.insert(
            "CodexErrorInfo".into(),
            json!({
                "oneOf": [
                    {
                        "type": "string",
                        "enum": [
                            "usageLimitExceeded",
                            "rateLimitExceeded",
                            "unauthorized",
                            "other"
                        ]
                    },
                    object_schema(
                        &["httpConnectionFailed"],
                        &[("httpConnectionFailed", object_schema(&[], &[]))],
                    )
                ]
            }),
        );
        for notification in ["TurnStartedNotification", "TurnCompletedNotification"] {
            definitions.insert(
                notification.into(),
                object_schema(
                    &["threadId", "turn"],
                    &[
                        ("threadId", json!({ "type": "string" })),
                        ("turn", reference("Turn")),
                    ],
                ),
            );
        }
        for notification in ["ItemStartedNotification", "ItemCompletedNotification"] {
            definitions.insert(
                notification.into(),
                object_schema(
                    &["threadId", "turnId", "item"],
                    &[
                        ("threadId", json!({ "type": "string" })),
                        ("turnId", json!({ "type": "string" })),
                        ("item", reference("ThreadItem")),
                    ],
                ),
            );
        }
        definitions.insert(
            "ThreadStartedNotification".into(),
            object_schema(&["thread"], &[("thread", reference("Thread"))]),
        );
        definitions.insert(
            "ThreadStatusChangedNotification".into(),
            object_schema(
                &["threadId", "status"],
                &[
                    ("threadId", json!({ "type": "string" })),
                    ("status", reference("ThreadStatus")),
                ],
            ),
        );
        definitions.insert(
            "ThreadResumeParams".into(),
            object_schema(&["threadId"], &[("threadId", json!({ "type": "string" }))]),
        );
        definitions.insert(
            "ThreadResumeResponse".into(),
            object_schema(&["thread"], &[("thread", reference("Thread"))]),
        );
        definitions.insert(
            "TurnStartParams".into(),
            object_schema(
                &["threadId", "input"],
                &[
                    ("threadId", json!({ "type": "string" })),
                    ("input", array_of(reference("UserInput"))),
                    ("clientUserMessageId", json!({ "type": ["string", "null"] })),
                ],
            ),
        );
        definitions.insert(
            "TurnSteerParams".into(),
            object_schema(
                &["threadId", "expectedTurnId", "input"],
                &[
                    ("threadId", json!({ "type": "string" })),
                    ("expectedTurnId", json!({ "type": "string" })),
                    ("input", array_of(reference("UserInput"))),
                    ("clientUserMessageId", json!({ "type": ["string", "null"] })),
                ],
            ),
        );
        definitions.insert(
            "TurnStartResponse".into(),
            object_schema(&["turn"], &[("turn", reference("Turn"))]),
        );
        definitions.insert(
            "TurnSteerResponse".into(),
            object_schema(&["turnId"], &[("turnId", json!({ "type": "string" }))]),
        );
        definitions.insert(
            "ThreadLoadedListResponse".into(),
            object_schema(
                &["data"],
                &[("data", array_of(json!({ "type": "string" })))],
            ),
        );
        definitions.insert(
            "HooksListParams".into(),
            object_schema(&[], &[("cwds", array_of(json!({ "type": "string" })))]),
        );
        definitions.insert(
            "HooksListResponse".into(),
            object_schema(
                &["data"],
                &[("data", array_of(reference("HooksListEntry")))],
            ),
        );
        definitions.insert(
            "HooksListEntry".into(),
            object_schema(
                &["hooks"],
                &[("hooks", array_of(reference("HookMetadata")))],
            ),
        );
        definitions.insert(
            "HookMetadata".into(),
            object_schema(
                &["currentHash", "isManaged", "key", "trustStatus"],
                &[
                    ("currentHash", json!({ "type": "string" })),
                    ("isManaged", json!({ "type": "boolean" })),
                    ("key", json!({ "type": "string" })),
                    ("trustStatus", reference("HookTrustStatus")),
                ],
            ),
        );
        definitions.insert(
            "HookTrustStatus".into(),
            json!({
                "type": "string",
                "enum": ["managed", "modified", "trusted", "untrusted"]
            }),
        );
        CodexProtocolSchemas {
            protocol: json!({ "definitions": definitions }),
            client_requests: method_schema(REQUIRED_CODEX_CLIENT_REQUESTS),
            client_notifications: method_schema(REQUIRED_CODEX_CLIENT_NOTIFICATIONS),
            server_requests: method_schema(&["currentTime/read"]),
            server_notifications: method_schema(REQUIRED_CODEX_SERVER_NOTIFICATIONS),
        }
    }

    fn write_fake_codex(
        root: &Path,
        name: &str,
        version: &str,
        schemas: &CodexProtocolSchemas,
    ) -> PathBuf {
        let fixture = root.join(format!("{name}-schemas"));
        fs::create_dir(&fixture).unwrap();
        for (filename, schema) in [
            (
                "codex_app_server_protocol.v2.schemas.json",
                &schemas.protocol,
            ),
            ("ClientRequest.json", &schemas.client_requests),
            ("ClientNotification.json", &schemas.client_notifications),
            ("ServerRequest.json", &schemas.server_requests),
            ("ServerNotification.json", &schemas.server_notifications),
        ] {
            fs::write(fixture.join(filename), serde_json::to_vec(schema).unwrap()).unwrap();
        }
        let path = root.join(name);
        fs::write(
            &path,
            format!(
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf '%s\\n' '{version}'; exit 0; fi\nout=\nwhile [ \"$#\" -gt 0 ]; do if [ \"$1\" = \"--out\" ]; then out=$2; break; fi; shift; done\n[ -n \"$out\" ] || exit 2\ncp '{fixture}/'*.json \"$out/\"\n",
                fixture = fixture.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn protocol_schema_gate_accepts_a_compatible_release_and_rejects_shape_drift() {
        let tmp = tempfile::tempdir().unwrap();
        let compatible = compatible_protocol_schemas();
        let patch = write_fake_codex(
            tmp.path(),
            "codex-compatible-patch",
            "codex-cli 0.150.0",
            &compatible,
        );
        ensure_supported_protocol(patch.to_str().unwrap()).unwrap();

        let mut incompatible = compatible_protocol_schemas();
        incompatible
            .protocol
            .pointer_mut("/definitions/ThreadActiveFlag/enum")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(Value::String("waitingOnFutureInput".into()));
        let incompatible = write_fake_codex(
            tmp.path(),
            "codex-incompatible-schema",
            "codex-cli 0.150.1",
            &incompatible,
        );
        let error = ensure_supported_protocol(incompatible.to_str().unwrap()).unwrap_err();
        assert!(format!("{error:#}").contains("ThreadActiveFlag changed"));
    }

    #[test]
    fn protocol_schema_gate_accepts_additive_items_and_server_requests() {
        let mut schemas = compatible_protocol_schemas();
        schemas
            .server_requests
            .get_mut("oneOf")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(
                method_schema(&["future/request"])
                    .get_mut("oneOf")
                    .unwrap()
                    .as_array_mut()
                    .unwrap()
                    .remove(0),
            );
        schemas
            .protocol
            .pointer_mut("/definitions/ThreadItem/oneOf")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(tagged_variant("futureItem", &[], &[]));

        verify_codex_protocol_schemas(&schemas).unwrap();
    }

    /// The classifier reads one word out of `Turn.error.codexErrorInfo` and depends on it being
    /// distinct from the quota words. A release that dropped or merged it must refuse the launch
    /// rather than let st2 report an exhausted allowance as a rejected credential.
    #[test]
    fn protocol_schema_gate_requires_the_distinct_credential_and_quota_error_words() {
        let mut schemas = compatible_protocol_schemas();
        let words = schemas
            .protocol
            .pointer_mut("/definitions/CodexErrorInfo/oneOf/0/enum")
            .unwrap()
            .as_array_mut()
            .unwrap();
        words.retain(|word| word.as_str() != Some("unauthorized"));
        let error = verify_codex_protocol_schemas(&schemas).unwrap_err();
        assert!(
            format!("{error:#}").contains("CodexErrorInfo has no 'unauthorized' word"),
            "{error:#}"
        );

        let mut merged = compatible_protocol_schemas();
        merged
            .protocol
            .pointer_mut("/definitions/CodexErrorInfo/oneOf/0/enum")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .retain(|word| word.as_str() != Some("rateLimitExceeded"));
        let error = verify_codex_protocol_schemas(&merged).unwrap_err();
        assert!(
            format!("{error:#}").contains("CodexErrorInfo has no 'rateLimitExceeded' word"),
            "{error:#}"
        );

        let mut untyped = compatible_protocol_schemas();
        untyped
            .protocol
            .pointer_mut("/definitions/Turn/properties")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("error");
        let error = verify_codex_protocol_schemas(&untyped).unwrap_err();
        assert!(
            format!("{error:#}").contains("Turn has no error property"),
            "{error:#}"
        );
    }

    #[test]
    fn protocol_rejection_reaches_the_declared_supervisor_once() {
        let tmp = tempfile::tempdir().unwrap();
        let worker = tmp.path().join("agents/h/worker/agent.kdl");
        let supervisor = tmp.path().join("agents/h/cos/agent.kdl");
        fs::create_dir_all(worker.parent().unwrap()).unwrap();
        fs::create_dir_all(supervisor.parent().unwrap()).unwrap();
        fs::write(
            &worker,
            r#"agent "worker" {
  host "h"
  supervisor "h.cos"
  command "true"
}
"#,
        )
        .unwrap();
        fs::write(
            &supervisor,
            r#"agent "cos" {
  host "h"
  command "true"
}
"#,
        )
        .unwrap();
        let mut incompatible = compatible_protocol_schemas();
        incompatible
            .protocol
            .pointer_mut("/definitions/ThreadActiveFlag/enum")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(Value::String("waitingOnFutureInput".into()));
        let codex = write_fake_codex(
            tmp.path(),
            "codex-rejected",
            "codex-cli 0.150.1",
            &incompatible,
        );
        let argv = vec![codex.display().to_string()];

        for _ in 0..2 {
            let error = run_controlled(
                tmp.path(),
                "h.worker".into(),
                "h.worker".into(),
                argv.clone(),
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("ThreadActiveFlag changed"));
        }

        let inbox = message::list_inbox(&message::inbox_dir(supervisor.parent().unwrap())).unwrap();
        assert_eq!(inbox.len(), 1, "the rejection report was not idempotent");
        assert_eq!(inbox[0].from.as_deref(), Some("h.worker"));
        assert_eq!(
            inbox[0].subject.as_deref(),
            Some("Codex protocol rejected: h.worker")
        );
        assert!(inbox[0].body.contains("Native delivery did not start"));
        assert!(inbox[0].body.contains("ThreadActiveFlag changed"));
    }

    #[test]
    fn unknown_thread_status_remains_a_hold_not_a_terminal_system_error() {
        let mut state = subscribed_state(CodexObservedState::Idle);
        state.observe_thread_status("futureStatus", None);
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::UnknownStatus,
                turn_id: None,
            }
        );
        state.observe_turn_completed("turn-future", CodexTurnOutcome::Indeterminate);
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::UnknownStatus,
                turn_id: None,
            }
        );
    }

    #[test]
    fn tui_loaded_deadline_precedes_the_outer_binding_deadline() {
        assert!(TUI_LOADED_TIMEOUT < STARTUP_TIMEOUT);
    }

    /// An agent directory with a parent to stage into, and a producer over it carrying a fixed
    /// incarnation so the record's provenance is assertable.
    fn context_producer(root: &Path) -> (PathBuf, CodexContextProducer) {
        let agent_dir = root.join("agents/h/worker");
        fs::create_dir_all(&agent_dir).unwrap();
        let writer =
            harness_context::Writer::new(&agent_dir, "h.worker", harness_context::Harness::Codex)
                .unwrap()
                .with_session("codex-incarnation");
        (agent_dir, CodexContextProducer::new(writer))
    }

    fn context_record(agent_dir: &Path) -> Option<harness_context::Observed> {
        harness_context::read(&harness_context::harness_context_path(agent_dir))
    }

    fn token_usage_frame(last_total: i64, window: Value) -> Value {
        json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-main",
                "turnId": "turn-1",
                "tokenUsage": {
                    "last": { "totalTokens": last_total },
                    "total": { "totalTokens": last_total },
                    "modelContextWindow": window
                }
            }
        })
    }

    fn compaction_item_frame(method: &str, turn_id: &str, item_id: &str) -> Value {
        json!({
            "method": method,
            "params": {
                "threadId": "thread-main",
                "turnId": turn_id,
                "item": { "id": item_id, "type": "contextCompaction" }
            }
        })
    }

    /// HC-R13's Codex fixture. The frames are a transposition, and the comment says which half came
    /// from where: the SHAPE is codex-cli 0.151.0's own app-server schema dump
    /// (`ThreadTokenUsageUpdatedNotification`, `AccountRateLimitsUpdatedNotification`), while the
    /// NUMBERS are verbatim from a real rollout captured on 2026-08-29 from a 0.150.1 session
    /// (`session_meta.payload.cli_version = "0.150.1"`) — its first and last `token_count` events
    /// and the `rate_limits` snapshot riding them. Fields the capture elided are omitted rather
    /// than invented; this producer reads three numbers and must not need the rest.
    ///
    /// What must fail here when a codex bump moves something: the 12,000 baseline (the percent
    /// changes), the numerator (`total` reads 100 and `last.inputTokens` without the baseline reads
    /// 36 against this very capture, both asserted below), and the version literal itself, which is
    /// the only thing tying this arithmetic to a build whose source was actually read.
    #[test]
    fn codex_context_recomputes_the_captured_reading_and_pins_its_verified_version() {
        assert_eq!(CODEX_CONTEXT_VERIFIED_VERSION, "0.151.0");
        assert_eq!(CODEX_BASELINE_TOKENS, 12_000);

        let frames = include_str!("../tests/fixtures/codex_token_usage_inbound.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 3);

        let tmp = tempfile::tempdir().unwrap();
        let (agent_dir, mut producer) = context_producer(tmp.path());

        // The session's FIRST reading: 32,237 of 258,400 with the baseline normalized out is 8%,
        // and no rate-limit notification has arrived yet, so both windows are honestly absent.
        assert!(producer.observe(&frames[0], "thread-main").unwrap());
        let first = context_record(&agent_dir).unwrap();
        assert_eq!(first.used_tokens, Some(32_237));
        assert_eq!(first.window_tokens, Some(258_400));
        assert_eq!(first.used_percent, Some(8.0));
        assert_eq!(first.rate_limits, harness_context::RateLimits::default());

        // The account-scoped snapshot carries no occupancy, so it writes nothing on its own and is
        // held for the next reading (HC-T06).
        assert!(!producer.observe(&frames[1], "thread-main").unwrap());
        let mut unchanged = context_record(&agent_dir).unwrap();
        // `age_ms` is derived at read time, not stored, so it moves between two reads of one
        // record. Everything the record itself carries — including `observed_at_ms`, which is what
        // proves no write happened — must be identical.
        assert!(unchanged.age_ms >= first.age_ms);
        unchanged.age_ms = first.age_ms;
        assert_eq!(unchanged, first);

        assert!(producer.observe(&frames[2], "thread-main").unwrap());
        let observed = context_record(&agent_dir).unwrap();
        assert_eq!(observed.harness, harness_context::Harness::Codex);
        assert_eq!(observed.used_tokens, Some(92_283));
        assert_eq!(observed.window_tokens, Some(258_400));
        // 100 − Codex's displayed "67% context left" for this exact capture.
        assert_eq!(observed.used_percent, Some(33.0));
        assert_eq!(observed.session_total_tokens, Some(2_235_329));
        // The channel carries neither: `Thread` has `modelProvider` and no model identifier, and
        // Codex reports no session cost anywhere in the protocol.
        assert_eq!(observed.model, None);
        assert_eq!(observed.cost_usd, None);
        // Only the seven-day window was ever captured on this harness; the five-hour leg is not
        // inferred from a field name (see `observe_rate_limits`).
        assert_eq!(
            observed.rate_limits,
            harness_context::RateLimits {
                five_hour: None,
                seven_day: Some(44.0),
            }
        );
        assert_eq!(observed.compactions, 0);
        assert_eq!(observed.last_compaction_ms, None);

        // The trap, asserted rather than described: the cumulative session total is 2,235,329
        // against a 258,400-token window. A producer that used it as the numerator would publish a
        // saturated 100 for a window that is a third full.
        assert_eq!(codex_used_percent(Some(258_400), 2_235_329), Some(100.0));
        assert_ne!(
            codex_used_percent(Some(258_400), 2_235_329),
            observed.used_percent
        );
        // And the baseline-free percent over the same operands is 36 — close enough to look right.
        let baseline_free = (92_283.0_f64 / 258_400.0 * 100.0).round();
        assert_eq!(baseline_free, 36.0);
        assert_ne!(Some(baseline_free), observed.used_percent);

        // Mirroring is not the same function as rounding the used percentage: at an exact half
        // they disagree. Effective window 200, used 101 — Codex displays 50% left, so st2 publishes
        // 50; rounding `used/effective` would publish 51.
        assert_eq!(codex_used_percent(Some(12_200), 12_101), Some(50.0));
        assert_eq!((101.0_f64 / 200.0 * 100.0).round(), 51.0);
    }

    /// HC-R02/HC-R03: the operands are the harness's and are published as they arrive; only the
    /// percent is withheld, and only where Codex's own normalization cannot run. A window at or
    /// below the baseline is the sharp case — Codex itself returns "0% remaining" there, which
    /// mirrored blindly would publish a fabricated 100% used.
    #[test]
    fn a_missing_or_unnormalizable_window_withholds_the_percent_but_not_the_operands() {
        for (window, expected_window) in [
            (Value::Null, None),
            (json!(12_000), Some(12_000)),
            (json!(0), None),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let (agent_dir, mut producer) = context_producer(tmp.path());
            assert!(
                producer
                    .observe(&token_usage_frame(92_283, window.clone()), "thread-main")
                    .unwrap()
            );
            let observed = context_record(&agent_dir).unwrap();
            assert_eq!(observed.used_tokens, Some(92_283), "window {window}");
            assert_eq!(observed.window_tokens, expected_window, "window {window}");
            assert_eq!(observed.used_percent, None, "window {window}");
        }

        // A window key that is absent rather than null reads the same way.
        let tmp = tempfile::tempdir().unwrap();
        let (agent_dir, mut producer) = context_producer(tmp.path());
        assert!(
            producer
                .observe(
                    &json!({
                        "method": "thread/tokenUsage/updated",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-1",
                            "tokenUsage": {
                                "last": { "totalTokens": 92_283 },
                                "total": { "totalTokens": 92_283 }
                            }
                        }
                    }),
                    "thread-main",
                )
                .unwrap()
        );
        let observed = context_record(&agent_dir).unwrap();
        assert_eq!(observed.window_tokens, None);
        assert_eq!(observed.used_percent, None);
    }

    /// Codex speaks once per model response — roughly 10-15 times a turn, and again on resume or
    /// re-attach. The core's quantization is the ONLY thing deciding what lands (HC-R09): this
    /// producer holds no reading of its own and imposes no cadence. The shape that catches a second
    /// guard is the last frame here — a bucket crossing arriving immediately after two suppressed
    /// readings, which any time floor in the producer would swallow.
    #[test]
    fn every_reading_reaches_the_core_guard_and_the_producer_imposes_no_cadence_of_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent_dir, mut producer) = context_producer(tmp.path());
        let window = json!(258_400);

        assert!(
            producer
                .observe(&token_usage_frame(92_283, window.clone()), "thread-main")
                .unwrap()
        );
        assert_eq!(context_record(&agent_dir).unwrap().used_percent, Some(33.0));

        // Both still round to 33% used, so both sit in the written bucket and neither lands.
        for moved in [93_000, 94_000] {
            assert_eq!(codex_used_percent(Some(258_400), moved), Some(33.0));
            assert!(
                !producer
                    .observe(&token_usage_frame(moved, window.clone()), "thread-main")
                    .unwrap()
            );
            assert_eq!(
                context_record(&agent_dir).unwrap().used_tokens,
                Some(92_283)
            );
        }

        // The crossing lands at once, with no elapsed time behind it.
        assert!(
            producer
                .observe(&token_usage_frame(95_000, window.clone()), "thread-main")
                .unwrap()
        );
        let observed = context_record(&agent_dir).unwrap();
        assert_eq!(observed.used_percent, Some(34.0));
        assert_eq!(observed.used_tokens, Some(95_000));

        // A reading for another thread is not this seat's.
        assert!(
            !producer
                .observe(&token_usage_frame(200_000, window), "thread-other")
                .unwrap()
        );
        assert_eq!(
            context_record(&agent_dir).unwrap().used_tokens,
            Some(95_000)
        );
    }

    /// HC-R12: one compaction is one count, however many of its spellings arrive. Codex publishes
    /// the live edge as an `item/started` AND an `item/completed` over the same
    /// `ContextCompactionThreadItem` id, and the protocol still carries a deprecated
    /// `thread/compacted` notification for the same event that names only the turn.
    #[test]
    fn one_compaction_is_counted_once_across_every_spelling_of_its_edge() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent_dir, mut producer) = context_producer(tmp.path());

        assert!(
            producer
                .observe(
                    &compaction_item_frame("item/started", "turn-1", "item-a"),
                    "thread-main"
                )
                .unwrap()
        );
        let first = context_record(&agent_dir).unwrap();
        assert_eq!(first.compactions, 1);
        assert_eq!(
            first.last_compaction_trigger,
            Some(harness_context::CompactionTrigger::Unknown),
            "the item carries an id and a type and no reason at all"
        );
        assert!(first.last_compaction_ms.is_some());

        // The same compaction's closing edge, and the deprecated notification for the same event.
        assert!(
            !producer
                .observe(
                    &compaction_item_frame("item/completed", "turn-1", "item-a"),
                    "thread-main"
                )
                .unwrap()
        );
        assert!(
            !producer
                .observe(
                    &json!({
                        "method": "thread/compacted",
                        "params": { "threadId": "thread-main", "turnId": "turn-1" }
                    }),
                    "thread-main",
                )
                .unwrap()
        );
        assert_eq!(context_record(&agent_dir).unwrap().compactions, 1);

        // A genuinely second compaction inside the same turn is a second count.
        assert!(
            producer
                .observe(
                    &compaction_item_frame("item/started", "turn-1", "item-b"),
                    "thread-main"
                )
                .unwrap()
        );
        assert_eq!(context_record(&agent_dir).unwrap().compactions, 2);

        // Interleaved lifecycles: two starts before either completion still count exactly two, so
        // the dedupe cannot be a single last-key memory.
        for (method, item) in [
            ("item/started", "item-c"),
            ("item/started", "item-d"),
            ("item/completed", "item-c"),
            ("item/completed", "item-d"),
        ] {
            producer
                .observe(
                    &compaction_item_frame(method, "turn-2", item),
                    "thread-main",
                )
                .unwrap();
        }
        assert_eq!(context_record(&agent_dir).unwrap().compactions, 4);

        // The deprecated notification arriving FIRST also claims the compaction, so the item that
        // follows it does not count a second time.
        assert!(
            producer
                .observe(
                    &json!({
                        "method": "thread/compacted",
                        "params": { "threadId": "thread-main", "turnId": "turn-3" }
                    }),
                    "thread-main",
                )
                .unwrap()
        );
        assert!(
            !producer
                .observe(
                    &compaction_item_frame("item/started", "turn-3", "item-e"),
                    "thread-main"
                )
                .unwrap()
        );
        assert_eq!(context_record(&agent_dir).unwrap().compactions, 5);

        // Another thread's compaction is not this seat's, and a non-compaction item is not an edge.
        assert!(
            !producer
                .observe(
                    &compaction_item_frame("item/started", "turn-9", "item-z"),
                    "thread-other"
                )
                .unwrap()
        );
        assert!(
            !producer
                .observe(
                    &json!({
                        "method": "item/started",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-4",
                            "item": { "id": "item-y", "type": "agentMessage" }
                        }
                    }),
                    "thread-main",
                )
                .unwrap()
        );
        assert_eq!(context_record(&agent_dir).unwrap().compactions, 5);
    }

    /// The producer runs beside a live delivery loop and sees every frame that loop sees. Replaying
    /// the captured #263 session — 23 real inbound frames, none of them a token count — must leave
    /// no record at all: absence here is "never observed", and a producer that manufactured a
    /// reading from a turn boundary would break exactly the HC-R03 rule the record exists for.
    #[test]
    fn captured_delivery_frames_carrying_no_token_count_publish_no_record() {
        let frames = include_str!("../tests/fixtures/codex_usage_limit_inbound.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 23);

        let tmp = tempfile::tempdir().unwrap();
        let (agent_dir, mut producer) = context_producer(tmp.path());
        for frame in &frames {
            assert!(
                !producer.observe(frame, "thread-main").unwrap(),
                "no captured delivery frame carries a context reading: {frame}"
            );
        }
        assert!(context_record(&agent_dir).is_none());
    }

    fn delivery_config(root: &Path) -> CodexDeliveryConfig {
        let agent_dir = root.join("agents/h/worker");
        CodexDeliveryConfig {
            catalog_root: root.to_path_buf(),
            inbox: message::inbox_dir(&agent_dir),
            agent_dir,
            identity: "h.worker".into(),
            this_host: "h".into(),
            supervisor: None,
            producer_version: Some("codex-cli 0.153.0".into()),
        }
    }

    fn subscribed_state(observed: CodexObservedState) -> CodexControlState {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state.subscribed = true;
        state.observed = observed;
        state
    }

    fn inbox_delivery(root: &Path, config: CodexDeliveryConfig) -> CodexInboxDelivery {
        CodexInboxDelivery::new(
            config,
            root.join("state").join(delivery_ledger::LEGACY_FILE),
            CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap(),
        )
        .unwrap()
    }

    /// Read the ledger back through its own loader and the real correlation derivation: a test
    /// that read the bytes directly would not notice a record the pump itself would refuse.
    fn ledger_entry(root: &Path, filename: &str) -> Option<delivery_ledger::Entry> {
        delivery_ledger::Ledger::open(
            &root.join("state").join(delivery_ledger::LEGACY_FILE),
            delivery_ledger::Harness::Codex.profile(),
            "h.worker",
            "h.worker",
            |thread, file| stable_client_user_message_id("h.worker", thread, file),
        )
        .entry(filename)
        .cloned()
    }

    fn acknowledge_tui_thread_loaded(events: &Receiver<ControlEvent>) {
        let ControlEvent::TuiThreadLoaded(acknowledge) =
            events.recv_timeout(Duration::from_secs(10)).unwrap()
        else {
            panic!("control did not report the TUI-loaded gate");
        };
        acknowledge.send(()).unwrap();
    }

    #[test]
    fn delivery_request_uses_typed_start_and_exact_turn_steer() {
        let start = codex_delivery_request(
            2,
            "thread-main",
            "st2:client",
            "notice",
            &CodexDeliveryMethod::Start,
        );
        assert_eq!(start["method"], "turn/start");
        assert_eq!(start["params"]["threadId"], "thread-main");
        assert_eq!(start["params"]["clientUserMessageId"], "st2:client");
        assert_eq!(start["params"]["input"][0]["type"], "text");
        assert_eq!(start["params"]["input"][0]["text"], "notice");
        assert!(start["params"].get("expectedTurnId").is_none());

        let steer = codex_delivery_request(
            3,
            "thread-main",
            "st2:client",
            "notice",
            &CodexDeliveryMethod::Steer {
                turn_id: "turn-current".into(),
            },
        );
        assert_eq!(steer["method"], "turn/steer");
        assert_eq!(steer["params"]["expectedTurnId"], "turn-current");
        assert!(steer["params"].get("model").is_none());
        assert!(steer["params"].get("approvalPolicy").is_none());
    }

    /// Behavioral oracle for the #268 §B projection: a projection that withheld every row — or
    /// that reported the two misclassified rows as indeterminate — fails here, because each
    /// emitting row is asserted positively.
    #[test]
    fn harness_projection_is_faithful_and_withholds_only_unprovable_rows() {
        use crate::harness_state::{Activity, Ask, BlockedOn, InputBuffer};
        let held = |reason| CodexObservedState::Held {
            reason,
            turn_id: None,
        };

        // Rows with no provable observation are withheld — and no absence may derive idle.
        for state in [
            CodexObservedState::AwaitingStatus,
            held(CodexHoldReason::NotLoaded),
            held(CodexHoldReason::SystemError),
        ] {
            assert_eq!(state.harness_observation(), None, "{state:?}");
        }

        // Codex positively reported work: active, even where st2 cannot name a steerable turn
        // (the two rows a naive steerability decomposition reported as unknown) or where the
        // delivery gate holds.
        for state in [
            CodexObservedState::Active {
                turn_id: "turn-current".into(),
            },
            held(CodexHoldReason::ActiveWithoutTurn),
            held(CodexHoldReason::ConflictingTurn),
            held(CodexHoldReason::Compaction),
            // Review's edges are model-emitted items inside a running turn: plain activity,
            // no human, no ask — the delivery hold is a separate axis.
            held(CodexHoldReason::Review),
        ] {
            let observation = state
                .harness_observation()
                .unwrap_or_else(|| panic!("{state:?} must emit"));
            assert_eq!(observation.state, Activity::Active, "{state:?}");
            assert_eq!(observation.blocked_on, BlockedOn::None, "{state:?}");
            assert_eq!(observation.input_buffer, InputBuffer::Unknown, "{state:?}");
        }

        // The holds a human resolves set the blocked axis instead of disappearing into active,
        // and each names its machine-readable ask kind so consumers never branch on `reason`.
        for (reason, ask) in [
            (CodexHoldReason::WaitingOnApproval, Ask::Permission),
            (CodexHoldReason::WaitingOnUserInput, Ask::Question),
        ] {
            let observation = held(reason)
                .harness_observation()
                .unwrap_or_else(|| panic!("{reason:?} must emit"));
            assert_eq!(observation.state, Activity::Active, "{reason:?}");
            assert_eq!(observation.blocked_on, BlockedOn::Human, "{reason:?}");
            assert_eq!(observation.ask, ask, "{reason:?}");
        }

        let idle = CodexObservedState::Idle.harness_observation().unwrap();
        assert_eq!(idle.state, Activity::Idle);
        assert_eq!(idle.blocked_on, BlockedOn::None);

        let ended = CodexObservedState::TerminalError {
            reason: CodexTerminalError::SystemError,
        }
        .harness_observation()
        .unwrap();
        assert_eq!(ended.state, Activity::Ended);
        assert_eq!(ended.reason.as_deref(), Some("systemError"));
    }

    #[test]
    #[cfg(unix)]
    fn a_failed_transition_write_is_retried_before_any_heartbeat() {
        use crate::harness_state::{self, Activity};
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let agent_dir = config.agent_dir.clone();
        let record_path = harness_state::harness_state_path(&agent_dir);
        let mut delivery = inbox_delivery(tmp.path(), config);

        delivery.observe_harness(&CodexObservedState::Active {
            turn_id: "turn-current".into(),
        });
        assert_eq!(
            harness_state::read(&record_path, None).unwrap().state,
            Activity::Active
        );

        // The transition to idle fails to land: the agent dir is briefly unwritable.
        let live = fs::metadata(&agent_dir).unwrap().permissions();
        fs::set_permissions(&agent_dir, fs::Permissions::from_mode(0o555)).unwrap();
        delivery.observe_harness(&CodexObservedState::Idle);
        fs::set_permissions(&agent_dir, live).unwrap();
        assert_eq!(
            harness_state::read(&record_path, None).unwrap().state,
            Activity::Active,
            "the failed write cannot have landed"
        );

        // No heartbeat may re-stamp the contradicted on-disk state; the retry lands the pending
        // transition on the NEXT pump pass — deliberately without advancing the presence
        // cadence, which gates only heartbeats.
        let stale_active = fs::read(&record_path).unwrap();
        delivery.next_presence_refresh = Instant::now() + status::STATUS_REFRESH;
        delivery.refresh_if_due().unwrap();
        let after = harness_state::read(&record_path, None).unwrap();
        assert_eq!(after.state, Activity::Idle, "pending transition retried");
        assert_ne!(fs::read(&record_path).unwrap(), stale_active);
    }

    #[test]
    fn pump_publishes_observations_and_stops_heartbeating_on_evidence_loss() {
        use crate::harness_state::{self, Activity};
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let agent_dir = config.agent_dir.clone();
        let record_path = harness_state::harness_state_path(&agent_dir);
        let mut delivery = inbox_delivery(tmp.path(), config);

        delivery.observe_harness(&CodexObservedState::Active {
            turn_id: "turn-current".into(),
        });
        let observed = harness_state::read(&record_path, None).expect("record written");
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(observed.harness.as_deref(), Some("codex"));

        // An indeterminate projection writes nothing and stops the heartbeat: the presence
        // refresh still runs, but the record's bytes stay untouched and age toward unknown.
        delivery.observe_harness(&CodexObservedState::Held {
            reason: CodexHoldReason::NotLoaded,
            turn_id: None,
        });
        let before = fs::read(&record_path).unwrap();
        delivery.refresh_if_due().unwrap();
        assert!(
            status::read_state(&status::status_path(&agent_dir)) != status::State::Offline,
            "presence refresh must still run"
        );
        assert_eq!(
            fs::read(&record_path).unwrap(),
            before,
            "no heartbeat without evidence"
        );

        // Evidence returning resumes both observation and heartbeat.
        delivery.observe_harness(&CodexObservedState::Idle);
        assert_eq!(
            harness_state::read(&record_path, None).unwrap().state,
            Activity::Idle
        );
        delivery.next_presence_refresh = Instant::now();
        delivery.refresh_if_due().unwrap();
        assert_ne!(
            fs::read(&record_path).unwrap(),
            before,
            "heartbeat resumes with evidence"
        );
    }

    #[test]
    fn evidence_loss_marks_the_stream_discontinuous_for_a_restated_state() {
        use crate::harness_state::{self, Activity};
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let record_path = harness_state::harness_state_path(&config.agent_dir);
        let mut delivery = inbox_delivery(tmp.path(), config);

        delivery.observe_harness(&CodexObservedState::Active {
            turn_id: "turn-a".into(),
        });
        let before = fs::read(&record_path).unwrap();

        // The same tuple restated across an unproven interval must not coalesce into the
        // pre-gap record — continuity was not observed, so a fresh transition opens.
        delivery.observe_harness(&CodexObservedState::Held {
            reason: CodexHoldReason::SystemError,
            turn_id: None,
        });
        delivery.observe_harness(&CodexObservedState::Active {
            turn_id: "turn-a".into(),
        });
        assert_ne!(
            fs::read(&record_path).unwrap(),
            before,
            "a restated state after an evidence gap must open a fresh transition"
        );
        assert_eq!(
            harness_state::read(&record_path, None).unwrap().state,
            Activity::Active
        );
    }

    #[test]
    fn delivery_client_id_is_stable_and_binds_every_identity_component() {
        let id =
            stable_client_user_message_id("h.worker", "thread-main", "1786380000000-abc123.md");
        assert_eq!(
            id,
            stable_client_user_message_id("h.worker", "thread-main", "1786380000000-abc123.md")
        );
        assert!(id.starts_with("st2:"));
        assert_ne!(
            id,
            stable_client_user_message_id("h.other", "thread-main", "1786380000000-abc123.md")
        );
        assert_ne!(
            id,
            stable_client_user_message_id("h.worker", "thread-other", "1786380000000-abc123.md")
        );
        assert_ne!(
            id,
            stable_client_user_message_id("h.worker", "thread-main", "1786380000000-def456.md")
        );
    }

    #[test]
    fn review_compaction_and_dnd_hold_the_unread_fifo_head() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("held"), None, &[], "body")
                .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        for reason in [CodexHoldReason::Review, CodexHoldReason::Compaction] {
            let state = subscribed_state(CodexObservedState::Held {
                reason,
                turn_id: Some("turn-current".into()),
            });
            assert_eq!(delivery.maybe_request(&state).unwrap(), None);
            assert!(config.inbox.join(&filename).is_file());
        }

        status::set_state(&status::status_path(&config.agent_dir), status::State::Dnd).unwrap();
        delivery.next_inbox_refresh = Instant::now();
        assert_eq!(
            delivery
                .maybe_request(&subscribed_state(CodexObservedState::Idle))
                .unwrap(),
            None
        );
        assert_eq!(message::list_inbox(&config.inbox).unwrap().len(), 1);
    }

    #[test]
    fn failed_turn_without_idle_allows_next_native_delivery_and_preserves_system_error() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("after error"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config);
        let mut state = subscribed_state(CodexObservedState::Idle);

        state
            .observe(&json!({
                "method": "turn/started",
                "params": {
                    "threadId": "thread-main",
                    "turn": { "id": "turn-failed" }
                }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "thread/status/changed",
                "params": {
                    "threadId": "thread-main",
                    "status": { "type": "systemError" }
                }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-main",
                    "turn": { "id": "turn-failed", "status": "failed" }
                }
            }))
            .unwrap();

        assert_eq!(
            state.observed(),
            &CodexObservedState::TerminalError {
                reason: CodexTerminalError::SystemError,
            }
        );

        let request = delivery
            .maybe_request(&state)
            .unwrap()
            .expect("a terminal system error must not block the next native delivery");
        assert_eq!(request["method"], "turn/start");
    }

    #[test]
    fn captured_usage_limit_boundary_allows_next_native_delivery() {
        // This fixture is a payload-minimized projection of all 23 inbound frames from the
        // #263 trivial capture. It preserves their order and methods while removing fields this
        // observer never reads. The second capture has the same method sequence. The recorder
        // stops at turn completion, so this test pins the boundary state only. The provider
        // source establishes that no later idle notification follows the system error.
        let frames = include_str!("../tests/fixtures/codex_usage_limit_inbound.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 23);

        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("after capture"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config);
        let mut state = subscribed_state(CodexObservedState::AwaitingStatus);

        for frame in &frames {
            state.observe(frame).unwrap();
        }

        assert_eq!(
            frames
                .last()
                .and_then(|frame| frame.get("method"))
                .and_then(Value::as_str),
            Some("turn/completed")
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::TerminalError {
                reason: CodexTerminalError::SystemError,
            }
        );
        let request = delivery
            .maybe_request(&state)
            .unwrap()
            .expect("a captured terminal system error must permit the next native delivery");
        assert_eq!(request["method"], "turn/start");
    }

    /// The credential class and the quota class arrive through the SAME frame sequence, differing
    /// only in one word of `Turn.error.codexErrorInfo`. This replays the auth-rejected shape and
    /// asserts the fork: `providerAuth` on the observed record, a native-driver diagnostic, and
    /// delivery still permitted — while the captured usage-limit fixture beside it keeps reading
    /// `systemError` with no diagnostic at all.
    #[test]
    fn a_rejected_codex_credential_reads_provider_auth_while_a_quota_failure_does_not() {
        let rejected = include_str!("../tests/fixtures/codex_provider_auth_inbound.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let quota = include_str!("../tests/fixtures/codex_usage_limit_inbound.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();

        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let agent_dir = config.agent_dir.clone();
        message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("after rejection"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config);
        let mut state = subscribed_state(CodexObservedState::AwaitingStatus);
        for frame in &rejected {
            state.observe(frame).unwrap();
            delivery.observe_provider_auth(frame, "thread-main");
        }

        assert_eq!(
            state.observed(),
            &CodexObservedState::TerminalError {
                reason: CodexTerminalError::ProviderAuthRejected,
            }
        );
        let observation = state.observed().harness_observation().unwrap();
        assert_eq!(observation.state, harness_state::Activity::Ended);
        assert_eq!(
            observation.reason.as_deref(),
            Some("providerAuth"),
            "the same word OpenCode's ProviderAuthError already publishes"
        );
        let record = driver_diagnostic::path(&agent_dir);
        let driver_diagnostic::Observed::Failure(failure) = driver_diagnostic::read(&record) else {
            panic!("a rejected credential must publish a native-driver diagnostic")
        };
        assert_eq!(failure.driver, driver_diagnostic::Driver::Codex);
        assert_eq!(failure.stage, driver_diagnostic::Stage::ProviderAuth);
        assert_eq!(
            failure.reason,
            driver_diagnostic::Reason::ProviderAuthRejected
        );
        assert_eq!(failure.source, driver_diagnostic::Source::TurnResult);
        assert_eq!(
            failure.producer_version.as_deref(),
            Some("codex-cli 0.153.0")
        );
        assert_eq!(failure.support, driver_diagnostic::Support::Supported);
        let request = delivery
            .maybe_request(&state)
            .unwrap()
            .expect("a rejected credential must not block the next native delivery");
        assert_eq!(request["method"], "turn/start");

        // A turn that reaches its ordinary end is the recovery edge.
        delivery.observe_provider_auth(
            &json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-main",
                    "turn": { "id": "turn-ok", "status": "completed" }
                }
            }),
            "thread-main",
        );
        assert_eq!(
            driver_diagnostic::read(&record),
            driver_diagnostic::Observed::Absent
        );

        // The quota capture walks the same methods and must stay unclassified.
        let quota_tmp = tempfile::tempdir().unwrap();
        let quota_config = delivery_config(quota_tmp.path());
        let quota_agent_dir = quota_config.agent_dir.clone();
        let mut quota_delivery = inbox_delivery(quota_tmp.path(), quota_config);
        let mut quota_state = subscribed_state(CodexObservedState::AwaitingStatus);
        for frame in &quota {
            quota_state.observe(frame).unwrap();
            quota_delivery.observe_provider_auth(frame, "thread-main");
        }
        assert_eq!(
            quota_state.observed(),
            &CodexObservedState::TerminalError {
                reason: CodexTerminalError::SystemError,
            }
        );
        assert_eq!(
            driver_diagnostic::read(&driver_diagnostic::path(&quota_agent_dir)),
            driver_diagnostic::Observed::Absent,
            "an exhausted allowance is not a rejected credential"
        );
    }

    #[test]
    fn idle_session_refreshes_stale_presence_without_inbox_activity() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let presence = status::status_path(&config.agent_dir);
        std::fs::create_dir_all(&config.agent_dir).unwrap();
        std::fs::write(&presence, "available\n").unwrap();
        std::fs::File::open(&presence)
            .unwrap()
            .set_modified(SystemTime::now() - status::STATUS_STALE - Duration::from_secs(1))
            .unwrap();
        assert_eq!(status::read_state(&presence), status::State::Unknown);

        let mut delivery = inbox_delivery(tmp.path(), config);
        delivery.refresh_if_due().unwrap();

        assert_eq!(status::read_state(&presence), status::State::Available);
        assert!(
            std::fs::read_to_string(&presence)
                .unwrap()
                .contains("\nv1 ")
        );
        assert!(delivery.head.is_none());
    }

    #[test]
    fn inbox_fallback_does_not_write_a_fifteen_second_presence_heartbeat() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let presence = status::status_path(&config.agent_dir);
        status::set_state(&presence, status::State::Available).unwrap();
        let before = std::fs::read_to_string(&presence).unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config);
        delivery.next_inbox_refresh = Instant::now();
        delivery.next_presence_refresh = Instant::now() + status::STATUS_REFRESH;

        delivery.refresh_if_due().unwrap();

        assert_eq!(std::fs::read_to_string(&presence).unwrap(), before);
    }

    #[test]
    fn a_rejected_exact_steer_has_no_fallback_and_remains_retryable_after_state_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("retry"), None, &[], "body")
                .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        let active = subscribed_state(CodexObservedState::Active {
            turn_id: "turn-current".into(),
        });
        let steer = delivery.maybe_request(&active).unwrap().unwrap();
        assert_eq!(steer["method"], "turn/steer");
        assert_eq!(steer["params"]["expectedTurnId"], "turn-current");
        let request_id = steer["id"].clone();
        let client_id = steer["params"]["clientUserMessageId"].clone();

        assert!(
            !delivery
                .accept_response(
                    &json!({
                        "id": request_id,
                        "method": "item/commandExecution/requestApproval",
                        "params": {}
                    }),
                    active.observed(),
                )
                .unwrap()
        );
        assert!(delivery
            .accept_response(
                &json!({ "id": request_id, "error": { "code": -32600, "message": "stale turn" } }),
                active.observed(),
            )
            .unwrap());
        assert_eq!(delivery.maybe_request(&active).unwrap(), None);
        assert!(config.inbox.join(&filename).is_file());

        let retry = delivery
            .maybe_request(&subscribed_state(CodexObservedState::Idle))
            .unwrap()
            .unwrap();
        assert_eq!(retry["method"], "turn/start");
        assert_eq!(retry["params"]["clientUserMessageId"], client_id);
        assert!(config.inbox.join(&filename).is_file());
    }

    #[test]
    fn a_success_response_is_only_an_attempt_and_does_not_archive_the_message() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename = message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("submitted"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        let idle = subscribed_state(CodexObservedState::Idle);
        let request = delivery.maybe_request(&idle).unwrap().unwrap();
        assert_eq!(
            delivery.ledger.entry(&filename).unwrap().phase,
            delivery_ledger::Phase::Attempted,
            "submission ownership is durable before transport"
        );
        assert!(
            delivery
                .accept_response(
                    &json!({ "id": request["id"], "result": { "turn": { "id": "turn-new" } } }),
                    idle.observed(),
                )
                .unwrap()
        );
        assert_eq!(
            delivery.ledger.entry(&filename).unwrap().phase,
            delivery_ledger::Phase::TransportAccepted,
            "a well-formed JSON result is transport, never typed acceptance"
        );
        assert_eq!(delivery.maybe_request(&idle).unwrap(), None);
        assert!(config.inbox.join(&filename).is_file());
    }

    #[test]
    fn only_a_completed_matching_user_message_persists_acceptance() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename = message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("receipt"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let state_path = tmp.path().join("state/delivery-state.json");
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        let mut idle = CodexControlState::new(&delivery.runtime, "thread-main".into());
        idle.subscribed = true;
        idle.observed = CodexObservedState::Idle;
        let request = delivery.maybe_request(&idle).unwrap().unwrap();
        let client_id = request["params"]["clientUserMessageId"]
            .as_str()
            .unwrap()
            .to_string();

        assert!(
            !delivery
                .accept_typed_receipt(
                    &json!({
                        "method": "item/started",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-delivery",
                            "item": { "type": "userMessage", "clientId": client_id }
                        }
                    }),
                    &idle,
                )
                .unwrap(),
            "item/started is progress, not acceptance"
        );
        assert!(
            !delivery
                .accept_typed_receipt(
                    &json!({
                        "method": "item/completed",
                        "params": {
                            "threadId": "thread-other",
                            "turnId": "turn-delivery",
                            "item": { "type": "userMessage", "clientId": client_id }
                        }
                    }),
                    &idle,
                )
                .unwrap(),
            "another thread cannot acknowledge this delivery"
        );
        assert!(
            delivery
                .accept_typed_receipt(
                    &json!({
                        "method": "item/completed",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-delivery",
                            "item": { "type": "userMessage", "clientId": client_id }
                        }
                    }),
                    &idle,
                )
                .unwrap()
        );
        assert_eq!(
            ledger_entry(tmp.path(), &filename).unwrap().phase,
            delivery_ledger::Phase::Consumed
        );
        assert!(
            !state_path.exists(),
            "consumption released the v1 rollback floor"
        );
        assert!(config.inbox.join(&filename).is_file());

        drop(delivery);
        let mut replacement = inbox_delivery(tmp.path(), config.clone());
        assert_eq!(
            replacement.maybe_request(&idle).unwrap(),
            None,
            "a fresh runtime incarnation restores accepted duplicate control"
        );

        message::archive_msg(
            &config.inbox,
            &message::archive_dir(&config.agent_dir),
            &filename,
        )
        .unwrap();
        replacement.next_inbox_refresh = Instant::now();
        assert_eq!(replacement.maybe_request(&idle).unwrap(), None);
        assert!(
            !state_path.exists(),
            "no outstanding delivery, no rollback floor"
        );
        assert!(
            ledger_entry(tmp.path(), &filename).is_none(),
            "archive precedence — the recipient agent's own act — releases the ledger entry"
        );
    }

    #[test]
    fn an_ambiguous_attempt_reconciles_resume_history_before_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename = message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("reconcile"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let idle = subscribed_state(CodexObservedState::Idle);
        let mut first = inbox_delivery(tmp.path(), config.clone());
        let request = first.maybe_request(&idle).unwrap().unwrap();
        let client_id = request["params"]["clientUserMessageId"]
            .as_str()
            .unwrap()
            .to_string();
        drop(first);

        let mut recovered = inbox_delivery(tmp.path(), config.clone());
        assert_eq!(recovered.maybe_request(&idle).unwrap(), None);
        recovered
            .reconcile_resume(
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": {
                        "thread": {
                            "id": "thread-main",
                            "turns": [{
                                "id": "turn-delivery",
                                "items": [{
                                    "type": "userMessage",
                                    "id": "item-delivery",
                                    "clientId": client_id,
                                    "content": []
                                }]
                            }]
                        }
                    }
                }),
                &idle,
            )
            .unwrap();
        assert_eq!(
            recovered.ledger.entry(&filename).unwrap().phase,
            delivery_ledger::Phase::Consumed,
            "a resumed history carrying the client ID is the same typed receipt, found late"
        );
        assert_eq!(recovered.maybe_request(&idle).unwrap(), None);
        assert!(config.inbox.join(&filename).is_file());

        // An authoritative resumed history WITHOUT the client ID proves the pre-crash attempt
        // never landed. Only that absence may re-authorize the same stable ID — so it needs its
        // own scenario, because the delivery above is settled and can never be un-settled.
        let absent_tmp = tempfile::tempdir().unwrap();
        let absent_config = delivery_config(absent_tmp.path());
        let absent_filename = message::send_to_inbox(
            &absent_config.inbox,
            "h.sender",
            Some("absent"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let mut attempted = inbox_delivery(absent_tmp.path(), absent_config.clone());
        let absent_client_id = attempted.maybe_request(&idle).unwrap().unwrap()
            ["params"]["clientUserMessageId"]
            .as_str()
            .unwrap()
            .to_string();
        drop(attempted);

        let mut replacement = inbox_delivery(absent_tmp.path(), absent_config);
        assert_eq!(
            replacement.maybe_request(&idle).unwrap(),
            None,
            "an ambiguous attempt is held and surfaced, never replayed on its own"
        );
        replacement
            .reconcile_resume(
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": { "thread": { "id": "thread-main", "turns": [] } }
                }),
                &idle,
            )
            .unwrap();
        assert_eq!(
            replacement
                .ledger
                .entry(&absent_filename)
                .unwrap()
                .negative,
            Some(delivery_ledger::NegativeReceipt::Absent),
            "the absence is retained as evidence, not erased"
        );
        let retry = replacement.maybe_request(&idle).unwrap().unwrap();
        assert_eq!(retry["params"]["clientUserMessageId"], absent_client_id);
    }

    /// v1 refused to START on a record whose client ID contradicted its own binding, which is the
    /// worst available failure: the control connection never comes up and nothing is delivered at
    /// all. The ledger fails closed instead — it starts, authorizes no transport, retains the
    /// reason, and destroys no evidence.
    #[test]
    fn a_tampered_v1_record_fails_closed_without_refusing_to_start() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("held"), None, &[], "body")
                .unwrap();
        let state_path = tmp.path().join("state/delivery-state.json");
        atomic_json(
            &state_path,
            &json!({
                "schema": delivery_ledger::CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "runtimeIncarnation": "incarnation-test",
                "threadId": "thread-main",
                "filename": "1786380000000-abc123.md",
                "clientId": "st2:tampered",
                "phase": "attempted"
            }),
        )
        .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        assert!(
            delivery
                .ledger
                .quarantined()
                .is_some_and(|reason| reason.contains("does not match its binding")),
            "the refusal names itself"
        );
        assert_eq!(
            delivery
                .maybe_request(&subscribed_state(CodexObservedState::Idle))
                .unwrap(),
            None,
            "a quarantined ledger authorizes no transport"
        );
        // The operator-visible surface is the existing typed delivery boundary. No new
        // vocabulary, and the raw quarantine reason stays in tracing rather than the record.
        let driver_diagnostic::Observed::Failure(failure) =
            driver_diagnostic::read(&driver_diagnostic::path(&config.agent_dir))
        else {
            panic!("a quarantined ledger must be diagnosed")
        };
        assert_eq!(failure.stage, driver_diagnostic::Stage::Delivery);
        assert_eq!(failure.reason, driver_diagnostic::Reason::DeliveryUnavailable);
        assert_eq!(failure.source, driver_diagnostic::Source::PromptTransport);
        assert!(
            state_path.exists(),
            "a record we refuse to read is not a record we may destroy"
        );
        assert!(config.inbox.join(&filename).is_file());
    }

    #[test]
    fn subscribed_control_pump_delivers_a_typed_reference_to_the_real_fifo_head() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("wired"), None, &[], "body")
                .unwrap();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server_filename = filename.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                // Parallel Darwin test runs can deschedule the in-process peer
                // for longer than the Linux-oriented two-second budget.
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": "thread-main", "status": { "type": "idle" } } }
                }),
            )
            .unwrap();
            let delivery = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(delivery["id"], FIRST_DELIVERY_REQUEST_ID);
            assert_eq!(delivery["method"], "turn/start");
            assert_eq!(delivery["params"]["threadId"], "thread-main");
            let head_id = server_filename
                .trim_end_matches(".md")
                .rsplit_once('-')
                .unwrap()
                .1;
            assert!(
                delivery["params"]["input"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains(head_id),
                "the transport payload must identify the actionable FIFO head"
            );
            assert_eq!(
                delivery["params"]["clientUserMessageId"],
                stable_client_user_message_id("h.worker", "thread-main", &server_filename)
            );
            let client_id = delivery["params"]["clientUserMessageId"]
                .as_str()
                .unwrap()
                .to_string();
            write_json_message(
                &mut websocket,
                &json!({
                    "id": FIRST_DELIVERY_REQUEST_ID,
                    "result": { "turn": { "id": "turn-delivery" } }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": "thread-main",
                        "turnId": "turn-delivery",
                        "item": {
                            "type": "userMessage",
                            "id": "item-delivery",
                            "clientId": client_id,
                            "content": []
                        }
                    }
                }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime_for_pump,
                None,
                Some(config),
                tx,
            )
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();
        assert!(delivery_config(tmp.path()).inbox.join(&filename).is_file());
        assert_eq!(
            ledger_entry(tmp.path(), &filename).unwrap().phase,
            delivery_ledger::Phase::Consumed
        );
    }

    /// The wiring, not the arithmetic: a `thread/tokenUsage/updated` arriving on the real control
    /// socket reaches the record. Every other context test drives the producer directly, so all of
    /// them would stay green if the pump stopped handing it frames — which is exactly how a
    /// producer silently stops producing.
    #[test]
    fn the_control_pump_publishes_a_context_reading_from_a_live_token_usage_notification() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let config = delivery_config(tmp.path());
        let agent_dir = config.agent_dir.clone();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": "thread-main", "status": { "type": "idle" } } }
                }),
            )
            .unwrap();
            write_json_message(&mut websocket, &token_usage_frame(92_283, json!(258_400))).unwrap();
            // Hold the connection open until the reading has landed: closing here would race the
            // pump's read of the frame just written. Bounded, so a pump that stopped handing
            // frames to the producer fails this test instead of hanging it.
            let deadline = Instant::now() + Duration::from_secs(10);
            while harness_context::read(&harness_context::harness_context_path(&agent_dir))
                .is_none()
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime,
                None,
                Some(config),
                tx,
            )
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let observed = context_record(&tmp.path().join("agents/h/worker"))
            .expect("the pump published nothing");
        assert_eq!(observed.harness, harness_context::Harness::Codex);
        assert_eq!(observed.used_tokens, Some(92_283));
        assert_eq!(observed.window_tokens, Some(258_400));
        assert_eq!(observed.used_percent, Some(33.0));
    }

    #[test]
    fn subscribed_control_pump_reconciles_an_ambiguous_attempt_without_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let config = delivery_config(tmp.path());
        let filename = message::send_to_inbox(
            &config.inbox,
            "h.sender",
            Some("recover"),
            None,
            &[],
            "body",
        )
        .unwrap();
        let client_id = stable_client_user_message_id("h.worker", "thread-main", &filename);
        let prior_runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let delivery_state_path = tmp.path().join("state/delivery-state.json");
        // The v1 record a prior binary left, in exactly the shape it wrote it. This is the
        // migration boundary: no ledger exists, so the new pump adopts this attempt.
        atomic_json(
            &delivery_state_path,
            &json!({
                "schema": delivery_ledger::CODEX_LEGACY_SCHEMA,
                "agent": "h.worker",
                "runtimeId": "h.worker",
                "runtimeIncarnation": prior_runtime.incarnation(),
                "threadId": "thread-main",
                "filename": &filename,
                "clientId": &client_id,
                "phase": "attempted"
            }),
        )
        .unwrap();

        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server_client_id = client_id.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            let loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(loaded["method"], "thread/loaded/list");
            assert_eq!(loaded["id"], CONTROL_TUI_LOADED_REQUEST_ID);
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": ["thread-main"] }
                }),
            )
            .unwrap();
            let subscribe = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(subscribe["method"], "thread/resume");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": {
                        "thread": {
                            "id": "thread-main",
                            "status": { "type": "idle" },
                            "turns": [{
                                "id": "turn-delivery",
                                "items": [{
                                    "type": "userMessage",
                                    "id": "item-delivery",
                                    "clientId": server_client_id,
                                    "content": []
                                }]
                            }]
                        }
                    }
                }),
            )
            .unwrap();
            assert!(matches!(
                poll_json_message(&mut websocket).unwrap(),
                ControlRead::Timeout
            ));
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let (resume_ready_tx, resume_ready_rx) = mpsc::channel();
        resume_ready_tx.send(()).unwrap();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime_for_pump,
                Some(ControlResume {
                    thread_id: "thread-main",
                    ready: resume_ready_rx,
                    tui_loaded_timeout: TUI_LOADED_TIMEOUT,
                }),
                Some(config),
                tx,
            )
        });
        acknowledge_tui_thread_loaded(&rx);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let recovered = ledger_entry(tmp.path(), &filename).expect("the adopted attempt survives");
        assert_eq!(recovered.phase, delivery_ledger::Phase::Consumed);
        assert_eq!(recovered.correlation.value, client_id);
        assert_eq!(
            recovered.adopted_from.as_deref(),
            Some(delivery_ledger::CODEX_LEGACY_SCHEMA),
            "the receipt landed on the entry carried forward from v1, not on a second attempt"
        );
        assert!(
            !delivery_state_path.exists(),
            "consumption released the v1 record it was adopted from"
        );
        assert!(delivery_config(tmp.path()).inbox.join(&filename).is_file());
    }

    #[test]
    fn control_initializes_before_recording_the_first_thread_only() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            let initialize = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialize["method"], "initialize");
            assert_eq!(initialize["params"]["clientInfo"]["name"], "st2");
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            let initialized = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialized["method"], "initialized");
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": "thread-main", "status": { "type": "idle" } } }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/status/changed",
                    "params": { "threadId": "thread-main", "status": { "type": "idle" } }
                }),
            )
            .unwrap();
            // JSON-RPC request IDs are per direction. A server request may reuse the client's
            // subscription ID and must not be consumed as a client response.
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "method": "item/commandExecution/requestApproval",
                    "params": {}
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": "thread-review", "status": { "type": "idle" } } }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "turn/started",
                    "params": { "threadId": "thread-main", "turn": { "id": "turn-main" } }
                }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let state = tmp.path().join("state");
        let binding_path = state.join("binding.json");
        let control_state_path = state.join("control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime_for_pump,
                None,
                None,
                tx,
            )
        });
        let first_event = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            matches!(first_event, ControlEvent::Bound),
            "first control event: {first_event:?}"
        );
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let binding = load_current_binding(&binding_path, &runtime)
            .unwrap()
            .unwrap();
        assert_eq!(binding.thread_id(), "thread-main");
        let state =
            load_current_control_state(&state.join("control-state.json"), &runtime, &binding)
                .unwrap()
                .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-main".into()
            }
        );
        assert!(state.subscribed());
    }

    #[test]
    fn expected_resume_waits_for_tui_loaded_thread_and_binds_from_control_response() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let (pre_gate_checked_tx, pre_gate_checked_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            let initialize = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialize["method"], "initialize");
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            let initialized = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(initialized["method"], "initialized");
            assert!(matches!(
                poll_json_message(&mut websocket).unwrap(),
                ControlRead::Timeout
            ));
            pre_gate_checked_tx.send(()).unwrap();
            websocket
                .get_mut()
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/started",
                    "params": {
                        "thread": { "id": "thread-unrelated", "status": { "type": "idle" } }
                    }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-unrelated",
                        "status": { "type": "active", "activeFlags": [] }
                    }
                }),
            )
            .unwrap();
            let first_loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(first_loaded["method"], "thread/loaded/list");
            assert_eq!(first_loaded["id"], CONTROL_TUI_LOADED_REQUEST_ID);
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": ["thread-unrelated"] }
                }),
            )
            .unwrap();
            let second_loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(second_loaded["method"], "thread/loaded/list");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": ["thread-unrelated", "thread-prior"] }
                }),
            )
            .unwrap();
            let subscribe = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(subscribe["method"], "thread/resume");
            assert_eq!(subscribe["params"]["threadId"], "thread-prior");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": {
                        "thread": { "id": "thread-prior", "status": { "type": "idle" } }
                    }
                }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let (resume_ready_tx, resume_ready_rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime_for_pump,
                Some(ControlResume {
                    thread_id: "thread-prior",
                    ready: resume_ready_rx,
                    tui_loaded_timeout: TUI_LOADED_TIMEOUT,
                }),
                None,
                tx,
            )
        });
        pre_gate_checked_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        resume_ready_tx.send(()).unwrap();
        acknowledge_tui_thread_loaded(&rx);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let binding = load_current_binding(&binding_path, &runtime)
            .unwrap()
            .unwrap();
        assert_eq!(binding.thread_id(), "thread-prior");
        let state = load_current_control_state(&control_state_path, &runtime, &binding)
            .unwrap()
            .unwrap();
        assert!(state.subscribed());
        assert_eq!(state.observed(), &CodexObservedState::Idle);
    }

    /// A resumed thread still holds its context, and the app-server replays
    /// `thread/tokenUsage/updated` to the newly attached connection — before the resume response,
    /// which the binding handshake otherwise discards along with every other notification. The
    /// construction that resumed this seat has already removed the predecessor's record, so a
    /// dropped replay leaves a resumed-and-idle seat reading `null` against a full window with
    /// nothing to correct it until its next model response.
    #[test]
    fn a_token_usage_replayed_before_the_resume_response_still_reaches_the_record() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let config = delivery_config(tmp.path());
        let agent_dir = config.agent_dir.clone();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let agent_dir_for_server = agent_dir.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            let loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(loaded["method"], "thread/loaded/list");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": ["thread-prior"] }
                }),
            )
            .unwrap();
            let subscribe = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(subscribe["method"], "thread/resume");
            assert_eq!(subscribe["params"]["threadId"], "thread-prior");
            // The replay, ahead of the response the handshake is waiting for.
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/tokenUsage/updated",
                    "params": {
                        "threadId": "thread-prior",
                        "turnId": "turn-prior",
                        "tokenUsage": {
                            "last": { "totalTokens": 92_283 },
                            "total": { "totalTokens": 2_235_329 },
                            "modelContextWindow": 258_400
                        }
                    }
                }),
            )
            .unwrap();
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": {
                        "thread": { "id": "thread-prior", "status": { "type": "idle" } }
                    }
                }),
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while harness_context::read(&harness_context::harness_context_path(
                &agent_dir_for_server,
            ))
            .is_none()
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let (resume_ready_tx, resume_ready_rx) = mpsc::channel();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime,
                Some(ControlResume {
                    thread_id: "thread-prior",
                    ready: resume_ready_rx,
                    tui_loaded_timeout: TUI_LOADED_TIMEOUT,
                }),
                Some(config),
                tx,
            )
        });
        resume_ready_tx.send(()).unwrap();
        acknowledge_tui_thread_loaded(&rx);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let observed =
            context_record(&agent_dir).expect("the replayed reading never reached the record");
        assert_eq!(observed.used_percent, Some(33.0));
        assert_eq!(observed.used_tokens, Some(92_283));
        assert_eq!(observed.session_total_tokens, Some(2_235_329));
    }

    #[test]
    fn tui_loaded_timeout_reports_the_specific_failure_before_outer_binding_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            let loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(loaded["method"], "thread/loaded/list");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": [] }
                }),
            )
            .unwrap();
            thread::sleep(Duration::from_millis(250));
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let (resume_ready_tx, resume_ready_rx) = mpsc::channel();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_path,
                &control_state_path,
                &runtime,
                Some(ControlResume {
                    thread_id: "thread-prior",
                    ready: resume_ready_rx,
                    tui_loaded_timeout: Duration::from_millis(50),
                }),
                None,
                tx,
            )
        });
        resume_ready_tx.send(()).unwrap();
        let ControlEvent::Failed(error) = rx.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("inner TUI-loaded deadline did not report its specific failure");
        };
        assert!(
            error.contains(
                "controlled Codex TUI did not load preserved thread thread-prior before control resume"
            ),
            "unexpected control failure: {error}"
        );

        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn missing_saved_rollout_fails_without_rebinding_the_incarnation() {
        let tmp = tempfile::tempdir().unwrap();
        let _stop_exclusive = stop_flag_tests();
        let binding_path = tmp.path().join("state/binding.json");
        let control_state_path = tmp.path().join("state/control-state.json");
        let prior_runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let prior_binding = CodexThreadBinding::new(&prior_runtime, "thread-prior".into());
        atomic_json(&binding_path, &prior_binding).unwrap();

        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialize"
            );
            write_json_message(
                &mut websocket,
                &json!({ "id": 0, "result": { "userAgent": "fake" } }),
            )
            .unwrap();
            assert_eq!(
                read_json_message(&mut websocket).unwrap().unwrap()["method"],
                "initialized"
            );
            let loaded = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(loaded["method"], "thread/loaded/list");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_TUI_LOADED_REQUEST_ID,
                    "result": { "data": ["thread-prior"] }
                }),
            )
            .unwrap();
            let resume = read_json_message(&mut websocket).unwrap().unwrap();
            assert_eq!(resume["method"], "thread/resume");
            assert_eq!(resume["params"]["threadId"], "thread-prior");
            write_json_message(
                &mut websocket,
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "error": {
                        "code": -32600,
                        "message": "no rollout found for thread id thread-prior"
                    }
                }),
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&socket).unwrap();
        let shutdown = stream.try_clone().unwrap();
        let websocket = initialize_control(stream)
            .unwrap()
            .expect("no stop raised in tests");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let (tx, rx) = mpsc::channel();
        let (resume_ready_tx, resume_ready_rx) = mpsc::channel();
        let runtime_for_pump = runtime.clone();
        let binding_for_pump = binding_path.clone();
        let control_state_for_pump = control_state_path.clone();
        let pump = thread::spawn(move || {
            pump_control(
                websocket,
                &binding_for_pump,
                &control_state_for_pump,
                &runtime_for_pump,
                Some(ControlResume {
                    thread_id: "thread-prior",
                    ready: resume_ready_rx,
                    tui_loaded_timeout: TUI_LOADED_TIMEOUT,
                }),
                None,
                tx,
            )
        });
        resume_ready_tx.send(()).unwrap();
        acknowledge_tui_thread_loaded(&rx);
        let ControlEvent::Failed(error) = rx.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("missing saved rollout did not fail closed");
        };
        assert!(error.contains("saved Codex resume binding has no persisted rollout"));

        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();
        assert_eq!(
            serde_json::from_slice::<CodexThreadBinding>(&fs::read(&binding_path).unwrap())
                .unwrap(),
            prior_binding
        );
        assert!(!control_state_path.exists());
    }

    #[test]
    fn a_binding_from_another_runtime_incarnation_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("binding.json");
        let prior = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let current = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        atomic_json(
            &path,
            &CodexThreadBinding::new(&prior, "thread-prior".into()),
        )
        .unwrap();
        assert_eq!(
            load_resume_thread(&path, "h.worker", "h.worker").unwrap(),
            Some("thread-prior".into()),
            "a validated prior binding may select resume but must not become current ownership"
        );
        let error = load_current_binding(&path, &current).unwrap_err();
        assert!(error.to_string().contains("different runtime incarnation"));
    }

    #[test]
    fn watcher_holds_without_an_exact_turn_and_tracks_one_unmatched_lifecycle() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());

        assert!(
            state
                .observe(&json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-main",
                        "status": { "type": "active", "activeFlags": [] }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::ActiveWithoutTurn,
                turn_id: None,
            }
        );

        assert!(
            state
                .observe(&json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": "thread-main",
                        "turn": { "id": "turn-1" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-1".into()
            }
        );

        assert!(
            !state
                .observe(&json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": "thread-other",
                        "turn": { "id": "turn-other" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-1".into()
            }
        );

        assert!(
            state
                .observe(&json!({
                    "method": "turn/completed",
                    "params": {
                        "threadId": "thread-main",
                        "turn": { "id": "turn-1" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(state.observed(), &CodexObservedState::Idle);
    }

    #[test]
    fn watcher_holds_review_compaction_and_conflicting_turns_until_safe() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());

        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-main",
                    "turnId": "turn-1",
                    "item": { "type": "enteredReviewMode" }
                }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-1".into()),
            }
        );

        state
            .observe(&json!({
                "method": "thread/status/changed",
                "params": {
                    "threadId": "thread-main",
                    "status": { "type": "active", "activeFlags": [] }
                }
            }))
            .unwrap();
        assert!(matches!(
            state.observed(),
            CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                ..
            }
        ));

        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-2" } }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-2".into()),
            }
        );

        // Codex can complete the preparatory review item after the reviewer turn starts. That
        // duplicate review event keeps the typed hold bound to the newer turn.
        assert!(
            !state
                .observe(&json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": "thread-main",
                        "turnId": "turn-1",
                        "item": { "type": "enteredReviewMode" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-2".into()),
            }
        );

        // The review hold also survives the stale turn completion. Only an idle thread releases
        // it.
        assert!(
            !state
                .observe(&json!({
                    "method": "turn/completed",
                    "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-2".into()),
            }
        );

        state
            .observe(&json!({
                "method": "thread/status/changed",
                "params": { "threadId": "thread-main", "status": { "type": "idle" } }
            }))
            .unwrap();
        assert_eq!(state.observed(), &CodexObservedState::Idle);

        // A real review can start its reviewer turn before Codex reports the preparatory turn's
        // typed review item. The typed non-steerable event refines that generic conflict.
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-late-1" } }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-late-2" } }
            }))
            .unwrap();
        assert!(matches!(
            state.observed(),
            CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                ..
            }
        ));
        state
            .observe(&json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-main",
                    "turnId": "turn-late-1",
                    "item": { "type": "enteredReviewMode" }
                }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-late-1".into()),
            }
        );
        state
            .observe(&json!({
                "method": "thread/status/changed",
                "params": { "threadId": "thread-main", "status": { "type": "idle" } }
            }))
            .unwrap();

        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-3" } }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-main",
                    "turnId": "turn-3",
                    "item": { "type": "contextCompaction" }
                }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Compaction,
                turn_id: Some("turn-3".into()),
            }
        );
        assert!(
            !state
                .observe(&json!({
                    "method": "turn/completed",
                    "params": { "threadId": "thread-main", "turn": { "id": "turn-3" } }
                }))
                .unwrap()
        );
        assert!(matches!(
            state.observed(),
            CodexObservedState::Held {
                reason: CodexHoldReason::Compaction,
                ..
            }
        ));
    }

    #[test]
    fn exiting_review_mode_mid_turn_restores_the_steerable_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("held"), None, &[], "body")
                .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());

        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state.subscribed = true;

        // An inline review runs as its own turn on the reviewed thread, so the hold binds to the
        // reviewer turn that `exitedReviewMode` later reports.
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-review" } }
            }))
            .unwrap();
        state
            .observe(&json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-main",
                    "turnId": "turn-review",
                    "item": { "type": "enteredReviewMode", "id": "item-1", "review": "review" }
                }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-review".into()),
            }
        );
        assert_eq!(delivery.maybe_request(&state).unwrap(), None);

        // Review ends while the turn keeps running: the typed exit item is the only signal, and it
        // must restore the exact turn the hold carried instead of waiting for the next idle.
        assert!(
            state
                .observe(&json!({
                    "method": "item/started",
                    "params": {
                        "threadId": "thread-main",
                        "turnId": "turn-review",
                        "item": { "type": "exitedReviewMode", "id": "item-2", "review": "review" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-review".into(),
            }
        );

        // Codex reports both lifecycle edges of the same item; the second one changes nothing.
        assert!(
            !state
                .observe(&json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": "thread-main",
                        "turnId": "turn-review",
                        "item": { "type": "exitedReviewMode", "id": "item-2", "review": "review" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-review".into(),
            }
        );

        // The payoff: native delivery steers the still-running turn instead of waiting for idle.
        let request = delivery.maybe_request(&state).unwrap().unwrap();
        assert_eq!(request["method"], "turn/steer");
        assert_eq!(request["params"]["threadId"], "thread-main");
        assert_eq!(request["params"]["expectedTurnId"], "turn-review");
        assert!(config.inbox.join(&filename).is_file());
    }

    #[test]
    fn delivery_irrelevant_items_and_foreign_turn_review_exits_keep_the_observed_state() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();

        // Most item types say nothing about steerability. They are ignored on purpose, not by
        // omission: the observed state and the changed flag both stay put.
        for item_type in ["agentMessage", "commandExecution", "webSearch"] {
            assert!(
                !state
                    .observe(&json!({
                        "method": "item/completed",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-1",
                            "item": { "type": item_type, "id": "item-1" }
                        }
                    }))
                    .unwrap()
            );
            assert_eq!(
                state.observed(),
                &CodexObservedState::Active {
                    turn_id: "turn-1".into(),
                }
            );
        }

        // A review exit reporting a turn the hold does not carry proves nothing about the held
        // turn, so the hold survives exactly as it did before typed exits were observed.
        state.observed = CodexObservedState::Held {
            reason: CodexHoldReason::Review,
            turn_id: Some("turn-2".into()),
        };
        let stale_exit = json!({
            "method": "item/completed",
            "params": {
                "threadId": "thread-main",
                "turnId": "turn-1",
                "item": { "type": "exitedReviewMode", "id": "item-2", "review": "review" }
            }
        });
        assert!(!state.observe(&stale_exit).unwrap());
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::Review,
                turn_id: Some("turn-2".into()),
            }
        );

        // A review exit never invents a turn on an idle thread and never releases another hold.
        for observed in [
            CodexObservedState::Idle,
            CodexObservedState::AwaitingStatus,
            CodexObservedState::Held {
                reason: CodexHoldReason::Compaction,
                turn_id: Some("turn-1".into()),
            },
            CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                turn_id: None,
            },
        ] {
            state.observed = observed.clone();
            assert!(
                !state
                    .observe(&json!({
                        "method": "item/started",
                        "params": {
                            "threadId": "thread-main",
                            "turnId": "turn-1",
                            "item": {
                                "type": "exitedReviewMode",
                                "id": "item-3",
                                "review": "review"
                            }
                        }
                    }))
                    .unwrap()
            );
            assert_eq!(state.observed(), &observed);
        }
    }

    #[test]
    fn an_unclassified_item_holds_until_the_next_idle_status() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();

        assert!(
            state
                .observe(&json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": "thread-main",
                        "turnId": "turn-1",
                        "item": { "type": "futureBlockingItem", "id": "item-1" }
                    }
                }))
                .unwrap()
        );
        assert!(matches!(
            state.observed(),
            CodexObservedState::Held {
                reason: CodexHoldReason::UnknownProtocol,
                turn_id: Some(turn_id),
            } if turn_id == "turn-1"
        ));

        assert!(
            state
                .observe(&json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-main",
                        "status": { "type": "idle" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(state.observed(), &CodexObservedState::Idle);
    }

    #[test]
    fn an_unclassified_server_request_holds_until_the_next_idle_status() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();

        assert!(
            !state
                .observe(&json!({
                    "id": 1,
                    "method": "item/commandExecution/requestApproval",
                    "params": {}
                }))
                .unwrap()
        );
        assert!(matches!(
            state.observed(),
            CodexObservedState::Active { .. }
        ));

        assert!(
            state
                .observe(&json!({
                    "id": 2,
                    "method": "future/request",
                    "params": {}
                }))
                .unwrap()
        );
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::UnknownProtocol,
                turn_id: Some("turn-1".into()),
            }
        );

        assert!(
            state
                .observe(&json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-main",
                        "status": { "type": "idle" }
                    }
                }))
                .unwrap()
        );
        assert_eq!(state.observed(), &CodexObservedState::Idle);
    }

    #[test]
    fn an_errored_turn_completes_into_the_named_error_not_a_conflicting_turn() {
        // Replays the captured terminal-error ordering (#264): a usage limit emits
        // `thread/status/changed -> systemError` immediately before the failed turn's
        // `turn/completed`. That completion reports one turn's lifecycle and carries no thread
        // status, so it is not evidence the thread recovered, and it is not evidence of a second
        // live turn either. The honest resolution is the condition the thread itself reported.
        for (status, reason) in [
            ("systemError", CodexHoldReason::SystemError),
            ("notLoaded", CodexHoldReason::NotLoaded),
        ] {
            let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
            let mut state = CodexControlState::new(&runtime, "thread-main".into());
            state.subscribed = true;
            state
                .observe(&json!({
                    "method": "turn/started",
                    "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
                }))
                .unwrap();
            assert!(
                state
                    .observe(&json!({
                        "method": "thread/status/changed",
                        "params": { "threadId": "thread-main", "status": { "type": status } }
                    }))
                    .unwrap()
            );
            assert_eq!(
                state.observed(),
                &CodexObservedState::Held {
                    reason,
                    turn_id: None
                }
            );

            // Completion makes a reported system error terminal. It preserves `notLoaded`, whose
            // owner is the later thread status that proves the thread loaded again.
            let changed = state
                .observe(&json!({
                    "method": "turn/completed",
                    "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
                }))
                .unwrap();

            if reason == CodexHoldReason::SystemError {
                assert!(changed);
                assert_eq!(
                    state.observed(),
                    &CodexObservedState::TerminalError {
                        reason: CodexTerminalError::SystemError,
                    }
                );
            } else {
                assert!(!changed);
                assert_eq!(
                    state.observed(),
                    &CodexObservedState::Held {
                        reason,
                        turn_id: None,
                    }
                );
            }

            let tmp = tempfile::tempdir().unwrap();
            let config = delivery_config(tmp.path());
            let filename =
                message::send_to_inbox(&config.inbox, "h.sender", Some("held"), None, &[], "body")
                    .unwrap();
            let mut delivery = inbox_delivery(tmp.path(), config.clone());
            if reason == CodexHoldReason::SystemError {
                let request = delivery
                    .maybe_request(&state)
                    .unwrap()
                    .expect("a terminal system error must permit the next turn");
                assert_eq!(request["method"], "turn/start");
            } else {
                assert_eq!(delivery.maybe_request(&state).unwrap(), None);
                assert!(
                    state
                        .observe(&json!({
                            "method": "thread/status/changed",
                            "params": {
                                "threadId": "thread-main",
                                "status": { "type": "idle" }
                            }
                        }))
                        .unwrap()
                );
                assert_eq!(state.observed(), &CodexObservedState::Idle);
                assert!(delivery.maybe_request(&state).unwrap().is_some());
            }
            assert!(config.inbox.join(&filename).is_file());
        }

        // The next provider turn replaces the terminal diagnostic with the exact live turn.
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state.subscribed = true;
        for message in [
            json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }),
            json!({
                "method": "thread/status/changed",
                "params": { "threadId": "thread-main", "status": { "type": "systemError" } }
            }),
            json!({
                "method": "turn/completed",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }),
        ] {
            state.observe(&message).unwrap();
        }
        assert_eq!(
            state.observed(),
            &CodexObservedState::TerminalError {
                reason: CodexTerminalError::SystemError,
            }
        );
        state
            .observe(&json!({
                "method": "thread/status/changed",
                "params": {
                    "threadId": "thread-main",
                    "status": { "type": "active", "activeFlags": [] }
                }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::ActiveWithoutTurn,
                turn_id: None,
            }
        );
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-2" } }
            }))
            .unwrap();
        assert_eq!(
            state.observed(),
            &CodexObservedState::Active {
                turn_id: "turn-2".into(),
            }
        );
    }

    #[test]
    fn persisted_control_state_is_bound_to_the_exact_runtime_incarnation() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("control-state.json");
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let binding = CodexThreadBinding::new(&runtime, "thread-main".into());
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        state.observed = CodexObservedState::Active {
            turn_id: "turn-1".into(),
        };
        atomic_json(&path, &state).unwrap();
        let persisted: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted["observed"]["turnId"], "turn-1");
        assert!(persisted["observed"].get("turn_id").is_none());

        assert_eq!(
            load_current_control_state(&path, &runtime, &binding)
                .unwrap()
                .unwrap(),
            state
        );

        let replacement = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let replacement_binding = CodexThreadBinding::new(&replacement, "thread-main".into());
        let error =
            load_current_control_state(&path, &replacement, &replacement_binding).unwrap_err();
        assert!(error.to_string().contains("different runtime binding"));
    }

    #[test]
    fn subscription_waits_for_a_rollout_without_claiming_success() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        let acceptance = state
            .accept_subscription(&json!({
                "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                "error": {
                    "code": -32600,
                    "message": "no rollout found for thread id thread-main"
                }
            }))
            .unwrap();

        assert!(matches!(acceptance, SubscriptionAcceptance::Deferred));
        assert!(!state.subscribed());
        assert_eq!(state.observed(), &CodexObservedState::AwaitingStatus);
    }

    #[test]
    fn app_server_receives_only_its_supported_global_configuration() {
        let authored = vec![
            "-c".into(),
            "projects={\"/workspace\"={trust_level=\"trusted\"}}".into(),
            "--model".into(),
            "gpt-test".into(),
            "--enable".into(),
            "one".into(),
            "--disable=two".into(),
            "--strict-config".into(),
            "--dangerously-bypass-approvals-and-sandbox".into(),
            "--dangerously-bypass-hook-trust".into(),
            "boot".into(),
        ];

        assert_eq!(
            controlled_app_server_args("unix:///server.sock", &authored).unwrap(),
            [
                "app-server",
                "-c",
                "projects={\"/workspace\"={trust_level=\"trusted\"}}",
                "--enable",
                "one",
                "--disable=two",
                "--strict-config",
                "--listen",
                "unix:///server.sock",
            ]
        );
    }

    #[test]
    fn remote_resume_projects_exact_hook_hashes_without_persisted_state() {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(tmp.path()).unwrap();
        let source = cwd.join(".codex/hooks.json");
        let untrusted_key = format!("{}:session_start:0:0", source.display());
        let modified_key = format!("{}:stop:1:0", source.display());
        let response = json!({
            "id": HOOK_TRUST_PREFLIGHT_REQUEST_ID,
            "result": {
                "data": [{
                    "cwd": cwd,
                    "hooks": [
                        {
                            "key": untrusted_key,
                            "currentHash": "sha256:one",
                            "trustStatus": "untrusted",
                            "isManaged": false,
                            "enabled": true
                        },
                        {
                            "key": modified_key,
                            "currentHash": "sha256:two",
                            "trustStatus": "modified",
                            "isManaged": false,
                            "enabled": false
                        },
                        {
                            "key": "already-trusted",
                            "currentHash": "sha256:three",
                            "trustStatus": "trusted",
                            "isManaged": false,
                            "enabled": true
                        },
                        {
                            "key": "managed",
                            "currentHash": "sha256:four",
                            "trustStatus": "managed",
                            "isManaged": true,
                            "enabled": true
                        }
                    ]
                }]
            }
        });

        let projection = hook_trust_projection_from_response(&response, &cwd)
            .unwrap()
            .unwrap();
        assert_eq!(projection.count, 2);
        let parsed: toml::Value = toml::from_str(&projection.override_value).unwrap();
        let state = parsed
            .get("hooks")
            .and_then(|hooks| hooks.get("state"))
            .and_then(toml::Value::as_table)
            .unwrap();
        assert_eq!(
            state[&untrusted_key]["trusted_hash"].as_str(),
            Some("sha256:one")
        );
        assert_eq!(
            state[&modified_key]["trusted_hash"].as_str(),
            Some("sha256:two")
        );
        assert!(!state.contains_key("already-trusted"));
        assert!(!state.contains_key("managed"));

        let mut args = controlled_app_server_args(
            "unix:///server.sock",
            &["--dangerously-bypass-hook-trust".into(), "boot".into()],
        )
        .unwrap();
        insert_app_server_config_override(&mut args, projection.override_value).unwrap();
        assert_eq!(args[args.len() - 4], "-c");
        assert!(args[args.len() - 3].starts_with("hooks.state="));
        assert_eq!(&args[args.len() - 2..], ["--listen", "unix:///server.sock"]);
    }

    #[test]
    fn hook_trust_projection_fails_closed_on_provider_shape_drift() {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(tmp.path()).unwrap();
        let response = json!({
            "result": {
                "data": [{
                    "cwd": cwd,
                    "hooks": [{
                        "key": "hook",
                        "currentHash": "not-a-provider-hash",
                        "trustStatus": "untrusted",
                        "isManaged": false
                    }]
                }]
            }
        });
        let error = hook_trust_projection_from_response(&response, &cwd).unwrap_err();
        assert!(error.to_string().contains("typed currentHash"));

        let response = json!({
            "result": {
                "data": [{
                    "cwd": cwd,
                    "hooks": [{
                        "key": "hook",
                        "currentHash": "sha256:value",
                        "trustStatus": "future-status",
                        "isManaged": false
                    }]
                }]
            }
        });
        let error = hook_trust_projection_from_response(&response, &cwd).unwrap_err();
        assert!(error.to_string().contains("unknown trustStatus"));
    }

    #[test]
    fn hook_preflight_uses_the_explicit_controlled_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let explicit = tmp.path().join("workspace");
        fs::create_dir(&explicit).unwrap();
        assert_eq!(
            controlled_hook_cwd(&[
                "--dangerously-bypass-hook-trust".into(),
                "--cd".into(),
                explicit.display().to_string(),
                "boot".into(),
            ])
            .unwrap(),
            fs::canonicalize(explicit).unwrap()
        );
        assert!(
            authored_bypasses_hook_trust(&[
                "--dangerously-bypass-hook-trust".into(),
                "boot".into()
            ])
            .unwrap()
        );
        assert!(
            !authored_bypasses_hook_trust(&["--".into(), "--dangerously-bypass-hook-trust".into()])
                .unwrap()
        );
    }

    #[test]
    fn process_group_cleanup_reaps_a_native_launcher_descendant() {
        let temporary = tempfile::tempdir().unwrap();
        let descendant_pidfile = temporary.path().join("descendant.pid");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(r#"sh -c 'printf "%s" "$$" > "$DESCENDANT_PIDFILE"; exec sleep 60' & sleep 60"#)
            .env("DESCENDANT_PIDFILE", &descendant_pidfile)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut launcher = spawn_process_group(&mut command, None).unwrap();
        let mut foreign_command = Command::new("/bin/sh");
        foreign_command
            .arg("-c")
            .arg("sleep 60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut foreign_owner = spawn_process_group(&mut foreign_command, None).unwrap();
        let foreign_pid = foreign_owner.id() as i32;
        let deadline = Instant::now() + Duration::from_secs(1);
        // The shell's `>` redirection creates an empty pidfile before `printf`
        // writes, so wait for parsable content, not mere file existence.
        let mut descendant = None;
        while descendant.is_none() && Instant::now() < deadline {
            if let Ok(content) = std::fs::read_to_string(&descendant_pidfile) {
                descendant = content.trim().parse::<i32>().ok();
            }
            if descendant.is_none() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let descendant = descendant.expect("the launcher did not create its native descendant");
        assert!(
            process_can_retain_cleanup_resources(descendant),
            "the native descendant was not alive before cleanup"
        );

        launcher.terminate();
        assert!(
            process_can_retain_cleanup_resources(foreign_pid),
            "cleanup killed a different live owner"
        );
        foreign_owner.terminate();
        let deadline = Instant::now() + Duration::from_secs(1);
        while process_can_retain_cleanup_resources(descendant) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let survived = process_can_retain_cleanup_resources(descendant);
        if survived {
            unsafe {
                libc::kill(descendant, libc::SIGKILL);
            }
        }
        assert!(
            !survived,
            "native descendant {descendant} survived process-group cleanup"
        );
    }

    #[test]
    fn dropping_a_process_group_owner_reaps_the_group_and_socket() {
        let temporary = tempfile::tempdir().unwrap();
        let socket_path = temporary.path().join("app-server.sock");
        let _listener = UnixListener::bind(&socket_path).unwrap();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("sleep 60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let launcher = spawn_process_group(&mut command, Some(&socket_path)).unwrap();
        let launcher_pid = launcher.id() as i32;
        assert!(process_can_retain_cleanup_resources(launcher_pid));

        drop(launcher);
        let deadline = Instant::now() + Duration::from_secs(1);
        while (process_can_retain_cleanup_resources(launcher_pid) || socket_path.exists())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !process_can_retain_cleanup_resources(launcher_pid),
            "the app-server survived owner cleanup"
        );
        assert!(
            !socket_path.exists(),
            "the app-server socket survived owner cleanup"
        );
    }

    #[test]
    fn a_live_socket_refuses_a_second_control_owner() {
        let temporary = tempfile::tempdir().unwrap();
        let socket_path = temporary.path().join("app-server.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();

        let error = prepare_socket_for_launch(&socket_path).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("refusing a second control owner")
        );
        assert!(socket_path.exists(), "the live owner socket was removed");
        assert!(
            UnixStream::connect(&socket_path).is_ok(),
            "the first owner stopped accepting connections"
        );
        drop(listener);
    }

    #[test]
    fn a_dead_socket_is_removed_before_launch() {
        let temporary = tempfile::tempdir().unwrap();
        let socket_path = temporary.path().join("app-server.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        drop(listener);
        assert!(socket_path.exists());

        prepare_socket_for_launch(&socket_path).unwrap();

        assert!(!socket_path.exists(), "the dead socket was not removed");
    }

    #[test]
    fn a_killed_wrapper_reaps_its_app_server_and_the_next_launch_recovers_its_socket() {
        const TEST_NAME: &str = "codex_app_server::tests::a_killed_wrapper_reaps_its_app_server_and_the_next_launch_recovers_its_socket";
        const ROLE: &str = "ST2_CODEX_ORPHAN_TEST_ROLE";
        const SOCKET_PATH: &str = "ST2_CODEX_ORPHAN_TEST_SOCKET";
        const PID_PATH: &str = "ST2_CODEX_ORPHAN_TEST_PID";
        const READY_PATH: &str = "ST2_CODEX_ORPHAN_TEST_READY";

        match std::env::var(ROLE).as_deref() {
            Ok("server") => {
                let socket_path = PathBuf::from(std::env::var_os(SOCKET_PATH).unwrap());
                let ready_path = PathBuf::from(std::env::var_os(READY_PATH).unwrap());
                let _listener = UnixListener::bind(socket_path).unwrap();
                fs::write(ready_path, b"ready").unwrap();
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                }
            }
            Ok("wrapper") => {
                let pid_path = PathBuf::from(std::env::var_os(PID_PATH).unwrap());
                let socket_path = PathBuf::from(std::env::var_os(SOCKET_PATH).unwrap());
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .arg("--exact")
                    .arg(TEST_NAME)
                    .arg("--nocapture")
                    .env(ROLE, "server")
                    .env(SOCKET_PATH, std::env::var_os(SOCKET_PATH).unwrap())
                    .env(READY_PATH, std::env::var_os(READY_PATH).unwrap())
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                let server = spawn_process_group(&mut command, Some(&socket_path)).unwrap();
                fs::write(pid_path, server.id().to_string()).unwrap();
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                }
            }
            Ok(role) => panic!("unknown orphan test role {role}"),
            Err(_) => {}
        }

        let temporary = tempfile::tempdir().unwrap();
        let socket_path = temporary.path().join("app-server.sock");
        let pid_path = temporary.path().join("app-server.pid");
        let ready_path = temporary.path().join("app-server.ready");
        let mut wrapper = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .env(ROLE, "wrapper")
            .env(SOCKET_PATH, &socket_path)
            .env(PID_PATH, &pid_path)
            .env(READY_PATH, &ready_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while (!pid_path.is_file() || !ready_path.is_file()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if !pid_path.is_file() || !ready_path.is_file() {
            let _ = wrapper.kill();
            let _ = wrapper.wait();
            panic!("the wrapper did not start its app-server");
        }
        let server_pid = fs::read_to_string(&pid_path)
            .expect("the wrapper did not report its app-server PID")
            .parse::<i32>()
            .unwrap();
        assert!(
            process_can_retain_cleanup_resources(server_pid),
            "the app-server was not alive before the wrapper died"
        );
        assert!(
            fs::symlink_metadata(&socket_path)
                .unwrap()
                .file_type()
                .is_socket(),
            "the app-server did not bind its socket"
        );

        unsafe {
            libc::kill(wrapper.id() as i32, libc::SIGKILL);
        }
        let _ = wrapper.wait();
        let deadline = Instant::now() + Duration::from_secs(2);
        while process_can_retain_cleanup_resources(server_pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let server_survived = process_can_retain_cleanup_resources(server_pid);
        if server_survived {
            unsafe {
                libc::kill(server_pid, libc::SIGKILL);
            }
        }
        assert!(!server_survived, "the app-server survived its wrapper");
        assert!(
            socket_path.exists(),
            "the app-server did not leave the expected recoverable socket"
        );
        let refusal_deadline = Instant::now() + Duration::from_secs(2);
        let refusal = loop {
            match UnixStream::connect(&socket_path) {
                Ok(stream) if Instant::now() < refusal_deadline => {
                    drop(stream);
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(_) => panic!("the residual socket still had a live listener"),
                Err(error) => break error,
            }
        };
        assert_eq!(refusal.kind(), std::io::ErrorKind::ConnectionRefused);

        prepare_socket_for_launch(&socket_path)
            .expect("the next launch did not recover the residual socket");
        assert!(
            !socket_path.exists(),
            "the next launch did not remove the residual socket"
        );
        let replacement = UnixListener::bind(&socket_path)
            .expect("the next app-server could not bind the recovered socket");
        assert!(
            UnixStream::connect(&socket_path).is_ok(),
            "the replacement app-server socket did not accept a connection"
        );
        drop(replacement);
    }

    #[test]
    fn app_server_configuration_extraction_fails_closed_at_ambiguous_boundaries() {
        let missing =
            controlled_app_server_args("unix:///server.sock", &["-c".into()]).unwrap_err();
        assert!(missing.to_string().contains("has no value"));

        let unknown = controlled_app_server_args(
            "unix:///server.sock",
            &["--future-option".into(), "value".into(), "boot".into()],
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown Codex option"));

        let sensitive = controlled_app_server_args(
            "unix:///server.sock",
            &["--future-token=do-not-log-this".into(), "boot".into()],
        )
        .unwrap_err();
        assert!(sensitive.to_string().contains("--future-token"));
        assert!(!sensitive.to_string().contains("do-not-log-this"));

        assert_eq!(
            controlled_app_server_args(
                "unix:///server.sock",
                &[
                    "--config=projects.x.trust_level=\"trusted\"".into(),
                    "resume".into(),
                    "thread-explicit".into(),
                ],
            )
            .unwrap(),
            [
                "app-server",
                "--config=projects.x.trust_level=\"trusted\"",
                "--listen",
                "unix:///server.sock",
            ]
        );
    }

    #[test]
    fn controlled_tui_resumes_a_prior_binding_without_overriding_authored_selection() {
        let authored = vec!["--model".into(), "gpt-test".into(), "boot".into()];
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &authored, None).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "--model",
                "gpt-test",
                "boot"
            ]
        );
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &authored, Some("thread-prior")).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "--model",
                "gpt-test",
                "thread-prior",
                "boot"
            ]
        );
        assert_eq!(
            controlled_tui_args(
                "unix:///server.sock",
                &["resume".into(), "thread-explicit".into()],
                Some("thread-prior")
            )
            .unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "thread-explicit"
            ]
        );
        assert_eq!(
            expected_resume_thread(
                &["resume".into(), "thread-explicit".into()],
                Some("thread-prior")
            )
            .unwrap(),
            None
        );

        let fork = vec![
            "--dangerously-bypass-hook-trust".into(),
            "fork".into(),
            "thread-explicit".into(),
        ];
        assert_eq!(
            controlled_tui_args("unix:///server.sock", &fork, Some("thread-prior")).unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "--dangerously-bypass-hook-trust",
                "fork",
                "thread-explicit"
            ]
        );
        assert_eq!(
            expected_resume_thread(&fork, Some("thread-prior")).unwrap(),
            None
        );
        assert_eq!(
            expected_resume_thread(&authored, Some("thread-prior")).unwrap(),
            Some("thread-prior")
        );
    }

    #[test]
    fn controlled_tui_resume_fails_closed_at_ambiguous_option_boundaries() {
        let unknown = controlled_tui_args(
            "unix:///server.sock",
            &["--future-option".into(), "value".into(), "prompt".into()],
            Some("thread-prior"),
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown Codex option"));

        let image = controlled_tui_args(
            "unix:///server.sock",
            &["--image".into(), "one.png".into(), "prompt".into()],
            Some("thread-prior"),
        )
        .unwrap_err();
        assert!(image.to_string().contains("explicit `--`"));

        assert_eq!(
            controlled_tui_args(
                "unix:///server.sock",
                &[
                    "--image".into(),
                    "one.png".into(),
                    "--".into(),
                    "prompt".into(),
                ],
                Some("thread-prior"),
            )
            .unwrap(),
            [
                "--remote",
                "unix:///server.sock",
                "resume",
                "--image",
                "one.png",
                "thread-prior",
                "--",
                "prompt"
            ]
        );
    }

    #[test]
    fn state_key_is_path_and_identity_specific_without_embedding_either() {
        let base = Path::new("/state");
        let first = state_dir_in(base, Path::new("/catalog/a"), "h.worker");
        let second = state_dir_in(base, Path::new("/catalog/b"), "h.worker");
        assert_ne!(first, second);
        assert!(first.starts_with("/state/st2/codex"));
        assert!(!first.display().to_string().contains("worker"));
        assert!(!first.display().to_string().contains("catalog/a"));
    }

    #[test]
    fn wrapper_diagnostics_keep_one_bounded_run_without_authored_input() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        secure_dir(&state).unwrap();

        {
            let mut diagnostics = WrapperDiagnostics::open(&state, "h.worker", "h.worker").unwrap();
            diagnostics.record("ownerAcquired", json!({})).unwrap();
            diagnostics
                .record("failed", json!({ "error": "control socket was not ready" }))
                .unwrap();
        }
        let path = state.join("wrapper.log");
        let first = fs::read_to_string(&path).unwrap();
        let entries = first
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["schema"], WRAPPER_DIAGNOSTIC_SCHEMA);
        assert_eq!(entries[0]["agent"], "h.worker");
        assert_eq!(entries[1]["stage"], "failed");
        assert!(first.contains("control socket was not ready"));
        assert!(!first.contains("prompt"));

        {
            let mut replacement = WrapperDiagnostics::open(&state, "h.worker", "h.worker").unwrap();
            replacement.record("ownerAcquired", json!({})).unwrap();
        }
        let replacement = fs::read_to_string(&path).unwrap();
        assert_eq!(replacement.lines().count(), 1);
        assert!(!replacement.contains("control socket was not ready"));
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn runtime_owner_lock_is_nonblocking_and_released_on_close() {
        let tmp = tempfile::tempdir().unwrap();
        let first = acquire_owner_lock(tmp.path()).unwrap();
        let error = acquire_owner_lock(tmp.path()).unwrap_err();
        assert!(error.to_string().contains("already has an owner"));
        drop(first);
        acquire_owner_lock(tmp.path()).unwrap();
    }

    #[test]
    fn waiting_on_a_human_holds_the_exact_turn_and_releases_it_when_the_flag_clears() {
        let runtime = CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap();
        let mut state = CodexControlState::new(&runtime, "thread-main".into());
        let status_changed = |flags: Value| {
            json!({
                "method": "thread/status/changed",
                "params": {
                    "threadId": "thread-main",
                    "status": { "type": "active", "activeFlags": flags }
                }
            })
        };
        let active_turn_1 = CodexObservedState::Active {
            turn_id: "turn-1".into(),
        };

        state.observe(&status_changed(json!([]))).unwrap();
        state
            .observe(&json!({
                "method": "turn/started",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();
        assert_eq!(state.observed(), &active_turn_1);

        for (flags, reason) in [
            (
                json!(["waitingOnApproval"]),
                CodexHoldReason::WaitingOnApproval,
            ),
            (
                json!(["waitingOnUserInput"]),
                CodexHoldReason::WaitingOnUserInput,
            ),
            (
                json!(["conversationHandoff", "waitingOnApproval"]),
                CodexHoldReason::WaitingOnApproval,
            ),
        ] {
            assert!(state.observe(&status_changed(flags)).unwrap());
            assert_eq!(
                state.observed(),
                &CodexObservedState::Held {
                    reason,
                    turn_id: Some("turn-1".into()),
                }
            );
            // Clearing the flag releases the same turn: no `turn/started` repeats mid-turn.
            assert!(state.observe(&status_changed(json!([]))).unwrap());
            assert_eq!(state.observed(), &active_turn_1);
        }

        // An unknown future flag value degrades to plain `active` instead of failing the frame.
        assert!(!state.observe(&status_changed(json!(["handoff"]))).unwrap());
        assert_eq!(state.observed(), &active_turn_1);

        // The same field is carried by `thread/started`, before any turn is known.
        let mut resumed = CodexControlState::new(&runtime, "thread-main".into());
        assert!(
            resumed
                .observe(&json!({
                    "method": "thread/started",
                    "params": {
                        "thread": {
                            "id": "thread-main",
                            "status": {
                                "type": "active",
                                "activeFlags": ["waitingOnUserInput"]
                            }
                        }
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            resumed.observed(),
            &CodexObservedState::Held {
                reason: CodexHoldReason::WaitingOnUserInput,
                turn_id: None,
            }
        );

        // A turn that ends while still flagged stays unsteerable and is released by the next
        // idle status. `observe_turn_completed` is not modified here; this pins only that the
        // flagged hold cannot decay into a steerable turn.
        state
            .observe(&status_changed(json!(["waitingOnApproval"])))
            .unwrap();
        state
            .observe(&json!({
                "method": "turn/completed",
                "params": { "threadId": "thread-main", "turn": { "id": "turn-1" } }
            }))
            .unwrap();
        assert!(matches!(state.observed(), CodexObservedState::Held { .. }));

        // A status arm without `activeFlags` keeps reading exactly as before.
        assert!(
            state
                .observe(&json!({
                    "method": "thread/status/changed",
                    "params": { "threadId": "thread-main", "status": { "type": "idle" } }
                }))
                .unwrap()
        );
        assert_eq!(state.observed(), &CodexObservedState::Idle);

        // Delivery declines to steer a session that is waiting on a human, and retains the head.
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let filename =
            message::send_to_inbox(&config.inbox, "h.sender", Some("held"), None, &[], "body")
                .unwrap();
        let mut delivery = inbox_delivery(tmp.path(), config.clone());
        for reason in [
            CodexHoldReason::WaitingOnApproval,
            CodexHoldReason::WaitingOnUserInput,
        ] {
            let blocked = subscribed_state(CodexObservedState::Held {
                reason,
                turn_id: Some("turn-1".into()),
            });
            assert_eq!(delivery.maybe_request(&blocked).unwrap(), None);
            assert!(config.inbox.join(&filename).is_file());
        }
        let released = delivery
            .maybe_request(&subscribed_state(active_turn_1.clone()))
            .unwrap()
            .expect("the retained head steers once the human has answered");
        assert_eq!(released["method"], "turn/steer");
        assert_eq!(released["params"]["expectedTurnId"], "turn-1");
    }
}
