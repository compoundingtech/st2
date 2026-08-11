//! Controlled Codex app-server launch and persistent thread ownership.
//!
//! Native delivery cannot infer a thread from cwd, process, PTY, or `thread/list`. This module
//! starts a dedicated provider daemon, initializes an observer connection before the interactive
//! client starts, and binds a typed start or successful-resume event to the exact wrapper process
//! incarnation that owns the PTY launch. Its control watcher persists delivery-relevant thread and
//! turn state. The native delivery layer selects one durable FIFO inbox head and submits typed
//! input only when that state proves an idle or one exact regular active turn.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::io::AsRawFd as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tungstenite::{Message as WebSocketMessage, WebSocket};

use crate::{ding, message, run, status};

pub const SUPPORTED_CODEX_CLI_VERSIONS: &[&str] = &["codex-cli 0.145.0", "codex-cli 0.146.0"];
const RUNTIME_SCHEMA: &str = "st2.codex-runtime.v1";
const BINDING_SCHEMA: &str = "st2.codex-thread-binding.v1";
const CONTROL_STATE_SCHEMA: &str = "st2.codex-control-state.v1";
const DELIVERY_STATE_SCHEMA: &str = "st2.codex-delivery-state.v1";
const CONTROL_SUBSCRIBE_REQUEST_ID: u64 = 1;
const FIRST_DELIVERY_REQUEST_ID: u64 = 2;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_POLL: Duration = Duration::from_millis(100);
const INBOX_REFRESH_FALLBACK: Duration = Duration::from_secs(15);
const SOCKET_PATH_BUDGET: usize = 96;

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
/// `Active` is the only state that permits `turn/steer`: its turn ID came from the latest
/// unmatched `turn/started` event. Every other non-idle state is an explicit hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CodexObservedState {
    AwaitingStatus,
    Idle,
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
    NotLoaded,
    SystemError,
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
        Ok(Self {
            catalog_root: catalog_root.to_path_buf(),
            inbox: message::inbox_dir(&agent_dir),
            agent_dir,
            identity: identity.to_string(),
            this_host,
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexDeliveryPhase {
    Attempted,
    Accepted,
}

/// One durable FIFO delivery attempt.
///
/// `Attempted` is written before transport. A replacement control connection reconciles that
/// ambiguous attempt against the resumed thread before it may send the client ID again. `Accepted`
/// is written only after the exact completed typed user-message event and remains until normal
/// message archive precedence removes the inbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CodexDeliveryState {
    schema: String,
    agent: String,
    runtime_id: String,
    runtime_incarnation: String,
    thread_id: String,
    filename: String,
    client_id: String,
    phase: CodexDeliveryPhase,
}

impl CodexDeliveryState {
    fn attempted(
        runtime: &CodexRuntime,
        thread_id: String,
        filename: String,
        client_id: String,
    ) -> Self {
        Self {
            schema: DELIVERY_STATE_SCHEMA.to_string(),
            agent: runtime.agent.clone(),
            runtime_id: runtime.runtime_id.clone(),
            runtime_incarnation: runtime.incarnation.clone(),
            thread_id,
            filename,
            client_id,
            phase: CodexDeliveryPhase::Attempted,
        }
    }
}

#[derive(Debug, Clone)]
struct RejectedCodexDelivery {
    filename: String,
    observed: CodexObservedState,
}

struct CodexInboxDelivery {
    config: CodexDeliveryConfig,
    state_path: PathBuf,
    runtime: CodexRuntime,
    wake: Receiver<()>,
    _watcher: Option<notify::RecommendedWatcher>,
    next_refresh: Instant,
    head: Option<message::Message>,
    suppressed: bool,
    state: Option<CodexDeliveryState>,
    pending: Option<PendingCodexDelivery>,
    rejected: Option<RejectedCodexDelivery>,
    next_request_id: u64,
}

impl CodexInboxDelivery {
    fn new(
        config: CodexDeliveryConfig,
        state_path: PathBuf,
        runtime: CodexRuntime,
    ) -> Result<Self> {
        fs::create_dir_all(&config.inbox).with_context(|| {
            format!(
                "creating Codex native delivery inbox {}",
                config.inbox.display()
            )
        })?;
        let (wake_tx, wake) = mpsc::channel();
        let watcher = crate::watch::watch_recursive_mutations(&config.agent_dir, wake_tx);
        let state = load_delivery_state(&state_path, &config.identity, runtime.runtime_id())?;
        Ok(Self {
            config,
            state_path,
            runtime,
            wake,
            _watcher: watcher,
            next_refresh: Instant::now(),
            head: None,
            suppressed: false,
            state,
            pending: None,
            rejected: None,
            next_request_id: FIRST_DELIVERY_REQUEST_ID,
        })
    }

    fn write_state(&mut self, state: CodexDeliveryState) -> Result<()> {
        atomic_json(&self.state_path, &state)?;
        self.state = Some(state);
        Ok(())
    }

    fn clear_state(&mut self) -> Result<()> {
        remove_state_file(&self.state_path)?;
        self.state = None;
        Ok(())
    }

    fn refresh_if_due(&mut self) -> Result<()> {
        let mut due = Instant::now() >= self.next_refresh;
        while self.wake.try_recv().is_ok() {
            due = true;
        }
        if !due {
            return Ok(());
        }
        let unread = message::list_inbox(&self.config.inbox)?;
        if self.state.as_ref().is_some_and(|state| {
            unread
                .iter()
                .all(|message| message.filename != state.filename)
        }) {
            self.clear_state()?;
        }
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
        self.next_refresh = Instant::now() + INBOX_REFRESH_FALLBACK;
        Ok(())
    }

    fn maybe_request(&mut self, state: &CodexControlState) -> Result<Option<Value>> {
        self.refresh_if_due()?;
        if self.pending.is_some() || !state.subscribed || self.suppressed {
            return Ok(None);
        }
        if let Some(delivery_state) = self.state.as_ref() {
            if delivery_state.thread_id == state.thread_id {
                return Ok(None);
            }
            // A newly selected thread is a different delivery binding. An old binding's receipt
            // must neither suppress nor acknowledge delivery to this thread.
            self.clear_state()?;
        }
        let Some(head) = self.head.as_ref() else {
            return Ok(None);
        };
        if self.rejected.as_ref().is_some_and(|rejected| {
            rejected.filename == head.filename && rejected.observed == state.observed
        }) {
            return Ok(None);
        }
        let method = match &state.observed {
            CodexObservedState::Idle => CodexDeliveryMethod::Start,
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
            head,
        );
        let request =
            codex_delivery_request(request_id, state.thread_id(), &client_id, &text, &method);
        self.write_state(CodexDeliveryState::attempted(
            &self.runtime,
            state.thread_id().to_string(),
            filename.clone(),
            client_id,
        ))?;
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
            if !self
                .state
                .as_ref()
                .is_some_and(|state| state.phase == CodexDeliveryPhase::Accepted)
            {
                self.clear_state()?;
            }
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
        self.rejected = None;
        Ok(true)
    }

    fn accept_typed_receipt(&mut self, message: &Value, state: &CodexControlState) -> Result<bool> {
        if message.get("method").and_then(Value::as_str) != Some("item/completed")
            || message.pointer("/params/item/type").and_then(Value::as_str) != Some("userMessage")
        {
            return Ok(false);
        }
        let Some(delivery_state) = self.state.as_ref() else {
            return Ok(false);
        };
        if message.pointer("/params/threadId").and_then(Value::as_str) != Some(state.thread_id())
            || delivery_state.thread_id != state.thread_id()
            || delivery_state.runtime_incarnation != self.runtime.incarnation()
            || state.runtime_incarnation != self.runtime.incarnation()
            || message
                .pointer("/params/item/clientId")
                .and_then(Value::as_str)
                != Some(delivery_state.client_id.as_str())
        {
            return Ok(false);
        }
        if delivery_state.phase == CodexDeliveryPhase::Accepted {
            return Ok(true);
        }
        let mut accepted = delivery_state.clone();
        accepted.phase = CodexDeliveryPhase::Accepted;
        self.write_state(accepted)?;
        Ok(true)
    }

    /// Reconcile a pre-crash attempt against the typed history returned by `thread/resume` before
    /// the same client ID can be sent again.
    fn reconcile_resume(&mut self, message: &Value, state: &CodexControlState) -> Result<()> {
        if message.get("error").is_some() {
            return Ok(());
        }
        let Some(delivery_state) = self.state.as_ref() else {
            return Ok(());
        };
        if delivery_state.thread_id != state.thread_id()
            || delivery_state.phase == CodexDeliveryPhase::Accepted
        {
            return Ok(());
        }
        let turns = message
            .pointer("/result/thread/turns")
            .and_then(Value::as_array)
            .context(
                "Codex thread/resume response has no typed turn history for delivery recovery",
            )?;
        let accepted = turns.iter().any(|turn| {
            turn.get("items")
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("type").and_then(Value::as_str) == Some("userMessage")
                            && item.get("clientId").and_then(Value::as_str)
                                == Some(delivery_state.client_id.as_str())
                    })
                })
        });
        if accepted {
            let mut state = delivery_state.clone();
            state.phase = CodexDeliveryPhase::Accepted;
            self.write_state(state)
        } else {
            self.clear_state()
        }
    }
}

fn load_delivery_state(
    path: &Path,
    identity: &str,
    runtime_id: &str,
) -> Result<Option<CodexDeliveryState>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let state: CodexDeliveryState = serde_json::from_slice(&bytes)
        .with_context(|| format!("reading Codex delivery state {}", path.display()))?;
    anyhow::ensure!(
        state.schema == DELIVERY_STATE_SCHEMA,
        "Codex delivery state has unsupported schema '{}'",
        state.schema
    );
    anyhow::ensure!(
        state.agent == identity && state.runtime_id == runtime_id,
        "Codex delivery state belongs to a different runtime"
    );
    anyhow::ensure!(
        !state.runtime_incarnation.is_empty()
            && !state.thread_id.is_empty()
            && message::is_message_filename(&state.filename),
        "Codex delivery state has an invalid runtime binding or filename"
    );
    anyhow::ensure!(
        state.client_id
            == stable_client_user_message_id(identity, &state.thread_id, &state.filename),
        "Codex delivery state client ID does not match its binding"
    );
    Ok(Some(state))
}

fn remove_state_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            File::open(path.parent().context("state file has no parent")?)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
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
        let before = (self.subscribed, self.observed.clone());
        self.subscribed = true;
        self.observe_thread_status(status);
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
                self.observe_thread_status(status);
            }
            "thread/status/changed" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let status = required_string(message, "/params/status/type", method)?;
                self.observe_thread_status(status);
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
                self.observe_turn_completed(turn_id);
            }
            "item/started" | "item/completed" => {
                let thread_id = required_string(message, "/params/threadId", method)?;
                if thread_id != self.thread_id {
                    return Ok(false);
                }
                let item_type = required_string(message, "/params/item/type", method)?;
                let reason = match item_type {
                    "enteredReviewMode" => CodexHoldReason::Review,
                    "contextCompaction" => CodexHoldReason::Compaction,
                    _ => return Ok(false),
                };
                let turn_id = required_string(message, "/params/turnId", method)?;
                self.observe_non_steerable(turn_id, reason);
            }
            _ => return Ok(false),
        }
        Ok(self.observed != before)
    }

    fn observe_thread_status(&mut self, status: &str) {
        self.observed = match status {
            "idle" => CodexObservedState::Idle,
            "active" => match &self.observed {
                CodexObservedState::Active { .. }
                | CodexObservedState::Held {
                    reason:
                        CodexHoldReason::Review
                        | CodexHoldReason::Compaction
                        | CodexHoldReason::ConflictingTurn,
                    ..
                } => self.observed.clone(),
                _ => CodexObservedState::Held {
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
                reason: CodexHoldReason::SystemError,
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
                reason: reason @ (CodexHoldReason::Review | CodexHoldReason::Compaction),
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

    fn observe_turn_completed(&mut self, turn_id: &str) {
        self.observed = match &self.observed {
            CodexObservedState::Idle => CodexObservedState::Idle,
            CodexObservedState::Active { turn_id: current } if current == turn_id => {
                CodexObservedState::Idle
            }
            CodexObservedState::Held {
                reason: CodexHoldReason::Review | CodexHoldReason::Compaction,
                ..
            } => self.observed.clone(),
            CodexObservedState::AwaitingStatus
            | CodexObservedState::Held {
                reason: CodexHoldReason::ActiveWithoutTurn,
                ..
            } => CodexObservedState::Idle,
            CodexObservedState::Held {
                reason: CodexHoldReason::ConflictingTurn,
                ..
            } => self.observed.clone(),
            _ => CodexObservedState::Held {
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
                    CodexHoldReason::Review | CodexHoldReason::Compaction
                ) =>
            {
                self.observed.clone()
            }
            _ if matches!(
                reason,
                CodexHoldReason::Review | CodexHoldReason::Compaction
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
    ensure_supported_version(&codex_argv[0])?;
    let delivery = CodexDeliveryConfig::resolve(catalog_root, &identity)?;

    let state_dir = state_dir(catalog_root, &identity);
    secure_dir(&state_dir)?;
    let _owner_lock = acquire_owner_lock(&state_dir)?;
    let binding_path = state_dir.join("binding.json");
    let resume_thread = load_resume_thread(&binding_path, &identity, &runtime_id)?;

    let socket_path = socket_path(catalog_root, &identity)?;
    let socket_dir = socket_path
        .parent()
        .context("Codex app-server socket has no parent")?;
    secure_dir(socket_dir)?;
    match fs::symlink_metadata(&socket_path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Codex app-server path already exists and is not a socket: {}",
                socket_path.display()
            );
            match UnixStream::connect(&socket_path) {
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
                    fs::remove_file(&socket_path).with_context(|| {
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

    // Publish a new incarnation only after this process holds the owner lock and has proved that no
    // older daemon is live. A rejected second owner must not invalidate the first owner's binding.
    let runtime = CodexRuntime::fresh(identity, runtime_id)?;
    atomic_json(&state_dir.join("runtime.json"), &runtime)?;

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(state_dir.join("app-server.log"))?;
    let endpoint = format!("unix://{}", socket_path.display());
    let mut server = Command::new(&codex_argv[0])
        .args(["app-server", "--listen", &endpoint])
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .with_context(|| format!("starting {} app-server", codex_argv[0]))?;

    let result = run_connected(
        &mut server,
        &socket_path,
        &endpoint,
        &runtime,
        &codex_argv,
        resume_thread.as_deref(),
        delivery,
    );
    terminate_child(&mut server);
    let _ = fs::remove_file(&socket_path);
    result
}

fn run_connected(
    server: &mut Child,
    socket_path: &Path,
    endpoint: &str,
    runtime: &CodexRuntime,
    codex_argv: &[String],
    resume_thread: Option<&str>,
    delivery: CodexDeliveryConfig,
) -> Result<()> {
    let state_dir = state_dir(&delivery.catalog_root, &delivery.identity);
    let tui_args = controlled_tui_args(endpoint, &codex_argv[1..], resume_thread)?;
    let expected_resume =
        expected_resume_thread(&codex_argv[1..], resume_thread)?.map(str::to_owned);
    let control = connect_control(server, socket_path, STARTUP_TIMEOUT)?;
    let shutdown = control.try_clone()?;
    let websocket = initialize_control(control)?;
    let (events_tx, events_rx) = mpsc::channel();
    let binding_path = state_dir.join("binding.json");
    let control_state_path = state_dir.join("control-state.json");
    let runtime_for_reader = runtime.clone();
    let event_thread = thread::spawn(move || {
        pump_control(
            websocket,
            &binding_path,
            &control_state_path,
            &runtime_for_reader,
            expected_resume.as_deref(),
            Some(delivery),
            events_tx,
        )
    });

    // The initialized observer is already reading before this child can issue thread/start or
    // thread/resume. Insert the remote endpoint as a global Codex option and preserve every authored
    // argument after the provider executable.
    let mut tui_command = Command::new(&codex_argv[0]);
    tui_command.args(tui_args);
    let mut tui = tui_command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("starting controlled {} TUI", codex_argv[0]))?;

    let result = wait_for_binding(&mut tui, &events_rx, STARTUP_TIMEOUT)
        .and_then(|_| monitor_bound_tui(&mut tui, &events_rx));
    if result.is_err() {
        terminate_child(&mut tui);
    }
    let _ = shutdown.shutdown(Shutdown::Both);
    let _ = event_thread.join();
    result
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
    let delimiter = authored_args.iter().position(|arg| arg == "--");
    let mut index = 0;
    while index < authored_args.len() {
        let argument = authored_args[index].as_str();
        if argument == "--" {
            return Ok(Some(index));
        }
        if !argument.starts_with('-') || argument == "-" {
            return if matches!(argument, "resume" | "fork") {
                Ok(None)
            } else {
                Ok(Some(index))
            };
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
            return Ok(Some(boundary));
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
            "cannot automatically resume through unknown Codex option '{argument}'"
        );
        index += 1;
    }
    Ok(Some(authored_args.len()))
}

fn connect_control(
    server: &mut Child,
    socket_path: &Path,
    timeout: Duration,
) -> Result<UnixStream> {
    let deadline = Instant::now() + timeout;
    loop {
        match UnixStream::connect(socket_path) {
            Ok(stream) => return Ok(stream),
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

fn initialize_control(stream: UnixStream) -> Result<WebSocket<UnixStream>> {
    stream.set_read_timeout(Some(STARTUP_TIMEOUT))?;
    let (mut websocket, response) = tungstenite::client("ws://localhost/", stream)
        .map_err(|error| anyhow::anyhow!("Codex WebSocket handshake failed: {error}"))?;
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

    loop {
        let message = read_json_message(&mut websocket)?
            .context("Codex app-server closed the control connection during initialize")?;
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
    write_json_message(
        &mut websocket,
        &json!({ "method": "initialized", "params": {} }),
    )?;
    websocket.get_ref().set_read_timeout(None)?;
    Ok(websocket)
}

#[derive(Debug)]
enum ControlEvent {
    Bound,
    Observed,
    Closed,
    Failed(String),
}

fn pump_control(
    mut websocket: WebSocket<UnixStream>,
    binding_path: &Path,
    control_state_path: &Path,
    runtime: &CodexRuntime,
    expected_resume: Option<&str>,
    delivery: Option<CodexDeliveryConfig>,
    events: Sender<ControlEvent>,
) {
    let result = (|| -> Result<()> {
        let mut control_state: Option<CodexControlState> = None;
        let mut subscription_pending = false;
        let delivery_state_path = control_state_path.with_file_name("delivery-state.json");
        let mut delivery = delivery
            .map(|config| {
                CodexInboxDelivery::new(config, delivery_state_path.clone(), runtime.clone())
            })
            .transpose()?;
        websocket.get_ref().set_read_timeout(Some(CONTROL_POLL))?;
        loop {
            let message = match poll_json_message(&mut websocket)? {
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
                    write_json_message(&mut websocket, &request)?;
                }
                continue;
            };
            if control_state.is_none() {
                let Some(thread_id) = binding_candidate(&message, expected_resume)? else {
                    continue;
                };
                {
                    atomic_json(
                        binding_path,
                        &CodexThreadBinding::new(runtime, thread_id.to_string()),
                    )?;
                    let mut bound = CodexControlState::new(runtime, thread_id.to_string());
                    // A fresh control client that observes the owning TUI's `thread/started`
                    // notification already receives that thread's broadcasts. Before its first
                    // turn there is no rollout for `thread/resume` to load. Saved bindings still
                    // require resume so their typed history can be reconciled before delivery.
                    bound.subscribed = expected_resume.is_none()
                        && message.get("method").and_then(Value::as_str) == Some("thread/started");
                    control_state = Some(bound);
                    atomic_json(
                        control_state_path,
                        control_state
                            .as_ref()
                            .context("Codex control state is unbound")?,
                    )?;
                    let _ = events.send(ControlEvent::Bound);
                }
            }

            let state = control_state
                .as_mut()
                .context("Codex control state is unbound")?;
            let delivery_response = match delivery.as_mut() {
                Some(delivery) => {
                    delivery.accept_response(&message, &state.observed)?
                        || delivery.accept_typed_receipt(&message, state)?
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
                match state.accept_subscription(&message)? {
                    SubscriptionAcceptance::Accepted { changed } => {
                        if let Some(delivery) = delivery.as_mut() {
                            delivery.reconcile_resume(&message, state)?;
                        }
                        changed
                    }
                    SubscriptionAcceptance::Deferred => false,
                }
            } else {
                state.observe(&message)?
            };
            if changed {
                atomic_json(control_state_path, state)?;
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
                )?;
                subscription_pending = true;
            }
            if let Some(delivery) = delivery.as_mut()
                && let Some(request) = delivery.maybe_request(state)?
            {
                write_json_message(&mut websocket, &request)?;
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

fn binding_candidate<'a>(
    message: &'a Value,
    expected_resume: Option<&str>,
) -> Result<Option<&'a str>> {
    match message.get("method").and_then(Value::as_str) {
        Some("thread/started") => {
            let thread_id = required_string(message, "/params/thread/id", "thread/started")?;
            Ok(expected_resume
                .is_none_or(|expected| expected == thread_id)
                .then_some(thread_id))
        }
        Some("thread/status/changed") if expected_resume.is_some() => {
            let thread_id = required_string(message, "/params/threadId", "thread/status/changed")?;
            let status = required_string(message, "/params/status/type", "thread/status/changed")?;
            Ok(
                (Some(thread_id) == expected_resume && matches!(status, "idle" | "active"))
                    .then_some(thread_id),
            )
        }
        _ => Ok(None),
    }
}

fn wait_for_binding(
    tui: &mut Child,
    events: &Receiver<ControlEvent>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
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
            Ok(ControlEvent::Bound) => return Ok(()),
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

fn monitor_bound_tui(tui: &mut Child, events: &Receiver<ControlEvent>) -> Result<()> {
    loop {
        if let Some(status) = tui.try_wait()? {
            return completed_tui(status);
        }
        match events.recv_timeout(CONTROL_POLL) {
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

fn ensure_supported_version(codex: &str) -> Result<()> {
    let output = Command::new(codex)
        .arg("--version")
        .output()
        .with_context(|| format!("reading Codex version from {codex}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{codex} --version failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let actual = String::from_utf8(output.stdout)
        .context("Codex version output is not UTF-8")?
        .trim()
        .to_string();
    anyhow::ensure!(
        SUPPORTED_CODEX_CLI_VERSIONS.contains(&actual.as_str()),
        "unsupported Codex app-server protocol version '{actual}' (expected one of: {})",
        SUPPORTED_CODEX_CLI_VERSIONS.join(", ")
    );
    Ok(())
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

fn read_json_message(websocket: &mut WebSocket<UnixStream>) -> Result<Option<Value>> {
    loop {
        let message = match websocket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Ok(None);
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
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;

    #[test]
    fn protocol_version_gate_accepts_only_the_exact_allowlist() {
        let tmp = tempfile::tempdir().unwrap();
        let write_version = |name: &str, version: &str| {
            let path = tmp.path().join(name);
            fs::write(&path, format!("#!/bin/sh\nprintf '%s\\n' '{version}'\n")).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            path
        };
        for (name, version) in [
            ("codex-0145", "codex-cli 0.145.0"),
            ("codex-0146", "codex-cli 0.146.0"),
        ] {
            ensure_supported_version(write_version(name, version).to_str().unwrap()).unwrap();
        }
        let error = ensure_supported_version(
            write_version("codex-0147", "codex-cli 0.147.0")
                .to_str()
                .unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("codex-cli 0.147.0"));
        assert!(
            error
                .to_string()
                .contains("codex-cli 0.145.0, codex-cli 0.146.0")
        );
    }

    fn delivery_config(root: &Path) -> CodexDeliveryConfig {
        let agent_dir = root.join("agents/h/worker");
        CodexDeliveryConfig {
            catalog_root: root.to_path_buf(),
            inbox: message::inbox_dir(&agent_dir),
            agent_dir,
            identity: "h.worker".into(),
            this_host: "h".into(),
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
            root.join("state/delivery-state.json"),
            CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap(),
        )
        .unwrap()
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
        delivery.next_refresh = Instant::now();
        assert_eq!(
            delivery
                .maybe_request(&subscribed_state(CodexObservedState::Idle))
                .unwrap(),
            None
        );
        assert_eq!(message::list_inbox(&config.inbox).unwrap().len(), 1);
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
            delivery.state.as_ref().unwrap().phase,
            CodexDeliveryPhase::Attempted,
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
            delivery.state.as_ref().unwrap().phase,
            CodexDeliveryPhase::Attempted,
            "JSON success is not typed acceptance"
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
            load_delivery_state(&state_path, "h.worker", "h.worker")
                .unwrap()
                .unwrap()
                .phase,
            CodexDeliveryPhase::Accepted
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
        replacement.next_refresh = Instant::now();
        assert_eq!(replacement.maybe_request(&idle).unwrap(), None);
        assert!(
            !state_path.exists(),
            "archive precedence clears the receipt"
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
            recovered.state.as_ref().unwrap().phase,
            CodexDeliveryPhase::Accepted
        );
        assert_eq!(recovered.maybe_request(&idle).unwrap(), None);
        assert!(config.inbox.join(&filename).is_file());

        // An authoritative resumed history without the client ID proves that the pre-send record
        // did not reach typed acceptance. Only then may the same stable ID be retried.
        recovered.state.as_mut().unwrap().phase = CodexDeliveryPhase::Attempted;
        atomic_json(
            &tmp.path().join("state/delivery-state.json"),
            recovered.state.as_ref().unwrap(),
        )
        .unwrap();
        recovered
            .reconcile_resume(
                &json!({
                    "id": CONTROL_SUBSCRIBE_REQUEST_ID,
                    "result": { "thread": { "id": "thread-main", "turns": [] } }
                }),
                &idle,
            )
            .unwrap();
        assert!(recovered.state.is_none());
        let retry = recovered.maybe_request(&idle).unwrap().unwrap();
        assert_eq!(retry["params"]["clientUserMessageId"], client_id);
    }

    #[test]
    fn malformed_delivery_state_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let config = delivery_config(tmp.path());
        let state_path = tmp.path().join("state/delivery-state.json");
        atomic_json(
            &state_path,
            &json!({
                "schema": DELIVERY_STATE_SCHEMA,
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
        let error = match CodexInboxDelivery::new(
            config,
            state_path,
            CodexRuntime::fresh("h.worker".into(), "h.worker".into()).unwrap(),
        ) {
            Ok(_) => panic!("accepted malformed delivery state"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("client ID does not match"));
    }

    #[test]
    fn subscribed_control_pump_delivers_a_typed_reference_to_the_real_fifo_head() {
        let tmp = tempfile::tempdir().unwrap();
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
                .set_read_timeout(Some(Duration::from_secs(2)))
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
        let websocket = initialize_control(stream).unwrap();
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
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();
        assert!(delivery_config(tmp.path()).inbox.join(filename).is_file());
        assert_eq!(
            load_delivery_state(
                &tmp.path().join("state/delivery-state.json"),
                "h.worker",
                "h.worker",
            )
            .unwrap()
            .unwrap()
            .phase,
            CodexDeliveryPhase::Accepted
        );
    }

    #[test]
    fn subscribed_control_pump_reconciles_an_ambiguous_attempt_without_replay() {
        let tmp = tempfile::tempdir().unwrap();
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
        atomic_json(
            &delivery_state_path,
            &CodexDeliveryState::attempted(
                &prior_runtime,
                "thread-main".into(),
                filename.clone(),
                client_id.clone(),
            ),
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
        let websocket = initialize_control(stream).unwrap();
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
                Some("thread-main"),
                Some(config),
                tx,
            )
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ControlEvent::Bound
        ));
        server.join().unwrap();
        let _ = shutdown.shutdown(Shutdown::Both);
        pump.join().unwrap();

        let recovered = load_delivery_state(&delivery_state_path, "h.worker", "h.worker")
            .unwrap()
            .unwrap();
        assert_eq!(recovered.phase, CodexDeliveryPhase::Accepted);
        assert_eq!(recovered.client_id, client_id);
        assert!(delivery_config(tmp.path()).inbox.join(filename).is_file());
    }

    #[test]
    fn control_initializes_before_recording_the_first_thread_only() {
        let tmp = tempfile::tempdir().unwrap();
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
        let websocket = initialize_control(stream).unwrap();
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
    fn a_successfully_loaded_expected_resume_is_bound_to_the_new_incarnation() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
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
            write_json_message(
                &mut websocket,
                &json!({
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": "thread-prior",
                        "status": { "type": "idle" }
                    }
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
        let websocket = initialize_control(stream).unwrap();
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
                Some("thread-prior"),
                None,
                tx,
            )
        });
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
    fn runtime_owner_lock_is_nonblocking_and_released_on_close() {
        let tmp = tempfile::tempdir().unwrap();
        let first = acquire_owner_lock(tmp.path()).unwrap();
        let error = acquire_owner_lock(tmp.path()).unwrap_err();
        assert!(error.to_string().contains("already has an owner"));
        drop(first);
        acquire_owner_lock(tmp.path()).unwrap();
    }
}
