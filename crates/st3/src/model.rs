use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug)]
pub struct St3Error {
    pub code: &'static str,
    pub message: String,
    pub details: serde_json::Map<String, Value>,
}

impl St3Error {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: serde_json::Map::new(),
        }
    }

    pub fn with_detail(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.details.insert(name.into(), value.into());
        self
    }
}

impl fmt::Display for St3Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for St3Error {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiResponse<T> {
    pub api_version: String,
    pub request_id: String,
    pub snapshot_host: String,
    pub store_index: u64,
    pub value: T,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiErrorResponse {
    pub api_version: String,
    pub request_id: String,
    pub snapshot_host: String,
    pub store_index: u64,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemberKind {
    Agent,
    Exec,
    Pty,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestartType {
    #[default]
    Always,
    OnFailure,
    Never,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemberLifecycle {
    #[default]
    Service,
    AdoptOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum LaunchSpec {
    Shell(String),
    Argv(Vec<String>),
}

impl From<&LaunchSpec> for st_runtime::Launch {
    fn from(value: &LaunchSpec) -> Self {
        match value {
            LaunchSpec::Shell(source) => Self::Shell(source.clone()),
            LaunchSpec::Argv(argv) => Self::Argv(argv.clone()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestartIntensity {
    pub attempts: u32,
    pub interval_ms: u64,
    pub delay_ms: u64,
    pub mode: String,
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self {
            attempts: 3,
            interval_ms: 60_000,
            delay_ms: 0,
            mode: "delay".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemberSpec {
    pub kind: MemberKind,
    pub host: String,
    pub runtime_id: String,
    pub workspace: String,
    pub cwd: String,
    pub terminal: bool,
    pub launch: LaunchSpec,
    pub environment: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub display_name: Option<String>,
    pub lifecycle: MemberLifecycle,
    pub restart: RestartType,
    pub restart_intensity: RestartIntensity,
    pub shutdown_timeout_ms: u64,
    pub driver: Option<String>,
    pub supervisor: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DesiredSubject {
    pub subject: String,
    pub kind: String,
    pub desired: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<CheckpointActivation>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlanState {
    Draft,
    Ready,
    Retired,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RevisionCutover {
    #[default]
    RestartActive,
    WhenIdle,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "dependency", rename_all = "kebab-case")]
pub enum DependencySpec {
    Step { step: String, state: String },
    Predicate { gate: GateSpec },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BaselineSpec {
    pub name: String,
    pub gates: Vec<GateSpec>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProductSpec {
    pub subject: String,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "source", rename_all = "kebab-case")]
pub enum UsedPlanSpec {
    Revision { plan: String, revision: String },
    StepOutput { step: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetrySpec {
    pub attempts: u32,
    pub backoff_ms: u64,
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            attempts: 1,
            backoff_ms: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StepSpec {
    pub id: String,
    pub path: String,
    pub title: Option<String>,
    #[serde(default)]
    pub goals: Vec<String>,
    pub timeout_ms: Option<u64>,
    pub retry: RetrySpec,
    pub finally: bool,
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub revision_owners: Vec<String>,
    #[serde(default)]
    pub revisions_human_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_reviewer: Option<String>,
    pub dependencies: Vec<DependencySpec>,
    #[serde(default)]
    pub baselines: Vec<BaselineSpec>,
    #[serde(default)]
    pub documents: Vec<String>,
    pub subgraph_kdl: Option<String>,
    pub products: Vec<ProductSpec>,
    #[serde(default)]
    pub produces_plan: Option<String>,
    #[serde(default)]
    pub uses_plan: Option<UsedPlanSpec>,
    pub gates: Vec<GateSpec>,
    pub nested_plan: Option<Box<PlanSpec>>,
    pub definition_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PlanSpec {
    pub id: String,
    pub subject: String,
    pub state: PlanState,
    pub revision: String,
    pub scope_template: Option<String>,
    #[serde(default)]
    pub revision_owners: Vec<String>,
    #[serde(default)]
    pub revisions_human_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_reviewer: Option<String>,
    #[serde(default)]
    pub revision_cutover: RevisionCutover,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subgraph_kdl: Option<String>,
    pub goals: Vec<String>,
    #[serde(default)]
    pub baselines: Vec<BaselineSpec>,
    #[serde(default)]
    pub products: Vec<ProductSpec>,
    #[serde(default)]
    pub gates: Vec<GateSpec>,
    pub steps: BTreeMap<String, StepSpec>,
    pub display_order: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointActivation {
    pub sequence: String,
    pub ordinal: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CheckpointSpec {
    pub subject: String,
    pub sequence: String,
    pub name: String,
    pub ordinal: u32,
    pub gates: Vec<GateSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LinkSpec {
    pub from: String,
    pub to: String,
    pub required: bool,
    pub on_unreachable: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MessageTemplate {
    pub from: String,
    pub to: String,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UnderSpec {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduleSpec {
    pub stopped: bool,
    pub host: String,
    pub at_unix_ms: Option<i64>,
    pub every_ms: Option<u64>,
    pub anchor_unix_ms: Option<i64>,
    pub catch_up: String,
    pub max_catch_up: Option<u32>,
    pub message: Option<MessageTemplate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalControlSpec {
    pub name: String,
    pub driver: String,
    pub contains: Vec<String>,
    pub selected: Option<String>,
    pub keys: Vec<String>,
    pub max_inputs: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "predicate", rename_all = "kebab-case")]
pub enum GateSpec {
    Exists {
        name: String,
        subject: String,
    },
    Empty {
        name: String,
        subject: String,
    },
    Field {
        name: String,
        path: String,
        subject: String,
        operator: String,
        value: Value,
    },
    Has {
        name: String,
        subject: String,
        text: String,
    },
    Lacks {
        name: String,
        subject: String,
        text: String,
    },
    Deadline {
        name: String,
        duration_ms: u64,
    },
    Mechanical {
        name: String,
        command: String,
        host: String,
        workspace: String,
        environment: BTreeMap<String, String>,
        time_limit_ms: u64,
    },
    Llm {
        name: String,
        model: String,
        host: String,
        workspace: String,
        tools: Vec<String>,
        environment: BTreeMap<String, String>,
        token_budget: u64,
        time_limit_ms: u64,
        prompt: String,
    },
    Human {
        name: String,
        reviewer: String,
        #[serde(default)]
        question: Option<String>,
        #[serde(default)]
        review_targets: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizedIntent {
    pub schema: String,
    pub source_hash: String,
    pub subjects: BTreeMap<String, DesiredSubject>,
    pub checkpoints: Vec<CheckpointSpec>,
    #[serde(default)]
    pub plans: BTreeMap<String, PlanSpec>,
    pub document_refs: BTreeSet<String>,
    pub normalized: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IntentInput {
    pub kdl: String,
    #[serde(default)]
    pub source_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanRequest {
    pub intent: IntentInput,
    #[serde(default)]
    pub at_index: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubjectChange {
    pub subject: String,
    pub change: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_revision: Option<String>,
    pub new_revision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlannedAction {
    pub subject: String,
    pub action: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanResponse {
    pub store_index: u64,
    pub source_hash: String,
    pub normalized: Value,
    pub resolved_intent: IntentInput,
    pub changes: Vec<SubjectChange>,
    pub predicted_actions: Vec<PlannedAction>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub subject_tokens: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningSessionStartRequest {
    pub plan: String,
    #[serde(default)]
    pub run: Option<String>,
    pub request: Vec<u8>,
    pub workspace: String,
    #[serde(default)]
    pub requester: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningCandidateSubmitRequest {
    pub actor: String,
    pub markdown: Vec<u8>,
    pub kdl: Vec<u8>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningRevisionRequest {
    pub actor: String,
    pub feedback: Vec<u8>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningApprovalRequest {
    pub actor: String,
    pub preview_hash: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningCancelRequest {
    pub actor: String,
    #[serde(default)]
    pub reason: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningCandidateView {
    pub variant: String,
    pub revision: u32,
    pub markdown: String,
    pub kdl: String,
    pub plan: String,
    pub plan_revision: String,
    pub submitted_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningPreviewView {
    pub variant: String,
    pub hash: String,
    pub candidate_revision: u32,
    pub store_index: u64,
    pub graph: String,
    pub diff: String,
    pub plan: PlanResponse,
    pub created_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningSessionView {
    pub subject: String,
    pub id: String,
    pub plan: String,
    pub request: String,
    pub workspace: String,
    pub requester: String,
    pub planner: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_plan_run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<PlanningCandidateView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<PlanningPreviewView>,
    #[serde(default)]
    pub variants: Vec<PlanningVariantView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_revision: Option<String>,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningVariantView {
    pub name: String,
    pub candidate: PlanningCandidateView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<PlanningPreviewView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanningProposalRequest {
    pub actor: String,
    pub reason: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApplyRequest {
    pub intent: IntentInput,
    pub expected_subjects: BTreeMap<String, Vec<String>>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApplyResponse {
    pub changed: bool,
    pub store_index: u64,
    pub batch_id: Option<String>,
    pub claim_ids: Vec<String>,
    pub subject_tokens: BTreeMap<String, Vec<String>>,
    pub reconcile_subjects: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DocumentPutRequest {
    pub name: String,
    pub bytes: Vec<u8>,
    #[serde(default)]
    pub expected_document: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DocumentVersion {
    pub name: String,
    pub hash: String,
    pub size: u64,
    pub created_index: u64,
    pub latest: bool,
    pub binding_claim_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GateResultRequest {
    pub operation_capability: String,
    pub verdict: String,
    pub reason: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClaimInput {
    pub subject: String,
    pub kind: String,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub fields: BTreeMap<String, Value>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub expected_subject: Option<Option<String>>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClaimRecord {
    pub id: String,
    pub store_index: u64,
    pub batch_id: String,
    pub subject: String,
    pub kind: String,
    pub origin: String,
    pub actor: Option<String>,
    pub body: Value,
    pub predecessors: Vec<String>,
    pub accepted_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClaimsPage {
    pub claims: Vec<ClaimRecord>,
    pub next_cursor: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvalStatus {
    pub scope: String,
    pub plan_run: String,
    pub lifecycle: String,
    pub phase: String,
    pub active_steps: Vec<String>,
    pub verdict: Option<String>,
    pub cleanup: String,
    pub store_index: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatusResponse {
    pub store_index: u64,
    pub subjects: Vec<SubjectStatus>,
    pub pending_actions: Vec<PlannedAction>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubjectStatus {
    pub subject: String,
    pub kind: Option<String>,
    pub desired_token: Option<String>,
    pub desired_revision: Option<String>,
    pub desired: Option<Value>,
    pub actual: Option<Value>,
    pub conflicts: Vec<String>,
    pub claims: Vec<String>,
    pub scopes: Vec<String>,
    pub gap: Option<String>,
    pub reachability: String,
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub under: Vec<UnderSpec>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventRecord {
    pub store_index: u64,
    pub kind: String,
    pub subject: String,
    pub body: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReviewRequest {
    pub decision: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub expected_subject: Option<Option<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageSendRequest {
    pub idempotency_key: String,
    pub from: String,
    pub to: String,
    pub content: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageLifecycleRequest {
    pub lifecycle: String,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub expected_subject: Option<Option<String>>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageView {
    pub subject: String,
    pub from: String,
    pub to: String,
    pub content: String,
    pub status: String,
    pub title: Option<String>,
    pub in_reply_to: Option<String>,
    pub tags: Vec<String>,
    pub created_index: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuickAgentRequest {
    pub subject: String,
    pub worktree: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub arguments: Vec<String>,
    #[serde(default)]
    pub expected_subject: Vec<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuickAgentResponse {
    pub subject: String,
    pub runtime_id: String,
    pub event_cursor: u64,
    pub incarnation_id: Option<String>,
    pub ready: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attachment {
    pub subject: String,
    pub runtime_id: String,
    pub incarnation_id: Option<String>,
    pub capability: String,
    pub websocket_path: String,
    pub expires_at_unix_ms: u128,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttachRequest {
    #[serde(default = "default_terminal_rows")]
    pub rows: u16,
    #[serde(default = "default_terminal_columns")]
    pub columns: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextClearRequest {
    pub expected_incarnation: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionSignalRequest {
    pub expected_incarnation: String,
    pub signal: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionInputMode {
    Line,
    Raw,
    Key,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionInputRequest {
    pub expected_incarnation: String,
    pub mode: SessionInputMode,
    pub value: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionLogChunk {
    pub subject: String,
    pub runtime_id: String,
    pub generation_id: String,
    pub previous: bool,
    pub start_offset: u64,
    pub next_offset: u64,
    pub data_base64: String,
    pub eof: bool,
    pub status: String,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionScreen {
    pub subject: String,
    pub runtime_id: String,
    pub incarnation_id: String,
    pub screen: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: String,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DoctorReport {
    pub status: String,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionControlResponse {
    pub subject: String,
    pub request_claim_id: String,
    pub result_claim_id: String,
    pub event_cursor: u64,
}

fn default_terminal_rows() -> u16 {
    24
}

fn default_terminal_columns() -> u16 {
    80
}

#[derive(Clone, Debug)]
pub struct Capability {
    pub kind: String,
    pub subject: String,
    pub incarnation_id: Option<String>,
    pub expires_at_unix_ms: u128,
    pub used: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvalStartRequest {
    pub name: String,
    pub bundle_hash: String,
    pub bundle: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvalStartResponse {
    pub scope: String,
    pub event_cursor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_run: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanRunRequest {
    pub plan: String,
    #[serde(default)]
    pub revision: Option<String>,
    pub workspace: String,
    #[serde(default)]
    pub requester: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanRunView {
    pub subject: String,
    pub id: String,
    pub plan: String,
    pub generation: String,
    pub initial_revision: String,
    pub revision: String,
    pub root_revision: String,
    pub root_plan_run: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_step_run: Option<String>,
    pub workspace: String,
    pub requester: String,
    pub run_scope: Option<String>,
    pub mode: String,
    pub status: String,
    pub phase: String,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
    #[serde(default)]
    pub steps: Vec<StepRunView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StepRunView {
    pub subject: String,
    pub run: String,
    pub generation: String,
    pub step: String,
    pub definition_hash: String,
    pub status: String,
    pub attempt: u32,
    pub assignee: Option<String>,
    pub title: Option<String>,
    #[serde(default)]
    pub goals: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub under: Vec<UnderSpec>,
    pub worker_reported: bool,
    pub lease_owner: Option<String>,
    pub lease_incarnation: Option<String>,
    pub lease_expires_at_unix_ms: Option<u128>,
    pub blocked_reason: Option<String>,
    pub not_before_unix_ms: Option<u128>,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkRequest {
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub incarnation: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanRevisionRequest {
    pub intent: IntentInput,
    pub actor: String,
    pub reason: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunGenerationView {
    pub subject: String,
    pub id: String,
    pub run: String,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<String>,
    pub status: String,
    pub actor: String,
    pub reason: String,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
    #[serde(default)]
    pub steps: Vec<StepRunView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevisionProposalView {
    pub subject: String,
    pub id: String,
    pub run: String,
    pub source_generation: String,
    pub candidate_revision: String,
    pub actor: String,
    pub reason: String,
    pub status: String,
    pub cutover: RevisionCutover,
    #[serde(default)]
    pub compatible_steps: Vec<String>,
    #[serde(default)]
    pub reviewers: Vec<String>,
    #[serde(default)]
    pub approvals: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successor_generation: Option<String>,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevisionSubmissionView {
    pub status: String,
    pub plan_run: PlanRunView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<RevisionProposalView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevisionApprovalRequest {
    pub actor: String,
    pub preview_hash: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevisionCancelRequest {
    pub actor: String,
    #[serde(default)]
    pub reason: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanProductionRequest {
    pub intent: IntentInput,
    pub actor: String,
    #[serde(default)]
    pub incarnation: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanOutputView {
    pub step: String,
    pub plan: String,
    pub revision: String,
    pub claim_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicationBatch {
    pub peer: String,
    #[serde(default)]
    pub replica_heads: BTreeMap<String, u64>,
    pub batches: Vec<ReplicaBatch>,
    pub blobs: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReplicationQuery {
    #[serde(default)]
    pub replica_heads: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicaBatch {
    pub id: String,
    pub origin: String,
    pub replica_sequence: u64,
    pub previous_hash: Option<String>,
    pub hash: String,
    pub accepted_at_unix_ms: u128,
    pub claims: Vec<ClaimRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicationResponse {
    pub accepted_through: u64,
    pub missing_sequences: Vec<u64>,
    #[serde(default)]
    pub accepted_heads: BTreeMap<String, u64>,
    #[serde(default)]
    pub missing_ranges: Vec<ReplicaRange>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicaRange {
    pub origin: String,
    pub from: u64,
    pub through: u64,
}
