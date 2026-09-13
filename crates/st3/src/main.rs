use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{IsTerminal as _, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use base64::Engine as _;
use clap::{Args, CommandFactory as _, Parser, Subcommand, ValueEnum};
use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use st3::api::{AppState, router, serve_unix};
use st3::archive::archive_eval;
use st3::client::{Client, Endpoint};
use st3::config::{Config, PeerConfig};
use st3::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, ClaimInput, ClaimRecord, ClaimsPage,
    DoctorReport, DocumentPutRequest, DocumentVersion, EvalStartRequest, EvalStartResponse,
    EvalStatus, EventRecord, GateResultRequest, IntentInput, MessageLifecycleRequest,
    MessageSendRequest, MessageView, MissionOutputView, MissionProductionRequest, MissionRequest,
    MissionResponse, MissionRevisionRequest, MissionRunView, MissionState, PlanningApprovalRequest,
    PlanningCandidateSubmitRequest, PlanningProposalRequest, PlanningSessionView,
    QuickAgentResponse, ReplicaRecordView, ReplicationRepairRequest, ReplicationStatus,
    ResourceRefreshView, ResourceWatchView, ReviewRequest, RevisionApprovalRequest,
    RevisionCancelRequest, RevisionProposalView, RevisionSubmissionView, RunGenerationView,
    SessionControlResponse, SessionInputMode, SessionInputRequest, SessionLogChunk, SessionScreen,
    SessionSignalRequest, StatusResponse, StepRunView, WorkRequest,
};
use st3::reconcile::Reconciler;
use st3::store::Store;
use tokio::sync::{Notify, watch};
use walkdir::WalkDir;

mod presentation;

use presentation::{
    OutputStyle, follow_snapshot, mission_run_signature, render_generation, render_generations,
    render_mission_run, render_revision_proposal, render_step_run, render_work_list,
};

#[derive(Parser)]
#[command(name = "st3", version, about = "Claims-graph agent reconciler")]
struct Cli {
    #[arg(long, global = true)]
    endpoint: Option<String>,
    #[arg(long, global = true, hide = true)]
    catalog: Option<PathBuf>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the HTTP API, readers, peers, and reconciler.
    Up(UpArgs),
    /// Publish and attach a Claude agent.
    Claude(QuickArgs),
    /// Publish and attach a Codex agent.
    Codex(QuickArgs),
    /// Preview a new-format KDL intent.
    Preview(FileArgs),
    /// Create and review a durable Codex planning session.
    Planning {
        #[command(subcommand)]
        command: PlanningCommand,
    },
    /// Inspect a mission or its one active run.
    Mission {
        #[command(subcommand)]
        command: MissionViewCommand,
    },
    /// Publish one KDL file as an atomic graph upsert.
    Publish(PublishArgs),
    /// Apply all new-format KDL files in one directory tree.
    Import(ImportArgs),
    /// Publish one exec member and follow its log.
    Exec(ExecArgs),
    /// Read or follow one exec member log.
    Logs(LogsArgs),
    /// Inspect and control terminal members.
    Pty {
        #[command(subcommand)]
        command: PtyCommand,
    },
    /// Show one subject with its recent claims.
    Inspect(InspectArgs),
    /// Show claim history and optionally follow new events.
    Trace(TraceArgs),
    /// Wait until a graph condition is true.
    Wait(WaitArgs),
    /// Check the daemon and runtime dependencies.
    Doctor(DoctorArgs),
    /// Inspect and repair fleet replication.
    Replication {
        #[command(subcommand)]
        command: ReplicationCommand,
    },
    /// Manage the Linux or macOS st3 user service.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Install and inspect the Claude channel plugin used by st3.
    ClaudeChannel {
        #[command(subcommand)]
        command: ClaudeChannelCommand,
    },
    /// Store or read immutable documents.
    Doc {
        #[command(subcommand)]
        command: DocCommand,
    },
    /// Run one explicit eval.
    Eval(EvalArgs),
    /// Show one running eval as a live graph.
    Graph(GraphArgs),
    /// Show the current claims view.
    Status(StatusArgs),
    /// Show declared agents and their current graph state.
    Agents(AgentsArgs),
    /// Inspect and recover mission-owned runtimes.
    Runtime {
        #[command(subcommand)]
        command: RuntimeCommand,
    },
    /// Read or update durable agent context documents.
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Read or update observed resource bindings.
    Resource {
        #[command(subcommand)]
        command: ResourceCommand,
    },
    /// Publish one registered typed observation.
    Claim(ClaimArgs),
    /// Inspect the authoritative subject, resource, and claim schema.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Record a human review decision.
    Review {
        #[command(subcommand)]
        command: ReviewCommand,
    },
    /// Claim and update durable mission work.
    Work {
        #[command(subcommand)]
        command: WorkCommand,
    },
    /// Send and receive Small Talk graph messages.
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    /// Record a running gate result.
    GateResult(GateResultArgs),
    /// Generate one shell completion script.
    Completions(CompletionsArgs),
    #[command(hide = true)]
    ReplicationWorker(ReplicationWorkerArgs),
    #[command(hide = true)]
    Driver(DriverArgs),
}

#[derive(Args)]
struct ReplicationWorkerArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    node: Option<String>,
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long)]
    socket: Option<PathBuf>,
    #[arg(long)]
    peer_listen: Option<String>,
    #[arg(long)]
    fleet_id: Option<String>,
    #[arg(long)]
    shared_secret_file: Option<PathBuf>,
    #[arg(long, value_parser = parse_peer)]
    peer: Vec<PeerConfig>,
}

#[derive(Args)]
struct UpArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    node: Option<String>,
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Use an existing PTY registry during an st2-to-st3 cutover.
    #[arg(long)]
    pty_root: Option<PathBuf>,
    #[arg(long)]
    socket: Option<PathBuf>,
    #[arg(long)]
    peer_listen: Option<String>,
    #[arg(long)]
    fleet_id: Option<String>,
    #[arg(long)]
    shared_secret_file: Option<PathBuf>,
    #[arg(long, value_parser = parse_peer)]
    peer: Vec<PeerConfig>,
}

#[derive(Args)]
struct QuickArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long, default_value = ".")]
    worktree: PathBuf,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    effort: Option<String>,
    /// Print the generated mission KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
}

#[derive(Args)]
struct FileArgs {
    file: Option<PathBuf>,
    #[arg(long, visible_alias = "at")]
    at_index: Option<u64>,
}

#[derive(Subcommand)]
enum PlanningCommand {
    Start(PlanningStartArgs),
    Show(PlanningSessionArgs),
    Preview(PlanningPreviewArgs),
    Submit(PlanningSubmitArgs),
    Revise(PlanningReviseArgs),
    Approve(PlanningApproveArgs),
    Cancel(PlanningCancelArgs),
    Compare(PlanningCompareArgs),
    Propose(PlanningProposeArgs),
}

#[derive(Subcommand)]
enum MissionViewCommand {
    Show(MissionShowArgs),
    /// Start one run from the current ready mission revision.
    Start(MissionRunStartArgs),
}

#[derive(Args)]
struct MissionShowArgs {
    mission_or_run: String,
    #[arg(long)]
    follow: bool,
}

#[derive(Args)]
struct MissionRunStartArgs {
    mission: String,
    #[arg(long)]
    id: Option<String>,
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    #[arg(long = "input", value_parser = parse_input)]
    inputs: Vec<(String, String)>,
    #[arg(long)]
    follow: bool,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    /// Print the exact mission-run KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct PlanningStartArgs {
    #[arg(long, required_unless_present = "run", conflicts_with = "run")]
    id: Option<String>,
    #[arg(long)]
    run: Option<String>,
    request: Option<PathBuf>,
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    #[arg(long = "as")]
    requester: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    effort: Option<String>,
    /// Print the planning-session KDL without storing the request or publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct PlanningSessionArgs {
    session: String,
}

#[derive(Args)]
struct PlanningPreviewArgs {
    session: String,
    #[arg(long)]
    variant: Option<String>,
}

#[derive(Args)]
struct PlanningSubmitArgs {
    session: String,
    #[arg(long, default_value = "default")]
    variant: String,
    #[arg(long)]
    markdown: PathBuf,
    #[arg(long)]
    kdl: PathBuf,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: String,
}

#[derive(Args)]
struct PlanningCompareArgs {
    session: String,
    left: String,
    right: String,
}

#[derive(Args)]
struct PlanningProposeArgs {
    session: String,
    variant: String,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(long)]
    reason: String,
}

#[derive(Args)]
struct PlanningReviseArgs {
    session: String,
    feedback: PathBuf,
    #[arg(long = "as")]
    actor: Option<String>,
    /// Print the feedback KDL without storing the feedback or publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct PlanningApproveArgs {
    session: String,
    preview_hash: String,
    #[arg(long = "as")]
    actor: Option<String>,
}

#[derive(Args)]
struct PlanningCancelArgs {
    session: String,
    #[arg(long = "as")]
    actor: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    /// Print the cancellation KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct PublishArgs {
    file: Option<PathBuf>,
    #[arg(long, visible_alias = "at")]
    at_index: Option<u64>,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: String,
}

#[derive(Args)]
struct ImportArgs {
    directory: PathBuf,
}

#[derive(Args)]
struct ExecArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long, default_value = "local")]
    host: String,
    #[arg(long)]
    cwd: Option<PathBuf>,
    #[arg(long = "env", value_parser = parse_env)]
    environment: Vec<(String, String)>,
    #[arg(long)]
    detach: bool,
    #[arg(long)]
    cancel_on_interrupt: bool,
    /// Print the generated mission KDL without publishing or running it.
    #[arg(long)]
    print_kdl: bool,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(last = true, required = true)]
    argv: Vec<String>,
}

#[derive(Args)]
struct LogsArgs {
    subject: String,
    #[arg(short = 'f', long)]
    follow: bool,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    previous: bool,
}

#[derive(Subcommand)]
enum PtyCommand {
    Ls,
    Attach(PtySubjectArgs),
    Peek(PtySubjectArgs),
    Send(PtySendArgs),
    Signal(PtySignalArgs),
    Ui,
}

#[derive(Args)]
struct PtySubjectArgs {
    subject: String,
}

#[derive(Args)]
struct PtySendArgs {
    subject: String,
    value: String,
    #[arg(long, conflicts_with = "key")]
    raw: bool,
    #[arg(long, conflicts_with = "raw")]
    key: bool,
}

#[derive(Args)]
struct PtySignalArgs {
    subject: String,
    #[arg(value_parser = ["interrupt", "hangup", "user-1", "user-2"])]
    signal: String,
}

#[derive(Args)]
struct InspectArgs {
    subject: String,
}

#[derive(Args)]
struct TraceArgs {
    subject: Option<String>,
    #[arg(long)]
    owner_run: Option<String>,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long)]
    after_index: Option<u64>,
    #[arg(short = 'f', long)]
    follow: bool,
}

#[derive(Args)]
struct WaitArgs {
    subject: String,
    #[arg(long = "for", default_value = "ready")]
    condition: String,
    #[arg(long, default_value = "10m")]
    timeout: String,
}

#[derive(Args)]
struct DoctorArgs {
    #[arg(long)]
    strict: bool,
}

#[derive(Subcommand)]
enum ServiceCommand {
    Install {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Status,
    Restart {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Stop every st3 runtime, erase all st3 state, and restart an empty daemon.
    Reset {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Uninstall,
}

#[derive(Subcommand)]
enum ReplicationCommand {
    /// Show fleet receipt, validation, projection, and peer health.
    Status,
    /// List invalid and unknown replicated records.
    Invalid {
        #[arg(long)]
        all: bool,
    },
    /// Show one replicated record diagnostic.
    Inspect { record: String },
    /// Compare local logical digests with one peer's last signed response.
    Diff { peer: String },
    /// Replace one invalid record with an admitted claim.
    Repair {
        record: String,
        #[arg(long = "with")]
        replacement_claim: String,
        #[arg(long)]
        reason: String,
        #[arg(long = "as", env = "ST_AGENT")]
        actor: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Subcommand)]
enum ClaudeChannelCommand {
    /// Install or update the user plugin and its machine approval policy.
    Install {
        #[arg(long)]
        no_policy: bool,
    },
    /// Verify the embedded files, registration, plugin, and machine policy.
    Status,
    /// Remove the plugin, marketplace, embedded files, and machine policy.
    Uninstall {
        #[arg(long)]
        keep_policy: bool,
    },
    #[command(hide = true)]
    InstallPolicy,
    #[command(hide = true)]
    UninstallPolicy,
}

#[derive(Subcommand)]
enum DocCommand {
    Put {
        file: PathBuf,
        #[arg(long = "as")]
        name: String,
    },
    Get {
        reference: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    List {
        name: Option<String>,
    },
}

#[derive(Args)]
struct EvalArgs {
    eval: PathBuf,
    #[arg(long = "input", value_parser = parse_input)]
    inputs: Vec<(String, String)>,
    /// Show one live graph screen with semantic state transitions.
    #[arg(long)]
    graph: bool,
    /// Print the resolved eval mission KDL without publishing or running it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct GraphArgs {
    mission_run: String,
}

#[derive(Args)]
struct StatusArgs {
    #[arg(env = "ST_AGENT")]
    subject: Option<String>,
    #[arg(long)]
    owner_run: Option<String>,
    #[arg(long, visible_alias = "at")]
    at_index: Option<u64>,
    #[arg(long, value_parser = ["available", "busy", "dnd", "offline"])]
    set: Option<String>,
}

#[derive(Args)]
struct AgentsArgs {
    #[arg(long)]
    status: Option<String>,
    #[arg(long)]
    enrich: bool,
}

#[derive(Subcommand)]
enum RuntimeCommand {
    /// List agent, exec, and PTY runtimes.
    Ls,
    /// Clear one runtime restart window with exact desired-state fencing.
    Reset {
        subject: String,
        #[arg(long)]
        reason: String,
        /// Print the reset KDL without publishing it.
        #[arg(long)]
        print_kdl: bool,
        #[arg(long = "as", env = "ST_AGENT")]
        actor: Option<String>,
    },
}

#[derive(Subcommand)]
enum ContextCommand {
    Read(ContextReadArgs),
    Write(ContextIdentityArgs),
    Append(ContextAppendArgs),
}

#[derive(Args)]
struct ContextIdentityArgs {
    #[arg(env = "ST_AGENT")]
    identity: Option<String>,
}

#[derive(Args)]
struct ContextReadArgs {
    #[arg(env = "ST_AGENT")]
    identity: Option<String>,
    #[arg(long)]
    decisions: bool,
    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct ContextAppendArgs {
    #[arg(env = "ST_AGENT")]
    identity: Option<String>,
    #[arg(long)]
    decision: String,
    #[arg(long)]
    why: String,
}

#[derive(Subcommand)]
enum ResourceCommand {
    Add(ResourceAddArgs),
    Ls(ResourceIdentityArgs),
    Read(ResourceReadArgs),
    Remove(ResourceReadArgs),
    /// Send a message when selected external resource facts change.
    Watch(ResourceWatchArgs),
    /// Stop one resource subscription.
    Unwatch(ResourceUnwatchArgs),
    /// Publish one named resource refresh request.
    Refresh {
        resource: String,
        #[arg(long, default_value = "30s")]
        timeout: String,
        /// Print the refresh KDL without publishing it.
        #[arg(long)]
        print_kdl: bool,
        #[arg(long = "as", env = "ST_AGENT")]
        actor: Option<String>,
    },
}

#[derive(Args)]
struct ResourceIdentityArgs {
    #[arg(env = "ST_AGENT")]
    identity: Option<String>,
}

#[derive(Args)]
struct ResourceAddArgs {
    url: String,
    #[arg(long)]
    title: Option<String>,
    #[arg(long = "tag", value_delimiter = ',')]
    tags: Vec<String>,
    #[arg(long)]
    relation: Option<String>,
    #[arg(long = "as", env = "ST_AGENT")]
    identity: Option<String>,
}

#[derive(Args)]
struct ResourceReadArgs {
    #[arg(num_args = 1..=2)]
    values: Vec<String>,
    #[arg(long = "as", env = "ST_AGENT")]
    identity: Option<String>,
}

#[derive(Args)]
struct ResourceWatchArgs {
    provider: String,
    locator: String,
    #[arg(long = "on", required = true)]
    fields: Vec<String>,
    #[arg(long = "to", alias = "as", env = "ST_AGENT")]
    target: Option<String>,
    /// Print the generated watch mission KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct ResourceUnwatchArgs {
    subscription: String,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    /// Print the cancellation KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct ClaimArgs {
    subject: String,
    kind: String,
    #[arg(long)]
    actor: Option<String>,
    #[arg(long = "field", value_parser = parse_field)]
    fields: Vec<(String, Value)>,
    #[arg(long)]
    evidence: Vec<String>,
    /// Return the same logical result when this graph-wide key is retried.
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Subcommand)]
enum SchemaCommand {
    /// List registered subject families.
    Subjects,
    /// List registered resource kinds.
    Resources,
    /// List claim kinds, optionally for one subject.
    Claims {
        #[arg(long)]
        subject: Option<String>,
    },
    /// Show one claim kind.
    Show { kind: String },
    /// Export the complete registry.
    Export,
}

#[derive(Subcommand)]
enum ReviewCommand {
    Approve(ReviewArgs),
    Reject(ReviewArgs),
}

#[derive(Subcommand)]
enum WorkCommand {
    Ls {
        #[arg(long = "as", env = "ST_AGENT")]
        actor: Option<String>,
        #[arg(long)]
        all: bool,
    },
    Show {
        subject: String,
    },
    Claim(WorkActionArgs),
    Renew(WorkActionArgs),
    Progress(WorkActionArgs),
    Complete(WorkActionArgs),
    Fail(WorkActionArgs),
    Release(WorkActionArgs),
    /// Publish the exact ready mission produced by one claimed step.
    PublishMission(WorkPublishMissionArgs),
    Revise(WorkReviseArgs),
    Revision {
        #[command(subcommand)]
        command: WorkRevisionCommand,
    },
}

#[derive(Subcommand)]
enum WorkRevisionCommand {
    Show {
        run: String,
    },
    Generations {
        run: String,
    },
    Generation {
        generation: String,
    },
    Approve {
        proposal: String,
        preview_hash: String,
        #[arg(long = "as", env = "ST_AGENT")]
        actor: Option<String>,
    },
    Cancel {
        proposal: String,
        #[arg(long = "as", env = "ST_AGENT")]
        actor: Option<String>,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Args)]
struct WorkActionArgs {
    subject: String,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(long, env = "ST3_INCARNATION")]
    incarnation: Option<String>,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    evidence: Vec<String>,
}

#[derive(Args)]
struct WorkReviseArgs {
    run: String,
    file: PathBuf,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(long)]
    reason: String,
    /// Print the revision KDL without publishing the candidate mission or revision.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct WorkPublishMissionArgs {
    subject: String,
    file: PathBuf,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(long, env = "ST3_INCARNATION")]
    incarnation: Option<String>,
}

#[derive(Subcommand)]
enum MessageCommand {
    Send(MessageSendArgs),
    Ls(MessageListArgs),
    Read(MessageReadArgs),
    Reply(MessageReplyArgs),
    Archive(MessageArchiveArgs),
    Thread(MessageReferenceArgs),
    /// Write a disposable mailbox tree for translated tools.
    Export {
        directory: PathBuf,
    },
}

#[derive(Args)]
struct MessageSendArgs {
    to: String,
    #[arg(short = 'm', long)]
    body: String,
    #[arg(long)]
    subject: Option<String>,
    #[arg(long)]
    in_reply_to: Option<String>,
    #[arg(long, value_delimiter = ',')]
    tags: Vec<String>,
    #[arg(
        long = "from",
        alias = "as",
        env = "ST_AGENT",
        default_value = "person/requester"
    )]
    from: String,
    /// Print the generated message mission KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct MessageListArgs {
    #[arg(env = "ST_AGENT")]
    identity: Option<String>,
    #[arg(long)]
    archive: bool,
    #[arg(long)]
    count: bool,
    #[arg(long = "from")]
    sender: Option<String>,
}

#[derive(Args)]
struct MessageReadArgs {
    #[arg(num_args = 1..=2)]
    values: Vec<String>,
    #[arg(long)]
    raw: bool,
    #[arg(long)]
    archive: bool,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
}

#[derive(Args)]
struct MessageReplyArgs {
    reference: String,
    #[arg(short = 'm', long)]
    body: String,
    #[arg(long)]
    subject: Option<String>,
    #[arg(
        long = "from",
        alias = "as",
        env = "ST_AGENT",
        default_value = "person/requester"
    )]
    from: String,
    /// Print the generated reply mission KDL without publishing it.
    #[arg(long)]
    print_kdl: bool,
}

#[derive(Args)]
struct MessageArchiveArgs {
    #[arg(num_args = 1..)]
    references: Vec<String>,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
}

#[derive(Args)]
struct MessageReferenceArgs {
    #[arg(num_args = 1..=2)]
    values: Vec<String>,
    #[arg(long = "as", env = "ST_AGENT")]
    actor: Option<String>,
    #[arg(long)]
    tree: bool,
}

#[derive(Args)]
struct ReviewArgs {
    resource: String,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    actor: Option<String>,
}

#[derive(Args)]
struct GateResultArgs {
    #[arg(value_parser = ["pass", "fail"])]
    verdict: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    evidence: Vec<String>,
    #[arg(long, env = "ST_GATE_CAPABILITY")]
    operation_capability: String,
}

#[derive(Args)]
struct DriverArgs {
    #[arg(value_parser = ["claude", "claude-mcp", "codex", "pi", "pi-channel", "omp", "opencode", "ding", "exec"])]
    driver: String,
    #[arg(long, env = "ST_AGENT")]
    subject: Option<String>,
    #[arg(long)]
    identity: Option<String>,
    #[arg(last = true)]
    argv: Vec<String>,
}

#[derive(Args)]
struct CompletionsArgs {
    #[arg(value_enum)]
    shell: CompletionShell,
}

#[derive(Clone, Copy, ValueEnum)]
enum CompletionShell {
    Bash,
    Zsh,
    Fish,
}

#[derive(Debug)]
struct CommandExit(u8);

impl std::fmt::Display for CommandExit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "the command selected exit status {}", self.0)
    }
}

impl std::error::Error for CommandExit {}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if let Some(exit) = error.downcast_ref::<CommandExit>() {
                return ExitCode::from(exit.0);
            }
            eprintln!("st3: {error:#}");
            let message = error.to_string();
            if message.contains("run `st3 up` first") || message.contains("connect to the st3 API")
            {
                ExitCode::from(5)
            } else if message.contains("stale-subject") {
                ExitCode::from(3)
            } else if message.contains("terminal status selected")
                || message.contains("wait timed out")
            {
                ExitCode::from(4)
            } else {
                ExitCode::from(2)
            }
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    if let Command::Up(args) = cli.command {
        return run_up(args).await;
    }
    if let Command::ReplicationWorker(args) = cli.command {
        let mut config = Config::load_unvalidated(args.config.as_deref())?;
        if let Some(value) = args.node {
            config.node = value;
        }
        if let Some(value) = args.state_dir {
            config.state_dir = value;
        }
        if let Some(value) = args.socket {
            config.socket = value;
        }
        if let Some(value) = args.peer_listen {
            config.peer_listen = Some(value);
        }
        if let Some(value) = args.fleet_id {
            config.fleet_id = Some(value);
        }
        if let Some(value) = args.shared_secret_file {
            config.shared_secret_file = Some(value);
        }
        if !args.peer.is_empty() {
            config.peers = args.peer;
        }
        return st3::peer::run_worker(config).await;
    }
    let config = Config::load_unvalidated(None)?;
    let endpoint = cli
        .endpoint
        .or_else(|| std::env::var("ST3_ENDPOINT").ok())
        .as_deref()
        .map(Endpoint::parse)
        .unwrap_or_else(|| Endpoint::Unix(config.socket.clone()));
    let client = Client::new(endpoint.clone());
    match cli.command {
        Command::Up(_) => unreachable!(),
        Command::ReplicationWorker(_) => unreachable!(),
        Command::Claude(args) => {
            run_quick(&client, endpoint, &config, args, "claude", cli.json).await
        }
        Command::Codex(args) => {
            run_quick(&client, endpoint, &config, args, "codex", cli.json).await
        }
        Command::Preview(args) => run_preview(&client, args, cli.json).await,
        Command::Planning { command } => run_planning(&client, command, cli.json).await,
        Command::Mission { command } => run_mission_view(&client, command, cli.json).await,
        Command::Publish(args) => publish_file(&client, args, cli.json).await,
        Command::Import(args) => run_import(&client, args, cli.json).await,
        Command::Exec(args) => run_exec(&client, args, cli.json).await,
        Command::Logs(args) => run_logs(&client, args, cli.json).await,
        Command::Pty { command } => run_pty(&client, endpoint, &config, command, cli.json).await,
        Command::Inspect(args) => run_inspect(&client, args, cli.json).await,
        Command::Trace(args) => run_trace(&client, args, cli.json).await,
        Command::Wait(args) => run_wait(&client, args, cli.json).await,
        Command::Doctor(args) => run_doctor(&client, args, cli.json).await,
        Command::Replication { command } => run_replication(&client, command, cli.json).await,
        Command::Service { command } => run_service(command),
        Command::ClaudeChannel { command } => run_claude_channel(command),
        Command::Doc { command } => run_doc(&client, command, cli.json).await,
        Command::Eval(args) => run_eval(&client, args, cli.json).await,
        Command::Graph(args) => run_graph(&client, args, cli.json).await,
        Command::Status(args) => run_status(&client, args, cli.json).await,
        Command::Agents(args) => run_agents(&client, args, cli.json).await,
        Command::Runtime { command } => run_runtime(&client, command, cli.json).await,
        Command::Context { command } => run_context(&client, command, cli.json).await,
        Command::Resource { command } => run_resource(&client, command, cli.json).await,
        Command::Claim(args) => run_claim(&client, args, cli.json).await,
        Command::Schema { command } => run_schema(&client, command, cli.json).await,
        Command::Review { command } => run_review(&client, command, cli.json).await,
        Command::Work { command } => run_work(&client, command, cli.json).await,
        Command::Message { command } => run_message(&client, command, cli.json).await,
        Command::GateResult(args) => run_gate_result(&client, args, cli.json).await,
        Command::Completions(args) => {
            let shell = match args.shell {
                CompletionShell::Bash => clap_complete::Shell::Bash,
                CompletionShell::Zsh => clap_complete::Shell::Zsh,
                CompletionShell::Fish => clap_complete::Shell::Fish,
            };
            clap_complete::generate(shell, &mut Cli::command(), "st3", &mut std::io::stdout());
            Ok(())
        }
        Command::Driver(args) => run_driver(&client, args, cli.catalog.as_deref()).await,
    }
}

async fn run_up(args: UpArgs) -> Result<()> {
    let mut config = Config::load_unvalidated(args.config.as_deref())?;
    if let Some(node) = args.node {
        config.node = node;
    }
    if let Some(state_dir) = args.state_dir {
        config.state_dir = state_dir;
    }
    if let Some(pty_root) = args.pty_root {
        config.pty_root = Some(pty_root);
    }
    if let Some(socket) = args.socket {
        config.socket = socket;
    }
    if let Some(peer_listen) = args.peer_listen {
        config.peer_listen = Some(peer_listen);
    }
    if let Some(fleet_id) = args.fleet_id {
        config.fleet_id = Some(fleet_id);
    }
    if let Some(shared_secret_file) = args.shared_secret_file {
        config.shared_secret_file = Some(shared_secret_file);
    }
    if !args.peer.is_empty() {
        config.peers = args.peer;
    }
    config.validate()?;
    fs::create_dir_all(&config.state_dir)?;
    let store = Arc::new(Store::open(
        &config.state_dir.join("claims.sqlite3"),
        &config.node,
    )?);
    if let Some(fleet_id) = &config.fleet_id {
        store.bind_fleet(fleet_id)?;
    }
    let admission = store.validate_replication_backlog()?;
    store.apply_replication_repairs()?;
    let projected = store.project_replication_backlog()?;
    if !projected {
        eprintln!(
            "st3: the replicated projection is stale; the daemon will use its last good graph"
        );
    }
    if admission.invalid != 0 || admission.unknown != 0 {
        eprintln!(
            "st3: replication has {} invalid and {} unknown records",
            admission.invalid, admission.unknown
        );
    }
    store.append_claim(&ClaimInput {
        subject: format!("daemon/{}", config.node),
        kind: "daemon.started".into(),
        actor: None,
        fields: BTreeMap::from([
            ("status".into(), Value::String("running".into())),
            ("pid".into(), Value::from(std::process::id())),
            (
                "version".into(),
                Value::String(env!("CARGO_PKG_VERSION").into()),
            ),
            (
                "schema".into(),
                Value::String(st3_schema::SCHEMA_NAME.into()),
            ),
            (
                "schema_digest".into(),
                Value::String(st3_schema::registry().digest()),
            ),
        ]),
        evidence: Vec::new(),
        expected_subject: None,
        idempotency_key: Some(format!(
            "daemon-start:{}:{}",
            config.node,
            std::process::id()
        )),
    })?;
    let notify = Arc::new(Notify::new());
    let (event_notify, _event_receiver) = watch::channel(0_u64);
    let pty_root = config
        .pty_root
        .clone()
        .unwrap_or_else(|| config.state_dir.join("pty"));
    let state = AppState {
        store: store.clone(),
        notify: notify.clone(),
        event_notify: event_notify.clone(),
        node: config.node.clone(),
        state_dir: config.state_dir.clone(),
        pty_root: pty_root.clone(),
        fleet_id: config.fleet_id.clone(),
        configured_peers: config.peers.iter().map(|peer| peer.name.clone()).collect(),
    };
    let reconciler = Arc::new(Reconciler::native(
        store.clone(),
        &config.state_dir,
        Some(&pty_root),
        config.node.clone(),
        config.socket.display().to_string(),
        notify.clone(),
        event_notify.clone(),
    ));
    tokio::spawn(reconciler.run());
    eprintln!("st3: local API listening at {}", config.socket.display());
    serve_unix(&config.socket, router(state)).await
}

async fn run_preview(client: &Client, args: FileArgs, json_output: bool) -> Result<()> {
    let (kdl, source_name) = read_intent(args.file.as_deref())?;
    let response: MissionResponse = client
        .post(
            "/v1/intent/mission",
            &MissionRequest {
                intent: IntentInput { kdl, source_name },
                at_index: args.at_index,
            },
        )
        .await?;
    print_mission(&response, json_output)
}

async fn run_planning(client: &Client, command: PlanningCommand, json_output: bool) -> Result<()> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let response = match command {
        PlanningCommand::Start(args) => {
            let (request, _) = read_intent(args.request.as_deref())?;
            anyhow::ensure!(
                !request.trim().is_empty(),
                "a planning request cannot be empty"
            );
            let workspace = fs::canonicalize(&args.workspace)
                .with_context(|| format!("resolve workspace {}", args.workspace.display()))?;
            let target = args.run.as_deref().map(|run| async {
                client
                    .get::<MissionRunView>(&format!(
                        "/v1/mission-runs/{}",
                        urlencoding::encode(run)
                    ))
                    .await
            });
            let target = match target {
                Some(target) => Some(target.await?),
                None => None,
            };
            let mission_id = target
                .as_ref()
                .map(|run| run.mission.trim_start_matches("mission/").to_owned())
                .or(args.id)
                .context("planning start needs --id or --run")?;
            let session_id = format!("planning/{mission_id}/{}", uuid::Uuid::now_v7().simple());
            let request_hash = hex::encode(Sha256::digest(request.as_bytes()));
            let request_name = format!("doc/planning/{session_id}/request");
            let request_reference = format!("{request_name}@{request_hash}");
            let requester = normalize_planning_requester(
                args.requester.as_deref().unwrap_or("person/requester"),
            )?;
            let kdl = planning_session_intent(
                &session_id,
                &mission_id,
                &request_reference,
                &workspace,
                &requester,
                args.model.as_deref(),
                args.effort.as_deref(),
                target.as_ref(),
            );
            if args.print_kdl {
                eprintln!(
                    "Store the request first: st3 doc put {} --as {}",
                    args.request
                        .as_deref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "REQUEST_FILE".into()),
                    request_name
                );
                print!("{kdl}");
                return Ok(());
            }
            put_document_bytes(client, request_name, request.into_bytes()).await?;
            publish_text(
                client,
                kdl,
                format!("st3 planning start {session_id}"),
                requester.clone(),
            )
            .await?;
            client
                .get::<PlanningSessionView>(&format!(
                    "/v1/planning-sessions/{}",
                    urlencoding::encode(&session_id)
                ))
                .await?
        }
        PlanningCommand::Show(args) => {
            client
                .get::<PlanningSessionView>(&format!(
                    "/v1/planning-sessions/{}",
                    urlencoding::encode(&args.session)
                ))
                .await?
        }
        PlanningCommand::Preview(args) => {
            let path = args.variant.as_deref().map_or_else(
                || {
                    format!(
                        "/v1/planning-sessions/{}/preview",
                        urlencoding::encode(&args.session)
                    )
                },
                |variant| {
                    format!(
                        "/v1/planning-sessions/{}/variants/{}/preview",
                        urlencoding::encode(&args.session),
                        urlencoding::encode(variant)
                    )
                },
            );
            client
                .post::<_, PlanningSessionView>(&path, &json!({}))
                .await?
        }
        PlanningCommand::Submit(args) => {
            client
                .post::<_, PlanningSessionView>(
                    &format!(
                        "/v1/planning-sessions/{}/variants/{}/submit",
                        urlencoding::encode(&args.session),
                        urlencoding::encode(&args.variant)
                    ),
                    &PlanningCandidateSubmitRequest {
                        actor: args.actor,
                        markdown: fs::read(&args.markdown).with_context(|| {
                            format!("read Markdown {}", args.markdown.display())
                        })?,
                        kdl: fs::read(&args.kdl)
                            .with_context(|| format!("read KDL {}", args.kdl.display()))?,
                        idempotency_key: format!("planning-submit:{nonce}"),
                    },
                )
                .await?
        }
        PlanningCommand::Revise(args) => {
            let actor =
                normalize_planning_requester(args.actor.as_deref().unwrap_or("person/requester"))?;
            let feedback = fs::read(&args.feedback)
                .with_context(|| format!("read feedback {}", args.feedback.display()))?;
            std::str::from_utf8(&feedback).context("planning feedback must be UTF-8 text")?;
            let hash = hex::encode(Sha256::digest(&feedback));
            let session = args
                .session
                .strip_prefix("planning-session/")
                .unwrap_or(&args.session);
            let document_name = format!("doc/planning/{session}/feedback/{hash}");
            let reference = format!("{document_name}@{hash}");
            let operation = format!("feedback-{}", uuid::Uuid::now_v7().simple());
            let kdl = planning_feedback_intent(session, &operation, &reference, "default");
            if args.print_kdl {
                eprintln!(
                    "Store the feedback first: st3 doc put {} --as {}",
                    args.feedback.display(),
                    document_name
                );
                print!("{kdl}");
                return Ok(());
            }
            put_document_bytes(client, document_name, feedback).await?;
            publish_text(client, kdl, format!("st3 planning revise {session}"), actor).await?;
            client
                .get::<PlanningSessionView>(&format!(
                    "/v1/planning-sessions/{}",
                    urlencoding::encode(session)
                ))
                .await?
        }
        PlanningCommand::Approve(args) => {
            client
                .post::<_, PlanningSessionView>(
                    &format!(
                        "/v1/planning-sessions/{}/approve",
                        urlencoding::encode(&args.session)
                    ),
                    &PlanningApprovalRequest {
                        actor: args.actor.unwrap_or_else(|| "person/requester".into()),
                        preview_hash: args.preview_hash,
                        idempotency_key: format!("planning-approve:{nonce}"),
                    },
                )
                .await?
        }
        PlanningCommand::Cancel(args) => {
            let actor =
                normalize_planning_requester(args.actor.as_deref().unwrap_or("person/requester"))?;
            let session = args
                .session
                .strip_prefix("planning-session/")
                .unwrap_or(&args.session);
            let operation = format!("cancel-{}", uuid::Uuid::now_v7().simple());
            let kdl = planning_cancellation_intent(
                session,
                &operation,
                args.reason
                    .as_deref()
                    .unwrap_or("the planning session was cancelled"),
            );
            if args.print_kdl {
                print!("{kdl}");
                return Ok(());
            }
            publish_text(client, kdl, format!("st3 planning cancel {session}"), actor).await?;
            client
                .get::<PlanningSessionView>(&format!(
                    "/v1/planning-sessions/{}",
                    urlencoding::encode(session)
                ))
                .await?
        }
        PlanningCommand::Compare(args) => {
            let response: Value = client
                .get(&format!(
                    "/v1/planning-sessions/{}/variants/{}/compare/{}",
                    urlencoding::encode(&args.session),
                    urlencoding::encode(&args.left),
                    urlencoding::encode(&args.right)
                ))
                .await?;
            return print_value(&response, json_output);
        }
        PlanningCommand::Propose(args) => {
            let actor = args
                .actor
                .context("a planning proposal needs --as or ST_AGENT")?;
            let response: RevisionSubmissionView = client
                .post(
                    &format!(
                        "/v1/planning-sessions/{}/variants/{}/propose",
                        urlencoding::encode(&args.session),
                        urlencoding::encode(&args.variant)
                    ),
                    &PlanningProposalRequest {
                        actor,
                        reason: args.reason,
                        idempotency_key: format!("planning-propose:{nonce}"),
                    },
                )
                .await?;
            return print_value(&response, json_output);
        }
    };
    if json_output {
        return print_value(&response, true);
    }
    println!("{}\t{}", response.status, response.subject);
    println!("Mission: mission/{}", response.mission);
    println!("Planner: {}", response.planner);
    if let Some(candidate) = &response.candidate {
        println!(
            "Candidate: {} ({})",
            candidate.revision, candidate.mission_revision
        );
    }
    if let Some(preview) = &response.preview {
        println!("Preview: {}", preview.hash);
        println!("\nGraph:\n{}", preview.graph);
        println!("\nDiff:\n{}", preview.diff);
        for warning in &preview.mission.warnings {
            println!("Warning: {warning}");
        }
        for blocker in &preview.mission.blockers {
            println!("Blocker: {blocker}");
        }
    }
    Ok(())
}

async fn run_mission_view(
    client: &Client,
    command: MissionViewCommand,
    json_output: bool,
) -> Result<()> {
    match command {
        MissionViewCommand::Show(args) => {
            let selected = args.mission_or_run;
            let run = if selected.starts_with("mission-run/") {
                client
                    .get::<MissionRunView>(&format!(
                        "/v1/mission-runs/{}",
                        urlencoding::encode(&selected)
                    ))
                    .await?
            } else {
                let runs: Vec<MissionRunView> = client
                    .get(&format!(
                        "/v1/mission-runs?mission={}",
                        urlencoding::encode(&selected)
                    ))
                    .await?;
                anyhow::ensure!(
                    runs.len() == 1,
                    "mission `{selected}` has {} active runs; use an exact mission run subject",
                    runs.len()
                );
                runs.into_iter().next().expect("one active run was checked")
            };
            if args.follow {
                return follow_mission_run(client, run, 0, json_output).await;
            }
            if json_output {
                return print_value(&run, true);
            }
            let runs = load_mission_run_tree(client, &run).await?;
            print!(
                "{}",
                render_mission_run(&run, &runs, OutputStyle::stdout(), current_unix_ms()?)
            );
            Ok(())
        }
        MissionViewCommand::Start(args) => start_mission_run(client, args, json_output).await,
    }
}

async fn publish_file(client: &Client, args: PublishArgs, json_output: bool) -> Result<()> {
    let (kdl, source_name) = read_intent(args.file.as_deref())?;
    let intent = IntentInput { kdl, source_name };
    let mission: MissionResponse = client
        .post(
            "/v1/intent/mission",
            &MissionRequest {
                intent: intent.clone(),
                at_index: args.at_index,
            },
        )
        .await?;
    anyhow::ensure!(
        mission.blockers.is_empty(),
        "{}",
        mission.blockers.join("; ")
    );
    let resolved_intent = mission.resolved_intent.clone();
    let idempotency_key = idempotency(&resolved_intent.kdl, &mission.subject_tokens);
    let response: ApplyResponse = client
        .post(
            "/v1/intent/apply",
            &ApplyRequest {
                intent: resolved_intent.clone(),
                expected_subjects: mission.subject_tokens,
                idempotency_key,
                actor: Some(args.actor),
            },
        )
        .await?;
    print_value(&response, json_output)
}

async fn start_mission_run(
    client: &Client,
    args: MissionRunStartArgs,
    json_output: bool,
) -> Result<()> {
    let mission_id = args
        .mission
        .strip_prefix("mission/")
        .unwrap_or(&args.mission);
    let mission: st3::model::MissionSpec = client
        .get(&format!("/v1/missions/{}", urlencoding::encode(mission_id)))
        .await?;
    anyhow::ensure!(
        mission.state == MissionState::Ready,
        "mission `mission/{mission_id}` is not ready"
    );
    let run_id = args
        .id
        .unwrap_or_else(|| format!("{mission_id}/{}", uuid::Uuid::now_v7().simple()));
    let run_id = run_id.strip_prefix("mission-run/").unwrap_or(&run_id);
    let workspace = args
        .workspace
        .canonicalize()
        .with_context(|| format!("resolve workspace {}", args.workspace.display()))?;
    let inputs = unique_pairs(args.inputs, "input")?;
    let actor = args.actor.unwrap_or_else(|| "person/requester".into());
    let requester = normalize_requester_subject(&actor);
    let kdl = mission_run_intent(
        run_id,
        mission_id,
        &mission.revision,
        &workspace,
        &requester,
        &inputs,
        "run",
    );
    if args.print_kdl {
        print!("{kdl}");
        return Ok(());
    }
    let response = publish_text(
        client,
        kdl,
        format!("st3 mission start {mission_id}"),
        actor,
    )
    .await?;
    let subject = format!("mission-run/{run_id}");
    let started: MissionRunView = client
        .get(&format!(
            "/v1/mission-runs/{}",
            urlencoding::encode(&subject)
        ))
        .await?;
    if !args.follow {
        return if json_output {
            print_value(
                &json!({"publication": response, "mission_run": started}),
                true,
            )
        } else {
            println!("{}", started.subject);
            Ok(())
        };
    }
    follow_mission_run(client, started, response.store_index, json_output).await
}

async fn follow_mission_run(
    client: &Client,
    mut run: MissionRunView,
    _cursor: u64,
    json_output: bool,
) -> Result<()> {
    let mut prior = String::new();
    let interactive = std::io::stdout().is_terminal();
    let _screen = if !json_output && interactive {
        Some(TerminalScreen::open()?)
    } else {
        None
    };
    let style = OutputStyle::stdout();
    loop {
        let runs = load_mission_run_tree(client, &run).await?;
        let summary = mission_run_signature(&runs)?;
        if summary != prior && !json_output {
            let frame = render_mission_run(&run, &runs, style, current_unix_ms()?);
            print!(
                "{}",
                follow_snapshot(&frame, interactive, !prior.is_empty())
            );
            std::io::stdout().flush()?;
            prior = summary;
        }
        match run.status.as_str() {
            status if mission_run_follow_succeeded(status) => {
                return if json_output {
                    print_value(&run, true)
                } else {
                    Ok(())
                };
            }
            "failed" | "cancelled" => {
                anyhow::bail!("mission run {} is {}", run.subject, run.status)
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        run = client
            .get(&format!(
                "/v1/mission-runs/{}",
                urlencoding::encode(&run.subject)
            ))
            .await?;
    }
}

async fn load_mission_run_tree(
    client: &Client,
    selected: &MissionRunView,
) -> Result<Vec<MissionRunView>> {
    let runs: Vec<MissionRunView> = client
        .get(&format!(
            "/v1/mission-runs?root={}",
            urlencoding::encode(&selected.root_mission_run)
        ))
        .await?;
    anyhow::ensure!(
        runs.iter().any(|run| run.subject == selected.subject),
        "mission run `{}` is absent from its root graph",
        selected.subject
    );
    Ok(runs)
}

fn mission_run_follow_succeeded(status: &str) -> bool {
    matches!(status, "completed" | "standing")
}

async fn run_import(client: &Client, args: ImportArgs, json_output: bool) -> Result<()> {
    let kdl = combine_kdl_tree(&args.directory)?;
    post_staged_documents(client, &args.directory, &kdl).await?;
    run_file_from_text(
        client,
        kdl,
        args.directory.display().to_string(),
        json_output,
    )
    .await
}

async fn run_exec(client: &Client, args: ExecArgs, json_output: bool) -> Result<()> {
    let name = args
        .name
        .unwrap_or_else(|| uuid::Uuid::now_v7().simple().to_string());
    let cwd = args.cwd.unwrap_or(std::env::current_dir()?);
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("resolve working directory {}", cwd.display()))?;
    let kdl = exec_intent(&name, &args.host, &cwd, &args.environment, &args.argv);
    if args.print_kdl {
        print!("{kdl}");
        return Ok(());
    }
    let actor = args.actor.context("st3 exec needs --as or ST_AGENT")?;
    let parsed = st3::parse_intent(&kdl, &args.host)?;
    let mission_id = format!("exec/{name}");
    let revision = parsed.missions[&mission_id].revision.clone();
    publish_text(
        client,
        kdl,
        format!("st3 exec {name} mission"),
        actor.clone(),
    )
    .await?;
    let run_id = format!("{mission_id}/{}", uuid::Uuid::now_v7().simple());
    let run_kdl = mission_run_intent(
        &run_id,
        &mission_id,
        &revision,
        &cwd,
        &normalize_requester_subject(&actor),
        &BTreeMap::new(),
        "run",
    );
    let applied = publish_text(
        client,
        run_kdl,
        format!("st3 exec {name} run"),
        actor.clone(),
    )
    .await?;
    let run: MissionRunView = client
        .get(&format!(
            "/v1/mission-runs/{}",
            urlencoding::encode(&format!("mission-run/{run_id}"))
        ))
        .await?;
    let subject = format!("exec/{}/{name}", run.id);
    if args.detach {
        if json_output {
            return print_value(
                &json!({
                "subject": subject,
                "store_index": applied.store_index,
                "detached": true,
                }),
                true,
            );
        }
        println!("{subject}");
        return Ok(());
    }
    #[cfg(unix)]
    let interrupt = {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        async move {
            signal.recv().await;
            Ok::<(), std::io::Error>(())
        }
    };
    #[cfg(not(unix))]
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    if !json_output {
        let mut stderr = std::io::stderr().lock();
        writeln!(stderr, "{}", subject)?;
        stderr.flush()?;
    }
    tokio::select! {
        result = wait_for_actual(client, &subject, applied.store_index) => result?,
        signal = &mut interrupt => {
            signal?;
            if args.cancel_on_interrupt {
                publish_text(client, cancel_run_intent(&run.subject), format!("st3 exec cancel {name}"), actor.clone()).await?;
            }
            return Err(CommandExit(130).into());
        }
    }
    let follow = follow_logs(client, &subject, false, true, true, !json_output);
    let final_chunk = tokio::select! {
        result = follow => result?,
        signal = &mut interrupt => {
            signal?;
            if args.cancel_on_interrupt {
                publish_text(client, cancel_run_intent(&run.subject), format!("st3 exec cancel {name}"), actor).await?;
            }
            return Err(CommandExit(130).into());
        }
    };
    let final_chunk = wait_for_exec_exit_status(client, &subject, final_chunk).await?;
    if json_output {
        print_value(&final_chunk, true)?;
    }
    if let Some(signal) = final_chunk.exit_signal {
        return Err(CommandExit((128_i32.saturating_add(signal)).clamp(1, 255) as u8).into());
    }
    if let Some(code) = final_chunk.exit_code
        && code != 0
    {
        return Err(CommandExit(code.clamp(1, 255) as u8).into());
    }
    Ok(())
}

async fn wait_for_exec_exit_status(
    client: &Client,
    subject: &str,
    mut chunk: SessionLogChunk,
) -> Result<SessionLogChunk> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while chunk.exit_code.is_none() && chunk.exit_signal.is_none() {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the exec exited before its status became available"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        chunk = client
            .get(&format!(
                "/v1/sessions/logs/{}?after={}&limit=1&previous=false&wait=false",
                urlencoding::encode(subject),
                u64::MAX
            ))
            .await?;
    }
    Ok(chunk)
}

fn exec_intent(
    name: &str,
    host: &str,
    cwd: &Path,
    environment: &[(String, String)],
    argv: &[String],
) -> String {
    let mut task = KdlNode::new("exec");
    task.entries_mut().push(KdlEntry::new(name));
    let mut body = KdlDocument::new();
    body.nodes_mut().push(kdl_node("host", [host]));
    let cwd = cwd.to_string_lossy().into_owned();
    body.nodes_mut().push(kdl_node("workspace", [cwd.as_str()]));
    body.nodes_mut().push(kdl_node("cwd", [cwd.as_str()]));
    body.nodes_mut()
        .push(kdl_node("argv", argv.iter().map(String::as_str)));
    if !environment.is_empty() {
        let mut environment_node = KdlNode::new("env");
        let mut environment_body = KdlDocument::new();
        for (name, value) in environment {
            environment_body
                .nodes_mut()
                .push(kdl_node(name, [value.as_str()]));
        }
        environment_node.set_children(environment_body);
        body.nodes_mut().push(environment_node);
    }
    body.nodes_mut().push(kdl_node("restart", ["never"]));
    task.set_children(body);

    let mut step = KdlNode::new("step");
    step.entries_mut().push(KdlEntry::new("execute"));
    let mut step_body = KdlDocument::new();
    step_body.nodes_mut().push(KdlNode::new("agentless"));
    step_body.nodes_mut().push(task);
    let mut gate = KdlNode::new("gate");
    gate.entries_mut().push(KdlEntry::new("the command exits"));
    let mut gate_body = KdlDocument::new();
    let subject = format!("exec/${{ST_MISSION_RUN}}/{name}");
    gate_body.nodes_mut().push(kdl_node(
        "field",
        ["status", subject.as_str(), "is", "exited"],
    ));
    gate.set_children(gate_body);
    step_body.nodes_mut().push(gate);
    step.set_children(step_body);
    let mut completion = KdlNode::new("completion");
    let mut completion_body = KdlDocument::new();
    completion_body
        .nodes_mut()
        .push(kdl_node("when", ["all-steps-exhausted"]));
    completion.set_children(completion_body);
    let mut mission = KdlNode::new("mission");
    mission
        .entries_mut()
        .push(KdlEntry::new(format!("exec/{name}")));
    mission
        .entries_mut()
        .push(KdlEntry::new_prop("state", "ready"));
    let mut mission_body = KdlDocument::new();
    mission_body
        .nodes_mut()
        .push(kdl_node("goal", ["Run the command to completion."]));
    mission_body.nodes_mut().push(step);
    mission_body.nodes_mut().push(completion);
    mission.set_children(mission_body);
    publication_document(mission)
}

fn cancel_run_intent(subject: &str) -> String {
    format!(
        "version 2\nmission-run {subject:?} {{ cancellation \"command-interrupted\" {{ reason \"the command was interrupted\" }} }}\n"
    )
}

fn mission_run_intent(
    run_id: &str,
    mission_id: &str,
    revision: &str,
    workspace: &Path,
    requester: &str,
    inputs: &BTreeMap<String, String>,
    mode: &str,
) -> String {
    let mut run = KdlNode::new("mission-run");
    run.entries_mut().push(KdlEntry::new(run_id));
    let mut body = KdlDocument::new();
    let exact_mission = format!("mission/{mission_id}@{revision}");
    body.nodes_mut()
        .push(kdl_node("mission", [exact_mission.as_str()]));
    body.nodes_mut().push(kdl_node(
        "workspace",
        [workspace.to_string_lossy().as_ref()],
    ));
    body.nodes_mut().push(kdl_node("requester", [requester]));
    if mode != "run" {
        body.nodes_mut().push(kdl_node("mode", [mode]));
    }
    for (name, value) in inputs {
        body.nodes_mut()
            .push(kdl_node("input", [name.as_str(), value.as_str()]));
    }
    run.set_children(body);
    publication_document(run)
}

fn normalize_requester_subject(actor: &str) -> String {
    if actor.starts_with("person/") || actor.starts_with("agent/") {
        actor.to_owned()
    } else {
        format!("person/{actor}")
    }
}

fn kdl_node<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> KdlNode {
    let mut node = KdlNode::new(name);
    node.entries_mut()
        .extend(values.into_iter().map(KdlEntry::new));
    node
}

fn publication_document(node: KdlNode) -> String {
    let mut document = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    document.nodes_mut().push(version);
    document.nodes_mut().push(node);
    document.autoformat();
    document.to_string()
}

fn publication_actor() -> Result<String> {
    std::env::var("ST_AGENT").context("publication needs --as or ST_AGENT")
}

async fn publish_text(
    client: &Client,
    kdl: String,
    source_name: String,
    actor: String,
) -> Result<ApplyResponse> {
    let intent = IntentInput {
        kdl,
        source_name: Some(source_name),
    };
    let mission: MissionResponse = client
        .post(
            "/v1/intent/mission",
            &MissionRequest {
                intent: intent.clone(),
                at_index: None,
            },
        )
        .await?;
    anyhow::ensure!(
        mission.blockers.is_empty(),
        "{}",
        mission.blockers.join("; ")
    );
    let resolved = mission.resolved_intent;
    client
        .post(
            "/v1/intent/apply",
            &ApplyRequest {
                idempotency_key: idempotency(&resolved.kdl, &mission.subject_tokens),
                intent: resolved,
                expected_subjects: mission.subject_tokens,
                actor: Some(actor),
            },
        )
        .await
}

async fn wait_for_actual(client: &Client, subject: &str, mut cursor: u64) -> Result<()> {
    loop {
        let status = status_for(client, subject).await?;
        if status
            .subjects
            .first()
            .and_then(|item| item.actual.as_ref())
            .is_some_and(|actual| {
                let fields = actual.get("fields").unwrap_or(actual);
                fields.get("runtime_id").and_then(Value::as_str).is_some()
                    && fields
                        .get("incarnation_id")
                        .and_then(Value::as_str)
                        .is_some()
                    && fields.get("terminal").and_then(Value::as_bool).is_some()
            })
        {
            return Ok(());
        }
        let events: Vec<EventRecord> = client
            .get(&format!(
                "/v1/events?after={cursor}&subject={}&wait=false",
                urlencoding::encode(subject)
            ))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
            if event.kind == "runtime.action.failed" {
                anyhow::bail!("{} failed: {}", subject, event.body);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_message_view(client: &Client, subject: &str, mut cursor: u64) -> Result<()> {
    loop {
        if read_message(client, subject).await.is_ok() {
            return Ok(());
        }
        let events: Vec<EventRecord> = client
            .get(&format!(
                "/v1/events?after={cursor}&subject={}&wait=false",
                urlencoding::encode(subject)
            ))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn run_logs(client: &Client, args: LogsArgs, json_output: bool) -> Result<()> {
    let chunk = follow_logs(
        client,
        &args.subject,
        args.previous,
        args.all,
        args.follow,
        !json_output,
    )
    .await?;
    if json_output {
        print_value(&chunk, true)?;
    }
    Ok(())
}

async fn follow_logs(
    client: &Client,
    subject: &str,
    previous: bool,
    all: bool,
    follow: bool,
    emit: bool,
) -> Result<SessionLogChunk> {
    let subject = normalize_member_subject(subject, "exec");
    let probe: SessionLogChunk = client
        .get(&format!(
            "/v1/sessions/logs/{}?after={}&limit=1&previous={previous}",
            urlencoding::encode(&subject),
            u64::MAX
        ))
        .await?;
    let mut offset = if all {
        0
    } else {
        probe.next_offset.saturating_sub(64 * 1024)
    };
    let generation = probe.generation_id.clone();
    loop {
        let chunk: SessionLogChunk = client
            .get(&format!(
                "/v1/sessions/logs/{}?after={offset}&limit={}&previous={previous}&wait=false",
                urlencoding::encode(&subject),
                64 * 1024
            ))
            .await?;
        anyhow::ensure!(
            chunk.generation_id == generation,
            "the exec generation changed while the log was open"
        );
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&chunk.data_base64)
            .context("the API returned invalid base64 log data")?;
        if emit && !bytes.is_empty() {
            std::io::stdout().write_all(&bytes)?;
            std::io::stdout().flush()?;
        }
        offset = chunk.next_offset;
        if chunk.eof {
            return Ok(chunk);
        }
        if !follow && offset >= probe.next_offset {
            return Ok(chunk);
        }
        if bytes.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn run_pty(
    client: &Client,
    endpoint: Endpoint,
    config: &Config,
    command: PtyCommand,
    json_output: bool,
) -> Result<()> {
    match command {
        PtyCommand::Ls => {
            let status: StatusResponse = client.get("/v1/status").await?;
            let sessions = status
                .subjects
                .into_iter()
                .filter(|subject| {
                    subject.actual.as_ref().is_some_and(|actual| {
                        actual
                            .get("fields")
                            .unwrap_or(actual)
                            .get("terminal")
                            .and_then(Value::as_bool)
                            == Some(true)
                    })
                })
                .collect::<Vec<_>>();
            print_value(&sessions, json_output)
        }
        PtyCommand::Attach(args) => {
            let subject = normalize_member_subject(&args.subject, "pty");
            let attachment: Attachment = client
                .post(
                    &format!("/v1/sessions/attach/{}", urlencoding::encode(&subject)),
                    &AttachRequest::default(),
                )
                .await?;
            client.proxy_terminal(&attachment.websocket_path).await
        }
        PtyCommand::Peek(args) => {
            let subject = normalize_member_subject(&args.subject, "pty");
            let screen: SessionScreen = client
                .get(&format!(
                    "/v1/sessions/screen/{}",
                    urlencoding::encode(&subject)
                ))
                .await?;
            if json_output {
                print_value(&screen, true)
            } else {
                print!("{}", screen.screen);
                Ok(())
            }
        }
        PtyCommand::Send(args) => {
            let subject = normalize_member_subject(&args.subject, "pty");
            let incarnation = session_incarnation(client, &subject).await?;
            let mode = if args.raw {
                SessionInputMode::Raw
            } else if args.key {
                SessionInputMode::Key
            } else {
                SessionInputMode::Line
            };
            let value = if args.raw {
                base64::engine::general_purpose::STANDARD.encode(args.value.as_bytes())
            } else {
                args.value
            };
            let response: SessionControlResponse = client
                .post(
                    &format!("/v1/sessions/input/{}", urlencoding::encode(&subject)),
                    &SessionInputRequest {
                        expected_incarnation: incarnation,
                        mode,
                        value,
                        idempotency_key: format!("pty-input:{}:{}", subject, now_ms()),
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
        PtyCommand::Signal(args) => {
            let subject = normalize_member_subject(&args.subject, "pty");
            let response: SessionControlResponse = client
                .post(
                    &format!("/v1/sessions/{}/signal", urlencoding::encode(&subject)),
                    &SessionSignalRequest {
                        expected_incarnation: session_incarnation(client, &subject).await?,
                        signal: args.signal,
                        idempotency_key: format!("pty-signal:{}:{}", subject, now_ms()),
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
        PtyCommand::Ui => {
            anyhow::ensure!(
                matches!(endpoint, Endpoint::Unix(_)),
                "st3 pty ui is available only with the local Unix endpoint"
            );
            let _: Value = client.get("/v1/health").await?;
            let pty_root = config
                .pty_root
                .clone()
                .unwrap_or_else(|| config.state_dir.join("pty"));
            let status = std::process::Command::new("pty")
                .env("PTY_ROOT", pty_root)
                .status()
                .context("start the PTY operator interface")?;
            anyhow::ensure!(
                status.success(),
                "the PTY operator interface exited with {status}"
            );
            Ok(())
        }
    }
}

async fn run_inspect(client: &Client, args: InspectArgs, json_output: bool) -> Result<()> {
    if args.subject.starts_with("resource/")
        && let Some((subject, claim_id)) = args.subject.rsplit_once('@')
    {
        let claim: ClaimRecord = client
            .get(&format!(
                "/v1/claims/by-id/{}",
                urlencoding::encode(claim_id)
            ))
            .await?;
        anyhow::ensure!(
            claim.subject == subject,
            "claim `{claim_id}` belongs to `{}`, not `{subject}`",
            claim.subject
        );
        return print_value(
            &json!({
                "reference": args.subject,
                "actual": claim.body,
                "claim": claim,
            }),
            json_output,
        );
    }
    let status = status_for(client, &args.subject).await?;
    let claims: ClaimsPage = client
        .get(&format!(
            "/v1/claims?subject={}&order=desc&limit=20",
            urlencoding::encode(&args.subject)
        ))
        .await?;
    print_value(
        &json!({ "status": status, "recent_claims": claims.claims }),
        json_output,
    )
}

async fn run_trace(client: &Client, args: TraceArgs, json_output: bool) -> Result<()> {
    anyhow::ensure!(
        args.limit > 0 && args.limit <= 500,
        "the trace limit must be 1 through 500"
    );
    let mut query = vec![format!("limit={}", args.limit), "order=desc".into()];
    if let Some(subject) = &args.subject {
        query.push(format!("subject={}", urlencoding::encode(subject)));
    }
    if let Some(owner_run) = &args.owner_run {
        query.push(format!("owner_run={}", urlencoding::encode(owner_run)));
    }
    if let Some(after) = args.after_index {
        query.push(format!("after_index={after}"));
    }
    let page: ClaimsPage = client
        .get(&format!("/v1/claims?{}", query.join("&")))
        .await?;
    let mut claims = page.claims;
    claims.reverse();
    let mut cursor = args.after_index.unwrap_or_default();
    for claim in claims {
        cursor = cursor.max(claim.store_index);
        if json_output {
            println!("{}", serde_json::to_string(&claim)?);
        } else {
            println!("{}\t{}\t{}", claim.store_index, claim.kind, claim.subject);
        }
    }
    if !args.follow {
        return Ok(());
    }
    loop {
        let mut event_query = vec![format!("after={cursor}")];
        if let Some(subject) = &args.subject {
            event_query.push(format!("subject={}", urlencoding::encode(subject)));
        }
        if let Some(owner_run) = &args.owner_run {
            event_query.push(format!("owner_run={}", urlencoding::encode(owner_run)));
        }
        let events: Vec<EventRecord> = client
            .get(&format!("/v1/events?{}", event_query.join("&")))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
            if json_output {
                println!("{}", serde_json::to_string(&event)?);
            } else {
                println!("{}\t{}\t{}", event.store_index, event.kind, event.subject);
            }
        }
    }
}

async fn run_wait(client: &Client, args: WaitArgs, json_output: bool) -> Result<()> {
    validate_wait_condition(&args.condition)?;
    let timeout = parse_timeout(&args.timeout)?;
    let wait = wait_for_condition(client, &args.subject, &args.condition);
    let value = if timeout.is_zero() {
        wait.await?
    } else {
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| anyhow::anyhow!("wait timed out after {}", args.timeout))??
    };
    print_value(&value, json_output)
}

async fn wait_for_condition(client: &Client, subject: &str, condition: &str) -> Result<Value> {
    let mut cursor = 0;
    let actor = std::env::var("ST_AGENT")
        .ok()
        .filter(|value| value.starts_with("agent/"));
    loop {
        if let Some(value) = condition_value(client, subject, condition).await? {
            return Ok(value);
        }
        if let Some(actor) = actor.as_deref()
            && let Some(reason) = agent_wait_interruption(client, actor).await?
        {
            anyhow::bail!(reason);
        }
        let scope = actor.as_ref().map_or_else(
            || format!("&subject={}", urlencoding::encode(subject)),
            |_| String::new(),
        );
        let events: Vec<EventRecord> = client
            .get(&format!("/v1/events?after={cursor}{scope}"))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
        }
    }
}

async fn agent_wait_interruption(client: &Client, actor: &str) -> Result<Option<String>> {
    let work: Vec<StepRunView> = client
        .get(&format!(
            "/v1/work?actor={}&include_terminal=false",
            urlencoding::encode(actor)
        ))
        .await?;
    let ready = work
        .iter()
        .filter(|step| step.status == "ready")
        .map(|step| step.subject.clone())
        .collect::<Vec<_>>();
    let has_claimed_work = work.iter().any(|step| {
        matches!(step.status.as_str(), "claimed" | "working")
            && step.claimant.as_deref() == Some(actor)
    });
    let messages: Vec<MessageView> = client
        .get(&format!("/v1/messages?to={}", urlencoding::encode(actor)))
        .await?;
    let unread = messages
        .iter()
        .filter(|message| matches!(message.status.as_str(), "sent" | "delivered"))
        .map(|message| message.subject.clone())
        .collect::<Vec<_>>();
    Ok(wait_interruption_reason(
        actor,
        has_claimed_work,
        &ready,
        &unread,
    ))
}

fn wait_interruption_reason(
    actor: &str,
    has_claimed_work: bool,
    ready: &[String],
    unread: &[String],
) -> Option<String> {
    if !unread.is_empty() {
        return Some(format!(
            "the wait stopped because {actor} has a new message: {}. Run `st3 message ls`",
            unread.join(", ")
        ));
    }
    if !ready.is_empty() {
        return Some(format!(
            "the wait stopped because {actor} has ready work: {}. Run `st3 work ls`",
            ready.join(", ")
        ));
    }
    (!has_claimed_work).then(|| {
        format!(
            "{actor} cannot wait without claimed work. Finish this turn and let native delivery start the next turn"
        )
    })
}

async fn condition_value(client: &Client, subject: &str, condition: &str) -> Result<Option<Value>> {
    if let Some(expected) = condition.strip_prefix("verdict=") {
        let eval: EvalStatus = client
            .get(&format!("/v1/evals/{}", urlencoding::encode(subject)))
            .await?;
        return Ok((eval.verdict.as_deref() == Some(expected)).then(|| json!(eval)));
    }
    let status = status_for(client, subject).await?;
    let item = status.subjects.first();
    let actual_status = projected_actual_status(item.and_then(|item| item.actual.as_ref()));
    let matches = match condition {
        "running" => matches!(actual_status, Some("running" | "ready")),
        "ready" => actual_status == Some("ready"),
        "standing" => actual_status == Some("standing"),
        "completed" => actual_status == Some("completed"),
        "failed" => actual_status == Some("failed"),
        "cancelled" => actual_status == Some("cancelled"),
        "delivered" => actual_status == Some("delivered"),
        "terminal" => matches!(actual_status, Some("completed" | "failed" | "cancelled")),
        "exited" => actual_status == Some("exited"),
        "stopped" => {
            item.is_none_or(|item| item.actual.is_none())
                || matches!(actual_status, Some("stopped" | "removed"))
        }
        _ => false,
    };
    Ok(matches.then(|| json!(status)))
}

fn projected_actual_status(actual: Option<&Value>) -> Option<&str> {
    let fields = actual.map(|actual| actual.get("fields").unwrap_or(actual))?;
    fields
        .get("status")
        .or_else(|| fields.pointer("/facts/status"))
        .and_then(Value::as_str)
}

fn validate_wait_condition(condition: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(
            condition,
            "running"
                | "ready"
                | "standing"
                | "completed"
                | "failed"
                | "cancelled"
                | "delivered"
                | "terminal"
                | "exited"
                | "stopped"
        ) || matches!(
            condition.strip_prefix("verdict="),
            Some("pass" | "fail" | "void")
        ),
        "unknown wait condition `{condition}`"
    );
    Ok(())
}

fn parse_timeout(value: &str) -> Result<Duration> {
    if value == "0" {
        return Ok(Duration::ZERO);
    }
    for (suffix, factor) in [("ms", 1_u64), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)] {
        if let Some(number) = value.strip_suffix(suffix) {
            let amount = number.parse::<u64>()?;
            anyhow::ensure!(amount > 0, "a timeout must be positive or zero");
            return Ok(Duration::from_millis(amount.saturating_mul(factor)));
        }
    }
    anyhow::bail!("a timeout must use ms, s, m, h, or zero")
}

async fn run_doctor(client: &Client, args: DoctorArgs, json_output: bool) -> Result<()> {
    let report: DoctorReport = client.get("/v1/doctor").await?;
    if json_output {
        print_value(&report, true)?;
    } else {
        for check in &report.checks {
            println!("{}\t{}\t{}", check.status, check.name, check.message);
        }
    }
    anyhow::ensure!(report.status != "fail", "st3 doctor found a failed check");
    anyhow::ensure!(
        !args.strict || report.status == "pass",
        "st3 doctor found a warning in strict mode"
    );
    Ok(())
}

async fn run_replication(
    client: &Client,
    command: ReplicationCommand,
    json_output: bool,
) -> Result<()> {
    match command {
        ReplicationCommand::Status => {
            let status: ReplicationStatus = client.get("/v1/replication/status").await?;
            if json_output {
                return print_value(&status, true);
            }
            println!(
                "fleet\t{}",
                status.fleet_id.as_deref().unwrap_or("local-only")
            );
            println!("authority-digest\t{}", status.authority_digest);
            println!("graph-digest\t{}", status.graph_digest);
            println!("envelopes\t{}", status.received_envelopes);
            println!(
                "records\tvalid={} pending={} unknown={} invalid={} repaired={}",
                status.valid_records,
                status.pending_records,
                status.unknown_records,
                status.invalid_records,
                status.repaired_records
            );
            println!("unhealthy-projections\t{}", status.unhealthy_projections);
            for peer in status.peers {
                println!(
                    "peer\t{}\t{}\t{}",
                    peer.peer,
                    peer.status,
                    peer.last_error.as_deref().unwrap_or("")
                );
            }
            Ok(())
        }
        ReplicationCommand::Invalid { all } => {
            let records: Vec<ReplicaRecordView> = client
                .get(&format!(
                    "/v1/replication/records?unresolved={}",
                    if all { "false" } else { "true" }
                ))
                .await?;
            if json_output {
                return print_value(&records, true);
            }
            for record in records {
                println!(
                    "{}\t{}\t{}:{}\t{}\t{}",
                    record.state,
                    record.record_ref,
                    record.writer,
                    record.sequence,
                    record.subject.as_deref().unwrap_or("unknown-subject"),
                    record.error_message.as_deref().unwrap_or("")
                );
            }
            Ok(())
        }
        ReplicationCommand::Inspect { record } => {
            let record: ReplicaRecordView = client
                .get(&format!(
                    "/v1/replication/records/{}",
                    urlencoding::encode(record.strip_prefix("record/").unwrap_or(&record))
                ))
                .await?;
            if json_output {
                return print_value(&record, true);
            }
            println!("record\t{}", record.record_ref);
            println!("state\t{}", record.state);
            println!("writer\t{}", record.writer);
            println!("sequence\t{}", record.sequence);
            println!("envelope\t{}", record.envelope_hash);
            println!("position\t{}", record.position);
            println!("claim\t{}", record.claim_id.as_deref().unwrap_or(""));
            println!("subject\t{}", record.subject.as_deref().unwrap_or(""));
            println!("kind\t{}", record.kind.as_deref().unwrap_or(""));
            println!("error-code\t{}", record.error_code.as_deref().unwrap_or(""));
            println!("error\t{}", record.error_message.as_deref().unwrap_or(""));
            println!(
                "replacement\t{}",
                record.replacement_claim_id.as_deref().unwrap_or("")
            );
            Ok(())
        }
        ReplicationCommand::Diff { peer } => {
            let status: ReplicationStatus = client.get("/v1/replication/status").await?;
            let remote = status
                .peers
                .iter()
                .find(|item| item.peer == peer)
                .with_context(|| format!("peer `{peer}` is not configured"))?;
            let value = json!({
                "peer": peer,
                "status": remote.status,
                "authority": {
                    "local": status.authority_digest,
                    "remote": remote.authority_digest,
                    "equal": remote.authority_digest.as_deref() == Some(status.authority_digest.as_str()),
                },
                "graph": {
                    "local": status.graph_digest,
                    "remote": remote.graph_digest,
                    "equal": remote.graph_digest.as_deref() == Some(status.graph_digest.as_str()),
                },
            });
            if json_output {
                return print_value(&value, true);
            }
            println!("peer\t{}\t{}", peer, remote.status);
            println!(
                "authority\t{}\t{}\t{}",
                if remote.authority_digest.as_deref() == Some(status.authority_digest.as_str()) {
                    "equal"
                } else {
                    "different"
                },
                status.authority_digest,
                remote.authority_digest.as_deref().unwrap_or("unknown")
            );
            println!(
                "graph\t{}\t{}\t{}",
                if remote.graph_digest.as_deref() == Some(status.graph_digest.as_str()) {
                    "equal"
                } else {
                    "different"
                },
                status.graph_digest,
                remote.graph_digest.as_deref().unwrap_or("unknown")
            );
            Ok(())
        }
        ReplicationCommand::Repair {
            record,
            replacement_claim,
            reason,
            actor,
            idempotency_key,
        } => {
            let record_ref = if record.starts_with("record/") {
                record
            } else {
                format!("record/{record}")
            };
            let idempotency_key = idempotency_key.unwrap_or_else(|| {
                hex::encode(Sha256::digest(
                    format!("repair\0{record_ref}\0{replacement_claim}\0{reason}\0{actor}")
                        .as_bytes(),
                ))
            });
            let request = ReplicationRepairRequest {
                record_ref,
                replacement_claim_id: replacement_claim,
                reason,
                actor,
                idempotency_key,
            };
            let claim: ClaimRecord = client.post("/v1/replication/repair", &request).await?;
            if json_output {
                print_value(&claim, true)
            } else {
                println!("repaired\t{}", claim.id);
                Ok(())
            }
        }
    }
}

fn run_service(command: ServiceCommand) -> Result<()> {
    match command {
        ServiceCommand::Install { config } => {
            st3::service::install(Config::load(config.as_deref())?)
        }
        ServiceCommand::Status => st3::service::status(),
        ServiceCommand::Restart { config } => {
            st3::service::restart(Config::load(config.as_deref())?)
        }
        ServiceCommand::Reset { config } => {
            let config = Config::load(config.as_deref())?;
            confirm_service_reset(&config)?;
            st3::service::reset(config)
        }
        ServiceCommand::Uninstall => st3::service::uninstall(),
    }
}

fn confirm_service_reset(config: &Config) -> Result<()> {
    anyhow::ensure!(
        std::io::stdin().is_terminal(),
        "st3 service reset requires an interactive terminal"
    );
    let mut answer = String::new();
    for (prompt, expected) in [
        ("Erase all st3 state? Type `yes`: ", "yes"),
        (
            &format!("Type the node name `{}`: ", config.node),
            config.node.as_str(),
        ),
        ("Type `erase st3 state`: ", "erase st3 state"),
    ] {
        eprint!("{prompt}");
        std::io::stderr().flush()?;
        answer.clear();
        std::io::stdin().read_line(&mut answer)?;
        anyhow::ensure!(
            answer.trim() == expected,
            "the st3 state reset was cancelled"
        );
    }
    Ok(())
}

fn run_claude_channel(command: ClaudeChannelCommand) -> Result<()> {
    match command {
        ClaudeChannelCommand::Install { no_policy } => {
            st2::claude_channel::install(no_policy).map(|_| ())
        }
        ClaudeChannelCommand::Status => st2::claude_channel::status(),
        ClaudeChannelCommand::Uninstall { keep_policy } => {
            st2::claude_channel::uninstall(keep_policy)
        }
        ClaudeChannelCommand::InstallPolicy => st2::claude_channel::install_policy().map(|_| ()),
        ClaudeChannelCommand::UninstallPolicy => st2::claude_channel::uninstall_policy(),
    }
}

async fn status_for(client: &Client, subject: &str) -> Result<StatusResponse> {
    client
        .get(&format!(
            "/v1/status?subject={}",
            urlencoding::encode(subject)
        ))
        .await
}

async fn session_incarnation(client: &Client, subject: &str) -> Result<String> {
    let status = status_for(client, subject).await?;
    status
        .subjects
        .first()
        .and_then(|item| item.actual.as_ref())
        .map(|actual| actual.get("fields").unwrap_or(actual))
        .and_then(|fields| fields.get("incarnation_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("subject `{subject}` has no live incarnation"))
}

fn normalize_member_subject(subject: &str, namespace: &str) -> String {
    if subject.contains('/') {
        subject.into()
    } else {
        format!("{namespace}/{subject}")
    }
}

async fn post_staged_documents(client: &Client, root: &Path, kdl: &str) -> Result<()> {
    let intent = st3::parse_intent(kdl, "local")?;
    for reference in intent.document_refs {
        let (name, hash) = reference
            .rsplit_once('@')
            .with_context(|| format!("staged document reference `{reference}` has no hash"))?;
        let versions: Vec<DocumentVersion> = client
            .get(&format!("/v1/documents?name={}", urlencoding::encode(name)))
            .await?;
        if versions.iter().any(|version| version.hash == hash) {
            continue;
        }
        let path = root.join(".st3-documents").join(hash);
        let metadata = fs::symlink_metadata(&path).with_context(|| {
            format!(
                "document `{reference}` is absent from the API and {} is not staged",
                path.display()
            )
        })?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink() && metadata.is_file(),
            "staged document {} must be a regular file",
            path.display()
        );
        let bytes = fs::read(&path)?;
        let actual_hash = hex::encode(Sha256::digest(&bytes));
        anyhow::ensure!(
            actual_hash == hash,
            "staged document {} has hash {actual_hash}, not {hash}",
            path.display()
        );
        let expected_document = versions
            .iter()
            .find(|version| version.latest)
            .map(|version| version.binding_claim_id.clone());
        let _: DocumentVersion = client
            .post(
                "/v1/documents",
                &DocumentPutRequest {
                    name: name.into(),
                    bytes,
                    expected_document,
                    idempotency_key: format!("document:{name}:{hash}"),
                },
            )
            .await?;
    }
    Ok(())
}

async fn run_file_from_text(
    client: &Client,
    kdl: String,
    source_name: String,
    json_output: bool,
) -> Result<()> {
    let response = publish_text(client, kdl, source_name, publication_actor()?).await?;
    print_value(&response, json_output)
}

async fn run_doc(client: &Client, command: DocCommand, json_output: bool) -> Result<()> {
    match command {
        DocCommand::Put { file, name } => {
            let metadata = fs::symlink_metadata(&file)
                .with_context(|| format!("inspect document {}", file.display()))?;
            anyhow::ensure!(
                !metadata.file_type().is_symlink(),
                "document input cannot be a symbolic link"
            );
            anyhow::ensure!(metadata.is_file(), "document input must be a regular file");
            let bytes =
                fs::read(&file).with_context(|| format!("read document {}", file.display()))?;
            let local_hash = hex::encode(Sha256::digest(&bytes));
            let versions: Vec<DocumentVersion> = client
                .get(&format!(
                    "/v1/documents?name={}",
                    urlencoding::encode(&name)
                ))
                .await?;
            let selected = versions.iter().find(|version| version.latest);
            if let Some(selected) = selected.filter(|version| version.hash != local_hash) {
                eprintln!(
                    "warning: local bytes have hash {local_hash}; the selected binding has hash {}",
                    selected.hash
                );
            }
            let response: DocumentVersion = client
                .post(
                    "/v1/documents",
                    &DocumentPutRequest {
                        idempotency_key: format!("document:{name}:{local_hash}"),
                        name,
                        bytes,
                        expected_document: selected.map(|version| version.binding_claim_id.clone()),
                    },
                )
                .await?;
            if json_output {
                print_value(&response, true)
            } else {
                println!("{}@{}", response.name, response.hash);
                Ok(())
            }
        }
        DocCommand::Get { reference, output } => {
            let path = format!(
                "/v1/documents/content?reference={}",
                urlencoding::encode(&reference)
            );
            let response: Value = client.get(&path).await?;
            let bytes = serde_json::from_value::<Vec<u8>>(
                response
                    .get("bytes")
                    .cloned()
                    .context("document response lacks bytes")?,
            )?;
            if let Some(output) = output {
                fs::write(&output, bytes)
                    .with_context(|| format!("write document {}", output.display()))?;
            } else {
                use std::io::Write as _;
                std::io::stdout().write_all(&bytes)?;
            }
            Ok(())
        }
        DocCommand::List { name } => {
            let path = name.map_or_else(
                || "/v1/documents".to_owned(),
                |name| format!("/v1/documents?name={}", urlencoding::encode(&name)),
            );
            let response: Vec<DocumentVersion> = client.get(&path).await?;
            if json_output {
                print_value(&response, true)
            } else {
                for version in response {
                    let latest = if version.latest { " latest" } else { "" };
                    println!(
                        "{}@{} {} bytes{latest}",
                        version.name, version.hash, version.size
                    );
                }
                Ok(())
            }
        }
    }
}

async fn run_status(client: &Client, args: StatusArgs, json_output: bool) -> Result<()> {
    if let Some(presence) = args.set {
        let identity = args
            .subject
            .as_deref()
            .context("status --set needs an identity or ST_AGENT")?;
        let subject = normalize_agent_subject(identity);
        let response: ClaimRecord = client
            .post(
                "/v1/claims",
                &ClaimInput {
                    subject,
                    kind: "agent.presence".into(),
                    actor: Some(normalize_agent_subject(identity)),
                    fields: BTreeMap::from([
                        ("presence".into(), Value::String(presence)),
                        ("reachability".into(), Value::String("reachable".into())),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                },
            )
            .await?;
        return print_value(&response, json_output);
    }
    let mut query = Vec::new();
    if let Some(subject) = args.subject {
        query.push(format!("subject={}", urlencoding::encode(&subject)));
    }
    if let Some(owner_run) = args.owner_run {
        query.push(format!("owner_run={}", urlencoding::encode(&owner_run)));
    }
    if let Some(at_index) = args.at_index {
        query.push(format!("at_index={at_index}"));
    }
    let path = if query.is_empty() {
        "/v1/status".to_owned()
    } else {
        format!("/v1/status?{}", query.join("&"))
    };
    let response: StatusResponse = client.get(&path).await?;
    let terminal = response.subjects.iter().any(|subject| {
        subject.reachability == "unreachable"
            || subject.actual.as_ref().is_some_and(|actual| {
                matches!(
                    actual.get("verdict").and_then(Value::as_str),
                    Some("fail" | "void")
                )
            })
    });
    print_value(&response, json_output)?;
    anyhow::ensure!(!terminal, "terminal status selected");
    Ok(())
}

async fn run_agents(client: &Client, args: AgentsArgs, json_output: bool) -> Result<()> {
    let response: StatusResponse = client.get("/v1/status").await?;
    let agents = response
        .subjects
        .into_iter()
        .filter(|subject| subject.subject.starts_with("agent/"))
        .filter(|subject| {
            args.status.as_deref().is_none_or(|selected| {
                subject
                    .actual
                    .as_ref()
                    .and_then(|actual| actual.get("presence"))
                    .and_then(Value::as_str)
                    == Some(selected)
            })
        })
        .collect::<Vec<_>>();
    if json_output {
        return print_value(&agents, true);
    }
    for agent in agents {
        let actual = agent.actual.as_ref();
        let state = actual
            .and_then(|value| value.get("presence"))
            .and_then(Value::as_str)
            .or_else(|| {
                actual
                    .and_then(|value| value.get("status"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("unknown");
        let display_name = agent
            .desired
            .as_ref()
            .and_then(|desired| desired_child_string(desired, "name"));
        if args.enrich {
            let driver = agent
                .desired
                .as_ref()
                .and_then(|desired| desired_child_string(desired, "harness"))
                .unwrap_or("-");
            let incarnation = actual
                .and_then(|value| value.get("incarnation_id"))
                .and_then(Value::as_str)
                .unwrap_or("-");
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                agent.subject,
                display_name.unwrap_or("-"),
                state,
                agent.reachability,
                driver,
                agent.owner_run.as_deref().unwrap_or("-"),
            );
            println!("  incarnation {incarnation}");
        } else {
            println!(
                "{}\t{}\t{}",
                agent.subject,
                display_name.unwrap_or("-"),
                state
            );
        }
        for grouping in agent.under {
            match grouping.reason {
                Some(reason) => println!("  under {} ({reason})", grouping.agent),
                None => println!("  under {}", grouping.agent),
            }
        }
    }
    Ok(())
}

fn desired_child_string<'a>(desired: &'a Value, child_name: &str) -> Option<&'a str> {
    desired
        .get("children")?
        .as_array()?
        .iter()
        .find(|child| child.get("name").and_then(Value::as_str) == Some(child_name))?
        .get("arguments")?
        .as_array()?
        .first()?
        .as_str()
}

async fn run_runtime(client: &Client, command: RuntimeCommand, json_output: bool) -> Result<()> {
    match command {
        RuntimeCommand::Ls => {
            let response: StatusResponse = client.get("/v1/status").await?;
            let runtimes = response
                .subjects
                .into_iter()
                .filter(|subject| matches!(subject.kind.as_deref(), Some("agent" | "exec" | "pty")))
                .collect::<Vec<_>>();
            if json_output {
                return print_value(&runtimes, true);
            }
            for runtime in runtimes {
                let status = runtime
                    .actual
                    .as_ref()
                    .and_then(|actual| actual.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let parked = runtime.reachability == "unreachable";
                println!(
                    "{}\t{}\t{}{}",
                    runtime.subject,
                    status,
                    runtime.reachability,
                    if parked { "\tparked" } else { "" }
                );
            }
            Ok(())
        }
        RuntimeCommand::Reset {
            subject,
            reason,
            print_kdl,
            actor,
        } => {
            anyhow::ensure!(
                subject.starts_with("agent/")
                    || subject.starts_with("exec/")
                    || subject.starts_with("pty/"),
                "runtime reset needs a full agent, exec, or PTY subject"
            );
            let status: StatusResponse = client
                .get(&format!(
                    "/v1/status?subject={}",
                    urlencoding::encode(&subject)
                ))
                .await?;
            let runtime = status
                .subjects
                .into_iter()
                .find(|candidate| candidate.subject == subject)
                .with_context(|| format!("runtime `{subject}` does not exist"))?;
            let run_subject = runtime
                .owner_run
                .context("the runtime has no owning mission run")?;
            let run: MissionRunView = client
                .get(&format!(
                    "/v1/mission-runs/{}",
                    urlencoding::encode(&run_subject)
                ))
                .await?;
            let operation = format!("reset-{}", uuid::Uuid::now_v7().simple());
            let response_kdl =
                runtime_reset_intent(&run.subject, &operation, &subject, &run.generation, &reason);
            if print_kdl {
                print!("{response_kdl}");
                return Ok(());
            }
            let actor = actor.context("runtime reset needs --as or ST_AGENT")?;
            let response = publish_text(
                client,
                response_kdl,
                format!("st3 runtime reset {subject}"),
                actor,
            )
            .await?;
            print_value(&response, json_output)
        }
    }
}

fn runtime_reset_intent(
    run: &str,
    operation: &str,
    runtime: &str,
    generation: &str,
    reason: &str,
) -> String {
    format!(
        "version 2\nmission-run {run:?} {{\n  reset {operation:?} {{\n    runtime {runtime:?}\n    from {generation:?}\n    reason {reason:?}\n  }}\n}}\n"
    )
}

async fn run_context(client: &Client, command: ContextCommand, json_output: bool) -> Result<()> {
    match command {
        ContextCommand::Read(args) => {
            let identity = required_identity(args.identity)?;
            let prefix = format!("doc/context/{identity}/");
            let versions: Vec<DocumentVersion> = client.get("/v1/documents").await?;
            let mut selected = versions
                .into_iter()
                .filter(|version| version.latest && version.name.starts_with(&prefix))
                .filter(|version| {
                    if args.decisions && !args.full {
                        version.name.starts_with(&format!("{prefix}decisions/"))
                    } else if !args.full {
                        version.name == format!("{prefix}now")
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>();
            selected.sort_by(|left, right| left.name.cmp(&right.name));
            if json_output {
                let mut values = Vec::new();
                for version in selected {
                    values.push(json!({
                        "reference": format!("{}@{}", version.name, version.hash),
                        "content": String::from_utf8(document_bytes(client, &version.name, &version.hash).await?)?,
                    }));
                }
                return print_value(&values, true);
            }
            for (index, version) in selected.iter().enumerate() {
                if index != 0 {
                    println!();
                }
                std::io::Write::write_all(
                    &mut std::io::stdout(),
                    &document_bytes(client, &version.name, &version.hash).await?,
                )?;
            }
            Ok(())
        }
        ContextCommand::Write(args) => {
            let identity = required_identity(args.identity)?;
            let mut bytes = Vec::new();
            std::io::stdin().read_to_end(&mut bytes)?;
            std::str::from_utf8(&bytes).context("context must be UTF-8 text")?;
            let name = format!("doc/context/{identity}/now");
            let version = put_document_bytes(client, name, bytes).await?;
            if json_output {
                print_value(&version, true)
            } else {
                println!("{}@{}", version.name, version.hash);
                Ok(())
            }
        }
        ContextCommand::Append(args) => {
            let identity = required_identity(args.identity)?;
            let content = format!("# Decision\n\n{}\n\n# Why\n\n{}\n", args.decision, args.why);
            let hash = hex::encode(Sha256::digest(content.as_bytes()));
            let name = format!(
                "doc/context/{identity}/decisions/{:020}-{}",
                now_ms(),
                &hash[..12]
            );
            let version = put_document_bytes(client, name, content.into_bytes()).await?;
            if json_output {
                print_value(&version, true)
            } else {
                println!("{}@{}", version.name, version.hash);
                Ok(())
            }
        }
    }
}

async fn put_document_bytes(
    client: &Client,
    name: String,
    bytes: Vec<u8>,
) -> Result<DocumentVersion> {
    let versions: Vec<DocumentVersion> = client
        .get(&format!(
            "/v1/documents?name={}",
            urlencoding::encode(&name)
        ))
        .await?;
    let expected_document = versions
        .iter()
        .find(|version| version.latest)
        .map(|version| version.binding_claim_id.clone());
    let hash = hex::encode(Sha256::digest(&bytes));
    client
        .post(
            "/v1/documents",
            &DocumentPutRequest {
                name: name.clone(),
                bytes,
                expected_document,
                idempotency_key: format!("document:{name}:{hash}"),
            },
        )
        .await
}

async fn run_resource(client: &Client, command: ResourceCommand, json_output: bool) -> Result<()> {
    match command {
        ResourceCommand::Add(args) => {
            let owner = required_identity(args.identity)?;
            let hash = hex::encode(Sha256::digest(args.url.as_bytes()));
            let subject = format!("resource/{}", &hash[..20]);
            let fields = BTreeMap::from([
                (
                    "kind".into(),
                    Value::String("custom.st3.external-reference".into()),
                ),
                ("status".into(), Value::String("active".into())),
                ("url".into(), Value::String(args.url)),
                (
                    "owner".into(),
                    Value::String(normalize_agent_subject(&owner)),
                ),
                (
                    "title".into(),
                    args.title.map(Value::String).unwrap_or(Value::Null),
                ),
                (
                    "relation".into(),
                    args.relation.map(Value::String).unwrap_or(Value::Null),
                ),
                (
                    "tags".into(),
                    Value::Array(args.tags.into_iter().map(Value::String).collect()),
                ),
            ]);
            let record: ClaimRecord = client
                .post(
                    "/v1/claims",
                    &ClaimInput {
                        subject: subject.clone(),
                        kind: "resource.observed".into(),
                        actor: Some(normalize_agent_subject(&owner)),
                        fields,
                        evidence: Vec::new(),
                        expected_subject: None,
                        idempotency_key: Some(format!("resource-add:{owner}:{hash}")),
                    },
                )
                .await?;
            if json_output {
                print_value(&record, true)
            } else {
                println!("{}", subject.trim_start_matches("resource/"));
                Ok(())
            }
        }
        ResourceCommand::Ls(args) => {
            let owner = args
                .identity
                .map(|identity| normalize_agent_subject(&identity));
            let response: StatusResponse = client.get("/v1/status").await?;
            let resources = response
                .subjects
                .into_iter()
                .filter(|subject| subject.subject.starts_with("resource/"))
                .filter(|subject| {
                    owner.as_deref().is_none_or(|owner| {
                        subject
                            .actual
                            .as_ref()
                            .and_then(|actual| actual.get("owner"))
                            .and_then(Value::as_str)
                            == Some(owner)
                    })
                })
                .collect::<Vec<_>>();
            if json_output {
                return print_value(&resources, true);
            }
            for resource in resources {
                let actual = resource.actual.unwrap_or(Value::Null);
                println!(
                    "{}\t{}\t{}",
                    resource.subject.trim_start_matches("resource/"),
                    actual
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                    actual.get("url").and_then(Value::as_str).unwrap_or("")
                );
            }
            Ok(())
        }
        ResourceCommand::Read(args) => {
            let (reference, _) = positional_identity_and_reference(args.values, args.identity)?;
            let subject = normalize_resource_subject(&reference);
            let response: StatusResponse = client
                .get(&format!(
                    "/v1/status?subject={}",
                    urlencoding::encode(&subject)
                ))
                .await?;
            let resource = response
                .subjects
                .into_iter()
                .next()
                .context("resource does not exist")?;
            print_value(&resource, json_output)
        }
        ResourceCommand::Remove(args) => {
            let (reference, identity) =
                positional_identity_and_reference(args.values, args.identity)?;
            let identity = required_identity(identity)?;
            let subject = normalize_resource_subject(&reference);
            let record: ClaimRecord = client
                .post(
                    "/v1/claims",
                    &ClaimInput {
                        subject: subject.clone(),
                        kind: "resource.observed".into(),
                        actor: Some(normalize_agent_subject(&identity)),
                        fields: BTreeMap::from([(
                            "status".into(),
                            Value::String("removed".into()),
                        )]),
                        evidence: Vec::new(),
                        expected_subject: None,
                        idempotency_key: Some(format!("resource-remove:{identity}:{subject}")),
                    },
                )
                .await?;
            print_value(&record, json_output)
        }
        ResourceCommand::Watch(args) => {
            let target = args
                .target
                .context("a resource watch needs --to or ST_AGENT")?;
            let target = normalize_message_subject(&target);
            let (kdl, mission_id, resource) =
                resource_watch_intent(&args.provider, &args.locator, &args.fields, &target)?;
            if args.print_kdl {
                print!("{kdl}");
                return Ok(());
            }
            let parsed = st3::parse_intent(&kdl, "local")?;
            let revision = parsed.missions[&mission_id].revision.clone();
            let published = publish_text(
                client,
                kdl,
                format!("st3 resource watch {resource}"),
                target.clone(),
            )
            .await?;
            let mut active: Vec<MissionRunView> = client
                .get(&format!(
                    "/v1/mission-runs?mission={}",
                    urlencoding::encode(&mission_id)
                ))
                .await?;
            anyhow::ensure!(
                active.len() <= 1,
                "resource watch mission `mission/{mission_id}` has more than one active run"
            );
            let run = if let Some(run) = active.pop() {
                anyhow::ensure!(
                    run.revision == revision,
                    "the resource watch mission has an unexpected active revision"
                );
                run
            } else {
                let workspace = std::env::current_dir()?.canonicalize()?;
                let run_kdl = mission_run_intent(
                    &mission_id,
                    &mission_id,
                    &revision,
                    &workspace,
                    &target,
                    &BTreeMap::new(),
                    "run",
                );
                let applied = publish_text(
                    client,
                    run_kdl,
                    format!("st3 resource watch {resource} run"),
                    target.clone(),
                )
                .await?;
                let _ = published.store_index.max(applied.store_index);
                client
                    .get::<MissionRunView>(&format!(
                        "/v1/mission-runs/{}",
                        urlencoding::encode(&format!("mission-run/{mission_id}"))
                    ))
                    .await?
            };
            let response = ResourceWatchView {
                resource,
                observer: format!("observer/{}/watch", run.id),
                subscription: format!("subscription/{}/watch", run.id),
            };
            if json_output {
                print_value(&response, true)
            } else {
                println!(
                    "{}\t{}\t{}",
                    response.resource, response.observer, response.subscription
                );
                Ok(())
            }
        }
        ResourceCommand::Unwatch(args) => {
            let actor = args
                .actor
                .context("a resource unwatch needs --as or ST_AGENT")?;
            let subscription = args
                .subscription
                .strip_prefix("subscription/")
                .unwrap_or(&args.subscription);
            let subject = format!("subscription/{subscription}");
            let status: StatusResponse = client
                .get(&format!(
                    "/v1/status?subject={}",
                    urlencoding::encode(&subject)
                ))
                .await?;
            let run = status
                .subjects
                .into_iter()
                .find(|item| item.subject == subject)
                .and_then(|item| item.owner_run)
                .context("the subscription has no owning mission run")?;
            let operation = format!("unwatch-{}", uuid::Uuid::now_v7().simple());
            let kdl = cancellation_intent(&run, &operation, "the resource watch stopped");
            if args.print_kdl {
                print!("{kdl}");
                return Ok(());
            }
            let response = publish_text(
                client,
                kdl,
                format!("st3 resource unwatch {subscription}"),
                actor,
            )
            .await?;
            print_value(&response, json_output)
        }
        ResourceCommand::Refresh {
            resource,
            timeout,
            print_kdl,
            actor,
        } => {
            let timeout = parse_timeout(&timeout)?;
            let timeout_ms =
                u64::try_from(timeout.as_millis()).context("the refresh timeout is too large")?;
            let resource = normalize_resource_subject(&resource);
            anyhow::ensure!(
                timeout_ms > 0 && timeout_ms <= 3_600_000,
                "a resource refresh timeout must be between 1 ms and 1 hour"
            );
            let operation = format!("refresh-{}", uuid::Uuid::now_v7().simple());
            let kdl = resource_refresh_intent(&resource, &operation, timeout_ms);
            if print_kdl {
                print!("{kdl}");
                return Ok(());
            }
            let actor = actor.context("a resource refresh needs --as or ST_AGENT")?;
            let response = publish_text(
                client,
                kdl,
                format!("st3 resource refresh {resource}"),
                actor,
            )
            .await?;
            let completed = follow_resource_refresh(client, &resource, &response, timeout).await?;
            print_value(&completed, json_output)
        }
    }
}

async fn follow_resource_refresh(
    client: &Client,
    resource: &str,
    publication: &ApplyResponse,
    timeout: Duration,
) -> Result<ResourceRefreshView> {
    let mut attempts = BTreeMap::new();
    for claim_id in &publication.claim_ids {
        let claim: ClaimRecord = client
            .get(&format!(
                "/v1/claims/by-id/{}",
                urlencoding::encode(claim_id)
            ))
            .await?;
        if claim.kind != "observer.refresh-requested" {
            continue;
        }
        if let Some(attempt) = claim
            .body
            .pointer("/fields/attempt")
            .and_then(Value::as_str)
        {
            attempts.insert(claim.subject, attempt.to_owned());
        }
    }
    anyhow::ensure!(
        !attempts.is_empty(),
        "the refresh publication did not identify an observer attempt"
    );
    let wait = async {
        let mut cursor = publication.store_index;
        let mut completed = BTreeSet::new();
        let mut changed = false;
        let mut completed_at_index = cursor;
        while completed.len() != attempts.len() {
            let events: Vec<EventRecord> = client
                .get(&format!(
                    "/v1/events?after={cursor}&wait=true&timeout_ms=30000"
                ))
                .await?;
            for event in events {
                cursor = cursor.max(event.store_index);
                let Some(expected) = attempts.get(&event.subject) else {
                    continue;
                };
                if event
                    .body
                    .pointer("/fields/attempt")
                    .and_then(Value::as_str)
                    != Some(expected.as_str())
                {
                    continue;
                }
                if event.kind == "observer.observed" {
                    changed |= event
                        .body
                        .pointer("/fields/changed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    completed_at_index = completed_at_index.max(event.store_index);
                    completed.insert(event.subject);
                } else if event.kind == "observer.state"
                    && event.body.pointer("/fields/state").and_then(Value::as_str)
                        == Some("unreachable")
                {
                    let reason = event
                        .body
                        .pointer("/fields/reason")
                        .and_then(Value::as_str)
                        .unwrap_or("the observer failed");
                    anyhow::bail!(
                        "observer `{}` could not refresh `{resource}`: {reason}",
                        event.subject
                    );
                }
            }
        }
        Ok(ResourceRefreshView {
            resource: resource.to_owned(),
            observers: attempts.keys().cloned().collect(),
            changed,
            completed_at_index,
        })
    };
    tokio::time::timeout(timeout, wait)
        .await
        .with_context(|| format!("resource `{resource}` did not finish its refresh in time"))?
}

fn normalize_resource_subject(reference: &str) -> String {
    if reference.starts_with("resource/") {
        reference.to_owned()
    } else {
        format!("resource/{reference}")
    }
}

fn resource_watch_intent(
    provider: &str,
    locator: &str,
    fields: &[String],
    target: &str,
) -> Result<(String, String, String)> {
    anyhow::ensure!(
        !fields.is_empty(),
        "a resource watch needs at least one field"
    );
    let (resource_name, resource_kind) = match provider {
        "github.pull-request" => {
            for field in fields {
                anyhow::ensure!(
                    matches!(field.as_str(), "head" | "state" | "review" | "checks"),
                    "GitHub pull request provider does not support field `{field}`"
                );
            }
            let (repository, number) = locator
                .rsplit_once('#')
                .context("a GitHub pull request locator needs OWNER/REPO#NUMBER")?;
            let (owner, repository) = repository
                .split_once('/')
                .context("a GitHub pull request locator needs OWNER/REPO#NUMBER")?;
            number
                .parse::<u64>()
                .context("a GitHub pull request number must be an integer")?;
            (
                format!("github/{owner}/{repository}/pull/{number}"),
                "vcs.pull-request",
            )
        }
        "github.repository" => {
            for field in fields {
                anyhow::ensure!(
                    matches!(field.as_str(), "pull_requests" | "issues"),
                    "GitHub repository provider does not support field `{field}`"
                );
            }
            let (owner, repository) = locator
                .split_once('/')
                .context("a GitHub repository locator needs OWNER/REPO")?;
            anyhow::ensure!(
                !owner.is_empty() && !repository.is_empty() && !repository.contains('/'),
                "a GitHub repository locator needs OWNER/REPO"
            );
            (format!("github/{owner}/{repository}"), "vcs.repository")
        }
        "local.file" => {
            anyhow::ensure!(
                Path::new(locator).is_absolute(),
                "a local file locator must be an absolute path"
            );
            for field in fields {
                anyhow::ensure!(
                    matches!(
                        field.as_str(),
                        "status" | "path" | "content_hash" | "size" | "mode" | "reason"
                    ),
                    "local file provider does not support field `{field}`"
                );
            }
            let hash = hex::encode(Sha256::digest(locator.as_bytes()));
            (
                format!("local-file/local/{}", &hash[..24]),
                "filesystem.file",
            )
        }
        _ => anyhow::bail!("resource provider `{provider}` is not registered"),
    };
    let fields = fields.iter().cloned().collect::<BTreeSet<_>>();
    let stable = serde_json::to_vec(&json!({
        "provider": provider,
        "locator": locator,
        "fields": fields,
        "target": target,
        "delivery": "message",
    }))?;
    let hash = hex::encode(Sha256::digest(stable));
    let mission_id = format!("resource-watch/{resource_name}/{}", &hash[..16]);
    let observer_fields = fields
        .iter()
        .map(|field| format!("      field {field:?}\n"))
        .collect::<String>();
    let subscription_fields = fields
        .iter()
        .map(|field| format!("      on {field:?}\n"))
        .collect::<String>();
    let kdl = format!(
        "version 2\nresource {resource_name:?} {{\n  kind {resource_kind:?}\n}}\nmission {mission_id:?} state=\"ready\" {{\n  goal \"Observe one resource and send its selected changes.\"\n  observer \"watch\" {{\n    resource {:?}\n    provider {provider:?}\n    locator {locator:?}\n{observer_fields}  }}\n  subscription \"watch\" {{\n    observer \"observer/watch\"\n    to {target:?}\n{subscription_fields}    delivery \"message\"\n  }}\n}}\n",
        format!("resource/{resource_name}")
    );
    Ok((kdl, mission_id, format!("resource/{resource_name}")))
}

fn resource_refresh_intent(resource: &str, operation: &str, timeout_ms: u64) -> String {
    let resource = resource.strip_prefix("resource/").unwrap_or(resource);
    format!(
        "version 2\nresource {resource:?} {{\n  refresh {operation:?} {{\n    timeout {:?}\n  }}\n}}\n",
        format!("{timeout_ms}ms")
    )
}

fn cancellation_intent(run: &str, operation: &str, reason: &str) -> String {
    format!(
        "version 2\nmission-run {run:?} {{\n  cancellation {operation:?} {{\n    reason {reason:?}\n  }}\n}}\n"
    )
}

fn normalize_message_subject(value: &str) -> String {
    let mission_run = std::env::var("ST_MISSION_RUN")
        .ok()
        .filter(|value| !value.is_empty());
    normalize_message_subject_in_run(value, mission_run.as_deref())
}

fn normalize_message_subject_in_run(value: &str, mission_run: Option<&str>) -> String {
    if value == "requester" {
        "person/requester".into()
    } else if value.contains('/') {
        value.into()
    } else if let Some(mission_run) = mission_run {
        format!("agent/{mission_run}/{value}")
    } else {
        format!("agent/{value}")
    }
}

fn positional_identity_and_reference(
    values: Vec<String>,
    explicit_identity: Option<String>,
) -> Result<(String, Option<String>)> {
    match values.as_slice() {
        [reference] => Ok((reference.clone(), explicit_identity)),
        [identity, reference] => Ok((
            reference.clone(),
            explicit_identity.or_else(|| Some(identity.clone())),
        )),
        _ => anyhow::bail!("the command needs one reference and at most one identity"),
    }
}

async fn document_bytes(client: &Client, name: &str, hash: &str) -> Result<Vec<u8>> {
    let value: Value = client
        .get(&format!(
            "/v1/documents/content?reference={}",
            urlencoding::encode(&format!("{name}@{hash}"))
        ))
        .await?;
    serde_json::from_value(
        value
            .get("bytes")
            .cloned()
            .context("document response lacks bytes")?,
    )
    .map_err(Into::into)
}

fn required_identity(identity: Option<String>) -> Result<String> {
    identity.context("the command needs an identity or ST_AGENT")
}

fn normalize_agent_subject(identity: &str) -> String {
    if identity.starts_with("agent/") {
        identity.to_owned()
    } else {
        format!("agent/{identity}")
    }
}

async fn run_claim(client: &Client, args: ClaimArgs, json_output: bool) -> Result<()> {
    let response: ClaimRecord = client
        .post(
            "/v1/claims",
            &ClaimInput {
                subject: args.subject,
                kind: args.kind,
                actor: args.actor,
                fields: args.fields.into_iter().collect(),
                evidence: args.evidence,
                expected_subject: None,
                idempotency_key: args.idempotency_key,
            },
        )
        .await?;
    if json_output {
        print_value(&response, true)
    } else {
        println!("{}", response.id);
        Ok(())
    }
}

async fn run_schema(client: &Client, command: SchemaCommand, json_output: bool) -> Result<()> {
    let value: Value = client.get("/v1/schema").await?;
    let selected = match command {
        SchemaCommand::Export => value,
        SchemaCommand::Subjects => value
            .get("subjects")
            .cloned()
            .context("the schema response lacks subjects")?,
        SchemaCommand::Resources => value
            .get("resources")
            .cloned()
            .context("the schema response lacks resources")?,
        SchemaCommand::Claims { subject } => {
            let claims = value
                .get("claims")
                .and_then(Value::as_object)
                .context("the schema response lacks claims")?;
            if let Some(subject) = subject {
                let family = subject
                    .split_once('/')
                    .map(|(family, _)| family)
                    .context("a schema subject must be a full subject")?;
                Value::Object(
                    claims
                        .iter()
                        .filter(|(_, spec)| {
                            spec.get("subjects")
                                .and_then(Value::as_array)
                                .is_some_and(|subjects| {
                                    subjects.iter().any(|candidate| {
                                        candidate.as_str() == Some("*")
                                            || candidate.as_str() == Some(family)
                                    })
                                })
                        })
                        .map(|(kind, spec)| (kind.clone(), spec.clone()))
                        .collect(),
                )
            } else {
                Value::Object(claims.clone())
            }
        }
        SchemaCommand::Show { kind } => {
            let escaped = kind.replace('~', "~0").replace('/', "~1");
            value
                .pointer(&format!("/claims/{escaped}"))
                .or_else(|| value.pointer(&format!("/resources/{escaped}")))
                .or_else(|| value.pointer(&format!("/subjects/{escaped}")))
                .cloned()
                .with_context(|| format!("schema item `{kind}` is not registered"))?
        }
    };
    print_value(&selected, json_output)
}

async fn run_review(client: &Client, command: ReviewCommand, json_output: bool) -> Result<()> {
    let (decision, args) = match command {
        ReviewCommand::Approve(args) => ("approved", args),
        ReviewCommand::Reject(args) => ("rejected", args),
    };
    let path = format!("/v1/reviews/{}", args.resource);
    let response: ClaimRecord = client
        .post(
            &path,
            &ReviewRequest {
                decision: decision.into(),
                reason: args.reason,
                actor: args.actor,
                expected_subject: None,
            },
        )
        .await?;
    print_value(&response, json_output)
}

async fn run_work(client: &Client, command: WorkCommand, json_output: bool) -> Result<()> {
    match command {
        WorkCommand::Ls { actor, all } => {
            let include_terminal = if json_output { all } else { true };
            let path = if let Some(actor) = actor.as_deref() {
                format!(
                    "/v1/work?actor={}&include_terminal={include_terminal}",
                    urlencoding::encode(actor)
                )
            } else {
                format!("/v1/work?include_terminal={include_terminal}")
            };
            let work: Vec<StepRunView> = client.get(&path).await?;
            if json_output {
                return print_value(&work, true);
            }
            print!(
                "{}",
                render_work_list(actor.as_deref(), &work, all, OutputStyle::stdout())
            );
            Ok(())
        }
        WorkCommand::Show { subject } => {
            let work: Vec<StepRunView> = client.get("/v1/work?include_terminal=true").await?;
            let normalized = if subject.starts_with("step-run/") {
                subject
            } else {
                format!("step-run/{subject}")
            };
            let step = work
                .into_iter()
                .find(|step| step.subject == normalized)
                .with_context(|| format!("step run `{normalized}` does not exist"))?;
            if json_output {
                print_value(&step, true)
            } else {
                print!(
                    "{}",
                    render_step_run(&step, OutputStyle::stdout(), current_unix_ms()?)
                );
                Ok(())
            }
        }
        WorkCommand::Claim(args) => post_work(client, "claim", args, json_output).await,
        WorkCommand::Renew(args) => post_work(client, "renew", args, json_output).await,
        WorkCommand::Progress(args) => post_work(client, "progress", args, json_output).await,
        WorkCommand::Complete(args) => post_work(client, "complete", args, json_output).await,
        WorkCommand::Fail(args) => post_work(client, "fail", args, json_output).await,
        WorkCommand::Release(args) => post_work(client, "release", args, json_output).await,
        WorkCommand::PublishMission(args) => publish_work_mission(client, args, json_output).await,
        WorkCommand::Revise(args) => {
            let actor = args
                .actor
                .context("a mission revision needs --as or ST_AGENT")?;
            let kdl = fs::read_to_string(&args.file)
                .with_context(|| format!("read KDL {}", args.file.display()))?;
            let run: MissionRunView = client
                .get(&format!(
                    "/v1/mission-runs/{}",
                    urlencoding::encode(&args.run)
                ))
                .await?;
            let parsed = st3::parse_intent(&kdl, "local")?;
            let mission_id = run.mission.strip_prefix("mission/").unwrap_or(&run.mission);
            let candidate = parsed.missions.get(mission_id).with_context(|| {
                format!(
                    "{} must contain the current mission `{mission_id}`",
                    args.file.display()
                )
            })?;
            anyhow::ensure!(
                parsed.missions.len() == 1,
                "a mission revision file must contain exactly one mission"
            );
            let operation = format!("revision-{}", uuid::Uuid::now_v7().simple());
            let revision_kdl = mission_revision_intent(
                &run.subject,
                &operation,
                mission_id,
                &candidate.revision,
                &run.generation,
                &args.reason,
            );
            if args.print_kdl {
                eprintln!(
                    "Publish the candidate mission first: st3 publish {} --as {}",
                    args.file.display(),
                    actor
                );
                print!("{revision_kdl}");
                return Ok(());
            }
            let response: RevisionSubmissionView = client
                .post(
                    &format!(
                        "/v1/mission-runs/{}/revision",
                        urlencoding::encode(&run.subject)
                    ),
                    &MissionRevisionRequest {
                        intent: IntentInput {
                            kdl,
                            source_name: Some(args.file.display().to_string()),
                        },
                        actor,
                        reason: args.reason,
                        idempotency_key: operation,
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
        WorkCommand::Revision { command } => run_work_revision(client, command, json_output).await,
    }
}

async fn run_work_revision(
    client: &Client,
    command: WorkRevisionCommand,
    json_output: bool,
) -> Result<()> {
    match command {
        WorkRevisionCommand::Show { run } => {
            let proposal: RevisionProposalView = client
                .get(&format!(
                    "/v1/mission-runs/{}/revision-proposal",
                    urlencoding::encode(&run)
                ))
                .await?;
            if json_output {
                print_value(&proposal, true)
            } else {
                print!(
                    "{}",
                    render_revision_proposal(&proposal, OutputStyle::stdout(), current_unix_ms()?)
                );
                Ok(())
            }
        }
        WorkRevisionCommand::Generations { run } => {
            let generations: Vec<RunGenerationView> = client
                .get(&format!(
                    "/v1/mission-runs/{}/generations",
                    urlencoding::encode(&run)
                ))
                .await?;
            if json_output {
                print_value(&generations, true)
            } else {
                let mission_run: MissionRunView = client
                    .get(&format!("/v1/mission-runs/{}", urlencoding::encode(&run)))
                    .await?;
                print!(
                    "{}",
                    render_generations(&mission_run, &generations, OutputStyle::stdout())
                );
                Ok(())
            }
        }
        WorkRevisionCommand::Generation { generation } => {
            let generation: RunGenerationView = client
                .get(&format!(
                    "/v1/run-generations/{}",
                    urlencoding::encode(&generation)
                ))
                .await?;
            if json_output {
                print_value(&generation, true)
            } else {
                print!("{}", render_generation(&generation, OutputStyle::stdout()));
                Ok(())
            }
        }
        WorkRevisionCommand::Approve {
            proposal,
            preview_hash,
            actor,
        } => {
            let actor = actor.context("a revision approval needs --as or ST_AGENT")?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let response: RevisionSubmissionView = client
                .post(
                    &format!(
                        "/v1/revision-proposals/{}/approve",
                        urlencoding::encode(&proposal)
                    ),
                    &RevisionApprovalRequest {
                        actor,
                        preview_hash,
                        idempotency_key: format!("revision-approve:{proposal}:{nonce}"),
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
        WorkRevisionCommand::Cancel {
            proposal,
            actor,
            reason,
        } => {
            let actor = actor.context("a revision cancellation needs --as or ST_AGENT")?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let response: RevisionProposalView = client
                .post(
                    &format!(
                        "/v1/revision-proposals/{}/cancel",
                        urlencoding::encode(&proposal)
                    ),
                    &RevisionCancelRequest {
                        actor,
                        reason,
                        idempotency_key: format!("revision-cancel:{proposal}:{nonce}"),
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
    }
}

async fn publish_work_mission(
    client: &Client,
    args: WorkPublishMissionArgs,
    json_output: bool,
) -> Result<()> {
    let actor = args
        .actor
        .context("publishing a mission output needs --as or ST_AGENT")?;
    let incarnation = match args.incarnation {
        Some(incarnation) => Some(incarnation),
        None => current_agent_incarnation(client, &actor).await?,
    };
    let kdl = fs::read_to_string(&args.file)
        .with_context(|| format!("read KDL {}", args.file.display()))?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let output: MissionOutputView = client
        .post(
            &format!("/v1/work/mission/{}", urlencoding::encode(&args.subject)),
            &MissionProductionRequest {
                intent: IntentInput {
                    kdl,
                    source_name: Some(args.file.display().to_string()),
                },
                actor,
                incarnation,
                idempotency_key: format!("mission-output:{}:{nonce}", args.subject),
            },
        )
        .await?;
    if json_output {
        print_value(&output, true)
    } else {
        println!("{}@{}", output.mission, output.revision);
        Ok(())
    }
}

async fn post_work(
    client: &Client,
    action: &str,
    args: WorkActionArgs,
    json_output: bool,
) -> Result<()> {
    let actor = args.actor.context("a work action needs --as or ST_AGENT")?;
    let incarnation = match args.incarnation {
        Some(incarnation) => Some(incarnation),
        None => current_agent_incarnation(client, &actor).await?,
    };
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let response: StepRunView = client
        .post(
            &format!("/v1/work/{action}/{}", urlencoding::encode(&args.subject)),
            &WorkRequest {
                actor: Some(actor.clone()),
                incarnation,
                summary: args.summary,
                reason: args.reason,
                evidence: args.evidence,
                idempotency_key: format!("work:{action}:{}:{actor}:{nonce}", args.subject),
            },
        )
        .await?;
    if json_output {
        print_value(&response, true)
    } else {
        if action == "claim" {
            print!(
                "{}",
                render_step_run(&response, OutputStyle::stdout(), current_unix_ms()?)
            );
        } else {
            println!("{}\t{}", response.status, response.subject);
        }
        Ok(())
    }
}

async fn current_agent_incarnation(client: &Client, actor: &str) -> Result<Option<String>> {
    let subject = if actor.starts_with("agent/") {
        actor.to_owned()
    } else {
        format!("agent/{actor}")
    };
    let status: StatusResponse = client
        .get(&format!(
            "/v1/status?subject={}",
            urlencoding::encode(&subject)
        ))
        .await?;
    Ok(status
        .subjects
        .first()
        .and_then(|subject| subject.actual.as_ref())
        .and_then(|actual| actual.get("fields").unwrap_or(actual).get("incarnation_id"))
        .and_then(Value::as_str)
        .map(str::to_owned))
}

async fn run_message(client: &Client, command: MessageCommand, json_output: bool) -> Result<()> {
    sync_message_projection(client).await?;
    match command {
        MessageCommand::Send(args) => {
            let Some(message) = send_message(client, args).await? else {
                return Ok(());
            };
            sync_message_projection(client).await?;
            if json_output {
                print_value(&message, true)
            } else {
                println!("{}", message.subject.trim_start_matches("message/"));
                Ok(())
            }
        }
        MessageCommand::Ls(args) => {
            let identity = args.identity.unwrap_or_default();
            let mut path = if identity.is_empty() {
                "/v1/messages".to_owned()
            } else {
                format!("/v1/messages?to={}", urlencoding::encode(&identity))
            };
            if args.archive {
                path.push_str(if path.contains('?') {
                    "&include_closed=true"
                } else {
                    "?include_closed=true"
                });
            }
            let mut messages: Vec<MessageView> = client.get(&path).await?;
            if let Some(sender) = args.sender {
                let sender = if sender.contains('/') {
                    sender
                } else {
                    format!("agent/{sender}")
                };
                messages.retain(|message| message.from == sender);
            }
            if args.count {
                println!("{}", messages.len());
                return Ok(());
            }
            if json_output {
                return print_value(&messages, true);
            }
            for message in messages {
                println!(
                    "{}\t{}\t{}\t{}",
                    message.subject.trim_start_matches("message/"),
                    message.status,
                    message.from,
                    message.title.as_deref().unwrap_or("message")
                );
            }
            Ok(())
        }
        MessageCommand::Read(args) => {
            let (reference, actor) = positional_identity_and_reference(args.values, args.actor)?;
            let message = read_message(client, &reference).await?;
            accept_message(client, &message, actor.as_deref()).await?;
            if json_output {
                print_value(&message, true)?;
            } else if args.raw {
                print!("{}", message.content);
            } else {
                println!("From: {}", message.from);
                println!("To: {}", message.to);
                if let Some(title) = &message.title {
                    println!("Subject: {title}");
                }
                println!();
                println!("{}", message.content);
            }
            if args.archive {
                close_message(client, &reference, actor.as_deref()).await?;
            }
            sync_message_projection(client).await?;
            Ok(())
        }
        MessageCommand::Reply(args) => {
            let original = read_message(client, &args.reference).await?;
            let message = send_message(
                client,
                MessageSendArgs {
                    to: original.from,
                    body: args.body,
                    subject: args
                        .subject
                        .or(original.title.map(|title| format!("Re: {title}"))),
                    in_reply_to: Some(original.subject),
                    tags: Vec::new(),
                    from: args.from,
                    print_kdl: args.print_kdl,
                },
            )
            .await?;
            let Some(message) = message else {
                return Ok(());
            };
            if json_output {
                print_value(&message, true)
            } else {
                println!("{}", message.subject.trim_start_matches("message/"));
                Ok(())
            }
        }
        MessageCommand::Archive(args) => {
            let mut claims = Vec::with_capacity(args.references.len());
            for reference in args.references {
                let message = read_message(client, &reference).await?;
                accept_message(client, &message, args.actor.as_deref()).await?;
                claims.push(close_message(client, &reference, args.actor.as_deref()).await?);
            }
            sync_message_projection(client).await?;
            if json_output {
                if claims.len() == 1 {
                    print_value(&claims[0], true)
                } else {
                    print_value(&claims, true)
                }
            } else {
                Ok(())
            }
        }
        MessageCommand::Thread(args) => {
            let (reference, _) = positional_identity_and_reference(args.values, args.actor)?;
            let selected = read_message(client, &reference).await?;
            let all: Vec<MessageView> = client.get("/v1/messages?include_closed=true").await?;
            let root = thread_root(&selected, &all);
            let mut thread = all
                .iter()
                .filter(|message| thread_root(message, &all).subject == root.subject)
                .cloned()
                .collect::<Vec<_>>();
            thread.sort_by_key(|message| message.created_index);
            print_value(&thread, json_output)
        }
        MessageCommand::Export { directory } => {
            let messages: Vec<MessageView> = client.get("/v1/messages?include_closed=true").await?;
            st3::projection::export_messages(&directory, &messages)?;
            if json_output {
                print_value(
                    &json!({"directory": directory, "messages": messages.len()}),
                    true,
                )
            } else {
                println!(
                    "exported {} messages to {}",
                    messages.len(),
                    directory.display()
                );
                Ok(())
            }
        }
    }
}

async fn send_message(client: &Client, args: MessageSendArgs) -> Result<Option<MessageView>> {
    let id = uuid::Uuid::now_v7().simple().to_string();
    let mission_id = format!("message/{id}");
    let from = normalize_message_subject(&args.from);
    let to = normalize_message_subject(&args.to);
    let kdl = message_mission_intent(
        &mission_id,
        &id,
        &from,
        &to,
        &args.body,
        args.subject.as_deref(),
        args.in_reply_to.as_deref(),
        &args.tags,
    );
    if args.print_kdl {
        print!("{kdl}");
        return Ok(None);
    }
    let actor = from;
    let parsed = st3::parse_intent(&kdl, "local")?;
    let revision = parsed.missions[&mission_id].revision.clone();
    publish_text(
        client,
        kdl,
        format!("st3 message send {id} mission"),
        actor.clone(),
    )
    .await?;
    let workspace = std::env::current_dir()?.canonicalize()?;
    let run_kdl = mission_run_intent(
        &mission_id,
        &mission_id,
        &revision,
        &workspace,
        &normalize_requester_subject(&actor),
        &BTreeMap::new(),
        "run",
    );
    let applied =
        publish_text(client, run_kdl, format!("st3 message send {id} run"), actor).await?;
    let subject = format!("message/{id}");
    wait_for_message_view(client, &subject, applied.store_index).await?;
    read_message(client, &subject).await.map(Some)
}

#[allow(clippy::too_many_arguments)]
fn message_mission_intent(
    mission_id: &str,
    message_id: &str,
    from: &str,
    to: &str,
    content: &str,
    title: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
) -> String {
    let mut message = KdlNode::new("message");
    message.entries_mut().push(KdlEntry::new(message_id));
    let mut message_body = KdlDocument::new();
    message_body.nodes_mut().push(kdl_node("from", [from]));
    message_body.nodes_mut().push(kdl_node("to", [to]));
    message_body
        .nodes_mut()
        .push(kdl_node("content", [content]));
    if let Some(title) = title {
        message_body.nodes_mut().push(kdl_node("title", [title]));
    }
    if let Some(parent) = in_reply_to {
        message_body
            .nodes_mut()
            .push(kdl_node("in-reply-to", [parent]));
    }
    for tag in tags {
        message_body
            .nodes_mut()
            .push(kdl_node("tag", [tag.as_str()]));
    }
    message.set_children(message_body);

    let mut step = KdlNode::new("step");
    step.entries_mut().push(KdlEntry::new("send"));
    let mut step_body = KdlDocument::new();
    step_body.nodes_mut().push(KdlNode::new("agentless"));
    step_body.nodes_mut().push(message);
    step.set_children(step_body);

    let mut completion = KdlNode::new("completion");
    let mut completion_body = KdlDocument::new();
    completion_body
        .nodes_mut()
        .push(kdl_node("when", ["all-steps-exhausted"]));
    completion.set_children(completion_body);

    let mut mission = KdlNode::new("mission");
    mission.entries_mut().push(KdlEntry::new(mission_id));
    mission
        .entries_mut()
        .push(KdlEntry::new_prop("state", "ready"));
    let mut mission_body = KdlDocument::new();
    mission_body
        .nodes_mut()
        .push(kdl_node("goal", ["Deliver one message."]));
    mission_body.nodes_mut().push(step);
    mission_body.nodes_mut().push(completion);
    mission.set_children(mission_body);
    publication_document(mission)
}

async fn read_message(client: &Client, reference: &str) -> Result<MessageView> {
    let reference = normalize_message_reference(reference);
    client
        .get(&format!(
            "/v1/messages/read/{}",
            urlencoding::encode(&reference)
        ))
        .await
}

async fn accept_message(client: &Client, message: &MessageView, actor: Option<&str>) -> Result<()> {
    if message.status != "delivered" {
        return Ok(());
    }
    let reference = message.subject.trim_start_matches("message/");
    let _: ClaimRecord = client
        .post(
            &format!("/v1/messages/{}/claims", urlencoding::encode(reference)),
            &MessageLifecycleRequest {
                lifecycle: "read".into(),
                actor: actor.map(str::to_owned),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: format!("message-read:{}", message.subject),
            },
        )
        .await?;
    Ok(())
}

async fn deliver_message(
    client: &Client,
    reference: &str,
    actor: &str,
    idempotency_key: String,
) -> Result<()> {
    let reference = normalize_message_reference(reference);
    let _: ClaimRecord = client
        .post(
            &format!("/v1/messages/{}/claims", urlencoding::encode(&reference)),
            &MessageLifecycleRequest {
                lifecycle: "delivered".into(),
                actor: Some(actor.into()),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key,
            },
        )
        .await?;
    Ok(())
}

async fn close_message(
    client: &Client,
    reference: &str,
    actor: Option<&str>,
) -> Result<ClaimRecord> {
    let reference = normalize_message_reference(reference);
    client
        .post(
            &format!("/v1/messages/{}/claims", urlencoding::encode(&reference)),
            &MessageLifecycleRequest {
                lifecycle: "closed".into(),
                actor: actor.map(str::to_owned),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: format!("message-closed:{reference}"),
            },
        )
        .await
}

fn normalize_message_reference(reference: &str) -> String {
    let file_reference = reference.ends_with(".md");
    let reference = if file_reference {
        Path::new(reference)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(reference)
            .trim_end_matches(".md")
    } else {
        reference
    };
    let reference = if file_reference {
        reference.split_once('-').map_or(reference, |(_, id)| id)
    } else {
        reference
    };
    let reference = urlencoding::decode(reference).unwrap_or(std::borrow::Cow::Borrowed(reference));
    reference.trim_start_matches("message/").to_owned()
}

async fn sync_message_projection(client: &Client) -> Result<()> {
    let Some(root) = std::env::var_os("ST3_MESSAGE_ROOT") else {
        return Ok(());
    };
    let messages: Vec<MessageView> = client.get("/v1/messages?include_closed=true").await?;
    st3::projection::export_messages(Path::new(&root), &messages)
}

fn thread_root<'a>(message: &'a MessageView, all: &'a [MessageView]) -> &'a MessageView {
    let mut current = message;
    let mut seen = BTreeSet::new();
    while let Some(parent) = current.in_reply_to.as_deref() {
        if !seen.insert(parent) {
            break;
        }
        let normalized = if parent.starts_with("message/") {
            parent.to_owned()
        } else {
            format!("message/{parent}")
        };
        let Some(next) = all.iter().find(|candidate| candidate.subject == normalized) else {
            break;
        };
        current = next;
    }
    current
}

async fn run_gate_result(client: &Client, args: GateResultArgs, json_output: bool) -> Result<()> {
    let idempotency_key = gate_result_idempotency_key(
        &args.operation_capability,
        &args.verdict,
        &args.reason,
        &args.evidence,
    );
    let response: ClaimRecord = client
        .post(
            "/v1/gate-results",
            &GateResultRequest {
                idempotency_key,
                operation_capability: args.operation_capability,
                verdict: args.verdict,
                reason: args.reason,
                evidence: args.evidence,
            },
        )
        .await?;
    print_value(&response, json_output)
}

fn gate_result_idempotency_key(
    operation_capability: &str,
    verdict: &str,
    reason: &str,
    evidence: &[String],
) -> String {
    let request_bytes = serde_json::to_vec(&(operation_capability, verdict, reason, evidence))
        .expect("a gate result request is JSON serializable");
    format!("gate-result:{}", hex::encode(Sha256::digest(request_bytes)))
}

async fn run_eval(client: &Client, args: EvalArgs, json_output: bool) -> Result<()> {
    anyhow::ensure!(
        !(args.graph && json_output),
        "--graph and --json cannot be used together"
    );
    anyhow::ensure!(
        !args.graph || std::io::stdout().is_terminal(),
        "--graph needs an interactive terminal"
    );
    if args.print_kdl {
        anyhow::ensure!(
            !args.graph,
            "--print-kdl and --graph cannot be used together"
        );
        let root = args
            .eval
            .canonicalize()
            .with_context(|| format!("resolve eval directory {}", args.eval.display()))?;
        let source = fs::read_to_string(root.join("eval.kdl"))?;
        let source = source.replace("${EVAL_ROOT}", &root.to_string_lossy());
        st3::parse_intent(&source, "local")?;
        print!("{source}");
        return Ok(());
    }
    let bundle = archive_eval(&args.eval)?;
    let bundle_hash = hex::encode(Sha256::digest(&bundle));
    let name = args
        .eval
        .file_name()
        .and_then(|name| name.to_str())
        .context("the eval name is not UTF-8")?
        .to_owned();
    let started: EvalStartResponse = client
        .post(
            "/v1/evals",
            &EvalStartRequest {
                name,
                bundle_hash,
                bundle,
                inputs: unique_pairs(args.inputs, "input")?,
            },
        )
        .await?;
    if json_output {
        print_value(&started, true)?;
    } else {
        println!("started {}", started.mission_run);
    }
    let cursor = started.event_cursor;
    let subject = started.mission_run;
    let run: MissionRunView = client
        .get(&format!(
            "/v1/mission-runs/{}",
            urlencoding::encode(&subject)
        ))
        .await?;
    if args.graph {
        follow_eval_graph(client, &run.subject).await
    } else {
        follow_mission_run(client, run, cursor, json_output).await
    }
}

async fn run_graph(client: &Client, args: GraphArgs, json_output: bool) -> Result<()> {
    anyhow::ensure!(!json_output, "graph and --json cannot be used together");
    anyhow::ensure!(
        std::io::stdout().is_terminal(),
        "graph needs an interactive terminal"
    );
    let eval: EvalStatus = client
        .get(&format!(
            "/v1/evals/{}",
            urlencoding::encode(&args.mission_run)
        ))
        .await?;
    follow_eval_graph(client, &eval.mission_run).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphNodeState {
    label: String,
    state: String,
    assignee: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphTransition {
    elapsed: Duration,
    label: String,
    from: String,
    to: String,
    assignee: Option<String>,
}

struct EvalGraphSnapshot {
    eval: EvalStatus,
    runs: Vec<MissionRunView>,
}

struct TerminalScreen;

impl TerminalScreen {
    fn open() -> Result<Self> {
        print!("\x1b[?25l");
        std::io::stdout().flush()?;
        Ok(Self)
    }
}

impl Drop for TerminalScreen {
    fn drop(&mut self) {
        print!("\x1b[?25h");
        let _ = std::io::stdout().flush();
    }
}

async fn follow_eval_graph(client: &Client, root: &str) -> Result<()> {
    let _screen = TerminalScreen::open()?;
    let started_at = Instant::now();
    let mut previous = BTreeMap::new();
    let mut transitions = Vec::new();
    let mut prior_signature = String::new();
    loop {
        let snapshot = load_eval_graph(client, root).await?;
        let current = graph_node_states(&snapshot);
        if !previous.is_empty() {
            record_graph_transitions(&previous, &current, started_at.elapsed(), &mut transitions);
        }
        let signature = format!(
            "{current:?}|{}|{:?}",
            snapshot.eval.cleanup, snapshot.eval.verdict
        );
        if signature != prior_signature {
            let frame = render_eval_graph(&snapshot, &transitions, started_at.elapsed());
            print!("\x1b[2J\x1b[H{frame}");
            std::io::stdout().flush()?;
            prior_signature = signature;
        }
        previous = current;
        match snapshot.eval.lifecycle.as_str() {
            "completed" => return Ok(()),
            "failed" | "cancelled" => {
                anyhow::bail!(
                    "eval {} is {}",
                    snapshot.eval.mission_run,
                    snapshot.eval.lifecycle
                )
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn load_eval_graph(client: &Client, root: &str) -> Result<EvalGraphSnapshot> {
    let eval: EvalStatus = client
        .get(&format!("/v1/evals/{}", urlencoding::encode(root)))
        .await?;
    let runs: Vec<MissionRunView> = client
        .get(&format!(
            "/v1/mission-runs?root={}",
            urlencoding::encode(root)
        ))
        .await?;
    anyhow::ensure!(!runs.is_empty(), "the eval graph has no mission runs");
    Ok(EvalGraphSnapshot { eval, runs })
}

fn graph_node_states(snapshot: &EvalGraphSnapshot) -> BTreeMap<String, GraphNodeState> {
    let mut states = BTreeMap::new();
    states.insert(
        snapshot.eval.mission_run.clone(),
        GraphNodeState {
            label: "eval".into(),
            state: format!("{} / {}", snapshot.eval.lifecycle, snapshot.eval.phase),
            assignee: None,
        },
    );
    for run in &snapshot.runs {
        states.insert(
            run.subject.clone(),
            GraphNodeState {
                label: run
                    .mission
                    .strip_prefix("mission/")
                    .unwrap_or(&run.mission)
                    .to_owned(),
                state: format!("{} / {}", run.status, run.phase),
                assignee: None,
            },
        );
        for step in &run.steps {
            let attempt = if step.attempt > 1 {
                format!(" (attempt {})", step.attempt)
            } else {
                String::new()
            };
            states.insert(
                step.subject.clone(),
                GraphNodeState {
                    label: step.title.clone().unwrap_or_else(|| step.step.clone()),
                    state: format!("{}{attempt}", step.status),
                    assignee: work_actor(step).map(short_actor).map(str::to_owned),
                },
            );
        }
    }
    states
}

fn record_graph_transitions(
    previous: &BTreeMap<String, GraphNodeState>,
    current: &BTreeMap<String, GraphNodeState>,
    elapsed: Duration,
    transitions: &mut Vec<GraphTransition>,
) {
    for (subject, state) in current {
        let Some(prior) = previous.get(subject) else {
            transitions.push(GraphTransition {
                elapsed,
                label: state.label.clone(),
                from: "created".into(),
                to: state.state.clone(),
                assignee: state.assignee.clone(),
            });
            continue;
        };
        if prior.state != state.state {
            transitions.push(GraphTransition {
                elapsed,
                label: state.label.clone(),
                from: prior.state.clone(),
                to: state.state.clone(),
                assignee: state.assignee.clone(),
            });
        }
    }
    for (subject, state) in previous {
        if !current.contains_key(subject) {
            transitions.push(GraphTransition {
                elapsed,
                label: state.label.clone(),
                from: state.state.clone(),
                to: "removed".into(),
                assignee: state.assignee.clone(),
            });
        }
    }
    if transitions.len() > 12 {
        transitions.drain(..transitions.len() - 12);
    }
}

fn render_eval_graph(
    snapshot: &EvalGraphSnapshot,
    transitions: &[GraphTransition],
    elapsed: Duration,
) -> String {
    use std::fmt::Write as _;

    let mut output = String::new();
    let name = snapshot
        .eval
        .mission_run
        .strip_prefix("mission-run/")
        .unwrap_or(&snapshot.eval.mission_run);
    let steps = snapshot
        .runs
        .iter()
        .flat_map(|run| run.steps.iter())
        .collect::<Vec<_>>();
    let completed = steps
        .iter()
        .filter(|step| step.status == "completed")
        .count();
    let active = steps
        .iter()
        .filter(|step| is_active_graph_state(&step.status))
        .count();
    let blocked = steps.iter().filter(|step| step.status == "blocked").count();
    let verdict = snapshot.eval.verdict.as_deref().unwrap_or("pending");
    let _ = writeln!(output, "ST3 EVAL GRAPH  {name}");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "STATE      {} · {}",
        snapshot.eval.lifecycle, snapshot.eval.phase
    );
    let _ = writeln!(output, "VERDICT    {verdict}");
    let _ = writeln!(output, "CLEANUP    {}", snapshot.eval.cleanup);
    let _ = writeln!(
        output,
        "PROGRESS   {completed}/{} completed · {active} active · {blocked} blocked",
        steps.len()
    );
    let _ = writeln!(output, "ELAPSED    {}", format_elapsed(elapsed));
    let _ = writeln!(output);
    let _ = writeln!(output, "WORK GRAPH");

    let children = snapshot
        .runs
        .iter()
        .filter_map(|run| run.parent_step_run.as_deref().map(|parent| (parent, run)))
        .collect::<BTreeMap<_, _>>();
    if let Some(root) = snapshot
        .runs
        .iter()
        .find(|run| run.subject == snapshot.eval.mission_run)
    {
        render_mission_steps(&mut output, root, &children, "  ");
    } else {
        let _ = writeln!(output, "  ! the root mission run is not available");
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "TRANSITIONS");
    if transitions.is_empty() {
        let _ = writeln!(output, "  Waiting for a state change.");
    } else {
        for transition in transitions {
            let actor = transition
                .assignee
                .as_deref()
                .map(|actor| format!(" · {actor}"))
                .unwrap_or_default();
            let _ = writeln!(
                output,
                "  {}  {}: {} → {}{}",
                format_elapsed(transition.elapsed),
                transition.label,
                transition.from,
                transition.to,
                actor
            );
        }
    }
    output
}

fn render_mission_steps(
    output: &mut String,
    run: &MissionRunView,
    children: &BTreeMap<&str, &MissionRunView>,
    indent: &str,
) {
    use std::fmt::Write as _;

    let base_depth = run
        .steps
        .iter()
        .map(|step| step.step.matches('/').count())
        .min()
        .unwrap_or_default();
    for step in run
        .steps
        .iter()
        .filter(|step| step.step.matches('/').count() == base_depth)
    {
        render_graph_step(output, step, indent);
        let nested_prefix = format!("{}/", step.step);
        let nested = run
            .steps
            .iter()
            .filter(|candidate| candidate.step.starts_with(&nested_prefix))
            .collect::<Vec<_>>();
        if !nested.is_empty() {
            let nested_completed = nested
                .iter()
                .filter(|candidate| candidate.status == "completed")
                .count();
            let _ = writeln!(
                output,
                "{indent}  ↳ nested work · {nested_completed}/{} completed",
                nested.len()
            );
            if is_active_graph_state(&step.status) || step.status == "failed" {
                for nested_step in nested {
                    let relative_depth = nested_step
                        .step
                        .matches('/')
                        .count()
                        .saturating_sub(base_depth);
                    render_graph_step(
                        output,
                        nested_step,
                        &format!("{indent}{}", "  ".repeat(relative_depth + 1)),
                    );
                }
            }
        }
        let Some(child) = children.get(step.subject.as_str()) else {
            continue;
        };
        let child_completed = child
            .steps
            .iter()
            .filter(|nested| nested.status == "completed")
            .count();
        let child_summary = format!(
            "{indent}  ↳ {} · {} · {child_completed}/{} completed",
            child
                .mission
                .strip_prefix("mission/")
                .unwrap_or(&child.mission),
            child.status,
            child.steps.len()
        );
        let _ = writeln!(output, "{child_summary}");
        if !matches!(child.status.as_str(), "completed" | "cancelled") {
            render_mission_steps(output, child, children, &format!("{indent}    "));
        }
    }
}

fn render_graph_step(output: &mut String, step: &StepRunView, indent: &str) {
    use std::fmt::Write as _;

    let actor = work_actor(step)
        .map(short_actor)
        .map(|actor| format!(" · {actor}"))
        .unwrap_or_default();
    let title = step.title.as_deref().unwrap_or(&step.step);
    let attempt = if step.attempt > 1 {
        format!(" · attempt {}", step.attempt)
    } else {
        String::new()
    };
    let queue = step
        .queue
        .as_deref()
        .zip(step.queue_position)
        .map(|(queue, position)| format!(" · queue {queue} #{position}"))
        .unwrap_or_default();
    let _ = writeln!(
        output,
        "{indent}{} {:<10} {} — {}{}{}{}",
        graph_state_mark(&step.status),
        step.status,
        step.step.rsplit('/').next().unwrap_or(&step.step),
        title,
        actor,
        attempt,
        queue
    );
    if let Some(reason) = &step.blocked_reason {
        let _ = writeln!(output, "{indent}  reason: {reason}");
    }
}

fn graph_state_mark(status: &str) -> &'static str {
    match status {
        "completed" => "✓",
        "working" | "verifying" => "▶",
        "claimed" => "◉",
        "ready" => "●",
        "blocked" => "!",
        "failed" => "✗",
        "cancelled" => "×",
        _ => "·",
    }
}

fn is_active_graph_state(status: &str) -> bool {
    matches!(
        status,
        "ready" | "claimed" | "working" | "verifying" | "blocked"
    )
}

fn short_actor(actor: &str) -> &str {
    actor.strip_prefix("agent/").unwrap_or(actor)
}

fn work_actor(step: &StepRunView) -> Option<&str> {
    step.claimant
        .as_deref()
        .or(step.assigned_to.as_deref())
        .or_else(|| (step.available_to.len() == 1).then(|| step.available_to[0].as_str()))
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!("{:02}:{:02}", seconds / 60, seconds % 60)
}

async fn run_quick(
    client: &Client,
    endpoint: Endpoint,
    config: &Config,
    args: QuickArgs,
    driver: &str,
    json_output: bool,
) -> Result<()> {
    let health: Value = client.get("/v1/health").await?;
    let node = health
        .get("node")
        .and_then(Value::as_str)
        .context("health lacks node")?;
    let name = args.name.unwrap_or_else(generated_name);
    let bus_id = if name.contains('.') {
        name
    } else {
        format!("{node}.{name}")
    };
    let worktree = fs::canonicalize(&args.worktree)
        .with_context(|| format!("resolve worktree {}", args.worktree.display()))?;
    let mission_id = format!("standing/{bus_id}");
    let kdl = quick_agent_intent(
        &mission_id,
        &bus_id,
        &worktree,
        driver,
        args.model.as_deref(),
        args.effort.as_deref(),
    );
    if args.print_kdl {
        print!("{kdl}");
        return Ok(());
    }
    let actor = args.actor.context("a quick agent needs --as or ST_AGENT")?;
    let parsed = st3::parse_intent(&kdl, node)?;
    let mission = parsed.missions[&mission_id].clone();
    let mission_publication = publish_text(
        client,
        kdl,
        format!("st3 {driver} {bus_id} mission"),
        actor.clone(),
    )
    .await?;
    let mut active: Vec<MissionRunView> = client
        .get(&format!(
            "/v1/mission-runs?mission={}",
            urlencoding::encode(&mission_id)
        ))
        .await?;
    anyhow::ensure!(
        active.len() <= 1,
        "standing mission `mission/{mission_id}` has more than one active run"
    );
    let mut cursor = mission_publication.store_index;
    let run = if let Some(current) = active.pop() {
        if current.revision != mission.revision {
            let revision_id = format!("quick-{}", uuid::Uuid::now_v7().simple());
            let revision_kdl = mission_revision_intent(
                &current.subject,
                &revision_id,
                &mission_id,
                &mission.revision,
                &current.generation,
                "the quick agent declaration changed",
            );
            let applied = publish_text(
                client,
                revision_kdl,
                format!("st3 {driver} {bus_id} revision"),
                actor.clone(),
            )
            .await?;
            cursor = cursor.max(applied.store_index);
            client
                .get::<MissionRunView>(&format!(
                    "/v1/mission-runs/{}",
                    urlencoding::encode(&current.subject)
                ))
                .await?
        } else {
            current
        }
    } else {
        let run_id = mission_id.clone();
        let run_kdl = mission_run_intent(
            &run_id,
            &mission_id,
            &mission.revision,
            &worktree,
            &normalize_requester_subject(&actor),
            &BTreeMap::new(),
            "run",
        );
        let applied = publish_text(
            client,
            run_kdl,
            format!("st3 {driver} {bus_id} run"),
            actor.clone(),
        )
        .await?;
        cursor = cursor.max(applied.store_index);
        client
            .get::<MissionRunView>(&format!(
                "/v1/mission-runs/{}",
                urlencoding::encode(&format!("mission-run/{run_id}"))
            ))
            .await?
    };
    let subject = format!("agent/{}/{bus_id}", run.id);
    let status: StatusResponse = client
        .get(&format!(
            "/v1/status?subject={}",
            urlencoding::encode(&subject)
        ))
        .await?;
    let ready = status.subjects.first().is_some_and(|subject| {
        subject
            .actual
            .as_ref()
            .map(|actual| actual.get("fields").unwrap_or(actual))
            .and_then(|actual| actual.get("state"))
            .and_then(Value::as_str)
            == Some("ready")
    });
    let created = QuickAgentResponse {
        subject,
        mission: format!("mission/{mission_id}"),
        mission_run: run.subject,
        generation: run.generation,
        runtime_id: format!("{}.{}", run.id.replace('/', "."), bus_id.replace('/', ".")),
        event_cursor: cursor,
        incarnation_id: None,
        ready,
    };
    if json_output {
        print_value(&created, true)?;
    } else {
        println!("waiting for {}", created.subject);
    }
    let mut cursor = created.event_cursor;
    if !created.ready {
        loop {
            let events: Vec<EventRecord> = client
                .get(&format!(
                    "/v1/events?after={cursor}&subject={}",
                    urlencoding::encode(&created.subject)
                ))
                .await?;
            let mut ready = false;
            for event in events {
                cursor = cursor.max(event.store_index);
                if (event.kind == "harness.observed"
                    && event.body.pointer("/fields/state").and_then(Value::as_str) == Some("ready"))
                    || (event.kind == "runtime.observed"
                        && event.body.pointer("/fields/status").and_then(Value::as_str)
                            == Some("ready"))
                {
                    ready = true;
                }
                if matches!(
                    event.kind.as_str(),
                    "harness.diagnostic" | "daemon.diagnostic" | "runtime.action.failed"
                ) {
                    anyhow::bail!("{} became unreachable: {}", created.subject, event.body);
                }
            }
            if ready {
                break;
            }
        }
    }
    let attachment: Attachment = client
        .post(
            &format!("/v1/sessions/attach/{}", created.subject),
            &AttachRequest::default(),
        )
        .await?;
    let _ = endpoint;
    let _ = config;
    client.proxy_terminal(&attachment.websocket_path).await
}

fn quick_agent_intent(
    mission_id: &str,
    agent_id: &str,
    worktree: &Path,
    driver: &str,
    model: Option<&str>,
    effort: Option<&str>,
) -> String {
    let mut harness_body = KdlDocument::new();
    if let Some(model) = model {
        harness_body.nodes_mut().push(kdl_node("model", [model]));
    }
    if let Some(effort) = effort {
        harness_body.nodes_mut().push(kdl_node("effort", [effort]));
    }
    if driver == "claude" {
        let mut development = KdlNode::new("dev-channels");
        development.entries_mut().push(KdlEntry::new(true));
        harness_body.nodes_mut().push(development);
    }
    let mut harness = KdlNode::new("harness");
    harness.entries_mut().push(KdlEntry::new(driver));
    harness.set_children(harness_body);

    let mut agent = KdlNode::new("agent");
    agent.entries_mut().push(KdlEntry::new(agent_id));
    let mut agent_body = KdlDocument::new();
    agent_body
        .nodes_mut()
        .push(kdl_node("identity", [agent_id]));
    agent_body
        .nodes_mut()
        .push(kdl_node("workspace", [worktree.to_string_lossy().as_ref()]));
    agent_body.nodes_mut().push(harness);
    agent.set_children(agent_body);

    let mut mission = KdlNode::new("mission");
    mission.entries_mut().push(KdlEntry::new(mission_id));
    mission
        .entries_mut()
        .push(KdlEntry::new_prop("state", "ready"));
    let mut mission_body = KdlDocument::new();
    mission_body.nodes_mut().push(kdl_node(
        "goal",
        ["Keep the agent ready for work and conversation."],
    ));
    mission_body.nodes_mut().push(agent);
    mission.set_children(mission_body);
    publication_document(mission)
}

fn mission_revision_intent(
    run: &str,
    operation_id: &str,
    mission_id: &str,
    revision: &str,
    from_generation: &str,
    reason: &str,
) -> String {
    format!(
        "version 2\nmission-run {run:?} {{\n  revision {operation_id:?} {{\n    mission {:?}\n    from {from_generation:?}\n    reason {reason:?}\n  }}\n}}\n",
        format!("mission/{mission_id}@{revision}")
    )
}

fn planning_session_intent(
    session_id: &str,
    mission_id: &str,
    request: &str,
    workspace: &Path,
    requester: &str,
    model: Option<&str>,
    effort: Option<&str>,
    target: Option<&MissionRunView>,
) -> String {
    let mut session = KdlNode::new("planning-session");
    session.entries_mut().push(KdlEntry::new(session_id));
    let mut body = KdlDocument::new();
    body.nodes_mut().push(kdl_node("mission", [mission_id]));
    body.nodes_mut().push(kdl_node("request", [request]));
    body.nodes_mut().push(kdl_node(
        "workspace",
        [workspace.to_string_lossy().as_ref()],
    ));
    body.nodes_mut().push(kdl_node("requester", [requester]));
    let mut planner = KdlNode::new("planner");
    planner.entries_mut().push(KdlEntry::new("codex"));
    let mut planner_body = KdlDocument::new();
    if let Some(model) = model {
        planner_body.nodes_mut().push(kdl_node("model", [model]));
    }
    if let Some(effort) = effort {
        planner_body.nodes_mut().push(kdl_node("effort", [effort]));
    }
    planner.set_children(planner_body);
    body.nodes_mut().push(planner);
    if let Some(target) = target {
        body.nodes_mut()
            .push(kdl_node("target-run", [target.subject.as_str()]));
        body.nodes_mut()
            .push(kdl_node("target-generation", [target.generation.as_str()]));
    }
    session.set_children(body);
    publication_document(session)
}

fn planning_feedback_intent(
    session_id: &str,
    operation_id: &str,
    document: &str,
    variant: &str,
) -> String {
    format!(
        "version 2\nplanning-session {session_id:?} {{\n  feedback {operation_id:?} {{\n    document {document:?}\n    variant {variant:?}\n  }}\n}}\n"
    )
}

fn planning_cancellation_intent(session_id: &str, operation_id: &str, reason: &str) -> String {
    format!(
        "version 2\nplanning-session {session_id:?} {{\n  cancellation {operation_id:?} {{\n    reason {reason:?}\n  }}\n}}\n"
    )
}

fn normalize_planning_requester(actor: &str) -> Result<String> {
    let actor = if actor.starts_with("person/") {
        actor.to_owned()
    } else if actor.contains('/') {
        actor.to_owned()
    } else {
        format!("person/{actor}")
    };
    anyhow::ensure!(
        actor.starts_with("person/"),
        "a planning requester must be a person subject"
    );
    Ok(actor)
}

async fn run_driver(client: &Client, args: DriverArgs, catalog: Option<&Path>) -> Result<()> {
    if args.driver == "ding" {
        anyhow::ensure!(
            args.argv.is_empty(),
            "the DING driver takes no provider argv"
        );
        let target = std::env::var("ST_AGENT").context("the DING exec has no owning ST_AGENT")?;
        return run_ding_driver(client, &normalize_agent_subject(&target)).await;
    }
    if args.driver == "pi-channel" {
        let identity = args
            .identity
            .as_deref()
            .context("the Pi channel has no identity")?;
        let _ = catalog.context("the Pi channel has no native driver catalog")?;
        anyhow::ensure!(
            args.argv.is_empty(),
            "the Pi channel takes no provider argv"
        );
        return run_pi_channel(client, &normalize_agent_subject(identity)).await;
    }
    let subject = args
        .subject
        .as_deref()
        .context("the driver has no subject")?;
    if args.driver == "claude-mcp" {
        anyhow::ensure!(
            args.argv.is_empty(),
            "the Claude MCP driver takes no provider argv"
        );
        return run_claude_mcp(client, &normalize_agent_subject(subject)).await;
    }
    if args.driver == "codex" {
        return run_codex_native(client, subject, args.argv).await;
    }
    if matches!(args.driver.as_str(), "claude" | "pi" | "omp" | "opencode") {
        return run_st2_native_driver(client, subject, &args.driver, args.argv).await;
    }
    let (program, arguments) = args.argv.split_first().context("driver argv is empty")?;
    let mut child = tokio::process::Command::new(program)
        .args(arguments)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("start {} provider", args.driver))?;
    let status = child.wait().await?;
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt as _;
        status.signal()
    };
    let _: ClaimRecord = client
        .post(
            "/v1/claims",
            &ClaimInput {
                subject: subject.into(),
                kind: "runtime.observed".into(),
                actor: Some(subject.into()),
                fields: BTreeMap::from([
                    ("status".into(), Value::String("exited".into())),
                    (
                        "exit_code".into(),
                        status.code().map(Value::from).unwrap_or(Value::Null),
                    ),
                    (
                        "exit_signal".into(),
                        signal.map(Value::from).unwrap_or(Value::Null),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            },
        )
        .await?;
    if args.driver == "exec" {
        let code = status
            .code()
            .unwrap_or_else(|| 128_i32.saturating_add(signal.unwrap_or(1)))
            .clamp(0, 255) as u8;
        if code != 0 {
            return Err(CommandExit(code).into());
        }
        return Ok(());
    }
    anyhow::ensure!(status.success(), "{} exited with {status}", args.driver);
    Ok(())
}

async fn run_ding_driver(client: &Client, target: &str) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let status: StatusResponse = client
            .get(&format!(
                "/v1/status?subject={}",
                urlencoding::encode(target)
            ))
            .await?;
        let Some(agent) = status.subjects.first() else {
            anyhow::bail!("DING target `{target}` does not exist");
        };
        let incarnation = agent
            .actual
            .as_ref()
            .and_then(|actual| actual.get("incarnation_id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(incarnation) = incarnation {
            let messages: Vec<MessageView> = client
                .get(&format!("/v1/messages?to={}", urlencoding::encode(target)))
                .await?;
            for message in messages
                .into_iter()
                .filter(|message| message.status == "sent")
            {
                let id = message.subject.trim_start_matches("message/");
                let wake = format!(
                    "[DING] new st3 message: [id:{id}] {} (from {}); run `st3 message ls`",
                    message.title.as_deref().unwrap_or("message"),
                    message.from
                );
                let _: SessionControlResponse = client
                    .post(
                        &format!("/v1/sessions/input/{}", urlencoding::encode(target)),
                        &SessionInputRequest {
                            expected_incarnation: incarnation.clone(),
                            mode: SessionInputMode::Line,
                            value: wake,
                            idempotency_key: format!(
                                "ding-input:{}:{incarnation}",
                                message.subject
                            ),
                        },
                    )
                    .await?;
                deliver_message(
                    client,
                    &message.subject,
                    target,
                    format!("ding-delivered:{}:{incarnation}", message.subject),
                )
                .await?;
            }
        }
    }
}

async fn run_st2_native_driver(
    client: &Client,
    subject: &str,
    driver: &str,
    argv: Vec<String>,
) -> Result<()> {
    anyhow::ensure!(!argv.is_empty(), "the {driver} driver argv is empty");
    let (catalog, agent_dir, identity, runtime_id) = prepare_native_driver(subject)?;
    let argv = if driver == "claude" {
        prepare_st3_claude_channel_argv(subject, argv)?
    } else {
        argv
    };
    let task_catalog = catalog.clone();
    let task_identity = identity.clone();
    let task_runtime = runtime_id.clone();
    let task_driver = driver.to_owned();
    let mut task = tokio::task::spawn_blocking(move || match task_driver.as_str() {
        "claude" => st2::claude_session::run(&task_catalog, task_identity, task_runtime, argv),
        "pi" => st2::pi_session::run(&task_catalog, task_identity, task_runtime, argv),
        "omp" => st2::omp_session::run(&task_catalog, task_identity, task_runtime, argv),
        "opencode" => st2::opencode_session::run(&task_catalog, task_identity, task_runtime, argv),
        _ => unreachable!("the native driver was checked"),
    });
    let inbox = st2::message::inbox_dir(&agent_dir);
    let archive = st2::message::archive_dir(&agent_dir);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut work_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    work_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewed_minute = None;
    let mut last_activity_fingerprint = None;
    let mut ready = false;
    loop {
        tokio::select! {
            result = &mut task => {
                let outcome = result?;
                let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                    subject: subject.into(),
                    kind: "runtime.observed".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("exited".into())),
                        ("runtime_id".into(), Value::String(runtime_id.clone())),
                        ("exit_code".into(), Value::from(if outcome.is_ok() { 0 } else { 1 })),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                }).await?;
                return outcome;
            }
            _ = interval.tick() => {
                if let Some(observed) = st2::harness_state::read(
                    &st2::harness_state::harness_state_path(&agent_dir),
                    None,
                ) {
                    if !ready
                        && driver != "claude"
                        && !matches!(
                            observed.state,
                            st2::harness_state::Activity::Unknown
                                | st2::harness_state::Activity::Ended
                        )
                    {
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: subject.into(),
                            kind: "harness.observed".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("state".into(), Value::String("ready".into())),
                                ("driver".into(), Value::String(driver.into())),
                                ("transport".into(), Value::String("native".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("native-ready:{subject}:{driver}")),
                        }).await?;
                        ready = true;
                    }
                    publish_harness_activity(
                        client,
                        subject,
                        driver,
                        &observed,
                        &mut last_activity_fingerprint,
                    )
                    .await?;
                }
                if driver != "claude" {
                    forward_projected_messages(client, subject, &inbox, &archive, "native").await?;
                }
            }
            _ = work_interval.tick(), if driver != "claude" => {
                sync_work_messages(client, subject).await?;
                let minute = unix_minute()?;
                if renewed_minute != Some(minute) {
                    renew_claimed_work(client, subject, minute).await?;
                    renewed_minute = Some(minute);
                }
            }
        }
    }
}

fn prepare_st3_claude_channel_argv(subject: &str, argv: Vec<String>) -> Result<Vec<String>> {
    let uses_channel = argv
        .windows(2)
        .any(|pair| pair[0] == "--channels" && pair[1] == st2::claude_channel::ST3_CHANNEL);
    if !uses_channel {
        return Ok(argv);
    }
    match st2::claude_channel::verify_st3_installed() {
        Ok(()) => Ok(argv),
        Err(error) => {
            eprintln!(
                "warning: the approved st3 Claude channel plugin is unavailable: {error:#}\n\
                 warning: using Claude's interactive development channel; Claude can ask for confirmation\n\
                 warning: run `st3 claude-channel install` for unattended startup"
            );
            let executable = std::env::current_exe()
                .context("resolving the st3 executable for the Claude development channel")?;
            st3_development_channel_argv(argv, &executable, subject)
        }
    }
}

fn st3_development_channel_argv(
    argv: Vec<String>,
    executable: &Path,
    subject: &str,
) -> Result<Vec<String>> {
    let mcp = serde_json::json!({
        "mcpServers": {
            "st3": {
                "type": "stdio",
                "command": executable,
                "args": ["driver", "claude-mcp", "--subject", subject]
            }
        }
    });
    let mut output = Vec::with_capacity(argv.len() + 2);
    let mut index = 0;
    let mut replaced = false;
    while index < argv.len() {
        if !replaced
            && argv[index] == "--channels"
            && argv.get(index + 1).map(String::as_str) == Some(st2::claude_channel::ST3_CHANNEL)
        {
            output.extend([
                "--mcp-config".to_string(),
                mcp.to_string(),
                "--strict-mcp-config".to_string(),
                "--dangerously-load-development-channels=server:st3".to_string(),
            ]);
            replaced = true;
            index += 2;
            continue;
        }
        output.push(argv[index].clone());
        index += 1;
    }
    anyhow::ensure!(
        replaced,
        "the st3 Claude plugin channel selector is missing"
    );
    Ok(output)
}

fn prepare_native_driver(subject: &str) -> Result<(PathBuf, PathBuf, String, String)> {
    let state_root = PathBuf::from(
        std::env::var_os("ST3_DRIVER_STATE_DIR")
            .context("the native driver has no ST3_DRIVER_STATE_DIR")?,
    );
    prepare_native_driver_in(subject, &state_root)
}

fn prepare_native_driver_in(
    subject: &str,
    state_root: &Path,
) -> Result<(PathBuf, PathBuf, String, String)> {
    let state_root = state_root.join(&hex::encode(Sha256::digest(subject.as_bytes()))[..24]);
    let catalog = state_root.join("catalog");
    let identity = subject.strip_prefix("agent/").unwrap_or(subject).to_owned();
    let host = st2::run::detect_host();
    let leaf = &hex::encode(Sha256::digest(identity.as_bytes()))[..16];
    let agent_dir = catalog.join("agents").join(&host).join(leaf);
    fs::create_dir_all(&agent_dir)?;
    let workspace = std::env::current_dir()?;
    let declaration = format!(
        "agent {identity:?} {{\n  identity {identity:?}\n  host {host:?}\n  workspace {:?}\n  command \"true\"\n}}\n",
        workspace.to_string_lossy()
    );
    fs::write(agent_dir.join("agent.kdl"), declaration)?;
    Ok((catalog, agent_dir, identity.clone(), identity))
}

fn harness_activity_state(activity: st2::harness_state::Activity) -> &'static str {
    match activity {
        st2::harness_state::Activity::Idle => "idle",
        st2::harness_state::Activity::Active | st2::harness_state::Activity::Child => "working",
        st2::harness_state::Activity::Ended => "ended",
        st2::harness_state::Activity::Unknown => "indeterminate",
    }
}

async fn publish_harness_activity(
    client: &Client,
    subject: &str,
    driver: &str,
    observed: &st2::harness_state::Observed,
    last_fingerprint: &mut Option<String>,
) -> Result<()> {
    let status = harness_activity_state(observed.state);
    let fields = BTreeMap::from([
        ("state".into(), Value::String(status.into())),
        ("driver".into(), Value::String(driver.into())),
        (
            "blocked_on".into(),
            Value::String(observed.blocked_on.as_str().into()),
        ),
        ("ask".into(), Value::String(observed.ask.as_str().into())),
        (
            "input_buffer".into(),
            Value::String(observed.input_buffer.as_str().into()),
        ),
        (
            "reason".into(),
            observed
                .reason
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "exit".into(),
            observed
                .exit
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
    ]);
    let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&(
        observed.since_ms,
        &fields,
    ))?));
    if last_fingerprint.as_deref() == Some(fingerprint.as_str()) {
        return Ok(());
    }
    let _: ClaimRecord = client
        .post(
            "/v1/claims",
            &ClaimInput {
                subject: subject.into(),
                kind: "harness.observed".into(),
                actor: Some(subject.into()),
                fields,
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("native-activity:{subject}:{fingerprint}")),
            },
        )
        .await?;
    *last_fingerprint = Some(fingerprint);
    Ok(())
}

async fn run_pi_channel(client: &Client, subject: &str) -> Result<()> {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    let identity = subject.strip_prefix("agent/").unwrap_or(subject);
    let context_name = format!("doc/context/{identity}/now");
    let context = latest_document_text(client, &context_name)
        .await?
        .unwrap_or_default();
    let ritual = "Run the st3 boot ritual now. Set your status to available. Drain and archive your graph message inbox. Set busy before work.";
    let session_context = if context.trim().is_empty() {
        ritual.into()
    } else {
        format!(
            "<context source=\"st3/context/now.md\" agent=\"{identity}\">\n{}\n</context>\n\n{ritual}",
            context.trim_end()
        )
    };
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(
            format!(
                "{}\n",
                serde_json::to_string(&json!({
                    "type": "hello",
                    "protocol": 1,
                    "identity": identity,
                    "sessionContext": session_context,
                }))?
            )
            .as_bytes(),
        )
        .await?;
    stdout.flush().await?;

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut delivered = BTreeSet::new();
    let mut work_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    work_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewed_minute = None;
    let mut frame_sequence = 0_u64;
    let session = std::env::var("ST2_PI_CHANNEL_SESSION").unwrap_or_else(|_| "unknown".into());
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { return Ok(()); };
                let Ok(frame) = serde_json::from_str::<Value>(&line) else { continue; };
                match frame.get("type").and_then(Value::as_str) {
                    Some("state") => {
                        let Some(state) = frame.get("state").and_then(Value::as_str) else { continue; };
                        let status = match state {
                            "active" => "working",
                            "idle" => "idle",
                            _ => continue,
                        };
                        frame_sequence = frame_sequence.saturating_add(1);
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: subject.into(),
                            kind: "harness.observed".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("state".into(), Value::String(status.into())),
                                ("driver".into(), Value::String("pi".into())),
                                ("transport".into(), Value::String("pi-channel".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("pi-state:{subject}:{session}:{frame_sequence}")),
                        }).await?;
                    }
                    Some("delivered") => {
                        let Some(message) = frame.pointer("/meta/messageId").and_then(Value::as_str) else { continue; };
                        deliver_message(
                            client,
                            message,
                            subject,
                            format!("pi-delivered:{subject}:{message}"),
                        )
                        .await?;
                    }
                    _ => {}
                }
            }
            _ = interval.tick() => {
                let messages: Vec<MessageView> = client
                    .get(&format!("/v1/messages?to={}", urlencoding::encode(subject)))
                    .await?;
                for message in messages.into_iter().filter(|message| message.status == "sent") {
                    if !delivered.insert(message.subject.clone()) {
                        continue;
                    }
                    let mut content = message_content(client, &message).await?;
                    if let Some(title) = &message.title {
                        content = format!("Subject: {title}\n\n{content}");
                    }
                    let frame = json!({
                        "type": "message",
                        "deliverAs": "steer",
                        "content": content,
                        "meta": {
                            "from": message.from,
                            "messageId": message.subject,
                            "threadId": message.in_reply_to.unwrap_or_else(|| message.subject.clone()),
                            "identity": identity,
                        },
                    });
                    stdout.write_all(serde_json::to_string(&frame)?.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
            }
            _ = work_interval.tick() => {
                sync_work_messages(client, subject).await?;
                let minute = unix_minute()?;
                if renewed_minute != Some(minute) {
                    renew_claimed_work(client, subject, minute).await?;
                    renewed_minute = Some(minute);
                }
            }
        }
    }
}

async fn latest_document_text(client: &Client, name: &str) -> Result<Option<String>> {
    let versions: Vec<DocumentVersion> = client
        .get(&format!("/v1/documents?name={}", urlencoding::encode(name)))
        .await?;
    let Some(version) = versions.into_iter().find(|version| version.latest) else {
        return Ok(None);
    };
    Ok(Some(String::from_utf8(
        document_bytes(client, &version.name, &version.hash).await?,
    )?))
}

async fn message_content(client: &Client, message: &MessageView) -> Result<String> {
    if message.content.starts_with("doc/") {
        let value: Value = client
            .get(&format!(
                "/v1/documents/content?reference={}",
                urlencoding::encode(&message.content)
            ))
            .await?;
        let bytes = serde_json::from_value::<Vec<u8>>(
            value
                .get("bytes")
                .cloned()
                .context("document response lacks bytes")?,
        )?;
        String::from_utf8(bytes).context("message document is not UTF-8")
    } else {
        Ok(message.content.clone())
    }
}

async fn run_codex_native(client: &Client, subject: &str, argv: Vec<String>) -> Result<()> {
    anyhow::ensure!(!argv.is_empty(), "the Codex driver argv is empty");
    let root = PathBuf::from(
        std::env::var_os("ST3_DRIVER_STATE_DIR")
            .context("the Codex driver has no ST3_DRIVER_STATE_DIR")?,
    )
    .join(&hex::encode(Sha256::digest(subject.as_bytes()))[..24]);
    let state_dir = root.join("state");
    let agent_dir = root.join("agent");
    let inbox = st2::message::inbox_dir(&agent_dir);
    let archive = st2::message::archive_dir(&agent_dir);
    let driver_root = root.clone();
    let driver_state = state_dir.clone();
    let driver_agent = agent_dir.clone();
    let identity = subject.strip_prefix("agent/").unwrap_or(subject).to_owned();
    let runtime_id = format!(
        "st3.{}",
        &hex::encode(Sha256::digest(subject.as_bytes()))[..16]
    );
    let mut task = tokio::task::spawn_blocking(move || {
        st2::codex_app_server::run_controlled_paths(
            &driver_root,
            &driver_state,
            &driver_agent,
            identity,
            runtime_id,
            argv,
        )
    });
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut work_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    work_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewed_minute = None;
    let mut ready = false;
    let mut last_control_warning = None;
    loop {
        tokio::select! {
            result = &mut task => return result?,
            _ = interval.tick() => {
                let tick: Result<()> = async {
                    if !ready && state_dir.join("binding.json").is_file() {
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: subject.into(),
                            kind: "harness.observed".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("state".into(), Value::String("ready".into())),
                                ("driver".into(), Value::String("codex".into())),
                                ("transport".into(), Value::String("app-server".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("codex-ready:{subject}")),
                        }).await?;
                        ready = true;
                    }
                    forward_projected_messages(
                        client,
                        subject,
                        &inbox,
                        &archive,
                        "app-server",
                    )
                    .await?;
                    if let Some(observed) = st2::harness_state::read(
                        &st2::harness_state::harness_state_path(&agent_dir),
                        None,
                    ) {
                        let status = harness_activity_state(observed.state);
                        let fields = BTreeMap::from([
                            ("state".into(), Value::String(status.into())),
                            ("driver".into(), Value::String("codex".into())),
                            ("blocked_on".into(), Value::String(observed.blocked_on.as_str().into())),
                            ("ask".into(), Value::String(observed.ask.as_str().into())),
                            ("input_buffer".into(), Value::String(observed.input_buffer.as_str().into())),
                            ("reason".into(), observed.reason.clone().map(Value::String).unwrap_or(Value::Null)),
                            ("exit".into(), observed.exit.clone().map(Value::String).unwrap_or(Value::Null)),
                        ]);
                        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&(
                            observed.since_ms,
                            &fields,
                        ))?));
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: subject.into(),
                            kind: "harness.observed".into(),
                            actor: Some(subject.into()),
                            fields,
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("codex-activity:{subject}:{fingerprint}")),
                        }).await?;
                    }
                    Ok(())
                }.await;
                if let Err(error) = tick {
                    tolerate_driver_api_outage(subject, error, &mut last_control_warning)?;
                }
            }
            _ = work_interval.tick() => {
                let tick: Result<()> = async {
                    sync_work_messages(client, subject).await?;
                    let minute = unix_minute()?;
                    if renewed_minute != Some(minute) {
                        renew_claimed_work(client, subject, minute).await?;
                        renewed_minute = Some(minute);
                    }
                    Ok(())
                }.await;
                if let Err(error) = tick {
                    tolerate_driver_api_outage(subject, error, &mut last_control_warning)?;
                }
            }
        }
    }
}

fn tolerate_driver_api_outage(
    subject: &str,
    error: anyhow::Error,
    last_warning: &mut Option<Instant>,
) -> Result<()> {
    let transient = error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("connect to the st3 API") || message.contains("incomplete HTTP response")
    });
    if !transient {
        return Err(error);
    }
    let now = Instant::now();
    if last_warning.is_none_or(|prior| now.duration_since(prior) >= Duration::from_secs(10)) {
        eprintln!("warning: `{subject}` lost the st3 API and will retry: {error:#}");
        *last_warning = Some(now);
    }
    Ok(())
}

async fn sync_work_messages(client: &Client, subject: &str) -> Result<()> {
    const TAG_PREFIX: &str = "st3-work:";
    let incarnation = current_agent_incarnation(client, subject).await?;
    let incarnation_key = work_incarnation_key(incarnation.as_deref());
    let messages: Vec<MessageView> = client
        .get(&format!(
            "/v1/messages?to={}&include_closed=true",
            urlencoding::encode(subject)
        ))
        .await?;
    let present = messages
        .iter()
        .cloned()
        .flat_map(|message| message.tags)
        .filter_map(|tag| tag.strip_prefix(TAG_PREFIX).map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let work: Vec<StepRunView> = client
        .get(&format!(
            "/v1/work?actor={}&include_terminal=true",
            urlencoding::encode(subject)
        ))
        .await?;
    for message in messages
        .iter()
        .filter(|message| matches!(message.status.as_str(), "delivered" | "read"))
    {
        let Some((step_subject, attempt, readiness_epoch, message_incarnation)) =
            work_message_target(message)
        else {
            continue;
        };
        if !work_message_should_close(
            &work,
            step_subject,
            attempt,
            readiness_epoch,
            message_incarnation,
            &incarnation_key,
        ) {
            continue;
        }
        if message.status == "delivered" {
            accept_message(client, message, Some(subject)).await?;
        }
        close_message(client, &message.subject, Some(subject)).await?;
    }
    for step in work
        .iter()
        .filter(|step| step.status == "ready" && should_notify_work_message(step, &work))
    {
        let tag_value = format!(
            "{}@{}@{}@{}",
            step.subject, step.attempt, step.readiness_epoch, incarnation_key
        );
        if present.contains(&tag_value) {
            continue;
        }
        let _: MessageView = client
            .post(
                "/v1/messages",
                &work_message_request(subject, step, tag_value),
            )
            .await?;
    }
    Ok(())
}

fn should_notify_work_message(step: &StepRunView, work: &[StepRunView]) -> bool {
    !work.iter().any(|candidate| {
        candidate.run == step.run
            && candidate.assigned_to == step.assigned_to
            && candidate.available_to == step.available_to
            && candidate.step.len() < step.step.len()
            && step.step.starts_with(&format!("{}/", candidate.step))
    })
}

fn work_message_target(message: &MessageView) -> Option<(&str, u32, u32, &str)> {
    message.tags.iter().find_map(|tag| {
        let mut parts = tag.strip_prefix("st3-work:")?.rsplitn(4, '@');
        let incarnation = parts.next()?;
        let readiness_epoch = parts.next()?.parse::<u32>().ok()?;
        let attempt = parts.next()?.parse::<u32>().ok()?;
        let step_subject = parts.next()?;
        Some((step_subject, attempt, readiness_epoch, incarnation))
    })
}

fn work_message_was_acknowledged(
    work: &[StepRunView],
    step_subject: &str,
    attempt: u32,
    readiness_epoch: u32,
) -> bool {
    work.iter().any(|step| {
        step.subject == step_subject
            && step.attempt == attempt
            && step.readiness_epoch == readiness_epoch
            && matches!(
                step.status.as_str(),
                "claimed" | "working" | "completed" | "failed" | "cancelled"
            )
    })
}

fn work_message_should_close(
    work: &[StepRunView],
    step_subject: &str,
    attempt: u32,
    readiness_epoch: u32,
    message_incarnation: &str,
    current_incarnation: &str,
) -> bool {
    let current = work.iter().any(|step| {
        step.subject == step_subject
            && step.attempt == attempt
            && step.readiness_epoch == readiness_epoch
    });
    !current
        || message_incarnation != current_incarnation
        || work_message_was_acknowledged(work, step_subject, attempt, readiness_epoch)
}

fn work_incarnation_key(incarnation: Option<&str>) -> String {
    incarnation.map_or_else(
        || "unknown".into(),
        |value| hex::encode(Sha256::digest(value.as_bytes()))[..12].to_owned(),
    )
}

fn work_message_request(
    subject: &str,
    step: &StepRunView,
    tag_value: String,
) -> MessageSendRequest {
    MessageSendRequest {
        idempotency_key: format!("work-message:{subject}:{tag_value}"),
        from: "daemon/runtime".into(),
        to: subject.into(),
        content: work_notification(step),
        title: Some(format!(
            "Mission step ready: {}",
            step.title.as_deref().unwrap_or(&step.step)
        )),
        in_reply_to: None,
        tags: vec![
            format!("st3-work:{tag_value}"),
            format!("mission-run:{}", step.run),
        ],
    }
}

fn work_notification(step: &StepRunView) -> String {
    let queue = step
        .queue
        .as_deref()
        .zip(step.queue_position)
        .map(|(queue, position)| format!("\nQueue: {queue} #{position}"))
        .unwrap_or_default();
    format!(
        "A mission step is ready: {0}. Run `st3 work claim {0}` to read and claim it.\n\nTitle: {1}",
        step.subject,
        step.title.as_deref().unwrap_or(&step.step),
    ) + &queue
}

async fn renew_claimed_work(client: &Client, subject: &str, minute: u64) -> Result<()> {
    let work: Vec<StepRunView> = client
        .get(&format!("/v1/work?actor={}", urlencoding::encode(subject)))
        .await?;
    for step in work.into_iter().filter(|step| {
        matches!(step.status.as_str(), "claimed" | "working")
            && step.claimant.as_deref() == Some(subject)
    }) {
        let _: StepRunView = client
            .post(
                &format!("/v1/work/renew/{}", urlencoding::encode(&step.subject)),
                &WorkRequest {
                    actor: Some(subject.into()),
                    incarnation: step.claim_incarnation,
                    summary: None,
                    reason: None,
                    evidence: Vec::new(),
                    idempotency_key: format!("native-renew:{}:{subject}:{minute}", step.subject),
                },
            )
            .await?;
    }
    Ok(())
}

fn unix_minute() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() / 60)
}

fn current_unix_ms() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

async fn forward_projected_messages(
    client: &Client,
    subject: &str,
    inbox: &Path,
    archive: &Path,
    transport: &str,
) -> Result<()> {
    const TAG_PREFIX: &str = "st3-message:";
    let messages: Vec<MessageView> = client
        .get(&format!(
            "/v1/messages?to={}&include_closed=true",
            urlencoding::encode(subject)
        ))
        .await?;
    sync_closed_projected_messages(inbox, archive, &messages)?;
    let present = projected_message_subjects(inbox, archive)?;
    for message in messages
        .into_iter()
        .filter(|message| message.status == "sent")
    {
        if !present.contains(&message.subject) {
            let content = if message.content.starts_with("doc/") {
                let value: Value = client
                    .get(&format!(
                        "/v1/documents/content?reference={}",
                        urlencoding::encode(&message.content)
                    ))
                    .await?;
                let bytes = serde_json::from_value::<Vec<u8>>(
                    value
                        .get("bytes")
                        .cloned()
                        .context("document response lacks bytes")?,
                )?;
                String::from_utf8(bytes).context("message document is not UTF-8")?
            } else {
                message.content.clone()
            };
            let mut tags = message.tags.clone();
            tags.push(format!("{TAG_PREFIX}{}", message.subject));
            st2::message::send_to_inbox(
                inbox,
                &message.from,
                message.title.as_deref(),
                message.in_reply_to.as_deref(),
                &tags,
                &content,
            )?;
        }
        deliver_message(
            client,
            &message.subject,
            subject,
            format!("native-delivered:{transport}:{subject}:{}", message.subject),
        )
        .await?;
    }
    Ok(())
}

fn sync_closed_projected_messages(
    inbox: &Path,
    archive: &Path,
    messages: &[MessageView],
) -> Result<()> {
    const TAG_PREFIX: &str = "st3-message:";
    let closed = messages
        .iter()
        .filter(|message| message.status == "closed")
        .map(|message| message.subject.as_str())
        .collect::<BTreeSet<_>>();
    for message in st2::message::list_dir(inbox)? {
        let is_closed = message
            .tags
            .iter()
            .filter_map(|tag| tag.strip_prefix(TAG_PREFIX))
            .any(|subject| closed.contains(subject));
        if is_closed {
            st2::message::archive_msg(inbox, archive, &message.filename)?;
        }
    }
    Ok(())
}

fn projected_message_subjects(inbox: &Path, archive: &Path) -> Result<BTreeSet<String>> {
    const TAG_PREFIX: &str = "st3-message:";
    Ok(st2::message::list_dir(inbox)?
        .into_iter()
        .chain(st2::message::list_dir(archive)?)
        .flat_map(|message| message.tags)
        .filter_map(|tag| tag.strip_prefix(TAG_PREFIX).map(str::to_owned))
        .collect())
}

async fn run_claude_mcp(client: &Client, subject: &str) -> Result<()> {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let mut initialized = false;
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut work_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    work_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut delivered = BTreeSet::new();
    let mut renewed_minute = None;
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { return Ok(()); };
                if line.trim().is_empty() { continue; }
                let request: Value = serde_json::from_str(&line).context("decode Claude MCP request")?;
                let id = request.get("id").cloned();
                let response = match request.get("method").and_then(Value::as_str) {
                    Some("initialize") => id.map(|id| json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "protocolVersion": request.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),
                            "capabilities": {"tools": {}, "experimental": {"claude/channel": {}}},
                            "serverInfo": {"name": "st3", "version": env!("CARGO_PKG_VERSION")}
                        }
                    })),
                    Some("notifications/initialized") => {
                        initialized = true;
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: subject.into(),
                            kind: "harness.observed".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("state".into(), Value::String("ready".into())),
                                ("driver".into(), Value::String("claude".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: None,
                        }).await?;
                        None
                    }
                    Some("tools/list") => id.map(|id| json!({"jsonrpc":"2.0","id":id,"result":{"tools":[]}})),
                    Some("resources/list") => id.map(|id| json!({"jsonrpc":"2.0","id":id,"result":{"resources":[]}})),
                    Some("prompts/list") => id.map(|id| json!({"jsonrpc":"2.0","id":id,"result":{"prompts":[]}})),
                    Some("ping") => id.map(|id| json!({"jsonrpc":"2.0","id":id,"result":{}})),
                    _ => None,
                };
                if let Some(response) = response {
                    stdout.write_all(serde_json::to_string(&response)?.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
            }
            _ = interval.tick(), if initialized => {
                let messages: Vec<MessageView> = client
                    .get(&format!("/v1/messages?to={}", urlencoding::encode(subject)))
                    .await?;
                for message in messages.into_iter().filter(|message| message.status == "sent") {
                    if !delivered.insert(message.subject.clone()) {
                        continue;
                    }
                    let content = if message.content.starts_with("doc/") {
                        let value: Value = client
                            .get(&format!("/v1/documents/content?reference={}", urlencoding::encode(&message.content)))
                            .await?;
                        let bytes = serde_json::from_value::<Vec<u8>>(value.get("bytes").cloned().context("document response lacks bytes")?)?;
                        String::from_utf8(bytes).context("message document is not UTF-8")?
                    } else {
                        message.content.clone()
                    };
                    let content = message.title.as_ref().map_or(content.clone(), |title| format!("Subject: {title}\n\n{content}"));
                    let notification = json!({
                        "jsonrpc": "2.0",
                        "method": "notifications/claude/channel",
                        "params": {
                            "content": content,
                            "meta": {
                                "from": message.from,
                                "messageId": message.subject,
                                "threadId": message.in_reply_to.clone().unwrap_or_else(|| message.subject.clone()),
                                "identity": subject
                            }
                        }
                    });
                    stdout.write_all(serde_json::to_string(&notification)?.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                    deliver_message(
                        client,
                        &message.subject,
                        subject,
                        format!("message-delivered:{}:{subject}", message.subject),
                    )
                    .await?;
                }
            }
            _ = work_interval.tick(), if initialized => {
                sync_work_messages(client, subject).await?;
                let minute = unix_minute()?;
                if renewed_minute != Some(minute) {
                    renew_claimed_work(client, subject, minute).await?;
                    renewed_minute = Some(minute);
                }
            }
        }
    }
}

fn read_intent(path: Option<&Path>) -> Result<(String, Option<String>)> {
    match path {
        Some(path) if path != Path::new("-") => Ok((
            fs::read_to_string(path).with_context(|| format!("read KDL {}", path.display()))?,
            Some(path.display().to_string()),
        )),
        _ => {
            let mut source = String::new();
            std::io::stdin().read_to_string(&mut source)?;
            Ok((source, None))
        }
    }
}

fn combine_kdl_tree(root: &Path) -> Result<String> {
    anyhow::ensure!(
        root.is_dir(),
        "import root {} is not a directory",
        root.display()
    );
    let mut files = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "import refuses symbolic link {}",
            entry.path().display()
        );
        if metadata.is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("kdl")
        {
            files.push(entry.path().to_path_buf());
        } else if !metadata.is_dir() && !metadata.is_file() {
            anyhow::bail!("import refuses special file {}", entry.path().display());
        }
    }
    files.sort();
    anyhow::ensure!(!files.is_empty(), "import root contains no .kdl files");
    let mut children = KdlDocument::new();
    for file in files {
        let source = fs::read_to_string(&file)?;
        let document: KdlDocument = source
            .parse()
            .with_context(|| format!("parse {}", file.display()))?;
        st2::kdl_version::ensure_st3_version(&document)
            .with_context(|| format!("check KDL version in {}", file.display()))?;
        let declarations = document
            .nodes()
            .iter()
            .filter(|node| node.name().value() != "version")
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !declarations.is_empty(),
            "{} contains no declarations",
            file.display()
        );
        anyhow::ensure!(
            declarations
                .iter()
                .all(|node| node.name().value() != "subgraph"),
            "{} uses the removed subgraph wrapper",
            file.display()
        );
        children
            .nodes_mut()
            .extend(declarations.into_iter().cloned());
    }
    let mut document = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    document.nodes_mut().push(version);
    document
        .nodes_mut()
        .extend(children.nodes().iter().cloned());
    document.autoformat();
    Ok(document.to_string())
}

fn print_mission(response: &MissionResponse, json_output: bool) -> Result<()> {
    if json_output {
        return print_value(response, true);
    }
    for warning in &response.warnings {
        eprintln!("warning: {warning}");
    }
    let normalized = response.normalized.to_string();
    if response.resolved_intent.kdl != normalized {
        println!("Resolved intent:\n{}", response.resolved_intent.kdl.trim());
    }
    for blocker in &response.blockers {
        eprintln!("blocked: {blocker}");
    }
    if response.changes.is_empty() {
        println!("No desired-state changes.");
    } else {
        for change in &response.changes {
            println!("{} {}", change.change, change.subject);
        }
    }
    for action in &response.predicted_actions {
        println!("  {} {}", action.action, action.subject);
    }
    Ok(())
}

fn print_value(value: &impl serde::Serialize, _json_output: bool) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn idempotency(kdl: &str, tokens: &BTreeMap<String, Vec<String>>) -> String {
    let mut hash = Sha256::new();
    hash.update(kdl.as_bytes());
    hash.update(serde_json::to_vec(tokens).expect("tokens serialize"));
    hex::encode(hash.finalize())
}

fn generated_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("session-{}-{millis}", std::process::id())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn parse_field(value: &str) -> Result<(String, Value), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| "a field must use KEY=VALUE".to_owned())?;
    if key.is_empty() {
        return Err("a field key is empty".into());
    }
    let value = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.into()));
    Ok((key.into(), value))
}

fn parse_peer(value: &str) -> Result<PeerConfig, String> {
    let (name, url) = value
        .split_once('=')
        .ok_or_else(|| "a peer must use NAME=http://ADDRESS".to_owned())?;
    Ok(PeerConfig {
        name: name.into(),
        url: url.into(),
    })
}

fn parse_env(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "an environment value must use NAME=VALUE".to_owned())?;
    if name.is_empty()
        || !name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err("an environment name is invalid".into());
    }
    Ok((name.into(), value.into()))
}

fn parse_input(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "a mission input must use NAME=VALUE".to_owned())?;
    if name.is_empty() || name.contains('/') || name.chars().any(char::is_whitespace) {
        return Err("a mission input name is invalid".into());
    }
    Ok((name.into(), value.into()))
}

fn unique_pairs(values: Vec<(String, String)>, kind: &str) -> Result<BTreeMap<String, String>> {
    let mut output = BTreeMap::new();
    for (name, value) in values {
        anyhow::ensure!(
            output.insert(name.clone(), value).is_none(),
            "the {kind} `{name}` repeats"
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gate_result_uses_a_bounded_content_key() {
        let reason = "evidence ".repeat(100);
        let first = gate_result_idempotency_key("capability", "pass", &reason, &[]);
        let retry = gate_result_idempotency_key("capability", "pass", &reason, &[]);
        let changed = gate_result_idempotency_key("capability", "fail", &reason, &[]);

        assert_eq!(first, retry);
        assert_ne!(first, changed);
        assert!(first.len() <= 512);
    }

    #[test]
    fn an_agent_wait_stops_for_messages_work_and_an_empty_lease() {
        let actor = "agent/worker";
        assert_eq!(
            wait_interruption_reason(actor, true, &[], &["message/new".into()]),
            Some(
                "the wait stopped because agent/worker has a new message: message/new. Run `st3 message ls`"
                    .into()
            )
        );
        assert_eq!(
            wait_interruption_reason(actor, true, &["step-run/new".into()], &[]),
            Some(
                "the wait stopped because agent/worker has ready work: step-run/new. Run `st3 work ls`"
                    .into()
            )
        );
        assert!(
            wait_interruption_reason(actor, false, &[], &[])
                .unwrap()
                .contains("cannot wait without claimed work")
        );
        assert_eq!(wait_interruption_reason(actor, true, &[], &[]), None);
    }

    #[test]
    fn a_short_message_party_resolves_to_its_current_mission_run() {
        assert_eq!(
            normalize_message_subject_in_run("worker", Some("run-id")),
            "agent/run-id/worker"
        );
        assert_eq!(
            normalize_message_subject_in_run("agent/global.worker", Some("run-id")),
            "agent/global.worker"
        );
        assert_eq!(
            normalize_message_subject_in_run("worker", None),
            "agent/worker"
        );
    }

    #[test]
    fn mission_start_accepts_an_explicit_run_id() {
        let cli = Cli::try_parse_from([
            "st3",
            "mission",
            "start",
            "release/demo",
            "--id",
            "release/demo/test",
            "--as",
            "agent/operator",
        ])
        .unwrap();
        let Command::Mission {
            command: MissionViewCommand::Start(args),
        } = cli.command
        else {
            panic!("the mission start command did not parse");
        };
        assert_eq!(args.mission, "release/demo");
        assert_eq!(args.id.as_deref(), Some("release/demo/test"));
        assert_eq!(args.actor.as_deref(), Some("agent/operator"));
    }

    #[test]
    fn mission_show_accepts_follow() {
        let cli = Cli::try_parse_from([
            "st3",
            "mission",
            "show",
            "mission-run/release/demo",
            "--follow",
        ])
        .unwrap();
        let Command::Mission {
            command: MissionViewCommand::Show(args),
        } = cli.command
        else {
            panic!("the mission show command did not parse");
        };
        assert_eq!(args.mission_or_run, "mission-run/release/demo");
        assert!(args.follow);
    }

    #[test]
    fn cli_rejects_the_removed_plan_command() {
        assert!(Cli::try_parse_from(["st3", "plan", "show", "example"]).is_err());
    }

    #[test]
    fn exec_cli_builds_a_normal_st3_member() {
        let source = exec_intent(
            "cli-test",
            "local",
            Path::new("/work/tree"),
            &[("MODE".into(), "test".into())],
            &["printf".into(), "%s".into(), "hello".into()],
        );
        assert!(source.starts_with("version 2\n"));
        let intent = st3::parse_intent(&source, "node").unwrap();
        let mission = &intent.missions["exec/cli-test"];
        assert!(matches!(
            mission.completion,
            Some(st3::model::CompletionSpec::AllStepsExhausted)
        ));
        let execution = mission.steps["execute"]
            .declarations_kdl
            .as_deref()
            .unwrap();
        assert!(execution.contains("exec cli-test"), "{execution}");
        assert!(execution.contains("host local"));
        assert!(execution.contains("workspace \"/work/tree\""));
        assert!(execution.contains("cwd \"/work/tree\""));
        assert!(execution.contains("argv printf %s hello"));
        assert!(execution.contains("MODE test"));
        assert!(execution.contains("restart never"));
        assert!(matches!(
            &mission.steps["execute"].gates[0],
            st3::model::GateSpec::Field { subject, .. }
                if subject == "exec/${ST_MISSION_RUN}/cli-test"
        ));
    }

    #[test]
    fn wait_timeout_accepts_bounded_units_and_zero() {
        assert_eq!(parse_timeout("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_timeout("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_timeout("0").unwrap(), Duration::ZERO);
        assert!(parse_timeout("forever").is_err());
    }

    #[test]
    fn wait_reads_resource_status_from_observed_facts() {
        let resource = json!({
            "baseline": true,
            "facts": {"status": "ready"},
            "kind": "filesystem.file"
        });
        assert_eq!(projected_actual_status(Some(&resource)), Some("ready"));

        let runtime = json!({"fields": {"status": "running"}});
        assert_eq!(projected_actual_status(Some(&runtime)), Some("running"));
    }

    #[test]
    fn wait_accepts_message_delivery() {
        validate_wait_condition("delivered").unwrap();
        let message = serde_json::json!({ "status": "delivered" });
        assert_eq!(projected_actual_status(Some(&message)), Some("delivered"));
    }

    #[test]
    fn wait_accepts_a_standing_mission_run() {
        validate_wait_condition("standing").unwrap();
        let run = serde_json::json!({ "status": "standing" });
        assert_eq!(projected_actual_status(Some(&run)), Some("standing"));
    }

    #[test]
    fn an_ended_harness_uses_the_registered_state() {
        assert_eq!(
            harness_activity_state(st2::harness_state::Activity::Ended),
            "ended"
        );
        st3_schema::registry()
            .validate_claim(
                "agent/run/worker",
                "harness.observed",
                &BTreeMap::from([("state".into(), Value::String("ended".into()))]),
            )
            .unwrap();
    }

    #[test]
    fn message_references_round_trip_nested_ids_and_projected_files() {
        assert_eq!(
            normalize_message_reference("message/kickoff/run-1"),
            "kickoff/run-1"
        );
        assert_eq!(
            normalize_message_reference("kickoff%2Frun-1"),
            "kickoff/run-1"
        );
        assert_eq!(
            normalize_message_reference("/tmp/inbox/00000000000000000008-kickoff%2Frun-1.md"),
            "kickoff/run-1"
        );
    }

    #[test]
    fn message_archive_accepts_more_than_one_reference() {
        let cli = Cli::try_parse_from([
            "st3",
            "message",
            "archive",
            "first",
            "second",
            "third",
            "--as",
            "agent/sup",
        ])
        .unwrap();
        let Command::Message {
            command: MessageCommand::Archive(args),
        } = cli.command
        else {
            panic!("the archive command did not parse");
        };
        assert_eq!(args.references, ["first", "second", "third"]);
        assert_eq!(args.actor.as_deref(), Some("agent/sup"));
    }

    #[test]
    fn publish_accepts_a_file_and_actor() {
        let cli = Cli::try_parse_from(["st3", "publish", "mission.kdl", "--as", "agent/operator"])
            .unwrap();
        let Command::Publish(args) = cli.command else {
            panic!("the publish command did not parse");
        };
        assert_eq!(args.file.as_deref(), Some(Path::new("mission.kdl")));
        assert_eq!(args.actor, "agent/operator");
    }

    #[test]
    fn intent_helpers_print_current_direct_kdl() {
        let quick = quick_agent_intent(
            "standing/example.worker",
            "example.worker",
            Path::new("/work/example"),
            "codex",
            None,
            None,
        );
        let quick = st3::parse_intent(&quick, "node").unwrap();
        assert!(quick.missions.contains_key("standing/example.worker"));

        let message = message_mission_intent(
            "message/test",
            "test",
            "person/sender",
            "person/recipient",
            "Hello.",
            Some("Greeting"),
            None,
            &["example".into()],
        );
        let message = st3::parse_intent(&message, "node").unwrap();
        assert!(message.missions.contains_key("message/test"));

        let (watch, mission, resource) = resource_watch_intent(
            "github.pull-request",
            "example/project#1",
            &["state".into()],
            "person/operator",
        )
        .unwrap();
        let watch = st3::parse_intent(&watch, "node").unwrap();
        assert!(watch.missions.contains_key(&mission));
        assert!(watch.subjects.contains_key(&resource));

        let refresh = resource_refresh_intent("resource/example", "after-change", 30_000);
        let refresh = st3::parse_intent(&refresh, "node").unwrap();
        assert_eq!(refresh.resource_refreshes.len(), 1);

        let reset = runtime_reset_intent(
            "mission-run/example",
            "retry",
            "agent/worker",
            "run-generation/01990000000070008000000000000000",
            "retry the worker",
        );
        let reset = st3::parse_intent(&reset, "node").unwrap();
        assert_eq!(reset.mission_runs["mission-run/example"].resets.len(), 1);

        let planning = planning_session_intent(
            "planning/example/01990000000070008000000000000000",
            "example",
            &format!("doc/planning/example/request@{}", "a".repeat(64)),
            Path::new("/work/example"),
            "person/operator",
            None,
            None,
            None,
        );
        let planning = st3::parse_intent(&planning, "node").unwrap();
        assert_eq!(planning.planning_sessions.len(), 1);
    }

    #[test]
    fn work_revise_accepts_print_only_mode() {
        let cli = Cli::try_parse_from([
            "st3",
            "work",
            "revise",
            "mission-run/release",
            "release.kdl",
            "--reason",
            "add a gate",
            "--as",
            "person/operator",
            "--print-kdl",
        ])
        .unwrap();
        let Command::Work {
            command: WorkCommand::Revise(args),
        } = cli.command
        else {
            panic!("the work revise command did not parse");
        };
        assert!(args.print_kdl);
    }

    #[tokio::test]
    async fn pty_ui_refuses_a_remote_endpoint_before_launch() {
        let endpoint = Endpoint::Http("http://example.invalid".into());
        let client = Client::new(endpoint.clone());
        let error = run_pty(&client, endpoint, &Config::default(), PtyCommand::Ui, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("local Unix endpoint"));
    }

    #[test]
    fn a_native_driver_gets_one_graph_message_projection() {
        let root = tempfile::tempdir().unwrap();
        let (catalog, agent_dir, identity, runtime_id) =
            prepare_native_driver_in("agent/node.worker", root.path()).unwrap();
        let discovery = agent_spec::discovery::discover_strict(&catalog);
        assert!(discovery.errors.is_empty(), "{:?}", discovery.errors);
        assert_eq!(discovery.specs.len(), 1);
        assert_eq!(discovery.specs[0].identity, "node.worker");
        assert_eq!(
            discovery.specs[0].path.parent().unwrap(),
            agent_dir.as_path()
        );
        assert_eq!(identity, "node.worker");
        assert_eq!(runtime_id, "node.worker");
    }

    #[test]
    fn the_st3_development_channel_is_an_explicit_fallback() {
        let argv = vec![
            "claude".into(),
            "--channels".into(),
            st2::claude_channel::ST3_CHANNEL.into(),
            "Do the work.".into(),
        ];
        let output =
            st3_development_channel_argv(argv, Path::new("/opt/st3/bin/st3"), "agent/node.worker")
                .unwrap();
        assert!(
            !output
                .iter()
                .any(|arg| arg == st2::claude_channel::ST3_CHANNEL)
        );
        assert!(
            output
                .iter()
                .any(|arg| { arg == "--dangerously-load-development-channels=server:st3" })
        );
        let config = output
            .windows(2)
            .find(|pair| pair[0] == "--mcp-config")
            .map(|pair| &pair[1])
            .expect("the fallback has an MCP config");
        let config: Value = serde_json::from_str(config).unwrap();
        assert_eq!(
            config["mcpServers"]["st3"]["args"],
            json!(["driver", "claude-mcp", "--subject", "agent/node.worker"])
        );
    }

    #[test]
    fn an_unread_native_message_is_ready_for_a_delivery_claim() {
        let root = tempfile::tempdir().unwrap();
        let inbox = root.path().join("inbox");
        let archive = root.path().join("archive");
        st2::message::send_to_inbox(
            &inbox,
            "requester",
            Some("Start"),
            None,
            &["st3-message:message/kickoff".into()],
            "Do the work.",
        )
        .unwrap();

        assert_eq!(
            projected_message_subjects(&inbox, &archive).unwrap(),
            BTreeSet::from(["message/kickoff".into()])
        );
    }

    #[test]
    fn a_graph_archive_moves_the_native_delivery_file() {
        let root = tempfile::tempdir().unwrap();
        let inbox = root.path().join("inbox");
        let archive = root.path().join("archive");
        let filename = st2::message::send_to_inbox(
            &inbox,
            "requester",
            Some("Start"),
            None,
            &["st3-message:message/kickoff/run-1".into()],
            "Do the work.",
        )
        .unwrap();
        let messages = vec![MessageView {
            subject: "message/kickoff/run-1".into(),
            from: "agent/requester".into(),
            to: "agent/worker".into(),
            content: "Do the work.".into(),
            status: "closed".into(),
            title: Some("Start".into()),
            in_reply_to: None,
            tags: Vec::new(),
            created_index: 1,
        }];

        sync_closed_projected_messages(&inbox, &archive, &messages).unwrap();

        assert!(!inbox.join(&filename).exists());
        assert!(archive.join(filename).is_file());
    }

    #[test]
    fn ready_work_is_an_idempotent_graph_message() {
        let mut step = StepRunView {
            subject: "step-run/run-1/build".into(),
            run: "mission-run/run-1".into(),
            generation: "run-generation/run-1".into(),
            step: "build".into(),
            queue: None,
            queue_position: None,
            definition_hash: "definition".into(),
            status: "ready".into(),
            attempt: 2,
            assigned_to: Some("agent/worker".into()),
            available_to: Vec::new(),
            agentless: false,
            title: Some("Build the change".into()),
            goals: vec!["Implement and test the requested change.".into()],
            constraints: Vec::new(),
            under: Vec::new(),
            worker_reported: false,
            claimant: None,
            claim_incarnation: None,
            claim_expires_at_unix_ms: None,
            readiness_epoch: 1,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        };

        let request = work_message_request(
            "agent/worker",
            &step,
            "step-run/run-1/build@2@1@incarnation".into(),
        );

        assert_eq!(request.from, "daemon/runtime");
        assert_eq!(request.to, "agent/worker");
        assert_eq!(
            request.idempotency_key,
            "work-message:agent/worker:step-run/run-1/build@2@1@incarnation"
        );
        assert_eq!(
            request.tags,
            [
                "st3-work:step-run/run-1/build@2@1@incarnation",
                "mission-run:mission-run/run-1"
            ]
        );
        assert_eq!(
            request.content,
            "A mission step is ready: step-run/run-1/build. Run `st3 work claim step-run/run-1/build` to read and claim it.\n\nTitle: Build the change"
        );
        assert!(!request.content.contains(&step.goals[0]));

        let message = MessageView {
            subject: "message/work".into(),
            from: request.from,
            to: request.to,
            content: request.content,
            status: "delivered".into(),
            title: request.title,
            in_reply_to: request.in_reply_to,
            tags: request.tags,
            created_index: 1,
        };
        assert_eq!(
            work_message_target(&message),
            Some(("step-run/run-1/build", 2, 1, "incarnation"))
        );
        assert!(!work_message_was_acknowledged(
            std::slice::from_ref(&step),
            "step-run/run-1/build",
            2,
            1,
        ));
        assert!(!work_message_should_close(
            std::slice::from_ref(&step),
            "step-run/run-1/build",
            2,
            1,
            "incarnation",
            "incarnation",
        ));
        assert!(work_message_should_close(
            std::slice::from_ref(&step),
            "step-run/old-generation/build",
            2,
            1,
            "incarnation",
            "incarnation",
        ));
        step.status = "claimed".into();
        assert!(work_message_was_acknowledged(
            &[step],
            "step-run/run-1/build",
            2,
            1,
        ));
    }

    #[test]
    fn inherited_nested_work_uses_the_parent_message() {
        let step = |subject: &str, path: &str, assignee: &str| StepRunView {
            subject: subject.into(),
            run: "mission-run/run-1".into(),
            generation: "run-generation/run-1".into(),
            step: path.into(),
            queue: None,
            queue_position: None,
            definition_hash: "definition".into(),
            status: "ready".into(),
            attempt: 1,
            assigned_to: Some(assignee.into()),
            available_to: Vec::new(),
            agentless: false,
            title: None,
            goals: Vec::new(),
            constraints: Vec::new(),
            under: Vec::new(),
            worker_reported: false,
            claimant: None,
            claim_incarnation: None,
            claim_expires_at_unix_ms: None,
            readiness_epoch: 1,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        };
        let parent = step("step-run/run-1/build", "build", "agent/builder");
        let inherited = step(
            "step-run/run-1/build/work/inspect",
            "build/work/inspect",
            "agent/builder",
        );
        let reassigned = step(
            "step-run/run-1/build/work/review",
            "build/work/review",
            "agent/reviewer",
        );
        let work = vec![parent.clone(), inherited.clone(), reassigned.clone()];

        assert!(should_notify_work_message(&parent, &work));
        assert!(!should_notify_work_message(&inherited, &work));
        assert!(should_notify_work_message(&reassigned, &work));
    }

    #[test]
    fn mission_follow_stops_for_completed_and_standing_runs() {
        assert!(mission_run_follow_succeeded("completed"));
        assert!(mission_run_follow_succeeded("standing"));
        assert!(!mission_run_follow_succeeded("running"));
        assert!(!mission_run_follow_succeeded("failed"));
    }

    #[test]
    fn eval_graph_renders_nested_state_and_semantic_transitions() {
        let root_subject = "mission-run/root";
        let parent_step = graph_step(
            "step-run/root/rename",
            root_subject,
            "rename",
            "working",
            Some("agent/base"),
        );
        let root = graph_run(
            root_subject,
            root_subject,
            None,
            vec![
                parent_step,
                graph_step(
                    "step-run/root/rename/work/inspect",
                    root_subject,
                    "rename/work/inspect",
                    "completed",
                    Some("agent/base"),
                ),
                graph_step(
                    "step-run/root/rename/work/change",
                    root_subject,
                    "rename/work/change",
                    "ready",
                    Some("agent/base"),
                ),
            ],
        );
        let snapshot = EvalGraphSnapshot {
            eval: EvalStatus {
                mission_run: root_subject.into(),
                lifecycle: "running".into(),
                phase: "normal".into(),
                active_steps: vec!["rename".into()],
                verdict: None,
                cleanup: "pending".into(),
                store_index: 9,
            },
            runs: vec![root],
        };
        let transitions = vec![GraphTransition {
            elapsed: Duration::from_secs(7),
            label: "Change the package".into(),
            from: "pending".into(),
            to: "ready".into(),
            assignee: Some("base".into()),
        }];

        let rendered = render_eval_graph(&snapshot, &transitions, Duration::from_secs(9));

        assert!(rendered.contains("ST3 EVAL GRAPH  root"));
        assert!(rendered.contains("STATE      running · normal"));
        assert!(rendered.contains("1/3 completed · 2 active"));
        assert!(rendered.contains("rename — Change the package · base"));
        assert!(rendered.contains("↳ nested work · 1/2 completed"));
        assert!(rendered.contains("inspect — Inspect the package · base"));
        assert!(rendered.contains("00:07  Change the package: pending → ready · base"));
    }

    #[test]
    fn eval_graph_records_only_changed_node_state() {
        let previous = BTreeMap::from([(
            "step-run/root/build".into(),
            GraphNodeState {
                label: "Build".into(),
                state: "ready".into(),
                assignee: Some("worker".into()),
            },
        )]);
        let current = BTreeMap::from([(
            "step-run/root/build".into(),
            GraphNodeState {
                label: "Build".into(),
                state: "working".into(),
                assignee: Some("worker".into()),
            },
        )]);
        let mut transitions = Vec::new();

        record_graph_transitions(
            &previous,
            &current,
            Duration::from_secs(3),
            &mut transitions,
        );

        assert_eq!(
            transitions,
            [GraphTransition {
                elapsed: Duration::from_secs(3),
                label: "Build".into(),
                from: "ready".into(),
                to: "working".into(),
                assignee: Some("worker".into()),
            }]
        );
    }

    fn graph_run(
        subject: &str,
        root: &str,
        parent_step_run: Option<&str>,
        steps: Vec<StepRunView>,
    ) -> MissionRunView {
        MissionRunView {
            subject: subject.into(),
            id: subject
                .strip_prefix("mission-run/")
                .unwrap_or(subject)
                .into(),
            mission: "mission/work".into(),
            generation: "run-generation/current".into(),
            initial_revision: "initial-revision".into(),
            revision: "revision".into(),
            root_revision: "root-revision".into(),
            root_mission_run: root.into(),
            parent_step_run: parent_step_run.map(str::to_owned),
            workspace: "/tmp/eval".into(),
            requester: "person/eval-requester".into(),
            inputs: BTreeMap::new(),
            mode: "eval".into(),
            timeout_ms: Some(1_200_000),
            deadline_at_unix_ms: Some(1_200_001),
            status: "running".into(),
            phase: "normal".into(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            steps,
        }
    }

    fn graph_step(
        subject: &str,
        run: &str,
        step: &str,
        status: &str,
        assignee: Option<&str>,
    ) -> StepRunView {
        StepRunView {
            subject: subject.into(),
            run: run.into(),
            generation: "run-generation/current".into(),
            step: step.into(),
            queue: None,
            queue_position: None,
            definition_hash: "definition".into(),
            status: status.into(),
            attempt: 1,
            assigned_to: assignee.map(str::to_owned),
            available_to: Vec::new(),
            agentless: assignee.is_none(),
            title: Some(match step.rsplit('/').next().unwrap_or(step) {
                "rename" | "change" => "Change the package".into(),
                "inspect" => "Inspect the package".into(),
                _ => step.into(),
            }),
            goals: Vec::new(),
            constraints: Vec::new(),
            under: Vec::new(),
            worker_reported: false,
            claimant: None,
            claim_incarnation: None,
            claim_expires_at_unix_ms: None,
            readiness_epoch: 1,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn a_codex_driver_retries_a_transient_st3_api_outage() {
        let mut last_warning = None;
        tolerate_driver_api_outage(
            "agent/run/worker",
            anyhow::anyhow!("incomplete HTTP response"),
            &mut last_warning,
        )
        .unwrap();
        assert!(last_warning.is_some());
    }

    #[test]
    fn a_codex_driver_does_not_retry_a_semantic_api_error() {
        let mut last_warning = None;
        let error = tolerate_driver_api_outage(
            "agent/run/worker",
            anyhow::anyhow!("the work claim is stale"),
            &mut last_warning,
        )
        .unwrap_err();
        assert!(error.to_string().contains("work claim is stale"));
        assert!(last_warning.is_none());
    }
}
