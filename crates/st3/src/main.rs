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
use st3::api::{AppState, router, serve_tcp, serve_unix};
use st3::archive::archive_eval;
use st3::client::{Client, Endpoint};
use st3::config::{Config, PeerConfig};
use st3::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, ClaimInput, ClaimRecord, ClaimsPage,
    DoctorReport, DocumentPutRequest, DocumentVersion, EvalStartRequest, EvalStartResponse,
    EvalStatus, EventRecord, IntentInput, JudgementRequest, MessageLifecycleRequest,
    MessageSendRequest, MessageView, PlanOutputView, PlanProductionRequest, PlanRequest,
    PlanResponse, PlanRevisionRequest, PlanRunRequest, PlanRunView, PlanState, QuickAgentRequest,
    QuickAgentResponse, ReviewRequest, SessionControlResponse, SessionInputMode,
    SessionInputRequest, SessionLogChunk, SessionScreen, SessionSignalRequest, StatusResponse,
    StepRunView, WorkRequest,
};
use st3::reconcile::Reconciler;
use st3::store::Store;
use tokio::sync::Notify;
use walkdir::WalkDir;

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
    Plan(FileArgs),
    /// Apply a new-format KDL intent.
    Run(RunArgs),
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
    /// Manage the Linux st3 user service.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
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
    /// Record a human review decision.
    Review {
        #[command(subcommand)]
        command: ReviewCommand,
    },
    /// Claim and update durable plan work.
    Work {
        #[command(subcommand)]
        command: WorkCommand,
    },
    /// Send and receive Small Talk graph messages.
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    /// Record a running judge result.
    Judgement(JudgementArgs),
    /// Generate one shell completion script.
    Completions(CompletionsArgs),
    #[command(hide = true)]
    Driver(DriverArgs),
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
}

#[derive(Args)]
struct FileArgs {
    file: Option<PathBuf>,
    #[arg(long, visible_alias = "at")]
    at_index: Option<u64>,
}

#[derive(Args)]
struct RunArgs {
    file: Option<PathBuf>,
    #[arg(long)]
    plan: Option<String>,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long, env = "ST_AGENT")]
    requester: Option<String>,
    #[arg(long)]
    detach: bool,
    #[arg(long, visible_alias = "at")]
    at_index: Option<u64>,
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
    scope: Option<String>,
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
    Uninstall,
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
    /// Show one live graph screen with semantic state transitions.
    #[arg(long)]
    graph: bool,
}

#[derive(Args)]
struct GraphArgs {
    scope: String,
}

#[derive(Args)]
struct StatusArgs {
    #[arg(env = "ST_AGENT")]
    subject: Option<String>,
    #[arg(long)]
    scope: Option<String>,
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
struct ClaimArgs {
    subject: String,
    kind: String,
    #[arg(long)]
    actor: Option<String>,
    #[arg(long = "field", value_parser = parse_field)]
    fields: Vec<(String, Value)>,
    #[arg(long)]
    evidence: Vec<String>,
}

#[derive(Subcommand)]
enum ReviewCommand {
    Approve(ReviewArgs),
    Reject(ReviewArgs),
    Revise(ReviewArgs),
}

#[derive(Subcommand)]
enum WorkCommand {
    Ls {
        #[arg(long = "as", env = "ST_AGENT")]
        assignee: Option<String>,
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
    /// Publish the exact ready plan produced by one claimed step.
    PublishPlan(WorkPublishPlanArgs),
    Revise(WorkReviseArgs),
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
}

#[derive(Args)]
struct WorkPublishPlanArgs {
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
        default_value = "requester"
    )]
    from: String,
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
        default_value = "requester"
    )]
    from: String,
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
struct JudgementArgs {
    #[arg(value_parser = ["pass", "fail"])]
    verdict: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    evidence: Vec<String>,
    #[arg(long, env = "ST3_JUDGE_CAPABILITY")]
    operation_capability: String,
}

#[derive(Args)]
struct DriverArgs {
    #[arg(value_parser = ["claude", "claude-mcp", "codex", "pi", "pi-channel", "opencode", "exec"])]
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
    let config = Config::load(None)?;
    let endpoint = cli
        .endpoint
        .or_else(|| std::env::var("ST3_ENDPOINT").ok())
        .as_deref()
        .map(Endpoint::parse)
        .unwrap_or_else(|| Endpoint::Unix(config.socket.clone()));
    let client = Client::new(endpoint.clone());
    match cli.command {
        Command::Up(_) => unreachable!(),
        Command::Claude(args) => {
            run_quick(&client, endpoint, &config, args, "claude", cli.json).await
        }
        Command::Codex(args) => {
            run_quick(&client, endpoint, &config, args, "codex", cli.json).await
        }
        Command::Plan(args) => run_plan(&client, args, cli.json).await,
        Command::Run(args) => run_file(&client, args, cli.json).await,
        Command::Import(args) => run_import(&client, args, cli.json).await,
        Command::Exec(args) => run_exec(&client, args, cli.json).await,
        Command::Logs(args) => run_logs(&client, args, cli.json).await,
        Command::Pty { command } => run_pty(&client, endpoint, &config, command, cli.json).await,
        Command::Inspect(args) => run_inspect(&client, args, cli.json).await,
        Command::Trace(args) => run_trace(&client, args, cli.json).await,
        Command::Wait(args) => run_wait(&client, args, cli.json).await,
        Command::Doctor(args) => run_doctor(&client, args, cli.json).await,
        Command::Service { command } => run_service(command),
        Command::Doc { command } => run_doc(&client, command, cli.json).await,
        Command::Eval(args) => run_eval(&client, args, cli.json).await,
        Command::Graph(args) => run_graph(&client, args, cli.json).await,
        Command::Status(args) => run_status(&client, args, cli.json).await,
        Command::Agents(args) => run_agents(&client, args, cli.json).await,
        Command::Context { command } => run_context(&client, command, cli.json).await,
        Command::Resource { command } => run_resource(&client, command, cli.json).await,
        Command::Claim(args) => run_claim(&client, args, cli.json).await,
        Command::Review { command } => run_review(&client, command, cli.json).await,
        Command::Work { command } => run_work(&client, command, cli.json).await,
        Command::Message { command } => run_message(&client, command, cli.json).await,
        Command::Judgement(args) => run_judgement(&client, args, cli.json).await,
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
    let mut config = Config::load(args.config.as_deref())?;
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
    if !args.peer.is_empty() {
        config.peers = args.peer;
    }
    config.validate()?;
    fs::create_dir_all(&config.state_dir)?;
    eprintln!("st3: security warning: v1 trusts every configured peer and has no TLS or ACLs");
    if let Some(address) = &config.peer_listen {
        eprintln!("st3: trusted peer API listening on http://{address}");
    }
    let store = Arc::new(Store::open(
        &config.state_dir.join("claims.sqlite3"),
        &config.node,
    )?);
    store.append_claim(&ClaimInput {
        subject: format!("daemon/{}", config.node),
        kind: "daemon.started".into(),
        actor: None,
        fields: BTreeMap::from([
            ("status".into(), Value::String("running".into())),
            (
                "version".into(),
                Value::String(env!("CARGO_PKG_VERSION").into()),
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
    let event_notify = Arc::new(Notify::new());
    let pty_root = config
        .pty_root
        .clone()
        .unwrap_or_else(|| config.state_dir.join("pty"));
    let trusted_peers = config
        .peers
        .iter()
        .map(|peer| peer.name.clone())
        .collect::<BTreeSet<_>>();
    let state = AppState {
        store: store.clone(),
        notify: notify.clone(),
        event_notify: event_notify.clone(),
        node: config.node.clone(),
        state_dir: config.state_dir.clone(),
        pty_root: pty_root.clone(),
        trusted_peers,
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
    st3::peer::start(
        store,
        config.node.clone(),
        config.peers.clone(),
        notify,
        event_notify,
    );
    if let Some(address) = config.peer_listen.clone() {
        let app = router(state.clone());
        tokio::spawn(async move {
            if let Err(error) = serve_tcp(&address, app).await {
                eprintln!("st3: peer server failed: {error:#}");
            }
        });
    }
    eprintln!("st3: local API listening at {}", config.socket.display());
    serve_unix(&config.socket, router(state)).await
}

async fn run_plan(client: &Client, args: FileArgs, json_output: bool) -> Result<()> {
    let (kdl, source_name) = read_intent(args.file.as_deref())?;
    let response: PlanResponse = client
        .post(
            "/v1/intent/plan",
            &PlanRequest {
                intent: IntentInput { kdl, source_name },
                at_index: args.at_index,
            },
        )
        .await?;
    print_plan(&response, json_output)
}

async fn run_file(client: &Client, args: RunArgs, json_output: bool) -> Result<()> {
    let file = args.file.clone();
    let (kdl, source_name) = read_intent(file.as_deref())?;
    let intent = IntentInput { kdl, source_name };
    let plan: PlanResponse = client
        .post(
            "/v1/intent/plan",
            &PlanRequest {
                intent: intent.clone(),
                at_index: args.at_index,
            },
        )
        .await?;
    anyhow::ensure!(plan.blockers.is_empty(), "{}", plan.blockers.join("; "));
    let resolved_intent = plan.resolved_intent.clone();
    let idempotency_key = idempotency(&resolved_intent.kdl, &plan.subject_tokens);
    let response: ApplyResponse = client
        .post(
            "/v1/intent/apply",
            &ApplyRequest {
                intent: resolved_intent.clone(),
                expected_subjects: plan.subject_tokens,
                idempotency_key,
            },
        )
        .await?;
    let parsed = st3::parse_intent(&resolved_intent.kdl, "local")?;
    let ready = parsed
        .plans
        .values()
        .filter(|plan| plan.state == PlanState::Ready)
        .collect::<Vec<_>>();
    if ready.is_empty() {
        return print_value(&response, json_output);
    }
    let selected = if let Some(selected) = args.plan.as_deref() {
        let selected = selected.strip_prefix("plan/").unwrap_or(selected);
        ready
            .iter()
            .find(|plan| plan.id == selected)
            .copied()
            .with_context(|| format!("ready plan `{selected}` is not in the file"))?
    } else {
        anyhow::ensure!(
            ready.len() == 1,
            "the file contains multiple ready plans; select one with --plan"
        );
        ready[0]
    };
    let workspace = args
        .workspace
        .or_else(|| {
            file.as_deref()
                .and_then(Path::parent)
                .map(Path::to_path_buf)
        })
        .unwrap_or(std::env::current_dir()?)
        .canonicalize()
        .context("resolve the plan run workspace")?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let started: PlanRunView = client
        .post(
            "/v1/plan-runs",
            &PlanRunRequest {
                plan: selected.id.clone(),
                revision: Some(selected.revision.clone()),
                workspace: workspace.to_string_lossy().into_owned(),
                requester: args.requester,
                mode: Some("run".into()),
                idempotency_key: format!(
                    "run:{}:{nonce}:{}",
                    selected.revision,
                    std::process::id()
                ),
            },
        )
        .await?;
    if args.detach {
        return print_value(&started, json_output);
    }
    follow_plan_run(client, started, json_output).await
}

async fn follow_plan_run(client: &Client, mut run: PlanRunView, json_output: bool) -> Result<()> {
    let mut prior = String::new();
    loop {
        let summary = plan_run_signature(&run)?;
        if summary != prior && !json_output {
            print_plan_run_tree(&run);
            prior = summary;
        }
        match run.status.as_str() {
            "completed" => {
                return if json_output {
                    print_value(&run, true)
                } else {
                    Ok(())
                };
            }
            "failed" | "cancelled" => anyhow::bail!("plan run {} is {}", run.subject, run.status),
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        run = client
            .get(&format!(
                "/v1/plan-runs/{}",
                urlencoding::encode(&run.subject)
            ))
            .await?;
    }
}

fn plan_run_signature(run: &PlanRunView) -> Result<String> {
    serde_json::to_string(&json!({
        "revision": run.revision,
        "status": run.status,
        "phase": run.phase,
        "steps": run.steps.iter().map(|step| json!({
            "step": step.step,
            "status": step.status,
            "attempt": step.attempt,
            "reason": step.blocked_reason,
        })).collect::<Vec<_>>(),
    }))
    .map_err(Into::into)
}

fn print_plan_run_tree(run: &PlanRunView) {
    println!(
        "{} {} ({}, revision {})",
        run.subject,
        run.status,
        run.phase,
        &run.revision[..run.revision.len().min(12)]
    );
    for (index, step) in run.steps.iter().enumerate() {
        let depth = step.step.matches('/').count();
        let branch = if index + 1 == run.steps.len() {
            "└─"
        } else {
            "├─"
        };
        let title = step.title.as_deref().unwrap_or(&step.step);
        let retry = if step.attempt > 1 {
            format!("; attempt {}", step.attempt)
        } else {
            String::new()
        };
        println!(
            "{}{} [{}{}] {}",
            "  ".repeat(depth),
            branch,
            step.status,
            retry,
            title
        );
        if let Some(reason) = &step.blocked_reason {
            println!("{}   {}", "  ".repeat(depth), reason);
        }
    }
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
    let explicit_name = args.name.is_some();
    let name = args.name.unwrap_or_else(|| {
        format!(
            "cli-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        )
    });
    let subject = format!("exec/{name}");
    if explicit_name {
        let status = status_for(client, &subject).await?;
        anyhow::ensure!(
            status
                .subjects
                .first()
                .is_none_or(|item| item.desired.is_none() && item.actual.is_none()),
            "subject `{subject}` already exists"
        );
    }
    let cwd = args.cwd.unwrap_or(std::env::current_dir()?);
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("resolve working directory {}", cwd.display()))?;
    let kdl = exec_intent(&name, &args.host, &cwd, &args.environment, &args.argv);
    let applied = apply_generated(client, kdl, format!("st3 exec {name}")).await?;
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
    wait_for_actual(client, &subject, applied.store_index).await?;
    if !json_output {
        eprintln!("{}", subject);
    }
    let follow = follow_logs(client, &subject, false, true, true, !json_output);
    let final_chunk = tokio::select! {
        result = follow => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            if args.cancel_on_interrupt {
                apply_generated(client, stop_intent(&subject), format!("st3 exec stop {name}")).await?;
            }
            return Err(CommandExit(130).into());
        }
    };
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
    subgraph_document(task)
}

fn stop_intent(subject: &str) -> String {
    let mut stop = KdlNode::new("stop");
    stop.entries_mut().push(KdlEntry::new(subject));
    subgraph_document(stop)
}

fn kdl_node<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> KdlNode {
    let mut node = KdlNode::new(name);
    node.entries_mut()
        .extend(values.into_iter().map(KdlEntry::new));
    node
}

fn subgraph_document(node: KdlNode) -> String {
    let mut children = KdlDocument::new();
    children.nodes_mut().push(node);
    let mut root = KdlNode::new("subgraph");
    root.set_children(children);
    let mut document = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    document.nodes_mut().push(version);
    document.nodes_mut().push(root);
    document.autoformat();
    document.to_string()
}

async fn apply_generated(
    client: &Client,
    kdl: String,
    source_name: String,
) -> Result<ApplyResponse> {
    let intent = IntentInput {
        kdl,
        source_name: Some(source_name),
    };
    let plan: PlanResponse = client
        .post(
            "/v1/intent/plan",
            &PlanRequest {
                intent: intent.clone(),
                at_index: None,
            },
        )
        .await?;
    anyhow::ensure!(plan.blockers.is_empty(), "{}", plan.blockers.join("; "));
    let resolved = plan.resolved_intent;
    client
        .post(
            "/v1/intent/apply",
            &ApplyRequest {
                idempotency_key: idempotency(&resolved.kdl, &plan.subject_tokens),
                intent: resolved,
                expected_subjects: plan.subject_tokens,
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
            .is_some()
        {
            return Ok(());
        }
        let events: Vec<EventRecord> = client
            .get(&format!(
                "/v1/events?after={cursor}&subject={}",
                urlencoding::encode(subject)
            ))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
            if event.kind == "action.failed" {
                anyhow::bail!("{} failed: {}", subject, event.body);
            }
        }
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
    if let Some(scope) = &args.scope {
        query.push(format!("scope={}", urlencoding::encode(scope)));
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
        if let Some(scope) = &args.scope {
            event_query.push(format!("scope={}", urlencoding::encode(scope)));
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
    loop {
        if let Some(value) = condition_value(client, subject, condition).await? {
            return Ok(value);
        }
        let events: Vec<EventRecord> = client
            .get(&format!(
                "/v1/events?after={cursor}&subject={}",
                urlencoding::encode(subject)
            ))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
        }
    }
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
    let fields = item
        .and_then(|item| item.actual.as_ref())
        .map(|actual| actual.get("fields").unwrap_or(actual));
    let actual_status = fields
        .and_then(|fields| fields.get("status"))
        .and_then(Value::as_str);
    let matches = match condition {
        "running" => matches!(actual_status, Some("running" | "ready")),
        "ready" => actual_status == Some("ready"),
        "exited" => actual_status == Some("exited"),
        "stopped" => {
            item.is_none_or(|item| item.actual.is_none())
                || matches!(actual_status, Some("stopped" | "removed"))
        }
        _ => false,
    };
    Ok(matches.then(|| json!(status)))
}

fn validate_wait_condition(condition: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(condition, "running" | "ready" | "exited" | "stopped")
            || matches!(
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

fn run_service(command: ServiceCommand) -> Result<()> {
    match command {
        ServiceCommand::Install { config } => {
            st3::service::install(Config::load(config.as_deref())?)
        }
        ServiceCommand::Status => st3::service::status(),
        ServiceCommand::Uninstall => st3::service::uninstall(),
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
    let intent = IntentInput {
        kdl,
        source_name: Some(source_name),
    };
    let plan: PlanResponse = client
        .post(
            "/v1/intent/plan",
            &PlanRequest {
                intent: intent.clone(),
                at_index: None,
            },
        )
        .await?;
    anyhow::ensure!(plan.blockers.is_empty(), "{}", plan.blockers.join("; "));
    let resolved_intent = plan.resolved_intent.clone();
    let response: ApplyResponse = client
        .post(
            "/v1/intent/apply",
            &ApplyRequest {
                idempotency_key: idempotency(&resolved_intent.kdl, &plan.subject_tokens),
                intent: resolved_intent,
                expected_subjects: plan.subject_tokens,
            },
        )
        .await?;
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
                    kind: "presence.observed".into(),
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
    if let Some(scope) = args.scope {
        query.push(format!("scope={}", urlencoding::encode(&scope)));
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
        if args.enrich {
            println!(
                "{}\t{}\t{}",
                agent.subject.trim_start_matches("agent/"),
                state,
                agent.reachability
            );
        } else {
            println!("{}\t{}", agent.subject.trim_start_matches("agent/"), state);
        }
    }
    Ok(())
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
                        kind: "resource.binding".into(),
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
                        kind: "resource.binding".into(),
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
    }
}

fn normalize_resource_subject(reference: &str) -> String {
    if reference.starts_with("resource/") {
        reference.to_owned()
    } else {
        format!("resource/{reference}")
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
                idempotency_key: None,
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

async fn run_review(client: &Client, command: ReviewCommand, json_output: bool) -> Result<()> {
    let (decision, args) = match command {
        ReviewCommand::Approve(args) => ("approved", args),
        ReviewCommand::Reject(args) => ("rejected", args),
        ReviewCommand::Revise(args) => ("revise", args),
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
        WorkCommand::Ls { assignee, all } => {
            let path = if let Some(assignee) = assignee {
                format!(
                    "/v1/work?assignee={}&include_terminal={all}",
                    urlencoding::encode(&assignee)
                )
            } else {
                format!("/v1/work?include_terminal={all}")
            };
            let work: Vec<StepRunView> = client.get(&path).await?;
            if json_output {
                return print_value(&work, true);
            }
            for step in work {
                println!(
                    "{}\t{}\t{}\t{}",
                    step.status,
                    step.assignee.as_deref().unwrap_or("-"),
                    step.subject,
                    step.title.as_deref().unwrap_or(&step.step)
                );
            }
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
                println!(
                    "{}\t{}\t{}",
                    step.status,
                    step.assignee.as_deref().unwrap_or("-"),
                    step.subject
                );
                if let Some(title) = step.title {
                    println!("Title: {title}");
                }
                if let Some(goal) = step.goal {
                    println!("Goal: {goal}");
                }
                Ok(())
            }
        }
        WorkCommand::Claim(args) => post_work(client, "claim", args, json_output).await,
        WorkCommand::Renew(args) => post_work(client, "renew", args, json_output).await,
        WorkCommand::Progress(args) => post_work(client, "progress", args, json_output).await,
        WorkCommand::Complete(args) => post_work(client, "complete", args, json_output).await,
        WorkCommand::Fail(args) => post_work(client, "fail", args, json_output).await,
        WorkCommand::Release(args) => post_work(client, "release", args, json_output).await,
        WorkCommand::PublishPlan(args) => publish_work_plan(client, args, json_output).await,
        WorkCommand::Revise(args) => {
            let actor = args
                .actor
                .context("a plan revision needs --as or ST_AGENT")?;
            let kdl = fs::read_to_string(&args.file)
                .with_context(|| format!("read KDL {}", args.file.display()))?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let response: PlanRunView = client
                .post(
                    &format!("/v1/plan-runs/{}/revision", urlencoding::encode(&args.run)),
                    &PlanRevisionRequest {
                        intent: IntentInput {
                            kdl,
                            source_name: Some(args.file.display().to_string()),
                        },
                        actor,
                        reason: args.reason,
                        idempotency_key: format!("plan-revision:{}:{nonce}", args.run),
                    },
                )
                .await?;
            print_value(&response, json_output)
        }
    }
}

async fn publish_work_plan(
    client: &Client,
    args: WorkPublishPlanArgs,
    json_output: bool,
) -> Result<()> {
    let actor = args
        .actor
        .context("publishing a plan output needs --as or ST_AGENT")?;
    let incarnation = match args.incarnation {
        Some(incarnation) => Some(incarnation),
        None => current_agent_incarnation(client, &actor).await?,
    };
    let kdl = fs::read_to_string(&args.file)
        .with_context(|| format!("read KDL {}", args.file.display()))?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let output: PlanOutputView = client
        .post(
            &format!("/v1/work/plan/{}", urlencoding::encode(&args.subject)),
            &PlanProductionRequest {
                intent: IntentInput {
                    kdl,
                    source_name: Some(args.file.display().to_string()),
                },
                actor,
                incarnation,
                idempotency_key: format!("plan-output:{}:{nonce}", args.subject),
            },
        )
        .await?;
    if json_output {
        print_value(&output, true)
    } else {
        println!("{}@{}", output.plan, output.revision);
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
        println!("{}\t{}", response.status, response.subject);
        if action == "claim" {
            if let Some(title) = response.title {
                println!("Title: {title}");
            }
            if let Some(goal) = response.goal {
                println!("Goal: {goal}");
            }
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
            let message = send_message(client, args).await?;
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
                },
            )
            .await?;
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

async fn send_message(client: &Client, args: MessageSendArgs) -> Result<MessageView> {
    let idempotency_key = format!(
        "message:{}",
        hex::encode(Sha256::digest(serde_json::to_vec(&(
            &args.from,
            &args.to,
            &args.body,
            &args.subject,
            &args.in_reply_to,
            &args.tags,
            now_ms(),
            std::process::id(),
        ))?))
    );
    client
        .post(
            "/v1/messages",
            &MessageSendRequest {
                idempotency_key,
                from: args.from,
                to: args.to,
                content: args.body,
                title: args.subject,
                in_reply_to: args.in_reply_to,
                tags: args.tags,
            },
        )
        .await
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
                lifecycle: "accepted".into(),
                actor: actor.map(str::to_owned),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: format!("message-accepted:{}", message.subject),
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

async fn run_judgement(client: &Client, args: JudgementArgs, json_output: bool) -> Result<()> {
    let response: ClaimRecord = client
        .post(
            "/v1/judgements",
            &JudgementRequest {
                idempotency_key: format!(
                    "judgement:{}:{}:{}",
                    args.operation_capability, args.verdict, args.reason
                ),
                operation_capability: args.operation_capability,
                verdict: args.verdict,
                reason: args.reason,
                evidence: args.evidence,
            },
        )
        .await?;
    print_value(&response, json_output)
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
            },
        )
        .await?;
    if json_output {
        print_value(&started, true)?;
    } else {
        println!("started {}", started.scope);
    }
    let subject = started
        .plan_run
        .context("the eval API did not return a plan run")?;
    let run: PlanRunView = client
        .get(&format!("/v1/plan-runs/{}", urlencoding::encode(&subject)))
        .await?;
    if args.graph {
        follow_eval_graph(client, &started.scope, &run.subject).await
    } else {
        follow_plan_run(client, run, json_output).await
    }
}

async fn run_graph(client: &Client, args: GraphArgs, json_output: bool) -> Result<()> {
    anyhow::ensure!(!json_output, "graph and --json cannot be used together");
    anyhow::ensure!(
        std::io::stdout().is_terminal(),
        "graph needs an interactive terminal"
    );
    let eval: EvalStatus = client
        .get(&format!("/v1/evals/{}", urlencoding::encode(&args.scope)))
        .await?;
    follow_eval_graph(client, &eval.scope, &eval.plan_run).await
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
    runs: Vec<PlanRunView>,
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

async fn follow_eval_graph(client: &Client, scope: &str, root: &str) -> Result<()> {
    let _screen = TerminalScreen::open()?;
    let started_at = Instant::now();
    let mut previous = BTreeMap::new();
    let mut transitions = Vec::new();
    let mut prior_signature = String::new();
    loop {
        let snapshot = load_eval_graph(client, scope, root).await?;
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
                    snapshot.eval.scope,
                    snapshot.eval.lifecycle
                )
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn load_eval_graph(client: &Client, scope: &str, root: &str) -> Result<EvalGraphSnapshot> {
    let eval: EvalStatus = client
        .get(&format!("/v1/evals/{}", urlencoding::encode(scope)))
        .await?;
    let runs: Vec<PlanRunView> = client
        .get(&format!("/v1/plan-runs?root={}", urlencoding::encode(root)))
        .await?;
    anyhow::ensure!(!runs.is_empty(), "the eval graph has no plan runs");
    Ok(EvalGraphSnapshot { eval, runs })
}

fn graph_node_states(snapshot: &EvalGraphSnapshot) -> BTreeMap<String, GraphNodeState> {
    let mut states = BTreeMap::new();
    states.insert(
        snapshot.eval.scope.clone(),
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
                    .plan
                    .strip_prefix("plan/")
                    .unwrap_or(&run.plan)
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
                    assignee: step.assignee.as_deref().map(short_actor).map(str::to_owned),
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
        .scope
        .strip_prefix("scope/eval/")
        .unwrap_or(&snapshot.eval.scope);
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
        .find(|run| run.subject == snapshot.eval.plan_run)
    {
        render_plan_steps(&mut output, root, &children, "  ");
    } else {
        let _ = writeln!(output, "  ! the root plan run is not available");
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

fn render_plan_steps(
    output: &mut String,
    run: &PlanRunView,
    children: &BTreeMap<&str, &PlanRunView>,
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
            child.plan.strip_prefix("plan/").unwrap_or(&child.plan),
            child.status,
            child.steps.len()
        );
        let _ = writeln!(output, "{child_summary}");
        if !matches!(child.status.as_str(), "completed" | "cancelled") {
            render_plan_steps(output, child, children, &format!("{indent}    "));
        }
    }
}

fn render_graph_step(output: &mut String, step: &StepRunView, indent: &str) {
    use std::fmt::Write as _;

    let actor = step
        .assignee
        .as_deref()
        .map(short_actor)
        .map(|actor| format!(" · {actor}"))
        .unwrap_or_default();
    let title = step.title.as_deref().unwrap_or(&step.step);
    let attempt = if step.attempt > 1 {
        format!(" · attempt {}", step.attempt)
    } else {
        String::new()
    };
    let _ = writeln!(
        output,
        "{indent}{} {:<10} {} — {}{}{}",
        graph_state_mark(&step.status),
        step.status,
        step.step.rsplit('/').next().unwrap_or(&step.step),
        title,
        actor,
        attempt
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
    let subject = format!("agent/{bus_id}");
    let status: StatusResponse = client
        .get(&format!(
            "/v1/status?subject={}",
            urlencoding::encode(&subject)
        ))
        .await?;
    let mut expected_subject = status
        .subjects
        .first()
        .map(|subject| {
            subject
                .desired_token
                .iter()
                .cloned()
                .chain(subject.conflicts.iter().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    expected_subject.sort();
    let worktree = fs::canonicalize(&args.worktree)
        .with_context(|| format!("resolve worktree {}", args.worktree.display()))?;
    let mut request = QuickAgentRequest {
        subject: subject.clone(),
        worktree: worktree.to_string_lossy().into_owned(),
        model: args.model,
        effort: args.effort,
        expected_subject,
        idempotency_key: String::new(),
    };
    request.idempotency_key = format!(
        "quick:{}",
        hex::encode(Sha256::digest(serde_json::to_vec(&(
            driver,
            &request.subject,
            &request.worktree,
            &request.model,
            &request.effort,
            &request.expected_subject,
        ))?))
    );
    let created: QuickAgentResponse = client.post(&format!("/v1/{driver}"), &request).await?;
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
                if event.kind == "harness.ready"
                    || (event.kind == "member.observed"
                        && event.body.pointer("/fields/status").and_then(Value::as_str)
                            == Some("ready"))
                {
                    ready = true;
                }
                if event.kind == "harness.error" || event.kind == "action.failed" {
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

async fn run_driver(client: &Client, args: DriverArgs, catalog: Option<&Path>) -> Result<()> {
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
    if matches!(args.driver.as_str(), "claude" | "pi" | "opencode") {
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
    let mut ready = false;
    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                #[cfg(unix)]
                let signal = {
                    use std::os::unix::process::ExitStatusExt as _;
                    status.signal()
                };
                let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                    subject: subject.into(),
                    kind: "member.observed".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("exited".into())),
                        ("exit_code".into(), status.code().map(Value::from).unwrap_or(Value::Null)),
                        ("exit_signal".into(), signal.map(Value::from).unwrap_or(Value::Null)),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                }).await?;
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
                return Ok(());
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)), if !ready && args.driver != "claude" => {
                let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                    subject: subject.into(),
                    kind: "harness.ready".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("ready".into())),
                        ("driver".into(), Value::String(args.driver.clone())),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                }).await?;
                ready = true;
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
    let mut ready = false;
    loop {
        tokio::select! {
            result = &mut task => {
                let outcome = result?;
                let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                    subject: subject.into(),
                    kind: "member.observed".into(),
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
                            kind: "harness.ready".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("status".into(), Value::String("ready".into())),
                                ("driver".into(), Value::String(driver.into())),
                                ("transport".into(), Value::String("native".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("native-ready:{subject}:{driver}")),
                        }).await?;
                        ready = true;
                    }
                    publish_harness_activity(client, subject, driver, &observed).await?;
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
                 warning: run `st2 claude-channel install` for unattended startup"
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

async fn publish_harness_activity(
    client: &Client,
    subject: &str,
    driver: &str,
    observed: &st2::harness_state::Observed,
) -> Result<()> {
    let status = match observed.state {
        st2::harness_state::Activity::Idle => "idle",
        st2::harness_state::Activity::Active | st2::harness_state::Activity::Child => "working",
        st2::harness_state::Activity::Ended => "exited",
        st2::harness_state::Activity::Unknown => "indeterminate",
    };
    let fields = BTreeMap::from([
        ("status".into(), Value::String(status.into())),
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
    let _: ClaimRecord = client
        .post(
            "/v1/claims",
            &ClaimInput {
                subject: subject.into(),
                kind: "harness.activity".into(),
                actor: Some(subject.into()),
                fields,
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("native-activity:{subject}:{fingerprint}")),
            },
        )
        .await?;
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
                            kind: "harness.activity".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("status".into(), Value::String(status.into())),
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
                        let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                            subject: message.into(),
                            kind: "message.delivered".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("status".into(), Value::String("delivered".into())),
                                ("recipient".into(), Value::String(subject.into())),
                                ("transport".into(), Value::String("pi-channel".into())),
                            ]),
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: Some(format!("pi-delivered:{subject}:{message}")),
                        }).await?;
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
    loop {
        tokio::select! {
            result = &mut task => return result?,
            _ = interval.tick() => {
                if !ready && state_dir.join("binding.json").is_file() {
                    let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                        subject: subject.into(),
                        kind: "harness.ready".into(),
                        actor: Some(subject.into()),
                        fields: BTreeMap::from([
                            ("status".into(), Value::String("ready".into())),
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
                    let status = match observed.state {
                        st2::harness_state::Activity::Idle => "idle",
                        st2::harness_state::Activity::Active | st2::harness_state::Activity::Child => "working",
                        st2::harness_state::Activity::Ended => "exited",
                        st2::harness_state::Activity::Unknown => "indeterminate",
                    };
                    let fields = BTreeMap::from([
                        ("status".into(), Value::String(status.into())),
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
                        kind: "harness.activity".into(),
                        actor: Some(subject.into()),
                        fields,
                        evidence: Vec::new(),
                        expected_subject: None,
                        idempotency_key: Some(format!("codex-activity:{subject}:{fingerprint}")),
                    }).await?;
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

async fn sync_work_messages(client: &Client, subject: &str) -> Result<()> {
    const TAG_PREFIX: &str = "st3-work:";
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
            "/v1/work?assignee={}&include_terminal=true",
            urlencoding::encode(subject)
        ))
        .await?;
    for message in messages
        .iter()
        .filter(|message| matches!(message.status.as_str(), "delivered" | "accepted"))
    {
        let Some((step_subject, attempt)) = work_message_target(message) else {
            continue;
        };
        if !work_message_was_acknowledged(&work, step_subject, attempt) {
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
        let tag_value = format!("{}@{}", step.subject, step.attempt);
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
            && candidate.assignee == step.assignee
            && candidate.step.len() < step.step.len()
            && step.step.starts_with(&format!("{}/", candidate.step))
    })
}

fn work_message_target(message: &MessageView) -> Option<(&str, u32)> {
    message.tags.iter().find_map(|tag| {
        tag.strip_prefix("st3-work:")
            .and_then(|value| value.rsplit_once('@'))
            .and_then(|(step_subject, attempt)| {
                attempt
                    .parse::<u32>()
                    .ok()
                    .map(|attempt| (step_subject, attempt))
            })
    })
}

fn work_message_was_acknowledged(work: &[StepRunView], step_subject: &str, attempt: u32) -> bool {
    work.iter().any(|step| {
        step.subject == step_subject
            && step.attempt == attempt
            && matches!(
                step.status.as_str(),
                "claimed" | "working" | "completed" | "failed" | "cancelled"
            )
    })
}

fn work_message_request(
    subject: &str,
    step: &StepRunView,
    tag_value: String,
) -> MessageSendRequest {
    MessageSendRequest {
        idempotency_key: format!("work-message:{}:{}", step.subject, step.attempt),
        from: "st3/runtime".into(),
        to: subject.into(),
        content: work_notification(step),
        title: Some(format!(
            "Plan step ready: {}",
            step.title.as_deref().unwrap_or(&step.step)
        )),
        in_reply_to: None,
        tags: vec![
            format!("st3-work:{tag_value}"),
            format!("plan-run:{}", step.run),
        ],
    }
}

fn work_notification(step: &StepRunView) -> String {
    format!(
        "A durable st3 plan step is ready. This Small Talk message contains the full assignment. Run `st3 work claim {0}` with plain output. Do not use `--json` or run help. A parent claim exposes its inherited nested steps. Those steps do not send separate Small Talk messages. Use plain `st3 work ls` to find, claim, and complete each ready nested step. The claim prints the step goal. Use `st3 work progress {0}` only for a material update. Finish with `st3 work complete {0}` or `st3 work fail {0}`. The `--evidence` option accepts stored claim IDs only.\n\nTitle: {1}\nGoal: {2}",
        step.subject,
        step.title.as_deref().unwrap_or(&step.step),
        step.goal
            .as_deref()
            .unwrap_or("Follow the step definition and publish its required graph products."),
    )
}

async fn renew_claimed_work(client: &Client, subject: &str, minute: u64) -> Result<()> {
    let work: Vec<StepRunView> = client
        .get(&format!(
            "/v1/work?assignee={}",
            urlencoding::encode(subject)
        ))
        .await?;
    for step in work.into_iter().filter(|step| {
        matches!(step.status.as_str(), "claimed" | "working")
            && step.lease_owner.as_deref() == Some(subject)
    }) {
        let _: StepRunView = client
            .post(
                &format!("/v1/work/renew/{}", urlencoding::encode(&step.subject)),
                &WorkRequest {
                    actor: Some(subject.into()),
                    incarnation: step.lease_incarnation,
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
        let _: ClaimRecord = client
            .post(
                "/v1/claims",
                &ClaimInput {
                    subject: message.subject.clone(),
                    kind: "message.delivered".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("delivered".into())),
                        ("recipient".into(), Value::String(subject.into())),
                        ("transport".into(), Value::String(transport.into())),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: Some(format!(
                        "native-delivered:{transport}:{subject}:{}",
                        message.subject
                    )),
                },
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
                            kind: "harness.ready".into(),
                            actor: Some(subject.into()),
                            fields: BTreeMap::from([
                                ("status".into(), Value::String("ready".into())),
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
                    let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                        subject: message.subject.clone(),
                        kind: "message.delivered".into(),
                        actor: Some(subject.into()),
                        fields: BTreeMap::from([
                            ("status".into(), Value::String("delivered".into())),
                            ("recipient".into(), Value::String(subject.into())),
                        ]),
                        evidence: Vec::new(),
                        expected_subject: None,
                        idempotency_key: Some(format!("message-delivered:{}:{subject}", message.subject)),
                    }).await?;
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
        let roots = document
            .nodes()
            .iter()
            .filter(|node| node.name().value() != "version")
            .collect::<Vec<_>>();
        let [root] = roots.as_slice() else {
            anyhow::bail!("{} does not contain exactly one root", file.display());
        };
        anyhow::ensure!(
            root.name().value() == "subgraph",
            "{} uses old or invalid KDL",
            file.display()
        );
        let body = root.children().context("an imported subgraph is empty")?;
        children.nodes_mut().extend(body.nodes().iter().cloned());
    }
    let mut root = KdlNode::new("subgraph");
    root.set_children(children);
    let mut document = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    document.nodes_mut().push(version);
    document.nodes_mut().push(root);
    document.autoformat();
    Ok(document.to_string())
}

fn print_plan(response: &PlanResponse, json_output: bool) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let member = intent.subjects["exec/cli-test"].member.as_ref().unwrap();
        assert_eq!(member.host, "node");
        assert_eq!(member.cwd, "/work/tree");
        assert_eq!(member.environment["MODE"], "test");
        assert_eq!(member.restart, st3::model::RestartType::Never);
        assert_eq!(
            member.launch,
            st3::model::LaunchSpec::Argv(vec!["printf".into(), "%s".into(), "hello".into()])
        );
    }

    #[test]
    fn wait_timeout_accepts_bounded_units_and_zero() {
        assert_eq!(parse_timeout("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_timeout("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_timeout("0").unwrap(), Duration::ZERO);
        assert!(parse_timeout("forever").is_err());
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
            run: "plan-run/run-1".into(),
            step: "build".into(),
            definition_hash: "definition".into(),
            status: "ready".into(),
            attempt: 2,
            assignee: Some("agent/worker".into()),
            title: Some("Build the change".into()),
            goal: Some("Implement and test the requested change.".into()),
            worker_reported: false,
            lease_owner: None,
            lease_incarnation: None,
            lease_expires_at_unix_ms: None,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        };

        let request = work_message_request("agent/worker", &step, "step-run/run-1/build@2".into());

        assert_eq!(request.from, "st3/runtime");
        assert_eq!(request.to, "agent/worker");
        assert_eq!(
            request.idempotency_key,
            "work-message:step-run/run-1/build:2"
        );
        assert_eq!(
            request.tags,
            ["st3-work:step-run/run-1/build@2", "plan-run:plan-run/run-1"]
        );
        assert!(
            request
                .content
                .contains("st3 work claim step-run/run-1/build")
        );
        assert!(
            request
                .content
                .contains("Implement and test the requested change.")
        );
        assert!(request.content.contains("Do not use `--json`"));

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
            Some(("step-run/run-1/build", 2))
        );
        assert!(!work_message_was_acknowledged(
            std::slice::from_ref(&step),
            "step-run/run-1/build",
            2
        ));
        step.status = "claimed".into();
        assert!(work_message_was_acknowledged(
            &[step],
            "step-run/run-1/build",
            2
        ));
    }

    #[test]
    fn inherited_nested_work_uses_the_parent_message() {
        let step = |subject: &str, path: &str, assignee: &str| StepRunView {
            subject: subject.into(),
            run: "plan-run/run-1".into(),
            step: path.into(),
            definition_hash: "definition".into(),
            status: "ready".into(),
            attempt: 1,
            assignee: Some(assignee.into()),
            title: None,
            goal: None,
            worker_reported: false,
            lease_owner: None,
            lease_incarnation: None,
            lease_expires_at_unix_ms: None,
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
    fn eval_graph_renders_nested_state_and_semantic_transitions() {
        let root_subject = "plan-run/root";
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
                scope: "scope/eval/demo/root".into(),
                plan_run: root_subject.into(),
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

        assert!(rendered.contains("ST3 EVAL GRAPH  demo/root"));
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
    ) -> PlanRunView {
        PlanRunView {
            subject: subject.into(),
            id: subject.strip_prefix("plan-run/").unwrap_or(subject).into(),
            plan: "plan/work".into(),
            revision: "revision".into(),
            root_revision: "root-revision".into(),
            root_plan_run: root.into(),
            parent_step_run: parent_step_run.map(str::to_owned),
            workspace: "/tmp/eval".into(),
            requester: "person/eval-requester".into(),
            run_scope: Some("scope/eval/demo/root".into()),
            mode: "eval".into(),
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
            step: step.into(),
            definition_hash: "definition".into(),
            status: status.into(),
            attempt: 1,
            assignee: assignee.map(str::to_owned),
            title: Some(match step.rsplit('/').next().unwrap_or(step) {
                "rename" | "change" => "Change the package".into(),
                "inspect" => "Inspect the package".into(),
                _ => step.into(),
            }),
            goal: None,
            worker_reported: false,
            lease_owner: None,
            lease_incarnation: None,
            lease_expires_at_unix_ms: None,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }
}
