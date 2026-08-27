use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use clap::{Args, CommandFactory as _, Parser, Subcommand, ValueEnum};
use kdl::{KdlDocument, KdlNode};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use st3::api::{AppState, router, serve_tcp, serve_unix};
use st3::archive::archive_cell;
use st3::client::{Client, Endpoint};
use st3::config::{Config, PeerConfig};
use st3::model::{
    ApplyRequest, ApplyResponse, AttachRequest, Attachment, ClaimInput, ClaimRecord,
    DocumentPutRequest, DocumentVersion, EvalStartRequest, EvalStartResponse, EventRecord,
    IntentInput, JudgementRequest, MessageLifecycleRequest, MessageSendRequest, MessageView,
    PlanRequest, PlanResponse, QuickAgentRequest, QuickAgentResponse, ReviewRequest,
    StatusResponse,
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
    Run(FileArgs),
    /// Apply all new-format KDL files in one directory tree.
    Import(ImportArgs),
    /// Store or read immutable documents.
    Doc {
        #[command(subcommand)]
        command: DocCommand,
    },
    /// Run one explicit eval cell.
    Eval(EvalArgs),
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
    /// Send and receive native graph messages.
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
struct ImportArgs {
    directory: PathBuf,
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
    cell: PathBuf,
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
}

#[derive(Subcommand)]
enum MessageCommand {
    Send(MessageSendArgs),
    Ls(MessageListArgs),
    Read(MessageReadArgs),
    Reply(MessageReplyArgs),
    Archive(MessageArchiveArgs),
    Thread(MessageArchiveArgs),
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
    #[arg(long)]
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

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("st3: {error:#}");
            let message = error.to_string();
            if message.contains("run `st3 up` first") || message.contains("connect to the st3 API")
            {
                ExitCode::from(5)
            } else if message.contains("stale-subject") {
                ExitCode::from(3)
            } else if message.contains("terminal status selected") {
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
        Command::Doc { command } => run_doc(&client, command, cli.json).await,
        Command::Eval(args) => run_eval(&client, args, cli.json).await,
        Command::Status(args) => run_status(&client, args, cli.json).await,
        Command::Agents(args) => run_agents(&client, args, cli.json).await,
        Command::Context { command } => run_context(&client, command, cli.json).await,
        Command::Resource { command } => run_resource(&client, command, cli.json).await,
        Command::Claim(args) => run_claim(&client, args, cli.json).await,
        Command::Review { command } => run_review(&client, command, cli.json).await,
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

async fn run_file(client: &Client, args: FileArgs, json_output: bool) -> Result<()> {
    let (kdl, source_name) = read_intent(args.file.as_deref())?;
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
                intent: resolved_intent,
                expected_subjects: plan.subject_tokens,
                idempotency_key,
            },
        )
        .await?;
    print_value(&response, json_output)
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
    print_value(&response, json_output)
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
            let (reference, actor) = positional_identity_and_reference(args.values, args.actor)?;
            let claim = close_message(client, &reference, actor.as_deref()).await?;
            sync_message_projection(client).await?;
            if json_output {
                print_value(&claim, true)
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
    client.get(&format!("/v1/messages/read/{reference}")).await
}

async fn accept_message(client: &Client, message: &MessageView, actor: Option<&str>) -> Result<()> {
    if message.status != "delivered" {
        return Ok(());
    }
    let reference = message.subject.trim_start_matches("message/");
    let _: ClaimRecord = client
        .post(
            &format!("/v1/messages/{reference}/claims"),
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
            &format!("/v1/messages/{reference}/claims"),
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
    let reference = Path::new(reference)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(reference)
        .trim_end_matches(".md")
        .trim_start_matches("message/");
    reference
        .rsplit_once('-')
        .map_or(reference, |(_, id)| id)
        .to_owned()
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
    let bundle = archive_cell(&args.cell)?;
    let bundle_hash = hex::encode(Sha256::digest(&bundle));
    let name = args
        .cell
        .file_name()
        .and_then(|name| name.to_str())
        .context("the eval cell name is not UTF-8")?
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
    let mut cursor = started.event_cursor;
    let mut verdict = None::<String>;
    loop {
        let events: Vec<EventRecord> = client
            .get(&format!(
                "/v1/events?after_index={cursor}&scope={}",
                urlencoding::encode(&started.scope)
            ))
            .await?;
        for event in events {
            cursor = cursor.max(event.store_index);
            if !json_output {
                println!("{} {} {}", event.store_index, event.kind, event.subject);
            }
            if event.kind == "eval.verdict" && event.subject == started.scope {
                verdict = Some(
                    event
                        .body
                        .pointer("/fields/verdict")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                );
            }
            if event.kind == "checkpoint.reached"
                && event.body.pointer("/fields/name").and_then(Value::as_str)
                    == Some("The temporary eval scope is empty")
            {
                let verdict = verdict.as_deref().unwrap_or("unknown");
                anyhow::ensure!(verdict == "pass", "eval verdict is {verdict}");
                return Ok(());
            }
        }
    }
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
        let _ = catalog.context("the Pi channel has no private driver catalog")?;
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
        return run_claude_mcp(client, subject).await;
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
                let _: ClaimRecord = client.post("/v1/claims", &ClaimInput {
                    subject: subject.into(),
                    kind: "member.observed".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("exited".into())),
                        ("exit_code".into(), status.code().map(Value::from).unwrap_or(Value::Null)),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: None,
                }).await?;
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
                if driver == "opencode" {
                    forward_projected_messages(client, subject, &inbox, &archive, "opencode-server").await?;
                }
            }
        }
    }
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
        }
    }
}

async fn forward_projected_messages(
    client: &Client,
    subject: &str,
    inbox: &Path,
    archive: &Path,
    transport: &str,
) -> Result<()> {
    const TAG_PREFIX: &str = "st3-message:";
    let present = projected_message_subjects(inbox, archive)?;
    let messages: Vec<MessageView> = client
        .get(&format!("/v1/messages?to={}", urlencoding::encode(subject)))
        .await?;
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
        let [root] = document.nodes() else {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_native_driver_gets_one_private_st2_projection() {
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
}
