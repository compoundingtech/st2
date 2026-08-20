//! st2 CLI. M0 exposes a single read-only command — `st2 ls <root>` — that slurps a catalog+inbox
//! folder and prints what it discovered (specs, warnings, errors). Reconcile/run land in later
//! milestones; this is the smoke test that discovery works end to end against a real folder.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, Parser, Subcommand};

use st2::{
    HostLock, Runner, SystemRunner, UpReport, detect_host, ding, discover, exec_state_dir, message,
    up_loop, up_once,
};

#[derive(Parser)]
#[command(
    name = "st2",
    version = st2::version::display_version(),
    about = "Harness-agnostic runner over a unified catalog+inbox folder"
)]
struct Cli {
    /// Catalog (or single-file fleet spec) to use. Defaults to $CATALOG, then
    /// ${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog.
    #[arg(long = "catalog", global = true, value_name = "PATH")]
    catalog_path: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Discover and print every agent spec under a catalog+inbox folder.
    Ls {
        /// Legacy positional catalog/spec path. Prefer --catalog; defaults to $CATALOG, then the
        /// default st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
    },
    /// Supervise a catalog+inbox folder: reconcile on a folder-watch + timer, keeping each agent's
    /// ptys running. With --once, do a single pass and exit.
    Up {
        /// Legacy positional catalog/spec path. Prefer --catalog; defaults to $CATALOG, then the
        /// default st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
        /// Host to filter on (which agents this machine runs). Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Do a single reconcile pass and exit, instead of looping.
        #[arg(long)]
        once: bool,
        /// Materialize every local agent's render block and exit without reconciling or spawning.
        #[arg(long, conflicts_with = "once")]
        materialize_only: bool,
        /// Limit materialization to one declared agent identity.
        #[arg(long)]
        agent: Option<String>,
        /// Select one exact local task. Use with --materialize-only to render only its owner, or
        /// with --once to render its owner and reconcile only that task.
        #[arg(long, conflicts_with = "agent")]
        task: Option<String>,
        /// Seconds between timer-driven reconcile passes when looping (folder changes reconcile
        /// immediately regardless).
        #[arg(long, default_value_t = 30)]
        interval: u64,
    },
    /// Native message bus: send/list/read/archive/reply over agents' `resources/inbox`.
    /// The stable wire format is a `<unix-ms>-<rand6>.md` Markdown file.
    #[command(subcommand)]
    Message(MessageCmd),
    /// Declared event streams: durable, bounded, idempotent ingress into an agent inbox.
    #[command(subcommand)]
    Event(EventCmd),
    /// Self-author declared event streams through the serialized catalog path.
    #[command(subcommand)]
    Stream(StreamCmd),
    /// Idempotent JSON request/reply transport for declared non-agent service principals.
    #[command(subcommand)]
    Request(RequestCmd),
    /// An agent's working-state context for lossless restart: read/write/append.
    #[command(subcommand)]
    Context(ContextCmd),
    /// An agent's linked resources (high-value output a peer can find): add/ls/read/remove.
    #[command(subcommand)]
    Resource(ResourceCmd),
    /// Install `st2 up` as a systemd-user service on headless Linux. macOS stays manual (TCC).
    /// Subcommands: install / status / uninstall.
    #[command(subcommand)]
    Service(ServiceCmd),
    /// Explicit lifecycle-hook management. `up` and materialization only verify; they never install
    /// or refresh hooks.
    #[command(subcommand)]
    Hooks(HooksCmd),
    /// Provider-native harness drivers and read-only typed-block expansion.
    #[command(subcommand)]
    Driver(DriverCmd),
    /// The ding sidecar: watch an agent's `resources/inbox` and poke its pty (`[DING] …`) on each new
    /// message. Busy does not suppress delivery; only fresh dnd defers FIFO. A startup backlog is
    /// coalesced into one recovery notice. Long-running — st2 keeps it alive as a task alongside the
    /// agent. Exits when the target pty session is gone.
    /// `st2 ping` is an alias (the maintainer is renaming ding → ping, since dinging is the runner's
    /// job now); it is the exact same command.
    #[command(visible_alias = "ping")]
    Ding {
        /// The target pty session to poke (a `pty` session ref). Optional — defaults to `--identity`
        /// (an agent IS its pty, so the session to poke is the identity), so `st2 ding --identity X`
        /// is the common form.
        session: Option<String>,
        /// Whose inbox to watch — bus id or identity. Defaults to `$ST_AGENT`. Also the default poke
        /// target when no positional session is given.
        #[arg(long)]
        identity: Option<String>,
        /// Catalog root. Defaults to `$CATALOG`.
        #[arg(long, conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
        /// Host used to resolve `<host>.<identity>` bus ids. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Poll/liveness cadence in milliseconds (folder changes poke immediately regardless).
        #[arg(long, default_value_t = 1000)]
        interval: u64,
    },
    /// Internal controlled Codex launch. Generated only for `deliver "app-server"` tasks.
    #[command(hide = true)]
    CodexAppServer {
        /// Exact agent bus identity that owns the controlled thread.
        #[arg(long)]
        identity: String,
        /// Exact reconciled PTY task identity for this runtime.
        #[arg(long)]
        runtime_id: String,
        /// Original structured Codex invocation, including its provider executable.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        codex_argv: Vec<String>,
    },
    /// Internal Claude MCP channel server started by Claude from its rendered project declaration.
    #[command(hide = true)]
    ClaudeMcp {
        #[arg(long)]
        identity: String,
    },
    /// Get or set an agent's presence status. No `--set` prints the status; no identity means yours
    /// (`$ST_AGENT`). Settable: offline | available | busy | away | dnd (`unknown` is derived).
    Status {
        /// Whose status — bus id or identity. Defaults to you (`--as` / `$ST_AGENT`).
        identity: Option<String>,
        /// Set your status to this state instead of printing it.
        #[arg(long = "set")]
        set: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Set or clear an agent's human-facing name without changing stable identity.
    Rename(PresentationArgs),
    /// Set or clear an agent's enduring responsibility description.
    Describe(PresentationArgs),
    /// Transactionally publish one canonical Agent Spec into the live catalog.
    #[command(subcommand)]
    Agent(AgentCmd),
    /// Canonical declaration snapshots and crash-recoverable whole-catalog application.
    #[command(subcommand)]
    Catalog(CatalogCmd),
    /// Explicit teardown: kill every live task of this host's catalog agents. The ONLY thing that ends
    /// tasks (stopping/crashing st2 never does). Idempotent.
    Down {
        /// Optional positional catalog/spec path. Prefer --catalog; defaults to $CATALOG, then the
        /// standard st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
        /// Host to tear down. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
    },
    /// Print shell exports for a catalog's bus — `eval "$(st2 env --catalog <catalog>)"` sets `CATALOG`/
    /// `ST_ROOT`/`PTY_ROOT` so native bus-aware tools target the catalog.
    Env {
        /// Optional positional catalog path. Prefer --catalog; defaults to $CATALOG, then the
        /// standard st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
    },
    /// Explicitly pre-trust workspaces in the ambient Claude and Codex configs. This is an operator
    /// utility for harnesses that use those ambient configs; `st2 up` never calls it automatically.
    /// Account-selecting commands should instead declare trust in the selected harness invocation.
    Pretrust {
        /// Workspace directories to mark trusted.
        #[arg(required = true)]
        dirs: Vec<PathBuf>,
    },
    /// Run an st2-spec eval end to end: copy the fixture, boot the team + judges, deliver the
    /// kickoff, wait for the sup's confirmation, run the judges → verdict. `st2 eval ./cells/<name>/`.
    Eval {
        /// The eval folder (or its `.kdl` spec file).
        folder: PathBuf,
        /// Host. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Preserve the run's temp catalog instead of deleting it — for inspecting the worker repo
        /// (`base..HEAD`), the judge outputs, and the bus after the run (e.g. a gate reproduction).
        /// Seats are still torn down (no leaks). Also honored via `ST2_EVAL_KEEP`.
        #[arg(long)]
        keep: bool,
        /// Emit the existing eval report as JSON without changing exit semantics.
        #[arg(long)]
        json: bool,
    },
    /// Run `pty` against this catalog's bus with the env auto-set, so pty subcommands and the
    /// interactive UI work without `eval "$(st2 env --catalog <catalog>)"` first. Catalog selection follows
    /// `--catalog`, `$CATALOG`, then the default st2 catalog. `CATALOG`/`ST_ROOT`/`PTY_ROOT` are
    /// exported for the child exactly as `st2 env` would. No arguments launches the interactive pty
    /// UI.
    Pty {
        /// Arguments passed through to `pty` verbatim (e.g. `ls`, `peek <session>`). None → the UI.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Drop into `$SHELL` with this catalog's bus env set (`CATALOG`/`ST_ROOT`/`PTY_ROOT`), so `pty`,
    /// bus-aware tools target the catalog for the whole session without `eval "$(st2 env …)"`.
    /// The general form of `st2 pty`. Catalog selection follows `--catalog`, `$CATALOG`, then the
    /// default st2 catalog; extra args go to the shell (e.g. `st2 shell -c "pty ls"`).
    Shell {
        /// Arguments passed through to `$SHELL` verbatim. None → an interactive shell.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Validate a rendered catalog against the runner contract (spec fields, folder layout, paths)
    /// so any renderer can confirm it hit the spec before running. One line per issue with
    /// a stable code; exits non-zero on any ERROR (`--strict` also fails on warnings). `--json` emits
    /// machine output for a renderer's build gate.
    Validate {
        /// Legacy positional catalog path. Prefer --catalog; defaults to $CATALOG, then the default
        /// st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
        /// Host whose external workspace/task paths should be checked. Structural checks always
        /// cover the whole catalog. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Fail (non-zero exit) on warnings too, not just errors.
        #[arg(long)]
        strict: bool,
        /// Emit the report as JSON instead of human-readable lines.
        #[arg(long)]
        json: bool,
    },
    /// Health check for a catalog: active agents alive, suspended agents not live, and retired
    /// agents fully absent. Exits non-zero on problems.
    Doctor {
        /// Legacy positional catalog path. Prefer --catalog; defaults to $CATALOG, then the default
        /// st2 catalog.
        #[arg(conflicts_with = "catalog_path")]
        root: Option<PathBuf>,
        /// Host to check. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Require a live long-running `st2 up` host lock. Omit for manual/--once operation.
        #[arg(long)]
        require_supervisor: bool,
    },
    /// List every agent in the catalog with presence and retirement state. `--json [--enrich]` is
    /// the stable machine-readable roster.
    Agents {
        /// The catalog folder (like `st2 ls`/`up`). Falls back to `--root`/`$CATALOG`.
        #[arg(conflicts_with = "catalog_path")]
        catalog: Option<PathBuf>,
        /// Only agents whose effective status matches (offline|available|busy|away|dnd|unknown).
        #[arg(long = "status")]
        status: Option<String>,
        /// Select one exact Agent Spec by its fully qualified `<host>.<identity>`.
        #[arg(long, value_name = "HOST.IDENTITY")]
        identity: Option<String>,
        /// Machine-readable JSON array, including retirement and declared Resource bindings.
        #[arg(long)]
        json: bool,
        /// With `--json`, add `lastActivity` + `inbox` count per agent.
        #[arg(long)]
        enrich: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Emit one fail-closed desired-task/runtime diagnostic snapshot. This is
    /// read-only observation, not reconciliation or cutover authority.
    Tasks {
        /// Host whose desired tasks and runtime generations to inspect. Defaults to this host.
        #[arg(long)]
        host: Option<String>,
        /// Emit the versioned machine-readable envelope. Required in v1.
        #[arg(long)]
        json: bool,
    },
    /// Clear one task's park after fixing what crash-looped it. A task parked by its `restart{}`
    /// policy (mode=fail) stays parked for the rest of the supervisor run, and this is its per-task
    /// exit: the running supervisor relaunches exactly this task on its next pass, leaving every
    /// other task on the host untouched. `st2 tasks --json` reports which tasks are parked.
    Unpark {
        /// The parked task's runtime id, exactly as `st2 tasks --json` reports it.
        task: String,
        /// Host whose selected-catalog supervisor should grant the request. Defaults to this host.
        #[arg(long)]
        host: Option<String>,
    },
    /// Print a shell completion script for `st2` to stdout (`st2 completions <bash|zsh|fish|…>`).
    /// Generated from the live command tree, so it never drifts from the actual flags.
    Completions {
        /// The shell to generate completions for.
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
enum DriverCmd {
    /// Print one typed driver block as plain Agent Spec KDL without running it.
    Expand {
        /// KDL declaration that contains the typed driver block.
        spec: PathBuf,
        /// Select one local or fully qualified identity when the file contains multiple agents.
        #[arg(long)]
        agent: Option<String>,
        /// Host fallback when neither the declaration nor its catalog path supplies one.
        #[arg(long)]
        host: Option<String>,
    },
    /// Run the existing controlled Codex app-server path.
    Codex {
        #[arg(long)]
        identity: String,
        #[arg(long)]
        runtime_id: String,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    /// Run the Claude session-owned MCP server over stdio.
    ClaudeMcp {
        #[arg(long)]
        identity: String,
    },
    /// Deprecated name for the Claude MCP server.
    // Keep this hidden command until no rendered configuration uses the old name.
    #[command(hide = true)]
    Claude {
        #[arg(long)]
        identity: String,
    },
    /// Run Claude under the session-owned presence wrapper.
    ClaudeSession {
        #[arg(long)]
        identity: String,
        #[arg(long)]
        runtime_id: String,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    /// Run pi under the session-owned presence wrapper.
    PiSession {
        #[arg(long)]
        identity: String,
        #[arg(long)]
        runtime_id: String,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    /// Run the pi native message channel over stdio, owned by the shipped pi extension.
    PiChannel {
        #[arg(long)]
        identity: String,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    /// Author reversible whole-agent lifecycle intent in one canonical KDL declaration.
    DesiredState {
        /// Exact bus identity, or a bare stable identity only when unique.
        identity: String,
        /// Desired whole-agent lifecycle state.
        #[arg(value_parser = ["running", "suspended", "retired"])]
        state: String,
        /// Required rationale for suspended/retired; forbidden for running.
        #[arg(long)]
        reason: Option<String>,
        /// Host used only to resolve declarations whose host is omitted.
        #[arg(long)]
        host: Option<String>,
        /// Emit a stable JSON authoring receipt.
        #[arg(long)]
        json: bool,
    },
    /// Compute the authoritative digest bound by `agent publish --input-sha256`.
    Digest {
        /// A canonical KDL file containing exactly one top-level `agent` node.
        #[arg(
            long,
            value_name = "FILE",
            required_unless_present = "bundle",
            conflicts_with = "bundle"
        )]
        spec: Option<PathBuf>,
        /// A create-only directory whose root contains exactly one canonical `agent.kdl`.
        #[arg(
            long,
            value_name = "DIR",
            required_unless_present = "spec",
            conflicts_with = "spec"
        )]
        bundle: Option<PathBuf>,
        /// Emit the typed source-digest receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Publish exactly one explicit-host, explicit-identity agent under a catalog-wide CAS lock.
    Publish {
        /// A canonical KDL file containing exactly one top-level `agent` node.
        #[arg(
            long,
            value_name = "FILE",
            required_unless_present = "bundle",
            conflicts_with = "bundle"
        )]
        spec: Option<PathBuf>,
        /// A create-only directory whose root contains exactly one canonical `agent.kdl`.
        #[arg(
            long,
            value_name = "DIR",
            required_unless_present = "spec",
            conflicts_with = "spec"
        )]
        bundle: Option<PathBuf>,
        /// Create only. An identical existing agent.kdl is reported as `unchanged`.
        #[arg(
            long,
            required_unless_present = "expect_sha256",
            conflicts_with = "expect_sha256"
        )]
        expect_absent: bool,
        /// Replace only when the current agent.kdl has this lowercase SHA-256.
        #[arg(
            long,
            value_name = "HEX",
            required_unless_present = "expect_absent",
            conflicts_with = "expect_absent"
        )]
        expect_sha256: Option<String>,
        /// SHA-256 returned by `st2 agent digest` for the exact source capability.
        #[arg(long, value_name = "HEX")]
        input_sha256: String,
        /// Emit the typed publication result as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum CatalogCmd {
    /// Compute the authoritative digest bound by `catalog apply --input-sha256`.
    Digest {
        /// Complete prepared declaration directory. Runtime state and control paths are rejected.
        #[arg(long, value_name = "DIR")]
        prepared: PathBuf,
        /// Emit the typed source-digest receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Compare one prepared declaration directory with the coherent live catalog without writing.
    Diff {
        /// Complete prepared declaration directory. Runtime state and control paths are rejected.
        #[arg(long, value_name = "DIR")]
        prepared: PathBuf,
        /// Expected canonical declaration-root SHA-256 of the live catalog.
        #[arg(long, value_name = "HEX")]
        expect_sha256: String,
        /// Emit the versioned semantic-diff receipt. Required in v1.
        #[arg(long)]
        json: bool,
    },
    /// Publish a complete prepared declaration directory as one absent catalog.
    Bootstrap {
        /// Complete prepared declaration directory. Runtime state and control paths are rejected.
        #[arg(long, value_name = "DIR")]
        prepared: PathBuf,
        /// Root SHA-256 of the exact prepared projection being published.
        #[arg(long, value_name = "HEX")]
        input_sha256: String,
        /// Emit the typed bootstrap receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Capture the coherent declaration plane into a create-only canonical directory.
    Snapshot {
        /// Destination directory. It must be outside the live catalog.
        #[arg(long, value_name = "DIR")]
        output: PathBuf,
        /// Hash and capture the declaration plane without parsing it. Only for repairing an
        /// invalid catalog; the captured directory remains unvalidated.
        #[arg(long)]
        raw_preimage: bool,
        /// Emit the typed snapshot receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Apply a complete canonical declaration directory under declaration-root CAS.
    Apply {
        /// Complete prepared declaration directory. Runtime state and control paths are rejected.
        #[arg(
            long,
            value_name = "DIR",
            required_unless_present = "resume",
            conflicts_with = "resume"
        )]
        prepared: Option<PathBuf>,
        /// Root SHA-256 of the exact prepared projection being applied.
        #[arg(
            long,
            value_name = "HEX",
            required_unless_present = "resume",
            conflicts_with = "resume"
        )]
        input_sha256: Option<String>,
        /// Expected canonical declaration-root SHA-256 of the live catalog.
        #[arg(
            long,
            value_name = "HEX",
            required_unless_present = "resume",
            conflicts_with = "resume"
        )]
        expect_sha256: Option<String>,
        /// Match the current declaration plane without parsing it. The prepared catalog is still
        /// fully validated, and this mode refuses an already-valid current catalog.
        #[arg(long, conflicts_with = "resume")]
        raw_preimage: bool,
        /// Resume the durable incomplete marker and internal stage without the original source.
        #[arg(long, conflicts_with_all = ["prepared", "input_sha256", "expect_sha256"])]
        resume: bool,
        /// Emit the typed application receipt as JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Shared context for message subcommands: where the catalog is, who "I" am, and the local host.
/// Defaults come from the same env st2 sets on every task it spawns (`$CATALOG`, `$ST_AGENT`), so a
/// running agent needs no flags.
#[derive(Args)]
struct MsgCtx {
    /// Legacy catalog/bus root override. Prefer global `--catalog`; defaults to `$CATALOG`, then the
    /// default st2 catalog.
    #[arg(long, conflicts_with = "catalog_path")]
    root: Option<PathBuf>,
    /// The acting identity — who the message is `from` / whose inbox is "mine". Defaults to
    /// `$ST_AGENT`.
    #[arg(long = "as")]
    as_id: Option<String>,
    /// Host used to resolve `<host>.<identity>` bus ids. Defaults to the local hostname.
    #[arg(long)]
    host: Option<String>,
}

#[derive(Args)]
struct PresentationArgs {
    /// Exact bus identity, or a bare stable identity only when unique in the selected catalog.
    identity: String,
    /// Presentation text. Use --clear to remove the field.
    #[arg(
        value_name = "TEXT",
        required_unless_present = "clear",
        conflicts_with = "clear"
    )]
    value: Option<String>,
    /// Remove the optional field.
    #[arg(long)]
    clear: bool,
    /// Emit a stable JSON receipt or classified refusal.
    #[arg(long)]
    json: bool,
    /// Host used only to resolve declarations whose host is omitted.
    #[arg(long)]
    host: Option<String>,
}

#[derive(Subcommand)]
enum ServiceCmd {
    /// Write the `st2.service` systemd-user unit, enable it (start on boot), and start it now.
    /// Idempotent — safe to re-run. The unit runs `st2 up --catalog <catalog>`; agents spawn in sibling
    /// scopes, so a service restart never cascades to them.
    Install {
        /// Legacy positional catalog/spec path for `st2 up`. Prefer --catalog; defaults to
        /// `$CATALOG`, then the default st2 catalog. It must exist at install time.
        #[arg(conflicts_with = "catalog_path")]
        catalog: Option<PathBuf>,
        /// Bake `--host <h>` into the unit. Omit to let `st2 up` auto-detect the hostname at runtime.
        #[arg(long)]
        host: Option<String>,
        /// Machine-local pty registry to export as PTY_ROOT in the unit. Omit to use
        /// `<catalog>/pty`. Useful when adopting live sessions from a legacy runner.
        #[arg(long)]
        pty_root: Option<PathBuf>,
        /// Supervisor memory ceiling (MiB). The agents live in sibling scopes and are NOT bounded.
        #[arg(long = "memory-max-mb", default_value_t = st2::service::DEFAULT_MEMORY_MAX_MB)]
        memory_max_mb: u64,
    },
    /// Show the `st2.service` systemd status.
    Status,
    /// Stop, disable, and remove the `st2.service` unit. Idempotent.
    Uninstall,
}

#[derive(Subcommand)]
enum HooksCmd {
    /// Atomically publish this binary's immutable hook set and select it with a receipt.
    Install {
        /// Select this binary's exact hook set even when it is older or cannot be ordered.
        #[arg(long)]
        replace: bool,
        /// Deprecated compatibility alias for `--replace`.
        #[arg(long, hide = true)]
        allow_downgrade: bool,
    },
    /// Read-only verification of the selected receipt and every embedded hook byte.
    Verify,
    /// Verify this binary's immutable hook set without requiring it to be selected.
    VerifyOwn,
}

#[derive(Subcommand)]
enum ResourceCmd {
    /// Link a resource (a URL you produced or reference) into your resource list.
    Add {
        /// The resource URL (any `scheme:` — http/https/file/pty/…).
        url: String,
        #[arg(long)]
        title: Option<String>,
        /// Comma-separated tags.
        #[arg(long = "tag", value_delimiter = ',')]
        tags: Vec<String>,
        /// A relation label (e.g. `output`, `reference`).
        #[arg(long)]
        relation: Option<String>,
        /// Read a body/notes from stdin.
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// List an agent's resources. Defaults to your own.
    Ls {
        identity: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Read one resource. With a leading identity, from that agent; otherwise your own.
    Read {
        first: String,
        second: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Remove one resource.
    Remove {
        first: String,
        second: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

#[derive(Subcommand)]
enum ContextCmd {
    /// Print an agent's context. Default = `now.md` (working state); `--decisions` the log; `--full` both.
    Read {
        /// Whose context — bus id or identity. Defaults to you (`$ST_AGENT`).
        identity: Option<String>,
        /// Print the decision log instead of the working state.
        #[arg(long)]
        decisions: bool,
        /// Print the working state and the decision log.
        #[arg(long)]
        full: bool,
        /// Print `now.md` only when it is newer than this many seconds.
        #[arg(long, value_name = "SECONDS")]
        fresh_within: Option<u64>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Overwrite an agent's working state (`now.md`) from stdin.
    Write {
        identity: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Append a single decision (with its reasoning) to the log.
    Append {
        identity: Option<String>,
        /// The decision — a single line.
        #[arg(long)]
        decision: String,
        /// Why — a single line.
        #[arg(long)]
        why: String,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

#[derive(Subcommand)]
enum MessageCmd {
    /// Send a new message to a recipient's inbox.
    Send {
        /// Recipient: a bus id (`<host>.<identity>`) or a bare identity in the catalog.
        to: String,
        /// The message body. Read from stdin when omitted.
        #[arg(short = 'm', long = "message")]
        body: Option<String>,
        #[arg(long)]
        subject: Option<String>,
        #[arg(long = "in-reply-to")]
        in_reply_to: Option<String>,
        /// Comma-separated tags.
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,
        /// Reuse one sender-owned operation result across exact retries.
        #[arg(long = "idempotency-key")]
        idempotency_key: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Reply to a message in your inbox — recipient and threading are derived from it.
    Reply {
        /// The message filename in your inbox to reply to.
        filename: String,
        /// The reply body. Read from stdin when omitted.
        #[arg(short = 'm', long = "message")]
        body: Option<String>,
        /// Override the subject (defaults to `re: <original subject>`).
        #[arg(long)]
        subject: Option<String>,
        /// Reuse one sender-owned operation result across exact retries.
        #[arg(long = "idempotency-key")]
        idempotency_key: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// List an inbox (or `--archive`), sorted by send time. Defaults to your own.
    Ls {
        /// Whose inbox — bus id or identity. Defaults to you (`--as` / `$ST_AGENT`).
        identity: Option<String>,
        /// List the archive instead of the inbox.
        #[arg(long)]
        archive: bool,
        /// Recovery-only: list the raw flat `<root>/<identity>` box without catalog resolution.
        #[arg(long)]
        orphan: bool,
        /// Print only the message count.
        #[arg(long)]
        count: bool,
        /// Include full message bodies in JSON output (opt-in; default shape is unchanged).
        #[arg(long)]
        include_body: bool,
        /// Show only messages from this sender.
        #[arg(long = "from")]
        from: Option<String>,
        /// Show only messages sent after this unix-millisecond timestamp.
        #[arg(long)]
        since: Option<u64>,
        /// Machine-readable JSON array.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// List sender-owned durable message history. Defaults to your own index.
    Sent {
        /// Whose sent index — bus id or identity. Defaults to you (`--as` / `$ST_AGENT`).
        identity: Option<String>,
        /// Print only the indexed message count. Refuses unavailable or partial coverage.
        #[arg(long)]
        count: bool,
        /// Include full message bodies in JSON output.
        #[arg(long)]
        include_body: bool,
        /// Show only messages addressed to this canonical recipient.
        #[arg(long = "to")]
        to: Option<String>,
        /// Show only messages sent after this unix-millisecond timestamp.
        #[arg(long)]
        since: Option<u64>,
        /// Machine-readable coverage envelope and rows.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Read one message. With a leading identity, read from that agent's box; otherwise your own.
    Read {
        /// Either the message filename, or an identity followed by a filename.
        first: String,
        /// The message filename (when `first` is an identity).
        second: Option<String>,
        /// Read from the archive instead of the inbox.
        #[arg(long)]
        archive: bool,
        /// Print the file verbatim (frontmatter + body), not a formatted view.
        #[arg(long)]
        raw: bool,
        /// Machine-readable JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Archive one message (inbox → archive). Defaults to your own inbox.
    Archive {
        /// Either the message filename, or an identity followed by a filename.
        first: String,
        /// The message filename (when `first` is an identity).
        second: Option<String>,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Show a message's thread — the message + everything replying to it (transitively), across the
    /// catalog. `--tree` indents by reply depth; otherwise flat chronological.
    Thread {
        /// Either the message filename, or an identity followed by a filename.
        first: String,
        /// The message filename (when `first` is an identity).
        second: Option<String>,
        /// Indented hierarchical output instead of flat chronological.
        #[arg(long)]
        tree: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

#[derive(Subcommand)]
enum EventCmd {
    /// Emit one producer-identified event into a declared agent stream.
    Emit {
        /// Owning agent: `<host>.<identity>` or a bare local identity.
        recipient: String,
        /// Declared stream name.
        #[arg(long)]
        stream: String,
        /// Stable producer-supplied event identity.
        #[arg(long = "event-id")]
        event_id: String,
        /// Producer grouping key used by --supersede.
        #[arg(long)]
        key: Option<String>,
        /// Archive the unread predecessor for the same key, or the stream-wide head without --key.
        #[arg(long)]
        supersede: bool,
        /// One-line wake-time summary.
        #[arg(long)]
        subject: Option<String>,
        /// Event body. Read from stdin when omitted.
        #[arg(short = 'm', long = "message")]
        body: Option<String>,
        /// Emit the stable machine receipt.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

#[derive(Subcommand)]
enum StreamCmd {
    /// Add a stream to your declaration, optionally with a supervised adapter launch.
    Add {
        name: String,
        /// Exact target agent; defaults to --as / $ST_AGENT.
        #[arg(long)]
        agent: Option<String>,
        /// Adapter command run under `sh -c`; omit both launch forms for external ingress.
        #[arg(long, conflicts_with = "adapter_argv")]
        command: Option<String>,
        /// Direct adapter argv after `--`. Element 0 is the program; values are preserved exactly.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        adapter_argv: Vec<String>,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Remove a stream from your declaration.
    Rm {
        name: String,
        /// Exact target agent; defaults to --as / $ST_AGENT.
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

#[derive(Subcommand)]
enum RequestCmd {
    /// Publish one idempotent JSON request from a declared service principal to an agent.
    Send {
        /// Recipient agent: a bus id (`<host>.<identity>`) or a local bare identity.
        to: String,
        #[arg(long = "idempotency-key")]
        idempotency_key: String,
        /// Typed request tag as `key=value` (repeatable).
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// JSON body. Read from stdin when omitted.
        #[arg(short = 'm', long = "message")]
        body: Option<String>,
        /// Emit the machine receipt as JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Decode one typed request from an agent's inbox.
    Read {
        request_filename: String,
        /// Emit the request envelope as JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Reply once to a typed request in an agent's inbox.
    Reply {
        request_filename: String,
        /// Typed reply tag as `key=value` (repeatable).
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// JSON body. Read from stdin when omitted.
        #[arg(short = 'm', long = "message")]
        body: Option<String>,
        /// Emit the machine receipt as JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Observe the typed reply for one previously published request.
    Status {
        #[arg(long = "idempotency-key")]
        idempotency_key: String,
        /// Emit the tagged status union as JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
}

fn main() -> Result<()> {
    let Cli {
        catalog_path,
        command,
    } = Cli::parse();
    initialize_catalog_env(catalog_path.as_deref())?;

    match command {
        Command::Ls { root } => {
            let root = catalog_arg(root)?;
            ls(&root)
        }
        Command::Up {
            root,
            host,
            once,
            materialize_only,
            interval,
            agent,
            task,
        } => {
            let root = catalog_arg(root)?;
            if task.is_some() && !materialize_only && !once {
                anyhow::bail!("--task requires --once or --materialize-only");
            }
            if agent.is_some() && !materialize_only {
                anyhow::bail!("--agent requires --materialize-only");
            }
            up(&root, host, once, materialize_only, interval, agent, task)
        }
        Command::Message(cmd) => message_cmd(cmd),
        Command::Event(cmd) => event_cmd(cmd),
        Command::Stream(cmd) => stream_cmd(cmd),
        Command::Request(cmd) => request_cmd(cmd),
        Command::Context(cmd) => context_cmd(cmd),
        Command::Resource(cmd) => resource_cmd(cmd),
        Command::Service(cmd) => service_cmd(cmd),
        Command::Hooks(cmd) => hooks_cmd(cmd),
        Command::Ding {
            session,
            identity,
            root,
            host,
            interval,
        } => ding_cmd(session, identity, root, host, interval),
        Command::CodexAppServer {
            identity,
            runtime_id,
            codex_argv,
        } => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::codex_app_server::run_controlled(&catalog, identity, runtime_id, codex_argv)
        }
        Command::ClaudeMcp { identity } => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::claude_mcp::run(&catalog, &identity)
        }
        Command::Driver(DriverCmd::Codex {
            identity,
            runtime_id,
            argv,
        }) => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::codex_app_server::run_controlled(&catalog, identity, runtime_id, argv)
        }
        Command::Driver(DriverCmd::ClaudeMcp { identity }) => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::claude_mcp::run(&catalog, &identity)
        }
        Command::Driver(DriverCmd::PiChannel { identity }) => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::pi_channel::run(&catalog, &identity)
        }
        Command::Driver(DriverCmd::PiSession {
            identity,
            runtime_id,
            argv,
        }) => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::pi_session::run(&catalog, identity, runtime_id, argv)
        }
        Command::Driver(DriverCmd::Claude { identity }) => {
            eprintln!("warning: `st2 driver claude` is deprecated; use `st2 driver claude-mcp`");
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::claude_mcp::run(&catalog, &identity)
        }
        Command::Driver(DriverCmd::ClaudeSession {
            identity,
            runtime_id,
            argv,
        }) => {
            let catalog = catalog_arg(None)?;
            let catalog = catalog.canonicalize().unwrap_or(catalog);
            st2::claude_session::run(&catalog, identity, runtime_id, argv)
        }
        Command::Driver(DriverCmd::Expand { spec, agent, host }) => {
            let catalog = catalog_arg(None)?;
            driver_expand_cmd(&catalog, &spec, agent.as_deref(), host.as_deref())
        }
        Command::Status { identity, set, ctx } => status_cmd(identity, set, ctx),
        Command::Rename(args) => presentation_cmd(st2::agent_author::PresentationField::Name, args),
        Command::Describe(args) => {
            presentation_cmd(st2::agent_author::PresentationField::Description, args)
        }
        Command::Agent(AgentCmd::DesiredState {
            identity,
            state,
            reason,
            host,
            json,
        }) => desired_state_cmd(identity, state, reason, host, json),
        Command::Agent(AgentCmd::Publish {
            spec,
            bundle,
            expect_absent,
            expect_sha256,
            input_sha256,
            json,
        }) => {
            let catalog = catalog_arg(None)?;
            let source = match (spec, bundle) {
                (Some(path), None) => st2::agent_publish::PublishSource::Spec(path),
                (None, Some(path)) => st2::agent_publish::PublishSource::Bundle(path),
                _ => unreachable!("clap enforces one publication source"),
            };
            let expectation = match (expect_absent, expect_sha256) {
                (true, None) => st2::agent_publish::PublishExpectation::Absent,
                (false, Some(hash)) => st2::agent_publish::PublishExpectation::Sha256(hash),
                _ => unreachable!("clap enforces one publication expectation"),
            };
            let result = st2::agent_publish::publish(st2::agent_publish::PublishRequest {
                catalog,
                source,
                expectation,
                input_sha256,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "{} {} {}",
                    match result.status {
                        st2::agent_publish::PublishStatus::Published => "published",
                        st2::agent_publish::PublishStatus::Unchanged => "unchanged",
                    },
                    result.bus_id,
                    result.path.display()
                );
            }
            Ok(())
        }
        Command::Agent(AgentCmd::Digest { spec, bundle, json }) => {
            let source = match (spec, bundle) {
                (Some(path), None) => st2::agent_publish::PublishSource::Spec(path),
                (None, Some(path)) => st2::agent_publish::PublishSource::Bundle(path),
                _ => unreachable!("clap enforces one source"),
            };
            let digest = st2::agent_publish::digest_source(source)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&digest)?);
            } else {
                println!("{}", digest.sha256);
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Bootstrap {
            prepared,
            input_sha256,
            json,
        }) => {
            let result =
                st2::catalog_transaction::bootstrap(st2::catalog_transaction::BootstrapRequest {
                    catalog: catalog_arg(None)?,
                    prepared,
                    input_sha256,
                })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "{} {}",
                    match result.status {
                        st2::catalog_transaction::BootstrapStatus::Created => "created",
                        st2::catalog_transaction::BootstrapStatus::Unchanged => "unchanged",
                    },
                    result.root_sha256
                );
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Digest { prepared, json }) => {
            let digest = st2::catalog_transaction::digest_prepared(&catalog_arg(None)?, &prepared)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&digest)?);
            } else {
                println!("{}", digest.root_sha256);
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Diff {
            prepared,
            expect_sha256,
            json,
        }) => {
            if !json {
                anyhow::bail!("`st2 catalog diff` v1 requires --json");
            }
            let result = st2::catalog_transaction::diff(st2::catalog_transaction::DiffRequest {
                catalog: catalog_arg(None)?,
                prepared,
                expect_sha256,
            })?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::Catalog(CatalogCmd::Snapshot {
            output,
            raw_preimage,
            json,
        }) => {
            let result =
                st2::catalog_transaction::snapshot(st2::catalog_transaction::SnapshotRequest {
                    catalog: catalog_arg(None)?,
                    output,
                    raw_preimage,
                })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "{} {} {}",
                    match result.status {
                        st2::catalog_transaction::SnapshotStatus::Created => "created",
                        st2::catalog_transaction::SnapshotStatus::Unchanged => "unchanged",
                    },
                    result.root_sha256,
                    result.output.display()
                );
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Apply {
            prepared,
            input_sha256,
            expect_sha256,
            raw_preimage,
            resume,
            json,
        }) => {
            let mode = if resume {
                st2::catalog_transaction::ApplyMode::Resume
            } else {
                let prepared = prepared.context("clap requires --prepared unless --resume")?;
                let input_sha256 =
                    input_sha256.context("clap requires --input-sha256 unless --resume")?;
                let expect_sha256 =
                    expect_sha256.context("clap requires --expect-sha256 unless --resume")?;
                if raw_preimage {
                    st2::catalog_transaction::ApplyMode::RawPreimage {
                        prepared,
                        input_sha256,
                        expect_sha256,
                    }
                } else {
                    st2::catalog_transaction::ApplyMode::Prepared {
                        prepared,
                        input_sha256,
                        expect_sha256,
                    }
                }
            };
            let result = st2::catalog_transaction::apply(st2::catalog_transaction::ApplyRequest {
                catalog: catalog_arg(None)?,
                mode,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "{} {}",
                    match result.status {
                        st2::catalog_transaction::ApplyStatus::Applied => "applied",
                        st2::catalog_transaction::ApplyStatus::Unchanged => "unchanged",
                    },
                    result.after_sha256
                );
            }
            Ok(())
        }
        Command::Agents {
            catalog,
            status,
            identity,
            json,
            enrich,
            ctx,
        } => agents_cmd(catalog, status, identity, json, enrich, ctx),
        Command::Tasks { host, json } => {
            if !json {
                anyhow::bail!("`st2 tasks` v1 requires --json");
            }
            let catalog = catalog_arg(None)?;
            tasks_cmd(&catalog, host)
        }
        Command::Unpark { task, host } => {
            let catalog = catalog_arg(None)?;
            unpark_cmd(&catalog, &task, host)
        }
        Command::Down { root, host } => {
            if root.is_none() && catalog_path.is_none() {
                anyhow::bail!(
                    "refusing implicit teardown target; pass --catalog <path> or an explicit catalog path"
                );
            }
            let root = catalog_arg(root)?;
            down_cmd(&root, host)
        }
        Command::Env { root } => {
            let root = catalog_arg(root)?;
            env_cmd(&root)
        }
        Command::Pretrust { dirs } => pretrust_cmd(&dirs),
        Command::Eval {
            folder,
            host,
            keep,
            json,
        } => eval_cmd(&folder, host, keep, json),
        Command::Validate {
            root,
            host,
            strict,
            json,
        } => {
            let root = catalog_arg(root)?;
            validate_cmd(&root, host, strict, json)
        }
        Command::Pty { args } => pty_cmd(&args),
        Command::Shell { args } => shell_cmd(&args),
        Command::Doctor {
            root,
            host,
            require_supervisor,
        } => {
            let root = catalog_arg(root)?;
            doctor_cmd(&root, host, require_supervisor)
        }
        Command::Completions { shell } => {
            // Generate from the live command tree so the script can never drift
            // from the actual flags (the flake gates this at build time).
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "st2", &mut std::io::stdout());
            Ok(())
        }
    }
}

fn driver_expand_cmd(
    catalog: &Path,
    path: &Path,
    agent: Option<&str>,
    host: Option<&str>,
) -> Result<()> {
    let (mut specs, warnings) = st2::discover_file(catalog, path)
        .with_context(|| format!("reading driver declaration {}", path.display()))?;
    for warning in warnings {
        eprintln!("warning: {warning}");
    }
    if let Some(agent) = agent {
        specs.retain(|spec| spec.identity == agent || spec.bus_id(host.unwrap_or("")) == agent);
    }
    anyhow::ensure!(
        specs.len() == 1,
        if agent.is_some() {
            format!(
                "{} contains {} matching agent blocks; expected exactly one",
                path.display(),
                specs.len()
            )
        } else {
            format!(
                "{} contains {} agent blocks; use --agent when it contains more than one",
                path.display(),
                specs.len()
            )
        }
    );
    let output = st2::driver::expand_driver(&specs[0], host.unwrap_or(""))?;
    print!("{output}");
    Ok(())
}

fn hooks_cmd(command: HooksCmd) -> Result<()> {
    match command {
        HooksCmd::Install {
            replace,
            allow_downgrade,
        } => {
            let dir = st2::hooks::install(replace || allow_downgrade)?;
            let root = st2::hooks::hooks_root()?;
            println!(
                "installed hook set {} in {}\nreceipt {}",
                st2::hooks::hookset_id(),
                dir.display(),
                root.join("current.json").display()
            );
        }
        HooksCmd::Verify => {
            let dir = st2::hooks::verify_installed()?;
            println!(
                "verified hook set {} in {}",
                st2::hooks::hookset_id(),
                dir.display()
            );
        }
        HooksCmd::VerifyOwn => {
            let dir = st2::hooks::verify_required_set()?;
            println!(
                "verified this binary's hook set {} in {}",
                st2::hooks::hookset_id(),
                dir.display()
            );
        }
    }
    Ok(())
}

fn down_cmd(root: &Path, host: Option<String>) -> Result<()> {
    // A single-file team spec: tear down the DECLARED team's sessions (symmetric with `st2 up`/`st2 ls`
    // over a spec — the "stop the fleet cleanly" verb). A catalog dir falls through to catalog teardown.
    if let Some(spec_file) = st2::eval_run::resolve_spec_path(root) {
        let (spec, spec_root) = st2::eval_run::load_spec(&spec_file)?;
        // Same host resolution as `st2 up <spec>`: --host › the spec's top-level host › OS hostname.
        let this_host = host
            .or_else(|| spec.host.clone())
            .unwrap_or_else(detect_host);
        let specs = st2::eval_run::spec_to_agent_specs(&spec.agents, &this_host, &spec_root);
        let runner = SystemRunner::new(spec_root, exec_state_dir(&this_host));
        let report = st2::down_specs(&specs, &this_host, &runner)?;
        println!(
            "teardown of spec {} on host '{this_host}':",
            spec_file.display()
        );
        print_report(&report);
        return Ok(());
    }

    let this_host = host.unwrap_or_else(detect_host);
    let catalog_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let runner = SystemRunner::new(catalog_root, exec_state_dir(&this_host));
    let report = st2::down(root, &this_host, &runner)?;
    println!("teardown on host '{this_host}':");
    print_report(&report);
    Ok(())
}

fn eval_cmd(folder: &Path, host: Option<String>, keep: bool, json: bool) -> Result<()> {
    let spec_file = st2::eval_run::resolve_spec_path(folder).with_context(|| {
        format!(
            "{} is not an st2 spec (a *.kdl file, or a folder with one)",
            folder.display()
        )
    })?;
    let keep = keep || std::env::var_os("ST2_EVAL_KEEP").is_some();
    if json {
        unsafe {
            std::env::set_var("ST2_EVAL_JSON", "1");
        }
    }
    let report = st2::eval_run::run_eval(&spec_file, host, keep)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        if report.passed() {
            return Ok(());
        }
        anyhow::bail!("VERDICT: FAIL")
    }

    if !report.done {
        println!(
            "(note: the team did not send a confirmation within max-timeout — judged the final state)"
        );
    }
    println!("\n== judges ==");
    let (mut pass, mut fail) = (0, 0);
    for j in &report.judges {
        if j.signal {
            // Show-but-don't-gate: runs + prints, but never counts toward SCORE/verdict.
            println!("  [SIGNAL] {}  ({})", j.name, j.detail);
            continue;
        }
        if j.passed {
            pass += 1;
        } else {
            fail += 1;
        }
        println!(
            "  {} {}  ({})",
            if j.passed { "[PASS]" } else { "[FAIL]" },
            j.name,
            j.detail
        );
    }
    println!(
        "SCORE: {pass} PASS / {fail} FAIL / {} gating judges",
        pass + fail
    );
    if report.passed() {
        println!("VERDICT: PASS");
        Ok(())
    } else {
        anyhow::bail!("VERDICT: FAIL")
    }
}

fn validate_cmd(root: &Path, host: Option<String>, strict: bool, json: bool) -> Result<()> {
    let catalog_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let host = host.unwrap_or_else(detect_host);
    let _catalog_lock = st2::CatalogLock::shared(&catalog_root)
        .context("acquire shared catalog-authoring lock for validation")?;
    let report = st2::validate::validate_for_host(&catalog_root, &host);
    let (errors, warnings) = (report.errors(), report.warnings());

    if json {
        let issues: Vec<serde_json::Value> = report
            .issues
            .iter()
            .map(|i| {
                serde_json::json!({
                    "severity": i.severity.tag(),
                    "code": i.code,
                    "path": i.path,
                    "agent": i.agent,
                    "message": i.message,
                })
            })
            .collect();
        let out = serde_json::json!({
            "schema": st2::validate::VALIDATE_RECEIPT_SCHEMA,
            "policyProfile": st2::validate::CORE_CATALOG_POLICY_PROFILE,
            "agentSpecRevision": agent_spec::AGENT_SPEC_REVISION,
            "issues": issues,
            "agents": report.agents,
            "errors": errors,
            "warnings": warnings,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        for i in &report.issues {
            println!("{}  {}: {}", i.severity.label(), i.path, i.message);
        }
        println!(
            "─ {errors} error{}, {warnings} warning{} across {} agent{}",
            plural(errors),
            plural(warnings),
            report.agents,
            plural(report.agents),
        );
    }

    // Exit non-zero on any error; under --strict, warnings fail too. Use a clean process exit so a
    // scriptable caller does not also get anyhow's "Error:" line after the report it already printed.
    if errors > 0 || (strict && warnings > 0) {
        std::process::exit(1);
    }
    Ok(())
}

/// The standard user catalog: `${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`.
fn default_catalog_root() -> Option<PathBuf> {
    nonempty_env_path("XDG_STATE_HOME")
        .map(|state| state.join("st2/default/catalog"))
        .or_else(|| {
            nonempty_env_path("HOME").map(|home| home.join(".local/state/st2/default/catalog"))
        })
}

fn nonempty_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Make even a not-yet-created output catalog absolute, so a later cwd change cannot retarget it.
fn absolute_catalog_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolving the current directory")?
            .join(path)
    };
    Ok(absolute.canonicalize().unwrap_or(absolute))
}

/// Seed `$CATALOG` once for every command. An explicit global flag wins over an inherited env var;
/// without either, the standard per-user catalog becomes the process default.
fn initialize_catalog_env(explicit: Option<&Path>) -> Result<()> {
    let selected = explicit
        .map(Path::to_path_buf)
        .or_else(|| nonempty_env_path("CATALOG"))
        .or_else(default_catalog_root);
    if let Some(path) = selected {
        let path = absolute_catalog_path(&path)?;
        // SAFETY: this is the first action after single-threaded CLI parsing, before any worker
        // threads or child processes exist.
        unsafe { std::env::set_var("CATALOG", path) };
    }
    Ok(())
}

/// Resolve an optional legacy positional path, otherwise use the shared catalog selection.
fn catalog_arg(explicit: Option<PathBuf>) -> Result<PathBuf> {
    match explicit {
        Some(path) => absolute_catalog_path(&path),
        None => catalog_root_for_env(),
    }
}

/// The selected catalog root for bus-aware commands: `$CATALOG`, then the standard user catalog.
/// `main` initializes `$CATALOG` from global `--catalog` before dispatch.
fn catalog_root_for_env() -> Result<PathBuf> {
    let root = nonempty_env_path("CATALOG")
        .or_else(default_catalog_root)
        .context(
            "no catalog selected: pass --catalog, set $CATALOG, or set $XDG_STATE_HOME/$HOME",
        )?;
    absolute_catalog_path(&root)
}

/// Set the same native catalog environment that `st2 env` prints. The catalog's own declared session
/// registry is used, not the caller's ambient one: these hand `pty` the roots of the *catalog*.
fn with_bus_env(cmd: &mut std::process::Command, root: &Path) {
    cmd.env("CATALOG", root)
        .env("ST_ROOT", root)
        .env("PTY_ROOT", st2::catalog::pty_root(root));
}

/// `st2 pty [<pty-args>…]` — a thin pass-through to `pty` with the catalog's bus env pre-set, so
/// the maintainer never has to `eval "$(st2 env …)"` first. **Replaces** this process with `pty` (via exec)
/// so the interactive UI keeps the tty, signals, and exit code.
fn pty_cmd(args: &[String]) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let root = catalog_root_for_env()?;
    let mut cmd = std::process::Command::new("pty");
    cmd.args(args);
    with_bus_env(&mut cmd, &root);
    // exec() only returns on failure (e.g. `pty` not on PATH).
    let err = cmd.exec();
    Err(anyhow::anyhow!("failed to exec `pty`: {err}"))
}

/// `st2 shell [<args>…]` — drop into `$SHELL` with the native catalog environment set. The general
/// form of `st2 pty`.
fn shell_cmd(args: &[String]) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let root = catalog_root_for_env()?;
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
    let mut cmd = std::process::Command::new(&shell);
    cmd.args(args);
    with_bus_env(&mut cmd, &root);
    let err = cmd.exec();
    Err(anyhow::anyhow!("failed to exec `{shell}`: {err}"))
}

fn env_cmd(root: &Path) -> Result<()> {
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let c = canonical.display();
    // The same roots st2 sets on every task it spawns.
    println!("export CATALOG={c}");
    println!("export ST_ROOT={c}");
    println!(
        "export PTY_ROOT={}",
        st2::catalog::pty_root(&canonical).display()
    );
    Ok(())
}

fn pretrust_cmd(dirs: &[PathBuf]) -> Result<()> {
    let n = st2::pretrust::pretrust(dirs)?;
    println!(
        "pre-trusted {n} workspace{} in the ambient Claude and Codex configs",
        if n == 1 { "" } else { "s" }
    );
    Ok(())
}

fn doctor_cmd(root: &Path, host: Option<String>, require_supervisor: bool) -> Result<()> {
    let this_host = host.unwrap_or_else(detect_host);
    let catalog = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    println!(
        "st2 doctor — catalog {}, host '{this_host}'",
        catalog.display()
    );
    let mut problems = 0usize;

    // 1) The tools a running fleet needs.
    report_check(
        &mut problems,
        tool_on_path("pty"),
        "`pty` on PATH",
        "not found",
    );

    // 2) Supervision mode. A one-shot/manual host intentionally has no lock. A caller that expects
    // a resident loop can opt into enforcing one; a stale file always indicates an unclean exit.
    let host_lock = st2::HostLock::new(root, &this_host);
    match host_lock.live_owner() {
        Some(_) => report_check(&mut problems, true, "supervisor (st2 up) running", ""),
        None if host_lock.has_stale_lock() => report_check(
            &mut problems,
            false,
            "supervision host-lock healthy",
            "stale host-lock from a dead supervisor",
        ),
        None if !require_supervisor => report_check(
            &mut problems,
            true,
            "supervision mode manual/--once (no live host-lock)",
            "",
        ),
        None => report_check(
            &mut problems,
            false,
            "supervisor (st2 up) running",
            "required but no live host-lock — run `st2 up`",
        ),
    }

    // 3) Per this-host declaration: active tasks require liveness and fresh presence; suspended
    // tasks require no live work; retired tasks require complete record absence.
    let _catalog_lock = st2::CatalogLock::shared(&catalog)
        .context("acquire shared catalog-authoring lock for doctor snapshot")?;
    let found = discover(&catalog);
    for e in &found.errors {
        report_check(
            &mut problems,
            false,
            &format!("catalog file {}", e.path.display()),
            &e.message,
        );
    }
    let runner = SystemRunner::new(catalog.clone(), exec_state_dir(&this_host));
    let sessions = match runner.list_sessions() {
        Ok(sessions) => sessions,
        Err(error) => {
            report_check(
                &mut problems,
                false,
                "task runtime readable",
                &format!("{error:#}"),
            );
            anyhow::bail!("{problems} problem(s) found");
        }
    };
    let live: std::collections::HashSet<String> = sessions
        .iter()
        .filter(|s| s.alive)
        .map(|s| s.pty_id.clone())
        .collect();
    let present: std::collections::HashMap<String, bool> = sessions
        .into_iter()
        .map(|session| (session.pty_id, session.alive))
        .collect();
    for spec in &found.specs {
        if spec.resolved_host(&this_host) != this_host {
            continue;
        }
        let bus_id = spec.bus_id(&this_host);
        if spec.desired_state.is_retired() {
            let still_present = spec
                .tasks
                .iter()
                .map(|task| {
                    task.id
                        .clone()
                        .unwrap_or_else(|| format!("{bus_id}.{}", task.name))
                })
                .filter_map(|id| {
                    present
                        .get(&id)
                        .map(|alive| format!("{id} ({})", if *alive { "alive" } else { "dead" }))
                })
                .collect::<Vec<_>>();
            report_check(
                &mut problems,
                still_present.is_empty(),
                &format!("{bus_id} retirement complete (all declared tasks absent)"),
                &format!("still present: {}", still_present.join(", ")),
            );
            continue;
        }
        if spec.desired_state.is_suspended() {
            let not_converged = spec
                .tasks
                .iter()
                .filter_map(|task| {
                    let id = task
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("{bus_id}.{}", task.name));
                    present.get(&id).and_then(|alive| {
                        (*alive || !(task.keep || spec.keep)).then(|| {
                            format!("{id} ({})", if *alive { "alive" } else { "dead non-keep" })
                        })
                    })
                })
                .collect::<Vec<_>>();
            report_check(
                &mut problems,
                not_converged.is_empty(),
                &format!("{bus_id} suspension effective (no live tasks)"),
                &format!("still present: {}", not_converged.join(", ")),
            );
            continue;
        }
        if !spec.has_delivery_transport() {
            report_advisory(
                &format!("{bus_id} delivery transport missing"),
                "declare `ding`, `deliver`, or a driver block; agent receives no DING",
            );
        }
        for task in &spec.tasks {
            let id = task
                .id
                .clone()
                .unwrap_or_else(|| format!("{bus_id}.{}", task.name));
            report_check(
                &mut problems,
                live.contains(&id),
                &format!("{bus_id} task '{}' alive", task.name),
                "session dead/missing",
            );
        }
        if let Some(dir) = spec.path.parent() {
            let path = st2::status::status_path(dir);
            if !path.is_file() {
                report_check(
                    &mut problems,
                    false,
                    &format!("{bus_id} presence missing"),
                    "no status file — is its session owner refreshing presence?",
                );
            } else {
                let state = st2::status::read_state(&path);
                report_check(
                    &mut problems,
                    state != st2::status::State::Unknown,
                    &format!("{bus_id} presence fresh (is `{}`)", state.as_str()),
                    "rotted to `unknown` — is its session owner refreshing presence?",
                );
            }
        }
    }

    if problems == 0 {
        println!("\n✓ all checks passed");
        Ok(())
    } else {
        anyhow::bail!("{problems} problem(s) found")
    }
}

fn tasks_cmd(root: &Path, host: Option<String>) -> Result<()> {
    let host = host.unwrap_or_else(detect_host);
    let catalog = match root.canonicalize() {
        Ok(catalog) => catalog,
        Err(error) => {
            let detail = format!("canonicalize catalog {}: {error}", root.display());
            let inventory =
                st2::task_inventory::TaskInventory::incomplete(root.to_path_buf(), host, detail);
            println!("{}", inventory.to_json());
            anyhow::bail!("task inventory incomplete")
        }
    };
    let before = match st2::catalog_lock::read_fence(&catalog) {
        Ok(fence) => fence,
        Err(error) => return print_incomplete_tasks(catalog, host, error.to_string()),
    };
    let found = discover(&catalog);
    let observed = match st2::catalog_lock::read_fence(&catalog) {
        Ok(fence) if fence == before => fence,
        Ok(_) => {
            return print_incomplete_tasks(
                catalog,
                host,
                "catalog generation changed during task discovery".to_string(),
            );
        }
        Err(error) => return print_incomplete_tasks(catalog, host, error.to_string()),
    };
    let runner = SystemRunner::new(catalog.clone(), exec_state_dir(&host));
    let parks = match st2::park::DirParkObserver::for_supervisor(&catalog, &host) {
        Ok(parks) => parks,
        Err(error) => {
            return print_incomplete_tasks(
                catalog,
                host,
                format!("open supervisor park projection: {error}"),
            );
        }
    };
    let mut inventory = st2::task_inventory::inventory(&catalog, &host, &found, &runner, &parks);
    let after = discover(&catalog);
    if !st2::task_inventory::same_discovery(&found, &after) {
        inventory.mark_incomplete("catalog declarations changed during task observation");
    }
    match st2::catalog_lock::read_fence(&catalog) {
        Ok(after) if after == observed => {}
        Ok(_) => inventory.mark_incomplete("catalog generation changed during task observation"),
        Err(error) => inventory.mark_incomplete(error.to_string()),
    }
    println!("{}", inventory.to_json());
    if inventory.complete() {
        Ok(())
    } else {
        anyhow::bail!("task inventory incomplete")
    }
}

/// Ask this host's supervisor to release one parked task.
///
/// The request is a file the supervisor drains at the top of its next pass, not a direct mutation:
/// the parked set lives in the supervisor's memory, and it is the only writer. That also means this
/// is best-effort by construction — with no supervisor running there is nothing to grant it, which is
/// why the confirmation says "requested" rather than claiming the task is back.
fn unpark_cmd(catalog: &Path, task: &str, host: Option<String>) -> Result<()> {
    let host = host.unwrap_or_else(detect_host);
    let catalog = catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", catalog.display()))?;
    let dir = st2::park::SupervisorScope::current(&catalog, &host)?.unpark_request_dir();
    st2::park::request_unpark(&dir, task)?;
    println!(
        "unpark requested for '{task}' in catalog {} on {host}; that supervisor grants it on its \
         next reconcile pass. Confirm with `st2 tasks --json` — the task's `parked` field clears.",
        catalog.display()
    );
    Ok(())
}

fn print_incomplete_tasks(catalog: PathBuf, host: String, detail: String) -> Result<()> {
    let inventory = st2::task_inventory::TaskInventory::incomplete(catalog, host, detail);
    println!("{}", inventory.to_json());
    anyhow::bail!("task inventory incomplete")
}

fn tool_on_path(tool: &str) -> bool {
    std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join(tool).is_file()))
        .unwrap_or(false)
}

fn report_check(problems: &mut usize, ok: bool, label: &str, detail: &str) {
    if ok {
        println!("  ✓ {label}");
    } else {
        *problems += 1;
        if detail.is_empty() {
            println!("  ✗ {label}");
        } else {
            println!("  ✗ {label} — {detail}");
        }
    }
}

fn report_advisory(label: &str, detail: &str) {
    println!("  ⚠ {label} — {detail}");
}

fn presentation_cmd(
    field: st2::agent_author::PresentationField,
    args: PresentationArgs,
) -> Result<()> {
    let PresentationArgs {
        identity,
        value,
        clear,
        json,
        host,
    } = args;
    let root = catalog_arg(None)?;
    let host = host.unwrap_or_else(detect_host);
    let actor = std::env::var("ST_AGENT")
        .ok()
        .filter(|value| !value.is_empty());
    let requested = if clear { None } else { value.as_deref() };
    match st2::agent_author::set_presentation(
        &root,
        &identity,
        &host,
        actor.as_deref(),
        field,
        requested,
    ) {
        Ok(receipt) => {
            if json {
                println!("{}", serde_json::to_string(&receipt)?);
            } else {
                let state = match (receipt.result, receipt.value.as_deref()) {
                    (st2::agent_author::AuthorOutcome::Changed, Some(value)) => {
                        format!("set to {value:?}")
                    }
                    (st2::agent_author::AuthorOutcome::Changed, None) => "cleared".to_owned(),
                    (st2::agent_author::AuthorOutcome::Unchanged, Some(value)) => {
                        format!("already {value:?}")
                    }
                    (st2::agent_author::AuthorOutcome::Unchanged, None) => {
                        "already clear".to_owned()
                    }
                };
                println!("{} {}: {state}", receipt.identity, field.as_str());
            }
            Ok(())
        }
        Err(error) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "result": "error",
                        "code": error.code(),
                        "identity": identity,
                        "field": field,
                        "error": error.to_string(),
                    })
                );
            }
            Err(error.into())
        }
    }
}

fn desired_state_cmd(
    identity: String,
    state: String,
    reason: Option<String>,
    host: Option<String>,
    json: bool,
) -> Result<()> {
    let state = match state.as_str() {
        "running" => st2::agent_author::DesiredStateValue::Running,
        "suspended" => st2::agent_author::DesiredStateValue::Suspended,
        "retired" => st2::agent_author::DesiredStateValue::Retired,
        _ => unreachable!("clap validates desired state"),
    };
    let root = catalog_arg(None)?;
    let host = host.unwrap_or_else(detect_host);
    let actor = std::env::var("ST_AGENT")
        .ok()
        .filter(|value| !value.is_empty());
    match st2::agent_author::set_desired_state(
        &root,
        &identity,
        &host,
        actor.as_deref(),
        state,
        reason.as_deref(),
    ) {
        Ok(receipt) => {
            if json {
                println!("{}", serde_json::to_string(&receipt)?);
            } else {
                println!(
                    "{} desired-state {}{} ({})",
                    receipt.identity,
                    receipt.desired_state.as_str(),
                    receipt
                        .reason
                        .as_deref()
                        .map(|reason| format!(" reason={reason:?}"))
                        .unwrap_or_default(),
                    match receipt.result {
                        st2::agent_author::AuthorOutcome::Changed => "changed",
                        st2::agent_author::AuthorOutcome::Unchanged => "unchanged",
                    }
                );
            }
            Ok(())
        }
        Err(error) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "result": "error",
                        "code": error.code(),
                        "identity": identity,
                        "desiredState": state,
                        "error": error.to_string(),
                    })
                );
            }
            Err(error.into())
        }
    }
}

fn status_cmd(identity: Option<String>, set: Option<String>, ctx: MsgCtx) -> Result<()> {
    let (root, host) = resolve_ctx(&ctx)?;
    let id = match identity {
        Some(i) => i,
        None => acting_id(&ctx)?,
    };
    let sp = st2::status::status_path(&agent_dir_of(&root, &id, &host)?);
    match set {
        None => println!("{}", st2::status::read_state(&sp).as_str()),
        Some(word) => {
            let state = st2::status::State::parse_settable(&word).with_context(|| {
                format!("invalid state '{word}' (settable: offline|available|busy|away|dnd)")
            })?;
            message::with_resolved_agent_dir(&root, &id, &host, |agent| {
                st2::status::set_state(&st2::status::status_path(agent), state)
            })?;
            println!("status: {}", state.as_str());
        }
    }
    Ok(())
}

fn agents_cmd(
    catalog: Option<PathBuf>,
    status_filter: Option<String>,
    identity: Option<String>,
    json: bool,
    enrich: bool,
    mut ctx: MsgCtx,
) -> Result<()> {
    if enrich && !json {
        anyhow::bail!("--enrich requires --json");
    }
    // A positional catalog (like `st2 ls`) takes precedence over `--root`/`$CATALOG`.
    if catalog.is_some() {
        ctx.root = catalog;
    }
    let (root, host) = resolve_ctx(&ctx)?;
    let _catalog_lock = st2::CatalogLock::shared(&root)
        .context("acquire shared catalog-authoring lock for agent roster")?;
    let found = if identity.is_some() {
        st2::discover_strict(&root)
    } else {
        st2::discover(&root)
    };
    if identity.is_some() && !found.errors.is_empty() {
        let errors = found
            .errors
            .iter()
            .map(|error| format!("{}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!(
            "cannot select an exact Agent Spec while catalog discovery has {} error(s): {errors}",
            found.errors.len()
        );
    }
    let mut rows = st2::agents::roster_from_discovered(&found, &host);
    if let Some(identity) = &identity {
        rows.retain(|row| row.identity == *identity);
        anyhow::ensure!(
            rows.len() == 1,
            "expected exactly one Agent Spec with identity `{identity}`, found {}",
            rows.len()
        );
    }
    if let Some(f) = &status_filter {
        rows.retain(|r| r.status.as_str() == f);
        if let Some(identity) = &identity {
            anyhow::ensure!(
                rows.len() == 1,
                "Agent Spec `{identity}` does not match status `{f}`"
            );
        }
    }
    if json {
        println!("{}", st2::agents::to_json(&rows, enrich));
    } else {
        for r in &rows {
            let lifecycle = if r.desired_state == "running" {
                String::new()
            } else {
                format!(
                    "\t[{}{}]",
                    r.desired_state,
                    r.desired_state_reason
                        .as_deref()
                        .map(|reason| format!(": {reason}"))
                        .unwrap_or_default()
                )
            };
            println!(
                "{}\t{}\t{}\t{}{}",
                r.identity,
                r.status.as_str(),
                r.name.as_deref().unwrap_or(""),
                r.description.as_deref().unwrap_or(""),
                lifecycle,
            );
        }
    }
    Ok(())
}

fn ding_cmd(
    session: Option<String>,
    identity: Option<String>,
    root: Option<PathBuf>,
    host: Option<String>,
    interval: u64,
) -> Result<()> {
    let ctx = MsgCtx {
        root,
        as_id: identity,
        host,
    };
    let (catalog_root, this_host) = resolve_ctx(&ctx)?;
    let id = acting_id(&ctx)?;
    // The pty to poke defaults to the identity — an agent IS its pty, so the session id == the agent
    // id. So `st2 ding --identity mix.worker` pokes pty `mix.worker` (the redundant positional is now
    // optional). An explicit positional still overrides for the rare non-agent case.
    let session = session.unwrap_or_else(|| id.clone());
    // Flat-bus aware: a native catalog agent → its resources/inbox; a catalog-LESS bus (an eval's
    // ST_ROOT) → the flat <root>/<id>/inbox. Status lives beside it either way.
    let agent_dir = message::resolve_agent_dir(&catalog_root, &id, &this_host)?
        .unwrap_or_else(|| catalog_root.join(&id));
    let inbox = resolve_message_inbox(&catalog_root, &id, &this_host)?;
    let status_path = st2::status::status_path(&agent_dir);
    eprintln!(
        "st2 ding: watching {}'s inbox ({}) → poking pty '{session}'",
        id,
        inbox.display()
    );
    let config = ding::DingConfig {
        poll: Duration::from_millis(interval),
        ..Default::default()
    };
    ding::serve(
        &catalog_root,
        &this_host,
        &id,
        &inbox,
        &status_path,
        &session,
        &config,
    )
}

/// Resolve the catalog root and local host from a message subcommand's shared context.
fn resolve_ctx(ctx: &MsgCtx) -> Result<(PathBuf, String)> {
    let root = match &ctx.root {
        Some(root) => absolute_catalog_path(root)?,
        None => catalog_root_for_env()?,
    };
    let host = ctx.host.clone().unwrap_or_else(detect_host);
    Ok((root, host))
}

/// The acting identity (`from` / whose inbox is "mine"): `--as`, else `$ST_AGENT`.
fn acting_id(ctx: &MsgCtx) -> Result<String> {
    ctx.as_id
        .clone()
        .or_else(|| std::env::var("ST_AGENT").ok())
        .filter(|s| !s.is_empty())
        .context("no acting identity: pass --as or set $ST_AGENT")
}

/// Resolve a recipient/identity to its agent folder in the catalog, or a clear error.
fn agent_dir_of(root: &Path, id: &str, host: &str) -> Result<PathBuf> {
    message::resolve_agent_dir(root, id, host)?
        .with_context(|| format!("no agent '{id}' found in catalog {}", root.display()))
}

/// Resolve ordinary declared messaging authority plus the exact external requester capability
/// injected only into canonical eval seats.
fn resolve_message_inbox(root: &Path, id: &str, host: &str) -> Result<PathBuf> {
    let external = std::env::var("ST2_EVAL_REQUESTER")
        .ok()
        .map(|identity| message::ExternalInbox::new(root, &identity))
        .transpose()?;
    message::resolve_inbox_with_external(root, id, host, external.as_ref())
}

/// Body from `-m`, else stdin (so `st2 message send x < file` works).
fn body_or_stdin(body: Option<String>) -> Result<String> {
    match body {
        Some(b) => Ok(b),
        None => {
            use std::io::Read as _;
            let mut s = String::new();
            std::io::stdin()
                .read_to_string(&mut s)
                .context("reading message body from stdin")?;
            Ok(s)
        }
    }
}

/// `[identity] <filename>` positionals: if `second` is present, `first` is the identity; otherwise
/// `first` is the filename and the box belongs to the acting identity.
fn box_target(first: String, second: Option<String>, ctx: &MsgCtx) -> Result<(String, String)> {
    match second {
        Some(filename) => Ok((first, filename)),
        None => Ok((acting_id(ctx)?, first)),
    }
}

fn message_cmd(cmd: MessageCmd) -> Result<()> {
    match cmd {
        MessageCmd::Send {
            to,
            body,
            subject,
            in_reply_to,
            tags,
            idempotency_key,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let from = acting_id(&ctx)?;
            let body = body_or_stdin(body)?;
            let filename = send_resolved_message(
                &root,
                &to,
                &host,
                &from,
                subject.as_deref(),
                in_reply_to.as_deref(),
                &tags,
                &body,
                idempotency_key.as_deref(),
            )?;
            println!("{filename}");
            Ok(())
        }
        MessageCmd::Reply {
            filename,
            body,
            subject,
            idempotency_key,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let from = acting_id(&ctx)?;
            let my_inbox = resolve_message_inbox(&root, &from, &host)?;
            let original = message::read_msg(&my_inbox, &filename)
                .or_else(|inbox_error| {
                    if my_inbox.join(&filename).try_exists()? {
                        return Err(inbox_error);
                    }
                    let my_archive = message::resolve_archive(&root, &from, &host)?;
                    message::read_msg(&my_archive, &filename)
                })
                .with_context(|| format!("no message '{filename}' in {}'s inbox", from))?;
            let to = original
                .from
                .clone()
                .with_context(|| format!("message '{filename}' has no `from` to reply to"))?;
            let subject = subject.or_else(|| message::reply_subject(original.subject.as_deref()));
            let body = body_or_stdin(body)?;
            let sent = send_resolved_message(
                &root,
                &to,
                &host,
                &from,
                subject.as_deref(),
                Some(&filename),
                &[],
                &body,
                idempotency_key.as_deref(),
            )?;
            println!("{sent}");
            Ok(())
        }
        MessageCmd::Sent {
            identity,
            count,
            include_body,
            to,
            since,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = identity.unwrap_or(acting_id(&ctx)?);
            let mut view = message::with_resolved_agent_dir(&root, &id, &host, |agent_dir| {
                message::list_sent(agent_dir, include_body)
            })?;
            if let Some(recipient) = &to {
                view.messages.retain(|message| message.to == *recipient);
            }
            if let Some(cursor) = since {
                view.messages.retain(|message| message.ts > cursor);
            }
            if count {
                match view.coverage {
                    st2_wire::message::SentCoverage::Since { .. } => {
                        println!("{}", view.messages.len());
                        return Ok(());
                    }
                    st2_wire::message::SentCoverage::Unavailable => {
                        anyhow::bail!("sent-message coverage is unavailable")
                    }
                    st2_wire::message::SentCoverage::Partial { pending, .. } => {
                        anyhow::bail!("sent-message coverage is partial ({pending} pending)")
                    }
                }
            }
            if json {
                println!("{}", serde_json::to_string(&view)?);
                return Ok(());
            }
            let coverage = match view.coverage {
                st2_wire::message::SentCoverage::Unavailable => "unavailable".to_string(),
                st2_wire::message::SentCoverage::Since { since } => format!("since {since}"),
                st2_wire::message::SentCoverage::Partial { since, pending } => {
                    format!("partial since {since}; {pending} pending")
                }
            };
            println!(
                "# {} sent message{} for {id} ({coverage})",
                view.messages.len(),
                plural(view.messages.len())
            );
            for message in &view.messages {
                let subject = message.subject.as_deref().unwrap_or("(no subject)");
                println!("{}  to {}  {subject}", message.filename, message.to);
            }
            Ok(())
        }
        MessageCmd::Ls {
            identity,
            archive,
            orphan,
            count,
            include_body,
            from,
            since,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = match identity {
                Some(id) => id,
                None => acting_id(&ctx)?,
            };
            let dir = message::resolve_list_box(&root, &id, &host, archive, orphan)?;
            let mut msgs = if archive {
                message::list_dir(&dir)?
            } else {
                message::list_inbox(&dir)?
            };
            if let Some(sender) = &from {
                msgs.retain(|m| m.from.as_deref() == Some(sender.as_str()));
            }
            if let Some(cursor) = since {
                msgs.retain(|m| m.ts_ms > cursor);
            }
            if count {
                println!("{}", msgs.len());
                return Ok(());
            }
            if json {
                let items: Vec<LsItemJson> = msgs
                    .iter()
                    .map(|m| LsItemJson::from_message(m, include_body))
                    .collect();
                println!("{}", serde_json::to_string(&items)?);
                return Ok(());
            }
            let box_name = if archive { "archive" } else { "inbox" };
            println!(
                "# {} message{} in {id} {box_name}",
                msgs.len(),
                plural(msgs.len())
            );
            for m in &msgs {
                let from = m.from.as_deref().unwrap_or("<unknown>");
                let subj = m.subject.as_deref().unwrap_or("(no subject)");
                println!("{}  from {from}  {subj}", m.filename);
            }
            Ok(())
        }
        MessageCmd::Read {
            first,
            second,
            archive,
            raw,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let (id, filename) = box_target(first, second, &ctx)?;
            let dir = if archive {
                message::resolve_archive(&root, &id, &host)
            } else {
                resolve_message_inbox(&root, &id, &host)
            }?;
            if raw {
                print!("{}", std::fs::read_to_string(dir.join(&filename))?);
                return Ok(());
            }
            let m = message::read_msg(&dir, &filename)?;
            if json {
                println!("{}", serde_json::to_string(&MessageJson::from(&m))?);
                return Ok(());
            }
            println!("from:        {}", m.from.as_deref().unwrap_or("<unknown>"));
            if let Some(s) = &m.subject {
                println!("subject:     {s}");
            }
            if let Some(irt) = &m.in_reply_to {
                println!("in-reply-to: {irt}");
            }
            if !m.tags.is_empty() {
                println!("tags:        {}", m.tags.join(", "));
            }
            println!();
            print!("{}", m.body);
            Ok(())
        }
        MessageCmd::Archive { first, second, ctx } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let (id, filename) = box_target(first, second, &ctx)?;
            message::archive_resolved_message(&root, &id, &host, &filename)?;
            println!("archived");
            Ok(())
        }
        MessageCmd::Thread {
            first,
            second,
            tree,
            ctx,
        } => {
            let (root, _host) = resolve_ctx(&ctx)?;
            // `[identity] filename` — the identity is irrelevant (the walk is catalog-wide).
            let filename = second.unwrap_or(first);
            let mut entries = message::collect_thread(&root, &filename)?;
            if entries.is_empty() {
                anyhow::bail!(
                    "no thread found for '{filename}' in catalog {}",
                    root.display()
                );
            }
            if !tree {
                entries.sort_by_key(|e| e.ts_ms); // flat chronological
            }
            for e in &entries {
                let indent = if tree {
                    "  ".repeat(e.depth)
                } else {
                    String::new()
                };
                let from = e.from.as_deref().unwrap_or("<unknown>");
                let subj = e.subject.as_deref().unwrap_or("(no subject)");
                println!("{indent}{}  from {from}  {subj}", e.filename);
            }
            Ok(())
        }
    }
}

fn event_admission_host() -> String {
    #[cfg(debug_assertions)]
    if let Ok(host) = std::env::var("ST2_TEST_EVENT_HOST") {
        return host;
    }
    detect_host()
}

fn send_resolved_message(
    root: &Path,
    to: &str,
    host: &str,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> Result<String> {
    let external = std::env::var("ST2_EVAL_REQUESTER")
        .ok()
        .map(|identity| message::ExternalInbox::new(root, &identity))
        .transpose()?;
    message::send_to_resolved_inbox(
        root,
        to,
        host,
        from,
        subject,
        in_reply_to,
        tags,
        body,
        idempotency_key,
        external.as_ref(),
    )
}

fn event_cmd(cmd: EventCmd) -> Result<()> {
    match cmd {
        EventCmd::Emit {
            recipient,
            stream,
            event_id,
            key,
            supersede,
            subject,
            body,
            json,
            ctx,
        } => {
            let (root, _caller_host) = resolve_ctx(&ctx)?;
            let host = event_admission_host();
            let body = body_or_stdin(body)?;
            let receipt = st2::event::emit(
                &root,
                &host,
                &recipient,
                &stream,
                &event_id,
                key.as_deref(),
                subject.as_deref(),
                &body,
                supersede,
            )?;
            if json {
                println!("{}", serde_json::to_string(&receipt)?);
            } else {
                println!("{}", receipt.filename);
            }
            Ok(())
        }
    }
}

fn stream_cmd(cmd: StreamCmd) -> Result<()> {
    let (name, agent, json, ctx, launch, remove) = match cmd {
        StreamCmd::Add {
            name,
            agent,
            command,
            adapter_argv,
            json,
            ctx,
        } => {
            let launch = match (command, adapter_argv.is_empty()) {
                (Some(command), true) => Some(agent_spec::StreamLaunch::Command(command)),
                (None, false) => Some(agent_spec::StreamLaunch::Argv(adapter_argv)),
                (None, true) => None,
                (Some(_), false) => anyhow::bail!("stream add got both --command and adapter argv"),
            };
            (name, agent, json, ctx, launch, false)
        }
        StreamCmd::Rm {
            name,
            agent,
            json,
            ctx,
        } => (name, agent, json, ctx, None, true),
    };
    let (root, host) = resolve_ctx(&ctx)?;
    let actor = ctx
        .as_id
        .clone()
        .or_else(|| std::env::var("ST_AGENT").ok())
        .filter(|value| !value.is_empty());
    let target = agent
        .or_else(|| actor.clone())
        .context("no stream target: pass --agent, --as, or set $ST_AGENT")?;
    if remove {
        let receipt =
            st2::agent_author::remove_stream(&root, &target, &host, actor.as_deref(), &name)?;
        if json {
            println!("{}", serde_json::to_string(&receipt)?);
        } else {
            println!(
                "{:?} stream {} on {}",
                receipt.result, receipt.name, receipt.identity
            );
        }
    } else {
        let receipt =
            st2::agent_author::add_stream(&root, &target, &host, actor.as_deref(), &name, launch)?;
        if json {
            println!("{}", serde_json::to_string(&receipt)?);
        } else {
            println!(
                "{:?} stream {} on {}",
                receipt.result, receipt.name, receipt.identity
            );
        }
    }
    Ok(())
}

fn request_cmd(cmd: RequestCmd) -> Result<()> {
    match cmd {
        RequestCmd::Send {
            to,
            idempotency_key,
            tags,
            body,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let principal = acting_id(&ctx)?;
            let body = parse_json_body(body)?;
            let receipt = st2::request::publish(
                &root,
                &host,
                &principal,
                &to,
                &idempotency_key,
                parse_typed_tags(tags)?,
                body,
            )?;
            print_publish_receipt(&receipt, json)
        }
        RequestCmd::Reply {
            request_filename,
            tags,
            body,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let agent = acting_id(&ctx)?;
            let body = parse_json_body(body)?;
            let receipt = st2::request::reply(
                &root,
                &host,
                &agent,
                &request_filename,
                parse_typed_tags(tags)?,
                body,
            )?;
            print_publish_receipt(&receipt, json)
        }
        RequestCmd::Read {
            request_filename,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let agent = acting_id(&ctx)?;
            let request = st2::request::read(&root, &host, &agent, &request_filename)?;
            if json {
                println!("{}", serde_json::to_string(&request)?);
            } else {
                println!(
                    "request {} from {} ({})",
                    request.idempotency_key, request.from, request.request_filename
                );
            }
            Ok(())
        }
        RequestCmd::Status {
            idempotency_key,
            json,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let principal = acting_id(&ctx)?;
            let status = st2::request::status(&root, &host, &principal, &idempotency_key)?;
            if json {
                println!("{}", serde_json::to_string(&status)?);
            } else {
                match status {
                    st2::request::RequestStatus::Pending {
                        idempotency_key,
                        request_filename,
                    } => println!("pending {idempotency_key} ({request_filename})"),
                    st2::request::RequestStatus::Replied {
                        idempotency_key,
                        request_filename,
                        from,
                        ..
                    } => println!("replied {idempotency_key} ({request_filename}) from {from}"),
                }
            }
            Ok(())
        }
    }
}

fn parse_json_body(body: Option<String>) -> Result<serde_json::Value> {
    let body = body_or_stdin(body)?;
    serde_json::from_str(&body).context("request body must be valid JSON")
}

fn parse_typed_tags(tags: Vec<String>) -> Result<std::collections::BTreeMap<String, String>> {
    let mut parsed = std::collections::BTreeMap::new();
    for tag in tags {
        let (key, value) = tag
            .split_once('=')
            .with_context(|| format!("typed tag must be `key=value`, got `{tag}`"))?;
        if key.is_empty() || value.is_empty() {
            anyhow::bail!("typed tag must have a non-empty key and value: `{tag}`");
        }
        if parsed.insert(key.to_string(), value.to_string()).is_some() {
            anyhow::bail!("duplicate typed tag key `{key}`");
        }
    }
    Ok(parsed)
}

fn print_publish_receipt(receipt: &st2::request::PublishReceipt, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(receipt)?);
    } else {
        println!("{}", receipt.filename);
    }
    Ok(())
}

/// `st2 message ls --json` row (stable st2 wire contract).
#[derive(serde::Serialize)]
struct LsItemJson<'a> {
    filename: &'a str,
    ts: u64,
    from: Option<&'a str>,
    subject: Option<&'a str>,
    #[serde(rename = "inReplyTo")]
    in_reply_to: Option<&'a str>,
    tags: &'a [String],
    priority: Option<&'a str>,
    #[serde(rename = "idempotencyKey", skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<&'a str>,
    #[serde(rename = "eventId", skip_serializing_if = "Option::is_none")]
    event_id: Option<&'a str>,
    #[serde(rename = "eventKey", skip_serializing_if = "Option::is_none")]
    event_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<&'a str>,
}

impl<'a> From<&'a st2::message::Message> for LsItemJson<'a> {
    fn from(m: &'a st2::message::Message) -> Self {
        LsItemJson {
            filename: &m.filename,
            ts: m.ts_ms,
            from: m.from.as_deref(),
            subject: m.subject.as_deref(),
            in_reply_to: m.in_reply_to.as_deref(),
            tags: &m.tags,
            priority: m.priority.as_deref(),
            idempotency_key: m.idempotency_key.as_deref(),
            stream: m.stream.as_deref(),
            event_id: m.event_id.as_deref(),
            event_key: m.event_key.as_deref(),
            body: None,
        }
    }
}

impl<'a> LsItemJson<'a> {
    fn from_message(m: &'a st2::message::Message, include_body: bool) -> Self {
        let mut item = Self::from(m);
        if include_body {
            item.body = Some(&m.body);
        }
        item
    }
}

/// `st2 message read --json` — the full message.
#[derive(serde::Serialize)]
struct MessageJson<'a> {
    filename: &'a str,
    ts: u64,
    from: Option<&'a str>,
    subject: Option<&'a str>,
    #[serde(rename = "inReplyTo")]
    in_reply_to: Option<&'a str>,
    tags: &'a [String],
    priority: Option<&'a str>,
    #[serde(rename = "idempotencyKey", skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<&'a str>,
    #[serde(rename = "eventId", skip_serializing_if = "Option::is_none")]
    event_id: Option<&'a str>,
    #[serde(rename = "eventKey", skip_serializing_if = "Option::is_none")]
    event_key: Option<&'a str>,
    body: &'a str,
}

impl<'a> From<&'a st2::message::Message> for MessageJson<'a> {
    fn from(m: &'a st2::message::Message) -> Self {
        MessageJson {
            filename: &m.filename,
            ts: m.ts_ms,
            from: m.from.as_deref(),
            subject: m.subject.as_deref(),
            in_reply_to: m.in_reply_to.as_deref(),
            tags: &m.tags,
            priority: m.priority.as_deref(),
            idempotency_key: m.idempotency_key.as_deref(),
            stream: m.stream.as_deref(),
            event_id: m.event_id.as_deref(),
            event_key: m.event_key.as_deref(),
            body: &m.body,
        }
    }
}

fn context_cmd(cmd: ContextCmd) -> Result<()> {
    use st2::context::{self, View};
    match cmd {
        ContextCmd::Read {
            identity,
            decisions,
            full,
            fresh_within,
            ctx,
        } => {
            let dir = resolve_context_dir(identity, &ctx)?;
            let view = if full {
                View::Full
            } else if decisions {
                View::Decisions
            } else {
                View::Now
            };
            if fresh_within.is_some() && view != View::Now {
                anyhow::bail!("--fresh-within cannot be combined with --decisions or --full");
            }
            let content = match fresh_within {
                Some(seconds) => context::read_now_fresh(&dir, Duration::from_secs(seconds)),
                None => context::read(&dir, view),
            };
            print!("{content}");
            Ok(())
        }
        ContextCmd::Write { identity, ctx } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = match identity {
                Some(identity) => identity,
                None => acting_id(&ctx)?,
            };
            let content =
                std::io::read_to_string(std::io::stdin()).context("reading context from stdin")?;
            message::with_resolved_state_dir(
                &root,
                &id,
                &host,
                &["resources", "context"],
                true,
                |dir| context::write_now(dir, &content),
            )?;
            eprintln!("context: wrote now.md ({} bytes)", content.len());
            Ok(())
        }
        ContextCmd::Append {
            identity,
            decision,
            why,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = match identity {
                Some(identity) => identity,
                None => acting_id(&ctx)?,
            };
            let filename = message::with_resolved_state_dir(
                &root,
                &id,
                &host,
                &["resources", "context", "decisions"],
                true,
                |dir| context::append_decision_to_dir(dir, &decision, &why),
            )?;
            println!("{filename}");
            Ok(())
        }
    }
}

fn service_cmd(cmd: ServiceCmd) -> Result<()> {
    match cmd {
        ServiceCmd::Install {
            catalog,
            host,
            pty_root,
            memory_max_mb,
        } => {
            let catalog = match catalog {
                Some(c) => c,
                None => catalog_root_for_env()?,
            };
            st2::service::install(&catalog, host, pty_root, memory_max_mb)
        }
        ServiceCmd::Status => st2::service::status(),
        ServiceCmd::Uninstall => st2::service::uninstall(),
    }
}

fn resource_cmd(cmd: ResourceCmd) -> Result<()> {
    match cmd {
        ResourceCmd::Add {
            url,
            title,
            tags,
            relation,
            body_stdin,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = acting_id(&ctx)?;
            let body = if body_stdin {
                std::io::read_to_string(std::io::stdin())?
            } else {
                String::new()
            };
            let f = message::with_resolved_state_dir(
                &root,
                &id,
                &host,
                &["resources", "links"],
                true,
                |dir| {
                    st2::resource::add(
                        dir,
                        &url,
                        title.as_deref(),
                        &tags,
                        relation.as_deref(),
                        &body,
                    )
                },
            )?;
            println!("{f}");
            Ok(())
        }
        ResourceCmd::Ls { identity, ctx } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let id = match identity {
                Some(i) => i,
                None => acting_id(&ctx)?,
            };
            let dir = st2::resource::links_dir(&agent_dir_of(&root, &id, &host)?);
            let items = st2::resource::list(&dir);
            println!("# {} resource{} for {id}", items.len(), plural(items.len()));
            for r in &items {
                let title = r.title.as_deref().unwrap_or("");
                println!("{}  {}  {title}", r.filename, r.url);
            }
            Ok(())
        }
        ResourceCmd::Read { first, second, ctx } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let (id, filename) = box_target(first, second, &ctx)?;
            let dir = st2::resource::links_dir(&agent_dir_of(&root, &id, &host)?);
            let r = st2::resource::read(&dir, &filename)?;
            println!("url:      {}", r.url);
            if let Some(t) = &r.title {
                println!("title:    {t}");
            }
            if !r.tags.is_empty() {
                println!("tags:     {}", r.tags.join(", "));
            }
            if let Some(rel) = &r.relation {
                println!("relation: {rel}");
            }
            if !r.body.is_empty() {
                println!();
                print!("{}", r.body);
            }
            Ok(())
        }
        ResourceCmd::Remove { first, second, ctx } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let (id, filename) = box_target(first, second, &ctx)?;
            anyhow::ensure!(
                message::is_message_filename(&filename),
                "invalid resource filename {filename:?}"
            );
            message::with_resolved_state_dir(
                &root,
                &id,
                &host,
                &["resources", "links"],
                false,
                |dir| st2::resource::remove(dir, &filename),
            )?;
            println!("removed");
            Ok(())
        }
    }
}

/// Resolve an agent's context dir (`<agent_dir>/resources/context`). Identity defaults to `$ST_AGENT`.
fn resolve_context_dir(identity: Option<String>, ctx: &MsgCtx) -> Result<PathBuf> {
    let (root, host) = resolve_ctx(ctx)?;
    let id = match identity {
        Some(i) => i,
        None => acting_id(ctx)?,
    };
    Ok(st2::context::context_dir(&agent_dir_of(&root, &id, &host)?))
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// `st2 up <single-file-spec>` — supervise the spec's top-level team as a fleet: keep-alive + respawn,
/// nomad-decoupled, exactly like `st2 up --catalog <catalog>`. `--once` does a
/// single boot pass; otherwise it supervises on a timer under a host lock until Ctrl-C.
fn up_spec_fleet(spec_file: &Path, host: Option<String>, once: bool, interval: u64) -> Result<()> {
    let (spec, root) = st2::eval_run::load_spec(spec_file)?;
    // File-level taxonomy (the maintainer): a file with NO agents is an eval-only ("job") file — it has nothing
    // to supervise; it is run to a verdict via `st2 eval`, not `st2 up`. Refuse it here with a pointer.
    if spec.agents.is_empty() {
        anyhow::bail!(
            "{} declares no agents — it is an eval-only file (run it with `st2 eval {}`), not a fleet to \
             supervise with `st2 up`",
            spec_file.display(),
            spec_file.display()
        );
    }
    // Run host: --host (explicit) › the spec's top-level `host` › the OS hostname. The declared host
    // wins over the OS hostname so a per-host file ups its own slice even when they differ.
    let this_host = host
        .or_else(|| spec.host.clone())
        .unwrap_or_else(detect_host);
    // Seats boot as fresh top-level agents. Authored bare `st2` commands use the PATH prepend while
    // generated DING sidecars bind directly to the executable captured by this context.
    let task_context = st2::reconcile::TaskCompileContext::current(root.clone())?;
    st2::eval_run::prepare_spawn_env(task_context.st2_executable());
    let mut specs = st2::eval_run::spec_to_agent_specs(&spec.agents, &this_host, &root);
    st2::reconcile::compile_generated_tasks(&mut specs, &this_host, &task_context)?;
    let runner = SystemRunner::new(root.clone(), exec_state_dir(&this_host));

    // One supervisor per (spec dir, host) — the same host-lock discipline as the catalog path.
    let lock = HostLock::new(&root, &this_host);
    if let Some(owner) = lock.live_owner() {
        eprintln!("st2: {}", lock.busy_warning(owner));
        std::process::exit(1);
    }

    if once {
        let report = st2::up_once_specs(&specs, &this_host, &runner);
        println!(
            "booted team from spec {} on host '{this_host}' (once):",
            spec_file.display()
        );
        print_report(&report);
        if report.skipped {
            anyhow::bail!("one-shot reconcile pass was skipped");
        }
        return Ok(());
    }

    if lock.has_stale_lock() {
        eprintln!("st2: reclaiming a stale lock left by a crashed st2.");
    }
    lock.acquire().context("acquiring host lock")?;
    eprintln!(
        "st2: supervising spec {} on host '{this_host}' ({} agents; reconcile every {interval}s; Ctrl-C to stop)",
        spec_file.display(),
        specs.len()
    );
    let result = st2::up_loop_specs(
        &specs,
        &root,
        &this_host,
        &runner,
        Duration::from_secs(interval),
        |report| {
            if report.is_noteworthy() {
                print_report(report);
            }
        },
    );
    lock.release();
    result
}

fn up(
    root: &Path,
    host: Option<String>,
    once: bool,
    materialize_only: bool,
    interval: u64,
    agent: Option<String>,
    task: Option<String>,
) -> Result<()> {
    // An st2-SPEC path (a `*.kdl` file, or a folder with one top-level spec `*.kdl`) supervises its
    // top-level team directly — no catalog discovery. Otherwise, the classic catalog reconcile loop.
    if let Some(spec_file) = st2::eval_run::resolve_spec_path(root) {
        if task.is_some() {
            anyhow::bail!("--task is for folder catalogs, not single-file specs");
        }
        if materialize_only {
            anyhow::bail!(
                "--materialize-only is for folder catalogs with agent render{{}} blocks, not single-file specs"
            );
        }
        return up_spec_fleet(&spec_file, host, once, interval);
    }
    let this_host = host.unwrap_or_else(detect_host);
    // Canonicalize the catalog root so `$CATALOG` expands to an absolute path (T01/R11).
    let catalog_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    if materialize_only {
        let _catalog_lock = st2::CatalogLock::shared(&catalog_root)
            .context("acquire shared catalog-authoring lock for materialization")?;
        let mut found = discover(&catalog_root);
        let ownership_specs = found.specs.clone();
        if let Some(selector) = task.as_deref() {
            let (owner, _, _) = st2::reconcile::resolve_task(&found.specs, selector, &this_host)?;
            let owner_identity = owner.identity.clone();
            let owner_path = owner.path.clone();
            found
                .specs
                .retain(|spec| spec.identity == owner_identity && spec.path == owner_path);
        }
        if let Some(identity) = agent.as_deref() {
            found
                .specs
                .retain(|spec| spec.identity == identity || spec.bus_id(&this_host) == identity);
        }
        for warning in &found.warnings {
            eprintln!("warning: {warning}");
        }
        for error in &found.errors {
            eprintln!("error: {}: {}", error.path.display(), error.message);
        }
        if st2::hooks::required_by_codex(&found.specs, &this_host, &catalog_root) {
            st2::hooks::verify_required_set().context(
                "verifying explicitly installed lifecycle hooks before Codex materialization",
            )?;
        }
        let report = st2::materialize::materialize_catalog_against(
            &catalog_root,
            &found.specs,
            &ownership_specs,
            &this_host,
        );
        for item in &report.materialized {
            println!("{item}");
        }
        for warning in &report.warnings {
            eprintln!("warning: {warning}");
        }
        for error in &report.errors {
            eprintln!("error: {error}");
        }
        println!(
            "materialized {} operation{} for host '{this_host}'; {} error{}",
            report.materialized.len(),
            plural(report.materialized.len()),
            found.errors.len() + report.errors.len(),
            plural(found.errors.len() + report.errors.len())
        );
        if !found.errors.is_empty() || !report.is_clean() {
            anyhow::bail!("materialization failed");
        }
        return Ok(());
    }

    let runner = SystemRunner::new(catalog_root.clone(), exec_state_dir(&this_host));

    // One supervisor per (folder, host). A single `--once` pass must also refuse while a loop owns
    // the lock (it would double-spawn) — but it does NOT take the lock itself (that would clobber the
    // loop's pid file); only the loop acquires + holds it.
    let lock = HostLock::new(root, &this_host);
    if let Some(owner) = lock.live_owner() {
        eprintln!("st2: {}", lock.busy_warning(owner));
        std::process::exit(1);
    }

    if once {
        let targeted = task.is_some();
        let report = match task.as_deref() {
            Some(selector) => {
                st2::run::up_once_selected(&catalog_root, selector, &this_host, &runner)?
            }
            None => up_once(&catalog_root, &this_host, &runner)?,
        };
        println!("reconcile pass on host '{this_host}':");
        print_report(&report);
        if report.skipped {
            anyhow::bail!("one-shot reconcile pass was skipped");
        }
        if targeted && !report.errors.is_empty() {
            anyhow::bail!("targeted one-shot reconcile pass reported errors");
        }
        return Ok(());
    }

    if lock.has_stale_lock() {
        eprintln!("st2: reclaiming a stale lock left by a crashed st2.");
    }
    lock.acquire().context("acquiring host lock")?;

    eprintln!(
        "st2: supervising {} on host '{this_host}' (reconcile every {interval}s + on change; Ctrl-C to stop)",
        root.display()
    );
    let result = up_loop(
        &catalog_root,
        &this_host,
        &runner,
        Duration::from_secs(interval),
        |report| {
            if report.is_noteworthy() {
                print_report(report);
            }
        },
    );
    lock.release();
    result
}

fn print_report(report: &UpReport) {
    report_line("launched", &report.launched);
    report_line("restarted", &report.restarted);
    report_line("torn down", &report.torn_down);
    report_line("gc", &report.gc);
    report_line("held", &report.held);
    report_line("flapping", &report.flapping);
    report_line("unparked", &report.unparked);
    report_line("adopted", &report.adopted);
    report_line("other-host", &report.other_host);
    report_line("unrunnable", &report.unrunnable);
    for w in &report.warnings {
        eprintln!("warning: {w}");
    }
    for e in &report.errors {
        eprintln!("error: {e}");
    }
}

fn report_line(label: &str, items: &[String]) {
    if !items.is_empty() {
        println!("  {label} ({}): {}", items.len(), items.join(", "));
    }
}

fn ls(root: &Path) -> Result<()> {
    // A single-file st2 spec: PREVIEW the declared team WITHOUT booting (a safe validate/inspect path
    // before `st2 up`). A catalog dir has no top-level spec .kdl, so it falls through to discovery.
    if let Some(spec_file) = st2::eval_run::resolve_spec_path(root) {
        let (spec, _dir) = st2::eval_run::load_spec(&spec_file)?;
        // File-level taxonomy: no agents = an eval-only file → `st2 eval`; agents = a fleet → `st2 up`.
        let classification = if spec.agents.is_empty() {
            "eval-only → st2 eval"
        } else {
            "agents → st2 up"
        };
        println!(
            "spec {}  [{classification}]  (host {}; {} agent{}{})",
            spec_file.display(),
            spec.host.as_deref().unwrap_or("<none>"),
            spec.agents.len(),
            if spec.agents.len() == 1 { "" } else { "s" },
            if spec.eval.is_some() {
                ", + eval block"
            } else {
                ""
            },
        );
        for a in &spec.agents {
            println!(
                "  {}  ws={}",
                a.id,
                a.workspace.as_deref().unwrap_or("<none>")
            );
            println!("      command: {}", a.command);
            for ex in &a.execs {
                println!("      + exec {}: {}", ex.id, ex.command);
            }
        }
        return Ok(());
    }
    let _catalog_lock = st2::CatalogLock::shared(root)
        .context("acquire shared catalog-authoring lock for catalog listing")?;
    let found = discover(root);
    let mut specs = found.specs.clone();
    let task_context = st2::reconcile::TaskCompileContext::current(root.to_path_buf())?;
    st2::reconcile::compile_generated_tasks(&mut specs, &detect_host(), &task_context)
        .context("compile generated tasks for catalog listing")?;

    if specs.is_empty() {
        println!("no specs found under {}", root.display());
    }
    for spec in &specs {
        let host = spec.host.as_deref().unwrap_or("<this-host>");
        let kind = match spec.job_type {
            st2::JobType::Service => "service",
        };
        let runnable = if spec.is_runnable() {
            ""
        } else {
            "  [UNRENDERED: no task launch]"
        };
        let lifecycle = if spec.desired_state.is_running() {
            String::new()
        } else {
            format!(
                "  [{}{}]",
                spec.desired_state.as_str(),
                spec.desired_state
                    .reason()
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            )
        };
        println!(
            "{host}.{ident}  [{kind}] ({n} task{plural}){runnable}{lifecycle}\n    {path}",
            ident = spec.identity,
            n = spec.tasks.len(),
            plural = if spec.tasks.len() == 1 { "" } else { "s" },
            path = spec.path.display(),
        );
        for task in &spec.tasks {
            let tk = match task.kind {
                st2::TaskKind::Pty => "pty",
                st2::TaskKind::Exec => "exec",
            };
            let launch = match (&task.command, &task.argv) {
                (Some(command), _) => format!("command {command}"),
                (_, Some(argv)) => format!("argv {argv:?}"),
                _ => "<none>".to_string(),
            };
            println!("      - {tk} {name}: {launch}", name = task.name);
        }
    }

    for w in &found.warnings {
        eprintln!("warning: {w}");
    }
    for e in &found.errors {
        eprintln!("error: {}: {}", e.path.display(), e.message);
    }

    Ok(())
}
