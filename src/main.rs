//! st2 CLI. M0 exposes a single read-only command — `st2 ls <root>` — that slurps a catalog+inbox
//! folder and prints what it discovered (specs, warnings, errors). Reconcile/run land in later
//! milestones; this is the smoke test that discovery works end to end against a real folder.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, Parser, Subcommand};

#[cfg(all(debug_assertions, target_os = "linux"))]
fn maybe_pause_at_cutover_test_boundary(boundary: &str) -> Result<()> {
    use std::fs::{File, OpenOptions};
    use std::io::Write as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    const BOUNDARY_ENV: &str = "ST2_TEST_CUTOVER_BOUNDARY";
    const SENTINEL_ENV: &str = "ST2_TEST_CUTOVER_SENTINEL";
    let (Some(requested_boundary), Some(sentinel)) = (
        std::env::var_os(BOUNDARY_ENV),
        std::env::var_os(SENTINEL_ENV),
    ) else {
        if std::env::var_os(BOUNDARY_ENV).is_some() || std::env::var_os(SENTINEL_ENV).is_some() {
            anyhow::bail!(
                "{BOUNDARY_ENV} and {SENTINEL_ENV} must be supplied together for a cutover test boundary"
            );
        }
        return Ok(());
    };
    if requested_boundary != boundary {
        return Ok(());
    }
    let sentinel = PathBuf::from(sentinel);
    if !sentinel.is_absolute() {
        anyhow::bail!("{SENTINEL_ENV} must be absolute");
    }
    let link_metadata = match std::fs::symlink_metadata(&sentinel) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect cutover test sentinel {}", sentinel.display()));
        }
    };
    let canonical = sentinel
        .canonicalize()
        .with_context(|| format!("canonicalize cutover test sentinel {}", sentinel.display()))?;
    if canonical != sentinel
        || !link_metadata.is_file()
        || link_metadata.file_type().is_symlink()
        || link_metadata.nlink() != 1
        || link_metadata.uid() != unsafe { libc::geteuid() }
        || link_metadata.permissions().mode() & 0o022 != 0
    {
        anyhow::bail!(
            "cutover test sentinel must be a canonical, singly linked, current-user regular file not writable by group or world"
        );
    }
    let parent = sentinel
        .parent()
        .context("cutover test sentinel has no parent")?;
    let parent_metadata = std::fs::symlink_metadata(parent)
        .with_context(|| format!("inspect cutover test directory {}", parent.display()))?;
    if parent.parent() != Some(Path::new("/tmp"))
        || !parent_metadata.is_dir()
        || parent_metadata.file_type().is_symlink()
        || parent_metadata.uid() != unsafe { libc::geteuid() }
        || parent_metadata.permissions().mode() & 0o077 != 0
    {
        anyhow::bail!(
            "cutover test sentinel must be inside a private current-user temporary directory directly under /tmp"
        );
    }
    let phase = sentinel.with_extension("phase");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&phase)
        .with_context(|| format!("create cutover test phase {}", phase.display()))?;
    writeln!(output, "{} {boundary}", std::process::id())?;
    output.sync_all()?;
    File::open(parent)?.sync_all()?;
    // SAFETY: this debug-only, explicitly armed boundary deliberately suspends the candidate so
    // the live user-systemd test can SIGKILL this exact process.
    if unsafe { libc::raise(libc::SIGSTOP) } != 0 {
        anyhow::bail!("raise SIGSTOP at cutover test boundary");
    }
    Ok(())
}

#[cfg(not(all(debug_assertions, target_os = "linux")))]
fn maybe_pause_at_cutover_test_boundary(_boundary: &str) -> Result<()> {
    Ok(())
}

use st2::{
    Runner, SystemRunner, UpReport, detect_host, ding, discover, exec_state_dir, message, up_loop,
    up_once,
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
    /// Exact, crash-recoverable lifecycle transactions for terminal-free exec tasks.
    #[command(subcommand)]
    Exec(ExecCmd),
    /// Read-only admission state for catalog cutover coordination.
    #[command(subcommand)]
    Cutover(CutoverCmd),
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
    /// Health check for a catalog: tools available, active agents alive with fresh presence, and
    /// retired agents fully absent. Exits non-zero on problems.
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
        /// Machine-readable JSON array, including retirement and declared Resource bindings.
        #[arg(long)]
        json: bool,
        /// With `--json`, add `lastActivity` + `inbox` count per agent.
        #[arg(long)]
        enrich: bool,
        #[command(flatten)]
        ctx: MsgCtx,
    },
    /// Emit one complete-or-indeterminate desired-task/runtime snapshot for an adoption-only
    /// supervisor cutover. This is a read-only typed boundary; it never reconciles.
    Tasks {
        /// Host whose desired tasks and runtime generations to inspect. Defaults to this host.
        #[arg(long)]
        host: Option<String>,
        /// Observe only tasks with this desired state. Omit for both running and absent.
        #[arg(long, value_enum)]
        desired_state: Option<st2::task_inventory::DesiredStateSelection>,
        /// Emit the versioned machine-readable envelope. Required in v1.
        #[arg(long)]
        json: bool,
    },
    /// Print a shell completion script for `st2` to stdout (`st2 completions <bash|zsh|fish|…>`).
    /// Generated from the live command tree, so it never drifts from the actual flags.
    Completions {
        /// The shell to generate completions for.
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
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
enum ExecCmd {
    /// Prepare or apply an immutable exact-retirement capability.
    #[command(subcommand)]
    Retirement(ExecRetirementCmd),
}

#[derive(Subcommand)]
enum ExecRetirementCmd {
    /// Read runtime authority and write one create-only immutable retirement plan.
    Prepare {
        /// Exact host-local exec namespace. Never inferred from a runtime id.
        #[arg(long)]
        host: String,
        /// Prepare one exact runtime id.
        #[arg(long)]
        id: String,
        /// Caller-held canonical declaration-root digest.
        #[arg(long, value_name = "HEX")]
        expect_catalog_sha256: String,
        /// Create-only immutable plan path outside the live catalog and exec state.
        #[arg(long, value_name = "FILE")]
        output: PathBuf,
        /// Emit `st2.exec-retirement-preparation.v1`.
        #[arg(long)]
        json: bool,
    },
    /// Apply or resume one immutable retirement plan.
    Apply {
        /// Create-only plan emitted by `exec retirement prepare`.
        #[arg(long, value_name = "FILE")]
        plan: PathBuf,
        /// Caller-held SHA-256 of the exact plan bytes.
        #[arg(long, value_name = "HEX")]
        expect_plan_sha256: String,
        /// Emit the typed completed receipt.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum CatalogCmd {
    /// Capture the coherent declaration plane into a create-only canonical directory.
    Snapshot {
        /// Destination directory. It must be outside the live catalog.
        #[arg(long, value_name = "DIR")]
        output: PathBuf,
        /// Emit the typed snapshot receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Derive one atomic service/adopt-only/provider-witness bundle from a retained snapshot.
    Project {
        /// Retained canonical snapshot produced by `catalog snapshot`.
        #[arg(long, value_name = "DIR")]
        snapshot: PathBuf,
        /// Exact declaration-root SHA-256 returned by `catalog snapshot`.
        #[arg(long, value_name = "HEX")]
        expect_sha256: String,
        /// Create-only atomic output bundle.
        #[arg(long, value_name = "DIR")]
        output: PathBuf,
        /// Emit the typed projection result and receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Apply a complete canonical declaration directory under declaration-root CAS.
    Apply {
        /// Complete prepared declaration directory. Runtime state and control paths are rejected.
        #[arg(long, value_name = "DIR")]
        prepared: Option<PathBuf>,
        /// Atomic projection bundle whose typed receipt must name the apply target.
        #[arg(long, value_name = "DIR")]
        projection_bundle: Option<PathBuf>,
        /// Apply-capable child selected from the verified projection bundle.
        #[arg(long, value_enum)]
        projection_child: Option<st2::catalog_transaction::CatalogProjectionChild>,
        /// Caller-held SHA-256 returned by `catalog project`; never read from the bundle itself.
        #[arg(long, value_name = "HEX")]
        expect_bundle_sha256: Option<String>,
        /// Expected canonical declaration-root SHA-256 of the live catalog.
        #[arg(long, value_name = "HEX")]
        expect_sha256: Option<String>,
        /// Resume the durable incomplete marker and internal stage without the original source.
        #[arg(long)]
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
enum CutoverCmd {
    /// Install and start the exact restart-always candidate service for one cutover request.
    Install {
        /// Canonical bounded st2.cutover-request.v2 file.
        #[arg(long, value_name = "FILE")]
        request: PathBuf,
        /// Caller-held SHA-256 of the exact request bytes.
        #[arg(long, value_name = "HEX")]
        expect_request_sha256: String,
    },
    /// Run or resume one exact durable cutover request, then retain host ownership as the successor
    /// supervisor after successful finalization.
    Run {
        /// Canonical bounded st2.cutover-request.v2 file.
        #[arg(long, value_name = "FILE")]
        request: PathBuf,
        /// Caller-held SHA-256 of the exact request bytes.
        #[arg(long, value_name = "HEX")]
        expect_request_sha256: String,
    },
    /// Report whether ordinary runtime mutation is currently admitted.
    ///
    /// Exits non-zero while a durable cutover gate is active or malformed, making this suitable
    /// for systemd ExecStartPre and other cooperative writers.
    Status {
        /// Host requesting runtime-mutation admission. Defaults to the local hostname.
        #[arg(long)]
        host: Option<String>,
        /// Emit the stable machine-readable admission record.
        #[arg(long)]
        json: bool,
    },
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
        Command::Status { identity, set, ctx } => status_cmd(identity, set, ctx),
        Command::Rename(args) => presentation_cmd(st2::agent_author::PresentationField::Name, args),
        Command::Describe(args) => {
            presentation_cmd(st2::agent_author::PresentationField::Description, args)
        }
        Command::Cutover(cmd) => cutover_cmd(cmd, catalog_path.as_deref()),
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
        Command::Exec(ExecCmd::Retirement(ExecRetirementCmd::Prepare {
            host,
            id,
            expect_catalog_sha256,
            output,
            json,
        })) => {
            let catalog = require_exec_retirement_catalog(
                catalog_path.clone(),
                json,
                "`exec retirement prepare` requires explicit --catalog <path>",
            )?;
            let result = exec_retirement_result(
                st2::exec_retirement::prepare(st2::exec_retirement::RetirementPrepareRequest {
                    catalog,
                    host,
                    selector: st2::exec_retirement::RetirementSelector::Id(id),
                    expect_catalog_sha256,
                    output,
                }),
                json,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "prepared {} {}",
                    result.plan_sha256,
                    result.output.display()
                );
            }
            Ok(())
        }
        Command::Exec(ExecCmd::Retirement(ExecRetirementCmd::Apply {
            plan,
            expect_plan_sha256,
            json,
        })) => {
            let catalog = require_exec_retirement_catalog(
                catalog_path.clone(),
                json,
                "`exec retirement apply` requires explicit --catalog <path>",
            )?;
            let result = exec_retirement_result(
                st2::exec_retirement::apply(st2::exec_retirement::RetirementApplyRequest {
                    catalog,
                    plan,
                    expect_plan_sha256,
                }),
                json,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("completed {}", result.request_sha256);
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Snapshot { output, json }) => {
            let result =
                st2::catalog_transaction::snapshot(st2::catalog_transaction::SnapshotRequest {
                    catalog: catalog_arg(None)?,
                    output,
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
        Command::Catalog(CatalogCmd::Project {
            snapshot,
            expect_sha256,
            output,
            json,
        }) => {
            let result = st2::catalog_transaction::project_catalog(
                st2::catalog_transaction::CatalogProjectionRequest {
                    catalog: catalog_arg(None)?,
                    snapshot,
                    expect_sha256,
                    output,
                },
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "created {} {}",
                    result.bundle_sha256,
                    result.output.display()
                );
            }
            Ok(())
        }
        Command::Catalog(CatalogCmd::Apply {
            prepared,
            projection_bundle,
            projection_child,
            expect_bundle_sha256,
            expect_sha256,
            resume,
            json,
        }) => {
            let catalog = catalog_arg(None)?;
            let result = match (
                prepared,
                projection_bundle,
                projection_child,
                expect_bundle_sha256,
                expect_sha256,
                resume,
            ) {
                (Some(prepared), None, None, None, Some(expect_sha256), false) => {
                    st2::catalog_transaction::apply(st2::catalog_transaction::ApplyRequest {
                        catalog,
                        mode: st2::catalog_transaction::ApplyMode::Prepared {
                            prepared,
                            expect_sha256,
                        },
                    })?
                }
                (
                    None,
                    Some(bundle),
                    Some(child),
                    Some(expect_bundle_sha256),
                    Some(expect_sha256),
                    false,
                ) => st2::catalog_transaction::apply_projection_bundle(
                    st2::catalog_transaction::CatalogProjectionApplyRequest {
                        catalog,
                        bundle,
                        child,
                        expect_bundle_sha256,
                        expect_sha256,
                    },
                )?,
                (None, None, None, None, None, true) => {
                    st2::catalog_transaction::apply(st2::catalog_transaction::ApplyRequest {
                        catalog,
                        mode: st2::catalog_transaction::ApplyMode::Resume,
                    })?
                }
                _ => anyhow::bail!(
                    "catalog apply requires exactly one complete mode: \
                     --prepared DIR --expect-sha256 HEX; \
                     --projection-bundle DIR --projection-child service|adopt-only \
                     --expect-bundle-sha256 HEX --expect-sha256 HEX; or --resume"
                ),
            };
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
            json,
            enrich,
            ctx,
        } => agents_cmd(catalog, status, json, enrich, ctx),
        Command::Tasks {
            host,
            desired_state,
            json,
        } => {
            if !json {
                anyhow::bail!("`st2 tasks` v1 requires --json");
            }
            let catalog = catalog_arg(None)?;
            tasks_cmd(&catalog, host, desired_state)
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

fn require_exec_retirement_catalog(
    catalog: Option<PathBuf>,
    json: bool,
    message: &str,
) -> Result<PathBuf> {
    match catalog {
        Some(catalog) => Ok(catalog),
        None if json => exec_retirement_error_exit("authority", message),
        None => anyhow::bail!("{message}"),
    }
}

fn exec_retirement_result<T>(
    result: st2::exec_retirement::RetirementResult<T>,
    json: bool,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if json => exec_retirement_error_exit(error.code.as_str(), &error.message),
        Err(error) => Err(error.into()),
    }
}

fn exec_retirement_error_exit<T>(code: &str, message: &str) -> Result<T> {
    let envelope = serde_json::json!({
        "schema": "st2.exec-retirement-error.v1",
        "code": code,
        "message": message,
    });
    eprintln!("{}", serde_json::to_string(&envelope)?);
    std::process::exit(1);
}

fn hooks_cmd(command: HooksCmd) -> Result<()> {
    match command {
        HooksCmd::Install {
            replace,
            allow_downgrade,
        } => {
            let catalog = catalog_root_for_env()?;
            // Hook publication may bootstrap before the default catalog exists. In that case no
            // durable gate can exist yet; an existing selected catalog must pass admission.
            if catalog.exists() {
                require_runtime_mutation_admission(&catalog, None)?;
            }
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
        let runner = SystemRunner::new(spec_root.clone(), exec_state_dir(&this_host));
        let report = st2::down_specs(&specs, &spec_root, &this_host, &runner)?;
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

fn mutation_admission(
    root: &Path,
    host: Option<String>,
) -> Result<(
    st2::cutover_admission::CanonicalCatalog,
    st2::cutover_admission::HostId,
    st2::cutover_admission::MutationAdmission,
)> {
    let catalog = st2::cutover_admission::CanonicalCatalog::open(root)?;
    let host = st2::cutover_admission::HostId::parse(host.unwrap_or_else(detect_host))?;
    let admission = st2::cutover_admission::probe_mutation_admission(&catalog, Some(&host))?;
    Ok((catalog, host, admission))
}

fn require_runtime_mutation_admission(root: &Path, host: Option<String>) -> Result<()> {
    let (_, _, admission) = mutation_admission(root, host)?;
    match admission {
        st2::cutover_admission::MutationAdmission::Available => Ok(()),
        st2::cutover_admission::MutationAdmission::Busy(busy) => {
            anyhow::bail!(
                "runtime mutation refused: {}",
                serde_json::to_string(&busy)?
            )
        }
    }
}

fn cutover_cmd(command: CutoverCmd, explicit_catalog: Option<&Path>) -> Result<()> {
    match command {
        CutoverCmd::Install {
            request,
            expect_request_sha256,
        } => {
            let loaded =
                st2::cutover_driver::LoadedCutoverRequest::load(&request, &expect_request_sha256)?;
            loaded.preflight()?;
            let request = request
                .canonicalize()
                .with_context(|| format!("canonicalize cutover request {}", request.display()))?;
            let spec = st2::service::CutoverCandidateServiceSpec::new(
                std::env::current_exe()?.canonicalize()?,
                loaded.request().canonical_catalog.clone(),
                request,
                expect_request_sha256,
                loaded.request().host.as_str().to_owned(),
                loaded.request().gate_id.as_str().to_owned(),
            )?;
            let unit = st2::service::install_cutover_candidate(&spec)?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "schema": "st2.cutover-candidate-service.v1",
                    "unit": spec.unit_name,
                    "unitPath": unit,
                    "restart": "always",
                    "restartSec": st2::service::CUTOVER_RESTART_SEC,
                }))?
            );
            Ok(())
        }
        CutoverCmd::Run {
            request,
            expect_request_sha256,
        } => {
            let loaded =
                st2::cutover_driver::LoadedCutoverRequest::load(&request, &expect_request_sha256)?;
            let catalog = loaded.request().canonical_catalog.clone();
            let host = loaded.request().host.as_str().to_owned();
            if let Some(explicit) = explicit_catalog {
                let explicit = absolute_catalog_path(explicit)?;
                if explicit != catalog {
                    anyhow::bail!(
                        "cutover request catalog {} conflicts with explicit --catalog {}",
                        catalog.display(),
                        explicit.display()
                    );
                }
            }
            let candidate = st2::service::CutoverCandidateServiceSpec::new(
                std::env::current_exe()?.canonicalize()?,
                catalog.clone(),
                request.canonicalize().with_context(|| {
                    format!("canonicalize cutover request {}", request.display())
                })?,
                expect_request_sha256,
                host.clone(),
                loaded.request().gate_id.as_str().to_owned(),
            )?;
            st2::service::validate_cutover_candidate_process(&candidate)?;
            maybe_pause_at_cutover_test_boundary("before-run")?;
            // SAFETY: CLI dispatch remains single-threaded here. The request is the sole cutover
            // authority; inherited ambient selection must not leak into a provider or helper.
            unsafe {
                std::env::set_var("CATALOG", &catalog);
                std::env::set_var("ST_ROOT", &catalog);
                std::env::set_var("PTY_ROOT", st2::catalog::pty_root(&catalog));
            }
            let runner = SystemRunner::new(catalog.clone(), exec_state_dir(&host));
            match loaded.run(&runner)? {
                st2::cutover_driver::DriverRunOutcome::Completed {
                    finalized,
                    provider_fleet_proof,
                } => {
                    maybe_pause_at_cutover_test_boundary("after-finalize")?;
                    let (finalized, ownership, readiness) = finalized.into_successor_parts();
                    println!(
                        "{}",
                        serde_json::to_string(&serde_json::json!({
                            "schema": "st2.cutover-run.v1",
                            "outcome": "completed",
                            "catalog": catalog,
                            "host": host,
                            "historyPath": finalized.history_path,
                            "gateId": finalized.marker.gate_id,
                            "requestSha256": finalized.marker.request_sha256,
                            "providerFleetProof": provider_fleet_proof,
                        }))?
                    );
                    eprintln!(
                        "st2: cutover finalized; supervising {} on host '{}' with retained ownership",
                        catalog.display(),
                        host
                    );
                    st2::run::up_loop_with_ownership_ready(
                        ownership,
                        &runner,
                        Duration::from_secs(30),
                        move || {
                            st2::service::retire_ordinary_supervisor_for_cutover()?;
                            match readiness {
                                Some(readiness) => readiness
                                    .supervisor_entered()
                                    .map_err(|error| anyhow::anyhow!(error)),
                                None => Ok(()),
                            }
                        },
                        |report| {
                            if report.is_noteworthy() {
                                print_report(report);
                            }
                        },
                    )
                }
                st2::cutover_driver::DriverRunOutcome::Finalized(finalized) => {
                    let (finalized, ownership, readiness) = finalized.into_successor_parts();
                    println!(
                        "{}",
                        serde_json::to_string(&serde_json::json!({
                            "schema": "st2.cutover-run.v1",
                            "outcome": "finalized",
                            "catalog": catalog,
                            "host": host,
                            "historyPath": finalized.history_path,
                            "gateId": finalized.marker.gate_id,
                            "requestSha256": finalized.marker.request_sha256,
                        }))?
                    );
                    eprintln!(
                        "st2: exact finalized cutover replay; supervising {} on host '{}' with reacquired ownership",
                        catalog.display(),
                        host
                    );
                    st2::run::up_loop_with_ownership_ready(
                        ownership,
                        &runner,
                        Duration::from_secs(30),
                        move || {
                            st2::service::retire_ordinary_supervisor_for_cutover()?;
                            match readiness {
                                Some(readiness) => readiness
                                    .supervisor_entered()
                                    .map_err(|error| anyhow::anyhow!(error)),
                                None => Ok(()),
                            }
                        },
                        |report| {
                            if report.is_noteworthy() {
                                print_report(report);
                            }
                        },
                    )
                }
                st2::cutover_driver::DriverRunOutcome::Fenced(fence) => {
                    let detail = match fence {
                        st2::cutover_driver::DriverFence::Active(busy) => serde_json::json!({
                            "kind": "active",
                            "busy": busy,
                        }),
                        st2::cutover_driver::DriverFence::Pending(pending) => serde_json::json!({
                            "kind": "pending",
                            "catalog": pending.catalog.as_path(),
                            "host": pending.host,
                            "gateId": pending.gate_id,
                            "requestSha256": pending.request_sha256,
                            "activePath": pending.active_path,
                        }),
                    };
                    println!(
                        "{}",
                        serde_json::to_string(&serde_json::json!({
                            "schema": "st2.cutover-run.v1",
                            "outcome": "fenced",
                            "detail": detail,
                        }))?
                    );
                    anyhow::bail!("cutover is fenced by another active authority")
                }
                st2::cutover_driver::DriverRunOutcome::NeedsCheckpoint {
                    action_index,
                    kind,
                    input_sha256,
                    receipt,
                } => {
                    println!(
                        "{}",
                        serde_json::to_string(&serde_json::json!({
                            "schema": "st2.cutover-run.v1",
                            "outcome": "needs-checkpoint",
                            "actionIndex": action_index,
                            "kind": kind,
                            "inputSha256": input_sha256,
                            "receipt": receipt,
                        }))?
                    );
                    anyhow::bail!(
                        "cutover requires external checkpoint evidence at action {action_index}"
                    )
                }
            }
        }
        CutoverCmd::Status { host, json } => {
            let root = catalog_root_for_env()?;
            let (catalog, host, admission) = mutation_admission(&root, host)?;
            match admission {
                st2::cutover_admission::MutationAdmission::Available => {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string(&serde_json::json!({
                                "schema": "st2.mutation-available.v1",
                                "catalog": catalog.as_path(),
                                "requestedHost": host,
                            }))?
                        );
                    } else {
                        println!(
                            "available\tcatalog={}\thost={}",
                            catalog.as_path().display(),
                            host.as_str()
                        );
                    }
                    Ok(())
                }
                st2::cutover_admission::MutationAdmission::Busy(busy) => {
                    if json {
                        println!("{}", serde_json::to_string(&busy)?);
                    } else {
                        println!(
                            "busy\treason={:?}\tcatalog={}\thost={}",
                            busy.reason,
                            busy.catalog.display(),
                            host.as_str()
                        );
                        println!("active-marker\t{}", busy.active_marker.display());
                    }
                    std::process::exit(1);
                }
            }
        }
    }
}

/// `st2 pty [<pty-args>…]` — a thin pass-through to `pty` with the catalog's bus env pre-set, so
/// the maintainer never has to `eval "$(st2 env …)"` first. **Replaces** this process with `pty` (via exec)
/// so the interactive UI keeps the tty, signals, and exit code.
fn pty_cmd(args: &[String]) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let root = catalog_root_for_env()?;
    require_runtime_mutation_admission(&root, None)?;
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
    require_runtime_mutation_admission(&root, None)?;
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

    // 3) Per this-host declaration: active tasks must be alive with fresh presence; retired tasks
    // must all be absent and need no presence file.
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
        if spec.retired {
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
                    "no status file — is its ding refreshing?",
                );
            } else {
                let state = st2::status::read_state(&path);
                report_check(
                    &mut problems,
                    state != st2::status::State::Unknown,
                    &format!("{bus_id} presence fresh (is `{}`)", state.as_str()),
                    "rotted to `unknown` — is its ding refreshing?",
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

fn tasks_cmd(
    root: &Path,
    host: Option<String>,
    selection: Option<st2::task_inventory::DesiredStateSelection>,
) -> Result<()> {
    let host = host.unwrap_or_else(detect_host);
    let catalog = match root.canonicalize() {
        Ok(catalog) => catalog,
        Err(error) => {
            let detail = format!("canonicalize catalog {}: {error}", root.display());
            let inventory = st2::task_inventory::TaskInventory::incomplete(
                root.to_path_buf(),
                host,
                detail,
                selection,
            );
            println!("{}", inventory.to_json());
            anyhow::bail!("task inventory incomplete")
        }
    };
    let _catalog_lock = match st2::CatalogLock::shared(&catalog) {
        Ok(lock) => lock,
        Err(error) => {
            let detail = format!("acquire shared catalog-authoring lock: {error:#}");
            let inventory =
                st2::task_inventory::TaskInventory::incomplete(catalog, host, detail, selection);
            println!("{}", inventory.to_json());
            anyhow::bail!("task inventory incomplete")
        }
    };
    let found = discover(&catalog);
    let runner = SystemRunner::new(catalog.clone(), exec_state_dir(&host));
    let inventory = st2::task_inventory::inventory(&catalog, &host, &found, &runner, selection);
    println!("{}", inventory.to_json());
    if inventory.complete() {
        Ok(())
    } else {
        anyhow::bail!("task inventory incomplete")
    }
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
            st2::status::set_state(&sp, state)?;
            println!("status: {}", state.as_str());
        }
    }
    Ok(())
}

fn agents_cmd(
    catalog: Option<PathBuf>,
    status_filter: Option<String>,
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
    let mut rows = st2::agents::roster(&root, &host);
    if let Some(f) = &status_filter {
        rows.retain(|r| r.status.as_str() == f);
    }
    if json {
        println!("{}", st2::agents::to_json(&rows, enrich));
    } else {
        for r in &rows {
            let retired = if r.retired { "\t[retired]" } else { "" };
            println!(
                "{}\t{}\t{}\t{}{}",
                r.identity,
                r.status.as_str(),
                r.name.as_deref().unwrap_or(""),
                r.description.as_deref().unwrap_or(""),
                retired,
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
    let agent_dir = message::resolve_agent_dir(&catalog_root, &id, &this_host)
        .unwrap_or_else(|| catalog_root.join(&id));
    let inbox = message::resolve_inbox(&catalog_root, &id, &this_host)?;
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
    ding::serve(&inbox, &status_path, &session, &config)
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
    message::resolve_agent_dir(root, id, host)
        .with_context(|| format!("no agent '{id}' found in catalog {}", root.display()))
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
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let from = acting_id(&ctx)?;
            let body = body_or_stdin(body)?;
            let dir = message::resolve_inbox(&root, &to, &host)?;
            let filename = message::send_to_inbox(
                &dir,
                &from,
                subject.as_deref(),
                in_reply_to.as_deref(),
                &tags,
                &body,
            )?;
            println!("{filename}");
            Ok(())
        }
        MessageCmd::Reply {
            filename,
            body,
            subject,
            ctx,
        } => {
            let (root, host) = resolve_ctx(&ctx)?;
            let from = acting_id(&ctx)?;
            let my_inbox = message::resolve_inbox(&root, &from, &host)?;
            let original = message::read_msg(&my_inbox, &filename)
                .with_context(|| format!("no message '{filename}' in {}'s inbox", from))?;
            let to = original
                .from
                .clone()
                .with_context(|| format!("message '{filename}' has no `from` to reply to"))?;
            let subject = subject.or_else(|| message::reply_subject(original.subject.as_deref()));
            let body = body_or_stdin(body)?;
            let dir = message::resolve_inbox(&root, &to, &host)?;
            let sent = message::send_to_inbox(
                &dir,
                &from,
                subject.as_deref(),
                Some(&filename),
                &[],
                &body,
            )?;
            println!("{sent}");
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
                message::resolve_inbox(&root, &id, &host)
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
            let inbox = message::resolve_inbox(&root, &id, &host)?;
            let archive = message::resolve_archive(&root, &id, &host)?;
            message::archive_msg(&inbox, &archive, &filename)?;
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
            let mut entries = message::collect_thread(&root, &filename);
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
            let dir = resolve_context_dir(identity, &ctx)?;
            let content =
                std::io::read_to_string(std::io::stdin()).context("reading context from stdin")?;
            context::write_now(&dir, &content)?;
            eprintln!("context: wrote now.md ({} bytes)", content.len());
            Ok(())
        }
        ContextCmd::Append {
            identity,
            decision,
            why,
            ctx,
        } => {
            let dir = resolve_context_dir(identity, &ctx)?;
            let filename = context::append_decision(&dir, &decision, &why)?;
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
        ServiceCmd::Uninstall => {
            let catalog = catalog_root_for_env()?;
            st2::service::uninstall(&catalog)
        }
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
            let dir = st2::resource::links_dir(&agent_dir_of(&root, &acting_id(&ctx)?, &host)?);
            let body = if body_stdin {
                std::io::read_to_string(std::io::stdin())?
            } else {
                String::new()
            };
            let f = st2::resource::add(
                &dir,
                &url,
                title.as_deref(),
                &tags,
                relation.as_deref(),
                &body,
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
            st2::resource::remove(
                &st2::resource::links_dir(&agent_dir_of(&root, &id, &host)?),
                &filename,
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
    // Seats boot as fresh top-level agents (strip the launcher's agent identity) and their bare
    // `st2 …` resolve to this binary (PATH prepend) — same prep `st2 eval` does.
    st2::eval_run::prepare_spawn_env();
    let specs = st2::eval_run::spec_to_agent_specs(&spec.agents, &this_host, &root);
    let runner = SystemRunner::new(root.clone(), exec_state_dir(&this_host));

    if once {
        let report = st2::up_once_specs(&specs, &root, &this_host, &runner);
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

    eprintln!(
        "st2: supervising spec {} on host '{this_host}' ({} agents; reconcile every {interval}s; Ctrl-C to stop)",
        spec_file.display(),
        specs.len()
    );
    st2::up_loop_specs(
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
    )
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
        let ownership = st2::host_lock::HostOwnership::acquire(&catalog_root, &this_host)
            .context("acquire runtime host ownership for materialization")?;
        let admission = st2::cutover_admission::RuntimeMutationAdmission::ordinary(&ownership)?;
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
        let report = st2::materialize::materialize_catalog_against_admitted(
            &admission.permission(),
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

    if once {
        let targeted = task.is_some();
        let report = match task.as_deref() {
            Some(selector) => {
                st2::run::up_once_selected(&catalog_root, selector, &this_host, &runner)?
            }
            None => up_once(root, &this_host, &runner)?,
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

    eprintln!(
        "st2: supervising {} on host '{this_host}' (reconcile every {interval}s + on change; Ctrl-C to stop)",
        root.display()
    );
    up_loop(
        root,
        &this_host,
        &runner,
        Duration::from_secs(interval),
        |report| {
            if report.is_noteworthy() {
                print_report(report);
            }
        },
    )
}

fn print_report(report: &UpReport) {
    report_line("launched", &report.launched);
    report_line("torn down", &report.torn_down);
    report_line("gc", &report.gc);
    report_line("held", &report.held);
    report_line("flapping", &report.flapping);
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

    if found.specs.is_empty() {
        println!("no specs found under {}", root.display());
    }
    for spec in &found.specs {
        let host = spec.host.as_deref().unwrap_or("<this-host>");
        let kind = match spec.job_type {
            st2::JobType::Service => "service",
        };
        let runnable = if spec.is_runnable() {
            ""
        } else {
            "  [UNRENDERED: no task launch]"
        };
        let retired = if spec.retired { "  [retired]" } else { "" };
        println!(
            "{host}.{ident}  [{kind}] ({n} task{plural}){runnable}{retired}\n    {path}",
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
