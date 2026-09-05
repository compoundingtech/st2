//! Controlled Claude launch with a session-owned presence lease.
//!
//! Claude can close its stdio MCP child after startup. That child cannot prove that the interactive
//! provider still lives. This wrapper launches the provider and refreshes presence while that exact
//! child remains alive. It uses the provider's existing terminal process group. The launch body
//! itself lives in [`crate::provider_session`], which every interactive harness wrapper shares.
//!
//! Observed harness state for Claude has two producers with one owner each: hook invocations
//! (`st2 driver claude-observe`, [`run_observe`]) write turn transitions, and the wrapper's poll
//! loop re-stamps and terminates the record through [`SessionObserver`] without ever overwriting a
//! state a hook wrote in between.

use std::io::{Read as _, Write as _};
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::harness_context::{self, Compaction, CompactionTrigger, Harness, RateLimits, Reading};
use crate::harness_state::{
    Activity, Ask, AskKind, BlockedOn, CapabilityEvidence, ConditionReport, ConversationClaim,
    ConversationState, FaultCategory, FaultReport, Frame, HistoryMutability, HumanAsk, InputBuffer,
    Observation, Recovery, WriteOutcome,
};
use crate::provider_session::{
    PROVIDER_POLL, STOP, SessionObserver, install_signal_handler, run_provider,
};
use crate::{driver_diagnostic, harness_state, message, status};

/// Run one interactive Claude provider and maintain its presence until it exits.
pub fn run(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    claude_argv: Vec<String>,
) -> Result<()> {
    let agent_dir =
        message::resolve_agent_dir(catalog_root, &identity, &crate::run::detect_host())?
            .with_context(|| format!("Claude driver agent '{identity}' is not declared"))?;
    anyhow::ensure!(
        !claude_argv.is_empty(),
        "Claude driver '{runtime_id}' has no provider argv"
    );
    let claude_argv = prepare_channel_argv(catalog_root, &identity, claude_argv)?;
    let workspace = std::env::current_dir().context("reading the Claude driver workspace")?;
    crate::pretrust::pretrust_claude(std::slice::from_ref(&workspace))
        .with_context(|| format!("admitting Claude driver workspace {}", workspace.display()))?;
    install_signal_handler();
    let observer = SessionObserver::new(&agent_dir, &identity, "claude", &runtime_id)?;
    // The runtime ID reaches hook subprocesses through the provider environment, so their
    // transitions carry the same pty session the wrapper's records do.
    let env = [
        (RUNTIME_ID_ENV.to_string(), runtime_id.clone()),
        // Hook subprocesses adopt the wrapper's incarnation token, so their transitions are this
        // session's records: the wrapper can re-stamp them, and its terminal record fences them.
        (SESSION_ENV.to_string(), observer.session().to_string()),
        (SESSION_SEQ_ENV.to_string(), observer.seq().to_string()),
    ];
    run_provider(
        "Claude",
        &status::status_path(&agent_dir),
        &claude_argv,
        &env,
        status::STATUS_REFRESH,
        PROVIDER_POLL,
        &STOP,
        Some(&observer),
    )
    .with_context(|| format!("running Claude driver '{runtime_id}'"))
}

fn requires_st2_channel(argv: &[String]) -> bool {
    argv.windows(2)
        .any(|pair| pair[0] == "--channels" && pair[1] == crate::claude_channel::CHANNEL)
}

/// Prefer the approved plugin, but preserve an interactive development path when it is absent.
///
/// The fallback keeps its MCP declaration in provider arguments. It does not write project state.
fn prepare_channel_argv(
    catalog_root: &Path,
    identity: &str,
    argv: Vec<String>,
) -> Result<Vec<String>> {
    if !requires_st2_channel(&argv) {
        return Ok(argv);
    }
    match crate::claude_channel::verify_installed() {
        Ok(()) => Ok(argv),
        Err(error) => {
            eprintln!(
                "warning: the approved st2 Claude channel plugin is unavailable: {error:#}\n\
                 warning: using Claude's interactive development channel; Claude can ask for confirmation\n\
                 warning: run `st2 claude-channel install` for unattended startup"
            );
            let executable = std::env::current_exe()
                .context("resolving the st2 executable for the Claude development channel")?;
            development_channel_argv(argv, &executable, catalog_root, identity)
        }
    }
}

fn development_channel_argv(
    argv: Vec<String>,
    executable: &Path,
    catalog_root: &Path,
    identity: &str,
) -> Result<Vec<String>> {
    let mcp = serde_json::json!({
        "mcpServers": {
            "st2": {
                "type": "stdio",
                "command": executable,
                "args": [
                    "--catalog",
                    catalog_root,
                    "driver",
                    "claude-mcp",
                    "--id",
                    identity
                ]
            }
        }
    });
    let mut output = Vec::with_capacity(argv.len() + 1);
    let mut index = 0;
    let mut replaced = false;
    while index < argv.len() {
        if !replaced
            && argv[index] == "--channels"
            && argv.get(index + 1).map(String::as_str) == Some(crate::claude_channel::CHANNEL)
        {
            output.extend([
                "--mcp-config".to_string(),
                serde_json::to_string(&mcp)
                    .context("serializing the Claude development channel")?,
                "--dangerously-load-development-channels=server:st2".to_string(),
            ]);
            replaced = true;
            index += 2;
            continue;
        }
        output.push(argv[index].clone());
        index += 1;
    }
    anyhow::ensure!(replaced, "the Claude plugin channel selector is missing");
    Ok(output)
}

/// Apply one Claude hook event (payload on stdin) to the agent's observed-harness-state record.
///
/// Invoked per event by the fail-open `claude-observe.sh` hook, so each invocation is its own
/// short-lived writer; the transition counter continues from disk.
/// The env var carrying the wrapper's runtime/task ID into Claude's hook subprocesses.
pub const RUNTIME_ID_ENV: &str = "ST2_CLAUDE_RUNTIME_ID";
/// The env var carrying the wrapper's session incarnation token into Claude's hook subprocesses.
pub const SESSION_ENV: &str = "ST2_CLAUDE_SESSION";
/// The env var carrying the wrapper's claimed ownership sequence beside the token.
pub const SESSION_SEQ_ENV: &str = "ST2_CLAUDE_SESSION_SEQ";

pub fn run_observe(
    catalog_root: &Path,
    identity: &str,
    runtime_id: Option<&str>,
    event: &str,
) -> Result<()> {
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("Claude driver agent '{identity}' is not declared"))?;
    // Counted only once the invocation has its application target: a hook for an undeclared
    // agent errors out before any state is applied and must not inflate `hook_invocations_total`.
    crate::metrics::record_hook_invocation("claude-observe", event);
    let mut raw = String::new();
    let _ = std::io::stdin().read_to_string(&mut raw);
    let payload = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
    // The numeric axis is independent of the categorical one and is applied first, because the
    // events that carry a compaction edge say nothing about top-level harness state and would
    // otherwise return below. Fail-open: a context record that cannot be written must never stop
    // a hook the harness is waiting on, and the numbers authorize nothing (HC-A02).
    if let Err(error) = observe_compaction(&agent_dir, identity, event, &payload) {
        tracing::warn!("st2 claude-observe: harness-context compaction write failed: {error:#}");
    }
    // The credential axis is independent of both the numbers and the categorical state, and is
    // applied before the observation guard below for the same reason the compaction write is:
    // an edge that carries no top-level state change must still reach its own record.
    if let Some(edge) = provider_auth_edge(event, &payload) {
        publish_provider_auth(&agent_dir, edge);
    }
    let Some(observed) = observe_hook_event(event, &payload) else {
        return Ok(());
    };
    let mut writer = observe_writer(
        &agent_dir,
        identity,
        runtime_id,
        event,
        &payload,
        std::env::var(SESSION_ENV).ok().filter(|t| !t.is_empty()),
        std::env::var(SESSION_SEQ_ENV)
            .ok()
            .and_then(|seq| seq.parse::<u64>().ok()),
    );
    if event == "SessionStart" {
        // The one event that names a session boundary: even if the new session's first state
        // matches a fresh predecessor record, continuity must not be claimed across the restart.
        writer.interrupt();
    }
    // A late hook finishing after the wrapper reaped Claude must not replace the terminal record
    // with a live state: the wrapper's `ended` carries this same token and is the session's last
    // word — on both surfaces below.
    if !writer.writes_condition_axis() {
        // The version this build emits carries no condition, tagged-ask, or conversation axis,
        // and this record has exactly ONE source of truth: state the legacy triple, exactly as
        // this adapter always has. Keeping a stated fault in a sidecar the session's sibling
        // processes cannot see would be that second source. Activation is one flip of the
        // writer's emitted version, after which the branch below takes over unchanged.
        // (`false` = suppressed; the hook has nothing else to do with it.)
        return writer
            .observe_unless_ended(observed.observation())
            .map(|_wrote| ());
    }
    match writer.publish_unless_ended(observed.frame())? {
        // Written or already-said are both "the record now says this".
        WriteOutcome::Landed | WriteOutcome::Coalesced => Ok(()),
        // A refusal is a VALUE here — a successor session's claim, a foreign schema, a condition
        // axis nobody has stated yet, this session's own terminal record — and a hook process has
        // no second record to report it through, so it is diagnosed rather than swallowed. Still
        // fail-open: the harness is waiting on this hook, and no observation is worth wedging it.
        WriteOutcome::Refused(refusal) => {
            tracing::warn!(
                "st2 claude-observe: observed-state write refused for {event}: {refusal:?}"
            );
            Ok(())
        }
    }
}

/// Select the ownership a hook write acts under. The wrapper's exported token makes hook writes
/// this session's records (adopted ownership when the claimed sequence travels beside it). A
/// wrapperless seat falls back to Claude's own session_id — and because token-only writers never
/// claim, the session-boundary arm IS that path's boundary and performs the WRITTEN claim
/// (degrading to token-only with a warning if the claim cannot be written); later hooks of the
/// same session adopt its records by token. What such a seat still lacks is a heartbeat and
/// terminal owner — the documented hooks-only limitation.
///
/// Which `SessionStart` is that boundary depends on what the emitted version can carry: see
/// [`claims_wrapperless_session`].
#[allow(clippy::too_many_arguments)]
fn observe_writer(
    agent_dir: &Path,
    identity: &str,
    runtime_id: Option<&str>,
    event: &str,
    payload: &serde_json::Value,
    exported_session: Option<String>,
    exported_seq: Option<u64>,
) -> harness_state::Writer {
    let pty_session = runtime_id.unwrap_or(identity).to_string();
    let writer = harness_state::Writer::new(agent_dir, identity, "claude", Some(pty_session));
    if let Some(session) = exported_session {
        return match exported_seq {
            Some(seq) => writer.with_ownership(session, seq),
            None => writer.with_session(session),
        };
    }
    if let Some(token) = wrapperless_token(payload) {
        if claims_wrapperless_session(writer.writes_condition_axis(), event, payload) {
            // Eligibility and the written takeover are ONE act under the record lock: a
            // hooks-only SessionStart racing a wrapper's startup can no longer steal the
            // sequence between the wrapper's read and its write. Ineligible (a live wrapper or
            // its fresh claim placeholder owns the record) or unwritable both degrade to
            // token-only.
            return match harness_state::claim_wrapperless(agent_dir, identity, "claude", &token) {
                Ok(Some(seq)) => writer.with_ownership(token, seq),
                Ok(None) => writer.with_session(token),
                Err(error) => {
                    tracing::warn!(
                        "st2 claude-observe: observed-state claim failed; degrading to token-only: {error:#}"
                    );
                    writer.with_session(token)
                }
            };
        }
        return writer.with_session(token);
    }
    writer
}

/// Whether this hook invocation is the WRAPPERLESS seat's session boundary — the one act that
/// performs the written claim.
///
/// While the emitted version carries no condition axis, this is every `SessionStart`, exactly as
/// this adapter has always behaved: same claim, same sequence, same bytes.
///
/// Once the condition axis is on the wire a claim is no longer neutral. A claim writes this
/// session's FENCE, and a fence is deliberately excluded from what an unstated axis inherits, so
/// claiming on a MID-PROCESS `SessionStart` — `compact`, `clear` — would drop a standing fault by
/// ownership: exactly the laundering the mapping refuses to do with `clear`, arriving through the
/// other door. Those events keep the token-only path, where the record already carries this
/// session's token, its sequence is adopted, and the standing condition carries. Only a genuinely
/// fresh incarnation claims, which is the one case where superseding the axis is the truth.
fn claims_wrapperless_session(
    writes_condition_axis: bool,
    event: &str,
    payload: &serde_json::Value,
) -> bool {
    event == "SessionStart"
        && (!writes_condition_axis || session_start_is_fresh_incarnation(payload))
}

/// Claude's own session id under the wrapperless prefix, for a seat that runs `claude` directly
/// with no session wrapper to export a token.
///
/// Factored out so [`observe_writer`] and [`context_writer`] derive the same token from the same
/// payload field: the hook subprocesses and the status-line tee of one seat must publish ONE
/// incarnation, or a reader could not tell a straggler from a sibling (HC-A03, HC-R15).
fn wrapperless_token(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .map(|id| format!("{}{id}", harness_state::WRAPPERLESS_PREFIX))
}

/// The Claude harness-context writer for one driver subprocess, under the ownership
/// [`observe_writer`] selects: the wrapper's exported token when a wrapper launched this seat,
/// otherwise Claude's own session id under the wrapperless prefix.
///
/// The claim half of that selection is deliberately absent. `incarnation` on this record is
/// PROVENANCE and is never consulted as a fence (HC-R15), so there is nothing here to claim —
/// which is also why the status-line tee, which is not a hook and has no session boundary, can
/// share the selection without sharing the takeover.
fn context_writer(
    agent_dir: &Path,
    identity: &str,
    payload: &serde_json::Value,
) -> Result<harness_context::Writer> {
    let writer = harness_context::Writer::new(agent_dir, identity, Harness::Claude)?;
    let exported = std::env::var(SESSION_ENV).ok().filter(|t| !t.is_empty());
    Ok(match exported.or_else(|| wrapperless_token(payload)) {
        Some(token) => writer.with_session(token),
        None => writer,
    })
}

/// Apply one Claude compaction edge to the agent's harness-context record (HC-R12).
///
/// Claude publishes THREE edges for one compaction — `PreCompact`, `PostCompact`, and a
/// `SessionStart` carrying `source: "compact"` — and each arrives in its own short-lived hook
/// process with nothing durable passed between them. A counter incrementing on more than one of
/// them would treble-count every compaction, so the dedupe is positional rather than stateful:
///
/// - **`PreCompact` is the sole counting edge.** It is the first, it fires for every compaction
///   including one whose `PostCompact` never arrives (a compaction that ends the session, or a
///   future build that drops the event), and it carries `trigger`. Counting on the FIRST edge is
///   what makes the dedupe stateless — "count on the second only if the first was seen" would
///   need per-compaction memory the record deliberately does not carry (HC-T02).
/// - **`PostCompact` does not count.** It holds the count it finds and advances
///   `lastCompactionMs` from when compaction *started* to when the window was actually emptied.
///   Only if it finds no counted compaction at all — no record, or one whose `PreCompact` write
///   never landed — does it count, because it is then the first evidence st2 has.
/// - **`SessionStart source=compact` is recognized and deliberately inert.** It is the same
///   compaction seen a third time; counting it is exactly the double count HC-R12 forbids.
///
/// The counter is incarnation-scoped: st2 does the counting, and `harness-context` is removed at
/// the relaunch claim (HC-R15), so the count describes this incarnation and not the seat's life.
fn observe_compaction(
    agent_dir: &Path,
    identity: &str,
    event: &str,
    payload: &serde_json::Value,
) -> Result<()> {
    // A subagent's compaction is not the top-level session's, and this record describes the
    // top-level window (the status-line payload carries no subagent window either — DQ-C9). The
    // guard matches `observe_hook_event`'s for the same reason.
    if payload
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|id| !id.is_empty())
    {
        return Ok(());
    }
    let trigger = compaction_trigger(payload);
    let edge = match event {
        "PreCompact" => Compaction::new(trigger),
        "PostCompact" => {
            match harness_context::read(&harness_context::harness_context_path(agent_dir)) {
                Some(observed) if observed.compactions > 0 => {
                    Compaction::new(trigger).with_count(observed.compactions)
                }
                _ => Compaction::new(trigger),
            }
        }
        _ => return Ok(()),
    };
    context_writer(agent_dir, identity, payload)?
        .compacted(edge)
        .map(|_landed| ())
}

/// The trigger word a Claude compaction hook carries. 2.1.250 publishes `manual` and `auto` on
/// both `PreCompact` and `PostCompact`; the vocabulary is closed and additive-tolerant, so a word
/// this version does not publish decodes as `unknown` rather than as a definite trigger.
fn compaction_trigger(payload: &serde_json::Value) -> CompactionTrigger {
    match payload.get("trigger").and_then(serde_json::Value::as_str) {
        Some("manual") => CompactionTrigger::Manual,
        Some("auto") => CompactionTrigger::Auto,
        _ => CompactionTrigger::Unknown,
    }
}

/// The Claude Code version this producer's arithmetic was measured against (HC-R13). A bump that
/// moves the numerator, the denominator, or the percent rule must fail the fixture rather than
/// silently publish a differently-meaning number.
pub const STATUSLINE_VERSION: &str = "2.1.250";

/// The environment variable holding the operator's downstream status-line renderer. Checked
/// FIRST, so one agent, a debugging session, or a test can override without editing a file.
pub const STATUSLINE_RENDERER_ENV: &str = "ST_CLAUDE_STATUSLINE_RENDERER";

/// The operator-owned renderer file, relative to `$HOME`. Schema
/// `dotfiles.claude-statusline-renderer.v1`, carrying `{"command": …}` — dotfiles owns the file
/// and its shape, and st2 reads `command` and nothing else (`DQ-C2`, dotfiles PR #2160).
///
/// It is a file rather than a settings key because the settings file st2 wins in is the one st2
/// rewrites: a renderer declared there would be the very thing the merge does not preserve, which
/// is HC-R18's inverse. A user-level file st2 never writes has no such hazard.
const STATUSLINE_RENDERER_FILE: &str = ".claude/statusline-renderer.json";

/// Project one Claude status-line payload onto a harness-context reading (HC-R02, HC-R03).
///
/// The status-line payload is the ONLY Claude channel carrying a window: hook payloads have no
/// token fields at all, and the transcript has per-message `usage` but no window size, so a
/// transcript-only producer would have to invent the denominator from a model table — which
/// cannot tell a 200k tier from a 1M tier for one model id, and is exactly what HC-R02 forbids.
///
/// The numerator is read from `current_usage`, NOT from the sibling `total_input_tokens`. They
/// agree whenever Claude knows its occupancy — the bundle's builder defines the latter as
/// `input_tokens + cache_creation_input_tokens + cache_read_input_tokens` of `current_usage` —
/// but it emits `0` for it precisely when `current_usage` is null, so before the session's first
/// API response the sibling is a zero DERIVED FROM AN ABSENCE. Reading `current_usage` makes the
/// withholding structural: no reading, no numerator (HC-R03).
///
/// `used_percent` is Claude's own integer, already clamped to 0..100 by `QUt` in the bundle, and
/// st2 never recomputes it from the operands (HC-R02). `sessionTotalTokens` is `null`: the
/// payload's `total_*` keys describe the last response, not the session, and calling them
/// cumulative would be the exact confusion HC-R16 names.
pub fn statusline_reading(payload: &serde_json::Value) -> Reading {
    let window = payload.pointer("/context_window");
    let usage = window
        .and_then(|window| window.get("current_usage"))
        .filter(|usage| !usage.is_null());
    Reading {
        used_tokens: usage.map(|usage| {
            [
                "input_tokens",
                "cache_creation_input_tokens",
                "cache_read_input_tokens",
            ]
            .into_iter()
            .filter_map(|key| usage.get(key).and_then(serde_json::Value::as_u64))
            .sum()
        }),
        window_tokens: window
            .and_then(|window| window.get("context_window_size"))
            .and_then(serde_json::Value::as_u64),
        used_percent: window
            .and_then(|window| window.get("used_percentage"))
            .and_then(serde_json::Value::as_f64),
        model: payload
            .pointer("/model/id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        cost_usd: payload
            .pointer("/cost/total_cost_usd")
            .and_then(serde_json::Value::as_f64),
        // The payload's `total_input_tokens`/`total_output_tokens` describe the LAST RESPONSE.
        // Claude publishes no cumulative session total, and a producer accumulating one itself
        // would be maintaining a second running total whose correctness depends on having seen
        // every render — a worse answer than none (HC-R16).
        session_total_tokens: None,
        rate_limits: RateLimits {
            five_hour: payload
                .pointer("/rate_limits/five_hour/used_percentage")
                .and_then(serde_json::Value::as_f64),
            seven_day: payload
                .pointer("/rate_limits/seven_day/used_percentage")
                .and_then(serde_json::Value::as_f64),
        },
    }
}

/// The status-line tee: record the payload, then chain to the operator's own renderer (HC-R18).
///
/// Claude's `statusLine` is a SINGLE slot whose winning declaration replaces the others outright
/// — measured 2026-08-29 against 2.1.250 through a real pty, `.claude/settings.local.json` >
/// `.claude/settings.json` > `~/.claude/settings.json`, with the losing renderer never invoked.
/// Since `.claude/settings.local.json` is exactly the file st2 materializes for a driver-declared
/// seat, an st2 entry that does not chain would silently and unconditionally remove the
/// operator's status line on every managed agent, with no warning.
///
/// Every failure here degrades to an EMPTY status line, and stdout carries the renderer's bytes
/// or nothing at all. The payload is a machine-readable JSON object — session id, transcript
/// path, model block, usage block — so echoing it where a status line belongs paints a wall of
/// JSON across the operator's terminal every five seconds, which is strictly worse for them than
/// a blank line and gives them nothing to act on. The reason to degrade is the same either way;
/// only the human-facing line is at stake, and recording is unaffected by which arm runs.
///
/// Recording is best-effort and only warns; so does each degraded arm. Both warn on stderr,
/// which Claude routes to its debug log rather than to the status line, so the diagnostic names
/// what failed without ever touching the rendered row.
pub fn run_statusline(catalog_root: &Path, identity: &str) -> Result<()> {
    let mut raw = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut raw);
    if let Err(error) = record_statusline(catalog_root, identity, &raw) {
        tracing::warn!("st2 claude-statusline: recording failed; chaining anyway: {error:#}");
    }
    chain_statusline(&raw)
}

fn record_statusline(catalog_root: &Path, identity: &str, raw: &[u8]) -> Result<()> {
    let payload: serde_json::Value = serde_json::from_slice(raw).unwrap_or(serde_json::Value::Null);
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("Claude driver agent '{identity}' is not declared"))?;
    // Deliberately uncounted. The tee builds no telemetry pipeline at all (`DQ-C13`, see
    // `main`), so a `record_hook_invocation` here could never reach a collector — and a metric
    // call that provably cannot record is worse than none: it reads as instrumentation.
    // `06-observability`'s spec already scopes `hook_invocations_total` to `claude-observe` and
    // says other hook surfaces are not instrumented yet, which is exactly this.
    context_writer(&agent_dir, identity, &payload)?
        .observe(statusline_reading(&payload))
        .map(|_landed| ())
}

/// Two sources in strict order, first hit wins, never merged and never both — so the resolution
/// has one answer and an operator debugging their status line has one place to look for it.
/// Neither resolving is the third case, handled by the caller as an empty status line.
fn downstream_renderer() -> Option<String> {
    if let Some(command) = std::env::var(STATUSLINE_RENDERER_ENV)
        .ok()
        .filter(|command| !command.trim().is_empty())
    {
        return Some(command);
    }
    let path = std::path::PathBuf::from(std::env::var_os("HOME")?).join(STATUSLINE_RENDERER_FILE);
    let declaration: serde_json::Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    declaration
        .get("command")
        .and_then(serde_json::Value::as_str)
        .filter(|command| !command.trim().is_empty())
        .map(str::to_string)
}

fn chain_statusline(raw: &[u8]) -> Result<()> {
    let Some(command) = downstream_renderer() else {
        // Both resolution paths named, because "no renderer resolved" is the whole diagnosis and
        // the operator's next move is to set one of exactly these two.
        tracing::warn!(
            "st2 claude-statusline: no downstream renderer resolved from \
             ${STATUSLINE_RENDERER_ENV} or ~/{STATUSLINE_RENDERER_FILE}; \
             rendering an empty status line"
        );
        return Ok(());
    };
    // The renderer is a shell command line, exactly as Claude's own `statusLine.command` is, so
    // it is run the way Claude would run it.
    let child = std::process::Command::new("sh")
        .arg("-c")
        .arg(&command)
        .stdin(std::process::Stdio::piped())
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        Err(error) => {
            tracing::warn!(
                "st2 claude-statusline: downstream renderer `{command}` could not start: {error}"
            );
            return Ok(());
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(raw);
    }
    let _ = child.wait();
    Ok(())
}

/// The ask surface Claude can state, closed at three words. `PermissionRequest`'s `tool_name` is
/// the ONLY ask signal any registered hook carries and it distinguishes exactly two kinds, so
/// both projections below are total functions of one decision. Storing a [`HumanAsk`] here
/// instead would force this adapter to invent a legacy word for `Pending(Review)` and for
/// `Unknown` — kinds Claude has no signal for at all — and an invented projection is how the
/// legacy and tagged axes start disagreeing about the same prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookAsk {
    None,
    Permission,
    Question,
}

impl HookAsk {
    fn tagged(self) -> HumanAsk {
        match self {
            Self::None => HumanAsk::None,
            Self::Permission => HumanAsk::Pending(AskKind::Permission),
            Self::Question => HumanAsk::Pending(AskKind::Question),
        }
    }

    /// The legacy pair, verbatim as this adapter has always written it: an ask is meaningful only
    /// while blocked on a human, which is exactly what the two pending kinds mean here.
    fn legacy(self) -> (BlockedOn, Ask) {
        match self {
            Self::None => (BlockedOn::None, Ask::None),
            Self::Permission => (BlockedOn::Human, Ask::Permission),
            Self::Question => (BlockedOn::Human, Ask::Question),
        }
    }
}

/// What one Claude hook event states about top-level harness state: every axis at once, decided
/// from one look at the payload, so no reader can observe the activity, the condition, the ask
/// and the conversation link half-applied.
///
/// `condition: Unchanged` is the load-bearing default. Almost every Claude edge is an activity or
/// ask edge that has observed NOTHING about whether the provider is faulted, and a condition may
/// be cleared only by a positive success edge (`Stop`) or a new incarnation — never by a turn
/// starting, a tool running, or a compaction restart. Stating the carry here rather than reading
/// the standing condition first also keeps the decision inside the single lock the write holds:
/// check-then-act across two acquisitions is the race `claim_wrapperless` exists to avoid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookState {
    pub state: Activity,
    pub ask: HookAsk,
    pub condition: ConditionReport,
    /// Stated only by the incarnation boundary and the turn-end edges — the ones that neither
    /// repeat nor fire several times a turn — because the link's `verifiedThroughMs` is minted
    /// from the hook's clock: restating it on a repeatable edge would make every repeat a
    /// different tuple, landing a write and restarting `sinceMs`. `None` leaves the axis exactly
    /// as the record has it, which is how the standing link carries.
    pub conversation: Option<ConversationState>,
    /// Diagnostic only, and one of a closed set of static words. No consumer branches on it.
    pub reason: Option<&'static str>,
}

impl HookState {
    fn new(state: Activity, ask: HookAsk, condition: ConditionReport) -> Self {
        Self {
            state,
            ask,
            condition,
            conversation: None,
            reason: None,
        }
    }

    fn with_reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self
    }

    fn with_conversation(mut self, conversation: Option<ConversationState>) -> Self {
        self.conversation = conversation;
        self
    }

    /// The legacy observation this event has always written — same activity, same legacy
    /// blocked/ask pair, same reason — for the version this build still emits. The condition and
    /// conversation axes are dropped rather than stored beside the record: the version 2 wire has
    /// nowhere to carry them, and a sidecar would give this record a second source of truth.
    fn observation(&self) -> Observation {
        let (blocked_on, ask) = self.ask.legacy();
        let observation =
            Observation::new(self.state, blocked_on, InputBuffer::Unknown).with_ask(ask);
        match self.reason {
            Some(reason) => observation.with_reason(reason),
            None => observation,
        }
    }

    /// The version 3 tuple: one statement of every axis this event resolved.
    fn frame(&self) -> Frame {
        let frame = Frame::new(
            self.state,
            InputBuffer::Unknown,
            self.condition.clone(),
            self.ask.tagged(),
        );
        let frame = match self.conversation.clone() {
            Some(conversation) => frame.with_conversation(conversation),
            None => frame,
        };
        match self.reason {
            Some(reason) => frame.with_reason(reason),
            None => frame,
        }
    }
}

/// Map one Claude hook event to the state it proves, or `None` when the event says nothing about
/// top-level harness state.
///
/// Claude gives no call identity on the event that enters `blocked` (`PermissionRequest` carries
/// no `tool_use_id`; its `prompt_id` is turn-scoped), so the exit edge is the next
/// `PreToolUse`/`PostToolUse`/`Stop`. Measured 2026-08-23 (Claude Code 2.1.237, DQ-H1): tool
/// execution serializes around an open permission prompt — no hook event fires while a prompt is
/// up, even for a parallel-batched allowlisted call — so that next event is the blocked call's own
/// resolution and the batched false-clear #268 §C predicted cannot occur. The residual limit is
/// denial: "No" ends the turn with zero further events (no Stop, and no PermissionDenied even when
/// registered), so `blocked` stands until the next `UserPromptSubmit`/`SessionStart`.
///
/// DECLARED UNSUPPORTED, stated rather than guessed. A user interrupt (Esc/Ctrl-C) emits no hook
/// event at all, so activity stays `active` with no available edge and no idle is invented.
/// Crash-versus-clean-exit is not knowable here — `SessionEnd` is not registered and its reason
/// vocabulary cannot tell SIGKILL from logout — so the wrapper's exit status remains the only
/// source of `ended`. `Notification`, the only `auth_success` signal, is not registered either,
/// which is why Claude has no paired clear and no `AskKind::Review` mapping exists.
pub fn observe_hook_event(event: &str, payload: &serde_json::Value) -> Option<HookState> {
    observe_hook_event_at(event, payload, message::now_ms())
}

/// [`observe_hook_event`] with the hook's own clock passed in, which is what makes the whole
/// mapping a pure function of `(event, payload, now)`: a fault's SEMANTIC observation instant and
/// a conversation link's finite verification bound are both stamped from it.
fn observe_hook_event_at(
    event: &str,
    payload: &serde_json::Value,
    now_ms: u64,
) -> Option<HookState> {
    // Any event carrying an agent identity is a subagent's and must never move top-level state:
    // a phantom `SubagentStop` trails every completed turn, 1.5-2.9s after `Stop`. That phantom
    // populating `agent_id` is an undocumented emergent property — if a future Claude build omits
    // it, subagent completions read as top-level activity again and nothing here would catch it.
    // Evaluated before every axis: a subagent event must not raise, clear, or move anything.
    if payload
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|id| !id.is_empty())
    {
        return None;
    }
    let observed = match event {
        "SessionStart" => HookState::new(
            Activity::Idle,
            HookAsk::None,
            // A fresh incarnation is one of the two edges that may clear the axis: a fault stands
            // until an explicit paired clear, a terminal record, a new claim, or a NEW
            // INCARNATION, and `run_observe`'s `interrupt()` already makes this event that
            // boundary. The mid-process sources are NOT boundaries and must not clear.
            if session_start_is_fresh_incarnation(payload) {
                ConditionReport::Clear
            } else {
                ConditionReport::Unchanged
            },
        )
        .with_reason("sessionStart")
        // The boundary edge establishes the link for the incarnation; every later frame inherits
        // it from the record.
        .with_conversation(conversation_claim(payload, now_ms)),
        // Activity edges. Each releases a residual ask — including the DQ-H1 denial residual, for
        // which `UserPromptSubmit` is the only recovery edge — and states nothing whatsoever about
        // the condition: a turn starting is not evidence that a rejected credential was repaired.
        //
        // They state nothing about the conversation either, and that is deliberate. These are the
        // REPEATABLE edges — several per turn, measured — and the link they would restate is
        // identical except for a freshly minted `verifiedThroughMs`. That one moving field would
        // make every repeat a different tuple: the write would land instead of coalescing, and
        // `sinceMs` would restart on a state the seat never left. The standing link carries.
        "UserPromptSubmit" | "PreToolUse" | "PostToolUse" => {
            HookState::new(Activity::Active, HookAsk::None, ConditionReport::Unchanged)
        }
        // THE positive success edge, and Claude's only one: a turn that reached its ordinary end
        // is progress a standing fault would have prevented — `ProgressProof::TurnCompleted` —
        // so it clears the whole axis. Stated inside the tuple rather than through
        // `Writer::clear_all` because the activity and the clear are ONE observation here: two
        // writes would publish an intermediate record showing `idle` with the fault still
        // standing, and the sibling clear on a fresh incarnation cannot use the condition-only
        // path at all, since that record belongs to the predecessor session.
        //
        // A turn boundary is once-per-turn and never repeats without an intervening prompt, so
        // re-verifying the conversation link here refreshes its bound without the churn the
        // repeatable edges above would cause.
        "Stop" => HookState::new(Activity::Idle, HookAsk::None, ConditionReport::Clear)
            .with_conversation(conversation_claim(payload, now_ms)),
        // `StopFailure` fires INSTEAD of `Stop` when an API error ended the turn (Claude Code's
        // own words, 2.1.259), at the same lifecycle point — so the activity is the one `Stop`
        // writes and the failure rides the condition axis beside it. Deliberately not `ended`:
        // the TUI is still live, a human can re-login and carry on, and this seat's terminal
        // record belongs to the wrapper (OHS-T04). A hook claiming `ended` here would be a false
        // terminal. And deliberately not an ask: the remediation a human owes is visible on the
        // condition axis, while a synthesized `pending` would fabricate a prompt nobody can
        // answer and would outrank nothing.
        "StopFailure" => HookState::new(
            Activity::Idle,
            HookAsk::None,
            stop_failure_condition(payload, now_ms),
        )
        .with_reason(match stop_failure_error(payload) {
            Some(CLAUDE_AUTH_REJECTED_ERROR) => "providerAuth",
            _ => "apiError",
        })
        .with_conversation(conversation_claim(payload, now_ms)),
        "PermissionRequest" => {
            // Driver-side classification (#162): the payload's tool_name distinguishes Claude's
            // question form from an ordinary permission prompt — the DQ-H1 captures show
            // AskUserQuestion arriving as a PermissionRequest like any other tool.
            let ask = if payload.get("tool_name").and_then(serde_json::Value::as_str)
                == Some("AskUserQuestion")
            {
                HookAsk::Question
            } else {
                HookAsk::Permission
            };
            // An ask is not a fault: a waiting human is the seat working as designed, so the
            // condition axis is untouched here. Nor is a prompt evidence about the conversation:
            // a batch can raise several, so this edge repeats and carries the standing link.
            HookState::new(Activity::Active, ask, ConditionReport::Unchanged)
                .with_reason("permissionRequest")
        }
        // `PreCompact`/`PostCompact` are harness-CONTEXT edges (see `observe_compaction`) and
        // every other event is unregistered or unmapped: silence rather than a guess.
        _ => return None,
    };
    Some(observed)
}

/// Whether one `SessionStart` names a genuinely NEW incarnation — the only kind that may clear a
/// standing condition.
///
/// Two of Claude's source words are mid-process and must not: `compact` is the same session's
/// third sighting of one compaction (see `observe_compaction`), and `clear` empties the
/// conversation inside the running process — same pty, same wrapper, same st2 incarnation. A
/// fault the provider still holds survives both, so clearing on either would silence it on every
/// automatic compaction or every `/clear`. `startup` and `resume` are real process boundaries,
/// and an absent word is treated as one because that is what the event otherwise means.
fn session_start_is_fresh_incarnation(payload: &serde_json::Value) -> bool {
    !matches!(
        payload.get("source").and_then(serde_json::Value::as_str),
        Some(COMPACT_SOURCE | CLEAR_SOURCE)
    )
}

/// The `SessionStart` source word for a compaction restart: the same incarnation, mid-session.
const COMPACT_SOURCE: &str = "compact";
/// The `SessionStart` source word for an in-process `/clear`: same incarnation, emptied history.
const CLEAR_SOURCE: &str = "clear";

/// The condition one `StopFailure` states, from the closed `error` word and nothing else.
///
/// PRESENCE-TESTED, not defaulted: every word Claude publishes has its own arm, so
/// `max_output_tokens` can mean "no mapping" — a truncated response is not a seat fault, and
/// collapsing it into the unrecognized-word arm would page an operator once per long answer.
/// Codes are provider-namespaced with a slash, category-routable underneath, and the word rides
/// verbatim so the diagnostic granularity survives the categories collapsing several words into
/// one.
///
/// No row is [`Recovery::Automatic`] and no row sets a next-observation deadline, because Claude
/// declares neither: it names no reset instant for `rate_limit`, and its internal retries are
/// gated inside the bundle and never surfaced. An `automatic` recovery is projected as
/// recovering/soon and NEVER pages while its deadline holds, so an automatic Claude fault would
/// launder a wedged seat indefinitely. Unclearable classes therefore take [`Recovery::Unknown`],
/// which pages, and Claude's next `Stop` clears them.
///
/// The credential class comes from this typed word alone. `error_details` and
/// `last_assistant_message` ride the same payload and are prose: splitting `invalid_request` into
/// a context fault by matching English on them is exactly the prose classification INVARIANTS
/// forbids, so the too-large case stays declared-unsupported instead of guessed.
fn stop_failure_condition(payload: &serde_json::Value, observed_at_ms: u64) -> ConditionReport {
    let Some(word) = stop_failure_error(payload).filter(|word| !word.is_empty()) else {
        // A `StopFailure` with no word at all: the turn provably failed and st2's own driver
        // plumbing is the most conservative truthful owner of an unclassified harness-reported
        // failure. Codeless rather than code-guessed — a minted word would be indistinguishable
        // from one Claude actually published.
        return ConditionReport::Fault(
            FaultReport::new(FaultCategory::Harness, Recovery::Unknown, observed_at_ms)
                .with_detail("stopFailure carried no error word"),
        );
    };
    let (category, recovery) = match word {
        // A rejected credential: one re-login repairs it.
        CLAUDE_AUTH_REJECTED_ERROR => (FaultCategory::Authentication, Recovery::Human),
        // An org entitlement no re-login satisfies — a policy refusal, not a credential.
        "oauth_org_not_allowed" => (FaultCategory::Policy, Recovery::Human),
        // The account itself is the obstacle. Not `quota`: no allowance window is exhausted, and
        // an operator's repair is billing rather than waiting.
        "account_on_hold" | "billing_error" => (FaultCategory::Account, Recovery::Human),
        // Throttled while the allowance itself is intact, with no reset instant published.
        "rate_limit" => (FaultCategory::RateLimit, Recovery::Unknown),
        // The provider failed on its own side.
        "overloaded" | "server_error" => (FaultCategory::Provider, Recovery::Unknown),
        // The request this seat's own declaration produced is wrong: a model it may not use, or
        // arguments the provider rejects. Both need an operator to change the seat.
        "invalid_request" | "model_not_found" => (FaultCategory::Configuration, Recovery::Human),
        // DELIBERATE NO MAPPING. The response hit its own output cap; the turn ended, the seat is
        // healthy, and nothing about the provider is faulted. An explicit arm rather than a
        // default so it can never drift into the unrecognized-word fault below.
        "max_output_tokens" => return ConditionReport::Unchanged,
        // Claude's own `unknown`, and every future word this build has never seen. Still a fault
        // — a turn failed — routed by its recovery and labelled with the most conservative
        // truthful category for a harness-reported failure nobody can classify. Guessing a
        // neighbouring category would invent a claim the harness never made; the verbatim word
        // stays visible in the code.
        _ => (FaultCategory::Harness, Recovery::Unknown),
    };
    ConditionReport::Fault(
        FaultReport::new(category, recovery, observed_at_ms).with_code(format!("claude/{word}")),
    )
}

/// Claude's conversation identity, from the one typed field every hook payload carries.
///
/// `session_id` is the provider's own conversation identity — already load-bearing here, since
/// [`wrapperless_token`] derives a whole ownership namespace from it — and it rides VERBATIM,
/// never the prefixed token, which is st2's ownership namespace rather than Claude's identity.
/// The incarnation is not stated: the writing session stamps it, so a reader can always falsify a
/// mismatch against the record's own.
///
/// `Rewritable`, because Claude compacts: `PreCompact`/`PostCompact` are registered and counted in
/// this same file, so a prefix read once may be gone. `Declared`, because that is pinned knowledge
/// of Claude 2.1.x and st2 never probed the transcript. `transcript_path` rides the same payload
/// and is deliberately not carried: this link is identity and capability only, and a host-local
/// transcript path is content-bearing.
///
/// A payload with no `session_id` states NOTHING — `None` leaves the axis as the record has it.
/// `Unsupported` would be false (Claude plainly has conversations) and a half-stated link is what
/// the reader degrades with its own rejection word.
fn conversation_claim(payload: &serde_json::Value, now_ms: u64) -> Option<ConversationState> {
    let conversation = payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())?;
    // The verification bound must be finite and positive; a zero clock is not a bound, so it
    // withholds the claim rather than writing one this build's own reader would degrade.
    if now_ms == 0 {
        return None;
    }
    Some(ConversationState::Linked(ConversationClaim {
        driver: "claude".to_string(),
        conversation: conversation.to_string(),
        history_mutability: HistoryMutability::Rewritable,
        capability_evidence: CapabilityEvidence::Declared,
        verified_through_ms: now_ms,
    }))
}

/// The `StopFailure` error word that names a rejected provider credential.
///
/// Claude Code classifies every 401/403 provider response as `authentication_failed` (measured on
/// 2.1.259, which also documents the event as "fires instead of Stop when an API error (rate
/// limit, auth failure, etc.) ended the turn"), so this one word IS the credential-rejected class.
/// The siblings in that closed vocabulary are deliberately not it: `rate_limit` and `overloaded`
/// are capacity, `oauth_org_not_allowed` is an org policy no re-login can satisfy,
/// `account_on_hold` and `billing_error` are account state, and `invalid_request`,
/// `model_not_found`, `server_error`, `max_output_tokens` and `unknown` are request or server
/// faults. Naming any of them a credential rejection would hand an operator the wrong repair.
const CLAUDE_AUTH_REJECTED_ERROR: &str = "authentication_failed";

/// The closed `StopFailure` error word, as the payload spells it. `error_details` and
/// `last_assistant_message` ride the same payload and are deliberately untouched: they are prose.
fn stop_failure_error(payload: &serde_json::Value) -> Option<&str> {
    payload.get("error").and_then(serde_json::Value::as_str)
}

/// What one hook event proves about the seat's provider credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderAuthEdge {
    Rejected,
    Accepted,
}

/// Read the credential edge out of one hook event, or `None` when the event proves nothing about
/// it — which must leave a standing rejection alone rather than clearing it.
fn provider_auth_edge(event: &str, payload: &serde_json::Value) -> Option<ProviderAuthEdge> {
    match event {
        "StopFailure" => (stop_failure_error(payload) == Some(CLAUDE_AUTH_REJECTED_ERROR))
            .then_some(ProviderAuthEdge::Rejected),
        // A turn that reached its ordinary end is positive proof the credential was accepted.
        // `SessionStart` is not: a fresh session has made no provider call yet.
        "Stop" => Some(ProviderAuthEdge::Accepted),
        _ => None,
    }
}

/// Record one credential edge on the seat's native-driver diagnostic.
///
/// Each hook invocation is its own short-lived writer, so the publisher's stage set starts empty
/// and its on-disk fallback is what lets a later `Stop` clear a rejection an earlier `StopFailure`
/// wrote from a different process. Fail-open like every other observation here: the publisher only
/// warns on a write it cannot land.
fn publish_provider_auth(agent_dir: &Path, edge: ProviderAuthEdge) {
    let mut publisher = driver_diagnostic::Publisher::new(
        agent_dir,
        driver_diagnostic::Driver::Claude,
        // A hook payload carries no Claude version — the common hook input is session id,
        // transcript path, cwd, prompt id, permission mode, agent identity and effort, and
        // nothing else (2.1.259) — and st2 gates no Claude version, so neither the producer
        // version nor its support status is knowable from here.
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::*;
    use crate::harness_state::harness_state_path;

    #[test]
    fn only_the_packaged_channel_requests_the_installation_preflight() {
        assert!(requires_st2_channel(&[
            "claude".into(),
            "--channels".into(),
            "plugin:st2-channel@st2".into(),
        ]));
        assert!(!requires_st2_channel(&[
            "claude".into(),
            "--channels".into(),
            "plugin:other@marketplace".into(),
        ]));
    }

    #[test]
    fn development_channel_fallback_is_inline_and_keeps_the_provider_arguments() {
        let argv = vec![
            "claude".into(),
            "--model".into(),
            "sonnet".into(),
            "--channels".into(),
            "plugin:st2-channel@st2".into(),
            "prompt".into(),
        ];
        let output = development_channel_argv(
            argv,
            Path::new("/opt/st2/bin/st2"),
            Path::new("/var/lib/st2/catalog"),
            "host.worker",
        )
        .unwrap();
        assert_eq!(&output[..3], &["claude", "--model", "sonnet"]);
        assert_eq!(
            output.last().map(String::as_str),
            Some("prompt"),
            "the user prompt remains last"
        );
        let config_index = output.iter().position(|arg| arg == "--mcp-config").unwrap();
        let mcp: serde_json::Value = serde_json::from_str(&output[config_index + 1]).unwrap();
        assert_eq!(mcp["mcpServers"]["st2"]["command"], "/opt/st2/bin/st2");
        assert_eq!(
            mcp["mcpServers"]["st2"]["args"],
            serde_json::json!([
                "--catalog",
                "/var/lib/st2/catalog",
                "driver",
                "claude-mcp",
                "--id",
                "host.worker"
            ])
        );
        assert!(
            output
                .iter()
                .any(|arg| arg == "--dangerously-load-development-channels=server:st2")
        );
        assert!(!output.iter().any(|arg| arg == "--channels"));
    }

    #[test]
    fn idle_provider_refreshes_presence_without_mcp_input() {
        let tmp = tempfile::tempdir().unwrap();
        let presence = status::status_path(tmp.path());
        status::set_state(&presence, status::State::Available).unwrap();
        let before = fs::read_to_string(&presence).unwrap();
        let stop = AtomicBool::new(false);

        run_provider(
            "Claude",
            &presence,
            &["sh".into(), "-c".into(), "sleep 0.12".into()],
            &[],
            Duration::from_millis(25),
            Duration::from_millis(5),
            &stop,
            None,
        )
        .unwrap();

        let after = fs::read_to_string(&presence).unwrap();
        assert_ne!(after, before);
        assert_eq!(status::read_state(&presence), status::State::Available);
    }

    #[test]
    fn hook_events_map_to_the_typed_tuple_with_the_blocked_edges() {
        let none = serde_json::Value::Null;

        let blocked = observe_hook_event("PermissionRequest", &none).unwrap();
        assert_eq!(blocked.state, Activity::Active);
        assert_eq!(blocked.ask, HookAsk::Permission);
        assert_eq!(blocked.ask.legacy(), (BlockedOn::Human, Ask::Permission));
        assert_eq!(blocked.reason, Some("permissionRequest"));

        // The exit edges: tool progress or a turn boundary clears the human hold.
        for event in ["PreToolUse", "PostToolUse"] {
            let cleared = observe_hook_event(event, &none).unwrap();
            assert_eq!(cleared.state, Activity::Active);
            assert_eq!(cleared.ask, HookAsk::None);
            assert_eq!(cleared.ask.legacy(), (BlockedOn::None, Ask::None));
        }
        let stop = observe_hook_event("Stop", &none).unwrap();
        assert_eq!(stop.state, Activity::Idle);
        assert_eq!(stop.ask, HookAsk::None);

        assert_eq!(
            observe_hook_event("UserPromptSubmit", &none).unwrap().state,
            Activity::Active
        );
        assert_eq!(
            observe_hook_event("SessionStart", &none).unwrap().state,
            Activity::Idle
        );

        // Unmapped events say nothing rather than guessing.
        assert_eq!(observe_hook_event("Notification", &none), None);
        assert_eq!(observe_hook_event("SubagentStop", &none), None);
    }

    /// The measured `StopFailure` payload shape (Claude Code 2.1.259: `hook_event_name`, the
    /// closed `error` word, optional `error_details` and `last_assistant_message`). It fires
    /// INSTEAD of `Stop`, so the turn is over whatever the word is — but only the
    /// credential class earns the `providerAuth` reason, and neither quota nor an org policy may
    /// borrow it: a re-login fixes exactly one of the three.
    #[test]
    fn stop_failure_classifies_only_the_credential_class_as_provider_auth() {
        let rejected = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "authentication_failed",
            "error_details": "Please run /login",
            "last_assistant_message": "",
        });
        let rate_limited = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "rate_limit",
        });
        let org_policy = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "oauth_org_not_allowed",
        });

        for (payload, reason) in [
            (&rejected, "providerAuth"),
            (&rate_limited, "apiError"),
            (&org_policy, "apiError"),
        ] {
            let observed = observe_hook_event("StopFailure", payload).unwrap();
            assert_eq!(observed.state, Activity::Idle, "the turn ended: {reason}");
            assert_eq!(observed.ask, HookAsk::None, "a fault is never an ask");
            assert_eq!(observed.reason, Some(reason));
        }

        assert_eq!(
            provider_auth_edge("StopFailure", &rejected),
            Some(ProviderAuthEdge::Rejected)
        );
        assert_eq!(
            provider_auth_edge("StopFailure", &rate_limited),
            None,
            "an exhausted allowance is not a rejected credential"
        );
        assert_eq!(
            provider_auth_edge("StopFailure", &org_policy),
            None,
            "an org policy no re-login can satisfy is not a rejected credential"
        );
        assert_eq!(
            provider_auth_edge("Stop", &serde_json::Value::Null),
            Some(ProviderAuthEdge::Accepted),
            "a turn that reached its ordinary end proves the credential worked"
        );
        assert_eq!(
            provider_auth_edge("SessionStart", &serde_json::Value::Null),
            None,
            "a fresh session has made no provider call to prove anything with"
        );
    }

    /// Each hook invocation is its own process, so the record must survive between them and the
    /// recovery edge must reach a failure a different process published.
    #[test]
    fn a_rejected_claude_credential_stands_until_a_turn_reaches_its_ordinary_end() {
        let tmp = tempfile::tempdir().unwrap();
        let record = driver_diagnostic::path(tmp.path());
        let rejected = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "authentication_failed",
        });

        publish_provider_auth(
            tmp.path(),
            provider_auth_edge("StopFailure", &rejected).unwrap(),
        );
        let driver_diagnostic::Observed::Failure(failure) = driver_diagnostic::read(&record) else {
            panic!("a rejected credential must publish a native-driver diagnostic")
        };
        assert_eq!(failure.driver, driver_diagnostic::Driver::Claude);
        assert_eq!(failure.stage, driver_diagnostic::Stage::ProviderAuth);
        assert_eq!(
            failure.reason,
            driver_diagnostic::Reason::ProviderAuthRejected
        );
        assert_eq!(failure.source, driver_diagnostic::Source::TurnResult);
        assert_eq!(
            failure.producer_version, None,
            "a hook payload names no Claude version"
        );
        assert_eq!(
            failure.support,
            driver_diagnostic::Support::Unknown,
            "st2 gates no Claude version, so support is not knowable from a hook"
        );

        // A later quota failure carries no credential edge, so the rejection stands.
        let rate_limited = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "rate_limit",
        });
        assert_eq!(provider_auth_edge("StopFailure", &rate_limited), None);
        assert!(matches!(
            driver_diagnostic::read(&record),
            driver_diagnostic::Observed::Failure(_)
        ));

        publish_provider_auth(tmp.path(), ProviderAuthEdge::Accepted);
        assert_eq!(
            driver_diagnostic::read(&record),
            driver_diagnostic::Observed::Absent,
            "the next ordinary turn end clears a rejection a sibling hook process published"
        );
    }

    #[test]
    fn subagent_events_never_move_top_level_state() {
        let subagent = serde_json::json!({"agent_id": "sub-1", "agent_type": ""});
        for event in [
            "Stop",
            "UserPromptSubmit",
            "PermissionRequest",
            "PostToolUse",
        ] {
            assert_eq!(observe_hook_event(event, &subagent), None, "{event}");
        }
        // An empty agent_id is the top-level shape.
        let top = serde_json::json!({"agent_id": ""});
        assert!(observe_hook_event("Stop", &top).is_some());
    }

    /// The measured grant-path sequence from the DQ-H1 capture (2026-08-23, Claude Code 2.1.237,
    /// `docs/vrs/05-harness-state/.experiments/2026-08-23-claude-batched-permission.md`): in a
    /// two-call batch where the first call needs permission, execution serializes around the open
    /// prompt, so the event after `PermissionRequest` is the granted call's own `PostToolUse` and
    /// the exit rule clears `blocked` at exactly the right moment. Replayed verbatim so a future
    /// mapping change that breaks the measured sequence fails here, not in the field.
    #[test]
    fn measured_batched_grant_sequence_holds_blocked_until_the_granted_calls_own_post() {
        let pre_touch = serde_json::json!({
            "hook_event_name": "PreToolUse", "tool_name": "Bash",
            "tool_input": {"command": "touch scratch2.txt"},
            "tool_use_id": "toolu_01HK5aLKavjdbrCk48cfd58k",
            "prompt_id": "0ea832de-ece1-4575-900f-4dab5e2f6849",
        });
        let permission_request = serde_json::json!({
            "hook_event_name": "PermissionRequest", "tool_name": "Bash",
            "tool_input": {"command": "touch scratch2.txt"},
            "permission_suggestions": [],
            "prompt_id": "0ea832de-ece1-4575-900f-4dab5e2f6849",
        });
        let post_touch = serde_json::json!({
            "hook_event_name": "PostToolUse", "tool_name": "Bash",
            "tool_input": {"command": "touch scratch2.txt"},
            "tool_use_id": "toolu_01HK5aLkavjdbrCk48cfd58k",
            "prompt_id": "0ea832de-ece1-4575-900f-4dab5e2f6849",
        });
        // The phantom SubagentStop trailing the turn: non-empty agent_id, EMPTY agent_type, no
        // subagent ran — the exact emergent shape the guard keys on, reproduced in this build.
        let phantom_subagent_stop = serde_json::json!({
            "hook_event_name": "SubagentStop",
            "agent_id": "a5c61ec4ef268c3cc", "agent_type": "",
            "prompt_id": "0ea832de-ece1-4575-900f-4dab5e2f6849",
        });

        let entered = observe_hook_event("PermissionRequest", &permission_request).unwrap();
        assert_eq!(
            (entered.state, entered.ask.legacy()),
            (Activity::Active, (BlockedOn::Human, Ask::Permission))
        );
        // 33 s of open prompt produced no intervening event in the capture; the very next event
        // is the granted call's own PostToolUse, which correctly releases the block.
        let released = observe_hook_event("PostToolUse", &post_touch).unwrap();
        assert_eq!(
            (released.state, released.ask.legacy()),
            (Activity::Active, (BlockedOn::None, Ask::None))
        );
        let _ = observe_hook_event("PreToolUse", &pre_touch);
        assert_eq!(
            observe_hook_event("SubagentStop", &phantom_subagent_stop),
            None,
            "the phantom SubagentStop must not resurrect activity after Stop"
        );
    }

    #[test]
    fn wrapper_heartbeat_re_stamps_without_clobbering_hook_written_state() {
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let observer =
            SessionObserver::new(tmp.path(), "hetz.worker", "claude", "hetz.worker").unwrap();

        // A hook process wrote a blocked observation between wrapper ticks — carrying the
        // wrapper's exported token, exactly as the env plumbing arranges in a real seat.
        harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        )
        .with_session(observer.session())
        .observe(
            observe_hook_event("PermissionRequest", &serde_json::Value::Null)
                .unwrap()
                .observation(),
        )
        .unwrap();
        let before = fs::read(&record).unwrap();

        std::thread::sleep(Duration::from_millis(2));
        observer.heartbeat();
        let after = fs::read(&record).unwrap();
        assert_ne!(before, after, "heartbeat must re-stamp bytes");
        let observed = harness_state::read(&record, None).unwrap();
        assert_eq!(observed.state, Activity::Active);
        assert_eq!(observed.blocked_on, BlockedOn::Human);
        assert_eq!(observed.reason.as_deref(), Some("permissionRequest"));
    }

    #[test]
    fn a_provider_killed_mid_turn_reads_ended_rather_than_active() {
        let tmp = tempfile::tempdir().unwrap();
        let presence = status::status_path(tmp.path());
        let record = harness_state_path(tmp.path());
        let observer =
            SessionObserver::new(tmp.path(), "hetz.worker", "claude", "hetz.worker").unwrap();
        let stop = AtomicBool::new(false);

        // A turn is in flight when the provider dies by signal.
        harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        )
        .observe(
            observe_hook_event("UserPromptSubmit", &serde_json::Value::Null)
                .unwrap()
                .observation(),
        )
        .unwrap();

        let result = run_provider(
            "Claude",
            &presence,
            &["sh".into(), "-c".into(), "kill -9 $$".into()],
            &[],
            Duration::from_millis(25),
            Duration::from_millis(5),
            &stop,
            Some(&observer),
        );
        assert!(result.is_err(), "a signalled provider is a failed run");

        let observed = harness_state::read(&record, None).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("signal 9"));
    }

    #[test]
    fn a_clean_provider_exit_writes_the_terminal_record() {
        let tmp = tempfile::tempdir().unwrap();
        let presence = status::status_path(tmp.path());
        let observer =
            SessionObserver::new(tmp.path(), "hetz.worker", "claude", "hetz.worker").unwrap();
        let stop = AtomicBool::new(false);

        run_provider(
            "Claude",
            &presence,
            &["true".into()],
            &[],
            Duration::from_millis(25),
            Duration::from_millis(5),
            &stop,
            Some(&observer),
        )
        .unwrap();

        let observed = harness_state::read(&harness_state_path(tmp.path()), None).unwrap();
        assert_eq!(observed.state, Activity::Ended);
        assert_eq!(observed.exit.as_deref(), Some("exit 0"));
    }

    #[test]
    fn permission_requests_classify_their_ask_kind_from_the_tool_name() {
        let permission = observe_hook_event(
            "PermissionRequest",
            &serde_json::json!({ "tool_name": "Bash", "tool_input": {} }),
        )
        .unwrap();
        assert_eq!(permission.ask, HookAsk::Permission);
        assert_eq!(
            permission.ask.tagged(),
            HumanAsk::Pending(AskKind::Permission)
        );
        assert_eq!(permission.ask.legacy(), (BlockedOn::Human, Ask::Permission));

        let question = observe_hook_event(
            "PermissionRequest",
            &serde_json::json!({ "tool_name": "AskUserQuestion", "tool_input": {} }),
        )
        .unwrap();
        assert_eq!(question.ask, HookAsk::Question);
        assert_eq!(question.ask.tagged(), HumanAsk::Pending(AskKind::Question));
        assert_eq!(question.ask.legacy(), (BlockedOn::Human, Ask::Question));

        // Claude has no review signal at all, so no mapping may produce one.
        assert_ne!(question.ask.tagged(), HumanAsk::Pending(AskKind::Review));

        // Non-blocking events carry no ask.
        let idle = observe_hook_event("Stop", &serde_json::json!({})).unwrap();
        assert_eq!(idle.ask, HookAsk::None);
        assert_eq!(idle.ask.tagged(), HumanAsk::None);
        assert_eq!(idle.ask.legacy(), (BlockedOn::None, Ask::None));
    }

    /// A deterministic clock, so the mapping is asserted as the pure function it is.
    const OBSERVED_AT_MS: u64 = 1_764_000_000_000;

    /// The closed `StopFailure` vocabulary (Claude Code 2.1.259), pinned word by word: ten words
    /// that state a condition and one — `max_output_tokens` — that deliberately states none. A
    /// `_ =>` default over this table would publish a page per truncated response, which is why
    /// every word has its own arm and why this test enumerates all eleven rather than sampling.
    #[test]
    fn every_stop_failure_word_maps_to_its_own_fault_or_to_no_mapping_at_all() {
        let table: [(&str, Option<(FaultCategory, &str, Recovery)>); 11] = [
            (
                "authentication_failed",
                Some((
                    FaultCategory::Authentication,
                    "claude/authentication_failed",
                    Recovery::Human,
                )),
            ),
            (
                "oauth_org_not_allowed",
                Some((
                    FaultCategory::Policy,
                    "claude/oauth_org_not_allowed",
                    Recovery::Human,
                )),
            ),
            (
                "account_on_hold",
                Some((
                    FaultCategory::Account,
                    "claude/account_on_hold",
                    Recovery::Human,
                )),
            ),
            (
                "billing_error",
                Some((
                    FaultCategory::Account,
                    "claude/billing_error",
                    Recovery::Human,
                )),
            ),
            (
                "rate_limit",
                Some((
                    FaultCategory::RateLimit,
                    "claude/rate_limit",
                    Recovery::Unknown,
                )),
            ),
            (
                "overloaded",
                Some((
                    FaultCategory::Provider,
                    "claude/overloaded",
                    Recovery::Unknown,
                )),
            ),
            (
                "server_error",
                Some((
                    FaultCategory::Provider,
                    "claude/server_error",
                    Recovery::Unknown,
                )),
            ),
            (
                "invalid_request",
                Some((
                    FaultCategory::Configuration,
                    "claude/invalid_request",
                    Recovery::Human,
                )),
            ),
            (
                "model_not_found",
                Some((
                    FaultCategory::Configuration,
                    "claude/model_not_found",
                    Recovery::Human,
                )),
            ),
            // The deliberate null: a response that hit its own output cap says nothing about the
            // seat's health, so the condition axis is carried untouched.
            ("max_output_tokens", None),
            (
                "unknown",
                Some((FaultCategory::Harness, "claude/unknown", Recovery::Unknown)),
            ),
        ];

        for (word, want) in table {
            let payload = serde_json::json!({"hook_event_name": "StopFailure", "error": word});
            let condition = stop_failure_condition(&payload, OBSERVED_AT_MS);
            let Some((category, code, recovery)) = want else {
                assert_eq!(
                    condition,
                    ConditionReport::Unchanged,
                    "{word} maps to nothing at all"
                );
                continue;
            };
            let ConditionReport::Fault(fault) = condition else {
                panic!("{word} states a fault")
            };
            assert_eq!(fault.category, category, "{word}");
            assert_eq!(fault.code.as_deref(), Some(code), "{word}");
            assert_eq!(fault.recovery, recovery, "{word}");
            // The SEMANTIC instant, from the hook's own clock.
            assert_eq!(fault.observed_at_ms, OBSERVED_AT_MS, "{word}");
            assert_eq!(
                fault.detail, None,
                "`error_details` is prose and never rides the fault: {word}"
            );
        }

        // A word this build has never seen is still a fault — visible, routed by its recovery,
        // and never guessed into a neighbouring category — carrying the verbatim word.
        let ConditionReport::Fault(future) = stop_failure_condition(
            &serde_json::json!({"error": "quantum_flux"}),
            OBSERVED_AT_MS,
        ) else {
            panic!("an unrecognized word is still a failed turn")
        };
        assert_eq!(future.category, FaultCategory::Harness);
        assert_eq!(future.code.as_deref(), Some("claude/quantum_flux"));
        assert_eq!(future.recovery, Recovery::Unknown);

        // Presence-tested, not defaulted: a `StopFailure` carrying no word at all is codeless
        // rather than code-guessed, because a minted word would be indistinguishable from one
        // Claude published.
        for payload in [
            serde_json::json!({"hook_event_name": "StopFailure"}),
            serde_json::json!({"error": ""}),
        ] {
            let ConditionReport::Fault(wordless) = stop_failure_condition(&payload, OBSERVED_AT_MS)
            else {
                panic!("a wordless StopFailure still failed the turn: {payload}")
            };
            assert_eq!(wordless.category, FaultCategory::Harness);
            assert_eq!(wordless.code, None);
            assert_eq!(wordless.recovery, Recovery::Unknown);
        }
    }

    /// The anti-laundering guard. `Recovery::Automatic` is projected as recovering/soon and NEVER
    /// pages while its deadline holds, so an automatic Claude fault would keep a wedged seat
    /// quiet indefinitely — and Claude has no retry visibility and publishes no reset instant to
    /// time one with. No row may declare it, and no row may set a deadline.
    #[test]
    fn claude_never_declares_a_recovery_it_cannot_time() {
        for word in [
            "authentication_failed",
            "oauth_org_not_allowed",
            "account_on_hold",
            "billing_error",
            "rate_limit",
            "overloaded",
            "server_error",
            "invalid_request",
            "model_not_found",
            "max_output_tokens",
            "unknown",
            "a_word_from_a_later_build",
            "",
        ] {
            let payload = serde_json::json!({"error": word});
            let ConditionReport::Fault(fault) = stop_failure_condition(&payload, OBSERVED_AT_MS)
            else {
                continue;
            };
            assert_ne!(fault.recovery, Recovery::Automatic, "{word}");
            assert_ne!(
                fault.recovery,
                Recovery::Terminal,
                "a re-login or a wait can still clear it: {word}"
            );
            assert_eq!(fault.next_observation_due_ms, None, "{word}");
        }
    }

    /// `StopFailure` is idle PLUS a fault: the TUI is live and only the wrapper's process exit
    /// writes the terminal word. It synthesizes no ask either — the remediation is on the
    /// condition axis, and a pending ask would fabricate a prompt nobody can answer.
    #[test]
    fn a_stop_failure_is_idle_with_a_fault_and_never_ended_or_an_ask() {
        let payload = serde_json::json!({
            "hook_event_name": "StopFailure",
            "error": "rate_limit",
            "session_id": "s-1",
            "error_details": "please try again later",
        });
        let observed = observe_hook_event_at("StopFailure", &payload, OBSERVED_AT_MS).unwrap();
        assert_eq!(observed.state, Activity::Idle);
        assert_ne!(
            observed.state,
            Activity::Ended,
            "the wrapper's exit is the sole terminal writer"
        );
        assert_eq!(observed.ask, HookAsk::None);
        let ConditionReport::Fault(fault) = &observed.condition else {
            panic!("the failure rides the condition axis")
        };
        assert_eq!(fault.category, FaultCategory::RateLimit);
        assert_eq!(fault.observed_at_ms, OBSERVED_AT_MS);

        // The version 3 statement: one frame, no exit, the ask still positively none.
        let frame = observed.frame();
        assert_eq!(frame.state, Activity::Idle);
        assert_eq!(frame.ask, HumanAsk::None);
        assert_eq!(frame.exit, None, "a fault never carries a terminal outcome");
        assert_eq!(frame.condition, observed.condition);
        assert_eq!(frame.reason.as_deref(), Some("apiError"));

        // The truncation word is the one `StopFailure` that raises nothing at all, while still
        // reporting the turn's end exactly as it always did.
        let truncated = observe_hook_event_at(
            "StopFailure",
            &serde_json::json!({"error": "max_output_tokens"}),
            OBSERVED_AT_MS,
        )
        .unwrap();
        assert_eq!(truncated.state, Activity::Idle);
        assert_eq!(truncated.condition, ConditionReport::Unchanged);
        assert_eq!(truncated.reason, Some("apiError"));
    }

    /// The clear edges, and the two `SessionStart` sources that look like boundaries but are not.
    #[test]
    fn a_completed_turn_and_a_fresh_incarnation_are_the_only_edges_that_clear() {
        let session = serde_json::json!({"session_id": "s-1"});
        let stop = observe_hook_event_at("Stop", &session, OBSERVED_AT_MS).unwrap();
        assert_eq!(
            (stop.state, stop.condition.clone()),
            (Activity::Idle, ConditionReport::Clear),
            "a completed turn is progress a standing fault would have prevented"
        );

        // Genuine process boundaries: a new incarnation supersedes whatever the last one held.
        for source in ["startup", "resume"] {
            let mut payload = session.clone();
            payload["source"] = source.into();
            let boundary = observe_hook_event_at("SessionStart", &payload, OBSERVED_AT_MS).unwrap();
            assert_eq!(boundary.condition, ConditionReport::Clear, "{source}");
            assert_eq!(boundary.state, Activity::Idle, "{source}");
        }
        // A `SessionStart` with no source word is treated as one, because that is what the event
        // otherwise means.
        assert_eq!(
            observe_hook_event_at("SessionStart", &session, OBSERVED_AT_MS)
                .unwrap()
                .condition,
            ConditionReport::Clear
        );

        // The MID-PROCESS sources. `compact` is the same session seeing one compaction a third
        // time (see `observe_compaction`); `clear` empties the conversation inside the running
        // process — same pty, same wrapper, same incarnation. A fault the provider still holds
        // survives both, so clearing on either would silence it on every automatic compaction and
        // every `/clear`.
        for source in ["compact", "clear"] {
            let mut payload = session.clone();
            payload["source"] = source.into();
            let mid_process =
                observe_hook_event_at("SessionStart", &payload, OBSERVED_AT_MS).unwrap();
            assert_eq!(
                mid_process.condition,
                ConditionReport::Unchanged,
                "{source}"
            );
            assert_eq!(mid_process.state, Activity::Idle, "{source}");
            assert_eq!(mid_process.reason, Some("sessionStart"), "{source}");
        }
    }

    /// An activity or ask edge has learned NOTHING about whether the provider is faulted. Carrying
    /// the axis is also what preserves a standing fault's semantic clock: a stated `clear` here
    /// would silence it, and a restated fault would re-mint `observedAtMs` on every tool call.
    #[test]
    fn an_activity_edge_never_clears_a_standing_condition() {
        let session = serde_json::json!({"session_id": "s-1"});
        for event in [
            "UserPromptSubmit",
            "PreToolUse",
            "PostToolUse",
            "PermissionRequest",
        ] {
            let observed = observe_hook_event_at(event, &session, OBSERVED_AT_MS).unwrap();
            assert_eq!(observed.state, Activity::Active, "{event}");
            assert_eq!(observed.condition, ConditionReport::Unchanged, "{event}");
            assert_eq!(
                observed.frame().condition,
                ConditionReport::Unchanged,
                "the carry must reach the write, not just the mapping: {event}"
            );
        }

        // A repeatable edge is CLOCK-INDEPENDENT: nothing it states may be minted from `now`, or
        // two identical observations would be two different tuples. The conversation link is the
        // one field that could be, so these edges state none and the standing link carries.
        for event in [
            "UserPromptSubmit",
            "PreToolUse",
            "PostToolUse",
            "PermissionRequest",
        ] {
            let first = observe_hook_event_at(event, &session, OBSERVED_AT_MS).unwrap();
            let later = observe_hook_event_at(event, &session, OBSERVED_AT_MS + 90_000).unwrap();
            assert_eq!(first.conversation, None, "{event}");
            assert_eq!(
                first, later,
                "a repeat must restate exactly the same tuple: {event}"
            );
            assert_eq!(
                first.frame(),
                later.frame(),
                "identical frames are what makes the repeat coalesce: {event}"
            );
        }

        // The measured DQ-H1 denial residual on the tagged axis: "No" ends the turn with no
        // further event, so the ask stands until the next prompt, which is the only edge that
        // releases it — and that release still touches no condition.
        let denied = observe_hook_event_at(
            "PermissionRequest",
            &serde_json::json!({"session_id": "s-1", "tool_name": "Bash"}),
            OBSERVED_AT_MS,
        )
        .unwrap();
        assert_eq!(denied.ask.tagged(), HumanAsk::Pending(AskKind::Permission));
        let next_prompt =
            observe_hook_event_at("UserPromptSubmit", &session, OBSERVED_AT_MS).unwrap();
        assert_eq!(next_prompt.ask, HookAsk::None);
        assert_eq!(next_prompt.condition, ConditionReport::Unchanged);
    }

    /// The write-rate consequence of the rule above, on the path this build emits: a repeated
    /// activity edge must not touch the record at all. If any axis were minted from `now`, the
    /// second write would land, `transitions` would advance, and `sinceMs` — when the state was
    /// ENTERED — would restart on a state the seat never left, on every tool call of every turn.
    #[test]
    fn a_repeated_activity_edge_neither_writes_nor_restarts_since() {
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let payload = serde_json::json!({"session_id": "s-1"});
        let mut writer = harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        );
        let publish = |writer: &mut harness_state::Writer| {
            writer
                .observe_unless_ended(
                    observe_hook_event("PostToolUse", &payload)
                        .unwrap()
                        .observation(),
                )
                .unwrap()
        };

        assert!(publish(&mut writer));
        let first = fs::read(&record).unwrap();
        let since_ms = harness_state::read(&record, None).unwrap().since_ms;

        std::thread::sleep(Duration::from_millis(2));
        assert!(publish(&mut writer), "the record still says this");
        assert_eq!(
            fs::read(&record).unwrap(),
            first,
            "a restated activity edge coalesces: nothing was written"
        );
        assert_eq!(
            harness_state::read(&record, None).unwrap().since_ms,
            since_ms,
            "`sinceMs` marks when the state was entered and must not restart"
        );
    }

    /// The conversation bridge, from the one typed field every hook payload carries.
    #[test]
    fn hook_states_link_claudes_session_id_with_bounded_declared_evidence() {
        let payload = serde_json::json!({
            "session_id": "abc-123",
            "transcript_path": "/home/x/.claude/projects/abc-123.jsonl",
        });
        let observed = observe_hook_event_at("Stop", &payload, OBSERVED_AT_MS).unwrap();
        let Some(ConversationState::Linked(link)) = observed.conversation.clone() else {
            panic!("Claude's session id is typed evidence of its own conversation")
        };
        assert_eq!(link.driver, "claude");
        assert_eq!(
            link.conversation, "abc-123",
            "the provider's identity rides verbatim"
        );
        assert_ne!(
            Some(link.conversation.clone()),
            wrapperless_token(&payload),
            "the prefixed token is st2's ownership namespace, not Claude's identity"
        );
        // Claude compacts — this file counts the edges — so a prefix read once may be gone.
        assert_eq!(link.history_mutability, HistoryMutability::Rewritable);
        // Pinned knowledge of 2.1.x; st2 never probed the transcript.
        assert_eq!(link.capability_evidence, CapabilityEvidence::Declared);
        // Finite and positive: a consumer ages the claim instead of trusting it forever.
        assert_eq!(link.verified_through_ms, OBSERVED_AT_MS);
        // Identity and capability ONLY: the transcript path rides the same payload and is
        // content-bearing, so it must not reach a replicated record.
        assert!(!format!("{link:?}").contains("transcript"), "{link:?}");
        assert_eq!(observed.frame().conversation, observed.conversation);

        // Nothing to state is stated as nothing: `Unsupported` would be false (Claude plainly
        // has conversations) and a half-stated link is what the reader degrades.
        for silent in [serde_json::json!({}), serde_json::json!({"session_id": ""})] {
            assert_eq!(
                observe_hook_event_at("Stop", &silent, OBSERVED_AT_MS)
                    .unwrap()
                    .conversation,
                None,
                "{silent}"
            );
        }
        // A zero clock is not a finite verification bound, so the claim is withheld rather than
        // written in a shape this build's own reader rejects.
        assert_eq!(
            observe_hook_event_at("Stop", &payload, 0)
                .unwrap()
                .conversation,
            None
        );
    }

    /// Source compatibility: while the writer does not emit the condition axis, every Claude hook
    /// writes exactly the bytes it always did. The condition and conversation axes are DROPPED
    /// rather than half-written or stashed in a sidecar, and their absence reads as `absent` —
    /// never as health.
    #[test]
    fn the_legacy_wire_is_unchanged_while_the_writer_omits_the_condition_axis() {
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let payload = serde_json::json!({"session_id": "s-1", "error": "rate_limit"});
        let mut writer = harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        );
        assert!(
            !writer.writes_condition_axis(),
            "this build's production writer is still on version 2"
        );
        assert!(
            writer
                .observe_unless_ended(
                    observe_hook_event("StopFailure", &payload)
                        .unwrap()
                        .observation()
                )
                .unwrap()
        );

        let raw = fs::read_to_string(&record).unwrap();
        for key in ["condition", "conversationRef", "humanAsk"] {
            assert!(
                !raw.contains(&format!("\"{key}\"")),
                "{key} must not reach the version 2 wire: {raw}"
            );
        }
        let observed = harness_state::read(&record, None).unwrap();
        assert_eq!(observed.state, Activity::Idle);
        assert_eq!(observed.blocked_on, BlockedOn::None);
        assert_eq!(observed.ask, Ask::None);
        assert_eq!(observed.reason.as_deref(), Some("apiError"));
        assert_eq!(
            observed.condition,
            harness_state::ConditionView::Absent,
            "a record that never spoke about health is not healthy"
        );
    }

    /// Claude's unobservable edges stay declared-unsupported: no mapping exists for an interrupt,
    /// for `SessionEnd`, or for `Notification` (the only `auth_success` signal, unregistered —
    /// which is why Claude has no paired clear). Pinned so a future build that starts emitting
    /// them fails loudly here instead of silently doing nothing in the field.
    #[test]
    fn claudes_unobservable_edges_stay_declared() {
        let session = serde_json::json!({"session_id": "s-1"});
        for event in [
            "Notification",
            "SessionEnd",
            "SubagentStart",
            "SubagentStop",
            "PermissionDenied",
            "Interrupt",
            // The compaction edges are harness-CONTEXT only and must never touch this axis.
            "PreCompact",
            "PostCompact",
        ] {
            assert_eq!(observe_hook_event(event, &session), None, "{event}");
        }
    }

    /// A subagent's event must not raise, clear, or move anything — on either axis.
    #[test]
    fn subagent_events_never_raise_or_clear_a_condition() {
        for (event, error) in [("StopFailure", "authentication_failed"), ("Stop", "")] {
            let payload = serde_json::json!({
                "session_id": "s-1",
                "agent_id": "sub-1",
                "agent_type": "",
                "error": error,
            });
            assert_eq!(observe_hook_event(event, &payload), None, "{event}");
        }
    }

    /// T2: a hook that finishes after the wrapper reaped Claude must not replace the terminal
    /// record — the wrapper's `ended` carries the shared token and is the session's last word —
    /// while a NEW session's boundary event still supersedes an old terminal record.
    #[test]
    fn a_late_hook_never_overwrites_this_sessions_terminal_record() {
        use crate::harness_state::{self, Activity};
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let observer =
            SessionObserver::new(tmp.path(), "hetz.worker", "claude", "hetz.worker").unwrap();
        observer.ended("exit 0");

        // The straggler hook shares the session token (env plumbing) and is suppressed.
        let mut late = harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        )
        .with_session(observer.session());
        assert!(
            !late
                .observe_unless_ended(
                    observe_hook_event("PostToolUse", &serde_json::Value::Null)
                        .unwrap()
                        .observation()
                )
                .unwrap()
        );
        assert_eq!(
            harness_state::read(&record, None).unwrap().state,
            Activity::Ended
        );

        // A wrapperless fresh session (token-only, the session_id fallback) cannot take over a
        // claimed record: only a written claim supersedes.
        let mut fallback = harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        )
        .with_session("claude-session-fresh");
        fallback.interrupt();
        assert!(
            !fallback
                .observe_unless_ended(
                    observe_hook_event("SessionStart", &serde_json::Value::Null)
                        .unwrap()
                        .observation()
                )
                .unwrap()
        );
        assert_eq!(
            harness_state::read(&record, None).unwrap().state,
            Activity::Ended
        );

        // A claimed new session — the wrapper path — supersedes the old terminal record.
        let next =
            SessionObserver::new(tmp.path(), "hetz.worker", "claude", "hetz.worker").unwrap();
        let mut fresh = harness_state::Writer::new(
            tmp.path(),
            "hetz.worker",
            "claude",
            Some("hetz.worker".to_string()),
        )
        .with_ownership(next.session().to_string(), next.seq());
        assert!(
            fresh
                .observe_unless_ended(
                    observe_hook_event("SessionStart", &serde_json::Value::Null)
                        .unwrap()
                        .observation()
                )
                .unwrap()
        );
        assert_eq!(
            harness_state::read(&record, None).unwrap().state,
            Activity::Idle
        );
    }

    /// W8-12: two sessions of a WRAPPERLESS seat. Session A's hooks write; session B's
    /// SessionStart performs the written claim and takes over; A's straggler is refused and B's
    /// later hooks adopt B's records.
    #[test]
    fn a_wrapperless_seat_survives_its_own_session_succession() {
        use crate::harness_state::{self, Activity};
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let payload_a = serde_json::json!({ "session_id": "aaa" });
        let payload_b = serde_json::json!({ "session_id": "bbb" });
        let drive = |event: &str, payload: &serde_json::Value| {
            let mut writer =
                observe_writer(tmp.path(), "hetz.worker", None, event, payload, None, None);
            writer
                .observe_unless_ended(observe_hook_event(event, payload).unwrap().observation())
                .unwrap()
        };

        assert!(drive("SessionStart", &payload_a), "A claims");
        assert!(drive("UserPromptSubmit", &payload_a), "A's hooks adopt");
        assert_eq!(
            harness_state::read(&record, None).unwrap().state,
            Activity::Active
        );

        assert!(drive("SessionStart", &payload_b), "B claims over A");
        assert!(
            !drive("PostToolUse", &payload_a),
            "A's straggler is refused"
        );
        assert!(drive("UserPromptSubmit", &payload_b), "B's hooks adopt");
        assert_eq!(
            harness_state::read(&record, None).unwrap().state,
            Activity::Active
        );
    }

    /// The wrapperless session boundary, decided in one predicate.
    ///
    /// The version 3 half is the load-bearing one: a claim writes this session's FENCE, which is
    /// excluded from what an unstated axis inherits, so claiming on a mid-process `SessionStart`
    /// would drop a standing fault by OWNERSHIP — the same laundering the mapping refuses to do
    /// with `clear`, arriving through the other door. Pinned as a pure function of
    /// `(writes_condition_axis, event, payload)` because this build's writer emits version 2 and
    /// cannot exercise the other arm end-to-end; the writer-activation change owns that proof.
    #[test]
    fn a_mid_process_session_start_claims_only_while_the_condition_axis_is_unwritable() {
        for source in [
            None,
            Some("startup"),
            Some("resume"),
            Some("compact"),
            Some("clear"),
        ] {
            let mut payload = serde_json::json!({"session_id": "aaa"});
            if let Some(source) = source {
                payload["source"] = source.into();
            }
            // The legacy wire: every `SessionStart` is the boundary and claims, unchanged.
            assert!(
                claims_wrapperless_session(false, "SessionStart", &payload),
                "{source:?}"
            );
            // With the condition axis on the wire, only a genuinely fresh incarnation claims.
            let fresh = !matches!(source, Some("compact") | Some("clear"));
            assert_eq!(
                claims_wrapperless_session(true, "SessionStart", &payload),
                fresh,
                "{source:?}"
            );
        }

        // No other event is a session boundary on either wire: token-only writers never claim.
        let payload = serde_json::json!({"session_id": "aaa"});
        for event in [
            "Stop",
            "StopFailure",
            "UserPromptSubmit",
            "PreToolUse",
            "PostToolUse",
            "PermissionRequest",
            "PreCompact",
        ] {
            assert!(
                !claims_wrapperless_session(false, event, &payload),
                "{event}"
            );
            assert!(
                !claims_wrapperless_session(true, event, &payload),
                "{event}"
            );
        }
    }

    /// The legacy path is byte- and sequence-exact: on the wire this build emits, a mid-process
    /// `SessionStart` claims exactly as a startup one does. Proved by parity rather than by a
    /// hardcoded sequence number, so the claim's own arithmetic stays owned by `harness_state`.
    #[test]
    fn a_wrapperless_mid_process_session_start_still_claims_on_the_legacy_wire() {
        let succeed = |source: &str| {
            let tmp = tempfile::tempdir().unwrap();
            let record = harness_state_path(tmp.path());
            let predecessor = serde_json::json!({"session_id": "aaa"});
            let mut successor = serde_json::json!({"session_id": "bbb"});
            successor["source"] = source.into();
            let drive = |event: &str, payload: &serde_json::Value| {
                let mut writer =
                    observe_writer(tmp.path(), "hetz.worker", None, event, payload, None, None);
                writer
                    .observe_unless_ended(observe_hook_event(event, payload).unwrap().observation())
                    .unwrap()
            };
            assert!(drive("SessionStart", &predecessor), "{source}");
            assert!(drive("UserPromptSubmit", &predecessor), "{source}");
            assert!(
                drive("SessionStart", &successor),
                "{source} claims over aaa"
            );
            let bytes: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&record).unwrap()).unwrap();
            (
                bytes["seq"].as_u64().unwrap(),
                bytes["incarnation"].as_str().unwrap().to_string(),
                bytes["state"].as_str().unwrap().to_string(),
            )
        };

        let startup = succeed("startup");
        assert_eq!(
            startup.1, "claude-session-bbb",
            "the successor owns the record"
        );
        assert_eq!(startup.2, "idle");
        for source in ["compact", "clear", "resume"] {
            assert_eq!(
                succeed(source),
                startup,
                "{source} must claim exactly as startup does while the wire has no condition axis"
            );
        }
    }

    /// The status-line payload captured verbatim from the version in the producer table, before
    /// the session's first API response.
    const PRE_TURN: &str =
        include_str!("../tests/fixtures/harness-context/claude-statusline-pre-turn.json");
    /// The same captured 2.1.250 envelope with a real `current_usage` object composed into it.
    ///
    /// **Composition, stated because it matters for what this fixture proves.** The envelope, its
    /// `context_window_size`, and its `rate_limits` are the verbatim 2.1.250 live capture; the
    /// `current_usage` object is a verbatim `usage` object off an assistant line of a real
    /// 2.1.250 transcript (`claude-opus-5`, 2026-08-29T11:46:14Z), and `used_percentage` /
    /// `total_input_tokens` are what the bundle's own builder computes from the two:
    ///
    /// ```js
    /// total_input_tokens: d.input_tokens + d.cache_creation_input_tokens + d.cache_read_input_tokens
    /// used: Math.min(100, Math.max(0, Math.round(r / t * 100)))
    /// ```
    ///
    /// So the numerator and the denominator are each measured, and the arithmetic joining them is
    /// quoted from the harness rather than inferred. What this fixture does NOT prove is that
    /// 2.1.250 emits exactly these bytes together in one payload — a populated live capture needs
    /// a paid turn and none was taken. A bump that moves the numerator's terms or the percent rule
    /// still fails here, which is what HC-R13 asks of it.
    const MID_SESSION: &str =
        include_str!("../tests/fixtures/harness-context/claude-statusline-mid-session.json");

    fn fixture(raw: &str) -> serde_json::Value {
        let payload: serde_json::Value = serde_json::from_str(raw).unwrap();
        // HC-R13: the version is asserted literally, so a fixture recaptured from a different
        // build cannot quietly keep proving the old arithmetic.
        assert_eq!(
            payload.get("version").and_then(serde_json::Value::as_str),
            Some(STATUSLINE_VERSION)
        );
        payload
    }

    #[test]
    fn a_mid_session_statusline_payload_yields_claudes_own_triple() {
        let reading = statusline_reading(&fixture(MID_SESSION));

        // input 2 + cache_creation 2837 + cache_read 191924, read off `current_usage` rather
        // than off the sibling `total_input_tokens` that agrees with it here.
        assert_eq!(reading.used_tokens, Some(194_763));
        assert_eq!(reading.window_tokens, Some(1_000_000));
        // Claude's own integer, taken as published: st2 never divides the operands to make one.
        assert_eq!(reading.used_percent, Some(19.0));
        assert_eq!(reading.model.as_deref(), Some("claude-opus-5"));
        assert_eq!(reading.cost_usd, Some(4.7312));
        assert_eq!(reading.rate_limits.five_hour, Some(31.0));
        assert_eq!(reading.rate_limits.seven_day, Some(55.0));
        // The payload's `total_*` keys describe the LAST RESPONSE, so nothing here is cumulative
        // session spend and the field stays null rather than being fed a number that would read
        // as occupancy on division (HC-R16).
        assert_eq!(reading.session_total_tokens, None);
    }

    #[test]
    fn a_pre_turn_statusline_payload_withholds_rather_than_reporting_zero() {
        let reading = statusline_reading(&fixture(PRE_TURN));

        // `current_usage` is null: Claude is positively declaring it does not yet know its own
        // occupancy, and the producer withholds (HC-R03). The trap is the sibling
        // `total_input_tokens`, which the bundle's builder emits as 0 for exactly this state —
        // a zero DERIVED FROM THE ABSENCE, which a producer reading it would publish as a
        // measurement of an empty window.
        assert_eq!(reading.used_tokens, None);
        assert_eq!(reading.used_percent, None);
        // The window is populated from the start and is reported (HC-R02): a legal record with a
        // window and no percent.
        assert_eq!(reading.window_tokens, Some(1_000_000));
        // Zero cost is an observation, not an absence: a producer filtering it to null would
        // lose the distinction between "free so far" and "not reported".
        assert_eq!(reading.cost_usd, Some(0.0));
        assert_eq!(reading.model.as_deref(), Some("claude-fable-5"));
        assert_eq!(reading.rate_limits.five_hour, Some(31.0));
    }

    fn agent_dir(tmp: &tempfile::TempDir) -> std::path::PathBuf {
        // One level down, so the writer has a parent to stage in outside the agent subtree.
        let dir = tmp.path().join("agents/Silber/fabric");
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn context(dir: &Path) -> crate::harness_context::Observed {
        harness_context::read(&harness_context::harness_context_path(dir)).unwrap()
    }

    #[test]
    fn a_pre_turn_reading_lands_once_and_then_sits_inside_its_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = agent_dir(&tmp);
        let payload = fixture(PRE_TURN);

        let mut writer = context_writer(&dir, "Silber.fabric", &payload).unwrap();
        assert!(writer.observe(statusline_reading(&payload)).unwrap());
        // A withheld percent has no bucket, so a second identical render is inside the written
        // one and, well inside the heartbeat, writes nothing. That is the write guard (HC-R09)
        // proved on the Claude path: a 5-second refresh interval does not mean 720 writes an hour.
        assert!(!writer.observe(statusline_reading(&payload)).unwrap());

        let observed = context(&dir);
        assert_eq!(observed.harness, Harness::Claude);
        assert_eq!(observed.used_percent, None);
        assert_eq!(observed.window_tokens, Some(1_000_000));
        assert_eq!(observed.compactions, 0);
    }

    #[test]
    fn claudes_three_compaction_edges_count_one_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = agent_dir(&tmp);
        let session = serde_json::json!({"session_id": "s-1"});
        let mut payload = session.clone();
        payload["trigger"] = "auto".into();

        observe_compaction(&dir, "Silber.fabric", "PreCompact", &payload).unwrap();
        let counted = context(&dir);
        assert_eq!(counted.compactions, 1);
        assert_eq!(
            counted.last_compaction_trigger,
            Some(CompactionTrigger::Auto)
        );

        // The completion edge holds the count and only moves `lastCompactionMs` forward.
        observe_compaction(&dir, "Silber.fabric", "PostCompact", &payload).unwrap();
        let completed = context(&dir);
        assert_eq!(
            completed.compactions, 1,
            "PostCompact must not double-count"
        );
        assert!(completed.last_compaction_ms >= counted.last_compaction_ms);

        // The third sighting of the same compaction. `SessionStart source=compact` is recognized
        // and deliberately inert — counting it is exactly the double count HC-R12 forbids.
        let mut restart = session;
        restart["source"] = "compact".into();
        observe_compaction(&dir, "Silber.fabric", "SessionStart", &restart).unwrap();
        assert_eq!(context(&dir).compactions, 1);
    }

    #[test]
    fn a_post_compact_without_a_counted_predecessor_counts_the_compaction_itself() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = agent_dir(&tmp);
        let payload = serde_json::json!({"session_id": "s-1", "trigger": "manual"});

        // No record at all: the PreCompact write never landed, so PostCompact is the first
        // evidence st2 has that a compaction happened and it counts rather than losing it.
        observe_compaction(&dir, "Silber.fabric", "PostCompact", &payload).unwrap();
        let observed = context(&dir);
        assert_eq!(observed.compactions, 1);
        assert_eq!(
            observed.last_compaction_trigger,
            Some(CompactionTrigger::Manual)
        );
    }

    #[test]
    fn a_subagents_compaction_never_touches_the_top_level_record() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = agent_dir(&tmp);
        let payload = serde_json::json!({
            "session_id": "s-1", "trigger": "auto", "agent_id": "sub-7"
        });

        observe_compaction(&dir, "Silber.fabric", "PreCompact", &payload).unwrap();
        assert!(harness_context::read(&harness_context::harness_context_path(&dir)).is_none());
    }

    #[test]
    fn an_unrecognized_trigger_word_decodes_as_unknown_not_as_a_definite_one() {
        assert_eq!(
            compaction_trigger(&serde_json::json!({"trigger": "auto"})),
            CompactionTrigger::Auto
        );
        assert_eq!(
            compaction_trigger(&serde_json::json!({"trigger": "manual"})),
            CompactionTrigger::Manual
        );
        // Additive tolerance in the direction that matters: a future word must not be guessed
        // into the closed vocabulary as if the harness had said it.
        assert_eq!(
            compaction_trigger(&serde_json::json!({"trigger": "idle"})),
            CompactionTrigger::Unknown
        );
        assert_eq!(
            compaction_trigger(&serde_json::json!({})),
            CompactionTrigger::Unknown
        );
    }

    #[test]
    fn the_tees_incarnation_is_the_same_token_the_hooks_publish() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = agent_dir(&tmp);
        let payload = serde_json::json!({"session_id": "abc"});

        // HC-A03: the wrapper's hook subprocesses and its status-line tee are driver processes of
        // ONE session, and they must publish one incarnation or a reader could not tell a
        // straggler from a sibling. Both derive it from the same payload field by the same rule.
        assert_eq!(
            wrapperless_token(&payload).as_deref(),
            Some("claude-session-abc")
        );
        context_writer(&dir, "Silber.fabric", &payload)
            .unwrap()
            .observe(statusline_reading(&payload))
            .unwrap();
        let raw = fs::read_to_string(harness_context::harness_context_path(&dir)).unwrap();
        assert!(
            raw.contains("\"incarnation\":\"claude-session-abc\""),
            "{raw}"
        );
    }
}
