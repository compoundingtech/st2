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

use std::io::Read as _;
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::harness_state::{Activity, Ask, BlockedOn, InputBuffer, Observation};
use crate::provider_session::{
    PROVIDER_POLL, STOP, SessionObserver, install_signal_handler, run_provider,
};
use crate::{harness_state, message, status};

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
    let mut raw = String::new();
    let _ = std::io::stdin().read_to_string(&mut raw);
    let payload = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
    let Some(observation) = observe_hook_event(event, &payload) else {
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
    // word. (`false` = suppressed; the hook has nothing else to do with it.)
    writer.observe_unless_ended(observation).map(|_wrote| ())
}

/// Select the ownership a hook write acts under. The wrapper's exported token makes hook writes
/// this session's records (adopted ownership when the claimed sequence travels beside it). A
/// wrapperless seat falls back to Claude's own session_id — and because token-only writers never
/// claim, the SessionStart arm IS that path's session boundary and performs the WRITTEN claim
/// (degrading to token-only with a warning if the claim cannot be written); later hooks of the
/// same session adopt its records by token. What such a seat still lacks is a heartbeat and
/// terminal owner — the documented hooks-only limitation.
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
    if let Some(id) = payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
    {
        let token = format!("claude-session-{id}");
        if event == "SessionStart" {
            return match harness_state::claim(agent_dir, identity, "claude", &token) {
                Ok(seq) => writer.with_ownership(token, seq),
                Err(error) => {
                    eprintln!(
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

/// Map one Claude hook event to an observation, or `None` when the event says nothing about
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
pub fn observe_hook_event(event: &str, payload: &serde_json::Value) -> Option<Observation> {
    // Any event carrying an agent identity is a subagent's and must never move top-level state:
    // a phantom `SubagentStop` trails every completed turn, 1.5-2.9s after `Stop`. That phantom
    // populating `agent_id` is an undocumented emergent property — if a future Claude build omits
    // it, subagent completions read as top-level activity again and nothing here would catch it.
    if payload
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|id| !id.is_empty())
    {
        return None;
    }
    match event {
        "SessionStart" => Some(
            Observation::new(Activity::Idle, BlockedOn::None, InputBuffer::Unknown)
                .with_reason("sessionStart"),
        ),
        "UserPromptSubmit" | "PreToolUse" | "PostToolUse" => Some(Observation::new(
            Activity::Active,
            BlockedOn::None,
            InputBuffer::Unknown,
        )),
        "Stop" => Some(Observation::new(
            Activity::Idle,
            BlockedOn::None,
            InputBuffer::Unknown,
        )),
        "PermissionRequest" => {
            // Driver-side classification (#162): the payload's tool_name distinguishes Claude's
            // question form from an ordinary permission prompt — the DQ-H1 captures show
            // AskUserQuestion arriving as a PermissionRequest like any other tool.
            let ask = if payload.get("tool_name").and_then(serde_json::Value::as_str)
                == Some("AskUserQuestion")
            {
                Ask::Question
            } else {
                Ask::Permission
            };
            Some(
                Observation::new(Activity::Active, BlockedOn::Human, InputBuffer::Unknown)
                    .with_ask(ask)
                    .with_reason("permissionRequest"),
            )
        }
        _ => None,
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
    fn hook_events_map_to_observations_with_the_blocked_edges() {
        let none = serde_json::Value::Null;

        let blocked = observe_hook_event("PermissionRequest", &none).unwrap();
        assert_eq!(blocked.state, Activity::Active);
        assert_eq!(blocked.blocked_on, BlockedOn::Human);
        assert_eq!(blocked.reason.as_deref(), Some("permissionRequest"));

        // The exit edges: tool progress or a turn boundary clears the human hold.
        for event in ["PreToolUse", "PostToolUse"] {
            let cleared = observe_hook_event(event, &none).unwrap();
            assert_eq!(cleared.state, Activity::Active);
            assert_eq!(cleared.blocked_on, BlockedOn::None);
        }
        let stop = observe_hook_event("Stop", &none).unwrap();
        assert_eq!(stop.state, Activity::Idle);
        assert_eq!(stop.blocked_on, BlockedOn::None);

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
            (entered.state, entered.blocked_on),
            (Activity::Active, BlockedOn::Human)
        );
        // 33 s of open prompt produced no intervening event in the capture; the very next event
        // is the granted call's own PostToolUse, which correctly releases the block.
        let released = observe_hook_event("PostToolUse", &post_touch).unwrap();
        assert_eq!(
            (released.state, released.blocked_on),
            (Activity::Active, BlockedOn::None)
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
        .observe(observe_hook_event("PermissionRequest", &serde_json::Value::Null).unwrap())
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
        .observe(observe_hook_event("UserPromptSubmit", &serde_json::Value::Null).unwrap())
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
        use crate::harness_state::Ask;
        let permission = observe_hook_event(
            "PermissionRequest",
            &serde_json::json!({ "tool_name": "Bash", "tool_input": {} }),
        )
        .unwrap();
        assert_eq!(permission.ask, Ask::Permission);

        let question = observe_hook_event(
            "PermissionRequest",
            &serde_json::json!({ "tool_name": "AskUserQuestion", "tool_input": {} }),
        )
        .unwrap();
        assert_eq!(question.ask, Ask::Question);

        // Non-blocking events carry no ask.
        let idle = observe_hook_event("Stop", &serde_json::json!({})).unwrap();
        assert_eq!(idle.ask, Ask::None);
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
                    observe_hook_event("PostToolUse", &serde_json::Value::Null).unwrap()
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
                    observe_hook_event("SessionStart", &serde_json::Value::Null).unwrap()
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
                    observe_hook_event("SessionStart", &serde_json::Value::Null).unwrap()
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
                .observe_unless_ended(observe_hook_event(event, payload).unwrap())
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
}
