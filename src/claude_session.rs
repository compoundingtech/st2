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

use crate::harness_state::{Activity, BlockedOn, InputBuffer, Observation};
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
    let observer = SessionObserver::new(&agent_dir, &identity, "claude");
    run_provider(
        "Claude",
        &status::status_path(&agent_dir),
        &claude_argv,
        &[],
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
pub fn run_observe(catalog_root: &Path, identity: &str, event: &str) -> Result<()> {
    let agent_dir = message::resolve_agent_dir(catalog_root, identity, &crate::run::detect_host())?
        .with_context(|| format!("Claude driver agent '{identity}' is not declared"))?;
    let mut raw = String::new();
    let _ = std::io::stdin().read_to_string(&mut raw);
    let payload = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
    let Some(observation) = observe_hook_event(event, &payload) else {
        return Ok(());
    };
    harness_state::Writer::new(&agent_dir, identity, "claude", Some(identity.to_string()))
        .observe(observation)
}

/// Map one Claude hook event to an observation, or `None` when the event says nothing about
/// top-level harness state.
///
/// Claude gives no call identity on the event that enters `blocked` (`PermissionRequest` carries
/// no `tool_use_id`), so the exit edge is the next `PreToolUse`/`PostToolUse`/`Stop`. Under
/// batched tool calls that can clear `blocked` while a later call in the batch still holds a
/// prompt; the limit is accepted and fenced in the VRS rather than papered over with a temporal
/// heuristic that fails for exactly the parallel case it would need to handle.
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
        "PermissionRequest" => Some(
            Observation::new(Activity::Active, BlockedOn::Human, InputBuffer::Unknown)
                .with_reason("permissionRequest"),
        ),
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

    #[test]
    fn wrapper_heartbeat_re_stamps_without_clobbering_hook_written_state() {
        let tmp = tempfile::tempdir().unwrap();
        let record = harness_state_path(tmp.path());
        let observer = SessionObserver::new(tmp.path(), "hetz.worker", "claude");

        // A hook process wrote a blocked observation between wrapper ticks.
        harness_state::Writer::new(tmp.path(), "hetz.worker", "claude", None)
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
        let observer = SessionObserver::new(tmp.path(), "hetz.worker", "claude");
        let stop = AtomicBool::new(false);

        // A turn is in flight when the provider dies by signal.
        harness_state::Writer::new(tmp.path(), "hetz.worker", "claude", None)
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
        let observer = SessionObserver::new(tmp.path(), "hetz.worker", "claude");
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
}
