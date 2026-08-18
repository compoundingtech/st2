# Claude observed harness state from lifecycle hooks

Date: 2026-08-17

## Question

Can st2 observe a Claude session's coarse harness state — active, idle, blocked on a human, ended —
promptly, from outside the model and without a screen scraper, and does it survive the harness dying?

## Method

Claude Code 2.1.231. The hook vocabulary was mined from the bundle, not from documentation: `strings` over
`bin/.claude-wrapped` yields the master enum `O4`, 31 event names of which the embedded `update-config`
skill documents 10, and `R1c`, the `SessionEnd` reason set
`["clear","resume","logout","prompt_input_exit","other","bypass_permissions_disabled"]`. An isolated harness
(scratch project and `CLAUDE_CONFIG_DIR`, no live catalog) registered one recorder on **all 31** names.
Seven runs: headless `claude -p`; interactive PTY under `--permission-mode manual`, sitting at a real prompt
unanswered for 30 s, and under `bypassPermissions` (what st2 ships, `examples/native/agent-claude.kdl:13`),
`plan` and `auto`; `claude -p` killed mid-turn by SIGTERM and by SIGKILL; a wrapper prototype plus SIGKILL.
The interactive runs used a `pty.fork()` driver interleaving hook-log lines with the screen judged by
`src/ding/harness/claude.rs:145`'s predicates, which makes the blocked latency measured.

## Result

The blocked capture, the load-bearing artifact, on the driver's clock:

```
[  5.892] HOOK UserPromptSubmit    [  8.061] HOOK PreToolUse "Bash"    [ 8.067] PermissionRequest
[  5.897] SCREEN spinner ON        [  8.068] SCREEN prompt visible — BLOCKED, 30 s unanswered
[ 14.097] HOOK Notification permission_prompt    [38.576] PostToolUse "Bash"    [40.626] Stop
```

Both observers report blocked inside the same 150 ms tick (their sub-millisecond ordering is an instrument
artifact). **`PermissionRequest` means exactly "about to ask a human"**: it fired in 2 of 9 captures —
`manual` (`tool_name="Bash"`) and `plan` (`tool_name="AskUserQuestion"`) — and in **zero** of the
`bypassPermissions` and `auto` runs, where a classifier approved the identical `curl`. One event covers
prompts and choice menus, discriminated by `tool_name` and carrying `session_id`, `prompt_id`,
`permission_mode`, `tool_input` and `permission_suggestions` as structured data.
`Notification`/`permission_prompt` is a slower second witness at +6.014 s / +6.010 s; `Stop` ends a turn and
`idle_prompt` arrives at Stop+60.01 s meaning "idle and untouched".

**Death is silent, for SIGTERM as well as SIGKILL, and the wrapper write closes only part of that gap.**
Both kill trials reached `PreToolUse` mid-turn and both logs end on `PostToolBatch` with no `SessionEnd` and
no `Stop`, consistent with `R1c`, none of whose six reasons denotes death, so a hooks-only record sticks on
`active` forever. A prototype mirroring `src/claude_session.rs:93-107` (spawn, `try_wait` at 250 ms) writes
a terminal record when the child is reaped: hooks-only said `active`, wrapper-owned said `ended` with
`"cause": "wrapper_observed_provider_exit/-9"`, one poll tick (0.25 s) later. Its limit is st2's own
teardown. `stop_provider_group` (`:114-135`) sends `kill(-getpgrp(), SIGTERM)`, which the wrapper's handler
(`:29-39`) survives — the measured case — but after `STOP_GRACE` (5 s, `:19`) the escalation
`kill(-getpgrp(), SIGKILL)` (`:130-132`) goes **into the wrapper's own group**. SIGKILL is uncatchable, so
the wrapper dies with the provider and the following `child.wait()` never runs; the same holds for `pty
kill` and for the forced kill named by invariant row 11
(`tests/nomad_survival.rs::forced_kill_and_binary_replacement_adopt_pty_unchanged_without_duplicate`).
Elsewhere the fallback is heartbeat decay, and a record inheriting `STATUS_STALE` reads `active` for up to
15 minutes after the agent is dead.

**The obvious reducer is wrong twice, and one correction is not implementable as stated.** **D1:**
`SubagentStop` lands 1.7–2.9 s *after* `Stop` in every completed-turn run, so a reducer counting it as
activity reads `active` forever after a normal turn; the fix keys on `agent_id`, which top-level `Stop`
lacks — sound here, but resting on a phantom `SubagentStop` populating `agent_id` where no subagent ran, an
undocumented and unversioned property. **D2:** `MessageDisplay` fires while blocked, and its proposed fix —
leave `blocked` only on a resolution of the same `tool_use_id` — **cannot be implemented:
`PermissionRequest` carries no `tool_use_id`** in either occurrence in the corpus, so the key falls through
to `tool_name` and exit unblocks on any `PostToolUse` of that tool. Claude batches calls (`PostToolBatch`
carries a plural `tool_calls`), so two `Bash` calls with the second needing permission clear `blocked` while
the human is still at the prompt, and correlating through the preceding `PreToolUse` fails for exactly that
case. The corpus cannot catch it: `blocked` is entered in 2 captures and exited in 1, three rows are one
scripted run under different kills, and the ground-truth harness strips `SessionEnd`, so both kill runs and
the wrapper run "pass" with `active` — the right pre-death answer and the wrong post-death one.

## Resolved decisions

- Claude observation is **hooks**, not a revived PTY scraper and not MCP tools, which the model must choose
  to call and so cannot report a state in which it is not running. No latency advantage is claimed: the two
  were inseparable at 150 ms, and a raw byte stream cannot be fairly compared against the rendered screen
  DING classifies.
- `PermissionRequest` is the blocked-on-a-human edge, and blocked is a **first-class axis** rather than a
  flavour of `active`: a person, not elapsed time, ends it. Under `bypassPermissions`, which st2 ships
  today, it is vacuous. `UserPromptSubmit` → active, `Stop` → idle, top-level only.
- **The `blocked` exit rule is unresolved.** `tool_use_id` is unavailable and `tool_name` is wrong under
  batched parallel calls. Ship blocked only once an exit rule is proved against a capture with two
  concurrent calls to one tool, which this corpus does not contain.
- Hooks cannot report death, and the wrapper's terminal write is **not sufficient** on its own. A liveness
  cross-check is required beside it, against **PTY** task generations: a driver-declared agent lowers to
  `TaskKind::Pty` (`crates/agent-spec/src/spec.rs:913-930`), so the primitive is pidfile plus `kill(pid,0)`
  (`src/ding/mod.rs:693-703`), not `ExecGeneration`.
- Undocumented event names dispatch from a plain `settings.local.json`, so the union may carry events not
  yet observed. Two blind spots stay uncovered — the "do you trust this folder" prompt and the
  `bypassPermissions` warning fire zero hooks — so only a "session never started" timeout catches a process
  alive, blocked and silent. Observed harness state stays separate from agent-declared presence:
  `src/claude_session.rs:100-104` refreshes presence on a timer that never consults child state, so presence
  keeps succeeding while the turn signal is stale.

## Conclusion

Yes for the live lifecycle, no for death. `SessionStart`, `UserPromptSubmit`, `Stop`, `PermissionRequest`,
`Notification` and `SessionEnd` give the coarse states from a fail-open shell hook, all registrable from the
`json-upsert` block already rendered at `examples/native/agent-claude.kdl:21-59`. Blocked is provable here;
its exit edge is not. The wrapper's write closes the death gap only when the child alone dies, since st2's
own teardown kills the wrapper in the same instant; closing it needs a PTY-correct generation check, not a
second write.

## VRS Impact

Adds an observed-harness-state model distinct from the presence model in `docs/vrs/spec.md`, with a Claude
arm carrying `session_id`, `prompt_id`, the blocked tool name and `permission_mode`, and a terminal cause
distinguishing `SessionEnd` from a wrapper-observed provider exit. Extends the hook set rendered by
`examples/native/agent-claude.kdl` from three events to six. Adds a wrapper obligation to
`src/claude_session.rs` and, beside it, a liveness cross-check against PTY task generations, without which
the wrapper write leaves a ≤15-minute wrong `active` on st2's own teardown path; both are invariant-shaped
and need named tests first. Left open: whether `TeammateIdle`, `Elicitation` or `PermissionDenied` dispatch
from settings at all; whether the Agent SDK's `includeHookEvents` exposes the same stream more cheaply; what
the compaction events look like; and the `blocked` exit rule.
