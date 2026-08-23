# Observed harness state open questions

Each entry links a spec `DQ-H*`. Questions leave this file when resolved —
into [spec.md](./spec.md) as decisions or `.experiments/` as tested
hypotheses.

- **DQ-H1 Claude blocked-exit edge.** The batched capture #268 §C asked for
  was taken on 2026-08-23 against Claude Code 2.1.237
  (`.experiments/2026-08-23-claude-batched-permission.md`), and it resolves
  the predicted false-clear in the rule's favor: **tool execution is
  serialized around an open permission prompt.** In both batch orderings
  (allowlisted-first and permission-first), no hook event of any kind fires
  while the prompt is open — a parallel-batched allowlisted call renders on
  screen but its `PreToolUse` waits 33 s for the grant — so the next
  `PreToolUse`/`PostToolUse`/`Stop` after `PermissionRequest` is precisely the
  blocked call's own resolution, and the shipped rule
  (`src/claude_session.rs::observe_hook_event`) is correct, not merely
  conservative. `PermissionRequest` still carries no `tool_use_id` (its
  `prompt_id` is turn-scoped, shared by every event in the turn — not a call
  correlator). The residual limit is the **deny path**: selecting "No" ends
  the turn with *zero* further events — no `PostToolUse`, no `Stop`, and no
  `PermissionDenied` even when that hook is registered — so `blockedOn: human`
  stands until the next `UserPromptSubmit` or `SessionStart`. That reading is
  semantically half-true (a human's direction is still what the session
  waits on) and it under-reports nothing, but the state axis says `active`
  while the model is not running. Resolves by: a Claude build whose denial
  emits any hook event; until then the deny window is the pinned limit.
- **DQ-H2 Transport cost of per-transition writes — measured 2026-08-23,
  resolved by the restatement guard.** The live smoke run caught the failure
  mode: the OpenCode producer restated its state per SSE frame and the
  envelope re-stamped every restatement — 679 byte-distinct writes in 221 s
  (~2.7/s while idle). The envelope now makes an unchanged observation a
  no-op until the refresh cadence is due, so a seat writes on transitions
  plus at most one re-stamp per five minutes (a Claude turn measured 3
  writes; a pi turn 2–3). What remains open is only the fleet-scale sync
  question: nothing yet measures what transition-rate writes cost a
  600-seat catalog's transport over a day. Resolves by: that measurement on
  a live catalog.
- **DQ-H3 `child` has no producer.** The word is reserved because the tuple's
  reasoning needs it (a long-running foreground command is neither the model
  working nor idle), but the producer that would have supplied it — the PTY
  screen observer — is cut: its idle proof was Codex-specific and its Claude
  arm collided with the empty-composer sentinel (#268 Limits). Resolves by: a
  harness exposing a positive child-process signal (Claude `PreToolUse`/
  `PostToolUse` pairs are the candidate), proven against batching.
- **DQ-H4 Ungraceful-death coverage.** The wrapper's terminal write covers
  child-reap and SIGTERM; nothing in-process covers SIGKILL escalation into
  the wrapper's own group or an external forced kill (invariant row 11's
  case). Same-host readers NARROW the window via the liveness cross-check —
  only for provably dead sessions; `pty kill` removes the pidfile and leaves
  the probe indeterminate, so a same-host reader then shares the
  fifteen-minute horizon (or waits for the next relaunch claim) exactly like
  a cross-host one. Resolves by: either
  accepting the horizon (documenting it as the cross-host bound) or a
  supervisor-side terminal write derived from reconcile's session state —
  which would need its own fencing rules to avoid a supervisor overwriting a
  live wrapper's record.
- **DQ-H5 Supervisor-following behavior.** Root `DQ3` sets two gates for
  catalog agent state: stale-state behavior (addressed throughout this
  subsystem) and supervisor-following behavior — what a *remote* supervisor
  may conclude and do from this record. The second gate is unmet, which is a
  reason decision 0006's spec ships Draft. #107 already bounds it: a remote
  `unknown` is no fresh observation, never proof of ill health, and never
  gates local work. Resolves by: specifying remote-reader semantics with a
  proof, or explicitly scoping the record same-host advisory.
- **DQ-H6 OpenCode blocked-entry capture — resolved 2026-08-23.** Both pairs
  were captured live on a headless 1.18.19 server (the earlier failure to get
  a prompt came from setting permissions via `PATCH /config`; the same
  `{"permission":{"bash":"ask"}}` in the *config file* asks reliably, no TUI
  needed). The capture corrected the producer twice: the entry events carry
  `properties.id` but the exit events spell it `properties.requestID`
  (`permission.replied`, `question.replied|rejected`) — the schema-derived
  extraction would have held `blockedOn: human` forever after a real grant —
  and `GET /event` over HTTP/1.1 is chunk-encoded, which the line-oriented
  SSE reader cannot parse safely, so the producer requests it over HTTP/1.0,
  which the server streams raw. Verbatim captured pairs are fixture tests
  (`src/opencode_session.rs::captured_permission_grant_pair_enters_and_exits_blocked`,
  `::captured_question_reply_pair_enters_and_exits_blocked`); the raw frames
  and commands are in `.experiments/2026-08-23-opencode-surface.md`.
