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
- **DQ-H2 Transport cost of per-transition writes.** Presence refreshes every
  five minutes; turn boundaries are far more frequent, and burst coalescing
  measured 4 transitions per turn 0.1–0.4 ms apart. No measurement establishes
  what per-transition replicated writes cost on a real catalog under a real
  transport (OHS-T01 accepts this for v1). Resolves by: measuring write and
  sync volume on a live catalog; if unacceptable, a minimum-interval
  coalescing window is the tuning knob, at the cost of spinner latency.
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
- **DQ-H6 OpenCode blocked-entry capture.** The state source itself is
  resolved: the server's SSE event surface, measured on 1.18.19
  (`.experiments/2026-08-23-opencode-surface.md`) and gated by
  `SUPPORTED_OPENCODE_VERSIONS` plus the live `/doc` subset check. What
  remains open is the blocked-on-human pair: `permission.asked` /
  `permission.replied` are schema-backed with explicit `^per` ids — the exit
  edge is clean by construction, unlike Claude's — but no live capture of a
  real permission prompt exists (headless runs with `{"bash":"ask"}` never
  asked). Resolves by: one capture from a TUI seat with a real permission
  prompt, confirming the events fire and carry the id the producer matches.
