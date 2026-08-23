# Observed harness state open questions

Each entry links a spec `DQ-H*`. Questions leave this file when resolved —
into [spec.md](./spec.md) as decisions or `.experiments/` as tested
hypotheses.

- **DQ-H1 Claude blocked-exit edge.** `PermissionRequest` carries no
  `tool_use_id`, so leaving `blockedOn: human` can only match on tool *name* —
  and Claude batches tool calls (`PostToolBatch` carries plural `tool_calls`),
  so the first call's `PostToolUse` would clear `blocked` while the human still
  faces the second call's prompt. The corpus enters `blocked` in 2 of 9
  captures and exits in 1, with one tool and no batching: a rule validated on
  a single exit path is not validated. Until then the producer holds
  `blockedOn: human` until turn end (`Stop`) rather than encoding an exit rule
  that cannot hold. Resolves by: a capture with batched tool calls where the
  second call needs permission, then specifying the exit edge against it.
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
  case). Same-host readers close the window via the liveness cross-check;
  cross-host readers wait out the fifteen-minute horizon. Resolves by: either
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
- **DQ-H6 OpenCode state source.** The producer needs a verified source.
  Candidate: OpenCode's server/SDK event surface (session state, message
  lifecycle); fallback: the shipped composer adapter's positive markers
  (`ctrl+p commands` footer) with documented limits. Nothing is measured yet —
  the DING adapter (#313) proves only composer classification. Resolves by:
  an `.experiments/` capture of OpenCode's event surface on a pinned version,
  then choosing the source and its skew policy — the repo's standing rule
  (pin where version skew fails silently, as `checks.pi-extension-types` and
  `SUPPORTED_CODEX_CLI_VERSIONS` do) applies.
