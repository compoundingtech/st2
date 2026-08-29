# Harness context open questions

Each entry links a spec `DQ-C*`. Questions leave this file when resolved — into
[spec.md](./spec.md) as decisions or `.experiments/` as tested hypotheses.

- **DQ-C1 Write policy — the accuracy half is settled; the wire-cost half is
  unmeasurable today.** The policy question closed on a replay benchmark over 45
  Codex rollouts and 45 Claude transcripts
  ([`.experiments/2026-08-29-write-policy-benchmark.md`](./.experiments/2026-08-29-write-policy-benchmark.md)):
  fixed 1% quantization plus a compaction edge and a 300 s heartbeat, chosen on
  accuracy and structure because **cost does not discriminate** — every
  candidate, including "write every distinct reading", is 100–1,000× below the
  rate `DQ-H2` recorded as a failure. What stays open is the half
  [`DQ-H2`](../05-harness-state/open-questions.md) already owns and this
  subsystem inherits: **per-sync wire bytes are unmeasured, because no
  replication transport runs on the fleet today** — the catalog is a local
  filesystem and the transport's adoption record is still open. The estimate
  (5.7 writes/s fleet-wide at 600 agents, 1.9% of the transport's own coalescing
  ceiling) is arithmetic over a measured write rate, not a measured wire cost.
  Resolves by: that measurement once a transport runs. If it turns out large,
  the documented step is a 2% bucket, at the cost of alarms having to sit on
  even percentages.
- **DQ-C2 The status-line renderer contract — resolved 2026-08-29 by dotfiles
  PR #2160.** st2's tee resolves the downstream renderer in strict order:
  `$ST_CLAUDE_STATUSLINE_RENDERER` first, then
  `~/.claude/statusline-renderer.json` (schema
  `dotfiles.claude-statusline-renderer.v1`, carrying `{"command": …}`), then —
  if neither resolves — passing stdin through unchanged. A user-owned file was
  chosen over a settings key because the settings file st2 wins in is the one
  st2 rewrites, so a renderer declared there is exactly what the merge does not
  preserve (HC-R18's inverse). Specified in [spec.md](./spec.md) under the
  Claude producer; the entry stays here only so the identifier does not go
  dangling.
- **DQ-C3 Status-line settings precedence — resolved 2026-08-29 by live
  capture.** Four cases driven through a real pty against Claude Code 2.1.250
  settle the order:
  `.claude/settings.local.json` > `.claude/settings.json` >
  `~/.claude/settings.json`, with a control proving the global would otherwise
  have rendered. The winning `statusLine` **replaces** the losing object — one
  slot, one command per render, no merge. So st2's tee does run, and it must
  chain (HC-R18), because the file it wins in is the one st2 itself renders.
  Evidence is in
  [`.experiments/2026-08-29-context-signals-and-write-placement.md`](./.experiments/2026-08-29-context-signals-and-write-placement.md);
  the consequence is specified in [spec.md](./spec.md) under the Claude
  producer. The entry stays here only so the identifier does not go dangling.
- **DQ-C4 Doctor exposure and any threshold — resolved 2026-08-29.** Doctor
  warns, advisory-only, at or above `HARNESS_CONTEXT_WARN_PERCENT` (80) and on a
  stale record beside a `running` desired state; neither changes its exit
  status. The threshold is documented as st2's own attention number rather than
  a prediction of the harness's compaction point, which is harness-, model-, and
  setting-specific. Specified in [spec.md](./spec.md) under Doctor and ratified
  as HC-R17; the entry stays here only so the identifier does not go dangling.
- **DQ-C5 Unreadable versus absent in the projection.** The reading table
  projects a missing record and an unreadable/malformed/foreign one identically
  as `null`, which is simpler than the categorical axis and loses the
  distinction that `driverDiagnostic` deliberately keeps ("absent" is not
  "indeterminate", and neither reads healthy). For a purely numeric advisory
  axis the conflation is defensible; for an operator debugging why a runtime
  reports nothing it is not. Resolves by: either accepting it explicitly with
  the warning log as the only signal, or adding a status field to the
  projection.
- **DQ-C6 History.** The record collapses every compaction into a counter and
  one timestamp (HC-T02), so "how often did this runtime compact this week" and
  "how fast does its window fill" are unanswerable. This is the same question
  observed harness state defers in [`OHS-T03`](../05-harness-state/requirements.md),
  and it should be answered once for both axes rather than twice differently —
  the natural shape being events on the agent's built-in stream
  ([`04-stream`](../04-stream/requirements.md)) with the record as a
  denormalized projection. Resolves by: a follow-up that specifies history for
  the categorical and numeric axes together. See [roadmap.md](./roadmap.md).
- **DQ-C7 Fleet transport cost — folded into `DQ-C1`.** Kept as a pointer so the
  identifier does not go dangling: the per-sync wire cost of a context write is
  the open half of `DQ-C1` above, and it is the same measurement
  [`DQ-H2`](../05-harness-state/open-questions.md) owes the categorical record.
- **DQ-C8 Supervisor actionability.** HC-A02 makes the record advisory, so
  nothing may branch on 92% fill today. Making it actionable — nudging a
  compaction, retiring before a wedge — requires bounded-staleness semantics for
  a *remote* reader, which is exactly
  [`DQ-H5`](../05-harness-state/open-questions.md), still unmet and one of the
  reasons decision 0006's spec ships Draft. Resolves by: `DQ-H5` first, then a
  separate decision defining the action vocabulary.
- **DQ-C9 Subagent context is invisible.** Claude's status-line payload
  describes the top-level session; a subagent's own window is not in it (there
  is a sibling `subagentStatusLine` slot, unexamined), and hook events carrying
  `agent_id` are excluded from top-level state by both producers — the
  categorical one, and now the numeric one's compaction edges. So a runtime whose
  subagent is saturating reports the parent's fill, and a subagent's compaction is
  not counted against it.
  Whether that matters depends on whether subagents are where saturation
  actually happens on this fleet. Resolves by: a capture of the subagent status
  line, and a decision about whether one record per agent is the right grain.
- **DQ-C10 OpenCode multi-session aggregation.** The categorical producer
  aggregates across all of the server's sessions (any busy session is activity),
  because that projection composes. Occupancy does not: two live sessions have
  two windows and one record. The spec's producer row describes reading the last
  non-summary assistant message without saying whose session it belongs to.
  Resolves by: deciding between the most recently observed session (matching
  where delivery targets), the fullest window, or a per-session shape this
  record does not have.
- **DQ-C11 `harness-state` has the same placement defect — resolved 2026-08-29
  with this subsystem's own.** Both driver records sit at the agent-directory
  root and matched none of the replication transport's include globs. The
  resolution names them in that list (`**/harness-state,**/harness-context`)
  rather than moving either, so the shipped record is fixed without migrating a
  live record and
  [decision 0006](../.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md)'s
  "readers read through the catalog they already sync" holds again. The include
  list is fleet-side; st2's obligation is the pinned-name test in HC-R05.
- **DQ-C12 A driver runtime record under the resource directory — withdrawn.**
  The question only existed while the record was to be moved under `resources/`
  to satisfy the include globs. It stays at the agent-directory root, so no
  driver record lives on the Resource-binding realization surface and the
  tension does not arise.
- **DQ-C13 The status-line tee's telemetry cadence.** Claude's
  `refreshInterval: 5` makes the tee 720 short-lived `st2` processes per hour per
  agent — the highest-cadence process st2 has, where every other one is long-lived
  or event-driven. `main` initializes telemetry for every invocation and tears it
  down on exit; with `OTEL_EXPORTER_OTLP_ENDPOINT` set, that teardown performs a
  final collect-and-export, and Claude waits for the command to exit, so the flush
  is in the render path. Nothing here is measured: the whole suite runs with no
  endpoint, so the exporting path has never executed under the tee. The
  process-unit classification is not the lever — it sets `service.name` only.
  Resolves by: measuring one render's wall clock with an endpoint configured. If
  it is material, the options are exempting the tee from telemetry
  initialization, raising `refreshInterval`, or making the flush non-blocking for
  short-lived units — the first two are cheap and the third is a
  `06-observability` change.
