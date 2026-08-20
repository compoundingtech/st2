# Stream open questions

Each entry links a spec `DQ-S*`. Questions leave this file when resolved —
into [spec.md](./spec.md) as decisions or [`.experiments/`](./.experiments/)
as tested hypotheses.

- **DQ-S3 Ring bound.** `K = 128` is a guess. Resolves by: measuring real
  adapter emit rates (CI flaps, timer sources) against replay windows; the
  bound must exceed the longest plausible producer retry horizon.
- **DQ-S4 Request absorption staging.** The typed request/reply envelopes
  (`request.rs`) are absorbed by events + ordinary replies (decision 0004),
  but its wire types carry `deny_unknown_fields` and its invariant row names
  live tests. Resolves by: a staged plan — land replacement proofs, re-point
  the invariant row, retire the module behind a deprecation window; blocked
  until the stream implementation itself is merged.
- **DQ-S5 Top-level shared streams.** One adapter feeding many agents
  (STREAM-T02). Must be defined as a generalization: a nested stream is a
  top-level stream whose owner and sole recipient is the enclosing agent.
  Blocked on: real demand — two catalogs actually duplicating an adapter.
- **DQ-S6 Parked-stream owner notification.** A crash-looping stream parks and
  surfaces to the declared supervisor; the owning agent — whose eyes just
  closed — learns nothing. Should the owner also receive one event/notice?
  Resolves by: operating v1; if agents act on stale world-views because a
  stream died silently, the answer is yes.
- **DQ-S7 Event body bounds.** Issue #238 wants bounded inbox bodies for
  one-inference DING handling; the DING frame carries no body, so an event's
  subject is the entire wake-time signal. Resolves with #238's outcome; until
  then adapters keep subjects self-sufficient.
- **DQ-S8 One-shot stream completion.** The task model has no run-to-completion
  lifecycle (`TaskLifecycle` is `Service | AdoptOnly`), so a wait-adapter that
  exits after its terminal event relaunches, flaps, and parks — a fault report
  for a successful wait. The accepted doctrine (spec: "Waits are standing
  feeds") makes this the rare residual case: common waits ride standing keyed
  feeds like `pty-lifecycle-watch`. If completion semantics are ever needed,
  they belong in the task model (`TaskLifecycle`/`JobType`, adjacent to root
  DQ1's scheduled exec work), not as a stream-level flag. Blocked on: real
  demand — an agent whose one-off custom waits are frequent enough that
  add/rm plus a kept-alive adapter measurably hurts.
