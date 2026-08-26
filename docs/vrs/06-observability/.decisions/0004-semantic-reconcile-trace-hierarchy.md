# Semantic reconcile trace hierarchy

Status: accepted

Recorded 2026-08-26 from the PR3 otelite hierarchy proof
([../.experiments/2026-08-26-otelite-reconcile-hierarchy.md](../.experiments/2026-08-26-otelite-reconcile-hierarchy.md)).

## Context

PR3's tracing-facade migration exported exactly one `st2.reconcile_pass` span. The span correlated
logs and carried final pass counts, but the trace remained flat: a slow or failing pass could not
show whether catalog locking, desired-state discovery, hook verification, materialization, runtime
observation, or runner mutation owned the time or failure. Function-level instrumentation would
make that question worse by exposing planning and wrapper structure rather than reconciliation
semantics.

The hierarchy must preserve the existing root and control flow, remain bounded per pass, obey the
central non-empty `span.label` and cardinality contract, and construct no child telemetry when the
tracer exporter is absent even though the stderr tracing subscriber is always installed.

## Evidence and Argument

The linked otelite capture proves the flat baseline can become one root plus five direct,
same-trace children on an empty catalog without changing the existing metric/log assertions.
Reading the reconciliation lifecycle identifies exactly six external operation boundaries; all
other candidate seams are pure planning, bookkeeping, wrappers, or unbounded per-task detail.

## Decision

Keep `st2.reconcile_pass` as the compatibility root and add only aggregate direct children at real
I/O or mutation boundaries:

- `st2.catalog.lock`
- `st2.catalog.discover`
- `st2.hooks.verify`, only when verification is required
- `st2.catalog.materialize`
- `st2.runtime.observe`, only for an authoritative in-pass `Runner::list_sessions`
- `st2.reconcile.execute`

Every span has a bounded, public `span.label`. Attributes contain bounded enums and counts only;
ids, paths, selectors, and error prose are excluded. A child gets OTel `ERROR` status when its
boundary fails or reports errors; the root gets `ERROR` when the pass fails. All child constructors
early-return behind the tracer-export-enabled `AtomicBool`, not `tracing::enabled!`. Children are
always direct root children, never nested by incidental call structure.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Bounded semantic aggregate children | Selected | Locates lifecycle latency/failure while keeping one fixed detail budget. |
| Flat root only | Rejected | Cannot locate latency or failure within a reconcile pass. |
| Function-mirroring spans | Rejected | Exposes implementation structure rather than external work. |
| Unbounded per-task or per-owner spans | Rejected | Volume and cardinality scale with catalog and mutation size. |


### Flat root only

Rejected. It preserves minimum volume but cannot locate latency or failure within a reconcile
pass; the trace adds little beyond the correlated completion log.

### Function-mirroring spans

Rejected. Spans for validation, planning, compilation, debounce, wrappers, watcher callbacks,
and report absorption would expose implementation structure, inflate volume, and make refactors
look like telemetry contract changes without representing additional external work.

### Unbounded per-task or per-owner spans

Rejected for this hierarchy. They may diagnose individual runner operations but cardinality and
volume scale with catalog size and mutation count. Such detail requires a separately specified hard
budget and privacy/cardinality review before adoption.

## Consequences

- One pass exports at most the fixed aggregate set; hook verification and external-snapshot
  observation are truthfully omitted when not performed.
- Grafana can attribute pass duration and error status to lifecycle boundaries without querying
  error strings.
- The empty-catalog otelite test proves one root, five direct children, shared trace identity,
  exact parent ids, and non-empty labels.
- Detail below aggregate execution remains unavailable by design; a future per-task proposal must
  define and prove its own bound rather than extending this decision implicitly.
