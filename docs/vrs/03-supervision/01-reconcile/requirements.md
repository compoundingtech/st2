# Reconcile requirements

## Context

Reconcile is the deciding half of supervision: given this host's declarations
and an observed view of what is running, work out the smallest set of changes
that closes the gap. It decides; it does not act.

This refines [`SUP-R04`](../requirements.md) through
[`SUP-R06`](../requirements.md) and [`SUP-R12`](../requirements.md), which in
turn decompose the root's [`R13`](../../requirements.md). Where this file and
its parent disagree, the parent wins and this file is wrong.

Materialization is specified here rather than as a separate lifecycle. That is
not a filing convenience: workspace content is rendered as the **first phase of
every pass**, so an agent's declared content is brought up to date on the same
cadence as its processes. A standalone mode exists that performs discovery and
materialization and then stops, but it exposes that phase — it does not make
materialization a separate authoring-time step.

## Assumptions

- **RECON-A01 Declarations are cheap to re-read:** Every pass re-discovers the
  catalog from scratch rather than tracking incremental edits. Correctness comes
  from recomputing, not from remembering.
  - Validation: implementation evidence — the pass takes no incremental state
    from its predecessor.
- **RECON-A02 Materialization is idempotent:** Rendering the same declared
  content over an unchanged workspace produces no change, so running it every
  pass is free in the common case.
  - Validation: implementation evidence; the module documents ordered,
    idempotent pre-boot materialization.

## Constraints

- **RECON-C01 The observed view is a snapshot, not a subscription:** Liveness is
  sampled once per pass. Anything that changes between the sample and the action
  is not visible to that pass.
- **RECON-C02 Some workspaces are under version control:** Rendering into a
  workspace can collide with content a human owns, so materialization cannot
  assume the destination is st2's to overwrite.

## Acceptable Tradeoffs

- **RECON-T01 Per-agent isolation over pass-level atomicity:** A pass is not a
  transaction. When one agent cannot be prepared, it drops out of that pass and
  every other agent proceeds. A partially converged host is preferable to a host
  where one bad declaration blocks all work.
- **RECON-T02 Recompute over incremental tracking:** Recomputing the whole delta
  every pass costs more than tracking edits, and is chosen anyway because it
  cannot drift. An event tells the pass to run; it never tells it what to do.

## Requirements

### Must decide before acting

- **RECON-R01 The plan is a value:** Deciding produces a description of the
  intended changes and performs none of them. It must be computable from
  declarations and observed sessions alone, with no filesystem or process
  effects, so it can be tested exhaustively without running anything.
- **RECON-R02 The plan is total:** Every declaration the pass considers lands in
  exactly one outcome — start missing work, tear down retired work, leave a
  fully-present agent alone, skip another host's, or name it as having nothing
  runnable. A declaration must never fall through unaccounted for.
- **RECON-R03 Reconciliation is per task, not per agent:** An agent whose tasks
  are partly running converges by starting only the missing ones. An agent is
  never restarted wholesale because one of its tasks died.

### Must prepare before it launches

- **RECON-R04 Materialization precedes launch in every pass:** Declared
  workspace content is rendered before any task of that agent is started, on
  every pass and not only at authoring time, so a launched task always sees
  content matching the declaration that launched it.
- **RECON-R05 Preparation failure is per agent:** An agent whose content cannot
  be rendered is removed from that pass — not launched, not partially launched —
  while every other agent proceeds (RECON-T01).
- **RECON-R06 An unsatisfied precondition defers only what depends on it:** When
  a precondition that some agents require cannot be confirmed, only the agents
  requiring it are held back. Agents that do not require it materialize and
  launch normally, and agents already running are unaffected — the failure
  defers preparation, it does not tear anything down.
- **RECON-R07 Materialization must not silently overwrite owned content:**
  Rendering into a destination a human's version control tracks must fail
  closed rather than change it.

### Must convert observed state into exactly one intent

- **RECON-R08 A retired declaration converges to absent:** Its live tasks are
  torn down; its dead task records are removed unless pinned; once nothing
  remains it requires no further action and no presence.
- **RECON-R09 An active declaration converges to fully running:** A task that is
  absent is started. A task whose record is dead is removed and started again.
  A task that is alive is left untouched.
- **RECON-R10 A pinned dead task is frozen:** When a task is pinned against
  collection and its record is dead, it is neither collected nor restarted. The
  pin preserves evidence, and preserving evidence necessarily means the task
  stays down until a human acts.
- **RECON-R11 A declaration with nothing runnable is named, not launched:** A
  declaration that carries no runnable command is reported as such rather than
  being silently treated as converged.
- **RECON-R12 Another host's declaration is accounted for and untouched:** It is
  counted in the report and no liveness query, launch, teardown, or
  materialization is performed for it.

### Must refuse to act on an unknown world

- **RECON-R13 No snapshot, no reconciliation:** If the observed view cannot be
  established, the pass performs nothing and says so. An empty view must never
  be treated as "everything is missing", because that would relaunch the whole
  host.
- **RECON-R14 Discovery failures are reported, never fatal:** A file that will
  not parse is surfaced against the pass and does not prevent every other
  declaration from converging.
