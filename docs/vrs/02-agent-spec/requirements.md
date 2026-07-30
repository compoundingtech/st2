# Agent Spec field-change requirements

## Context

This subnode decomposes the root requirements for Agent Spec compliance
([R01](../requirements.md#must-implement-the-agent-contract)), restartable
launch definitions ([R06](../requirements.md#must-preserve-delivery-and-launch-behavior)),
nondisruptive control-plane replacement
([R11](../requirements.md#must-preserve-delivery-and-launch-behavior)), and
shortest-path and targeted reconciliation
([R13–R19](../requirements.md#must-externalize-agent-state-and-scope)).
It defines how one normalized declaration delta affects already-running work.
It does not change the product vision, define provider or harness fields, or
require a content-addressed catalog.

The field matrix and current implementation gaps are in [spec.md](./spec.md).
Where this file and the root VRS disagree, the root wins and this file is
wrong.

## Assumptions

- **SPEC-A01 Complete local declaration:** A destructive delta is computed only
  from a complete, valid, unambiguous local catalog snapshot. A partially
  delivered or invalid snapshot is not evidence that a previously declared
  task was intentionally removed.
- **SPEC-A02 Immutable running process:** A running process cannot be updated in
  place. Its command, argv, working directory, environment, and backend
  attributes were fixed when its exact incarnation launched.
- **SPEC-A03 Stable task identity:** Reconciliation addresses a task by its
  normalized host-qualified runtime ID. An identity change is a removal plus an
  addition, not an update of the old process.
- **SPEC-A04 Partition-local sufficiency:** Classification, preview,
  materialization, and reconciliation use a complete local catalog plus
  host-local runtime state. They require no online CAS service, lock server, or
  cross-host RPC.

## Acceptable tradeoffs

- **SPEC-T01 Visible drift over surprise churn:** Preserving a healthy
  interactive process and reporting launch drift is preferable to silently
  restarting it.
- **SPEC-T02 Explicit migration over inferred moves:** A host, workspace,
  identity, task-ID, or other state-bearing move may require a staged migration.
  Refusal is preferable to guessing that two differently addressed states are
  the same live work.
- **SPEC-T03 Safe removal over eager cleanup:** A task may remain until a
  complete valid snapshot and an exact affected ID prove intentional removal.
  Invalid or ambiguous input never authorizes teardown.

## Requirements

- **SPEC-R01 Normalize before comparison:** Formatting, comments, declaration
  order, and source-path changes are semantic no-ops only when they lower to the
  same semantic field projection, render plan, task IDs, resolved paths, and
  effective launch definitions. Explicit identity prevents a path move from
  inventing a new identity; it does not hide a path-derived host or cwd, a
  declared workspace, or a state-anchor change.
- **SPEC-R02 Classify by effect:** Every normalized field delta receives an
  action-driving class: source-only, declaration metadata, runner-opaque
  extension, render input, task-set identity, launch fingerprint, subsequent
  policy, retirement, or migration boundary. Retirement and migration
  boundaries dominate lower-impact effects of the same edit. Unknown
  harness/provider fields remain runner-opaque and are interpreted only by
  compile-agent or another provider layer.
- **SPEC-R03 Preserve unrelated work:** One field or task change affects only
  its exact owner/task IDs or its proven shared render dependency set. It never
  restarts, tears down, or rewrites an unrelated task or agent.
- **SPEC-R04 Keep metadata nondisruptive:** Role, Resource bindings,
  descriptive task tags, and runner-opaque extension changes update declaration
  state and may wake their specialized consumer. They do not stop, replace, or
  relaunch a healthy task.
- **SPEC-R05 Materialize without process churn:** A render delta is preflighted
  against the complete active local fleet, then applied idempotently. A
  conflict fails every affected owner before the first write. Successful
  materialization does not itself restart a healthy task.
- **SPEC-R06 Reconcile task-set deltas narrowly:** Adding one uniquely
  identified PTY or exec task launches only that missing child. Removing one
  from a complete valid snapshot explicitly tears down only that old child.
  Compact DING addition and removal follow the same derived-task rule.
- **SPEC-R07 Expose launch drift:** Task kind, command or argv, effective cwd,
  effective process environment, and every other spawn-affecting field form a
  versioned desired launch fingerprint. An absent or dead task launches the
  latest desired definition. A healthy task whose observed incarnation does
  not match remains alive and is visibly `drifted` or `unknown`; replacement is
  a separate explicit, task-scoped action with an exact incarnation recheck.
- **SPEC-R08 Apply policies prospectively:** `keep`, `restart`, and task
  `lifecycle` changes alter subsequent reconciliation without replacing a
  healthy process. `retired #true` is the explicit exception: it tears down the
  declared task set and prevents relaunch.
- **SPEC-R09 Treat address changes as migrations:** Agent identity changes are
  retire-old/add-new. Task name or explicit ID changes are remove-old/add-new.
  Host, workspace, catalog-root, and PTY-root changes are migration boundaries,
  not in-place updates. `adopt-only` may fence launch/replacement during a
  cutover, but it does not by itself move state or prove migration complete.
- **SPEC-R10 Fail closed and explain first:** Invalid, partial, ambiguous,
  conflicting, or unreadable desired/actual state authorizes no destructive
  action. Status and true no-write dry-run surfaces report the semantic delta,
  affected task IDs, action class, drift state, reasons, and refusals before
  mutation.

## Evidence boundary

This draft is normative documentation only. After approval, a paired external
field matrix must prove every row in [spec.md](./spec.md) against the public CLI
and isolated PTY/exec state. Unit tests remain necessary source evidence but are
not sufficient acceptance for this contract.
