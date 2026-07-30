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
  from a complete, valid, unambiguous owner and render-dependency set in the
  local catalog. A partially delivered or invalid input is not evidence that a
  previously declared task was intentionally removed.
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
- **SPEC-T02 Explicit migration over inferred moves:** A host, identity,
  task-ID, or explicitly selected sensitive-root move may require a staged
  migration. Refusal is preferable to guessing that two differently addressed
  states are the same live work.
- **SPEC-T03 Safe removal over eager cleanup:** A task may remain until a
  complete valid owner plus exact host-local runtime attribution prove
  intentional removal. Legacy or unattributed work remains held for explicit
  recovery; invalid or ambiguous input never authorizes guessed teardown.

## Requirements

- **SPEC-R01 Normalize before comparison:** Formatting, comments, declaration
  order, and source-path changes are semantic no-ops only when they lower to the
  same semantic field projection, render plan, task IDs, resolved paths, and
  effective launch definitions. Explicit identity prevents a path move from
  inventing a new identity; it does not hide a path-derived host or cwd, a
  declared workspace, or a state-anchor change. A source-only no-op authorizes
  no materialization or process-lifecycle action by itself; it does not suppress
  ordinary actual-state reconciliation of an independently absent or dead task.
- **SPEC-R02 Classify by effect:** Every normalized field delta receives an
  action-driving class: source-only, declaration metadata, render input,
  task-set identity, launch fingerprint, subsequent policy, retirement, or
  migration boundary. Retirement and migration boundaries dominate
  lower-impact effects of the same edit. Core st2 has no semantic state for
  fields it deliberately ignores. compile-agent or another provider may
  interpret those inputs and lower them into runner-normative fields; core
  classifies only that lowered delta.
- **SPEC-R03 Preserve unrelated work:** One field or task change affects only
  its exact owner/task IDs or its proven shared render dependency set. It never
  restarts, tears down, or rewrites an unrelated task or agent.
- **SPEC-R04 Keep metadata nondisruptive:** Role and Resource binding changes
  update observable declaration metadata. They do not stop, replace, or
  relaunch a healthy task.
- **SPEC-R05 Materialize without process churn:** A render delta is preflighted
  against the complete active local fleet, then applied idempotently. A
  conflict fails every affected owner before the first write. Successful
  materialization does not itself restart a healthy task.
- **SPEC-R06 Reconcile task-set deltas narrowly:** Adding one uniquely
  identified PTY or exec task launches only that missing child. Removing one
  from a complete valid current owner explicitly tears down only an old child
  whose ordinary host-local runtime metadata proves the exact catalog, host,
  owner, task ID, and current incarnation. Legacy or unattributed tasks
  hold/refuse for explicit recovery. No synced prior snapshot, tombstone, or
  CAS is required. Compact DING addition and removal follow the same
  derived-task rule.
- **SPEC-R07 Expose launch drift:** Task kind, command or argv, effective cwd,
  effective process environment, task tags passed to the backend, and every
  other spawn-affecting field form a versioned desired launch fingerprint. An
  absent or dead task launches the latest desired definition. A healthy task
  whose observed incarnation does not match remains alive and is visibly
  `drifted` or `unknown`; replacement is a separate explicit, task-scoped
  action with an exact incarnation recheck.
- **SPEC-R08 Apply policies prospectively:** `keep`, `restart`, and task
  `lifecycle` changes alter subsequent reconciliation without replacing a
  healthy process. `retired #true` is the explicit exception: it tears down the
  declared task set and prevents relaunch. A simultaneous retirement plus child
  removal does not make the removed child safe to forget: teardown still
  requires the exact removal attribution in SPEC-R06.
- **SPEC-R09 Treat address changes as migrations:** Agent identity changes are
  retire-old/add-new. Task name or explicit ID changes are remove-old/add-new.
  Host, catalog-root, PTY-root, and another explicitly selected sensitive-root
  change are migration boundaries, not in-place updates. A workspace change
  ordinarily produces launch drift through effective cwd plus any independently
  resolved render or Resource delta; it does not inherently invoke
  sensitive-root migration. `adopt-only` may fence launch/replacement during a
  cutover, but it does not by itself move state or prove migration complete.
- **SPEC-R10 Fail closed and explain first:** Invalid, partial, ambiguous,
  conflicting, or unreadable desired/actual state blocks the smallest
  owner/render-dependency set whose completeness cannot be proved. The block
  broadens only when attribution or isolation itself is unprovable; an invalid
  remote-host declaration does not block valid isolated local work. Partial or
  invalid input retains last-known-good ownership and never authorizes removal.
  Status and true no-write dry-run surfaces quietly report affected task IDs,
  action/refusal class, proof scope, drift state, and reasons before mutation.
- **SPEC-R11 Make moved intent explicit and optional:** Ordinary agent identity,
  task name, task ID, or host edits remain retire/remove-old plus add-new; st2
  never infers a rename. A future catalog-native `moved` mapping may explicitly
  relate one fully qualified old address to one fully qualified new address as
  migration intent, not an alias, hidden history, or global identity authority.
  Preflight proves exact old catalog/host/owner/task/incarnation ownership, a
  one-to-one acyclic mapping, and no conflicting live or desired destination.
  The same live incarnation is preserved only when the backend can atomically
  re-address it without changing any process-visible identity or launch field;
  otherwise the mapping guides explicit scoped replacement or staged host
  migration. Cross-host execution is local-first and holds until old retirement
  is proven, without synchronous all-host availability, CAS, or an external
  registry. Quiet status and true dry-run report `pending`, `refused`, or
  `completed`; removing the mapping before completion fails closed.

## Evidence boundary

This draft is normative documentation only. After approval, a paired external
field matrix must prove every row in [spec.md](./spec.md) against the public CLI
and isolated PTY/exec state. Unit tests remain necessary source evidence but are
not sufficient acceptance for this contract.
