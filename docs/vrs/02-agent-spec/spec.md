# Agent Spec field-change specification

This document maps the field-change requirements to the current parser,
materializer, reconciler, runner, and CLI. It builds on
[requirements.md](./requirements.md) and is intentionally harness-agnostic and
complete without CAS.

## Status

Draft for principal review. The **Normative action** column is the desired
contract. The **Current state** column records implementation evidence and gaps;
it does not weaken the normative action.

## Normalization boundary

Canonical KDL, supported TOML, and supported JSON lower through the shared
[`AgentSpec` and `Task` model](../../../crates/agent-spec/src/spec.rs).
[Discovery](../../../crates/agent-spec/src/discovery.rs) resolves explicit or
path-derived placement, and the
[KDL reader](../../../crates/agent-spec/src/kdl_format.rs) ignores
provider-owned fields while lowering runner-normative fields.

Comparison is over a semantic projection of normalized effects, not source
bytes or the `AgentSpec.path` storage field itself. Two sources are semantically
equal only when all of these are equal:

1. agent identity, host placement, and exact runtime task IDs;
2. runner-visible metadata;
3. the separately parsed render plan and its resolved targets;
4. task set and derived compact tasks;
5. effective launch fingerprints;
6. subsequent reconciliation policies; and
7. retirement and state-bearing path boundaries.

A source path is therefore not universally inert. Explicit `identity` and
`host` make placement path-independent, but the declaration path remains the
fallback cwd and a state/resource anchor. A move is a no-op only when those
normalized effects also remain equal.

## Field matrix

| Field or change category | Source of current semantics | Normative action | Current state |
| --- | --- | --- | --- |
| Formatting, comments, KDL node order, map order | [KDL lowering](../../../crates/agent-spec/src/kdl_format.rs), [shared lowering](../../../crates/agent-spec/src/spec.rs) | If the complete normalized model and render plan are unchanged, the desired delta is `no-op` and authorizes no materialization or process-lifecycle action. Ordinary actual-state reconciliation may still discover and heal a separately absent/dead task. | Parsing is order-insensitive for agent env and maps; the resident watcher still treats a write as evidence and runs an ordinary pass. It does not yet publish a semantic-delta classification. |
| Declaration path with explicit identity and host | [path/default resolution](../../../crates/agent-spec/src/discovery.rs), [cwd resolution and bus environment](../../../src/run.rs) | A move is a source-only no-op only when task IDs, resolved cwd/workspace, and state/resource anchors are unchanged. Otherwise classify the affected path or placement boundary explicitly. | Explicit identity plus host are path-independent. A moved file can still change fallback cwd or the declaration-relative state anchor, so unconditional path no-op is not implemented. |
| Agent `type` | [job type model and lowering](../../../crates/agent-spec/src/spec.rs), [validation](../../../src/validate.rs) | Omitted and explicit `service` are equivalent. Any other value makes that owner scope invalid and authorizes no launch or destructive action in the unproven scope. | The normalized model supports only `service`, and `validate` reports other values. Lowering currently maps every raw value to `service`, so a caller that reconciles without a clean validation gate can still treat an invalid type as runnable. |
| `role`, `resource` | [Agent Spec fields](../../../crates/agent-spec/src/spec.rs), [catalog inspection](../../../src/agents.rs) | Update observable declaration metadata. Do not change the launch fingerprint or churn a healthy task. | These fields do not drive the current ID-only adoption decision. Resource inspection exists. No general field-delta event/status surface exists. |
| Unknown `harness`, `model`, `persona`, `permissions`, `transport`, `strategy`, `meta`, or provider extension | [ignored KDL fields](../../../crates/agent-spec/src/kdl_format.rs), [runner model boundary](../../../crates/agent-spec/src/spec.rs) | Exclude ignored source from core semantic equality: core cannot compare it or wake a specialized consumer for it. A compiler/provider may observe its own input and lower a change into render, task-set, launch, or metadata fields; core acts only on that lowered delta. | Core parsing ignores these fields. Their effects become visible only after a provider lowers them into runner-normative fields or render operations. |
| `render {}` operation, template bytes, resolved destination | [render parsing and execution](../../../src/materialize.rs), [full-pass gate](../../../src/run.rs) | Preflight all resolved claims against the complete active local fleet; reject incompatible shared ownership before any write; otherwise apply idempotently. Never restart a healthy task merely because workspace bytes changed. | Implemented for full and selected materialization, including complete-fleet conflict analysis and tracked-file safety. The watcher does not yet compute the smallest template dependency set described by root R13. |
| Add one explicit `pty`/`exec`, or add compact `ding` | [compact/explicit task lowering](../../../crates/agent-spec/src/spec.rs), [task-level reconcile](../../../src/reconcile.rs) | Launch only the uniquely missing child from desired bytes. Preserve every existing sibling PID/incarnation. | Implemented by task-ID reconciliation; generated compact DING is a normal derived exec task. |
| Remove one explicit `pty`/`exec`, or remove compact `ding` | [discovery](../../../crates/agent-spec/src/discovery.rs), [task-level reconcile](../../../src/reconcile.rs), [PTY/exec runtime state](../../../src/run.rs) | A complete valid current owner plus ordinary host-local runtime metadata must attribute the old child to the exact catalog, host, owner, task ID, and current incarnation. Then tear down only that child. Legacy/unattributed records hold/refuse explicit recovery; no synced prior snapshot, tombstone, or CAS is required. | Gap: current reconciliation sees only current declarations, while runtime records do not yet provide the full ownership/incarnation proof, so a removed task becomes invisible and may remain running. |
| Task `kind`, `command`/`argv`, resolved effective `cwd`, agent/task `env`, task `tags`, synthesized managed environment including supervisor routing | [normalized task fields](../../../crates/agent-spec/src/spec.rs), [effective task target](../../../src/reconcile.rs), [spawn construction](../../../src/run.rs) | Change the desired launch/backend fingerprint. Dead/absent launches use the latest desired definition. A healthy older or unproven incarnation remains alive and reports `drifted` or `unknown` until an explicit task-scoped replacement. A source-path edit that changes effective cwd also belongs here. | Gap tracked by [#40](https://github.com/compoundingtech/st2/issues/40): current reconciliation adopts any healthy matching task ID without a desired/observed fingerprint or incarnation binding. `run.rs` currently passes task tags as backend spawn arguments, so they cannot be classified as pure metadata. |
| `keep`, `restart`, task `lifecycle` | [policy fields/defaults](../../../crates/agent-spec/src/spec.rs), [policy reconcile](../../../src/reconcile.rs), [restart cap](../../../src/flapping.rs) | Apply on the next relevant liveness/reconciliation decision. Do not replace a healthy process merely because policy changed. Unknown policy values make the snapshot invalid rather than silently selecting a default. | `keep` and restart policy are prospective. `adopt-only` holds dead/absent tasks; returning to `service` authorizes ordinary replacement. Healthy matching IDs remain adopted. Unknown lifecycle values fail parsing, but malformed restart duration/mode values currently fall back to defaults. |
| `retired #true` | [retired lowering](../../../crates/agent-spec/src/spec.rs), [retired plan](../../../src/reconcile.rs), [plan execution](../../../src/run.rs) | Explicitly tear down only the declaration's exact live tasks, clean eligible dead state, and never relaunch. If the same edit also removes a child, retirement of the remaining declared IDs is not proof about the now-invisible child; that child still requires exact host-local removal attribution. | Implemented for still-declared tasks. Safe removal of the retired declaration after completion remains [#42](https://github.com/compoundingtech/st2/issues/42); simultaneous child removal is not currently detected. |
| Agent `identity` | [identity/host resolution](../../../crates/agent-spec/src/discovery.rs), [bus/task IDs](../../../crates/agent-spec/src/spec.rs) | Treat as retire-old/add-new, never an in-place rename. Complete old retirement before removing the old declaration or activating a colliding new identity. | A staged old declaration with `retired #true` is supported. Editing identity in place does not retire the old runtime; the new ID can launch while old state remains. Historical naming is separately tracked by [#21](https://github.com/compoundingtech/st2/issues/21) and [#89](https://github.com/compoundingtech/st2/issues/89). |
| Task `name` or explicit `id` | [task lowering](../../../crates/agent-spec/src/spec.rs), [runtime ID resolution](../../../src/reconcile.rs) | Treat as remove-old/add-new after complete valid-owner and runtime-attribution proof; never infer that differently addressed tasks are the same incarnation. | Adding the new ID works; exact old-child teardown has the same missing ownership/incarnation-attribution gap as task removal. |
| Agent `host` | [placement resolution](../../../crates/agent-spec/src/discovery.rs), [host filter](../../../src/reconcile.rs) | Treat as an explicit cross-host migration: retire/verify old placement, then add/activate new placement. Each host acts from a complete local snapshot without synchronous coordination. | A direct edit makes the old host stop seeing the declaration and lets the new host launch; it does not prove old teardown. Exact staged publication/acknowledgement remains unresolved. |
| Agent `workspace` | [workspace/cwd model](../../../crates/agent-spec/src/spec.rs), [render target resolution](../../../src/materialize.rs), [spawn construction](../../../src/run.rs) | Ordinarily classify the edit as launch drift when it changes effective cwd, plus any independently resolved render or Resource delta. Preserve a healthy process and require explicit replacement for launch drift; do not infer a state-root move. | Current ID-only adoption hides the launch drift while materialization may target the newly resolved workspace. `adopt-only` ([#98](https://github.com/compoundingtech/st2/issues/98)) can fence replacement during a cutover but is not a mover. |
| Catalog root; effective `PTY_ROOT`; another explicitly selected sensitive root | [catalog root selection](../../../src/catalog.rs), [effective PTY root](../../../src/run.rs) | Treat as a guided state-bearing migration: freeze the affected scope, prove old-location quiescence, move/update state, and resume exactly once. | Guided catalog/PTY-root migration is specified but unimplemented in [#85](https://github.com/compoundingtech/st2/issues/85). Ordinary workspace edits are outside this boundary. |
| Invalid, partial, ambiguous, conflicting, or unreadable desired/actual state | [discovery diagnostics](../../../crates/agent-spec/src/discovery.rs), [validation](../../../src/validate.rs), [materialization failures](../../../src/materialize.rs), [runtime-list failure](../../../src/run.rs) | Block the smallest owner/render-dependency set whose completeness cannot be proved; broaden only when attribution/isolation is unprovable. Invalid remote-host input does not block valid isolated local work. Retain last-known-good ownership and never infer removal from partial/invalid input. Report affected identities, refusal, and proof scope. | Runtime-list failure skips the affected pass and render conflicts fail affected owners before writes. Discovery collects errors while continuing with valid specs, but no explicit last-known-good/removal-attribution layer yet proves safe destructive deltas. |
| Any single normalized delta | [selected reconcile](../../../src/reconcile.rs), [selected/full execution](../../../src/run.rs), [declaration watcher](../../../src/watch.rs) | Compute the smallest action set; unrelated tasks/agents have zero launch, restart, teardown, materialization, and write actions. | Exact selected reconciliation is task-scoped. The resident loop still audits the full local catalog after a declaration event, though ID reconciliation normally preserves healthy siblings. |

## Reconciliation decision table

This table applies after a complete valid snapshot has classified the field
delta. `observed` is trustworthy only when bound to the exact current runtime
incarnation; a stale or absent binding is `unknown`, never converged.

| Desired task | Actual task | Delta class | Normative action |
| --- | --- | --- | --- |
| unchanged active | healthy | source/metadata/render/policy no-op | adopt; materialize or update declaration state only when that class requires it |
| active | absent | any launch definition | launch latest desired bytes once |
| active | dead | `service` | retain diagnostics, reap eligible dead state, launch latest desired bytes once |
| active | dead or absent | `adopt-only` | hold; do not reap or launch |
| active | healthy | launch fingerprint matches bound incarnation | adopt as `converged` |
| active | healthy | fingerprint differs | adopt as `drifted`; require explicit task-scoped replacement |
| active | healthy | observed binding absent/stale/wrong incarnation | adopt as `unknown`; require proof before replacement |
| removed from complete valid owner with exact runtime attribution | healthy or dead | task-set removal | teardown/clean only the exactly attributed removed task ID |
| retired | healthy or dead | retirement | teardown/clean declared IDs; never relaunch |
| invalid, partial, ambiguous, conflicting, unreadable | any | refusal | no destructive action in the unproven scope; valid isolated scopes may proceed |

## Reporting and dry-run

Before mutation, quiet status and true dry-run must expose, per affected task or
refused proof scope:

- normalized desired task ID and pinned host;
- changed field category, without leaking secret environment values;
- action class: `no-op`, `materialize`, `launch`, `hold`, `adopt`,
  `drifted`, `replace-required`, `teardown`, `migrate`, or `refuse`;
- desired and observed launch-fingerprint identities and exact-incarnation
  binding state when available;
- actionable refusal reasons.

Unrelated siblings are omitted from routine output and receive zero actions.

The current [`UpReport`](../../../src/run.rs) and
[human CLI reporting](../../../src/main.rs) expose executed launch, teardown,
GC, hold, adoption, and errors, but not a pre-mutation semantic delta or
machine-readable fingerprint state. A true no-write plan is tracked by
[#53](https://github.com/compoundingtech/st2/issues/53);
`--materialize-only` remains mutating and is not a dry-run.

## Chosen boundaries for principal review

1. **Source-only equality:** Source movement is a no-op only when the semantic
   projection, fallback cwd, and state/resource anchors remain unchanged.
   Source-only no-op means the desired delta authorizes no action; ordinary
   actual-state healing remains independent.
2. **No-CAS removal proof:** A complete valid current owner plus exact
   host-local runtime ownership/incarnation attribution proves a child was
   removed. Ordinary runtime metadata may carry the proof. Legacy or
   unattributed children hold/refuse explicit recovery; there is no synced
   prior snapshot, tombstone, or CAS dependency.
3. **Launch/backend fingerprint:** Backend kind, lowered command/argv, resolved
   cwd, effective managed/declared environment, and task tags passed at spawn
   are included. A newly demonstrated spawn-affecting backend attribute joins
   the versioned encoding; role and Resource bindings remain metadata.
4. **Smallest fail-closed scope:** Block the smallest owner/render-dependency
   set whose completeness cannot be proved, broadening only when attribution or
   isolation itself is unknown. Valid isolated local work survives invalid
   remote input; last-known-good ownership prohibits removal from partial
   input.
5. **Migration split:** Identity and host use retire-old/add-new; task name/ID
   uses proven remove-old/add-new. Workspace changes produce launch drift plus
   independently resolved render changes. Guided state migration is reserved
   for catalog root, PTY root, or another explicitly selected sensitive root.

After these decisions and merge, external acceptance should execute a
table-driven field matrix against both PTY and exec tasks, including compact
DING, partitions, invalid snapshots, and unrelated-sibling identity proofs.
