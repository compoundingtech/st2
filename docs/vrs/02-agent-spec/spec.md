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
2. metadata and opaque extension state;
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
| Formatting, comments, KDL node order, map order | [KDL lowering](../../../crates/agent-spec/src/kdl_format.rs), [shared lowering](../../../crates/agent-spec/src/spec.rs) | If the complete normalized model and render plan are unchanged, classify `no-op`: no materialization, runtime query, launch, GC, or teardown. | Parsing is order-insensitive for agent env and maps; the resident watcher still treats a write as evidence and runs a pass. It does not yet publish a semantic-delta classification. |
| Declaration path with explicit identity and host | [path/default resolution](../../../crates/agent-spec/src/discovery.rs), [cwd resolution and bus environment](../../../src/run.rs) | A move is a source-only no-op only when task IDs, resolved cwd/workspace, and state/resource anchors are unchanged. Otherwise classify the affected path or placement boundary explicitly. | Explicit identity plus host are path-independent. A moved file can still change fallback cwd or the declaration-relative state anchor, so unconditional path no-op is not implemented. |
| Agent `type` | [job type model and lowering](../../../crates/agent-spec/src/spec.rs), [validation](../../../src/validate.rs) | Omitted and explicit `service` are equivalent. Any other value makes the snapshot invalid and authorizes no launch or destructive action. | The normalized model supports only `service`, and `validate` reports other values. Lowering currently maps every raw value to `service`, so a caller that reconciles without a clean validation gate can still treat an invalid type as runnable. |
| `role`, `resource`, descriptive task `tags` | [Agent Spec fields](../../../crates/agent-spec/src/spec.rs), [task targets](../../../src/reconcile.rs), [catalog inspection](../../../src/agents.rs) | Update observable declaration metadata. Do not change the launch fingerprint or churn a healthy task; wake only a specialized consumer when one exists. | These fields do not drive the current ID-only adoption decision. Resource inspection exists. No general field-delta event/status surface exists. |
| Unknown `harness`, `model`, `persona`, `permissions`, `transport`, `strategy`, `meta`, or provider extension | [ignored KDL fields](../../../crates/agent-spec/src/kdl_format.rs), [runner model boundary](../../../crates/agent-spec/src/spec.rs) | Keep runner-opaque. compile-agent/provider layers own interpretation and may lower a changed value into a render, task-set, launch, or metadata delta; core st2 never guesses harness semantics. | Core parsing ignores these fields. Their effects are visible only after a provider lowers them into runner-normative fields or render operations. |
| `render {}` operation, template bytes, resolved destination | [render parsing and execution](../../../src/materialize.rs), [full-pass gate](../../../src/run.rs) | Preflight all resolved claims against the complete active local fleet; reject incompatible shared ownership before any write; otherwise apply idempotently. Never restart a healthy task merely because workspace bytes changed. | Implemented for full and selected materialization, including complete-fleet conflict analysis and tracked-file safety. The watcher does not yet compute the smallest template dependency set described by root R13. |
| Add one explicit `pty`/`exec`, or add compact `ding` | [compact/explicit task lowering](../../../crates/agent-spec/src/spec.rs), [task-level reconcile](../../../src/reconcile.rs) | Launch only the uniquely missing child from desired bytes. Preserve every existing sibling PID/incarnation. | Implemented by task-ID reconciliation; generated compact DING is a normal derived exec task. |
| Remove one explicit `pty`/`exec`, or remove compact `ding` | [discovery](../../../crates/agent-spec/src/discovery.rs), [task-level reconcile](../../../src/reconcile.rs) | Only after a complete valid snapshot proves the prior task was intentionally removed, tear down that exact old child and preserve every sibling. | Gap: reconciliation sees only the current declaration and does not retain the prior task set, so a removed task becomes invisible and may remain running. The exact prior-snapshot/tombstone mechanism is unresolved. |
| Task `kind`, `command`/`argv`, resolved effective `cwd`, agent/task `env`, synthesized managed environment including supervisor routing | [normalized task fields](../../../crates/agent-spec/src/spec.rs), [effective task target](../../../src/reconcile.rs), [spawn construction](../../../src/run.rs) | Change the desired launch fingerprint. Dead/absent launches use the latest desired definition. A healthy older or unproven incarnation remains alive and reports `drifted` or `unknown` until an explicit task-scoped replacement. A workspace or source-path edit that changes effective cwd is additionally governed by the stronger migration/path boundary. | Gap tracked by [#40](https://github.com/compoundingtech/st2/issues/40): current reconciliation adopts any healthy matching task ID without a desired/observed fingerprint or incarnation binding. |
| `keep`, `restart`, task `lifecycle` | [policy fields/defaults](../../../crates/agent-spec/src/spec.rs), [policy reconcile](../../../src/reconcile.rs), [restart cap](../../../src/flapping.rs) | Apply on the next relevant liveness/reconciliation decision. Do not replace a healthy process merely because policy changed. Unknown policy values make the snapshot invalid rather than silently selecting a default. | `keep` and restart policy are prospective. `adopt-only` holds dead/absent tasks; returning to `service` authorizes ordinary replacement. Healthy matching IDs remain adopted. Unknown lifecycle values fail parsing, but malformed restart duration/mode values currently fall back to defaults. |
| `retired #true` | [retired lowering](../../../crates/agent-spec/src/spec.rs), [retired plan](../../../src/reconcile.rs), [plan execution](../../../src/run.rs) | Explicitly tear down only the declaration's exact live tasks, clean eligible dead state, and never relaunch. This is the destructive policy exception. | Implemented for still-declared tasks. Safe removal of the retired declaration after completion remains [#42](https://github.com/compoundingtech/st2/issues/42). |
| Agent `identity` | [identity/host resolution](../../../crates/agent-spec/src/discovery.rs), [bus/task IDs](../../../crates/agent-spec/src/spec.rs) | Treat as retire-old/add-new, never an in-place rename. Complete old retirement before removing the old declaration or activating a colliding new identity. | A staged old declaration with `retired #true` is supported. Editing identity in place does not retire the old runtime; the new ID can launch while old state remains. Historical naming is separately tracked by [#21](https://github.com/compoundingtech/st2/issues/21) and [#89](https://github.com/compoundingtech/st2/issues/89). |
| Task `name` or explicit `id` | [task lowering](../../../crates/agent-spec/src/spec.rs), [runtime ID resolution](../../../src/reconcile.rs) | Treat as remove-old/add-new after complete valid-catalog proof; never infer that differently addressed tasks are the same incarnation. | Adding the new ID works; exact old-child teardown has the same prior-task-set gap as task removal. |
| Agent `host` | [placement resolution](../../../crates/agent-spec/src/discovery.rs), [host filter](../../../src/reconcile.rs) | Treat as an explicit cross-host migration: retire/verify old placement, then add/activate new placement. Each host acts from a complete local snapshot without synchronous coordination. | A direct edit makes the old host stop seeing the declaration and lets the new host launch; it does not prove old teardown. Exact staged publication/acknowledgement remains unresolved. |
| Agent `workspace`; catalog root; effective `PTY_ROOT` | [workspace/cwd model](../../../crates/agent-spec/src/spec.rs), [catalog root selection](../../../src/catalog.rs), [effective PTY root](../../../src/run.rs) | Treat a state-bearing move as a migration boundary. Hold or retire affected tasks, prove the old location quiescent, move/update state, then adopt or launch exactly once at the new location. | `adopt-only` ([#98](https://github.com/compoundingtech/st2/issues/98)) is a launch/replacement fence, not a mover. Guided catalog/PTY-root migration is specified but unimplemented in [#85](https://github.com/compoundingtech/st2/issues/85). Exact workspace-move orchestration is not yet specified. |
| Invalid, partial, ambiguous, conflicting, or unreadable desired/actual state | [discovery diagnostics](../../../crates/agent-spec/src/discovery.rs), [validation](../../../src/validate.rs), [materialization failures](../../../src/materialize.rs), [runtime-list failure](../../../src/run.rs) | Fail closed before every destructive action. In particular, a parse error or incomplete snapshot cannot masquerade as task removal. Report the exact affected identities and refusal reason. | Runtime-list failure skips the pass and render conflicts fail affected owners before writes. Full discovery currently collects errors while continuing with valid specs, and no complete-version marker distinguishes a partially synchronized folder, so whole-snapshot destructive fail-closure is not yet implemented. |
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
| removed from complete valid snapshot | healthy or dead | task-set removal | teardown/clean only the exact removed task ID |
| retired | healthy or dead | retirement | teardown/clean declared IDs; never relaunch |
| invalid, partial, ambiguous, conflicting, unreadable | any | refusal | no destructive action |

## Reporting and dry-run

Before mutation, status and true dry-run must expose, per affected task:

- normalized desired task ID and pinned host;
- changed field category, without leaking secret environment values;
- action class: `no-op`, `materialize`, `launch`, `hold`, `adopt`,
  `drifted`, `replace-required`, `teardown`, `migrate`, or `refuse`;
- desired and observed launch-fingerprint identities and exact-incarnation
  binding state when available;
- every sibling proven unaffected; and
- actionable refusal reasons.

The current [`UpReport`](../../../src/run.rs) and
[human CLI reporting](../../../src/main.rs) expose executed launch, teardown,
GC, hold, adoption, and errors, but not a pre-mutation semantic delta or
machine-readable fingerprint state. A true no-write plan is tracked by
[#53](https://github.com/compoundingtech/st2/issues/53);
`--materialize-only` remains mutating and is not a dry-run.

## Principal decisions requested

1. **Path equality:** Confirm that path movement is a no-op only when the full
   normalized declaration, fallback cwd, and state/resource anchors remain
   unchanged; explicit identity alone is insufficient.
2. **Removal memory:** Select the smallest non-CAS representation that proves a
   prior task existed and that the current complete snapshot intentionally
   removed it. This draft does not choose snapshot cache, tombstone, or another
   mechanism.
3. **Fingerprint scope:** Confirm the minimal versioned encoding of backend
   kind, lowered command/argv, resolved cwd, and effective managed/declared
   environment; decide whether any backend attribute beyond those is
   spawn-affecting.
4. **Fail-closed granularity:** Decide whether any discovery error blocks every
   destructive action in the local snapshot or only actions whose complete
   owner/dependency set cannot be proven.
5. **Migration sequencing:** Confirm retire-old/add-new for identity and host,
   remove-old/add-new for task IDs, and whether workspace moves extend the
   guided state-path operation in #85 or need a narrower task-scoped workflow.

After these decisions and merge, external acceptance should execute a
table-driven field matrix against both PTY and exec tasks, including compact
DING, partitions, invalid snapshots, and unrelated-sibling identity proofs.
