# Agent Spec field-change specification

This document is a draft for principal review.
This document defines desired behavior.
A gap entry records behavior that the current product does not have.
A gap entry does not change the desired behavior.
[requirements.md](./requirements.md) defines the technical terms in this
document.

## Action and report words

A **dry-run** reports a plan.
A dry-run makes no write.
A **quiet status report** omits unaffected IDs.
The following rules apply to a quiet status report and a dry-run report.
Both reports must include only affected IDs.
Both reports must include the change class, action, refusal, and proof scope.
When applicable, both reports must include desired and observed launch
fingerprints.
When applicable, both reports must include the exact incarnation binding.
Unrelated agents and tasks must receive zero actions.

The action words have these meanings:

- `no-op` means that st2 found no normalized change.
- `metadata` means that st2 changes observed declaration data only.
- `context` means that st2 commits live data without a restart.
- `materialize` means that st2 writes the final desired workspace files.
- `add` means that st2 creates a desired local ID that is absent.
- `adopt` means that st2 keeps the current incarnation.
- `drifted` means that the observed launch fingerprint differs from the
  desired launch fingerprint.
- `unknown` means that st2 cannot prove the current state.
- `hold` means that st2 waits and makes no requested lifecycle change.
- `replace` means that st2 stops one incarnation under explicit authority.
  st2 then boots a new incarnation.
- `remove` means that st2 stops an ID that is not desired.
  st2 also cleans the ID.
- `retire` means that st2 removes a declared set.
  st2 also prevents a new launch.
- `notify` means that st2 writes a durable event.
  st2 then tries DING.
- `moved-pending` means that a moved intent is not complete.
- `moved-completed` means that st2 completed a moved intent.
- `refuse` means that st2 makes no change because validation or proof failed.

## Exhaustive normalized field rules

- **F01 Source form or path.** st2 must report format, comments, declaration
  order, or map order as `no-op` only when all normalized effects match.
  A source path move is `no-op` only when the normalized fields match.
  The render plan and task IDs must also match.
  The fallback `cwd`, state anchor, and Resource anchor must also match.
  If an effect differs, st2 must classify that effect.
  A `no-op` must not write files.
  A `no-op` must not send a notification.
  A `no-op` must not prevent st2 from healing an independently dead task.
  See [discovery and parser](../../../crates/agent-spec/src/discovery.rs).

- **F02 `identity`.** An ordinary identity change must retire the old ID.
  st2 must add the new ID.
  st2 must not infer a rename.
  Before a conflicting local add, st2 must prove exact attribution for the old
  ID.
  Status must report the old remove or retire action and the new add action.
  If ownership proof fails, status must report `refuse`.
  See [identity lowering](../../../crates/agent-spec/src/spec.rs).

- **F03 `host`.** Each supervisor must evaluate membership for its local host
  only.
  After a complete present-to-absent change, the old host must remove its local
  member.
  After a complete absent-to-present change, the new host must add its local
  member.
  Supervisors on different hosts must not coordinate this change.
  Catalog skew can cause a temporary overlap or absence.
  During catalog skew, each host must keep its local LKG state.
  See [host filter](../../../src/reconcile.rs).

- **F04 `type`.** An omitted value and `service` have the same meaning.
  st2 must `refuse` the owner component when `type` has any other value.
  st2 must refuse before a launch, write, or teardown.
  See [validation](../../../src/validate.rs).

- **F05 `role`.** A role change must update observable declaration metadata
  only.
  A role change must not change the launch fingerprint.
  A role change must not write workspace files.
  A role change must not send a notification.
  A role change must not change a healthy task.
  See [agent model](../../../crates/agent-spec/src/spec.rs).

- **F06 `workspace`.** For a survivor, st2 must commit the new `context`.
  st2 must then write one event with the old path and the new path.
  st2 must then try DING.
  The workspace change must not restart the survivor.
  The workspace change must not replace the survivor.
  An absent or dead incarnation must boot with the latest workspace.
  This boot must not send a workspace-change notification.
  A related render change must use the F08 ownership and write rules.
  st2 must combine related workspace and render changes into one event.
  An explicit task `cwd` change remains F11 launch drift.
  See [`cwd` resolution](../../../src/run.rs).

- **F07 `resource` name, `_tag`, or `uri`.** st2 must update the Resource data in
  the normalized Agent Spec.
  The update must not change the launch fingerprint.
  A committed update that is visible to a survivor must send one notification.
  A new or replaced incarnation must read the latest Resource state at boot.
  This boot must not send a Resource-change notification.
  See [Resource model](../../../crates/agent-spec/src/spec.rs).

- **F08 `render {}` operation, template, or resolved target.** Before a write,
  st2 must check ownership for all local owners.
  If owners conflict, st2 must refuse all affected owners.
  st2 must write a target only when its desired bytes differ.
  A changed target that is visible to a survivor must send one notification.
  Unchanged bytes must not send a notification.
  A deletion must have explicit desired state.
  A deletion must have ownership proof.
  A deletion must not remove a source declaration from the catalog.
  See [materializer](../../../src/materialize.rs).

- **F09 Task set: `pty`, `exec`, or compact `ding`.** An addition must launch
  only the unique missing child.
  A removal must stop only an old child with exact attribution.
  The removal must clean only that child.
  A DING task from compact `ding` must use the same rule.
  st2 must not change a sibling incarnation.
  See [task lowering](../../../crates/agent-spec/src/spec.rs).

- **F10 Task `name` or explicit `id`.** An ordinary change must remove the old
  ID.
  st2 must add the new ID.
  st2 must not infer that the two IDs identify one incarnation.
  Status must report both actions.
  If ownership proof is missing, status must report `hold` or `refuse`.
  See [task IDs](../../../src/reconcile.rs).

- **F11 Spawn inputs.** Task `kind`, `command`, `argv`, explicit `cwd`, and task
  `env` are spawn inputs.
  Agent `env`, `tags`, and `supervisor` are also spawn inputs.
  Any other field that starts a task is also a spawn input.
  st2 must put all spawn inputs in the versioned launch fingerprint.
  An absent or dead incarnation must boot with the latest spawn inputs.
  If a healthy fingerprint differs, st2 must keep the incarnation alive.
  st2 must report this condition as `drifted` or `unknown`.
  Only explicit task-scoped replacement authority can replace this
  incarnation.
  Before replacement, st2 must check the incarnation again.
  An `env` key named `PTY_ROOT` is only a task launch input.
  See [spawn construction](../../../src/run.rs).

- **F12 Future policy.** Agent or task `keep` is future policy.
  Restart `attempts`, `interval`, `delay`, and `mode` are future policy.
  Task `lifecycle` is also future policy.
  A future policy change must not change healthy work.
  st2 must `adopt` healthy work.
  `adopt-only` must put absent or dead work in `hold`.
  `service` must let st2 reconcile absent or dead work.
  st2 must `refuse` the owner component when policy is invalid.
  See [policy plan](../../../src/reconcile.rs).

- **F13 `retired` set to `#true`.** st2 must fence each declared ID.
  st2 must stop each declared ID with exact attribution.
  st2 must clean each declared ID with exact attribution.
  st2 must prevent a new launch.
  If the same change removes a child, st2 must still use F02, F09, or F10
  attribution.
  See [retirement plan](../../../src/reconcile.rs).

- **F14 Compact agent fields.** Compact `command`, `argv`, `env`, `lifecycle`,
  and `ding` must lower to the generated agent PTY and sidecar.
  The generated tasks must use F09, F11, and F12.
  Compact syntax must not create other behavior.
  See [shared lowering](../../../crates/agent-spec/src/spec.rs).

- **F15 Ignored fields.** Core st2 must ignore `harness`, `model`, `persona`,
  `permissions`, and `transport`.
  Core st2 must also ignore `strategy`, `meta`, and provider extensions.
  These fields must not change core equality, wake behavior, or actions.
  A provider can lower an ignored field into F05 through F14 fields.
  Core st2 must act only on the core fields that the provider produces.
  See [KDL boundary](../../../crates/agent-spec/src/kdl_format.rs).

- **F16 Invalid or incomplete state.** If st2 cannot read or validate a
  snapshot, it must `refuse` the affected local component.
  If snapshot data is ambiguous or conflicting, st2 must `refuse` the affected
  local component.
  If actual state is ambiguous, st2 must `refuse` the affected local component.
  st2 must keep LKG ownership for that component.
  st2 must not do a destructive action in that component.
  st2 can continue work in an independent component with complete proof.

Catalog roots and PTY roots are host runtime inputs.
They are not Agent Spec fields.
Their migration contract is outside this scope.
See [issue 85](https://github.com/compoundingtech/st2/issues/85).

## Optional local moved intent

A future map can relate one exact old address to one exact new address.
The same host projection must contain both addresses.
The map must be one-to-one.
The map must not contain a cycle.
The map must not be an alias.
The map must not be a history record.
The map must not be a global authority.
The map must identify the exact old incarnation.
The new address must not have a conflict.

st2 completes an **atomic address change** in one operation.
An atomic address change does not expose an intermediate address.
st2 can preserve a process only when it can do an atomic address change.
The change must not change process-visible data.
The change must not change the launch fingerprint.
An `identity`, `ST_AGENT`, or F11 change must use local remove-before-add.
If an operator removes a pending map, st2 must `refuse` the change.
A host change must not use moved intent.
The parser and runtime do not yet support moved intent.

## Local component execution order

For each local component, st2 must first complete the plan.
The plan must include the snapshot, difference, attribution, conflicts, and
rollback proof.
If st2 cannot complete a required proof, it must `refuse` before the first
phase.

st2 must use these phases in this order:

1. **FENCE.** st2 must prevent a launch for old, replaced, and retired IDs.
2. **REMOVE/QUIESCE.** st2 must stop exact old incarnations.
   st2 must release their ports, locks, and ownership.
3. **MATERIALIZE.** st2 must write final state for survivors and additions.
4. **ADD/BOOT.** st2 must boot missing or explicitly replaced tasks.
   st2 must use the final desired bytes.
5. **NOTIFY.** st2 must notify surviving live incarnations only.
   st2 must combine related workspace, render, and Resource changes.
6. **VERIFY/REPORT.** st2 must verify exact results.
   st2 must report exact results.
   If st2 proves a rollback, it can restore the earlier state.
   Otherwise, st2 must `hold` or `refuse`.

st2 must skip an empty phase.
A pure add has no remove phase.
A pure remove has no add phase.
A survivor-only change has a materialize or commit phase and a notify phase.
Each proven local component must progress independently.
This order must not coordinate hosts.
This order must not authorize replacement of a `drifted` incarnation.

## Current implementation gaps

- **G01 F01, F04, and F15.** A watcher pass does not receive the semantic
  difference from a source change.
  Invalid KDL `type` data can lower to `service`.
  See [parser](../../../crates/agent-spec/src/kdl_format.rs).
- **G02 F06 through F08.** The materializer checks ownership.
  The materializer avoids a write when the bytes match.
  The materializer does not select agents that depend on changed data.
  The materializer does not notify these agents after a commit.
  See [materializer](../../../src/materialize.rs).
- **G03 F02 and F09 through F13.** When st2 adopts by ID only, it does not bind
  the launch fingerprint or incarnation.
  [Issue 40](https://github.com/compoundingtech/st2/issues/40) tracks this gap.
  Removal does not prove exact attribution.
  st2 does not support remove and retire in the same change.
  Invalid restart fields use default values.
  See [model](../../../crates/agent-spec/src/spec.rs) and
  [reconciler](../../../src/reconcile.rs).
- **G04 F06 and F11.** The workspace is the current startup `cwd` fallback.
  A workspace change does not update live context.
  See [runner](../../../src/run.rs).
- **G05 F03.** st2 filters declarations for the local host.
  Exact removal proof for the old host projection does not exist.
  This contract does not require a cross-host mechanism.
  See [host filter](../../../src/reconcile.rs).
- **G06 Notifications.** Inbox and DING behavior exist.
  st2 does not write stable events for a reconcile pass.
  See [message](../../../src/message.rs) and [DING](../../../src/ding/mod.rs).
- **G07 Plan and report behavior.** st2 does not plan and commit a local
  component as one unit.
  A dry-run does not yet meet the definition above.
  [Issue 53](https://github.com/compoundingtech/st2/issues/53) tracks this gap.
  The `materialize-only` action writes files.
  See [runner](../../../src/run.rs).
- **G08 Moved intent.** The parser, status report, and executor do not support
  moved intent.
  This document does not specify the syntax.
  See [parser](../../../crates/agent-spec/src/kdl_format.rs).

## Paired acceptance matrix

- A source `no-op` test must show zero change actions.
  The test must also heal an independently dead task.
- A role or policy test must preserve live incarnations.
  An invalid value in this test must produce `refuse`.
- A workspace test must send one event to a survivor.
  An absent or dead incarnation must boot with the latest workspace.
  An explicit task `cwd` change must produce `drifted`.
- A changed render or Resource test must commit before one combined event.
  Unchanged, failed, rolled-back, periodic, new, replaced, and retired paths
  must send no event.
- A DING failure test must keep the durable event.
  A repeated event must have no additional effect.
  An inbox event must not cause another inbox event.
- Add, remove, rename, and retire tests must use exact IDs.
  The tests must include simultaneous child removal.
  Legacy or partial proof must produce `hold` or `refuse`.
- A fingerprint match must produce `adopt`.
  A fingerprint mismatch must produce `drifted`.
  An explicit replace test must prove the exact incarnation at each phase.
  The test must use the order fence, quiesce, materialize, and boot.
- Host projection tests must include overlap and absence.
  The tests must include recovery after hosts reconnect.
  Each host must reach its desired local state without one shared receipt.
- A moved-intent test must reject a cycle, conflict, or host change.
  It must use remove-before-add unless an atomic address change is possible.
- A refused component must not block an independent component.
  Every result must name the IDs, action, and proof.
