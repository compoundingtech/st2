# Agent Spec field-change specification

Status: draft for principal review; desired behavior is normative and “gap” is not.

## Action and reporting vocabulary

Quiet status and true no-write dry-run report only affected IDs, changed class,
action/refusal, proof scope, desired/observed fingerprint, and exact incarnation
binding when relevant. Actions: `no-op`, `metadata`, `context`, `materialize`, `add`,
`adopt`, `drifted`, `unknown`, `hold`, `replace`, `remove`, `retire`, `notify`,
`moved-pending/completed`, `refuse`. Unrelated siblings receive zero actions.

## Exhaustive normalized field mapping

- **F01 Source form/path** — Formatting, comments, declaration/map order, and a
  path move are `no-op` only when normalized fields, render plan, task IDs, fallback
  cwd, and state/Resource anchors match. Otherwise classify resulting effects.
  `no-op` writes/notifies nothing; actual healing remains independent.
  [Discovery/parser](../../../crates/agent-spec/src/discovery.rs)
- **F02 `identity`** — Ordinary change is retire-old/add-new; no inferred rename.
  Exact old attribution precedes conflicting local add. Status shows
  remove/retire and add, or refusal. [Identity lowering](../../../crates/agent-spec/src/spec.rs)
- **F03 `host`** — Each supervisor evaluates only local membership: complete
  present→absent removes locally; absent→present adds locally. Hosts model no
  relationship/ordering; skew may overlap/omit without interrupting LKG.
  [Host filter](../../../src/reconcile.rs)
- **F04 `type`** — Omitted and `service` are equal. Any other value refuses the
  owner scope before launch, write, or teardown. [Validation](../../../src/validate.rs)
- **F05 `role`** — Metadata only: update observable declaration state; no
  launch fingerprint, write, notification, or healthy-task churn.
  [Agent model](../../../crates/agent-spec/src/spec.rs)
- **F06 `workspace`** — For a survivor, commit `context`, enqueue one event with
  old/new paths, then DING; never restart/replace. Absent/dead boots latest without
  notification. Render changes retain preflight/materialization and coalesce.
  Explicit task `cwd` remains F11 launch drift. [cwd resolution](../../../src/run.rs)
- **F07 `resource` name/`_tag`/`uri`** — Update typed declaration context
  without launch churn. A committed change visible to a survivor notifies once;
  new/replaced boot observes latest state without notification.
  [Resource model](../../../crates/agent-spec/src/spec.rs)
- **F08 `render {}` operation/template/resolved target** — Complete-fleet
  ownership preflight precedes writes; conflicts refuse all affected owners. Apply
  idempotently; changed survivor targets notify once, unchanged bytes do not.
  Deletion needs desired-state plus ownership and never targets catalog source.
  [Materializer](../../../src/materialize.rs)
- **F09 Task set (`pty`, `exec`, compact `ding`)** — Addition launches only the
  uniquely missing child. Removal tears down/cleans only an exactly attributed
  old child; derived DING follows the same rule. Sibling incarnations remain.
  [Task lowering](../../../crates/agent-spec/src/spec.rs)
- **F10 Task `name`/explicit `id`** — Ordinary change is proven
  remove-old/add-new; never inferred same incarnation. Status reports both
  actions or holds/refuses missing ownership proof. [Task IDs](../../../src/reconcile.rs)
- **F11 Task `kind`, `command`/`argv`, explicit `cwd`, task/agent `env`, `tags`,
  and agent `supervisor`** — These lower into the versioned launch/backend
  fingerprint. Absent/dead boots latest; healthy mismatch stays alive as
  `drifted`/`unknown` until explicit task-scoped replacement with incarnation
  recheck. An env key named `PTY_ROOT` is only task launch input.
  [Spawn construction](../../../src/run.rs)
- **F12 Agent/task `keep`, restart `attempts`/`interval`/`delay`/`mode`, task
  `lifecycle`** — Prospective policy only; healthy work is adopted. `adopt-only`
  holds dead/absent; `service` resumes ordinary reconcile. Invalid policy refuses.
  [Policy plan](../../../src/reconcile.rs)
- **F13 `retired #true`** — Fence, teardown/clean exactly declared IDs, never
  relaunch. A simultaneously removed child still needs F02/F09/F10 attribution.
  [Retirement plan](../../../src/reconcile.rs)
- **F14 Compact agent `command`/`argv`/`env`/`lifecycle`/`ding`** — Lower to the
  generated agent PTY/sidecar, then inherit F09/F11/F12 exactly; compact syntax
  creates no separate semantics. [Shared lowering](../../../crates/agent-spec/src/spec.rs)
- **F15 Ignored `harness`, `model`, `persona`, `permissions`, `transport`,
  `strategy`, `meta`, provider extension** — No core equality/wake/action. Providers
  may lower them to F05–F14; core acts only on concrete output.
  [KDL boundary](../../../crates/agent-spec/src/kdl_format.rs)
- **F16 Invalid/partial/ambiguous/conflicting/unreadable snapshot or actual
  state** — `refuse` the smallest unproven owner/render-conflict component,
  retain last-known-good ownership, and perform no destructive action there;
  valid isolated components proceed.

Catalog/PTY roots are host-runtime inputs, not Agent Spec fields; their migration
contract is out of scope ([#85](https://github.com/compoundingtech/st2/issues/85)).

## Optional local moved intent

A future mapping may relate one fully qualified old address to one new address in the
same host projection. It is one-to-one, acyclic, not alias/history/global authority,
and requires exact old incarnation plus no destination conflict. Preserve a process
only through atomic re-address with no process-visible/launch change; identity/
`ST_AGENT` or F11 changes therefore use local remove-before-add. Removing a pending
mapping fails closed. Host changes are excluded; syntax and runtime remain unsupported.

## Local component execution order

```text
plan complete snapshot/diff/attribution/conflicts/rollback; refuse if unproven
FENCE old/replaced/retired IDs against relaunch
REMOVE/QUIESCE exact old incarnations; release ports/locks/ownership
MATERIALIZE final survivor/addition state
ADD/BOOT missing or explicitly replaced tasks from final desired bytes
NOTIFY surviving live incarnations only; coalesce workspace/render/Resource
VERIFY/REPORT exact results; rollback where proven, otherwise hold/refuse
```

Skip empty phases: pure add has no remove, pure remove no add, and survivor-only does
materialize/commit→notify. Order is per proven local component; independent components
progress separately. It never coordinates hosts or authorizes drift replacement.

## Current implementation gaps

- **G01 F01/F04/F15:** watcher passes lack semantic deltas; invalid KDL `type` can
  lower to `service`. [parser](../../../crates/agent-spec/src/kdl_format.rs)
- **G02 F06–F08:** render ownership/idempotence exists; dependency targeting and
  post-commit notification do not. [materializer](../../../src/materialize.rs)
- **G03 F02/F09–F13:** ID-only adoption lacks fingerprint/incarnation binding
  ([#40](https://github.com/compoundingtech/st2/issues/40)); removal attribution
  and simultaneous remove/retire are absent; malformed restart fields default.
  [model](../../../crates/agent-spec/src/spec.rs) · [reconciler](../../../src/reconcile.rs)
- **G04 F06/F11:** workspace is current startup-cwd fallback, not live-churn
  authority. [runner](../../../src/run.rs)
- **G05 F03:** local host filtering exists; exact old-projection removal proof does
  not; no cross-host mechanism is required. [host filter](../../../src/reconcile.rs)
- **G06 Notifications:** inbox/DING exist without stable reconcile events.
  [message](../../../src/message.rs) · [DING](../../../src/ding/mod.rs)
- **G07 Planning/reporting:** no component transaction or true dry-run
  ([#53](https://github.com/compoundingtech/st2/issues/53)); materialize-only writes.
  [runner](../../../src/run.rs)
- **G08 Moved:** no parser/status/executor; syntax unspecified. [parser](../../../crates/agent-spec/src/kdl_format.rs)

## Paired acceptance matrix

- source no-op performs zero delta actions while an independently dead task heals;
- role/policy changes preserve live incarnations; invalid values refuse;
- workspace survivor gets one event, absent/dead boots latest, task-cwd drifts;
- changed render/Resource commits then coalesces one event; unchanged, failed,
  rolled-back, periodic, new/replaced, and retired paths emit none;
- DING failure retains the event; replay is harmless; inbox cannot recurse;
- add/remove/rename/retire touch exact IDs, including simultaneous child removal;
  legacy/partial proof holds/refuses;
- fingerprint match adopts, mismatch drifts, explicit replace proves
  fence→quiesce→materialize→boot on the exact incarnation;
- host projections converge under overlap/absence/partition recovery without receipt;
- moved rejects cycles/conflicts/host changes and remove-before-adds unless atomic;
- refused components do not block isolated work; every result names IDs/action/proof.
