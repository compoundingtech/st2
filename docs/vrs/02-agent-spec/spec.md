# st2 Agent Spec field-change conformance specification

This draft defines desired st2 conformance behavior and st2-specific gaps. It
does not redefine the Agent Spec. A general field-change rule in this document
is proposed until the canonical evals specification and proof corpus adopt it.

## Rules common to every field

Compare normalized effects and preserve unrelated work. Prove exact ownership
before destructive action. Each host acts only on its local projection. Fence
and remove before a conflicting add. Notify surviving agents only after commit.

A dry-run makes no writes. Dry-run and quiet status name only affected IDs,
with the change, action or refusal, and proof scope. They include desired and
observed fingerprints and the exact incarnation when relevant. `hold` waits
without the requested lifecycle change. `refuse` makes no change because
validation or proof failed.

Each **Authoring** link points to the canonical evals Agent Spec at exact commit
`b3cd5fbd98c11179a4555d0f9bbccfe98351a734`, which is pinned to st2
`9887b28`. It is the authoring authority, not proof of current st2 runtime
behavior. **st2 source** and **Evidence** links show this implementation on the
PR base.

## Field rules

<h3 id="f01">F01 Source form or path</h3>

Formatting, comments, order, and a source path change are `no-op` only when all
normalized fields, render plans, task IDs, fallback `cwd`, state anchors, and
Resource anchors match. Otherwise, classify the changed effect. A `no-op`
writes and notifies nothing but does not block healing of absent or dead work.

Authoring: [pinned discovery, identity, and host][evals-discovery]. st2 source:
[KDL parser](../../../crates/agent-spec/src/kdl_format.rs). Evidence:
[discovery](../../../crates/agent-spec/src/discovery.rs).

<h3 id="f02">F02 <code>identity</code></h3>

Retire the old identity and add the new identity. Do not infer a rename. Prove
exact old ownership before a conflicting add. Report both actions, or refuse
when proof is missing.

Authoring: [pinned discovery and identity][evals-discovery]. st2 source:
[`AgentSpec::identity`](../../../crates/agent-spec/src/spec.rs). Evidence:
[reconciliation](../../../src/reconcile.rs).

<h3 id="f03">F03 <code>host</code></h3>

Each supervisor evaluates only local membership. A complete present-to-absent
change removes locally; absent-to-present adds locally. This is not migration,
and the hosts do not coordinate. Skew can cause overlap or absence. Each host
keeps its local last-known-good ownership.

Authoring: [pinned discovery and host][evals-discovery]. st2 source:
[`AgentSpec::host`](../../../crates/agent-spec/src/spec.rs). Evidence:
[host filtering](../../../src/reconcile.rs).

<h3 id="f04">F04 <code>type</code></h3>

An omitted value and `service` have the same effect. Any other value refuses
the owner component before launch, write, or teardown.

Authoring: [pinned complete declaration][evals-fields]. st2 source:
[`JobType` and `RawSpec::job_type`](../../../crates/agent-spec/src/spec.rs).
Evidence: [validation](../../../src/validate.rs).

<h3 id="f05">F05 <code>role</code></h3>

Update observable declaration metadata only. Do not change the fingerprint,
workspace files, notification state, or a healthy task.

Authoring: [pinned complete declaration][evals-fields]. st2 source:
[`AgentSpec::role`](../../../crates/agent-spec/src/spec.rs). Evidence:
[KDL lowering](../../../crates/agent-spec/src/kdl_format.rs).

<h3 id="f06">F06 <code>workspace</code></h3>

For a healthy survivor, keep the process and commit the new live context. After
commit, write one durable event with the old and new paths, then try
transport-neutral DING through the configured adapter. Do not restart or
replace. Absent or dead work boots with the latest workspace and no notification.

Related render changes use F08 and join the same event. Explicit task `cwd` is
F11 launch drift.

Authoring: [pinned complete declaration][evals-fields]. st2 source:
[`AgentSpec::workspace`](../../../crates/agent-spec/src/spec.rs). Evidence:
[`cwd` resolution](../../../src/run.rs).

<h3 id="f07">F07 Resource <code>name</code>, <code>_tag</code>, or <code>uri</code></h3>

Update typed Resource data without changing the launch fingerprint. Notify a
survivor once after commit. New or replaced work reads the latest state at boot
and gets no change notification.

Authoring gap: the [pinned supported-field list][evals-supported-fields] and
st2 `9887b28` predate Resource bindings. Current st2 source:
[`Resource`](../../../crates/agent-spec/src/spec.rs). Evidence:
[declared Resource projection](../../../src/agents.rs).

<h3 id="f08">F08 <code>render {}</code> operation, template, or resolved target</h3>

Prove ownership for every affected local owner before writing. Conflicts refuse
all affected owners. Write only changed bytes, then notify survivors that can
see the committed target. Unchanged bytes do not notify. Deletion needs explicit
desired state and ownership and never removes a catalog source declaration.

Authoring: [pinned render contract][evals-render]. st2 implementation and
evidence: [materializer](../../../src/materialize.rs).

<h3 id="f09">F09 Task set: <code>pty</code>, <code>exec</code>, or compact <code>ding</code></h3>

Add only the unique missing child. Remove and clean only an old child with exact
ownership proof. A compact DING task uses the same rule. Do not change siblings.

Authoring: [pinned compact and explicit tasks][evals-tasks]. st2 source:
[`Task` and `TaskKind`](../../../crates/agent-spec/src/spec.rs). Evidence:
[task reconciliation](../../../src/reconcile.rs).

<h3 id="f10">F10 Task <code>name</code> or explicit <code>id</code></h3>

Remove the exact old ID and add the new ID. Do not infer one incarnation.
Report both actions, or `hold` or `refuse` when ownership proof is missing.

Authoring: [pinned explicit tasks][evals-tasks]. st2 source:
[`Task::name` and `Task::id`](../../../crates/agent-spec/src/spec.rs). Evidence:
[task reconciliation](../../../src/reconcile.rs).

<h3 id="f11">F11 Spawn inputs</h3>

Task `kind`, `command`, `argv`, explicit `cwd`, and task `env`, plus agent
`env`, `tags`, `supervisor`, and any other start input form the versioned launch
fingerprint. Absent or dead work boots with the latest inputs.

A healthy mismatch stays alive as `drifted` or `unknown`; report both
fingerprints. Command drift stays visible and does not restart automatically.
Replacement needs task-scoped authority and a fresh exact-incarnation check.
An `env` key named `PTY_ROOT` is only a task launch input.

Authoring: [pinned tasks][evals-tasks] and [environment][evals-environment]. The
[pinned explicit-task list][evals-task-fields] and st2 `9887b28` predate `argv`.
Current st2 source: [`AgentSpec` and `Task`](../../../crates/agent-spec/src/spec.rs).
Evidence: [spawn construction](../../../src/run.rs).

<h3 id="f12">F12 Future policy</h3>

Agent or task `keep`, restart `attempts`, `interval`, `delay`, and `mode`, and
task `lifecycle` are future policy. Adopt healthy work. `adopt-only` holds absent
or dead work; `service` reconciles it normally. Invalid policy refuses the owner.

Authoring: [pinned complete declaration][evals-fields]. The
[pinned explicit-task list][evals-task-fields] and st2 `9887b28` predate task
`lifecycle`. Current st2 source:
[`Restart` and `TaskLifecycle`](../../../crates/agent-spec/src/spec.rs).
Evidence: [policy planning](../../../src/reconcile.rs).

<h3 id="f13">F13 <code>retired #true</code></h3>

Fence, stop, and clean every declared ID with exact ownership proof, and prevent
relaunch. A child removed in the same change still uses F02, F09, or F10 proof.

Authoring: [pinned complete declaration][evals-fields]. st2 source:
[`AgentSpec::retired`](../../../crates/agent-spec/src/spec.rs). Evidence:
[retirement planning](../../../src/reconcile.rs).

<h3 id="f14">F14 Compact agent fields</h3>

Compact `command`, `argv`, `env`, `lifecycle`, and `ding` lower to the generated
agent PTY and sidecar. The tasks use F09, F11, and F12. Compact syntax adds no
other behavior.

Authoring: [pinned compact tasks][evals-tasks]. That document and st2 `9887b28`
predate compact `argv` and `lifecycle`. Current st2 source:
[KDL fields](../../../crates/agent-spec/src/kdl_format.rs). Evidence:
[`RawSpec` lowering](../../../crates/agent-spec/src/spec.rs).

<h3 id="f15">F15 Provider and ignored fields</h3>

Core st2 ignores `harness`, `model`, `persona`, `permissions`, `transport`,
`strategy`, `meta`, and provider extensions. They do not change core equality,
wake behavior, or actions. Providers may lower them into F05 through F14; core
acts only on that concrete output.

Authoring: [pinned complete declaration][evals-fields]. st2 source:
[KDL field boundary](../../../crates/agent-spec/src/kdl_format.rs). Evidence:
[`RawSpec` lowering](../../../crates/agent-spec/src/spec.rs).

<h3 id="f16">F16 Invalid or incomplete state</h3>

Refuse the smallest affected local component when desired or actual state is
unreadable, invalid, ambiguous, or conflicting. Keep last-known-good ownership
and perform no destructive action there. Independent proved work can continue.

Authoring: [pinned validation, health, and lifecycle][evals-lifecycle]. st2
source: [`RawSpec` and `AgentSpec`](../../../crates/agent-spec/src/spec.rs).
Evidence: [validation](../../../src/validate.rs) and
[reconciliation](../../../src/reconcile.rs).

Catalog and PTY roots are host runtime inputs, not Agent Spec fields. Their
migration contract is outside this VRS. See
[#85](https://github.com/compoundingtech/st2/issues/85).

## Unsupported moved intent

A possible future same-host map could bind one exact old address to one exact
new address. It would need one-to-one, acyclic mapping, exact old-incarnation
proof, and no destination conflict. It is not an alias, history, global
authority, or host migration. Only an atomic address change with no
process-visible or fingerprint change could preserve a process. `identity`,
`ST_AGENT`, F11, and host changes remove before add. Removing a pending map
would refuse. The parser and runtime do not support moved intent.

Source: [KDL parser](../../../crates/agent-spec/src/kdl_format.rs).

## Execution order

Plan the snapshot, normalized difference, ownership, conflicts, and rollback.
Refuse before mutation when a required proof is missing. Then omit empty phases
from this fixed order:

1. **FENCE:** prevent launch of old, replaced, and retired IDs.
2. **REMOVE/QUIESCE:** stop exact old incarnations and release their resources.
3. **MATERIALIZE:** write final state for survivors and additions.
4. **ADD/BOOT:** boot missing or explicitly replaced work from final bytes.
5. **NOTIFY:** notify survivors after commit and coalesce related changes.
6. **VERIFY/REPORT:** verify exact results; roll back only when proved,
   otherwise hold or refuse.

Independent proved components progress separately. The order does not
coordinate hosts or authorize replacement of drifted work.

## Current implementation gaps

- **G01, F01/F04/F15:** watcher passes lack semantic differences, and invalid
  KDL `type` can lower to `service`. [Parser](../../../crates/agent-spec/src/kdl_format.rs)
- **G02, F06-F08:** render ownership and idempotent writes exist, but dependency
  targeting and post-commit notification do not. [Materializer](../../../src/materialize.rs)
- **G03, F02/F09-F13:** ID-only adoption lacks fingerprint and incarnation
  binding. Exact removal and simultaneous remove/retire are absent. Malformed
  restart fields use defaults. See [#40](https://github.com/compoundingtech/st2/issues/40),
  the [model](../../../crates/agent-spec/src/spec.rs), and
  [reconciler](../../../src/reconcile.rs).
- **G04, F06/F11:** workspace is the current startup `cwd` fallback, not live
  context. [Runner](../../../src/run.rs)
- **G05, F03:** host filtering exists, but exact old-projection removal does
  not. No cross-host mechanism is required. [Host filter](../../../src/reconcile.rs)
- **G06, notifications:** inbox and DING exist, but reconciliation writes no
  stable change event. [Message](../../../src/message.rs) and
  [DING](../../../src/ding/mod.rs)
- **G07, planning/reporting:** st2 has no component transaction or true dry-run;
  `materialize-only` writes. See [#53](https://github.com/compoundingtech/st2/issues/53)
  and [runner](../../../src/run.rs).
- **G08, moved intent:** parser, status, and executor support are absent; syntax
  is unspecified. [Parser](../../../crates/agent-spec/src/kdl_format.rs)

## Acceptance cases

- Source `no-op` changes nothing while an independently dead task still heals.
- Role and policy changes preserve healthy work; invalid values refuse.
- A workspace survivor gets one event. Absent or dead work boots the latest
  workspace. Explicit task `cwd` drift stays visible.
- Changed render or Resource state commits before one combined event. Unchanged,
  failed, rolled-back, periodic, new, replaced, and retired paths emit none.
- DING failure retains the event. Replay has no extra effect, and inbox events
  do not cause inbox events.
- Add, remove, rename, and retirement affect exact IDs, including simultaneous
  child removal. Legacy or partial proof holds or refuses.
- Matching fingerprints adopt. Mismatches drift. Explicit replacement proves
  the exact incarnation through fence, quiesce, materialize, and boot.
- Host projections converge after overlap, absence, and reconnection without a
  shared receipt.
- Moved intent rejects cycles, conflicts, and host changes. It removes before
  add unless a future atomic address change can preserve the process.
- A refused component does not block independent work. Every result names the
  affected IDs, action, and proof scope.

[evals-discovery]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#discovery-identity-and-host
[evals-fields]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#complete-declaration-shape
[evals-supported-fields]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#L73-L95
[evals-tasks]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#compact-and-explicit-tasks
[evals-task-fields]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#L147-L151
[evals-environment]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#environment-and-expansion
[evals-render]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#render-contract
[evals-lifecycle]: https://github.com/compoundingtech/evals/blob/b3cd5fbd98c11179a4555d0f9bbccfe98351a734/AGENT-SPEC.md#validation-health-and-lifecycle
