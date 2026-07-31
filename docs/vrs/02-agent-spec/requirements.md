# Agent Spec field-change requirements

## Technical terms

- A **VRS node** is one part of the st2 requirements and specification.
- A **normalized Agent Spec** is the result after st2 parses a declaration and
  applies default values.
- A **core field** is an Agent Spec field that core st2 uses.
- **Lower** means change a field into one or more core fields.
- A **semantic projection** is the set of effects from a normalized Agent Spec.
- A **render plan** lists the final files and bytes that st2 must write.
- A **catalog** is the set of Agent Spec declarations that st2 reads.
- **Runtime state** contains the local processes, files, and ownership records.
- A **local component** is a group of agents that share an owner, a rendered
  file, or another conflict.
- An **incarnation** is one live generation of a task.
- **Attribution** is proof that binds an object to its exact catalog, host,
  owner, task ID, and incarnation.
- A **launch fingerprint** identifies all inputs that start one version of a
  task.
- A **host projection** is the set of declarations that one host selects.
- **Catalog skew** occurs when two hosts have different catalog versions.
- A **host migration** is a coordinated move of a process between hosts.
- **Last-known-good state (LKG)** is the latest local state for which st2 proved
  ownership.
- A **survivor** is a healthy incarnation that st2 does not replace.
- A **fence** prevents st2 from starting an ID while st2 changes that ID.
- **Quiesce** means stop an incarnation and release its ports, locks, and
  ownership.
- A **rollback** restores the state that existed before a failed change.
- A **snapshot** is the complete catalog and runtime input for one st2 pass.
- A **Resource** is a typed binding in an Agent Spec declaration.
- A **provider** changes provider fields into concrete core fields.
- A **materializer** is the st2 component that writes desired files.
- A **sidecar** is a support task that st2 creates from compact syntax.
- A **parser** reads a catalog declaration and creates a normalized Agent Spec.
- A **watcher** detects a catalog file change.
- A **reconcile pass** compares desired state with actual state and then applies
  allowed changes.
- A **reconciler** runs a reconcile pass.
- An **executor** applies planned actions.
- **Replacement authority** is explicit permission to replace one exact
  incarnation.
- A **destructive action** removes a file or stops, retires, or replaces work.
- **DING** is a best-effort delivery attempt.
  It tells an agent to read its inbox.
  st2 does not guarantee delivery.
  A configured adapter can use terminal input or a post-turn hook.
  This contract does not require terminal input or harness-specific behavior.
- A **PTY task** is an interactive terminal task.
- An **exec task** is a non-interactive task.
- The **CLI** is the st2 command-line interface.
- **KDL** is one catalog declaration format.
- An **anchor** is the declaration folder that owns state or Resources.
- An **idempotency ID** identifies one event. A repeated event with this ID has
  no additional effect.
- **Moved intent** is a possible future map from one old local address to one
  new local address.
- An **address** identifies one Agent Spec object without ambiguity.
- A **no-op** reports that st2 found no normalized change.
- A **hold** waits and makes no requested lifecycle change.
- A **refusal** makes no change because validation or proof failed.
- **Drifted** reports that a launch fingerprint differs.
- **Unknown** reports that st2 cannot prove actual state.
- **Materialize** means write the final desired files.
- **Compare-and-swap (CAS)** is a write rule. The write succeeds only when the
  current value equals an expected value.

## Context

This VRS node defines how normalized Agent Spec changes affect work on one
host.
It supports root
[R01, R06, R11, and R13 through R19](../requirements.md).
The same contract applies to all tools that run an agent or test.
A valid local catalog and host-local runtime state provide all required input.
The contract does not require CAS, a lock service, a cross-host call, or an
external registry.
[spec.md](./spec.md) lists all field rules and current gaps.
If this node conflicts with the root VRS, the root VRS has authority.

## Normative requirements

- **SPEC-R01 Normalize and classify before an action.** st2 must compare
  normalized effects before it acts.
  - st2 must compare the normalized semantic projection.
  - st2 must compare the render plan and all resolved addresses and paths.
  - st2 must compare the task set and each launch fingerprint.
  - st2 must compare policy, retirement state, and host membership.
  - st2 must not use source bytes as the only comparison.
  - st2 can report `no-op` for format, order, comment, or source-path changes
    only when all normalized effects match.
  - A source `no-op` must not authorize a write.
  - A source `no-op` must not authorize a notification.
  - A source `no-op` must not prevent st2 from healing independently absent or
    dead work.
  - Provider fields do not create a core change until a provider lowers them
    into core fields.

- **SPEC-R02 Stop changes when local proof is incomplete.** st2 must divide
  local work into local components.
  - Before st2 changes a component, it must validate all input for that
    component.
  - st2 must prove the current owner of each affected process and file.
  - If st2 cannot complete this proof, it must not change that component.
  - st2 can change an independent component when proof for that component is
    complete.
  - If input is partial or unreadable, st2 must keep LKG ownership.
  - If input is ambiguous or conflicting, st2 must keep LKG ownership.
  - Incomplete input must not prove that st2 can remove an ID.
  - Removal proof must bind the catalog, host, owner, task ID, and current
    incarnation.
  - st2 must put work from an earlier version in `hold` when it has no
    attribution.
  - st2 must put other unattributed work in `hold`.
  - st2 must not require a previous synchronized snapshot.
  - st2 must not require a deletion record.
  - st2 must not require CAS.

- **SPEC-R03 Preserve live work unless an explicit action changes it.** A role
  change must not change a healthy task.
  - A future `keep`, restart, or `lifecycle` policy must not change a healthy
    task.
  - The agent `workspace` field supplies live context.
  - If a workspace change is visible to a survivor, st2 must notify that
    survivor.
  - If an incarnation is absent or dead, st2 must boot it with the latest
    workspace.
  - A boot for an absent or dead incarnation must not send this notification.
  - st2 must write changed render or Resource state only when the desired bytes
    differ.
  - If a changed render or Resource is visible to a survivor, st2 must notify
    that survivor.
  - F11 spawn inputs must form the launch fingerprint.
  - If a healthy launch fingerprint differs, st2 must report `drifted` or
    `unknown`.
  - A fingerprint difference must not authorize replacement.
  - Unrelated work must receive no action.

- **SPEC-R04 Change membership and lifecycle only for exact IDs.** st2 must add
  only missing IDs.
  - st2 must remove only old IDs with exact attribution.
  - Retirement must stop the declared set.
  - Retirement must prevent a new launch.
  - An identity change must remove the old ID.
  - An identity change must add the new ID.
  - A task name or task ID change must remove the old ID.
  - A task name or task ID change must add the new ID.
  - st2 must not infer a rename from these changes.
  - Moved intent is provisional.
  - st2 does not support moved intent.
  - Moved intent is limited to one host.
  - The agent `host` field defines host projection membership.
  - A `host` change is not a migration.
  - The old host must remove its local member after a complete
    present-to-absent change.
  - The new host must add its local member after a complete
    absent-to-present change.
  - The two hosts must not require a shared transition, order, receipt, or
    proof.
  - Catalog skew can cause a temporary overlap or absence.
  - During catalog skew, each host must use its local LKG state.

- **SPEC-R05 Plan and run each local component in phases.** st2 must plan all
  actions and refusals before it changes a component.
  - The plan must include all proof, conflicts, and rollback conditions.
  - st2 must use this order: `FENCE`, `REMOVE/QUIESCE`, `MATERIALIZE`,
    `ADD/BOOT`, `NOTIFY`, and `VERIFY/REPORT`.
  - st2 must omit an empty phase.
  - If an add conflicts with an old incarnation, st2 must first prove that the
    old incarnation is quiescent.
  - A conflicting add must use the final desired bytes.
  - A deletion must have explicit desired state.
  - A deletion must have ownership proof.
  - A deletion must not remove a source declaration from the catalog.
  - If st2 proves a rollback, it can use that rollback after a failure.
  - If st2 cannot prove a rollback, it must `hold` or `refuse` before a
    dependent phase.
  - An independent component must not wait for another component.
  - A `drifted` incarnation needs explicit replacement authority.

- **SPEC-R06 Send one quiet event after a commit.** This rule applies when a
  committed workspace, render, or Resource change is visible to a survivor.
  - st2 must write one durable inbox event.
  - st2 must combine related changes into this one event.
  - After st2 writes the event, it must try DING.
  - The event must contain a stable idempotency ID.
  - The event must contain the agent, host, changed path, and change class.
  - A workspace event can contain the old path and the new path.
  - The event must not contain file contents or secrets.
  - A DING failure must not remove the event.
  - A DING failure must not roll back the commit.
  - st2 must not send an event for `no-op` or unchanged work.
  - st2 must not send an event for a failed or rolled-back change.
  - st2 must not send an event for a periodic pass.
  - st2 must not send an event for new, replaced, or retired work.
  - A notification must not cause a restart.
  - A notification must not cause another notification.

## Evidence boundary

After approval, an external test matrix must test every
[field rule and acceptance case](./spec.md).
The matrix must use public CLI behavior and isolated PTY and exec state.
Unit tests alone do not prove this contract.
