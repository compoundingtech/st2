# Agent Spec field-change requirements

## Context

This subnode defines how normalized Agent Spec changes affect running work under root
[R01, R06, R11, and R13–R19](../requirements.md). It is harness-agnostic, complete
from a valid local catalog plus host-local runtime state, and requires no CAS, lock
service, cross-host RPC, or external registry. The exhaustive field contract and
current gaps are in [spec.md](./spec.md); where the root VRS disagrees, it wins.

## Normative requirements

- **SPEC-R01 Normalize and classify before acting.** Compare the normalized
  semantic projection, render plan, resolved addresses/paths, task set, launch
  fingerprints, policy, retirement, and host membership—not source bytes. Formatting,
  ordering, comments, or source movement are `no-op` only when all effects match. A
  source-only no-op authorizes no mutation but does not suppress healing of independently
  absent/dead actual state. Ignored provider fields have no core delta until lowered.
- **SPEC-R02 Fail closed at the smallest proven local scope.** Before mutation,
  require a complete valid local owner/render-dependency/conflict component and
  exact actual-state attribution. Invalid, partial, unreadable, ambiguous, or conflicting
  input retains last-known-good ownership, never proves removal, and blocks only the
  smallest component whose isolation cannot be proved. Removal requires host-local
  metadata binding exact catalog, host, owner, task ID, and current incarnation;
  legacy/unattributed work holds. No prior synced snapshot, tombstone, or CAS is required.
- **SPEC-R03 Preserve live work unless its action is explicit.** Role metadata
  and prospective keep/restart/lifecycle policy do not churn a healthy task.
  Agent `workspace` is live context: notify a survivor; boot absent/dead with latest.
  Changed render/Resource state materializes idempotently and notifies survivors.
  Explicit task `cwd`, command/argv, kind, env, tags, supervisor-derived env, and other
  spawn inputs form a versioned launch fingerprint; visible `drifted`/`unknown` mismatch
  never authorizes surprise replacement. Unrelated work receives no action.
- **SPEC-R04 Reconcile membership and lifecycle narrowly.** Add only missing
  IDs; remove only exactly attributed old IDs; retirement tears down the declared set
  and prevents relaunch. Identity or task-name/ID changes are retire/remove-old plus
  add-new, never inferred rename. Explicit moved intent is provisional, unsupported, and
  local-host-only. Agent `host` is projection membership, never migration: old-host
  present→absent removes locally and new-host absent→present adds locally, with no shared
  transition, ordering, receipt, or proof. Skew may yield overlap/absence under local LKG.
- **SPEC-R05 Plan and execute each local component in phases.** Compute every
  action/refusal, component, proof, conflict, and rollback prerequisite before mutation.
  Execute `FENCE → REMOVE/QUIESCE → MATERIALIZE → ADD/BOOT → NOTIFY survivors →
  VERIFY/REPORT`, omitting empty phases. Conflicting add waits for exact old quiescence
  and final bytes; deletion needs explicit desire plus ownership and never targets
  canonical catalog source. On failure, roll back if proved, else hold/refuse before
  dependent phases. Components are not a global barrier; drift needs replacement authority.
- **SPEC-R06 Deliver one quiet post-commit event.** A committed workspace,
  render, or Resource update visible to a surviving incarnation persists one coalesced
  inbox event, then attempts DING. It carries a stable idempotency ID, agent/host, paths
  and class, never contents/secrets; workspace may include old/new paths. Inbox is
  durable; DING is best-effort and never rolls back the commit. No event follows no-op,
  unchanged, failed/rolled-back, periodic, new/replaced, or retired work. Notification
  cannot force restart or recurse.

## Evidence boundary

After approval, a paired external matrix must prove every
[field mapping and acceptance case](./spec.md) against public CLI behavior and isolated
PTY/exec state; unit tests alone are insufficient.
