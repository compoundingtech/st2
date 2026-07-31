# Host-local supervision — Spec

This is a concise map from the
[Host-local supervision requirements](requirements.md) to current mechanisms
and evidence. It does not define transport, remote attachment, deployment
policy, or a second Doctor health model.

## Subject selection and ownership

A resident st2 control-plane instance selects one catalog and an explicit or
locally detected host, then reconciles only that pair. A host may run separate
resident instances for other catalogs. This instance is the st2 process, not
the host's root agent or an ordinary supervisor persona. A
[`HostLock`](../../../src/host_lock.rs#L1-L67) tracks resident ownership of the
selected `(catalog, host)` subject independently from another host's slice of
the same synced catalog.

## Current mechanism and evidence

| Requirement | Current mechanism | Evidence |
| --- | --- | --- |
| HOST-R01 | Reconciliation filters every declaration through its resolved host; remote declarations are reported but not acted on. | [`reconcile`](../../../src/reconcile.rs#L106-L191); [host-placement tests](../../../tests/reconcile.rs#L135-L153) |
| HOST-R02 | One resident-instance pass discovers declarations, obtains an authoritative local session view, computes desired versus actual, and then adopts or executes the plan. A failed session listing skips the whole pass. | [reconcile pass](../../../src/run.rs#L737-L813); [reconcile-plan model](../../../src/reconcile.rs#L66-L81) |
| HOST-R03 | The resident st2 control-plane instance checks for a live owner of the same catalog and host, records its own PID, and reclaims stale ownership. | [control-plane entry](../../../src/main.rs#L1738-L1838); [`HostLock` tests](../../../src/host_lock.rs#L83-L143) |
| HOST-R04 | PTY and exec tasks survive normal or forced resident st2 control-plane instance termination and binary replacement; the successor instance preserves PID and creation identity while adopting them. | [replacement acceptance](../../../tests/nomad_survival.rs#L592-L701) |
| HOST-R05 | Normal resident st2 control-plane instance exit leaves tasks running. Teardown and retirement are separate paths that target only the selected host's declared task IDs. | [`down` and teardown](../../../src/run.rs#L987-L1056); [explicit-lifecycle acceptance](../../../tests/nomad_survival.rs#L703-L780) |
| HOST-R06 | The deterministic loop surfaces bounded crash-loop failure to the declared supervisor persona; the distinct root-agent responsibility is owned by root R04. | [crash-loop surfacing](../../../src/run.rs#L1090-L1189); [R04](../requirements.md#L43-L46) |
| HOST-R07 | Agent/task liveness and the resident st2 control-plane instance's `HostLock` record are separate observations. A replacement instance adopts matching work visible in its selected current state. Sidecar-only work does not make an otherwise unrunnable agent live. No global catalog-liveness classifier is implemented. | [replacement adoption](https://github.com/compoundingtech/st2/blob/661c88b6e50cddbdf85e8ffaca9245c46491a1e0/tests/nomad_survival.rs#L608-L717); [separate control-plane report](https://github.com/compoundingtech/st2/blob/661c88b6e50cddbdf85e8ffaca9245c46491a1e0/src/main.rs#L985-L1008); [DING-only boundary](https://github.com/compoundingtech/st2/blob/661c88b6e50cddbdf85e8ffaca9245c46491a1e0/tests/reconcile.rs#L533-L561) |
| HOST-R08 | Stable catalog-root and PTY-root path lifetime is an accepted constraint. The guided coordinated relocation operation is not implemented. | [migration contract](https://github.com/compoundingtech/st2/issues/85) |

## Catalog liveness and path lifetime

A live canonical catalog agent keeps the catalog live across resident st2
control-plane instance downtime. The absent instance is separate factual state:
it can delay convergence, but it cannot turn the continuing agent into dead
work, erase the last-applied catalog, or authorize teardown. On restart, st2
adopts matching work that is visible in the selected current state and launches
only genuinely missing work. A surviving generated DING sidecar without a
canonical agent does not satisfy the catalog-agent liveness predicate.

This is a host-local contract, not cross-root or global discovery. Under a
partition or otherwise incomplete observation, st2 cannot infer that a catalog
is globally not live merely because it cannot currently see a canonical agent.
Resident st2 control-plane instance state remains independently reportable.

While the catalog is live, or while its resident st2 control-plane instance is
running, its resolved catalog root and PTY root are stable mounted state paths.
Ordinary edits and file sync within the catalog remain allowed under the
separate complete-version and last-known-good rules; this is a path-lifetime
constraint, not an opaque-database contract. Relocating either resolved root
requires the explicit coordinated operation in
[issue #85](https://github.com/compoundingtech/st2/issues/85). Ordinary
`up`, `doctor`, and reconciliation remain scoped to the currently selected
paths and never scan arbitrary old roots.

## Partition boundary

The root [R18/R22](../requirements.md) contract requires a complete, validated,
locally applied catalog to remain authoritative through transport loss. A plain
synced catalog folder and direct KDL remain a complete operating path. Neither
R18/R22 nor last-known-good host operation requires catalog publication,
compare-and-swap, durable staging, or a content-addressed store. Optional
transactional authoring may be added, but it cannot become a prerequisite for
ordinary direct-KDL operation.

The current reconciler discovers the live catalog filesystem on each pass
([source](../../../src/run.rs#L737-L758)), while validation is a separate
read-only command ([source](../../../src/validate.rs#L1-L24)). st2 does not yet
identify and order complete candidate versions or retain a durable
last-known-good receipt. That implementation gap does not prescribe CAS or a
content-addressed activation mechanism.

Peer reachability is not currently a reconciler input. This is consistent with
peer absence being neutral, but the declaration shape for an explicit local
operation that depends on a peer or source is not yet specified.

## Open questions

- What identifies a complete candidate catalog and orders it after the locally
  applied version?
- What durable receipt lets a replacement control plane recover the
  last-known-good version after interruption without making a transactional
  authoring path mandatory?
- How does a declaration express a local operation's dependency on a peer or
  source without turning peer presence into general health?
- [`HostLock` acquisition](../../../src/host_lock.rs#L26-L48) is currently
  check-then-write rather than an atomic create. What ownership primitive
  closes simultaneous first-start races while retaining host-scoped
  stale-owner recovery?
