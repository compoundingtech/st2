# Host-local supervision — Requirements

Host-local supervision is one machine's application of the root
[st2 vision](../vision.md). It refines host placement, root supervision, and
control-plane replacement in
[R03](../requirements.md#L38-L39),
[R04](../requirements.md#L43-L46), and
[R11](../requirements.md#L65-L70). Partition safety remains a fleet-level
contract in [R18/R22](../requirements.md); this sub-VRS does not redefine it.

## Requirements

- **HOST-R01 One local subject:** Each resident st2 control-plane instance
  selects one catalog and one host and reconciles only that pair. One host may
  run separate resident instances for other catalogs. The instance is the st2
  process, not the host's root agent or an ordinary supervisor persona. Another
  host's declarations and runtime records are outside the selected subject.
- **HOST-R02 Local desired-versus-actual convergence:** The deterministic
  resident st2 control-plane instance compares declarations pinned to the
  selected host with that host's observed task state, adopts matching live
  work, and starts only genuinely missing work.
- **HOST-R03 One control-plane writer:** At most one resident st2 control-plane
  instance may reconcile a given catalog and host at a time. A different host
  running an instance against the same synced catalog is a different subject.
- **HOST-R04 Independent task lifetime:** Stopping, killing, or replacing the
  resident st2 control-plane instance does not stop or replace running agent
  tasks. A successor instance adopts surviving tasks without duplicating them.
- **HOST-R05 Explicit destructive lifecycle:** Resident st2 control-plane
  instance absence, restart, or loss of a transport peer is not teardown
  authority. Local tasks are stopped only by an explicit local lifecycle
  decision, including a locally applied retirement declaration or teardown
  command.
- **HOST-R06 Intelligent local escalation:** The selected host's root agent
  observes local health, performs bounded recovery, and escalates unresolved
  failures without turning unavailable peer state into a fleet-health verdict.
  This agent role is distinct from the resident st2 control-plane instance and
  from an ordinary supervisor persona.
- **HOST-R07 Catalog liveness is not control-plane-instance liveness:** A
  catalog remains live while any canonical agent belonging to it is running,
  including while its resident st2 control-plane instance is stopped,
  restarting, or unavailable. Instance state is reported separately. Its
  absence may delay convergence but does not make continuing agents dead,
  authorize teardown, or erase the last-applied catalog. DING/sidecar survival
  alone does not satisfy this agent-liveness predicate. Under incomplete or
  partitioned observation, absence of evidence cannot prove the catalog
  globally not live.
- **HOST-R08 Stable resolved state roots:** While a catalog is live, or while
  its resident st2 control-plane instance is running, the resolved catalog root
  and PTY root remain stable mounted state paths and must not be relocated.
  Their contents have different semantics; ordinary catalog edit/sync remains
  allowed. Relocation requires the explicit coordinated migration contract in
  [issue #85](https://github.com/compoundingtech/st2/issues/85).

Current mechanisms, executable evidence, and the remaining partition questions
are mapped in [spec.md](spec.md).
