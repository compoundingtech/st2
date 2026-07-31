# Doctor requirements

Doctor checks one catalog from one host. It follows the root
[vision](../vision.md). It refines [R03](../requirements.md#L46-L47),
[R04](../requirements.md#L51-L54), and [R08](../requirements.md#L92-L95).
It does not define fleet health.

## Requirements

- **DOCTOR-R01 Diagnostic subject:** One run checks one selected catalog and one
  selected host. The result does not claim that a remote host is available.
- **DOCTOR-R02 Gate result:** A healthy result returns zero. Any diagnosed
  problem returns non-zero. Each problem names the affected declaration, task,
  runtime, catalog, or supervision subject.
- **DOCTOR-R03 Read-only diagnosis:** A run must not change catalog, presence,
  or runtime state. It must not reconcile, launch, stop, reap, repair, or
  materialize work.
- **DOCTOR-R04 Bounded non-interactive operation:** Doctor and its external
  probes must not read the caller's standard input. A probe must stop within a
  fixed time. A timed-out probe is a failed check, not a hung caller.
- **DOCTOR-R05 Active local health:** Doctor checks each active declaration on
  the selected host. It checks that declared work is alive and that presence is
  maintained. It does not check remote availability.
- **DOCTOR-R06 Retired absence:** A retired declaration is healthy only when all
  declared task records are absent. A live or dead record is unhealthy. A
  retired declaration does not require presence or active-declaration checks.

The [Retirement health invariant](../../../INVARIANTS.md#L20) and its tests prove
the retirement rule. The [specification](spec.md) owns the mechanism, check
groups, and known gaps.
