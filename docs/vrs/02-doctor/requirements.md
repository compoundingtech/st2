# Doctor — Requirements

Doctor is the on-demand diagnostic gate for one catalog as seen from one host.
It inherits the root [vision](../vision.md) and refines host placement,
supervision, and observability in
[R03](../requirements.md#L38-L39), [R04](../requirements.md#L43-L46), and
[R08](../requirements.md#L72-L78).
It does not define a second health model for the fleet.

## Requirements

- **DOCTOR-R01 One diagnostic subject:** A run evaluates one selected catalog
  and one selected host. Its result says whether that catalog is healthy from
  that host's perspective; it does not claim that remote hosts are available.
- **DOCTOR-R02 Useful as a gate:** A healthy result exits successfully and any
  diagnosed problem exits unsuccessfully. Every problem in the human-readable
  report names the declaration, task, runtime, catalog, or supervision subject
  that failed.
- **DOCTOR-R03 Read-only diagnosis:** A run must not change catalog, presence,
  or runtime state. It reports failures but never reconciles, launches, stops,
  reaps, repairs, or materializes work.
- **DOCTOR-R04 Safe non-interactive operation:** Doctor and every external probe
  it starts must not consume the caller's standard input. Diagnostic work must
  be bounded so a wedged dependency becomes a failed check instead of a hung
  caller.
- **DOCTOR-R05 Host-local live health:** For active declarations assigned to the
  selected host, doctor diagnoses whether declared work is alive and whether
  agent presence remains maintained. Declarations assigned to other hosts are
  outside the run's availability claim.
- **DOCTOR-R06 Retirement is absence:** A retired declaration is healthy only
  after all of its declared task records are absent, whether a remaining record
  is live or dead. Retired declarations do not require presence and are not
  evaluated by the active-declaration checks.

The exact retirement distinction is pinned by the
[Retirement health invariant](../../../INVARIANTS.md#L19) and its executable
proofs. Mechanism, check categories, and known gaps belong in
[spec.md](spec.md), not in these guarantees.
