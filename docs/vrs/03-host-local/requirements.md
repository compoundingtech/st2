# Host-local supervision requirements

This sub-VRS applies the root [st2 requirements](../requirements.md) to one
host. It refines R03, R04, R11, R18, and R28. It does not change them.

## Requirements

- **HOST-R01 One local subject:** A resident st2 process selects one catalog and
  one host. It reconciles only that pair. A host can run another process for a
  different catalog. The process is not the root agent or a supervisor persona.
- **HOST-R02 Local convergence:** The process compares declarations for the
  selected host with local task state. It adopts matching work. It starts only
  missing work.
- **HOST-R03 One writer:** Only one resident st2 process can reconcile a catalog
  and host pair at one time. A process for another host has a different subject.
- **HOST-R04 Independent task lifetime:** A resident process can stop, fail, or
  be replaced without stopping agent tasks. Its successor adopts surviving work
  and does not duplicate it.
- **HOST-R05 Explicit destructive action:** Process absence, process restart,
  and peer loss do not authorize teardown. Only an explicit local retirement or
  teardown action can stop local tasks.
- **HOST-R06 Local escalation:** The root agent observes local health. It makes
  bounded recovery attempts and reports unresolved failures. It does not treat
  an unavailable peer as a fleet-health result.
- **HOST-R07 Separate catalog liveness:** A catalog is live while one of its
  canonical agents runs. The resident process has separate state. A DING
  sidecar does not keep a catalog live. An incomplete view cannot prove that a
  catalog is globally stopped.
- **HOST-R08 Stable state-root paths:** The catalog root and PTY root remain at
  stable mounted paths while the catalog is live or the resident process runs.
  File edits and sync can continue. Relocation requires the coordinated
  operation in [issue #85](https://github.com/compoundingtech/st2/issues/85).

[spec.md](spec.md) maps these requirements to code, tests, and open gaps.
