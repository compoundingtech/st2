# Doctor — Requirements

## Context

Doctor is st2's health check for one catalog as seen from one host. It answers a
single question — is this catalog healthy here, right now — and reports the
answer as an exit status a script, hook, or supervising agent can act on.

This node refines [R03](../requirements.md) (host-pinned placement),
[R04](../requirements.md) (root supervision), and [R08](../requirements.md)
(catalog observability): doctor is the read-only consumer of the catalog-backed
state those requirements produce. Presence vocabulary, freshness, and refresh
semantics belong to R08 and are not redefined here; doctor only decides which
presence readings count as healthy.

The retirement guarantee stated here is the one already pinned in
`INVARIANTS.md` under **Retirement health**.

## Assumptions

- **DOCTOR-A01 One host per run:** A run evaluates exactly one (catalog, host)
  pair. Declarations pinned to another host are outside the run's scope, even
  though they live in the same synced catalog.
  - Validation: implementation evidence — the per-declaration loop skips any
    declaration whose resolved host differs from the checked host.
- **DOCTOR-A02 The runtime is authoritative for liveness:** Whether a declared
  task exists and is running is decided by the task runtime's own session list,
  not by st2's records of what it once launched.
  - Validation: implementation evidence, plus `tests/doctor.rs` — the retirement
    outcomes are driven entirely by a substituted session listing.
- **DOCTOR-A03 Presence is a file beside the declaration:** An agent's presence
  is read from a file in the declaration's own directory, written by that
  agent's own refresher.
  - Validation: `tests/native_only.rs::clean_path_supports_help_validate_env_and_doctor`.

## Constraints

- **DOCTOR-C01 The runtime probe is a foreign process:** Reading the session
  list means running an external program. Doctor cannot assume it terminates,
  behaves, or leaves stdin alone.
- **DOCTOR-C02 Non-interactive callers:** Doctor runs from scripts, hooks,
  supervisors, and CI, where stdin may be a pipe nobody will ever write to and
  stderr may be routed away from stdout.

## Acceptable Tradeoffs

- **DOCTOR-T01 One hard stop:** Doctor reports every problem it can still
  evaluate rather than stopping at the first, with a single exception: an
  unreadable task runtime ends the run, because every remaining check depends on
  it. A partial report naming the real blocker is preferable to a full report
  built on a failed probe.
- **DOCTOR-T02 Flat severity:** Every problem counts the same and any one of
  them fails the run. Callers express the only tier that exists by choosing
  whether to require a supervisor.

## Requirements

### Must define health precisely

- **DOCTOR-R01 Structural integrity is catalog-wide:** Any catalog file that
  looks like a declaration but cannot be parsed or resolved is a problem,
  regardless of which host it names. A file that does not parse cannot be
  attributed to a host.
- **DOCTOR-R02 Supervision mode is the caller's declaration:** The absence of a
  live host lock is healthy, because a manual or one-shot host has no resident
  loop. A caller that expects a resident supervisor must be able to demand one
  and get a problem when it is missing. A lock file whose owner is dead is
  always a problem, in either mode, because it is evidence of an unclean exit.
- **DOCTOR-R03 Declared tasks must be alive:** Every task declared by a
  non-retired declaration pinned to the checked host must have a live session in
  the runtime. A missing session and an exited session are the same problem.
- **DOCTOR-R04 Presence must be readable and unrotted:** Every non-retired
  declaration pinned to the checked host must have a presence file, and that
  file must not have decayed to the derived unknown state. Every state an agent
  can actually declare is healthy, including `offline` — doctor checks that
  presence is being maintained, not what it says.
- **DOCTOR-R05 Retirement is complete only when nothing remains:** A retired
  declaration is healthy only once none of its declared task IDs appears in the
  runtime at all. A live session and a dead session record are both incomplete
  retirement.
- **DOCTOR-R06 Retirement excludes the live checks:** A retired declaration must
  not be required to have presence, and its tasks must not be checked for
  liveness. Requiring presence from something deliberately shut down would make
  correct retirement permanently unhealthy.
- **DOCTOR-R07 Required tooling must be present:** The tools a running fleet
  depends on must resolve on PATH, and their absence must be a problem in its
  own right rather than surfacing later as unexplained task failures.

### Must be safe to run anywhere, at any time

- **DOCTOR-R08 Read-only:** Doctor must not create, modify, or remove any
  catalog file, runtime-state file, or log. Running it must never be the reason
  a subsequent check reads differently.
- **DOCTOR-R09 Diagnosis without repair:** Doctor must not launch, stop, reap,
  reclaim, or materialize anything. It reports; recovery is a separate,
  explicit action.
- **DOCTOR-R10 Bounded:** Doctor must terminate even when the task runtime is
  wedged. An unresponsive runtime is a reported problem, not a hung health
  check.
- **DOCTOR-R11 Must not touch its caller's stdin:** Doctor and every process it
  runs must leave the caller's standard input unread. A health check that blocks
  on an inherited pipe is indistinguishable from an unhealthy fleet.

### Must be usable as a gate

- **DOCTOR-R12 Exit contract:** Zero problems exits successfully; one or more
  problems exits non-zero. This is the whole contract a caller needs.
- **DOCTOR-R13 Every problem names its subject:** Each reported problem
  identifies what failed — the file, the lock, the agent, or the specific
  task — and, where one exists, why.
- **DOCTOR-R14 The report survives stream separation:** The per-check report
  must remain readable when a caller keeps only standard output, and the failure
  summary must not be the only place a problem is visible.
