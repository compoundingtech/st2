# Doctor — Spec

This document specifies the doctor health check. It builds on
[requirements.md](./requirements.md) and sits under the st2
[spec](../spec.md).

## Status

Active. This is a map to the implementation and its evidence, not a replacement
for the CLI help or the tests.

## Scope

Doctor resolves a catalog and a host, runs a fixed sequence of checks, prints
one line per check, and exits non-zero if any check failed. It reads catalog
files, one lock file, the task runtime's session list, and per-agent presence
files. It writes nothing.

Doctor is not a supervisor and not a repair tool. It never reconciles, launches,
tears down, or materializes.

## Catalog and host resolution

- **Catalog:** an explicit argument, made absolute so a later working-directory
  change cannot retarget it; otherwise the catalog named by the environment;
  otherwise the standard per-user catalog under the state directory. The path is
  canonicalized for reporting, falling back to the given path when it cannot be.
- **Host:** the explicit host argument, otherwise the machine's short hostname
  (the first dotted label), otherwise `localhost`.

Both are printed in the header line, so a captured report says which catalog on
which host produced it.

## Check sequence

| Order | Check | Inspects | Problem when |
| --- | --- | --- | --- |
| 1 | required tooling | PATH entries | a required tool does not resolve to a file |
| 2 | supervision | `<catalog>/.st2.<host>.lock` | the recorded owner is dead, or no lock exists and one was demanded |
| 3 | catalog structure | every declaration-shaped file in the catalog | a file fails to parse or resolve an identity |
| 4 | task runtime readable | the runtime's session list | the listing fails or exceeds its bound — ends the run |
| 5 | per declaration | declarations pinned to the checked host | see below |

Checks 1 through 3 accumulate problems and continue. Check 4 is the single hard
stop (DOCTOR-T01): without a session list, nothing in check 5 can be evaluated,
so doctor fails immediately with the problems already found.

### 1. Required tooling

`pty` must resolve to a file in one of the PATH entries. Absence is reported as
`not found`. The check is existence only: it does not inspect the executable
bit, run the tool, or compare versions.

### 2. Supervision

Supervision is a per-(catalog, host) pid file, `.st2.<host>.lock`, so one host's
lock in a synced catalog never speaks for another host. Doctor reads it and
never writes or clears it.

| Lock state | Supervisor demanded | Outcome |
| --- | --- | --- |
| a live owner other than this process | either | healthy — a supervisor is running |
| file present, owner dead | either | problem — a stale lock from a dead supervisor |
| no file | no | healthy — manual or one-shot mode, no resident loop expected |
| no file | yes | problem — required but no live host lock |

The stale case is a problem in both modes because a dead owner's lock is
evidence of an unclean exit, not of a deliberate mode choice.

### 3. Catalog structure

Discovery walks the whole catalog and returns the declarations it resolved, the
files it could not parse, and non-fatal warnings. Doctor reports one problem per
unparsed file, labelled with the file path and carrying the parser's message.

These problems are not filtered by host (DOCTOR-R01). Discovery warnings —
notably an identity or host that disagrees between a file's path and its
contents — are not problems here; they belong to validation.

### 4. Task runtime

One bounded, non-interactive probe collects the runtime's view of every session.
Three properties make it safe to run from anywhere:

- standard input is `/dev/null`, so the probe cannot consume or block on the
  caller's stdin (DOCTOR-R11);
- the child is placed in its own session, and on timeout the whole process group
  is killed, so a wrapper's descendants do not survive holding the pipes;
- the wait is bounded and the reap happens off the critical path, so a wedged
  runtime cannot hold a short-lived doctor process open (DOCTOR-R10).

Failure — including the bound being exceeded — is reported as a problem against
`task runtime readable`, carrying the underlying error (for a bound, the elapsed
limit), and the run ends there.

The session list is the union of the terminal-backed and terminal-free backends.
From it doctor derives two views used by check 5:

- **alive:** the IDs whose session is running.
- **present:** every ID the runtime knows about, mapped to whether it is alive.
  A session that has exited is still present.

### 5. Per-declaration checks

Declarations whose resolved host is not the checked host are skipped entirely —
no task check, no presence check, no output line.

A task's runtime ID is its explicitly declared ID, or otherwise the declaration's
bus ID joined with the task name. A declaration authored in the compact form —
the agent is itself one terminal task — carries the bus ID as that task's ID.

**Retired declarations** get exactly one check: no declared task ID appears in
**present**. Because it consults the full present map rather than the alive set,
a leftover exited session record fails just as a running one does. On failure
the line names each remaining ID with its liveness, `(alive)` or `(dead)`.
Retired declarations produce no task-liveness line and no presence line
(DOCTOR-R06).

**Non-retired declarations** get:

- one check per declared task: its ID must be in **alive**. Anything else —
  never launched, exited, unknown to the runtime — reports the same
  `session dead/missing`.
- one presence check for the declaration as a whole. Presence is an agent-level
  check keyed on the declaration's directory, not a per-task one, so a
  declaration that launches no task still gets one.

Presence resolves to one of three outcomes:

| Presence file | Outcome |
| --- | --- |
| absent | problem — presence missing; the agent's refresher is not running |
| readable, derived unknown | problem — rotted to unknown |
| readable, any declared state | healthy, and the line names the state |

`offline` is a healthy reading. Doctor checks that presence is being maintained,
not what it currently says; the states themselves and the staleness window that
produces the derived unknown belong to the presence contract in
[R08](../requirements.md).

## Output shape

```text
st2 doctor — catalog <path>, host '<host>'
  ✓ <label>
  ✗ <label> — <detail>

✓ all checks passed
```

- One header line naming the resolved catalog and host.
- One indented line per check, marked pass or fail. A failing line appends its
  detail after an em dash; a check with no useful detail prints the label alone.
- A clean run ends with `all checks passed` and exits successfully.
- A run with problems exits non-zero, and the count of problems is the process
  error, which reaches standard error. The per-check lines stay on standard
  output, so a caller that keeps only stdout still sees which checks failed
  (DOCTOR-R14).

The report is human-readable only; there is no machine-readable mode. The exit
status is the machine contract (DOCTOR-R12).

## Evidence

| Guarantee | Proof |
| --- | --- |
| DOCTOR-R02 supervision modes and the stale lock | `tests/doctor.rs::manual_mode_is_healthy_without_a_host_lock_but_can_require_one` |
| DOCTOR-R05, DOCTOR-R06 retirement health | `tests/doctor.rs::retired_declaration_is_healthy_when_tasks_and_presence_are_absent`; `tests/doctor.rs::retired_declaration_is_unhealthy_while_a_declared_task_is_alive`; `tests/doctor.rs::retired_declaration_is_unhealthy_while_a_dead_task_record_remains` |
| DOCTOR-R10 bounded probe, reported as a problem | `tests/doctor.rs::doctor_bounds_a_hung_pty_probe_and_reports_the_runtime_error` |
| DOCTOR-R11 caller's stdin untouched | `tests/doctor.rs::doctor_closes_stdin_for_the_noninteractive_pty_probe` |
| DOCTOR-R04 missing presence fails, `offline` passes | `tests/native_only.rs::clean_path_supports_help_validate_env_and_doctor` |

DOCTOR-R01, DOCTOR-R03, DOCTOR-R07, DOCTOR-R08, DOCTOR-R09, DOCTOR-R12, and
DOCTOR-R13 hold by construction in the doctor path and are exercised indirectly
by the tests above, but no test isolates them.

## Open design questions

- **DOCTOR-DQ1 Read-only is asserted, not proven:** DOCTOR-R08 and DOCTOR-R09
  are established by reading the doctor path — it opens files, runs one probe,
  and prints. No test asserts that the catalog and runtime-state directories are
  unchanged after a run, the way materialization proves its tracked-target
  guarantee by inspecting the tree afterwards. Until an equivalent check exists,
  the strongest guarantee is an inspected one.
- **DOCTOR-DQ2 Discovery warnings have no home in health:** A declaration whose
  path and contents disagree about identity or host parses cleanly, so doctor
  passes it, yet on a running host that disagreement decides which machine
  supervises the agent. Whether it is a health problem, a warning tier doctor
  does not have, or correctly validation's business alone is unsettled.
- **DOCTOR-DQ3 The required tool set is fixed and unproven:** `pty` is the only
  tool checked, and it is checked unconditionally — a host running only
  terminal-free tasks does not need it. Whether the required set should be
  derived from what the checked declarations actually use is unsettled, and no
  test covers a missing tool.
- **DOCTOR-DQ4 Only retirement looks for residue:** For a retired declaration
  doctor asks whether anything remains that should not; for a live one it asks
  only whether the declared tasks are alive. A session belonging to no
  declaration on this host — an orphan from a renamed or deleted agent — is
  invisible. Whether that asymmetry is deliberate scope or a gap is unsettled.
- **DOCTOR-DQ5 No structured report:** A supervising agent that wants to know
  which agent is unhealthy, rather than that something is, must parse the human
  lines. Whether doctor should emit a machine-readable report — and if so
  whether it shares a shape with the other inspection surfaces — is unsettled.
