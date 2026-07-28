# Launch spec

This document specifies how a plan becomes running work. It builds on
[requirements.md](./requirements.md) and sits under the supervision
[spec](../spec.md).

## Status

Active.

## Applying a plan

Order is part of the contract:

```text
1. collect standalone dead records
2. for each task to start:
     policy decision ──► clear dead record ──► start ──► record the attempt
3. tear down retired tasks
```

Step 1 deliberately skips any record belonging to a task step 2 will consider.
Those are cleared inside the start loop instead, *after* the policy decision, so
that a task the policy has abandoned keeps its evidence rather than having it
collected out from under it (LAUNCH-R11).

Every operation's failure is recorded against the pass and the loop continues
(SUP-R11).

## The two mechanisms

| Kind | Mechanism | Why |
| --- | --- | --- |
| terminal-backed | delegated to the terminal tool, which allocates a pseudo-terminal | the work is an interactive harness that renders to a screen |
| terminal-free | supervised directly: new session, output to a log, process id recorded | the work must not acquire a controlling terminal |

Both receive the complete effective definition (LAUNCH-R01): the command
verbatim under a shell (LAUNCH-R02), the resolved working directory
(LAUNCH-R03), and the resolved environment. The definition is rebuilt from the
declaration at each start, so a supervised restart and a manual restart of the
same declaration are equivalent.

## Isolation

Each task is started into its own isolation domain — a sibling of the
supervisor's, never a descendant (LAUNCH-R06) — and into its own process session
(LAUNCH-R05). Two independent properties follow:

- Ending the supervisor, by any means, does not end the task.
- Terminating the supervisor's isolation domain does not cascade into tasks.

On platforms without the isolation mechanism, the session detachment alone is
the defence and the domain wrapping is a pass-through.

Because a task leads its own process grouping, deliberate termination targets
the grouping rather than the recorded process (LAUNCH-R07), so a shell wrapper's
children, a pipeline, or a daemon's workers all go with it.

## The restart policy

Consulted before every would-be start. Four outcomes:

| Outcome | When | Effect |
| --- | --- | --- |
| allow | within spacing and attempt budget | start it |
| delaying | too soon after the last start | skip quietly, retry next pass |
| rate-limited | budget exhausted, policy says keep trying | skip until the window clears |
| abandoned | budget exhausted, policy says stop | give up, surface once, keep evidence |

Two details carry weight:

- **The attempt is recorded only after a successful start** (LAUNCH-R09). A
  failed start leaves the budget untouched.
- **Abandonment is terminal only within one supervisor lifetime.** The
  bookkeeping is in memory, so a replacement supervisor begins with an empty
  abandoned set — which is the documented way to clear one. See
  [`03-adoption`](../03-adoption/spec.md).

Abandonment is surfaced once per supervisor lifetime (LAUNCH-R12): a message to
the operator's error stream, and a message over the bus to the agent's declared
supervising agent. An agent that declares none produces no bus message.

## Liveness debouncing

A task reported not-alive that was observed alive within a short grace window is
treated as a reporting artefact rather than a death (LAUNCH-R13). Its collection
and relaunch are deferred and recorded as such — deliberately not counted as
noteworthy, because a deferral is a no-op by design.

## Preparation before restart

Clearing a dead record precedes starting a replacement (LAUNCH-A02). The clear
preserves the just-finished run's diagnostics to one prior generation, so a
crash-looping task yields current-plus-previous evidence rather than either
nothing or an unbounded pile (LAUNCH-R15, LAUNCH-C03). Final removal — the
retirement path — clears the record and all retained generations.

If the clear fails, the start is skipped for that pass (LAUNCH-R14).

## Evidence

| Guarantee | Proof |
| --- | --- |
| LAUNCH-R01 restart equivalence | `tests/run.rs::up_once_reaps_dead_nonkeep_then_respawns` |
| LAUNCH-R05, LAUNCH-R06 isolation from supervisor lifetime and cascade | `tests/transport_isolation.rs`; `tests/transport_isolation_macos.rs`; `tests/nomad_survival.rs::explicit_teardown_kills_exec_but_plain_stop_does_not`; `::explicit_teardown_kills_pty_but_plain_stop_does_not` |
| LAUNCH-R07 termination reaps the whole grouping | `tests/exec_backend.rs::exec_kill_reaps_the_whole_process_group_not_just_the_leader` |
| LAUNCH-R08, LAUNCH-R10 policy governs and abandons | `tests/run.rs::flapping_cap_parks_a_fail_mode_task_that_keeps_dying` |
| LAUNCH-R12 surfaced once, and only with a declared supervising agent | `tests/run.rs::surface_crash_loop_notifies_the_supervisor_over_the_bus`; `::surface_crash_loop_without_supervisor_sends_nothing` |
| LAUNCH-R14 failed preparation cancels the restart | `tests/run.rs::up_once_does_not_restart_a_task_when_diagnostic_reap_fails` |
| LAUNCH-R15 bounded diagnostics | `tests/exec_backend.rs::exec_restart_reap_keeps_bounded_diagnostics_and_final_remove_cleans_them`; `tests/run.rs::up_once_finally_removes_dead_retired_tasks_without_restarting_them` |
| Errors collected, never fatal | `tests/run.rs::up_once_collects_spawn_errors_without_aborting` |

LAUNCH-R02, LAUNCH-R03, LAUNCH-R04, LAUNCH-R09, LAUNCH-R11, and LAUNCH-R13 hold
by construction and are exercised indirectly above, but no test isolates them.
LAUNCH-R09 and LAUNCH-R13 are the two where that gap is worth closing — see
below.

## Open design questions

- **LAUNCH-DQ1 The attempt-accounting rule is unproven.** That a failed start
  does not consume an attempt is a deliberate and consequential choice — it is
  what stops a task with a temporarily broken environment from exhausting its
  policy without ever running — and no test varies start success against the
  attempt budget.
- **LAUNCH-DQ2 The debounce grace is unproven and unexplained.** The window is a
  fixed constant. Nothing establishes why that duration, whether it should
  relate to the pass interval, or what happens when a task genuinely dies twice
  inside one window. No test exercises the deferral path.
- **LAUNCH-DQ3 Abandonment is announced once but lasts indefinitely.** The
  operator-facing notice is emitted a single time per supervisor lifetime, while
  the state it describes persists until the supervisor is replaced. An operator
  who arrives later — or who missed the line — finds a task that is simply not
  running, with nothing restating *why*. The signal that separates "crashed and
  will be retried" from "abandoned, and no retry will happen until you restart
  supervision" decays while the condition it describes does not.

  Two facts bound how bad this is, and both are worth stating so a reader does
  not over-correct:

  - **It is not silent.** An abandoned task has no live session, so the health
    check reports it as a problem on every run. What the health check cannot say
    is that the cause is abandonment rather than an ordinary death — it reports
    the symptom, not the reason.
  - **The documented remedy is correct, including for identity reuse.** The
    notice tells the operator to fix the cause and then restart supervision.
    Because the abandoned set is keyed by task identity and nothing removes an
    entry within a supervisor's lifetime, editing the declaration alone leaves
    the task abandoned on sight — the restart is not incidental advice, it is
    the required second step, and the notice does say it.

  What is unsettled is whether the reason should remain discoverable after the
  one-time notice: whether abandonment should be visible in catalog-backed state
  the way presence is, so that a health check or a roster could report *why* a
  task is down rather than only that it is.

  A related and smaller point: the bookkeeping collections are never pruned, so
  they retain one entry per task identity a supervisor has ever launched. The
  entries are inert — they are consulted only for currently-declared tasks — and
  bounded by how many identities one supervisor sees, so this is noted rather
  than raised.
