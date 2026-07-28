# Adoption spec

This document specifies how supervision survives its own replacement. It builds
on [requirements.md](./requirements.md) and sits under the supervision
[spec](../spec.md).

## Status

Active.

## The property, and why it is nearly free

```text
supervisor A ──starts──► tasks ─────────────────────────► still running
     │                     ▲
   stopped / killed        │ same identity, re-observed
   / replaced              │
     ▼                     │
supervisor B ──────────────┘  adopts; starts only what is missing
```

Adoption is not implemented as a feature. It falls out of three properties
specified elsewhere:

1. Tasks are started detached and isolated, so they do not die with their
   supervisor ([`02-launch`](../02-launch/spec.md)).
2. Actual state is observed every pass rather than remembered
   ([`../requirements.md`](../requirements.md), SUP-A02).
3. Task identity is derived from the declaration, not minted at launch.

Given those, a replacement supervisor's first pass is indistinguishable from any
other pass: it discovers, observes, and finds the tasks already alive. They land
in the adopted bucket and no launch is produced (ADOPT-R05, ADOPT-R08).

**There is no adoption code path.** That is the design: a special first pass
would be the least-exercised code in supervision and would run at exactly the
moment the operator is least able to tolerate a mistake.

## Ending supervision versus ending work

| Action | Effect on running tasks |
| --- | --- |
| supervisor exits normally | none |
| supervisor is forcibly killed | none |
| supervising binary is replaced and restarted | none; the replacement adopts |
| the explicit teardown operation | every live task of this host's agents ends |

Only the last row ends work (ADOPT-R03). On exit, the supervisor states that it
is leaving sessions running (ADOPT-R04), so the operator does not have to infer
it.

Teardown is idempotent: tasks already gone are simply absent from the live set,
and per-task failures are collected rather than fatal.

## Granularity

Adoption is decided per task, not per agent (ADOPT-R07). A replacement meeting
an agent whose harness survived but whose sidecar died starts exactly the
sidecar. This is the same per-task rule that governs every pass
([`01-reconcile`](../01-reconcile/spec.md)), which is why no separate rule is
needed here.

## What a replacement forgets

Restart bookkeeping — which tasks have been started how recently, and which have
been abandoned — lives in memory for the supervisor's lifetime and is not
written down (ADOPT-C01). A replacement therefore starts with an empty
abandonment set and will retry a task its predecessor gave up on, under the
declared policy from scratch (ADOPT-R09).

This is deliberate rather than an oversight, and the operator-facing message
proves the intent: when a task is abandoned, the supervisor tells the operator
to fix the cause and then restart supervision. Restarting is the supported way
to clear an abandonment (ADOPT-T02).

The consequence worth stating: a supervisor restarted for an unrelated reason —
an upgrade, a reboot — also clears every abandonment. A task parked for a cause
nobody fixed will be retried, will fail its policy again, and will be surfaced
again. The system is self-consistent, but abandonment is a property of a
supervisor's lifetime rather than of the task.

## Evidence

| Guarantee | Proof |
| --- | --- |
| ADOPT-R01, ADOPT-R02 stop, kill, and binary replacement leave tasks unchanged | `tests/nomad_survival.rs::normal_stop_and_binary_replacement_adopt_exec_unchanged_without_duplicate`; `::forced_kill_and_binary_replacement_adopt_exec_unchanged_without_duplicate`; `::normal_stop_and_binary_replacement_adopt_pty_unchanged_without_duplicate`; `::forced_kill_and_binary_replacement_adopt_pty_unchanged_without_duplicate` |
| ADOPT-R03 only explicit teardown ends work | `tests/nomad_survival.rs::explicit_teardown_kills_exec_but_plain_stop_does_not`; `::explicit_teardown_kills_pty_but_plain_stop_does_not` |
| ADOPT-R05, ADOPT-R06 adopt rather than relaunch, by identity | `tests/reconcile.rs::all_tasks_live_is_adopted`; `tests/run.rs::up_once_adopts_when_all_tasks_already_live` |
| ADOPT-R07 only missing work is started | `tests/reconcile.rs::one_dead_task_launches_only_the_missing_one`; `tests/run.rs::up_once_launches_only_the_missing_task` |
| Teardown is host-scoped and idempotent | `tests/run.rs::down_tears_down_this_hosts_live_tasks_only` |

The survival proofs above assert the same process identity before and after and
the absence of a duplicate, which is what makes ADOPT-R01 and ADOPT-R02
falsifiable rather than merely observed.

ADOPT-R04 (stop announces itself) and ADOPT-R08 (no distinct adoption mode) hold
by construction; the first is a message on exit, the second is the absence of a
branch.

## Open design questions

- **ADOPT-DQ1 Forgetting abandonment is documented in a message, not a
  contract.** The only place the intent appears is the operator-facing text
  advising a restart. No test asserts that a replacement retries a previously
  abandoned task, so the behaviour the design depends on is unproven and could
  be changed by an unrelated refactor without anything failing.
- **ADOPT-DQ2 Adoption assumes the surviving task is the one that was
  declared.** A replacement matches by identity and treats anything alive under
  that identity as the declared task. If a declaration's command changed while
  the supervisor was down, the surviving task is running the *old* command and
  is adopted as though it were current — it will not be restarted, because it is
  alive. Whether a replacement should detect that divergence is unsettled, and
  nothing records the launched definition to compare against.
- **ADOPT-DQ3 A frozen task and an adopted agent are indistinguishable in the
  report.** An agent all of whose tasks are alive, and one whose dead task is
  pinned against collection, both count as fully present. A replacement reports
  both as adopted. See RECON-DQ2.
