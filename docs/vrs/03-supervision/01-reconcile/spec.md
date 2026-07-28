# Reconcile spec

This document specifies how a pass decides what to change. It builds on
[requirements.md](./requirements.md) and sits under the supervision
[spec](../spec.md).

## Status

Active.

## Phases of a pass

```text
discover ──► verify preconditions ──► materialize ──► observe ──► plan ──► apply
             (defers dependent          (per agent)   (snapshot) (pure)
              agents only)
```

The first four phases establish inputs; `plan` is a pure function of the last
two; `apply` is specified in [`02-launch`](../02-launch/spec.md).

### 1. Discover

The catalog is walked and every declaration-shaped file is parsed. Files that
fail to parse are collected as errors against the pass and do not stop the walk
(RECON-R14). Warnings — such as a declaration whose path and contents disagree
about identity or host — are recorded separately.

### 2. Verify preconditions

Some agents require a precondition to be confirmed before their content may be
rendered. When confirmation fails, the failure is recorded and **only the agents
requiring it** are excluded from the materialization set (RECON-R06). Agents
that do not require it are unaffected, and nothing already running is torn down
— the pass defers preparation rather than converging toward absence. The
precondition itself is specified outside this tree.

### 3. Materialize

Declared workspace content is rendered for every agent still in the set
(RECON-R04). This runs on every pass, which is safe because rendering unchanged
content is a no-op (RECON-A02).

Two failure grades:

| Grade | Effect |
| --- | --- |
| gating render failure | that agent is dropped from the pass; nothing of it is launched |
| advisory failure | recorded as a warning; the agent still launches |

Agents that failed to materialize are subtracted from the set the plan is
computed over, so a failed render cannot be followed by a launch into an
unprepared workspace (RECON-R05).

Materialization refuses to change a destination tracked by a human's version
control, simulating each content operation before writing (RECON-R07).

A standalone mode performs discovery and materialization and then stops. It is
the same phase, exposed on its own — not a separate lifecycle.

### 4. Observe

One snapshot of what is running, unioned across both task backends. If it cannot
be obtained the pass marks itself skipped and returns immediately, having
changed nothing (RECON-R13). This is the pass's only hard stop.

### 5. Plan

A pure function of declarations and the snapshot (RECON-R01). Every declaration
lands in exactly one bucket (RECON-R02):

| Bucket | Meaning |
| --- | --- |
| launch | active, this host, at least one declared task not currently live |
| teardown | retired, this host, with live tasks |
| adopt | active, this host, every declared task already accounted for |
| other host | resolves elsewhere; skipped entirely |
| unrunnable | active, this host, but no task carries a command |
| collect | dead, unpinned records of declared tasks |

Reconciliation is per task, so an agent with one live and one dead task yields a
launch of exactly the missing one (RECON-R03).

#### The transition table

Each declared task's record is alive, dead, or absent, and the declaration is
active or retired. Those two facts fully determine the intent:

| Declaration | Record | Pinned | Intent |
| --- | --- | --- | --- |
| retired | alive | either | tear down |
| retired | dead | no | collect |
| retired | dead | yes | nothing |
| retired | absent | either | nothing |
| active | alive | either | nothing |
| active | dead | no | collect, then start |
| active | dead | yes | **frozen** — neither collected nor started |
| active | absent | either | start |

The frozen row is the one worth stating plainly (RECON-R10). Pinning a task
against collection also prevents its restart, because the pin's purpose is to
preserve the dead session as evidence and restarting would destroy it. A pinned
task that dies stays down until a human intervenes. An agent all of whose tasks
are either alive or frozen counts as fully present.

#### Derived task values

Building a launch target resolves the task id (its explicit id, else the agent's
bus id joined with the task name), the working directory default chain, and the
environment. The declared supervising agent is written into the environment from
the declaration and **removed** when the declaration names none, so the authored
relationship and the runtime value cannot disagree (SUP-R13).

## Wakeups and cadence

Specified in the parent ([`../spec.md`](../spec.md)): a deny-by-default watcher
over authored declaration inputs, mutation-only, with a bounded timer as the
floor. Reconcile's contribution is that an event never carries information —
it only causes a pass to run, and the pass recomputes everything (RECON-T02).

## Evidence

| Guarantee | Proof |
| --- | --- |
| RECON-R03 per-task convergence | `tests/reconcile.rs::one_dead_task_launches_only_the_missing_one`; `tests/run.rs::up_once_launches_only_the_missing_task` |
| RECON-R02 bucket totality | `tests/reconcile.rs::fresh_service_launches_all_tasks_pty_and_exec`; `::all_tasks_live_is_adopted`; `::other_host_specs_are_skipped`; `::unrendered_job_without_commands_is_unrunnable` |
| RECON-R08 retirement converges to absent | `tests/reconcile.rs::retired_with_live_sessions_is_torn_down`; `tests/run.rs::up_once_finally_removes_dead_retired_tasks_without_restarting_them` |
| RECON-R09 active converges to running | `tests/reconcile.rs::exited_session_is_reaped_and_relaunched`; `tests/run.rs::up_once_reaps_dead_nonkeep_then_respawns` |
| RECON-R10 the frozen row | `tests/reconcile.rs::dead_keep_task_is_frozen_not_reaped`; `tests/reconcile.rs::agent_level_keep_pins_all_task_targets` |
| RECON-R11 unrunnable is named | `tests/reconcile.rs::generated_ding_only_job_is_unrunnable_and_does_not_launch`; `tests/run.rs::up_once_surfaces_discovery_errors_and_unrunnable` |
| RECON-R12 other host untouched | `tests/reconcile.rs::other_host_specs_are_skipped`; `tests/run.rs::up_once_skips_other_host_specs` |
| RECON-R13 no snapshot, no reconciliation | `tests/run.rs::up_once_marks_a_list_failure_as_a_skipped_pass` |
| RECON-R14 discovery failures reported, not fatal | `tests/run.rs::up_once_surfaces_discovery_errors_and_unrunnable` |
| RECON-R07 tracked destinations fail closed | `tests/materialize.rs::every_content_directive_refuses_to_change_a_tracked_target_before_any_write`; `::byte_identical_tracked_target_is_allowed_without_modification`; `::untracked_and_non_git_targets_remain_materializable` |
| Derived task values | `tests/reconcile.rs::declared_supervisor_is_the_single_source_for_the_spawn_environment`; `::workspace_is_carried_into_task_targets_for_cwd_defaulting`; `::host_none_defaults_to_this_host_with_fallback_id` |

RECON-R01 (purity) holds by construction — the planning function takes
declarations and sessions and returns a value — and is relied on by every test
above, which construct sessions directly rather than running processes.

RECON-R04, RECON-R05, and RECON-R06 are **not isolated by a test**; see
RECON-DQ1.

## Open design questions

- **RECON-DQ1 Materialization-inside-the-pass has no direct proof.** That
  rendering precedes launch on every pass, that a gating failure drops exactly
  one agent, and that an unsatisfied precondition defers only dependent agents
  are established by reading the pass, not by a test that exercises the
  ordering. The materialization tests prove the *rendering* contract in
  isolation; the reconcile and pass tests do not vary materialization outcomes.
  A test that fails one agent's render and asserts the others still launch would
  close this.
- **RECON-DQ2 The frozen state has no exit other than a human.** A pinned task
  that dies is never restarted and never collected, and nothing reports it as
  needing attention on the reconcile path — it simply counts as present. Whether
  a frozen task should surface the way a task abandoned by restart policy does
  is unsettled.
- **RECON-DQ3 Discovery warnings have no consumer here.** A declaration whose
  path and contents disagree about host parses cleanly and reconciles under the
  contents' host. The warning is recorded on the pass and nothing acts on it,
  even though host disagreement decides which machine supervises the agent.
