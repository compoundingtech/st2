# Supervision spec

This document specifies the mechanism shared by every part of supervision. It
builds on [requirements.md](./requirements.md) and sits under the st2
[spec](../spec.md).

## Status

Active. A map to the implementation and its evidence, not a replacement for the
CLI help or the tests.

## Scope

One resident process per catalog and host, waking on a bounded timer or an
admitted declaration change, running a pass that converges this host's declared
agents. Everything specific to *what* a pass decides, *how* work is started, or
*what happens across the supervisor's own replacement* is in the child nodes.

## The pass

```text
  timer tick ──┐
               ├──► pass ──► decide ──► apply ──► report
  admitted ────┘              (pure)    (effects)
  declaration
  change
```

Each pass is self-contained. It re-reads the catalog, re-observes what is
running, and computes the whole delta from scratch. No state is carried between
passes except the restart bookkeeping described in
[`02-launch`](./02-launch/spec.md).

Deciding is a pure function of declarations and observed sessions
(SUP-R06); applying is the only part with effects. That split is what makes the
decision exhaustively testable without processes.

## Host scope and single ownership

The supervision record is a file in the catalog named for the host, holding the
owning process id. Because the catalog may be synced (SUP-A01), a per-host name
is what keeps one host's claim from binding another — host A's record travels to
host B, and B reads only its own.

Three states, and they are not symmetric (SUP-R03):

| Record | Owner | Meaning |
| --- | --- | --- |
| absent | — | Manual or single-pass operation. Valid. |
| present | alive, not us | Another supervisor owns this pair. Refuse to start. |
| present | gone | An unclean exit. The next start reclaims it. |

A declaration whose resolved host is not this host is counted and skipped
(SUP-R01) — the pass records that it belongs elsewhere and performs no
liveness query for it.

## Wakeups

Two independent sources, with different guarantees.

**The timer** is the floor (SUP-R09). It runs on a bounded interval no matter
what the watcher does. The interval is waited in short slices rather than one
long sleep, so a stop request is honoured promptly (SUP-R10).

**The watcher** is an optimization, and is deny-by-default (SUP-R07). It admits
a path only when:

- the path's first component under the catalog is the shared-template
  directory, or
- the file's name is exactly the canonical declaration filename.

Everything else is ignored. Concretely that means the session registry, the
message bus and inboxes, logs, supervision records, and generated workspace
content cannot wake reconciliation — which matters because the supervisor
*writes* several of those itself, and reacting to them would make its own
effects its next input.

On top of the path filter, only mutating events are admitted (SUP-R08):
creation, modification, and removal. A read or an open is discarded. This is not
a refinement but a correctness requirement: on at least one supported platform
the watch mechanism reports file access, and a supervisor that reads its catalog
every pass would otherwise wake itself forever (SUP-C02).

> The root's `R14` enumerates create, modify, **rename**, and remove. The
> implementation has three arms because the watch library models a rename as a
> modification of the path — renames are admitted, under the modify arm. This is
> a difference in how the events are named, not in which are admitted.

Because both filters are restrictive, an unadmitted change is not lost: it is
picked up by the next timer pass (SUP-T01).

## Failure handling

Two tiers, and the distinction is the whole of SUP-T02.

**Per-operation failures are collected.** A launch that fails, a teardown that
fails, a reap that fails, one declaration that will not parse, one workspace
that will not materialize — each is recorded against the pass and the pass
continues for every other agent (SUP-R11).

**An unobtainable view of actual state ends the pass.** If the session snapshot
cannot be established, the pass marks itself skipped and performs no
reconciliation at all (SUP-R12). This is the one hard stop, and the reason is
that an empty snapshot is indistinguishable from "nothing is running": acting on
it would tear down and relaunch the entire host. A resident supervisor simply
retries on its next pass; a single-pass caller must exit unsuccessfully.

## Derived runtime values

Where a declaration names a relationship that running tasks need — the agent to
notify about this agent's failures — the supervisor writes it into the task's
environment from the declaration, and **removes** any inherited value when the
declaration names none (SUP-R13). An author cannot make the authored
relationship and the runtime value disagree, and a renderer never needs to
restate it.

## Evidence

| Guarantee | Proof |
| --- | --- |
| SUP-R07, SUP-R08 watcher scope and mutation-only wakeups | `src/watch.rs::only_mutations_wake_watch_loops`; `src/watch.rs::declaration_filter_ignores_runtime_state`; `src/watch.rs::linux_reads_are_silent_but_real_mutations_wake` |
| SUP-R08 the supervisor does not spin on its own reads | `src/run.rs::idle_supervisor_does_not_spin_on_its_own_catalog_reads` |
| SUP-R01 host scoping | `tests/reconcile.rs::other_host_specs_are_skipped`; `tests/run.rs::up_once_skips_other_host_specs` |
| SUP-R02, SUP-R03 single ownership and its three states | `src/host_lock.rs::acquire_release_cycle`; `::lock_path_is_host_scoped_and_dot_prefixed`; `::stale_lock_from_dead_pid_is_detected_and_not_an_owner`; `::a_live_foreign_pid_is_reported_as_owner`; `::release_does_not_clobber_a_foreign_lock` |
| SUP-R11 per-operation failures do not abort the pass | `tests/run.rs::up_once_collects_spawn_errors_without_aborting`; `tests/run.rs::up_once_surfaces_discovery_errors_and_unrunnable` |
| SUP-R12 an unobtainable snapshot skips the pass | `tests/run.rs::up_once_marks_a_list_failure_as_a_skipped_pass` |
| SUP-R13 derived, never re-authored | `tests/reconcile.rs::declared_supervisor_is_the_single_source_for_the_spawn_environment` |

SUP-R04, SUP-R05, SUP-R06, SUP-R09, and SUP-R10 hold by construction and are
exercised indirectly by the child nodes' proofs, but no test isolates them at
this level.

## Open design questions

- **SUP-DQ1 The watcher admits less than discovery accepts.** Discovery accepts
  three declaration file formats and two naming forms — a file named for the
  canonical declaration name, or one named for the agent itself. The watcher
  admits only the canonical KDL filename. A declaration authored in either other
  format, or named for its agent, is therefore discovered and reconciled
  normally but never wakes a pass early; it waits for the timer. Whether that is
  deliberate — the canonical form earning the fast path, consistent with the
  root's canonical-KDL requirement — or an unnoticed narrowing is unsettled.
  Nothing in the code states an intent, and no test covers a non-canonical
  declaration waking the supervisor.
- **SUP-DQ2 Single ownership is not enforced against a concurrent start.** The
  record is read, then written. Two supervisors starting at the same instant
  could both observe an absent record. Whether the gap is accepted because the
  operator is trusted and starts are rare, or simply unaddressed, is not stated
  anywhere and no test covers the race.
