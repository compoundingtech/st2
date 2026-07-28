# Supervision requirements

## Context

Supervision is how one host keeps its declared agents running. A resident
process reads the catalog, works out what should be running here versus what is
running here, and moves the second toward the first — repeatedly, cheaply, and
without needing to be the thing that owns the agents' lifetimes.

This decomposes [`R03`](../requirements.md) (host-pinned placement),
[`R04`](../requirements.md) (root supervision), [`R13`](../requirements.md)
(shortest-path reconciliation), [`R14`](../requirements.md) (explicit
filesystem-event contracts), and [`R15`](../requirements.md) (bounded event
coalescing) into the obligations shared by every part of supervision. Where this
file and the root disagree, the root wins and this file is wrong.

The three parts are specified separately:

- [`01-reconcile`](./01-reconcile/requirements.md) — deciding what to change.
- [`02-launch`](./02-launch/requirements.md) — starting and restarting work.
- [`03-adoption`](./03-adoption/requirements.md) — surviving the supervisor's
  own replacement.

Hook installation and catalog validation are **not** supervision. Both are
preconditions supervision consumes, and both are specified elsewhere. Where a
supervision obligation depends on one, this tree states the dependency and its
failure behaviour without specifying the dependency itself.

## Assumptions

- **SUP-A01 The catalog is shared, the runtime is not:** One catalog may be
  synced across hosts, so it contains other hosts' declarations and other hosts'
  supervision records. Everything a host writes about *running* — its lock, its
  process records — is host-scoped and is not meaningful on another machine.
  - Validation: implementation evidence — the supervision lock is named per host
    and a host reads only its own.
- **SUP-A02 Actual state is observed, never remembered:** What is running is
  established by asking, each pass, not by trusting what a previous pass
  launched. A supervisor that has just started has the same view as one that has
  run for a week.
  - Validation: implementation evidence; this is what makes
    [`03-adoption`](./03-adoption/requirements.md) possible at all.
- **SUP-A03 Declarations change rarely, runtime state changes constantly:** The
  catalog is simultaneously the authored input and the place the runtime writes.
  The authored part is edited by a human occasionally; the runtime part churns.
  - Validation: implementation evidence — the supervisor's watcher admits only
    authored paths and ignores everything the runtime writes.

## Constraints

- **SUP-C01 Filesystem events are lossy and platform-dependent:** Events can be
  dropped, coalesced by the OS, arrive out of order, or not arrive at all.
  Nothing may depend on receiving one.
- **SUP-C02 Reads are events too:** On at least one supported platform the
  watch mechanism reports file *access*. A supervisor that reacts to its own
  reads of the tree it watches will wake itself forever.

## Acceptable Tradeoffs

- **SUP-T01 A narrow watcher plus a timer, over a broad watcher:** Admitting few
  paths risks missing a change until the next timer pass. Admitting many risks a
  self-wake loop and unbounded work. Latency is the cheaper failure, so the
  watcher is deny-by-default and the timer is the floor that makes a missed
  event a delay rather than a lost change.
- **SUP-T02 Partial progress over an aborted pass:** One agent's failure should
  not deny every other agent its reconciliation. Per-operation failures are
  collected and reported while the pass continues, except where continuing would
  mean acting on an unknown world.

## Requirements

### Must confine itself to this host

- **SUP-R01 Host-pinned scope:** A supervisor acts only on declarations that
  resolve to its own host. A declaration belonging to another host is accounted
  for and then left entirely alone — not launched, not torn down, not inspected
  for liveness.
- **SUP-R02 One supervisor per catalog and host:** At most one supervising
  instance may act on a given catalog for a given host, and the record of which
  one is host-scoped so a synced catalog cannot make one host's claim bind
  another. Two supervisors on one pair would double-launch every task.
- **SUP-R03 Absence of a supervisor is a valid state:** A host may be operated
  by single passes rather than a resident loop. No supervision record means
  manual operation, not failure. A record whose owner is gone is a different
  thing: evidence of an unclean exit.

### Must converge by the shortest correct path

- **SUP-R04 Delta, not refresh:** A pass computes the difference between what is
  declared here and what is running here, and acts only on that difference. It
  does not restart, re-materialize, or re-examine work that already matches.
- **SUP-R05 A no-op pass does nothing observable:** When declared and actual
  already agree, the pass performs no launch, no teardown, no reap, and no
  write. "Nothing to do" must be genuinely free, because it is the common case
  on every timer tick.
- **SUP-R06 Deciding is separable from doing:** Working out what should change
  must be possible without changing anything, so the decision can be examined
  and tested on its own. Effects are applied from that decision, not woven into
  reaching it.

### Must have explicit, bounded wakeups

- **SUP-R07 Only authored inputs wake supervision:** The supervisor's watcher is
  deny-by-default. Only changes to declaration inputs may wake a pass early.
  Everything the runtime itself writes — session registries, the message bus,
  logs, supervision records, and generated workspace content — must never wake
  reconciliation, because reacting to it would make the supervisor's own effects
  its next input.
- **SUP-R08 Only mutations wake supervision:** Reading or opening a watched path
  must never wake a pass. A supervisor reads its catalog every pass; if reads
  woke it, it would spin without ever being idle.
- **SUP-R09 A bounded timer is the floor:** A pass runs on a bounded interval
  regardless of events. Given SUP-C01 and SUP-R07, a change that no watcher
  admits is delayed until the next tick — never lost.
- **SUP-R10 Waking must remain responsive to stopping:** However the interval is
  waited out, a stop request must be honoured promptly rather than at the end of
  a full interval.

### Must fail in the open, not in the dark

- **SUP-R11 Per-operation failures are collected, not fatal:** A failure
  affecting one agent or one task is recorded and the pass continues for
  everything else.
- **SUP-R12 An unknowable world stops the pass:** If a pass cannot establish
  what is actually running, it must perform no reconciliation at all rather than
  act on an empty or partial view — which would read as "everything is missing"
  and relaunch the world. A resident supervisor retries on the next pass; a
  single-pass caller must report failure.

### Must not duplicate what it derives

- **SUP-R13 Declared relationships are derived, never re-authored:** Where a
  declaration states a relationship that running tasks need to know about, the
  supervisor derives the runtime representation from the declaration and
  replaces any conflicting value, including removing it when the declaration
  states none. An author must not be able to make the two disagree.
