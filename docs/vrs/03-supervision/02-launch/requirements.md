# Launch requirements

## Context

Launch is the acting half of supervision: turning a plan into started, stopped,
and collected work. It also owns the decision of whether a task that keeps dying
should be started again.

This decomposes [`R06`](../../requirements.md) — a restarted task receives its
complete effective launch definition — and refines
[`SUP-R11`](../requirements.md). Where this file and its parent disagree, the
parent wins and this file is wrong.

What to change is decided in [`01-reconcile`](../01-reconcile/requirements.md).
Surviving the supervisor's own replacement is in
[`03-adoption`](../03-adoption/requirements.md).

## Assumptions

- **LAUNCH-A01 The declaration is the whole launch definition:** Everything a
  task needs to run — its command, working directory, and environment — is
  derivable from the declaration at the moment of launch. Nothing is
  accumulated from a previous run.
  - Validation: implementation evidence; this is what makes a manual restart and
    a supervised restart equivalent.
- **LAUNCH-A02 A dead record blocks its own replacement:** A task's identity is
  reused across restarts, so the previous instance's record must be removed
  before a new instance can take the same identity.
  - Validation: implementation evidence — restart reaps before spawning.

## Constraints

- **LAUNCH-C01 Two kinds of work, one of which needs a terminal:** An
  interactive harness must be given a terminal; a plain process must not be.
  This is a property of the work, not a preference, and it selects the mechanism.
- **LAUNCH-C02 Liveness observation is noisy under load:** A task can be
  reported not-alive while it is in fact running. Treating every such reading as
  death would destroy healthy work.
- **LAUNCH-C03 Diagnostics are finite:** Evidence from failed runs must be
  retained to be useful and bounded to be safe, because a crash-looping task
  would otherwise produce it without limit.

## Acceptable Tradeoffs

- **LAUNCH-T01 A stuck task over a destroyed one:** Where a reading is ambiguous
  or a preparatory step fails, the task is left as it is and retried on a later
  pass rather than reaped and restarted. Delay is recoverable; destroyed
  evidence is not.
- **LAUNCH-T02 Giving up loudly over retrying forever:** A task that cannot be
  kept alive is abandoned and surfaced rather than restarted indefinitely. An
  operator acting on one clear report is better served than by a task that
  consumes the host quietly.

## Requirements

### Must launch completely

- **LAUNCH-R01 The complete effective definition, every time:** A task started
  by supervision receives the same command, working directory, and environment
  it would receive from a manual start of the same declaration. A restart must
  not produce a differently-configured task from the first start.
- **LAUNCH-R02 The command is run verbatim:** A declared command is passed
  through as authored and never parsed, split, or rewritten by supervision.
- **LAUNCH-R03 The working directory follows a declared chain:** A task's
  directory resolves from its own declared value, then the agent's workspace,
  then the declaration's own location — so an author states it once at whichever
  level is meaningful.
- **LAUNCH-R04 The right mechanism for the kind:** Work declared as needing a
  terminal is given one; work declared as terminal-free must not be given one
  (LAUNCH-C01).

### Must isolate what it starts

- **LAUNCH-R05 A task is not a child of its supervisor's lifetime:** Each task
  is started detached, in its own process grouping, so that ending the
  supervisor by any means does not end the task.
- **LAUNCH-R06 A task is isolated from supervisor-directed termination:** Each
  task is placed in its own isolation domain, a sibling of the supervisor's
  rather than a descendant, so that terminating the supervisor's domain cannot
  cascade into running work.
- **LAUNCH-R07 Ending a task ends all of it:** Deliberately stopping a task
  terminates the whole process grouping it leads, not only the process directly
  recorded, so nothing it started is orphaned.

### Must govern restarts

- **LAUNCH-R08 Every relaunch is governed by the declared policy:** How many
  attempts within what window, the minimum spacing between them, and what
  happens when attempts run out are declared per agent and consulted on every
  would-be relaunch.
- **LAUNCH-R09 An attempt is consumed only by a successful start:** A start that
  fails must not count against the attempt budget, or a task whose environment
  is temporarily broken would exhaust its policy without ever having run.
- **LAUNCH-R10 Exhaustion has two declared meanings:** Running out of attempts
  either abandons the task or merely rate-limits it until the window clears, per
  the declaration. Abandonment is terminal within a supervisor's lifetime;
  rate-limiting is not.
- **LAUNCH-R11 An abandoned task keeps its evidence:** A task given up on is
  left as it is — its last record is not collected — so the failure remains
  inspectable.
- **LAUNCH-R12 An abandoned task is surfaced once:** Abandonment is reported to
  the operator and to the agent's declared supervising agent exactly once per
  supervisor lifetime, not on every subsequent pass. An agent that declares no
  supervising agent produces no notification.

### Must not destroy what it cannot confirm

- **LAUNCH-R13 An ambiguous liveness reading defers, it does not reap:** A task
  reported not-alive that was alive within a short grace window is treated as a
  reporting artefact. Its collection and relaunch are deferred to a later pass
  rather than performed (LAUNCH-T01).
- **LAUNCH-R14 A failed preparation cancels the restart:** If removing a dead
  record fails, the task is not started again on that pass. Starting without
  having cleared the previous instance would either fail or produce a task whose
  diagnostics are interleaved with its predecessor's.
- **LAUNCH-R15 Retained diagnostics are bounded:** Restart preserves the
  just-finished run's evidence to a fixed number of generations. Final removal
  clears them.
