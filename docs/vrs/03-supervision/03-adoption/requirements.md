# Adoption requirements

## Context

Adoption is what makes the supervisor replaceable. The supervisor starts agents
but does not own their lifetimes: it can be stopped, killed, upgraded, and
started again while every agent it launched keeps running, and the replacement
takes ownership of those processes instead of launching duplicates.

This decomposes [`R11`](../../requirements.md) — control-plane replacement
safety — and depends on [`SUP-A02`](../requirements.md), that actual state is
observed rather than remembered. Where this file and its parent disagree, the
parent wins and this file is wrong.

The isolation that lets a task outlive its supervisor is specified in
[`02-launch`](../02-launch/requirements.md); this node specifies what the
replacement must then do.

## Assumptions

- **ADOPT-A01 Identity is stable across supervisors:** A task's runtime identity
  is derived from its declaration, not minted at launch, so a replacement
  supervisor computes the same identity for the same declared task and can match
  it against what is running.
  - Validation: implementation evidence — identity is the declared id, else the
    agent's bus id joined with the task name.
- **ADOPT-A02 The runtime outlives the supervisor:** The mechanisms holding
  running tasks — the terminal tool's registry and the recorded process ids —
  persist independently of the supervising process.
  - Validation: implementation evidence, and the survival proofs cited in the
    spec.

## Constraints

- **ADOPT-C01 A replacement has no memory of its predecessor:** Anything a
  supervisor knew that was not written down is gone. A replacement can only
  re-derive from declarations and re-observe what is running.

## Acceptable Tradeoffs

- **ADOPT-T01 Re-deriving over handing off:** No state is passed between a
  supervisor and its replacement. This costs a full re-observation at startup
  and is chosen anyway, because a handoff protocol would itself need to survive
  the failures adoption exists to tolerate.
- **ADOPT-T02 Losing restart bookkeeping on replacement:** A replacement forgets
  which tasks its predecessor abandoned, and will try them again. This is
  accepted, and is in fact the mechanism by which an operator clears an
  abandonment.

## Requirements

### Must not end what it did not intend to end

- **ADOPT-R01 Stopping the supervisor never stops an agent:** Normal exit,
  forced termination, and crash all leave every running task alive, with the
  same process identity it had before.
- **ADOPT-R02 Replacing the supervisor never stops an agent:** The supervising
  binary can be replaced and started again while tasks continue unchanged.
- **ADOPT-R03 Only an explicit lifecycle action ends a task:** There is exactly
  one operation that deliberately ends running work, and stopping supervision is
  not it. This must remain true no matter how supervision ends.
- **ADOPT-R04 Stopping says what it did:** A supervisor that exits reports that
  it is leaving work running, so an operator is never left inferring whether
  their agents died with it.

### Must take ownership without duplicating

- **ADOPT-R05 A fully-present agent is adopted, not restarted:** When every
  declared task of an agent is accounted for, the pass performs no launch for it
  and records it as adopted.
- **ADOPT-R06 Adoption is by stable identity:** A replacement matches running
  work to declared work by the identity derived from the declaration, and never
  by a handle only its predecessor held.
- **ADOPT-R07 A replacement starts only genuinely missing work:** Adoption and
  launch are decided per task, so a replacement meeting an agent with one
  surviving and one dead task starts exactly the dead one.
- **ADOPT-R08 Adoption is not a distinct mode:** A replacement runs the same
  pass as a supervisor that has been running for a week. There is no startup
  path that behaves differently, because a special first pass would be the least
  tested and most dangerous code in supervision.

### Must be honest about what it forgets

- **ADOPT-R09 Restart bookkeeping does not survive replacement:** A replacement
  begins with no record of which tasks its predecessor abandoned and will
  attempt them again under the declared policy. This is the supported way to
  clear an abandonment, and the operator-facing message about an abandoned task
  must therefore direct the operator to it.
