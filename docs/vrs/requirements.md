# st2 requirements

## Context

st2 implements the executable agent contract in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).
The vision is defined in [vision.md](./vision.md). These requirements are the
current ratified product floor, not the entire future scheduler roadmap; new
scheduling and workflow constraints belong here only after their behavior is
accepted.

## Assumptions

- **A01 Declared fleet:** Each agent has one authored declaration and one
  pinned host.
- **A02 Local ownership:** Each machine's root reconciles work declared for
  that machine.

## Acceptable Tradeoffs

- **T01 Explicit limits:** A documented unsupported case is preferable to a
  hidden distributed guarantee.
- **T02 Stable scope first:** Non-agent identities may wait until the agent and
  st2 specs are stable.

## Requirements

### Must implement the agent contract

- **R01 Agent-spec compliance:** st2 validates and implements every agent-spec
  capability it claims to support, and identifies unsupported capabilities.
- **R02 Canonical KDL:** Hand-authored KDL is the canonical declaration; any
  generator is optional and its output is inspectable before reconciliation.
- **R03 Host-pinned placement:** Every runnable agent or task resolves to its
  declared host; host-local roots own reconciliation.

### Must preserve delivery and launch behavior

- **R04 DING/archive semantics:** Inbox delivery, archive precedence, retries,
  suppression, and restart recovery are deterministic and tested. DING may
  interrupt agent work, but it must not alter or submit a human's active draft;
  an unknown interaction state defers delivery.
- **R05 Restartable launch definitions:** A restarted PTY or exec receives the
  complete effective launch definition, including environment and supported
  launch fields.
- **R06 Verified hooks:** Required hook content is installed explicitly and
  verified before a rendered agent depends on it.

### Must preserve agent state and scope

- **R07 State externalization:** An agent's current work and durable decisions
  can survive process replacement without depending on its transcript.
- **R08 Agent-only identity:** st2 models agents, not arbitrary non-agent
  identities, until Nathan explicitly changes that scope.
