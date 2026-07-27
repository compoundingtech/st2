# st2 vision

## The Problem

1. **Agent runtimes are hard to operate reliably.** Declarations, launch
   configuration, hooks, messages, and durable state can drift or disappear
   across restarts and machines.
2. **Agent work needs a dependable execution substrate.** Scheduling,
   workflows, message passing, and recovery should not require a person to
   continually nurse individual terminals.

## The Vision

- A reliable, declared runtime for agents.
- A distributed scheduler and workflow engine for agent work.
- Durable message passing and encapsulated agent state.
- A concrete implementation of the executable agent spec maintained by
  `compoundingtech/evals`.

## What This Is Not

- A general identity platform for people, services, or arbitrary non-agent
  actors while the agent and st2 specs are still stabilizing.

## Success Criteria

1. A canonical agent declaration can be validated, materialized, launched, and
   restarted without losing its effective launch definition or durable state.
2. Host placement, hooks, message delivery, archive handling, and recovery have
   explicit contracts and executable evidence.
3. Agent work can be scheduled or triggered as durable, inspectable workflows
   across the declared fleet.
4. Every supported agent-spec capability has an implemented and validated st2
   path, with unsupported behavior stated plainly.
5. Agent messages arrive reliably without corrupting, combining with, or
   submitting a human's active input.
