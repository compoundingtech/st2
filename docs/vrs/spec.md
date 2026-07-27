# st2 specification

This document specifies st2's current implementation contract. It builds on
[requirements.md](./requirements.md).

## Status

Active. This is a concise map to the implementation and its evidence, not a
replacement for the README, CLI help, KDL examples, or tests.

## Scope

st2 validates a declared agent fleet, materializes agent workspaces, launches
host-local work, supervises restartable tasks, and delivers messages. The
agent grammar and harness-facing contract remain canonical in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).

## Host-local scheduling and supervision

```text
hand-authored KDL
       │
       ▼
validate ──► materialize ──► host-local st2 scheduler/reconciler
                                      │
                             ┌────────┴────────┐
                             ▼                 ▼
                        PTY / exec       DING sidecar
                             │                 │
                             └──── state + bus ┘
                                      ▲
                                      │
                            one intelligent root agent
                         observes · recovers · escalates
```

- **R01–R03:** Fleet validation separates structural errors from selected-host
  runtime facts. Materialization is inspectable and host reconciliation starts
  only declarations pinned to the local host.
- **R04:** Each machine schedules and reconciles only its pinned work. The st2
  loop is deterministic; exactly one declared root agent provides intelligent
  host-local supervision, bounded recovery, and escalation.
- **R06:** st2 passes the complete effective task definition to the underlying
  launcher so manual and supervised restarts are equivalent.
- **R07:** Hook bundles are explicit, content-addressed, installed separately,
  and verified before materialization references them.

## Message lifecycle

```text
atomic inbox file → DING attempt → agent reads → archive receipt
       └──────── archive with same filename wins ────────┘
```

- **R05:** A matching archive filename makes an inbox copy handled; stale
  duplicates are removed without another DING. Fresh `dnd` suppresses delivery;
  `busy` does not. Failed delivery remains retryable. Sidecar restart emits a
  bounded recovery notice instead of replaying the inbox. Delivery may wake an
  agent while it is working, but an active or uncertain human composer must be
  left untouched.

## State and scope

- **R08:** Presence and activity status are separate signals. The catalog must
  also expose the agent's current plan and step with explicit freshness so a
  human or supervising agent can understand progress without PTY inspection.
  Current presence/status files provide only part of this contract; the
  canonical plan-progress shape is not yet specified.
- **R09:** Durable work state is external to the model transcript and is
  restored into replacement sessions through declared workspace files and
  verified hooks.
- **R10:** Fleet identities are agents. General-purpose identity kinds are
  unsupported.

The owner updates this spec whenever implementation changes.
Changing [vision.md](./vision.md) or [requirements.md](./requirements.md)
requires Nathan's explicit approval.

## Open design questions

- **DQ1 Scheduled work:** The vision includes per-machine schedulers that form a
  distributed workflow engine, but the KDL shape, event inbox, deduplication
  boundary, and execution receipts are not yet specified. A successful
  executable eval and Nathan's approval should resolve this before adding
  scheduler requirements.
- **DQ2 Safe DING delivery:** The current screen/composer heuristic is not yet a
  reliable proof that pasting is safe and has occasionally interrupted human
  typing. Prefixing the displayed notice with two blank lines can improve
  readability but does not satisfy R04. Resolve this with a stronger evented
  signal or other measured classifier; a small on-device model is an optional
  experiment, not a required architecture.
- **DQ3 Catalog agent state:** Define the catalog paths, schemas, freshness
  rules, and atomic update semantics for presence, activity status, current
  plan, and current plan step. Prove that stale state is distinguishable and
  that a supervisor can follow plan progress without inspecting a PTY before
  adding the shape to `AGENT-SPEC.md`.
