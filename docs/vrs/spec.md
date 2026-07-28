# st2 specification

This document specifies st2's current implementation contract. It builds on
[requirements.md](./requirements.md).

## Status

Active. This is a concise map to the implementation and its evidence, not a
replacement for the README, CLI help, KDL examples, or tests.

## Scope

st2 validates a declared agent fleet, materializes agent workspaces, launches
host-local work, adopts and supervises independently surviving tasks, and
delivers messages. The agent grammar and harness-facing contract remain
canonical in
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
  host-local supervision, bounded recovery, and escalation. Filesystem reads
  never wake reconciliation; only create, modify, rename, or remove events may
  wake it before the bounded timer.
- **R06:** st2 passes the complete effective task definition to the underlying
  launcher so manual and supervised restarts are equivalent.
- **R07:** Hook bundles are explicit, content-addressed, installed separately,
  and verified before materialization references them.
- **R11:** `st2 up` is a replaceable control plane, not the lifetime owner of
  its agents. Normal exit, forced termination, binary replacement, and restart
  leave every running agent PID and creation identity unchanged. The new
  control plane adopts those processes and starts only missing work; it does
  not duplicate them. Agent stop or retirement requires a separate explicit
  lifecycle action.

  Executable acceptance starts an agent, terminates `st2 up` normally and with
  a forced kill, verifies the agent remains alive and usable, replaces the st2
  binary, starts the control plane again, and proves adoption with the same
  agent PID/creation identity and no duplicate process.

  Teardown, the one action that ends tasks, resolves its catalog from the
  command line alone. Every other verb may inherit `$CATALOG` or fall back to
  the standard catalog; both are ambient — st2 exports `CATALOG` into every task
  it launches, and the standard default is derived from `$HOME` — so an inferred
  teardown target always exists and a missing argument would end a live fleet.

- **Session registry:** A catalog owns the `pty` registry holding its tasks.
  `<catalog>/pty` is the default; a catalog may declare another so that one host
  can share a single registry across catalogs. Resolution is an exported
  `PTY_ROOT`, then the catalog's declaration, then the default, applied
  uniformly to spawn, list, kill, and the bus environment st2 hands to native
  tools. Tasks and sidecars receive that root in their spawn environment; a
  process that resolves its registry from the environment alone therefore agrees
  when st2 launched it, and only then. A declaration whose field set does not
  match fails `st2 validate` rather than resolving silently back to the default.

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
  left untouched. Unsafe delivery retries use a bounded backoff so an active
  composer cannot create a short-lived PTY probe on every inbox poll. Inbox
  reads do not wake the sidecar; only mutations bypass its bounded poll cadence.

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

## Event contracts (R13–R15)

An event is evidence, not permission to run the world. The reconciler retains
path and kind, maps them to the affected identity or template dependency set,
and computes the smallest desired-versus-actual delta. One declaration affects
only that agent; a template affects only its dependents. A no-op performs zero
PTY queries, launches, teardowns, materialization, or writes.

Watchers are deny-by-default. The classifier/action contract is:

| Event | Minimal action |
| --- | --- |
| `agents/**/agent.kdl` create/modify/remove | validate, materialize, and converge that agent and derived tasks |
| referenced `_templates/**` mutation | converge dependent agents only |
| inbox create/archive/remove | DING consumer only; supervisor no-op |
| plan/resource/status mutation | specialized consumer only; supervisor no-op |
| PTY/exec/log/PID/socket/lock/temp/backup/read/open/unknown | no-op |

Startup, timer, watcher overflow/loss, and ambiguity are bounded full-audit
fallbacks. Accepted streams use head/tail coalescing: immediate head response,
one quiet tail after a burst, and a hard maximum. Executable proof covers
positive declaration/template wakes, negative runtime/bus events, bounded
discovery/materialization/PTY queries and writes, continuous-event starvation,
and no-op desired-equals-actual behavior.

## Open design questions

- **DQ1 Scheduled work:** The vision includes per-machine schedulers that form a
  distributed workflow engine, but the KDL shape, event inbox, deduplication
  boundary, and execution receipts are not yet specified. A successful
  executable eval and Nathan's approval should resolve this before adding
  scheduler requirements.
- **DQ2 Safe DING delivery:** Bounded observation now replaces the fixed
  paste-to-Return delay: maintained Codex and Claude composers must be
  positively empty before paste and show the exact staged notice twice before
  a separate Return. Human, modal, active, changed, timed-out, and unknown
  states fail closed, with staged-payload ownership preventing duplicate paste.
  This measured screen heuristic is still not an evented proof and renderer
  changes may defer delivery. Resolve the remaining gap with a stronger evented
  signal or other measured classifier; a small on-device model is an optional
  experiment, not a required architecture.
- **DQ3 Catalog agent state:** Define the catalog paths, schemas, freshness
  rules, and atomic update semantics for presence, activity status, current
  plan, and current plan step. Prove that stale state is distinguishable and
  that a supervisor can follow plan progress without inspecting a PTY before
  adding the shape to `AGENT-SPEC.md`.
