# Stream requirements

## Context

This subsystem defines declared event streams: named agent-owned subscriptions
that turn external-world changes into durable, deduplicated inbox events,
waking the agent through the existing delivery transports. It refines root
requirement [`R05`](../requirements.md) for inbox delivery, [`R22`](../requirements.md)
for named declared subscriptions ("do not replace the schedule with repeated
messages or polls" — a stream is the declared name a poll hides behind), and
inherits stable identity from `R19` and `R24`. Ordinary messages remain owned
by [`03-message/requirements.md`](../03-message/requirements.md); terminal
delivery remains owned by [`01-ding/requirements.md`](../01-ding/requirements.md).
The record-kind and locality decisions are recorded in
[`.decisions/0004`](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)
and [`.decisions/0005`](../.decisions/0005-streams-are-agent-nested-and-stream-named.md);
executable evidence lives in [`.experiments/`](./.experiments/).

## Assumptions

- **STREAM-A01 Host-local streams:** A stream runs on its owning agent's
  declared host (`R03`). Cross-host observation is served by placing the
  adapter on the right host, not by remote streams.
- **STREAM-A02 Trusted producers:** Emitting into a declared stream is gated
  by the declaration's existence and the trusted-fleet model (root `A02`), not
  by authentication. An external producer that names an undeclared stream is
  refused.
- **STREAM-A03 World logic stays outside:** Adapters (GitHub pollers, timers,
  monitoring bridges) are host tooling packaged outside st2; st2 carries no
  HTTP client, scheduler, or provider-specific event logic.

## Acceptable Tradeoffs

- **STREAM-T01 No catch-up:** A restarted or resumed stream has no cursor; it
  re-observes current state and leans on event dedup. Missed intermediate
  transitions are not reconstructed.
- **STREAM-T02 Per-subscriber adapters:** Two agents watching the same feed
  run two adapter processes. A shared top-level stream form is future work and
  must be a generalization of the nested form, not a second mechanism.
- **STREAM-T03 No declared cadence:** v1 ships no `every`; a periodic source
  is a long-running adapter that sleeps and emits. Declared cadence, if it
  ever arrives, extends restart policy rather than adding an in-process loop.

## Requirements

### Must be a declared subscription

- **STREAM-R01 Declared streams:** An agent declares zero or more named
  streams. A stream exists iff declared. A stream with a `command` lowers to
  one derived exec companion (the stream task) that supervises the adapter; a
  stream without a `command` is an external ingress endpoint. Stream names
  follow the task-name grammar and are unique per agent alongside task names.
- **STREAM-R02 Self-authoring:** An agent adds or removes its own streams by
  editing its own declaration through the serialized catalog-authoring path,
  under the same authority boundary as presentation authoring (`R25`).

### Must ingest exactly once

- **STREAM-R03 Mandatory event identity:** Every emit names the target agent,
  a declared stream, and a producer-supplied `event-id`. Emits without an
  `event-id` are refused. Dedup scope is `(stream, event-id)` per recipient.
- **STREAM-R04 Idempotent ingress:** One external event becomes exactly one
  durable inbox record. Replaying an `event-id` — including concurrent and
  crash-interrupted retries — returns the original filename, creates no second
  record, and never re-notifies; an archive receipt retains its existing
  authority. The machine receipt distinguishes `created` from `deduplicated`.
- **STREAM-R05 Bounded stream state:** Per-stream durable dedup state is
  constant-size (a bounded ring). Event publication does not write the Agent
  Sent ledger and performs constant work per emit.

### Must deliver as ordinary inbox work

- **STREAM-R06 Ordinary transport:** An event is an ordinary inbox record
  carrying event frontmatter (producing stream, `event-id`, optional `key`).
  Delivery rides the unchanged inbox paths; DING renders events with the `»`
  marker and inherits every fail-closed guarantee untouched.
- **STREAM-R07 Producer-side supersession:** An emit may declare supersession:
  the stream's unread predecessor for the same `key` is archived before the
  successor publishes. Supersession never touches DING staged ownership — a
  staged notice is released only through the existing archive-receipt rule —
  and a fast-superseding stream stays within `R15` bounds.

### Must couple to the owning agent's lifecycle

- **STREAM-R08 Companion lifecycle:** The stream task launches with its agent
  and is stopped while the agent is held, suspended, retired, or terminally
  parked, under the existing derived-companion contract. A crash-looping
  adapter parks the stream task without disturbing its agent and surfaces to
  the declared supervisor.
- **STREAM-R09 Suspension means eyes closed:** While an agent is suspended no
  events accumulate for it. Resume re-observes current state; re-emitting
  still-current state is safe under `STREAM-R03` dedup.

## Evidence

Four executable records in [`.experiments/`](./.experiments/): the ingress
comparison, the lifecycle spike, and the two design-pole explorations
(unification vs differentiation), each with the tests it ran and the
pre-existing failure set it verified against baseline.
