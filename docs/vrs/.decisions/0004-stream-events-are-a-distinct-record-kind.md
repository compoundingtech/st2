# Stream events are a distinct record kind on the shared bus transport

Status: accepted

Design decision made by Johannes on 2026-08-20 (interview over four executable
prototypes). Merge and acceptance approval required: upstream maintainers, per
the DQ1 approval loop noted in [issue #137](https://github.com/compoundingtech/st2/issues/137).

## Context

Agents need to subscribe to external-world changes (CI runs, PRs, timers,
monitoring) and be woken with each change through existing message delivery.
Issue #137 fixed the ingress contract: one external event becomes exactly one
durable inbox item, deduplicated by a producer-supplied identity, waking the
recipient through the normal inbox path.

Three overlapping mechanisms could carry such an event: ordinary agent→agent
messages (durable sender ledger, `(sender, recipient, key)` idempotency),
service-principal requests (`request.rs`: typed envelopes, reserved-filename
idempotency, no ledger), or a new record kind. Two e2e prototypes and two
independent design explorations (unification pole vs differentiation pole)
were built to decide; their reports live in
[`04-stream/.experiments/`](../04-stream/.experiments/).

## Decision

An **event** is a second record kind on the one existing transport. It is an
ordinary inbox message file — same filename grammar, same inbox/archive
directories, same DING/MCP/app-server delivery, no new notice path — carrying
event frontmatter: the producing stream, a mandatory producer-supplied
`event-id`, and an optional grouping `key`.

Event publication does not write the permanent hash-chained Agent Sent ledger.
Dedup state is a bounded, constant-size receipt ring plus one in-flight
publication reservation per stream; replaying an `event-id` within the
retained receipt horizon returns the original filename and never re-notifies.
The next emit reconciles an abandoned reservation from whether its chosen
filename reached the inbox or archive, without requiring stale producer
replay or retaining payload bytes.
An evicted identity is accepted as new without scanning inbox or archive
history; archive receipts keep their existing authority for known filenames.
An emit may declare `--supersede`, which publishes the successor before
archiving the newest still-unread matching predecessor among the retained
receipts — log-compaction semantics with bounded lookup and a duplicate-wakeup
rather than lost-wakeup crash bias — implemented producer-side only; DING's
staged ownership is never touched.

Ordinary messages and `MESSAGE-R01..R11` are unchanged. The typed
service-principal request/reply envelopes are absorbed over time: a reply to
an event is an ordinary `message reply`, and the `pending | replied` status
derives from `in-reply-to`. Retirement is conditional on defining and proving
a routable reply endpoint for the stream producer; a stream-qualified `from`
identity is not by itself such an endpoint.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| One record family, one ledger: everything is a message with a `kind` frontmatter discriminator, every producer writes the sender ledger | Rejected | The ledger answers "prove what this agent said" — a question no machine producer has. CI flaps would enter permanent O(history)-validated history, supersession and bounded retention are incoherent inside an append-only proof chain, and the pole's own report conceded a "keys-only profile" (re-splitting the store) for high-volume sources. |
| Two record kinds, one transport: events carry stream identity, supersession, and a bounded dedup ring; messages stay untouched | Selected | The extra semantics are measured needs, and the option still captures the poles' converged 80%: two identity kinds, requests absorbed, one transport and delivery path, fan-out fixed by scoping dedup per recipient. |
| Stream semantics as `message send` flags: one verb whose `--stream` presence flips ledger semantics to ring semantics | Rejected | A second mechanism wearing the first one's name; the divergent durable behavior hides behind a flag instead of a kind. |

## Evidence and Argument

Two independent design explorations were built from opposite poles, each with
an executable spike of its riskiest seam
([`04-stream/.experiments/`](../04-stream/.experiments/)). The unification
spike proved a non-agent sender can survive crash injection at all nine
publication checkpoints of the ledger transaction — so safety does not decide
the question. The differentiation spike proved the semantics only its model
offers: producer-side supersession never re-pastes a DING-staged predecessor
(24 supersedes in 146 ms → one fresh poke, zero staged retries, one unread
head), stream state stays constant-size, and R15 boundedness holds under a
fast-superseding stream. The earlier ingress prototype had already shown the
ordinary-message path cannot express a non-agent producer and writes one Agent
Sent row per event against MESSAGE-R11. The decisive argument is fit: a ledger
built to prove speech misfits facts that supersede each other, and both poles'
reports agreed on that sentence from opposite directions.

## Consequences

- The DING sidecar gains exactly one branch: the `»` glyph for events.
  Everything else in the fail-closed DING contract is inherited untouched.
- `request.rs`'s wire types carry `deny_unknown_fields` and cannot evolve in
  place; absorption therefore uses the fresh-namespace pattern with a
  deprecation window, and its dedup receipts are retired, not migrated.
- A restarted stream has no cursor; it re-observes current state and leans on
  the dedup ring. Catch-up history is explicitly not a v1 capability.
