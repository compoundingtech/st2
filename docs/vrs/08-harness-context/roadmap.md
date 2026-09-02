# Harness context roadmap

Non-normative. This file records the direction this subsystem is expected to
move in, so that v1's deliberate simplifications read as choices rather than
oversights. Nothing here is a commitment, and nothing here constrains
[spec.md](./spec.md) — where the two differ, the spec is what st2 does.

## Where v1 stands

v1 is a **denormalized snapshot**: one record per agent, overwritten in place,
quantized so it writes on information rather than on every reading, carrying the
last reading and a compaction counter. Every consumer polls the roster to read it. That shape was
chosen because it is small, because it composes with the two records already
beside it, and because the guard makes its write rate bounded and explainable.

Its costs are known and stated as tradeoffs, not defects: readings between
bucket crossings are lost, bounded at one bucket of error (HC-T05), the compaction detail of
everything but the last event is gone (HC-T02), and a reader learns about a
change only when it next polls.

## Direction: readings and compactions as events, the record as a projection

The lossiness is the part worth undoing first. Both signals this subsystem
carries are naturally *edges* — a reading was taken at a time, a compaction
happened at a time — and st2 already has a durable record kind for edges: the
agent's built-in stream
([`04-stream`](../04-stream/requirements.md),
[decision 0004](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)).
A design where the producer appends an event and the harness-context record
becomes a denormalized projection of the fold would be less lossy and closer to
real time, and it would make the guard a projection-update policy rather than an
information filter.

Two things stand between here and there, and neither is cheap:

- The hook-process producers (Claude's status-line tee, its compaction hooks)
  are short-lived subprocesses with no stream-append plumbing today.
- Stream direction today is ingress *into* an agent, not egress *about* one —
  the reason decision 0006 rejected the same idea for the categorical axis.

So the event-driven design is a direction, not a deferred implementation: it
becomes tractable if and when egress streams exist, and it should be revisited
then rather than approximated now.

## Direction: history, once, for both axes

`DQ-C6` and [`OHS-T03`](../05-harness-state/requirements.md) are the same
question asked twice. Answering it once — a history of state transitions and of
context readings on one mechanism — buys what neither axis can answer alone:
histograms of token growth per turn, time-to-first-compaction, whether a runtime
that compacts often is also one that idles blocked. Answering it twice would
give the fleet two incompatible histories to join.

## Direction: supervisor actionability

The record is advisory by decision (HC-A02), and the gate is not this
subsystem's to open: a remote supervisor acting on a number needs the
bounded-staleness semantics [`DQ-H5`](../05-harness-state/open-questions.md)
still owes both axes. If that lands, "nudge a compaction before the window
wedges" becomes a real option and needs its own action vocabulary, its own proof
that a stale reading is a hard no-op, and its own decision record.

## Direction: a metrics feed through tokenlens, not through st2

Fleet-scale questions ("how much of the fleet is above 80% fill right now") want
a metrics pipeline, not a roster poll. The constraint is already written down:
[`OHS-R15`](../05-harness-state/requirements.md) and `O11Y-R09` forbid agent,
runtime, session, and message identity in metric labels, so per-agent fill can
never be a label — only histograms and fleet aggregates.

tokenlens is the natural exporter and already the fleet's registered token and
cost producer, with a `compaction_count` derived retrospectively from
transcripts and a Grafana dashboard in place; it has no context-window column
today and lists context replay as future work. The reconciliation problem to
solve first is that a live st2 counter and a transcript-derived tokenlens
counter are two sources of truth for the same quantity. That is a tokenlens-side
design, not an st2 record change.

## Direction: consumers

The roster wire is the contract; the interesting consumer is a TUI row. Nothing
in the fleet renders token or context information today. A numeric column beside
uptime and inbox count is an additive change on the consumer side — a new
*state* would need enum, glyph, and legend entries, a number would not — and
pinned consumers already ignore unknown keys, so the axis can land before any of
them read it.
