# DELTA-005: `costUsd` is a per-message figure on pi and omp, not a session cost

Status: open

## Divergence

Ratified [`HC-R16`](../08-harness-context/requirements.md) says the record
carries "the harness-reported **session** cost", and the record's own field
description in [`08-harness-context/spec.md`](../08-harness-context/spec.md)
repeats it: "`costUsd` is the harness-reported session cost, in the harness's own
accounting."

The pi and omp producers shipped on 2026-08-29 publish the **last assistant
message's** `usage.cost.total`, which is what those two harnesses actually
report.

The other three harnesses are not affected: Claude's `cost.total_cost_usd` and
OpenCode's `session.info.cost` genuinely are session totals, and Codex reports no
cost at all.

## VRS

[HC-R16](../08-harness-context/requirements.md) requires the adjacent facts to be
carried as the harness reported them and nothing more, and names the cost fact a
session cost. The spec's own producer table — HC-R16's "which adjacent facts each
channel supplies" — already said "per-message `usage.cost.total`" for both rows,
so the two halves of the spec disagreed with each other before any producer
existed, and the implementation had to pick one. The pi and omp producer sections
in [`08-harness-context/spec.md`](../08-harness-context/spec.md) state the
divergence at the point of use.

## Implementation

Turning pi's or omp's per-message cost into a session total means summing every
message's `usage.cost.total` in the producer. That is precisely the
producer-side accumulator HC-R16 already refuses one field over, for
`sessionTotalTokens`, and the reasoning transfers unchanged: the sum's
correctness depends on having observed every message, an extension loaded into a
session that is already running has not, and a half-observed total is a worse
answer than an honest smaller one. Nothing else in st2 reconciles this number —
HC-R16's "carried as what the harness reported and nothing more" is the binding
half of the requirement, and a fabricated sum would violate it in order to
satisfy the word "session".

The alternative — writing `null` — was rejected because the per-message figure
is real, is what pi and omp show their own operators, and is strictly more
information than nothing.

One consequence is load-bearing in the implementation and is documented in the
producer sections: because the record replaces a reading's fields wholesale,
a frame emitted from an event that carries no cost would *erase* the published
one. The extension holds the last assistant cost and restates it on every frame,
and clears the hold on session replacement.

## Direction

update VRS

## Resolution Signal

HC-R16 amended so that the adjacent cost fact is "the harness-reported cost, at
whatever scope that harness reports it, stated per harness in the spec's producer
table" — which is what all five rows then describe consistently — and the
record's field description in the spec widened to match. Until then, the producer
table is the operative statement for pi and omp, and consumers must read the
`harness` discriminator before comparing this field across harnesses — a
comparison that is already required for `usedTokens`, whose meaning differs
between pi and omp for unrelated reasons (HC-T03).
