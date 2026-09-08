# DELTA-007: tokenless canonical delivery entries remain readable during P2 rollout

Status: open

## Divergence

R43 makes an exact attempt token part of every delivery mutation. Canonical
`st2.delivery-ledger.v1` entries written before P2 have no such field. Codex and
OpenCode can still hold live durable evidence in those entries, so rejecting the
record would discard the ownership R43 exists to preserve.

T04 therefore permits one temporary reader case: a tokenless canonical Codex or
OpenCode entry. This is not the pre-ledger `delivery-state.json` translation
owned by DELTA-006 and never applies to Claude, pi, or OMP.

## VRS

[T04](../requirements.md) requires the reader to run inside the same R43
transaction lock, durably add the token before any other mutation or transport,
remain countable, and be deleted after fleet evidence proves that no tokenless
entry can return. Amendment 1 of
[decision 0018](../.decisions/0018-harness-identity-and-delivery-evidence-policy-are-distinct.md)
records Johannes's Q35 choice and makes a pre-token writer an unsupported
rollback target after a token-bearing claim.

## Implementation

The canonical entry parser accepts an absent `attemptToken` only long enough for
the locked transaction loader to derive a deterministic 128-bit token from the
exact immutable entry bytes and persist the token-bearing entry. New claims use
operating-system randomness. Every public mutation receives a canonical entry
with a token and compares that exact token.

The reader never emits tokenless bytes, never dual-writes, and never rewrites a
foreign, malformed, or unsupported ledger. Read-only roster and Doctor
observation count tokenless entries without invoking the backfill.

## Direction

update implementation

## Resolution Signal

`st2 doctor` prints, per seat, a `tokenless delivery ledger (DELTA-007)` advisory
with `tokenlessEntries=<n>` whenever a Codex or OpenCode ledger contains one or
more tokenless canonical entries. Silence means the reader contributed no input
on that seat; observation is read-only and does not make the signal clear.

The delta resolves after the advisory is absent on every admitted host for seven
consecutive days. Decision 0018 Amendment 1 already records that a pre-token
writer is not a supported rollback target, so no second rollback decision is
required. Deletion removes the absent-token parser arm, deterministic backfill,
tokenless counter, Doctor advisory, and this delta in one change. Token-bearing
`st2.delivery-ledger.v1` remains the canonical format.
