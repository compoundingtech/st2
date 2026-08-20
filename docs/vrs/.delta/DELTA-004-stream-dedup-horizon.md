# DELTA-004: stream deduplication is bounded to the receipt ring

## Current mismatch

Ratified [`STREAM-R04`](../04-stream/requirements.md) promises that replaying
an event identity always returns its original filename, including after the
event is archived. [`STREAM-R05`](../04-stream/requirements.md) says
correctness never depends on the bounded ring because unread inbox copies and
archive receipts anchor replay identity.

The shipped implementation deliberately keeps only 128 receipts per stream
and performs no inbox or archive identity scan. Within that horizon, replay is
idempotent and conflicting content fails. After eviction, the same event ID is
honestly accepted as a new event. Archive receipts remain authoritative for
their known filenames during crash recovery, but they are not an index from
`(stream, event-id)` to filename.

## Why the implementation differs

Searching every archive would make emit cost proportional to retained stream
history and contradict the bounded-state goal. An unread-only fallback would
make idempotency change when an agent archives an event. A bounded receipt
window gives a precise operational contract and keeps ingress work independent
of inbox/archive history.

## Required resolution

Requirements are protected. Maintainer approval is required to amend
STREAM-R04/R05 to make the retained receipt horizon the idempotency boundary.
Until then, the living stream spec and invariant table describe implemented
behavior, and adapters must use stable transition identities, bound their retry
and rediscovery windows, or maintain a provider-side cursor when a stronger
guarantee is required.

Resolution must also update DQ-S3 and decision 0004, whose current wording
claims that archive receipts preserve replay identity outside the ring.
