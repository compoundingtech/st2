# DELTA-002: typed request transport is design-superseded but still shipped

Status: open

## Divergence

[Decision 0004](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)
absorbs the typed service-principal request/reply transport into stream events
plus ordinary message replies. The implementation (`src/request.rs`, the
`st2 request` command group, `resources/request-state`) and its normative spec
sections (root [Service-principal request transport](../spec.md), 03-message
[Typed requests](../03-message/spec.md)) remain fully in force. Docs now carry
an accepted direction the code does not yet implement.

## VRS

The accepted target is stated in decision 0004 and staged as
[04-stream DQ-S4](../04-stream/open-questions.md): a reply to an event is an
ordinary `message reply`; `pending | replied` derives from `in-reply-to`;
`MESSAGE-R11`'s separation purpose survives because events never write the
sender ledger. The superseded sections carry forward-pointers to this delta.

## Implementation

`src/request.rs` is untouched and its invariant row (**Idempotent service
requests**, `tests/request_cli.rs`) is green and load-bearing. Two hard
constraints for the retirement, both verified during exploration: the request
wire types carry `deny_unknown_fields` and cannot evolve in place (fresh
namespace, not migration), and `request-state` dedup receipts are retired, not
converted — an announced loss after a deprecation window. The eval-owned
external requester capability (`MESSAGE-A01`) must keep a working path
throughout.

## Direction

update implementation

## Resolution Signal

Staged, in order: (1) stream events plus reply-derived status land with their
own invariant proofs; (2) the **Idempotent service requests** row is re-pointed
to the replacement proofs in the same commit that starts the `st2 request`
deprecation window (`qualified_proof_references_resolve` enforces the
ordering); (3) after the window, `src/request.rs` and `request-state` writes
are removed, the superseded spec sections are deleted, and `MESSAGE-R11` is
re-worded to its surviving form. Close this delta at step 3 with the exact
removal commit recorded.
