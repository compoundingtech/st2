# Smart Resource lifecycle state-space prototype

Date: 2026-08-29

## Question

Can one small state model support profile defaults, validated binding selectors, atomic snapshots, thin invalidations, delivery unavailability, and one latest-state catch-up without an event ledger or per-transition backlog?

## Method

A disposable Rust model represented:

- a descriptor with published topics and default topics;
- a binding with an optional selector;
- one canonical snapshot digest;
- last-delivered digest, delivery availability, and pending relevance;
- observations carrying a new digest and one semantic topic;
- thin invalidation effects.

The driver exhaustively enumerated every sequence through depth seven over five actions: relevant failure, non-default success, relevant conflict, delivery unavailable, and delivery available. An independent oracle tracked expected current snapshot and pending relevance after every transition. A separate case proved that a selector naming an unpublished topic fails validation.

## Evidence

The first model stored a `pending_digest`. Enumeration found this shortest class of counterexample:

```text
delivery unavailable
relevant observation at digest 1
irrelevant observation at digest 2
```

The binding retained pending digest 1 while canonical state had advanced to digest 2. Delivering digest 1 on resume would point the agent at a stale generation even though the relevance trigger remained valid.

The corrected model stores only `pending_relevant_change: bool`. The current snapshot digest remains the single source of truth. If any relevant change occurred while delivery was unavailable, resume emits one invalidation for the then-current digest, including later irrelevant state changes.

The corrected model exhaustively checked 97,656 action sequences. All snapshot, selector, and catch-up invariants held.

## Result

One compact per-binding state is sufficient:

```text
currentSnapshotDigest: Digest?
lastDeliveredDigest: Digest?
pendingRelevantChange: boolean
deliverable: boolean
```

No pending digest, event backlog, provider cursor, or transition ledger is needed for st2 delivery semantics. Provider implementations may retain their own cursor when their native observation mechanism requires one.

## Conclusion

The state-first design becomes simpler when pending delivery is level-triggered. st2 records that relevant current state is unseen, not which historical event caused it. Resume always references the current canonical snapshot.

The same lifecycle reducer is independent of shared versus per-binding runtime topology. Topology changes how observations arrive, not how a binding validates, publishes, filters, or catches up.

## VRS Impact

- Define one built-in Resource-update delivery path keyed by binding rather than profile-defined multiple streams.
- Store a pending-relevance bit and current snapshot digest, not a pending event or digest.
- Reuse existing event supersession and DING transport for thin invalidations.
- Keep provider cursors, webhook delivery identifiers, polling intervals, and observation repair inside the profile implementation.
- Specify shared and per-binding runtimes behind one normalized host protocol; do not duplicate delivery state machines.
