# Resource Profiles are state-first read-and-observe capabilities

Status: accepted

Johannes selected this direction through ten recorded interview rounds on
2026-08-29 and confirmed the combined design before VRS work began. Event-first
authority remains a separate exploration in [issue #376](https://github.com/compoundingtech/st2/issues/376).

## Context

A Resource Profile maps an opaque Resource URI to a contained local carrier.
That passive mapping alone cannot observe a remotely changing Resource,
publish its current state, validate profile-specific attention settings, report
runtime health, or catch an agent up after delivery was unavailable.

GitHub pull requests and issues make the gap concrete. Mergeability, CI,
reviews, comments, and lifecycle state change independently. Raw webhook or
check events can be duplicated, reordered, missed, or noisy, and failed webhook
deliveries are not necessarily replayed. Existing st2 delivery paths likewise
do not promise a complete canonical event log. Treating any of those inputs as
authoritative would claim ordering and replay guarantees the substrates do not
provide.

Two prototypes bounded the design. A synthetic 16-observation pull-request
sequence showed that equal-state suppression removed only two candidate wakes,
while provider-aware semantic filtering reduced the sequence to seven
actionable or four blocking/terminal wakes. A Rust state-space model then
checked 97,656 lifecycle sequences and found that storing a pending historical
digest can point a resumed agent at stale state; one pending-relevance
condition plus the current snapshot digest is sufficient.

## Decision

1. One atomic current snapshot per Resource binding is canonical. Provider
   events, polls, webhooks, and demands are observations used to reconcile that
   snapshot. Notifications are invalidations, not a complete event log.
2. st2 owns a provider-neutral read-and-observe contract: descriptor execution,
   selector validation, runtime lifecycle, host-owned publication, health,
   bounded delivery, and current-state catch-up. Downstream profiles own URI
   semantics, provider authentication, observation, reconciliation, snapshot
   schema, semantic topics and facts, and selector defaults.
3. Descriptor ABI 3 contains the capabilities, selector schema, default
   selector, published topics, runtime topology, snapshot media type, and
   snapshot schema identity. A binding may provide validated selector
   configuration; omission uses the profile default.
4. `Publication` is the one reusable publication value. It contains schema
   identity, media type, snapshot bytes, topics, and optional bounded typed
   facts. Periodic `Publish` and the demanded `Published` result carry that same
   value through one host acceptance, digest, publication, relevance, and
   catch-up path.
5. The profile implementation chooses its provider-native observation
   mechanism. It may use push, polling, or a hybrid and retains its provider
   cursor, conditional cache, rate limits, backoff, and repair state. The closed
   resolver module gains no ambient network or credential imports.
6. The descriptor declares shared or per-binding runtime topology. Both use one
   normalized host protocol, directional owner fencing, registration fencing,
   and the same per-binding publication and delivery state.
7. Demand observation is an explicit, deny-by-default runtime capability. For
   an enabled active registration, the host may send `Observe` with a positive
   demand watermark. The runtime answers that demand with exactly one atomic
   `ObservationResult` for the same owner, binding, registration, and watermark:
   `Unchanged`, `Failed` with an optional bounded diagnostic, or `Published`
   with a `Publication`. There is no separate publication/settlement pair and
   no host timestamp in the protocol.
8. Demand is a level-triggered scheduling hint, not a provider-specific
   reconcile command or provider write. One in-flight dispatch and one latest
   trailing watermark coalesce bursts without dropping demand that arrives
   during observation. Exact result, registration replacement, or
   provider-process failure evidence closes accepted work; clocks do not.
9. A client wait bound limits only that client's wait. Disconnect or expiry
   does not cancel accepted demand, retract the supervisor's obligation, or
   participate in provider scheduling.
10. When delivery is unavailable, st2 retains the current and last-delivered
    digests, one pending-relevance condition, and the latest relevant selected
    topics and facts. Resume emits at most one invalidation for then-current
    state with that semantic envelope.
11. Read and observe do not authorize provider mutations. Actions, approvals,
    and a canonical event log require separate authority, idempotency, audit,
    and result-delivery designs.

## Options

| Option | Why it was not selected |
| --- | --- |
| Canonical Resource event log | Current delivery substrates do not provide complete replay, ordering, retention, cursor recovery, or gap repair. Tracked separately in issue #376. |
| Hybrid snapshot plus several semantic streams | Provides transition fidelity and independent policies, but adds public stream contracts before notification-volume evidence proves that need. |
| Keep st2 resolver-only | Existing primitives can be composed downstream, but every profile would reimplement lifecycle, validation, health, catch-up, and publication. |
| One event or snapshot facet per semantic topic | Enables selective reads but introduces cross-facet consistency and generation lifecycle. One atomic snapshot is sufficient for the evidence-backed use case. |
| Deliver snapshot bytes in invalidations | Duplicates the canonical carrier, increases delivery size, and weakens the state-first read boundary. Bounded topics and typed facts convey why a current-state read matters. |
| Preserve every update while delivery is unavailable | Produces a backlog that conflicts with state-first authority and the explicit noise constraint. |
| Separate `Publish` and demand settlement frames | Allows settlement and publication to disagree or be lost independently. One tagged atomic demand result has one acceptance point and one outcome. |
| Host-selected observation mechanics | Conflates generic demand with provider-specific reconciliation and transfers cursor, cache, rate-limit, and backoff policy to st2. |
| Standardize provider actions | No generic action workflow, authority model, approval contract, or idempotency evidence grounds such an API. |

## Evidence and Argument

- [GitHub attention-filter prototype](../07-resource-profile/.experiments/2026-08-29-github-attention-filter-prototype.md)
- [Smart Resource lifecycle state-space prototype](../07-resource-profile/.experiments/2026-08-29-smart-resource-lifecycle-prototype.md)
- [Selector and runtime protocol prototype](../07-resource-profile/.experiments/2026-08-29-selector-and-runtime-protocol-prototype.md)
- [Existing stream differentiation experiment](../04-stream/.experiments/2026-08-20-pipes-event-model-differentiation.md)
- [Existing Resource Profile boundary comparison](../07-resource-profile/.experiments/2026-08-26-plugin-boundary-comparison.md)
- [GitHub webhook best practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks)
- [GitHub webhook redelivery](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)

State-first authority makes delivery loss and coalescing safe: reconciliation
repairs current state, and an invalidation only prompts a read. Provider-aware
classification is necessary because byte equality cannot distinguish
actionable failure from routine progress. Binding selectors preserve consumer
control, while descriptor defaults avoid declaration noise. Host-owned
publication prevents a network-capable runtime from bypassing containment.

Pending notification is a level condition, not an event identity. If a relevant
change occurs while delivery is unavailable, a later publication may advance
the current digest. Catch-up therefore combines pending relevance and its
bounded semantic envelope with the authoritative current digest rather than
retaining a historical digest or transition backlog.

The same reasoning governs demand. A watermark identifies host work, not
provider history. An atomic tagged result cannot race a separate publication
against settlement, and owner plus registration fencing prevents a replaced
runtime or binding generation from satisfying current work.

## Consequences

- Descriptor ABI and host-protocol compatibility require conformance fixtures
  before independently released third-party modules or runtimes are supported.
- st2 gains a trusted host-process boundary for observable runtimes. The
  catalog operator, not the wasm guest or Resource URI, selects the executable
  and its deployed credentials, capabilities, and egress.
- The built-in `resync` stream carries Resource invalidations keyed by binding;
  no new stream family or delivery plane is required.
- Profile implementations may optimize aggressively for native real-time
  observation, but st2 judges convergent publication, exact demand results, and
  explicit health. It does not prescribe polling intervals or webhook repair.
- Actions and event-first authority remain out of scope rather than being
  smuggled into descriptor or runtime extensibility.
