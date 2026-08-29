# Resource Profiles are state-first read-and-observe capabilities

Status: accepted

Johannes selected this direction through ten recorded interview rounds on 2026-08-29 and confirmed the combined design before VRS work began. Event-first authority remains a separate exploration in [issue #376](https://github.com/compoundingtech/st2/issues/376).

## Context

Resource Profiles previously mapped an opaque URI to one contained local carrier. Filesystem resync could notify a live agent after local bytes changed, but the profile had no way to observe a remote Resource, publish current state, validate profile-specific attention settings, report runtime health, or catch an agent up after delivery was unavailable.

GitHub pull requests and issues made the gap concrete. Mergeability, CI, reviews, comments, and lifecycle state change independently. Raw webhook or check events can be duplicated, reordered, missed, or noisy. GitHub does not automatically redeliver failed webhook deliveries. Existing st2 stream ingress intentionally has bounded deduplication, and filesystem resync intentionally has no catch-up. Treating either delivery path as a complete canonical event log would claim guarantees that neither substrate provides.

Two prototypes bounded the design. A synthetic 16-observation pull-request sequence showed that equal-state suppression removed only two candidate wakes, while provider-aware semantic filtering reduced the sequence to seven actionable or four blocking/terminal wakes. A Rust state-space model then checked 97,656 lifecycle sequences and found that storing a pending historical digest can point a resumed agent at stale state; one pending-relevance bit plus the current snapshot digest is sufficient.

## Decision

1. One atomic current snapshot per Resource binding is canonical. Provider events, polls, and webhooks are observations used to reconcile that snapshot. Notifications are invalidations, not a complete event log.
2. st2 owns a provider-neutral read-and-observe contract: descriptor execution, selector validation, runtime lifecycle, host-owned snapshot publication, health, bounded delivery, and current-state catch-up. Downstream profiles own URI semantics, provider authentication, provider observation, reconciliation, snapshot schema, semantic topics, and defaults.
3. The bounded profile module exports a versioned descriptor containing capabilities, selector schema, default selector, published topics, runtime topology, snapshot media type, and snapshot schema identity. A binding may provide validated selector configuration; omission uses the profile default.
4. The first contract exposes one atomic snapshot rather than named facets or a generation manifest. A changed snapshot emits a thin invalidation containing binding identity, current digest, and selected semantic topics. It carries no snapshot bytes or rendered summary.
5. The profile implementation chooses the most efficient provider-native observation mechanism. It may use push, polling, or a hybrid and retains any provider cursor or repair state. The closed resolver module gains no ambient network or credential imports.
6. The descriptor declares shared or per-binding runtime topology. Both use one normalized host protocol and the same per-binding delivery state.
7. When delivery is unavailable, st2 retains only `pending_relevant_change` beside current and last-delivered digests. Resume emits at most one invalidation for current state.
8. The initial capability set stops at read and observe. Provider mutations, actions, approvals, and a canonical event log require separate research and design.

## Options

| Option | Why it was not selected |
| --- | --- |
| Canonical Resource event log | Most principled long-term possibility, but current delivery substrates do not provide complete replay, ordering, retention, cursor recovery, or gap repair. Tracked separately in issue #376. |
| Hybrid snapshot plus several semantic streams | Provides transition fidelity and independent policies, but adds public stream contracts before notification-volume evidence proves that need. |
| Keep st2 resolver-only | Existing primitives can be composed downstream, but every profile would reimplement lifecycle, validation, health, catch-up, and publication. |
| One event or snapshot facet per semantic topic | Enables selective reads but introduces cross-facet consistency and generation lifecycle. One snapshot is sufficient for the first evidence-backed use case. |
| Deliver profile-rendered summaries | May save a read, but increases prompt noise and creates redaction/rendering obligations without measured token savings. |
| Preserve every update while delivery is unavailable | Produces a backlog that conflicts with state-first authority and the explicit noise constraint. |
| Standardize provider actions now | No concrete action workflow, authority model, approval contract, or idempotency prototype grounds a generic action API. |

## Evidence and Argument

- [GitHub attention-filter prototype](../07-resource-profile/.experiments/2026-08-29-github-attention-filter-prototype.md)
- [Smart Resource lifecycle state-space prototype](../07-resource-profile/.experiments/2026-08-29-smart-resource-lifecycle-prototype.md)
- [Existing stream differentiation experiment](../04-stream/.experiments/2026-08-20-pipes-event-model-differentiation.md)
- [Existing Resource Profile boundary comparison](../07-resource-profile/.experiments/2026-08-26-plugin-boundary-comparison.md)
- [GitHub webhook best practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks)
- [GitHub webhook redelivery](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)

State-first authority makes delivery loss and coalescing safe: reconciliation repairs current state, and an invalidation only prompts a read. Provider-aware classification is necessary because byte equality cannot distinguish actionable CI failure from routine progress. Binding selectors preserve consumer control, while descriptor defaults avoid declaration noise. Host-owned publication prevents a network-capable runtime from bypassing containment or replacing the carrier through an unsafe filesystem path.

The lifecycle prototype supplies the main complexity reduction. A pending notification is a level condition, not an event identity. If a relevant change occurred while delivery was unavailable, any later snapshot update advances the current digest. Resume therefore points at current state by combining one pending bit with the authoritative digest. Shared and per-binding runtime topology do not need separate delivery reducers.

## Consequences

- Resource Profile ABI evolution becomes immediate rather than hypothetical. Descriptor and host-protocol compatibility require conformance fixtures before third-party modules or runtimes are supported.
- Agent Spec needs one profile-specific selector encoding. Its exact KDL-to-schema representation remains open and blocks implementation of binding overrides, not profile defaults.
- st2 gains a trusted host-process boundary for observable runtimes. The catalog operator, not the wasm guest or Resource URI, selects the executable and its deployed credentials and egress.
- The existing built-in `resync` stream can carry smart Resource invalidations keyed by binding; no new stream family or delivery plane is required.
- Profile implementations may optimize aggressively for native real-time observation, but st2 judges only convergent publication and explicit health. It does not prescribe polling intervals or webhook repair.
- Actions and event-first authority remain out of scope rather than being smuggled into descriptor extensibility.

## Amendment 1: selector encoding and runtime fencing

On 2026-08-29 Johannes selected raw JSON in a KDL `selector` property (Q12)
after a runnable comparison of concise, nested, and adversarial selectors.
Normalized JSON remains the cross-format and runtime value. KDL is only an
authoring representation; its canonical renderer chooses the smallest safe raw
string hash fence. This resolves the open selector-encoding consequence above.

The same experiment resolved runtime restart ownership without adding another
lifecycle protocol. Each process incarnation receives a directional owner
claim, each binding registration receives a token, and host acceptance requires
both to match. A new claim fences all prior output and clears registrations.
Shared and per-binding topologies use the same reducer.

The normalized wire protocol contains only `register`, `unregister`, `publish`,
and `health`. EOF and existing supervisor lifecycle replace `shutdown`;
implementation-owned observation replaces host `reconcile`. Evidence:
[selector and runtime protocol prototype](../07-resource-profile/.experiments/2026-08-29-selector-and-runtime-protocol-prototype.md).
