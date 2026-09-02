# Resource Profile open questions

The resolver registry, closed core-wasm resolution boundary, transactional
ownership of catalog-relative wasm artifacts, state-first snapshot authority,
universal WASIp2 observable-provider envelope, domain-typed capability rule,
fresh Store per call, host-only fenced proposal commit, deterministic durable
outbox intent, typed semantic facts, and atomic demand result are accepted and
therefore are not open questions.

The runtime-neutral proposal/publication foundation and the component executor
are separate conformance layers with one commit contract. This separation does
not create a native-provider alternative: publications from any source cross
the same `ProposalFence` and host-owned transition, while observable provider
execution conforms only through the component world.

DQ-P3 is resolved by the raw JSON `selector` property and its round-trip
prototype. DQ-P4 is resolved by treating initial readable state as a relevant
state transition when the publication names a selected topic. DQ-P5 is resolved
by explicit selector, snapshot, diagnostic, and typed-fact bounds plus
domain-interface-specific request, response, concurrency, and deadline bounds.
DQ-P6 is resolved by fresh per-call execution and
`ProposalFence { generation, revision, prior_digest }`; the earlier
host-process owner claim, registration token, topology, and JSON-line protocol
are superseded mechanisms. Evidence is recorded in the
[selector experiment](./.experiments/2026-08-29-selector-and-runtime-protocol-prototype.md)
and the
[WASIp2 component and atomic publication experiments](./.experiments/2026-09-01-wasip2-component-and-atomic-publication-prototypes.md).

- **DQ-P1 Component compatibility.** Exact WIT world and domain-interface
  versions make selection explicit, but old-component/new-host,
  new-component/old-host, capability-version, and AOT-cache compatibility are
  not yet proven across independent releases. Resolve before independently
  released third-party components or capabilities with frozen WIT fixtures, a
  cross-version conformance matrix, and cache-key rejection fixtures. Tracked
  in the [spec](./spec.md#design-questions).
- **DQ-P2 Provider observability.** The design separates component loading,
  descriptor, selector, capability, observation, proposal validation,
  publication, outbox, and delivery health, but has no operated-provider
  evidence for the minimum low-noise logs, spans, metrics, freshness display,
  or operator commands. Resolve with one GitHub provider and prove that an
  operator can distinguish credential failure, provider outage, component
  failure, capability denial, invalid proposal, stale fence, stale snapshot,
  and undeliverable agent without hot-path noise.

WASIp3 production execution, Store pooling, generic exec/raw
HTTP/filesystem/socket authority, a parallel native provider framework, and
runtime WAC graphs are deliberate exclusions rather than open implementation
questions. Their evidence gates are normative in the
[spec](./spec.md#deliberate-exclusions-and-evidence-gates-profile-r21r25).
