# Resource Profile open questions

The resolver registry, wasm-only pure module boundary, transactional ownership
of catalog-relative modules, state-first publication authority, typed semantic
facts, and atomic demand-observation result are accepted and therefore are not
open questions. Demand is explicitly capability-gated; fenced by owner,
registration, and watermark; coalesced to one in-flight plus one trailing
dispatch; and not cancelled by client wait expiry.

DQ-P3 is resolved by the raw JSON `selector` property and its round-trip
prototype. DQ-P4 is resolved by treating initial readable state as a relevant
state transition when the publication names a selected topic. DQ-P5 is resolved
by explicit 16 KiB selector, 2 MiB protocol-line, 1 MiB snapshot, 16 KiB health
detail and failed-result diagnostic, and typed-fact bounds. Representative st2
issue and pull payloads remained below 41 KiB per item, leaving substantial
space for normalized reviews and check state without permitting unbounded
allocation. DQ-P6 is resolved by one directional owner claim per runtime
incarnation, one token per binding registration, EOF-owned termination, and the
shared ownership reducer. Evidence for selector and runtime ownership is in the
[selector and runtime protocol experiment](./.experiments/2026-08-29-selector-and-runtime-protocol-prototype.md).

- **DQ-P1 ABI compatibility.** Descriptor ABI 3 makes version selection
  explicit, but old-module/new-host, new-module/old-host, and runtime-protocol
  compatibility are not yet proven. Resolve before independently released
  third-party modules or runtimes with a compatibility matrix, frozen fixtures,
  and cross-version conformance tests. Tracked in the
  [spec](./spec.md#design-questions).
- **DQ-P2 Runtime observability.** The design separates descriptor, selector,
  runtime, observation, publication, and delivery health, but has no dogfood
  evidence for the minimum low-noise logs, spans, metrics, freshness display,
  or operator commands. Resolve by operating one GitHub PR/issue profile and
  proving that an operator can distinguish credential failure, provider outage,
  runtime crash, invalid publication, stale snapshot, and undeliverable agent
  without hot-path noise.
