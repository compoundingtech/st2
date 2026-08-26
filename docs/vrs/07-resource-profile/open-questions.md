# Resource Profile open questions

The registry/SDK boundary (Q8) and wasm-only foundation (Q10) are accepted and
therefore are not open questions.

- **DQ-P1 ABI compatibility.** The core-wasm ABI has three exports but no
  version negotiation. Before independently released third-party modules need
  compatibility guarantees, define old-guest/new-host and new-guest/old-host
  behavior and prove it with a compatibility matrix. Tracked in the
  [spec](./spec.md#design-questions).
- **DQ-P2 Runtime observability.** Reconciliation now reports a warning naming
  each registered-profile binding that degraded to unwatchable, so the
  production path is no longer silent. Resolve from dogfood evidence whether
  low-volume spans/logs/metrics must further separate feature-disabled builds,
  module defects, traps/fuel, malformed returns, and containment violations.
