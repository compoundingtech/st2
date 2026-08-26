# Resource Profiles use a feature-gated wasm boundary

Status: accepted

Johannes selected the registry/SDK shape on 2026-08-26 (decision Q8), explicitly
amending the prior dotfiles direction that rejected a semantic registry. After
requesting real prototypes rather than a paper choice (Q9), he selected the
wasm-only foundation (Q10) and its goal of absorbing userland complexity once
inside a principled boundary.

## Context

Root `R20` deliberately preserved Resource URIs while leaving their schemes
open and downstream-owned. Resync could observe `file://` and relative paths,
but a binding such as
`dev.schickling.agent-goal://dev3/cos` remained unwatchable even when the local
carrier was `<agent-dir>/resources/goal.md`. Hard-coding that private scheme in
st2 would violate the portable envelope; leaving resolution to every consumer
would reproduce security, lifecycle, and observability decisions in userland.

Q8 chose an upstream registry/SDK boundary rather than a fixed catalog template
or a dotfiles-owned contracts dependency. Q9 then required declarative,
out-of-process exec, and wasm approaches to be built and measured against the
same goal-resolution shape before choosing the runtime boundary.

## Decision

1. st2 owns an injectable, exact-scheme Resource Profile registry and typed
   resolver SDK contract. The built-in registry is empty; scheme meaning stays
   downstream-owned.
2. A catalog-declared profile names one wasm module and one trusted
   `immediate | coalesced | silent` notification class. Wasm is the only profile
   mechanism: no declarative template tier and no host-exec tier.
3. The module runs as closed core wasm: no WASI, no imports, finite fuel,
   bounded linear memory, fresh instance state per resolution, and host-side
   containment of returned paths under the agent directory. Compiled modules
   are cached by path.
4. Wasmtime is gated behind `wasm-resolver`. Default builds retain the baseline
   dependency tree and binary surface; feature builds consciously absorb the
   runtime cost.
5. Resolver failure is contained to the binding. It becomes unwatchable, the
   supervisor remains alive, and no alternate mechanism guesses a result.

## Options

| Option | Measured runtime | Isolation and complexity | Result |
| --- | --- | --- | --- |
| Declarative path templates | warm **0.624 µs**; cold registry construction + resolve **2.647 µs** | Smallest runtime and declaration surface, but creates a second mechanism whose placeholders, validation, containment, documentation, and evolution st2 must own alongside programmable profiles. | Rejected as a separate tier. Static mappings use the same wasm boundary. |
| Out-of-process exec plugins | compiled Rust cold p50 **760 µs** (p95 1,117; p99 1,603); bash p50 **2.901 ms** (p95 4.024; p99 4.941); TTL cache hits **0.1–0.4 µs** | Process isolation tests passed for exit, 60 s hang bounded by timeout, garbage, oversized output, and crash-mid-write. About **295 core LOC** once plus **12–16 LOC/profile**, but every plugin is ambient host code and repeats packaging/security review. | Rejected. Cold path is 38–145× wasm and execution is RCE by design. |
| Feature-gated wasm plugins | warm p50 **0.29 µs**; cold p50 **20.1 µs**, p99 **33.8 µs**; compile module once **2.6 ms** | Closed sandbox; 12 isolation tests cover traps, fuel-bounded infinite loops, garbage returns, invalid memory, and oversized allocation. One runtime absorbs policy for every consumer. Costs **+71 lock packages**, **+16.35 MB / +188%** binary, and **+45.7 s** serial-equivalent compile work when enabled. | **Selected**, feature-gated. |

A combined declarative-plus-wasm design was also viable on raw performance, but
it preserved two resolver concepts forever to save roughly 18 µs on a cold call.
The selected foundation values one auditable composition boundary over that
imperceptible savings. Declarative-plus-exec was rejected because exec preserves
neither one mechanism nor a sandbox.

## Evidence and Argument

The comparison is recorded in
[`07-resource-profile/.experiments/2026-08-26-plugin-boundary-comparison.md`](../07-resource-profile/.experiments/2026-08-26-plugin-boundary-comparison.md).
All three prototypes used the same logical operation: resolve
`dev.schickling.agent-goal://<host>/<identity>` to
`<agent-dir>/resources/goal.md` and supply a notification class.

The wasm prototype established the properties that matter for a load-bearing
plugin boundary:

- a 1,469-byte Rust guest artifact implements the closed ABI;
- the module receives no host imports or WASI capabilities;
- 5,000,000 fuel units bound computation and 64 MiB bounds linear memory;
- each resolution gets fresh store/instance state while compilation is cached;
- the host validates memory, UTF-8/JSON, and lexical path containment;
- twelve hostile-module tests leave the supervisor alive;
- cold p99 is 33.8 µs, far below even a compiled exec plugin's p50.

The cost is also material and was not hidden: a feature build grew the binary
from 8,672,072 to 25,026,152 bytes, added 71 lockfile packages, and added 45.7 s
of serial-equivalent compile work. That is why optional compilation is part of
the decision rather than a later optimization. The repository dev shell also
adds `lld`, required to link `wasm32-unknown-unknown` guests.

The real-shape proof is recorded in
[`07-resource-profile/.experiments/2026-08-26-dotfiles-real-shape-e2e.md`](../07-resource-profile/.experiments/2026-08-26-dotfiles-real-shape-e2e.md).
The actual Nix-generated standing-seat declaration resolved through the wasm
profile and observed a whole-file goal rename in about 600 ms, emitted exactly
one deterministic resync event, and kept an equal-byte rewrite silent in two
fresh runs. The selected boundary therefore composes with real catalog,
watcher, and event behavior rather than only a microbenchmark.

## Consequences

- st2 assumes a permanent guest ABI compatibility obligation; versioning is
  tracked as `DQ-P1` in the Resource Profile spec.
- Operators who need profiles build with `wasm-resolver` and distribute guest
  artifacts; operators who do not need them pay no wasmtime cost.
- Even trivial path mappings require a wasm artifact. This is intentional: one
  module boundary replaces recurring resolver-specific host logic.
- Host-exec resolvers and declarative templates are not dormant alternate paths
  or follow-up tiers. Adding either would require new evidence and a new
  decision.
- Scheme ownership remains federated. The registry maps exact strings supplied
  by the catalog; it does not turn st2 into the authority for private URI
  semantics.
- Resolver observability and same-path module-cache invalidation remain explicit
  design questions; neither weakens the containment and feature-gating contract.
