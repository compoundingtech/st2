# Resource Profile requirements

## Context

A Resource binding keeps an RFC 3986 absolute URI as a portable, opaque
identity under root [`R20`](../requirements.md). This subsystem defines the
optional, operator-supplied bridge from such an identity to a local carrier
that st2 can observe. It refines [`06-resync`](../06-resync/requirements.md)
without moving scheme ownership into st2 or making successful resolution a
condition of agent launch.

Johannes selected the registry/SDK shape (decision Q8) and a wasm-only resolver
foundation after the measured three-way comparison (decision Q10). The
accepted rationale is recorded in
[decision 0009](../.decisions/0009-resource-profiles-use-a-feature-gated-wasm-boundary.md).

## Assumptions

- **PROFILE-A01 Downstream scheme ownership:** URI schemes and their semantic
  meaning remain downstream-owned. st2 ships no built-in Resource profiles;
  an unregistered scheme stays opaque and unwatchable.
- **PROFILE-A02 Catalog trust:** The catalog operator chooses which resolver
  module owns a scheme and which notification class its results carry. The
  module itself is untrusted computation and receives no ambient host access.
- **PROFILE-A03 Local denotation:** A successful profile resolution denotes a
  path inside the bound agent's directory. It does not grant authority over the
  URI, establish remote access, or change agent/task lifecycle semantics.

## Acceptable Tradeoffs

- **PROFILE-T01 Feature-gated runtime weight:** Wasmtime's dependency, binary,
  and compile-time cost is accepted in builds that enable `wasm-resolver` so
  the sandbox complexity is absorbed once. Default builds retain the baseline
  dependency and binary surface.
- **PROFILE-T02 Owned guest ABI:** st2 owns a small core-wasm ABI and its future
  compatibility burden. Avoiding WASI and the component model keeps the initial
  capability surface closed, but ABI evolution must be explicit.
- **PROFILE-T03 Stateless calls:** A compiled module is cached, while each
  resolution receives a fresh store and instance. The extra instantiation cost
  is accepted for state, fuel, and memory isolation between calls.

## Requirements

### Must provide one principled extension boundary

- **PROFILE-R01 Resolver SDK contract:** The reusable `agent-spec` library
  exposes one typed resolution contract: an exact URI scheme selects a
  resolver, and resolution takes the preserved URI plus the agent directory and
  returns either a contained local path with a notification class or a
  structured failure. The registry is injectable so catalogs can add or
  replace profiles without hard-coding downstream schemes into st2.
- **PROFILE-R02 Wasm-only profile mechanism:** Every declared Resource profile
  names a wasm resolver module. Declarative path-template and host-exec resolver
  tiers are not part of the foundation; a static mapping uses the same wasm
  boundary as arbitrary logic.
- **PROFILE-R03 Exact, deny-by-default registry:** Lookup is by an exact RFC
  3986 scheme beginning with an ASCII letter. Duplicate declarations, malformed
  declarations, unknown profile fields, and unsupported notification classes
  fail validation. Missing profiles do not fall back to guessed semantics.

### Must contain resolver behavior

- **PROFILE-R04 Closed sandbox:** Resolver modules run with no WASI and no host
  imports. Module files are opened nonblocking and accepted only as regular
  files, under a finite module-byte admission bound, per-call fuel budget,
  linear-memory cap, and table-element cap. Traps, infinite loops, missing
  exports, invalid memory ranges, malformed output, and special or oversized
  module, table, or allocation attempts cannot unwind into or terminate the
  supervisor.
- **PROFILE-R05 Host-enforced path boundary:** A module's non-empty returned
  path is decoded and normalized by the host and accepted only when it remains
  inside the agent directory. Every root, ancestor, and final component is
  opened without following symlinks; only regular final files are read.
  Guest-chosen paths never bypass host containment or block the worker on a
  special file.
- **PROFILE-R06 Failure is local and observable:** An unregistered scheme is an
  ordinary opaque binding. A registered profile that cannot load or resolve is
  distinguishable from a miss through the SDK, reported by reconciliation,
  degrades only that binding to unwatchable, and leaves the resident supervisor
  and unrelated profiles alive.

### Must preserve optionality and composition

- **PROFILE-R07 Feature isolation:** `wasm-resolver` is an opt-in build feature.
  A default build carries no wasmtime dependency. It can still parse and retain
  profile declarations, but attempting to resolve one reports that the feature
  is unavailable rather than silently substituting another mechanism.
- **PROFILE-R08 Resync composition:** Successful resolution supplies both the
  local carrier path and its declared `immediate` or `coalesced` class to
  resync. Profile class takes precedence over basename heuristics; `silent`
  carriers are excluded before resolver execution. Watching, digest
  deduplication, event identity, and delivery otherwise remain the
  [`06-resync`](../06-resync/requirements.md) contract.
- **PROFILE-R09 Nondisruptive bindings:** Adding, removing, changing, or failing
  a Resource profile does not make URI possession authoritative and does not
  change task launch targets. Resource-only declaration changes preserve root
  [`R21`](../requirements.md): healthy work is adopted without stop,
  replacement, or relaunch.

## Evidence

The mechanism choice and sandbox bounds are supported by the
[plugin-boundary comparison](./.experiments/2026-08-26-plugin-boundary-comparison.md).
Composition against the real Nix-generated standing-seat shape is supported by
the [real-shape end-to-end experiment](./.experiments/2026-08-26-dotfiles-real-shape-e2e.md).
