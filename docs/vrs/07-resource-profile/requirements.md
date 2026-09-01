# Resource Profile requirements

## Context

A Resource binding keeps an RFC 3986 absolute URI as a portable, opaque
identity under root [`R20`](../requirements.md). This subsystem defines the
optional, operator-supplied bridge from such an identity to a local carrier
that st2 can observe. It refines [`06-resync`](../06-resync/requirements.md)
without moving scheme ownership into st2 or making successful resolution a
condition of agent launch.

The accepted resolver registry, wasm boundary, and transactional ownership are
recorded in
[decision 0009](../.decisions/0009-resource-profiles-use-a-feature-gated-wasm-boundary.md).
The state-first read-and-observe authority, atomic publication and demand
result, typed semantic envelope, and latest-state catch-up are recorded in
[decision 0014](../.decisions/0014-resource-profiles-are-state-first-read-and-observe-capabilities.md).

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
- **PROFILE-A04 State-first authority:** For an observable profile, one atomic
  current snapshot is authoritative. Notifications are invalidations, not a
  complete event log, and no consumer may require every provider transition.
- **PROFILE-A05 Downstream observation semantics:** A profile implementation
  owns provider authentication, observation, reconciliation, semantic topics,
  typed-fact meaning, snapshot schema, and selector defaults. st2 owns the
  generic lifecycle, validation, atomic publication and demand result,
  coalescing, fencing, delivery, health, and containment contracts.
- **PROFILE-A06 Read-and-observe scope:** Read and observe do not mutate
  provider state or standardize actions. Comments, CI reruns, label changes,
  close, merge, approval, and other provider writes require a separate
  authority, approval, idempotency, audit, and result-delivery design.

## Acceptable Tradeoffs

- **PROFILE-T01 Feature-gated runtime weight:** Wasmtime's dependency, binary,
  and compile-time cost is accepted in builds that enable `wasm-resolver` so
  the sandbox complexity is absorbed once. Default builds retain the baseline
  dependency and binary surface.
- **PROFILE-T02 Owned guest ABI:** st2 owns core-wasm descriptor ABI 3 and its
  compatibility burden. Avoiding WASI and the component model keeps the
  capability surface closed, but ABI evolution must remain explicit.
- **PROFILE-T03 Stateless calls:** Successful compilations and unchanged
  compilation failures share a bounded cache, while each successful resolution
  receives a fresh store and instance. The extra instantiation cost is accepted
  for state, fuel, and memory isolation between calls.
- **PROFILE-T04 Superset-biased leaf publication:** Whole-catalog apply may
  leave an unreferenced new module after a crash. Ordered atomic leaf
  publication is preferred over a multi-file swap because catalog readers are
  already fenced by the transaction marker and recovery can remove the
  harmless superset; the catalog declaration must never point to a missing new
  catalog-owned module.
- **PROFILE-T05 Provider-native observation:** st2 does not require polling,
  webhooks, or a hybrid. The profile implementation may use the most efficient
  provider-native mechanism, accepting responsibility for convergence,
  backpressure, rate limits, and any provider cursor or repair state.
- **PROFILE-T06 One snapshot rather than facets:** The contract rewrites one
  atomic profile-defined snapshot even when provider facets change
  independently. This avoids generation manifests, facet consistency, and
  retention machinery until measured payload or read costs justify them.
- **PROFILE-T07 Schema execution:** Discovering profile capabilities, selector
  vocabulary, defaults, and validation requires executing the same bounded
  module chosen by the catalog. This keeps the contract and implementation
  atomic at the cost of making descriptor execution part of validation.


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

### Must transact catalog-owned modules

- **PROFILE-R10 Transactional module ownership:** A resolver module whose
  declared path is catalog-relative is a first-class declaration input.
  Snapshot, digest, diff, bootstrap, prepare, apply, and recovery include its
  exact normalized path and bytes in the catalog projection and root hash.
  Duplicate references to one normalized path contribute one input. A missing,
  escaping, symlinked, special, or oversized catalog-owned module and an
  unprojected prepared module fail validation before publication. Literal
  absolute module paths remain external immutable inputs and are not copied
  into catalog bundles. Publication orders new module bytes before the
  `catalog.kdl` that names them and retires old module bytes only after that
  declaration stops naming them.

### Must opt into chain notification

- **PROFILE-R11 Explicit chain notification:** A profile may opt into
  supervisor-chain notification. For a binding through that profile, resync
  includes active same-scheme carriers from every non-retired supervisor
  ancestor in the descendant's watch set. The flag is false by default, does
  not change path containment or task launch, and an invalid supervisor chain
  is reported rather than silently approximated.

### Must describe and validate observable capabilities

- **PROFILE-R12 Versioned profile descriptor:** A profile module exposes one
  bounded descriptor in addition to resolution. Descriptor ABI 3 declares
  supported capabilities, selector schema, semantic topic vocabulary, default
  selector value, runtime topology, snapshot media type, and snapshot schema
  identity. The host validates the descriptor under the same fuel, memory,
  output, import, and failure isolation as resolution. Unknown required
  capabilities or ABI versions fail that profile locally.
- **PROFILE-R13 Validated binding selectors:** An observable Resource binding
  may carry profile-specific selector configuration. Absence means the
  descriptor's default. KDL encodes the value as compact JSON in a `selector`
  raw-string property whose canonical renderer chooses the smallest safe hash
  fence; JSON and TOML forms carry a native JSON value. All forms lower to one
  normalized value that st2 validates against the descriptor before activating
  observation. A selector may choose only profile-published topics; it changes
  attention, never Resource URI identity, access authority, snapshot contents,
  or provider observation.

### Must publish one canonical current snapshot

- **PROFILE-R14 Atomic snapshot authority:** Each active observable binding has
  at most one profile-defined canonical current snapshot. `Publication` is the
  reusable payload for every publication form and contains schema identity,
  media type, snapshot bytes, semantic topics, and optional ordered typed
  facts. The host validates one complete `Publication`, computes its content
  digest from accepted bytes, replaces the snapshot atomically, and never
  exposes partial bytes. Periodic `Publish` and demand-result `Published`
  traverse the same acceptance, digest, relevance, and catch-up core. Equal
  bytes do not create a state transition. The first accepted publication with
  at least one selected topic schedules the same superseding invalidation as a
  later relevant change. The snapshot remains authoritative after missed,
  duplicated, reordered, or coalesced provider observations.
- **PROFILE-R15 Implementation-owned observation:** A profile implementation
  chooses polling, push, native subscription, or a hybrid and retains its own
  provider mechanism, cursor, conditional cache, rate-limit state, backoff, and
  repair policy. A generic demand may pull an eligible observation forward but
  never selects the provider mechanism, resets provider state, or becomes a
  provider-specific reconcile command. Provider payloads never bypass
  `Publication` to become canonical delivery records, and demand observation
  never authorizes a provider write.
- **PROFILE-R16 Declared runtime topology and fencing:** The descriptor declares
  either one shared runtime per catalog and exact scheme or one runtime per
  active binding. Both modes use one host protocol and per-binding lifecycle
  state. Each runtime incarnation receives a directional owner claim; each
  binding registration receives a token. The host accepts or addresses output
  only while owner, binding, and registration match current state. EOF and the
  supervisor process lifecycle own termination and restart. Shared-runtime
  failure may affect observation for many bindings but reports health per
  binding; per-binding failure remains local.
- **PROFILE-R16A Finite protocol and publication bounds:** A selector's
  canonical compact JSON is at most 16 KiB. One encoded runtime-protocol line is
  at most 2 MiB including its newline. Decoded snapshot bytes are at most 1 MiB.
  Health detail and a failed demand diagnostic are each at most 16 KiB of
  UTF-8. One `Publication` carries at most 32 ordered facts; each fact key is at
  most 128 bytes and each before/after value is at most 1 KiB of printable
  single-line UTF-8. st2 rejects an oversized value without truncation and
  contains the failure to the affected runtime or binding.
- **PROFILE-R16B Declared atomic demand:** Demand observation is explicitly
  declared and denied by default. Only a runtime declaration with the `demand`
  capability may receive `Observe`. Each `Observe` carries a positive demand
  watermark and current owner, binding, and registration fences. The runtime
  answers exactly once for that demand with one correspondingly fenced
  `ObservationResult`: `Unchanged`; `Failed` with an optional bounded
  diagnostic; or `Published` with one complete `Publication`. There is no
  separate demand publication and settlement, digest supplied by the runtime,
  or protocol observation timestamp.
- **PROFILE-R16C Coalesced, non-cancelling demand:** For one active
  registration, st2 keeps at most one demand dispatch in flight and one latest
  trailing watermark. Demand accepted during an in-flight observation survives
  its result and coalesces into the trailing dispatch. Only an exact atomic
  result, replacement of its fenced registration, or provider-process failure
  closes accepted work; no clock participates in correctness. `Published`
  settles as `settledChanged` with the host-computed accepted-publication
  digest, including when equal bytes create no state transition or resync
  delivery emission fails after the snapshot and catch-up transaction commits.
  A missing active binding maps to `absentBinding`; a binding whose runtime did
  not declare demand also maps to `absentBinding` with the explicit diagnostic
  `the profile runtime does not declare the demand capability`. A client
  generation older than the resident supervisor maps to `staleGeneration`; a
  newer generation remains queued until supervisor refresh. Provider failure
  maps to `providerUnavailable`. Client disconnect or wait expiry does not
  cancel accepted work, retract it, or alter the runtime's observation schedule.
- **PROFILE-R16D Durable demand intent:** Observe request and receipt records
  carry the exact schema identities `st2.resource-observe-request.v1` and
  `st2.resource-observe-receipt.v1`. They are private to one supervisor scope
  and bounded to 64 KiB each. Durable admission permits at most 256 unresolved
  requests per scope; an attempt beyond that cap receives submission
  backpressure before it is admitted, and request scanning remains bounded by
  the same cap. An admitted request record remains the durable, retryable
  intent until a terminal receipt is durably committed; only then may the
  request be removed. In-memory enqueue and nonterminal receipts do not
  transfer that ownership.
  A failed terminal receipt commit retains retryable state and
  leaves the request eligible for restart.
  Receipt status values use camelCase: `accepted` and `backpressured` are
  nonterminal;
  `settledUnchanged`, `settledChanged`, `settledFailed`, `absentBinding`,
  `staleGeneration`, and `providerUnavailable` are terminal. Only
  `settledChanged` carries the host-computed digest of accepted publication
  bytes. Provider diagnostics normalize to an optional bounded receipt value.

### Must bound attention and catch up to current state

- **PROFILE-R17 Semantic invalidation:** Every Resource invalidation carries the
  same bounded ordered fact envelope in its durable body and renders at most
  three whole facts into a subject of at most 96 Unicode scalars. Both periodic
  and demand publications may supply facts and semantic topics in
  `Publication`; st2 validates the facts, applies the binding selector to
  topics, and retains the selected topics and facts through catch-up. Passive
  carrier changes publish one `content` topic and a short digest-transition
  fact. Agent Spec declaration changes publish ordered binding-label facts for
  added, removed, and semantically changed Resource declarations without
  exposing URIs or reasons; unavailable declaration parsing falls back to a
  digest-transition fact rather than dropping the invalidation. Snapshot bytes
  and provider payloads remain in the authoritative carrier, not the event.
- **PROFILE-R18 Built-in superseding delivery:** Smart Resource invalidations
  reuse one built-in per-agent delivery stream and the existing inbox, DING,
  deduplication, and producer-side supersession machinery. The binding name is
  the supersession key. Profiles do not create one stream per topic or a third
  delivery plane.
- **PROFILE-R19 Level-triggered catch-up:** Snapshot reconciliation continues
  while delivery is unavailable. Per binding, st2 retains the current snapshot
  digest, the last-delivered digest, one pending-relevance condition, and the
  latest relevant selected topics and facts, not a transition backlog or
  pending historical digest. When delivery becomes available, pending relevant
  state emits at most one invalidation for the then-current snapshot digest
  with that retained semantic envelope.
- **PROFILE-R20 Observable health:** st2 reports descriptor, selector,
  observation, reconciliation, publication, and delivery health separately.
  Failure degrades only the affected profile runtime or binding, preserves the
  last proven snapshot with explicit freshness, and never presents stale bytes
  as newly observed state.

## Evidence

The mechanism choice and sandbox bounds are supported by the
[plugin-boundary comparison](./.experiments/2026-08-26-plugin-boundary-comparison.md).
Composition against the real Nix-generated standing-seat shape is supported by
the [real-shape end-to-end experiment](./.experiments/2026-08-26-dotfiles-real-shape-e2e.md).
The state-first attention boundary is supported by the
[GitHub attention-filter prototype](./.experiments/2026-08-29-github-attention-filter-prototype.md).
The minimal catch-up state and topology-independent lifecycle are supported by
the [smart Resource lifecycle state-space prototype](./.experiments/2026-08-29-smart-resource-lifecycle-prototype.md).
