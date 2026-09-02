# Resource Profile requirements

## Context

A Resource binding keeps an RFC 3986 absolute URI as a portable, opaque
identity under root [`R20`](../requirements.md). This subsystem defines the
optional, operator-supplied bridge from such an identity to a local carrier
that st2 can observe. It refines [`06-resync`](../06-resync/requirements.md)
without moving scheme ownership into st2 or making successful resolution a
condition of agent launch.

The accepted resolver registry, closed core-wasm boundary, transactional
ownership, and the Q39 amendment separating observable WASIp2 providers from
resolution are recorded in
[decision 0009](../.decisions/0009-resource-profiles-use-a-feature-gated-wasm-boundary.md).
The state-first read-and-observe authority, atomic publication and demand
result, typed semantic envelope, and latest-state catch-up are recorded in
[decision 0014](../.decisions/0014-resource-profiles-are-state-first-read-and-observe-capabilities.md).

## Assumptions

- **PROFILE-A01 Downstream scheme ownership:** URI schemes and their semantic
  meaning remain downstream-owned. st2 ships no built-in Resource profiles;
  an unregistered scheme stays opaque and unwatchable.
- **PROFILE-A02 Catalog trust:** The catalog operator chooses which resolver
  module owns a scheme, which optional provider component implements
  observation, which exact domain capabilities it may import, and which
  notification class its results carry. Both wasm artifacts are untrusted
  computation and receive no ambient host access.
- **PROFILE-A03 Local denotation:** A successful profile resolution denotes a
  path inside the bound agent's directory. It does not grant authority over the
  URI, establish remote access, or change agent/task lifecycle semantics.
- **PROFILE-A04 State-first authority:** For an observable profile, one atomic
  current snapshot is authoritative. Notifications are invalidations, not a
  complete event log, and no consumer may require every provider transition.
- **PROFILE-A05 Downstream observation semantics:** A profile implementation
  owns provider reconciliation, normalization, semantic topics, typed-fact
  meaning, snapshot schema, selector defaults, and the authentication
  requirements of its domain capabilities. The host owns credential material,
  capability policy, generic lifecycle, validation, atomic publication and
  demand result, coalescing, fencing, delivery, health, and containment.
- **PROFILE-A06 Read-and-observe scope:** Read and observe do not mutate
  provider state or standardize actions. Comments, CI reruns, label changes,
  close, merge, approval, and other provider writes require a separate
  authority, approval, idempotency, audit, and result-delivery design.

## Acceptable Tradeoffs

- **PROFILE-T01 Feature-gated runtime weight:** Wasmtime's dependency, binary,
  and compile-time cost is accepted in builds that enable `wasm-resolver` so
  the sandbox complexity is absorbed once. Default builds retain the baseline
  dependency and binary surface.
- **PROFILE-T02 Two typed wasm contracts:** st2 owns the closed core-wasm
  resolver ABI and the versioned WASIp2 Component Model provider world. The
  resolver remains deliberately smaller because it only maps identity to a
  contained carrier; observable providers use the richer component type
  system without gaining ambient authority.
- **PROFILE-T03 Fresh observation state:** Compiled resolver modules and
  provider components may be cached, but each resolution, descriptor call, and
  provider observation receives a fresh Store and instance. The instantiation
  cost is accepted so guest memory, tables, resources, host state, fuel,
  deadlines, cancellations, and traps cannot leak between calls.
- **PROFILE-T04 Superset-biased leaf publication:** Whole-catalog apply may
  leave an unreferenced new wasm artifact after a crash. Ordered atomic leaf
  publication is preferred over a multi-file swap because catalog readers are
  already fenced by the transaction marker and recovery can remove the
  harmless superset; the catalog declaration must never point to a missing new
  catalog-owned artifact.
- **PROFILE-T05 Provider-domain capabilities:** st2 hosts explicit typed
  provider capabilities rather than generic process, network, or filesystem
  access. Adding a provider domain therefore requires a reviewed host
  interface, but its authority, validation, limits, and errors remain visible
  in the type boundary.
- **PROFILE-T06 One snapshot rather than facets:** The contract rewrites one
  atomic profile-defined snapshot even when provider facets change
  independently. This avoids generation manifests, facet consistency, and
  retention machinery until measured payload or read costs justify them.
- **PROFILE-T07 Schema execution:** Discovering profile capabilities, selector
  vocabulary, defaults, and validation requires instantiating the same bounded
  component selected by the catalog. This keeps the contract and
  implementation atomic at the cost of making descriptor execution part of
  validation.
- **PROFILE-T08 One component per observation:** A provider runs as one
  directly linked component invocation, not a long-lived process or runtime
  composition graph. Provider-native caches, cursors, and webhook repair state
  therefore live in host-owned domain capability state or durable provider
  state rather than guest Store state.

## Requirements

### Must provide one principled extension boundary

- **PROFILE-R01 Resolver SDK contract:** The reusable `agent-spec` library
  exposes one typed resolution contract: an exact URI scheme selects a
  resolver, and resolution takes the preserved URI plus the agent directory and
  returns either a contained local path with a notification class or a
  structured failure. The registry is injectable so catalogs can add or
  replace profiles without hard-coding downstream schemes into st2.
- **PROFILE-R02 Wasm-only profile mechanisms:** Every declared Resource profile
  names one closed core-wasm resolver module. A profile that declares observable
  capabilities additionally names one WASIp2 Component Model provider.
  Declarative path-template, host-exec, native-provider, and alternate runtime
  tiers are not part of the boundary; static mapping uses the resolver and all
  observable behavior uses the component envelope.
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

- **PROFILE-R07 Feature isolation:** `wasm-resolver` is an opt-in build feature
  covering both the resolver engine and observable component executor. A
  default build carries no wasmtime dependency. It can still parse and retain
  profile declarations, but attempting to resolve or observe one reports that
  the feature is unavailable rather than silently substituting another
  mechanism.
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

- **PROFILE-R10 Transactional wasm ownership:** A resolver module or provider
  component whose declared path is catalog-relative is a first-class
  declaration input. Snapshot, digest, diff, bootstrap, prepare, apply, and
  recovery include its exact normalized path and bytes in the catalog
  projection and root hash. Duplicate references to one normalized path
  contribute one input. A missing, escaping, symlinked, special, oversized, or
  unprojected catalog-owned artifact fails validation before publication.
  Literal absolute paths remain external immutable inputs and are not copied
  into catalog bundles. Publication orders new artifact bytes before the
  `catalog.kdl` that names them and retires old artifact bytes only after that
  declaration stops naming them.

### Must opt into chain notification

- **PROFILE-R11 Explicit chain notification:** A profile may opt into
  supervisor-chain notification. For a binding through that profile, resync
  includes active same-scheme carriers from every non-retired supervisor
  ancestor in the descendant's watch set. The flag is false by default, does
  not change path containment or task launch, and an invalid supervisor chain
  is reported rather than silently approximated.

### Must describe and validate observable capabilities

- **PROFILE-R12 Versioned component descriptor:** An observable provider
  exposes one bounded, typed descriptor through the universal WASIp2 provider
  world. It declares capabilities, selector schema, semantic topic vocabulary,
  default selector value, snapshot media type, and snapshot schema identity.
  The host validates the descriptor under the same fresh-Store, resource-bound,
  no-ambient-authority, and failure-isolation policy as observation. Unknown
  required capabilities or provider-world versions fail that profile locally.
- **PROFILE-R13 Validated binding selectors:** An observable Resource binding
  may carry profile-specific selector configuration. Absence means the
  descriptor's default. KDL encodes the value as compact JSON in a `selector`
  raw-string property whose canonical renderer chooses the smallest safe hash
  fence; JSON and TOML forms carry a native JSON value. All forms lower to one
  normalized value that st2 validates against the descriptor before activating
  observation. A selector may choose only profile-published topics; it changes
  attention, never Resource URI identity, access authority, snapshot contents,
  or provider observation.

### Must commit one fenced proposal atomically

- **PROFILE-R14 Atomic proposal authority:** Each active observable binding has
  at most one profile-defined canonical current snapshot. A component may
  return `Unchanged`, a typed failure, or one complete `Publication`. For a
  publication result the host constructs a proposal from that payload and the
  invocation's `ProposalFence { generation, revision, prior_digest }`, validates
  it, and alone owns the commit. One atomic transition makes the carrier,
  current digest, resulting revision, freshness, semantic catch-up state, and
  deterministic durable `PublicationIntent` visible together; the component
  cannot write any of them. Equal accepted bytes do not create a state
  transition. A crash before publication leaves the prior state authoritative.
  A crash after publication but before delivery acknowledgement leaves the
  outbox intent retryable.
- **PROFILE-R15 Domain-typed observation:** Every observable provider executes
  through the same versioned WASIp2 Component Model envelope and may import
  only host-linked, provider-domain capabilities explicitly declared for that
  component. Host implementations own credentials, endpoint and operation
  allowlists, response and concurrency bounds, deadlines, cancellation, and
  redacted typed errors. The component owns provider normalization and may
  propose canonical state; it receives no ambient WASI command, environment,
  clock, random, process, raw HTTP, filesystem, or socket authority.
- **PROFILE-R16 Fresh-Store execution and fencing:** Every descriptor call and
  observation creates a fresh Store and component instance, invokes it once,
  validates the result, and drops the Store. The host may reuse an Engine,
  Linker, and compiled component keyed by exact artifact and engine
  compatibility, but never pools or resets Store or instance state. Every
  proposal is fenced by the current binding generation, expected revision, and
  prior carrier digest. The host rejects stale generation or stale prior state
  without publication; concurrent proposals from one prior state have at most
  one commit winner, and retry of the same deterministic proposal is
  idempotent.
- **PROFILE-R16A Finite execution and proposal bounds:** A selector's canonical
  compact JSON is at most 16 KiB. Decoded snapshot bytes are at most 1 MiB.
  Health detail and a failed observation diagnostic are each at most 16 KiB of
  UTF-8. One `Publication` carries at most 32 ordered facts; each fact key is at
  most 128 bytes and each before/after value is at most 1 KiB of printable
  single-line UTF-8. Each domain capability defines request, response,
  concurrency, and deadline bounds before it is linked. st2 rejects an
  oversized or invalid value without truncation and contains the failure to
  the affected observation or binding.
- **PROFILE-R16B Declared atomic demand:** Demand observation is explicitly
  declared and denied by default. Only a component descriptor with the
  `demand` capability may receive a demand invocation. Each invocation carries
  a positive demand watermark and a
  `ProposalFence { generation, revision, prior_digest }`. It returns exactly
  once with `Unchanged`; a bounded typed failure; or one `Publication`. There
  is no separate publication and settlement, digest supplied as authority by
  the component, or protocol observation timestamp.
- **PROFILE-R16C Coalesced, non-cancelling demand:** For one active binding
  generation, st2 keeps at most one demand invocation in flight and one latest
  trailing watermark. Demand accepted during an in-flight observation survives
  its result and coalesces into the trailing invocation. Only an exact atomic
  result, replacement of its fenced generation, or executor failure closes
  accepted work; no clock participates in correctness. `Committed` settles as
  `settledChanged` with the host-computed accepted-publication digest, including
  when resync delivery fails after the carrier and outbox intent commit;
  proposal-commit `Unchanged` settles as `settledUnchanged`. A missing active
  binding or one without demand maps to `absentBinding`; an older client
  generation maps to `staleGeneration`; a newer generation remains queued
  until supervisor refresh; provider failure maps to `providerUnavailable`.
  Client disconnect or wait expiry does not cancel accepted work, retract it,
  or alter observation scheduling.
- **PROFILE-R16D Durable demand intent:** Observe request and receipt records
  carry the exact schema identities `st2.resource-observe-request.v1` and
  `st2.resource-observe-receipt.v1`. They are private to one supervisor scope
  and bounded to 64 KiB each. Durable admission permits at most 256 unresolved
  requests per scope; an attempt beyond that cap receives submission
  backpressure before it is admitted, and request scanning remains bounded by
  the same cap. An admitted request record remains the durable, retryable
  intent until a terminal receipt is durably committed; only then may the
  request be removed. In-memory enqueue and nonterminal receipts do not
  transfer that ownership. A failed terminal receipt commit retains retryable
  state and leaves the request eligible for restart. Receipt status values use
  camelCase: `accepted` and `backpressured` are nonterminal;
  `settledUnchanged`, `settledChanged`, `settledFailed`, `absentBinding`,
  `staleGeneration`, and `providerUnavailable` are terminal. Only
  `settledChanged` carries the host-computed digest of accepted publication
  bytes. Provider diagnostics normalize to an optional bounded receipt value.

### Must bound attention and catch up to current state

- **PROFILE-R17 Semantic invalidation:** Every Resource invalidation carries the
  same bounded ordered fact envelope in its durable body and renders at most
  three whole facts into a subject of at most 96 Unicode scalars. For a
  published result, the host validates facts and topics, applies the binding
  selector, and derives the deterministic `PublicationIntent` committed with
  the carrier. Passive carrier changes publish one `content` topic and a short
  digest-transition fact. Agent Spec declaration changes publish ordered
  binding-label facts for added, removed, and semantically changed Resource
  declarations without exposing URIs or reasons; unavailable declaration
  parsing falls back to a digest-transition fact rather than dropping the
  invalidation. Snapshot bytes and provider payloads remain in the
  authoritative carrier, not the event.
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
- **PROFILE-R20 Observable health:** st2 reports component loading, descriptor,
  selector, capability, observation, proposal validation, publication, and
  delivery health separately. Failure degrades only the affected profile or
  binding, preserves the last proven snapshot with explicit freshness, and
  never presents stale bytes as newly observed state.

### Must keep the component authority narrow

- **PROFILE-R21 WASIp2 production baseline:** Observable providers target the
  WASIp2 Component Model. WASIp3 production execution is not part of this
  contract.
- **PROFILE-R22 No Store pooling:** Provider Stores and instances are never
  pooled, reused, or reset across observations.
- **PROFILE-R23 No generic host escape hatches:** The provider world exposes no
  caller-selected executable or arguments, shell, raw HTTP client, arbitrary
  filesystem path, or raw socket capability. Domain interfaces are not thin
  aliases for those authorities.
- **PROFILE-R24 One provider framework:** st2 does not maintain a parallel
  native or host-process provider framework. Observable profiles use the
  component envelope.
- **PROFILE-R25 No runtime component graph:** The host links one provider
  component directly to reviewed domain capabilities. Runtime WAC composition
  and provider-selected component graphs are not part of execution.

## Evidence

The closed resolver mechanism and sandbox bounds are supported by the
[plugin-boundary comparison](./.experiments/2026-08-26-plugin-boundary-comparison.md).
Composition against the real Nix-generated standing-seat shape is supported by
the [real-shape end-to-end experiment](./.experiments/2026-08-26-dotfiles-real-shape-e2e.md).
The state-first attention boundary and minimal catch-up state are supported by
the [GitHub attention-filter](./.experiments/2026-08-29-github-attention-filter-prototype.md)
and [lifecycle state-space](./.experiments/2026-08-29-smart-resource-lifecycle-prototype.md)
prototypes. The component envelope, domain-typed capabilities, fresh-Store
policy, cancellation fences, host-only commit, atomic outbox publication, and
separate idempotent delivery are supported by the
[WASIp2 component and atomic publication experiments](./.experiments/2026-09-01-wasip2-component-and-atomic-publication-prototypes.md).
