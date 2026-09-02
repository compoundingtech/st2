# Resource Profile spec

This document specifies the Resource Profile registry, closed core-wasm
resolver, universal WASIp2 observable-provider envelope, and state-first
atomic publication authority. It builds on
[`requirements.md`](./requirements.md).

## Status

Draft. The runtime-neutral fenced proposal, atomic publication, and durable
outbox contract is the foundation shared by passive and component-produced
publications. Component-enabled conformance additionally requires the execution
and capability boundary below; a native runtime is not a fallback.

## Ownership and flow

This subsystem owns scheme-to-profile registration, both guest ABIs, descriptor
and selector validation, sandbox budgets, host path containment, provider
component lifecycle, host-only atomic proposal commit, and the handoff of
passive and observable carriers to [`06-resync`](../06-resync/spec.md). It does
not own Resource URI semantics, provider credentials, provider mutation, task
launch, a canonical provider event log, or ambient host access. Those remain
downstream semantics or explicit non-goals.

## Architecture (PROFILE-R01..R25)

Resolution remains a closed core-wasm operation:

```text
Agent Spec resource URI (opaque, byte-preserved)
             |
             v exact RFC 3986 scheme
<catalog>/catalog.kdl
  profile "<scheme>" { wasm "<resolver>"; class "<class>"; ... }
             |
             +--> catalog-relative resolver -- normalized no-follow projection
             |                                + catalog root hash / transaction
             |
             v ResourceProfileRegistry (injectable; built-ins empty)
      bounded compiled-module cache
             |
             v fresh Store + Instance per resolution
   closed core-wasm guest (no imports / no WASI)
             |
             v UTF-8 JSON path -> host lexical containment
      Resolution { path, containment_root, declared ProfileClass }
             |
             v descriptor-relative, no-follow digest reads
      resync watch set and existing event pipeline
```

An observable profile adds one component artifact; it does not replace or
enlarge the resolver:

```text
WASIp2 provider component
  export describe()
  export observe(request)
  import only reviewed domain capability interfaces
             |
             v fresh Store + Instance for one call
Unchanged | typed failure | Publication
             |
             v host pairs Publication with ProposalFence
validate generation + revision + prior digest + payload bounds
             |
             v one host-owned atomic transition
carrier + current digest/revision + freshness/catch-up + PublicationIntent
             |
             v separate idempotent delivery from durable outbox
built-in resync event (key=binding, supersede=true)
             |
             v existing inbox + DING
agent rereads canonical carrier
```

The SDK is a typed, trait-shaped boundary rather than a set of scheme-specific
branches. Its concrete public surface is `ResourceProfile`, `ProfileSource`,
`ResourceProfileRegistry`, `Resolution`, `WasmResolver`, `WasmInstance`, and
`WasmResolveError`. The registry expresses the resolver contract as:

```rust
fn try_resolve(
    &self,
    agent_dir: &Path,
    uri: &str,
) -> Result<Option<Resolution>, String>;
```

`Ok(None)` means no syntactically valid scheme or no exact registration.
`Ok(Some(_))` is a contained local denotation, its enforced read root, and the
catalog-declared class. `Err(_)` means the scheme was registered but its wasm
implementation was unavailable or failed. `resolve` is the containment-oriented
convenience API that folds the final case into unwatchable; callers needing
diagnostics use `try_resolve`.

There is one source variant:

```rust
ProfileSource::Wasm {
    module: PathBuf,
    class: ProfileClass,
    containment_root: Option<PathBuf>,
}
```

`containment_root` is the trusted descriptor-traversal root for
catalog-relative resolver modules and is absent for explicitly external
absolute modules. Observable component configuration is separate from
`ProfileSource`: it cannot participate in path resolution or change
`ProfileClass`.

There is no template or exec variant. `ResourceProfileRegistry::builtin()` is
empty. `with_profile` and `with_profiles` inject catalog-owned registrations;
a later programmatic insertion for the same exact scheme replaces the prior
entry, while duplicate schemes in one catalog declaration are rejected before
registry construction. Observable execution is present only when the same
profile also declares one component and its exact allowed WIT capability
interfaces.

## Catalog declaration (PROFILE-R02..R03)

Profiles are top-level `catalog.kdl` nodes, siblings of the optional `catalog`
block:

```kdl
catalog {
  pty-root "pty"
}

profile "dev.schickling.agent-goal" {
  wasm "resolvers/agent-goal.wasm"
  class "immediate"
  notify-chain #true
}

profile "github-issue" {
  wasm "resolvers/github-issue.wasm"
  component "providers/github-issue.component.wasm"
  capability "st2:github-issue/source@1.0.0"
  class "coalesced"
}
```

Grammar:

```text
profile <non-empty-scheme> {        # exactly one positional value; no properties
  wasm <non-empty-path>              # exactly once; closed resolver
  component <non-empty-path>         # zero or one; observable provider
  capability <wit-interface-id>      # zero or more; requires component
  class immediate|coalesced|silent  # zero or one; default coalesced
  notify-chain <boolean>             # zero or one; default false
}
```

The profile scheme follows RFC 3986: it begins with an ASCII letter, then
accepts ASCII alphanumeric characters plus `+`, `-`, and `.`, and rejects `/`;
lookup remains exact and case-sensitive. The profile node takes exactly one
quoted positional scheme and no properties. `wasm`, `component`, and `class`
each take one quoted positional value; `notify-chain` takes one boolean and
each `capability` takes one exact versioned WIT interface ID. Capability IDs
are owned by their WIT package namespace; the host compares the canonical
package, interface, and complete semantic version, not a display alias.
Duplicate capability IDs, capability without component, unknown or extra
entries, duplicate singleton children, missing `wasm`, unsupported class
values, and duplicate profile schemes fail parsing.

A literal absolute wasm artifact path remains an external runtime input. Every
other resolver or component declaration expands `$CATALOG` and environment
variables, resolves lexically against the catalog root, and must remain
strictly beneath that root; internal `.`/`..` components normalize away, while
traversal outside the root fails validation.

`st2 validate` reports malformed declarations, missing or unsafe
catalog-relative artifacts, provider-world mismatches, and component imports
outside the profile's exact capability set. `st2 up` loads declared profiles
before it spawns tasks, so an invalid profile fails loudly rather than silently
removing watch or observation coverage.

The scheme namespace remains downstream-owned. A private profile uses its
owner's reverse-domain scheme (for example `dev.schickling.agent-goal`); st2
registers no public scheme and assigns no semantics to the URI authority or
path. Representative behavior:

| Input | Registration | Result |
| --- | --- | --- |
| `dev.schickling.agent-goal://dev3/cos` | exact profile declared | invoke that module |
| `worktree://repo/main` | none | opaque and unwatchable |
| `resources/goal.md` | no URI scheme | profile registry miss; resync's local-path rule applies |
| `file:///tmp/x` | none | profile registry miss; resync's `file://` rule applies |
| `dev.schickling.agent-goal://x` with broken module | exact profile declared | registered-profile failure; no fallback |

## Catalog transaction projection (PROFILE-R10)

```text
exact root catalog.kdl
          |
          v parse + expand each non-absolute wasm/component path
lexically normalized path strictly below catalog root
          |
          v descriptor-relative O_NOFOLLOW open
regular bounded artifact
          |
          v deduplicate by normalized relative path
catalog transaction projection + declaration-root hash
          |
          v snapshot / digest / diff / bootstrap / apply / recovery
prepared bundle and live catalog contain the same declaration + artifact bytes
```

The whole-catalog projector parses only the exact `catalog.kdl` at its
projection root. Each catalog-relative resolver module and provider component
is opened from a retained root capability: every ancestor is a no-follow
directory and the final no-follow, nonblocking descriptor must be a regular
file within its admission bound. Missing files, symlinked ancestors or leaves,
FIFOs and other special files, paths escaping the root, and oversized artifacts
reject the transaction and `st2 validate`. References whose paths normalize to
the same relative path contribute one projected entry. Any file present in a
prepared catalog but absent from this closed projection remains an
unprojected-input error.

A literal absolute `wasm` or `component` path is external and immutable from
the catalog transaction's perspective. Its bytes are neither copied nor hashed,
even when the literal happens to name a file physically below the live catalog.
A non-absolute declaration that expands through `$CATALOG` or an environment
variable is catalog-owned and must still normalize beneath the logical catalog
root; expansion cannot turn a relative declaration into an escape hatch.

Apply publishes new or changed projected inputs before atomically replacing
`catalog.kdl`, then removes stale projected inputs after the declaration no
longer names them. The incomplete-apply marker fences cooperating catalog
readers throughout this sequence. The crash bias is a safe superset: failure
before declaration replacement may leave an unreferenced new artifact, and
failure after it may leave an unreferenced old artifact, but live `catalog.kdl`
never names a missing newly published artifact. Durable-stage recovery repeats
the same ordering and removes the superset.

## Core wasm ABI (PROFILE-R04..R05)

The resolver is a core wasm module, not a WASI program or component. It imports
nothing and exports exactly the functions/memory the host calls:

```text
memory
alloc(len: i32) -> i32
resolve(uri_ptr: i32, uri_len: i32,
        dir_ptr: i32, dir_len: i32) -> i64
```

The host copies the preserved URI and the UTF-8 agent-directory path into guest
linear memory through `alloc`. `resolve` returns a packed unsigned
`(ptr << 32) | len`. The selected bytes must be UTF-8 JSON:

```json
{"path":"resources/goal.md","class":"goal"}
```

`path` is required and non-empty. `class` is an optional, free-form guest
classification and is not the resync notification class: notification policy
comes only from the trusted catalog declaration's `ProfileClass`. This prevents
an untrusted guest from escalating a `silent` profile to `immediate`.

Before reading or acting, the host checks allocator pointers and the returned
pointer/length range against linear memory, parses UTF-8 and JSON, rejects an
empty path, joins the returned path to `agent_dir`, and lexically normalizes
`.`/`..`. A result equal to or outside `agent_dir`, or one crossing an existing
symlink below it, is rejected; a resolver must denote a carrier beneath the
root rather than the confinement root itself. Existence is not required at
resolution time; the carrier may be created later.

Successful resolution carries the normalized agent-directory root into resync.
Every later digest read opens that root and then each relative component with
no-follow semantics, using each directory descriptor for the next lookup, and
reads the already-open final descriptor. A root, ancestor, or final component
replaced by a symlink before or during traversal fails closed; admission-time
metadata alone is never treated as a durable proof.

## Runtime containment (PROFILE-R04..R07)

Each module is opened nonblocking and no-follow, accepted only as a regular
file, and read through a 16 MiB admission cap before validation or compilation.
Catalog-relative modules are traversed descriptor-relative from the catalog
root with `O_NOFOLLOW` on every ancestor and the final component.
The bounded 32-entry LRU cache stores both successful modules and compilation
failures by normalized module path, admission policy, byte digest, and stable
file metadata. Admission policy distinguishes an external open from
descriptor-relative traversal under a specific confinement root. Lexical
normalization is used only for cache identity; the admission read always opens
the declared module spelling so filesystem-significant `..` after a symlink or
missing component cannot be erased. Refresh-local snapshot outcomes use the
same path-and-policy key, so resolving one spelling externally can never
authorize a contained declaration. Registry clones and concurrent subscribers
coalesce one compilation attempt only for an unchanged identity under the same
policy. Byte replacement or metadata identity change invalidates that entry.
Each resolution of a successfully compiled module creates a fresh `Store` and
`Instance`. One fuel allowance covers the module start function and its single
resolution call; the instance is then discarded.

| Boundary | Contract |
| --- | --- |
| Module file | regular, nonblocking; catalog-relative paths use descriptor-relative no-follow traversal for every component; 16 MiB maximum before Wasmtime compilation |
| Imports | none; import-requiring modules fail instantiation |
| Fuel | 5,000,000 fuel units for start + the single resolution call |
| Linear memory | 64 MiB maximum |
| Resolver return | memory range must be valid and at most 64 KiB before UTF-8/JSON decoding |
| Memories | at most 1 |
| Tables | at most 4, with at most 10,000 elements each |
| Instance state | fresh per registry resolution |
| Compiled code/failure | 32-entry LRU by normalized module path + admission policy (external or exact confinement root) + byte digest + file metadata; shared across registry clones only for the same policy |

Failure taxonomy:

| Failure | SDK result | Supervisor effect |
| --- | --- | --- |
| special/oversized module or module load/instantiation | `Instantiation` | binding unwatchable; reconcile warning; supervisor lives |
| missing `memory`, `alloc`, or `resolve` | `MissingExport` | same |
| unreachable/stack/memory trap | `Trap` | same |
| infinite start function or call | `FuelExhausted` | same |
| invalid pointer, oversized return, UTF-8, JSON, empty/escaped path, symlink, or special-file read | `BadReturn` or unreadable carrier | same |
| feature disabled | registered-profile error | same; no alternate resolver |

All wasmtime code and dependencies are gated by `wasm-resolver`, forwarded from
the root st2 crate to `agent-spec`. Without that feature the declaration and
registry types remain available, but registered resolution reports the missing
feature. Default binaries therefore retain the baseline dependency surface.
Building a Rust guest for `wasm32-unknown-unknown` requires `lld`, which the
repository dev shell supplies.

## Resync composition (PROFILE-R08..R09, PROFILE-R11)

For each active Resource binding, resync applies this precedence:

1. A URI with a registered `silent` profile is excluded without loading or
   executing its resolver. Any other registered profile resolves through the
   registry. Success adds the contained path with the profile's declared
   class. Failure leaves only that binding unwatchable and adds a reconcile
   warning naming the agent and binding.
2. An unregistered `file://` URI uses the existing absolute-file rule.
3. A schemeless path uses the existing agent-directory-relative rule.
4. Every other unregistered scheme remains opaque and unwatchable.

Passive resolved carriers use the same Resource fact envelope as observable
publications. A content transition emits topic `content` and one
`digest=<old-short>→<new-short>` fact. Declaration subscriptions retain a
bounded summary keyed by binding label whose values digest URI, reason,
inactive state, and selector. On a declaration flush, one bounded catalog parse
derives the current summary: added labels transition absent→`declared`, removed
labels transition `declared`→absent, and changed summary digests publish
`label=changed`, ordered by label. URIs and reasons never enter the event. A
parse failure, unchanged Resource summary, or summary outside fact bounds falls
back to the declaration carrier's digest transition. Parent-directory
observation, rename replacement, digest seeding, equal-byte deduplication,
deterministic transition identity, and bounded windows otherwise remain the
[`06-resync`](../06-resync/spec.md) pipeline.

`notify-chain #true` extends only subscription selection. For each active
binding through that profile, resync validates the bound agent's supervisor
chain against the complete catalog and adds every active same-scheme carrier
declared by non-retired ancestors. Each ancestor URI is resolved unchanged
against that ancestor's own directory and remains inside that directory's host
containment root. Retired ancestors are skipped without severing traversal.
Owner-qualified event keys keep layers independent. An invalid chain or failed
ancestor resolution produces a reconcile warning; st2 never synthesizes a URI
or silently claims complete chain coverage. The default `false` preserves the
agent-local behavior above.
Profile resolution is observation metadata only and never enters task launch
targets.

## Observable provider world (PROFILE-R12..R13)

An observable profile retains the closed resolver and adds one component that
conforms to the repository-owned
`st2:resource-provider/provider@0.1.0` WASIp2 world. The package and interface
names are lowercase ASCII WIT identifiers and the complete semantic version is
part of compatibility. A host accepts only an exact supported world; it does
not guess across unknown major, minor, or patch versions.

The world has two host-called exports:

```text
describe() -> result<ProviderDescriptor, DescriptorError>

observe(ObserveRequest {
  uri,
  selector,
  prior_digest?,
  demand_watermark?
}) -> ObservationResult {
  Unchanged
  | Failed { diagnostic? }
  | Published { publication: Publication }
}

Publication {
  schema_id,
  media_type,
  bytes,
  topics,
  facts?
}
```

`ProviderDescriptor` declares supported scheduling capabilities, selector
schema, default selector, published semantic topics, snapshot media type, and
snapshot schema identity. The typed provider world replaces descriptor ABI 3's
packed core-wasm JSON export and the host-process JSON protocol. It does not
replace the resolver ABI.

The host calls `describe` in a fresh Store under descriptor phase policy.
Operational domain capabilities return a typed phase denial if the component
attempts to call them while describing itself. The descriptor is bounded and
fully validated before any binding is activated. Unknown required
capabilities, duplicate or empty topic names, an invalid selector schema or
default, mismatched snapshot identity, and an unsupported world version fail
only that profile.

Agent Spec KDL carries normalized selector JSON as a `selector` raw-string
property:

```kdl
resource "pr" uri="github-pr://example/1" reason="Review." \
  selector=#"{"topics":["ci.failure","review.requested"]}"#
```

The canonical renderer serializes compact normalized JSON and chooses the
smallest raw-string hash fence whose closing delimiter does not occur in the
payload. JSON and TOML Agent Spec forms carry the selector as a native JSON
value. All forms lower to the same `serde_json::Value`; KDL spelling is not
preserved. The selected value must validate against the descriptor schema and
may name only published topics. It cannot change resolution, snapshot bytes,
credentials, linked capabilities, or provider authority.

## Component execution and capabilities (PROFILE-R15..R16A, R21..R25)

```text
host scheduler
     |
     v load exact catalog-selected Component
reuse Engine + Linker + compiled Component when cache identity matches
     |
     v create fresh Store<ProviderHost>
link only catalog-approved domain WIT interfaces
     |
     v instantiate once; describe or observe once
     |
     v validate typed result; cancel outstanding capability work
drop instance + Store
```

The host creates a new `Store<ProviderHost>` and component instance for every
descriptor call and observation. No Store, instance, guest memory, table,
resource handle, host call counter, deadline state, fuel state, or cancellation
state survives the call. An observation invokes `observe` exactly once.

One process may retain an Engine, Linker, and compiled Component. Cache identity
includes the exact component SHA-256, Wasmtime/runtime build identity, target
triple, and a fingerprint of every compilation-relevant Engine setting. A
host-produced AOT artifact is deserialized only after its length and SHA-256
match an authenticated host-owned manifest and that complete compatibility key.
Any mismatch is a cache miss. Guest-supplied precompiled bytes never cross the
unsafe deserialization boundary.

Provider components are WASIp2 components but are not WASI command programs.
The Linker supplies neither `wasi:cli`, inherited environment, ambient clocks
or random, filesystem preopens, sockets, raw HTTP, nor a caller-controlled
process API. Every non-foundation import must:

1. be an exact versioned WIT interface listed by the profile's `capability`
   declarations;
2. represent one provider-domain operation with typed inputs, outputs, policy
   denials, and operational failures;
3. keep credentials, endpoint selection, allowlists, and authoritative
   resource handles in host state rather than guest-selected strings;
4. define request, response, retained-output, concurrency, and deadline bounds;
5. redact secrets and provider payloads from errors, health, and receipts; and
6. be async and cancellation-safe when it can block.

For example, a GitHub Issue source capability may accept a typed
`{ owner, repository, number }` and return a bounded typed issue response while
the host fixes HTTPS endpoint policy, authentication, redirects, and deadlines.
A local PTY statistics capability may accept a closed `scope` variant while the
host fixes the executable, argument shape, empty environment, working
directory, output caps, deadline, and process containment. An interface that
accepts a URL, executable path, arbitrary argument vector, environment, cwd,
filesystem path, socket address, or shell text is generic authority even if its
package name sounds domain-specific and is non-conforming.

Cancellation owns both sides of the boundary. Epoch interruption bounds
non-yielding guest CPU; cancellation or drop of the invocation future must
cancel blocking host imports; capability implementations must reap owned work;
and Store drop occurs only after the in-flight call no longer borrows it. A
cancelled observation cannot commit even if the component already returned a
value.

The executor enforces finite component-byte, memory, table, fuel, result, and
deadline bounds as one versioned Engine policy. Snapshot bytes decode to at
most 1 MiB; canonical selector JSON is at most 16 KiB; health and failed-result
diagnostics are at most 16 KiB UTF-8; and one `Publication` carries at most 32
ordered facts with the key and value limits in
[`PROFILE-R16A`](./requirements.md). Bounds are checked before allocation or
decoding where the typed transport permits. Values are rejected, never
truncated.

Observation uses one directly linked provider component. There is no
long-lived runtime topology, stdin/stdout protocol, runtime-selected WAC graph,
or parallel native-provider lifecycle. Provider ETags, cursors, conditional
caches, rate limits, webhook repair, and backoff therefore live in bounded
host-owned domain capability state or explicit durable provider state, never
in Store lifetime.

## Demand invocation (PROFILE-R16B..R16D)

Demand remains deny-by-default. A descriptor must declare `demand` before the
host supplies a positive demand watermark in `ObserveRequest`. A missing
watermark is an ordinary scheduled observation; a present watermark identifies
host work but is not provider history, a provider-specific reconcile command,
or authority to mutate provider state.

For each active binding generation the host keeps at most one observation in
flight and one latest trailing demand watermark. Demand admitted during the
in-flight invocation survives its result and coalesces into the trailing
invocation. Replacement of the binding generation fences the old invocation.
Executor failure provides failure evidence. A client wait deadline limits only
that client's wait and never cancels admitted demand.

The private durable request and receipt records remain bounded to 64 KiB and
use schema identities `st2.resource-observe-request.v1` and
`st2.resource-observe-receipt.v1`. One supervisor scope admits at most 256
unresolved requests and scans at most that cap. An admitted request remains the
durable retryable intent until a terminal receipt commits; in-memory enqueue,
client disconnect, wait expiry, and nonterminal receipts do not transfer or
cancel ownership.

Receipt statuses are camelCase. `accepted` and `backpressured` are nonterminal.
The terminal set is exactly `settledUnchanged`, `settledChanged`,
`settledFailed`, `absentBinding`, `staleGeneration`, and
`providerUnavailable`. Only `settledChanged` carries the host-computed digest.
An active provider without `demand` maps to `absentBinding` with diagnostic
`the profile component does not declare the demand capability`. A client
generation older than the resident generation is `staleGeneration`; a newer
generation remains queued until supervisor refresh.

## Fenced atomic publication (PROFILE-R14, R16)

Before invocation the host reads the binding's current state and creates:

```rust
ProposalFence {
    generation,
    revision,
    prior_digest,
}
```

`generation` changes when the binding is replaced. `revision` advances only on
a committed publication in that generation. `prior_digest` is the current
carrier digest or absence. The fence belongs to the host invocation; the
component cannot choose it or refresh it after observation.

`Unchanged` closes the observation without mutation. `Failed` records bounded
health and preserves the last proven carrier. For `Published`, the host pairs
the returned `Publication` with the invocation fence, validates current binding
identity, schema and media type, byte and fact bounds, published topics,
selector relevance, and path containment, then computes the authoritative
snapshot digest and deterministic proposal identity.

The validated internal `PublicationIntent` carries binding identity, the
deterministic `proposal_id`, generation, expected revision and prior digest,
resulting digest, selected topics, and ordered facts. It contains delivery
intent and semantic metadata, not snapshot bytes or provider credentials.

The proposal ID is:

```text
SHA-256(
  "st2.resource-publication-proposal.v1\0" ||
  serde_json::to_vec(ProposalIdentity {
    bindingId,
    generation,
    expectedRevision,
    priorDigest,
    digest,
    selectedTopics,
    facts
  })
)
```

`ProposalIdentity` is serialized as camelCase JSON with fields in exactly the
displayed order. `digest` is the host-computed accepted snapshot digest;
`selectedTopics` and ordered `facts` are the post-validation semantic envelope.
Including that envelope prevents equal carrier bytes with conflicting delivery
meaning from sharing an identity.

```rust
ProposalCommit::Committed(PublicationCommit)
ProposalCommit::AlreadyCommitted(PublicationCommit)
ProposalCommit::Unchanged { generation, revision, digest }
ProposalCommit::StaleGeneration { actual_generation, actual_revision }
ProposalCommit::StalePrior {
    actual_generation,
    actual_revision,
    actual_digest,
}

PublicationCommit {
    proposal_id,
    generation,
    resulting_revision,
    digest,
}
```

`Committed` is the single state transition. `AlreadyCommitted` is an
idempotent retry of the same deterministic proposal and returns the original
commit identity. `Unchanged` reports equal accepted bytes without advancing
revision or creating an outbox intent. `StaleGeneration` fences replacement.
`StalePrior` rejects a competing proposal whose expected revision or digest is
no longer current. Concurrent proposals from one prior state therefore have at
most one winner.

The storage transaction makes these values visible together:

```text
contained carrier bytes
current generation + resulting revision + digest + freshness
lastIntent: complete deterministic PublicationIntent
pending delivery reducer state
```

Before that publication point, readers observe the complete prior state and no
new intent. After it, readers observe the complete successor and its intent.
A process crash before publication leaves the prior state authoritative. A
crash after publication but before acknowledgement leaves the successor and
intent durable. Retrying the same proposal returns `AlreadyCommitted`; an
outbox worker may retry delivery by `proposal_id` until its durable
acknowledgement exists.

Equal snapshot bytes return `Unchanged`; they do not advance revision or create
an intent. The initial accepted publication changes the binding from
unavailable to readable; when it has selected topics, its atomic intent
schedules the same superseding invalidation as a later relevant change.

Atomic publication is runtime-neutral: passive observation and the component
executor submit the same fenced `Publication` to one host API. The foundation
is conforming without an enabled executor, but observable provider execution is
conforming only through the component world; there is no native fallback.

## Semantic invalidation and catch-up (PROFILE-R17..R20)

For every changed digest, including the initial accepted publication, proposal
validation preserves the `Publication`'s ordered facts and intersects its topics
with the normalized binding selector before commit. The resulting
`PublicationIntent` retains selected topics and facts with the committed
snapshot digest. An empty intersection updates canonical state and freshness
but requires no agent wake.

The durable binding and catch-up state is:

```text
generation: u64
revision: u64
current_snapshot_digest: Digest?
last_intent: PublicationIntent?
last_delivered_digest: Digest?
pending_relevant_change: bool
pending_from_last_intent: bool
pending_selected_topics: Topic[]
pending_facts: ResourceFact[]
deliverable: bool
```

`last_intent` is the full deterministic intent: proposal and binding IDs,
generation, expected revision, prior and resulting digests, selected topics,
and ordered facts. `last_commit()` derives its receipt from that authority
rather than storing a second commit record. When pending delivery refers to
`last_intent`, `pending_from_last_intent` is true and the pending topic/fact
fields stay empty; readers derive that envelope from the intent.

Publication never calls the event sink inside the commit transaction. Recovery
folds an eligible staged WAL intent into this durable state atomically with the
carrier and removes the WAL; eligibility requires its resulting digest to match
the authoritative carrier. A separate outbox worker reads `last_intent`. If the
resulting state is relevant and delivery is available, the worker emits one
event on the existing built-in `resync` stream:

```text
stream = resync
key = binding name
supersede = true
idempotency = PublicationIntent.proposal_id
subject = <binding> · <up to three whole facts> [<topics>]
body = { binding, snapshotDigest, topics, facts }
```

Subjects are at most 96 Unicode scalars. Facts retain publication order and are
included only whole; topic space is reserved before facts are admitted. If no
fact fits, a compatible bounded fallback remains. The durable body retains the
complete bounded fact list. It contains no snapshot bytes, provider payload,
credential, URI, reason, or provider cursor. Existing inbox storage, DING
rendering, deduplication, and supersession apply unchanged. Multiple topics for
one atomic publication produce one invalidation.

Acknowledgement is a separate durable transition keyed by deterministic
`proposal_id`. Loss of acknowledgement leaves the intent retryable; a retry
uses the same event identity. An obsolete intent is not replayed as history:
the worker reconciles against current binding state and may replace delivery
with at most one invalidation for the then-current digest.

If delivery is unavailable, a relevant publication sets
`pending_relevant_change` and initially points it at `last_intent` without
duplicating topics or facts. Before a later irrelevant publication replaces
`last_intent`, the reducer materializes the older relevant envelope into
`pending_selected_topics` and `pending_facts` and clears
`pending_from_last_intent`. The later publication may advance the current
digest but does not clear pending relevance. When delivery becomes available,
catch-up emits at most one invalidation for the then-current digest with the
retained latest relevant envelope, and clears pending state only after durable
acknowledgement. No transition backlog is delivered. This is level-triggered
current-state catch-up, not event replay.

Health has separate component loading, descriptor, selector, capability,
observation, proposal validation, publication, and delivery stages. Every stage
reports affected scheme and binding without including URI credentials or
provider payloads. The last proven snapshot stays readable with explicit
freshness when observation fails; failure never relabels old bytes as newly
observed state.

## Deliberate exclusions and evidence gates (PROFILE-R21..R25)

| Exclusion | Evidence required before reconsideration |
| --- | --- |
| WASIp3 production execution | A stable Wasmtime/WIT toolchain and component ecosystem, a production provider requirement that WASIp2 cannot express, cross-version fixtures, and cancellation plus capability-containment evidence at least as strong as the WASIp2 suite. |
| Store or instance pooling | Representative provider profiles showing fresh Store creation materially violates an accepted latency or resource bound, plus an exhaustive reset proof covering guest memory, tables, resources, host state, traps, fuel, deadlines, async imports, and cancellation with zero cross-observation leakage. |
| Generic exec, raw HTTP, arbitrary filesystem, or raw socket authority | A real provider operation that cannot be represented as a narrower typed domain interface, an explicit threat and credential model, fixed authority and resource bounds, cancellation/containment tests, and an accepted decision for the enlarged trust boundary. |
| Parallel native or host-process provider framework | A production provider that cannot run through the component world, measurements showing the incompatibility is fundamental rather than packaging cost, and evidence that a second lifecycle, health, fencing, and security model is safer and simpler than extending typed capabilities. |
| Runtime WAC or provider-selected component graph | At least two production providers requiring runtime composition, a closed graph ownership and versioning model, transitive capability review, deterministic failure/fencing semantics, and measurements showing host linkage is the limiting constraint. |

These are evidence gates, not deferred implementation commitments. Until a gate
is met and an accepted decision changes the contract, the excluded mechanism is
non-conforming.

## Design questions

- **DQ-P1 Component compatibility:** Prove old-component/new-host,
  new-component/old-host, provider-world, and domain-capability compatibility
  with frozen WIT fixtures and a cross-version conformance matrix before
  independently released third-party components or capabilities.
- **DQ-P2 Provider observability:** Derive the minimum low-noise component,
  capability, proposal, publication, freshness, outbox, and delivery health
  surfaces from one operated GitHub provider.

Resolved design questions and their evidence remain recorded in
[`open-questions.md`](./open-questions.md).
