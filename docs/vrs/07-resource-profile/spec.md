# Resource Profile spec

This document specifies the Resource Profile registry, resolver SDK boundary, and
wasm execution contract. It builds on
[`requirements.md`](./requirements.md).

**Status:** Active

## Scope

This subsystem owns scheme-to-profile registration, the guest ABI, descriptor
and selector validation, sandbox budgets, host path containment, observable
runtime lifecycle, atomic snapshot publication, and the handoff of passive and
observable carriers to [`06-resync`](../06-resync/spec.md). It does not own
Resource URI semantics, provider authentication, provider observation strategy,
provider mutation, task launch, or a canonical provider event log. Those remain
downstream profile concerns or explicit non-goals.

## Architecture (PROFILE-R01..R20)

```text
Agent Spec resource URI (opaque, byte-preserved)
             |
             v exact RFC 3986 scheme
<catalog>/catalog.kdl
  profile "<scheme>" { wasm "<module>"; class "<class>"; notify-chain #true; }
             |
             +--> catalog-relative module -- normalized no-follow projection
             |                              + catalog root hash / transaction
             |
             v ResourceProfileRegistry (injectable; built-ins empty)
      bounded outcome cache keyed by normalized module path + admission policy + file identity
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

An observable profile extends the same contained carrier without changing URI
identity or introducing another delivery plane:

```text
closed wasm describe() -> capabilities + selector schema/default + topology
                                      |
catalog-trusted host runtime argv ----+
       |
       v provider-native observation
publish(binding-id, bytes, topics, facts)
       |
       v host validation + contained atomic replacement
canonical snapshot + current digest
       |
       v selector + pending-relevance reducer retaining topics + facts
built-in resync event (key=binding, supersede=true)
       |
       v existing inbox + DING
agent rereads canonical snapshot
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

`containment_root` is the trusted descriptor-traversal root for catalog-relative
modules and is absent for explicitly external absolute modules.

There is no template or exec variant. `ResourceProfileRegistry::builtin()` is
empty. `with_profile` and `with_profiles` inject catalog-owned registrations;
a later programmatic insertion for the same exact scheme replaces the prior
entry, while duplicate schemes in one catalog declaration are rejected before
registry construction.

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
```

Grammar:

```text
profile <non-empty-scheme> {        # exactly one positional value; no properties
  wasm <non-empty-path>              # exactly once
  class immediate|coalesced|silent  # zero or one; default coalesced
  notify-chain <boolean>             # zero or one; default false
}
```

The profile scheme follows RFC 3986: it begins with an ASCII letter, then
accepts ASCII alphanumeric characters plus `+`, `-`, and `.`, and rejects `/`;
lookup remains exact and case-sensitive. The profile
node takes exactly one quoted positional scheme and no properties. `wasm` and
`class` each take one quoted positional value; `notify-chain` takes one boolean.
Unknown or extra entries, unknown children, duplicate children, a missing
`wasm`, unsupported class values, and duplicate profile schemes fail parsing.
A literal absolute module path remains an external runtime input. Every other
declaration expands `$CATALOG` and environment variables, resolves lexically
against the catalog root, and must remain strictly beneath that root; internal
`.`/`..` components normalize away, while traversal outside the root fails
validation.

`st2 validate` reports malformed declarations and missing or unsafe
catalog-relative modules. `st2 up` loads declared profiles before it spawns
tasks, so a malformed profile block fails loudly rather than silently removing
watch coverage.

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
          v parse + expand each non-absolute wasm path
lexically normalized path strictly below catalog root
          |
          v descriptor-relative O_NOFOLLOW open
regular module <= 16 MiB
          |
          v deduplicate by normalized relative path
catalog transaction projection + declaration-root hash
          |
          v snapshot / digest / diff / bootstrap / apply / recovery
prepared bundle and live catalog contain the same catalog.kdl + module bytes
```

The whole-catalog projector parses only the exact `catalog.kdl` at its
projection root. Each catalog-relative module is opened from a retained root
capability: every ancestor is a no-follow directory and the final no-follow,
nonblocking descriptor must be a regular file no larger than the runtime's
16 MiB module cap. Missing files, symlinked ancestors or leaves, FIFOs and
other special files, paths escaping the root, and oversized modules reject the
transaction and `st2 validate`. Two profiles whose paths normalize to the same
relative path contribute one projected entry. Any file present in a prepared
catalog but absent from this closed projection remains an unprojected-input
error.

A literal absolute `wasm` path is external and immutable from the catalog
transaction's perspective. Its bytes are neither copied nor hashed, even when
the literal happens to name a file physically below the live catalog. A
non-absolute declaration that expands through `$CATALOG` or an environment
variable is catalog-owned and must still normalize beneath the logical catalog
root; expansion cannot turn a relative declaration into an escape hatch.

Apply publishes new or changed projected inputs before atomically replacing
`catalog.kdl`, then removes stale projected inputs after the declaration no
longer names them. The incomplete-apply marker fences cooperating catalog
readers throughout this sequence. The crash bias is a safe superset: failure
before the declaration replacement may leave an unreferenced new module, and
failure after it may leave an unreferenced old module, but the live
`catalog.kdl` never names a missing newly published module. Durable-stage
recovery repeats the same ordering and removes the superset.

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
`Instance`. One fuel allowance
covers the module start function and the first resolution call; a reused
instance receives one fresh allowance before each later call:

| Boundary | Contract |
| --- | --- |
| Module file | regular, nonblocking; catalog-relative paths use descriptor-relative no-follow traversal for every component; 16 MiB maximum before Wasmtime compilation |
| Imports | none; import-requiring modules fail instantiation |
| Fuel | 5,000,000 fuel units for start + first call; same budget per later call |
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

## Observable profile descriptor (PROFILE-R12..R13)

An observable profile retains the resolver ABI and adds one bounded descriptor
export. The descriptor is the single source of truth for the profile contract:

```text
describe() -> packed(ptr, len)
```

The returned UTF-8 JSON uses the same 64 KiB output bound, pointer checks,
fresh-instance policy, fuel budget, and no-import rule as `resolve`:

```json
{
  "abiVersion": 3,
  "capabilities": ["resolve", "read", "observe"],
  "selectorSchema": {
    "type": "object",
    "properties": {
      "topics": {
        "type": "array",
        "items": { "type": "string" },
        "uniqueItems": true
      }
    },
    "additionalProperties": false
  },
  "defaultSelector": {
    "topics": ["ci.failure", "mergeability.conflict", "review.requested"]
  },
  "topics": [
    { "name": "ci.failure" },
    { "name": "ci.success" },
    { "name": "mergeability.conflict" },
    { "name": "review.requested" }
  ],
  "runtime": { "topology": "shared" },
  "snapshot": {
    "mediaType": "application/json",
    "schemaId": "dev.example.github-pr.snapshot.v1"
  }
}
```

`abiVersion` governs the complete descriptor and host protocol. Capabilities
are closed strings known by that ABI version; v2 accepts `resolve`, `read`, and
`observe`. `topics[].name` values are unique, non-empty profile-owned semantic
identifiers. `defaultSelector` must validate against `selectorSchema` and name
only published topics. A binding selector is validated against the same schema
and topic set before registration. Selector configuration is observation
metadata: it is not part of the Resource URI and cannot change resolution,
snapshot bytes, credentials, or provider access.

Agent Spec KDL carries the normalized selector JSON as a `selector` raw-string
property on the Resource node:

```kdl
resource "pr" uri="github-pr://example/1" reason="Review." \
  selector=#"{"topics":["ci.failure","review.requested"]}"#
```

The canonical renderer serializes normalized compact JSON and chooses the
smallest raw-string hash fence whose closing delimiter does not occur in the
payload. JSON and TOML Agent Spec forms carry the selector as a native JSON
value. All forms lower to the same `serde_json::Value`; KDL spelling is not
preserved and cannot change selector semantics.

## Observable runtime declaration and protocol (PROFILE-R15..R16)

The closed wasm module never receives network, credential, filesystem, process,
or clock imports. A profile with `observe` therefore also has one
catalog-trusted host runtime declaration:

```kdl
profile "github-pr" {
  wasm "resolvers/github-pr.wasm"
  class "coalesced"
  runtime {
    argv "github-resource-runtime" "pr"
  }
}
```

`runtime` is forbidden unless the descriptor declares `observe`, and
`observe` is unusable without `runtime`. The block accepts exactly one
non-empty `argv` child and never invokes a shell. The executable is an external
operator-trusted input; the guest cannot choose or rewrite it. Environment,
credentials, egress, and provider permissions belong to the downstream runtime
deployment and are not inferred from URI possession.

The descriptor selects `shared` or `perBinding` topology. `shared` starts one
runtime for the exact `(catalog, scheme, profile generation)` and multiplexes
bindings. `perBinding` starts one instance for each active binding. A
per-binding runtime is the same protocol with one registration; topology does
not select another lifecycle model.

Both modes speak the same versioned, newline-delimited JSON protocol over
supervisor-owned stdin/stdout:

```text
host -> register {
  owner: { incarnation, claim },
  bindingId, registration, uri, selector, carrierPath, previousDigest?
}
host -> unregister {
  owner: { incarnation, claim },
  bindingId, registration
}

runtime -> publish {
  owner: { incarnation, claim },
  bindingId, registration,
  schemaId, mediaType, bytes, topics, facts?, observedAt?
}
runtime -> health {
  owner: { incarnation, claim },
  bindingId?, registration?,
  state: starting|ready|degraded|failed, detail?
}
```

The supervisor assigns a fresh directional owner claim to every runtime
incarnation. A new claim atomically fences the prior process and clears its
binding registrations. Every `publish` and binding-scoped `health` is accepted
only when both the owner claim and the host-generated registration token match
current state. `bindingId` is an opaque incarnation-scoped address, never the
binding name or URI.

EOF ends the runtime protocol. The supervisor's existing process lifecycle is
the only shutdown and restart authority; there is no protocol `shutdown`
message. Observation belongs to the implementation, so there is no host
`reconcile` message. The runtime begins or resumes provider-native observation
after `register` and may use `previousDigest` to avoid redundant publication.

Each encoded protocol line is at most 2 MiB, including the newline. `publish`
encodes `bytes` as a padded RFC 4648 base64 string; the decoded snapshot is
opaque. Selectors are at most 16 KiB when encoded as canonical compact JSON.
Decoded snapshot bytes are at most 1 MiB. Health `detail` is at most 16 KiB of
UTF-8. A publication has at most 32 ordered facts; keys are at most 128 bytes
and values at most 1 KiB of printable single-line UTF-8. A fact carries
`key` plus `before`, `after`, or both; explicit JSON null denotes absence.
These bounds are checked before allocation or decoding where the transport
permits and fail only the affected binding or runtime. st2 never truncates
canonical snapshot bytes, facts, or health text to satisfy a bound.

The host rejects unknown bindings, stale owners or registrations, mismatched
schema or media type, unpublished topics, invalid messages, output after
unregister, and messages exceeding protocol bounds. A shared-runtime protocol
failure degrades every registered binding honestly but cannot publish across
schemes, profile generations, runtime incarnations, or binding registrations.

Any provider cursor, webhook delivery identity, redelivery, polling interval,
rate-limit state, and repair strategy remain runtime-private.

## Snapshot publication (PROFILE-R14)

The resolver's contained carrier path is the observable snapshot path. A
successful `publish` follows one host-owned transaction:

```text
validate binding + schema + topics + facts + size
        |
        v
write new bytes to contained sibling temporary file
        |
        v fsync file + atomic rename + parent sync
        |
        v
compute/record current sha256 digest and freshness
        |
        `-> equal digest: no invalidation
            changed digest: apply binding selector and retain selected topics + facts

The runtime never writes the carrier directly. Descriptor-relative no-follow
containment from the existing resolver contract applies to the temporary file,
final file, and replacement. Publication failure preserves the last proven
snapshot and marks freshness/health degraded. The first successful publication
is a state transition from unavailable to readable. If the publication carries
at least one selected topic, st2 schedules the same superseding invalidation as
for every later changed digest. This explicit first-publication wake prevents a
live agent from retaining an unreadable view after delayed startup or recovery.
Equal publications and publications without selected topics remain silent.

Snapshot bytes are profile-defined and opaque to st2. `schemaId` and
`mediaType` make the bytes interpretable without making st2 own their semantics.
The first contract has one snapshot per binding: no named facets, generation
manifest, profile event log, or host retention policy.

## Semantic invalidation and catch-up (PROFILE-R17..R20)

For every changed digest, including the first successful publication, the host
validates and preserves the runtime's ordered facts and intersects
`publish.topics` with the normalized binding selector. An empty intersection
updates the canonical snapshot and freshness without scheduling delivery. A
non-empty intersection updates this bounded per-binding state:

```text
current_snapshot_digest: Digest?
last_delivered_digest: Digest?
pending_relevant_change: bool
pending_selected_topics: Topic[]
pending_facts: ResourceFact[]
deliverable: bool
```

If delivery is available, st2 emits one event on the existing built-in
`resync` stream:

```text
stream = resync
key = binding name
supersede = true
subject = <binding> · <up to three whole facts> [<topics>]
body = { binding, snapshotDigest, topics, facts }
```

Subjects are at most 96 Unicode scalars. Facts retain publication order and are
included only whole; topic space is reserved before facts are admitted. If no
fact fits, a compatible bounded fallback remains. The durable body always
retains the complete bounded fact list. It contains no snapshot bytes, provider
payload, credential, URI, reason, or provider cursor. Existing event
deduplication, inbox storage, DING rendering, and supersession apply unchanged.
Multiple topics for one atomic publication produce one invalidation.

If delivery is unavailable, a relevant publication replaces the pending
selected topics and facts with the latest relevant publication and sets
`pending_relevant_change = true`. Later irrelevant publications may advance
`current_snapshot_digest` but do not clear that state. When delivery becomes
available, st2 emits at most one invalidation for the then-current digest with
the retained latest relevant fact envelope, and clears it only after event
ingress accepts the record. No transition backlog exists. This is
level-triggered current-state catch-up, not event replay.

Health has separate descriptor, selector, runtime, observation, publication,
and delivery stages. Every stage reports affected scheme and binding without
including URI credentials or provider payloads. The last proven snapshot stays
readable with explicit freshness when observation fails; failure never relabels
old bytes as a newly observed snapshot.

## Design questions

- **DQ-P1 ABI compatibility:** Prove descriptor and host-protocol compatibility
  with frozen fixtures and cross-version conformance tests before third-party
  implementations.
- **DQ-P2 Runtime observability:** Derive the minimum low-noise health, freshness,
  log, span, metric, and operator surfaces from GitHub-profile dogfood.

Resolved design questions and their evidence remain recorded in
[`open-questions.md`](./open-questions.md).
