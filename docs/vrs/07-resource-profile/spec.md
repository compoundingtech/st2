# Resource Profile spec

This document specifies the Resource Profile registry, resolver SDK boundary, and
wasm execution contract. It builds on
[`requirements.md`](./requirements.md).

**Status:** Active

## Scope

This subsystem owns scheme-to-resolver registration, the guest ABI, sandbox
budgets, host path containment, transactional ownership of catalog-relative
modules, and the handoff of resolved carriers to
[`06-resync`](../06-resync/spec.md). It does not own Resource URI semantics,
remote access, Agent Spec binding grammar, event delivery, or task lifecycle.
Those remain downstream profile concerns or existing root contracts.

## Architecture (PROFILE-R01..R10)

```text
Agent Spec resource URI (opaque, byte-preserved)
             |
             v exact RFC 3986 scheme
<catalog>/catalog.kdl
  profile "<scheme>" { wasm "<module>"; class "<class>"; }
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
}
```

Grammar:

```text
profile <non-empty-scheme> {        # exactly one positional value; no properties
  wasm <non-empty-path>              # exactly once
  class immediate|coalesced|silent  # zero or one; default coalesced
}
```

The profile scheme follows RFC 3986: it begins with an ASCII letter, then
accepts ASCII alphanumeric characters plus `+`, `-`, and `.`, and rejects `/`;
lookup remains exact and case-sensitive. The profile
node takes exactly one quoted positional scheme and no properties. Each child
takes exactly one quoted positional value. Unknown or extra entries, unknown
children, duplicate `wasm`, duplicate `class`, a missing `wasm`, unsupported
class values, and duplicate profile schemes fail parsing. A literal absolute
module path remains an external runtime input. Every other declaration expands
`$CATALOG` and environment variables, resolves lexically against the catalog
root, and must remain strictly beneath that root; internal `.`/`..` components
normalize away, while traversal outside the root fails validation.

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
descriptor-relative traversal under a specific confinement root. Refresh-local
snapshot outcomes use the same path-and-policy key, so resolving one spelling
externally can never authorize a contained declaration. Registry clones and
concurrent subscribers coalesce one compilation attempt only for an unchanged
identity under the same policy. Byte replacement or metadata identity change
invalidates that entry and retries compilation.
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

## Resync composition (PROFILE-R08..R09)

For each active Resource binding, resync applies this precedence:

1. A URI with a registered `silent` profile is excluded without loading or
   executing its resolver. Any other registered profile resolves through the
   registry. Success adds the contained path with the profile's declared
   class. Failure leaves only that binding unwatchable and adds a reconcile
   warning naming the agent and binding.
2. An unregistered `file://` URI uses the existing absolute-file rule.
3. A schemeless path uses the existing agent-directory-relative rule.
4. Every other unregistered scheme remains opaque and unwatchable.

After a path enters the watch set, Resource Profiles add no event semantics.
Parent-directory observation, rename replacement, digest seeding, equal-byte
deduplication, deterministic transition identity, bounded windows, and built-in
`resync` delivery remain the [`06-resync`](../06-resync/spec.md) pipeline.
Profile resolution is observation metadata only and never enters task launch
targets.

## Design questions

- **DQ-P1 ABI compatibility:** What explicit version negotiation replaces the
  current unversioned three-export ABI before independently released third-party
  modules need compatibility guarantees? Resolve with a compatibility matrix
  and an old-guest/new-host conformance test.
- **DQ-P2 Runtime observability:** Which structured log/span/metric surface must
  report registered-profile failures without making hot-path cache hits noisy?
  Resolve by dogfood evidence that distinguishes module defects, hostile input,
  and feature-disabled builds.
