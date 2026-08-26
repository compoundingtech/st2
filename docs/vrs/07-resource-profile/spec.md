# Resource Profile spec

This document specifies the Resource Profile registry, resolver SDK boundary, and
wasm execution contract. It builds on
[`requirements.md`](./requirements.md).

**Status:** Active

## Scope

This subsystem owns scheme-to-resolver registration, the guest ABI, sandbox
budgets, host path containment, and the handoff of resolved carriers to
[`06-resync`](../06-resync/spec.md). It does not own Resource URI semantics,
remote access, Agent Spec binding grammar, event delivery, or task lifecycle.
Those remain downstream profile concerns or existing root contracts.

## Architecture (PROFILE-R01..R09)

```text
Agent Spec resource URI (opaque, byte-preserved)
             |
             v exact RFC 3986 scheme
<catalog>/catalog.kdl
  profile "<scheme>" { wasm "<module>"; class "<class>"; }
             |
             v ResourceProfileRegistry (injectable; built-ins empty)
      compiled Module cache keyed by module path
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
}
```

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
class values, and duplicate profile schemes fail parsing. Relative module
paths anchor at the catalog root after the existing `$CATALOG`/environment
expansion; absolute paths remain absolute.

`st2 validate` reports malformed declarations. `st2 up` loads declared profiles
before it spawns tasks, so a malformed profile block fails loudly rather than
silently removing watch coverage.

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
empty path, joins the returned path to `agent_dir`, lexically normalizes
`.`/`..`, and rejects any result outside `agent_dir` or crossing an existing
symlink below it. Existence is not required at resolution time; the carrier may
be created later.

Successful resolution carries the normalized agent-directory root into resync.
Every later digest read opens that root and then each relative component with
no-follow semantics, using each directory descriptor for the next lookup, and
reads the already-open final descriptor. A root, ancestor, or final component
replaced by a symlink before or during traversal fails closed; admission-time
metadata alone is never treated as a durable proof.

## Runtime containment (PROFILE-R04..R07)

Each module is opened nonblocking and no-follow, accepted only as a regular
file, and read through a 16 MiB admission cap before validation or compilation.
It is then compiled once per module path and shared by registry clones.
Each resolution creates a fresh `Store` and `Instance`. One fuel allowance
covers the module start function and the first resolution call; a reused
instance receives one fresh allowance before each later call:

| Boundary | Contract |
| --- | --- |
| Module file | regular, no-follow, nonblocking open; 16 MiB maximum before Wasmtime compilation |
| Imports | none; import-requiring modules fail instantiation |
| Fuel | 5,000,000 fuel units for start + first call; same budget per later call |
| Linear memory | 64 MiB maximum |
| Memories | at most 1 |
| Tables | at most 4, with at most 10,000 elements each |
| Instance state | fresh per registry resolution |
| Compiled code | cached per module path |

Failure taxonomy:

| Failure | SDK result | Supervisor effect |
| --- | --- | --- |
| special/oversized module or module load/instantiation | `Instantiation` | binding unwatchable; reconcile warning; supervisor lives |
| missing `memory`, `alloc`, or `resolve` | `MissingExport` | same |
| unreachable/stack/memory trap | `Trap` | same |
| infinite start function or call | `FuelExhausted` | same |
| invalid pointer, UTF-8, JSON, empty/escaped path, symlink, or special-file read | `BadReturn` or unreadable carrier | same |
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
- **DQ-P3 Module replacement:** Should replacing wasm bytes at the same catalog
  path invalidate the compiled-module cache in a resident supervisor, and what
  file identity or digest defines that transition? Resolve with an activation-
  style replacement experiment and an explicit cache lifecycle contract.
