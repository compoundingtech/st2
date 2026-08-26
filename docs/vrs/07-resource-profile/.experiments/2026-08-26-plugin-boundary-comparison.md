# Resource Profile plugin-boundary comparison

## Question

Which Resource Profile boundary gives st2 one reliable, performant extension
mechanism while absorbing complexity in the foundation rather than repeating it
for every downstream consumer: declarative templates, out-of-process exec
plugins, or sandboxed wasm modules?

## Common workload

All three prototypes began at the same injectable exact-scheme registry seam and
implemented the same logical profile:

```text
dev.schickling.agent-goal://<host>/<identity>
    -> <agent-dir>/resources/goal.md
    -> notification class immediate
```

Measurements ran on the same Linux 6.18 host with an AMD Ryzen 9 7950X3D.
Release-mode wall time came from `std::time::Instant`; the OS page cache remained
warm. “Cold” therefore means no process-local resolver cache or wasm instance,
not a machine after reboot. Results compare boundary overhead, not remote I/O.

## Method

### Declarative template

The prototype hardened the likely shipping declaration:

```kdl
profile "dev.schickling.agent-goal" {
  carrier "{agent_dir}/resources/{file}"
  class "immediate"
}
```

It implemented four placeholders (`{host}`, `{identity}`, `{agent_dir}`, and
`{file}`), strict unknown/ambiguous placeholder checks, duplicate-scheme
rejection, component-level normalization, and an agent-directory depth floor.
The benchmark command was:

```text
nix develop -c cargo run --release -p agent-spec --example profile_bench
```

Each workload ran 1,000 operations for five rounds; the median round was
reported. Warm resolution reused one registry containing six schemes and
rotated through a small URI set. Cold resolution rebuilt the registry and
performed its first lookup for every operation. Separate loops measured a
hostile traversal rejection and an unregistered-scheme miss.

### Out-of-process exec

An `ExecResolver` invoked `<executable> <uri>` with `cwd=agent_dir`, read one
JSON line (`{"path": ..., "class": ...}`), and cached successful results by
URI with a configurable TTL (default five seconds). Bash and compiled Rust
plugins implemented the same protocol. The benchmark command was:

```text
nix develop -c cargo run --release -p agent-spec --example bench-exec \
  -- 1000 <goal-resolver.sh> <goal-resolver-rs>
```

Warm mode used one resolver with a one-hour TTL after warm-up, so all measured
calls were cache hits and spawned no process. Cold mode constructed a fresh
resolver for every call, causing exactly 1,000 process spawns per plugin.
Percentiles are over the 1,000 sorted per-call wall-time samples. Spawn and call
cost were not split: each plugin only prints one short JSON document, so cold
cost is predominantly fork/exec, interpreter or binary startup, pipe read, and
reap.

Failure-isolation tests used plugins that exited 1, slept for 60 seconds behind
a 150 ms resolver timeout, emitted invalid/truncated/empty/oversized output, or
aborted after a partial write. Failures were not cached; each test invoked a
healthy resolution afterward to prove recovery in the same host process.

### Wasm

A core-wasm `WasmResolver` compiled one module, instantiated a fresh store and
instance per cold resolution, and also exposed a warm loop over one live
instance. The guest exported `memory`, `alloc`, and `resolve`, imported nothing,
and returned packed pointer/length JSON. The command was:

```text
nix develop -c cargo run --release -p agent-spec --example wasm_bench
```

Each mode ran 1,000 resolutions; percentiles are per-call wall times. Module
compilation and instance construction were measured separately. Two consecutive
runs agreed within about 10 percent. The isolation suite contained 12 tests for
normal registry composition and hostile modules: explicit traps, stack/memory
faults, infinite loops exhausted by fuel, missing exports, garbage/invalid
pointer returns, agent-directory escape, and oversized allocations under the
memory limiter.

Dependency cost was measured by Cargo.lock package-name set difference, binary
byte size before/after the feature build, and `cargo build --release --timings`
with a wiped target. Per-unit timing totals provide serial-equivalent work; wall
clock on the 32-thread host would hide parallel compilation.

## Result

### Latency

| Boundary and mode | p50 | p95 | p99 | max | Per-call external spawn |
| --- | ---: | ---: | ---: | ---: | ---: |
| Declarative warm | **0.624 µs** | — | — | — | 0 |
| Declarative cold registry + resolve | **2.647 µs** | — | — | — | 0 |
| Exec Rust warm TTL hit | **0.1–0.4 µs** | same range | same range | — | 0 |
| Exec Rust cold | **760 µs** | 1,117 µs | 1,603 µs | 3,863 µs | 1 |
| Exec bash warm TTL hit | **0.1–0.4 µs** | same range | same range | — | 0 |
| Exec bash cold | **2.901 ms** | 4.024 ms | 4.941 ms | 9.788 ms | 1 |
| Wasm warm call | **0.29 µs** | 0.33 µs | 0.51 µs | 0.68 µs | 0 |
| Wasm cold (fresh instance + call) | **20.1 µs** | 29.0 µs | **33.8 µs** | 42.8 µs | 0 |
| Wasm instance only | **10.2 µs** | 14.4 µs | 16.8 µs | ~720 µs scheduler outlier; otherwise ~25 µs | 0 |

Compiling the wasm module once took **2.6 ms**. Fuel accounting is included in
wasm call measurements and added roughly 0.1–0.2 µs. Wasm cold p50 was about
38× faster than a compiled exec plugin and 145× faster than bash. Declarative
was roughly 8× faster than wasm cold, but both are far below a resync window.

Additional declarative figures were 0.524 µs for hostile traversal rejection
and 0.021 µs for an unregistered-scheme miss.

### Failure isolation

| Failure | Declarative | Exec | Wasm |
| --- | --- | --- | --- |
| Path escapes agent directory | declaration/runtime normalization rejects | host validates returned path | host normalization rejects guest result |
| Non-termination | not programmable | timeout kills and reaps child; supervisor recovers | fuel yields `FuelExhausted`; supervisor recovers |
| Crash/trap | not programmable | non-zero/signal/partial output is unresolvable | typed trap is contained in the store |
| Garbage or oversized result | strict template parser | 64 KiB stdout cap and strict JSON/class parser | checked memory range, UTF-8/JSON parse, 64 MiB memory cap |
| State after failure | no mutable runtime | next process/cache miss is fresh | next resolution has a fresh store/instance |

The exec process-isolation tests were green. The wasm isolation suite was
**12/12 green**, including explicit trap, fuel-bounded infinite loop, garbage
return, and oversized-allocation cases. Both programmable approaches preserved
the supervisor; wasm did so without granting an executable ambient host access.

### Complexity and audit surface

| Boundary | One-time st2 cost | Recurring consumer cost | Dependency/runtime cost |
| --- | --- | --- | --- |
| Declarative | Placeholder grammar, template validation, containment, registry/catalog integration | one declaration per profile | negligible |
| Exec | about **295 core LOC** for spawn, timeout, cap, parse, cache, and integration | **12–16 LOC/profile** for registry wiring plus a Bash/Rust plugin; executable packaging and security review | no new Rust runtime dependency; one process per cold call |
| Wasm | about 355 host/seam LOC in the prototype plus one ABI and sandbox policy | demo guest: 111 Rust LOC, producing a **1,469-byte** artifact | **+71 lock packages**; binary 8,672,072 -> 25,026,152 bytes (**+16,354,080 / +188%**); **+45.7 s** serial-equivalent compile work; `lld` required for guest linking |

The wasmtime cost is paid only with `wasm-resolver`; default dependency trees do
not contain wasmtime. No-WASI core modules avoid a component/WIT toolchain and
reduce the guest capability surface to three exports and zero imports.

## Conclusion

Raw speed favored templates, but a declarative tier would coexist forever with
the programmable boundary and save only about 18 µs against wasm on a cold
resolution. Exec supplied process isolation but was 38–145× slower cold,
exposed ambient host execution, and moved packaging/security complexity to each
consumer. Feature-gated wasm supplied one expressive mechanism, deterministic
compute and memory bounds, contained all hostile cases, and remained negligible
against resync timing.

The evidence therefore supports a **wasm-only, feature-gated foundation** behind
the chosen registry/SDK seam. The dependency and binary cost is real and is why
the feature gate is constitutional, not optional cleanup.

## VRS Impact

Supports [`PROFILE-R01..R08`](../requirements.md) and the sandbox/ABI sections
of [`spec.md`](../spec.md). It resolves Q8 and Q10 into
[decision 0009](../../.decisions/0009-resource-profiles-use-a-feature-gated-wasm-boundary.md).
Declarative templates and exec plugins are rejected alternatives, not deferred
implementation tiers.
