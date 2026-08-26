# Observability specification

This document owns the mechanism behind [requirements](requirements.md): the crate stack, exporter
configuration, span roots, unit propagation, test strategy, and delivery order. Naming,
provenance, and span-label rules are referenced from the dotfiles context `observability` tree
(`01-conventions`); the six producer obligations from its `09-integration` spec. st2-side
obligations land here; registry/dashboard/census obligations are deferred cross-repo
(O11Y-R08).

## Crate stack

```
opentelemetry      = "0.30"
opentelemetry_sdk  = "0.30"
opentelemetry-otlp = { version = "0.30", default-features = false,
                       features = ["http-json", "reqwest-blocking-client", "internal-logs"] }
```

The SDK needs no runtime feature: the batch exporter is driven by the blocking reqwest client on
st2's own threads, so no `rt-tokio` (or any async-runtime) feature is enabled.

The otlp feature set is load-bearing, not stylistic:

- **No gRPC client.** The fleet pipeline is OTLP/HTTP JSON only (`otel-stack.md`); no gRPC clients
  anywhere.
- **Blocking reqwest client only.** With both `reqwest-client` and `reqwest-blocking-client`
  enabled (the defaults include blocking alongside async), the crate compiles but every runtime
  client-selection cfg arm requires *not*-having the other feature, so export fails with
  `NoHttpClient` at span-export time. Exactly one of the two must be enabled.
- **Blocking chosen over async** because st2 has no tokio reactor; the async batch exporter
  panicked without one. See the prototype evidence
  ([.experiments/2026-08-25-rust-to-otelite-capture.md](.experiments/2026-08-25-rust-to-otelite-capture.md)).
- **`internal-logs`** keeps exporter-internal errors observable instead of swallowed.

## Exporter and provider setup

One module, `src/telemetry.rs`, owns init and teardown via `Telemetry::init(unit)` /
`Telemetry::shutdown()`:

- **Endpoint**: none configured in code. The exporter resolves `OTEL_EXPORTER_OTLP_ENDPOINT` and
  related `OTEL_*` variables from the environment automatically. Unset → no provider is installed
  at all (R02): the guard is checked once at init, before any SDK object exists, so the unset case
  allocates nothing.
- **Protocol**: HTTP JSON (`http-json` + protobuf-free wire), batch exporter, targeting the local
  Alloy forwarder at `127.0.0.1:4318` by convention.
- **Resource**: `service.name` = `st2-<unit>` selected per entrypoint (below; `src/main.rs`
  passes `supervisor`, `hook`, or `cli`), `service.version` from `crate::version::machine_version`, and
  `host.name` from the existing host detection. The remaining R04 fleet attributes
  (`service.namespace`,
  `service.instance.id`, `sk.site`, `sk.role`, `deployment.environment.name`) are not set yet —
  tracked as an [open question](open-questions.md).
- **Flush/shutdown**: `force_flush` + global `shutdown` registered to run at process exit. The
  batch exporter buffers; without explicit flush at exit, tail spans of short-lived CLI runs are
  lost. This pairing is required for delivery, not optional.

### service.name values (R05)

| Process unit | `service.name` |
| --- | --- |
| Supervisor loop (`st2 up` daemon / systemd unit) | `st2-supervisor` |
| One-shot CLI invocations | `st2-cli` |
| Hook executions (`st2 driver claude-observe`; other hook surfaces not instrumented yet) | `st2-hook` |

## Trace roots

Instrumented in PR1, one root span per unit of work:

- **Supervisor loop pass** — each iteration of the `up_loop_until` loop (`src/run.rs`,
  `up_loop_until`) wraps one reconcile pass in a span named `st2.reconcile_pass` with attribute
  `st2.host`; after the pass it records `st2.crash_loops` and `st2.unparked` counts.
- **One-shot up** — `up_once` (`src/run.rs`, `up_once`) wraps its single pass in the same
  `st2.reconcile_pass` shape with `st2.host`.

Not yet instrumented (follow-ups, not PR1 scope):

- Provider session lifecycles (claude / codex / opencode spawn, attach, teardown) beyond the
  PR2 launch/reap counters, exec sidecars (`src/exec_backend.rs`).

Span names follow the central `01-conventions` rules (`span.label` discipline included). Names are
registered st2-side; this list plus PR2's metric set is that registry's seed.

## Metrics (PR2)

Landed RED-minimal set per interview decision Q5; every label value comes from a bounded enum,
and identifiers never become metric labels (ids stay in span attributes). `src/metrics.rs` owns
the instruments; every record call early-outs unless a meter provider is installed.

| Instrument | Type | Labels |
| --- | --- | --- |
| `reconcile_passes_total` | counter | `result` = `pass` \| `fail` |
| `task_launches_total` | counter | `driver` = `codex` \| `claude` \| `opencode` \| `pi` \| `exec` \| `other` |
| `task_reaps_total` | counter | `driver` (same enum as launches) |
| `hook_invocations_total` | counter | `hook` = registry name (`claude-observe`), `event` = bounded Claude hook-event set, unknown → `other` |
| `message_deliveries_total` | counter | `result` = `pass` \| `fail` |
| `crash_loops_total` | counter | — |
| `reconcile_pass_duration_seconds` | histogram | — |
| `session_start_duration_seconds` | histogram | — |

The duration histograms use seconds-scale explicit buckets (`1ms … 10s`, see
`DURATION_BUCKET_BOUNDARIES` in `src/telemetry.rs`) instead of the SDK's millisecond-tuned
defaults, so sub-second passes and spawns stay distinguishable.

Scope notes: passes are counted at all three `st2.reconcile_pass` sites (catalog loop pass,
one-shot up, and the single-file spec path — `reconcile_pass_specs_with_sessions`, which now
emits the same root span shape); `fail` means the pass collected errors. Reaps count the
restart path in the launch loop, where driver context exists. Deliveries cover bus deliveries
onto a recipient inbox (`deliver_record`, send + retry paths); ding/native transport outcomes
are separate follow-ups. Hook invocations are observed at the single in-process application
point (`st2 driver claude-observe`); hook scripts the harnesses execute directly are not
visible to st2. The `driver` label is a closed enum resolved by precedence: `exec` task kind
first, then a typed driver declaration, then an observational argv/shell token heuristic
(alphanumeric tokens matched in launch order: `codex`, `claude`, `opencode`, `pi`; anything
else → `other`). Because the heuristic inspects arbitrary user work, a hand-authored seat may
be labeled by what its command line merely mentions — the label is diagnostic only and never
influences reconcile decisions.

The meter provider shares PR1's plumbing: `Telemetry::init` installs an `SdkMeterProvider`
with a `PeriodicReader` + OTLP/HTTP-JSON metric exporter behind the same
`OTEL_EXPORTER_OTLP_ENDPOINT` guard and resource; unset → no provider and the global meter is
a silent no-op (R02 zero-overhead). `Telemetry::shutdown` force-flushes metric points alongside
spans so short-lived CLI runs deliver them.

## Log bridge (PR3)

Approach open ([open-questions](open-questions.md)): candidate is bridging the existing
diagnostic output through an OpenTelemetry logs emitter so log records join the same resource and
trace context. Ad-hoc `println!`/`eprintln!` on correlated paths migrate onto it; pure UI output
does not.

## Systemd unit propagation

`src/service.rs` builds the supervisor unit and serializes the operator's `OTEL_*` environment
into `Environment=` lines alongside the existing `PATH`/`PTY_ROOT` serialization, so
`st2 up --install-unit` preserves R02 (ambient endpoint) under systemd. Unit tests in
`service.rs` extend the existing serialization assertions.

## Testing strategy

- **Integration tests** (`tests/otel_export.rs`, cargo integration tests): the receiver is a
  prebuilt `otelite` binary passed by path via `ST2_OTELITE_BIN` (the effect-utils flake package
  output; gate wiring supplies it, and a gate run hard-fails without it unless
  `ST2_ALLOW_OTEL_SKIP=1` explicitly allows a local skip). Each test spawns
  `otelite capture` on an ephemeral port (`--http-port 0`), points the binary under test at it
  via `OTEL_EXPORTER_OTLP_ENDPOINT`, drives one command, then stops the receiver by closing its
  stdin — EOF flushes the capture to disk — and asserts on the captured traces (span names,
  resource attributes). Precedent: dotfiles op-proxy tests use `captureEnvTrace`; dotfiles
  branchy checks consume `effect-utils.packages.<system>.otelite`.
  - Caveat baked into harness design: `otelite capture` treats stdin EOF as termination, so the
    harness closes stdin deliberately as the stop signal rather than leaking `/dev/null`.
- **No-op proof**: a test asserts that with `OTEL_EXPORTER_OTLP_ENDPOINT` unset, the command
  completes normally with no export activity — guarding R02.
- **Flake check wiring**: the check pulls effect-utils' `otelite` package output, mirroring the
  branchy-check pattern, so CI proves R03 end-to-end without network access to dev3.

## Delivery: gh stack of three PRs

1. **PR1 — traces.** SDK init, OTLP/HTTP-JSON exporter with the exact feature set above, resource
   attributes, trace roots, unit `OTEL_*` propagation, otelite-based integration tests and flake
   check wiring, plus this VRS tree.
2. **PR2 — metrics.** Metric set finalized per open questions; shares provider/exporter/resource
   plumbing from PR1; otelite assertions extended to metrics.
3. **PR3 — log bridge.** Approach finalized per open questions; migrates correlated diagnostics;
   otelite assertions extended to logs.

Each PR lands CI-green independently; PR2/PR3 depend on PR1's plumbing only.
