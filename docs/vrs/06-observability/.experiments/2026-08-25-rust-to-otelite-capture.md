# Rust sync exporter to otelite capture, proven end-to-end

2026-08-25. Prototype run by the parent agent before implementation begins. Question, method, and
result recorded here so PR1's crate choices rest on evidence, not hope.

## Question

Can a synchronous Rust program using `opentelemetry-otlp` 0.30 export spans over OTLP/HTTP JSON
into an `otelite` capture receiver — without a tokio runtime — and can the result be asserted
with `inspect`? Which crate feature set actually works?

## Method

A minimal sync Rust program:

- `opentelemetry` + `opentelemetry-sdk` + `opentelemetry-otlp`, HTTP-JSON wire;
- two spans emitted under distinct `service.name` resource values;
- ambient `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a local `otelite capture` instance.

Receiver: `otelite` (`@overeng/utils-dev/otelite` on effect-utils main, exposed as the
effect-utils flake package output `otelite`) — native axum/tonic OTLP receiver accepting
`/v1/{traces,metrics,logs}` in JSON and protobuf from any process. Capture mode runs
receiver-only until SIGINT/SIGTERM/stdin EOF; `inspect` summarizes and asserts on the capture.

## Result

- Both spans were received over HTTP JSON into the capture; `otelite inspect` showed them grouped
  by `service.name`. End-to-end path proven: Rust sync → otlp 0.30 → HTTP JSON → otelite.
- The ambient-endpoint contract holds: the program set no endpoint in code; exporter resolution
  picked up `OTEL_EXPORTER_OTLP_ENDPOINT`.

### Finding 1: the NoHttpClient feature trap

`opentelemetry-otlp` 0.30 with **both** `reqwest-client` and `reqwest-blocking-client` features
enabled compiles cleanly and fails at runtime with `NoHttpClient`. Cause: all three client-
selection cfg arms require the *absence* of the competing client feature, so no client is ever
constructed. Defaults include blocking alongside async, so the naive dependency line hits this.

Working feature set (the one PR1 pins):

```
default-features = false, features = ["http-json", "reqwest-blocking-client", "internal-logs"]
```

### Finding 2: async batch export needs a reactor

The default async batch exporter panicked under st2's sync process model — there is no tokio
reactor to drive it. The blocking client + batch exporter combination works. Consequence carried
into [decision 0003](../.decisions/0003-blocking-http-json-exporter.md): blocking client chosen,
with batch exporter plus explicit `force_flush`/`shutdown` at exit.

### Caveat: otelite capture stdin EOF kills it

`otelite capture` treats **stdin EOF as termination**. A test harness spawning capture with
stdin closed or `/dev/null` loses the receiver mid-test. The planned cargo integration tests must
hold the child's stdin open (held-open pipe) until the capture window ends. Recorded in the
[specification](../spec.md#testing-strategy).


## Conclusion

A synchronous Rust program exports spans end-to-end into an `otelite` capture receiver over
OTLP/HTTP JSON with no tokio runtime, and `otelite inspect` asserts on them grouped by
`service.name`. Two constraints are load-bearing: exactly one reqwest client feature may be
enabled (the both-features combination compiles but dies with `NoHttpClient` at export time),
and the batch exporter must be driven by the blocking client since there is no reactor. A test
harness must also hold the capture child's stdin open — EOF terminates it mid-test.

## VRS Impact

- Evidence behind [decision 0003](../.decisions/0003-blocking-http-json-exporter.md) and
  requirement O11Y-R07 (sync process model): blocking client + batch exporter + explicit flush
  at exit is the proven configuration, pinned verbatim in the [specification](../spec.md).
- Ambient-endpoint resolution confirmed for O11Y-R02 before any code was written.
- The stdin-EOF caveat is recorded in the specification's testing strategy and is baked into
  the integration-test harness design. Metrics/logs ingestion remains unproven until PR2/PR3;
  no requirement text changes.

## Limits

- Prototype exercised traces only; metrics and log ingestion through the same receiver are
  untested until PR2/PR3 (otelite accepts all three endpoints, but assertions must be proven).
- Single-run evidence; the CI integration tests are themselves the durable reproduction, which is
  why R03 makes them part of the done-condition.
