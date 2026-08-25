# Blocking HTTP-JSON exporter, single-client feature set, explicit flush at exit

Status: draft

Recorded 2026-08-25 from the aligned observability interview, backed by prototype evidence
([../.experiments/2026-08-25-rust-to-otelite-capture.md](../.experiments/2026-08-25-rust-to-otelite-capture.md)).

## Context

The exporter design had to satisfy four constraints simultaneously:

- Fleet pipeline is **OTLP/HTTP JSON only** — no gRPC clients anywhere.
- st2's process model is **synchronous**: there is no tokio reactor in the supervisor or CLI.
- Telemetry must be a **zero-overhead no-op** when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset.
- Spans from short-lived CLI runs must actually arrive — batched exports die with the process if
  nobody flushes.

Two traps surfaced during prototyping:

1. **The feature-interaction trap.** `opentelemetry-otlp` 0.30 with *both* `reqwest-client` and
   `reqwest-blocking-client` enabled (the defaults include blocking) compiles cleanly but fails
   at runtime with `NoHttpClient`: all three client-selection cfg arms require the absence of the
   other client feature. The failure appears only when the first span exports.
2. **The async-batch trap.** The default async batch exporter requires a tokio runtime; under
   st2's sync process model it panicked at export time.

## Decision

st2 uses `opentelemetry-otlp` 0.30 with `default-features = false` and exactly
`features = ["http-json", "reqwest-blocking-client", "internal-logs"]`. Exactly one reqwest
client feature is ever enabled.

The blocking client backs the **batch exporter**, with explicit `force_flush` + `shutdown`
registered at process exit so short-lived CLI spans reach the collector.

Endpoint configuration stays ambient: the exporter resolves `OTEL_EXPORTER_OTLP_ENDPOINT` itself,
and init checks it once before installing any SDK object — unset means no provider, no threads,
no allocation (O11Y-R02).

## Consequences

- No tokio dependency is pulled into st2's runtime path for telemetry.
- The compile-clean/runtime-dead feature combination is structurally excluded by pinning the
  exact feature list in `Cargo.toml`; any future feature addition must re-check the cfg-arm
  interaction.
- Export happens on st2's own threads via the blocking client; a slow local Alloy forwarder can
  stall an exporting thread up to the client timeout. Accepted for now — the forwarder is
  loopback-local; revisit only if real stalls appear.
- `internal-logs` keeps exporter failures diagnosable instead of silently dropped.
