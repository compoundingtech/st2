# All three signals via a three-PR gh stack

Status: draft

Recorded 2026-08-25 from the aligned observability interview (axe decision catalog Q1 + Q3).

## Context

st2 needs OpenTelemetry instrumentation, but scope and delivery were open:

- **Scope**: traces only (the classic first step), or all three OTel signals?
- **Delivery**: one large PR, or a stack?

Arguments for traces-first: smallest diff, fastest feedback, metrics/logs can wait. Arguments for
one PR: single review surface. Against both: the exporter/provider plumbing is the hard part and
is shared by every signal; once it lands, metrics and logs are incremental. Deferring them
invites "traces shipped, rest never happens" — and the CI proof obligation (otelite capture +
assertions) is signal-generic anyway.

## Decision

**Q1 — Scope**: all three signals (traces, metrics, logs) are the target. Not traces-only; not
traces-first-with-maybe-later.

**Q3 — Delivery**: a gh stack of three PRs:

1. SDK init + OTLP/HTTP-JSON exporter + trace spans (+ this VRS tree);
2. metrics;
3. log bridge.

Each PR lands green independently; 2 and 3 build on 1's shared plumbing
(provider/resource/exporter) only.

## Alternatives considered

- **Traces-first, defer the rest** — rejected: defers most of the value (rates/durations live in
  metrics; correlated diagnostics in logs) for no real risk reduction, since the risky part
  (feature set, sync export, flush-at-exit) is identical across signals.
- **Single PR** — rejected: couples an unreviewable diff (instrumentation across `run.rs`,
  `exec_backend.rs`, `hooks.rs`, plus unit changes and test harness) to the plumbing; a regression
  in any slice blocks all of it.
- **Spool-files instead of direct OTLP export** — writing telemetry records to local spool files
  for a separate shipper to forward — rejected: adds a moving part st2 must own (rotation,
  retention, crash-safety) to solve a problem the fleet pipeline already solves at
  `127.0.0.1:4318`; the ambient-endpoint no-op contract would need re-inventing.

## Consequences

- The done-condition (CI-proven signals, zero-overhead no-op, unit env propagation, complete VRS
  tree) is fully met only after PR3; PR1 alone meets it for traces.
- Reviewers see plumbing once, then small incremental slices.
- The stack order fixes the open-question deadlines: metric set must settle before PR2 opens, log
  bridge approach before PR3 ([open questions](../open-questions.md)).
