# All three signals via a three-PR gh stack

Status: accepted

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

## Evidence and Argument

The exporter/provider plumbing and otelite capture harness are shared across all three signals,
while rates/durations require metrics and correlated diagnostics require logs. Splitting delivery
at signal boundaries isolates review and CI failures without treating traces as the finished scope.

## Decision

**Q1 — Scope**: all three signals (traces, metrics, logs) are the target. Not traces-only; not
traces-first-with-maybe-later.

**Q3 — Delivery**: a gh stack of three PRs:

1. SDK init + OTLP/HTTP-JSON exporter + trace spans (+ this VRS tree);
2. metrics;
3. log bridge.

Each PR lands green independently; 2 and 3 build on 1's shared plumbing
(provider/resource/exporter) only.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| All three signals in a three-PR stack | Selected | Shares the risky plumbing while isolating signal-specific review and CI failures. |
| Traces first, defer the rest | Rejected | Defers rates, durations, and correlated diagnostics without reducing exporter risk. |
| Single PR | Rejected | Couples all instrumentation sites and the capture harness into one blocking review surface. |
| Spool files instead of direct OTLP | Rejected | Adds rotation, retention, and crash-safety machinery for a pipeline the fleet already provides. |

## Consequences

- The done-condition (CI-proven signals, zero-overhead no-op, unit env propagation, complete VRS
  tree) is fully met only after PR3; PR1 alone meets it for traces.
- Reviewers see plumbing once, then small incremental slices.
- The stack order fixes the open-question deadlines: metric set must settle before PR2 opens, log
  bridge approach before PR3 ([open questions](../open-questions.md)).
