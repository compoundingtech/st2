# Otelite reconciliation hierarchy proof

2026-08-26. End-to-end evidence for the bounded semantic hierarchy in
[decision 0004](../.decisions/0004-semantic-reconcile-trace-hierarchy.md).

## Question

One real `st2 up --catalog <empty> --once` invocation can export a non-flat reconcile trace over
OTLP/HTTP JSON while retaining exactly one compatibility root, one metric set, and one correlated
completion log. Every semantic child should be a direct root child with the same trace id and a
non-empty bounded `span.label`.

## Method

Ran the repository integration test against the flake-provided real `otelite` receiver:

```console
nix develop -c cargo test --test otel_export -- --nocapture
```

The test launches the real `st2` binary through `otelite run --protocol http/json`, supplies an
empty catalog, decodes `traces.ndjson`, `metrics.ndjson`, and `logs.ndjson`, and asserts ids and
attributes from the raw OTLP export request rather than an in-memory tracing subscriber.

## Result

The command passed both integration tests (`2 passed; 0 failed; 0 ignored`). The exported trace
for the endpoint-enabled invocation contained exactly six spans:

```text
st2.reconcile_pass             span.label=catalog
├── st2.catalog.lock           span.label=shared
├── st2.catalog.discover       span.label=catalog
├── st2.catalog.materialize    span.label=catalog
├── st2.runtime.observe        span.label=all sessions
└── st2.reconcile.execute      span.label=apply plan
```

The decoded/raw assertions proved:

- exactly one `st2.reconcile_pass` root;
- every listed child occurred exactly once;
- every child's `parentSpanId` equaled the root `spanId`;
- every child's `traceId` equaled the root `traceId`;
- every root and child had a non-empty `span.label`;
- `st2.hooks.verify` was absent because an empty catalog has no hook consumer;
- the pre-existing one reconcile counter point, one reconcile duration sample, one completion log
  correlated to the root trace/span ids, and no `BatchLogProcessor.Emit.AfterShutdown` assertions
  remained green;
- the endpoint-unset invocation completed and left traces, metrics, and logs absent or empty.

## Conclusion

The aggregate semantic hierarchy is visible in real OTLP output, direct parentage is stable, and
conditional omissions represent operations that did not occur. The same test is the durable CI
reproduction required by O11Y-R03.

## VRS Impact

The hierarchy, labels, attributes/status rules, path applicability, rejected spans, and disabled
construction guard are normative in [the specification](../spec.md#reconciliation-trace-hierarchy).
No requirement text changes: the evidence strengthens O11Y-R01, O11Y-R02, and O11Y-R03 without
changing their scope.
