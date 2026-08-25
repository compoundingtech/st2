# Observability open questions

Kept minimal; each blocks exactly one delivery slice, not the tree.

- **PR2 metric set.** Which metrics exactly, and their labels/cardinality budget? The shape
  (counters for passes/spawns/reaps/hooks/errors, histograms for reconcile duration and session
  start latency) is proposed in the [specification](spec.md#metrics-pr2); concrete instrument
  names and label sets need one pass over `src/run.rs` before PR2 opens.
- **`st2 up <spec>` span coverage.** The catalog reconcile paths are instrumented
  (`st2.reconcile_pass` at the `up_loop_until` loop pass and in `up_once`), but the single-file
  spec path (`reconcile_pass_specs` / `reconcile_pass_specs_with_sessions`, `src/run.rs`) emits
  no spans yet. Same span shape applies; needs one pass over that call chain before it can join
  the registry.
- **Remaining R04 resource attributes.** Only `service.name`, `service.version`, and `host.name`
  are set today; `service.namespace`, `service.instance.id`, `sk.site`, `sk.role`, and
  `deployment.environment.name` still need wiring (and a decision on which come from ambient
  `OTEL_RESOURCE_ATTRIBUTES` versus st2-side detection).
- **PR3 log bridge approach.** Bridge existing diagnostic output through an OTel logs emitter
  versus introducing a structured logging facade (`tracing` + opentelemetry layer) and migrating
  emit sites. The facade is cleaner long-term but touches more call sites; decide when PR1's
  trace plumbing is in tree and the real migration cost is visible.
- **Unit env mechanism.** `Environment=` lines vs `EnvironmentFile=` for `OTEL_*` propagation
  ([specification](spec.md#systemd-unit-propagation)). Lines are simple and match the existing
  PATH/PTY_ROOT pattern; a file scales to many variables and lets operators edit without
  reinstalling the unit. Decide at PR1 implementation time based on how many variables survive
  filtering.
- **Sampling.** Default is always-on given st2's low event volume. If supervisor-loop span volume
  proves noisy in Grafana, revisit parent-based sampling ratios — not before there is data.
