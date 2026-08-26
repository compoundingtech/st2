# Observability open questions

Kept minimal; each blocks exactly one delivery slice, not the tree.

- **PR2 metric set.** Resolved by interview (decision record Q5): the RED-minimal
  set — counters `reconcile_passes_total{result}`, `task_launches_total{driver}`,
  `task_reaps_total{driver}`, `hook_invocations_total{hook,event}`,
  `message_deliveries_total{result}`, `crash_loops_total`; histograms
  `reconcile_pass_duration_seconds`, `session_start_duration_seconds`. Labels only
  from bounded enums (result/driver/hook); ids stay in span attributes.
- **`st2 up <spec>` span coverage.** The catalog reconcile paths are instrumented
  (`st2.reconcile_pass` at the `up_loop_until` loop pass and in `up_once`), but the single-file
  spec path (`reconcile_pass_specs` / `reconcile_pass_specs_with_sessions`, `src/run.rs`) emits
  no spans yet. Same span shape applies; folded into PR2, which needs a pass over that call
  chain for metrics anyway.
- **Remaining R04 resource attributes.** Resolved by source read (dotfiles dev3
  `monitoring.nix` transform block): the platform edge stamps `service.namespace`,
  `sk.site`, `sk.role`, `deployment.environment.name` where absent, and the central
  contract forbids hand-stamping them producer-side. st2 keeps `service.name`,
  `service.version`, `host.name`; nothing left to wire.
- **PR3 log bridge approach.** Resolved by interview (decision record Q6): adopt the
  `tracing` facade (`tracing-opentelemetry` + `opentelemetry-appender-tracing`) and migrate
  emit sites to tracing macros, unifying spans and logs on one subscriber. Larger diff accepted
  for the long-term win; PR1's dual-path helper is not built.
- **Unit env mechanism.** Resolved at PR1 implementation time: `Environment=` lines,
  captured at install time and unit-tested (`src/service.rs`); matches the existing
  PATH/PTY_ROOT pattern and the expected handful of variables. Revisit
  `EnvironmentFile=` only if the variable count grows.
- **Sampling.** Default is always-on given st2's low event volume. If supervisor-loop span volume
  proves noisy in Grafana, revisit parent-based sampling ratios — not before there is data.
