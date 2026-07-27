# st2 specification

This document specifies st2's current implementation contract. It builds on
[requirements.md](./requirements.md).

## Status

Active. This is a concise map to the implementation and its evidence, not a
replacement for the README, CLI help, KDL examples, or tests.

## Scope

st2 validates a declared agent fleet, materializes agent workspaces, launches
host-local work, supervises restartable tasks, and delivers messages. The
agent grammar and harness-facing contract remain canonical in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).

## Runtime model

```text
hand-authored KDL
       │
       ▼
validate ──► materialize ──► reconcile declared host
                                  │
                         ┌────────┴────────┐
                         ▼                 ▼
                    PTY / exec       DING sidecar
                         │                 │
                         └──── state + bus ┘
```

- **R01–R03:** Fleet validation separates structural errors from selected-host
  runtime facts. Materialization is inspectable and host reconciliation starts
  only declarations pinned to the local host.
- **R05:** PTY launch metadata persists the st2-managed effective environment
  and supported fields for manual restart. Reconciliation reconstructs PTY and
  exec tasks from their declarations and waits for an old PTY daemon to finish
  before reusing its session id.
- **R06:** Hook bundles are explicit, content-addressed, installed separately,
  and verified before materialization references them.

## Message lifecycle

```text
atomic inbox file → DING attempt → agent reads → archive receipt
       └──────── archive with same filename wins ────────┘
```

- **R04:** A matching archive filename makes an inbox copy handled; stale
  duplicates are removed without another DING. Fresh `dnd` suppresses delivery;
  `busy` does not. Failed delivery remains retryable. Sidecar restart emits a
  bounded recovery notice instead of replaying the inbox. Delivery may wake an
  agent while it is working, but an active or uncertain human composer must be
  left untouched.

## State and scope

- **R07:** Current work and durable decisions live in catalog-backed context
  files outside the model transcript. Session-start hooks restore that context
  and expose the current inbox to a replacement harness session.
- **R08:** The runtime schema and canonical KDL parser model `agent`
  declarations only. General-purpose identity kinds have no parser or runtime
  representation.

## Evidence map

| Requirement | Mechanism and documentation | Executable evidence |
| --- | --- | --- |
| R01 | [`src/validate.rs`](../../src/validate.rs), [`src/discovery.rs`](../../src/discovery.rs), and the canonical external agent spec | [`tests/validate.rs::a_hand_authored_native_catalog_validates_without_errors`](../../tests/validate.rs); [`tests/native_only.rs::clean_path_executes_the_maintained_native_authoring_guide`](../../tests/native_only.rs) |
| R02 | [`src/kdl_format.rs`](../../src/kdl_format.rs), [`src/compile_agent.rs`](../../src/compile_agent.rs), and [`examples/native/`](../../examples/native/) | [`tests/compile_agent.rs::canonical_hand_authored_examples_parse`](../../tests/compile_agent.rs); [`tests/compile_agent.rs::compile_agent_generates_codex_then_materializes_composed_agents_md`](../../tests/compile_agent.rs) |
| R03 | [`src/reconcile.rs`](../../src/reconcile.rs) and [`src/run.rs`](../../src/run.rs) | [`tests/reconcile.rs::other_host_specs_are_skipped`](../../tests/reconcile.rs); [`tests/run.rs::up_once_skips_other_host_specs`](../../tests/run.rs) |
| R04 | [`src/message.rs`](../../src/message.rs) and [`src/ding.rs`](../../src/ding.rs) | [`src/message.rs::tests::archive_receipt_suppresses_and_idempotently_cleans_a_restored_inbox_copy`](../../src/message.rs); [`src/ding.rs::tests::pending_delivery_ignores_busy_but_respects_fresh_dnd_archive_and_retry`](../../src/ding.rs); [`src/ding.rs::tests::startup_backlog_gets_one_generic_recovery_then_new_arrivals_poke`](../../src/ding.rs) |
| R05 | [`src/run.rs`](../../src/run.rs) and [`src/exec_backend.rs`](../../src/exec_backend.rs) | [`tests/nomad_survival.rs::manual_pty_restart_preserves_every_st2_managed_environment_and_config_value`](../../tests/nomad_survival.rs); [`tests/exec_backend.rs::exec_restart_reap_keeps_bounded_diagnostics_and_final_remove_cleans_them`](../../tests/exec_backend.rs) |
| R06 | [`src/hooks.rs`](../../src/hooks.rs) and [`src/materialize.rs`](../../src/materialize.rs) | [`tests/hooks.rs::hooks_cli_is_explicit_receipted_idempotent_and_verify_only`](../../tests/hooks.rs); [`tests/hooks.rs::codex_materialization_verifies_before_writing_and_renders_a_versioned_path`](../../tests/hooks.rs) |
| R07 | [`src/context.rs`](../../src/context.rs) and [`hooks/`](../../hooks/) | [`src/context.rs::tests::write_then_read_now_roundtrips`](../../src/context.rs); [`tests/codex_hooks.rs::session_start_emits_current_codex_context_envelope`](../../tests/codex_hooks.rs) |
| R08 | [`src/spec.rs`](../../src/spec.rs) and [`src/kdl_format.rs::parse_kdl`](../../src/kdl_format.rs) | [`tests/discovery.rs::multiple_agent_nodes_in_one_kdl_file_yield_multiple_specs`](../../tests/discovery.rs); [`tests/validate.rs::unknown_type_is_an_error`](../../tests/validate.rs) |

The owner updates this spec and evidence map whenever implementation changes.
Changing [vision.md](./vision.md) or [requirements.md](./requirements.md)
requires Nathan's explicit approval.

## Open design question

- **DQ1 Scheduled work:** The vision includes distributed scheduling and
  workflows, but the KDL shape, event inbox, deduplication boundary, and
  execution receipts are not yet specified. The current preview is rejected by
  [`tests/validate.rs::the_future_schedule_preview_is_explicitly_rejected`](../../tests/validate.rs).
  A successful executable eval and Nathan's approval should resolve this before
  adding scheduler requirements.
- **DQ2 Safe DING delivery:** The current screen/composer heuristic is not yet a
  reliable proof that pasting is safe and has occasionally interrupted human
  typing. Prefixing the displayed notice with two blank lines can improve
  readability but does not satisfy R04. Resolve this with a stronger evented
  signal or other measured classifier; a small on-device model is an optional
  experiment, not a required architecture.
