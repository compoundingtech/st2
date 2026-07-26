# Invariants

These are st2's load-bearing guarantees. Every entry names a green proof; changes to lifecycle,
materialization, messaging, DING, or presence must preserve them.

| Invariant | Guarantee | Proof |
|---|---|---|
| **Supervisor-decoupled lifecycle** | SIGTERM or SIGKILL of st2 never kills a task. A new runner adopts survivors; only explicit teardown kills. This covers PTY and exec tasks. | `tests/nomad_survival.rs` |
| **Transport-decoupled lifecycle** | Each task is isolated from a supervisor/transport process-group or cgroup cascade. | `tests/transport_isolation.rs`; `tests/transport_isolation_macos.rs` |
| **Clean exec teardown** | Killing an exec task reaps its whole process group. | `tests/exec_backend.rs::exec_kill_reaps_the_whole_process_group_not_just_the_leader` |
| **Exactly-once-safe native bus** | Messages use stable `<unix-ms>-<rand6>.md` files. An archive filename is a durable receipt that suppresses restored inbox replicas and makes repeated archive cleanup idempotent. | `src/message.rs::filename_grammar`; `src/message.rs::archive_receipt_suppresses_and_idempotently_cleans_a_restored_inbox_copy`; `tests/message.rs` |
| **Prompt-safe native DING** | DING and shepherd never send Return into a Codex modal, active turn, or human draft. The exact stable idle plan prompt may receive Escape only, followed by a fresh exact-notice check. Unsafe panes and `busy`/`dnd` defer without losing or duplicating work. | `src/ding.rs::stable_plan_modal_dismisses_with_escape_then_delivers_exact_notice`; `src/ding.rs::changed_plan_modal_defers_without_escape_or_return`; `src/ding.rs::post_paste_modal_or_composer_mismatch_defers_without_return_then_submits_when_safe`; `src/ding.rs::deferred_fifo_delivers_once_when_safe_and_dnd_is_untouched`; `src/shepherd.rs::shepherd_unsafe_pane_defers_without_attempt_or_backoff_then_delivers` |
| **Stable roster JSON** | `st2 agents --json [--enrich]` preserves field names, order, null handling, status, activity, and inbox counts. | `src/agents.rs::agents_json_has_stable_wire_shape`; `tests/status_agents.rs` |
| **Presence liveness** | DING refreshes a live agent's status; an unrefreshed dead agent ages to `unknown`. | `src/ding.rs::run_ding_refreshes_presence_while_alive`; `src/status.rs::stale_mtime_reads_as_unknown_regardless_of_contents` |
| **Crash loops surface** | A task parked by a fail-mode restart policy notifies its supervisor once over the bus. | `tests/run.rs::surface_crash_loop_notifies_the_supervisor_over_the_bus` |
| **Tracked workspaces fail closed** | Materialization simulates content operations before writing and refuses a real change to any Git-tracked target. Byte-identical tracked, untracked, and non-Git targets retain useful behavior. | `tests/materialize.rs::every_content_directive_refuses_to_change_a_tracked_target_before_any_write`; `::byte_identical_tracked_target_is_allowed_without_modification`; `::untracked_and_non_git_targets_remain_materializable` |
| **Native flat root** | Without an authored override, catalog tasks, eval messaging, shell helpers, and DING all use the catalog itself as `ST_ROOT`; no nested bus directory is synthesized. | `src/eval_run.rs::bus_root_expands_st_root_else_defaults`; `tests/eval_run_e2e.rs::st2_eval_runs_a_benign_folder_to_a_pass_verdict`; `tests/pty.rs` |
