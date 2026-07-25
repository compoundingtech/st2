# Invariants

The load-bearing guarantees of st2. Each names the test that proves it — **break the test, you broke
the guarantee.** Don't touch the run loop, the bus wire format, teardown, or the presence model
without checking the named test still passes (`cargo test`).

This list is sacred but small: it grows only when a genuinely load-bearing invariant appears — a
handful, never a bureaucracy. Every entry must name a real, green test. Incident/why context lives in
the VRS gist (R21) and memory, not here.

| Invariant | Guarantee | Proof (green test) |
|---|---|---|
| **Supervisor-decoupled lifecycle** (VRS R21) | Stopping/crashing st2 (SIGTERM *and* SIGKILL) never kills a task; a fresh runner re-adopts the survivors, not cold-boots; only an explicit `retired` teardown kills. pty + exec. | `tests/nomad_survival.rs` |
| **Transport-decoupled lifecycle** (VRS R21b) | A task survives a **cgroup-cascade** kill of its supervisor/transport unit, not just the supervisor's *process* death — st2 spawns each task into its own systemd `--user` scope (own cgroup, a sibling of the transport unit), so a `systemctl restart <transport>` cannot take it as collateral. The permanent fix for the fleet-fragility incident. Linux (scope) + macOS (setsid/reparent; no cgroups to cascade). | `tests/transport_isolation.rs::{exec,pty}_task_survives_transport_cgroup_cascade`; `tests/transport_isolation_macos.rs::task_survives_spawner_group_kill` |
| **Clean exec teardown** | Killing an exec task reaps its whole process group — no orphaned grandchildren. | `tests/exec_backend.rs::exec_kill_reaps_the_whole_process_group_not_just_the_leader` |
| **Wire-compatible, exactly-once-safe bus** | A message is a `<unix-ms>-<rand6>.md` file whose grammar + frontmatter byte-match smalltalk; `st2 message` sends/replies/archives interoperably during migration. An archive filename is a durable receipt that suppresses a same-named inbox replica and makes archive cleanup idempotent. | `src/message.rs::filename_grammar`, `::render_then_parse_roundtrips`, `::archive_receipt_suppresses_and_idempotently_cleans_a_restored_inbox_copy`; `src/ding.rs::archived_receipt_suppresses_remove_reappear_and_restart_pokes`; `tests/message.rs::send_by_bus_id_lands_in_recipient_inbox` |
| **Prompt-safe native DING** | Native DING and scheduled shepherd prompts never send Return into a Codex modal, active turn, or human draft. They stage text without Enter, re-check the rendered composer, and submit only the exact staged text. Unsafe panes and `busy`/`dnd` defer without losing or duplicating inbox work; a shepherd pane deferral consumes neither its attempt backoff nor delivery latch. | `src/ding.rs::post_paste_modal_or_composer_mismatch_defers_without_return_then_submits_when_safe`, `::deferred_fifo_delivers_once_when_safe_and_dnd_is_untouched`; `src/shepherd.rs::shepherd_unsafe_pane_defers_without_attempt_or_backoff_then_delivers` |
| **`agents --json` parity** | `st2 agents --json [--enrich]` is byte-compatible with `st agents --json [--enrich]` (field names, order, null handling). | `src/agents.rs::agents_json_is_byte_compatible_with_smalltalk` |
| **Presence liveness** | The ding refreshes a live agent's status so a healthy-but-idle agent never rots to `unknown`; an unrefreshed (dead) agent correctly ages into `unknown` past the stale window. | `src/ding.rs::run_ding_refreshes_presence_while_alive`; `src/status.rs::stale_mtime_reads_as_unknown_regardless_of_contents` |
| **Crash-loop is never silent** | A task parked past its `restart{}` policy (mode=fail) surfaces to its `supervisor` over the bus (deduped, once per park) — not only an stderr line an operator has to be watching. | `tests/run.rs::surface_crash_loop_notifies_the_supervisor_over_the_bus` |
| **Render is behavior-neutral with convoy** (swap safety) | `st2 render` and convoy render the same agent to identical wiring — command, ding, `ST_AGENT`, `PERSONA.md`, `DING-BUS.md`, loader byte-identical (env/ding paths modulo `$CATALOG`). The M3 swap can't change how an agent boots. | `tests/render_neutrality.rs::st2_render_is_behavior_neutral_with_convoy` |
