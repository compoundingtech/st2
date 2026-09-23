# Conversations and messaging live E2E — 2026-09-23

This audit exercised the existing Hetz/Silber st3 network before and after the
conversation and delivery hardening on the `st3` branch. Tests used isolated
mailboxes and archived every probe message after inspection.

## Baseline

- Installed binary: `/home/myobie/.local/bin/st3` 0.1.0, SHA-256
  `db597cbf…`.
- Isolated parties:
  `agent/e2e-conversations-20260923t1217z-live-e2e/hetz` and
  `agent/e2e-conversations-20260923t1217z-live-e2e/silber`.
- Cross-host evidence: seven Silber-owned sessions were visible and readable
  from Hetz. Direct SSH access to Silber was unavailable, so this proves
  replicated visibility from the current node rather than a remote shell run.
- Independent standing-agent evidence: COS sent
  `message/76a79ac91850e1af` immediately after this agent compacted. This agent
  replied with `message/216b65705e6f4964`; the user then compacted COS and
  confirmed that the reply still arrived.

The baseline matrix passed message send (JSON, tags, reply references, and KDL
printing), explicit mailbox list/filter/count/archive, canonical and bare reads,
raw and multi-message reads, recipient replies, idempotent archive, thread
rendering, session pagination, timeline import across the seven Silber sessions,
and deterministic session export (1,163 files).

The live run exposed five compatibility defects:

1. A reply by the original sender addressed the sender instead of the other
   participant.
2. `conversations sessions --all --limit 200 --json` could not decode a `u128`
   usage timestamp.
3. First-page session and timeline reads could surface transient
   `PageCursorExpired` errors during write churn.
4. `read` and `read --archive` returned the lifecycle state from before their
   mutation.
5. Bare `conversations ls` ignored `ST_AGENT` and listed fleet-wide messages.

## Hardening

The CLI now routes replies to the other participant and rejects outsiders,
decodes usage timestamps portably, preserves unknown future timeline variants,
retries only cursor-free first pages, refetches after lifecycle mutations, and
requires an explicit mailbox via `--agent` or a non-empty `ST_AGENT`.

Delivery hardening also adds bounded Codex snapshot retries, ignores late
request IDs, and allows a known-idle harness to recover after three failed
snapshot attempts. PTY launches are serialized across processes, wait for the
exact published incarnation, and leave a durable pending fence when publication
cannot be confirmed. Reconciliation withdraws stale staged wakes with daemon
authority and provider-auth failures are no longer considered ready.

The two blockers from review of the superseded PR were addressed:

- failed Codex thread reads no longer permanently fence an otherwise idle
  delivery;
- OpenCode import uses a checked global sequence and bounded suffix, so more
  than 16 completed tool calls cannot duplicate timeline IDs.

Nix exposed and verified three additional portability/concurrency fixes:
shell-probe tests no longer depend on `/bin/sh`, takeover tests no longer depend
on `/bin/bash` or `/bin/sleep`, and the terminal capability key is published
atomically so simultaneous attachments cannot read a partially written key.

## Verification

- `nix build .#checks.x86_64-linux.st3 --no-link`
  - st-runtime: 19 passed
  - st3 library: 361 passed
  - st3 CLI: 71 passed
  - client-v0 CLI/contract: 20 passed
  - examples/evals: 27 passed
  - operational-state contract: 12 passed
  - st3-migrate: 17 passed
  - st3-schema: 15 passed
- `cargo test -p st2 codex_app_server::tests --lib`: 70 passed.
- `cargo test -p st3-client`: 17 passed across unit and integration suites.
- The terminal-key concurrency regression passed 20 consecutive focused runs.
- `cargo fmt --all -- --check`, `git diff --check`, and
  `cargo run -p st3-client-codegen -- --check` passed.

Source-binary probes against the pre-upgrade daemon also passed: a 200-session
page decoded with `has_more=true`; 20 consecutive fresh timeline pages survived
write churn; scoped bare list behavior was exact; and isolated send/read/reply/
archive checks returned the committed lifecycle states and routed both reply
directions correctly.

## Post-install network proof

Pending installation and live-network rerun.

## Known external constraint

The `pty-rust` Claude account reports an expired login. The new provider-auth
state prevents typed auth failures from appearing delivery-ready, but restoring
that provider requires an interactive account login and is separate from the
Codex conversation/delivery path tested here.
