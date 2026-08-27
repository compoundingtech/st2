# st3 stabilization report

This report records the Codex-only stabilization work from 2026-08-27.

## Migration audit

The generated migration tree contains 58 eval KDL files.

Opaque Codex cells: 16.

Opaque Claude cells: 26.

Native Codex cells: 0.

Native Claude cells: 0.

The migrator preserves typed source driver blocks. It does not infer a driver from an opaque command.

The source owners must rewrite the 42 affected cells before the final migration.

The three local practice cells use native Codex drivers. They do not use a provider command string.

## Deterministic verification

The st3 package passes 52 tests. The migration suite passes 11 tests.

The graph simulator covers native readiness, messages, judges, a verdict, and scope cleanup.

The restart tests cover every restart type, accepted message idempotency, and restart adoption.

## Practice eval receipts

The first license MIT attempt stopped before the held-out judges.

The native bridge staged the kickoff but did not publish its delivery claim.

The agents still completed, committed, and verified the requested change.

The bridge now claims delivery after a durable unread-inbox write. A regression test covers this state.

The allowance remained at 87% after the attempt.

The second license MIT attempt passed readiness, completion, and isolation.

Its migrated coordination script rejected canonical `agent/` actor prefixes.

The script now accepts those prefixes. It passes against the preserved attempt export.

The allowance remained at 87% after the second attempt.

The third license MIT attempt passed every mechanical judge.

The Codex judge posted a semantic pass but used 204,252 tokens against an 8,192-token cap.

Each practice judge now uses one bounded evidence command. The new caps match measured Codex accounting.

The allowance remained at 87% after the third attempt.

The fourth license MIT attempt passed every checkpoint and cleaned its scope.

The bounded Codex judge used 46,461 tokens against its 65,536-token cap.

The allowance remained at 87% after the passing attempt.

The first ghost-bug attempt passed every checkpoint and cleaned its scope.

The mutation-valid regression failed on the old source and passed on the fixed source.

The bounded Codex judge used 45,544 tokens against its 65,536-token cap.

The allowance remained at 87% after this proof.

The first signal-rename attempt stopped before any provider agent started.

An event waiter consumed the shared reconcile notification after materialization.

The API now uses separate event and reconcile notifications. A regression test covers the lost-wake case.

The second signal-rename attempt also stopped before provider startup.

The installed Codex CLI changed from 0.145.0 to 0.150.0 during the work.

The native wrapper correctly rejected the unverified protocol version.

A generated schema comparison found unchanged delivery requests and additive delivery responses.

The exact version gate now admits 0.150.0. The third and fourth attempts provide live protocol evidence.

The third signal-rename attempt passed team completion and all five mechanical judges.

The LLM judge failed because its prompt caused it to read message directories as files.

The eval now supplies one checked semantic evidence script.

The fourth signal-rename attempt passed every checkpoint and cleaned its scope.

The integrated result passed 26 package tests and every held-out judge.

The bounded Codex judge used 52,237 tokens against its 98,304-token cap.

The allowance remained at 86% after both model-backed signal attempts.

## Final verification

All five example and practice KDL files plan without blockers after their documents are posted.

Migration parity passes for 25 catalog files, 73 catalog documents, 58 eval files, and 40 eval documents.

The root library and the channel, driver, eval, native-only, and validation integration suites pass.

The scoped clippy check passes with warnings denied.

The formatting check, diff check, and receipt JSON checks pass.

The full workspace check passes with PTY `0.12.0+500eab2`.

This PTY version includes the metadata patch behavior required by the survival tests.

Nix is unavailable on this host, so `nix flake check` did not run.

## Future overlap work

The [st2 to st3 parity inventory](./st2-parity-inventory.md) records the remaining gaps and test coverage.
