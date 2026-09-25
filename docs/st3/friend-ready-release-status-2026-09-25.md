# Friend-ready candidate status — 2026-09-25

**Release hold.** Candidate `b6837bb` is deployed to both active hosts, but the
default 24-hour idle and 72-hour bidirectional delivery gates have not elapsed.
Do not describe the friend-ready trial as released until both reports pass and
their retained evidence is reviewed. The continuously running gate watcher
notifies the standing st3 operator of post-due transitions; it does not turn a
short diagnostic into release evidence.
The [friend trial handoff](friend-trial-handoff.md) is staged for that review,
not an invitation to start the trial while this hold is active.

Monitoring commit `8817b15` adds an early post-rollout evidence preflight. It
notifies the operator of a bad sample, PID change, stale host, or excessive gap
before the 24-hour deadline, with retry after a failed notification send. The
preflight is currently healthy; it cannot pass the release gate.
Watcher update `cfe036f` also sends an early CPU-risk notice after two hours
and 60 quiet intervals per host if the unchanged CPU limits are exceeded. It
is installed on Hetz; its own fixture test and first service run passed. The
24-hour report remains the authority.

At 04:17:39 UTC the delivery monitor recorded a Silber-to-Hetz `stage=send`
failure. The exact request (`message/b67a6fcad1e2b29e`) and one linked reply
(`message/992b7fc53bef47b0`) were already durable, but the Fabric `exec`
caller timed out without a response after the send committed. The retained
evidence does not establish whether that exact remote CLI process exited
promptly. The failed monitor line remains in the retained log. Commits
`602e090` and `506cd51` recover an exact-token
request from the recipient's durable mailbox and then require its linked
reply, allowing the full receipt deadline for recovery. A deterministic test
discarded the Fabric response after a real remote commit and logged one
recovered request and one successful receipt. The repaired monitor restarted
at 04:25:22 UTC and logged a fresh exact receipt in both directions by
04:30:54 UTC. Its 72-hour marker was reset to that monitor start; the old
window cannot pass. A six-minute one-per-direction diagnostic passed, but it
is not release evidence.

## Candidate and recovery proof

- Each daemon restart was preceded by a normalized ACK from the independent COS
  seat. Rollback binaries are retained locally on the corresponding host.
- The prior `0da07d7` window reached a diagnostic Hetz quiet CPU median of
  19.7% over 26 quiet intervals, above the unchanged 15% limit. `b6837bb`
  keeps usage samples and lease renewals durable, replicated, and visible to
  clients without waking the full reconciler for those claims alone. It
  retains full reconciliation for harness readiness, mission transitions,
  non-quiet replicated claims, and projection recovery. The full st3/stui
  suites passed (408 st3 library, 86 CLI, and 17 stui tests; 2 live stui
  tests ignored). A new full-day measurement is required to establish whether
  this reduces whole-daemon idle CPU enough.
- `b6837bb` was installed on Hetz at 04:09:08 UTC (PID `482626`, binary SHA
  `567f1a2c48517ba86b326a2d6f629c330915172cca7579e20e9a1177e1dafcc8`)
  and Silber at 04:13:59 UTC (PID `76944`, binary SHA
  `d1527811ff7b4f08ed42f0e3f0a5c9fa1512328133bc0527be41862e3cf324ee`).
  Both doctors passed. COS independently confirmed each restart left both
  seats reachable, signed replication healthy, and native inbound delivery
  working (receipts `message/d6a01a230f02e039` and
  `message/dc64cdd57e21d3ae`). The prior binaries remain in each host's
  `rollout-backups/b6837bb-20260925-0415/st3-before`.
- The previous `0da07d7` rollout had both doctors pass; both signed peers were up with
  zero pending, invalid, or unhealthy replication records. Exact linked message
  receipts passed Silber-to-Hetz in 24 seconds and Hetz-to-Silber in 22 seconds.
  COS independently confirmed both seat incarnations remained reachable and a
  new message reached it natively through the restarted Silber daemon.
- The live Hetz trace before `326904a` saw about 16,328 preparations of the same
  mailbox version SQL in 10 seconds. After the change, that SQL was absent from
  the top 20 preparation counts in an equivalent 10-second trace. The previous
  harness-history fix reduced its hot query from roughly 198,000 to roughly 150
  SQLite steps per 10 seconds. These are path-specific traces, not availability
  or whole-system benchmarks.
- `8866c11` limits work-wake reconciliation to indexed sent-message candidates
  with work tags while retaining closed attempts. The focused regression and
  full st3 test suite passed. `0da07d7` additionally limits deadline wake-history
  enrichment to the next ready item for each local agent. The parity regression
  and full st3 test suite passed. The combined whole-daemon idle CPU is now
  being measured on the new `b6837bb` window.

## Product checks

- `cargo test -p st3`, `cargo fmt --all -- --check`, both soak-report fixture
  tests, `cargo test -p stui`, and the TUI PTY smoke passed.
- `nix build .#st3 --no-link` passed on packaging commit `3cdbe84`, including
  its release-profile tests and the `stui` suite; `checks.x86_64-linux.st3-help`
  passed for the packaged commands. The packaged `st3`, `st`, and `st3-migrate`
  binaries ran with this shell's non-Nix allocator preload removed from their
  environment; the inherited preload is not compatible with the Nix runtime.
  The package also contains the pinned `pty` executable. An isolated,
  local-only daemon started from the package and passed every doctor check,
  including PTY runtime discovery, before it was stopped. The packaged `stui`
  passed normal exit, SIGTERM restoration, navigation latency, and a
  stalled-connection PTY smoke against the live daemon. Its debug-only panic
  restoration check passed separately on the debug binary.
- The package was rebuilt and its help check passed again from the deployed
  `b6837bb` daemon source (tree at `2d861e6`); release-profile checks passed
  408 st3 library, 86 CLI, and 17 stui tests, with two live stui tests ignored.
- Live TUI bootstrap was 121 ms and full snapshot 437 ms against the local
  daemon. On the current source, all six iOS logic tests, the TypeScript
  typecheck, and `npm run export:ios` passed; the offline export produced a
  1.6 MiB iOS Hermes bundle. Prior signed Debug simulator and paired-gateway
  proofs remain the native device evidence; an exported bundle alone does not
  prove pairing or a physical iPhone installation, which is optional for this
  mission.
- The earlier `326904a` build had a three-minute **diagnostic only** with no
  sample errors, unchanged daemon PIDs, and quiet median CPU of 13.3% on Hetz
  and 17.4% on Silber. Those figures must not be attributed to `0da07d7` or
  used to pass the default gate.

## Pending release evidence

- The post-rollout two-host idle window restarted with the first final Silber
  PID sample at 2026-09-25 04:14:57 UTC. The unmodified report cannot pass
  before 2026-09-26 04:14:57 UTC. It must show one stable PID per host, no missing or
  bad samples, enough quiet intervals, CPU within the stated limits, and flat
  last-hour RSS relative to the first hour.
- The complete-failure-logging delivery window restarted at 2026-09-25
  04:25:22 UTC. Its unmodified 72-hour report cannot pass before 2026-09-28
  04:25:22 UTC. It requires at least 400 exact receipts per direction, no failures or
  excessive gaps, and an active, current monitor.
- Keep inspecting peer status, last successful exchange, and errors throughout
  the soak. The current status API does not expose exact live TCP dial counts;
  the retained-client regression proves connection reuse in a focused TCP test,
  not an invented production counter. The earlier
  [idle baseline](idle-stability-2026-09-23.md) is not a matched 24-hour window.
- Long-running Fabric `exec` clients were observed hanging after remote
  commands had stopped, while short commands and st3 signed replication kept
  working. The Fabric owner has requested a graph-queued diagnosis and
  regression; this operator-path issue remains open rather than being silently
  counted as a passed st3 messaging check.

The exact gate definitions and override rules are in the
[friend-ready delivery gate](friend-ready-delivery-gate.md). Any failure is a
release hold requiring diagnosis and a new complete window after remediation;
do not delete evidence or lower thresholds to obtain a pass.
