# Friend-ready candidate status — 2026-09-25

**Release hold.** Candidate `326904a` is deployed to both active hosts, but the
default 24-hour idle and 72-hour bidirectional delivery gates have not elapsed.
Do not describe the friend-ready trial as released until both reports pass and
their retained evidence is reviewed. The continuously running gate watcher
notifies the standing st3 operator of post-due transitions; it does not turn a
short diagnostic into release evidence.

## Candidate and recovery proof

- Each daemon restart was preceded by a normalized ACK from the independent COS
  seat. Rollback binaries are retained locally on the corresponding host.
- After the final restart, both doctors passed; both signed peers were up with
  zero pending, invalid, or unhealthy replication records. Exact linked message
  receipts passed in both directions (12 and 28 seconds). COS independently
  confirmed both seat incarnations remained reachable and a new message reached
  it natively through the restarted Silber daemon.
- The live Hetz trace before `326904a` saw about 16,328 preparations of the same
  mailbox version SQL in 10 seconds. After the change, that SQL was absent from
  the top 20 preparation counts in an equivalent 10-second trace. The previous
  harness-history fix reduced its hot query from roughly 198,000 to roughly 150
  SQLite steps per 10 seconds. These are path-specific traces, not availability
  or whole-system benchmarks.

## Product checks

- `cargo test -p st3`, `cargo fmt --all -- --check`, both soak-report fixture
  tests, `cargo test -p stui`, and the TUI PTY smoke passed.
- Live TUI bootstrap was 121 ms and full snapshot 437 ms against the local
  daemon. All six iOS logic tests and the TypeScript typecheck passed. Prior
  signed Debug simulator and paired-gateway proofs remain the device evidence;
  a physical iPhone installation is optional for this mission.
- A three-minute **diagnostic only** after the final rollout had no sample
  errors, unchanged daemon PIDs, and quiet median CPU of 13.3% on Hetz and
  17.4% on Silber. Both are inside the provisional absolute CPU limits, but this
  does not establish flat RSS over a day or justify passing the default gate.

## Pending release evidence

- The post-rollout two-host idle window begins with the first final Silber PID
  sample at 2026-09-25 02:31:31 UTC. The unmodified report cannot pass before
  2026-09-26 02:31:31 UTC. It must show one stable PID per host, no missing or
  bad samples, enough quiet intervals, CPU within the stated limits, and flat
  last-hour RSS relative to the first hour.
- The complete-failure-logging delivery monitor began at 2026-09-24 22:50:25
  UTC. Its unmodified 72-hour report cannot pass before 2026-09-27 22:50:25
  UTC. It requires at least 400 exact receipts per direction, no failures or
  excessive gaps, and an active, current monitor.
- Keep inspecting peer status, last successful exchange, and errors throughout
  the soak. The current status API does not expose exact live TCP dial counts;
  the retained-client regression proves connection reuse in a focused TCP test,
  not an invented production counter. The earlier
  [idle baseline](idle-stability-2026-09-23.md) is not a matched 24-hour window.

The exact gate definitions and override rules are in the
[friend-ready delivery gate](friend-ready-delivery-gate.md). Any failure is a
release hold requiring diagnosis and a new complete window after remediation;
do not delete evidence or lower thresholds to obtain a pass.
