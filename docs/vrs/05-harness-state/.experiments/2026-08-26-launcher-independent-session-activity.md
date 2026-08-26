# Launcher-independent PTY session activity

2026-08-26, Linux, a downstream catalog with 627 declared seats and 60 live PTY sessions during the measurement. The investigation followed a deployed observed-harness-state envelope whose reader was live in st2 and fractal while every seat still returned `observedState: null`.

## Question

What is the lowest-global-complexity, launcher-agnostic source of coarse harness activity for every managed session, and can it remain efficient in a busy 627-seat catalog?

## Method

The investigation traced the deployed producer/read paths in st2, identified the process refreshing presence, inspected the PTY daemon's output and persistence paths, measured the existing candidate surfaces, and compared their asymptotic and measured fleet costs.

Reproduction commands:

```sh
# Current-scale cost and live-session count
time pty stats --json > stats.json
jq 'length' stats.json

# Persisted registry shape and event distribution
jq 'keys' <pty-root>/<session>.json
jq -r '.type' <pty-root>/<session>.events.jsonl | sort | uniq -c | sort -rn

# Direct-read baseline (single process)
time jq -s 'length' <pty-root>/*.json >/dev/null

# Source contracts
rg -n 'scrollbackUsed|scrollbackCapacity|ptyProcess.onData' src/server.ts
```

## Result

**Coverage was conditional on the launch path, not the harness inventory.** The deployed catalog launched harnesses through an external wrapper. st2's rich producers run only inside st2's native session drivers (or their hook/channel siblings), while the harness-blind ding sidecar refreshed presence for every live seat. `st2 hooks verify`, presence, and catalog checks were green with zero `harness-state` records. A launcher may therefore adopt the roster reader without any producer; nothing measured that gap.

**The PTY daemon is the universal observer.** Its `onData` handler already receives every PTY output chunk before feeding xterm-headless and clients. Stamping a timestamp there is O(1) and adds no observer, stream, process, or harness/launcher coupling.

**Terminal-buffer deltas are not an activity clock.** `pty stats` reports `scrollbackUsed = buf.length` and capacity `rows + scrollback` (`src/server.ts`). The buffer is bounded; once full, its length stops advancing while output continues. Deriving activity from length deltas therefore fails systematically on the longest-running sessions.

**The existing event stream is sparse, not an output stream.** The three largest sampled event logs carried 840–961 records and were 99% `title_change`; only a few `user.agent.status`, lifecycle, bell, or cursor events appeared. Harnesses and launchers may emit useful semantic edges, but absence of an event is not evidence of idle output.

**Shelling to `pty stats` is too expensive at fleet scale.** One bulk `pty stats --json` snapshot took 520 ms for 60 sessions (~8.7 ms/session), projecting to ~5.5 s for 627 sessions. The cost includes process/resource probes that observed-state composition does not need.

**Direct metadata joins are cheap.** Reading 300 persisted session JSON files took 19 ms in one process (and 379 ms in the deliberately worst process-per-file form). A native Rust reader over the small files is well below the interactive roster budget. No subprocess is required.

**The implemented composed roster remains sub-second at full declared-fleet
scale.** A synthetic catalog with 627 local, live sessions (627 pid probes and
627 metadata reads; half stamped 500 ms ago, half 120 s ago) ran
`st2 agents --json` ten times after one warm-up: 361.89 ms minimum, 394.39 ms
median, 551.58 ms maximum, 412.64 ms mean. The result contained exactly
314 `active` and 313 `idle` session-fidelity observations. The benchmark used
the debug binary, so it is a conservative bound rather than a release-build
claim.

## Conclusion

The global minimum-complexity shape is:

```text
PTY output -> daemon lastOutputAtMs stamp (O(1)/chunk)
           -> locked session metadata persist (trailing debounce <= 1/s)
           -> st2 read-time join (alive + recent output => active; alive + older => idle)
           -> fresh definite driver observation takes precedence
```

The coarse session projection does not write `harness-state`, so it introduces no writer identity, fencing, heartbeat, history, or retention contract. It is launcher- and harness-agnostic. `fidelity = session | driver` tells consumers which axes are proved; session fidelity covers only `state` and `since`.

The implemented benchmark covered all 627 sessions simultaneously live, with
the liveness and metadata join active for every row. The one-second metadata
debounce is a write-amplification bound, not an activity threshold; st2 owns
the 60-second activity window and 30-second future-skew guard. These constants
require tuning only if captured turn streams show maintained harnesses going
silent for longer than the window while still actively producing a turn.

## VRS Impact

- Amend decision 0006: the driver-written record remains the fine layer, not the only coverage mechanism.
- Add OHS-A04 and OHS-R11–R13: PTY evidence, read-time projection, precedence, and fleet cost.
- Extend OHS-R09 with `observedState.fidelity = driver | session`.
- Update the spec's overview, exposure wire, verification plan, and ontology.
- Leave DQ-H5 (remote supervisor semantics) open; session fidelity is deliberately same-host because PTY metadata is host-local.
