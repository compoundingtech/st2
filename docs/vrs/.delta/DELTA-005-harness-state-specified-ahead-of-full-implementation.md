# DELTA-005: observed harness state is specified ahead of its producers

## Current mismatch

[`05-harness-state`](../05-harness-state/spec.md) and decision
[`0006`](../.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md)
specify one observed-state record with producers on all four maintained
harnesses, a roster join, Doctor exposure, and scoped delivery-input watching.
The shipped implementation at the time this delta was filed is the envelope
alone: `src/harness_state.rs` (record, writer, derivation, liveness
cross-check hook) with its module tests. No producer writes the record, the
roster does not expose it, the Codex delivery pump still watches its whole
agent directory unfiltered, and OpenCode has no typed driver.

## Why the docs land first

The vocabulary and derivation rules are the contract the fractal TUI pins;
they were interview-settled on 2026-08-23 and are cheaper to review as one
document set than re-derived per producer PR. Producers are independent
vertical slices (Codex projection, Claude hooks + wrapper, pi extension,
OpenCode driver) and land incrementally against this spec.

## Direction

Update implementation to match the spec, in dependency order.

## Resolution Signal

Checked off as each lands with its named tests green; this delta is deleted by
the change that completes the last box.

- [ ] Scoped delivery-input watching replaces the Codex pump's unfiltered
      agent-dir watch, with the no-self-wake regression test.
- [ ] Codex producer: projection per the spec table, wired to the control
      pump's transitions, heartbeat, and terminal write.
- [ ] Claude producer: hook-side classification plus wrapper-owned heartbeat
      and terminal write (pre-escalation ordering fixed).
- [ ] pi producer: evented extension transitions over the channel; wrapper
      writes.
- [ ] OpenCode: typed driver + `opencode-session` wrapper (presence lease),
      then its producer, then the native delivery transport.
- [ ] Roster `observedState` join with the pinned assertions updated and the
      stable-roster invariant wording amended in the same change.
- [ ] Doctor advisory for owned agents.
