# Codex observed harness state and transition history

Date: 2026-08-17

## Question

Does the Codex app-server protocol carry a coarse observed harness state — specifically a "blocked on a
human" signal distinct from "actively working" — and can its transitions be replicated in the catalog, or
must they stay host-local?

## Method

Protocol vocabulary was generated from the installed binary rather than inferred: `codex app-server
generate-json-schema --out <dir>` on codex-cli 0.147.0 yields 246 typed definitions plus aggregate
`ServerNotification`, `ServerRequest`, and `ClientRequest` schemas. A disposable Python driver spoke the
real protocol over `codex app-server --listen stdio://` and wrote every frame to JSONL with a local receipt
timestamp: a trivial prompt under `approvalPolicy: never`, and a command-forcing prompt under
`approvalPolicy: untrusted` intended to provoke an approval round-trip. Both were replayed through the real
`CodexControlState::observe` in a temporary `#[cfg(test)]` harness, printing the projected coarse state
after every changing frame. The replay surfaced the two defects below; reading had not.

## Result

**The blocked-on-a-human signal is on the wire and st2 discards it.** `ThreadStatus`'s `active` arm carries
a required `activeFlags: ThreadActiveFlag[]`, where `ThreadActiveFlag = "waitingOnApproval" |
"waitingOnUserInput"`. `observe_thread_status` (`src/codex_app_server.rs:770`) reads `/params/status/type`
only.

**The limit of that evidence.** The field was confirmed on the live wire as
`{"status":{"type":"active","activeFlags":[]}}` — present, never populated. The account hit its ChatGPT
usage limit and both captured turns failed with `usageLimitExceeded`, so no model turn ran to block on. The
schema proves the vocabulary; no capture proves a value. `CodexControlState` (`:191-201`) has no field to
hold it either, so any mirror decodes an empty vector until one is added.

A second, independent blocked signal exists as ten server→client JSON-RPC **requests**
(`item/commandExecution/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput`,
`mcpServer/elicitation/request`, siblings). These carry a `method`, so they fall through `observe()`'s match
to `_ => return Ok(false)` (`:775`). The exit edge is the `serverRequest/resolved` notification, so
blocked-dwell-time is measurable. st2 attaches as an observer, not the answering client (`:1-10`), so
ignoring these loses information but hangs nothing.

The replay measured **4 coarse transitions per turn, plus one at bind**:

```
t=  2265.2ms (+ 2265.2) thread/started        -> Idle                        [bind]
t=  2334.3ms (+   69.1) thread/status/changed -> Held { ActiveWithoutTurn }
t=  2334.4ms (+    0.1) turn/started          -> Active { turn_id: … }
t=  5655.3ms (+ 3320.9) thread/status/changed -> Held { SystemError }
t=  5655.7ms (+    0.4) turn/completed        -> Held { ConflictingTurn }
```

**A failed turn terminates in `Held{ConflictingTurn}`.** `observe_turn_completed` (`:826`) has no arm for
`Held{SystemError}` or `Held{NotLoaded}`, so both fall to `_ => Held{ConflictingTurn}`. A usage-limit error
therefore makes st2 report two conflicting turns, and `ConflictingTurn` is preserved by both
`observe_turn_completed` and `observe_thread_status("active")`, so it clears only on an `idle` status. This
gates delivery, not merely display.

**Every turn start passes through a hold that never existed.** From `Idle`,
`observe_thread_status("active")` hits its `_` arm and yields `Held{ActiveWithoutTurn}` for the 0.1 ms until
`turn/started` arrives. Separately, `observe()` never matches `ExitedReviewModeThreadItem` (`:760`) and both
`observe_thread_status("active")` (`:775-785`) and `observe_turn_completed` (`:833`) preserve
`Held{Review}`, so review holds until the thread reports `idle`, blocking steer for the rest of a turn whose
review ended mid-flight.

All 20 of 20 notifications carry `emittedAtMs`, but **frames within a burst share one value** —
`thread/status/changed`, `error`, and `turn/completed` were all stamped 1786982519409 — so `emittedAtMs`
cannot order a history by itself. The installed and measured version, 0.147.0, is rejected by
`SUPPORTED_CODEX_CLI_VERSIONS` (`:38`), which admits only 0.145.0 and 0.146.0. Corrected after this
experiment ran: schemas generated from 0.145.0 and 0.146.0 binaries carry `ActiveThreadStatus` with
`required: ["activeFlags","type"]` and the same two-value `ThreadActiveFlag`, so no finding here about
`activeFlags` depends on admitting 0.147.0.

## Resolved decisions

- Coarse volume is not a catalog concern: four transitions per turn makes a long session hundreds of
  records. Delta notifications (`item/agentMessage/delta`, `item/reasoning/textDelta`,
  `item/commandExecution/outputDelta`, siblings) are excluded by construction, and the 16 unmatched item
  types are activity detail rather than lifecycle.
- Sub-millisecond bursts must be coalesced to the settled state, and the history needs a monotonic sequence
  number alongside `emittedAtMs`. An uncoalesced history records `Held{ActiveWithoutTurn}` on every turn
  start, a state that never meaningfully existed.
- **Blocked-on-a-human is a first-class axis, not a sub-state of `active`.** The model is stopped and a
  person restarts it, so a consumer that reads it as `active` answers "do not interrupt" for the one state
  where reaching the agent is the correct action. It is neither a new `CodexObservedState` variant nor a new
  `CodexHoldReason`; it is an orthogonal dimension carried beside the coarse state
  ([`2026-08-17-observed-activity-status.md`](2026-08-17-observed-activity-status.md)). Whether a blocked
  agent permits `turn/steer` is a product decision, not a protocol one, and it governs the documented
  invariant that `Active` is the only steerable state (`:162`).

## Conclusion

Yes on both counts, with the evidence for the first split unevenly. Blocked-on-a-human is carried twice over
— level-triggered `activeFlags` on a notification st2 already handles, and edge-triggered server requests
with a matching resolved event — and st2 discards both. The vocabulary is proved from the generated schema
and the live frame; a populated value is not, and needs a capture on an account with turn quota. Transitions
are low enough in volume for the replicated catalog, provided they are coalesced across sub-millisecond
bursts and ordered by a sequence number. The two defects the replay surfaced are worth more than the feature
that found them.

## VRS Impact

Adds a Codex observed-harness-state ontology distinct from agent-declared presence: a parity core of
`active`/`idle`/`held`, a first-class blocked-on-human axis, and a Codex arm carrying the pending
server-request method, the active flag, the turn id, the hold reason, and the terminal turn outcome, plus a
transition-history record keyed by `emittedAtMs` and a monotone sequence. It changes the observed-state
requirements around `turn/completed`, which must retain `TurnStatus` and error rather than collapsing every
outcome to idle; it touches the steerability invariant at `src/codex_app_server.rs:162` pending the
blocked-axis decision. It does **not** require admitting codex-cli 0.147.0: `activeFlags` was later
verified present and `required`, with an identical `ThreadActiveFlag` enum, on 0.145.0 and 0.146.0 — both
already admitted — so the field is available on a supported version today. It does not change the vision or the agent-declared presence model, which remains an independent
last-observed value.
