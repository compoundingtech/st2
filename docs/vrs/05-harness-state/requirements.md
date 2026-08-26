# Observed harness state requirements

## Context

This subsystem defines observed harness state: the driver-written record of
what a harness is seen doing, published into the catalog beside the agent's
declared presence. It refines the slot root [`R08`](../requirements.md) leaves
undefined — R08 ratifies *declared* activity status and root spec `DQ3` marks
activity, plan, and plan step "remain undefined" — by specifying the
*observed* axis only. Declared activity status, current plan, and current plan
step stay unspecified and are not claimed here. Where this file and the root
disagree, the root wins and this file is wrong.

Provider-specific idle/active/child/unknown classification belongs to harness
drivers per [#162](https://github.com/compoundingtech/st2/issues/162); st2
core owns only the generic envelope, its fencing, freshness, and exposure.
The record-shape and coverage decisions are recorded in
[`.decisions/0006`](../.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md).
Delivery gating is explicitly not this subsystem's concern: DING and the
native transports keep their own evidence
([`01-ding/requirements.md`](../01-ding/requirements.md)), and no delivery
path reads this record.

## Assumptions

- **OHS-A01 Drivers can observe their harness:** Every maintained harness
  offers a positive observation source — the Codex app-server control stream,
  Claude lifecycle hooks plus the wrapper's child poll, pi's injected
  extension, and — conditionally, until `DQ-H6`'s capture confirms its event
  semantics — OpenCode's server surface, which at this layer is the candidate
  an experiment must validate before the producer trusts it. Where a source
  exists the driver
  projects it; where none exists the driver writes nothing, and the absence of
  a record is itself honest ("never observed"), never a fabricated state.
- **OHS-A02 Advisory surface:** Consumers are humans, supervisors, Doctor, and
  a roster/TUI. The record authorizes nothing: no delivery, no lifecycle
  action, no reconciliation. A wrong-but-fresh record can mislead an operator;
  it cannot misdeliver a message.
- **OHS-A03 Trusted writers:** The record is unauthenticated catalog state
  under the trusted-fleet model (root `A02`). The writers are the owning
  session's driver processes — the wrapper that owns the presence lease, and
  the channel or hook subprocesses it shares its incarnation token with;
  nothing verifies that claim.
- **OHS-A04 PTY output is universal session evidence:** Every maintained
  launcher runs the agent task in the declared PTY session. The PTY daemon
  necessarily observes every output byte to maintain terminal state, regardless
  of which launcher or harness produced it. This is sufficient evidence for a
  coarse `active | idle` classification, but proves nothing about `blockedOn`,
  `ask`, or `inputBuffer`.

## Acceptable Tradeoffs

- **OHS-T01 Unmeasured transport cost:** Turn boundaries are far more frequent
  than the five-minute presence refresh, and no measurement establishes what a
  per-transition replicated write costs on a real catalog under a real
  transport (`DQ-H2`). v1 accepts this, bounded by writes only on transition
  plus the slow heartbeat.
- **OHS-T02 Blocked is vacuous under bypass:** `blockedOn: human` can only be
  produced where a harness asks a human anything. Under Claude
  `bypassPermissions` — what `examples/native/agent-claude.kdl` ships — the
  axis never fires. The field is still v1: a later-added axis decodes as
  `unknown` in every pinned reader, the opposite of a conservative default.
- **OHS-T03 No history:** v1 records only the current state. Measured burst
  coalescing (4 transitions per turn, 0.1–0.4 ms apart, #268) would erase any
  state shorter than the coalescing window, so a v1 history could not be
  truthful about dwell time. `transitions` and `sinceMs` keep a later history
  additive.
- **OHS-T04 Ungraceful-death windows:** SIGKILL cannot be caught, so an
  external forced kill can leave a live-state record whose writer is gone.
  Same-host readers narrow that window with the liveness cross-check
  (OHS-R07) where the session is PROVABLY dead — pidfile present, process
  gone — while `pty kill` removes the pidfile and leaves the probe
  indeterminate for the rest of the window; the next session's written
  ownership claim supersedes the orphan at relaunch, and cross-host readers
  wait out the staleness horizon.

## Requirements

### Must publish one observed envelope

- **OHS-R01 Observed envelope:** Each agent has at most one observed-state
  record, `<agent-dir>/harness-state`, schema `st2.harness-state.v1`, written
  only by the owning session's driver processes — the wrapper, its channel,
  or its hooks; one logical owner per record, sharing one incarnation token,
  and nothing outside the driver writes it. It carries the full v1
  tuple: `state ∈ idle | active | child | ended`, `blockedOn ∈ human | none`
  (with `ask ∈ none | permission | question | review` naming the kind of
  human ask machine-readably, so no consumer branches on `reason`; `review`
  is reserved — no maintained producer emits it),
  `inputBuffer ∈ empty | nonempty | unknown`, plus the observing harness, a
  diagnostic `reason` no consumer branches on, and fencing/freshness fields.
  `child` is reserved: part of the contract, decoded by v1 readers, no
  producer yet (`DQ-H3`). The record is additive-tolerant on read; unknown
  future enum words decode as indeterminate, never as any definite value.
- **OHS-R02 Derived-only unknown:** `unknown` is mandatory in the read
  vocabulary, derived, and never written. One constructor produces every
  indeterminate observation, each absence carries a distinct reason
  (malformed, stale, future-skew, session-dead), and no path derives `idle` —
  or anything else — from missing evidence. A missing record reads as "never
  observed", which is distinct from `unknown`.
- **OHS-R03 Transport-safe freshness:** Freshness lives in the record bytes:
  an embedded origin timestamp with its own staleness and future-skew
  constants, deliberately not aliases of the presence constants. No read path
  consults file mtime. Every write is byte-distinct, so a transport that
  carries content but not metadata always carries a refresh.

### Must never wake what it informs

- **OHS-R04 Scoped delivery-input watching:** A write to the observed-state
  record wakes no delivery pump, no reconciliation pass, and no watcher owned
  by its own writer. Delivery pumps watch their inbox and the presence record,
  not the agent directory wholesale. This is a prerequisite: the record sits
  in a tree the Codex pump watches unfiltered today.

### Must derive from positive evidence

- **OHS-R05 Driver-owned projection:** Classification is driver work. The
  Codex producer projects the existing control state with the corrected rows:
  `ActiveWithoutTurn`, `ConflictingTurn`, and `Review` report `active` —
  review's enter and exit are model-emitted items inside a running turn, so
  nothing there awaits a human (matching the projection's
  `Held { Review }` → `active` / `blockedOn: none` row) — while
  `WaitingOnApproval` and `WaitingOnUserInput` report `active` with
  `blockedOn: human`. `Compaction` reports `active`, and
  `NotLoaded`/`SystemError`/`AwaitingStatus` withhold rather than write.
  `Held` — a delivery predicate — never appears in the published vocabulary.
- **OHS-R06 Heartbeat only on evidence:** A writer re-stamps the record on the
  presence cadence only while it still observes its harness, and stops on
  evidence loss so the record ages to `unknown` instead of staying confidently
  wrong. On teardown the wrapper writes its terminal record — carrying the
  exit outcome — *before* any escalation that could take the wrapper itself,
  and a terminal record is never re-stamped.
- **OHS-R07 Liveness cross-check:** The record names the pty session whose
  liveness vouches for its live states. A same-host reader that positively
  proves that session dead — its pidfile present, its process gone — reads
  the record as `unknown` even while fresh; an indeterminate probe (an
  unreadable registry, or a pidfile `pty kill` already removed) downgrades
  nothing — unprovable evidence is never reported as death. The check is a
  narrowing, not a closure: what it cannot prove, the relaunch-time written
  claim supersedes and the staleness horizon bounds. A fresh `ended`
  survives the check: a terminal record is supposed to outlive its writer.
- **OHS-R08 All-harness coverage:** Every local running agent has session
  fidelity independent of its launcher or harness. Codex, Claude, pi,
  OpenCode, and OMP additionally ship fine driver producers for sessions the
  corresponding st2 driver owns. Fine coverage may vary by launch path;
  session coverage may not.

### Must be readable beside declared presence

- **OHS-R09 Roster join:** `st2 agents --json` carries `observedState` beside
  declared `status` in one payload — the wedged-agent comparison (declared
  `busy`, observed `idle`) must not require joining two commands. Observed
  state is a third independent axis: it never rewrites presence, desired
  lifecycle, or `lastActivity`. `observedState.fidelity ∈ driver | session`
  tells consumers which axes are proved: driver fidelity covers the full
  tuple; session fidelity covers `state` only and leaves `blockedOn`, `ask`,
  and `inputBuffer` `unknown`. The pinned roster wire assertions change
  deliberately, in the same change, with the new proof named.
- **OHS-R10 Doctor exposure:** Doctor surfaces composed observed state for
  agents it owns as advisory output — fidelity and an indeterminate reason are
  explicit; absence names a missing driver record *and* missing PTY activity
  stamp. None is an exit-code failure in v1.
- **OHS-R11 Launcher-agnostic session projection:** The PTY daemon stamps
  `lastOutputAtMs` while processing output and persists it to the canonical
  session metadata, debounced to at most one metadata write per second per busy
  session. st2 joins that metadata at read time using the canonical agent task
  mapping `pty_id = bus_id`: alive plus output inside the activity window
  derives session-fidelity `active`; alive plus older output derives
  session-fidelity `idle`; missing liveness or output evidence derives nothing.
  st2 never branches on, imports, or names the launcher.
- **OHS-R12 Fine-over-session precedence:** A definite fresh driver record
  wins over session activity. A missing or derived-`unknown` driver record
  falls back to session activity. The session projection never becomes a
  `harness-state` writer and therefore introduces no writer identity, fencing,
  heartbeat, or record-retention contract.
- **OHS-R13 Bounded fleet cost:** Output stamping is O(1) in the PTY daemon's
  existing parse path. The persist debounce bounds write amplification.
  Roster reads consume the small per-session metadata directly; they do not
  shell out to `pty stats`, follow event streams, or scan terminal buffers.

## Evidence

The measurements are #268's, taken 2026-08-16/17 on one host and carried with
their original caveats: 1298 presence files all legacy one-line records, 4
transitions per turn 0.1–0.4 ms apart, Claude hook timelines (blocked entry in
2 of 9 captures, exit in 1), silent Claude death under SIGTERM/SIGKILL, and
the Codex `activeFlags` schema present on all supported codex-cli versions
(#268's first comment). The shipped code evidence is in-repo: the Codex state
machine and its hold reasons, the unfiltered agent-dir watch beside the
presence refresh that writes into it, and `src/harness_state.rs`, which
implements the envelope this file ratifies.

The session-fidelity measurements were taken 2026-08-26 on a 627-seat
downstream catalog: the shipped envelope had zero producer records because the
launcher bypassed st2 drivers; `pty stats --json` cost 520 ms for 60 sessions
(~5.5 s projected to 627); `scrollbackUsed` is the bounded terminal-buffer
length and saturates; the PTY event log is sparse and title-change dominated;
reading 300 persisted session metadata files cost 19 ms in one process. These
rule out stats polling, scrollback deltas, and event-following in favor of one
daemon stamp plus a direct read-time join. See
[`05-harness-state/.experiments/2026-08-26-launcher-independent-session-activity.md`](./.experiments/2026-08-26-launcher-independent-session-activity.md).
