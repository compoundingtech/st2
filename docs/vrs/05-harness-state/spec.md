# Observed harness state specification

This document specifies the observed-harness-state record. It builds on
[requirements.md](./requirements.md). Declared presence remains in the root
spec's R08 section; terminal delivery remains in
[`01-ding/spec.md`](../01-ding/spec.md) and reads none of this.

## Status

Draft. The envelope below is implemented (`src/harness_state.rs`); producers
and exposure land incrementally under
[`DELTA-005`](../.delta/DELTA-005-harness-state-specified-ahead-of-full-implementation.md).
Open questions are tracked in [open-questions.md](./open-questions.md).

## Scope

This specification defines the record, its freshness and derivation rules, the
per-harness producers, and the roster/Doctor exposure. It does not define:
transition history or a `--watch` surface (deferred, OHS-T03); idle thresholds,
escalation, or notification policy (#173's, per root `R20`); the withdrawn
host-local hot tier; the cut PTY screen observer; or any `AGENT-SPEC.md`
change (separate authority, root `DQ3`).

## Overview

```text
   codex app-server        claude hooks +          pi extension        opencode
   control stream          wrapper child poll      (evented)           server surface
        |                        |                      |                  |
        v                        v                      v                  v
   [driver-owned projection: idle/active/child/ended × blockedOn (+ask) × inputBuffer]
        |
        v                                          declared axis (unchanged)
   <agent-dir>/harness-state                       <agent-dir>/status
   st2.harness-state.v1                            presence, agent-authored
   transition writes + 5-min heartbeat                     |
        |                                                  |
        +--------------------+-----------------------------+
                             v
              st2 agents --json  (status ∥ observedState)
              st2 doctor         (advisory)
              downstream TUI     (spinner, blocked-on-you)
```

## Record (OHS-R01, OHS-R03)

One JSON object, atomically written (tmp sibling + rename), newline-terminated:

```json
{
  "schema": "st2.harness-state.v1",
  "agent": "<identity>",
  "harness": "codex | claude | pi | opencode",
  "state": "idle | active | child | ended",
  "blockedOn": "none | human",
  "ask": "none | permission | question | review (reserved; no producer emits it)",
  "inputBuffer": "empty | nonempty | unknown",
  "reason": "<diagnostic, optional>",
  "exit": "<ended only: e.g. 'exit 0', 'signal 9', optional>",
  "ptySession": "<session name for the liveness cross-check, optional>",
  "sinceMs": 1787690000000,
  "writtenAtMs": 1787690300000,
  "transitions": 41
}
```

Field rules, matching `src/harness_state.rs`:

- `state` on disk is never `unknown` (OHS-R02) and never `child` today
  (`DQ-H3`). Readers decode both, plus unrecognized future words as `unknown`.
- `blockedOn` unrecognized words decode indeterminate, never `none` — a v2
  axis value must not read as "not blocked".
- `ask` names the kind of human ask, machine-readably, while `blockedOn` is
  `human` (`none` otherwise; unrecognized words decode indeterminate) — the
  axis consumers filter on, so nothing branches on diagnostic `reason`.
- `incarnation` is the writing session's token and `seq` its monotonic
  ownership sequence. A claim is a WRITTEN act under the record lock: the
  session starting up writes an exitless `ended (superseded)` takeover
  record carrying its token and a sequence one above the highest this seat
  has seen — the maximum of the on-disk record's sequence and the
  `.harness-state.seq` floor sidecar, a sibling file (written
  stage-and-rename under the same lock on every claim) that keeps claims
  monotonic even when the record itself is unreadable. Racing
  claimers therefore mint distinct sequences, and a predecessor's
  still-fresh live record is superseded at relaunch, where the
  pty-name-based probe cannot tell sessions apart; the seat reads
  `ended (superseded)` until the session's first real observation. Ownership
  — coalescing, heartbeat eligibility, terminal suppression (which applies
  only to exit-bearing terminal records, never the claim placeholder) — is
  token equality with the claim's direction: a straggler whose claim is
  below the on-disk sequence is refused in live and terminal paths alike,
  and a token-only writer never claims — it adopts its own session's
  records, starts virgin ones, and is refused against foreign tokens.
  Sibling writer processes of one session share the claimer's exported
  token and sequence; records predating either field decode with an empty
  token and sequence zero, which no session owns and any claim supersedes.
  The residual: with the record unreadable AND the floor sidecar missing or
  damaged, a claim restarts at sequence one and a lingering predecessor
  holding a higher sequence could fence it — accepted, because it takes
  both files independently damaged, and refusing the claim instead would
  wedge the seat permanently.
  Every landed write carries a strictly monotonic per-record stamp (never
  inherited from beyond the future-skew trust bound) so it stays
  byte-distinct even against a same-millisecond predecessor.
- `reason` is diagnostic only; no consumer branches on it.
- `sinceMs` is when the current state was entered and survives heartbeat
  re-stamps; `writtenAtMs` is the heartbeat. `transitions` is a monotonic
  counter continued across writer restarts; with `writtenAtMs` it keeps every
  write byte-distinct.
- Deserialization is additive-tolerant (no `deny_unknown_fields`): a reader
  may be older than its writer.

Constants, deliberately not aliases of the presence constants (OHS-R03):
`HARNESS_STATE_STALE` 15 min, `HARNESS_STATE_REFRESH` 5 min,
`HARNESS_STATE_FUTURE_SKEW` 60 s.

## Derivation (OHS-R02, OHS-R07)

What a reader reports, in evaluation order:

| Evidence | Reads as | Reason |
| --- | --- | --- |
| No record file | no observation (`null`) | never observed ≠ `unknown` |
| Unparseable / non-v1-shaped bytes | `unknown` | `malformed-record`; never falls back to mtime |
| `writtenAtMs` > now + 60 s | `unknown` | `future-skew` |
| `writtenAtMs` ≤ now − 15 min | `unknown` | `stale` |
| Literal `unknown` state (never written by this crate) | `unknown` | `literal-unknown` |
| Live state, same-host probe proves `ptySession` dead | `unknown` | `session-dead` |
| Live state, probe indeterminate | the recorded state | unprovable evidence downgrades nothing |
| `ended`, any probe result | `ended` | a terminal record outlives its writer |
| Otherwise | the recorded tuple | — |

Every `unknown` row routes through one constructor and blanks every axis;
there is no path from any absence to a definite state.

## Codex producer (OHS-R05)

The projection reads the state the control pump already maintains; it adds no
observation path. `Held` never enters the published vocabulary — it is the
complement of steerable, a delivery predicate (decision 0001's boundary).

| `CodexObservedState` | state | blockedOn | ask | reason |
| --- | --- | --- | --- | --- |
| `AwaitingStatus` | *withhold* | — | — | no evidence yet; the record ages |
| `Idle` | `idle` | `none` | `none` | |
| `Active { turnId }` | `active` | `none` | `none` | |
| `TerminalError { systemError }` | `ended` | `none` | `none` | `systemError` |
| `Held { ActiveWithoutTurn }` | `active` | `none` | `none` | `activeWithoutTurn` — Codex said active; st2 merely cannot name a steerable turn |
| `Held { ConflictingTurn }` | `active` | `none` | `none` | `conflictingTurn` — two turns believed live is maximally active |
| `Held { Review }` | `active` | `none` | `none` | `review` — review's enter and exit are model-emitted items inside a running turn; nothing awaits a human |
| `Held { Compaction }` | `active` | `none` | `none` | `compaction` |
| `Held { WaitingOnApproval }` | `active` | `human` | `permission` | `waitingOnApproval` |
| `Held { WaitingOnUserInput }` | `active` | `human` | `question` | `waitingOnUserInput` |
| `Held { NotLoaded }` | *withhold* | — | — | thread not loaded proves nothing about work |
| `Held { SystemError }` | *withhold* | — | — | see #264's catch-all defect |

`inputBuffer` is `unknown` from this producer: the control stream does not see
the composer. The projection test must be behavioral — a table that would pass
with every row mapped to `unknown` is not an oracle (#268 §B).

## Claude producer (OHS-R05, OHS-R06)

Two cooperating writers. The hook side classifies turn lifecycle: a submitted
prompt or tool activity writes `active`; `Stop` writes `idle`;
`PermissionRequest` writes `active` + `blockedOn: human` with its ask kind
classified from the payload's `tool_name` (`AskUserQuestion` → `question`,
anything else → `permission`) — (its meaning is
specifically "a human is about to be asked" — it fires only under permission
modes that ask). Events carrying `agent_id` are subagent-nested and never move
top-level state. The blocked *exit* edge is the next `PreToolUse`,
`PostToolUse`, or `Stop` — measured-correct, not merely conservative: the
2026-08-23 batched-permission capture (`DQ-H1`) shows tool execution
serializes around an open permission prompt, so no event can clear the
blocked state early. The residual limit is the eventless deny path pinned in
`DQ-H1`. The wrapper side owns liveness: it re-stamps the heartbeat on its
existing presence cadence while the child is alive — through a fresh writer
each time, so it never clobbers a state a hook process wrote in between —
and writes the terminal record from its `try_wait` reap and its SIGTERM
path, before any SIGKILL escalation into its own process group, which no
in-process write survives (OHS-T04). Hook registration has one canonical
shape (`hooks::claude_settings_registration`): the maintained example
declaration (`examples/native/agent-claude.kdl`) carries it by hand, and
`expand_claude` renders the same `.claude/settings.local.json` upsert for
driver-declared seats, so both surfaces register identical hooks and a test
fails if they drift. A live seat converges without disruption: render output
is not part of the launch fingerprint, the merged settings land on the next
materialization pass, and the hooks take effect at the next session start.

## pi producer (OHS-R05, OHS-R08)

The injected extension observes `agent_start`/`agent_end` — a positive,
evented, in-process signal — and reports transitions over the existing
channel; the wrapper writes the record and owns heartbeat and terminal writes.
This is the first producer that satisfies root `DQ2`'s "stronger evented
signal" clause for any harness. pi's composer offers nothing to scrape, so
`inputBuffer` stays `unknown`.

## OpenCode producer (OHS-R08)

OpenCode's interactive TUI is also a server, so the driver needs no screen
observation at all (measured on 1.18.19 —
`.experiments/2026-08-23-opencode-surface.md`). The `opencode-session` wrapper
allocates a loopback port and a per-seat password, launches the TUI bound to
them, owns the presence lease and the observed-state record, and projects the
`/event` SSE stream:

| Signal | state | blockedOn | reason |
|---|---|---|---|
| `session.status {type: busy}` | `active` | `none` | — |
| `session.status {type: retry}` | `active` | `none` | `retry` |
| `session.status {type: idle}` / `session.idle` | `idle` | `none` | — |
| `permission.asked` … `permission.replied` (same id) | `active` | `human` | `permission` |
| `question.asked` … `question.replied\|rejected` (same id) | `active` | `human` | `question` |
| `session.error {ProviderAuthError}` | `ended` | `none` | `providerAuth` |
| `session.error` (other arms) | `idle` | `none` | `error:<name>` |
| child exit / stop path | `ended` | `none` | exit status |

A dedicated seat aggregates across the server's sessions: any busy session is
activity, any open ask is a human block, and idle requires positive level
evidence (`/session/status` omits idle sessions, so an empty map over a live
server is the idle proof, re-read on every SSE (re)connect). A dropped stream
stops the heartbeat; `inputBuffer` stays `unknown` — the `/tui/*` surface is
write-only.

The native delivery transport mirrors the Codex FIFO discipline: an
`Attempted` receipt persisted before transport, `POST
/session/<id>/prompt_async` with a stable caller `messageID` derived from
(identity, session, filename), and acceptance only when the exact message
reads back durably from the server. The `/tui/append-prompt` and
`/tui/submit-prompt` endpoints acknowledge input even with no TUI attached
(measured) and are therefore never a receipt. Delivery is fail-closed behind
two gates: a `SUPPORTED_OPENCODE_VERSIONS` pin and a live `/doc` OpenAPI
subset check naming every arm st2 consumes; observation runs behind the
`/doc` check alone, since its vocabulary already degrades to indeterminate.
Deliveries target the most recently observed session; a seat whose TUI has not
yet created one waits rather than creating sessions itself.

## Exposure (OHS-R09, OHS-R10)

`st2 agents --json` (both forms) appends one field per row:

```json
"observedState": {
  "state": "active",
  "blockedOn": "human",
  "inputBuffer": "unknown",
  "ask": "permission",
  "harness": "codex",
  "since": 1787690000000,
  "reason": "waitingOnApproval",
  "exit": null
}
```

`null` when no record exists. The derivation above is already applied — a
consumer never re-implements staleness. Roster reads pass the same-host
liveness probe only for agents whose resolved host is this host. `status`,
`desiredState`, and `lastActivity` keep their exact meanings; the three
full-string pinned assertions and the stable-roster invariant wording are
updated deliberately in the change that adds the field. Doctor prints an
advisory (not a failure) for an owned agent whose record is stale,
session-dead, or `ended` while desired state is `running`.

## Verification plan

The invariant rows this subsystem must add or amend when implementation lands,
each only once a real test proves it (per `CLAUDE.md`):

- **Scoped delivery-input watching** — a write to `harness-state` does not
  wake the Codex delivery pump; an inbox write still does. Test in the shape
  of `src/ding/mod.rs::idle_ding_does_not_spin_on_its_own_inbox_reads`.
- **Stable roster JSON** (existing row) — wording gains the third axis;
  `src/agents.rs::agents_json_has_stable_wire_shape` is edited in the same
  change and stays the proof.
- **Observed harness state discipline** — derived-only `unknown` with distinct
  reasons, byte-distinct writes, no-mtime staleness, terminal-write-before-
  escalation. Proving tests live in `src/harness_state.rs` today (11 tests)
  plus the planned per-producer suites; the SIGKILL-mid-turn test (`ended`,
  not `active`) gates the teardown row.

## Open design questions

Tracked with context in [open-questions.md](./open-questions.md): DQ-H1 Claude
blocked-exit edge, DQ-H2 transport cost, DQ-H3 `child` producer, DQ-H4
ungraceful-death coverage, DQ-H5 supervisor-following, DQ-H6 OpenCode state
source.
