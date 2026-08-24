# Observed harness state is a driver-written catalog record

Status: accepted

Design decision made by Johannes on 2026-08-23 (interview over three research
reports and issue #268, which carries the measurements cited below). Merge and
acceptance approval required: upstream maintainers.

## Context

st2 exposes what an agent *declares* — presence, on a five-minute heartbeat
with a fifteen-minute staleness horizon — and nothing about what its harness is
*observed* doing. The one observed state machine that exists,
`CodexObservedState`, persists host-local under `$XDG_STATE_HOME`, has zero
production readers, and is invisible to the roster, Doctor, and every remote
supervisor. Claude, pi, and OpenCode have no observed-state type at all. The
concrete consumer is the one #261 and #268 name: a downstream TUI rendering a
seat's working state, which today cannot distinguish "alive and doing nothing"
from "recently active" because `lastActivity` advances on the agent's own
heartbeat. #162 assigns provider-specific idle/active classification to
drivers and keeps the generic envelope in core; this decision fills that
envelope.

## Decision

Observed harness state is **one driver-written record in the catalog**,
`<agent-dir>/harness-state`, schema `st2.harness-state.v1`, carrying the full
tuple in v1: `state ∈ idle | active | child | ended` (`unknown` derived, never
written; `child` reserved with no producer), **`blockedOn` as a field**
(`human | none`), and `inputBuffer ∈ empty | nonempty | unknown`. The record
follows the presence record's transport discipline — embedded origin
timestamp, atomic tmp+rename, byte-distinct on every write — with its own
staleness constants, deliberately not aliases of `status::STATUS_*`. Freshness
is **transition writes plus a slow heartbeat** on the existing five-minute
presence cadence, and a writer that loses sight of its harness stops
heartbeating rather than refreshing a state it cannot see. **All four
harnesses ship producers**: Codex projects its existing control state, Claude
combines hooks with a wrapper-owned terminal write, pi exports the injected
extension's positive idle signal as an evented producer, and OpenCode gains a
full driver — session wrapper, presence lease, state producer, and a native
delivery transport at parity with the other drivers. Exposure is **joined into
`agents --json`** beside declared presence, plus a Doctor advisory. Teardown
gets both fixes: the wrapper writes its terminal record *before* escalating
SIGKILL into its own process group, and same-host readers cross-check the
record's pty session, where only provable death downgrades. Transition history
is deferred; `transitions` and `sinceMs` keep the record forward-compatible
with it. The work proceeds without waiting on #242 and flags the delivery
contract as that PR's territory.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Catalog record beside `status`, presence transport discipline | Selected | Remote supervisors and the TUI read through the catalog they already sync; the presence record's embedded-timestamp discipline is proven against transports that drop mtime. Accepted cost: a replicated write per state transition, unmeasured (OHS-T01). |
| Host-local record plus CLI exposure only | Rejected | Zero transport cost, but remote readers get nothing and the consumer TUI must run on the owning host — the exact invisibility that leaves `control-state.json` unread today. |
| Ride the stream/event ingress (#300) | Rejected | Events land in the inbox and DING the agent on every transition, emit is gated on a live owner binding and a healthy catalog, and stream direction is ingress into an agent, not egress about one. |
| `blockedOn` as a fifth state | Rejected | An agent can be blocked while a child command runs; the field composes, a fifth state forces an ordering. A later-added axis would decode as `unknown` in every pinned v1 reader, so the field ships in v1 while the window is open. |
| Sibling `st2 harness-state --json` command | Rejected for v1 | Zero pinned-wire edits, but the value of the surface is the comparison: a declared `busy` beside an observed `idle` is the wedged-agent signal, and a caller made to join two commands will skip it. The three pinned roster literals and the roster invariant wording change deliberately, in one change. |
| Codex-only v1, other harnesses follow | Rejected | The TUI pins v1 when it ships; a fleet's Claude/pi/OpenCode seats reading `null` indefinitely re-creates the two-tier observability this exists to close. |
| History (JSONL transitions) in v1 | Deferred | Measured burst coalescing — 4 transitions per turn, 0.1–0.4 ms apart (#268) — erases states shorter than the coalescing window, defeating the dwell-time analysis history is justified by. Deferred, not deleted: `transitions` + `sinceMs` make a later history additive. |

## Evidence and Argument

The signal already exists, typed and durable, for exactly one harness in the
wrong place: `CodexObservedState` is written on every transition to
`$XDG_STATE_HOME/st2/codex/<hash>/control-state.json`, and at current main it
has no production reader — the pump rebuilds from `AwaitingStatus` on restart.
So the Codex producer is a projection of shipped state, not a new observation
path. The two rows #268's adversarial review corrected are binding here:
`ActiveWithoutTurn` and `ConflictingTurn` project to `active`, because Codex
positively reported activity and st2 merely cannot name a steerable turn; and
`Held` never enters the published vocabulary, because it is defined as the
complement of steerable — a delivery predicate, which decision 0001 already
established must not govern an observability surface. pi is the strongest
evidence for the evented path: the injected extension's `ctx.isIdle()` is a
positive in-process signal (false exactly for `agent_start`..`agent_end`),
which is the "stronger evented signal" root DQ2 asks for, on the one harness
that already has a channel. On vocabulary provenance: the
`idle|active|child|unknown × inputBuffer` words #268 reuses are from PR #123,
which remains open — this decision *defines* those words for st2 rather than
reusing something landed. The self-wake hazard #268 warns about is live today:
the Codex delivery pump arms an unfiltered recursive watch on the whole agent
directory while its own five-minute presence refresh writes into that tree, so
scoping delivery-input watching is a prerequisite, not a precaution.

## Consequences

- The vocabulary (`idle`, `active`, `child`, `ended`, `unknown`; `blockedOn`;
  `inputBuffer`) is defined by this decision and
  [`05-harness-state`](../05-harness-state/requirements.md), not inherited
  from PR #123. If #123 lands with different words, that PR reconciles to
  these or supersedes this decision explicitly.
- Homographs to manage: *observed harness state* is neither **presence**
  (agent-authored), **session state** (task-record liveness), R08's declared
  **activity status**, nor R09's **working state** (restored context). The
  ontology pins all four apart.
- The roster wire shape changes: the three full-string pinned assertions and
  the stable-roster invariant wording are edited deliberately in the same
  change that adds `observedState`, with the new proof named.
- `blocked` is vacuous under `bypassPermissions`, which is what
  `examples/native/agent-claude.kdl` ships today; a fleet running entirely on
  it gains nothing from the axis until that changes (OHS-T02).
- The OpenCode native delivery transport is designed against #242's contract
  shape and does not wait for it; #242 reconciles at review.
- The `.decisions/` series carries two documents numbered 0005 from separate
  PRs. This decision takes 0006 and leaves the collision as recorded history;
  numbers are not reused.
- Root DQ2 and DQ3 are updated: pi gains an evented signal, the observed half
  of DQ3 is specified here, and the declared half (activity status, plan,
  plan step) plus supervisor-following behavior remain open.
