# Codex harness specification

This document defines native app-server delivery and the legacy Codex screen
grammar. It realizes [`../requirements.md`](../requirements.md) through the
selection and durability rules in [`../spec.md`](../spec.md).

## Status

Active.

## Native app-server transport

`deliver "app-server"` selects this transport. st2 delivers through the Codex
app-server control protocol. It does not inspect the rendered screen, write to
the composer, or start the legacy `ding` sidecar.

The native path uses typed user input only. It must never use
`thread/inject_items`. Raw injection does not start an idle turn, does not have
the typed user-message receipt below, and has different persistence behavior.

### Controlled launch and thread identity

st2 starts one app-server daemon for the declared agent on a host-local Unix
socket. It opens and initializes its control connection before it starts the
Codex TUI with `codex --remote unix://PATH`. The control client must be able to
observe `thread/started` before the TUI can create a new thread.

For a new session, st2 records the thread ID from `thread/started`. For a
resumed session, st2 loads the recorded thread ID and calls `thread/resume`
before it permits delivery. It does not infer ownership from `thread/list`, a
working directory, a process, or a PTY. Those surfaces do not identify which
TUI owns a thread.

The thread binding is persistent runtime state. It includes the exact agent
runtime incarnation that owns it. st2 rejects a binding from a prior
incarnation. A missing, conflicting, or stale binding makes the native
transport unavailable and leaves every message unread.

The control client remains subscribed to these events for the bound thread:

- `thread/status/changed`,
- `turn/started`,
- `turn/completed`, and
- `item/started` and `item/completed`.

`ThreadStatus` distinguishes idle and active states. It does not carry the
active turn ID. Only the turn lifecycle supplies that ID.

### Delivery state machine

The adapter applies this rule to the bound thread and a bounded body-bearing
FIFO inbox view. The view contains the largest complete prefix that fits 16
messages and 16 KiB. It never truncates a body. If the head does not fit, the
view identifies that message without its body, and all later messages remain
unread behind it.

The byte limit bounds inbox input handed to one model inference. The message
limit bounds the action set created by a burst of small messages. These values
are transport bounds, not efficiency thresholds. Their token and inference cost
remains unmeasured under `DING-R15` through `DING-R18`.

| Observed state | Request | Result |
| --- | --- | --- |
| Idle | `turn/start` with typed text | Start a turn and wake Codex |
| Active regular turn with exact current ID | `turn/steer` with typed text and `expectedTurnId` | Queue input on that turn |
| Review or manual compaction | None | Hold until a later idle state |
| No active turn ID, conflicting events, or stale ID | None | Reconcile state and hold |

Every `turn/start` and `turn/steer` request includes a stable
`clientUserMessageId` derived from the recipient, thread binding, and FIFO head
filename. The identifier controls duplicate transport for the delivered view;
it does not settle any included message. The adapter sends no turn-level
overrides with `turn/steer`.

`turn/steer` must use the exact ID from the latest unmatched `turn/started`
event. A `turn/completed` event clears that ID. A steering error for no active
turn, a stale `expectedTurnId`, review, or compaction is a hold result. The
adapter must not fall back to `turn/start` or `thread/inject_items` in the same
attempt.

The adapter marks review and compaction as non-steerable when
`enteredReviewMode` or `contextCompaction` item events appear. The app server
can reject a request before those events arrive. The same hold rule applies to
that race.

### Typed acceptance receipt

A JSON-RPC success response is not a delivery receipt. A returned turn ID is
not a delivery receipt. st2 completes the DING attempt only after this event:

```text
item/completed
  item.type = "userMessage"
  item.clientId = <the exact clientUserMessageId>
  threadId = <the bound thread ID>
```

`item/completed` is the authoritative item state. An `item/started` event may
show progress, but it does not complete delivery. An item for another client
ID, thread, or runtime incarnation does not complete delivery.

The adapter records submission state before it sends a request. If the control
connection closes after submission and before the typed receipt, the attempt is
ambiguous. On reconnect, st2 resumes the bound thread and reconciles its typed
user-message history before it sends that client ID again.

The app server does not promise duplicate rejection for
`clientUserMessageId`. st2 therefore owns duplicate control. It persists one
accepted receipt for the FIFO head that identifies the delivered view and its
runtime binding. Watcher events, poll events, reconnects, and supervisor
restarts consult that receipt before they send. Archive precedence removes
obsolete receipt state.

### Durable inbox and shutdown

The selected catalog inbox remains authoritative. Native delivery does not
archive or delete any included message. The agent handles and archives each
message through normal message commands.

Before each attempt, the adapter runs the normal message sweep. An archive
record for the identifying FIFO head wins. A held, rejected, disconnected,
unknown, or ambiguous attempt leaves every message unread and retryable.

Control transport close is a normal adapter stop. The adapter sends nothing
after close. A restarted adapter must restore the exact thread binding and
duplicate-control state before it attempts delivery.

### Remote TUI evidence

The native transport is not accepted until a live `codex --remote` test proves
all of these results against the same app server and control client:

- The normal TUI can start a new bound thread and resume that exact thread.
- An idle inbox message produces one typed `turn/start` user message and
  observable agent work.
- A message during a regular active turn produces one typed `turn/steer` user
  message without corrupting terminal input or the active turn.
- Review, compaction, stale-turn, and no-active-turn states hold the message.
- The inbox file remains until the agent reads and archives it.
- A deliberate protocol or receipt break makes the test fail.

The evidence must also record every user-visible difference between a local
TUI and the remote TUI. Known protocol limits are not silently treated as
parity.

Codex app-server and its remote transport are provider experimental surfaces.
The implementation pins its accepted protocol schema to a tested Codex
version. An incompatible schema or event change makes delivery unavailable and
leaves the inbox unread.

## Legacy screen transport

The remaining sections define the unchanged screen grammar selected by
`ding`.

## Locating the composer

This harness is located against the **raw** screen, including escape sequences,
rather than against ANSI-stripped text. Its composer is recognized by the exact
styled prompt sequences it emits: one set of markers for an empty composer, a
second set for a composer holding text. The styling is part of the evidence —
matching the prompt character alone would also match the same character sitting
in transcript output.

Several marker variants exist for each case because the emitted sequence differs
across renderer versions. The lowest occurrence on the screen is the live
composer.

Matching raw bytes yields a byte offset, while a harness matching stripped text
yields a line index. These are not comparable, so the located position is
converted to a screen row before dispatch compares it against other harnesses
(`DING-R04`).

Harness identification is the successful location of a composer, with the
product name and the model-identifier line as corroborating evidence.

## Proving idle

The footer is read from the composer downward. Idle requires exactly one
model-identifier status row in that suffix, and that row must be the final
non-empty row. Its separated fields must include either an absolute or
home-relative working directory, or a bounded `Context <percent>% left|used`
field. Missing, malformed, duplicated, or trailing unknown footer chrome is
ambiguous.

## Composer contents

Text recovered from the composer is normalized before comparison, because the
renderer may introduce styling and spacing that are not part of the logical
input. An empty composer proves emptiness directly from the empty-composer
markers rather than by inspecting recovered text.

Comparison against the expected notice uses the shared soft-wrap candidate
enumeration in [`../spec.md`](../spec.md).

## Post-submit receipt

The exact notice as the complete lowest live composer is `RetainedSafe` only
with the ordinary idle proof and no blocking state; otherwise it is
`RetainedBlocked`. `Accepted` requires both an empty lowest live composer and
the expected notice text in the adapter's submitted-prompt or queued-message
pattern. A parsed empty composer or different live draft that is not accepted
is `NotRetained`. Disappearance and unrecognized pixels are `Unproven`.

## Blocked states

Active-turn and modal detection is owned here rather than shared
(`DING-R06`). Shapes drawn by another harness are not evidence about this one:
this harness's panes are not made unsafe by another harness's interrupt hints,
and must not be made to look safe by their absence.

That ownership is currently structural rather than complete. The predicate lives
in this harness and can be changed without touching the other, but its contents
are still the undivided set inherited from the shared predicate that preceded
the split, including several shapes only the other harness renders. Splitting
ownership and narrowing the contents were deliberately separated so that the
move carried no behavior change; narrowing is outstanding, and is recorded as a
known limit below rather than described as done.

## Known limits

- Recognition depends on exact styled sequences, so a renderer change that
  alters the emitted styling defers delivery until the marker set is updated. An
  unrecognized composer is `Ambiguous`, never assumed idle.
- A transcript that contains a captured screen from another harness is resolved
  by positional dispatch rather than by this harness's own matching; see
  `DING-R04`.
- This harness's blocked-state predicate still carries shapes only the other
  harness renders, inherited from the shared predicate that preceded the split.
  The effect is over-blocking, not under-blocking, so it is consistent with
  `DING-T01`; the cost is that a pane can defer on evidence drawn from a screen
  this harness never emits. Narrowing it to this harness's own shapes is
  outstanding.
