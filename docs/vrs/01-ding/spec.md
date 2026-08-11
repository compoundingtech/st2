# DING specification

This document specifies the mechanism shared by every maintained harness. It
builds on [requirements.md](./requirements.md). Per-harness screen grammars are
specified in [`01-claude/spec.md`](./01-claude/spec.md) and
[`02-codex/spec.md`](./02-codex/spec.md).

## Status

Active. A map to the implementation and its evidence, not a replacement for the
tests.

## Delivery selection

Delivery is opt-in. An agent declaration selects one transport:

| Declaration | Transport |
| --- | --- |
| `ding` | Legacy screen transport |
| `deliver "mcp"` | Claude native MCP transport |
| `deliver "app-server"` | Codex native app-server transport |
| Neither node | No delivery |

The selector table defines the transport contract, not current implementation
parity. Current st2 implements the Codex app-server adapter. It does not
implement the production Claude MCP adapter. The parser accepts `deliver
"mcp"`, leaves the authored Claude launch unchanged, and derives no legacy
`ding` sidecar. A Claude agent that selects it therefore receives no DING. Do
not deploy that selector until the production adapter exists. The Claude
channel specification and standalone probe are not that adapter.

`ding` and `deliver` are mutually exclusive. More than one `deliver` node is
invalid. Any other `deliver` value is invalid. st2 does not infer a transport
from the agent command because command arguments are opaque.

The native selector is a new `deliver` node. A binary released before this
contract ignores that unknown agent child. It lowers a valid `deliver`-only
agent with no delivery sidecar. The agent receives no DING. It does not silently
use the legacy screen transport. This is a visible delivery outage.

A binary that supports `deliver` validates its value and its mutual exclusion
with `ding`. Native delivery must not be encoded as an argument to `ding`,
because a pre-change parser would accept that form as legacy `ding` and use the
wrong transport. Doctor reports an active agent that declares no transport. The
report does not make the valid no-delivery opt-out an error.

The durable inbox is the source of truth for every transport. An archive with
the same message name wins. A native adapter completes delivery only after its
declared provider-specific success condition. If the adapter is closed,
unavailable, stale, or in an unknown state, it sends no unsafe input and leaves
the inbox message unread for retry. Native adapters do not use the
rendered-screen classifier.

## Efficiency accounting

Delivery efficiency is measured per delivered inbox message, not per provider
turn or session (`DING-R15`). The denominator is the exact number of messages
covered by a positive receipt: one FIFO head for legacy delivery, or the
complete messages in one accepted native view. A held or failed attempt, a
retry, overflow, and an oversized-head metadata fallback have a denominator of
zero. Correctness evidence reports those outcomes separately; it does not hide
them inside a cost average.

Each experiment reports these values separately (`DING-R16`, `DING-R17`):

| Metric | Current requirement status |
| --- | --- |
| Model inferences per delivered message | Unmeasured; no pass threshold |
| Model tool-boundary crossings per delivered message | Unmeasured; no pass threshold |
| Input tokens per delivered message | Unmeasured; no pass threshold |
| Output tokens per delivered message | Unmeasured; no pass threshold |
| Cache-read input tokens per delivered message | Unmeasured; no pass threshold |
| Cache-creation input tokens per delivered message | Unmeasured; no pass threshold |

A provider turn is not evidence of one inference or a fixed number of tool
crossings. Counts must come from the provider's authoritative event or usage
surface. If a provider omits a requested count, the result is `unknown`. If the
experiment cannot isolate the target messages or expose comparable counts for
both transports, that metric is incomparable rather than zero or improved
(`DING-R18`). Accepted evaluation evidence can fill the baseline and threshold;
the specification does not invent either value.

The rest of this document defines the unchanged legacy screen transport. The
native wire contracts are in each maintained harness specification.

## Composer states

One inspection of a rendered screen, evaluated against one exact expected
notice, yields exactly one state:

| State | Meaning |
| --- | --- |
| `EmptySafe` | A maintained harness is positively idle with an empty composer |
| `ExactSafe` | The exact notice is the complete composer, and the harness is idle |
| `ExactBlocked` | The exact notice is present, but a modal, active turn, or non-idle footer blocks Return |
| `Changed` | A maintained composer holds different text, including a human draft |
| `Ambiguous` | No maintained, unambiguous composer state was proven |

`Ambiguous` is the default for anything not positively understood, per
`DING-R03`. It is not an error state and carries no diagnostic obligation.

## Delivery

```text
record ownership ─► combined transport (paste ─► 0.5s ─► Return) ─► receipt
                         │ command failure or ambiguity                 │
                         └──────────────────────────────────────────────► Staged

receipt ─┬─ Accepted ──────────────────────────────────────────────► Delivered
         └─ RetainedSafe / RetainedBlocked / NotRetained / Unproven ► Staged

staged retry ─► receipt ─┬─ Accepted ──────────────────────────────► Delivered
                         ├─ RetainedSafe ─► final receipt ─┬─ Accepted ─► Delivered
                         │                                 ├─ RetainedSafe ─► Return ─► receipt
                         │                                 └─ other ────────► Staged
                         ├─ NotRetained + archived ────────────────► release head
                         ├─ NotRetained + unread ──────────────────► Staged
                         └─ RetainedBlocked / Unproven ─────────────► Staged
```

Fresh legacy delivery preserves the production transport: one bounded PTY
transaction contains a bracketed paste, a 0.5 second delay, and Return
(`DING-R01`). Ownership is recorded immediately before that transaction. The
production path does not inspect the composer first and does not use the
separate staging helper.

Every failure of that terminal command or of the following receipt observation
resolves to `Staged` (`DING-R07`): the paste and Return may already have reached
the harness, so ownership is retained and retry re-inspects instead of
re-pasting. A staged retry is inspect-only unless two adjacent `RetainedSafe`
observations authorize one bare Return (`DING-R02`).

Return is transport, not a delivery receipt. After any submission attempt, a
bounded observation loop asks the selected harness adapter for one of five
states:

| Receipt state | Meaning |
| --- | --- |
| `Accepted` | The expected notice text is visible in an adapter-recognized submitted-prompt or queued-message pattern while the lowest live composer is empty or an accepted idle placeholder |
| `RetainedSafe` | The exact notice remains the complete live composer and Return is currently safe |
| `RetainedBlocked` | The exact notice remains the complete live composer but the harness is active or blocked |
| `NotRetained` | A maintained adapter parsed the live composer and positively proved that the exact notice is neither its complete contents nor an accepted submission |
| `Unproven` | No positive acceptance or exact retained-composer state was proven |

Only `Accepted` becomes `Delivered` (`DING-R10`). PTY command success, generic
screen change, disappearance alone, a changed composer, unreadable output, and
observation timeout never become delivery. `NotRetained` requires successful
parsing by a maintained adapter; missing or unrecognized composer evidence stays
`Unproven`. A staged retry completes without input when it observes `Accepted`;
it may send one bare Return only after two adjacent `RetainedSafe` observations,
then must obtain the same positive receipt. `NotRetained` releases ownership
only when the notice is already archived; an unread notice remains staged.
`RetainedBlocked`, `NotRetained`, and `Unproven` send no input. No retry
re-pastes.

## Harness dispatch

Each maintained harness implements one interface: locate its composer on a
screen, reporting the row it occupies, and classify that screen against the
expected notice. Only the row crosses the interface — the located composer's
contents stay inside the harness that found them, so the registry never carries
a harness-specific payload and stays a uniform list. A registry enumerates the
maintained harnesses; adding one is a new module and a new registry entry, with
no edit to shared classification.

Dispatch is positional, not preferential. Every registered harness is asked to
locate a composer, and the one **lowest on the screen** is classified
(`DING-R04`). Scrollback is above the live composer by construction, so this
resolves harness-shaped transcript text — a captured screen from another harness
pasted into a pane — without a per-pair special case. When exactly one harness
locates a composer, that one is classified; when none do, the screen is
`Ambiguous`.

Post-submit receipt classification uses that same positional dispatch and a
shared `ReceiptState` type. Each adapter owns the renderer-specific proof that
the exact notice is retained or accepted; shared delivery code never matches
Codex or Claude pixels directly (`DING-R06`, `DING-R10`).

Positions are compared in one unit. Harnesses do not agree on what they match
against: some locate against the raw screen including escape sequences, others
against ANSI-stripped lines. A raw byte offset and a stripped line index are not
comparable, so each harness reports its position converted to a screen row, and
the comparison is over rows.

## Ownership of vocabulary

Shared code owns only what is common to every harness: the composer states, the
dispatch, ANSI stripping, soft-wrap candidate enumeration, and screen shapes
that are not specific to any harness.

Each harness owns its own composer markers, footer chrome, idle proof, and
active-turn and modal detection (`DING-R06`). Active-turn detection is
deliberately not shared: interrupt hints, progress indicators, and modal
prompts are rendering details of one harness, and applying one harness's strings
to another's pane makes the safety gate depend on a screen the other harness
never draws. Some near-identical strings are therefore duplicated across
harnesses, which is the intended cost.

Widening what counts as blocked is the fail-closed direction and is always safe.
Widening what counts as idle is not, and requires evidence from a real screen.

## Soft-wrap candidates

A composer may wrap a notice across rows. At each renderer-proven wrap boundary
the original text either lost one inter-word space or split a token, so each
boundary yields exactly two candidates. Comparison against the expected notice
succeeds if any candidate matches exactly. The bounded notice length keeps the
candidate set small; an unfamiliar multiline shape yields no match and fails
closed: it is `Ambiguous` before submission and `Unproven` after transport,
never positive `NotRetained` evidence.

## Retry and suppression

Deferred notices retain FIFO order and retry on a bounded backoff, so an
indefinitely occupied composer cannot spawn a terminal probe per inbox poll
(`DING-R08`). A staged archived head advances FIFO only after a maintained
adapter positively observes `NotRetained`; an unread head does not release.
Archive receipts remove pending notices that do not already own a transport.
Declared `busy` never suppresses delivery; only fresh `dnd` defers it
(`DING-R09`).

## Known limits

- Legacy idle proof depends on footer chrome that a harness may render
  differently across permission or approval modes. A harness whose footer is
  not recognized in a given mode defers indefinitely rather than delivering.
  This is an explicit limit per `T01`, and each harness spec states which modes
  it proves.
- The legacy classifier is a measured heuristic over rendered text, not an
  evented signal, so a renderer change can defer legacy delivery until the
  grammar is updated. Maintained native transports do not use this classifier.
