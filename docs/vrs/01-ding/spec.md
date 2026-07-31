# DING specification

This document specifies the mechanism shared by every maintained harness. It
builds on [requirements.md](./requirements.md). Per-harness screen grammars are
specified in [`01-claude/spec.md`](./01-claude/spec.md) and
[`02-codex/spec.md`](./02-codex/spec.md).

## Status

Active. A map to the implementation and its evidence, not a replacement for the
tests.

Bare `ding` is the active compatibility path described first below. The
adapter-selected path later in this document is experimental and depends on
PTY's generic activity and guarded-send protocols.

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
         └─ RetainedSafe / RetainedBlocked / Unproven ────────────► Staged

staged retry ─► receipt ─┬─ Accepted ──────────────────────────────► Delivered
                         ├─ RetainedSafe ─► final receipt ─┬─ Accepted ─► Delivered
                         │                                 ├─ RetainedSafe ─► Return ─► receipt
                         │                                 └─ other ────────► Staged
                         └─ RetainedBlocked / Unproven ─────────────► Staged
```

Fresh delivery preserves the production transport: one bounded PTY transaction
contains a bracketed paste, a 0.5 second delay, and Return (`DING-R01`).
Ownership is recorded immediately before that transaction. The production path
does not inspect the composer first and does not use the separate staging
helper.

Every failure of that terminal command or of the following receipt observation
resolves to `Staged` (`DING-R07`): the paste and Return may already have reached
the harness, so ownership is retained and retry re-inspects instead of
re-pasting. A staged retry is inspect-only unless two adjacent `RetainedSafe`
observations authorize one bare Return (`DING-R02`).

Return is transport, not a delivery receipt. After any submission attempt, a
bounded observation loop asks the selected harness adapter for one of four
states:

| Receipt state | Meaning |
| --- | --- |
| `Accepted` | The expected notice text is visible in an adapter-recognized submitted-prompt or queued-message pattern while the lowest live composer is empty or an accepted idle placeholder |
| `RetainedSafe` | The exact notice remains the complete live composer and Return is currently safe |
| `RetainedBlocked` | The exact notice remains the complete live composer but the harness is active or blocked |
| `Unproven` | No positive acceptance or exact retained-composer state was proven |

Only `Accepted` becomes `Delivered` (`DING-R10`). PTY command success, generic
screen change, disappearance alone, a changed composer, unreadable output, and
observation timeout retain `Staged` ownership. A staged retry completes without
input when it observes `Accepted`; it may send one bare Return only after two
adjacent `RetainedSafe` observations, then must obtain the same positive
receipt. `RetainedBlocked` and `Unproven` send no input. No retry re-pastes.

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
closed.

## Retry and suppression

Deferred notices retain FIFO order and retry on a bounded backoff, so an
indefinitely occupied composer cannot spawn a terminal probe per inbox poll
(`DING-R08`). Archive receipts remove pending notices without another attempt.
Declared `busy` never suppresses delivery; only fresh `dnd` defers it
(`DING-R09`).

## Experimental adapter-selected delivery

The sole rich policy is selected by adapter presence; no provider or delivery
enum is built into core:

```kdl
ding {
  adapter {
    argv "$ADAPTER_ROOT/bin/activity" "--format" "jsonl"
  }
}
```

The argv values follow normal task launch semantics. The first value is the
executable and remaining values are exact arguments. st2 first resolves the
generated sidecar's complete managed environment, including `CATALOG`,
`ST_ROOT`, and declared task/agent `env`, and then expands each argv token
against that final map without changing argument boundaries. It neither runs a
shell nor adds adapter arguments or adapter-specific environment. Bare `ding`
continues to lower to
`st2 ding --identity <id> --root $ST_ROOT` exactly.

The long-running adapter owns provider-native interpretation and publishes
PTY's harness-neutral activity lease. Its bounded stdout is newline-delimited
JSON. Core accepts only this strict v1 event:

```json
{"v":1,"kind":"activity","session":"host.identity","incarnation":"opaque-epoch","generation":"opaque-pty-generation","sequence":7,"state":"idle","inputBuffer":"empty","validForMs":250,"reason":"opaque"}
```

- Required common fields are `v`, `kind`, `session`, `incarnation`,
  `generation`, and a nonzero strictly increasing `sequence`.
- `state` is `idle`, `active`, `child`, or `unknown`.
- `inputBuffer` is `empty`, `nonempty`, or `unknown`. This is deliberately not
  named composer state; adapters translate their own UI facts.
- `validForMs` is anchored when st2 receives the line and capped at two seconds.
  A newer event invalidates the prior lease.
- Unknown fields, versions, or kinds; malformed/oversized/non-UTF-8 lines;
  identity or sequence mismatch; adapter EOF/error; and tuple changes fail
  closed. Opaque `reason` is not interpreted.

Only fresh `idle` + `empty` proceeds. st2 reads one PTY STATUS packet and
requires exact equality across:

```text
event.session     = PTY session
event.incarnation = activity.producerEpoch
event.generation  = STATUS generation = activity.generation
event.sequence    = activity.sequence
event.state       = activity.state = idle
```

It then records exact durable attempt ownership and sends the existing
normalized bracketed-paste DING notice plus Return as opaque bytes in PTY's
generation/I/O-revision guarded packet. PTY compares both tokens and writes once
in the same event-loop turn. A successful guard creates PTY ownership, not a
positive harness receipt, and the notice is not written again while unread,
including after a sidecar restart. A proven conflict writes zero guarded bytes
and clears attempt ownership. A transport error is ambiguous after packet write,
so ownership remains fail-closed. Both outcomes invalidate the lease and require
a newer qualifying event before any unowned work can proceed.

The PTY contract is defined by the stacked experimental
[activity PR](https://github.com/compoundingtech/pty/pull/131) and
[guarded-send PR](https://github.com/compoundingtech/pty/pull/133). Rich DING
must remain opt-in until that substrate and external acceptance are available.

## Turn-boundary hook ownership

Hook delivery is separate from the activity adapter. A rendered provider hook
first injects exact unread filenames into an already-occurring next context,
then calls:

```text
st2 ding-control --identity <host.identity> hook-owned \
  --message <exact-unread-filename.md> [--message ...]
```

The ingress resolves the durable inbox, rejects invalid, duplicate, archived,
or missing filenames, and atomically records ownership beside the inbox. The
sidecar rechecks receipts on every wake/restart. Hook-owned filenames are
removed from rich PTY work while unread; archive removes their durable
ownership. The command does not read provider hook JSON, install hooks, trigger
a model call, press Return, or provide mid-turn interruption.

Rich logs emit typed JSON receipts for `held`, staged `error`, `hook-owned`,
`guard-conflict`, and `pty-owned`. The legacy path emits one `fallback` receipt
only to make its unchanged selection explicit.

## Known limits

- Idle proof depends on footer chrome that a harness may render differently
  across permission or approval modes. A harness whose footer is not recognized
  in a given mode defers indefinitely rather than delivering. This is an
  explicit limit per `T01`, and each harness spec states which modes it proves.
- The classifier is a measured heuristic over rendered text, not an evented
  signal, so a renderer change can defer delivery until the grammar is updated.
  Tracked as `DQ2` in [`../spec.md`](../spec.md).
