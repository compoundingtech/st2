# DING specification

This document specifies the mechanism shared by every maintained harness. It
builds on [requirements.md](./requirements.md). Per-harness screen grammars are
specified in [`01-claude/spec.md`](./01-claude/spec.md) and
[`02-codex/spec.md`](./02-codex/spec.md).

## Status

Active. A map to the implementation and its evidence, not a replacement for the
tests.

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
peek ─► classify ─┬─ ExactSafe ────► final observation ─► Return ─► Delivered
                  ├─ ExactBlocked ─────────────────────────────────► Staged
                  ├─ Changed / Ambiguous ──────────────────────────► Deferred
                  └─ EmptySafe ─► paste ─► observe until deadline ─┬► Delivered
                                                                   ├► Staged
                                                                   └► Deferred
```

Delivery is two-phase: a bracketed paste that carries no Return, then a separate
bare Return gated on a second exact observation (`DING-R02`). The observation
loop after paste runs to a bounded deadline; expiry yields `Staged`, never a
Return.

Every failure of a terminal command after paste has begun resolves to `Staged`
rather than `Deferred` (`DING-R07`): the paste may already have reached the
harness, so ownership is retained and retry re-inspects instead of re-pasting. A
retry of a staged payload is inspect-only — it may submit or defer, but it never
pastes.

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

## Known limits

- Idle proof depends on footer chrome that a harness may render differently
  across permission or approval modes. A harness whose footer is not recognized
  in a given mode defers indefinitely rather than delivering. This is an
  explicit limit per `T01`, and each harness spec states which modes it proves.
- The classifier is a measured heuristic over rendered text, not an evented
  signal, so a renderer change can defer delivery until the grammar is updated.
  Tracked as `DQ2` in [`../spec.md`](../spec.md).
