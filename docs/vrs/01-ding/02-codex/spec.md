# Codex harness specification

The screen grammar by which DING recognizes a Codex composer. It realizes
[`../requirements.md`](../requirements.md) through the mechanism in
[`../spec.md`](../spec.md).

## Status

Active.

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

The footer is read from the composer downward. Idle requires the status line
this harness renders below its composer, identified by the model-identifier
prefix together with the separator that precedes its command hint.

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
pattern. An empty composer alone, disappearance, a different live draft, and
unrecognized pixels are `Unproven`.

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
