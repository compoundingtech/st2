# Codex harness specification

This document describes the transport as implemented today. The intended destination is that st2
writes inbox files and a watcher owned by the session pushes them into the provider channel, as
specified for Claude. The control protocol described below is expected to be replaced by that shape.

The screen grammar by which DING recognizes a Codex composer. It realizes
[`../requirements.md`](../requirements.md) through the mechanism in
[`../spec.md`](../spec.md).

## Status

Active.

## Provider-native delivery

The controlled app-server transport starts a turn after the watcher sees
`idle` or a completed turn with `systemError`. A `systemError` blocks delivery
until `turn/completed` makes the failed turn terminal. The completed error
remains the typed `TerminalError { SystemError }` diagnostic until a new
`turn/started` event or later provider status replaces it. This diagnostic
permits `turn/start`; it never permits `turn/steer`.

Codex preserves its system-error status after turn completion and sends no
later status notification. The next turn clears the provider error. The
`notLoaded`, review, compaction, human-wait, and conflicting-turn states remain
delivery holds.

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
enumeration in [`../spec.md`](../spec.md). Codex can wrap after a hyphen when
the next chunk does not fit, so the row before a continuation can be shorter
than the composer width. Each non-empty continuation starts with the renderer's
two-cell indent. The adapter treats that indent as a wrap boundary, although
screen pixels cannot distinguish it from a hard newline followed by two literal
spaces. This knowingly admitted ambiguity is tracked in
[#250](https://github.com/compoundingtech/st2/issues/250). An unfamiliar
continuation shape remains unsupported.

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

## Failed turns

This section describes the control protocol named in the opening paragraph, not the screen.

A turn that ends because the thread reported an error resolves to that error, held. The thread
status is the only signal that reports a thread condition, so the turn's own completion neither
clears the condition nor stands as evidence of a second live turn. Delivery is gated for as long
as the hold stands: a thread that has just reported a system error, or that is not loaded, is not
a thread st2 sends into. Only a later thread status releases it — `idle` directly, or `active`
followed by the turn that starts under it — never turn traffic alone.

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
