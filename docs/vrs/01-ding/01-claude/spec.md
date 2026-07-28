# Claude harness specification

The screen grammar by which DING recognizes a Claude composer. It realizes
[`../requirements.md`](../requirements.md) through the mechanism in
[`../spec.md`](../spec.md).

## Status

Active.

## Locating the composer

The composer is drawn between two horizontal rules. Both are located by
structure rather than by width: a rule is a line whose trimmed content is at
least forty characters and consists entirely of the box-drawing horizontal
character. The composer occupies the rows between the last two rules on the
ANSI-stripped screen; the footer is everything below the lower rule.

The prompt is the `❯` character followed by a no-break space. A narrow space is
load-bearing here: the no-break space is Unicode whitespace, so trimming a
composer row before removing the prompt erases the prompt on an *empty*
composer and makes it unlocatable. The prompt is therefore removed before any
trimming.

Locating the composer is what identifies the harness. Startup output, including
the version banner, is not used: an inspection sees the current viewport, so
anything printed once at session start is absent from every screen after the
first (`DING-R05`). The banner is in fact weaker than that argument assumes — at
common pane widths it is not rendered at all, so it is absent from the very
first screen too.

Failing to locate a composer means this harness proves nothing about the screen;
it does not by itself make the screen `Ambiguous`. Under positional dispatch the
screen is offered to every maintained harness, so a pane holding another
harness's live composer is classified by that harness (`DING-R04`). `Ambiguous`
is the result when *no* harness locates a composer.

## Proving idle

The footer carries the permission-mode indicator that ships with the composer
chrome. Idle requires both the mode marker and the permission state to be
present.

The keybinding hint that may appear beside them is **not** an idle signal. It is
composed at runtime from a keybinding lookup, so its text changes when the
binding is remapped and it is absent from many screens that are equally idle.
It may corroborate an idle footer; it may not decide one.

## Proving empty

Two shapes count as positively empty:

- an empty composer, and
- a composer holding only the rotating example placeholder, matched as the exact
  `Try "<single-line example>"` grammar with a bounded, control-free example.

The empty composer is the stronger of the two. The placeholder is not styled
differently from typed input, so recognizing it relies on a text grammar a human
could in principle type; an empty composer cannot be confused with a human
draft, because a draft is not empty.

## Blocked states

Return is withheld while the screen shows an active turn or a modal. Two
distinct shapes prove an active turn:

- an explicit interrupt hint, and
- the animated progress line, which carries a rotating verb followed by an
  ellipsis, and which **carries no interrupt hint at all**.

The second shape is required. A pane mid-turn has an empty composer and
unchanged footer chrome, so without it a working agent's screen satisfies both
the idle and empty proofs and would receive a Return mid-turn. On the current
build the first shape is not observed at all: no interrupt hint accompanies the
progress line at any width, so the second shape is the only one that fires. The
first is retained because it is cheap and fails closed.

The predicate is derived from real captured screens, and two properties of those
screens constrain it more tightly than it first appears.

**The leading glyph is not a stable key.** It animates through several
box-drawing and punctuation characters within a single turn. Matching any one of
them fails open for most of the turn.

**The elapsed timer is not always present.** Some active frames render the verb
and ellipsis alone, with no parenthesized timer. Requiring a timer would fail
open on exactly those frames.

**A completed turn renders an almost identical line**, distinguished only by
using a past-tense verb with an elapsed duration in place of the ellipsis. This
is the trap: that completed line sits above *every* idle composer, immediately
after every turn. A predicate keyed on the glyph, or on any shape that both
lines share, would therefore treat every idle pane as blocked and stop delivery
permanently, rather than erring conservatively as a widened blocked-check
normally does. The distinguishing signal is the ellipsis directly following a
single verb; nothing coarser is safe in the delivery-stopping direction.

Modal prompts specific to this harness — plan prompts, retry and capacity
notices, queued-message notices — are also blocking and are owned here, not by
shared code (`DING-R06`).

## Known limits

- The permission-state literal proves one permission mode. A pane in another
  mode does not reach a positively idle state and defers indefinitely rather
  than delivering. Widening this is a behavior change to what counts as idle,
  which per [`../spec.md`](../spec.md) requires evidence from a real screen, not
  a looser match.
- Recognition is tied to this harness's current rendering. A renderer change
  defers delivery until the grammar is updated; it does not produce a wrong
  delivery, because every unrecognized shape is `Ambiguous`.
