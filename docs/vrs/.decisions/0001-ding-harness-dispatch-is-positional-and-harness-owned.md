# DING harness dispatch is positional, and each harness owns its own vocabulary

Status: accepted

## Context

DING classified a screen by asking, in a fixed order, whether it looked like one
maintained harness and then the other, and it applied one shared active-turn and
modal predicate to every harness. Both choices were sound while the classifier
was small. Both produced defects once a real fleet ran against it.

Ordered dispatch has no way to express *which composer will receive the
keystrokes*. It answers "does this screen contain harness X" when the safety
question is "what will this pane do with input". A shared blocked-predicate has
the same shape of error one level down: it lets one harness's rendering decide
another harness's safety gate.

Splitting the implementation into one module per harness forced the question,
because a module boundary has to be drawn somewhere, and the wrong boundary
would have preserved both defects behind a tidier layout.

## Evidence and Argument

Three independent forms, none of which is reasoning alone:

- **Implementation fact.** The shared active-turn predicate contains literals
  drawn from one harness's rendering — plan prompts, capacity notices, retry
  hints — and applies them to every pane. It also *omitted* that harness's
  animated progress line, which does not always carry an interrupt hint. The
  omission is only invisible while a separate parse defect keeps such screens
  unclassifiable; removing that defect makes a mid-turn pane satisfy both the
  idle and empty proofs.
- **Independent critique.** An automated review of the change that reordered the
  two harness checks identified the transcript case: a pane whose scrollback
  contains a captured screen from another harness would be classified on that
  scrollback rather than on its own live composer, admitting a paste — or a
  Return — into a human's draft. This was found by an adversarial reader, not by
  the change's author.
- **Implementation fact.** The two harnesses do not match against the same
  representation. One locates its composer in the raw screen including escape
  sequences, yielding a byte offset; the other locates against ANSI-stripped
  lines, yielding a line index. Any positional rule must therefore normalize to
  a common unit, which an ordered chain never had to confront and which a naive
  positional comparison would get silently wrong.

## Options

| Option | Tradeoffs |
| --- | --- |
| Keep ordered dispatch, special-case the transcript screen | Smallest change. The rule has to be written for each pair of harnesses, so it grows quadratically, and the next harness reopens the same class of defect. Does not address the shared predicate. |
| Reverse the order instead | Trades one wrong answer for another: it restores the original defect, where a used pane of the first harness is unclassifiable, from the opposite side. |
| Positional dispatch over a harness registry, with per-harness vocabulary | Requires an interface, a position type, and a unit conversion that did not previously exist. Duplicates some near-identical interrupt literals across harnesses. Makes the transcript case structural rather than special-cased, and confines each harness's rendering to its own safety gate. |

## Decision

Positional dispatch over a registry, with each harness owning its composer
markers, footer chrome, idle proof, and active-turn and modal detection.

Every registered harness is asked to locate a composer; the one lowest on the
screen is classified. This is correct by construction rather than by
enumeration: scrollback is above the live composer, so the composer that will
receive the keystrokes is the lowest one, whatever a transcript happens to
contain. Positions are normalized to a screen row before comparison, because the
harnesses report incomparable units.

Shared code keeps only what is genuinely common to every harness. The duplicated
interrupt literals are accepted cost: a shared vocabulary is what let one
harness's strings, and one harness's omissions, silently govern another's panes.

## Consequences

- Adding a harness is a new module and a registry entry; it does not edit shared
  classification. This is the property that keeps the transcript defect from
  recurring per pair.
- Widening what counts as blocked remains always-safe and may be done per
  harness. Widening what counts as idle requires evidence from a real screen —
  a rule this decision does not relax.
- The requirements this pins are `DING-R04` and `DING-R06` in
  [`../01-ding/requirements.md`](../01-ding/requirements.md).
