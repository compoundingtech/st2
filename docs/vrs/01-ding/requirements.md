# DING requirements

## Context

DING is the sidecar that turns an unread inbox message into text in a running
agent's terminal composer. It is the only st2 subsystem that writes into a
surface a human may also be typing into, so its contract is written as a set of
preconditions for pressing Return rather than as a delivery guarantee.

This decomposes [`R05`](../requirements.md) — the ratified floor for inbox
delivery, archive precedence, retries, suppression, and restart recovery — into
the subsystem's own obligations, and inherits the watcher obligations in `R14`
and `R15`. Where this file and the root disagree, the root wins and this file is
wrong.

The realization per maintained harness is specified in
[`01-claude/spec.md`](./01-claude/spec.md) and
[`02-codex/spec.md`](./02-codex/spec.md). The mechanism shared by all harnesses
is in [`spec.md`](./spec.md).

## Assumptions

- **DING-A01 Rendered screens only:** The only available evidence about a
  composer's state is a rendered terminal screen. No maintained harness exposes
  an evented idle signal, so every precondition below is a measured heuristic
  over text. `DQ2` in [`../spec.md`](../spec.md) tracks closing that gap.
- **DING-A02 Cooperative human:** The human sharing a pane is not adversarial.
  A screen that deliberately imitates another harness's composer is a
  correctness concern, not a security boundary, consistent with `A02`.

## Acceptable Tradeoffs

- **DING-T01 Deferral over delivery:** A message that is never delivered is a
  worse outcome than one delivered late, but both are better than text typed
  into a human's draft or a Return pressed on their behalf. Every ambiguous
  case resolves toward deferral.
- **DING-T02 Narrow positive recognition:** Recognizing fewer screen states
  than a harness can render is preferred to recognizing a state loosely. A
  documented unsupported screen is consistent with `T01`; a loosely matched one
  is not.

## Requirements

### Must never disturb a human

- **DING-R01 Positive idle precondition:** Text is pasted only after a
  maintained harness's composer has been positively identified as present,
  empty, and idle. Absence of evidence that a human is typing is not evidence
  of an idle composer.
- **DING-R02 Two adjacent exact observations:** Return is pressed only after the
  exact staged notice has been observed as the complete composer contents twice
  in immediately adjacent inspections, with the final observation adjacent to
  the Return itself. Any change, block, or uncertainty between them prevents
  submission.
- **DING-R03 Fail-closed default:** A changed composer, a human draft, an
  active turn, a modal, an unreadable screen, an unrecognized harness, and a
  bounded observation timeout all withhold Return. The default for anything not
  positively understood is deferral.

### Must classify the surface it will actually type into

- **DING-R04 Live composer, not transcript:** Classification targets the
  composer that will receive the keystrokes — the live composer at the bottom of
  the viewport. Harness-shaped text appearing anywhere in scrollback, including
  a captured or pasted screen from another harness, must not be classified in
  its place. A pane is classified by what it will do with input, not by what its
  transcript resembles.
- **DING-R05 No dependence on transient output:** Harness identification relies
  on durable screen structure. Output present only near session start, such as a
  startup banner, must not be load-bearing, because a screen inspection sees the
  current viewport rather than the session's history.
- **DING-R06 Harness-owned recognition:** Each maintained harness owns the
  vocabulary that identifies its own composer, footer chrome, and active or
  modal states. Shared classification code carries only what is genuinely common
  to every harness, so one harness's rendering cannot silently decide another
  harness's safety gate.

### Must remain bounded and idempotent

- **DING-R07 Staged ownership:** Once a paste command has started, the payload
  is owned by that attempt. Ambiguity about whether the paste landed is resolved
  by inspection only; the same notice is never pasted again until the exact
  staged payload has disappeared or changed.
- **DING-R08 Bounded probing:** Deferred delivery retries on a bounded backoff,
  so an indefinitely busy composer cannot make every inbox poll spawn another
  terminal probe.
- **DING-R09 Presence gate:** Declared `busy` is observable but never suppresses
  delivery; only fresh `dnd` defers it. Delivery may wake a working agent.
- **DING-R10 Positive harness receipt:** `Delivered` requires adapter-provided
  positive evidence that the unique exact notice moved from the lowest live
  composer into the maintained harness's rendered submitted or queued surface.
  PTY command success, generic screen change, disappearance alone, and
  ambiguous pixels are not receipts. Until that evidence exists, a transport
  attempt retains staged ownership and retries by inspection without
  re-pasting.

## Evidence

Each guarantee above is pinned by a named test in
[`INVARIANTS.md`](../../../INVARIANTS.md) under **Fail-closed observed native
DING**, **Bounded DING PTY probe churn**, **Mutation-only filesystem wakeups**,
and **Agent-declared presence discipline**. A change that keeps those tests
green but violates a requirement here means the invariant set is incomplete, not
that the requirement is satisfied.
