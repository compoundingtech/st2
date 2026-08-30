# `gate` versus `judge` in st3

This inventory uses commit `a827714` on 2026-08-30. It separates grammar changes from internal and historical wording.

## The existing meaning of `gate`

`gate` is already an implemented st3 KDL node. It belongs inside `supervisor` and controls bounded terminal input.

The runtime parses `GateSpec`, matches screen text, sends fixed keys, and records `gate.observed`. The design contains three KDL gate examples.

Production Rust contains 55 gate-family tokens across four files. No checked-in eval uses the supervisor gate yet.

st2 has no KDL gate node. Its production source uses `gate` 41 times across 11 files for boot, validation, and test barriers.

Therefore, `gates` and `gate` would give one st3 document two unrelated gate concepts. Parse context can disambiguate them, but readers still must.

## Question 1: rename `judges` and `judge`?

### Arguments for `gates` and `gate`

- Every condition controls passage to the next state. `gate` names that operational effect directly.
- Built-in graph predicates are conditions, not actors. `field`, `exists`, and `empty` read naturally as gate conditions.
- Dependency predicates use the same internal predicate model. `gate` fits their hold-and-release effect better than `judge`.
- The plural block is a conjunction. All conditions must pass, which resembles a bank of gates.

### Arguments for keeping `judges` and `judge`

- `gate` already names the supervisor screen-input feature. Reuse creates a real language collision, not only a style concern.
- Mechanical, LLM, and human forms evaluate evidence and return reasons. `judge` describes their work and authority.
- The CLI, API, claims, driver names, environment variables, and operation subjects already use the judge family.
- `judge` separates evaluation from passage. A judge decides; the step state controls passage.
- A deadline is a failure limit, not a closed gate waiting to open. Neither word is perfect, but `gate` adds that wrong implication.

### Fit across the current runtime

The singular `judge` keyword names only mechanical and LLM runners. Human review, deadlines, and stored predicates keep their own child names.

| Runtime form | `gate` fit | `judge` fit |
|---|---|---|
| `exists`, `empty`, `field`, `has`, and `lacks` | Strong for the blocking effect. | Acceptable as a generic predicate, but it sounds human. |
| Dependency predicates | Strong because they hold step access. | Weak because no evaluator exists. |
| `deadline` | Mixed because it stays satisfied until expiry. | Mixed because it is a clock guard. |
| Mechanical and LLM runners | Acceptable for their effect. | Strong because they inspect evidence and decide. |
| Human review | Strong as an approval gate. | Strong as an authority that decides. |

`gate` fits the consequence of all nine `JudgeSpec` forms. It does not fit the nature of every form.

## Counted rename surface

| Surface | Count at the inventory commit | What must move |
|---|---:|---|
| Eval KDL | 20 files contain 55 `judges` blocks and 68 running `judge` nodes. | A grammar rename must update all 20 files. |
| Example KDL | Four of nine files contain 18 blocks and two running nodes. | A grammar rename must update those four files. |
| `produces` KDL | Nine eval files contain 37 blocks. Examples contain none. | Only a `produces` decision affects these files. |
| Production Rust identifiers | Seven files contain 233 judge-family identifier occurrences. | A KDL-only alias can keep them. A complete terminology rename cannot. |
| Rust integration-test identifiers | One file contains 11 judge-family identifier occurrences. | Only a complete terminology rename needs these names. |
| st3 documentation | Nine files contain 271 exact `judge`, `judges`, or `judgement` words. | Current specifications must move. Dated reports and snapshots should remain historical. |
| Eval support material | 55 files sit under ten `judges` directories. Another 69 files contain 172 judge-family words. | Script paths and run receipts can remain. They do not define grammar. |
| Graph viewer output | The viewer has zero fixed `judge` or `gate` labels. | It needs no code change for a KDL-only rename. |

The viewer prints authored step IDs and titles. The eval corpus has 21 judge-containing step IDs across 20 files, so those names can remain visible.

A complete rename also reaches `st3 judgement`, `/v1/judgements`, two result claim names, two driver names, and `ST3_JUDGE_*` variables.

## Question 2: namespace `produces`?

The runtime does not create a `JudgeSpec` from `produces`. It parses `ProductSpec` values and calls `products_hold` before declared judges.

The mental model is still correct about effect. A missing product silently prevents step completion and keeps the step in `verifying`.

### Arguments for the current `produces`

- It reads as an output contract and stays short beside `produces-plan`.
- The worker must publish the output. st3 only verifies the declared shape.
- The internal product model stays independent from evaluator terminology.

### Arguments for a namespaced form

- A prefix makes the hidden completion condition visible to a KDL reader.
- It distinguishes an output promise from an action that creates an output.
- It groups product checks with other acceptance conditions.

### Costs of the proposed prefixes

- `gate.produces` reuses the existing supervisor word and deepens the collision.
- `judge.produces` avoids that collision, but it implies a judge object that the runtime does not create.
- The dot provides visual grouping only. The parser still treats the complete text as one step-child keyword.
- Either prefix couples the product contract to the separate `gate` versus `judge` decision.
- A `judges { product ... }` form would make acceptance structural. It would no longer sit beside `produces-plan` as an output contract.

The namespacing answer therefore changes with the chosen word. `judge.produces` has less collision risk, while `gate.produces` has stronger effect language.

## Cost of deciding later

st3 is absent from `origin/main`, every tag, and every release. I found no supported external KDL consumer.

The public design branch exists, so copied experiments are possible. They do not create a published compatibility promise.

A rename now affects the corpus and documentation but needs no external migration period. A rename after release needs aliases or a new KDL version.

A grammar alias can normalize to the current Rust model and preserve plan hashes. A full internal rename changes serialized names and can change revisions.

Choosing `gate` now risks a later collision with supervisor gates. Keeping `judge` risks a later compatibility alias if its predicate meaning remains uncomfortable.

## My view

Keep `judges` and `judge` because `gate` already has a different implemented KDL meaning. Decide the product-condition name separately from that choice.
