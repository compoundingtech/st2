# The linked-record plane is retired and `resource` names one concept

Status: accepted

Design decision made by Johannes on 2026-08-27 (interview over the
07-resource measurements and
[dotfiles#2071](https://github.com/schickling/dotfiles/pull/2071)). Merge and
acceptance approval required: upstream maintainers.

## Context

Two durable edges were both called *resource*: Agent Spec Resource bindings, and
the link records written by `st2 resource add`. An agent reading one while
reasoning about the other measured the wrong store — a declaration with five
bindings and no link records reported `# 0 resources`, and a declaration with
two bindings and fourteen link records reported fourteen rows containing neither
binding. Both surfaces answered correctly; neither said which question it had
answered.

The measurements (07-resource `.experiments/2026-08-27-resource-read-surfaces.md`,
one live catalog of 655 declarations) established that the planes are disjoint by
construction rather than by coincidence: 889 distinct binding URIs and 233
distinct link URLs share **zero** members, exactly or normalized. Bindings carry
what an agent is for; link records carried what it produced.

They also established that the link plane is legacy. It was adopted by 82 of 655
declarations, its creation rate decayed from a peak of 45/day on 2026-08-08 to
none after 2026-08-26, **the only reader of a link record in the source tree is
its own `ls`/`read` verb**, and `axe work update --artifact <path> --pty <name>`
now covers the job — matching the observed `relation` values (`output` 185,
`evidence`, `produces`) and the 8 `pty://` URLs.

## Decision

The linked-record plane is **retired**. `st2 resource add|ls|read|remove` and
`resources/links/` are removed, and *resource* names exactly one concept: a
declared Resource binding. The freed verb becomes the binding surface, which the
declared plane never had — bindings were previously visible only through
`st2 agents --json`.

The 241 existing records are left in place as orphaned files rather than
migrated. Most belong to retired declarations whose worktrees are gone, and
`axe work` writes to a gitignored per-worktree path that no longer exists for
them.

Producing agents record artifacts through `axe work update --artifact/--pty`.

## Consequences

- `resource` is unambiguous across the CLI, the declaration, and the corpus.
  The read that produced the friction cannot recur, because there is no second
  plane to read.
- `<agent-dir>/resources/` is untouched and remains canonical for an agent's
  resource files. Only `links/` goes. The directory is the realization surface
  for bindings, not a second sense of the word.
- `templates/bus.st2.md` loses the advertisement that produced the adoption.
- Surviving `resources/links/` files become unreferenced. The ontology keeps a
  retired entry so a reader who meets one can identify it.
- Nothing downstream breaks: no consumer other than the retired verb read them.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Retire the plane; `st2 resource` becomes the binding surface | Selected | Nothing but its own verb ever read a linked record, adoption was 12.5% and decaying, and `axe work update --artifact/--pty` covers the job. Retiring makes *resource* unambiguous by removing the second plane rather than by wording around it. |
| Freeze read-only: keep `ls`/`read`, drop `add` | Rejected | Keeps records reachable, but *resource* keeps naming two things for as long as any record survives, so the misread stays possible. |
| Migrate the 241 records into `axe work`, then retire | Rejected | Highest fidelity, but `axe work` writes to `<repo-root>/tmp/worklog/`, gitignored and per-worktree; most of the 82 declarations are retired and have no reachable worktree to write into. |
| Keep and invest: add `--json`, a requirement, a consumer | Rejected | Asks the fleet to adopt a second evidence ledger beside the one it already uses. In the plane's whole lifetime nothing consumed it. |
| Unify both planes behind one typed reference ([#122](https://github.com/compoundingtech/st2/issues/122)) | Rejected | With zero measured overlap a shared descriptor deduplicates nothing and joins nothing, and #122's proposed `{_tag, uri}` assumes a field removed in #307. |

## Evidence and Argument

The measurement is
[`07-resource/.experiments/2026-08-27-resource-read-surfaces.md`](../07-resource/.experiments/2026-08-27-resource-read-surfaces.md),
taken against one live catalog of 655 declarations. Three findings decide it.

**The planes are disjoint by construction.** 889 distinct binding URIs and 233
distinct linked-record URLs share zero members, under exact match and after
normalization on all five schemes both planes used. 81 of the 82
linked-record-carrying declarations also carried bindings, so they coexisted
constantly and still never named one thing. The cause is semantic: 728 bindings
were self-state carriers and 378 were work inputs, while 196 of 241 linked
records were `output`, `produces`, `evidence`, or `verified`. That is what kills
#122 — a shared type has nothing to deduplicate.

**The plane was write-only.** A source search for `links_dir` finds exactly two
readers, both inside the `st2 resource ls|read` implementation. No projection,
roster, or doctor consumed a linked record in the plane's lifetime.

**It was already being abandoned.** Creation peaked at 45 records on 2026-08-08,
fell to 5 on 2026-08-26, and stopped; 14 of the 25 most recent records come from
one declaration on one day. Meanwhile `axe work update` grew `--artifact` and
`--pty`, which match the observed `relation` values and the 8 `pty://` URLs
exactly.

The counter-argument — that low adoption is a surfacing problem, not obsolescence
— is answered by the second finding: a plane nothing reads cannot be surfaced
into usefulness by adding `--json` to it.
