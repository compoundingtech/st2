# Resource read surfaces and link-plane adoption

**Date:** 2026-08-27
**Catalog:** one live catalog, host `dev3`, 655 agent declarations.

## Question

Do the two edges called *resource* — declared bindings and `st2 resource` link
records — name the same things, and is the link plane still carrying work?

## Method

Aggregate shell census over every `agent.kdl` and every
`resources/links/*.md` in the catalog: URI and URL sets compared exactly and
after normalization (trailing slash stripped, `%20` decoded) on the schemes both
planes use; link-record creation time read from the `<unix-ms>` filename prefix;
readers of the link plane found by searching the source tree for `links_dir`.

A shell renderer
([`render-read-surfaces.sh`](./render-read-surfaces.sh)) prints candidate
`st2 resource ls` shapes for one declaration, reading the same bytes st2 reads.
It was checked against the two declarations in
[dotfiles#2071](https://github.com/schickling/dotfiles/pull/2071) —
`dotfiles.fb-batch1.docs.worker` (5 bindings, 0 link records) and
`dotfiles.vista.pr1506-final.worker` (2 bindings, 14 link records).

## Result

The renderer reproduced both reported outputs exactly, including
`# 0 resources for dev3.dotfiles.fb-batch1.docs.worker`, so it is faithful to
the surface under test.

Disjointness:

- 1161 bindings across 647 declarations; 241 link records across 82.
- 889 distinct binding URIs, 233 distinct link URLs, **0 coinciding** under
  exact match and under normalization on all five shared schemes (`file`,
  `worktree`, `git-commit`, `https`, `st2-message`).
- 81 of 82 link-record-carrying declarations also carry bindings.
- 13 path-prefix hits across 6 declared worktrees: link outputs living inside a
  declared worktree. Containment, not identity.

Link-plane adoption:

- 82 of 655 declarations (12.5%) ever wrote a link record.
- Creation peaked 2026-08-07 through 08-09 (44, 45, 29 records/day), then
  decayed; 5 records on 2026-08-26 and none since.
- 14 of the 25 most recent records come from a single declaration on one day.
- **The only reader of link records in the source tree is the
  `st2 resource ls|read` verb itself.** No projection, roster, doctor, or other
  consumer reads them.
- `axe work update --artifact <path> --pty <name>` covers the same job, and
  matches the observed `relation` values (`output` 185, `evidence`, `produces`)
  and the 8 `pty://` URLs.

Binding realization:

- Self-state carriers realize into the agent's `resources/` directory:
  `dev.schickling.agent-goal://` → `resources/goal.md`,
  `decision-tree://` → `resources/context/decisions/`.
- `resources/context/now.md` exists on 605 of 655 declarations and is declared
  by none; no binding URI anywhere mentions it.

Reason-field quality:

- 987 of 1161 bindings carry the placeholder
  `reason="Legacy binding retained without recorded rationale."` from a
  2026-08-22 backfill. Declarations touched on 2026-08-26 and 2026-08-27 are
  26 of 27 placeholder-free and carry real prose.

## Conclusion

The two planes are disjoint by construction, not by coincidence: bindings carry
what an agent is for, link records carry what it produced. A shared descriptor
type would deduplicate nothing.

The link plane is legacy. It is write-only by construction, adopted by an eighth
of the fleet, decaying, and superseded by `axe work`. The `resources/` directory
is not a third meaning of the word — it is the realization surface for the
bindings, and remains canonical for an agent's resource files.

`reason` is working; the 987 placeholders are one un-cleaned backfill.

## VRS Impact

- [spec.md](../spec.md): the two edges, their disjointness, the write-cost
  asymmetry that caused the drift, and the retirement of the link plane.
- [ontology.md](../../ontology.md): `Resource`, `Resource binding`,
  `linked record` (retired), `agent resource directory`, `working state`.
- [open-questions.md](../open-questions.md): DQ-R4 (write-cost, unresolved),
  DQ-R7 (placeholder cleanup), DQ-R8 (upstream sync).
- No requirements change. A delta was drafted for the working-state carrier and
  withdrawn once [#351](https://github.com/compoundingtech/st2/pull/351) rewrote
  R20; see [decision 0012](../../.decisions/0012-working-state-is-a-declared-carrier.md)
  Amendment 1.

## Limits

One catalog, one host, one downstream convention. Zero overlap is strong
evidence that the planes are disjoint *as used here*; it does not prove no
consumer would ever want to name one Resource from both. The renderer covers
read shape only and says nothing about write cost, which
[DQ-R4](../open-questions.md) holds open.
