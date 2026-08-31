# Root counting must fold retirement before graph construction (#402)

Date: 2026-08-31
Worktree: schickling/2026-08-31-issue-402 @ 67b18b7 + fix

## Question

Does the one-root-per-host invariant (root-count, admitted topology) reject the live dev3
catalog because legacy `retired #true` declarations hold the root slot, and does folding
retirement before counting fix it without weakening the invariant for genuine faults?

## Method

Built the worktree binary and ran `st2 validate` and `st2 catalog graph --json` against
five temp catalogs (dev3 shape: 1 running root + 2 legacy-retired + 1 new-style-retired
root-shaped declarations; suspended-only root; tombstone-only host; headless host — active
worker under a retired root; two running roots) and once against the live dev3 catalog
under a read-only shared lock. Baseline comparison: same commands with the deployed
pre-#399 `st2` and with the unmodified-main binary (keyed-stash run).

## Result

- Unfixed binary, dev3 shape: `root-count: host 'dev3' must declare exactly one root
  agent; found 4`; graph `complete: false`; `declarations[].agents[].desiredState` null
  for legacy-retired declarations. Live dev3: same error with `found 8`, later confirmed
  as `cos` + 7 root-shaped legacy-retired declarations.
- Fixed binary, dev3 shape and live dev3: validate carries no root-count error; live
  graph reports exactly one counted root (`dev3.cos`), `cos` gets `rootId: dev3.cos,
  depth: 0`, and 622 declaration entries publish the folded `desiredState: "retired"`.
- Suspended-only root: green — a suspended root still counts.
- Tombstone-only host and headless host: `root-count … found 0` — genuine faults stay
  errors. Two running roots: `found 2` — regression intact.
- Full `cargo test --test validate --test catalog_graph` green after the fix and the
  six stale-test repairs; every CI-gated flake target green.

## Conclusion

The defect was #399's root counting (validate.rs root_counts, catalog_graph.rs
admitted_topology) ignoring the folded desired state, not the spec model — the fold
existed end-to-end and only the invariant's predicate and the declarations view missed
it. Excluding retired declarations (either spelling) via one shared predicate
(`supervisor_chain::is_counted_root`) fixes the live catalog while every genuine
topology fault still errors. Zero-count hosts remain faults by design (headless org).


Post-review addendum (#405, Codex P1): the first cut of the predicate opened a
hole the pre-fix code had closed only by accident — one active root plus a
retired root still supervising an active worker validated clean (`found 1`,
`complete: true`) while publishing the tombstone as the worker's `rootId`.
Reproduced on the PR head, then closed with a `retired-root` validation error:
an active agent's chain must terminate at a counted root. Retired chains under
a retired root stay legal; the fixture is
`an_active_chain_may_not_terminate_at_a_retired_root`.

## VRS Impact

`docs/vrs/spec.md` (catalog graph / R04–R05 area) now states the counting fold: retired
declarations never hold the root slot, suspended roots still count, and the declarations
view folds legacy `retired #true` to `desiredState: "retired"`. Requirements R04/R35 are
unchanged — "exactly one root" is interpreted over the non-retired org chart.
