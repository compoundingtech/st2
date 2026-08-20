# DELTA-003: the stream subsystem is ratified with no shipped implementation

Status: open

## Divergence

[04-stream/requirements.md](../04-stream/requirements.md) (STREAM-R01..R09) is
ratified and [04-stream/spec.md](../04-stream/spec.md) is a settled Draft, but
`main` contains no stream code: no `stream` KDL node, no `st2 event emit`, no
`st2 stream add/rm`, no dedup ring, no `»` DING marker. The evidence behind the
design lives in the four committed
[04-stream experiments](../04-stream/.experiments/). The implementation is
under review in the stacked [PR #300](https://github.com/compoundingtech/st2/pull/300),
not yet shipped on `main`.

## VRS

Decisions [0004](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)
and [0005](../.decisions/0005-streams-are-agent-nested-and-stream-named.md) are
accepted; design issue
[#286](https://github.com/compoundingtech/st2/issues/286) requests upstream
review per root spec DQ1's approval bar. Open sub-questions are
[DQ-S1..DQ-S7](../04-stream/open-questions.md); DQ-S1 (producer `from`
grammar) blocks the event-record wire shape and should be resolved first.

## Implementation

[PR #300](https://github.com/compoundingtech/st2/pull/300) is the durable
implementation record. Its reviewed commit series implements declared ingress,
fail-closed and no-follow boundaries, bounded publication state, stream
authoring, lifecycle lowering, and publish-before-compact crash safety. The
normative design inputs remain decision 0005 and the committed differentiation
and lifecycle experiments; local worktree names are deliberately not
provenance.

The remaining review work is tracked on that PR and its exact head rather than
copied here as a mutable commit list. In particular, its publication state must
reconcile an abandoned pending reservation as specified in STREAM-R05 before
this delta can close. Wait-style adapters must keep their process alive after
a terminal emit (DQ-S8): the task model has no run-to-completion lifecycle, so
an exiting adapter flaps into a park.

Out-of-repo obligation: the `stream` node is an Agent Spec capability, so the
canonical `compoundingtech/evals/AGENT-SPEC.md` and the
[02-agent-spec](../02-agent-spec/spec.md) field rules must gain it in the same
effort — root R01 forbids shipping an undeclared capability, and R02's
admission must move `stream` from unknown-node rejection to typed validation.

## Direction

update implementation

## Resolution Signal

The verification plan in [04-stream/spec.md](../04-stream/spec.md) is
realized: the ingress, bounded-state, supersession, and companion-lifecycle
proofs exist under their final test names, the new INVARIANTS rows land with
`qualified_proof_references_resolve` green, `AGENT-SPEC.md` and 02-agent-spec
carry the `stream` field rules, and 04-stream/spec.md flips its Status from
Draft to Active. Close this delta in the commit that flips the Status line.
