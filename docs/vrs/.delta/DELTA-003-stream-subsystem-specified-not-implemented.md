# DELTA-003: the stream subsystem is ratified with no shipped implementation

Status: open

## Divergence

[04-stream/requirements.md](../04-stream/requirements.md) (STREAM-R01..R09) is
ratified and [04-stream/spec.md](../04-stream/spec.md) is a settled Draft, but
`main` contains no stream code: no `stream` KDL node, no `st2 event emit`, no
`st2 stream add/rm`, no dedup ring, no `»` DING marker. The evidence behind the
design lives as committed spikes in four exploration worktrees, not in this
tree.

## VRS

Decisions [0004](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)
and [0005](../.decisions/0005-streams-are-agent-nested-and-stream-named.md) are
accepted; design issue
[#286](https://github.com/compoundingtech/st2/issues/286) requests upstream
review per root spec DQ1's approval bar. Open sub-questions are
[DQ-S1..DQ-S7](../04-stream/open-questions.md); DQ-S1 (producer `from`
grammar) blocks the event-record wire shape and should be resolved first.

## Implementation

The implementing agent starts from the differentiation-pole worktree —
`.bare/.claude/worktrees/agent-ae190cd91778e7149` (`src/event.rs`,
`tests/event_e2e.rs`, 12 green tests) — with two renames per decision 0005:
its per-emit `stream` axis becomes `key`, and its top-level `source`
declaration becomes the agent-nested `stream` node. The lifecycle worktree
`agent-a540e72fcb3229f96` contributes the declaration/lowering/companion tests
(27 green) modulo the `pipe`→`stream` rename and the removal of its line-
protocol runner (adapters call `st2 event emit` directly). Still unbuilt
anywhere: the `resources/streams/<name>/` ring store behind DQ-S2/DQ-S3, the
`st2 stream add/rm` authoring commands, the nix-layer real adapters — the
first two are `gh-ci-watch` and `pty-lifecycle-watch` (the "waits are standing
feeds" doctrine in the spec: keyed pty phase/exit events replace agents
polling `pty peek`) — and the new INVARIANTS rows named in the spec's
verification plan. Wait-style adapters must keep their process alive after a
terminal emit (DQ-S8): the task model has no run-to-completion lifecycle, so
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
