# Harness identity and delivery evidence policy are distinct

Status: accepted

Johannes confirmed the design through the issue #506 interview, and Nathan
approved T03 and the proposed transactional-delivery requirement there. The
proposal called that requirement R41, but R41 already names typed agent desired
state, so the unchanged requirement takes the next unused identifier, R43.

## Context

st2 had one durable native-delivery ledger for Codex and OpenCode. Its harness
discriminator served two jobs: stable persisted identity and evidence grading.
Extending that match directly for Claude, pi, and OMP would put provider policy
branches into the ledger core and make the wire identity imply evidence those
providers cannot produce.

Codex observes correlated transport acceptance and consumption. OpenCode
observes correlated transport acceptance and durable read-back. Claude, pi, and
OMP expose no authoritative positive or negative receipt after the local
transport boundary. A local write, flush, extension return, result label,
process observation, or timeout therefore cannot settle their attempt.

## Options

| Option | Tradeoffs |
| --- | --- |
| Keep five stable harness identities and map them to three closed evidence policies — selected | Preserves wire and migration identity, keeps grading provider-neutral, and states attempt-only limits directly. |
| Add one ledger-core branch per provider | Rejected because provider growth would duplicate policy and couple the core to adapter details. |
| Use a runtime evidence-policy registry | Rejected because the five maintained harnesses need three static policies; runtime extension adds configuration and failure modes without a current consumer. |
| Collapse harness identity into evidence-policy identity | Rejected because three providers share attempt-only semantics but still require distinct persisted identity and foreign-ledger rejection. |
| Keep downgrade readers, dual writes, or translation bridges | Rejected under T03 because attempt-only adoption is forward-only and recovery rolls forward. |

## Decision

The delivery ledger has five stable harness identities: Claude, Codex, pi,
OpenCode, and OMP. Each identity maps to exactly one of three closed evidence
policies:

- attempt-only for Claude, pi, and OMP;
- Codex receipts for Codex; and
- OpenCode receipts for OpenCode.

Harness identity owns the persisted discriminator, foreign-ledger rejection,
and migration selection. Evidence policy owns phase grading and release. The
ledger core accepts the identity-policy pair and does not branch on provider
identity to decide evidence.

An attempt-only policy admits `Attempted` as valid durable state but grades no
evidence above it and never releases it. This makes an ambiguous attempt hold
across restart without a special retry path.

Adoption is forward-only. After the first deployed attempt-only record, a
release that cannot interpret it is unsupported. st2 adds no compatibility
path, downgrade reader, dual write, translation bridge, or fence.

## Evidence and Argument

The existing ledger's policy surface was three methods on `Profile`: grade,
prove, and release. Re-keying those methods from harness identity to a closed
policy preserves every Codex and OpenCode decision. Golden compact-JSON tests
pin the complete serialized bytes for both existing identities. A synthetic
identity-policy pairing drives the same ledger and proves the core grades by
policy without another provider branch.

Issue #506 records two additional experiments for later implementation steps. A
bounded two-message, two-writer model found duplicate permits in the current
split authorization boundary. Barrier-controlled pi and OMP assets emitted
success before hidden completion and could emit failure after a side effect.
Those findings justify attempt-only semantics and the later transactional
boundary; this decision does not claim those later steps are implemented.

## Consequences

- Codex and OpenCode keep their existing compact ledger bytes and decisions.
- Claude, pi, and OMP can adopt the same core without claiming nonexistent
  receipts.
- The policy set is compiler-closed and adds no runtime registry.
- Transactional mutation, exact attempt tokens, FIFO ownership, correlation,
  operator evidence, and the three remaining driver adoptions remain separate
  implementation changes under R43.
- `INVARIANTS.md` names Codex and OpenCode ownership until the later driver
  adoption proofs pass.

## Amendment 1 — 2026-09-08 (Q35)

Johannes approved one bounded exception to the no-compatibility consequence
above. R43 requires a fresh durable attempt token, but the canonical
`st2.delivery-ledger.v1` entry predates that field. Codex and OpenCode already
hold live durable evidence in this format. Rejecting those bytes would preserve
implementation purity by discarding the safety property this work exists to
provide.

A tokenless canonical Codex or OpenCode entry is therefore read only inside the
same locked transaction that owns every mutation. st2 derives a deterministic
128-bit token from the exact immutable entry bytes and persists the token before
any other mutation or transport. All later operations require the exact token.
The reader never dual-writes, never accepts an attempt-only legacy record, and
never makes a pre-token writer a supported rollback target.

This is temporary implementation state under T04. Its remaining inputs stay
countable, and a named deletion signal owns removal of the reader. T03 remains
unchanged for Claude, pi, and OMP: no tokenless attempt-only record has shipped,
so their adoption stays a clean forward-only boundary.
