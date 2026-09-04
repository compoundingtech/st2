# st3 data authority review

Status: required design review before the first fleet cutover.

## Goal

The st3 database stores each authoritative fact once.

A repeated value is valid only when it is a declared projection that the authority can rebuild exactly.

## Classification rule

Each stored field has one class:

- Authority stores the fact and accepts its mutation.
- Reference names one authority record or content hash.
- Derived projection supports a query and accepts writes only from its reducer.
- Local cursor records progress against an external or replicated sequence.
- Short-lived capability controls one bounded operation.

A table name does not select the class. The review classifies each field and each write path.

## Checkable acceptance

The review is complete when all these checks pass:

1. A checked-in manifest classifies every database column.
2. Every projection field names its authority fields and one deterministic reducer.
3. A test copies only authority rows into a clean database and rebuilds every durable projection.
4. The test compares the original and rebuilt public snapshots at the same claim index.
5. An API test proves that no public operation writes a projection without its authority record.
6. A drift test changes one projection row and proves that `doctor` reports the exact mismatch.
7. Replication sends authority records and blobs. It does not replicate derived projection rows.

A projection is a second authority when any required check cannot pass. The design must then remove the duplicate or select one authority.

## Worked st2 warning

The st2 message sender ledger repeats the message payload, sender, and recipient after delivery.

The recipient mailbox already stores those values. The timestamp in each mailbox filename already supports display order.

The sender ledger also adds a hash chain to order completion timestamps. This extra structure does not create a new user fact.

The result is two durable authorities for one delivered message, plus a second ordering mechanism.

This example does not authorize a current st2 rewrite.

## Current st3 risk areas

The following areas need the field classification and rebuild proof:

- `claims` and `events` repeat the claim kind, subject, index, and body.
- `claims`, `desired`, `plan_revisions`, and `plan_definitions` repeat selected intent and plan data.
- `claims`, `plan_runs`, `run_generations`, `step_runs`, and `revision_proposals` repeat run state.
- `claims`, `planning_sessions`, `planning_candidates`, and `planning_previews` repeat planning state and document references.
- `idempotency.response` can become a second result authority when an endpoint returns its stored response.
- `documents` must remain a name-to-hash reference. Only `blobs` stores document bytes.
- `batches` and `claims` repeat origin and acceptance data. The review must name which fields establish causality.
- `peer_cursors` and `peer_replica_cursors` are local progress state. They must not become replicated graph facts.
- `capabilities` are local, short-lived authority. Claims can record their results but cannot reactivate them.
- Resource observation claims own normalized external facts. Provider cursors and observer health cannot replace those facts.
- Runtime files and runtime observation claims can disagree. The design must name the observation authority at each lifecycle boundary.

The existing projection code is not proof by itself. The rebuild test and drift check make the rule operational.
