# st3 data authority

This document classifies each SQLite table in schema version 9.

Schema version 9 does not migrate an earlier database. Start the renamed system with a new state directory.

The claim log and immutable blobs are the durable graph authority.

All other tables are indexes, projections, local capabilities, or transport recovery state.

| Table | Class | Rebuild or recovery source |
|---|---|---|
| `meta` | Local store metadata | Store initialization and configured node identity |
| `batches` | Claim-log authority | Accepted local and replicated batch headers |
| `claims` | Claim-log authority | Accepted local and replicated claims |
| `blobs` | Content authority | Posted bytes, verified by SHA-256 |
| `operations` | Projection | Claim `_operation` metadata |
| `documents` | Projection | `doc.bound` claims and blobs |
| `desired` | Projection | Selected `intent.desired` heads |
| `events` | Projection | Effective accepted claims |
| `mission_revisions` | Projection | `mission.published` claims |
| `mission_definitions` | Projection | Selected `mission.published` heads |
| `mission_runs` | Projection | `mission-run.*` claims |
| `run_generations` | Projection | `run-generation.*` claims |
| `step_runs` | Projection | `step-run.*` and `work.*` claims |
| `revision_proposals` | Projection | `revision-proposal.*` claims |
| `planning_sessions` | Projection | `planning-session.*` claims |
| `planning_candidates` | Projection | `planning-session.candidate-submitted` claims |
| `planning_previews` | Projection | `planning-session.previewed` claims |
| `idempotency` | Opaque response cache | Hashed caller keys and derived responses |
| `mission_run_requests` | Opaque validation cache | Hashed caller keys and request digests |
| `capabilities` | Local short-lived authority | Dedicated API issuance; capabilities do not replicate |
| `peer_cursors` | Transport recovery state | Successful exchanges with a configured peer |
| `peer_replica_cursors` | Transport recovery state | Accepted per-origin replica sequences |

The store rebuilds operation and planning projections when it opens.

Replica import rebuilds planning projections after it accepts a complete claim batch.

`st3 doctor` compares the operation projection with the claim log.

An idempotent claim stores a keyed hash of the caller key in its `_operation` metadata.

`st3 claim --idempotency-key KEY` opts a public claim into this contract.

The claim stores the canonical request digest and canonical claim ID.

An exact retry returns the original claim.

A retry with different input returns `idempotency-mismatch`.

Conflicting replicated operations mark the affected graph subject as indeterminate.

The database does not store the caller key.

Eval cleanup removes every desired projection row owned by the terminal eval run.

Eval history stays in the immutable claim log.

A cleanup residue produces `eval.verdict` with `verdict=fail`.

A cleanup infrastructure error produces `eval.verdict` with `verdict=void`.
