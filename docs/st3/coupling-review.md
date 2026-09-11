# st3 coupling review

This review follows the replication recovery work. It examines failure, data, policy, and process boundaries.

## Replication

Replication is optional. A node with no fleet settings remains local and does not start a replication listener.

The authenticated replication worker is a separate supervised process. It waits for the main daemon before it opens the shared store.

The worker receives signed envelopes, validates records, projects valid claims, and reports health. A worker crash cannot stop the main daemon or its runtimes.

The worker and daemon still share one SQLite file. Short transactions and a busy timeout limit lock contention, but the file remains a deliberate coupling point.

The worker wakes from one dedicated file. It does not watch the database or state directory, so its own writes cannot create a hot loop.

The worker contacts peers only through loopback HTTP. Fabric or another approved loopback exposer owns transport between hosts.

## Authority and projections

Envelope receipt commits before payload validation. Validation commits before projection. Invalid records cannot roll back valid receipt or later records.

Projection uses only admitted claims. A projection failure keeps the last good graph and marks projection health as stale.

All graph projections still share one projection transaction. One projector defect can delay new projections, but it cannot remove authority or stop replication receipt.

This aggregate transaction is the main remaining data coupling. Split it only when measured failures show a need for per-aggregate progress.

## Runtime and observation

The reconciler uses a runtime-control interface. Runtime process behavior is not coupled directly to one harness implementation.

Observers run as bounded asynchronous tasks, but the reconciler still schedules them. A separate observer process is a useful future boundary if provider faults affect daemon reliability.

The reconciler reads the concrete store directly. A narrow read-and-claim interface would make independent reconciler testing easier, but it is not a current failure boundary.

## API and storage modules

The API module includes transport and several product workflows. The store module includes authority, projections, missions, and planning.

These modules are large, but size alone is not a reason to split them. Future splits should follow a measured failure boundary or an independent protocol boundary.

## Decision

The replication process split is required and complete. No other coupling change is required for this recovery.

The next coupling review should use live fault and latency evidence. It should examine the shared SQLite file, observer scheduling, and the aggregate projection transaction first.
