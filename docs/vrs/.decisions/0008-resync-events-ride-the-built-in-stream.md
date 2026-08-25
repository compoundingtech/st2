# Resync events ride the built-in stream with digest identity

Status: accepted

Design decisions made by Johannes on 2026-08-25 (decision requests Q1–Q6,
recorded in the ox-alpha.local decision tree), grounding
[issue #341](https://github.com/compoundingtech/st2/issues/341).

## Context

A live agent's context is assembled from declared resource carriers. When a
carrier changes on disk after launch — configuration management replaced it,
a supervisor republished it — the session keeps acting on the stale version
until restart. Issue #341 proposes resync events: a supervisor notification
into the owning agent's inbox, delivered like any other message.

The delivery surface was already settled once: decision
[0004](./0004-stream-events-are-a-distinct-record-kind.md) made stream events
the second record kind, and `st2 event emit` with ring deduplication and
producer-side supersession shipped. The open question was how resync events
reach that machinery without a third mechanism, what they watch, how noisy
they are allowed to be, and what their identity is.

## Decision

1. **Delivery (Q1):** a built-in per-agent stream named `resync` exists
   without declaration; the supervisor emits through the unchanged ingress.
   One carve-out in stream resolution accepts the reserved name for running
   agents; declaring a user stream named `resync` is refused. No third
   record kind; no declaration boilerplate; default-on for every running
   agent.
2. **Watch scope (Q2):** watchable carriers are local files only — absolute
   `file://` URIs and catalog-relative paths resolved against the agent
   directory. Other schemes are silently unwatched with observability in the
   catalog projection. The agent's own declaration file is also a source.
3. **Noise model (Q3):** fixed class defaults in code for v1 — immediate for
   the declaration and goal carriers, silent for agent-authored stores,
   coalesced for everything else. The per-binding `notify` attribute waits
   for real volume data; this keeps the change out of the canonical Agent
   Spec grammar and #305's Nix-regeneration coupling.
4. **Event identity (Q5):** event-id = SHA-256 of the new carrier bytes,
   key = binding label, every emit supersedes. Equal-content rewrites
   deduplicate to no wake; A→B→A oscillations notify honestly each time the
   content actually differs.
5. **Build approach (Q6):** direct test-first implementation; the PR's
   integration tests are the prototype evidence.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Built-in reserved stream on the existing event transport | Selected | Reuses implemented dedup/supersede/DING semantics; zero declaration cost; one small resolution carve-out. |
| Explicit declared `stream "resync" {}` per consumer | Rejected | Opt-in safety property silently missing when a spec forgets it; Nix-generated specs need regeneration (#305). |
| Third record kind for resync | Rejected | 0004 settled two kinds on one transport precisely to avoid a third mechanism to publish, list, deliver, and test. |
| mtime-serial event identity | Rejected | Wakes on no-op saves; mtime can move backwards on restore. Digests give strictly better noise behavior for carriers. |
| Whole-set digest across all carriers | Rejected | Cross-binding churn on multi-writer catalogs; per-binding supersession already bounds unread heads. |

## Evidence and Argument

Source reads of this branch on 2026-08-25 established: the event ingress is
implemented (`src/event.rs`: ring capacity 128, receipt validation,
supersession); `resolve_stream` requires declaration plus running desired
state, so the built-in name needs exactly one carve-out; the catalog watcher
(`src/watch.rs`) deliberately prunes payload trees (#333/#335) so carrier
watching must be explicit per-file, and its directory-identity tracking is
the proven pattern that makes rename-replacement visible via parent-directory
watches. Content-derived identity is sound here where it failed for world
events because equal bytes genuinely mean equal state for an on-disk carrier
(the unsoundness demonstrated in `04-stream/.experiments/` does not
transfer). Q2–Q6 were decided by Johannes over these grounded options; Q1
options were all grounded in the same reads except where marked otherwise.

## Consequences

- DING, inbox, archive, and stream-state code gain no new semantics; the
  only core-code deltas are the reserved-name carve-out, the watcher, and
  classification.
- The `resync` stream name becomes reserved grammar; validation must refuse
  shadowing from day one.
- Window lengths and class membership ship provisional and are expected to
  be amended after live observation (RESYNC-T02, DQ-R1).
