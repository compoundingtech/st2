# Candidate catalog rollout contract

This document is a design plan. It does not change a catalog or a running service.

## Outcome

A candidate binary must prove compatibility with the selected catalog before any host runs that binary.

Repository CI is necessary, but it is not this proof. Repository CI does not contain the private fleet catalog.

The gate applies to st2 upgrades, st3 upgrades, and an st2-to-st3 network replacement.

## Failure this contract prevents

A candidate can pass every repository check while rejecting most live declarations.

This happens when a schema rule lands without its catalog migration.

The runtime can sometimes keep supervising those declarations through a compatibility path. Validation still becomes red and graph data becomes incomplete.

That split is a degraded state. A rollout must not discover it after binary activation.

## Required order

The rollout uses this order:

1. Build the exact candidate revision.
2. Select one exact catalog snapshot.
3. Count the expected declaration corpus.
4. Validate that snapshot with the exact candidate for every declared host.
5. Apply and revalidate any required catalog migration.
6. Prove runtime prerequisites with the candidate.
7. Build and hash every platform artifact.
8. Deploy one host and hold it before the next host starts.

A binary deployment never precedes its required catalog migration.

## Candidate validation

The validation job runs outside the live catalog. It uses a read-only snapshot or a private replica.

The job records these values:

- candidate source revision;
- candidate binary digest;
- Agent Spec revision;
- catalog snapshot digest;
- declaration count;
- host count and exact host names;
- error and warning counts for each host;
- runtime prerequisite results.

The declaration count must match the selected snapshot inventory. A zero count cannot pass a nonempty fleet gate.

Every declared host gets a scoped validation run. Fleet-wide structural checks remain active in each run.

The candidate must report zero errors. A release policy can also require zero warnings.

## Schema migrations

A new required field is a catalog migration unless the specification defines a default.

The implementation must not invent a default to hide a missing migration.

Explicit readiness, ownership, security, and delivery fields remain explicit when their specification requires it.

The migration runs against the catalog snapshot first. The exact candidate then validates the migrated result.

Only the validated migration enters the live catalog.

If migration work will be discarded by an imminent network replacement, the rollout remains held.

## Runtime prerequisites

Catalog syntax is only one gate. The candidate must also prove its external runtime contract.

The initial prerequisite set includes:

- the required lifecycle hook set;
- the exact PTY capability and behavioral proof;
- native driver admission;
- Claude channel assets, marketplace, plugin, and policy when Claude is declared;
- every selected host spawn profile;
- every declared program on the constructed `PATH`.

A missing prerequisite blocks activation. It does not trigger a workload restart.

## CI and private fleet checks

Repository CI keeps representative catalog fixtures for every supported schema revision.

A separate private release gate runs the candidate against the current fleet snapshot.

The private gate publishes only counts, digests, revisions, and redacted issue codes.

It does not publish catalog contents, workspace paths, credentials, or agent prompts.

Both gates must pass. One gate cannot substitute for the other.

## Host rollout

The first host has the smallest recoverable workload set.

The rollout replaces only the orchestrator binary and service. It does not restart healthy members.

The hold checks these facts:

- the service runs the exact binary digest;
- existing member process identities remain unchanged;
- the supervisor adopts every expected local member;
- new launches remain possible;
- socket-owner and orphan counts do not increase;
- the bus and PTY remain usable;
- validation and graph completeness remain green.

The next host starts only after the hold receipt is complete.

## Fail-closed conditions

The gate stops before deployment when any condition is true:

- the catalog snapshot is absent or unreadable;
- the measured declaration corpus is unexpectedly empty;
- the candidate reports a validation error;
- a declared host has no scoped result;
- the Agent Spec revision changes without a migration decision;
- a runtime prerequisite is absent or unproved;
- an artifact digest does not match its selected revision;
- the first host hold changes a workload process identity unexpectedly.

The operator receives the exact failed condition and the measured corpus size.

## Rollback

Every host keeps the prior versioned binary until its hold passes.

Rollback restores the previous selector and service command. It does not restart healthy members.

A catalog migration has its own reversible commit. Binary rollback does not silently revert catalog data.

When the old binary cannot read the migrated schema, the rollout needs a forward-compatible migration before deployment.

## st3 network replacement

A new network still needs this gate. A rebuild does not make its declarations compatible by definition.

The st3 bootstrap catalog becomes the selected snapshot. Its expected declaration count is part of the receipt.

The exact st3 candidate validates, plans, and checks runtime prerequisites against that snapshot before activation.

The first live reconcile then verifies the same snapshot digest. It does not discover a different catalog during rollout.

## Done condition

The contract is implemented when one command produces a complete candidate-catalog receipt and fails closed on every condition above.

The receipt must be usable before any service or selector mutation occurs.
