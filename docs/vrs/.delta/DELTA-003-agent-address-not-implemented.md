# DELTA-003: immutable subject ID and mutable address are not implemented

Status: open

## Divergence

[Decision 0015](../.decisions/0015-immutable-agent-id-and-mutable-address.md)
and root requirements R19 and R24-R26 define the accepted target identity
model. The implementation still uses positional `identity` plus current host as
logical subject ID, human route, ownership key, task prefix, state selector, and
`ST_AGENT`. `AgentSpec` has no explicit `id` or `address`; ordinary resolution,
roster output, graph output, supervisor edges, messages, authoring, and PTY
metadata all retain the pre-decision behavior.

## VRS

The target has an explicit catalog-global immutable agent ID, an optional
host-local mutable agent address, a derived bus address, and separate non-unique
presentation. Migration assigns each legacy subject its existing host-qualified
bus identity as an explicit ID without moving state; new subjects use UUIDv7.
Positional `identity` remains the legacy address fallback until an explicit
address is assigned. Address assignment is an immediate route cutover with no
alias or history. Exact ID selection is explicit and ordinary references use
the fail-closed bare-or-qualified address algorithm.

## Implementation

No runtime code changes are part of the VRS pull request that opens this delta.
The implementation must begin with tests at the Agent Spec and address-book
boundaries, then propagate one typed ID/address distinction through catalog
validation and legacy explicit-ID migration, authoring, graph and roster
projections, message provenance and readable sender projection, supervisor
edges, `ST_AGENT`, runtime ownership, task inventory, PTY schema-2 metadata,
state paths, driver arguments, DING, and downstream evals/generators.

The migration must add each legacy explicit ID before ID-aware routing activates
and must not move existing declarations or state. Until the complete
compatibility path is implemented and proven, existing identity resolution and
every current invariant remain normative implementation behavior. Do not
partially activate ID, address, `ST_AGENT`, PTY metadata, or message changes
against a mixed resolver.

## Direction

update implementation

## Resolution Signal

Close this delta only after the canonical Agent Spec requires `id` and accepts
`address`, all legacy subjects have explicit frozen IDs, st2 and all supported
downstream generators emit and consume the new model, and the model-free proof
corpus passes against an immutable st2 artifact. The proof must cover UUIDv7
creation, legacy bus-ID migration, global uniqueness, address fallback and
grammar, dotted bare/qualified disambiguation, host-local address uniqueness,
explicit ID selection, immediate address cutover and reuse, retired-address
release, transactional concurrency, authority and Nix refusal, nondisruptive
live continuity, readable message/DING sender projection, host/graph/launch
lifecycle controls, and every public machine wire shape named by decision 0015.
The implementation must update the `Runner-owned task identity` invariant and
its named tests from host-qualified `ST_AGENT` to the raw immutable ID, and must
update the PTY actor-tag invariant atomically from schema 1
`agent.actor.path` to schema 2 `agent.actor.id` plus
`agent.actor.address`. Record the exact release and eval artifact here when
closing.
