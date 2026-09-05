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
presentation. Migration freezes every live legacy subject's existing
host-qualified bus identity without moving declarations or declaration-parent
state. A structurally archived subject freezes the same bytes when they remain
unique across the combined live-and-archived set; an archived collision receives
UUIDv7 in its declaration and tombstone. Migration also records every
reassigned legacy bus identity with the subject that kept it and the archived
subject's generated ID, so a tolerant reader never retypes colliding
version-1 bytes into the wrong subject. Supervisor
resolution uses the combined pre-migration live-and-archived subject index; the
same migration rewrites every reference to the parent's migrated ID. A missing
or ambiguous reference refuses before writes with
`legacy-supervisor-unresolved`; the operator must unarchive and repair that
declaration through the pre-activation legacy authoring path, then retry.
Positional `identity` remains the legacy address fallback until an explicit
address is assigned. Address assignment is an immediate route cutover with no
alias or history. Exact ID selection is explicit and ordinary references use
the fail-closed bare-or-qualified address algorithm.

Agent endpoints persist an immutable ID plus a publication-time address
snapshot. Principal and external endpoints persist an explicit endpoint kind
and canonical typed address instead of pretending that address is an agent ID.
New durable message records use version 2 because strict version-1 readers
reject the new endpoint and snapshot fields.

## Implementation

The typed ID/address distinction is implemented in st2 and covered by model-free
tests. `agent-spec` owns `AgentId`, `AgentAddress`, UUIDv7 creation, frozen
legacy IDs, `Subject`, `AgentSelector`, and the fail-closed `AddressBook`;
`AgentSpec` carries explicit `id` and optional `address` and exposes exactly
three typed accessors — `agent_id` (ownership), `bus_address` (route), and
`legacy_bus_identity` (positional declaration key). An unmigrated declaration's
`agent_id` is by construction the value migration freezes, which is why no
declaration-anchored state, task ID, or socket path moves.
`AgentSpec::bus_id` is deleted, so every former call site had to choose a
meaning at compile time.

Landed with it: `st2 catalog migrate-ids` as one additive, idempotent catalog
transaction with durable collision metadata and `legacy-supervisor-unresolved`;
ID-validating unarchive; `st2 agent address --id`; ID-only identity authoring
for `rename`, `describe`, and `desired-state`; disjoint address/exact-ID inputs
on every agent-selecting command; raw-ID `ST_AGENT`; `<agent-id>.<task-name>`
default task IDs; PTY schema-2 owned metadata; ID-keyed graph, roster, task
inventory, supervisor edges, resource observation, stream ingress, and resync
ownership; typed message endpoints with collision-aware version-1 attribution;
and tolerant readers for message versions 1-2, harness-state 1-2, and
harness-context 1-2.

What remains before this delta closes: the canonical Agent Spec and the
downstream evals/generators still predate `id`/`address`; no real catalog has
been migrated; and the target writers stay behind their switches
(`message::WRITE_MESSAGE_RECORD_VERSION_2` and the harness-record version-2
writer gate) pending steps 2-5 below. Reader-first is therefore satisfied while
activation is not.

The propagation surface this covered:

- live and archived catalog validation, explicit-ID migration, unarchive, and
  ID-keyed supervisor references;
- every agent-selecting CLI, generated hook, channel adapter, driver argument,
  ambient `ST_AGENT` consumer, authoring command, graph, roster, and Doctor
  projection;
- runtime ownership, default task IDs, task inventory, socket admission, PTY
  schema-2 metadata, and launch metadata while keeping declaration-parent state
  and Resource paths stable;
- ordinary messages, replies, version-2 Sent records, typed non-Agent endpoints,
  DING sender projection, stream ingress and ownership, resync subscriptions,
  harness-state, and harness-context records; and
- all supported downstream evals and generators.

Activation is a reader-first transition, not a one-version flag day:

1. Deploy readers that accept legacy and target Agent Specs, message versions 1
   and 2, PTY schemas 1 and 2, harness-state and harness-context schemas 1 and
   2, and old and new projections. Keep every writer on legacy output.
2. Prove reader readiness on every admitted host and supported downstream
   consumer. An unreadable or unknown reader is not ready.
3. In one catalog transaction, add migrated unique IDs to live and structurally
   archived declarations, update archived tombstones, and rewrite every
   supervisor reference to its already-resolved migrated ID.
4. Re-prove reader readiness immediately before enabling target writers.
5. Activate UUIDv7 creation, mutable-address routing, raw-ID `ST_AGENT`, ID-keyed
   runtime ownership, message version 2, harness-state and harness-context
   version 2, and PTY schema 2 together.

No timeout substitutes for readiness. Until step 5 completes, existing identity
resolution and every current invariant remain normative implementation behavior.
After activation, an unmigrated archived declaration cannot re-enter the
catalog; unarchive validates ID uniqueness, and a transition from retired to
routable validates full-catalog address uniqueness.

## Direction

update implementation

## Resolution Signal

Close this delta only after the canonical Agent Spec requires `id` and accepts
`address`, all live and archived legacy subjects have explicit migrated IDs and
ID-keyed supervisor references, st2 and all supported downstream generators emit
and consume the new model, and the model-free proof corpus passes against an
immutable st2 artifact. The proof must cover UUIDv7 creation, legacy bus-ID
migration, global uniqueness, address fallback and grammar, dotted
bare/qualified disambiguation including host-pinned qualified input, host-local
address uniqueness, explicit ID selection, immediate address cutover and reuse,
retired-address release and reactivation validation, transactional concurrency,
authority and Nix refusal, nondisruptive live continuity, readable
message/DING sender projection and cosmetic fallback, version-1 record
compatibility including collision-aware legacy attribution, harness-state and
harness-context version-2 records read by tolerant readers, typed non-Agent
endpoints, host/graph/launch lifecycle controls, and every public machine wire
shape named by decision 0015.

Update these load-bearing invariant rows and their named tests in the same
implementation:

- `Runner-owned task identity`, including its PTY subject-tag clause, from
  host-qualified `ST_AGENT` and schema-1 `agent.actor.path` to raw immutable ID
  and schema-2 `agent.subject.id` plus `agent.subject.address`, leaving the
  external-actor tag `agent.actor.id` untouched;
- `Stable roster JSON` for appended immutable ID, nullable current bus address,
  presentation, and migrated supervisor projection;
- `R23 fail-closed diagnostic inventory` for ID-keyed ownership and nullable
  address without weakening completeness;
- `Archival leaves the live catalog` for frozen archived IDs and safe unarchive;
  and
- `Unbindable session sockets fail at admission` for the new default task-ID
  shape and preserved legacy socket continuity.

Record the exact release, rollout evidence, and eval artifact here when closing.
