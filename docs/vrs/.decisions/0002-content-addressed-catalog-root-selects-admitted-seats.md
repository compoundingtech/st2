# A content-addressed catalog root selects admitted Agent Spec seats

Status: proposed

## Context

The recursive `agent.kdl` catalog makes declaration source, discovery path, and
mutable agent state share one directory. That is convenient for authored files,
but it cannot atomically select a fleet assembled from independently managed
Agent Specs. It also makes a dynamic agent manager choose between rewriting
Nix-managed declarations and projecting mutable symlinks into a recursively
discovered tree.

The use case is a mixed catalog: static tooling may build and pin exact Agent
Spec bytes, while an agent manager may select or roll back seats at runtime.
Messages, context, status, and future state still need stable mutable paths when
the selected declaration object changes.

This proposal does not change canonical authored KDL, the trusted-private-fleet
assumption, or the current recursive catalog. It incubates a second,
experimental resolution path with executable evidence.

## Options

| Option | Tradeoffs |
| --- | --- |
| Rewrite or symlink `agent.kdl` projections | Reuses discovery, but exposes partial multi-seat updates and conflates immutable source with mutable state. |
| One mutable head per seat | Gives seat-local CAS, but readers cannot name or validate one atomic fleet snapshot. |
| Immutable seat admissions selected by one complete catalog root | Adds object types and a globally contended root CAS, but gives one atomic visibility boundary and preserves stable resource paths. |

## Proposed Decision

Store exact Agent Spec bytes and immutable commits below the hidden
`.st2/catalog-v1` namespace. A `SeatAdmission` joins one exact Agent Spec ref
commit to one exact resource-binding commit. A parent-linked
`CatalogRootCommit` maps every bus id to its admission. After validating the
complete prospective graph, publish one mutable root head atomically.

```text
exact KDL object <- ref commit ----\
                                    SeatAdmission <- CatalogRootCommit <- root
stable agent_dir <- binding commit /
```

`AgentSpec.path` is the immutable declaration source. `AgentSpec.agent_dir` is
the stable mutable state root used by messages, context, status, and runtime
state. Resolution does not create an `agent.kdl` projection.

Static and dynamic managers use the same protocol:

1. `prepare` imports exact bytes without changing selection.
2. `stage` publishes a ref commit and resource binding without changing the
   selected root.
3. `admit` atomically selects one or many staged seats in a complete root.

Manager fencing prevents a different manager from advancing an owned ref or
admitting ref/binding commits it does not own. The root's `manager` records the
transaction actor; it does not grant whole-root custody. A manager may CAS from
another manager's current root while preserving untouched foreign admissions
byte-identically. Operation ids make an acknowledged ref/root update replayable
after response loss. Rollback is a new parent-linked commit, not a head rewind.
Manager names are logical coordination labels under the trusted same-user
assumption; they are not authentication or authorization.

## Validation Boundary

Before moving the root head, st2 resolves every admission in the prospective
root and verifies:

- every digest in the selected reachable graph and every referenced object;
- bus-id, host, identity, manager, and schema joins;
- exactly one explicit-host, explicit-identity declaration per object;
- active declarations lower to runnable Agent Specs; and
- every resource state path is a normal catalog-relative path;
- no state root is under reserved `.st2`, crosses an existing symlink
  component, or is shared by two selected seats.

Readers observe the old or new complete root across the atomic head rename.
Test-scoped failpoints prove process-level visibility around that boundary and
operation replay after response loss. They do not prove power-loss durability
for every filesystem, mount, kernel, or storage device.

## Consequences and Limits

- Nix can manage immutable source objects while an agent manager owns only
  admission, without requiring a private projection directory.
- Mutable messages, context, and status remain ordinary files at stable
  `agent_dir` paths; they are not content addressed.
- State roots are seat-exclusive, catalog-relative, outside `.st2`, and may not
  traverse an existing symlink component.
- Root publication serializes writers and copies the complete admission map.
  Scaling, sharding, and compaction require evidence before changing this.
- Source-relative `render copy` inputs need a future immutable resource-bundle
  contract. Inline render content works now; silently reading mutable files
  adjacent to an object would weaken reproducibility.
- `prepare` content-addresses exact KDL bytes, not a self-contained closure.
  Workspaces, templates, hooks, and other referenced inputs are not captured.
- "Immutable" means content-addressed protocol publication: st2 refuses
  replacement and verifies selected bytes under trusted same-user store
  custody. Verification and later use are not sealed into one file descriptor;
  verified-FD use or stronger filesystem sealing remains future hardening.
- Validation covers the selected reachable graph. Parent links record lineage
  and replay identity, but ancestor history is not recursively audited.
- Discovery integration, GC, replication, replacement semantics, daemon
  sockets, typed resource contracts, and an authorization framework are
  explicitly outside this proposal.

Acceptance requires the experiment record to remain green and a separate human
decision. Until then, the JSON CLI and on-disk schema are experimental.
