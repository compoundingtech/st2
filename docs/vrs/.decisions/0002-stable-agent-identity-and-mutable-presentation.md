# Stable agent identity is separate from mutable presentation

Status: draft

Approval required: Nathan

## Context

Agent Spec currently overloads one identity string as both an automation key
and the only human recognition surface. Operators cannot improve a label
without changing routing, durable paths, task identity, and process lifecycle.
An experimental sibling `name` file introduced a second source of truth and did
not compose with transactional Agent Spec publication.

The fleet requires a stable automation identity and presentation that can
change while the running process, PTY generation, bus, and durable state remain
continuous. The change must not introduce a stable-ID alias, dual parser, or
long-lived migration branch.

## Decision

The existing positional Agent Spec identity remains the sole stable automation
ID. Its grammar and the established `identity` JSON/TOML/roster spelling remain
unchanged. There is no stable-ID rename operation.

Agent Spec adds direct optional `name` and `description` fields. They are
non-authoritative, non-unique presentation. Omission means absence. The Agent
Spec declaration is their sole source of truth; a sibling `name` file is neither
read nor written.

Constrained KDL-only commands may mutate one presentation field without
publishing a second representation. Catalog agents may edit themselves or
declared descendants; operators may use the same constrained path. Nix-owned
declarations remain writable only at their Nix source. st2 serializes these
edits with its catalog lock, preserves unrelated source bytes, detects stale
source, and atomically replaces the declaration.

Healthy runtime reconciliation uses the atomic exact-ID-only `pty metadata
patch --id <stable-id>` operation. It projects name to native PTY `displayName`
and a versioned st2-owned tag snapshot containing stable actor identity plus
optional description. Name is not duplicated in tags. One real patch emits one
coherent `metadata_change` event; an unchanged patch emits none. Automation
never uses human display-name resolution. Presentation drift degrades and
retries without restart or lifecycle accounting.

## Consequences

- Human labels can improve without breaking routing or continuity.
- Duplicate or absent names are valid; stable IDs remain visible for exact
  disambiguation and automation.
- The old equality between stable identity and every presentation surface is
  superseded, but stable routing semantics are preserved.
- Existing declarations require no compatibility marker because the new fields
  are optional and additive. Adoption begins only after compatible PTY and st2
  binaries are deployed.
- This decision is not accepted until Nathan approves the protected requirement
  changes.

## Evidence required for acceptance

- parser and roster tests across KDL, TOML, and JSON;
- source-preservation, authority, Nix refusal, and stale-writer tests;
- exact-ID PTY projection tests for set, clear, idempotence, and partial failure;
- a live no-restart test preserving stable task ID, PID, creation identity, and
  generation across presentation changes;
- a genuine lifecycle-change control that still performs ordinary replacement.
