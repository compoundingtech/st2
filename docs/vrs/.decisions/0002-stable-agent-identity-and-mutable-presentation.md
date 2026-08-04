# Stable agent identity is separate from mutable presentation

Status: draft

Requirements change authorized by Johannes on 2026-07-31.

Merge and acceptance approval required: Nathan

## Context

Agent Spec currently overloads one identity string as both an automation key
and the only human recognition surface. Operators cannot improve a label
without changing routing, durable paths, task identity, and process lifecycle.
An experimental sibling `name` file is a second source of truth and does not
compose with canonical Agent Spec authoring.

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
accepting a second authored representation. Within the trusted-fleet model,
caller-supplied `ST_AGENT` limits an invocation to itself or declared
descendants; this is an operational guardrail, not an authenticated capability,
and absence selects the operator path. Declarations explicitly marked Nix-owned
remain writable only at their Nix source, and Nix emitters must publish that
marker before authoring is activated. st2 serializes these
edits with the shared persistent `.st2/catalog-authoring.lock`, preserves
unrelated source bytes, detects stale source, and atomically replaces the
declaration. The lock covers cooperating local st2 writers in one POSIX
filesystem/kernel lock domain; it does not claim exclusion across independently
synchronized hosts or direct external writers.

Healthy runtime reconciliation uses the atomic exact-ID-only `pty metadata
patch --id <task-id>` operation. It projects name to native PTY `displayName`
and a versioned st2-owned tag snapshot containing stable actor identity plus
optional description. Name is not duplicated in tags. One real patch emits one
coherent `metadata_change` event; an unchanged patch emits none. Automation
never uses human display-name resolution. Presentation drift degrades and
retries without restart or lifecycle accounting.

st2 also publishes the same desired presentation as one fixed provider-neutral
derived state snapshot at `<agent-dir>/resources/presentation.json`. The
snapshot contains a versioned schema, host, stable unqualified identity, and
explicitly nullable name and description. It is intrinsic state rather than a
declared Resource binding and carries no provider, account, session, or
lifecycle data. Harness drivers own provider-native translation; the generic
projection grants them no lifecycle authority. Equal canonical bytes are a
no-op, while changed bytes are durably replaced through a synced
same-directory temporary and atomic rename. No catalog-wide generation is
embedded because unrelated declaration changes do not revise presentation.

## Consequences

- Human labels can improve without breaking routing or continuity.
- Duplicate or absent names are valid; stable IDs remain visible for exact
  disambiguation and automation.
- The old equality between stable identity and every presentation surface is
  superseded, but stable routing semantics are preserved.
- Existing declarations require no presentation compatibility marker because
  the new fields are optional and additive. Adoption begins only after
  compatible PTY and st2 binaries are deployed; Nix-generated declarations
  first add their ownership marker.
- This decision remains draft and is not accepted or mergeable until Nathan
  approves it.

## Evidence required for acceptance

- parser and roster tests across KDL, TOML, and JSON;
- source-preservation, authority, Nix refusal, and stale-writer tests;
- exact-ID PTY projection tests for set, clear, idempotence, and partial failure;
- exact presentation snapshot schema/path, nullable clearing, unchanged-byte
  no-op, atomic durable replacement, and state-plane exclusion tests;
- a live no-restart test preserving stable task ID, PID, creation identity, and
  generation across PTY and snapshot presentation changes;
- a genuine lifecycle-change control that still performs ordinary replacement.
