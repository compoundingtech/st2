# Typed agent desired state separates reversible suspension from retirement

Status: accepted

Requirements change authorized by Johannes on 2026-08-04.

Merge and acceptance approval required: upstream maintainers

## Context

Agent Spec has one irreversible-looking `retired` boolean. Operators sometimes
need to keep an agent discoverable and addressable while intentionally running
none of its tasks. Removing or commenting out its declaration loses catalog
intent; overloading `keep`, task `lifecycle`, presence, or runtime pause would
conflate independent contracts. A bare boolean also cannot explain why an
agent is unavailable.

## Decision

Agent Spec uses one typed whole-agent desired state: running, suspended, or
retired. Running is the omitted default. Suspended and new retired declarations
carry a bounded human rationale. The parser retains legacy `retired #true` as
retired without a rationale and rejects every declaration that combines old
and new lifecycle syntax.

Suspension and retirement both desire no live owned tasks, including generated
companions. Suspension remains reversible, preserves catalog-backed durable
state, and permits dead records only when existing `keep` policy explicitly
pins them. Retirement keeps the established stronger completion rule: every
declared task record is absent. Returning to running uses ordinary
reconciliation and does not override `keep`, `adopt-only`, drift, or ownership
proof.

The KDL-only authoring command edits one exact declaration through the existing
catalog-authoring transaction. Running removes lifecycle syntax; the other
states use `desired-state "..." reason="..."`. The receipt proves only that
the declaration was durably authored. Roster, inventory, human listing, and
Doctor expose desired intent separately from presence and observed liveness.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Keep `retired` plus an implicit suspension field | Rejected | Leaves invalid combinations and no closed lifecycle model. |
| Add independent `suspended` and `retired` booleans | Rejected | Creates four source combinations for three states and makes precedence part of every consumer. |
| Use one closed desired-state enum with a rationale and legacy retirement read path | Selected | Makes illegal combinations unrepresentable in the normalized model and preserves compatibility. |

## Evidence and Argument

The independent bounded oracle passed all 9 validity and 36 planning cases.
The real-path prototype then proved the selected model through parsing,
source-preserving authoring, reconciliation, PTY and exec backends, a generated
DING, message retention, task inventory, roster, and cleanup. It exposed one
necessary distinction: suspension converges at no-live plus only keep-pinned
dead records, while retirement converges only at total record absence. The same
run showed that resume needs no new runtime mechanism; ordinary reconcile
already preserves keep and adopt-only semantics.

## Consequences

- One enum owns lifecycle intent; suspension does not introduce another runtime
  state machine.
- Existing declarations continue to parse, and legacy retirement stays visible
  through the compatibility `retired` projection.
- DING stops while suspended, but inbox and other durable state remain
  addressable and resume unchanged.
- Consumers that need the rationale use the new desired-state fields rather
  than presence or the legacy retirement projection.
- Nix-owned declarations remain editable only through their Nix source.

## Evidence required for acceptance

- exhaustive state/reason parser controls and legacy compatibility;
- reconcile matrices for alive, absent, dead, keep, adopt-only, derived, remote,
  resume, and retirement controls;
- source-preserving CLI, authority, stale-writer, malformed, and Nix refusal
  tests;
- Doctor and machine-wire tests distinguishing suspension from retirement;
- a live PTY plus exec companion suspend/resume run that preserves a sibling
  generation and an inbox message.
