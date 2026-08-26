# Numbered subsystem under st2's VRS root, referencing dotfiles' central tree

Status: accepted

Recorded 2026-08-25 from the aligned observability interview (axe decision catalog Q2).

## Context

Observability semantics could live in three places:

1. A numbered subsystem directory under st2's existing `docs/vrs` root, alongside
   [01-ding](../../01-ding/) through [05-harness-state](../../05-harness-state/).
2. A standalone documentation root inside st2, separate from the VRS hierarchy.
3. Only in the fleet-wide dotfiles context `observability` tree, with st2 documenting nothing of
   its own.

Fleet-wide naming/provenance rules and producer obligations genuinely belong to dotfiles'
central tree (`01-conventions`, `09-integration/spec.md`). But st2-specific decisions — crate
feature set, trace roots in `src/run.rs`, unit env propagation — have no home there and would be
invisible to anyone working in this repo.

## Evidence and Argument

The repository already uses numbered VRS subsystems for product-specific lifecycle contracts,
while the dotfiles observability tree owns fleet-wide semantics. A local subsystem with explicit
central references preserves both contributor locality and one authority per shared rule.

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Numbered st2 subsystem | Selected | Keeps local mechanisms and evidence beside their code while referencing central rules. |
| Standalone st2 documentation root | Rejected | Duplicates the established VRS hierarchy. |
| Central dotfiles tree only | Rejected | Leaves st2-specific crate, process, and CI decisions without repository-local authority. |

## Decision

This tree is a **numbered subsystem under st2's existing `docs/vrs` root**: `06-observability`
(Q2). It follows the established subsystem shape — requirements/spec/open-questions plus `.decisions/`
and `.experiments/`.

Fleet-wide semantics stay owned centrally by dotfiles context `observability` (`01-conventions`
for naming/provenance/span-label rules; `09-integration/spec.md` for the six producer
obligations). This tree **references them and does not duplicate them**: it defines only what is
st2-side — registered span/metric names, concrete `service.name` values, the crate stack, and the
CI proof strategy. Central obligations that remain cross-repo work (registry entry, dashboard,
census subject) are named explicitly as deferred in O11Y-R08 rather than restated here.

## Consequences

- st2 contributors find their telemetry contract next to the code it describes, in the same
  conventions as every other subsystem.
- One authority per rule: central rules are cited, never copied; drift between fleet convention
  and st2 usage shows up as a broken reference, not divergent prose.
- Renumbering risk exists if another subsystem claims 06 first; the number carries no meaning
  beyond ordering, so a rename is mechanical.
