# `st2 resource` is a mediated binding write surface

Status: accepted

Design decision made by Johannes on 2026-08-27 (interview; answers
[#231](https://github.com/compoundingtech/st2/issues/231)). Merge and acceptance
approval required: upstream maintainers.

## Context

Changing one declared binding requires the caller to render a complete candidate
`agent.kdl`, validate it, compute its digest, publish under compare-and-swap, and
read it back. That ceremony is safe and disproportionate.

It also has a measurable consequence. Of 241 link records on one live catalog, 45
were not products at all but dependency and reference edges — `current-work`
(which duplicates the `work` binding), `depends-on-slice-*`, `supervises`,
`blocked-design`. Agents wrote them into the linked-record plane because that was
the plane they were permitted to write cheaply. **The drift between the two
planes was caused by write-cost asymmetry, not by two different relations.**

Retiring the linked-record plane (decision 0011) removes the escape hatch. It
does not remove the pressure that produced it.

## Decision

`st2 resource` gains mediated write verbs alongside `ls` and `read`:

```text
st2 resource add <name> --uri <uri> --reason <text>
st2 resource remove <name>
st2 resource rename <old> <new>
```

Each performs read-modify-CAS-publish internally. The caller never renders KDL.
Full-catalog validation, exact-target selection, compare-and-swap, and
fail-closed concurrent-change behavior are preserved, and a binding-only change
does not stop, replace, or relaunch healthy work (R21).

This is the fourth instance of an existing pattern, not new machinery:
`src/agent_author.rs` already mediates `add_stream`/`remove_stream`
(`st2 agent stream`), `set_desired_state` (`st2 agent desired-state`), and
`set_presentation` (`st2 rename` / `st2 describe`).

## Consequences

- The reason agents reached for the cheap plane is removed, not just the plane.
- Declaring the working-state carrier (decision 0012) across a fleet becomes one
  command per declaration instead of a rendered-and-published candidate each.
- URI possession still grants nothing. A mediated write changes a declaration;
  it does not touch the thing the URI names, and confers no access to it
  ([#61](https://github.com/compoundingtech/st2/issues/61)).
- Authoring authority is unchanged: whoever may publish the declaration may
  mutate its bindings, and no one else.

## Limits

This does not address a declaration generated read-only by configuration
management, where the next activation overwrites a runtime edit
([#305](https://github.com/compoundingtech/st2/issues/305)). On the catalog
measured, declarations are writable regular files carrying
`meta { managed-by "agent-spec-authoring" }` with no `/nix/store` symlinks, so
the mediated write applies there. The generated-declaration case stays open in
[DQ-R4](../07-resource/open-questions.md).

## Options

| Option | Result | Reason |
| --- | --- | --- |
| Read plus mediated `add`/`remove`/`rename` | Selected | Removes the cause of the drift, not just its symptom. The machinery is the fourth instance of an existing pattern in `src/agent_author.rs`, so the marginal risk is small. |
| Read-only `ls`/`read` | Rejected | Smallest change and a literal reading of #61's read-oriented boundary, but it leaves the write-cost asymmetry intact while decision 0011 removes the escape hatch — pressure with nowhere to go. |
| Ship read-only now, add writes as a follow-up | Rejected | Sequences a breaking rename away from a new write path, which is genuinely safer, but leaves the same interval in which agents have an expensive plane and no cheap one. |

## Evidence and Argument

The link-record census supplies the causal evidence. Of 241 records, 196 are
products — the plane's stated purpose. The remaining 45 are not: `supervises`
(11), `reference` (7), `current-work`, `depends-on-slice-1`,
`depends-on-slice-2`, `depends-on-slice-5`, `depends-on-slices-1-4`,
`blocked-design`. `current-work` duplicates the `work` binding outright, and the
`depends-on-slice-*` records are dependency edges filed in a products store.

Agents did not confuse the two planes. They wrote dependency edges into the
products plane because a binding required publisher authority and
whole-declaration republication under compare-and-swap, while a linked record
required one file write. The observed misfiling is what write-cost asymmetry
looks like from the inside.

That the machinery already exists is the second half of the argument.
[`src/agent_author.rs`](../../../src/agent_author.rs) implements exactly this
read-modify-CAS-publish shape three times — `add_stream`/`remove_stream` behind
`st2 agent stream`, `set_desired_state` behind `st2 agent desired-state`, and
`set_presentation` behind `st2 rename` and `st2 describe`. Bindings are the
fourth field of the same declaration, mutated by the same protocol; this is a new
caller of proven machinery rather than a new mechanism.

[#231](https://github.com/compoundingtech/st2/issues/231) asked for precisely
this and listed the properties it must keep — full-catalog validation, exact
target selection, CAS, fail-closed on concurrent change, non-disruption of
healthy work, and machine-readable publication evidence. All are properties the
existing three callers already have.
