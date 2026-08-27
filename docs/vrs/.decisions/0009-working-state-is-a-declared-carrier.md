# Working state is a declared carrier under `working-state://`

Status: accepted

Design decision made by Johannes on 2026-08-27 (interview; supersedes the open
part of [#261](https://github.com/compoundingtech/st2/issues/261), which asked
st2 to pick the name). Merge and acceptance approval required: upstream
maintainers.

## Context

An agent's self-state carriers are declared as Resource bindings and realized
under `<agent-dir>/resources/`: `dev.schickling.agent-goal://` realizes as
`resources/goal.md`, `decision-tree://` as `resources/context/decisions/`, and
so on for notes, private notes, and the friction log.

Working state — R09's restored durable context, written through `st2 context` —
is the exception. Measured on one live catalog of 655 declarations, 605 have a
`resources/context/now.md` and **none declares it**; no binding URI anywhere
mentions it. Consumers reach it by joining a literal path onto the declaration's
own directory, which one downstream author annotated in-line as "a CONVENTION,
not a declaration".

## Decision

Working state becomes the sixth self-state carrier, declared like its siblings:

```kdl
resource "working-state" \
  uri="working-state://<host>/<identity>" \
  reason="Working state for lossless restart."
```

realized at `<agent-dir>/resources/context/now.md`, resolver owned by st2
(`st2 context`). The binding grants no authority, as for every binding.

The scheme is **`working-state`**, un-prefixed. It takes the ontology's already
canonical term for R09's restored durable context — a term the ontology also
already guards against being read as a liveness or activity signal — and the
scheme inherits that guard.

## Consequences

- The carrier is addressable by URI like its five siblings, so a consumer
  resolves a declaration instead of walking a path.
- The ontology gains a `working state` entry naming the term, the verb, the
  realization path, and the scheme.
- Declaring it across the fleet is a bulk binding write, which is why it
  sequences after the mediated write surface (decision 0010).
- This required a requirements delta, confirmed in the same interview and
  written as **R35 st2-owned Resource profiles**. `working-state` is the first
  scheme st2 itself owns and resolves, which R20 ("st2 does not register
  schemes") otherwise forbids; R35 names that as an enumerated exception rather
  than leaving R20 contradicted.

  Three delta shapes were weighed. Amending R09 in place was smaller but would
  bury an envelope-level exception inside a continuity requirement, where a
  reader of R20 would never find it. Writing no delta at all was cheapest and was
  rejected because R20 does not merely omit the case — it asserts the opposite,
  so an authoritative sentence would have been left standing and false. A
  separate enumerated requirement was chosen because the set will grow: st2
  already writes more than one carrier's bytes, and R20 should be qualified once
  rather than per carrier. The ID is R35, not the R22 first drafted; R22 is taken
  by "Quiet coordination after events".

## Options

| Option | Result | Reason |
| --- | --- | --- |
| `working-state://` | Selected | Takes the ontology's already canonical term for R09's restored durable context — a term the ontology also already guards against being read as liveness or activity — so the scheme inherits the guard. No known downstream clash. `decision-tree://` sets the un-prefixed precedent. |
| `agent-context://` | Rejected | Matches the `st2 context` verb, the `resources/context/` directory, and the `agent-notes://` prefix pattern, but [#261](https://github.com/compoundingtech/st2/issues/261) states the requesting consumer already uses "Agent Context" for message-envelope relations. Hands them a collision for internal symmetry. |
| `agent-state://` | Rejected before posing | *State* is the most overloaded word in this ontology — session state, observed harness state, desired state, presence — which already carries explicit collision rules. A scheme by that name recreates the ambiguity this work removes. |
| `agent-working-state://` | Rejected | No clash and full prefix symmetry, but `agent-` carries no information: every per-agent carrier is per-agent and the URI authority already names the agent. |
| Leave working state undeclared | Rejected | `st2 context read <identity>` already resolves it, but the five sibling carriers are declared and the asymmetry is what forces downstream path-joining and filesystem crawls. |

## Evidence and Argument

Measured on one live catalog of 655 declarations
([experiment](../07-resource/.experiments/2026-08-27-resource-read-surfaces.md)):
605 declarations have a `resources/context/now.md`, **none** declares it, and no
binding URI anywhere mentions it. Every other near-universal per-agent carrier is
declared — `notes` on 618 declarations, and `goal`, `private-notes`,
`friction-log`, `decisions` on 27 each — and each realizes into the same
`resources/` directory that working state realizes into.

So the asymmetry is not a design boundary, it is an omission: the one carrier st2
itself writes, through `st2 context`, is the one carrier nothing declares.

#261 documents what the omission costs a consumer — a TUI joining a literal path
onto the declaration's directory, annotated in-line by its own author as "a
CONVENTION, not a declaration", plus an hourly timer crawling
`find <agent>/resources` behind a hand-maintained exclusion list to report which
live declarations lack a `now.md`. Both are downstream workarounds for a fact
that no declaration states.

The naming argument turns on which layer owns the word. The verb and the
directory both say *context*; the ontology says *working state*, and says it in a
rule that already exists to stop the term being confused with liveness. A scheme
is read far from its verb, so it should carry the term that travels with its own
guard.
