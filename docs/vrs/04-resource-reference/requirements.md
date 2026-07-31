# Resource reference requirements

## Context

st2 has two durable edge types that point to a Resource:

- an Agent Spec binding gives a declared Resource an agent-local name; and
- a linked-reference record gives an agent-owned Resource a relation and
  optional descriptive metadata.

Both edge types need the same typed Resource identity. They do not have the
same authority, storage, mutation, projection, or lifetime.

This document refines root requirements
[`R20` and `R21`](../requirements.md). It resolves
the contract questions in [st2 issue 122](https://github.com/compoundingtech/st2/issues/122)
without changing the linked-reference boundary in
[st2 issue 62](https://github.com/compoundingtech/st2/issues/62). Where this
file and the root requirements disagree, the root requirements win.

## Requirements

- **RESOURCE-R01 One portable reference value:** Every declared binding and
  linked-reference record carries a `ResourceRef` with exactly two required
  fields: a non-empty, open `_tag` and an exact RFC 3986 absolute `uri`. st2
  preserves both strings and does not resolve the URI or attach authority,
  readiness, access, or lifecycle policy to it.
- **RESOURCE-R02 Distinct edge identities:** A declared binding is identified
  by agent identity plus binding name. A linked-reference record is identified
  by agent identity plus its durable record ID. A Resource URI is not an edge
  identity. Several edges may refer to the same exact URI.
- **RESOURCE-R03 Separate authority and lifetime:** Agent Spec authors and
  catalog publishers control declared bindings. The owning agent controls its
  linked-reference records. A mutation on one edge type never creates,
  rewrites, or removes the other edge type.
- **RESOURCE-R04 Separate projections:** Agent Spec readers and
  `st2 agents --json` expose declared bindings. `st2 resource` remains a
  linked-reference-only surface. No command presents a combined mutable
  Resource inventory.
- **RESOURCE-R05 Open link relations:** A link relation is absent or a
  non-empty, open string. st2 does not define a closed relation registry and
  does not infer mutation authority from a relation.
- **RESOURCE-R06 Lossless exact-URI grouping:** A read-only consumer may group
  edges only when their `uri` strings are byte-for-byte equal. Grouping retains
  every edge, `_tag`, and item of edge metadata. It does not normalize a URI,
  select a winning `_tag`, coalesce records, or change storage.
- **RESOURCE-R07 Explicit legacy-link transition:** Readers accept both the
  canonical `_tag` plus `uri` link form and the former `url` form. A legacy
  `url` receives the same documented scheme-derived default `_tag` as a new
  link whose author omits an explicit type. New writes use only the canonical
  form. Reads never rewrite a legacy file.
- **RESOURCE-R08 Nondisruptive edge changes:** Adding, changing, or removing
  only Resource bindings or linked-reference records does not change a task's
  effective launch definition and does not stop, replace, or relaunch healthy
  work.

## Acceptable tradeoffs

- **RESOURCE-T01 Compatibility default is less precise:** A URI scheme is a
  deterministic fallback type for an untyped legacy link, but an explicit
  `_tag` can describe the Resource more precisely. Compatibility is preferred
  to inventing type knowledge during migration.
- **RESOURCE-T02 Duplicate views over destructive deduplication:** Consumers
  may see several edges for one exact URI. Keeping their distinct authorities
  and metadata is preferred to a simpler view that loses durable intent.

## Non-goals

- Resource resolution, a Resource registry, or mutation of an external object.
- Treating possession of a URI as authority.
- Moving linked outputs into Agent Spec.
- Making an external content store, publisher, or controller necessary for
  plain-folder Agent Spec bindings or linked-reference records.
