# Resource reference requirements

## Context

st2 has two durable edge types that point to a Resource:

- An Agent Spec binding gives a Resource an agent-local name.
- A link record gives a Resource a relation and optional metadata.

Both edge types use the same typed Resource identity. They have separate
authority, storage, mutation, projection, and lifetime rules.

This document refines root requirements
[`R20` and `R21`](../requirements.md). It resolves
[st2 issue 122](https://github.com/compoundingtech/st2/issues/122) and keeps the
link boundary in [st2 issue 62](https://github.com/compoundingtech/st2/issues/62).
The root requirements have authority if the documents disagree.

## Requirements

- **RESOURCE-R01 One portable reference value:** Each binding and link record
  contains one `ResourceRef`. It has exactly two required fields: a non-empty,
  open `_tag` and an exact RFC 3986 absolute `uri`. st2 preserves both strings.
  It does not resolve the URI or add authority, access, readiness, or lifecycle
  policy.
- **RESOURCE-R02 Distinct edge identities:** A binding identity is the agent
  identity plus binding name. A link identity is the agent identity plus durable
  record ID. A URI is not an edge identity. Many edges can use the same URI.
- **RESOURCE-R03 Separate authority and lifetime:** Agent Spec authors and
  catalog publishers control bindings. The owning agent controls its link
  records. A change to one edge type does not create, change, or remove the
  other edge type.
- **RESOURCE-R04 Separate projections:** Agent Spec readers and
  `st2 agents --json` show bindings. `st2 resource` shows only link records. No
  command provides one combined mutable Resource inventory.
- **RESOURCE-R05 Open link relations:** A relation is absent or is a non-empty,
  open string. st2 has no closed relation registry. A relation does not give
  mutation authority.
- **RESOURCE-R06 Lossless exact-URI grouping:** A read-only consumer can group
  edges only when the `uri` strings are byte-for-byte equal. A group keeps each
  edge, `_tag`, and metadata item. Grouping does not normalize a URI, select one
  `_tag`, combine records, or change storage.
- **RESOURCE-R07 Explicit legacy-link transition:** Readers accept the
  canonical `_tag` and `uri` form and the former `url` form. For a legacy `url`,
  the reader uses the same scheme-derived default `_tag` that a new untyped link
  uses. New writes use only the canonical form. A read does not rewrite a file.
- **RESOURCE-R08 Nondisruptive edge changes:** A change to only bindings or link
  records does not change the effective task launch definition. It does not
  stop, replace, or relaunch healthy work.

## Acceptable tradeoffs

- **RESOURCE-T01 Less precise compatibility default:** A URI scheme gives a
  deterministic fallback type for an untyped legacy link. An explicit `_tag` can give
  a more precise type. Compatibility is better than invented type knowledge.
- **RESOURCE-T02 Duplicate views preserve intent:** A consumer can see many
  edges for one exact URI. Separate edges preserve their authority and metadata.

## Non-goals

- Resolve a Resource, add a Resource registry, or change an external object.
- Treat a URI as authority.
- Move linked outputs into Agent Spec.
- Require an external store, publisher, or controller for plain-folder data.
