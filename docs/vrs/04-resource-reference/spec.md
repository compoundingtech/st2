# Resource reference specification

This document specifies the shared value and the two distinct Resource edge
types required by [requirements.md](./requirements.md).

## Status

Draft. This is an API and migration contract for a later implementation.

## Data model

The shared value is:

```text
ResourceRef {
  _tag: NonEmptyString,
  uri: AbsoluteUri
}
```

`_tag` is an open discriminator. st2 preserves it exactly and does not require
registration. `uri` uses the existing Agent Spec absolute-URI validation and
is preserved byte-for-byte. A valid value does not imply that the Resource
exists, is reachable, or grants access.

The two edges embed that value:

```text
Binding {
  name: NonEmptyString,
  resource: ResourceRef
}

Link {
  id: RecordId,
  relation: NonEmptyString?,
  resource: ResourceRef,
  title: String?,
  tags: [String],
  body: String?
}
```

The embedding may remain flat on an existing wire. In particular, the
canonical Agent Spec form stays:

```kdl
resource "work" _tag="github-issue" uri="github-issue://example/project/123"
```

and its JSON projection stays:

```json
{"name":"work","_tag":"github-issue","uri":"github-issue://example/project/123"}
```

The shared value is an API boundary, not a requirement to add a nested
`resource` object to those established forms.

## Identity, authority, and mutation

| Edge | Durable identity | Source of truth | Writer | Mutation |
| --- | --- | --- | --- | --- |
| Binding | agent identity + `name` | Agent Spec declaration | declaration author or catalog publisher | replace the declaration |
| Link | agent identity + `id` | `resources/links/<id>.md` | owning agent | append or remove one link record |

The URI identifies the referenced Resource. It does not identify either edge.
Two binding names, two link IDs, or one edge of each kind may carry the same
URI without conflict.

A declaration edit does not write under `resources/links/`. A link add or
remove does not write an Agent Spec. Neither operation mutates the object named
by the URI.

## Projections

Declared bindings remain on the Agent Spec read model and in
`st2 agents --json`. Linked-reference records remain on
`st2 resource add|ls|read|remove`.

`st2 resource ls` does not add declared bindings, agent inbox state, context,
or other durable material. A consumer that needs both edge types reads both
projections and joins them in memory. The joined view has no write operation.

The linked-reference read model exposes `id`, `_tag`, `uri`, `relation`,
`title`, `tags`, and `body`. Human `ls` output includes the record ID, `_tag`,
URI, and optional title. Human `read` output labels `_tag` as `type` and
replaces the former `url` label with `uri`.

## Link relation

`relation` remains optional, open text. If present, it must be non-empty. Values
such as `output`, `reference`, and `blocked-by` are conventions, not a closed
enumeration. Unknown values survive reads and writes unchanged.

The `Link` edge tag supplies the edge kind. `relation` refines why that link
exists; it does not replace `ResourceRef._tag` and does not grant authority.

## Exact-URI view grouping

The only generic grouping key is the exact `ResourceRef.uri` string.

For example, a binding and two links with the exact URI
`https://example.test/work/1` may appear as one read-only group containing all
three edges. If one spelling changes case, escaping, a trailing slash, query,
or fragment, it forms a different group. st2 performs no normalization or
network lookup.

A group retains every edge in stable source order and retains every `_tag`.
Different `_tag` values for one exact URI are visible disagreement. A consumer
must not choose one silently. Grouping never deletes or rewrites an edge.

## Canonical linked-reference wire

New link files use `_tag` and `uri` in YAML frontmatter:

```markdown
---
_tag: github-pull-request
uri: https://github.com/example/project/pull/123
title: Resource reference contract
tags: docs, vrs
relation: output
---
Optional notes.
```

The filename remains the durable link ID. `title`, `tags`, `relation`, and the
body remain link metadata rather than fields on `ResourceRef`.

The authoring interface is:

```text
st2 resource add <uri> [--type <_tag>] [--title <text>]
  [--tag <tag>...] [--relation <relation>] [--body-stdin]
```

`--type` is the Resource discriminator. Existing `--tag` continues to collect
link-search metadata. When `--type` is absent, the writer derives `_tag` from
the URI scheme, converted to lowercase, and writes that value explicitly.
An explicit `--type` is preferred when the scheme is less precise than the
Resource type.

All new writes use canonical `_tag` plus `uri` frontmatter. The writer never
emits `url`.

## Legacy `url` transition

The link reader recognizes exactly two forms:

| Form | Required fields | Result |
| --- | --- | --- |
| Canonical | `_tag`, `uri` | validate and construct that exact `ResourceRef` |
| Legacy | `url` | validate the URL as an absolute URI; derive `_tag` from its lowercase scheme; preserve the URL bytes as `uri` |

A file that mixes `url` with `_tag` or `uri`, or supplies only one canonical
field, is ambiguous and fails to decode. An invalid absolute URI or empty
explicit `_tag` also fails to decode. A list operation reports the record ID
and error instead of omitting the bad record.

The dual reader has no expiry in this contract. Reading or listing a legacy
record does not rewrite it, change its ID, or change its metadata. Removing a
legacy record removes only that record. Every later add writes the canonical
form, so new state converges without a fleet-wide rewrite.

The scheme-derived tag is a compatibility value, not inferred domain
knowledge. For example, legacy
`https://github.com/example/project/pull/123` becomes:

```json
{"_tag":"https","uri":"https://github.com/example/project/pull/123"}
```

An author who knows that this is a GitHub pull request can instead add a new
link with `_tag` `github-pull-request`.

## Plain-folder behavior and task liveness

Both edge types remain fully usable from their current plain files: direct
Agent Spec KDL and `resources/links/*.md`. No registry, resolver, remote
service, or alternate content store is required to parse, project, add, or
remove them.

Resource data is excluded from effective task launch identity. A reconciler
may adopt changed Resource metadata, but a Resource-only change cannot request
a stop, replacement, or relaunch.

## Implementation map

The implementation must introduce public `agent_spec::spec::ResourceRef` with
one reusable constructor and validator rather than duplicate the envelope.
The existing `agent_spec::spec::Resource` remains the binding wrapper and
delegates its `_tag` and `uri` fields to that value while preserving its
current accessors and flat serialization. The linked-record model also carries
that same public value.

The current ownership map is:

- [`agent-spec::spec::Resource`](../../../crates/agent-spec/src/spec.rs) owns
  the current declared binding fields and absolute-URI validation.
- [`agent-spec` KDL lowering](../../../crates/agent-spec/src/kdl_format.rs)
  owns the flat canonical binding syntax.
- [`st2::resource`](../../../src/resource.rs) owns linked-record storage,
  parsing, and rendering.
- [`ResourceCmd`](../../../src/main.rs) owns the linked-reference CLI.
- [`resource_only_changes_do_not_replace_or_relaunch_a_live_task`](../../../tests/reconcile.rs)
  is the existing nondisruption proof.

Implementation evidence must cover:

1. the same constructor validating both edge types;
2. unchanged Agent Spec KDL and JSON projection;
3. canonical link write and read;
4. legacy read with a scheme-derived tag and no rewrite;
5. mixed or incomplete wire refusal with a visible record ID;
6. separate declared and linked projections;
7. exact-URI grouping that retains every edge and tag; and
8. no healthy task replacement for either Resource-only edge change.
