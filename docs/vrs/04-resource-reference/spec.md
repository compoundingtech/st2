# Resource reference specification

This document defines the shared value and the two Resource edge types in
[requirements.md](./requirements.md).

## Status

Draft. This document defines an API and migration contract for later work.

## Data model

The shared value is:

```text
ResourceRef {
  _tag: NonEmptyString,
  uri: AbsoluteUri
}
```

`_tag` is an open discriminator. st2 preserves it and does not require
registration. `uri` uses the Agent Spec absolute-URI validation. st2 preserves
the URI byte-for-byte. A valid value does not prove that the Resource exists,
is reachable, or gives access.

Each edge contains the shared value:

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

An existing wire can keep the fields flat. The Agent Spec form stays:

```kdl
resource "work" _tag="github-issue" uri="github-issue://example/project/123"
```

Its JSON projection stays:

```json
{"name":"work","_tag":"github-issue","uri":"github-issue://example/project/123"}
```

The API uses one shared value. It does not require a nested `resource` object on
an established wire.

## Identity, authority, and mutation

| Edge | Identity | Source and writer | Mutation |
| --- | --- | --- | --- |
| Binding | agent + `name` | Agent Spec; author or catalog publisher | replace declaration |
| Link | agent + `id` | `resources/links/<id>.md`; owning agent | add or remove one record |

The URI identifies the referenced Resource. It does not identify an edge. Many
bindings and links can use the same URI without a conflict.

A declaration change does not write to `resources/links/`. A link change does
not write an Agent Spec. Neither operation changes the object named by the URI.

## Projections

Agent Spec readers and `st2 agents --json` show bindings. The commands
`st2 resource add|ls|read|remove` show only link records.

`st2 resource ls` does not show bindings, inbox state, context, or other durable
data. A consumer can read both projections and join them in memory. The joined
view is read-only.

The link read model shows `id`, `_tag`, `uri`, `relation`, `title`, `tags`, and
`body`. Human `ls` output shows the ID, `_tag`, URI, and optional title. Human
`read` output labels `_tag` as `type` and labels the URI as `uri`, not `url`.

## Link relation

`relation` is optional open text. A present value is not empty. Values such as
`output`, `reference`, and `blocked-by` are conventions. Unknown values survive
reads and writes.

The link tag gives the edge kind. `relation` gives the reason for the link. It
does not replace `ResourceRef._tag` or give authority.

## Exact-URI grouping

The only generic grouping key is the exact `ResourceRef.uri` string. A binding
and two links with `https://example.test/work/1` can form one read-only group.
If case, escaping, a slash, a query, or a fragment differs, the URI forms a
different group. st2 does not normalize the value or use the network.

A group keeps every edge in stable source order. It also keeps every `_tag`.
Different tags for one URI remain visible. A consumer does not select one tag
without an explicit policy. Grouping does not delete or rewrite an edge.

## Canonical link wire

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

The filename is the durable link ID. `title`, `tags`, `relation`, and the body
are link metadata. They are not fields on `ResourceRef`.

The authoring command is:

```text
st2 resource add <uri> [--type <_tag>] [--title <text>]
  [--tag <tag>...] [--relation <relation>] [--body-stdin]
```

`--type` sets the Resource discriminator. `--tag` continues to add link-search
metadata. If `--type` is absent, the writer converts the URI scheme to lowercase
and writes it as `_tag`. Use an explicit type when the scheme is not precise.

New writes use only `_tag` and `uri`. They do not write `url`.

## Legacy `url` transition

The link reader accepts exactly two forms:

- The canonical form has `_tag` and `uri`. The reader validates both fields and
  constructs the exact `ResourceRef`.
- The legacy form has `url`. The reader validates an absolute URI, derives the
  lowercase scheme tag, and preserves the URL bytes as `uri`.

A file fails to decode if it mixes `url` with `_tag` or `uri`, or if it has only
one canonical field. An invalid URI or an empty explicit `_tag` also fails. A
list operation reports the record ID and the error. It does not omit the record.

The dual reader has no expiry in this contract. A read does not rewrite a legacy
record or change its ID or metadata. A remove operation removes only that record.
Each later add uses the canonical form. New state can converge without a fleet
rewrite.

The scheme tag is a compatibility value. It is not domain knowledge. For
example, this legacy value:

```text
https://github.com/example/project/pull/123
```

becomes:

```json
{"_tag":"https","uri":"https://github.com/example/project/pull/123"}
```

An author can instead add a new link with `_tag` `github-pull-request`.

## Plain-folder behavior and task liveness

Both edge types work from their current plain files: Agent Spec KDL and
`resources/links/*.md`. Parse, project, add, and remove operations need no
registry, resolver, remote service, or alternate content store.

Resource data is not part of the effective task launch identity. A reconciler
can adopt changed Resource metadata. A Resource-only change cannot request a
stop, replacement, or relaunch.

## Implementation map

The implementation must add public `agent_spec::spec::ResourceRef`. It must use
one constructor and validator for both edge types. The existing
`agent_spec::spec::Resource` remains the binding wrapper. It delegates `_tag`
and `uri` to the shared value and keeps its accessors and flat serialization.
The link model also contains the same public value.

The current owners are:

- [`agent-spec::spec::Resource`](../../../crates/agent-spec/src/spec.rs) owns
  binding fields and absolute-URI validation.
- [`agent-spec` KDL lowering](../../../crates/agent-spec/src/kdl_format.rs) owns
  the flat binding syntax.
- [`st2::resource`](../../../src/resource.rs) owns link storage, parsing, and
  rendering.
- [`ResourceCmd`](../../../src/main.rs) owns the link CLI.
- [`resource_only_changes_do_not_replace_or_relaunch_a_live_task`](../../../tests/reconcile.rs)
  proves current nondisruption.

Implementation evidence must prove:

1. One constructor validates both edge types.
2. Agent Spec KDL and JSON projections do not change.
3. A canonical link write and read succeed.
4. A legacy read derives the scheme tag and does not rewrite the record.
5. A mixed or incomplete wire fails and reports the record ID.
6. Binding and link projections stay separate.
7. Exact-URI grouping keeps every edge and tag.
8. A Resource-only edge change does not replace healthy work.
