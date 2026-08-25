# Resync spec

Implements [`06-resync/requirements.md`](./requirements.md). Vocabulary
inherits [stream](../04-stream/spec.md) (`event`, `event-id`, `key`,
supersession) and [Agent Spec](../02-agent-spec/spec.md) (`resource binding`,
`carrier`).

## Data flow

```text
writer (Nix activation / tool / agent)
   |
   v  whole-file replace, in-place write, …
<agent-dir>/resources/goal.md          (or any watchable carrier)
   |
   v  inotify mutation on the parent directory (non-recursive)
resync watcher thread
   |- classify carrier: immediate | silent | coalesced
   |- digest new bytes (sha256), compare to seeded/last digest
   '- equal → nothing; changed → queue per class window
   |
   v  window boundary
st2::event::emit(bus_id, stream="resync",
                 event-id=<sha256(new bytes)>,
                 key=<binding label>, --supersede,
                 subject="resource <binding> changed")
   |
   v  unchanged #286 ingress: dedup ring, receipt validation,
      supersession, inbox file + frontmatter
resources/inbox/<unix-ms>-<rand6>.md   → DING » marker, normal wake
```

## Carrier resolution

A binding URI resolves to a watchable local path when:

1. It is an absolute `file://` URI — the path component is used verbatim.
2. It is a catalog-relative path (no scheme) — resolved against the agent's
   declaration directory.

Everything else (any other scheme) is not watchable. Watchability of every
active binding is projected into `st2 agents --json` as part of the declared
Resource projection so absence of coverage is observable, never silent.

## Classification

| Class | Members (by resolved path relative to the agent dir, or the declaration itself) | Window |
| --- | --- | --- |
| immediate | the agent's own `agent.kdl`; carriers whose binding name is `goal` or whose basename is `goal.md` | short (500 ms provisional) |
| silent | paths under `resources/context`, `resources/decisions`, `resources/friction` | never emits |
| coalesced | every other watchable local carrier | long (5 s provisional) |

Windows are constants in one place, named as provisional, and tuned by
observed notification volume (issue #341 rollout note). A burst inside a
window collapses to one pass; within that pass each changed carrier still
gets its own event so per-binding supersession stays meaningful.

## Watcher mechanics

- One non-recursive inotify watch per distinct parent directory of the watch
  set, registered through the same `notify` backend and mutation-only filter
  as [`src/watch.rs`](../../../src/watch.rs). Parent-directory watching is
  what makes whole-file replacement by rename visible: the watch attaches to
  the directory inode, which survives its children being replaced.
- Directory identity tracking plus event-time invalidation re-registers on
  same-path replacement, mirroring `CatalogDeclarationWatcher`.
- The watcher owns no reconcile authority: a resync mutation does not wake a
  full-catalog pass. It shares only the observation primitives.
- Watch set is recomputed after each reconcile pass (launch, resume, binding
  edits) — new carriers are watched before their next change can matter.
- Installation failure is diagnosed once and degrades to timer-based digest
  polling over the watch set (bounded by the number of bindings), never to
  silence about the mechanism.

## Built-in stream

`resolve_stream` accepts the reserved stream name `resync` for any running
agent without a declaration check; every other rule (host ownership,
desired-state running, owner-binding validation, ring transaction) is the
implemented [`STREAM-R03..R05`](../04-stream/spec.md) path untouched.
Declaring a stream named `resync` in an Agent Spec is a validation error:
the reservation must not be shadowable.

Digest state lives with the supervisor process (seeded at start, updated on
each observed change); the durable dedup horizon remains the stream receipt
ring, exactly as for external producers.

## What this does not do

- No `notify` attribute on resource bindings (deferred until volume data
  justifies per-binding overrides).
- No remote/non-local carrier watching.
- No write attribution beyond static classes (RESYNC-T01).
- No catch-up replay of missed changes (RESYNC-T03).
