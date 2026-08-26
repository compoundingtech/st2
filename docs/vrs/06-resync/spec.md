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
                 event-id=<sha256(canonical binding/path/old/new/occurrence body)>,
                 key=<binding label>, --supersede,
                 subject="resource <binding> changed")
   |
   v  unchanged #286 ingress: dedup ring, receipt validation,
      supersession, inbox file + frontmatter
resources/inbox/<unix-ms>-<rand6>.md   → DING » marker, normal wake
```

## Carrier resolution

A binding URI resolves to a watchable local path when:

1. It is an absolute `file://` URI — the path component is lexically cleaned
   (`.`/`..`) without following symlinks.
2. It is a catalog-relative path (no scheme) — resolved against the agent's
   declaration directory and lexically cleaned the same way.

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
window collapses to one pass; when subscribers of different classes share a
path, each remains dirty until its own class window. Within one due class each
changed carrier gets its own event so per-binding supersession stays meaningful.

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
- After lifecycle execution, each reconcile pass atomically replaces the watch
  set with agents whose canonical seat was observed alive or successfully
  launched/restarted in that pass. Desired declarations, dead keep-retained
  seats, and companion-only launches never become watched. If strict discovery
  temporarily rejects a declaration whose exact canonical seat remains
  observed alive, its prior declaration subscription survives with its digest
  and pending transition; it drops as soon as that seat is not live. Existing
  valid subscriptions are matched by declaration path and binding label. Each
  refresh takes bus id, canonical seat id, carrier path, label, and class from
  the current declaration while retaining digest, the per-subscription
  occurrence sequence, any immutable pending transition, and dirty state; only
  new subscriptions seed silently with sequence zero.
- A previously blind path is digest-diffed both before and after its recovered
  parent watch is registered, closing the poll-to-registration gap.
- Installation failure degrades to timer-based digest polling over the watch
  set (bounded by the number of bindings), never to silence about the
  mechanism. Polling only marks observed changes dirty and schedules the
  carrier's ordinary class deadline; it neither bypasses coalescing nor emits
  ahead of an immutable pending transition.
- A runtime watcher-backend error may mean mutation events were dropped, so it
  schedules every changed carrier through the same pending-aware classified
  path. Equal digests remain silent.
- Digest reads open carriers nonblocking, accept regular files only, and feed
  bytes incrementally into SHA-256 with bounded memory; FIFOs, other special
  files, and large carriers cannot stall or exhaust the worker.

## Built-in stream

`resolve_stream` accepts the reserved stream name `resync` for any running
agent without a declaration check; every other rule (host ownership,
desired-state running, owner-binding validation, ring transaction) is the
implemented [`STREAM-R03..R05`](../04-stream/spec.md) path untouched.
Declaring a stream named `resync` in an Agent Spec is a validation error:
the reservation must not be shadowable. Resource binding names use the event-key
grammar (1..=200 bytes, no surrounding whitespace or controls), and
`declaration` is reserved for the synthetic declaration carrier so supersession
keys cannot collide.

Digest and occurrence-sequence state live with the supervisor process (seeded
at start) and have no durable store, consistent with RESYNC-T03. A captured
transition gets an occurrence token
`v1:<catalog-lock-dev>:<catalog-lock-inode>:<supervisor-pid>:<supervisor-start-time-ticks>:<subscription-sequence>`
in its canonical body before that body is hashed for the event ID. Each
subscription advances its sequence only when capturing a new immutable
transition. Failed publication retains the old digest, exact target digest,
canonical body, occurrence token, and event ID; retry replays those bytes
before observing a newer carrier digest. Repeated A→B legs therefore remain
distinct occurrences, while a retry remains the same reservation. A supervisor
restart changes the incarnation namespace and silently seeds new subscription
sequences. The durable dedup horizon remains the stream receipt ring.

## What this does not do

- No `notify` attribute on resource bindings (deferred until volume data
  justifies per-binding overrides).
- No remote/non-local carrier watching.
- No write attribution beyond static classes (RESYNC-T01).
- No catch-up replay of missed changes (RESYNC-T03).
