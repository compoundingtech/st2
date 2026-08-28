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
   |- observe present(sha256) | missing; compare to seeded/last state
   '- equal → nothing; changed → queue per class window
   |
   v  window boundary
st2::event::emit_builtin_resync(bus_id,
                 event-id=<sha256(canonical binding/path/old/new/occurrence body)>,
                 key=<binding label>, supersede=true,
                 subject="resource <binding> changed")
   |
   v  supervisor-only admission, then unchanged #286 machinery:
      dedup ring, receipt validation, supersession, inbox file + frontmatter
resources/inbox/<unix-ms>-<rand6>.md   → DING » marker, normal wake
```

## Carrier resolution

A binding URI resolves to a watchable local path in this precedence order:

1. A URI whose exact scheme has a declared
   [`07-resource-profile`](../07-resource-profile/spec.md) resolves through that
   profile to a host-contained path and declared notification class.
2. An unregistered absolute `file://` URI uses its lexically cleaned path
   component (`.`/`..`) without following symlinks.
3. A catalog-relative path with no scheme resolves against the agent's
   declaration directory and is lexically cleaned the same way.

Any other unregistered scheme is not watchable. A registered profile that
cannot load or resolve also leaves only that binding unwatchable; it never
falls back to a guessed local rule or stops the supervisor. Watchability of
every active binding is projected into `st2 agents --json` as part of the
declared Resource projection so absence of coverage is observable.

## Classification

Classification is decided before a path enters the watch set:

| Source | Class | Window |
| --- | --- | --- |
| profile-resolved carrier | the trusted `class` beside the catalog's resolver module | `immediate`: short; `coalesced`: long; `silent`: excluded |
| native declaration | immediate | short (500 ms provisional) |
| native local carrier named `goal` or with basename `goal.md` | immediate | short (500 ms provisional) |
| native local path under `resources/context`, `resources/decisions`, or `resources/friction` | silent | excluded |
| every other native local carrier | coalesced | long (5 s provisional) |

Profile class takes precedence over basename/path heuristics; the guest module
cannot choose it. Windows are constants in one place, named as provisional,
and tuned by observed notification volume (issue #341 rollout note). A burst
inside a window collapses to one pass; when subscribers of different classes
share a path, each remains dirty until its own class window. Within one due
class each changed carrier gets its own event so per-binding supersession stays
meaningful.

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
  observed alive, its prior declaration subscription survives with its carrier
  state and pending transition; it drops as soon as that seat is not live.
  Existing valid subscriptions are matched by declaration path and binding
  label. Each refresh takes bus id, canonical seat id, carrier path, label, and
  class from the current declaration while retaining carrier state, the
  per-subscription occurrence sequence, any immutable pending transition, and
  dirty state; only new subscriptions seed silently with sequence zero.
- A previously blind path is state-diffed both before and after its recovered
  parent watch is registered, closing the poll-to-registration gap.
- Installation failure degrades to timer-based carrier polling over the watch
  set (bounded by the number of bindings), never to silence about the
  mechanism. Polling only marks observed changes dirty and schedules the
  carrier's ordinary class deadline; it neither bypasses coalescing nor emits
  ahead of an immutable pending transition.
- A runtime watcher-backend error may mean mutation events were dropped, so it
  schedules every changed carrier through the same pending-aware classified
  path. Equal states remain silent.
- Reads open carriers nonblocking and without following the final symlink
  (every component for confined carriers). A proven regular file becomes
  `present(<sha256>)`; `ENOENT` or a stable non-regular replacement becomes
  `missing`. Permission and transient I/O errors are diagnosed and retried
  without changing state. FIFOs and other special files therefore cannot stall
  the worker and produce one tombstone transition rather than silent ambiguity.

## Built-in stream

`emit_builtin_resync` is crate-internal, fixes the stream name to the reserved
`resync` value, and is called only by the supervisor's resync watcher. Public
`event::emit` and the CLI always require the recipient to declare the requested
stream, so neither can claim or supersede built-in resync events. Both admission
paths then share the same host ownership, desired-state running,
owner-binding validation, catalog-lock serialization, event validation, and
ring transaction implementing [`STREAM-R03..R05`](../04-stream/spec.md).
Declaring a stream named `resync` in an Agent Spec is a validation error:
the reservation must not be shadowable. Resource binding names use the event-key
grammar (1..=200 bytes, no surrounding whitespace or controls), and
`declaration` is reserved for the synthetic declaration carrier so supersession
keys cannot collide.

Carrier state and occurrence-sequence state live with the supervisor process
(seeded at start) and have no durable store, consistent with RESYNC-T03. A
carrier state is `present(<sha256>)` or `missing`. Present→missing emits a
canonical tombstone whose body contains `old: <digest>` and `new: missing`;
missing→present emits a creation even when that digest matches the bytes from
before deletion. Repeated missing observations are silent.

A captured transition gets an occurrence token
`v1:<catalog-lock-dev>:<catalog-lock-inode>:<supervisor-pid>:<supervisor-start-time-ticks>:<subscription-sequence>`
in its canonical body before that body is hashed for the event ID. Each
subscription advances its sequence only when capturing a new immutable
transition. Failed publication retains the old state, exact target state,
canonical body, occurrence token, and event ID; retry replays those bytes
before observing newer carrier state. Repeated transition legs therefore
remain distinct occurrences, while a retry remains the same reservation. A
supervisor restart changes the incarnation namespace and silently seeds new
subscription sequences. The durable dedup horizon remains the stream receipt
ring.

## What this does not do

- No `notify` attribute on resource bindings (deferred until volume data
  justifies per-binding overrides).
- No remote/non-local carrier watching.
- No write attribution beyond static classes (RESYNC-T01).
- No catch-up replay of missed changes (RESYNC-T03).
