# st3 client v0 contract

Status: contract baseline with operational and launch read projections implemented. The files in
[`schemas`](schemas) and [`fixtures`](fixtures) are the normative wire examples. Rust and Swift
clients consume the same JSON; neither client parses CLI output, Markdown, KDL, claim envelopes, or
harness transcript files.

## Boundary and transport

The client API is a projection and command gateway, not a graph replica. Its version is
`st3.client.v0` and its routes live below `/v1/client`. A local client connects to the daemon's Unix
socket. A remote client connects to a loopback-only gateway through authenticated Fabric. The
daemon and gateway MUST NOT bind this API to a non-loopback TCP address.

The gateway authenticates a paired device and derives the actor and scopes for the connection.
Request bodies never select an actor. Device credentials are scoped, individually revocable, and
different from fleet replication secrets. A remote client is always online: v0 has no offline
mutation queue, push notification service, cached graph authority, or multi-master replication.

Every JSON response has `api_version`, `request_id`, and either `value` or a versioned error. Read
responses also carry one `snapshot`:

```json
{
  "api_version": "st3.client.v0",
  "request_id": "req/0199...",
  "snapshot": {
    "id": "snapshot/host-a/1842/2fc9...",
    "host_id": "host/host-a",
    "store_index": 1842,
    "projection_version": "client-projection.v0",
    "created_at": "2026-09-20T12:00:00.000Z"
  },
  "value": {}
}
```

A snapshot ID identifies the complete projected state at one local store index and projection
version. All items in a response are computed in one read transaction. A page cursor is opaque,
bound to the snapshot ID, filter, page size, and final sort tuple, and expires no earlier than the
advertised `cursor_expires_at`. A cursor cannot silently move to a newer snapshot.

## Discovery, lists, and details

`GET /v1/client/capabilities` is the first request. It returns the negotiated limits, schema URIs,
feed retention boundary, authenticated session actor, and capabilities. Unknown required capability
versions stop the client; unknown optional capabilities may be ignored.

The following resources have list and detail routes. List routes accept `limit` and `cursor` plus
documented filters. The server clamps `limit` to `capabilities.limits.max_page_items`. Empty values
sort after present values, strings compare as Unicode scalar sequences, and the final key is always
the stable `id` ascending. No locale-sensitive ordering is permitted.

| Resource | List and detail routes | Deterministic list order |
|---|---|---|
| Attention | `/attention`, `/attention/{id}` | priority descending, requested time ascending, ID |
| Messages | `/messages`, `/messages/{id}` | sent time descending, ID |
| Launches | `/launches`, `/launches/{id}` | updated time descending, ID |
| Launch variants | `/launches/{id}/variants`, `/launches/{id}/variants/{variant_id}` | ordinal, ID |
| Launch decisions | `/launches/{id}/decisions`, `/launches/{id}/decisions/{decision_id}` | requested time, ID |
| Launch approvals | `/launches/{id}/approvals`, `/launches/{id}/approvals/{approval_id}` | decided time, ID |
| Missions | `/missions`, `/missions/{id}` | updated time descending, ID |
| Work | `/work`, `/work/{id}` | ready first, readiness epoch, path, ID |
| Agents | `/agents`, `/agents/{id}` | presentation name, ID |
| Runtimes | `/runtimes`, `/runtimes/{id}` | owning agent, runtime kind, ID |
| Operations | `/operations`, `/operations/{id}` | severity descending, component, ID |
| History | `/history`, `/history/{id}` | occurred time descending, store index descending, ID |
| Sessions | `/sessions`, `/sessions/{id}` | updated time descending, ID |
| Session timeline | `/sessions/{id}/timeline` | sequence ascending |

IDs are stable opaque strings with a type prefix. Renames change labels, not IDs. A detail response
uses the same representation as its list item plus its documented detail fields. Deletion is
represented by an event tombstone; an ID is never reused.

`operations` is the client-safe operational view: daemon health, host reachability, transport
health, resource observers, and diagnostics. `history` is a typed audit projection. It does not
expose raw claims, replication envelopes, or repair internals.

## Harness-neutral session timeline

The timeline schema deliberately contains no Claude, Codex, Pi, OMP, or transcript-file types. A
session has ordered entries with a strictly increasing `sequence`, stable entry ID, RFC 3339
timestamp, `role` (`system`, `user`, `assistant`, or `tool`), and one typed body:

- `message`: logical turn metadata;
- `content`: text or an attachment reference with a media type;
- `tool_call`: stable call ID, tool name, and JSON arguments;
- `tool_result`: the matching call ID, JSON/text content, and success status;
- `status`: queued, running, waiting, completed, failed, or cancelled;
- `error`: versioned safe code, message, retryability, and details;
- `usage`: input, output, cached, and total tokens plus optional cost data;
- `redaction`: reason and the byte or item count withheld;
- `truncation`: omitted range, reason, and a continuation cursor when recoverable.

Incremental timeline events use `append`, `replace`, or `finalize`. `replace` targets an existing
entry and increments its revision; it cannot change the entry's ID, sequence, role, or type.
`finalize` makes the entry immutable. Tool results must refer to a preceding tool call. Timeline
pages and updates are bounded by the negotiated byte and item limits.

## Launches, variants, decisions, and approvals

`launch` is the only user-facing noun for authoring and reviewing a mission. Planning remains an
internal phase and is not an API resource or a CLI compatibility alias.

A launch owns its request, target (new mission or an exact mission run generation), variants,
decisions, approvals, and terminal outcome. Variant content is typed projected mission data; clients
do not submit or receive KDL or Markdown. A preview returns its normalized graph, validation
diagnostics, and a deterministic token:

```
lpv0:<lowercase SHA-256 of RFC 8785 canonical JSON {
  api_version, launch_id, variant_id, candidate_revision,
  target_generation, normalized_mission, diagnostics
}>
```

The same values always produce the same token on every host. Approval carries that exact token and
the launch revision fence. Any candidate, target generation, normalized mission, or diagnostic
change produces another token. Approval publishes a mission revision but does not start it.

Every preview also carries `structured_diff` and a `st3.visualization.v0` model. The model has
graph nodes and edges, timeline entries, assignment swimlanes, revision and risk summaries, and
live-progress placeholders with explicit attempts, leases, progress, blockers, attention, errors,
and cursors. It is the shared input to graphical and textual clients; those clients never recover
structure from prose.

## Typed actions and fences

All mutations use `POST /v1/client/actions` and the `ActionRequest` union in the schema. The common
fields are:

- `id`: a client-generated stable action ID;
- `type`: the action discriminator;
- `idempotency_key`: unique within the paired session for at least 30 days;
- `fence`: the snapshot and exact mutable identities the user acted on;
- `parameters`: the type-specific body.

The authenticated session supplies actor and scopes. `actor`, `credential`, and fleet secrets are
invalid request fields. Repeating the same key and byte-equivalent action returns the original
result. Reusing a key with different bytes returns `idempotency-conflict`. Stale generation,
revision, incarnation, or snapshot fences return `stale-fence` without a partial mutation.
Multi-subject actions commit atomically or have no effect.

The v0 action discriminators are:

| Family | Actions | Required fences |
|---|---|---|
| Attention | `attention.resolve` | attention revision |
| Messages | `message.send`, `message.read`, `message.close` | reply/message revision when present |
| Launches | `launch.create`, `launch.revise`, `launch.preview`, `launch.approve`, `launch.cancel` | launch revision; target generation and preview token where applicable |
| Missions | `mission.start`, `mission.revise`, `mission.approve-revision`, `mission.cancel` | mission revision and current generation where applicable |
| Work | `work.claim`, `work.renew`, `work.progress`, `work.complete`, `work.fail`, `work.release`, `work.publish-mission` | generation, definition, attempt, readiness epoch, and claimant incarnation after claim |
| Runtimes | `runtime.stop`, `runtime.restart`, `runtime.reset`, `runtime.context-clear`, `runtime.signal` | runtime incarnation and desired revision |
| Terminals | `terminal.input`, `terminal.resize`, `terminal.attach`, `terminal.detach` | runtime incarnation and terminal sequence |
| Pairing | `pairing.begin`, `pairing.complete`, `pairing.revoke` | pairing/device revision where applicable |

An accepted action returns one stable operation ID and status. `202 accepted` means the command is
durable, not complete; clients follow operation events or read `/operations/{id}`. Result objects
include affected stable IDs and the resulting snapshot ID.

## Event feed and resynchronization

`GET /v1/client/events?after=CURSOR&limit=N&wait_ms=M` returns at most the negotiated event and byte
limits. `wait_ms` is clamped and is only a bounded long poll. The opaque cursor denotes the next
projection event, not a graph or replication position. Events are ordered by `(epoch, sequence)` and
contain a unique ID, previous and next cursor, timestamp, `upsert`, `delete`, `timeline.delta`,
`terminal.available`, or `capabilities.changed`, affected resource IDs, and the resulting snapshot
ID. Replaying `after` is safe and may repeat the last page; clients deduplicate by event ID.

The response advertises `oldest_cursor` and `resume_cursor`. If `after` is unknown, expired, belongs
to another authenticated scope, or precedes retention, the server returns the versioned
`cursor-gap` error with `full_resync: true`. The client discards projection caches, fetches fresh
snapshot pages, and resumes from the capabilities response's `event_cursor`. It must not infer
missing mutations or request graph replication.

## Pairing and remote access

Pairing is device-to-person and requires an authenticated person on the local Unix API to begin.
The response shows a short-lived single-use code and pairing ID. A remote device reaches the
loopback gateway through Fabric, proves the code, supplies its public key, and receives a scoped
credential bound to that key. The resulting session returns its derived actor and granted scopes.
Pairing codes expire after five minutes, reveal no fleet secret, and cannot request their own actor
or scopes. Revocation takes effect for new requests and closes active feeds and terminal streams.

Read-only scope permits snapshots, details, timelines, and event feeds. `terminal.control` adds
terminal input and resize; other control scopes are action-family-specific. A capabilities response
must distinguish unavailable, ungranted, and unsupported features.

## Terminal protocol

Terminal access is a client protocol, not raw PTY ownership. `terminal.attach` returns a short-lived,
single-use stream capability and a WebSocket URL on the same Unix or loopback gateway. The WebSocket
subprotocol is `st3.client.terminal.v0`. The first server message is an atomic screen snapshot with
runtime incarnation, dimensions, cursor state, title, ordered screen lines, and `next_sequence`.

Subsequent server frames have contiguous sequence numbers and are one of `output`, `resize`,
`title`, `bell`, `exit`, or `resync`. On reconnect, the client supplies the runtime incarnation and
last applied sequence. The server resumes while retained; otherwise it sends a new atomic screen
snapshot. A changed incarnation always requires a new snapshot. Input and resize are fenced typed
actions. Attach and detach affect only the viewer; detaching never stops the runtime.

Read-only terminal scope permits screen snapshots and frames but rejects input and resize. Frame and
screen payloads obey negotiated byte limits and use explicit `redacted` or `truncated` markers.

## Errors and evolution

Errors have `error_version: st3.client.error.v0`, a stable kebab-case code, safe message,
`retryable`, structured details, and optional `retry_after_ms`. Required v0 codes are `not-found`,
`forbidden`, `unsupported-capability`, `validation-failed`, `idempotency-conflict`, `stale-fence`,
`cursor-gap`, `page-cursor-expired`, `rate-limited`, and `internal`.

Adding optional fields is compatible. Removing or retyping a field, changing ordering or token
rules, adding a required action parameter, or changing action semantics requires a new capability
version or API version. Clients preserve unknown enum cases for display but never send an action
whose capability version they do not understand.

## Conformance assets

[`schemas/client-v0.schema.json`](schemas/client-v0.schema.json) contains the shared wire types.
[`schemas/operations.json`](schemas/operations.json) is the machine-readable route/action/capability
manifest. [`fixtures/manifest.json`](fixtures/manifest.json) maps every golden fixture to its root
schema definition. The Rust tests validate fixture coverage, IDs, ordering, fences, timeline links,
and deterministic preview tokens. Ignored baseline tests exercise the missing implementation and
are intentionally red until the corresponding server work lands.
