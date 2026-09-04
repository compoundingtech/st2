# Message specification

This document specifies sender-owned native message history. It builds on
[requirements.md](./requirements.md). Recipient terminal delivery remains in
[`01-ding/spec.md`](../01-ding/spec.md).

## Status

Active.

## Scope

This specification defines ordinary Agent `message send`, `message reply`, and `message sent`
publication and observation. It does not project typed service-principal request state, define
replication guarantees, or reconstruct messages sent before sender-index initialization
(`MESSAGE-R01`, `MESSAGE-R11`).

## State ownership

```text
sender resources/sent/                 recipient resources/
  index.json                             inbox/<filename>.md
  active.json                            archive/<filename>.md
  .lock
  pending/<sha256>.json
  messages/<filename>.md.json
  commits/<sha256>.json
  keys/<sha256>.json
```

`index.json` is a constant-size atomic head. `active.json` identifies the only publication allowed
in flight under the sender lock. Pending records own the exact recipient operation. Sender rows own
the canonical endpoints, filename, metadata, body, optional idempotency key, and exact rendered
recipient bytes. Immutable content-addressed commit nodes link every completed row to its predecessor.
Recipient state is never scanned during sent enumeration (`MESSAGE-R01`, `MESSAGE-R02`,
`MESSAGE-R10`).

The version-1 index is:

```json
{
  "version": 1,
  "since": 1786550000000,
  "count": 1,
  "tip": "8b1d..."
}
```

A version-1 commit node is:

```json
{
  "version": 1,
  "ordinal": 1,
  "previous": null,
  "filename": "1786550000123-abc123.md",
  "rowDigest": "be42..."
}
```

The version-1 record is:

```json
{
  "version": 1,
  "filename": "1786550000123-abc123.md",
  "ts": 1786550000123,
  "from": "h.sender",
  "to": "h.recipient",
  "subject": null,
  "inReplyTo": null,
  "tags": [],
  "priority": null,
  "idempotencyKey": null,
  "body": "message body\n",
  "renderedMessage": "---\nfrom: h.sender\n---\nmessage body\n"
}
```

Committed record filenames append `.json` to the canonical message filename. Pending and commit
filenames are the SHA-256 digest of their canonical JSON content plus `.json`. A pending filename
therefore binds its content before `active.json` exists. Temporary siblings use the reserved
`.message.tmp-` prefix and are invisible to readers. A complete read follows exactly `count` nodes
from `tip` to genesis, verifies
each content digest, ordinal, predecessor, row digest, and payload/filename relation, then compares
the exact reachable node and row sets with both directories. Only an active intent may explain an
otherwise unreachable row or commit node. Missing, extra, substituted, corrupt, unreadable, or
unsupported-version state fails closed (`MESSAGE-R03`, `MESSAGE-R04`). The atomic head is the bounded
local trust root; coordinated rewrite of the head and all matching sender state is outside this loss
and corruption contract (`MESSAGE-A03`).

## Publication transaction

```text
per-sender LOCK
      |
      v
head -> pending -> active -> recipient -> row -> immutable node -> head -> cleanup
          |          |                                           |
          +----------+------ retry reuses filename and bytes -----+
```

One advisory kernel lock at `resources/sent/.lock` serializes local publications for that sender.
The lock file is persistent; dropping the process releases the lock. A send then performs these
steps (`MESSAGE-R05`, `MESSAGE-R06`, `MESSAGE-R08`):

1. Atomically create `index.json` if absent. Its `since` precedes every indexed send attempt.
2. Recover the single pending/active publication, if present.
3. Resolve both Agent endpoints to canonical bus identities. A message using the explicit eval
   external requester capability at either endpoint bypasses ordinary Sent indexing.
4. Atomically create one pending record with a fresh canonical message filename.
5. Atomically create `active.json` containing its filename and record digest.
6. Materialize the exact recipient bytes under that filename. An identical inbox file or archive
   receipt is success; different bytes are a collision.
7. Atomically create the identical sender record under `messages/`.
8. Create the immutable commit node from the old head and sender-row digest.
9. Atomically replace the head with the new count and node digest, then publish the scoped immutable
   idempotency receipt when the caller supplied a key.
10. Remove the pending record and active pointer, then return the filename.

The directories do not share one transaction. This ordering chooses recipient-only partial state
over a false completed Sent row (`MESSAGE-T01`). A crash before step 4 has no message effect. A crash
from steps 4 through 10 leaves enough sender-owned state to resume. A pending record created before
active publication is an explicit recoverable partial state. An active pointer without pending fails
unless the head proves that intent already committed. A row or commit node before head publication is
explained only by the active intent and is not completed Sent. A head-advanced publication with stale
active or pending state is recoverable cleanup. A pending-only cleanup record must match the current
head-tip filename, the tip row digest, and the immutable sender row digest. A shared-lock read reports
that exact record as completed coverage but does not clean it. The next exclusive sender operation
publishes a missing key receipt and removes the pending record. No recovery mutates an immutable row
or node. A pending record for an older committed row still fails closed.

## Retry identity

`--idempotency-key` is optional on `send` and `reply`. The durable key scope is `(canonical sender,
canonical recipient, key)`. Under the sender lock, a matching completed record returns its original
filename. Different content under the same scoped key fails. Different recipients may reuse a key
(`MESSAGE-R07`).

Without a key, pending recovery can reuse the interrupted intent. After the pending record is
cleared, a crash before stdout creates unavoidable response ambiguity. Repeating identical unkeyed
input is therefore a new send. Payload hashing is not used because it would collapse intentional
identical messages (`MESSAGE-T03`, `MESSAGE-R08`).

## Sent API and wire shape

```text
st2 message sent [identity]
  [--count]
  [--include-body]
  [--since <unix-ms>]
  [--to <canonical-recipient>]
  [--json]
```

Identity defaults to `--as`, then `ST_AGENT`. Rows sort by `(ts, filename)`. `--since` is strict;
`--to` compares the canonical persisted recipient. `--include-body` adds `body`; otherwise the key
is absent. `--count` prints a number only for `since` coverage and refuses unavailable or partial
coverage (`MESSAGE-R03`, `MESSAGE-R09`).

The optional identity follows the existing catalog read-authority model of `message ls [identity]`:
catalog filesystem read access permits observation of any declared Agent selected by bare identity
or canonical bus ID. Selecting an identity does not grant publication authority or change the acting
identity.

The shared JSON envelope is a tagged coverage union:

```json
{
  "coverage": { "_tag": "since", "since": 1786550000000 },
  "messages": [
    {
      "filename": "1786550000123-abc123.md",
      "ts": 1786550000123,
      "to": "h.recipient",
      "subject": null,
      "inReplyTo": null,
      "tags": [],
      "priority": null
    }
  ]
}
```

Coverage variants are exact:

| `_tag` | Additional fields | Meaning |
| --- | --- | --- |
| `unavailable` | none | No index marker exists; empty rows make no historical claim. |
| `since` | `since` | All successful indexed sends at or after the boundary are represented. |
| `partial` | `since`, `pending` | The boundary exists, but one or more intents are incomplete. |

`SentMessages`, `SentCoverage`, and `SentMessageRow` live in `st2-wire`. A sent row has `to`, never
`from`; optional fields remain nullable and an unrequested body remains absent
(`MESSAGE-R02`, `MESSAGE-R04`).

### Fractal consumer semantics (non-normative)

For a Fractal Chat projection, `to` is the conversation peer for an outbound row. An empty message
array with `coverage._tag = "since"` is a complete empty result from that boundary. `unavailable` and
`partial` require a visible coverage warning and must not be presented as a complete empty history.
These semantics support a Chat-only projection; they do not claim that ordinary Sent history is a
generic activity feed or that typed service-principal requests belong in it.

## Typed requests

`request send` and `request reply` retain their service-principal publication records under
`resources/request-state`. Their stable idempotency and status contract is a lower-level precedent
for reserving a filename before delivery, but service principals are not Agents and their records do
not enter ordinary Sent history (`MESSAGE-R11`).

The typed request surface is design-superseded by stream events
([decision 0004](../.decisions/0004-stream-events-are-a-distinct-record-kind.md)): a reply to an
event is an ordinary `message reply`, and the `pending | replied` union derives from `in-reply-to`.
This section stays normative until the staged absorption in
[04-stream DQ-S4](../04-stream/open-questions.md) completes; the drift is fenced by
[DELTA-002](../.delta/DELTA-002-typed-request-absorption-pending.md). `MESSAGE-R11`'s separation
purpose survives the absorption: events remain excluded from ordinary Sent history by never writing
the sender ledger at all.

## Verification

`tests/message_cli.rs` injects failure after coverage, pending creation, active publication,
recipient materialization, sender-row creation, commit-node creation, head publication, pending
cleanup, and active cleanup. It covers payload/filename mismatch; missing, extra, corrupt,
unreadable, and unsupported-version head/node/row state; digest, predecessor, ordinal, count, and
genesis mismatch; valid-prefix rollback with an unexplained suffix; idempotency scope; unkeyed
response loss; symmetric external-eval exclusion; and recipient-independent reads.
`crates/st2-wire/src/message.rs` verifies the coverage tags, `to` direction, and body omission.

The catalog performance procedure is defined in the experiment record. It compares Sent with
same-sender message-ls on one captured catalog and must satisfy both the absolute and relative
`MESSAGE-R10` gates on the exact candidate commit.

Publication creates a constant number of files and atomically replaces one constant-size head, so
its history-dependent write work and storage increment are O(1). Complete read validation traverses
the linked ledger and scans exact sender-owned sets, so it is O(history).
