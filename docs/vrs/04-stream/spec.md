# Stream specification

This document specifies declared event streams. It builds on
[requirements.md](./requirements.md). Terminal delivery remains in
[`01-ding/spec.md`](../01-ding/spec.md); ordinary messages remain in
[`03-message/spec.md`](../03-message/spec.md).

## Status

Draft. The shapes below are the interview-settled design over the executable
prototypes in [`.experiments/`](./.experiments/); field names and on-disk
layouts may still move during implementation, the open questions are tracked
in [open-questions.md](./open-questions.md).

## Scope

This specification defines the `stream` declaration, its lowering to a derived
companion, the `st2 event emit` ingress boundary, the event record and its
frontmatter, per-stream dedup state, and supersession. It does not define
scheduled work (the reserved `schedule` node, root spec DQ1), message
threading (issue #49), or the staged retirement of the typed request/reply
envelopes (tracked as a DQ below).

## Overview

```text
            agent "demo" {
              stream "gh-ci" { command "gh-ci-watch --repo st2" }
              stream "webhook" {}
            }
                   |                                +----------------+
        lowering   v                                | external       |
   derived exec task demo.stream-gh-ci              | producer       |
   supervises adapter, restart policy,              +-------+--------+
   parks alone, stops with the agent                        |
                   |                                        |
                   |  adapter line of work                  |
                   v                                        v
        st2 event emit dev3.demo --stream gh-ci   st2 event emit dev3.demo
            --event-id run-812 --key pr-42            --stream webhook ...
            --supersede --subject 'CI: failure'
                   |
                   v
        (stream, event-id) dedup ring ── duplicate → original filename, no DING
                   |
                   v
        resources/inbox/<unix-ms>-<rand6>.md    (ordinary record + event keys)
                   |
                   v
        unchanged delivery: DING (» glyph) / MCP push / app-server
```

## Declaration and lowering (STREAM-R01, STREAM-R08)

```kdl
agent "demo" {
  stream "gh-ci" {
    command "gh-ci-watch --repo compoundingtech/st2"
  }
  stream "tick" {
    argv "tick-source" "--daily"
  }
  stream "webhook" {}
}
```

Exactly one of `command` (runs under `sh -c`) or `argv`, or neither (external
ingress endpoint). No other children in v1; `every` remains rejected with its
own diagnostic (STREAM-T03). Stream names are lowercase ASCII alphanumerics
and inner hyphens, starting and ending alphanumeric, and share the per-agent
uniqueness space with task names; `stream "x"` and a task named `stream-x`
collide at validation.

A command-bearing stream lowers to one derived exec task named
`stream-<name>`, synthesized beside the derived DING through the same seam and
late-bound to the running st2 binary each reconcile pass. It inherits the
derived-companion contract unchanged: launch with the agent's canonical task,
stop while held/suspended/retired/parked, its own restart accounting, crash
surfacing to the supervisor. The adapter process calls `st2 event emit`
itself; there is no line-protocol runner in between.

## Ingress boundary (STREAM-R03, STREAM-R04)

```text
st2 event emit <host>.<agent>
  --stream <name>            # must be declared on the recipient
  --event-id <id>            # mandatory, producer-supplied
  [--key <key>]              # grouping axis for supersession
  [--supersede]              # archive unread predecessor for (stream, key)
  [--subject <line>]
  [--json]                   # receipt: recipient, filename, created|deduplicated
  [body on stdin]
```

Emitting to an undeclared stream or unknown agent is refused before writes,
and so is a recipient whose desired state is not running — suspension means
eyes closed for external producers too (STREAM-R09): the emit returns a typed
refusal rather than accumulating events a suspended agent will wake to.
Replaying `(stream, event-id)` — concurrently or across a crash — returns the
original filename with `deduplicated` while that identity remains in the
stream's retained receipt ring. Conflicting reuse with different content
fails within the same horizon. Once an identity is evicted, ingress honestly
treats it as new; inbox and archive files are not searched as an unbounded
secondary index. An archive receipt keeps its `03-message` authority for a
known filename, so crash recovery never restores that filename to the inbox.

## Event record (STREAM-R06)

An event is an ordinary inbox file (`<unix-ms>-<rand6>.md`, same directories,
same archive semantics) whose frontmatter carries the event keys:

```markdown
---
from: dev3.demo/gh-ci
stream: gh-ci
event-id: run-812
key: pr-42
subject: 'CI: failure on PR #42'
---
{"conclusion":"failure","run":812,"pr":42}
```

`parse_message` ignores unknown frontmatter keys, so old readers see a normal
message; new readers classify on the presence of `stream` + `event-id`. The
exact `from` grammar for a nested stream's producer identity is DQ-S1.

DING renders events as `[DING] » <from>: <subject> [id:…]` — the `»` marker is
the only DING change; classification, staged ownership, retries, and presence
gating are inherited.

## Stream state (STREAM-R05)

Per-stream durable state is one constant-size record under the owning agent's
resources (exact path DQ-S2): a ring of the last `K`
`(event-id → filename, key, content digest)` receipts plus a single in-flight
publication reservation. The ring is the entire deduplication,
conflicting-content-detection, and supersession-lookup horizon: a replay hit
answers in O(1), while a miss is accepted as a new identity without scanning
the unread inbox or archive.

Under the per-stream lock, every emit first reconciles an existing
reservation. If its chosen filename exists as a regular file in the inbox or
archive, its stored identity, filename, key, and content digest are promoted
to the retained receipt ring and the reservation is cleared. If the filename
exists in neither location, the unpublished reservation is abandoned and
cleared. The current event is then evaluated against the resulting ring; no
producer replay and no retained payload bytes are required to unblock the
stream.

For a new identity, emit persists a reservation with its identity, chosen
filename, key, content digest, and supersession intent; it then materializes
that filename and finally atomically clears the reservation while inserting
the receipt. A crash before materialization leaves a safely abandonable
intent; a crash after materialization leaves a filename whose presence is
enough to finalize the receipt on the next emit. This prevents both an
unrecorded inbox file and a receipt for a file that never materialized, while
adding only one bounded state slot.

Nothing is chained, hashed, or validated O(history), and publication work is
independent of retained stream history. `K` is 128 pending measurement
(DQ-S3). An ordinary archive receipt remains authoritative for its known
filename, but random inbox/archive filenames do not form a reverse index from
`(stream, event-id)` and are not searched as one.

## Supersession (STREAM-R07)

`--supersede` collapses the stream to its latest unread event per `key`
(absent `key`: per stream) — log-compaction semantics — in wakeup-safe order:
the successor is published first, then the newest matching receipt among the
retained `K` whose filename still exists in the inbox is archived. The lookup
examines at most `K` receipt candidates and performs no unread-backlog scan;
an older predecessor outside the horizon is not compacted. A crash between
the two steps leaves both events unread — the failure bias is a duplicate
wake, never a lost one — and the next emit or a supersede retry completes the
compaction idempotently. This is the ordering the differentiation experiment
proved (steps 3–4 materialize the successor before touching the predecessor).
The archive move uses the ordinary archive path, so a DING-staged predecessor
resolves through the existing archive-receipt rule: pasted at most once ever,
never re-pasted, successor delivers next. Proven: 24 supersedes in 146 ms
produced one fresh poke, zero staged retries, one unread head
(`.experiments/2026-08-20-pipes-event-model-differentiation.md`).

## Waits are standing feeds (doctrine, STREAM-R01 + STREAM-R07)

"Wake me when X finishes" — a build in a pty, CI on a PR, a human decision —
is modeled as a standing stream with the individual wait as `key`, never as a
per-wait subscription:

```kdl
agent "demo" {
  stream "pty" { command "pty-lifecycle-watch" }
}
```

One supervised adapter watches all of the agent's pty sessions and emits
`--key <session>` events on phase changes (superseding) plus a terminal
exit event. Starting a new build requires no declaration change and leaves
nothing to clean up; the same shape serves CI (`gh-ci-watch` discovers the
agent's PRs itself, `key` per PR) and approval watching. This is deliberate:
the task model has no run-to-completion lifecycle (`TaskLifecycle` is
`Service | AdoptOnly`), so an adapter that exits on success would relaunch
into a flap and park. A rare genuinely one-off custom wait uses
`st2 stream add`/`rm` and an adapter that keeps its process alive; first-class
completion semantics are deferred to DQ-S8 and, if ever needed, belong in the
task model rather than a stream-level flag.

## Authoring (STREAM-R02)

`st2 stream add <name> [--command …]` / `st2 stream rm <name>` edit exactly
one declaration through the persistent catalog-authoring lock with the same
source-preserving, fail-closed contract as `st2 rename` (R25 authority: self
or declared descendant; Nix-owned declarations refuse).

## Verification plan

The invariant rows this subsystem must add or keep green when implementation
lands: idempotent event ingress (replacing the request-transport row when the
absorption completes), bounded stream state, supersession-vs-DING safety, and
the derived-companion row extended to stream tasks. The prototype tests in
`.experiments/` name the intended proofs; they become `tests/event_e2e.rs`,
`tests/stream_lifecycle.rs`, and `agent-spec` declaration tests.

## Open design questions

Tracked with context in [open-questions.md](./open-questions.md): DQ-S1
producer-identity grammar, DQ-S2 state path, DQ-S3 ring bound, DQ-S4 request
absorption staging, DQ-S5 top-level shared streams, DQ-S6 parked-stream
owner notification, DQ-S7 event body bounds vs issue #238, DQ-S8 one-shot
stream completion.
