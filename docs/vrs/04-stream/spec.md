# Stream specification

This document specifies declared event streams. It builds on
[requirements.md](./requirements.md). Terminal delivery remains in
[`01-ding/spec.md`](../01-ding/spec.md); ordinary messages remain in
[`03-message/spec.md`](../03-message/spec.md).

## Status

Active. The declared-stream ingress, record, bounded-state, and companion
lifecycle shapes are implemented. Remaining extensions are tracked in
[open-questions.md](./open-questions.md).

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

A launched stream lowers its authored `command` or `argv` directly to one
derived exec task named `stream-<name>`, synthesized beside the derived DING
through the same seam. st2 adds no wrapper or line protocol: the adapter is
the task process and calls `st2 event emit` itself. It inherits the
derived-companion contract unchanged: launch with the agent's canonical task,
stop while held/suspended/retired/parked, its own restart accounting, crash
surfacing to the supervisor. An empty stream declaration creates only an
external ingress endpoint and no task.

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

Emitting to an undeclared stream or unknown agent is refused before writes.
Replaying `(stream, event-id)` — concurrently or across a crash — returns the
original filename with `deduplicated`; conflicting reuse of an `event-id` with
different content fails. A duplicate never re-notifies: archive receipts keep
their `03-message` authority.

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
message; new readers classify on the presence of `stream` + `event-id`.
Producer identity is `<host>.<agent>/<stream>`: the slash makes the declared
stream subordinate to the stable bus ID without colliding with the dotted
runtime task ID `<host>.<agent>.stream-<stream>`.

DING renders events as `[DING] » <from>: <subject> [id:…]` — the `»` marker is
the only DING change; classification, staged ownership, retries, and presence
gating are inherited.

## Stream state (STREAM-R05)

Per-stream durable state is
`<agent>/resources/streams/<name>/state.json`: a pending publication record
plus a constant-size ring of the last `K` event receipts. Each receipt binds
`event-id`, filename, optional `key`, and the rendered-content digest.
Publication is constant work with respect to total stream history; the
current implementation uses `K = 128` pending measurement (DQ-S3).

The ring is the entire dedup horizon. Inbox and archive files are not searched
to recover evicted identities. Replaying an `event-id` after its receipt has
fallen out of the ring creates a new event even when the earlier inbox or
archive file survives. This is the bounded-state honesty boundary.

## Supersession (STREAM-R07)

`--supersede` archives the most recent retained predecessor for the same
`(stream, key)` before publishing the successor; without `--key`, it archives
the stream-wide head — log-compaction semantics. The archive move uses the ordinary archive path, so
a DING-staged predecessor resolves through the existing archive-receipt rule:
pasted at most once ever, never re-pasted, successor delivers next. Proven:
24 supersedes in 146 ms produced one fresh poke, zero staged retries, one
unread head (`.experiments/2026-08-20-pipes-event-model-differentiation.md`).

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

`st2 stream add <name> [--agent <identity>] [--command <shell> | <adapter argv…>]`
and `st2 stream rm <name> [--agent <identity>]` edit exactly one declaration
through the persistent catalog-authoring lock with the same source-preserving,
fail-closed contract as `st2 rename` (R25 authority: self or declared
descendant; Nix-owned declarations refuse). Without `--agent`, the actor's
`--as`/`ST_AGENT` identity is also the target.

## Verification

The load-bearing proof names are recorded in [`INVARIANTS.md`](../../../INVARIANTS.md).
They cover idempotent and concurrent ingress, fail-closed admission, keyed
supersession, bounded ring honesty, the CLI/DING record shape, typed Agent Spec
lowering, and companion lifecycle. Typed request retirement remains separately
staged by DELTA-002; stream ingress does not weaken its existing invariant.

## Open design questions

Tracked with context in [open-questions.md](./open-questions.md): DQ-S3 ring
bound, DQ-S4 request absorption staging, DQ-S5 top-level shared streams, DQ-S6
parked-stream owner notification, DQ-S7 event body bounds vs issue #238, and
DQ-S8 one-shot stream completion.
