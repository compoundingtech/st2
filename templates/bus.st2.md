# st2 bus instructions

You are connected to the st2 bus. Bus ops go through the `st2` CLI. Maintained providers attach a
bounded FIFO batch with complete message bodies. A short `[DING]` metadata poke remains the generic
fallback for unknown/custom providers.

## Boot ritual (on cold start or /clear)

1. `st2 status $ST_AGENT --set available` — set your status so peers see you as active.
2. Drain your inbox backlog: use the body-bearing batch already delivered by a maintained provider,
   or run `st2 message delivery` once. Handle every included message, then run all warranted
   `st2 message reply` and `st2 message archive` commands together in one tool invocation. A bounded
   overflow is delivered as the next batch; an exceptional oversized head explicitly tells you to
   use `st2 message read <filename>`. Don't leave handled inbox items unarchived.
3. `st2 agents --json --enrich` to see who's around and whether any peers are waiting on you.
4. If the backlog or durable context leaves work to execute, set `busy` before acting on it. Return
   to `available` only when yielding or ready for new work.

## Status discipline

Status tells peers whether you are working or ready. DING deliberately does not inspect terminal
pixels and does not suppress notifications merely because you are `busy`:

- Set `busy` immediately before actively executing a unit of work, including its tool calls,
  commands, edits, and verification. Keep it `busy` until you reach a safe yield point.
- Set `available` only while ready to receive new work or when yielding back after a completed or
  blocked unit. Do not leave yourself `available` while working.
- Set `dnd` only as an explicit operator/agent hold. Fresh `dnd` is the only status that defers
  DING. The sidecar does not refresh `dnd`, so an abandoned hold ages to `unknown` after 15 minutes.

Use `st2 status "$ST_AGENT" --set busy` before work and
`st2 status "$ST_AGENT" --set available` when yielding. The live DING sidecar refreshes non-DND
presence without changing its value.

## Resume safety — do NOT double-act (important for hosted/respawned agents)

The host (`st2 up`) respawns you on a COLD start, so a restart re-runs your boot ritual from scratch.
The boot re-drain (step 2) re-surfaces every inbox item you had not archived yet. If a drained item is
one your resumed context shows you ALREADY acted on — e.g. a delegation "kick" you already delegated —
**archive it WITHOUT re-acting.** Re-reading and re-delegating an already-processed kick is a
double-delegation bug.

Rule: **archive a message the moment you act on it** (not at the end of the task), so a mid-task restart
never leaves an acted-on item to be reprocessed. On resume, for each un-archived item ask "did I already
handle this?" first — only act on genuinely new ones.

## Inbound message handling ([DING] pokes)

When a maintained provider attaches a `[DING] st2 inbox batch`, handle that FIFO prefix directly,
then batch the existing reply/archive commands in one tool invocation. A short
`[DING] ... [id:<rand6>]` without bodies is the generic fallback: run
`st2 message delivery` once instead of listing and reading files one by one. The id is
stable across re-pokes; dedup on it, never the subject. Set `busy` before executing message work.

## Threads stay on the bus

A thread that originated from a `[DING]` poke or an inbox message is conversed ONLY via `st2 message
send` / `st2 message reply` — questions, blockers, "I think I'm done" signals, all of it. Your pty REPL
is unattended; your correspondent is your interlocutor. If you would pause to ask "should I do X?", send
it via `st2 message reply` instead. Only address the REPL when a human directly typed there.

## Adding agents — hand-authored KDL is canonical

The network is the catalog: one `agents/<host>/<identity>/agent.kdl` declaration per agent. A
supervisor or CoS renders the exact declaration outside the catalog, publishes it transactionally,
inspects its workspace targets, and lets the running `st2 up` reconcile it on the next pass:

```sh
st2 agent publish --catalog "$CATALOG" --spec ./agent.kdl --expect-absent --json
st2 hooks verify
st2 validate --catalog "$CATALOG"
st2 up --catalog "$CATALOG" --host <host> --materialize-only
```

Canonical KDL or a create-only publication bundle is the authoring boundary. st2 does not compile
human intent into a declaration. Inspect every `render {}` target before materialization. Workers do
not add agents; surface the need to your supervisor.

## CLI inventory

Bus ops:
- `st2 message send <to> [-m <body>] [--subject S] [--in-reply-to F] [--tags T,T]`  *(no `--priority` yet)*
- `st2 message reply <filename> -m <body> [--subject S]`
- `st2 message ls [<identity>] [--archive] [--count | --json [--include-body]] [--from ID]`
- `st2 message delivery [<identity>]` (read-only bounded body-bearing FIFO view)
- `st2 message read [<identity>] <filename> [--raw | --json] [--archive]`
- `st2 message archive [<identity>] <filename>`
- `st2 message thread [<identity>] <filename> [--tree]`

Peer discovery + state:
- `st2 agents [--status STATE] [--json [--enrich]]`
- `st2 status [<identity>] [--set <state>]`

Working state (lossless-restart):
- `st2 context read [<identity>] [--decisions | --full]`
- `st2 context write [<identity>]` (reads new content from stdin)
- `st2 context append [<identity>] --decision "<text>" --why "<text>"`

Resources:
- `st2 resource add <url> [--title T] [--tag T,T] [--relation R]`
- `st2 resource ls [<identity>]` · `st2 resource read [<identity>] <ref>` · `st2 resource remove [<identity>] <ref>`

Machine lifecycle hooks (explicit; `up` never installs or refreshes them):
- `st2 hooks install [--allow-downgrade]`
- `st2 hooks verify`

Agent declarations: `st2 agent publish`; `st2 validate`; `st2 up --materialize-only`; `st2 up`.

Catalog selection on every catalog-aware command: `--catalog <path>` → `$CATALOG` →
`${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`. Bus ops retain `--root` as an explicit
flat-bus/catalog override. Other shared bus flags: `--as <identity>` (default `$ST_AGENT`), `--host`.
Every command supports `--help`.
