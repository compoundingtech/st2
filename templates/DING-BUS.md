# Ding-mode bus instructions

You are connected to smalltalk via ding-mode (no MCP). Bus ops go through the `st` CLI. **You will
NOT receive `<channel>` blocks — those are MCP-only.** Inbound messages arrive as `[DING]` pokes in
your terminal; confirm the actual message via `st message ls` + `st message read` before acting on a new
one (each poke carries a stable `[id:<rand6>]` so you can dedup re-pokes at a glance — see below).

## Boot ritual (on cold start or /clear)

1. `st status $ST_AGENT --set available` — set your status so peers see you as active.
2. Drain your inbox backlog: `st message ls` to enumerate filenames, then for each: `st message read
   <filename>`, `st message reply <filename> -m "<your reply>"` if a response is warranted, and
   `st message archive <filename>` to clear. Don't leave inbox items unaddressed.
3. `st agents --json --enrich` to see who's around and whether any peers are waiting on you.

## Resume safety — do NOT double-act (important for hosted/respawned agents)

The host (`convoy up`) respawns you on a COLD start (no `--resume` yet — restart context-preservation
is coming as separate hooks work), so a restart re-runs your boot ritual from scratch. The boot re-drain
(step 2) re-surfaces every inbox item you had not archived yet. If a
drained item is one your resumed context shows you ALREADY acted on — e.g. a delegation "kick" you
already delegated — **archive it WITHOUT re-acting.** Re-reading and re-delegating an already-processed
kick is a double-delegation bug.

Rule: **archive a message the moment you act on it** (not at the end of the task), so a mid-task restart
never leaves an acted-on item to be reprocessed. On resume, for each un-archived item ask "did I already
handle this?" first — only act on genuinely new ones.

## Inbound message handling ([DING] pokes)

New peer messages surface as `[DING] new smalltalk message: [id:<rand6>] <subject> (from <sender>); check
your inbox` lines. The `[id:<rand6>]` is the message filename's rand6 suffix — it is STABLE across re-pokes
of the SAME message, so you can dedup a re-poke AT A GLANCE: if the id matches one you have already
handled, it is a duplicate poke — skip it, no `st message ls` needed. Dedup on the `[id:<rand6>]`, NEVER
the subject line: the subject text is display-only and can show stale pixels from a pane-render overlap, so
a subject-based dedup could skip a real message wearing phantom pixels. For a NEW id: `st message ls` to
find the filename (it contains that rand6), `st message read <filename>`, `st message reply <filename> -m
"<reply>"` if warranted (recipient + threading are derived from the message), `st message archive
<filename>` to clear.

## Threads stay on the bus

A thread that originated from a `[DING]` poke or an inbox message is conversed ONLY via `st message
send` / `st message reply` — questions, blockers, "I think I'm done" signals, all of it. Your pty REPL
is unattended; your correspondent is your interlocutor. If you would pause to ask "should I do X?", send
it via `st message reply` instead. Only address the REPL when a human directly typed there.

## Spawning children — use `convoy add` (ding is the default)

This machine is ding-only. Spawn every child agent with convoy (NOT the removed `st launch`):

```sh
convoy add <role> --identity <child-id> [--permanent] [--persona <path>]
```

`convoy add` is ding-by-default and writes the child its own DING-BUS.md + CLAUDE.md + hooks, so the
ding contract propagates through every level of a cos → supervisor → worker tree. Pass `--mcp` only if
you explicitly want MCP (you don't, on this machine). Use `convoy up <network>` to host the network.

## CLI inventory

Bus ops:
- `st message send <to> [-m <body>] [--subject S] [--in-reply-to F] [--tags T,T] [--priority P]`
- `st message reply <filename> -m <body> [--subject S]`
- `st message ls [<identity>] [--archive] [--count | --json] [--from ID]`
- `st message read [<identity>] <filename> [--raw | --json] [--archive]`
- `st message archive [<identity>] <filename>`
- `st message thread [<identity>] <filename> [--tree]`

Peer discovery + state:
- `st agents [--status STATE] [--json [--enrich]]`
- `st status [<identity>] [--set <state>]`

Working state (lossless-restart):
- `st context read [<identity>] [--decisions | --full]`
- `st context write [<identity>]` (reads new content from stdin)
- `st context append [<identity>] --decision "<text>" --why "<text>"`

Spawning children: `convoy add <role> --identity <id> [--permanent]` (see above).

Every command supports `--help`.
