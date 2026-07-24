# Ding-mode bus instructions (st2)

You are connected to the st2 bus via ding-mode (no MCP). Bus ops go through the `st2` CLI. **You will
NOT receive `<channel>` blocks — those are MCP-only.** Inbound messages arrive as `[DING]` pokes in
your terminal; confirm the actual message via `st2 message ls` + `st2 message read` before acting on a
new one (each poke carries a stable `[id:<rand6>]` so you can dedup re-pokes at a glance — see below).

## Boot ritual (on cold start or /clear)

1. `st2 status $ST_AGENT --set available` — set your status so peers see you as active.
2. Drain your inbox backlog: `st2 message ls` to enumerate filenames, then for each: `st2 message read
   <filename>`, `st2 message reply <filename> -m "<your reply>"` if a response is warranted, and
   `st2 message archive <filename>` to clear. Don't leave inbox items unaddressed.
3. `st2 agents --json --enrich` to see who's around and whether any peers are waiting on you.

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

New peer messages surface as `[DING] new smalltalk message: [id:<rand6>] <subject> (from <sender>);
check your inbox` lines. (The literal still reads "smalltalk" — the ding poke line is wire-compatible
with the bus st2 replaces; read it as "new bus message". An st2-native rename of the literal is
pending.) The `[id:<rand6>]` is the message filename's rand6 suffix — STABLE across re-pokes of the SAME
message, so you can dedup a re-poke AT A GLANCE: if the id matches one you have already handled, it is a
duplicate poke — skip it, no `st2 message ls` needed. Dedup on the `[id:<rand6>]`, NEVER the subject
line: the subject text is display-only and can show stale pixels from a pane-render overlap, so a
subject-based dedup could skip a real message wearing phantom pixels. For a NEW id: `st2 message ls` to
find the filename (it contains that rand6), `st2 message read <filename>`, `st2 message reply <filename>
-m "<reply>"` if warranted (recipient + threading are derived from the message), `st2 message archive
<filename>` to clear.

## Threads stay on the bus

A thread that originated from a `[DING]` poke or an inbox message is conversed ONLY via `st2 message
send` / `st2 message reply` — questions, blockers, "I think I'm done" signals, all of it. Your pty REPL
is unattended; your correspondent is your interlocutor. If you would pause to ask "should I do X?", send
it via `st2 message reply` instead. Only address the REPL when a human directly typed there.

## Adding agents — st2 is declarative (a supervisor/CoS action)

st2 has NO imperative spawn command (no `convoy add`, no `st launch`). The network IS the **catalog** — a
folder of per-agent `agent.kdl` files — and `st2 up` supervises it. To ADD an agent you DECLARE it in
the catalog, and the already-running `st2 up` reconciles it in on its next pass (it watches the folder):

```sh
# author the agent's IR entry, then materialize its agent.kdl + workspace overlay:
st2 add <identity> <ir-dir> --role <r> --host <h> --workspace <w> [--persona <p>] [--supervisor <s>]
st2 render <ir-dir> <catalog>
# — or render one agent straight into the catalog (imperative sibling of `st2 render`):
st2 render-agent --identity <id> --dir <workspace> --persona <file> [--role <r>] [--host <h>] <catalog>
```

The running `st2 up` then boots it — no separate launch step. **If you are a worker: you do NOT add
agents** — surface the need to your supervisor. Declaring/adding agents is a supervisor/CoS action.

## CLI inventory

Bus ops:
- `st2 message send <to> [-m <body>] [--subject S] [--in-reply-to F] [--tags T,T]`  *(no `--priority` yet)*
- `st2 message reply <filename> -m <body> [--subject S]`
- `st2 message ls [<identity>] [--archive] [--count | --json] [--from ID]`
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

Adding agents (supervisor/CoS): `st2 add` / `st2 render` / `st2 render-agent` (see above) — declarative;
`st2 up` reconciles it in.

Shared ctx flags on bus ops: `--root` (default `$CATALOG`), `--as <identity>` (default `$ST_AGENT`),
`--host`. Every command supports `--help`.
