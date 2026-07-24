# st2

**One tool that renders, runs, and messages an agent network** — the runner (and, in progress, the
renderer + message bus) for the [VRS agent-spec](https://gist.github.com/myobie/0f4d1fce7259da20c1dd8cb2f61b2b53).
st2 is set to **replace convoy and smalltalk**: you declare a network as files in a folder, and st2
brings it up.

Three capabilities, monolith-first:

- **RENDER** — compile a higher-level IR (persona + harness + permissions) down to the final
  agent-spec catalog. *(in progress)*
- **RUN** — reconcile a catalog folder against reality and keep every agent's tasks running. ✅
- **MESSAGE** — a native message bus + the ding + presence + crash-loop surfacing, so agents talk with
  no external dependency. ✅ *(core complete; a couple parity CLI verbs remain before the swap)*

The **spec is a contract, not a program** (VRS R01): st2 is *a* runner, not *the* runner — any
conformant runner runs the same folder. st2 stays harness-agnostic: it runs each task's **explicit**
command verbatim and never needs to know what `claude` is.

Its load-bearing guarantees — and the test that proves each — are indexed in
**[INVARIANTS.md](INVARIANTS.md)**. Break the test, you broke the guarantee.

## The catalog folder

The folder is the whole truth — plain files, one folder per agent, holding its job spec and its
resources (inbox, plans, transcripts):

```
catalog/
  personas/<persona>.md              # render-layer sources
  agents/{host}/{id}/
    agent.kdl                        # the final job (KDL canonical; TOML/JSON allowed)
    resources/{id}/resource.md       # inbox/, planning/, sessions/, …
    archive/{id}/
```

Discovery is **content-based**: st2 slurps `agents/**/*.{kdl,toml,json}`, and a file is a job if it
carries agent-shaped content. The path supplies `host`+`id` as defaults; spec content wins; a mismatch
warns. **Dot-prefixed names are runner-state** and are skipped (R03).

### Selecting the catalog

Every catalog-aware command uses the same selection order:

1. `--catalog <path>`
2. `$CATALOG`
3. `${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog`

That makes the default network path-free:

```sh
st2 agents
st2 validate --strict
st2 pty ls
st2 up --once
```

Use `--catalog` anywhere in the command when targeting another network, for example
`st2 agents --catalog /srv/st2/catalog`. The older positional catalog arguments and bus-level
`--root` flags remain accepted for compatibility.

## The job — `agent.kdl`

```kdl
agent "fabric-claude" {
  host      "silber"
  type      "service"                // service (respawns) — the only job type; evals use `st2 eval`
  workspace "/repos/fabric"          // defaults each task's cwd
  supervisor "cos"
  restart { attempts 3; interval "60s"; delay "5s"; mode "delay" }

  pty  "agent" { command #"exec claude --permission-mode bypassPermissions 'boot'"# }  // terminal
  exec "ding"  { command #"st ding silber.fabric --identity silber.fabric-claude"# }    // terminal-free
}
```

st2 reads only the runner-normative subset — `identity`, `host`, `type`, `workspace`, `retired`,
`keep`, `supervisor`, `restart{}`, and the **`pty`/`exec` tasks** (`id`, `command`, `cwd`, `env`,
`tags`, `keep`). Everything render-only (`harness`, `model`, `role`, `persona`, `permissions`,
`transport`, `strategy`, `meta{}`) is baked into the commands by RENDER and ignored here.

- **`pty` vs `exec`** — `pty` allocates a terminal (an agent harness); `exec` is a plain process with
  **no controlling terminal** (the ding, daemons) (R09). Both run under `sh -c`.
- **cwd** — declared `cwd`, else `workspace`, else the spec dir.
- **Expansion** — st2 expands `$VAR`/`${VAR}` in `env`/`cwd`/`tags` at spawn (unset → literal token);
  `$CATALOG` = the catalog root; the `command` is expanded by `sh -c` (R11).
- **Supervisor environment** — `supervisor "lead"` is the single declaration; at spawn st2 derives
  `ST_SUPERVISOR=lead` for the agent's tasks so lifecycle hooks can notify it without a duplicate
  `env` entry.
- **`restart{}`** — `attempts` within `interval`, `delay` spacing, `mode` (`fail` = park + surface;
  `delay` = keep restarting, rate-limited). Declared, so every runner behaves identically (R16).

## Status — milestones (against the mandate)

- **M1 — RUN the v2 spec (service agents)** ✅ new catalog layout; `type`; `pty`/`exec` task split
  (exec terminal-free via direct supervision); declarative `restart{}`; cwd=workspace; `$CATALOG`.
  Reconcile on a folder-watch + timer; keep-aware GC; host-lock; decoupled Nomad-style stop.
- **M2 — MESSAGING** *(in progress)* — extract smalltalk's bus + ding into native st2.
  - **M2.1** ✅ the bus core: `st2 message send/ls/read/archive/reply` over each agent's
    `resources/inbox` (VRS §5), wire-compatible `<unix-ms>-<rand6>.md` files, catalog-native
    recipient resolution (`$CATALOG`/`$ST_AGENT` defaults).
  - **M2.2** ✅ the ding sidecar: `st2 ding <session>` watches an agent's `resources/inbox` and pokes
    its pty (`[DING] new smalltalk message: …`) on each new arrival — wire-identical to smalltalk's
    `st ding` (same line, same `pty send --seq … --seq key:return` injection), with a startup grace +
    miss-debounce so a launch race doesn't kill it.
  - **M2.3** ✅ presence status + roster: `st2 status [--set]` (a per-agent `status` file — one of
    `offline|available|busy|away|dnd`, with `unknown` derived from mtime staleness >15 min) and
    `st2 agents [--status] [--json [--enrich]]` (byte-compatible with `st agents --json`). The **ding
    refreshes** an agent's status mtime while its pty is alive (preserving the value), so a working
    agent never rots to `unknown`. (The §5.1 *planning* projection — `planning/versions`+`events` →
    `state/progress/current/blockers` — is a separate, richer view, deferred to a later milestone.)
  - **M2.4** ✅ crash-loop surfacing: when a task crash-loops past its `restart{}` policy (mode=fail)
    and st2 parks it, st2 sends a one-shot `crash-loop`-tagged message to the agent's `supervisor`
    over the native bus (deduped — once per park), so a crash-loop isn't only an stderr line an
    operator has to be watching.
  - **M2.5** ✅ parity verbs: `message thread [--tree]` (catalog-wide reply-chain walk) and `--json` on
    `message ls`/`read` (smalltalk `LsItem` shape). (`agents --json` ✅ in M2.3.) `send --priority` deferred.
- **M3 — RENDER** — `st2 render` (IR → catalog per the spec's `ir-example.md`).
  - **M3.1** ✅ the behavior-neutral spine: `st2 render <ir-dir> <catalog-dir>` compiles each IR
    `agent` (role/harness/model/persona/workspace) into `<catalog>/<host>/<identity>/agent.kdl` with
    wiring **identical to convoy** (`exec claude --permission-mode bypassPermissions … '<boot>'`,
    `ST_AGENT`/`ST_ROOT`/`PTY_ROOT` env, `st ding` on the smalltalk bus), materializes the persona as
    a git-excluded workspace overlay (`.claude/rules/convoy.md` → `@import .convoy/PERSONA.md`), and
    creates the bus dirs. Acceptance = render→run round-trip (the rendered catalog is a runnable spec).
  - **M3.2** ✅ tool-level permissions (the simple, useful-today thing): IR `permissions { tools {
    allow […]; deny […] }; spawn #false }` renders a `.claude/hooks/permissions.sh` PreToolUse gate
    that **blocks** a disallowed tool via `permissionDecision: deny` — it blocks *without prompting*,
    so an autonomous `bypassPermissions` agent never hangs (both allow-list and deny-list are safe).
    A hook-only `settings.json` registers it; every call is recorded to `.convoy/events/`. No block →
    nothing rendered → open (the swap-safe default).
    **TODO — extends later (parsed as a seam, not yet rendered):** path read/write scopes, curated
    shims (`bin/gh`), `ask`→human/supervisor routing (spec DQ5), and a deny-floor / restrictive
    `defaultMode` for true bounded-by-default.
- **M4 — EVALS run** — the native `st2 eval ./cell/` path: one readable `.kdl` + a folder, run
  folder→verdict (see `eval_spec`/`eval_run`). (The earlier `type = batch` staged executor is retired.)
- **M5 — EVALS green** — fork/adapt the evals suite onto st2; all cells green (the acceptance gate).
  - **Also required at M5 (gate before the M6 swap):** wire CI for the st2 repo and make the
    decoupling gates a **required** check with `pty` + a systemd user manager installed, so the
    load-bearing survival guarantees cannot silently regress: the Nomad-decoupling gate
    (`tests/nomad_survival.rs`, survives the supervisor's *process* death) **and** the
    transport-decoupling gate (`tests/transport_isolation.rs`, survives a *cgroup-cascade* kill of the
    transport unit — the fleet-fragility fix). Proven today, but not yet CI-enforced (no CI in the repo).
- **M6 — SWAP** — migrate the live fleet from convoy+smalltalk to st2. Two *sequenced* swaps, each
  proven independently: (1) the **renderer** swap (convoy → `st2 render`), behavior-neutral with agents
  still on the smalltalk bus (M3); (2) the **bus cutover** — smalltalk's flat `<root>/<id>/inbox` →
  st2-native unified `resources/inbox` (+ `st ding` → `st2 ding`), its own milestone needing its own
  parity/neutrality proof (M2's wire-compatible bus is the head start).

## Running as a service (headless Linux)

On a headless host (like hetz) install the supervisor as a systemd-user unit — the direct
replacement for `convoy-up.service`:

```sh
st2 service install --catalog <catalog> [--host <h>] [--memory-max-mb N] # write, enable, start
st2 service status                                               # systemctl --user status st2.service
st2 service uninstall                                            # stop, disable, remove (idempotent)
```

The unit runs `st2 up --catalog <catalog>` with `Restart=on-failure`. This is safe in a way
`convoy-up.service` never was: st2 spawns every task in its own transient scope (a **sibling** of
`st2.service`, see [`src/isolate.rs`](src/isolate.rs)), so restarting the service reaps only the
supervisor loop — the agents survive and a fresh supervisor adopts them
([`tests/nomad_survival.rs`](tests/nomad_survival.rs),
[`tests/transport_isolation.rs`](tests/transport_isolation.rs)).

**Linux-only, by design.** macOS stays **manual** — the maintainer runs `st2 up` themselves there, because a
launchd-owned process can't inherit his GUI/keychain (TCC) trust. `st2 service` bails loud on
non-systemd hosts.

## Build & try it

```sh
cargo build
cargo test                          # 162 tests
./target/debug/st2 ls --catalog examples/catalog
./target/debug/st2 up --catalog examples/catalog --host demo --once   # launch a few demo agents
./target/debug/st2 up --catalog examples/catalog --host demo          # supervise (Ctrl-C to stop)
```

The `pty` binary must be on `PATH` for the full suite: the pty-path Nomad-decoupling gate
(`tests/nomad_survival.rs`) fails rather than silently skipping without it (set `ST2_ALLOW_PTY_SKIP=1`
to skip on a dev box that has no `pty`). The transport-decoupling gate (`tests/transport_isolation.rs`)
additionally needs a systemd `--user` manager (it spawns and cgroup-kills real transient scopes) and
fails loud without it (set `ST2_ALLOW_ISOLATION_SKIP=1` to skip on a box without systemd). CI/gating
must install `pty` **and** run under a user systemd instance so both gates actually run.

See [`examples/README.md`](examples/README.md) for a full walkthrough. Stopping st2 leaves the agents
running — they are decoupled from the supervisor (Nomad model); only a `retired` spec tears one down.
This is proven with real processes (kill the runner with SIGTERM/SIGKILL → tasks survive → a fresh st2
adopts them; only `retired` kills) in [`tests/nomad_survival.rs`](tests/nomad_survival.rs).

## Usable end-to-end: render → up → talk

Run a real fleet from an IR folder — [`examples/ir/`](examples/ir/) has a worker agent (role / harness
/ model / persona / workspace, + a `permissions{}` tool gate). Cold-start demo, copy-paste:

```sh
# 0. Put st2 on PATH (one-time). Needs `claude` + `st` + `pty` on PATH too (the tools convoy uses).
cargo install --path .                  # → ~/.cargo/bin/st2

# 1. Stage the example against a fresh workspace (edit fleet.kdl's `workspace` for a real repo).
D="$HOME/st2-demo"; mkdir -p "$D/repo" "$D/ir/personas"
cp examples/ir/personas/worker.md "$D/ir/personas/"
sed "s|/replace/with/a/real/repo/path|$D/repo|" examples/ir/fleet.kdl > "$D/ir/fleet.kdl"

# 2. Render → a runnable catalog + the workspace overlay (.claude/, .convoy/; git-excluded).
st2 render "$D/ir" --catalog "$D/catalog"
st2 ls --catalog "$D/catalog"           # the runner sees a runnable 2-task agent

# 3. Supervise — the agent boots (`exec claude …`) and talks on the smalltalk bus.
#    Needs `claude` + `st` + `pty` on PATH (the same tools convoy uses).
st2 up --catalog "$D/catalog"
```

In another shell, watch + talk to it (the bus verbs read `$CATALOG`):

```sh
export CATALOG="$HOME/st2-demo/catalog"
st2 agents --host demo --json --enrich              # roster + presence
st2 message send demo.demo-worker-claude -m "hi"    # its ding pokes it; it reads + replies
st2 message thread <file> --tree                    # follow the conversation
```

Rendered agents wire **identically to convoy** (proven byte-for-byte in
[`tests/render_neutrality.rs`](tests/render_neutrality.rs)), so `st2 up` is a drop-in swap of the
*runner* — the fleet keeps talking on the same bus. Full migration map: [PARITY.md](PARITY.md).

## Messaging — `st2 message`

The bus is just files: a message is a `<unix-ms>-<rand6>.md` markdown file (YAML frontmatter + body)
written into the recipient's `<catalog>/<host>/<identity>/resources/inbox/`. The recipient is the
path; `from` is in the frontmatter; archiving is an inbox→archive rename. Nothing is mutated after
write. The root follows `--catalog`, `$CATALOG`, then the default catalog; the acting identity
defaults to `$ST_AGENT`. st2 sets both for every task it spawns, so a running agent needs no flags.

```sh
st2 message send hetz.cos-claude -m "M2.1 landed" --subject "status"   # → prints the filename
st2 message ls                       # your inbox (--archive, --count, --from <id>, --since <unix-ms>)
st2 message read <file>              # formatted view (--raw for the verbatim file)
st2 message reply <file> -m "ack"   # recipient + `re:` subject + in-reply-to derived from <file>
st2 message archive <file>          # inbox → archive
```

The **ding sidecar** turns a new message into a terminal poke. st2 keeps one running per agent (a
task alongside the agent's pty); it watches the inbox and injects `[DING] new smalltalk message: …`
into the agent's pty on each arrival:

```sh
st2 ding <pty-session> --identity <id>   # long-running; exits when the target session is gone
```

## Presence — `st2 status` / `st2 agents`

Each agent has a `status` file (sibling of `agent.kdl`) holding one word — `offline | available | busy
| away | dnd`. `unknown` is *derived*, never written: a status untouched for >15 min reads as
`unknown`, so a crashed agent stops reading as its last value. The ding keeps a live agent's mtime
fresh (preserving the value), so only genuinely-idle/gone agents age into `unknown`.

```sh
st2 status --set busy         # set your presence (atomic write); no --set prints it
st2 status <id>               # read another agent's presence
st2 agents                    # roster: id / status / name, tab-separated
st2 agents --status available # filter by state
st2 agents --json --enrich    # [{identity,status,name,lastActivity,inbox}] — byte-compatible with `st`
```
