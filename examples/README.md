# st2 example — a runnable VRS catalog

A hand-authored **catalog folder** in the VRS agent-spec format you can run end to end. This is what
RENDER *would* materialize; `st2 up` runs it knowing nothing about harnesses.

> `st2` below means `target/debug/st2` (run `cargo build` first) — or `cargo run --`.

## The folder

```
examples/catalog/
  personas/worker.md                # render-layer source (not run)
  agents/
    demo/                           # host "demo"
      planner/
        agent.kdl                   # KDL: pty agent + exec ding, restart{}, $CATALOG env
        resources/inbox/            # the agent's inbox lives with its spec
      fetcher/agent.toml            # TOML: a pty task + restart{}
      reporter/agent.json           # JSON: a pty task
    laptop/
      stranger/agent.toml           # a DIFFERENT host → skipped under --host demo
```

What each job shows:

- **`planner/agent.kdl`** — the canonical shape: a **`pty`** agent (terminal) + an **`exec`** ding
  (terminal-free, R09), a declarative **`restart{}`**, `keep #true` on the ding, and an env value
  `DATA_DIR = "$CATALOG/data/planner"` that **st2 expands at spawn**.
- **`fetcher` / `reporter`** — the same in TOML and JSON.
- **`laptop/stranger`** — host `laptop`; `st2 up --host demo` reports it as `other-host` and never
  touches it (the laptop's own st2 runs it once the folder syncs there).

> The commands are harmless heartbeat loops so the demo runs. In production the `pty` agent is
> `exec claude …` and the `exec` ding is `st ding …`; st2 runs whatever the command says, verbatim.

## Run it

**Discover:**

```
$ st2 ls examples/catalog
demo.fetcher  [service] (1 task)
    examples/catalog/agents/demo/fetcher/agent.toml
      - pty agent: while true; do echo "[fetcher] polling…"; sleep 12; done
demo.planner  [service] (2 tasks)
    examples/catalog/agents/demo/planner/agent.kdl
      - pty agent: while true; do echo "[planner] working… $(date +%T)"; sleep 10; done
      - exec ding: while true; do echo "[planner.ding] would watch resources/inbox/"; sleep 30; done
demo.reporter [service] (1 task)  …
laptop.stranger [service] (1 task)  …
```

**Reconcile once** — launch this host's agents (the exec ding is supervised terminal-free, so it is
*not* in `pty list`; `stranger` is skipped):

```
$ st2 up examples/catalog --host demo --once
reconcile pass on host 'demo':
  launched (4): demo.fetcher, demo.planner, demo.planner.ding, demo.reporter
  other-host (1): stranger
```

Run it **again** → everything is adopted, nothing relaunched:

```
$ st2 up examples/catalog --host demo --once
reconcile pass on host 'demo':
  adopted (3): fetcher, planner, reporter
  other-host (1): stranger
```

**Confirm `$CATALOG` expansion** in the planner's env, and that the exec ding is terminal-free:

```
$ PID=$(pty list --json | python3 -c "import sys,json;print(next(s['pid'] for s in json.load(sys.stdin) if s['name']=='demo.planner'))")
$ tr '\0' '\n' < /proc/$PID/environ | grep DATA_DIR
DATA_DIR=/abs/path/to/examples/catalog/data/planner

$ ls ~/.local/state/st2/demo/exec/          # the ding's pid+log (not a pty session)
demo.planner.ding.log  demo.planner.ding.pid
```

**Supervise continuously** (edit/add a spec and watch it react; Ctrl-C stops st2 but leaves the agents
running — Nomad-decoupled):

```
$ st2 up examples/catalog --host demo
```

**Tear down** when done:

```
$ for s in demo.planner demo.fetcher demo.reporter; do pty kill $s; pty rm $s; done
$ kill "$(cat ~/.local/state/st2/demo/exec/demo.planner.ding.pid)"   # the exec ding
```

## Messaging

There is none to run yet — native messaging is M2. In the running spec, `st message send` writes into
an agent's `resources/inbox/`, the `exec` ding watches it, and the sync layer delivers the folder
across hosts. st2's part is keeping the `pty` agent + `exec` ding alive.
