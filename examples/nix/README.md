# nix → catalog — a second renderer targeting the st2 contract

An **illustrative** nix derivation that renders agent-spec files into an st2 **catalog**. It's the
render-side companion to [`../ir`](../ir) (convoy-style IR): a *different* renderer producing the
*same* generic spec st2 runs.

## The Layers idea

The VRS spec separates **RENDER** from **RUN**:

- **RENDER** is a compiler — it knows harnesses (`claude`/`codex`), personas, models, hooks. It lowers
  high-level agent data into concrete per-agent specs.
- **RUN** is st2 — a dumb reconciler over a catalog folder. It reads already-rendered `agent.kdl`
  files (each carrying an explicit `command`) and keeps them running. It never needs to know what
  "claude" is.

Because the contract between them is just *the catalog on disk*, **any** renderer works. convoy is one
(TypeScript, IR → catalog). This example is another: **nix** projecting agent-spec files into a catalog
so that Johannes's nix-managed fleet can be run by st2 with nothing convoy-specific in the loop.

```
         RENDER (any of many)                    RUN (one)
   ┌────────────────────────────┐         ┌──────────────────┐
   │  convoy   (IR → catalog)   │         │                  │
   │  nix      (this example)   │  ─────▶ │   st2 up <cat>    │
   │  …your renderer…           │  catalog│                  │
   └────────────────────────────┘         └──────────────────┘
                    │
                    ▼
              st2 validate <cat>   ← the contract-check every renderer gates on
```

## What it emits

`catalog.nix` renders two agents — a **CoS/root** agent and a **worker** it supervises — into:

```
result/
  demo/
    demo-cos-claude/
      agent.kdl                    # the runner-normative spec (§2): identity, host, type,
      resources/{inbox,archive}/   #   workspace, supervisor, pty "agent" + exec "ding"
    demo-worker-claude/
      agent.kdl
      resources/{inbox,archive}/
  overlay/<identity>/PERSONA.md    # staged persona overlay
  activate.sh                      # materializes the overlay into each workspace (impure step)
```

The `agent.kdl` is byte-shaped exactly as st2 consumes it — the harness (`exec claude …`), the ding
sidecar, `ST_AGENT`/`ST_ROOT`/`PTY_ROOT`, and a literal `$CATALOG` st2 expands at spawn. Render-only
inputs (harness, model, persona) are baked into the `command`, never emitted as spec keys — that is
what keeps st2 render-agnostic.

## Pure vs impure — the one nix nuance

A nix derivation is pure: it can only write into `$out`. So the **catalog** (agent.kdl + bus dirs) is
emitted purely into `$out`, while the **workspace overlay** (`.claude/rules` + `.convoy/PERSONA.md`,
which live in the agent's *repo*, outside the store) is *staged* in `$out/overlay/` and materialized by
`activate.sh` at deploy time — the same split home-manager makes between a built profile and its
activation.

## Use it

```sh
nix-build catalog.nix --argstr workspace /abs/path/to/a/repo
st2 validate ./result        # confirm the render hit st2's contract (0 errors → good)
./result/activate.sh         # materialize the persona overlay into the workspace
st2 up ./result              # supervise: agents boot (exec claude) + talk on the bus
```

`st2 validate` is the handshake that makes this a *shared* contract: the nix build gate runs it, and a
render change that breaks the spec (a typo'd `type`, a relative path, a dangling supervisor) is caught
before anything runs. The exact catalog this file emits validates clean — 0 errors, 0 warnings.

> Illustrative, not built in CI here (this repo has no nix toolchain). The emitted `agent.kdl` format
> is verified against `st2 validate`; treat `catalog.nix` as a shape to adapt, like `../ir/fleet.kdl`.
