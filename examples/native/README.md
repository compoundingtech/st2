# Canonical native agent declarations

These maintained, hand-authored declarations are the canonical starting points:

- [`agent-claude.kdl`](agent-claude.kdl) uses Claude Code's rules loader and
  native `SessionStart`, `PreCompact`, and `StopFailure` hooks.
- [`agent-codex.kdl`](agent-codex.kdl) composes the persona and bus contract
  into `AGENTS.md` and uses Codex's native `SessionStart`, `PreCompact`, and
  `Stop` hooks.

The examples use `<host>`, `<identity>`, and `<workspace>` placeholders. st2 provides `CATALOG`,
`ST_ROOT`, `PTY_ROOT`, and `ST_HOOKS` when it starts a task, so hook declarations contain no
machine-specific install paths. Copy the appropriate file into
`<catalog>/agents/<host>/<identity>/agent.kdl`, replace every placeholder, and add the referenced
catalog-owned templates.

## Lifecycle

`compile-agent` is an experimental generation aid. It writes one declaration, catalog-owned
templates, and the agent's `resources/{inbox,archive,context,links}` directories without changing
the workspace. Inspect all generated KDL and workspace targets before use.

Use this sequence for hand-authored or generated declarations:

```sh
st2 validate <catalog>
st2 up <catalog> --host <host> --materialize-only
st2 up <catalog> --host <host> --once
```

The `render { ... }` block is ordered. `copy`, `file`, `json-upsert`, and `ensure-line` are
boot-gating operations; a failure prevents that agent from starting. Materialization refuses any
real change to a Git-tracked target before its first workspace write. A byte-identical tracked
target is safe and idempotent; untracked and non-Git targets remain writable. `git-exclude` is
advisory, so a non-Git workspace or exclusion failure does not prevent a boot.

Materialization is idempotent. JSON values are deep-merged, existing unrelated
settings survive, loader lines are added once, and generated workspace files
are excluded without changing the repository's committed `.gitignore`.
