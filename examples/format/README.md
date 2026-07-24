# Canonical compact catalog format

These are the current declarative agent shapes emitted by `st2 compile-agent`
(also available as `st2 build-agent`):

- [`agent-claude.kdl`](agent-claude.kdl) uses Claude Code's rules loader and
  native `SessionStart`, `PreCompact`, and `StopFailure` hooks.
- [`agent-codex.kdl`](agent-codex.kdl) composes the persona and bus contract
  into `AGENTS.md` and uses Codex's native `SessionStart`, `PreCompact`, and
  `Stop` hooks.

The examples use `<host>`, `<identity>`, and `<workspace>` placeholders. st2
provides `CATALOG`, `ST_ROOT`, `PTY_ROOT`, and `ST_HOOKS` when it starts a task,
so generated hook declarations contain no machine-specific install paths.

## Lifecycle

`compile-agent` is generation-only: it writes the compact `agent.kdl`,
catalog-owned templates, and the agent's
`resources/{inbox,archive,context,links}` directories. It does not change the
workspace.

Use the same sequence for generated and hand-authored declarations:

```sh
st2 validate <catalog>
st2 up <catalog> --host <host> --materialize-only
st2 up <catalog> --host <host> --once
```

The `render { ... }` block is ordered. `copy`, `file`, `json-upsert`, and
`ensure-line` are boot-gating operations; a failure prevents that agent from
starting. `git-exclude` is advisory, so a non-Git workspace or exclusion
failure does not prevent a boot.

Materialization is idempotent. JSON values are deep-merged, existing unrelated
settings survive, loader lines are added once, and generated workspace files
are excluded without changing the repository's committed `.gitignore`.
