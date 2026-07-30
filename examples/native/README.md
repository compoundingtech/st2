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
catalog-owned templates. `role` is optional metadata; `supervisor` is optional runtime routing.
Uncomment them when the agent has an assigned role or reports to another bus identity. In the Codex
declaration, replacing `<workspace>` in both places keeps its command-local project trust key
byte-identical to the declared workspace. st2 treats that command as opaque; the trust flag is a
Codex launch convention, not part of generic catalog validation.

## Lifecycle

`compile-agent` is an experimental generation aid. It writes one declaration, catalog-owned
templates, and the agent's `resources/{inbox,archive,context,links}` directories without changing
the workspace. Inspect all generated KDL and workspace targets before use.

Use this sequence for hand-authored or generated declarations:

```sh
st2 hooks install
st2 hooks verify
st2 validate <catalog>
st2 up <catalog> --host <host> --materialize-only
st2 up <catalog> --host <host> --once
```

Hook installation is explicit and receipt-bearing. `up` and materialization only verify the
selected immutable hook set; they never refresh shared scripts. Managed settings resolve
`$ST_HOOKS/<script>` into that versioned set.

## Status discipline

Both maintained declarations load the shipped bus contract. Agents must declare `busy` before
actively executing a unit of work and return to `available` only when yielding or ready for new
work. Busy agents still receive DING. `dnd` is the only delivery hold and the sidecar does not renew
it, so an abandoned hold becomes stale after 15 minutes. st2 intentionally does not inspect either
harness's terminal pixels.

The `render { ... }` block is ordered. `copy`, `file`, `json-upsert`, and `ensure-line` are
boot-gating operations; a failure prevents that agent from starting. Materialization refuses any
real change to a Git-tracked target before its first workspace write. A byte-identical tracked
target is safe and idempotent; untracked and non-Git targets remain writable. `git-exclude` is
advisory, so a non-Git workspace or exclusion failure does not prevent a boot.

Tracked-target detection invokes `git` and fails closed if it cannot inspect a workspace that appears
to belong to a Git worktree.

Materialization is idempotent. JSON values are deep-merged, existing unrelated
settings survive, loader lines are added once, and generated workspace files
are excluded without changing the repository's committed `.gitignore`.
