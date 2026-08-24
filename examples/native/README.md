# Canonical native agent declarations

These maintained, hand-authored declarations are the canonical starting points:

- [`agent-claude.kdl`](agent-claude.kdl) uses Claude Code's rules loader and
  native `SessionStart`, `PreCompact`, and `StopFailure` hooks.
- [`agent-codex.kdl`](agent-codex.kdl) composes the persona and bus contract
  into `AGENTS.md` and uses Codex's native `SessionStart`, `PreCompact`, and
  `Stop` hooks.
- [`agent-pi.kdl`](agent-pi.kdl) composes the persona and bus contract into
  `AGENTS.md` and delivers natively. pi has no hook mechanism of its own: st2
  injects a channel extension from the same immutable set, so this declaration
  declares no `ding` and renders nothing for delivery.

The examples use `<host>`, `<identity>`, and `<workspace>` placeholders. st2 provides `CATALOG`,
`ST_ROOT`, `PTY_ROOT`, and `ST_HOOKS` when it starts a task, so hook declarations contain no
machine-specific install paths. Copy the appropriate file outside the catalog, replace every
placeholder, assemble it and the referenced files under the publication bundle's `assets/`
directory, then publish the bundle. `role` is optional metadata; `supervisor` is optional runtime routing.
Uncomment them when the seat has an assigned role or reports to another bus identity.

## Lifecycle

st2 accepts exact canonical KDL or a create-only bundle; it does not compile human intent. Inspect
all KDL and workspace targets before publication.

Use this sequence:

```sh
st2 hooks install
st2 hooks verify
input_sha256="$(st2 agent digest --bundle <bundle>)"
st2 agent publish --catalog <catalog> --bundle <bundle> \
  --input-sha256 "$input_sha256" --expect-absent --json
st2 validate <catalog>
st2 up <catalog> --host <host> --materialize-only
st2 up <catalog> --host <host> --once
```

Hook installation is explicit and receipt-bearing. `up` and materialization only verify the
selected immutable hook set; they never refresh shared scripts. Managed settings resolve
`$ST_HOOKS/<script>` into that versioned set.

## Status discipline

All three maintained declarations load the shipped bus contract. Agents must declare `busy` before
actively executing a unit of work and return to `available` only when yielding or ready for new
work. Busy agents still receive DING. `dnd` is the only delivery hold and the sidecar does not renew
it, so an abandoned hold becomes stale after 15 minutes. st2 intentionally does not inspect any
harness's terminal pixels.

A pi seat never enters the DING path at all — a declaration carrying both `ding` and `deliver` is
refused — so its delivery rests on `pi.sendUserMessage()` and pi's own idle proof rather than on a
composer heuristic.

The `render { ... }` block is ordered. `copy`, `file`, `json-upsert`, and `ensure-line` are
boot-gating operations; a failure prevents that agent from starting. Materialization refuses any
content or mode change to a Git-tracked target before its first workspace write. A byte-identical
tracked target with the declared mode is safe and idempotent. Untracked and non-Git targets remain
writable. `git-exclude` is advisory, so a non-Git workspace or exclusion failure does not prevent a
boot.

Each content directive accepts `executable=#true`. The property selects exact mode `0755`; its
absence or false value selects exact mode `0644`. Materialization corrects mode drift even when the
bytes already match. A `copy` source mode does not affect the destination mode. An unchanged
operation is not reported as materialized.

Tracked-target detection invokes `git` and fails closed if it cannot inspect a workspace that appears
to belong to a Git worktree.

Materialization is idempotent. JSON values are deep-merged, existing unrelated
settings survive, loader lines are added once, and generated workspace files
are excluded without changing the repository's committed `.gitignore`.
