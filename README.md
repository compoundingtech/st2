# st2

st2 runs a declarative network of Codex and Claude agents from one catalog. It owns process
reconciliation, native messages, safe terminal DING delivery, presence, durable context, workspace
materialization, and explicit teardown.

Hand-authored KDL is the canonical interface. `st2 compile-agent` is experimental and must be
reviewed before its output is materialized.

## Install

Prerequisites:

- Rust and Cargo;
- `pty` on `PATH`;
- at least one supported harness on `PATH`: `codex` or `claude`;
- Git when a declaration materializes workspace files.

From a checkout:

```sh
cargo install --path . --locked
st2 --help
pty --help
st2 hooks install
st2 hooks verify
```

The standard catalog is:

```text
${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog
```

Every catalog-aware command accepts `--catalog`; otherwise st2 uses `$CATALOG`, then that standard
location.

Lifecycle hooks are installed only by the explicit `st2 hooks install` command. The installer
publishes an immutable content-addressed set, then atomically selects it with a receipt. `st2 up`
and workspace materialization verify that receipt but never create, refresh, or rewrite hooks.
An intentional rollback to an older set requires `st2 hooks install --allow-downgrade`.

`ST_HOOKS` overrides the machine-local hook root for installation, verification, and managed tasks.
During materialization, hook commands such as `$ST_HOOKS/codex-stop.sh` resolve to the selected
immutable set, so rendered settings are versioned without embedding a machine-specific root in the
declaration.

## Author a native agent

Start from the maintained [Codex](examples/native/agent-codex.kdl) or
[Claude](examples/native/agent-claude.kdl) declaration:

```sh
export CATALOG="${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog"
mkdir -p "$CATALOG/agents/<host>/<identity>" "$CATALOG/_templates"
cp examples/native/agent-codex.kdl "$CATALOG/agents/<host>/<identity>/agent.kdl"
${EDITOR:-vi} "$CATALOG/agents/<host>/<identity>/agent.kdl"
```

Replace `<host>`, `<identity>`, `<workspace>`, and `<boot prompt>`. Add every file referenced by
`copy` under `$CATALOG/_templates`.

The compact declaration shape is:

```kdl
agent "<identity>" {
  host "<host>"
  workspace "<workspace>"
  // Optional metadata:
  // role "worker"
  // supervisor "<supervisor-bus-id>"
  env { ST_AGENT "<host>.<identity>" }
  command #"exec codex --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust '<boot prompt>'"#
  ding

  render {
    copy "_templates/<host>.<identity>.AGENTS.md" "AGENTS.md"
    json-upsert ".codex/hooks.json" #"""
{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"$ST_HOOKS/codex-session-start.sh","timeout":5}]}]}}
"""#
    git-exclude "AGENTS.md" ".codex/hooks.json"
  }
}
```

st2 provides `CATALOG`, flat native `ST_ROOT`, local `PTY_ROOT`, `ST_AGENT`, and `ST_HOOKS` to the
task. Declarations should not contain machine-specific install paths.

## Validate and materialize

Gate the declaration before starting anything:

```sh
st2 hooks verify
st2 validate --catalog "$CATALOG"
st2 up --catalog "$CATALOG" --host <host> --materialize-only
```

Materialization simulates all content operations before writing. It refuses any real change to a
Git-tracked target, including `AGENTS.md`; byte-identical tracked content is accepted. Inspect the
declared targets and keep generated overlays untracked. Detection invokes `git` and fails closed if
the executable is unavailable or a workspace that appears to be a worktree cannot be inspected.

`git-exclude` is advisory. `copy`, `file`, `json-upsert`, and `ensure-line` are boot-gating.

## Run

On headless Linux, install the systemd user service:

```sh
st2 service install --catalog "$CATALOG" --host <host>
st2 service status
```

Each task runs in its own transient scope, so restarting the supervisor does not kill live agents.

On macOS, use a manual one-shot reconcile:

```sh
st2 up --catalog "$CATALOG" --host <host> --once
```

There is intentionally no resident macOS service path.

For a foreground supervisor on any host:

```sh
st2 up --catalog "$CATALOG" --host <host>
```

## Messages, DING, status, and context

Inside a managed task, `CATALOG`, `ST_ROOT`, and `ST_AGENT` are already set:

```sh
st2 status "$ST_AGENT" --set available
st2 message send <recipient> --subject "work" -m "Please handle the scoped task."
st2 message ls
st2 message read <filename>
st2 message reply <filename> -m "Handled."
st2 message archive <filename>
st2 agents --json --enrich
st2 context read --full
```

For a catalog-backed agent, every native bus operation resolves the same agent directory used by
the roster: presence is `<agent-dir>/status`, while unread messages, archive receipts, context, and
links live under `<agent-dir>/resources/`. The flat `<root>/<identity>` layout remains only as the
intentional catalog-less fallback used by isolated folder evals.

Adopters should cut directly to this native layout rather than stage through a retired compatibility
transport. Before launching a migrated identity, install and verify hooks, validate and materialize
its hand-authored declaration, stop the predecessor transport, and decide how any unread legacy
backlog will be archived or forwarded. Never run old and native DING owners concurrently for the same
identity.

Native DING watches the recipient inbox and safely stages:

```text
[DING] new st2 message: [id:<rand6>] <subject> (from <sender>); check your inbox
```

Consumers must key on the `[DING]` prefix and stable id, not descriptive words. Codex delivery
bracketed-pastes without Return, re-inspects the bottom-most composer, and submits only the exact
staged notice. For the exact idle `Create a plan? … esc dismiss` prompt only, st2 confirms the same
modal twice, sends Escape without Return, and re-inspects before delivery. Every other modal, active
turn, draft, `busy`, or `dnd` state defers. After a DING-sidecar restart, st2 can resume one notice
only when the visible bottom composer starts with `[DING]` and its stable id names exactly one
still-unread seeded inbox message. Only the description before `[id:…]` may differ across a rolling
binary window; the normalized tail must still match that message's subject, sender/defaults, and
`check your inbox`. The whole visible composer must then survive the normal exact-text guard. st2
never replays the seeded backlog.

## Cleanup

Explicit teardown is the only operation that ends declared tasks:

```sh
st2 down --catalog "$CATALOG" --host <host>
```

On Linux, remove the supervisor service after teardown when it is no longer wanted:

```sh
st2 service uninstall
```

## Command surface

```text
ls, up, down, validate, doctor
message, ding, agents, status, context, resource
env, pty, shell, pretrust
hooks, service, eval
compile-agent (experimental)
completions
```

`st2 completions <shell>` emits completions from the live command tree. No generated completion or
manpage tree is committed.

## Clean-room verification

The test suite builds a temporary `PATH` containing only the current `st2` binary, required Git,
and `pty`, `codex`, and `claude` shims. It installs and verifies a scratch hook receipt, instantiates
both maintained hand-authored KDL examples in fresh Git workspaces, validates and materializes them
twice, and verifies the help/doctor/native authoring surface without any retired binary:

```sh
cargo test --test native_only --all-features
```

Run the complete local gate with:

```sh
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

## Eval corpus

The Codex evidence ledger is pinned at
[`67b45d2694ac40762b09f51bf625d092ab68de74`](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/HARNESS-MATRIX.md).

Current native Codex examples:

- [`license-mit-codex`](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/cells/license-mit-codex/license-mit-codex.kdl):
  `bin/check-codex-native.sh cells/license-mit-codex` is the free static gate. The opt-in paid run is
  `st2 eval ./cells/license-mit-codex/ --keep`; pinned evidence is 6/6 PASS in 1m39s.
- [`signal-rename-codex`](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/cells/signal-rename-codex/signal-rename-codex.kdl):
  `bin/check-codex-native.sh cells/signal-rename-codex` is the free static gate. The opt-in paid run
  is `st2 eval ./cells/signal-rename-codex/ --keep`; pinned evidence is 6/6 PASS in 8m07s.

The pinned [native static checker](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/bin/check-codex-native.sh)
does not consume model usage.

The Claude native-readiness ledger is pinned at
[`a52b68dfe4bdb65a3bb6a1ba51c674476cf197df`](https://github.com/compoundingtech/evals/blob/a52b68dfe4bdb65a3bb6a1ba51c674476cf197df/CLAUDE-NATIVE-READINESS.md).
Its current event-first native examples and gates are immutable at
[`56b86a04837dc176f1a53d9f90dee3f3a7e57499`](https://github.com/compoundingtech/evals/commit/56b86a04837dc176f1a53d9f90dee3f3a7e57499):

- [`ding-reply`](https://github.com/compoundingtech/evals/blob/56b86a04837dc176f1a53d9f90dee3f3a7e57499/cells/ding-reply/ding-reply.kdl):
  one Claude seat with a native bare `ding`.
- [`signal-rename`](https://github.com/compoundingtech/evals/blob/56b86a04837dc176f1a53d9f90dee3f3a7e57499/cells/signal-rename/signal-rename.kdl):
  one Claude supervisor and three specialists, each with a native bare `ding`.

Run `bin/check-claude-native.sh` and `bin/check-claude-reset.sh` from that tree for the free static
acceptance. The gates reject polling language and legacy bus declarations, require event-first DING
guidance, and prove repeatable clean fixture resets. They do not invoke a model. A current-build
`st2 eval` model run remains pending explicit authorization, so these examples have static acceptance
but no current native live-smoke claim.

[`team-standup`](https://github.com/compoundingtech/evals/blob/a52b68dfe4bdb65a3bb6a1ba51c674476cf197df/cells/team-standup/team-standup.kdl)
remains a legacy reference to the retired generated-declaration flow, not a current native example.
