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
```

The standard catalog is:

```text
${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog
```

Every catalog-aware command accepts `--catalog`; otherwise st2 uses `$CATALOG`, then that standard
location.

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
  role "worker"
  workspace "<workspace>"
  supervisor "<supervisor-bus-id>"
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

Native DING watches the recipient inbox and safely stages:

```text
[DING] new st2 message: [id:<rand6>] <subject> (from <sender>); check your inbox
```

Consumers must key on the `[DING]` prefix and stable id, not descriptive words. Codex delivery
bracketed-pastes without Return, re-inspects the bottom-most composer, and submits only the exact
staged notice. For the exact idle `Create a plan? … esc dismiss` prompt only, st2 confirms the same
modal twice, sends Escape without Return, and re-inspects before delivery. Every other modal, active
turn, draft, `busy`, or `dnd` state defers.

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
service, eval
compile-agent (experimental)
```

The project ships no completion or manpage generator.

## Clean-room verification

The test suite builds a temporary `PATH` containing only the current `st2` binary plus `pty`,
`codex`, and `claude` shims. It verifies the supported help/doctor/native authoring surface without
any retired binary:

```sh
cargo test --test native_only --all-features
```

Run the complete local gate with:

```sh
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

## Eval corpus

The evidence ledger is pinned at
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

Claude corpus references are historical and are not native-current examples:

- [`ding-reply`](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/cells/ding-reply/ding-reply.kdl):
  free syntax gate `bash -n cells/ding-reply/judges/*.sh`; authoritative run
  `st2 eval ./cells/ding-reply/ --keep`, opt-in paid.
- [`team-standup`](https://github.com/compoundingtech/evals/blob/67b45d2694ac40762b09f51bf625d092ab68de74/cells/team-standup/team-standup.kdl):
  free syntax gate `bash -n cells/team-standup/judges/*.sh`; authoritative run
  `st2 eval ./cells/team-standup/ --keep`, opt-in paid.

There is currently no native-current Claude eval KDL or free authoritative folder-eval parser.
Claude conversion, a native static gate, and an explicitly authorized current-build run remain open.
