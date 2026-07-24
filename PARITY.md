# Migration parity matrix

The done-condition for migration: **st2 must do everything convoy + smalltalk do today** on the new
agent-spec catalog layout, so we migrate without losing what we have. Each row is a **claim** (st2 can
do X); a `DONE` row must be backed by a real test/verification (same rule as [INVARIANTS.md](INVARIANTS.md)).

Parity is **smart, not 1:1** (the maintainer): the bar is (1) a running system — render → up → message →
agents coordinate end-to-end — and (2) anything used a lot is there or has an alternative. `N-A` rows
are justified and verifiable; unused surface is deliberately dropped, not rebuilt.

**Status:** `DONE` · `GAP` (needs a milestone) · `N-A` (justified). **Usage:** heavy / occasional / rare.

## convoy

| Command | Usage | Status | Backing test / milestone / justification |
|---|---|---|---|
| `up` | heavy | **DONE** | `tests/run.rs`, `tests/nomad_survival.rs` (supervise + decouple) |
| `render` | heavy | **DONE** | `tests/render.rs`, `tests/render_neutrality.rs` (M3.1/M3.2) |
| `ls` | heavy | **DONE** | `tests/discovery.rs` (`st2 ls`) |
| `down` | heavy | **DONE** | `st2 down <catalog>` — explicit teardown, kills this host's live tasks; `tests/run.rs::down_tears_down_this_hosts_live_tasks_only` |
| `add` | heavy | **DONE** | `st2 add <identity> <ir-dir> …` — authors the IR entry (declare-a-new-agent); sanity-parsed |
| `remove` | occasional | **DONE** | `st2 remove <identity> <ir-dir>` — deletes the IR entry (inverse of `add`) |
| `reload` | occasional | **N-A (composable)** | re-render + restart one agent = `st2 render` + `st2 down` + `st2 up` — the pieces exist; a single verb is convenience only |
| `init` | occasional | **N-A** | network scaffold — manual (or per-agent `st2 add`) for the swap; a convenience verb is M6-tooling |
| `cos` | rare | **N-A** | bootstrap-a-CoS convenience = declare+render a cos agent (`add`+`render`); no separate verb needed |
| `personas` | occasional | **N-A** | render consumes `personas/<p>.md`; the clone/install is a one-time setup step (git clone), not runtime |
| `env` | occasional | **DONE** | `st2 env <catalog>` prints `export CATALOG/ST_ROOT/PTY_ROOT` — `eval "$(st2 env …)"` sets a shell for st/pty |
| `doctor` | occasional | **DONE** | `st2 doctor <catalog>` — tools on PATH, supervisor lock, per-agent task-alive + presence-fresh; exits non-zero on problems |
| `run` | occasional | **N-A** | ad-hoc (undeclared, unreconciled) session; the declared-fleet path is `add`+`up`. Confirm: dev-only |
| `shell` | rare | **N-A** | interactive subshell with net env exported — a convenience, not fleet runtime |
| `pretrust` | rare | **N-A** | Claude-Code trust-race workaround for spawning many at once; folded into `up`. Revisit only if the race bites |
| `rename` | rare | **N-A** | rename an agent; do-by-hand (edit catalog + move bus folder) during migration |
| `install-cli` | once | **N-A** | packaging/PATH setup (symlink binaries), not runtime — handled by distribution/`cargo install` |
| `app` | rare | **N-A (verify-Mac)** | Mac menubar / TCC anchor — N-A on Linux (hetz). VERIFY the Mac side (cos, app-apple hosting) needs no st2 app-equivalent for TCC before settling |

## smalltalk

| Command | Usage | Status | Backing test / milestone / justification |
|---|---|---|---|
| `send` | heavy | **DONE** | `tests/message.rs`, `src/message.rs` (`message send`) |
| `reply` | heavy | **DONE** | `tests/message.rs::reply_threads_back_to_the_original_sender` |
| `ls` | heavy | **DONE** | `src/message.rs` (`message ls`) |
| `read` | heavy | **DONE** | `src/message.rs` (`message read`) |
| `archive` | heavy | **DONE** | `src/message.rs::send_list_read_archive_cycle` |
| `agents` | heavy | **DONE** | `src/agents.rs::agents_json_is_byte_compatible_with_smalltalk` (`--json --enrich` byte-compat) |
| `status` | heavy | **DONE** | `src/status.rs` unit + `tests/status_agents.rs` |
| `ding` | heavy | **DONE** | `src/ding.rs` unit + end-to-end smoke |
| `hooks` (boot) | heavy | **DONE** | `tests/render_neutrality.rs` (SessionStart/PreCompact/StopFailure in settings.local.json) |
| `gc` | occasional | **N-A** | st2's supervisor GCs dead non-keep sessions automatically (`tests/run.rs`); no manual verb needed |
| `thread` | heavy | **DONE** | `tests/message.rs::thread_walks_the_reply_chain_across_both_agents` (`message thread [--tree]`, catalog-wide walk) |
| `read --json` / `ls --json` | occasional | **DONE** | `message ls/read --json` (smalltalk `LsItem` shape: filename/ts/from/subject/inReplyTo/tags/priority) |
| `context` | heavy | **DONE** | `src/context.rs` unit tests — `context read/write/append` over `resources/context` (`now.md` + `decisions/`, smalltalk-compatible `- <ISO> …. why: ….` log) |
| `resource` | occasional | **DONE** | `src/resource.rs` unit tests — `resource add/ls/read/remove` over `resources/links/` (url/title/tags/relation record) |
| `overview` | occasional | **N-A** | `agents --json --enrich` covers the roster/presence/activity we use; a spawn-tree view is a future nice-to-have (build the delta only if wanted) |
| `watch` | rare | **N-A** | live TUI watch; `agents --json` + logs cover monitoring for the swap |
| `init` | occasional | **N-A** | overlaps convoy `init` (network scaffold); see there |
| `mcp` | — | **N-A** | the maintainer confirmed OUT — ding-only is the standing posture; no MCP surface |
| `sync` | — | **N-A** | fabric owns cross-host file sync (the maintainer decision) |
| `sync-fabric` | — | **N-A** | fabric owns it (the maintainer decision) |

## st2 beyond parity (improvements, not gaps)

Crash-loop surfacing to the supervisor (M2.4), the Nomad-decoupling gate + exec group-teardown, the
render↔convoy neutrality diff, and INVARIANTS.md — all net-new guarantees over convoy+smalltalk.

## Gap roadmap (what's left to migration-ready)

- **M2.5** — `message thread`, `--json` on `message ls/read`, `overview` delta. (Parity-required-before-M6 verbs.)
- **M2.6** — `context` (read/write/append) + `resource` (add/ls/read/remove). Net-new, both heavy-ish.
- **M3.3** ✅ `add` + `down` + `remove` done; `reload` = composable (render+down+up), N-A as a verb.
- **M3.4** ✅ light `env` (print exports) + `doctor` (health check) — both DONE.
  wired? dings delivering? stale/parked?). Cos-reclassified from N-A — both lean-on this session.
- **M6-tooling** — `init` network scaffold (or manual). Verify `app` on the Mac side (TCC).

**st2 is migration-ready: every row is `DONE` (test-backed) or a confirmed `N-A`.** Next: M4 evals (the trust gate) → then the M6 swap (see SWAP.md).
