# Materialize plan — declarative overlay in the catalog (`render{}` blocks)

Scope for the self-contained-catalog milestone (maintainer-picked): express the agent's workspace
overlay (persona, bus instructions, hooks, permissions) as **declarative directives in `agent.kdl`**
(a `render{}` block), executed by `st2 up` at boot.

**The two roles (maintainer-clarified):**
- `st2 compile-agent` / `st2 compile` = the **GENERATOR (compiler)**. Harness-aware (knows claude vs
  codex), it PRODUCES `catalog/<host>/<id>/agent.kdl` from identity/role/harness and writes the
  harness-appropriate `render{}` block into it. It does NOT materialize the overlay.
- `st2 up` = the **EXECUTOR (runtime)**. It reads + executes the whole `agent.kdl` INCLUDING the
  `render{}` block — materializing the overlay into the workspace (generic file ops: copy / write /
  merge / ensure-line / git-exclude; it NEVER learns what a persona or a hook is) — THEN boots. **The
  materialize primitive lives HERE.** This is what keeps the runner render-AGNOSTIC while still
  self-provisioning the workspace: up executes generic directives, not persona/hook knowledge.

**Naming caution (maintainer):** `st2 compile` (GENERATE the kdl) must not collide with materialize
(EXECUTE the render block). Keep the verbs distinct — the no-boot materialize path is `st2 up
--materialize-only` (an `up` mode) — an EXECUTE verb, never named `compile` (that is GENERATE).

Plan-first gate: this is the scope. The format is now BLESSED; **build only after this plan is approved.**
Canonical format examples (claude + codex) live in [`examples/native/`](examples/native/).

## The `render{}` block (directives)

`render{}` is KEPT (not flattened) because it is a **phase GATE**, not just grouping: an ORDERED
pre-boot materialize phase. `st2 up` runs its directives in declaration order and, if any GATING
directive fails, does NOT spawn the command (no half-rendered agent booting broken). `git-exclude` lives
INSIDE `render{}` as the one BEST-EFFORT (non-gating) member — the content directives (copy/file/
ensure-line) write the actual persona/hooks so their failure = broken agent → they gate; `git-exclude`
only hides the overlay from git, so a failure or a non-git workspace is cosmetic (untracked overlay),
never a reason to block boot.

Inside a catalog `agent.kdl`, a `render { … }` block of generic materialize directives. All overlay
paths are **st2-native** — `.st2/…` (NOT `.convoy/…`), and the loader is `.claude/rules/st2.md`:

- `copy "<src>" "<dest>"` — byte-for-byte copy. `<src>` catalog-relative (e.g.
  `_templates/bus.st2.md`); `<dest>` workspace-relative. **GATING.** Render-OWNED static files
  (`.st2/PERSONA.md`, `.st2/bus.md`, `AGENTS.md` for codex). Overwrite (render is source of truth).
- `file "<dest>" { content "<text>" }` — write `<text>` with `$VAR` expansion via the **existing
  env-cascade** (`$CATALOG`/`$ST_ROOT`/`$ST_AGENT`/`$ST_HOOKS`, same `expand_catalog`). **GATING.**
  Render-OWNED non-JSON templated files (e.g. `permissions.sh`). Overwrite.
- `json-upsert "<dest>" { content "<json>" }` — **NEW (maintainer).** DEEP-MERGE the JSON into an
  existing file, preserving the user's other keys; create if absent. **GATING.** This is how the
  user-shared `.claude/settings.local.json` (boot hooks) and `.claude/settings.json` (permissions hook)
  are handled — MERGE, never clobber (they are claude-local settings a user may also edit). Replaces
  `file{}` for JSON.
- `ensure-line "<dest>" "<line>"` — idempotent append-if-absent. **GATING.** The `.claude/rules/st2.md`
  `@`-import lines (`@../../.st2/PERSONA.md`, `@../../.st2/bus.md`) — must not clobber a user's
  loader, must not duplicate on re-up.
- `git-exclude "<path>"` — its own directive, INSIDE `render{}`, **ADVISORY (non-gating).** Append
  `<path>` to `<workspace>/.git/info/exclude`; git-repo-conditional, idempotent; a failure or a non-git
  workspace never blocks boot. Excludes `.st2/`, `.claude/rules/st2.md`, `.claude/settings.local.json`,
  `AGENTS.md`.

(`.claude-session-id` is DROPPED — unused off `--resume`, no hook references it.)

Vendored static sources live in `<catalog>/_templates/` (the `copy` sources).

## Install/state layout + sync boundary (roots are st2-PROVIDED, not kdl-hardcoded)

The kdl no longer hardcodes `ST_ROOT="$CATALOG/smalltalk"` / `PTY_ROOT="$CATALOG/pty"`. **st2 provides
all roots from its install/state layout**, so the catalog is PORTABLE across machines (same synced
catalog + each machine's st2 supplies its own local runtime roots). Proposed layout (per the maintainer):

```
~/.local/state/st2/<network>/         # <network> = "default" or the network name
  catalog/     # SYNCED (fabric) — the whole network home:                          → $CATALOG (== $ST_ROOT)
               #   <host>/<id>/{agent.kdl, resources/{inbox,archive,context,links}}, _templates/, personas/
  pty/         # LOCAL           — live pty sessions (sockets/pids/scrollback)       → $PTY_ROOT
  run/         # LOCAL           — exec-task state, logs, .runs (machine runtime)
<st2-install-owned>/hooks/            # LOCAL — st2-shipped native hook scripts       → $ST_HOOKS
```

**No separate `smalltalk/` root — the bus is CO-LOCATED** (confirmed feasible; `resolve_inbox` already
resolves a catalog agent's inbox at `<agent-dir>/resources/inbox`, only falling back to a flat
`<root>/<id>/inbox` for the catalog-less eval case). Each agent's `resources/{inbox,archive,context,
links}` lives beside its `agent.kdl` — the agent-id dir is the complete home (definition + bus + context).
So `$ST_ROOT` collapses into `$CATALOG` (the bus is derived from CATALOG + host + id); the flat
smalltalk-style root remains only as the eval catalog-less fallback.

**The sync boundary (the load-bearing call):**
- **SYNCED** (portable = the shared network): the `catalog/` — declarations, personas, `_templates/`,
  AND each agent's `resources/` (the bus + context). Cross-host messaging relies on this (a send to
  `silber.cos-claude` writes into `catalog/silber/cos-claude/resources/inbox`, which fabric syncs), and
  the append-only `<unix-ms>-<rand6>.md` wire makes concurrent writes conflict-free. Note: `resources/`
  is git-EXCLUDED (runtime) but fabric-SYNCED — **synced ≠ git-tracked**; the tracked/versioned surface
  is just `agent.kdl` (+ the persona source).
- **LOCAL** (machine-specific runtime, MUST NOT sync): `pty/` (sockets/pids are meaningless + harmful
  across machines), `run/` (exec state, the auto-log `logs/`, `.runs`), and the hooks (`$ST_HOOKS`,
  per-install). **The auto-log `logs/` + eval `exec/` state must move OUT of `$CATALOG` into the LOCAL
  `run/` root** (today they sit under the catalog — a synced catalog would drag them across machines).
  A required consequence of this layout, not just overlay work.
- st2 sets on every seat: `CATALOG`, `ST_ROOT` (== `CATALOG`), `PTY_ROOT`, `ST_HOOKS` — from the layout,
  so no machine path ever appears in the kdl or the overlay. (`$ST_HOOKS/<hook>.sh` is how
  `settings.local.json` references the hooks — see item 4.)

**Folder-watch filter (new, from the co-location).** `watch_folder` currently fires on ANY change (it
does not filter), and inbox writes already trigger it today (reconcile is idempotent → correct, but a
reconcile per message). With the high-churn bus now definitively in the catalog, add a spec-file filter
so the watcher reacts only to `*.{kdl,toml,json}` (or ignores `resources/`) — a real efficiency win.
Small; not a correctness blocker (works without it, same as today).

**Resource descriptors (maintainer).** When st2 creates an agent's `resources/<kind>/` folders, emit a
`resource.md` in each describing it, with a typed frontmatter so future UI can render known resource
types in a sidebar (resources become self-describing + typed):

```markdown
---
type: inbox            # one of: inbox | archive | context | links
---
# Inbox
Incoming bus messages for this agent — append-only `<unix-ms>-<rand6>.md` files (`st2 message`).
```

Define the small set of types (inbox/archive/context/links) + a one-line human description each; emit
on folder creation (render / first-touch), idempotent.

Cross-cutting note: this touches where roots are set today (`run.rs`/`eval_run.rs` currently derive
`ST_ROOT`/`PTY_ROOT` from `$CATALOG`; evals root exec-state + logs under the catalog). Realizing the
layout is its own sub-work, sequenced with (or just before) the overlay materialization.

## Work items (each test-tied)

1. **Parser** — `render{}` + the five directives (copy/file/json-upsert/ensure-line/git-exclude) in `kdl_format`/`discovery` (the catalog format).
   Render-only fields stay ignored by the runner subset; `render{}` is consumed by the new materialize
   step, not by reconcile.
2. **Materialize primitive** — a generic `materialize_overlay(agent, workspace)` run in `st2 up`'s boot
   path *before* spawning the pty: execute each directive (copy bytes / write-with-env-expand /
   json-deep-merge / ensure-line / git-exclude append), idempotent (safe on every reconcile pass). Generic file I/O — no
   persona/harness knowledge. This is the one (mild) expansion of `st2 up`: today it is read-only on the
   catalog + spawn-only; now it also writes the declared overlay into the workspace.

2b. **Materialize-only / dry-run path (the cheap test surface)** — the executor's no-boot mode:
   **`st2 up --materialize-only`** runs step 2 (execute the `render{}` block) for each agent and STOPS —
   no reconcile, no spawn, no pty, no claude, zero token cost. (Named as an `up` mode per the naming
   caution — it does NOT collide with `st2 compile` the generator.) This is how the format is verified:
   materialize → diff the workspace against convoy's output, at zero cost; every materialize test uses
   temp dirs + file assertions, never a live agent. **Budget constraint (maintainer): no eval RUNS / no
   fresh-team boots until after Sunday** — so this materialize-only path + its file-diff tests are the
   near-term buildable/verifiable surface; any agent-booting test is held until after Sunday.
3. **Render emits blocks** — `st2 compile`/`compile-agent` change from writing the overlay directly →
   emitting the `render{}` block into `agent.kdl` + vendoring the static files into `_templates/` (for
   codex, render pre-composes `_templates/AGENTS.md` = persona + bus). The overlay *content* is
   unchanged; it moves from render-writes-workspace to kdl-declares → up-materializes.
   - **GOLDEN-FILE test (maintainer insight, zero eval cost):** the two committed examples
     [`examples/native/agent-{claude,codex}.kdl`] ARE the compile-agent fixtures. The test invokes
     compile-agent with a fixed generified IR (identity/role/host/workspace/harness) and asserts its
     output EQUALS the example (per harness) — a pure generator unit test, no boot. Composes with 2b:
     the golden-file test proves compile-agent EMITS the right blocks; the render-only materialize test
     proves `st2 up` EXECUTES them into the workspace. Both halves proven with NO eval runs → so we
     likely do NOT need a booted eval cell for the render path at all.
4. **The hard edge: st2-native hooks (decouple from smalltalk's external install).** Today
   `settings.local.json` bakes an ABSOLUTE machine path to smalltalk's install hook scripts
   (`ST_BIN=<abs st> <abs smalltalk>/…/hooks/<hook>.sh`) — machine-specific AND an external dependency.
   Fix (decisions 5–6): **DROP `ST_BIN`** (st2 IS the bin — the hooks call bare `st2`, resolved via the
   seat PATH `st2 up` already sets). **st2 SHIPS the native hook scripts** and PROVIDES **`$ST_HOOKS`**
   (a root from the install layout above), so `settings.local.json` references `$ST_HOOKS/<hook>.sh` —
   no per-workspace copy, no machine path in the kdl. The 3 scripts (session-start / pre-compact /
   stop-failure) replicate the behavior (boot-ritual reminder + `context/now.md` injection +
   crash-notify) but shell out to `st2`, not `st`. Produced (canonical, from st2) when the milestone
   builds. `settings.local.json` (+ `settings.json`) are written via `json-upsert` so a user's own keys
   survive.
5. **Systematic `st` → `st2` pass in agent-facing text — the native templates are canonical FROM st2.**
   st2 owns producing the canonical st2-native templates; the catalog vendors them into `_templates/`:
   - **`st2` bus.md** — DONE: `templates/bus.st2.md` (was DING-BUS.st2.md; renamed + the ding-mode/MCP framing dropped — ding-only now, `[DING]` kept only as the poke name) (st verbs → st2, `--priority` dropped,
     spawn section rewritten to the st2 declarative story: no `convoy add`/`st launch` — declare in the
     catalog via `st2 add`/`st2 compile`/`st2 compile-agent` and the running `st2 up` reconciles it in).
     NOTE: the `[DING]` poke-line LITERAL still reads "new smalltalk message" (wire-compatible in
     `ding.rs`) — the doc says so; an st2-native rename of that literal is a small pending follow-up
     (touches the ding wire text — do it carefully, agents pattern-match the line).
   - **st2-native hook scripts** — item 4 (produced when the milestone builds).
   - boot prompts — the catalog owner does these on the prototype (`st2 status`, etc.).
6. **Tests** — materialize each directive incl. idempotency on re-up; render emits the correct block +
   `_templates/`; a rendered catalog boots and materializes the overlay; **neutrality**: the
   materialized overlay is byte-identical to what render-writes-directly produced today (behavior
   preserved); st2-native hooks reference workspace-relative paths (no machine path).

## Open questions (settle before/at build)

- **Layout confirm**: the `<state>/<network>/{catalog,smalltalk,pty,run}` tree + `$ST_HOOKS` location —
  confirm the exact paths + the `<network>` selector (env? `st2 up` flag? default "default"). Decides
  catalog portability.
- **When to materialize**: every reconcile pass (cheap, idempotent) vs first-boot-only.
- **st2-native hook behavior parity**: replicate smalltalk's session-start `now.md`-injection exactly, or
  simplify to the boot-ritual reminder? (Grounded at build time against the smalltalk scripts.)
- **`--priority` on `st2 message send`**: not implemented (deferred). If the fleet's templates use it,
  add it as a small pre-req.
- (RESOLVED) clobber policy → `json-upsert` (merge) for JSON, overwrite for render-owned files.
- (RESOLVED) `render{}` block vs flat → kept as the ordered gating phase; `git-exclude` inside, advisory.

## Invariants

Don't touch reconcile/teardown/wire-format/presence (INVARIANTS.md). The materialize step is additive +
behavior-neutral; add a neutrality test (materialized overlay == render-direct overlay). If it earns a
row, it's "self-contained-catalog materialization is behavior-neutral", proven by that test.

## st2 bus command surface (for the `st` → `st2` template pass)

st2 subsumes smalltalk's bus verb-for-verb (built wire-compatible). Mapping is `st <verb>` →
`st2 <verb>`:

- `st2 message send <to> [-m <body>] [--subject S] [--in-reply-to F] [--tags T,T]` — **no `--priority`
  yet** (deferred).
- `st2 message reply <file> -m <body> [--subject S]`
- `st2 message ls [<id>] [--archive] [--count | --json] [--from ID]`
- `st2 message read [<id>] <file> [--raw | --json] [--archive]`
- `st2 message archive [<id>] <file>` · `st2 message thread [<id>] <file> [--tree]`
- `st2 status [<id>] [--set offline|available|busy|away|dnd]`
- `st2 agents [--status S] [--json [--enrich]]` (byte-compatible with `st agents --json`)
- `st2 context read/write/append` · `st2 resource add/ls/read/remove`
- `st2 ding --identity <id> --root <root>` (**identity-only now**; positional session optional;
  `st2 ping` is the alias)
- Shared ctx flags: `--root` (default `$CATALOG`), `--as` (acting identity, default `$ST_AGENT`),
  `--host`. Roots are st2-PROVIDED (from the install layout): `CATALOG` / `ST_ROOT` / `PTY_ROOT` /
  `ST_HOOKS` (+ `ST_AGENT` per agent). No `ST_BIN` (st2 IS the bin).
