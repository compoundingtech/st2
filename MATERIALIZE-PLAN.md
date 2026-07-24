# Materialize plan — declarative overlay in the catalog (`render{}` blocks)

Scope for the self-contained-catalog milestone (maintainer-picked): express the agent's workspace
overlay (persona, ding-bus instructions, hooks, permissions) as **declarative directives in
`agent.kdl`**, and have `st2 up` materialize them generically at boot. This keeps the catalog
self-contained AND the render-agnostic invariant intact: `st2 up` copies/writes **generic files** — it
never learns what a persona or a hook is. All the "which files, what content" knowledge stays in
`st2 render`, which now *emits* the directives instead of writing the workspace directly.

Plan-first gate: this is the scope; **build only after the format is blessed + this plan is approved.**

## The `render{}` block (directives)

Inside a catalog `agent.kdl`, a `render { … }` block of generic materialize directives:

- `copy "<src>" "<dest>"` — byte-for-byte copy. `<src>` is catalog-relative (e.g.
  `_templates/DING-BUS.md`); `<dest>` is workspace-relative. For the big static files (persona,
  DING-BUS, AGENTS.md). Overwrite semantics TBD (see open Qs).
- `file "<dest>" { content "<text>" }` — write `<text>`, with `$VAR` expansion via the **existing
  env-cascade** (`$CATALOG`/`$ST_ROOT`/`$ST_AGENT`/…, same `expand_catalog` st2 already runs on
  env/cwd/tags). For the small templated files (settings.json, permissions.sh).
- `ensure-line "<dest>" "<line>"` — idempotent append-if-absent. For the `.claude/rules/convoy.md`
  `@`-import lines (must not clobber a user's loader, must not duplicate on re-up).
- `git-exclude "<path>"` — its own directive (maintainer's instinct: the `.git/info/exclude` append is
  special). Append `<path>` to `<workspace>/.git/info/exclude`, git-repo-conditional, idempotent.

Vendored static sources live in `<catalog>/_templates/` (the `copy` sources).

## Work items (each test-tied)

1. **Parser** — `render{}` + the four directives in `kdl_format`/`discovery` (the catalog format).
   Render-only fields stay ignored by the runner subset; `render{}` is consumed by the new materialize
   step, not by reconcile.
2. **Materialize primitive** — a generic `materialize_overlay(agent, workspace)` run in `st2 up`'s boot
   path *before* spawning the pty: execute each directive (copy bytes / write-with-env-expand /
   ensure-line / git-exclude append), idempotent (safe on every reconcile pass). Generic file I/O — no
   persona/harness knowledge. This is the one (mild) expansion of `st2 up`: today it is read-only on the
   catalog + spawn-only; now it also writes the declared overlay into the workspace.

2b. **Render-only / dry-run path (the cheap test surface)** — expose the primitive STANDALONE so the
   overlay can be materialized WITHOUT booting an agent (no pty, no claude, zero token cost): a
   `st2 materialize <catalog> [--host]` (or `st2 up --materialize-only`) that runs step 2 for each
   agent and stops — no reconcile, no spawn. This is how the format is verified: materialize → diff the
   workspace against convoy's output, at zero cost. It is also what every materialize test uses (temp
   dirs + file assertions, never a live agent). **Budget constraint (maintainer): no eval RUNS / no
   fresh-team boots until after Sunday** — so this render-only path + its file-diff tests are the
   near-term buildable/verifiable surface; any agent-booting test is held until after Sunday.
3. **Render emits blocks** — `st2 render`/`render-agent` change from writing the overlay directly →
   emitting the `render{}` block into `agent.kdl` + vendoring the static files into `_templates/`. The
   overlay *content* is unchanged; it moves from render-writes-workspace to kdl-declares →
   up-materializes.
4. **The hard edge: st2-native hooks (decouple from smalltalk's external install).** Today
   `settings.local.json` bakes an **absolute machine path** to smalltalk's install hook scripts
   (`ST_BIN=<abs st> <abs smalltalk>/examples/claude-code/hooks/<hook>.sh`) — machine-specific AND an
   external dependency, so it cannot be self-contained. Fix: vendor **st2-native** hook scripts
   (`session-start`/`pre-compact`/`stop-failure`) into st2's `templates/`, replicating the behavior
   (boot-ritual system-reminder on cold start / resume / compact + last-working-state `context/now.md`
   injection), but resolving via st2's own paths and shelling out to **`st2`** verbs, not smalltalk's
   `st`. Materialize them into `<workspace>/.claude/hooks/` via `copy`, and reference them
   **workspace-relative** in `settings.local.json` → zero machine path, zero external dependency.
5. **Systematic `st` → `st2` pass in agent-facing text** — boot prompts, the DING-BUS template, and the
   hook scripts all reference the old `st` CLI; st2 subsumes the bus verb-for-verb (see the CLI surface
   below), so vendor an **st2** DING-BUS template + st2 boot prompts + the st2-native hooks. Composes
   with item 4.
6. **Tests** — materialize each directive incl. idempotency on re-up; render emits the correct block +
   `_templates/`; a rendered catalog boots and materializes the overlay; **neutrality**: the
   materialized overlay is byte-identical to what render-writes-directly produced today (behavior
   preserved); st2-native hooks reference workspace-relative paths (no machine path).

## Open questions (settle before/at build)

- **Overwrite vs only-if-absent** for `copy`/`file` on re-up: overwrite (render is source of truth) or
  skip-if-present (respect local edits)? `ensure-line`/`git-exclude` are already append-idempotent.
- **When to materialize**: every reconcile pass (cheap, idempotent) vs first-boot-only.
- **st2-native hook behavior parity**: replicate smalltalk's session-start now.md-injection exactly, or
  simplify to the boot-ritual reminder? (Grounded at build time against the smalltalk scripts.)
- **`--priority` on `st2 message send`**: not implemented (deferred). If the fleet's templates use it,
  add it as a small pre-req.

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
  `--host`. Env unchanged: `ST_AGENT` / `ST_ROOT` / `CATALOG` / `PTY_ROOT`.
