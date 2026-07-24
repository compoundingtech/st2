# Canonical catalog format — the `render{}` materialize block

These two examples are the canonical documentation of the st2 catalog agent format with the declarative
overlay (`render{}`), blessed on the prototype:

- [`agent-claude.kdl`](agent-claude.kdl) — the **claude** harness shape.
- [`agent-codex.kdl`](agent-codex.kdl) — the **codex** harness shape (correct codex form: brief in
  `AGENTS.md`, no `.claude` overlay, no hooks — see the file for why it differs).

They are **generified** (`<host>`/`<identity>`/`<workspace>` placeholders) and carry **no machine
paths**: st2 provides the roots (`CATALOG`/`ST_ROOT`/`PTY_ROOT`/`ST_HOOKS`) from its install layout, so
the same catalog is portable across machines.

**Status:** this is the TARGET format. The `render{}` parser + the generic materialize primitive are
built by the materialize milestone (see [`../../MATERIALIZE-PLAN.md`](../../MATERIALIZE-PLAN.md)); until
then these files are the spec, not yet parsed by `st2 up`.

## The shape

- `agent "<identity>" { host; workspace; env { ST_AGENT }; command; ding }` — the run spec (an agent
  IS its pty; `ding` is the built-in sidecar).
- `render { … }` — an **ordered, gating** pre-boot phase. `st2 up` runs the directives in order before
  spawning the command; a failed **gating** directive means the command does not boot (no half-rendered
  agent). Directives: `copy` (byte copy), `file` (write + `$VAR`-expand), `json-upsert` (deep-merge JSON,
  preserve user keys), `ensure-line` (idempotent append), `git-exclude` (**advisory**, non-gating).
- The overlay is **st2-native**: `.st2/PERSONA.md`, `.st2/bus.md`, loader `.claude/rules/st2.md`.
- The bus + context runtime is **co-located** at `<agent-dir>/resources/{inbox,archive,context,links}`
  (synced by fabric, git-ignored — synced ≠ tracked).
