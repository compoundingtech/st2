# `st2 validate <catalog>` — plan / scope (for CoS review, build HELD)

**Why.** st2 is about to become a **shared contract**: the maintainer's renderer today (convoy IR → catalog),
Johannes's nix derivations tomorrow (nix → catalog). A second renderer needs a way to know it **hit the
contract** before running — a malformed `agent.kdl`, a missing field, a bad path, a host/id that fights
its folder. `st2 validate` is that check: it reads a catalog, reports every issue clearly, and **exits
non-zero on problems** so any renderer's build can gate on it.

**Boundary.** Validate st2's **runner-normative contract** (spec.md §2 fields + §5 resource layout +
discovery precedence) — that is st2's authority. Render-only artifacts (persona overlay, hooks) are lint,
not law (SD2). No new wire format, no behavior change to run — validate is read-only.

---

## 1. Grounding — what "correct" means (already in the code)

Validate is not inventing rules; it promotes what `discover()` + the spec/batch parsers already know:

- **`discover()`** (`src/discovery.rs`) already yields `errors` (files that looked like specs but failed
  to parse/resolve) and `warnings` (identity/host path↔content mismatches). Validate surfaces both as
  first-class issues instead of the run loop's best-effort notes.
- **`RawSpec` / `AgentSpec`** (`src/spec.rs`) define the runner-normative field set: `identity` (required),
  `host`, `type` (service|batch), `workspace`, `supervisor`, `retired`, `keep`, `restart{}`, and the
  `pty`/`exec` tasks. Render-only keys (`harness`, `model`, `role`, `persona`, `permissions`, …) are
  deliberately dropped — validate does **not** treat their absence as an error.
- **Resource layout** (`src/message.rs`, `resource.rs`, `context.rs`): `resources/{inbox,archive,context,
  links}` per agent, created on demand.
- **Batch** (`src/batch.rs`): `parse_batch` + `topo_order` already reject stage cycles / unknown deps.

So the build is mostly: run `discover()`, run a handful of net-new per-spec + cross-spec + batch checks,
format the issues, exit on severity.

---

## 2. The check catalog

Each check: **what** · **severity** · **grounded in**. Example messages in §3.

**Structural (per file) — already caught by discovery, promoted:**
1. **Parse failure** — malformed KDL/TOML/JSON. **ERROR.** (`discover().errors`)
2. **No identity** — spec-shaped file, no identity in content or path. **ERROR.** (`resolve_spec` bail)
3. **identity/host path↔content mismatch** — content wins per the gist, but flagged. **WARN.**
   (`discover().warnings`)

**Spec fields (per resolved agent) — net-new promotion:**
4. **Unknown `type`** — not service|batch (e.g. `"srvice"`). Today silently → service; a renderer wants
   to know it typo'd. **ERROR.** (spec.rs job_type mapping)
5. **Unknown task kind** — a task block that isn't `pty`/`exec`. **ERROR.** (spec.rs TaskKind)
6. **Not runnable** — a **`service`** agent with zero tasks, or a service task with no `command`. Legal
   for a *declared-but-unrendered* agent, so **WARN** ("declared, not yet rendered"), not error.
   **Service-only:** a `type = batch` job legitimately carries **no** `pty`/`exec` tasks (its work lives
   in `stages`/`run`), so this check must never fire on a batch job — confirmed against `examples/batch`,
   where `demo-eval` has zero tasks by design. (`is_runnable`)
7. **Bad path** — `workspace` / task `cwd` that is a **relative literal** (expected absolute or
   `$CATALOG`-rooted), or an absolute/`$CATALOG`-expanded literal that **does not exist**. **WARN**
   (runtime may create it; `$VAR`-bearing paths that can't be resolved are skipped, not guessed). (SD3)

**Cross-spec (whole catalog) — net-new:**
8. **Duplicate bus id** — two specs resolving the same `<host>.<identity>`. The runner can't run both.
   **ERROR.** (net-new; `bus_id`)
9. **Dangling supervisor** — `supervisor "<id>"` naming an identity not present in the catalog. **WARN**
   (may live on another host / catalog). (spec.rs supervisor)

**Batch jobs (`type = batch`) — reuse the batch parser:**
10. **Malformed batch** — stage DAG cycle / unknown `after` dep / no `verdict` stage / kick `to` naming a
    seat that isn't declared / missing `done-when`. **ERROR.** (`parse_batch` + `topo_order`)

**Overlay lint (render artifacts — SD2, optional, WARN only):**
11. **Dangling persona / @import ref** — a materialized overlay (`.claude/rules/convoy.md` → `@…PERSONA.md`)
    whose import target is missing. **WARN.** Helpful for the nix flow; not st2's normative law.

---

## 3. CLI + output contract

```
st2 validate <catalog> [--strict] [--json]
```

- **Human output** — one line per issue, `SEVERITY  <rel-path>: <message>`, then a summary:
  ```
  ERROR  hetz/st2-claude/agent.kdl: unknown type 'srvice' (expected service|batch)
  WARN   hetz/worker/agent.kdl: workspace 'repo' is relative (expected absolute or $CATALOG-rooted)
  WARN   hetz/worker/agent.kdl: supervisor 'ghost' is not an agent in this catalog
  ─ 1 error, 2 warnings across 7 agents
  ```
- **Exit code** — `0` clean (warnings alone still 0), **non-zero if any ERROR**. `--strict` promotes
  WARN → ERROR so a renderer's CI can demand a spotless catalog.
- **`--json`** — `{ "issues": [ {severity, path, agent, code, message} ], "agents": N, "errors": E,
  "warnings": W }` — machine output for a second renderer's build gate (Johannes's nix flow consumes this).
- Each issue carries a stable **`code`** (e.g. `unknown-type`, `dup-id`, `bad-path`) so scripts match on
  code, not prose.

---

## 4. Reuse + shape

- New `src/validate.rs`: `pub struct Issue { severity, code, path, agent, message }` + `pub fn
  validate(root) -> Vec<Issue>`. Runs `discover()`, folds its errors/warnings into issues, then the
  net-new per-spec / cross-spec / batch / overlay checks.
- `src/main.rs`: `Validate { root, strict, json }` + `validate_cmd` (print, exit on severity).
- Zero change to run/reconcile/wire — read-only.

## 5. Test plan

- A **fixture corpus** under `tests/fixtures/validate/`: one clean catalog (exit 0, no issues) + one
  catalog per failure mode (unknown-type, dup-id, no-identity, bad-path, dangling-supervisor, malformed
  batch, dangling-persona). Each test asserts the **exact issue code + severity + exit**.
- The shipped `examples/batch` catalog must validate **clean**, and `examples/ir` **rendered** (via
  `st2 render`) must validate clean — a standing guard that our own output conforms (and a regression
  tripwire). Note `examples/ir/fleet.kdl` is IR *source*, not a catalog — validating it directly is a
  category error (it warns identity↔path by construction); validate runs on the **rendered** output.
- `--strict` and `--json` each get a test.

---

## 6. Scope decisions for you (the gate)

- **SD1 — severity split.** Proposed ERROR (blocks): parse, no-identity, unknown-type, unknown-task-kind,
  dup-id, malformed-batch. Proposed WARN (advisory): path↔content mismatch, not-runnable, bad-path,
  dangling-supervisor, dangling-persona. Plus `--strict` to promote all WARN → ERROR. **OK, or move any?**
  (Key judgment: is *unknown-type* an error or a warn? I lean ERROR — silent typo→service is the exact
  footgun a contract-check exists to stop.)
- **SD2 — scope boundary.** Validate the **runner-normative contract** as authoritative (errors), and
  **lint render-overlay refs** (persona/@import, check #11) as WARN only? Or keep validate strictly to the
  runner subset and leave overlay-linting out entirely? I lean "lint as WARN" — it's cheap and directly
  serves the nix→catalog flow — but it's your call since overlay isn't st2's law.
- **SD3 — path checking depth.** Check literal + `$CATALOG`-expanded paths (flag relative literals, flag
  nonexistent absolutes); **skip** paths bearing other `$VAR`s rather than guess. Agree?
- **SD4 — `--json`.** Include it now (a second renderer's CI is the whole point) — confirm.

**Not building until you confirm scope.** Then: `src/validate.rs` + `st2 validate` + fixture corpus,
same commit rhythm as M4a. The nix example derivations follow after validate (task 2, lower priority).
