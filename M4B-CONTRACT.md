# M4b — the st2-side contract for a real eval seat

**Status:** DRAFT for CoS scope review. st2-side only. **The evals repo is untouched by this doc** —
it describes what *st2 provides* so a real supervisor/worker seat (a rendered `claude`) drops into the
batch executor where the M4a benign-shell stand-in sits today. The actual 44-cell migration
(repoint-vs-native) and eval timing are the maintainer + evals-claude coordination and are **not** in scope here.

---

## 1. Where M4b sits

Two halves are already built and proven:

- **The batch executor (M4a, `7c1e26f`)** — `st2 batch` runs a `type = batch` job through the stage DAG
  `setup → run → grade`: the run stage spins the team's seats, seeds the kick, blocks on the team's
  `done`, tears down; the grade stage's exit is the verdict. Proven green **and** red, zero leaked seats.
- **Render (M3.1, `4b1ad63`, neutrality-proven `f371d4b`)** — `st2 render <ir> <catalog>` emits an
  `agent.kdl` with convoy's **exact** wiring: `exec claude --permission-mode bypassPermissions [--model]
  [boot]`, `ST_AGENT` / `ST_ROOT=$CATALOG/…` / `PTY_ROOT`, persona overlay into the workspace, and the
  ding command.

**M4b is bolting the second half onto the first.** The executor already spins seats as ordinary agents
via `up_once`; it does not care whether a seat's task is `sh script.sh` or `claude …`. So most of M4b is
already latent — this contract pins the few seams where a *real* claude seat differs from the stand-in.

---

## 2. The seam, piece by piece

For each: **M4a stand-in** (what the example does today) → **real seat** (what a rendered claude needs)
→ **st2-side gap** (net-new work, if any).

### A. Seat spec — hand-authored → rendered
- **M4a:** `demo/demo-sup/agent.kdl` is hand-written with `exec "task" { command "sh …/supervisor.sh" }`.
- **Real:** the seat is a rendered agent — `st2 render` produces `agent.kdl` with `claude …` + env +
  persona + ding, byte-for-byte convoy-neutral.
- **Gap:** the batch job's `run { seat "<name>" { agent "<identity>" } }` must resolve to a *rendered*
  catalog entry. **Proposed convention:** a `setup` stage runs `st2 render <cell-ir> <catalog>` so the
  catalog is materialized before the run stage looks for seats — the cell stays self-describing, no
  pre-render step outside the job. (Alternative: pre-rendered catalog handed to `st2 batch`. Prefer the
  setup-stage render.)

### B. Waking the seat — the kick must POKE, not just land
- **M4a:** `supervisor.sh` **polls** its own inbox (`st2 message ls --count` in a loop).
- **Real:** claude does not poll. `run_team` seeds the kick by writing the message file directly into the
  seat's `resources/inbox/`; a real seat only wakes if its **ding** sidecar sees that inbox write and
  pokes the seat. Render already wires the ding command per agent.
- **Gap:** the run stage must ensure **each seat's ding is alive** alongside the seat, so the kick's
  inbox-write fires the ding → pokes the seat. Today `up_once` spins the seat task; it must also spin (or
  the rendered catalog must declare, and the executor must keep alive) the seat's ding. Small, testable.

### C. Seat backing — pty vs exec (**load-bearing**)
- A ding pokes a **pty** (types the poke + Enter). A terminal-free **exec** claude has no pty to poke.
- **Therefore:** an interactive, kick-driven seat must be **pty-backed claude**, so B works. `exec` stays
  for headless/one-shot work (and the batch job itself). st2 already routes pty vs exec per task
  (`SystemRunner`), so this is a spec choice on the rendered seat, not new plumbing — but it's the seam
  the whole wake-path hangs on. **CONFIRMED (CoS, Q1):** kick-driven seats are pty-backed claude —
  the ding delivers the kick to a pty, so an exec seat (nothing to poke) cannot be kick-driven. Settled
  as a technical necessity; it pre-answers the seam for when M4b is greenlit (does not restart the build).

### D. Spin order — workers first, supervisor last
- The evals lifecycle spins workers **before** the supervisor so the supervisor's kick finds a ready team.
- **M4a:** single seat; `up_once` spins everything in one reconcile, order-agnostic.
- **Gap:** honor a spin order for multi-seat teams — either declared order, or a seat-level `after`
  (mirrors the stage `after`). Small; needs a multi-seat test.

### E. Done — self-declared bus message (DQ1, unchanged)
- **M4a:** `supervisor.sh` sends a **self-addressed** `done`-tagged message; `run_team` polls the seat
  inbox for the `done` tag, timeout-bounded.
- **Real:** claude sends the same message via its Bash tool (`st2 message send … --tags done`). Mechanism
  is unchanged and harness-agnostic. **Open (Q5):** keep the self-addressed done-sink, or give the
  executor a dedicated batch-controller inbox the supervisor addresses? Self-addressed works today.

### F. Grade — exec the cell's grader (unchanged)
- **M4a:** the `grade` stage execs `grade.sh`, which checks the workspace deliverable; verdict = exit.
- **Real:** identical — the grade stage execs the cell's `fixture/grade.sh` against the sandbox the team
  mutated. `$CATALOG` (and the sandbox path) are in the stage env. Nothing st2-side changes.

### G. Hermetic sandbox — workspace = the frozen sandbox dir (unchanged mechanism)
- The seat's workspace (render sets `cwd = workspace`) **is** the hermetic sandbox (frozen base commit).
- **st2-side:** a `setup` stage runs the cell's `fixture/setup-sandbox.sh` to materialize it; the rendered
  seat's cwd points at it. Materialization logic lives in the cell (Q3) — st2 just runs it as a stage.

### H. Isolation + teardown — proven, cell owns the gate
- Nomad-safe teardown (process-group kill, zero zombies) is proven (M4a + `nomad_survival`). Per-host
  isolated `PTY_ROOT` / exec state keeps seats from bleeding across runs.
- The **isolation grade axis** (the hard gate) is a check inside the cell's grader (grade stage) — st2
  provides the clean-teardown + isolation *substrate*; the cell asserts the *axis*.

### I. Matrix — deferred to the next increment (DQ2)
- Q1 confirmed single-variant first. When matrix lands: `run_batch` runs the whole pipeline once per
  model-family variant (`matrix { claude; codex; mixed }`), overall verdict = **ALL-PASS**. st2-side =
  a variant loop with a per-variant model override on render; the first real cell proves single-variant.

---

## 3. st2-side work items (only if greenlit)

Small, each tied to a test — nothing here touches the evals repo:

1. **Ensure-ding-per-seat** in the run stage (B) — the kick wakes a real seat. *Test:* a rendered seat +
   ding, kick-seed → ding fires. (Can be proven with a fake-poke seat, no real claude.)
2. **Seat spin-ordering** (D) — declared order or seat `after`. *Test:* multi-seat, assert order.
3. **Pty-backed seat convention** (C) — confirm + document that kick-driven seats render as pty tasks.
   *Test:* render a seat IR → assert pty task + ding wiring.
4. **`render` setup-stage convention** (A) — a job whose setup stage renders its own seats. *Test:* the
   example cell gains a rendered seat instead of a hand-authored one.

The batch executor, done-detection, grade, teardown, and verdict wiring are **already done** — M4b adds
only the wake-path + multi-seat ordering + the render-into-the-job convention.

---

## 4. Open questions — NOT solo-decidable (the maintainer / evals-claude)

- **Q1 (load-bearing): CONFIRMED (CoS).** Real seat backing = **pty claude** (so the ding can poke it) —
  the ding delivers the kick to a pty; an exec seat has nothing to poke. Technical necessity, settled;
  pins the whole wake-path (§B/C). Pre-answered for the greenlight — does not restart the build.
- **Q2 (the maintainer):** migration approach — do cells keep their existing harness (`bin/lib-harness.sh`) and
  just **repoint spin → st2**, or become **native `type = batch`** jobs? st2 provides the same seam either
  way; this decides the evals-repo shape, which I do not touch.
- **Q3 (evals-claude):** sandbox materialization = run the cell's existing `setup-sandbox.sh` unchanged as
  a setup stage?
- **Q4 (evals-claude):** persona composition — does render's persona overlay match the evals SHA-pinned
  composition (task-lane + boot + BASE + role)? Render does the overlay (M3.1); confirm it lines up.
- **Q5:** done-sink — self-addressed `done` (M4a) vs a dedicated batch-controller inbox?

---

## 5. Boundary

After this draft, **everything left needs the maintainer** (migration approach + timing + the live swap) **or
evals-claude** (the 44 cells). st2 stands feature-complete at this line: service + batch runner, render,
native bus, full smart-parity, swap runbook. This contract is the hand-off surface for the evals-claude
coordination — nothing in it requires reaching into another repo.
