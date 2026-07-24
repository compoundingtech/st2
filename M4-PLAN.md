# M4 plan — the batch executor (evals), for scope review

M4 is the **trust gate**: evals-green-on-st2 earns the live swap. This is the batch half of the runner
(service agents = M1; batch = M4). **Plan for review — do not build until scope is confirmed.**

## The `type = batch` schema (VRS spec, grounded)

A batch job is an agent-team eval expressed in the catalog. Unlike a service job (respawns via
`restart{}`), a batch job **runs to completion** through a stage DAG and produces a **verdict**.

```kdl
agent "license-mit-eval" {
  type "batch"
  run {
    seat "supervisor" {}
    seat "worker" { workspace "$CATALOG/evals/license-mit/widget" }
    kick      { to "supervisor"; from-file "$CATALOG/evals/license-mit/kick-supervisor.md" }
    done-when { event "done"; timeout "20m" }
  }
  stage "setup" { exec { command "$CATALOG/evals/license-mit/setup-sandbox.sh" } }
  stage "run"   { after "setup"; run { … } }              // spins the seats, seeds the kick, waits done-when
  stage "grade" { after "run"; exec { command "…/grade.sh" }; verdict "exit-code" }
  matrix {
    claude { supervisor { agent "eval-sup-claude" }; worker { agent "eval-wk-claude" } }
    codex  { supervisor { agent "eval-sup-codex"  }; worker { agent "eval-wk-codex"  } }
    mixed  { supervisor { agent "eval-sup-claude" }; worker { agent "eval-wk-codex"  } }
  }
}
```

- **seats** — named positions filled by rendered agents, per matrix variant.
- **kick** — `{ to <seat>; from-file <path> }` — the one hermetic message seeded into the seat's inbox.
- **done-when** — `{ event "done"; timeout <dur> }` — the completion signal + a hard ceiling.
- **stages** — a DAG (`stage "<n>" { after "<dep>"…; exec|run; verdict "exit-code"? }`): setup → run → grade.
- **verdict** — the `verdict "exit-code"` stage's exit code IS the verdict (grader exit 0 = pass). The
  team's self-report is advisory only.
- **matrix** — the whole pipeline once per variant; one verdict each.

## The two settled DQs (from my M1 cos-answers — confirm still hold)

- **DQ1 — what emits `done`:** the **supervisor-confirmation message** — the team's supervisor sends a
  "done" message on the bus; the executor treats that as the `done` event. (No new harness primitive.)
- **DQ2 — matrix aggregate:** **per-variant verdict + overall all-pass** — the job passes iff every
  matrix variant passes.

## M4a — the executor (the net-new build)

`st2` executes a `type = batch` job:

1. **Stage DAG runner** — topologically order stages by `after`; run each (`exec` → shell it; `run` →
   the seat-spin below). Halt on the first non-zero non-verdict stage.
2. **The `run` stage** (per matrix variant): fill each seat with its variant agent (render → `st2 up`
   the team in an isolated catalog), **seed the kick** (`from-file` → the seat's inbox), then **wait
   for done-when** — poll the supervisor's outbound for the `done` confirmation message, bounded by
   `timeout`. Then **tear the team down** (no zombies — reuse `st2 down`; the nomad gate guarantees a
   clean teardown).
3. **Verdict** — the `verdict "exit-code"` stage's exit code; record it (per §5, to the job's
   `planning/events/`).
4. **Matrix** — loop 1–3 per variant; aggregate per DQ2 (all-pass).
5. **Isolation** — each run in a throwaway sandbox + isolated bus (short `PTY_ROOT`), self-cleaning on
   failure — mirrors the eval framework's hermetic-world requirement.

Reuses what's built: `render` (fill seats), `up`/`down` (spin/teardown, nomad-safe), `message`
(seed kick + detect done), `status`/`doctor` (observe). New: the stage-DAG runner + done-when watcher +
matrix loop + verdict recording.

## M4b — migrate the cells onto st2

The 44 cells (`~/src/.../evals`) today spin via **convoy + smalltalk** (`bin/lib-harness.sh` `stev_*`:
`convoy init/add/ls`, kick → `$ST_ROOT/<who>/inbox`, teardown). Migration = adapt the harness to st2:
`convoy init` → make a catalog; `convoy add` → `st2 add` + `st2 render`; spin → `st2 up`; kick → the
seat inbox (behavior-neutral: still the smalltalk bus at `$CATALOG/smalltalk` — the bus cutover is a
separate later step); `convoy ls` → `st2 agents`; teardown → `st2 down`. The **graders** (`grade.sh`/
`probe.sh`) are largely bus-agnostic (git attribution + message-thread + deliverable files).

Open scope question for M4b: express cells as **st2 `type=batch` jobs** (the native path — the executor
runs them), OR keep the shell fixtures and just repoint the harness at st2 (smaller, faster to green).
My lean: repoint the harness first (fastest path to a green signal), then express the canonical cells
as native batch jobs once the executor is proven.

## M5 — all cells green (the acceptance gate)

Run the migrated suite; every cell passes → the gate that earns the live swap.

## Scope questions for you

1. **M4a build scope** — confirm the executor above (stage-DAG + seat-spin + done-when watcher + matrix
   + verdict), reusing render/up/down/message. Start with a **single-variant, single-cell** end-to-end
   (spin → kick → done → grade → verdict) before the full matrix + all 44 cells?
2. **DQ1/DQ2** — confirm the M1 answers still hold (done = supervisor-confirmation-msg; matrix all-pass).
3. **M4b path** — repoint-the-harness-first (my lean) vs native-batch-jobs-first?
4. **Timing** — the maintainer's call: does M4/M5 gate the swap, or run alongside? (You flagged evals have
   product stakes + are demo material.)
