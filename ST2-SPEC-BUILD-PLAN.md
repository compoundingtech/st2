# st2 spec build plan — native eval/agent format

Build plan for the **st2 spec** nailed down in review. This is a **gate**:
plan → checked against the design → then build. No code until approved.

Replaces the wrapped/generated `type = batch` approach (a line-by-line review killed it). Command
is `st2` everywhere; only `pty` on PATH; zero convoy/`st`/coord strings.

## What the format is (my read of the contract)

One `.kdl` file + a folder. `st2 eval ./folder` runs it end-to-end; `st2 up ./folder` boots just the
top-level team. Constructs:

- `env { }` — **cascades** top-level → agent → process (child overrides parent). Declared once at the
  highest shared level.
- `team "name" { }` — nests agents and **prefixes** their ids (`team "mix"` → `mix.sup`, `mix.worker`).
- `agent "id" { }` — the label **is** the id (no `identity`/`host`/`type`). An **agent IS one pty**: its
  own `command` + `env` + `workspace` are the main process; extra processes follow as `exec` blocks.
- `exec "id" { }` — an extra process under the agent (inherits the agent's env). The ding is one, kept
  **fully expanded** for now with a `# ding {}` comment; the future `ding {}` collapses to it —
  **st2 unit-tests that expansion**.
- `eval { }` — only `st2 eval` runs it: `copy "./fixture"` (1:1 into the temp catalog = the start world),
  `message { from to content }` (kickoff, identical to `st2 message send`; content = file or inline),
  `max-timeout`, optional eval-only `agent`/`team` (e.g. a judge), and `judges { }`.
- `judges { }` — **all must pass**, each with an optional `timeout`. Three flavors:
  - **declarative**: `file "p" has/lacks "…"`, `json "p" field "x" is "y"`, `committed "p"`.
  - **bash**: `exec "sh ./judges/x.sh"` (exit 0 = pass).
  - **ask-agent**: `ask "judge" "<prompt incl. exact reply format>"` — message the judge agent, wait for
    its reply, read PASS/FAIL. The judge is a real `agent { }`; its command line *is* the model/harness.
- **eval-runner = the `requester` identity**: sends the kickoff, receives the sup's confirmation (the done
  signal), sends the judge `ask`s.

## Architecture — reuse vs new

REUSE (already built, load-bearing): the `SystemRunner` spawn/kill (pty via `pty run`, exec via
`ExecBackend`), `isolate` (per-task systemd scope), `effective_pty_root` (short hermetic root for the
104-byte socket limit — the eval mints one like `st2 batch` did), the `message` module (send/inbox/list),
`st2 ding`, `pretrust`, teardown (`down`/pgroup-kill).

NEW:
1. **Spec parser** (`src/spec_kdl.rs` or similar) — the single-file format above → an in-memory model
   (`Spec { env, teams/agents, eval? }`). Env cascade resolution, team id-prefixing, agent-is-pty,
   exec blocks, eval block, judges (3 flavors). NOT the existing `agent.kdl`/IR parser (different shape).
2. **Team boot** — from a parsed spec, spawn each agent's main pty + its exec blocks with the resolved
   cascaded env, isolated, teardown-clean. Backs both `st2 up <spec>` (boot + supervise) and the eval
   flow. No catalog discovery/reconcile — direct team boot from the one file.
3. **`st2 eval <folder>`** — mint a temp catalog + short PTY_ROOT; `copy` the fixture 1:1; boot the base
   team + eval agents; pretrust the workspaces; deliver the kickoff message (requester→sup); wait for the
   sup→requester confirmation (done) or `max-timeout`; run judges; teardown; verdict = all-pass.
4. **Judge engine** (`src/judge.rs`) — declarative (file has/lacks, json field is, committed), bash
   (exec exit 0), ask-agent (message the judge, wait for reply, parse PASS/FAIL), per-judge timeout,
   all-must-pass, legible per-judge report.
5. **`st2 render <path|glob>`** — TBD, see Q2.

## Build phases (each test-tied; I walk my own diff + run the two real evals)

- **P1 Parser + model** — parse the reference `license-mit.kdl` into the model; env-cascade + team-prefix
  + agent-is-pty + eval + all 3 judge flavors; **unit-test the `# ding {}` → `exec` expansion** (design
  requirement). Round-trip/validate the reference example.
- **P2 Team boot + `st2 up <spec>`** — spawn the team from the spec (reuse SystemRunner/isolate), env
  cascade applied per process, ids team-prefixed; clean teardown; isolation test (no leak).
- **P3 `st2 eval` flow** — temp catalog + copy + boot + kickoff + wait-done + teardown; the requester
  identity + done-on-confirmation. Integration test with a benign team (like the batch demo) proving the
  flow end-to-end without a real harness.
- **P4 Judge engine** — the 3 flavors + per-judge timeout + all-must-pass; declarative/bash unit-tested;
  ask-agent tested with a benign judge stand-in.
- **P5 Prove the two real evals** — with evals-claude authoring `license-mit/` and `ghost-bug/` folders in
  this format, `st2 eval ./cells/<name>/` runs each from its folder only. CoS verifies by running + reading.
- **P6 `st2 render`** (pending Q2) + retire `type = batch`/`st2 batch`/`batch.rs` once the two evals pass
  natively (pending Q3).

## Coordination
evals-claude authors the two eval **folders** (`.kdl` + `fixture/` + `task.md` + `judges/*.sh`) to this
format; I build the **engine**. We align on the exact surface via the reference example. Its `gen-batch.sh`
+ `type=batch` specs are superseded (retire per Q3).

## Runner/judge-engine details (settled with evals-claude, 2026-07-23)

These are my engineering calls (they don't touch the design); evals-claude authors the two eval folders
to match. Surfaced here so the CoS sees them in the plan review.

- **Git in the fixture — `_git` → `.git` copy rename.** Every judge diffs `base..HEAD`, so `fixture/worker/`
  must ship a real repo (base commit + pinned author). But a committed `.git` nests as a gitlink inside the
  evals repo. So `st2 eval`'s `copy` renames any directory named `_git` → `.git` (recursively, by basename)
  in the run catalog. The fixture stores the git db at `worker/_git/`, working-tree files as normal READABLE
  files matching the base commit; copy lands a live repo, no git needed at copy time, working tree stays
  legible. (Considered a git-bundle; rejected — less legible, needs git at copy.) `_git` is a reserved name.
- **Judge exec env.** CWD = the copied catalog root (= `$CATALOG`); `$CATALOG` exported to every judge exec
  (as the batch stages already do). Bus dirs use **bare team-dotted ids, no host prefix**
  (`$CATALOG/smalltalk/mix.sup/{inbox,archive}`, `…/requester/…`, `…/judge/…`) — the new spec has no host
  and `team "mix"` prefixes to `mix.sup`. (The `smalltalk` path segment itself is Q4.)
- **Exec flavor — shebang/bash honored, never forced through POSIX sh.** A bash-judge `exec "<cmd>"` runs via
  `sh -c "<cmd>"`, so `exec "bash ./judges/x.sh"` runs bash (arrays/`[[ ]]`/`local`/procsub fine) and
  `exec "./judges/x.sh"` honors the shebang. Closes the phase-2 127-wrapper bug.
- **Done-signal discriminator** = Q1 below (pending); evals codes fixtures defensively meanwhile.

## Open questions (the gate — I want these settled before P3+)

**Q1 — the done signal vs the ack (the big one).** The design says the requester "receives the sup's
confirmation (the done signal — no polling)". But we just proved (ghost-bug) that a well-behaved sup
**acknowledges the kick before doing the work** (sup→requester "received, delegated…"), so "first
sup→requester message" fires on the ack → judges run before any work. Options: (a) **persona/task
convention** — the sup sends only the FINAL confirmation to the requester (acks stay internal); simplest,
but relies on authored discipline. (b) fire on the sup→requester message that **post-dates a worker→sup
report** (the gate grader's own discriminator). (c) keep a **grade-poll** fallback (the just-shipped
`done-when { grade }`) as the robust option, though the design says "no polling". Which do you + the maintainer
want? My lean: (a) as the design's default (legible, no magic), with (b) available if acks-to-requester
turn out common. This is the one that most affects whether license-mit/ghost-bug pass honestly.

**Q2 — what does `st2 render <path|glob>` render, in THIS phase?** The doc lists it as a new verb but the
higher-level agent-spec → st2-spec render is explicitly "later." So now: (a) materialize a spec's team
into the on-disk catalog form (for inspection / `st2 up`), (b) a placeholder stub for the future
agent-spec render, or (c) something else? Need the target + output shape.

**Q3 — retire `type = batch` now or after the two evals pass?** The native `st2 eval` replaces it. Plan:
build `st2 eval`, prove license-mit + ghost-bug natively, THEN remove `st2 batch` + `batch.rs` +
`gen-batch.sh` (evals-claude's). Confirm that sequencing (don't want two eval paths lingering).

**Q4 — bus layout + the "smalltalk" dir name.** The example's `ST_ROOT`/ding use `$CATALOG/smalltalk`.
Keep that dir name (operated by `st2`, `st` binary gone) or rename it (design wants zero convoy/coord
strings — is "smalltalk" as a dir name acceptable, or rename to e.g. `$CATALOG/bus`)? And confirm the bus
is the smalltalk-compatible flat layout `st2 ding`/`st2 message` already speak.

**Q5 — ST_AGENT: author-set or auto-derived?** The example sets `env { ST_AGENT "mix.sup" }` per agent.
Since the agent id is already the team-prefixed `mix.sup`, st2 could **auto-set** ST_AGENT from the id
(less duplication, matches the "declare once" spirit). Auto-derive, or keep it explicit as in the example?

**Q6 — path semantics.** `workspace "./sup"`, `copy "./fixture"`, `content "./task.md"`,
`file "worker/LICENSE"`, `exec "sh ./judges/x.sh"` — I read: `copy`/`content`/`workspace`/judge-script
paths are relative to the **spec file's folder**; declarative judge paths (`worker/LICENSE`) are relative
to the **temp catalog root** (the copied world). Confirm, and confirm the fixture layout
(`fixture/{sup,worker,…}` → copied to the catalog root, agents workspace into `<catalog>/sup` etc.).

**Q7 — ask-agent PASS/FAIL parsing + judge run order.** Reply format is "PASS or FAIL, then one sentence."
Parse = the reply contains `PASS` and not `FAIL` (leading token)? And do judges run in declared order,
collecting ALL results (legible full report) rather than short-circuiting? My lean: yes to both.
