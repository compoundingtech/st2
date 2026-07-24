# Evals → st2 migration — the pre-swap trust gate

**Status:** PLAN for CoS gate (mapping + sequence) before the big port. **Owner split:** st2-claude leads
the **st2 side** (the surfaces the eval harness calls); evals-claude owns the **evals repo** (the harness
`bin/lib-harness.sh` + the 45 cells + graders). This doc does not edit the evals repo — it is the shared
contract + the sequence.

## The gate

the maintainer's logic: when every eval that passed on convoy passes on st2 — honest green, held-out graders, no
hollow passes — we **know** st2 works for the same use cases convoy did. That is the trust gate that
green-lights swapping the live fleet. This runs BEFORE the swap, not after.

**Done-bar:** every convoy eval reproduced GREEN on st2, matching its convoy result, at the evals-integrity
standard (isolation hard-gate + held-out acceptance + cross-family quality judge — no gamed passes).

## The strategic call (the CoS delegated this to me)

**Repoint the existing harness from convoy → st2's runner surfaces (`render`/`up`/`down`/`message`/`ding`).
NOT a `type = batch` rewrite, NOT an IR-for-evals authoring layer — neither is the vehicle for this gate.**

Why:
- The gate's whole logic is *"the SAME cell that passed on convoy passes on st2."* That requires running the
  **same** fixtures, personas, kicks, graders, and sandboxes — swapping only the runner underneath. A
  `type = batch` port **re-authors** each cell into a new declarative model, so a green batch job proves *a
  new cell passes*, not *the existing cell reproduces*. That breaks the equivalence proof the gate rests on.
- st2 `render` is already **byte-neutral with convoy render** (`tests/render_neutrality.rs`) and st2 has
  `up`/`down`/`message`/`ding`/`doctor` — so the repoint is mostly a drop-in, and where st2 diverges from
  convoy we WANT the eval to catch it (that's the gate working).
- The `type = batch` executor (M4a, shipped) + an IR-for-evals authoring path are genuinely valuable — as a
  **future native authoring model for NEW evals**, decoupled from this gate. Building them now would delay
  the gate and muddy the equivalence. Recommend: **defer both; revisit after the swap.**

So the batch executor is orthogonal to this migration. The migration is: **repoint the harness + close the
st2-side surface gaps + reclassify the convoy-mechanism cells.**

## The surface the harness needs (convoy → st2 contract)

A team cell's `spin.sh` drives, in order (verified in `cells/ghost-bug/fixture/spin.sh`):

| Harness call (convoy/smalltalk) | st2 equivalent | Status |
|---|---|---|
| `convoy init <NET>` | isolated catalog + bus dirs | **GAP** — no `st2 init`; needs an init-equivalent (or harness `mkdir` + `st2 render` seeds bus dirs) |
| `convoy pretrust <dir>…` | batch workspace pre-trust before boot | **GAP** — st2 has no pretrust; without it multi-spawn flakes on the trust dialog |
| `convoy add <role> --identity --network --dir --persona --harness [--mcp]` | author a runnable catalog agent from imperative flags | **GAP** — `st2 render` takes IR, not flags; **and** no codex rig |
| `convoy up --once <NET>` | `st2 up <catalog> --host <H> --once` | ✓ present |
| `convoy down <NET> --force` | `st2 down <catalog>` | ✓ present (confirm force/idempotent parity) |
| `convoy ls <NET>` | `st2 ls <catalog>` / `st2 agents` | ✓ present |
| `st message send` (kick, on the isolated bus) | `st2 message send` (native bus, smalltalk wire-compatible) | ✓ present (bus decision below) |
| ding sidecar (convoy add wires it) | `st2 render` wires ding + `st2 ding` | ✓ claude (neutrality-proven); **GAP** codex |

## The 45 cells split into THREE migration classes

Not every cell "runs on st2" the same way — the survey (`cells.manifest` + per-cell `task.toml`) shows three
distinct classes, each with a different migration path:

### Class 1 — team-loop evals (~23): THE trust-gate core
`ghost-bug, incident-response, poisoned-pr, security-audit, migration, signal-rename, test-writing,
feature-fit, docs, fork-in-the-road, team-standup, tui-build, restart-continuity, license-mit, ding-mode,
ding-reply, inbox-hygiene, hook-integrity, skill-inheritance, weird-git-setup, crash-ding` + the codex
variants (`ghost-bug-codex, fork-in-the-road-codex, poisoned-pr-codex, restorability-codex,
license-mit-codex`).

These grade an **agent team doing real work** on a real bus. They repoint cleanly: swap the harness's
`stev_convoy_*` for st2 surfaces; fixtures/personas/kicks/graders unchanged. **This is what proves st2 =
convoy as a runner** — the heart of the gate.

### Class 2 — convoy-mechanism cells (~14): grade convoy's OWN commands
`convoy-doctor-{canwork,foreign-box,preinit,structure,teardown}, convoy-init-{narration,structure},
convoy-add-structure, convoy-worktree-cutting, convoy-network, clean-compose, compose-config-load,
compose-global-skill, restorability`.

These assert **convoy's** render/lifecycle/doctor behavior (e.g. `convoy doctor --full`, `convoy init
--megarepo`, `convoy add` overlay structure). They do **not** "run on st2" — they test a tool that is being
retired. Each needs a per-cell **decision** (needs the maintainer/CoS, see open questions):
- **Port the assertion to the st2 equivalent** where st2 has one — `convoy doctor` → `st2 doctor`,
  `convoy-add-structure`/`clean-compose`/`compose-*` → the st2 `render` overlay (neutrality already proves
  st2 produces the same wiring, so an `st2-render-structure` cell is a near-mirror), `convoy-init-structure`
  → the st2 catalog/bus layout.
- **Retire** where the behavior is convoy-only and disappears with convoy (e.g. `convoy-init-narration`,
  `convoy-worktree-cutting` if st2 doesn't own worktree-cutting).
- **Keep as convoy-regression** during the transition (they still guard convoy while both run).

This class is the biggest judgment call and the main thing to settle with the maintainer + evals-claude — it is NOT
a mechanical repoint.

### Class 3 — substrate / onboarding (~6): bus + pty + first-run
`two-networks-coexist, pty-send-peek` (deterministic bus/pty isolation — repoint the bus/pty root to st2's;
several are LLM-free so they're fast, high-signal pilots), `bootstrap-network, first-run` (onboarding,
per-gate friction grading — `bootstrap-network` uses `convoy init`/spawn, so it repoints with Class 1).

## st2-side work items (MINE, each test-tied)

1. **Codex rendering** — `st2 render` must emit the codex rig (AGENTS.md from persona, codex session, a
   `st ding`/`st2 ding` wake sidecar since codex has no asyncRewake, `~/.codex/config.toml` pre-trust),
   matching `convoy add --harness codex`. Blocks the 5–6 codex cells. *Test:* render a codex agent →
   assert the rig + a render-neutrality diff vs convoy's codex output.
2. **Imperative agent authoring** — a convoy-add-shaped st2 surface: `st2 render`-from-flags (or extend
   `st2 add`) taking `--identity --dir --persona --harness --role`, writing the runnable catalog agent
   directly (claude + codex). Cleanest drop-in for the harness (near-mechanical `s/convoy add/st2 …/`).
   *Test:* flags → catalog agent.kdl byte-matches the IR-rendered one.
3. **Workspace pre-trust** — an `st2 pretrust <dir>…` batch equivalent (or fold pre-trust into
   `st2 up`/render), so multi-spawn teams don't stall on the trust dialog. *Test:* multi-agent spin, assert
   no trust-dialog stall.
4. **Init-equivalent** — decide `st2 init <catalog>` vs harness `mkdir` + render seeds bus dirs. *Test:* a
   fresh catalog boots a team.
5. **Reconcile-GC-race interaction (RAISED PRIORITY)** — the pre-existing race I flagged (under concurrent
   `pty list` load, reconcile can transiently see a live pty as Dead and GC it) will bite eval runs, which
   spin **concurrent multi-agent pty teams**. This is now on the critical path for honest green — flagging
   that the CoS-tracked follow-up likely needs to land before/with the team-loop bulk. Not starting it
   without the CoS's separate scope (per the standing instruction), but the migration surfaces its urgency.

## evals-side work items (evals-claude's repo — I do NOT touch)

- Repoint `lib-harness.sh` `stev_convoy_{init,add,teardown}` + `stev_seed_kick` from convoy/smalltalk to the
  st2 surfaces above (the contract table). Uniform — most Class-1 cells inherit it for free.
- Class-2 per-cell decisions (port-to-st2-equivalent / retire / keep-as-convoy-regression).
- Confirm persona composition (task-lane + boot + BASE + role, SHA-pinned) lines up with st2 render's
  overlay (M3.1 does the overlay; Q4 from the M4b contract).
- Grader + held-out + sandbox `setup-sandbox.sh` stay unchanged (harness-agnostic) — confirm.

## Sequence

1. **PILOT — one Class-1 claude cell end-to-end (`ghost-bug`).** Manually drive st2 against its unchanged
   fixtures (render the 2 agents → `st2 up --once` → seed kick via `st2 message` → run to `done` → run
   `grade.sh` → `st2 down`), WITHOUT editing the evals repo. Proves the surfaces + surfaces every real gap;
   hands evals-claude a concrete repoint recipe. (Needs real claude for the team — the first honest run.)
2. **Close the st2-side gaps** the pilot exposes (items 1–4 above), each test-tied.
3. **Repoint the harness** (evals-claude) → the **Class-1 claude bulk** goes green, matching convoy.
4. **Codex** — land st2 codex rendering → the Class-1 codex cells.
5. **Class-3 substrate** — the deterministic bus/pty cells (fast, LLM-free) as continuous regression.
6. **Class-2 decisions** — settle port/retire/keep with the maintainer + evals-claude; execute.
7. **Full green** — all runnable cells green on st2 = the gate met = swap unlocked.

Pilot-first is deliberate: one real cell end-to-end de-risks the whole port before the bulk, exactly like
the isolation work (prove it on one real case, then scale).

## Open questions (need the maintainer / evals-claude)

- **Q-A (the maintainer):** Class-2 convoy-mechanism cells — which **port to an st2 equivalent**, which **retire**
  with convoy, which **stay as convoy-regression** during the transition? Biggest scoping decision.
- **Q-B (the maintainer/CoS):** **bus** — run the team-loop cells on st2's **native bus** (`st2 message`, full st2)
  or keep the **smalltalk** bus (wire-compatible) for the runner-first phase, matching SWAP.md's two
  sub-swaps? Recommend: native st2 bus for the gate (proves the whole st2 stack), unless we want to isolate
  the runner variable first.
- **Q-C (evals-claude):** does render's persona overlay match the SHA-pinned eval composition exactly? (M4b
  Q4 — likely yes, confirm.)
- **Q-D:** the reconcile-GC-race follow-up — does it land before the team-loop bulk (it bites concurrent
  multi-agent runs)?

## Boundary

st2-claude owns the st2 surfaces + this contract + driving the pilot; evals-claude owns the harness repoint +
Class-2 cell decisions + graders. Neither reaches into the other's repo. This plan is the hand-off surface.
