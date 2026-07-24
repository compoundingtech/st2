# Migration swap runbook

How the live `hetz`/`silber` fleet moves from **convoy + smalltalk** to **st2**, without losing what we
have. This is the PLAN — **do not execute the live-fleet swap without the maintainer's explicit go** (after the
morning demo + the evals gate). Every step is **reversible** and **verified** before the next.

## Gates (all must hold before ANY live step)

- [x] **Full smart-parity** — [PARITY.md](PARITY.md): every row DONE (test-backed) or justified N-A.
- [x] **Render neutrality proven** — `tests/render_neutrality.rs`: st2 render == convoy render, byte-for-byte
  (command/env/ding/persona/DING-BUS/loader/boot-hooks), isolated diff.
- [x] **Nomad decoupling proven** — `tests/nomad_survival.rs`: killing the runner leaves tasks alive;
  a fresh runner adopts them; only explicit teardown kills. This is what makes the runner swap safe.
- [ ] **Evals green on st2** — M4/M5, THE trust gate. The swap does not happen until every eval cell
  passes on st2.
- [ ] **Live demo validated** — the maintainer sees render → up → message on an st2-rendered catalog.

## The swap is TWO sequenced sub-swaps, each proven independently

Minimize dependencies: swap one thing at a time, verify, keep the previous path as rollback.

### Sub-swap 1 — the RENDERER + RUNNER (behavior-neutral; agents stay on the smalltalk bus)

The rendered agent wires identically to convoy (proven), so the fleet keeps talking on the **same
smalltalk bus** (`ST_ROOT=$CATALOG/smalltalk`, `st ding`, `st message`). Only *who renders* and *who
supervises* changes.

1. **Render the fleet through st2, in parallel.** For each agent, `st2 render <ir> <catalog-new>` into a
   NEW catalog dir (don't touch the live one). Diff each rendered `agent.kdl` + workspace overlay
   against convoy's current output — expect neutral (the test proves the shape; confirm on the real IR).
2. **Canary one agent.** Point one non-critical agent's workspace overlay at the st2 render; let convoy
   still run it. Confirm it boots + talks identically. Rollback: re-render with convoy (git-excluded
   overlay, no tracked-state change).
3. **Swap the runner.** `convoy down` is NOT used here — the Nomad property means we stop `convoy up`
   and start `st2 up <catalog>`; agents keep running; **st2 adopts** them (proven). Verify with
   `st2 doctor <catalog>` (supervisor up, tasks alive, presence fresh) + `st2 agents --json`.
   - **Rollback:** stop `st2 up`, restart `convoy up` — it adopts the same live sessions. Zero task
     downtime either direction (that's the whole point of the decouple gate).
4. **Cut the renderer.** Once st2 is supervising, retire convoy render (agents already st2-rendered).

At the end of sub-swap 1: **st2 renders + runs the fleet; agents still on the smalltalk bus.** Fully
reversible up to step 4.

### Sub-swap 2 — the BUS CUTOVER (smalltalk → st2-native)

The riskier one: the message layout changes from smalltalk's flat `<root>/<id>/inbox` to st2-native
unified `<catalog>/<host>/<id>/resources/inbox`, and agents move from `st ding`/`st message` to
`st2 ding`/`st2 message`. **This needs its own parity/neutrality proof** — M2's wire-compatible bus
(same `<unix-ms>-<rand6>.md` format) is the head start, but the LAYOUT differs.

1. **Prove bus parity** — a test like the render-neutrality diff but for the bus: a message sent via
   `st message` and read via `st2 message` (and vice versa) round-trips; layouts map 1:1.
2. **Migrate in place** — copy each `<root>/smalltalk/<id>/{inbox,archive}` → `<catalog>/<host>/<id>/
   resources/{inbox,archive}` (filenames unchanged — same grammar). Context/resources likewise.
3. **Re-render agents to native wiring** — flip the rendered `exec "ding"` from `st ding … --root
   $CATALOG/smalltalk` to `st2 ding …`, and agents' DING-BUS.md CLI refs from `st` to `st2`. `st2 up`
   reconciles the new ding.
4. **Verify** — dings deliver on the new inbox; `st2 agents`/`status`/`message`/`thread` all work
   against the unified layout; nothing left in the old smalltalk root.
   - **Rollback:** the old smalltalk root is untouched until this is verified; flip the ding back.

At the end of sub-swap 2: **fully st2-native — convoy and smalltalk are retired.**

## Order of operations (live day, the maintainer's go)

1. Demo validated + evals green (gates).
2. Sub-swap 1 (renderer+runner), verify with `st2 doctor` + a live message round-trip, soak.
3. Sub-swap 2 (bus cutover) once its parity test is green, verify, soak.
4. Decommission convoy + smalltalk.

Each step: reversible, verified, soaked before the next. Real fleet, real cost — deliberate, not fast.
