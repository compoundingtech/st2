# Sub-swap-1 (renderer + runner) — staged dry-run result

Prep for the morning fleet swap (SWAP.md). **Isolated dry-run, zero live-fleet touch.** The point:
prove the renderer+runner swap procedure AND its rollback *before* the maintainer pulls it live, so his morning
swap is a pre-tested one-step pull, not a first-time live attempt.

Reproducible: `scratchpad/swap_dryrun2.sh` (isolated short `PTY_ROOT` `/tmp/sdr2/pty`; benign `sleep`
stand-ins; the live registry `~/.local/state/convoy/default/pty` is only ever READ). `st2` @ HEAD.

## Result: sub-swap-1 WORKS — st2 adopts convoy's sessions; NO fix needed

> **Correction.** An earlier pass of this doc reported a "double-run" adoption gap. That was **wrong** —
> it mistook the `pty list` **display** column (`displayName`, e.g. `hetz.st2-claude`) for the on-disk
> session id. st2's reconcile matches the on-disk **`name`** field (`run.rs:161`), which for convoy's
> live sessions is `hetz.st2` / `hetz.st2.ding` — **exactly** what st2 render emits. Verified below
> against convoy's OWN renderer and the live fleet. No id change is needed; changing st2's ids would
> *cause* the mismatch.

Sub-swap-1 step 3: *"stop `convoy up`, start `st2 up`; agents keep running; st2 **adopts** them; zero
task downtime."* This holds. st2 keys adoption off the pinned task `id`, and st2's rendered ids equal
convoy's on-disk session ids:

| session | convoy on-disk id (its renderer + live fleet) | st2 rendered id | match? |
|---|---|---|---|
| agent | `<host>.<short>` — e.g. `silber.fabric` / live `hetz.st2` | `silber.fabric` / `hetz.st2` | ✓ |
| ding  | `<host>.<short>.ding` — e.g. `silber.fabric.ding` / live `hetz.st2.ding` | `silber.fabric.ding` / `hetz.st2.ding` | ✓ |

(`hetz.st2-claude` / `hetz.st2-ding` are only the human-facing `displayName`, not the adoption key.)

### Empirical proof (isolated)

- **Ground truth, live fleet (read-only):** the st2-claude agent's own convoy sessions have on-disk
  `name = hetz.st2` (agent) / `hetz.st2.ding` (ding); `displayName = hetz.st2-claude` / `hetz.st2-ding`.
- **Both renderers, same agent (`silber/fabric-claude`):** `convoy render` → `pty.toml` ids
  `silber.fabric` / `silber.fabric.ding`; `st2 render` → `agent.kdl` ids `silber.fabric` /
  `silber.fabric.ding` → **MATCH (agent AND ding).**
- **Adoption:** stood up a session under convoy's on-disk id (`silber.fabric`), then `st2 up --once` →
  `launched (1): silber.fabric.ding` only (the ding wasn't pre-created); the **agent pid 155310 →
  155310 UNCHANGED = ADOPTED** (no second process), and `st2 down` tore both down, confirming st2 owns
  them. The ding adopts by the same mechanism (its id matches too).
- **Isolation:** live registry **16 → 16 sessions, byte-identical** before/after. **Zero live-fleet
  state touched.**

## Render + validate

`st2 render` of the agent → catalog; `st2 validate` **CLEAN (0/0/0)**. Behavior-neutral wiring is
covered by `tests/render_neutrality.rs`; this dry-run additionally confirms the runner-bookkeeping
**session id** matches convoy's (the field the neutrality test does not diff).

## Rollback / reversibility

Reversible both directions. Forward: st2 adopts the live convoy sessions (zero downtime). Back: stop
`st2 up`, restart `convoy up` — it re-adopts the same sessions. And st2 never kills a session it didn't
declare, so a foreign session is never destroyed by an `st2 up`.

## Bottom line for the morning

- **Sub-swap-1 is ready-to-pull.** st2 renders + adopts + runs the fleet with the agents still on the
  smalltalk bus; adoption is proven against convoy's own renderer and the live fleet.
- **No code change required** for the runner swap (the earlier "id fix" was chasing a misread — do not
  apply it; it would introduce the very mismatch it purported to fix).
- Rollback is safe. The dry-run touched **zero** live-fleet state.
- Follow-up (nice-to-have, not blocking): extend `tests/render_neutrality.rs` to also assert the
  session-id equals convoy's, so this equivalence is locked and can't silently regress.
