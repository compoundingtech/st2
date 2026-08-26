# omp is a fifth native driver with its own channel and a hard version gate

Status: accepted

## Context

st2 maintains four typed drivers (claude, codex, pi, opencode), each pairing a pure KDL
expansion with a session wrapper that owns presence, publishes observed harness state, and
delivers inbox messages natively. omp — an earendil-works/pi-family harness in daily use on
this fleet — has none of this: seats are hand-authored tasks with no presence lease, no
observed state, and no delivery path. The question was how deep support should go: a full
native driver, an alias onto the pi driver's machinery, or a documented hand-authored
pattern.

## Options
| Option | Result | Reason |
| --- | --- | --- |
| Full native driver (fifth expansion arm, own wrapper, own channel) | Selected | The 2026-08-25 capture shows the pi mechanism ports and omp's approval events add an axis pi lacks. Cost: ~pi-driver scale code and a per-minor admission checklist. |
| Alias onto the pi driver | Rejected | Cheapest, but measured divergence makes it wrong: no `agent_settled` means the observed idle edge never fires or blips at `agent_end`, and version gates would pin the wrong harness. Rejected on evidence, not effort. |
| Docs-only hand-authored pattern | Rejected | No presence lease, observed state, or native delivery; inconsistent with all four existing drivers — not "first-class". |
| Blocked axis deferred out of v1 | Rejected | Smaller diff, but omp seats would read busy while actually waiting on a human — exactly what st2's wedged-agent signal exists to catch; both events verified firing (q2). |
| Warn-only version handling | Rejected | omp releases near-daily; silent degradation reads as healthy while a refused launch is loud (q3). |

## Evidence and Argument

Measured against omp v18.0.3 on 2026-08-25; full record:
[`06-omp-driver/.experiments/2026-08-25-omp-harness-integration.md`](../06-omp-driver/.experiments/2026-08-25-omp-harness-integration.md).

- **The pi mechanism ports.** omp loads pi-style extensions; its extension argument carries
  the same `sendUserMessage` / `sendMessage` / `on` calls; a live interactive run delivered
  an idle message end-to-end and drove a complete model turn without touching a screen.
- **But it is not pi.** `agent_settled` — the pi channel's entire idle edge — does not exist
  (absent from the binary); idle must be derived by polling `ctx.isIdle()` after
  `agent_end`. Conversely omp is *richer* than pi where it matters for st2: it exposes
  `tool_approval_requested` / `tool_approval_resolved` with a `toolCallId`, giving the
  blocked-on-human axis pi cannot express at all.

An alias onto the pi driver would bake both divergences into the wrong place: observed state
would hang waiting for an event that never fires or blip idle at the wrong boundary, and
each future divergence would become a special case inside "pi". The measured differences
are exactly why the channel forks rather than shares a file.

## Decision

1. **Full native driver** (`driver omp` block + `omp-session` wrapper + `omp-channel.ts`
   extension + `omp-channel` process), per OMP-R01.
2. **Own channel asset, forked from pi's**, not a shared parameterized file — the idle-edge
   and approval logic differ per harness (OMP-R04).
3. **Publish the blocked-on-human axis in v1** from the two approval events (OMP-R02) —
   verified firing, not speculative.
4. **Hard version gate on the minor** (18.x initially) under the codex/opencode admission
   convention, because the delivery-critical surface is versioned behavior, not API contract
   (OMP-R05). Chosen over warn-only despite omp's near-daily releases: a silently degraded
   fleet reads as healthy; a refused launch reads as what it is.
5. **No DING screen adapter for omp** in v1 (OMP-T03), matching the other native-channel
   drivers.

Interview decisions q1–q4 (2026-08-25, Johannes): full native driver; blocked axis included;
hard gate; VRS lives in [`06-omp-driver/`](../06-omp-driver/).

## Consequences

- A fifth expansion arm, wrapper module, channel process, hook asset, and ding launch
  classification follow the existing per-harness pattern.
- Every omp minor bump costs one capture run before admission; the required checklist is in
  the subsystem spec.
- The deny path, ask-axis discrimination, steer/modal interactions, and update-banner
  suppression remain open (`DQ-OMP-1..5`) and bound v1's claims.
