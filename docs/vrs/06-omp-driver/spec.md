# omp driver specification

This document specifies the omp native driver implementation. It builds on
[requirements.md](./requirements.md) (OMP-R01..R05, OMP-T01..T03) and the
measured surface in
[`2026-08-25-omp-harness-integration.md`](./.experiments/2026-08-25-omp-harness-integration.md).

## Status

Implemented in this change set: the `omp` driver block and expansion, the
`omp-session` wrapper with the hard 18.x gate, the `omp-channel.ts` asset (type-checked and
smoke-driven under `checks.pi-extension-types`), and the shared channel loop's blocked-frame
parsing. The driver-level decisions are recorded in
[decision 0007](../.decisions/0007-omp-is-a-fifth-native-driver-with-its-own-channel-and-a-hard-version-gate.md).
Open questions are tracked in [open-questions.md](./open-questions.md).

## Overview

```text
agent spec                    expansion (pure)              runtime
──────────────                ──────────────────            ─────────────────────────────
driver omp {                 ┌──────────────┐   task argv: st2 driver omp-session
  model    "…"               │ expand_omp    │             --identity … --runtime-id …
  thinking "high"     ─────► │  in driver.rs │──────────►  -- <omp --model … -e <set>/omp-channel.ts>
  prompt   "…"               └──────────────┘                        │
}                                                          wrapper: presence lease,
                                                           terminal harness-state record
                                                                   │ spawns
                                                           st2 driver omp-channel --identity …
                                                            ▲ newline-JSON frames over stdio
                                                           omp-channel.ts (in-process extension)
```

## Driver block

`OmpDriver { model: Option<String>, thinking: Option<String>, prompt: String }`, KDL
kebab-case, mirroring `PiDriver`. omp has no effort flag; its analog is `--thinking`
(off|minimal|low|medium|high|xhigh|max|auto), so the field is named for what omp exposes.
Expansion prepends `--model` and `--thinking` when set; extra args ride the existing driver
args escape.

## Wrapper (`src/omp_session.rs`)

Shape of `pi_session.rs`:

- Resolves the agent dir, installs the signal handler, claims the observed-state record under
  a freshly minted session token.
- Version gate first: runs `<omp> --version`, parses a strict `MAJOR.MINOR.PATCH` release, and
  admits only a MINOR already measured (`SUPPORTED_OMP_MINORS`); outside that set the launch
  fails loudly with the measured-checks message, per OMP-R05 and decision 0007. Admission is per
  minor: a patch inside an admitted minor launches without new evidence, and a later *minor*
  stays rejected until the checks are repeated against it. The parse is what keeps "per minor"
  from decaying into "starts with 18" — minors are compared numerically (`18.10` is not `18.1`),
  exactly three components are required, and a pre-release or build-metadata suffix
  (`18.0.9-rc1`) does not parse at all, so it is never admitted as its base release.
  Which token the release is read FROM matters as much as how it parses: an `omp/<release>`
  token is omp naming itself and decides outright, and if what it named cannot be parsed the
  gate refuses rather than reading some other token in the banner. Otherwise `omp/18.1.0-rc1
  18.0.9` would launch an unverified provider on the strength of a version omp never claimed —
  which is the shape DQ-OMP-5's update banner could produce. With no own label, every parseable
  release in the banner must agree.
- Injects the channel extension from the verified hook set (`with_channel_extension` shape —
  resolved from this binary's immutable asset, never a catalog-pinned path).
- Applies offline defaults (`PI_OFFLINE=1`, `PI_SKIP_VERSION_CHECK=1`) unless the operator's
  declaration already set them; suppression of the update banner itself is DQ-OMP-5.
- Exports the channel env (`ST2_OMP_CHANNEL_{BIN,CATALOG,IDENTITY,RUNTIME_ID,SESSION,SEQ}`)
  using fresh names — an omp seat must never adopt a stray pi channel env.
- On provider exit writes the terminal observed record; presence decays by staleness as for
  pi (SIGKILL produces no terminal event).

## Channel (`hooks/omp-channel.ts`)

Forked from `pi-channel.ts`; same frame protocol discipline (LF-delimited JSON, hello /
message / delivered / failed / state frames, PROTOCOL constant). Differences:

- **Idle edge:** on `agent_end`, poll `ctx.isIdle()` every ~100 ms with a bounded window;
  emit `{type:"state", state:"idle"}` at the first true sample. `agent_start` emits active.
  No `agent_settled` listener exists.
- **Blocked axis:** `tool_approval_requested` → state frame carrying
  `blockedOn:"human", ask:"permission"`, reason `"<toolName>:<truncated command if
  available>"`; `tool_approval_resolved` → exit edge restoring prior activity/idle per
  OMP-R02. Frames extend the state frame with optional fields; unknown-frame readers ignore
  extras.
- **Session lifecycle:** `session_start` opens the channel and seeds state from
  `isIdle()`; replacement sessions close their predecessor exactly as the pi channel does
  (close-named-channel rule); `session_shutdown` closes only on `reason === "quit"`.
  No abort-signal wiring exists (`ctx = {ui}`).
- Restored-context seeding uses `sendMessage({customType:"st2-session-start", …},
  {deliverAs:"nextTurn"})` unchanged.

## Rust channel process

`st2 driver omp-channel` reuses the pi channel's loop (`pi_channel.rs`) parameterized by
harness label `"omp"`; the state frame parser accepts the blocked fields. The ding side
gains no omp adapter (OMP-T03): delivery is channel-only, failing closed when absent.

## Admission evidence required for a new minor

Per OMP-R05 and decision 0007, admitting a new omp MINOR requires re-running: extension-load
probe, lifecycle event inventory, idle-edge sampling, approval-event capture, live delivery
loop — updating the `.experiments/` capture and `SUPPORTED_OMP_MINORS` together. Each probe must
record measured output; a minor that was not measured is not admitted, so the admitted set is
asserted literally in the wrapper's tests.

Patches inside an admitted minor cost nothing: omp releases near-daily, and gating them blocked
the fleet on changes the capture already covered — 18.0.10 shipped within hours of 18.0.9 being
admitted. The evidence a minor is admitted on is a measurement of *some* release in that minor,
and the risk accepted is that a patch could move delivery-critical behavior within it. That has
been observed once and absorbed: between 18.0.3 and 18.0.9 the idle edge moved from ~251 ms to
~25 ms, which the bounded polling rule (OMP-R03) handles without change.

Captures, per measured release:

- 18.0.3 — [`2026-08-25-omp-harness-integration.md`](./.experiments/2026-08-25-omp-harness-integration.md)
  (also the original port evidence).
- 18.0.9 — [`2026-08-28-omp-18-0-9-admission.md`](./.experiments/2026-08-28-omp-18-0-9-admission.md).
