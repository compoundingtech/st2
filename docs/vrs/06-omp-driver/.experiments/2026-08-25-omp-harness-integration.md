# omp harness integration surface

## Question

Does st2's pi-driver mechanism — an injected TypeScript extension speaking newline-delimited
JSON frames over stdio to a channel process — port to omp, and what diverges? Sub-questions:
which lifecycle events exist, when does omp become provably idle, does native delivery drive a
real turn, and is there any waiting-on-human signal?

Date: 2026-08-25. Binary: omp v18.0.3 (`omp` on dev3 PATH, Nix store
`zv3xvic38a2gcdpxk3wd5fsk6plsgvbn-omp-18.0.3`). Linux x86_64, driven through live `omp`
processes (print mode for API probes, a PTY-held interactive TUI for lifecycle and delivery
runs).

## Method


Six throwaway TypeScript extensions, each loaded via `omp -e ./probeN.ts`, writing observations
to files beside them. Print-mode runs used `--no-session --no-tools --no-lsp --no-skills
--no-rules`. Interactive runs held the TUI in a supervised PTY session and drove prompts by
typing into the pane; one run forced approvals with `--approval-mode always-ask`.

## Result

**omp is pi-family.** omp reads pi's env fallbacks (`PI_SMOL_MODEL` documented in `--help`),
loads pi-style default-export TypeScript extensions unchanged at the module boundary, and its
extension first argument carries the same three calls st2's pi channel uses:
`sendUserMessage(content, options?)`, `sendMessage(message, options?)`, `on(event, handler)`.
The argument additionally carries an internals namespace under `.pi`; irrelevant to st2.

**Lifecycle events use pi's names, minus `agent_settled`.** Observed firing in one interactive
turn: `session_start`, `agent_start`, `turn_start`, `message_start`, `message_end`,
`turn_end`, `agent_end`, `session_shutdown`. `agent_settled` never fired and the string does
not occur anywhere in the binary (1M-string scan). Handler registration for unknown names does
not throw, so absence is silent.

**The idle edge exists but is sampled, not evented.** In the interactive run,
`ctx.isIdle()` was still `false` at `agent_end` and flipped `true` by the +251 ms sample. Rule:
idle is `agent_end` followed by bounded polling until `isIdle()` is true; a queued follow-up
turn keeps it false, so no spurious idle blip.

**Idle delivery lands and drives a full turn.** Interactive run with tools enabled: extension
polled a trigger file every 200 ms, called `pi.sendUserMessage(body)` while idle; the model
received it, acted (wrote the requested file correctly), turn events fired, and the session
settled back to idle. End-to-end native delivery confirmed without touching any screen.

**Mid-turn steer accepted in print mode.** From inside `message_start`, `sendUserMessage(text,
{ deliverAs: "steer" })` returned without error. Not yet visually verified in the interactive
TUI (DQ-OMP-4).

**Approval events exist and fire — omp is richer than pi here.** Under
`--approval-mode always-ask`, a bash call produced:

```
tool_approval_requested { type, sessionId, toolName: "bash", toolCallId, approvalMode }
tool_approval_resolved  { ..., approved: true }
```

The requested event carries a correlating `toolCallId`, giving the blocked exit edge pi lacks
entirely. Without forced approval mode the events did not fire (the same command auto-ran).

**Context object differences.** Event-handler `ctx` exposes `{ ui }` only — no abort `signal`
(pi's channel uses `ctx.signal?.addEventListener` optionally, so the fork loses nothing).
`ctx.ui.notify` is present.

**Unresolved by this capture:** the update-check banner appeared in every interactive boot;
whether `PI_OFFLINE`/`PI_SKIP_VERSION_CHECK` suppress it was not established in print mode
(the banner never renders there) — DQ-OMP-5.

## Conclusion

The pi-driver mechanism ports to omp with one structural divergence: the idle edge must be
derived by polling `ctx.isIdle()` after `agent_end` instead of listening for `agent_settled`.
A native omp driver is viable at full parity — presence lease, observed state including the
blocked-on-human axis pi cannot express, and native delivery. The measured divergences
justify forking the channel asset rather than parameterizing pi's.

## VRS Impact

Grounds OMP-R02 (observed axes, from the approval-event payloads and idle sampling),
OMP-R03 (idle edge rule), OMP-R04 (fork rationale), OMP-T01/T02 (deny path and ask axis
unmeasured), and DQ-OMP-3..5 (steer visual, modal interaction, banner suppression).

## Driver e2e run

The same day, the implemented driver ran end-to-end on dev3: a scratch catalog declared
`agent "omp-smoke"` with a `driver omp` block; `st2 hooks install` published the set including
`omp-channel.ts`; `st2 driver omp-session` launched the TUI under the wrapper. Verified: the
presence record read `available`; the harness-state record seeded `idle` under
`harness: "omp"`, fenced by the wrapper's session token (`seq 1`, pty session bound); a
`st2 message send` landed in the live TUI via the channel together with the
`st2-session-start` restored-context block; observed state cycled active → idle
(transitions 3→5) around the delivery. The model's reply itself failed with a provider-side
429 weekly usage limit — outside st2's surface.
