# Harness context specification

This document specifies the harness-context record, its guard, its per-harness
producers, and its roster exposure. It builds on
[requirements.md](./requirements.md). The categorical observation axis remains
in [`05-harness-state/spec.md`](../05-harness-state/spec.md) and reads none of
this; declared presence remains in the root spec's R08 section.

## Status

Partially implemented. The core is in the tree as of 2026-08-29: the record and
its schema, the quantized write guard, the reading projection, the roster's
fourth axis, the session-boundary removal, the Doctor advisory, and the pinned
replicated-record names — `src/harness_context.rs`, with the invariant rows
*Harness context discipline* and *Replicated-path discipline* naming their
proofs.

**No producer ships yet.** Every row of the producer table below, the
per-harness fixtures (HC-R13), and the status-line tee (HC-R18) remain
unimplemented, so in the shipped tree the record is written by nothing and every
declaration's `context` reads `null`. That is the honest state, not a defect of
the envelope.

The placement, guard, and Claude producer were first built as a throwaway spike
([`.experiments/2026-08-29-context-signals-and-write-placement.md`](./.experiments/2026-08-29-context-signals-and-write-placement.md)),
and the spike's tree is not the shipping tree. The write policy is no longer
provisional: it was chosen on a replay benchmark over 90 real sessions
([`.experiments/2026-08-29-write-policy-benchmark.md`](./.experiments/2026-08-29-write-policy-benchmark.md)),
which also moved the record's path. The residuals that remain: the Claude
producer's status-line contract with dotfiles is unsettled (`DQ-C2`), and the
transport half of `DQ-C1` is unmeasurable until a transport runs. Open questions
are tracked in [open-questions.md](./open-questions.md); the direction this
design deliberately does not take yet is in [roadmap.md](./roadmap.md).

## Scope

This specification defines the harness-context record, its freshness and
staleness projection, its write guard, the five per-harness producers and their
version-coupled arithmetic, compaction accounting, session-boundary behavior,
and the roster exposure. It does not define: the categorical observation axis or
its fencing ([`05-harness-state`](../05-harness-state/spec.md)); per-compaction
history or a context event stream (deferred, HC-T02, `DQ-C6`); idle or
idle thresholds, escalation, or
notification policy beyond Doctor's advisory line (root `R20`); the fleet
metrics feed (`06-observability`, roadmap); and any
supervisor action taken on a number (HC-A02, `DQ-C8`).

## Overview

```text
 claude statusLine     codex app-server        pi extension      omp extension    opencode
 stdin JSON            thread/tokenUsage/      getContextUsage() getContextUsage() message.updated
 (+ PreCompact/        updated                 + session_compact + session_compact + /config/providers
  PostCompact hooks)   + contextCompaction                                        + session.compacted
        |                     |                      |                 |                |
        v                     v                      v                 v                v
   [driver-owned, harness-native arithmetic: usedTokens / windowTokens / usedPercent]
        |
        v  write when: bucket changed | compaction | record older than heartbeat
        |
   <agent-dir>/harness-context          <agent-dir>/harness-state      <agent-dir>/status
   st2.harness-context.v1               st2.harness-state.v1           presence, agent-authored
   numeric, quantized,                  categorical, transition-       declared
   unfenced, 60-min horizon             guarded, fenced
        |                                        |                              |
        +----------------+-----------------------+------------------------------+
                         v
          st2 agents --json  (status | observedState | driverDiagnostic | context)
```

The three records are independent axes on one agent directory. The context
record's numbers stay readable when the state record derives `unknown`
(HC-R07), and the state record's fencing does not extend to them (HC-T04).

## Schema identifiers

`st2.harness-context.v1` follows the repository-local convention the presence,
harness-state, and driver-diagnostic records already use: `st2.<record-kind>.vN`,
lowercase, dot-separated, where `<record-kind>` is the record's file name. The
namespace is st2's own and is not registered anywhere; st2 core is its steward.
The version suffix is the read contract, not the field set: additive fields do
not bump it, and a reader that finds any other schema string refuses the record
rather than guessing at fields spelled like this version's. Examples: valid
`st2.harness-context.v1`; a future breaking shape `st2.harness-context.v2`;
foreign and therefore refused `com.example.harness-context.v1`; invalid (no
version) `st2.harness-context`.

## Record (HC-R01, HC-R04)

One JSON object at `<agent-dir>/harness-context`, beside `harness-state`,
written under its own lock `.harness-context.lock`, newline-terminated, and
replaced atomically by a rename from a staging file held **outside** the
replicated subtree (see
[Placement and transport](#placement-and-transport-hc-r05)):

```json
{
  "schema": "st2.harness-context.v1",
  "agent": "<identity>",
  "harness": "claude | codex | pi | omp | opencode",
  "usedTokens": 92283,
  "windowTokens": 258400,
  "usedPercent": 33,
  "model": null,
  "costUsd": null,
  "sessionTotalTokens": 2235329,
  "rateLimits": { "fiveHour": 31, "sevenDay": 55 },
  "compactions": 3,
  "lastCompactionMs": 1788000097290,
  "lastCompactionTrigger": "manual | auto | threshold | overflow | idle | unknown",
  "incarnation": "<the writing session's token>",
  "observedAtMs": 1788000100000,
  "writtenAtMs": 1788000100000
}
```

Field rules:

- `harness` is the discriminator for the arithmetic. There is deliberately no
  second `semantics` field: one discriminator for one semantic axis, and a
  reader that knows the harness knows which producer row of the table below
  produced the number. A `harness` this reader does not recognize makes the
  numbers uninterpretable, so the record reads as absent.
- `usedTokens`, `windowTokens`, and `usedPercent` are independently optional and
  each is `null` when withheld (HC-R02, HC-R03). `usedPercent` present with
  `windowTokens` absent is legal — Claude publishes its own integer percent
  alongside a window it may not yet have populated — but `usedPercent` is never
  computed by st2 from a window st2 guessed.
- `usedPercent` may exceed 100, and readers must handle that rather than assume
  a 0..100 range. pi and omp report a float that runs above 100 when a turn
  overruns the window (585.6% measured in the pi lab against a 4,000-token
  window). Claude clamps to 0..100 itself, and Codex's displayed percentage is
  clamped by construction — but Codex's *operands* are not: 2 of 287,010
  replayed Codex readings give `used / window` up to 1.041, so a producer
  computing from them can legitimately produce 104. **The record carries the raw
  value and never clamps it**: a reading above 100 is a real observation of an
  overrun, and clamping at the producer would hide exactly the saturation this
  record exists to show. Clamping is a *display* concern — a consumer rendering
  a bar or a percentage clamps for its own layout, and does so knowing it is
  discarding information the record deliberately kept.
- `sessionTotalTokens` is cumulative lifetime spend for the session and is
  **never** occupancy. It is named for that distinction (HC-R16): a measured
  Codex session read 2,235,329 cumulative against a 258,400-token window, so a
  consumer dividing it by `windowTokens` reports >800%.
- `costUsd` is the harness-reported session cost, in the harness's own
  accounting, and no st2 path recomputes or reconciles it. Codex reports none;
  the field is `null` there.
- `rateLimits` is harness-reported and account-scoped (HC-T06). It repeats
  across every agent runtime sharing an account. Absent windows are `null`.
- `lastCompactionTrigger` is a closed union, additive-tolerant on read: an
  unrecognized future word decodes as `unknown`, never as a definite trigger.
  `unknown` is a legitimate v1 value for three of the five harnesses.
- `incarnation` is the writing session's token, carried as provenance only
  (HC-R15). Nothing refuses a write on it, no sequence accompanies it, and there
  is no floor sidecar. A reader may use it to tell "this number came from the
  session currently running" from "this number predates it"; that is its whole
  purpose.
- `observedAtMs` is when the reading was taken, not when the file was written,
  and is never re-stamped without a new reading behind it. A producer holding no
  fresh reading does not write at all, so the record ages rather than looking
  refreshed — see the heartbeat rule in the write policy.
- `writtenAtMs` is when the record was written, and is strictly monotonic per
  record. **Added during implementation** (2026-08-29), additively and therefore
  without a version bump. The two stamps answer different questions and
  collapsing them is wrong in a way that shows up immediately: the write
  policy's heartbeat clause asks how old the *record* is, while `ageMs` asks how
  old the *reading* is, so a producer that writes at T carrying a reading taken
  at T−4min would look four minutes stale against a five-minute heartbeat and
  re-publish almost at once. Monotonicity is also what keeps HC-R04's
  byte-distinctness intact against a same-millisecond predecessor. Readers
  derive nothing from it: `ageMs` and `stale` come from `observedAtMs` alone.
- `usedPercent` and the other fractional fields are JSON numbers, so an integral
  reading may serialize as `33.0` rather than `33`. That is the cost of carrying
  pi's and omp's floats (585.6 was measured) in one field, and it is
  numerically identical to a consumer.
- Deserialization is additive-tolerant (no strict unknown-field rejection).

Constants, deliberately not aliases of the presence or harness-state constants
(HC-R04):

| Constant | Value | Meaning |
| --- | --- | --- |
| `HARNESS_CONTEXT_STALE` | 60 min | past this age a reading is returned marked `stale` |
| `HARNESS_CONTEXT_FUTURE_SKEW` | 60 s | beyond this the clock is untrusted and the record reads absent |
| `HARNESS_CONTEXT_BUCKET_PERCENT` | 1 | window fraction that defines a write bucket |
| `HARNESS_CONTEXT_WARN_PERCENT` | 80 | at or above this reading Doctor emits an advisory; st2's own attention threshold, not a compaction prediction |
| `HARNESS_CONTEXT_HEARTBEAT` | 300 s | maximum interval between writes while a reading is available; deliberately equal to `HARNESS_STATE_REFRESH`, so this record never re-stamps more often than the state record beside it |

The 60-minute horizon is four times harness-state's 15-minute one on purpose: a
categorical state that is an hour old is a dangerous claim about what an agent
is doing right now, while a token count that is an hour old is a still-useful
lower bound on how full a window has become.

## Reading (HC-R06, HC-R07)

What a reader projects, in evaluation order:

| Evidence | Projects as | Reason |
| --- | --- | --- |
| No record file | `null` | never observed |
| Unreadable, unparseable, or non-v1 `schema` | `null`, warning logged | nothing trustworthy to report; `DQ-C5` asks whether this deserves an explicit `indeterminate` the way `driverDiagnostic` has one |
| Unrecognized `harness` | `null`, warning logged | the arithmetic is unknown, so the numbers have no meaning |
| `observedAtMs` > now + `HARNESS_CONTEXT_FUTURE_SKEW` | `null` | a clock this wrong makes the derived age meaningless |
| `observedAtMs` ≤ now − `HARNESS_CONTEXT_STALE` | the reading, `stale: true`, with `ageMs` | HC-R06: returned with its age, never derived away |
| Otherwise | the reading, `stale: false`, with `ageMs` | — |

`ageMs` is derived by the reader from `observedAtMs`; no read path consults file
mtime. There is no `unknown` vocabulary on this axis and no path from any
absence to a fabricated number.

The projection is computed independently of the harness-state record. It is not
consulted by, and does not consult, the state derivation: an agent whose
`observedState` reads `unknown` for any of that record's nine reasons still
reports its last context reading with the age it has (HC-R07).

## Write policy (HC-R09, HC-R10)

```text
reading arrives
   |
   +-- compaction edge?                                  -> write
   +-- floor(used / (BUCKET_PERCENT% of window)) changed
   |     since the last written reading?                  -> write
   +-- record older than HARNESS_CONTEXT_HEARTBEAT?       -> write
   +-- otherwise                                          -> skip
```

Fixed quantization at 1% of the window — 100 buckets — plus a compaction edge
and a 300-second heartbeat. The heartbeat fires only when the producer holds a
reading taken since the last write: it re-publishes a *fresh* reading whose
bucket happens not to have changed, and never re-stamps a stale one. A producer
with no new reading writes nothing at all, and the record ages visibly through
`ageMs` instead. This is what keeps HC-R04's byte-distinctness rule intact — a
write that changed nothing would carry no refresh across a transport that
compares content — and it is why the constant is described as the maximum
interval between writes *while a reading is available*. The policy was chosen on a replay benchmark over 45
Codex rollouts (median span 30.6 h, 4,220 readings each, real in-protocol
windows) and 45 Claude transcripts
([`.experiments/2026-08-29-write-policy-benchmark.md`](./.experiments/2026-08-29-write-policy-benchmark.md)).

### Why not a movement guard with a time floor

The spike's candidate — skip identical, write on a ≥5% move, otherwise at most
once per 60 s — is dominated, and the reason is structural rather than a matter
of tuning its constants:

| Policy | Codex writes/h/agent | error p95 (% of window) | missed warnings @90% |
| --- | --- | --- | --- |
| every distinct reading | 100.7 | 0.00 | 0% |
| turn boundary only | 1.8 | 48.76 | 92.1% |
| movement guard 5% / 60 s (the spike) | 19.2 | 3.43 | 23.1% |
| movement guard 1% / 60 s | 32.1 | 0.82 | 2.8% |
| bucket 5% + 300 s heartbeat | 13.1 | 3.80 | 0% |
| **bucket 1% + 300 s heartbeat** | **34.1** | **0.76** | **0%** |

"Missed warnings" is the share of real compaction events where occupancy truly
crossed the threshold but the last written reading did not — the "warn me before
this agent compacts" metric this record exists for, measured over 821 eligible
Codex events at the 90% threshold.

Three results from that benchmark decide the shape:

1. **A time floor makes cost track producer chattiness, not the agent.** On
   Claude, the movement guard at 1%, 5%, and 10% with a 60-second floor all land
   at 59 writes per active hour — 3600/60. The movement clause is nearly inert
   and the policy degenerates to "one write per minute while active". Because
   Codex emits roughly 25× more readings per hour than Claude, any time-floored
   policy needs per-harness tuning. Quantization does not: writes per window fill
   are capped at `100 / BUCKET_PERCENT` regardless of how often a producer
   speaks, which is precisely why **one constant is correct for all five
   harnesses and there is no per-harness knob** (HC-R09).
2. **Cost does not discriminate.** `DQ-H2`'s recorded failure was 679 writes in
   221 s — 3.07 writes/s on one agent, 11,061 writes/hour/agent. Every candidate
   above is 100–1,000× below that, including "write every distinct reading" at
   1/110 of the failure rate. The recommended policy is 1/324 of it: 34.1
   writes/h/agent × 600 agents = 5.7 writes/s fleet-wide. Cost is not binding
   anywhere in the candidate set, so the choice is made on accuracy and
   structure.
3. **A coarse bucket couples the write policy to the reader's alarm** (HC-R10).
   A bucket write fires on *entry* to a bucket, so the reader always shares the
   truth's bucket — an alarm sitting on a bucket boundary can never be missed,
   one between boundaries can. Bucket 5% is perfect at 90% and 95% and *worse
   than the movement guard* in between: 51%, 70%, 74%, and 79% of warnings
   missed at thresholds of 91, 92, 93, and 94% (n = 672, 590, 436, 289). Bucket
   1% is the only measured policy with zero missed warnings at every threshold
   from 80% to 97%, which is what makes the write policy independent of whatever
   a future consumer decides to alarm on.

Turn-boundary writing is disqualified outright: 48.8% p95 error and 92% of
warnings missed, because the wedge scenario is a single long turn.

### What would change this

- **If the reader stopped surfacing `ageMs` and `stale`**, quantization's
  unbounded lag would become silent instead of visible, and the time floor would
  do real work again; the fallback is the movement guard at 1% / 60 s (32.1
  writes/h/agent, 1–22% missed). HC-T05 states this dependency.
- **If per-sync wire cost turns out to be large** — unmeasured, and the open
  half of `DQ-C1` — the next stop is a 2% bucket at 23.3 writes/h/agent, with
  the consequence that alarms must then sit on even percentages.
- **Occupancy above the window caps the guarantee.** `100 / BUCKET_PERCENT` is a
  write cap only up to the first overflow; above 100% the bucket index keeps
  climbing.

## Placement and transport (HC-R05)

The record lives at `<agent-dir>/harness-context`, beside `harness-state`. The
driver records belong together at the agent-directory root; what makes them
replicate is the transport's include list naming them, not their location.

The measurement that raised the question stands: the fleet's replication
transport syncs an agent directory through an include list that named only
`**/resources/**` and `**/status`, so a driver record at the agent-directory
root matched nothing and would silently never reach a remote reader — no error,
just a record no remote consumer ever sees. That defeats the reason a catalog
record was chosen over host-local state
([decision 0006](../.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md):
"remote supervisors and the TUI read through the catalog they already sync").

The resolution is to name the driver records in that list —
`**/harness-state,**/harness-context` — rather than to move them under a
directory whose meaning is the realization surface for Resource bindings. The
include list is a fleet-side trait specification —
`context/fleet/traits/fabric/spec.md` in the dotfiles repository — so this is a
cross-repository change, and it has one property worth stating: it fixes the **already-shipped**
`harness-state` record, which has the same defect today, without migrating a
live record.

Two obligations follow on st2's side:

- **Pin the names.** st2 asserts, in a test, the exact record names it expects
  the transport to carry. The include list lives in another repository, so a
  rename here would otherwise stop replication silently and nothing in this
  repository would notice (HC-T08). The test does not prove the other side
  carries them; it makes st2's half of the contract explicit and breakable.
- **Stage outside the replicated subtree.** A sibling temporary name inside it
  is propagated as a real key and restored after a later local delete, so the
  atomic-write helper stages on the same filesystem but outside the sync root
  and renames only the canonical path in. Measured free: p50 0.217 ms staged
  outside versus 0.222 ms as a sibling.

  **Where, decided during implementation (2026-08-29): the agent directory's
  parent** — `<catalog>/agents/<host>/` for the layout st2 publishes. The
  obvious alternative, walking up for the catalog's control directory and
  staging in `<catalog>/.st2/staging`, reads better and is wrong: the search has
  no way to tell *this* catalog's control directory from any unrelated one above
  the agent, and it was caught doing exactly that in a test — an agent directory
  under `/tmp` on a host carrying a stray `/tmp/.st2` staged into a foreign
  tree, and potentially a foreign filesystem, which costs the rename its
  atomicity. The parent needs no discovery, cannot escape, holds nothing the
  transport replicates (only agent directories, and a dotted temporary name
  matches no include entry), and is correct for a flat catalog as well as a
  published one. An agent directory with no parent is an error rather than a
  quiet write inside the subtree.

  `harness-state` keeps staging beside itself for now. The two records share the
  extracted `write_json_atomic` helper, which takes the staging directory as an
  argument precisely because they answer this question differently; moving the
  shipped record's staging is a separate change with its own risk.

The delivery watcher is unaffected (HC-R08): its allowlist is
`starts_with(<agent-dir>/resources/inbox)` or the exact path
`<agent-dir>/status`, and the record is a sibling of neither, so no pump wakes
and no production change is needed.

No correctness property here depends on the transport. Its adoption record is
open and forbids exactly that, so replication is what is gained when it arrives,
never what this subsystem needs to work.

## Producers (HC-R02, HC-R11, HC-R13)

Every row was measured on 2026-08-29 against the named version. `usedPercent`
is that harness's own displayed number (HC-R02); where st2 computes it, the row
says so.

| Harness | Version verified | Channel | `usedTokens` | `windowTokens` | `usedPercent` |
| --- | --- | --- | --- | --- | --- |
| claude | 2.1.250 | `statusLine` command stdin JSON | `context_window.total_input_tokens` = `input + cache_creation + cache_read` of the last response | `context_window.context_window_size` | `context_window.used_percentage` — Claude's own integer, clamped 0..100 |
| codex | codex-cli 0.150.1 | app-server `thread/tokenUsage/updated` | `tokenUsage.last.totalTokens` | `tokenUsage.modelContextWindow` | st2 computes with the baseline rule below; equals `100 −` Codex's displayed "% context left" |
| pi | 0.84.2 | injected extension `ctx.getContextUsage()` | `.tokens` = last assistant `totalTokens` (input + output + cacheRead + cacheWrite) | `.contextWindow` | `.percent` (float) |
| omp | 18.0.9 (and 18.0.3) | injected extension `ctx.getContextUsage()` | `.tokens` = last assistant **`input`** only | `.contextWindow` | `.percent` (float) |
| opencode | 1.18.25 | SSE `message.updated` joined with `GET /config/providers` | last **non-summary** assistant `tokens.total` | `providers[].models[<modelID>].limit.context` | st2 computes `usedTokens / windowTokens`; the server displays none |

Which adjacent facts each channel actually supplies (HC-R16) — everything not
listed is `null`, and no producer computes a fact its channel does not carry:

| Harness | `model` | `costUsd` | `rateLimits` | `sessionTotalTokens` |
| --- | --- | --- | --- | --- |
| claude | `model.id` | `cost.total_cost_usd` | `rate_limits.{five_hour,seven_day}` | `null` — the payload's `total_*` keys describe the last response, not the session |
| codex | `null` — the thread carries `modelProvider` only | `null` — Codex reports no cost | `account/rateLimits/updated` | `tokenUsage.total.totalTokens` |
| pi | `ctx.model.id` | per-message `usage.cost.total` | `null` | `null` in v1 — only obtainable by summing every message's usage, which is a producer-side accumulator, not a free reading |
| omp | `ctx.model.id` | per-message `usage.cost.total` | `null` | `null` in v1 — same reason as pi |
| opencode | `session.info.model` | `session.info.cost` | `null` | `session.info.tokens` (cumulative, no `total` key — summed by the producer) |

`sessionTotalTokens` is carried only where the channel already computes it. A
producer that would have to accumulate it itself writes `null` rather than
maintaining a second running total whose correctness depends on having seen
every message — tokenlens owns lifetime accounting, and a half-observed sum
would be a worse answer than none.

### claude (2.1.250)

The status-line payload is the only channel that carries a window. Hook payloads
carry no token fields at all, and the transcript carries per-message `usage` but
no window size — so a transcript-only producer would have to invent the
denominator from a model table, which cannot distinguish a 200k from a 1M tier
for one model id, and would violate HC-R02.

st2 owns the `statusLine` slot for driver-declared agents: the rendered
`.claude/settings.local.json` names a tee that records the stdin JSON to the
harness-context record and then execs the operator's own downstream renderer, so
a human's status line keeps working:

```text
.claude/settings.local.json (st2-rendered)
  statusLine: { type: "command", command: "<st2 status-line tee>", refreshInterval: 5 }

tee:  stdin JSON --> st2 driver claude-observe (writes harness-context)
                 --> exec ${ST_CLAUDE_STATUSLINE_RENDERER}              (DQ-C2)
                 --> if no renderer resolves: pass stdin through unchanged
```

**Chaining is mandatory, not a courtesy** (HC-R18). When no downstream renderer
resolves, the tee passes its stdin through unchanged rather than discarding it —
the fallback is transparency, not silence, so the worst case of st2 occupying
the slot is the operator's own payload rendered verbatim rather than an empty
status line. Precedence was captured live
on 2026-08-29 against Claude Code 2.1.250 in four cases through a real pty
([evidence](./.experiments/2026-08-29-context-signals-and-write-placement.md)):
`.claude/settings.local.json` > `.claude/settings.json` >
`~/.claude/settings.json`, and the winning `statusLine` **replaces** the losing
object outright — a single slot, one command per render, no merge. Since
`.claude/settings.local.json` is precisely the file st2 renders for
driver-declared agents, an st2 entry that does not exec the operator's renderer
silently and unconditionally removes their status line on every managed agent,
with no warning. On a host whose global renderer already displays agent id,
model, context fill, and cost, that is a visible regression st2 would have
caused without saying so.

The inverse is not fixed by chaining and is worth stating: a `statusLine` a
human sets in a managed agent's `.claude/settings.local.json` is not preserved
by st2's materialization of that file, whose merge owns only st2's own hook
entries. A human renderer therefore belongs in the operator's own settings, with
st2's tee chaining to it — not in the file st2 rewrites.

What names the downstream renderer — an environment variable or a settings key —
is still unsettled and shared with the dotfiles change that owns the slot today:
`DQ-C2`.

Withholding: `current_usage`, `used_percentage`, and `remaining_percentage` are
`null` until the session's first API response, while `context_window_size` is
populated from the start. The producer writes the window and withholds
`usedTokens` and `usedPercent` (HC-R03). `total_input_tokens` must not be
mistaken for a measurement in that state: the payload builder emits `0` for it
precisely when `current_usage` is null, so the zero is derived from the absence
rather than observed. `current_usage` being null is Claude declaring it does not
yet know.

Adjacent facts on the same payload: `model.id`, `cost.total_cost_usd`, and
`rate_limits.{five_hour,seven_day}.used_percentage`. The payload describes the
top-level session; a subagent's own window is not in it, so a runtime whose
subagent is saturating reports the parent's fill — the grain question is
`DQ-C9`.

Compaction comes from hooks regardless of the status line: `PreCompact` and
`PostCompact` carry `trigger ∈ manual | auto`, and `SessionStart` carries
`source: "compact"` as a third post-compaction edge. `PreCompact` must be routed
through the observe hook **in addition to** the existing pre-compact stub script
that writes a working-state reconstruction — the two do different jobs and the
registration carries both.

### codex (codex-cli 0.150.1)

```text
window = tokenUsage.modelContextWindow        // absent        -> withhold percent
used   = max(0, tokenUsage.last.totalTokens - 12000)
eff    = window - 12000                        // window <= 12000 -> withhold percent
usedPercent = clamp(round(used / eff * 100), 0, 100)
```

The 12,000-token `BASELINE_TOKENS` constant is subtracted from **both** the
numerator and the denominator; it is hardcoded in two Codex crates with no
configuration override, and it is exactly the kind of version-coupled constant
HC-T03 names. Recomputed against the measured capture (window 258,400,
`last.totalTokens` 92,283): 33% used, matching Codex's displayed "67% context
left"; a naive `last.inputTokens / window` gives ~36% — close enough to look
right and wrong by construction.

Three traps, all measured:

- `tokenUsage.total` is **cumulative session spend**, not occupancy. It goes to
  `sessionTotalTokens` and nowhere near a percent.
- The numerator is `last.totalTokens`, not `last.inputTokens`.
- The window appears in exactly one place in the whole app-server protocol —
  this notification. A resumed or re-attached thread is not blind: the
  app-server replays `thread/tokenUsage/updated` to the newly attached
  connection before any new turn.

`account/rateLimits/updated` is a separate account-scoped notification and
supplies `rateLimits`. Codex reports no session cost, so `costUsd` is `null`,
and the app-server `Thread` object carries `modelProvider` but no model
identifier, so `model` is `null` too — both examples above are Codex readings
and show those nulls deliberately.

Compaction: the `contextCompaction` thread item is the live edge (the older
`thread/compacted` notification is deprecated in the protocol). It carries no
sizes and no reason, so the trigger is `unknown`.

### pi (0.84.2)

One call answers everything, present on the `ctx` of every lifecycle event:
`getContextUsage()` returns `{ tokens, contextWindow, percent }`.

The honest-unknown behavior is the load-bearing one: immediately after a
compaction, and across a process restart until the next assistant usage arrives,
`tokens` and `percent` are both `null` while `contextWindow` stays populated. pi
positively says it does not know, and the producer withholds rather than
substituting (HC-R03) — the same discipline
[`OHS-R02`](../05-harness-state/requirements.md) applies to the categorical
axis.

`session_compact` carries `reason ∈ manual | threshold | overflow` and
`tokensBefore`. The count is harness-durable: `ctx.sessionManager.getEntries()`
filtered to `type === "compaction"` survives restarts (measured 2 → 3 across a
compaction and read back correctly on the next process's `session_start`).
`ctx.model.id` supplies `model`; the per-message `usage.cost.total` supplies
`costUsd`.

### omp (18.0.9, also verified on 18.0.3)

The same call, the same shape — and a different meaning. omp's `tokens` settles
to the last assistant message's **`input`** alone, where pi's settles to
`totalTokens`; measured in a controlled lab where the fake provider reported
prompt tokens of 900, 9,900, and 22,500 and `getContextUsage().tokens` returned
exactly those figures. So omp under-reports relative to pi by output plus cache
on an otherwise identical API. This is HC-T03's second version-coupled constant
and the reason the two harnesses get separate producer rows rather than a shared
one.

`session_compact` carries **no** `reason` and no `willRetry` (pi's does), so the
trigger is `unknown` even though omp internally calls its auto-compaction with
`"idle"` and `"threshold"` — those words are not projected onto the event. The
count is harness-durable through the same `sessionManager.getEntries()` path as
pi's.

Correction of record: the 18.0.3 capture
([`2026-08-25-omp-harness-integration.md`](../06-omp-driver/.experiments/2026-08-25-omp-harness-integration.md))
states that the handler `ctx` exposes `{ui}` only. The 2026-08-29 probe run
against both binaries shows `getContextUsage` present and working on 18.0.3 and
18.0.9 alike (35 occurrences in each binary) — a probe artifact in the older
capture, not a version change. `signal` and `agent_settled` genuinely are absent
from both, as that capture says.

### opencode (1.18.25)

The only producer that needs two sources: the numerator is pushed on the SSE
stream and the denominator is pulled from the config surface. Both traps here
are measured and both silently produce a wrong number:

- **`summary` is overloaded.** On user messages it is an object (`{diffs: []}`)
  and therefore truthy; the filter for a compaction summary must be
  `role === "assistant" && summary === true`.
- **The summarizer's own message poisons the last-assistant reading.** After a
  compaction the newest assistant message is the summary, whose `tokens.total`
  is the cost of the summarization call (1,511 measured), not the new context
  size. The producer skips summary messages when picking the last reading.
- **`session.updated`'s `info.tokens` is cumulative** and carries no `total`
  key. It grows without bound and is `sessionTotalTokens`, never occupancy.

`tokens.total` appears only on the final `message.updated` for a message; the
first carries zeros and no `total`. Which session's reading this is when one
server holds several — the categorical producer aggregates across them, but two
live sessions have two windows and one record — is unsettled: `DQ-C10`. `session.compacted` carries `{sessionID}`
and nothing else — no reason, no timestamp, no sizes — so the trigger is
`unknown` and the count is derived by counting assistant messages with
`summary === true`. The richer v2 events (`session.next.compaction.started` /
`ended`, which do carry a reason) exist in the OpenAPI document but did not fire
once on the legacy path in the measured runs; they are schema-only and no
producer depends on them.

## Compaction accounting (HC-R12)

| Harness | Edge | Trigger carried | Trigger words yielded | Count source | Counter scope |
| --- | --- | --- | --- | --- | --- |
| claude | `PreCompact` / `PostCompact` hooks; `SessionStart source=compact` | `trigger` | `manual`, `auto` | st2 counts (one hook process per event) | incarnation |
| codex | `contextCompaction` thread item | none | `unknown` | st2 counts | incarnation |
| pi | `session_compact` | `reason` | `manual`, `threshold`, `overflow` | `sessionManager.getEntries()` type `compaction` | harness-durable |
| omp | `session_compact` | none | `unknown` | `sessionManager.getEntries()` type `compaction` | harness-durable |
| opencode | `session.compacted` | none | `unknown` | assistant messages with `summary === true` | incarnation |

The vocabulary `manual | auto | threshold | overflow | idle | unknown` is closed
and additive-tolerant. `idle` is in v1 because omp's internal auto-compaction
names it; **no v1 producer emits it**, since omp does not project its reason
onto the event. A word arriving that a reader does not recognize decodes as
`unknown`.

"Incarnation" scope means the counter starts at zero when the session record is
claimed (HC-R15); "harness-durable" means the harness's own session store
answers the question and the count spans restarts. A consumer comparing counts
across harnesses is comparing two different questions, which is why the scope is
stated per row rather than averaged into one number.

Per-compaction detail — sizes, the reason of every earlier compaction, dwell
between them — is not recorded (HC-T02, `DQ-C6`).

## Session boundary and provenance (HC-R15)

```text
relaunch: wrapper claims harness-state (existing written claim)
            \-- and removes harness-context
first reading of the new session -> new record, incarnation = the claiming token
straggler write from the old session -> lands; visibly older incarnation, aged by
                                        observedAtMs; the next real reading overwrites it
```

The claim path gains one file removal and nothing else. A reader after a
relaunch sees `null` ("no context yet") rather than the previous incarnation's
190k, which is the honest answer for a window that has just been emptied. The
alternative — leaving the old numbers to age out over the 60-minute horizon —
would show a crash-looping agent the previous incarnation's fill as if it were
current.

There is deliberately no sequence, no written claim on this record, and no floor
sidecar (HC-T04).

## Exposure (HC-R14)

`st2 agents --json` (both forms) appends one field per row, always present:

```json
"context": {
  "harness": "codex",
  "usedTokens": 92283,
  "windowTokens": 258400,
  "usedPercent": 33,
  "model": null,
  "costUsd": null,
  "sessionTotalTokens": 2235329,
  "rateLimits": { "fiveHour": 31, "sevenDay": 55 },
  "compactions": 3,
  "lastCompactionMs": 1788000097290,
  "lastCompactionTrigger": "unknown",
  "observedAt": 1788000100000,
  "ageMs": 4210,
  "stale": false
}
```

`null` when no record exists — the roster's existing convention for
`observedState`, kept rather than omitting the key, so the wire has one
convention and not two. The reading projection above is already applied: a
consumer never re-implements staleness, and never sees a record it would have to
age itself. `incarnation` and `writtenAtMs` are deliberately not exposed: the
first is provenance for a reader inspecting the record itself, and the second is
a write-policy mechanism a roster consumer has no use for once `ageMs` and
`stale` are computed.

Human `st2 agents` output carries the same axis compactly as `ctx:92% ⟳1`,
beside the existing `obs:` column and prefixed for the same reason — two bare
words in one row is exactly the ambiguity the ontology's collision rules name.
`ctx:-` is no record; a withheld percent renders as `ctx:-` with its compaction
count beside it; a stale reading is suffixed `stale`. The percent is rounded for
width and never clamped.

`status`, `desiredState`, `lastActivity`, `observedState`, and
`driverDiagnostic` keep their exact meanings. `context` is a fourth independent
axis: none of the five is derived from another.

## Doctor (HC-R17)

Doctor emits an advisory line for an agent it owns in two cases:

| Condition | Advisory | Exit status |
| --- | --- | --- |
| `usedPercent` ≥ `HARNESS_CONTEXT_WARN_PERCENT` | context fill, with the reading's age | unchanged |
| record `stale` while desired state is `running` | the numbers are older than the horizon | unchanged |
| no record, unreadable record, or a fresh reading below the threshold | nothing | unchanged |

Neither case is ever a failure. This matches how Doctor already treats the
categorical axis ([`OHS-R10`](../05-harness-state/requirements.md)): a stale or
session-dead record beside a `running` desired state is worth a warning and
never an exit-code failure, and the numeric axis adds no new authority
(HC-A02).

**80 is st2's number, not the harness's.** It is a threshold for "worth a
human's attention", chosen so an operator sees a filling window with room left
to act. It is deliberately **not** an estimate of where the harness will
compact: that point depends on the harness, the model, and the operator's own
settings — Codex reserves a 12,000-token baseline and a separate auto-compact
buffer, pi compacts when occupancy passes `contextWindow − reserveTokens` with
a 16,384-token default, omp exposes an idle-triggered path with its own
threshold, and Claude's auto-compact window is overridable by environment
variable. A single st2 constant claiming to predict that would be wrong in both
directions on most harnesses, and would silently change meaning under a harness
bump — the failure mode HC-T03 already names for the producer constants.

Because the threshold is st2's own, it carries no coupling to the write policy:
quantization at 1% is threshold-agnostic (HC-R10), so moving this constant needs
no change to what producers write, and a reader alarming at some other number is
equally well served.

## Verification plan

The invariant rows this subsystem must add or amend when implementation lands,
each only once a real test proves it (per `CLAUDE.md`):

- **Stable roster JSON** (existing row) — wording gains the fourth axis and its
  null convention. `src/agents.rs::agents_json_has_stable_wire_shape` is edited
  in the same change and stays the proof; the pinned full-string assertions in
  `src/agents.rs` change deliberately alongside it.
- **Scoped delivery-input wakeups** (existing row) — the row's prose enumerates
  the runtime records that must never wake a delivery pump by name, and gains
  `harness-context`. The delivery watcher is an allowlist
  (`starts_with(resources/inbox)` or the exact `status` path), so
  `src/watch.rs::delivery_watcher_ignores_runtime_records_but_wakes_on_inbox_and_status`
  stays the proof and stays green with no production change — the row's wording
  is what moves.
- **Harness context discipline** (new row, once proved) — stale readings
  returned with their age rather than derived away; numbers surviving every
  `observedState: unknown` derivation; withheld values never fabricated; a
  reading within the written bucket not written; a bucket crossing, a compaction
  edge, and a record older than the heartbeat each written; the record removed
  on the relaunch claim.
- **Doctor advisory, never failure** — a reading at or above the threshold and a
  stale record beside `running` each produce advisory output and leave the exit
  status untouched; a fresh reading below the threshold produces nothing. Test
  in the shape of the existing Doctor coverage in `tests/doctor.rs`.
- **Replicated-path discipline** (new row, once proved) — st2 pins the exact
  driver-record names it expects the transport's include list to carry, and no
  staging file is ever created inside the replicated subtree. Both are silent
  failures in production, so both need a test that asserts the names and paths
  themselves: one pinning `harness-state` and `harness-context` as the names
  st2 publishes for replication, one asserting that a write leaves no
  non-canonical file behind in the agent directory.
- **Status-line slot chaining** (HC-R18) — a rendered status-line registration
  invokes the operator's downstream renderer. The slot is single-valued and the
  winner replaces rather than merges, so a test that only checks st2's command
  is present would pass while the operator's renderer is silently gone.
- **Per-harness fixture tests** (HC-R13) — one per producer, each decoding a
  payload captured verbatim from the version named in the producer table and
  asserting the resulting triple, with the version asserted literally. The Codex
  fixture must fail if the 12,000 baseline moves; the omp fixture must fail if
  `tokens` stops meaning prompt-only input; the OpenCode fixture must include a
  post-compaction message list so a producer that stops skipping summary
  messages fails it.

## Open design questions

Tracked with context in [open-questions.md](./open-questions.md): `DQ-C1` write
policy benchmark, `DQ-C2` status-line renderer contract, `DQ-C5` unreadable
versus absent, `DQ-C6` history, `DQ-C7` fleet transport cost, `DQ-C8` supervisor
actionability, `DQ-C9` subagent context, `DQ-C10` OpenCode multi-session
aggregation.
