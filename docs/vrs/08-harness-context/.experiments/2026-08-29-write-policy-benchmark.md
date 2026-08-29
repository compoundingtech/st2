# Choosing the harness-context write policy on replayed sessions

## Question

The placement spike proposed a write guard — skip an identical reading, write on
a ≥5% window move, otherwise at most once per 60 s — with constants set by
counting writes in one synthetic Claude turn. Two things were unestablished:

1. **Is that policy right**, measured against real sessions rather than a
   synthetic turn, and judged on what the record is *for* (would a reader have
   seen a high-occupancy warning before the agent compacted) rather than only on
   write count?
2. **What does a write actually cost** at fleet scale, which is the half
   [`DQ-H2`](../../05-harness-state/open-questions.md) left open for the
   categorical record and this record inherits?

## Method

**Corpus.** 45 Codex rollouts (median span 30.6 h, 4,220 readings each, real
in-protocol windows of 258,400 / 272,000 / 353,400) and 45 Claude transcripts
(median 21.9 h, 630 readings). Codex occupancy is
`info.last_token_usage.total_tokens` — `info: null` frames are rate-limit
updates, not readings. Claude occupancy is `input + cache_creation + cache_read`,
deduplicated by `message.id` (the same usage is repeated on several JSONL lines
per API turn, so summing overcounts roughly 2×); `isSidechain` entries and
subagent transcripts are excluded because they have a different window. Duty
cycle — time inside a working stretch over wall span — is 27.0% for Codex and
7.8% for Claude, and writes/hour/agent are reported against that, not against
active hours.

Claude publishes no window in its transcript, so it is inferred as 200k or 1M
(the only two shipped) from peak occupancy; 39 of 45 infer 1M and reach a median
99% of it, which is what confirms the classification. Codex is the
better-grounded column throughout and is what the conclusions rest on.

**Replay.** Each candidate policy is run over each session's reading series,
recording every write it would have made. Three metrics: writes per hour per
agent; time-weighted error, the gap between the true occupancy and the last
written value as a percentage of the window; and **missed warnings** — the share
of real compaction events where occupancy truly crossed a threshold but the last
written value did not. The last is the metric that matches what the record is
for, and it is swept across thresholds from 80% to 97% rather than fixed at one.

**Local write cost.** 1,000 iterations of the spike's write path (exclusive
lock, write temporary, rename; no fsync) with a 304-byte record on the real
catalog filesystem, comparing a temporary staged as a sibling against one staged
outside the replicated subtree.

**Transport.** Inspection of the fleet's replication configuration and of what
is actually running on the host.

## Result

### The noise floor rules out "write every distinct reading" as a distinct policy

| \|Δ occupancy\| between consecutive readings, % of window | p50 | p90 | p99 | ≥1% |
| --- | --- | --- | --- | --- |
| Codex | 0.15 | 1.51 | 9.19 | 14.8% |
| Claude | 0.11 | 0.24 | 1.81 | 1.3% |

94.6% (Codex) and 98.4% (Claude) of consecutive readings differ at all, so
"write every distinct reading" is in practice "write every reading". Re-running
Codex on prompt-only occupancy gives p90 = 1.58% and an identical ranking, so
the ≥1% moves are real prompt growth rather than output jitter.

### Cost does not discriminate; accuracy and structure do

| Policy | Codex w/h/agent | Claude w/h/agent | error p95 (cx) | error max | missed @90% (cx) |
| --- | --- | --- | --- | --- | --- |
| every reading | 100.7 | 14.8 | 0.00 | 0.0 | 0.0% |
| turn boundary | 1.8 | 4.6 | 48.76 | 91.4 | 92.1% |
| movement guard 5% / 60 s (the spike) | 19.2 | 4.6 | 3.43 | 5.0 | 23.1% |
| movement guard 1% / 60 s | 32.1 | 4.7 | 0.82 | 1.0 | 2.8% |
| movement guard 5% / 300 s | 10.9 | 1.8 | 4.44 | 5.0 | 29.6% |
| time-only 60 s | 14.5 | 4.6 | 6.02 | 104.1 | 49.8% |
| bucket 5% + 300 s heartbeat | 13.1 | 2.2 | 3.80 | 5.0 | 0.0% |
| bucket 2% + 300 s heartbeat | 23.3 | 2.5 | 1.54 | 2.0 | 0.0% |
| **bucket 1% + 300 s heartbeat** | **34.1** | **3.2** | **0.76** | **1.0** | **0.0%** |

`DQ-H2`'s recorded failure was 679 writes in 221 s — 3.07 writes/s on a single
agent, or 11,061 writes/hour/agent. **Every policy in the table is 100–1,000×
below that**, the most expensive of them at 1/110 of the failure rate. The cost
axis therefore does not choose between them, which reframes the whole question:
the guard's constants cannot be justified as cost control.

### Three structural results

**1. A time floor makes cost track the harness, not the agent.** On Claude the
movement guard at N = 1%, 5%, and 10% with T = 60 s all land at 59 writes per
active hour — 3600/60. The movement clause is nearly inert; the policy is "one
write per minute while active". Its cost is therefore set by how often a
producer speaks, and Codex emits about 25× more readings per hour than Claude,
so any time-floored policy needs per-harness tuning.

**2. Quantization ties writes to information instead.** Bucket 5% costs 42.5
writes per active hour on Codex and 7.1 on Claude — a 6× spread that is exactly
how much faster Codex burns window per active hour, even though it emits 25×
more readings. Writes per window fill are capped at `100 / N` regardless of
producer chattiness, which is what makes one constant correct across all five
harnesses, and which makes the failure `DQ-H2` actually recorded — a producer
restating per event — structurally impossible rather than merely guarded
against.

**3. A coarse bucket couples the write policy to the reader's alarm.** A bucket
write fires on entry to a bucket, so the reader always shares the truth's
bucket: an alarm on a bucket boundary can never be missed, one between
boundaries can. Codex missed warnings by threshold:

| Policy | 80 | 85 | 88 | 90 | 91 | 92 | 93 | 94 | 95 | 96 | 97 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| *eligible events n* | *1239* | *1121* | *973* | *821* | *672* | *590* | *436* | *289* | *45* | *18* | *7* |
| bucket 1% + heartbeat | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| bucket 2% + heartbeat | 0 | 3 | 0 | 0 | 8 | 0 | 17 | 0 | 13 | 0 | 0 |
| bucket 5% + heartbeat | 0 | 0 | 11 | 0 | **51** | **70** | **74** | **79** | 0 | 0 | 0 |
| movement guard 5% / 60 s | 3 | 6 | 13 | 23 | 35 | 43 | 48 | 57 | 36 | 6 | 0 |
| movement guard 1% / 60 s | 1 | 1 | 2 | 3 | 4 | 8 | 7 | 22 | 7 | 0 | 0 |

Bucket 5% is excellent at multiples of five and worse than the movement guard at
91–94%. Only 1% is threshold-agnostic. The denominator shrinks sharply above
95% — few compactions happen up there — so the flat zeros in the last three
columns rest on 7–45 events and are not evidence that anything is fine at those
thresholds; the 91–94% band (n = 289–672) is where the mechanism shows.

Turn-boundary is disqualified outright: 48.8% p95 error, 91.4% maximum, 92.1% of
warnings missed — the wedge scenario is a single long turn, and this policy is
silent for its whole duration.

### Two placement findings that outrank any byte count

**Nothing replicates the catalog today.** The catalog is a local filesystem;
none of the usual replication daemons is running. The intended transport is
specified in the root spec, but its adoption record is open and states that no
st2 correctness property may depend on it.

That makes the following the operative results rather than a wire measurement:

- **The spike's path would not replicate at all.** The transport's agent-facing
  sync includes `**/resources/**` and `**/status` and nothing else under an
  agent directory. `<agent-dir>/harness-context` matches neither, and
  **the shipped `harness-state` record has the same defect**, which leaves
  decision 0006's own "readers read through the catalog they already sync" unmet
  as written. Resolved by naming both driver records in the include list rather
  than by moving them (q11): moving would have fixed only the new record, left
  the shipped one unreplicated, and put driver runtime state on the
  Resource-binding realization surface.
- **A staged sibling temporary is explicitly forbidden inside the replicated
  subtree**, because the transport propagates that name as a real key and
  restores it after a later local delete. Staging must happen outside the sync
  root, on the same filesystem so the rename stays atomic.

### Local write cost is negligible either way

| Variant | p50 | p95 | p99 | max | writes/s |
| --- | --- | --- | --- | --- | --- |
| temporary as a sibling (the spike) | 0.222 ms | 1.33 | 3.59 | 19.5 | 2,332 |
| temporary staged outside the sync root | 0.217 ms | 1.71 | 5.98 | 93.0 | 1,464 |
| sibling plus fsync before rename | 0.620 ms | 5.13 | 21.0 | 96.1 | 514 |

Moving the staging file out is free at p50. One record occupies 4,096 bytes on
disk regardless of payload. At the recommended policy, 600 agents produce 5.7
writes/s fleet-wide — 0.13% of one host's single-threaded write capacity, and
1.9% of the transport's own per-entry coalescing ceiling, so the coalescer never
engages and each write is one sync.

## Conclusion

Fixed quantization at 1% of the window, plus a compaction edge and a 300-second
heartbeat, replaces the spike's movement guard. It is the only measured policy
with zero missed warnings at every alarm threshold from 80% to 97%, its error is
bounded at one bucket unconditionally, and its write cost is capped by
construction at 100 writes per window fill regardless of how chatty a producer
is. The heartbeat is set to the existing state-record refresh interval so this
record never re-stamps more often than the record beside it.

The movement guard is not merely mistuned: its cost is set by its time floor
rather than by its movement clause, so it tracks harness event cadence instead
of agent behavior and would need per-harness constants. Quantization needs one
constant for all five harnesses for the same structural reason.

Pure quantization is tolerable only because the reader surfaces `ageMs` and
`stale` instead of deriving an old reading away — quantization alone leaves lag
unbounded (p95 583 s on Codex, 6,638 s on Claude, and up to 107 h on a parked
agent), and the heartbeat plus visible age is what makes that honest rather than
silent.

Separately, and independently of the write policy, the record's path had to
move: at the spike's location it would not have replicated at all.

## VRS Impact

- Closes the accuracy half of `DQ-C1` and rewrites the spec's write policy:
  `HARNESS_CONTEXT_BUCKET_PERCENT = 1` and `HARNESS_CONTEXT_HEARTBEAT = 300 s`
  replace the provisional `HARNESS_CONTEXT_WRITE_FLOOR` and
  `HARNESS_CONTEXT_MOVEMENT_PERCENT`.
- Adds HC-R09 (quantized writes, and the reason there is no per-harness knob)
  and HC-R10 (threshold-agnostic resolution), replacing the earlier guarded-write
  requirement.
- Adds HC-R05 (transport-visible placement, a pinned-name test, and staging
  outside the replicated subtree) and HC-T08 (replication depends on an include
  list this repository does not own). The record stays at
  `<agent-dir>/harness-context`; the include list is what changes.
- Restates HC-T05: the tradeoff is no longer "provisional constants" but
  "quantization bounds error, only the heartbeat bounds lag, and that is
  acceptable solely because age is visible".
- Leaves the transport half of `DQ-C1` open. `DQ-C11` (the same defect in the
  shipped `harness-state` record) closes with this one, since naming both
  records in the include list fixes
  [`05-harness-state`](../../05-harness-state/spec.md)'s record with no
  migration.
- Supplies the >100% data point in the record's field rules: 2 of 287,010
  replayed Codex readings give `used / window` up to 1.041.
