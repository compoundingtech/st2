# Observed harness state and transition history storage

Date: 2026-08-17

## Question

Where does per-agent observed harness state and its transition history live, given that the presence
`status` file replicates inside the catalog while Codex's `control-state.json` is host-local and does not —
and what does each choice cost at the maintainer's fleet size (~11 agents, 3 hosts)?

## Method

Evidence is **PROVED** (executed and measured here) or **UNKNOWN**; nothing was reconstructed. Five
disposable harnesses, none in the repository: one replays a simulated hour of 11 agents across 3 hosts into
a catalog-shaped tree for four designs, counting local writes and distinct `(path, sha256)` versions under
the replicated subtree — the transport-neutral cost unit; a second replays the same hour into a real Git
repository at one commit per 30 simulated seconds; two shell harnesses measure divergence, concurrent prune
and append, retention disagreement, and rotation; a fourth reproduces the version 1 presence failure; plus a
read-latency microbenchmark. The rate input is measured — real frames through `CodexControlState::observe`
yield **4 transitions per turn plus 1 at bind, clustered 0.1–0.4 ms apart** — and a record is ~110–130 B.

## Result

**The transport was not identified.** `docs/vrs/spec.md:773-775` names three (Fabric preferred, Git over
SSH, plain copy). Fabric is **UNKNOWN** — no binary on `PATH`, no repository, no systemd user unit, no sync
process, no network mount over the catalog — and its chunking was not inferred from its name. **Every number
below is Git or transport-neutral version counting; there is no measured cost model for the preferred
transport.** Design against the ratified constraint instead: no supported transport preserves file metadata
(`spec.md:778`). Live catalog (PROVED on `dev3`): 17 GB and 1,114,670 files, 1,110,732 under `*/resources/`,
so control files are ~3,400 and the question is churn, not size; 25,976 bus messages over 347 active hours
give median 48/h, p90 185/h, max 556/h. All 628 `status` files are legacy one-line records, so the v1 record
has **not** reached this fleet and a new one ships byte-versioned from its first write, skipping the legacy
migration and three-receipt fallback gate (`spec.md:812-820`).

**Turn-frequency writing into the catalog is viable on bytes and not on versions.** One simulated peak hour,
11 agents, 3 hosts:

| Design | repl. versions/h | whole-file bytes/h | packed Git bytes/h |
| --- | --- | --- | --- |
| A replicated mutable file per transition | 1,981 | 310,300 | 226,600 |
| B replicated shared append-only JSONL | 1,981 | 23,608,237 | 236,803 |
| C immutable file per transition | 3,830 | 554,047 | 646,619 |
| D two-tier, 60 s-throttled summary | 486 | 75,823 | 156,550 |
| **recommendation** (D + coalesced 64-entry segments) | ~1,110 | 2,350,920 | **362,592** |

B is the worst replicated shape because every version re-sends the grown file and "plain copy" has no delta.
Under Git the commit cadence dominates, so A vs. D is 1.4× rather than 4.1×; the recommendation costs 1.6×
A, which carries no history at all, and 1.8× less than C. A **50 ms debounce removes 74.7% of durable writes
at peak** (2,211 → 560/hour) while 250 ms and 1000 ms buy 0.1 point more, the burst being one millisecond
wide; 64-entry segments are the knee against 28.7 MB/hour for an unbounded log.

**Divergence and retention (PROVED).**

| Case | Result |
| --- | --- |
| Two hosts append to the same `lifecycle.jsonl`, then merge | **CONFLICT**, markers inside the log |
| Host A rotates `lifecycle.jsonl` → `.1` while host B appends | **CONFLICT** |
| Two hosts each create a distinct immutable file, then merge | clean |
| Host A prunes 3 old files while host B appends | clean |
| Host A keeps 2 entries, host B keeps 5, both converge | host B silently left holding **2** |

Resolution: `<agent_dir>/lifecycle/history/<host>.<segment>.jsonl`, 20× less packed Git than
file-per-transition and 5% more than in-place rotation. **Single-writer-per-agent is a hard rule**
(`R01`/`R03`), not a property of the naming; the host component is a **fork detector**, so a violation shows
up as a cleanly-merging second segment rather than a corrupted log. Since retention resolves to the minimum
and destroys data, the replicated knob is per-agent or catalog-wide and keyed on a time window.

**Current state and history are separate files ordered by `seq`, not `ts`.** A constant-size `current.json`
reads in 9.2 µs median against **1,798 µs** for a `listdir` over one day of file-per-transition events, 195×
and degrading with retention. Ordering cannot use `emittedAtMs`, since frames inside one burst share an
identical value; a per-agent `seq` surviving restart orders them, and overlap across host segments is a
fork.

**Stuck-*wrong* is reachable, not just stuck-*old*.** A failed Codex turn leaves the observed state wrong
until the next idle, and a heartbeat that preserves the recorded state — what `status::refresh` does
(`src/status.rs:148-153`) — keeps a wrong state fresh indefinitely, so staleness alone is not enough: a
bounded non-idle dwell on one `turnId` and a live-generation cross-check are both needed. **The cross-check
as first specified targets the wrong backend**: it cites `ExecGeneration` (`src/exec_backend.rs:34-48`), but
a driver-declared agent lowers to `TaskKind::Pty` (`crates/agent-spec/src/spec.rs:913-930`), so the
applicable primitive is pidfile plus `kill(pid,0)` (`src/ding/mod.rs:693-703`) — and being host-local it
leaves a cross-host reader with the staleness pair alone.

**A write-side self-wake is a prerequisite, not a footnote.** `src/codex_app_server.rs:326` watches the
whole agent directory recursively and unfiltered, so a `lifecycle/` beside the inbox makes the writer wake
its own delivery pump on every write. The **Mutation-only filesystem wakeups** invariant does not cover it:
`is_mutation` (`src/watch.rs:67-73`) filters `Access`, stopping *read* self-wakes only. The supervisor is
unaffected (`src/watch.rs:52-65`); DING is exposed via its `inbox_dir.parent()` fallback (`:909-915`).

## Resolved decisions

- History is append-only JSONL segmented by writing host and ordinal, closing at 64 entries or one hour,
  never scanned to answer "what is this agent doing", and ordered by a `seq` recovered as `max(current.seq,
  last parsable history seq) + 1`, the append preceding the `current.json` update. Every replicated write
  embeds `writtenAtMs` and `seq`, so the version 1 bug cannot return: PROVED under Git that rewriting
  `available\n` identically stages nothing and that a checkout resets mtime to the checkout time.
- Bursts coalesce into one durable write at 50 ms. **This buys 74.7% of writes and costs fidelity**: a
  coalesced history cannot answer dwell-time questions about states shorter than the window, which is the
  use the history was argued for. It is a metrics record, not a faithful trace.
- Writes reuse `atomic_json` (`src/codex_app_server.rs:2178-2200`), which fsyncs the file and its parent;
  `src/status.rs:290-301` does not, and the new record must not copy that. Appends are one `O_APPEND` write
  per line, so only the final line can be torn and readers drop it.
- Liveness has three defences because a heartbeat keeps a wrong state fresh forever, and the generation
  cross-check must be re-specified against PTY task generations before it ships. Retention is per-agent with
  a `catalog.kdl` default, never per-host, and the Codex delivery watcher must be scoped before the record
  lands beside it.

## Conclusion

**Two tiers with a segmented append-only history.** `$XDG_STATE_HOME/st2/lifecycle/<runtime-key>/` is
authoritative, hot and never replicated; `<agent_dir>/lifecycle/` is a throttled publish with a 5-minute
heartbeat whose segments close at 64 entries or one hour. That is cheaper on every metric measured here than
either single-tier alternative, and the host partition is what makes the history conflict-free.

**One stated cadence has lost its justification.** The host-local tier was specified at 15 s refresh and a
60 s stale window "so a delivery gate never routes on a state older than one minute", but
`docs/vrs/.decisions/0004-…` declines to let observed state authorize a PTY write, so that pair must be
re-derived from whatever actually reads it. The replicated tier is unaffected.

## VRS Impact

Proposed, pending review; this experiment does not itself amend the spec.

- A requirement family (suggested `LIFECYCLE-R01`..) for observed harness state, separate from the presence
  contract at `spec.md:700-830`; it must not extend or reuse the `status` file.
- An Agent Spec field rule for `lifecycle-history { retain; max-entries }` plus a `catalog.kdl` fleet
  default, which requires relaxing `src/catalog.rs:70` from hard error to warn-and-ignore *before* any
  catalog declares the field, or older binaries refuse a newer catalog — a rollout ordering constraint, not
  a design choice.
- Invariant candidates once tests exist: a replicated write is never byte-identical to its predecessor and
  no freshness decision reads mtime; given one declared writer, history is totally ordered by a monotone
  `seq` surviving restart, with overlap across host segments reported as a fork; a state is trusted only
  while fresh, within its dwell bound, and attributable to a live **PTY** generation.
- A prerequisite outside this scope: scope the Codex delivery watcher (`:326`) before a frequently-written
  `lifecycle/` lands beside it.
