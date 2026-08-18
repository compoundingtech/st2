# Observed harness state: driver observers and consumption surfaces

Date: 2026-08-17

## Question

Can `DING-R12`'s harness-neutral tuple — `idle | active | child | unknown` crossed with an input buffer of
`empty | nonempty | unknown` — be *published* as durable per-agent observability, produced by three
structurally different drivers (Codex app-server events, Claude hooks, PTY scraping) and consumed
generically, without weakening the pinned **Fail-closed observed native DING**, **Stable roster JSON**,
**Mutation-only filesystem wakeups**, or **Bounded DING PTY probe churn**? And is it *sufficient* for what
st2 observes?

## Method

A prototype was built in a worktree and compiled, not sketched: wire types in
`crates/st2-wire/src/activity.rs`; the observer trait, three observers, a write-on-transition publisher and
a `--watch` stream in `src/activity_observe.rs`; and one projection, `ding::observe_screen`, in
`src/ding/mod.rs` reusing `classify_composer` unchanged. Sufficiency was tested against a real enumeration —
all six merged `CodexHoldReason` values decomposed into the tuple — and an adversarial reader then
re-derived every claim from the repo's own fixtures, finding the two defects below.

## Result

24 new tests pass and the lib suite is green at 332, including the 18 named DING proofs. **That is weaker
than it reads**: the prototype adds ~35 lines to `ding/mod.rs` and calls nothing on the delivery path, so
the suite is a regression statement about unchanged code, and the two tests advertised as invariant oracles
pin structure — one asserts that a function whose body is `matches!(screen, ProvenIdle)` equals `screen ==
ProvenIdle`; the hold-reason sweep would pass if every row mapped to `unknown`.

**The tuple is lossy, not residue-free.** A total function from a 6-element enum into a 4-element set always
exists; faithfulness is what matters, and four of six rows report `unknown`. Two are states Codex positively
reported as **active**: `ActiveWithoutTurn` is minted inside the `"active"` arm of `observe_thread_status`
(`src/codex_app_server.rs:782-786`), where st2 merely cannot name a turn to steer, and `ConflictingTurn`
(`:818-821`, `:845-848`) is two turns believed live. A third, `SystemError`, is the `_ =>` catch-all
(`:795-798`), so a future unrecognized status word surfaces as a fabricated error; only `NotLoaded` is
honestly unknown. **What a consumer loses:** the machine-readable half is wrong in the permissive direction
for two of six rows, and `is_interruptible(Unknown)` is `true`, so the plane calls *routable* exactly the
agents Codex's own gate refuses (`:420-422`); what survives sits in `reason`, which no consumer may branch
on.

**Blocked-on-a-human is a first-class axis, not a per-driver detail.** Both harnesses prove it: Claude's
`PermissionRequest` fires in exactly the 2 of 9 captures where a human was blocked, and Codex carries a
level-triggered `activeFlags` plus ten approval and user-input requests with a `serverRequest/resolved` exit
edge. Folding it into `active` makes `is_interruptible` answer **false** for the one state where reaching
the agent is the right action, and the argument keeping `child` first-class applies with more force since
blocked also changes *who* must unblock. Deferral is not free: `ActivityState` makes `Unknown` its
`#[serde(other)]` catch-all (`crates/st2-wire/src/activity.rs:82-83`), so a v1-pinned reader decodes a later
`blocked` as `unknown`, hence routable — and `publish`'s change comparison
(`src/activity_observe.rs:564-566`) ignores `reason`, so `Held{Compaction} → Held{Review}` is `Unchanged`
and the word carrying "a human is blocking" is swallowed.

**The PTY observer proves `idle` for a Claude pane exactly once, and never again.** `observe_screen` is
`classify_composer(screen, "")`, mapping only `EmptySafe` to `ProvenIdle` and discarding the `ExactSafe` arm
because an empty composer matches a harness's Empty marker rather than `Typed("")`. That holds for Codex,
whose markers are distinct (`src/ding/harness/codex.rs:56-67`), and fails for Claude, which extracts
composer text and string-compares (`src/ding/harness/claude.rs:96-131`): `logical_soft_wrap_candidates("",
1)` hits the `rows.first()` `None` arm (`src/ding/composer.rs:46-48`, `"".lines()` yielding zero rows) and
returns `Proven([""])`, so the exact match is true. Measured against the repo's own fixtures,
`mature_idle_claude_screen` with `expected=""` yields `ExactSafe` → **unproven** while `idle_claude_screen`
yields `EmptySafe` → `ProvenIdle`; Claude shows its placeholder only on an unused pane, so the observer
proves `idle` before anyone types and reports unproven forever after. The root cause is the sentinel:
`expected = ""` **collides with the Claude arm's representation of an empty composer**, the same shape of
error decision `0001` corrected. The direction is safe — never a false `idle` — but the arm delivers nothing
for one of two shipped harnesses, and its test `the_pty_observer_proves_idle_or_admits_it_proved_nothing`
fixtures only Codex in the positive slot.

Three further prototype defects: `atomic_write` uses a **fixed** temp name (`:619-624`) where
`src/status.rs:303-313` uses pid plus a process-local counter, so two publishers on one host collide on
`activity.tmp`; `--watch` re-projects on wakeup and diffs against the row it last *saw* (`:689-725`),
reporting the latest state and never each intermediate; and `ACTIVITY_STALE` and its siblings are `=
status::STATUS_*` (`:62-66`), aliases of constants pinned by a named invariant row, so tuning presence
silently retunes this plane.

## Resolved decisions

- **The concept is named "observed harness state".** It is deliberately not "observed activity status":
  `agents --json` already ships `lastActivity` under the pinned **Stable roster JSON** row, `R08`
  (`docs/vrs/spec.md:685-689`) reserves "activity status" for *agent-declared* progress, and `DQ3`
  (`:959-962`) leaves the term undefined, so a third meaning of "activity" in one roster payload would
  collide with a pinned guarantee.
- The state words are `DING-R12`'s, reused rather than proposed, with `child` distinct from `active` and the
  input axis a field rather than a state. `unknown` stays derived and never written, mirroring
  `src/status.rs` and `R23`, and may never be flattened into `idle`.
- **A blocked-on-a-human axis is added before v1 is pinned**, with `is_interruptible(Blocked)` true, and the
  change comparison must include `reason` or the transition carrying it is dropped.
- `Held` is decomposed, never imported — its doc comment makes it the complement of *steerable*, a delivery
  predicate — and the decomposition must stop reporting `unknown` for the two rows Codex reported active.
- Write only on transition, or the observer becomes the **Mutation-only filesystem wakeups** failure from
  the write side; the staleness horizon is a documented constant of its own, not a re-export. Observation is
  a separate trait from `Driver`, in the lib crate, since `src/driver.rs` states expansion does not read
  files, inspect a harness, or execute a process. It projects durable evidence into the tuple rather than
  acquiring it.

## Conclusion

Qualified yes. The `DING-R12` vocabulary is the right base and should be published rather than duplicated,
but it is **not sufficient as it stands**: it needs a blocked-on-a-human axis, and the Codex decomposition
must stop answering `unknown` for two states Codex reported active. The PTY arm is broken for Claude and
must be fixed or cut before it is described as covering unmanaged panes, and must in any case be driven by
the DING loop's budgeted peek rather than by a reader. Observed state must never authorize a PTY write
(`docs/vrs/.decisions/0004-…`). Increment 1 should therefore be Codex-only with the blocked axis, behind a
new sibling CLI command. One half of the repo's own bar is also unmet: `DQ3` requires proving both
stale-state *and* supervisor-following behaviour before this shape enters `AGENT-SPEC.md`, and
supervisor-following is addressed nowhere in this bundle.

## VRS Impact

Adds `st2.observed-harness-state.v1` and its transition and report shapes to the wire surface, publishing
the `DING-R12` vocabulary for non-delivery consumers plus one new axis, and an ontology distinction not yet
in writing: an agent has a **declared** presence it sets for itself and an **observed harness state** st2
witnesses, reported side by side so they can be seen to disagree.

**Stable roster JSON** is untouched for increment 1: the CLI surface is a new sibling command on the `st2
tasks --json` precedent, so the pinned assertions in `src/agents.rs` need no edits, and folding observed
state into `st2 agents --json` later costs three literal edits plus an amendment to that row's wording. Two
invariant rows are earned only once real observers write — a freshness discipline mirroring **Agent-declared
presence**, and the boundary in `docs/vrs/.decisions/0004-…` — and neither should be added before a test
proves it.
