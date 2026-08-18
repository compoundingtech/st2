# Only a synchronous proof authorizes a PTY write

Status: proposed — the transport-authorization half stands; the scheduling half is withdrawn to an open
question. Do not treat as accepted.

## Context

Two positions were chosen independently. Delivery was to converge onto observed harness state, so that the
per-agent state becomes the source of truth for "is it safe to DING this agent right now", replacing the
one-shot composer gate. Separately, routing was to be **fail-open** for unobservable agents, so a broken
hook never stalls an inbox. But `INVARIANTS.md` pins **Fail-closed observed native DING** — "unread,
blocked, timed-out, errored, unknown, and unrecognized states retain ownership and later FIFO work remains
blocked", with eighteen named tests in `src/ding/mod.rs` — so a design that lets "unknown = interruptible"
reach the DING path deletes that clause.

This record does not reconcile the two positions. It **declines convergence on transport authorization** —
the first position is overruled — and defers the second.

## Decision

Observed harness state is **inadmissible as transport authorization**. The PTY transport layer asks the
synchronous, adjacent composer proof, unchanged; only `ScreenObservation::ProvenIdle` authorizes;
`ActivityState` is not a parameter of that decision and must not become one.

The screen scraper is folded in as an observer that *feeds* shared state — the DING loop publishes what its
already-budgeted peek proved — never as a consumer that reads it. This keeps **Bounded DING PTY probe
churn** intact: a reader must never drive a `pty peek`.

**Withdrawn: whether observed state may schedule.** The routing layer that would call `is_interruptible`
does not exist, in code or in `spec.md`, so fail-open routing is an open question rather than a decision. It
carries a live cost: if the DING loop ever withholds a poke because observed state is `active`, that is a
**second pre-attempt deferral gate**, and invariant row 17 ("`busy` delivers immediately; only fresh `dnd`
defers", pinned by `pending_delivery_ignores_busy_but_respects_fresh_dnd_archive_and_retry`) needs amending.
This record does not amend it and must not be read as having cleared it.

## Options

| Option | Tradeoffs |
| --- | --- |
| Observed state gates DING directly, fail-open on `unknown` | Delivers the stated goal literally. Deletes the invariant's central clause: an unobservable agent is pasted into blind. Loses adjacency, so it is unsafe even when the observed state is correct. |
| Fail-closed everywhere; unobservable agents are never routed | Preserves the invariant trivially. Makes a broken hook silently remove an agent from the fleet. |
| Only the synchronous proof authorizes; scheduling left undecided | Requires holding two verdicts, and gives PTY agents no "never stalls an inbox" property. Keeps the invariant intact by construction and makes the residual stall visible for the first time. |

## Evidence and Argument

- **Implementation fact — the code already splits the two layers.** Before any bytes are sent,
  `observed_poke_with_window` maps an unprovable screen to `PokeOutcome::Deferred`
  (`src/ding/mod.rs:607-609`) and `flush_pending` breaks without staging (`:1106`); once a paste may have
  landed, every ambiguity yields `Staged` (`:614-617`, `:626-628`, `:680-687`). The invariant's "retain
  ownership" clause governs the post-transport phase: it is an *exactly-once* guarantee, not a
  never-attempt-when-unsure guarantee.
- **Implementation fact — the native exemption is enforced, not merely true.**
  `crates/agent-spec/src/spec.rs:887-888` refuses a declaration carrying both `ding` and `deliver`, so a
  natively-delivered agent cannot enter the PTY path at all.
- **Implementation fact — durable state cannot supply what the gate requires.** The gate's safety comes from
  *adjacency*: `submit_after_final_observation` is intentionally adjacent to the bare-Return operation, and
  `retry_staged_with_window` sends one bare Return only after **two adjacent** `RetainedSafe` observations.
  A record written by another process at another time supplies neither. PR #123 draws the same line from the
  other side — its lease is capped at two seconds and re-verified against a live PTY STATUS packet before a
  generation-guarded write, and a durable record is strictly weaker evidence than that lease.
- **Independent critique — delivered, and it weakened the record.** The reviewer looked for a path by which
  an activity signal authorizes a write or releases staged ownership and found none, which is the direct
  question. But three supports fell. "rustc enforces the separation" does not survive:
  `pty_transport_authorized` and `is_interruptible` have **no production caller** — every call site is
  `#[cfg(test)]` — so the property is trivially true of uncalled code and needs a real mechanism the moment
  a consumer exists. The two claimed invariant oracles pin structure rather than behaviour; one asserts that
  a function whose body is `matches!(screen, ProvenIdle)` equals `screen == ProvenIdle`. And the reviewer
  found a concrete defect in the prototype this record cites: its PTY observer proves `idle` for a Claude
  pane exactly once, before anyone types into it.

## Consequences

- **Fail-closed observed native DING** needs no amendment. Nothing here reaches the phase it governs.
- Fail-open would not deliver "a broken hook never stalls an inbox" for PTY agents and cannot: if a pane is
  permanently unprovable the message stays undelivered whatever a router decides. The honest gain is that
  the stall becomes *visible* as `unknown`/`unproven` beside a declared `busy`. Any escape hatch that pastes
  into an unprovable pane must be explicitly authored.
- The `reason` field is diagnostic, not a safety hinge; nothing gates on it.
- The **Bounded DING PTY probe churn** claim above is about `pty peek` only. A separate prerequisite is
  unresolved: `CodexInboxDelivery` watches the whole agent directory unfiltered
  (`src/codex_app_server.rs:326`), so a frequently-written `lifecycle/` beside the inbox turns every
  observed transition into an inbox scan. **Mutation-only filesystem wakeups** does not cover it —
  `is_mutation` (`src/watch.rs:67-73`) filters `Access`, stopping *read* self-wakes only — so that watcher
  must be scoped before the record ships.
- Convergence remains possible on what is actually shared — pre-attempt selection, FIFO ordering, backoff
  scheduling. The adjacent proof stays regardless.

## Evidence required for acceptance

- a named test pinning that no PTY write path takes an `ActivityState`. This is a **precondition**, not a
  consequence: without a production consumer the guarantee is vacuous;
- a production consumer of the observed state, so the separation is a property of the system;
- a ruling on the withdrawn scheduling half, with invariant row 17 either amended or confirmed unaffected by
  a named test;
- the PTY observer's Claude defect closed, or the observer cut from this increment.
