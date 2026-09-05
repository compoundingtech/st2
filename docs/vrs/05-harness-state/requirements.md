# Observed harness state requirements

## Context

This subsystem defines observed harness state: the driver-written record of
what a harness is seen doing, published into the catalog beside the agent's
declared presence. It refines the slot root [`R08`](../requirements.md) leaves
undefined — R08 ratifies *declared* activity status and root spec `DQ3` marks
activity, plan, and plan step "remain undefined" — by specifying the
*observed* axis only. Declared activity status, current plan, and current plan
step stay unspecified and are not claimed here. Where this file and the root
disagree, the root wins and this file is wrong.

Provider-specific idle/active/child/unknown classification belongs to harness
drivers per [#162](https://github.com/compoundingtech/st2/issues/162); st2
core owns only the generic envelope, its fencing, freshness, and exposure.
The record-shape and coverage decisions are recorded in
[`.decisions/0006`](../.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md).
Delivery gating is explicitly not this subsystem's concern: DING and the
native transports keep their own evidence
([`01-ding/requirements.md`](../01-ding/requirements.md)), and no delivery
path reads the observed-state record. The adjacent native-driver diagnostic
snapshot reports where that independent evidence path failed; it never
authorizes or changes delivery.

## Assumptions

- **OHS-A01 Drivers can observe their harness:** Every maintained harness
  offers a positive observation source — the Codex app-server control stream,
  Claude lifecycle hooks plus the wrapper's child poll, pi's injected
  extension, and — conditionally, until `DQ-H6`'s capture confirms its event
  semantics — OpenCode's server surface, which at this layer is the candidate
  an experiment must validate before the producer trusts it. Where a source
  exists the driver
  projects it; where none exists the driver writes nothing, and the absence of
  a record is itself honest ("never observed"), never a fabricated state.
- **OHS-A02 Advisory surface:** Consumers are humans, supervisors, Doctor, and
  a roster/TUI. The record authorizes nothing: no delivery, no lifecycle
  action, no reconciliation. A wrong-but-fresh record can mislead an operator;
  it cannot misdeliver a message.
- **OHS-A03 Trusted writers:** The record is unauthenticated catalog state
  under the trusted-fleet model (root `A02`). The writers are the owning
  session's driver processes — the wrapper that owns the presence lease, and
  the channel or hook subprocesses it shares its incarnation token with;
  nothing verifies that claim.

## Acceptable Tradeoffs

- **OHS-T01 Unmeasured transport cost:** Turn boundaries are far more frequent
  than the five-minute presence refresh, and no measurement establishes what a
  per-transition replicated write costs on a real catalog under a real
  transport (`DQ-H2`). v1 accepts this, bounded by writes only on transition
  plus the slow heartbeat.
- **OHS-T02 Blocked is vacuous under bypass:** `blockedOn: human` can only be
  produced where a harness asks a human anything. Under Claude
  `bypassPermissions` — what `examples/native/agent-claude.kdl` ships — the
  axis never fires. The field is still v1: a later-added axis decodes as
  `unknown` in every pinned reader, the opposite of a conservative default.
- **OHS-T03 No history:** v1 records only the current state. Measured burst
  coalescing (4 transitions per turn, 0.1–0.4 ms apart, #268) would erase any
  state shorter than the coalescing window, so a v1 history could not be
  truthful about dwell time. `transitions` and `sinceMs` keep a later history
  additive.
- **OHS-T04 Ungraceful-death windows:** SIGKILL cannot be caught, so an
  external forced kill can leave a live-state record whose writer is gone.
  Same-host readers narrow that window with the liveness cross-check
  (OHS-R07) where the session is PROVABLY dead — pidfile present, process
  gone — while `pty kill` removes the pidfile and leaves the probe
  indeterminate for the rest of the window; the next session's written
  ownership claim supersedes the orphan at relaunch, and cross-host readers
  wait out the staleness horizon.

## Requirements

### Must publish one observed envelope

- **OHS-R01 Observed envelope:** Each agent has at most one observed-state
  record, `<agent-dir>/harness-state`, schema `st2.harness-state.v1`, written
  only by the owning session's driver processes — the wrapper, its channel,
  or its hooks; one logical owner per record, sharing one incarnation token,
  and nothing outside the driver writes it. It carries the full v1
  tuple: `state ∈ idle | active | child | ended`, `blockedOn ∈ human | none`
  (with `ask ∈ none | permission | question | review` naming the kind of
  human ask machine-readably, so no consumer branches on `reason`; `review`
  is reserved — no maintained producer emits it),
  `inputBuffer ∈ empty | nonempty | unknown`, plus the observing harness, a
  diagnostic `reason` no consumer branches on, and fencing/freshness fields.
  `child` is reserved: part of the contract, decoded by v1 readers, no
  producer yet (`DQ-H3`). The record is additive-tolerant on read; unknown
  future enum words decode as indeterminate, never as any definite value.
- **OHS-R02 Derived-only unknown:** `unknown` is mandatory in the read
  vocabulary, derived, and never written. One constructor produces every
  indeterminate observation, each absence carries a distinct reason
  (malformed, stale, future-skew, session-dead), and no path derives `idle` —
  or anything else — from missing evidence. A missing record reads as "never
  observed", which is distinct from `unknown`.
- **OHS-R03 Transport-safe freshness:** Freshness lives in the record bytes:
  an embedded origin timestamp with its own staleness and future-skew
  constants, deliberately not aliases of the presence constants. No read path
  consults file mtime. Every write is byte-distinct, so a transport that
  carries content but not metadata always carries a refresh.

### Must never wake what it informs

- **OHS-R04 Scoped delivery-input watching:** A write to the observed-state
  record wakes no delivery pump, no reconciliation pass, and no watcher owned
  by its own writer. Delivery pumps watch their inbox and the presence record,
  not the agent directory wholesale. This is a prerequisite: the record sits
  in a tree the Codex pump watches unfiltered today.

### Must be produced by drivers under the evidence rule

- **OHS-R05 Driver-owned projection:** Classification is driver work. The
  Codex producer projects the existing control state with the corrected rows:
  `ActiveWithoutTurn`, `ConflictingTurn`, and `Review` report `active` —
  review's enter and exit are model-emitted items inside a running turn, so
  nothing there awaits a human (matching the projection's
  `Held { Review }` → `active` / `blockedOn: none` row) — while
  `WaitingOnApproval` and `WaitingOnUserInput` report `active` with
  `blockedOn: human`. `Compaction` and `UnknownProtocol` report `active`, and
  `NotLoaded`/`SystemError`/`AwaitingStatus` withhold rather than write.
  `Held` — a delivery predicate — never appears in the published vocabulary.
- **OHS-R06 Heartbeat only on evidence:** A writer re-stamps the record on the
  presence cadence only while it still observes its harness, and stops on
  evidence loss so the record ages to `unknown` instead of staying confidently
  wrong. On teardown the wrapper writes its terminal record — carrying the
  exit outcome — *before* any escalation that could take the wrapper itself,
  and a terminal record is never re-stamped.
- **OHS-R07 Liveness cross-check:** The record names the pty session whose
  liveness vouches for its live states. A same-host reader that positively
  proves that session dead — its pidfile present, its process gone — reads
  the record as `unknown` even while fresh; an indeterminate probe (an
  unreadable registry, or a pidfile `pty kill` already removed) downgrades
  nothing — unprovable evidence is never reported as death. The check is a
  narrowing, not a closure: what it cannot prove, the relaunch-time written
  claim supersedes and the staleness horizon bounds. A fresh `ended`
  survives the check: a terminal record is supposed to outlive its writer.
- **OHS-R08 All-harness coverage:** Codex, Claude, pi, and OpenCode each ship
  a producer. pi's is evented through the injected extension (the positive
  idle signal root `DQ2` asks for). OpenCode reaches driver parity first —
  typed driver, session wrapper owning the presence lease, then its producer
  and native delivery transport.

### Must be readable beside declared presence

- **OHS-R09 Roster join:** `st2 agents --json` carries `observedState` beside
  declared `status` in one payload — the wedged-agent comparison (declared
  `busy`, observed `idle`) must not require joining two commands. Observed
  state is a third independent axis: it never rewrites presence, desired
  lifecycle, or `lastActivity`, and the pinned roster wire assertions change
  deliberately, in the same change, with the new proof named.
- **OHS-R10 Doctor exposure:** Doctor surfaces observed state for agents it
  owns as advisory output — a stale or session-dead record beside a `running`
  desired state is worth a warning, never an exit-code failure in v1.

### Must make native-driver degradation structured and recoverable

- **OHS-R11 Closed driver diagnostic:** Each agent has at most one current
  `<agent-dir>/driver-diagnostic` record, schema
  `st2.driver-diagnostic.v1`, owned by native driver core. The tagged,
  additive-tolerant snapshot carries a closed stage, reason, source, producer
  version/support classification, origin timestamp, and clearing contract.
  Readers derive evidence age. Generic roster and Doctor consumers branch on
  these typed fields, never provider error prose.
- **OHS-R12 Fail-visible reading:** An absent record is explicitly `absent`;
  malformed bytes, an unsupported schema, unknown vocabulary, future clock
  skew, or a known reason paired with the wrong stage/source are
  `indeterminate`. None reads
  as healthy. A valid record is `failure`; success is represented only by
  stage recovery clearing that stage, with the record removed after the last
  outstanding stage recovers.
- **OHS-R13 Exact OpenCode boundaries:** The OpenCode driver publishes or
  clears diagnostics at `versionGate`, `apiGate`, `sse`, `seed`, `delivery`,
  and `readBack`. The in-process publisher retains at most one failure per
  stage and persists the earliest failing boundary, so a downstream transport
  symptom cannot hide an admission failure. Publishing is advisory and must
  not change prompt submission, retry, durable read-back, or archive
  semantics.
- **OHS-R14 Shared exposure and repair:** `st2 agents --json` carries the
  complete `driverDiagnostic` projection for every row, including explicit
  absent and indeterminate states. For a declaration whose native driver
  publishes the record (OpenCode, Claude, Codex, omp in this version), Doctor
  reads the same core projection and emits advisory-only stable repair text for
  every state; the two surfaces cannot independently interpret provider strings.
  Absence is advised only for a driver that publishes a boundary result on
  every launch: Claude, Codex, and omp publish only on a credential rejection,
  so absence is their healthy steady state and earns no advisory.
- **OHS-R15 Bounded telemetry:** Each failure/recovery transition emits a
  driver-diagnostic span/event and counter. Metric labels and `span.label`
  are limited to closed `driver`, `stage`, `reason`, `source`, `support`, and
  `outcome` vocabularies. Raw versions and any agent/runtime/session/message
  identity are forbidden from metric labels and `span.label`; raw prompt,
  message body, and path data are forbidden from the durable record and all
  labels.
- **OHS-R16 Provider credential boundary:** A `providerAuth` stage carries the
  single reason `providerAuthRejected` from the single source `turnResult`, and
  is published by the Claude, Codex, and omp drivers from their own typed
  turn-failure signal — never from provider prose and never from credential
  knowledge, which st2 does not hold. It projects between `seed` and
  `delivery`: the four gates that prove st2 can read the producer at all
  outrank it, and the delivery and read-back symptoms it causes do not. The
  same rejection sets `observedState.reason` to `providerAuth`, the word the
  OpenCode producer already publishes. Only positive evidence — a turn that
  reached its ordinary end — clears it; an unclassified failure leaves a
  standing rejection standing. Delivery semantics are unchanged.

### Must separate what is wrong from what is being asked

- **OHS-R17 Condition is its own axis:** The record carries a tagged
  `condition ∈ clear | fault`, published under its own record version so the
  meaning of a version's bytes stays decidable from those bytes. A fault
  carries a CLOSED `category ∈ authentication | account | quota | rateLimit |
  provider | context | configuration | policy | harness` — closed because
  consumers route on it — an OPEN, provider-namespaced `code` for diagnostic
  granularity, a `recovery ∈ automatic | human | terminal | unknown`, and its
  own semantic observation time. Provider prose is diagnostic only; no
  consumer branches on it. A category word outside the closed set leaves the
  fault UNTYPED and still routed by its recovery: neither borrowing a
  neighbouring category nor discarding the whole observation is acceptable,
  because the first invents a claim and the second makes a real fault stop
  being reported. Versions without the axis project it as EXPLICITLY absent —
  never `clear`, and no fault is inferred from their legacy words.
- **OHS-R18 An ask is an actual human prompt:** The ask axis is tagged so
  `none`, `pending` with its kind, and `unknown` are three distinct
  statements, and it speaks only about prompts. A fault is not an ask: a
  throttled provider asks nobody anything. Where a fault and an ask coexist,
  remediation is primary and the ask remains visible on the raw axis.
- **OHS-R19 Strict edges, typed indeterminacy:** A record whose OBSERVATION
  axes contradict each other is not a weaker observation; it is not an
  observation. Every rejection carries its own reason word — a `clear`
  bearing fault evidence, a fault missing recovery or its observation time, a
  recovery deadline on a recovery this version recognizes as non-automatic,
  an inverted deadline, an ask that names a kind while claiming none — so an
  operator can tell a producer bug from a stale seat and one bug from
  another. Rejection is scoped to what the contradiction actually damages:
  strictness must not destroy evidence. A deadline beside an UNRECOGNIZED
  recovery word is kept, because that class may be automatic in a version
  the reader predates and rejecting it would turn a fault that pages into a
  non-paging row; and a badly stated conversation reference degrades only
  that axis, because a broken side-channel is not evidence about the harness.
  Indeterminacy is exposed TYPED, carrying that word and the age of the
  evidence it was derived from when the bytes carried a usable stamp; the
  legacy scalar reason remains a projection of the same single value and is
  never derived independently.
- **OHS-R20 Two clocks:** Transport freshness and semantic observation are
  separate. The heartbeat proves only that a writer still holds evidence; it
  never moves a fault's observation time or its recovery deadline, and
  attention is derived at READ time. An `automatic` recovery past its own
  deadline becomes an untyped, unknown-recovery fault that pages until an
  explicit paired clear, a terminal record, a new claim, or a new incarnation
  replaces it — a recovery that missed its own deadline is no longer evidence
  of anything automatic.
- **OHS-R21 One shared disposition:** st2 owns normalization and publishes
  ONE derived disposition — exactly three closed axes: a state, how soon a
  human is needed, and what that human would do first — from one pure
  function, exposed on the roster, the catalog graph, and Doctor. Consumers
  read it; none re-derives urgency, because two independent derivations are
  how one consumer starts paging for what another ignores. Raw activity, the
  actual human ask, the condition, and recovery stay orthogonal and ride
  beside it, so a consumer that disagrees can see exactly what was folded.
  Ended and record-level indeterminate never page. A native-driver diagnostic
  failure contributes through the same function — it is a fault the harness
  could not report itself — and still never changes delivery.
- **OHS-R22 Conversation reference is identity and capability:** The
  conversation bridge is tagged `linked | unavailable | unsupported`, and a
  linked reference carries the driver's namespace, the provider's opaque
  conversation identity, the runtime incarnation, an explicit history
  mutability claim with the evidence for it, and a FINITE verification bound
  so a consumer ages the claim instead of trusting it forever. A record that
  states nothing about a conversation claims no capability, which is distinct
  from `unsupported`. Conversation content stays out of the record entirely.
  A `linked` reference that is not fully stated is not trusted and not
  discarded either: it degrades to `unavailable` carrying st2's own closed
  rejection word — never provider prose — and the observation's activity,
  condition, and ask axes stand untouched.
- **OHS-R23 Reader-first activation with a positive drain gate:** A record
  version is read, strictly validated, and projected before any writer emits
  it, and exactly one writer-selection point decides which version this build
  writes. Every projected row carries the EXACT version its record declared,
  including versions the build cannot interpret, so a migration's drain gate
  is positive — "every row reads the new version" is checkable, while "no row
  is still the old one" is not checkable from any absence. The ownership
  envelope is version-independent, so a claim honors the schema fence and the
  ownership sequence of a record whose meaning it cannot read rather than
  treating those bytes as an empty seat.

## Evidence

The measurements are #268's, taken 2026-08-16/17 on one host and carried with
their original caveats: 1298 presence files all legacy one-line records, 4
transitions per turn 0.1–0.4 ms apart, Claude hook timelines (blocked entry in
2 of 9 captures, exit in 1), silent Claude death under SIGTERM/SIGKILL, and
the Codex `activeFlags` schema present in the measured codex-cli versions
(#268's first comment). The startup gate now checks that generated schema
directly. The shipped code evidence is in-repo: the Codex state
machine and its hold reasons, the unfiltered agent-dir watch beside the
presence refresh that writes into it, and `src/harness_state.rs`, which
implements the envelope this file ratifies.
