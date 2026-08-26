# Resync requirements

## Context

This subsystem defines resync events: supervisor-emitted notifications that
tell a live agent one of its declared resource carriers changed on disk, so
the session can re-read it without a restart. The analogy is hot module
replacement for context: the running process stays up; the changed module
(the carrier) is swapped underneath it and the process is told which module
to reload.

Delivery rides the declared event-stream machinery ([`04-stream`](../04-stream/requirements.md))
through a built-in per-agent `resync` stream that exists without declaration.
The decision records are [`.decisions/0008`](../.decisions/0008-resync-events-ride-the-built-in-stream.md);
watch scope, noise model, and event identity were settled with the principal
on 2026-08-25 (decision requests Q1–Q6). Filesystem watching refines root
[`R14`](../requirements.md) (explicit filesystem-event contracts) and
[`R15`](../requirements.md) (bounded event coalescing); delivery inherits
root [`R05`](../requirements.md); Resource bindings are defined by
[`02-agent-spec/F07`](../02-agent-spec/spec.md) and root [`R20`](../requirements.md)/[`R21`](../requirements.md).

## Assumptions

- **RESYNC-A01 Local carriers only:** A carrier is watchable iff its binding
  URI denotes a local regular file: an absolute `file://` URI, a
  catalog-relative path resolved against the agent's directory, or a scheme URI
  successfully resolved to a contained local path by a declared
  [`07-resource-profile`](../07-resource-profile/requirements.md). Bindings
  whose scheme has no registered profile, or whose registered profile fails,
  are never watched and never stop the supervisor; their watchability remains
  observable through catalog inspection. The agent's own declaration file is
  covered by the existing declaration watcher and is also a resync source.
- **RESYNC-A02 Trusted fleet:** The supervisor is the only resync producer;
  it runs inside the owner host's existing trust domain (root `A02`). No new
  authentication surface exists.
- **RESYNC-A03 Equal bytes mean equal state:** For an on-disk carrier,
  content equality is state equality, so an equal-byte rewrite is suppressed
  before event capture. Distinct observed transitions remain distinct
  occurrences even when they repeat the same old/new byte states.

## Acceptable Tradeoffs

- **RESYNC-T01 Static classes, no write attribution:** Classification comes
  from a Resource Profile declaration for profile-resolved carriers and from
  fixed path roles for native local carriers; it does not infer who wrote the
  change. An external edit into a store the agent authors (context, decisions)
  is silent by classification; a st2-mediated write to an immediate-class
  carrier would notify. True authorship attribution is deferred until evidence
  shows the gap matters.
- **RESYNC-T02 Provisional windows:** Coalescing window lengths ship as
  provisional constants tuned by observation, per the rollout note in
  [issue #341](https://github.com/compoundingtech/st2/issues/341).
- **RESYNC-T03 No catch-up:** Like any restarted stream, a restarted
  supervisor re-observes current digests silently and emits only subsequent
  changes. Changes missed while the supervisor was down are discovered by the
  agent's own next read or by reconcile, not reconstructed as events.

## Requirements

### Must observe declared carriers

- **RESYNC-R01 Watch set:** While an agent is running on this host, the
  supervisor watches exactly the local files denoted by that agent's active
  resource bindings plus its own declaration file. The watch attaches to each
  carrier's parent directory non-recursively and tracks directory identity so
  whole-file replacement by rename keeps working. Nothing outside this set is
  watched; installation failure degrades to timer-based digest polling rather
  than losing the capability.
- **RESYNC-R02 Explicit watch contract:** The resync watcher states what it
  watches and why, denies everything else by default, ignores read/open
  access events, and never traverses payload trees — refining [`R14`](../requirements.md)
  and the mutation-only wakeup invariant.
- **RESYNC-R03 Seeded baseline:** On supervisor start the current digest of
  every watchable carrier is recorded without emitting. Only a transition
  between observed contents produces an event; a restart alone wakes nobody.

### Must classify before notifying

- **RESYNC-R04 Class defaults:** Each carrier belongs to exactly one class:
  *immediate*, *silent*, or *coalesced*. A profile-resolved carrier takes the
  trusted class declared beside its resolver; `silent` excludes it from the
  watch set. Native local carriers use fixed path defaults: immediate for the
  agent's declaration file and goal carriers, silent for stores under the
  agent directory that it authors (context, decisions, friction logs), and
  coalesced for every other local carrier. Immediate changes notify within a
  short coalescing window; coalesced changes notify within a longer window; a
  burst of mutations collapses to one notification pass per window.
- **RESYNC-R05 Bounded coalescing:** Window behavior is bounded and tested
  per [`R15`](../requirements.md): emissions happen at window boundaries, not
  per writer event, and a fast-rewriting carrier cannot produce unbounded
  notifications.

### Must emit honest, deduplicated events

- **RESYNC-R06 Event shape and occurrence identity:** One carrier change
  becomes one stream event on the built-in `resync` stream: subject
  `resource <binding> changed`, body naming the binding label, resolved path,
  old and new content digests, and an occurrence token. The token combines
  the current supervisor incarnation — catalog-lock device/inode plus
  supervisor PID/start-time ticks — with a sequence retained independently by
  each subscription. A subscription advances its sequence only when it
  captures a new immutable transition; a failed publication retries the same
  body, token, and identity. The event identity is the SHA-256 of that
  canonical rendered body. Thus replay is stable, while A→B, B→A, A→B gives
  the repeated A→B legs distinct identities. The grouping key is the binding
  label; every emit declares supersession so a binding collapses to one unread
  head.
- **RESYNC-R07 Built-in stream:** The `resync` stream exists on every agent
  without declaration and is reserved: a user-declared stream of that name is
  refused. Only the supervisor's crate-internal publisher admits the built-in
  stream; public event ingress remains declaration-gated. After that admission,
  publication reuses the ordinary ring deduplication, receipt validation,
  supersession semantics, inbox transport, and DING `»` marker inherited from
  [`STREAM-R03..R07`](../04-stream/requirements.md).
- **RESYNC-R08 Lifecycle honesty:** No events accumulate for suspended or
  retired agents. Digest seeding happens when a seat launches or resumes, so
  resume re-observes current state silently and only later transitions notify.

## Evidence

The composition (parent-directory watch → classification → digest-keyed
superseded emit → DING wake) is proven by integration tests introduced with
the implementation; see [`.experiments/`](./.experiments/) for the record
and the pre-existing failure baseline it was verified against.
