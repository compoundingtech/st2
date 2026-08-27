# st2 language

This is a small disambiguation index, not a behavioral specification. Each
canonical term links to the source that owns its meaning. Follow that source
for behavior, lifecycle, ordering, and wire or file formats.

st2 inherits agent-authoring language from the
[canonical Agent Spec](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#canonical-agent-specification).
Terms without an independent authority are deliberately absent.

## Canonical language

### agent

The actor st2 models.

Authority: [Agent Spec overview](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#canonical-agent-specification);
[R10 agent-only identity](requirements.md#L98-L99)

### agent declaration

The authored KDL representation of one agent.

Authority: [Agent Spec discovery and declaration shape](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#discovery-identity-and-host);
[R02 canonical KDL](requirements.md#L36-L45)

### agent runtime

A running instance of an agent declaration.

Authority: [declared runtime vision](vision.md#L16-L26);
[`AgentSpec` runtime model](../../crates/agent-spec/src/spec.rs#L24-L50)

### agent task

A terminal-backed or terminal-free unit declared for an agent.

Authority: [Agent Spec task contract](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#compact-and-explicit-tasks);
[`Task`](../../crates/agent-spec/src/spec.rs#L121-L148)

### agent identity

The bare `identity` value of an agent declaration. It is not a claim of
fleet-wide uniqueness.

Authority: [`AgentSpec::identity`](../../crates/agent-spec/src/spec.rs#L24-L50);
[Agent Spec identity rules](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#discovery-identity-and-host)

### bus ID

The host-qualified agent address `<host>.<identity>`.

Authority: [`AgentSpec::bus_id`](../../crates/agent-spec/src/spec.rs#L203-L211);
[Agent Spec bus identity](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#discovery-identity-and-host)

### catalog

The selected folder containing agent declarations and catalog-backed state.

Authority: [Agent Spec catalog boundary](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#discovery-identity-and-host);
[catalog selection](../../src/main.rs#L867-L927)

### catalog root

The filesystem path selected as the catalog for a command.

Authority: [catalog path resolution](../../src/main.rs#L867-L927)

### supervisor scope

The ownership boundary formed by one canonical catalog folder and one host. A
supervisor run owns policy decisions and recovery requests only within this
scope.

Authority: [R31 reachable restart bounds](requirements.md);
[F12 future policy](02-agent-spec/spec.md)

### root agent

The agent assigned host-local health observation, recovery, and escalation.

Authority: [R04 root supervision](requirements.md#L51-L54);
[vision](vision.md#L21-L22)

### control plane

The replaceable `st2 up` process that reconciles host-local work.

Authority: [R11 control-plane replacement safety](requirements.md#L83-L88);
[`up_loop`](../../src/run.rs#L1440-L1452)

### supervisor run

One live incarnation of the control plane supervising one supervisor scope. It
owns the restart accounting and park decisions made during that incarnation.

Authority: [R31 reachable restart bounds](requirements.md);
[F12 future policy](02-agent-spec/spec.md)

### declared supervisor

The agent reference carried by a declaration for supervisory routing.

Authority: [`AgentSpec::supervisor`](../../crates/agent-spec/src/spec.rs#L24-L50);
[Agent Spec field](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#complete-declaration-shape)

### presence

The agent-authored availability signal read from catalog-backed state.

Authority: [`status::State`](../../src/status.rs#L24-L45);
[R08 catalog observability](requirements.md#L92-L95)

### session state

st2's runtime observation of whether a task record is alive.

Authority: [`reconcile::Session`](../../src/reconcile.rs#L16-L26)

### observed harness state

The driver-written record of what a harness is seen doing: activity
(`idle`/`active`/`child`/`ended`, with `unknown` derived and never written),
who it is blocked on — and, when blocked on a human, what kind of ask holds
it (`permission`/`question`/`review`) — and what its input buffer holds. The observed
counterpart of the declared axes: it is not [presence](#presence) (agent-
authored availability), not [session state](#session-state) (task-record
liveness), not R08's *declared activity status*, and not R09's *working
state* (restored context).

Authority: [`harness_state`](../../src/harness_state.rs);
[05-harness-state requirements](05-harness-state/requirements.md);
[decision 0006](.decisions/0006-observed-harness-state-is-a-driver-written-catalog-record.md)

### native driver diagnostic

The current typed explanation that a native harness driver's boundary cannot
produce trustworthy evidence or complete transport work. It is advisory
evidence, not the provider's free-form error, not observed harness state, and
not authority to launch, retry, deliver, reconcile, or archive.

Authority: [05-harness-state requirements OHS-R11–OHS-R15](05-harness-state/requirements.md);
[`driver_diagnostic`](../../src/driver_diagnostic.rs)

### diagnostic stage

One closed native-driver boundary at which a diagnostic is observed:
version gate, API gate, event stream, state seed, delivery, or read-back. A
stage owns its bounded reasons and sources; a reason paired with another stage
is unknown evidence rather than a best-effort match.

Authority: [native driver diagnostic snapshot](05-harness-state/spec.md#native-driver-diagnostic-snapshot-ohs-r11ohs-r15)

### diagnostic evidence age

Reader-derived elapsed time since the current native driver diagnostic was
observed. The origin timestamp is durable; age is a projection and never file
mtime.

Authority: [native driver diagnostic snapshot](05-harness-state/spec.md#native-driver-diagnostic-snapshot-ohs-r11ohs-r15)
### Resource

An externally identified thing an agent points at, named by an absolute URI.
st2 preserves the URI's exact bytes and never normalizes them. The scheme is the
exact lookup key for an optional, catalog-declared
[Resource Profile](07-resource-profile/requirements.md); scheme meaning stays
downstream-owned and st2 ships no built-in profiles, so an unregistered scheme
stays opaque. Possession of a URI grants no authority, access, or capability.

One concept, one edge: a Resource is reached through a
[Resource binding](#resource-binding). The [linked record](#linked-record-retired)
plane that once shared the word is retired.

Authority: [R20 portable Resource bindings](requirements.md#L161-L168);
[issue #61 resolution](https://github.com/compoundingtech/st2/issues/61)

### Resource binding

A publisher-declared edge from one agent to one Resource, written as a
`resource` node in the agent declaration and carrying an agent-local unique
name, the URI, a required `reason`, and an optional `inactive-reason`. Bindings
are desired state: they change only through compare-and-swap publication of the
whole declaration, and a binding-only change never stops, replaces, or relaunches
healthy work.

A binding says what an agent *is for* — the work it reads and the durable state
carriers it owns. It is not a record of what the agent produced.

Authority: [`Resource`](../../crates/agent-spec/src/spec.rs#L239-L245) — the
live contract; [R20](requirements.md#L161-L168); [R21](requirements.md#L169-L172).
The canonical [Agent Spec Resource bindings](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#agent-spec-resource-bindings)
anchor still describes the pre-#307 envelope of name and `uri` only, and would
reject the required `reason`; it is pending sync (07-resource DQ-R8).

### linked record (retired)

An agent-owned record of something an agent produced, stored as one markdown
file with `url`, optional `title`/`tags`/`relation`, and an optional body under
`<agent-dir>/resources/links/`, written through `st2 resource add`.

**Retired.** The term is kept only so a reader who meets a surviving record
under `resources/links/` can identify it. Recording produced artifacts is
`axe work update --artifact <path> --pty <name>`. Nothing in st2 reads a linked
record, and *resource* now names only the declared plane.

Authority: [07-resource spec](07-resource/spec.md);
[decision 0008](.decisions/0008-the-linked-record-plane-is-retired.md)

### agent resource directory

The per-agent directory `<agent-dir>/resources/`, canonical for an agent's
resource files. It holds the message planes (`inbox/`, `archive/`, `sent/`),
working state and decisions (`context/`), scratch material (`tmp/`), and the
realized carriers themselves (`goal.md` and siblings).

It is not a separate meaning of *resource*. It is the **realization surface**
for [Resource bindings](#resource-binding): a binding names a carrier by URI and
the carrier's bytes live here. `dev.schickling.agent-goal://<host>/<identity>`
realizes as `resources/goal.md`; `decision-tree://<host>/<identity>` realizes as
`resources/context/decisions/`. Identity is the URI; the path is realization,
and st2 does not resolve one into the other ([R20](requirements.md#L161-L168)).

Authority: [`message::with_resolved_state_dir`](../../src/message.rs);
[07-resource spec](07-resource/spec.md)

### working state

An agent's restored durable context — what it is doing, what it decided, and what
it ruled out — written through `st2 context` and realized at
`resources/context/now.md`. Never a liveness or activity term; the observed
signal is [observed harness state](#observed-harness-state).

Addressed as a [Resource binding](#resource-binding) under the st2-owned scheme
`working-state://<host>/<identity>`.

Authority: [R09 state continuity](requirements.md#L131-L132);
[`context`](../../src/context.rs);
[decision 0009](.decisions/0009-working-state-is-a-declared-carrier.md)

### restart policy

The declared rules that bound when and how an agent task may be relaunched
after it stops.

Authority: [R31 reachable restart bounds](requirements.md);
[F12 future policy](02-agent-spec/spec.md)

### restart budget

The launches available to an agent task under its restart policy before the
policy delays or parks it.

Authority: [R31 reachable restart bounds](requirements.md);
[F12 future policy](02-agent-spec/spec.md)

### parked task

An agent task whose owning supervisor run has decided not to relaunch it after
its fail-mode restart budget was exhausted. Parking is a supervisor decision,
not a session state or runtime observation.

Authority: [R23 fail-closed task inventory and R31 reachable restart bounds](requirements.md)

### operator action

An explicit instruction from an operator that requests a named control-plane
change. It is distinct from an automatic policy reaction. A projected recovery
operator action is structured executable argv and carries its supervisor
scope's exact canonical catalog folder and selected host; those axes do not
come from ambient defaults at invocation time.

Authority: [R31 reachable restart bounds](requirements.md)

### unpark request

An operator action asking the owning supervisor run to clear one parked task's
park decision and restart accounting.

Authority: [R31 reachable restart bounds](requirements.md);
[F12 future policy](02-agent-spec/spec.md)

### agent desired state

The declaration-owned whole-agent lifecycle intent: `running`, `suspended`, or
`retired`. It is distinct from presence and session state.

Authority: [R27 typed agent desired state](requirements.md);
[`AgentDesiredState`](../../crates/agent-spec/src/spec.rs)

### suspension

Reversible desired absence of an agent's live tasks while its declaration and
catalog-backed durable state remain available. Suspension is not process pause
or checkpointing.

Authority: [R27 typed agent desired state](requirements.md);
[Agent Spec field rules](02-agent-spec/spec.md)

### retirement

Terminal desired absence whose completion additionally requires every declared
task record to be collected. Legacy `retired #true` is a readable spelling.

Authority: [R27 typed agent desired state](requirements.md);
[Doctor retired absence](02-doctor/requirements.md)

### desired-state rationale

The bounded human explanation required by a new suspended or retired desired
state. It explains intent and grants no lifecycle authority of its own.

Authority: [R27 typed agent desired state](requirements.md)

### reconciliation

Comparing declared host-local work with observed runtime state to produce a
plan.

Authority: [`reconcile`](../../src/reconcile.rs#L1-L10);
[reconcile pass](../../src/run.rs#L1007-L1017)

### materialization

Rendering catalog-declared content into an agent workspace.

Authority: [`materialize_catalog`](../../src/materialize.rs#L837-L850)

### message

A durable addressed record in an agent inbox or archive.

Authority: [`message::Message`](../../src/message.rs)

### sent message

One completed ordinary Agent send or reply represented by a sender-owned row. The selected sender
is implicit and the row's directional peer is `to`.

Authority: [MESSAGE-R01 and MESSAGE-R02](03-message/requirements.md);
[`SentMessageRow`](../../crates/st2-wire/src/message.rs)

### sender history

The durable sender-owned sequence enumerated by `message sent`. Recipient inboxes, archives, and
typed service-principal request state do not supply its rows.

Authority: [MESSAGE-R01 and MESSAGE-R11](03-message/requirements.md);
[message specification](03-message/spec.md);
[`list_sent`](../../src/message.rs)

### sender history coverage

The explicit proof boundary attached to sender history: `unavailable`, `since`, or `partial`. An
empty row sequence is a complete empty result only with `since` coverage.

Authority: [MESSAGE-R03](03-message/requirements.md);
[`SentCoverage`](../../crates/st2-wire/src/message.rs)

### sent commit ledger

The sender-owned immutable chain that commits each completed Sent row. One constant-size atomic head
names the chain tip and count; complete reads verify the chain to genesis and its exact reachable
sender-row set.

Authority: [MESSAGE-R03 and MESSAGE-R08](03-message/requirements.md);
[state ownership](03-message/spec.md#state-ownership)

### idempotency key

An optional caller-supplied operation identity scoped by canonical sender, canonical recipient, and
key. It makes an exact send or reply retry return the original filename; it is not inferred from
message content.

Authority: [MESSAGE-R07 and MESSAGE-R08](03-message/requirements.md);
[retry identity](03-message/spec.md#retry-identity);
[`send_to_resolved_inbox`](../../src/message.rs)

### DING

The terminal notification that makes an agent aware of unread messages.

Authority: [DING module contract](../../src/ding/mod.rs#L1-L14)

### stream

A named declared event producer feeding one agent. With a `command` or `argv`
adapter form, st2 supervises its adapter; with neither, it is an external
ingress endpoint.

Authority: [STREAM-R01 declared streams](04-stream/requirements.md);
[decision 0005](.decisions/0005-streams-are-agent-nested-and-stream-named.md)

### event

One durable inbox record produced on a stream. A fact observed in the world,
not addressed speech; a distinct record kind on the shared bus transport.

Authority: [STREAM-R04 and STREAM-R06](04-stream/requirements.md);
[decision 0004](.decisions/0004-stream-events-are-a-distinct-record-kind.md)

### event-id

The mandatory producer-supplied identity of one event, deduplicated per
`(stream, event-id)`.

Authority: [STREAM-R03 mandatory event identity](04-stream/requirements.md)

### key

The optional grouping axis within a stream. Supersession collapses unread
events per `(stream, key)`.

Authority: [STREAM-R07 producer-side supersession](04-stream/requirements.md)

### adapter

The world-specific program a stream runs, declared as either a shell `command`
or direct `argv`. Packaged outside st2; it emits through `st2 event emit`.

Authority: [STREAM-A03 world logic stays outside](04-stream/requirements.md)

### stream task

The derived exec companion supervising a stream's adapter, under the same
generated-companion lifecycle as the derived DING.

Authority: [STREAM-R08 companion lifecycle](04-stream/requirements.md)

## Structure

```text
supervisor scope
`-- supervisor run
    `-- applies restart policy to agent task
        `-- spends restart budget
            `-- may decide parked task

operator action --creates--> unpark request --targets--> owning supervisor run
```

`supervisor` is the leitwort for the ownership family. `restart` is the
leitwort for policy and budget; `park` links the terminal decision to its
explicit `unpark` recovery request.

```text
native driver boundary --publishes/clears--> native driver diagnostic
       |                                      |
       `-- diagnostic stage                   `-- derives diagnostic evidence age
```

`diagnostic` is the leitwort for typed driver degradation; `observed` remains
the leitwort for harness activity evidence.

## Collision rules

- Qualify **root** as [root agent](requirements.md#L51-L54) or
  [catalog root](../../src/main.rs#L867-L927). Bare *root* does not identify
  which concept is meant.
- Qualify **supervisor** as [control plane](requirements.md#L83-L88) or
  [declared supervisor](../../crates/agent-spec/src/spec.rs#L24-L50).
- Use **supervisor run** for one live control-plane incarnation and
  **declared supervisor** for the agent reference used in supervisory routing.
  **Control plane** names the replaceable process concept, not an owning run.
- Use [presence](../../src/status.rs#L24-L45) for the agent-authored signal and
  [session state](../../src/reconcile.rs#L16-L26) for runtime liveness. Avoid
  bare *agent status* when either could be meant.
- Use [observed harness state](05-harness-state/requirements.md) for the
  driver-observed activity signal. It is a third axis beside presence and
  session state: R08's *activity status* stays the declared, agent-authored
  signal, and neither axis rewrites the other. Bare *activity* does not
  identify which is meant.
- *Working state* remains R09's restored durable context and is never a
  liveness or activity term; the observed signal is **observed harness
  state**, not *working state*.
- Use **native driver diagnostic** for typed boundary degradation and
  **observed harness state** for the harness activity projection. Neither is
  presence or session state, and a driver diagnostic never rewrites the
  observed tuple.
- Use **diagnostic stage**, **reason**, and **source** for the closed fields.
  Provider error prose may be logged, but must not become consumer branching
  vocabulary.
- Use **parked task** or **park decision** for the owning supervisor's policy
  decision. Do not use *parked* as a session state or replace the runtime
  observation with it.
- Use **recovery action** for the structured argv projected with a parked task;
  it names the exact supervisor scope rather than relying on ambient defaults.
- Use [agent identity](../../crates/agent-spec/src/spec.rs#L24-L50) for the bare
  value and [bus ID](../../crates/agent-spec/src/spec.rs#L203-L211) for the
  host-qualified address.
- **Resource** names one concept: a [Resource binding](#resource-binding) and
  nothing else. The [linked record](#linked-record-retired) plane that once shared the
  word is retired. Do not reintroduce a second sense.
- The [agent resource directory](#agent-resource-directory) is not a second
  sense either — it is where bindings are realized. Say *binding* for the
  declared edge and *carrier* for the realized bytes when both are in view.
- Use [working state](#working-state) for R09's restored durable context. The
  verb is `st2 context` and the directory is `resources/context/`, but the
  canonical term and its scheme are *working state*, not *context*.
- Use [message](../../src/message.rs#L26-L46) for the durable record and
  [DING](../../src/ding/mod.rs#L1-L14) for its terminal notification.
- Qualify **event**: a bare *event* in stream context is the durable
  [event](04-stream/requirements.md) record; the R13–R15 filesystem-watcher
  usage is a **watcher event**. New requirements text keeps the qualification.
- Use [key](04-stream/requirements.md) for the event-side grouping axis. The
  message-side thread/topic axis explored in
  [issue #49](https://github.com/compoundingtech/st2/issues/49) is a separate
  concept; do not merge the two by name.
- Use [stream](04-stream/requirements.md) for the declared producer and
  **stream task** for its derived companion. A *watcher stream* (R15) is
  watcher machinery, not a declared stream.

## Avoid

**seat** is not canonical st2 language. Choose by meaning: [agent](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#canonical-agent-specification)
for the actor, [agent declaration](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#complete-declaration-shape)
for the catalog entry, and [agent runtime](vision.md#L16-L24) for a running
instance. For admission, use an already-authoritative admission term only if
one exists; this index does not canonize one.

The rejected word remains visible as design leakage in
[issue #52](https://github.com/compoundingtech/st2/issues/52),
[draft PR #55](https://github.com/compoundingtech/st2/pull/55), and one
[Agent Spec phrase](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md#free-authoring-gate).
Those references are evidence of recurrence, not authorities for a new st2
concept.
