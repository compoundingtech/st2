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

### root agent

The agent assigned host-local health observation, recovery, and escalation.

Authority: [R04 root supervision](requirements.md#L51-L54);
[vision](vision.md#L21-L22)

### control plane

The replaceable `st2 up` process that reconciles host-local work.

Authority: [R11 control-plane replacement safety](requirements.md#L83-L88);
[`up_loop`](../../src/run.rs#L1440-L1452)

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

Authority: [`message::Message`](../../src/message.rs#L26-L46)

### DING

The delivery signal that makes an agent aware of unread messages. A declaration
selects either the legacy terminal transport or one provider-native control
transport.

Authority: [DING specification](./01-ding/spec.md) and
[DING module contract](../../src/ding/mod.rs#L1-L14)

## Collision rules

- Qualify **root** as [root agent](requirements.md#L51-L54) or
  [catalog root](../../src/main.rs#L867-L927). Bare *root* does not identify
  which concept is meant.
- Qualify **supervisor** as [control plane](requirements.md#L83-L88) or
  [declared supervisor](../../crates/agent-spec/src/spec.rs#L24-L50).
- Use [presence](../../src/status.rs#L24-L45) for the agent-authored signal and
  [session state](../../src/reconcile.rs#L16-L26) for runtime liveness. Avoid
  bare *agent status* when either could be meant.
- Use [agent identity](../../crates/agent-spec/src/spec.rs#L24-L50) for the bare
  value and [bus ID](../../crates/agent-spec/src/spec.rs#L203-L211) for the
  host-qualified address.
- Use [message](../../src/message.rs#L26-L46) for the durable record and
  [DING](./01-ding/spec.md) for its delivery signal.

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
