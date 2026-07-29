# st2 language

This is a small disambiguation index, not a behavioral specification. Each
canonical term links to the source that owns its meaning. Follow that source
for behavior, lifecycle, ordering, and wire or file formats.

st2 inherits agent-authoring language from the
[canonical Agent Spec](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L1-L14).
Terms without an independent authority are deliberately absent.

## Canonical language

### agent

The actor st2 models.

Authority: [Agent Spec overview](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L1-L10);
[R10 agent-only identity](requirements.md#L80-L81)

### agent declaration

The authored KDL representation of one agent.

Authority: [Agent Spec discovery and declaration shape](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L16-L31);
[R02 canonical KDL](requirements.md#L36-L38)

### agent runtime

A running instance of an agent declaration.

Authority: [declared runtime vision](vision.md#L16-L24);
[`AgentSpec` runtime model](../../crates/agent-spec/src/spec.rs#L1-L13)

### agent task

A terminal-backed or terminal-free unit declared for an agent.

Authority: [Agent Spec task contract](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L103-L120);
[`Task`](../../crates/agent-spec/src/spec.rs#L58-L72)

### agent identity

The bare `identity` value of an agent declaration. It is not a claim of
fleet-wide uniqueness.

Authority: [`AgentSpec::identity`](../../crates/agent-spec/src/spec.rs#L23-L40);
[Agent Spec identity rules](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L24-L31)

### bus ID

The host-qualified agent address `<host>.<identity>`.

Authority: [`AgentSpec::bus_id`](../../crates/agent-spec/src/spec.rs#L123-L128);
[Agent Spec bus identity](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L29-L31)

### catalog

The selected folder containing agent declarations and catalog-backed state.

Authority: [Agent Spec catalog boundary](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L29-L34);
[catalog selection](../../src/main.rs#L848-L907)

### catalog root

The filesystem path selected as the catalog for a command.

Authority: [catalog path resolution](../../src/main.rs#L848-L907)

### root agent

The agent assigned host-local health observation, recovery, and escalation.

Authority: [R04 root supervision](requirements.md#L41-L46);
[vision](vision.md#L20-L23)

### control plane

The replaceable `st2 up` process that reconciles host-local work.

Authority: [R11 control-plane replacement safety](requirements.md#L64-L71);
[`up_loop`](../../src/run.rs#L1079-L1111)

### declared supervisor

The agent reference carried by a declaration for supervisory routing.

Authority: [`AgentSpec::supervisor`](../../crates/agent-spec/src/spec.rs#L23-L40);
[Agent Spec field](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L73-L90)

### presence

The agent-authored availability signal read from catalog-backed state.

Authority: [`status::State`](../../src/status.rs#L19-L40);
[R08 catalog observability](requirements.md#L72-L78)

### session state

st2's runtime observation of whether a task record is alive.

Authority: [`reconcile::Session`](../../src/reconcile.rs#L18-L25)

### reconciliation

Comparing declared host-local work with observed runtime state to produce a
plan.

Authority: [`reconcile`](../../src/reconcile.rs#L1-L10);
[reconcile pass](../../src/run.rs#L736-L810)

### materialization

Rendering catalog-declared content into an agent workspace.

Authority: [`materialize_catalog`](../../src/materialize.rs#L700-L727)

### message

A durable addressed record in an agent inbox or archive.

Authority: [`message::Message`](../../src/message.rs#L1-L35)

### DING

The terminal notification that makes an agent aware of unread messages.

Authority: [DING module contract](../../src/ding/mod.rs#L1-L14)

## Collision rules

- Qualify **root** as [root agent](requirements.md#L41-L46) or
  [catalog root](../../src/main.rs#L848-L907). Bare *root* does not identify
  which concept is meant.
- Qualify **supervisor** as [control plane](requirements.md#L64-L71) or
  [declared supervisor](../../crates/agent-spec/src/spec.rs#L23-L40).
- Use [presence](../../src/status.rs#L19-L40) for the agent-authored signal and
  [session state](../../src/reconcile.rs#L18-L25) for runtime liveness. Avoid
  bare *agent status* when either could be meant.
- Use [agent identity](../../crates/agent-spec/src/spec.rs#L23-L40) for the bare
  value and [bus ID](../../crates/agent-spec/src/spec.rs#L123-L128) for the
  host-qualified address.
- Use [message](../../src/message.rs#L1-L35) for the durable record and
  [DING](../../src/ding/mod.rs#L1-L14) for its terminal notification.

## Avoid

**seat** is not canonical st2 language. Choose by meaning: [agent](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L1-L10)
for the actor, [agent declaration](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L16-L31)
for the catalog entry, and [agent runtime](vision.md#L16-L24) for a running
instance. For admission, use an already-authoritative admission term only if
one exists; this index does not canonize one.

The rejected word remains visible as design leakage in
[issue #52](https://github.com/compoundingtech/st2/issues/52),
[draft PR #55](https://github.com/compoundingtech/st2/pull/55), and one
[Agent Spec phrase](https://github.com/compoundingtech/evals/blob/78210568e47244d80de99c18d0eea2d6b641c18a/AGENT-SPEC.md#L422-L429).
Those references are evidence of recurrence, not authorities for a new st2
concept.
