# Resource subscriptions

Status: current design.

## Outcome

An agent or a person can request a message when selected facts about an external resource change.

The request is durable graph state. A supervised observer checks the external resource without using an agent turn.

The first provider for this design observes a GitHub pull request. The graph model does not depend on GitHub.

## Agent command

This command creates one watch operation:

```sh
st3 resource watch github.pull-request compoundingtech/st2#403 \
  --on head --on state --on review --on checks
```

`ST_AGENT` supplies the delivery target. A person can use `--to agent/HOST.IDENTITY`.

The command returns the resource, observer, and subscription subjects. The operation creates all three subjects atomically.

The subscription key includes the provider kind, provider locator, selected fields, target, and delivery type. An exact retry returns the same subjects.

## Graph shape

The command publishes this graph shape:

```kdl
resource "github/compoundingtech/st2/pull/403" {
  kind "vcs.pull-request"
  binding "late"
}

observer "github/compoundingtech/st2/pull/403" {
  resource "resource/github/compoundingtech/st2/pull/403"
  provider "github.pull-request"
  locator "compoundingtech/st2#403"
  field "head"
  field "state"
  field "review"
  field "checks"
}

subscription "github/compoundingtech/st2/pull/403/hetz.st2/7bf411a0" {
  observer "observer/github/compoundingtech/st2/pull/403"
  to "agent/hetz.st2"
  on "head"
  on "state"
  on "review"
  on "checks"
  delivery "message"
}
```

The resource stores normalized external facts. The observer stores observation intent and health.

The subscription stores the selected changes and delivery intent. The agent declaration does not change.

A provider locator is an opaque provider value. st3 does not assign meaning to it outside the registered provider.

## Provider contract

A registered provider converts one locator into normalized resource fields.

The provider returns an unchanged result or one complete observation. A partial response cannot replace the last good observation.

The provider can use a webhook, a stream, or a conditional request. A conditional provider returns one next-check deadline.

The daemon records that deadline as a one-shot wake. It does not run a periodic discovery sweep.

Conditional requests use provider cursors such as an ETag. Cursors are local progress state, not resource facts.

The provider applies bounded retries and backoff. It records authentication, rate-limit, and transport failures on the observer subject.

One observer serves all subscriptions for the same provider locator. Its field set is the union of their selected fields.

st3 does not fetch once for each target. A subscription update can expand or reduce the observer field set.

## Change and delivery rules

The first successful observation establishes the baseline. It sends no update message.

A later observed field change creates one `resource.observed` claim. An unchanged observation creates no resource claim.

Each subscription that selected a changed field creates one message. Its stable key uses the observation claim and subscription subject.

A daemon restart can repeat an external request. It cannot create a duplicate observation or message.

The normalized observation is the resource authority. A raw provider response can be immutable evidence, but it cannot accept a separate resource mutation.

A missing delivery target creates a warning and keeps the subscription pending. It does not block the observer or another subscription.

## Lifecycle

`st3 resource unwatch SUBSCRIPTION` stops one subscription. It does not remove the resource or other subscriptions.

The observer stops after its last subscription stops unless another graph relation keeps it active.

The GitHub pull request provider observes the final merge or closure change. The subscription remains active until an explicit unwatch in the MVP.

A later version can add an `until` predicate or one deadline. This option is not part of the MVP.

## Acceptance proof

- An exact command retry creates no duplicate graph subject.
- The first observation creates a baseline and no message.
- An unchanged provider result creates no claim and no message.
- A selected field change creates one observation claim and one message.
- An unselected field change creates an observation claim and no message for that subscription.
- A daemon restart creates no duplicate message.
- Two subscriptions share one observer and receive separate messages.
- A missing target stays pending without blocking observation.
- A provider failure changes observer health without changing the last good resource facts.
- A second provider passes the same contract without a GitHub-specific graph field.
