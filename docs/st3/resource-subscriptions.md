# Resource subscriptions

Status: current design.

## Outcome

An agent or a person can request a message when selected facts about an external resource change.

The request is durable graph state. A supervised observer checks the external resource without using an agent turn.

The first provider observes a GitHub pull request. The second provider observes one local file.

The GitHub provider supports `head`, `state`, `review`, and `checks`.

The local file provider supports `status`, `path`, `content_hash`, `size`, `mode`, and `reason`. It never returns file content.

## Agent command

This command creates one watch operation:

```sh
st3 resource watch github.pull-request compoundingtech/st2#403 \
  --on head --on state --on review --on checks
```

`ST_AGENT` supplies the delivery target. A person can use `--to agent/HOST.IDENTITY`.

The command returns the resource, observer, and subscription subjects. It also creates one standing watch plan run.

The subscription key includes the provider kind, provider locator, selected fields, target, and delivery type. An exact retry returns the same subjects.

## Graph shape

The command publishes this graph shape:

```kdl
resource "github/compoundingtech/st2/pull/403" {
  kind "vcs.pull-request"
}

plan "resource-watch/github/compoundingtech/st2/pull/403/KEY" state="ready" {
  goal "Observe one resource and send its selected changes."
  observer "watch" {
    resource "resource/github/compoundingtech/st2/pull/403"
    provider "github.pull-request"
    locator "compoundingtech/st2#403"
    field "head"
    field "state"
    field "review"
    field "checks"
  }

  subscription "watch" {
    observer "observer/watch"
    to "agent/example.worker"
    on "head"
    on "state"
    on "review"
    on "checks"
    delivery "message"
  }
}
```

The resource begins unbound. The resource stores normalized external facts after its first observation.

The plan run owns the observer and subscription.

The returned subjects use `observer/RUN/watch` and `subscription/RUN/watch`.

The subscription stores the selected changes and delivery intent. The agent declaration does not change.

A provider locator is an opaque provider value. st3 does not assign meaning to it outside the registered provider.

The GitHub provider reads `GH_TOKEN` first and `GITHUB_TOKEN` second. It uses public API access when both values are absent.

## Observation without delivery

An observer does not need a subscription. This form records one resource without sending a message:

```kdl
resource "workspace/config" {
  kind "filesystem.file"
}

plan "observe-config" state="ready" {
  goal "Keep the configuration metadata current."
  observer "config" {
    resource "resource/workspace/config"
    provider "local.file"
    locator "/work/project/config.toml"
    field "status"
    field "content_hash"
    field "size"
    field "mode"
  }
}
```

The plan run owns the observer. Plan cancellation stops the observer.

Use this command to request an immediate observation and wait for that exact attempt:

```sh
st3 resource refresh resource/workspace/config --timeout 30s
```

The command returns `changed=false` when the provider confirms the same facts. That success adds no resource claim.

## Provider contract

A registered provider converts one locator into normalized resource fields.

The provider returns an unchanged result or one complete observation. A partial response cannot replace the last good observation.

The provider can use a webhook, a stream, or a conditional request. A conditional provider returns one next-check deadline.

The daemon records that deadline as a one-shot wake. It does not run a periodic discovery sweep.

Conditional requests use provider cursors such as an ETag. Cursors are local progress state, not resource facts.

The provider applies bounded retries and backoff. It records authentication, rate-limit, and transport failures on the observer subject.

An observer checks its declared fields. Its effective field set also includes the union of its subscription fields.

st3 does not fetch once for each target. A subscription update can expand or reduce the observer field set.

## Change and delivery rules

The first successful observation establishes the baseline. It sends no update message.

A later observed field change creates one `resource.observed` claim. An unchanged observation creates no resource claim.

Each subscription that selected a changed field creates one message. Its stable key uses the observation claim and subscription subject.

A native harness driver can deliver that message. Another harness can use an explicit `st3 driver ding` child owned by the same plan run.

A daemon restart can repeat an external request. It cannot create a duplicate observation or message.

The normalized observation is the resource authority. A raw provider response can be immutable evidence, but it cannot accept a separate resource mutation.

A missing delivery target creates a warning and keeps the subscription pending. It does not block the observer or another subscription.

The subscription becomes active when its delivery target appears.

## Lifecycle

`st3 resource unwatch SUBSCRIPTION` cancels its watch plan run. It does not remove the resource or other watch runs.

Cleanup stops the owned observer and subscription before the watch run becomes cancelled.

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
- The local file provider reports metadata and never reports file content.
- A direct observer without a subscription records observations and sends no message.
- A manual refresh waits for its exact attempt and reports an unchanged success without a new resource claim.
