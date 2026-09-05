# Account usage reactions

Status: architecture proposal only.

## Requirement

An agent declares the model account that it uses.

An observer refreshes account usage on the provider schedule. A ten-minute interval is a reasonable default for a conditional provider.

A declared threshold can change the graph before the account reaches its limit. The change can notify a person, stop selected agents, or select another account.

This behavior must not require an agent turn.

## Current gap

st3 already has observers, subscriptions, and runtime actions. These parts do not form an account protection loop.

An observer records normalized resource facts. A subscription can create a message when selected facts change.

The current `harness.usage` claim records usage for one agent. It does not describe the shared account limit.

The reconciler starts and stops runtimes from desired graph state. No declaration converts an observed threshold into new desired graph state.

Therefore, st3 can record account usage but cannot act on it.

## Account resource contract

Add one built-in `harness.account` resource kind. Each agent can reference one account resource in its declaration.

The reference identifies capacity ownership. It does not give the agent access to account credentials.

The normalized account facts must carry:

- the provider name;
- an opaque, stable account identifier;
- the observation status and observation time;
- one or more named usage windows;
- the used percentage for each window;
- the reset time for each window, when the provider supplies it;
- the provider limit status, such as `ready`, `warning`, `limited`, or `unknown`.

The account identifier must not contain an email address, credential, or provider token. Credentials remain in provider configuration outside the claims store.

Each usage window needs a stable name. Examples include `five-hour`, `weekly`, and `monthly`.

The observer writes the complete normalized fact set in one `resource.observed` claim. A partial provider response cannot replace the last good facts.

The observer records failures on its own subject. A stale or failed observation cannot silently clear an active protection action.

The agent declaration also needs a protection class or explicit policy membership. st3 must not infer business importance from agent activity.

## Condition-to-subgraph action

Add a plan-owned reaction declaration. A reaction observes typed resource fields and selects one declared subgraph branch.

This example is a syntax sketch, not a final KDL contract:

```kdl
plan "chat/low-priority" state="ready" {
  goal "Keep one assistant available while account capacity permits it."

  subgraph {
    agent "assistant" {
      account "resource/accounts/openai-primary"
      protection-class "discretionary"
    }

    reaction "protect-primary-account" {
      when {
        field "resource/accounts/openai-primary/usage/weekly/used-percent" gte=90
      }

      then {
        subgraph {
          agent "assistant" { stop }
          message "account-warning" {
            to "person/nathan"
            content "The primary account reached 90 percent usage."
          }
        }
      }
    }
  }
}
```

The reaction reducer evaluates only committed observations. It does not call a provider or run shell commands.

The reducer records each branch change as an immutable claim. That claim cites the observation and reaction revision as evidence.

The selected branch becomes a desired-state overlay. The normal reconciler then computes and performs the required runtime actions.

The reaction does not call a runtime driver directly. This rule keeps preview, authorization, idempotency, and recovery in the existing graph path.

An exact reevaluation must return the existing branch-selection claim. A stale observation cannot create a second action.

The false branch removes the overlay and reveals the base desired state. This behavior lets a fresh post-reset observation resume protected agents.

The reaction does not cancel its plan run. A cancellation is permanent and cannot provide automatic recovery after an account reset.

The declaration should support separate enter and leave thresholds. This hysteresis prevents repeated changes near one percentage boundary.

A reaction should first target only its owning plan run. This limit preserves the current ownership and runtime-stop boundaries.

One fleet plan can own many protected agents. Separate plans can each observe the shared account and apply their own declared reaction.

A later design can permit cross-plan changes through an explicit capability. That apply transaction must use expected subject heads.

The first version should use explicit targets. A later selector can choose agents by account reference and protection class.

## Reconciliation position

The account observer keeps its existing one-shot deadline and provider cursor.

After `resource.observed` commits, the daemon wakes the normal reducer. The reducer evaluates affected reactions before it produces the desired snapshot.

If a branch changes, the store commits the selection claim and its desired overlay atomically. The reconciler then receives the new snapshot.

Messages still use the existing delivery lifecycle. Runtime changes still use the existing request, result, deadline, and incarnation fences.

Replica import runs the same pure reaction reducer. It cannot repeat an external action because the branch claim and runtime fences are deterministic.

## Existing parts that remain sufficient

This design does not need a second polling service. The resource observer already supports provider deadlines and normalized facts.

It does not need an account usage database. Immutable resource observations and projections already supply history and current state.

It does not need a new message system. Existing subscriptions and message claims can notify Nathan when a branch changes.

It does not need new process control. Existing desired-state reconciliation can stop, start, or replace an agent runtime.

It does not need an agent planning turn. The reaction reducer is deterministic system behavior.

It does not need credentials in the graph. The provider adapter keeps credentials outside the claims store.

It does not need a special plan type. A reaction is durable plan-owned graph state.

## Decisions before implementation

Nathan must approve the account field names, the reaction KDL surface, and the cross-plan capability boundary.

The first implementation should prove one account observer, one threshold, one notification, one stop action, and one automatic recovery after reset.
