# st3 plan graph runtime

This document describes the implemented st3 plan model on 2026-08-27.

The earlier idea and feedback files remain design snapshots. This file records current behavior.

Every authored st3 KDL document starts with `version 2`. st3 rejects a missing version because it means st2 version zero.

st2 accepts version zero or one. This rule lets tools select the correct runtime before they interpret the document body.

## Core model

A `plan` is a stored definition. A plan does not start when st3 publishes it.

A plan has an explicit `draft`, `ready`, or `retired` state. Only a ready revision can start.

A plan run binds to one immutable plan revision. A revision hash comes from the normalized plan definition.

A `step` is one durable work unit in a plan run. Source order controls display only.

A step without `depends-on` can start immediately. Dependencies form a directed graph and can run concurrently.

The authored st3 grammar does not accept the old `checkpoints` form. Use `st3-migrate` before publication.

## Example

```kdl
version 2

subgraph {
  scope "signal-rename/${PLAN_RUN}"
    retention="temporary"
    change-policy="agent" {

    plan "signal-rename" state="ready" {
      step "start-workers" {
        title "The workers are ready"

        subgraph {
          agent "sig.base" {
            workspace "${WORKSPACE}/base"
            harness "codex" {
              model "gpt-5.6-sol"
              effort "medium"
              prompt "Claim assigned st3 work and report progress."
            }
          }
        }
      }

      step "rename-base" timeout="20m" {
        title "Rename the base package"
        goal "Rename the product and preserve runtime primitives."
        assigned-to "agent/sig.base"

        depends-on {
          step "start-workers" completed
        }

        plan "work" {
          step "inspect" { }
          step "change" { depends-on { step "inspect" completed } }
          step "test" { depends-on { step "change" completed } }
        }

        produces {
          resource "plan-run/${PLAN_RUN}/base-change" {
            kind "vcs.revision"
            state "published"
          }
        }
      }

      step "integrate" {
        depends-on {
          step "rename-base" completed
          step "update-config" completed
        }
      }

      step "nathan-approves" {
        depends-on { step "integrate" completed }
        judges { human "person/nathan" }
      }

      step "cleanup" finally=#true {
        subgraph { scope "signal-rename/${PLAN_RUN}" { stop } }
        judges { empty "scope/signal-rename/${PLAN_RUN}" }
      }
    }
  }
}
```

## Step behavior

`depends-on` controls access to a step. It can contain step states or graph predicates.

```kdl
depends-on {
  step "build" completed
  step "test" terminal
  field "decision" "resource/review" "is" "approved"
}
```

Predicate dependencies latch after they pass. A later graph change does not move an active step backwards.

`subgraph` declares desired runtime state. st3 materializes the subgraph and waits for convergence.

`assigned-to` makes the step durable work for one declared agent. The step blocks when the agent is not in the desired graph.

The native harness polls the durable work queue. st3 creates one idempotent Small Talk message when an assigned parent becomes ready.

Inherited nested steps use the parent message. A nested step with a different assignee gets its own message.

The native driver transports graph messages to the harness. It does not create a second message source.

A work claim acknowledges and closes its Small Talk message. The durable work state remains the source of truth.

`produces` declares a required graph shape. A worker report enters `verifying` until each product and judge passes.

`judges` supports graph predicates, mechanical judges, LLM judges, and exact step-run human reviews.

`finally=#true` selects cleanup work after normal success, failure, or cancellation. A plan run waits for final steps.

`retry` controls the attempt count and backoff boundary.

```kdl
retry {
  attempts 3
  backoff "30s"
}
```

## Nested plans

A nested plan is part of the submitted revision. Its first eligible steps start with the parent step.

Nested steps inherit the parent assignment unless a child overrides it. Their state stays in st3, not in harness memory.

The current grammar does not implement `uses-plan` or `produces-plan`. A nested authored plan is supported.

## Meta variables

st3 expands these variables when it creates a run or materializes a step:

| Variable | Value |
| --- | --- |
| `${PLAN}` | The plan ID. |
| `${PLAN_REVISION}` | The active revision hash. |
| `${PLAN_RUN}` | The plan run ID. |
| `${ROOT_PLAN_RUN}` | The root `plan-run/...` subject. |
| `${RUN_SCOPE}` | The expanded run scope, when present. |
| `${WORKSPACE}` | The run workspace. |
| `${STEP}` | The step path. |
| `${STEP_RUN}` | The exact `step-run/...` subject. |
| `${PARENT_STEP_RUN}` | The parent step-run subject. |
| `${ATTEMPT}` | The current attempt number. |
| `${ASSIGNEE}` | The normalized assignee subject. |
| `${REQUESTER}` | The normalized requester subject. |

An unknown variable makes the plan invalid.

Graph subject IDs are global. Use `${PLAN_RUN}` in a message or resource ID when each run needs a new subject.

## Work commands

The work API uses a ten-minute lease. A native harness renews its lease once each minute.

An active lease binds to the agent identity and its supplied incarnation. Another incarnation cannot update that work.

```sh
st3 work ls --as agent/sig.base
st3 work show step-run/RUN/rename-base
st3 work claim step-run/RUN/rename-base --as agent/sig.base
st3 work progress step-run/RUN/rename-base --summary "The base tests pass."
st3 work complete step-run/RUN/rename-base --summary "The revision is published."
st3 work fail step-run/RUN/rename-base --reason "The package cannot compile."
st3 work release step-run/RUN/rename-base --reason "The agent must hand off this work."
```

A completion report is not a correctness result. The declared products and judges decide final completion.

## Revisions and review

The default scope change policy is `agent`. An assignee can revise only its assigned step subtree.

`supervisor` and `human-review` policies require a `change-authority`. The actor must match that authority.

```sh
st3 work revise PLAN_RUN replacement.kdl \
  --as agent/sig.base \
  --reason "The package also contains a generated protocol table."
```

An active run adopts an accepted revision. Changed steps and their dependents reset.

Unchanged independent steps keep their completion. The original root revision remains in the run record.

A human decision binds to the exact step-run subject.

```sh
st3 review approve step-run/RUN/nathan-approves --actor person/nathan
st3 review revise step-run/RUN/rename-base --actor person/nathan \
  --reason "The plan must preserve one more protocol name."
```

A `revise` decision blocks only the selected step. The replacement plan uses the normal revision command.

These policies are workflow rules. st3 does not authenticate peer identity yet.

st3 is not secure by default. TLS, authenticated identity, and ACLs remain future work.

st3 has no Fabric-specific behavior. A deployment can use Fabric, Tailscale, another VPN, or a VPC.

## Documents

Post document bytes before a plan refers to them.

```sh
st3 doc put ./task.md --as doc/evals/signal-rename/task
```

The command prints `doc/NAME@SHA256`. Put that exact name and hash in the KDL.

Publication refuses when the stored binding changed after planning. Older immutable versions remain valid.

## Main commands

```sh
st3 up
st3 plan plan.kdl
st3 run plan.kdl
st3 run plan.kdl --plan signal-rename --workspace ./workspace
st3 import ./catalog
st3 eval ./evals/st3/signal-rename
```

`st3 run` prints a live plan and step tree. `--detach` returns the durable run record instead.

The st3 evals are in `evals/st3/license-mit`, `evals/st3/ghost-bug`, and `evals/st3/signal-rename`.

Their st2 counterparts use the same names under `evals/st2`.
