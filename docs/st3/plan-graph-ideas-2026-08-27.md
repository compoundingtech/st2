# st3 plan graph ideas snapshot

This document records a design conversation from 2026-08-27.

These ideas are not the current st3 specification.

The examples show the intended shape. The current parser does not accept this proposed grammar.

This snapshot stays separate from later feedback. It gives us a stable point for comparison.

## Goals

st3 should store a complete plan before agents start normal work whenever possible.

The graph should retain plan state when an agent, a harness, or the st3 daemon restarts.

The plan should support concurrent branches, joins, human gates, and nested agent work.

An operator should see each agent's current work without opening each agent TUI.

The graph should store useful progress and evidence. It should not store private chain-of-thought.

Plan publication and plan execution must be separate operations.

An imported catalog must not start every stored plan.

## Proposed authored vocabulary

The proposed authored model uses `plan` and `step`.

It does not expose `checkpoints`, `checkpoint`, `plan`, and `step` as four competing terms.

The engine can retain an internal checkpoint concept if it is useful. Authors should not need it.

An st2 migration tool should translate old checkpoint KDL before publication. st3 should not accept old KDL through an adapter.

The design ignores the old `team` block. A scope and explicit graph relations provide the required structure.

## Harness declarations

An agent uses one contextual `harness` child.

The harness name is data. A new harness does not consume another reserved KDL node name.

```kdl
agent "sig.base" {
  workspace "${EVAL_ROOT}/base"

  harness "codex" {
    model "gpt-5.6-sol"
    effort "medium"
    args "--dangerously-bypass-approvals-and-sandbox"
  }
}
```

The same form supports other harnesses:

```kdl
harness "claude" { }
harness "pi" { }
harness "opencode" { }
harness "company-internal" { }
```

A normal managed agent should have exactly one harness. An adopt-only agent can omit the harness.

This shape should also be backported to st2. st2 should retain its Claude marketplace installation and fallback behavior.

## Plan definitions and plan runs

A plan definition describes possible work. A plan run is one execution of an exact definition revision.

A definition can have these states:

- `draft` is visible and editable, but it cannot execute.
- `ready` is frozen and available for a run or `uses-plan`.
- `retired` cannot create a new run.

Saving a draft can create a new content-addressed revision. The named draft head can move between those revisions.

A ready revision is immutable. A plan run binds to its exact revision hash.

A ready plan does not start because it exists. It can remain unused for any period.

A plan run can have these derived states:

- `pending`
- `running`
- `blocked`
- `completed`
- `failed`
- `cancelled`

The product can offer three separate actions:

1. Save a draft.
2. Publish a ready revision.
3. Start a run.

A planning TUI can combine the last two actions behind a single **Submit plan** action.

`st3 import` should store definitions and desired graph state. It should not create plan runs.

## Step dependencies

`depends-on` defines execution order.

A step with no `depends-on` becomes ready when its plan run becomes active.

The model does not need a special `init` dependency.

Source order controls presentation only. Source order never controls execution.

Multiple dependencies form an all-of join:

```kdl
step "integrate" {
  depends-on "rename-base" "rename-support"
}
```

Both branches must complete before `integrate` becomes ready.

This model supports parallel work inside one plan. The current ordered checkpoint sequence does not.

## Desired state and automatic convergence

A step can contain a `subgraph` with desired runtime state.

The reconciler must first materialize that subgraph. The step can then evaluate its other completion conditions.

The engine already knows when these common targets hold:

- An agent holds after it reports `ready`, `working`, or `idle`.
- A message holds after delivery.
- A stopped member holds after it is not live.
- A stopped scope holds after its recorded actual member set is empty.

The plan model should use this knowledge as an automatic convergence condition.

Authors should not need a redundant judge such as this one:

```kdl
field "status" "agent/sig.base" is "ready"
```

A judge remains useful when convergence cannot prove the required result.

## Step completion

A step completes when all of its present conditions hold.

The conditions are:

1. Every dependency completed.
2. The step subgraph converged, if it has a subgraph.
3. The assigned worker reported completion, if it has an assignment.
4. The nested plan completed, if it has a nested plan.
5. Every judge passed, if it has judges.

A subgraph-only step completes after convergence.

An assigned step waits for the worker's completion evidence.

A judges-only step waits for its judges.

A human gate is a judges-only step with a human judge.

## Nested plans

An assigned step can contain a nested plan.

The complete nested plan should normally be present in the submitted top-level revision.

The assigned agent should not publish its initial plan after it starts normal work.

The nested plan belongs directly under the step. It does not belong inside `subgraph`.

`subgraph` describes desired runtime state. A nested plan describes the work structure.

```kdl
step "rename-base" {
  depends-on "start-workers"
  assigned-to "agent/sig.base"
  goal "Rename the base package and preserve runtime primitives."

  plan "work" {
    step "inspect-the-current-package" { }

    step "identify-the-product-names" {
      depends-on "inspect-the-current-package"
    }

    step "change-the-base-package" {
      depends-on "identify-the-product-names"
    }

    step "run-the-base-tests" {
      depends-on "change-the-base-package"
    }
  }
}
```

The nested root step becomes ready when the parent step becomes active.

The child plan inherits the parent assignment unless a child step overrides it.

The parent remains `working` while its child plan runs.

The parent completes after the child plan and its other conditions complete.

## Reusable plans

A step can use a ready plan that already exists in the graph.

```kdl
step "execute-the-release-plan" {
  depends-on "prepare-the-release"
  assigned-to "agent/release"
  uses-plan "release/work@REVISION_HASH"
}
```

`uses-plan` should select an exact ready revision. It should never follow a moving draft head.

The selected plan stays dormant until the containing step becomes active.

The containing assignment becomes the default assignment for the used plan.

## Plan production during a run

Sometimes an author cannot make an honest detailed plan in advance.

The graph should expose planning as work instead of inventing false detail.

```kdl
step "produce-the-base-work-plan" {
  depends-on "start-workers"
  assigned-to "agent/sig.base"
  produces-plan "rename-base/work"
}

step "rename-base" {
  depends-on "produce-the-base-work-plan"
  assigned-to "agent/sig.base"
  uses-plan "rename-base/work"
}
```

The producing step completes only after it publishes a valid ready revision.

A human judge can review that revision before the execution step starts.

## Plan revisions

An agent can discover that an accepted plan is incorrect.

The agent should propose or publish a replacement revision. It should not silently change hidden TUI state.

A revision proposal needs this information:

```text
plan.revision.proposed {
  plan: "rename-base/work"
  replaces: REVISION_HASH
  author: "agent/sig.base"
  reason: "The package has a second generated protocol table."
}
```

The graph should retain completion evidence for unchanged steps.

The engine can compare normalized step definition hashes.

A changed step loses its old completion. A dependent step also loses completion when the change invalidates its inputs.

An unrelated branch keeps its completion and continues to run.

## Scope change policy

The scope selects the plan change policy.

```kdl
scope "signal-rename" change-policy="agent" {
  // Scope members and plans.
}
```

The default policy is `agent`.

The `agent` policy should have narrow authority. The current assignee can revise the plan below its owned step.

The current assignee cannot revise an unrelated sibling branch.

Possible stricter policies are `supervisor` and `human-review`.

A pending review blocks only the affected work. Independent branches continue.

Without authenticated identities and ACLs, this policy is a workflow rule. It is not a security boundary.

st3 should not know about Fabric. Later security can work through Fabric, Tailscale, another VPN, or a VPC.

## Assignment and work notification

`assigned-to` is durable graph state. It replaces a manual dispatch message.

An assignment belongs to a materialized step run. A stored plan definition does not create active work.

When a step becomes ready, st3 creates one durable `work.available` record for its assignee.

The record identity includes the plan run, step run, and assignment revision.

Repeated reconciliation produces the same record. The reconciler does not send repeated notifications.

The native driver waits until the agent TUI is ready. It then presents the assignment at a safe prompt boundary.

The assignment remains available through a durable work queue. Correctness does not depend on one terminal notification.

The proposed notification rules are:

| Graph change | Notify the agent |
| --- | --- |
| Save or update a draft plan | No |
| Publish a ready but unused plan | No |
| Materialize a blocked step | No |
| Make a step ready | Yes |
| Change the assignee of unclaimed ready work | Yes |
| Update progress or evidence | No |
| Add ready work to an active sub-plan | Yes, for the new work |
| Restart the reconciler | No duplicate notification |

If the assignee is not running, the step waits for the assignee.

The reconciler starts the declared agent and delivers the pending assignment after the TUI becomes ready.

An unresolved assignee is a graph error. The assignment must not invent an agent configuration.

An unclaimed assignment can move to a new assignee. st3 revokes the old offer and creates one new offer.

A claimed or working step requires a release, a handoff, or an accepted plan revision.

st3 must not silently create two workers for one claim.

An explicit `message` remains useful for real communication. It is not necessary for routine work dispatch.

## External progress and work claims

Step status should derive from immutable claims.

Useful step states include:

- `blocked`
- `ready`
- `claimed`
- `working`
- `awaiting-review`
- `completed`
- `failed`
- `cancelled`

A work claim should record these fields:

- The plan run.
- The step run.
- The agent identity.
- The agent incarnation.
- The assignment revision.
- The lease expiry.

Useful progress claims include:

```text
step.claimed
step.started
step.progress
step.completed
step.failed
step.released
step.blocked
```

`step.progress` can record intentional todos, a short summary, and evidence references.

It must not require chain-of-thought or private model reasoning.

A lease prevents a dead agent incarnation from owning work forever.

After a lease expires, the step can become ready again. The durable history still shows the earlier attempt.

## Human judges

A human judge is a normal judge with a person subject.

```kdl
step "nathan-approves" {
  depends-on "held-out-judges-pass"

  judges {
    human "person/nathan"
  }
}
```

The engine creates a durable review request after the earlier completion conditions hold.

An approval should bind to these inputs:

- The plan revision.
- The step run.
- The judge configuration.
- The evidence snapshot.

A material input change makes the approval stale. The human must review the new inputs.

Initial human decisions can include `pass`, `fail`, and `changes-requested`.

`changes-requested` leaves the step open and starts the plan revision workflow.

Until st3 has authenticated identities, `person/nathan` is a declared actor. st3 cannot prove who supplied the decision.

## Human gates for costly evals

The complete plan can include costly work without running it immediately.

```kdl
step "prove-one-complex-eval" {
  depends-on "implementation-is-ready"
  judges {
    judge "The complex Codex eval passes" {
      exec "st3 eval ./evals/st3/signal-rename/eval.kdl"
    }
  }
}

step "nathan-reviews-the-proof" {
  depends-on "prove-one-complex-eval"
  judges {
    human "person/nathan"
  }
}

step "run-all-translated-evals" {
  depends-on "nathan-reviews-the-proof"
  judges {
    judge "Every translated eval passes" {
      exec "./scripts/run-all-translated-evals.sh"
    }
  }
}
```

The graph shows the complete intended work from the start.

The human gate prevents token use until the proof receives approval.

## Repository Markdown plans

Some users will keep a plan as a committed Markdown file.

st3 can support this workflow without giving Markdown special server semantics.

The graph uses one step to compile the file and another step to execute the produced plan.

```kdl
plan "bootstrap-project" state="ready" {
  step "compile-the-repository-plan" {
    assigned-to "agent/planner"
    goal "Read doc/project/plan@SHA256 and publish its complete ready graph plan before implementation starts."
    produces-plan "project/work"
  }

  step "execute-the-project-plan" {
    depends-on { step "compile-the-repository-plan" completed }
    assigned-to "agent/planner"
    uses-plan output-of="compile-the-repository-plan"
  }
}
```

The first step reads the Markdown and publishes a complete ready graph plan.

The second step activates the produced plan and tracks its progress.

One agent can perform both steps and keep the same session.

This split prevents substantial work before plan externalization.

A committed file reference records its repository revision, path, and content hash.

An uncommitted document must be uploaded first. The plan then references its name and hash.

The publish operation refuses a reference when the stored bytes no longer match the hash.

## Concurrent plan example

This example combines the main ideas. Its grammar remains illustrative.

```kdl
scope "signal-rename" retention="temporary" change-policy="agent" {
  plan "signal-rename" state="ready" {
    step "start-workers" {
      subgraph {
        agent "sig.sup" {
          workspace "${EVAL_ROOT}/sup"
          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
          }
        }

        agent "sig.base" {
          workspace "${EVAL_ROOT}/base"
          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
          }
        }

        agent "sig.support" {
          workspace "${EVAL_ROOT}/support"
          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
          }
        }
      }
    }

    step "rename-base" {
      depends-on "start-workers"
      assigned-to "agent/sig.base"
      goal "Rename the base package and preserve runtime primitives."

      plan "work" {
        step "inspect-the-base-package" { }

        step "change-the-product-names" {
          depends-on "inspect-the-base-package"
        }

        step "run-the-base-tests" {
          depends-on "change-the-product-names"
        }
      }
    }

    step "rename-support" {
      depends-on "start-workers"
      assigned-to "agent/sig.support"
      goal "Rename the support packages and preserve runtime primitives."

      plan "work" {
        step "inspect-the-support-packages" { }

        step "change-the-product-names" {
          depends-on "inspect-the-support-packages"
        }

        step "run-the-support-tests" {
          depends-on "change-the-product-names"
        }
      }
    }

    step "integrate" {
      depends-on "rename-base" "rename-support"
      assigned-to "agent/sig.sup"
      goal "Integrate both branches and run the complete suite."

      plan "work" {
        step "merge-the-base-change" { }
        step "merge-the-support-change" { }

        step "run-the-complete-suite" {
          depends-on "merge-the-base-change" "merge-the-support-change"
        }
      }
    }

    step "held-out-judges-pass" {
      depends-on "integrate"

      judges {
        judge "Every commit stayed in its package lane" {
          exec "bash ./judges/isolation.sh"
        }

        judge "The complete suite passes" {
          exec "bash ./judges/suite-green.sh"
        }

        judge "The rename is complete" {
          exec "bash ./judges/rename.sh"
        }

        judge "The runtime primitives remain intact" {
          exec "bash ./judges/primitive.sh"
        }
      }
    }

    step "nathan-approves" {
      depends-on "held-out-judges-pass"

      judges {
        human "person/nathan"
      }
    }

    step "stop-the-eval-scope" {
      depends-on "nathan-approves"

      subgraph {
        scope "signal-rename" { stop }
      }
    }
  }
}
```

`rename-base` and `rename-support` start together.

`integrate` waits for both branches.

No kickoff message is necessary because each ready assignment creates durable work.

The final stop step completes through automatic scope convergence.

## Operator view

The CLI or UI can render nested plans as one tree:

```text
signal-rename [running]
├── start-workers [completed]
├── rename-base [working: agent/sig.base]
│   └── work
│       ├── inspect-the-base-package [completed]
│       ├── change-the-product-names [working]
│       └── run-the-base-tests [blocked]
├── rename-support [working: agent/sig.support]
│   └── work
│       ├── inspect-the-support-packages [completed]
│       ├── change-the-product-names [working]
│       └── run-the-support-tests [blocked]
├── integrate [blocked]
├── held-out-judges-pass [blocked]
├── nathan-approves [blocked]
└── stop-the-eval-scope [blocked]
```

The view comes from graph claims. It does not depend on private harness memory.

## Important separations

These separations keep the model understandable:

- A plan definition is not a plan run.
- A step assignment is not a message.
- A notification is not the source of assignment truth.
- A subgraph is not a nested plan.
- Runtime convergence is not a semantic judge result.
- A human judge is not an authenticated identity yet.
- A progress summary is not chain-of-thought.
- A migration tool is not a runtime compatibility adapter.

## Open design work

The conversation did not finalize these items:

- The exact KDL property names and nesting rules.
- The exact API resources for drafts, revisions, runs, and reviews.
- The exact claim schemas and lease duration.
- The handoff flow for claimed work.
- The invalidation algorithm for downstream completion evidence.
- The CLI names for plan publication, activation, work queues, and human review.
- The UI behavior for a safe harness notification.
- The later TLS, identity, and ACL model.

These open items do not change the main model recorded in this snapshot.
