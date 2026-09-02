# st3 plan graph runtime

Status: current language and runtime specification.

Every st3 KDL document starts with `version 2`. It contains one untyped `subgraph` root.

A plan is an immutable definition in the claims graph. Publishing a plan does not start it. A plan run binds to one exact plan revision.

## Core rules

- A plan has `draft`, `ready`, or `retired` state.
- Only a ready plan can start.
- A plan needs one through three `goal` nodes.
- A step accepts zero through three `goal` nodes.
- Plans and steps can repeat `baseline` and `gate`.
- Plans and steps can contain one `produces` block.
- All sibling products must exist. All sibling gates must pass.
- Every normal step must complete before the plan can complete.
- `depends-on` defines execution order. Source order defines display order only.
- A missing `depends-on` makes a step a root. It does not imply a dependency on the previous step.
- st3 rejects missing step references and dependency cycles.
- st3 does not accept `outcome`, `judges`, or `judge`.

## Complete example

```kdl
version 2

subgraph {
  scope "release/${ST_PLAN_RUN}" retention="temporary" change-policy="agent" {
    plan "release" state="ready" {
      goal "Produce a verified release decision."
      goal "Keep the source and test evidence visible in the graph."

      baseline "the release request is ready" {
        field "status" "resource/release-request" "is" "ready"
      }

      produces {
        resource "plan-run/${ST_PLAN_RUN}/release-decision" {
          kind "release.decision"
          state "published"
        }
      }

      gate "the requester approves the release" type="human" {
        reviewer "person/nathan"
        question "Is this release ready?"
        review "resource/plan-run/${ST_PLAN_RUN}/release-decision"
      }

      step "start-team" {
        title "The release team is ready"
        subgraph {
          agent "release.lead" {
            workspace "${ST_WORKSPACE}/lead"
            harness "codex" {
              model "gpt-5.6-sol"
              effort "medium"
              prompt "Claim assigned st3 work and publish the release decision."
            }
          }
          agent "release.test" {
            under "release.lead" reason="the lead combines the test evidence"
            workspace "${ST_WORKSPACE}/test"
            harness "codex" {
              model "gpt-5.6-sol"
              effort "medium"
              prompt "Claim assigned st3 work and publish the test evidence."
            }
          }
        }
      }

      step "inspect" timeout="20m" {
        title "The source is inspected"
        goal "Inspect the exact release source and publish an inspection report."
        assigned-to "agent/release.lead"
        depends-on { step "start-team" completed }
        baseline "the source is present" {
          exists "resource/release-source"
        }
        produces {
          resource "plan-run/${ST_PLAN_RUN}/inspection" {
            kind "release.inspection"
            state "published"
          }
        }
        gate "the report contains a revision" {
          field "revision" "resource/plan-run/${ST_PLAN_RUN}/inspection" "starts-with" "git:"
        }
      }

      step "verify" timeout="20m" {
        title "The release decision is verified"
        goal "Run the tests and publish the final release decision."
        assigned-to "agent/release.test"
        depends-on { step "inspect" completed }
        retry { attempts 2; backoff "30s" }
        gate "the release tests pass" {
          exec "./verify-release.sh"
          host "local"
          workspace "${ST_WORKSPACE}/test"
          time-limit "5m"
        }
      }

      step "cleanup" finally=#true {
        title "The temporary release scope is empty"
        subgraph { scope "release/${ST_PLAN_RUN}" { stop } }
        gate "the scope has no live member" {
          empty "scope/release/${ST_PLAN_RUN}"
        }
      }
    }
  }
}
```

The plan baseline protects the run admission boundary. The inspection product is intermediate step output. The release decision is a final plan product. The human gate is a plan-level acceptance condition.

## Plan syntax

```kdl
plan "PLAN_ID" state="ready" {
  goal "One measurable plan goal."
  goal "An optional second goal."
  goal "An optional third goal."

  baseline "NAME" { GRAPH_PREDICATE }
  produces { PRODUCT... }
  gate "NAME" { GATE_BODY }

  step "STEP_ID" { ... }
}
```

The `state` property is required for a top-level or scoped plan. A nested plan defaults to ready because it is already part of a submitted parent revision.

Plan IDs can contain path separators. Step IDs cannot. IDs cannot be empty, contain whitespace, start or end with `/`, or contain `//`.

Plan goal order is preserved. Each plan must have one, two, or three goals.

A plan can repeat baselines and gates. Their names must be unique within that plan. A plan has at most one `produces` block.

A plan must contain at least one step.

## Step syntax

```kdl
step "STEP_ID" timeout="20m" finally=#false {
  title "A display title"
  goal "One optional goal."
  goal "A second optional goal."
  goal "A third optional goal."
  assigned-to "agent/node.worker"
  document "doc/project/request@SHA256"

  depends-on {
    step "earlier-step" completed
  }

  baseline "NAME" { GRAPH_PREDICATE }
  subgraph { DESIRED_STATE... }
  plan "nested-work" { ... }
  retry { attempts 3; backoff "30s" }
  produces { PRODUCT... }
  produces-plan "generated-plan"
  uses-plan output-of="producer-step"
  gate "NAME" { GATE_BODY }
}
```

`title`, `assigned-to`, `subgraph`, `plan`, `retry`, `produces`, `produces-plan`, and `uses-plan` are single fields.

`goal`, `document`, `depends-on`, `baseline`, and `gate` can repeat. A step accepts at most three goals.

`timeout` applies to the complete step attempt. A step cannot use a deadline gate because its timeout is the one step deadline.

`finally=#true` selects final-phase work. Final steps run after normal success, failure, or cancellation. A final step does not make normal work optional.

## Goals

A goal is a concise, falsifiable statement about the result.

Use one `goal` node for one statement. Use up to three nodes when the plan or step has separate required outcomes.

Do not use source order or bullet syntax inside one string to create hidden execution structure. Steps and `depends-on` own execution structure.

## Baselines

A baseline records state that must be true before new work starts.

```kdl
baseline "the incident is still open" {
  field "status" "resource/incident" "is" "open"
  lacks "doc/incident/decision@SHA256" "closed"
}
```

A baseline contains one or more graph predicates. Its predicates form an AND relation.

Baselines accept `exists`, `empty`, `field`, `has`, and `lacks`. They do not execute shell, LLM, human, or deadline work.

Plan baselines run before root work admission. A false plan baseline puts the plan run in blocked state, and st3 rechecks it after relevant graph changes while admission remains blocked. Once normal work is admitted, the plan baseline is latched and is not re-evaluated as a continuous gate.

Step baselines run after dependencies hold and before each attempt becomes ready. A false step baseline blocks the step. It does not consume an attempt. A retry checks the baseline again.

A baseline is not historical storage by itself. The plan request or a prior claim must publish the measured state that the predicate names.

## Products

`produces` declares graph state that the work promises to create.

```kdl
produces {
  resource "plan-run/${ST_PLAN_RUN}/artifact" {
    kind "build.artifact"
    state "published"
  }
  message "plan-run/${ST_PLAN_RUN}/handoff" {
    status "accepted"
  }
}
```

Products can match `resource`, `message`, `agent`, `exec`, `pty`, or `scope` subjects. Each product can require scalar fields.

All products in one block must hold.

A step product is intermediate output for that step. A plan product is a final contract for the complete normal phase.

The worker creates or observes products. st3 verifies them. The `produces` keyword does not perform the action.

A plan product can refer to output created during any step. Do not duplicate a step product at plan level unless the same graph subject is intentionally both an intermediate and final contract.

## Gates

A gate decides whether a completed work boundary can pass.

Plans and steps use repeated flat nodes:

```kdl
gate "the artifact exists" { exists "resource/build-artifact" }
gate "the report is green" { field "status" "resource/report" "is" "green" }
```

Sibling gates form an AND relation. There is no `gates` wrapper.

Step gates run after the subgraph, worker report, nested work, used plan, and products hold. Plan gates run after every normal step and all plan products hold.

Each running gate records `gate.requested` and `gate.result`. The result cites operation evidence. A pass releases the boundary. A failure fails the step or plan. A pending graph or human gate keeps the boundary pending.

### Predicate gates

```kdl
gate "subject exists" { exists "resource/result" }
gate "scope is empty" { empty "scope/temporary-work" }
gate "field matches" { field "status" "resource/result" "is" "green" }
gate "prefix matches" { field "revision" "resource/result" "starts-with" "git:" }
gate "text contains value" { has "doc/report@SHA256" "GREEN" }
gate "text omits value" { lacks "message/report" "UNVERIFIED" }
```

`field` uses this argument order: path, full subject, operator, value. Operators are `is`, `starts-with`, and `contains`.

`has` and `lacks` accept file, document, or message subjects.

A plan gate or checkpoint gate can also use `deadline "10m"`. A step uses its `timeout` property instead.

### Mechanical gates

```kdl
gate "the tests pass" {
  exec "cargo test --workspace"
  host "local"
  workspace "${ST_WORKSPACE}"
  env { RUST_BACKTRACE "1" }
  time-limit "10m"
}
```

A mechanical gate requires `exec`, `host`, and `workspace`. `env` is optional. `time-limit` defaults to two minutes.

The command runs through the supervised exec runtime. Its result is attempt-bound and durable.

### LLM gates

```kdl
gate "the migration preserves the public contract" type="llm" {
  model "gpt-5.6-sol"
  host "local"
  workspace "${ST_WORKSPACE}"
  tools "shell" "git"
  token-budget 12000
  time-limit "10m"
  prompt "Inspect the diff and evidence. Return PASS or FAIL with a reason."
}
```

An LLM gate requires an explicit model, host, workspace, tool list, positive token budget, time limit, and prompt.

Registered tools are `shell`, `git`, `gh`, and `network`.

The gate fails if its structured usage exceeds the declared token budget.

### Human gates

```kdl
gate "the release is approved" type="human" {
  reviewer "person/nathan"
  question "Is the release ready?"
  review "resource/release-candidate"
  review "doc/release/report@SHA256"
}
```

A human gate requires a full `person/...` reviewer. The question and repeated review targets are optional.

st3 creates one `review.requested` claim for the exact plan or step revision and attempt. A review decision must match that request.

## Dependencies

`depends-on` is the only step ordering language.

```kdl
depends-on {
  step "build" completed
  step "test" terminal
  field "status" "resource/change-window" "is" "open"
}
```

A step dependency can require `completed`, `failed`, or `terminal`. `completed` is the default when the state is omitted.

Graph predicate dependencies accept the same deterministic predicates as baselines. They latch after they pass. A later graph change does not move active work backward.

Dependencies inside a nested plan refer to sibling steps in that nested plan.

## Runtime sequence

Each run starts with all steps in pending state.

For a normal step, st3 performs this sequence:

1. Wait for the normal phase.
2. Wait for the parent nested step, when present.
3. Wait for every explicit dependency.
4. Evaluate all step baselines.
5. Verify that the assignee is present, when present.
6. Mark the attempt ready.
7. Materialize its subgraph.
8. Wait for subgraph convergence.
9. Wait for the assigned worker report.
10. Wait for nested plan steps or an exact used plan.
11. Verify products.
12. Evaluate gates.
13. Mark the step completed or failed.

When a step fails and its retry policy permits another attempt, st3 increments the attempt, applies backoff, and starts again at dependency and baseline admission. Retryable failure does not terminate the plan before the retry.

After all normal steps complete, st3 verifies plan products and plan gates. It then enters the final phase or completes the run.

## Nested plans

A nested `plan` is part of its parent plan revision.

The parent step starts the nested roots after the parent is active. Nested steps inherit the parent assignment unless a child overrides it.

Nested work remains durable graph state. It is not stored only in harness memory.

## Produced and used plans

A step can publish one complete ready plan as an attempt-bound output.

```kdl
step "compile-plan" {
  assigned-to "agent/planner"
  document "doc/project/plan@SHA256"
  produces-plan "project-work"
}
```

The worker must hold the producing step or one of its same-assignee nested steps.

```sh
st3 work publish-plan step-run/RUN/compile-plan generated.kdl --as agent/planner
```

The published document must contain exactly one ready plan with the declared ID. st3 publishes the immutable revision and binds it to the producing definition and attempt.

A later step can start that exact output:

```kdl
step "execute-plan" {
  assigned-to "agent/planner"
  depends-on { step "compile-plan" completed }
  uses-plan output-of="compile-plan"
}
```

The output form requires an explicit completed dependency on the producer.

A step can also use an already published exact revision:

```kdl
uses-plan "project-work@REVISION_SHA256"
```

A used plan starts one linked child run. The wrapper completes only after the child completes. A failed or cancelled child fails the wrapper.

## Automatic context

st3 supplies these exact context names:

| Name | Value |
| --- | --- |
| `ST_PLAN` | Plan ID. |
| `ST_PLAN_REVISION` | Active plan revision hash. |
| `ST_PLAN_RUN` | Plan run ID without the `plan-run/` prefix. |
| `ST_ROOT_PLAN_RUN` | Full root `plan-run/...` subject. |
| `ST_SCOPE` | Expanded run scope, or an empty value. |
| `ST_WORKSPACE` | Absolute run workspace. |
| `ST_REQUESTER` | Normalized requester subject. |
| `ST_STEP` | Step path in a step context. |
| `ST_STEP_RUN` | Full step-run subject in a step context. |
| `ST_ATTEMPT` | Current attempt number in a step context. |
| `ST_ASSIGNEE` | Normalized assignee, or an empty value. |
| `ST_PARENT_STEP_RUN` | Parent step-run subject, or an empty value. |
| `ST_GATE` | Gate name in a running gate context. |
| `ST_AGENT` | Runtime agent or member identity. |

The values are available for `${NAME}` KDL interpolation when the current plan, step, member, or gate context defines them. Step members and running gates receive the same applicable values as environment variables.

For example, use `${ST_PLAN_RUN}` directly. Do not write a manual mapping such as `env { PLAN_RUN "${ST_PLAN_RUN}" }` only to rename the built-in value.

The exact built-in names are reserved in authored `env` maps. st3 rejects an attempt to replace them. Other names, including other `ST_*` names, remain available to applications.

An unknown variable or a variable that is not available in the current phase is an error.

## Agent grouping

`under` is repeatable agent metadata.

```kdl
agent "researcher" {
  under "lead" reason="the lead combines the research"
  under "design-group"
  workspace "/work/research"
  command "research"
}
```

A bare target uses the local host identity. A full `agent/host.name` target stays full.

The relation is visible in `st3 agents --json`, status, and assigned work. It is suitable for a tree or graph UI.

The relation does not create permission, lifecycle, scheduling, or mandatory reporting behavior.

Missing targets, self-relations, and cycles create warnings during preview. They do not block publication or another agent.

## Work commands

Assigned work uses a renewable lease bound to the agent identity and runtime incarnation.

```sh
st3 work ls --as agent/node.worker
st3 work show step-run/RUN/step
st3 work claim step-run/RUN/step --as agent/node.worker
st3 work progress step-run/RUN/step --summary "The tests are running."
st3 work complete step-run/RUN/step --summary "The product is published."
st3 work fail step-run/RUN/step --reason "The compiler rejected the source."
st3 work release step-run/RUN/step --reason "The work needs another owner."
```

A worker completion report is not a correctness result. Products and gates still control final completion.

The native driver renews active leases and delivers one idempotent Small Talk assignment for ready parent work.

## Plan revisions

The default scoped change policy is `agent`. An assignee can revise its assigned step subtree.

`supervisor` and `human-review` policies require a `change-authority`.

```sh
st3 work revise PLAN_RUN replacement.kdl \
  --as agent/node.worker \
  --reason "The generated source adds one verification step."
```

Changed steps and their dependents reset. Unchanged independent steps keep their completed state. The run keeps its original root revision.

## Immutable documents

`document` on a step always requires `doc/NAME@SHA256`.

```sh
st3 doc put request.md --as doc/project/request
st3 doc get doc/project/request@SHA256 --output request.md
st3 doc list doc/project/request
```

Bare document names can appear in an intent before preview. The preview resolves them to the current exact hash. Apply validates the bytes and binds that exact version.

## Preview, publish, and run

`st3 preview FILE` validates KDL, resolves documents, displays changes, returns subject tokens, and performs no write.

`st3 run FILE` repeats preview, applies the exact tokens, selects one ready plan, starts one run, and follows it unless `--detach` is present.

If a file contains multiple ready plans, select one with `--plan`.

## Planning mode

Planning mode asks one durable Codex harness to author Markdown and KDL for review.

```sh
st3 plan start --id release-plan request.md \
  --workspace ./project \
  --as person/nathan \
  --model gpt-5.6-sol \
  --effort medium

st3 plan show SESSION
st3 plan preview SESSION
st3 plan revise SESSION feedback.md --as person/nathan
st3 plan approve SESSION PREVIEW_HASH --as person/nathan
st3 plan cancel SESSION --as person/nathan --reason "The request changed."
```

The planner uses this command:

```sh
st3 plan submit SESSION --markdown PLAN.md --kdl plan.kdl
```

The session stores the request, feedback, Markdown, and KDL as immutable documents. Small Talk carries document references, not mutable file paths.

The candidate must contain exactly one ready plan with the requested ID.

Preview returns these review values:

- the candidate and plan revisions;
- a static dependency graph;
- the graph subject diff;
- warnings and blockers;
- exact subject tokens;
- one hash over the complete preview.

Revision invalidates the prior preview. Approval requires the current preview hash and current subject tokens.

Approval publishes the ready plan and one `plan.documents` claim. It does not start a run. Approval and cancellation stop the planner.

Controllers should wait on `planning-session.*` events. They must not spend an agent turn to poll session status.

## Public gate result

A running mechanical or LLM gate gets a one-use operation capability. Its runner records the terminal result with:

```sh
st3 gate-result pass \
  --operation-capability OPERATION_CAPABILITY \
  --reason "The checks passed." \
  --evidence claim/EVIDENCE
```

The API endpoint is `POST /v1/gate-results`. The durable kinds are `gate.requested` and `gate.result`.

## Claims and evidence

Plan execution uses these important claim kinds:

- `plan.published` records an immutable plan revision.
- `plan.documents` links an approved planning session to Markdown and KDL.
- `plan-run.created`, `plan-run.revised`, and `plan-run.state` record run history.
- `step-run.state` and `step-run.retry` record step and attempt history.
- `plan.produced` binds a generated plan to one producing attempt.
- `gate.requested` and `gate.result` record gate operations and evidence.
- `review.requested` and `review.decision` record exact human gates.

Evidence is a list of claim IDs or immutable graph references that support a result. The evidence does not replace the gate. The gate definition says what must be decided; evidence records why the result is trustworthy.

## Eval contract

`st3 eval DIRECTORY` archives the explicit directory, posts staged documents, applies its version 2 intent, and starts the selected eval plan.

The planning-mode eval uses one real Codex planner. A controller waits on the event stream and directly approves the first valid candidate. Mechanical gates prove that the plan was hidden before approval, the preview graph and diff were rendered, the exact hash was approved, one ready plan was published, no run started, immutable documents were linked, the planner stopped, and the workspace did not change.

The revision and stale-preview path is a deterministic API test. It does not spend a model run.
