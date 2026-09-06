# st3 plan graph runtime

Status: current language and runtime specification.

See [kdl-lifecycle.md](./kdl-lifecycle.md) for complete day-to-day publication workflows.

Every st3 KDL document starts with `version 2`. Declarations follow the version directly.

A plan is an immutable definition in the claims graph. Publishing a plan does not start it.

A plan run has one stable subject. Each immutable run generation binds that run to one exact plan revision.

## Core rules

- A plan has `draft`, `ready`, or `retired` state.
- Only a ready plan can start.
- A plan needs one through three `goal` nodes.
- A step accepts zero through three `goal` nodes.
- Plans and steps can repeat `baseline` and `gate`.
- Plans and steps can contain one `produces` block.
- All sibling products must exist. All sibling gates must pass.
- A plan completes only through its explicit `completion` block.
- A plan without `completion` stays open after its available work is exhausted.
- `depends-on` defines execution order. Source order defines display order only.
- A missing `depends-on` makes a step a root. It does not imply a dependency on the previous step.
- st3 rejects missing step references and dependency cycles.
- st3 does not accept `outcome`, `judges`, or `judge`.
- `assigned-to` and `available-to` can set a plan default or select one step.
- `agentless` is step-only. A step with no inherited selector is also agentless.
- A selector does not grant revision authority.
- A plan allows one active run by default.
- `concurrent-runs` enables concurrent active runs. An optional `max` property bounds them.
- A plan can declare exact text and resource inputs.

## Complete example

```kdl
version 2

plan "release" state="ready" revisions="human-only" revision-reviewer="person/nathan" revision-cutover="when-idle" {
    input "source" kind="resource"
    goal "Produce a verified release decision."
    goal "Keep the source and test evidence visible in the graph."
    completion { when "all-steps-exhausted" }

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

    baseline "the release request is ready" {
      field "status" "${input.source}" "is" "ready"
    }

    produces {
      resource "plan-run/${ST_PLAN_RUN}/release-decision" {
        kind "custom.st3.release-decision"
        state "published"
      }
    }

    gate "the requester approves the release" type="human" {
      reviewer "person/nathan"
      question "Is this release ready?"
      review "resource/plan-run/${ST_PLAN_RUN}/release-decision"
    }

    step "start-team" {
      agentless
      title "The release team is ready"
      gate "the lead exists" { exists "agent/${ST_PLAN_RUN}/release.lead" }
      gate "the test agent exists" { exists "agent/${ST_PLAN_RUN}/release.test" }
    }

    step "inspect" timeout="20m" {
      title "The source is inspected"
      goal "Inspect the exact release source and publish an inspection report."
      assigned-to "agent/${ST_PLAN_RUN}/release.lead"
      depends-on { step "start-team" completed }
      produces {
        resource "plan-run/${ST_PLAN_RUN}/inspection" {
          kind "custom.st3.release-inspection"
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
      assigned-to "agent/${ST_PLAN_RUN}/release.test"
      depends-on { step "inspect" completed }
      retry { attempts 2; backoff "30s" }
      gate "the release tests pass" {
        exec "./verify-release.sh"
        host "local"
        workspace "${ST_WORKSPACE}/test"
        time-limit "5m"
      }
    }
}
```

The plan body owns the complete plan revision. Its agents use stable subjects inside one plan run.

The plan baseline protects the run admission boundary. The inspection product is intermediate step output.

The release decision is a final plan product. The human gate is a plan-level acceptance condition.

After acceptance, cleanup stops both agents before the run becomes completed.

## Plan syntax

```kdl
plan "PLAN_ID"
  state="ready"
  revisions="human-only"
  revision-reviewer="person/reviewer"
  revision-cutover="when-idle" {
  goal "One measurable plan goal."
  goal "An optional second goal."
  goal "An optional third goal."

  input "message" kind="text"
  input "source" kind="resource"
  concurrent-runs max=4

  assigned-to "agent/${ST_PLAN_RUN}/owner"
  // Or repeat available-to. A plan cannot declare agentless.

  completion { when "all-steps-exhausted" }
  // Or: completion { depends-on { step "publish" completed } }

  baseline "NAME" { GRAPH_PREDICATE }
  produces { PRODUCT... }
  gate "NAME" { GATE_BODY }

  PLAN_AGENTS...

  step "STEP_ID" { ... }
  finally { step "CLEANUP_ID" { ... } }
}
```

The `state` property is required for a top-level plan. A nested plan defaults to ready because it is already part of a submitted parent revision.

Plan IDs can contain path separators. Step IDs cannot. IDs cannot be empty, contain whitespace, start or end with `/`, or contain `//`.

Plan goal order is preserved. Each plan must have one, two, or three goals.

A plan can repeat baselines and gates. Their names must be unique within that plan. A plan has at most one `produces` block.

A plan can repeat `input`. Each input name is unique and uses `kind="text"` or `kind="resource"`.

The input set and kinds cannot change across revisions of an active run. Input values remain immutable across all run generations.

The default active run limit is one. Bare `concurrent-runs` removes the limit. `concurrent-runs max=4` sets a positive limit.

When active revisions declare different limits, st3 uses the strictest limit. A lower limit does not cancel existing runs.

An exact idempotent retry returns its existing run before the capacity check. A direct start error lists the active run subjects.

`revisions="human-only"` is optional and inherited by child steps. `revision-reviewer` requires that protection.

The reviewer defaults to the plan run requester. `revision-cutover` is `restart-active` by default or `when-idle` when declared. The value in the current generation controls how its successor starts; a candidate cannot select its own cutover.

A plan can contain direct declarations. Direct agents in the plan can revise the complete plan.

A plan can contain zero steps. A zero-step plan without `completion` becomes standing after reconciliation.

`completion` accepts one shortcut or one dependency block. The two forms cannot appear together.

`when "all-steps-exhausted"` selects all normal steps without listing them. Failed retryable work is not exhausted.

The dependency form uses the same explicit dependency language as a step. It can select a smaller completion frontier.

A completion dependency cannot reference a final step.

Without `completion`, the plan never becomes terminal because it exhausted its steps.

## Run ownership and concurrency

A plan run is the sole owner of execution state. An agent cannot exist outside a plan run.

An authored runtime ID is local to the run. st3 expands it to these subjects:

- `agent/RUN/LOCAL_ID`;
- `exec/RUN/LOCAL_ID`;
- `pty/RUN/LOCAL_ID`;
- `observer/RUN/LOCAL_ID`;
- `subscription/RUN/LOCAL_ID`;
- `schedule/RUN/LOCAL_ID`.

Two concurrent runs can use the same local IDs. Two declaration sites in one generation cannot declare the same runtime subject.

An open plan keeps its runtimes present. This rule supports long-lived chat agents without a second plan type.

A runtime `stop` can occur only inside the owner plan. Root control uses a named plan-run `cancellation` instead.

The default plan permits one nonterminal run. This default also lets `st3 plan show PLAN` identify the current run.

Bare `concurrent-runs` permits unlimited nonterminal runs. `concurrent-runs max=N` sets a positive limit.

The capacity check runs after the idempotency check. A child start waits when capacity is full, but a direct start returns `plan-run-capacity`.

Plans and agents do not support in-place ownership changes. Publish a replacement plan and cancel the old run when ownership must change.

## Step syntax

```kdl
step "STEP_ID" timeout="20m" revisions="human-only" revision-reviewer="person/reviewer" {
  title "A display title"
  goal "One optional goal."
  goal "A second optional goal."
  goal "A third optional goal."
  available-to "agent/${ST_PLAN_RUN}/worker-a"
  available-to "agent/${ST_PLAN_RUN}/worker-b"
  document "doc/project/request@SHA256"

  depends-on {
    step "earlier-step" completed
  }

  baseline "NAME" { GRAPH_PREDICATE }
  DESIRED_STATE...
  plan "nested-work" { ... }
  retry { attempts 3; backoff "30s" }
  produces { PRODUCT... }
  produces-plan "generated-plan"
  uses-plan output-of="producer-step"
  gate "NAME" { GATE_BODY }
}
```

`title`, `assigned-to`, `agentless`, `plan`, `retry`, `produces`, `produces-plan`, and `uses-plan` are single fields.

`available-to`, `goal`, `document`, `depends-on`, `baseline`, and `gate` can repeat. A step accepts at most three goals.

`timeout` applies to the complete step attempt. A step cannot use a deadline gate because its timeout is the one step deadline.

`finally {}` contains final-phase steps. Final steps run after normal success, failure, or cancellation.

A plan can have one `finally` block. Final steps can depend on other final steps.

Dependencies cannot cross the normal and final phases. A final step does not make normal work optional.

Step revision protection adds to inherited plan protection. Direct agents in the step can revise that step subtree.

## Goals

A goal is a concise, falsifiable statement about the result.

Use one `goal` node for one statement. Use up to three nodes when the plan or step has separate required outcomes.

Do not use source order or bullet syntax inside one string to create hidden execution structure. Steps and `depends-on` own execution structure.

## Plan inputs

A ready top-level plan can declare text and resource inputs.

```kdl
input "message" kind="text"
input "source" kind="resource"
```

A start request must provide exactly the declared names. Missing and extra names are errors.

Use `${input.message}` and `${input.source}` in execution content. st3 preserves quoted and multiline text when it writes interpolated KDL.

A resource input accepts `resource/NAME` or `resource/NAME@CLAIM_ID`.
st3 resolves a bare subject to its latest `resource.observed` claim atomically.

The run stores the exact resource subject and claim ID. Later claims do not change gates, inspection, or execution for that input.

Inputs do not support defaults, lists, secrets, schemas, or automatic environment export. Put an input in `env` when a process needs it.

Nested child plans cannot declare inputs in this version.

```sh
st3 publish plan.kdl --as person/operator
st3 plan start PLAN_ID \
  --input message="Review this release." \
  --input source=resource/release-source \
  --as person/operator

st3 claim resource/plan-inputs/source resource.observed \
  --field kind=custom.st3.document-source \
  --field state=ready
st3 eval ./evals/st3/plan-inputs \
  --input message="Input proof." \
  --input source=resource/plan-inputs/source
```

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

Plan baselines run before root work admission. st3 does not materialize a plan runtime before these baselines pass.

A false plan baseline puts the plan run in blocked state. st3 rechecks it after relevant graph changes while admission remains blocked.

Once normal work is admitted, the plan baseline is latched. st3 does not re-evaluate it as a continuous gate.

Step baselines run after dependencies hold and before each attempt becomes ready. A false step baseline blocks the step. It does not consume an attempt. A retry checks the baseline again.

A baseline is not historical storage by itself. The plan request or a prior claim must publish the measured state that the predicate names.

## Products

`produces` declares graph state that the work promises to create.

```kdl
produces {
  resource "plan-run/${ST_PLAN_RUN}/artifact" {
    kind "custom.st3.build-artifact"
    state "published"
  }
  message "plan-run/${ST_PLAN_RUN}/handoff" {
    status "read"
  }
}
```

Products can match `resource`, `message`, `agent`, `exec`, or `pty` subjects. Each product can require scalar fields.

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

Step gates run after direct declarations, worker report, nested work, used plan, and products hold. Plan gates run after every normal step and all plan products hold.

Each running gate records `gate.requested` and `gate.result`. The result cites operation evidence. A pass releases the boundary. A failure fails the step or plan. A pending graph or human gate keeps the boundary pending.

### Predicate gates

```kdl
gate "subject exists" { exists "resource/result" }
gate "run has no live runtime" { empty "plan-run/${ST_PLAN_RUN}" }
gate "field matches" { field "status" "resource/result" "is" "green" }
gate "prefix matches" { field "revision" "resource/result" "starts-with" "git:" }
gate "text contains value" { has "doc/report@SHA256" "GREEN" }
gate "text omits value" { lacks "message/report" "UNVERIFIED" }
```

`field` uses this argument order: path, full subject, operator, value. Operators are `is`, `starts-with`, and `contains`.

`has` and `lacks` accept file, document, or message subjects.

A plan gate can also use `deadline "10m"`. A step uses its `timeout` property instead.

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

st3 creates one `gate.requested` claim for the exact plan or step revision and attempt. A review decision must match that request.

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
5. Resolve the nearest work selector.
6. Verify that at least one eligible agent is present, when the selector names agents.
7. Mark the attempt ready and increment its readiness epoch.
8. Materialize its direct declarations.
9. Wait for declaration convergence.
10. Wait for a worker report when an agent claimed the step.
11. Wait for nested plan steps or an exact used plan.
12. Verify products.
13. Evaluate gates.
14. Mark the step completed or failed.

When a step fails and its retry policy permits another attempt, st3 increments the attempt, applies backoff, and starts again at dependency and baseline admission. Retryable failure does not terminate the plan before the retry.

The `completion` frontier selects when st3 checks plan products and gates. st3 then enters the final phase when one exists.

The run reaches `completed` after successful final work. A final failure makes the run failed.

A plan without `completion` stays open. It is `standing` when no step can move and no failure blocks movement.

## Nested plans

A nested `plan` is part of its parent plan revision.

The parent step starts the nested roots after the parent is active. Nested steps inherit the nearest work selector unless a child overrides it.

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

The worker must hold the producing step or one of its nested steps with the same assigned agent.

```sh
st3 work publish-plan step-run/GENERATION/compile-plan generated.kdl --as agent/planner
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
| `ST_PLAN_RUN` | Stable plan run ID without the `plan-run/` prefix. |
| `ST_RUN_GENERATION` | Current generation ID without the `run-generation/` prefix. |
| `ST_ROOT_PLAN_RUN` | Full root `plan-run/...` subject. |
| `ST_WORKSPACE` | Absolute run workspace. |
| `ST_REQUESTER` | Normalized requester subject. |
| `ST_STEP` | Step path in a step context. |
| `ST_STEP_RUN` | Full step-run subject in a step context. |
| `ST_ATTEMPT` | Current attempt number in a step context. |
| `ST_ASSIGNEE` | Fixed `assigned-to` agent, or an empty value for pools and agentless work. |
| `ST_PARENT_STEP_RUN` | Parent step-run subject, or an empty value. |
| `ST_GATE` | Gate name in a running gate context. |
| `ST3_SUBJECT` | Full subject of the current runtime member. |
| `ST_AGENT` | Full owning agent subject. It is absent for agentless runtimes. |

The plan and step values are available for `${NAME}` KDL interpolation when the current context defines them. Step members and running gates receive those values as environment variables.

`ST3_SUBJECT` and `ST_AGENT` are runtime-only values because their values depend on the materialized member.

For example, use `${ST_PLAN_RUN}` directly. Do not write a manual mapping such as `env { PLAN_RUN "${ST_PLAN_RUN}" }` only to rename the built-in value.

The exact built-in names are reserved in authored `env` maps. st3 rejects an attempt to replace them. Other names, including other `ST_*` names, remain available to applications.

`${PATH}` is also available for KDL interpolation. It uses the deterministic daemon service path.

An agent receives its own subject in both `ST3_SUBJECT` and `ST_AGENT`. A nested task receives its task subject in `ST3_SUBJECT` and its parent agent in `ST_AGENT`.

An agentless `exec` or `pty` receives `ST3_SUBJECT` and no `ST_AGENT`.

An unknown variable or a variable that is not available in the current phase is an error.

## Workspace existence

st3 requires every member workspace to exist before the member starts.

Use an explicit create property when the plan owns creation of that directory:

```kdl
workspace "${ST_WORKSPACE}/generated" create=#true
```

The default refusal prevents a spelling error from creating an unintended directory.

## Message delivery for generic harnesses

A harness driver can implement native message delivery. A generic terminal harness can use an explicit DING child:

```kdl
agent "worker" {
  workspace "${ST_WORKSPACE}/worker"
  command "worker-harness"
  exec "ding" {
    argv "st3" "driver" "ding"
    restart "on-failure"
  }
}
```

The nested exec receives the agent subject through `ST_AGENT`. It checks the local st3 API once per second and sends an incarnation-fenced terminal line.

The plan run owns and cleans up both runtimes. No implicit `ding` field exists in st3 KDL.

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

A bare target inside a plan uses the same plan run. A full external agent subject stays full.

The relation is visible in `st3 agents --json`, status, and assigned work. It is suitable for a tree or graph UI.

The relation does not create permission, lifecycle, scheduling, or mandatory reporting behavior.

Missing targets, self-relations, and cycles create warnings during preview. They do not block publication or another agent.

## Work selection

A plan or step can declare one agent selector kind. Only a step can declare `agentless`.

```kdl
assigned-to "agent/${ST_PLAN_RUN}/only-worker"
```

`assigned-to` means that only the named agent can claim the work.

```kdl
available-to "agent/${ST_PLAN_RUN}/worker-a"
available-to "agent/${ST_PLAN_RUN}/worker-b"
```

`available-to` creates an explicit pool. The first eligible claim wins one step atomically.

The same agent can claim multiple ready steps. A pool does not impose a one-step limit.

```kdl
agentless
```

`agentless` means that the reconciler performs the work without an agent claim. Subgraphs and gates can complete an agentless step.

A local selector replaces the inherited selector. It does not add to it.

The inheritance order is the step, its plan, its parent step, and its parent plan. The nearest selector wins.

A step without an explicit or inherited selector is agentless.

A duplicate pool member is invalid. Combining selector kinds in one plan or step is invalid.

A missing eligible agent creates a preview warning. A step blocks only when none of its eligible agents exist in desired state.

## Work commands

Claimed work uses a renewable claim bound to the agent identity and runtime incarnation.

```sh
st3 work ls --as agent/RUN/node.worker
st3 work show step-run/GENERATION/step
st3 work claim step-run/GENERATION/step --as agent/RUN/node.worker
st3 work progress step-run/GENERATION/step --summary "The tests are running."
st3 work complete step-run/GENERATION/step --summary "The product is published."
st3 work fail step-run/GENERATION/step --reason "The compiler rejected the source."
st3 work release step-run/GENERATION/step --reason "The work needs another owner."
```

A worker completion report is not a correctness result. Products and gates still control final completion.

The native driver renews active claims. It delivers one Small Talk message for each readiness epoch and harness incarnation.

A pool message closes when another agent wins the claim. Release or expiry creates a new readiness epoch and a new message.

The work queue is authoritative. A notification only tells an agent that the queue might contain new work.

The reconciler does not send periodic reminders. A future harness-stall policy can create a new explicit epoch after a measured timeout.

The MVP does not implement that harness-stall timeout.

## Standing runs and cancellation

All plan runs use the same state machine. There is no separate standing plan type.

An open plan becomes `standing` when it has no next step and has no explicit completion result.

An open plan still asserts its direct declarations. This rule lets a standing conversation plan keep its agent present.

`st3 codex` and `st3 claude` publish a deterministic zero-step standing plan. The first command for an agent starts one run.

An exact retry returns the same run. A later configuration change creates a new generation in that run.

The quick command returns the plan, plan run, run generation, and agent subjects.

A plan run does not stop because a controller deletes its runtime. The graph must publish cancellation.

```kdl
version 2
plan-run "RUN_ID" {
  cancellation "request-withdrawn" {
    reason "The request was withdrawn."
  }
}
```

Cancellation revokes active claims and cancels normal work. It then runs the adjacent `finally` graph.

Cancellation also cancels active descendant plan runs. Each descendant uses its own final phase.

The terminal state is `cancelled` after successful final work. A final failure makes the run failed.

After final work, st3 enters cleanup and stops every runtime owned by the plan run.

The run becomes terminal only after those runtime subjects report a stopped, absent, or exited state.

An exact repeated cancellation is idempotent. The old run and its immutable generations remain readable.

st3 sends a cancellation message to each active claimant. The message tells the agent to stop that step.

## Plan revisions

Revision authority comes from agent placement in the current generation.

- A direct agent in a step can revise that step subtree.
- A direct agent in a plan can revise the complete plan.
- A direct agent adjacent to plans can revise those plans.

The run requester can propose any revision. A work selector does not grant revision authority.

st3 checks the current generation. A candidate cannot add itself as an owner and use that new authority.

`revisions="human-only"` protects a plan or step. The protection is inherited by nested steps.

`revision-reviewer="person/NAME"` selects the human reviewer. The run requester is the reviewer when this property is absent.

All distinct reviewers for the changed paths must approve. Each approval names the exact proposal preview hash.

```sh
st3 work revise PLAN_RUN replacement.kdl \
  --as agent/RUN/worker \
  --reason "The generated source adds one verification step."

st3 work revision show PLAN_RUN
st3 work revision approve PROPOSAL PREVIEW_HASH --as person/reviewer
st3 work revision cancel PROPOSAL --as person/reviewer --reason "The request changed."
```

A run can have one pending proposal. A second proposal fails until the first proposal is applied or cancelled.

### Deferred declarative revision intent

A future KDL operation can propose a produced plan revision against one live plan run.

The operation should compile into the existing revision proposal claims. It must not create a second revision or generation model.

Publishing a plan revision alone must not move an active run. The declaration must identify the target run and exact revision.

A parent plan could target a linked child run. This would let a controller publish the parent plan from a shell heredoc.

The plan could then sequence the proposal and verify the successor generation with normal dependencies and gates.

A human-only approval must remain an external authorized claim. Publishing the controlling KDL must not imply that approval.

This direction is deferred. The first design must define target selection, authority, idempotency, cancellation, and failure behavior.

### Immutable generations

Each accepted revision creates one immutable successor generation. The stable plan-run subject points to the current generation.

The cutover transaction creates generation-specific step-run subjects. It also marks the old generation as superseded.

```sh
st3 work revision generations PLAN_RUN
st3 work revision generation RUN_GENERATION
```

st3 compares normalized step definition hashes. A changed step and every transitive dependent start without prior completion.

Every compatible state carries to the successor. Compatible claimed, working, or verifying work restarts in ready state.

The old generation remains readable. A late work action against it fails with `stale-run-generation`.

Plan and step members record their owner run and generation. The reconciler stops members left only in the superseded generation lineage.

A compatible member keeps the same run-local subject in the successor. A plan revision cannot move it to another plan run.

A cutover cancels active descendant plan runs that started from predecessor steps. `when-idle` also waits for claimed, working, or verifying descendant work before cutover.

The default `restart-active` cutover creates the successor after approval. Open work messages for the old generation close as superseded.

`revision-cutover="when-idle"` changes the run phase to `revision-draining`. Existing active work can settle, but no new work can claim.

The reconciler creates the successor after the active work reaches a stable state. Cancellation returns the run to its normal phase.

The run keeps its initial revision and root revision. Its current revision is the revision of its current generation.

## Immutable documents

`document` on a step always requires `doc/NAME@SHA256`.

```sh
st3 doc put request.md --as doc/project/request
st3 doc get doc/project/request@SHA256 --output request.md
st3 doc list doc/project/request
```

Bare document names can appear in an intent before preview. The preview resolves them to the current exact hash. Apply validates the bytes and binds that exact version.

## Preview, publish, and start

`st3 preview FILE` validates KDL, resolves documents, displays changes, returns subject tokens, and performs no write.

`st3 publish FILE --as ACTOR` repeats preview and applies the exact tokens. It never starts a plan run.

`st3 plan start PLAN --as ACTOR` publishes one plan-run declaration for the current ready revision. Add `--follow` to follow the run until it becomes terminal or standing.

`st3 plan show PLAN_RUN` reads one exact run. `st3 plan show PLAN` works only when that plan has exactly one nonterminal run.

The plan shortcut fails when it finds zero or multiple active runs. The error tells the caller to use an exact plan-run subject.

## Planning mode

Planning mode asks one durable Codex harness to author Markdown and KDL for review.

```sh
st3 planning start --id release-plan request.md \
  --workspace ./project \
  --as person/nathan \
  --model gpt-5.6-sol \
  --effort medium

st3 planning show SESSION
st3 planning preview SESSION
st3 planning revise SESSION feedback.md --as person/nathan
st3 planning approve SESSION PREVIEW_HASH --as person/nathan
st3 planning cancel SESSION --as person/nathan --reason "The request changed."
```

Planning can also prepare a revision for one current plan run:

```sh
st3 planning start --run PLAN_RUN request.md \
  --workspace ./project \
  --as person/nathan

st3 planning preview SESSION --variant compact
st3 planning preview SESSION --variant extended
st3 planning compare SESSION compact extended
st3 planning propose SESSION extended \
  --as person/nathan \
  --reason "The extended variant covers the discovered risk."
```

The planner uses this command:

```sh
st3 planning submit SESSION --variant compact --markdown PLAN.md --kdl plan.kdl
```

The session stores the request, feedback, Markdown, and KDL as immutable documents. Small Talk carries document references, not mutable file paths.

Each named candidate must contain exactly one ready plan with the requested ID.

A run-targeted session stores the exact source generation and an immutable run context document.

The session can hold multiple draft variants. Proposing one variant rejects a stale source generation.

Preview returns these review values:

- the candidate and plan revisions;
- a static dependency graph;
- the graph subject diff;
- warnings and blockers;
- exact subject tokens;
- one hash over the complete preview.

Revision invalidates the prior preview. Approval requires the current preview hash and current subject tokens.

Approval publishes the ready plan and one `planning-session.approved` claim. It does not start a run. Approval and cancellation stop the planner.

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
- `planning-session.approved` links an approved planning session to Markdown and KDL.
- `plan-run.created` and `plan-run.state` record stable run history.
- `run-generation.created`, `run-generation.superseded`, and `run-generation.state` record revision lineage.
- `revision-proposal.created`, `revision-proposal.approved`, `revision-proposal.cancelled`, and `revision-proposal.applied` record revision review.
- `step-run.carried`, `step-run.state`, and `step-run.retried` record generation-specific step history.
- `plan.produced` binds a generated plan to one producing attempt.
- `gate.requested` and `gate.result` record gate operations and evidence.
- `gate.requested` and `gate.result` record exact human gates.

Evidence is a list of claim IDs or immutable graph references that support a result. The evidence does not replace the gate. The gate definition says what must be decided; evidence records why the result is trustworthy.

## Eval contract

`st3 eval DIRECTORY` archives the explicit directory, posts staged documents, applies its version 2 intent, and starts the selected eval plan.

The planning-mode eval uses one real Codex planner. A controller waits on the event stream and directly approves the first valid candidate. Mechanical gates prove that the plan was hidden before approval, the preview graph and diff were rendered, the exact hash was approved, one ready plan was published, no run started, immutable documents were linked, the planner stopped, and the workspace did not change.

The planning variant and stale-generation paths are deterministic API tests. They do not spend a model run.

The run-generation revision eval proves an approved cutover, lineage, state carry-over, and automatic generation context. It uses no model run.
