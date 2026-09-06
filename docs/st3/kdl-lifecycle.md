# st3 KDL lifecycle

Status: current authoring and publication contract.

## One publication rule

Every KDL document starts with `version 2`. All declarations follow that line directly.

`st3 publish` is an atomic upsert. The daemon first parses, resolves document references, validates, and checks the current subject heads. It then applies every change in one transaction. One failure rejects the full publication.

Omission has no effect. Removing a declaration from a later file does not stop or delete its existing graph state. Retirement, cancellation, and refresh are explicit declarations.

The removed wrapper keyword is an error. There is no compatibility form.

```sh
st3 preview release.kdl
st3 publish release.kdl --as person/operator
```

The JSON publication receipt contains the resolved KDL, subject changes, accepted operations, claim IDs, and final store index.

## Definitions do not start work

This publication creates or updates one immutable plan revision. It does not start a run.

```kdl
version 2

plan "release" state="ready" {
  goal "Publish a verified release decision."
  completion { when "all-steps-exhausted" }

  agent "builder" {
    workspace "${ST_WORKSPACE}/builder" create=#true
    harness "codex" {
      prompt "Claim the available release work and complete it."
    }
  }

  step "build" {
    assigned-to "agent/${ST_PLAN_RUN}/builder"
    goal "Build and test the release."
  }
}
```

Direct runtime declarations in a plan belong to each run of that plan. Direct declarations in a step become desired when that step activates. They stop being desired when their owner run ends or a successor generation removes them.

## Starting a plan run

A plan run names one exact plan revision. A custom run ID can be a readable operational name. The helper generates a UUIDv7 suffix when no ID is supplied.

```kdl
version 2

plan-run "release/demo" {
  plan "plan/release@0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  workspace "/work/release"
  requester "person/operator"
  input "target" "demo"
}
```

The plan and plan-run can be in one atomic publication when the run names the revision in that same file. They can also be two publications. The second form is useful when a person wants to review the plan definition before starting it.

The helper reads the current ready revision and publishes the exact run declaration:

```sh
st3 plan start release \
  --id release/demo \
  --workspace /work/release \
  --input target=demo \
  --as person/operator
```

Use `--follow` to wait for a terminal or standing run. Use `--print-kdl` to inspect or save the generated declaration without publishing it.

The default plan capacity is one active run. `concurrent-runs` removes the limit. `concurrent-runs max=4` sets a limit. A capacity error rejects the full publication.

## Standing work

A plan completes only through `completion`. A plan with no completion block remains open after it exhausts its current work. The run is then standing.

This rule supports a long-lived conversation agent without a separate plan type. The open plan continues to assert the agent declaration. New plan revisions can add work. A named revision moves the run to a successor generation.

## Named operations

An operational block has an ID inside its owner subject.

```kdl
version 2

plan-run "release/demo" {
  cancellation "operator-stop" {
    reason "the release was withdrawn"
  }
}
```

The identity is the owner subject, operation kind, and operation ID. Repeating the exact content is a no-op. Reusing that ID with changed content is an error. A retry with new intent needs a new operation ID.

Failed static validation does not record the operation. The caller can correct the document and publish it with a new ID. An accepted asynchronous action can later record a durable failure claim.

### Revision

Publish the candidate plan revision first. Then publish an operation against the run's exact current generation.

```kdl
version 2

plan-run "release/demo" {
  revision "add-security-gate" {
    plan "plan/release@fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
    from "run-generation/01990000000070008000000000000000"
    reason "the build exposed a new security boundary"
  }
}
```

An authorized revision with immediate cutover creates one successor generation atomically. Compatible completed work moves forward. Changed or dependent work becomes available again. The old generation becomes superseded.

A human-protected revision creates a durable revision proposal. The named operation remains accepted and idempotent. A reviewer approves the exact preview through the observed review command. The approval then creates the successor generation.

`st3 work revise RUN FILE --reason TEXT` publishes the candidate and the named revision. `--print-kdl` prints only the operation and tells the operator which candidate file to publish first.

### Runtime reset

```kdl
version 2

plan-run "release/demo" {
  reset "retry-builder" {
    runtime "agent/builder"
    from "run-generation/01990000000070008000000000000000"
    reason "the operator corrected the runtime dependency"
  }
}
```

The daemon expands a run-local runtime ID inside the owner run. The generation fence prevents an old reset from affecting a replacement generation.

### Resource refresh

```kdl
version 2

resource "release-pr" {
  refresh "after-push" {
    timeout "30s"
  }
}
```

The resource must already have an active observer. `st3 resource refresh` publishes this form and waits for the exact observer attempts. An unchanged observation is a successful refresh.

## Planning a new plan

Planning uses an immutable request document and a declarative planning session.

```sh
st3 doc put request.md --as doc/planning/release/request
st3 planning start --id release request.md \
  --workspace /work/release \
  --as person/operator
```

The helper stores the request first. It then publishes a declaration like this:

```kdl
version 2

planning-session "planning/release/01990000000070008000000000000000" {
  plan "release"
  request "doc/planning/release/request@REQUEST_SHA256"
  workspace "/work/release"
  requester "person/operator"
  planner "codex" {}
}
```

The session creates a session-scoped Codex planner with a bounded runtime ID. Candidate submission is an observed result, so it is not authored in KDL. Candidate submission creates an exact preview automatically. A blocked preview stays durable for review.

Human approval is also observed input. It publishes the approved plan revision but does not start it. Approval and cancellation stop the session planner. Repeating either terminal action repairs a missing planner stop. The operator starts an approved new plan separately with `st3 plan start`.

`st3 planning start --print-kdl` does not store the request. It prints the required `st3 doc put` command and the planning-session KDL.

### Review or resume a planning session

The planning session and its document references are durable graph state. A reviewer can continue from another client after the state replicates.

```sh
st3 planning show planning/release/SESSION_ID
st3 planning preview planning/release/SESSION_ID --variant default
st3 planning revise planning/release/SESSION_ID feedback.md --as person/operator
st3 planning approve planning/release/SESSION_ID PREVIEW_HASH --as person/operator
```

`show` returns the current candidate and preview. `revise` stores the exact feedback document and publishes a named feedback operation. `approve` accepts only the current preview hash. It cannot approve a replaced candidate.

Use `st3 planning cancel SESSION --reason TEXT --as ACTOR` to end an unwanted session. Use `--print-kdl` to inspect the cancellation declaration before publication.

## Revising a plan through planning mode

Use `--run` to bind the session to one exact run generation.

```sh
st3 planning start --run plan-run/release/demo feedback.md \
  --workspace /work/release \
  --as person/operator
```

The declaration contains both `target-run` and `target-generation`. If the run moves before publication, the planning session is rejected as stale.

Feedback reopens an existing session with a named operation:

```kdl
version 2

planning-session "planning/release/01990000000070008000000000000000" {
  feedback "clarify-security-gate" {
    document "doc/planning/release/feedback@FEEDBACK_SHA256"
    variant "default"
  }
}
```

The exact feedback document is stored first. The session returns to the planner and replaces the prior preview.

Approval of a targeted planning preview publishes the candidate plan and creates its revision proposal. When the same person is the required revision reviewer, that one approval counts at both boundaries.

## Human gates

A human gate is declared with the plan. Its decision is observed input, not authored intent.

```kdl
gate "the operator approves deployment" type="human" {
  reviewer "person/operator"
  question "Deploy this exact result?"
  review "resource/release-decision"
}
```

The plan pauses at the gate. A later review command records the decision against the exact gate request. Editing and republishing the plan does not forge a decision.

```sh
st3 review approve resource/release-decision \
  --actor person/operator \
  --reason "the exact release result is accepted"
```

## Cancellation and cleanup

Cancellation is explicit and additive. Omission never cancels a run.

```kdl
version 2

plan-run "release/demo" {
  cancellation "withdraw-release" {
    reason "the requester withdrew the release"
  }
}
```

Cancellation revokes active work claims, enters adjacent `finally` work, and cascades to descendant plan runs. The run becomes terminal only after its owned runtimes stop.

A planning session uses the same noun:

```kdl
version 2

planning-session "planning/release/01990000000070008000000000000000" {
  cancellation "operator-stop" {
    reason "planning is no longer needed"
  }
}
```

## Print-only helpers

Intent helpers support `--print-kdl`. Print-only mode performs no publication and no document upload.

The main helpers are:

- `st3 codex --print-kdl` and `st3 claude --print-kdl`;
- `st3 exec --print-kdl`;
- `st3 message send --print-kdl` and `st3 message reply --print-kdl`;
- `st3 resource watch`, `unwatch`, and `refresh` with `--print-kdl`;
- `st3 runtime reset --print-kdl`;
- `st3 planning start`, `revise`, and `cancel` with `--print-kdl`;
- `st3 work revise --print-kdl`;
- `st3 eval --print-kdl`.

The eval helper prints the resolved eval plan. Eval bundle upload remains a packaging boundary because it transfers the complete fixture workspace before it publishes and starts the eval plan.

## Publication failure boundaries

Static failures reject the full publication. Examples include an unknown field, a missing exact document, a stale subject head, a stale generation, an immutable operation ID mismatch, a missing plan revision, or a capacity violation.

An accepted operation can cause later runtime work. A later start, stop, observer, delivery, or gate failure is a durable claim. It does not roll back the accepted intent.

This split keeps authored intent atomic and keeps real-world effects observable.

The [lifecycle eval receipts](kdl-lifecycle-eval-receipts.md) record the exact paid runs, discovered defects, fixes, and retained continuation proof.
