# st3 plan graph feedback delta

This document records the review of external feedback on 2026-08-27.

It sits beside `plan-graph-ideas-2026-08-27.md`.

The earlier file remains a frozen conversation snapshot. This file records candidate changes.

These ideas are not the current st3 specification. The current parser does not accept the example KDL.

## Sources

The review compared two public gists:

- [The older st3 worked examples by Nathan](https://gist.github.com/myobie/65de911c7abf7a6c345ca985eabdaf05).
- [The language review and fleet mapping from Johannes's agent](https://gist.github.com/schickling-assistant/e800affeba7b558fb98370dffa888d13).

The feedback mixes three separate questions:

1. Does the old example KDL make sense?
2. Does st2 provide the fields used by those examples?
3. Would st3 solve the measured fleet problems?

Some conclusions move between those questions too quickly. The review still contains important design findings.

## Candidate changes

### Bind every judge to its inputs

A judge attempt must bind to these values:

- The plan run.
- The step run.
- The judge definition hash.
- The input frontier hash.
- The repository and resource revisions.
- The judge attempt.

A possible identity is:

```text
judge-run/
  PLAN_RUN/
  STEP_RUN/
  JUDGE_ID/
  DEFINITION_HASH/
  INPUT_FRONTIER_HASH
```

An unchanged judge definition must run again when a declared input changes.

An unrelated graph claim must not invalidate the result.

### Preserve observation outcomes

The evaluator must preserve these outcomes:

```text
satisfied
unsatisfied
indeterminate
error
```

The following conditions are not equivalent:

- A subject is absent.
- A field is absent.
- An observation is stale.
- An observation failed.
- A value has the wrong type.
- A present value does not match.

The operator view must show the exact condition.

A step can wait while a condition is unsatisfied or indeterminate. Its timeout can later produce a terminal result.

`empty` must fail closed. Every recorded incarnation needs explicit stopped, exited, or absent evidence.

### Give runtime outputs run-scoped roles

A step can declare outputs before it runs:

```kdl
produces {
  revision "candidate-revision"
  resource "pull-request" kind="vcs.pull-request"
}
```

The authored name is a logical role. The plan materializer creates a physical subject under the plan run.

```text
output/PLAN_RUN/STEP_ID/pull-request
```

An old run cannot satisfy a new run through a stale resource binding.

The current assignee can bind only the outputs of its claimed step.

An output binding records the concrete external ID, producer step run, actor, and evidence.

This model also gives runtime messages a predictable role.

### Expose the complete judge lifecycle

The graph should expose these judge states:

```text
not-eligible
waiting-for-input
queued
running
passed
failed
indeterminate
error
stale
```

The engine records `judge.requested`, `judge.started`, and `judge.result` claims.

`st3 doctor` should find these conditions:

- An eligible judge was never requested.
- A requested judge never started.
- A running judge stopped without a result.
- A result lacks its declared evidence.
- A result refers to stale inputs.

Built-in judges need executable negative tests.

An important custom judge should cite a calibration receipt that proves it can reject a known bad input.

### Support a human revision outcome

A human review supports these semantic outcomes:

```text
approved
rejected
revise
```

`revise` is non-terminal. It says that the plan or the review premise needs a change.

The decision creates a plan revision request. Independent branches continue.

An approval binds to the exact plan revision, step run, evidence frontier, and reviewed resource revision.

### Separate stable identity from prose

A step uses a stable machine ID:

```kdl
step "rename-base" {
  title "Rename the base package"
  goal "Rename the product while preserving runtime primitives."
}
```

The title and goal can change without changing the step identity.

Source order affects presentation only. It never creates identity or execution order.

### Put timeouts on work

A timeout is control flow. It is not evidence about the world.

```kdl
step "wait-for-required-checks" timeout="30m" {
  judges { }
}
```

A running judge can also have its own `time-limit`. The two limits have different purposes.

### Define evidence authority per field

Subject kind alone does not determine evidence quality.

A field schema should state:

- Which actor can produce the field.
- Which adapter observed the field.
- When the adapter observed it.
- Which revision the field describes.
- How long the observation remains current.

A reconciler-owned PID observation can be strong member evidence.

An agent's self-reported `idle` value is weak semantic evidence.

A worker completion claim means that the worker reports completion. It does not prove correctness.

Independent judges provide correctness evidence when the plan requires it.

### Use exact authored identities

The new grammar should not derive an agent identity from its host or punctuation.

```kdl
agent "sig.base" {
  host "local"
}
```

`agent/sig.base` is the exact normalized identity in this example.

The runtime injects `ST_AGENT`. The author does not repeat it in `env`.

The migration tool can expand st2 shorthand before it publishes new KDL.

### Reserve `@` for immutable revisions

A resource role can use a path:

```kdl
resource "release-work/pull-request"
```

An immutable reference can use `@REVISION_HASH`.

This convention avoids using `@` for both a role and a content pin.

## Findings that do not require a new core model

st3 can add message delivery and closure claims even though st2 did not have them.

A rare relay topology makes an old example unrepresentative. It does not make relay work invalid.

Mechanical liveness checks can detect many agent wedges. An LLM judge is not the only detector.

Token limits need separate input, output, and total meanings. Peak context and billed tokens are not interchangeable.

A content hash proves which bytes an agent received. It does not prove that the instructions match installed software.

st3 should support checkable requirements well. It should not claim that all behavioral rules become mechanical.

## Signal Rename plan example

This KDL combines the plan ideas with the feedback changes.

The example uses several proposed forms that still need grammar design.

### Proposed helper forms

`instance="per-run"` creates a new physical scope and member set for each plan run.

`scope/run` refers to the current physical run scope.

`output/STEP/NAME` is a run-relative logical output reference.

`inputs` binds a step to exact upstream outputs.

`produces` declares output roles before work starts.

`observe` requires evidence from one adapter and one maximum observation age.

`is-output` compares an observed field with an exact upstream output value.

`calibration` cites a receipt that proves a judge rejects a known bad fixture.

`finally #true` makes a step run after success, failure, cancellation, or void.

The plan run does not finish cleanup until every final step converges.

### Complete illustrative KDL

```kdl
scope "eval/signal-rename"
  instance="per-run"
  retention="temporary"
  change-policy="agent" {

  link "run-requires-supervisor" {
    from "scope/run"
    to "agent/sig.sup"
    required #true
    on-unreachable "void"
  }

  plan "signal-rename" state="ready" {
    input "task"
      document="doc/evals/signal-rename/task@f12083c4ec4a7b2d6d9af20a5acdd89663a31532f4a3cee10c43fe283f61f1de"

    step "materialize-fixture" timeout="3m" {
      title "Materialize the isolated workspace"

      subgraph {
        exec "materialize" {
          host "local"
          workspace "${EVAL_ROOT}"
          cwd "${EVAL_ROOT}"
          command "bash ./materialize.sh"
          restart "never"
        }
      }

      judges {
        observe "exec/materialize" via="reconciler/local" {
          field "exit_code" is 0
        }
      }
    }

    step "start-workers" timeout="5m" {
      title "Start the four agents"
      depends-on "materialize-fixture"

      subgraph {
        agent "sig.sup" {
          host "local"
          workspace "${EVAL_ROOT}/sup"

          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
            prompt "Read each ready assignment from st3. Claim it, update its nested plan, and publish evidence."
            args "--dangerously-bypass-approvals-and-sandbox"
          }
        }

        agent "sig.base" {
          host "local"
          workspace "${EVAL_ROOT}/base"

          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
            prompt "Read each ready assignment from st3. Work only in the declared base-package lane."
            args "--dangerously-bypass-approvals-and-sandbox"
          }
        }

        agent "sig.relay" {
          host "local"
          workspace "${EVAL_ROOT}/relay"

          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
            prompt "Read each ready assignment from st3. Work only in the declared relay lane."
            args "--dangerously-bypass-approvals-and-sandbox"
          }
        }

        agent "sig.hub" {
          host "local"
          workspace "${EVAL_ROOT}/hub"

          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
            prompt "Read each ready assignment from st3. Work only in the declared hub lane."
            args "--dangerously-bypass-approvals-and-sandbox"
          }
        }
      }
    }

    step "publish-base-transition" timeout="20m" {
      title "Publish the base rename with a temporary compatibility window"
      depends-on "start-workers"
      assigned-to "agent/sig.base"
      goal "Rename the base product first and preserve temporary compatibility for both consumers."

      plan "work" {
        step "inspect-the-base-package" { }

        step "separate-product-and-runtime-names" {
          depends-on "inspect-the-base-package"
        }

        step "add-the-beacon-name-and-temporary-alias" {
          depends-on "separate-product-and-runtime-names"
        }

        step "run-the-base-tests" {
          depends-on "add-the-beacon-name-and-temporary-alias"
        }

        step "publish-the-base-revision" {
          depends-on "run-the-base-tests"
        }
      }

      produces {
        revision "base-transition-revision"
      }
    }

    step "migrate-relay" timeout="20m" {
      title "Migrate the relay consumer"
      depends-on "publish-base-transition"
      assigned-to "agent/sig.relay"
      goal "Use the Beacon product names and preserve AbortSignal, controller.signal, and SIGTERM."

      inputs {
        revision "base" from="output/publish-base-transition/base-transition-revision"
      }

      plan "work" {
        step "inspect-the-relay-package" { }

        step "change-only-product-names" {
          depends-on "inspect-the-relay-package"
        }

        step "prove-the-runtime-primitives-remain" {
          depends-on "change-only-product-names"
        }

        step "run-the-relay-tests" {
          depends-on "prove-the-runtime-primitives-remain"
        }
      }

      produces {
        revision "relay-revision"
      }
    }

    step "migrate-hub" timeout="20m" {
      title "Migrate the hub consumer"
      depends-on "publish-base-transition"
      assigned-to "agent/sig.hub"
      goal "Use the Beacon package and resource scheme without changing runtime primitives."

      inputs {
        revision "base" from="output/publish-base-transition/base-transition-revision"
      }

      plan "work" {
        step "inspect-the-hub-package" { }

        step "change-the-package-and-resource-scheme" {
          depends-on "inspect-the-hub-package"
        }

        step "run-the-hub-tests" {
          depends-on "change-the-package-and-resource-scheme"
        }
      }

      produces {
        revision "hub-revision"
      }
    }

    step "update-root-and-config" timeout="20m" {
      title "Update the workspace root and product configuration"
      depends-on "publish-base-transition"
      assigned-to "agent/sig.sup"
      goal "Update only the root and config lane for the Beacon cutover."

      inputs {
        revision "base" from="output/publish-base-transition/base-transition-revision"
      }

      plan "work" {
        step "update-the-root-workspaces" { }
        step "update-the-product-config" { }

        step "run-the-root-checks" {
          depends-on "update-the-root-workspaces" "update-the-product-config"
        }
      }

      produces {
        revision "root-config-revision"
      }
    }

    step "close-base-compatibility" timeout="15m" {
      title "Remove the temporary base compatibility window"
      depends-on "migrate-relay" "migrate-hub"
      assigned-to "agent/sig.base"
      goal "Remove every legacy product alias after both consumers use Beacon."

      inputs {
        revision "base-transition" from="output/publish-base-transition/base-transition-revision"
        revision "relay" from="output/migrate-relay/relay-revision"
        revision "hub" from="output/migrate-hub/hub-revision"
      }

      plan "work" {
        step "remove-the-legacy-package-alias" { }
        step "remove-the-legacy-protocol" { }

        step "prove-the-base-cutover-is-clean" {
          depends-on "remove-the-legacy-package-alias" "remove-the-legacy-protocol"
        }
      }

      produces {
        revision "base-final-revision"
      }
    }

    step "integrate-and-open-pull-request" timeout="20m" {
      title "Integrate the exact lane revisions and open a pull request"
      depends-on "close-base-compatibility" "update-root-and-config"
      assigned-to "agent/sig.sup"
      goal "Integrate every declared lane revision, push one candidate branch, and bind its pull request."

      inputs {
        revision "base" from="output/close-base-compatibility/base-final-revision"
        revision "relay" from="output/migrate-relay/relay-revision"
        revision "hub" from="output/migrate-hub/hub-revision"
        revision "root-config" from="output/update-root-and-config/root-config-revision"
      }

      plan "work" {
        step "integrate-the-declared-revisions" { }

        step "run-the-complete-suite" {
          depends-on "integrate-the-declared-revisions"
        }

        step "push-the-candidate-branch" {
          depends-on "run-the-complete-suite"
        }

        step "open-the-pull-request" {
          depends-on "push-the-candidate-branch"
        }

        step "bind-the-candidate-and-pull-request" {
          depends-on "open-the-pull-request"
        }
      }

      produces {
        revision "candidate-revision"
        resource "pull-request" kind="vcs.pull-request"
      }
    }

    step "held-out-eval-passes" timeout="30m" {
      title "Run the held-out Signal Rename evaluation"
      depends-on "integrate-and-open-pull-request"

      inputs {
        checkout "candidate" from="output/integrate-and-open-pull-request/candidate-revision"
      }

      judges {
        judge "package-lane-isolation" input="candidate" {
          exec "bash ./judges/isolation.sh"
          time-limit "5m"
          calibration "calibration/signal-rename/isolation@REVISION_HASH"
        }

        judge "all-package-suites-pass" input="candidate" {
          exec "bash ./judges/suite-green.sh"
          time-limit "10m"
          calibration "calibration/signal-rename/suite-green@REVISION_HASH"
        }

        judge "the-product-rename-is-complete" input="candidate" {
          exec "bash ./judges/rename.sh"
          time-limit "5m"
          calibration "calibration/signal-rename/rename@REVISION_HASH"
        }

        judge "runtime-primitives-remain-intact" input="candidate" {
          exec "bash ./judges/primitive.sh"
          time-limit "5m"
          calibration "calibration/signal-rename/primitive@REVISION_HASH"
        }

        judge "the-renamed-stack-works-end-to-end" input="candidate" {
          exec "bash ./judges/e2e.sh"
          time-limit "5m"
          calibration "calibration/signal-rename/e2e@REVISION_HASH"
        }

        judge "the-cutover-sequence-and-result-are-sound" type="llm" input="candidate" {
          harness "codex" {
            model "gpt-5.6-sol"
            effort "medium"
          }
          tools "shell" "git"
          tokens input=131072 output=8192 total=196608
          time-limit "10m"
          prompt "Inspect the exact candidate revision. Verify the base-first sequence, all package lanes, the final clean cutover, and preserved runtime primitives."
          calibration "calibration/signal-rename/semantic@REVISION_HASH"
        }
      }
    }

    step "required-pull-request-checks-pass" timeout="30m" {
      title "Wait for the required pull-request checks"
      depends-on "integrate-and-open-pull-request"

      inputs {
        revision "candidate" from="output/integrate-and-open-pull-request/candidate-revision"
        resource "pull-request" from="output/integrate-and-open-pull-request/pull-request"
      }

      judges {
        observe "output/integrate-and-open-pull-request/pull-request"
          via="observer/github"
          max-age="2m" {
          present
          field "state" is "open"
          field "head.revision" is-output "output/integrate-and-open-pull-request/candidate-revision"
          field "checks.required.status" is "success"
        }
      }
    }

    step "pull-request-is-green-and-ready-for-review" timeout="10m" {
      title "Confirm that the exact candidate pull request is review-ready"
      depends-on "held-out-eval-passes" "required-pull-request-checks-pass"

      inputs {
        revision "candidate" from="output/integrate-and-open-pull-request/candidate-revision"
        resource "pull-request" from="output/integrate-and-open-pull-request/pull-request"
        evidence "held-out" from="step/held-out-eval-passes"
        evidence "required-checks" from="step/required-pull-request-checks-pass"
      }

      judges {
        observe "output/integrate-and-open-pull-request/pull-request"
          via="observer/github"
          max-age="2m" {
          field "state" is "open"
          field "draft" is #false
          field "head.revision" is-output "output/integrate-and-open-pull-request/candidate-revision"
          field "mergeable" is #true
        }
      }
    }

    step "nathan-reviews-pull-request" timeout="7d" {
      title "Nathan reviews the green pull request"
      depends-on "pull-request-is-green-and-ready-for-review"

      inputs {
        revision "candidate" from="output/integrate-and-open-pull-request/candidate-revision"
        resource "pull-request" from="output/integrate-and-open-pull-request/pull-request"
        evidence "held-out" from="step/held-out-eval-passes"
        evidence "green" from="step/pull-request-is-green-and-ready-for-review"
      }

      judges {
        human "person/nathan" {
          review "output/integrate-and-open-pull-request/pull-request"
          outcomes "approved" "rejected" "revise"
        }
      }
    }

    step "merge-the-approved-pull-request" timeout="15m" {
      title "Merge the approved candidate"
      depends-on "nathan-reviews-pull-request"
      assigned-to "agent/sig.sup"
      goal "Merge only the candidate revision that received the green checks and human approval."

      inputs {
        revision "candidate" from="output/integrate-and-open-pull-request/candidate-revision"
        resource "pull-request" from="output/integrate-and-open-pull-request/pull-request"
        decision "approval" from="step/nathan-reviews-pull-request"
      }

      produces {
        revision "merge-revision"
      }

      judges {
        observe "output/integrate-and-open-pull-request/pull-request"
          via="observer/github"
          max-age="2m" {
          field "state" is "merged"
          field "head.revision" is-output "output/integrate-and-open-pull-request/candidate-revision"
        }
      }
    }

    step "report-the-result" timeout="5m" {
      title "Report the merged result"
      depends-on "merge-the-approved-pull-request"
      assigned-to "agent/sig.sup"
      goal "Report the decomposition, sequence, evidence, pull request, and merge revision."

      inputs {
        resource "pull-request" from="output/integrate-and-open-pull-request/pull-request"
        revision "merge" from="output/merge-the-approved-pull-request/merge-revision"
      }

      produces {
        message "final-report" to="person/morgan"
      }
    }

    step "stop-the-run-scope" timeout="10m" {
      title "Stop every temporary run member"
      finally #true

      subgraph {
        scope "run" { stop }
      }
    }
  }
}
```

## Example execution shape

The main dependency shape is:

```text
materialize-fixture
└── start-workers
    └── publish-base-transition
        ├── migrate-relay ────────────┐
        ├── migrate-hub ──────────────┴── close-base-compatibility ─┐
        └── update-root-and-config ─────────────────────────────────┴── integrate-and-open-pull-request
                                                                        ├── held-out-eval-passes ───────────────┐
                                                                        └── required-pull-request-checks-pass ──┴── pull-request-is-green-and-ready-for-review
                                                                                                                      └── nathan-reviews-pull-request
                                                                                                                          └── merge-the-approved-pull-request
                                                                                                                              └── report-the-result

stop-the-run-scope runs as a final step after every terminal outcome.
```

The relay, hub, and root changes run concurrently after the base transition exists.

The base removes compatibility only after both consumers finish.

The integration step consumes exact revision outputs from every lane.

The pull request output belongs to this plan run. An old pull request cannot satisfy the judges.

The held-out eval and the required GitHub checks run as parallel graph branches.

The GitHub observation must describe the exact candidate head revision.

The human approval binds to the same candidate and evidence frontier.

If the pull-request head changes, the green evidence and human approval become stale.

An approved head can merge. A revised head must pass the held-out eval, GitHub checks, and human review again.

`revise` starts the plan revision workflow without failing unrelated work.

## Questions that this example exposes

The example suggests several grammar decisions:

- Does every plan run receive an automatic run scope?
- Does `finally #true` give enough cleanup control?
- Should an output role have one immutable value per step attempt?
- How does a new step attempt supersede old output bindings?
- Which output types should st3 define in version one?
- Should `observe` be a judge group or a separate evidence declaration?
- How does an adapter prove that a pull-request observation describes the current head?
- Which calibration receipts are mandatory for release and eval judges?

These questions belong in later design work. They do not change the dependency structure shown here.
