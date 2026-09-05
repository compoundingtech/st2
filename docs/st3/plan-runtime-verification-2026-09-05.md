# st3 plan runtime verification

This report records the plan runtime design verification from 2026-09-05.

## Code verification

The st3 package passes 155 tests.

The release build succeeds.

The source tree passes `cargo fmt` and `git diff --check`.

The tests cover plan ownership, inputs, selectors, standing runs, generations, planning, and cleanup.

The tests also cover failed runtime starts and cancellation races.

## Model-free evals

All 12 model-free evals pass with the exact release binary.

The run used a fresh daemon state.

Plan Inputs passed with one exact text input and one exact resource claim input.

Run Generation Revision passed with the required generation lineage.

The network evals wait for terminal plan-run cleanup and remove only their isolated PTY records.

A repeated cleanup proof found no process under either test root after both evals completed.

## Model-backed evals

Eight model-backed evals completed with all authored gates green.

These evals were Claude Skill Inheritance, Ghost Bug, License MIT, Mixed Worker Pool, Poisoned Pull Request, Restart Continuity, Test Writing, and Weird Git Setup.

Four evals completed their model work but found fixture errors after that work.

Fork in the Road used an email allow-list for an owner check.

The corrected gate uses the exact Git author name. All seven retained gates pass.

Plan Document Lift constructed step subjects from a plan run ID instead of its generation ID.

The corrected graph gate passes against the retained completed child plan.

Signal Rename used the removed `ST3_WORKSPACE` variable in one shared judge helper.

The corrected semantic evidence passes all five mechanical checks and the required claim order.

Planning Mode assumed that the planner submitted the `default` variant.

The corrected controller selects the submitted variant. The retained candidate passes every controller assertion.

Planning approval also stopped its planner after the cancellation race fix.

## Budget

This work used the 19 remaining paid eval attempts.

The total is 25 attempts, including the six earlier attempts.

No additional paid run is required for a source change in this branch.

The four corrected fixtures need fresh end-to-end receipts in a later budget if release policy requires them.
