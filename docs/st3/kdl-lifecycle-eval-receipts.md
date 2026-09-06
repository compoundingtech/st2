# st3 KDL lifecycle eval receipts

Date: 2026-09-06.

Exactly two paid `st3 eval` invocations were made. Neither invocation was retried.

## Paid invocation 1: new-plan planning

Eval: `evals/st3/planning-mode`.

Run: `plan-run/452c6da44679c54a7529cfb0f858c626`.

Result: failed.

The generated planner runtime name had 105 bytes. Its Unix socket path exceeded the Linux kernel limit under a normal state directory. The planner did not start, and the eval step expired.

The implementation now derives a stable 20-hex planner suffix from the complete planning-session subject. The resulting runtime name has 28 bytes. A deterministic test checks stability, separation, and the 32-byte upper bound.

## Paid invocation 2: targeted live-run revision

Eval: `evals/st3/run-generation-revision`.

Run: `plan-run/c19d0460309fbc265d85466957154039`.

Result: failed because the eval assertion was wrong.

The compact planner started and submitted candidate revision 1 after 12 minutes. The candidate had no preview blocker. The preview graph contained `stable`, `changed`, and `generation-environment` in the required positions.

The controller expected step detail in the one-line plan diff. The valid diff was `update plan/generation-proof`, so the controller exited with code 1. The assertion now checks the plan update. The graph assertion continues to check the step details.

## Continuation of paid invocation 2

The existing candidate was continued without another model call and without another eval invocation. The requester approved preview `c2a0e17227d5a5cfbb9b545bf4ddfadb21332143f8bf675c8341bbd8c305f559`.

The approval published revision `6a07a750bafa431d922734e26585be97433a37ad13fe1779c04a0cd31cd92eb1`. It replaced generation `run-generation/01a0775e1cd67a22a1e810eaf792dcd5` with `run-generation/5bfe1432af60dec3b47ecbeb71fd7db5`.

The first generation became `superseded`. The successor named it as the predecessor. The unchanged `stable` step carried its completed state. The changed step ran again. The new generation environment gate passed. The successor run reached `completed`.

The continuation also exposed a planner teardown gap. Approval recorded the session result but did not stop a planner created by a direct planning-session declaration. The implementation now publishes an internal stop for that planner. A terminal-action replay repairs a missing stop.

After a daemon restart, the exact approval was replayed against the retained state. The adopted planner changed to desired kind `stop` and reached actual status `stopped` with no graph gap.

## Deterministic proof

The deterministic suite covers atomic planning-session creation, bounded planner IDs, exact target context, approval publication, successor generations, state carry-forward, and planner teardown. The complete workspace suite passes when the documented external OTLP binary check is skipped.
