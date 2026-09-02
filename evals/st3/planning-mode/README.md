# Planning Mode

This paid st3 eval proves the first planning-mode workflow with one native Codex planner.

The controller starts a durable planning session. It waits on the planning-session event stream until the planner submits Markdown and KDL documents. It then renders the static graph and graph diff and directly approves the exact preview hash.

Mechanical gates prove these boundaries:

- No plan is published before approval.
- The preview shows the explicit `inspect` to `verify` dependency.
- Approval publishes exactly one ready plan.
- Approval does not start a plan run.
- The published plan links the immutable Markdown and KDL documents.
- The planner stops after approval.
- The planning workspace does not change.

The revision path and stale-preview refusal are deterministic API tests. This paid eval uses direct approval so the model budget measures planning, not a forced rewrite.

Run it with `st3 eval ./evals/st3/planning-mode`.
