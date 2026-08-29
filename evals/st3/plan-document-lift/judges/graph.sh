#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the root plan run}"
root="plan-run/$PLAN_RUN"
producer="step-run/$PLAN_RUN/lift-plan-document"
publisher="$producer/work/publish-ready-graph-plan"
consumer="step-run/$PLAN_RUN/execute-lifted-plan"
work="$(env -u ST_AGENT st3 work ls --all --json)"

for step in \
  start-planner \
  lift-plan-document \
  lift-plan-document/work/read-exact-plan-document \
  lift-plan-document/work/author-complete-graph-plan \
  lift-plan-document/work/publish-ready-graph-plan \
  execute-lifted-plan; do
  jq -e --arg run "$root" --arg step "$step" \
    'any(.[]; .run == $run and .step == $step and .status == "completed")' \
    <<<"$work" >/dev/null
done

output="$(st3 trace "$producer" --json --limit 100 | jq -s '[.[] | select(.kind == "plan.produced")] | last')"
plan="$(jq -r '.body.fields.plan' <<<"$output")"
revision="$(jq -r '.body.fields.revision' <<<"$output")"
output_index="$(jq -r '.store_index' <<<"$output")"
test "$plan" = "plan/eval/plan-document-lift/work"
test "${#revision}" -eq 64

publisher_claim="$(st3 trace "$publisher" --json --limit 100 | jq -s '[.[] | select(.kind == "work.claim")] | last | .store_index')"
publisher_complete="$(st3 trace "$publisher" --json --limit 100 | jq -s '[.[] | select(.kind == "work.complete")] | last | .store_index')"
test "$output_index" -gt "$publisher_claim"
test "$output_index" -lt "$publisher_complete"

child_runs="$(jq -r --arg root "$root" '[.[] | select(.run != $root) | .run] | unique | .[]' <<<"$work")"
test "$(grep -c . <<<"$child_runs")" -eq 1
child="$(head -n 1 <<<"$child_runs")"

created="$(st3 trace "$child" --json --limit 100 | jq -s '[.[] | select(.kind == "plan-run.created")] | first')"
test "$(jq -r '.body.fields.plan' <<<"$created")" = "$plan"
test "$(jq -r '.body.fields.revision' <<<"$created")" = "$revision"
test "$(jq -r '.body.fields.root_plan_run' <<<"$created")" = "$root"
test "$(jq -r '.body.fields.parent_step_run' <<<"$created")" = "$consumer"
test "$(jq -r '.store_index' <<<"$created")" -gt "$output_index"

for step in inspect-inventory write-result verify-result publish-result; do
  jq -e --arg run "$child" --arg step "$step" \
    'any(.[]; .run == $run and .step == $step and .status == "completed" and .assignee == "agent/pdl.agent" and (.title | length > 0) and (.goal | length > 0))' \
    <<<"$work" >/dev/null
done

published="$(st3 trace plan/eval/plan-document-lift/work --json --limit 100 | jq -s --arg revision "$revision" '[.[] | select(.kind == "plan.published" and .body.revision == $revision)] | first')"
jq -e '
  .body.display_order == ["inspect-inventory", "write-result", "verify-result", "publish-result"]
  and .body.steps["write-result"].dependencies[0].step == "inspect-inventory"
  and .body.steps["verify-result"].dependencies[0].step == "write-result"
  and .body.steps["publish-result"].dependencies[0].step == "verify-result"
' <<<"$published" >/dev/null

st3 inspect "resource/$root/plan-result" --json \
  | jq -e '.status.subjects[0].actual | (.fields // .) | .kind == "document.result" and .state == "published"' >/dev/null

echo "PASS: st3 used the exact attempt-bound plan output after publication"
