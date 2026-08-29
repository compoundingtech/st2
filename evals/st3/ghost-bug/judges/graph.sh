#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the judged plan run}"
run="plan-run/$PLAN_RUN"
work="$(env -u ST_AGENT st3 work ls --all --json)"

steps=(
  start-team
  delegate-debug-brief
  delegate-debug-brief/work/read-bug-report
  delegate-debug-brief/work/send-debug-brief
  diagnose-and-fix
  diagnose-and-fix/work/read-debug-brief
  diagnose-and-fix/work/reproduce-option-leak
  diagnose-and-fix/work/identify-root-cause
  diagnose-and-fix/work/add-red-regression-test
  diagnose-and-fix/work/implement-smallest-fix
  diagnose-and-fix/work/verify-regression-and-suite
  diagnose-and-fix/work/publish-fix-revision
  diagnose-and-fix/work/report-fix-evidence
  verify-and-confirm
  verify-and-confirm/work/read-worker-report
  verify-and-confirm/work/verify-fix-read-only
  verify-and-confirm/work/confirm-requester
)

for step in "${steps[@]}"; do
  jq -e --arg run "$run" --arg step "$step" \
    'any(.[]; .run == $run and .step == $step and .status == "completed")' \
    <<<"$work" >/dev/null
done

while read -r name kind; do
  st3 inspect "resource/plan-run/$PLAN_RUN/$name" --json \
    | jq -e --arg kind "$kind" '.status.subjects[0].actual | (.fields // .) | .kind == $kind and .state == "published"' >/dev/null
done <<'PRODUCTS'
debug-brief message.receipt
fix-revision vcs.revision
worker-report message.receipt
final-confirmation message.receipt
PRODUCTS

echo "PASS: the graph records the complete Ghost Bug plan and products"
