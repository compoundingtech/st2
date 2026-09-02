#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
run="plan-run/$ST_PLAN_RUN"
work="$(env -u ST_AGENT st3 work ls --all --json)"

steps=(
  start-team
  delegate-license-change
  delegate-license-change/work/read-request
  delegate-license-change/work/send-worker-brief
  implement-license-change
  implement-license-change/work/read-license-brief
  implement-license-change/work/inspect-license-surface
  implement-license-change/work/apply-mit-license
  implement-license-change/work/verify-license-change
  implement-license-change/work/publish-license-revision
  implement-license-change/work/report-worker-result
  verify-and-confirm
  verify-and-confirm/work/read-worker-report
  verify-and-confirm/work/verify-worker-revision-read-only
  verify-and-confirm/work/confirm-requester
)

for step in "${steps[@]}"; do
  jq -e --arg run "$run" --arg step "$step" \
    'any(.[]; .run == $run and .step == $step and .status == "completed")' \
    <<<"$work" >/dev/null
done

while read -r name kind; do
  st3 inspect "resource/plan-run/$ST_PLAN_RUN/$name" --json \
    | jq -e --arg kind "$kind" '.status.subjects[0].actual | (.fields // .) | .kind == $kind and .state == "published"' >/dev/null
done <<'PRODUCTS'
license-brief message.receipt
license-revision vcs.revision
worker-report message.receipt
final-confirmation message.receipt
PRODUCTS

echo "PASS: the graph records the complete License MIT plan and products"
