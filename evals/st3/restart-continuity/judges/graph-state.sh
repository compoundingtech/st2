#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
work="$(env -u ST_AGENT st3 work ls --all --json)"
run="plan-run/$ST_PLAN_RUN"

completed_steps=(
  start-team
  process-before-restart
  process-before-restart/work/inspect-durable-state
  process-before-restart/work/process-item-1
  process-before-restart/work/process-item-2
  process-before-restart/work/publish-pre-restart-revision
  inject-cold-restart
  process-after-restart
  process-after-restart/work/inspect-recovered-state
  process-after-restart/work/process-item-3
  process-after-restart/work/process-item-4
  process-after-restart/work/verify-complete-batch
  process-after-restart/work/publish-batch-revision
  process-after-restart/work/report-to-supervisor
  verify-and-confirm
  verify-and-confirm/work/inspect-graph-history
  verify-and-confirm/work/verify-ledger-read-only
  verify-and-confirm/work/confirm-requester
)

for step in "${completed_steps[@]}"; do
  count="$(jq \
    --arg run "$run" \
    --arg step "$step" \
    '[.[] | select(.run == $run and .step == $step and .status == "completed")] | length' \
    <<<"$work")"
  test "$count" -eq 1
done

while read -r name kind state_name; do
  subject="resource/plan-run/$ST_PLAN_RUN/$name"
  status="$(st3 inspect "$subject" --json)"
  jq -e --arg kind "$kind" --arg state "$state_name" \
    '.status.subjects[0].actual | (.fields // .)
      | (.kind == $kind) and (.state == $state)' \
    <<<"$status" >/dev/null
  bindings="$(st3 trace "$subject" --json --limit 20 \
    | jq -s '[.[] | select(.kind == "resource.binding")] | length')"
  test "$bindings" -eq 1
done <<'PRODUCTS'
pre-restart vcs.revision published
restart cold-restart injected
batch vcs.revision published
worker-report message.receipt published
verification message.receipt published
PRODUCTS

echo "PASS: the graph records every work step and one binding for each required product"
