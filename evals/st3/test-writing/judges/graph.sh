#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
work="$(env -u ST_AGENT st3 work ls --all --json)"
run="plan-run/$ST_PLAN_RUN"

completed_steps=(
  start-team
  prepare-test-brief
  prepare-test-brief/work/inspect-module-read-only
  prepare-test-brief/work/send-test-brief
  write-regression-suite
  write-regression-suite/work/read-test-brief
  write-regression-suite/work/test-letter-boundaries
  write-regression-suite/work/test-gpa-and-summary
  write-regression-suite/work/run-complete-suite
  write-regression-suite/work/publish-test-revision
  write-regression-suite/work/report-test-result
  verify-test-suite
  verify-test-suite/work/read-developer-report
  verify-test-suite/work/verify-read-only
  verify-test-suite/work/confirm-requester
)

for step in "${completed_steps[@]}"; do
  count="$(jq --arg run "$run" --arg step "$step" \
    '[.[] | select(.run == $run and .step == $step and .status == "completed")] | length' \
    <<<"$work")"
  test "$count" -eq 1
done

while read -r name kind; do
  subject="resource/plan-run/$ST_PLAN_RUN/$name"
  status="$(st3 inspect "$subject" --json)"
  jq -e --arg kind "$kind" '
    .status.subjects[0].actual | (.fields // .)
      | (.kind == $kind) and (.state == "published")' \
    <<<"$status" >/dev/null
  bindings="$(st3 trace "$subject" --json --limit 20 \
    | jq -s '[.[] | select(.kind == "resource.binding")] | length')"
  test "$bindings" -ge 1
done <<'PRODUCTS'
test-brief message.receipt
test-revision vcs.revision
developer-report message.receipt
final-assessment message.receipt
PRODUCTS

published="$(st3 inspect "resource/plan-run/$ST_PLAN_RUN/test-revision" --json \
  | jq -r '.status.subjects[0].actual | (.fields // .) | .revision')"
current="$(git -C "$CATALOG/worker" rev-parse HEAD)"
test "$published" = "$current"

echo "PASS: the graph records the complete Test Writing plan and products"
