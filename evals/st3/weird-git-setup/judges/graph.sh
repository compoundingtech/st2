#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the judged plan run}"
work="$(env -u ST_AGENT st3 work ls --all --json)"
run="plan-run/$PLAN_RUN"

completed_steps=(
  materialize-megarepo
  start-worker
  repair-feature-worktree
  repair-feature-worktree/work/resolve-checkout
  repair-feature-worktree/work/reproduce-failure
  repair-feature-worktree/work/fix-root-cause
  repair-feature-worktree/work/verify-complete-suite
  repair-feature-worktree/work/publish-feature-revision
  repair-feature-worktree/work/report-requester
)

for step in "${completed_steps[@]}"; do
  count="$(jq --arg run "$run" --arg step "$step" \
    '[.[] | select(.run == $run and .step == $step and .status == "completed")] | length' \
    <<<"$work")"
  test "$count" -eq 1
done

while read -r name kind; do
  subject="resource/plan-run/$PLAN_RUN/$name"
  status="$(st3 inspect "$subject" --json)"
  jq -e --arg kind "$kind" '
    .status.subjects[0].actual | (.fields // .)
      | (.kind == $kind) and (.state == "published")' \
    <<<"$status" >/dev/null
  bindings="$(st3 trace "$subject" --json --limit 20 \
    | jq -s '[.[] | select(.kind == "resource.binding")] | length')"
  test "$bindings" -ge 1
done <<'PRODUCTS'
feature-revision vcs.revision
final-report message.receipt
PRODUCTS

published="$(st3 inspect "resource/plan-run/$PLAN_RUN/feature-revision" --json \
  | jq -r '.status.subjects[0].actual | (.fields // .) | .revision')"
current="$(git -C "$CATALOG/wt/feature" rev-parse HEAD)"
test "$published" = "$current"

echo "PASS: the graph records the complete Weird Git Setup plan and products"
