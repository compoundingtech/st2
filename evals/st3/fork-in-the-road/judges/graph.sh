#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the judged plan run}"
work="$(env -u ST_AGENT st3 work ls --all --json)"
run="plan-run/$PLAN_RUN"

completed_steps=(
  start-team
  draft-per-human
  draft-shared
  draft-federated
  critique-per-human
  critique-shared
  critique-federated
  revise-per-human
  revise-shared
  revise-federated
  synthesize
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
  test "$bindings" -eq 1
done <<'PRODUCTS'
proposal-a-draft vcs.revision
proposal-b-draft vcs.revision
proposal-c-draft vcs.revision
proposal-a-final vcs.revision
proposal-b-final vcs.revision
proposal-c-final vcs.revision
recommendation vcs.revision
final-report message.receipt
PRODUCTS

echo "PASS: the graph records each panel stage and each required product"
