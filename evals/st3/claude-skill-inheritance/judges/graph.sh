#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
run="plan-run/$ST_PLAN_RUN"
plan="$(env -u ST_AGENT st3 --json plan show "$run")"

completed_steps=(
  start-worker
  exercise-skill-union
  exercise-skill-union/work/discover-eval-skills
  exercise-skill-union/work/invoke-project-skill
  exercise-skill-union/work/invoke-plugin-skill
  exercise-skill-union/work/verify-skill-effects
  exercise-skill-union/work/report-skill-check
)

for step in "${completed_steps[@]}"; do
  count="$(jq --arg run "$run" --arg step "$step" \
    '[.steps[] | select(.step == $step and .status == "completed")] | length' \
    <<<"$plan")"
  test "$count" -eq 1
done

subject="resource/plan-run/$ST_PLAN_RUN/skill-report"
status="$(st3 inspect "$subject" --json)"
jq -e '
  .status.subjects[0].actual | (.fields // .)
    | (.kind == "message.receipt") and (.state == "published")' \
  <<<"$status" >/dev/null
bindings="$(st3 trace "$subject" --json --limit 20 \
  | jq -s '[.[] | select(.kind == "resource.binding")] | length')"
test "$bindings" -ge 1

echo "PASS: the graph records the complete Claude Skill Inheritance plan and report"
