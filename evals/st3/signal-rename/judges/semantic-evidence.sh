#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"

printf '\n### plan run claim\n'
st3 inspect "plan-run/$ST_PLAN_RUN" --json \
  | jq -c '{status: (.status.subjects[0].actual.fields.status // .status.subjects[0].actual.status)}'

printf '\n### durable work state\n'
env -u ST_AGENT st3 work ls --all --json \
  | jq -c --arg run "plan-run/$ST_PLAN_RUN" \
      '[.[] | select(.run == $run) | {step, status, assignee, updated_at_unix_ms}]'

for product in \
  base-compatibility \
  relay-revision \
  hub-revision \
  config-revision \
  base-final-revision \
  integrated-revision
do
  printf '\n### product: %s\n' "$product"
  st3 inspect "resource/plan-run/$ST_PLAN_RUN/$product" --json \
    | jq -c '[.recent_claims[] | select(.kind == "resource.binding")][0] | {store_index, actor, fields: (.body.fields // .body)}'
done

printf '\n### integrated commits and changed files\n'
. "$(dirname "$0")/_integrate.sh"
git -C "$W" log --reverse \
  --format='commit %H%nAuthor: %an <%ae>%nSubject: %s' \
  --name-only "$BASE"..HEAD

printf '\n### mechanical results\n'
for judge in isolation suite-green rename primitive e2e; do
  bash "$(dirname "$0")/$judge.sh"
done
