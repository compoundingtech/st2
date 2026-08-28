#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the judged plan run}"

printf '\n### plan run claim\n'
st3 inspect "plan-run/$PLAN_RUN" --json

printf '\n### durable work state\n'
st3 work ls --all --json \
  | jq --arg run "plan-run/$PLAN_RUN" '[.[] | select(.run == $run)]'

for product in \
  base-compatibility \
  relay-revision \
  hub-revision \
  config-revision \
  base-final-revision \
  integrated-revision
do
  printf '\n### product: %s\n' "$product"
  st3 inspect "resource/plan-run/$PLAN_RUN/$product" --json
done

for lane in base relay hub; do
  printf '\n### %s: last three commits and changed files\n' "$lane"
  git -C "$lane" log -3 \
    --format='commit %H%nAuthor: %an <%ae>%nDate: %aI%nSubject: %s' \
    --name-status
done
