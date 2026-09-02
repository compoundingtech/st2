#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"

messages="$(st3 message ls local.morgan --from sig.sup --json)"
matching="$(
  jq \
    --arg tag "plan-run:$ST_PLAN_RUN" \
    '[.[] | select(.tags | index($tag))]' \
    <<<"$messages"
)"

count="$(jq 'length' <<<"$matching")"
[ "$count" -eq 1 ] || {
  echo "FAIL: expected one final report for plan run $ST_PLAN_RUN, found $count"
  exit 1
}

body="$(jq -r '.[0].content' <<<"$matching")"
grep -qi 'beacon' <<<"$body"
grep -qiE 'commit|revision' <<<"$body"
grep -qiE 'test|green|pass' <<<"$body"

echo "PASS: Morgan received one tagged final report with revision and test evidence"
