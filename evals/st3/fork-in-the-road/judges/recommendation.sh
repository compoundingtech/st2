#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
requester_inbox="$(st3 message ls person/eval-requester --json)"
requester_archive="$(st3 message ls person/eval-requester --archive --json)"
requester_messages="$(jq -s 'add | unique_by(.subject)' \
  <(printf '%s' "$requester_inbox") \
  <(printf '%s' "$requester_archive"))"

jq -e \
  --arg sender "agent/$ST_PLAN_RUN/fd.sup" \
  --arg run_tag "plan-run:$ST_PLAN_RUN" \
  '[.[] | select(.from == $sender and (.tags | index($run_tag)))] | length == 1' \
  <<<"$requester_messages" >/dev/null

echo "PASS: the supervisor sent one recommendation to the requester"
