#!/usr/bin/env bash
set -euo pipefail

: "${PLAN_RUN:?PLAN_RUN must identify the judged plan run}"

dev_messages="$(st3 message ls rc.dev --archive --json)"
sup_messages="$(st3 message ls rc.sup --archive --json)"
requester_messages="$(st3 message ls requester --archive --json)"
work_tag="plan-run:plan-run/$PLAN_RUN"
direct_tag="plan-run:$PLAN_RUN"
injector="exec/eval/restart-continuity/inject/$PLAN_RUN"

assignments="$(jq --arg tag "$work_tag" \
  '[.[] | select(.from == "st3/runtime" and (.tags | index($tag)))]' <<<"$dev_messages")"
duplicates="$(jq --arg tag "$direct_tag" --arg injector "$injector" \
  '[.[] | select(
    .from == $injector
    and (.tags | index($tag))
    and (.tags | index("duplicate-work:process-before-restart"))
    and (.content | contains("DUPLICATE-BATCH-RC-7B9D"))
  )]' <<<"$dev_messages")"
reports="$(jq --arg tag "$direct_tag" \
  '[.[] | select(.from == "agent/rc.dev" and (.tags | index($tag)))]' <<<"$sup_messages")"
confirmations="$(jq --arg tag "$direct_tag" \
  '[.[] | select(.from == "agent/rc.sup" and (.tags | index($tag)))]' <<<"$requester_messages")"

test "$(jq 'length' <<<"$assignments")" -eq 2
test "$(jq '[.[] | select(.status == "closed")] | length' <<<"$assignments")" -eq 2
test "$(jq 'length' <<<"$duplicates")" -eq 1
test "$(jq -r '.[0].status' <<<"$duplicates")" = closed
test "$(jq 'length' <<<"$reports")" -eq 1
test "$(jq -r '.[0].status' <<<"$reports")" = closed
test "$(jq 'length' <<<"$confirmations")" -eq 1

duplicate_subject="$(jq -r '.[0].subject' <<<"$duplicates")"
restart_subject="$(st3 inspect "resource/plan-run/$PLAN_RUN/restart" --json \
  | jq -r '.status.subjects[0].actual | (.fields // .) | .duplicate_message')"
test "$duplicate_subject" = "$restart_subject"

report_index="$(jq -r '.[0].created_index' <<<"$reports")"
confirmation_index="$(jq -r '.[0].created_index' <<<"$confirmations")"
test "$confirmation_index" -gt "$report_index"

echo "PASS: Small Talk has two assignments, one closed duplicate, one report, and one later confirmation"
