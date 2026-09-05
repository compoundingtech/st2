#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
run_tag="plan-run:$ST_PLAN_RUN"
latest_critique=0

for recipient in fd.a fd.b fd.c; do
  recipient_agent="agent/$ST_PLAN_RUN/$recipient"
  inbox="$(st3 message ls "$recipient_agent" --json)"
  archive="$(st3 message ls "$recipient_agent" --archive --json)"
  messages="$(jq -s 'add | unique_by(.subject)' <(printf '%s' "$inbox") <(printf '%s' "$archive"))"
  critiques="$(jq --arg recipient "$recipient_agent" --arg prefix "agent/$ST_PLAN_RUN/fd." --arg run_tag "$run_tag" '
    [.[] | select(
      (.from | startswith($prefix))
      and .from != $recipient
      and (.tags | index($run_tag))
      and (.tags | index("panel-critique"))
    )]' <<<"$messages")"
  test "$(jq 'length' <<<"$critiques")" -eq 2
  recipient_latest="$(jq '[.[].created_index] | max' <<<"$critiques")"
  if [ "$recipient_latest" -gt "$latest_critique" ]; then
    latest_critique="$recipient_latest"
  fi
done

requester_inbox="$(st3 message ls person/eval-requester --json)"
requester_archive="$(st3 message ls person/eval-requester --archive --json)"
requester_messages="$(jq -s 'add | unique_by(.subject)' <(printf '%s' "$requester_inbox") <(printf '%s' "$requester_archive"))"
recommendations="$(jq --arg run_tag "$run_tag" --arg sup "agent/$ST_PLAN_RUN/fd.sup" '
  [.[] | select(.from == $sup and (.tags | index($run_tag)))]' <<<"$requester_messages")"

test "$(jq 'length' <<<"$recommendations")" -eq 1
test "$(jq -r '.[0].created_index' <<<"$recommendations")" -gt "$latest_critique"

echo "PASS: Small Talk has six peer critiques and one later recommendation"
