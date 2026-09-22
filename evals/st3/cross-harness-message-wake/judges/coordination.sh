#!/usr/bin/env bash
set -euo pipefail

: "${ST_MISSION_RUN:?ST_MISSION_RUN must identify the judged mission run}"
test -s controller-state.json

codex="agent/$ST_MISSION_RUN/wake.codex"
claude="agent/$ST_MISSION_RUN/wake.claude"
run_tag="mission-run:$ST_MISSION_RUN"

messages_to() {
  local recipient=$1
  jq -s 'add | unique_by(.subject)' \
    <(st3 conversations ls "$recipient" --json) \
    <(st3 conversations ls "$recipient" --archive --json)
}

codex_messages="$(messages_to "$codex")"
claude_messages="$(messages_to "$claude")"
requester_messages="$(messages_to person/eval-requester)"

codex_kickoff="$(jq -r '.kickoffs.codex' controller-state.json)"
claude_kickoff="$(jq -r '.kickoffs.claude' controller-state.json)"

require_lifecycle() {
  local subject=$1
  local claims
  claims="$(st3 trace show "$subject" --json --limit 50 | jq -s '.')"
  jq -e 'map(.kind) | index("message.delivered") != null and index("message.read") != null' \
    <<<"$claims" >/dev/null
}

require_lifecycle "$codex_kickoff"
require_lifecycle "$claude_kickoff"

codex_fact="$(jq -er --arg from "$codex" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:fact"))
    and (.content | contains("FACT EMBER"))
  )] | if length == 1 then .[0] else error("expected one Codex fact") end
' <<<"$claude_messages")"
claude_fact="$(jq -er --arg from "$claude" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:fact"))
    and (.content | contains("FACT ORBIT"))
  )] | if length == 1 then .[0] else error("expected one Claude fact") end
' <<<"$codex_messages")"

codex_agreement="$(jq -er --arg from "$codex" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:agreement"))
    and (.content | contains("AGREEMENT EMBER+ORBIT"))
  )] | if length == 1 then .[0] else error("expected one Codex agreement") end
' <<<"$claude_messages")"
claude_agreement="$(jq -er --arg from "$claude" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:agreement"))
    and (.content | contains("AGREEMENT EMBER+ORBIT"))
  )] | if length == 1 then .[0] else error("expected one Claude agreement") end
' <<<"$codex_messages")"

codex_result="$(jq -er --arg from "$codex" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:result"))
    and (.content | contains("CONSENSUS EMBER+ORBIT"))
  )] | if length == 1 then .[0] else error("expected one Codex result") end
' <<<"$requester_messages")"
claude_result="$(jq -er --arg from "$claude" --arg run_tag "$run_tag" '
  [.[] | select(
    .from == $from
    and (.tags | index($run_tag))
    and (.tags | index("consensus-wake:result"))
    and (.content | contains("CONSENSUS EMBER+ORBIT"))
  )] | if length == 1 then .[0] else error("expected one Claude result") end
' <<<"$requester_messages")"

for message in "$codex_fact" "$claude_fact" "$codex_agreement" "$claude_agreement"; do
  require_lifecycle "$(jq -r .subject <<<"$message")"
done

codex_fact_index="$(jq -r .created_index <<<"$codex_fact")"
claude_fact_index="$(jq -r .created_index <<<"$claude_fact")"
codex_agreement_index="$(jq -r .created_index <<<"$codex_agreement")"
claude_agreement_index="$(jq -r .created_index <<<"$claude_agreement")"
codex_result_index="$(jq -r .created_index <<<"$codex_result")"
claude_result_index="$(jq -r .created_index <<<"$claude_result")"

test "$codex_agreement_index" -gt "$claude_fact_index"
test "$claude_agreement_index" -gt "$codex_fact_index"
test "$codex_result_index" -gt "$claude_agreement_index"
test "$claude_result_index" -gt "$codex_agreement_index"

session_for() {
  local owner=$1
  st3 conversations sessions --all --json --limit 200 \
    | jq -er --arg owner "$owner" '
        [.value.items[] | select(.owner_id == $owner)]
        | sort_by(.started_at)
        | last
        | .id
      '
}

assert_worked_after_kickoff() {
  local owner=$1 session sent_at
  session="$(session_for "$owner")"
  sent_at="$(jq -r .sent_at controller-state.json)"
  st3 conversations timeline "$session" --json --limit 200 \
    | jq -e --arg sent_at "$sent_at" '
        [.value.items[] | select(
          .type == "status"
          and .body.status == "running"
          and .timestamp > $sent_at
        )] | length >= 1
      ' >/dev/null
}

assert_worked_after_kickoff "$codex"
assert_worked_after_kickoff "$claude"

echo "PASS: idle Codex and Claude sessions woke, exchanged facts and agreements, and reported one consensus"
