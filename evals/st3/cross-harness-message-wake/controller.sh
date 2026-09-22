#!/usr/bin/env bash
set -euo pipefail

: "${ST_MISSION_RUN:?ST_MISSION_RUN must identify the eval mission run}"

codex="agent/$ST_MISSION_RUN/wake.codex"
claude="agent/$ST_MISSION_RUN/wake.claude"
run_tag="mission-run:$ST_MISSION_RUN"

agent_json() {
  st3 agents show "$1" --all --json
}

wait_for_quiescent() {
  local agent=$1
  local snapshot=""
  for _ in $(seq 1 360); do
    snapshot="$(agent_json "$agent" 2>/dev/null || true)"
    if jq -e '
      .actual.status == "running"
      and .actual.reachability == "reachable"
      and (.harness.state == "idle" or .harness.state == "ready")
    ' <<<"$snapshot" >/dev/null 2>&1; then
      printf '%s' "$snapshot"
      return 0
    fi
    sleep 0.5
  done
  printf '%s did not become quiescent before the kickoff\n' "$agent" >&2
  return 1
}

codex_idle="$(wait_for_quiescent "$codex")"
claude_idle="$(wait_for_quiescent "$claude")"

render_prompt() {
  local template=$1 peer=$2
  sed -e "s|{{PEER}}|$peer|g" -e "s|{{RUN}}|$ST_MISSION_RUN|g" "$template"
}

codex_body="$(render_prompt prompts/codex.md "$claude")"
claude_body="$(render_prompt prompts/claude.md "$codex")"
sent_at="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"

codex_kickoff="$(st3 conversations send "$codex" --from person/eval-requester \
  --subject "Cross-harness consensus" \
  --tags "$run_tag,consensus-wake:kickoff" \
  --body "$codex_body" --json | jq -er .subject)"
claude_kickoff="$(st3 conversations send "$claude" --from person/eval-requester \
  --subject "Cross-harness consensus" \
  --tags "$run_tag,consensus-wake:kickoff" \
  --body "$claude_body" --json | jq -er .subject)"

jq -n \
  --arg run "$ST_MISSION_RUN" \
  --arg codex "$codex" \
  --arg claude "$claude" \
  --arg codex_kickoff "$codex_kickoff" \
  --arg claude_kickoff "$claude_kickoff" \
  --arg sent_at "$sent_at" \
  --argjson codex_observed "$(jq -r '.harness.observed_at_unix_ms' <<<"$codex_idle")" \
  --argjson claude_observed "$(jq -r '.harness.observed_at_unix_ms' <<<"$claude_idle")" \
  '{
    run: $run,
    sent_at: $sent_at,
    agents: {
      codex: {id: $codex, quiescent_observed_at_unix_ms: $codex_observed},
      claude: {id: $claude, quiescent_observed_at_unix_ms: $claude_observed}
    },
    kickoffs: {codex: $codex_kickoff, claude: $claude_kickoff}
  }' >controller-state.json

for _ in $(seq 1 300); do
  inbox="$(st3 conversations ls person/eval-requester --json)"
  archive="$(st3 conversations ls person/eval-requester --archive --json)"
  reports="$(jq -s --arg run_tag "$run_tag" '
    add
    | unique_by(.subject)
    | [.[] | select(
        (.tags | index($run_tag))
        and (.tags | index("consensus-wake:result"))
        and (.content | contains("CONSENSUS EMBER+ORBIT"))
      )]
  ' <(printf '%s' "$inbox") <(printf '%s' "$archive"))"
  codex_reports="$(jq --arg from "$codex" '[.[] | select(.from == $from)] | length' <<<"$reports")"
  claude_reports="$(jq --arg from "$claude" '[.[] | select(.from == $from)] | length' <<<"$reports")"
  if [ "$codex_reports" -eq 1 ] && [ "$claude_reports" -eq 1 ]; then
    printf 'both harnesses reported consensus\n'
    exit 0
  fi
  sleep 1
done

printf 'the two harnesses did not report consensus before the deadline\n' >&2
exit 1
