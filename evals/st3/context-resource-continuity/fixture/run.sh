#!/usr/bin/env bash
set -euo pipefail

state="$PWD/state"
socket="$PWD/st3.sock"
daemon=""
: >result.txt
stop_daemon() {
  if [ -n "$daemon" ]; then
    kill -TERM "$daemon" 2>/dev/null || true
    wait "$daemon" 2>/dev/null || true
    daemon=""
  fi
  rm -f "$socket"
}
start_daemon() {
  rm -f "$socket"
  st3 up --node continuity --state-dir "$state" --socket "$socket" >daemon.log 2>&1 &
  daemon=$!
  for _ in $(seq 1 100); do
    st3 --endpoint "$socket" doctor >/dev/null 2>&1 && return
    sleep 0.05
  done
  return 1
}
trap stop_daemon EXIT

start_daemon
printf '%s\n' CONTEXT-NOW-7b9d | ST_AGENT=cr.agent st3 --endpoint "$socket" context write cr.agent
ST_AGENT=cr.agent st3 --endpoint "$socket" context append cr.agent --decision DECISION-7b9d --why DECISION-WHY-7b9d >/dev/null
ref="$(st3 --endpoint "$socket" resource add https://example.invalid/context-resource-7b9d --as cr.agent --title CONTINUITY-RESOURCE-7b9d --tag continuity,restart --relation output)"
printf '%s\n' "$ref" >resource-ref
stop_daemon
for cycle in 1 2; do
  start_daemon
  st3 --endpoint "$socket" doctor >/dev/null
  printf 'cycle-%s\n' "$cycle" >>result.txt
  stop_daemon
done
start_daemon
st3 --endpoint "$socket" context read cr.agent --full >>result.txt
st3 --endpoint "$socket" resource read "$ref" >>result.txt
printf 'cleanup-green\n' >>result.txt
stop_daemon
trap - EXIT
