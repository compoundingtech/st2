#!/usr/bin/env bash
set -euo pipefail

root="$(mktemp -d "${TMPDIR:-/tmp}/st3-network-smoke.XXXXXX")"
state="$root/state"
socket="$root/st3.sock"
daemon=""
cleanup() {
  if [ -S "$socket" ]; then
    printf '%s\n' 'version 2' 'subgraph { stop "agent/net.dev" }' >stop.kdl
    st3 --endpoint "$socket" run stop.kdl >/dev/null 2>&1 || true
  fi
  if [ -n "$daemon" ]; then kill -TERM "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true; fi
  rm -f "$socket"
  rm -rf "$root"
}
trap cleanup EXIT
st3 up --node smoke --state-dir "$state" --socket "$socket" >daemon.log 2>&1 &
daemon=$!
for _ in $(seq 1 100); do st3 --endpoint "$socket" doctor >/dev/null 2>&1 && break; sleep 0.05; done
st3 --endpoint "$socket" doctor >/dev/null
printf 'NETWORK-SMOKE-HEALTH-GREEN\n' >result.txt
cat >network.kdl <<KDL
version 2
subgraph {
  agent "net.dev" {
    workspace "$PWD"
    command "sleep 300"
    restart "never"
    env { ST3_MESSAGE_ROOT "$state/messages" }
  }
}
KDL
st3 --endpoint "$socket" run network.kdl >/dev/null
for _ in $(seq 1 100); do st3 --endpoint "$socket" agents --json | jq -e '.[] | select(.subject == "agent/net.dev" and .status == "ready")' >/dev/null 2>&1 && break; sleep 0.05; done
id="$(st3 --endpoint "$socket" message send net.dev --from tester -m NETWORK-SMOKE-ROUNDTRIP)"
for _ in $(seq 1 100); do
  st3 --endpoint "$socket" inspect "message/$id" --json | jq -e '.recent_claims | map(.kind) | index("message.delivered") != null' >/dev/null 2>&1 && break
  sleep 0.05
done
st3 --endpoint "$socket" inspect "message/$id" --json | jq -e '.recent_claims | map(.kind) | index("message.delivered") != null' >/dev/null
printf 'NETWORK-SMOKE-DELIVERY-GREEN\n' >>result.txt
cleanup
trap - EXIT
