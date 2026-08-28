#!/usr/bin/env bash
set -euo pipefail

declare -a daemons=()
root="$(mktemp -d "${TMPDIR:-/tmp}/st3-network-isolation.XXXXXX")"
cleanup() {
  for name in a b; do
    socket="$root/$name/st3.sock"
    if [ -S "$socket" ]; then
      printf 'version 2\nsubgraph { stop "agent/net.%s"; stop "pty/session-%s" }\n' "$name" "$name" >"$name/stop.kdl"
      st3 --endpoint "$socket" run "$name/stop.kdl" >/dev/null 2>&1 || true
    fi
  done
  for daemon in "${daemons[@]:-}"; do kill -TERM "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true; done
  rm -rf "$root"
}
trap cleanup EXIT
for name in a b; do
  mkdir -p "$name"
  mkdir -p "$root/$name"
  socket="$root/$name/st3.sock"
  state="$root/$name/state"
  st3 up --node "net-$name" --state-dir "$state" --socket "$socket" >"$name/daemon.log" 2>&1 &
  daemons+=("$!")
  for _ in $(seq 1 100); do st3 --endpoint "$socket" doctor >/dev/null 2>&1 && break; sleep 0.05; done
  cat >"$name/network.kdl" <<KDL
version 2
subgraph {
  agent "net.$name" { workspace "$PWD"; command "sleep 300"; restart "never"; env { ST3_MESSAGE_ROOT "$state/messages" } }
  pty "session-$name" { workspace "$PWD"; command "bash -c 'echo ${name^^}-READY; sleep 300'"; restart "never" }
}
KDL
  st3 --endpoint "$socket" run "$name/network.kdl" >/dev/null
done
sleep 1
ida="$(st3 --endpoint "$root/a/st3.sock" message send net.a --from source -m NETA-SECRET)"
idb="$(st3 --endpoint "$root/b/st3.sock" message send net.b --from source -m NETB-SECRET)"
st3 --endpoint "$root/a/st3.sock" pty ls --json >a/pty.json
st3 --endpoint "$root/b/st3.sock" pty ls --json >b/pty.json
st3 --endpoint "$root/a/st3.sock" message ls net.a --json >a/messages.json
st3 --endpoint "$root/b/st3.sock" message ls net.b --json >b/messages.json
jq -e 'map(.subject) | index("pty/session-a") != null and index("pty/session-b") == null' a/pty.json >/dev/null
jq -e 'map(.subject) | index("pty/session-b") != null and index("pty/session-a") == null' b/pty.json >/dev/null
jq -e 'map(.content) | index("NETA-SECRET") != null and index("NETB-SECRET") == null' a/messages.json >/dev/null
jq -e 'map(.content) | index("NETB-SECRET") != null and index("NETA-SECRET") == null' b/messages.json >/dev/null
printf 'NETWORK-A-ISOLATED\nNETWORK-B-ISOLATED\n' >result.txt
cleanup
trap - EXIT
