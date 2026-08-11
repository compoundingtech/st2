#!/usr/bin/env bash
# Prove that an MCP server can wake an idle Claude Code session.
#
#   ./run-probe.sh            # measure it
#
# It starts a throwaway MCP server that registers no tools. The server waits,
# then pushes one `notifications/claude/channel`. The provider debug log must
# record the notification and a later engine turn. The model may refuse the
# embedded output instruction because channel content is untrusted; model
# obedience is not the receipt.
#
# It prints PROVED or FAILED, and it says which stage failed and which gate is
# shut. That matters on a machine where channelsEnabled has never been set: the
# probe tells you whether the development flag alone is enough, which is a
# question we could not answer on a machine that already had the policy set.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${TMPDIR:-/tmp}/channel-probe-$$"
TOKEN="CHANNEL-PROBE-$RANDOM$RANDOM"
TOKENS="${PROBE_TOKENS:-$TOKEN}"
DELAY="${PROBE_DELAY:-20000}"
INTERVAL="${PROBE_INTERVAL:-1000}"
if [[ ! "$TOKENS" =~ ^[A-Za-z0-9._:-]+(,[A-Za-z0-9._:-]+)*$ ]]; then
  echo "PROBE_TOKENS must be a comma-separated list of simple identifiers." >&2
  exit 2
fi
TOKEN_COUNT=$(awk -F, '{ print NF }' <<<"$TOKENS")

children_of() {
  ps -eo pid=,ppid= | awk -v parent="$1" '$2 == parent { print $1 }'
}

stop_tree() {
  local parent="$1"
  local child
  [[ "$parent" =~ ^[0-9]+$ ]] || return 0
  [ "$parent" -gt 1 ] || return 0
  for child in $(children_of "$parent"); do
    stop_tree "$child"
  done
  if kill -0 "$parent" 2>/dev/null; then
    kill "$parent" 2>/dev/null || true
  fi
}

SPID=""
WPID=""
trap 'if [ -n "$WPID" ]; then stop_tree "$WPID"; fi; if [ -n "$SPID" ]; then stop_tree "$SPID"; fi' EXIT
mkdir -p "$WORK"
cp "$HERE/probe-server.js" "$WORK/"
cat > "$WORK/.mcp.json" <<JSON
{
  "mcpServers": {
    "probe": {
      "command": "node",
      "args": ["$WORK/probe-server.js"],
      "env": {
        "PROBE_LOG": "$WORK/probe.log",
        "PROBE_TOKENS": "$TOKENS",
        "PROBE_DELAY": "$DELAY",
        "PROBE_INTERVAL": "$INTERVAL"
      }
    }
  }
}
JSON
cd "$WORK"
mkfifo "$WORK/input.fifo"
# One Return answers the development-channels warning. A new folder also asks a
# trust question, so send a second Return. Both appear only at startup.
if script --version 2>/dev/null | grep -qi 'util-linux'; then
  script -q -c \
    'exec claude --debug-file debug.log --dangerously-load-development-channels server:probe --permission-mode bypassPermissions' \
    "$WORK/session.txt" <"$WORK/input.fifo" >/dev/null 2>&1 &
else
  script -q "$WORK/session.txt" \
    claude --debug-file "$WORK/debug.log" \
    --dangerously-load-development-channels server:probe \
    --permission-mode bypassPermissions <"$WORK/input.fifo" >/dev/null 2>&1 &
fi
SPID=$!
{ sleep 4; printf '\r'; sleep 4; printf '\r'; sleep 90; } >"$WORK/input.fifo" &
WPID=$!
FOUND=""
for i in $(seq 1 85); do
  sleep 1
  if [ -f "$WORK/debug.log" ]; then
    FIRST_NOTIFICATION=$(grep -nF 'MCP server "probe": notifications/claude/channel:' \
      "$WORK/debug.log" | head -1 | cut -d: -f1)
    FIRST_TURN_START=$(grep -nE '\[engine\] turn [0-9]+ start' "$WORK/debug.log" |
      head -1 | cut -d: -f1)
    if [ "$TOKEN_COUNT" -eq 1 ]; then
      if [ -n "$FIRST_NOTIFICATION" ] && [ -n "$FIRST_TURN_START" ] &&
        [ "$FIRST_NOTIFICATION" -lt "$FIRST_TURN_START" ]; then
        FOUND="$i"
        break
      fi
    else
      SECOND_NOTIFICATION=$(grep -nF 'MCP server "probe": notifications/claude/channel:' \
        "$WORK/debug.log" | sed -n '2p' | cut -d: -f1)
      LAST_NOTIFICATION=$(grep -nF 'MCP server "probe": notifications/claude/channel:' \
        "$WORK/debug.log" | tail -1 | cut -d: -f1)
      FIRST_TURN_END=$(grep -nE '\[engine\] turn [0-9]+ end' "$WORK/debug.log" |
        head -1 | cut -d: -f1)
      SECOND_TURN_START=$(grep -nE '\[engine\] turn [0-9]+ start' "$WORK/debug.log" |
        sed -n '2p' | cut -d: -f1)
      SECOND_TURN_END=$(grep -nE '\[engine\] turn [0-9]+ end' "$WORK/debug.log" |
        sed -n '2p' | cut -d: -f1)
      if [ -f "$WORK/probe.log" ]; then
        PUSH_COUNT=$(grep -c -- '--> PUSHED notifications/claude/channel' \
          "$WORK/probe.log" || true)
      else
        PUSH_COUNT=0
      fi
      if [ "$PUSH_COUNT" -eq "$TOKEN_COUNT" ] &&
        [ -n "$FIRST_NOTIFICATION" ] && [ -n "$FIRST_TURN_START" ] &&
        [ -n "$SECOND_NOTIFICATION" ] && [ -n "$LAST_NOTIFICATION" ] &&
        [ -n "$FIRST_TURN_END" ] && [ -n "$SECOND_TURN_START" ] &&
        [ -n "$SECOND_TURN_END" ] &&
        [ "$FIRST_NOTIFICATION" -lt "$FIRST_TURN_START" ] &&
        [ "$FIRST_TURN_START" -lt "$SECOND_NOTIFICATION" ] &&
        [ "$LAST_NOTIFICATION" -lt "$FIRST_TURN_END" ] &&
        [ "$FIRST_TURN_END" -lt "$SECOND_TURN_START" ] &&
        [ "$SECOND_TURN_START" -lt "$SECOND_TURN_END" ]; then
        FOUND="$i"
        break
      fi
    fi
  fi
done
stop_tree "$WPID"
wait "$WPID" 2>/dev/null || true
WPID=""
stop_tree "$SPID"
wait "$SPID" 2>/dev/null || true
SPID=""
sleep 1

echo "workdir: $WORK"
if [ -n "$FOUND" ]; then
  if [ "$TOKEN_COUNT" -eq 1 ]; then
    echo "PROVED: the notification started an engine turn ${FOUND}s after start."
  else
    echo "PROVED: ${TOKEN_COUNT} notifications crossed an active turn without loss."
    echo "Waiting notifications started a second engine turn ${FOUND}s after start."
  fi
  echo "No input was sent after the two bounded startup confirmations."
  exit 0
fi
echo "FAILED. The stage that failed:"
CLEAN=$(sed 's/\x1b\[[0-9;?]*[a-zA-Z]//g' "$WORK/session.txt" 2>/dev/null |
  tr '\r' '\n')
if printf '%s' "$CLEAN" | grep -qi "org policy"; then
  echo "  GATE 1 is shut: channels are not enabled by org policy."
  echo "  Set channelsEnabled: true in managed settings."
elif printf '%s' "$CLEAN" | grep -qi "approved channels allowlist"; then
  echo "  GATE 3 is shut: this server is not on the approved allowlist."
  echo "  Add --dangerously-load-development-channels, or allowlist a plugin."
elif [ ! -f "$WORK/probe.log" ]; then
  echo "  The MCP server never started. Claude did not reach it."
  echo "  A trust question or a startup error usually causes this."
elif ! grep -q "initialize result" "$WORK/probe.log"; then
  echo "  The server started, and the handshake did not finish."
elif ! grep -q "PUSHED" "$WORK/probe.log"; then
  echo "  The handshake finished, and the server never pushed."
else
  echo "  The server pushed, and the required later engine turn was not observed."
  printf '%s' "$CLEAN" | grep -iE "channel" | tail -3 | sed 's/^/  session says: /'
fi
exit 1
