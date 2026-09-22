#!/usr/bin/env bash
set -euo pipefail

: "${ST_MISSION_RUN:?ST_MISSION_RUN must identify the judged mission run}"

codex="agent/$ST_MISSION_RUN/wake.codex"
claude="agent/$ST_MISSION_RUN/wake.claude"

if [ -s pty-audit.log ]; then
  printf 'direct terminal interaction was observed:\n' >&2
  sed -n '1,40p' pty-audit.log >&2
  exit 1
fi

for agent in "$codex" "$claude"; do
  count="$(st3 trace show "$agent" --json --limit 200 \
    | jq -s '[.[] | select(.kind == "terminal.input.requested")] | length')"
  test "$count" -eq 0
done

echo "PASS: neither the graph input API nor a direct terminal executable participated"
