#!/usr/bin/env bash
# st2 Claude observe hook: forward one hook event (name in $1, payload on stdin) to the agent's
# observed-harness-state record. Fail-open; observation must never wedge or slow the harness.

set -u

event="${1:-}"
identity="${ST_AGENT:-}"
root="${ST_ROOT:-${CATALOG:-}}"
if [[ -z "$event" || -z "$identity" || -z "$root" ]] || ! command -v st2 >/dev/null 2>&1; then
  exit 0
fi

st2 --catalog "$root" driver claude-observe --identity "$identity" --event "$event" \
  >/dev/null 2>&1 || true
exit 0
