#!/usr/bin/env bash
# st2 Claude observe hook: forward one hook event (name in $1, payload on stdin) to the agent's
# observed-harness-state record. Fail-open; observation must never wedge or slow the harness.

set -u

event="${1:-}"
identity="${ST_AGENT:-}"
# CATALOG-first, deliberately diverging from the sibling hooks' ST_ROOT-first order: their
# ST_ROOT is a bus root for message writes, while --catalog here resolves the agent DECLARATION —
# with a custom bus root (ST_ROOT != CATALOG) declaration resolution under ST_ROOT finds nothing
# and every transition would silently drop.
root="${CATALOG:-${ST_ROOT:-}}"
runtime_id="${ST2_CLAUDE_RUNTIME_ID:-$identity}"
if [[ -z "$event" || -z "$identity" || -z "$root" ]] || ! command -v st2 >/dev/null 2>&1; then
  exit 0
fi

st2 --catalog "$root" driver claude-observe --identity "$identity" --runtime-id "$runtime_id" \
  --event "$event" >/dev/null 2>&1 || true
exit 0
