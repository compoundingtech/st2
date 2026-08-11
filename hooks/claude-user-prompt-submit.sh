#!/usr/bin/env bash
# Attach the bounded inbox view to the same Claude inference that received a short generic DING.
# This maintained-provider adapter is stateless and fail-open; reply/archive remain authoritative.

set -u

identity="${ST_AGENT:-}"
root="${ST_ROOT:-${CATALOG:-}}"
if [[ -z "$identity" || -z "$root" ]] || ! command -v st2 >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  exit 0
fi

delivery="$(st2 message delivery "$identity" --root "$root" 2>/dev/null || true)"
if [[ -z "$(printf '%s' "$delivery" | tr -d '[:space:]')" ]]; then
  exit 0
fi

printf '%s' "$delivery" | jq -Rs '{
  continue: true,
  hookSpecificOutput: {
    hookEventName: "UserPromptSubmit",
    additionalContext: .
  }
}'
exit 0
