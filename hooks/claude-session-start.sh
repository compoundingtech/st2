#!/usr/bin/env bash
# st2 Claude SessionStart hook: restore fresh durable context and attach a bounded inbox view.
# Delivered as `hookSpecificOutput.additionalContext` on stdout, the supported model-visible channel.
# Missing state remains a valid cold start; missing dependencies fail open.

set -u

identity="${ST_AGENT:-}"
root="${ST_ROOT:-${CATALOG:-}}"
ritual="Run the st2 boot ritual now: set your status to available, then handle the body-bearing inbox batch already attached when present; otherwise drain once with st2 message ls --json --include-body. Reply when useful and archive each handled message. Before resuming or starting work, set your status to busy; set available only when yielding or ready for new work."

if ! command -v jq >/dev/null 2>&1; then
  exit 0
fi

context=""
delivery=""
if [[ -n "$identity" && -n "$root" ]] && command -v st2 >/dev/null 2>&1; then
  stale_s="${ST_REHYDRATE_STALE_S:-86400}"
  context="$(st2 context read "$identity" --root "$root" --fresh-within "$stale_s" 2>/dev/null || true)"
  delivery="$(st2 message delivery "$identity" --root "$root" 2>/dev/null || true)"
fi

context_block=""
if [[ -n "$(printf '%s' "$context" | tr -d '[:space:]')" ]]; then
  context_block="$(
    printf '<context source="st2/context/now.md" agent="%s">\n' "$identity"
    printf '%s' "$context"
    [[ "$context" == *$'\n' ]] || printf '\n'
    printf '</context>'
  )"
fi

additional="$ritual"
if [[ -n "$context_block" ]]; then
  additional="${context_block}"$'\n\n'"${additional}"
fi
if [[ -n "$(printf '%s' "$delivery" | tr -d '[:space:]')" ]]; then
  additional="${additional}"$'\n\n'"${delivery}"
fi

printf '%s' "$additional" | jq -Rs '{
  continue: true,
  hookSpecificOutput: {
    hookEventName: "SessionStart",
    additionalContext: .
  }
}'
exit 0
