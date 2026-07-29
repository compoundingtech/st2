#!/usr/bin/env bash
# st2 Claude SessionStart hook. Claude treats exit 2 + stderr as a model-visible reminder; the hook
# restores fresh durable context before that reminder. Missing state remains a valid cold start.

set -u

identity="${ST_AGENT:-}"
root="${ST_ROOT:-${CATALOG:-}}"
context=""
if [[ -n "$identity" && -n "$root" ]] && command -v st2 >/dev/null 2>&1; then
  stale_s="${ST_REHYDRATE_STALE_S:-86400}"
  context="$(st2 context read "$identity" --root "$root" --fresh-within "$stale_s" 2>/dev/null || true)"
fi

{
  if [[ -n "$(printf '%s' "$context" | tr -d '[:space:]')" ]]; then
    printf '<context source="st2/context/now.md" agent="%s">\n' "$identity"
    printf '%s' "$context"
    [[ "$context" == *$'\n' ]] || printf '\n'
    printf '</context>\n\n'
  fi
  printf '%s\n' "Run the st2 boot ritual now: set your status to available, then drain your inbox by reading, acting on, replying when useful, and archiving each handled message. Before resuming or starting work, set your status to busy; set available only when yielding or ready for new work."
} >&2

exit 2
