#!/usr/bin/env bash
set -euo pipefail

ledger="$CATALOG/worker"
bus="${ST_ROOT:-$CATALOG}"
state="$CATALOG/.stev"
stamp="$state/restart.done"
log="$state/restart.log"
mkdir -p "$state"

item_commits() {
  git -C "$ledger" log --format='%s' 2>/dev/null | grep -cE '^feat: item ' || true
}

worker_route() {
  local route
  route="$(find "$bus" -mindepth 1 -maxdepth 1 -type d \( -name 'rc.dev' -o -name '*.rc.dev' \) -print -quit 2>/dev/null)"
  printf '%s\n' "${route:-$bus/rc.dev}"
}

if [ ! -f "$stamp" ]; then
  while [ "$(item_commits)" -lt 2 ]; do sleep 0.25; done

  route="$(worker_route)"
  original=""
  while [ -z "$original" ]; do
    original="$(grep -lRE '^from:[[:space:]]*([a-z0-9][a-z0-9._-]*[.])?rc[.]sup([[:space:]]|$)' "$route/archive" "$route/inbox" 2>/dev/null | head -1 || true)"
    [ -n "$original" ] || sleep 0.25
  done

  pre_head="$(git -C "$ledger" rev-parse HEAD)"
  message_name="$(date +%s%3N)-duplicate-rc.md"
  mkdir -p "$route/inbox"
  cp -- "$original" "$route/inbox/$message_name"

  {
    printf 'restart_epoch=%s\n' "$(date +%s)"
    printf 'pre_restart_head=%s\n' "$pre_head"
    printf 'item_commits_at_restart=%s\n' "$(item_commits)"
    printf 'duplicate_message=%s\n' "$message_name"
    printf 'action=cold_restart\n'
  } >"$log"
  touch "$stamp"

  st2 pty kill rc.dev
  printf 'restart_requested=true\n' >>"$log"
fi

while :; do sleep 3600; done
