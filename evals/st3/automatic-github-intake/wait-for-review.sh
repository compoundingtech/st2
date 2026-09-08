#!/usr/bin/env bash
set -euo pipefail

deadline=$((SECONDS + 600))

while ((SECONDS < deadline)); do
  mapfile -d '' reviews < <(
    find reviews -name REVIEW.md -type f -size +0c -print0 2>/dev/null
  )
  if ((${#reviews[@]} == 1)); then
    exit 0
  fi
  if ((${#reviews[@]} > 1)); then
    printf 'expected one review, found %s\n' "${#reviews[@]}" >&2
    exit 1
  fi
  sleep 2
done

printf 'no completed review appeared before the deadline\n' >&2
exit 1
