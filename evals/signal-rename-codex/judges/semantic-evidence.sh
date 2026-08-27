#!/usr/bin/env bash
set -euo pipefail

for owner in sig.sup local.morgan; do
  printf '\n### messages for %s\n' "$owner"
  find ".st3-messages/$owner" -type f -print0 \
    | sort -z \
    | xargs -0 -r sed -n '1,240p'
done

for lane in base relay hub; do
  printf '\n### %s: last three commits and changed files\n' "$lane"
  git -C "$lane" log -3 \
    --format='commit %H%nAuthor: %an <%ae>%nDate: %aI%nSubject: %s' \
    --name-status
done
