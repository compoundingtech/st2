#!/usr/bin/env bash
set -Eeuo pipefail

grep -Fxq PLANNING-MODE-GREEN result.txt
test -s approved-preview-hash.txt
test -s approved-plan.md
test -s approved-plan.kdl
jq -e '
  .status == "approved"
  and .preview.hash == $hash
  and .published_revision == .candidate.plan_revision
' --arg hash "$(cat approved-preview-hash.txt)" approved.json >/dev/null
jq -e '
  ([.recent_claims[] | select(.kind == "plan.published")] | length) == 1
' plan-inspect.json >/dev/null
jq -e '
  ([.recent_claims[] | select(.kind == "planning-session.approved")] | length) == 1
' planning-session-inspect.json >/dev/null
