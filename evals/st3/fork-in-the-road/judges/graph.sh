#!/usr/bin/env bash
set -euo pipefail

: "${ST_PLAN_RUN:?ST_PLAN_RUN must identify the judged plan run}"
agents="$(env -u ST_AGENT st3 agents --json)"
run="plan-run/$ST_PLAN_RUN"
plan="$(env -u ST_AGENT st3 --json plan show "$run")"
grouping_reason="the supervisor combines the panel recommendation"

for member in fd.a fd.b fd.c; do
  jq -e --arg subject "agent/$ST_PLAN_RUN/$member" --arg sup "agent/$ST_PLAN_RUN/fd.sup" --arg reason "$grouping_reason" '
    [.[] | select(.subject == $subject)] as $agents
    | ($agents | length) == 1
      and ($agents[0].under == [{"agent":$sup,"reason":$reason}])
  ' <<<"$agents" >/dev/null
done

completed_steps=(
  start-team
  draft-per-human
  draft-shared
  draft-federated
  critique-per-human
  critique-shared
  critique-federated
  revise-per-human
  revise-shared
  revise-federated
  synthesize
)

for step in "${completed_steps[@]}"; do
  count="$(jq --arg run "$run" --arg step "$step" \
    '[.steps[] | select(.step == $step and .status == "completed")] | length' \
    <<<"$plan")"
  test "$count" -eq 1
done

while read -r name kind; do
  subject="resource/plan-run/$ST_PLAN_RUN/$name"
  status="$(st3 inspect "$subject" --json)"
  jq -e --arg kind "$kind" '
    .status.subjects[0].actual | (.fields // .)
      | (.kind == $kind) and (.state == "published")' \
    <<<"$status" >/dev/null
  bindings="$(st3 trace "$subject" --json --limit 20 \
    | jq -s '[.[] | select(.kind == "resource.observed")] | length')"
  test "$bindings" -ge 1
done <<'PRODUCTS'
proposal-a-draft vcs.commit
proposal-b-draft vcs.commit
proposal-c-draft vcs.commit
proposal-a-final vcs.commit
proposal-b-final vcs.commit
proposal-c-final vcs.commit
recommendation vcs.commit
final-report custom.st3.message-receipt
PRODUCTS

while read -r role name; do
  subject="resource/plan-run/$ST_PLAN_RUN/$name"
  published="$(st3 inspect "$subject" --json \
    | jq -r '.status.subjects[0].actual | (.fields // .) | .sha')"
  current="$(git -C "$CATALOG/$role" rev-parse HEAD)"
  test "$published" = "$current"
done <<'FINAL_REVISIONS'
a proposal-a-final
b proposal-b-final
c proposal-c-final
sup recommendation
FINAL_REVISIONS

echo "PASS: the graph records each panel stage, required product, and panel grouping"
