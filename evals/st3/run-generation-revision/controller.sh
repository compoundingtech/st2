#!/usr/bin/env bash
set -Eeuo pipefail

readonly EVAL_ROOT=$PWD
readonly REQUESTER=person/eval-requester

st3 --json run "$EVAL_ROOT/initial.kdl" \
  --workspace "$EVAL_ROOT" \
  --requester "$REQUESTER" \
  --detach > "$EVAL_ROOT/initial-run.json"

run_subject=$(jq -er '.subject' "$EVAL_ROOT/initial-run.json")
run_id=$(jq -er '.id' "$EVAL_ROOT/initial-run.json")
old_generation=$(jq -er '.generation' "$EVAL_ROOT/initial-run.json")
stable_subject=$(jq -er '.steps[] | select(.step == "stable") | .subject' "$EVAL_ROOT/initial-run.json")

st3 claim "resource/generation-proof/$run_id/stable" resource.binding \
  --actor "$REQUESTER" \
  --field state=done >/dev/null
st3 wait "$stable_subject" --for completed --timeout 2m >/dev/null

st3 --json work revise \
  "$run_subject" \
  "$EVAL_ROOT/revised.kdl" \
  --as "$REQUESTER" \
  --reason "the first work definition needs a generation-aware check" \
  > "$EVAL_ROOT/proposed.json"

proposal=$(jq -er '.proposal.subject' "$EVAL_ROOT/proposed.json")
preview_hash=$(jq -er '.proposal.preview_hash' "$EVAL_ROOT/proposed.json")
jq -e '
  .status == "pending-approval"
  and .proposal.reviewers == ["person/eval-requester"]
' "$EVAL_ROOT/proposed.json" >/dev/null

st3 --json work revision approve \
  "$proposal" \
  "$preview_hash" \
  --as "$REQUESTER" \
  > "$EVAL_ROOT/applied.json"

new_generation=$(jq -er '.plan_run.generation' "$EVAL_ROOT/applied.json")
environment_subject=$(jq -er '.plan_run.steps[] | select(.step == "generation-environment") | .subject' "$EVAL_ROOT/applied.json")
st3 wait "$environment_subject" --for completed --timeout 2m >/dev/null

st3 --json work revision generations "$run_subject" > "$EVAL_ROOT/generations.json"
st3 --json work revision generation "$old_generation" > "$EVAL_ROOT/old-generation.json"
st3 --json work revision generation "$new_generation" > "$EVAL_ROOT/new-generation.json"

jq -e --arg old "$old_generation" --arg new "$new_generation" '
  .status == "applied"
  and .proposal.source_generation == $old
  and .proposal.successor_generation == $new
  and .plan_run.initial_revision != .plan_run.revision
' "$EVAL_ROOT/applied.json" >/dev/null

jq -e --arg old "$old_generation" --arg new "$new_generation" '
  length == 2
  and .[0].subject == $old
  and .[0].status == "superseded"
  and .[1].subject == $new
  and .[1].predecessor == $old
  and .[1].status == "running"
' "$EVAL_ROOT/generations.json" >/dev/null

jq -e '
  .status == "superseded"
  and (.steps[] | select(.step == "stable") | .status) == "completed"
' "$EVAL_ROOT/old-generation.json" >/dev/null
jq -e '
  (.steps[] | select(.step == "stable") | .status) == "completed"
  and (.steps[] | select(.step == "changed") | .status) != "completed"
' "$EVAL_ROOT/new-generation.json" >/dev/null

test "$(cat "$EVAL_ROOT/observed-generation.txt")" = "${new_generation#run-generation/}"

st3 claim "resource/generation-proof/$run_id/changed" resource.binding \
  --actor "$REQUESTER" \
  --field state=done >/dev/null
st3 wait "$run_subject" --for completed --timeout 2m >/dev/null

printf '%s\n' RUN-GENERATION-REVISION-GREEN > "$EVAL_ROOT/result.txt"
