#!/bin/sh
set -eu

"$ST3_BIN" --endpoint "$ST3_ENDPOINT" claim \
  "resource/eval/loop-best-of-n/candidate-$ST_CANDIDATE_INDEX" \
  resource.observed \
  --field kind=custom.eval.loop-candidate \
  --field "score=$ST_CANDIDATE_INDEX" \
  --idempotency-key "eval-loop-candidate-$ST_CANDIDATE_INDEX"
