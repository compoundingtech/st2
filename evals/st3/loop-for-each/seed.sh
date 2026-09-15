#!/bin/sh
set -eu

"$ST3_BIN" --endpoint "$ST3_ENDPOINT" claim \
  resource/eval/loop-for-each/items \
  resource.observed \
  --field kind=custom.eval.loop-items \
  --field 'items=[{"id":"one","name":"first"},{"id":"two","name":"second"},{"id":"three","name":"third"}]' \
  --idempotency-key eval-loop-for-each-items
