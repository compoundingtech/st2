#!/usr/bin/env bash
set -euo pipefail

test "$(wc -l <"$CATALOG/events.tsv")" -eq 2
sed -n '1p' "$CATALOG/events.tsv" | grep -Fq $'1\tresource-ready'
sed -n '2p' "$CATALOG/events.tsv" | grep -Fq $'2\tdelivery'
test "$(find "$CATALOG/net/local/worker/resources/links" -type f | wc -l)" -eq 1
test "$(grep -rl 'work://eval/cold-start is ready' "$CATALOG/net/local/worker/resources/inbox" | wc -l)" -eq 1
grep -Fq RESOURCE-COLD-START-GREEN "$RUNS_DIR/contract.out"
