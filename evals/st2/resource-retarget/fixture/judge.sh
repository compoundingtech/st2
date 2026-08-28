#!/usr/bin/env bash
set -euo pipefail

diff -u <(cut -f2 "$CATALOG/events.tsv") <(printf 'grant\nrevoke\ngrant\nidle\n')
test "$(find "$CATALOG/net/local/worker/resources/links" -type f 2>/dev/null | wc -l)" -eq 0
