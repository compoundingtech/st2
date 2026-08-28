#!/usr/bin/env bash
set -euo pipefail

diff -u "$CATALOG/events.tsv" <(printf '1\ta\tactive\n2\ta\trevoked\n3\tb\tactive\n')
test "$(find "$CATALOG/net/local/a/resources/links" -type f 2>/dev/null | wc -l)" -eq 0
test "$(find "$CATALOG/net/local/b/resources/links" -type f 2>/dev/null | wc -l)" -eq 1
