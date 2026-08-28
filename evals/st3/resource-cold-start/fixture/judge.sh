#!/usr/bin/env bash
set -euo pipefail

test "$(wc -l <events.tsv)" -eq 2
resource="$(awk -F '\t' 'NR == 1 { print $3 }' events.tsv)"
message="$(awk -F '\t' 'NR == 2 { print $3 }' events.tsv)"
st3 resource read "$resource" --json | jq -e '.actual.status == "active" and .actual.url == "work://eval/cold-start"' >/dev/null
st3 inspect "$message" --json | jq -e '.recent_claims | map(.kind) | index("message.sent") != null' >/dev/null
test "$(st3 message ls rc.worker --json | jq '[.[] | select(.content == "work://eval/cold-start is ready")] | length')" -eq 1
