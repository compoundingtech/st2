#!/usr/bin/env bash
set -euo pipefail

crash="exec/eval/crash-escalation/crash/$ST_PLAN_RUN"
clean="exec/eval/crash-escalation/clean/$ST_PLAN_RUN"
st3 trace "$crash" --json --limit 30 | jq -s -e '[.[] | select(.kind == "supervision.decision") | .body.fields.decision] | index("raise") != null' >/dev/null
st3 trace "$clean" --json --limit 30 | jq -s -e '[.[] | select(.kind == "supervision.decision") | .body.fields.decision] | index("raise") == null' >/dev/null
