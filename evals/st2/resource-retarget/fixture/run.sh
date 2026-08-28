#!/usr/bin/env bash
set -euo pipefail

net="$CATALOG/net"
events="$CATALOG/events.tsv"
old="$(st2 resource add work://eval/target-a --catalog "$net" --host local --as local.worker --relation assignment)"
printf '1\tgrant\twork://eval/target-a\n' >"$events"
st2 resource remove local.worker "$old" --catalog "$net" --host local >/dev/null
printf '2\trevoke\twork://eval/target-a\n' >>"$events"
new="$(st2 resource add work://eval/target-b --catalog "$net" --host local --as local.worker --relation assignment)"
printf '3\tgrant\twork://eval/target-b\n' >>"$events"
st2 resource read local.worker "$new" --catalog "$net" --host local | grep -Fq work://eval/target-b
st2 resource remove local.worker "$new" --catalog "$net" --host local >/dev/null
printf '4\tidle\twork://eval/target-b\n' >>"$events"
