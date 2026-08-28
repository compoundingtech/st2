#!/usr/bin/env bash
set -euo pipefail

net="$CATALOG/net"
events="$CATALOG/events.tsv"
ref="$(st2 resource add work://eval/cold-start --catalog "$net" --host local --as local.worker --title "Cold start work" --tag work,ready --relation assignment)"
st2 resource read local.worker "$ref" --catalog "$net" --host local | grep -Fq work://eval/cold-start
printf '1\tresource-ready\t%s\n' "$ref" >"$events"
message="$(st2 message send local.worker --catalog "$net" --host local --as local.worker --subject "Resource ready" -m "work://eval/cold-start is ready")"
printf '2\tdelivery\t%s\n' "$message" >>"$events"
printf 'RESOURCE-COLD-START-GREEN\n'
