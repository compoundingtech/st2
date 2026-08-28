#!/usr/bin/env bash
set -euo pipefail

net="$CATALOG/net"
uri="work://eval/shared-handoff"
a="$(st2 resource add "$uri" --catalog "$net" --host local --as local.a --relation assignment)"
printf '1\ta\tactive\n' >"$CATALOG/events.tsv"
st2 resource remove local.a "$a" --catalog "$net" --host local >/dev/null
printf '2\ta\trevoked\n' >>"$CATALOG/events.tsv"
b="$(st2 resource add "$uri" --catalog "$net" --host local --as local.b --relation assignment)"
printf '3\tb\tactive\n' >>"$CATALOG/events.tsv"
st2 resource read local.b "$b" --catalog "$net" --host local | grep -Fq "$uri"
