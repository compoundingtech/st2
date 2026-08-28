#!/usr/bin/env bash
# JUDGE: coordination — the full team loop is visible on the bus, and the confirmation is VERIFIED (not a bare ack).
#
# The loop must show, on the st3 graph message API:
#   (1) a supervisor -> worker delegation,
#   (2) a worker -> supervisor report, and
#   (3) a supervisor -> requester confirmation that POST-DATES the worker's report — i.e. the sup confirmed
#       AFTER the work came back, not just the initial "on it, will confirm" ack it fires right after the kick.
# A completed change with no worker->sup report is the signature of out-of-band / sup-did-it-itself work.
#
# PASS (exit 0): all three present, with (3) post-dating the oldest report.
set -uo pipefail
ROOT="${CATALOG:-$PWD}"
SM="${ST_ROOT:?st3 eval must export ST_ROOT}"
SUP_ID="${SUP_ID:-lmc.sup}"; WORKER_ID="${WORKER_ID:-lmc.worker}"; REQUESTER="${REQUESTER:-requester}"

# Resolve an id to its on-disk bus dir, tolerating a host or team prefix.
busdir(){ local id="$1" d; d="$(ls -d "$SM"/*."$id" "$SM/$id" 2>/dev/null | head -1)"; printf '%s\n' "${d:-$SM/$id}"; }
# Messages in the owner's inbox and archive whose `from:` is the selected graph actor.
msgs_from(){ local owner from; owner="$(busdir "$1")"; from="$2"
  grep -lRE "^from:[[:space:]]*(agent/)?([a-z0-9][a-z0-9._-]*\.)?$from([[:space:]]|\$)" "$owner/inbox" "$owner/archive" 2>/dev/null; }
nlines(){ [ -z "$1" ] && echo 0 || printf '%s\n' "$1" | grep -c .; }
# The numeric filename prefix gives the stable message order.
newest_ts(){ local t max=0;             for f in $1; do t="$(basename "$f" | grep -oE '^[0-9]+')"; [ "${t:-0}" -gt "$max" ] && max="$t"; done; echo "$max"; }
oldest_ts(){ local t min=9999999999999; for f in $1; do t="$(basename "$f" | grep -oE '^[0-9]+')"; [ -n "$t" ] && [ "$t" -lt "$min" ] && min="$t"; done; [ -n "$1" ] && echo "$min" || echo 0; }

fail=0
deleg=$(msgs_from "$WORKER_ID" "$SUP_ID")     # sup -> worker (lands in the worker's box)
report=$(msgs_from "$SUP_ID" "$WORKER_ID")    # worker -> sup (lands in the sup's box)
confirm=$(msgs_from "$REQUESTER" "$SUP_ID")   # sup -> requester (lands in the requester's box)

[ -n "$deleg" ]  && echo "PASS: sup -> worker delegation present ($(nlines "$deleg") msg)" \
                 || { echo "FAIL: no sup -> worker delegation on the bus (delegation not visible => possible out-of-band work)"; fail=1; }
[ -n "$report" ] && echo "PASS: worker -> sup report present ($(nlines "$report") msg)" \
                 || { echo "FAIL: no worker -> sup report on the bus (execute/report not visible => possible sup-did-it-itself)"; fail=1; }
if [ -z "$confirm" ]; then
  echo "FAIL: no sup -> $REQUESTER confirmation on the bus (the loop never closed)"; fail=1
elif [ -n "$report" ] && [ "$(newest_ts "$confirm")" -gt "$(oldest_ts "$report")" ]; then
  echo "PASS: sup -> $REQUESTER confirmation post-dates the worker's report (verified confirm, not a bare ack)"
else
  echo "FAIL: sup -> $REQUESTER messages exist but none post-date the worker's report (looks like an ack only)"; fail=1
fi

# AUTONOMY (signal, non-gating): count requester -> sup messages. 1 = the kick only (fully autonomous);
# >1 = that many post-kick rescues. Reported for the human; it does not affect this judge's verdict.
kicks=$(msgs_from "$SUP_ID" "$REQUESTER"); n=$(nlines "$kicks")
if [ "$n" -le 1 ]; then echo "  autonomy: $n requester->sup message(s) — no post-kick rescue (fully autonomous)"
else echo "  autonomy: $n requester->sup messages — $((n-1)) look like post-kick rescue(s)"; fi

exit "$fail"
