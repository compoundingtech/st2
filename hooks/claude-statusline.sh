#!/usr/bin/env bash
# st2 Claude status-line tee: record the status-line payload (stdin JSON) to the agent's
# harness-context record, then hand the SAME bytes to the operator's own renderer.
#
# Claude's `statusLine` is a single slot and the winning declaration replaces the others outright
# — it does not merge — so st2 occupying it in `.claude/settings.local.json` would silently remove
# whatever the operator had on every managed seat. Chaining is therefore mandatory (HC-R18), and
# every failure path here degrades to a rendered status line rather than a blank one:
#
#   - no identity, no catalog root, or no `st2` on PATH -> `cat`, the payload verbatim
#   - `st2` present -> it records (fail-open) and chains; it never exits non-zero on its own
#
# `exec` in both arms deliberately: no command substitution anywhere, so the stdin bytes reach the
# downstream renderer unchanged rather than through the shell's trailing-newline stripping.

set -u

identity="${ST_AGENT:-}"
# CATALOG-first, matching claude-observe.sh: `--catalog` resolves the agent DECLARATION, and with
# a custom bus root (ST_ROOT != CATALOG) resolution under ST_ROOT would find nothing.
root="${CATALOG:-${ST_ROOT:-}}"
if [[ -z "$identity" || -z "$root" ]] || ! command -v st2 >/dev/null 2>&1; then
  exec cat
fi

exec st2 --catalog "$root" driver claude-statusline --identity "$identity"
