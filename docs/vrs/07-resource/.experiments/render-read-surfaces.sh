#!/usr/bin/env bash
# Render candidate `st2 resource ls` output shapes for one agent declaration.
#
# Reads the same bytes st2 reads; changes nothing. Used to check whether a
# candidate surface stops the reported instances from misleading.
#
#   render-read-surfaces.sh <host> <identity> <today|pointer|both>
#
# Catalog defaults to $CATALOG, else the --catalog path the runner was started
# with. Pass it explicitly with CATALOG=... if neither applies.
set -euo pipefail

catalog="${CATALOG:?set CATALOG to the catalog root}"
host="${1:?host}"; identity="${2:?identity}"; variant="${3:-both}"
dir="$catalog/agents/$host/$identity"

bindings() { rg --no-filename -o '^\s*resource "[^"]*"[^\n]*' "$dir/agent.kdl" 2>/dev/null || true; }
links()    { fd -t f -e md . "$dir/resources/links" 2>/dev/null | sort || true; }
field()    { rg --no-filename -o "^$1: .*" "$2" 2>/dev/null | sed "s/^$1: //" || true; }

nb=$(bindings | grep -c . || true); nl=$(links | grep -c . || true)
s() { [ "$1" = 1 ] || printf s; }

case "$variant" in
today)
  printf '# %s resource%s for %s.%s\n' "$nl" "$(s "$nl")" "$host" "$identity"
  links | while read -r f; do
    printf '%s  %s  %s\n' "$(basename "$f")" "$(field url "$f")" "$(field title "$f")"
  done
  ;;

pointer)
  printf '# %s recorded link%s for %s.%s\n' "$nl" "$(s "$nl")" "$host" "$identity"
  links | while read -r f; do
    printf '%s  %s  %s\n' "$(basename "$f")" "$(field url "$f")" "$(field title "$f")"
  done
  [ "$nb" -gt 0 ] && printf '# (%s declared Resource binding%s in agent.kdl — see `st2 agent resource ls`)\n' "$nb" "$(s "$nb")"
  ;;

both)
  printf '# %s.%s\n\n' "$host" "$identity"
  printf '## declared (%s) — publisher-owned, from agent.kdl\n' "$nb"
  if [ "$nb" -eq 0 ]; then printf '  (none)\n'; else
    bindings | sed 's/^[[:space:]]*resource //' | while read -r line; do
      name=${line%%\"*}; name=$(printf '%s' "$line" | sed 's/^"\([^"]*\)".*/\1/')
      uri=$(printf '%s' "$line" | rg -o 'uri="[^"]*"' | sed 's/uri="//; s/"$//')
      printf '  %-18s %s\n' "$name" "$uri"
    done
  fi
  printf '\n## recorded (%s) — agent-owned, from resources/links/\n' "$nl"
  if [ "$nl" -eq 0 ]; then printf '  (none)\n'; else
    links | while read -r f; do
      rel=$(field relation "$f"); title=$(field title "$f")
      printf '  %-10s %-24s %s\n' "${rel:--}" "$(basename "$f")" "${title:-$(field url "$f")}"
    done
  fi
  ;;

*) echo "unknown variant: $variant" >&2; exit 2 ;;
esac
