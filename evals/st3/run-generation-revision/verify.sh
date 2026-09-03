#!/usr/bin/env bash
set -Eeuo pipefail

grep -Fxq RUN-GENERATION-REVISION-GREEN result.txt
jq -e 'length == 2' generations.json >/dev/null
jq -e '.status == "superseded"' old-generation.json >/dev/null
jq -e '.predecessor == $old' \
  --arg old "$(jq -er '.subject' old-generation.json)" \
  new-generation.json >/dev/null
jq -e '.status == "applied"' applied.json >/dev/null
test -s observed-generation.txt
