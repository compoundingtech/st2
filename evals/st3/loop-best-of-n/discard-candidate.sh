#!/bin/sh
set -eu

printf '%s\n' "$ST_CANDIDATE_INDEX" > "$ST_WORKSPACE/discarded-$ST_CANDIDATE_INDEX.txt"
