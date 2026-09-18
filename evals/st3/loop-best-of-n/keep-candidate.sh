#!/bin/sh
set -eu

printf '%s\n' "$ST_CANDIDATE_INDEX" > "$ST_WORKSPACE/winner-$ST_CANDIDATE_INDEX.txt"
