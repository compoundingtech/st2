#!/usr/bin/env bash
set -euo pipefail

outer="step-run/${ST_RUN_GENERATION}/outer"
claim=""
for _ in $(seq 1 300); do
  if claim=$(st3 work claim "$outer" 2>/dev/null); then
    break
  fi
  sleep 0.2
done
test -n "$claim"

mission_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the mission result local.' | cut -d: -f1)
outer_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the outer result deterministic.' | cut -d: -f1)
test "$mission_line" -lt "$outer_line"

st3 work complete "$outer" --summary "The claim contained both inherited constraints in order." >/dev/null

leaf="step-run/${ST_RUN_GENERATION}/outer/work/leaf"
claim=""
for _ in $(seq 1 300); do
  if claim=$(st3 work claim "$leaf" 2>/dev/null); then
    break
  fi
  sleep 0.2
done
test -n "$claim"

mission_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the mission result local.' | cut -d: -f1)
outer_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the outer result deterministic.' | cut -d: -f1)
nested_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the nested mission result readable.' | cut -d: -f1)
leaf_line=$(printf '%s\n' "$claim" | grep -nF 'Constraint: Keep the leaf result complete.' | cut -d: -f1)
test "$mission_line" -lt "$outer_line"
test "$outer_line" -lt "$nested_line"
test "$nested_line" -lt "$leaf_line"

: >constraint-inheritance.ok
st3 work complete "$leaf" --summary "The claim contained all four inherited constraints in order." >/dev/null
