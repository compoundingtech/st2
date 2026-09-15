#!/bin/sh
set -eu

printf '%s\n' "$ST_LOOP_ITEM_ID" > "$ST_WORKSPACE/item-$ST_LOOP_ITEM_ID.txt"
