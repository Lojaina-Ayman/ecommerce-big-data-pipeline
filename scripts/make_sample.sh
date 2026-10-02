#!/bin/sh
set -eu

SRC=/work/data/raw/2019-Oct.csv
OUT=/work/data/sample/events_sample_100k.csv
CHUNK=20000

awk -v c="$CHUNK" '
  NR==1 { print; next }
  (NR >= 2        && NR < 2 + c)        ||
  (NR >= 10000001 && NR < 10000001 + c) ||
  (NR >= 20000001 && NR < 20000001 + c) ||
  (NR >= 30000001 && NR < 30000001 + c) ||
  (NR >= 40000001 && NR < 40000001 + c)
  { print }
' "$SRC" > "$OUT"

wc -l "$OUT"
