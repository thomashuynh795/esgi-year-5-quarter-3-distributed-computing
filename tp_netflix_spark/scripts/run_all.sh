#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYTHONPATH="${PYTHONPATH:-}:tp_netflix_spark/src"

INPUT="netflix_dataset.csv"
OUTPUT="tp_netflix_spark/results"

echo "============================================"
echo " Step 1 — Statistical analysis"
echo "============================================"
python3 tp_netflix_spark/src/main.py \
  --input "$INPUT" \
  --local-output "$OUTPUT"

echo ""
echo "============================================"
echo " Step 2 — KMeans clustering + recommendations"
echo "============================================"
python3 tp_netflix_spark/src/ml_main.py \
  --input "$INPUT" \
  --local-output "$OUTPUT" \
  --k-min 5 \
  --k-max 14 \
  --examples "Inception" "Shrek" "The Notebook" "Paranormal Activity" "Avengers: Endgame"

echo ""
echo "============================================"
echo " Done — results in $OUTPUT"
echo "============================================"
find "$OUTPUT" -maxdepth 1 -type f | sort
