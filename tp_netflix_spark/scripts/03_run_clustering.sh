#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYTHONPATH="${PYTHONPATH:-}:tp_netflix_spark/src"

python3 tp_netflix_spark/src/ml_main.py \
  --input netflix_dataset.csv \
  --local-output tp_netflix_spark/results \
  --k-min 5 \
  --k-max 14 \
  --examples "Inception" "Shrek" "The Notebook" "Paranormal Activity" "Avengers: Endgame"
