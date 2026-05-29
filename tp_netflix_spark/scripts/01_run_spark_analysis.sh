#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYTHONPATH="${PYTHONPATH:-}:tp_netflix_spark/src"

.venv/bin/python tp_netflix_spark/src/main.py \
  --input netflix_dataset.csv \
  --local-output tp_netflix_spark/results
