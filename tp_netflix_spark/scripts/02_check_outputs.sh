#!/usr/bin/env bash
set -euo pipefail

echo "Local results:"
find tp_netflix_spark/results -maxdepth 1 -type f | sort

echo
echo "films_by_genre preview:"
head -12 tp_netflix_spark/results/films_by_genre.csv
