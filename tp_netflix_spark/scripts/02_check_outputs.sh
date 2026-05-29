#!/usr/bin/env bash
set -euo pipefail

echo "Resultats locaux:"
find tp_netflix_spark/results -maxdepth 1 -type f | sort

echo
echo "Apercu films_by_genre:"
head -12 tp_netflix_spark/results/films_by_genre.csv
