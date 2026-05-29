# Mini-projet Spark - Analyse du dataset Netflix

## Objectif

Ce projet repond au syllabus du cours `Introduction aux traitements distribues` :

- lire et parser un dataset avec PySpark ;
- realiser une analyse statistique avec les APIs Spark DataFrame et Spark SQL ;
- manipuler plusieurs APIs Spark ;
- produire des visualisations ;
- presenter le travail en soutenance avec une demonstration.

Le dataset utilise est `netflix_dataset.csv`, qui contient des films avec leur date de sortie, titre, popularite, nombre de votes, note moyenne, langue originale et genres.

## Architecture

```text
CSV local: netflix_dataset.csv
  -> PySpark lit le CSV
Spark DataFrames + Spark SQL
  -> resultats CSV locaux
  -> graphiques locaux
```

## Lancer le projet

Lancer l'analyse Spark :

```bash
bash tp_netflix_spark/scripts/01_run_spark_analysis.sh
```

Verifier les sorties locales :

```bash
bash tp_netflix_spark/scripts/02_check_outputs.sh
```

Les resultats sont crees dans :

```text
tp_netflix_spark/results
```

## Analyses produites

- `summary` : taille du dataset nettoye, nombre de langues, nombre de genres, periode couverte, notes moyennes.
- `films_by_genre` : nombre de films par genre.
- `avg_vote_by_genre` : note moyenne et popularite moyenne par genre.
- `films_by_language` : nombre de films par langue originale.
- `films_by_year` : nombre de films par annee.
- `films_by_decade` : nombre de films et note moyenne par decennie.
- `top_popular_movies` : films les plus populaires.
- `high_quality_popular_movies` : films les mieux notes avec au moins 1000 votes.

## Ce qu'il faut expliquer en soutenance

Spark sert au traitement distribue. Dans ce projet, PySpark lit le fichier CSV local, construit des DataFrames, applique des transformations, execute des aggregations, puis exporte des resultats exploitables pour la visualisation.

La colonne `Genre` contient plusieurs genres separes par des virgules. Pour compter correctement les films par genre, le script utilise `split`, puis `explode`. Cela transforme une ligne comme :

```text
Spider-Man: No Way Home -> Action, Adventure, Science Fiction
```

en plusieurs lignes :

```text
Spider-Man: No Way Home -> Action
Spider-Man: No Way Home -> Adventure
Spider-Man: No Way Home -> Science Fiction
```

Ensuite Spark peut faire un `GROUP BY genre_name` et compter les films.

## Lien avec MapReduce

Le calcul `films_by_genre` suit la logique MapReduce :

```text
Map:
  film -> (genre, 1)

Shuffle/Sort:
  Action -> [1, 1, 1, ...]
  Drama -> [1, 1, 1, ...]

Reduce:
  Action -> somme
  Drama -> somme
```

Spark ne demande pas d'ecrire manuellement les phases `map`, `shuffle` et `reduce`, mais les operations `explode`, `groupBy`, `count` et `orderBy` declenchent un plan d'execution distribue equivalent.

## Fichiers importants

- `src/analyse_netflix_spark.py` : code PySpark principal.
- `scripts/01_run_spark_analysis.sh` : execution Spark.
- `scripts/02_check_outputs.sh` : verification des sorties locales.
- `results/*.csv` : resultats produits par Spark.
- `results/*.png` : graphiques de la presentation.
