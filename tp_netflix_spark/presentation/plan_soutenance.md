# Plan de soutenance - 20 minutes

## 1. Introduction du sujet - 2 min

Nous avons choisi un dataset de films Netflix/TMDB. L'objectif est de montrer une chaine d'analyse avec Spark : lecture du CSV, parsing, nettoyage, analyse statistique, puis visualisation.

Phrase utile :

> Le coeur du projet est PySpark : on lit un dataset, on le transforme en DataFrame, puis on produit des statistiques et des graphiques.

## 2. Architecture Spark locale - 3 min

Expliquer :

- le dataset est un fichier CSV local ;
- Spark lit le CSV avec `spark.read.csv` ;
- le Driver lance l'application ;
- Spark execute les transformations et actions en local avec `local[*]` ;
- les resultats sont exportes dans `tp_netflix_spark/results`.

Commande a montrer :

```bash
bash tp_netflix_spark/scripts/01_run_spark_analysis.sh
```

## 3. Lecture et parsing PySpark - 4 min

Montrer `src/analyse_netflix_spark.py`.

Points a expliquer :

- `spark.read.csv` lit le fichier `netflix_dataset.csv`.
- `to_date`, `cast(DoubleType)`, `cast(IntegerType)` typent les colonnes.
- `dropna` retire les lignes incompletes.
- `year` extrait l'annee de sortie.
- `split` et `explode` normalisent les genres.

## 4. Analyses statistiques - 4 min

Analyses a presenter :

- nombre de films par genre ;
- nombre de films par langue ;
- evolution par annee ;
- note moyenne par genre ;
- top films populaires ;
- films les mieux notes avec au moins 1000 votes.

Phrase utile :

> J'utilise a la fois l'API DataFrame et Spark SQL. Les requetes SQL sont lisibles pour l'analyse, tandis que DataFrame permet de preparer proprement les donnees.

## 5. Visualisations - 3 min

Presenter les graphiques :

- `top_genres.png`
- `top_languages.png`
- `films_by_year.png`
- `avg_vote_by_genre.png`

Interpretation attendue :

- Drama, Comedy, Action et Thriller dominent le catalogue.
- L'anglais est tres majoritaire.
- Les annees recentes sont tres presentes.
- les genres History, War, Music et Animation ont de bonnes notes moyennes.

## 6. Conclusion et limites - 4 min

Conclusion :

- Le pipeline Spark fonctionne de bout en bout.
- Spark simplifie le paradigme MapReduce avec des APIs haut niveau.
- Le projet montre la difference entre transformation, action et aggregation.

Limites :

- Le dataset est petit pour du Big Data reel.
- L'execution est locale, pas sur un cluster.
- Une suite possible serait d'ajouter Spark MLlib pour predire la note ou la popularite.
