# Spark Mini-Project - Netflix Dataset Analysis

## Objective

This project answers the requirements of the `Introduction to Distributed Processing` course:

- read and parse a dataset with PySpark;
- check data quality;
- run statistical analysis with Spark DataFrame and Spark SQL APIs;
- use several Spark APIs;
- produce visualizations;
- present the work with a short demonstration.

The dataset is `netflix_dataset.csv`. It contains movies with their release date, title, popularity, vote count, average rating, original language, and genres.

## Architecture

```text
Local CSV: netflix_dataset.csv
  -> PySpark reads the CSV
Spark DataFrames + Spark SQL
  -> local CSV results written by Spark
  -> local charts created from Spark aggregated results
```

## Run the Project

Run the Spark analysis:

```bash
bash tp_netflix_spark/scripts/01_run_spark_analysis.sh
```

Check the local outputs:

```bash
bash tp_netflix_spark/scripts/02_check_outputs.sh
```

The results are created in:

```text
tp_netflix_spark/results
```

## Produced Analyses

- `summary`: cleaned dataset size, number of languages, number of genres, covered period, average rating, and average popularity.
- `data_quality_missing_values`: number of missing values by important column.
- `data_quality_outlier_thresholds`: outlier detection thresholds using the IQR rule.
- `data_quality_outlier_examples`: examples of movies considered outliers.
- `films_by_genre`: number of movies by genre.
- `avg_vote_by_genre`: average rating and average popularity by genre.
- `films_by_language`: number of movies by original language.
- `films_by_year`: number of movies by release year.
- `films_by_decade`: number of movies and average rating by decade.
- `top_popular_movies`: most popular movies.
- `high_quality_popular_movies`: highest-rated movies with at least 1,000 votes.

## What to Explain During the Presentation

Spark is used for distributed data processing. In this project, PySpark reads the local CSV file, builds DataFrames, applies transformations, runs aggregations, and exports the results as CSV files with the Spark writer.

The Python script uses explicit type annotations as much as possible: typed configuration, typed constants, typed aliases for report rows, typed Spark DataFrames, typed local variables, and explicit conversions for scalar values returned by Spark rows.

The charts are created with Matplotlib only from small results already aggregated by Spark. The data processing logic, cleaning, missing value detection, outlier detection, and aggregations remain in Spark.

The script starts with a data quality check:

- a value is considered missing when an important column is null or empty;
- a value is considered an outlier when it is far from the normal distribution according to the IQR rule;
- the IQR rule uses `Q1 - 1.5 * IQR` and `Q3 + 1.5 * IQR` as bounds;
- an outlier is not necessarily an error, but it must be identified because it can influence averages or rankings.

The `Genre` column contains several genres separated by commas. To count movies correctly by genre, the script uses `split`, then `explode`. It transforms a row like:

```text
Spider-Man: No Way Home -> Action, Adventure, Science Fiction
```

into several rows:

```text
Spider-Man: No Way Home -> Action
Spider-Man: No Way Home -> Adventure
Spider-Man: No Way Home -> Science Fiction
```

Spark can then run a `GROUP BY genre_name` and count the movies.

## Link With MapReduce

The `films_by_genre` calculation follows the MapReduce logic:

```text
Map:
  movie -> (genre, 1)

Shuffle/Sort:
  Action -> [1, 1, 1, ...]
  Drama -> [1, 1, 1, ...]

Reduce:
  Action -> sum
  Drama -> sum
```

Spark does not require manually writing the `map`, `shuffle`, and `reduce` phases, but the `explode`, `groupBy`, `count`, and `orderBy` operations trigger an equivalent distributed execution plan.

## Important Files

- `src/analyze_netflix_spark.py`: main PySpark code.
- `scripts/01_run_spark_analysis.sh`: Spark execution script.
- `scripts/02_check_outputs.sh`: local output verification script.
- `results/*.csv`: results produced by Spark.
- `results/*.png`: charts used in the presentation.
