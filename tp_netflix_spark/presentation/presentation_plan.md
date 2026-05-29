# Presentation Plan - 20 Minutes

## 1. Topic Introduction - 2 min

We chose a Netflix/TMDB movie dataset. The goal is to demonstrate an analysis pipeline with Spark: CSV reading, parsing, cleaning, statistical analysis, and visualization.

Useful sentence:

> The core of the project is PySpark: the dataset is loaded into Spark DataFrames, transformed, analyzed, and exported as results and charts.

## 2. Local Spark Architecture - 3 min

Explain:

- the dataset is a local CSV file;
- Spark reads the CSV with `spark.read.csv`;
- the Driver starts the application;
- Spark executes transformations and actions locally with `local[*]`;
- results are exported to `tp_netflix_spark/results`.

Command to show:

```bash
bash tp_netflix_spark/scripts/01_run_spark_analysis.sh
```

## 3. PySpark Reading and Parsing - 4 min

Show `src/analyze_netflix_spark.py`.

Key points:

- `spark.read.csv` reads the `netflix_dataset.csv` file.
- `to_date`, `cast(DoubleType)`, and `cast(IntegerType)` type the columns.
- missing values are checked on important columns.
- outliers are detected with the IQR rule.
- `dropna` removes incomplete rows from the analysis dataset.
- `year` extracts the release year.
- `split` and `explode` normalize genres.

Useful sentence:

> I consider a value missing when an important analysis column is empty or null. I consider a value an outlier when it is outside the bounds Q1 - 1.5*IQR and Q3 + 1.5*IQR. It is not automatically an error, but it can influence the results.

## 4. Statistical Analysis - 4 min

Analyses to present:

- missing value report;
- outlier thresholds and examples;
- number of movies by genre;
- number of movies by language;
- evolution by year;
- average rating by genre;
- top popular movies;
- highest-rated movies with at least 1,000 votes.

Useful sentence:

> I use both the DataFrame API and Spark SQL. SQL queries are readable for analysis, while DataFrame operations prepare and clean the data.

## 5. Visualizations - 3 min

Charts to present:

- `top_genres.png`
- `top_languages.png`
- `films_by_year.png`
- `avg_vote_by_genre.png`
- `missing_values.png`
- `outliers_by_metric.png`

Useful sentence:

> The charts do not process the dataset. Spark produces aggregated results first, then Matplotlib only displays those results as images.

Expected interpretation:

- Drama, Comedy, Action, and Thriller dominate the catalog.
- English is by far the most common original language.
- Recent years are strongly represented.
- History, War, Music, and Animation have strong average ratings.
- Outliers are not automatically removed: they are reported to keep the analysis transparent.

## 6. Conclusion and Limits - 4 min

Conclusion:

- The Spark pipeline works end to end.
- Spark simplifies the MapReduce paradigm with high-level APIs.
- The project shows the difference between transformations, actions, and aggregations.

Limits:

- The dataset is small compared to real Big Data workloads.
- The execution is local, not on a real cluster.
- A possible extension would be to add Spark MLlib to predict rating or popularity.
