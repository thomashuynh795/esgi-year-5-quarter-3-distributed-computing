from analysis.aggregations import (
    get_avg_vote_by_genre,
    get_films_by_decade,
    get_films_by_genre,
    get_films_by_language,
    get_films_by_year,
)
from analysis.rankings import get_high_quality_popular_movies, get_top_popular_movies

__all__ = [
    "get_avg_vote_by_genre",
    "get_films_by_decade",
    "get_films_by_genre",
    "get_films_by_language",
    "get_films_by_year",
    "get_high_quality_popular_movies",
    "get_top_popular_movies",
]
