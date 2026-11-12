"""Paths and tunable parameters shared by every job."""

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
CLEAN_DIR = DATA_DIR / "clean"
DQ_DIR = DATA_DIR / "dq"
ENRICH_DIR = DATA_DIR / "enrich"
STREAM_DIR = DATA_DIR / "stream"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/project/results"))

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.recentchange")

IMDB_BASE_URL = "https://datasets.imdbws.com"

# Every IMDb non-commercial table and its raw TSV column order.
IMDB_TABLES = {
    "name.basics": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title.akas": [
        "titleId", "ordering", "title", "region", "language", "types",
        "attributes", "isOriginalTitle",
    ],
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.crew": ["tconst", "directors", "writers"],
    "title.episode": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    "title.principals": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}

# Analysis parameters (kept in one place so they are easy to audit).
CURRENT_YEAR = 2026
FEATURE_MIN_RUNTIME = 40      # AMPAS / AFI / BFI: a feature film runs > 40 min
YEAR_FROM, YEAR_TO = 1980, 2025
MIN_VOTES = 100               # minimum votes for a rating to be considered
SUCCESS_RATING = 7.0          # "successful" film: rating >= 7.0 ...
SUCCESS_VOTES = 5000          # ... and at least 5,000 votes
ENRICH_TOP_N = 10             # top N films per (genre, year)
ENRICH_PRINCIPALS = 3         # principals per film to enrich
