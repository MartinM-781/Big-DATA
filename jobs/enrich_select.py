"""Enrichment step 1/3 (Spark): choose which films and people to enrich.

Scope: feature-length films released between YEAR_FROM and YEAR_TO; for each
(genre, year) the TOP_N films ranked by IMDb votes (ties: rating, tconst);
for each selected film the first N_PRINCIPALS principals by billing order.

Everything is parameterised: passing ``--top-n 0 --principals 0`` (no limit)
selects the full IMDb dataset with the very same code path.
"""

import argparse
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from imdb_project import config, results
from imdb_project.spark import get_spark, read_clean, write_parquet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=config.ENRICH_TOP_N)
    parser.add_argument("--principals", type=int,
                        default=config.ENRICH_PRINCIPALS)
    parser.add_argument("--year-from", type=int, default=config.YEAR_FROM)
    parser.add_argument("--year-to", type=int, default=config.YEAR_TO)
    args = parser.parse_args()

    spark = get_spark("imdb-enrich-select")
    titles = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    principals = read_clean(spark, "title.principals")

    films = (titles.where((F.col("titleType") == "movie")
                          & (F.col("runtimeMinutes")
                             >= config.FEATURE_MIN_RUNTIME)
                          & F.col("startYear").between(args.year_from,
                                                       args.year_to))
             .join(ratings, "tconst", "left")
             .withColumn("numVotes", F.coalesce("numVotes", F.lit(0)))
             .select("tconst", "primaryTitle", "originalTitle", "startYear",
                     "runtimeMinutes", "averageRating", "numVotes",
                     F.explode("genres").alias("genre")))
    rank_w = Window.partitionBy("genre", "startYear").orderBy(
        F.desc("numVotes"), F.desc_nulls_last("averageRating"), "tconst")
    ranked = films.withColumn("genre_year_rank", F.row_number().over(rank_w))
    if args.top_n > 0:
        ranked = ranked.where(F.col("genre_year_rank") <= args.top_n)
    write_parquet(ranked, config.ENRICH_DIR / "selection_films", 4)
    ranked = spark.read.parquet(str(config.ENRICH_DIR / "selection_films"))

    film_ids = ranked.select("tconst").distinct()
    pr_w = Window.partitionBy("tconst").orderBy("ordering")
    people = (principals.join(film_ids, "tconst")
              .withColumn("principal_rank", F.row_number().over(pr_w)))
    if args.principals > 0:
        people = people.where(F.col("principal_rank") <= args.principals)
    people = people.select("tconst", "nconst", "ordering", "category",
                           "principal_rank")
    write_parquet(people, config.ENRICH_DIR / "selection_people", 4)
    people = spark.read.parquet(str(config.ENRICH_DIR / "selection_people"))

    # Hand-off files for the (non-Spark) fetcher: one id per line.
    # Successful films (exploration Q6) need their original language from
    # Wikidata P364; same hand-off mechanism.
    successful = (titles.where(F.col("titleType") == "movie")
                  .join(ratings, "tconst")
                  .where((F.col("averageRating") >= config.SUCCESS_RATING)
                         & (F.col("numVotes") >= config.SUCCESS_VOTES)))
    ids = {
        "films.txt": [r.tconst for r in film_ids.collect()],
        "success_films.txt": [r.tconst for r in
                              successful.select("tconst").collect()],
        "people.txt": [r.nconst for r in
                       people.select("nconst").distinct().collect()],
    }
    config.ENRICH_DIR.mkdir(parents=True, exist_ok=True)
    for name, values in ids.items():
        (config.ENRICH_DIR / name).write_text("\n".join(sorted(values)) + "\n")
    stats = {
        "film_slots": ranked.count(),
        "distinct_films": len(ids["films.txt"]),
        "principal_links": people.count(),
        "distinct_people": len(ids["people.txt"]),
        "genres": ranked.select("genre").distinct().count(),
    }
    print(json.dumps(stats, indent=2))
    results.save(spark.createDataFrame(list(stats.items()),
                                       ["metric", "value"]),
                 "enrich", "selection_stats", title="Enrichment scope")
    spark.stop()


if __name__ == "__main__":
    main()
