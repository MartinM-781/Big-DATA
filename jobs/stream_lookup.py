"""Build the static lookup table used to match Wikipedia edits to IMDb.

Two kinds of keys (column ``key_type``):

* ``exact``  - English Wikipedia article titles linked to IMDb ids through
  Wikidata P345 during the enrichment step (high confidence). The mapping
  is generic: run the fetcher on more ids to widen it.
* ``base``   - normalised IMDb titles / person names, matched against the
  article title stripped of its disambiguator, e.g. ``Dune (2021 film)`` ->
  ``dune``. Restricted to *popular* entities (votes threshold) to keep the
  table small and the false-positive rate low. Extra rules are applied in
  the streaming job on the disambiguator.
"""

import argparse

from pyspark.sql import functions as F

from imdb_project import config, results
from imdb_project.spark import get_spark, read_clean, write_parquet


def normalise(col):
    """Lower-case, strip a trailing '(...)' disambiguator and spaces."""
    base = F.regexp_replace(col, r"\s*\([^()]*\)\s*$", "")
    return F.lower(F.trim(F.regexp_replace(base, r"\s+", " ")))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--title-votes", type=int, default=2000)
    parser.add_argument("--person-votes", type=int, default=20000)
    args = parser.parse_args()

    spark = get_spark("imdb-stream-lookup")
    titles = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    names = read_clean(spark, "name.basics")

    popular = (titles.where(F.col("titleType").isin(
        "movie", "tvSeries", "tvMiniSeries", "tvMovie"))
        .join(ratings.where(F.col("numVotes") >= args.title_votes), "tconst"))
    title_keys = popular.select(
        F.lit("base").alias("key_type"),
        normalise(F.col("primaryTitle")).alias("match_key"),
        F.when(F.col("titleType") == "movie", "film")
        .otherwise("series").alias("entity_type"),
        F.col("tconst").alias("imdb_id"),
        F.col("primaryTitle").alias("imdb_name"),
        F.col("startYear").alias("year"),
        F.col("numVotes").alias("popularity"))

    known = (names.select("nconst", "primaryName",
                          F.explode("knownForTitles").alias("tconst"))
             .join(ratings, "tconst")
             .groupBy("nconst", "primaryName")
             .agg(F.max("numVotes").alias("popularity"))
             .where(F.col("popularity") >= args.person_votes))
    person_keys = known.select(
        F.lit("base").alias("key_type"),
        normalise(F.col("primaryName")).alias("match_key"),
        F.lit("person").alias("entity_type"),
        F.col("nconst").alias("imdb_id"),
        F.col("primaryName").alias("imdb_name"),
        F.lit(None).cast("int").alias("year"),
        F.col("popularity"))

    frames = [title_keys, person_keys]
    raw_dir = config.ENRICH_DIR / "raw"
    for entity, kind in [("films", "film"), ("people", "person")]:
        # The Wikidata mapping (IMDb id -> enwiki article URL) is enough;
        # it does not need the article text.
        path = raw_dir / f"{entity}_wikidata.jsonl"
        if path.exists():
            title = F.regexp_replace(F.expr(
                "reflect('java.net.URLDecoder', 'decode', "
                "substring_index(article_url, '/wiki/', -1), 'UTF-8')"),
                "_", " ")
            wiki = (spark.read.json(str(path))
                    .where(F.col("article_url").isNotNull())
                    .select("imdb_id", title.alias("wiki_title")).distinct())
            frames.append(wiki.select(
                F.lit("exact").alias("key_type"),
                F.lower(F.col("wiki_title")).alias("match_key"),
                F.lit(kind).alias("entity_type"), "imdb_id",
                F.col("wiki_title").alias("imdb_name"),
                F.lit(None).cast("int").alias("year"),
                F.lit(None).cast("int").alias("popularity")))
    lookup = frames[0]
    for frame in frames[1:]:
        lookup = lookup.unionByName(frame)
    lookup = lookup.where(F.length("match_key") >= 3).dropDuplicates(
        ["key_type", "match_key", "imdb_id"])
    write_parquet(lookup, config.STREAM_DIR / "lookup", 4)
    lookup = spark.read.parquet(str(config.STREAM_DIR / "lookup"))
    stats = lookup.groupBy("key_type", "entity_type").agg(
        F.count(F.lit(1)).alias("keys"),
        F.countDistinct("imdb_id").alias("imdb_entities")).orderBy(
        "key_type", "entity_type")
    results.save(stats, "stream", "lookup_stats",
                 title="Streaming lookup table")
    spark.stop()


if __name__ == "__main__":
    main()
