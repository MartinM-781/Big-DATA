"""Data preparation: profiling, data-quality checks and cleaning.

Steps
-----
1. Read every raw IMDb TSV as *strings* (no inference) so that the profiling
   sees the data exactly as delivered (``\\N`` tokens, empty strings,
   malformed rows).
2. Profile: per column, % of ``\\N`` nulls, % of empty/blank strings and the
   total missing %. Rank the five worst columns of the whole dataset.
3. Type-cast and apply row-level validation rules (impossible dates, invalid
   values, duplicates, ...).
4. Referential integrity: detect foreign keys pointing to missing parents.
5. Handle the issues (see REPORT.md): typed nulls, invalid values nulled and
   flagged in a ``dq_flags`` column, orphans moved to a quarantine dataset,
   duplicates removed. Write the clean layer as parquet.
"""

import argparse
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from imdb_project import config, results
from imdb_project.spark import get_spark, write_parquet

NULL_TOKEN = "\\N"
CORRUPT = "_corrupt_record"
ARRAY_COLUMNS = {
    "name.basics": ["primaryProfession", "knownForTitles"],
    "title.akas": ["types", "attributes"],
    "title.basics": ["genres"],
    "title.crew": ["directors", "writers"],
}
VALID_TITLE_TYPES = {
    "movie", "short", "tvSeries", "tvEpisode", "tvMovie", "tvMiniSeries",
    "tvSpecial", "video", "videoGame", "tvShort", "tvPilot",
}
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]


# --------------------------------------------------------------------------
# Reading & profiling
# --------------------------------------------------------------------------
def read_raw(spark: SparkSession, table: str) -> DataFrame:
    """Read a raw TSV with an all-string schema plus a corrupt-record column."""
    columns = config.IMDB_TABLES[table]
    schema = T.StructType(
        [T.StructField(c, T.StringType()) for c in columns]
        + [T.StructField(CORRUPT, T.StringType())]
    )
    return (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", "")           # IMDb TSVs are not quoted
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .schema(schema)
        .csv(str(config.RAW_DIR / f"{table}.tsv.gz"))
    )


def profile(df: DataFrame, table: str) -> list:
    """One aggregation pass returning (table, column, ...) metric rows."""
    columns = config.IMDB_TABLES[table]
    aggs = [F.count(F.lit(1)).alias("__rows"),
            F.count(F.col(CORRUPT)).alias("__corrupt")]
    for c in columns:
        col = F.col(c)
        aggs.append(F.sum(F.when(col.isNull() | (col == NULL_TOKEN), 1)
                          .otherwise(0)).alias(f"{c}__null"))
        aggs.append(F.sum(F.when((col != NULL_TOKEN) & (F.trim(col) == ""), 1)
                          .otherwise(0)).alias(f"{c}__empty"))
        if c in ARRAY_COLUMNS.get(table, []) or c == "characters":
            # "[]" or a list made only of separators is an empty collection
            aggs.append(F.sum(F.when(col.isin("[]", ","), 1).otherwise(0))
                        .alias(f"{c}__emptylist"))
    stats = df.agg(*aggs).first().asDict()
    rows_total = stats["__rows"]
    out = []
    for c in columns:
        nulls = stats[f"{c}__null"]
        empty = stats[f"{c}__empty"] + stats.get(f"{c}__emptylist", 0)
        out.append((
            table, c, rows_total, nulls, empty,
            100.0 * nulls / rows_total, 100.0 * empty / rows_total,
            100.0 * (nulls + empty) / rows_total,
        ))
    print(f"[profile] {table}: {rows_total:,} rows, "
          f"{stats['__corrupt']:,} malformed")
    return out, rows_total, stats["__corrupt"]


# --------------------------------------------------------------------------
# Typing
# --------------------------------------------------------------------------
def nullify(col_name: str):
    col = F.col(col_name)
    return (F.when(col.isNull() | (col == NULL_TOKEN) | (F.trim(col) == ""),
                   None).otherwise(F.trim(col)))


def to_array(col_name: str):
    value = nullify(col_name)
    return F.when(value.isNull(), None).otherwise(
        F.filter(F.split(value, ","), lambda x: x != ""))


def to_int(col_name: str):
    value = nullify(col_name)
    return F.when(value.rlike(r"^-?\d+$"), value.cast("int"))


def typed(raw: dict) -> dict:
    """Cast every raw table to its target schema (still uncleaned)."""
    nb = raw["name.basics"].select(
        nullify("nconst").alias("nconst"),
        nullify("primaryName").alias("primaryName"),
        to_int("birthYear").alias("birthYear"),
        to_int("deathYear").alias("deathYear"),
        to_array("primaryProfession").alias("primaryProfession"),
        to_array("knownForTitles").alias("knownForTitles"),
    )
    tb = raw["title.basics"].select(
        nullify("tconst").alias("tconst"),
        nullify("titleType").alias("titleType"),
        nullify("primaryTitle").alias("primaryTitle"),
        nullify("originalTitle").alias("originalTitle"),
        nullify("isAdult").alias("isAdult_raw"),
        to_int("startYear").alias("startYear"),
        to_int("endYear").alias("endYear"),
        to_int("runtimeMinutes").alias("runtimeMinutes"),
        to_array("genres").alias("genres"),
    )
    ak = raw["title.akas"].select(
        nullify("titleId").alias("titleId"),
        to_int("ordering").alias("ordering"),
        nullify("title").alias("title"),
        nullify("region").alias("region"),
        nullify("language").alias("language"),
        to_array("types").alias("types"),
        to_array("attributes").alias("attributes"),
        (nullify("isOriginalTitle") == "1").alias("isOriginalTitle"),
    )
    cr = raw["title.crew"].select(
        nullify("tconst").alias("tconst"),
        to_array("directors").alias("directors"),
        to_array("writers").alias("writers"),
    )
    ep = raw["title.episode"].select(
        nullify("tconst").alias("tconst"),
        nullify("parentTconst").alias("parentTconst"),
        to_int("seasonNumber").alias("seasonNumber"),
        to_int("episodeNumber").alias("episodeNumber"),
    )
    pr = raw["title.principals"].select(
        nullify("tconst").alias("tconst"),
        to_int("ordering").alias("ordering"),
        nullify("nconst").alias("nconst"),
        nullify("category").alias("category"),
        nullify("job").alias("job"),
        nullify("characters").alias("characters_raw"),
    )
    ra = raw["title.ratings"].select(
        nullify("tconst").alias("tconst"),
        nullify("averageRating").cast("double").alias("averageRating"),
        to_int("numVotes").alias("numVotes"),
    )
    return {"name.basics": nb, "title.basics": tb, "title.akas": ak,
            "title.crew": cr, "title.episode": ep, "title.principals": pr,
            "title.ratings": ra}


# --------------------------------------------------------------------------
# Validation rules
# --------------------------------------------------------------------------
def issue_rows(df: DataFrame, table: str, rule: str, key: str,
               condition, detail) -> DataFrame:
    """Standard issue record: table, rule, key, detail."""
    return df.where(condition).select(
        F.lit(table).alias("table"), F.lit(rule).alias("rule"),
        F.col(key).cast("string").alias("key"),
        detail.cast("string").alias("detail"),
    )


def add_flags(df: DataFrame, rules: dict) -> DataFrame:
    """Add a ``dq_flags`` array listing every rule a row violates."""
    flags = [F.when(cond, F.lit(name)) for name, cond in rules.items()]
    return df.withColumn("dq_flags",
                         F.filter(F.array(*flags), lambda x: x.isNotNull()))


def clean_names(nb: DataFrame):
    year = config.CURRENT_YEAR
    rules = {
        "death_before_birth": F.col("deathYear") < F.col("birthYear"),
        "birth_in_future": F.col("birthYear") > year,
        "death_in_future": F.col("deathYear") > year,
        "lifespan_over_122y": (F.col("deathYear") - F.col("birthYear")) > 122,
        "alive_over_122y": F.col("deathYear").isNull()
        & (F.col("birthYear") < year - 122),
        "missing_name": F.col("primaryName").isNull(),
    }
    flagged = add_flags(nb, rules)
    impossible = F.array_contains("dq_flags", "death_before_birth") \
        | F.array_contains("dq_flags", "lifespan_over_122y")
    cleaned = (
        flagged
        # We cannot know which of the two years is wrong -> null both.
        .withColumn("birthYear", F.when(impossible | F.array_contains(
            "dq_flags", "birth_in_future"), None).otherwise(F.col("birthYear")))
        .withColumn("deathYear", F.when(impossible | F.array_contains(
            "dq_flags", "death_in_future"), None).otherwise(F.col("deathYear")))
    )
    issues = flagged.where(F.size("dq_flags") > 0).select(
        F.lit("name.basics").alias("table"),
        F.explode("dq_flags").alias("rule"), F.col("nconst").alias("key"),
        F.concat_ws("-", F.col("birthYear").cast("string"),
                    F.col("deathYear").cast("string")).alias("detail"))
    return cleaned, issues


def clean_titles(tb: DataFrame):
    year = config.CURRENT_YEAR
    valid_types = list(VALID_TITLE_TYPES)
    series_types = ["tvSeries", "tvMiniSeries"]
    rules = {
        "end_before_start": F.col("endYear") < F.col("startYear"),
        "start_in_far_future": F.col("startYear") > year + 5,
        "end_year_on_non_series": F.col("endYear").isNotNull()
        & ~F.col("titleType").isin(series_types),
        "runtime_non_positive": F.col("runtimeMinutes") <= 0,
        "runtime_extreme_gt_24h": F.col("runtimeMinutes") > 24 * 60,
        "invalid_isAdult": F.col("isAdult_raw").isNull()
        | ~F.col("isAdult_raw").isin("0", "1"),
        "invalid_titleType": ~F.col("titleType").isin(valid_types)
        | F.col("titleType").isNull(),
        "missing_title": F.col("primaryTitle").isNull(),
        "missing_genres": F.col("genres").isNull(),
        "more_than_3_genres": F.size(F.coalesce("genres", F.array())) > 3,
    }
    flagged = add_flags(tb, rules)
    has = lambda r: F.array_contains("dq_flags", r)  # noqa: E731
    cleaned = (
        flagged
        .withColumn("isAdult", F.when(F.col("isAdult_raw").isin("0", "1"),
                                      F.col("isAdult_raw") == "1"))
        .withColumn("endYear", F.when(has("end_before_start")
                                      | has("end_year_on_non_series"), None)
                    .otherwise(F.col("endYear")))
        .withColumn("runtimeMinutes", F.when(has("runtime_non_positive"), None)
                    .otherwise(F.col("runtimeMinutes")))
        .withColumn("primaryTitle", F.coalesce("primaryTitle", "originalTitle"))
        .withColumn("originalTitle", F.coalesce("originalTitle", "primaryTitle"))
        .drop("isAdult_raw")
    )
    issues = flagged.where(F.size("dq_flags") > 0).select(
        F.lit("title.basics").alias("table"),
        F.explode("dq_flags").alias("rule"), F.col("tconst").alias("key"),
        F.concat_ws("|", "titleType", F.col("startYear").cast("string"),
                    F.col("endYear").cast("string"),
                    F.col("runtimeMinutes").cast("string"),
                    "isAdult_raw").alias("detail"))
    return cleaned, issues


def orphans(child: DataFrame, fk: str, parent_keys: DataFrame, table: str,
            rule: str, child_key: str) -> DataFrame:
    """Rows of ``child`` whose ``fk`` has no parent in ``parent_keys``."""
    return (
        child.where(F.col(fk).isNotNull())
        .join(parent_keys.withColumnRenamed("pk", fk), on=fk, how="left_anti")
        .select(F.lit(table).alias("table"), F.lit(rule).alias("rule"),
                F.col(child_key).cast("string").alias("key"),
                F.col(fk).cast("string").alias("detail"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-profile", action="store_true")
    parser.add_argument("--reuse-staging", action="store_true",
                        help="reuse typed parquet from a previous run")
    parser.add_argument("--finish-principals", action="store_true",
                        help="only (re)write clean title.principals from "
                             "staging + the issue table")
    args = parser.parse_args()

    spark = get_spark("imdb-prepare")
    if args.finish_principals:
        finish_principals(spark)
        spark.stop()
        return
    raw = {t: read_raw(spark, t) for t in config.IMDB_TABLES}

    # 1-2. Profiling --------------------------------------------------------
    if not args.skip_profile:
        prof_rows, volume_rows = [], []
        for table, df in raw.items():
            rows, total, corrupt = profile(df, table)
            prof_rows += rows
            volume_rows.append((table, total, corrupt))
        prof = spark.createDataFrame(prof_rows, [
            "table", "column", "rows", "null_count", "empty_count",
            "null_pct", "empty_pct", "missing_pct"])
        results.save(prof.orderBy("table", "column"), "prep", "null_profile",
                     preview=60, title="Missing values per column")
        results.save(prof.orderBy(F.desc("null_pct")).limit(5), "prep",
                     "top5_null_columns", title="Top 5 columns by % null")
        results.save(prof.orderBy(F.desc("missing_pct")).limit(5), "prep",
                     "top5_missing_columns",
                     title="Top 5 columns by % null + empty")
        results.save(spark.createDataFrame(volume_rows, [
            "table", "rows", "malformed_rows"]).orderBy("table"), "prep",
            "row_counts", title="Raw volumes")

    # 3. Typing + row-level rules ------------------------------------------
    # Gzip is not splittable: decode each TSV once into a typed staging layer.
    staged = {}
    for table, df in typed(raw).items():
        path = config.STAGING_DIR / table.replace(".", "_")
        if not args.reuse_staging or not path.exists():
            write_parquet(df, path, 64)
        staged[table] = spark.read.parquet(str(path))
    t = staged
    names, name_issues = clean_names(t["name.basics"])
    titles, title_issues = clean_titles(t["title.basics"])

    # Duplicated primary keys (keep the first occurrence)
    dup_issues = []
    for table, key in [("name.basics", "nconst"), ("title.basics", "tconst"),
                       ("title.ratings", "tconst"), ("title.crew", "tconst"),
                       ("title.episode", "tconst")]:
        dup_issues.append(
            t[table].groupBy(key).count().where("count > 1").select(
                F.lit(table).alias("table"),
                F.lit("duplicate_primary_key").alias("rule"),
                F.col(key).alias("key"),
                F.col("count").cast("string").alias("detail")))
    dup_issues.append(
        t["title.principals"].groupBy("tconst", "ordering").count()
        .where("count > 1").select(
            F.lit("title.principals").alias("table"),
            F.lit("duplicate_primary_key").alias("rule"),
            F.concat_ws("#", "tconst", F.col("ordering").cast("string"))
            .alias("key"), F.col("count").cast("string").alias("detail")))

    names = names.where(F.col("nconst").isNotNull()).dropDuplicates(["nconst"])
    titles = titles.where(F.col("tconst").isNotNull()) \
        .dropDuplicates(["tconst"])
    write_parquet(names, config.CLEAN_DIR / "name_basics", 32)
    write_parquet(titles, config.CLEAN_DIR / "title_basics", 32)
    names = spark.read.parquet(str(config.CLEAN_DIR / "name_basics"))
    titles = spark.read.parquet(str(config.CLEAN_DIR / "title_basics"))

    title_keys = titles.select(F.col("tconst").alias("pk")).cache()
    name_keys = names.select(F.col("nconst").alias("pk")).cache()

    # 4. Referential integrity ---------------------------------------------
    ratings = t["title.ratings"].dropDuplicates(["tconst"])
    akas = t["title.akas"].dropDuplicates(["titleId", "ordering"])
    episodes = t["title.episode"].dropDuplicates(["tconst"])
    crew = t["title.crew"].dropDuplicates(["tconst"])
    principals = t["title.principals"].dropDuplicates(["tconst", "ordering"])

    rating_issues = [
        issue_rows(ratings, "title.ratings", "rating_out_of_range", "tconst",
                   (F.col("averageRating") < 1) | (F.col("averageRating") > 10)
                   | F.col("averageRating").isNull(),
                   F.col("averageRating")),
        issue_rows(ratings, "title.ratings", "votes_non_positive", "tconst",
                   (F.col("numVotes") <= 0) | F.col("numVotes").isNull(),
                   F.col("numVotes")),
    ]
    episode_rules = [
        issue_rows(episodes, "title.episode", "missing_season_number",
                   "tconst", F.col("seasonNumber").isNull(),
                   F.col("parentTconst")),
        issue_rows(episodes, "title.episode", "missing_episode_number",
                   "tconst", F.col("episodeNumber").isNull(),
                   F.col("parentTconst")),
        issue_rows(episodes, "title.episode", "season_number_gt_1000",
                   "tconst", F.col("seasonNumber") > 1000,
                   F.col("seasonNumber")),
    ]

    fk_issues = [
        orphans(ratings, "tconst", title_keys, "title.ratings",
                "fk_title_missing", "tconst"),
        orphans(akas, "titleId", title_keys, "title.akas",
                "fk_title_missing", "titleId"),
        orphans(episodes, "tconst", title_keys, "title.episode",
                "fk_episode_title_missing", "tconst"),
        orphans(episodes, "parentTconst", title_keys, "title.episode",
                "fk_parent_series_missing", "tconst"),
        orphans(crew, "tconst", title_keys, "title.crew",
                "fk_title_missing", "tconst"),
        orphans(principals, "tconst", title_keys, "title.principals",
                "fk_title_missing", "tconst"),
        orphans(principals, "nconst", name_keys, "title.principals",
                "fk_person_missing", "tconst"),
    ]
    crew_people = crew.select(
        "tconst", F.explode(F.concat(F.coalesce("directors", F.array()),
                                     F.coalesce("writers", F.array())))
        .alias("nconst")).distinct()
    fk_issues.append(orphans(crew_people, "nconst", name_keys, "title.crew",
                             "fk_person_missing", "tconst"))
    known_for = names.select(
        "nconst", F.explode("knownForTitles").alias("tconst"))
    fk_issues.append(orphans(known_for, "tconst", title_keys, "name.basics",
                             "fk_knownFor_title_missing", "nconst"))

    # Cross-table temporal consistency: credited before birth / after death
    life = names.select("nconst", "birthYear", "deathYear")
    when_ = titles.select("tconst", "startYear")
    pr_life = principals.join(when_, "tconst").join(life, "nconst")
    temporal = [
        issue_rows(pr_life, "title.principals", "credited_before_birth",
                   "tconst", F.col("startYear") < F.col("birthYear"),
                   F.concat_ws("|", "nconst", F.col("birthYear").cast("string"),
                               F.col("startYear").cast("string"))),
        issue_rows(pr_life.where(~F.col("category").isin(ARCHIVE_CATEGORIES)),
                   "title.principals", "credited_gt_5y_after_death", "tconst",
                   F.col("startYear") > F.col("deathYear") + 5,
                   F.concat_ws("|", "nconst", "category",
                               F.col("deathYear").cast("string"),
                               F.col("startYear").cast("string"))),
    ]
    ep_years = (episodes.join(when_, "tconst")
                .join(when_.withColumnRenamed("tconst", "parentTconst")
                      .withColumnRenamed("startYear", "parentStart"),
                      "parentTconst"))
    temporal.append(issue_rows(
        ep_years, "title.episode", "episode_before_series_start", "tconst",
        F.col("startYear") < F.col("parentStart"),
        F.concat_ws("|", "parentTconst", F.col("parentStart").cast("string"),
                    F.col("startYear").cast("string"))))

    all_issues = reduce(DataFrame.unionByName, [
        name_issues, title_issues, *dup_issues, *rating_issues,
        *episode_rules, *fk_issues, *temporal])
    write_parquet(all_issues, config.DQ_DIR / "issues", 16)
    issues = spark.read.parquet(str(config.DQ_DIR / "issues"))

    summary = (issues.groupBy("table", "rule")
               .agg(F.count(F.lit(1)).alias("violations"),
                    F.first("key").alias("example_key"),
                    F.first("detail").alias("example_detail"))
               .orderBy("table", F.desc("violations")))
    results.save(summary, "prep", "dq_issue_summary", preview=60,
                 title="Data-quality rule violations")

    examples = (issues.where(F.col("rule") == "death_before_birth")
                .join(names.select(F.col("nconst").alias("key"),
                                   "primaryName"), "key", "left")
                .orderBy("key").limit(15))
    results.save(examples, "prep", "examples_death_before_birth",
                 title="Examples: died before being born")

    # 5. Handling: apply orphan removal & write the clean layer ------------
    ratings = (ratings.join(title_keys.withColumnRenamed("pk", "tconst"),
                            "tconst", "left_semi")
               .where(F.col("averageRating").between(1, 10)
                      & (F.col("numVotes") > 0)))
    akas = akas.join(title_keys.withColumnRenamed("pk", "titleId"),
                     "titleId", "left_semi")
    episodes = (episodes
                .join(title_keys.withColumnRenamed("pk", "tconst"),
                      "tconst", "left_semi")
                .join(title_keys.withColumnRenamed("pk", "parentTconst"),
                      "parentTconst", "left_semi"))
    principals_clean = clean_principals(principals, issues)
    # crew: keep the row, drop dangling person ids from the arrays
    crew_ok = (crew_people.join(name_keys.withColumnRenamed("pk", "nconst"),
                                "nconst", "left_semi")
               .groupBy("tconst").agg(F.collect_set("nconst").alias("ok")))
    crew_clean = (
        crew.join(title_keys.withColumnRenamed("pk", "tconst"), "tconst",
                  "left_semi")
        .join(crew_ok, "tconst", "left")
        .withColumn("ok", F.coalesce("ok", F.array()))
        .withColumn("directors", F.array_intersect(
            F.coalesce("directors", F.array()), "ok"))
        .withColumn("writers", F.array_intersect(
            F.coalesce("writers", F.array()), "ok"))
        .drop("ok")
    )
    write_parquet(ratings, config.CLEAN_DIR / "title_ratings", 8)
    write_parquet(akas, config.CLEAN_DIR / "title_akas", 64)
    write_parquet(episodes, config.CLEAN_DIR / "title_episode", 16)
    write_parquet(crew_clean, config.CLEAN_DIR / "title_crew", 16)
    write_parquet(principals_clean, config.CLEAN_DIR / "title_principals")
    write_clean_counts(spark)
    spark.stop()


def clean_principals(principals: DataFrame, issues: DataFrame) -> DataFrame:
    """Drop orphan credits without re-joining 100M rows to the key tables.

    The orphan (tconst, nconst) pairs were already found by the FK checks
    and stored in the issue table; they are few (thousands), so a broadcast
    anti-join removes them with no shuffle of the 100M-row table. Title-level
    orphans are removed the same way.
    """
    pr_issues = issues.where(F.col("table") == "title.principals")
    bad_people = (pr_issues.where(F.col("rule") == "fk_person_missing")
                  .select(F.col("key").alias("tconst"),
                          F.col("detail").alias("nconst")).distinct())
    bad_titles = (pr_issues.where(F.col("rule") == "fk_title_missing")
                  .select(F.col("key").alias("tconst")).distinct())
    return (
        principals
        .join(F.broadcast(bad_titles), "tconst", "left_anti")
        .join(F.broadcast(bad_people), ["tconst", "nconst"], "left_anti")
        .withColumn("characters", F.when(
            F.col("characters_raw").startswith("["),
            F.from_json("characters_raw", T.ArrayType(T.StringType()))))
        .drop("characters_raw")
    )


def write_clean_counts(spark: SparkSession) -> None:
    """Before/after volumes for the report."""
    after = []
    for table in config.IMDB_TABLES:
        df = spark.read.parquet(str(config.CLEAN_DIR / table.replace(".", "_")))
        after.append((table, df.count()))
    results.save(spark.createDataFrame(after, ["table", "clean_rows"])
                 .orderBy("table"), "prep", "clean_row_counts",
                 title="Rows in the clean layer")


def finish_principals(spark: SparkSession) -> None:
    """Resume a run that stopped while writing title.principals."""
    staged = spark.read.parquet(str(config.STAGING_DIR / "title_principals"))
    issues = spark.read.parquet(str(config.DQ_DIR / "issues"))
    principals = staged.dropDuplicates(["tconst", "ordering"]) \
        if issues.where("table = 'title.principals' AND "
                        "rule = 'duplicate_primary_key'").count() else staged
    write_parquet(clean_principals(principals, issues),
                  config.CLEAN_DIR / "title_principals")
    write_clean_counts(spark)


if __name__ == "__main__":
    main()
