"""Data exploration: the eight analytical questions of the assignment.

Run all questions:   spark-submit jobs/explore.py
Run a subset:        spark-submit jobs/explore.py --only q1 q4
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from imdb_project import config, results
from imdb_project.languages import original_language
from imdb_project.spark import get_spark, read_clean

ACTING = ["actor", "actress"]


class Data:
    """Lazily loaded clean tables + common derived frames."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.titles = read_clean(spark, "title.basics")
        self.ratings = read_clean(spark, "title.ratings")
        self.names = read_clean(spark, "name.basics")
        self.principals = read_clean(spark, "title.principals")
        self.crew = read_clean(spark, "title.crew")
        self.episodes = read_clean(spark, "title.episode")
        self.akas = read_clean(spark, "title.akas")
        self.movies = (
            self.titles.where(F.col("titleType") == "movie")
            .join(self.ratings, "tconst", "left")
            .select("tconst", "primaryTitle", "originalTitle", "startYear",
                    "runtimeMinutes", "genres", "isAdult", "averageRating",
                    "numVotes")
            .cache()
        )

    def rated_movies(self, min_votes: int = config.MIN_VOTES) -> DataFrame:
        return self.movies.where(F.col("numVotes") >= min_votes)


# --------------------------------------------------------------------------
def q1_runtime_outliers(d: Data) -> None:
    """Feature films whose runtime deviates notably from their genre mean."""
    feature = (d.movies
               .where(F.col("runtimeMinutes") >= config.FEATURE_MIN_RUNTIME)
               .select("tconst", "primaryTitle", "startYear",
                       "runtimeMinutes", F.explode("genres").alias("genre")))
    # Mean / std are computed on plausible runtimes (<= 600 min) so that a
    # handful of art installations (e.g. 857-hour films) do not inflate the
    # standard deviation of a whole genre; every film is still evaluated.
    stats = (feature.where(F.col("runtimeMinutes") <= 600)
             .groupBy("genre")
             .agg(F.count(F.lit(1)).alias("n_films"),
                  F.avg("runtimeMinutes").alias("genre_mean"),
                  F.stddev("runtimeMinutes").alias("genre_std"),
                  F.percentile_approx("runtimeMinutes", 0.5)
                  .alias("genre_median")))
    scored = (feature.join(stats, "genre")
              .withColumn("z_score", (F.col("runtimeMinutes")
                                      - F.col("genre_mean"))
                          / F.col("genre_std"))
              .where(F.abs("z_score") >= 3)
              .withColumn("direction", F.when(F.col("z_score") > 0, "longer")
                          .otherwise("shorter"))).cache()
    per_genre = (scored.groupBy("genre").agg(
        F.count(F.lit(1)).alias("n_outliers"),
        F.sum(F.when(F.col("direction") == "longer", 1).otherwise(0))
        .alias("n_longer"),
        F.sum(F.when(F.col("direction") == "shorter", 1).otherwise(0))
        .alias("n_shorter"))
        .join(stats, "genre")
        .withColumn("outlier_pct", 100 * F.col("n_outliers") / F.col("n_films"))
        .withColumn("threshold_long_min",
                    F.col("genre_mean") + 3 * F.col("genre_std"))
        .select("genre", "n_films", "genre_mean", "genre_std", "genre_median",
                "threshold_long_min", "n_outliers", "n_longer", "n_shorter",
                "outlier_pct")
        .orderBy("genre"))
    results.save(per_genre, "explore", "q1_runtime_outliers_by_genre",
                 preview=30, title="Runtime outliers (|z| >= 3) per genre")
    results.save(scored.select("genre", "tconst", "primaryTitle", "startYear",
                               "runtimeMinutes", "genre_mean", "z_score",
                               "direction")
                 .orderBy(F.desc(F.abs("z_score"))),
                 "explore", "q1_runtime_outliers", preview=20,
                 title="Most extreme runtime outliers")
    scored.unpersist()


# --------------------------------------------------------------------------
def genre_year(d: Data) -> DataFrame:
    """Per (year, genre) popularity metrics for 1980-2025 movies."""
    m = (d.movies.where(F.col("startYear").between(config.YEAR_FROM,
                                                   config.YEAR_TO))
         .withColumn("numVotes", F.coalesce("numVotes", F.lit(0)))
         .select("tconst", "startYear", "numVotes", "averageRating",
                 F.explode("genres").alias("genre")))
    gy = m.groupBy(F.col("startYear").alias("year"), "genre").agg(
        F.countDistinct("tconst").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        (F.sum(F.col("averageRating") * F.col("numVotes"))
         / F.sum(F.when(F.col("averageRating").isNotNull(), F.col("numVotes"))))
        .alias("weighted_rating"))
    w = Window.partitionBy("year").orderBy(F.desc("total_votes"),
                                           F.desc("n_movies"), "genre")
    year_tot = Window.partitionBy("year")
    return (gy.withColumn("rank", F.row_number().over(w))
            .withColumn("vote_share_pct", 100 * F.col("total_votes")
                        / F.sum("total_votes").over(year_tot))
            .withColumn("movie_share_pct", 100 * F.col("n_movies")
                        / F.sum("n_movies").over(year_tot))).cache()


def q2_q3_top_genres(d: Data) -> None:
    gy = genre_year(d)
    top3 = gy.where("rank <= 3").orderBy("year", "rank")
    results.save(top3.select("year", "rank", "genre", "n_movies",
                             "total_votes", "vote_share_pct",
                             "weighted_rating"),
                 "explore", "q2_top3_genres_per_year", preview=30,
                 title="Top-3 genres per year (popularity = IMDb votes)")
    # Pivot to one compact row per year for the report
    compact = (top3.groupBy("year").agg(
        F.concat_ws(", ", F.transform(
            F.array_sort(F.collect_list(F.struct("rank", "genre"))),
            lambda s: s["genre"])).alias("top3")).orderBy("year"))
    results.save(compact, "explore", "q2_top3_compact", preview=46,
                 title="Top-3 genres per year")

    # Q3: top-3 vs the rest. Movies with several genres are counted in each
    # of their genres (genre-level view), consistent with Q2.
    grp = F.when(F.col("rank") <= 3, "top3").otherwise("rest")
    cmp_ = (gy.withColumn("group", grp).groupBy("year", "group").agg(
        F.sum("n_movies").alias("n_movies"),
        F.sum("total_votes").alias("votes"),
        F.countDistinct("genre").alias("n_genres"),
        (F.sum(F.col("weighted_rating") * F.col("total_votes"))
         / F.sum("total_votes")).alias("weighted_rating"),
        F.sum("vote_share_pct").alias("vote_share_pct"),
        F.sum("movie_share_pct").alias("movie_share_pct")))
    pivot = (cmp_.groupBy("year").pivot("group", ["top3", "rest"]).agg(
        F.first("vote_share_pct").alias("vote_share"),
        F.first("movie_share_pct").alias("movie_share"),
        F.first("weighted_rating").alias("rating"),
        F.first(F.col("votes") / F.col("n_movies")).alias("votes_per_movie"),
        F.first("n_genres").alias("n_genres"))
        .withColumn("votes_per_movie_ratio",
                    F.col("top3_votes_per_movie") / F.col("rest_votes_per_movie"))
        .orderBy("year"))
    results.save(pivot, "explore", "q3_top3_vs_rest", preview=46,
                 title="Top-3 genres vs remaining genres, per year")
    freq = (gy.where("rank <= 3").groupBy("genre").agg(
        F.count(F.lit(1)).alias("years_in_top3"),
        F.sum(F.when(F.col("rank") == 1, 1).otherwise(0)).alias("years_rank1"),
        F.min("year").alias("first_year"), F.max("year").alias("last_year"))
        .orderBy(F.desc("years_in_top3")))
    results.save(freq, "explore", "q3_top3_frequency",
                 title="How often each genre reaches the top 3 (1980-2025)")
    gy.unpersist()


# --------------------------------------------------------------------------
def directed_movies(d: Data) -> DataFrame:
    return (d.crew.select("tconst", F.explode("directors").alias("nconst"))
            .join(d.rated_movies(), "tconst"))


def q4_one_hit_directors(d: Data) -> None:
    dm = directed_movies(d)
    per_dir = dm.groupBy("nconst").agg(
        F.sum(F.when(F.col("averageRating") >= 8.0, 1).otherwise(0))
        .alias("n_hits"),
        F.max(F.when(F.col("averageRating") >= 8.0,
                     F.struct("averageRating", "primaryTitle", "startYear",
                              "numVotes"))).alias("hit"),
        F.sum(F.when(F.col("averageRating") < 8.0, 1).otherwise(0))
        .alias("n_other"),
        F.avg(F.when(F.col("averageRating") < 8.0, F.col("averageRating")))
        .alias("avg_other_rating"))
    out = (per_dir.where((F.col("n_hits") == 1) & (F.col("n_other") >= 2)
                         & (F.col("avg_other_rating") <= 6.0))
           .join(d.names.select("nconst", "primaryName"), "nconst")
           .select("nconst", "primaryName",
                   F.col("hit.primaryTitle").alias("hit_title"),
                   F.col("hit.startYear").alias("hit_year"),
                   F.col("hit.averageRating").alias("hit_rating"),
                   F.col("hit.numVotes").alias("hit_votes"),
                   "n_other", "avg_other_rating")
           .orderBy(F.desc("hit_votes")))
    results.save(out, "explore", "q4_one_hit_directors", preview=25,
                 title=f"Directors with exactly one >= 8.0 movie and >= 2 "
                       f"others averaging <= 6.0 (movies with >= "
                       f"{config.MIN_VOTES} votes)")


# --------------------------------------------------------------------------
def q5_series_best_worst(d: Data, min_votes: int = 10) -> None:
    eps = (d.episodes.where(F.col("seasonNumber").isNotNull()
                            & (F.col("seasonNumber") > 0))
           .join(d.titles.where(F.col("titleType").isin(
               "tvSeries", "tvMiniSeries"))
               .select(F.col("tconst").alias("parentTconst"),
                       F.col("primaryTitle").alias("series_title")),
               "parentTconst"))
    season_sizes = eps.groupBy("parentTconst", "seasonNumber").agg(
        F.count(F.lit(1)).alias("n_episodes"))
    series = season_sizes.groupBy("parentTconst").agg(
        F.sum(F.when(F.col("n_episodes") >= 10, 1).otherwise(0))
        .alias("seasons_ge10"),
        F.max("seasonNumber").alias("last_season"),
        F.count(F.lit(1)).alias("n_seasons")).where("seasons_ge10 >= 3")
    rated = (eps.join(d.ratings.where(F.col("numVotes") >= min_votes),
                      "tconst")
             .join(series, "parentTconst"))
    worst_w = Window.partitionBy("parentTconst").orderBy(
        F.asc("averageRating"), F.desc("numVotes"), "tconst")
    best_w = Window.partitionBy("parentTconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), "tconst")
    ep_struct = F.struct("seasonNumber", "episodeNumber", "averageRating",
                         "numVotes", "tconst")
    ranked = (rated.withColumn("worst_r", F.row_number().over(worst_w))
              .withColumn("best_r", F.row_number().over(best_w)))
    agg = ranked.groupBy("parentTconst", "series_title", "n_seasons",
                         "seasons_ge10", "last_season").agg(
        F.max(F.when(F.col("worst_r") == 1, ep_struct)).alias("worst"),
        F.max(F.when(F.col("best_r") == 1, ep_struct)).alias("best"),
        F.count(F.lit(1)).alias("rated_episodes"),
        F.max("seasonNumber").alias("last_rated_season"))
    # The worst episode must be in the last season *with ratings* that is
    # also the series' last season, the best one in season 1 or 2.
    out = (agg.where((F.col("worst.seasonNumber") == F.col("last_season"))
                     & F.col("best.seasonNumber").isin(1, 2))
           .join(d.ratings.withColumnRenamed("tconst", "parentTconst")
                 .select("parentTconst", F.col("numVotes")
                         .alias("series_votes")), "parentTconst", "left")
           .select("parentTconst", "series_title", "n_seasons", "seasons_ge10",
                   "rated_episodes",
                   F.format_string("S%dE%d", "best.seasonNumber",
                                   "best.episodeNumber").alias("best_ep"),
                   F.col("best.averageRating").alias("best_rating"),
                   F.format_string("S%dE%d", "worst.seasonNumber",
                                   "worst.episodeNumber").alias("worst_ep"),
                   F.col("worst.averageRating").alias("worst_rating"),
                   "series_votes")
           .orderBy(F.desc_nulls_last("series_votes")))
    results.save(out, "explore", "q5_series_decline", preview=25,
                 title="Series (>= 3 seasons of >= 10 episodes) whose best "
                       "episode is in S1-S2 and worst in the last season")


# --------------------------------------------------------------------------
def q6_multilingual_actors(d: Data) -> None:
    lang = original_language(d.spark, d.titles.where(
        F.col("titleType") == "movie"), d.akas)
    success = (d.movies.where((F.col("averageRating") >= config.SUCCESS_RATING)
                              & (F.col("numVotes") >= config.SUCCESS_VOTES))
               .join(lang, "tconst"))
    acted = (d.principals.where(F.col("category").isin(ACTING))
             .select("tconst", "nconst").distinct()
             .join(success, "tconst"))
    per_actor = (acted.groupBy("nconst").agg(
        F.countDistinct("original_language").alias("n_languages"),
        F.array_sort(F.collect_set("original_language")).alias("languages"),
        F.countDistinct("tconst").alias("n_successful_films"))
        .where("n_languages >= 2")
        .join(d.names.select("nconst", "primaryName"), "nconst")
        .select("nconst", "primaryName", "n_languages", "languages",
                "n_successful_films")
        .orderBy(F.desc("n_languages"), F.desc("n_successful_films"))).cache()
    results.save(per_actor, "explore", "q6_actors_2plus_languages",
                 preview=20, title="Actors with successful films in >= 2 "
                                   "original languages")
    results.save(per_actor.where("n_languages >= 3"), "explore",
                 "q6_actors_3plus_languages", preview=30,
                 title="Actors with successful films in >= 3 languages")
    successful = d.movies.where(
        (F.col("averageRating") >= config.SUCCESS_RATING)
        & (F.col("numVotes") >= config.SUCCESS_VOTES))
    coverage = (successful.join(lang, "tconst", "left").agg(
        F.count(F.lit(1)).alias("successful_movies"),
        F.count("original_language").alias("with_language"),
        F.sum(F.when(F.col("language_source") == "wikidata_P364", 1)
              .otherwise(0)).alias("from_wikidata"),
        F.sum(F.when(F.col("language_source") == "akas_heuristic", 1)
              .otherwise(0)).alias("from_akas_heuristic"))
        .withColumn("coverage_pct", 100 * F.col("with_language")
                    / F.col("successful_movies")))
    results.save(coverage, "explore", "q6_language_coverage",
                 title="Coverage of the original-language heuristic")
    results.save(success.groupBy("original_language")
                 .agg(F.count(F.lit(1)).alias("successful_films"))
                 .orderBy(F.desc("successful_films")).limit(15),
                 "explore", "q6_successful_films_by_language",
                 title="Successful films per inferred language")
    per_actor.unpersist()


# --------------------------------------------------------------------------
def q7_early_peak(d: Data) -> None:
    films = (d.principals.where(F.col("category").isin(ACTING))
             .select("tconst", "nconst").distinct()
             .join(d.rated_movies().where(F.col("startYear").isNotNull()),
                   "tconst"))
    w = Window.partitionBy("nconst")
    career = (films
              .withColumn("first_year", F.min("startYear").over(w))
              .withColumn("last_year", F.max("startYear").over(w))
              .withColumn("n_movies", F.count(F.lit(1)).over(w))
              .where((F.col("last_year") - F.col("first_year") >= 20)
                     & (F.col("n_movies") >= 15))
              .withColumn("pos", (F.col("startYear") - F.col("first_year"))
                          / (F.col("last_year") - F.col("first_year"))))
    best_w = Window.partitionBy("nconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"))
    per = (career.withColumn("r", F.row_number().over(best_w))
           .groupBy("nconst", "first_year", "last_year", "n_movies").agg(
               F.max(F.when(F.col("r") == 1, F.struct(
                   "pos", "primaryTitle", "startYear", "averageRating"))
               ).alias("best"),
               F.avg(F.when(F.col("pos") <= 0.5, F.col("averageRating")))
               .alias("early_avg"),
               F.avg(F.when(F.col("pos") > 0.5, F.col("averageRating")))
               .alias("late_avg"),
               F.avg(F.when(F.col("pos") <= 0.5, F.col("numVotes")))
               .alias("early_avg_votes"),
               F.avg(F.when(F.col("pos") > 0.5, F.col("numVotes")))
               .alias("late_avg_votes")))
    out = (per.where((F.col("best.pos") <= 1.0 / 3)
                     & (F.col("early_avg") - F.col("late_avg") >= 0.5))
           .join(d.names.select("nconst", "primaryName"), "nconst")
           .select("nconst", "primaryName", "first_year", "last_year",
                   "n_movies",
                   F.col("best.primaryTitle").alias("best_movie"),
                   F.col("best.startYear").alias("best_year"),
                   F.col("best.averageRating").alias("best_rating"),
                   "early_avg", "late_avg",
                   (F.col("early_avg") - F.col("late_avg")).alias("decline"),
                   "early_avg_votes", "late_avg_votes")
           .orderBy(F.desc("decline")))
    results.save(out, "explore", "q7_early_peak_actors", preview=25,
                 title="Actors (20+ year career, >= 15 movies) who peaked "
                       "early")
    total = per.count()
    print(f"[q7] eligible careers: {total:,}")
    results.save_text("explore", "q7_eligible.txt", f"{total}\n")


# --------------------------------------------------------------------------
def q8_multi_talented(d: Data) -> None:
    movies = d.rated_movies().select("tconst", "averageRating", "numVotes",
                                     "primaryTitle", "startYear")
    roles = (
        d.crew.select("tconst", F.explode("directors").alias("nconst"))
        .withColumn("role", F.lit("director"))
        .unionByName(d.crew.select("tconst", F.explode("writers")
                                   .alias("nconst"))
                     .withColumn("role", F.lit("writer")))
        .unionByName(d.principals.where(F.col("category").isin(ACTING))
                     .select("tconst", "nconst")
                     .withColumn("role", F.lit("actor")))
        .distinct()
        .join(movies, "tconst")
    )
    per_title = roles.groupBy("nconst", "tconst", "averageRating").agg(
        F.array_sort(F.collect_set("role")).alias("roles"))
    # A person is multi-talented when they hold >= 2 distinct roles, each in
    # at least 3 rated movies.
    per_person_role = roles.groupBy("nconst", "role").agg(
        F.countDistinct("tconst").alias("n"))
    talented = (per_person_role.where("n >= 3").groupBy("nconst").agg(
        F.array_sort(F.collect_set("role")).alias("talents"))
        .where(F.size("talents") >= 2))
    pt = (per_title.join(talented, "nconst")
          .withColumn("multi", F.size("roles") >= 2)).cache()
    person = pt.groupBy("nconst", "talents").agg(
        F.sum(F.col("multi").cast("int")).alias("n_multi_role"),
        F.sum((~F.col("multi")).cast("int")).alias("n_single_role"),
        F.avg(F.when(F.col("multi"), F.col("averageRating")))
        .alias("avg_multi"),
        F.avg(F.when(~F.col("multi"), F.col("averageRating")))
        .alias("avg_single"))
    combo = F.concat_ws("+", "talents")
    summary = (person.groupBy(combo.alias("talents")).agg(
        F.count(F.lit(1)).alias("people"),
        F.sum("n_multi_role").alias("multi_role_films"),
        F.sum("n_single_role").alias("single_role_films"),
        F.avg("avg_multi").alias("mean_rating_multi"),
        F.avg("avg_single").alias("mean_rating_single"),
        F.avg(F.col("avg_multi") - F.col("avg_single")).alias("paired_diff"),
        (100 * F.avg(F.when(F.col("avg_multi").isNotNull()
                            & F.col("avg_single").isNotNull(),
                            (F.col("avg_multi") > F.col("avg_single"))
                            .cast("int")))).alias("pct_better_multi"))
        .orderBy(F.desc("people")))
    results.save(summary, "explore", "q8_multi_talent_summary",
                 title="Multi-talented professionals: multi-role vs "
                       "single-role movies")
    by_roleset = (pt.groupBy(F.concat_ws("+", "roles").alias("roles_on_film"))
                  .agg(F.count(F.lit(1)).alias("person_films"),
                       F.avg("averageRating").alias("mean_rating"))
                  .orderBy(F.desc("person_films")))
    results.save(by_roleset, "explore", "q8_rating_by_roles_on_film",
                 title="Mean rating by combination of roles held on a film")
    top = (person.where("n_multi_role >= 5 AND n_single_role >= 5")
           .join(d.names.select("nconst", "primaryName"), "nconst")
           .select("nconst", "primaryName",
                   F.concat_ws("+", "talents").alias("talents"),
                   "n_multi_role", "n_single_role", "avg_multi", "avg_single",
                   (F.col("avg_multi") - F.col("avg_single")).alias("diff"))
           .orderBy(F.desc("n_multi_role")))
    results.save(top, "explore", "q8_top_multi_talented", preview=25,
                 title="Most prolific multi-role professionals")
    pt.unpersist()


QUESTIONS = {
    "q1": q1_runtime_outliers,
    "q2": q2_q3_top_genres,
    "q4": q4_one_hit_directors,
    "q5": q5_series_best_worst,
    "q6": q6_multilingual_actors,
    "q7": q7_early_peak,
    "q8": q8_multi_talented,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="*", default=list(QUESTIONS),
                        help="subset of questions (q3 runs with q2)")
    args = parser.parse_args()
    spark = get_spark("imdb-explore")
    data = Data(spark)
    for key in args.only:
        print(f"===== {key} =====")
        QUESTIONS[key](data)
    spark.stop()


if __name__ == "__main__":
    main()
