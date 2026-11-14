"""Open question: do recurring director-lead actor partnerships pay off?

Question
--------
When a director casts the same lead actor again, are the resulting films
rated better than the director's one-off pairings - and does the quality of a
partnership improve or wear off over successive collaborations?

Data: title.basics, title.crew (directors), title.principals (lead cast =
the first three acting credits), title.ratings, name.basics.

Method
------
1. Feature films with >= MIN_VOTES votes; one row per (director, lead actor,
   film) - a director acting in their own film is excluded.
2. Collaboration index k = rank of the film among that pair's films by year.
3. Director fixed effect: ``rel_rating = rating - mean rating of the
   director's films``. Comparing a director with themselves removes the
   "good directors make good films" confound.
4. Compare one-off pairs vs recurring pairs, the evolution of rel_rating with
   k, and the within-pair change between the 1st and the last collaboration.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from imdb_project import config, results
from imdb_project.spark import get_spark, read_clean

LEAD_BILLING = 3


def main() -> None:
    spark = get_spark("imdb-open-question")
    titles = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    crew = read_clean(spark, "title.crew")
    principals = read_clean(spark, "title.principals")
    names = read_clean(spark, "name.basics")

    films = (titles.where((F.col("titleType") == "movie")
                          & (F.col("runtimeMinutes")
                             >= config.FEATURE_MIN_RUNTIME)
                          & F.col("startYear").isNotNull())
             .join(ratings.where(F.col("numVotes") >= config.MIN_VOTES),
                   "tconst")
             .select("tconst", "primaryTitle", "startYear", "averageRating",
                     "numVotes"))
    directors = crew.select("tconst",
                            F.explode("directors").alias("director"))
    bill_w = Window.partitionBy("tconst").orderBy("ordering")
    leads = (principals.where(F.col("category").isin("actor", "actress"))
             .withColumn("billing", F.row_number().over(bill_w))
             .where(F.col("billing") <= LEAD_BILLING)
             .select("tconst", F.col("nconst").alias("actor"), "billing"))

    # Director baseline over *distinct* films (not weighted by cast size)
    dir_stats = (films.join(directors, "tconst").groupBy("director").agg(
        F.count(F.lit(1)).alias("dir_films"),
        F.avg("averageRating").alias("dir_mean")))
    pairs = (films.join(directors, "tconst").join(leads, "tconst")
             .where(F.col("director") != F.col("actor"))
             .dropDuplicates(["tconst", "director", "actor"])
             .join(dir_stats, "director"))
    pair_w = Window.partitionBy("director", "actor")
    k_w = pair_w.orderBy("startYear", "tconst")
    pairs = (pairs
             .withColumn("pair_films", F.count(F.lit(1)).over(pair_w))
             .withColumn("k", F.row_number().over(k_w))
             .withColumn("rel_rating", F.col("averageRating")
                         - F.col("dir_mean"))
             .where(F.col("dir_films") >= 5)).cache()

    kind = F.when(F.col("pair_films") == 1, "one-off").otherwise("recurring")
    overview = (pairs.groupBy(kind.alias("pair_type")).agg(
        F.countDistinct("director", "actor").alias("pairs"),
        F.count(F.lit(1)).alias("pair_films"),
        F.avg("averageRating").alias("mean_rating"),
        F.avg("rel_rating").alias("mean_rel_rating"),
        F.expr("percentile_approx(numVotes, 0.5)").alias("median_votes"))
        .orderBy("pair_type"))
    results.save(overview, "open", "overview",
                 title="One-off vs recurring director-lead pairings "
                       "(directors with >= 5 rated films)")

    k_bucket = F.when(F.col("k") >= 5, "5+").otherwise(F.col("k")
                                                        .cast("string"))
    by_k = (pairs.where(F.col("pair_films") >= 2)
            .groupBy(k_bucket.alias("collaboration_no")).agg(
                F.count(F.lit(1)).alias("films"),
                F.avg("averageRating").alias("mean_rating"),
                F.avg("rel_rating").alias("mean_rel_rating"),
                F.stddev("rel_rating").alias("std_rel_rating"))
            .withColumn("ci95", 1.96 * F.col("std_rel_rating")
                        / F.sqrt("films"))
            .orderBy("collaboration_no"))
    results.save(by_k, "open", "by_collaboration_number",
                 title="Rating relative to the director's mean, by "
                       "collaboration number (recurring pairs)")

    # Within-pair trajectory: first vs last collaboration
    traj = (pairs.where(F.col("pair_films") >= 3)
            .groupBy("director", "actor", "pair_films").agg(
                F.max_by("rel_rating", F.when(F.col("k") == 1, 1)
                         .otherwise(0)).alias("first_rel"),
                F.max_by("rel_rating", "k").alias("last_rel"),
                F.avg("rel_rating").alias("pair_rel"),
                F.min("startYear").alias("from_year"),
                F.max("startYear").alias("to_year")))
    results.save(traj.agg(
        F.count(F.lit(1)).alias("pairs_3plus"),
        F.avg(F.col("last_rel") - F.col("first_rel")).alias("mean_change"),
        (100 * F.avg((F.col("last_rel") < F.col("first_rel")).cast("int")))
        .alias("pct_declining"),
        F.avg("pair_rel").alias("mean_pair_rel")), "open", "trajectory",
        title="First vs last collaboration (pairs with >= 3 films)")

    by_size = (pairs.groupBy(F.when(F.col("pair_films") >= 6, "6+")
                             .otherwise(F.col("pair_films").cast("string"))
                             .alias("films_together")).agg(
        F.countDistinct("director", "actor").alias("pairs"),
        F.avg("rel_rating").alias("mean_rel_rating"),
        F.avg("averageRating").alias("mean_rating"))
        .orderBy("films_together"))
    results.save(by_size, "open", "by_partnership_size",
                 title="Relative rating by partnership length")

    people = names.select("nconst", "primaryName")
    top = (traj.where("pair_films >= 5")
           .join(people.withColumnRenamed("nconst", "director")
                 .withColumnRenamed("primaryName", "director_name"),
                 "director")
           .join(people.withColumnRenamed("nconst", "actor")
                 .withColumnRenamed("primaryName", "actor_name"), "actor")
           .select("director_name", "actor_name", "pair_films", "from_year",
                   "to_year", "pair_rel", "first_rel", "last_rel")
           .orderBy(F.desc("pair_rel")))
    results.save(top, "open", "top_partnerships", preview=20,
                 title="Partnerships with >= 5 films, best relative rating "
                       "first")
    results.save(top.orderBy("pair_rel"), "open", "worst_partnerships",
                 preview=10, title="Partnerships with >= 5 films, worst "
                                   "relative rating first")
    by_decade = (pairs.withColumn("decade", (F.floor(F.col("startYear") / 10)
                                             * 10).cast("int"))
                 .groupBy("decade").agg(
                     F.avg(F.when(F.col("pair_films") >= 2, 1.0)
                           .otherwise(0.0)).alias("share_recurring"),
                     F.avg(F.when(F.col("pair_films") >= 2,
                                  F.col("rel_rating"))).alias("rel_recurring"),
                     F.avg(F.when(F.col("pair_films") == 1,
                                  F.col("rel_rating"))).alias("rel_one_off"),
                     F.count(F.lit(1)).alias("pair_films"))
                 .where("decade >= 1920").orderBy("decade"))
    results.save(by_decade, "open", "by_decade", preview=12,
                 title="Recurring-pair premium by decade")
    spark.stop()


if __name__ == "__main__":
    main()
