"""Original-language resolution for IMDb titles.

IMDb exposes no "original language" column, so two sources are combined:

1. **Wikidata** property P364 ("original language of film or TV show"),
   fetched by IMDb id (P345) with ``jobs/enrich_fetch.py --only languages``.
   Human-curated and exact: this is the primary source.
2. **Fallback heuristic** on ``title.akas`` for titles without a Wikidata
   value: among the akas whose text equals the ``originalTitle``, take the
   explicit ``language`` code, else map the ``region`` to its dominant
   language with the table below; the most frequent candidate wins (ties:
   explicit code, then lowest ``ordering``).

The heuristic alone is noisy: many regions (AE, NL, MX, ...) reuse the
romanised original title of foreign films, so a Korean film can collect
"ar" or "nl" votes. That is why Wikidata comes first; ``language_source``
records which method produced each value.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from imdb_project import config

# Dominant (first official / most spoken) language per region code.
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "CA": "en", "AU": "en", "NZ": "en", "IE": "en",
    "ZA": "en", "NG": "en", "JM": "en", "SG": "en",
    "FR": "fr", "BE": "fr", "CH": "de", "LU": "fr", "SN": "fr", "CI": "fr",
    "DE": "de", "AT": "de", "XWG": "de", "DDDE": "de",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "VE": "es", "UY": "es", "CU": "es", "EC": "es", "BO": "es", "PY": "es",
    "DO": "es", "GT": "es", "CR": "es", "PR": "es",
    "IT": "it", "PT": "pt", "BR": "pt", "NL": "nl", "SE": "sv", "NO": "no",
    "DK": "da", "FI": "fi", "IS": "is", "PL": "pl", "CZ": "cs", "CSHH": "cs",
    "SK": "sk", "HU": "hu", "RO": "ro", "BG": "bg", "GR": "el", "TR": "tr",
    "RU": "ru", "SUHH": "ru", "UA": "uk", "BY": "be", "RS": "sr", "YUCS": "sr",
    "HR": "hr", "SI": "sl", "BA": "bs", "MK": "mk", "AL": "sq", "EE": "et",
    "LV": "lv", "LT": "lt", "GE": "ka", "AM": "hy", "AZ": "az", "KZ": "kk",
    "IL": "he", "IR": "fa", "EG": "ar", "MA": "ar", "DZ": "ar", "TN": "ar",
    "LB": "ar", "SA": "ar", "AE": "ar", "IQ": "ar", "SY": "ar", "JO": "ar",
    "IN": "hi", "PK": "ur", "BD": "bn", "LK": "si", "NP": "ne",
    "CN": "zh", "TW": "zh", "HK": "zh", "JP": "ja", "KR": "ko", "TH": "th",
    "VN": "vi", "ID": "id", "MY": "ms", "PH": "tl",
}

WIKIDATA_LANG_SCHEMA = T.StructType([
    T.StructField("imdb_id", T.StringType()),
    T.StructField("languages", T.ArrayType(T.StringType())),
    T.StructField("retrieved_at", T.StringType()),
])


def region_language_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(list(REGION_LANGUAGE.items()),
                                 ["region", "region_language"])


def heuristic_language(spark: SparkSession, titles: DataFrame,
                       akas: DataFrame) -> DataFrame:
    """Return (tconst, original_language, language_source) from akas."""
    cand = (
        akas.join(titles.select(F.col("tconst").alias("titleId"),
                                "originalTitle"), "titleId")
        .where(F.lower(F.col("title")) == F.lower(F.col("originalTitle")))
        .join(F.broadcast(region_language_df(spark)), "region", "left")
        .withColumn("lang", F.coalesce(F.lower("language"),
                                       F.col("region_language")))
        .where(F.col("lang").isNotNull())
        .groupBy("titleId", "lang")
        .agg(F.count(F.lit(1)).alias("votes"),
             F.min("ordering").alias("first_ordering"),
             F.max(F.col("language").isNotNull().cast("int"))
             .alias("explicit"))
    )
    rank = Window.partitionBy("titleId").orderBy(
        F.desc("votes"), F.desc("explicit"), F.asc("first_ordering"))
    return (
        cand.withColumn("r", F.row_number().over(rank)).where("r = 1")
        .select(F.col("titleId").alias("tconst"),
                F.col("lang").alias("original_language"),
                F.lit("akas_heuristic").alias("language_source"))
    )


def wikidata_languages(spark: SparkSession) -> DataFrame:
    """(tconst, languages) from Wikidata P364; empty if not fetched yet."""
    path = config.ENRICH_DIR / "raw" / "films_language.jsonl"
    if not path.exists():
        return spark.createDataFrame([], "tconst string, "
                                     "languages array<string>")
    raw = spark.read.schema(WIKIDATA_LANG_SCHEMA).json(str(path))
    return (raw.where(F.size("languages") > 0)
            .dropDuplicates(["imdb_id"])
            .select(F.col("imdb_id").alias("tconst"),
                    F.array_sort("languages").alias("languages")))


def original_language(spark: SparkSession, titles: DataFrame,
                      akas: DataFrame) -> DataFrame:
    """Wikidata first, akas heuristic as a fallback.

    A film with several Wikidata original languages (e.g. en+de+fr) is
    assigned the one the heuristic agrees with, else the first code in
    alphabetical order, so that one film counts for one language only.
    """
    wd = wikidata_languages(spark)
    heur = heuristic_language(spark, titles, akas)
    resolved = (
        wd.join(heur.select("tconst", F.col("original_language")
                            .alias("heur")), "tconst", "left")
        .select("tconst",
                F.when(F.array_contains("languages", F.col("heur")),
                       F.col("heur"))
                .otherwise(F.col("languages")[0]).alias("original_language"),
                F.lit("wikidata_P364").alias("language_source")))
    return resolved.unionByName(heur.join(wd.select("tconst"), "tconst",
                                          "left_anti"))
