"""PySpark Structured Streaming: Wikipedia recent changes x IMDb.

Kafka topic ``wiki.recentchange`` (raw EventStreams JSON) is parsed with an
explicit schema, restricted to English-Wikipedia article edits, and joined
with the static IMDb lookup built by ``stream_lookup.py``.

Queries (each with its own checkpoint, all on event time ``meta.dt``):

* ``matches``  - every matched (event, IMDb entity) pair, appended to parquet.
* ``windows``  - 5-minute tumbling windows of matched edits per entity type.
* ``volume``   - 5-minute windows of *all* enwiki article edits (denominator
  for the match rate).

Late data: a 10-minute watermark bounds the state of the windowed
aggregations and of the de-duplication; windows are emitted once (append
mode) when the watermark passes their end; records later than the watermark
are dropped and counted by Spark (``numRowsDroppedByWatermark``).
"""

import argparse
import json
import shutil
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener

from imdb_project import config
from imdb_project.spark import get_spark

EVENT_SCHEMA = T.StructType([
    T.StructField("meta", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("dt", T.StringType()),
        T.StructField("domain", T.StringType()),
    ])),
    T.StructField("id", T.LongType()),
    T.StructField("type", T.StringType()),
    T.StructField("namespace", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("user", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("minor", T.BooleanType()),
    T.StructField("length", T.StructType([
        T.StructField("old", T.LongType()),
        T.StructField("new", T.LongType()),
    ])),
    T.StructField("revision", T.StructType([
        T.StructField("old", T.LongType()),
        T.StructField("new", T.LongType()),
    ])),
    T.StructField("server_url", T.StringType()),
    T.StructField("wiki", T.StringType()),
])

PERSON_DISAMBIGUATORS = [
    "", "actor", "actress", "director", "film director", "filmmaker",
    "screenwriter", "producer", "film producer", "writer", "comedian",
    "singer", "musician", "composer", "cinematographer", "voice actor",
    "entertainer", "television presenter", "model",
]


class ProgressFileListener(StreamingQueryListener):
    """Append every progress report as one JSON line (metrics for REPORT)."""

    def __init__(self, path):
        self.path = path

    def onQueryStarted(self, event):
        print(f"[listener] started {event.name} ({event.id})", flush=True)

    def onQueryProgress(self, event):
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(event.progress.json.replace("\n", " ") + "\n")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[listener] terminated {event.id} exception={event.exception}",
              flush=True)


def normalise(col):
    base = F.regexp_replace(col, r"\s*\([^()]*\)\s*$", "")
    return F.lower(F.trim(F.regexp_replace(base, r"\s+", " ")))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=20)
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--reset", action="store_true",
                        help="delete previous outputs and checkpoints")
    args = parser.parse_args()

    out = config.STREAM_DIR
    if args.reset:
        for sub in ("matches", "windows", "volume", "checkpoints"):
            shutil.rmtree(out / sub, ignore_errors=True)
        (out / "progress.jsonl").unlink(missing_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    spark = get_spark("imdb-wiki-streaming",
                      spark__sql__shuffle__partitions="8")
    spark.streams.addListener(ProgressFileListener(out / "progress.jsonl"))
    lookup = F.broadcast(spark.read.parquet(str(out / "lookup")).cache())

    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
             .option("subscribe", config.KAFKA_TOPIC)
             .option("startingOffsets", args.starting_offsets)
             .option("failOnDataLoss", "false")
             .option("maxOffsetsPerTrigger", 200000)
             .load())

    events = (kafka.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("timestamp").alias("kafka_time"))
        .select("e.*", "kafka_time")
        .withColumn("event_time", F.to_timestamp("meta.dt"))
        .withColumn("event_uid", F.coalesce("meta.id",
                                            F.col("id").cast("string")))
        .where((F.col("wiki") == "enwiki") & (F.col("namespace") == 0)
               & F.col("type").isin("edit", "new")
               & F.col("event_time").isNotNull())
        .withWatermark("event_time", args.watermark))

    keyed = (events
             .withColumn("disamb", F.lower(F.regexp_extract(
                 "title", r"\(([^()]*)\)\s*$", 1)))
             .withColumn("keys", F.array(
                 F.struct(F.lit("exact").alias("key_type"),
                          F.lower("title").alias("match_key")),
                 F.struct(F.lit("base").alias("key_type"),
                          normalise(F.col("title")).alias("match_key"))))
             .withColumn("k", F.explode("keys"))
             .select("*", "k.key_type", "k.match_key").drop("keys", "k"))

    year_in_disamb = F.regexp_extract("disamb", r"(\d{4})", 1)
    rule = (
        (F.col("key_type") == "exact")
        | ((F.col("entity_type") == "film")
           & ((F.col("disamb").rlike(r"(^|\s)film$")
               & ((year_in_disamb == "")
                  | (year_in_disamb.cast("int") == F.col("year"))))
              | ((F.col("disamb") == "") & (F.col("popularity") >= 100000))))
        | ((F.col("entity_type") == "series")
           & (F.col("disamb").rlike(r"(tv series|miniseries|tv program)")
              | ((F.col("disamb") == "") & (F.col("popularity") >= 100000))))
        # People: an occupational disambiguator ("X (actor)") is enough; a
        # bare name must have >= 2 words and belong to a very popular person
        # (homonyms such as "Asia" or politicians otherwise flood the stream).
        | ((F.col("entity_type") == "person")
           & ((F.col("disamb").isin(PERSON_DISAMBIGUATORS)
               & (F.col("disamb") != ""))
              | ((F.col("disamb") == "")
                 & F.col("imdb_name").contains(" ")
                 & (F.col("popularity") >= 100000))))
    )
    matches = (keyed.join(lookup, ["key_type", "match_key"])
               .where(rule)
               .withColumn("confidence", F.when(F.col("key_type") == "exact",
                                                "high").otherwise("medium"))
               .withColumn("byte_delta", F.col("length.new")
                           - F.coalesce(F.col("length.old"), F.lit(0)))
               .withColumn("lateness_sec", F.col("kafka_time").cast("long")
                           - F.col("event_time").cast("long"))
               # Kafka/SSE re-deliveries: same event matched twice
               .dropDuplicatesWithinWatermark(["event_uid", "imdb_id"])
               .select("event_time", "kafka_time", "lateness_sec", "event_uid",
                       "title", "type", "user", "bot", "byte_delta",
                       "comment", "entity_type", "imdb_id", "imdb_name",
                       "confidence", "key_type",
                       F.col("revision.new").alias("revision_id")))

    trigger = {"processingTime": args.trigger}
    queries = [
        matches.writeStream.queryName("matches").format("parquet")
        .option("path", str(out / "matches"))
        .option("checkpointLocation", str(out / "checkpoints" / "matches"))
        .outputMode("append").trigger(**trigger).start(),

        matches.groupBy(F.window("event_time", args.window), "entity_type")
        .agg(F.count(F.lit(1)).alias("matched_edits"),
             F.approx_count_distinct("imdb_id").alias("distinct_entities"),
             F.sum(F.col("bot").cast("int")).alias("bot_edits"),
             F.sum("byte_delta").alias("byte_delta"),
             F.max("lateness_sec").alias("max_lateness_sec"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "entity_type",
                "matched_edits", "distinct_entities", "bot_edits",
                "byte_delta", "max_lateness_sec")
        .writeStream.queryName("windows").format("parquet")
        .option("path", str(out / "windows"))
        .option("checkpointLocation", str(out / "checkpoints" / "windows"))
        .outputMode("append").trigger(**trigger).start(),

        events.groupBy(F.window("event_time", args.window))
        .agg(F.count(F.lit(1)).alias("enwiki_edits"),
             F.sum(F.col("bot").cast("int")).alias("bot_edits"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "enwiki_edits",
                "bot_edits")
        .writeStream.queryName("volume").format("parquet")
        .option("path", str(out / "volume"))
        .option("checkpointLocation", str(out / "checkpoints" / "volume"))
        .outputMode("append").trigger(**trigger).start(),

        matches.select("event_time", "entity_type", "confidence",
                       "imdb_name", "title")
        .writeStream.queryName("console").format("console")
        .option("truncate", "false").option("numRows", 15)
        .outputMode("append").trigger(**trigger).start(),
    ]

    deadline = time.time() + args.minutes * 60
    while time.time() < deadline:
        time.sleep(15)
        for q in queries:
            if q.exception():
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")
        p = queries[0].lastProgress
        if p:
            print(json.dumps({"t": p["timestamp"], "batch": p["batchId"],
                              "in_rows": p["numInputRows"],
                              "in_rps": p.get("inputRowsPerSecond"),
                              "proc_rps": p.get("processedRowsPerSecond")}),
                  flush=True)
    # Plain stop: the producer keeps writing, so processAllAvailable() could
    # chase the head of the topic forever. Sinks commit per micro-batch, so
    # stopping loses nothing that was committed; a re-run resumes from the
    # checkpoints.
    for q in queries:
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()
