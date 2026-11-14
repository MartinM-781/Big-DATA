"""Summarise the outputs of a streaming run into results/stream/*."""

import json

from pyspark.sql import functions as F

from imdb_project import config, results
from imdb_project.spark import get_spark


def main() -> None:
    spark = get_spark("imdb-stream-report")
    out = config.STREAM_DIR
    matches = spark.read.parquet(str(out / "matches"))
    windows = spark.read.parquet(str(out / "windows"))
    volume = spark.read.parquet(str(out / "volume"))

    span = matches.agg(F.min("event_time").alias("first_event"),
                       F.max("event_time").alias("last_event"),
                       F.count(F.lit(1)).alias("matched_pairs"),
                       F.countDistinct("event_uid").alias("matched_events"),
                       F.countDistinct("imdb_id").alias("distinct_entities"))
    results.save(span, "stream", "run_summary", title="Streaming run summary")
    results.save(matches.groupBy("entity_type", "confidence").agg(
        F.countDistinct("event_uid").alias("events"),
        F.countDistinct("imdb_id").alias("entities"),
        F.sum(F.col("bot").cast("int")).alias("bot_edits"))
        .orderBy("entity_type", "confidence"), "stream", "matches_by_type",
        title="Matched events by entity type and confidence")
    results.save(matches.groupBy("entity_type", "imdb_id", "imdb_name").agg(
        F.count(F.lit(1)).alias("edits"),
        F.countDistinct("user").alias("editors"),
        F.sum("byte_delta").alias("byte_delta"),
        F.first("title").alias("article"))
        .orderBy(F.desc("edits"), "imdb_name").limit(25), "stream",
        "top_entities", title="Most edited IMDb entities during the run")
    results.save(matches.orderBy("event_time")
                 .select("event_time", "lateness_sec", "entity_type",
                         "confidence", "imdb_id", "imdb_name", "title", "user",
                         "byte_delta").limit(40),
                 "stream", "sample_matches", preview=40,
                 title="First matched events")
    results.save(windows.orderBy("window_start", "entity_type"), "stream",
                 "windows_emitted", preview=40,
                 title="Windows finalised by the watermark (append mode)")
    rate = (windows.groupBy("window_start").agg(
        F.sum("matched_edits").alias("matched_edits"))
        .join(volume.select("window_start", "enwiki_edits"), "window_start")
        .withColumn("match_rate_pct", 100 * F.col("matched_edits")
                    / F.col("enwiki_edits")).orderBy("window_start"))
    results.save(rate, "stream", "match_rate", preview=40,
                 title="Match rate per window")
    late = matches.select(F.percentile_approx(
        "lateness_sec", [0.5, 0.9, 0.99, 1.0]).alias("p"),
        F.sum((F.col("lateness_sec") > 600).cast("int")).alias("gt_10min"))
    results.save(late.select(
        F.col("p")[0].alias("p50_sec"), F.col("p")[1].alias("p90_sec"),
        F.col("p")[2].alias("p99_sec"), F.col("p")[3].alias("max_sec"),
        "gt_10min"), "stream", "lateness",
        title="Event-time lateness (Kafka ingest time - event time) of "
              "accepted matches")

    # Progress metrics from the listener
    rows = []
    path = out / "progress.jsonl"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            p = json.loads(line)
            dropped = sum(s.get("numRowsDroppedByWatermark", 0)
                          for s in p.get("stateOperators", []))
            state = sum(s.get("numRowsTotal", 0)
                        for s in p.get("stateOperators", []))
            rows.append((p["name"], p["batchId"], p["numInputRows"],
                         float(p.get("inputRowsPerSecond") or 0.0),
                         float(p.get("processedRowsPerSecond") or 0.0),
                         int(p.get("durationMs", {}).get("triggerExecution",
                                                         0)),
                         dropped, state, p.get("eventTime", {})
                         .get("watermark")))
    if rows:
        prog = spark.createDataFrame(rows, [
            "query", "batch", "input_rows", "input_rps", "processed_rps",
            "trigger_ms", "dropped_by_watermark", "state_rows", "watermark"])
        results.save(prog.groupBy("query").agg(
            F.count(F.lit(1)).alias("micro_batches"),
            F.sum("input_rows").alias("input_rows"),
            F.avg("input_rps").alias("avg_input_rps"),
            F.avg("processed_rps").alias("avg_processed_rps"),
            F.avg("trigger_ms").alias("avg_trigger_ms"),
            F.max("trigger_ms").alias("max_trigger_ms"),
            F.sum("dropped_by_watermark").alias("dropped_by_watermark"),
            F.max("state_rows").alias("max_state_rows"),
            F.max("watermark").alias("final_watermark"))
            .orderBy("query"), "stream", "query_metrics",
            title="Structured Streaming metrics per query")
    spark.stop()


if __name__ == "__main__":
    main()
