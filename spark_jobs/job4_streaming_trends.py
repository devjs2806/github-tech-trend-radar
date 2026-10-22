#!/usr/bin/env python3
"""
JOB 4 - Real-time trend detection with Spark Structured Streaming.

Pipeline:  GitHub Events API -> fetch_live_events.py -> Flume -> HDFS /github/raw/live
           -> THIS JOB (reads new files as they arrive) -> live results

Every micro-batch (STREAM_TRIGGER) it updates sliding windows of
STREAM_WINDOW length (sliding every STREAM_SLIDE) and writes:
  output/live_trending.csv     - most-starred repos in the newest window
  output/live_event_types.csv  - event-type mix in the newest window
  output/live_throughput.csv   - events per minute (shows the stream is live)
and prints the leaderboard on the console.

Run (keep fetch_live_events.py and Flume running at the same time):
  spark-submit spark_jobs/job4_streaming_trends.py
Stop with Ctrl+C.
"""
import os
import shutil
import sys
from datetime import datetime, timezone

from common import EVENT_SCHEMA, config, get_spark
from pyspark.sql import functions as F


def write_csv(df, name):
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, name)
    df.toPandas().to_csv(path + ".tmp", index=False)
    os.replace(path + ".tmp", path)


def main():
    reset = "--reset" in sys.argv
    if reset and os.path.isdir(config.CHECKPOINT_DIR):
        shutil.rmtree(config.CHECKPOINT_DIR)
        print("checkpoints cleared")

    spark = get_spark("GitHubTrendRadar-Job4-Streaming")
    print(f"Watching {config.LIVE_PATH} for new files ...")

    raw = (spark.readStream.schema(EVENT_SCHEMA)
           .option("maxFilesPerTrigger", 50)
           .json(config.LIVE_PATH))
    ev = (raw.withColumn("ts", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
          .filter(F.col("ts").isNotNull() & F.col("repo_name").isNotNull()))

    # 1) sliding-window counts per repo and event type
    windowed = (ev.groupBy(F.window("ts", config.STREAM_WINDOW, config.STREAM_SLIDE).alias("w"),
                           "repo_name", "type")
                .count())

    # 2) events per minute
    per_min = ev.groupBy(F.window("ts", "1 minute").alias("w")).count()

    def handle_windows(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] no data yet")
            return
        latest_end = batch_df.agg(F.max("w.end")).first()[0]
        newest = batch_df.filter(F.col("w.end") == F.lit(latest_end)).cache()

        trending = (newest.filter(F.col("type") == "WatchEvent")
                    .groupBy("repo_name").agg(F.sum("count").alias("stars_in_window"))
                    .join(newest.groupBy("repo_name").agg(F.sum("count").alias("all_events")), "repo_name")
                    .orderBy(F.desc("stars_in_window"), F.desc("all_events"))
                    .limit(20)
                    .withColumn("window_end_utc", F.lit(str(latest_end)))
                    .withColumn("updated_at_utc", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))))
        types = (newest.groupBy("type").agg(F.sum("count").alias("events"))
                 .orderBy(F.desc("events"))
                 .withColumn("window_end_utc", F.lit(str(latest_end))))

        write_csv(trending, "live_trending.csv")
        write_csv(types, "live_event_types.csv")
        print(f"\n[batch {batch_id}] window ending {latest_end} UTC - top starred repos:")
        trending.select("repo_name", "stars_in_window", "all_events").show(10, truncate=False)
        newest.unpersist()

    def handle_throughput(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        out = (batch_df.select(F.col("w.start").alias("minute_utc"), F.col("count").alias("events"))
               .orderBy(F.desc("minute_utc")).limit(120).orderBy("minute_utc"))
        write_csv(out, "live_throughput.csv")

    q1 = (windowed.writeStream.outputMode("complete")
          .foreachBatch(handle_windows)
          .option("checkpointLocation", os.path.join(config.CHECKPOINT_DIR, "windows"))
          .trigger(processingTime=config.STREAM_TRIGGER)
          .start())
    q2 = (per_min.writeStream.outputMode("complete")
          .foreachBatch(handle_throughput)
          .option("checkpointLocation", os.path.join(config.CHECKPOINT_DIR, "throughput"))
          .trigger(processingTime=config.STREAM_TRIGGER)
          .start())

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries ...")
        q1.stop()
        q2.stop()
        spark.stop()


if __name__ == "__main__":
    main()
