#!/usr/bin/env python3
"""
JOB 2 - Technology (language + topic) trends.

Joins the raw star events with repo metadata (output/repo_meta.csv, made by
enrich_metadata.py) to answer: which LANGUAGES and TOPICS are gaining the most
attention right now, and which are accelerating?

Result tables:
  language_summary   - stars, repos, share and momentum per language
  language_timeline  - stars per language per 6-hour block (for line charts)
  topic_trends       - most-starred topics (AI, web, devops ...) with momentum

Run AFTER job 1 and enrich_metadata.py:
  spark-submit spark_jobs/job2_language_trends.py
"""
import os
import time

import pandas as pd
from common import banner, config, get_spark, load_events, save_result
from pyspark.sql import functions as F


def main():
    t0 = time.time()
    meta_path = os.path.join(config.OUTPUT_DIR, "repo_meta.csv")
    if not os.path.exists(meta_path):
        raise SystemExit("output/repo_meta.csv not found. Run: python3 enrich_metadata.py")

    spark = get_spark("GitHubTrendRadar-Job2-LanguageTrends")
    meta_pd = pd.read_csv(meta_path, dtype=str).fillna("")
    meta_pd = meta_pd[meta_pd["status"] == "ok"][["repo_name", "language", "topics"]]
    if meta_pd.empty:
        raise SystemExit("repo_meta.csv has no usable rows.")
    meta = spark.createDataFrame(meta_pd)
    print(f"  metadata for {len(meta_pd)} repos")

    banner("Loading star events")
    ev = load_events(spark)
    stars = ev.filter(F.col("type") == "WatchEvent").select("repo_name", "ts").join(F.broadcast(meta), "repo_name")
    stars = stars.cache()

    bounds = stars.agg(F.min("ts").alias("a"), F.max("ts").alias("b")).first()
    if bounds["a"] is None:
        raise SystemExit("No star events for the enriched repos.")
    end_sec = int(bounds["b"].timestamp())
    half = max(3600, (end_sec - int(bounds["a"].timestamp())) // 2)
    sec = F.unix_timestamp("ts")
    period = F.when(sec > end_sec - half, "recent").otherwise("previous")

    banner("Saving results")
    # ---------- languages ----------
    lang = (stars.withColumn("period", period)
            .groupBy("language").agg(
                F.count("*").alias("stars"),
                F.approx_count_distinct("repo_name").alias("repos"),
                F.sum(F.when(F.col("period") == "recent", 1).otherwise(0)).alias("recent_stars"),
                F.sum(F.when(F.col("period") == "previous", 1).otherwise(0)).alias("previous_stars")))
    total = stars.count()
    tp = stars.withColumn("period", period).groupBy().agg(
        F.sum(F.when(F.col("period") == "recent", 1).otherwise(0)).alias("r"),
        F.sum(F.when(F.col("period") == "previous", 1).otherwise(0)).alias("p")).first()
    tot_r, tot_p = max(tp["r"] or 0, 1), max(tp["p"] or 0, 1)
    share_momentum = F.round(((F.col("recent_stars") + 1) / tot_r) / ((F.col("previous_stars") + 1) / tot_p), 2)
    lang = (lang.withColumn("share_percent", F.round(F.col("stars") * 100.0 / total, 2))
            .withColumn("momentum", share_momentum)
            .withColumn("trend", F.when(F.col("momentum") >= 1.1, "rising")
                        .when(F.col("momentum") <= 0.9, "cooling").otherwise("stable"))
            .orderBy(F.desc("stars")))
    save_result(lang, "language_summary", spark)

    top_langs = [r["language"] for r in lang.limit(8).collect()]
    timeline = (stars.filter(F.col("language").isin(top_langs))
                .withColumn("block", F.window("ts", "6 hours").start)
                .groupBy("block", "language").count()
                .withColumnRenamed("count", "stars")
                .orderBy("block", "language"))
    save_result(timeline, "language_timeline", spark)

    # ---------- topics ----------
    topics = (stars.filter(F.col("topics") != "")
              .withColumn("topic", F.explode(F.split("topics", r"\|")))
              .withColumn("period", period)
              .groupBy("topic").agg(
                  F.count("*").alias("stars"),
                  F.approx_count_distinct("repo_name").alias("repos"),
                  F.sum(F.when(F.col("period") == "recent", 1).otherwise(0)).alias("recent_stars"),
                  F.sum(F.when(F.col("period") == "previous", 1).otherwise(0)).alias("previous_stars"))
              .filter(F.col("repos") >= 2)
              .withColumn("momentum", share_momentum)
              .orderBy(F.desc("stars")).limit(config.TOP_N))
    save_result(topics, "topic_trends", spark)

    banner(f"Job 2 finished in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
