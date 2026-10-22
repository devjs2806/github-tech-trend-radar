#!/usr/bin/env python3
"""
JOB 1 - Core big-data analysis of GitHub activity (batch, PySpark).

Produces these result tables (output/*.csv and Hive github_radar.*):
  dataset_summary      - size and time range of the dataset
  event_type_counts    - how many of each event type
  hourly_activity      - events per hour over time (time series)
  activity_heatmap     - day-of-week x hour-of-day activity (IST)
  top_starred_repos    - most-starred repos in the whole period
  trending_repos       - repos whose star rate is ACCELERATING (recent vs previous window)
  emerging_repos       - brand-new repos already collecting stars
  top_orgs             - most active organisations
  top_contributors     - most active humans (bots removed)
  repo_health          - event mix per repo: active vs hype-only
  bot_activity         - share of events made by bots, per event type

Run:  spark-submit spark_jobs/job1_trend_analysis.py
"""
import time

from common import banner, config, get_spark, load_events, save_result
from pyspark.sql import Window, functions as F


def main():
    t0 = time.time()
    spark = get_spark("GitHubTrendRadar-Job1-TrendAnalysis")
    banner("Loading events")
    ev = load_events(spark, include_live=False).cache()

    # ---------------- dataset summary ----------------
    summ = ev.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("repo_name").alias("distinct_repos"),
        F.countDistinct("actor_login").alias("distinct_users"),
        F.min("ts").alias("first_event_utc"),
        F.max("ts").alias("last_event_utc"),
    ).withColumn("hours_covered",
                 F.round((F.unix_timestamp("last_event_utc") - F.unix_timestamp("first_event_utc")) / 3600, 1))
    row = summ.first()
    if row["total_events"] == 0:
        raise SystemExit("Dataset is empty.")
    print(f"  {row['total_events']:,} events | {row['distinct_repos']:,} repos | "
          f"{row['distinct_users']:,} users | {row['first_event_utc']} -> {row['last_event_utc']}")
    banner("Saving results")
    save_result(summ, "dataset_summary", spark)

    # ---------------- event types ----------------
    total = row["total_events"]
    etype = (ev.groupBy("type").count()
             .withColumn("percent", F.round(F.col("count") * 100.0 / total, 2))
             .orderBy(F.desc("count")))
    save_result(etype, "event_type_counts", spark)

    # ---------------- hourly time series ----------------
    hourly = (ev.groupBy("hour_ts").agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("type") == "WatchEvent", 1).otherwise(0)).alias("stars"),
        F.sum(F.when(F.col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
        F.sum(F.when(F.col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_requests"),
        F.countDistinct("actor_login").alias("active_users"))
        .orderBy("hour_ts"))
    save_result(hourly, "hourly_activity", spark)

    # ---------------- heatmap (local time) ----------------
    local = F.from_utc_timestamp("ts", config.HEATMAP_TIMEZONE)
    heat = (ev.withColumn("local_ts", local)
            .withColumn("dow_num", F.dayofweek("local_ts"))           # 1=Sun..7=Sat
            .withColumn("dow", F.date_format("local_ts", "EEE"))
            .withColumn("hour", F.hour("local_ts"))
            .groupBy("dow_num", "dow", "hour").count()
            .orderBy("dow_num", "hour"))
    save_result(heat, "activity_heatmap", spark)

    # ---------------- stars ----------------
    stars = ev.filter(F.col("type") == "WatchEvent")
    forks = ev.filter(F.col("type") == "ForkEvent").groupBy("repo_name").agg(F.count("*").alias("forks"))

    top_starred = (stars.groupBy("repo_name").agg(F.count("*").alias("stars"),
                                                  F.countDistinct("actor_login").alias("unique_stargazers"))
                   .join(forks, "repo_name", "left").fillna(0, ["forks"])
                   .orderBy(F.desc("stars")).limit(config.TOP_N))
    save_result(top_starred, "top_starred_repos", spark)

    # ---------------- trending (acceleration) ----------------
    hours_cov = max(row["hours_covered"] or 0, 2)
    window_h = int(min(config.MAX_TREND_WINDOW_HOURS, max(1, hours_cov // 2)))
    end_ts = row["last_event_utc"]
    end_sec = int(end_ts.timestamp()) if hasattr(end_ts, "timestamp") else None
    sec = F.unix_timestamp("ts")
    w = window_h * 3600
    print(f"  trend windows: recent = last {window_h}h, previous = {window_h}h before that")

    win = (stars.withColumn("bucket",
                            F.when(sec > end_sec - w, "recent")
                            .when(sec > end_sec - 2 * w, "previous"))
           .filter(F.col("bucket").isNotNull())
           .groupBy("repo_name")
           .pivot("bucket", ["recent", "previous"]).count()
           .fillna(0))
    trending = (win.filter(F.col("recent") >= config.MIN_RECENT_STARS)
                .withColumn("growth_ratio", F.round((F.col("recent") + 1) / (F.col("previous") + 1), 2))
                # score rewards both speed-up and absolute size
                .withColumn("trend_score",
                            F.round(F.log1p("recent") * F.log2(F.col("growth_ratio") + 1), 3))
                .withColumn("window_hours", F.lit(window_h))
                .orderBy(F.desc("trend_score")).limit(config.TOP_N))
    save_result(trending, "trending_repos", spark)

    # ---------------- emerging (new repos) ----------------
    created = (ev.filter((F.col("type") == "CreateEvent") & (F.col("ref_type") == "repository"))
               .groupBy("repo_name").agg(F.min("ts").alias("created_ts")))
    emerging = (created.join(stars, "repo_name")
                .filter(F.col("ts") >= F.col("created_ts"))
                .groupBy("repo_name", "created_ts").agg(F.count("*").alias("stars_since_creation"))
                .filter(F.col("stars_since_creation") >= config.MIN_EMERGING_STARS)
                .withColumn("hours_old",
                            F.round((F.lit(end_sec) - F.unix_timestamp("created_ts")) / 3600, 1))
                .withColumn("stars_per_hour",
                            F.round(F.col("stars_since_creation") / F.greatest(F.col("hours_old"), F.lit(1.0)), 2))
                .orderBy(F.desc("stars_since_creation")).limit(config.TOP_N))
    save_result(emerging, "emerging_repos", spark)

    # ---------------- organisations & contributors ----------------
    orgs = (ev.filter(F.col("org_login").isNotNull())
            .groupBy("org_login").agg(F.count("*").alias("events"),
                                      F.countDistinct("repo_name").alias("repos"),
                                      F.countDistinct("actor_login").alias("contributors"))
            .orderBy(F.desc("events")).limit(config.TOP_N))
    save_result(orgs, "top_orgs", spark)

    humans = ev.filter(~F.col("is_bot") & F.col("type").isin("PushEvent", "PullRequestEvent",
                                                            "IssuesEvent", "PullRequestReviewEvent",
                                                            "IssueCommentEvent"))
    contrib = (humans.groupBy("actor_login").agg(F.count("*").alias("events"),
                                                 F.countDistinct("repo_name").alias("repos_touched"))
               .orderBy(F.desc("events")).limit(config.TOP_N))
    save_result(contrib, "top_contributors", spark)

    bots = (ev.groupBy("type").agg(F.count("*").alias("events"),
                                   F.sum(F.col("is_bot").cast("int")).alias("bot_events"))
            .withColumn("bot_percent", F.round(F.col("bot_events") * 100.0 / F.col("events"), 2))
            .orderBy(F.desc("events")))
    save_result(bots, "bot_activity", spark)

    # ---------------- repo health (event mix) ----------------
    def cnt(event_type, name):
        return F.sum(F.when(F.col("type") == event_type, 1).otherwise(0)).alias(name)

    health = (ev.groupBy("repo_name").agg(
                  F.count("*").alias("total_events"),
                  cnt("WatchEvent", "stars"), cnt("ForkEvent", "forks"), cnt("PushEvent", "pushes"),
                  cnt("PullRequestEvent", "pull_requests"), cnt("IssuesEvent", "issues"),
                  F.countDistinct("actor_login").alias("unique_actors"))
              .filter(F.col("stars") >= config.MIN_RECENT_STARS)
              .withColumn("dev_activity", F.col("pushes") + F.col("pull_requests") + F.col("issues"))
              .withColumn("dev_to_star_ratio", F.round(F.col("dev_activity") / F.col("stars"), 3))
              .withColumn("status",
                          F.when(F.col("dev_to_star_ratio") >= 1.0, "actively developed")
                          .when(F.col("dev_to_star_ratio") >= 0.2, "healthy")
                          .otherwise("hype (stars, little dev)"))
              .orderBy(F.desc("stars")).limit(config.TOP_N * 4))
    save_result(health, "repo_health", spark)

    banner(f"Job 1 finished in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
