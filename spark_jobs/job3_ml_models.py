#!/usr/bin/env python3
"""
JOB 3 - Machine learning with Spark MLlib.

Part A: STAR FORECASTING (regression)
  For the top repos, build an hourly time series and predict how many stars a
  repo will get in the next hour from its recent history (lag features).
  Models: Linear Regression and Gradient-Boosted Trees, compared against a
  naive baseline ("next hour = this hour"). Time-based train/test split.

Part B: REPO CLUSTERING (unsupervised)
  K-Means groups repos by their activity profile (share of stars, forks,
  pushes, PRs, issues) into types such as "viral / hype" and
  "active development".

Result tables:
  ml_forecast_metrics      - RMSE / MAE / R2 for each model
  ml_forecast_predictions  - actual vs predicted stars on the test hours
  ml_next_hour_forecast    - predicted stars for the next hour per repo
  ml_feature_importance    - which features the GBT model relies on
  repo_clusters            - every clustered repo with its cluster name
  cluster_summary          - average profile of each cluster

Run:  spark-submit spark_jobs/job3_ml_models.py
"""
import time
from datetime import timedelta

from common import banner, config, get_spark, load_events, save_result
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import Window, functions as F

FEATURES = ["lag1", "lag2", "lag3", "lag6", "roll6_mean", "forks_lag1", "pushes_lag1",
            "actors_lag1", "hour_of_day"]


# ----------------------------------------------------------------- forecasting
def build_hourly_dataset(spark, ev):
    top = (ev.filter(F.col("type") == "WatchEvent").groupBy("repo_name").count()
           .orderBy(F.desc("count")).limit(config.FORECAST_TOP_REPOS).select("repo_name"))
    sub = ev.join(F.broadcast(top), "repo_name")

    agg = sub.groupBy("repo_name", "hour_ts").agg(
        F.sum(F.when(F.col("type") == "WatchEvent", 1).otherwise(0)).alias("stars"),
        F.sum(F.when(F.col("type") == "ForkEvent", 1).otherwise(0)).alias("forks"),
        F.sum(F.when(F.col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
        F.approx_count_distinct("actor_login").alias("actors"))

    # full repo x hour grid so that hours with no activity become 0
    b = ev.agg(F.min("hour_ts").alias("a"), F.max("hour_ts").alias("b")).first()
    hours = spark.sql(
        f"SELECT explode(sequence(timestamp'{b['a']}', timestamp'{b['b']}', interval 1 hour)) AS hour_ts")
    grid = top.crossJoin(hours)
    df = grid.join(agg, ["repo_name", "hour_ts"], "left").fillna(0, ["stars", "forks", "pushes", "actors"])

    w = Window.partitionBy("repo_name").orderBy("hour_ts")
    w6 = w.rowsBetween(-6, -1)
    df = (df.withColumn("lag1", F.lag("stars", 1).over(w))
          .withColumn("lag2", F.lag("stars", 2).over(w))
          .withColumn("lag3", F.lag("stars", 3).over(w))
          .withColumn("lag6", F.lag("stars", 6).over(w))
          .withColumn("roll6_mean", F.avg("stars").over(w6))
          .withColumn("forks_lag1", F.lag("forks", 1).over(w))
          .withColumn("pushes_lag1", F.lag("pushes", 1).over(w))
          .withColumn("actors_lag1", F.lag("actors", 1).over(w))
          .withColumn("hour_of_day", F.hour("hour_ts")))
    return df, b["b"]


def forecasting(spark, ev):
    banner("Part A: star forecasting")
    df, last_hour = build_hourly_dataset(spark, ev)
    data = df.dropna(subset=FEATURES).withColumn("label", F.col("stars").cast("double")).cache()

    hours = [r["hour_ts"] for r in data.select("hour_ts").distinct().orderBy("hour_ts").collect()]
    if len(hours) < 10:
        print("  not enough hours of data for forecasting (need >= 10 after lags). Download more data.")
        return
    split_hour = hours[int(len(hours) * 0.8)]
    train = data.filter(F.col("hour_ts") < F.lit(split_hour))
    test = data.filter(F.col("hour_ts") >= F.lit(split_hour))
    print(f"  {len(hours)} usable hours | train < {split_hour} <= test | "
          f"train rows {train.count():,}, test rows {test.count():,}")

    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    train_v, test_v = assembler.transform(train), assembler.transform(test)

    lr = LinearRegression(featuresCol="features", labelCol="label", regParam=0.1).fit(train_v)
    gbt = GBTRegressor(featuresCol="features", labelCol="label", maxIter=40, maxDepth=5, seed=42).fit(train_v)

    rows = []
    preds = {}
    for name, model in [("Linear Regression", lr), ("Gradient-Boosted Trees", gbt)]:
        p = model.transform(test_v).withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
        preds[name] = p
        rows.append((name, *metrics(p, "prediction")))
    naive = test_v.withColumn("prediction", F.col("lag1").cast("double"))
    rows.append(("Naive baseline (last hour)", *metrics(naive, "prediction")))

    m = spark.createDataFrame(rows, ["model", "rmse", "mae", "r2"])
    m.show(truncate=False)
    save_result(m, "ml_forecast_metrics", spark)

    best_name = min(rows[:2], key=lambda r: r[1])[0]
    best = lr if best_name.startswith("Linear") else gbt
    print(f"  best model: {best_name}")

    top10 = [r["repo_name"] for r in test.groupBy("repo_name").agg(F.sum("stars").alias("s"))
             .orderBy(F.desc("s")).limit(10).collect()]
    out = (preds[best_name].filter(F.col("repo_name").isin(top10))
           .select("repo_name", "hour_ts", F.col("label").alias("actual_stars"),
                   F.round("prediction", 2).alias("predicted_stars"))
           .withColumn("model", F.lit(best_name)).orderBy("repo_name", "hour_ts"))
    save_result(out, "ml_forecast_predictions", spark)

    # next-hour forecast: shift features so the newest hour becomes "lag1"
    w = Window.partitionBy("repo_name").orderBy("hour_ts")
    w6 = w.rowsBetween(-5, 0)
    nxt = (df.withColumn("lag1", F.col("stars"))
           .withColumn("lag2", F.lag("stars", 1).over(w))
           .withColumn("lag3", F.lag("stars", 2).over(w))
           .withColumn("lag6", F.lag("stars", 5).over(w))
           .withColumn("roll6_mean", F.avg("stars").over(w6))
           .withColumn("forks_lag1", F.col("forks"))
           .withColumn("pushes_lag1", F.col("pushes"))
           .withColumn("actors_lag1", F.col("actors"))
           .withColumn("hour_of_day", (F.hour("hour_ts") + 1) % 24)
           .filter(F.col("hour_ts") == F.lit(last_hour)).dropna(subset=FEATURES))
    fc = (best.transform(assembler.transform(nxt))
          .select("repo_name", F.col("stars").alias("stars_last_hour"),
                  F.round(F.greatest(F.col("prediction"), F.lit(0.0)), 2).alias("predicted_next_hour"))
          .withColumn("forecast_for_hour_utc", F.lit(str(last_hour + timedelta(hours=1))))
          .orderBy(F.desc("predicted_next_hour")).limit(config.TOP_N))
    save_result(fc, "ml_next_hour_forecast", spark)

    imp = spark.createDataFrame(
        [(f, float(v)) for f, v in zip(FEATURES, gbt.featureImportances.toArray())],
        ["feature", "importance"]).orderBy(F.desc("importance"))
    save_result(imp, "ml_feature_importance", spark)
    data.unpersist()


def metrics(pred_df, col):
    ev = RegressionEvaluator(labelCol="label", predictionCol=col)
    return (round(ev.setMetricName("rmse").evaluate(pred_df), 4),
            round(ev.setMetricName("mae").evaluate(pred_df), 4),
            round(ev.setMetricName("r2").evaluate(pred_df), 4))


# ----------------------------------------------------------------- clustering
PROFILE = ["star_share", "fork_share", "push_share", "pr_share", "issue_share",
           "log_events", "actors_per_event"]


def clustering(spark, ev):
    banner("Part B: repo clustering (K-Means)")

    def c(t):
        return F.sum(F.when(F.col("type") == t, 1).otherwise(0))

    prof = (ev.filter(~F.col("is_bot")).groupBy("repo_name").agg(
                F.count("*").alias("events"),
                c("WatchEvent").alias("stars"), c("ForkEvent").alias("forks"),
                c("PushEvent").alias("pushes"), c("PullRequestEvent").alias("prs"),
                c("IssuesEvent").alias("issues"),
                F.approx_count_distinct("actor_login").alias("actors"))
            .filter(F.col("events") >= config.MIN_EVENTS_FOR_CLUSTER))
    for src, dst in [("stars", "star_share"), ("forks", "fork_share"), ("pushes", "push_share"),
                     ("prs", "pr_share"), ("issues", "issue_share")]:
        prof = prof.withColumn(dst, F.col(src) / F.col("events"))
    prof = (prof.withColumn("log_events", F.log1p("events"))
            .withColumn("actors_per_event", F.col("actors") / F.col("events")).cache())

    n = prof.count()
    print(f"  {n:,} repos with >= {config.MIN_EVENTS_FOR_CLUSTER} events")
    if n < config.NUM_CLUSTERS * 5:
        print("  too few repos to cluster. Download more data or lower MIN_EVENTS_FOR_CLUSTER.")
        return

    vec = VectorAssembler(inputCols=PROFILE, outputCol="raw").transform(prof)
    scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True).fit(vec).transform(vec)
    km = KMeans(k=config.NUM_CLUSTERS, seed=42, featuresCol="features").fit(scaled)
    clustered = km.transform(scaled)
    sil = ClusteringEvaluator(featuresCol="features").evaluate(clustered)
    print(f"  silhouette score = {sil:.3f}  (closer to 1 = better separated)")

    summary = (clustered.groupBy("prediction").agg(
                   F.count("*").alias("repos"),
                   *[F.round(F.avg(p), 3).alias(p) for p in PROFILE],
                   F.round(F.avg("events"), 1).alias("avg_events"))
               .orderBy("prediction"))
    names = name_clusters(summary.collect())
    name_map = F.create_map(*[x for k, v in names.items() for x in (F.lit(k), F.lit(v))])

    summary = (summary.withColumn("cluster_name", name_map[F.col("prediction")])
               .withColumn("silhouette", F.lit(round(sil, 4)))
               .withColumnRenamed("prediction", "cluster"))
    summary.show(truncate=False)
    save_result(summary, "cluster_summary", spark)

    repos = (clustered.withColumn("cluster_name", name_map[F.col("prediction")])
             .select("repo_name", F.col("prediction").alias("cluster"), "cluster_name", "events",
                     "stars", "forks", "pushes", "prs", "issues", "actors")
             .orderBy(F.desc("events")))
    save_result(repos, "repo_clusters", spark, max_rows=20000)


def name_clusters(rows):
    """Give each cluster a human-readable name based on its strongest trait."""
    labels = [("star_share", "Viral / hype (mostly stars)"),
              ("push_share", "Active development (mostly pushes)"),
              ("pr_share", "Community collaboration (PRs)"),
              ("issue_share", "Support-heavy (many issues)"),
              ("fork_share", "Fork magnet (templates / tutorials)")]
    names, used = {}, set()
    remaining = list(rows)
    for feat, label in labels:
        if not remaining:
            break
        best = max(remaining, key=lambda r: r[feat])
        names[best["prediction"]] = label
        used.add(best["prediction"])
        remaining = [r for r in remaining if r["prediction"] not in used]
    for r in remaining:
        names[r["prediction"]] = f"Mixed activity #{r['prediction']}"
    return names


def main():
    t0 = time.time()
    spark = get_spark("GitHubTrendRadar-Job3-ML")
    banner("Loading events")
    ev = load_events(spark).cache()
    forecasting(spark, ev)
    clustering(spark, ev)
    banner(f"Job 3 finished in {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
