"""
Shared helpers for all PySpark jobs:
  - creating the SparkSession (with Hive support when available)
  - loading the GitHub events that Flume wrote into HDFS
  - saving result tables (CSV for the dashboard + Hive table for SQL)
"""
import os
import sys
import time

# Work in UTC everywhere (Python driver + Spark) so timestamps never shift by +5:30
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

# make "import config" work when a job is started from any folder
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import config  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

EVENT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("created_at", StringType()),
    StructField("actor_login", StringType()),
    StructField("repo_name", StringType()),
    StructField("org_login", StringType()),
    StructField("action", StringType()),
    StructField("ref_type", StringType()),
])

_hive_ok = False


def get_spark(app_name):
    """Create a SparkSession. Falls back to plain Spark if Hive is not set up."""
    global _hive_ok
    builder = (SparkSession.builder
               .appName(app_name)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "16")
               .config("spark.driver.memory", "3g")
               .config("spark.ui.showConsoleProgress", "false"))
    spark = None
    if config.SAVE_TO_HIVE:
        try:
            spark = builder.enableHiveSupport().getOrCreate()
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.HIVE_DB}")
            _hive_ok = True
        except Exception as e:  # Hive not configured -> CSV only
            print(f"[warn] Hive not available ({str(e)[:120]}). Results will be saved as CSV only.")
            spark = None
    if spark is None:
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_events(spark, include_live=False):
    """Read raw events, parse timestamps, drop bad rows and duplicates."""
    paths = [config.ARCHIVE_PATH]
    if include_live:
        paths.append(config.LIVE_PATH)

    frames = []
    for p in paths:
        try:
            frames.append(spark.read.schema(EVENT_SCHEMA).json(p))
        except Exception as e:
            print(f"[warn] could not read {p}: {str(e)[:150]}")
    if not frames:
        raise SystemExit(f"No data found at {paths}. Did Flume run? (see README step 6)")

    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)

    df = (df
          .withColumn("ts", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
          .filter(F.col("ts").isNotNull() & F.col("repo_name").isNotNull() & F.col("type").isNotNull())
          .dropDuplicates(["id"])
          .withColumn("day", F.to_date("ts"))
          .withColumn("hour_ts", F.date_trunc("hour", "ts"))
          .withColumn("is_bot", F.lower(F.col("actor_login")).rlike(r"(\[bot\]$|-bot$|^bot-|^dependabot|^renovate)")))
    return df


def save_result(df, name, spark=None, max_rows=100000):
    """Save a (small) result DataFrame to output/<name>.csv and to Hive."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    pdf = df.limit(max_rows).toPandas()
    path = os.path.join(config.OUTPUT_DIR, f"{name}.csv")
    pdf.to_csv(path, index=False)
    msg = f"  saved {name}: {len(pdf):,} rows -> output/{name}.csv"
    if _hive_ok:
        try:
            df.limit(max_rows).write.mode("overwrite").saveAsTable(f"{config.HIVE_DB}.{name}")
            msg += f" + Hive table {config.HIVE_DB}.{name}"
        except Exception as e:
            msg += f" (Hive save failed: {str(e)[:80]})"
    print(msg)
    return pdf


def banner(text):
    print("\n" + "=" * 70 + f"\n  {text}\n" + "=" * 70)
