# GitHub Tech Trend Radar
### Real-time big-data analysis of developer activity using Flume, HDFS, Hive and PySpark

The system captures public GitHub activity (stars, forks, pushes, pull requests, issues, new repos), ships it into HDFS with **Apache Flume**, queries it with **Hive**, analyses it with **PySpark** (batch + Structured Streaming + MLlib), and shows the results in a **Streamlit** dashboard.

```
GH Archive (hourly dumps) ──> fetch_gharchive.py ──┐
                                                   ├─> ~/github_spool/{archive,live} ──> FLUME ──> HDFS /github/raw/{archive,live}
GitHub Events API (live) ──> fetch_live_events.py ─┘                                               │
                                                                                                   ├─> HIVE (external tables + HiveQL)
GitHub REST API ──> enrich_metadata.py (language, topics)                                          ├─> PySpark Job 1  trends
                                                                                                   ├─> PySpark Job 2  languages/topics
                                                                                                   ├─> PySpark Job 3  MLlib (GBT forecast + K-Means)
                                                                                                   └─> PySpark Job 4  Structured Streaming
                                                                       output/*.csv + Hive tables ──> Streamlit dashboard
```

---

## 0. What you need installed

| Software | Version that works | Check with |
|---|---|---|
| Linux / Ubuntu / WSL2 | any recent | |
| Java | 8 or 11 (what your Hadoop uses) | `java -version` |
| Hadoop (HDFS) | 3.x | `hdfs version` |
| Apache Flume | 1.9 – 1.11 | `flume-ng version` |
| Apache Hive | 2.3 / 3.x / 4.x | `hive --version` |
| Apache Spark | 3.3 – 3.5 (4.x also works) | `spark-submit --version` |
| Python | 3.9 – 3.11 recommended | `python3 --version` |

If Spark is not installed, `pip install pyspark` is enough (the scripts fall back to `python3`).

---

## 1. Put the project in your home folder

```bash
cd ~
unzip github_trend_radar.zip
cd ~/github_trend_radar
```

## 2. Get a free GitHub token (2 minutes)

1. Open https://github.com/settings/tokens → **Generate new token (classic)**
2. Name: `trend-radar`, expiry: 30 days, **do not tick any scopes**
3. Copy the token (`ghp_...`)

Without a token GitHub allows only 60 API calls/hour; with it, 5,000.

## 3. One-time setup

```bash
bash setup.sh
nano radar_env.sh          # paste your token in place of PASTE_YOUR_TOKEN_HERE, save
source radar_env.sh        # do this in EVERY new terminal
```

`setup.sh` creates the spool folders, installs Python packages, detects your HDFS address, writes `flume/github-agent.conf`, and creates `/github/raw/archive` and `/github/raw/live` in HDFS.

## 4. Start Hadoop

```bash
start-dfs.sh
start-yarn.sh              # optional
jps                        # must show NameNode and DataNode
hdfs dfs -mkdir -p /github/raw/archive /github/raw/live    # (if setup.sh couldn't)
```

## 5. Terminal 1 — start Flume (leave it running)

```bash
source radar_env.sh
flume-ng agent -n agent -c $FLUME_HOME/conf -f flume/github-agent.conf -Dflume.root.logger=INFO,console
```

You should see `Spooling Directory Source runner has started` twice (archive + live).

## 6. Terminal 2 — download historical data

```bash
source radar_env.sh
python3 fetch_gharchive.py --days 1 --hours 0-5     # quick 6-hour test first
python3 fetch_gharchive.py --days 2                 # real dataset: 48 hours
```

Each hour is a download of roughly 50–200 MB (it varies) and contains ~150,000–300,000 events; the script keeps only the 8 fields we use. Two days = tens of millions of events = genuine big data. Use `--days 3` if your machine and internet allow it.

Watch Terminal 1: Flume picks up each file and renames it `*.COMPLETED`. Check HDFS:

```bash
hdfs dfs -ls /github/raw/archive
hdfs dfs -du -h /github/raw
hdfs dfs -cat /github/raw/archive/events.*.json | head -3
```

## 7. Hive

```bash
hive -f hive/create_tables.sql        # external tables over the HDFS data
hive -f hive/analysis_queries.sql     # 10 HiveQL analyses (screenshot these)
```

## 8. Spark batch analysis (Jobs 1–3)

```bash
source radar_env.sh
bash run_batch.sh
```

This runs, in order:

| Step | Script | What it produces |
|---|---|---|
| Job 1 | `spark_jobs/job1_trend_analysis.py` | dataset summary, event types, hourly time series, IST heatmap, top starred, **trending (accelerating) repos**, **emerging new repos**, top orgs, top developers, bot share, repo health (real dev vs hype) |
| Enrich | `enrich_metadata.py` | language + topics for the ~500 most interesting repos via the GitHub REST API |
| Job 2 | `spark_jobs/job2_language_trends.py` | language share & momentum, language timeline, hot topics |
| Job 3 | `spark_jobs/job3_ml_models.py` | **MLlib**: next-hour star forecast (Linear Regression vs Gradient-Boosted Trees vs naive baseline, RMSE/MAE/R²), feature importance, **K-Means** repo clustering + silhouette score |

Results are written to `output/*.csv` and to Hive tables in database `github_radar`.

You can also run any job on its own:
```bash
spark-submit --master local[*] --driver-memory 4g spark_jobs/job1_trend_analysis.py
```

## 9. Real-time streaming demo (Job 4)

Keep Flume (Terminal 1) running, then:

**Terminal 3** — live capture from the GitHub Events API:
```bash
source radar_env.sh
python3 fetch_live_events.py
```

**Terminal 4** — Spark Structured Streaming:
```bash
source radar_env.sh
spark-submit --master local[*] spark_jobs/job4_streaming_trends.py
```

Every 30 s it prints the top-starred repos of the current 15-minute sliding window and updates `output/live_*.csv`. Add `--reset` to start fresh. Ctrl+C to stop.

## 10. Dashboard

**Terminal 5**:
```bash
source radar_env.sh
streamlit run dashboard/app.py
```

Open http://localhost:8501. Tabs: Trending · Languages & topics · Activity · ML forecast · Repo clusters · Live stream (switch on auto-refresh during the streaming demo).

---

## Demo-day order (what to show the examiner)

1. `jps` → Hadoop running
2. Terminal 1 Flume log → files being shipped
3. `hdfs dfs -ls /github/raw/archive` and `hdfs dfs -du -h /github/raw`
4. `hive -f hive/analysis_queries.sql` → HiveQL results
5. Dashboard: Trending → Languages → Activity heatmap → ML metrics → Clusters
6. Start `fetch_live_events.py` + Job 4 → Live tab updates in real time

---

## Backup plans

**Hadoop/Flume broken on the day?** Spark can read the spool folders directly:
```bash
export RADAR_USE_HDFS=0 RADAR_SAVE_TO_HIVE=0
# do NOT run Flume in this mode
python3 fetch_gharchive.py --days 2
bash run_batch.sh
```

**No internet?** Generate synthetic test data in the same format (testing only, use real data for your report):
```bash
python3 tools/make_sample_data.py --hours 48
python3 tools/make_sample_data.py --live --hours 1
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Flume: `NoSuchMethodError ... com.google.common.base.Preconditions` | Guava clash with Hadoop 3: `rm $FLUME_HOME/lib/guava-*.jar && cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $FLUME_HOME/lib/` |
| Flume: `ClassNotFoundException ... hadoop` / HDFS sink fails | Make sure `HADOOP_HOME` is set and `hadoop` is on PATH before starting Flume |
| Flume: `Connection refused localhost:9000` | Your HDFS port differs. `hdfs getconf -confKey fs.defaultFS` → put that value in `radar_env.sh` (RADAR_HDFS_URI) and in `flume/github-agent.conf`, or re-run `bash setup.sh` |
| Flume stops: `File name has been re-used` | Delete that file from `~/github_spool/...` and restart Flume |
| Hive: `Cannot find class JsonSerDe` | Check the jar path in the `ADD JAR` line (`find $HIVE_HOME -name "hive-hcatalog-core*.jar"`). On Hive 4 remove the line |
| Hive shows 0 rows | `hdfs dfs -ls /github/raw/archive` – files starting with `_` are still being written; wait ~5 min |
| Spark: `No data found` | Data not in HDFS yet, or wrong HDFS URI in `radar_env.sh` |
| Spark: `Python worker failed to connect` / Python version error | `export PYSPARK_PYTHON=python3` and use Python 3.9–3.11 |
| Spark: out of memory | Use fewer days, or `--driver-memory 6g` |
| Spark Hive tables not visible in `hive` CLI | Copy `$HIVE_HOME/conf/hive-site.xml` to `$SPARK_HOME/conf/`. (The CSVs in `output/` are always written either way) |
| `pip install` → `externally-managed-environment` | `pip install --break-system-packages -r requirements.txt` or use a venv |
| `enrich_metadata.py` slow / rate limited | Token missing — check `echo $GITHUB_TOKEN` |
| Job 3: "not enough hours of data" | Download at least 1 full day (24 consecutive hours) |

---

## Project structure

```
github_trend_radar/
├── README.md                 this guide
├── setup.sh                  one-time setup
├── run_batch.sh              runs Job 1 → enrich → Job 2 → Job 3
├── requirements.txt
├── config.py                 all settings (paths, thresholds, windows)
├── utils.py                  shared helpers for the fetchers
├── fetch_gharchive.py        historical capture  → Flume spool
├── fetch_live_events.py      live capture        → Flume spool
├── enrich_metadata.py        GitHub REST API: language + topics
├── flume/
│   └── github-agent.conf.template   2 spooldir sources → 2 HDFS sinks
├── hive/
│   ├── create_tables.sql     external tables + view
│   └── analysis_queries.sql  10 HiveQL analyses
├── spark_jobs/
│   ├── common.py             SparkSession, loading, saving
│   ├── job1_trend_analysis.py
│   ├── job2_language_trends.py
│   ├── job3_ml_models.py
│   └── job4_streaming_trends.py
├── dashboard/app.py          Streamlit dashboard
├── tools/make_sample_data.py offline synthetic data
└── output/                   result CSVs (created when jobs run)
```

## Key techniques (for your report)

- **Data capture:** Flume spooling-directory sources, memory channels, HDFS sinks with time-based roll; atomic file hand-off from the fetchers.
- **Storage:** HDFS; Hive external tables with JSON SerDe and a cleaned view.
- **Trend detection:** star *acceleration* = stars in recent window ÷ stars in previous window, combined with volume into a trend score `log(1+recent) × log2(1+growth)`.
- **Repo health:** ratio of development events (push + PR + issue) to stars separates actively developed projects from hype.
- **Forecasting:** hourly repo × time grid, lag features (t-1, t-2, t-3, t-6, 6-h rolling mean, forks, pushes, actors, hour of day), time-based 80/20 split, Linear Regression vs GBT vs naive baseline.
- **Clustering:** activity-mix features, StandardScaler, K-Means (k = 4), silhouette score, automatic cluster naming.
- **Streaming:** Spark Structured Streaming file source, 15-min sliding windows every 5 min, `foreachBatch` sinks, checkpointing.
