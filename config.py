"""
Central configuration for the GitHub Tech Trend Radar project.
Edit the values in this file to match your machine.
"""
import os

# ---------------------------------------------------------------
# 1. GitHub token
#    Create one at https://github.com/settings/tokens
#    ("Generate new token (classic)", NO scopes need to be ticked).
#    Then, in every terminal you use:
#        export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
# ---------------------------------------------------------------
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
if GITHUB_TOKEN.startswith("PASTE"):   # placeholder from radar_env.sh
    GITHUB_TOKEN = ""

# ---------------------------------------------------------------
# 2. Local folders (Flume watches the two spool folders)
# ---------------------------------------------------------------
HOME = os.path.expanduser("~")
SPOOL_ARCHIVE_DIR = os.path.join(HOME, "github_spool", "archive")
SPOOL_LIVE_DIR = os.path.join(HOME, "github_spool", "live")
STAGING_DIR = os.path.join(HOME, "github_staging")  # temp files, same disk as spool

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")          # CSV results for the dashboard
CHECKPOINT_DIR = os.path.join(PROJECT_DIR, "checkpoints")  # Spark streaming checkpoints

# ---------------------------------------------------------------
# 3. Where Spark reads the data from
#    USE_HDFS = True  -> read what Flume wrote into HDFS (normal mode)
#    USE_HDFS = False -> read the local spool folders directly.
#                        Use this ONLY as a backup if Hadoop/Flume are not
#                        working, and do NOT run Flume at the same time.
#    You can also switch without editing:  export RADAR_USE_HDFS=0
# ---------------------------------------------------------------
USE_HDFS = os.environ.get("RADAR_USE_HDFS", "1") == "1"

# Must match fs.defaultFS in $HADOOP_HOME/etc/hadoop/core-site.xml
# (common values: hdfs://localhost:9000 or hdfs://localhost:8020)
HDFS_URI = os.environ.get("RADAR_HDFS_URI", "hdfs://localhost:9000")

if USE_HDFS:
    ARCHIVE_PATH = HDFS_URI + "/github/raw/archive"
    LIVE_PATH = HDFS_URI + "/github/raw/live"
else:
    ARCHIVE_PATH = "file://" + SPOOL_ARCHIVE_DIR
    LIVE_PATH = "file://" + SPOOL_LIVE_DIR

# Also save Spark results as Hive tables (database below).
# Results are ALWAYS saved as CSV in output/ as well.
SAVE_TO_HIVE = os.environ.get("RADAR_SAVE_TO_HIVE", "0") == "1"
HIVE_DB = "github_radar"

# ---------------------------------------------------------------
# 4. Analysis settings
# ---------------------------------------------------------------
TOP_N = 50                     # rows kept in "top" result tables
MIN_RECENT_STARS = 20          # stars needed in the recent window to be "trending"
MIN_EMERGING_STARS = 5         # stars needed for a brand-new repo to be "emerging"
MAX_TREND_WINDOW_HOURS = 72    # cap on recent/previous window size
MAX_REPOS_FOR_METADATA = 500   # GitHub API calls to fetch language + topics
HEATMAP_TIMEZONE = "Asia/Kolkata"
FORECAST_TOP_REPOS = 200       # repos used to train the star-forecast model
MIN_EVENTS_FOR_CLUSTER = 30    # repos need this many events to be clustered
NUM_CLUSTERS = 4

# ---------------------------------------------------------------
# 5. Streaming settings
# ---------------------------------------------------------------
LIVE_POLL_SECONDS = 60         # how often the live fetcher calls the GitHub API
STREAM_WINDOW = "15 minutes"
STREAM_SLIDE = "5 minutes"
STREAM_TRIGGER = "30 seconds"
