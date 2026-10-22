#!/usr/bin/env bash
# ==================================================================
#  Runs the whole batch analysis after the data is in HDFS:
#    job 1 -> metadata enrichment -> job 2 -> job 3
#  Usage:  source radar_env.sh && bash run_batch.sh
# ==================================================================
set -e
cd "$(dirname "$0")"

if command -v spark-submit >/dev/null 2>&1; then
  RUN="spark-submit --master local[4] --driver-memory 3g"
else
  echo "spark-submit not found, running with python3 (pip pyspark)"
  RUN="python3"
fi

echo ">>> JOB 1: trend analysis";        $RUN spark_jobs/job1_trend_analysis.py
echo ">>> STEP: GitHub API enrichment";  python3 enrich_metadata.py
echo ">>> JOB 2: language & topic trends"; $RUN spark_jobs/job2_language_trends.py
echo ">>> JOB 3: machine learning";      $RUN spark_jobs/job3_ml_models.py

echo
echo "All batch jobs done. Results are in output/. Start the dashboard with:"
echo "   streamlit run dashboard/app.py"
