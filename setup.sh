#!/usr/bin/env bash
# ==================================================================
#  One-time setup for GitHub Tech Trend Radar
#  Run from the project folder:   bash setup.sh
# ==================================================================
set -e
cd "$(dirname "$0")"
PROJECT="$(pwd)"

echo "== 1. Creating local folders =="
mkdir -p "$HOME/github_spool/archive" "$HOME/github_spool/live" "$HOME/github_staging" output
echo "   $HOME/github_spool/archive , $HOME/github_spool/live"

echo "== 2. Installing Python packages =="
python3 -m pip install -r requirements.txt \
  || python3 -m pip install --user -r requirements.txt \
  || python3 -m pip install --break-system-packages -r requirements.txt \
  || echo "   !! pip install failed - install manually: pip install -r requirements.txt (see README troubleshooting)"

echo "== 3. Detecting HDFS address =="
HDFS_URI=""
if command -v hdfs >/dev/null 2>&1; then
  HDFS_URI="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
fi
if [ -z "$HDFS_URI" ] || [[ "$HDFS_URI" == file:* ]]; then
  HDFS_URI="hdfs://localhost:9000"
  echo "   could not read fs.defaultFS, using default $HDFS_URI (edit radar_env.sh if wrong)"
else
  echo "   fs.defaultFS = $HDFS_URI"
fi

echo "== 4. Writing flume/github-agent.conf =="
sed -e "s#__HOME__#$HOME#g" -e "s#__HDFS_URI__#$HDFS_URI#g" \
    flume/github-agent.conf.template > flume/github-agent.conf

echo "== 5. Writing radar_env.sh =="
if [ ! -f radar_env.sh ]; then
cat > radar_env.sh <<EOF
# source this file in EVERY new terminal:   source radar_env.sh
export GITHUB_TOKEN="PASTE_YOUR_TOKEN_HERE"
export RADAR_HDFS_URI="$HDFS_URI"
export RADAR_USE_HDFS=1
export RADAR_SAVE_TO_HIVE=1
export PYSPARK_PYTHON=python3
cd "$PROJECT"
EOF
fi
echo "   now open radar_env.sh and paste your GitHub token"

echo "== 6. Creating HDFS folders =="
if command -v hdfs >/dev/null 2>&1; then
  hdfs dfs -mkdir -p /github/raw/archive /github/raw/live && echo "   /github/raw/archive , /github/raw/live created"
else
  echo "   'hdfs' command not found - start Hadoop and run:"
  echo "   hdfs dfs -mkdir -p /github/raw/archive /github/raw/live"
fi

chmod +x run_batch.sh 2>/dev/null || true
echo
echo "Setup complete. Next: edit radar_env.sh, then follow README step 4."
