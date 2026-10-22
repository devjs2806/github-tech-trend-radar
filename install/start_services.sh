#!/usr/bin/env bash
source "$HOME/bigdata/env.sh"
sudo service ssh start >/dev/null 2>&1 || sudo systemctl start ssh || true
start-dfs.sh
sleep 8
hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
jps
echo "HDFS web UI: http://localhost:9870"
