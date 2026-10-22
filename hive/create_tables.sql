-- ==================================================================
--  GitHub Tech Trend Radar - Hive tables over the data Flume wrote to HDFS
--  Run:  hive -f hive/create_tables.sql
-- ==================================================================

-- JSON SerDe. On Hive 2.x / 3.x this jar is needed; on Hive 4 it is built in
-- (if this line errors on your version, comment it out).
ADD JAR ${env:HIVE_HOME}/hcatalog/share/hcatalog/hive-hcatalog-core.jar;

CREATE DATABASE IF NOT EXISTS github_radar;
USE github_radar;

-- Raw historical events (GH Archive), written by Flume archive flow
DROP TABLE IF EXISTS events_archive;
CREATE EXTERNAL TABLE events_archive (
  id           STRING,
  type         STRING,
  created_at   STRING,
  actor_login  STRING,
  repo_name    STRING,
  org_login    STRING,
  action       STRING,
  ref_type     STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/github/raw/archive';

-- Raw live events (GitHub Events API), written by Flume live flow
DROP TABLE IF EXISTS events_live;
CREATE EXTERNAL TABLE events_live (
  id           STRING,
  type         STRING,
  created_at   STRING,
  actor_login  STRING,
  repo_name    STRING,
  org_login    STRING,
  action       STRING,
  ref_type     STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/github/raw/live';

-- Clean view used by the analysis queries: parsed timestamp + date + hour
DROP VIEW IF EXISTS events;
CREATE VIEW events AS
SELECT id, type, actor_login, repo_name, org_login, action, ref_type,
       from_unixtime(unix_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss'Z'")) AS event_time,
       to_date(from_unixtime(unix_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss'Z'"))) AS event_date,
       hour(from_unixtime(unix_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss'Z'"))) AS event_hour
FROM events_archive
WHERE repo_name IS NOT NULL;

-- quick check
SELECT COUNT(*) AS total_archive_events FROM events_archive;
SELECT COUNT(*) AS total_live_events FROM events_live;
