-- ==================================================================
--  GitHub Tech Trend Radar - HiveQL analysis
--  Run:  hive -f hive/analysis_queries.sql     (after create_tables.sql)
--  Take screenshots of these outputs for your report.
-- ==================================================================
ADD JAR ${env:HIVE_HOME}/hcatalog/share/hcatalog/hive-hcatalog-core.jar;
USE github_radar;
SET hive.cli.print.header=true;

-- Q1. Event type distribution
SELECT type, COUNT(*) AS events
FROM events
GROUP BY type
ORDER BY events DESC;

-- Q2. Top 20 most-starred repositories (WatchEvent = someone starred it)
SELECT repo_name, COUNT(*) AS stars
FROM events
WHERE type = 'WatchEvent'
GROUP BY repo_name
ORDER BY stars DESC
LIMIT 20;

-- Q3. Top 20 most-forked repositories
SELECT repo_name, COUNT(*) AS forks
FROM events
WHERE type = 'ForkEvent'
GROUP BY repo_name
ORDER BY forks DESC
LIMIT 20;

-- Q4. Activity per day
SELECT event_date, COUNT(*) AS events, COUNT(DISTINCT actor_login) AS active_users
FROM events
GROUP BY event_date
ORDER BY event_date;

-- Q5. Activity per hour of day (UTC)
SELECT event_hour, COUNT(*) AS events
FROM events
GROUP BY event_hour
ORDER BY event_hour;

-- Q6. Most active organisations
SELECT org_login, COUNT(*) AS events, COUNT(DISTINCT repo_name) AS repos
FROM events
WHERE org_login IS NOT NULL
GROUP BY org_login
ORDER BY events DESC
LIMIT 20;

-- Q7. Brand-new repositories created in the period
SELECT COUNT(*) AS new_repositories
FROM events
WHERE type = 'CreateEvent' AND ref_type = 'repository';

-- Q8. Bot vs human activity
SELECT CASE WHEN actor_login LIKE '%[bot]' THEN 'bot' ELSE 'human' END AS actor_kind,
       COUNT(*) AS events
FROM events
GROUP BY CASE WHEN actor_login LIKE '%[bot]' THEN 'bot' ELSE 'human' END;

-- Q9. Most active human developers (pushes + PRs)
SELECT actor_login, COUNT(*) AS contributions
FROM events
WHERE type IN ('PushEvent', 'PullRequestEvent') AND actor_login NOT LIKE '%[bot]'
GROUP BY actor_login
ORDER BY contributions DESC
LIMIT 20;

-- Q10. Repos with the highest share of pull requests (collaboration)
SELECT repo_name,
       SUM(CASE WHEN type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS prs,
       COUNT(*) AS total_events
FROM events
GROUP BY repo_name
HAVING COUNT(*) >= 100
ORDER BY prs DESC
LIMIT 20;

-- Q11. Results produced by the Spark jobs (only if Spark saved them to Hive,
--      see README "Hive metastore" note)
-- SELECT * FROM github_radar.trending_repos LIMIT 20;
-- SELECT * FROM github_radar.language_summary;
-- SELECT * FROM github_radar.ml_forecast_metrics;
