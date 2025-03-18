MODEL (
  name multiengine.metric_1,
  kind FULL,
  gateway duckdb_pg,
  start '2025-03-05 19:20:00',
  cron '@hourly'
);

SELECT
   action,
   count(*) as action_count
FROM
  multiengine.event 
group by action
