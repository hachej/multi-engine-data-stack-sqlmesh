MODEL (
  name multiengine.metrics,
  start '2025-03-05 19:20:00',
  cron '@hourly'
);

SELECT
   action, count(*) as count
FROM
  events
GROUP BY
    action