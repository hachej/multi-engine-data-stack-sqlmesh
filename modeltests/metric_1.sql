MODEL (
  name multiengine.metrics,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_timestamp
  ),
  start '2025-02-27',
  cron '@hourly',
  gateway 'athena'
);

SELECT
    ...
FROM
  multiengine.events
WHERE
  event_timestamp BETWEEN @start_date AND @end_date
GROUP BY
  ...