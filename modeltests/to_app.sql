MODEL (
  name multiengine.metrics,
  gateway 'duckdb_pg'
);

SELECT
    ...
FROM
  multiengine.events
WHERE
  event_timestamp BETWEEN @start_date AND @end_date
GROUP BY
  ...