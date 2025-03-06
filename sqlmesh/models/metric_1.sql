MODEL (
  name multiengine.metric_1,
  gateway athena,
  start '2025-03-05 19:20:00',
  cron '@hourly',
  physical_properties (
    table_format=iceberg,
    s3_base_location='s3://sumeo-parquet-data-lake/multiengine/metric_1/'
  )
);

SELECT
   action, count(*) as count
FROM
  multiengine.events
GROUP BY
    action