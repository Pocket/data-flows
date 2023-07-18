{% set sql_engine = "mysql" %}
SELECT
  tile_id,
  source_id,
  type,
  CONVERT_TZ(created_at, @@session.time_zone, '+00:00') as created_at,
  CONVERT_TZ(updated_at, @@session.time_zone, '+00:00') as updated_at
FROM tile_source
INTO OUTFILE S3 's3-us-east-1://pocket-astronomer-airflow/curated_feed_exports_aurora/tile_source'
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
OVERWRITE ON;
