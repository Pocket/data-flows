{% set sql_engine = "mysql" %}
SELECT
  prospect_id,
  feed_id,
  resolved_id,
  type,
  status,
  curator,
  CONVERT_TZ(FROM_UNIXTIME(time_added), @@session.time_zone, '+00:00') as time_added,
  CONVERT_TZ(FROM_UNIXTIME(time_updated), @@session.time_zone, '+00:00') as time_updated,
  top_domain_id,
  title,
  excerpt,
  image_src
FROM curated_feed_prospects
INTO OUTFILE S3 's3-us-east-1://pocket-astronomer-airflow/curated_feed_exports_aurora/curated_feed_prospects{{environment}}'
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
OVERWRITE ON;
