{% set sql_engine = "mysql" %}
SELECT
  curated_rec_id,
  feed_id,
  resolved_id,
  prospect_id,
  queued_id,
  status,
  CONVERT_TZ(FROM_UNIXTIME(time_live), @@session.time_zone, '+00:00') as time_live,
  CONVERT_TZ(FROM_UNIXTIME(time_added), @@session.time_zone, '+00:00') as time_added,
  CONVERT_TZ(FROM_UNIXTIME(time_updated), @@session.time_zone, '+00:00') as time_updated
FROM curated_feed_items
INTO OUTFILE S3 's3-us-east-1://pocket-astronomer-airflow/curated_feed_exports_aurora/curated_feed_items{{}}'
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
OVERWRITE ON;
