{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
truncate table curated_feed_items_deleted;
COPY INTO curated_feed_items_deleted
(ID, CURATED_REC_ID, FEED_ID, RESOLVED_ID, PROSPECT_ID, QUEUED_ID, STATUS, TIME_LIVE, TIME_ADDED, TIME_UPDATED, DELETED_USER_ID, DELETED_AT)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_items_deleted{{environment}}
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
