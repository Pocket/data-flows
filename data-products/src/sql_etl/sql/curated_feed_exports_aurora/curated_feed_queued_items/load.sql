{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
truncate table curated_feed_queued_items;
COPY INTO curated_feed_queued_items
(QUEUED_ID, FEED_ID, RESOLVED_ID, PROSPECT_ID, STATUS, CURATOR, RELEVANCE_LENGTH, TOPIC_ID, WEIGHT, TIME_ADDED, TIME_UPDATED)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_queued_items{{environment}}
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
