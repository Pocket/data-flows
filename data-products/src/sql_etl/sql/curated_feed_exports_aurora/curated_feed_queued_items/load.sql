{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
create TRANSIENT TABLE if not exists CURATED_FEED_QUEUED_ITEMS (
	SNOWFLAKE_LOADED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
	QUEUED_ID VARCHAR(16777216),
	FEED_ID VARCHAR(16777216),
	RESOLVED_ID VARCHAR(16777216),
	PROSPECT_ID VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	CURATOR VARCHAR(16777216),
	RELEVANCE_LENGTH VARCHAR(16777216),
	TOPIC_ID VARCHAR(16777216),
	WEIGHT VARCHAR(16777216),
	TIME_ADDED VARCHAR(16777216),
	TIME_UPDATED VARCHAR(16777216)
);
truncate table curated_feed_queued_items;
COPY INTO curated_feed_queued_items
(QUEUED_ID, FEED_ID, RESOLVED_ID, PROSPECT_ID, STATUS, CURATOR, RELEVANCE_LENGTH, TOPIC_ID, WEIGHT, TIME_ADDED, TIME_UPDATED)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_queued_items{{environment}}
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
