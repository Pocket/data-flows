{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
create TRANSIENT TABLE if not exists TILE_SOURCE (
	TILE_ID VARCHAR(16777216),
	SOURCE_ID VARCHAR(16777216),
	SNOWFLAKE_LOADED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
	TYPE VARCHAR(16777216),
	CREATED_AT VARCHAR(16777216),
	UPDATED_AT VARCHAR(16777216)
);
truncate table tile_source;
COPY INTO tile_source
(TILE_ID, SOURCE_ID, TYPE, CREATED_AT, UPDATED_AT)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/tile_source{{environment}}
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
