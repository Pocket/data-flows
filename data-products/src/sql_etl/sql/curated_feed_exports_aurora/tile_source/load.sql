{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
truncate table tile_source;
COPY INTO tile_source
(TILE_ID, SOURCE_ID, TYPE, CREATED_AT, UPDATED_AT)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/tile_source{{environment}}
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
