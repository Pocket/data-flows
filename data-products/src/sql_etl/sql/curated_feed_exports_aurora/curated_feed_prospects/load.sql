{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
truncate table airflow.mysql.curated_feed_prospects;
COPY INTO airflow.mysql.curated_feed_prospects
(PROSPECT_ID, FEED_ID, RESOLVED_ID, TYPE, STATUS, CURATOR, TIME_ADDED, TIME_UPDATED, TOP_DOMAIN_ID, TITLE, excerpt, image_src)
  FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_prospects.
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
