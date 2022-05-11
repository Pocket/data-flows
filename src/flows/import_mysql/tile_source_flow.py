from prefect import Flow

from utils.config import get_mysql_executor
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

EXTRACT_SQL = """
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
"""

LOAD_SQL = """
    TRUNCATE TABLE airflow.mysql.tile_source;
    COPY INTO airflow.mysql.tile_source
        (TILE_ID, SOURCE_ID, TYPE, CREATED_AT, UPDATED_AT)
          FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/tile_source
          FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
"""

with Flow(FLOW_NAME) as flow:
    execute = get_mysql_executor()
    extract_result = execute(query=EXTRACT_SQL)
    execute(query=LOAD_SQL, upstream_tasks=[extract_result])

