from prefect import Flow

from utils.config import get_mysql_executor
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

EXTRACT_SQL = """
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
    INTO OUTFILE S3 's3-us-east-1://pocket-astronomer-airflow/curated_feed_exports_aurora/curated_feed_items'
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n'
    OVERWRITE ON;
"""

LOAD_SQL = """
    TRUNCATE TABLE airflow.mysql.curated_feed_items;
    COPY INTO airflow.mysql.curated_feed_items (
        CURATED_REC_ID, 
        FEED_ID, 
        RESOLVED_ID, 
        PROSPECT_ID, 
        QUEUED_ID, 
        STATUS, 
        TIME_LIVE, 
        TIME_ADDED, 
        TIME_UPDATED
    )
      FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_items
      FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
"""


with Flow(FLOW_NAME) as flow:
    execute = get_mysql_executor()
    extract_result = execute(query=EXTRACT_SQL)
    execute(query=LOAD_SQL, upstream_tasks=[extract_result])

