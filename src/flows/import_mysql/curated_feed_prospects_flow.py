from prefect import Flow

from utils.config import get_mysql_executor
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

EXTRACT_SQL = """
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
    INTO OUTFILE S3 's3-us-east-1://pocket-astronomer-airflow/curated_feed_exports_aurora/curated_feed_prospects'
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n'
    OVERWRITE ON;
"""

LOAD_SQL = """
    TRUNCATE TABLE airflow.mysql.curated_feed_prospects;
    COPY INTO airflow.mysql.curated_feed_prospects
    (PROSPECT_ID, FEED_ID, RESOLVED_ID, TYPE, STATUS, CURATOR, TIME_ADDED, TIME_UPDATED, TOP_DOMAIN_ID, TITLE, excerpt, image_src)
      FROM @airflow.mysql.airflow_stage/curated_feed_exports_aurora/curated_feed_prospects
      FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0);
"""


with Flow(FLOW_NAME) as flow:
    execute = get_mysql_executor()
    extract_result = execute(query=EXTRACT_SQL)
    execute(query=LOAD_SQL, upstream_tasks=[extract_result])

