"""Delete all raw snowplow events for users who have requested to have their accounts deleted.

 Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""
import os

from prefect import Flow, task
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.dbt import DbtCloudRunJob
from prefect.tasks.snowflake import SnowflakeQuery, SnowflakeQueriesFromFile

from utils.config import SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT, SRC_DIR
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

# Run first day of month at midnight UTC.
SCHEDULE = Schedule(clocks=[CronClock("0 0 1 * *")])

DBT_MODEL_CLEANUP_JOB_ID = 17774

DELETE_RAW_USER_ROWS_FILE_PATH = os.path.join(SRC_DIR, 'data_retention', 'delete_raw_user_rows.sql')

DELETE_SNOWPLOW_EVENTS_SQL = """
DELETE FROM "SNOWPLOW"."ATOMIC"."EVENTS" as e
USING "ANALYTICS"."DBT_STAGING"."STG_ACCOUNT_DELETIONS" as d 
WHERE (e.CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id = d.hashed_user_id
  OR e.CONTEXTS_COM_POCKET_USER_1[0]:user_id = d.user_id)
"""


@task()
def delete_user_snowplow_events():
    return SnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(query=DELETE_SNOWPLOW_EVENTS_SQL)


@task()
def delete_user_raw_rows():
    return SnowflakeQueriesFromFile(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(file_path=DELETE_RAW_USER_ROWS_FILE_PATH)


@task()
def delete_user_dbt_rows():
    DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_MODEL_CLEANUP_JOB_ID, wait_for_job_run_completion=True)


with Flow(FLOW_NAME, schedule=SCHEDULE) as flow:
    delete_raw_rows_result = delete_user_raw_rows()
    delete_snowplow_events_result = delete_user_snowplow_events()
    delete_user_dbt_rows(upstream_tasks=[delete_raw_rows_result, delete_snowplow_events_result])
