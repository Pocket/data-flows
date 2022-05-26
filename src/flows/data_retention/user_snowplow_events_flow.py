"""Delete all raw snowplow events for users who have requested to have their accounts deleted.

 Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""

from prefect import Flow, task
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.dbt import DbtCloudRunJob
from prefect.tasks.snowflake import SnowflakeQuery

from utils.config import SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

# Run first day of month at midnight UTC.
SCHEDULE = Schedule(clocks=[CronClock("0 0 1 * *")])

DBT_MODEL_CLEANUP_JOB_ID = 17774

DELETE_SQL = """
DELETE FROM "SNOWPLOW"."ATOMIC"."EVENTS" as e
USING "ANALYTICS"."DBT_STAGING"."STG_ACCOUNT_DELETIONS" as d 
WHERE e.CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id = d.hashed_user_id
"""


@task()
def delete_user_snowplow_events():
    return SnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(query=DELETE_SQL)


@task()
def delete_user_dbt_rows():
    DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_MODEL_CLEANUP_JOB_ID, wait_for_job_run_completion=True)


with Flow(FLOW_NAME, schedule=SCHEDULE) as flow:
    delete_result = delete_user_snowplow_events()
    delete_user_dbt_rows(upstream_tasks=[delete_result])
