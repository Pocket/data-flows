"""Delete all raw snowplow events for users who have requested to have their accounts deleted.

 Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""

from prefect import Flow
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

# Run first day of month at midnight UTC.
SCHEDULE = Schedule(clocks=[CronClock("0 0 1 * *")])

DELETE_SQL = """
DELETE FROM "SNOWPLOW"."ATOMIC"."EVENTS" as e
USING "ANALYTICS"."DBT_STAGING"."STG_ACCOUNT_DELETIONS" as d 
WHERE e.CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id = d.hashed_user_id
AND d.happened_at > DATEADD("day", -90, NOW())
"""

with Flow(FLOW_NAME, schedule=SCHEDULE) as flow:
    execute = PocketSnowflakeQuery()
    extract_result = execute(query=DELETE_SQL)

