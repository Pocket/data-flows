"""DELETE all DEVELOPMENT DB schemas older than 89 days

Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""
from prefect import Flow, task, unmapped
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.snowflake import SnowflakeQuery

from utils.flow import get_flow_name
from utils.config import SNOWFLAKE_DEFAULT_DICT

FLOW_NAME = get_flow_name(__file__)
DATABASE = "DEVELOPMENT"

# Run daily at midnight UTC.
SCHEDULE = Schedule(clocks=[CronClock("0 0 * * *")])

GET_SCHEMAS_SQL = """
SELECT catalog_name as database,
       schema_name,
       schema_owner,
       created,
       last_altered
FROM information_schema.schemata
WHERE created < DATEADD("day", -89, CURRENT_TIMESTAMP())
AND schema_name not in ('PUBLIC');
"""


def query():
    return SnowflakeQuery(**SNOWFLAKE_DEFAULT_DICT, database=DATABASE)


@task()
def drop_schema(row: list[str]):
    schema_name = row[1]
    return query().run(query=f'DROP SCHEMA {schema_name}')


@task()
def get_schemas():
    return query().run(query=GET_SCHEMAS_SQL)


with Flow(FLOW_NAME) as flow:
    schemas = get_schemas()
    drop_schema.map(schemas)


if __name__ == "__main__":
    flow.run()
