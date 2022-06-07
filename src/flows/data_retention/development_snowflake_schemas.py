"""DELETE all DEVELOPMENT DB schemas older than 89 days

Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""
from prefect import Flow, task, unmapped
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from utils.flow import get_flow_name

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
AND schema_name not in ('DBT_SBISWAS', 'PUBLIC');
"""
"""
We are excluding schema `DBT_SBISWAS` as its being actively used for delivering Android push notifications experiments.
Kirill will be doing an experiment to explore doing this work our DBT workflow and as a result putting this data in our
ANALYTICS database. Once we have results on this experiment we will decide next steps for this schema exclusion.

https://pocket.slack.com/archives/C03E28D1GUD/p1653672644243659?thread_ts=1653582347.792469&cid=C03E28D1GUD
"""


DROP_SCHEMA_SQL = "DROP SCHEMA DEVELOPMENT.{schema_name}"
"""
We added the fully qualified schema name to be 100% confident that we are deleting the appropriate schemas. :)
"""


@task()
def delete_schema(row: dict[str, str], ex: PocketSnowflakeQuery):
    return ex(query=DROP_SCHEMA_SQL, data={"schema_name": row['schema_name']})


with Flow(FLOW_NAME) as flow:
    executor = PocketSnowflakeQuery(database=DATABASE)
    results = executor(query=GET_SCHEMAS_SQL)
    delete_schema.map(results, unmapped(executor))

