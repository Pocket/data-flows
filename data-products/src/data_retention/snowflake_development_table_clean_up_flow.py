"""DELETE all DEVELOPMENT DB tables older than 89 days

Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""

from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, unmapped
from prefect_snowflake.database import snowflake_query

GET_TABLES_SQL = """
SELECT table_catalog || '.' || table_schema || '.' || table_name as table_name
FROM development.information_schema.tables
WHERE created < DATEADD("day", -89, CURRENT_TIMESTAMP())
AND table_schema not in ('PUBLIC')
AND table_type = 'BASE TABLE'
AND table_catalog = 'DEVELOPMENT';
"""


@flow()
async def delete_old_dev_tables():
    sfc = MozSnowflakeConnector()

    tables = await snowflake_query(query=GET_TABLES_SQL, snowflake_connector=sfc)
    statements = [f"DROP TABLE {t[0]}" for t in tables]

    await snowflake_query.map(query=statements, snowflake_connector=unmapped(sfc))  # type: ignore  # noqa: E501


FLOW_SPEC = FlowSpec(
    flow=delete_old_dev_tables,
    docker_env="base",
    deployments=[
        FlowDeployment(name="deployment", cron="0 0 * * *"),
    ],
)

if __name__ == "__main__":
    import asyncio

    asyncio.run(delete_old_dev_tables())  # type: ignore
