"""DELETE all DEVELOPMENT DB schemas older than 89 days

Reference: https://getpocket.atlassian.net/wiki/spaces/CP/pages/2703949851/Snowflake+Data+Deletion+and+Retention
"""
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, unmapped
from prefect_snowflake.database import snowflake_query

GET_SCHEMAS_SQL = """
SELECT catalog_name || '.' || schema_name as schema_name
FROM information_schema.schemata
WHERE created < DATEADD("day", -89, CURRENT_TIMESTAMP())
AND schema_name not in ('PUBLIC');
"""


@flow()
async def delete_old_dev_schemas():
    sfc = MozSnowflakeConnector()

    schemas = await snowflake_query(query=GET_SCHEMAS_SQL, snowflake_connector=sfc)
    statements = [f"DROP SCHEMA {s[0]}" for s in schemas]
    await snowflake_query.map(query=statements, snowflake_connector=unmapped(sfc))  # type: ignore  # noqa: E501


FLOW_SPEC = FlowSpec(
    flow=delete_old_dev_schemas,
    docker_env="base",
    deployments=[
        FlowDeployment(name="deployment", cron="0 0 * * *"),
    ],
)

if __name__ == "__main__":
    flow.run()
