import prefect
from prefect import task, Flow
import sys
import os

from src.api_clients.pocket_snowflake_query import PocketSnowflakeQuery

# Setting variables used for the flow
FLOW_NAME = "Debug Prefect Storage"

@task
def get_debug_info():
    logger = prefect.context.get("logger")

    logger.info(f"sys.path = {sys.path}")
    logger.info(f"/ = {os.listdir('/')}")
    logger.info(f"/ = {os.listdir('/src')}")


with Flow(FLOW_NAME) as flow:
    debug_info = get_debug_info()
    query_result = PocketSnowflakeQuery()(query="SELECT 1,2,3;")
    query_result.set_upstream(debug_info)

# for execution in development only
if __name__ == "__main__":
    flow.run()
