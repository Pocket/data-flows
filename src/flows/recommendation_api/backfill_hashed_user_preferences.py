from prefect import Flow, Parameter, unmapped, task
from prefect.executors import LocalDaskExecutor
from utils.flow import get_flow_name, get_interval_schedule
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.athena import AthenaQuery

FLOW_NAME = get_flow_name(__file__)

ATHENA_SQL = """
SELECT *
FROM "sagemaker_featurestore"."development-user-recommendation-preferences-v1-1654826050" 
limit 10
"""

@task
def print_results(param_name, param_value):
    print(f'{param_name}: {param_value}')


with Flow(FLOW_NAME) as flow:

    user_topics_prefs = AthenaQuery(
        query=ATHENA_SQL,
    )

    print_results('user_topics_prefs', user_topics_prefs, task_args=dict(name="user_topics_prefs"))


if __name__ == "__main__":
    flow.run()
