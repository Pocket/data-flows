from prefect import Flow, Parameter, case
from prefect.tasks.control_flow import merge

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
from global_training_features_flow import DAY_IN_MINUTES, QUERY_PARAMS, check_refresh, \
    load_query, load_feature_record

FLOW_NAME = get_flow_name(__file__)

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=DAY_IN_MINUTES)) as flow:

    # full_refresh if feature group needs to be rebuilt from scratch, e.g. schema change
    full_refresh = Parameter("full_refresh", default=False)

    training_feature_group = Parameter("feature group",
                                       default=f"{config.ENVIRONMENT}-timespent-prospect-modeling-data-v1")

    refresh_query_file = Parameter("refresh_query_file",
                                   default="timespent_training_full.sql")

    inc_query_file = Parameter("inc_query_file",
                               default="timespent_training_inc.sql")

    refresh = check_refresh(full_refresh)
    print(refresh)

    with case(refresh, True):
        query_r = load_query(refresh_query_file)

    with case(refresh, False):
        query_i = load_query(inc_query_file)

    query = merge(query_i, query_r)

    reviewed_prospects = PocketSnowflakeQuery()(
        query=query,
        data=QUERY_PARAMS,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DATA_FRAME,
    )

    load_feature_record(reviewed_prospects, feature_group_name=training_feature_group)

if __name__ == "__main__":
    flow.run(parameters=dict(full_refresh=False))
