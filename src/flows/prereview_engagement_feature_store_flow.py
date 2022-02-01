from prefect import Flow

# Setting the working directory to project root makes the path start at "src"
from api_clients.prefect_key_value_store_client import get_last_executed_value, update_last_executed_value
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from common_tasks.load_data import dataframe_to_feature_group
from utils import config

# Setting variables used for the flow
FLOW_NAME = "PreReview Engagement to Feature Group Flow"
FEATURE_GROUP_NAME = f"{config.ENV}-prereview-engagement-metrics-v1"

extract_sql = f"""
        select
            RESOLVED_ID_TIME_ADDED_FEED_ID_KEY as ID,
            to_varchar(updated_at,'yyyy-MM-dd"T"HH:mm:ssZ') as UPDATED_AT,
            to_varchar(time_added,'yyyy-MM-dd"T"HH:mm:ssZ') as TIME_ADDED,
            RESOLVED_ID as RESOLVED_ID,
            FEED_ID as FEED_ID,
            STATUS as STATUS,
            TYPE as TYPE,
            RESOLVED_URL as RESOLVED_URL,
            DAY1_SAVE_COUNT as DAY1_SAVE_COUNT,
            DAY2_SAVE_COUNT as DAY2_SAVE_COUNT,
            DAY3_SAVE_COUNT as DAY3_SAVE_COUNT,
            DAY4_SAVE_COUNT as DAY4_SAVE_COUNT,
            DAY5_SAVE_COUNT as DAY5_SAVE_COUNT,
            DAY6_SAVE_COUNT as DAY6_SAVE_COUNT,
            DAY7_SAVE_COUNT as DAY7_SAVE_COUNT,
            WEEK1_SAVE_COUNT as WEEK1_SAVE_COUNT,
            WEEK1_OPEN_COUNT as WEEK1_OPEN_COUNT,
            WEEK1_SHARE_COUNT as WEEK1_SHARE_COUNT,
            WEEK1_FAVORITE_COUNT as WEEK1_FAVORITE_COUNT
        from analytics.dbt.pre_curated_reading_metrics
        where updated_at > %s
        ;
    """

with Flow(FLOW_NAME) as flow:
    promised_get_last_executed_flow_result = get_last_executed_value(flow_name=FLOW_NAME)

    promised_extract_from_snowflake_result = PocketSnowflakeQuery()(
        data=(promised_get_last_executed_flow_result,),
        query=extract_sql
    )

    promised_dataframe_to_feature_group_result = dataframe_to_feature_group(
        dataframe=promised_extract_from_snowflake_result,
        feature_group_name=FEATURE_GROUP_NAME
    )

    # Set upstream dependency on the "dataframe_to_feature_group" task
    promised_update_last_executed_flow_result = \
        update_last_executed_value(for_flow=FLOW_NAME).set_upstream(promised_dataframe_to_feature_group_result)

# for execution in development only
if __name__ == "__main__":
    flow.run()
