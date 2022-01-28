from prefect import task, Flow

# Setting the working directory to project root makes the path start at "src"
from api_clients.prefect_key_value_store_client import get_last_executed_value, update_last_executed_value
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from common_tasks.transform_data import df_field_strip
from common_tasks.load_data import dataframe_to_feature_group
from utils import config

# Setting variables used for the flow
FLOW_NAME = "PostReview Engagement to Feature Group Flow"
FEATURE_GROUP_NAME = f"{config.ENV}-postreview-enagement-aggregate-metrics-v1"

extract_sql = f"""
    select
        to_varchar(TIME_LIVE,'yyyy-MM-dd"T"HH:mm:ssZ')::string as TIME_LIVE,
        to_varchar(UPDATED_AT,'yyyy-MM-dd"T"HH:mm:ssZ')::string as UPDATED_AT,
        FEED_ID,
        PROSPECT_ID,
        RESOLVED_ID,
        TITLE,
        STATUS,
        CURATOR,
        NEWTAB_IMPRESSIONS_FIRST_DAY,
        NEWTAB_IMPRESSIONS_FIRST_WEEK,
        NEWTAB_IMPRESSIONS_FIRST_MONTH,
        NEWTAB_OPENS_FIRST_DAY,
        NEWTAB_OPENS_FIRST_WEEK,
        NEWTAB_OPENS_FIRST_MONTH,
        POCKET_APP_SAVES_FIRST_DAY,
        POCKET_APP_SAVES_FIRST_WEEK,
        POCKET_APP_SAVES_FIRST_MONTH,
        POCKET_APP_OPENS_FIRST_DAY,
        POCKET_APP_OPENS_FIRST_WEEK,
        POCKET_APP_OPENS_FIRST_MONTH,
        RECS_SURFACES_SAVES_FIRST_DAY,
        RECS_SURFACES_SAVES_FIRST_WEEK,
        RECS_SURFACES_SAVES_FIRST_MONTH,
        RECS_SURFACES_OPENS_FIRST_DAY,
        RECS_SURFACES_OPENS_FIRST_WEEK,
        RECS_SURFACES_OPENS_FIRST_MONTH,
        POCKET_APP_TIMESPEND_FIRST_DAY,
        POCKET_APP_TIMESPEND_FIRST_WEEK,
        POCKET_APP_TIMESPEND_FIRST_MONTH,
        TIME_PERIOD_TOTAL_NEWTAB_SAVES,
        TIME_PERIOD_TOTAL_NEWTAB_DISMISSALS            
    from analytics.dbt.all_surfaces_engagements_past_30_day_aggregations
    where UPDATED_AT > %s
    ;
"""

with Flow(FLOW_NAME) as flow:
    promised_get_last_executed_flow_result = get_last_executed_value(flow_name=FLOW_NAME)

    promised_extract_from_snowflake_result = PocketSnowflakeQuery()(
        query=extract_sql,
        data=(promised_get_last_executed_flow_result,)
    )

    promised_transformed_result = df_field_strip(dataframe=promised_extract_from_snowflake_result, field_name='TITLE')

    promised_dataframe_to_feature_group_result = dataframe_to_feature_group(
        dataframe=promised_transformed_result,
        feature_group_name=FEATURE_GROUP_NAME
    )

    # Set upstream dependency on the "dataframe_to_feature_group" task
    promised_update_last_executed_flow_result = update_last_executed_value(for_flow=FLOW_NAME).set_upstream(promised_dataframe_to_feature_group_result)

# for execution in development only
if __name__ == "__main__":
    flow.run()
