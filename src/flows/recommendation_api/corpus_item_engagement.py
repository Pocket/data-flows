from prefect import Flow
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery

from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

BASE_QUERY = """
SELECT
    SURFACE_CONFIGURATION_ITEM_KEY as KEY,
    TO_CHAR(updated_at_day, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
    RECOMMENDATION_SURFACE_ID,
    CORPUS_SLATE_CONFIGURATION_ID,
    CORPUS_ITEM_ID,
    TRAILING_1_DAY_IMPRESSIONS,
    TRAILING_1_DAY_OPENS,
    TRAILING_7_DAY_IMPRESSIONS,
    TRAILING_7_DAY_OPENS,
    TRAILING_14_DAY_IMPRESSIONS,
    TRAILING_14_DAY_OPENS,
    TRAILING_21_DAY_IMPRESSIONS,
    TRAILING_21_DAY_OPENS,
    TRAILING_28_DAY_IMPRESSIONS,
    TRAILING_28_DAY_OPENS
FROM ANALYTICS.DBT.CORPUS_RECOMMENDATION_ENGAGEMENT_TRAILING_DAYS_BY_ITEM
"""


# TODO: Ideally, this flow would be triggered after Dbt model used in the above query is updated, instead of every hour.
with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    dataframe_to_feature_group(
        dataframe=PocketSnowflakeQuery()(query=BASE_QUERY),
        feature_group_name=f"{config.ENVIRONMENT}-corpus-engagement-v1")

if __name__ == "__main__":
    flow.run()
