from prefect import task, Flow, Parameter
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
import pandas as pd
import datetime

from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

BASE_QUERY = """
SELECT
    'HOME' as RECOMMENDATION_SURFACE_ID,  -- TDP-192: Get surface id from Snowflake to support surfaces other than Home.
    CORPUS_SLATE_CONFIGURATION_ID,
    CORPUS_ITEM_ID,

    -- aggregate impressions
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE      then impression_count else 0 end) as TRAILING_1_DAY_IMPRESSIONS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 6  then impression_count else 0 end) as TRAILING_7_DAY_IMPRESSIONS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 13 then impression_count else 0 end) as TRAILING_14_DAY_IMPRESSIONS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 20 then impression_count else 0 end) as TRAILING_21_DAY_IMPRESSIONS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 27 then impression_count else 0 end) as TRAILING_28_DAY_IMPRESSIONS,

    -- aggregate opens
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE      then open_count else 0 end) as TRAILING_1_DAY_OPENS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 6  then open_count else 0 end) as TRAILING_7_DAY_OPENS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 13 then open_count else 0 end) as TRAILING_14_DAY_OPENS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 20 then open_count else 0 end) as TRAILING_21_DAY_OPENS,
    sum(case when HAPPENED_AT_DAY >= CURRENT_DATE - 27 then open_count else 0 end) as TRAILING_28_DAY_OPENS

FROM "ANALYTICS"."DBT"."CORPUS_RECOMMENDATION_ENGAGEMENT_BY_ITEM_BY_DAY"
WHERE HAPPENED_AT_DAY >= CURRENT_DATE - 27
GROUP BY 1, 2, 3
"""


@task
def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    key_columns = ['RECOMMENDATION_SURFACE_ID', 'CORPUS_SLATE_CONFIGURATION_ID', 'CORPUS_ITEM_ID']
    df['KEY'] = df[key_columns].agg('/'.join, axis=1)
    df['UPDATED_AT'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


# TODO: Ideally, this flow would be triggered after Dbt model used in the above query is updated, instead of every hour.
with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    snowflake_result = PocketSnowflakeQuery()(query=BASE_QUERY)

    transformed_df = transform_df(snowflake_result)

    dataframe_to_feature_group(transformed_df, feature_group_name=f"{config.ENVIRONMENT}-corpus-engagement-v1")

if __name__ == "__main__":
    flow.run()
