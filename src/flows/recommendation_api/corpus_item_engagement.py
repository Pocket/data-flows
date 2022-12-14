from prefect import task, Flow, Parameter
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
import pandas as pd
import datetime

from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

BASE_QUERY = """
with trailing_engagement as (
    SELECT
        'HOME' as RECOMMENDATION_SURFACE_ID,
        e.CORPUS_SLATE_CONFIGURATION_ID,
        e.CORPUS_ITEM_ID,

        -- aggregate impressions
        sum(case when happened_at >= CURRENT_DATE      then impression_count else 0 end) as trailing_1_day_impressions,
        sum(case when happened_at >= CURRENT_DATE - 6  then impression_count else 0 end) as trailing_7_day_impressions,
        sum(case when happened_at >= CURRENT_DATE - 13 then impression_count else 0 end) as trailing_14_day_impressions,
        sum(case when happened_at >= CURRENT_DATE - 20 then impression_count else 0 end) as trailing_21_day_impressions,
        sum(case when happened_at >= CURRENT_DATE - 27 then impression_count else 0 end) as trailing_28_day_impressions,

        -- aggregate opens
        sum(case when happened_at >= CURRENT_DATE      then open_count else 0 end) as trailing_1_day_opens,
        sum(case when happened_at >= CURRENT_DATE - 6  then open_count else 0 end) as trailing_7_day_opens,
        sum(case when happened_at >= CURRENT_DATE - 13 then open_count else 0 end) as trailing_14_day_opens,
        sum(case when happened_at >= CURRENT_DATE - 20 then open_count else 0 end) as trailing_21_day_opens,
        sum(case when happened_at >= CURRENT_DATE - 27 then open_count else 0 end) as trailing_28_day_opens

    FROM "ANALYTICS"."DBT_MMIERMANS"."CORPUS_RECOMMENDATION_ENGAGEMENT_BY_ITEM_BY_DAY" e
    WHERE e.happened_at >= CURRENT_DATE - 27
    GROUP BY 1, 2, 3
)

SELECT 
    
    trailing_engagement.*
FROM trailing_engagement;
"""


@task
def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    key_columns = ['RECOMMENDATION_SURFACE_ID', 'CORPUS_SLATE_CONFIGURATION_ID', 'CORPUS_ITEM_ID']
    df['KEY'] = df[key_columns].agg('/'.join, axis=1)
    df['UPDATED_AT'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


# Ideally, this flow would be triggered after Dbt model used in the above query is updated.
with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    feature_group = Parameter("feature group", default=f"{config.ENVIRONMENT}-corpus-engagement-v1")

    snowflake_result = PocketSnowflakeQuery()(query=BASE_QUERY)

    transformed_df = transform_df(snowflake_result)
    dataframe_to_feature_group(df=transformed_df, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
