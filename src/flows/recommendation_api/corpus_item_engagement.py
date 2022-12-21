import pandas as pd
import prefect
from prefect import Flow, task, case

from api_clients.athena import athena_query
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery

from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

TRAILING_ENGAGEMENT_QUERY = """
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
FROM ANALYTICS.DBT_MMIERMANS.CORPUS_RECOMMENDATION_ENGAGEMENT_TRAILING_DAYS_BY_ITEM
"""

# Query to get records that were not written to at the latest updated_at timestamp.
OLD_RECORDS_QUERY = """
WITH latest_records AS (
    SELECT *,
    row_number() OVER (
        PARTITION BY key ORDER BY updated_at DESC
    ) AS row_number
    FROM "sagemaker_featurestore"."development-corpus-engagement-v1-1671042691"
),
max_updated_at AS (
    SELECT MAX(updated_at) as max_updated_at
    FROM "sagemaker_featurestore"."development-corpus-engagement-v1-1671042691"
)
SELECT key, updated_at FROM latest_records, max_updated_at
WHERE row_number = 1
AND updated_at < max_updated_at
AND NOT is_deleted;
"""

@task()
def log_query_result(result):
    logger = prefect.context.get("logger")
    logger.info(f'Query result: {result}')


@task()
def get_updated_at(df: pd.DataFrame) -> str:
    unique_updated_at = df['UPDATED_AT'].unique()
    if len(unique_updated_at) == 1:
        return unique_updated_at[0]
    else:
        raise ValueError("All rows are expected to have the same updated_at timestamp")


# TODO: Ideally, this flow would be triggered after Dbt model used in the above query is updated, instead of every hour.
with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    new_records = PocketSnowflakeQuery()(query=TRAILING_ENGAGEMENT_QUERY)

    load_task = dataframe_to_feature_group(new_records, feature_group_name=f"{config.ENVIRONMENT}-corpus-engagement-v1")

    # Find and delete records from a previous run.
    records_to_delete = athena_query(query=OLD_RECORDS_QUERY, query_parameters=[updated_at], upstream_tasks=[load_task])
    log_query_result(records_to_delete)


if __name__ == "__main__":
    flow.run()
