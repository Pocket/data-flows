from common.databases.snowflake_utils import (
    CS,
    MozSnowflakeConnector,
    query_to_dataframe,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow
from shared.feature_store import dataframe_to_feature_group

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
FROM ANALYTICS.DBT_FACTS.CORPUS_RECOMMENDATION_ENGAGEMENT_TRAILING_DAYS_BY_ITEM
"""


@flow()
async def corpus_item_engagement():
    sfc = MozSnowflakeConnector()

    engagement_data = await query_to_dataframe(
        snowflake_connector=sfc, query=BASE_QUERY
    )

    environment_map = {"dev": "development", "production": "production"}

    feature_group = f"{environment_map[CS.dev_or_production]}-corpus-engagement-v1"

    await dataframe_to_feature_group(
        dataframe=engagement_data, feature_group_name=feature_group
    )


FLOW_SPEC = FlowSpec(
    flow=corpus_item_engagement,
    docker_env="base",
    deployments=[
        FlowDeployment(
            tags=["daily-sla"],
            name="deployment",
            job_variables={
                "cpu": 4096,
                "memory": 8192,
            },
        ),
    ],
)

if __name__ == "__main__":
    import asyncio

    asyncio.run(corpus_item_engagement())
