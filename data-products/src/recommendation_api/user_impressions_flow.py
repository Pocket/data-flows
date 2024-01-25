from common.databases.snowflake_utils import (
    CS,
    MozSnowflakeConnector,
    query_to_dataframe,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow
from shared.feature_store import dataframe_to_feature_group

BASE_QUERY = """
WITH prep AS (
  SELECT
      r.HASHED_USER_ID,
      r.CORPUS_ITEM_ID,
      COUNT(*) as impression_count,
      MIN(i.happened_at) as first_impression_time
  FROM "ANALYTICS"."DBT"."IMPRESSIONS" i
  JOIN "ANALYTICS"."DBT_STAGING"."STG_CORPUS_SLATE_RECOMMENDATIONS" r on i.CORPUS_RECOMMENDATION_ID = r.CORPUS_RECOMMENDATION_ID
  WHERE i.HAPPENED_AT > CURRENT_DATE - %(AGG_WINDOW_DAYS)s
    AND r.HASHED_USER_ID IS NOT NULL
  GROUP BY 1,2
  ORDER BY impression_count DESC
)

SELECT 
    "HASHED_USER_ID",
    current_date as "UPDATED_AT",
    ('[' || LISTAGG(DISTINCT CORPUS_ITEM_ID, ',') || ']') AS "CORPUS_IDS"
FROM 
    (SELECT * FROM prep WHERE impression_count > %(MAX_IMPRS)s
     UNION 
     SELECT * FROM prep WHERE first_impression_time < CURRENT_DATE - %(MAX_IMPR_AGE)s )
GROUP BY 1,2
"""


@flow()
async def user_impressions(max_impr_age: int = 14, max_impr_count: int = 9):
    sfc = MozSnowflakeConnector()

    impression_data = await query_to_dataframe(
        snowflake_connector=sfc,
        query=BASE_QUERY,
        params={
            "MAX_IMPR_AGE": max_impr_age,
            "MAX_IMPRS": max_impr_count,
            "AGG_WINDOW_DAYS": 29,
        },
    )

    impression_data["UPDATED_AT"] = impression_data.UPDATED_AT.apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    environment_map = {"dev": "development", "production": "production"}

    feature_group = f"{environment_map[CS.dev_or_production]}-user-impressions-v2"

    await dataframe_to_feature_group(
        dataframe=impression_data, feature_group_name=feature_group
    )


FLOW_SPEC = FlowSpec(
    flow=user_impressions,
    docker_env="base",
    deployments=[
        FlowDeployment(
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

    asyncio.run(user_impressions())
