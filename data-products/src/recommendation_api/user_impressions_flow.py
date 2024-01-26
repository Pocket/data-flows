from common.databases.snowflake_utils import (
    CS,
    MozSnowflakeConnector,
    query_to_dataframe_batches,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from dask.distributed import Client
from prefect import flow
from prefect.utilities.annotations import quote
from prefect_dask import DaskTaskRunner
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
    sfc = MozSnowflakeConnector(
        warehouse=f"PREFECT_WH_{CS.dev_or_production.upper()}_XLARGE"
    )

    impression_data = query_to_dataframe_batches(
        snowflake_connector=sfc,
        query=BASE_QUERY,
        params={
            "MAX_IMPR_AGE": max_impr_age,
            "MAX_IMPRS": max_impr_count,
            "AGG_WINDOW_DAYS": 29,
        },
    )

    environment_map = {"dev": "development", "production": "production"}

    feature_group = f"{environment_map[CS.dev_or_production]}-user-impressions-v2"

    client = Client()

    async for df in impression_data:  # type: ignore
        df["UPDATED_AT"] = df.UPDATED_AT.apply(  # type: ignore
            lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        # wrapping large dataframes in quote
        await dataframe_to_feature_group.with_options(
            task_runner=DaskTaskRunner(address=client.scheduler.address)  # type: ignore
        )(
            dataframe=quote(df), feature_group_name=feature_group  # type: ignore
        )


FLOW_SPEC = FlowSpec(
    flow=user_impressions,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="deployment",
            cron="0 18 * * *",
            timezone="America/Chicago",
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
