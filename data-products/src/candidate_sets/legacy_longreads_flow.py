from typing import List
# from prefect import Flow, task, unmapped

# from common.databases.snowflake_utils import PktSnowflakeConnector
# from prefect_snowflake.database import snowflake_query


from common.databases.snowflake_utils import PktSnowflakeConnector
from snowflake.connector import DictCursor
from prefect import flow, task, unmapped
from prefect_snowflake.database import snowflake_query
from shared.api_clients.sqs import (
    NewTabFeedID,
    validate_candidate_items,
    RecommendationCandidate,
    put_results,
)
from common.deployment import FlowSpec, FlowEnvar, FlowDeployment
from prefect.server.schemas.schedules import CronSchedule
from common.settings import CommonSettings

# from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
# from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID, validate_candidate_items
# from utils import config
# from utils.flow import get_flow_name, get_interval_schedule


# FLOW_NAME = get_flow_name(__file__)

CS = CommonSettings() # type: ignore

CURATED_LONGREADS_CANDIDATE_SET_ID_EN = "dacc55ea-db8d-4858-a51d-e1c78298337e"
CURATED_LONGREADS_CANDIDATE_SET_ID_DE = "cff478b9-301e-47cb-accf-ef2fe84ef17a"

# Export approved corpus items by language and recency
EXPORT_LONGREADS_ITEMS_SQL = """
SELECT 
    a.resolved_id as "ID", 
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" AS a
JOIN "ANALYTICS"."DBT"."CONTENT" AS c
  ON c.CONTENT_ID = a.CONTENT_ID
WHERE a.REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD("day", -90, current_timestamp())
AND c.WORD_COUNT >= 4500
AND a.CORPUS_REVIEW_STATUS = 'recommendation'
AND a.SCHEDULED_SURFACE_ID = %(SURFACE_ID)s
AND a.LANGUAGE = %(LANG)s
AND a.IS_SYNDICATED = 0
ORDER BY REVIEWED_CORPUS_ITEM_UPDATED_AT desc
LIMIT 90
"""

@task()
async def transform_to_candidates(records: dict, feed_id: int) -> List[RecommendationCandidate]:
    return [
        RecommendationCandidate(
            item_id=rec["ID"], publisher=rec["PUBLISHER"], feed_id=feed_id
    ) 
    for rec in records
]


# with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=180)) as flow:
@flow()
async def main():
    sfc = PktSnowflakeConnector() # copied from curared_candidates_flow.py

    set_params = [
        {
            "LANG": "EN", 
            "CANDIDATE_SET_ID": CURATED_LONGREADS_CANDIDATE_SET_ID_EN,
            "SURFACE_ID": "NEW_TAB_EN_US", 
            "FEED_ID": int(NewTabFeedID.en_US)
        },
        {
            "LANG": "DE", 
             "CANDIDATE_SET_ID": CURATED_LONGREADS_CANDIDATE_SET_ID_DE,
            "SURFACE_ID": "NEW_TAB_DE_DE", 
            "FEED_ID": int(NewTabFeedID.de_DE)
        }
    ]
    
    longreads_candidate_items = await snowflake_query.map(
        query=unmapped(EXPORT_LONGREADS_ITEMS_SQL),  # type: ignore
        snowflake_connector=unmapped(sfc),  # type: ignore
        params=set_params,  # type: ignore
        cursor_type=DictCursor,  # type: ignore # why isn't this unmapped?
    )


    # query = PocketSnowflakeQuery(
    #     database=config.SNOWFLAKE_ANALYTICS_DATABASE,
    #     schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
    #     output_type=OutputType.DICT
    # )

    

    # Fetch the most recent curated longreads per langauge
    # longreads_candidate_items = query.map(data=set_params, query=unmapped(EXPORT_LONGREADS_ITEMS_SQL))

    valid_longreads_candidate_items = validate_candidate_items.map(longreads_candidate_items)

    # Write longreads candidate sets to SQS
    candidate_sets = transform_to_candidates.map(valid_longreads_candidate_items,
                                                 [p["FEED_ID"] for p in set_params])

    put_results.map(
        [p["CANDIDATE_SET_ID"] for p in set_params],
        candidate_sets, 
        curated=unmapped(True)
    )

FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-credentials",
        ),
        FlowEnvar(
            envar_name="FREESTAR_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/freestar-credentials",
        ),
    ],
    deployments=[
        FlowDeployment(
            deployment_name="base"
        ), # type: ignore
    ],
)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main()) # type: ignore
