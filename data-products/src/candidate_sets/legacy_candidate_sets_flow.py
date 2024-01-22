from candidate_config import SET_PARAM_CONFIG
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, task, unmapped
from prefect_snowflake.database import snowflake_query
from shared.api_clients.sqs import (
    NewTabFeedID,
    RecommendationCandidate,
    put_results,
    validate_candidate_items,
)
from snowflake.connector import DictCursor


@task()
def transform_to_candidates(
    records: dict, feed_id: int, collns_only: bool = False, filter_synd: bool = False
) -> list[RecommendationCandidate]:
    if collns_only:
        return [
            RecommendationCandidate(
                item_id=rec["ID"], publisher=rec["PUBLISHER"], feed_id=feed_id
            )
            for rec in records
            if rec["IS_COLLECTION"] == 1
        ]
    elif filter_synd:
        return [
            RecommendationCandidate(
                item_id=rec["ID"], publisher=rec["PUBLISHER"], feed_id=feed_id
            )
            for rec in records
            if rec["IS_SYNDICATED"] == 0
        ]
    else:
        return [
            RecommendationCandidate(
                item_id=rec["ID"], publisher=rec["PUBLISHER"], feed_id=feed_id
            )
            for rec in records
        ]


@flow()
async def create_legacy_candidate_set(set_params_id: str):
    sfc = MozSnowflakeConnector()

    async def get_params(set_params_id):
        set_params = SET_PARAM_CONFIG[set_params_id]
        if x := set_params.get("items_sql"):
            items = await snowflake_query(
                query=x,  # type: ignore
                snowflake_connector=unmapped(sfc),  # type: ignore
                cursor_type=DictCursor,  # type: ignore # why isn't this unmapped?
            )
            set_params["items"] = items
            set_params["candidate_set_ids"] = [
                i["LEGACY_CURATED_CORPUS_CANDIDATE_SET_ID"] for i in items  # type: ignore  # noqa: E501
            ]
        return set_params

    set_params = await get_params(set_params_id)

    candidate_items = await snowflake_query.map(
        query=unmapped(set_params["sql"]),  # type: ignore
        snowflake_connector=unmapped(sfc),  # type: ignore
        params=set_params["items"],  # type: ignore
        cursor_type=DictCursor,  # type: ignore # why isn't this unmapped?
    )

    valid_candidate_items = validate_candidate_items.map(candidate_items)

    candidate_sets = transform_to_candidates.map(
        valid_candidate_items,  # type: ignore
        [p.get("FEED_ID", int(NewTabFeedID.en_US)) for p in set_params["items"]],  # type: ignore  # noqa: E501
        [p.get("COLLNS_ONLY", False) for p in set_params["items"]],  # type: ignore
        [p.get("FILTER_SYND", False) for p in set_params["items"]],  # type: ignore  # noqa: E501
    )
    candidate_set_ids = set_params.get(  # type: ignore
        "candidate_set_ids", [p.get("CANDIDATE_SET_ID") for p in set_params["items"]]  # type: ignore  # noqa: E501
    )

    put_results.map(
        candidate_set_ids,
        candidate_sets,  # type: ignore
        curated=unmapped(set_params["curated"]),  # type: ignore
    )


FLOW_SPEC = FlowSpec(
    flow=create_legacy_candidate_set,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="topics", cron="*/30 * * * *", parameters={"set_params_id": "topics"}
        ),
        FlowDeployment(
            name="longreads",
            cron="*/180 * * * *",
            parameters={"set_params_id": "longreads"},
        ),
        FlowDeployment(
            name="shortreads",
            cron="*/180 * * * *",
            parameters={"set_params_id": "shortreads"},
        ),
        FlowDeployment(
            name="curated_feeds",
            cron="*/60 * * * *",
            parameters={"set_params_id": "curated_feeds"},
        ),
        FlowDeployment(
            name="syndicated_feed",
            cron="*/60 * * * *",
            parameters={"set_params_id": "syndicated_feed"},
        ),
    ],
)


if __name__ == "__main__":
    import asyncio
    import sys

    asyncio.run(create_legacy_candidate_set(sys.argv[1]))  # type: ignore
