from common.databases.snowflake_utils import MozSnowflakeConnector
from prefect import flow, task, unmapped
from prefect_snowflake.database import snowflake_query
from shared.api_clients.sqs import (
    NewTabFeedID,
    RecommendationCandidate,
    put_results,
    validate_candidate_items,
)
from snowflake.connector import DictCursor

CURATED_LONGREADS_CANDIDATE_SET_ID_EN = "dacc55ea-db8d-4858-a51d-e1c78298337e"
CURATED_LONGREADS_CANDIDATE_SET_ID_DE = "cff478b9-301e-47cb-accf-ef2fe84ef17a"

CURATED_EN_US_CANDIDATE_SET_ID = "35018233-48cd-4ec4-bcfd-7b1b1ccf30de"
CURATED_DE_DE_CANDIDATE_SET_ID = "c66a1485-6c87-4c68-b29e-e7e838465ff7"
CURATED_EN_US_NO_SYND_CANDIDATE_SET_ID = "493a5556-9800-449f-8f8c-c27bb6c8c810"
COLLECTIONS_EN_US_CANDIDATE_SET_ID = "303174fc-a9ff-4a51-984a-e09ce7120d18"

CURATED_SHORTREADS_CANDIDATE_SET_ID_EN = "7ef90242-ff7a-44ac-8a32-53193e4a23eb"
CURATED_SHORTREADS_CANDIDATE_SET_ID_DE = "57e4d3d1-9b4a-4a35-82f4-e577d88f6521"

SYNDICATED_EN_US_CANDIDATE_SET_ID = "a8425a46-187a-4cdb-8157-5d2f308c52cd"

LONGREADS_SQL = """SELECT 
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
LIMIT 90"""

CURATED_FEEDS_SQL = """SELECT 
    a.resolved_id as "ID", 
    a.IS_SYNDICATED as "IS_SYNDICATED",
    a.IS_COLLECTION as "IS_COLLECTION",
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" AS a
JOIN "ANALYTICS"."DBT"."CONTENT" AS c
  ON c.CONTENT_ID = a.CONTENT_ID
WHERE a.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD("day", -7, current_timestamp()) AND current_timestamp()
AND a.SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE)s
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT desc
LIMIT 300"""  # noqa: E501

SHORTREADS_SQL = """SELECT 
    a.resolved_id as "ID", 
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" AS a
JOIN "ANALYTICS"."DBT"."CONTENT" AS c
  ON c.CONTENT_ID = a.CONTENT_ID
WHERE a.REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD("day", -90, current_timestamp())
AND c.WORD_COUNT <= 900
AND a.CORPUS_REVIEW_STATUS = 'recommendation'
AND a.SCHEDULED_SURFACE_ID = %(SURFACE_ID)s
AND a.LANGUAGE = %(LANG)s
AND a.IS_SYNDICATED = 0
ORDER BY REVIEWED_CORPUS_ITEM_UPDATED_AT desc
LIMIT 90"""

SYNDICATED_FEED_SQL = """SELECT 
    s.resolved_id as "ID", 
    c.DOMAIN as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" AS s
JOIN "ANALYTICS"."DBT"."SYNDICATED_ARTICLES" AS c
  ON c.POCKET_RESOLVED_ID = s.RESOLVED_ID
WHERE s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp()) AND current_timestamp()
AND s.SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
ORDER BY s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT desc
LIMIT 180"""  # noqa: E501

TOPICS_CORPUS_ITEMS_SQL = """SELECT 
    a.resolved_id as "ID",
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" as a
JOIN "ANALYTICS"."DBT".content as c ON c.content_id = a.content_id
WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD('day', -90, current_timestamp())
AND a.TOPIC = %(CORPUS_TOPIC_ID)s
AND a.LANGUAGE = 'EN'
AND a.CORPUS_REVIEW_STATUS = 'recommendation'
order by a.REVIEWED_CORPUS_ITEM_UPDATED_AT desc
limit 45"""

GET_TOPICS_SQL = """select 
    LEGACY_CURATED_CORPUS_CANDIDATE_SET_ID as "LEGACY_CURATED_CORPUS_CANDIDATE_SET_ID",
    CORPUS_TOPIC_ID as "CORPUS_TOPIC_ID"
from "ANALYTICS"."DBT"."STATIC_CORPUS_CANDIDATE_SET_TOPICS"
"""

SET_PARAM_CONFIG = {
    "longreads": {
        "sql": LONGREADS_SQL,
        "items": [
            {
                "LANG": "EN",
                "CANDIDATE_SET_ID": CURATED_LONGREADS_CANDIDATE_SET_ID_EN,
                "SURFACE_ID": "NEW_TAB_EN_US",
                "FEED_ID": int(NewTabFeedID.en_US),
            },
            {
                "LANG": "DE",
                "CANDIDATE_SET_ID": CURATED_LONGREADS_CANDIDATE_SET_ID_DE,
                "SURFACE_ID": "NEW_TAB_DE_DE",
                "FEED_ID": int(NewTabFeedID.de_DE),
            },
        ],
        "curated": True,
    },
    "curated_feeds": {
        "sql": CURATED_FEEDS_SQL,
        "items": [
            {
                "SCHEDULED_SURFACE": "NEW_TAB_EN_US",
                "CANDIDATE_SET_ID": CURATED_EN_US_CANDIDATE_SET_ID,
                "FEED_ID": int(NewTabFeedID.en_US),
                "COLLNS_ONLY": False,
                "FILTER_SYND": False,
            },
            {
                "SCHEDULED_SURFACE": "NEW_TAB_EN_US",
                "CANDIDATE_SET_ID": CURATED_EN_US_NO_SYND_CANDIDATE_SET_ID,
                "FEED_ID": int(NewTabFeedID.en_US),
                "COLLNS_ONLY": False,
                "FILTER_SYND": True,
            },
            {
                "SCHEDULED_SURFACE": "NEW_TAB_EN_US",
                "CANDIDATE_SET_ID": COLLECTIONS_EN_US_CANDIDATE_SET_ID,
                "FEED_ID": int(NewTabFeedID.en_US),
                "COLLNS_ONLY": True,
                "FILTER_SYND": True,
            },
            {
                "SCHEDULED_SURFACE": "NEW_TAB_DE_DE",
                "CANDIDATE_SET_ID": CURATED_DE_DE_CANDIDATE_SET_ID,
                "FEED_ID": int(NewTabFeedID.de_DE),
                "COLLNS_ONLY": False,
                "FILTER_SYND": False,
            },
        ],
        "curated": True,
    },
    "shortreads": {
        "sql": SHORTREADS_SQL,
        "items": [
            {
                "LANG": "EN",
                "CANDIDATE_SET_ID": CURATED_SHORTREADS_CANDIDATE_SET_ID_EN,
                "SURFACE_ID": "NEW_TAB_EN_US",
                "FEED_ID": int(NewTabFeedID.en_US),
            },
            {
                "LANG": "DE",
                "CANDIDATE_SET_ID": CURATED_SHORTREADS_CANDIDATE_SET_ID_DE,
                "SURFACE_ID": "NEW_TAB_DE_DE",
                "FEED_ID": int(NewTabFeedID.de_DE),
            },
        ],
        "curated": True,
    },
    "syndicated_feed": {
        "sql": SYNDICATED_FEED_SQL,
        "items": [
            {
                "MAX_AGE_DAYS": -9,
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
                "CANDIDATE_SET_ID": SYNDICATED_EN_US_CANDIDATE_SET_ID,
            }
        ],
        "curated": False,
    },
    "topics": {
        "sql": TOPICS_CORPUS_ITEMS_SQL,
        "items_sql": GET_TOPICS_SQL,
        "curated": True,
    },
}

SFC = MozSnowflakeConnector()


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
async def create_set(set_params_id: str):
    sfc = MozSnowflakeConnector()

    async def get_params(set_params_id):
        set_params = SET_PARAM_CONFIG[set_params_id]
        if set_params_id == "topics":
            items = await snowflake_query(
                query=set_params["items_sql"],  # type: ignore
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


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_set("topics"))  # type: ignore
