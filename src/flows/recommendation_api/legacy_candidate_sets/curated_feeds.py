from typing import List
from prefect import Flow, task, unmapped

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID, validate_candidate_items
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
FLOW_NAME = get_flow_name(__file__)

CURATED_EN_US_CANDIDATE_SET_ID = "35018233-48cd-4ec4-bcfd-7b1b1ccf30de"
CURATED_DE_DE_CANDIDATE_SET_ID = "c66a1485-6c87-4c68-b29e-e7e838465ff7"
CURATED_EN_US_NO_SYND_CANDIDATE_SET_ID = "493a5556-9800-449f-8f8c-c27bb6c8c810"

# Export approved corpus items by language and recency
EXPORT_SCHEDULED_ITEMS_SQL = """
SELECT 
    a.resolved_id as "ID", 
    a.IS_SYNDICATED as "IS_SYNDICATED",
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" AS a
JOIN "ANALYTICS"."DBT"."CONTENT" AS c
  ON c.CONTENT_ID = a.CONTENT_ID
WHERE a.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD("day", -7, current_timestamp()) AND current_timestamp()
AND a.SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE)s
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT desc
LIMIT 180
"""

@task()
def transform_to_candidates(records: dict, feed_id: int, filter_synd: bool) -> List[RecommendationCandidate]:

    if filter_synd:
        return [RecommendationCandidate(
            item_id=rec["ID"],
            publisher=rec["PUBLISHER"],
            feed_id=feed_id
        ) for rec in records if rec["IS_SYNDICATED"] == 0]
    else:
        return [RecommendationCandidate(
            item_id=rec["ID"],
            publisher=rec["PUBLISHER"],
            feed_id=feed_id
        ) for rec in records]


with Flow(FLOW_NAME) as flow:

    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT
    )

    set_params = [{"SCHEDULED_SURFACE": "NEW_TAB_EN_US", "CANDIDATE_SET_ID": CURATED_EN_US_CANDIDATE_SET_ID,
                   "FEED_ID": int(NewTabFeedID.en_US), "FILTER_SYND": False},
                  {"SCHEDULED_SURFACE": "NEW_TAB_EN_US", "CANDIDATE_SET_ID": CURATED_EN_US_NO_SYND_CANDIDATE_SET_ID,
                   "FEED_ID": int(NewTabFeedID.en_US), "FILTER_SYND": True},
                  {"SCHEDULED_SURFACE": "NEW_TAB_DE_DE", "CANDIDATE_SET_ID": CURATED_DE_DE_CANDIDATE_SET_ID,
                   "FEED_ID": int(NewTabFeedID.de_DE), "FILTER_SYND": False}]

    # Fetch the most recent curated items per candidate set
    scheduled_candidate_items = query.map(data=set_params, query=unmapped(EXPORT_SCHEDULED_ITEMS_SQL))

    valid_scheduled_candidate_items = validate_candidate_items.map(scheduled_candidate_items)

    # post curated candidate sets to SQS
    candidate_sets = transform_to_candidates.map(valid_scheduled_candidate_items,
                                                 [p["FEED_ID"] for p in set_params],
                                                 [p["FILTER_SYND"] for p in set_params])

    put_results.map([p["CANDIDATE_SET_ID"] for p in set_params],
                    candidate_sets, curated=unmapped(True))

if __name__ == "__main__":
    flow.run()
