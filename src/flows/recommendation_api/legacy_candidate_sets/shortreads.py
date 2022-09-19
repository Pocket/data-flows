from typing import List
from prefect import Flow, task, unmapped

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID, validate_candidate_items
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
FLOW_NAME = get_flow_name(__file__)

CURATED_SHORTREADS_CANDIDATE_SET_ID_EN = "7ef90242-ff7a-44ac-8a32-53193e4a23eb"
CURATED_SHORTREADS_CANDIDATE_SET_ID_DE = "57e4d3d1-9b4a-4a35-82f4-e577d88f6521"

# Export approved corpus items by language and recency
EXPORT_SHORTREADS_ITEMS_SQL = """
SELECT 
    a.resolved_id as "ID", 
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" AS a
JOIN "ANALYTICS"."DBT"."CONTENT" AS c
  ON c.CONTENT_ID = a.CONTENT_ID
WHERE a.REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD("day", -90, current_timestamp())
AND c.WORD_COUNT <= 900
AND a.CORPUS_REVIEW_STATUS = 'recommendation'
AND a.LANGUAGE = %(LANG)s
AND a.IS_SYNDICATED = 0
ORDER BY REVIEWED_CORPUS_ITEM_UPDATED_AT desc
LIMIT 90
"""

@task()
def transform_to_candidates(records: dict, feed_id: int) -> List[RecommendationCandidate]:
    return [RecommendationCandidate(
        item_id=rec["ID"],
        publisher=rec["PUBLISHER"],
        feed_id=feed_id
    ) for rec in records]


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=180)) as flow:

    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT
    )

    set_params = [{"LANG": "EN", "CANDIDATE_SET_ID": CURATED_SHORTREADS_CANDIDATE_SET_ID_EN,
                   "FEED_ID": int(NewTabFeedID.en_US)},
                  {"LANG": "DE", "CANDIDATE_SET_ID": CURATED_SHORTREADS_CANDIDATE_SET_ID_DE,
                   "FEED_ID": int(NewTabFeedID.de_DE)}]

    # Fetch the most recent curated shortreads per langauge
    shortreads_candidate_items = query.map(data=set_params, query=unmapped(EXPORT_SHORTREADS_ITEMS_SQL))

    valid_shortreads_candidate_items = validate_candidate_items.map(shortreads_candidate_items)

    # Write shortreads candidate sets to SQS
    candidate_sets = transform_to_candidates.map(valid_shortreads_candidate_items,
                                                 [p["FEED_ID"] for p in set_params])

    put_results.map([p["CANDIDATE_SET_ID"] for p in set_params],
                    candidate_sets, curated=unmapped(True))


if __name__ == "__main__":
    flow.run()
