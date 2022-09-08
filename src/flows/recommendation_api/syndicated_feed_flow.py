from typing import List
from prefect import Flow, task

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID
from common_tasks.corpus_candidate_set import validate_candidate_items
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
FLOW_NAME = get_flow_name(__file__)

SYNDICATED_EN_US_CANDIDATE_SET_ID = "a8425a46-187a-4cdb-8157-5d2f308c52cd"

# Export approved corpus items by language and recency
EXPORT_SCHEDULED_ITEMS_SQL = """
SELECT 
    s.resolved_id as "ID", 
    c.DOMAIN as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" AS s
JOIN "ANALYTICS"."DBT"."SYNDICATED_ARTICLES" AS c
  ON c.POCKET_RESOLVED_ID = s.RESOLVED_ID
WHERE s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp()) AND current_timestamp()
AND s.SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
ORDER BY s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT desc
LIMIT 180
"""

@task()
def transform_to_candidates(records: dict, feed_id: int) -> List[RecommendationCandidate]:
    return [RecommendationCandidate(
        item_id=rec["ID"],
        publisher=rec["PUBLISHER"],
        feed_id=feed_id
    ) for rec in records]


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:

    # query snowflake for items from pocket hits
    records = PocketSnowflakeQuery()(
        query=EXPORT_SCHEDULED_ITEMS_SQL,
        data={
            "MAX_AGE_DAYS": -9,
            "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    valid_scheduled_candidate_items = validate_candidate_items(records)

    # post curated candidate sets to SQS
    candidate_set = transform_to_candidates(valid_scheduled_candidate_items, int(NewTabFeedID.en_US))

    put_results(SYNDICATED_EN_US_CANDIDATE_SET_ID, candidate_set)

if __name__ == "__main__":
    flow.run()
