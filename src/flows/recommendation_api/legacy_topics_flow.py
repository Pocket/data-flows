from typing import List
from prefect import Flow, Parameter, unmapped, task
from prefect.executors import LocalDaskExecutor
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    validate_candidate_items,
)
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID

FLOW_NAME = get_flow_name(__file__)

# Export approved candidate items by language and recency
EXPORT_CORPUS_ITEMS_SQL = """
SELECT 
    a.resolved_id as "ID", 
    c.top_domain_name as "PUBLISHER"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" as a
JOIN "ANALYTICS"."DBT".content as c ON c.content_id = a.content_id
WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD('day', -90, current_timestamp())
AND a.TOPIC = %(CORPUS_TOPIC_ID)s
AND a.LANGUAGE = 'EN'
"""

GET_TOPICS_SQL = """
select 
    CURATEDCANDIDATEID as "CURATED_CORPUS_CANDIDATE_SET_ID",
    CORPUS_TOPIC_ID as "CORPUS_TOPIC_ID"    
from "ANALYTICS"."DBT"."STATIC_TOPIC_MAP_GUIDS" as m
join "ANALYTICS"."DBT"."STATIC_TOPIC_LIST" as t on t.id = m.id
where CURATEDCANDIDATEID is not null and CORPUS_TOPIC_ID is not null
"""

@task()
def get_candidate_set_ids(topics):
    return [i['CURATED_CORPUS_CANDIDATE_SET_ID'] for i in topics]

@task()
def transform_to_candidates(records: dict) -> List[RecommendationCandidate]:
    return [RecommendationCandidate(
        item_id=rec["ID"],
        publisher=rec["PUBLISHER"],
        feed_id=int(NewTabFeedID.en_US)
    ) for rec in records]

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30), executor=LocalDaskExecutor()) as flow:
    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type = OutputType.DICT
    )

    # Fetch a list of Topic Candidates
    topics = query(query=GET_TOPICS_SQL)

    candidate_set_ids = get_candidate_set_ids(topics)

    # Fetch the most recent Topic Candidate Items
    topic_candidate_items = query.map(data=topics, query=unmapped(EXPORT_CORPUS_ITEMS_SQL))

    # Fetch the most recent Topic Candidate Items
    valid_topic_candidate_items = validate_candidate_items.map(topic_candidate_items)

    # Write Topic Candidate sets to SQS
    candidates = transform_to_candidates.map(valid_topic_candidate_items)
    put_results.map(candidate_set_ids, candidates, curated=unmapped(True))

if __name__ == "__main__":
    flow.run()
