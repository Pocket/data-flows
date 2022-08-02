from typing import List

from prefect import Flow, task

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from utils import config
from utils.flow import get_flow_name, get_interval_schedule
from api_clients.sqs import put_results, RecommendationCandidate, NewTabFeedID

'''
Builds pocket hits candidate sets for en and de and writes it to SQS and Feature Store
'''

FLOW_NAME = get_flow_name(__file__)

POCKETHITS_EN_CANDIDATE_SET_ID = "92411893-ebdb-4a43-ad29-aa79e56e2136"

POCKETHITS_SQL = """
SELECT
    s.resolved_id as id,   
    c.top_domain_name as publisher,
    s.title,
    s.scheduled_corpus_item_scheduled_at,
    s.scheduled_surface_name,
    s.scheduled_surface_id,
    s.topic,
    s.approved_corpus_item_external_id
FROM scheduled_corpus_items as s
JOIN content as c
  ON c.content_id = s.content_id
WHERE s.scheduled_surface_id = %(SURFACE_GUID)s
  AND s.scheduled_corpus_item_scheduled_at  BETWEEN (current_date - %(MAX_AGE_DAYS)s) AND current_date 
ORDER BY s.scheduled_corpus_item_scheduled_at DESC
"""


@task()
def transform_to_candidates(records: dict) -> List[RecommendationCandidate]:
    # recs api candidate sets don't yet include topic information
    return [RecommendationCandidate(
        item_id=rec["ID"],
        publisher=rec["PUBLISHER"],
        feed_id=int(NewTabFeedID.en_US)
    ) for rec in records]


@task()
def transform_to_corpus_items(records: dict) -> List[dict]:
    # corpus candidate sets don't yet include publisher information
    return [
        {'ID': rec['ID'], 'TOPIC': rec['TOPIC']}
        for rec in records]


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:

    records = PocketSnowflakeQuery()(
        query=POCKETHITS_SQL,
        data={
            "MAX_AGE_DAYS": 9,
            "SURFACE_GUID": "POCKET_HITS_EN_US"
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # SageMaker Feature Store won't be used until we switch in RecAPI
    corpus_items = transform_to_corpus_items(records)
    corpus_items = validate_corpus_items(corpus_items)
    feature_group_record = create_corpus_candidate_set_record(
        id=POCKETHITS_EN_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

    # write to sqs
    candidates = transform_to_candidates(records)
    put_results(POCKETHITS_EN_CANDIDATE_SET_ID, candidates, curated=True)

if __name__ == "__main__":
    flow.run()
