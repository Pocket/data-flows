from prefect import Flow, Parameter

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
Builds Coronovirus candidate set and writes it to SQS and Feature Store
'''

FLOW_NAME = get_flow_name(__file__)

CORONAVIRUS_CANDIDATE_SET_ID = 'c29fbf48-0093-49d2-ae55-4e01a3cf800e'

CORONAVIRUS_SQL = """
SELECT
    s.resolved_id,   
    c.top_domain_name as publisher,
    s.resolved_url,
    s.scheduled_corpus_item_scheduled_at,
    s.scheduled_surface_name,
    s.topic,
    s.approved_corpus_item_external_id
FROM scheduled_corpus_items as s
JOIN content as c
  ON c.content_id = s.content_id
WHERE s.TOPIC = 'CORONAVIRUS'
  AND s.scheduled_surface_id = 'NEW_TAB_EN_US'
ORDER BY s.scheduled_corpus_item_scheduled_at DESC
LIMIT 60
"""

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=CORONAVIRUS_SQL,
        data={
            'scheduled_at_start_day': -60,
            'language': 'EN',
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    #todo: blocklists?

    # SafeMaker Feature Store won't be used until we switch in RecAPI
    print("Posting Coronavirus candidates to SageMaker Feature store.")
    feature_group_record = create_corpus_candidate_set_record(
        id=CORONAVIRUS_CANDIDATE_SET_ID,
        corpus_items=corpus_items,
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

    print("Posting Coronavirus candidates to SQS.")
    candidates = [RecommendationCandidate(
        item_id=recommendation_item["resolved_id"],
        publisher=recommendation_item["publisher"],
        feed_id=NewTabFeedID.en_US
    ) for recommendation_item in corpus_items]
    put_results(CORONAVIRUS_CANDIDATE_SET_ID, candidates, curated=True)

if __name__ == "__main__":
    flow.run()
