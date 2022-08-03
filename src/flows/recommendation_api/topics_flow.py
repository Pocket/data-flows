from prefect import Flow, Parameter, unmapped, task
from prefect.executors import LocalDaskExecutor
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

# Export approved corpus items by language and recency
EXPORT_CORPUS_ITEMS_SQL = """
SELECT 
    approved_corpus_item_external_id as "ID", 
    topic as "TOPIC"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD('day', -90, current_timestamp())
AND TOPIC = %(CORPUS_TOPIC_ID)s
AND LANGUAGE = 'EN'
"""

GET_TOPICS_SQL = """
SELECT 
    curated_corpus_candidate_set_id as "CURATED_CORPUS_CANDIDATE_SET_ID", 
    corpus_topic_id as "CORPUS_TOPIC_ID"
FROM analytics.dbt.static_corpus_candidate_set_topics
"""

@task()
def get_candidate_set_ids(topics):
    return [i['CURATED_CORPUS_CANDIDATE_SET_ID'] for i in topics]

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30), executor=LocalDaskExecutor()) as flow:
    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type = OutputType.DICT
    )

    topics = query(query=GET_TOPICS_SQL)

    candidate_set_ids = get_candidate_set_ids(topics)

    topic_corpus_items = query.map(data=topics, query=unmapped(EXPORT_CORPUS_ITEMS_SQL))

    valid_topic_corpus_items = validate_corpus_items.map(topic_corpus_items)

    feature_group_record = create_corpus_candidate_set_record.map(
        id=candidate_set_ids,
        corpus_items=valid_topic_corpus_items,
    )
    load_feature_record.map(feature_group_record, feature_group_name=unmapped(feature_group))

if __name__ == "__main__":
    flow.run()
