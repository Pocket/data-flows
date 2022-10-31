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
WITH prep as (
    SELECT 
        approved_corpus_item_external_id as "ID", 
        topic as "TOPIC",
        publisher as "PUBLISHER",
        reviewed_corpus_item_updated_at as "REVIEW_TIME" 
    FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" 
    WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD('day', -90, current_timestamp())
    AND CORPUS_REVIEW_STATUS = 'recommendation'
    AND TOPIC = %(CORPUS_TOPIC_ID)s
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_collection
    
    UNION
    
    SELECT 
        approved_corpus_item_external_id as "ID", 
        topic as "TOPIC",
        publisher as "PUBLISHER",
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD('day', -90, current_timestamp()) AND current_timestamp()
    AND CORPUS_ITEM_LOADED_FROM = 'MANUAL'
    AND TOPIC = %(CORPUS_TOPIC_ID)s
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_collection
    
    )

SELECT
    ID,
    TOPIC,
    PUBLISHER
FROM PREP
QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
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
