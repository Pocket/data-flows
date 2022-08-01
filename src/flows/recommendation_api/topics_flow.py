from prefect import Flow, Parameter, unmapped, task

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

CORPUS_CANDIDATE_SET_ID = 'a765b8ab-9631-4046-9d2e-8706324efc96'

# Export approved corpus items by language and recency
EXPORT_CORPUS_ITEMS_SQL = """
SELECT *
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
WHERE APPROVED_AT >= DATE_ADD('day', -90, CURRENT_DATE())
AND TOPIC = %(topic)s
AND LANGUAGE = 'en'
"""

GET_TOPICS_SQL = """
SELECT id, topic 
FROM "ANALYTICS"."DBT"."CORPUS_CANDIDATE_SETS"
"""

@task()
def get_topic_ids(topics):
    return [i['corpus_candidate_set_id'] for i in topics]


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:
    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
    )

    topics = query(query=GET_TOPICS_SQL, output_type=OutputType.DICT)
    topic_ids = get_topic_ids(topics)

    topic_corpus_items = query.map(data=topics, query=unmapped(EXPORT_CORPUS_ITEMS_SQL))

    topic_corpus_items = validate_corpus_items.map(topic_corpus_items)

    feature_group_record = create_corpus_candidate_set_record.map(
        id=topic_ids,
        corpus_items=topic_corpus_items,
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()


