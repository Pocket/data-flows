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
FLOW_NAME = get_flow_name(__file__)

CURATED_LIFEHACKS_CANDIDATE_SET_ID = "da9cb7a1-3a34-4211-b918-73819a5586c8"

# Export approved corpus items by language and recency
EXPORT_LIFEHACKS_ITEMS_SQL = """
SELECT 
    approved_corpus_item_external_id as "ID", 
    topic as "TOPIC"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp())
AND TOPIC IN (%(CORPUS_TOPIC_LIST)s)
AND CORPUS_REVIEW_STATUS = 'recommendation'
AND LANGUAGE = 'EN'
ORDER BY REVIEWED_CORPUS_ITEM_UPDATED_AT desc
"""

@task()
def transform_to_corpus_items(records: dict) -> List[dict]:
    # corpus candidate sets don't yet include publisher information
    return [
        {'ID': rec['ID'], 'TOPIC': rec['TOPIC']}
        for rec in records]


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:

    # query snowflake for items from lifehacks topic set
    records = PocketSnowflakeQuery()(
        query=EXPORT_LIFEHACKS_ITEMS_SQL,
        data={
            "MAX_AGE_DAYS": -30,
            "CORPUS_TOPIC_LIST": ['SELF_IMPROVEMENT','CAREER','HEALTH_FITNESS','PERSONAL_FINANCE']
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # create candidate set
    corpus_items = transform_to_corpus_items(records)
    corpus_items = validate_corpus_items(corpus_items)
    feature_group_record = create_corpus_candidate_set_record(
        id=CURATED_LIFEHACKS_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    # load candidate set into feature group
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
