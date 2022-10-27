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
WITH recently_updated_items as (
    SELECT 
        approved_corpus_item_external_id as "ID", 
        topic as "TOPIC",
        publisher as "PUBLISHER",
        reviewed_corpus_item_updated_at as "REVIEW_TIME" 
    FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" 
    WHERE CORPUS_REVIEW_STATUS = 'recommendation'
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_syndicated
    AND NOT is_collection
),

recently_scheduled_items as (    
    SELECT 
        approved_corpus_item_external_id as "ID", 
        topic as "TOPIC",
        publisher as "PUBLISHER",
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < current_timestamp()
    AND CORPUS_ITEM_LOADED_FROM = 'MANUAL'  -- should this be removed?
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_syndicated
    AND NOT is_collection
)

SELECT 
    "ID",
    "TOPIC",
    "PUBLISHER" 
FROM 
    (SELECT * FROM recently_scheduled_items
     UNION ALL
     SELECT * FROM recently_updated_items)
WHERE TOPIC IN (%(CORPUS_TOPIC_LIST)s)
AND REVIEW_TIME >= current_date() - %(MAX_AGE_DAYS)s
QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
ORDER BY REVIEW_TIME DESC
"""

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:

    # query snowflake for items from lifehacks topic set
    records = PocketSnowflakeQuery()(
        query=EXPORT_LIFEHACKS_ITEMS_SQL,
        data={
            "MAX_AGE_DAYS": 30,
            "CORPUS_TOPIC_LIST": ['SELF_IMPROVEMENT','CAREER','HEALTH_FITNESS','PERSONAL_FINANCE']
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # create candidate set
    corpus_items = validate_corpus_items(records)
    feature_group_record = create_corpus_candidate_set_record(
        id=CURATED_LIFEHACKS_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    # load candidate set into feature group
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
