from prefect import Flow

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

RECENT_RECOMMENDED_CANDIDATE_SET_ID = "2066c835-a940-45ec-b1f7-267457d9e0a2"


EXPORT_RECOMMENDED_CANDIDATE_SET_SQL = """
WITH recently_updated_items as (
    SELECT 
        approved_corpus_item_external_id as "ID", 
        topic as "TOPIC",
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
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < current_timestamp()
    AND CORPUS_ITEM_LOADED_FROM = 'MANUAL'  -- should this be removed?
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_syndicated
    AND NOT is_collection
),

all_recent_items as (
    SELECT * FROM recently_scheduled_items
    UNION ALL 
    SELECT * FROM recently_updated_items
),

-- Deduplicate based on the CorpusItem id
deduped as (
  SELECT * FROM all_recent_items
  QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
)

-- Select the n most recent items per topic
SELECT
    ID,
    TOPIC
FROM deduped
QUALIFY row_number() OVER (PARTITION BY TOPIC ORDER BY REVIEW_TIME DESC) <= %(N_RECS_PER_TOPIC)s;
"""

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_RECOMMENDED_CANDIDATE_SET_SQL,
        data={"N_RECS_PER_TOPIC": 6},
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # Validate the corpus item schema
    corpus_items = validate_corpus_items(corpus_items)

    feature_group_record = create_corpus_candidate_set_record(
        id=RECENT_RECOMMENDED_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
