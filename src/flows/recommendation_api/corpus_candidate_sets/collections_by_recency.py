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

RECENT_COLLECTIONS_CANDIDATE_SET_ID = "92af3dae-25c9-46c3-bf05-18082aacc7e1"

EXPORT_COLLECTIONS_CANDIDATE_SET_SQL = """
SELECT 
    approved_corpus_item_external_id as "ID", 
    topic as "TOPIC"
FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp())
AND IS_COLLECTION
AND CORPUS_REVIEW_STATUS = 'recommendation'
AND LANGUAGE = 'EN'
ORDER BY REVIEWED_CORPUS_ITEM_UPDATED_AT desc
"""

with Flow(FLOW_NAME) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_COLLECTIONS_CANDIDATE_SET_SQL,
        data={"MAX_AGE_DAYS": -60},
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # Validate the corpus item schema
    corpus_items = validate_corpus_items(corpus_items)

    feature_group_record = create_corpus_candidate_set_record(
        id=RECENT_COLLECTIONS_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
