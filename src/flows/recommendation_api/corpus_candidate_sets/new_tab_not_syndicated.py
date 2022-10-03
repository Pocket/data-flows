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

NEW_TAB_EN_US_NOT_SYNDICATED_CANDIDATE_SET_ID = "5f0dae93-a5a8-439a-a2e2-5d418c04bc98"

EXPORT_NEW_TAB_EN_US_NOT_SYNDICATED_CANDIDATE_SET_SQL = """
SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as "ID",
    TOPIC as "TOPIC"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
WHERE SCHEDULED_SURFACE_ID = %(SURFACE_GUID)s
AND NOT IS_SYNDICATED
AND NOT IS_COLLECTION
AND SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD(day, %(MAX_AGE_DAYS)s, CURRENT_DATE) AND CURRENT_DATE
QUALIFY row_number() OVER (PARTITION BY APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC) = 1
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC
"""

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_NEW_TAB_EN_US_NOT_SYNDICATED_CANDIDATE_SET_SQL,
        data={"MAX_AGE_DAYS": -3, "SURFACE_GUID": "NEW_TAB_EN_US"},
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # Validate the corpus item schema
    corpus_items = validate_corpus_items(corpus_items)

    feature_group_record = create_corpus_candidate_set_record(
        id=NEW_TAB_EN_US_NOT_SYNDICATED_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
