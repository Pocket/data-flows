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
WITH recent_collections AS (
  SELECT
    IFNULL(s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT, a.REVIEWED_CORPUS_ITEM_CREATED_AT) as recency,
    s.SCHEDULED_SURFACE_IANA_TIMEZONE,
    a.*
  FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" a
  LEFT JOIN "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" s ON (
    s.approved_corpus_item_external_id = a.approved_corpus_item_external_id
    AND s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < current_timestamp()
    AND s.SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
  )
  WHERE a.IS_COLLECTION
  AND a.CORPUS_REVIEW_STATUS = 'recommendation'
  AND a.LANGUAGE = 'EN'
  AND recency > DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp())
  QUALIFY row_number() OVER (PARTITION BY a.APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY recency DESC) = 1
  ORDER BY recency DESC
)

SELECT
    approved_corpus_item_external_id as "ID", 
    topic as "TOPIC"
FROM recent_collections
ORDER BY recency DESC
"""

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
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
