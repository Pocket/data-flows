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

'''
Builds pocket hits candidate sets for en (and some day de) and writes it to Feature Store
'''

FLOW_NAME = get_flow_name(__file__)

POCKETHITS_EN_CANDIDATE_SET_ID = "92411893-ebdb-4a43-ad29-aa79e56e2136"

POCKETHITS_SQL = """
SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as "ID",
    TOPIC as "TOPIC",
    PUBLISHER as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
WHERE SCHEDULED_SURFACE_ID = %(SURFACE_GUID)s
-- BACK-1668: `scheduled_at` is always 12am UTC, which is unexpected because Pocket Hits goes out at 10am EST.
--            The workaround below overrides the time to be 2pm UTC = 10am EST. Can be removed when BACK-1668 is fixed.
AND DATEADD('hour', 14, DATE_TRUNC('DAY', SCHEDULED_CORPUS_ITEM_SCHEDULED_AT)) < CURRENT_TIMESTAMP
QUALIFY row_number() OVER (PARTITION BY APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC) = 1
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC
LIMIT 8  -- Only include past Pocket Hits stories if today's aren't available. There are 8 stories per email.
"""


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:

    # query snowflake for items from pocket hits
    records = PocketSnowflakeQuery()(
        query=POCKETHITS_SQL,
        data={
            "SURFACE_GUID": "POCKET_HITS_EN_US"
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    # create candidate set
    corpus_items = validate_corpus_items(records)
    feature_group_record = create_corpus_candidate_set_record(
        id=POCKETHITS_EN_CANDIDATE_SET_ID,
        corpus_items=corpus_items
    )

    # load candidate set into feature group
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
