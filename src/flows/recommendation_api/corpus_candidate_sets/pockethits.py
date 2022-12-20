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
-- Pocket Hits content should be available on Home at 3am EST = 7am UTC. Override the scheduled time to 7am.
-- `DATE_TRUNC` truncates the time, and `DATEADD` adds 7 hours to set the time to 7am UTC. This corresponds to 3am EST.
AND DATEADD('hour', 7, DATE_TRUNC('DAY', SCHEDULED_CORPUS_ITEM_SCHEDULED_AT)) < CURRENT_TIMESTAMP
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
