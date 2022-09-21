from prefect import Flow, Parameter

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

SETUP_MOMENT_CORPUS_CANDIDATE_SET_ID = 'deea0f06-9dc9-44a5-b864-fea4a4d0beb7'

# Export approved corpus items by language and recency
EXPORT_CORPUS_ITEMS_SQL = """
SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as ID,
    TOPIC
FROM "SCHEDULED_CORPUS_ITEMS"
WHERE LANGUAGE = %(language)s
AND IS_SYNDICATED = TRUE
AND SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD(day, %(scheduled_at_start_day)s, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP
QUALIFY row_number() OVER (PARTITION BY APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC) = 1
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC
LIMIT 500;
"""

with Flow(FLOW_NAME) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_CORPUS_ITEMS_SQL,
        data={
            'scheduled_at_start_day': -60,
            'language': 'EN',
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    corpus_items = validate_corpus_items(corpus_items)

    feature_group_record = create_corpus_candidate_set_record(
        id=SETUP_MOMENT_CORPUS_CANDIDATE_SET_ID,
        corpus_items=corpus_items,
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
