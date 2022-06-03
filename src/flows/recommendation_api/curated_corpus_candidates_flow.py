from typing import Dict, Sequence
import json
import datetime

from prefect import task, Flow, Parameter, unmapped
from prefect.tasks.aws.s3 import S3List
import pandas as pd
import boto3
from sagemaker.feature_store.feature_group import FeatureGroup, FeatureValue
from sagemaker.session import Session

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

NEW_TAB_EN_US_CORPUS_CANDIDATE_SET_ID = 'deea0f06-9dc9-44a5-b864-fea4a4d0beb7'

# Export approved corpus items by language and recency
EXPORT_CORPUS_ITEMS_SQL = """
SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as ID,
    TOPIC
FROM "APPROVED_CORPUS_ITEMS"
WHERE LANGUAGE = %(language)s
AND REVIEWED_CORPUS_ITEM_CREATED_AT BETWEEN DATEADD(day, %(scheduled_at_start_day)s, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP
ORDER BY REVIEWED_CORPUS_ITEM_CREATED_AT DESC
LIMIT 500;
"""


@task()
def create_corpus_candidate_set_record(
        id: str,
        corpus_items: Dict,
        unloaded_at: datetime.datetime = datetime.datetime.now()
) -> Sequence[FeatureValue]:
    return [
        FeatureValue('id', id),
        FeatureValue('unloaded_at', unloaded_at.strftime("%Y-%m-%dT%H:%M:%SZ")),
        FeatureValue('corpus_items', json.dumps(corpus_items)),
    ]


@task()
def load_feature_record(record: Sequence[FeatureValue], feature_group_name):
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.put_record(record)


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_CORPUS_ITEMS_SQL,
        data={
            'scheduled_at_start_day': -14,
            'language': 'EN',
        },
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    feature_group = Parameter("feature group", default=f"{config.ENVIRONMENT}-corpus-candidate-sets-v1")
    feature_group_record = create_corpus_candidate_set_record(
        id=NEW_TAB_EN_US_CORPUS_CANDIDATE_SET_ID,
        corpus_items=corpus_items,
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
