from typing import Dict, Sequence, List

from prefect import Flow, Parameter, unmapped, task
from prefect.executors import LocalDaskExecutor
from utils.flow import get_flow_name, get_interval_schedule
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.athena import AthenaQuery
from sagemaker.feature_store.inputs import FeatureValue

from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)

FLOW_NAME = get_flow_name(__file__)

ATHENA_SQL = """
SELECT *
FROM "sagemaker_featurestore"."development-user-recommendation-preferences-v1-1654826050" 
limit 10
"""

HASHED_USER_ID_QUERY = """
SELECT user_id, hashed_user_id
FROM "foobar" -- TODO
WHERE user_id = %(user_id)s
"""


V2_FEATURE_GROUP_NAME = 'development-user-recommendation-preferences-v2'

MOCK_V1_RECORDS = [{
    'user_id': '1234',
    'updated_at': '2022-08-08T23:05:55Z',
    'preferred_topics': '[{"id": "1bf756c0-632f-49e8-9cce-324f38f4cc71"}, {"id": "45f8e740-42e0-4f54-8363-21310a084f1f"}]'
}]

LIST_USER_IDS = [
    {'user_id': "1234", 'hashed_user_id': "1bf756c0-632f-49e8-9cce-324f38f4cc71"}
]

# Maps integer to hashed user id
HAHSED_USER_IDS = {"1234": "1bf756c0-632f-49e8-9cce-324f38f4cc71"}

MOCK_V2_FEATURE_GROUP_RECORDS = [
    [
        FeatureValue('hashed_user_id', '123456-632f-49e8-9cce-324f38f4cc71'),
        FeatureValue('updated_at', '2022-08-08T23:05:55Z'),
        FeatureValue('preferred_topics', '[{"id": "1bf756c0-632f-49e8-9cce-324f38f4cc71"}, {"id": "45f8e740-42e0-4f54-8363-21310a084f1f"}]'), # TODO: Check feature name
    ]
]


@task
def print_results(param_name, param_value):
    print(f'{param_name}: {param_value}')


@task
def get_query_params(records):
    return [{'user_id': record[0]} for record in records]

@task
def get_user_id_mapping(rows: List[Dict]) -> Dict[int, str]:
    result = {}
    for row in rows:
        result[row['user_id']] = row['hashed_user_id']
    return result


@task()
def create_v2_feature_group_record(
        v1_record: Dict,
        hashed_user_id_map: Dict
) -> Sequence[FeatureValue]:
    hashed_user_id = hashed_user_id_map[v1_record['user_id']]

    return [
        FeatureValue('hashed_user_id', hashed_user_id),
        FeatureValue('updated_at', v1_record['updated_at']),
        FeatureValue('preferred_topics', v1_record['preferred_topics']),
    ]


with Flow(FLOW_NAME) as flow:

    # user_topics_prefs = AthenaQuery(
    #     query=HASHED_USER_ID_QUERY,
    # )
    #
    # query_params = get_query_params(user_topics_prefs)
    #
    # hashed_user_ids = PocketSnowflakeQuery.map(
    #     query=unmapped(HASHED_USER_ID_QUERY),
    #     data=query_params
    # )
    #

    hashed_user_ids = get_user_id_mapping(LIST_USER_IDS)

    v2_feature_group_records = create_v2_feature_group_record.map(
        v1_record=MOCK_V1_RECORDS,
        hashed_user_id_map=unmapped(hashed_user_ids),
    )
    load_feature_record.map(v2_feature_group_records, feature_group_name=unmapped(V2_FEATURE_GROUP_NAME))


if __name__ == "__main__":
    flow.run()
