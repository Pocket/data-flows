from typing import Dict, Sequence, List
from prefect import Flow, Parameter, unmapped, task
from prefect.executors import LocalDaskExecutor
from utils.flow import get_flow_name, get_interval_schedule
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from api_clients.athena import AthenaQuery
from sagemaker.feature_store.inputs import FeatureValue
from utils import config
from common_tasks.corpus_candidate_set import load_feature_record

FLOW_NAME = get_flow_name(__file__)

ATHENA_SQL = """
SELECT 
    user_id, 
    updated_at, 
    preferred_topics
FROM "sagemaker_featurestore"."development-user-recommendation-preferences-v1-1654826050" 
limit 10
"""

HASHED_USER_ID_MAP_QUERY = """
SELECT 
    user_id as "USER_ID", 
    hashed_user_id as "HASHED_USER_ID"
FROM "ANALYTICS"."DBT"."USERS"
WHERE user_id = %(user_id)s
"""

V2_FEATURE_GROUP_NAME = 'development-user-recommendation-preferences-v2'

@task()
def get_user_topics_pref_rows(df):
    return df.to_dict('records')

@task()
def cleanup_v2_user_topics_prefs(v2_record: List[Dict]) -> Sequence[FeatureValue]:
    return list(filter(lambda x: x != None, v2_record))

@task()
def build_v2_user_topics_prefs(
        v1_record: Dict
) -> Sequence[FeatureValue]:

    query = PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type = OutputType.DICT
    )
    hashed_user_id_map = query.run(
        query=HASHED_USER_ID_MAP_QUERY,
        data=v1_record,
    )

    return [
        FeatureValue('hashed_user_id', hashed_user_id_map[0]['HASHED_USER_ID']),
        FeatureValue('updated_at', v1_record['updated_at']),
        FeatureValue('preferred_topics', v1_record['preferred_topics']),
    ] if len(hashed_user_id_map) else None

with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:

    # Extracts user preferences from the v1 (integer user_id based) Feature group
    # using Athena query into Pandas DataFrame
    user_topics_prefs = AthenaQuery(
        query=ATHENA_SQL,
    )

    # Extracts user preferences from the v1 (integer user_id based) Feature group using Athena query
    user_topics_pref_rows = get_user_topics_pref_rows(user_topics_prefs)

    # Prepare v2 user preferences using user_id to hash_user_id maps from Snowflake DB
    v2_user_topics_prefs = build_v2_user_topics_prefs.map(v1_record=user_topics_pref_rows)

    # Remove data with missing hash_user_id maps
    v2_user_topics_prefs_clean = cleanup_v2_user_topics_prefs(v2_user_topics_prefs)

    # Load user preferences to v2 Feature group
    load_feature_record.map(v2_user_topics_prefs_clean, feature_group_name=unmapped(V2_FEATURE_GROUP_NAME))

if __name__ == "__main__":
    flow.run()
