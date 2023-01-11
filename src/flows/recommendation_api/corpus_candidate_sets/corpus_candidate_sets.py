import os
from pathlib import Path
from typing import List

import prefect
from prefect import Flow, task, unmapped, flatten
from prefect.executors import LocalDaskExecutor
from prefect.tasks.core.operators import Add
from sagemaker.feature_store.inputs import FeatureValue

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from models.corpus_candidate_set_configs import static_candidate_set_configs, CorpusCandidateSetConfig
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

GET_TOPICS_SQL = """
SELECT 
    curated_corpus_candidate_set_id as "CORPUS_CANDIDATE_SET_ID", 
    concat('en_us/', LOWER(corpus_topic_id)) as "NAME",
    corpus_topic_id as "CORPUS_TOPIC_ID",
    'NEW_TAB_EN_US' as "SCHEDULED_SURFACE_ID"
FROM analytics.dbt.static_corpus_candidate_set_topics

UNION ALL

SELECT
    german_curated_corpus_candidate_set_id as "CORPUS_CANDIDATE_SET_ID",
    concat('de_de/', LOWER(corpus_topic_id)) as "NAME",
    corpus_topic_id as "CORPUS_TOPIC_ID",
    'NEW_TAB_DE_DE' as "SCHEDULED_SURFACE_ID"
FROM analytics.dbt.static_corpus_candidate_set_topics
"""


@task()
def topic_candidate_set_configs() -> List[CorpusCandidateSetConfig]:
    """
    :return: Config objects to generate Topic candidate sets for en-US and de-DE.
    """
    topics = PocketSnowflakeQuery().run(
        query=GET_TOPICS_SQL,
        output_type=OutputType.DICT,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
    )

    return [
        CorpusCandidateSetConfig(
            id=t['CORPUS_CANDIDATE_SET_ID'],
            name=t['NAME'],
            query_filename='topic.sql',
            query_params={
                'CORPUS_TOPIC_ID': t['CORPUS_TOPIC_ID'],
                'SCHEDULED_SURFACE_ID': t['SCHEDULED_SURFACE_ID']
            },
        ) for t in topics
    ]


@task()
def query_corpus_candidate_set(candidate_set_config: CorpusCandidateSetConfig) -> List[FeatureValue]:
    """
    Queries a candidate set and formats it as a Feature Group record.
    :param candidate_set_config: Configuration object specifying the candidate set id, query, and query params.
    :return: corpus-candidate-sets Feature Group record, where `corpus_items` is a JSON-encoded list of corpus items.
    """
    logger = prefect.context.get("logger")
    logger.info(f'Querying candidate set {candidate_set_config}')

    # Read a query from the sql/ directory located next to this flow file.
    sql_path = os.path.join(os.path.dirname(__file__), 'sql/', candidate_set_config.query_filename)
    sql_query = Path(sql_path).read_text()

    corpus_items = PocketSnowflakeQuery().run(
        query=sql_query,
        data=candidate_set_config.query_params,
        output_type=OutputType.DICT,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
    )

    validate_corpus_items.run(corpus_items)

    return create_corpus_candidate_set_record.run(
        id=candidate_set_config.id,
        corpus_items=corpus_items
    )


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60), executor=LocalDaskExecutor()) as flow:
    # Combine candidate set configs defined in this repo with topic configs generated from a Snowflake query.
    all_candidate_set_configs = Add()(
        static_candidate_set_configs,
        topic_candidate_set_configs()
    )

    records = query_corpus_candidate_set.map(all_candidate_set_configs)

    load_feature_record.map(records, feature_group_name=unmapped(feature_group))

if __name__ == "__main__":
    flow.run()
