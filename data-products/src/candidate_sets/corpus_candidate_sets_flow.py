import os
from pathlib import Path

import pandas as pd
from common.databases.snowflake_utils import CS, MozSnowflakeConnector
from prefect import flow, get_run_logger

# from prefect_dask.task_runners import DaskTaskRunner
from prefect_snowflake.database import snowflake_query
from shared.async_utils import process_parallel_subflows_task
from shared.feature_store import dataframe_to_feature_group
from shared.models.corpus_candidate_set_configs import (
    CorpusCandidateSetConfig,
    static_candidate_set_configs,
)
from shared.tasks import validate_corpus_items
from snowflake.connector import DictCursor

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


@flow()
async def create_all_candidate_set_configs(
    static_candidate_set_configs: list[CorpusCandidateSetConfig],
    snowflake_connector: MozSnowflakeConnector,
):
    topic_candidate_set_config_data = await snowflake_query(
        query=GET_TOPICS_SQL,
        snowflake_connector=snowflake_connector,
        cursor_type=DictCursor,  # type: ignore
    )

    topic_candidate_set_configs = [
        CorpusCandidateSetConfig(
            id=t["CORPUS_CANDIDATE_SET_ID"],
            name=t["NAME"],
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": t["CORPUS_TOPIC_ID"],
                "SCHEDULED_SURFACE_ID": t["SCHEDULED_SURFACE_ID"],
            },
        )
        for t in topic_candidate_set_config_data
    ]

    return static_candidate_set_configs + topic_candidate_set_configs


@flow()
async def load_corpus_candidate_set_records(
    candidate_set_config: CorpusCandidateSetConfig,
    snowflake_connector: MozSnowflakeConnector,
):
    logger = get_run_logger()
    logger.info(f"Querying candidate set {candidate_set_config}")

    # Read a query from the sql/ directory located next to this flow file.
    sql_path = os.path.join(
        os.path.dirname(__file__), "sql/", candidate_set_config.query_filename
    )
    sql_query = Path(sql_path).read_text()

    corpus_items = await snowflake_query(
        query=sql_query,
        snowflake_connector=snowflake_connector,
        params=candidate_set_config.query_params,  # type: ignore
        cursor_type=DictCursor,  # type: ignore
    )

    validate_corpus_items(
        corpus_items,  # type: ignore
        min_item_count=0 if "coronavirus" in candidate_set_config.name else 1,
    )

    df = pd.DataFrame.from_dict(corpus_items)  # type: ignore

    environment_map = {"dev": "development", "production": "production"}

    feature_group = f"{environment_map[CS.dev_or_production]}-corpus-candidate-sets-v1"

    await dataframe_to_feature_group(dataframe=df, feature_group_name=feature_group)


@flow()
async def corpus_candidate_sets():
    sfc = MozSnowflakeConnector()

    configs = await create_all_candidate_set_configs(
        static_candidate_set_configs=static_candidate_set_configs,
        snowflake_connector=sfc,
    )

    load_flows = [
        load_corpus_candidate_set_records(
            candidate_set_config=c, snowflake_connector=sfc
        )
        for c in configs
    ]

    await process_parallel_subflows_task(load_flows)


if __name__ == "__main__":
    import asyncio

    asyncio.run(corpus_candidate_sets())
