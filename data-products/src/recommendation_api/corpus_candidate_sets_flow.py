import datetime
import json
import os
from pathlib import Path

import dask.distributed
import pandas as pd
from common.databases.snowflake_utils import CS, MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, get_run_logger, task
from prefect_dask import DaskTaskRunner
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


@task()
def prep_dataframe(candidate_set_config: CorpusCandidateSetConfig, corpus_items: list):
    record = [
        {
            "id": candidate_set_config.id,
            "unloaded_at": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "corpus_items": json.dumps(corpus_items),
        }
    ]
    df = pd.DataFrame.from_dict(record)  # type: ignore
    return df


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
            id=t["CORPUS_CANDIDATE_SET_ID"],  # type: ignore
            name=t["NAME"],  # type: ignore
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": t["CORPUS_TOPIC_ID"],  # type: ignore
                "SCHEDULED_SURFACE_ID": t["SCHEDULED_SURFACE_ID"],  # type: ignore
            },
        )
        for t in topic_candidate_set_config_data
    ]

    return static_candidate_set_configs + topic_candidate_set_configs


@flow()
async def load_corpus_candidate_set_records(
    candidate_set_config: CorpusCandidateSetConfig,
    snowflake_connector: MozSnowflakeConnector,
    dask_client: dask.distributed.Client,
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

    validate = validate_corpus_items(
        corpus_items,  # type: ignore
        min_item_count=0 if "coronavirus" in candidate_set_config.name else 1,
    )

    df_prep = prep_dataframe(
        candidate_set_config, corpus_items=corpus_items, wait_for=[validate]  # type: ignore  # noqa: E501
    )

    environment_map = {"dev": "development", "production": "production"}

    feature_group = f"{environment_map[CS.dev_or_production]}-corpus-candidate-sets-v1"

    await dataframe_to_feature_group.with_options(
        task_runner=DaskTaskRunner(address=dask_client.scheduler.address)  # type: ignore
    )(dataframe=df_prep, feature_group_name=feature_group)


@flow()
async def corpus_candidate_sets():
    sfc = MozSnowflakeConnector()

    configs = await create_all_candidate_set_configs(
        static_candidate_set_configs=static_candidate_set_configs,
        snowflake_connector=sfc,
    )

    client = dask.distributed.Client()

    load_flows = [
        load_corpus_candidate_set_records(
            candidate_set_config=c, snowflake_connector=sfc, dask_client=client
        )
        for c in configs
    ]

    await process_parallel_subflows_task(load_flows)


FLOW_SPEC = FlowSpec(
    flow=corpus_candidate_sets,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="deployment",
            tags=["hourly-sla"],
            cron="*/60 * * * *",
            job_variables={
                "cpu": 4096,
                "memory": 8192,
            },
        ),
    ],
)

if __name__ == "__main__":
    import asyncio

    asyncio.run(corpus_candidate_sets())
