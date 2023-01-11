import os
from pathlib import Path

from prefect import Flow, task, unmapped
from prefect.executors import LocalDaskExecutor

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from models.corpus_candidate_set_configs import corpus_candidate_set_configs, CorpusCandidateSetConfig
from utils import config
from utils.config import SQL_DIR
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)


@task()
def get_corpus_candidate_set_record(candidate_set_config: CorpusCandidateSetConfig):
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
    records = get_corpus_candidate_set_record.map(corpus_candidate_set_configs)

    load_feature_record.map(records, feature_group_name=unmapped(feature_group))

if __name__ == "__main__":
    flow.run()
