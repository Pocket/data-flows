from prefect import Flow, context, task, Parameter
from prefect.executors import LocalDaskExecutor
import datetime
import re
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

from api_clients.pocket_snowflake_query import OutputType, PocketSnowflakeQuery
from common_tasks.braze import mask_email_domain_outside_production
from common_tasks.mapping import split_in_chunks
from utils import config
from utils.flow import get_s3_result, get_flow_name
from api_clients.braze import models
from api_clients.braze.client import (
    BrazeClient,
    IDENTIFY_USER_ALIAS_LIMIT
)
from api_clients.braze.pocket_config import EMAIL_ALIAS_LABEL

from utils.iteration import chunks

FLOW_NAME = get_flow_name(__file__)

DEFAULT_MAX_OPERATIONS_PER_TASK_RUN = 100000  # The workload is run in parallel in chunks of this many rows.
DEFAULT_TABLE_NAME = 'BRAZE_USERS'


@task()
def trigger_segment_export(segment_id: str):
    logger = context.get("logger")
    response = BrazeClient(logger=logger).users_by_segment(models.UsersBySegmentInput(
        segment_id=segment_id,
        # TODO: Do we need any other braze user data?
        fields_to_export=['external_id', 'user_aliases', 'braze_id', 'email']
    ))
    json = response.json()
    object_prefix = json['object_prefix']
    # the path that the data is dumped into s3
    # typically bucket/segment-export/{segment_id}/{date}/{object_prefix}
    # data is dumped in Gziped files, in json format.
    return object_prefix


@task()
def import_to_snowflake():
    logger = context.get("logger")


with Flow(FLOW_NAME, result=get_s3_result()) as flow:
    # To backfill data we can manually run this flow and override the Snowflake database, schema, and table.
    # snowflake_database = Parameter('snowflake_database', default=config.SNOWFLAKE_ANALYTICS_DATABASE)
    # snowflake_schema = Parameter('snowflake_schema', default=config.SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA)
    # import_query_table_name = Parameter('snowflake_table_name', default=DEFAULT_TABLE_NAME)
    # This parameter controls the number of rows processed by each task run. Higher number = less parallelism.
    # max_operations_per_task_run = Parameter('max_operations_per_task_run', default=DEFAULT_MAX_OPERATIONS_PER_TASK_RUN)

    segment_export_task = trigger_segment_export(segment_id='')
    import_to_snowflake().set_upstream([segment_export_task])

if __name__ == "__main__":
    flow.run()
