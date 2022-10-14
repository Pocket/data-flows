from prefect import Flow, context, task, Parameter
import datetime
import re
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

from api_clients.pocket_snowflake_query import OutputType, PocketSnowflakeQuery
from common_tasks.mapping import split_in_chunks
from utils import config
from utils.flow import get_s3_result, get_flow_name, get_cron_schedule
from api_clients.braze import models
from api_clients.braze.client import (
    BrazeClient,
    IDENTIFY_USER_ALIAS_LIMIT
)

from utils.iteration import chunks

FLOW_NAME = get_flow_name(__file__)

EXTRACT_QUERY = """
SELECT
    BRAZE_ID
FROM "{table_name}"
WHERE EXTERNAL_ID is null 
    AND user_aliases is null 
ORDER BY EXTERNAL_ID ASC
"""

DEFAULT_MAX_OPERATIONS_PER_TASK_RUN = 100000  # The workload is run in parallel in chunks of this many rows.
DEFAULT_TABLE_NAME = 'USER'


@dataclass
class UserToDelete:
    """
    This class corresponds to the query result from EXTRACT_QUERY.
    """

    """Unique Braze user identifier (based on the Pocket hashed user id)"""
    braze_id: str

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        # Snowflake returns uppercase column names, and we use lowercase variables in Python.
        return UserToDelete(**{k.lower(): v for k, v in d.items()})


@task()
def get_users_to_delete_from_dicts(dicts: List[Dict]) -> List[UserToDelete]:
    """
    Converts a dicts to UserToDelete objects
    :param dicts: Dictionary where keys match attributes in UserToDelete (case-insensitive)
    :return: List of UserToDelete objects
    """
    return [UserToDelete.from_dict(d) for d in dicts]


@task()
def prepare_extract_query_and_parameters(table_name: str) -> Tuple[str, Dict]:
    """
    :return: Tuple where the first element is the query string, and the second the query parameters
    """
    # Table name can only contain alphanumeric characters and underscores to prevent SQL-injection.
    assert re.fullmatch(r'[a-zA-Z0-9_]+', table_name), "Invalid table name"
    query = EXTRACT_QUERY.format(table_name=table_name)

    query_params = {}

    logger = context.get("logger")
    logger.info(f"extract query_params: {query_params}")

    return query, query_params


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def delete_users(users_to_delete: List[UserToDelete]):
    """
    Deletes a set of users who we do not have enough information on (ie. a pocket user id OR an email alias)

    Use cases:
    -
    """
    logger = context.get("logger")

    for chunk in chunks(users_to_delete, IDENTIFY_USER_ALIAS_LIMIT):
        logger.info(f"Deleting {len(chunk)} user profiles with braze_ids=[{[c.braze_id for c in chunk]}]")

        BrazeClient(logger=logger).delete_users(models.UserDeleteInput(
            braze_ids=[
               user.braze_id for user in chunk
            ]
        ))


with Flow(FLOW_NAME, result=get_s3_result()) as flow:
    # To backfill data we can manually run this flow and override the Snowflake database, schema, and table.
    snowflake_database = Parameter('snowflake_database', default=config.SNOWFLAKE_FIVETRAN_DATABASE)
    snowflake_schema = Parameter('snowflake_schema', default=config.SNOWFLAKE_FIVETRAN_BRAZE_SCHEMA)
    extract_query_table_name = Parameter('snowflake_table_name', default=DEFAULT_TABLE_NAME)
    # This parameter controls the number of rows processed by each task run. Higher number = less parallelism.
    max_operations_per_task_run = Parameter('max_operations_per_task_run', default=DEFAULT_MAX_OPERATIONS_PER_TASK_RUN)

    extract_query, extract_query_params = prepare_extract_query_and_parameters(table_name=extract_query_table_name)

    users_to_delete_dicts = PocketSnowflakeQuery()(
        query=extract_query,
        data=extract_query_params,
        database=snowflake_database,
        schema=snowflake_schema,
        output_type=OutputType.DICT,
    )

    # Convert Snowflake dicts to @dataclass objects.
    all_users_to_delete = get_users_to_delete_from_dicts(users_to_delete_dicts)

    # Delete braze users that have no external id or email alias set.
    delete_users_task = delete_users.map(
        split_in_chunks(
            all_users_to_delete,
            chunk_size=max_operations_per_task_run,
        ),
    )

if __name__ == "__main__":
    flow.run()
