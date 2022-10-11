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
from utils.flow import get_s3_result, get_flow_name, get_interval_schedule, get_cron_schedule
from api_clients.braze import models
from api_clients.braze.client import (
    BrazeClient,
    IDENTIFY_USER_ALIAS_LIMIT
)
from api_clients.braze.pocket_config import EMAIL_ALIAS_LABEL

from utils.iteration import chunks

FLOW_NAME = get_flow_name(__file__)

EXTRACT_QUERY = """
SELECT
    BRAZE_ID,
    EXTERNAL_ID,
    EMAIL,
FROM "{table_name}"
ORDER BY EXTERNAL_ID ASC
"""
# TODO: ^ Add a where filter to this query to grab those with an external_id and no user alias.

DEFAULT_MAX_OPERATIONS_PER_TASK_RUN = 100000  # The workload is run in parallel in chunks of this many rows.
DEFAULT_TABLE_NAME = 'BRAZE_USERS'


@dataclass
class UserToIdentify:
    """
    This class corresponds to the query result from EXTRACT_QUERY.
    """

    """Unique Braze user identifier (based on the Pocket hashed user id)"""
    external_id: str
    email: str

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        # Snowflake returns uppercase column names, and we use lowercase variables in Python.
        return UserToIdentify(**{k.lower(): v for k, v in d.items()})


@task()
def get_users_to_identify_from_dicts(dicts: List[Dict]) -> List[UserToIdentify]:
    """
    Converts a dicts to UserDelta objects
    :param dicts: Dictionary where keys match attributes in UserDelta (case-insensitive)
    :return: List of UserDelta objects
    """
    return [UserToIdentify.from_dict(d) for d in dicts]


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
def identify_users(user_deltas: List[UserToIdentify]):
    """
    Identifies a previously created alias-only user with an external id.

    Use cases:
    - If signs up on the Pocket Hits signup page, and then creates a Pocket account, these profiles should be linked.
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, IDENTIFY_USER_ALIAS_LIMIT):
        logger.info(f"Identifying {len(chunk)} user profiles with external_ids=[{[c.external_id for c in chunk]}]")

        BrazeClient(logger=logger).identify_users(models.IdentifyUsersInput(
            aliases_to_identify=[
                models.UserAliasIdentifier(
                    external_id=user.external_id,
                    user_alias=models.UserAlias(
                        alias_label=EMAIL_ALIAS_LABEL,
                        alias_name=user.email,
                    ),
                ) for user in chunk
            ]
        ))


# TODO: Should this only run after a braze data export is successful???
with Flow(FLOW_NAME, schedule=get_cron_schedule(cron='0 1 * * *'), result=get_s3_result()) as flow:
    # To backfill data we can manually run this flow and override the Snowflake database, schema, and table.
    snowflake_database = Parameter('snowflake_database', default=config.SNOWFLAKE_ANALYTICS_DATABASE)
    snowflake_schema = Parameter('snowflake_schema', default=config.SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA)
    extract_query_table_name = Parameter('snowflake_table_name', default=DEFAULT_TABLE_NAME)
    # This parameter controls the number of rows processed by each task run. Higher number = less parallelism.
    max_operations_per_task_run = Parameter('max_operations_per_task_run', default=DEFAULT_MAX_OPERATIONS_PER_TASK_RUN)

    extract_query, extract_query_params = prepare_extract_query_and_parameters(table_name=extract_query_table_name)

    user_deltas_dicts = PocketSnowflakeQuery()(
        query=extract_query,
        data=extract_query_params,
        database=snowflake_database,
        schema=snowflake_schema,
        output_type=OutputType.DICT,
    )

    # Prevent us from accidentally emailing users from our dev environment by changing all domains to @example.com,
    # unless we are in the production environment.
    # Be aware: Sending a large number of emails to fake (@example.com) accounts can harm our email reputation score.
    user_deltas_dicts = mask_email_domain_outside_production(user_deltas_dicts)

    # Convert Snowflake dicts to @dataclass objects.
    all_user_deltas = get_users_to_identify_from_dicts(user_deltas_dicts)

    # Identify ('merge') Pocket users with their email alias any time we have a new email for them.
    # This deletes their old email alias.
    identify_users_task = identify_users.map(
        split_in_chunks(
            all_user_deltas,
            chunk_size=max_operations_per_task_run,
        ),
    )

if __name__ == "__main__":
    flow.run()
