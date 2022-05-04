from typing import Any, Dict, List, Optional, Sequence, Tuple

from prefect import Flow, task, context, Parameter, case
from prefect.executors import LocalDaskExecutor

from api_clients.braze import models
from api_clients.braze.client import (
    BrazeClient,
    USER_DELETE_LIMIT,
)
from api_clients.braze.pocket_config import EMAIL_ALIAS_LABEL
from api_clients.prefect_key_value_store_client import format_key
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils.flow import get_flow_name
from utils.iteration import chunks
from utils import config
from common_tasks.mapping import split_in_chunks

FLOW_NAME = get_flow_name(__file__)

# Delete users who have deleted their Pocket account
EXTERNAL_ID_QUERY = """
SELECT
    DISTINCT(external_id)
FROM "STG_BRAZE_USER_DELTAS"
WHERE LOADED_AT >= '2022-03-22'
AND USER_EVENT_TRIGGER = 'account_delete'
"""


DEFAULT_LOADED_AT_START = '2022-03-22'  # Value to use for the loaded_at_start query param if KV-store key is missing.
LAST_LOADED_AT_KV_STORE_KEY = format_key(FLOW_NAME, "last_loaded_at")  # KV-store key name
MAX_OPERATIONS_PER_TASK_RUN = 1000  # The workload is split up in chunks of this size, and each chunk is run separately.
DEFAULT_TABLE_NAME = 'STG_BRAZE_USER_DELTAS'
DELETE_CONFIRMATION_STRING = f'PERMANENTLY_DELETE_{config.ENVIRONMENT.upper()}_USERS'


@task()
def is_valid_confirmation_string(confirmation_user_input: str) -> bool:
    return confirmation_user_input == DELETE_CONFIRMATION_STRING


@task()
def delete_user_profiles_by_external_id(users_to_delete: List[Dict[str, str]]):
    logger = context.get("logger")

    for chunk in chunks(users_to_delete, USER_DELETE_LIMIT):
        logger.info(f"Deleting {len(chunk)} user profiles")

        BrazeClient(logger=logger).delete_users(models.UserDeleteInput(
            external_ids=[u['EXTERNAL_ID'] for u in chunk],
        ))


with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:
    # Engineer needs to enter 'PERMANENTLY_DELETE_PRODUCTION_USERS' to confirm users.
    confirm_deletion = Parameter(f'Confirm with {DELETE_CONFIRMATION_STRING}', default='')

    with case(confirm_deletion, DELETE_CONFIRMATION_STRING):
        external_ids = PocketSnowflakeQuery()(
            query=EXTERNAL_ID_QUERY,
            database=config.SNOWFLAKE_ANALYTICS_DATABASE,
            schema=config.SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA,
            output_type=OutputType.DICT,
        )

        delete_user_profiles_by_external_id.map(
            split_in_chunks(
                external_ids,
                chunk_size=MAX_OPERATIONS_PER_TASK_RUN,
            )
        )

if __name__ == "__main__":
    flow.run()
