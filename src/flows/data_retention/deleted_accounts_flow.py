import os
from prefect import task, Flow, Parameter
from utils.flow import get_flow_name, get_flow_directory, get_cron_schedule
from utils import config
from prefect.tasks.snowflake import SnowflakeQuery, SnowflakeQueriesFromFile

FLOW_NAME = get_flow_name(__file__)

# with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 1 * *")) as flow:
with Flow(FLOW_NAME) as flow:
    add_deleted_users_result = SnowflakeQueriesFromFile(**config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(
        file_path=os.path.join(get_flow_directory(__file__), 'deleted_account_users.sql'),
        account=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("account"),
        user=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("user"),
    )
    add_deleted_emails_result = SnowflakeQueriesFromFile(**config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(
        file_path=os.path.join(get_flow_directory(__file__), 'deleted_account_emails.sql'),
        account=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("account"),
        user=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("user"),
    )
    add_deleted_emails_result.set_upstream(add_deleted_users_result)

    delete_snowplow_events_result = SnowflakeQueriesFromFile(**config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(
        file_path=os.path.join(get_flow_directory(__file__), 'delete_snowplow_events.sql'),
        account=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("account"),
        user=config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT.get("user"),
        database=config.SNOWPLOW_DB,
        schema=config.SNOWPLOW_SCHEMA,
    )
    delete_snowplow_events_result.set_upstream(add_deleted_users_result)

if __name__ == "__main__":
    flow.run()
