from prefect import Flow, task
from prefect.tasks.snowflake import SnowflakeQueriesFromFile

from utils import config
from utils.flow import get_flow_name, get_cron_schedule, get_flow_file_path

FLOW_NAME = get_flow_name(__file__)


@task()
def query_file(file_name: str, **kwargs):
    cfg = config.SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT
    # SnowflakeQueriesFromFile has a bug where it throws a validation error if user and account
    # are not passed to `run()`.
    return SnowflakeQueriesFromFile(**cfg).run(
        file_path=get_flow_file_path(__file__, file_name),
        account=cfg['account'],
        user=cfg['user'],
        **kwargs
    )


# with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 1 * *")) as flow:
with Flow(FLOW_NAME) as flow:
    # Maintain a list of deleted accounts in a protected DB/Schema tables
    add_deleted_users_result = query_file(file_name='deleted_account_users.sql',
                                          task_args=dict(name="Saving Deleted User Account: user_ids")
                                          )
    add_deleted_emails_result = query_file(file_name='deleted_account_emails.sql',
                                          task_args=dict(name="Saving Deleted User Account: emails")
                                          )
    add_deleted_emails_result.set_upstream(add_deleted_users_result)

    backup_results = [
        add_deleted_users_result,
        add_deleted_emails_result,
    ]
    # Delete Snowplow Raw events of deleted user accounts
    delete_snowplow_events_result = query_file(file_name='delete_snowplow_events.sql',
                                               database=config.SNOWFLAKE_SNOWPLOW_DB,
                                               schema=config.SNOWFLAKE_SNOWPLOW_SCHEMA,
                                               upstream_tasks=backup_results,
                                               task_args=dict(name=f"Deleting Snowplow raw events from "
                                                                   f"{config.SNOWFLAKE_SNOWPLOW_DB}/"
                                                                   f"{config.SNOWFLAKE_SNOWPLOW_SCHEMA}")
                                               )

    # Delete Raw data of deleted user accounts from other streaming sources
    delete_raw_data_result = query_file(file_name='delete_raw_user_rows.sql',
                                        database=config.SNOWFLAKE_RAWDATA_DB,
                                        schema=config.SNOWFLAKE_RAWDATA_FIREHOSE_SCHEMA,
                                        upstream_tasks=backup_results,
                                        task_args=dict(name=f"Deleting Raw data from {config.SNOWFLAKE_RAWDATA_DB}"
                                                            f"/{config.SNOWFLAKE_RAWDATA_FIREHOSE_SCHEMA}")
                                        )

if __name__ == "__main__":
    flow.run()
