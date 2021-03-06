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


with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 15 * *")) as flow:

    # Maintain a list of deleted accounts in a protected DB/Schema tables
    add_deleted_users_result = query_file(file_name='deleted_account_users.sql',
                                          task_args=dict(name="SavingDeletedUserAccount_user_ids")
                                          )
    add_deleted_emails_result = query_file(file_name='deleted_account_emails.sql',
                                          task_args=dict(name="SavingDeletedUserAccount_emails")
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
                                               task_args=dict(name="DeletingRawData_SnowplowRawEvents")
                                               )

    # Delete Raw data of deleted user accounts from other streaming sources
    delete_raw_data_result = query_file(file_name='delete_raw_user_rows.sql',
                                        database=config.SNOWFLAKE_RAWDATA_DB,
                                        schema=config.SNOWFLAKE_RAWDATA_FIREHOSE_SCHEMA,
                                        upstream_tasks=backup_results,
                                        task_args=dict(name="DeletingRawData_RawFirehose")
                                        )

    # Delete Snapshot Raw data of deleted user accounts from other streaming sources
    delete_snapshot_data_result = query_file(file_name='delete_snapshot_firehose_rows.sql',
                                        database=config.SNOWFLAKE_SNAPSHOT_DB,
                                        schema=config.SNOWFLAKE_SNAPSHOT_FIREHOSE_SCHEMA,
                                        upstream_tasks=backup_results,
                                        task_args=dict(name="DeletingRawData_SnapshotFirehose")
                                        )

    # Delete Miscellaneous table data for deleted user accounts
    delete_miscellaneous_data_result = query_file(file_name='delete_miscellaneous_table_rows.sql',
                                        upstream_tasks=backup_results,
                                        task_args=dict(name="DeletingRawData_MiscellaneousTable")
                                        )

    # Delete Stripe data for deleted user accounts
    delete_stripe_data_result = query_file(file_name='delete_stripe_table_rows.sql',
                                        upstream_tasks=backup_results,
                                        task_args=dict(name="DeletingRawData_Stripe")
                                        )

if __name__ == "__main__":
    flow.run()
