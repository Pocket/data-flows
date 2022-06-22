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
    return SnowflakeQueriesFromFile(**cfg)(
        file_path=get_flow_file_path(__file__, file_name),
        account=cfg['account'],
        user=cfg['user'],
        **kwargs
    )


# with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 1 * *")) as flow:
with Flow(FLOW_NAME) as flow:
    backup_results = [
        query_file('deleted_account_emails.sql'),
        query_file('deleted_account_users.sql'),
    ]

    query_file('delete_snowplow_events.sql', upstream_tasks=backup_results)
    query_file('delete_raw_user_rows.sql', upstream_tasks=backup_results)

if __name__ == "__main__":
    flow.run()
