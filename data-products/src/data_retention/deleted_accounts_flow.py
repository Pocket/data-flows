from pathlib import Path

from common import get_script_path
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow
from prefect_snowflake.database import snowflake_multiquery, snowflake_query

SCRIPT_PATH = get_script_path()


def read_sql_file(file_name: str) -> str:
    p = Path(SCRIPT_PATH, file_name)
    return p.read_text()


@flow()
async def delete_deleted_account_data():
    sfc = MozSnowflakeConnector()

    # Maintain a list of deleted accounts in a protected DB/Schema tables
    add_deleted_users_result = await snowflake_query.with_options(
        name="SavingDeletedUserAccount_user_ids"
    )(query=read_sql_file("deleted_account_users.sql"), snowflake_connector=sfc)
    add_deleted_emails_result = await snowflake_query.with_options(
        name="SavingDeletedUserAccount_emails"
    )(query=read_sql_file("deleted_account_emails.sql"), snowflake_connector=sfc)

    backup_results = [
        add_deleted_users_result,
        add_deleted_emails_result,
    ]

    deletion_statement_map = {
        "DeletingRawData_SnowplowRawEvents": "delete_snowplow_events.sql",
        "DeletingRawData_RawFirehose": "delete_raw_user_rows.sql",
        "DeletingRawData_SnapshotFirehose": "delete_snapshot_firehose_rows.sql",
        "DeletingRawData_MiscellaneousTable": "delete_miscellaneous_table_rows.sql",
        "DeletingRawData_Stripe": "delete_stripe_table_rows.sql",
    }

    for k, v in deletion_statement_map.items():
        # Delete Snowplow Raw events of deleted user accounts
        await snowflake_multiquery.with_options(name=k)(
            queries=list(filter(bool, read_sql_file(v).split(";"))),
            snowflake_connector=sfc,
            wait_for=[backup_results],
        )


FLOW_SPEC = FlowSpec(
    flow=delete_deleted_account_data,
    docker_env="base",
    deployments=[
        FlowDeployment(name="deployment", cron="0 0 15 * *"),
    ],
)
