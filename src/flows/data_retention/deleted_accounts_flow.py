from prefect import task, Flow

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from utils.flow import get_flow_name, get_cron_schedule
from utils.config import SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT

FLOW_NAME = get_flow_name(__file__)

# Add new deleted user accounts
ADD_DELETED_USERS_SQL = """
insert into development.gaurang_data_retention_deletion.DELETED_USERS
    (hashed_user_id, user_id)
select distinct a.hashed_user_id, a.user_id
from analytics.dbt_staging.stg_account_deletions as a
left join development.gaurang_data_retention_deletion.DELETED_USERS as b 
    on b.hashed_user_id = a.hashed_user_id and b.user_id = a.user_id
where b.hashed_user_id is null;
"""

# Add new deleted user emails
ADD_DELETED_EMAILS_SQL = """
insert into development.gaurang_data_retention_deletion.DELETED_EMAILS
    (email)
select distinct m.email
from analytics.dbt_staging.stg_user_to_email_map as m
join development.gaurang_data_retention_deletion.DELETED_USERS as u on u.user_id = m.user_id
left join development.gaurang_data_retention_deletion.DELETED_EMAILS as e on e.email = m.email
where e.email is null;
"""

# with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 1 * *")) as flow:
with Flow(FLOW_NAME) as flow:
    delete_user_result = PocketSnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(query=ADD_DELETED_USERS_SQL)
    delete_email_result = PocketSnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(query=ADD_DELETED_EMAILS_SQL)
    delete_email_result.set_upstream(delete_user_result)

if __name__ == "__main__":
    flow.run()
