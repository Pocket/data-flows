from prefect import task, Flow

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from utils.flow import get_flow_name, get_cron_schedule
from utils.config import SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT, SNOWFLAKE_DATA_RETENTION_DB, SNOWFLAKE_DATA_RETENTION_SCHEMA

FLOW_NAME = get_flow_name(__file__)

# Add new deleted user accounts
ADD_DELETED_USERS_SQL = """
insert into %(snowflake_data_retention_db)s.%(snowflake_data_retention_schema)s.DELETED_USERS
    (hashed_user_id, user_id)
select distinct a.hashed_user_id, a.user_id
from analytics.dbt_staging.stg_account_deletions as a
left join %(snowflake_data_retention_db)s.%(snowflake_data_retention_schema)s.DELETED_USERS as b 
    on b.hashed_user_id = a.hashed_user_id and b.user_id = a.user_id
where b.hashed_user_id is null;
"""

# Add new deleted user emails
ADD_DELETED_EMAILS_SQL = """
insert into %(snowflake_data_retention_db)s.%(snowflake_data_retention_schema)s.DELETED_EMAILS
    (email)
select distinct m.email
from analytics.dbt_staging.stg_user_to_email_map as m
join %(snowflake_data_retention_db)s.%(snowflake_data_retention_schema)s.DELETED_USERS as u on u.user_id = m.user_id
left join %(snowflake_data_retention_db)s.%(snowflake_data_retention_schema)s.DELETED_EMAILS as e on e.email = m.email
where e.email is null;
"""

# with Flow(FLOW_NAME, schedule=get_cron_schedule(cron="0 0 1 * *")) as flow:
with Flow(FLOW_NAME) as flow:
    delete_user_result = PocketSnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(
        query=ADD_DELETED_USERS_SQL,
        data={
            'snowflake_data_retention_db': SNOWFLAKE_DATA_RETENTION_DB,
            'snowflake_data_retention_schema': SNOWFLAKE_DATA_RETENTION_SCHEMA,
        }
    )
    delete_email_result = PocketSnowflakeQuery(**SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT)(
        query=ADD_DELETED_EMAILS_SQL,
        data={
            'snowflake_data_retention_db': SNOWFLAKE_DATA_RETENTION_DB,
            'snowflake_data_retention_schema': SNOWFLAKE_DATA_RETENTION_SCHEMA,
        }
    )
    delete_email_result.set_upstream(delete_user_result)

if __name__ == "__main__":
    flow.run()
