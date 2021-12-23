from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.snowflake import SnowflakeQuery

# https://docs.prefect.io/api/latest/tasks/aws.html#awssecretsmanager

def get_snowflake_query():
    account_id = PrefectSecret('SNOWPLOW_ACCOUNT_ID')
    user = PrefectSecret('SNOWPLOW_USER')
    password = PrefectSecret('SNOWPLOW_PASSWORD')
    return SnowflakeQuery(
            account = account_id.run(),
           user = user.run(),
           password = password.run(),
            database = 'anything',
            schema = 'anything',
        #   role: str = None,
          warehouse = 'anything',
        #     data: tuple = None,
        #   autocommit: bool = None,
        #  cursor_type: SnowflakeCursor = SnowflakeCursor,
        # **kwargs,
    )
