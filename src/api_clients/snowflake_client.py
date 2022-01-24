import os
from typing import Any

from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.snowflake import SnowflakeQuery
from prefect.utilities.tasks import defaults_from_attrs
from snowflake.connector import DictCursor

class AuthenticableSnowflakeQuery(SnowflakeQuery):
    def __init__(
            self,
            private_key_env_var_name: str = "SNOWFLAKE_PRIVATE_KEY",
            **kwargs: Any,
    ):
        super.__init__(**kwargs)
        self.private_key_env_var_name = private_key_env_var_name

    @defaults_from_attrs("private_key_env_var_name")
    def run(
            self,
            private_key_env_var_name: str = None,
            private_key: bytes = None,
            **kwargs):
        if private_key is None and private_key_env_var_name in os.environ:
            private_key = os.environ.get(private_key_env_var_name)

        super.run(private_key = private_key, **kwargs)

def get_query():
    # PrefectSecret.run can only be called during Flow run, not during registration.
    snowflake_account = PrefectSecret('SNOWFLAKE_ACCOUNT').run()
    snowflake_user = PrefectSecret('SNOWFLAKE_USER').run()

    return AuthenticableSnowflakeQuery(
        account=snowflake_account,
        user=snowflake_user,
        role=PrefectSecret('SNOWFLAKE_ROLE').run(),
        warehouse=PrefectSecret('SNOWFLAKE_WAREHOUSE').run(),
        cursor_type=DictCursor
    )
