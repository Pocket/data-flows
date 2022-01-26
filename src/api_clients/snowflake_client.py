import base64
import os
from typing import Any

from prefect.tasks.snowflake import SnowflakeQuery
from prefect.utilities.tasks import defaults_from_attrs
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor


class AuthenticableSnowflakeQuery(SnowflakeQuery):
    def __init__(
            self,
            private_key_env_var_name: str = "SNOWFLAKE_PRIVATE_KEY",
            account_env_var_name: str = "SNOWFLAKE_ACCOUNT",
            user_env_var_name: str = "SNOWFLAKE_USER",
            role_env_var_name: str = "SNOWFLAKE_ROLE",
            warehouse_env_var_name: str = "SNOWFLAKE_WAREHOUSE",
            cursor_type: SnowflakeCursor = DictCursor,
            **kwargs: Any,
    ):
        super().__init__(cursor_type=cursor_type, **kwargs)
        self.private_key_env_var_name = private_key_env_var_name
        self.account_env_var_name = account_env_var_name
        self.user_env_var_name = user_env_var_name
        self.role_env_var_name = role_env_var_name
        self.warehouse_env_var_name = warehouse_env_var_name

    @defaults_from_attrs(
        "private_key_env_var_name",
        "account_env_var_name",
        "user_env_var_name",
        "role_env_var_name",
        "warehouse_env_var_name",
        "cursor_type",
    )
    def run(
            self,
            private_key_env_var_name: str = None,
            private_key: bytes = None,
            account_env_var_name: str = None,
            account: str = None,
            user_env_var_name: str = None,
            user: str = None,
            role_env_var_name: str = None,
            role: str = None,
            warehouse_env_var_name: str = None,
            warehouse: str = None,
            cursor_type: SnowflakeCursor = DictCursor,
            **kwargs):
        if private_key is None and private_key_env_var_name in os.environ:
            private_key = base64.b64decode(os.environ.get(private_key_env_var_name))

        if account is None and account_env_var_name in os.environ:
            account = os.environ.get(account_env_var_name)

        if user is None and user_env_var_name in os.environ:
            user = os.environ.get(user_env_var_name)

        if role is None and role_env_var_name in os.environ:
            role = os.environ.get(role_env_var_name)

        if warehouse is None and warehouse_env_var_name in os.environ:
            warehouse = os.environ.get(warehouse_env_var_name)

        return super().run(
            private_key=private_key,
            account=account,
            user=user,
            role=role,
            warehouse=warehouse,
            **kwargs
        )


def get_query():
    return AuthenticableSnowflakeQuery()
