import base64
from enum import Enum
import os
from typing import Any

from prefect.tasks.snowflake import SnowflakeQuery
from prefect.utilities.tasks import defaults_from_attrs
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor
import pandas as pd


class OutputType(Enum):
    DICT = 1
    DATA_FRAME = 2


class PocketSnowflakeQuery(SnowflakeQuery):
    """
    PocketSnowflakeQuery automatically connects to our Snowflake warehouse if the right environment variables are set:
    - SNOWFLAKE_PRIVATE_KEY
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_ROLE
    - SNOWFLAKE_WAREHOUSE

    It defaults to returning the query request as a Pandas DataFrame.
    """

    def __init__(
            self,
            query: str = None,
            data: tuple = None,
            private_key_env_var_name: str = "SNOWFLAKE_PRIVATE_KEY",
            account_env_var_name: str = "SNOWFLAKE_ACCOUNT",
            user_env_var_name: str = "SNOWFLAKE_USER",
            role_env_var_name: str = "SNOWFLAKE_ROLE",
            warehouse_env_var_name: str = "SNOWFLAKE_WAREHOUSE",
            cursor_type: SnowflakeCursor = DictCursor,
            output_type: OutputType = OutputType.DATA_FRAME,
            **kwargs: Any,  # See the base SnowflakeQuery class for additional arguments
    ):
        super().__init__(
            query=query,
            data=data,
            cursor_type=cursor_type,
            **kwargs
        )
        self.private_key_env_var_name = private_key_env_var_name
        self.account_env_var_name = account_env_var_name
        self.user_env_var_name = user_env_var_name
        self.role_env_var_name = role_env_var_name
        self.warehouse_env_var_name = warehouse_env_var_name
        self.output_type = output_type

    @defaults_from_attrs(
        "query",
        "data",
        "private_key_env_var_name",
        "account_env_var_name",
        "user_env_var_name",
        "role_env_var_name",
        "warehouse_env_var_name",
        "cursor_type",
        "output_type",
    )
    def run(
            self,
            query: str = None,
            data: tuple = None,
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
            output_type: OutputType = OutputType.DATA_FRAME,
            **kwargs):
        if output_type == OutputType.DATA_FRAME and cursor_type != DictCursor:
            raise ValueError('cursor_type should be DictCursor (the default) to return the result as a DataFrame')

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

        query_result = super().run(
            query=query,
            data=data,
            private_key=private_key,
            account=account,
            user=user,
            role=role,
            warehouse=warehouse,
            cursor_type=cursor_type,
            **kwargs
        )

        self.logger.info(f'Row Count: {len(query_result)}')

        if output_type == OutputType.DICT:
            return query_result
        elif output_type == OutputType.DATA_FRAME:
            return pd.DataFrame(query_result)


def get_query():
    return PocketSnowflakeQuery()
