import os
from typing import Any

from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.snowflake import SnowflakeQuery
from prefect.utilities.tasks import defaults_from_attrs
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class AuthenticableSnowflakeQuery(SnowflakeQuery):
    def __init__(
            self,
            private_key_env_var_name: str = "SNOWFLAKE_PRIVATE_KEY",
            private_key_passphrase_env_var_name: str = "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
            **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.private_key_env_var_name = private_key_env_var_name
        self.private_key_passphrase_env_var_name = private_key_passphrase_env_var_name

    @defaults_from_attrs("private_key_env_var_name", "private_key_passphrase_env_var_name")
    def run(
            self,
            private_key_env_var_name: str = None,
            private_key: bytes = None,
            private_key_passphrase_env_var_name: str = None,
            **kwargs):
        if private_key is None and private_key_env_var_name in os.environ:
            private_key = os.environ.get(private_key_env_var_name).encode()

        if private_key_passphrase_env_var_name:
            private_key_passphrase = os.environ[private_key_passphrase_env_var_name].encode()

            private_key = serialization.load_pem_private_key(
                private_key,
                password=private_key_passphrase,
                backend=default_backend()
            )

            private_key = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())

        super().run(private_key = private_key, **kwargs)

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
