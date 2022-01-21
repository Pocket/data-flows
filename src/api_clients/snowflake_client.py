from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect.tasks.snowflake import SnowflakeQuery
from prefect.tasks.secrets import PrefectSecret
from snowflake.connector import DictCursor

def get_query():
    # PrefectSecret.run can only be called during Flow run, not during registration.
    private_key = PrefectSecret('SNOWFLAKE_PRIVATE_KEY').run().encode()
    snowflake_passphrase = PrefectSecret('SNOWFLAKE_PASSPHRASE').run().encode()

    p_key = serialization.load_pem_private_key(
        private_key,
        password=snowflake_passphrase,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    snowflake_account = PrefectSecret('SNOWFLAKE_ACCOUNT').run()
    snowflake_user = PrefectSecret('SNOWFLAKE_USER').run()
    private_key = PrefectSecret('SNOWFLAKE_PRIVATE_KEY').run().encode()
    snowflake_passphrase = PrefectSecret('SNOWFLAKE_PASSPHRASE').run().encode()

    p_key = serialization.load_pem_private_key(
        private_key,
        password=snowflake_passphrase,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return SnowflakeQuery(
        account=snowflake_account,
        user=snowflake_user,
        private_key=pkb,
        role=PrefectSecret('SNOWFLAKE_ROLE').run(),
        warehouse=PrefectSecret('SNOWFLAKE_WAREHOUSE').run(),
        cursor_type=DictCursor
    )
