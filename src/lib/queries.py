from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect.tasks.snowflake import SnowflakeQuery
from prefect.tasks.secrets import PrefectSecret

with open("src/lib/rsa_key.p8", "rb") as key:
    snowflake_passphrase = PrefectSecret('SNOWFLAKE_PASSPHRASE').run().encode()
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=snowflake_passphrase,
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

# https://docs.prefect.io/api/latest/tasks/aws.html#awssecretsmanager

def get_snowflake_query():
    snowflake_account = PrefectSecret('SNOWFLAKE_ACCOUNT').run()
    snowflake_user = PrefectSecret('SNOWFLAKE_USER').run()
    return SnowflakeQuery(
        account=snowflake_account,
        user=snowflake_user,
        private_key=pkb
    )
