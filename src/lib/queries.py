import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect.tasks.snowflake import SnowflakeQuery

with open("rsa_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=os.environ['SNOWFLAKE_PASSPHRASE'].encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

# https://docs.prefect.io/api/latest/tasks/aws.html#awssecretsmanager

# def get_snowflake_query():
#     account_id = PrefectSecret('SNOWPLOW_ACCOUNT_ID')
#     user = PrefectSecret('SNOWPLOW_USER')
#     password = PrefectSecret('SNOWPLOW_PASSWORD')
#     return SnowflakeQuery(
#             account = account_id.run(),
#            user = user.run(),
#         # password='',
#         private_key='',
#             database = 'anything',
#             schema = 'anything',
#         #   role: str = None,
#           warehouse = 'anything',
#         #     data: tuple = None,
#         #   autocommit: bool = None,
#         #  cursor_type: SnowflakeCursor = SnowflakeCursor,
#         # **kwargs,
#     )

query = SnowflakeQuery(
    account="https://cka72749.us-east-1.snowflakecomputing.com",
    user="ctroy@getpocket.com",
    private_key=pkb
)
query.run(query="select * from table")
