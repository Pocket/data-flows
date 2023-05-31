from pathlib import PosixPath

from prefect_snowflake.credentials import SnowflakeCredentials
from pydantic import SecretStr

from common.databases.snowflake_utils import PktSnowflakeConnector


def test_pkt_snowflake_connector():
    x = PktSnowflakeConnector()
    assert x == PktSnowflakeConnector(
        credentials=SnowflakeCredentials(
            account="test.us-test-1",
            user="test@mozilla.com",
            password=None,
            private_key=None,
            private_key_path=PosixPath("tmp/test.p8"),
            private_key_passphrase=SecretStr("**********"),
            authenticator="snowflake",
            token=None,
            endpoint=None,
            role=None,
            autocommit=None,
        ),
        database="test",
        warehouse="test",
        fetch_size=1,
        poll_frequency_s=1,
    )
