from pathlib import PosixPath
from unittest.mock import patch

from common.databases.snowflake_utils import PktSnowflakeConnector, get_gcs_stage
from pydantic import SecretStr


def test_pkt_snowflake_connector():
    x = PktSnowflakeConnector()
    assert x.credentials.account == "test.us-test-1"
    assert x.credentials.user == "test@mozilla.com"
    assert x.credentials.password is None
    assert x.credentials.private_key is None
    assert x.credentials.private_key_path == PosixPath("tmp/test.p8")
    assert isinstance(x.credentials.private_key_passphrase, SecretStr)
    assert x.credentials.role == "test"
    assert x.database == "development"
    assert x.warehouse == "prefect_wh_test"
    assert x.schema_ == "test"


@patch("common.settings.CommonSettings.is_production")
def test_pkt_snowflake_connector_production(mock):
    mock.return_value = True
    x = PktSnowflakeConnector()
    assert x.credentials.account == "test.us-test-1"
    assert x.credentials.user == "test@mozilla.com"
    assert x.credentials.password is None
    assert x.credentials.private_key is None
    assert x.credentials.private_key_path == PosixPath("tmp/test.p8")
    assert isinstance(x.credentials.private_key_passphrase, SecretStr)
    assert x.credentials.role == "test"
    assert x.database == "prefect"
    assert x.warehouse == "prefect_wh_test"
    assert x.schema_ == "test"


def test_get_gcs_stage():
    x = get_gcs_stage()
    assert x == "DEVELOPMENT.PUBLIC.PREFECT_GCS_STAGE_PARQ_DEV"

@patch("common.settings.CommonSettings.is_production")
def test_get_gcs_stage_production(mock):
    mock.return_value = True
    x = get_gcs_stage()
    assert x == "PREFECT.PUBLIC.PREFECT_GCS_STAGE_PARQ_PROD"
