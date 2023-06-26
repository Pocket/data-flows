from pathlib import PosixPath
from unittest.mock import patch

from pydantic import SecretStr

from common.databases.snowflake_utils import (
    PktSnowflakeConnector,
    get_gcs_stage,
    get_pocket_snowflake_connector_block,
)
from prefect import flow


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
    assert x.stage_name == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_DEV"
    assert x.stage_location == "gs://test"
    assert str(x) == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_DEV"


def test_get_gcs_stage_id():
    x = get_gcs_stage("gcs_pocket_shared")
    assert x.stage_name == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_SHARED"
    assert x.stage_location == "gs://test"
    assert str(x) == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_SHARED"


def test_get_pocket_snowflake_connector_block():
    @flow
    def test_get_pocket_snowflake_connector_block_flow():
        test = get_pocket_snowflake_connector_block(
            warehouse_override="prefect_wh_override", schema_override="override"
        )
        return test

    x = test_get_pocket_snowflake_connector_block_flow()
    assert x.credentials.account == "test.us-test-1"
    assert x.credentials.user == "test@mozilla.com"
    assert x.credentials.password is None
    assert x.credentials.private_key is None
    assert x.credentials.private_key_path == PosixPath("tmp/test.p8")
    assert isinstance(x.credentials.private_key_passphrase, SecretStr)
    assert x.credentials.role == "test"
    assert x.database == "development"
    assert x.warehouse == "prefect_wh_override"
    assert x.schema_ == "override"
