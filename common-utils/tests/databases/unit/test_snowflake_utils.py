import importlib
from pathlib import PosixPath
from unittest.mock import MagicMock

import pytest
from common.databases.snowflake_utils import query_to_dataframe
from pydantic import SecretStr

DB_MAPPING = {"dev": "development", "staging": "development", "main": "prefect"}


@pytest.mark.parametrize("deployment_type", ["dev", "staging", "main"])
def test_pkt_snowflake_connector(deployment_type):
    import common.databases.snowflake_utils as sfu

    importlib.reload(sfu)
    sfu.CS.deployment_type = deployment_type
    x = sfu.MozSnowflakeConnector()
    assert x.credentials.account == "test.us-test-1"
    assert x.credentials.user == "test@mozilla.com"
    assert x.credentials.password is None
    assert x.credentials.private_key is None
    assert x.credentials.private_key_path == PosixPath("tmp/test.p8")
    assert isinstance(x.credentials.private_key_passphrase, SecretStr)
    assert x.credentials.role == "test"
    assert x.database == DB_MAPPING[deployment_type]
    assert x.warehouse == "prefect_wh_test"
    assert x.schema_ == "test"


def test_get_gcs_stage():
    from common.databases.snowflake_utils import (
        SnowflakeGcsStageSettings,
        get_gcs_stage,
    )

    stage_data = SnowflakeGcsStageSettings()  # type: ignore
    x = get_gcs_stage(stage_data.snowflake_gcp_stage_data)
    assert x.stage_name == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_DEV"
    assert x.stage_location == "gs://test"
    assert str(x) == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_DEV"


def test_get_gcs_stage_id():
    from common.databases.snowflake_utils import (
        SnowflakeGcsStageSettings,
        get_gcs_stage,
    )

    stage_data = SnowflakeGcsStageSettings()  # type: ignore
    x = get_gcs_stage(stage_data.snowflake_gcp_stage_data, "gcs_pocket_shared")
    assert x.stage_name == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_SHARED"
    assert x.stage_location == "gs://test"
    assert str(x) == "DEVELOPMENT.TEST.PREFECT_GCS_STAGE_PARQ_SHARED"


# borrowed from Prefect Snowflake
# should contribute the dataframe stuff


class SnowflakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_async(self, query, params):
        query_id = "1234"
        self.result = {query_id: [(query, params)]}
        return {"queryId": query_id}

    def get_results_from_sfqid(self, query_id):
        self.query_result = self.result[query_id]

    def fetchall(self):
        return self.query_result

    def execute(self, query, params=None):
        self.query_result = [(query, params, "sync")]
        return self

    def fetch_pandas_all(self):
        return None


class SnowflakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return SnowflakeCursor()

    def is_still_running(self, state):
        return state

    def get_query_status_throw_if_error(self, query_id):
        return False


@pytest.fixture()
def snowflake_connector(snowflake_connect_mock):
    snowflake_connector_mock = MagicMock()
    snowflake_connector_mock.get_connection.return_value = SnowflakeConnection()
    return snowflake_connector_mock


@pytest.mark.asyncio
async def test_query_to_dataframe():
    snowflake_connector_mock = MagicMock()
    snowflake_connector_mock.get_connection.return_value = SnowflakeConnection()
    await query_to_dataframe.fn(snowflake_connector_mock, "select 1")
    assert snowflake_connector_mock.call_count == 0
