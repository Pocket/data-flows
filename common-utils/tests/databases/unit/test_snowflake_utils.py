from pathlib import PosixPath

import pytest
from pydantic import SecretStr
import importlib

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
