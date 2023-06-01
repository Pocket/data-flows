import os
from asyncio import run

import pytest
from common import get_script_path
from common.databases.snowflake_utils import PktSnowflakeConnector
from common.testing_utils import mock_snowflake_task, reset_script_path  # noqa: F401
from prefect import flow
from prefect_snowflake.database import snowflake_query

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


@pytest.mark.asyncio
@pytest.mark.script_path_override(SCRIPT_PATH)
@pytest.mark.script_path_namespace("common")
@pytest.mark.query_results([("42",)])
@pytest.mark.snowflake_task_name("snowflake_query")
async def test_all(mock_snowflake_task, reset_script_path):  # noqa: F811
    assert get_script_path() == SCRIPT_PATH
    sfc = PktSnowflakeConnector(schema="public", warehouse="prefect_wh_dev")

    @flow()
    async def test_flow():
        x = await snowflake_query(query="select 1", snowflake_connector=sfc)
        assert x[0][0] == "42"

    await test_flow()  # type: ignore
