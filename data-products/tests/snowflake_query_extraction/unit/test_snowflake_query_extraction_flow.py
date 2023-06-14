import os
from asyncio import run
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common.testing_utils import reset_script_path  # noqa: F401
from prefect import task

from snowflake_query_extraction.snowflake_query_extraction_flow import (
    SfExtractionInputs,
    create_extraction_job,
    main,
)

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
NAMESPACE_OVERRIDE = "snowflake_query_extraction.snowflake_query_extraction_flow"


@pytest.mark.script_path_override(SCRIPT_PATH)
@pytest.mark.script_path_namespace(NAMESPACE_OVERRIDE)
def test_extraction_inputs(reset_script_path):  # noqa: F811
    x = SfExtractionInputs(
        sql_name="test",
        offset_key="test_date",
        default_offset="1970-01-01",
        kwargs={
            "database_name": "test",
            "schema_name": "test",
            "table_name": "test",
        },
    )
    assert (
        x.current_offset_sql
        == "select coalesce(any_value(state), '1970-01-01') as state\n    from query_extraction_state\n    where sql_name = 'test';"  # noqa: E501
    )
    assert x.new_offset_sql == "select max(test_date)\nfrom test.test.test"


@pytest.mark.script_path_override(SCRIPT_PATH)
@pytest.mark.script_path_namespace(NAMESPACE_OVERRIDE)
def test_create_extraction_job(reset_script_path):  # noqa: F811
    x = create_extraction_job.fn(
        sf_extraction_input=SfExtractionInputs(
            sql_name="test",
            offset_key="test_date",
            default_offset="1970-01-01",
            kwargs={
                "database_name": "test",
                "schema_name": "test",
                "table_name": "test",
            },
        ),
        current_offset="1",
        new_offset="2",
    )
    assert (
        "copy into @DEVELOPMENT.PUBLIC.PREFECT_GCS_STAGE_PARQ_DEV/test"
        in x.extraction_sql
    )
    assert (
        "from (select *\nfrom test.test.test\nwhere test_date > '1'\nand test_date <= '2'\nand test_date > current_timestamp - interval '24 hours'\n\n\n)\n    header = true\n    overwrite = true\n    max_file_size = 104857600"  # noqa: E501
        in x.extraction_sql
    )
    assert (
        x.persist_state_sql
        == "merge into query_extraction_state dt using (\n        select 'test' as sql_name, \n        current_timestamp as created_at, \n        current_timestamp as updated_at,\n        '2' as state\n    ) st on st.sql_name = dt.sql_name\n    when matched then update \n    set updated_at = st.updated_at,\n        state = st.state\n    when not matched then insert (sql_name, created_at, updated_at, state) \n    values (st.sql_name, st.created_at, st.updated_at, st.state);"  # noqa: E501
    )  # noqa: E501


@pytest.mark.asyncio
@pytest.mark.script_path_override(SCRIPT_PATH)
@pytest.mark.script_path_namespace(NAMESPACE_OVERRIDE)
async def test_flow(reset_script_path, monkeypatch):  # noqa: F811
    @task
    async def fake_task(query, snowflake_connector):
        return "test"

    monkeypatch.setattr(
        "snowflake_query_extraction.snowflake_query_extraction_flow.snowflake_query",
        fake_task,
    )

    t = SfExtractionInputs(
        sql_name="test",
        offset_key="test",
        default_offset="1970-01-01",
        kwargs={
            "database_name": "test",
            "schema_name": "test",
            "table_name": "test",
        },
    )
    await main(sf_extraction_input=t)  # type: ignore
