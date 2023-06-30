import os
from unittest.mock import patch

import pytest
from common.databases.snowflake_utils import PktSnowflakeConnector
from pendulum import now as pd_now
from prefect import flow, task
from sql_etl.run_jobs_flow import SF_GCP_STAGE_ID, SqlEtlJob, interval, main

SQL_JOB_TEST_DATETIME = pd_now(tz="utc").start_of("day")


def test_sql_template_path():
    with patch("os.environ") as mock:
        mock.return_value = {}
        t = SqlEtlJob(
            sql_folder_name="test",
            initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=3)
            .subtract(microseconds=1000)
            .to_iso8601_string(),
            kwargs={"destination_table_name": "test.test.test"},
            with_external_state=True,
            source_system="snowflake",
            snowflake_stage_id=SF_GCP_STAGE_ID,
        )
        assert t._sql_template_path == os.path.join(os.getcwd(), "src/sql_etl/sql")


def test_sql_job():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=3)
        .subtract(microseconds=1000)
        .to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        with_external_state=True,
        source_system="snowflake",
        snowflake_stage_id=SF_GCP_STAGE_ID,
    )
    intervals = t.get_intervals()
    assert [x.dict() for x in intervals] == [
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=3).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "base_start": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "base_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": True,
            "is_final": False,
        },
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
            "base_start": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "base_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": False,
            "is_final": False,
        },
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "base_start": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "base_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": False,
            "is_final": True,
        },
    ]
    interval_input = intervals[0]
    offset_persist = SQL_JOB_TEST_DATETIME.add(hours=19).to_iso8601_string()
    assert (
        t.get_extraction_sql(interval_input)
        == f"copy into '{t.get_snowflake_stage_uri(interval_input)}/data'\n        from (SELECT\n\n    *   \n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}')\n        header = true\n        overwrite = true\n        max_file_size = 104857600"  # noqa: E501
    )
    assert (
        t.get_last_offset_sql()
        == "select any_value(last_offset) as last_offset\n    from sql_offset_state\n    where sql_folder_name = 'test';"  # noqa: E501
    )
    assert (
        t.get_load_sql(interval_input)
        == f"copy into test.test.test (\n              batch_id,\n              updated_at,\n              data,\n              _gs_file_name,\n            _gs_file_row_number,\n            _gs_file_date,\n            _gs_file_time,\n            _loaded_at\n            )\n        from (\n            select\n                {interval_input.partition_timestamp},\n                $1:updated_at as updated_at,\n                $1 as data,\n                metadata$filename,\n            metadata$file_row_number,\n            split_part(metadata$filename,'/', -2),\n            split_part(metadata$filename,'/', -1),\n            sysdate()\n            from {t.get_snowflake_stage_uri(interval_input)}\n        )\n        file_format = (type = 'PARQUET')"  # noqa: E501
    )
    assert (
        t.get_new_offset_sql(interval_input)
        == f"SELECT\n\n    max(updated_at) as new_offset\n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}'"  # noqa: E501
    )
    assert (
        t.get_persist_offset_sql(offset_persist)
        == f"merge into sql_offset_state dt using (\n        select 'test' as sql_folder_name, \n        current_timestamp as created_at, \n        current_timestamp as updated_at,\n        '{offset_persist}' as last_offset\n    ) st on st.sql_folder_name = dt.sql_folder_name\n    when matched then update \n    set updated_at = st.updated_at,\n        last_offset = st.last_offset\n    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) \n    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset);"  # noqa: E501
    )


def test_sql_job_external_state_biqquery():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
        kwargs={
            "destination_table_name": "test.test.test",
        },
        source_system="bigquery",
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    assert [x.dict() for x in intervals] == [
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=1)
            .add(microseconds=1000)
            .to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "base_start": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "base_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": True,
            "is_final": True,
        }
    ]
    interval_input = intervals[0]
    offset_persist = SQL_JOB_TEST_DATETIME.add(hours=19).to_iso8601_string()
    assert (
        t.get_extraction_sql(interval_input)
        == f"EXPORT DATA OPTIONS(\n          uri='gs://test/test/{interval_input.partition_folders}/data*.parq',\n          format='PARQUET',\n          compression='SNAPPY',\n          overwrite=true) AS\n        SELECT\n\n    *   \n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}'"  # noqa: E501
    )
    assert (
        t.get_last_offset_sql()
        == "select max(updated_at) as last_offset\nfrom test.test.test"
    )
    assert (
        t.get_load_sql(interval_input)
        == f"copy into test.test.test (\n              batch_id,\n              updated_at,\n              data,\n              _gs_file_name,\n            _gs_file_row_number,\n            _gs_file_date,\n            _gs_file_time,\n            _loaded_at\n            )\n        from (\n            select\n                {interval_input.partition_timestamp},\n                $1:updated_at as updated_at,\n                $1 as data,\n                metadata$filename,\n            metadata$file_row_number,\n            split_part(metadata$filename,'/', -2),\n            split_part(metadata$filename,'/', -1),\n            sysdate()\n            from {t.get_snowflake_stage_uri(interval_input)}\n        )\n        file_format = (type = 'PARQUET')"  # noqa: E501
    )  # noqa: E501
    assert (
        t.get_new_offset_sql(interval_input)
        == f"SELECT\n\n    max(updated_at) as new_offset\n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}'"  # noqa: E501
    )  # noqa: E501
    assert (
        t.get_persist_offset_sql(offset_persist)
        == f"merge into sql_offset_state dt using (\n        select 'test' as sql_folder_name, \n        current_timestamp as created_at, \n        current_timestamp as updated_at,\n        '{offset_persist}' as last_offset\n    ) st on st.sql_folder_name = dt.sql_folder_name\n    when matched then update \n    set updated_at = st.updated_at,\n        last_offset = st.last_offset\n    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) \n    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset);"  # noqa: E501
    )
    assert (
        t.get_file_list_sql(interval_input)
        == f"LIST '@{t.snowflake_stage}/{t.sql_folder_name}/{interval_input.partition_date_folder}'"
    )  # noqa: E501
    assert (
        t.get_file_remove_sql(interval_input.partition_folders)
        == f"REMOVE '@{t.snowflake_stage}/{t.sql_folder_name}/{interval_input.partition_folders}'"
    )
    with patch("sql_etl.run_jobs_flow.Path") as mock:
        mock.return_value.exists.return_value = False
        assert t.get_load_sql(interval_input) is None


@pytest.mark.asyncio
async def test_interval():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(
            microseconds=1000
        ).to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        with_external_state=True,
        source_system="snowflake",
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    interval_input = intervals[0]
    offset_persist = SQL_JOB_TEST_DATETIME.add(hours=19).to_iso8601_string()

    mock_state = {
        "result_mocks": [
            [(offset_persist,)],
            [],
            ["success!"],
            ["success!"],
            ["success!"],
        ],
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        result = mock_state["result_mocks"][mock_state["call_count"]]
        mock_state["call_count"] += 1
        return result

    with patch("sql_etl.run_jobs_flow.snowflake_query", new=fake_task):
        await interval(t, interval_input, PktSnowflakeConnector())
    assert mock_state["call_count"] == len(mock_state["result_mocks"])


@pytest.mark.asyncio
async def test_interval_bq_no_load_no_external_state():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(
            microseconds=1000
        ).to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        source_system="bigquery",
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    interval_input = intervals[0]
    offset_persist = SQL_JOB_TEST_DATETIME.add(hours=19).to_iso8601_string()

    mock_state = {
        "result_mocks": [
            [(offset_persist,)],
            [],
            ["success!"],
        ],
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        result = mock_state["result_mocks"][mock_state["call_count"]]
        mock_state["call_count"] += 1
        return result

    with patch("sql_etl.run_jobs_flow.bigquery_query", new=fake_task):
        with patch("sql_etl.run_jobs_flow.snowflake_query", new=fake_task):
            with patch("sql_etl.run_jobs_flow.Path") as mock:
                mock.return_value.exists.return_value = False
                await interval(t, interval_input, PktSnowflakeConnector())
    assert mock_state["call_count"] == len(mock_state["result_mocks"])


@pytest.mark.asyncio
async def test_interval_none_offset():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(
            microseconds=1000
        ).to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        with_external_state=True,
        source_system="bigquery",
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    interval_input = intervals[0]

    mock_state = {
        "result_mocks": [[(None,)]],
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        result = mock_state["result_mocks"][mock_state["call_count"]]
        mock_state["call_count"] += 1
        return result

    with patch("sql_etl.run_jobs_flow.bigquery_query", new=fake_task):
        with patch("sql_etl.run_jobs_flow.snowflake_query", new=fake_task):
            await interval(t, interval_input, PktSnowflakeConnector())
    assert mock_state["call_count"] == len(mock_state["result_mocks"])


@pytest.mark.asyncio
async def test_main_include_now():
    t = SqlEtlJob(
        sql_folder_name="test",
        override_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=3)
        .subtract(microseconds=2000)
        .to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        with_external_state=True,
        include_now=True,
        source_system="bigquery",
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        return [(None,)]

    @flow
    async def fake_flow(*args, **kwargs):
        mock_state["call_count"] += 1

    with patch("sql_etl.run_jobs_flow.snowflake_query", new=fake_task):
        with patch("sql_etl.run_jobs_flow.interval", new=fake_flow):
            await main(t)
    assert mock_state["call_count"] == 5
