import os
from unittest.mock import patch

import pytest
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
            .subtract(microseconds=1)
            .to_iso8601_string(),
            kwargs={"destination_table_name": "test.test.test"},
            with_external_state=True,
            source_system="snowflake",
            snowflake_stage_id=SF_GCP_STAGE_ID,
        )
        assert t._sql_template_path == os.path.join(os.getcwd(), "src/sql_etl/sql")


def test_sql_job_assert_intervals_external_state():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=3)
        .subtract(microseconds=1)
        .to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        with_external_state=True,
        snowflake_stage_id=SF_GCP_STAGE_ID,
    )
    intervals = t.get_intervals()
    assert [x.dict() for x in intervals] == [
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=3).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "first_interval_start": SQL_JOB_TEST_DATETIME.subtract(
                days=3
            ).to_iso8601_string(),
            "sets_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": True,
            "is_final": False,
        },
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=2).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
            "first_interval_start": SQL_JOB_TEST_DATETIME.subtract(
                days=3
            ).to_iso8601_string(),
            "sets_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": False,
            "is_final": False,
        },
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "first_interval_start": SQL_JOB_TEST_DATETIME.subtract(
                days=3
            ).to_iso8601_string(),
            "sets_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": False,
            "is_final": True,
        },
    ]
    offset = t.get_last_offset_sql()
    assert (
        offset.sql_text.strip().replace(" ", "").replace("\n", "")
        == "selectany_value(last_offset)aslast_offsetfromsql_offset_statewheresql_folder_name='test';"  # noqa: E501
    )
    assert offset.sql_engine == "snowflake"


def test_sql_job_assert_statements():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(days=1)
        .subtract(microseconds=1)
        .to_iso8601_string(),
        kwargs={
            "destination_table_name": "test.test.test",
        },
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    assert [x.dict() for x in intervals] == [
        {
            "batch_start": SQL_JOB_TEST_DATETIME.subtract(days=1).to_iso8601_string(),
            "batch_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "first_interval_start": SQL_JOB_TEST_DATETIME.subtract(
                days=1
            ).to_iso8601_string(),
            "sets_end": SQL_JOB_TEST_DATETIME.to_iso8601_string(),
            "is_initial": True,
            "is_final": True,
        }
    ]
    interval_input = intervals[0]
    offset_persist = SQL_JOB_TEST_DATETIME.add(hours=19).to_iso8601_string()
    ex = t.get_extraction_sql(interval_input)
    assert ex.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"EXPORT DATA OPTIONS(\nuri='gs://test/test/{interval_input.partition_folders}/data*.parq',\nformat='PARQUET',\ncompression='SNAPPY',\noverwrite=true) AS\nSELECT\n\n*\n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}'".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )  # noqa: E501
    assert ex.sql_engine == "bigquery"
    offset = t.get_last_offset_sql()
    assert offset.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == "select max(updated_at) as last_offset\nfrom test.test.test".strip().replace(
        " ", ""
    ).replace(
        "\n", ""
    )
    assert offset.sql_engine == "snowflake"
    lo = t.get_load_sql(interval_input)
    assert lo.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"copy into test.test.test (\nbatch_id,\nupdated_at,\ndata,\n_gs_file_name,\n_gs_file_row_number,\n_gs_file_date,\n_gs_file_time,\n_loaded_at\n)\nfrom (\nselect\n{interval_input.partition_timestamp},\n$1:updated_at as updated_at,\n$1 as data,\nmetadata$filename,\nmetadata$file_row_number,\nsplit_part(metadata$filename,'/', -3),\nsplit_part(metadata$filename,'/', -2),\nsysdate()\nfrom {t.get_snowflake_stage_uri(interval_input)}\n)\nfile_format = (type = 'PARQUET')".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )  # noqa: E501  # noqa: E501
    assert lo.sql_engine == "snowflake"
    new_off = t.get_new_offset_sql(interval_input)
    assert new_off.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"SELECT\n\n max(updated_at) as new_offset\n\nFROM \n\nfrom \n\nwhere updated_at >= '{interval_input.batch_start}'\nand updated_at < '{interval_input.batch_end}'".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )  # noqa: E501  # noqa: E501
    assert new_off.sql_engine == "bigquery"
    p = t.get_persist_offset_sql(offset_persist)
    assert p.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"merge into sql_offset_state dt using (\nselect 'test' as sql_folder_name, \ncurrent_timestamp as created_at, \ncurrent_timestamp as updated_at,\n'{offset_persist}' as last_offset\n) st on st.sql_folder_name = dt.sql_folder_name\n when matched then update \n set updated_at = st.updated_at,\nlast_offset = st.last_offset\nwhen not matched then insert (sql_folder_name, created_at, updated_at, last_offset) \nvalues (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset);".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )  # noqa: E501
    assert p.sql_engine == "snowflake"
    ls = t.get_file_list_sql(interval_input)
    assert ls.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"LIST '@{t.snowflake_stage}/{t.sql_folder_name}/{interval_input.partition_date_folder}'".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )  # noqa: E501
    assert ls.sql_engine == "snowflake"
    rm = t.get_file_remove_sql(interval_input.partition_folders)
    assert rm.sql_text.strip().replace(" ", "").replace(
        "\n", ""
    ) == f"REMOVE '@{t.snowflake_stage}/{t.sql_folder_name}/{interval_input.partition_folders}'".strip().replace(  # noqa: E501
        " ", ""
    ).replace(
        "\n", ""
    )
    assert rm.sql_engine == "snowflake"
    with patch("sql_etl.run_jobs_flow.Path") as mock:
        mock.return_value.exists.return_value = False
        assert not t.has_load_sql
        assert not t.has_extraction_sql
        assert not t.is_incremental


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

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        await interval(t, interval_input)
    assert mock_state["call_count"] == len(mock_state["result_mocks"])


@pytest.mark.asyncio
async def test_interval_bq_no_load_no_external_state():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(
            microseconds=1000
        ).to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
        snowflake_stage_id=SF_GCP_STAGE_ID,  # type: ignore
    )
    intervals = t.get_intervals()
    interval_input = intervals[0]

    mock_state = {
        "result_mocks": [
            ["success!"],
        ],
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        result = mock_state["result_mocks"][mock_state["call_count"]]
        mock_state["call_count"] += 1
        return result

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        with patch("sql_etl.run_jobs_flow.Path") as mock:
            mock.return_value.exists.return_value = False
            await interval(t, interval_input)
    assert mock_state["call_count"] == len(mock_state["result_mocks"])


@pytest.mark.asyncio
async def test_interval_with_cleanup():
    t = SqlEtlJob(
        sql_folder_name="test",
        initial_last_offset=SQL_JOB_TEST_DATETIME.subtract(
            microseconds=1000
        ).to_iso8601_string(),
        kwargs={"destination_table_name": "test.test.test"},
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

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        with patch("sql_etl.run_jobs_flow.get_files_for_cleanup") as c:
            c.return_value = [
                (
                    "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-17/time=22-00-00-000/data_0_0_0.snappy.parquet",
                )
            ]
            await interval(t, interval_input)
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

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        await interval(t, interval_input)
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

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        with patch("sql_etl.run_jobs_flow.interval", new=fake_flow):
            await main(t)
    assert mock_state["call_count"] == 5


@pytest.mark.asyncio
async def test_non_incremental():
    t = SqlEtlJob(
        sql_folder_name="test_non_incremental",
    )

    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        mock_state["call_count"] += 1

    with patch("shared.utils.SqlStmt") as s:
        s.return_value.run_query_task = fake_task
        await main(t)
    assert mock_state["call_count"] == 1
