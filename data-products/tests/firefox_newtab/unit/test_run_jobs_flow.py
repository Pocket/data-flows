import os
from unittest.mock import AsyncMock, AsyncMagicMixin

import pytest
from common import get_script_path
from firefox_newtab.run_jobs_flow import (
    SQL_LOCATION,
    MozGcpCredentials,
    MozSnowflakeConnector,
    SnowflakeGcsStageSettings,
    get_cached_settings,
    get_gcs_stage,
    get_intervals,
    interval,
    main,
)
from pendulum import now as pd_now
from prefect import task, flow

SQL_JOB_TEST_DATETIME = pd_now(tz="utc").start_of("day")

TEST_SQL_LOCATION = os.path.join(get_script_path(), "sql")


def test_get_intervals():
    today = pd_now().start_of("day")
    tomorrow = today.add(days=1)
    today_str = today.to_datetime_string()
    tomorrow_str = tomorrow.to_datetime_string()
    two_days_ago_str = today.subtract(days=2).to_datetime_string()
    yesterday_str = today.subtract(days=1).to_datetime_string()
    x1 = get_intervals("2024-07-01", "2024-07-06")
    assert x1 == [
        ("2024-07-01 00:00:00", "2024-07-02 00:00:00"),
        ("2024-07-02 00:00:00", "2024-07-03 00:00:00"),
        ("2024-07-03 00:00:00", "2024-07-04 00:00:00"),
        ("2024-07-04 00:00:00", "2024-07-05 00:00:00"),
        ("2024-07-05 00:00:00", "2024-07-06 00:00:00"),
    ]
    x2 = get_intervals(two_days_ago_str)
    assert x2 == [
        (two_days_ago_str, yesterday_str),
        (yesterday_str, today_str),
    ]
    x4 = get_intervals(include_now=True)
    assert x4 == [
        (yesterday_str, today_str),
        (today_str, tomorrow_str),
    ]
    x5 = get_intervals(two_days_ago_str, include_now=True)
    assert x5 == [
        (two_days_ago_str, yesterday_str),
        (yesterday_str, today_str),
        (today_str, tomorrow_str),
    ]
    x6 = get_intervals()
    assert x6 == [
        (yesterday_str, today_str),
    ]


@pytest.mark.asyncio
async def test_interval(monkeypatch):

    load_inputs = []
    extract_inputs = []

    test_files = os.path.join(TEST_SQL_LOCATION, "test")

    @task()
    async def fake_bq(*args, **kwargs):
        extract_inputs.append(args)

    @task()
    async def fake_sf(*args, **kwargs):
        load_inputs.append(kwargs)

    monkeypatch.setattr("firefox_newtab.run_jobs_flow.bigquery_query", fake_bq)
    monkeypatch.setattr("firefox_newtab.run_jobs_flow.snowflake_multiquery", fake_sf)

    gcp_creds = MozGcpCredentials()
    sf_creds = MozSnowflakeConnector()
    stg_data = get_cached_settings(SnowflakeGcsStageSettings)
    sf_stage = get_gcs_stage(stg_data.snowflake_gcp_stage_data, "default")

    await interval(
        test_files,
        gcp_creds,
        sf_creds,
        sf_stage,
        ("2024-07-01 00:00:00", "2024-07-02 00:00:00"),
        {"for_backfill": True},
    )  # type: ignore

    assert (
        "EXPORT DATA OPTIONS(\n          uri='gs:///Users/mozilla/projects/data-flows/data-products/tests/firefox_newtab/unit/sql/test/2024-07-01"
        in extract_inputs[0][0]
    )
    assert (
        "SELECT\n    *   \nFROM \n\nfrom _stable\n\nwhere updated_at >= '2024-07-01 00:00:00'\nand updated_at < '2024-07-02 00:00:00'"
        in extract_inputs[0][0]
    )
    assert (
        "copy into  (\n              batch_id,\n              updated_at,\n              data,\n              _gs_file_name,\n            _gs_file_row_number,\n            _gs_file_date,\n            _gs_file_time,\n            _loaded_at\n            )\n        from (\n            select\n                ,\n                $1:updated_at as updated_at,\n                $1 as data,\n                metadata$filename,\n            metadata$file_row_number,\n            split_part(metadata$filename,'/', -3),\n            split_part(metadata$filename,'/', -2),\n            sysdate()\n            from /Users/mozilla/projects/data-flows/data-products/tests/firefox_newtab/unit/sql/test"
        in load_inputs[0]["queries"][0]
    )


@pytest.mark.asyncio
async def test_main(monkeypatch):
    mock_results = {"call_count": 0}

    @flow()
    async def fake_flow(*args, **kwargs):
        mock_results["call_count"] += 1
    
    monkeypatch.setattr("firefox_newtab.run_jobs_flow.SQL_LOCATION", TEST_SQL_LOCATION)
    monkeypatch.setattr("firefox_newtab.run_jobs_flow.interval", fake_flow)
    await main("test")
    assert mock_results["call_count"] == 3
