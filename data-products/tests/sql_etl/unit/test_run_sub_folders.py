from unittest.mock import patch

import pytest
from pendulum import now as pd_now
from prefect import flow, task
from sql_etl.run_jobs_flow import SF_GCP_STAGE_ID, SqlEtlJob, main

SQL_JOB_TEST_DATETIME = pd_now(tz="utc").start_of("day")


@pytest.mark.asyncio
async def test_main_subfolders():
    t = SqlEtlJob(
        sql_folder_name="test_sub_folders",
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
    assert mock_state["call_count"] == 10
