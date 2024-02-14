import pytest
from prefect import task

from data_retention.snowflake_development_table_clean_up_flow import (
    delete_old_dev_tables,
)


@pytest.mark.asyncio
async def test_delete_dev_tables(monkeypatch):
    state = {"sf_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        if state["sf_call_count"] == 1:
            assert kwargs["query"] == "DROP TABLE TEST.TEST.TEST"
        state["sf_call_count"] += 1
        return [("TEST.TEST.TEST",)]

    monkeypatch.setattr(
        "data_retention.snowflake_development_table_clean_up_flow.snowflake_query",
        fake_task,
    )
    await delete_old_dev_tables()
    assert state["sf_call_count"] == 2
