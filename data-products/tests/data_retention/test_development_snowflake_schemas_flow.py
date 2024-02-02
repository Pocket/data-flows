import pytest
from prefect import task

from data_retention.development_snowflake_schemas_flow import delete_old_dev_schemas


@pytest.mark.asyncio
async def test_delete_deleted_account_data(monkeypatch):
    state = {"sf_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        if state["sf_call_count"] == 1:
            assert kwargs["query"] == "DROP SCHEMA TEST.TEST"
        state["sf_call_count"] += 1
        return [("TEST.TEST",)]

    monkeypatch.setattr(
        "data_retention.development_snowflake_schemas_flow.snowflake_query", fake_task
    )
    await delete_old_dev_schemas()
    assert state["sf_call_count"] == 2
