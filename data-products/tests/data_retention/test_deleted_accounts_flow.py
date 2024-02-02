import pytest
from data_retention.deleted_accounts_flow import (
    delete_deleted_account_data,
    read_sql_file,
)
from prefect import task


@pytest.mark.asyncio
async def test_delete_deleted_account_data(monkeypatch):
    state = {"sf_call_count": 0, "query_list": []}

    @task()
    async def fake_task(*args, **kwargs):
        state["sf_call_count"] += 1
        state["query_list"].append(kwargs.get("queries", kwargs.get("query")))
        return []

    monkeypatch.setattr(
        "data_retention.deleted_accounts_flow.snowflake_query", fake_task
    )
    monkeypatch.setattr(
        "data_retention.deleted_accounts_flow.snowflake_multiquery", fake_task
    )
    await delete_deleted_account_data()
    assert state["sf_call_count"] == 7
    assert state["query_list"][0] == read_sql_file("deleted_account_users.sql")
    assert state["query_list"][1] == read_sql_file("deleted_account_emails.sql")
    assert state["query_list"][2] == list(
        filter(bool, read_sql_file("delete_snowplow_events.sql").split(";"))
    )
    assert state["query_list"][3] == list(
        filter(bool, read_sql_file("delete_raw_user_rows.sql").split(";"))
    )
    assert state["query_list"][4] == list(
        filter(bool, read_sql_file("delete_snapshot_firehose_rows.sql").split(";"))
    )
    assert state["query_list"][5] == list(
        filter(bool, read_sql_file("delete_miscellaneous_table_rows.sql").split(";"))
    )
    assert state["query_list"][6] == list(
        filter(bool, read_sql_file("delete_stripe_table_rows.sql").split(";"))
    )
