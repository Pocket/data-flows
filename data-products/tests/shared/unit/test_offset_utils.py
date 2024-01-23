import pendulum
import pytest
from common.databases.snowflake_utils import MozSnowflakeConnector
from prefect import flow, task
from shared.offset_utils import get_last_offset, upsert_new_offset


@pytest.mark.asyncio
async def test_offset_utils(monkeypatch):
    @task()
    async def fake_task_get(*args, **kwargs):
        assert pendulum.parser.parse(kwargs["params"]["default_offset"]).is_utc()  # type: ignore  # noqa: E501
        assert kwargs["params"]["offset_key"] == "test"
        assert (
            kwargs["query"]
            == "select coalesce(any_value(last_offset), %(default_offset)s) as last_offset\n    from public.sql_offset_state\n    where sql_folder_name = %(offset_key)s"  # noqa: E501
        )

    @task()
    async def fake_task_upsert(*args, **kwargs):
        assert pendulum.parser.parse(kwargs["params"]["new_offset"]).is_utc()  # type: ignore  # noqa: E501
        assert kwargs["params"]["offset_key"] == "test"
        assert (
            kwargs["query"]
            == "\n    merge into sql_offset_state dt using (\n    select %(offset_key)s as sql_folder_name, \n    current_timestamp as created_at, \n    current_timestamp as updated_at,\n    %(new_offset)s as last_offset\n    ) st on st.sql_folder_name = dt.sql_folder_name\n    when matched then update \n    set updated_at = st.updated_at,\n        last_offset = st.last_offset\n    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) \n    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset)"  # noqa: E501
        )

    @flow()
    async def fake_flow():
        sfc = MozSnowflakeConnector()
        monkeypatch.setattr("shared.offset_utils.snowflake_query", fake_task_get)
        await get_last_offset(snowflake_connector=sfc, offset_key="test")
        monkeypatch.setattr("shared.offset_utils.snowflake_query", fake_task_upsert)
        await upsert_new_offset(
            snowflake_connector=sfc,
            offset_key="test",
            new_offset=pendulum.now().subtract(hours=1).to_datetime_string(),
        )

    await fake_flow()
