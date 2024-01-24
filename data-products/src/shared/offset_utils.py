from typing import Any

import pendulum
from prefect_snowflake.database import SnowflakeConnector, snowflake_query

GET_OFFSET_QUERY = """select coalesce(any_value(last_offset), %(default_offset)s) as last_offset
    from public.sql_offset_state
    where sql_folder_name = %(offset_key)s"""  # noqa: E501

UPDATE_OFFSET_SQL = """
    merge into sql_offset_state dt using (
    select %(offset_key)s as sql_folder_name, 
    current_timestamp as created_at, 
    current_timestamp as updated_at,
    %(new_offset)s as last_offset
    ) st on st.sql_folder_name = dt.sql_folder_name
    when matched then update 
    set updated_at = st.updated_at,
        last_offset = st.last_offset
    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) 
    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset)"""


async def get_last_offset(
    offset_key: str,
    snowflake_connector: SnowflakeConnector,
    default_offset: str = pendulum.now().subtract(hours=1).to_datetime_string(),
    **kwargs
) -> Any:
    return await snowflake_query(
        query=GET_OFFSET_QUERY,
        params={"offset_key": offset_key, "default_offset": default_offset},
        snowflake_connector=snowflake_connector,
        **kwargs
    )


async def upsert_new_offset(
    offset_key: str, new_offset: str, snowflake_connector: SnowflakeConnector, **kwargs
) -> Any:
    return await snowflake_query(
        query=UPDATE_OFFSET_SQL,
        params={"new_offset": new_offset, "offset_key": offset_key},
        snowflake_connector=snowflake_connector,
        **kwargs
    )
