"""The Extract Data Module
This module holds common data extract tasks (part of the ETL process) used by Prefect.
"""
from datetime import datetime
from pandas import DataFrame
import pandas as pd
from src.api_clients import snowflake_client
from prefect import task
import prefect


class NoDataFromPastDayException(Exception):
    def __init__(self):
        super().__init__("Prefect has received no new click data for the past day. There's probably a problem.")

@task
def extract_from_snowflake(flow_last_executed: datetime, query: str) -> DataFrame:
    """
    Pull data from snowflake materialized tables and save it to a dataframe.

    Args:
    - flow_last_executed: The earliest date for which we'd like to pull updates to this table.

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    logger = prefect.context.get("logger")
    query_result = snowflake_client.get_query().run(query=query, data=(flow_last_executed,))
    df = pd.DataFrame(query_result)
    logger.info(f'Row Count: {len(df)}')
    if not len(df) and (datetime.utcnow() - flow_last_executed).seconds / 3600 > 23:
        raise NoDataFromPastDayException()
    return df
