import os
import pandas as pd
from pathlib import Path

import prefect
from prefect import Flow, task
from prefect.executors import LocalDaskExecutor

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

ONE_DAYS_MINS = 24*60  # using this to schedule flow update interval
VERSION = 1

FEATURE_GROUP_NAME = f"{config.ENVIRONMENT}-PublisherFeatureGroup-v{VERSION}"

FLOW_NAME = get_flow_name(__file__)

@task()
def transform_publisher_features(input_df: pd.DataFrame) -> pd.DataFrame:

    required_columns = {"TOP_DOMAIN_NAME", "LOG_TOTAL_SAVES", "TOTAL_SAVE_COUNT", "UPDATED_AT"}
    if len(required_columns.difference(set(input_df.columns))) > 0:
        missing = required_columns.difference(set(input_df.columns))
        raise KeyError(f"input dataframe is missing columns: {missing}")

    # TOP_DOMAIN_NAME is required key for feature group
    input_df = input_df.dropna(subset=["TOP_DOMAIN_NAME"])
    # reformat update column
    input_df["UPDATED_AT"] = input_df.UPDATED_AT.apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
    # remove extra column from query
    input_df = input_df.drop(columns="APPROVED_SOURCE")

    # deal with rows for sources without approved items
    approved_cols = [c for c in input_df.columns if c.startswith("NUM_APPROVED")]
    for c in approved_cols:
        input_df[c] = input_df[c].fillna(0).astype(int)

    return input_df


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=ONE_DAYS_MINS), executor=LocalDaskExecutor()) as flow:

    logger = prefect.context.get("logger")
    logger.info(f'Querying snowflake for publisher data')

    # Read a query from the sql/ directory located next to this flow file.
    sql_path = os.path.join(os.path.dirname(__file__), "sql/publisher_features.sql")
    sql_query = Path(sql_path).read_text()

    # create query parameters
    query_params = {"MAX_APPROVED_AGE": 360,
                    "MIN_WORD_COUNT": 900}

    # retrieve data from snowflake
    snowflake_result = PocketSnowflakeQuery().run(
        query=sql_query,
        data=query_params,
        output_type=OutputType.DATA_FRAME,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
    )
    print(f"snowflake query returned {len(snowflake_result)} rows")

    # transform data
    publisher_df = transform_publisher_features.run(snowflake_result)

    # load data
    result = dataframe_to_feature_group(
        dataframe=publisher_df,
        feature_group_name=FEATURE_GROUP_NAME
    )

if __name__ == "__main__":
    flow.run()