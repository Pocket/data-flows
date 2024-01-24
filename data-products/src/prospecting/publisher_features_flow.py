from pathlib import Path

import pandas as pd
from common import get_script_path
from common.databases.snowflake_utils import MozSnowflakeConnector, query_to_dataframe
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CS
from prefect import flow, task
from shared.feature_store import dataframe_to_feature_group

VERSION = 1

# Setting variables used for the flow

FEATURE_ENV_MAP = {"production": "production", "dev": "development"}

FEATURE_GROUP_NAME = (
    f"{FEATURE_ENV_MAP[CS.dev_or_production]}-PublisherFeatureGroup-v{VERSION}"
)


@task()
def transform_publisher_features(input_df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {
        "TOP_DOMAIN_NAME",
        "LOG_TOTAL_SAVES",
        "TOTAL_SAVE_COUNT",
        "UPDATED_AT",
    }
    if len(required_columns.difference(set(input_df.columns))) > 0:
        missing = required_columns.difference(set(input_df.columns))
        raise KeyError(f"input dataframe is missing columns: {missing}")

    # TOP_DOMAIN_NAME is required key for feature group
    input_df = input_df.dropna(subset=["TOP_DOMAIN_NAME"])
    # reformat update column
    input_df["UPDATED_AT"] = input_df.UPDATED_AT.apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    # remove extra column from query which breaks feature group ingestion
    input_df = input_df.drop(columns="APPROVED_SOURCE")

    # deal with rows for sources without approved items
    approved_cols = [c for c in input_df.columns if c.startswith("NUM_APPROVED")]
    for c in approved_cols:
        input_df[c] = input_df[c].fillna(0).astype(int)

    return input_df


@flow()
async def publisher_features():
    sfc = MozSnowflakeConnector()

    # create query parameters
    query_params = {"MAX_APPROVED_AGE": 360, "MIN_WORD_COUNT": 900}

    # get script path
    script_path = get_script_path()

    # get sql query
    sql = Path(script_path, "sql/publisher_features.sql").read_text()

    # query data from snowflake
    sf_data_results = await query_to_dataframe(
        query=sql,
        snowflake_connector=sfc,
        params=query_params,
    )

    # transform data
    publisher_df = transform_publisher_features(sf_data_results)
    # load data
    await dataframe_to_feature_group(
        dataframe=publisher_df, feature_group_name=FEATURE_GROUP_NAME
    )


FLOW_SPEC = FlowSpec(
    flow=publisher_features,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="publisher_features",
            cron="0 7 * * *",
            timezone="America/Chicago",
            job_variables={
                "cpu": 2048,
                "memory": 4096,
            },
        ),
    ],
)


if __name__ == "__main__":
    import asyncio

    asyncio.run(publisher_features())  # type: ignore
