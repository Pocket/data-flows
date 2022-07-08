import datetime
import pandas as pd
import boto3
import re

from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import merge

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

DAY_IN_MINUTES = 1440
QUERY_PARAMS = {
    "PROSPECT_SURFACE_GUID": "NEW_TAB_EN_US",
    "SURFACE_START_DATE": "2022-05-30",
    "MAX_INC_AGE": 1,
    "MAX_BACKFILL_AGE": 360,
    "PROSPECT_LEGACY_FEED_ID": 1
}

FLOW_NAME = get_flow_name(__file__)


def create_global_prospect_record(row: pd.Series):
    now = datetime.datetime.utcnow()
    now_as_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    regex = re.compile("[^a-zA-Z 0-9]")
    record = [{"FeatureName": c,
               "ValueAsString": regex.sub("", str(row[c]))} for c in row.index]
    record.extend([{"FeatureName": "UNLOADED_AT", "ValueAsString": now_as_string},
                   {"FeatureName": "VERSION", "ValueAsString": "0.1.0"}]
                  )
    return record


@task()
def check_refresh(refresh: bool):
    return refresh == True

@task()
def load_query(query_file: str) -> str:
    print(f"loading query from {query_file}")
    with open(query_file, "r") as fp:
        query = fp.read()
    return query

@task()
def load_feature_record(prospects_df: pd.DataFrame, feature_group_name: str):
    fs_client = boto3.client('sagemaker-featurestore-runtime', region_name='us-east-1')

    nitems = prospects_df.RESOLVED_ID.nunique()
    print(f"update includes {len(prospects_df)} prospects ({nitems})")
    responses = list()
    for i, row in prospects_df.iterrows():
        responses.append(fs_client.put_record(FeatureGroupName=feature_group_name,
                                              Record=create_global_prospect_record(row)))


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=DAY_IN_MINUTES)) as flow:
# with Flow(FLOW_NAME) as flow:  # to run in production in pycharm need to disable schedule

    print(f"Starting flow {FLOW_NAME}")
    # full_refresh if feature group needs to be rebuilt from scratch, e.g. schema change
    full_refresh = Parameter("full_refresh", default=False)

    training_feature_group = Parameter("feature group",
                                       default=f"{config.ENVIRONMENT}-global-prospect-modeling-data-v1")

    refresh_query_file = Parameter("refresh_query_file",
                                   default="global_training_full.sql")

    inc_query_file = Parameter("inc_query_file",
                               default="global_training_inc.sql")

    refresh = check_refresh(full_refresh)
    print(refresh)

    with case(refresh, True):
        query_r = load_query(refresh_query_file)

    with case(refresh, False):
        query_i = load_query(inc_query_file)

    query = merge(query_i, query_r)

    reviewed_prospects = PocketSnowflakeQuery()(
        query=query,
        data=QUERY_PARAMS,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DATA_FRAME,
    )

    load_feature_record(reviewed_prospects, feature_group_name=training_feature_group)

if __name__ == "__main__":
    flow.run(parameters=dict(full_refresh=False))
