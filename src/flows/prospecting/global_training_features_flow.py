import datetime
import pandas as pd
import boto3

from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import merge

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

DAY_IN_MINUTES = 1440
FLOW_NAME = get_flow_name(__file__)

def create_global_prospect_record(row: pd.Series):
    now = datetime.datetime.utcnow()
    now_as_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    record = [{"FeatureName": c,
               "ValueAsString": str(row[c])} for c in row.index]
    record.extend([{"FeatureName": "UNLOADED_AT", "ValueAsString": now_as_string},
                   {"FeatureName": "VERSION", "ValueAsString": "0.1.0"}]
                  )
    return record

def load_query(query_file: str) -> str:
    print("loading inc query")
    with open(query_file, "r") as fp:
        query = fp.read()
    return query


@task()
def check_refresh(refresh: bool):
    return refresh == True

@task()
def load_refresh_query():
    return load_query("global_training_full.sql")

@task()
def load_inc_query():
    return load_query("global_training_inc.sql")

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

    # full_refresh if feature group needs to be rebuilt from scratch, e.g. schema change
    full_refresh = Parameter("full_refresh", default=False)

    training_feature_group = Parameter("feature group",
                                       default=f"{config.ENVIRONMENT}-global-prospect-modeling-data-v1")

    query_params = {
        "PROSPECT_SURFACE_GUID": "NEW_TAB_EN_US",
        "SURFACE_START_DATE": "2022-05-30",
        "MAX_INC_AGE": 1,
        "MAX_BACKFILL_AGE": 360,
        "PROSPECT_LEGACY_FEED_ID": 1
    }

    refresh = check_refresh(full_refresh)
    print(refresh)

    with case(refresh, True):
        query_r = load_refresh_query()

    with case(refresh, False):
        query_i = load_inc_query()

    query = merge(query_i, query_r)

    reviewed_prospects = PocketSnowflakeQuery()(
        query=query,
        data=query_params,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DATA_FRAME,
    )

    load_feature_record(reviewed_prospects, feature_group_name=training_feature_group)

if __name__ == "__main__":
    flow.run(parameters=dict(full_refresh=True))
