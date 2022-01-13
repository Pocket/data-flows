import json
from datetime import datetime

import boto3
import pandas as pd
import pytz
from pandas import DataFrame
from prefect import task, Flow
from sagemaker.feature_store.feature_group import FeatureGroup, IngestionManagerPandas
from sagemaker.session import Session

from src.api_clients.prefect_key_value_store_client import set_kv, get_kv
# Setting the working directory to project root makes the path start at "src"
from src.api_clients import snowflake_client

FLOW_NAME = "PreReview Engagement to Feature Group Flow1"
FEATURE_GROUP_NAME = "prereview-engagement-metrics"

@task
def get_last_executed_value(flow_name: str, default_if_absent='2000-01-01 00:00:00') -> datetime:
    """
    Query Prefect KV Store to get the execution date from previous Flow state

    Args:
        - flow_name: The name of the flow in Prefect Cloud to fetch metadata from
        - default_if_absent: The date to use as the last executed date if it is absent from the metadata, which will allow this flow to run the first time targeting a new Prefect Cloud env.

    Returns:
    'last_executed_date' from the json metadata that represents the most recent execution date before right now

    """
    default_state_params_json = json.dumps({'last_executed': default_if_absent})
    state_params_json = get_kv(flow_name, default_state_params_json)
    last_executed = json.loads(state_params_json).get('last_executed')
    return datetime.strptime(last_executed, "%Y-%m-%d %H:%M:%S")

@task
def update_last_executed_value(for_flow: str, default_if_absent='2000-01-01 00:00:00') -> None:
    """
     Does the following:
     - Increments the execution date by a variable amount, passed in via the named parameters to timedelta like days, hours, and seconds: Represents the next run data for the Flow
     - Updates the Prefect KV Store to set the 'last_executed' with the next execution date

     Args:
        - for_flow: The name of the flow in Prefect Cloud to write metadata to
        - default_if_absent: The date to use as the last executed date if it isn't specified. THIS RESETS THE FLOW to fetch every record from the table!!

     Returns:
     The next execution date
     """
    default_state_params_json = json.dumps({'last_executed': default_if_absent,})
    state_params_json = get_kv(for_flow, default_state_params_json)

    state_params_dict = json.loads(state_params_json)

    now = datetime.now()
    timezone = pytz.utc
    now_pacific_time = timezone.localize(now)
    state_params_dict['last_executed'] = now_pacific_time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"Set last executed time to: {state_params_dict['last_executed']}")
    set_kv(for_flow, json.dumps(state_params_dict))

@task
def extract_from_snowflake(flow_last_executed: datetime) -> DataFrame:
    """
    Pull data from snowflake materialized tables and save it to a dataframe.

    Args:
    - flow_last_executed: The earliest date for which we'd like to pull updates to this table.

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    prereview_engagement_sql = f"""
            select
                RESOLVED_ID_TIME_ADDED_FEED_ID_KEY as ID,
                to_varchar(updated_at,'yyyy-MM-dd"T"HH:mm:ssZ') as UPDATED_AT,
                to_varchar(time_added,'yyyy-MM-dd"T"HH:mm:ssZ') as TIME_ADDED,
                RESOLVED_ID as RESOLVED_ID,
                FEED_ID as FEED_ID,
                STATUS as STATUS,
                TYPE as TYPE,
                RESOLVED_URL as RESOLVED_URL,
                DAY1_SAVE_COUNT as DAY1_SAVE_COUNT,
                DAY2_SAVE_COUNT as DAY2_SAVE_COUNT,
                DAY3_SAVE_COUNT as DAY3_SAVE_COUNT,
                DAY4_SAVE_COUNT as DAY4_SAVE_COUNT,
                DAY5_SAVE_COUNT as DAY5_SAVE_COUNT,
                DAY6_SAVE_COUNT as DAY6_SAVE_COUNT,
                DAY7_SAVE_COUNT as DAY7_SAVE_COUNT,
                WEEK1_SAVE_COUNT as WEEK1_SAVE_COUNT,
                WEEK1_OPEN_COUNT as WEEK1_OPEN_COUNT,
                WEEK1_SHARE_COUNT as WEEK1_SHARE_COUNT,
                WEEK1_FAVORITE_COUNT as WEEK1_FAVORITE_COUNT
            from analytics.dbt.pre_curated_reading_metrics
            where updated_at > %s
            ;
        """

    query_result = snowflake_client.get_query().run(query=prereview_engagement_sql, data=(flow_last_executed,))
    df = pd.DataFrame(query_result)
    return df

@task
def dataframe_to_feature_group(dataframe: pd.DataFrame, feature_group_name: str) -> IngestionManagerPandas :
    """
    Update SageMaker feature group.

    Args:
        df : the data in a dataframe to upload to the feature group
        feature_group_name: the name of the feature group to upload the data to

    Returns:
        EITHER -
        success case: the success message from the feature group API
        failure case: a description of the feature group as it currently stands for debugging
    """
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    return feature_group.ingest(data_frame=dataframe, max_workers=4, max_processes=4, wait=True)

with Flow(FLOW_NAME) as flow:
    promised_get_last_executed_flow_result = get_last_executed_value(flow_name=FLOW_NAME)

    # this variable name is used in testing
    promised_update_last_executed_flow_result = update_last_executed_value(for_flow=FLOW_NAME)

    promised_extract_from_snowflake_result = extract_from_snowflake(flow_last_executed=promised_get_last_executed_flow_result)
    promised_dataframe_to_feature_group_result = dataframe_to_feature_group(dataframe=promised_extract_from_snowflake_result, feature_group_name=FEATURE_GROUP_NAME)

if __name__ == "__main__":
    flow.run()
