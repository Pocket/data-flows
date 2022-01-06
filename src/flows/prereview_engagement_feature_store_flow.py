import json
from datetime import datetime

import boto3
import pandas as pd
import pytz
from prefect import task, Flow
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

from src.lib.kv_utils import set_kv, get_kv
# Setting the working directory to project root makes the path "src.lib.queries" for get_snowflake_query
from src.lib.queries import get_snowflake_query


@task
def prereview_engagement_from_snowflake_to_dataframe(flow_last_executed):
    """
    Pull data from snowflake materialized tables

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    prereview_engagement_sql = f"""
            select
                RESOLVED_ID_TIME_ADDED_KEY::string as ID,
                to_varchar(time_added,'yyyy-MM-dd"T"HH:mm:ssZ')::string as UNLOADED_AT,
                --to_varchar(time_updated,'yyyy-MM-dd"T"HH:mm:ssZ')::string as TIME_UPDATED,
                RESOLVED_ID::string as RESOLVED_ID,
                'null'::string as RESOLVED_URL,
                DAY7_SAVE_COUNT::integer as "7_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY6_SAVE_COUNT::integer as "6_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY5_SAVE_COUNT::integer as "5_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY4_SAVE_COUNT::integer as "4_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY3_SAVE_COUNT::integer as "3_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY2_SAVE_COUNT::integer as "2_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT",
                DAY1_SAVE_COUNT::integer as "1_DAYS_PRIOR_SAVE_COUNT",
                WEEK1_SAVE_COUNT::integer as ALL_TIME_SAVE_COUNT,
                WEEK1_OPEN_COUNT::integer as ALL_TIME_OPEN_COUNT,
                WEEK1_SHARE_COUNT::integer as ALL_TIME_SHARE_COUNT,
                WEEK1_FAVORITE_COUNT::integer as ALL_TIME_FAVORITE_COUNT,
                '1.1'::string as VERSION
            from analytics.dbt.pre_curated_reading_metrics
            where time_added > '{flow_last_executed}'
            ;
        """

    print(f"RUNNING {prereview_engagement_sql}")
    query_result =  get_snowflake_query().run(query=prereview_engagement_sql)
    df = pd.DataFrame(query_result)
    return df

@task
def dataframe_to_feature_group(df: pd.DataFrame, feature_group_name: str):
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
    return feature_group.ingest(data_frame=df, max_workers=4, max_processes=4, wait=True)

@task
def get_last_executed_value(flow_name, default_if_absent='2000-01-01 00:00:00'):
    """
    Query Prefect KV Store to get the execution date from previous Flow state
     data extraction

    Returns:
    'start_data' from the json metadata that represents the execution date

    """
    default_state_params_json = json.dumps({'last_executed': default_if_absent})
    state_params_json = get_kv(flow_name, default_state_params_json)
    last_executed = json.loads(state_params_json).get('last_executed')
    return datetime.strptime(last_executed, "%Y-%m-%d %H:%M:%S")

@task
def update_last_executed_value(for_flow, default_if_absent='2000-01-01 00:00:00'):
    """
     Does the following:
     - Increments the execution date by a variable amount, passed in via the named parameters to timedelta like days, hours, and seconds: Represents the next run data for the Flow
     - Updates the Prefect KV Store to set the 'last_executed' with the next execution date

     Returns:
     The next execution date
     """
    default_state_params_json = json.dumps({'last_executed': default_if_absent,})
    state_params_json = get_kv(for_flow, default_state_params_json)

    state_params_dict = json.loads(state_params_json)

    now = datetime.now()
    timezone = pytz.timezone("America/Los_Angeles")
    now_pacific_time = timezone.localize(now)
    state_params_dict['last_executed'] = now_pacific_time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"Set last executed time to: {state_params_dict['last_executed']}")
    set_kv(for_flow, json.dumps(state_params_dict))

FLOW_NAME = "PreReview Engagement to Feature Group Flow"
with Flow(FLOW_NAME) as flow:
    flow_last_executed = get_last_executed_value(flow_name=FLOW_NAME)
    dataframe = prereview_engagement_from_snowflake_to_dataframe(flow_last_executed=flow_last_executed)
    result = dataframe_to_feature_group(df=dataframe, feature_group_name='new-tab-prospect-modeling-data')
    update_last_executed_value(for_flow=FLOW_NAME)

flow.run()
