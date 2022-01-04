import json
import os
from datetime import datetime, timedelta
from typing import cast

import pandas as pd
from prefect import task, Flow

import boto3
from prefect.tasks.secrets import PrefectSecret
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

import prefect
from prefect.run_configs import ECSRun

# Setting the working directory to project root makes the path "src.lib.queries" for get_snowflake_query
from src.lib.queries import get_snowflake_query
from src.lib.kv_utils import set_kv, get_kv

FLOW_NAME = "PreReview Engagement to Feature Group Flow"

# Using Prefect KV Store to capture state parameters
STATE_PARAMETERS = {
    'last_executed': '2021-12-01 00:00:00',
}

@task
def extract(last_executed_date):
    """
    Pull data from snowflake materialized tables.

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    set_kv(FLOW_NAME, json.dumps(STATE_PARAMETERS))
    prereview_engagement_sql = f"""
        select
            RESOLVED_ID_TIME_ADDED_KEY::string as ID,
            to_varchar(time_added,'yyyy-MM-dd"T"HH:mm:ssZ')::string as UNLOADED_AT,
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
        from analytics.dbt_gkatre.pre_curated_reading_metrics
        where time_added = '{last_executed_date}'
        ;
    """
    query_result =  get_snowflake_query().run(query=prereview_engagement_sql)
    df = pd.DataFrame(query_result, columns=['ID', 'UNLOADED_AT', 'RESOLVED_ID', 'RESOLVED_URL',
                                               '7_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '6_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '5_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '4_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '3_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '2_DAYS_PRIOR_CUMULATIVE_SAVE_COUNT',
                                               '1_DAYS_PRIOR_SAVE_COUNT',
                                               'ALL_TIME_SAVE_COUNT',
                                               'ALL_TIME_OPEN_COUNT',
                                               'ALL_TIME_SHARE_COUNT',
                                               'ALL_TIME_FAVORITE_COUNT',
                                               'VERSION',
                                               ])
    print(f'Row Count: {len(df)}')
    return df

@task
def load(df: pd.DataFrame, feature_group_name):
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
    try:
        result = feature_group.ingest(data_frame=df, max_workers=4, max_processes=4, wait=True)
    except:
        # This assumes that the feature group already exists.
        # If it doesn't, you'll need to create it in AWS. We have a notebook you can repurpose CreateFeatureGroupForPrefect.ipynb in Chelsea's Sagemaker Studio in dev
        result = feature_group.describe()
    return result

@task
def print_results(results, text):
    """
    Print results.
    """
    print(f'{text}{results}')

@task
def get_last_executed():
    """
    Query Prefect KV Store to get Flow state parameters.

    Returns:
    'start_data' from the json metadata that represents the execution date

    TODO: Consider a more suitable name instead of 'last_executed'...perhaps 'run_date'?
    """
    default_state_params_json = json.dumps({
    'last_executed': '2021-12-01 00:00:00',
})
    state_params_json = get_kv(FLOW_NAME, default_state_params_json)
    last_executed = json.loads(state_params_json).get('last_executed')
    return datetime.strptime(last_executed, "%Y-%m-%d %H:%M:%S")

@task
def increment_set_next_execution_date(last_executed_date, **kwargs):
    """
     Does the following:
     - Increments the execution date by a variable amount, passed in via the named parameters to timedelta like days, hours, and seconds: Represents the next run data for the Flow
     - Updates the Prefect KV Store to set the 'last_executed' with the next execution date

     Returns:
     The next execution date
     """
    default_state_params_json = json.dumps({
    'last_executed': '2021-12-01 00:00:00',
})
    state_params_json = get_kv(FLOW_NAME, default_state_params_json)

    last_executed_date += timedelta(**kwargs)
    state_params_dict = json.loads(state_params_json)
    state_params_dict['last_executed'] = last_executed_date.strftime('%Y-%m-%d %H:%M:%S')

    set_kv(FLOW_NAME, json.dumps(state_params_dict))
    default_state_params_json = json.dumps({
    'last_executed': '2021-12-01 00:00:00',
})
    state_params_json = get_kv(FLOW_NAME, default_state_params_json)
    last_executed = json.loads(state_params_json).get('last_executed')
    return datetime.strptime(last_executed, "%Y-%m-%d %H:%M:%S")


with Flow(FLOW_NAME) as flow:
    """
     The Flow:
     - get_execution_date(): Get the execution date from pervious Flow state stored in Prefect KV Store to apply for 
     data extraction
     - extract(): Extract Snowflake data for ingestion
     - load(): Load extracted data to SageMaker Feature store
     - increment_set_next_execution_date(): Set the next execution data in the Prefect KV Store to be used for the 
     next Flow execution
     """
    last_executed = get_last_executed()
    print_results(last_executed, 'Last Executed: ')
    df = extract(last_executed)
    result = load(df, 'new-tab-prospect-modeling-data')
    print_results(result, 'Feature Group load response: ')
    next_execution_date = increment_set_next_execution_date(last_executed)
    print_results(next_execution_date, 'Next Execution Date: ')

flow.run()

#MARK: Deployment

# flow.storage = prefect.storage.S3(
#     bucket=f"pocket-dataflows-storage-{ os.getenv('DEPLOYMENT_ENVIRONMENT') }",
#     add_default_labels=False
# )
#
# flow.run_config = ECSRun(
#     # task_definition_path="test.yaml",
#     labels=['Dev'],
#     task_role_arn=PrefectSecret('PREFECT_TASK_ROLE_ARN').run(),
#     # execution_role_arn='arn:aws:iam::12345678:role/prefect-ecs',
#     image='prefecthq/prefect:latest-python3.9',
# )
#
# flow.register(project_name="engagement-data")
