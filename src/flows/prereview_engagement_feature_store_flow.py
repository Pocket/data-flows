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
from src.api_clients.prefect_key_value_store_client import get_last_executed_value, update_last_executed_value

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
                RESOLVED_ID_TIME_ADDED_FEED_ID_KEY::string as ID,
                to_varchar(time_added,'yyyy-MM-dd"T"HH:mm:ssZ')::string as UNLOADED_AT,
                to_varchar(time_updated,'yyyy-MM-dd"T"HH:mm:ssZ')::string as TIME_UPDATED,
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
                WEEK1_FAVORITE_COUNT::integer as ALL_TIME_FAVORITE_COUNT
            from analytics.dbt.pre_curated_reading_metrics
            where time_updated > %s
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

FLOW_NAME = "PreReview Engagement to Feature Group Flow"
with Flow(FLOW_NAME) as flow:
    promised_get_last_executed_flow_result = get_last_executed_value(flow_name=FLOW_NAME)

    # this variable name is used in testing
    promised_update_last_executed_flow_result = update_last_executed_value(for_flow=FLOW_NAME)

    promised_extract_from_snowflake_result = extract_from_snowflake(flow_last_executed=promised_get_last_executed_flow_result)
    promised_dataframe_to_feature_group_result = dataframe_to_feature_group(dataframe=promised_extract_from_snowflake_result, feature_group_name='new-tab-prospect-modeling-data')

flow.run()
