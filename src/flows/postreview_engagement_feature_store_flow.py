from datetime import datetime

import boto3
import pandas as pd
from pandas import DataFrame
from prefect import task, Flow
from sagemaker.feature_store.feature_group import FeatureGroup, IngestionManagerPandas
from sagemaker.session import Session

from src.api_clients.prefect_key_value_store_client import get_last_executed_value, update_last_executed_value
# Setting the working directory to project root makes the path start at "src"
from src.api_clients import snowflake_client

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
            to_varchar(TIME_LIVE,'yyyy-MM-dd"T"HH:mm:ssZ')::string as TIME_LIVE,
            to_varchar(UPDATED_AT,'yyyy-MM-dd"T"HH:mm:ssZ')::string as UPDATED_AT,
            FEED_ID,
            PROSPECT_ID,
            RESOLVED_ID,
            TITLE,
            STATUS,
            CURATOR,
            NEWTAB_IMPRESSIONS_FIRST_DAY,
            NEWTAB_IMPRESSIONS_FIRST_WEEK,
            NEWTAB_IMPRESSIONS_FIRST_MONTH,
            NEWTAB_OPENS_FIRST_DAY,
            NEWTAB_OPENS_FIRST_WEEK,
            NEWTAB_OPENS_FIRST_MONTH,
            POCKET_APP_SAVES_FIRST_DAY,
            POCKET_APP_SAVES_FIRST_WEEK,
            POCKET_APP_SAVES_FIRST_MONTH,
            POCKET_APP_OPENS_FIRST_DAY,
            POCKET_APP_OPENS_FIRST_WEEK,
            POCKET_APP_OPENS_FIRST_MONTH,
            RECS_SURFACES_SAVES_FIRST_DAY,
            RECS_SURFACES_SAVES_FIRST_WEEK,
            RECS_SURFACES_SAVES_FIRST_MONTH,
            RECS_SURFACES_OPENS_FIRST_DAY,
            RECS_SURFACES_OPENS_FIRST_WEEK,
            RECS_SURFACES_OPENS_FIRST_MONTH,
            POCKET_APP_TIMESPEND_FIRST_DAY,
            POCKET_APP_TIMESPEND_FIRST_WEEK,
            POCKET_APP_TIMESPEND_FIRST_MONTH,
            TIME_PERIOD_TOTAL_NEWTAB_SAVES,
            TIME_PERIOD_TOTAL_NEWTAB_DISMISSALS            
        from analytics.dbt.all_surfaces_engagements_past_30_day_aggregations
        where UPDATED_AT > %s
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

FLOW_NAME = "PostReview Engagement to Feature Group Flow"
with Flow(FLOW_NAME) as flow:
    promised_get_last_executed_flow_result = get_last_executed_value(flow_name=FLOW_NAME)
    promised_extract_from_snowflake_result = extract_from_snowflake(flow_last_executed=promised_get_last_executed_flow_result)
    promised_dataframe_to_feature_group_result = dataframe_to_feature_group(dataframe=promised_extract_from_snowflake_result, feature_group_name='postreview-enagement-aggregate-metrics')
    promised_update_last_executed_flow_result = update_last_executed_value(for_flow=FLOW_NAME)

flow.run()
