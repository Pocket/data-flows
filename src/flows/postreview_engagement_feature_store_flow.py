import os

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

@task
def extract():
    """
    Pull data from snowflake materialized tables.

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    postreview_engagement_sql = """
        select
            to_varchar(TIME_LIVE,'yyyy-MM-dd"T"HH:mm:ssZ')::string as TIME_LIVE,
            PROSPECT_ID::integer as PROSPECT_ID,
            RESOLVED_ID::string as RESOLVED_ID,
            TITLE::string as TITLE,
            STATUS::string as STATUS,
            CURATOR::string as CURATOR,
            NEWTAB_IMPRESSIONS_FIRST_DAY::integer as NEWTAB_IMPRESSIONS_FIRST_DAY,
            NEWTAB_IMPRESSIONS_FIRST_WEEK::integer as NEWTAB_IMPRESSIONS_FIRST_WEEK,
            NEWTAB_IMPRESSIONS_FIRST_MONTH::integer as NEWTAB_IMPRESSIONS_FIRST_MONTH,
            NEWTAB_OPENS_FIRST_DAY::integer as NEWTAB_OPENS_FIRST_DAY,
            NEWTAB_OPENS_FIRST_WEEK::integer as NEWTAB_OPENS_FIRST_WEEK,
            NEWTAB_OPENS_FIRST_MONTH::integer as NEWTAB_OPENS_FIRST_MONTH,
            POCKET_APP_SAVES_FIRST_DAY::integer as POCKET_APP_SAVES_FIRST_DAY,
            POCKET_APP_SAVES_FIRST_WEEK::integer as POCKET_APP_SAVES_FIRST_WEEK,
            POCKET_APP_SAVES_FIRST_MONTH::integer as POCKET_APP_SAVES_FIRST_MONTH,
            POCKET_APP_OPENS_FIRST_DAY::integer as POCKET_APP_OPENS_FIRST_DAY,
            POCKET_APP_OPENS_FIRST_WEEK::integer as POCKET_APP_OPENS_FIRST_WEEK,
            POCKET_APP_OPENS_FIRST_MONTH::integer as POCKET_APP_OPENS_FIRST_MONTH,
            RECS_SURFACES_SAVES_FIRST_DAY::integer as RECS_SURFACES_SAVES_FIRST_DAY,
            RECS_SURFACES_SAVES_FIRST_WEEK::integer as RECS_SURFACES_SAVES_FIRST_WEEK,
            RECS_SURFACES_SAVES_FIRST_MONTH::integer as RECS_SURFACES_SAVES_FIRST_MONTH,
            RECS_SURFACES_OPENS_FIRST_DAY::integer as RECS_SURFACES_OPENS_FIRST_DAY,
            RECS_SURFACES_OPENS_FIRST_WEEK::integer as RECS_SURFACES_OPENS_FIRST_WEEK,
            RECS_SURFACES_OPENS_FIRST_MONTH::integer as RECS_SURFACES_OPENS_FIRST_MONTH,
            POCKET_APP_TIMESPEND_FIRST_DAY::string as POCKET_APP_TIMESPEND_FIRST_DAY,
            POCKET_APP_TIMESPEND_FIRST_WEEK::string as POCKET_APP_TIMESPEND_FIRST_WEEK,
            POCKET_APP_TIMESPEND_FIRST_MONTH::string as POCKET_APP_TIMESPEND_FIRST_MONTH,
            TIME_PERIOD_TOTAL_NEWTAB_SAVES::integer as TIME_PERIOD_TOTAL_NEWTAB_SAVES,
            TIME_PERIOD_TOTAL_NEWTAB_DISMISSALS::integer as TIME_PERIOD_TOTAL_NEWTAB_DISMISSALS,            
            '1.1'::string as VERSION
        from analytics.dbt_ctroy.all_surfaces_engagements_past_30_day_aggregations
        limit 10;
    """
    query_result = get_snowflake_query().run(query=postreview_engagement_sql)
    return pd.DataFrame(query_result, columns=[
        "TIME_LIVE",
        "PROSPECT_ID",
        "RESOLVED_ID",
        "TITLE",
        "STATUS",
        "CURATOR",
        "NEWTAB_IMPRESSIONS_FIRST_DAY",
        "NEWTAB_IMPRESSIONS_FIRST_WEEK",
        "NEWTAB_IMPRESSIONS_FIRST_MONTH",
        "NEWTAB_OPENS_FIRST_DAY",
        "NEWTAB_OPENS_FIRST_WEEK",
        "NEWTAB_OPENS_FIRST_MONTH",
        "POCKET_APP_SAVES_FIRST_DAY",
        "POCKET_APP_SAVES_FIRST_WEEK",
        "POCKET_APP_SAVES_FIRST_MONTH",
        "POCKET_APP_OPENS_FIRST_DAY",
        "POCKET_APP_OPENS_FIRST_WEEK",
        "POCKET_APP_OPENS_FIRST_MONTH",
        "RECS_SURFACES_SAVES_FIRST_DAY",
        "RECS_SURFACES_SAVES_FIRST_WEEK",
        "RECS_SURFACES_SAVES_FIRST_MONTH",
        "RECS_SURFACES_OPENS_FIRST_DAY",
        "RECS_SURFACES_OPENS_FIRST_WEEK",
        "RECS_SURFACES_OPENS_FIRST_MONTH",
        "POCKET_APP_TIMESPEND_FIRST_DAY",
        "POCKET_APP_TIMESPEND_FIRST_WEEK",
        "POCKET_APP_TIMESPEND_FIRST_MONTH",
        "TIME_PERIOD_TOTAL_NEWTAB_SAVES",
        "TIME_PERIOD_TOTAL_NEWTAB_DISMISSALS",
        "VERSION",
    ])


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
                                    sagemaker_featurestore_runtime_client=boto_session.client(
                                        service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    try:
        result = feature_group.ingest(data_frame=df, max_workers=4, max_processes=4, wait=True)
    except:
        # This assumes that the feature group already exists.
        # If it doesn't, you'll need to create it in AWS. We have a notebook you can repurpose CreateFeatureGroupForPrefect.ipynb in Chelsea's Sagemaker Studio in dev
        result = feature_group.describe()
    return result


@task
def print_results(results):
    print(results)

with Flow("PostReview Engagement to Feature Group Flow") as flow:
    df = extract()
    print_results(df['RESOLVED_ID'])  #This is the record identifier in the feature group and can be used to check if a record is indeed in the feature group
    result = load(df, 'postreview-enagement-aggregate-metrics')
    print_results(result)

flow.run()

