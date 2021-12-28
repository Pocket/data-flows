import pandas as pd
from prefect import task, Flow

import boto3
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

# Setting the working directory to project root makes the path "src.lib.queries" for get_snowflake_query
from src.lib.queries import get_snowflake_query

@task
def extract():
    """
    Pull data from snowflake materialized tables.

    Returns:

    A dataframe containing the results of a snowflake query represented as a pandas dataframe
    """
    query_result =  get_snowflake_query().run(query="""
    select 
    
    resolved_id, 
    resolved_url, 
    to_varchar(published_at,'yyyy-MM-dd"T"HH:mm:ssZ')::string as published_at
    
    from ANALYTICS.DBT.CONTENT limit 10
    """)
    return pd.DataFrame(query_result, columns=['RESOLVED_ID', 'RESOLVED_URL', 'PUBLISHED_AT'])

    # TODO: How to put this in a data frame?

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
def print_results(results):
    print(results)

with Flow("User Impression Feature Group Flow") as flow:
    df = extract()
    print_results(df)
    result = load(df, 'test-prefect')
    print_results(result)

flow.run()

# flow.storage = prefect.storage.S3(
#     bucket='pocket-dataflows-storage-prod',
#     add_default_labels=False
# )
#
# flow.run_config = ECSRun(
#     labels=['Prod'],
#     image='996905175585.dkr.ecr.us-east-1.amazonaws.com/dataflows-prod-app:latest',
# )
#
# flow.register(project_name="prefect-tutorial")
