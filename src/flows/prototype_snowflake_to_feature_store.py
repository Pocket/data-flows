import boto3
import pandas as pd
from prefect import task, Flow
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

# Setting the working directory to project root makes the path "src.lib.queries" for get_snowflake_query
from src.lib.queries import get_snowflake_query

@task
def extract():
    """
    Pull data from snowflake materialized tables.
    Returns:
    """
    return get_snowflake_query().run(query="""
    select resolved_id, resolved_url from ANALYTICS.DBT_CTROY.CONTENT limit 10
    """)

    # TODO: How to put this in a data frame?

@task
def load(df: pd.DataFrame, feature_group_name):
    """
    Update SageMaker feature group.

    Args:cd ..
    See Also
        df:
        feature_group_name:

    Returns:

    """
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.ingest(df, max_workers=4, max_processes=4, wait=True)

@task
def print_results(results):
    print(results)


with Flow("User Impression Feature Group Flow") as flow:
    df = extract()
    print_results(df)
    # load(df, 'some_name')

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
