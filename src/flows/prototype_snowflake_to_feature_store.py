import prefect
from prefect import task, Flow, Parameter, unmapped
from prefect.run_configs import ECSRun
from prefect.tasks.aws.s3 import S3List
from prefect.tasks.snowflake import SnowflakeQuery
import pandas as pd
import boto3
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
from ..lib.queries import get_snowflake_query


@task
def extract():
    """
    Pull data from snowflake materialized tables.
    Returns:
    """
    get_snowflake_query.run("""
    SELECT * FROM table_name
    """)

    # TODO: How to put this in a data frame?

@task
def load(df: pd.DataFrame, feature_group_name):
    """
    Update SageMaker feature group.

    Args:
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


with Flow("User Impression Feature Group Flow") as flow:
    df = extract.run()
    load(df, 'some_name')


# flow.run()

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
