import boto3
import pandas as pd
from prefect import task
from sagemaker.feature_store.feature_group import FeatureGroup, IngestionManagerPandas
from sagemaker.session import Session

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
