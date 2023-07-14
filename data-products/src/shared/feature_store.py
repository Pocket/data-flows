import boto3
import pandas as pd
from common.settings import Settings, CommonSettings
from prefect import get_run_logger, task
from sagemaker.feature_store.feature_group import FeatureGroup, IngestionManagerPandas
from sagemaker.session import Session

CS = CommonSettings()


class FeatureGroupSettings(Settings):
    """Settings for Sagemaker Feature Groups."""

    feature_group_prefix: str = "production" if CS.is_production else "development"
    corpus_engagement_feature_group_name: str = f"{feature_group_prefix}-corpus-engagement-v1"


@task
def dataframe_to_feature_group(dataframe: pd.DataFrame, feature_group_name: str) -> IngestionManagerPandas:
    """
    Update SageMaker feature group.

    Args:
        dataframe : the data in a dataframe to upload to the feature group
        feature_group_name: the name of the feature group to upload the data to

    Returns:
        EITHER -
        success case: the success message from the feature group API
        failure case: a description of the feature group as it currently stands for debugging
    """
    logger = get_run_logger()
    logger.info(f"Feature Group: {feature_group_name}")
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    return feature_group.ingest(data_frame=dataframe, max_workers=4, max_processes=4, wait=True)
