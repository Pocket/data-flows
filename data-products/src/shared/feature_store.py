import asyncio
import logging
import random
from asyncio import run
from typing import NamedTuple

import aioboto3
import pandas as pd
from common.settings import Settings, CommonSettings
from prefect import get_run_logger, task, flow
from prefect_dask import DaskTaskRunner

CS = CommonSettings()


class FeatureGroupSettings(Settings):
    """Settings for Sagemaker Feature Groups."""

    feature_group_prefix: str = "production" if CS.is_production else "development"
    corpus_engagement_feature_group_name: str = f"{feature_group_prefix}-corpus-engagement-v1"


@flow(validate_parameters=False)
async def dataframe_to_feature_group(dataframe: pd.DataFrame, feature_group_name: str, concurrency_limit: int = 100):
    """
    Update SageMaker feature group.

    :param dataframe: the data in a dataframe to upload to the feature group
    :param feature_group_name: the name of the feature group to upload the data to
    :param concurrency_limit: Maximum number of concurrent HTTP requests to make to the FeatureGroup.
    """
    logger = get_run_logger()
    logger.info(f"Feature Group: {feature_group_name}")

    aioboto3_session = aioboto3.Session()
    semaphore = asyncio.Semaphore(concurrency_limit)

    async with aioboto3_session.client('sagemaker-featurestore-runtime') as featurestore:
        tasks = [
            ingest_row(
                semaphore=semaphore,
                row=row,
                feature_group_name=feature_group_name,
                featurestore=featurestore,
            )
            for row in dataframe.itertuples(index=False)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

    exception_count = len([result for result in results if isinstance(result, Exception)])
    success_count = len(results) - exception_count
    logger.info(f'Successfully ingested {success_count} records. Failed to ingest {exception_count} records.')

    if exception_count:
        # Re-raise first exception to let the flow fail.
        raise next(result for result in results if isinstance(result, Exception))


# @task(retries=3)  # Wrapping function in a Prefect task results in RuntimeErrors "bound to a different event loop"
async def ingest_row(
    semaphore: asyncio.Semaphore,
    row: NamedTuple,
    feature_group_name: str,
    featurestore,
    retries=2,
    retry_delay_seconds=1,
):
    """Ingest a single Dataframe row into FeatureStore.

    Args:
        :param semaphore: Semaphore to limit concurrency.
        :param row: current row that is being ingested
        :param feature_group_name: name of the Feature Group.
        :param featurestore: aioboto3 client for sagemaker-featurestore-runtime
        :param retries: Number of times to retry on failure
        :param retry_delay_seconds: Delay in seconds before retrying
    """
    async with semaphore:
        # Ideally, we use Prefect to retry, but this resulted in an event loop error, and seemed much slower.
        for retry in range(retries + 1):
            try:
                await featurestore.put_record(
                    FeatureGroupName=feature_group_name,
                    Record=[
                        {
                            'FeatureName': column_name,
                            'ValueAsString': str(getattr(row, column_name)),
                        }
                        for column_name in row._fields  # Seems this is a common way to iterate over a NamedTuple's fields?
                    ]
                )
            except Exception as e:
                logger = get_run_logger()
                logger.error(f"Failed featurestore.put_record on retry {retry}/{retries} with {e}")
                if retry == retries:
                    raise e

                await asyncio.sleep(retry_delay_seconds)
            else:
                # Success
                break


if __name__ == "__main__":
    run(dataframe_to_feature_group(
        dataframe=pd.DataFrame([{
            'KEY': 'NEW_TAB_FR_FR/41081474-0b70-5cd8-8177-f495b371030b/eca15def-ec21-49c4-b828-5014815b1918',
            'UPDATED_AT': '2023-07-28T18:52:17Z',
            'RECOMMENDATION_SURFACE_ID': 'NEW_TAB_FR_FR',
            'CORPUS_SLATE_CONFIGURATION_ID': '41081474-0b70-5cd8-8177-f495b371030b',
            'CORPUS_ITEM_ID': 'eca15def-ec21-49c4-b828-5014815b1918',
            'TRAILING_1_DAY_IMPRESSIONS': 3487376, 'TRAILING_1_DAY_OPENS': 17304,
            'TRAILING_7_DAY_IMPRESSIONS': 0, 'TRAILING_7_DAY_OPENS': 0,
            'TRAILING_14_DAY_IMPRESSIONS': 0, 'TRAILING_14_DAY_OPENS': 0,
            'TRAILING_21_DAY_IMPRESSIONS': 0, 'TRAILING_21_DAY_OPENS': 0,
            'TRAILING_28_DAY_IMPRESSIONS': 0, 'TRAILING_28_DAY_OPENS': 0,
        }]),
        feature_group_name=FeatureGroupSettings().corpus_engagement_feature_group_name,
    ))
