import asyncio
from asyncio import run
from typing import NamedTuple

import aioboto3
import pandas as pd
from common.settings import CommonSettings, Settings
from prefect import flow, get_run_logger, task
from prefect_dask import DaskTaskRunner

CS = CommonSettings()


class FeatureGroupSettings(Settings):
    """Settings for Sagemaker Feature Groups."""

    feature_group_prefix: str = "production" if CS.is_production else "development"
    corpus_engagement_feature_group_name: str = (
        f"{feature_group_prefix}-corpus-engagement-v1"
    )


@flow(
    validate_parameters=False,
    task_runner=DaskTaskRunner(
        cluster_kwargs={"n_workers": 4, "threads_per_worker": 1}
    ),
)
async def dataframe_to_feature_group(
    dataframe: pd.DataFrame | str,
    feature_group_name: str,
    concurrency_limit: int = 100,
):
    """
    Update SageMaker feature group.

    :param dataframe: the data in a dataframe to upload to the feature group, can also be path
    to pickle file.
    :param feature_group_name: the name of the feature group to upload the data to
    :param concurrency_limit: Maximum number of concurrent HTTP requests to make to the FeatureGroup.
    """
    if not isinstance(dataframe, pd.DataFrame):
        dataframe = pd.read_pickle(dataframe)

    # Split dataframe into chunks with at most concurrency_limit rows.
    tasks = [
        ingest_dataframe(
            dataframe=dataframe[i : i + concurrency_limit],
            feature_group_name=feature_group_name,
        )
        for i in range(0, dataframe.shape[0], concurrency_limit)
    ]

    logger = get_run_logger()
    logger.info(
        f"Ingesting {len(dataframe)} records in {len(tasks)} chunks into {feature_group_name}"
    )
    await asyncio.gather(*tasks)


INGEST_ROWS_RETRIES = 2


@task(retries=INGEST_ROWS_RETRIES)
async def ingest_dataframe(dataframe: pd.DataFrame, feature_group_name: str):
    async with aioboto3.Session().client(
        "sagemaker-featurestore-runtime"
    ) as featurestore:
        tasks = [
            ingest_row(
                row=row,
                featurestore=featurestore,
                feature_group_name=feature_group_name,
            )
            for row in dataframe.itertuples(index=False)
        ]

        await asyncio.gather(*tasks)


async def ingest_row(
    row: NamedTuple,
    feature_group_name: str,
    featurestore,
):
    """Ingest a single Dataframe row into FeatureStore.

    Args:
        :param row: current row that is being ingested
        :param feature_group_name: name of the Feature Group.
        :param featurestore: aioboto3 client for sagemaker-featurestore-runtime
    """
    await featurestore.put_record(
        FeatureGroupName=feature_group_name,
        Record=[
            {
                "FeatureName": column_name,
                "ValueAsString": str(getattr(row, column_name)),
            }
            for column_name in row._fields  # Seems this is a common way to iterate over a NamedTuple's fields?
        ],
    )


if __name__ == "__main__":
    run(
        dataframe_to_feature_group(
            dataframe=pd.DataFrame(
                [
                    {
                        "KEY": "NEW_TAB_FR_FR/41081474-0b70-5cd8-8177-f495b371030b/eca15def-ec21-49c4-b828-5014815b1918",
                        "UPDATED_AT": "2023-07-28T18:52:17Z",
                        "RECOMMENDATION_SURFACE_ID": "NEW_TAB_FR_FR",
                        "CORPUS_SLATE_CONFIGURATION_ID": "41081474-0b70-5cd8-8177-f495b371030b",
                        "CORPUS_ITEM_ID": "eca15def-ec21-49c4-b828-5014815b1918",
                        "TRAILING_1_DAY_IMPRESSIONS": 3487376,
                        "TRAILING_1_DAY_OPENS": 17304,
                        "TRAILING_7_DAY_IMPRESSIONS": 0,
                        "TRAILING_7_DAY_OPENS": 0,
                        "TRAILING_14_DAY_IMPRESSIONS": 0,
                        "TRAILING_14_DAY_OPENS": 0,
                        "TRAILING_21_DAY_IMPRESSIONS": 0,
                        "TRAILING_21_DAY_OPENS": 0,
                        "TRAILING_28_DAY_IMPRESSIONS": 0,
                        "TRAILING_28_DAY_OPENS": 0,
                    }
                ]
            ),
            feature_group_name=FeatureGroupSettings().corpus_engagement_feature_group_name,
        )
    )
