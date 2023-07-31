import asyncio
import logging
from unittest import mock
from typing import Dict, List

import aioboto3
import pandas as pd
import pytest
import pytest_asyncio
from botocore.exceptions import StubAssertionError
from botocore.stub import Stubber
from common.settings import CommonSettings
from prefect.testing.utilities import prefect_test_harness

from shared.feature_store import ingest_row, dataframe_to_feature_group

CS = CommonSettings()  # type: ignore


@pytest.fixture()
def df_features() -> pd.DataFrame:
    return pd.DataFrame(
        [{"id": "foobar-1", "clicks": 200}, {"id": "foobar-2", "clicks": 300}]
    )


@pytest.fixture()
def features_row(df_features):
    return list(df_features.itertuples(index=False))[0]


def stub_featurestore(
    client, expected_feature_group_name: str, expected_record: List[Dict]
):
    stubber = Stubber(client)
    stubber.add_response(
        "put_record",
        {},
        expected_params={
            "FeatureGroupName": expected_feature_group_name,
            "Record": expected_record,
        },
    )
    stubber.activate()


@pytest_asyncio.fixture
async def stubbed_featurestore():
    async with aioboto3.Session().client(
        "sagemaker-featurestore-runtime"
    ) as featurestore:
        stub_featurestore(
            featurestore,
            expected_feature_group_name="my_feature_group",
            expected_record=[
                {"FeatureName": "id", "ValueAsString": "foobar-1"},
                {"FeatureName": "clicks", "ValueAsString": "200"},
            ],
        )

        yield featurestore


@pytest.mark.asyncio
async def test_ingest_row(features_row, stubbed_featurestore, caplog):
    await ingest_row(
        semaphore=asyncio.Semaphore(1),
        row=features_row,
        feature_group_name="my_feature_group",
        featurestore=stubbed_featurestore,
        logger=logging.getLogger(),
    )

    assert not any(r.levelname == "ERROR" for r in caplog.records)


@pytest.mark.asyncio
async def test_ingest_row_exception(features_row, stubbed_featurestore, caplog):
    with pytest.raises(StubAssertionError):
        await ingest_row(
            semaphore=asyncio.Semaphore(1),
            row=features_row,
            feature_group_name="wrong_name_will_raise_exception",
            featurestore=stubbed_featurestore,
            logger=logging.getLogger(),
            retry_delay_seconds=0.1,
        )

        error_log_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_log_records) == 3
        assert "StubAssertionError" in error_log_records[0].message


@pytest.mark.asyncio
async def test_dataframe_to_feature_group(df_features, caplog):
    with prefect_test_harness():
        with mock.patch(
            "shared.feature_store.ingest_row",
            new_callable=mock.AsyncMock,
            return_value=None,
        ):
            await dataframe_to_feature_group(
                df_features,
                feature_group_name="my_feature_group",
            )

            assert any(
                r.message
                == "Successfully ingested 2 records. Failed to ingest 0 records."
                for r in caplog.records
            )


@pytest.mark.asyncio
async def test_dataframe_to_feature_group_exception(df_features, caplog):
    with prefect_test_harness():
        with mock.patch("shared.feature_store.ingest_row", new_callable=mock.AsyncMock) as mock_ingest_row:
            mock_ingest_row.side_effect = ValueError()
            with pytest.raises(ValueError):
                await dataframe_to_feature_group(
                    df_features,
                    feature_group_name="my_feature_group",
                )

                assert any(
                    r.message
                    == "Successfully ingested 0 records. Failed to ingest 2 records."
                    for r in caplog.records
                )
