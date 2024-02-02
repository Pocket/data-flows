from unittest import mock

import aioboto3
import pandas as pd
import pytest
import pytest_asyncio
from botocore.stub import Stubber
from common.settings import CommonSettings
from prefect.testing.utilities import prefect_test_harness
from shared.feature_store import (
    INGEST_ROWS_RETRIES,
    dataframe_to_feature_group,
    ingest_row,
)

CS = CommonSettings()  # type: ignore


@pytest.fixture()
def df_features() -> pd.DataFrame:
    return pd.DataFrame(
        [{"id": "foobar-1", "clicks": 200}, {"id": "foobar-2", "clicks": 300}]
    )


@pytest.fixture()
def features_row(df_features):
    return list(df_features.itertuples(index=False))[0]


@pytest.fixture()
def features_row_expected_params(features_row):
    return {
        "FeatureGroupName": "my_feature_group",
        "Record": [
            {"FeatureName": "id", "ValueAsString": str(features_row.id)},
            {"FeatureName": "clicks", "ValueAsString": str(features_row.clicks)},
        ],
    }


@pytest_asyncio.fixture
async def stubbed_featurestore(features_row_expected_params):
    async with aioboto3.Session().client(
        "sagemaker-featurestore-runtime"
    ) as featurestore:
        stubber = Stubber(featurestore)
        stubber.add_response(
            "put_record",
            {},
            expected_params=features_row_expected_params,
        )
        stubber.activate()

        yield featurestore
        stubber.assert_no_pending_responses()


@pytest_asyncio.fixture
async def stubbed_featurestore_error(features_row_expected_params):
    async with aioboto3.Session().client(
        "sagemaker-featurestore-runtime"
    ) as featurestore:
        stubber = Stubber(featurestore)
        stubber.add_client_error(
            "put_record",
            service_error_code="ServiceUnavailable",
            expected_params=features_row_expected_params,
        )
        stubber.activate()

        yield featurestore
        stubber.assert_no_pending_responses()


@pytest.mark.asyncio
async def test_ingest_row(features_row, stubbed_featurestore, caplog):
    await ingest_row(
        row=features_row,
        feature_group_name="my_feature_group",
        featurestore=stubbed_featurestore,
    )


@pytest.mark.asyncio
async def test_ingest_row_exception(features_row, stubbed_featurestore_error, caplog):
    with pytest.raises(stubbed_featurestore_error.exceptions.ServiceUnavailable):
        await ingest_row(
            row=features_row,
            feature_group_name="my_feature_group",
            featurestore=stubbed_featurestore_error,
        )


@pytest.mark.asyncio
async def test_dataframe_to_feature_group(df_features, caplog):
    with prefect_test_harness():
        with mock.patch(
            "shared.feature_store.ingest_row",
            new_callable=mock.AsyncMock,
            return_value=None,
        ) as mock_ingest_row:
            await dataframe_to_feature_group(
                df_features, feature_group_name="my_feature_group"
            )

            assert mock_ingest_row.call_count == len(df_features)


@pytest.mark.asyncio
async def test_dataframe_to_feature_group_exception(df_features, caplog):
    with prefect_test_harness():
        with mock.patch(
            "shared.feature_store.ingest_row", new_callable=mock.AsyncMock
        ) as mock_ingest_row:
            mock_ingest_row.side_effect = ValueError()
            with pytest.raises(ValueError):
                await dataframe_to_feature_group(
                    df_features,
                    feature_group_name="my_feature_group",
                )

            assert mock_ingest_row.call_count == (INGEST_ROWS_RETRIES + 1) * len(
                df_features
            )


@pytest.mark.asyncio
async def test_dataframe_file_to_feature_group_(df_features, caplog):
    with prefect_test_harness():
        with mock.patch(
            "shared.feature_store.ingest_row",
            new_callable=mock.AsyncMock,
            return_value=None,
        ) as mock_ingest_row:
            df_file = "/tmp/test+df_features.pkl"
            df_features.to_pickle(df_file)
            await dataframe_to_feature_group(
                df_file,
                feature_group_name="my_feature_group",
            )

            assert mock_ingest_row.call_count == len(df_features)
