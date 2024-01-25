import importlib
import uuid
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import src.new_tab_recommendations.aggregate_engagement_flow as ntr
from prefect.testing.utilities import prefect_test_harness
from tests.utils import async_patch


@pytest.fixture
def mock_bigquery_snowflake_data(request):
    # This is a dynamic fixture that can mock data from activity_stream or glean.
    data_type = request.param

    if data_type == "activity_stream":
        join_column_name = "TILE_ID"
        ids = [4139552719614857, 2352534083175407, 3984239929814862]
    elif data_type == "glean":
        join_column_name = "CORPUS_RECOMMENDATION_ID"
        ids = [
            "f7c76d5f-6df4-4588-9028-22a88ce6927c",
            "9a1dfb60-072d-49e2-83b1-4c99f59fb0dc",
            "336da8e7-405d-4a7e-afd2-b845441a8f8c",
        ]
    else:
        raise ValueError(f"Unsupported request param {data_type}")

    df_bq = pd.DataFrame(
        {
            join_column_name: ids,
            "UPDATED_AT": [
                "2023-11-28T21:46:39Z",
                "2023-11-28T21:46:39Z",
                "2023-11-28T21:46:39Z",
            ],
            "TRAILING_1_DAY_IMPRESSIONS": [11315860, 9522976, 9506341],
            "TRAILING_1_DAY_OPENS": [101535, 85236, 85746],
        }
    )

    snowflake_data = [
        {
            join_column_name: id_value,
            "KEY": f"NEW_TAB_DE_DE/b3701f72-744a-5b5f-a5b5-7dae5a57d0fd/{uuid.uuid4()}",
            "RECOMMENDATION_SURFACE_ID": "NEW_TAB_DE_DE",
            "CORPUS_SLATE_CONFIGURATION_ID": "b3701f72-744a-5b5f-a5b5-7dae5a57d0fd",
            "CORPUS_ITEM_ID": str(uuid.uuid4()),
        }
        for id_value in ids
    ]

    return join_column_name, df_bq, snowflake_data


MODULE = "src.new_tab_recommendations.aggregate_engagement_flow"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_bigquery_snowflake_data", ["activity_stream", "glean"], indirect=True
)
async def test_export_telemetry_by_corpus_item_id(mock_bigquery_snowflake_data):
    join_column_name, bigquery_data, snowflake_data = mock_bigquery_snowflake_data

    with (
        async_patch(f"{MODULE}.bigquery_query", return_value=bigquery_data),
        async_patch(f"{MODULE}.snowflake_query", return_value=snowflake_data),
        patch(f"{MODULE}.MozGcp", return_value=MagicMock()),
    ):
        result = await ntr.export_telemetry_by_corpus_item_id(
            "select foo from bar", join_column_name
        )

        assert len(result) == len(bigquery_data)

        # Assert correct dataframe structure and content
        assert set(result.columns) == {
            "UPDATED_AT",
            "TRAILING_1_DAY_IMPRESSIONS",
            "TRAILING_1_DAY_OPENS",
            "KEY",
            "RECOMMENDATION_SURFACE_ID",
            "CORPUS_SLATE_CONFIGURATION_ID",
            "CORPUS_ITEM_ID",
        }


@pytest.mark.asyncio
async def test_aggregate_engagement():
    # Mock data returned by export_telemetry_by_corpus_item_id
    return_values = [
        pd.DataFrame(
            {
                "UPDATED_AT": ["1", "2"],
                "TRAILING_1_DAY_IMPRESSIONS": [100, 200],
                "TRAILING_1_DAY_OPENS": [1, 2],
                "KEY": ["1", "2"],
                "RECOMMENDATION_SURFACE_ID": ["r1", "r1"],
                "CORPUS_SLATE_CONFIGURATION_ID": ["s1", "s1"],
                "CORPUS_ITEM_ID": ["foo1", "foo2"],
            }
        ),
        pd.DataFrame(
            {
                "UPDATED_AT": ["2", "3"],
                "TRAILING_1_DAY_IMPRESSIONS": [300, 400],
                "TRAILING_1_DAY_OPENS": [3, 4],
                "KEY": ["2", "3"],
                "RECOMMENDATION_SURFACE_ID": ["r1", "r1"],
                "CORPUS_SLATE_CONFIGURATION_ID": ["s1", "s1"],
                "CORPUS_ITEM_ID": ["foo2", "foo3"],
            }
        ),
    ]

    with (
        prefect_test_harness(),
        async_patch(
            f"{MODULE}.export_telemetry_by_corpus_item_id", side_effect=return_values
        ) as mock_export,
        async_patch(
            f"{MODULE}.dataframe_to_feature_group"
        ) as mock_dataframe_to_feature_group,
    ):
        # Call the aggregate_engagement function
        await ntr.aggregate_engagement()  # type: ignore

        # Assert that dataframe_to_feature_group is called with the expected DataFrame
        assert mock_dataframe_to_feature_group.call_count == 1  # type: ignore

        pd.testing.assert_frame_equal(
            mock_dataframe_to_feature_group.call_args.kwargs["dataframe"],  # type: ignore  # noqa: E501
            pd.DataFrame(
                {
                    "UPDATED_AT": ["1", "2", "3"],
                    "KEY": ["1", "2", "3"],
                    "RECOMMENDATION_SURFACE_ID": ["r1", "r1", "r1"],
                    "CORPUS_SLATE_CONFIGURATION_ID": ["s1", "s1", "s1"],
                    "CORPUS_ITEM_ID": ["foo1", "foo2", "foo3"],
                    "TRAILING_1_DAY_IMPRESSIONS": [100, 500, 400],
                    "TRAILING_1_DAY_OPENS": [1, 5, 4],
                    "TRAILING_7_DAY_IMPRESSIONS": [0, 0, 0],
                    "TRAILING_7_DAY_OPENS": [0, 0, 0],
                    "TRAILING_14_DAY_IMPRESSIONS": [0, 0, 0],
                    "TRAILING_14_DAY_OPENS": [0, 0, 0],
                    "TRAILING_21_DAY_IMPRESSIONS": [0, 0, 0],
                    "TRAILING_21_DAY_OPENS": [0, 0, 0],
                    "TRAILING_28_DAY_IMPRESSIONS": [0, 0, 0],
                    "TRAILING_28_DAY_OPENS": [0, 0, 0],
                }
            ),
        )


@pytest.mark.parametrize("deployment_type", ["dev", "staging", "main"])
def test_optional_suffix(deployment_type):
    importlib.reload(ntr)  # type: ignore
    ntr.CS.deployment_type = deployment_type
    x = ntr.optional_suffix()
    if deployment_type == "main":
        assert x == ""
    else:
        assert x == f"_{deployment_type}"
