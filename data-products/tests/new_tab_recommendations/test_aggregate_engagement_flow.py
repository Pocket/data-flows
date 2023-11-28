import functools
import uuid
from unittest.mock import patch, MagicMock, AsyncMock

import pandas as pd
import pytest

from src.new_tab_recommendations.aggregate_engagement_flow import (
    export_telemetry_by_corpus_item_id,
)


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


async_patch = functools.partial(patch, new_callable=AsyncMock)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_bigquery_snowflake_data", ["activity_stream", "glean"], indirect=True
)
async def test_export_telemetry_by_corpus_item_id(mock_bigquery_snowflake_data):
    join_column_name, bigquery_data, snowflake_data = mock_bigquery_snowflake_data

    module = "src.new_tab_recommendations.aggregate_engagement_flow"

    with (
        async_patch(f"{module}.bigquery_query", return_value=bigquery_data),
        async_patch(f"{module}.snowflake_query", return_value=snowflake_data),
        patch(f"{module}.PktGcpCredentials", return_value=MagicMock()),
    ):
        result = await export_telemetry_by_corpus_item_id(
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
